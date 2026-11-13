//! Router-level authentication for the A2A REST routes.
//!
//! A single [`A2aAuthLayer`] (a [`tower::Layer`]) is installed on the SDK REST
//! sub-router by [`super::a2a_router`] / [`super::a2a_router_with_jobset`], so
//! authentication is checked for EVERY A2A route BEFORE the SDK parses or
//! validates the request body. The public agent card router is merged in
//! separately and is never gated.
//!
//! The layer enforces the same rules as the (previously per-method) handler
//! authorization:
//! - a request that PRESENTS a jobset credential header pair is verified
//!   against the *target* task/agent of the request (target-aware, constant
//!   time); a request that presents no jobset credential is accepted only when
//!   the operator/bearer verifier accepts it (or no verifier is configured --
//!   the network-policy-only model).
//!
//! A request that fails answers HTTP `401` with the A2A `application/problem+json`
//! envelope (status `UNAUTHENTICATED`) and a `WWW-Authenticate: Bearer` header,
//! per the A2A HTTP+JSON binding. The body shape mirrors the SDK's own error
//! envelope so a client sees one consistent error format.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::Service;

use super::JobsetCred;
use crate::core::a2a::ingress::{self, HopGuard};
use crate::core::a2a::AuthVerifier;

/// Largest `/message:send` body the auth layer reads to resolve the target (the
/// server's own message cap is far below this).
const SEND_BODY_LIMIT: usize = 64 * 1024;

/// The router-level A2A auth layer.
#[derive(Clone)]
pub struct A2aAuthLayer {
    verifier: Option<AuthVerifier>,
    jobset_registry: Option<Arc<crate::core::jobset::JobsetRegistry>>,
    /// The receiver-authoritative hop guard for jobset-authenticated sends
    /// (`None` when this router has no jobset context: the claimed hop stays
    /// bounded by the handler's own check there).
    hop_guard: Option<Arc<HopGuard>>,
}

impl A2aAuthLayer {
    pub fn new(verifier: Option<AuthVerifier>) -> Self {
        Self {
            verifier,
            jobset_registry: None,
            hop_guard: None,
        }
    }

    pub fn with_jobset(verifier: Option<AuthVerifier>, jobset: Option<&JobsetCred>) -> Self {
        Self {
            verifier,
            jobset_registry: jobset.map(|j| j.registry.clone()),
            // One guard per router: replay + budget state is in-memory and
            // shared across the layer's clones (Arc).
            hop_guard: jobset.map(|_| Arc::new(HopGuard::new())),
        }
    }
}

impl<S> tower::Layer<S> for A2aAuthLayer {
    type Service = A2aAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        A2aAuth {
            inner,
            verifier: self.verifier.clone(),
            jobset_registry: self.jobset_registry.clone(),
            hop_guard: self.hop_guard.clone(),
        }
    }
}

/// The service produced by [`A2aAuthLayer`].
#[derive(Clone)]
pub struct A2aAuth<S> {
    inner: S,
    verifier: Option<AuthVerifier>,
    jobset_registry: Option<Arc<crate::core::jobset::JobsetRegistry>>,
    hop_guard: Option<Arc<HopGuard>>,
}

impl<S> Service<Request> for A2aAuth<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, S::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let verifier = self.verifier.clone();
        let jobset_registry = self.jobset_registry.clone();
        let hop_guard = self.hop_guard.clone();
        let inner = self.inner.clone();

        Box::pin(async move {
            let (parts, body) = request.into_parts();

            // The layer reads the body ONLY for a `/message:send` request it
            // must rewrite: every NON-operator send gets the `a2a.interrupt`
            // escalation request stripped (N6: interrupt is the operator's
            // escalation; a jobset member's message is still delivered, just
            // never interrupted), and a jobset-authenticated send gets the
            // receiver-authoritative hop check (N1). Every other request's
            // body is forwarded untouched, so the SDK parses it exactly as
            // before.
            let path = parts.uri.path();
            let is_send = matches!(path, "/message:send" | "/message/send");
            let presented = presented_jobset(&parts.headers);
            // The operator credential: no jobset headers presented AND the
            // bearer verifier explicitly accepts. A presented jobset
            // credential never downgrades to the operator (the decide below
            // is target-aware and refuses an invalid one), so an
            // operator-verified request is exactly: no jobset headers, and
            // the verifier accepts. With no verifier configured (the explicit
            // `api_allow_unauthenticated` model) nothing is operator-verified
            // and every send is rewritten like a jobset-member message.
            let operator_ok =
                presented.is_none() && verifier.as_ref().is_some_and(|v| v(&parts.headers));

            let body = if is_send && !operator_ok {
                // Bounded read: an unauthenticated peer must not make the
                // layer buffer an arbitrary body just to learn the target id.
                let bytes = match http_body_util::Limited::new(body, SEND_BODY_LIMIT)
                    .collect()
                    .await
                {
                    Ok(c) => c.to_bytes(),
                    Err(_) => return Ok(unauthenticated_response()),
                };
                let mut text = String::from_utf8_lossy(&bytes).into_owned();

                // N6: the explicit `interrupt` escalation is honoured only for
                // the operator credential. From any other sender the flag is
                // stripped here (before the SDK parses the body), so the
                // message is still delivered but never escalates to
                // kill-and-resume.
                if strip_interrupt_from_send_body(&mut text) {
                    eprintln!(
                        "a2a auth: stripped a2a.interrupt from a non-operator send (message delivered, no interrupt)"
                    );
                }

                // The credential check runs BEFORE the hop guard below, so a
                // refused credential can never charge the per-jobset message
                // budget or the replay cache.
                if !decide(
                    jobset_registry.as_deref(),
                    verifier.as_ref(),
                    &parts.headers,
                    &parts.uri,
                    presented.as_ref().map(|(id, _)| id.as_str()),
                    Some(text.as_str()),
                ) {
                    return Ok(unauthenticated_response());
                }

                // N1: a jobset-authenticated send goes through the
                // receiver-authoritative hop guard (the signed hop token,
                // replay protection, jobset budget): the sender's own
                // `a2a.hop` integer is ignored, the verified hop is written
                // into the metadata the handler reads. Fail closed: a missing,
                // invalid, expired or replayed token (or an exhausted budget)
                // is refused before the SDK sees the request.
                if presented.is_some() {
                    if let Some(guard) = hop_guard.as_deref() {
                        let Some(registry) = jobset_registry.as_ref() else {
                            return Ok(unauthenticated_response());
                        };
                        match guarded_send_body(&text, registry, guard) {
                            Ok(rewritten) => text = rewritten,
                            Err(reject) => return Ok(reject.into_response()),
                        }
                    }
                    // A presented jobset credential without a hop guard
                    // (no jobset context on this router): the credential
                    // itself cannot verify; the decide above refuses.
                }
                Body::from(text)
            } else {
                if !decide(
                    jobset_registry.as_deref(),
                    verifier.as_ref(),
                    &parts.headers,
                    &parts.uri,
                    presented.as_ref().map(|(id, _)| id.as_str()),
                    None,
                ) {
                    return Ok(unauthenticated_response());
                }
                body
            };

            let mut inner = inner;
            inner.call(Request::from_parts(parts, body)).await
        })
    }
}

/// The presented jobset credential header pair (id + secret), when BOTH are
/// present and non-empty (mirrors the handler's `presented_jobset_credential`).
fn presented_jobset(headers: &HeaderMap) -> Option<(String, String)> {
    let id = header_value(headers, crate::core::jobset::JOBSET_ID_HEADER).filter(|s| !s.is_empty());
    let secret =
        header_value(headers, crate::core::jobset::JOBSET_SECRET_HEADER).filter(|s| !s.is_empty());
    match (id, secret) {
        (Some(id), Some(secret)) => Some((id, secret)),
        _ => None,
    }
}

/// Reads one header value (case-insensitive) from an axum [`HeaderMap`].
fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// Resolves the TARGET task/agent of a request from the URI (and, for a
/// `/message:send` request, the message body) -- the same target the handler's
/// per-method authorization uses, so the router-level check is equivalent.
fn target_for(uri: &Uri, send_body: Option<&str>) -> Option<String> {
    let path = uri.path();
    if let Some(rest) = path.strip_prefix("/tasks/") {
        let id = rest.split(['/', ':']).next().unwrap_or(rest);
        if !id.is_empty() {
            // Normalise percent-encoded keys BEFORE the auth decision: axum's
            // Path extractor decodes the segment, so the id the handler sees
            // is the decoded one -- the membership check must verify the same
            // identity, never the raw encoding.
            return Some(percent_decode(id));
        }
    }
    if path == "/tasks" {
        return uri.query().and_then(query_context_id);
    }
    if matches!(path, "/message:send" | "/message/send") {
        return send_body.and_then(send_target_id);
    }
    None
}

/// The `?contextId=` value of a `GET /tasks` request, percent-decoded.
fn query_context_id(query: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some(v) = pair.strip_prefix("contextId=") {
            if !v.is_empty() {
                return Some(percent_decode(v));
            }
        }
    }
    None
}

/// The addressed task id of a `SendMessage` body: the message's `taskId`,
/// else its `contextId` (mirrors the handler's resolution).
fn send_target_id(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let message = value.get("message")?;
    let task_id = message
        .get("taskId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let context_id = message
        .get("contextId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    task_id.or(context_id)
}

/// Minimal percent-decoding (`%XX` -> byte); enough for a contextId lookup.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Removes the `a2a.interrupt` escalation request from a `/message:send`
/// body (N6: interrupt is honoured only for the operator credential; the
/// message itself is untouched and still delivered). Returns `true` when a
/// flag was present and removed. A body that is not a JSON object with a
/// `message` is forwarded unchanged (the SDK rejects it as before).
fn strip_interrupt_from_send_body(text: &mut String) -> bool {
    let Ok(mut value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let Some(message) = value.get_mut("message").and_then(Value::as_object_mut) else {
        return false;
    };
    let removed = message
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .is_some_and(|meta| {
            meta.remove(crate::core::a2a::delivery::META_INTERRUPT)
                .is_some()
        });
    if !removed {
        return false;
    }
    match serde_json::to_string(&value) {
        Ok(rewritten) => {
            *text = rewritten;
            true
        }
        // Re-serialization failure leaves the body unchanged (the SDK then
        // sees the original text; the flag would have been honoured, but a
        // serialization failure on a parsed JSON object cannot happen in
        // practice and fail-open on it is acceptable for a parsed object).
        Err(_) => false,
    }
}

/// A rejection from the guarded send path: `Unauthenticated` answers 401,
/// `Budget` answers 429 with `Retry-After`.
enum LayerReject {
    Unauthenticated,
    Budget { retry_after: u64 },
}

impl From<ingress::A2aReject> for LayerReject {
    fn from(reject: ingress::A2aReject) -> Self {
        match reject {
            ingress::A2aReject::BudgetExhausted { retry_after } => Self::Budget { retry_after },
            _ => Self::Unauthenticated,
        }
    }
}

impl LayerReject {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthenticated => unauthenticated_response(),
            Self::Budget { retry_after } => budget_exhausted_response(retry_after),
        }
    }
}

/// The HTTP 429 response for an exhausted per-jobset message budget, with the
/// `Retry-After` header the sender needs to pace its retry.
fn budget_exhausted_response(retry_after: u64) -> Response {
    let detail = a2a::errordetails::TypedDetail::error_info(
        "rate_limited",
        a2a::errordetails::PROTOCOL_DOMAIN,
        None,
    );
    let body = json!({
        "error": {
            "code": 429,
            "status": "RATE_LIMITED",
            "message": "a2a message budget exhausted for this jobset",
            "details": [detail],
        }
    });
    let retry = retry_after.to_string();
    (
        StatusCode::TOO_MANY_REQUESTS,
        [
            (header::CONTENT_TYPE, "application/problem+json"),
            (header::RETRY_AFTER, retry.as_str()),
        ],
        Json(body),
    )
        .into_response()
}

/// Runs the receiver-authoritative hop guard over a jobset-authenticated
/// `/message:send` body (N1): the sender's own `a2a.hop` integer is ignored,
/// the hop is read ONLY from the signed `a2a.hop_token` (HMAC'd with the
/// target jobset's secret), the message id is replay-checked and the per-jobset
/// message budget is charged -- all before the SDK parses the request. On
/// success returns the body with the VERIFIED hop written into the metadata
/// (and the one-time token removed), so the handler's own ingress check sees a
/// trusted hop.
///
/// Fail closed: a missing, malformed, invalid, expired or replayed token, or
/// an exhausted budget, is a rejection (401, or 429 with `Retry-After` for the
/// budget).
fn guarded_send_body(
    text: &str,
    registry: &crate::core::jobset::JobsetRegistry,
    guard: &HopGuard,
) -> Result<String, LayerReject> {
    // The whole request, so the guarded ingress sees the same parts and
    // metadata the handler's own check would.
    let request: a2a::SendMessageRequest =
        serde_json::from_str(text).map_err(|_| LayerReject::Unauthenticated)?;

    // The target must be a registered jobset member: its (jobset id, secret)
    // pair is what the token is verified against.
    let message_id = request.message.message_id.trim();
    if message_id.is_empty() {
        return Err(LayerReject::Unauthenticated);
    }
    let target = request
        .message
        .task_id
        .as_deref()
        .or(request.message.context_id.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or(LayerReject::Unauthenticated)?;
    let Some(entry) = registry.get_or_remembered(target) else {
        return Err(LayerReject::Unauthenticated);
    };
    // The secret the token must verify with: derived from the root key when
    // one is configured (the sender's credential file holds the same derived
    // value), else the registered random secret.
    let secret = match registry.root_key() {
        Some(root_key) => {
            crate::core::jobset::derive_jobset_secret(&root_key, &entry.jobset_id, entry.epoch)
        }
        None => entry.secret().to_string(),
    };

    let now = crate::core::a2a::inbox::now_unix();
    // The guarded ingress: size, empty-text and sender checks of the plain
    // path plus the authoritative hop, replay protection and the budget.
    let (verified, _info) = ingress::process_ingress_guarded(
        &request.message.parts,
        request.message.metadata.as_ref(),
        message_id,
        &entry.jobset_id,
        secret.as_bytes(),
        false,
        now,
        guard,
    )?;

    // Rewrite the body: the metadata the handler reads carries the VERIFIED
    // hop; the one-time token is removed (it is single-use).
    let mut value: Value = serde_json::from_str(text).map_err(|_| LayerReject::Unauthenticated)?;
    {
        let message = value
            .get_mut("message")
            .and_then(Value::as_object_mut)
            .ok_or(LayerReject::Unauthenticated)?;
        let meta = message.entry("metadata").or_insert_with(|| json!({}));
        let Some(meta) = meta.as_object_mut() else {
            return Err(LayerReject::Unauthenticated);
        };
        meta.insert(
            crate::core::a2a::client::META_HOP.to_string(),
            json!(verified.hop),
        );
        meta.remove(ingress::META_HOP_TOKEN);
    }
    serde_json::to_string(&value).map_err(|_| LayerReject::Unauthenticated)
}

/// The authorization decision: `true` = allowed, `false` = refuse (401).///
/// `presented_id` is the jobset id the request presents (when a jobset
/// credential header pair is present); `send_body` is the `/message:send` body
/// (only when a jobset credential is presented). A presented jobset credential
/// is verified against the TARGET of the request; a request that names no
/// target is refused.
fn decide(
    jobset_registry: Option<&crate::core::jobset::JobsetRegistry>,
    verifier: Option<&AuthVerifier>,
    headers: &HeaderMap,
    uri: &Uri,
    presented_id: Option<&str>,
    send_body: Option<&str>,
) -> bool {
    use crate::core::jobset::{verify_jobset_credential, HeaderGetter};
    if presented_id.is_some() {
        let Some(registry) = jobset_registry else {
            // No jobset context on this router: a presented jobset credential
            // cannot be verified, so it is refused (never downgraded to the
            // operator token).
            return false;
        };
        let owned_headers = headers.clone();
        let getter: HeaderGetter = Arc::new(move |name: &str| header_value(&owned_headers, name));
        let target = target_for(uri, send_body);
        return match target.as_deref() {
            Some(target) => verify_jobset_credential(registry, target, &getter),
            // No target named (an untargeted list like `GET /tasks` without
            // `?contextId=`, a send that addresses nothing): REFUSED. A
            // jobset credential only ever authorizes the jobset(s) it names,
            // so an untargeted request would either need any-member trust
            // (which reads every job on the pod across jobsets) or a
            // jobset-wide filter the layer cannot apply. Fail closed: the
            // caller must name its target (a task id or a `?contextId=`),
            // which is then membership-checked.
            None => false,
        };
    }
    // No jobset credential presented: the operator/bearer gate decides.
    match verifier {
        Some(verifier) => verifier(headers),
        None => true,
    }
}

/// The HTTP 401 `UNAUTHENTICATED` response: the A2A `application/problem+json`
/// envelope (the shape the other errors use) plus the `WWW-Authenticate`
/// header the A2A HTTP+JSON binding requires.
fn unauthenticated_response() -> Response {
    let timestamp = chrono::Utc::now().to_rfc3339();
    let detail = a2a::errordetails::TypedDetail::error_info(
        "UNAUTHENTICATED",
        a2a::errordetails::PROTOCOL_DOMAIN,
        Some(HashMap::from([("timestamp".to_string(), timestamp)])),
    );
    let body = json!({
        "error": {
            "code": 401,
            "status": "UNAUTHENTICATED",
            "message": "authentication required",
            "details": [detail],
        }
    });
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CONTENT_TYPE, "application/problem+json"),
            (header::WWW_AUTHENTICATE, "Bearer"),
        ],
        Json(body),
    )
        .into_response()
}

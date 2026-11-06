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
use serde_json::json;
use tower::Service;

use super::JobsetCred;
use crate::core::a2a::AuthVerifier;

/// Largest `/message:send` body the auth layer reads to resolve the target (the
/// server's own message cap is far below this).
const SEND_BODY_LIMIT: usize = 64 * 1024;

/// The router-level A2A auth layer.
#[derive(Clone)]
pub struct A2aAuthLayer {
    verifier: Option<AuthVerifier>,
    jobset_registry: Option<Arc<crate::core::jobset::JobsetRegistry>>,
}

impl A2aAuthLayer {
    pub fn new(verifier: Option<AuthVerifier>) -> Self {
        Self {
            verifier,
            jobset_registry: None,
        }
    }

    pub fn with_jobset(verifier: Option<AuthVerifier>, jobset: Option<&JobsetCred>) -> Self {
        Self {
            verifier,
            jobset_registry: jobset.map(|j| j.registry.clone()),
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
        }
    }
}

/// The service produced by [`A2aAuthLayer`].
#[derive(Clone)]
pub struct A2aAuth<S> {
    inner: S,
    verifier: Option<AuthVerifier>,
    jobset_registry: Option<Arc<crate::core::jobset::JobsetRegistry>>,
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
        let inner = self.inner.clone();

        Box::pin(async move {
            let (parts, body) = request.into_parts();

            // The layer reads the body ONLY for a `/message:send` request that
            // presents a jobset credential (the target-aware check needs the
            // addressed task id). Every other request's body is forwarded
            // untouched, so the SDK parses it exactly as before.
            let path = parts.uri.path();
            let is_send = matches!(path, "/message:send" | "/message/send");
            let presented = presented_jobset(&parts.headers);
            let need_body = jobset_registry.is_some() && is_send && presented.is_some();

            let (send_body, body) = if need_body {
                // Bounded read: an unauthenticated peer must not make the
                // layer buffer an arbitrary body just to learn the target id.
                let bytes = match http_body_util::Limited::new(body, SEND_BODY_LIMIT)
                    .collect()
                    .await
                {
                    Ok(c) => c.to_bytes(),
                    Err(_) => return Ok(unauthenticated_response()),
                };
                let text = String::from_utf8_lossy(&bytes).into_owned();
                (Some(text.clone()), Body::from(text))
            } else {
                (None, body)
            };

            if !decide(
                jobset_registry.as_deref(),
                verifier.as_ref(),
                &parts.headers,
                &parts.uri,
                presented.as_ref().map(|(id, _)| id.as_str()),
                send_body.as_deref(),
            ) {
                return Ok(unauthenticated_response());
            }

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
            return Some(id.to_string());
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

/// The authorization decision: `true` = allowed, `false` = refuse (401).
///
/// `presented_id` is the jobset id the request presents (when a jobset
/// credential header pair is present); `send_body` is the `/message:send` body
/// (only when a jobset credential is presented).
fn decide(
    jobset_registry: Option<&crate::core::jobset::JobsetRegistry>,
    verifier: Option<&AuthVerifier>,
    headers: &HeaderMap,
    uri: &Uri,
    presented_id: Option<&str>,
    send_body: Option<&str>,
) -> bool {
    use crate::core::jobset::{verify_jobset_credential, HeaderGetter};
    if let Some(presented_id) = presented_id {
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
            // No target named: the presented secret must still verify, against
            // a live member of the presented jobset.
            None => registry
                .any_member_of(presented_id)
                .is_some_and(|member| verify_jobset_credential(registry, &member, &getter)),
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

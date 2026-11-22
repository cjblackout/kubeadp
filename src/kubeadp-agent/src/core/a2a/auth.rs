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
//!   a state the mount sites never produce by accident: a server without an
//!   operator token mounts open A2A routes only with `KUBEADP_API_ALLOW_OPEN`
//!   set, otherwise it refuses to start, see
//!   `crate::core::server::token::a2a_open_permitted`).
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

/// How many seconds to wait before retrying a peer's `:end` propagation that
/// did not answer (SPEC §7.16: "retries with backoff until each peer has
/// answered"). The retry delay grows linearly and is capped so a temporarily
/// unreachable peer never hammers the mesh.
pub const END_RETRY_BASE_SECS: u64 = 1;
/// Upper bound of the linear backoff for peer `:end` propagation.
pub const END_RETRY_MAX_SECS: u64 = 10;
/// How many times a peer's `:end` is retried before the local end is
/// considered final (the local end stays permanent even if a peer is down).
pub const END_RETRY_ATTEMPTS: usize = 8;

/// Header the sender of a jobset-authenticated request presents: its own agent
/// id (SPEC §7.4). Verified against the derived member proof; any sender name
/// inside the message body or metadata is ignored.
pub use crate::core::jobset::AGENT_ID_HEADER;
/// Header the sender of a jobset-authenticated request presents: its derived
/// member proof (SPEC §7.3/§7.4). Binds the sender's agent id to the jobset.
pub use crate::core::jobset::AGENT_PROOF_HEADER;

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

/// Router-level AUTH layer that accepts ONLY the jobset credential (SPEC
/// §7.4/§7.5): the jobset-only layer carries NO bearer verifier and NO
/// open-mode pass-through. A request that presents no valid jobset credential
/// is refused `401` / `403` BEFORE any body validation.
///
/// This is the target layer of the stream-3 flag day (C5); it is BUILT here
/// (C4a) and selectable by the router builders, but is NOT yet mounted in
/// production — the bearer path stays the default until C5 flips both pods
/// together. See [`super::a2a_router_with_jobset_only`] / the C5 mount sites.
///
/// The layer verifies, per request, in order (SPEC §7.4):
///   1. the jobset has a local row, is not ended, and `X-Jobset-Secret`
///      equals the derived secret at the row's current epoch (constant time);
///   2. `X-Agent-Proof` equals the derived member proof of `X-Agent-Id` for
///      that jobset and epoch (constant time).
///
/// Failures of 1/2 answer `401 invalid_jobset_secret`; a request that names a
/// RECIPIENT (a send with a target, a `/tasks/{id}`) additionally requires
/// that the target is a `jobset_member` row (step 3); failure there answers
/// `403 no_shared_jobset`. A request that names no recipient
/// (`GET /tasks`, `GET /a2a/agents`) needs only 1 and 2.
///
/// The verified `X-Agent-Id` is passed down as `X-Agent-Id` (sender identity
/// for framing/rate/audit); the message body and metadata are untouched.
///
/// Error bodies are `application/problem+json` with `WWW-Authenticate: Bearer`
/// on `401`. Rate limiting and hops are handled by the guarded send path below.
#[derive(Clone)]
pub struct JobsetAuthLayer {
    registry: Arc<crate::core::jobset::JobsetRegistry>,
    hop_guard: Arc<HopGuard>,
    /// The shared per-(sender, recipient) token bucket (SPEC §7.6).
    rate: Arc<std::sync::Mutex<rlimits::Buckets>>,
}

impl JobsetAuthLayer {
    /// Builds the jobset-only layer over the given registry. One hop guard per
    /// router (replay + budget state is in-memory and shared across clones).
    pub fn new(registry: Arc<crate::core::jobset::JobsetRegistry>) -> Self {
        Self {
            registry,
            hop_guard: Arc::new(HopGuard::new()),
            rate: Arc::new(std::sync::Mutex::new(rlimits::Buckets::new(
                RATE_CAPACITY,
                RATE_REFILL_SECS,
            ))),
        }
    }
}

impl<S> tower::Layer<S> for JobsetAuthLayer {
    type Service = JobsetAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        JobsetAuth {
            inner,
            registry: self.registry.clone(),
            hop_guard: self.hop_guard.clone(),
            rate: self.rate.clone(),
        }
    }
}

/// The service produced by [`JobsetAuthLayer`].
#[derive(Clone)]
pub struct JobsetAuth<S> {
    inner: S,
    registry: Arc<crate::core::jobset::JobsetRegistry>,
    hop_guard: Arc<HopGuard>,
    rate: Arc<std::sync::Mutex<rlimits::Buckets>>,
}

impl<S> Service<Request> for JobsetAuth<S>
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
        let registry = self.registry.clone();
        let hop_guard = self.hop_guard.clone();
        let rate = self.rate.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let path = parts.uri.path();
            let is_send = matches!(path, "/message:send" | "/message/send");
            // The routes OUTSIDE the SDK REST set, served on the same
            // authorization layer (SPEC §7.9): a jobset-scoped agent list and
            // the owner-only jobset end route.
            let is_agents = path == "/a2a/agents";
            let is_end = end_path_id(path);

            // The per-(sender, recipient) rate limit of SPEC §7.6 (token
            // bucket: capacity 5, refill 1 per 6 s) is charged BEFORE the hop
            // guard, so a rate-exceeding sender cannot charge the per-jobset
            // budget or the replay cache either. It is keyed by the VERIFIED
            // sender id (never a claimed body/metadata sender) and applies to
            // a message send between that sender and its addressed recipient;
            // a list has no recipient and skips the limiter.

            if let Some(id) = is_end.as_deref() {
                // `POST /a2a/jobsets/{id}:end` (SPEC §7.16): the creator
                // proof of §7.2 item 3 authorizes ending the jobset (that
                // authority is broader than a plain membership, so it is
                // verified distinctly; a presented jobset credential for a
                // member is NOT enough). The layer verifies steps 1 and 2
                // against the row at its current epoch AND that X-Agent-Id is
                // the jobset's creator; the per-pod end + peer propagation
                // runs in the route handler (this layer only authorizes).
                match verify_jobset_creator(&registry, &parts.headers, id) {
                    None => return Ok(unauthenticated_response()),
                    Some(AuthOutcome::Invalid) => return Ok(unauthenticated_response()),
                    Some(AuthOutcome::Forbidden) => return Ok(no_shared_jobset_response()),
                    Some(AuthOutcome::Authorized(_)) => {}
                }
                return inner.call(Request::from_parts(parts, body)).await;
            }

            let body = if is_send {
                // The credential check runs BEFORE the hop guard below, so a
                // refused credential can never charge the per-jobset message
                // budget or the replay cache.
                let bytes = match http_body_util::Limited::new(body, SEND_BODY_LIMIT)
                    .collect()
                    .await
                {
                    Ok(c) => c.to_bytes(),
                    Err(_) => return Ok(unauthenticated_response()),
                };
                let mut text = String::from_utf8_lossy(&bytes).into_owned();
                // A jobset credential presented on a send names the shared
                // jobset; the recipient must be a member (step 3). A request
                // that presents no jobset credential is refused outright: this
                // is the jobset-only layer (SPEC §7.5).
                let Some((presented_id, _)) = presented_jobset(&parts.headers) else {
                    return Ok(unauthenticated_response());
                };
                let Some(recipient) = send_target_id(&text) else {
                    return Ok(unauthenticated_response());
                };
                let sender =
                    match verify_jobset(&registry, &parts.headers, Some(recipient.as_str())) {
                        None => return Ok(unauthenticated_response()),
                        Some(AuthOutcome::Invalid) => return Ok(unauthenticated_response()),
                        Some(AuthOutcome::Forbidden) => return Ok(no_shared_jobset_response()),
                        Some(AuthOutcome::Authorized(sender)) => sender,
                    };
                // SPEC §7.6 per-(sender, recipient) token bucket, keyed by the
                // VERIFIED sender id (never a claimed body/metadata sender).
                if let Some(retry_after) = sender_recipient_allow(&rate, &sender, &recipient) {
                    return Ok(rate_limited_response(retry_after));
                }
                // N1: the receiver-authoritative hop guard (signed hop token,
                // replay protection, jobset budget); the VERIFIED jobset is
                // the presented one (step 2 bound sender to it).
                match guarded_send_body_jobset(
                    &text,
                    &registry,
                    &hop_guard,
                    &recipient,
                    &presented_id,
                    &sender,
                ) {
                    Ok(rewritten) => text = rewritten,
                    Err(reject) => return Ok(reject.into_response()),
                }
                Body::from(text)
            } else if is_agents {
                // `GET /a2a/agents` names no recipient: steps 1 and 2 only
                // (SPEC §7.4). The handler below lists the members.
                match verify_jobset(&registry, &parts.headers, None) {
                    None => return Ok(unauthenticated_response()),
                    Some(AuthOutcome::Invalid) => return Ok(unauthenticated_response()),
                    Some(AuthOutcome::Forbidden) => return Ok(no_shared_jobset_response()),
                    Some(AuthOutcome::Authorized(_)) => {}
                }
                body
            } else {
                // SDK REST routes: a targeted request additionally requires
                // the target's membership (step 3).
                let target = target_for(&parts.uri, None);
                match verify_jobset(&registry, &parts.headers, target.as_deref()) {
                    None => return Ok(unauthenticated_response()),
                    Some(AuthOutcome::Invalid) => return Ok(unauthenticated_response()),
                    Some(AuthOutcome::Forbidden) => return Ok(no_shared_jobset_response()),
                    Some(AuthOutcome::Authorized(_)) => {}
                }
                body
            };

            inner.call(Request::from_parts(parts, body)).await
        })
    }
}

/// The target-agent id of a `:end` path (`/a2a/jobsets/{id}:end`), if the
/// path is one.
fn end_path_id(path: &str) -> Option<String> {
    let id = path.strip_prefix("/a2a/jobsets/")?.strip_suffix(":end")?;
    (!id.is_empty()).then(|| id.to_string())
}

/// SPEC §7.6 rate limit configuration: capacity 5, refill 1 per 6 s (10 per
/// minute).
pub const RATE_CAPACITY: u64 = 5;
/// Seconds between refills of one token (SPEC §7.6).
pub const RATE_REFILL_SECS: u64 = 6;

/// SPEC §7.6 per-(sender, recipient) token bucket, keyed by the VERIFIED
/// sender id and the addressed recipient. Returns `Some(retry_after_secs)`
/// when the bucket is empty (429 + Retry-After), `None` when the message is
/// admitted (and the bucket is charged one slot).
fn sender_recipient_allow(
    rate: &std::sync::Mutex<rlimits::Buckets>,
    sender: &str,
    recipient: &str,
) -> Option<u64> {
    let mut guard = rate.lock().unwrap_or_else(|e| e.into_inner());
    guard.try_take(sender, recipient)
}

/// The SPEC §7.6 rate limiter internals (token bucket).
mod rlimits {
    use std::collections::HashMap;

    /// A per-(sender, recipient) token bucket: capacity `capacity` tokens,
    /// refilled `1` every `refill_secs`.
    pub struct Buckets {
        capacity: u64,
        refill_secs: u64,
        last: HashMap<(String, String), (u64, u64)>,
    }

    impl Buckets {
        pub fn new(capacity: u64, refill_secs: u64) -> Self {
            Self {
                capacity,
                refill_secs,
                last: HashMap::new(),
            }
        }

        /// `Ok(())` when a token was available (one token consumed);
        /// `Err(retry_after)` when the bucket is empty.
        pub fn try_take(&mut self, sender: &str, recipient: &str) -> Option<u64> {
            let key = (sender.to_string(), recipient.to_string());
            let now = crate::core::a2a::inbox::now_unix();
            let (mut tokens, mut since) =
                self.last.get(&key).copied().unwrap_or((self.capacity, now));
            // Refill one token per refill_secs, up to capacity.
            let elapsed = now.saturating_sub(since);
            let refills = elapsed / self.refill_secs;
            tokens = (tokens + refills).min(self.capacity);
            since = now.saturating_sub(elapsed % self.refill_secs);
            if tokens == 0 {
                // Next slot in refill_secs - (elapsed % refill_secs).
                let retry_after = self
                    .refill_secs
                    .saturating_sub(elapsed % self.refill_secs)
                    .max(1);
                self.last.insert(key, (tokens, since));
                return Some(retry_after);
            }
            self.last.insert(key, (tokens - 1, since));
            None
        }
    }
}

/// Outcome of a jobset credential verification: authorized (with the verified
/// agent id), or a distinct refusal (401/403).
enum AuthOutcome {
    /// Credential valid; `X-Agent-Id` is the verified sender.
    Authorized(String),
    /// Step 1/2 failed (missing/wrong credential, unknown/ended jobset) — 401.
    Invalid,
    /// Step 3 failed (recipient unknown or not a member) — 403.
    Forbidden,
}

/// Verifies the presented jobset credential per SPEC §7.4 against the store
/// derived from `registry` from the root key. `target` is the addressed
/// recipient for a recipient-naming request (`Some`); `None` for a list.
///
/// Returns `None` for the 401 class, `Some(Forbidden)` for the 403 class, and
/// `Some(Authorized(agent_id))` on success.
fn verify_jobset(
    registry: &crate::core::jobset::JobsetRegistry,
    headers: &HeaderMap,
    target: Option<&str>,
) -> Option<AuthOutcome> {
    use crate::core::jobset::{derive_jobset_secret, derive_member_proof};
    let (id, secret) = presented_jobset(headers)?;
    let agent_id = header_value(headers, AGENT_ID_HEADER)?;
    if agent_id.is_empty() {
        return Some(AuthOutcome::Invalid);
    }
    let proof = header_value(headers, AGENT_PROOF_HEADER)?;
    if proof.is_empty() {
        return Some(AuthOutcome::Invalid);
    }

    // Step 1: the jobset row exists, is not ended, and the presented secret
    // equals the derived secret at the row's current epoch. The persisted epoch
    // source is the SQLite store (C1); the registry's root key drives the
    // derivation. Fall back to the in-memory registry for the test/default path
    // (no root key configured) where `derive_jobset_secret` yields a random
    // per-process value and the registry entry holds it.
    let root_key = registry.root_key();
    let epoch = store_epoch(registry, &id);
    let local_secret = match (&root_key, epoch) {
        (Some(key), Some(epoch)) => Some(derive_jobset_secret(key, &id, epoch)),
        (Some(_key), None) => {
            // A root key is configured but the jobset has no row: an unknown
            // jobset — 401 (there is nothing to derive against).
            return Some(AuthOutcome::Invalid);
        }
        (None, _) => {
            // No root key: only the in-memory registry knows the secret.
            registry.get(agent_id.as_str()).map(|e| {
                if e.jobset_id == id {
                    e.secret().to_string()
                } else {
                    String::new()
                }
            })
        }
    };
    let Some(local_secret) = local_secret else {
        return Some(AuthOutcome::Invalid);
    };
    if !crypto_digest_eq(secret.as_bytes(), local_secret.as_bytes()) {
        return Some(AuthOutcome::Invalid);
    }

    // Step 2: the presented proof equals the derived member proof of
    // X-Agent-Id for this jobset at the row's epoch.
    match &root_key {
        Some(key) => {
            let Some(epoch) = epoch else {
                return Some(AuthOutcome::Invalid);
            };
            let derived = derive_member_proof(key, &id, epoch, &agent_id);
            if !crypto_digest_eq(proof.as_bytes(), derived.as_bytes()) {
                return Some(AuthOutcome::Invalid);
            }
        }
        None => {
            // No root key: proofs cannot be derived; the member proof is
            // carried in the credential file but the registry does not hold it
            // today. Fail closed (jobset-only layer without a root key cannot
            // verify a member proof).
            return Some(AuthOutcome::Invalid);
        }
    }

    // Step 3: a recipient-naming request additionally requires the recipient
    // to be a `jobset_member` row of the presented jobset.
    if let Some(target) = target {
        if !target_is_member(registry, &id, target) {
            return Some(AuthOutcome::Forbidden);
        }
    }
    Some(AuthOutcome::Authorized(agent_id))
}

/// Whether `target` is a member of `jobset_id`: a `jobset_member` row in the
/// SQLite store (C1) when reachable, else a live registry entry registered
/// under the same jobset. Shared by the layer's step 3 and the sender guard,
/// so the membership check is identical everywhere.
fn target_is_member(
    registry: &crate::core::jobset::JobsetRegistry,
    jobset_id: &str,
    target: &str,
) -> bool {
    store_is_member(registry, jobset_id, target).unwrap_or(false)
        || registry
            .get(target)
            .is_some_and(|e| e.jobset_id == jobset_id && !registry.is_ended(jobset_id))
}

/// The persisted epoch of `jobset_id`, from the SQLite store when one is open
/// for the registry's data dir (C1), else the registry's live entry epoch.
fn store_epoch(
    registry: &crate::core::jobset::JobsetRegistry,
    jobset_id: &str,
) -> Option<crate::core::jobset::Epoch> {
    if let Some(dir) = registry.data_dir() {
        if let Ok(store) = crate::state::jobsets::JobsetStore::open_path(&dir) {
            if let Ok(Some(row)) = store.get(jobset_id) {
                return Some(row.epoch);
            }
        }
    }
    // No store row: fall back to a live member's epoch (the registry's
    // authoritative value for the running process).
    registry
        .snapshot()
        .into_iter()
        .find(|(_, e)| e.jobset_id == jobset_id)
        .map(|(_, e)| e.epoch)
}

/// Whether `agent` is a `jobset_member` row of `jobset_id` in the SQLite
/// store (C1), if reachable.
fn store_is_member(
    registry: &crate::core::jobset::JobsetRegistry,
    jobset_id: &str,
    agent: &str,
) -> Option<bool> {
    let dir = registry.data_dir()?;
    let store = crate::state::jobsets::JobsetStore::open_path(&dir).ok()?;
    store.is_member(jobset_id, agent).ok()
}

/// Constant-time byte-string equality through SHA-256 digest comparison
/// (shared helper; length of digests is fixed).
fn crypto_digest_eq(a: &[u8], b: &[u8]) -> bool {
    crate::core::crypto::digest_eq(a, b)
}

/// The sender-verified `JobsetAuth` guard for a `/message:send` body: the
/// receiver keeps the hop receiver-authoritative (token issued with the target
/// jobset's secret) and charges the per-jobset budget, exactly like the
/// operator-path guard. `registry` drives the target's jobset/secret lookup.
///
/// `verified_jobset` is the jobset id the layer already VERIFIED on the request
/// headers (steps 1/2 above). The guard independently confirms that the
/// addressed recipient belongs to that same jobset (step 3), so the target
/// secret/epoch it signs the token with is the presented jobset's -- a member
/// of one jobset can never forward through a recipient of another.
fn guarded_send_body_jobset(
    text: &str,
    registry: &crate::core::jobset::JobsetRegistry,
    guard: &HopGuard,
    target: &str,
    verified_jobset: &str,
    verified_sender: &str,
) -> Result<String, LayerReject> {
    let request: a2a::SendMessageRequest =
        serde_json::from_str(text).map_err(|_| LayerReject::Unauthenticated)?;
    let message_id = request.message.message_id.trim();
    if message_id.is_empty() {
        return Err(LayerReject::Unauthenticated);
    }
    // The recipient must be a member of the VERIFIED jobset (its target
    // registration matches the presented one).
    if !target_is_member(registry, verified_jobset, target) {
        return Err(LayerReject::Unauthenticated);
    }
    // The secret and epoch the token must verify with: derived from the root
    // key at the jobset's current (persisted) epoch, else the registered
    // random secret.
    let (secret, epoch) = match registry.root_key() {
        Some(root_key) => {
            let epoch = store_epoch(registry, verified_jobset).unwrap_or(0);
            (
                crate::core::jobset::derive_jobset_secret(&root_key, verified_jobset, epoch),
                epoch,
            )
        }
        None => {
            let entry = registry.get(target).ok_or(LayerReject::Unauthenticated)?;
            (entry.secret().to_string(), entry.epoch)
        }
    };
    let _ = epoch;

    let now = crate::core::a2a::inbox::now_unix();
    let (verified, _info) = ingress::process_ingress_guarded(
        &request.message.parts,
        request.message.metadata.as_ref(),
        message_id,
        verified_jobset,
        secret.as_bytes(),
        false,
        now,
        guard,
    )?;

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
        // The verified sender replaces whatever the body claimed: the framing
        // shows the identity the proof bound to the jobset, never a claim.
        meta.insert(
            crate::core::a2a::client::META_SENDER.to_string(),
            json!(verified_sender),
        );
        meta.remove(ingress::META_HOP_TOKEN);
    }
    serde_json::to_string(&value).map_err(|_| LayerReject::Unauthenticated)
}

/// The HTTP 403 response for a recipient that is not a member of the presented
/// jobset (SPEC §7.4 step 3): `no_shared_jobset`.
fn no_shared_jobset_response() -> Response {
    let detail = a2a::errordetails::TypedDetail::error_info(
        "NO_SHARED_JOBSET",
        a2a::errordetails::PROTOCOL_DOMAIN,
        None,
    );
    let body = json!({
        "error": {
            "code": 403,
            "status": "PERMISSION_DENIED",
            "message": "recipient is not a member of the presented jobset",
            "details": [detail],
        }
    });
    (
        StatusCode::FORBIDDEN,
        [(header::CONTENT_TYPE, "application/problem+json")],
        Json(body),
    )
        .into_response()
}

/// Verifies the CREATOR proof of a `POST /a2a/jobsets/{id}:end` request
/// (SPEC §7.2 item 3 / §7.16): plus the jobset id in `X-Jobset-Id` must equal
/// the path's `{id}`, and the verified `X-Agent-Id` must equal the jobset's
/// `creator_agent_id`. Steps 1 and 2 of §7.4 are a prerequisite (the secret
/// and the member proof derive from the same root key and epoch).
///
/// `id` is the path's `{id}` (already suffix-stripped). Returns `None` for
/// the 401 class and `Some(AuthOutcome)` for 403 / success.
fn verify_jobset_creator(
    registry: &crate::core::jobset::JobsetRegistry,
    headers: &HeaderMap,
    id: &str,
) -> Option<AuthOutcome> {
    // Steps 1 + 2 first (secret + member proof); the presented jobset must be
    // the path's, so an agent cannot end a jobset it is not even a member of.
    let (presented_id, _) = presented_jobset(headers)?;
    if presented_id != id {
        return Some(AuthOutcome::Invalid);
    }
    let agent_id = match verify_jobset(registry, headers, None) {
        Some(AuthOutcome::Authorized(agent_id)) => agent_id,
        Some(AuthOutcome::Forbidden) => return Some(AuthOutcome::Forbidden),
        None => return Some(AuthOutcome::Invalid),
        Some(AuthOutcome::Invalid) => return Some(AuthOutcome::Invalid),
    };
    // Step 3 (creator): the verified sender must be the jobset's creator.
    let creator = store_creator(registry, id)
        .unwrap_or_else(|| registry_creator(registry, id).unwrap_or_default());
    if !crate::core::crypto::ct_eq(agent_id.as_bytes(), creator.as_bytes()) {
        return Some(AuthOutcome::Forbidden);
    }
    Some(AuthOutcome::Authorized(agent_id))
}

/// The creator of `jobset_id` from the SQLite store (C1), if reachable.
fn store_creator(
    registry: &crate::core::jobset::JobsetRegistry,
    jobset_id: &str,
) -> Option<String> {
    let dir = registry.data_dir()?;
    let store = crate::state::jobsets::JobsetStore::open_path(&dir).ok()?;
    store
        .get(jobset_id)
        .ok()
        .flatten()
        .map(|j| j.creator_agent_id)
}

/// The creator of `jobset_id` from the in-memory registry's member entries
/// (used when the store is not reachable): every registered member knows its
/// jobset id, but the creator is not stored on the entry; the row's creator
/// equals its jobset id for the default jobset (SPEC §7.2 item 1), which is
/// the common case for the end route's per-pod scope.
fn registry_creator(
    _registry: &crate::core::jobset::JobsetRegistry,
    jobset_id: &str,
) -> Option<String> {
    Some(jobset_id.to_string())
}

/// The HTTP 429 response for an exhausted per-(sender, recipient) token bucket
/// (SPEC §7.6), with the `Retry-After` header the sender needs to pace its
/// retry. Same envelope/code as the per-jobset budget rejection.
fn rate_limited_response(retry_after: u64) -> Response {
    let detail = a2a::errordetails::TypedDetail::error_info(
        "rate_limited",
        a2a::errordetails::PROTOCOL_DOMAIN,
        None,
    );
    let body = json!({
        "error": {
            "code": 429,
            "status": "RATE_LIMITED",
            "message": "a2a message rate limit exceeded for this sender and recipient",
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
pub(crate) fn unauthenticated_response() -> Response {
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

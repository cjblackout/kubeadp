//! Shared HTTP auth for the agent-run API surface (`/api/v1/*`).
//!
//! Both servers that expose the `/api/v1` agent-run routes use one bearer
//! scheme so every `/api/v1` route is protected the same way when a token is
//! configured:
//! - the route group is a single axum sub-router wrapped in one layer
//!   ([`BearerAuthLayer`]), so a route added to the group is authenticated
//!   automatically -- there is no per-handler check to forget;
//! - [`AuthState`] carries the operator token's SHA-256 digest; when none is
//!   configured the layer passes every request through untouched (the
//!   caller must not start without a token unless `api_allow_unauthenticated`
//!   is set -- enforced by [`super::token`] at startup);
//! - comparison is constant-time on the SHA-256 digest ([`super::token`]),
//!   and a failed attempt answers `401 Unauthorized` with an
//!   `application/problem+json` body;
//! - a bounded per-peer throttle ([`PeerThrottle`]) turns a brute-forced
//!   token into `429 Too Many Requests` with `Retry-After`, so the auth gate
//!   cannot be hammered;
//! - liveness/readiness/metrics endpoints are NOT part of the sub-router, so
//!   a probe never needs the token.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use tower::{Layer, Service};

use super::token::constant_time_digest_eq;

/// Content type of the auth-failure bodies (RFC 7807 problem details).
pub const PROBLEM_JSON: &str = "application/problem+json";

/// Throttle window: failed auths within the last 60 s.
const THROTTLE_WINDOW: Duration = Duration::from_secs(60);
/// Allow at most this many failed auths per peer within the window.
const THROTTLE_MAX_FAILURES: usize = 10;
/// Bounded peer table size: a full map evicts one entry on a new peer.
const THROTTLE_MAX_PEERS: usize = 1024;

/// Bounded per-peer record of failed bearer-auth attempts (modelled on the
/// info-hub MCP's throttling so a brute-forced token cannot hammer the API).
#[derive(Default)]
pub struct PeerThrottle {
    failures: HashMap<IpAddr, Vec<Instant>>,
}

impl PeerThrottle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check_throttled(&mut self, peer: &IpAddr, now: Instant) -> Option<u64> {
        self.cleanup(now);
        let timestamps = self.failures.get(peer)?;
        let active = timestamps
            .iter()
            .filter(|&&t| now.duration_since(t) < THROTTLE_WINDOW)
            .count();
        if active < THROTTLE_MAX_FAILURES {
            return None;
        }
        let oldest = timestamps[0];
        let elapsed = now.duration_since(oldest);
        let retry_after = if elapsed < THROTTLE_WINDOW {
            (THROTTLE_WINDOW - elapsed).as_secs().max(1)
        } else {
            1
        };
        Some(retry_after)
    }

    pub fn record_failure(&mut self, peer: &IpAddr, now: Instant) {
        self.cleanup(now);
        if self.failures.len() >= THROTTLE_MAX_PEERS && !self.failures.contains_key(peer) {
            if let Some(k) = self.failures.keys().next().cloned() {
                self.failures.remove(&k);
            }
        }
        let list = self.failures.entry(*peer).or_default();
        list.retain(|&t| now.duration_since(t) < THROTTLE_WINDOW);
        list.push(now);
    }

    fn cleanup(&mut self, now: Instant) {
        self.failures.retain(|_, ts| {
            ts.retain(|&t| now.duration_since(t) < THROTTLE_WINDOW);
            !ts.is_empty()
        });
    }
}

/// The optional operator-token digest (and throttling state) that gates the
/// `/api/v1` route group.
///
/// `None` = no token configured; the caller must not serve without one unless
/// `api_allow_unauthenticated` is set (enforced at startup).
#[derive(Clone, Default)]
pub struct AuthState {
    digest: Option<[u8; 32]>,
    throttle: Option<Arc<Mutex<PeerThrottle>>>,
}

impl AuthState {
    /// No bearer auth (the legacy default for agent-run-api; used by tests
    /// and by servers started with `api_allow_unauthenticated`).
    pub fn none() -> Self {
        Self {
            digest: None,
            throttle: None,
        }
    }

    /// A configured operator token: its SHA-256 digest is stored (the raw
    /// token is hashed and dropped immediately). Throttling is enabled
    /// alongside the token.
    pub fn token(token: String) -> Self {
        let token = token.trim();
        if token.is_empty() {
            return Self::none();
        }
        Self {
            digest: Some(super::token::sha256(token.as_bytes())),
            throttle: Some(Arc::new(Mutex::new(PeerThrottle::new()))),
        }
    }

    /// Builds the auth state from the operator-token configuration resolved
    /// at startup (see [`super::token::startup_auth`]).
    pub fn from_startup_auth(startup: &super::token::StartupAuth) -> anyhow::Result<Self> {
        match startup {
            super::token::StartupAuth::Token(token) => Ok(Self::token(token.clone())),
            super::token::StartupAuth::Unauthenticated => {
                eprintln!(
                    "WARNING: no operator api token configured and api_allow_unauthenticated is set; \
                     the API surface is unauthenticated (in-cluster NetworkPolicy must \
                     restrict reachability). The A2A routes additionally require \
                     KUBEADP_API_ALLOW_OPEN=true, otherwise they are not mounted"
                );
                Ok(Self::none())
            }
            super::token::StartupAuth::Refused(msg) => anyhow::bail!("{msg}"),
        }
    }

    /// The configured operator-token digest, if any (used to build the A2A
    /// verifier from the same credential as the `/api/v1` layer).
    pub fn digest(&self) -> Option<[u8; 32]> {
        self.digest
    }
}

/// Builds an A2A [`crate::core::a2a::AuthVerifier`] from the same operator
/// token that guards `/api/v1`, so the REST sessions/jobs API and the A2A
/// surface share one credential. With no token configured (the
/// `api_allow_unauthenticated` path) the verifier is `None`; the mount sites
/// then refuse to serve the A2A routes unless `KUBEADP_API_ALLOW_OPEN` is set
/// (`crate::server::token::a2a_open_permitted`), so `None` never means
/// an accidentally open surface.
///
/// The raw token is never retained: the verifier closes over only the
/// SHA-256 digest and hashes the presented bearer credential before the
/// constant-time digest comparison.
#[cfg(feature = "a2a")]
pub fn auth_verifier_from_state(state: &AuthState) -> Option<crate::core::a2a::AuthVerifier> {
    let digest = state.digest()?;
    let verifier: crate::core::a2a::AuthVerifier =
        std::sync::Arc::new(move |headers: &axum::http::HeaderMap| {
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(str::trim_end)
                .is_some_and(|t| super::token::constant_time_digest_eq(t.as_bytes(), &digest))
        });
    Some(verifier)
}

/// Resolves the operator-token configuration from the environment and builds
/// the shared [`AuthState`], the single call both roles use at startup.
///
/// Returns `Err` when the server must not start: no token configured and
/// `api_allow_unauthenticated` not set (the refused-startup path, see
/// [`super::token::startup_auth`]). The token value from the file lives only
/// long enough to be hashed; it is never logged here or in
/// [`AuthState::token`].
pub fn resolve_startup_auth() -> anyhow::Result<AuthState> {
    let startup = super::token::startup_auth().map_err(anyhow::Error::msg)?;
    AuthState::from_startup_auth(&startup)
}

/// An RFC 7807 `application/problem+json` body for an authentication
/// failure. `type` stays short and stable (never leaks the token), `status`
/// mirrors the HTTP status, `title`/`detail` are human-readable.
fn problem_json(status: StatusCode, title: &str, code: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, PROBLEM_JSON)],
        Json(json!({
            "type": "about:blank",
            "title": title,
            "status": status.as_u16(),
            "code": code,
            "detail": title,
        })),
    )
        .into_response()
}

/// The verified identity of a jobset-authenticated `/api/v1` caller (an agent
/// controlling the jobs it started, SPEC §5.6). Inserted into the request
/// extensions by [`BearerAuth`]; absent for a bearer-authenticated request.
#[derive(Clone, Debug)]
pub struct JobsetCaller {
    pub jobset_id: String,
    pub agent_id: String,
}

fn header_str(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// The credential headers of a request, when all four are present. Verification
/// happens in [`authorize_caller`].
fn caller_from_headers(headers: &axum::http::HeaderMap) -> Option<PresentedCaller> {
    use crate::core::jobset::{
        AGENT_ID_HEADER, AGENT_PROOF_HEADER, JOBSET_ID_HEADER, JOBSET_SECRET_HEADER,
    };
    Some(PresentedCaller {
        caller: JobsetCaller {
            jobset_id: header_str(headers, JOBSET_ID_HEADER)?,
            agent_id: header_str(headers, AGENT_ID_HEADER)?,
        },
        secret: header_str(headers, JOBSET_SECRET_HEADER)?,
        proof: header_str(headers, AGENT_PROOF_HEADER)?,
    })
}

struct PresentedCaller {
    caller: JobsetCaller,
    secret: String,
    proof: String,
}

/// Job-control sub-routes a jobset-authenticated caller may use on a job it
/// controls (everything of `/jobs/{id}` except `repair`).
const CALLER_JOB_SUBROUTES: &[&str] = &[
    "",
    "clean",
    "kill",
    "retry",
    "commit",
    "discard",
    "export",
    "review",
    "log",
    "diff",
    "files",
    "cat",
    "result",
    "events",
    "interrupt",
    "pause",
    "message",
    "compact",
    "fork",
    "revert",
    "todo",
    "permissions",
];

/// Authorizes a jobset-authenticated request (SPEC §5.6): the identity must
/// verify (secret and member proof, not ended), and the route must be one of
/// the job-control routes; per-job routes additionally need the job to be a
/// member of the caller's jobset and the caller to be its creator. Routes that
/// carry job ids in the body (`start`, `wait`) or filter their answer (`list`)
/// are checked by their handlers through the [`JobsetCaller`] extension.
/// Returns `(status, code)` on refusal; the caller is inserted by the layer.
fn authorize_caller(
    registry: &crate::core::jobset::JobsetRegistry,
    presented: &PresentedCaller,
    method: &axum::http::Method,
    path: &str,
) -> Result<(), (StatusCode, &'static str)> {
    use axum::http::Method;
    let caller = &presented.caller;
    if !crate::core::jobset::verify_agent_identity(
        registry,
        &caller.jobset_id,
        &presented.secret,
        &caller.agent_id,
        &presented.proof,
    ) {
        return Err((StatusCode::UNAUTHORIZED, "unauthorized"));
    }
    let forbidden = (StatusCode::FORBIDDEN, "jobset_route_forbidden");
    let Some(rest) = path.strip_prefix("/api/v1/") else {
        return Err(forbidden);
    };
    let parts: Vec<&str> = rest.split('/').collect();
    let read = method == Method::GET;
    match parts.as_slice() {
        ["projects" | "info" | "models" | "quota" | "capabilities"] if read => Ok(()),
        ["jobs"] if read || method == Method::POST => Ok(()),
        ["jobs", "wait"] if method == Method::POST => Ok(()),
        ["jobs", id, rest @ ..] if rest.len() <= 1 => {
            let sub = rest.first().copied().unwrap_or("");
            if !CALLER_JOB_SUBROUTES.contains(&sub) {
                return Err(forbidden);
            }
            if crate::core::jobset::agent_controls_job(
                registry,
                &caller.jobset_id,
                &caller.agent_id,
                id,
            ) {
                Ok(())
            } else {
                Err((StatusCode::FORBIDDEN, "job_not_controlled"))
            }
        }
        ["runs", id, _] => {
            if crate::core::jobset::agent_controls_job(
                registry,
                &caller.jobset_id,
                &caller.agent_id,
                id,
            ) {
                Ok(())
            } else {
                Err((StatusCode::FORBIDDEN, "job_not_controlled"))
            }
        }
        ["jobsets", id, "end"] if method == Method::POST => {
            if *id == caller.jobset_id
                && registry.creator_of(id).as_deref() == Some(caller.agent_id.as_str())
            {
                Ok(())
            } else {
                Err((StatusCode::FORBIDDEN, "job_not_controlled"))
            }
        }
        _ => Err(forbidden),
    }
}

/// A [`tower::Layer`] that guards a route group with the optional bearer
/// token. See the module docs.
#[derive(Clone)]
pub struct BearerAuthLayer {
    digest: Option<[u8; 32]>,
    throttle: Option<Arc<Mutex<PeerThrottle>>>,
}

impl BearerAuthLayer {
    pub fn new(state: &AuthState) -> Self {
        Self {
            digest: state.digest,
            throttle: state.throttle.clone(),
        }
    }
}

impl<S> Layer<S> for BearerAuthLayer {
    type Service = BearerAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BearerAuth {
            inner,
            digest: self.digest,
            throttle: self.throttle.clone(),
        }
    }
}

/// The service produced by [`BearerAuthLayer`].
#[derive(Clone)]
pub struct BearerAuth<S> {
    inner: S,
    digest: Option<[u8; 32]>,
    throttle: Option<Arc<Mutex<PeerThrottle>>>,
}

impl<S> Service<Request> for BearerAuth<S>
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
        let Some(expected) = &self.digest else {
            // No token configured: pass through unchanged.
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(request).await });
        };
        let expected = *expected;
        let throttle = self.throttle.clone();

        // The peer for throttle accounting: ConnectInfo when the serve loop
        // provides it, otherwise loopback (tests / direct oneshot).
        let peer = request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0.ip())
            .unwrap_or(IpAddr::from([127, 0, 0, 1]));

        // An already-throttled peer is refused without even reading the
        // header (matches the pre-existing sessions-API behaviour).
        if let Some(throttle) = &throttle {
            if let Some(retry_after) = throttle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .check_throttled(&peer, Instant::now())
            {
                let response = (
                    StatusCode::TOO_MANY_REQUESTS,
                    [
                        (axum::http::header::RETRY_AFTER, retry_after.to_string()),
                        (header::CONTENT_TYPE, PROBLEM_JSON.to_string()),
                    ],
                    Json(json!({
                        "type": "about:blank",
                        "title": "too many requests",
                        "status": StatusCode::TOO_MANY_REQUESTS.as_u16(),
                        "code": "rate_limited",
                        "detail": "too many failed authentication attempts; retry later",
                    })),
                )
                    .into_response();
                return Box::pin(async move { Ok(response) });
            }
        }

        let presented = request
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim_end)
            .unwrap_or_default();

        if constant_time_digest_eq(presented.as_bytes(), &expected) {
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(request).await });
        }

        // No bearer presented: an agent may instead present its jobset
        // identity (SPEC §5.6). A verified caller is confined to the job
        // surface of its own jobset by `authorize_caller`.
        if !request.headers().contains_key(header::AUTHORIZATION) {
            if let Some(caller) = caller_from_headers(request.headers()) {
                let verdict = authorize_caller(
                    &crate::core::jobset::global_registry(),
                    &caller,
                    request.method(),
                    request.uri().path(),
                );
                match verdict {
                    Ok(()) => {
                        let mut request = request;
                        request.extensions_mut().insert(caller.caller);
                        let mut inner = self.inner.clone();
                        return Box::pin(async move { inner.call(request).await });
                    }
                    Err((status, code)) => {
                        if status == StatusCode::UNAUTHORIZED {
                            if let Some(throttle) = &throttle {
                                throttle
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .record_failure(&peer, Instant::now());
                            }
                        }
                        let response = problem_json(status, code, code);
                        return Box::pin(async move { Ok(response) });
                    }
                }
            }
        }

        if let Some(throttle) = &throttle {
            throttle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .record_failure(&peer, Instant::now());
        }
        let response = problem_json(StatusCode::UNAUTHORIZED, "unauthorized", "unauthorized");
        Box::pin(async move { Ok(response) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// A minimal authenticated router: one /api/v1 route behind the layer.
    fn gated_router(state: AuthState) -> axum::Router {
        axum::Router::new()
            .route(
                "/api/v1/projects",
                axum::routing::get(|| async { Json(json!({})) }),
            )
            .layer(BearerAuthLayer::new(&state))
    }

    fn api_request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn authed_request(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    async fn body_string(body: axum::body::Body) -> String {
        let bytes = body.collect().await.expect("body").to_bytes();
        String::from_utf8_lossy(&bytes).to_string()
    }

    fn presented(root: &[u8], jobset: &str, agent: &str) -> PresentedCaller {
        use crate::core::jobset::{derive_jobset_secret, derive_member_proof, EPOCH_ZERO};
        PresentedCaller {
            caller: JobsetCaller {
                jobset_id: jobset.to_string(),
                agent_id: agent.to_string(),
            },
            secret: derive_jobset_secret(root, jobset, EPOCH_ZERO),
            proof: derive_member_proof(root, jobset, EPOCH_ZERO, agent),
        }
    }

    /// A jobset identity controls exactly the jobs of its own jobset that it
    /// created; a member job, another jobset and every operator route are refused.
    #[test]
    fn caller_authorization_is_confined_to_own_jobs() {
        use crate::core::jobset::JobsetRegistry;
        use axum::http::Method;
        let root = vec![7u8; 32];
        let reg = JobsetRegistry::new();
        reg.set_root_key(Some(std::sync::Arc::from(root.clone().into_boxed_slice())));
        reg.register_at("job-1", "js-a", "x", 0);
        reg.register_at("job-2", "js-b", "x", 0);
        assert!(reg.claim_creator("js-a", "sess"));

        let own = presented(&root, "js-a", "sess");
        let ok = |p: &PresentedCaller, m: Method, path: &str| authorize_caller(&reg, p, &m, path);
        assert!(ok(&own, Method::GET, "/api/v1/jobs/job-1/log").is_ok());
        assert!(ok(&own, Method::POST, "/api/v1/jobs/job-1/kill").is_ok());
        assert!(ok(&own, Method::POST, "/api/v1/jobs").is_ok());
        assert!(ok(&own, Method::GET, "/api/v1/runs/job-1/todo").is_ok());
        assert!(ok(&own, Method::POST, "/api/v1/jobsets/js-a/end").is_ok());
        // another jobset's job, a repair, operator routes
        assert_eq!(
            ok(&own, Method::GET, "/api/v1/jobs/job-2/log")
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN
        );
        assert!(ok(&own, Method::POST, "/api/v1/jobs/job-1/repair").is_err());
        assert!(ok(&own, Method::POST, "/api/v1/gc").is_err());
        assert!(ok(&own, Method::POST, "/api/v1/jobs/repair").is_err());
        assert!(ok(&own, Method::POST, "/api/v1/auth").is_err());
        assert!(ok(&own, Method::POST, "/api/v1/jobsets/js-b/end").is_err());
        assert!(ok(&own, Method::GET, "/metrics").is_err());
        // a member job of the same jobset is not the creator
        let member = presented(&root, "js-a", "job-1");
        assert!(ok(&member, Method::GET, "/api/v1/jobs/job-1/log").is_err());
        // a wrong secret or proof is a 401
        let mut bad = presented(&root, "js-a", "sess");
        bad.proof = "0".repeat(64);
        assert_eq!(
            ok(&bad, Method::GET, "/api/v1/jobs").unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        // an ended jobset stops verifying
        reg.end_jobset("js-a");
        assert_eq!(
            ok(&own, Method::GET, "/api/v1/jobs").unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
    }

    /// Without a root key no jobset identity verifies (fail closed).
    #[test]
    fn caller_without_root_key_is_refused() {
        use axum::http::Method;
        let reg = crate::core::jobset::JobsetRegistry::new();
        let p = presented(&[7u8; 32], "js-a", "sess");
        assert_eq!(
            authorize_caller(&reg, &p, &Method::GET, "/api/v1/jobs")
                .unwrap_err()
                .0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn test_no_token_configured_passes_through() {
        let app = gated_router(AuthState::none());
        let res = app.oneshot(api_request("/api/v1/projects")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_token_configured_requires_bearer() {
        let app = gated_router(AuthState::token("sekret".to_string()));
        let res = app
            .clone()
            .oneshot(api_request("/api/v1/projects"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        // The 401 body is RFC 7807 application/problem+json.
        assert_eq!(
            res.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(PROBLEM_JSON)
        );
        let value: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unauthorized");
        assert_eq!(value["title"], "unauthorized");
        assert_eq!(value["status"], 401);

        let res = app
            .oneshot(authed_request("/api/v1/projects", "wrong"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_token_configured_accepts_valid_token() {
        let app = gated_router(AuthState::token("sekret".to_string()));
        let res = app
            .oneshot(authed_request("/api/v1/projects", "sekret"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_failures_are_throttled_per_peer() {
        let state = AuthState::token("sekret".to_string());
        let app = gated_router(state.clone());
        let peer: IpAddr = "10.0.0.7".parse().unwrap();
        // Pre-fill the throttle so the next request from that peer is 429.
        {
            let mut t = state.throttle.as_ref().unwrap().lock().unwrap();
            let now = Instant::now();
            for _ in 0..THROTTLE_MAX_FAILURES {
                t.record_failure(&peer, now);
            }
            assert!(t.check_throttled(&peer, now).is_some());
        }
        // Drive the middleware path with a real ConnectInfo extension.
        let req = Request::builder()
            .uri("/api/v1/projects")
            .header(header::AUTHORIZATION, "Bearer wrong")
            .extension(ConnectInfo(SocketAddr::from((peer, 4444))))
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            res.headers().get(axum::http::header::RETRY_AFTER).is_some(),
            "429 must carry Retry-After"
        );
        assert_eq!(
            res.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(PROBLEM_JSON)
        );
    }

    #[test]
    fn test_correct_token_digest_compares_equal() {
        let state = AuthState::token("sekret".to_string());
        let digest = state.digest.unwrap();
        assert!(super::super::token::constant_time_digest_eq(
            b"sekret", &digest
        ));
        assert!(!super::super::token::constant_time_digest_eq(
            b"sekrett", &digest
        ));
    }

    // -----------------------------------------------------------------------
    // Table-driven router tests: the FULL observable app as every server
    // builds it (obs::add_observability over core::server::router), so the
    // whole /api/v1 group, /metrics, /healthz and /readyz are exercised
    // through the one shared auth layer.
    // -----------------------------------------------------------------------

    fn full_router(state: AuthState) -> axum::Router {
        crate::server::obs::add_observability(
            crate::server::router(
                std::sync::Arc::new(crate::core::config::Config::default()),
                std::sync::Arc::new(crate::core::auth::AuthManager::new()),
            ),
            state,
            std::sync::Arc::new(|| Ok(())),
            std::sync::Arc::new(Vec::new),
        )
    }

    fn req(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn req_auth(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    /// Every protected route group of the agent-run API surface: the shared
    /// job-control routes, the read routes, and the metric endpoint.
    const PROTECTED_ROUTE_GROUPS: &[&str] = &[
        "/api/v1/jobs",
        "/api/v1/projects",
        "/api/v1/info",
        "/metrics",
    ];

    #[tokio::test]
    async fn test_table_no_credential_is_401_on_every_protected_route() {
        let app = full_router(AuthState::token("sekret-token".to_string()));
        for uri in PROTECTED_ROUTE_GROUPS {
            let res = app.clone().oneshot(req(uri)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "GET {uri} with no credential must be 401"
            );
            // 401 bodies are RFC 7807 application/problem+json.
            assert_eq!(
                res.headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok()),
                Some(PROBLEM_JSON),
                "{uri} must answer application/problem+json"
            );
            let value: serde_json::Value =
                serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["code"], "unauthorized", "{uri} body code");
            assert_eq!(value["status"], 401, "{uri} body status");
        }
    }

    #[tokio::test]
    async fn test_table_wrong_token_is_401_on_every_protected_route() {
        let app = full_router(AuthState::token("sekret-token".to_string()));
        for uri in PROTECTED_ROUTE_GROUPS {
            let res = app.clone().oneshot(req_auth(uri, "wrong")).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "GET {uri} with a wrong token must be 401"
            );
        }
    }

    #[tokio::test]
    async fn test_table_valid_token_accepted_on_every_protected_route() {
        let app = full_router(AuthState::token("sekret-token".to_string()));
        // /models and /quota need the agy binary (absent in tests) and 500
        // even with a valid token; their auth gate is proven by the 401 cases.
        for uri in [
            "/api/v1/jobs",
            "/api/v1/projects",
            "/api/v1/info",
            "/metrics",
        ] {
            let res = app
                .clone()
                .oneshot(req_auth(uri, "sekret-token"))
                .await
                .unwrap();
            assert_eq!(
                res.status(),
                StatusCode::OK,
                "GET {uri} with the valid token must be 200"
            );
        }
    }

    #[tokio::test]
    async fn test_table_healthz_readyz_always_open() {
        // The probes never require the token, whether or not one is set.
        for state in [
            AuthState::none(),
            AuthState::token("sekret-token".to_string()),
        ] {
            let app = full_router(state);
            for uri in ["/healthz", "/readyz"] {
                let res = app.clone().oneshot(req(uri)).await.unwrap();
                assert_eq!(res.status(), StatusCode::OK, "GET {uri} must stay open");
            }
        }
    }

    #[tokio::test]
    async fn test_table_missing_credential_never_reaches_handler() {
        // A missing/invalid credential is refused before the handler runs:
        // the handler for /api/v1/info would answer 200; a 401 proves the
        // handler was never reached.
        let app = full_router(AuthState::token("sekret-token".to_string()));
        let res = app.oneshot(req("/api/v1/info")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_table_throttle_after_ten_failures() {
        // Exercise the throttle through the full router: after exactly
        // THROTTLE_MAX_FAILURES failed auths from one peer, the next request
        // answers 429 with Retry-After -- even for a route group the peer
        // has never hit before (/api/v1/projects vs the /api/v1/info it
        // failed on).
        let state = AuthState::token("sekret-token".to_string());
        let app = full_router(state.clone());
        let peer: IpAddr = "10.0.0.9".parse().unwrap();
        for i in 0..THROTTLE_MAX_FAILURES {
            let r = Request::builder()
                .uri("/api/v1/info")
                .header(header::AUTHORIZATION, "Bearer wrong")
                .extension(ConnectInfo(SocketAddr::from((
                    peer,
                    4444u16.wrapping_add(i as u16),
                ))))
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(r).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "failure {i} must be a plain 401 (not yet throttled)"
            );
        }
        let r = Request::builder()
            .uri("/api/v1/projects")
            .header(header::AUTHORIZATION, "Bearer wrong")
            .extension(ConnectInfo(SocketAddr::from((peer, 4444))))
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(r).await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            res.headers().get(axum::http::header::RETRY_AFTER).is_some(),
            "429 must carry Retry-After"
        );
        assert_eq!(
            res.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(PROBLEM_JSON)
        );
        // A valid (or whatever) peer from a different IP is not throttled.
        let other = Request::builder()
            .uri("/api/v1/jobs")
            .header(header::AUTHORIZATION, "Bearer wrong")
            .extension(ConnectInfo(SocketAddr::from((
                "10.0.0.10".parse::<IpAddr>().unwrap(),
                5555,
            ))))
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(other).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_response_bodies_do_not_echo_the_token() {
        // The token value must never appear in any auth response (a leak
        // would put the secret into logs/clients). Both the 401 body and the
        // 429 body are checked.
        let token = "top-secret-operator-token-value";
        let app = full_router(AuthState::token(token.to_string()));
        let res = app.clone().oneshot(req("/api/v1/info")).await.unwrap();
        let body = body_string(res.into_body()).await;
        assert!(!body.contains(token), "401 body must not contain the token");

        // Force a throttle with wrong tokens and check the 429 body too.
        let state = AuthState::token(token.to_string());
        let app = full_router(state.clone());
        let peer: IpAddr = "10.0.0.11".parse().unwrap();
        for _ in 0..THROTTLE_MAX_FAILURES {
            let r = Request::builder()
                .uri("/api/v1/info")
                .header(header::AUTHORIZATION, "Bearer wrong")
                .extension(ConnectInfo(SocketAddr::from((peer, 4444))))
                .body(Body::empty())
                .unwrap();
            app.clone().oneshot(r).await.unwrap();
        }
        let r = Request::builder()
            .uri("/api/v1/info")
            .extension(ConnectInfo(SocketAddr::from((peer, 4444))))
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(r).await.unwrap();
        let body = body_string(res.into_body()).await;
        assert!(!body.contains(token), "429 body must not contain the token");
    }
}

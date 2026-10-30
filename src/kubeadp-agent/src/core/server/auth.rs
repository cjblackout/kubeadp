//! Shared HTTP auth for the agent-run API surface (`/api/v1/*`).
//!
//! Both servers that expose the `/api/v1` agent-run routes use one bearer
//! scheme so every `/api/v1` route is protected the same way when a token is
//! configured:
//! - the route group is a single axum sub-router wrapped in one layer
//!   ([`BearerAuthLayer`]), so a route added to the group is authenticated
//!   automatically -- there is no per-handler check to forget;
//! - [`AuthState`] carries an optional token; when none is configured the
//!   layer passes every request through untouched (agent-run-api's
//!   network-policy-only model, exactly the no-token behaviour it had before
//!   the token existed);
//! - comparison is constant-time and a failed attempt answers the standard
//!   JSON error shape (`{"error","code"}`) with `401 Unauthorized`;
//! - a bounded per-peer throttle ([`PeerThrottle`]) turns a brute-forced
//!   token into `429 Too Many Requests` with `Retry-After`, so the auth gate
//!   cannot be hammered;
//! - liveness/readiness endpoints are NOT part of the sub-router, so a probe
//!   never needs the token.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use tower::{Layer, Service};

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

/// The optional bearer token (and throttling state) that gates the
/// `/api/v1` route group.
///
/// `None` = no token configured: the routes are reachable without
/// credentials (the operator's network policy is the access control).
#[derive(Clone, Default)]
pub struct AuthState {
    token: Option<Arc<str>>,
    throttle: Option<Arc<Mutex<PeerThrottle>>>,
}

impl AuthState {
    /// No bearer auth (the default for agent-run-api).
    pub fn none() -> Self {
        Self {
            token: None,
            throttle: None,
        }
    }

    /// A configured bearer token. The value is trimmed; an empty string is
    /// treated as "no token" so a blank environment variable cannot lock
    /// every route behind an unusable comparison. Throttling is enabled
    /// alongside the token.
    pub fn token(token: String) -> Self {
        let token = token.trim().to_string();
        Self {
            token: (!token.is_empty()).then(|| Arc::from(token)),
            throttle: Some(Arc::new(Mutex::new(PeerThrottle::new()))),
        }
    }
}

/// Constant-time comparison of two byte strings (a length mismatch short
/// circuits; equal-length inputs are XORed byte by byte so timing does not
/// reveal the position of the first difference).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A [`tower::Layer`] that guards a route group with the optional bearer
/// token. See the module docs.
#[derive(Clone)]
pub struct BearerAuthLayer {
    token: Option<Arc<str>>,
    throttle: Option<Arc<Mutex<PeerThrottle>>>,
}

impl BearerAuthLayer {
    pub fn new(state: &AuthState) -> Self {
        Self {
            token: state.token.clone(),
            throttle: state.throttle.clone(),
        }
    }
}

impl<S> Layer<S> for BearerAuthLayer {
    type Service = BearerAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BearerAuth {
            inner,
            token: self.token.clone(),
            throttle: self.throttle.clone(),
        }
    }
}

/// The service produced by [`BearerAuthLayer`].
#[derive(Clone)]
pub struct BearerAuth<S> {
    inner: S,
    token: Option<Arc<str>>,
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
        let Some(expected) = &self.token else {
            // No token configured: pass through unchanged (in-cluster
            // NetworkPolicy is the access control, matching agent-run-api).
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(request).await });
        };
        let expected = expected.clone();
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
                    [(axum::http::header::RETRY_AFTER, retry_after.to_string())],
                    Json(json!({ "error": "too many requests", "code": "rate_limited" })),
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

        if constant_time_eq(presented.as_bytes(), expected.as_bytes()) {
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(request).await });
        }

        if let Some(throttle) = &throttle {
            throttle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .record_failure(&peer, Instant::now());
        }
        let response = (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized", "code": "unauthorized" })),
        )
            .into_response();
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
        let value: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unauthorized");
        assert_eq!(value["error"], "unauthorized");

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
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"a-b-c", b"a-b-c"));
        assert!(!constant_time_eq(b"a-b-c", b"a-b-d"));
        assert!(!constant_time_eq(b"short", b"longer"));
    }
}

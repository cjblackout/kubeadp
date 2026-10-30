//! Shared observability endpoints for the agent-run HTTP servers:
//!
//! - `GET /readyz` -- unauthenticated (like `/healthz`); answers
//!   `{"status":"ready"}` when the server is ready to serve, otherwise
//!   `503` with `{"error","code":"not_ready"}`. "Ready" is a probe closure
//!   the server supplies (state store / sessions registry usable), so the
//!   shared module knows nothing about either server's internals.
//! - `GET /metrics` -- Prometheus text format, guarded by the same bearer
//!   auth as `/api/v1` when a token is configured. The metric set is small
//!   and fixed: process uptime, a per-status gauge of the server's primary
//!   workload (jobs by status for agent-run-api, sessions by state for
//!   agent-orchestrate; supplied by the caller as a closure), and a cheap
//!   counter of completed responses by status class for the whole router.
//!
//! [`add_observability`] wraps an authenticated router (everything a
//! [`crate::core::server::router`] carries, i.e. every `/api/v1` route) with the
//! bearer-auth layer and appends the unauthenticated `public` routes last.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde_json::json;

/// Readiness probe for `/readyz`: `Ok(())` means ready, `Err(reason)` is
/// returned to the operator via the 503 body and a server-side log line.
pub type Readiness = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// Extra state-gauge metrics for `/metrics`: a list of `(label, value)`
/// pairs (already grouped; a caller returns one entry per status/distinct
/// label). Returned verbatim as `agent_state{status="<label>"}` lines, so
/// agent-run-api can publish jobs by status and agent-orchestrate sessions
/// by state without the shared module knowing either vocabulary.
pub type StateMetrics = Arc<dyn Fn() -> Vec<(String, u64)> + Send + Sync>;

/// A constant-time guarantee set (no-op) for the metrics body's ordering.
#[derive(Default)]
struct StatusClasses {
    ok: u64,
    redir: u64,
    client_err: u64,
    server_err: u64,
}

impl StatusClasses {
    fn record(&mut self, code: u16) {
        match code {
            200..=299 => self.ok += 1,
            300..=399 => self.redir += 1,
            400..=499 => self.client_err += 1,
            _ => self.server_err += 1,
        }
    }

    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (self.ok, self.redir, self.client_err, self.server_err)
    }
}

/// Shared per-process counters. Injected as an axum extension so the
/// counting middleware and the `/metrics` handler read one source of truth.
struct ServerMetrics {
    started: Instant,
    status_classes: std::sync::Mutex<StatusClasses>,
}

impl ServerMetrics {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            status_classes: std::sync::Mutex::new(StatusClasses::default()),
        }
    }

    fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn record(&self, code: u16) {
        self.status_classes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record(code);
    }
}

/// Wraps `api_router` (the authenticated `/api/v1` routes, its state already
/// applied) with the bearer-auth layer, adds `GET /metrics` into the
/// authenticated group, appends the unauthenticated `GET /healthz` and
/// `GET /readyz`, and counts every completed response by status class.
///
/// - `auth` is the shared [`super::auth::AuthState`]; when no token is
///   configured (agent-run-api) the layer passes everything through and the
///   responses are byte-identical to the pre-auth router.
/// - `ready` and `state_metrics` are caller-supplied (see the type aliases),
///   so the shared module stays independent of both servers' state shapes.
pub fn add_observability(
    api_router: Router,
    auth: super::auth::AuthState,
    ready: Readiness,
    state_metrics: StateMetrics,
) -> Router {
    let metrics = Arc::new(ServerMetrics::new());

    let api = api_router.route("/metrics", get(metrics_handler));
    // Every `/api/v1` route (and `/metrics`) lives behind this one layer: a
    // route added to the sub-router is authenticated automatically.
    let gated = api.layer(super::auth::BearerAuthLayer::new(&auth));

    // Public, never-auth routes: merged last, on their own router, so their
    // paths are served without the bearer layer.
    let public = Router::new()
        .route("/readyz", get(readyz_handler))
        .route("/healthz", get(healthz_handler));

    let merged = gated.merge(public);

    let counted = merged.layer(axum::middleware::from_fn({
        let metrics = Arc::clone(&metrics);
        move |request: Request, next: Next| {
            let metrics = Arc::clone(&metrics);
            async move {
                let response = next.run(request).await;
                metrics.record(response.status().as_u16());
                response
            }
        }
    }));

    counted
        .layer(Extension(metrics))
        .layer(Extension(ready))
        .layer(Extension(state_metrics))
}

/// `GET /readyz` -- liveness/compatibility probe (never-auth).
async fn readyz_handler(Extension(ready): Extension<Readiness>) -> Response {
    match ready() {
        Ok(()) => Json(json!({ "status": "ready" })).into_response(),
        Err(reason) => {
            eprintln!("[agent-run] readyz: not ready ({reason})");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "not ready", "code": "not_ready" })),
            )
                .into_response()
        }
    }
}

/// `GET /healthz` -- liveness probe (never-auth).
async fn healthz_handler() -> Response {
    Json(json!({ "status": "ok" })).into_response()
}

/// `GET /metrics` -- Prometheus text format (auth-gated like `/api/v1`).
async fn metrics_handler(
    Extension(metrics): Extension<Arc<ServerMetrics>>,
    Extension(state_metrics): Extension<StateMetrics>,
) -> Response {
    let (ok, redir, client_err, server_err) = {
        let guard = metrics
            .status_classes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.snapshot()
    };

    // Group the caller's gauges deterministically (status labels sort
    // lexically) so repeated scrapes render in a stable order.
    let mut states: BTreeMap<String, u64> = BTreeMap::new();
    for (label, value) in state_metrics() {
        *states.entry(label).or_default() += value;
    }

    let mut out = String::new();
    out.push_str("# TYPE agent_process_uptime_seconds gauge\n");
    out.push_str("# HELP agent_process_uptime_seconds Process uptime in seconds.\n");
    out.push_str(&format!(
        "agent_process_uptime_seconds {}\n",
        metrics.uptime_secs()
    ));
    out.push_str("# TYPE agent_state gauge\n");
    out.push_str("# HELP agent_state Workload count by status (jobs for agent-run-api, sessions for agent-orchestrate).\n");
    for (label, value) in states {
        out.push_str(&format!("agent_state{{status=\"{}\"}} {}\n", label, value));
    }
    out.push_str("# TYPE agent_requests_total counter\n");
    out.push_str("# HELP agent_requests_total HTTP responses by status class.\n");
    out.push_str(&format!("agent_requests_total{{class=\"2xx\"}} {ok}\n"));
    out.push_str(&format!("agent_requests_total{{class=\"3xx\"}} {redir}\n"));
    out.push_str(&format!(
        "agent_requests_total{{class=\"4xx\"}} {client_err}\n"
    ));
    out.push_str(&format!(
        "agent_requests_total{{class=\"5xx\"}} {server_err}\n"
    ));

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
        .into_response()
}

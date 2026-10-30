//! A2A v1 agent protocol surface (agent card + JSON-RPC and HTTP+JSON bindings).
//!
//! This module (behind the crate's `a2a` cargo feature, which itself depends
//! on the `server` feature) provides the server side of the A2A v1 protocol
//! for the agent wrappers. It is *not* mounted by this crate: the
//! `agent-orchestrate` and `agent-run-api` crates re-export the router and
//! mount it in-process (see the A2A plan, Vikunja HL-43/HL-56).
//!
//! # SDK decision
//!
//! Types come from the official [`a2a-lf`](https://docs.rs/a2a-lf) crate
//! (`a2aproject/a2a-rs`, Apache-2.0), pinned exactly in `Cargo.toml`. The
//! `a2a-server-lf` crate is *not* used: its routers build their own state and
//! its REST error envelope (`{"error": {"status": ..., "details": [...]}}`)
//! differs from the crate-wide `{"error": "...", "code": "..."}` JSON error
//! shape, and it maps "unsupported operation" to HTTP 400 rather than the
//! required 501 `code: "unsupported"`. Instead the thin JSON-RPC and
//! HTTP+JSON bindings are implemented here directly over the `a2a-lf` types,
//! keeping the wire format identical to the official SDK.
//!
//! # Surface
//!
//! - `GET /.well-known/agent-card.json` -- the public agent card (no auth).
//! - JSON-RPC (`POST /rpc`) and REST bindings for `GetTask`, `ListTasks`,
//!   `CancelTask` over the [`TaskBackend`] trait.
//! - `SendMessage`, `SendStreamingMessage`, `SubscribeToTask`,
//!   push-notification config and `GetExtendedAgentCard` answer HTTP 501 /
//!   JSON-RPC `UNSUPPORTED_OPERATION` (`code: "unsupported"`), never 404.
//!
//! All non-card routes require a bearer token through the caller-supplied
//! verifier ([`AuthVerifier`]); the card is public.

mod backend;
mod bindings;
mod card;

pub use backend::{map_job_status, A2AError, A2aTask, TaskBackend};
pub use card::{A2aModel, WELL_KNOWN_AGENT_CARD_PATH};

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

/// Caller-supplied request authorizer for the non-card routes.
///
/// Returns `true` when the request's headers carry an accepted bearer token.
/// The wrapper serving the router builds this from its configured token
/// (constant-time compare; see the helper in this module). A `None` value
/// disables the check entirely -- the wrapper then relies on network policy
/// to keep the endpoint unreachable, matching the agent-run-api /
/// agent-orchestrate "no token configured" model.
pub type AuthVerifier = Arc<dyn Fn(&HeaderMap) -> bool + Send + Sync>;

/// State shared by every handler of the A2A router.
#[derive(Clone)]
struct A2aState {
    model: A2aModel,
    backend: Arc<dyn TaskBackend>,
    verifier: Option<AuthVerifier>,
}

/// Builds the A2A axum router.
///
/// The agent card route is public; every other route is wrapped in the
/// bearer-token check (see [`AuthVerifier`]). Nothing here is mounted by
/// this crate -- the caller merges the returned router into its own and is
/// responsible for the listening address.
pub fn a2a_router(
    model: A2aModel,
    backend: Arc<dyn TaskBackend>,
    auth: Option<AuthVerifier>,
) -> Router {
    let state = A2aState {
        model,
        backend,
        verifier: auth,
    };

    // JSON-RPC binding (official SDK wire format: POST one endpoint, route
    // by the `method` field of the JSON-RPC 2.0 envelope).
    let jsonrpc = Router::new().route(bindings::JSONRPC_PATH, post(bindings::jsonrpc_handler));

    // REST (HTTP+JSON) binding. Paths follow the official SDK / A2A spec,
    // including the `:cancel` / `:subscribe` action suffixes.
    let rest = Router::new()
        .route("/tasks", get(bindings::rest_list_tasks))
        .route(
            "/tasks/{id}",
            get(bindings::rest_get_task_or_subscribe).post(bindings::rest_task_action),
        )
        .route(
            "/tasks/{id}/cancel",
            post(bindings::rest_cancel_task_legacy),
        )
        .route(
            "/tasks/{id}/subscribe",
            get(bindings::rest_subscribe_unsupported).post(bindings::rest_subscribe_unsupported),
        )
        .route(
            "/message:send",
            post(bindings::rest_send_message_unsupported),
        )
        .route(
            "/message/send",
            post(bindings::rest_send_message_unsupported),
        )
        .route(
            "/message:stream",
            post(bindings::rest_stream_message_unsupported),
        )
        .route(
            "/message/stream",
            post(bindings::rest_stream_message_unsupported),
        )
        .route(
            "/extendedAgentCard",
            get(bindings::rest_extended_card_unsupported),
        )
        .route(
            "/agent-card/extended",
            get(bindings::rest_extended_card_unsupported),
        )
        .route(
            "/tasks/{id}/pushNotificationConfigs",
            post(bindings::rest_push_configs_unsupported)
                .get(bindings::rest_push_configs_unsupported),
        )
        .route(
            "/tasks/{id}/pushNotificationConfigs/{config_id}",
            get(bindings::rest_push_config_unsupported)
                .delete(bindings::rest_push_config_unsupported),
        )
        .route(
            "/tasks/{id}/push-configs",
            post(bindings::rest_push_configs_unsupported)
                .get(bindings::rest_push_configs_unsupported),
        )
        .route(
            "/tasks/{id}/push-configs/{config_id}",
            get(bindings::rest_push_config_unsupported)
                .delete(bindings::rest_push_config_unsupported),
        );

    let protected = jsonrpc
        .merge(rest)
        .layer(middleware::from_fn_with_state(state.clone(), bearer_auth));

    Router::new()
        .route(WELL_KNOWN_AGENT_CARD_PATH, get(card::agent_card))
        .merge(protected)
        .with_state(state)
}

/// Rejects requests whose headers the caller's verifier does not accept
/// (HTTP 401, crate `{"error","code"}` JSON shape). A `None` verifier lets
/// everything through -- the wrapper relies on network policy then.
async fn bearer_auth(State(state): State<A2aState>, request: Request, next: Next) -> Response {
    let Some(verifier) = &state.verifier else {
        return next.run(request).await;
    };
    if verifier(request.headers()) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "authentication required", "code": "unauthorized" })),
    )
        .into_response()
}

/// Constant-time comparison of two byte strings, used to validate a bearer
/// token without leaking the length-independent comparison timing. The
/// wrapper's verifier ([`AuthVerifier`]) is expected to use this helper.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

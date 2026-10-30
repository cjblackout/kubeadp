//! A2A v1 agent protocol surface (agent card + JSON-RPC and HTTP+JSON
//! bindings), served by the official `a2a-server-lf` SDK.
//!
//! This module (behind the crate's `a2a` cargo feature, which itself depends
//! on the `server` feature) provides the server side of the A2A v1 protocol
//! for the agent wrappers. It is mounted by the wrapper roles: the
//! orchestrator merges it into its HTTP server in
//! [`crate::orchestrate::http_server::build_router`], the worker in
//! [`crate::worker::entry`].
//!
//! # SDK decision
//!
//! Since the A2A plan (Vikunja HL-43/HL-56) was written, the official
//! [`a2a-server-lf`](https://docs.rs/a2a-server-lf) SDK
//! (`a2aproject/a2a-rs`, Apache-2.0) has matured past the "not a server
//! framework" caveat: its `jsonrpc_router`/`rest_router`/`agent_card_router`
//! are plain axum 0.8 routers that merge into ours, and its REST error
//! envelope is the A2A-spec shape (`{"error":{"code","status","message",
//! "details"}}`, `application/problem+json`). This module therefore replaces
//! the hand-written JSON-RPC/REST bindings with the SDK routers while keeping
//! the wire-compatible `a2a-lf` 0.4.2 type layer (same type-pinned version).
//!
//! The one deliberate SDK delta, documented here so it is not re-litigated:
//! the SDK maps `UNSUPPORTED_OPERATION` to HTTP 400 while the previous
//! hand-written bindings answered 501. The A2A REST spec (§9.6.2) does not
//! reserve 501 for unsupported operations; the JSON-RPC binding reports
//! unsupported methods as `UNSUPPORTED_OPERATION` (-32004) in the envelope
//! with HTTP 200 either way, so the wire-visible REST status for
//! `SendStreamingMessage`/`SubscribeToTask` changes from 501 to 400.
//!
//! # Surface
//!
//! - `GET /.well-known/agent-card.json` -- the public agent card (no auth,
//!   CORS for discovery) served by the SDK's `agent_card_router`.
//! - SDK `jsonrpc_router` (POST `/`) and `rest_router` (`/message:send`,
//!   `/tasks`, `/tasks/{id}`, `/tasks/{id}/cancel`, ...) over a
//!   [`ServerRequestHandler`] that implements `GetTask`, `ListTasks`,
//!   `CancelTask` over the [`TaskBackend`] trait and `SendMessage` over an
//!   optional [`MessageSink`].
//! - `SendStreamingMessage`, `SubscribeToTask`, push-notification config and
//!   `GetExtendedAgentCard` answer the SDK's dedicated errors.
//!
//! Authorization is enforced inside [`ServerRequestHandler`] from the
//! caller's [`AuthVerifier`] (each `RequestHandler` method receives the
//! request's `ServiceParams`); the agent card stays public.

mod backend;
mod card;
mod handler;

pub use backend::{map_job_status, A2AError, A2aTask, TaskBackend};
pub use card::{A2aModel, WELL_KNOWN_AGENT_CARD_PATH};
pub use handler::{MessageSink, ServerRequestHandler};

use std::sync::Arc;

use a2a_server::agent_card::agent_card_router;
use a2a_server::jsonrpc::jsonrpc_router;
use a2a_server::rest::rest_router;
use a2a_server::StaticAgentCard;
use axum::http::HeaderMap;
use axum::Router;

/// Caller-supplied request authorizer for the protected routes.
///
/// Returns `true` when the request's headers carry an accepted bearer token.
/// The wrapper serving the router builds this from its configured token
/// (constant-time compare; see [`constant_time_eq`]). A `None` value disables
/// the check entirely -- the wrapper then relies on network policy to keep
/// the endpoint unreachable, matching the agent-run-api / agent-orchestrate
/// "no token configured" model.
pub type AuthVerifier = Arc<dyn Fn(&HeaderMap) -> bool + Send + Sync>;

/// Builds the A2A axum router from the SDK's three sub-routers.
///
/// - the public agent card router (no auth)
/// - the SDK JSON-RPC router (POST `/`)
/// - the SDK REST router (`/message:send`, `/tasks`, ...)
///
/// The JSON-RPC and REST sub-routers share one [`ServerRequestHandler`]
/// carrying the optional bearer verifier and [`MessageSink`]; an absent
/// verifier lets everything through (network-policy-only access control).
/// The agent card stays public.
pub fn a2a_router(
    model: A2aModel,
    backend: Arc<dyn TaskBackend>,
    sink: Option<Arc<dyn MessageSink>>,
    auth: Option<AuthVerifier>,
) -> Router {
    let handler = Arc::new(ServerRequestHandler::new(backend, sink, auth));

    // The SDK's jsonrpc_router serves POST `/`; nest it under the crate's
    // historical JSON-RPC path so the wire contract (/rpc) is preserved.
    let jsonrpc = jsonrpc_router(handler.clone());
    let rest = rest_router(handler);

    let card_router = agent_card_router(Arc::new(StaticAgentCard::new(model.to_agent_card())));

    card_router.merge(axum::Router::new().nest("/rpc", jsonrpc).merge(rest))
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

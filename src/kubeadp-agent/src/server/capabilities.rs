//! `GET /api/v1/capabilities` (design HL-91 E1).
//!
//! The handler walks [`crate::engine::EngineRegistry::enabled_ids()`] and
//! returns each engine's effective (post-`EmulatingEngine`, E4) capability
//! table. It is built purely from the static [`crate::engine::capability::ENDPOINTS`]
//! table plus each engine's `capabilities()` report and never reaches a live
//! engine process.
//!
//! The route is registered in [`crate::server::router_with_rc`] (behind
//! the same bearer layer as every other `/api/v1` route), carrying the engine
//! registry as an axum `Extension<Arc<EngineRegistry>>`. `router_with_rc`
//! supplies an empty registry by default; the engine-aware entry point
//! [`crate::server::router_with_engines`] lets the role entry points
//! pass their configured registry through (E3/E5). Like the rest of E1,
//! nothing routes through the engine trait yet: with no engines registered
//! the endpoint returns an empty list.

use axum::extract::Extension;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::engine::capability::CapabilityTable;
use crate::engine::EngineRegistry;

/// The response shape: `{"capabilities": [CapabilityTable, ...]}` keyed by
/// engine (unique per engine+transport). Deterministic: registry ids are
/// returned sorted and each engine's table is a `BTreeMap`.
pub fn capabilities_json(registry: &EngineRegistry) -> Value {
    let tables: Vec<CapabilityTable> = registry
        .enabled_ids()
        .iter()
        .filter_map(|id| registry.capability_table(id))
        .collect();
    json!({ "capabilities": tables })
}

/// `GET /api/v1/capabilities` handler (engine-gated: the route is registered
/// only when the crate builds with the `engine` feature).
pub async fn capabilities_handler(Extension(registry): Extension<Arc<EngineRegistry>>) -> Response {
    Json(capabilities_json(&registry)).into_response()
}

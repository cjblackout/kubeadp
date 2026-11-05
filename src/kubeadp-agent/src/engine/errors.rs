//! Engine error classification (design HL-91 section 2.6).
//!
//! [`EngineError`] is the trait's error type. The endpoint layer maps
//! [`EngineError::Unsupported`] to HTTP 501 with the body
//! `{"error": ..., "code": "unsupported", "engine": <id>}` via
//! [`to_response`]; the other variants are internal plumbing that maps to the
//! existing 500/404 shapes.
//!
//! 501 is reserved for genuinely-impossible cases only: a future engine that
//! honestly reports a gap on the conformance suite can ship while answering
//! this body for that one endpoint. During E1 nothing routes through the
//! trait yet -- this module exists so the mapping is testable and the
//! endpoint layer has the arm in place.

use crate::engine::capability::CapEndpointKey;
use crate::engine::run::{EngineId, RunId};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::fmt;

/// An engine operation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// The engine cannot perform `endpoint`; the HTTP layer answers 501 with
    /// `{"error", "code":"unsupported", "engine":<id>}`.
    Unsupported {
        engine: EngineId,
        endpoint: CapEndpointKey,
    },
    /// An engine named by a request/config that this pod did not register.
    /// The endpoint layer answers 400 `{"code":"unknown_engine"}` (design
    /// 2.4 rule: an unregistered engine is a selection error, never a 501).
    UnknownEngine { engine: EngineId },
    /// No run with this id.
    NotFound { run: RunId },
    /// A state/ordering conflict (one resume at a time, etc.).
    Conflict(String),
    /// Any other engine failure; maps to 500 `{"code":"internal"}`.
    Internal(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Unsupported { engine, endpoint } => write!(
                f,
                "engine '{engine}' does not support endpoint '{endpoint}'"
            ),
            EngineError::UnknownEngine { engine } => {
                write!(f, "unknown engine '{engine}'")
            }
            EngineError::NotFound { run } => write!(f, "no such run: '{run}'"),
            EngineError::Conflict(msg) => write!(f, "{msg}"),
            EngineError::Internal(msg) => write!(f, "engine error: {msg}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl EngineError {
    /// Constructs an `Unsupported` error for `engine` + `endpoint`.
    pub fn unsupported(engine: impl Into<EngineId>, endpoint: CapEndpointKey) -> Self {
        Self::Unsupported {
            engine: engine.into(),
            endpoint,
        }
    }
}

/// Maps an [`EngineError`] to its HTTP response:
///
/// - `Unsupported` -> 501 `{"error": "<msg>", "code": "unsupported",
///   "engine": "<id>"}` (the E1 contract body),
/// - `UnknownEngine` -> 400 `{"error","code":"unknown_engine","engine"}`,
/// - `NotFound`    -> 404 `{"error","code":"not_found"}`,
/// - `Conflict`    -> 409 `{"error","code":"conflict"}`,
/// - `Internal`    -> 500 `{"error","code":"internal"}`.
pub fn to_response(err: &EngineError) -> Response {
    match err {
        EngineError::Unsupported {
            engine,
            endpoint: _,
        } => (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "error": err.to_string(),
                "code": "unsupported",
                "engine": engine,
            })),
        )
            .into_response(),
        EngineError::UnknownEngine { engine } => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": err.to_string(),
                "code": "unknown_engine",
                "engine": engine,
            })),
        )
            .into_response(),
        EngineError::NotFound { run } => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no such run: '{run}'"), "code": "not_found" })),
        )
            .into_response(),
        EngineError::Conflict(msg) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": msg, "code": "conflict" })),
        )
            .into_response(),
        EngineError::Internal(msg) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": msg, "code": "internal" })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn body_string(body: axum::body::Body) -> String {
        let bytes = body.collect().await.expect("body").to_bytes();
        String::from_utf8_lossy(&bytes).to_string()
    }

    #[tokio::test]
    async fn unsupported_maps_to_501_body() {
        let err = EngineError::unsupported("agy", "fork");
        let res = to_response(&err);
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
        let value: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unsupported");
        assert_eq!(value["engine"], "agy");
        assert!(
            value["error"].as_str().unwrap().contains("fork"),
            "500 body error should name the endpoint: {value}"
        );
    }

    #[tokio::test]
    async fn other_variants_map_to_expected_status_codes() {
        let res = to_response(&EngineError::NotFound {
            run: "nope".to_string(),
        });
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = to_response(&EngineError::Conflict("locked".to_string()));
        assert_eq!(res.status(), StatusCode::CONFLICT);

        let res = to_response(&EngineError::Internal("boom".to_string()));
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let value: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "internal");
    }

    #[tokio::test]
    async fn unknown_engine_maps_to_400_not_501() {
        // Design 2.4 hard rule: an unregistered engine is a selection error
        // (400 unknown_engine), never a 501 -- 501 is reserved for genuinely
        // impossible endpoints of a registered engine.
        let res = to_response(&EngineError::UnknownEngine {
            engine: "bogus".to_string(),
        });
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unknown_engine");
        assert_eq!(value["engine"], "bogus");
    }
}

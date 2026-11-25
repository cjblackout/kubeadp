//! Minimal default implementations helper (design HL-91 E1: "a minimal
//! default-implementations helper if the design calls for it").
//!
//! [`EmptyEngineCtx`] is the default [`crate::engine::EngineCtx`] used before
//! a driver fills in a real context (E2/E3): everything answers the neutral
//! default (no requested/configured engine, worker role, no resolved
//! engine). Selection then falls back to the registry defaults.

use crate::engine::run::PodRole;

/// The default engine context: no selection hints, worker role, no resolved
/// engine. E1 code (the `/api/v1/capabilities` handler tests) carries this;
/// the drivers supply their own in E2/E3.
#[derive(Debug, Clone, Default)]
pub struct EmptyEngineCtx;

impl crate::engine::EngineCtx for EmptyEngineCtx {
    fn requested_engine(&self) -> Option<&str> {
        None
    }

    fn configured_engine(&self) -> Option<&str> {
        None
    }

    fn role(&self) -> PodRole {
        PodRole::Worker
    }

    fn engine(&self) -> Option<&str> {
        None
    }
}

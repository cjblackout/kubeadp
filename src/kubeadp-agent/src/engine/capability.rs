//! Capability reporting (design HL-91 section 2.3).
//!
//! [`Capability`] is an engine's answer for one endpoint key:
//! `native` (the engine implements it itself), `emulated` (the emulation
//! layer of E4 fronts it) or `unsupported` (genuinely impossible; the
//! endpoint would answer 501). The static endpoint table [`ENDPOINTS`]
//! enumerates every key the API can address, with the run kinds each key
//! serves.
//!
//! [`CapabilityTable`] is a per-engine (+ per-claude-transport) report served
//! by `GET /api/v1/capabilities`. E1 builds it purely from [`ENDPOINTS`] plus
//! the sets an engine declares via [`CapabilityTableBuilder`]; it never
//! reaches a live engine process. E4 overlays `Emulated` rows with
//! [`CapabilityTable::with_emulated`].

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::engine::run::{EngineId, Transport};

/// An engine's capability answer for one endpoint key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Capability {
    Native,
    Emulated,
    Unsupported,
}

/// A stable endpoint key name (identical across engines).
pub type CapEndpointKey = &'static str;

/// One row of the static endpoint table: the key plus the run kinds that can
/// reach it (metadata for clients; the capability value lives in the table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CapEndpoint {
    pub key: CapEndpointKey,
    /// Reachable in job mode.
    pub job: bool,
    /// Reachable in session mode.
    pub session: bool,
}

/// The static endpoint table every capability table is built from. Keys are
/// stable across engines and transports.
pub const ENDPOINTS: &[CapEndpoint] = &[
    CapEndpoint {
        key: "session.start",
        job: false,
        session: true,
    },
    CapEndpoint {
        key: "session.stop",
        job: false,
        session: true,
    },
    CapEndpoint {
        key: "session.restart",
        job: false,
        session: true,
    },
    CapEndpoint {
        key: "session.log",
        job: false,
        session: true,
    },
    CapEndpoint {
        key: "session.message",
        job: false,
        session: true,
    },
    CapEndpoint {
        key: "job.start",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "job.status",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "job.list",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "job.kill",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "job.retry",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "job.events",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "job.result",
        job: true,
        session: false,
    },
    CapEndpoint {
        key: "interrupt",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "pause",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "fork",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "compact",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "revert",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "todo",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "permissions",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "workspace.*",
        job: true,
        session: true,
    },
    CapEndpoint {
        key: "a2a.send_message",
        job: true,
        session: true,
    },
];

/// Looks up the static row (capability applicability metadata) for a key.
pub fn endpoint_by_key(key: CapEndpointKey) -> Option<&'static CapEndpoint> {
    ENDPOINTS.iter().find(|e| e.key == key)
}

/// A per-engine capability report, served by `GET /api/v1/capabilities`
/// (design 2.3). `transport` is only meaningful for claude (PTY vs
/// stream-json); other engines leave it `None`. Serialized for the HTTP
/// response; the endpoint keys borrow from the static [`ENDPOINTS`] table, so
/// the type is `Serialize` (not `Deserialize`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapabilityTable {
    pub engine: EngineId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,
    pub endpoints: BTreeMap<CapEndpointKey, Capability>,
}

impl CapabilityTable {
    /// Starts a table for `engine` with every endpoint `Unsupported`.
    pub fn builder(engine: impl Into<EngineId>) -> CapabilityTableBuilder {
        CapabilityTableBuilder {
            engine: engine.into(),
            transport: None,
            native: BTreeSet::new(),
            emulated: BTreeSet::new(),
        }
    }

    /// The capability for `key`, or `None` when the key is not in
    /// [`ENDPOINTS`].
    pub fn capability(&self, key: CapEndpointKey) -> Option<Capability> {
        self.endpoints.get(key).copied()
    }

    /// The effective (post-emulation) label after E4 engages: flips `key` to
    /// `Emulated` in the report, mirroring [`crate::engine::emulate`]'s
    /// fronting of the endpoint. Unknown keys are ignored.
    pub fn with_emulated(mut self, keys: &[CapEndpointKey]) -> Self {
        for key in keys {
            if endpoint_by_key(key).is_some() {
                self.endpoints.insert(key, Capability::Emulated);
            }
        }
        self
    }
}

/// Builds a [`CapabilityTable`] from the static [`ENDPOINTS`] table plus the
/// engine's declared native/emulated sets; every other endpoint is
/// `Unsupported`. A key stated in both sets resolves to `Emulated`.
#[derive(Debug, Clone)]
pub struct CapabilityTableBuilder {
    engine: EngineId,
    transport: Option<Transport>,
    native: BTreeSet<CapEndpointKey>,
    emulated: BTreeSet<CapEndpointKey>,
}

impl CapabilityTableBuilder {
    /// Sets the claude transport the table describes.
    pub fn transport(mut self, transport: Transport) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Declares endpoints the engine implements natively.
    pub fn native(mut self, keys: &[CapEndpointKey]) -> Self {
        self.native.extend(keys.iter().copied());
        self
    }

    /// Declares endpoints the E4 emulation layer fronts (the report shows
    /// them `Emulated` from the start).
    pub fn emulated(mut self, keys: &[CapEndpointKey]) -> Self {
        self.emulated.extend(keys.iter().copied());
        self
    }

    /// Consumes the builder into the final table.
    pub fn build(self) -> CapabilityTable {
        // Unknown keys in either set are a programming error: silently
        // dropping them would misreport the endpoint as Unsupported.
        for key in self.native.union(&self.emulated) {
            assert!(
                endpoint_by_key(key).is_some(),
                "capability table built with unknown endpoint key '{key}'"
            );
        }
        let mut endpoints = BTreeMap::new();
        for row in ENDPOINTS {
            let capability = if self.emulated.contains(row.key) {
                Capability::Emulated
            } else if self.native.contains(row.key) {
                Capability::Native
            } else {
                Capability::Unsupported
            };
            endpoints.insert(row.key, capability);
        }
        CapabilityTable {
            engine: self.engine,
            transport: self.transport,
            endpoints,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AGY_NATIVE: &[CapEndpointKey] = &[
        "job.start",
        "job.status",
        "job.list",
        "job.kill",
        "job.retry",
        "job.events",
        "job.result",
        "pause",
        "workspace.*",
    ];
    const AGY_EMULATED: &[CapEndpointKey] = &[
        "interrupt",
        "session.start",
        "session.stop",
        "session.restart",
        "session.log",
        "session.message",
        "a2a.send_message",
    ];

    #[test]
    fn static_endpoint_table_covers_both_modes() {
        let keys: BTreeSet<&str> = ENDPOINTS.iter().map(|e| e.key).collect();
        for key in [
            "session.start",
            "session.stop",
            "session.restart",
            "session.log",
            "session.message",
            "job.start",
            "job.status",
            "job.list",
            "job.kill",
            "job.retry",
            "job.events",
            "job.result",
            "interrupt",
            "pause",
            "fork",
            "compact",
            "revert",
            "todo",
            "permissions",
            "workspace.*",
            "a2a.send_message",
        ] {
            assert!(
                keys.contains(key),
                "capability endpoint table must cover '{key}'"
            );
        }
        // Applicability flags are consistent: session keys are session-only,
        // job keys job-only, control keys both.
        assert!(endpoint_by_key("session.start").unwrap().session);
        assert!(!endpoint_by_key("session.start").unwrap().job);
        assert!(endpoint_by_key("job.start").unwrap().job);
        assert!(!endpoint_by_key("job.start").unwrap().session);
        assert!(endpoint_by_key("interrupt").unwrap().job);
        assert!(endpoint_by_key("interrupt").unwrap().session);
    }

    #[test]
    fn builder_marks_native_emulated_rest_unsupported() {
        let table = CapabilityTable::builder("agy")
            .native(AGY_NATIVE)
            .emulated(AGY_EMULATED)
            .build();

        // Every static endpoint key has a value in the table.
        assert_eq!(table.endpoints.len(), ENDPOINTS.len());
        assert_eq!(table.capability("job.start"), Some(Capability::Native));
        assert_eq!(
            table.capability("session.start"),
            Some(Capability::Emulated)
        );
        // Not declared -> Unsupported.
        assert_eq!(table.capability("fork"), Some(Capability::Unsupported));
        assert_eq!(
            table.capability("permissions"),
            Some(Capability::Unsupported)
        );
        // transport stays None for non-claude engines.
        assert_eq!(table.transport, None);
        // Unknown key -> None.
        assert_eq!(table.capability("bogus.endpoint"), None);
    }

    #[test]
    fn emulated_wins_over_native_in_builder() {
        let table = CapabilityTable::builder("agy")
            .native(&["job.start"])
            .emulated(&["job.start", "todo"])
            .build();
        assert_eq!(table.capability("job.start"), Some(Capability::Emulated));
        assert_eq!(table.capability("todo"), Some(Capability::Emulated));
    }

    #[test]
    fn with_emulated_flips_rows_and_ignores_unknown() {
        let table = CapabilityTable::builder("agy")
            .native(&["job.start"])
            .build()
            .with_emulated(&["job.start", "no.such.key"]);
        assert_eq!(table.capability("job.start"), Some(Capability::Emulated));
        assert_eq!(table.capability("job.kill"), Some(Capability::Unsupported));
    }

    #[test]
    fn capability_serde_is_lowercase() {
        assert_eq!(
            serde_json::to_string(&Capability::Native).unwrap(),
            "\"native\""
        );
        assert_eq!(
            serde_json::to_string(&Capability::Emulated).unwrap(),
            "\"emulated\""
        );
        assert_eq!(
            serde_json::to_string(&Capability::Unsupported).unwrap(),
            "\"unsupported\""
        );
        assert_eq!(
            serde_json::from_str::<Capability>("\"native\"").unwrap(),
            Capability::Native
        );
    }

    #[test]
    fn table_serialises_engine_transport_endpoints() {
        let table = CapabilityTable::builder("claude")
            .transport(Transport::StreamJson)
            .native(&["job.start", "session.start"])
            .build();
        let value = serde_json::to_value(&table).unwrap();
        assert_eq!(value["engine"], "claude");
        assert_eq!(value["transport"], "stream_json");
        assert!(value["endpoints"].is_object());
        assert_eq!(value["endpoints"]["job.start"], "native");
        assert_eq!(value["endpoints"]["fork"], "unsupported");

        // A non-claude table omits transport entirely.
        let value = serde_json::to_value(CapabilityTable::builder("agy").build()).unwrap();
        assert!(value.get("transport").is_none());
    }

    #[test]
    #[should_panic(expected = "unknown endpoint key")]
    fn builder_rejects_unknown_key() {
        let _ = CapabilityTable::builder("x")
            .native(&["job.start", "no.such.key"])
            .build();
    }
}

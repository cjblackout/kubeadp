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

/// One A2A injection channel (design HL-113 §7.14): the native context
/// channel a harness uses to put a queued A2A message into a run's model
/// context. The injection is the acknowledgement; a channel that delivered a
/// message marks it `injected` in the run's inbox
/// ([`crate::core::a2a::inbox`]).
///
/// - `Hook` (Claude Code): the harness-configured session hooks; the
///   `kubeadp-agent a2a-hook <event>` subcommand prints the hook JSON for the
///   messages queued at the hook boundary.
/// - `ToolResult` (every engine): the harness-owned `mcp-a2a` server appends
///   the queued messages to the result of its tool calls; `wait_message` is a
///   blocking receive whose result is the message.
/// - `IdleTurn` (every engine): at an idle turn boundary the harness starts
///   the next turn with the message (phase 2 wiring).
/// - `Interrupt`: the explicit last resort (the existing kill-and-resume,
///   only for `interrupt: true` messages or a force timeout; phase 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectChannel {
    Hook,
    ToolResult,
    IdleTurn,
    Interrupt,
}

impl InjectChannel {
    /// The stable channel name (the inbox marker's `channel` field).
    pub fn as_str(&self) -> &'static str {
        match self {
            InjectChannel::Hook => "hook",
            InjectChannel::ToolResult => "tool_result",
            InjectChannel::IdleTurn => "idle_turn",
            InjectChannel::Interrupt => "interrupt",
        }
    }
}

/// The injection channels each engine supports (design HL-113 §7.14). Both
/// engines support `hook`, `tool_result` and `idle_turn` (the parity
/// requirement: the agy hook channel is a native agy plugin `PreInvocation`
/// hook, verified live; the Claude Code hook channel is the
/// PostToolUse/UserPromptSubmit/Stop hooks). `Interrupt` is not listed: it is
/// the explicit last resort every engine already reaches through the existing
/// kill-and-resume, never an advertised channel. Unknown engines answer empty
/// (no injection).
pub fn inject_channels(engine: &str) -> &'static [InjectChannel] {
    const CHANNELS: &[InjectChannel] = &[
        InjectChannel::Hook,
        InjectChannel::ToolResult,
        InjectChannel::IdleTurn,
    ];
    match engine {
        "claude" | "agy" => CHANNELS,
        _ => &[],
    }
}

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
    CapEndpoint {
        key: "a2a.inject",
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
    /// The A2A injection channels this engine supports (design HL-113 §7.14,
    /// [`inject_channels`]); empty (and omitted) for engines without
    /// injection.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inject_channels: Vec<InjectChannel>,
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
        let inject_channels = inject_channels(&self.engine).to_vec();
        CapabilityTable {
            engine: self.engine,
            transport: self.transport,
            endpoints,
            inject_channels,
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
            "a2a.inject",
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
        assert!(endpoint_by_key("a2a.inject").unwrap().job);
        assert!(endpoint_by_key("a2a.inject").unwrap().session);
    }

    #[test]
    fn inject_channels_are_declared_per_engine() {
        // The design parity: both engines support hook + tool_result +
        // idle_turn (the agy hook channel is the verified native agy plugin
        // PreInvocation hook). Interrupt is the last resort, never an
        // advertised channel.
        for engine in ["claude", "agy"] {
            assert_eq!(
                inject_channels(engine),
                &[
                    InjectChannel::Hook,
                    InjectChannel::ToolResult,
                    InjectChannel::IdleTurn
                ],
                "{engine} channels"
            );
        }
        assert!(inject_channels("other").is_empty());
        // The stable channel names serialize snake_case.
        assert_eq!(InjectChannel::ToolResult.as_str(), "tool_result");
        assert_eq!(
            serde_json::to_string(&InjectChannel::IdleTurn).unwrap(),
            "\"idle_turn\""
        );
        assert_eq!(
            serde_json::from_str::<InjectChannel>("\"hook\"").unwrap(),
            InjectChannel::Hook
        );
    }

    #[test]
    fn inject_channels_land_in_the_table() {
        let agy = CapabilityTable::builder("agy")
            .native(&["a2a.inject"])
            .build();
        assert_eq!(
            agy.inject_channels,
            vec![
                InjectChannel::Hook,
                InjectChannel::ToolResult,
                InjectChannel::IdleTurn
            ]
        );
        assert_eq!(agy.capability("a2a.inject"), Some(Capability::Native));

        // An engine without injection omits the field entirely.
        let other = CapabilityTable::builder("testcfg").build();
        assert!(other.inject_channels.is_empty());
        let value = serde_json::to_value(&other).unwrap();
        assert!(value.get("inject_channels").is_none());
        // A table with channels serializes them.
        let value = serde_json::to_value(&agy).unwrap();
        assert_eq!(
            value["inject_channels"],
            serde_json::json!(["hook", "tool_result", "idle_turn"])
        );
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

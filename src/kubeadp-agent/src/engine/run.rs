//! The unified engine run model (design HL-91 section 2.1).
//!
//! These types are the union view that the engine trait, the future run
//! registry and `GET /api/v1/capabilities` share during E1-E6. They replace
//! no existing registry (`JobMetadata`/`SessionState` stay authoritative for
//! jobs and sessions); `Run` is the shape an engine reports through the
//! trait.
//!
//! `Transport` records how a claude run talks to the harness (interactive PTY
//! with `--remote-control`, or headless `--input-format stream-json`); the
//! other engines carry no transport.
//!
//! `Role` is the pod role that hosts a run (the orchestrator supervisor pod
//! and the worker job-execution pod). Selection precedence: request field >
//! per-run config > per-role default.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A run's identity, unique within its registry. Today the job API uses the
/// job id (`core/job.rs`) and the sessions API the session name
/// (`orchestrate/state.rs`); the trait speaks in this id.
pub type RunId = String;

/// An engine's stable identifier (`"claude"`, `"agy"`, ...): the value the
/// `engine` fields of config bodies carry, and the `engine` key of the 501
/// body and of `CapabilityTable`.
pub type EngineId = String;

/// The harness's own context id for a run: the claude session id / agy
/// conversation id. `None` until the engine has created one.
pub type ContextId = String;

/// How a claude run talks to the harness. `None` (no transport) for engines
/// without transports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    /// Interactive PTY with `--remote-control` (long-lived pod sessions).
    PtyRemoteControl,
    /// Headless `claude -p --input-format stream-json` (jobs and
    /// non-interactive sessions).
    StreamJson,
}

/// Whether a run is a long-lived session or a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    Session,
    Job,
}

/// The lifecycle status of a run, drawn from the union of the job taxonomy
/// (`core/job.rs`) and the session states (`orchestrate/session.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Not yet started (session armed / job queued).
    Pending,
    /// Spawning / in the launch handshake.
    Starting,
    /// The harness process is live.
    Running,
    /// Orphan / restart backoff (orchestrate specific).
    Backoff,
    /// Paused (SIGSTOP on the process group).
    Paused,
    /// Stopped (session stopped by the control API).
    Stopped,
    /// A failed attempt at the launch handshake.
    Failed,
    /// Killed (bounded kill via process-group signal).
    Killed,
    /// Finished cleanly.
    Done,
    /// Interrupted mid-run (E4 emulation result).
    Interrupted,
}

/// Exit information mirroring the job exit fields (`core/reaper.rs`): signal
/// or code, whichever the engine observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitInfo {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

/// A message role within an [`EngineEvent::MessageDelta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    Tool,
}

/// A Todo-write item surfaced as an [`EngineEvent::Todo`] (E4 parses
/// `TodoWrite` tool calls / plan steps into these).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoItem {
    pub content: String,
    pub status: Option<String>,
}

/// A structured engine event: the typed surface the trait's `events()`
/// returns (the raw `/log` byte contract stays engine-independent).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EngineEvent {
    /// A status transition.
    Status(RunStatus),
    /// Newly appended message text for `role`.
    MessageDelta { role: Role, text: String },
    /// A tool invocation with its JSON input.
    ToolCall {
        name: String,
        input: serde_json::Value,
    },
    /// A todo item written by the harness.
    Todo(TodoItem),
    /// The run finished with `status`.
    Done { status: RunStatus },
    /// A harness error.
    Error { message: String },
}

/// The union run view: everything the endpoint layer and the registry need to
/// talk to an engine about one run. Not a replacement for `JobMetadata` or
/// `SessionState` -- the union of what the trait needs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub engine: EngineId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,
    pub kind: RunKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<ContextId>,
    pub status: RunStatus,
    pub workdir: PathBuf,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit: Option<ExitInfo>,
    /// Monotone cursor for `events(after)`.
    pub events_seq: u64,
    /// Engine-specific payload (conv id, transcript path, pid, ...).
    #[serde(default)]
    pub meta: serde_json::Value,
}

impl Run {
    /// A fresh run pre-selection: engine/transport empty, status Pending.
    pub fn new(id: impl Into<RunId>, kind: RunKind, workdir: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            engine: String::new(),
            transport: None,
            kind,
            context_id: None,
            status: RunStatus::Pending,
            workdir: workdir.into(),
            started_at: None,
            ended_at: None,
            exit: None,
            events_seq: 0,
            meta: serde_json::Value::Object(Default::default()),
        }
    }

    /// Returns `true` when the run has reached a terminal status.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            RunStatus::Stopped
                | RunStatus::Failed
                | RunStatus::Killed
                | RunStatus::Done
                | RunStatus::Interrupted
        )
    }
}

/// The pod role hosting a run (selection fallback).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PodRole {
    Orchestrator,
    Worker,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_fields_default_and_terminal_classification() {
        let mut run = Run::new("r-1", RunKind::Job, "/workspace/proj");
        assert_eq!(run.status, RunStatus::Pending);
        assert!(!run.is_terminal());
        assert_eq!(run.transport, None);

        run.status = RunStatus::Done;
        assert!(run.is_terminal());
        run.status = RunStatus::Running;
        assert!(!run.is_terminal());
        run.status = RunStatus::Interrupted;
        assert!(run.is_terminal());
    }

    #[test]
    fn run_serde_roundtrip() {
        let run = Run {
            id: "r-2".to_string(),
            engine: "claude".to_string(),
            transport: Some(Transport::StreamJson),
            kind: RunKind::Session,
            context_id: Some("ctx-9".to_string()),
            status: RunStatus::Running,
            workdir: PathBuf::from("/workspace/proj"),
            started_at: None,
            ended_at: None,
            exit: None,
            events_seq: 4,
            meta: serde_json::json!({ "pid": 42 }),
        };
        let json = serde_json::to_string(&run).unwrap();
        let back: Run = serde_json::from_str(&json).unwrap();
        assert_eq!(back, run);
    }

    #[test]
    fn run_kind_and_transport_serde_names() {
        assert_eq!(
            serde_json::to_string(&RunKind::Session).unwrap(),
            "\"session\""
        );
        assert_eq!(serde_json::to_string(&RunKind::Job).unwrap(), "\"job\"");
        assert_eq!(
            serde_json::to_string(&Transport::PtyRemoteControl).unwrap(),
            "\"pty_remote_control\""
        );
        assert_eq!(
            serde_json::to_string(&Transport::StreamJson).unwrap(),
            "\"stream_json\""
        );
        assert_eq!(
            serde_json::from_str::<Transport>("\"stream_json\"").unwrap(),
            Transport::StreamJson
        );
    }

    #[test]
    fn engine_event_serialises_tagged() {
        let ev = EngineEvent::MessageDelta {
            role: Role::Assistant,
            text: "hi".to_string(),
        };
        let value = serde_json::to_value(&ev).unwrap();
        assert_eq!(value["type"], "message_delta");
        assert_eq!(value["role"], "assistant");
        assert_eq!(value["text"], "hi");
    }
}

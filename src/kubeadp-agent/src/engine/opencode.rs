//! The opencode engine (KA-4): a job executor over the opencode `run` CLI.
//!
//! [`OpenCodeEngine`] implements the [`crate::engine::Engine`] trait for the
//! opencode harness:
//!
//! - **job mode** invokes headless `opencode run --format json [--model
//!   provider/model] [--session <id>] [--auto] --dir <worktree> <prompt>`.
//!   The prompt is a positional argv element (opencode's CLI contract;
//!   verified live against the in-cluster binary), and a retry resumes the
//!   harness conversation via `--session <conversation_id>`.
//! - **session mode** runs `opencode run -i --format json --auto --dir
//!   <workdir> <start-message>` inside the supervisor PTY, mirroring the agy
//!   live-stream approach: the interactive opencode process reads follow-up
//!   turns as user lines typed on the PTY.
//!
//! opencode has no `--remote-control` and no hook mechanism, so its A2A
//! injection ladder is the floor (only `idle_turn`): a queued message resumes
//! a finished job as the next turn; a RUNNING opencode job has no live
//! channel and answers `Unsupported` for `idle_turn`.
//!
//! The engine carries only the process-wide [`Config`] (the opencode binary
//! path plus the state root the job rows live under); per-run state lives in
//! the job store (`core::job`), so the engine is stateless and cheaply shared
//! behind an `Arc`. The job driver is synchronous (the runner spawns and
//! monitors the process; status classification reads the job store), exactly
//! like [`crate::engine::agy::AgyEngine`].

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::core::config::Config;
use crate::core::job::{compute_status, JobMetadata, JobStatus};

use crate::engine::capability::{Capability, CapabilityTable};
use crate::engine::run::{ContextId, EngineEvent, ExitInfo, Role, Run, RunId, RunStatus};
use crate::engine::{
    Engine, EngineCtx, EngineError, JobSpec, LaunchPlan, ResumeMode, SessionDef, SessionSpec,
    StdinMode, TurnInput,
};

/// The stable opencode engine id.
pub const OPENCODE_ENGINE_ID: &str = "opencode";

/// The opencode engine: a thin adapter over the opencode `run` CLI driven by
/// the engine-aware job runner.
pub struct OpenCodeEngine {
    config: Arc<Config>,
}

impl OpenCodeEngine {
    /// Constructs the engine over the process-wide config.
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// The process-wide config this engine runs jobs under.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Structured events for a job, parsed from its opencode JSON-event log
    /// (the typed surface the trait exposes; the raw `/log` byte contract stays
    /// engine-independent). Each NDJSON event line becomes an
    /// [`EngineEvent`]; non-`{` lines are skipped. `after` is accepted for
    /// signature compatibility; like the agy adapter this is cursor-ignoring
    /// (cursor semantics arrive with the emulation layer).
    pub(crate) fn events_of(&self, run: &Run, _after: u64) -> Result<Vec<EngineEvent>> {
        let log_path = JobMetadata::log_path(&self.config.state_root, &run.id);
        let bytes = match std::fs::read(&log_path) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(Vec::new()),
        };
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let mut events = Vec::new();
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || !trimmed.starts_with('{') {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                continue;
            };
            match value.get("type").and_then(|t| t.as_str()) {
                Some("step_start") => events.push(EngineEvent::Status(RunStatus::Running)),
                Some("text") => {
                    let text = value
                        .get("part")
                        .and_then(|p| p.get("text"))
                        .and_then(|t| t.as_str())
                        .unwrap_or_default();
                    if !text.is_empty() {
                        events.push(EngineEvent::MessageDelta {
                            role: Role::Assistant,
                            text: text.to_string(),
                        });
                    }
                }
                Some("tool_use") => {
                    let part = value.get("part");
                    let name = part
                        .and_then(|p| p.get("tool"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("tool")
                        .to_string();
                    let input = part
                        .and_then(|p| p.get("state"))
                        .and_then(|s| s.get("input"))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    events.push(EngineEvent::ToolCall { name, input });
                }
                Some("step_finish") => {
                    // A completed turn (reason "stop") finishes the run.
                    let reason = value
                        .get("part")
                        .and_then(|p| p.get("reason"))
                        .and_then(|r| r.as_str())
                        .unwrap_or("");
                    if reason == "stop" {
                        events.push(EngineEvent::Done {
                            status: RunStatus::Done,
                        });
                    }
                }
                Some("error") => {
                    let message = value
                        .get("error")
                        .and_then(|e| e.get("data"))
                        .and_then(|d| d.get("message"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("opencode error")
                        .to_string();
                    events.push(EngineEvent::Error { message });
                    events.push(EngineEvent::Done {
                        status: RunStatus::Failed,
                    });
                }
                _ => {}
            }
        }
        Ok(events)
    }
}

/// Maps the opencode job taxonomy to the engine [`RunStatus`]. The
/// `From<JobStatus> for RunStatus` implementation is shared with the agy
/// engine (defined in [`crate::engine::agy`]): every engine maps the same
/// `JobStatus` taxonomy to the same `RunStatus` union.
#[async_trait]
impl Engine for OpenCodeEngine {
    fn id(&self) -> String {
        OPENCODE_ENGINE_ID.to_string()
    }

    async fn start_job(
        &self,
        _spec: JobSpec,
        _ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        // The opencode job launch needs the fully-built `JobMetadata` row
        // (worktree, branch, prompt, ...) that the job command layer
        // (`commands::start`) constructs before launching; the row is not
        // derivable from the trait's `JobSpec`. Today's `/api/v1/jobs` endpoint
        // drives the engine's `job_command` (through `core::runner`) and never
        // reaches this async seam. It therefore answers `Internal` (never
        // 501), exactly like the agy engine.
        Err(EngineError::Internal(
            "opencode start_job is driven by the job command layer (built JobMetadata row)"
                .to_string(),
        ))
    }

    async fn resume_job(&self, run: &Run, prompt: &str) -> Result<(), EngineError> {
        // Same wiring note as `start_job`: `commands::retry` drives the resume
        // path; this async seam becomes reachable when the endpoint layer
        // routes through the trait.
        let _ = (run, prompt);
        Err(EngineError::Internal(
            "opencode resume_job is driven by the job command layer (built JobMetadata row)"
                .to_string(),
        ))
    }

    async fn job_status(&self, run: &Run) -> Result<RunStatus, EngineError> {
        let meta = JobMetadata::load(&self.config.state_root, &run.id)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        Ok(RunStatus::from(compute_status(
            &meta,
            &self.config.state_root,
        )))
    }

    fn is_idle(&self, run: &Run) -> Result<bool, EngineError> {
        // A finished opencode job is idle (the next turn can be started with
        // the message); a RUNNING opencode job is never idle -- it has no
        // live stdin channel, so the `idle_turn` step cannot inject mid-run.
        let meta = JobMetadata::load(&self.config.state_root, &run.id)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        Ok(compute_status(&meta, &self.config.state_root) != JobStatus::Running)
    }

    async fn idle_turn(&self, run: &Run, joined: &str) -> Result<(), EngineError> {
        // The `idle_turn` step of §7.14: the message is started as the run's
        // NEXT turn. opencode has no live channel, so only a finished job is
        // injectable: it is resumed with the joined text as the follow-up
        // turn (same conversation), never killed. A RUNNING opencode job
        // answers Unsupported (the floor of the injection ladder).
        let meta = JobMetadata::load(&self.config.state_root, &run.id)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        if compute_status(&meta, &self.config.state_root) == JobStatus::Running {
            return Err(EngineError::unsupported(self.id(), "a2a.inject.idle_turn"));
        }
        let args = crate::core::args::RetryArgs {
            job_id: run.id.clone(),
            prompt: Some(joined.to_string()),
            ..Default::default()
        };
        crate::core::commands::retry::run(&self.config, &args).map_err(|e| {
            EngineError::Internal(format!("failed to resume job '{}': {e}", run.id))
        })?;
        Ok(())
    }

    async fn start_session(
        &self,
        _spec: SessionSpec,
        _ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        // opencode sessions run through the supervisor session machinery
        // (`session_command`); this async seam is unwired like the agy
        // engine's.
        Err(EngineError::unsupported(self.id(), "session.start"))
    }

    async fn send_message(&self, run: &Run, text: &str) -> Result<(), EngineError> {
        let _ = (run, text);
        Err(EngineError::unsupported(self.id(), "session.message"))
    }

    async fn stop_session(&self, run: &Run, restart: bool) -> Result<(), EngineError> {
        let _ = (run, restart);
        Err(EngineError::unsupported(self.id(), "session.stop"))
    }

    fn job_command(
        &self,
        job: &JobMetadata,
        turn: TurnInput<'_>,
    ) -> Result<LaunchPlan, EngineError> {
        let mut args: Vec<String> = vec![
            "run".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ];

        // The model is passed as `provider/model` (`--model`), exactly like
        // the job row stores it.
        if let Some(m) = &job.model {
            args.push("--model".to_string());
            args.push(m.clone());
        }

        // Headless auto-approve: opencode's `--auto` bypasses permission
        // prompts, which cannot be answered in a background job.
        args.push("--auto".to_string());

        let prompt = match turn {
            TurnInput::Start { prompt, .. } => prompt.to_string(),
            TurnInput::Resume {
                prompt,
                conversation,
                ..
            } => {
                if let Some(conv_id) = conversation {
                    // opencode's `run --session <id>` continues the harness
                    // conversation.
                    args.push("--session".to_string());
                    args.push(conv_id.to_string());
                } else {
                    // Bare `run --continue` (opencode's `-c`) resumes the last
                    // session. `retry` refuses a resume without a conversation
                    // id before reaching here; kept so this remains a total
                    // mapping over TurnInput.
                    args.push("--continue".to_string());
                }
                prompt.to_string()
            }
        };

        // The run directory: opencode runs in the job worktree. (The runner
        // also sets the process cwd to the worktree; the flag makes it
        // explicit for opencode, which may be invoked with a different cwd by
        // the sandbox wrapper.)
        args.push("--dir".to_string());
        args.push(job.worktree_path.to_string_lossy().to_string());

        if !prompt.is_empty() {
            args.push(prompt);
        }

        Ok(LaunchPlan {
            program: self.config.opencode_path.clone(),
            args,
            env: Vec::new(),
            stdin: StdinMode::Null,
            pty: false,
        })
    }

    fn session_command(
        &self,
        session: &SessionDef,
        _resume: ResumeMode,
    ) -> Result<LaunchPlan, EngineError> {
        // The supervisor runs an interactive `opencode run -i` inside the PTY:
        // the process stays alive reading follow-up turns as user lines typed
        // on the PTY (opencode has no stream-json live loop and no
        // `--remote-control`). A constant neutral start message is required
        // (opencode refuses an empty message positional).
        let args: Vec<String> = vec![
            "run".to_string(),
            "-i".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--auto".to_string(),
            "--dir".to_string(),
            session.workdir.to_string_lossy().to_string(),
            OPENCODE_SESSION_START.to_string(),
        ];

        let mut env: Vec<(String, String)> = session
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        env.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(LaunchPlan {
            program: session.agy_binary.clone(),
            args,
            env,
            stdin: StdinMode::Null,
            pty: true,
        })
    }

    fn classify_exit(&self, exit: ExitInfo, log_tail: &[u8]) -> JobStatus {
        let log = String::from_utf8_lossy(log_tail);
        crate::core::job::determine_status_from_opencode(exit.code, &log)
    }

    fn conversation_id(&self, job: &JobMetadata) -> Option<ContextId> {
        if let Some(ref conv_id) = job.conversation_id {
            if !conv_id.trim().is_empty() {
                return Some(conv_id.clone());
            }
        }
        let log_path = JobMetadata::log_path(&self.config.state_root, &job.job_id);
        crate::core::job::extract_conversation_id_from_log(&log_path)
    }

    async fn events(&self, run: &Run, after: u64) -> Result<Vec<EngineEvent>, EngineError> {
        self.events_of(run, after)
            .map_err(|e| EngineError::Internal(e.to_string()))
    }

    fn capabilities(&self) -> CapabilityTable {
        // opencode is engine-parity with agy/claude for the job surface (the
        // engine-aware runner provides the same start/status/retry/events
        // lifecycle over the JSON-event log) plus `workspace.*` (operates on
        // the job row / worktree, engine-independent). Session mode mirrors
        // the agy live-stream PTY. `interrupt`/`pause`/`fork`/`compact`/
        // `revert`/`todo` are emulated by the E4 layer, never 501.
        let native: &[crate::engine::capability::CapEndpointKey] = &[
            "job.start",
            "job.status",
            "job.list",
            "job.kill",
            "job.retry",
            "job.events",
            "job.result",
            "workspace.*",
            "a2a.inject",
        ];
        let emulated: &[crate::engine::capability::CapEndpointKey] = &[
            "interrupt",
            "pause",
            "fork",
            "compact",
            "revert",
            "todo",
            "session.start",
            "session.stop",
            "session.restart",
            "session.log",
            "session.message",
            "a2a.send_message",
        ];
        // `Emulated` wins over `Native` in the builder (see capability.rs), so
        // the declared sets mirror the agy fixture exactly.
        let _ = Capability::Emulated;
        CapabilityTable::builder(OPENCODE_ENGINE_ID)
            .native(native)
            .emulated(emulated)
            .build()
    }

    fn display_name(&self) -> &str {
        "opencode"
    }

    fn configured_program(&self, config: &crate::core::config::Config) -> PathBuf {
        config.opencode_path.clone()
    }

    fn session_file(
        &self,
        _supervisor_config: &crate::orchestrate::config::SupervisorConfig,
        _session: &SessionDef,
    ) -> PathBuf {
        PathBuf::new()
    }

    fn has_session_transcript(&self) -> bool {
        false
    }

    fn supports_legacy_memory_file(&self) -> bool {
        false
    }

    fn frame_pty_message(&self, payload: &[u8]) -> Vec<u8> {
        // opencode reads follow-up turns as user lines typed on the PTY, so a
        // message is a plain text line terminated by Enter (no bracketed
        // paste, no NDJSON envelope -- the default trait framing).
        let line = crate::core::a2a_live::frame_user_line(&String::from_utf8_lossy(payload));
        let mut frame = Vec::with_capacity(line.len() + 1);
        frame.extend_from_slice(line.as_bytes());
        frame.push(b'\r');
        frame
    }

    fn materialize_instructions(
        &self,
        home: &std::path::Path,
        workdir: &std::path::Path,
        global_text: Option<(PathBuf, String)>,
        per_session_text: Option<(PathBuf, String)>,
        log: &mut Vec<String>,
    ) -> bool {
        crate::core::instructions::materialize_opencode(home, workdir, global_text, per_session_text, log)
    }
}

/// The constant opening message used on session start: opencode's `run`
/// requires a non-empty message positional (it refuses with "You must provide
/// a message or a command" otherwise), so the session boots with a neutral
/// "await instructions" turn and then idles for real user input on the PTY.
const OPENCODE_SESSION_START: &str =
    "Start a working session in this workspace. Do nothing yet: wait for the next user message.";

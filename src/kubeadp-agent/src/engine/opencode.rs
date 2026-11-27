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
//! opencode has no Claude-format hooks, so its A2A injection channels (KA-7,
//! SPEC §7.14) are carried by the harness's opencode plugin and config
//! (`worker::opencode_config`, materialised into `~/.config/opencode/`):
//!
//! - **`hook`**: the plugin's `experimental.chat.messages.transform` hook
//!   drains the run's inbox (via `kubeadp-agent a2a-hook transform`) and
//!   splices the framed text into the outgoing model messages.
//! - **`tool_result`**: the harness-owned `mcp-a2a` stdio server registered
//!   as `mcp.a2a` in `opencode.json`.
//! - **`idle_turn`**: a queued message resumes a FINISHED job as the next
//!   turn (`retry`); a RUNNING opencode job (`run --format json`) has no live
//!   stdin channel (it is single-turn and exits after `step_finish`), so it
//!   answers `Unsupported` for the `idle_turn` step. Sessions carry queued
//!   messages via the PTY (`frame_pty_message`), engine-agnostic.
//!
//! The engine carries only the process-wide [`Config`] (the opencode binary
//! path plus the state root the job rows live under); per-run state lives in
//! the job store (`core::job`), so the engine is stateless and cheaply shared
//! behind an `Arc`. The job driver is synchronous (the runner spawns and
//! monitors the process; status classification reads the job store), exactly
//! like [`crate::engine::agy::AgyEngine`].

/// The NDJSON event parser (2018 file+dir layout).
pub mod events;

/// Server-backed sessions: the supervised `opencode serve` and its loopback HTTP client.
pub mod serve;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::core::config::Config;
use crate::core::job::{compute_status, JobMetadata, JobStatus};

use crate::engine::capability::{Capability, CapabilityTable};
use crate::engine::claude::session_pty::ClaudePtyAdapter;
use crate::engine::run::{ContextId, EngineEvent, ExitInfo, Run, RunId, RunKind, RunStatus};
use crate::engine::{
    Engine, EngineCtx, EngineError, JobSpec, LaunchPlan, ResumeMode, ServerDelivery, SessionDef,
    SessionSpec, StdinMode, TurnInput,
};

/// The stable opencode engine id.
pub const OPENCODE_ENGINE_ID: &str = "opencode";

/// Derives a stable run id from a workdir (jobs have no supervisor-seeded
/// name; the path's final component is the natural id).
fn run_id_from_workdir(workdir: &std::path::Path) -> String {
    workdir
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "opencode-job".to_string())
}

/// Session naming mirrors the supervisor config: `SessionConfig.name` is the
/// canonical handle for `start`/`stop`/`message`. When starting by workdir
/// alone, the final path component is the natural name.
fn session_name_from_workdir(workdir: &std::path::Path) -> String {
    run_id_from_workdir(workdir)
}

/// The opencode engine: a thin adapter over the opencode `run` CLI driven by
/// the engine-aware job runner.
pub struct OpenCodeEngine {
    config: Arc<Config>,
    pty: Option<ClaudePtyAdapter>,
}

impl OpenCodeEngine {
    /// Constructs the engine over the process-wide config.
    pub fn new(config: Arc<Config>) -> Self {
        Self { config, pty: None }
    }

    /// Constructs the engine with an optional supervisor-adapter (`pty`) and config.
    /// `None` disables session mode operations (answering `unsupported`).
    pub fn with_pty(config: Arc<Config>, pty: Option<ClaudePtyAdapter>) -> Self {
        Self { config, pty }
    }

    fn adapter(&self, endpoint: &'static str) -> Result<&ClaudePtyAdapter, EngineError> {
        self.pty
            .as_ref()
            .ok_or_else(|| EngineError::unsupported(OPENCODE_ENGINE_ID, endpoint))
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
            events.extend(events::parse_opencode_line(line));
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
        // the message); a RUNNING opencode job is never idle -- its
        // `run --format json` mode is single-turn with no live stdin channel,
        // so the `idle_turn` step cannot inject mid-run (the `hook` and
        // `tool_result` channels cover the mid-run window instead).
        // A session is idle when it is RUNNING (between turns: the PTY is idle,
        // so the user line of the `idle_turn` can be delivered). Everything else
        // (starting, stopped, failed, backoff) is not idle.
        match run.kind {
            RunKind::Job => {
                let meta = JobMetadata::load(&self.config.state_root, &run.id)
                    .map_err(|e| EngineError::Internal(e.to_string()))?;
                Ok(compute_status(&meta, &self.config.state_root) != JobStatus::Running)
            }
            RunKind::Session => {
                let adapter = self.adapter("a2a.inject.idle_turn")?;
                let st = adapter
                    .state
                    .supervisor_state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                Ok(st
                    .sessions
                    .get(&run.id)
                    .is_some_and(|s| s.status == crate::orchestrate::state::SessionStatus::Running))
            }
        }
    }

    async fn idle_turn(&self, run: &Run, joined: &str) -> Result<(), EngineError> {
        // The `idle_turn` step of §7.14: the message is started as the run's
        // NEXT turn. A finished job is resumed with the joined text as the
        // follow-up turn (same conversation), never killed. A RUNNING opencode
        // job answers Unsupported (the `hook`/`tool_result` channels deliver
        // its mid-run messages instead). An interactive session receives the
        // message delivered as user input over the supervisor PTY adapter.
        match run.kind {
            RunKind::Job => {
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
            RunKind::Session => {
                let adapter = self.adapter("a2a.inject.idle_turn")?;
                let run_for = Run::new(run.id.clone(), RunKind::Session, &run.workdir);
                let outcome = crate::orchestrate::sessions_api::deliver_message(
                    &adapter.state,
                    &run.id,
                    crate::orchestrate::sessions_api::REST_SENDER,
                    joined,
                    false,
                );
                crate::engine::claude::session_pty::outcome_to_error(outcome, &run_for)
            }
        }
    }

    async fn start_session(
        &self,
        spec: SessionSpec,
        _ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        let adapter = self.adapter("session.start")?;
        let name = session_name_from_workdir(&spec.workdir);
        adapter.start(&name)?;
        Ok(name)
    }

    async fn send_message(&self, run: &Run, text: &str) -> Result<(), EngineError> {
        let adapter = self.adapter("session.message")?;
        adapter.send_message(&run.id, text)
    }

    async fn stop_session(&self, run: &Run, restart: bool) -> Result<(), EngineError> {
        let adapter = self.adapter("session.stop")?;
        adapter.stop(&run.id, restart)
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
        // The supervised process is the headless `opencode serve`, loopback only
        // (`opencode run -i` answers its first message and exits, so it cannot be a
        // long-lived session). The port of this start and the opencode session id
        // (kept across restarts) live in a per-session state file that the HTTP
        // delivery path reads (`deliver_session_message`).
        let port = serve::pick_free_port().map_err(|e| {
            EngineError::Internal(format!("cannot pick a port for opencode serve: {e}"))
        })?;
        let state_file = serve::state_path(&self.config.state_root, &session.name);
        let session_id = serve::read_state(&state_file).and_then(|s| s.session_id);
        serve::write_state(
            &state_file,
            &serve::ServeState {
                port,
                session_id,
                ready: false,
            },
        )
        .map_err(|e| {
            EngineError::Internal(format!("cannot write {}: {e}", state_file.display()))
        })?;

        let args: Vec<String> = vec![
            "serve".to_string(),
            "--hostname".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            port.to_string(),
        ];

        let mut env: Vec<(String, String)> = session
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        env.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(LaunchPlan {
            program: self.config.opencode_path.clone(),
            args,
            env,
            stdin: StdinMode::Null,
            pty: true,
        })
    }

    /// opencode sessions run the engine's own binary (the configured
    /// `opencode_path`), not the per-session agy/claude binaries.
    fn readiness(
        &self,
        session: &SessionDef,
        poll_interval: std::time::Duration,
        shutdown: &std::sync::atomic::AtomicBool,
        _paused: &std::sync::atomic::AtomicBool,
    ) -> (bool, String) {
        crate::orchestrate::readiness::binary_ready(
            &self.config.opencode_path,
            &session.env,
            poll_interval,
            shutdown,
        )
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
        // `revert`/`todo` are emulated by the E4 layer, never 501. The A2A
        // inject channels (KA-7) are `hook`, `tool_result` and `idle_turn`,
        // carried by the harness plugin + `mcp-a2a` config (SPEC §7.14).
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

    fn classify_status(&self, exit_code: Option<i32>, log: &str) -> JobStatus {
        crate::core::job::determine_status_from_opencode(exit_code, log)
    }

    fn extract_response(&self, log: &str) -> Option<String> {
        crate::core::job::extract_opencode_response(log)
    }

    fn extract_error(&self, log: &str) -> Option<String> {
        crate::core::job::extract_opencode_error(log)
    }

    fn extract_usage(&self, log: &str) -> Option<crate::engine::JobUsage> {
        crate::core::job::extract_opencode_usage(log)
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

    fn prepare_session(
        &self,
        session: &SessionDef,
        _cfg_path: &std::path::Path,
        _settings_path: &std::path::Path,
    ) -> Result<(), String> {
        // Materialise the opencode plugin and mcp.a2a config into ~/.config/opencode/
        // for the session environment (KA-7).
        let home = session
            .env
            .iter()
            .find(|(k, _)| k.as_str() == "HOME")
            .map(|(_, v)| std::path::PathBuf::from(v))
            .or_else(|| std::env::var("HOME").ok().map(std::path::PathBuf::from))
            .unwrap_or_else(|| std::path::PathBuf::from("/home/coder"));
        crate::worker::opencode_config::prepare_opencode_home(&home)
    }

    fn deliver_session_message(&self, session: &SessionDef, text: &str) -> Option<ServerDelivery> {
        let state_file = serve::state_path(&self.config.state_root, &session.name);
        match serve::deliver(&state_file, text, SESSION_DELIVERY_WAIT) {
            Ok(()) => Some(ServerDelivery::Ok),
            // A cold server (up but not yet answering) is retryable; a dead
            // server or any other failure keeps the historical 409 mapping.
            Err(e) if serve::is_not_ready(&e) => Some(ServerDelivery::NotReady(e)),
            Err(e) => Some(ServerDelivery::Err(e)),
        }
    }

    /// Starts the background warm-up for the just-spawned `opencode serve`: a
    /// thread tied to this spawn's stop flag that polls readiness with the long
    /// warm-up deadline and then creates (and persists) the opencode session,
    /// so the first user message never waits for a cold server.
    fn after_session_spawn(
        &self,
        session: &SessionDef,
        stop: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        let state_file = serve::state_path(&self.config.state_root, &session.name);
        if serve::read_state(&state_file).is_none() {
            return;
        }
        let stop = std::sync::Arc::clone(stop);
        let session_name = session.name.clone();
        let result = std::thread::Builder::new()
            .name(format!("opencode-warmup-{session_name}"))
            .spawn(move || {
                serve::warm_up(
                    &state_file,
                    &stop,
                    serve::WARMUP_DEADLINE,
                    std::time::Duration::from_millis(500),
                )
            })
            .ok()
            .and_then(|h| h.join().ok());
        if let Some(Err(e)) = result {
            crate::orchestrate::logging::log_event(
                &session_name,
                &format!("opencode serve warm-up failed: {e}"),
            );
        }
    }

    /// The session's engine is warm once the background warm-up has finished
    /// (the `ready` flag in the per-session state file).
    fn engine_ready(&self, session: &SessionDef) -> Option<bool> {
        let state_file = serve::state_path(&self.config.state_root, &session.name);
        serve::read_state(&state_file).map(|s| s.ready)
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
        // The A2A injection channels (KA-7): write the harness opencode plugin
        // (the `hook` channel) and merge the `mcp.a2a` local server (the
        // `tool_result` channel) into the opencode global config
        // (`~/.config/opencode/opencode.json`), idempotent. Same target as the
        // AGENTS.md materialisation below; never touches the user's committed
        // files. Best effort: an error is logged and the `tool_result` channel
        // still delivers.
        if let Err(e) = crate::worker::opencode_config::prepare_opencode_home(home) {
            log.push(format!(
                "ERROR: opencode A2A plugin/config write failed: {e} (continuing; tool_result channel still delivers)"
            ));
        }
        crate::core::instructions::materialize_opencode(
            home,
            workdir,
            global_text,
            per_session_text,
            log,
        )
    }
}

/// How long a session message waits for the supervised `opencode serve` to
/// answer. The background warm-up (see `after_session_spawn`) normally has the
/// server ready well before the first message; this bounds a delivery that
/// races a still-cold server and is answered with a retryable `503 not_ready`
/// when it elapses.
const SESSION_DELIVERY_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::capability::Capability;
    use crate::orchestrate::session_control::SessionControlRegistry;
    use crate::orchestrate::session_log::SessionLogs;
    use crate::orchestrate::sessions_api::SessionsApiState;
    use crate::orchestrate::state::{SessionState, SessionStatus, SupervisorState};
    use std::sync::Mutex;

    #[test]
    fn id_is_stable() {
        let engine = OpenCodeEngine::new(Arc::new(Config::default()));
        assert_eq!(engine.id(), "opencode");
    }

    #[test]
    fn run_id_and_session_name_derive_from_workdir() {
        assert_eq!(
            run_id_from_workdir(std::path::Path::new("/workspace/proj")),
            "proj"
        );
        assert_eq!(
            session_name_from_workdir(std::path::Path::new("/workspace/proj")),
            "proj"
        );
        assert_eq!(
            session_name_from_workdir(std::path::Path::new("/")),
            "opencode-job"
        );
    }

    #[test]
    fn session_ops_without_supervisor_answer_unsupported() {
        let engine = OpenCodeEngine::new(Arc::new(Config::default()));
        let mut ctx = crate::engine::EmptyEngineCtx;
        let spec = SessionSpec {
            workdir: PathBuf::from("/workspace/proj"),
            transport: None,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let err = rt
            .block_on(engine.start_session(spec, &mut ctx))
            .unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Unsupported { endpoint, .. } if endpoint == "session.start"
            ),
            "expected session.start unsupported, got {err:?}"
        );

        let run = Run::new("sess-1", RunKind::Session, "/workspace/proj");
        let err = rt.block_on(engine.send_message(&run, "hi")).unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Unsupported { endpoint, .. } if endpoint == "session.message"
            ),
            "expected session.message unsupported, got {err:?}"
        );

        let err = rt.block_on(engine.stop_session(&run, false)).unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Unsupported { endpoint, .. } if endpoint == "session.stop"
            ),
            "expected session.stop unsupported, got {err:?}"
        );

        let err = engine.is_idle(&run).unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Unsupported { endpoint, .. } if endpoint == "a2a.inject.idle_turn"
            ),
            "expected is_idle unsupported, got {err:?}"
        );

        let err = rt.block_on(engine.idle_turn(&run, "msg")).unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Unsupported { endpoint, .. } if endpoint == "a2a.inject.idle_turn"
            ),
            "expected idle_turn unsupported, got {err:?}"
        );
    }

    #[test]
    fn session_ops_with_supervisor_adapter() {
        let registry = SessionControlRegistry::new();
        let state = Arc::new(Mutex::new(SupervisorState::new(1)));
        {
            let mut st = state.lock().unwrap();
            let mut sess = SessionState::new("sess-1".to_string());
            sess.status = SessionStatus::Running;
            st.sessions.insert("sess-1".to_string(), sess);
        }
        let adapter = ClaudePtyAdapter {
            state: SessionsApiState::new(state, registry, Arc::new(SessionLogs::new(1024))),
            claude_dir: PathBuf::from("/home/agent/.opencode"),
        };

        let engine = OpenCodeEngine::with_pty(Arc::new(Config::default()), Some(adapter));
        let run = Run::new("sess-1", RunKind::Session, "/workspace/proj");

        // is_idle checks SessionStatus::Running
        let idle = engine.is_idle(&run).expect("is_idle query");
        assert!(idle, "running session must be idle between turns");

        let stopped_run = Run::new("sess-missing", RunKind::Session, "/workspace/proj");
        let idle_missing = engine.is_idle(&stopped_run).expect("is_idle for missing");
        assert!(!idle_missing, "missing/non-running session is not idle");
    }

    #[test]
    fn test_opencode_session_command() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            state_root: state_dir.path().to_path_buf(),
            ..Config::default()
        };
        let engine = OpenCodeEngine::new(Arc::new(cfg));
        let mut env_map = std::collections::HashMap::new();
        env_map.insert("BBB".to_string(), "2".to_string());
        env_map.insert("AAA".to_string(), "1".to_string());

        let session = SessionDef {
            name: "test-sess".to_string(),
            workdir: PathBuf::from("/workspace/test-repo"),
            session_id: "sess-123".to_string(),
            engine: Some("opencode".to_string()),
            claude_binary: PathBuf::from("claude"),
            claude_args: Vec::new(),
            agy_binary: PathBuf::from("agy"),
            env: env_map,
            session_file: None,
            transcript_path: None,
            memory_file: None,
            instructions_file: None,
            readiness_poll_secs: 5.0,
            hang_timeout_secs: 0.0,
            kill_grace_secs: 5.0,
            pty_eof_grace_secs: 10.0,
            kill_wait_secs: 15.0,
            sandbox: true,
            grants: None,
            base_backoff_secs: 2.0,
            max_backoff_secs: 60.0,
            healthy_run_secs: 30.0,
            max_crashes: 5,
            crash_window_secs: 60.0,
            prefix_output: false,
            failed_cooldown_secs: 300.0,
            backoff_window_secs: 600.0,
            resume_fail_quarantine_after: 5,
            resume_fail_uptime_secs: 5.0,
        };

        let plan = engine
            .session_command(&session, ResumeMode::Start)
            .expect("session_command Start");
        assert_eq!(plan.program, engine.config.opencode_path);
        // `opencode serve` on loopback with the port chosen for this start; the
        // port is recorded in the per-session state file.
        assert_eq!(
            &plan.args[..4],
            ["serve", "--hostname", "127.0.0.1", "--port"]
        );
        let port: u16 = plan.args[4].parse().expect("a numeric port");
        let state = serve::read_state(&serve::state_path(&engine.config.state_root, "test-sess"))
            .expect("the state file of the session");
        assert_eq!(state.port, port);
        assert!(state.session_id.is_none());
        assert!(plan.pty);
        assert_eq!(plan.stdin, StdinMode::Null);
        assert_eq!(
            plan.env,
            vec![
                ("AAA".to_string(), "1".to_string()),
                ("BBB".to_string(), "2".to_string())
            ]
        );

        // A restart keeps the opencode session id and takes a fresh port.
        serve::write_state(
            &serve::state_path(&engine.config.state_root, "test-sess"),
            &serve::ServeState {
                port,
                session_id: Some("ses_keep".to_string()),
                ready: true,
            },
        )
        .unwrap();
        let again = engine
            .session_command(&session, ResumeMode::Resume)
            .unwrap();
        let state2 =
            serve::read_state(&serve::state_path(&engine.config.state_root, "test-sess")).unwrap();
        assert_eq!(state2.session_id.as_deref(), Some("ses_keep"));
        assert_eq!(again.args[4], state2.port.to_string());
    }

    #[test]
    fn capabilities_honest_transport_and_keys() {
        let engine = OpenCodeEngine::new(Arc::new(Config::default()));
        let table = engine.capabilities();
        assert_eq!(table.engine, "opencode");
        assert_eq!(table.transport, None);
        assert_eq!(table.capability("job.start"), Some(Capability::Native));
        assert_eq!(table.capability("job.status"), Some(Capability::Native));
        assert_eq!(table.capability("a2a.inject"), Some(Capability::Native));
        assert_eq!(
            table.capability("session.start"),
            Some(Capability::Emulated)
        );
    }
}

//! The agy engine (design HL-91 section 4, item E3): the agy job driver
//! behind the [`Engine`] trait.
//!
//! This is migration step M2 of the design, scoped as a THIN ADAPTER: every
//! method delegates to the exact function the existing job command layer
//! (`core::commands::*`, `core::runner`) calls, so no behaviour moves and no
//! existing endpoint changes. Nothing is rewritten -- a job launched through
//! the engine produces the byte-identical HTTP bodies, the 137/143 taxonomy,
//! the sandbox wrapping and the launch handshake as the `commands::start`
//! path it wraps.
//!
//! The job driver is synchronous: the job monitor (`run_internal_runner`)
//! runs on an in-process OS thread, the launch handshake is a blocking poll,
//! and status classification reads the job store. The [`Engine`] trait is
//! async, so its job methods load the run's row and forward to the same sync
//! helpers -- one source of truth, no fork in behaviour between the two
//! entry points.
//!
//! `AgyEngine` carries only the process-wide [`Config`] (paths, the agy
//! binary, the sandbox launcher, the perf limits); per-run state always lives
//! in the job store (`core::job`), so the engine is stateless and cheaply
//! shared behind an `Arc`.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::core::config::Config;
use crate::core::job::{compute_status, JobMetadata, JobStatus};
use crate::core::runner::launch_background_job;

use crate::engine::capability::{Capability, CapabilityTable};
use crate::engine::run::{ContextId, EngineEvent, ExitInfo, Role, Run, RunId, RunStatus};
use crate::engine::{
    Engine, EngineCtx, EngineError, JobSpec, LaunchPlan, ResumeMode, SessionDef, SessionSpec,
    StdinMode, TurnInput,
};
use std::path::PathBuf;

/// The stable agy engine id (the value `config`/request `engine` bodies and
/// the capability tables carry).
pub const AGY_ENGINE_ID: &str = "agy";

/// The agy job engine: a thin wrapper over the existing agy job driver.
pub struct AgyEngine {
    config: Arc<Config>,
}

impl AgyEngine {
    /// Constructs the engine over the process-wide config.
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// The process-wide config this engine runs jobs under.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// A registry carrying the agy engine (the per-role default for jobs). The
    /// worker role builds this at startup so `/api/v1/jobs` and
    /// `/api/v1/capabilities` resolve it through the trait. When the config
    /// sets `default_engine` (HL-91 E1, selection precedence level 2) it
    /// becomes the registry's job fallback (level 3 is per-role "agy").
    ///
    /// The E4 emulation layer is armed for every control endpoint the agy
    /// engine lacks natively (fork/compact/revert, plus interrupt/todo from
    /// the base wrapper) so the shared control routes answer them as
    /// `emulated` -- never 501 because of the engine (HL-71 S2). `pause` is
    /// declared native by the engine even though the trait default is
    /// `Unsupported`; the config-attached emulation wrapper also fronts it, so
    /// the two tables are kept in sync by the worker's `enable_all_emulated`.
    pub fn build_registry(config: Arc<Config>) -> crate::engine::EngineRegistry {
        let mut registry = crate::engine::EngineRegistry::new();
        registry.register(Arc::new(Self::new(config.clone())));
        // Arm the E4 emulation for endpoints the agy engine has no native
        // implementation for (fork/compact/revert; interrupt/todo already
        // emulated by the wrapper's base table). The worker entry point then
        // runs `enable_all_emulated` again -- idempotent.
        registry.enable_all_emulated();
        if let Some(default) = config
            .default_engine
            .clone()
            .filter(|s| !s.trim().is_empty())
        {
            registry.set_default(crate::engine::RunKind::Job, default);
        }
        registry
    }

    // ---- sync driver (used by the async trait impl and, unchanged, by the
    //     job command layer; each method calls the SAME function the command
    //     calls directly) ---------------------------------------------------

    /// Launches a job row in the background and returns the confirmed agy
    /// PID. Delegates to the shared launch path
    /// ([`launch_background_job`]) exactly as `commands::start` and
    /// `commands::retry` do -- the command layer builds the `JobMetadata`
    /// row (worktree, branch, prompt, ...), and this drives the agy spawn +
    /// monitor thread.
    pub fn launch(&self, meta: &mut JobMetadata) -> Result<u32> {
        launch_background_job(&self.config, meta)
    }

    /// The job's current status, classified from the row + log by the agy
    /// exit/log taxonomy ([`compute_status`]).
    pub(crate) fn classify(&self, meta: &JobMetadata) -> JobStatus {
        compute_status(meta, &self.config.state_root)
    }

    /// Structured events for a job, parsed from its stream-json log (the
    /// typed surface the trait exposes; the raw `/log` byte contract stays
    /// engine-independent). Each `init` / `step_update` / `result` NDJSON
    /// line becomes an [`EngineEvent`]; non-event lines are skipped. `after`
    /// is accepted for signature compatibility; the E3 adapter is
    /// cursor-ignoring (cursor semantics arrive with the emulation layer).
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
            match value.get("event").and_then(|e| e.as_str()) {
                Some("init") => events.push(EngineEvent::Status(RunStatus::Running)),
                Some("step_update") => {
                    let text_delta = value
                        .get("step_update")
                        .and_then(|s| s.get("text_delta"))
                        .and_then(|t| t.as_str())
                        .unwrap_or_default();
                    if !text_delta.is_empty() {
                        events.push(EngineEvent::MessageDelta {
                            role: Role::Assistant,
                            text: text_delta.to_string(),
                        });
                    }
                }
                Some("result") => {
                    let inner = value.get("result");
                    let ok = inner.and_then(|r| r.get("status")).and_then(|s| s.as_str())
                        == Some("SUCCESS");
                    let response = inner
                        .and_then(|r| r.get("response"))
                        .and_then(|r| r.as_str())
                        .unwrap_or_default();
                    let done = if ok {
                        RunStatus::Done
                    } else {
                        RunStatus::Failed
                    };
                    if !response.is_empty() {
                        events.push(EngineEvent::MessageDelta {
                            role: Role::Assistant,
                            text: response.to_string(),
                        });
                    }
                    events.push(EngineEvent::Done { status: done });
                }
                _ => {}
            }
        }
        Ok(events)
    }
}

/// Maps the agy job taxonomy to the engine [`RunStatus`]. Total over
/// [`JobStatus`], keeping the 137/143-relevant `Killed` distinct.
impl From<JobStatus> for RunStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Running => RunStatus::Running,
            JobStatus::Done => RunStatus::Done,
            JobStatus::Failed => RunStatus::Failed,
            JobStatus::SelfTerminated => RunStatus::Done,
            JobStatus::Killed => RunStatus::Killed,
            JobStatus::Incomplete => RunStatus::Failed,
            JobStatus::QuotaExhausted => RunStatus::Failed,
        }
    }
}

#[async_trait]
impl Engine for AgyEngine {
    fn id(&self) -> String {
        AGY_ENGINE_ID.to_string()
    }

    async fn start_job(
        &self,
        spec: JobSpec,
        ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        // The agy job launch needs the fully-built `JobMetadata` row (worktree,
        // branch, prompt, hindsight, ...) that the job command layer
        // (`commands::start`) constructs before launching; the row is not
        // derivable from the trait's `JobSpec` (a superset lives in the state
        // store). Until E5 makes the row build engine-independent, today's
        // `/api/v1/jobs` endpoint drives the engine's sync driver
        // (`AgyEngine::launch`) through `commands::start` and never reaches
        // this async seam. It therefore answers `Internal` (never 501): this
        // is a "not wired yet" state, not a gap in the engine's capabilities.
        let _ = (spec, ctx);
        Err(EngineError::Internal(
            "agy start_job is driven by the job command layer (built JobMetadata row)".to_string(),
        ))
    }

    async fn resume_job(&self, run: &Run, prompt: &str) -> Result<(), EngineError> {
        // Same wiring note as `start_job`: `commands::retry` drives the resume
        // path through the engine's sync driver; this async seam becomes
        // reachable when the endpoint layer routes through the trait (E5).
        let _ = (run, prompt);
        Err(EngineError::Internal(
            "agy resume_job is driven by the job command layer (built JobMetadata row)".to_string(),
        ))
    }

    async fn job_status(&self, run: &Run) -> Result<RunStatus, EngineError> {
        let meta = JobMetadata::load(&self.config.state_root, &run.id)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        Ok(self.classify(&meta).into())
    }

    fn is_idle(&self, run: &Run) -> Result<bool, EngineError> {
        // A finished job is idle (the next turn can be started with the
        // message); a RUNNING job is idle only while a live stdin channel can
        // carry the framed message (§7.14). Mirrors the worker sink's
        // `idle_turn_available`.
        let meta = JobMetadata::load(&self.config.state_root, &run.id)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        if compute_status(&meta, &self.config.state_root) != JobStatus::Running {
            return Ok(true);
        }
        Ok(self.config.a2a_live_stdin && crate::core::a2a_live::lookup(&run.id).is_some())
    }

    async fn idle_turn(&self, run: &Run, joined: &str) -> Result<(), EngineError> {
        // The `idle_turn` step of §7.14: the message is started as the run's
        // NEXT turn. A finished job is resumed with the joined framed text; a
        // RUNNING live-channel job gets it over the pipe. Never kills.
        let meta = JobMetadata::load(&self.config.state_root, &run.id)
            .map_err(|e| EngineError::Internal(e.to_string()))?;
        if compute_status(&meta, &self.config.state_root) == JobStatus::Running {
            if !self.config.a2a_live_stdin {
                return Err(EngineError::unsupported(self.id(), "a2a.inject.idle_turn"));
            }
            if let Some(channel) = crate::core::a2a_live::lookup(&run.id) {
                if channel.is_alive_for(meta.pid, meta.pid_start_time) {
                    if channel.deliver_line(joined).is_ok() {
                        channel.touch();
                        return Ok(());
                    }
                    crate::core::a2a_live::close_for(&run.id);
                }
            }
            return Err(EngineError::Internal(
                "no live stdin channel for the RUNNING job".to_string(),
            ));
        }
        // A finished job is resumed with the message as the follow-up turn.
        // The resume goes through `commands::retry` with a follow-up prompt,
        // which continues the SAME conversation (conversation_id) with the new
        // turn; it never marks the job user-killed or failed. (A RUNNING job
        // was already handled above; the worker's `resume_with` additionally
        // stops the running attempt, but the `idle_turn` step never needs to
        // kill -- the live-channel path is the only RUNNING case.)
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
        spec: SessionSpec,
        ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        // agy has no session mode; sessions (E5) run on the claude engine.
        let _ = (spec, ctx);
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
        let live = turn.is_live()
            || (self.config.a2a_live_stdin
                && (crate::core::jobset::host_jobset().is_some()
                    || crate::core::a2a_live::live_member(&self.config.state_root, &job.job_id)));

        let mut args: Vec<String> = vec![
            "--dangerously-skip-permissions".to_string(),
            "--mode".to_string(),
            job.mode.clone(),
            "--output-format".to_string(),
            "stream-json".to_string(),
        ];

        let stdin = match turn {
            TurnInput::Start { prompt, .. } => {
                if live {
                    args.push("--print".to_string());
                    args.push("".to_string());
                    args.push("--input-format".to_string());
                    args.push("stream-json".to_string());
                    StdinMode::Pipe {
                        first_line: if prompt.is_empty() {
                            None
                        } else {
                            Some(prompt.to_string())
                        },
                    }
                } else {
                    if !prompt.is_empty() {
                        args.push("-p".to_string());
                        args.push(prompt.to_string());
                    }
                    StdinMode::Null
                }
            }
            TurnInput::Resume {
                prompt,
                conversation,
                ..
            } => {
                if let Some(conv_id) = conversation {
                    args.push("--conversation".to_string());
                    args.push(conv_id.to_string());
                } else {
                    args.push("--continue".to_string());
                }
                if live {
                    args.push("--print".to_string());
                    args.push("".to_string());
                    args.push("--input-format".to_string());
                    args.push("stream-json".to_string());
                    StdinMode::Pipe {
                        first_line: if prompt.is_empty() {
                            None
                        } else {
                            Some(prompt.to_string())
                        },
                    }
                } else {
                    if !prompt.is_empty() {
                        args.push("-p".to_string());
                        args.push(prompt.to_string());
                    }
                    StdinMode::Null
                }
            }
        };

        if let Some(m) = &job.model {
            args.push("--model".to_string());
            args.push(m.clone());
        }

        if let Some(e) = &job.effort {
            args.push("--effort".to_string());
            args.push(e.clone());
        }

        if job.sandbox {
            args.push("--sandbox".to_string());
        }

        args.push("--add-dir".to_string());
        args.push(job.worktree_path.to_string_lossy().to_string());

        for dir in &job.add_dirs {
            args.push("--add-dir".to_string());
            args.push(dir.to_string_lossy().to_string());
        }

        if let Some(timeout) = &job.print_timeout {
            args.push("--print-timeout".to_string());
            args.push(timeout.clone());
        }

        Ok(LaunchPlan {
            program: self.config.agy_path.clone(),
            args,
            env: Vec::new(),
            stdin,
            pty: false,
        })
    }

    fn session_command(
        &self,
        session: &SessionDef,
        _resume: ResumeMode,
    ) -> Result<LaunchPlan, EngineError> {
        let args = vec![
            "--dangerously-skip-permissions".to_string(),
            "--mode".to_string(),
            "accept-edits".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--print".to_string(),
            "".to_string(),
            "--input-format".to_string(),
            "stream-json".to_string(),
            "--add-dir".to_string(),
            session.workdir.to_string_lossy().to_string(),
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
        crate::core::job::determine_status_from_signals(exit.code, &log)
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
        // The agy job endpoints are native; the control surface the E4
        // emulation layer fronts (interrupt via kill, the session surface for
        // E5 cross-use, mid-run messages) is reported emulated from the start;
        // permissions are unsupported-by-design (headless auto-approve) -- a
        // documented gap, never a 501.
        let native: &[crate::engine::capability::CapEndpointKey] = &[
            "job.start",
            "job.status",
            "job.list",
            "job.kill",
            "job.retry",
            "job.events",
            "job.result",
            "workspace.*",
            // Design HL-113 §7.14: the harness puts queued A2A messages into
            // the model's context (the mcp-a2a tool_result channel); the
            // supported channels are listed in `inject_channels`.
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
        // the declared sets mirror the E1 no-op fixture exactly.
        let _ = Capability::Emulated;
        CapabilityTable::builder(AGY_ENGINE_ID)
            .native(native)
            .emulated(emulated)
            .build()
    }

    fn display_name(&self) -> &str {
        "agy"
    }

    fn normalize_prompt(&self, prompt: &str, job_id: &str, retry_count: u32) -> String {
        crate::core::agy_conversations::normalize_ref_prompt(prompt, job_id, retry_count)
    }

    fn configured_program(&self, config: &crate::core::config::Config) -> PathBuf {
        config.agy_path.clone()
    }

    fn prepare_session(
        &self,
        _session: &SessionDef,
        _cfg_path: &std::path::Path,
        _settings_path: &std::path::Path,
    ) -> Result<(), String> {
        Ok(())
    }

    fn readiness(
        &self,
        session: &SessionDef,
        poll_interval: std::time::Duration,
        shutdown: &std::sync::atomic::AtomicBool,
        _paused: &std::sync::atomic::AtomicBool,
    ) -> (bool, String) {
        crate::orchestrate::readiness::binary_ready(
            &session.agy_binary,
            &session.env,
            poll_interval,
            shutdown,
        )
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
        let line = crate::core::a2a_live::frame_user_line(&String::from_utf8_lossy(payload));
        let mut frame = Vec::with_capacity(line.len() + 1);
        frame.extend_from_slice(line.as_bytes());
        frame.push(b'\r');
        frame
    }

    fn materialize_instructions(
        &self,
        home: &std::path::Path,
        _workdir: &std::path::Path,
        global_text: Option<(PathBuf, String)>,
        per_session_text: Option<(PathBuf, String)>,
        log: &mut Vec<String>,
    ) -> bool {
        crate::core::instructions::materialize_agy(home, global_text, per_session_text, log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::fs;
    #[cfg(feature = "test-hooks")]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// A minimal job row for launch/classify tests (mirrors `runner.rs`'s
    /// test meta shape).
    fn test_meta(job_id: &str, worktree: &std::path::Path) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            project: "project-a".to_string(),
            prompt: "Test prompt".to_string(),
            resolved_prompt: "Test prompt".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: worktree.to_path_buf(),
            branch: format!("agent-run/{job_id}"),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: None,
            status: None,
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        }
    }

    #[test]
    fn job_status_maps_taxonomy_to_run_status() {
        // The mapping from the agy `JobStatus` taxonomy to the engine
        // `RunStatus` is total and keeps the 137/143-relevant Killed distinct.
        let cases: [(JobStatus, RunStatus); 7] = [
            (JobStatus::Running, RunStatus::Running),
            (JobStatus::Done, RunStatus::Done),
            (JobStatus::Failed, RunStatus::Failed),
            (JobStatus::Killed, RunStatus::Killed),
            (JobStatus::SelfTerminated, RunStatus::Done),
            (JobStatus::Incomplete, RunStatus::Failed),
            (JobStatus::QuotaExhausted, RunStatus::Failed),
        ];
        for (job, run) in cases {
            let mapped: RunStatus = job.into();
            assert_eq!(mapped, run);
        }
    }

    #[test]
    fn capabilities_report_native_job_endpoints() {
        // The agy engine's own capability table: job endpoints native, pause native
        // (SPEC §3.4), a2a.send_message native, the E4 control surface emulated,
        // permissions unsupported (never a 501).
        let engine = AgyEngine::new(Arc::new(Config::default()));
        let table = engine.capabilities();
        assert_eq!(table.engine, "agy");
        assert_eq!(table.capability("job.start"), Some(Capability::Native));
        assert_eq!(table.capability("job.kill"), Some(Capability::Native));
        assert_eq!(
            table.capability("session.start"),
            Some(Capability::Emulated)
        );
        assert_eq!(table.capability("interrupt"), Some(Capability::Emulated));
        assert_eq!(table.capability("pause"), Some(Capability::Emulated));
        assert_eq!(
            table.capability("permissions"),
            Some(Capability::Unsupported)
        );
        assert_eq!(table.capability("fork"), Some(Capability::Emulated));
        assert_eq!(
            table.capability("a2a.send_message"),
            Some(Capability::Emulated)
        );
        assert_eq!(table.transport, None);
    }

    #[test]
    fn registry_resolves_agy_and_rejects_unknown_engine() {
        let registry = AgyEngine::build_registry(Arc::new(Config::default()));
        assert!(registry.contains("agy"));
        let resolved = registry
            .resolve(crate::engine::run::RunKind::Job, Some("agy"), None)
            .expect("agy must resolve");
        assert_eq!(resolved.id(), "agy");
        // The per-role default (no request/config) is agy for jobs.
        let resolved = registry
            .resolve(crate::engine::run::RunKind::Job, None, None)
            .expect("default job engine must resolve");
        assert_eq!(resolved.id(), "agy");
        // A named-but-unregistered engine is a selection error (400), never a
        // 501 (design 2.4 hard rule).
        let err = registry
            .resolve(crate::engine::run::RunKind::Job, Some("claude"), None)
            .err()
            .expect("unregistered engine must not resolve");
        assert!(matches!(err, EngineError::UnknownEngine { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn classify_reads_job_row_via_compute_status() {
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        fs::create_dir_all(&state_root).unwrap();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();

        let engine = AgyEngine::new(Arc::new(Config {
            state_root: state_root.clone(),
            ..Default::default()
        }));

        // A row with a live PID (this test process) classifies running.
        let job_id = format!("cls{}", std::process::id());
        let mut meta = test_meta(&job_id, &wt);
        meta.pid = Some(std::process::id());
        meta.save(&state_root).unwrap();
        assert_eq!(engine.classify(&meta), JobStatus::Running);

        // Once the process is gone with exit 0 but no real response was
        // recorded, `compute_status` classifies it incomplete.
        meta.pid = None;
        meta.exit_code = Some(0);
        meta.save(&state_root).unwrap();
        assert_eq!(engine.classify(&meta), JobStatus::Incomplete);

        // The trait surface produces the engine `RunStatus` from the row.
        let run = Run::new(job_id.clone(), crate::engine::run::RunKind::Job, "/w");
        let status = engine.job_status(&run).await.expect("job_status");
        assert_eq!(status, RunStatus::Failed);
    }

    #[test]
    fn events_parse_stream_json_log_lines() {
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        fs::create_dir_all(&state_root).unwrap();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();

        let engine = AgyEngine::new(Arc::new(Config {
            state_root: state_root.clone(),
            ..Default::default()
        }));

        let job_id = format!("evx{}", std::process::id());
        test_meta(&job_id, &wt).save(&state_root).unwrap();
        let log_path = JobMetadata::log_path(&state_root, &job_id);
        fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        fs::write(
            &log_path,
            "\
some non-json noise line
{\"event\":\"init\",\"init\":{\"conversation_id\":\"c1\"}}
{\"event\":\"step_update\",\"step_update\":{\"step_index\":1,\"state\":\"DONE\",\"step_type\":\"agent_response\",\"text_delta\":\"hello world\"}}
{\"event\":\"result\",\"result\":{\"conversation_id\":\"c1\",\"status\":\"SUCCESS\",\"response\":\"done\"}}
",
        )
        .unwrap();

        let run = Run {
            id: job_id.clone(),
            engine: "agy".to_string(),
            transport: None,
            kind: crate::engine::run::RunKind::Job,
            context_id: Some("c1".to_string()),
            status: RunStatus::Done,
            workdir: PathBuf::from("/w"),
            started_at: None,
            ended_at: None,
            exit: None,
            events_seq: 0,
            meta: serde_json::Value::Object(Default::default()),
        };
        let events = engine.events_of(&run, 0).unwrap();
        assert!(
            events.iter().any(|e| matches!(
                e,
                EngineEvent::MessageDelta { text, .. } if text == "hello world"
            )),
            "expected the step_update text delta, got {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, EngineEvent::Done { status } if *status == RunStatus::Done)),
            "expected a Done event, got {events:?}"
        );
    }

    #[cfg(feature = "test-hooks")]
    fn poll_until(deadline_ms: u64, mut check: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(deadline_ms);
        while std::time::Instant::now() < deadline {
            if check() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        check()
    }

    /// Hermetic end-to-end across the engine's sync launch driver (`launch`,
    /// the exact path `commands::start`/`commands::retry` use): a fake agy
    /// that records its argv must be spawned with the exact agy contract
    /// arguments. Runs under `test-hooks` where the sandbox passthrough seam
    /// (default ON) returns the plain agy argv, so the assertion sees the
    /// real command line.
    #[cfg(feature = "test-hooks")]
    #[test]
    fn start_path_spawns_agy_with_exact_argv() {
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        let argvfile = tmp.path().join("fake-agy.argv");
        let pidfile = tmp.path().join("fake-agy.pid");
        // Record argv (one element per line) so the test can assert the exact
        // command, then park until the launcher confirms the PID (saving the
        // pid lets the test kill the process group cleanly at the end).
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\necho $$ > {}\nsleep 30\n",
                argvfile.display(),
                pidfile.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let engine = AgyEngine::new(Arc::new(Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy.clone(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            agy_home: Some(tmp.path().join("agy-home")),
            ..Default::default()
        }));

        let job_id = format!("agyyargv{}", std::process::id());
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        let mut meta = test_meta(&job_id, &wt);
        meta.resolved_prompt = "do the thing".to_string();

        // Route the launch through the engine (the same function the commands
        // use).
        let pid = engine.launch(&mut meta).expect("launch through the engine");
        assert!(pid > 1, "the engine must return a real agy PID");

        let argv = poll_until(4000, || fs::read_to_string(&argvfile).is_ok())
            .then(|| fs::read_to_string(&argvfile).unwrap())
            .expect("fake agy must record its argv");
        let args: Vec<&str> = argv.lines().collect();
        assert!(
            args.contains(&"--dangerously-skip-permissions"),
            "agy argv missing --dangerously-skip-permissions: {args:?}"
        );
        assert!(
            args.iter()
                .position(|a| *a == "-p")
                .and_then(|i| args.get(i + 1).copied())
                .map(|s| s.contains("do the thing"))
                .unwrap_or(false),
            "agy argv must pass -p <prompt>: {args:?}"
        );
        assert!(
            args.iter()
                .position(|a| *a == "--add-dir")
                .and_then(|i| args.get(i + 1).copied())
                .map(|s| s.contains(wt.to_string_lossy().as_ref()))
                .unwrap_or(false),
            "agy argv must pass the worktree as --add-dir: {args:?}"
        );

        // Clean up: terminate the spawned fake agy process group so the test
        // host is left clean (the job monitor thread reaps the child).
        let _ = crate::core::commands::kill::send_signal(pid, "KILL");
        crate::core::reaper::untrack_child(pid);
    }

    /// HL-71 S2 parity: through the exact production registry wiring
    /// (`build_registry` + claude registration + `attach_config` +
    /// `enable_all_emulated`, mirroring `worker/entry.rs`), agy and claude
    /// report the SAME effective capability for every job-control endpoint
    /// the emulation layer fronts -- `Native` for the proven job lifecycle,
    /// `Emulated` for interrupt/pause/message/fork/compact/revert/todo. No
    /// control endpoint may be `Unsupported` for one engine and declared for
    /// the other (owner rule: no job-control route answers 501 because of the
    /// engine). Table-driven over the endpoint keys.
    #[test]
    fn agy_claude_job_control_parity_after_enable_all_emulated() {
        use crate::engine::capability::CapEndpointKey;
        use crate::engine::capability::Capability;
        use crate::engine::claude::ClaudeEngine;

        let config = Arc::new(Config::default());
        let mut registry = AgyEngine::build_registry(Arc::clone(&config));
        registry.register(Arc::new(ClaudeEngine::new(
            std::path::PathBuf::from("claude"),
            None,
        )));
        registry.attach_config(Arc::clone(&config));
        registry.enable_all_emulated();

        // Job lifecycle endpoints stay native for both (parity with the
        // black-box suite's T172-T180 proof for claude and T02-T46 for agy).
        for key in [
            "job.start",
            "job.status",
            "job.list",
            "job.kill",
            "job.retry",
            "job.events",
            "job.result",
            "workspace.*",
        ] {
            let agy_cap = registry
                .emulating("agy")
                .expect("agy wrapper")
                .capability(key);
            let claude_cap = registry
                .emulating("claude")
                .expect("claude wrapper")
                .capability(key);
            assert_eq!(
                agy_cap, claude_cap,
                "parity: agy and claude must both report the same effective capability for '{key}' (agy={agy_cap:?} claude={claude_cap:?})"
            );
            assert_eq!(
                agy_cap,
                Capability::Native,
                "proven job endpoint '{key}' must stay native for agy"
            );
        }

        // The emulated job-control surface: identical, non-unsupported
        // declarations for both engines.
        let control_keys: &[CapEndpointKey] = &[
            "interrupt",
            "pause",
            "a2a.send_message",
            "fork",
            "compact",
            "revert",
            "todo",
        ];
        for key in control_keys {
            let agy_cap = registry
                .emulating("agy")
                .expect("agy wrapper")
                .capability(key);
            let claude_cap = registry
                .emulating("claude")
                .expect("claude wrapper")
                .capability(key);
            assert_eq!(
                agy_cap, claude_cap,
                "parity: agy and claude must declare '{key}' identically (agy={agy_cap:?} claude={claude_cap:?})"
            );
            assert_ne!(
                agy_cap,
                Capability::Unsupported,
                "owner rule: job-control endpoint '{key}' must never be unsupported for agy"
            );
            assert_ne!(
                claude_cap,
                Capability::Unsupported,
                "owner rule: job-control endpoint '{key}' must never be unsupported for claude"
            );
        }
    }

    #[test]
    fn test_agy_job_command_start_normal_pinned() {
        use std::path::Path;
        let config = Arc::new(Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            ..Config::default()
        });
        let engine = AgyEngine::new(config);
        let mut meta = test_meta("job-agy-start", Path::new("/tmp/wt"));
        meta.mode = "normal".to_string();
        meta.model = Some("gemini-2.5-flash".to_string());
        meta.effort = Some("medium".to_string());
        meta.sandbox = true;
        meta.add_dirs = vec![PathBuf::from("/extra/dir")];
        meta.print_timeout = Some("60s".to_string());

        let plan = engine
            .job_command(
                &meta,
                TurnInput::Start {
                    prompt: "initial task prompt",
                    live: false,
                },
            )
            .expect("plan");

        assert_eq!(plan.program, PathBuf::from("/usr/local/bin/agy"));
        assert_eq!(
            plan.args,
            vec![
                "--dangerously-skip-permissions",
                "--mode",
                "normal",
                "--output-format",
                "stream-json",
                "-p",
                "initial task prompt",
                "--model",
                "gemini-2.5-flash",
                "--effort",
                "medium",
                "--sandbox",
                "--add-dir",
                "/tmp/wt",
                "--add-dir",
                "/extra/dir",
                "--print-timeout",
                "60s",
            ]
        );
        assert_eq!(plan.env, Vec::<(String, String)>::new());
        assert_eq!(plan.stdin, StdinMode::Null);
        assert!(!plan.pty);
    }

    #[test]
    fn test_agy_job_command_resume_with_conversation_pinned() {
        use std::path::Path;
        let config = Arc::new(Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            ..Config::default()
        });
        let engine = AgyEngine::new(config);
        let mut meta = test_meta("job-agy-resume", Path::new("/tmp/wt"));
        meta.mode = "normal".to_string();

        let plan = engine
            .job_command(
                &meta,
                TurnInput::Resume {
                    prompt: "continue with next step",
                    conversation: Some("conv-12345"),
                    live: false,
                },
            )
            .expect("plan");

        assert_eq!(plan.program, PathBuf::from("/usr/local/bin/agy"));
        assert_eq!(
            plan.args,
            vec![
                "--dangerously-skip-permissions",
                "--mode",
                "normal",
                "--output-format",
                "stream-json",
                "--conversation",
                "conv-12345",
                "-p",
                "continue with next step",
                "--add-dir",
                "/tmp/wt",
            ]
        );
        assert_eq!(plan.stdin, StdinMode::Null);
        assert!(!plan.pty);
    }

    #[test]
    fn test_agy_job_command_resume_without_conversation_pinned() {
        use std::path::Path;
        let config = Arc::new(Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            ..Config::default()
        });
        let engine = AgyEngine::new(config);
        let mut meta = test_meta("job-agy-resume-bare", Path::new("/tmp/wt"));
        meta.mode = "plan".to_string();

        let plan = engine
            .job_command(
                &meta,
                TurnInput::Resume {
                    prompt: "continue prompt",
                    conversation: None,
                    live: false,
                },
            )
            .expect("plan");

        assert_eq!(
            plan.args,
            vec![
                "--dangerously-skip-permissions",
                "--mode",
                "plan",
                "--output-format",
                "stream-json",
                "--continue",
                "-p",
                "continue prompt",
                "--add-dir",
                "/tmp/wt",
            ]
        );
        assert_eq!(plan.stdin, StdinMode::Null);
    }

    #[test]
    fn test_agy_job_command_live_mode_pinned() {
        use std::path::Path;
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().to_path_buf();
        let config = Arc::new(Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            state_root: state_root.clone(),
            a2a_live_stdin: true,
            ..Config::default()
        });
        let engine = AgyEngine::new(config);
        let mut meta = test_meta("job-live-1", Path::new("/tmp/wt"));
        meta.mode = "normal".to_string();

        // Mark as live member
        crate::core::a2a_live::set_live_member(&state_root, "job-live-1", true);

        let plan = engine
            .job_command(
                &meta,
                TurnInput::Start {
                    prompt: "live mesh prompt",
                    live: true,
                },
            )
            .expect("plan");

        assert_eq!(
            plan.args,
            vec![
                "--dangerously-skip-permissions",
                "--mode",
                "normal",
                "--output-format",
                "stream-json",
                "--print",
                "",
                "--input-format",
                "stream-json",
                "--add-dir",
                "/tmp/wt",
            ]
        );
        assert_eq!(
            plan.stdin,
            StdinMode::Pipe {
                first_line: Some("live mesh prompt".to_string()),
            }
        );
    }

    #[test]
    fn test_agy_classify_exit_and_conversation_id() {
        use std::path::Path;
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().to_path_buf();
        let config = Arc::new(Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            state_root: state_root.clone(),
            ..Config::default()
        });
        let engine = AgyEngine::new(config);

        let exit_0 = ExitInfo {
            code: Some(0),
            signal: None,
        };
        let exit_137 = ExitInfo {
            code: Some(137),
            signal: None,
        };

        assert_eq!(
            engine.classify_exit(exit_137, b"killed by sigkill"),
            JobStatus::SelfTerminated
        );

        let success_log =
            b"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"done\"}}\n";
        assert_eq!(engine.classify_exit(exit_0, success_log), JobStatus::Done);

        let mut meta = test_meta("job-conv", Path::new("/tmp/wt"));
        meta.conversation_id = Some("c-stored-123".to_string());
        assert_eq!(
            engine.conversation_id(&meta).as_deref(),
            Some("c-stored-123")
        );

        meta.conversation_id = None;
        let log_dir = JobMetadata::log_dir(&state_root);
        fs::create_dir_all(&log_dir).unwrap();
        let log_file = JobMetadata::log_path(&state_root, "job-conv");
        fs::write(&log_file, "Conversation ID: c-from-log-456\n").unwrap();
        assert_eq!(
            engine.conversation_id(&meta).as_deref(),
            Some("c-from-log-456")
        );
    }

    #[test]
    fn test_agy_session_command() {
        let config = Arc::new(Config::default());
        let engine = AgyEngine::new(config);

        let mut env_map = std::collections::HashMap::new();
        env_map.insert("FOO".to_string(), "bar".to_string());
        env_map.insert("ALPHA".to_string(), "1".to_string());

        let session = SessionDef {
            name: "test-sess".to_string(),
            workdir: PathBuf::from("/workspace/repo"),
            session_id: "sid-123".to_string(),
            engine: Some("agy".to_string()),
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: PathBuf::from("/opt/tools/agy"),
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

        let plan_start = engine
            .session_command(&session, ResumeMode::Start)
            .expect("session_command Start");
        assert_eq!(plan_start.program, PathBuf::from("/opt/tools/agy"));
        assert_eq!(
            plan_start.args,
            vec![
                "--dangerously-skip-permissions",
                "--mode",
                "accept-edits",
                "--output-format",
                "stream-json",
                "--print",
                "",
                "--input-format",
                "stream-json",
                "--add-dir",
                "/workspace/repo",
            ]
        );
        assert!(plan_start.pty);
        assert_eq!(plan_start.stdin, StdinMode::Null);
        assert_eq!(
            plan_start.env,
            vec![
                ("ALPHA".to_string(), "1".to_string()),
                ("FOO".to_string(), "bar".to_string())
            ]
        );

        let plan_resume = engine
            .session_command(&session, ResumeMode::Resume)
            .expect("session_command Resume");
        // agy has identical arguments on resume (stream-json idle PTY)
        assert_eq!(plan_resume.args, plan_start.args);
        assert_eq!(plan_resume.program, plan_start.program);
    }
}

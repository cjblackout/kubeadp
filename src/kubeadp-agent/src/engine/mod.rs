//! The pluggable engine layer (design HL-91 section 2).
//!
//! [`Engine`] is the object-safe, async, `Send + Sync` trait every harness
//! (claude, agy, later engines) implements. It has a job mode and a session
//! mode plus the shared control surface whose E4 emulation layer fronts. The
//! endpoint layer only ever talks to the trait (or to an emulation wrapper)
//! -- no endpoint may name `agy`/`claude` after E1.
//!
//! E1 wires the module tree, the run model, the capability table and the 501
//! plumbing. E3 adds the agy job engine ([`agy::AgyEngine`]) behind the trait
//! (design migration step M2): a thin adapter that delegates to the exact
//! `core::commands::*` / `core::runner` functions the job API already uses,
//! so no behaviour moves and no existing endpoint changes. The registry and
//! the selection precedence (request > config > per-role default) still
//! exist; the worker role registers the agy engine at startup so `/api/v1`
//! resolves and reports it. The claude engine and the emulation layer follow
//! in E2/E4.
//!
//! The module is behind the `server` feature so the pure `cli` build stays
//! untouched.

pub mod agy;
pub mod capability;
pub mod emulate;
pub mod errors;
pub mod run;

/// The claude engine (HL-91 E2): stream-json job driver, transcript parser
/// and the PTY session adapter over the supervisor machinery.
pub mod claude;

mod agnostic;

pub use agnostic::EmptyEngineCtx;
pub use agy::AgyEngine;
pub use errors::EngineError;
pub use run::{
    ContextId, EngineEvent, EngineId, ExitInfo, PodRole, Run, RunId, RunKind, RunStatus, Transport,
};

use crate::core::job::{JobMetadata, JobStatus};
use crate::engine::capability::CapabilityTable;
pub use crate::orchestrate::config::SessionConfig as SessionDef;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
/// Everything the trait methods need that today is globbed in `Config` /
/// `SupervisorConfig` (paths, perf limits, kill bounds, sandbox launcher),
/// kept behind an object-safe handle so trait methods stay testable without a
/// live pod. E1 carries an empty context; drivers populate it in E2/E3.
pub trait EngineCtx: Send + Sync {
    /// The engine selection requested for this run, if any (precedence
    /// level 1).
    fn requested_engine(&self) -> Option<&str> {
        None
    }
    /// The per-run configured engine (precedence level 2).
    fn configured_engine(&self) -> Option<&str> {
        None
    }
    /// The role hosting the run (precedence level 3 fallback).
    fn role(&self) -> PodRole {
        PodRole::Worker
    }
    /// The engine the run resolved to (filled by the registry on spawn).
    fn engine(&self) -> Option<&str> {
        None
    }
}

/// Specification for [`Engine::start_job`].
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct JobSpec {
    /// Prompt text to start the job with.
    pub prompt: String,
    /// `None` for a fresh job, `Some(conversation_id)` for a retry/resume.
    pub conversation_id: Option<ContextId>,
    /// The job mode string (`"normal"`, `"plan"`, ...).
    pub mode: String,
    /// The run's worktree.
    pub workdir: PathBuf,
    /// The project name (for git/registry bookkeeping).
    pub project: String,
    /// Engine-specific start options (opaque JSON; interpreted by the engine
    /// that resolves the run). Absent/`null` means "engine defaults".
    #[serde(default)]
    pub options: serde_json::Value,
}

/// Specification for [`Engine::start_session`].
#[derive(Debug, Clone)]
pub struct SessionSpec {
    /// The session's workspace directory.
    pub workdir: PathBuf,
    /// The claude transport to use for this session (`None` for engines
    /// without transports).
    pub transport: Option<Transport>,
}

/// Standard input delivery mode for [`LaunchPlan`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StdinMode {
    /// No stdin delivered (or /dev/null).
    Null,
    /// Piped stdin with an optional first line to write upon launch.
    Pipe { first_line: Option<String> },
}

/// Execution launch plan describing program, argv, env, stdin mode and PTY requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchPlan {
    /// Program executable path.
    pub program: PathBuf,
    /// Command line arguments.
    pub args: Vec<String>,
    /// Environment variables to set.
    pub env: Vec<(String, String)>,
    /// Standard input delivery mode.
    pub stdin: StdinMode,
    /// Whether execution requires allocation of a pseudo-terminal (PTY).
    pub pty: bool,
}

/// Turn input for [`Engine::job_command`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnInput<'a> {
    /// Initial start turn with prompt.
    Start { prompt: &'a str },
    /// Resumed/retry turn with prompt and optional conversation identifier.
    Resume {
        prompt: &'a str,
        conversation: Option<&'a str>,
    },
}

/// Resume mode for [`Engine::session_command`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeMode {
    /// Fresh start turn (e.g. `--session-id`).
    Start,
    /// Resumed turn (e.g. `--resume`).
    Resume,
}

/// The engine trait (design HL-91 2.2). Object-safe, async where needed,
/// `Send + Sync`. Methods without an explicit default return an
/// `Unsupported` error via [`agnostic`] so a partial engine implementation
/// still satisfies the conformance contract honestly.
#[async_trait]
pub trait Engine: Send + Sync {
    /// The stable engine identifier (`"claude"`, `"agy"`, ...).
    fn id(&self) -> EngineId;

    // ---- job mode -------------------------------------------------------

    /// Starts a job and returns the run id.
    async fn start_job(&self, spec: JobSpec, ctx: &mut dyn EngineCtx)
        -> Result<RunId, EngineError>;

    /// Resumes a running/stopped job with a continuation prompt.
    async fn resume_job(&self, run: &Run, prompt: &str) -> Result<(), EngineError>;

    /// The job's current status.
    async fn job_status(&self, run: &Run) -> Result<run::RunStatus, EngineError>;

    // ---- session mode ----------------------------------------------------

    /// Starts a session and returns the run id.
    async fn start_session(
        &self,
        spec: SessionSpec,
        ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError>;

    /// Delivers a message into a running session.
    async fn send_message(&self, run: &Run, text: &str) -> Result<(), EngineError>;

    /// Stops (or stops-and-arms-for-restart) a session.
    async fn stop_session(&self, run: &Run, restart: bool) -> Result<(), EngineError>;

    // ---- launch description ----------------------------------------------

    /// Builds the execution launch plan for a job turn.
    fn job_command(
        &self,
        job: &JobMetadata,
        turn: TurnInput<'_>,
    ) -> Result<LaunchPlan, EngineError>;

    /// Builds the execution launch plan for a session.
    fn session_command(
        &self,
        session: &SessionDef,
        resume: ResumeMode,
    ) -> Result<LaunchPlan, EngineError>;

    /// Classifies terminal status of an exited process from exit information and log tail.
    fn classify_exit(&self, exit: ExitInfo, log_tail: &[u8]) -> JobStatus;

    /// Extracts or returns the engine-specific conversation context identifier.
    fn conversation_id(&self, job: &JobMetadata) -> Option<ContextId>;

    // ---- A2A idle step (design HL-113 §7.14, stream-3 C3) -----------------

    /// Whether the run is idle at a turn boundary -- the `idle_turn` step of
    /// the A2A escalation ladder (§7.14) may start the next turn with a queued
    /// message only when this is true. A finished job is idle; a RUNNING agy
    /// job is idle only while a live stdin channel can carry the message; a
    /// running interactive session is idle between turns. The ladder watchers
    /// (`core/a2a/sink.rs`) consult this through the engine before applying an
    /// `idle_turn`. Default: unsupported.
    fn is_idle(&self, run: &Run) -> Result<bool, EngineError> {
        let _ = run;
        Err(EngineError::unsupported(self.id(), "a2a.inject.idle_turn"))
    }

    /// Starts the run's next turn with `joined` (the joined framed text of the
    /// claimed pending messages) as the input to the agent's context -- the
    /// `idle_turn` channel of §7.14. For a finished job this resumes the same
    /// conversation with the message as the follow-up turn; for a RUNNING live
    /// agy job it writes the framed message into the live stdin channel; for a
    /// running interactive session it delivers the message as the next user
    /// input. Never kills. Default: unsupported.
    async fn idle_turn(&self, run: &Run, joined: &str) -> Result<(), EngineError> {
        let _ = (run, joined);
        Err(EngineError::unsupported(self.id(), "a2a.inject.idle_turn"))
    }

    // ---- shared control (E4 emulation layer fronts these) -----------------

    /// Interrupts a run. Default: unsupported.
    async fn interrupt(&self, run: &Run) -> Result<(), EngineError> {
        let _ = run;
        Err(EngineError::unsupported(self.id(), "interrupt"))
    }

    /// Pauses (`true`) or resumes (`false`) a run.
    async fn pause(&self, run: &Run, paused: bool) -> Result<(), EngineError> {
        let _ = (run, paused);
        Err(EngineError::unsupported(self.id(), "pause"))
    }

    /// Forks a run into a new run, returning the new run id.
    async fn fork(&self, run: &Run, from: &Run) -> Result<RunId, EngineError> {
        let _ = (run, from);
        Err(EngineError::unsupported(self.id(), "fork"))
    }

    /// Compacts a run's context. Default: unsupported.
    async fn compact(&self, run: &Run) -> Result<(), EngineError> {
        let _ = run;
        Err(EngineError::unsupported(self.id(), "compact"))
    }

    /// Reverts a run's worktree to `commit`. Default: unsupported.
    async fn revert(&self, run: &Run, commit: &str) -> Result<(), EngineError> {
        let _ = (run, commit);
        Err(EngineError::unsupported(self.id(), "revert"))
    }

    // ---- structured read -------------------------------------------------

    /// Structured events after cursor `after` (the raw `/log` byte contract
    /// stays engine-independent).
    async fn events(&self, run: &Run, after: u64) -> Result<Vec<EngineEvent>, EngineError> {
        let _ = (run, after);
        Err(EngineError::unsupported(self.id(), "job.events"))
    }

    /// The engine's capability report (see [`capability`]).
    fn capabilities(&self) -> CapabilityTable;
}

// ---------------------------------------------------------------------------
// Registry and selection (design 2.4)
// ---------------------------------------------------------------------------

/// The `{ job, session }` per-role default engines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDefaults {
    pub job: EngineId,
    pub session: EngineId,
}

impl Default for RoleDefaults {
    fn default() -> Self {
        Self {
            job: "agy".to_string(),
            session: "claude".to_string(),
        }
    }
}

/// The engine registry: every engine a pod registered, plus the per-role
/// defaults used when a request/config does not select an engine.
///
/// E1 registers a no-op test engine in tests only; production registration
/// (from config, before the role entry points) lands with the drivers in
/// E3/E5. Since E4 every registered engine is stored wrapped in
/// [`emulate::EmulatingEngine`], which fronts the shared control surface; the
/// registry exposes it both as the opaque `Arc<dyn Engine>` trait objects the
/// endpoint layer talks to and (via [`EngineRegistry::emulating`]) as the
/// concrete wrapper the E4 run-control routes use for the emulated `/todo`
/// list. The registry is `Send + Sync` so it can be shared behind `Arc`.
#[derive(Default)]
pub struct EngineRegistry {
    engines: HashMap<EngineId, Arc<emulate::EmulatingEngine>>,
    defaults: RoleDefaults,
}

impl EngineRegistry {
    /// Resolves the engine for an existing job metadata record.
    pub fn for_job(&self, job: &JobMetadata) -> Result<Arc<dyn Engine>, EngineError> {
        self.resolve(RunKind::Job, job.engine.as_deref(), None)
    }

    /// Creates an engine registry pre-populated with the builtin engines (agy, claude).
    pub fn with_builtin(config: &crate::core::config::Config) -> Self {
        let mut reg = Self::new();
        reg.register(Arc::new(AgyEngine::new(Arc::new(config.clone()))));
        reg.register(Arc::new(claude::ClaudeEngine::new(
            config.claude_path.clone(),
            None,
        )));
        reg
    }

    /// A registry with the default per-role engines, initially empty.
    pub fn new() -> Self {
        Self {
            engines: HashMap::new(),
            defaults: RoleDefaults::default(),
        }
    }

    /// Registers an engine, wrapped in the E4 emulation layer so every
    /// registered engine answers the shared control surface (`interrupt`,
    /// `pause`, `fork`, `compact`, `revert`, `todo`). The wrapper is
    /// transparent until a key is enabled: dispatching is decided from the
    /// inner engine's own capability report, and `EmulatingEngine::enable*`
    /// only flips the reported table.
    pub fn register(&mut self, engine: Arc<dyn Engine>) {
        let wrapped = Arc::new(emulate::EmulatingEngine::new_over(engine));
        self.engines.insert(wrapped.id(), wrapped);
    }

    /// Sets the per-role default engine (precedence level 3).
    pub fn set_default(&mut self, kind: RunKind, engine: impl Into<EngineId>) {
        let engine = engine.into();
        match kind {
            RunKind::Job => self.defaults.job = engine,
            RunKind::Session => self.defaults.session = engine,
        }
    }

    /// The id of the per-role default engine for `kind`.
    pub fn default_for(&self, kind: RunKind) -> &EngineId {
        match kind {
            RunKind::Job => &self.defaults.job,
            RunKind::Session => &self.defaults.session,
        }
    }

    /// Whether `engine_id` is registered (used by `resolve` to turn a
    /// request for an unknown engine into 400, not 501).
    pub fn contains(&self, engine_id: &str) -> bool {
        self.engines.contains_key(engine_id)
    }

    /// The registered engine count (for tests / `/info`).
    pub fn len(&self) -> usize {
        self.engines.len()
    }

    /// Whether no engine is registered.
    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }

    /// The registered engine ids, sorted (deterministic for `/capabilities`).
    pub fn enabled_ids(&self) -> Vec<EngineId> {
        let mut ids: Vec<EngineId> = self.engines.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// The [`CapabilityTable`] for one registered engine, or `None`.
    pub fn capability_table(&self, engine_id: &str) -> Option<CapabilityTable> {
        self.engines.get(engine_id).map(|engine| engine.table())
    }

    /// The concrete E4 emulation wrapper for one registered engine, or `None`.
    /// The run-control routes use this to reach the emulated `/todo` list and
    /// the enabled capability gates without naming `agy`/`claude` in the
    /// endpoint layer.
    pub fn emulating(&self, engine_id: &str) -> Option<Arc<emulate::EmulatingEngine>> {
        self.engines.get(engine_id).cloned()
    }

    /// Attaches the host `Config` to every registered wrapper so the emulation
    /// layer can drive job-mode control that needs real process state (the
    /// emulated interrupt of the job's child process group). Called by the
    /// role entry points after registration; idempotent for later registrations
    /// (they pick the config up at the next attach).
    pub fn attach_config(&self, config: Arc<crate::core::config::Config>) {
        for wrapped in self.engines.values() {
            wrapped.set_config(Arc::clone(&config));
        }
    }

    /// Arms the emulation (E4) for every control endpoint the inner engine
    /// does not implement natively, for every registered wrapper. The worker
    /// role calls this after registration so claude/agy jobs report
    /// interrupt/pause/fork/compact/revert/todo emulated (never unsupported);
    /// the capability table and the endpoint guard then agree. A self-recursive
    /// call is impossible: [`emulate::EMULATED_KEYS`] are the only keys it
    /// enables.
    pub fn enable_all_emulated(&self) {
        for wrapped in self.engines.values() {
            wrapped.enable_all();
        }
    }

    /// Whether some registered wrapper adopts the config (for tests).
    pub fn config_attached(&self) -> bool {
        self.engines.values().any(|w| w.config().is_some())
    }

    /// Resolves the engine to use for `kind`, applying the
    /// precedence: request field > per-run config > per-kind default. A
    /// request/config naming an unregistered engine is
    /// [`EngineError::UnknownEngine`] (400 in the endpoint layer) -- the
    /// hard rule of design 2.4 (an unregistered engine is a selection error,
    /// never a 501).
    pub fn resolve(
        &self,
        kind: RunKind,
        request: Option<&str>,
        configured: Option<&str>,
    ) -> Result<Arc<dyn Engine>, EngineError> {
        let selected = request.or(configured);
        let engine_id = match selected {
            Some(id) => id.to_string(),
            None => self.default_for(kind).clone(),
        };
        if let Some(engine) = self.engines.get(&engine_id) {
            let wrapped: Arc<dyn Engine> = engine.clone();
            return Ok(wrapped);
        }
        Err(EngineError::UnknownEngine { engine: engine_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-op engine used to test the registry/selection and the trait
    /// defaults. Not the E2/E3 drivers -- a minimal stand-in so the trait
    /// contract is exercised without a live harness.
    pub(crate) struct NoopEngine {
        id: &'static str,
        capabilities: CapabilityTable,
    }

    impl NoopEngine {
        pub(crate) fn agy() -> Self {
            Self {
                id: "agy",
                capabilities: CapabilityTable::builder("agy")
                    .native(&[
                        "job.start",
                        "job.status",
                        "job.list",
                        "pause",
                        "workspace.*",
                    ])
                    .emulated(&["job.kill", "interrupt"])
                    .build(),
            }
        }

        pub(crate) fn claude() -> Self {
            Self {
                id: "claude",
                capabilities: CapabilityTable::builder("claude")
                    .transport(Transport::StreamJson)
                    .native(&[
                        "session.start",
                        "session.stop",
                        "session.restart",
                        "session.log",
                    ])
                    .build(),
            }
        }
    }

    #[async_trait]
    impl Engine for NoopEngine {
        fn id(&self) -> EngineId {
            self.id.to_string()
        }

        async fn start_job(
            &self,
            _spec: JobSpec,
            _ctx: &mut dyn EngineCtx,
        ) -> Result<RunId, EngineError> {
            Ok("noop-job".to_string())
        }

        async fn resume_job(&self, _run: &Run, _prompt: &str) -> Result<(), EngineError> {
            Ok(())
        }

        async fn job_status(&self, run: &Run) -> Result<run::RunStatus, EngineError> {
            Ok(run.status)
        }

        async fn start_session(
            &self,
            _spec: SessionSpec,
            _ctx: &mut dyn EngineCtx,
        ) -> Result<RunId, EngineError> {
            Ok("noop-session".to_string())
        }

        async fn send_message(&self, _run: &Run, _text: &str) -> Result<(), EngineError> {
            Ok(())
        }

        async fn stop_session(&self, _run: &Run, _restart: bool) -> Result<(), EngineError> {
            Ok(())
        }

        fn job_command(
            &self,
            job: &JobMetadata,
            _turn: TurnInput<'_>,
        ) -> Result<LaunchPlan, EngineError> {
            Ok(LaunchPlan {
                program: PathBuf::from("/bin/true"),
                args: vec![job.job_id.clone()],
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
            Ok(LaunchPlan {
                program: PathBuf::from("/bin/true"),
                args: vec![session.name.clone()],
                env: Vec::new(),
                stdin: StdinMode::Null,
                pty: true,
            })
        }

        fn classify_exit(&self, exit: ExitInfo, _log_tail: &[u8]) -> JobStatus {
            match exit.code {
                Some(0) => JobStatus::Done,
                _ => JobStatus::Failed,
            }
        }

        fn conversation_id(&self, job: &JobMetadata) -> Option<ContextId> {
            job.conversation_id.clone()
        }

        fn capabilities(&self) -> CapabilityTable {
            self.capabilities.clone()
        }
    }

    fn registry_with_defaults() -> EngineRegistry {
        let mut reg = EngineRegistry::new();
        reg.register(Arc::new(NoopEngine::agy()));
        reg.register(Arc::new(NoopEngine::claude()));
        reg
    }

    #[test]
    fn registry_reports_enabled_ids_and_capabilities() {
        let reg = registry_with_defaults();
        assert_eq!(reg.len(), 2);
        assert_eq!(
            reg.enabled_ids(),
            vec!["agy".to_string(), "claude".to_string()]
        );
        let table = reg.capability_table("claude").unwrap();
        assert_eq!(table.engine, "claude");
        assert_eq!(table.transport, Some(Transport::StreamJson));
        assert_eq!(reg.capability_table("bogus"), None);
    }

    #[test]
    fn resolve_prefers_request_over_config_over_default() {
        let reg = registry_with_defaults();
        let kind = RunKind::Job;

        // No request/config: the per-role default ("agy" for jobs) wins.
        let engine = reg.resolve(kind, None, None).expect("default resolve");
        assert_eq!(engine.id(), "agy");

        // Config (level 2) beats the default: "claude" is registered.
        let engine = reg
            .resolve(kind, None, Some("claude"))
            .expect("config resolve");
        assert_eq!(engine.id(), "claude");

        // Request (level 1) beats config.
        let engine = reg
            .resolve(kind, Some("agy"), Some("claude"))
            .expect("request resolve");
        assert_eq!(engine.id(), "agy");
    }

    #[test]
    fn resolve_per_kind_session_default() {
        let reg = registry_with_defaults();
        // Sessions default to "claude"; callers can still pick agy
        // explicitly (any role, any engine).
        let engine = reg
            .resolve(RunKind::Session, None, None)
            .expect("session default");
        assert_eq!(engine.id(), "claude");
        let engine = reg
            .resolve(RunKind::Session, Some("agy"), None)
            .expect("explicit session engine");
        assert_eq!(engine.id(), "agy");
    }

    #[test]
    fn resolve_unknown_engine_is_unknown_engine_not_unsupported() {
        let reg = registry_with_defaults();
        let err = reg
            .resolve(RunKind::Job, Some("bogus"), None)
            .err()
            .expect("unknown engine must error");
        assert_eq!(
            err,
            EngineError::UnknownEngine {
                engine: "bogus".to_string()
            }
        );
        let err = reg
            .resolve(RunKind::Job, None, Some("also-bogus"))
            .err()
            .expect("unknown config engine must error");
        assert!(matches!(err, EngineError::UnknownEngine { .. }));
    }

    #[test]
    fn set_default_changes_fallback() {
        let mut reg = registry_with_defaults();
        assert_eq!(reg.default_for(RunKind::Job), "agy");
        reg.set_default(RunKind::Job, "claude");
        assert_eq!(reg.default_for(RunKind::Job), "claude");
    }

    #[tokio::test]
    async fn unsupported_defaults_answer_documented_keys() {
        // Methods the no-op engine does not implement fall back to the
        // default Unsupported answer -- the honest 501 contract surface.
        let engine: Arc<dyn Engine> = Arc::new(NoopEngine::claude());
        let run = Run::new("r", RunKind::Session, "/workspace/proj");
        let cases: [(EngineError, &str); 6] = [
            (engine.interrupt(&run).await.unwrap_err(), "interrupt"),
            (engine.pause(&run, true).await.unwrap_err(), "pause"),
            (
                engine
                    .fork(&run, &Run::new("from", RunKind::Session, "/w"))
                    .await
                    .unwrap_err(),
                "fork",
            ),
            (engine.compact(&run).await.unwrap_err(), "compact"),
            (engine.revert(&run, "abc").await.unwrap_err(), "revert"),
            (engine.events(&run, 0).await.unwrap_err(), "job.events"),
        ];
        for (err, key) in cases {
            assert!(
                matches!(err, EngineError::Unsupported { endpoint, .. } if endpoint == key),
                "expected Unsupported('{key}'), got {err:?}"
            );
        }
    }
}

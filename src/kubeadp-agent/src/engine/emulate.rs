//! The E4 emulation layer (design HL-91 section 2.5): [`EmulatingEngine`].
//!
//! [`EmulatingEngine`] wraps a registered [`Engine`] and fronts the shared
//! control surface (`interrupt`, `pause`, `fork`, `compact`, `revert`,
//! `todo`) so a partial engine -- or one whose harness has no native
//! mechanism -- still answers every endpoint honestly: either by delegating
//! to the inner engine's own (native) implementation, or by synthesising the
//! behaviour from the run's own state (emulated), or by reporting it
//! unsupported (501). The wrapper is deliberately transparent until a key is
//! enabled: dispatching is decided from the *inner* engine's capability
//! report, and [`EmulatingEngine::enable`] only flips the reported table so
//! `/capabilities` and the endpoint guard agree.
//!
//! The emulated semantics per endpoint (design 2.5):
//!
//! - `interrupt` -- a run that the inner engine cannot interrupt natively is
//!   interrupted through the wrapped engine's own stop surface where it has
//!   one (a session-mode run), otherwise the endpoint stays unsupported. The
//!   run's per-turn git checkpoint ([`emulate::revert`]) survives, so the
//!   conversation can be resumed later.
//! - `pause`/`resume` -- an emulated pause flag per run plus a per-run message
//!   queue ([`emulate::queue`]) that holds messages written while paused or
//!   mid-turn and flushes them into the next turn. [`EmulatingEngine::send_message`]
//!   queues for engines without a native mid-run channel; [`Engine::resume_job`]
//!   drains the queue into the continuation prompt.
//! - `fork` -- a new run seeded from the source run's context (fresh id, the
//!   source's conversation id, a copied worktree) when the engine exposes a
//!   conversation id.
//! - `compact` -- summarise-then-restart ([`emulate::compact`]): the run's
//!   worktree history is summarised into a continuation prompt, a compact
//!   record is written next to the worktree, and the job is restarted through
//!   the wrapped engine with that prompt, recording the new context id.
//! - `revert`/`unrevert` -- per-turn git checkpoints taken on the run's own
//!   clone ([`emulate::revert`]); engine-independent.
//! - `todo` -- a structured list of [`TodoItem`]s collected from the run's
//!   [`EngineEvent`] stream (the `TodoWrite` tool calls of claude / plan-step
//!   events of agy both surface as [`EngineEvent::Todo`]).
//! - `permissions` -- no emulation surface: the [`Engine`] trait has no
//!   permissions method (E4 routes only the six endpoints above), so an
//!   engine without a native permission surface reports it `Unsupported`.
//!
//! Capability reporting mirrors the implementation: the wrapper reports
//! `Emulated` for the endpoints it fronts once enabled, `Native` for the ones
//! the inner engine implements itself, and keeps `Unsupported` otherwise, via
//! [`CapabilityTable::with_emulated`].
//!
//! The endpoint layer talks to `Arc<dyn Engine>` only; the registry wraps
//! every registered engine in an `Arc<EmulatingEngine>` (see
//! [`crate::engine::EngineRegistry::register`]). The wrapper is `Send + Sync`
//! and never blocks: per-run state lives on disk (queue files, checkpoints,
//! compact records) and in process-shared lock-protected maps.
//!
//! [`EngineRunRegistry`] is the minimal per-run lookup the E4 routes use to
//! resolve a job/session id to a [`Run`]. It is intentionally *not* a refactor
//! of `core::job` / `orchestrate::state`: it is the tiny in-memory mapping the
//! `Run` model needs so the new control endpoints can address a run before
//! the full run registry lands in E6.

pub mod compact;
pub mod queue;
pub mod revert;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use async_trait::async_trait;

use crate::engine::capability::{CapEndpointKey, Capability, CapabilityTable};
use crate::engine::errors::EngineError;
use crate::engine::run::{EngineEvent, Run, RunId, RunKind, RunStatus, TodoItem};
use crate::engine::{Engine, EngineCtx, JobSpec, SessionSpec};

/// The endpoint keys the emulation layer can front (the shared control
/// surface for the six E4 run-control routes; `permissions` is *not* in the
/// list because the [`Engine`] trait has no permissions method yet, so the
/// emulation layer cannot honestly claim to front it -- an engine without a
/// native permission surface reports it `Unsupported` rather than be told it
/// is emulated).
pub const EMULATED_KEYS: &[CapEndpointKey] =
    &["interrupt", "pause", "fork", "compact", "revert", "todo"];

/// Per-run volatile bookkeeping kept in process memory (everything durable
/// lives on disk in the run's worktree).
#[derive(Clone, Debug, Default)]
struct RunTopic {
    /// Emulated pause flag.
    paused: bool,
    /// The todo items collected from the run's events.
    todos: Vec<TodoItem>,
}

/// The in-process per-run bookkeeping shared by all [`EmulatingEngine`]
/// wrappers of one process (each wrapper holds a handle to the same state, so
/// one run is seen consistently across engines).
#[derive(Default)]
struct EmulationState {
    runs: Mutex<HashMap<RunId, RunTopic>>,
}

/// A per-run event cursor for the emulated [`Engine::events`] stream.
#[derive(Default)]
struct EventCursor {
    after: HashMap<RunId, u64>,
}

/// A [`crate::engine::Run`] lookup for the E4 control endpoints: maps a
/// job/session id (the `{run_id}` of the new routes) to the [`Run`] the
/// emulation talks through. Minimal by design -- see the module docs.
#[derive(Clone, Default)]
pub struct EngineRunRegistry {
    runs: Arc<RwLock<BTreeMap<RunId, Run>>>,
}

impl EngineRunRegistry {
    /// The process-wide registry the emulation wrappers and the E4 routes
    /// share by default (the role entry points may construct their own in
    /// E3/E5 and pass it in via [`EmulatingEngine::new_over_with`]).
    pub fn global() -> Arc<EngineRunRegistry> {
        static GLOBAL: OnceLock<Arc<EngineRunRegistry>> = OnceLock::new();
        Arc::clone(GLOBAL.get_or_init(|| Arc::new(EngineRunRegistry::default())))
    }

    /// Registers (or replaces) the mapping of `run.id` to `run`.
    pub fn register(&self, run: Run) {
        self.runs
            .write()
            .expect("run registry lock poisoned")
            .insert(run.id.clone(), run);
    }

    /// Removes a run mapping. Returns whether it was present.
    pub fn unregister(&self, run_id: &str) -> bool {
        self.runs
            .write()
            .expect("run registry lock poisoned")
            .remove(run_id)
            .is_some()
    }

    /// Looks up a run by id.
    pub fn lookup(&self, run_id: &str) -> Option<Run> {
        self.runs
            .read()
            .expect("run registry lock poisoned")
            .get(run_id)
            .cloned()
    }

    /// All registered runs (deterministic ordering by id).
    pub fn all(&self) -> Vec<Run> {
        self.runs
            .read()
            .expect("run registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

/// The E4 emulation wrapper engine (design 2.5): implements [`Engine`] for an
/// inner engine, fronting the shared control surface.
pub struct EmulatingEngine {
    inner: Arc<dyn Engine>,
    table: Arc<RwLock<CapabilityTable>>,
    state: Arc<EmulationState>,
    cursor: Arc<Mutex<EventCursor>>,
    runs: Arc<EngineRunRegistry>,
    fork_seq: AtomicU64,
    /// The host `Config` for the job-mode emulated interrupt (process-group
    /// termination of the job's real child). Unset for standalone
    /// engines/tests; a job-mode interrupt then stays unsupported.
    config: Arc<Mutex<Option<Arc<crate::core::config::Config>>>>,
}

impl EmulatingEngine {
    /// Wraps `inner` with the emulation layer, using the process-wide
    /// [`EngineRunRegistry::global`] for fork registrations. The initial
    /// capability table is the inner engine's own report.
    pub fn new_over(inner: Arc<dyn Engine>) -> Self {
        Self::new_over_with(inner, EngineRunRegistry::global())
    }

    /// Wraps `inner` with the emulation layer, registering forked runs into
    /// `runs` (used by tests and by role entry points that carry their own
    /// registry).
    pub fn new_over_with(inner: Arc<dyn Engine>, runs: Arc<EngineRunRegistry>) -> Self {
        let table = inner.capabilities();
        Self {
            inner,
            table: Arc::new(RwLock::new(table)),
            state: Arc::new(EmulationState::default()),
            cursor: Arc::new(Mutex::new(EventCursor::default())),
            runs,
            fork_seq: AtomicU64::new(0),
            config: Arc::new(Mutex::new(None)),
        }
    }

    /// Attaches the host `Config` so the emulation layer can drive job-mode
    /// control that needs real process state (the emulated interrupt of the
    /// job's child process group). Called by the role entry points that
    /// construct the registry from config.
    pub fn set_config(&self, config: Arc<crate::core::config::Config>) {
        *self.config.lock().expect("config lock poisoned") = Some(config);
    }

    /// The host config, when attached.
    pub fn config(&self) -> Option<Arc<crate::core::config::Config>> {
        self.config.lock().expect("config lock poisoned").clone()
    }

    /// The engine behind the wrapper.
    pub fn inner(&self) -> &Arc<dyn Engine> {
        &self.inner
    }

    /// The run registry this wrapper registers forked runs into.
    pub fn runs(&self) -> &Arc<EngineRunRegistry> {
        &self.runs
    }

    /// Arms the emulation for `key`: flips the reported capability to
    /// [`Capability::Emulated`] so `/capabilities` and the endpoint guard
    /// agree (the implementation itself is always engaged).
    pub fn enable(&self, key: CapEndpointKey) {
        *self.table.write().expect("table lock poisoned") = self.table().with_emulated(&[key]);
    }

    /// Arms the emulation for every endpoint [`EMULATED_KEYS`] this engine
    /// does not already implement natively.
    pub fn enable_all(&self) {
        let keys: Vec<CapEndpointKey> = EMULATED_KEYS
            .iter()
            .copied()
            .filter(|key| {
                self.table()
                    .capability(key)
                    .map(|c| c != Capability::Native)
                    .unwrap_or(true)
            })
            .collect();
        if !keys.is_empty() {
            *self.table.write().expect("table lock poisoned") = self.table().with_emulated(&keys);
        }
    }

    /// Arms the emulation for `key` only when the inner engine has no native
    /// implementation. Returns whether the emulation fronts the endpoint.
    pub fn enable_if_needed(&self, key: CapEndpointKey) -> bool {
        if self.table().capability(key) == Some(Capability::Native) {
            return false;
        }
        self.enable(key);
        true
    }

    /// The current reported capability table.
    pub fn table(&self) -> CapabilityTable {
        let table = self.table.read().expect("table lock poisoned").clone();
        // The emulation layer always fronts the structured `todo` surface: the
        // [`Engine`] trait has no todo method, so it can never be native --
        // todos are collected from the run's event stream and served from the
        // wrapper itself, unconditionally.
        table.with_emulated(&["todo"])
    }

    /// The reported capability for one endpoint key.
    pub fn capability(&self, key: CapEndpointKey) -> Capability {
        self.table()
            .capability(key)
            .unwrap_or(Capability::Unsupported)
    }

    fn topic(&self, run: &Run) -> RunTopic {
        self.state
            .runs
            .lock()
            .expect("emulation state lock poisoned")
            .get(&run.id)
            .cloned()
            .unwrap_or_default()
    }

    fn topic_mut<F>(&self, run: &Run, f: F)
    where
        F: FnOnce(&mut RunTopic),
    {
        f(self
            .state
            .runs
            .lock()
            .expect("emulation state lock poisoned")
            .entry(run.id.clone())
            .or_default());
    }

    /// Records a [`TodoItem`] for `run` (from a [`EngineEvent::Todo`]). Public
    /// so the event pump (E3/E5 drivers) can feed a run's event stream into
    /// the emulated todo list.
    pub fn record_todo(&self, run: &Run, item: TodoItem) {
        self.topic_mut(run, |t| t.todos.push(item));
    }

    /// The structured todo list for `run` (from its event stream).
    pub fn todo_for(&self, run: &Run) -> Vec<TodoItem> {
        self.topic(run).todos
    }

    /// Removes the per-run bookkeeping (called when a run is removed).
    pub fn forget_run(&self, run_id: &str) {
        self.state
            .runs
            .lock()
            .expect("emulation state lock poisoned")
            .remove(run_id);
        self.cursor
            .lock()
            .expect("cursor lock poisoned")
            .after
            .remove(run_id);
    }
}

impl std::fmt::Debug for EmulatingEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmulatingEngine")
            .field("inner", &self.inner.id())
            .finish()
    }
}

/// Emulated implementations of the run-control endpoints, dispatched from the
/// [`Engine`] impl below.
impl EmulatingEngine {
    /// Best-effort per-turn checkpoint taken before the emulated endpoints
    /// hand the worktree to the engine, so `revert` has something to undo.
    /// Failures degrade to `Ok` -- the checkpoint is an emulation affordance,
    /// not an endpoint contract (a worktree that is not a usable git repo
    /// simply has no revert; the run itself must still proceed).
    fn checkpoint_turn(run: &Run) -> Result<(), EngineError> {
        let _ = revert::take_checkpoint(run, "turn");
        Ok(())
    }

    /// Emulated interrupt: best-effort stop of the inner engine's run, using
    /// the engine's own stop surface where it has one.
    async fn emulated_interrupt(&self, run: &Run) -> Result<(), EngineError> {
        match run.kind {
            RunKind::Session => self.inner.stop_session(run, false).await,
            RunKind::Job => {
                // A job's real child must be stopped at the process level: the
                // job control surface (`commands::kill`) is the only thing
                // that reaches the sandboxed child, so a claude/agy job
                // without a native interrupt is interrupted the same way an
                // operator kill does -- clean SIGTERM to the process group,
                // grace, then SIGKILL -- but WITHOUT the kill bookkeeping:
                // this is an interrupt, not a user kill, and the job is meant
                // to be resumed afterwards. The stoppage mirrors
                // `worker::a2a.rs::resume_with` (the "[a2a interrupt]"
                // pattern) except the log lines carry the engine `interrupt`
                // prefix so status classification and A2A parsing agree.
                let config = self
                    .config
                    .lock()
                    .expect("config lock poisoned")
                    .clone()
                    .ok_or_else(|| EngineError::unsupported(self.inner.id(), "interrupt"))?;
                let job_id = run.id.clone();
                let Some(meta) =
                    crate::core::job::JobMetadata::resolve(&config.state_root, &job_id).ok()
                else {
                    return Err(EngineError::NotFound { run: job_id });
                };
                let is_running = crate::core::job::compute_status(&meta, &config.state_root)
                    == crate::core::job::JobStatus::Running;
                use std::io::Write;
                let log_path =
                    crate::core::job::JobMetadata::log_path(&config.state_root, &meta.job_id);
                let append = |line: String| {
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log_path)
                    {
                        let _ = f.write_all(format!("[engine interrupt] {line}\n").as_bytes());
                    }
                };
                if let (true, Some(pid)) = (is_running, meta.pid) {
                    crate::core::commands::kill::terminate_process_to(
                        &config,
                        &meta.job_id,
                        pid,
                        meta.pid_start_time,
                        Some(meta.retry_count),
                        &mut |_stderr, line| append(line),
                    )
                    .map_err(|e| {
                        EngineError::Internal(format!(
                            "failed to interrupt job '{}': {e}",
                            meta.job_id
                        ))
                    })?;
                    append(format!(
                        "job '{}' (PID {}) interrupted (engine emulation).",
                        meta.job_id, pid
                    ));
                } else {
                    append(format!(
                        "job '{}' interrupt requested but the job is not running (status: {}).",
                        meta.job_id,
                        crate::core::job::compute_status(&meta, &config.state_root).as_str()
                    ));
                }
                Ok(())
            }
        }
    }

    /// Emulated pause: flag the run paused and let the inner engine know. The
    /// engine may not have a native pause (an `Unsupported` there is fine);
    /// the queue + flag still provide the pause/resume contract.
    async fn emulated_pause(&self, run: &Run, paused: bool) -> Result<(), EngineError> {
        self.topic_mut(run, |t| t.paused = paused);
        match self.inner.pause(run, paused).await {
            Ok(()) | Err(EngineError::Unsupported { .. }) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// Emulated fork: a new run seeded from the source run's context (fresh
    /// id, the source's conversation id, a copied worktree), registered in the
    /// run registry. Returns the new run id.
    async fn emulated_fork(&self, from: &Run) -> Result<RunId, EngineError> {
        let seq = self.fork_seq.fetch_add(1, Ordering::Relaxed);
        let new_id = format!("{}-fork-{seq}", from.id);

        let new_workdir = fork_workdir(&from.workdir, &new_id)?;
        copy_worktree(&from.workdir, &new_workdir).map_err(|e| {
            EngineError::Internal(format!(
                "failed to copy worktree for fork '{}': {e}",
                new_id
            ))
        })?;

        let mut new_run = from.clone();
        new_run.id = new_id.clone();
        new_run.status = RunStatus::Pending;
        new_run.workdir = new_workdir;
        new_run.started_at = None;
        new_run.ended_at = None;
        new_run.exit = None;
        new_run.events_seq = 0;
        new_run.meta = serde_json::json!({
            "forked_from": from.id,
            "forked_from_context": from.context_id,
        });
        self.runs.register(new_run);
        Ok(new_id)
    }

    /// Emulated compact: summarise the run's worktree, write the compact
    /// record, and restart the run through the wrapped engine with the
    /// continuation prompt. The new context id is recorded on the run in the
    /// registry.
    async fn emulated_compact(&self, run: &Run) -> Result<(), EngineError> {
        let _ = revert::take_checkpoint(run, "compact-before");
        let summary = compact::summarise_and_write(run, None)
            .map_err(|e| EngineError::Internal(e.to_string()))?;

        match run.kind {
            RunKind::Job => {
                // Restart the JOB with the summarised continuation prompt
                // through the job command layer (`commands::retry`), so the
                // emulated compact drives the same machinery both engines use
                // for a resumed turn (the agy trait `start_job` is not wired;
                // the job layer is the real driver for both agy and claude,
                // HL-71 S2). The restart is best-effort: a wrapper without an
                // attached config (test/registry usage) or a job row that
                // cannot be resumed still answers the endpoint honestly -- the
                // compact record is written and survives for a later resume.
                let restart = self.config.lock().expect("config lock poisoned").clone();
                if let Some(config) = restart {
                    let job_id = run.id.clone();
                    match crate::core::commands::retry::run(
                        &config,
                        &crate::core::args::RetryArgs {
                            job_id: job_id.clone(),
                            hindsight: None,
                            hindsight_query: None,
                            prompt: Some(summary.next_prompt.clone()),
                        },
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            // A row that cannot be resumed (still running, no
                            // conversation id) leaves the compact record -- the
                            // summarised context survives -- and the restart is
                            // deferred. The endpoint still reports the compact
                            // honestly; it is not an engine gap (a 501).
                            append_compact_note(
                                &config,
                                &job_id,
                                &format!("compact written; restart deferred: {e}"),
                            );
                        }
                    }
                }
            }
            RunKind::Session => {
                // No session-restart surface in E1; record the summary and
                // report success so the compact endpoint answers a usable
                // body. The run's context id is kept.
                let _ = summary;
            }
        }
        Ok(())
    }

    /// Emulated revert: reset the run's worktree to its latest checkpoint
    /// (`commit` is ignored for the emulated path; `revert::revert` always
    /// resets to the checkpoint). An empty `commit` means *unrevert*: restore
    /// the work `revert` discarded.
    async fn emulated_revert(&self, run: &Run, commit: &str) -> Result<(), EngineError> {
        let _ = commit;
        match run.kind {
            RunKind::Job => {
                if commit.is_empty() {
                    // Unrevert: restore the work `revert` discarded. A job that
                    // was never reverted has no preserved work -- that is a
                    // graceful no-op (the endpoint always answers a usable
                    // body), not an engine failure.
                    match revert::preserved_work_sha(run) {
                        Ok(Some(_)) => revert::unrevert(run)
                            .map(|_| ())
                            .map_err(|e| EngineError::Internal(e.to_string())),
                        Ok(None) => Ok(()),
                        Err(e) => Err(EngineError::Internal(e.to_string())),
                    }
                } else {
                    revert::revert(run)
                        .map(|_| ())
                        .map_err(|e| EngineError::Internal(e.to_string()))
                }
            }
            RunKind::Session => Err(EngineError::unsupported(self.inner.id(), "revert")),
        }
    }
}

/// A fresh worktree directory for a fork, derived from the source worktree's
/// parent (`<parent>/<source-name>-fork-<seq>`).
fn fork_workdir(source: &Path, new_id: &str) -> Result<PathBuf, EngineError> {
    let parent = source.parent().ok_or_else(|| {
        EngineError::Conflict(format!("worktree {} has no parent", source.display()))
    })?;
    let name = source.file_name().and_then(|n| n.to_str()).unwrap_or("run");
    Ok(parent.join(format!("{name}-{new_id}")))
}

/// Copies a worktree directory into `dst` (best effort; the `.git` entry is
/// left out so the fork is an independent directory, not a second worktree of
/// the same repository). Symlinks are recreated as symlinks rather than
/// followed so a self-referential symlink cannot trigger infinite recursion
/// and a symlink to an external path cannot materialize external file contents.
fn copy_worktree(src: &Path, dst: &Path) -> std::io::Result<()> {
    if !src.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("source worktree {} is not a directory", src.display()),
        ));
    }
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let meta = std::fs::symlink_metadata(&from)?;
        let file_type = meta.file_type();
        if file_type.is_symlink() {
            let link_target = std::fs::read_link(&from)?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(&link_target, &to)?;
        } else if file_type.is_dir() {
            copy_worktree(&from, &to)?;
        } else if file_type.is_file() {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// Appends a `[engine compact]` note to a job's log (best-effort; the note
/// records that the emulated compact wrote its record but the restart was
/// deferred).
fn append_compact_note(config: &crate::core::config::Config, job_id: &str, note: &str) {
    use std::io::Write;
    let log_path = crate::core::job::JobMetadata::log_path(&config.state_root, job_id);
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        let _ = f.write_all(format!("[engine compact] {note}\n").as_bytes());
    }
}

#[async_trait]
impl Engine for EmulatingEngine {
    fn id(&self) -> crate::engine::EngineId {
        self.inner.id()
    }

    async fn start_job(
        &self,
        spec: JobSpec,
        ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        self.inner.start_job(spec, ctx).await
    }

    async fn resume_job(&self, run: &Run, prompt: &str) -> Result<(), EngineError> {
        // Deliver any queued mid-run messages as part of the next turn's
        // prompt (the agy queue: messages written while paused or mid-turn are
        // delivered at the next turn boundary). The queue is drained first and
        // its messages are passed along with the prompt, so a real agy resume
        // naturally carries them via `-p`.
        let queued = queue::drain(run).map_err(|e| {
            EngineError::Internal(format!(
                "failed to drain message queue for run '{}': {e}",
                run.id
            ))
        })?;
        let full_prompt = if queued.is_empty() {
            prompt.to_string()
        } else {
            format!("{}\n\n{}", prompt, queued.join("\n"))
        };
        Self::checkpoint_turn(run)?;
        self.inner.resume_job(run, &full_prompt).await
    }

    async fn job_status(&self, run: &Run) -> Result<RunStatus, EngineError> {
        self.inner.job_status(run).await
    }

    async fn start_session(
        &self,
        spec: SessionSpec,
        ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        self.inner.start_session(spec, ctx).await
    }

    async fn send_message(&self, run: &Run, text: &str) -> Result<(), EngineError> {
        // Mid-run messages for a batch engine are queued to the run's per-run
        // queue file and delivered at the next turn boundary (resume). The
        // native path (a live PTY) is preferred when the engine has one.
        match self.inner.send_message(run, text).await {
            Ok(()) => {
                Self::checkpoint_turn(run)?;
                Ok(())
            }
            Err(EngineError::Unsupported { .. }) => {
                let _ = queue::enqueue(run, text).map_err(|e| {
                    EngineError::Internal(format!(
                        "failed to queue message for run '{}': {e}",
                        run.id
                    ))
                })?;
                Ok(())
            }
            Err(other) => Err(other),
        }
    }

    async fn stop_session(&self, run: &Run, restart: bool) -> Result<(), EngineError> {
        self.inner.stop_session(run, restart).await
    }

    fn job_command(
        &self,
        job: &crate::core::job::JobMetadata,
        turn: crate::engine::TurnInput<'_>,
    ) -> Result<crate::engine::LaunchPlan, EngineError> {
        self.inner.job_command(job, turn)
    }

    fn classify_exit(
        &self,
        exit: crate::engine::run::ExitInfo,
        log_tail: &[u8],
    ) -> crate::core::job::JobStatus {
        self.inner.classify_exit(exit, log_tail)
    }

    fn conversation_id(
        &self,
        job: &crate::core::job::JobMetadata,
    ) -> Option<crate::engine::ContextId> {
        self.inner.conversation_id(job)
    }

    async fn interrupt(&self, run: &Run) -> Result<(), EngineError> {
        // A session-mode run is always interrupted through the wrapped
        // engine's own stop surface: the only way to interrupt a live session
        // is to stop it (an inner engine's native `interrupt` default is
        // `Unsupported` anyway). A job-mode run uses the inner engine's
        // native interrupt when it reports one.
        if run.kind == RunKind::Session {
            Self::checkpoint_turn(run)?;
            return self.emulated_interrupt(run).await;
        }
        if self.inner.capabilities().capability("interrupt") == Some(Capability::Native) {
            return self.inner.interrupt(run).await;
        }
        Self::checkpoint_turn(run)?;
        self.emulated_interrupt(run).await
    }

    async fn pause(&self, run: &Run, paused: bool) -> Result<(), EngineError> {
        if self.inner.capabilities().capability("pause") == Some(Capability::Native) {
            return self.inner.pause(run, paused).await;
        }
        self.emulated_pause(run, paused).await
    }

    async fn fork(&self, run: &Run, from: &Run) -> Result<RunId, EngineError> {
        // `run` is the (new) run the caller pre-created; `from` is the source.
        if self.inner.capabilities().capability("fork") == Some(Capability::Native) {
            return self.inner.fork(run, from).await;
        }
        let _ = run;
        self.emulated_fork(from).await
    }

    async fn compact(&self, run: &Run) -> Result<(), EngineError> {
        if self.inner.capabilities().capability("compact") == Some(Capability::Native) {
            return self.inner.compact(run).await;
        }
        self.emulated_compact(run).await
    }

    async fn revert(&self, run: &Run, commit: &str) -> Result<(), EngineError> {
        if self.inner.capabilities().capability("revert") == Some(Capability::Native) {
            return self.inner.revert(run, commit).await;
        }
        self.emulated_revert(run, commit).await
    }

    async fn events(&self, run: &Run, after: u64) -> Result<Vec<EngineEvent>, EngineError> {
        // An engine with a native event stream surfaces it directly; any
        // `Todo` events it carries are also recorded into the emulated list so
        // `/todo` is populated even for engines that stream todos natively.
        let evs = if self.inner.capabilities().capability("job.events") == Some(Capability::Native)
        {
            self.inner.events(run, after).await?
        } else {
            Vec::new()
        };
        for ev in &evs {
            if let EngineEvent::Todo(item) = ev {
                self.record_todo(run, item.clone());
            }
        }
        if !evs.is_empty() {
            return Ok(evs);
        }

        // No native stream (emulated): the todos gathered so far are surfaced
        // as `Todo` events, each exactly once, in the order they were
        // recorded. `sent` is the per-run count already delivered (seeded from
        // the caller's `after` on first poll), so a re-poll only yields the
        // newly recorded items.
        let mut cursor = self.cursor.lock().expect("cursor lock poisoned");
        let sent = cursor.after.entry(run.id.clone()).or_insert(after);
        let todos = self.todo_for(run);
        let events: Vec<EngineEvent> = todos
            .iter()
            .skip(*sent as usize)
            .cloned()
            .map(EngineEvent::Todo)
            .collect();
        *sent = todos.len() as u64;
        Ok(events)
    }

    fn capabilities(&self) -> CapabilityTable {
        self.table()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::capability::CapabilityTable;
    use crate::engine::run::{Run, RunKind};

    /// A recording mock inner engine: every call is appended to `calls`, and
    /// its capability table is built from the native/emulated sets given.
    struct MockInner {
        id: &'static str,
        table: CapabilityTable,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockInner {
        fn new(
            id: &'static str,
            native: &[CapEndpointKey],
            emulated: &[CapEndpointKey],
            calls: Arc<Mutex<Vec<String>>>,
        ) -> Self {
            Self {
                id,
                table: CapabilityTable::builder(id)
                    .native(native)
                    .emulated(emulated)
                    .build(),
                calls,
            }
        }
    }

    #[async_trait]
    impl Engine for MockInner {
        fn id(&self) -> crate::engine::EngineId {
            self.id.to_string()
        }
        async fn start_job(
            &self,
            spec: JobSpec,
            _ctx: &mut dyn crate::engine::EngineCtx,
        ) -> Result<RunId, EngineError> {
            let ctx = spec.conversation_id.clone().unwrap_or_default();
            self.calls
                .lock()
                .unwrap()
                .push(format!("start_job|{ctx}|{}", spec.prompt));
            Ok(format!("{}-started", self.id))
        }
        async fn resume_job(&self, run: &Run, prompt: &str) -> Result<(), EngineError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("resume_job|{}|{}", run.id, prompt));
            Ok(())
        }
        async fn job_status(&self, _run: &Run) -> Result<RunStatus, EngineError> {
            Ok(RunStatus::Running)
        }
        async fn start_session(
            &self,
            _spec: SessionSpec,
            _ctx: &mut dyn crate::engine::EngineCtx,
        ) -> Result<RunId, EngineError> {
            Ok(format!("{}-session", self.id))
        }
        async fn send_message(&self, run: &Run, text: &str) -> Result<(), EngineError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("send_message|{}|{}", run.id, text));
            Err(EngineError::unsupported(self.id, "session.message"))
        }
        async fn stop_session(&self, run: &Run, restart: bool) -> Result<(), EngineError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("stop_session|{}|{}", run.id, restart));
            Ok(())
        }
        fn job_command(
            &self,
            job: &crate::core::job::JobMetadata,
            _turn: crate::engine::TurnInput<'_>,
        ) -> Result<crate::engine::LaunchPlan, EngineError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("job_command|{}", job.job_id));
            Ok(crate::engine::LaunchPlan {
                program: std::path::PathBuf::from("/bin/true"),
                args: vec![job.job_id.clone()],
                env: Vec::new(),
                stdin: crate::engine::StdinMode::Null,
                pty: false,
            })
        }
        fn classify_exit(
            &self,
            exit: crate::engine::run::ExitInfo,
            _log_tail: &[u8],
        ) -> crate::core::job::JobStatus {
            match exit.code {
                Some(0) => crate::core::job::JobStatus::Done,
                _ => crate::core::job::JobStatus::Failed,
            }
        }
        fn conversation_id(
            &self,
            job: &crate::core::job::JobMetadata,
        ) -> Option<crate::engine::ContextId> {
            job.conversation_id.clone()
        }
        async fn pause(&self, run: &Run, paused: bool) -> Result<(), EngineError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("pause|{}|{}", run.id, paused));
            Ok(())
        }
        fn capabilities(&self) -> CapabilityTable {
            self.table.clone()
        }
    }

    #[tokio::test]
    async fn wrapper_is_transparent_until_enabled() {
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new(
            "mock",
            &["interrupt", "pause"],
            &[],
            Arc::clone(&calls),
        ));
        let engine = EmulatingEngine::new_over(inner);
        let run = Run::new("r-1", RunKind::Session, "/workspace/proj");

        // Transparent capability report before any enable: the inner table.
        assert_eq!(engine.capability("interrupt"), Capability::Native);
        assert_eq!(engine.capability("fork"), Capability::Unsupported);

        // Native interrupt passes through to the inner engine's stop surface.
        engine.interrupt(&run).await.unwrap();
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("stop_session|r-1|")));

        // enable() flips only the reported label, not the dispatch.
        engine.enable("pause");
        assert_eq!(engine.capability("pause"), Capability::Emulated);
        engine.pause(&run, true).await.unwrap();
        assert!(
            calls.lock().unwrap().iter().any(|c| c == "pause|r-1|true"),
            "inner pause must still be the native dispatch (enable only flips the table)"
        );
    }

    #[tokio::test]
    async fn emulated_interrupt_uses_stop_surface_for_sessions() {
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new("mock", &[], &[], Arc::clone(&calls)));
        let engine = EmulatingEngine::new_over(inner);
        let run = Run::new("r-2", RunKind::Session, "/workspace/proj");

        engine.interrupt(&run).await.unwrap();
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c == "stop_session|r-2|false"));
    }

    #[tokio::test]
    async fn emulated_interrupt_for_job_without_native_is_unsupported() {
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new("mock", &[], &[], Arc::clone(&calls)));
        let engine = EmulatingEngine::new_over(inner);
        let run = Run::new("r-3", RunKind::Job, "/workspace/proj");
        assert!(matches!(
            engine.interrupt(&run).await.unwrap_err(),
            EngineError::Unsupported {
                endpoint: "interrupt",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn resume_job_drains_queued_messages_into_prompt() {
        let tmp = tempfile::tempdir().unwrap();
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new("mock", &[], &[], Arc::clone(&calls)));
        let engine = EmulatingEngine::new_over(inner);
        let run = Run::new("r-4", RunKind::Job, tmp.path());

        // send_message is Unsupported in the mock -> queued.
        engine.send_message(&run, "mid-run ping").await.unwrap();
        assert_eq!(queue::len(&run).unwrap(), 1);

        // resume drains the queue and passes the queued text with the prompt.
        engine.resume_job(&run, "continue").await.unwrap();
        let recorded = calls.lock().unwrap().clone();
        let resume = recorded
            .iter()
            .find(|c| c.starts_with("resume_job|r-4|"))
            .cloned()
            .unwrap();
        assert!(
            resume.contains("mid-run ping"),
            "queued message lost: {resume}"
        );
        assert_eq!(queue::len(&run).unwrap(), 0, "queue must drain on resume");
    }

    #[tokio::test]
    async fn emulated_compact_writes_record_and_is_graceful_without_config() {
        let tmp = tempfile::tempdir().unwrap();
        let workdir = tmp.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        let init = crate::core::reaper::output_tracked(
            crate::core::git::hardened_git()
                .arg("init")
                .arg("-b")
                .arg("main")
                .arg(&workdir),
        )
        .unwrap();
        assert!(init.status.success());
        std::process::Command::new("git")
            .args(["-C", workdir.to_string_lossy().as_ref()])
            .args(["config", "user.name", "emulate-compact-test"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["-C", workdir.to_string_lossy().as_ref()])
            .args(["config", "user.email", "emulate@example.com"])
            .status()
            .unwrap();
        std::fs::write(workdir.join("a.txt"), "a\n").unwrap();
        crate::core::git::commit_uncommitted_changes(&workdir, "summ", "add a").unwrap();

        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new("mock", &[], &[], Arc::clone(&calls)));
        let runs = Arc::new(EngineRunRegistry::default());
        let engine = EmulatingEngine::new_over_with(inner, Arc::clone(&runs));
        let mut run = Run::new("r-5", RunKind::Job, &workdir);
        run.context_id = Some("ctx-5".to_string());
        run.engine = "mock".to_string();
        runs.register(run.clone());

        // No config attached (test/registry wrapper): the compact record is
        // still written and the endpoint answers Ok -- the restart is
        // best-effort and deferred, never an engine gap.
        engine.compact(&run).await.unwrap();

        assert!(
            compact::compact_meta_path(&run).exists(),
            "compact must write its record file"
        );
        let record: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(compact::compact_meta_path(&run)).unwrap(),
        )
        .unwrap();
        assert!(
            record["summary"]
                .as_str()
                .is_some_and(|s| s.contains("add a")),
            "compact summary must capture the worktree history"
        );

        // No restart was attempted: the wrapper has no config, so the mock's
        // start_job was never called.
        let recorded = calls.lock().unwrap().clone();
        assert!(
            recorded.iter().all(|c| !c.starts_with("start_job|")),
            "compact without a config must not attempt a restart: {recorded:?}"
        );
    }

    #[tokio::test]
    async fn enable_all_flips_only_emulated_capabilities() {
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        // Native: interrupt + pause; everything else emulated-able.
        let inner = Arc::new(MockInner::new(
            "mock",
            &["interrupt", "pause"],
            &[],
            Arc::clone(&calls),
        ));
        let engine = EmulatingEngine::new_over(inner);
        engine.enable_all();

        // Native endpoints stay Native.
        assert_eq!(engine.capability("interrupt"), Capability::Native);
        assert_eq!(engine.capability("pause"), Capability::Native);
        // The remaining control keys are now Emulated.
        for key in ["fork", "compact", "revert", "todo"] {
            assert_eq!(engine.capability(key), Capability::Emulated, "{key}");
        }
        // Session-only native surface is untouched.
        assert_eq!(engine.capability("session.start"), Capability::Unsupported);
    }

    #[tokio::test]
    async fn emulated_events_surface_each_todo_once() {
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new("mock", &[], &[], Arc::clone(&calls)));
        let engine = EmulatingEngine::new_over(inner);
        let run = Run::new("r-6", RunKind::Job, "/workspace/proj");

        engine.record_todo(
            &run,
            TodoItem {
                content: "one".into(),
                status: None,
            },
        );
        engine.record_todo(
            &run,
            TodoItem {
                content: "two".into(),
                status: None,
            },
        );

        // First poll within cursor range surfaces both.
        let evs = engine.events(&run, 0).await.unwrap();
        let todos: Vec<&EngineEvent> = evs
            .iter()
            .filter(|e| matches!(e, EngineEvent::Todo(_)))
            .collect();
        assert_eq!(todos.len(), 2);

        // Second poll (same cursor) surfaces nothing new, not a duplicate.
        let evs2 = engine.events(&run, 0).await.unwrap();
        assert!(evs2.is_empty(), "re-poll must not duplicate: {evs2:?}");
    }

    #[tokio::test]
    async fn emulated_fork_registers_a_new_run_with_copied_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("work");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), "a\n").unwrap();
        // A .git dir is excluded from the copy (the fork is an independent dir).
        std::fs::create_dir_all(src.join(".git")).unwrap();
        std::fs::write(src.join(".git/config"), "x\n").unwrap();

        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MockInner::new("mock", &[], &[], Arc::clone(&calls)));
        let runs = Arc::new(EngineRunRegistry::default());
        let engine = EmulatingEngine::new_over_with(inner, Arc::clone(&runs));

        let mut from = Run::new("r-fork", RunKind::Job, &src);
        from.context_id = Some("ctx-f".to_string());
        from.engine = "mock".to_string();

        let placeholder = Run::new("r-fork:pending", RunKind::Job, &src);
        let new_id = engine.fork(&placeholder, &from).await.unwrap();
        assert!(new_id.starts_with("r-fork-fork-"), "{new_id}");

        let forked = runs.lookup(&new_id).expect("forked run must be registered");
        assert_eq!(forked.context_id.as_deref(), Some("ctx-f"));
        assert!(forked.workdir.join("a.txt").exists());
        assert!(
            !forked.workdir.join(".git").exists(),
            "the fork must not carry the source .git dir"
        );
    }

    #[test]
    fn copy_worktree_preserves_symlinks_without_following_cycles() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), "hello\n").unwrap();
        // A directory symlink loop (`loop -> .`) would infinitely recurse if followed.
        std::os::unix::fs::symlink(".", src.join("loop")).unwrap();
        std::os::unix::fs::symlink("file.txt", src.join("rel_link")).unwrap();

        copy_worktree(&src, &dst).expect("copy_worktree must succeed on symlinks and cycles");
        let loop_meta = std::fs::symlink_metadata(dst.join("loop")).unwrap();
        assert!(loop_meta.file_type().is_symlink());
        assert_eq!(
            std::fs::read_link(dst.join("loop")).unwrap(),
            std::path::PathBuf::from(".")
        );
        let rel_meta = std::fs::symlink_metadata(dst.join("rel_link")).unwrap();
        assert!(rel_meta.file_type().is_symlink());
        assert_eq!(
            std::fs::read_link(dst.join("rel_link")).unwrap(),
            std::path::PathBuf::from("file.txt")
        );
    }
}

//! The claude engine (design HL-91 E2).
//!
//! [`ClaudeEngine`] implements the E1 [`crate::engine::Engine`] trait for the
//! claude harness, supporting both transports:
//!
//! - **job mode** via headless `claude -p --input-format stream-json
//!   --output-format stream-json` ([`stream_json`]),
//! - **session mode** by delegating to the existing supervisor machinery
//!   ([`session_pty`]) that today runs claude in an interactive PTY with
//!   `--remote-control`.
//!
//! Nothing routes through the trait yet (E1 state): this is the engine-side
//! implementation that E3/E5 wire the endpoint layer onto.

use crate::engine::capability::CapabilityTable;
use crate::engine::claude::session_pty::ClaudePtyAdapter;
use crate::engine::claude::stream_json::{
    parse_result_session_id, parse_stream_json_line, render_text_request, stream_events,
    StreamJsonArgs, StreamJsonMode,
};
use crate::engine::run::{ContextId, EngineEvent, EngineId, Run, RunId, RunKind, RunStatus};
use crate::engine::{Engine, EngineCtx, EngineError, JobSpec, SessionSpec, Transport};
use async_trait::async_trait;
use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Sub-modules (2018 file+dir layout): transcript parser, headless stream-json
// driver and the PTY session adapter.
// ---------------------------------------------------------------------------
pub mod session_pty;
pub mod stream_json;
pub mod transcript;

/// The stable engine identifier.
pub const ENGINE_ID: &str = "claude";

/// Bounded event buffer size per job (drop-oldest), so an idle engine does not
/// accumulate an unbounded transcript for a long-lived job.
const JOB_EVENT_BUFFER_CAP: usize = 10_000;

/// A bounded, ordered in-memory event buffer appended by a job's output reader
/// thread. Cursor semantics mirror the session path: a cursor is a 0-based
/// index into the retained events (`after` saturates at the end, so a cursor
/// past the last event yields an empty tail).
struct JobEvents {
    events: Vec<EngineEvent>,
    cap: usize,
}

impl JobEvents {
    fn new(cap: usize) -> Self {
        Self {
            events: Vec::new(),
            cap,
        }
    }

    /// Appends `event`, dropping the oldest when the buffer is at capacity.
    fn push(&mut self, event: EngineEvent) {
        if self.events.len() >= self.cap {
            self.events.remove(0);
        }
        self.events.push(event);
    }

    /// The events at and after index `after` (saturating).
    fn after(&self, after: u64) -> Vec<EngineEvent> {
        events_after_index(&self.events, after)
    }
}

/// Pure cursor helper shared by the job event buffer and [`JobEvents::after`]
/// so the "index at/after `after`, saturating" contract is unit-testable
/// without a child process.
fn events_after_index(events: &[EngineEvent], after: u64) -> Vec<EngineEvent> {
    let start = (after as usize).min(events.len());
    events[start..].to_vec()
}

/// A single live claude job child (stream-json headless mode).
struct JobHandle {
    child: Child,
    /// The child's stdin, kept so further user messages can be delivered over
    /// the harness's stream-json input (`None` while a resume is spawning).
    #[allow(dead_code)] // held open on purpose: dropping it would close the child's input
    stdin: Option<ChildStdin>,
    /// The harness session id captured from the stream-json output
    /// (`parse_result_session_id`), shared with the reader thread and used by
    /// `resume_job` to derive the `--resume` target when the caller passes no
    /// `context_id`.
    session_id: Arc<Mutex<Option<ContextId>>>,
    /// The bounded, ordered event buffer appended by the reader thread.
    events: Arc<Mutex<JobEvents>>,
    /// Observed terminal status, `None` while the child is (or may be)
    /// running or already reaped. `Child::try_wait` only reports an exit once,
    /// so after a `job_status` reaps the child a later probe answers
    /// `Ok(None)`; this slot keeps the observed outcome so `resume_job` can
    /// tell "exited" from "still running".
    terminal: Option<RunStatus>,
}

/// The claude engine.
pub struct ClaudeEngine {
    /// The claude binary path.
    binary: std::path::PathBuf,
    /// Session-mode adapter over the supervisor API state (`None` when the
    /// engine runs without a supervising process, e.g. the worker role;
    /// session operations then answer `unsupported`).
    pty: Option<ClaudePtyAdapter>,
    /// Live job children keyed by run id.
    jobs: Mutex<HashMap<RunId, JobHandle>>,
    /// The `--mode` value for jobs.
    mode: StreamJsonMode,
}

impl ClaudeEngine {
    /// Builds the engine with a supervisor-adapter (`pty`) and a binary.
    /// `None` disables session mode (operations answer `unsupported`).
    pub fn new(binary: std::path::PathBuf, pty: Option<ClaudePtyAdapter>) -> Self {
        Self {
            binary,
            pty,
            jobs: Mutex::new(HashMap::new()),
            mode: StreamJsonMode::Normal,
        }
    }

    fn adapter(&self, endpoint: &'static str) -> Result<&ClaudePtyAdapter, EngineError> {
        self.pty
            .as_ref()
            .ok_or_else(|| EngineError::unsupported(ENGINE_ID, endpoint))
    }
}

/// Spawns a claude stream-json child for `run` (fresh job or resume),
/// configures piped stdio, delivers the first user message and starts a
/// reader thread that maps output lines to [`EngineEvent`]s in `events`
/// and records the harness session id into `session_id`. Returns the child
/// and its stdin handle; the caller owns them (the job map is only
/// touched by the caller, so a resume can reuse the same `events` /
/// `session_id` buffers across child spawns).
///
/// A free function (not `&self`) so `resume_job` can spawn while it holds a
/// mutable borrow of the `jobs` map entry it is replacing.
///
/// The worktree policy file is written into `run.workdir`'s parent
/// (mirroring the job launcher's state root) so it can fail closed
/// alongside the run's other artifacts.
fn spawn_job(
    binary: &Path,
    mode: &StreamJsonMode,
    run: &Run,
    resume: Option<&str>,
    prompt: &str,
    events: Arc<Mutex<JobEvents>>,
    session_id: Arc<Mutex<Option<ContextId>>>,
) -> Result<(Child, ChildStdin), EngineError> {
    let binary_str = binary.to_string_lossy().to_string();
    let args = StreamJsonArgs {
        binary: binary_str.clone(),
        workdir: run.workdir.clone(),
        session_id: None,
        resume: resume.map(|s| s.to_string()),
        mode: mode.clone(),
        dangerous: true,
    };
    let argv = args.argv();

    let run_id = run.id.clone();
    let workdir = run.workdir.clone();
    let state_root = workdir
        .parent()
        .unwrap_or_else(|| Path::new("/tmp"))
        .to_path_buf();
    std::fs::create_dir_all(&state_root)
        .map_err(|e| EngineError::Internal(format!("failed to create job state root: {e}")))?;
    let policy_path = state_root.join(format!("{run_id}.claude.policy.json"));
    let base_policy =
        crate::sandbox::SandboxPolicy::for_job(&run_id, &workdir, None, Some(binary), None);
    crate::sandbox::SandboxPolicy::write_policy_file(&base_policy, &policy_path)
        .map_err(|e| EngineError::Internal(format!("failed to write sandbox policy: {e}")))?;

    let launch = crate::engine::claude::stream_json::launch_argv(binary, &argv[1..], &policy_path);
    let mut cmd = std::process::Command::new(&launch[0]);
    cmd.args(&launch[1..]);
    cmd.current_dir(&workdir);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    crate::core::runner::detached_session(&mut cmd);
    let mut child =
        crate::core::spawn::spawn_command_tracked(&mut cmd, "claude job").map_err(|e| {
            EngineError::Internal(format!("failed to spawn claude job '{run_id}': {e}"))
        })?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| EngineError::Internal("claude job stdin is not piped".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| EngineError::Internal("claude job stdout is not piped".to_string()))?;

    // Deliver the first user message (the job's prompt) over stdin.
    let request_line = render_text_request(prompt);
    let mut stdin_buf = stdin;
    stdin_buf
        .write_all(format!("{request_line}\n").as_bytes())
        .map_err(|e| EngineError::Internal(format!("failed to write claude job prompt: {e}")))?;
    stdin_buf
        .flush()
        .map_err(|e| EngineError::Internal(format!("failed to flush claude job stdin: {e}")))?;

    let events_reader = Arc::clone(&events);
    let session_reader = Arc::clone(&session_id);
    let reader_run = run_id.clone();
    std::thread::Builder::new()
        .name(format!("claude-job:{reader_run}"))
        .spawn(move || read_job_stream(stdout, events_reader, session_reader))
        .map_err(|e| {
            EngineError::Internal(format!("failed to spawn claude job reader thread: {e}"))
        })?;

    Ok((child, stdin_buf))
}

#[async_trait]
impl Engine for ClaudeEngine {
    fn id(&self) -> EngineId {
        ENGINE_ID.to_string()
    }

    async fn start_job(
        &self,
        spec: JobSpec,
        _ctx: &mut dyn EngineCtx,
    ) -> Result<RunId, EngineError> {
        let run_id = run_id_from_workdir(&spec.workdir);
        let mut run = Run::new(run_id.clone(), RunKind::Job, &spec.workdir);
        run.engine = ENGINE_ID.to_string();
        run.transport = Some(Transport::StreamJson);
        run.context_id = spec.conversation_id.clone();

        // Fast path: a workdir that already has a live job is a conflict, so
        // no child is spawned at all. The authoritative check happens again
        // under the same lock at insert time (below) to close the race where
        // a second `start_job` for the same workdir lands between this probe
        // and the insert.
        if self
            .jobs
            .lock()
            .map_err(|e| EngineError::Internal(format!("claude job map poisoned: {e}")))?
            .contains_key(&run_id)
        {
            return Err(EngineError::Conflict(format!(
                "a claude job for workdir '{}' is already running",
                spec.workdir.display()
            )));
        }

        let events = Arc::new(Mutex::new(JobEvents::new(JOB_EVENT_BUFFER_CAP)));
        let session_id = Arc::new(Mutex::new(None));
        let (child, stdin) = spawn_job(
            &self.binary,
            &self.mode,
            &run,
            None,
            &spec.prompt,
            events.clone(),
            session_id.clone(),
        )?;

        // Two jobs cannot silently overwrite each other in the `jobs` map: a
        // run id that already has a live handle is a conflict, not a replace
        // (the check and the insert share one lock acquisition so a racing
        // start_job with the same workdir cannot interleave in between).
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|e| EngineError::Internal(format!("claude job map poisoned: {e}")))?;
        if jobs.contains_key(&run_id) {
            // The freshly spawned child must not be leaked by replacing the
            // handle; it is a new process for a workdir that already has a
            // live job, so it is killed and reaped.
            let mut stray = child;
            let _ = stray.kill();
            let _ = stray.wait();
            return Err(EngineError::Conflict(format!(
                "a claude job for workdir '{}' is already running",
                spec.workdir.display()
            )));
        }
        jobs.insert(
            run_id.clone(),
            JobHandle {
                child,
                stdin: Some(stdin),
                session_id,
                events,
                terminal: None,
            },
        );
        Ok(run_id)
    }

    async fn resume_job(&self, run: &Run, prompt: &str) -> Result<(), EngineError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|e| EngineError::Internal(format!("claude job map poisoned: {e}")))?;
        let handle = jobs.get_mut(&run.id).ok_or_else(|| EngineError::NotFound {
            run: run.id.clone(),
        })?;
        // A resume is keyed on the harness conversation/session id.
        let resume_session = if let Some(id) = run.context_id.clone() {
            id
        } else {
            // A caller that lost the resolved id can still resume through the
            // harness session id the output stream captured.
            handle
                .session_id
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .unwrap_or_default()
        };
        if resume_session.is_empty() {
            return Err(EngineError::Conflict(format!(
                "job '{}' has no conversation id; cannot resume",
                run.id
            )));
        }
        // One resume at a time: the previous child must have exited. The
        // `terminal` slot keeps the observed outcome across `job_status`
        // probes, because `Child::try_wait` reports an exit only once.
        let exited = match handle.terminal {
            Some(RunStatus::Done) | Some(RunStatus::Failed) | Some(RunStatus::Killed) => true,
            Some(_) => false,
            None => match handle.child.try_wait() {
                Ok(Some(status)) => {
                    handle.terminal = Some(if status.success() {
                        RunStatus::Done
                    } else {
                        RunStatus::Failed
                    });
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    return Err(EngineError::Internal(format!(
                        "failed to probe claude job child: {e}"
                    )))
                }
            },
        };
        if !exited {
            return Err(EngineError::Conflict(format!(
                "job '{}' is still running; cannot resume",
                run.id
            )));
        }
        // Reuse the handle's event/session buffers so the resumed turn's
        // events are contiguous with the previous run's.
        let run_events = Arc::clone(&handle.events);
        let run_session = Arc::clone(&handle.session_id);
        let (child, stdin) = spawn_job(
            &self.binary,
            &self.mode,
            run,
            Some(&resume_session),
            prompt,
            Arc::clone(&run_events),
            Arc::clone(&run_session),
        )?;
        *handle = JobHandle {
            child,
            stdin: Some(stdin),
            session_id: run_session,
            events: run_events,
            terminal: None,
        };
        Ok(())
    }

    async fn job_status(&self, run: &Run) -> Result<RunStatus, EngineError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|e| EngineError::Internal(format!("claude job map poisoned: {e}")))?;
        let handle = jobs.get_mut(&run.id).ok_or_else(|| EngineError::NotFound {
            run: run.id.clone(),
        })?;
        if let Some(terminal) = handle.terminal {
            return Ok(terminal);
        }
        match handle.child.try_wait() {
            Ok(Some(status)) => {
                let outcome = if status.success() {
                    RunStatus::Done
                } else {
                    RunStatus::Failed
                };
                handle.terminal = Some(outcome);
                Ok(outcome)
            }
            Ok(None) => Ok(RunStatus::Running),
            Err(e) => Err(EngineError::Internal(format!(
                "failed to probe claude job child: {e}"
            ))),
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

    async fn events(&self, run: &Run, after: u64) -> Result<Vec<EngineEvent>, EngineError> {
        match run.kind {
            RunKind::Job => {
                let jobs = self
                    .jobs
                    .lock()
                    .map_err(|e| EngineError::Internal(format!("claude job map poisoned: {e}")))?;
                let handle = jobs.get(&run.id).ok_or_else(|| EngineError::NotFound {
                    run: run.id.clone(),
                })?;
                let buffer = handle.events.lock().map_err(|e| {
                    EngineError::Internal(format!("claude job events poisoned: {e}"))
                })?;
                Ok(buffer.after(after))
            }
            // Session structured reads come from the claude transcript.
            RunKind::Session => {
                let adapter = self.adapter("session.log")?;
                adapter.events(run, after)
            }
        }
    }

    fn capabilities(&self) -> CapabilityTable {
        // The claude job driver (engine-aware runner) provides the full job
        // lifecycle over the same HTTP/CLI surface as agy (HL-71 S1 engine
        // parity): start/status/retry/events plus list/kill/result and the
        // worktree inspection endpoints, which are engine-agnostic (they
        // operate on the job row / worktree and already parse claude's
        // stream-json text). Tested end-to-end through the black-box suite
        // (tests/cli/run-tests.sh, T172+) and the job.rs/result.rs unit
        // tests.
        CapabilityTable::builder(ENGINE_ID)
            .transport(Transport::StreamJson)
            .native(&[
                "job.start",
                "job.status",
                "job.list",
                "job.kill",
                "job.retry",
                "job.events",
                "job.result",
                "workspace.*",
                "session.start",
                "session.stop",
                "session.restart",
                "session.log",
                "session.message",
                // Design HL-113 §7.14: the harness puts queued A2A messages
                // into the model's context (mcp-a2a tool_result + the session
                // hooks); the supported channels are in `inject_channels`.
                "a2a.inject",
            ])
            .emulated(&[
                // HL-71 S2: the job-scoped message surface is fronted by the
                // E4 emulation layer for every engine without a live mid-run
                // channel. Claude's PTY adapter handles `session.message`
                // natively when a session exists, but a job-mode run (worker,
                // no PTY) has no live channel -- `send_message` falls back to
                // the per-run queue, so `a2a.send_message` is emulated for the
                // job surface (the wrapper never reports a job message 501).
                "a2a.send_message",
            ])
            .build()
    }
}

/// Reads the job's stream-json stdout line by line, records the harness
/// session id when the harness echoes one and appends the mapped engine
/// events to the handle's bounded buffer. Runs on a dedicated thread so job
/// output is captured without blocking `start_job`/`resume_job`.
fn read_job_stream(
    stdout: ChildStdout,
    events: Arc<Mutex<JobEvents>>,
    session_id: Arc<Mutex<Option<ContextId>>>,
) {
    use std::io::BufRead;
    let reader = std::io::BufReader::new(stdout);
    for line in reader.lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        if let Some(id) = parse_result_session_id(&line) {
            if let Ok(mut slot) = session_id.lock() {
                if slot.is_none() {
                    *slot = Some(id);
                }
            }
        }
        // A malformed line is skipped (the reader stays alive for the rest of
        // the stream), matching the parser's tolerant contract.
        if let Ok(message) = parse_stream_json_line(&line) {
            let mapped = stream_events(std::slice::from_ref(&message));
            if !mapped.is_empty() {
                if let Ok(mut buffer) = events.lock() {
                    for event in mapped {
                        buffer.push(event);
                    }
                }
            }
        }
    }
}

/// Derives a stable run id from a workdir (jobs have no supervisor-seeded
/// name; the path's final component is the natural id).
fn run_id_from_workdir(workdir: &std::path::Path) -> String {
    workdir
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "claude-job".to_string())
}

/// Session naming mirrors the supervisor config: `SessionConfig.name` is the
/// canonical handle for `start`/`stop`/`message`. When starting by workdir
/// alone, the final path component is the natural name.
fn session_name_from_workdir(workdir: &std::path::Path) -> String {
    run_id_from_workdir(workdir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::capability::Capability;

    /// Writes a fake `claude` shell script into `dir` that behaves like the
    /// stream-json harness for the (hermetic) job tests: it reads one line
    /// from stdin (the rendered text request), records its own argv and the
    /// received line into files in `dir`, then emits a `system` +
    /// `assistant` + `result` stream-json sequence carrying `session_id`
    /// `"fake-session-1"`. The script is used through the `test-hooks` sandbox
    /// passthrough seam ([`crate::sandbox::testhook`]), so no sandbox runtime
    /// is needed.
    #[cfg(feature = "test-hooks")]
    fn write_fake_claude(dir: &std::path::Path) -> std::path::PathBuf {
        let argv_log = dir.join("argv.log");
        let stdin_log = dir.join("stdin.log");
        let script = dir.join("fake-claude.sh");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo "$@" > '{argv}'
if IFS= read -r line; then
  printf '%s\n' "$line" > '{stdin}'
fi
printf '%s\n' '{{"type":"system","subtype":"init"}}'
printf '%s\n' '{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"hello from fake claude"}}]}},"session_id":"fake-session-1"}}'
printf '%s\n' '{{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"fake-session-1"}}'
exit 0
"#,
                argv = argv_log.display(),
                stdin = stdin_log.display(),
            ),
        )
        .unwrap();
        #[allow(unused_imports)]
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        script
    }

    /// Polls up to `timeout_ms` until `predicate` returns `true` (used to wait
    /// for the reader thread to drain the job's stream-json output).
    #[cfg(feature = "test-hooks")]
    fn wait_until(timeout_ms: u64, mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        while std::time::Instant::now() < deadline {
            if predicate() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        predicate()
    }

    /// Builds a `JobSpec` rooted at a temp worktree whose parent is another
    /// temp dir (the job's policy file and the run's artifacts live there).
    #[cfg(feature = "test-hooks")]
    fn job_spec(root: &std::path::Path, prompt: &str) -> JobSpec {
        let workdir = root.join("proj");
        std::fs::create_dir_all(&workdir).unwrap();
        JobSpec {
            prompt: prompt.to_string(),
            conversation_id: None,
            mode: "normal".to_string(),
            workdir,
            project: "hl91".to_string(),
            options: serde_json::Value::Null,
        }
    }

    /// Builds a `ClaudeEngine` whose binary is a fake claude script written
    /// into `tmp`, with the `test-hooks` sandbox passthrough explicitly on so
    /// the child runs unwrapped.
    #[cfg(feature = "test-hooks")]
    fn job_engine_with(tmp: &tempfile::TempDir) -> ClaudeEngine {
        let fake = write_fake_claude(tmp.path());
        crate::sandbox::testhook::set_sandbox_passthrough(true);
        ClaudeEngine::new(fake, None)
    }

    #[test]
    fn id_is_stable() {
        let engine = ClaudeEngine::new(std::path::PathBuf::from("claude"), None);
        assert_eq!(engine.id(), "claude");
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
            "claude-job"
        );
    }

    #[test]
    fn session_ops_without_supervisor_answer_unsupported() {
        let engine = ClaudeEngine::new(std::path::PathBuf::from("claude"), None);
        let mut ctx = crate::engine::EmptyEngineCtx;
        let spec = SessionSpec {
            workdir: std::path::PathBuf::from("/workspace/proj"),
            transport: Some(Transport::StreamJson),
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
    }

    #[test]
    fn capabilities_honest_transport_and_keys() {
        let engine = ClaudeEngine::new(std::path::PathBuf::from("claude"), None);
        let table = engine.capabilities();
        assert_eq!(table.engine, "claude");
        assert_eq!(table.transport, Some(Transport::StreamJson));
        assert_eq!(table.capability("job.start"), Some(Capability::Native));
        assert_eq!(table.capability("job.status"), Some(Capability::Native));
        // job.events / job.retry are native (event buffer + --resume).
        assert_eq!(table.capability("job.retry"), Some(Capability::Native));
        assert_eq!(table.capability("job.events"), Some(Capability::Native));
        // HL-71 S1: the claude job driver reaches the full job lifecycle and
        // the worktree endpoints over the same surface as agy (list/kill/
        // result are engine-agnostic; the black-box suite proves them).
        assert_eq!(table.capability("job.list"), Some(Capability::Native));
        assert_eq!(table.capability("job.kill"), Some(Capability::Native));
        assert_eq!(table.capability("job.result"), Some(Capability::Native));
        assert_eq!(table.capability("workspace.*"), Some(Capability::Native));
        assert_eq!(table.capability("session.start"), Some(Capability::Native));
        assert_eq!(table.capability("session.log"), Some(Capability::Native));
        assert_eq!(
            table.capability("session.message"),
            Some(Capability::Native)
        );
        // Control endpoints the engine does not implement are honest gaps.
        assert_eq!(table.capability("interrupt"), Some(Capability::Unsupported));
        assert_eq!(table.capability("fork"), Some(Capability::Unsupported));
    }

    #[test]
    fn unsupported_job_resume_is_honest() {
        let engine = ClaudeEngine::new(std::path::PathBuf::from("claude"), None);
        let run = Run::new("nope", RunKind::Job, "/workspace/proj");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt.block_on(engine.resume_job(&run, "continue"));
        assert!(matches!(err, Err(EngineError::NotFound { .. })));
    }

    #[test]
    fn event_gap_for_unknown_job_is_not_found() {
        let engine = ClaudeEngine::new(std::path::PathBuf::from("claude"), None);
        let run = Run::new("missing", RunKind::Job, "/workspace/proj");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt.block_on(engine.events(&run, 0));
        assert!(matches!(err, Err(EngineError::NotFound { .. })));
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn start_job_runs_fake_claude_and_captures_status_events_session() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = job_engine_with(&tmp);
        let spec = job_spec(tmp.path(), "say hi");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let run_id = rt.block_on(async {
            let mut ctx = crate::engine::EmptyEngineCtx;
            engine.start_job(spec, &mut ctx).await.expect("start_job")
        });
        assert_eq!(run_id, "proj");

        // The delivered prompt must reach the fake claude as exactly one
        // newline-terminated stream-json user line.
        assert!(wait_until(5000, || tmp.path().join("stdin.log").exists()));
        let stdin_log = std::fs::read_to_string(tmp.path().join("stdin.log")).unwrap();
        let line = stdin_log.trim_end();
        let value: serde_json::Value = serde_json::from_str(line).expect("stdin line is json");
        assert_eq!(value["type"], "user");
        assert_eq!(value["message"]["content"][0]["text"], "say hi");
        // The fake claude argv carried the stream-json headless flags.
        let argv_log = std::fs::read_to_string(tmp.path().join("argv.log")).unwrap();
        assert!(argv_log.contains("--input-format"));
        assert!(argv_log.contains("stream-json"));
        assert!(argv_log.contains("--output-format"));
        // Token set of the recorded argv (the fake records `echo "$@"`,
        // space-joined). `--verbose` is last on a fresh start, so a
        // token-boundary match via split, not a substring, is used.
        let argv_tokens: Vec<&str> = argv_log.split_whitespace().collect();
        // `--output-format stream-json` with `-p` requires `--verbose` on the
        // real claude CLI.
        assert!(
            argv_tokens.contains(&"--verbose"),
            "claude job argv must carry --verbose: {argv_log}"
        );
        // The agy-only `--mode` flag must never reach the claude harness (the
        // real CLI rejects it with `error: unknown option '--mode'`). A token
        // match, so `--permission-mode` does not trip it.
        assert!(
            !argv_tokens.contains(&"--mode"),
            "claude job argv must not carry the agy-only --mode flag: {argv_log}"
        );

        let run = Run::new(run_id.clone(), RunKind::Job, tmp.path().join("proj"));

        // job_status ends Done (the fake script exits 0). The reader thread
        // and the child exit asynchronously, so poll until Done.
        assert!(wait_until(5000, || rt
            .block_on(engine.job_status(&run))
            .unwrap_or(RunStatus::Running)
            == RunStatus::Done));

        // events carry the assistant text and the result/session id is
        // captured into the handle for resume/context.
        assert!(wait_until(5000, || {
            rt.block_on(engine.events(&run, 0))
                .map(|events| {
                    events.iter().any(|ev| {
                        matches!(
                            ev,
                            EngineEvent::MessageDelta {
                                role: crate::engine::run::Role::Assistant,
                                text,
                            } if text == "hello from fake claude"
                        )
                    })
                })
                .unwrap_or(false)
        }));
        // Cursor semantics: after the first event, the assistant text is
        // still present when the cursor is 0; a cursor past the buffer tail
        // yields an empty list.
        let after_all = rt.block_on(engine.events(&run, 100)).unwrap();
        assert!(after_all.is_empty());

        // The harness session id is stored on the handle (used for resume
        // and for the run's context_id).
        let jobs = engine.jobs.lock().unwrap();
        let handle = jobs.get(&run_id).unwrap();
        assert_eq!(
            handle.session_id.lock().unwrap().as_deref(),
            Some("fake-session-1")
        );
        drop(jobs);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn resume_job_spawns_with_resume_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = job_engine_with(&tmp);
        let spec = job_spec(tmp.path(), "first prompt");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let run_id = rt.block_on(async {
            let mut ctx = crate::engine::EmptyEngineCtx;
            engine.start_job(spec, &mut ctx).await.expect("start_job")
        });
        let workdir = tmp.path().join("proj");
        let run = Run::new(run_id.clone(), RunKind::Job, &workdir);

        // Wait for the first child to drive its output to EOF (the handle
        // then reports Done) AND the harness session id to be captured by the
        // reader thread (resume relies on it when the run has no context_id).
        assert!(wait_until(5000, || {
            rt.block_on(engine.job_status(&run))
                .unwrap_or(RunStatus::Pending)
                == RunStatus::Done
                && engine
                    .jobs
                    .lock()
                    .unwrap()
                    .get(&run_id)
                    .and_then(|h| h.session_id.lock().unwrap().clone())
                    .is_some()
        }));

        // Resuming with the harness context id must spawn a new child whose
        // argv carries `--resume fake-session-1`.
        let resumed = rt
            .block_on(engine.resume_job(&run, "continue please"))
            .is_ok();
        assert!(resumed, "resume_job must succeed after the child exited");
        // The fake claude records its argv right at startup; wait for it to
        // be overwritten by the resumed spawn before asserting.
        assert!(wait_until(5000, || {
            std::fs::read_to_string(tmp.path().join("argv.log"))
                .map(|log| log.contains("--resume") && log.contains("fake-session-1"))
                .unwrap_or(false)
        }));
        let argv_log = std::fs::read_to_string(tmp.path().join("argv.log")).unwrap();
        assert!(
            argv_log.contains("--resume"),
            "resume must pass --resume to the harness: {argv_log}"
        );
        assert!(
            argv_log.contains("fake-session-1"),
            "resume must name the harness session id: {argv_log}"
        );

        // The resumed job is live again and eventually Done.
        assert!(wait_until(5000, || rt
            .block_on(engine.job_status(&run))
            .unwrap_or(RunStatus::Pending)
            == RunStatus::Done));
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn resume_requires_exited_child() {
        let tmp = tempfile::tempdir().unwrap();
        // A fake claude that never exits: the resume is refused while the
        // child is still running.
        let script = tmp.path().join("slow-claude.sh");
        std::fs::write(&script, "#!/bin/sh\nwhile :; do read -r l || break; done\n").unwrap();
        #[allow(unused_imports)]
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        crate::sandbox::testhook::set_sandbox_passthrough(true);
        let engine = ClaudeEngine::new(script, None);
        let spec = job_spec(tmp.path(), "slow");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let run_id = rt.block_on(async {
            let mut ctx = crate::engine::EmptyEngineCtx;
            engine.start_job(spec, &mut ctx).await.expect("start_job")
        });
        // Give the child a moment to be live.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut run = Run::new(run_id.clone(), RunKind::Job, "/workspace/proj");
        run.context_id = Some("fake-session-1".to_string());
        let err = rt.block_on(engine.resume_job(&run, "go"));
        assert!(
            matches!(err, Err(EngineError::Conflict(_))),
            "resume while running must conflict, got {err:?}"
        );
        // Cleanup: kill the never-exiting child so the test does not leak it.
        let mut jobs = engine.jobs.lock().unwrap();
        if let Some(handle) = jobs.get_mut(&run_id) {
            let _ = handle.child.kill();
        }
        drop(jobs);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn start_job_same_workdir_conflicts() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = job_engine_with(&tmp);
        let spec = job_spec(tmp.path(), "one");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let run_id = rt.block_on(async {
            let mut ctx = crate::engine::EmptyEngineCtx;
            engine
                .start_job(spec.clone(), &mut ctx)
                .await
                .expect("start")
        });
        assert_eq!(run_id, "proj");
        let err = rt.block_on(async {
            let mut ctx = crate::engine::EmptyEngineCtx;
            engine.start_job(spec, &mut ctx).await
        });
        assert!(
            matches!(err, Err(EngineError::Conflict(_))),
            "second start for the same workdir must conflict, got {err:?}"
        );
    }
}

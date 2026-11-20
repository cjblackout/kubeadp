use crate::orchestrate::backoff::{BackoffManager, CrashWindowTracker, SlideWindowBackoff};
use crate::orchestrate::claude_config::ensure_claude_config;
use crate::orchestrate::config::{warn_if_symlink_workdir, SessionConfig, SupervisorConfig};
use crate::orchestrate::logging::log_event;
use crate::orchestrate::pty::{
    kill_process_group_with_grace, poll_and_read_pty, spawn_in_pty, KillResult, Pty,
};
use crate::orchestrate::session_file::{
    classify_transcript, get_transcript_fingerprint, output_contains_resume_not_found,
    quarantine_corrupt_session_file, ResumeFailTracker, TranscriptStatus, RESUME_NOT_FOUND_ERROR,
};
use crate::orchestrate::state::{
    save_supervisor_state, SessionState, SessionStatus, SupervisorState,
};
use crate::orchestrate::stdout_writer::StdoutSender;

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

/// Truncates the byte buffer to at most `max_bytes` then lossy-decodes to UTF-8,
/// replacing newlines with `\n`. Never panics on non-char boundaries.
pub fn format_tail_utf8_safe(buf: &[u8], max_bytes: usize) -> String {
    let slice = if buf.len() > max_bytes {
        &buf[buf.len() - max_bytes..]
    } else {
        buf
    };
    String::from_utf8_lossy(slice).replace('\n', "\\n")
}

#[allow(clippy::too_many_arguments)]
pub fn run_session_loop(
    session: SessionConfig,
    supervisor_config: SupervisorConfig,
    state_root: PathBuf,
    shared_state: Arc<Mutex<SupervisorState>>,
    shutdown: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    pty_input: Arc<Mutex<crate::orchestrate::pty::PtyInput>>,
    mut stdout_sender: StdoutSender,
) {
    // Register the session in the process-wide jobset registry under its name,
    // the id peers address it by over A2A (idempotent; a
    // respawned session reuses its jobset). The entry is dropped when this
    // thread exits (the drop guard covers Stopped, Failed, and any unexpected
    // exit), so the A2A surface stops accepting the session's credential the
    // moment the session ends.
    crate::core::jobset::register_agent(&session.name);
    let _session_jobset_drop = crate::core::jobset::JobsetDropGuard::new(session.name.clone());

    // Fix #4: use SlideWindowBackoff for backoff computation.
    let mut swb = SlideWindowBackoff::new(
        session.base_backoff_secs,
        session.max_backoff_secs,
        session.backoff_window_secs,
    );
    // Keep BackoffManager alive for compat (on_run_finished / PTY-eof path still uses it).
    let mut backoff_mgr = BackoffManager::new(
        session.base_backoff_secs,
        session.max_backoff_secs,
        session.healthy_run_secs,
    );
    let mut crash_tracker = CrashWindowTracker::new(session.max_crashes, session.crash_window_secs);
    let mut restart_count: u32 = 0;
    let mut force_resume = false;
    let mut resume_fail_tracker = ResumeFailTracker::new();

    warn_if_symlink_workdir(&session.name, &session.workdir);

    let home = match std::env::var("HOME") {
        Ok(h) if !h.trim().is_empty() => PathBuf::from(h.trim()),
        _ => PathBuf::from("/home/coder"),
    };
    let cfg_path = supervisor_config
        .claude_config_dir
        .as_ref()
        .and_then(|p| p.parent().map(|d| d.join(".claude.json")))
        .unwrap_or_else(|| home.join(".claude.json"));
    let settings_path = supervisor_config
        .claude_config_dir
        .as_ref()
        .map(|p| p.join("settings.json"))
        .unwrap_or_else(|| home.join(".claude").join("settings.json"));

    while !shutdown.load(Ordering::SeqCst) && !paused.load(Ordering::SeqCst) {
        // Test-only trigger: simulate an unhandled thread termination
        #[cfg(feature = "test-hooks")]
        if let Ok(die_file) = std::env::var("CLAUDE_SUPERVISOR_TEST_THREAD_DIE") {
            let p = Path::new(&die_file);
            if p.exists() {
                let _ = std::fs::remove_file(p);
                log_event(
                    &session.name,
                    "Test trigger: exiting session thread immediately",
                );
                return;
            }
        }

        let child_pid_tracker = Arc::new(AtomicU32::new(0));
        let child_pid_clone = Arc::clone(&child_pid_tracker);
        // The child's process-identity anchor (`/proc/<pid>/stat` start
        // ticks), recorded fresh each attempt at spawn alongside the pid, so
        // the recovery/cooldown kill paths can tell the recorded child from
        // an unrelated process that later reused the PID (never signal a
        // recycled PID).
        let child_start_tracker = Arc::new(AtomicU64::new(0));
        let child_start_clone = Arc::clone(&child_start_tracker);
        let session_name = session.name.clone();

        let iteration_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Test-only trigger: simulate an unhandled panic in the session loop
            #[cfg(feature = "test-hooks")]
            if let Ok(panic_file) = std::env::var("CLAUDE_SUPERVISOR_TEST_PANIC_ONCE") {
                let p = Path::new(&panic_file);
                if p.exists() {
                    let _ = std::fs::remove_file(p);
                    panic!("test-induced intentional panic");
                }
            }

            run_session_iteration(
                &session,
                &supervisor_config,
                &state_root,
                &shared_state,
                &shutdown,
                &paused,
                &cfg_path,
                &settings_path,
                &mut swb,
                &mut backoff_mgr,
                &mut crash_tracker,
                &mut restart_count,
                &child_pid_clone,
                &child_start_clone,
                &mut stdout_sender,
                &mut force_resume,
                &mut resume_fail_tracker,
                &pty_input,
            )
        }));

        match iteration_result {
            Ok(should_continue) => {
                if !should_continue || shutdown.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(payload) => {
                let panic_msg = if let Some(s) = payload.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic payload".to_string()
                };

                log_event(
                    &session_name,
                    &format!("Session loop caught panic: {}. Recovering...", panic_msg),
                );

                // Reclaim any PTY master fd stored before the panic so a closed
                // raw fd cannot remain published in `pty_input` during backoff.
                pty_input.lock().unwrap_or_else(|e| e.into_inner()).take();

                // Terminate child process group if child was running
                let pid = child_pid_tracker.load(Ordering::SeqCst);
                if pid > 0 {
                    let start_ticks = child_start_tracker.load(Ordering::SeqCst);
                    let start_time = (start_ticks > 0).then_some(start_ticks);
                    crate::orchestrate::pty::signal_process_group_if_target(
                        pid,
                        start_time,
                        libc::SIGKILL,
                    );
                    let mut status: libc::c_int = 0;
                    let _ = unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) };
                }

                // Account as crash
                restart_count += 1;
                let now_epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                let failed = crash_tracker.record_crash(now_epoch);

                // Clone state, release lock, THEN write (fix #1).
                let state_snapshot = {
                    let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(sess_st) = st.sessions.get_mut(&session.name) {
                        sess_st.pid = None;
                        sess_st.restart_count = restart_count;
                        sess_st.last_exit_reason = Some(format!("panic: {}", panic_msg));
                        if failed {
                            sess_st.status = SessionStatus::Failed;
                        } else {
                            sess_st.status = SessionStatus::Backoff;
                        }
                    }
                    st.clone()
                };
                let _ = save_supervisor_state(&state_root, &state_snapshot);

                if failed {
                    // Fix #3: failed cooldown instead of sleeping forever.
                    handle_failed_cooldown(
                        &session,
                        &state_root,
                        &shared_state,
                        &shutdown,
                        &paused,
                        Some(&child_pid_tracker),
                        Some(&child_start_tracker),
                        &mut crash_tracker,
                        &mut restart_count,
                        None,
                    );
                    // After cooldown we resume supervising (break this match arm
                    // so the outer while loop continues).
                    continue;
                }

                // Fix #4: use sliding-window backoff.
                let backoff_duration = swb.record_restart_and_delay(now_epoch);
                log_event(
                    &session.name,
                    &format!(
                        "Backing off for {:.2}s before retry following panic...",
                        backoff_duration.as_secs_f64()
                    ),
                );
                let sleep_start = Instant::now();
                while sleep_start.elapsed() < backoff_duration {
                    if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    // On shutdown exit
    let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(sess_st) = st.sessions.get_mut(&session.name) {
        if sess_st.status != SessionStatus::Failed {
            sess_st.status = SessionStatus::Stopped;
        }
    }
}

fn is_pid_alive_sys(pid: u32) -> bool {
    let mut status: libc::c_int = 0;
    let res = unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) };
    if res > 0 {
        return false;
    }
    if res < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
        let ret = unsafe { libc::kill(pid as i32, 0) };
        if ret != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return false;
        }
        return true;
    }
    if res == 0 {
        return true;
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn handle_failed_cooldown(
    session: &SessionConfig,
    state_root: &Path,
    shared_state: &Arc<Mutex<SupervisorState>>,
    shutdown: &Arc<AtomicBool>,
    paused: &Arc<AtomicBool>,
    child_pid_tracker: Option<&Arc<AtomicU32>>,
    child_start_tracker: Option<&Arc<AtomicU64>>,
    crash_tracker: &mut CrashWindowTracker,
    restart_count: &mut u32,
    stuck_pid: Option<u32>,
) {
    handle_failed_cooldown_custom(
        session,
        state_root,
        shared_state,
        shutdown,
        paused,
        child_pid_tracker,
        child_start_tracker,
        crash_tracker,
        restart_count,
        stuck_pid,
        is_pid_alive_sys,
    );
}

#[allow(clippy::too_many_arguments)]
pub fn handle_failed_cooldown_custom<F>(
    session: &SessionConfig,
    state_root: &Path,
    shared_state: &Arc<Mutex<SupervisorState>>,
    shutdown: &Arc<AtomicBool>,
    paused: &Arc<AtomicBool>,
    child_pid_tracker: Option<&Arc<AtomicU32>>,
    child_start_tracker: Option<&Arc<AtomicU64>>,
    crash_tracker: &mut CrashWindowTracker,
    restart_count: &mut u32,
    stuck_pid: Option<u32>,
    mut is_pid_alive_fn: F,
) where
    F: FnMut(u32) -> bool,
{
    let cooldown = Duration::from_secs_f64(session.failed_cooldown_secs.max(0.01));

    if let Some(pid) = stuck_pid {
        log_event(
            &session.name,
            &format!(
                "Session marked failed (unkillable child pid {}). Entering cooldown for {:.0}s before checking for pid disappearance.",
                pid,
                cooldown.as_secs_f64()
            ),
        );
    } else {
        log_event(
            &session.name,
            &format!(
                "Session marked failed after {} crashes within {:.1}s. Entering cooldown for {:.0}s before retrying.",
                session.max_crashes,
                session.crash_window_secs,
                cooldown.as_secs_f64()
            ),
        );
    }

    let cooldown_start = Instant::now();
    while cooldown_start.elapsed() < cooldown {
        if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }

    if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
        return;
    }

    if let Some(pid) = stuck_pid {
        while is_pid_alive_fn(pid) {
            if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_millis(200));
        }

        if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
            return;
        }

        if let Some(tracker) = child_pid_tracker {
            tracker.store(0, Ordering::SeqCst);
        }
        if let Some(start_tracker) = child_start_tracker {
            start_tracker.store(0, Ordering::SeqCst);
        }
        log_event(
            &session.name,
            &format!(
                "Stuck child pid {} has disappeared. Clearing crash tracker and resuming supervision.",
                pid
            ),
        );
    } else {
        log_event(
            &session.name,
            "Failed cooldown expired. Clearing crash tracker and resuming supervision.",
        );
    }

    crash_tracker.clear();
    *restart_count = 0;

    let state_snapshot = {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.pid = None;
            sess_st.status = SessionStatus::Starting;
        }
        st.clone()
    };
    let _ = save_supervisor_state(state_root, &state_snapshot);
}

/// Bounded escalation helper:
/// Checks try_wait / waitpid. If still alive:
/// 1. Sends SIGTERM to child process group.
/// 2. Polls up to `kill_grace` with WNOHANG.
/// 3. If still alive: sends SIGKILL to child process group.
/// 4. Polls up to `kill_wait` with WNOHANG.
/// 5. If reaped: returns KillResult::Reaped.
/// 6. If not reaped within deadline: returns KillResult::Unkillable.
///
/// `start_time` is the process identity anchor recorded at spawn (see
/// [`crate::orchestrate::pty::is_same_process`]): before every signal the
/// PID is re-verified, so a recycled PID held by an unrelated process is
/// never signalled (the escalation signatures are unchanged in effect -- the
/// identity check only ever refuses to signal a PID that no longer belongs
/// to the child this `Child` handle describes).
pub fn bounded_child_escalation<F>(
    child_pid: u32,
    start_time: Option<u64>,
    child: &mut std::process::Child,
    kill_grace: Duration,
    kill_wait: Duration,
    mut waitpid_fn: F,
) -> KillResult
where
    F: FnMut(libc::pid_t, *mut libc::c_int, libc::c_int) -> libc::pid_t,
{
    let mut status: libc::c_int = 0;
    let res = waitpid_fn(child_pid as i32, &mut status, libc::WNOHANG);
    if res > 0 {
        let _ = child.try_wait();
        return KillResult::Reaped(Some(status));
    } else if res < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
        let _ = child.try_wait();
        return KillResult::Reaped(None);
    }

    let _ = crate::orchestrate::pty::signal_process_group_if_target(
        child_pid,
        start_time,
        libc::SIGTERM,
    );

    let start = Instant::now();
    let mut reaped = false;
    let mut exit_status = None;

    while start.elapsed() < kill_grace {
        let mut st: libc::c_int = 0;
        let res = waitpid_fn(child_pid as i32, &mut st, libc::WNOHANG);
        if res > 0 {
            exit_status = Some(st);
            reaped = true;
            break;
        } else if res < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
            reaped = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    if !reaped {
        let _ = crate::orchestrate::pty::signal_process_group_if_target(
            child_pid,
            start_time,
            libc::SIGKILL,
        );
        let kill_start = Instant::now();
        while kill_start.elapsed() < kill_wait {
            let mut st: libc::c_int = 0;
            let res = waitpid_fn(child_pid as i32, &mut st, libc::WNOHANG);
            if res > 0 {
                exit_status = Some(st);
                reaped = true;
                break;
            } else if res < 0
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
            {
                reaped = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    if reaped {
        let _ = child.try_wait();
        for _ in 0..1024 {
            let mut st: libc::c_int = 0;
            let res = waitpid_fn(-(child_pid as i32), &mut st, libc::WNOHANG);
            if res <= 0 {
                break;
            }
        }
        KillResult::Reaped(exit_status)
    } else {
        KillResult::Unkillable
    }
}

struct SessionIterationContext<'a> {
    session: &'a SessionConfig,
    state_root: &'a Path,
    shared_state: &'a Arc<Mutex<SupervisorState>>,
    shutdown: &'a Arc<AtomicBool>,
    paused: &'a Arc<AtomicBool>,
}

#[allow(clippy::too_many_arguments)]
fn run_session_iteration(
    session: &SessionConfig,
    supervisor_config: &SupervisorConfig,
    state_root: &Path,
    shared_state: &Arc<Mutex<SupervisorState>>,
    shutdown: &Arc<AtomicBool>,
    paused: &Arc<AtomicBool>,
    cfg_path: &Path,
    settings_path: &Path,
    swb: &mut SlideWindowBackoff,
    backoff_mgr: &mut BackoffManager,
    crash_tracker: &mut CrashWindowTracker,
    restart_count: &mut u32,
    child_pid_tracker: &Arc<AtomicU32>,
    child_start_tracker: &Arc<AtomicU64>,
    stdout_sender: &mut StdoutSender,
    force_resume: &mut bool,
    resume_fail_tracker: &mut ResumeFailTracker,
    pty_input: &Arc<Mutex<crate::orchestrate::pty::PtyInput>>,
) -> bool {
    let ctx = SessionIterationContext {
        session,
        state_root,
        shared_state,
        shutdown,
        paused,
    };

    // The engine this session runs on (HL-91 E5 cross-use): per-session
    // `engine` > `default_session_engine` > "claude". It selects the spawn
    // args, the setup steps and the message-delivery format. claude is the
    // historical supervisor PTY path; agy uses its live stream-json mode
    // (`--print "" --input-format stream-json`) reading consecutive turns
    // from the PTY, supervised by the same crash/restart/backoff loop.
    // `default_session_engine` may come from the supervisor config OR the
    // worker-side core config file (mirror of `AGENT_ORCHESTRATE_DEFAULT_SESSION_ENGINE`
    // on the supervisor side); both are resolved here so the seed below agrees
    // with the engine actually launched.
    let default_session_engine = supervisor_config
        .default_session_engine
        .clone()
        .or_else(|| {
            crate::core::config::Config::load()
                .ok()
                .and_then(|c| c.default_session_engine)
        });
    let engine = session.effective_engine(default_session_engine.as_deref());

    // 1. Engine-specific setup. claude needs its headless configuration
    // (onboarding/trust/dangerous-mode), the A2A hook settings, the
    // per-project CLAUDE.md and the transcript machinery; an agy session
    // needs none of those (its prompts travel as stream-json lines, and A2A
    // delivery uses the typed user events below).
    if engine == "agy" {
        // (nothing to configure for agy: no claude config, no CLAUDE.md, no
        // cjson transcript; the sandbox still applies below)
    } else {
        // 1. Ensure Claude configuration (onboarding, trust, dangerous mode bypass)
        if let Err(e) = ensure_claude_config(cfg_path, settings_path, &[&session.workdir]) {
            log_event(
                &session.name,
                &format!("Failed to update Claude configuration: {}", e),
            );
        }

        // 1b. Merge the A2A hooks into the session settings (design HL-113
        // §7.14, phase 2): PostToolUse / UserPromptSubmit / Stop pointing at
        // `kubeadp-agent a2a-hook`, so the hook channel injects queued messages
        // at the next tool call / prompt / turn boundary. Existing hook entries
        // are kept; the merge is idempotent. FAIL-OPEN: on error, log and start
        // the session without the hooks (the ladder's escalation steps still
        // deliver).
        if let Err(e) = crate::orchestrate::claude_config::ensure_a2a_hooks(settings_path) {
            log_event(
                &session.name,
                &format!("Failed to merge the a2a hook settings: {}", e),
            );
        }
    }

    // 2. Wait until Claude is installed and logged in
    {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        let sess_st = st
            .sessions
            .entry(session.name.clone())
            .or_insert_with(|| SessionState::new(session.name.clone()));
        if sess_st.engine.is_none() {
            // A session state created before engine selection (or by a path
            // that did not seed it) records the effective engine so the API
            // reports it and a restart keeps the engine stable (HL-91 E5).
            sess_st.engine = Some(session.effective_engine(default_session_engine.as_deref()));
        }
        sess_st.status = SessionStatus::Starting;
    }

    let (ready, health_reason) = if engine == "agy" {
        // An agy session needs no login gate: agy runs headless in stream-json
        // mode. Readiness = the binary resolves and answers `--version`.
        crate::orchestrate::readiness::binary_ready(
            &session.agy_binary,
            &session.env,
            Duration::from_secs_f64(session.readiness_poll_secs),
            shutdown,
        )
    } else {
        crate::orchestrate::readiness::wait_until_ready_with_pause(
            &session.claude_binary,
            &session.env,
            &session.name,
            Duration::from_secs_f64(session.readiness_poll_secs),
            shutdown,
            paused,
        )
    };

    {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.last_health_result = Some(health_reason);
        }
    }

    if !ready {
        return !shutdown.load(Ordering::SeqCst) && !paused.load(Ordering::SeqCst);
    }

    // 2b. Materialise the instruction-file hierarchy (stream 6, design 6.1/6.2/6.3)
    // into the engine's witness files before spawning. FAIL-OPEN: on error, log
    // prominently and still start the session.
    //
    // Hierarchy (highest wins last): global -> per-session. The per-session
    // level is the session's private `instructions_file` when set (it takes
    // PRECEDENCE over the legacy `memory_file`), else
    // `<sessions-dir>/<name>.md`; the legacy `memory_file` keeps working
    // unchanged when the new per-session file is absent.
    {
        use crate::core::instructions::{self, ReadOutcome};
        // HOME as the run sees it: the value the (sandboxed or trusted)
        // session child inherits, so the witness files land where the engine
        // actually reads them. Sandboxed sessions run under the pod's
        // `sandbox_home` (default `/home/agent`); a trusted session inherits
        // the supervisor's HOME.
        let run_home = if !session.sandbox {
            std::env::var("HOME")
                .ok()
                .filter(|h| !h.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/home/agent"))
        } else {
            crate::core::config::Config::load()
                .map(|c| c.sandbox_home.clone())
                .unwrap_or_else(|_| PathBuf::from("/home/agent"))
        };

        let global = match instructions::read(&instructions::global_instructions_path()) {
            ReadOutcome::None => None,
            ReadOutcome::Content { path, text } => Some((path, text)),
            ReadOutcome::Missing(path) => {
                log_event(
                    &session.name,
                    &format!(
                        "instructions: global file {} not present; continuing without it",
                        path.display()
                    ),
                );
                None
            }
            ReadOutcome::Unreadable { path, reason } => {
                log_event(
                    &session.name,
                    &format!(
                        "ERROR: instructions: global file {} cannot be read: {} (continuing without it)",
                        path.display(),
                        reason
                    ),
                );
                None
            }
        };

        // Per-session level: an explicit `instructions_file` wins over the
        // default path; the new per-session file takes precedence over the
        // legacy `memory_file` when present.
        let explicit = session.instructions_file.as_deref();
        let per_session_default = instructions::session_instructions_path(&session.name);
        let per_used = explicit
            .map(PathBuf::from)
            .unwrap_or(per_session_default.clone());
        let per_session = match instructions::read(&per_used) {
            ReadOutcome::None => None,
            ReadOutcome::Content { path, text } => Some((path, text)),
            ReadOutcome::Missing(path) => {
                // An explicit per-session instructions_file that is missing is
                // logged once and skipped; the legacy memory_file fallback is
                // handled below.
                log_event(
                    &session.name,
                    &format!(
                        "instructions: per-session file {} not present{}",
                        path.display(),
                        if engine == "claude" && session.memory_file.is_some() {
                            "; falling back to the legacy memory_file"
                        } else {
                            "; continuing without it"
                        }
                    ),
                );
                None
            }
            ReadOutcome::Unreadable { path, reason } => {
                log_event(
                    &session.name,
                    &format!(
                        "ERROR: instructions: per-session file {} cannot be read: {} (continuing without it)",
                        path.display(),
                        reason
                    ),
                );
                None
            }
        };

        let (_materialized, lines) = instructions::materialize(
            &engine,
            &run_home,
            &session.workdir,
            global,
            // claude per-session CLAUDE.md: new instructions_file takes
            // precedence over legacy memory_file when present.
            if per_session.is_some() {
                per_session
            } else if engine == "claude" {
                session.memory_file.as_ref().and_then(|mf| {
                    match instructions::read(mf) {
                        ReadOutcome::Content { path, text } => Some((path, text)),
                        ReadOutcome::Missing(path) => {
                            log_event(
                                &session.name,
                                &format!(
                                    "instructions: legacy memory_file {} not present; continuing without it",
                                    path.display()
                                ),
                            );
                            None
                        }
                        ReadOutcome::Unreadable { path, reason } => {
                            log_event(
                                &session.name,
                                &format!(
                                    "ERROR: instructions: legacy memory_file {} cannot be read: {} (continuing without it)",
                                    path.display(),
                                    reason
                                ),
                            );
                            None
                        }
                        ReadOutcome::None => None,
                    }
                })
            } else {
                None
            },
        );
        for line in &lines {
            log_event(&session.name, line);
        }
    }

    // 3. Check session transcript validity (fix #2: new policy). Claude-only:
    // agy sessions have no ~/.claude transcript; `resolve_session_file` and
    // the quarantine/resume logic are claude-specific.
    let session_file = if engine == "agy" {
        std::path::PathBuf::new()
    } else {
        supervisor_config.resolve_session_file(session)
    };
    if engine != "agy" {
        match classify_transcript(&session_file) {
            TranscriptStatus::Unusable => {
                // Empty / garbage file: quarantine and start fresh.
                *force_resume = false;
                match quarantine_corrupt_session_file(&session_file) {
                    Ok(backup) => {
                        log_event(
                            &session.name,
                            &format!(
                                "Session transcript has no parseable lines. Quarantined {} -> {}",
                                session_file.display(),
                                backup.display()
                            ),
                        );
                    }
                    Err(e) => {
                        log_event(
                            &session.name,
                            &format!("Failed to quarantine unusable session transcript: {}", e),
                        );
                    }
                }
            }
            // Usable or Missing: proceed normally (no quarantine for truncated/corrupt lines).
            TranscriptStatus::Usable | TranscriptStatus::Missing => {}
        }
    }

    let fingerprint_before = get_transcript_fingerprint(&session_file);
    let is_resume = *force_resume || session_file.exists();

    // 4. Build command arguments using the resolved engine's session_command (design B2).
    let core_config = crate::core::config::Config::load().unwrap_or_default();
    let registry = crate::engine::EngineRegistry::with_builtin(&core_config);
    let eng = match registry.resolve(
        crate::engine::RunKind::Session,
        session.engine.as_deref(),
        default_session_engine.as_deref(),
    ) {
        Ok(e) => e,
        Err(e) => {
            log_event(
                &session.name,
                &format!("failed to resolve session engine: {}", e),
            );
            handle_spawn_error(
                &ctx,
                swb,
                crash_tracker,
                restart_count,
                format!("engine-resolve-error: {}", e),
            );
            return true;
        }
    };

    let resume_mode = if is_resume {
        crate::engine::ResumeMode::Resume
    } else {
        crate::engine::ResumeMode::Start
    };

    let launch_plan = match eng.session_command(session, resume_mode) {
        Ok(p) => p,
        Err(e) => {
            log_event(
                &session.name,
                &format!("failed to build session launch plan: {}", e),
            );
            handle_spawn_error(
                &ctx,
                swb,
                crash_tracker,
                restart_count,
                format!("launch-plan-error: {}", e),
            );
            return true;
        }
    };

    let mut cmd = Command::new(&launch_plan.program);
    cmd.current_dir(&session.workdir);
    cmd.envs(launch_plan.env.iter().cloned());
    cmd.args(&launch_plan.args);

    // A trusted session (`sandbox: false`) runs `claude` directly, unwrapped.
    // A jobset member session is only ever handed its derived secret through
    // the sandbox `secret` channel inside the sandboxed branch below, so a
    // trusted session never receives a jobset secret fd.
    let mut session_secret_fd = None;
    // HL-113 (sqlite inbox): the session's durable A2A inbox. Named to the
    // agent by the `KUBEADP_A2A_INBOX_DIR` env var (below, after the command
    // is finalized); the inbox itself lives in the shared per-pod
    // `state.sqlite` and the sandbox gets NO directory grant for it (requests
    // go through the pod-local HTTP API, authenticated by the per-run token).
    // Rooted at the shared per-pod state root (like the worker's inboxes),
    // NOT the deprecated /tmp state file location: the inbox must survive a
    // pod restart like the rest of the state.
    #[cfg(feature = "a2a")]
    let a2a_inbox_dir = crate::core::a2a::inbox::inbox_dir_for(state_root, &session.name);
    // Under `test-hooks`, the process-global `sandbox_passthrough` switch also lets
    // tests run unwrapped on platforms without sandbox support.
    let wrap_in_sandbox = session.sandbox && {
        #[cfg(feature = "test-hooks")]
        {
            !crate::sandbox::testhook::sandbox_passthrough()
        }
        #[cfg(not(feature = "test-hooks"))]
        {
            true
        }
    };
    if wrap_in_sandbox {
        let mut grants_cfg =
            session
                .grants
                .as_ref()
                .map(|g| crate::sandbox::policy::GrantsConfig {
                    serviceaccount: g.serviceaccount,
                    ro_paths: g.ro_paths.clone(),
                    rw_paths: g.rw_paths.clone(),
                    env: g.env.clone(),
                });
        // The session's own `env` entries are explicit operator choices: their
        // names pass the sandbox env filter without a separate grant.
        if !session.env.is_empty() {
            let g = grants_cfg.get_or_insert_with(Default::default);
            for k in session.env.keys() {
                if !g.env.contains(k) {
                    g.env.push(k.clone());
                }
            }
        }
        let base_policy = crate::sandbox::SandboxPolicy::for_session(
            &session.session_id,
            &session.workdir,
            grants_cfg,
        );
        // Jobset wiring (same as the job launcher): if this session is a jobset
        // member (registered in the process-wide registry), its secret rides the
        // sandbox `secret` channel and only the jobset id + the secret file PATH
        // reach the environment (never the secret value). A session with no
        // registered jobset is launched unchanged. The policy's agent id
        // (`session.session_id`) only names the private tmp; the registry key is the
        // session name.
        let session_jobset = crate::core::jobset::agent_jobset(&session.name);
        let policy = match &session_jobset {
            Some(_) => crate::core::jobset::build_jobset_policy(&base_policy),
            None => base_policy,
        };
        // The operator token file (config `api_token_file`) is hidden so a
        // sandboxed session can never read the credential that guards the host's
        // /api/v1 and A2A surface. No-op when the host has no token file.
        let policy = policy.hide_operator_token_file();
        // The jobset root key file is hidden too (agents see only their derived
        // per-jobset secret through the `secret` channel). No-op when unset.
        let policy = policy.hide_jobset_root_key_file();
        // HL-113 phase 3 (sqlite inbox): the session's A2A inbox lives in the
        // shared per-pod `state.sqlite`, which the sandbox NEVER gets (the
        // grant was removed: the database is host-owned). The sandboxed
        // receive side reaches its inbox through the pod-local HTTP API,
        // named by `KUBEADP_A2A_INBOX_DIR` and authenticated by the per-run
        // token set below.
        // (No sandbox grant: the inbox is host-side only.)
        let policy_path =
            std::env::temp_dir().join(format!("session-policy-{}.json", session.session_id));
        if let Err(e) = crate::sandbox::SandboxPolicy::write_policy_file(&policy, &policy_path) {
            // A session whose policy file cannot be written must not start
            // unsandboxed: fail the launch with a clear reason like every other
            // pre-exec failure below.
            log_event(
                &session.name,
                &format!(
                    "failed to write session sandbox policy '{}': {}",
                    policy_path.display(),
                    e
                ),
            );
            handle_spawn_error(
                &ctx,
                swb,
                crash_tracker,
                restart_count,
                format!("sandbox-policy-error: {}", e),
            );
            return true;
        }
        let launcher = crate::sandbox::find_launcher_exe();
        let mut sandboxed_cmd = Command::new(launcher);
        sandboxed_cmd.current_dir(&session.workdir);
        sandboxed_cmd.envs(&session.env);
        // Jobset env (jobset id + secret file PATH, never the secret value), and
        // the secret itself handed on fd 3 via the memfd below. The `KUBEADP_*`
        // vars match the default allow pattern so the sandbox passes them through.
        if let Some(cred) = &session_jobset {
            let private_tmp = policy
                .private_tmp_path()
                .unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
            for (k, v) in crate::core::jobset::build_jobset_env(cred, &private_tmp) {
                sandboxed_cmd.env(k, v);
            }
            session_secret_fd = Some(crate::orchestrate::pty::secret_memfd(&cred.secret));
        }

        // Pass through granted environment variables from the supervisor's process environment
        if let Some(ref g) = session.grants {
            for (k, v) in std::env::vars() {
                let allowed = g.env.iter().any(|pattern| {
                    if pattern.ends_with('*') {
                        let prefix = &pattern[..pattern.len() - 1];
                        k.starts_with(prefix)
                    } else {
                        &k == pattern
                    }
                });
                if allowed {
                    sandboxed_cmd.env(k, v);
                }
            }
        }
        // The session's own `env` wins over a same-named pod variable that a
        // grant pattern passed through above.
        sandboxed_cmd.envs(&session.env);

        sandboxed_cmd.arg("sandbox-exec");
        sandboxed_cmd.arg("--policy");
        sandboxed_cmd.arg(policy_path.to_string_lossy().to_string());
        sandboxed_cmd.arg("--");
        sandboxed_cmd.arg(&launch_plan.program);
        sandboxed_cmd.args(&launch_plan.args);
        cmd = sandboxed_cmd;
    }

    // HL-113: Apply A2A sender identity so messages from this session carry
    // `<A2A_SELF_PREFIX>/<session_name>`.
    crate::core::runner::apply_a2a_self_name(&mut cmd, &session.name);

    // HL-113 phase 1 (a2a builds): name the session's durable A2A inbox for
    // the injecting processes (the `mcp-a2a` receive side and the `a2a-hook`
    // subcommand), on the final (sandboxed or trusted) command. The var
    // matches the default `KUBEADP_*` sandbox allow pattern (and the
    // session's grants filter), so it passes through.
    #[cfg(feature = "a2a")]
    cmd.env(crate::core::a2a::inbox::INBOX_ENV, &a2a_inbox_dir);
    // Record the attempt's start timestamp BEFORE minting the per-run inbox
    // token and saving `session_state` so `start_time_epoch` in SQLite and the
    // HMAC-bound token given to the child agree on the exact second (N3).
    let start_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // HL-113 phase 3: the sandbox has NO database grant, so the receive side
    // reaches its inbox through the pod-local HTTP API. Hand it the per-run
    // token (derived host-side from the jobset root key; the sandbox never
    // sees the key). Absent a root key nothing is set and the receive side
    // falls back to the (now ungranted) direct inbox, which a sandboxed
    // agent simply finds empty.
    #[cfg(feature = "a2a")]
    if let Some(token) = crate::core::a2a::http::token_for_run(&a2a_inbox_dir, start_epoch) {
        cmd.env(crate::core::a2a::receive::INBOX_TOKEN_ENV, token);
    }
    // N4: hand the sandboxed receive side the REAL inbox API address — the
    // loopback form of the same listener this host serves — so a changed
    // listen port cannot break delivery silently.
    #[cfg(feature = "a2a")]
    if let Some(url) = crate::core::a2a::receive::inbox_url_for_listen(
        &crate::orchestrate::http_server::listen_addr_from_env(),
    ) {
        cmd.env(crate::core::a2a::receive::INBOX_URL_ENV, url);
    }

    // 5. Open PTY
    let mut pty = match Pty::open() {
        Ok(p) => p,
        Err(e) => {
            log_event(&session.name, &format!("Failed to open PTY: {}", e));
            handle_spawn_error(
                &ctx,
                swb,
                crash_tracker,
                restart_count,
                format!("pty-error: {}", e),
            );
            return true;
        }
    };

    if let Err(e) = pty.set_master_nonblocking() {
        log_event(
            &session.name,
            &format!("Failed to set PTY non-blocking: {}", e),
        );
        handle_spawn_error(
            &ctx,
            swb,
            crash_tracker,
            restart_count,
            format!("pty-error: {}", e),
        );
        return true;
    }

    // 6. Spawn child process (fix #8: spawn from session thread for PDEATHSIG)
    let start_time_instant = Instant::now();
    let start_rfc3339 = crate::orchestrate::logging::rfc3339_now();

    let mut child = match spawn_in_pty(cmd, &pty, session_secret_fd) {
        Ok(c) => c,
        Err(e) => {
            log_event(&session.name, &format!("Failed to spawn Claude: {}", e));
            handle_spawn_error(
                &ctx,
                swb,
                crash_tracker,
                restart_count,
                format!("spawn-error: {}", e),
            );
            return true;
        }
    };

    // Slave is duplicated in child, close supervisor's copy of slave
    pty.close_slave();
    // Publish the master fd so the sessions API can type messages into the
    // live child; drained on the shutdown path below (and via the drop guard
    // if an unexpected unwind occurs before `pty` is dropped).
    struct PtySlotGuard<'a>(&'a Mutex<crate::orchestrate::pty::PtyInput>);
    impl Drop for PtySlotGuard<'_> {
        fn drop(&mut self) {
            self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        }
    }
    pty_input
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .put(pty.master);
    let _pty_slot_guard = PtySlotGuard(pty_input);

    let child_pid = child.id();
    child_pid_tracker.store(child_pid, Ordering::SeqCst);
    // Record the process-identity anchor for this attempt alongside the pid:
    // the recovery paths (panic, cooldown, pty-close, hang, shutdown) use it
    // to refuse signaling a PID that an unrelated process has reused.
    child_start_tracker.store(
        crate::core::job::get_process_start_time(child_pid).unwrap_or(0),
        Ordering::SeqCst,
    );
    // The local identity anchor read once after spawn: every kill path below
    // re-verifies the PID against it before signaling, so a PID recycled by
    // an unrelated process is never signaled.
    let child_start_time = {
        let ticks = child_start_tracker.load(Ordering::SeqCst);
        (ticks > 0).then_some(ticks)
    };

    // Clone state snapshot before writing (fix #1: release lock before I/O)
    let state_snapshot = {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.pid = Some(child_pid);
            sess_st.start_time = Some(start_rfc3339);
            sess_st.start_time_epoch = Some(start_epoch);
            sess_st.status = SessionStatus::Running;
        }
        st.clone()
    };
    let _ = save_supervisor_state(state_root, &state_snapshot);

    // 7. PTY Output forwarder & hang detection loop
    let mut tail_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut line_buf: Vec<u8> = Vec::new();
    let mut last_pty_output = Instant::now();
    let mut exit_reason = None;
    let mut captured_status = None;
    let mut child_is_unkillable = false;
    let mut buf = [0u8; 1024];

    while !shutdown.load(Ordering::SeqCst) && !paused.load(Ordering::SeqCst) {
        match poll_and_read_pty(pty.master, &mut buf, 100) {
            Ok(Some(0)) => {
                // EOF on PTY master
                break;
            }
            Ok(Some(n)) => {
                last_pty_output = Instant::now();
                if session.prefix_output {
                    for &byte in &buf[..n] {
                        line_buf.push(byte);
                        if byte == b'\n' {
                            let mut formatted =
                                Vec::with_capacity(line_buf.len() + session.name.len() + 3);
                            formatted.extend_from_slice(format!("[{}] ", session.name).as_bytes());
                            formatted.extend_from_slice(&line_buf);
                            stdout_sender.send_or_drop(&session.name, formatted);
                            line_buf.clear();
                        }
                    }
                } else {
                    // Raw and immediate forwarding
                    stdout_sender.send_or_drop(&session.name, buf[..n].to_vec());
                }

                // Maintain rolling tail buffer
                tail_buf.extend_from_slice(&buf[..n]);
                if tail_buf.len() > 4096 {
                    let excess = tail_buf.len() - 4096;
                    tail_buf.drain(..excess);
                }
            }
            Ok(None) => {
                // Timeout (100ms)
                if session.prefix_output && !line_buf.is_empty() {
                    let mut formatted = Vec::with_capacity(line_buf.len() + session.name.len() + 3);
                    formatted.extend_from_slice(format!("[{}] ", session.name).as_bytes());
                    formatted.extend_from_slice(&line_buf);
                    stdout_sender.send_or_drop(&session.name, formatted);
                    line_buf.clear();
                }

                // Hang detection
                if session.hang_timeout_secs > 0.0
                    && last_pty_output.elapsed()
                        >= Duration::from_secs_f64(session.hang_timeout_secs)
                {
                    log_event(
                        &session.name,
                        &format!(
                            "Hang detected (no output for {:.1}s). Terminating process group...",
                            session.hang_timeout_secs
                        ),
                    );
                    let reaped = kill_process_group_with_grace(
                        child_pid,
                        child_start_time,
                        Duration::from_secs_f64(session.kill_grace_secs),
                        Duration::from_secs_f64(session.kill_wait_secs),
                    );
                    match reaped {
                        KillResult::Reaped(Some(status)) => {
                            captured_status =
                                Some(std::os::unix::process::ExitStatusExt::from_raw(status));
                        }
                        KillResult::Reaped(None) => {}
                        KillResult::Unkillable => {
                            child_is_unkillable = true;
                        }
                    }
                    exit_reason = None;
                    break;
                }

                // Check if child has exited
                match child.try_wait() {
                    Ok(Some(s)) => {
                        captured_status = Some(s);
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
            Err(e) => {
                log_event(&session.name, &format!("PTY forwarder error: {}", e));
                break;
            }
        }
    }

    // Flush any remaining partial line in prefix mode
    if session.prefix_output && !line_buf.is_empty() {
        let mut formatted = Vec::with_capacity(line_buf.len() + session.name.len() + 3);
        formatted.extend_from_slice(format!("[{}] ", session.name).as_bytes());
        formatted.extend_from_slice(&line_buf);
        stdout_sender.send_or_drop(&session.name, formatted);
        line_buf.clear();
    }

    // 8. Clean up process & master fd
    // Reclaim the shared PTY slot (any in-flight API message races the close
    // and drops cleanly) before the master fd is closed.
    drop(_pty_slot_guard);
    pty.close_master();

    if captured_status.is_none() {
        if let Ok(Some(s)) = child.try_wait() {
            captured_status = Some(s);
        }
    }

    let mut pty_exited_on_its_own = false;

    if captured_status.is_none()
        && !shutdown.load(Ordering::SeqCst)
        && !paused.load(Ordering::SeqCst)
    {
        // Child is still alive after PTY EOF/error.
        // Wait up to pty_eof_grace_secs before escalating to kill_process_group_with_grace.
        let pty_eof_grace = Duration::from_secs_f64(session.pty_eof_grace_secs);
        let grace_start = Instant::now();
        while grace_start.elapsed() < pty_eof_grace
            && !shutdown.load(Ordering::SeqCst)
            && !paused.load(Ordering::SeqCst)
        {
            match child.try_wait() {
                Ok(Some(s)) => {
                    captured_status = Some(s);
                    pty_exited_on_its_own = true;
                    break;
                }
                Ok(None) => thread::sleep(Duration::from_millis(50)),
                Err(_) => break,
            }
        }

        if captured_status.is_none() && !shutdown.load(Ordering::SeqCst) {
            log_event(
                &session.name,
                &format!(
                    "PTY closed and child still alive after {:.1}s grace. Killing process group (reason: pty-closed)...",
                    session.pty_eof_grace_secs
                ),
            );
            let reaped = kill_process_group_with_grace(
                child_pid,
                child_start_time,
                Duration::from_secs_f64(session.kill_grace_secs),
                Duration::from_secs_f64(session.kill_wait_secs),
            );
            match reaped {
                KillResult::Reaped(Some(status)) => {
                    captured_status = Some(std::os::unix::process::ExitStatusExt::from_raw(status));
                }
                KillResult::Reaped(None) => {}
                KillResult::Unkillable => {
                    child_is_unkillable = true;
                }
            }
            if exit_reason.is_none() {
                exit_reason = Some("pty-closed".to_string());
            }
        }
    }

    // Bounded child wait: do not block in child.wait() if child is still alive.
    // Poll try_wait, honouring shutdown and kill-grace/kill-wait deadlines (SIGTERM -> SIGKILL -> bounded wait).
    if captured_status.is_none() && !child_is_unkillable {
        let kill_grace = Duration::from_secs_f64(session.kill_grace_secs);
        let kill_wait = Duration::from_secs_f64(session.kill_wait_secs);
        let res = bounded_child_escalation(
            child_pid,
            child_start_time,
            &mut child,
            kill_grace,
            kill_wait,
            |p, st, opt| unsafe { libc::waitpid(p, st, opt) },
        );
        match res {
            KillResult::Reaped(Some(status)) => {
                captured_status = Some(std::os::unix::process::ExitStatusExt::from_raw(status));
            }
            KillResult::Reaped(None) => {}
            KillResult::Unkillable => {
                child_is_unkillable = true;
            }
        }
    }

    if child_is_unkillable {
        log_event(
            &session.name,
            &format!("child pid {} unkillable (uninterruptible)", child_pid),
        );
        exit_reason = Some("unkillable".to_string());
    } else {
        child_pid_tracker.store(0, Ordering::SeqCst);
        child_start_tracker.store(0, Ordering::SeqCst);
    }
    let uptime = start_time_instant.elapsed().as_secs_f64();

    let exit_code = captured_status.as_ref().and_then(|s| s.code());
    let exit_signal = captured_status.as_ref().and_then(|s| s.signal());

    if exit_reason.is_none() {
        if exit_code == Some(125) {
            // 125 is the sandbox launcher's "setup failed, command never ran":
            // surface the reason in `status` instead of a bare `exit: 125`.
            let first = String::from_utf8_lossy(&tail_buf)
                .lines()
                .find(|l| !l.trim().is_empty())
                .unwrap_or("")
                .trim()
                .chars()
                .take(200)
                .collect::<String>();
            exit_reason = Some(format!("sandbox-setup-failed (rc=125): {first}"));
        } else if let Some(code) = exit_code {
            exit_reason = Some(format!("exit: {}", code));
        } else if let Some(sig) = exit_signal {
            exit_reason = Some(format!("signal: {}", sig));
        } else {
            exit_reason = Some("unknown".to_string());
        }
    }

    let sanitized_tail = format_tail_utf8_safe(&tail_buf, 500);
    let exit_desc = match (exit_code, exit_signal) {
        (Some(c), _) => format!("rc={}", c),
        (None, Some(s)) => format!("signal={}", s),
        (None, None) => "rc=unknown".to_string(),
    };

    log_event(
        &session.name,
        &format!(
            "Process exited ({}, uptime={:.1}s); last output: {}",
            exit_desc, uptime, sanitized_tail
        ),
    );

    // Self-healing: if launch with --session-id exited within 15s and output contains "already in use",
    // switch to --resume for subsequent launches in memory.
    let tail_str = String::from_utf8_lossy(&tail_buf);
    // The claude-only resume/transcript machinery: an agy session never
    // resumes a claude transcript (its conversation lives in agy's own store,
    // keyed by `--conversation`), so the `already in use` switch and the
    // fast-resume quarantine do not apply.
    if engine != "agy" {
        if !is_resume && uptime <= 15.0 && !*force_resume && tail_str.contains("already in use") {
            log_event(
                &session.name,
                "Detected 'already in use' with --session-id; switching to --resume for subsequent launches",
            );
            *force_resume = true;
        }

        let fingerprint_after = get_transcript_fingerprint(&session_file);
        if fingerprint_after.is_none() {
            resume_fail_tracker.reset();
        } else if let (Some(last_fp), Some(curr_fp)) = (
            resume_fail_tracker.last_fingerprint(),
            fingerprint_after.as_ref(),
        ) {
            if last_fp != curr_fp {
                resume_fail_tracker.reset();
            }
        }

        if is_resume && exit_code != Some(0) && uptime <= session.resume_fail_uptime_secs {
            if output_contains_resume_not_found(&tail_str) {
                if fingerprint_before.is_some() && fingerprint_after == fingerprint_before {
                    let count = resume_fail_tracker.record_failure(fingerprint_after);
                    if session.resume_fail_quarantine_after > 0
                        && count >= session.resume_fail_quarantine_after
                    {
                        match quarantine_corrupt_session_file(&session_file) {
                            Ok(backup) => {
                                log_event(
                                    &session.name,
                                    &format!(
                                        "Session transcript failed to resume {} consecutive times within {:.1}s with missing session error ('{}') and identical size/mtime. Quarantined {} -> {} and starting fresh with --session-id.",
                                        count,
                                        session.resume_fail_uptime_secs,
                                        RESUME_NOT_FOUND_ERROR,
                                        session_file.display(),
                                        backup.display()
                                    ),
                                );
                                *force_resume = false;
                                resume_fail_tracker.reset();
                            }
                            Err(e) => {
                                log_event(
                                    &session.name,
                                    &format!(
                                        "Failed to quarantine stuck session transcript: {}",
                                        e
                                    ),
                                );
                            }
                        }
                    } else {
                        log_event(
                            &session.name,
                            &format!(
                                "Resume failed early with missing session error (consecutive fast resume failures: {}/{}); {} not quarantined (no decision yet), retrying next iteration.",
                                count,
                                session.resume_fail_quarantine_after,
                                session_file.display()
                            ),
                        );
                    }
                } else {
                    resume_fail_tracker.reset();
                }
            }
            // Failures whose output does not contain the resume error neither count nor reset an existing streak.
        } else if !is_resume || exit_code == Some(0) {
            resume_fail_tracker.reset();
        }
    }

    // Fix #1: clone state, release lock, THEN write.
    let state_snapshot = {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.pid = if child_is_unkillable {
                Some(child_pid)
            } else {
                None
            };
            sess_st.uptime_secs = Some(uptime);
            sess_st.last_exit_reason = exit_reason;
            if child_is_unkillable {
                sess_st.status = SessionStatus::Failed;
            }
        }
        st.clone()
    };
    let _ = save_supervisor_state(state_root, &state_snapshot);

    if shutdown.load(Ordering::SeqCst) {
        let state_snapshot = {
            let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(sess_st) = st.sessions.get_mut(&session.name) {
                sess_st.status = SessionStatus::Stopped;
            }
            st.clone()
        };
        let _ = save_supervisor_state(state_root, &state_snapshot);
        return false;
    }

    // Controlled stop (STOP control from the sessions API): the child was
    // deliberately killed, not crashed. Exit without crash accounting so a
    // STOP never trips max_crashes or flips the session to Failed.
    if paused.load(Ordering::SeqCst) {
        let state_snapshot = {
            let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(sess_st) = st.sessions.get_mut(&session.name) {
                sess_st.pid = None;
                sess_st.status = SessionStatus::Stopped;
            }
            st.clone()
        };
        let _ = save_supervisor_state(state_root, &state_snapshot);
        return false;
    }

    if child_is_unkillable {
        *restart_count += 1;
        {
            let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(sess_st) = st.sessions.get_mut(&session.name) {
                sess_st.restart_count = *restart_count;
            }
        }
        handle_failed_cooldown(
            session,
            state_root,
            shared_state,
            shutdown,
            paused,
            Some(child_pid_tracker),
            Some(child_start_tracker),
            crash_tracker,
            restart_count,
            Some(child_pid),
        );
        return !shutdown.load(Ordering::SeqCst);
    }

    // 9. Backoff & crash window accounting
    *restart_count += 1;
    {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.restart_count = *restart_count;
        }
    }

    let pty_healthy = pty_exited_on_its_own && exit_code == Some(0);
    let healthy = pty_healthy || backoff_mgr.on_run_finished(uptime);
    if healthy {
        crash_tracker.clear();
        resume_fail_tracker.reset();
        if pty_healthy {
            backoff_mgr.reset();
        }
    } else {
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let failed = crash_tracker.record_crash(now_epoch);

        if failed {
            // Fix #1: clone before write.
            let state_snapshot = {
                let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(sess_st) = st.sessions.get_mut(&session.name) {
                    sess_st.status = SessionStatus::Failed;
                }
                st.clone()
            };
            let _ = save_supervisor_state(state_root, &state_snapshot);

            // Fix #3: cooldown + resume instead of sleeping forever.
            handle_failed_cooldown(
                session,
                state_root,
                shared_state,
                shutdown,
                paused,
                Some(child_pid_tracker),
                Some(child_start_tracker),
                crash_tracker,
                restart_count,
                None,
            );
            // After cooldown, resume supervising.
            return !shutdown.load(Ordering::SeqCst);
        }
    }

    // Fix #4: compute backoff from sliding window.
    let now_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let backoff_duration = swb.record_restart_and_delay(now_epoch);

    // Fix #1: clone before write.
    let state_snapshot = {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.status = SessionStatus::Backoff;
        }
        st.clone()
    };
    let _ = save_supervisor_state(state_root, &state_snapshot);

    log_event(
        &session.name,
        &format!(
            "Backing off for {:.2}s before retry...",
            backoff_duration.as_secs_f64()
        ),
    );

    let sleep_start = Instant::now();
    while sleep_start.elapsed() < backoff_duration {
        if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    true
}

fn handle_spawn_error(
    ctx: &SessionIterationContext,
    swb: &mut SlideWindowBackoff,
    crash_tracker: &mut CrashWindowTracker,
    restart_count: &mut u32,
    reason: String,
) {
    let session = ctx.session;
    let state_root = ctx.state_root;
    let shared_state = ctx.shared_state;
    let shutdown = ctx.shutdown;
    let paused = ctx.paused;
    *restart_count += 1;
    let now_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);

    let failed = crash_tracker.record_crash(now_epoch);

    // Fix #1: clone before write.
    let state_snapshot = {
        let mut st = shared_state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sess_st) = st.sessions.get_mut(&session.name) {
            sess_st.restart_count = *restart_count;
            sess_st.last_exit_reason = Some(reason);
            if failed {
                sess_st.status = SessionStatus::Failed;
            } else {
                sess_st.status = SessionStatus::Backoff;
            }
        }
        st.clone()
    };
    let _ = save_supervisor_state(state_root, &state_snapshot);

    if failed {
        // Fix #3: cooldown + resume.
        handle_failed_cooldown(
            session,
            state_root,
            shared_state,
            shutdown,
            paused,
            None,
            None,
            crash_tracker,
            restart_count,
            None,
        );
        return;
    }

    // Fix #4: sliding window backoff.
    let backoff_duration = swb.record_restart_and_delay(now_epoch);
    let sleep_start = Instant::now();
    while sleep_start.elapsed() < backoff_duration {
        if shutdown.load(Ordering::SeqCst) || paused.load(Ordering::SeqCst) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_tail_utf8_safe_ascii() {
        let data = b"Hello, world!\nLine 2";
        let tail = format_tail_utf8_safe(data, 10);
        assert_eq!(tail, "ld!\\nLine 2");
    }

    #[test]
    fn test_format_tail_utf8_safe_multibyte_straddle() {
        // '─' is 3 bytes: 0xE2, 0x94, 0x80
        // '🤖' is 4 bytes: 0xF0, 0x9F, 0xA4, 0x96
        let mut data = Vec::new();
        data.extend_from_slice("prefix: ".as_bytes());
        for _ in 0..100 {
            data.extend_from_slice("───────".as_bytes());
            data.extend_from_slice("🤖".as_bytes());
        }
        // Test slicing at various offsets that land in the middle of multi-byte characters
        for max_bytes in [1, 2, 3, 4, 5, 10, 25, 50, 100, 500] {
            let tail = format_tail_utf8_safe(&data, max_bytes);
            // Must not panic and must produce valid string
            assert!(!tail.is_empty());
        }
    }

    #[test]
    fn test_format_tail_utf8_safe_empty() {
        let tail = format_tail_utf8_safe(b"", 500);
        assert_eq!(tail, "");
    }

    #[test]
    fn test_format_tail_utf8_safe_shorter_than_max() {
        let data = "Short string 🚀".as_bytes();
        let tail = format_tail_utf8_safe(data, 500);
        assert_eq!(tail, "Short string 🚀");
    }

    #[test]
    fn test_bounded_child_escalation_unkillable() {
        // Spawns a real child; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let mut child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn child");
        let child_pid = child.id();

        let start = Instant::now();
        let grace = Duration::from_millis(40);
        let kill_wait = Duration::from_millis(40);

        // Stub waitpid: always returns 0 (child appears unwaitable / stuck in D-state)
        let res = bounded_child_escalation(
            child_pid,
            crate::core::job::get_process_start_time(child_pid),
            &mut child,
            grace,
            kill_wait,
            |_p, _status, _opt| 0,
        );
        let elapsed = start.elapsed();

        // Clean up child process cleanly
        let _ = child.kill();
        let _ = child.wait();

        assert_eq!(res, KillResult::Unkillable);
        assert!(
            elapsed >= Duration::from_millis(70),
            "elapsed {:?} should be at least grace + kill_wait",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(300),
            "elapsed {:?} exceeded bounded deadline",
            elapsed
        );
    }

    #[test]
    fn test_bounded_child_escalation_reaped_on_term() {
        // Spawns a real child; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        use std::os::unix::process::CommandExt;
        let mut cmd = Command::new("sleep");
        cmd.arg("60");
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn child");
        let child_pid = child.id();

        let grace = Duration::from_millis(500);
        let kill_wait = Duration::from_millis(500);

        let res = bounded_child_escalation(
            child_pid,
            crate::core::job::get_process_start_time(child_pid),
            &mut child,
            grace,
            kill_wait,
            |p, st, opt| unsafe { libc::waitpid(p, st, opt) },
        );

        assert!(res.is_reaped());
    }

    fn make_test_session(name: &str) -> SessionConfig {
        SessionConfig {
            name: name.to_string(),
            workdir: PathBuf::from("/tmp"),
            session_id: "id-test".to_string(),
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: std::path::PathBuf::from("agy"),
            env: std::collections::HashMap::new(),
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
        }
    }

    #[test]
    fn test_handle_failed_cooldown_with_stuck_pid() {
        let mut session = make_test_session("test-session");
        session.failed_cooldown_secs = 0.02;

        let state_root = std::env::temp_dir().join(format!(
            "test_state_root_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&state_root);
        std::fs::create_dir_all(&state_root).unwrap();
        let shared_state = Arc::new(Mutex::new(SupervisorState::new(std::process::id())));
        {
            let mut st = shared_state.lock().unwrap();
            let mut sess = SessionState::new("test-session".to_string());
            sess.status = SessionStatus::Failed;
            sess.pid = Some(12345);
            st.sessions.insert("test-session".to_string(), sess);
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let child_pid_tracker = Arc::new(AtomicU32::new(12345));
        let child_start_tracker = Arc::new(AtomicU64::new(0));
        let mut crash_tracker = CrashWindowTracker::new(3, 60.0);
        crash_tracker.record_crash(1.0);
        let mut restart_count = 5;

        let mut check_count = 0;
        handle_failed_cooldown_custom(
            &session,
            &state_root,
            &shared_state,
            &shutdown,
            &shutdown,
            Some(&child_pid_tracker),
            Some(&child_start_tracker),
            &mut crash_tracker,
            &mut restart_count,
            Some(12345),
            |_pid| {
                check_count += 1;
                // Alive on first check, gone on second
                check_count <= 1
            },
        );

        assert_eq!(restart_count, 0);
        assert_eq!(child_pid_tracker.load(Ordering::SeqCst), 0);
        let st = shared_state.lock().unwrap();
        let sess = st.sessions.get("test-session").unwrap();
        assert_eq!(sess.status, SessionStatus::Starting);
        assert_eq!(sess.pid, None);
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn test_session_a2a_self_name_identity() {
        let _g = crate::test_env_lock();
        let saved_prefix = std::env::var("A2A_SELF_PREFIX").ok();
        let saved_name = std::env::var("A2A_SELF_NAME").ok();

        std::env::set_var("A2A_SELF_PREFIX", "kubeadp-agent-orchestrator-service");
        std::env::remove_var("A2A_SELF_NAME");

        let mut cmd = Command::new("claude");
        crate::core::runner::apply_a2a_self_name(&mut cmd, "homelab");

        let self_name = cmd
            .get_envs()
            .find(|(k, _)| *k == std::ffi::OsStr::new("A2A_SELF_NAME"))
            .and_then(|(_, v)| v.map(|v| v.to_string_lossy().into_owned()));
        assert_eq!(
            self_name.as_deref(),
            Some("kubeadp-agent-orchestrator-service/homelab")
        );

        // Restore env
        if let Some(p) = saved_prefix {
            std::env::set_var("A2A_SELF_PREFIX", p);
        } else {
            std::env::remove_var("A2A_SELF_PREFIX");
        }
        if let Some(n) = saved_name {
            std::env::set_var("A2A_SELF_NAME", n);
        } else {
            std::env::remove_var("A2A_SELF_NAME");
        }
    }
}

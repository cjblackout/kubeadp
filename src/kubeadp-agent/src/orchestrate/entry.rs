//! Entry point for the agent-orchestrate role (the former
//! `agent-orchestrate` crate main). Kept in the library so both the
//! `kubeadp-agent` role dispatcher and the thin `agent-orchestrate`
//! binary wrapper run identical code with identical behaviour.

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::orchestrate::config::SupervisorConfig;
use crate::orchestrate::logging::log_event;
use crate::orchestrate::pty::signal_process_group;
use crate::orchestrate::session::run_session_loop;
use crate::orchestrate::session_control::{take_restart, SessionControl, SessionControlRegistry};
use crate::orchestrate::state::{
    check_status, import_legacy_state, load_supervisor_state, save_supervisor_state, SessionState,
    SessionStatus, StatusCheckError, SupervisorState,
};
use crate::orchestrate::stdout_writer::StdoutWriter;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn signal_handler(sig: libc::c_int) {
    if sig == libc::SIGTERM || sig == libc::SIGINT || sig == libc::SIGHUP {
        SHUTDOWN.store(true, Ordering::SeqCst);
    }
}

fn register_signals() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = signal_handler as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);

        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGHUP, &sa, std::ptr::null_mut());
    }
}

fn print_help() {
    eprintln!(
        "Usage:
  agent-orchestrate [run] [--config <path>]
  agent-orchestrate print-config [--config <path>]
  agent-orchestrate status [--config <path>] [--state-file <path>] [--stale-threshold-secs <secs>]
  agent-orchestrate --help
"
    );
}

pub fn run() {
    let args: Vec<String> = env::args().collect();
    let mut cmd = "run";
    let mut config_path: Option<PathBuf> = None;
    let mut state_file_override: Option<PathBuf> = None;
    let mut stale_thresh_override: Option<f64> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "run" => {
                cmd = "run";
                i += 1;
            }
            "print-config" => {
                cmd = "print-config";
                i += 1;
            }
            "status" => {
                cmd = "status";
                i += 1;
            }
            "--config" | "-c" => {
                if i + 1 < args.len() {
                    config_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else {
                    eprintln!("Error: --config requires a path argument");
                    std::process::exit(1);
                }
            }
            "--state-file" | "-s" => {
                if i + 1 < args.len() {
                    state_file_override = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else {
                    eprintln!("Error: --state-file requires a path argument");
                    std::process::exit(1);
                }
            }
            "--stale-threshold-secs" => {
                if i + 1 < args.len() {
                    if let Ok(v) = args[i + 1].parse::<f64>() {
                        stale_thresh_override = Some(v);
                        i += 2;
                    } else {
                        eprintln!("Error: --stale-threshold-secs requires a number");
                        std::process::exit(1);
                    }
                } else {
                    eprintln!("Error: --stale-threshold-secs requires a number argument");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                print_help();
                std::process::exit(1);
            }
        }
    }

    match cmd {
        "print-config" => print_supervisor_config(config_path),
        "run" => run_supervisor(config_path),
        "status" => {
            check_supervisor_status(config_path, state_file_override, stale_thresh_override)
        }
        _ => {
            print_help();
            std::process::exit(1);
        }
    }
}

fn check_supervisor_status(
    config_path: Option<PathBuf>,
    _state_file_override: Option<PathBuf>,
    stale_thresh_override: Option<f64>,
) {
    let cfg = load_base_config(config_path);
    let stale_threshold = stale_thresh_override.unwrap_or(cfg.stale_heartbeat_secs);

    // The supervisor state lives in the shared per-pod state database
    // (`<state_root>/state.sqlite`, next to the jobs table); the deprecated
    // `--state-file` flag and the `state_file` config key are accepted but
    // ignored. The state root comes from the shared core config resolution
    // (AGENT_RUN_STATE_ROOT / config.toml / default).
    let state_root = match crate::core::config::Config::load() {
        Ok(c) => c.state_root,
        Err(e) => {
            eprintln!("Failed to resolve the supervisor state store: {:#}", e);
            std::process::exit(3);
        }
    };
    let state = match load_supervisor_state(&state_root) {
        Ok(Some(s)) => s,
        Ok(None) => {
            eprintln!(
                "Failed to read supervisor state from {}: no supervisor state in the state database (is the supervisor running?)",
                crate::core::job::state_db_path(&state_root).display()
            );
            std::process::exit(3);
        }
        Err(e) => {
            eprintln!("Failed to read supervisor state: {}", e);
            std::process::exit(3);
        }
    };

    // Print state JSON to stdout
    if let Ok(json_str) = serde_json::to_string_pretty(&state) {
        println!("{}", json_str);
    }

    match check_status(&state, stale_threshold) {
        Ok(()) => {
            std::process::exit(0);
        }
        Err(StatusCheckError::StaleHeartbeat {
            age_secs,
            threshold_secs,
        }) => {
            eprintln!(
                "Probe check failed: heartbeat is stale (age: {:.1}s > threshold: {:.1}s)",
                age_secs, threshold_secs
            );
            std::process::exit(1);
        }
        Err(StatusCheckError::SessionFailed { failed_sessions }) => {
            eprintln!(
                "Probe check failed: session(s) in failed state: {:?}",
                failed_sessions
            );
            std::process::exit(2);
        }
        Err(StatusCheckError::Stopped { reason }) => {
            eprintln!("Probe check failed: {}", reason);
            std::process::exit(4);
        }
    }
}

fn load_base_config(config_path: Option<PathBuf>) -> SupervisorConfig {
    let env_config = std::env::var("AGENT_ORCHESTRATE_CONFIG")
        .ok()
        .or_else(|| std::env::var("CLAUDE_SUPERVISOR_CONFIG").ok());
    let etc_path = Path::new("/etc/agent-orchestrate/config.json");
    let mut cfg = match crate::orchestrate::config::resolve_base_config_from_sources(
        config_path.as_deref(),
        env_config.as_deref(),
        etc_path,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            log_event("supervisor", &format!("Configuration error: {}", e));
            std::process::exit(1);
        }
    };
    if let Err(e) = cfg.validate_durations() {
        eprintln!("{}", e);
        log_event("supervisor", &e);
        std::process::exit(1);
    }
    // The agy binary for cross-engine sessions (HL-91 E5): env wins over the
    // config file's `agy_path`, mirroring `AGENT_RUN_AGY_PATH` on the worker.
    if cfg.agy_path.is_none() {
        if let Ok(p) = std::env::var("AGENT_ORCHESTRATE_AGY_PATH") {
            cfg.agy_path = Some(PathBuf::from(p));
        }
    }
    for s in &mut cfg.sessions {
        if let Err(e) = s.validate_durations() {
            eprintln!("{}", e);
            log_event("supervisor", &e);
            std::process::exit(1);
        }
        // A forbidden grant is dropped, not fatal: the session still starts.
        let name = s.name.clone();
        if let Some(g) = s.grants.as_mut() {
            for w in g.sanitize(&name) {
                eprintln!("{}", w);
                log_event(&name, &w);
            }
        }
    }
    cfg
}

fn print_supervisor_config(config_path: Option<PathBuf>) {
    let base = load_base_config(config_path);
    let resolved = match crate::orchestrate::workspaces::resolve_supervisor_config(base) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };
    match serde_json::to_string_pretty(&resolved) {
        Ok(json_str) => {
            println!("{}", json_str);
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("Failed to serialize config to JSON: {}", e);
            std::process::exit(1);
        }
    }
}

/// Spawns a single session thread, returning the join handle (or `None` on
/// spawn failure). `control` provides the paused flag the session loop
/// watches; its `running`/`restart` flags are set on respawn paths.
#[allow(clippy::too_many_arguments)]
fn spawn_session_thread(
    name: &str,
    session: crate::orchestrate::config::SessionConfig,
    config: &SupervisorConfig,
    state_root: &Path,
    state: &Arc<Mutex<SupervisorState>>,
    shutdown_flag: &Arc<AtomicBool>,
    control: &SessionControl,
    stdout_writer: &StdoutWriter,
) -> Option<thread::JoinHandle<()>> {
    let cfg_clone = config.clone();
    let state_root_clone = state_root.to_path_buf();
    let state_clone = Arc::clone(state);
    let shut_clone = Arc::clone(shutdown_flag);
    let paused_flag = control.paused.clone();
    let pty_input = control.pty_input.clone();
    control.running.store(true, Ordering::SeqCst);
    let stdout_sender = stdout_writer.sender();

    let handle = thread::Builder::new()
        .name(format!("session-{name}"))
        .spawn(move || {
            run_session_loop(
                session,
                cfg_clone,
                state_root_clone,
                state_clone,
                shut_clone,
                paused_flag,
                pty_input,
                stdout_sender,
            );
        });

    match handle {
        Ok(h) => Some(h),
        Err(e) => {
            control.running.store(false, Ordering::SeqCst);
            log_event(
                "supervisor",
                &format!("Failed to spawn thread for session {name}: {e}"),
            );
            None
        }
    }
}

fn run_supervisor(config_path: Option<PathBuf>) {
    register_signals();

    let base_config = load_base_config(config_path);
    let config = match crate::orchestrate::workspaces::resolve_supervisor_config(base_config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            log_event("supervisor", &format!("Configuration error: {}", e));
            std::process::exit(1);
        }
    };

    log_event(
        "supervisor",
        &format!(
            "Starting Claude Supervisor (pid={}, sessions={})...",
            std::process::id(),
            config.sessions.len()
        ),
    );

    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // The shared per-pod state root: the state database that holds the jobs
    // table AND the supervisor/session state next to it (one db per pod).
    // The supervisor is the only writer of the supervisor/session rows.
    // Loaded once; a store that cannot be opened (corrupt database, bad core
    // config) is a clear fatal error instead of silent state loss.
    let core_config = match crate::core::config::Config::load() {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Failed to open the state store: {e:#}");
            eprintln!("{msg}");
            log_event("supervisor", &msg);
            std::process::exit(1);
        }
    };
    let state_root = core_config.state_root.clone();

    // Initialize session states. Each session records the engine it resolved
    // (per-session `engine` > `default_session_engine` > "claude", HL-91 E5)
    // so the sessions API and a restart keep the engine stable. The default
    // may come from the supervisor config OR the worker-side core config file
    // (the same resolution `run_session_iteration` applies at spawn time).
    let default_session_engine = config
        .default_session_engine
        .clone()
        .or_else(|| core_config.default_session_engine.clone());

    // One-time import of the legacy JSON state file (the deprecated
    // `state_file` config key): imported once into the state db, then
    // renamed to `*.migrated`.
    match import_legacy_state(&config.state_file, &state_root) {
        Ok(Some(_)) => {
            log_event(
                "supervisor",
                "Imported the legacy JSON state file into the state database",
            );
        }
        Ok(None) => {}
        Err(e) => {
            log_event("supervisor", &format!("Legacy state import skipped: {}", e));
        }
    }

    let state = {
        // Base state: restore the previous supervisor/session state (the
        // state survives a pod restart -- the real gain over the wiped /tmp
        // JSON file) or start fresh.
        let mut restored = match load_supervisor_state(&state_root) {
            Ok(Some(prev)) => {
                log_event(
                    "supervisor",
                    "Restored the previous session state from the state database",
                );
                prev
            }
            Ok(None) => SupervisorState::new(std::process::id()),
            Err(e) => {
                let msg = format!("Failed to read the supervisor state: {}", e);
                eprintln!("{msg}");
                log_event("supervisor", &msg);
                std::process::exit(1);
            }
        };
        // Refresh the supervisor identity and heartbeat for THIS run; each
        // configured session keeps its crash accounting (restart counts, last
        // exit reason) but gets a fresh attempt (no child process in this pod
        // yet, status Starting until its thread moves it forward).
        restored.supervisor_pid = std::process::id();
        restored.update_heartbeat();
        restored.status = "running".to_string();
        restored
            .sessions
            .retain(|name, _| config.sessions.iter().any(|s| &s.name == name));
        for sess in &config.sessions {
            let session_state = restored
                .sessions
                .entry(sess.name.clone())
                .or_insert_with(|| SessionState::new(sess.name.clone()));
            session_state.pid = None;
            session_state.start_time = None;
            session_state.start_time_epoch = None;
            session_state.uptime_secs = None;
            session_state.last_health_result = None;
            session_state.status = SessionStatus::Starting;
            session_state.engine = Some(sess.effective_engine(default_session_engine.as_deref()));
        }
        Arc::new(Mutex::new(restored))
    };

    // Write initial state (into the state database)
    let st_snapshot = state.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if let Err(e) = save_supervisor_state(&state_root, &st_snapshot) {
        log_event(
            "supervisor",
            &format!("Failed to write initial supervisor state: {}", e),
        );
    }

    // Initialize dedicated non-blocking stdout writer thread (keeps a shared
    // per-session output ring buffer for the sessions REST API).
    let session_logs =
        std::sync::Arc::new(crate::orchestrate::session_log::SessionLogs::from_env());
    let stdout_writer = crate::orchestrate::stdout_writer::StdoutWriter::with_session_logs(Some(
        session_logs.clone(),
    ));

    // Registry of live per-session control handles, shared with the sessions
    // REST API (pause / resume / restart / tail).
    let controls = SessionControlRegistry::new();
    for session in &config.sessions {
        controls.insert(Arc::new(SessionControl::new(session.clone())));
    }

    // Jobsets: the root key and the membership store must be ready BEFORE the
    // first session registers itself. Otherwise the session gets a random
    // secret that the shared root key cannot derive, and no worker job of its
    // jobset can ever present a credential this process accepts.
    crate::core::jobset::register_self();
    if let Err(e) = crate::core::jobset::init_global_jobsets(&state_root) {
        log_event("supervisor", &format!("jobset init failed: {e}"));
    }

    // Live session threads, keyed by name.
    let mut handles: HashMap<String, thread::JoinHandle<()>> = HashMap::new();

    // Spawn the initial thread for each session.
    for session in &config.sessions {
        let handle = spawn_session_thread(
            &session.name,
            session.clone(),
            &config,
            &state_root,
            &state,
            &shutdown_flag,
            &controls.get(&session.name).expect("control just seeded"),
            &stdout_writer,
        );
        if let Some(h) = handle {
            handles.insert(session.name.clone(), h);
        }
    }

    // Start the HTTP API: the copied agent-run router plus the sessions group,
    // on its own tokio runtime in a background thread.
    let http_shutdown = Arc::new(AtomicBool::new(false));
    let _http_thread = match crate::orchestrate::http_server::spawn_http_server(
        state.clone(),
        controls.clone(),
        session_logs,
        http_shutdown.clone(),
    ) {
        Ok(t) => Some(t),
        Err(e) => {
            log_event("supervisor", &format!("Failed to start HTTP server: {e:#}"));
            None
        }
    };

    // Main coordinator heartbeat loop
    let heartbeat_interval = Duration::from_secs_f64(config.heartbeat_interval_secs);
    let mut last_heartbeat = Instant::now();

    while !SHUTDOWN.load(Ordering::SeqCst) {
        // 1. Session lifecycle: detect finished threads (a session thread only
        //    exits on shutdown or a STOP, both recorded as Stopped by the
        //    session loop; anything else under the current handle that ended is
        //    an unexpected exit -> Failed), then respawn any session that asks
        //    for a restart (START-after-stop / RESTART from the sessions API).
        for control in controls.all() {
            let name = control.config.name.clone();

            // Thread completion: the handle is the source of truth. The session
            // loop never exits on a crash (it self-heals with backoff), so a
            // finished thread means STOP or shutdown.
            let finished = handles.get(&name).is_some_and(|h| h.is_finished());
            if finished {
                let st_snapshot = {
                    let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(sess_st) = st.sessions.get_mut(&name) {
                        if sess_st.status != SessionStatus::Stopped
                            && sess_st.status != SessionStatus::Failed
                        {
                            log_event(
                                "supervisor",
                                &format!(
                                    "Session thread for '{name}' exited unexpectedly. Marking session failed."
                                ),
                            );
                            sess_st.status = SessionStatus::Failed;
                            sess_st.pid = None;
                            sess_st.last_exit_reason = Some("thread-exited".to_string());
                        }
                        Some(st.clone())
                    } else {
                        None
                    }
                };
                if let Some(st) = st_snapshot {
                    let _ = save_supervisor_state(&state_root, &st);
                }
                handles.remove(&name);
                control.running.store(false, Ordering::SeqCst);
            }

            // Explicit restart request (START after a stop, or RESTART).
            if take_restart(&control) && !handles.contains_key(&name) {
                log_event(&name, "Restart requested; spawning fresh session thread.");
                // Fresh control: the paused flag is an Arc that cannot be
                // swapped in place, so the fresh thread gets its own.
                let fresh = SessionControl::new(control.config.clone());
                if let Some(h) = spawn_session_thread(
                    &name,
                    control.config.clone(),
                    &config,
                    &state_root,
                    &state,
                    &shutdown_flag,
                    &fresh,
                    &stdout_writer,
                ) {
                    controls.insert(Arc::new(fresh));
                    handles.insert(name.clone(), h);
                } else {
                    log_event(&name, "Failed to spawn fresh session thread (spawn error).");
                }
            }
        }

        // 2. Periodic heartbeat write + stale-handle sweep.
        if last_heartbeat.elapsed() >= heartbeat_interval {
            let st_snapshot = {
                let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
                st.update_heartbeat();
                st.clone()
            };
            if let Err(e) = save_supervisor_state(&state_root, &st_snapshot) {
                log_event("supervisor", &format!("Failed to write state: {}", e));
            }
            last_heartbeat = Instant::now();
        }

        // Small sleep interval for fast shutdown response
        thread::sleep(Duration::from_millis(100));
    }

    // Shutdown initiated
    log_event(
        "supervisor",
        "Shutdown signal received (SIGTERM/SIGINT). Draining sessions...",
    );
    shutdown_flag.store(true, Ordering::SeqCst);

    // Ask the HTTP server (own tokio runtime) to drain and stop accepting.
    http_shutdown.store(true, Ordering::SeqCst);
    if let Some(t) = _http_thread {
        let _ = t.join();
    }

    // Collect all active child PIDs from state
    let active_pids: Vec<u32> = {
        let st = state.lock().unwrap_or_else(|e| e.into_inner());
        st.sessions.values().filter_map(|s| s.pid).collect()
    };

    // Send SIGTERM to all child process groups
    for &pid in &active_pids {
        let _ = signal_process_group(pid, libc::SIGTERM);
    }

    // Wait up to shutdown_grace_secs for threads to finish (bounded, never blocking indefinitely on join)
    let grace_start = Instant::now();
    let shutdown_grace = Duration::from_secs_f64(config.shutdown_grace_secs);

    let mut remaining_handles: Vec<thread::JoinHandle<()>> = handles.into_values().collect();
    while grace_start.elapsed() < shutdown_grace {
        remaining_handles.retain(|h| !h.is_finished());
        if remaining_handles.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Escalate with SIGKILL only to child processes still tracked as live in
    // session state after the grace window (processes reaped during
    // shutdown_grace cleared their `pid` in state and must not be signaled
    // again, avoiding PID reuse hazards).
    let tracked_pids: Vec<u32> = {
        let st = state.lock().unwrap_or_else(|e| e.into_inner());
        st.sessions.values().filter_map(|s| s.pid).collect()
    };

    for &pid in &tracked_pids {
        let _ = signal_process_group(pid, libc::SIGKILL);
    }

    // Fix #7: Reap only tracked child processes (never waitpid(-1) because session
    // threads may still be running and own their own children).
    let reap_deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < reap_deadline {
        let mut any_pending = false;
        for &pid in &tracked_pids {
            let mut status: libc::c_int = 0;
            let res = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
            if res == 0 {
                any_pending = true;
            }
        }
        if !any_pending {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    // Update state to stopped
    let st_snapshot = {
        let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
        st.status = "stopped".to_string();
        for sess in st.sessions.values_mut() {
            if sess.status != SessionStatus::Failed {
                sess.status = SessionStatus::Stopped;
            }
            sess.pid = None;
        }
        st.update_heartbeat();
        st.clone()
    };
    let _ = save_supervisor_state(&state_root, &st_snapshot);

    log_event("supervisor", "Shutdown complete. Clean exit.");
    std::process::exit(0);
}

//! Entry point for the agent-run API worker role (the former
//! `agent-run-api` crate main). Kept in the library so both the
//! `kubeadp-agent` role dispatcher and the thin `agent-run-api` binary
//! wrapper run identical code with identical behaviour.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{agy_home, server, supervisor};
use crate::core::auth::AuthManager;
use crate::core::config::Config;
use crate::core::runner::mark_child_subreaper;

const DEFAULT_LISTEN: &str = "0.0.0.0:8001";

/// The former agent-run-api `main`, preserved verbatim as an async
/// entry function.
pub async fn run() -> Result<()> {
    // defaults. Loading also ensures the state store (state.sqlite +
    // logs/) exists, so no command needs an explicit `init`.
    let config = Config::load().context("failed to load agent-run configuration")?;

    // The core's `start::run` resolves relative `@prompt` files (and
    // relative patch paths) against the caller's cwd. For remote requests
    // the "caller" is this server, so the workspace root is the right cwd.
    if let Err(e) = std::env::set_current_dir(&config.workspace_root) {
        eprintln!(
            "agent-run-api: warning: could not chdir to workspace root {}: {} (relative @prompt/patch paths will resolve against the startup cwd instead)",
            config.workspace_root.display(),
            e
        );
    }

    // agy's home (~/.gemini/config) is created here as the server's uid, so a root-owned
    // directory created by a kubelet subPath mount cannot block agy. Never fatal.
    let home = std::path::PathBuf::from(
        std::env::var("HOME").unwrap_or_else(|_| "/home/agent".to_string()),
    );
    let mcp_source = std::env::var_os("AGENT_RUN_AGY_MCP_SOURCE").map(std::path::PathBuf::from);
    if let Err(e) = agy_home::prepare_agy_home(&home, mcp_source.as_deref()) {
        eprintln!("agent-run-api: ERROR: agy home setup: {e}");
    }

    // Process-wide orphan-reparenting target for agy's own `cmd &` children
    // (the flag lives in the process signal struct, so this one call covers
    // every runner thread -- see mark_child_subreaper's docs).
    mark_child_subreaper();

    // This pod has no init (no tini): the server is PID 1, so orphaned
    // children that agy (or the black-box/fake-agy test harness) leaves
    // behind -- e.g. its background `sleep 1 &` -- would linger as
    // `[sleep] <defunct>` zombies for the pod's lifetime. Start the
    // in-server reaper (background thread, `waitpid(-1, WNOHANG)`, skips the
    // pids the job monitors `wait()` on themselves). Only when PID 1: in
    // local dev/test runs the parent reaps for us and the loop would only
    // race test-owned children. Never touches the graceful-shutdown path.
    // (Alternative considered and rejected: a tini ENTRYPOINT -- it would
    // also change signal delivery, the container image, and still could not
    // reap children this server already owns; see DOCS.md.)
    if crate::core::reaper::is_pid1() {
        crate::core::reaper::start_reaper_thread();
        eprintln!(
            "agent-run-api: running as PID 1 without init -- in-server zombie reaper enabled ({} min stall threshold for status/list, AGENT_RUN_STALL_MINUTES)",
            config.stall_minutes
        );
    }

    // Opt-in max-runtime enforcement: no limit is set anywhere by default, so
    // this never changes the unbounded behavior of ordinary runs -- when a
    // job carries no limit (per-job or default) each pass is a few cheap
    // SELECTs every 10 s and does nothing. A job with a limit -- whether that
    // came from `start --max-runtime` or the server default -- is terminated
    // the same way `kill` does once its wall time exceeds the limit, and the
    // persisted start anchor means a restart does not reset the clock.
    {
        let enforcer_config: Arc<crate::core::config::Config> = Arc::new(config.clone());
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                let cfg = std::sync::Arc::clone(&enforcer_config);
                if let Ok(killed) = tokio::task::spawn_blocking(move || {
                    crate::core::max_runtime::enforce_once(&cfg)
                })
                .await
                {
                    for id in killed {
                        eprintln!(
                            "agent-run-api: job '{id}' exceeded its max runtime (terminated, status: killed)"
                        );
                    }
                }
            }
        });
    }

    let listen: SocketAddr = std::env::var("AGENT_RUN_API_LISTEN")
        .unwrap_or_else(|_| DEFAULT_LISTEN.to_string())
        .parse()
        .with_context(|| {
            format!("AGENT_RUN_API_LISTEN must be a host:port address (default {DEFAULT_LISTEN})")
        })?;

    // Startup config summary: which settings are actually active.
    eprintln!(
        "agent-run-api: config -- workspace root: {}, state root: {}, agy: {}, max parallel jobs: {}, default model: {}, default effort: {}, default base branch: {}, gc max age: {}d, hindsight: {}{}",
        config.workspace_root.display(),
        config.state_root.display(),
        config.agy_path.display(),
        config.max_parallel,
        config.default_model.as_deref().unwrap_or("(none)"),
        config.default_effort.as_deref().unwrap_or("(none)"),
        config.default_base_branch,
        config.gc_max_age_days,
        if config.hindsight_enabled { "enabled" } else { "disabled" },
        match &config.hindsight_bank {
            Some(bank) => format!(", bank: {bank}"),
            None => String::new(),
        }
    );

    let state = Arc::new(config);
    // One global auth manager (at most one in-progress OAuth flow) for
    // the auth routes.
    let auth = Arc::new(AuthManager::new());
    let rc_state = server::RemoteControlState::default();
    // agent-run-api has no bearer token by design (its NetworkPolicy is the
    // access control): the shared auth layer passes requests through
    // untouched, so all responses are identical to the pre-token server.
    let ready = {
        let state = Arc::clone(&state);
        Arc::new(move || {
            crate::core::job::ensure_state_store(&state.state_root).map_err(|e| e.to_string())
        })
    };
    let state_metrics = {
        let state = Arc::clone(&state);
        Arc::new(move || {
            let Ok(jobs) = crate::core::job::list_jobs(&state.state_root, None) else {
                return Vec::new();
            };
            let mut counts: std::collections::BTreeMap<&'static str, u64> =
                std::collections::BTreeMap::new();
            for job in jobs {
                let status = crate::core::job::compute_status(&job, &state.state_root).as_str();
                *counts.entry(status).or_default() += 1;
            }
            counts
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
        })
    };
    let app = server::obs::add_observability(
        server::router_with_rc(state.clone(), auth, rc_state.clone()),
        server::auth::AuthState::none(),
        ready,
        state_metrics,
    );

    #[cfg(feature = "a2a")]
    let app = {
        let a2a_model = crate::core::a2a::A2aModel {
            name: "agent-run-api".to_string(),
            description: "Remote execution daemon for agent-run jobs".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            skills: Vec::new(),
            json_rpc_url: format!("http://{listen}/rpc"),
        };
        let a2a_backend = Arc::new(crate::worker::a2a::JobTaskBackend::new(state.clone()));
        // agent-run-api has no bearer token by default (cluster NetworkPolicy is access control).
        // Passing None as AuthVerifier allows unauthenticated in-cluster access.
        // No MessageSink: a running agy job has no interactive stdin channel,
        // so SendMessage answers UNSUPPORTED_OPERATION for the worker role.
        let a2a_routes = crate::core::a2a::a2a_router(a2a_model, a2a_backend, None, None);
        app.merge(a2a_routes)
    };
    eprintln!("agent-run-api: listening on {listen}");

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind {listen}"))?;

    let (start_supervisor, warning) =
        supervisor::should_start_supervisor(state.remote_control, state.remote_control_autostart);
    if let Some(msg) = warning {
        eprintln!("agent-run-api: warning: {msg}");
    }
    let supervisor_handle = if start_supervisor {
        Some(supervisor::start_supervisor(
            state.clone(),
            rc_state.suppressed.clone(),
        ))
    } else {
        None
    };

    // Graceful shutdown on SIGTERM/SIGINT: stop accepting new connections,
    // then give running agy jobs a clean SIGTERM so each one writes its final
    // structured JSON line (conversation id + status) to its job log instead
    // of a pod death leaving the row without a conversation id. The in-process
    // job monitor threads then persist the final status themselves; the drain
    // only waits for the agy processes plus a short monitor tail. Never
    // SIGKILL -- the pod's own termination grace period does that if needed.
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm =
                    signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
                tokio::select! {
                    _ = sigterm.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
        })
        .await
        .context("server error")?;

    if let Some(handle) = supervisor_handle {
        handle.abort();
    }

    let grace = std::time::Duration::from_secs(state.shutdown_grace_secs);
    let drained = crate::core::commands::shutdown::terminate_running_jobs_gracefully(&state, grace);
    if !drained.is_empty() {
        eprintln!(
            "agent-run-api: graceful shutdown: sent SIGTERM to {} running job(s): {}",
            drained.len(),
            drained.join(", ")
        );
    }
    Ok(())
}

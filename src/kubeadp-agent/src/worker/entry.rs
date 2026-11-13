//! Entry point for the agent-run API worker role (the former
//! `agent-run-api` crate main). Kept in the library so both the
//! `kubeadp-agent` role dispatcher and the thin `agent-run-api` binary
//! wrapper run identical code with identical behaviour.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{agy_home, agy_plugin, server, supervisor};
use crate::core::auth::AuthManager;
use crate::core::config::Config;
use crate::core::runner::mark_child_subreaper;

const DEFAULT_LISTEN: &str = "0.0.0.0:8002";

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

    // The A2A hook channel (HL-113 phase 2): install the NATIVE agy plugin
    // (flat PreInvocation/PostInvocation/Stop handler lists) via `agy plugin
    // install`. The content is run-independent (each hook reads the run's
    // inbox from the environment), so one install covers every run; it never
    // touches the user's other plugins. Best effort, never fatal.
    match agy_plugin::install(&config.state_root, &config.agy_path, &home) {
        Ok(()) => eprintln!(
            "agent-run-api: a2a agy plugin installed ({}), hook channel armed",
            agy_plugin::plugin_dir_for(&config.state_root).display()
        ),
        Err(e) => eprintln!("agent-run-api: warning: a2a agy plugin install: {e}"),
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

    let state_root = config.state_root.clone();
    let state = Arc::new(config);
    // One global auth manager (at most one in-progress OAuth flow) for
    // the auth routes.
    let auth = Arc::new(AuthManager::new());
    let rc_state = server::RemoteControlState::default();
    // The operator bearer token guards the WHOLE /api/v1 surface (agent-run
    // routes) plus, under `a2a`, the A2A routes. It is resolved once at
    // startup: the raw value exists only long enough to be hashed into the
    // AuthState digest, and is never logged. Without a token the server
    // refuses to start unless api_allow_unauthenticated is set.
    let auth_state = server::auth::resolve_startup_auth()
        .with_context(|| "agent-run-api: refusing to start".to_string())?;
    // Register this worker's own jobset membership (if it is a member) so the
    // A2A surface can accept a presented jobset credential against itself.
    // No-op when the worker is not a jobset member (an operator request).
    crate::core::jobset::register_self();
    // Jobset robustness startup: resolve + cache the jobset root key, point
    // the persisted membership store at the worker's state dir, and reload
    // memberships (a restart then still verifies a retried job's re-derived
    // secret). A configured-but-broken root key file is a startup error naming
    // only the path (never its content).
    crate::core::jobset::init_global_jobsets(&state_root).map_err(|e| anyhow::anyhow!("{e}"))?;
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
    let app = {
        #[cfg(feature = "engine")]
        {
            // Register the agy engine (the per-role default for jobs) and the
            // claude engine (HL-91 E5 cross-use: claude drives jobs through
            // the engine-aware runner and reports its capabilities) so
            // /api/v1/jobs and /api/v1/capabilities resolve them through the
            // trait (HL-91 E3/E5).
            let mut registry = crate::engine::AgyEngine::build_registry(Arc::clone(&state));
            // The claude job engine's capabilities and registry presence come
            // from the trait; the launcher is the engine-aware runner
            // (`meta.engine == "claude"`), not the trait's in-memory driver.
            registry.register(std::sync::Arc::new(
                crate::engine::claude::ClaudeEngine::new(state.claude_path.clone(), None),
            ));
            // The emulation layer drives job-mode control against the real job
            // rows, so the wrappers get the host config (HL-71 S2: the
            // emulated interrupt terminates the job's child process group).
            registry.attach_config(Arc::clone(&state));
            // Arm the E4 emulation for every control endpoint each inner
            // engine lacks: claude interrupt/pause/fork/compact/revert and agy
            // fork/compact/revert become emulated (never unsupported), so no
            // /api/v1/jobs control route answers 501 because of the engine.
            registry.enable_all_emulated();
            let engines = std::sync::Arc::new(registry);
            server::obs::add_observability(
                server::router_with_engines(state.clone(), auth, rc_state.clone(), engines),
                auth_state.clone(),
                ready,
                state_metrics,
            )
        }
        #[cfg(not(feature = "engine"))]
        {
            server::obs::add_observability(
                server::router_with_rc(state.clone(), auth, rc_state.clone()),
                auth_state.clone(),
                ready,
                state_metrics,
            )
        }
    };

    #[cfg(feature = "a2a")]
    let app = {
        // The agent card identifies the RUNNING POD (not the retired
        // agent-run-api image): the name is the pod's own name, the
        // description and skills name the worker role, the version is the
        // crate version, and the advertised URL is the cluster Service DNS
        // (KUBEADP_A2A_URL) when set, else the listen address.
        let advertised = crate::core::a2a::advertised_url(&format!("http://{listen}"));
        let a2a_model = crate::core::a2a::A2aModel {
            name: crate::core::a2a::pod_name(crate::core::a2a::ROLE_WORKER),
            description: crate::core::a2a::role_description(crate::core::a2a::ROLE_WORKER)
                .to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            skills: crate::core::a2a::role_skills(crate::core::a2a::ROLE_WORKER),
            url: advertised,
        };
        let a2a_backend = Arc::new(crate::worker::a2a::JobTaskBackend::new(state.clone()));
        // SendMessage resumes the addressed job's conversation with the framed
        // message as the next turn (interrupt-and-resume): the sink stops a
        // running attempt cleanly, then continues the same conversation through
        // the retry follow-up path.
        let a2a_sink = Arc::new(crate::worker::a2a::JobMessageSink::new(state.clone()));
        // Messages queued before a restart have no ladder watcher: start one
        // per run that still holds an uninjected message.
        a2a_sink.recover_pending();
        // The A2A surface sits behind the SAME operator token as /api/v1
        // (see auth_state above): the verifier hashes the presented bearer
        // credential and compares digests in constant time, exactly like the
        // shared layer. With no token the A2A routes stay open only when
        // KUBEADP_API_ALLOW_OPEN is set; otherwise the mount is refused below
        // (fail closed, never an accidentally open surface).
        // The operator token gate (same credential as /api/v1) plus, when this
        // worker is a jobset member, the per-jobset credential as an
        // independent alternative on the A2A routes (never on /api/v1).
        let a2a_auth = crate::core::server::auth::auth_verifier_from_state(&auth_state);
        // Fail closed: a `None` operator verifier (no KUBEADP_API_TOKEN_FILE)
        // means open only when KUBEADP_API_ALLOW_OPEN=true is set explicitly;
        // otherwise the server refuses to start with a clear error naming the
        // flag. Production deployments always configure the token.
        if !crate::core::server::token::a2a_open_permitted(
            a2a_auth.is_some(),
            crate::core::server::token::allow_open_from_env(),
        ) {
            anyhow::bail!(
                "agent-run-api: no operator api token configured (KUBEADP_API_TOKEN_FILE) and \
                 KUBEADP_API_ALLOW_OPEN is not set; refusing to mount open A2A routes -- \
                 supply the operator token or explicitly set KUBEADP_API_ALLOW_OPEN=true"
            );
        }
        let a2a_jobset = crate::core::a2a::JobsetCred::from_host();
        let a2a_routes = crate::core::a2a::a2a_router_with_jobset(
            a2a_model,
            a2a_backend,
            Some(a2a_sink),
            a2a_auth,
            a2a_jobset,
        );
        // The sandboxed run's inbox API: reachable pod-locally by the run's
        // own process under a per-run token, NEVER the operator bearer
        // (a sandboxed agent has no operator token). Mounted after the
        // bearer layer exactly like the A2A routes.
        let state = crate::core::a2a::http::InboxHttpState::resolve(state.state_root.clone())
            .map_err(|e| anyhow::anyhow!("failed to resolve the A2A inbox token key: {e}"))?;
        let a2a_inbox_http = crate::core::a2a::http::inbox_http_router(state);
        app.merge(a2a_routes).merge(a2a_inbox_http)
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

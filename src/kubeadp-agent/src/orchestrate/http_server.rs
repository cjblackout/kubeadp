//! HTTP server for the supervisor: merges the shared `crate::core::server`
//! router (identical `/api/v1` routes and schemas so the `agent-run` CLI's
//! `/workspace` client works unchanged) with the supervisor's own
//! `/api/v1/sessions` control group.
//!
//! Runs on its own tokio runtime in a background thread so it never blocks
//! the supervisor's (blocking, std::thread based) session loop. The listen
//! address defaults to `127.0.0.1:8002` and is overridable with
//! `AGENT_ORCHESTRATE_LISTEN`; it is intended to be reachable only from
//! inside the cluster (NetworkPolicy), never through an ingress.
//!
//! Optional bearer auth for the WHOLE `/api/v1` group (agent-run AND
//! sessions routes alike): when `AGENT_ORCHESTRATE_TOKEN_FILE` (or
//! `AGENT_ORCHESTRATE_TOKEN`) is set, every `/api/v1/*` route requires
//! `Authorization: Bearer <token>` -- enforced once by the shared
//! [`crate::core::server::obs::add_observability`] layer, so a route added to
//! either router is authenticated automatically. `/healthz` and `/readyz`
//! are never gated by the token.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;

#[cfg(not(feature = "engine"))]
use crate::orchestrate::server::router_with_rc;
use crate::orchestrate::server::RemoteControlState;
use crate::orchestrate::session_control::SessionControlRegistry;
use crate::orchestrate::session_log::SessionLogs;
use crate::orchestrate::state::SupervisorState;
use crate::server::sessions::{bearer_token_from_env, SessionsApiState};

pub const DEFAULT_LISTEN: &str = "127.0.0.1:8002";

/// Builds the merged router: agent-run routes + sessions group, all behind
/// one shared bearer-auth layer (`/readyz`, `/healthz` and `/metrics` are
/// added by [`crate::core::server::obs::add_observability`]; `/metrics` is
/// behind the same auth as `/api/v1` when a token is configured).
pub fn build_router(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    session_logs: Arc<SessionLogs>,
) -> Result<Router> {
    let core_config = crate::core::config::Config::load().unwrap_or_else(|e| {
        crate::orchestrate::logging::log_event(
            "supervisor",
            &format!("agent-run config load failed ({}); using defaults", e),
        );
        crate::core::config::Config::default()
    });
    let auth = Arc::new(crate::core::auth::AuthManager::new());
    let rc_state = RemoteControlState::default();
    let state_root = core_config.state_root.clone();
    // The escalation ladder (design HL-113 §7.14, phase 2): the session
    // inboxes live under the shared agent-run state root, and the ladder
    // knobs are the shared a2a_ack_secs / a2a_force_secs / inbox TTL keys
    // (captured before the config moves into the core router).
    #[cfg(feature = "a2a")]
    let a2a_ladder = crate::core::a2a::delivery::LadderConfig {
        ack_secs: core_config.a2a_ack_secs,
        force_secs: core_config.a2a_force_secs,
        ttl_secs: core_config.a2a_inbox_ttl_secs,
    };
    #[cfg(not(feature = "engine"))]
    let base = router_with_rc(
        Arc::new(core_config.clone()),
        auth.clone(),
        rc_state.clone(),
    );

    // Engine-aware base router (HL-91 E1/E5): the orchestrator merges the
    // same router the worker uses so `/api/v1/capabilities` and the E4
    // run-control routes resolve; the registry carries the claude session
    // engine and the agy job engine (the orchestrator's roles per the E5
    // cross-use matrix). The shared/router_with_engines is behaviourally
    // identical to router_with_rc except for these engine routes.
    #[cfg(feature = "engine")]
    let base = {
        use crate::engine::EngineRegistry;
        use crate::engine::RunKind;
        let mut registry = EngineRegistry::new();
        // The claude session engine fronts the supervisor machinery; agy is
        // the job engine (and reports its capability surface).
        registry.register(std::sync::Arc::new(
            crate::engine::claude::ClaudeEngine::new(core_config.claude_path.clone(), None),
        ));
        registry.register(std::sync::Arc::new(crate::engine::AgyEngine::new(
            std::sync::Arc::new(core_config.clone()),
        )));
        // The opencode job engine (KA-4): same trait surface as agy/claude,
        // driven by the engine-aware runner.
        registry.register(std::sync::Arc::new(
            crate::engine::opencode::OpenCodeEngine::new(std::sync::Arc::new(core_config.clone())),
        ));
        // Selection precedence level 2 (config): the configured defaults
        // override the per-role fallbacks (agy for jobs, claude for
        // sessions); a request still wins. Empty values keep the fallback.
        if let Some(default) = core_config
            .default_engine
            .clone()
            .filter(|s| !s.trim().is_empty())
        {
            registry.set_default(RunKind::Job, default);
        }
        if let Some(default) = core_config
            .default_session_engine
            .clone()
            .filter(|s| !s.trim().is_empty())
        {
            registry.set_default(RunKind::Session, default);
        }
        crate::core::server::router_with_engines(
            std::sync::Arc::new(core_config.clone()),
            auth,
            rc_state,
            std::sync::Arc::new(registry),
        )
    };

    let api_state = SessionsApiState::new(state.clone(), controls.clone(), session_logs);
    let base = crate::server::router_with_sessions(base, Some(api_state.clone()));
    // The operator bearer token guards the WHOLE /api/v1 group (agent-run AND
    // sessions routes) plus, under `a2a`, the A2A routes. It is resolved once
    // at startup: the raw value lives only long enough to be hashed into the
    // AuthState digest, and is never logged. Without a token the server
    // refuses to start unless api_allow_unauthenticated is set. The legacy
    // AGENT_ORCHESTRATE_TOKEN(_FILE) (bearer_token_from_env) is folded in as
    // a fallback so existing deployments keep working unchanged.
    let auth_state = resolve_startup_auth_fallback()?;

    // Register this orchestrator's own jobset membership (if it is a member)
    // so the A2A surface can accept a presented jobset credential against
    // itself. No-op when it is not a member (an operator request).
    crate::core::jobset::register_self();
    // Jobset robustness startup: resolve + cache the jobset root key, point
    // the persisted membership store at the shared agent-run state dir, and
    // reload memberships. A configured-but-broken root key file is a startup
    // error naming only the path (never its content).
    crate::core::jobset::init_global_jobsets(&state_root).map_err(|e| {
        crate::orchestrate::logging::log_event("supervisor", &format!("jobset init failed: {e}"));
        anyhow::anyhow!("{e}")
    })?;

    #[cfg(feature = "a2a")]
    // Audit + text retention (SPEC §7.15): purge the expired `a2a_audit` rows
    // and the text-less inbox rows 14 days after their timestamp. A pass runs
    // at startup and then at least hourly on a background thread.
    crate::core::a2a::inbox::start_purge_task(state_root.clone());

    // The A2A surface authenticates with jobset credentials only (SPEC §7.4):
    // no operator bearer and no open mode. It therefore requires the jobset
    // root key: without it the server refuses to start (fail closed).
    #[cfg(feature = "a2a")]
    crate::core::a2a::require_root_key("agent-orchestrate")?;

    // The A2A router shares the SAME SessionsApiState (and thus the same
    // message rate limiter) as the REST /api/v1/sessions router above, so the
    // REST and A2A message paths compete for one budget per (session, sender).
    #[cfg(feature = "a2a")]
    let a2a_jobset = crate::core::a2a::JobsetCred::from_host()
        .ok_or_else(|| anyhow::anyhow!("agent-orchestrate: no jobset registry available"))?;
    // The session inbox root is the shared agent-run state root: the ONE
    // definition both the sink and the session launcher resolve
    // (`core::a2a::inbox::inbox_state_root`, derived exactly like the
    // agent-run config's `state_root` above). The session's
    // `KUBEADP_A2A_INBOX_DIR` therefore points at this exact root, so a
    // delivered message lands in the directory the session's hook/mcp-a2a
    // reader and sandbox grant cover.
    #[cfg(feature = "a2a")]
    let a2a_routes = crate::orchestrate::a2a::a2a_router_jobset_only(
        state.clone(),
        controls.clone(),
        api_state,
        a2a_jobset,
        crate::core::a2a::peer_end_notifier_from_env(),
        crate::core::a2a::inbox::inbox_state_root(),
        a2a_ladder,
    );

    let ready = {
        let state = Arc::clone(&state);
        Arc::new(move || -> Result<(), String> {
            let st = state.lock().map_err(|e| e.to_string())?;
            crate::orchestrate::state::check_status(&st, 60.0).map_err(|e| format!("{e:?}"))
        })
    };
    let state_metrics = {
        let state = Arc::clone(&state);
        Arc::new(move || {
            let st = state.lock().unwrap_or_else(|e| e.into_inner());
            let mut counts: std::collections::BTreeMap<String, u64> =
                std::collections::BTreeMap::new();
            for sess in st.sessions.values() {
                *counts.entry(sess.status.to_string()).or_default() += 1;
            }
            counts.into_iter().collect()
        })
    };

    let app = base;
    // The operator bearer layer guards `/api/v1` (and `/metrics`) only. The A2A
    // routes carry their own authentication layer (operator bearer OR a
    // jobset credential, plus the public agent card), so they are merged
    // AFTER the bearer layer: wrapping them in it would reject every jobset
    // member and hide the agent card.
    let app = crate::core::server::obs::add_observability(app, auth_state, ready, state_metrics);
    #[cfg(feature = "a2a")]
    let app = app.merge(a2a_routes);
    // The sandboxed session's inbox API: reachable pod-locally by the run's
    // own process under a per-run token, NEVER the operator bearer (a
    // sandboxed agent has no operator token). Mounted after the bearer layer
    // exactly like the A2A routes.
    #[cfg(feature = "a2a")]
    let app = {
        let state = crate::core::a2a::http::InboxHttpState::resolve(
            crate::core::a2a::inbox::inbox_state_root(),
        )
        .map_err(|e| anyhow::anyhow!("failed to resolve the A2A inbox token key: {e}"))?;
        app.merge(crate::core::a2a::http::inbox_http_router(state))
    };

    Ok(app)
}

/// Resolves the operator-token auth state for the supervisor's HTTP API.
///
/// The shared `KUBEADP_API_TOKEN_FILE` (config `api_token_file`) is the
/// primary source for both roles; when it is not set, the legacy
/// `AGENT_ORCHESTRATE_TOKEN_FILE` / `AGENT_ORCHESTRATE_TOKEN` is honoured so
/// existing deployments keep working unchanged. With no token configured the
/// server refuses to start unless `api_allow_unauthenticated` is set (a
/// missing/empty legacy token is never a valid reason to serve open: the
/// caller must pass the explicit flag).
fn resolve_startup_auth_fallback() -> anyhow::Result<crate::core::server::auth::AuthState> {
    // If the shared env is configured as a _FILE, that is authoritative and
    // error-prone (unreadable = fatal) exactly like resolve_operator_token.
    if std::env::var_os(crate::core::server::token::API_TOKEN_FILE_ENV).is_some() {
        return crate::core::server::auth::resolve_startup_auth();
    }
    // Otherwise fold the legacy orchestrator token in as the fallback token.
    if let Some(token) = bearer_token_from_env() {
        return Ok(crate::core::server::auth::AuthState::token(token));
    }
    // No shared and no legacy token: apply the startup gate.
    crate::core::server::auth::resolve_startup_auth()
}

/// Default listen address for the sessions HTTP server.
pub fn listen_addr_from_env() -> String {
    std::env::var("AGENT_ORCHESTRATE_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_string())
}

/// Runs the HTTP server on a dedicated background thread (tokio multi-thread
/// runtime) so the supervisor's blocking session loop never starves handlers.
pub fn spawn_http_server(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    session_logs: Arc<SessionLogs>,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>> {
    let listen = listen_addr_from_env();
    let listen_log = listen.clone();

    let thread = std::thread::Builder::new()
        .name("http-server".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    crate::orchestrate::logging::log_event(
                        "supervisor",
                        &format!("failed to start tokio runtime for HTTP server: {e}"),
                    );
                    return;
                }
            };
            rt.block_on(async move {
                if let Err(e) = run_server(state, controls, session_logs, shutdown, &listen).await {
                    crate::orchestrate::logging::log_event(
                        "supervisor",
                        &format!("HTTP server error: {e:#}"),
                    );
                }
            });
        })
        .with_context(|| "failed to spawn HTTP server thread")?;

    crate::orchestrate::logging::log_event(
        "supervisor",
        &format!("sessions HTTP API listening on {listen_log}"),
    );
    Ok(thread)
}

async fn run_server(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    session_logs: Arc<SessionLogs>,
    shutdown: Arc<AtomicBool>,
    listen: &str,
) -> Result<()> {
    let app = build_router(state, controls, session_logs)?;

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind sessions HTTP API on {listen}"))?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        while !shutdown.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .context("sessions HTTP server error")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_env_lock<R>(f: impl FnOnce() -> R) -> R {
        let _guard = crate::test_env_lock();
        f()
    }

    #[test]
    fn test_resolve_startup_auth_shared_token_file_wins() {
        with_env_lock(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("api-token");
            std::fs::write(&path, "shared-token").unwrap();
            unsafe {
                std::env::set_var(crate::core::server::token::API_TOKEN_FILE_ENV, &path);
                std::env::remove_var("AGENT_ORCHESTRATE_TOKEN_FILE");
                std::env::remove_var("AGENT_ORCHESTRATE_TOKEN");
            }
            let state = resolve_startup_auth_fallback().unwrap();
            assert!(state.digest().is_some());
            unsafe {
                std::env::remove_var(crate::core::server::token::API_TOKEN_FILE_ENV);
            }
        });
    }

    #[test]
    fn test_resolve_startup_auth_legacy_orchestrator_token_fallback() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(crate::core::server::token::API_TOKEN_FILE_ENV);
                std::env::set_var("AGENT_ORCHESTRATE_TOKEN", "legacy-token");
            }
            let state = resolve_startup_auth_fallback().unwrap();
            assert!(state.digest().is_some());
            unsafe {
                std::env::remove_var("AGENT_ORCHESTRATE_TOKEN");
            }
        });
    }

    #[test]
    fn test_resolve_startup_auth_refused_without_any_token() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(crate::core::server::token::API_TOKEN_FILE_ENV);
                std::env::remove_var("AGENT_ORCHESTRATE_TOKEN_FILE");
                std::env::remove_var("AGENT_ORCHESTRATE_TOKEN");
            }
            // With the allow flag explicitly false, no token is fatal.
            unsafe {
                std::env::set_var(
                    crate::core::server::token::ALLOW_UNAUTHENTICATED_ENV,
                    "false",
                );
            }
            let refused = resolve_startup_auth_fallback();
            assert!(
                refused.is_err(),
                "no token + flag false must refuse to start"
            );

            // With the flag true the server starts open (digest None).
            unsafe {
                std::env::set_var(
                    crate::core::server::token::ALLOW_UNAUTHENTICATED_ENV,
                    "true",
                );
            }
            let open = resolve_startup_auth_fallback();
            assert!(open.unwrap().digest().is_none());

            unsafe {
                std::env::remove_var(crate::core::server::token::ALLOW_UNAUTHENTICATED_ENV);
            }
        });
    }
}

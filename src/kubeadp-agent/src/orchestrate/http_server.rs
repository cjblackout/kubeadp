//! HTTP server for the supervisor: merges the shared `crate::core::server`
//! router (identical `/api/v1` routes and schemas so the `agent-run` CLI's
//! `/workspace` client works unchanged) with the supervisor's own
//! `/api/v1/sessions` control group.
//!
//! Runs on its own tokio runtime in a background thread so it never blocks
//! the supervisor's (blocking, std::thread based) session loop. The listen
//! address defaults to `127.0.0.1:8003` and is overridable with
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

use crate::orchestrate::server::{router_with_rc, RemoteControlState};
use crate::orchestrate::session_control::SessionControlRegistry;
use crate::orchestrate::session_log::SessionLogs;
use crate::orchestrate::sessions_api::{
    bearer_token_from_env, router as sessions_router, SessionsApiState,
};
use crate::orchestrate::state::SupervisorState;

pub const DEFAULT_LISTEN: &str = "127.0.0.1:8003";

/// Builds the merged router: agent-run routes + sessions group, all behind
/// one shared bearer-auth layer (`/readyz`, `/healthz` and `/metrics` are
/// added by [`crate::core::server::obs::add_observability`]; `/metrics` is
/// behind the same auth as `/api/v1` when a token is configured).
pub fn build_router(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    session_logs: Arc<SessionLogs>,
) -> Router {
    let core_config = crate::core::config::Config::load().unwrap_or_else(|e| {
        crate::orchestrate::logging::log_event(
            "supervisor",
            &format!("agent-run config load failed ({}); using defaults", e),
        );
        crate::core::config::Config::default()
    });
    let auth = Arc::new(crate::core::auth::AuthManager::new());
    let rc_state = RemoteControlState::default();
    let base = router_with_rc(Arc::new(core_config), auth, rc_state);

    let api_state = SessionsApiState::new(state.clone(), controls.clone(), session_logs);
    let sessions = sessions_router(api_state.clone());

    let auth_token = bearer_token_from_env();
    let auth_state = match &auth_token {
        Some(token) => crate::core::server::auth::AuthState::token(token.clone()),
        None => crate::core::server::auth::AuthState::none(),
    };

    #[cfg(feature = "a2a")]
    let a2a_auth: Option<crate::core::a2a::AuthVerifier> = auth_token.map(|token| {
        let expected = token.into_bytes();
        std::sync::Arc::new(move |headers: &axum::http::HeaderMap| {
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| {
                    v.strip_prefix("Bearer ").is_some_and(|t| {
                        crate::core::a2a::constant_time_eq(t.as_bytes(), &expected)
                    })
                })
        }) as crate::core::a2a::AuthVerifier
    });

    #[cfg(feature = "a2a")]
    // The A2A router shares the SAME SessionsApiState (and thus the same
    // message rate limiter) as the REST /api/v1/sessions router above, so the
    // REST and A2A message paths compete for one budget per (session, sender).
    let a2a_routes =
        crate::orchestrate::a2a::a2a_router(state.clone(), controls.clone(), api_state, a2a_auth);

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

    let app = base.merge(sessions);
    #[cfg(feature = "a2a")]
    let app = app.merge(a2a_routes);

    crate::core::server::obs::add_observability(app, auth_state, ready, state_metrics)
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
    let app = build_router(state, controls, session_logs);

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

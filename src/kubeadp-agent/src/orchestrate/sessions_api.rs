//! HTTP REST API for supervising agent sessions (LIST, GET, START, STOP,
//! RESTART, LOG tail).
//!
//! This is the supervisor's own addition on top of the shared
//! `crate::core::server` module: it exposes the sessions the supervisor
//! manages so the `agent-run` CLI (or any in-cluster client) can inspect and
//! control them. It deliberately never writes to a session's PTY, so it
//! cannot become a remote shell: control is limited to pausing/resuming the
//! supervision thread (which then kills the child process group) and to a
//! redacted log tail served from the ring buffer.
//!
//! Auth is handled ONCE, by the shared [`crate::core::server::obs`] layer that
//! wraps the whole `/api/v1` group in [`crate::core::server::auth`] (see
//! `crate::orchestrate::http_server::build_router`): every `/api/v1` route -- the
//! agent-run routes and these sessions routes alike -- requires the bearer
//! token when `AGENT_ORCHESTRATE_TOKEN_FILE` (or `AGENT_ORCHESTRATE_TOKEN`)
//! is configured, and none when it is not. There are no per-handler auth
//! checks here, so a route added to this router is protected automatically.
//! `/healthz` and `/readyz` never require a token.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::core::server::reject::{ApiPath, ApiQuery};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::orchestrate::config::SupervisorConfig;
use crate::orchestrate::logging::log_event;
use crate::orchestrate::session_control::{set_paused, SessionControl, SessionControlRegistry};
use crate::orchestrate::session_log::SessionLogs;
use crate::orchestrate::state::{SessionStatus, SupervisorState};

/// Shared runtime state handed to the sessions API handlers as an Extension.
#[derive(Clone)]
pub struct SessionsApiState {
    /// The supervisor's in-memory session state (pid, status, restart counts).
    pub supervisor_state: Arc<Mutex<SupervisorState>>,
    /// Per-session control handles (paused/running/restart flags).
    pub controls: SessionControlRegistry,
    /// Per-session redacted log ring buffers.
    pub session_logs: Arc<SessionLogs>,
    /// Per-session sliding-window rate limiter for injected messages.
    pub message_limits: Arc<Mutex<MessageRateLimiter>>,
}

impl std::fmt::Debug for SessionsApiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SessionsApiState {{ controls: {} }}",
            self.controls.len()
        )
    }
}

impl SessionsApiState {
    /// Builds API state from the pieces the supervisor already holds.
    pub fn new(
        supervisor_state: Arc<Mutex<SupervisorState>>,
        controls: SessionControlRegistry,
        session_logs: Arc<SessionLogs>,
    ) -> Arc<Self> {
        Arc::new(Self {
            supervisor_state,
            controls,
            session_logs,
            message_limits: Arc::new(Mutex::new(MessageRateLimiter::new())),
        })
    }
}

/// Builds the `/api/v1/sessions` router group as a `Router<()>` so it merges
/// with the shared agent-run server router.
///
/// Auth is applied once, to the whole merged `/api/v1` group, by
/// [`crate::orchestrate::http_server::build_router`] -- this router itself has no
/// per-handler auth check.
pub fn router(state: Arc<SessionsApiState>) -> Router<()> {
    Router::new()
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/{name}", get(get_session))
        .route("/api/v1/sessions/{name}/start", post(start_session))
        .route("/api/v1/sessions/{name}/stop", post(stop_session))
        .route("/api/v1/sessions/{name}/restart", post(restart_session))
        .route("/api/v1/sessions/{name}/log", get(get_session_log))
        .route("/api/v1/sessions/{name}/message", post(inject_message))
        .with_state(())
        .layer(Extension(state))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn session_state_snapshot(
    state: &SessionsApiState,
    name: &str,
) -> Option<(crate::orchestrate::state::SessionState, bool, bool)> {
    let st = state
        .supervisor_state
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let sess = st.sessions.get(name)?;
    let control = state.controls.get(name);
    let (paused, running) = match control {
        Some(c) => (
            c.paused.load(Ordering::SeqCst),
            c.running.load(Ordering::SeqCst),
        ),
        None => (false, false),
    };
    Some((sess.clone(), paused, running))
}

fn session_json(
    name: &str,
    sess: &crate::orchestrate::state::SessionState,
    paused: bool,
    running: bool,
) -> Value {
    json!({
        "name": name,
        "status": sess.status.to_string(),
        "pid": sess.pid,
        "start_time": sess.start_time,
        "start_time_epoch": sess.start_time_epoch,
        "uptime_secs": sess.uptime_secs,
        "restart_count": sess.restart_count,
        "last_exit_reason": sess.last_exit_reason,
        "last_health_result": sess.last_health_result,
        "paused": paused,
        "controlled": running,
    })
}

async fn list_sessions(Extension(state): Extension<Arc<SessionsApiState>>) -> Response {
    let names: Vec<String> = {
        let st = state
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        st.sessions.keys().cloned().collect()
    };
    let mut sessions: Vec<Value> = Vec::with_capacity(names.len());
    for name in names {
        if let Some((sess, paused, running)) = session_state_snapshot(&state, &name) {
            sessions.push(session_json(&name, &sess, paused, running));
        }
    }
    Json(json!({ "sessions": sessions })).into_response()
}

async fn get_session(
    ApiPath(name): ApiPath<String>,
    Extension(state): Extension<Arc<SessionsApiState>>,
) -> Response {
    match session_state_snapshot(&state, &name) {
        Some((sess, paused, running)) => {
            Json(session_json(&name, &sess, paused, running)).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no session named '{}'", name), "code": "session_not_found" })),
        )
            .into_response(),
    }
}

/// Finds a control or returns a 404 response.
fn find_control(
    state: &SessionsApiState,
    name: &str,
) -> Result<Arc<crate::orchestrate::session_control::SessionControl>, Box<Response>> {
    let ctl = state.controls.get(name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no session named '{}'", name), "code": "session_not_found" })),
        )
            .into_response()
    })?;
    Ok(ctl)
}

fn control_response(state: &SessionsApiState, name: &str) -> Response {
    match session_state_snapshot(state, name) {
        Some((sess, paused, running)) => {
            Json(session_json(name, &sess, paused, running)).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no session named '{}'", name), "code": "session_not_found" })),
        )
            .into_response(),
    }
}

/// `POST /api/v1/sessions/{name}/stop`: pause the supervision thread. The
/// loop exits and the bounded child-escalation path kills the process group.
async fn stop_session(
    Extension(state): Extension<Arc<SessionsApiState>>,
    ApiPath(name): ApiPath<String>,
) -> Response {
    let _control = match find_control(&state, &name) {
        Ok(c) => c,
        Err(resp) => return *resp,
    };
    let cloned = state.controls.get(&name).expect("control just fetched");
    set_paused(&cloned, true);
    // Mark status Stopped immediately so callers see a deterministic result
    // even before the thread exits.
    {
        let mut st = state
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(sess) = st.sessions.get_mut(&name) {
            if sess.status != SessionStatus::Failed {
                sess.status = SessionStatus::Stopped;
            }
        }
    }
    control_response(&state, &name)
}

/// `POST /api/v1/sessions/{name}/start`: resume a stopped/paused session or
/// (re)spawn its thread if none is running.
async fn start_session(
    Extension(state): Extension<Arc<SessionsApiState>>,
    ApiPath(name): ApiPath<String>,
) -> Response {
    let control = match find_control(&state, &name) {
        Ok(c) => c,
        Err(resp) => return *resp,
    };
    if control.running.load(Ordering::SeqCst) {
        // Live thread: unpause it.
        set_paused(&control, false);
    } else {
        // No live thread: unpause and ask the heartbeat loop to (re)spawn one.
        set_paused(&control, false);
        control.restart.store(true, Ordering::SeqCst);
    }
    control_response(&state, &name)
}

/// `POST /api/v1/sessions/{name}/restart`: stop the running child (same path
/// as `stop`), wait until its thread has fully exited, then clear `paused` and
/// ask the heartbeat loop to spawn a fresh session thread.
///
/// The exit wait matters: the heartbeat loop only honours `restart` when no
/// live handle is registered, so setting `restart` while the old thread is
/// still draining would consume the flag and leave the session stopped with
/// `paused: true` (the defect this fixes).
async fn restart_session(
    Extension(state): Extension<Arc<SessionsApiState>>,
    ApiPath(name): ApiPath<String>,
) -> Response {
    let control = match find_control(&state, &name) {
        Ok(c) => c,
        Err(resp) => return *resp,
    };
    if !stop_and_wait_for_exit(&control).await {
        // The thread did not drain within the kill bounds (stuck child).
        // Leave it paused and do not arm a restart the draining thread could
        // consume; a later restart (or start) retries.
        return control_response(&state, &name);
    }
    // Clear paused so the respawned thread runs. restart_count is left
    // unchanged: like `stop`, this is a controlled pause, not a crash, so no
    // crash accounting applies.
    set_paused(&control, false);
    control.restart.store(true, Ordering::SeqCst);
    control_response(&state, &name)
}

/// Pauses a session (same path as `stop`) and blocks until its supervision
/// thread has exited (or up to the configured kill bounds). Returns true when
/// the thread is confirmed gone, false on timeout. A session with no live
/// thread counts as already exited.
async fn stop_and_wait_for_exit(control: &SessionControl) -> bool {
    set_paused(control, true);
    let max_wait = Duration::from_secs_f64(
        control.config.kill_grace_secs
            + control.config.kill_wait_secs
            + control.config.pty_eof_grace_secs
            + 2.0,
    );
    let deadline = tokio::time::Instant::now() + max_wait;
    while control.running.load(Ordering::SeqCst) {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
}

#[derive(Deserialize)]
struct LogQuery {
    /// Tail lines (default 5000) or tail bytes, resolved on the log itself.
    tail: Option<u64>,
}

/// `GET /api/v1/sessions/{name}/log?tail=N`: serve a redacted tail of the
/// session ring buffer. `tail` is an approximate byte budget; the response
/// starts at a line boundary so clients get whole lines.
async fn get_session_log(
    Extension(state): Extension<Arc<SessionsApiState>>,
    ApiPath(name): ApiPath<String>,
    ApiQuery(query): ApiQuery<LogQuery>,
) -> Response {
    if !state.controls.contains_key(&name) && {
        let st = state
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        !st.sessions.contains_key(&name)
    } {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no session named '{}'", name), "code": "session_not_found" })),
        )
            .into_response();
    }

    let tail_lines = query.tail.unwrap_or(5000).clamp(1, 4_000_000) as usize;
    let raw = state.session_logs.tail_lines(&name, tail_lines);
    let Some(raw) = raw else {
        return Json(json!({ "session": name, "log": "" })).into_response();
    };

    let redacted = crate::orchestrate::session_log::redact_secrets(&raw);
    let text = String::from_utf8_lossy(&redacted);
    // Trim to a newline boundary so we don't cut mid-line.
    let text = match text.rfind('\n') {
        Some(idx) if idx + 1 < text.len() + 1 => &text[..=idx],
        _ => text.as_ref(),
    };
    Json(json!({ "session": name, "log": text })).into_response()
}

// ---------------------------------------------------------------------------
// Message injection
// ---------------------------------------------------------------------------

/// Maximum message size in bytes (after UTF-8 validation and trimming). This is
/// the single PTY-delivery cap; the A2A ingress size limit is derived from it
/// (see [`crate::core::a2a::ingress::MAX_INGRESS_TEXT_BYTES`]).
pub const MESSAGE_MAX_BYTES: usize = 8192;
/// Bracketed paste open: the child treats everything until `CSI 201~` as
/// pasted input, so embedded newlines never submit early.
const PASTE_START: &[u8] = b"\x1b[200~";
/// Bracketed paste close.
const PASTE_END: &[u8] = b"\x1b[201~";
/// Messages allowed per (session, sender) per minute.
pub const MESSAGE_RATE_MAX: usize = 30;
const MESSAGE_RATE_WINDOW: Duration = Duration::from_secs(60);

/// Sliding-window rate limiter: at most `MESSAGE_RATE_MAX` injected messages
/// per (session, sender) within `MESSAGE_RATE_WINDOW`, with `Retry-After` on
/// the burst. The budget is keyed by the *sender* so one noisy sender cannot
/// exhaust a session's budget and starve the legitimate peers of the same
/// session (the REST path uses the "rest" sender, the A2A path uses the
/// claimed `a2a.sender`).
#[derive(Default)]
pub struct MessageRateLimiter {
    per_sender: HashMap<String, Vec<Instant>>,
}

impl MessageRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one accepted message from `sender` into `session` and returns
    /// `Some(retry_after_secs)` when that (session, sender) pair exceeds the
    /// window budget, in which case nothing is added.
    pub fn record(&mut self, session: &str, sender: &str, now: Instant) -> Option<u64> {
        self.cleanup(now);
        let key = format!("{session}\x1f{sender}");
        let window = self.per_sender.entry(key).or_default();
        if window.len() >= MESSAGE_RATE_MAX {
            let oldest = window[0];
            let elapsed = now.duration_since(oldest);
            return Some(if elapsed < MESSAGE_RATE_WINDOW {
                (MESSAGE_RATE_WINDOW - elapsed).as_secs().max(1)
            } else {
                1
            });
        }
        window.push(now);
        None
    }

    fn cleanup(&mut self, now: Instant) {
        self.per_sender.retain(|_, ts| {
            ts.retain(|&t| now.duration_since(t) < MESSAGE_RATE_WINDOW);
            !ts.is_empty()
        });
    }

    /// How many accepted messages the (session, sender) pair currently has in
    /// its sliding window (test/introspection hook).
    pub fn uses(&self, session: &str, sender: &str) -> usize {
        self.per_sender
            .get(&format!("{session}\x1f{sender}"))
            .map(|ts| ts.len())
            .unwrap_or(0)
    }
}

/// The synthetic sender identity for messages injected over the REST API
/// (which has no A2A claimed sender). Distinct from any A2A `a2a.sender`.
pub const REST_SENDER: &str = "\x00rest";

/// `POST /api/v1/sessions/{name}/message` body.
#[derive(Deserialize)]
struct MessageRequest {
    text: String,
}

/// Strips control bytes and escape sequences from a message, keeping `\n` and
/// `\r` (both are delivered as paste content, never submitted early). An
/// embedded `ESC[201~` is removed wholesale so input cannot close the paste
/// bracket in the middle of the payload. `\r` is converted into `\n` so all
/// newlines arrive uniformly inside the paste.
pub fn sanitize_message_text(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\n' {
            out.push(b'\n');
            i += 1;
        } else if b == b'\r' {
            out.extend_from_slice(b"\n");
            i += 1;
        } else if b == 0x1b {
            let rem = &bytes[i..];
            if rem.starts_with(PASTE_END) {
                i += PASTE_END.len();
            } else {
                // Consume a whole CSI/OSC escape: ESC followed by a bracket
                // byte and an argument set terminated by a final byte in the
                // 0x40..=0x7E range (or a lone ESC otherwise).
                if rem.len() >= 2 && (rem[1] == b'[' || rem[1] == b']') {
                    let mut j = 2;
                    while j < rem.len() && (0x20..=0x3F).contains(&rem[j]) {
                        j += 1;
                    }
                    if j < rem.len() {
                        j += 1;
                    }
                    i += j;
                } else {
                    i += 1;
                }
            }
        } else if (0x00..0x20).contains(&b) || b == 0x7f {
            i += 1;
        } else {
            out.push(b);
            i += 1;
        }
    }
    out
}

/// FNV-1a prefix of the message bytes for the audit log (never the text).
pub fn message_hash_prefix(text: &[u8], max_hex: usize) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in text {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let hex = format!("{hash:016x}");
    hex.chars().take(max_hex).collect()
}

/// `POST /api/v1/sessions/{name}/message`: inject one message into a running
/// session's PTY as a bracketed paste (so newlines never submit early and the
/// text cannot run as a command) followed by a single Enter. Claude Code
/// queues text typed during an active turn, so this never kills the session.
async fn inject_message(
    Extension(state): Extension<Arc<SessionsApiState>>,
    ApiPath(name): ApiPath<String>,
    crate::core::server::reject::ApiJson(req): crate::core::server::reject::ApiJson<MessageRequest>,
) -> Response {
    let MessageRequest { text } = req;
    // deliver_message does a (deadline-bounded) blocking PTY write and takes
    // the pty_input mutex; run it off the tokio worker threads so a stuck
    // child cannot hold a worker thread (and the mutex) and starve /healthz.
    let state = Arc::clone(&state);
    let closure_name = name.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        deliver_message(&state, &closure_name, REST_SENDER, &text)
    })
    .await
    .expect("spawn_blocking task panicked or runtime shut down");
    match outcome {
        DeliverOutcome::Accepted { bytes } => (
            StatusCode::ACCEPTED,
            Json(json!({ "session": name, "accepted": true, "bytes": bytes })),
        )
            .into_response(),
        DeliverOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no session named '{name}'"), "code": "session_not_found" })),
        )
            .into_response(),
        DeliverOutcome::Empty => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "message text must not be empty", "code": "invalid_body" })),
        )
            .into_response(),
        DeliverOutcome::TooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({ "error": "message text is too large", "code": "too_large" })),
        )
            .into_response(),
        DeliverOutcome::RateLimited { retry_after_secs } => (
            StatusCode::TOO_MANY_REQUESTS,
            [
                (axum::http::header::RETRY_AFTER, retry_after_secs.to_string()),
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/json".to_string(),
                ),
            ],
            Json(json!({ "code": "rate_limited" })),
        )
            .into_response(),
        DeliverOutcome::NotRunning => (StatusCode::CONFLICT, Json(json!({ "code": "not_running" }))).into_response(),
    }
}

/// Outcome of a [`deliver_message`] attempt, shared by the REST handler and
/// the A2A `SendMessage` sink so both paths use the same session lookup, PTY
/// write and message budget.
pub enum DeliverOutcome {
    /// The message was written to the session PTY.
    Accepted { bytes: usize },
    /// No such session.
    NotFound,
    /// Trimmed text is empty.
    Empty,
    /// Text exceeds [`MESSAGE_MAX_BYTES`].
    TooLarge,
    /// The per-session budget is exhausted.
    RateLimited { retry_after_secs: u64 },
    /// The session has no live PTY (stopped/paused/stale slot).
    NotRunning,
}

/// Shared message-delivery core behind both `POST /api/v1/sessions/{name}/message`
/// and the A2A `SendMessage` sink: validates the session and text, sanitizes
/// the payload, respects the shared per-(session, sender) rate budget and
/// writes a bracketed-paste payload (never a raw command line) into the
/// session PTY. `sender` identifies who is injecting (the REST caller for the
/// REST path, the claimed `a2a.sender` for the A2A path) so one noisy sender
/// cannot exhaust the session's budget.
pub fn deliver_message(
    state: &SessionsApiState,
    name: &str,
    sender: &str,
    text: &str,
) -> DeliverOutcome {
    // 404/not_found for an unknown session.
    let Some(control) = state.controls.get(name) else {
        return DeliverOutcome::NotFound;
    };

    // 413 too_large is evaluated on the trimmed byte length.
    let text = text.trim();
    if text.is_empty() {
        return DeliverOutcome::Empty;
    }
    if text.len() > MESSAGE_MAX_BYTES {
        return DeliverOutcome::TooLarge;
    }

    // not_running when the session has no live PTY (stopped/paused or a
    // dead thread left a stale slot).
    let payload = sanitize_message_text(text);
    let write_ok = {
        let mut slot = control.pty_input.lock().unwrap_or_else(|e| e.into_inner());
        if !slot.taken() {
            false
        } else {
            let mut limits = state
                .message_limits
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(retry_after) = limits.record(name, sender, Instant::now()) {
                return DeliverOutcome::RateLimited {
                    retry_after_secs: retry_after,
                };
            }
            // The PTY write is bounded by a deadline (see PtyInput::write_all):
            // a stuck child that never drains its input queue turns into a
            // timeout instead of wedging the tokio worker / session thread.
            let deadline = Instant::now() + crate::orchestrate::pty::PTY_WRITE_TOTAL_DEADLINE;
            let written = slot
                .write_all(PASTE_START, deadline)
                .and_then(|_| slot.write_all(&payload, deadline))
                .and_then(|_| slot.write_all(PASTE_END, deadline))
                .and_then(|_| slot.write_all(b"\r", deadline));
            written.is_ok()
        }
    };
    let n = payload.len();

    if !write_ok {
        return DeliverOutcome::NotRunning;
    }

    // Audit line: session, byte count and a hash prefix, never the message
    // text itself (the session log ring is never touched).
    log_event(
        name,
        &format!(
            "message injected bytes={n} sha={}",
            message_hash_prefix(&payload, 12)
        ),
    );
    DeliverOutcome::Accepted { bytes: n }
}

/// Builds the bearer token from env (file takes precedence over inline).
pub fn bearer_token_from_env() -> Option<String> {
    if let Some(path) = std::env::var_os("AGENT_ORCHESTRATE_TOKEN_FILE") {
        let p = std::path::PathBuf::from(&path);
        if let Ok(bytes) = std::fs::read(&p) {
            let s = String::from_utf8_lossy(&bytes).trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    std::env::var("AGENT_ORCHESTRATE_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Returns the list of session names from config (used by main.rs to seed the
/// registry before spawning threads).
pub fn config_session_names(config: &SupervisorConfig) -> Vec<String> {
    config.sessions.iter().map(|s| s.name.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrate::session_control::SessionControl;
    use crate::orchestrate::state::SessionState;

    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn sample_state(name: &str) -> SupervisorState {
        let mut st = SupervisorState::new(1234);
        st.sessions
            .insert(name.to_string(), SessionState::new(name.to_string()));
        st
    }

    fn api_state() -> Arc<SessionsApiState> {
        let st = Arc::new(Mutex::new(sample_state("sess-a")));
        let logs = Arc::new(SessionLogs::new(10_000));
        logs.append("sess-a", b"line one\nBearer secret-abc\nline three\n");
        SessionsApiState::new(st, SessionControlRegistry::new(), logs)
    }

    fn seeded_registry_state() -> Arc<SessionsApiState> {
        let st = Arc::new(Mutex::new(sample_state("sess-a")));
        let logs = Arc::new(SessionLogs::new(10_000));
        logs.append("sess-a", b"hello world\n");
        let controls = SessionControlRegistry::new();
        controls.insert(Arc::new(SessionControl::new(
            crate::orchestrate::config::SessionConfig {
                name: "sess-a".to_string(),
                workdir: std::path::PathBuf::from("/tmp"),
                session_id: "session-a".to_string(),
                claude_binary: std::path::PathBuf::from("/bin/true"),
                claude_args: vec![],
                env: Default::default(),
                session_file: None,
                memory_file: None,
                readiness_poll_secs: 1.0,
                hang_timeout_secs: 0.0,
                kill_grace_secs: 1.0,
                pty_eof_grace_secs: 1.0,
                kill_wait_secs: 2.0,
                sandbox: true,
                grants: None,
                base_backoff_secs: 1.0,
                max_backoff_secs: 10.0,
                healthy_run_secs: 30.0,
                max_crashes: 3,
                crash_window_secs: 60.0,
                prefix_output: true,
                transcript_path: None,
                failed_cooldown_secs: 1.0,
                backoff_window_secs: 60.0,
                resume_fail_quarantine_after: 3,
                resume_fail_uptime_secs: 60.0,
            },
        )));
        SessionsApiState::new(st, controls, logs)
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("request")
    }

    fn get_request_auth(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .expect("request")
    }

    fn post_request(uri: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .body(Body::empty())
            .expect("request")
    }

    async fn body_string(body: axum::body::Body) -> String {
        let bytes = body.collect().await.expect("body").to_bytes();
        String::from_utf8_lossy(&bytes).to_string()
    }

    // -----------------------------------------------------------------------
    // Auth (shared layer): the whole merged /api/v1 group -- agent-run and
    // sessions routes alike -- is gated by the shared bearer layer from
    // `crate::core::server::obs`. These tests build the router the same way
    // `crate::orchestrate::http_server::build_router` does.
    // -----------------------------------------------------------------------

    fn authed_router(token: &str) -> axum::Router {
        let base = crate::core::server::router(
            Arc::new(crate::core::config::Config::default()),
            Arc::new(crate::core::auth::AuthManager::new()),
        );
        let sess = router(api_state());
        crate::core::server::obs::add_observability(
            base.merge(sess),
            crate::core::server::auth::AuthState::token(token.to_string()),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        )
    }

    fn open_router() -> axum::Router {
        let base = crate::core::server::router(
            Arc::new(crate::core::config::Config::default()),
            Arc::new(crate::core::auth::AuthManager::new()),
        );
        let sess = router(api_state());
        crate::core::server::obs::add_observability(
            base.merge(sess),
            crate::core::server::auth::AuthState::none(),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        )
    }

    #[tokio::test]
    async fn test_read_routes_require_token_when_configured() {
        // Every /api/v1 read route must answer 401 without a token, and 200
        // with one: sessions (the supervisor's own group) as well as jobs and
        // info (the shared agent-run group) -- no route can be added without
        // auth by accident.
        let app = authed_router("sekret-token");
        for uri in [
            "/api/v1/sessions",
            "/api/v1/jobs",
            "/api/v1/info",
            "/api/v1/sessions/sess-a",
            "/api/v1/sessions/sess-a/log",
        ] {
            let res = app.clone().oneshot(get_request(uri)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "GET {uri} without token must be 401"
            );
            let res_ok = app
                .clone()
                .oneshot(get_request_auth(uri, "sekret-token"))
                .await
                .unwrap();
            assert_eq!(
                res_ok.status(),
                StatusCode::OK,
                "GET {uri} with token must be 200"
            );
        }
    }

    #[tokio::test]
    async fn test_wrong_token_is_401_with_json_body() {
        let app = authed_router("sekret-token");
        let res = app
            .oneshot(get_request_auth("/api/v1/sessions", "wrong"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unauthorized");
    }

    #[tokio::test]
    async fn test_no_token_configured_is_reachable() {
        // No token set: the routes are reachable without credentials
        // (in-cluster NetworkPolicy is the access control), matching
        // agent-run-api.
        let app = open_router();
        for uri in ["/api/v1/sessions", "/api/v1/jobs", "/api/v1/info"] {
            let res = app.clone().oneshot(get_request(uri)).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK, "GET {uri} must stay open");
        }
    }

    #[tokio::test]
    async fn test_health_and_ready_stay_open_with_token_configured() {
        let app = authed_router("sekret-token");
        let res = app.clone().oneshot(get_request("/healthz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let res = app.clone().oneshot(get_request("/readyz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_requires_token_when_configured() {
        let app = authed_router("sekret-token");
        let res = app.clone().oneshot(get_request("/metrics")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let res = app
            .oneshot(get_request_auth("/metrics", "sekret-token"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_session_not_found_carries_code() {
        let app = open_router();
        let res = app
            .oneshot(get_request("/api/v1/sessions/nope"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "session_not_found");
    }

    #[tokio::test]
    async fn test_session_not_found_body_leaks_no_state_path() {
        // HL-52: a not-found body must never carry a server-side state path
        // -- a path-shaped session name must not echo any filesystem location
        // back to the client; the only body content is the fixed message.
        let st = api_state();
        let app = router(st.clone());
        let res = app
            .oneshot(get_request("/api/v1/sessions/var-lib-agent-orchestrate"))
            .await
            .unwrap();
        let body = body_string(res.into_body()).await;
        assert!(
            !body.contains("agent-orchestrate-state.sqlite") && !body.contains("/var/lib"),
            "state path leaked: {body}"
        );
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["code"], "session_not_found");
    }

    #[tokio::test]
    async fn test_bearer_auth_redaction_in_log() {
        // The log handler returns a redacted tail: the secret after "Bearer "
        // must be masked even in the HTTP body.
        let st = api_state(); // contains "Bearer secret-abc" in the ring
        let app = router(st.clone());
        let res = app
            .oneshot(get_request("/api/v1/sessions/sess-a/log"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        assert!(body.contains("Bearer **********"), "body: {body}");
        assert!(!body.contains("secret-abc"), "secret leaked: {body}");
    }

    #[tokio::test]
    async fn test_list_sessions_handler_returns_names() {
        let st = api_state();
        let app = router(st.clone());
        let res = app.oneshot(get_request("/api/v1/sessions")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        let sessions = value["sessions"].as_array().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["name"], "sess-a");
    }

    #[tokio::test]
    async fn test_get_session_handler_found_and_missing() {
        let st = api_state();
        let app = router(st.clone());
        let ok = app
            .clone()
            .oneshot(get_request("/api/v1/sessions/sess-a"))
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        let body = body_string(ok.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["name"], "sess-a");

        let missing = app
            .oneshot(get_request("/api/v1/sessions/nope"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_stop_sets_paused_and_status() {
        let st = seeded_registry_state();
        let app = router(st.clone());
        let res = app
            .clone()
            .oneshot(post_request("/api/v1/sessions/sess-a/stop"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert!(value["paused"].as_bool().unwrap());
        let control = st.controls.get("sess-a").unwrap();
        assert!(control.paused.load(Ordering::SeqCst));

        // 404 for an unregistered session.
        let missing = app
            .oneshot(post_request("/api/v1/sessions/nope/stop"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_start_unpauses_or_restarts() {
        let st = seeded_registry_state();
        st.controls
            .get("sess-a")
            .unwrap()
            .paused
            .store(true, Ordering::SeqCst);
        let app = router(st.clone());
        let res = app
            .oneshot(post_request("/api/v1/sessions/sess-a/start"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let control = st.controls.get("sess-a").unwrap();
        assert!(!control.paused.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_start_when_not_running_sets_restart() {
        // No live thread (running stays false): start flips the restart flag.
        let st = seeded_registry_state();
        let app = router(st.clone());
        let res = app
            .oneshot(post_request("/api/v1/sessions/sess-a/start"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let control = st.controls.get("sess-a").unwrap();
        assert!(!control.paused.load(Ordering::SeqCst));
        assert!(control.restart.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_restart_clears_paused_and_sets_restart() {
        // A session with no live thread: restart must clear paused and ask the
        // heartbeat loop to spawn a fresh thread (previously it left the
        // session stopped with paused=true, which nothing ever undid).
        let st = seeded_registry_state();
        let app = router(st.clone());
        let res = app
            .oneshot(post_request("/api/v1/sessions/sess-a/restart"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert!(!value["paused"].as_bool().unwrap());
        let control = st.controls.get("sess-a").unwrap();
        assert!(!control.paused.load(Ordering::SeqCst));
        assert!(control.restart.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_restart_waits_for_thread_exit_before_arming() {
        // A live thread that never drains: restart pauses it but must NOT
        // clear paused or arm restart behind its back (they race the drain).
        let st = seeded_registry_state();
        let control = st.controls.get("sess-a").unwrap();
        control.running.store(true, Ordering::SeqCst);
        let app = router(st.clone());
        let res = app
            .oneshot(post_request("/api/v1/sessions/sess-a/restart"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(control.paused.load(Ordering::SeqCst));
        assert!(!control.restart.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_stop_and_wait_for_exit_returns_once_thread_is_done() {
        // The helper must only return true after running flips to false, so
        // the caller can safely arm restart without racing the draining thread.
        let st = seeded_registry_state();
        let control = st.controls.get("sess-a").unwrap();
        control.running.store(true, Ordering::SeqCst);
        let ctl = control.clone();
        let joiner = tokio::spawn(async move { stop_and_wait_for_exit(&ctl).await });
        // Let the helper observe the running flag a few times...
        tokio::time::sleep(Duration::from_millis(150)).await;
        control.running.store(false, Ordering::SeqCst);
        let done = tokio::time::timeout(Duration::from_secs(2), joiner)
            .await
            .expect("helper must return once the thread is done")
            .unwrap();
        assert!(done);
        assert!(control.paused.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_log_handler_tail_redacted_needs_control() {
        // Control is required for the log route (or state presence); helper
        // checks both. A known session with bytes serves the tail.
        let st = api_state();
        let app = router(st);
        let res = app
            .oneshot(get_request("/api/v1/sessions/sess-a/log?tail=5"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["session"], "sess-a");
        assert!(value["log"].as_str().unwrap().contains("Bearer **********"));
    }

    #[tokio::test]
    async fn test_log_handler_unknown_session_404() {
        let st = api_state();
        let app = router(st);
        let res = app
            .oneshot(get_request("/api/v1/sessions/ghost/log"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_log_handler_empty_log_returns_empty_string() {
        // A session present in state but with no collected bytes still yields
        // 200 with an empty log (logged-in shape), not an error.
        let st = seeded_registry_state();
        let app = router(st);
        let res = app
            .oneshot(get_request("/api/v1/sessions/sess-a/log"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_bearer_token_from_env_file_and_inline() {
        // File env takes precedence over inline env.
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "  from-file-token  \n").unwrap();
        unsafe {
            std::env::set_var("AGENT_ORCHESTRATE_TOKEN_FILE", &token_file);
            std::env::set_var("AGENT_ORCHESTRATE_TOKEN", "from-inline");
        }
        let got = bearer_token_from_env();
        unsafe {
            std::env::remove_var("AGENT_ORCHESTRATE_TOKEN_FILE");
            std::env::remove_var("AGENT_ORCHESTRATE_TOKEN");
        }
        assert_eq!(got.as_deref(), Some("from-file-token"));
    }

    // -----------------------------------------------------------------------
    // Message injection: sanitizer, rate limiter and the endpoint end to end.
    // -----------------------------------------------------------------------

    fn message_request(uri: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    /// Puts a real PTY master into the control's input slot (as the session
    /// thread would) so the endpoint has something live to write to.
    fn seed_pty_input(control: &Arc<SessionControl>, pty: &crate::orchestrate::pty::Pty) {
        control
            .pty_input
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(pty.master);
    }

    /// Switches the slave side to raw mode (input processing off, so an `\r`
    /// written to the master is not translated to `\n`). Must run before the
    /// write: the n_tty discipline translates input as it arrives.
    fn make_slave_raw(slave: libc::c_int) {
        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        unsafe {
            libc::tcgetattr(slave, &mut t);
            libc::cfmakeraw(&mut t);
            libc::tcsetattr(slave, libc::TCSANOW, &t);
        }
    }

    /// Blocks the caller reading the slave side of a PTY until `expected`
    /// bytes arrive or the timeout elapses, then returns them.
    fn read_slave_available(slave: libc::c_int, expected: usize, timeout: Duration) -> Vec<u8> {
        let deadline = std::time::Instant::now() + timeout;
        let mut out = Vec::new();
        let mut buf = [0u8; 512];
        while out.len() < expected && std::time::Instant::now() < deadline {
            let mut pfd = libc::pollfd {
                fd: slave,
                events: libc::POLLIN,
                revents: 0,
            };
            let ret = unsafe { libc::poll(&mut pfd, 1, 100) };
            if ret > 0 && (pfd.revents & libc::POLLIN) != 0 {
                let n =
                    unsafe { libc::read(slave, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if n > 0 {
                    out.extend_from_slice(&buf[..n as usize]);
                }
            }
        }
        out
    }

    #[test]
    fn test_sanitize_keeps_newlines_removes_controls() {
        let out = sanitize_message_text("line one\nline two\t\ngo  \x1b[31mred\x1b[0m now\n");
        // Controls, tab and escape sequences stripped; newlines kept.
        assert_eq!(out, b"line one\nline two\ngo  red now\n".to_vec());
    }

    #[test]
    fn test_sanitize_strips_embedded_csi_201() {
        // An embedded ESC[201~ must not close the paste bracket early.
        let out = sanitize_message_text("hello\x1b[201~world");
        assert_eq!(out, b"helloworld".to_vec());
        let out2 = sanitize_message_text("\x1b[201~");
        assert!(out2.is_empty());
    }

    #[test]
    fn test_sanitize_normalizes_carriage_returns() {
        let out = sanitize_message_text("a\r\nb");
        assert_eq!(out, b"a\n\nb".to_vec()); // \r -> \n, \n kept
    }

    #[test]
    fn test_message_hash_prefix_stable_and_strips() {
        // Deterministic per input and short enough to never leak the text.
        let h = message_hash_prefix(b"some message", 12);
        assert_eq!(h.len(), 12);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(h, message_hash_prefix(b"some message", 12));
        assert_ne!(h, message_hash_prefix(b"some messagf", 12));
    }

    #[test]
    fn test_message_rate_limiter_allows_30_then_rejects() {
        let mut lim = MessageRateLimiter::new();
        let now = Instant::now();
        for _ in 0..MESSAGE_RATE_MAX {
            assert!(lim.record("s", REST_SENDER, now).is_none());
        }
        let retry = lim.record("s", REST_SENDER, now);
        assert!(retry.is_some());
        // Other sessions are unaffected.
        assert!(lim.record("other", REST_SENDER, now).is_none());
    }

    #[test]
    fn test_message_rate_limiter_per_sender_budget() {
        // One sender exhausting a session's budget must NOT starve another
        // sender of the same session: the budget is per (session, sender).
        let mut lim = MessageRateLimiter::new();
        let now = Instant::now();
        for _ in 0..MESSAGE_RATE_MAX {
            assert!(lim.record("s", "noisy", now).is_none());
        }
        // "noisy" is now at its limit...
        assert!(lim.record("s", "noisy", now).is_some());
        // ...but a legitimate peer of the same session still has a full budget.
        assert!(lim.record("s", "peer", now).is_none());
        // And "rest" (the REST caller) has its own budget too.
        assert!(lim.record("s", REST_SENDER, now).is_none());
    }

    #[test]
    fn test_message_rate_limiter_sliding_window() {
        let mut lim = MessageRateLimiter::new();
        let now = Instant::now();
        for _ in 0..MESSAGE_RATE_MAX {
            lim.record("s", REST_SENDER, now);
        }
        // Once the oldest request falls out of the window the budget is free.
        let later = now + MESSAGE_RATE_WINDOW + Duration::from_secs(1);
        assert!(lim.record("s", REST_SENDER, later).is_none());
    }

    #[tokio::test]
    async fn test_inject_unknown_session_404() {
        let st = seeded_registry_state();
        let app = router(st);
        let res = app
            .oneshot(message_request(
                "/api/v1/sessions/ghost/message",
                r#"{"text":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_inject_no_live_pty_409() {
        // Session exists but no PTY slot is filled (stopped/paused).
        let st = seeded_registry_state();
        let app = router(st);
        let res = app
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                r#"{"text":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        let body = body_string(res.into_body()).await;
        assert!(body.contains("not_running"), "body: {body}");
    }

    #[tokio::test]
    async fn test_inject_bad_body_400() {
        let st = seeded_registry_state();
        let app = router(st);
        // Not JSON at all -> 400 with the standard {error, code} JSON shape.
        let res = app
            .clone()
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                "not json",
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "invalid_body");
        assert!(value["error"].is_string());
        // JSON but no `text` field -> 422 (axum's JsonDataError).
        let res2 = app
            .clone()
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                r#"{"other":1}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res2.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let value2: Value = serde_json::from_str(&body_string(res2.into_body()).await).unwrap();
        assert_eq!(value2["code"], "invalid_body");
        // Whitespace-only text.
        let res3 = app
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                r#"{"text":"   "}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res3.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_inject_wrong_content_type_is_415_json() {
        // HL-51: a request with the wrong content type is rejected with the
        // JSON error shape (415 unsupported media type), like every other
        // POST route.
        let st = seeded_registry_state();
        let app = router(st);
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/sessions/sess-a/message")
            .header("Content-Type", "text/plain")
            .body(Body::from(r#"{"text":"hi"}"#))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "invalid_content_type");
    }

    #[tokio::test]
    async fn test_inject_too_large_413() {
        let st = seeded_registry_state();
        let app = router(st);
        let big = "x".repeat(MESSAGE_MAX_BYTES + 1);
        let body = format!(r#"{{"text":"{}"}}"#, big);
        let res = app
            .oneshot(message_request("/api/v1/sessions/sess-a/message", &body))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_inject_accepted_bytes_and_not_in_logs() {
        let st = seeded_registry_state();
        let pty = crate::orchestrate::pty::Pty::open().expect("pty");
        make_slave_raw(pty.slave);
        seed_pty_input(&st.controls.get("sess-a").unwrap(), &pty);
        let app = router(st.clone());

        // "…red…" is text; "{red}" is an escape sequence that the sanitizer
        // strips, so the pasted payload never contains the ANSI codes.
        let body = "{\"text\":\"ZULU SECRET qi7\\u001b[31m{red}\\u001b[0m now\"}";
        let res = app
            .oneshot(message_request("/api/v1/sessions/sess-a/message", body))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["session"], "sess-a");
        assert_eq!(value["accepted"], true);
        assert!(value["bytes"].as_u64().unwrap() > 0);

        // The session log ring must be untouched: the secret never lands there.
        let raw = st.session_logs.tail_redacted("sess-a", 10_000).unwrap();
        let text = String::from_utf8_lossy(&raw);
        assert!(
            !text.contains("ZULU SECRET qi7"),
            "leaked into ring: {text}"
        );
    }

    #[tokio::test]
    async fn test_inject_reaches_slave_as_bracketed_paste() {
        let st = seeded_registry_state();
        let pty = crate::orchestrate::pty::Pty::open().expect("pty");
        make_slave_raw(pty.slave);
        seed_pty_input(&st.controls.get("sess-a").unwrap(), &pty);
        let app = router(st);

        let expected = b"hello there\nsecond line".as_slice();
        let wait_len = PASTE_START.len() + expected.len() + PASTE_END.len() + 1;

        let res = app
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                r#"{"text":"hello there\nsecond line\n"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let received = read_slave_available(pty.slave, wait_len, Duration::from_secs(2));
        assert_eq!(
            received,
            [PASTE_START, expected, PASTE_END, b"\r"].concat(),
            "child must see bracketed paste + one final Enter"
        );
    }

    #[tokio::test]
    async fn test_inject_embedded_csi201_not_closing_early() {
        let st = seeded_registry_state();
        let pty = crate::orchestrate::pty::Pty::open().expect("pty");
        make_slave_raw(pty.slave);
        seed_pty_input(&st.controls.get("sess-a").unwrap(), &pty);
        let app = router(st);

        // The sanitised payload is "hi"; a nested ESC[201~ must be dropped so
        // the paste cannot be closed and re-submitted from inside.
        let res = app
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                // real ESC 0x1b via JSON \u001b
                "{\"text\":\"hi\\u001b[201~\"}",
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["bytes"], 2);

        let wait_len = PASTE_START.len() + 2 + PASTE_END.len() + 1;
        let received = read_slave_available(pty.slave, wait_len, Duration::from_secs(2));
        assert_eq!(received, [PASTE_START, b"hi", PASTE_END, b"\r"].concat());
    }

    #[tokio::test]
    async fn test_inject_rate_limited_429() {
        let st = seeded_registry_state();
        let pty = crate::orchestrate::pty::Pty::open().expect("pty");
        seed_pty_input(&st.controls.get("sess-a").unwrap(), &pty);
        // Pre-fill the limiter to the ceiling for this session's REST sender.
        {
            let mut lim = st.message_limits.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            for _ in 0..MESSAGE_RATE_MAX {
                lim.record("sess-a", REST_SENDER, now);
            }
        }
        let app = router(st.clone());
        let res = app
            .oneshot(message_request(
                "/api/v1/sessions/sess-a/message",
                r#"{"text":"one too many"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            res.headers().get(axum::http::header::RETRY_AFTER).is_some(),
            "429 must carry Retry-After"
        );
    }
}

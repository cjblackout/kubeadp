//! A2A backend and router integration for `agent-orchestrate`.
//!
//! Exposes Claude Code sessions managed by the supervisor as A2A tasks:
//! - Task ID = session name
//! - Context ID = session name
//! - State mapping:
//!   - `Running` -> `Working`
//!   - `Starting` -> `Working`
//!   - `Backoff` -> `Working`
//!   - `Stopped` -> `Completed` (or `Canceled` if paused/controlled)
//!   - `Failed` -> `Failed`
//! - `list_tasks` lists sessions (matching optional context_id and state filters)
//! - `get_task` returns the session as an `A2aTask`
//! - `cancel_task` stops the session (reusing the stop path)

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::core::a2a::{
    advertised_url, pod_name, role_description, role_skills, A2AError, A2aModel, A2aTask,
    AuthVerifier, TaskBackend, ROLE_ORCHESTRATOR,
};
use crate::orchestrate::sessions_api;
use a2a::TaskState;
use axum::Router;

use crate::orchestrate::session_control::{set_paused, SessionControlRegistry};
use crate::orchestrate::state::{SessionState, SessionStatus, SupervisorState};

/// Backend implementing [`TaskBackend`] over supervisor sessions.
pub struct SupervisorTaskBackend {
    supervisor_state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
}

impl SupervisorTaskBackend {
    pub fn new(
        supervisor_state: Arc<std::sync::Mutex<SupervisorState>>,
        controls: SessionControlRegistry,
    ) -> Self {
        Self {
            supervisor_state,
            controls,
        }
    }

    fn session_to_a2a_task(&self, sess: &SessionState) -> A2aTask {
        let control = self.controls.get(&sess.name);
        let paused = control
            .as_ref()
            .is_some_and(|c| c.paused.load(Ordering::SeqCst));

        let state = map_session_status(sess.status, paused);
        let status_message = sess
            .last_health_result
            .clone()
            .or_else(|| sess.last_exit_reason.clone());
        let error = if sess.status == SessionStatus::Failed {
            sess.last_exit_reason.clone()
        } else {
            None
        };

        A2aTask {
            id: sess.name.clone(),
            context_id: Some(sess.name.clone()),
            state,
            status_message,
            error,
            artifacts: None,
        }
    }
}

/// Maps a [`SessionStatus`] and paused flag to A2A [`TaskState`].
///
/// - `Starting`, `Running`, `Backoff` -> `TaskState::Working`
/// - `Stopped`: if paused, `TaskState::Canceled`, else `TaskState::Completed`
/// - `Failed` -> `TaskState::Failed`
pub fn map_session_status(status: SessionStatus, paused: bool) -> TaskState {
    match status {
        SessionStatus::Starting | SessionStatus::Running | SessionStatus::Backoff => {
            TaskState::Working
        }
        SessionStatus::Stopped => {
            if paused {
                TaskState::Canceled
            } else {
                TaskState::Completed
            }
        }
        SessionStatus::Failed => TaskState::Failed,
    }
}

impl TaskBackend for SupervisorTaskBackend {
    fn get_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        let st = self
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let sess = st
            .sessions
            .get(task_id)
            .ok_or_else(|| A2AError::task_not_found(task_id))?;
        Ok(self.session_to_a2a_task(sess))
    }

    fn list_tasks(
        &self,
        context_id: Option<&str>,
        state: Option<&TaskState>,
    ) -> Result<Vec<A2aTask>, A2AError> {
        let st = self
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut tasks = Vec::with_capacity(st.sessions.len());
        for sess in st.sessions.values() {
            let task = self.session_to_a2a_task(sess);
            if let Some(expected_ctx) = context_id {
                if task.context_id.as_deref() != Some(expected_ctx) {
                    continue;
                }
            }
            if let Some(expected_state) = state {
                if &task.state != expected_state {
                    continue;
                }
            }
            tasks.push(task);
        }
        // Deterministic ordering by ID/name
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(tasks)
    }

    fn cancel_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        // Reuse the stop path from sessions_api::stop_session
        let control = self
            .controls
            .get(task_id)
            .ok_or_else(|| A2AError::task_not_found(task_id))?;

        // Pause the supervision thread
        set_paused(&control, true);

        // Update session status to Stopped if not already Failed
        let mut st = self
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let sess = st
            .sessions
            .get_mut(task_id)
            .ok_or_else(|| A2AError::task_not_found(task_id))?;

        if sess.status != SessionStatus::Failed {
            sess.status = SessionStatus::Stopped;
        }

        Ok(self.session_to_a2a_task(sess))
    }
}

/// A2A `SendMessage` sink that delivers text into a supervised session via
/// the shared [`crate::orchestrate::sessions_api::deliver_message`] core, so
/// the A2A path and the REST `POST /api/v1/sessions/{name}/message` path
/// share the same session lookup, bracketed-paste writing and per-session
/// rate budget.
struct SessionMessageSink {
    state: Arc<sessions_api::SessionsApiState>,
}

impl SessionMessageSink {
    fn new(state: Arc<sessions_api::SessionsApiState>) -> Self {
        Self { state }
    }
}

impl crate::core::a2a::MessageSink for SessionMessageSink {
    fn deliver(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        _message_id: &str,
    ) -> Result<(), a2a::A2AError> {
        use a2a::A2AError;
        // Runs on the tokio blocking pool (the handler wraps the call in
        // spawn_blocking), so the deadline-bounded PTY write never occupies a
        // worker thread. Shares the same state/rate limiter as the REST path.
        // The session path deduplicates on (session, sender) budget, not on the
        // A2A message id, so `message_id` is not used here.
        match sessions_api::deliver_message(&self.state, task_id, sender, text) {
            sessions_api::DeliverOutcome::Accepted { .. } => Ok(()),
            sessions_api::DeliverOutcome::NotFound => Err(A2AError::task_not_found(task_id)),
            sessions_api::DeliverOutcome::Empty => {
                Err(A2AError::invalid_params("message text must not be empty"))
            }
            sessions_api::DeliverOutcome::TooLarge => {
                Err(A2AError::invalid_params("message text is too large"))
            }
            sessions_api::DeliverOutcome::RateLimited { .. } => {
                Err(A2AError::invalid_params("session message budget exhausted"))
            }
            sessions_api::DeliverOutcome::NotRunning => Err(A2AError::invalid_params(
                "session is not running and has no live input",
            )),
        }
    }
}

/// Builds the A2A router for agent-orchestrate.
///
/// `api_state` is the SAME [`sessions_api::SessionsApiState`] the REST
/// `/api/v1/sessions` router uses: the A2A `SendMessage` sink and the REST
/// `POST .../message` handler therefore share ONE rate limiter (the claimed
/// "shared budget" is real), rather than the A2A router building a second one.
pub fn a2a_router(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    api_state: Arc<sessions_api::SessionsApiState>,
    auth: Option<AuthVerifier>,
    jobset: Option<crate::core::a2a::JobsetCred>,
) -> Router {
    // The agent card identifies the RUNNING POD (not the retired
    // agent-orchestrate image): the name is the pod's own name, the
    // description and skills name the orchestrator role, the version is the
    // crate version, and the advertised URL is the cluster Service DNS
    // (KUBEADP_A2A_URL) when set, else the configured listen address.
    let listen = crate::orchestrate::http_server::listen_addr_from_env();
    let model = A2aModel {
        name: pod_name(ROLE_ORCHESTRATOR),
        description: role_description(ROLE_ORCHESTRATOR).to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        skills: role_skills(ROLE_ORCHESTRATOR),
        url: advertised_url(&format!("http://{listen}")),
    };

    let sink = Arc::new(SessionMessageSink::new(api_state));
    let backend = Arc::new(SupervisorTaskBackend::new(state, controls));
    crate::core::a2a::a2a_router_with_jobset(model, backend, Some(sink), auth, jobset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrate::session_control::SessionControl;

    fn make_test_backend() -> (
        Arc<SupervisorTaskBackend>,
        Arc<std::sync::Mutex<SupervisorState>>,
        SessionControlRegistry,
    ) {
        let mut state = SupervisorState::new(100);
        let mut s1 = SessionState::new("sess-1".to_string());
        s1.status = SessionStatus::Running;
        state.sessions.insert("sess-1".to_string(), s1);

        let mut s2 = SessionState::new("sess-2".to_string());
        s2.status = SessionStatus::Failed;
        s2.last_exit_reason = Some("crashed".to_string());
        state.sessions.insert("sess-2".to_string(), s2);

        let mut s3 = SessionState::new("sess-3".to_string());
        s3.status = SessionStatus::Stopped;
        state.sessions.insert("sess-3".to_string(), s3);

        let st = Arc::new(std::sync::Mutex::new(state));
        let controls = SessionControlRegistry::new();
        controls.insert(Arc::new(SessionControl::new(
            crate::orchestrate::config::SessionConfig {
                name: "sess-1".to_string(),
                workdir: std::path::PathBuf::from("/tmp"),
                session_id: "s1".to_string(),
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

        let backend = Arc::new(SupervisorTaskBackend::new(st.clone(), controls.clone()));
        (backend, st, controls)
    }

    #[test]
    fn test_map_session_status() {
        assert_eq!(
            map_session_status(SessionStatus::Starting, false),
            TaskState::Working
        );
        assert_eq!(
            map_session_status(SessionStatus::Running, false),
            TaskState::Working
        );
        assert_eq!(
            map_session_status(SessionStatus::Backoff, false),
            TaskState::Working
        );
        assert_eq!(
            map_session_status(SessionStatus::Stopped, false),
            TaskState::Completed
        );
        assert_eq!(
            map_session_status(SessionStatus::Stopped, true),
            TaskState::Canceled
        );
        assert_eq!(
            map_session_status(SessionStatus::Failed, false),
            TaskState::Failed
        );
        assert_eq!(
            map_session_status(SessionStatus::Failed, true),
            TaskState::Failed
        );
    }

    #[test]
    fn test_get_task_success_and_unknown() {
        let (backend, _, _) = make_test_backend();

        let t1 = backend.get_task("sess-1").expect("sess-1 exists");
        assert_eq!(t1.id, "sess-1");
        assert_eq!(t1.context_id.as_deref(), Some("sess-1"));
        assert_eq!(t1.state, TaskState::Working);

        let t2 = backend.get_task("sess-2").expect("sess-2 exists");
        assert_eq!(t2.id, "sess-2");
        assert_eq!(t2.state, TaskState::Failed);
        assert_eq!(t2.error.as_deref(), Some("crashed"));

        let err = backend.get_task("nonexistent").unwrap_err();
        assert_eq!(err.code, "taskNotFound");
    }

    #[test]
    fn test_list_tasks_and_filtering() {
        let (backend, _, _) = make_test_backend();

        let all = backend.list_tasks(None, None).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "sess-1");
        assert_eq!(all[1].id, "sess-2");
        assert_eq!(all[2].id, "sess-3");

        let filtered_ctx = backend.list_tasks(Some("sess-1"), None).unwrap();
        assert_eq!(filtered_ctx.len(), 1);
        assert_eq!(filtered_ctx[0].id, "sess-1");

        let filtered_state = backend.list_tasks(None, Some(&TaskState::Working)).unwrap();
        assert_eq!(filtered_state.len(), 1);
        assert_eq!(filtered_state[0].id, "sess-1");

        let filtered_failed = backend.list_tasks(None, Some(&TaskState::Failed)).unwrap();
        assert_eq!(filtered_failed.len(), 1);
        assert_eq!(filtered_failed[0].id, "sess-2");
    }

    #[test]
    fn test_cancel_task_success_and_unknown() {
        let (backend, st, controls) = make_test_backend();

        // Unknown session
        let err = backend.cancel_task("unknown").unwrap_err();
        assert_eq!(err.code, "taskNotFound");

        // Cancel sess-1
        let cancelled = backend.cancel_task("sess-1").unwrap();
        assert_eq!(cancelled.id, "sess-1");
        assert_eq!(cancelled.state, TaskState::Canceled);

        // Verify control paused flag set
        let c = controls.get("sess-1").unwrap();
        assert!(c.paused.load(Ordering::SeqCst));

        // Verify state is Stopped
        let guard = st.lock().unwrap();
        assert_eq!(
            guard.sessions.get("sess-1").unwrap().status,
            SessionStatus::Stopped
        );
    }

    #[tokio::test]
    async fn test_http_a2a_card_and_tasks() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let (backend, _, _) = make_test_backend();
        let api_state = sessions_api::SessionsApiState::new(
            backend.supervisor_state.clone(),
            backend.controls.clone(),
            Arc::new(crate::orchestrate::session_log::SessionLogs::from_env()),
        );
        // The card name is derived from the environment (the pod name); pin it
        // deterministically for the card assertion (the model is built eagerly,
        // so the guard only needs to be held while the router is built).
        let app = {
            let _env = crate::core::a2a::testenv::Guard::set(&[
                ("KUBEADP_A2A_NAME", None),
                ("A2A_SELF_NAME", Some("kubeadp-agent-orchestrator-test-1")),
                ("POD_NAME", None),
                ("HOSTNAME", None),
                ("KUBEADP_A2A_URL", None),
            ]);
            a2a_router(
                backend.supervisor_state.clone(),
                backend.controls.clone(),
                api_state,
                None,
                None,
            )
        };

        // Public agent card at /.well-known/agent-card.json
        let req = Request::builder()
            .uri(crate::core::a2a::WELL_KNOWN_AGENT_CARD_PATH)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let card: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(card["name"], "kubeadp-agent-orchestrator-test-1");
        // The orchestrator card names the role and advertises the skill.
        assert!(card["description"]
            .as_str()
            .unwrap()
            .contains("orchestrator"));
        assert_eq!(card["skills"][0]["id"], "session-message");

        // REST GET /tasks
        let req = Request::builder()
            .uri("/tasks")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let tasks_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(tasks_val["tasks"].as_array().unwrap().len(), 3);

        // REST GET /tasks/sess-1
        let req = Request::builder()
            .uri("/tasks/sess-1")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let task_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(task_val["id"], "sess-1");
        assert_eq!(task_val["status"]["state"], "TASK_STATE_WORKING");

        // REST POST /tasks/sess-1:cancel
        let req = Request::builder()
            .method("POST")
            .uri("/tasks/sess-1:cancel")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let cancel_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(cancel_val["status"]["state"], "TASK_STATE_CANCELED");
    }

    #[tokio::test]
    async fn test_http_a2a_auth_enforced() {
        use axum::body::Body;
        use axum::http::{HeaderMap, Request, StatusCode};
        use tower::ServiceExt;

        let (backend, _, _) = make_test_backend();
        let auth_verifier: AuthVerifier = Arc::new(|headers: &HeaderMap| {
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v == "Bearer test-secret")
        });
        let api_state = sessions_api::SessionsApiState::new(
            backend.supervisor_state.clone(),
            backend.controls.clone(),
            Arc::new(crate::orchestrate::session_log::SessionLogs::from_env()),
        );
        let app = a2a_router(
            backend.supervisor_state.clone(),
            backend.controls.clone(),
            api_state,
            Some(auth_verifier),
            None,
        );

        // Agent card is public even with auth configured
        let req = Request::builder()
            .uri(crate::core::a2a::WELL_KNOWN_AGENT_CARD_PATH)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /tasks without token -> HTTP 401 UNAUTHENTICATED (the A2A
        // problem+json envelope) with the WWW-Authenticate challenge required
        // by the A2A HTTP+JSON binding.
        let req = Request::builder()
            .uri("/tasks")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok()),
            Some("Bearer")
        );

        // POST /message:send without token -> 401, even for a body that would
        // otherwise be invalid (auth runs before body validation).
        let req = Request::builder()
            .method("POST")
            .uri("/message:send")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // GET /tasks with correct token -> 200
        let req = Request::builder()
            .uri("/tasks")
            .header("authorization", "Bearer test-secret")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_a2a_router_shares_rest_rate_limiter() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let (_, st, controls) = make_test_backend();
        // The SAME SessionsApiState the REST /api/v1/sessions router would
        // use: the A2A router must build no second rate limiter of its own.
        let logs = Arc::new(crate::orchestrate::session_log::SessionLogs::from_env());
        let api_state = sessions_api::SessionsApiState::new(st.clone(), controls.clone(), logs);

        // Give sess-1 a live PTY so delivery actually succeeds (not NotRunning).
        let pty = crate::orchestrate::pty::Pty::open().expect("pty");
        controls
            .get("sess-1")
            .unwrap()
            .pty_input
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(pty.master);

        let app = a2a_router(st.clone(), controls.clone(), api_state.clone(), None, None);

        let send = |id: u32| -> String {
            format!(
                r#"{{"message":{{"messageId":"m{id}","taskId":"sess-1","role":"ROLE_USER","parts":[{{"text":"hello"}}],"metadata":{{"a2a.sender":"noisy","a2a.hop":1}}}}}}"#
            )
        };
        let send_req = |app: Router, body: String| async move {
            let req = Request::builder()
                .method("POST")
                .uri("/message:send")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            app.oneshot(req).await.unwrap()
        };

        // First message from "noisy" is accepted...
        let res = send_req(app.clone(), send(1)).await;
        assert_eq!(res.status(), StatusCode::OK);
        // ...and it charged the SHARED limiter (the api_state the REST router
        // also uses) under the claimed sender -- not a second A2A-only one.
        {
            let lim = api_state.message_limits.lock().unwrap();
            assert_eq!(
                lim.uses("sess-1", "noisy"),
                1,
                "A2A send must land in the shared per-(session, sender) budget"
            );
        }

        // Exhaust the shared (sess-1, "noisy") budget: the very same limiter
        // the REST handler uses must now reject the A2A path.
        {
            let mut lim = api_state.message_limits.lock().unwrap();
            let now = std::time::Instant::now();
            while lim.uses("sess-1", "noisy") < sessions_api::MESSAGE_RATE_MAX {
                lim.record("sess-1", "noisy", now);
            }
        }
        let res2 = send_req(app.clone(), send(2)).await;
        assert_eq!(res2.status(), StatusCode::BAD_REQUEST);
        let bytes = res2.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);
        assert!(
            body_str.contains("budget exhausted"),
            "error body: {body_str}"
        );
    }
}

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

use crate::core::a2a::{A2AError, A2aModel, A2aTask, AuthVerifier, TaskBackend};
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

/// Builds the A2A router for agent-orchestrate.
pub fn a2a_router(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    auth: Option<AuthVerifier>,
) -> Router {
    let session_names = {
        let st = state.lock().unwrap_or_else(|e| e.into_inner());
        let mut names: Vec<String> = st.sessions.keys().cloned().collect();
        names.sort();
        names
    };

    let model = A2aModel {
        name: "agent-orchestrate".to_string(),
        description: "Process supervisor for Claude Code agent sessions".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        skills: session_names,
        json_rpc_url: "/rpc".to_string(),
    };

    let backend = Arc::new(SupervisorTaskBackend::new(state, controls));
    crate::core::a2a::a2a_router(model, backend, auth)
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
        let app = a2a_router(
            backend.supervisor_state.clone(),
            backend.controls.clone(),
            None,
        );

        // Public agent card at /.well-known/agent-card.json
        let req = Request::builder()
            .uri(crate::core::a2a::WELL_KNOWN_AGENT_CARD_PATH)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let card: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(card["name"], "agent-orchestrate");

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

        // JSON-RPC GetTask
        let req = Request::builder()
            .method("POST")
            .uri("/rpc")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"sess-1"}}"#,
            ))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let rpc_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rpc_val["result"]["id"], "sess-1");

        // JSON-RPC CancelTask
        let req = Request::builder()
            .method("POST")
            .uri("/rpc")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"jsonrpc":"2.0","id":2,"method":"CancelTask","params":{"id":"sess-1"}}"#,
            ))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let rpc_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rpc_val["result"]["status"]["state"], "TASK_STATE_CANCELED");
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
        let app = a2a_router(
            backend.supervisor_state.clone(),
            backend.controls.clone(),
            Some(auth_verifier),
        );

        // Agent card is public even with auth configured
        let req = Request::builder()
            .uri(crate::core::a2a::WELL_KNOWN_AGENT_CARD_PATH)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /tasks without token -> 401
        let req = Request::builder()
            .uri("/tasks")
            .body(Body::empty())
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
}

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
//!   - an inbound message queued in the session's inbox -> `Submitted`
//!     (the sender's task state while the message waits for a boundary;
//!     design HL-113 §7.14)
//! - `list_tasks`/`get_task`/`cancel_task`: list, lookup, stop the session.
//!
//! Inbound delivery is the escalation ladder of design §7.14 (phase 2,
//! [`crate::core::a2a::delivery`]): an accepted message is enqueued into the
//! session's durable inbox (queued -> sender task `submitted`), the hook
//! channel (the session's Claude Code hook settings,
//! [`crate::orchestrate::claude_config::ensure_a2a_hooks`]) injects it at the
//! next tool call / prompt / turn boundary, and the ladder escalates to the
//! `idle_turn` step (the queued PTY input) after `a2a_ack_secs` with the
//! session running and to the `interrupt` step immediately for
//! `interrupt: true` or past `a2a_force_secs`. Default: never kill (nothing
//! is killed on the session path; a PTY write queues the input).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::core::a2a::delivery::{sender_task_state, LadderConfig, LadderStep, SenderTaskState};
use crate::core::a2a::inbox::{Inbox, QueuedMessage};
use crate::core::a2a::inject::join_framed;
use crate::core::a2a::DeliveryMeta;
use crate::core::a2a::{
    advertised_url, pod_name, role_description, role_skills, A2AError, A2aModel, A2aTask,
    AuthVerifier, TaskBackend, ROLE_ORCHESTRATOR,
};
use crate::engine::capability::InjectChannel;
use crate::orchestrate::sessions_api;
use a2a::TaskState;
use axum::Router;

use crate::orchestrate::session_control::{set_paused, SessionControlRegistry};
use crate::orchestrate::state::{SessionState, SessionStatus, SupervisorState};

/// How often the escalation-ladder watcher re-evaluates a session's inbox.
const LADDER_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// Backend implementing [`TaskBackend`] over supervisor sessions.
pub struct SupervisorTaskBackend {
    supervisor_state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    /// Root under which each session's durable inbox lives (the sender's
    /// task-state mapping: queued messages mean `submitted`, an expiry
    /// marker fails the task). Shared with the message sink.
    a2a_inbox_root: PathBuf,
}

impl SupervisorTaskBackend {
    pub fn new(
        supervisor_state: Arc<std::sync::Mutex<SupervisorState>>,
        controls: SessionControlRegistry,
        a2a_inbox_root: PathBuf,
    ) -> Self {
        Self {
            supervisor_state,
            controls,
            a2a_inbox_root,
        }
    }

    /// The session's durable inbox directory.
    fn inbox_dir(&self, session: &str) -> PathBuf {
        crate::core::a2a::inbox::inbox_dir_for(&self.a2a_inbox_root, session)
    }

    fn session_to_a2a_task(&self, sess: &SessionState) -> A2aTask {
        let control = self.controls.get(&sess.name);
        let paused = control
            .as_ref()
            .is_some_and(|c| c.paused.load(Ordering::SeqCst));

        let mut state = map_session_status(sess.status, paused);
        let status_message = sess
            .last_health_result
            .clone()
            .or_else(|| sess.last_exit_reason.clone());
        let mut error = if sess.status == SessionStatus::Failed {
            sess.last_exit_reason.clone()
        } else {
            None
        };

        // The escalation ladder (design HL-113 §7.14, phase 2): while an
        // inbound message is queued for the session the sender's task is
        // `submitted`; once injected the normal mapping applies; a durable
        // expiry marker fails the task with its clear reason.
        {
            let inbox = Inbox::new(self.inbox_dir(&sess.name));
            let expiry = crate::core::a2a::delivery::read_expiry(&self.inbox_dir(&sess.name));
            match sender_task_state(inbox.pending_count(), expiry.as_ref()) {
                SenderTaskState::Submitted => state = TaskState::Submitted,
                SenderTaskState::Failed { reason } => {
                    state = TaskState::Failed;
                    error = Some(reason);
                }
                SenderTaskState::Working => {}
            }
        }

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

/// A2A `SendMessage` sink: the message is enqueued into the session's
/// durable inbox and delivered by the escalation ladder (design §7.14,
/// phase 2). The hook settings inject at the next boundary; the watcher
/// escalates to the queued PTY input past `a2a_ack_secs` with the session
/// running, or immediately for `interrupt: true` / past `a2a_force_secs`.
/// Nothing is killed.
struct SessionMessageSink {
    state: Arc<sessions_api::SessionsApiState>,
    /// Root under which each session's durable inbox lives.
    inbox_root: PathBuf,
    /// The escalation-ladder configuration.
    ladder: LadderConfig,
    /// The escalation-ladder watchers (one per session).
    ladders: Mutex<HashMap<String, Arc<()>>>,
}

impl SessionMessageSink {
    fn new(
        state: Arc<sessions_api::SessionsApiState>,
        inbox_root: PathBuf,
        ladder: LadderConfig,
    ) -> Self {
        Self {
            state,
            inbox_root,
            ladder,
            ladders: Mutex::new(HashMap::new()),
        }
    }

    /// The session's durable inbox directory.
    fn inbox_dir(&self, session: &str) -> PathBuf {
        crate::core::a2a::inbox::inbox_dir_for(&self.inbox_root, session)
    }

    /// One ladder pass for the session: computes the step for the oldest
    /// pending message and applies it (the queued PTY input). A free function
    /// over the shared state pieces so the watcher thread needs no handle on
    /// the sink.
    fn ladder_pass(
        state: &sessions_api::SessionsApiState,
        inbox_root: &std::path::Path,
        ladder: &LadderConfig,
        session: &str,
        interrupt: bool,
    ) -> LadderStep {
        let inbox_dir = crate::core::a2a::inbox::inbox_dir_for(inbox_root, session);
        // An already-expired message is never delivered (the marker decides).
        if let Some(record) = crate::core::a2a::delivery::read_expiry(&inbox_dir) {
            return LadderStep::Expired {
                reason: record.reason,
            };
        }
        // The inbox is read WITHOUT its own TTL filter: the ladder applies
        // the TTL itself so the expiry marker is recorded first.
        let inbox = Inbox::with_ttl(&inbox_dir, u64::MAX);
        let pending = inbox.pending();
        if pending.is_empty() {
            return LadderStep::Injected;
        }
        let running = {
            let st = state
                .supervisor_state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            st.sessions
                .get(session)
                .is_some_and(|s| s.status == SessionStatus::Running)
        };
        let step =
            crate::core::a2a::delivery::next_step(&crate::core::a2a::delivery::LadderInput {
                now: crate::core::a2a::inbox::now_unix(),
                queued_at: pending[0].queued_at,
                interrupt,
                idle_turn_available: running,
                expired: false,
                config: ladder,
            });
        match step {
            LadderStep::Expired { reason } => {
                crate::core::a2a::delivery::record_expiry(
                    &inbox_dir,
                    &pending[0].message_id,
                    &reason,
                );
                LadderStep::Expired { reason }
            }
            LadderStep::IdleTurn | LadderStep::Interrupt => {
                // The queued PTY input (design §7.14: "queued PTY input or
                // the Stop hook"). A bracketed paste queues the message as
                // the next user input; nothing is killed. NotRunning / a
                // throttled budget keeps the message queued.
                let joined = join_framed(&pending);
                let channel = match step {
                    LadderStep::Interrupt => InjectChannel::Interrupt,
                    _ => InjectChannel::IdleTurn,
                };
                if let sessions_api::DeliverOutcome::Accepted { .. } =
                    sessions_api::deliver_message(state, session, &pending[0].sender, &joined)
                {
                    inbox.drain(channel.as_str());
                }
                step
            }
            other => other,
        }
    }

    /// Ensures the ladder watcher is running for the session (one per
    /// session; the watcher exits when the inbox drains or expires).
    fn ensure_ladder(&self, session: &str) {
        let mut ladders = self.ladders.lock().expect("ladders lock poisoned");
        ladders.retain(|_, guard| Arc::strong_count(guard) > 1);
        if ladders.contains_key(session) {
            return;
        }
        let guard = Arc::new(());
        ladders.insert(session.to_string(), Arc::clone(&guard));
        drop(ladders);
        let state = Arc::clone(&self.state);
        let inbox_root = self.inbox_root.clone();
        let ladder = self.ladder;
        let name = session.to_string();
        let _ = std::thread::Builder::new()
            .name(format!("a2a-ladder-{name}"))
            .spawn(move || {
                loop {
                    let step =
                        Self::ladder_pass(state.as_ref(), &inbox_root, &ladder, &name, false);
                    match step {
                        LadderStep::Injected | LadderStep::Expired { .. } => break,
                        _ => std::thread::sleep(LADDER_POLL),
                    }
                }
                drop(guard);
            });
    }
}

impl crate::core::a2a::MessageSink for SessionMessageSink {
    fn deliver_with_meta(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
        meta: DeliveryMeta,
    ) -> Result<(), a2a::A2AError> {
        use a2a::A2AError;
        // Runs on the tokio blocking pool (the handler wraps the call in
        // spawn_blocking). The session path deduplicates on (session, sender)
        // budget in the PTY-write escalation, not on the A2A message id; the
        // durable inbox is the dedupe store.
        {
            let st = self
                .state
                .supervisor_state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !st.sessions.contains_key(task_id) {
                return Err(A2AError::task_not_found(task_id));
            }
        }

        // The shared per-(session, sender) message budget (the SAME limiter
        // the REST POST .../message path uses) is charged at the acceptance
        // point: a throttled delivery is rejected and the sender retries.
        {
            let mut limits = self
                .state
                .message_limits
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(_retry_after) = limits.record(task_id, sender, std::time::Instant::now()) {
                return Err(A2AError::invalid_params("session message budget exhausted"));
            }
        }

        // Enqueue into the session's durable inbox. A fresh delivery
        // supersedes an expiry recorded for an earlier message.
        let inbox_dir = self.inbox_dir(task_id);
        crate::core::a2a::delivery::clear_expiry(&inbox_dir);
        Inbox::new(&inbox_dir)
            .enqueue(QueuedMessage {
                message_id: message_id.to_string(),
                sender: sender.to_string(),
                hop: meta.hop,
                context_id: None,
                task_id: None,
                framed: text.to_string(),
                queued_at: crate::core::a2a::inbox::now_unix(),
            })
            .map(|_| ())
            .map_err(|e| {
                A2AError::internal(format!(
                    "failed to enqueue the A2A message into the session inbox: {e}"
                ))
            })?;

        // The explicit escalation (interrupt: true): the PTY input now.
        if meta.interrupt {
            let inbox = Inbox::with_ttl(&inbox_dir, u64::MAX);
            let joined = join_framed(&inbox.pending());
            let _ = sessions_api::deliver_message(&self.state, task_id, sender, &joined);
            inbox.drain(InjectChannel::Interrupt.as_str());
            return Ok(());
        }

        self.ensure_ladder(task_id);
        Ok(())
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
    a2a_inbox_root: PathBuf,
    ladder: LadderConfig,
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

    let sink = Arc::new(SessionMessageSink::new(
        api_state,
        a2a_inbox_root.clone(),
        ladder,
    ));
    let backend = Arc::new(SupervisorTaskBackend::new(state, controls, a2a_inbox_root));
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
                engine: None,
                claude_binary: std::path::PathBuf::from("/bin/true"),
                claude_args: vec![],
                agy_binary: std::path::PathBuf::from("agy"),
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

        let backend = Arc::new(SupervisorTaskBackend::new(
            st.clone(),
            controls.clone(),
            std::path::PathBuf::from("/tmp/test-a2a-inbox-root-0"),
        ));
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
                std::path::PathBuf::from("/tmp/test-a2a-inbox-root-1"),
                Default::default(),
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
            std::path::PathBuf::from("/tmp/test-a2a-inbox-root-2"),
            Default::default(),
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

        let app = a2a_router(
            st.clone(),
            controls.clone(),
            api_state.clone(),
            None,
            None,
            std::path::PathBuf::from("/tmp/test-a2a-inbox-root-3"),
            Default::default(),
        );

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

    /// Test 2 (HL-113 path-mismatch fix): end-to-end through the real sink
    /// backend to the session hook reader. The sink (`SessionMessageSink`,
    /// the same backend `http_server.rs` wires to the A2A router) and the
    /// session launcher resolve the inbox from the SAME shared root
    /// (`inbox_state_root()`, the agent-run `state_root` -- the session
    /// launcher's `KUBEADP_A2A_INBOX_DIR` now names this exact directory, not
    /// `state_file.parent()`). Enqueue through the sink's backend, then run
    /// the `a2a-hook` reader pointed at the session's
    /// `KUBEADP_A2A_INBOX_DIR`: it must see the message (and mark it
    /// injected), proving a delivered A2A message reaches the session's
    /// receive side. Runs under a non-default `state_file` too: the sink and
    /// the session agree on a directory under the agent-run state root, never
    /// under the state file's parent.
    #[test]
    fn sink_backend_to_session_hook_sees_the_message() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("state-root");
        // A non-default agent-run state root (the orchestrator pod sets
        // AGENT_RUN_STATE_ROOT or relies on the default ~/.agent-run; here
        // the test forces a fresh root so the assertion is hermetic). The
        // default HOME could otherwise point at a real ~/.agent-run.
        let root_str = root.to_str().unwrap().to_string();
        let (inbox, inbox_dir) = crate::core::a2a::testenv::with_env(
            &[("AGENT_RUN_STATE_ROOT", Some(&root_str))],
            || {
                // A supervisor config whose state file lives somewhere
                // unrelated to the inbox root (the pre-fix session side would
                // have derived the inbox from this parent).
                let supervisor_config = crate::orchestrate::config::SupervisorConfig {
                    state_file: tmp.path().join("elsewhere").join("state.json"),
                    ..Default::default()
                };
                let session = "homelab";
                // The ONE shared root: the sink's `a2a_router` input and the
                // session launcher's `supervisor_config.a2a_inbox_root()` agree
                // by construction (both route through inbox_state_root();
                // asserted in the inbox.rs unit test too). The session's
                // launcher names KUBEADP_A2A_INBOX_DIR with this exact
                // directory.
                assert_eq!(
                    supervisor_config.a2a_inbox_root(),
                    crate::core::a2a::inbox::inbox_state_root()
                );
                let inbox_root = supervisor_config.a2a_inbox_root();
                let inbox_dir = crate::core::a2a::inbox::inbox_dir_for(&inbox_root, session);

                // A running session so the sink accepts the delivery.
                let mut state = SupervisorState::new(100);
                let mut s = SessionState::new(session.to_string());
                s.status = SessionStatus::Running;
                state.sessions.insert(session.to_string(), s);
                let st = Arc::new(std::sync::Mutex::new(state));
                let controls = SessionControlRegistry::new();
                let api_state = sessions_api::SessionsApiState::new(
                    st,
                    controls,
                    Arc::new(crate::orchestrate::session_log::SessionLogs::from_env()),
                );

                // The sink backend, rooted at the SHARED inbox root (exactly
                // what a2a_router builds from http_server's
                // inbox_state_root()): it derives each session's inbox as
                // <root>/a2a-inbox/<session>.
                let sink =
                    SessionMessageSink::new(api_state, inbox_root.clone(), Default::default());
                crate::core::a2a::MessageSink::deliver(
                    &sink,
                    session,
                    "peer-agent",
                    "hello from the sink",
                    "msg-1",
                )
                .expect("sink accepts the delivery");

                // The message is queued in the session's inbox directory (the
                // one the sink writes, under the agent-run state root).
                let inbox = Inbox::new(&inbox_dir);
                let pending = inbox.pending();
                assert_eq!(pending.len(), 1, "exactly one queued message");
                assert!(pending[0].framed.contains("hello from the sink"));
                assert!(
                    inbox_dir.starts_with(&root),
                    "the session inbox must live under the shared agent-run state root"
                );
                assert_ne!(
                    inbox_dir,
                    crate::core::a2a::inbox::inbox_dir_for(
                        supervisor_config
                            .state_file
                            .parent()
                            .unwrap_or(std::path::Path::new("/tmp")),
                        session
                    ),
                    "the session inbox must not be derived from the state file's parent"
                );

                (inbox, inbox_dir)
            },
        );

        // The hook/inbox reader pointed at the session's KUBEADP_A2A_INBOX_DIR
        // (the env the session launcher sets on the final command): it sees
        // the message the sink wrote and marks it injected. Runs under its own
        // fresh env guard (the shared testenv lock is not re-entrant), with
        // only the inbox var set.
        let inbox_dir_str = inbox_dir.to_str().unwrap().to_string();
        let hook = crate::core::a2a::testenv::with_env(
            &[(crate::core::a2a::inbox::INBOX_ENV, Some(&inbox_dir_str))],
            || crate::core::a2a::inject::run_a2a_hook(&["PostToolUse".to_string()]),
        );
        assert_eq!(hook, 0, "the hook reader runs clean");
        assert!(inbox.injected("msg-1"), "the hook acknowledged the message");
        assert!(inbox.pending().is_empty(), "nothing stays queued");
    }
}

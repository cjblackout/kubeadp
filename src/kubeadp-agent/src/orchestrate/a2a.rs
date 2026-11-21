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

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::core::a2a::delivery::{sender_task_state, LadderConfig, LadderStep, SenderTaskState};
use crate::core::a2a::inbox::{Inbox, QueuedMessage};
use crate::core::a2a::inject::join_framed;
#[cfg(test)]
use crate::core::a2a::AuthVerifier;
use crate::core::a2a::DeliveryMeta;
use crate::core::a2a::{
    advertised_url, pod_name, role_description, role_skills, A2AError, A2aModel, A2aTask,
    TaskBackend, ROLE_ORCHESTRATOR,
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
    ladders: crate::core::a2a::delivery::LadderWatchers,
    /// Sessions with a queued `interrupt: true` message whose immediate PTY
    /// write could not complete (e.g. session was in `Starting` or `Backoff`).
    /// Retained until the watcher injects the batch or the inbox drains.
    interrupt_pending: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
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
            ladders: crate::core::a2a::delivery::LadderWatchers::new(),
            interrupt_pending: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Restart recovery: the ladder watchers are in-memory threads, so the
    /// messages that were queued when the previous process ended have no
    /// watcher. Starts one for every run that still holds an uninjected
    /// message and prunes the acknowledged rows past their retention.
    /// Called once when the router is built.
    fn recover_pending(&self) {
        crate::core::a2a::inbox::sweep_retention(&self.inbox_root);
        for run in crate::core::a2a::inbox::runs_with_uninjected(&self.inbox_root) {
            self.ensure_ladder(&run);
        }
    }

    /// The session's durable inbox directory.
    fn inbox_dir(&self, session: &str) -> PathBuf {
        crate::core::a2a::inbox::inbox_dir_for(&self.inbox_root, session)
    }

    /// One ladder pass for the session: computes the step for the oldest
    /// pending message and applies it (the queued PTY input). A free function
    /// over the shared state pieces so the watcher thread needs no handle on
    /// the sink. Stream-3 C3: this is the SHARED ladder state machine of
    /// `core/a2a/sink.rs`, the same one the worker's JobMessageSink runs,
    /// driven through a [`SessionLadder`] adapter that applies the queued PTY
    /// input (never a kill) and reports a session idle only while Running.
    fn ladder_pass(
        state: &sessions_api::SessionsApiState,
        inbox_root: &std::path::Path,
        ladder: &LadderConfig,
        session: &str,
        interrupt: bool,
    ) -> LadderStep {
        crate::core::a2a::sink::ladder_pass(
            &SessionLadder {
                state,
                inbox_root,
                ladder,
            },
            session,
            interrupt,
            crate::core::a2a::inbox::now_unix(),
        )
    }

    /// Ensures the ladder watcher is running for the session (one per
    /// session; the watcher exits when the session has no uninjected message
    /// left).
    fn ensure_ladder(&self, session: &str) {
        let state = Arc::clone(&self.state);
        let inbox_root = self.inbox_root.clone();
        let ladder = self.ladder;
        let name = session.to_string();
        let idle_dir = self.inbox_dir(session);
        let interrupt_pending = Arc::clone(&self.interrupt_pending);
        let idle_interrupt_pending = Arc::clone(&self.interrupt_pending);
        let idle_name = session.to_string();
        self.ladders.ensure(
            session,
            move || {
                let is_interrupt = interrupt_pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .contains(&name);
                let step =
                    Self::ladder_pass(state.as_ref(), &inbox_root, &ladder, &name, is_interrupt);
                if matches!(step, LadderStep::Interrupt | LadderStep::Injected) {
                    interrupt_pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&name);
                }
                step
            },
            move || {
                let empty = Inbox::new(&idle_dir).uninjected_count() == 0;
                if empty {
                    idle_interrupt_pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&idle_name);
                }
                empty
            },
        );
    }
}

/// The orchestrator's [`crate::core::a2a::sink::AgentSink`] adapter
/// (stream-3 C3): what the SHARED ladder ([`crate::core::a2a::sink::ladder_pass`])
/// drives on the session path, replacing the old in-sink ladder. The
/// `idle_turn` and `interrupt` steps BOTH apply the queued PTY input (a
/// bracketed paste; nothing is ever killed on the session path) with the
/// shared A2A message budget charged at acceptance (N7), and a session is
/// idle only while it is Running (Starting/Backoff are not idle). The
/// `undeliverable` predicate: a session that no longer exists must never be
/// woken.
struct SessionLadder<'a> {
    state: &'a sessions_api::SessionsApiState,
    inbox_root: &'a std::path::Path,
    ladder: &'a LadderConfig,
}

impl crate::core::a2a::sink::AgentSink for SessionLadder<'_> {
    fn ladder_config(&self) -> LadderConfig {
        *self.ladder
    }

    fn inbox_dir(&self, run_id: &str) -> PathBuf {
        crate::core::a2a::inbox::inbox_dir_for(self.inbox_root, run_id)
    }

    fn undeliverable(&self, run_id: &str) -> Option<String> {
        let st = self
            .state
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if st.sessions.contains_key(run_id) {
            None
        } else {
            Some(format!(
                "A2A message not delivered: session '{run_id}' no longer exists"
            ))
        }
    }

    fn is_idle(&self, run_id: &str) -> bool {
        let st = self
            .state
            .supervisor_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        st.sessions
            .get(run_id)
            .is_some_and(|s| s.status == SessionStatus::Running)
    }

    fn apply_idle_turn(&self, run_id: &str, claimed: &[QueuedMessage]) -> bool {
        // The queued PTY input: a bracketed paste queues the message as the
        // next user input; nothing is killed.
        let joined = join_framed(claimed);
        // N7: the A2A sink charged this message at acceptance, so the
        // ladder's PTY write does not charge again.
        matches!(
            sessions_api::deliver_message(self.state, run_id, &claimed[0].sender, &joined, false,),
            sessions_api::DeliverOutcome::Accepted { .. }
        )
    }

    fn apply_interrupt(&self, run_id: &str, claimed: &[QueuedMessage]) -> bool {
        // On the session path the `interrupt` step is the SAME queued PTY
        // input, delivered immediately for `interrupt: true`; nothing is
        // killed (a session has no kill-and-resume).
        self.apply_idle_turn(run_id, claimed)
    }

    fn unclaimed_step(&self) -> LadderStep {
        // A session's watchers re-poll while a lease is held (Starting/Backoff
        // candidates), so an empty claim stays Queued, not Injected.
        LadderStep::Queued
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
        // The configured TTL is the one the enqueue-time prune applies, so it
        // can never disagree with the TTL the ladder expires messages at.
        Inbox::with_ttl(&inbox_dir, self.ladder.ttl_secs)
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

        // The explicit escalation (interrupt: true): the PTY input now,
        // through the same atomic claim as the ladder. A message is marked
        // injected only when the PTY write was accepted; otherwise (session
        // not running, budget throttled) it stays queued and the watcher
        // below delivers it with `interrupt = true` preserved.
        if meta.interrupt {
            self.interrupt_pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task_id.to_string());
            let step = Self::ladder_pass(
                self.state.as_ref(),
                &self.inbox_root,
                &self.ladder,
                task_id,
                true,
            );
            if matches!(step, LadderStep::Interrupt | LadderStep::Injected)
                || Inbox::new(&inbox_dir).uninjected_count() == 0
            {
                self.interrupt_pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(task_id);
                if Inbox::new(&inbox_dir).uninjected_count() == 0 {
                    return Ok(());
                }
            }
        }

        self.ensure_ladder(task_id);
        Ok(())
    }
}

/// The model, sink and backend of the orchestrator's A2A surface (shared by
/// the production builder and the test builder).
fn orchestrator_parts(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    api_state: Arc<sessions_api::SessionsApiState>,
    a2a_inbox_root: PathBuf,
    ladder: LadderConfig,
) -> (
    A2aModel,
    Arc<SupervisorTaskBackend>,
    Arc<SessionMessageSink>,
) {
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
    // Messages queued before a restart have no ladder watcher: start one per
    // session that still holds an uninjected message.
    sink.recover_pending();
    let backend = Arc::new(SupervisorTaskBackend::new(state, controls, a2a_inbox_root));
    (model, backend, sink)
}

/// Builds the orchestrator's A2A router: every route except the public agent
/// card sits behind jobset authorization only (SPEC §7.4, §7.5): no operator
/// bearer and no open mode.
///
/// `api_state` is the SAME [`sessions_api::SessionsApiState`] the REST
/// `/api/v1/sessions` router uses: the A2A `SendMessage` sink and the REST
/// `POST .../message` handler therefore share ONE rate limiter (the claimed
/// "shared budget" is real), rather than the A2A router building a second one.
/// `notifier` propagates a jobset end to the mesh peers (SPEC §7.16).
pub fn a2a_router_jobset_only(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    api_state: Arc<sessions_api::SessionsApiState>,
    jobset: crate::core::a2a::JobsetCred,
    notifier: Arc<dyn crate::core::a2a::jobset_routes::PeerEndNotifier>,
    a2a_inbox_root: PathBuf,
    ladder: LadderConfig,
) -> Router {
    let (model, backend, sink) =
        orchestrator_parts(state, controls, api_state, a2a_inbox_root, ladder);
    crate::core::a2a::a2a_router_with_jobset_only(model, backend, Some(sink), jobset, notifier)
}

/// Test-harness builder with the operator-bearer verifier (the handler and
/// sink mechanics these unit tests cover are independent of the auth layer).
/// Never mounted in production.
#[cfg(test)]
pub fn a2a_router(
    state: Arc<std::sync::Mutex<SupervisorState>>,
    controls: SessionControlRegistry,
    api_state: Arc<sessions_api::SessionsApiState>,
    auth: Option<AuthVerifier>,
    jobset: Option<crate::core::a2a::JobsetCred>,
    a2a_inbox_root: PathBuf,
    ladder: LadderConfig,
) -> Router {
    let (model, backend, sink) =
        orchestrator_parts(state, controls, api_state, a2a_inbox_root, ladder);
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
                instructions_file: None,
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

    /// An `interrupt: true` message whose PTY write is not accepted (here:
    /// the session has no live PTY) must stay queued and watched. It used to
    /// be marked injected regardless of the write result, i.e. lost while the
    /// sender was told it was delivered.
    #[test]
    fn interrupt_delivery_that_cannot_be_written_stays_queued() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let session = "homelab";
        let mut state = SupervisorState::new(100);
        let mut s = SessionState::new(session.to_string());
        s.status = SessionStatus::Running;
        state.sessions.insert(session.to_string(), s);
        let api_state = sessions_api::SessionsApiState::new(
            Arc::new(std::sync::Mutex::new(state)),
            SessionControlRegistry::new(),
            Arc::new(crate::orchestrate::session_log::SessionLogs::from_env()),
        );
        let sink = SessionMessageSink::new(api_state, root.clone(), Default::default());
        crate::core::a2a::MessageSink::deliver_with_meta(
            &sink,
            session,
            "peer-agent",
            "urgent",
            "msg-1",
            DeliveryMeta {
                hop: 0,
                interrupt: true,
            },
        )
        .expect("the message is accepted into the inbox");

        let inbox = Inbox::new(crate::core::a2a::inbox::inbox_dir_for(&root, session));
        assert!(
            !inbox.injected("msg-1"),
            "an unwritten message must not be acknowledged"
        );
        assert_eq!(inbox.pending().len(), 1, "the claim was handed back");
        assert!(
            sink.ladders.is_active(session),
            "the queued message keeps a ladder watcher"
        );
        assert!(
            sink.interrupt_pending.lock().unwrap().contains(session),
            "interrupt flag must remain pending while queued"
        );
    }

    #[test]
    fn interrupt_queued_during_starting_delivers_on_interrupt_channel_when_ready() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let session = "homelab";
        let mut state = SupervisorState::new(100);
        let mut s = SessionState::new(session.to_string());
        s.status = SessionStatus::Starting;
        state.sessions.insert(session.to_string(), s);

        let controls = SessionControlRegistry::new();
        let sess_cfg = crate::orchestrate::config::SessionConfig {
            name: session.to_string(),
            workdir: std::path::PathBuf::from("/tmp"),
            session_id: "id-1".to_string(),
            engine: None,
            claude_binary: std::path::PathBuf::from("claude"),
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
        };
        let control = Arc::new(crate::orchestrate::session_control::SessionControl::new(
            sess_cfg,
        ));
        controls.insert(Arc::clone(&control));

        let api_state = sessions_api::SessionsApiState::new(
            Arc::new(std::sync::Mutex::new(state)),
            controls,
            Arc::new(crate::orchestrate::session_log::SessionLogs::from_env()),
        );
        // High ack_secs (3600s) proves delivery happens via interrupt escalation,
        // not via IdleTurn aging.
        let ladder = LadderConfig {
            ack_secs: 3600,
            force_secs: 7200,
            ttl_secs: 14400,
        };
        let sink = SessionMessageSink::new(Arc::clone(&api_state), root.clone(), ladder);

        // Queue while Starting (no PTY yet).
        crate::core::a2a::MessageSink::deliver_with_meta(
            &sink,
            session,
            "peer-agent",
            "urgent-during-backoff",
            "msg-int-1",
            DeliveryMeta {
                hop: 0,
                interrupt: true,
            },
        )
        .expect("accepted into inbox");

        assert!(
            sink.interrupt_pending.lock().unwrap().contains(session),
            "interrupt_pending must be recorded when initial write fails"
        );

        // Now attach a live PTY and transition session to Running.
        let pty = crate::orchestrate::pty::Pty::open().expect("open pty");
        control.pty_input.lock().unwrap().put(pty.master);
        {
            let mut st = api_state.supervisor_state.lock().unwrap();
            st.sessions.get_mut(session).unwrap().status = SessionStatus::Running;
        }

        // Wait briefly for the watcher pass to deliver the interrupt message.
        let inbox = Inbox::new(crate::core::a2a::inbox::inbox_dir_for(&root, session));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while !inbox.injected("msg-int-1") && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        control.pty_input.lock().unwrap().take();

        assert!(
            inbox.injected("msg-int-1"),
            "watcher must inject queued interrupt message without waiting for ack_secs"
        );
        // The watcher marks the message injected and then clears the pending
        // entry in the same pass; wait for the clear instead of racing it.
        let clear_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while sink.interrupt_pending.lock().unwrap().contains(session)
            && std::time::Instant::now() < clear_deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(
            !sink.interrupt_pending.lock().unwrap().contains(session),
            "interrupt_pending must be cleared after injection"
        );
    }
}

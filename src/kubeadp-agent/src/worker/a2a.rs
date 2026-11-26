//! A2A (Agent-to-Agent) protocol task backend for agent-run-api.
//!
//! Exposes agent-run jobs as A2A tasks over the [`crate::core::a2a::TaskBackend`]
//! trait:
//! - An A2A Task corresponds to a job (id = job id; contextId = job id).
//! - State mapping converts `JobStatus` into `TaskState`:
//!   - `Running` -> `TaskState::Working`
//!   - `Done` -> `TaskState::Completed`
//!   - `Failed`, `Incomplete`, `QuotaExhausted` -> `TaskState::Failed`
//!   - `SelfTerminated`, `Killed` -> `TaskState::Canceled`
//!   - an inbound message queued in the run's inbox -> `TaskState::Submitted`
//!     (the sender's task state while the message waits; design HL-113 §7.14)
//! - Artifacts: the job's final response text (if present), plus the git diff
//!   text (if available and cheap).
//! - `ListTasks`/`GetTask`/`CancelTask`: list, exact-id lookup, kill.
//!
//! Inbound delivery is the escalation ladder of design §7.14 (phase 2,
//! [`crate::core::a2a::delivery`]): the message is enqueued into the run's
//! durable inbox (queued -> sender task `submitted`), the hook channel
//! injects it at the next model-call boundary, and the ladder escalates to
//! `idle_turn` after `a2a_ack_secs` with the run idle and to the explicit
//! `interrupt` only for `interrupt: true` / past `a2a_force_secs`.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::core::a2a::{
    a2a_rate_limited, map_job_status, A2AError, A2aTask, MessageSink, TaskBackend,
};
use crate::core::commands;
use crate::core::config::Config;
use crate::core::job::{compute_status, extract_final_response, list_jobs, JobMetadata, JobStatus};
use a2a::{Artifact, Part, TaskState};
// The `MessageSink` trait (and its helpers) return the SDK's `a2a::A2AError`,
// a distinct type from the backend `A2AError` (string `code`) used above for
// the `TaskBackend` methods.
use crate::core::a2a::delivery::{
    sender_task_state as map_sender_task_state, LadderConfig, LadderStep, SenderTaskState,
};
use crate::core::a2a::inbox::{Inbox, QueuedMessage};
use crate::core::a2a::inject::join_framed;
use crate::core::a2a::DeliveryMeta;
use crate::engine::capability::InjectChannel;
use a2a::A2AError as SdkA2AError;

/// Bound on the sink's global idempotency cache of delivered message ids.
const SEEN_CACHE_CAP: usize = 4096;
/// Upper bound on the number of distinct in-memory per-job escalation slots.
///
/// Each delivered job gets one `JobSlot`; a slot is never explicitly removed
/// once created (a job can idle for a long time and then receive a new
/// message), so without a bound the map would keep one entry per job the
/// worker ever spoke to. When a NEW job would exceed the bound, the oldest
/// slot is evicted: the evicted job's `JobSlot` is recreated from disk on its
/// next message (the gate is only in-process serialization on top of the
/// atomic store claim, so ephemeral eviction is safe). 16384 slots covers any
/// realistic in-process job churn and keeps the map bounded.
const MAX_SLOTS: usize = 16384;
/// TaskBackend implementation that operates on agent-core's SQLite job store and worktrees.
pub struct JobTaskBackend {
    config: Arc<Config>,
}

impl JobTaskBackend {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// Converts a [`JobMetadata`] row into an [`A2aTask`] with state and artifacts.
    fn job_to_task(&self, meta: &JobMetadata) -> A2aTask {
        let status = compute_status(meta, &self.config.state_root);
        let mut state = map_job_status(status);

        let mut status_message = None;
        let mut error = None;
        let mut artifacts_vec = Vec::new();

        // The ladder (design §7.14): queued == the sender's task is
        // `submitted`; a durable expiry marker fails the task.
        {
            let inbox_dir =
                crate::core::a2a::inbox::inbox_dir_for(&self.config.state_root, &meta.job_id);
            let inbox = Inbox::new(&inbox_dir);
            let expiry = crate::core::a2a::delivery::read_expiry(&inbox_dir);
            match map_sender_task_state(inbox.pending_count(), expiry.as_ref()) {
                SenderTaskState::Submitted => state = TaskState::Submitted,
                SenderTaskState::Failed { reason } => {
                    state = TaskState::Failed;
                    error = Some(reason);
                }
                SenderTaskState::Working => {}
            }
        }

        // 1. Final response from job log (if any)
        let log_path = JobMetadata::log_path(&self.config.state_root, &meta.job_id);
        if let Ok(bytes) = fs::read(&log_path) {
            let log = String::from_utf8_lossy(&bytes);
            if let Some(resp) = extract_final_response(&log) {
                if !resp.is_empty() {
                    status_message = Some(resp.clone());
                    artifacts_vec.push(Artifact {
                        artifact_id: format!("{}-result", meta.job_id),
                        name: Some("result.txt".to_string()),
                        description: Some("Job result text".to_string()),
                        parts: vec![Part::text(resp)],
                        metadata: None,
                        extensions: None,
                    });
                }
            }
        }

        // Error message for failed/terminal states (an inbox expiry reason
        // already occupies the error slot).
        if (status == JobStatus::Failed
            || status == JobStatus::Incomplete
            || status == JobStatus::QuotaExhausted)
            && error.is_none()
        {
            error = Some(format!("Job ended with status: {status}"));
        }

        // 2. Git diff artifact if worktree exists and cheap to compute.
        // Refuse to compute a diff from a worktree agent-run owns whose .git
        // pointer does not resolve inside the project's repos root / workspace
        // checkout (a tampered/stale pointer could redirect the diff into an
        // attacker-controlled repo); on refusal the task is returned without
        // the diff artifact rather than failing. External (--from-worktree)
        // worktrees are user-supplied and are not validated against our roots.
        let pointer_ok = if meta.external_worktree {
            true
        } else {
            crate::core::git::worktree_gitdir_in_expected_roots(
                &meta.worktree_path,
                &crate::core::git::expected_worktree_roots(&self.config, &meta.project),
            )
            .is_ok()
        };
        if meta.worktree_path.exists() && pointer_ok {
            if let Ok((diff_text, _)) = commands::diff::job_diff_text(meta) {
                if !diff_text.is_empty() {
                    artifacts_vec.push(Artifact {
                        artifact_id: format!("{}-diff", meta.job_id),
                        name: Some("changes.diff".to_string()),
                        description: Some("Worktree git diff".to_string()),
                        parts: vec![Part::text(diff_text)],
                        metadata: None,
                        extensions: None,
                    });
                }
            }
        }

        let artifacts = if artifacts_vec.is_empty() {
            None
        } else {
            Some(artifacts_vec)
        };

        A2aTask {
            id: meta.job_id.clone(),
            context_id: Some(meta.job_id.clone()),
            state,
            status_message,
            error,
            artifacts,
        }
    }
}

impl TaskBackend for JobTaskBackend {
    fn get_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        let meta = JobMetadata::load(&self.config.state_root, task_id)
            .map_err(|_| A2AError::task_not_found(task_id))?;
        Ok(self.job_to_task(&meta))
    }

    fn list_tasks(
        &self,
        context_id: Option<&str>,
        state_filter: Option<&TaskState>,
    ) -> Result<Vec<A2aTask>, A2AError> {
        let all_jobs = list_jobs(&self.config.state_root, None)
            .map_err(|e| A2AError::internal(format!("failed to list jobs: {e}")))?;

        let mut tasks = Vec::new();
        for meta in all_jobs {
            if let Some(ctx) = context_id {
                // context_id matches job_id
                if meta.job_id != ctx {
                    continue;
                }
            }
            let task = self.job_to_task(&meta);
            if let Some(target_state) = state_filter {
                if &task.state != target_state {
                    continue;
                }
            }
            tasks.push(task);
        }

        Ok(tasks)
    }

    fn cancel_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        // (M4) CancelTask / task-id lookup: use an EXACT job-id match
        let meta = JobMetadata::load(&self.config.state_root, task_id)
            .map_err(|_| A2AError::task_not_found(task_id))?;
        let status = compute_status(&meta, &self.config.state_root);

        // A job that is already terminal cannot be canceled
        if status.is_terminal() {
            return Err(A2AError::task_not_cancelable(task_id));
        }

        // Run kill command using exact job id (never resolving prefixes)
        let _ = commands::kill::run_exact(&self.config, &meta.job_id)
            .map_err(|e| A2AError::internal(format!("failed to kill job {task_id}: {e}")))?;

        // Re-read task state after kill
        let fresh_meta = JobMetadata::load(&self.config.state_root, &meta.job_id).unwrap_or(meta);
        Ok(self.job_to_task(&fresh_meta))
    }
}

/// Per-job state: a `gate` mutex serialising at most one resume at a time
/// and the `last_interrupt` timestamp (the per-job storm limit). The durable
/// per-run inbox replaced the in-memory coalesce buffer (design §7.14).
struct JobSlot {
    /// Serializes every in-process escalation for the job: the ladder
    /// watcher, the manual ladder pass, and the explicit interrupt share it,
    /// so two in-process passages of the ladder for the SAME job can never
    /// both claim an attempt for the same message. Cross-process safety comes
    /// from the atomic queue claim (`Inbox::claim_for_resume`).
    gate: Arc<Mutex<()>>,
    last_interrupt: Mutex<Option<Instant>>,
}

impl JobSlot {
    fn new() -> Self {
        Self {
            gate: Arc::new(Mutex::new(())),
            last_interrupt: Mutex::new(None),
        }
    }
}

/// A2A `SendMessage` sink for the worker: the message is enqueued into the
/// job's durable inbox and delivered by the escalation ladder (design §7.14,
/// phase 2). The hook channel injects at the next model-call boundary; the
/// watcher (one per job) escalates to `idle_turn` past `a2a_ack_secs` with
/// the run idle (a RUNNING live-mode job over its stdin pipe, the §7.11 live
/// path; a finished job is resumed) and to `interrupt` only for
/// `interrupt: true` / past `a2a_force_secs` (0 = never). An operator-KILLED
/// job is never woken; unknown / cleaned / discarded jobs answer
/// `TASK_NOT_FOUND`. At most one resume in flight per job; a repeated
/// message id is applied at most once.
pub struct JobMessageSink {
    config: Arc<Config>,
    slots: Mutex<HashMap<String, Arc<JobSlot>>>,
    /// First-appearance order of the tracked job slots (FIFO eviction).
    slot_order: Mutex<VecDeque<String>>,
    seen: Mutex<VecDeque<String>>,
    /// The ladder watchers (one per job).
    ladders: crate::core::a2a::delivery::LadderWatchers,
    /// Test-only seam: when set, `deliver_with_meta` does NOT start the
    /// background ladder watcher, so a test drives every escalation through
    /// the explicit [`JobMessageSink::ladder_pass`] entry point it calls
    /// itself. This removes the wall-clock race where the background watcher
    /// (one pass per second) would resume or expire the queued message before
    /// a test whose thread is slow on a loaded runner. It is always present
    /// but defaults to `false`, and the only writer
    /// ([`set_test_ladder_disabled`]) is `#[cfg(test)]`-gated, so production
    /// can never flip it and the delivered behaviour is unchanged.
    test_ladder_disabled: std::sync::atomic::AtomicBool,
}

impl JobMessageSink {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            slots: Mutex::new(HashMap::new()),
            slot_order: Mutex::new(VecDeque::new()),
            seen: Mutex::new(VecDeque::new()),
            ladders: crate::core::a2a::delivery::LadderWatchers::new(),
            test_ladder_disabled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Test-only: stop `deliver_with_meta` from starting the background
    /// ladder watcher for this sink (see [`test_ladder_disabled`]). Tests call
    /// it before delivering so the queued message is escalated only by the
    /// explicit [`JobMessageSink::ladder_pass`] the test drives.
    #[cfg(test)]
    pub(crate) fn set_test_ladder_disabled(&self, disabled: bool) {
        self.test_ladder_disabled
            .store(disabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Restart recovery: the ladder watchers are in-memory threads, so the
    /// messages that were queued when the previous process ended have no
    /// watcher. Starts one for every run that still holds an uninjected
    /// message (it escalates or expires them exactly like a fresh delivery)
    /// and prunes the acknowledged rows past their retention. Called once
    /// when the worker builds the sink.
    pub fn recover_pending(&self) {
        crate::core::a2a::inbox::sweep_retention(&self.config.state_root);
        for run in crate::core::a2a::inbox::runs_with_uninjected(&self.config.state_root) {
            self.ensure_ladder(&run);
        }
    }

    /// The per-job slot, creating it on first use; the job's own `gate`
    /// (acquired by the caller) serialises a resume for that job. The map is
    /// bounded by [`MAX_SLOTS`]; the oldest slot is evicted when a new job
    /// would exceed the bound and is transparently recreated on its next
    /// message.
    fn slot(&self, task_id: &str) -> Arc<JobSlot> {
        let mut order = self.slot_order.lock().expect("slot_order lock poisoned");
        let mut map = self.slots.lock().expect("slots lock poisoned");
        if !map.contains_key(task_id) {
            while map.len() >= MAX_SLOTS {
                if let Some(oldest) = order.pop_front() {
                    map.remove(&oldest);
                } else {
                    break;
                }
            }
            order.push_back(task_id.to_string());
        }
        map.entry(task_id.to_string())
            .or_insert_with(|| Arc::new(JobSlot::new()))
            .clone()
    }

    /// The idempotency key of a delivery: the message id SCOPED to the
    /// addressed job. An id seen for one job must not suppress the same id
    /// addressed to another job.
    fn seen_key(task_id: &str, message_id: &str) -> String {
        format!("{task_id}\u{0}{message_id}")
    }

    /// True when this (job, message id) was already ACCEPTED (idempotency).
    fn is_seen(&self, task_id: &str, message_id: &str) -> bool {
        let key = Self::seen_key(task_id, message_id);
        let seen = self.seen.lock().expect("seen lock poisoned");
        seen.iter().any(|id| *id == key)
    }

    /// Records an accepted (job, message id). Called only AFTER the message
    /// was stored: a delivery that was rejected (unknown or killed job, a
    /// failed enqueue) must stay retryable under the same message id instead
    /// of turning the sender's retry into a silent no-op success.
    fn mark_seen(&self, task_id: &str, message_id: &str) {
        let key = Self::seen_key(task_id, message_id);
        let mut seen = self.seen.lock().expect("seen lock poisoned");
        if seen.iter().any(|id| *id == key) {
            return;
        }
        seen.push_back(key);
        while seen.len() > SEEN_CACHE_CAP {
            seen.pop_front();
        }
    }

    /// The run's inbox for a job (the ladder's queue and dedupe store). The
    /// configured TTL is the one the enqueue-time prune applies, so it can
    /// never disagree with the TTL the ladder expires messages at.
    fn inbox(&self, task_id: &str) -> Inbox {
        Inbox::with_ttl(
            crate::core::a2a::inbox::inbox_dir_for(&self.config.state_root, task_id),
            self.config.a2a_inbox_ttl_secs,
        )
    }

    /// Enqueues `text` into the job's durable inbox (a fresh delivery
    /// supersedes an earlier expiry).
    fn enqueue(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
        meta: DeliveryMeta,
    ) -> Result<(), SdkA2AError> {
        let inbox = self.inbox(task_id);
        crate::core::a2a::delivery::clear_expiry(inbox.dir());
        inbox
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
                SdkA2AError::internal(format!(
                    "failed to enqueue the A2A message into the run inbox: {e}"
                ))
            })
    }

    /// Ensures the ladder watcher is running for the job (it exits when the
    /// run has no uninjected message left). The watcher drives the SAME
    /// `ladder_pass` entry point as the manual/escalation path under the
    /// job's gate, so every in-process escalation of one job is serialized
    /// and there is exactly one code path through the ladder.
    fn ensure_ladder(&self, task_id: &str) {
        let config = Arc::clone(&self.config);
        let job_id = task_id.to_string();
        let slot = self.slot(task_id);
        let idle_dir = crate::core::a2a::inbox::inbox_dir_for(&self.config.state_root, task_id);
        self.ladders.ensure(
            task_id,
            move || Self::ladder_pass(&config, &slot, &job_id, false),
            move || Inbox::new(&idle_dir).uninjected_count() == 0,
        );
    }

    /// Like [`ensure_ladder`] but honouring the test-only seam: when
    /// `test_ladder_disabled` is set (only ever by tests, via
    /// [`set_test_ladder_disabled`]), the background watcher is NOT started
    /// and the test drives the ladder pass itself. Production never sets the
    /// flag, so it always starts the watcher here.
    fn ensure_ladder_for_delivery(&self, task_id: &str) {
        if self
            .test_ladder_disabled
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.ensure_ladder(task_id);
    }

    /// The ladder configuration for this worker.
    fn ladder_config(config: &Config) -> LadderConfig {
        LadderConfig {
            ack_secs: config.a2a_ack_secs,
            force_secs: config.a2a_force_secs,
            ttl_secs: config.a2a_inbox_ttl_secs,
        }
    }

    /// Whether the `idle_turn` step is available (the run is idle at a turn
    /// boundary): a finished job is idle; a RUNNING job only with a live
    /// stdin channel (the §7.11 live path, off unless the idle turn needs it).
    fn idle_turn_available(config: &Config, job_id: &str) -> bool {
        match JobMetadata::resolve(&config.state_root, job_id) {
            Ok(meta) => {
                if compute_status(&meta, &config.state_root) != JobStatus::Running {
                    return true;
                }
                config.a2a_live_stdin && crate::core::a2a_live::lookup(job_id).is_some()
            }
            Err(_) => false,
        }
    }

    /// Applies the `idle_turn` step: a RUNNING live-channel job gets the
    /// message over the pipe; a finished job is resumed (no kill). Returns
    /// `true` when the message was handed to the run.
    fn apply_idle_turn(config: &Config, job_id: &str, joined: &str) -> bool {
        match JobMetadata::resolve(&config.state_root, job_id) {
            Ok(meta) => {
                let running = compute_status(&meta, &config.state_root) == JobStatus::Running;
                if !running {
                    return Self::resume_with(config, &meta, joined).is_ok();
                }
                if !config.a2a_live_stdin {
                    return false;
                }
                if let Some(channel) = crate::core::a2a_live::lookup(job_id) {
                    if channel.is_alive_for(meta.pid, meta.pid_start_time) {
                        if channel.deliver_line(joined).is_ok() {
                            channel.touch();
                            return true;
                        }
                        crate::core::a2a_live::close_for(job_id);
                    }
                }
                false
            }
            Err(_) => false,
        }
    }

    /// One ladder pass for the job (`idle_turn`/`interrupt` escalation),
    /// serialized against every other in-process ladder pass for the SAME job
    /// by the job's gate mutex (the pre-resolved [`JobSlot`], created once per
    /// job when the sink first touches it). The gate is what makes a manual
    /// [`JobMessageSink::ladder_pass`] and the background watcher
    /// ([`ensure_ladder`]) mutually exclusive for one job within this process;
    /// the atomic store claim ([`Inbox::claim_for_resume`], see
    /// [`JobMessageSink::ladder_pass_impl`]) is what makes the resume claim
    /// win exactly once even across processes/channels.
    ///
    /// The production entry point of the escalation ladder: the background
    /// watcher drives it for every job, so the production path and the tests
    /// exercise the SAME function (a `ladder_pass` with `interrupt = true`
    /// exercises the interrupt branch of the shared ladder, e.g. the force
    /// timeout; the explicit `interrupt: true` send still shortcuts through
    /// [`JobMessageSink::escalate_interrupt`]).
    fn ladder_pass(config: &Config, slot: &JobSlot, job_id: &str, interrupt: bool) -> LadderStep {
        let _gate = slot.gate.lock().unwrap_or_else(|e| e.into_inner());
        Self::ladder_pass_impl(config, job_id, interrupt)
    }

    /// One ladder pass for the job: computes the step for the oldest pending
    /// message and applies it; the caller re-polls on a non-terminal step.
    ///
    /// The resume (the `idle_turn`/`interrupt` escalation) is claimed
    /// ATOMICALLY in the store: [`Inbox::claim_for_resume`] marks the pending
    /// rows `claimed_at` in one transaction and returns only the messages
    /// THIS caller won. The winner alone resumes the job and acknowledges
    /// the claimed messages; a loser (another ladder pass, the hook channel,
    /// or the sandboxed HTTP receive side) gets an empty claim and steps
    /// `Injected` without touching the job. Without the claim two concurrent
    /// ladder passes for the same job would both read the same pending
    /// message and both resume it, claiming TWO attempts for ONE message.
    /// The caller also holds the per-job gate (see [`ensure_ladder`]) so
    /// in-process passages are serialized on top.
    fn ladder_pass_impl(config: &Config, job_id: &str, interrupt: bool) -> LadderStep {
        // Stream-3 C3: the SAME ladder state machine the orchestrator's
        // SessionMessageSink runs, over the same InboxStore surface
        // (core/a2a/sink.rs). The adapter supplies the §7.11 idle test/the
        // idle_turn and the interrupt application exactly as the pre-C3
        // ladder did, so no behaviour moves.
        crate::core::a2a::sink::ladder_pass(
            &JobLadder { config },
            job_id,
            interrupt,
            crate::core::a2a::inbox::now_unix(),
        )
    }

    /// Resume the job's conversation with `prompt` as the next turn. For a
    /// RUNNING job the current `agy` attempt is stopped cleanly first (the
    /// retry follow-up path refuses `Running` jobs); a finished job is resumed
    /// directly. The resume goes through `retry::run` with a follow-up prompt,
    /// which continues the SAME conversation (conversation_id) with the new
    /// turn -- it never marks the job user-killed or failed.
    fn resume_with(config: &Config, meta: &JobMetadata, prompt: &str) -> Result<(), SdkA2AError> {
        use crate::core::args::RetryArgs;
        use crate::core::commands::retry;
        use std::io::Write;

        // A running job must be stopped before the follow-up retry can claim a
        // new attempt (`retry::run` refuses `Running`). Stop it the same way an
        // operator kill does -- clean SIGTERM to the process group, grace, then
        // SIGKILL -- but WITHOUT the kill bookkeeping: this is an interrupt for
        // a message, not a user kill, and the job is about to be resumed. The
        // kill command would also mark the row `killed`, which we must not do,
        // so terminate the process directly via the shared mechanism. The old
        // monitor thread that reaps the stopped child writes its final status
        // guarded by `retry_count`; `retry::run` bumps `retry_count` before it
        // launches, so that stale save is a no-op and cannot mark the job killed.
        let is_running = compute_status(meta, &config.state_root) == JobStatus::Running;
        if let (true, Some(pid)) = (is_running, meta.pid) {
            if let Err(e) = crate::core::commands::kill::terminate_process_to(
                config,
                &meta.job_id,
                pid,
                meta.pid_start_time,
                Some(meta.retry_count),
                &mut |_stderr, line| {
                    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log_path)
                    {
                        let _ = f.write_all(format!("[a2a interrupt] {line}\n").as_bytes());
                    }
                },
            ) {
                return Err(SdkA2AError::internal(format!(
                    "failed to stop the running attempt before resuming job '{}': {e}",
                    meta.job_id
                )));
            }
        }

        // Resume the same conversation with the framed message as a follow-up
        // turn. `retry::run` re-reads the row, recovers the conversation id if
        // the row lost it, bumps the attempt, and launches agy continuing the
        // prior conversation with `prompt` as the new prompt.
        let args = RetryArgs {
            job_id: meta.job_id.clone(),
            prompt: Some(prompt.to_string()),
            ..Default::default()
        };
        let _out = retry::run(config, &args).map_err(|e| {
            SdkA2AError::invalid_params(format!("failed to resume job '{}': {e}", meta.job_id))
        })?;
        // A launched resume prints the new PID; the A2A response returns the
        // task state and the caller observes progress via GetTask.
        Ok(())
    }

    /// Checks whether the per-job interrupt storm limit (`a2a_interrupt_min_secs`,
    /// 0 = off) currently rejects an explicit `interrupt: true` request, under
    /// `last_interrupt` lock.
    fn check_interrupt_rate_limit(&self, job: &JobSlot) -> Result<(), SdkA2AError> {
        let min_secs = self.config.a2a_interrupt_min_secs;
        if min_secs > 0 {
            let last = *job
                .last_interrupt
                .lock()
                .expect("last_interrupt lock poisoned");
            if let Some(last) = last {
                let since = last.elapsed().as_secs();
                if since < min_secs {
                    let retry_after = min_secs.saturating_sub(since).max(1);
                    return Err(a2a_rate_limited(format!(
                        "interrupt rate limit: this job was interrupted {since}s ago; retry in {retry_after}s",
                    )));
                }
            }
        }
        Ok(())
    }

    /// The explicit `interrupt` escalation (design §7.14, last resort): the
    /// kill-and-resume of §7.11. Applies the per-job storm limit
    /// (`a2a_interrupt_min_secs`, 0 = off), then resumes with the joined
    /// pending messages -- claimed ATOMICALLY so only one concurrent
    /// interrupt (or ladder pass) resumes for the same message -- marked
    /// injected through the `interrupt` channel. Runs under the job's gate.
    fn escalate_interrupt(&self, task_id: &str) -> Result<(), SdkA2AError> {
        let job = self.slot(task_id);
        let gate = job.gate.lock().expect("gate lock poisoned");
        if let Err(err) = self.check_interrupt_rate_limit(&job) {
            drop(gate);
            return Err(err);
        }
        let meta = JobMetadata::resolve(&self.config.state_root, task_id)
            .map_err(|_| SdkA2AError::task_not_found(task_id))?;
        let inbox = self.inbox(task_id);
        let claimed = inbox.claim_for_resume();
        if claimed.is_empty() {
            drop(gate);
            return Ok(());
        }
        *job.last_interrupt
            .lock()
            .expect("last_interrupt lock poisoned") = Some(Instant::now());
        let joined = join_framed(&claimed);
        let result = Self::resume_with(&self.config, &meta, &joined);
        if result.is_ok() {
            for msg in &claimed {
                inbox.mark_injected(&msg.message_id, InjectChannel::Interrupt.as_str());
            }
        } else {
            for msg in &claimed {
                inbox.release_claim(&msg.message_id);
            }
        }
        drop(gate);
        result
    }
}

impl MessageSink for JobMessageSink {
    fn deliver_with_meta(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
        meta: DeliveryMeta,
    ) -> Result<(), SdkA2AError> {
        // Idempotency: a repeated A2A message id is a no-op success; the
        // durable inbox is the dedupe store.
        if self.is_seen(task_id, message_id) {
            return Ok(());
        }

        let meta_job = match JobMetadata::resolve(&self.config.state_root, task_id) {
            Ok(m) => m,
            Err(_) => return Err(SdkA2AError::task_not_found(task_id)),
        };

        if meta_job.status == Some(JobStatus::Killed) {
            return Err(SdkA2AError::invalid_params(format!(
                "job '{}' was killed by the operator and cannot be resumed with a message",
                task_id
            )));
        }

        if !meta_job.worktree_path.exists() {
            return Err(SdkA2AError::task_not_found(task_id));
        }

        // The explicit last resort (interrupt: true, trusted metadata): the
        // storm limit applies before the enqueue, then escalate immediately.
        if meta.interrupt {
            let job = self.slot(task_id);
            self.check_interrupt_rate_limit(&job)?;
            self.enqueue(task_id, sender, text, message_id, meta)?;
            self.mark_seen(task_id, message_id);
            let result = self.escalate_interrupt(task_id);
            if result.is_err() {
                // The interrupt did not apply (concurrent storm limit, failed resume)
                // but the message IS queued: it must not sit unwatched, so
                // the ladder delivers it at the next boundary / idle turn.
                self.ensure_ladder_for_delivery(task_id);
            }
            return result;
        }

        // The default path: enqueue into the durable inbox -- the hooks
        // inject at the next boundary and the ladder escalates.
        self.enqueue(task_id, sender, text, message_id, meta)?;
        self.mark_seen(task_id, message_id);
        self.ensure_ladder_for_delivery(task_id);
        Ok(())
    }
}

/// The worker's [`crate::core::a2a::sink::AgentSink`] adapter (stream-3 C3):
/// this is what the SHARED ladder ([`crate::core::a2a::sink::ladder_pass`])
/// drives for the worker role, replacing the old in-worker ladder
/// `ladder_pass_impl`. Every hook delegates to the SAME logic the pre-C3
/// ladder used, so no behaviour moves: the ladder configuration, the §7.11
/// idle test, the `idle_turn` (never a kill) and the `interrupt` (the
/// kill-and-resume via [`JobMessageSink::resume_with`]) all keep their exact
/// semantics; the sink also resolves undeliverability (a killed or vanished
/// job must never be woken).
struct JobLadder<'a> {
    config: &'a Config,
}

impl crate::core::a2a::sink::AgentSink for JobLadder<'_> {
    fn ladder_config(&self) -> LadderConfig {
        JobMessageSink::ladder_config(self.config)
    }

    fn inbox_dir(&self, run_id: &str) -> std::path::PathBuf {
        crate::core::a2a::inbox::inbox_dir_for(&self.config.state_root, run_id)
    }

    fn undeliverable(&self, run_id: &str) -> Option<String> {
        match JobMetadata::resolve(&self.config.state_root, run_id) {
            Ok(meta) if meta.status == Some(JobStatus::Killed) => Some(format!(
                "A2A message not delivered: job '{run_id}' was killed by the operator"
            )),
            Ok(_) => None,
            Err(_) => Some(format!(
                "A2A message not delivered: job '{run_id}' no longer exists"
            )),
        }
    }

    fn is_idle(&self, run_id: &str) -> bool {
        JobMessageSink::idle_turn_available(self.config, run_id)
    }

    fn apply_idle_turn(&self, run_id: &str, claimed: &[QueuedMessage]) -> bool {
        let joined = join_framed(claimed);
        JobMessageSink::apply_idle_turn(self.config, run_id, &joined)
    }

    fn apply_interrupt(&self, run_id: &str, claimed: &[QueuedMessage]) -> bool {
        let joined = join_framed(claimed);
        match JobMetadata::resolve(&self.config.state_root, run_id) {
            Ok(meta) => JobMessageSink::resume_with(self.config, &meta, &joined).is_ok(),
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{open_state_db, JobStatus};
    use http_body_util::BodyExt;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> Config {
        Config {
            state_root: dir.path().to_path_buf(),
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        }
    }

    fn create_test_job(config: &Config, id: &str, status: JobStatus) -> JobMetadata {
        let meta = JobMetadata {
            job_id: id.to_string(),
            project: "test-proj".to_string(),
            prompt: "test prompt".to_string(),
            resolved_prompt: "test prompt".to_string(),
            model: Some("test-model".to_string()),
            effort: None,
            sandbox: false,
            mode: "fast".to_string(),
            add_dirs: Vec::new(),
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: config.workspace_root.join(id),
            branch: format!("agent-run/{id}"),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            exit_code: match status {
                JobStatus::Done => Some(0),
                JobStatus::Running => None,
                _ => Some(1),
            },
            status: Some(status),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            pid_start_time: None,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(&config.state_root).unwrap();

        let log_path = JobMetadata::log_path(&config.state_root, id);
        if let Some(parent) = log_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if status == JobStatus::Done {
            let _ = fs::write(
                &log_path,
                r#"{"status":"SUCCESS","response":"Success result"}"#,
            );
        } else if status == JobStatus::Failed {
            let _ = fs::write(&log_path, r#"{"status":"ERROR","error":"Failed error"}"#);
        }

        meta
    }

    #[test]
    fn test_unknown_task_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config);

        let err = backend.get_task("nonexistent").unwrap_err();
        assert_eq!(err.code, "taskNotFound");
    }

    #[test]
    fn test_state_mapping_and_get_task() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        create_test_job(&config, "job-done", JobStatus::Done);
        create_test_job(&config, "job-failed", JobStatus::Failed);
        create_test_job(&config, "job-killed", JobStatus::Killed);

        let t1 = backend.get_task("job-done").unwrap();
        assert_eq!(t1.id, "job-done");
        assert_eq!(t1.context_id.as_deref(), Some("job-done"));
        assert_eq!(t1.state, TaskState::Completed);

        let t2 = backend.get_task("job-failed").unwrap();
        assert_eq!(t2.state, TaskState::Failed);
        assert!(t2.error.is_some());

        let t3 = backend.get_task("job-killed").unwrap();
        assert_eq!(t3.state, TaskState::Canceled);
    }

    #[test]
    fn test_list_tasks_and_filtering() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        create_test_job(&config, "job1", JobStatus::Done);
        create_test_job(&config, "job2", JobStatus::Failed);

        let all = backend.list_tasks(None, None).unwrap();
        assert_eq!(all.len(), 2);

        let filtered_ctx = backend.list_tasks(Some("job1"), None).unwrap();
        assert_eq!(filtered_ctx.len(), 1);
        assert_eq!(filtered_ctx[0].id, "job1");

        let filtered_state = backend.list_tasks(None, Some(&TaskState::Failed)).unwrap();
        assert_eq!(filtered_state.len(), 1);
        assert_eq!(filtered_state[0].id, "job2");
    }

    #[test]
    fn test_cancel_terminal_task_returns_not_cancelable() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        create_test_job(&config, "term-job", JobStatus::Done);
        let err = backend.cancel_task("term-job").unwrap_err();
        assert_eq!(err.code, "taskNotCancelable");
    }

    #[test]
    fn test_artifacts_from_job_log() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        let meta = create_test_job(&config, "job-with-res", JobStatus::Done);
        let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
        fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        fs::write(
            &log_path,
            r#"{"status":"SUCCESS","response":"Found the bug in file.rs"}"#,
        )
        .unwrap();

        let task = backend.get_task("job-with-res").unwrap();
        assert_eq!(
            task.status_message.as_deref(),
            Some("Found the bug in file.rs")
        );
        let arts = task.artifacts.expect("artifacts should be present");
        assert_eq!(arts.len(), 1);
        assert_eq!(arts[0].name.as_deref(), Some("result.txt"));
        assert_eq!(arts[0].parts[0].as_text(), Some("Found the bug in file.rs"));
    }

    #[tokio::test]
    async fn test_http_a2a_endpoints() {
        use crate::core::a2a::{a2a_router, A2aModel, AuthVerifier, WELL_KNOWN_AGENT_CARD_PATH};
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        create_test_job(&config, "job-http-1", JobStatus::Done);

        let model = A2aModel {
            name: "kubeadp-agent-worker-http-1".to_string(),
            description: "Test".to_string(),
            version: "0.1.0".to_string(),
            skills: vec![],
            url: "http://localhost:8002".to_string(),
        };
        let backend = Arc::new(JobTaskBackend::new(config));
        let auth: AuthVerifier = Arc::new(|headers: &axum::http::HeaderMap| {
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v == "Bearer http-token")
        });
        let app = a2a_router(model, backend, None, Some(auth));
        let bearer = |builder: axum::http::request::Builder| {
            builder.header("authorization", "Bearer http-token")
        };

        // 1. Agent card (public, no auth)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(WELL_KNOWN_AGENT_CARD_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let card: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(card["name"], "kubeadp-agent-worker-http-1");

        // 2. GET /tasks/job-http-1 (authenticated)
        let res = app
            .clone()
            .oneshot(
                bearer(Request::builder())
                    .uri("/tasks/job-http-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 3. GET /tasks/nonexistent (authenticated) -> 404
        let res = app
            .clone()
            .oneshot(
                bearer(Request::builder())
                    .uri("/tasks/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // 4. POST /rpc, unauthenticated -> 401 (the auth layer runs before
        // routing, so an unauthenticated peer cannot probe which paths exist;
        // JSON-RPC /rpc is not mounted)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // 5. POST /message:send with an empty body, UNAUTHENTICATED -> 401.
        // Authentication is checked before the body is parsed/validated, so
        // this answers 401 (not the body's "message must address a task or
        // context" 400).
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/message:send")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // 6. POST /message:send with an empty body, AUTHENTICATED -> 400: the
        // message names no task/context (INVALID_PARAMS).
        let res = app
            .clone()
            .oneshot(
                bearer(Request::builder())
                    .method("POST")
                    .uri("/message:send")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_cancel_task_exact_match_only() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        // Create tasks whose IDs are prefixes of each other or contain % and _
        create_test_job(&config, "job-prefix", JobStatus::Done);
        create_test_job(&config, "job-prefix-extra", JobStatus::Done);
        create_test_job(&config, "job%1", JobStatus::Done);
        create_test_job(&config, "job_1", JobStatus::Done);

        // Cancel with prefix "job-pre" must NOT resolve "job-prefix" -> taskNotFound
        let err = backend.cancel_task("job-pre").unwrap_err();
        assert_eq!(err.code, "taskNotFound");

        // Cancel with prefix "job-prefix" matches "job-prefix" exactly, but NOT "job-prefix-extra"
        let err = backend.cancel_task("job-prefix").unwrap_err();
        assert_eq!(err.code, "taskNotCancelable"); // because it's JobStatus::Done

        // Cancel with "%" or "_" wildcard attempts must NOT match other jobs
        let err = backend.cancel_task("job%").unwrap_err();
        assert_eq!(err.code, "taskNotFound");

        let err = backend.cancel_task("job_").unwrap_err();
        assert_eq!(err.code, "taskNotFound");

        // Exact match for "job%1" finds the single job
        let err = backend.cancel_task("job%1").unwrap_err();
        assert_eq!(err.code, "taskNotCancelable");

        // Exact match for "job_1" finds the single job
        let err = backend.cancel_task("job_1").unwrap_err();
        assert_eq!(err.code, "taskNotCancelable");
    }

    // ---- JobMessageSink -------------------------------------------------

    const CONV: &str = "550e8400-e29b-41d4-a716-446655440000";

    /// A fake agy that records its argv and prints a structured SUCCESS.
    fn write_fake_agy(bin: &std::path::Path, worktree: &std::path::Path) {
        let script = format!(
            "#!/bin/sh\n{{ printf '%s\\n' \"$@\" > {argv}; echo '{{\"status\":\"SUCCESS\",\"response\":\"ok\"}}'\n}}\n",
            argv = worktree.join("argv.txt").display(),
        );
        crate::core::spawn::write_executable_script(bin, &script).unwrap();
    }

    /// Saves a DONE job (exit 0 + structured SUCCESS + a conversation id) whose
    /// worktree exists: the minimum a `JobMessageSink` resume can continue.
    fn save_resumable_done_job(
        temp: &TempDir,
        job_id: &str,
        min_secs: u64,
    ) -> (Config, std::path::PathBuf) {
        let state_root = temp.path().join("state");
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: state_root.clone(),
            workspace_root: temp.path().join("ws"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            agy_path: temp.path().join("fake-agy"),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            a2a_interrupt_min_secs: min_secs,
            ..Default::default()
        };
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let meta = JobMetadata {
            job_id: job_id.to_string(),
            project: "p".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: worktree.clone(),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: Some(CONV.to_string()),
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(0),
            status: Some(JobStatus::Done),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(&state_root).unwrap();
        let log_path = JobMetadata::log_path(&state_root, job_id);
        std::fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        std::fs::write(
            &log_path,
            "{\"status\":\"SUCCESS\",\"response\":\"done ok\"}\n",
        )
        .unwrap();
        (config, worktree)
    }

    #[test]
    fn sink_unknown_job_returns_task_not_found() {
        let temp = TempDir::new().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        crate::core::job::ensure_state_store(&config.state_root).unwrap();
        let sink = JobMessageSink::new(Arc::new(config));
        let err = sink
            .deliver_with_meta("nope", "sender", "hello", "msg-1", DeliveryMeta::default())
            .unwrap_err();
        assert_eq!(err.code, a2a::error_code::TASK_NOT_FOUND);
    }

    #[test]
    fn sink_killed_job_is_never_woken() {
        let temp = TempDir::new().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let state_root = temp.path().join("state");
        let config = Config {
            state_root: state_root.clone(),
            ..Default::default()
        };
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let meta = JobMetadata {
            job_id: "killed".to_string(),
            project: "p".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: worktree,
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: Some(CONV.to_string()),
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(137),
            status: Some(JobStatus::Killed),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(&state_root).unwrap();

        let sink = JobMessageSink::new(Arc::new(config));
        let err = sink
            .deliver_with_meta(
                "killed",
                "sender",
                "hello",
                "msg-1",
                DeliveryMeta::default(),
            )
            .unwrap_err();
        assert_eq!(err.code, a2a::error_code::INVALID_PARAMS);
        assert!(
            err.message.contains("killed"),
            "the refusal must name the killed state, got: {}",
            err.message
        );
    }

    #[test]
    fn sink_worktree_gone_returns_task_not_found() {
        let temp = TempDir::new().unwrap();
        let state_root = temp.path().join("state");
        let config = Config {
            state_root: state_root.clone(),
            ..Default::default()
        };
        crate::core::job::ensure_state_store(&state_root).unwrap();
        // A job whose worktree directory does not exist (cleaned/discarded).
        let meta = JobMetadata {
            job_id: "ghost".to_string(),
            project: "p".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: temp.path().join("does-not-exist"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: Some(CONV.to_string()),
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(0),
            status: Some(JobStatus::Done),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(&state_root).unwrap();

        let sink = JobMessageSink::new(Arc::new(config));
        let err = sink
            .deliver_with_meta("ghost", "sender", "hello", "msg-1", DeliveryMeta::default())
            .unwrap_err();
        assert_eq!(err.code, a2a::error_code::TASK_NOT_FOUND);
    }

    #[test]
    fn sink_rate_limits_second_interrupt_within_interval() {
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "ratelimit", 3600);
        write_fake_agy(&config.agy_path, &worktree);
        let sink = JobMessageSink::new(Arc::new(config));

        let interrupt = DeliveryMeta {
            hop: 1,
            interrupt: true,
        };
        assert!(
            sink.deliver_with_meta("ratelimit", "sender", "first", "m1", interrupt)
                .is_ok(),
            "the first interrupt must resume the job"
        );
        // A second interrupt within the configured interval is rate-limited
        // BEFORE enqueueing or consuming the message id, so the caller can
        // retry the same message id after the window expires.
        let err = sink
            .deliver_with_meta("ratelimit", "sender", "second", "m2", interrupt)
            .unwrap_err();
        assert_eq!(err.code, a2a::error_code::INVALID_PARAMS);
        assert!(
            err.message.contains("rate limit"),
            "the refusal must say it is rate-limited, got: {}",
            err.message
        );
        assert!(
            !sink.is_seen("ratelimit", "m2"),
            "a rate-limited interrupt must not consume the message id"
        );
        assert!(
            sink.inbox("ratelimit").pending().is_empty(),
            "a rate-limited interrupt must not enqueue before the storm limit check"
        );
    }

    #[test]
    fn sink_applies_a_message_id_only_once() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "idem", 0);
        write_fake_agy(&config.agy_path, &worktree);
        let state_root = config.state_root.clone();
        let sink = JobMessageSink::new(Arc::new(config));

        let plain = DeliveryMeta::default();
        assert!(sink
            .deliver_with_meta("idem", "sender", "hello", "m1", plain)
            .is_ok());
        // The same A2A message id delivered again is a no-op (not re-queued).
        assert!(
            sink.deliver_with_meta("idem", "sender", "hello", "m1", plain)
                .is_ok(),
            "a duplicate message id must be accepted as a no-op"
        );
        // Exactly ONE message sits in the run's durable inbox (the dedupe
        // store) and nothing was resumed yet (the ladder escalates only past
        // the ack window).
        assert_eq!(sink.inbox("idem").pending().len(), 1);
        let after = JobMetadata::load(&state_root, "idem").unwrap();
        assert_eq!(
            after.retry_count, 0,
            "a plain delivery must not resume anything yet, retry_count = {}",
            after.retry_count
        );
    }

    #[test]
    fn sink_deliver_queues_and_the_ladder_resumes_done_job_at_the_idle_turn() {
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "e2e", 0);
        // A wide ack window (far larger than any test delay) so the message
        // only escalates once this test ages it past the window explicitly;
        // force_secs stays 0 (never kill).
        let config = Config {
            a2a_ack_secs: 300,
            ..config
        };
        let agy = config.agy_path.clone();
        write_fake_agy(&agy, &worktree);
        let state_root = config.state_root.clone();
        let sink = JobMessageSink::new(Arc::new(config));
        // Drive the ladder only through the explicit pass below: without this
        // the background watcher could resume the job on its own 1 s wall-clock
        // poll before this (possibly slow) test thread reaches the age/pass.
        sink.set_test_ladder_disabled(true);

        // Deliver: the message is queued (sender task submitted), nothing is
        // resumed yet.
        assert!(sink
            .deliver_with_meta(
                "e2e",
                "other-agent",
                "please fix the flake",
                "m1",
                DeliveryMeta::default()
            )
            .is_ok());
        assert_eq!(sink.inbox("e2e").pending().len(), 1);

        // Age past the ack window: the ladder escalates to the idle turn (the
        // job is done, so the next turn is a fresh resume) and the injection
        // is the ack. force_secs=0: never an interrupt.
        age_pending_message(&sink.inbox("e2e"), 301);
        let slot = JobSlot::new();
        assert_eq!(
            JobMessageSink::ladder_pass(&sink.config, &slot, "e2e", false),
            LadderStep::IdleTurn
        );
        assert!(sink.inbox("e2e").pending().is_empty());

        // The row moved to a new attempt carrying the message as the follow-up
        // prompt, and the conversation id is preserved (the same conversation
        // continues).
        let after = JobMetadata::load(&state_root, "e2e").unwrap();
        assert_eq!(after.retry_count, 1, "the resume must claim a new attempt");
        assert!(
            after
                .retry_prompt
                .as_deref()
                .is_some_and(|p| p.contains("please fix the flake")),
            "the follow-up prompt must carry the delivered message, got: {:?}",
            after.retry_prompt
        );
        assert_eq!(
            after.conversation_id.as_deref(),
            Some(CONV),
            "the resume must continue the same conversation"
        );

        // The resumed turn actually ran: the fake agy recorded its argv and its
        // SUCCESS line lands in the log.
        let log_path = JobMetadata::log_path(&state_root, "e2e");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let argv = std::fs::read_to_string(worktree.join("argv.txt")).unwrap_or_default();
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            if argv.contains("please fix the flake") && log.contains("SUCCESS") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the resumed turn never reached agy / its SUCCESS line never landed"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// Ages every pending message past `seconds` (rewrites the stored
    /// `queued_at` in the run's shared state database).
    fn age_pending_message(inbox: &Inbox, seconds: u64) {
        let now = crate::core::a2a::inbox::now_unix();
        let (run_id,) = (inbox.run_id(),);
        let _ = inbox.with_conn(|conn| {
            conn.execute(
                "UPDATE a2a_message SET queued_at=?1 WHERE run_id=?2 AND fake=?3 AND injected_at IS NULL",
                rusqlite::params![now.saturating_sub(seconds), run_id, i64::from(inbox.fake())],
            )
            .map_err(|e| format!("failed to age the A2A messages: {e}"))?;
            Ok(())
        });
    }

    #[test]
    fn concurrent_ladder_passes_resume_exactly_once() {
        // Regression (the double-resume race): two concurrent ladder passes
        // for the SAME job and the SAME pending message must never BOTH claim
        // an attempt. The background watcher (`ensure_ladder`) and a manual
        // `ladder_pass` can both reach the idle turn; the atomic resume claim
        // (`Inbox::claim_for_resume`) plus the per-job gate must make exactly
        // one of them resume. Run it many times to shake out the ordering.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        for i in 0..20 {
            let temp = TempDir::new().unwrap();
            let job_id = format!("concur-{i}");
            let (config, worktree) = save_resumable_done_job(&temp, &job_id, 0);
            // A wide ack window (far larger than any test delay) so the message
            // only escalates once this test ages it past the window explicitly;
            // force_secs stays 0 (never kill).
            let config = Config {
                a2a_ack_secs: 300,
                ..config
            };
            write_fake_agy(&config.agy_path, &worktree);
            let state_root = config.state_root.clone();
            let sink = JobMessageSink::new(Arc::new(config));
            // Drive the ladder only through the two explicit passes below:
            // without this the background watcher could resume the job on its
            // own 1 s wall-clock poll and leave the two racing passes with
            // nothing to claim (0 pending) before they start.
            sink.set_test_ladder_disabled(true);
            let sink = Arc::new(sink);

            // Deliver the message and age it past the ack window: the idle
            // turn is the escalation that resumes the (now DONE) job.
            assert!(sink
                .deliver_with_meta(
                    &job_id,
                    "selma",
                    "please fix the concurrency flake",
                    "cm1",
                    DeliveryMeta::default(),
                )
                .is_ok());
            age_pending_message(&sink.inbox(&job_id), 301);
            assert_eq!(sink.inbox(&job_id).pending().len(), 1);

            // Two threads race the same ladder pass on the same message.
            // The slot is looked up once (as the sink does); the two races
            // share it exactly like the watcher and a manual pass in
            // production.
            let slot = sink.slot(&job_id);
            let config = Arc::clone(&sink.config);
            let config2 = Arc::clone(&sink.config);
            let j1 = job_id.clone();
            let j2 = job_id.clone();
            let s1 = Arc::clone(&slot);
            let s2 = Arc::clone(&slot);
            let t1 =
                std::thread::spawn(move || JobMessageSink::ladder_pass(&config, &s1, &j1, false));
            let t2 =
                std::thread::spawn(move || JobMessageSink::ladder_pass(&config2, &s2, &j2, false));
            let step1 = t1.join().unwrap();
            let step2 = t2.join().unwrap();
            // Exactly ONE resume must have claimed the new attempt, no matter
            // which pass won (the loser steps Injected without resuming).
            let after = JobMetadata::load(&state_root, &job_id).unwrap();
            assert_eq!(
                after.retry_count, 1,
                "iteration {i}: exactly one of the two concurrent passes must resume, got retry_count = {}, steps = {step1:?}/{step2:?}",
                after.retry_count,
            );
            assert!(
                after
                    .retry_prompt
                    .as_deref()
                    .is_some_and(|p| p.contains("please fix the concurrency flake")),
                "iteration {i}: the winner's resume must carry the pending message, got: {:?}",
                after.retry_prompt
            );
            // Both passes draining here must leave nothing claimed or queued
            // (the loser's drain is over the already-injected row).
            assert!(
                sink.inbox(&job_id).pending().is_empty(),
                "iteration {i}: the message must be acknowledged (injected), not left pending"
            );
        }
    }

    #[test]
    fn ladder_expires_the_sender_task_after_the_ttl() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "expire", 0);
        // Wide ack window and a 1 h TTL (far larger than any test delay); the
        // test ages the message explicitly, so nothing depends on wall-clock
        // timing.
        let config = Config {
            a2a_ack_secs: 300,
            a2a_inbox_ttl_secs: 3600,
            ..config
        };
        write_fake_agy(&config.agy_path, &worktree);
        let sink = JobMessageSink::new(Arc::new(config.clone()));
        // Drive the ladder only through the explicit passes below (no
        // background watcher racing this test thread).
        sink.set_test_ladder_disabled(true);

        // A BUSY (running) job: the idle turn is unavailable, force_secs is
        // 0 (never kill) -- queued until the TTL expires it.
        save_running_job(&config, "expire", &worktree);
        sink.deliver_with_meta(
            "expire",
            "sender",
            "busy member message",
            "m1",
            DeliveryMeta::default(),
        )
        .unwrap();
        // Inside the TTL (900 s < 3600 s): still queued.
        age_pending_message(&sink.inbox("expire"), 900);
        let slot = JobSlot::new();
        assert_eq!(
            JobMessageSink::ladder_pass(&sink.config, &slot, "expire", false),
            LadderStep::Queued,
            "busy run inside the ttl: queued"
        );
        // Past the TTL (3601 s >= 3600 s): the message expires.
        age_pending_message(&sink.inbox("expire"), 3601);
        assert!(matches!(
            JobMessageSink::ladder_pass(&sink.config, &slot, "expire", false),
            LadderStep::Expired { .. }
        ));
    }

    #[test]
    fn rejected_delivery_stays_retryable_under_the_same_message_id() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "retry", 0);
        write_fake_agy(&config.agy_path, &worktree);
        let sink = JobMessageSink::new(Arc::new(config));
        let plain = DeliveryMeta::default();
        // The first attempt addresses a job that does not exist: rejected,
        // nothing stored.
        assert!(sink
            .deliver_with_meta("ghost", "sender", "hello", "m1", plain)
            .is_err());
        // The rejected attempt must not be remembered as delivered, and the
        // idempotency key is scoped to the addressed job: the same message id
        // for a real job is stored, not answered as a silent no-op.
        sink.deliver_with_meta("retry", "sender", "hello", "m1", plain)
            .unwrap();
        assert_eq!(sink.inbox("retry").pending().len(), 1);
    }

    #[test]
    fn ladder_never_wakes_a_job_killed_after_the_enqueue() {
        let temp = TempDir::new().unwrap();
        // The default ack window (45 s) keeps the background watcher from
        // resuming the job before the kill below is recorded.
        let (config, worktree) = save_resumable_done_job(&temp, "latekill", 0);
        write_fake_agy(&config.agy_path, &worktree);
        let state_root = config.state_root.clone();
        let sink = JobMessageSink::new(Arc::new(config));
        sink.deliver_with_meta("latekill", "sender", "hello", "m1", DeliveryMeta::default())
            .unwrap();
        // The operator kills the job while the message is still queued.
        let mut meta = JobMetadata::load(&state_root, "latekill").unwrap();
        meta.status = Some(JobStatus::Killed);
        meta.save(&state_root).unwrap();
        age_pending_message(&sink.inbox("latekill"), 100);

        let slot = JobSlot::new();
        assert!(matches!(
            JobMessageSink::ladder_pass(&sink.config, &slot, "latekill", false),
            LadderStep::Expired { .. }
        ));
        let after = JobMetadata::load(&state_root, "latekill").unwrap();
        assert_eq!(after.retry_count, 0, "a killed job must not be resumed");
        assert_eq!(sink.inbox("latekill").uninjected_count(), 0);
        let inbox_dir = crate::core::a2a::inbox::inbox_dir_for(&state_root, "latekill");
        assert!(crate::core::a2a::delivery::read_expiry(&inbox_dir).is_some());
    }

    #[test]
    fn an_expired_message_is_removed_and_does_not_block_the_next_one() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "unblock", 0);
        // Wide ack window and a 1 h TTL (far larger than any test delay); the
        // test ages the message explicitly, so nothing depends on wall-clock
        // timing.
        let config = Config {
            a2a_ack_secs: 300,
            a2a_inbox_ttl_secs: 3600,
            ..config
        };
        write_fake_agy(&config.agy_path, &worktree);
        let sink = JobMessageSink::new(Arc::new(config.clone()));
        // Drive the ladder only through the explicit passes below (no
        // background watcher racing this test thread).
        sink.set_test_ladder_disabled(true);
        // A BUSY job: nothing is delivered, the message runs into the TTL.
        save_running_job(&config, "unblock", &worktree);
        let plain = DeliveryMeta::default();
        sink.deliver_with_meta("unblock", "sender", "first", "m1", plain)
            .unwrap();
        // Past the TTL (3601 s >= 3600 s): the first message expires.
        age_pending_message(&sink.inbox("unblock"), 3601);
        let slot = JobSlot::new();
        assert!(matches!(
            JobMessageSink::ladder_pass(&sink.config, &slot, "unblock", false),
            LadderStep::Expired { .. }
        ));
        // The expired row is gone: no hook / wait_message can still serve a
        // message whose sender was told it failed.
        assert_eq!(sink.inbox("unblock").uninjected_count(), 0);

        // A fresh message is handled on its own age, not expired behind m1.
        sink.deliver_with_meta("unblock", "sender", "second", "m2", plain)
            .unwrap();
        assert_eq!(
            JobMessageSink::ladder_pass(&sink.config, &slot, "unblock", false),
            LadderStep::Queued
        );
        assert_eq!(sink.inbox("unblock").pending().len(), 1);
    }

    #[test]
    fn recover_pending_starts_a_watcher_for_messages_queued_before_a_restart() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "recover", 0);
        write_fake_agy(&config.agy_path, &worktree);
        save_running_job(&config, "recover", &worktree);
        // A message left in the durable inbox by the previous process.
        let dir = crate::core::a2a::inbox::inbox_dir_for(&config.state_root, "recover");
        Inbox::new(&dir)
            .enqueue(QueuedMessage {
                message_id: "m1".to_string(),
                sender: "sender".to_string(),
                hop: 0,
                context_id: None,
                task_id: None,
                framed: "left over".to_string(),
                queued_at: crate::core::a2a::inbox::now_unix(),
            })
            .unwrap();

        let sink = JobMessageSink::new(Arc::new(config));
        assert!(!sink.ladders.is_active("recover"));
        sink.recover_pending();
        assert!(
            sink.ladders.is_active("recover"),
            "a queued message must get a ladder watcher after a restart"
        );
    }

    // ---- Live stdin delivery ---------------------------------------------

    /// A fake agy that records its argv and reads NDJSON `user` messages from
    /// stdin, appending each delivered content line to `received.txt` (one per
    /// line), then prints a structured SUCCESS and exits. Used to verify the
    /// live fast path wrote directly into a RUNNING process.
    fn write_fake_agy_live(bin: &std::path::Path, worktree: &std::path::Path) {
        let script = format!(
            "#!/bin/sh\n\
             {{\n\
             \x20 printf '%s\\n' \"$@\" > {argv}\n\
             \x20 while IFS= read -r line; do\n\
             \x20\x20 printf '%s\\n' \"$line\" >> {received}\n\
             \x20 done\n\
             \x20 echo '{{\"status\":\"SUCCESS\",\"response\":\"ok\",\"conversation_id\":\"{conv}\"}}'\n\
             }}\n",
            argv = worktree.join("argv.txt").display(),
            received = worktree.join("received.txt").display(),
            conv = CONV,
        );
        crate::core::spawn::write_executable_script(bin, &script).unwrap();
    }

    /// Saves a RUNNING job row (live pid, matching start time via `this`
    /// process) whose worktree exists and whose agy path is already wired, so
    /// the live channel can be registered against it and `deliver` takes the
    /// fast path. The row's pid points at the CURRENT test process (alive), so
    /// `is_alive_for` and `compute_status == Running` both pass.
    fn save_running_job(config: &Config, job_id: &str, worktree: &std::path::Path) -> JobMetadata {
        let meta = JobMetadata {
            job_id: job_id.to_string(),
            project: "p".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: worktree.to_path_buf(),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: Some(CONV.to_string()),
            retry_count: 0,
            pid: Some(std::process::id()),
            // The test process's own start time, matching the channel.
            pid_start_time: crate::core::job::get_process_start_time(std::process::id()),
            exit_code: None,
            status: Some(JobStatus::Running),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(&config.state_root).unwrap();
        let log_path = JobMetadata::log_path(&config.state_root, job_id);
        if let Some(parent) = log_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(
            &log_path,
            "{\"status\":\"SUCCESS\",\"response\":\"live-ok\"}\n",
        );
        meta
    }

    /// Registers a live channel for `job_id` whose write end is fed to a
    /// `/bin/sh` reader that appends its stdin to `received.txt` in the
    /// worktree (so a delivery is observable), keyed by the SAME pid/start
    /// time the job row carries (the current test process, which is alive).
    /// Returns the reader child for cleanup.
    fn register_live_channel_for_test(
        config: &Config,
        job_id: &str,
        meta: &JobMetadata,
        worktree: &std::path::Path,
    ) -> std::process::Child {
        let received = worktree.join("received.txt");
        let received_s = received.to_string_lossy().into_owned();
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("cat >> {received_s}"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn reader");
        let stdin = child.stdin.take().expect("piped stdin");
        crate::core::a2a_live::register(
            job_id,
            std::process::id(),
            meta.pid_start_time,
            stdin,
            JobMetadata::log_path(&config.state_root, job_id),
        );
        child
    }

    #[test]
    fn sink_live_delivery_writes_to_running_stdin() {
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        crate::core::a2a_live::with_isolated_live(|| {
            let temp = TempDir::new().unwrap();
            let state_root = temp.path().join("state");
            let worktree = temp.path().join("wt");
            std::fs::create_dir_all(&worktree).unwrap();
            crate::core::job::ensure_state_store(&state_root).unwrap();
            let config = Config {
                state_root: state_root.clone(),
                workspace_root: temp.path().join("ws"),
                repos_root: temp.path().join("ws").join(".agent-repos"),
                agy_path: temp.path().join("fake-agy"),
                claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
                opencode_path: std::path::PathBuf::from("opencode"),
                a2a_interrupt_min_secs: 0,
                a2a_live_stdin: true,
                // A wide ack window (far larger than any test delay) so the
                // message only escalates once this test ages it explicitly.
                a2a_ack_secs: 300,
                ..Default::default()
            };
            let meta = save_running_job(&config, "livedel", &worktree);
            write_fake_agy_live(&config.agy_path, &worktree);
            let mut reader = register_live_channel_for_test(&config, "livedel", &meta, &worktree);

            let sink = JobMessageSink::new(Arc::new(config));
            // Drive the ladder only through the explicit pass below: without
            // this the background watcher could write the stdin line on its
            // own 1 s wall-clock poll and leave the manual pass with
            // `Injected` instead of `IdleTurn`.
            sink.set_test_ladder_disabled(true);
            assert!(sink
                .deliver_with_meta(
                    "livedel",
                    "peer",
                    "please live-deliver",
                    "m1",
                    DeliveryMeta::default()
                )
                .is_ok());
            // Age past the ack window: the idle_turn step writes the stdin
            // line into the RUNNING process.
            age_pending_message(&sink.inbox("livedel"), 301);
            let slot = JobSlot::new();
            assert_eq!(
                JobMessageSink::ladder_pass(&sink.config, &slot, "livedel", false),
                LadderStep::IdleTurn
            );
            assert!(sink.inbox("livedel").pending().is_empty());
            // The running process actually received the framed user line.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let content =
                    std::fs::read_to_string(worktree.join("received.txt")).unwrap_or_default();
                if content.contains("please live-deliver") {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the live process never received the message; got: {content:?}"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            let content = std::fs::read_to_string(worktree.join("received.txt")).unwrap();
            for line in content.lines() {
                let v: serde_json::Value = serde_json::from_str(line).unwrap();
                assert_eq!(v["event"], "user");
            }
            // No resume was triggered: retry_count is unchanged.
            let after = JobMetadata::load(&state_root, "livedel").unwrap();
            assert_eq!(
                after.retry_count, 0,
                "the live stdin delivery must not claim a resume attempt"
            );
            // Idempotency still applies to a repeated message id.
            assert!(sink
                .deliver_with_meta("livedel", "peer", "dup", "m1", DeliveryMeta::default())
                .is_ok());
            let _ = reader.kill();
            let _ = reader.wait();
            crate::core::a2a_live::close_for("livedel");
        });
    }

    #[test]
    fn slot_map_is_bounded_and_evicts_oldest_first() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "slotbound", 0);
        write_fake_agy(&config.agy_path, &worktree);
        let sink = JobMessageSink::new(Arc::new(config));

        // Create slots for MAX_SLOTS + margin distinct jobs. `slot` is a
        // private helper; drive it through `deliver_with_meta` (it calls
        // `slot` at least for the `interrupt` and normal paths) -- but that
        // also enqueues durable messages. To keep it cheap, exercise the
        // sink's internal map directly through the recorded slot entries.
        for i in 0..(MAX_SLOTS + 50) {
            let job = format!("j{i}");
            sink.slot(&job);
        }

        let map = sink.slots.lock().unwrap();
        assert!(
            map.len() <= MAX_SLOTS,
            "slot map grew beyond the bound: {} > {}",
            map.len(),
            MAX_SLOTS
        );
        assert!(!map.contains_key("j0"), "oldest slot must be evicted first");
        assert!(
            map.contains_key(&format!("j{}", MAX_SLOTS + 49)),
            "the most recent slot must be retained"
        );
        // Re-creating an evicted job's slot works (gate recreated on demand).
        drop(map);
        let recreated = sink.slot("j0");
        assert!(recreated.gate.try_lock().is_ok());
    }
}

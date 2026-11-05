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
//! - Artifacts: the job's final response text (if present), plus the git diff
//!   text (if available and cheap).
//! - `ListTasks`: lists jobs, optionally filtered by contextId (matching job_id)
//!   and/or A2A TaskState.
//! - `GetTask`: returns one task by exact job ID.
//! - `CancelTask`: kills the job via `crate::core::commands::kill::run_exact`.

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
use a2a::A2AError as SdkA2AError;

/// Maximum number of in-memory (coalesced) pending messages held per job while
/// a resume is in flight; overflow drops the oldest (bounded, no persistent
/// inbox).
const MAX_COALESCE_MSGS: usize = 8;
/// Maximum combined bytes of the per-job in-memory pending buffer.
const MAX_COALESCE_BYTES: usize = 8 * 1024;
/// Bound on the sink's global idempotency cache of delivered message ids.
const SEEN_CACHE_CAP: usize = 4096;

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
        let state = map_job_status(status);

        let mut status_message = None;
        let mut error = None;
        let mut artifacts_vec = Vec::new();

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

        // Error message for failed/terminal states
        if status == JobStatus::Failed
            || status == JobStatus::Incomplete
            || status == JobStatus::QuotaExhausted
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

/// Per-job resume state: a `gate` mutex that serialises at most one resume at
/// a time for the job, the bounded coalesced `pending` buffer (messages that
/// queued while a resume was in flight), and the `last_interrupt` timestamp
/// (the per-job storm limit). Never persisted (there is no inbox).
struct JobSlot {
    gate: Mutex<()>,
    pending: Mutex<VecDeque<String>>,
    last_interrupt: Mutex<Option<Instant>>,
}

impl JobSlot {
    fn new() -> Self {
        Self {
            gate: Mutex::new(()),
            pending: Mutex::new(VecDeque::new()),
            last_interrupt: Mutex::new(None),
        }
    }
}

/// A2A `SendMessage` sink for the worker: an incoming A2A message is injected
/// as the NEXT conversation turn of the addressed job (interrupt-and-resume).
///
/// - A RUNNING job's `agy` process is stopped cleanly (SIGTERM group, grace,
///   SIGKILL) exactly like `kill` does, then the same conversation is resumed
///   with the framed message as a follow-up turn through the retry code path
///   (NOT marked user-killed or failed).
/// - A FINISHED job (done/failed/incomplete/self-terminated, not cleaned or
///   discarded) is resumed the same way (a fresh follow-up turn).
/// - An operator-KILLED job is never woken: delivery is refused with a clear
///   error. Unknown / cleaned / discarded jobs answer `TASK_NOT_FOUND`.
///
/// Concurrency: at most one resume is in flight per job; concurrent messages
/// coalesce in memory into the next resume prompt (bounded, oldest dropped).
/// A per-job minimum interval (`a2a_interrupt_min_secs`, 0 = off) plus the
/// caller's existing per-jobset hop-token budget rate-limit message storms; a
/// throttled delivery is rejected with a rate-limit error. A repeated A2A
/// message id is applied at most once (idempotency).
pub struct JobMessageSink {
    config: Arc<Config>,
    slots: Mutex<HashMap<String, Arc<JobSlot>>>,
    seen: Mutex<VecDeque<String>>,
}

impl JobMessageSink {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            slots: Mutex::new(HashMap::new()),
            seen: Mutex::new(VecDeque::new()),
        }
    }

    /// The per-job slot, creating it on first use. The global lock is held only
    /// long enough to look up / insert the `Arc`; the job's own `gate`
    /// (acquired by the caller) is what serialises a resume for that job.
    fn slot(&self, task_id: &str) -> Arc<JobSlot> {
        let mut map = self.slots.lock().expect("slots lock poisoned");
        map.entry(task_id.to_string())
            .or_insert_with(|| Arc::new(JobSlot::new()))
            .clone()
    }

    /// True when this message id has already been delivered (idempotency).
    /// On `true` it is not re-added.
    fn mark_seen(&self, message_id: &str) -> bool {
        let mut seen = self.seen.lock().expect("seen lock poisoned");
        if seen.iter().any(|id| id == message_id) {
            return true;
        }
        seen.push_back(message_id.to_string());
        while seen.len() > SEEN_CACHE_CAP {
            seen.pop_front();
        }
        false
    }
}

impl JobMessageSink {
    /// Enqueue `msg` into the per-job coalesce buffer, dropping the oldest with
    /// a note to stay within the bounds. Returns `Err` if a single message
    /// alone exceeds the byte cap (it would never fit and must be refused).
    fn push_coalesced(pending: &mut VecDeque<String>, msg: String) -> Result<(), SdkA2AError> {
        if msg.len() > MAX_COALESCE_BYTES {
            return Err(SdkA2AError::invalid_params("message text is too large"));
        }
        pending.push_back(msg);
        let mut dropped = false;
        // Enforce the byte bound (drop oldest, keep newest).
        while pending.iter().map(|s| s.len()).sum::<usize>() > MAX_COALESCE_BYTES
            && pending.len() > 1
        {
            pending.pop_front();
            dropped = true;
        }
        // Enforce the count bound.
        while pending.len() > MAX_COALESCE_MSGS {
            pending.pop_front();
            dropped = true;
        }
        // A single drop note records that overflow happened (bounded, so at most
        // one; the note is appended after the trims and is itself tiny).
        if dropped {
            pending
                .push_back("[note] earlier message(s) dropped by the delivery buffer".to_string());
            while pending.len() > MAX_COALESCE_MSGS {
                pending.pop_front();
            }
        }
        Ok(())
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

    /// Drives the per-job resume loop under the held gate (F7): drains the
    /// coalesced `pending` buffer, resumes with (the coalesced messages plus)
    /// `text` on the first pass, then keeps draining and resuming until
    /// `pending` is empty. A message that arrives during an in-flight resume
    /// therefore still gets delivered on the next pass instead of being
    /// stranded. Stops at the first resume error and returns it.
    fn drain_and_resume<F>(job: &JobSlot, text: &str, resume: &F) -> Result<(), SdkA2AError>
    where
        F: Fn(&str) -> Result<(), SdkA2AError>,
    {
        let mut result = Ok(());
        let mut first = true;
        loop {
            let mut msgs: Vec<String> = {
                let mut pending = job.pending.lock().expect("pending lock poisoned");
                pending.drain(..).collect()
            };
            if first {
                msgs.push(text.to_string());
                first = false;
            }
            if msgs.is_empty() {
                break;
            }
            result = resume(&join_prompts(&msgs));
            if result.is_err() {
                break;
            }
        }
        result
    }
}

impl MessageSink for JobMessageSink {
    fn deliver(
        &self,
        task_id: &str,
        _sender: &str,
        text: &str,
        message_id: &str,
    ) -> Result<(), SdkA2AError> {
        // Idempotency: the same A2A message id is applied at most once. A
        // duplicate of an already-delivered id is accepted as a no-op (the
        // sender gets success, not an error), so a retry is harmless.
        if self.mark_seen(message_id) {
            return Ok(());
        }

        // Unknown job (or a cleaned/discarded row): TASK_NOT_FOUND. The handler
        // already validated existence, but the row can vanish (gc/clean) between
        // that check and delivery.
        let meta = match JobMetadata::resolve(&self.config.state_root, task_id) {
            Ok(m) => m,
            Err(_) => return Err(SdkA2AError::task_not_found(task_id)),
        };

        // An operator-killed job is never woken.
        if meta.status == Some(JobStatus::Killed) {
            return Err(SdkA2AError::invalid_params(format!(
                "job '{}' was killed by the operator and cannot be resumed with a message",
                task_id
            )));
        }

        // Worktree already gone (cleaned/discarded): TASK_NOT_FOUND.
        if !meta.worktree_path.exists() {
            return Err(SdkA2AError::task_not_found(task_id));
        }

        // The per-job gate serialises at most one resume at a time for this
        // job (and only this job -- other jobs are unaffected). A delivery that
        // cannot take the gate immediately means a resume is already in flight:
        // coalesce the message into the pending buffer and return success (it
        // rides the next resume). Coalescing is cheap (bounded deque), so it
        // never blocks a worker thread.
        let job = self.slot(task_id);
        let gate = match job.gate.try_lock() {
            Ok(g) => g,
            Err(_) => {
                let mut pending = job.pending.lock().expect("pending lock poisoned");
                return Self::push_coalesced(&mut pending, text.to_string());
            }
        };

        // Per-job storm limit: refuse a second interrupt younger than the
        // configured interval. The per-jobset hop-token budget (charged by the
        // caller's ingress) is an independent, coarser cap on the whole
        // jobset's message rate.
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

        // Drain any messages that queued (coalesced) while a previous resume
        // was in flight, in order, ahead of the current one, and bound the
        // combined prompt (oldest dropped first) so a burst cannot exceed the
        // argv prompt limit. After each resume completes, drain AGAIN and keep
        // resuming until the pending buffer is empty: a message that arrived
        // while THIS resume held the gate coalesced into `pending` (its
        // `deliver` could not take the gate), and without the post-resume
        // drain it would be stranded indefinitely. The buffer is bounded and
        // the per-job/per-jobset rate limits bound the rest, so the loop
        // terminates.
        let result = Self::drain_and_resume(&job, text, &|prompt| {
            *job.last_interrupt
                .lock()
                .expect("last_interrupt lock poisoned") = Some(Instant::now());
            Self::resume_with(&self.config, &meta, prompt)
        });
        drop(gate);
        result
    }
}

/// Joins the coalesced messages into a single next-turn prompt, bounding the
/// combined length (oldest dropped first) so a burst cannot exceed the argv
/// prompt limit. A single message longer than the cap is kept as-is (retry
/// will refuse it with the standard prompt-size error).
fn join_prompts(msgs: &[String]) -> String {
    const MAX: usize = crate::core::runner::PROMPT_ARGV_MAX_BYTES;
    if msgs.len() == 1 {
        return msgs[0].clone();
    }
    let mut out: Vec<String> = Vec::new();
    let mut bytes = 0usize;
    // Newest-first so the newest (most relevant) is kept; then reverse.
    for m in msgs.iter().rev() {
        if out.is_empty() {
            out.push(m.clone());
            bytes = m.len();
        } else if bytes + m.len() + 2 <= MAX {
            out.push(m.clone());
            bytes += m.len() + 2;
        } else {
            break;
        }
    }
    out.reverse();
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{open_state_db, JobStatus};
    use http_body_util::BodyExt;
    use std::os::unix::fs::PermissionsExt;
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
        std::fs::write(
            bin,
            format!(
                "#!/bin/sh\n{{ printf '%s\\n' \"$@\" > {argv}; echo '{{\"status\":\"SUCCESS\",\"response\":\"ok\"}}'\n}}\n",
                argv = worktree.join("argv.txt").display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755)).unwrap();
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
            .deliver("nope", "sender", "hello", "msg-1")
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
        };
        meta.save(&state_root).unwrap();

        let sink = JobMessageSink::new(Arc::new(config));
        let err = sink
            .deliver("killed", "sender", "hello", "msg-1")
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
        };
        meta.save(&state_root).unwrap();

        let sink = JobMessageSink::new(Arc::new(config));
        let err = sink
            .deliver("ghost", "sender", "hello", "msg-1")
            .unwrap_err();
        assert_eq!(err.code, a2a::error_code::TASK_NOT_FOUND);
    }

    #[test]
    fn sink_rate_limits_second_interrupt_within_interval() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "ratelimit", 3600);
        write_fake_agy(&config.agy_path, &worktree);
        let sink = JobMessageSink::new(Arc::new(config));

        // First interrupt: no recent interrupt, so it resumes.
        assert!(
            sink.deliver("ratelimit", "sender", "first", "m1").is_ok(),
            "the first interrupt must resume the job"
        );
        // A second interrupt within the configured interval is rate-limited.
        let err = sink
            .deliver("ratelimit", "sender", "second", "m2")
            .unwrap_err();
        assert_eq!(err.code, a2a::error_code::INVALID_PARAMS);
        assert!(
            err.message.contains("rate limit"),
            "the refusal must say it is rate-limited, got: {}",
            err.message
        );
    }

    #[test]
    fn sink_applies_a_message_id_only_once() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "idem", 0);
        write_fake_agy(&config.agy_path, &worktree);
        let state_root = config.state_root.clone();
        let sink = JobMessageSink::new(Arc::new(config));

        assert!(sink.deliver("idem", "sender", "hello", "m1").is_ok());
        // The same A2A message id delivered again is a no-op (not re-applied).
        assert!(
            sink.deliver("idem", "sender", "hello", "m1").is_ok(),
            "a duplicate message id must be accepted as a no-op"
        );
        // Only ONE resume was launched: retry_count bumped exactly once.
        let after = JobMetadata::load(&state_root, "idem").unwrap();
        assert_eq!(
            after.retry_count, 1,
            "a duplicated message id must not launch a second turn, retry_count = {}",
            after.retry_count
        );
    }

    #[test]
    fn sink_coalesce_buffer_is_bounded() {
        let mut pending = VecDeque::new();
        // 20 small messages must be capped at MAX_COALESCE_MSGS (8) with a note.
        for i in 0..20 {
            JobMessageSink::push_coalesced(&mut pending, format!("msg-{i}")).unwrap();
        }
        assert!(
            pending.len() <= MAX_COALESCE_MSGS,
            "the coalesce buffer must stay within {} entries, got {}",
            MAX_COALESCE_MSGS,
            pending.len()
        );
        assert!(
            pending.iter().any(|s| s.contains("dropped")),
            "overflow must be recorded with a drop note"
        );
        // A single oversized message is refused outright.
        let mut one = VecDeque::new();
        let big = "x".repeat(MAX_COALESCE_BYTES + 1);
        assert!(JobMessageSink::push_coalesced(&mut one, big).is_err());
    }

    #[test]
    fn sink_deliver_resumes_done_job_as_followup_turn() {
        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "e2e", 0);
        let agy = config.agy_path.clone();
        write_fake_agy(&agy, &worktree);
        let state_root = config.state_root.clone();
        let sink = JobMessageSink::new(Arc::new(config));

        assert!(
            sink.deliver("e2e", "other-agent", "please fix the flake", "m1")
                .is_ok(),
            "a message to a done job must resume it"
        );

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

    #[test]
    fn join_prompts_bounds_the_combined_length() {
        assert_eq!(join_prompts(&["only".to_string()]), "only");
        let mut msgs = Vec::new();
        let pad = "y".repeat(2000);
        for i in 0..100 {
            let mut s = format!("m{i}:");
            s.push_str(&pad);
            msgs.push(s);
        }
        let joined = join_prompts(&msgs);
        assert!(
            joined.len() <= crate::core::runner::PROMPT_ARGV_MAX_BYTES,
            "the joined prompt must stay within the argv limit"
        );
        // The newest message is always kept.
        assert!(joined.contains("m99:"));
    }

    #[test]
    fn message_arriving_during_inflight_resume_is_not_stranded() {
        // F7: a message that arrives while a resume is in flight (its `deliver`
        // cannot take the per-job gate, so it coalesces into `pending`) must be
        // drained and delivered once the in-flight resume finishes, instead of
        // being stranded. The in-flight window is created by parking the job
        // monitor's post-spawn hook (which blocks PID recording, keeping the
        // gate held until the test releases it).
        use crate::core::runner::{clear_test_hooks, register_test_post_spawn_hook};
        use std::sync::{Condvar, Mutex};

        let temp = TempDir::new().unwrap();
        let (config, worktree) = save_resumable_done_job(&temp, "e2e", 0);
        let agy = config.agy_path.clone();
        write_fake_agy(&agy, &worktree);
        let state_root = config.state_root.clone();
        let sink = Arc::new(JobMessageSink::new(Arc::new(config)));

        let arrived = Arc::new((Mutex::new(0u32), Condvar::new()));
        let go = Arc::new((Mutex::new(false), Condvar::new()));
        {
            let arrived = Arc::clone(&arrived);
            let go = Arc::clone(&go);
            register_test_post_spawn_hook("e2e", move || {
                // Park the monitor of the FIRST spawn only (so PID recording --
                // and thus the held gate -- is delayed until the test releases
                // it); the resumed turn's own retry monitor passes through.
                let is_first = {
                    let (m, c) = &*arrived;
                    let mut n = m.lock().unwrap();
                    if *n == 0 {
                        *n += 1;
                        c.notify_all();
                        true
                    } else {
                        false
                    }
                };
                if is_first {
                    let (m, c) = &*go;
                    let mut g = m.lock().unwrap();
                    while !*g {
                        g = c.wait(g).unwrap();
                    }
                }
            });
        }

        // Thread A: resume "first" and hold the gate while the monitor parks.
        let job = Arc::clone(&sink);
        let a = std::thread::spawn(move || {
            job.deliver("e2e", "other-agent", "please fix the flake", "m1")
        });

        // Wait until the first resume's monitor is parked (gate held).
        {
            let (m, c) = &*arrived;
            let mut n = m.lock().unwrap();
            while *n == 0 {
                n = c.wait(n).unwrap();
            }
        }

        // Thread B: arrives mid-flight, cannot take the gate, must coalesce.
        let job = Arc::clone(&sink);
        let b = std::thread::spawn(move || {
            job.deliver("e2e", "other-agent", "and the second one too", "m2")
        });
        let b_ok = b.join().unwrap();
        assert!(
            b_ok.is_ok(),
            "a message arriving during an in-flight resume must coalesce and succeed, got: {b_ok:?}"
        );

        // Release the parked monitor; the drain loop must now pick up "m2"
        // and resume again.
        *go.0.lock().unwrap() = true;
        go.1.notify_all();
        assert!(
            a.join().unwrap().is_ok(),
            "the in-flight resume must succeed"
        );
        clear_test_hooks("e2e");

        // Both messages were delivered: two attempts claimed, two SUCCESS
        // lines in the log (one per resume), and no stranded pending message.
        let after = JobMetadata::load(&state_root, "e2e").unwrap();
        assert_eq!(
            after.retry_count, 2,
            "both deliveries must claim an attempt (second ride the post-resume drain), got retry_count={}",
            after.retry_count
        );
        let log_path = JobMetadata::log_path(&state_root, "e2e");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            if log.matches("SUCCESS").count() >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the second (drained) resume never ran agy; log: {log}"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(
            sink.slot("e2e").pending.lock().unwrap().is_empty(),
            "the pending buffer must be fully drained"
        );
    }
}

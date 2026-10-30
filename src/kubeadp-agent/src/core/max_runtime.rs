//! Server-side max-runtime enforcement for long-running jobs.
//!
//! A job may carry an opt-in maximum wall-clock runtime
//! (`JobMetadata::max_runtime`, set from `start --max-runtime` or the
//! server-wide `AGENT_RUN_MAX_RUNTIME`/config.toml `max_runtime` default).
//! Both are unset by default: with neither a limit exists and jobs run
//! unbounded exactly as before. The limit and the attempt's start anchor
//! (`JobMetadata::started_at`) are persisted on the job row, so enforcement
//! survives a server restart.
//!
//! The caller (the `agent-run-api` server) drives this on a single periodic
//! interval; [`enforce_once`] performs one scan-and-enforce pass. It only
//! ever acts on jobs that [`crate::core::job::compute_status`] reports as
//! `Running` with a live, identity-verified PID, and it terminates them
//! exactly the way [`crate::core::commands::kill`] does (SIGTERM the agy process
//! group, escalate to SIGKILL, then mark the row `killed`), adding a
//! `max runtime exceeded (<limit>)` line to the job log.
//!
//! Concurrency notes:
//! * Each due job is terminated in its OWN worker thread, so one stubborn
//!   job (the kill path can wait out its ~20 s grace period) does not delay
//!   enforcement of the others; the pass additionally stops COLLECTING
//!   after [`ENFORCE_PASS_BUDGET_SECS`] (unfinished workers continue in the
//!   background, still guarded by the conditional `mark_job_killed`).
//! * The `killed` state is written only after a post-termination RE-READ
//!   shows the job is still running, and only when the terminate call
//!   reports WE made the process go (`AlreadyGone` never marks), and
//!   `mark_job_killed` is itself a conditional UPDATE (refusing rows that
//!   finished successfully on its own -- status `done`/`killed` or a
//!   recorded exit code of 0), so a job that finished cleanly between the
//!   decision and the write keeps its final status. A row the runner
//!   already recorded with the signal-derived exit code (143) is ours: it
//!   still becomes `killed`/137.
//! * The log line is appended only after the mark was applied, at most once
//!   per job id (the in-memory [`logged_job_ids`] set), so a failing
//!   terminate does not re-append it on every 10 s tick.
//!
//! The pass is best-effort and panic-free: a DB error or a job that raced
//! into a newer retry attempt is skipped (with a stderr note), never an
//! error for the caller.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::core::config::Config;
use crate::core::job::{
    compute_status, mark_job_killed, max_runtime_kill_due, JobMetadata, JobStatus,
};

/// How long a single enforcement pass waits for the terminations it spawned
/// to settle. Each due job runs in its own worker thread (see the module
/// docs), so this bounds only how long the pass thread blocks collecting
/// results; an unfinished worker keeps running in the background and its
/// writes remain guarded by the conditional `mark_job_killed`.
pub(crate) const ENFORCE_PASS_BUDGET_SECS: u64 = 30;

/// Process-termination outcome: did WE make the process go, or was it
/// already gone (exited on its own between selection and the terminate
/// attempt), or did the kill path decline (identity mismatch, signal
/// error, job moved to a newer retry)? Only `Terminated` may lead to the
/// `killed` state -- and even then only after a fresh re-read shows the
/// job still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KillOutcome {
    /// We signaled the process (or it exited in response) and it is gone.
    Terminated,
    /// Nothing was terminated by this call: the process was already dead,
    /// or the kill path failed / refused.
    AlreadyGone,
}

/// A selected due job: everything the termination worker needs.
pub(crate) struct DueJob {
    state_root: PathBuf,
    job_id: String,
    pid: u32,
    pid_start_time: Option<u64>,
    retry_count: u32,
    limit: chrono::Duration,
    log_path: PathBuf,
}

/// Job ids whose `max runtime exceeded` log line has already been appended
/// (best-effort, in-memory). Without the dedup a race between two
/// overlapping passes could append the line twice for the same job; a
/// failing terminate must never cause it. The set only clears on a server
/// restart, at which point the log itself remains the durable record.
fn logged_job_ids() -> &'static Mutex<HashSet<String>> {
    static LOGGED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    LOGGED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn already_logged(job_id: &str) -> bool {
    logged_job_ids()
        .lock()
        .map(|set| set.contains(job_id))
        .unwrap_or(false)
}

fn remember_logged(job_id: &str) {
    if let Ok(mut set) = logged_job_ids().lock() {
        let _ = set.insert(job_id.to_string());
    }
}

fn terminate_with_kill_path(config: &Config, job: &DueJob) -> KillOutcome {
    if !crate::core::job::is_pid_alive(job.pid) {
        // The process exited on its own between selection and now: there is
        // nothing to terminate, and the job's own final-status write (done
        // via `save_final_status_unless_killed`) must be the one that
        // stands.
        return KillOutcome::AlreadyGone;
    }
    match crate::core::commands::kill::terminate_process(
        config,
        &job.job_id,
        job.pid,
        job.pid_start_time,
        Some(job.retry_count),
    ) {
        Ok(()) => KillOutcome::Terminated,
        Err(e) => {
            eprintln!(
                "agent-run: max-runtime enforce: failed to terminate job '{}': {e}",
                job.job_id
            );
            KillOutcome::AlreadyGone
        }
    }
}

/// Terminate any running job whose wall time since its recorded start anchor
/// has exceeded its max-runtime limit. Returns the ids of the jobs that were
/// terminated and marked. Safe to call repeatedly and concurrently (each
/// pass re-derives the live set, and the kill path is guarded on the row's
/// retry count so a job that just moved on to a newer attempt is never
/// touched).
pub fn enforce_once(config: &Config) -> Vec<String> {
    let cfg = config.clone();
    enforce_pass(config, move |job| terminate_with_kill_path(&cfg, job))
}

/// One scan-and-enforce pass:
///
/// 1. **Select** (serial, fast): every row that
///    [`crate::core::job::max_runtime_kill_due`] says is over its limit.
/// 2. **Terminate** (one spawned worker thread per job, via the injected
///    `terminate` so tests can stand in for the ~20 s kill path): on
///    `KillOutcome::Terminated` the worker RE-READS the row and only a row
///    that is still `Running` gets the `max runtime exceeded` log line and
///    the conditional `mark_job_killed` UPDATE (line appended only when the
///    UPDATE was applied, at most once per job id).
/// 3. **Collect** (bounded): results are gathered over a channel; once
///    [`ENFORCE_PASS_BUDGET_SECS`] has elapsed the pass stops waiting and
///    returns what has settled.
pub(crate) fn enforce_pass<F>(config: &Config, terminate: F) -> Vec<String>
where
    F: Fn(&DueJob) -> KillOutcome + Send + Sync + 'static,
{
    let state_root = &config.state_root;
    let jobs = match crate::core::job::list_jobs(state_root, None) {
        Ok(jobs) => jobs,
        Err(e) => {
            eprintln!("agent-run: max-runtime enforce: failed to list jobs: {e}");
            return Vec::new();
        }
    };

    let now = Utc::now();
    let mut due: Vec<DueJob> = Vec::new();

    for meta in &jobs {
        // A job we may not signal (mid-launch, orphan, pidless, or not
        // currently classified running) is skipped up front.
        let status = compute_status(meta, state_root);
        let pid = match meta.pid {
            Some(p) => p,
            None => continue,
        };
        // No limit, no start time, not running, or not yet over: leave the
        // job alone. This is the entire "should we kill?" policy, kept as a
        // pure, unit-tested decision function (never kills a job without a
        // limit).
        if !max_runtime_kill_due(status, meta.started_at, meta.max_runtime, now) {
            continue;
        }

        // The decision above is `true` only with a running job that HAS a
        // limit, so this is guaranteed `Some`; the `None` arm is
        // belt-and-suspenders (keep the path panic-free, as the doc
        // promises).
        let limit = match meta.max_runtime {
            Some(l) => l,
            None => continue,
        };

        due.push(DueJob {
            state_root: state_root.to_path_buf(),
            job_id: meta.job_id.clone(),
            pid,
            pid_start_time: meta.pid_start_time,
            retry_count: meta.retry_count,
            limit,
            log_path: JobMetadata::log_path(state_root, &meta.job_id),
        });
    }

    let pending = due.len();
    let (tx, rx) = mpsc::channel::<(String, bool, bool)>();
    // One worker thread per job, sharing the (possibly test-injected)
    // terminate function through an Arc.
    let term = std::sync::Arc::new(terminate);
    for job in due {
        let tx = tx.clone();
        let term = std::sync::Arc::clone(&term);
        thread::spawn(move || {
            let outcome = term(&job);
            let mut marked = false;
            if outcome == KillOutcome::Terminated {
                // Re-read the row NOW: between selection and here the job
                // may have finished on its own (its monitor wrote `done` +
                // the real exit code and the process exited, so the
                // terminate path saw a dead PID and succeeded). Only a row
                // that is STILL running gets marked below -- and even then
                // `mark_job_killed` double-checks with a conditional
                // UPDATE that refuses only a row that finished
                // successfully on its own.
                match JobMetadata::load(&job.state_root, &job.job_id) {
                    Ok(fresh) if compute_status(&fresh, &job.state_root) != JobStatus::Done => {
                        marked = true;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!(
                            "agent-run: max-runtime enforce: failed to re-read job '{}' after termination: {e}",
                            job.job_id
                        );
                    }
                }
            }
            let mut applied = false;
            if marked {
                match mark_job_killed(&job.state_root, &job.job_id, job.retry_count) {
                    Ok(a) => applied = a,
                    Err(e) => {
                        eprintln!(
                            "agent-run: max-runtime enforce: failed to mark job '{}' killed: {e}",
                            job.job_id
                        );
                    }
                }
            }
            // The log line goes out only after the mark was applied (a real
            // termination of a still-running job), and at most once per job
            // id: a failing terminate leaves the row running-and-over-limit,
            // and two overlapping passes must not double-append.
            if applied && !already_logged(&job.job_id) {
                crate::core::hindsight::append_to_log_file(
                    &job.log_path,
                    &format!(
                        "max runtime exceeded ({})",
                        crate::core::job::format_max_runtime(&job.limit)
                    ),
                );
                remember_logged(&job.job_id);
            }
            let _ = tx.send((job.job_id, marked, applied));
        });
    }
    drop(tx);

    let mut killed: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(ENFORCE_PASS_BUDGET_SECS);
    for _ in 0..pending {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match rx.recv_timeout(remaining) {
            Ok((job_id, _marked, applied)) => {
                if applied {
                    killed.push(job_id);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                eprintln!(
                    "agent-run: max-runtime enforce: pass budget ({ENFORCE_PASS_BUDGET_SECS}s) \
                     exhausted; unfinished terminations continue in the background"
                );
                break;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    killed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{
        ensure_state_store, get_process_start_time, save_final_status_unless_killed,
    };
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    /// Spawns a `sleep 300` as the leader of its own process group; returns
    /// the child plus the identity anchor (pid + recorded start time). A
    /// live `sleep`-named process with a matching start time is exactly what
    /// makes a seeded row classify as `Running`. The caller must kill and
    /// wait on the child before the test ends.
    fn spawn_sleep() -> (std::process::Child, u32, Option<u64>) {
        use std::os::unix::process::CommandExt;
        let child = Command::new("sleep")
            .arg("300")
            .process_group(0)
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        // Let sleep come up before capturing its start time.
        thread::sleep(Duration::from_millis(100));
        (child, pid, get_process_start_time(pid))
    }

    fn reap(child: &mut std::process::Child) {
        let _ = child.kill();
        let _ = child.wait();
    }

    fn test_state() -> (tempfile::TempDir, Config) {
        let temp_dir = tempfile::tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        ensure_state_store(&state_dir).unwrap();
        let config = Config {
            state_root: state_dir,
            ..Default::default()
        };
        (temp_dir, config)
    }

    fn seed_running_job(state_root: &Path, job_id: &str, pid: u32, pid_start_time: Option<u64>) {
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
            created_at: Utc::now(),
            worktree_path: PathBuf::from("/workspace/wt"),
            branch: format!("agent-run/{job_id}"),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: Some(pid),
            exit_code: None,
            status: Some(JobStatus::Running),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            pid_start_time,
            hindsight_bank: None,
            proxy: None,
            max_runtime: Some(chrono::Duration::seconds(30)),
            started_at: Some(Utc::now() - chrono::Duration::seconds(120)),
            hindsight_query: None,
        };
        meta.save(state_root).unwrap();
    }

    fn count_limit_lines(state_root: &Path, job_id: &str) -> usize {
        let path = JobMetadata::log_path(state_root, job_id);
        fs::read_to_string(&path)
            .map(|c| {
                c.lines()
                    .filter(|l| l.contains("max runtime exceeded"))
                    .count()
            })
            .unwrap_or(0)
    }

    #[test]
    fn test_enforce_terminates_running_job_marks_killed_and_logs_once() {
        let (_tmp, config) = test_state();
        let (mut child, pid, st) = spawn_sleep();
        seed_running_job(&config.state_root, "mr-ok-1", pid, st);

        let killed = enforce_pass(&config, |_| KillOutcome::Terminated);
        reap(&mut child);

        assert_eq!(killed, vec!["mr-ok-1".to_string()]);
        let reloaded = JobMetadata::load(&config.state_root, "mr-ok-1").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));
        assert_eq!(
            count_limit_lines(&config.state_root, "mr-ok-1"),
            1,
            "exactly one 'max runtime exceeded' line"
        );
    }

    #[test]
    fn test_enforce_job_that_finished_between_decide_and_terminate_stays_done() {
        let (_tmp, config) = test_state();
        let (mut child, pid, st) = spawn_sleep();
        seed_running_job(&config.state_root, "mr-race-1", pid, st);

        // Simulates the done-race: at SELECTION time the row is running with
        // a live, identity-matching pid; between the decision and the mark,
        // the job's monitor writes the final status `done` + exit 0 (the
        // terminate path has already seen the process go, so it reports
        // success/`Terminated`). The post-terminate re-read + the
        // conditional `mark_job_killed` must keep `done`.
        let killed = enforce_pass(&config, |job| {
            save_final_status_unless_killed(
                &job.state_root,
                "mr-race-1",
                JobStatus::Done,
                Some(0),
                None,
                None,
            )
            .unwrap();
            KillOutcome::Terminated
        });
        reap(&mut child);

        assert!(
            killed.iter().all(|id| id != "mr-race-1"),
            "a job that finished on its own must not count as terminated by us: {killed:?}"
        );
        let reloaded = JobMetadata::load(&config.state_root, "mr-race-1").unwrap();
        assert_eq!(
            reloaded.status,
            Some(JobStatus::Done),
            "a finished job must keep its done status, never be overwritten to killed"
        );
        assert_eq!(reloaded.exit_code, Some(0));
        assert_eq!(
            count_limit_lines(&config.state_root, "mr-race-1"),
            0,
            "no 'max runtime exceeded' line for a job we never terminated"
        );
    }

    #[test]
    fn test_enforce_failing_terminate_appends_at_most_one_line() {
        let (_tmp, config) = test_state();
        let (mut child, pid, st) = spawn_sleep();
        seed_running_job(&config.state_root, "mr-fail-1", pid, st);

        // Two consecutive enforcement passes with a terminate that keeps
        // failing (identity mismatch / signal error): the row stays running
        // and over its limit, so both passes select it again. The limit
        // line may appear at most once in total.
        enforce_pass(&config, |_| KillOutcome::AlreadyGone);
        let after_first = count_limit_lines(&config.state_root, "mr-fail-1");
        enforce_pass(&config, |_| KillOutcome::AlreadyGone);
        let after_second = count_limit_lines(&config.state_root, "mr-fail-1");
        reap(&mut child);

        assert!(
            after_second <= 1,
            "two failing passes must append at most one line, wrote {after_second} (after first pass: {after_first})"
        );
        let reloaded = JobMetadata::load(&config.state_root, "mr-fail-1").unwrap();
        assert_eq!(
            reloaded.status,
            Some(JobStatus::Running),
            "a failed terminate must not mark the job killed"
        );
        assert_eq!(reloaded.exit_code, None);
    }

    #[test]
    fn test_enforce_stubborn_job_does_not_delay_other_jobs() {
        let (_tmp, config) = test_state();
        let (mut child_a, pid_a, st_a) = spawn_sleep();
        let (mut child_b, pid_b, st_b) = spawn_sleep();
        let (mut child_c, pid_c, st_c) = spawn_sleep();
        seed_running_job(&config.state_root, "mr-slow-1", pid_a, st_a);
        seed_running_job(&config.state_root, "mr-slow-2", pid_b, st_b);
        seed_running_job(&config.state_root, "mr-slow-3", pid_c, st_c);

        // Three jobs whose terminates each stall for 1 s. In the old serial
        // loop the pass took >= 3 s; with one worker thread per job it
        // takes ~1 s. The bound below fails under the old serial loop.
        let stall = Duration::from_millis(1000);
        let start = Instant::now();
        let killed = enforce_pass(&config, move |_| {
            thread::sleep(stall);
            KillOutcome::Terminated
        });
        let elapsed = start.elapsed();
        reap(&mut child_a);
        reap(&mut child_b);
        reap(&mut child_c);

        for id in ["mr-slow-1", "mr-slow-2", "mr-slow-3"] {
            assert!(
                killed.contains(&id.to_string()),
                "{id} must be reported terminated, got {killed:?}"
            );
            let reloaded = JobMetadata::load(&config.state_root, id).unwrap();
            assert_eq!(
                reloaded.status,
                Some(JobStatus::Killed),
                "{id} must be marked killed"
            );
            assert_eq!(
                count_limit_lines(&config.state_root, id),
                1,
                "{id} needs its log line"
            );
        }
        assert!(
            elapsed < stall * 3 - stall / 2,
            "the jobs must be terminated in parallel, not serially (pass took {elapsed:?}; three 1s serial terminations would take >= 3s)"
        );
    }
}

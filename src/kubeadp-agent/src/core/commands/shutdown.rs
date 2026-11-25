//! Graceful-shutdown drain for running agy jobs.
//!
//! When the agent-run-api pod receives SIGTERM (a rollout), the HTTP server
//! first stops accepting new connections; before the process then exits,
//! [`terminate_running_jobs_gracefully`] sends SIGTERM to the *process group*
//! of every job whose status computes to `Running`, so agy gets a clean
//! signal and writes its final structured JSON line (conversation id +
//! status) into the job log -- instead of the pod killing it abruptly and
//! leaving the job row without a conversation id. The in-process job monitor
//! thread (see `runner::run_internal_runner`) reaps agy and persists the
//! final status itself; this function only waits for the agy processes to
//! die (at most `grace`, polling every 250 ms) plus a short tail so the
//! monitor threads can finish their DB write. SIGKILL is deliberately never
//! sent here -- the pod's own termination grace period does that if needed.

use std::thread;
use std::time::{Duration, Instant};

use crate::core::commands::kill::{send_signal, ProcessIdentity};
use crate::core::config::Config;
use crate::core::job::{compute_status, list_jobs, JobStatus};

/// Drain-wait polling interval.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Extra tail after the agy processes are gone, giving the in-process job
/// monitor threads time to finish their final status DB write.
const MONITOR_WRITE_TAIL: Duration = Duration::from_millis(500);

/// Sends SIGTERM to the process groups of all currently running jobs and
/// waits for those agy processes to exit -- at most `grace`, polling every
/// 250 ms -- plus a short tail so the in-process job monitor threads finish
/// their final status DB write before the server process exits.
///
/// Returns the ids of the jobs whose process group was signalled. A `grace`
/// of zero disables the drain entirely (no jobs are signalled, nothing is
/// waited for). Every candidate is re-checked with the same process identity
/// guard `kill` uses ([`ProcessIdentity`]) so a recycled PID can never be
/// signalled, and SIGKILL is intentionally never sent from here.
pub fn terminate_running_jobs_gracefully(config: &Config, grace: Duration) -> Vec<String> {
    if grace.is_zero() {
        return Vec::new();
    }

    let jobs = match list_jobs(&config.state_root, None) {
        Ok(jobs) => jobs,
        Err(e) => {
            eprintln!("agent-run: graceful shutdown: failed to list jobs: {e}");
            return Vec::new();
        }
    };

    // Only rows that both `compute_status` and the kill process-identity
    // check agree are still a live job process.
    let candidates: Vec<(String, ProcessIdentity)> = jobs
        .iter()
        .filter(|meta| matches!(compute_status(meta, &config.state_root), JobStatus::Running))
        .filter_map(|meta| {
            meta.pid.map(|pid| {
                (
                    meta.job_id.clone(),
                    ProcessIdentity {
                        pid,
                        start_time: meta.pid_start_time,
                    },
                )
            })
        })
        .filter(|(_, identity)| identity.is_still_target(config))
        .collect();

    let mut signaled: Vec<String> = Vec::new();
    for (job_id, identity) in &candidates {
        match send_signal(identity.pid, "TERM") {
            Ok(()) => {
                eprintln!(
                    "agent-run: graceful shutdown: sent SIGTERM to job '{}' (PID {}); waiting up to {:?} for agy to finish",
                    job_id, identity.pid, grace
                );
                signaled.push(job_id.clone());
            }
            Err(e) => eprintln!(
                "agent-run: graceful shutdown: failed to send SIGTERM to job '{}' (PID {}): {e}",
                job_id, identity.pid
            ),
        }
    }

    // Poll only the process groups we actually signalled; once none of them
    // answer the identity check anymore, agy has exited and the monitor
    // thread is writing the final status.
    let wait_for: Vec<(String, ProcessIdentity)> = candidates
        .into_iter()
        .filter(|(job_id, _)| signaled.contains(job_id))
        .collect();
    if wait_for.is_empty() {
        return signaled;
    }

    let deadline = Instant::now() + grace;
    loop {
        let remaining: Vec<&(String, ProcessIdentity)> = wait_for
            .iter()
            .filter(|(_, identity)| identity.is_still_target(config))
            .collect();
        if remaining.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "agent-run: graceful shutdown: {:?} grace period elapsed with {} job(s) still running; leaving them to the pod's own termination grace period: {}",
                grace,
                remaining.len(),
                remaining
                    .iter()
                    .map(|(job_id, _)| job_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            break;
        }
        thread::sleep(POLL_INTERVAL);
    }

    // Give the monitor threads a short tail to finish their final status DB
    // write now that the agy processes are gone (or the grace period expired).
    thread::sleep(MONITOR_WRITE_TAIL);

    signaled
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{ensure_state_store, is_pid_alive, JobMetadata};
    use crate::core::runner::launch_background_job;
    use chrono::Utc;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// A fresh job row, as `commands::start` stores it before launching the
    /// monitor (the same shape the kill/runner tests use).
    fn fresh_meta(job_id: &str, worktree: PathBuf) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            project: "p".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "shutdown drain prompt".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: worktree,
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: None,
            status: None,
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
        }
    }

    /// Like the real agy on shutdown: sleeps until signalled, then on SIGTERM
    /// writes its final structured JSON line (conversation id + status) and
    /// exits 0. `sleep & wait` keeps the trap responsive (a bare `sleep 60`
    /// would swallow the signal until it finished).
    const FAKE_AGY_TERM_SCRIPT: &str = "#!/bin/sh\n\
        trap 'printf \"%s\\n\" \"{\\\"status\\\":\\\"SUCCESS\\\",\\\"response\\\":\\\"term-drain-ok\\\",\\\"conversation_id\\\":\\\"conv-term-1\\\"}\"; exit 0' TERM\n\
        echo \"ready\"\n\
        sleep 60 &\n\
        wait $!\n\
        exit 0\n";

    /// A one-shot fake agy that prints a final structured line and exits 0
    /// immediately.
    const FAKE_AGY_FAST_SCRIPT: &str = "#!/bin/sh\n\
        echo '{\"status\":\"SUCCESS\",\"response\":\"already-done\",\"conversation_id\":\"conv-done-1\"}'\n\
        exit 0\n";

    /// Polls the state store until `exit_code` is recorded (the monitor
    /// thread finished the job) or the deadline passes.
    fn wait_for_exit_code(
        state_root: &std::path::Path,
        job_id: &str,
        deadline: Instant,
    ) -> JobMetadata {
        loop {
            let meta = JobMetadata::load(state_root, job_id).expect("job row must exist");
            if meta.exit_code.is_some() {
                return meta;
            }
            assert!(
                Instant::now() < deadline,
                "job {job_id} did not finish in time"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn test_graceful_shutdown_signals_running_job_and_drains() {
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let ws = tmp.path().join("ws");
        fs::create_dir_all(&ws).unwrap();
        ensure_state_store(&state_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        fs::write(&agy, FAKE_AGY_TERM_SCRIPT).unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            repos_root: ws.join(".agent-repos"),
            workspace_root: ws,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            ..Default::default()
        };

        let job_id = format!("shr{}", std::process::id());
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        let mut meta = fresh_meta(&job_id, wt);
        // Started the way the runner starts jobs: the monitor thread holds
        // the agy child, so the PID is the real (setsid'd) agy process.
        let pid = launch_background_job(&config, &mut meta).expect("launch should confirm a PID");
        assert!(pid > 1, "expected a real child PID");
        assert!(
            is_pid_alive(pid),
            "fake agy must still be running before the drain"
        );

        let log_path = JobMetadata::log_path(&state_root, &job_id);
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < ready_deadline {
            if fs::read_to_string(&log_path)
                .map(|s| s.contains("ready"))
                .unwrap_or(false)
            {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        let started = Instant::now();
        let signaled = terminate_running_jobs_gracefully(&config, Duration::from_secs(20));
        assert_eq!(
            signaled,
            vec![job_id.clone()],
            "the running job must be reported as signalled"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "drain must return once agy exits on TERM, not wait out the grace period"
        );

        // The monitor thread (still inside our process) reaped agy and
        // persisted the final row from the line agy wrote on SIGTERM.
        let deadline = Instant::now() + Duration::from_secs(10);
        let meta = wait_for_exit_code(&state_root, &job_id, deadline);
        assert_eq!(
            meta.exit_code,
            Some(0),
            "agy exited 0 from its TERM handler"
        );
        assert_eq!(meta.status, Some(JobStatus::Done));
        assert_eq!(
            meta.conversation_id.as_deref(),
            Some("conv-term-1"),
            "the SIGTERM final line must yield the conversation id (the bug R2 fixes)"
        );

        // And the final structured line really is in the job log.
        let log = fs::read_to_string(JobMetadata::log_path(&state_root, &job_id)).unwrap();
        assert!(
            log.contains("term-drain-ok"),
            "job log must contain agy's final JSON line: {log}"
        );
    }

    #[test]
    fn test_graceful_shutdown_with_zero_grace_does_nothing() {
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let ws = tmp.path().join("ws");
        fs::create_dir_all(&ws).unwrap();
        ensure_state_store(&state_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        fs::write(&agy, FAKE_AGY_TERM_SCRIPT).unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            repos_root: ws.join(".agent-repos"),
            workspace_root: ws,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            ..Default::default()
        };

        let job_id = format!("shrz{}", std::process::id());
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        let mut meta = fresh_meta(&job_id, wt);
        let pid = launch_background_job(&config, &mut meta).expect("launch should confirm a PID");

        let signaled = terminate_running_jobs_gracefully(&config, Duration::from_secs(0));
        assert!(
            signaled.is_empty(),
            "grace 0 disables the drain: nothing may be signalled"
        );
        assert!(
            is_pid_alive(pid),
            "grace 0 must not touch the running job's process"
        );
        let log = fs::read_to_string(JobMetadata::log_path(&state_root, &job_id)).unwrap();
        assert!(
            !log.contains("term-drain-ok"),
            "no final line may have been written by the drain: {log}"
        );

        // Cleanup: kill the fake agy so the test leaves no 60 s sleep behind.
        let _ = send_signal(pid, "KILL");
        let deadline = Instant::now() + Duration::from_secs(5);
        while is_pid_alive(pid) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(50));
        }
        // Best effort only: the behaviour under test is asserted above, and on a slow
        // CI runner the killed process can take longer than the deadline to go away.
        let _ = is_pid_alive(pid);
    }

    #[test]
    fn test_graceful_shutdown_without_running_jobs_returns_empty() {
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let ws = tmp.path().join("ws");
        fs::create_dir_all(&ws).unwrap();
        ensure_state_store(&state_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        fs::write(&agy, FAKE_AGY_FAST_SCRIPT).unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        // A store holding one FINISHED job (dead agy, terminal status row):
        // it exists, but nothing is running, so it must not be picked up and
        // the drain must return immediately (no 500 ms monitor tail).
        let config = Config {
            repos_root: ws.join(".agent-repos"),
            workspace_root: ws,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            ..Default::default()
        };
        let job_id = format!("shrd{}", std::process::id());
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        let mut meta = fresh_meta(&job_id, wt);
        let pid = launch_background_job(&config, &mut meta).expect("launch should confirm a PID");
        let meta = wait_for_exit_code(
            &state_root,
            &job_id,
            Instant::now() + Duration::from_secs(10),
        );
        assert_eq!(meta.status, Some(JobStatus::Done));
        assert!(!is_pid_alive(pid), "one-shot fake agy must already be gone");

        let started = Instant::now();
        let signaled = terminate_running_jobs_gracefully(&config, Duration::from_secs(20));
        let elapsed = started.elapsed();
        assert!(
            signaled.is_empty(),
            "no running jobs: nothing may be signalled"
        );
        assert!(
            elapsed < MONITOR_WRITE_TAIL,
            "with nothing signalled the drain must skip the monitor tail wait, took {elapsed:?}"
        );
    }
}

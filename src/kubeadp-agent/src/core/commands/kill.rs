use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::thread;
use std::time::{Duration, Instant};

use crate::core::args::JobIdArg;
use crate::core::config::Config;
use crate::core::job::{compute_status, is_pid_alive, mark_job_killed, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

pub use crate::core::job::get_process_start_time;

/// Sends `SIG<sig_name>` (e.g. "TERM", "KILL") to `pid`.
///
/// Jobs always run as the server's own process user, so a direct `libc::kill()`
/// is always permitted. The process group is signaled first (the agy child is a
/// session/process-group leader, see `detached_session`), falling back to the
/// single PID when the group no longer exists (ESRCH).
/// Lightweight sanity check to guard against PID reuse before signaling.
///
/// Verifies /proc/<pid>/cmdline or /proc/<pid>/exe still looks like an expected
/// agent process (agy, agent-run, or test runner processes like sleep).
pub(crate) fn is_pid_plausible_job_process(config: &Config, pid: u32) -> bool {
    let cmdline_path = format!("/proc/{}/cmdline", pid);
    if let Ok(cmdline) = std::fs::read(&cmdline_path) {
        let s = String::from_utf8_lossy(&cmdline);
        if !s.is_empty() {
            let agy_stem = config
                .agy_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "agy".to_string());
            if s.contains(&agy_stem)
                || s.contains("agy")
                || s.contains("agent-run")
                || s.contains("sleep")
            {
                return true;
            }
            return false;
        }
    }

    let exe_path = format!("/proc/{}/exe", pid);
    if let Ok(target) = std::fs::read_link(&exe_path) {
        let path_str = target.to_string_lossy();
        if path_str.contains("agy") || path_str.contains("agent-run") || path_str.contains("sleep")
        {
            return true;
        }
        return false;
    }

    true
}

/// Tracks the identity of an OS process to prevent signaling or waiting on recycled PIDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_time: Option<u64>,
}

impl ProcessIdentity {
    #[allow(dead_code)]
    pub fn capture(pid: u32) -> Self {
        Self {
            pid,
            start_time: get_process_start_time(pid),
        }
    }

    /// Verifies whether the process with PID `self.pid` is still alive and represents
    /// the same target process (has not exited and had its PID recycled).
    ///
    /// Returns `true` if:
    /// 1. The PID is alive (`is_pid_alive`).
    /// 2. The process is still a plausible job process (`is_pid_plausible_job_process`).
    /// 3. If a start time was captured initially, the current process with this PID has
    ///    the identical start time; if none was captured, the cmdline must still
    ///    look like a plausible job process (conservative fallback instead of
    ///    trusting the PID, so a recycled PID cannot pass the check).
    ///
    /// Returns `false` if the process has exited or the PID now belongs to a different process.
    pub fn is_still_target(&self, config: &Config) -> bool {
        if !is_pid_alive(self.pid) {
            return false;
        }
        if !is_pid_plausible_job_process(config, self.pid) {
            return false;
        }
        match self.start_time {
            Some(init_start) => get_process_start_time(self.pid) == Some(init_start),
            // No start time recorded: re-verify the process still looks like a
            // job process rather than passing the identity check by default.
            None => is_pid_plausible_job_process(config, self.pid),
        }
    }
}

pub fn send_signal(pid: u32, sig_name: &str) -> Result<()> {
    let sig = if sig_name == "KILL" {
        libc::SIGKILL
    } else {
        libc::SIGTERM
    };
    // Try signaling the process group (-pid) first
    let res = unsafe { libc::kill(-(pid as i32), sig) };
    if res != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            // If not process group leader, signal single PID
            let res_single = unsafe { libc::kill(pid as i32, sig) };
            if res_single != 0 {
                let err_single = std::io::Error::last_os_error();
                if err_single.raw_os_error() != Some(libc::ESRCH) {
                    bail!(
                        "Failed to send SIG{} to PID {}: {}",
                        sig_name,
                        pid,
                        err_single
                    );
                }
            }
        } else {
            bail!(
                "Failed to send SIG{} to process group {}: {}",
                sig_name,
                pid,
                err
            );
        }
    }
    Ok(())
}
/// Terminates a running job process: validates PID plausible and process start-time matches
/// launch-time ground truth, verifies the job's retry_count in the state store has not changed
/// since termination started, sends SIGTERM to process group, waits grace period while re-verifying
/// process identity (to guard against PID recycling) and retry generation, escalates to SIGKILL
/// if necessary, and verifies the process has exited.
pub fn terminate_process(
    config: &Config,
    job_id: &str,
    pid: u32,
    known_start_time: Option<u64>,
    expected_retry_count: Option<u32>,
) -> Result<()> {
    let mut emit = |stderr: bool, line: String| {
        if stderr {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    };
    terminate_process_to(
        config,
        job_id,
        pid,
        known_start_time,
        expected_retry_count,
        &mut emit,
    )
}

/// Collected variant of [`terminate_process`]: progress lines are routed
/// through `emit(stderr, line)` instead of being printed directly, so the
/// run/execute split can record them in a [`CommandOutput`].
pub fn terminate_process_to(
    config: &Config,
    job_id: &str,
    pid: u32,
    known_start_time: Option<u64>,
    expected_retry_count: Option<u32>,
    emit: &mut dyn FnMut(bool, String),
) -> Result<()> {
    if !is_pid_alive(pid) {
        return Ok(());
    }

    if let Some(expected_retry) = expected_retry_count {
        let current_meta = JobMetadata::load(&config.state_root, job_id)
            .or_else(|_| JobMetadata::resolve(&config.state_root, job_id))
            .with_context(|| {
                format!(
                    "Failed to verify retry count for job '{}' in state store before signaling",
                    job_id
                )
            })?;
        if current_meta.retry_count != expected_retry {
            bail!(
                "Sanity check failed: job '{}' retry count changed from {} to {} since termination started (this job has moved on to a newer attempt); refusing to signal a process from a different attempt than the one this call was meant to target",
                job_id,
                expected_retry,
                current_meta.retry_count
            );
        }
    }

    let identity = ProcessIdentity {
        pid,
        start_time: known_start_time,
    };

    if !identity.is_still_target(config) {
        bail!(
            "Sanity check failed: PID {} does not appear to belong to an agent-run job (process identity or command line / executable did not match expected launch target); refusing to signal potentially reused PID",
            pid
        );
    }

    emit(
        false,
        format!("Sending SIGTERM to job '{}' (PID {})...", job_id, pid),
    );
    send_signal(pid, "TERM")?;

    let grace_period = Duration::from_secs(20);
    let start = Instant::now();
    let mut terminated = false;

    while start.elapsed() < grace_period {
        if !identity.is_still_target(config) {
            terminated = true;
            break;
        }
        if let Some(expected_retry) = expected_retry_count {
            if let Ok(current_meta) = JobMetadata::load(&config.state_root, job_id)
                .or_else(|_| JobMetadata::resolve(&config.state_root, job_id))
            {
                if current_meta.retry_count != expected_retry {
                    // Job moved on to a newer attempt while waiting; stop waiting so we do not escalate to SIGKILL
                    terminated = true;
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    if !terminated {
        // Re-verify process identity before escalating to SIGKILL:
        // if the PID no longer refers to the target process, it has already terminated.
        if identity.is_still_target(config) {
            if let Some(expected_retry) = expected_retry_count {
                let current_meta = JobMetadata::load(&config.state_root, job_id)
                    .or_else(|_| JobMetadata::resolve(&config.state_root, job_id))
                    .with_context(|| {
                        format!(
                            "Failed to verify retry count for job '{}' in state store before SIGKILL escalation",
                            job_id
                        )
                    })?;
                if current_meta.retry_count != expected_retry {
                    bail!(
                        "Sanity check failed: job '{}' retry count changed from {} to {} during termination grace period (this job has moved on to a newer attempt); refusing to proceed while process might belong to a newer attempt",
                        job_id,
                        expected_retry,
                        current_meta.retry_count
                    );
                }
            }

            emit(
                false,
                format!(
                    "Process did not exit after grace period. Sending SIGKILL to PID {}...",
                    pid
                ),
            );
            send_signal(pid, "KILL")?;
            thread::sleep(Duration::from_millis(200));
        }
    }

    if identity.is_still_target(config) {
        if let Some(expected_retry) = expected_retry_count {
            if let Ok(current_meta) = JobMetadata::load(&config.state_root, job_id)
                .or_else(|_| JobMetadata::resolve(&config.state_root, job_id))
            {
                if current_meta.retry_count != expected_retry {
                    bail!(
                        "Sanity check failed: job '{}' retry count changed from {} to {} during termination (this job has moved on to a newer attempt); refusing to proceed while process might belong to a newer attempt",
                        job_id,
                        expected_retry,
                        current_meta.retry_count
                    );
                }
            }
        }
        bail!(
            "Job '{}' (PID {}) is still alive after SIGTERM and SIGKILL; refusing to proceed \
             while the underlying process keeps running.",
            job_id,
            pid
        );
    }

    Ok(())
}

pub fn run(config: &Config, args: &JobIdArg) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    let mut output = CommandOutput::new(json!({
        "job_id": meta.job_id,
        "terminated": false,
        "pid": Value::Null,
    }));

    // Decide on the job's computed status, not raw PID liveness: a job that
    // already finished but whose PID has since been recycled by an unrelated
    // process would otherwise look "alive" here, and we would try to
    // terminate an innocent process (hard "Sanity check failed" error).
    if status != JobStatus::Running {
        output.push_out(format!(
            "Job '{}' is not currently running (status: {}).",
            args.job_id, status
        ));
        return Ok(output);
    }

    let pid = match meta.pid {
        Some(p) if is_pid_alive(p) => p,
        _ => {
            output.push_out(format!(
                "Job '{}' is not currently running (status: {}).",
                args.job_id, status
            ));
            return Ok(output);
        }
    };

    let mut emit = |stderr: bool, line: String| {
        if stderr {
            output.push_err(line);
        } else {
            output.push_out(line);
        }
    };
    terminate_process_to(
        config,
        &meta.job_id,
        pid,
        meta.pid_start_time,
        Some(meta.retry_count),
        &mut emit,
    )?;

    // Mark the row killed with a TARGETED update of status/exit_code only
    // (guarded by retry_count, like `save_final_status_unless_killed`). The
    // previous load-modify-full-row-save could re-write every column from a
    // stale snapshot, clobbering a `conversation_id` the background runner
    // wrote concurrently. No other column is touched here.
    let _ = mark_job_killed(&config.state_root, &meta.job_id, meta.retry_count);

    output.push_out(format!(
        "Job '{}' (PID {}) terminated successfully.",
        meta.job_id, pid
    ));
    output.data = json!({
        "job_id": meta.job_id,
        "terminated": true,
        "pid": pid,
    });
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn test_send_signal_local_mode_terminates_real_process() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();

        send_signal(pid, "TERM").expect("send_signal should succeed");

        // Give the process a moment to actually exit after SIGTERM.
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            !is_pid_alive(pid),
            "process should be terminated after SIGTERM"
        );

        let _ = child.wait();
    }

    #[test]
    fn test_kill_resolves_unique_job_id_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_dir).unwrap();
        let meta = JobMetadata {
            job_id: "kljob01".to_string(),
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
            worktree_path: temp.path().join("missing-worktree"),
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
        };
        meta.save(&state_dir).unwrap();

        let config = Config {
            state_root: state_dir,
            ..Default::default()
        };

        // A unique prefix resolves the job (not running, so no signal is
        // sent) while an unknown ID is still rejected.
        let res = run(
            &config,
            &JobIdArg {
                job_id: "kljo".to_string(),
            },
        );
        assert!(
            res.is_ok(),
            "kill with a unique ID prefix should resolve: {:?}",
            res.err()
        );

        let err = run(
            &config,
            &JobIdArg {
                job_id: "nope".to_string(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("No such job"), "got: {}", err);
    }

    #[test]
    fn test_process_identity_verification_and_pid_reuse() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let config = Config::default();
        let identity = ProcessIdentity::capture(pid);

        // 1. Target process is alive and identity matches
        assert!(
            identity.is_still_target(&config),
            "Identity check should succeed for running process"
        );

        // 2. Simulated PID recycling (different start time)
        let recycled = ProcessIdentity {
            pid,
            start_time: Some(identity.start_time.unwrap_or(100) + 99999),
        };
        assert!(
            !recycled.is_still_target(&config),
            "Identity check must detect PID reuse with different start time"
        );

        // 3. Process termination
        let _ = child.kill();
        let _ = child.wait();
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !identity.is_still_target(&config),
            "Identity check must return false once process has exited"
        );
    }

    #[test]
    fn test_process_identity_without_start_time_requires_plausible_cmdline() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let config = Config::default();
        // No start time recorded (older rows): the identity check must fall
        // back to the cmdline plausibility check instead of passing by
        // default, so a reused PID cannot masquerade as the target.
        let identity = ProcessIdentity {
            pid,
            start_time: None,
        };
        assert!(
            identity.is_still_target(&config),
            "a live, plausible process with no recorded start time is still the target"
        );

        let _ = child.kill();
        let _ = child.wait();
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !identity.is_still_target(&config),
            "the fallback must not keep a dead PID passing"
        );
    }

    #[test]
    fn test_terminate_process_succeeds_and_reaps() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let config = Config::default();
        let res = terminate_process(&config, "test-job", pid, get_process_start_time(pid), None);
        assert!(
            res.is_ok(),
            "terminate_process should succeed: {:?}",
            res.err()
        );
        let _ = child.wait();
        assert!(
            !is_pid_alive(pid),
            "Process should be dead after terminate_process"
        );
    }

    #[test]
    fn test_terminate_process_rejects_mismatched_start_time_without_signaling() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let real_start = get_process_start_time(pid);
        let config = Config::default();
        let wrong_start = Some(real_start.unwrap_or(100) + 12345);

        // Mismatched start time must be rejected before any signal is sent
        let res = terminate_process(&config, "test-job", pid, wrong_start, None);
        assert!(
            res.is_err(),
            "terminate_process must fail for mismatched start time"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("Sanity check failed"), "got: {}", err_msg);

        // Process must still be alive! Proves SIGTERM was never sent.
        assert!(
            is_pid_alive(pid),
            "Process must not have received SIGTERM on start time mismatch"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn test_pid_reused_job_not_killed_and_not_marked() {
        // A FINISHED job whose stored PID now belongs to an unrelated live
        // process (PID reuse) must be (a) not classified running, (b) not
        // signalled, and (c) never marked `killed`.
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_dir).unwrap();
        let meta = JobMetadata {
            job_id: "reus01".to_string(),
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
            worktree_path: temp.path().join("wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: Some(pid),
            // The original (dead) job process's start time: differs from the
            // start time of the process that now holds the recycled PID.
            pid_start_time: Some(424242),
            exit_code: Some(1),
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
        };
        meta.save(&state_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            ..Default::default()
        };

        // (a) The pid_start_time mismatch keeps a recycled PID from flipping
        // a finished job back to Running.
        let resolved = JobMetadata::resolve(&state_dir, "reus01").unwrap();
        let status = compute_status(&resolved, &state_dir);
        assert_ne!(
            status,
            JobStatus::Running,
            "a pid_start_time mismatch must not look Running"
        );

        // (b)+(c) `kill` refuses without signaling and WITHOUT marking the
        // row killed (it returns Ok with a not-running report before ever
        // reaching terminate_process / mark_job_killed).
        let output = run(
            &config,
            &JobIdArg {
                job_id: "reus01".to_string(),
            },
        )
        .expect("kill of a finished job must be Ok, not a hard error");
        assert_eq!(output.exit_code, 0);
        let lines: String = output.stdout_lines.join("\n");
        assert!(
            lines.contains("is not currently running"),
            "expected a not-running report, got: {}",
            lines
        );
        assert!(
            !lines.contains("terminated successfully"),
            "the kill path must not have been entered"
        );
        assert!(
            !lines.contains("SIGTERM"),
            "no signal must be mentioned, got: {}",
            lines
        );
        // The unrelated process holding the recycled PID was never touched.
        assert!(
            is_pid_alive(pid),
            "the recycled PID's process must NOT have been signalled"
        );
        // And the row is byte-for-byte what the runner wrote: no killed
        // status, no fabricated 137 exit code.
        let reloaded = JobMetadata::load(&state_dir, "reus01").unwrap();
        assert_eq!(
            reloaded.status, None,
            "a pid-reused finished job must never be marked killed"
        );
        assert_ne!(
            reloaded.exit_code,
            Some(137),
            "a fabricated 137 must never be recorded"
        );
        assert_eq!(reloaded.exit_code, Some(1));

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn test_kill_execute_rejects_recycled_pid_before_sigterm() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let real_start = get_process_start_time(pid);

        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_dir).unwrap();

        let meta = JobMetadata {
            job_id: "recyc01".to_string(),
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
            worktree_path: temp.path().join("wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: Some(pid),
            pid_start_time: Some(real_start.unwrap_or(100) + 99999), // Stale/wrong start time
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
        };
        meta.save(&state_dir).unwrap();

        let config = Config {
            state_root: state_dir,
            ..Default::default()
        };

        let res = run(
            &config,
            &JobIdArg {
                job_id: "recyc01".to_string(),
            },
        );
        // A row whose start time no longer matches the live PID is no longer
        // classified Running, so `run` reports the job as not running (Ok,
        // exit 0) instead of signaling the recycled PID (which used to be a
        // hard "Sanity check failed" error).
        let output = res.expect("kill must not hard-error on a recycled PID");
        assert_eq!(output.exit_code, 0, "not-running kill must exit 0");

        let lines: String = output.stdout_lines.join("\n");
        assert!(
            lines.contains("Job 'recyc01' is not currently running (status: self-terminated)"),
            "expected a not-running report, got: {}",
            lines
        );
        assert!(
            !lines.contains("terminated successfully"),
            "the kill path must not have been entered"
        );
        assert!(
            is_pid_alive(pid),
            "Process must NOT have been killed by execute"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn test_terminate_process_rejects_bumped_retry_count_before_sigterm() {
        use std::os::unix::process::CommandExt;
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(50));

        let real_start = get_process_start_time(pid);

        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_dir).unwrap();

        // Stored job record has moved on to retry_count: 1 (simulating a retry that occurred)
        let meta = JobMetadata {
            job_id: "retryguard01".to_string(),
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
            worktree_path: temp.path().join("wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 1, // Current attempt in DB is 1
            pid: Some(pid),
            pid_start_time: real_start,
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
        };
        meta.save(&state_dir).unwrap();

        let config = Config {
            state_root: state_dir,
            ..Default::default()
        };

        // Caller captured retry_count: 0 before the retry happened
        let res = terminate_process(&config, "retryguard01", pid, real_start, Some(0));
        assert!(
            res.is_err(),
            "terminate_process must fail when stored retry count has changed"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("Sanity check failed"),
            "expected sanity check failure, got: {}",
            err_msg
        );
        assert!(
            err_msg.contains("retry count changed from 0 to 1"),
            "expected retry count change error, got: {}",
            err_msg
        );

        // Process must still be alive! Proves SIGTERM was never sent.
        assert!(
            is_pid_alive(pid),
            "Process must NOT have been signaled after retry count mismatch was detected"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn test_kill_finished_job_with_recycled_pid_not_running() {
        // The job has already finished, but its pid field now points at a
        // live, unrelated process (PID recycled). This test uses the
        // test process's own PID as the "recycled" PID: the row records a
        // start time that does not match it, so `compute_status` classifies
        // the job from its exit code (1 -> Failed) instead of reporting
        // Running. The OLD flow matched `meta.pid` on raw `is_pid_alive`
        // and would enter the kill path against this innocent process.
        let live_unrelated_pid = std::process::id();

        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_dir).unwrap();

        let meta = JobMetadata {
            job_id: "finrec01".to_string(),
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
            worktree_path: temp.path().join("wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: Some(live_unrelated_pid),
            // The original (dead) job process's start time: differs from
            // whatever process now holds the recycled PID.
            pid_start_time: Some(1),
            exit_code: Some(1),
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
        };
        meta.save(&state_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            ..Default::default()
        };

        // The computed status is Failed: the live recycled PID must not
        // flip a finished job back to Running (start time mismatch).
        let resolved = JobMetadata::resolve(&state_dir, "finrec01").unwrap();
        assert_eq!(
            compute_status(&resolved, &state_dir),
            JobStatus::Failed,
            "a finished job must not report Running just because its PID is alive"
        );

        let res = run(
            &config,
            &JobIdArg {
                job_id: "finrec01".to_string(),
            },
        );
        let output =
            res.expect("kill of a finished job must return Ok (exit code 0), not a hard error");
        assert_eq!(output.exit_code, 0, "not-running kill must exit 0");

        let lines: String = output.stdout_lines.join("\n");
        assert!(
            lines.contains("Job 'finrec01' is not currently running (status: failed)"),
            "expected a not-running report, got: {}",
            lines
        );
        assert!(
            !lines.contains("terminated successfully"),
            "the kill path must not have been entered"
        );
        assert!(
            output.data.get("terminated").and_then(|v| v.as_bool()) == Some(false),
            "data payload must say the job was not terminated"
        );

        // The unrelated process holding the recycled PID was never signalled.
        assert!(
            is_pid_alive(live_unrelated_pid),
            "the recycled PID's process must NOT have been signalled"
        );
    }
}

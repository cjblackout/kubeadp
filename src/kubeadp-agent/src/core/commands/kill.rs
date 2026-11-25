use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::ffi::OsStr;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use crate::core::args::JobIdArg;
use crate::core::config::Config;
use crate::core::job::{compute_status, is_pid_alive, mark_job_killed, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

pub use crate::core::job::get_process_start_time;

/// Returns the basename stem of a configured engine path (e.g.
/// `/usr/local/libexec/agy` -> `agy`, `/usr/local/bin/claude` -> `claude`, a
/// fake test script -> its filename), lowercased. The stem is the token the
/// cmdline match compares against, since the configured path may be a
/// symlink/copy (in tests the fake harness has an arbitrary temp filename).
fn engine_stem(path: &Path) -> String {
    path.file_name()
        .and_then(OsStr::to_str)
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_default()
}

/// Sends `SIG<sig_name>` (e.g. "TERM", "KILL") to `pid`.
///
/// Jobs always run as the server's own process user, so a direct `libc::kill()`
/// is always permitted. The process group is signaled first (the agy child is a
/// session/process-group leader, see `detached_session`), falling back to the
/// single PID when the group no longer exists (ESRCH).
/// Extracts executable/command basenames from a NUL-separated `/proc/<pid>/cmdline`
/// buffer so launch targets are matched as command tokens rather than arbitrary
/// substrings inside flag values or file paths.
///
/// Inspects:
/// - `argv[0]` basename,
/// - when `argv[0]` is a shell (`sh`, `bash`, `dash`) or env/runtime wrapper
///   (`env`, `python3`, `node`), the first non-flag positional script basename
///   or `-c` command's leading word,
/// - any token immediately following `--` (for `sandbox-exec --policy ... -- <bin>`),
/// - `argv[1]` when `argv[0]` is `kubeadp-agent` / `agent-run` (`sandbox-exec`).
pub(crate) fn cmdline_matches_expected_targets(cmdline: &[u8], expected: &[&str]) -> bool {
    let args: Vec<&str> = cmdline
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .filter_map(|s| std::str::from_utf8(s).ok())
        .collect();
    if args.is_empty() {
        return false;
    }

    let matches_target = |candidate: &str| -> bool {
        let base = Path::new(candidate.trim())
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or(candidate.trim())
            .to_ascii_lowercase();
        if base.is_empty() {
            return false;
        }
        expected.iter().any(|&t| {
            !t.is_empty()
                && (base == t
                    || base
                        .strip_prefix(t)
                        .is_some_and(|rest| rest.starts_with('-') || rest.starts_with('.')))
        })
    };

    // 1. Direct executable (`argv[0]`).
    if matches_target(args[0]) {
        return true;
    }

    // 2. Subcommand or wrapped binary after `--` (e.g. `sandbox-exec ... -- /path/to/claude`).
    for (idx, &arg) in args.iter().enumerate() {
        if arg == "--" {
            if let Some(&wrapped) = args.get(idx + 1) {
                if matches_target(wrapped) {
                    return true;
                }
            }
        }
    }
    if let Some(&subcmd) = args.get(1) {
        if subcmd == "sandbox-exec" && matches_target(subcmd) {
            return true;
        }
    }

    // 3. Interpreter / shell invocation (`sh /tmp/.../fake-agy.sh` or `sh -c "sleep 30"`).
    let argv0_base = Path::new(args[0])
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or(args[0])
        .to_ascii_lowercase();
    if matches!(argv0_base.as_str(), "sh" | "bash" | "dash" | "zsh" | "env") {
        let mut i = 1;
        while i < args.len() {
            let a = args[i];
            if a == "-c" {
                if let Some(&script) = args.get(i + 1) {
                    for sep in script.split([';', '&', '|', '\n']) {
                        if let Some(first_word) = sep.split_whitespace().next() {
                            if matches_target(first_word) {
                                return true;
                            }
                        }
                    }
                }
                break;
            } else if !a.starts_with('-') && !a.contains('=') {
                if matches_target(a) {
                    return true;
                }
                break;
            }
            i += 1;
        }
    }

    false
}

/// Lightweight sanity check to guard against PID reuse before signaling.
///
/// Verifies `/proc/<pid>/cmdline` or `/proc/<pid>/exe` still looks like an expected
/// agent process: the engine the job actually used (`agy` for agy jobs, the
/// `claude` harness for claude jobs), the always-on `sandbox-exec` wrapper both
/// engines run under, the `agent-run`/`kubeadp-agent` launchers, or test
/// runner processes like `sleep`.
///
/// Command basenames are compared against the launch-target tokens, so a
/// `claude` job whose recorded PID is the `sandbox-exec` wrapper
/// (`sandbox-exec --policy <file> -- /usr/local/bin/claude ...`) passes while
/// an unrelated process whose arguments happen to contain a target name as a
/// substring cannot masquerade as one. Fails closed (`false`) when neither
/// `/proc/<pid>/cmdline` nor `/proc/<pid>/exe` can be inspected.
pub(crate) fn is_pid_plausible_job_process(config: &Config, pid: u32) -> bool {
    let agy_stem = engine_stem(&config.agy_path);
    let claude_stem = engine_stem(&config.claude_path);
    let mut expected: Vec<&str> = vec![
        "agy",
        "claude",
        "sandbox-exec",
        "agent-run",
        "kubeadp-agent",
        "kubeadp_agent",
        "agent_core",
        "sleep",
    ];
    if !agy_stem.is_empty() {
        expected.push(agy_stem.as_str());
    }
    if !claude_stem.is_empty() {
        expected.push(claude_stem.as_str());
    }

    let cmdline_path = format!("/proc/{}/cmdline", pid);
    if let Ok(cmdline) = std::fs::read(&cmdline_path) {
        if !cmdline.is_empty() {
            return cmdline_matches_expected_targets(&cmdline, &expected);
        }
    }

    let exe_path = format!("/proc/{}/exe", pid);
    if let Ok(target) = std::fs::read_link(&exe_path) {
        let path_str = target.to_string_lossy();
        let clean = path_str.strip_suffix(" (deleted)").unwrap_or(&path_str);
        let base = Path::new(clean)
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or(clean)
            .to_ascii_lowercase();
        return expected.iter().any(|&t| {
            !t.is_empty()
                && (base == t
                    || base
                        .strip_prefix(t)
                        .is_some_and(|rest| rest.starts_with('-') || rest.starts_with('.')))
        });
    }

    false
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
        // Re-verify process identity immediately before escalating to SIGKILL:
        // if the PID no longer refers to the target process, it has already terminated
        // and may have been reused by an unrelated process.
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

            // Immediately before sending the escalated SIGKILL signal, re-verify
            // process identity so a process that exited right at grace period expiry
            // and had its PID reused is never killed.
            if !identity.is_still_target(config) {
                return Ok(());
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
    run_with_meta(config, &args.job_id, meta)
}

/// Terminates an exact job record (without prefix resolution).
pub fn run_exact(config: &Config, job_id: &str) -> Result<CommandOutput> {
    let meta = JobMetadata::load(&config.state_root, job_id)?;
    run_with_meta(config, job_id, meta)
}

fn run_with_meta(config: &Config, job_id: &str, meta: JobMetadata) -> Result<CommandOutput> {
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
            job_id, status
        ));
        return Ok(output);
    }

    let pid = match meta.pid {
        Some(p) if is_pid_alive(p) => p,
        _ => {
            output.push_out(format!(
                "Job '{}' is not currently running (status: {}).",
                job_id, status
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

    /// Writes an executable shell script into `dir` that sleeps forever (a
    /// `trap '' TERM` keeps it alive against a misdirected signal during the
    /// test), so a process spawned through it keeps its argv visible in
    /// `/proc/<pid>/cmdline` long enough for the identity checks below.
    fn write_sleep_script(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(
            &p,
            "#!/bin/sh\ntrap '' TERM\nwhile true; do sleep 1; done\n",
        )
        .unwrap();
        std::process::Command::new("chmod")
            .arg("+x")
            .arg(&p)
            .status()
            .unwrap();
        p
    }

    /// Reaps a spawned fake-engine child (kills + waits). Leaves nothing behind.
    fn reap_child(mut child: std::process::Child) {
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn test_plausibility_accepts_real_claude_and_agy_launch_shapes() {
        // Defect 1 (HL-71 S1): a claude job's recorded PID is the always-on
        // `sandbox-exec` wrapper (`sandbox-exec --policy <file> --
        // /usr/local/bin/claude --dangerously-skip-permissions -p ...`), and
        // the old name-only check (`agy`/`agent-run`/`sleep`) rejected it,
        // so `agent-run kill` refused and the job kept running. The check must
        // accept the engine launch target the JOB actually used: the claude
        // harness, the agy binary, and the wrapping sandbox-exec (whose cmdline
        // names the wrapped engine).
        let dir = tempfile::tempdir().unwrap();
        let sandbox_exec = write_sleep_script(dir.path(), "sandbox-exec");
        let claude = write_sleep_script(dir.path(), "claude");
        let agy = write_sleep_script(dir.path(), "agy");

        let config = Config {
            agy_path: agy.clone(),
            claude_path: claude.clone(),
            ..Default::default()
        };

        // The claude wrapper shape (the live recorded-PID cmdline).
        let wrapper = Command::new(&sandbox_exec)
            .arg("--policy")
            .arg("/state/logs/j1.policy.json")
            .arg("--")
            .arg(&claude)
            .arg("--dangerously-skip-permissions")
            .arg("-p")
            .arg("--output-format")
            .arg("stream-json")
            .spawn()
            .expect("spawn sandbox-exec wrapper");
        std::thread::sleep(Duration::from_millis(50));
        let wrapper_pid = wrapper.id();
        assert!(
            is_pid_plausible_job_process(&config, wrapper_pid),
            "the claude sandbox-exec wrapper must look like a job process"
        );
        // The wrapper is the launch target the job killer must accept even
        // against the recorded start time.
        let identity = ProcessIdentity {
            pid: wrapper_pid,
            start_time: get_process_start_time(wrapper_pid),
        };
        assert!(
            identity.is_still_target(&config),
            "the claude wrapper must pass the full identity check"
        );
        reap_child(wrapper);

        // The claude child itself (whenever the harness is the recorded PID).
        let claude_child = Command::new(&claude)
            .arg("--dangerously-skip-permissions")
            .arg("-p")
            .spawn()
            .expect("spawn claude child");
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            is_pid_plausible_job_process(&config, claude_child.id()),
            "a direct claude child must look like a job process"
        );
        reap_child(claude_child);

        // The agy child itself (agy engine).
        let agy_child = Command::new(&agy)
            .arg("--mode")
            .arg("accept-edits")
            .spawn()
            .expect("spawn agy child");
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            is_pid_plausible_job_process(&config, agy_child.id()),
            "an agy child must look like a job process"
        );
        reap_child(agy_child);
    }

    #[test]
    fn test_plausibility_rejects_unrelated_process_and_reused_pid() {
        let dir = tempfile::tempdir().unwrap();
        let claude = write_sleep_script(dir.path(), "claude");
        let config = Config {
            agy_path: dir.path().join("agy"),
            claude_path: claude.clone(),
            ..Default::default()
        };

        // (1) A claude-shaped process matches, and a fabricated start-time
        //     mismatch (PID reuse) is detected even though the cmdline still
        //     looks like a job process.
        let claude_child = Command::new(&claude)
            .arg("--dangerously-skip-permissions")
            .arg("-p")
            .spawn()
            .expect("spawn claude child");
        std::thread::sleep(Duration::from_millis(50));
        let pid = claude_child.id();
        let real_start = get_process_start_time(pid);
        assert!(
            is_pid_plausible_job_process(&config, pid),
            "the claude-shaped process must be plausible"
        );
        let reused = ProcessIdentity {
            pid,
            start_time: Some(real_start.unwrap_or(100) + 99999),
        };
        assert!(
            !reused.is_still_target(&config),
            "a start-time mismatch must restore the reused-PID safety"
        );
        reap_child(claude_child);

        // (2) An unrelated process (a shell one-liner whose cmdline shares no
        //     launch-target token -- no 'sleep', no engine name) is NOT a job
        //     process.
        let unrelated = Command::new("sh")
            .arg("-c")
            .arg("trap '' TERM; while true; do :; done")
            .spawn()
            .expect("spawn unrelated process");
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !is_pid_plausible_job_process(&config, unrelated.id()),
            "an unrelated process (sh -c loop) must not look like a job process"
        );
        reap_child(unrelated);
    }

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
            engine: None,
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
            engine: None,
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
            engine: None,
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
            engine: None,
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
            engine: None,
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

    #[test]
    fn test_cmdline_matches_expected_targets_rejects_argument_substrings() {
        let expected = &[
            "agy",
            "claude",
            "sandbox-exec",
            "agent-run",
            "kubeadp-agent",
            "sleep",
        ];

        // Unrelated command whose arguments contain "claude", "agy", or "sleep"
        // as path segments or flag values must NOT match.
        let unrelated_python =
            b"python3\0/workspace/claude-eval/run_agy_benchmark.py\0--sleep=10\0";
        assert!(
            !cmdline_matches_expected_targets(unrelated_python, expected),
            "argument substring must not match as a job process"
        );

        let unrelated_cat = b"/usr/bin/cat\0/tmp/agent-run-notes.txt\0";
        assert!(
            !cmdline_matches_expected_targets(unrelated_cat, expected),
            "file path argument containing agent-run must not match"
        );

        // Legitimate invocations must match.
        let direct_claude = b"/usr/local/bin/claude\0-p\0hello\0";
        assert!(cmdline_matches_expected_targets(direct_claude, expected));

        let sandbox_wrapped =
            b"/usr/local/bin/kubeadp-agent\0sandbox-exec\0--policy\0/tmp/p.json\0--\0/usr/bin/env\0claude\0";
        assert!(cmdline_matches_expected_targets(sandbox_wrapped, expected));

        let sh_fake_agy = b"/bin/sh\0/tmp/test/fake-agy.sh\0--print\0";
        assert!(cmdline_matches_expected_targets(
            sh_fake_agy,
            &["agy", "fake-agy.sh"]
        ));

        let sh_c_sleep = b"sh\0-c\0sleep 30\0";
        assert!(cmdline_matches_expected_targets(sh_c_sleep, expected));

        // Non-existent PID fails closed (`false`).
        let cfg = Config::default();
        assert!(
            !is_pid_plausible_job_process(&cfg, u32::MAX),
            "unreadable /proc/<pid> must fail closed"
        );
    }
}

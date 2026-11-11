use crate::orchestrate::logging::log_event;
use std::collections::HashMap;
use std::env;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Resolves and checks if binary is reachable and executable.
/// If binary contains '/', checks it directly.
/// Otherwise searches PATH: checks session `env_vars["PATH"]` first, then process `PATH`.
pub fn resolve_executable_binary(
    binary: &Path,
    env_vars: &HashMap<String, String>,
) -> Option<std::path::PathBuf> {
    let binary_str = binary.to_string_lossy();
    if binary_str.contains('/') {
        if binary.exists() {
            let c_path = CString::new(binary.as_os_str().as_bytes()).ok()?;
            if unsafe { libc::access(c_path.as_ptr(), libc::X_OK) == 0 } {
                return Some(binary.to_path_buf());
            }
        }
        None
    } else {
        let path_var = env_vars
            .get("PATH")
            .cloned()
            .or_else(|| env::var("PATH").ok())?;
        for dir in env::split_paths(&path_var) {
            let candidate = dir.join(binary);
            if candidate.exists() {
                if let Ok(c_path) = CString::new(candidate.as_os_str().as_bytes()) {
                    if unsafe { libc::access(c_path.as_ptr(), libc::X_OK) == 0 } {
                        return Some(candidate);
                    }
                }
            }
        }
        None
    }
}

pub fn is_executable_binary(binary: &Path) -> bool {
    resolve_executable_binary(binary, &HashMap::new()).is_some()
}

/// Engine-agnostic readiness for a session that needs no login gate (an agy
/// session, HL-91 E5 cross-use): the binary must resolve (and be executable);
/// agy has no `auth status` concept, so a successful resolution is enough.
pub fn binary_ready(
    binary: &Path,
    env_vars: &HashMap<String, String>,
    timeout: Duration,
    shutdown: &AtomicBool,
) -> (bool, String) {
    if resolve_executable_binary(binary, env_vars).is_some() {
        let _ = timeout;
        (true, "binary-found".to_string())
    } else {
        let _ = shutdown;
        (false, "binary-missing".to_string())
    }
}

/// Checks readiness by running `claude auth status --json`.
///
/// Fix #6:
/// * Takes the `shutdown` flag and aborts promptly if set during the 30s wait loop.
/// * On EVERY exit path (timeout, try_wait error, shutdown) the child is killed
///   and wait()ed so no zombie/orphan `claude auth status` is left.
/// * Non-zero exit → `status-failed (rc=..)` regardless of stdout content.
/// * `not-logged-in` only when rc==0 and `loggedIn` is not `true`.
///
/// Returns `(is_ready, reason)`.
pub fn claude_readiness(
    binary: &Path,
    env_vars: &HashMap<String, String>,
    timeout: Duration,
    shutdown: &AtomicBool,
) -> (bool, String) {
    let resolved_binary = match resolve_executable_binary(binary, env_vars) {
        Some(b) => b,
        None => return (false, "binary-missing".to_string()),
    };

    let mut cmd = Command::new(&resolved_binary);
    cmd.args(["auth", "status", "--json"]);
    cmd.envs(env_vars);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let start = Instant::now();
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return (false, format!("binary-broken ({})", e)),
    };

    // Helper: kill + reap child before returning.
    // We use a macro so the borrow checker is satisfied for each early return.
    macro_rules! kill_and_wait {
        ($child:expr) => {{
            let _ = $child.kill();
            let _ = $child.wait();
        }};
    }

    let mut exited = false;
    let mut exit_status = None;

    while start.elapsed() < timeout {
        // Abort promptly on shutdown (fix #6).
        if shutdown.load(Ordering::SeqCst) {
            kill_and_wait!(child);
            return (false, "shutdown".to_string());
        }

        match child.try_wait() {
            Ok(Some(status)) => {
                exited = true;
                exit_status = Some(status);
                break;
            }
            Ok(None) => thread::sleep(Duration::from_millis(50)),
            Err(e) => {
                kill_and_wait!(child);
                return (false, format!("binary-broken ({})", e));
            }
        }
    }

    if !exited {
        // Timeout: kill + reap so no orphan/zombie.
        kill_and_wait!(child);
        return (false, "binary-broken (timeout)".to_string());
    }

    // Child exited normally; collect output.
    let output = match child.wait_with_output() {
        Ok(o) => o,
        Err(e) => return (false, format!("binary-broken ({})", e)),
    };

    // Fix #6: check rc FIRST.
    let rc = exit_status.as_ref().and_then(|s| s.code()).unwrap_or(-1);

    if rc != 0 {
        // Non-zero rc → status-failed regardless of stdout.
        let stderr_tail = {
            let mut start = output.stderr.len().saturating_sub(200);
            while start < output.stderr.len() && (output.stderr[start] & 0xC0) == 0x80 {
                start += 1;
            }
            String::from_utf8_lossy(&output.stderr[start..])
                .trim()
                .to_string()
        };
        return (
            false,
            format!(
                "status-failed (rc={}{})",
                rc,
                if stderr_tail.is_empty() {
                    String::new()
                } else {
                    format!(": {}", stderr_tail)
                }
            ),
        );
    }

    // rc == 0: check loggedIn.
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
        if val.get("loggedIn") == Some(&serde_json::Value::Bool(true)) {
            return (true, "ok".to_string());
        }
    }

    (false, "not-logged-in".to_string())
}

/// Blocks until the engine binary is installed and logged in (claude uses
/// `auth status --json`), logging changes in reason.
/// Can be cleanly interrupted by `shutdown`.
pub fn wait_until_ready(
    binary: &Path,
    env_vars: &HashMap<String, String>,
    session_name: &str,
    poll_interval: Duration,
    shutdown: &AtomicBool,
) -> (bool, String) {
    let mut last_reason: Option<String> = None;
    let timeout = Duration::from_secs(30);

    while !shutdown.load(Ordering::SeqCst) {
        let (ready, reason) = claude_readiness(binary, env_vars, timeout, shutdown);
        if ready {
            return (true, reason);
        }
        if reason == "shutdown" {
            return (false, "shutdown".to_string());
        }

        if last_reason.as_deref() != Some(&reason) {
            if reason == "not-logged-in" {
                log_event(
                    session_name,
                    "Claude Code is not logged in; run 'claude auth login' in the code-server terminal. Waiting (no restart needed).",
                );
            } else {
                log_event(
                    session_name,
                    &format!(
                        "Claude Code unavailable: {}. This is NOT an auth problem; check the image. Retrying every {:.0}s.",
                        reason,
                        poll_interval.as_secs_f64()
                    ),
                );
            }
            last_reason = Some(reason.clone());
        }

        // Sleep poll_interval in 100ms chunks to respond immediately to shutdown
        let poll_start = Instant::now();
        while poll_start.elapsed() < poll_interval {
            if shutdown.load(Ordering::SeqCst) {
                return (false, "shutdown".to_string());
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    (false, "shutdown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn make_shutdown() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    fn test_env_with_bin(bin_dir: &Path) -> HashMap<String, String> {
        let mut env_vars = HashMap::new();
        let current_path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_string());
        env_vars.insert(
            "PATH".to_string(),
            format!("{}:{}", bin_dir.display(), current_path),
        );
        env_vars
    }

    static DIR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let cnt = DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "{}_{}_{}_{}",
            prefix,
            std::process::id(),
            cnt,
            nanos
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    /// Helper: write a tiny executable shell script.
    fn write_script(path: &Path, content: &str) {
        std::fs::write(path, content).unwrap();
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }

    #[test]
    fn test_missing_binary() {
        let shutdown = make_shutdown();
        let (ready, reason) = claude_readiness(
            Path::new("/nonexistent/binary/path/12345"),
            &HashMap::new(),
            Duration::from_secs(1),
            &shutdown,
        );
        assert!(!ready);
        assert_eq!(reason, "binary-missing");
    }

    #[test]
    fn test_binary_lookup_in_session_env_path() {
        let temp_dir = unique_test_dir("test_readiness_path");
        let custom_claude = temp_dir.join("my-custom-claude");

        write_script(&custom_claude, "#!/bin/sh\necho '{\"loggedIn\": true}'\n");

        let env_vars = test_env_with_bin(&temp_dir);

        let shutdown = make_shutdown();
        let (ready, reason) = claude_readiness(
            Path::new("my-custom-claude"),
            &env_vars,
            Duration::from_secs(2),
            &shutdown,
        );
        assert!(ready);
        assert_eq!(reason, "ok");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    /// Fix #6: non-zero rc with junk stdout → status-failed (not not-logged-in).
    #[test]
    fn test_nonzero_rc_with_junk_stdout_is_status_failed() {
        let temp_dir = unique_test_dir("test_readiness_rc");
        let script = temp_dir.join("fake-claude-rc1");

        write_script(&script, "#!/bin/sh\necho 'junk output'\nexit 1\n");

        let env_vars = test_env_with_bin(&temp_dir);

        let shutdown = make_shutdown();
        let (ready, reason) = claude_readiness(
            Path::new("fake-claude-rc1"),
            &env_vars,
            Duration::from_secs(2),
            &shutdown,
        );
        assert!(!ready);
        assert!(
            reason.starts_with("status-failed (rc=1"),
            "expected status-failed, got: {}",
            reason
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    /// Fix #6: rc==0 but loggedIn is false → not-logged-in.
    #[test]
    fn test_rc0_not_logged_in() {
        let temp_dir = unique_test_dir("test_readiness_notlogged");
        let script = temp_dir.join("fake-claude-notlogged");

        write_script(&script, "#!/bin/sh\necho '{\"loggedIn\": false}'\nexit 0\n");

        let env_vars = test_env_with_bin(&temp_dir);

        let shutdown = make_shutdown();
        let (ready, reason) = claude_readiness(
            Path::new("fake-claude-notlogged"),
            &env_vars,
            Duration::from_secs(2),
            &shutdown,
        );
        assert!(!ready);
        assert_eq!(reason, "not-logged-in");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    /// Fix #6: hanging child is killed and reaped on timeout (no zombie/orphan).
    #[test]
    fn test_hanging_child_killed_on_timeout() {
        let temp_dir = unique_test_dir("test_readiness_hang");
        let script = temp_dir.join("fake-claude-hang");
        let pid_file = temp_dir.join("child.pid");

        // Write PID to file then sleep
        write_script(
            &script,
            &format!(
                "#!/bin/sh\necho $$ > \"{}\"\nexec sleep 300\n",
                pid_file.display()
            ),
        );

        let env_vars = test_env_with_bin(&temp_dir);

        let shutdown = make_shutdown();
        let before = std::time::Instant::now();
        let (ready, reason) = claude_readiness(
            Path::new("fake-claude-hang"),
            &env_vars,
            Duration::from_millis(200),
            &shutdown,
        );
        let elapsed = before.elapsed();
        assert!(!ready);
        assert!(
            reason.contains("timeout"),
            "expected timeout reason, got: {}",
            reason
        );
        // Must return promptly after timeout (not block for 300s).
        assert!(
            elapsed < Duration::from_secs(2),
            "took too long: {:?}",
            elapsed
        );

        // Verify no leftover process: child pid must be gone
        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
            if let Ok(cpid) = pid_str.trim().parse::<i32>() {
                // Wait briefly for SIGKILL cleanup if needed
                std::thread::sleep(Duration::from_millis(50));
                let res = unsafe { libc::kill(cpid, 0) };
                assert_ne!(res, 0, "child process {} must be terminated", cpid);
            }
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    /// Fix #6: shutdown signal aborts the wait promptly and terminates child.
    #[test]
    fn test_shutdown_aborts_wait() {
        let temp_dir = unique_test_dir("test_readiness_shut");
        let script = temp_dir.join("fake-claude-shut");
        let pid_file = temp_dir.join("child.pid");

        write_script(
            &script,
            &format!(
                "#!/bin/sh\necho $$ > \"{}\"\nexec sleep 60\n",
                pid_file.display()
            ),
        );

        let env_vars = test_env_with_bin(&temp_dir);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_c = Arc::clone(&shutdown);

        // Set shutdown after 100ms.
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            shutdown_c.store(true, Ordering::SeqCst);
        });

        let before = std::time::Instant::now();
        let (ready, reason) = claude_readiness(
            Path::new("fake-claude-shut"),
            &env_vars,
            Duration::from_secs(30),
            &shutdown,
        );
        let elapsed = before.elapsed();

        let _ = handle.join();

        assert!(!ready);
        assert_eq!(reason, "shutdown");
        // Must abort well before the 30s timeout.
        assert!(
            elapsed < Duration::from_secs(2),
            "shutdown abort took too long: {:?}",
            elapsed
        );

        // Verify no leftover process
        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
            if let Ok(cpid) = pid_str.trim().parse::<i32>() {
                std::thread::sleep(Duration::from_millis(50));
                let res = unsafe { libc::kill(cpid, 0) };
                assert_ne!(
                    res, 0,
                    "child process {} must be terminated on shutdown",
                    cpid
                );
            }
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}

//! Lifecycle management of the agy remote-control daemon.
//!
//! Controls `<agy_path> remote-control start|stop|status` so the machine running
//! `agent-run-api` appears in the Antigravity "Remote Control" instance list under
//! a static hostname.

use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::core::args::{RemoteControlAction, RemoteControlArgs};
use crate::core::config::Config;
use crate::core::error::AgentRunError;
use crate::core::output::CommandOutput;

/// Maximum time to wait for agy remote-control commands to finish.
const REMOTE_CONTROL_TIMEOUT: Duration = Duration::from_secs(180);

/// Parsed representation of `agy remote-control status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteControlStatus {
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_name: Option<String>,
    pub message: String,
}

/// Reads the local hostname using `libc::gethostname`.
pub fn get_hostname() -> Result<String> {
    let mut buf = vec![0u8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret != 0 {
        return Err(AgentRunError::internal(
            "remote_control_failed",
            format!(
                "failed to get hostname: {}",
                std::io::Error::last_os_error()
            ),
        )
        .into());
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let s = String::from_utf8_lossy(&buf[..len]).trim().to_string();
    if s.is_empty() {
        return Err(AgentRunError::internal("remote_control_failed", "hostname is empty").into());
    }
    Ok(s)
}

/// Builds the argument vector for `enable`.
pub fn build_enable_args(hostname: &str) -> Vec<String> {
    vec![
        "remote-control".to_string(),
        "start".to_string(),
        "--session".to_string(),
        "--name".to_string(),
        hostname.to_string(),
    ]
}

/// Builds the argument vector for `disable`.
pub fn build_disable_args() -> Vec<String> {
    vec!["remote-control".to_string(), "stop".to_string()]
}

/// Builds the argument vector for `status`.
pub fn build_status_args() -> Vec<String> {
    vec!["remote-control".to_string(), "status".to_string()]
}

/// Extracts PID from a status line (e.g. `Daemon status: active (background process, PID 12345)`).
fn extract_pid(s: &str) -> Option<u32> {
    let lower = s.to_lowercase();
    if let Some(idx) = lower.find("pid") {
        let rest = &s[idx + 3..];
        let digits: String = rest
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits.parse::<u32>().ok()
    } else {
        None
    }
}

/// Parses the standard stdout of `agy remote-control status`.
pub fn parse_status(stdout: &str) -> RemoteControlStatus {
    let mut lines = stdout.lines();
    let message = lines
        .next()
        .unwrap_or("")
        .trim_end_matches('\r')
        .trim()
        .to_string();

    let lower_msg = message.to_lowercase();
    let active = lower_msg.contains("active") && !lower_msg.contains("inactive");
    let pid = if active { extract_pid(&message) } else { None };

    let mut instance_name = None;
    for line in stdout.lines() {
        let trimmed = line.trim_end_matches('\r').trim();
        let lower = trimmed.to_lowercase();
        if lower.starts_with("instance name:") {
            let rest = trimmed["instance name:".len()..].trim();
            if !rest.to_lowercase().starts_with("not assigned yet") && !rest.is_empty() {
                let name = rest
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .trim_matches('"');
                if !name.is_empty() {
                    instance_name = Some(name.to_string());
                }
            }
            break;
        }
    }

    RemoteControlStatus {
        active,
        pid,
        instance_name,
        message,
    }
}

/// Summarises and bounds stderr output to at most 500 characters.
fn summarize_stderr(stderr: &str) -> String {
    let trimmed = stderr.trim();
    if trimmed.chars().count() > 500 {
        let truncated: String = trimmed.chars().take(497).collect();
        format!("{truncated}...")
    } else {
        trimmed.to_string()
    }
}

/// Helper to read all bytes from a pipe into a Vec.
fn read_pipe_to_vec(mut pipe: impl std::io::Read) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = std::io::Read::read_to_end(&mut pipe, &mut buf);
    buf
}

/// Spawns `<config.agy_path> <args...>` without a shell, applying proxy settings,
/// tracking the child in the server reaper, draining output pipes via reader threads,
/// and bounding the execution duration to `timeout`.
fn execute_agy(config: &Config, args: &[String], timeout: Duration) -> Result<(String, String)> {
    let agy = config.agy_path.to_string_lossy().to_string();
    let mut cmd = Command::new(&config.agy_path);
    cmd.args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    crate::core::proxy::apply_proxy(&mut cmd, Some(config), None);

    let mut child = cmd.spawn().map_err(|e| {
        AgentRunError::internal(
            "remote_control_failed",
            format!("failed to spawn `{agy}`: {e}"),
        )
    })?;

    let tracked = crate::core::reaper::TrackedChild::new(child.id());
    let stdout_pipe = child.stdout.take().ok_or_else(|| {
        AgentRunError::internal("remote_control_failed", "failed to capture stdout")
    })?;
    let stderr_pipe = child.stderr.take().ok_or_else(|| {
        AgentRunError::internal("remote_control_failed", "failed to capture stderr")
    })?;

    let stdout_reader = std::thread::spawn(move || read_pipe_to_vec(stdout_pipe));
    let stderr_reader = std::thread::spawn(move || read_pipe_to_vec(stderr_pipe));

    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let status = loop {
        let poll_status = match child.try_wait() {
            Ok(status) => status,
            Err(e) => match tracked.recover_echild(&e) {
                Some(status) => Some(status),
                None => {
                    return Err(AgentRunError::internal(
                        "remote_control_failed",
                        format!("`{agy}` wait error: {e}"),
                    )
                    .into());
                }
            },
        };
        match poll_status {
            Some(status) => break Some(status),
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    let stdout_buf = stdout_reader.join().unwrap_or_default();
    let stderr_buf = stderr_reader.join().unwrap_or_default();
    let stdout = String::from_utf8_lossy(&stdout_buf).to_string();
    let stderr = String::from_utf8_lossy(&stderr_buf).to_string();

    let cmd_str = args.join(" ");
    if timed_out {
        let summary = summarize_stderr(&stderr);
        let msg = if summary.is_empty() {
            format!("`{agy} {cmd_str}` timed out after {} s", timeout.as_secs())
        } else {
            format!(
                "`{agy} {cmd_str}` timed out after {} s: {summary}",
                timeout.as_secs()
            )
        };
        return Err(AgentRunError::internal("remote_control_failed", msg).into());
    }

    let status = status.ok_or_else(|| {
        AgentRunError::internal(
            "remote_control_failed",
            format!("`{agy} {cmd_str}` exited unexpectedly"),
        )
    })?;

    if !status.success() {
        let code = crate::core::runner::exit_code_from_status(status);
        let summary = summarize_stderr(&stderr);
        let msg = if summary.is_empty() {
            format!("`{agy} {cmd_str}` exited with code {code}")
        } else {
            format!("`{agy} {cmd_str}` exited with code {code}: {summary}")
        };
        return Err(AgentRunError::internal("remote_control_failed", msg).into());
    }

    Ok((stdout, stderr))
}

/// Renders a short human-readable status text.
fn render_status_text(status: &RemoteControlStatus) -> String {
    if status.active {
        match (&status.pid, &status.instance_name) {
            (Some(pid), Some(name)) => {
                format!("Remote control daemon is active (PID {pid}, instance: {name})")
            }
            (Some(pid), None) => format!("Remote control daemon is active (PID {pid})"),
            (None, Some(name)) => format!("Remote control daemon is active (instance: {name})"),
            (None, None) => "Remote control daemon is active".to_string(),
        }
    } else {
        match &status.instance_name {
            Some(name) => format!("Remote control daemon is inactive (instance: {name})"),
            None => "Remote control daemon is inactive".to_string(),
        }
    }
}

/// Query status of the remote-control daemon.
pub fn status(config: &Config) -> Result<CommandOutput> {
    let args = build_status_args();
    let (stdout, _) = execute_agy(config, &args, REMOTE_CONTROL_TIMEOUT)?;
    let parsed = parse_status(&stdout);
    let text = render_status_text(&parsed);
    let data = serde_json::to_value(&parsed).map_err(|e| {
        AgentRunError::internal(
            "remote_control_failed",
            format!("failed to serialize status: {e}"),
        )
    })?;
    Ok(CommandOutput {
        exit_code: 0,
        stdout_lines: vec![text],
        stderr_lines: Vec::new(),
        data,
    })
}

/// Enable the remote-control daemon on the server.
pub fn enable(config: &Config) -> Result<CommandOutput> {
    if !config.remote_control {
        return Err(AgentRunError::forbidden(
            "remote_control_disabled",
            "remote control is disabled on the server (set AGENT_RUN_REMOTE_CONTROL=1 or true to enable)",
        )
        .into());
    }

    let hostname = get_hostname()?;
    eprintln!("agent-run: remote-control enable on {hostname}");

    // If daemon is already active, do not start it again and return the current status
    let current = status(config)?;
    let is_active = current
        .data
        .get("active")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if is_active {
        return Ok(current);
    }

    let args = build_enable_args(&hostname);
    execute_agy(config, &args, REMOTE_CONTROL_TIMEOUT)?;
    status(config)
}

/// Disable the remote-control daemon on the server.
pub fn disable(config: &Config) -> Result<CommandOutput> {
    let hostname = get_hostname().unwrap_or_else(|_| "unknown".to_string());
    eprintln!("agent-run: remote-control disable on {hostname}");

    // If daemon is already inactive, succeed and return the status
    let current = status(config)?;
    let is_active = current
        .data
        .get("active")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !is_active {
        return Ok(current);
    }

    let args = build_disable_args();
    execute_agy(config, &args, REMOTE_CONTROL_TIMEOUT)?;
    status(config)
}

/// Dispatches execution based on `RemoteControlArgs`.
pub fn run(config: &Config, args: &RemoteControlArgs) -> Result<CommandOutput> {
    match args.action {
        RemoteControlAction::Enable => enable(config),
        RemoteControlAction::Disable => disable(config),
        RemoteControlAction::Status => status(config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::ErrorKind;

    #[test]
    fn test_parse_status_active_with_pid_and_instance() {
        let stdout = r#"Daemon status: active (background process, PID 12345)
Instance name: test-server (find it at https://antigravity.google.com)
Check logs in: /home/agent/.gemini/antigravity-cli/remote-control.log
"#;
        let parsed = parse_status(stdout);
        assert!(parsed.active);
        assert_eq!(parsed.pid, Some(12345));
        assert_eq!(parsed.instance_name, Some("test-server".to_string()));
        assert_eq!(
            parsed.message,
            "Daemon status: active (background process, PID 12345)"
        );

        let text = render_status_text(&parsed);
        assert_eq!(
            text,
            "Remote control daemon is active (PID 12345, instance: test-server)"
        );
    }

    #[test]
    fn test_parse_status_active_without_instance_name() {
        let stdout = r#"Daemon status: active (background process, PID 54321)
Instance name: not assigned yet; the daemon generates one on its first start.
Check logs in: /home/agent/.gemini/antigravity-cli/remote-control.log
"#;
        let parsed = parse_status(stdout);
        assert!(parsed.active);
        assert_eq!(parsed.pid, Some(54321));
        assert_eq!(parsed.instance_name, None);
        assert_eq!(
            parsed.message,
            "Daemon status: active (background process, PID 54321)"
        );

        let text = render_status_text(&parsed);
        assert_eq!(text, "Remote control daemon is active (PID 54321)");
    }

    #[test]
    fn test_parse_status_inactive_without_instance_name() {
        let stdout = r#"Daemon status: inactive
Instance name: not assigned yet; the daemon generates one on its first start.
"#;
        let parsed = parse_status(stdout);
        assert!(!parsed.active);
        assert_eq!(parsed.pid, None);
        assert_eq!(parsed.instance_name, None);
        assert_eq!(parsed.message, "Daemon status: inactive");

        let text = render_status_text(&parsed);
        assert_eq!(text, "Remote control daemon is inactive");
    }

    #[test]
    fn test_parse_status_inactive_with_instance_name() {
        let stdout = r#"Daemon status: inactive
Instance name: test-server (find it at https://antigravity.google.com)
"#;
        let parsed = parse_status(stdout);
        assert!(!parsed.active);
        assert_eq!(parsed.pid, None);
        assert_eq!(parsed.instance_name, Some("test-server".to_string()));
        assert_eq!(parsed.message, "Daemon status: inactive");

        let text = render_status_text(&parsed);
        assert_eq!(
            text,
            "Remote control daemon is inactive (instance: test-server)"
        );
    }

    #[test]
    fn test_parse_status_unexpected_output() {
        let stdout = "some unexpected daemon warning\nsecond line";
        let parsed = parse_status(stdout);
        assert!(!parsed.active);
        assert_eq!(parsed.pid, None);
        assert_eq!(parsed.instance_name, None);
        assert_eq!(parsed.message, "some unexpected daemon warning");

        let text = render_status_text(&parsed);
        assert_eq!(text, "Remote control daemon is inactive");
    }

    #[test]
    fn test_parse_status_empty() {
        let parsed = parse_status("");
        assert!(!parsed.active);
        assert_eq!(parsed.pid, None);
        assert_eq!(parsed.instance_name, None);
        assert_eq!(parsed.message, "");
    }

    #[test]
    fn test_argument_vectors() {
        let enable_args = build_enable_args("worker-node-1");
        assert_eq!(
            enable_args,
            vec![
                "remote-control",
                "start",
                "--session",
                "--name",
                "worker-node-1"
            ]
        );

        let disable_args = build_disable_args();
        assert_eq!(disable_args, vec!["remote-control", "stop"]);

        let status_args = build_status_args();
        assert_eq!(status_args, vec!["remote-control", "status"]);
    }

    #[test]
    fn test_safety_gate_refusal_when_disabled() {
        let config = Config {
            remote_control: false,
            ..Default::default()
        };

        let err = enable(&config).expect_err("enable must fail when remote_control is false");
        let typed = err
            .downcast_ref::<AgentRunError>()
            .expect("must downcast to AgentRunError");
        assert_eq!(typed.code, "remote_control_disabled");
        assert_eq!(typed.kind, ErrorKind::Forbidden);
        assert!(typed.message.contains("AGENT_RUN_REMOTE_CONTROL"));
    }

    #[test]
    fn test_get_hostname_returns_non_empty() {
        let hostname = get_hostname().expect("get_hostname should succeed");
        assert!(!hostname.is_empty());
    }
}

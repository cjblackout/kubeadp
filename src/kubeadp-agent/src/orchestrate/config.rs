use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn default_state_file() -> PathBuf {
    PathBuf::from("/tmp/claude-supervisor-state.json")
}

fn default_heartbeat_interval() -> f64 {
    2.0
}

fn default_stale_heartbeat() -> f64 {
    15.0
}

fn default_shutdown_grace() -> f64 {
    5.0
}

fn default_sandbox() -> bool {
    true
}

fn default_claude_binary() -> PathBuf {
    PathBuf::from("claude")
}

fn default_claude_args() -> Vec<String> {
    vec![
        "--dangerously-skip-permissions".to_string(),
        "--remote-control".to_string(),
        "ai-server".to_string(),
    ]
}

fn default_readiness_poll() -> f64 {
    5.0
}

fn default_hang_timeout() -> f64 {
    0.0
}

fn default_kill_grace() -> f64 {
    5.0
}

fn default_base_backoff() -> f64 {
    2.0
}

fn default_max_backoff() -> f64 {
    60.0
}

fn default_healthy_run() -> f64 {
    30.0
}

fn default_max_crashes() -> usize {
    5
}

fn default_crash_window() -> f64 {
    60.0
}

fn default_pty_eof_grace() -> f64 {
    10.0
}

fn default_prefix_output() -> bool {
    false
}

fn default_failed_cooldown() -> f64 {
    300.0
}

fn default_backoff_window() -> f64 {
    600.0
}

fn default_kill_wait() -> f64 {
    15.0
}

fn default_resume_fail_quarantine_after() -> usize {
    5
}

fn default_resume_fail_uptime() -> f64 {
    5.0
}

/// Maximum allowed value for any duration field (30 days in seconds).
const MAX_DURATION_SECS: f64 = 30.0 * 24.0 * 3600.0;

/// Validates a single f64 duration field.
/// Returns `Err(message)` if the value is negative, non-finite, or > 30 days.
pub fn validate_duration_field(
    field_name: &str,
    session_name: &str,
    val: f64,
) -> Result<(), String> {
    if !val.is_finite() {
        return Err(format!(
            "Configuration error: field '{}' in session '{}' is not finite (got {})",
            field_name, session_name, val
        ));
    }
    if val < 0.0 {
        return Err(format!(
            "Configuration error: field '{}' in session '{}' must be >= 0 (got {})",
            field_name, session_name, val
        ));
    }
    if val > MAX_DURATION_SECS {
        return Err(format!(
            "Configuration error: field '{}' in session '{}' exceeds 30 days (got {})",
            field_name, session_name, val
        ));
    }
    Ok(())
}

/// Validates a duration field on the supervisor config (no session name context).
pub fn validate_supervisor_duration(field_name: &str, val: f64) -> Result<(), String> {
    validate_duration_field(field_name, "supervisor", val)
}

/// Per-session sandbox resource and environment grants (HL-151).
///
/// Configured explicitly per session in supervisor configuration files.
/// Strict validation: unknown fields are rejected, paths must be absolute,
/// and environment variable names must be non-empty valid identifiers with
/// an optional trailing wildcard `*` only.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionGrants {
    /// Grants read-only access to the pod's Kubernetes ServiceAccount token directory.
    #[serde(default)]
    pub serviceaccount: bool,

    /// Absolute paths granted read-only.
    #[serde(default)]
    pub ro_paths: Vec<PathBuf>,

    /// Absolute paths granted read-write.
    #[serde(default)]
    pub rw_paths: Vec<PathBuf>,

    /// Environment variable names passed through from supervisor process environment.
    /// Trailing wildcard `*` allowed (e.g. `VIKUNJA_*`). Names only, never values.
    #[serde(default)]
    pub env: Vec<String>,
}

impl SessionGrants {
    /// Validates the grants configuration according to sandbox security constraints.
    pub fn validate(&self, session_name: &str) -> Result<(), String> {
        for p in &self.ro_paths {
            validate_grant_path(p, "ro_paths", session_name)?;
        }
        for p in &self.rw_paths {
            validate_grant_path(p, "rw_paths", session_name)?;
        }
        for var in &self.env {
            validate_grant_env_name(var, session_name)?;
        }
        Ok(())
    }
}

/// Validates that a granted path is absolute, not root or forbidden system dirs,
/// and has no path traversal components.
fn validate_grant_path(path: &Path, field: &str, session_name: &str) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!(
            "Configuration error: field 'grants.{}' in session '{}' contains non-absolute path '{}'",
            field,
            session_name,
            path.display()
        ));
    }
    let s = path.to_string_lossy();
    if s == "/" || s == "/proc" || s.starts_with("/proc/") || s == "/dev" || s.starts_with("/dev/")
    {
        return Err(format!(
            "Configuration error: field 'grants.{}' in session '{}' contains forbidden path '{}'",
            field,
            session_name,
            path.display()
        ));
    }
    // Disallow traversal components (".." or ".")
    for comp in path.components() {
        if let std::path::Component::ParentDir = comp {
            return Err(format!(
                "Configuration error: field 'grants.{}' in session '{}' contains parent traversal '..' in '{}'",
                field,
                session_name,
                path.display()
            ));
        }
    }
    Ok(())
}

/// Validates an environment variable name: valid identifier, trailing '*' allowed only.
fn validate_grant_env_name(name: &str, session_name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!(
            "Configuration error: field 'grants.env' in session '{}' contains empty variable name",
            session_name
        ));
    }
    let (ident, has_wildcard) = if let Some(stripped) = name.strip_suffix('*') {
        (stripped, true)
    } else {
        (name, false)
    };
    if has_wildcard && ident.contains('*') {
        return Err(format!(
            "Configuration error: field 'grants.env' in session '{}' has '*' not at the trailing position in '{}'",
            session_name, name
        ));
    }
    // Identifier must be ASCII alphanumeric or '_'
    if ident.is_empty() && has_wildcard {
        return Err(format!(
            "Configuration error: field 'grants.env' in session '{}' wildcard '*' requires prefix in '{}'",
            session_name, name
        ));
    }
    if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "Configuration error: field 'grants.env' in session '{}' contains invalid characters in '{}'",
            session_name, name
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionConfig {
    pub name: String,
    pub workdir: PathBuf,
    pub session_id: String,

    #[serde(default = "default_claude_binary")]
    pub claude_binary: PathBuf,

    #[serde(default = "default_claude_args")]
    pub claude_args: Vec<String>,

    #[serde(default)]
    pub env: HashMap<String, String>,

    #[serde(default)]
    pub session_file: Option<PathBuf>,

    /// Absolute path of a per-project CLAUDE.md source file to inject into the
    /// session's workdir (as `CLAUDE.md`) before every spawn. Managed outside
    /// the repository, e.g. a Kubernetes ConfigMap-mounted file.
    #[serde(default)]
    pub memory_file: Option<PathBuf>,

    #[serde(default = "default_readiness_poll")]
    pub readiness_poll_secs: f64,

    #[serde(default = "default_hang_timeout")]
    pub hang_timeout_secs: f64,

    #[serde(default = "default_kill_grace")]
    pub kill_grace_secs: f64,

    #[serde(default = "default_pty_eof_grace")]
    pub pty_eof_grace_secs: f64,

    #[serde(default = "default_kill_wait")]
    pub kill_wait_secs: f64,

    /// Run the session under `sandbox-exec` (default). `false` marks a TRUSTED
    /// session that runs directly with the supervisor's full environment,
    /// filesystem and ServiceAccount token (the pre-sandbox behaviour); set
    /// it explicitly per session in the supervisor config, never globally.
    #[serde(default = "default_sandbox")]
    pub sandbox: bool,

    /// Explicit sandbox grants for sandboxed sessions (HL-151).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grants: Option<SessionGrants>,

    #[serde(default = "default_base_backoff")]
    pub base_backoff_secs: f64,

    #[serde(default = "default_max_backoff")]
    pub max_backoff_secs: f64,

    #[serde(default = "default_healthy_run")]
    pub healthy_run_secs: f64,

    #[serde(default = "default_max_crashes")]
    pub max_crashes: usize,

    #[serde(default = "default_crash_window")]
    pub crash_window_secs: f64,

    #[serde(default = "default_prefix_output")]
    pub prefix_output: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_path: Option<PathBuf>,

    /// Cooldown in seconds after entering `failed` state before the supervisor
    /// clears the crash tracker and resumes supervising (fix #3).
    #[serde(default = "default_failed_cooldown")]
    pub failed_cooldown_secs: f64,

    /// Window in seconds for the sliding-window backoff (fix #4).
    #[serde(default = "default_backoff_window")]
    pub backoff_window_secs: f64,

    #[serde(default = "default_resume_fail_quarantine_after")]
    pub resume_fail_quarantine_after: usize,

    #[serde(default = "default_resume_fail_uptime")]
    pub resume_fail_uptime_secs: f64,
}

impl SessionConfig {
    /// Validates all f64 duration fields.  Returns `Err` with a human-readable
    /// message on the first violation.  Must be called at load time so that
    /// no `Duration::from_secs_f64` can panic later.
    pub fn validate_durations(&self) -> Result<(), String> {
        let n = &self.name;
        validate_duration_field("readiness_poll_secs", n, self.readiness_poll_secs)?;
        validate_duration_field("hang_timeout_secs", n, self.hang_timeout_secs)?;
        validate_duration_field("kill_grace_secs", n, self.kill_grace_secs)?;
        validate_duration_field("pty_eof_grace_secs", n, self.pty_eof_grace_secs)?;
        validate_duration_field("kill_wait_secs", n, self.kill_wait_secs)?;
        validate_duration_field("base_backoff_secs", n, self.base_backoff_secs)?;
        validate_duration_field("max_backoff_secs", n, self.max_backoff_secs)?;
        validate_duration_field("healthy_run_secs", n, self.healthy_run_secs)?;
        validate_duration_field("crash_window_secs", n, self.crash_window_secs)?;
        validate_duration_field("failed_cooldown_secs", n, self.failed_cooldown_secs)?;
        validate_duration_field("backoff_window_secs", n, self.backoff_window_secs)?;
        validate_duration_field("resume_fail_uptime_secs", n, self.resume_fail_uptime_secs)?;
        if let Some(ref mf) = self.memory_file {
            if !mf.is_absolute() {
                return Err(format!(
                    "Configuration error: field 'memory_file' in session '{}' must be an absolute path (got {})",
                    n,
                    mf.display()
                ));
            }
        }
        if let Some(ref g) = self.grants {
            g.validate(n)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SupervisorConfig {
    #[serde(default = "default_state_file")]
    pub state_file: PathBuf,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: f64,

    #[serde(default = "default_stale_heartbeat")]
    pub stale_heartbeat_secs: f64,

    #[serde(default = "default_shutdown_grace")]
    pub shutdown_grace_secs: f64,

    #[serde(default)]
    pub claude_config_dir: Option<PathBuf>,

    #[serde(default)]
    pub sessions: Vec<SessionConfig>,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            state_file: default_state_file(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            stale_heartbeat_secs: default_stale_heartbeat(),
            shutdown_grace_secs: default_shutdown_grace(),
            claude_config_dir: None,
            sessions: Vec::new(),
        }
    }
}

impl SupervisorConfig {
    /// Validates all supervisor-level f64 duration fields.
    pub fn validate_durations(&self) -> Result<(), String> {
        validate_supervisor_duration("heartbeat_interval_secs", self.heartbeat_interval_secs)?;
        validate_supervisor_duration("shutdown_grace_secs", self.shutdown_grace_secs)?;
        validate_supervisor_duration("stale_heartbeat_secs", self.stale_heartbeat_secs)?;
        Ok(())
    }
}

/// Derives the project slug for ~/.claude/projects/<slug> from a workdir path.
/// Matches Claude CLI 2.1.286 behavior: canonicalizes symlinks (falling back to workdir),
/// trims trailing slashes, and replaces every non-alphanumeric ASCII character with '-'.
pub fn derive_project_slug(workdir: &Path) -> String {
    let canonical = std::fs::canonicalize(workdir).unwrap_or_else(|_| workdir.to_path_buf());
    let path_str = canonical.to_string_lossy();
    let trimmed = if path_str.len() > 1 {
        path_str.trim_end_matches('/')
    } else {
        &path_str
    };
    trimmed
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Warns on startup if a session workdir is a symlink.
pub fn warn_if_symlink_workdir(session_name: &str, workdir: &Path) {
    if let Ok(meta) = std::fs::symlink_metadata(workdir) {
        if meta.file_type().is_symlink() {
            let target = std::fs::read_link(workdir)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            crate::orchestrate::logging::log_event(
                session_name,
                &format!(
                    "WARNING: workdir '{}' is a symlink (target: '{}'). Claude Code keys transcripts by canonical path; if the symlink target changes (e.g. git-sync checkouts), conversation history will be lost on retarget.",
                    workdir.display(),
                    target
                ),
            );
        }
    }
}

impl SupervisorConfig {
    /// Loads a session configuration file. The format is chosen by file
    /// extension: `.toml` files are deserialized as TOML through the same
    /// [`SessionConfig`]/[`SupervisorConfig`] structs (an array of tables for
    /// the `sessions` array, field names identical to the JSON form), every
    /// other extension is parsed as JSON.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file {}: {}", path.display(), e))?;
        match path.extension().and_then(|e| e.to_str()) {
            Some("toml") => Self::from_toml_str(&content),
            _ => Self::from_json_str(&content),
        }
    }

    pub fn from_json_str(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| format!("Failed to parse config JSON: {}", e))
    }

    /// Parses a TOML session configuration file. Field names match the JSON
    /// form exactly; `sessions` is an array of tables. Error messages name
    /// the TOML field, never the raw document content.
    pub fn from_toml_str(s: &str) -> Result<Self, String> {
        toml::from_str(s).map_err(|e| {
            format!(
                "Failed to parse config TOML at {}:{}: {}",
                e.span().map(|sp| sp.start).unwrap_or(0),
                e.span().map(|sp| sp.end).unwrap_or(0),
                e.message(),
            )
        })
    }

    /// Resolves the session transcript file path for a session.
    /// Honors explicit `session.transcript_path`, then `session.session_file`.
    /// Otherwise, derives `~/.claude/projects/<slug>/<session_id>.jsonl` using `derive_project_slug`.
    pub fn resolve_session_file(&self, session: &SessionConfig) -> PathBuf {
        if let Some(ref path) = session.transcript_path {
            return path.clone();
        }
        if let Some(ref path) = session.session_file {
            return path.clone();
        }
        let home = match std::env::var("HOME") {
            Ok(h) if !h.trim().is_empty() => PathBuf::from(h.trim()),
            _ => PathBuf::from("/home/coder"),
        };
        let claude_dir = self
            .claude_config_dir
            .clone()
            .unwrap_or_else(|| home.join(".claude"));
        let slug = derive_project_slug(&session.workdir);
        claude_dir
            .join("projects")
            .join(slug)
            .join(format!("{}.jsonl", session.session_id))
    }
}

/// Resolves base configuration from ordered sources:
/// 1. Explicit CLI argument (`--config <path>`)
/// 2. Environment variable `AGENT_ORCHESTRATE_CONFIG` (legacy `CLAUDE_SUPERVISOR_CONFIG` still accepted)
/// 3. Standard location `/etc/agent-orchestrate/config.json` (only if it exists)
/// 4. Built-in defaults
///
/// If an explicit CLI path or environment variable path fails to load, returns Err.
pub fn resolve_base_config_from_sources(
    cli_path: Option<&Path>,
    env_config_path: Option<&str>,
    etc_path: &Path,
) -> Result<SupervisorConfig, String> {
    if let Some(path) = cli_path {
        SupervisorConfig::from_file(path)
    } else if let Some(env_path) = env_config_path.filter(|s| !s.trim().is_empty()) {
        SupervisorConfig::from_file(env_path.trim())
    } else if etc_path.exists() {
        SupervisorConfig::from_file(etc_path)
    } else {
        Ok(SupervisorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let json_data = r#"{
            "sessions": [
                {
                    "name": "sess1",
                    "workdir": "/tmp/work",
                    "session_id": "1111-2222"
                }
            ]
        }"#;
        let cfg = SupervisorConfig::from_json_str(json_data).expect("Must parse");
        assert_eq!(cfg.sessions.len(), 1);
        let s = &cfg.sessions[0];
        assert_eq!(s.name, "sess1");
        assert_eq!(s.workdir, PathBuf::from("/tmp/work"));
        assert_eq!(s.session_id, "1111-2222");
        assert_eq!(s.claude_binary, PathBuf::from("claude"));
        assert_eq!(
            s.claude_args,
            vec![
                "--dangerously-skip-permissions",
                "--remote-control",
                "ai-server"
            ]
        );
        assert_eq!(s.hang_timeout_secs, 0.0);
        assert_eq!(s.base_backoff_secs, 2.0);
        assert_eq!(s.max_crashes, 5);
        assert!(!s.prefix_output);
        assert_eq!(cfg.heartbeat_interval_secs, 2.0);
        // New fields have expected defaults.
        assert_eq!(s.failed_cooldown_secs, 300.0);
        assert_eq!(s.backoff_window_secs, 600.0);
    }

    #[test]
    fn test_parse_full_config() {
        let json_data = r#"{
            "state_file": "/custom/state.json",
            "heartbeat_interval_secs": 1.0,
            "stale_heartbeat_secs": 10.0,
            "shutdown_grace_secs": 2.5,
            "sessions": [
                {
                    "name": "custom",
                    "workdir": "/workspace",
                    "session_id": "custom-id",
                    "claude_binary": "/usr/local/bin/my-claude",
                    "claude_args": ["--foo", "bar"],
                    "env": {"KEY": "VAL"},
                    "session_file": "/custom/session.jsonl",
                    "readiness_poll_secs": 1.0,
                    "hang_timeout_secs": 5.0,
                    "kill_grace_secs": 1.0,
                    "base_backoff_secs": 0.5,
                    "max_backoff_secs": 10.0,
                    "healthy_run_secs": 15.0,
                    "max_crashes": 3,
                    "crash_window_secs": 30.0,
                    "prefix_output": true,
                    "failed_cooldown_secs": 60.0,
                    "backoff_window_secs": 120.0
                }
            ]
        }"#;
        let cfg = SupervisorConfig::from_json_str(json_data).expect("Must parse");
        assert_eq!(cfg.state_file, PathBuf::from("/custom/state.json"));
        assert_eq!(cfg.shutdown_grace_secs, 2.5);
        let s = &cfg.sessions[0];
        assert_eq!(s.name, "custom");
        assert_eq!(s.claude_binary, PathBuf::from("/usr/local/bin/my-claude"));
        assert_eq!(s.env.get("KEY").unwrap(), "VAL");
        assert_eq!(s.session_file, Some(PathBuf::from("/custom/session.jsonl")));
        assert_eq!(s.hang_timeout_secs, 5.0);
        assert_eq!(s.max_crashes, 3);
        assert!(s.prefix_output);
        assert_eq!(s.failed_cooldown_secs, 60.0);
        assert_eq!(s.backoff_window_secs, 120.0);
    }

    #[test]
    fn test_resolve_session_file() {
        let json_data = r#"{
            "sessions": [
                {
                    "name": "sess1",
                    "workdir": "/home/coder/project",
                    "session_id": "test-session"
                }
            ]
        }"#;
        let cfg = SupervisorConfig::from_json_str(json_data).unwrap();
        let s = &cfg.sessions[0];
        let path = cfg.resolve_session_file(s);
        let path_str = path.to_string_lossy();
        assert!(path_str.ends_with(".claude/projects/-home-coder-project/test-session.jsonl"));
    }

    #[test]
    fn test_invalid_json() {
        let res = SupervisorConfig::from_json_str("{ invalid json }");
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_toml_config() {
        let toml_data = r#"
state_file = "/tmp/state-toml.json"
heartbeat_interval_secs = 1.0
stale_heartbeat_secs = 10.0
shutdown_grace_secs = 2.5

[[sessions]]
name = "sess1"
workdir = "/tmp/work"
session_id = "1111-2222"
claude_binary = "/usr/local/bin/my-claude"
claude_args = ["--foo", "bar"]
env = { KEY = "VAL" }
session_file = "/custom/session.jsonl"
readiness_poll_secs = 1.0
hang_timeout_secs = 5.0
kill_grace_secs = 1.0
base_backoff_secs = 0.5
max_backoff_secs = 10.0
healthy_run_secs = 15.0
max_crashes = 3
crash_window_secs = 30.0
prefix_output = true
failed_cooldown_secs = 60.0
backoff_window_secs = 120.0
"#;
        let cfg = SupervisorConfig::from_toml_str(toml_data).expect("Must parse");
        assert_eq!(cfg.state_file, PathBuf::from("/tmp/state-toml.json"));
        assert_eq!(cfg.shutdown_grace_secs, 2.5);
        let s = &cfg.sessions[0];
        assert_eq!(s.name, "sess1");
        assert_eq!(s.claude_binary, PathBuf::from("/usr/local/bin/my-claude"));
        assert_eq!(s.claude_args, vec!["--foo".to_string(), "bar".to_string()]);
        assert_eq!(s.env.get("KEY").unwrap(), "VAL");
        assert_eq!(s.session_file, Some(PathBuf::from("/custom/session.jsonl")));
        assert_eq!(s.hang_timeout_secs, 5.0);
        assert_eq!(s.max_crashes, 3);
        assert!(s.prefix_output);
        assert_eq!(s.failed_cooldown_secs, 60.0);
        assert_eq!(s.backoff_window_secs, 120.0);
        // Unknown/defaulted fields get the same defaults as JSON.
        assert_eq!(s.pty_eof_grace_secs, 10.0);
        assert_eq!(s.kill_wait_secs, 15.0);
        assert_eq!(cfg.heartbeat_interval_secs, 1.0);
    }

    #[test]
    fn test_parse_toml_multiple_sessions_with_defaults() {
        let toml_data = r#"
[[sessions]]
name = "a"
workdir = "/tmp/a"
session_id = "id-a"

[[sessions]]
name = "b"
workdir = "/tmp/b"
session_id = "id-b"
"#;
        let cfg = SupervisorConfig::from_toml_str(toml_data).expect("Must parse");
        assert_eq!(cfg.sessions.len(), 2);
        for s in &cfg.sessions {
            // Defaults applied identically to the JSON path.
            assert_eq!(s.claude_binary, PathBuf::from("claude"));
            assert_eq!(s.max_crashes, 5);
            assert_eq!(s.hang_timeout_secs, 0.0);
            assert_eq!(s.readiness_poll_secs, 5.0);
            assert_eq!(s.base_backoff_secs, 2.0);
        }
        assert_eq!(
            cfg.state_file,
            PathBuf::from("/tmp/claude-supervisor-state.json")
        );
        assert_eq!(cfg.heartbeat_interval_secs, 2.0);
    }

    #[test]
    fn test_parse_toml_invalid() {
        // Not an array-of-tables for sessions.
        let bad = "sessions = 5\n";
        let err = SupervisorConfig::from_toml_str(bad).unwrap_err();
        assert!(err.contains("Failed to parse config TOML"), "{err}");
        // Malformed TOML.
        let bad2 = "heartbeat_interval_secs = \"\n";
        let err2 = SupervisorConfig::from_toml_str(bad2).unwrap_err();
        assert!(err2.contains("Failed to parse config TOML"), "{err2}");
    }

    #[test]
    fn test_from_file_extension_selects_format() {
        let temp_dir = std::env::temp_dir().join(format!("test_cfg_toml_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);

        let toml_file = temp_dir.join("config.toml");
        fs::write(
            &toml_file,
            "heartbeat_interval_secs = 1.1\n[[sessions]]\nname=\"s\"\nworkdir=\"/tmp\"\nsession_id=\"i\"\n",
        )
        .unwrap();
        let cfg = SupervisorConfig::from_file(&toml_file).expect("TOML parses");
        assert_eq!(cfg.heartbeat_interval_secs, 1.1);
        assert_eq!(cfg.sessions.len(), 1);

        let json_file = temp_dir.join("config.json");
        fs::write(&json_file, "{\"heartbeat_interval_secs\": 2.2}").unwrap();
        let cfg2 = SupervisorConfig::from_file(&json_file).expect("JSON parses");
        assert_eq!(cfg2.heartbeat_interval_secs, 2.2);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_resolve_config_sources_cli_and_env() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_cfg_sources_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);

        let cli_file = temp_dir.join("cli_config.json");
        fs::write(&cli_file, r#"{"heartbeat_interval_secs": 1.1}"#).unwrap();

        let env_file = temp_dir.join("env_config.json");
        fs::write(&env_file, r#"{"heartbeat_interval_secs": 2.2}"#).unwrap();

        let etc_file = temp_dir.join("etc_config.json");
        fs::write(&etc_file, r#"{"heartbeat_interval_secs": 3.3}"#).unwrap();

        let nonexistent = temp_dir.join("nonexistent.json");

        // 1. Explicit CLI wins over env and etc
        let cfg1 = resolve_base_config_from_sources(
            Some(&cli_file),
            Some(&env_file.to_string_lossy()),
            &etc_file,
        )
        .unwrap();
        assert_eq!(cfg1.heartbeat_interval_secs, 1.1);

        // 2. Env wins when no CLI
        let cfg2 =
            resolve_base_config_from_sources(None, Some(&env_file.to_string_lossy()), &etc_file)
                .unwrap();
        assert_eq!(cfg2.heartbeat_interval_secs, 2.2);

        // 3. Etc used when no CLI and no Env
        let cfg3 = resolve_base_config_from_sources(None, None, &etc_file).unwrap();
        assert_eq!(cfg3.heartbeat_interval_secs, 3.3);

        // 4. Fallback to default when etc does not exist
        let cfg4 = resolve_base_config_from_sources(None, None, &nonexistent).unwrap();
        assert_eq!(cfg4.heartbeat_interval_secs, 2.0); // default

        // 5. Explicit CLI failure returns Err (never ignored)
        let err_cli = resolve_base_config_from_sources(Some(&nonexistent), None, &etc_file);
        assert!(err_cli.is_err());

        // 6. Explicit Env failure returns Err (never ignored)
        let err_env =
            resolve_base_config_from_sources(None, Some(&nonexistent.to_string_lossy()), &etc_file);
        assert!(err_env.is_err());

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_derive_project_slug() {
        assert_eq!(
            derive_project_slug(Path::new("/tmp/slug_t.est/a b")),
            "-tmp-slug-t-est-a-b"
        );
        assert_eq!(derive_project_slug(Path::new("/home/coder")), "-home-coder");
        assert_eq!(
            derive_project_slug(Path::new("/workspace/project_1+2@3/")),
            "-workspace-project-1-2-3"
        );

        // Test symlink canonicalization
        let temp_dir =
            std::env::temp_dir().join(format!("test_slug_symlink_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let real_target = temp_dir.join("real_target_dir");
        let _ = fs::create_dir_all(&real_target);
        let symlink_path = temp_dir.join("link_to_target");
        let _ = std::os::unix::fs::symlink(&real_target, &symlink_path);

        let slug = derive_project_slug(&symlink_path);
        let expected_real = std::fs::canonicalize(&real_target).unwrap();
        let expected_slug: String = expected_real
            .to_string_lossy()
            .trim_end_matches('/')
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        assert_eq!(slug, expected_slug);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_explicit_transcript_path_honored() {
        let cfg = SupervisorConfig::default();
        let mut session = SessionConfig {
            name: "test".to_string(),
            workdir: PathBuf::from("/workspace/foo"),
            session_id: "id-123".to_string(),
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            env: HashMap::new(),
            session_file: Some(PathBuf::from("/old/session.jsonl")),
            transcript_path: Some(PathBuf::from("/custom/explicit/transcript.jsonl")),
            memory_file: None,
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

        // Explicit transcript_path wins over session_file and derived path
        let resolved = cfg.resolve_session_file(&session);
        assert_eq!(resolved, PathBuf::from("/custom/explicit/transcript.jsonl"));

        // If transcript_path is None, session_file wins
        session.transcript_path = None;
        let resolved2 = cfg.resolve_session_file(&session);
        assert_eq!(resolved2, PathBuf::from("/old/session.jsonl"));
    }

    // -----------------------------------------------------------------------
    // Fix #5: Duration validation tests
    // -----------------------------------------------------------------------

    fn make_session(name: &str) -> SessionConfig {
        SessionConfig {
            name: name.to_string(),
            workdir: PathBuf::from("/tmp"),
            session_id: "id-test".to_string(),
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            env: HashMap::new(),
            session_file: None,
            transcript_path: None,
            memory_file: None,
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
        }
    }

    #[test]
    fn test_validation_valid_session() {
        let s = make_session("valid");
        assert!(s.validate_durations().is_ok());
    }

    #[test]
    fn test_validation_negative_field_rejected() {
        let mut s = make_session("neg");
        s.base_backoff_secs = -1.0;
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("base_backoff_secs"), "err: {}", err);
        assert!(err.contains("neg"), "err: {}", err);
    }

    #[test]
    fn test_validation_nan_field_rejected() {
        let mut s = make_session("nantest");
        s.kill_grace_secs = f64::NAN;
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("kill_grace_secs"), "err: {}", err);
    }

    #[test]
    fn test_validation_infinite_field_rejected() {
        let mut s = make_session("inftest");
        s.hang_timeout_secs = f64::INFINITY;
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("hang_timeout_secs"), "err: {}", err);
    }

    #[test]
    fn test_validation_too_large_field_rejected() {
        let mut s = make_session("huge");
        s.readiness_poll_secs = MAX_DURATION_SECS + 1.0;
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("readiness_poll_secs"), "err: {}", err);
    }

    #[test]
    fn test_validation_kill_wait_secs_rejected() {
        let mut s = make_session("bad_kill_wait");
        s.kill_wait_secs = -1.0;
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("kill_wait_secs"), "err: {}", err);

        s.kill_wait_secs = f64::NAN;
        let err2 = s.validate_durations().unwrap_err();
        assert!(err2.contains("kill_wait_secs"), "err: {}", err2);
    }

    #[test]
    fn test_validation_resume_fail_uptime_secs_rejected() {
        let mut s = make_session("bad_resume_uptime");
        s.resume_fail_uptime_secs = -0.5;
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("resume_fail_uptime_secs"), "err: {}", err);

        s.resume_fail_uptime_secs = MAX_DURATION_SECS + 10.0;
        let err2 = s.validate_durations().unwrap_err();
        assert!(err2.contains("resume_fail_uptime_secs"), "err: {}", err2);
    }

    #[test]
    fn test_validation_negative_from_config_file_exits_error() {
        // Parsing negative values from JSON should succeed; validation catches them.
        let json = r#"{
            "sessions": [{
                "name": "bad",
                "workdir": "/tmp",
                "session_id": "id",
                "base_backoff_secs": -5.0
            }]
        }"#;
        let cfg = SupervisorConfig::from_json_str(json).expect("parses");
        let s = &cfg.sessions[0];
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("base_backoff_secs"), "err: {}", err);
        assert!(err.contains("bad"), "err: {}", err);
    }

    #[test]
    fn test_validation_huge_value_from_config_file() {
        let json = r#"{
            "sessions": [{
                "name": "huge",
                "workdir": "/tmp",
                "session_id": "id",
                "heartbeat_interval_secs": 999999999.0,
                "hang_timeout_secs": 99999999999.0
            }]
        }"#;
        // The session field "hang_timeout_secs" is 9e10 > 30 days.
        let cfg = SupervisorConfig::from_json_str(json).expect("parses");
        let s = &cfg.sessions[0];
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("hang_timeout_secs"), "err: {}", err);
    }

    #[test]
    fn test_failed_cooldown_default_and_min() {
        let s = make_session("cooldown");
        assert_eq!(s.failed_cooldown_secs, 300.0);
        // Zero is allowed (validates to >= 0)
        let mut s2 = s.clone();
        s2.failed_cooldown_secs = 0.0;
        assert!(s2.validate_durations().is_ok());
        // Negative is rejected
        let mut s3 = s.clone();
        s3.failed_cooldown_secs = -1.0;
        assert!(s3.validate_durations().is_err());
    }

    #[test]
    fn test_memory_file_parses_from_json_absolute() {
        let json = r#"{
            "sessions": [{
                "name": "mem",
                "workdir": "/tmp",
                "session_id": "id",
                "memory_file": "/config/project-CLAUDE.md"
            }]
        }"#;
        let cfg = SupervisorConfig::from_json_str(json).expect("parses");
        let s = &cfg.sessions[0];
        assert_eq!(
            s.memory_file,
            Some(PathBuf::from("/config/project-CLAUDE.md"))
        );
        assert!(s.validate_durations().is_ok());
    }

    #[test]
    fn test_memory_file_relative_path_rejected() {
        let json = r#"{
            "sessions": [{
                "name": "mem",
                "workdir": "/tmp",
                "session_id": "id",
                "memory_file": "project-CLAUDE.md"
            }]
        }"#;
        let cfg = SupervisorConfig::from_json_str(json).expect("parses");
        let s = &cfg.sessions[0];
        assert_eq!(s.memory_file, Some(PathBuf::from("project-CLAUDE.md")));
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("memory_file"), "err: {}", err);
    }
}

#[cfg(test)]
mod sandbox_flag_tests {
    use super::*;

    #[test]
    fn sandbox_defaults_on_and_can_be_disabled_per_session() {
        let on: SessionConfig =
            serde_json::from_str(r#"{"name":"a","workdir":"/w","session_id":"s"}"#).unwrap();
        assert!(on.sandbox);
        assert!(on.grants.is_none());
        let off: SessionConfig =
            serde_json::from_str(r#"{"name":"a","workdir":"/w","session_id":"s","sandbox":false}"#)
                .unwrap();
        assert!(!off.sandbox);
    }

    #[test]
    fn test_session_grants_parsing_and_validation() {
        // Valid grants JSON parses and validates
        let valid_json = r#"{
            "name": "sess",
            "workdir": "/w",
            "session_id": "s",
            "grants": {
                "serviceaccount": true,
                "ro_paths": ["/opt/tools", "/opt/ssh-home-cloud"],
                "rw_paths": ["/workspace", "/home/agent"],
                "env": ["GH_TOKEN", "VIKUNJA_*", "PATH"]
            }
        }"#;
        let s: SessionConfig = serde_json::from_str(valid_json).expect("valid grants must parse");
        assert!(s.validate_durations().is_ok());
        let g = s.grants.unwrap();
        assert!(g.serviceaccount);
        assert_eq!(g.ro_paths.len(), 2);
        assert_eq!(g.rw_paths.len(), 2);
        assert_eq!(g.env.len(), 3);

        // Unknown key in grants rejected (deny_unknown_fields)
        let unknown_key = r#"{
            "name": "sess",
            "workdir": "/w",
            "session_id": "s",
            "grants": {
                "serviceaccount": true,
                "unknown_key": "bad"
            }
        }"#;
        assert!(serde_json::from_str::<SessionConfig>(unknown_key).is_err());

        // Relative path rejected
        let rel_path = r#"{
            "name": "sess",
            "workdir": "/w",
            "session_id": "s",
            "grants": {
                "ro_paths": ["relative/path"]
            }
        }"#;
        let s: SessionConfig = serde_json::from_str(rel_path).unwrap();
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("non-absolute path"), "err: {err}");

        // Path traversal rejected
        let traversal = r#"{
            "name": "sess",
            "workdir": "/w",
            "session_id": "s",
            "grants": {
                "rw_paths": ["/workspace/../etc"]
            }
        }"#;
        let s: SessionConfig = serde_json::from_str(traversal).unwrap();
        let err = s.validate_durations().unwrap_err();
        assert!(err.contains("parent traversal"), "err: {err}");

        // Root or /proc or /dev forbidden
        for forbidden in ["/", "/proc", "/proc/1", "/dev", "/dev/null"] {
            let json = format!(
                r#"{{"name":"sess","workdir":"/w","session_id":"s","grants":{{"ro_paths":["{forbidden}"]}}}}"#
            );
            let s: SessionConfig = serde_json::from_str(&json).unwrap();
            let err = s.validate_durations().unwrap_err();
            assert!(err.contains("forbidden path"), "err: {err}");
        }

        // Invalid env names (empty, bad chars, inner wildcard)
        for bad_env in ["", "BAD*VAR", "BAD-VAR", "*", "FOO**"] {
            let json = format!(
                r#"{{"name":"sess","workdir":"/w","session_id":"s","grants":{{"env":["{bad_env}"]}}}}"#
            );
            let s: SessionConfig = serde_json::from_str(&json).unwrap();
            assert!(
                s.validate_durations().is_err(),
                "bad env should fail: {bad_env}"
            );
        }
    }
}

use crate::orchestrate::config::{warn_if_symlink_workdir, SessionConfig, SupervisorConfig};
use crate::orchestrate::logging::log_event;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Fixed namespace UUID for generating deterministic UUIDv5 session IDs for workspaces.
/// This constant must never be changed so that session IDs remain stable across runs.
pub const WORKSPACE_NAMESPACE: Uuid = Uuid::from_u128(0xa689b7b9_3b60_449a_8b17_0d5b7a14e911);

/// Environment configuration holder.
#[derive(Debug, Clone, Default)]
pub struct EnvConfig {
    pub workspaces: Option<String>,
    pub workspace_root: Option<PathBuf>,
    pub remote_prefix: Option<String>,
    pub default_session: Option<String>,
    pub default_session_id: Option<String>,
    pub session_args: Option<String>,
    pub home: Option<PathBuf>,
    pub max_crashes: Option<usize>,
    pub sandbox: Option<String>,
}

fn get_non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

impl EnvConfig {
    pub fn from_process_env() -> Self {
        Self {
            workspaces: get_non_empty_env("CLAUDE_WORKSPACES"),
            workspace_root: get_non_empty_env("CLAUDE_WORKSPACE_ROOT").map(PathBuf::from),
            remote_prefix: get_non_empty_env("CLAUDE_REMOTE_PREFIX"),
            default_session: get_non_empty_env("CLAUDE_DEFAULT_SESSION"),
            default_session_id: get_non_empty_env("CLAUDE_DEFAULT_SESSION_ID"),
            session_args: get_non_empty_env("CLAUDE_SESSION_ARGS"),
            home: get_non_empty_env("HOME")
                .map(PathBuf::from)
                .or_else(|| Some(PathBuf::from("/home/coder"))),
            max_crashes: get_non_empty_env("CLAUDE_MAX_CRASHES")
                .and_then(|v| v.parse::<usize>().ok()),
            sandbox: get_non_empty_env("CLAUDE_SANDBOX"),
        }
    }
}

/// Generates a stable UUIDv5 for a workspace session based on the string `ai-server-<name>`.
/// Note: Always uses `ai-server-<name>` rather than any custom prefix, ensuring IDs remain stable.
pub fn generate_workspace_session_id(workspace_name: &str) -> String {
    let input = format!("ai-server-{}", workspace_name);
    let id = Uuid::new_v5(&WORKSPACE_NAMESPACE, input.as_bytes());
    id.to_string()
}

/// Validates that a workspace/session name matches `^[a-z0-9][a-z0-9._-]*$`.
pub fn is_valid_workspace_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-')
}

/// Parses a boolean flag from an environment variable value.
/// Returns false for "0", "off", "false", "no" (case-insensitive).
pub fn parse_bool_value(val: Option<&str>, default: bool) -> bool {
    match val {
        Some(v) => match v.trim().to_lowercase().as_str() {
            "0" | "off" | "false" | "no" => false,
            "1" | "on" | "true" | "yes" => true,
            _ => default,
        },
        None => default,
    }
}

/// Discovers workspaces in `auto` mode: every non-hidden direct child of `root`
/// that is a directory (or symlink to directory) and contains a `.git` entry (file or dir).
/// Returns candidates sorted alphabetically by name.
pub fn discover_auto_workspaces(root: &Path) -> Vec<(String, PathBuf)> {
    let mut results = Vec::new();
    let read_dir = match std::fs::read_dir(root) {
        Ok(rd) => rd,
        Err(e) => {
            log_event(
                "supervisor",
                &format!(
                    "Workspace root directory '{}' not accessible: {}",
                    root.display(),
                    e
                ),
            );
            return results;
        }
    };

    for entry in read_dir.flatten() {
        let path = entry.path();
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();

        // Skip hidden entries (starting with '.')
        if name_str.starts_with('.') {
            continue;
        }

        // Must be a directory or symlink to a directory
        if !path.is_dir() {
            continue;
        }

        // Must contain a .git entry (file or dir)
        let git_entry = path.join(".git");
        if !git_entry.exists() {
            continue;
        }

        // Validate name format
        if !is_valid_workspace_name(&name_str) {
            log_event(
                "supervisor",
                &format!(
                    "Skipping workspace '{}': invalid name (must match [a-z0-9][a-z0-9._-]*)",
                    name_str
                ),
            );
            continue;
        }

        results.push((name_str.to_string(), path));
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));
    results
}

/// Parses explicit workspace names/paths from `CLAUDE_WORKSPACES`.
/// Supports comma- and/or whitespace-separated tokens. Absolute paths are accepted.
pub fn parse_explicit_workspaces(root: &Path, ws_str: &str) -> Vec<(String, PathBuf)> {
    let mut results = Vec::new();
    let tokens: Vec<&str> = ws_str
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    for token in tokens {
        let (name, path) = if Path::new(token).is_absolute() {
            let p = PathBuf::from(token);
            let n = p
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();
            (n, p)
        } else {
            (token.to_string(), root.join(token))
        };

        if !is_valid_workspace_name(&name) {
            log_event(
                "supervisor",
                &format!(
                    "Skipping workspace '{}': invalid name (must match [a-z0-9][a-z0-9._-]*)",
                    name
                ),
            );
            continue;
        }

        if !path.is_dir() {
            log_event(
                "supervisor",
                &format!(
                    "Skipping workspace '{}': path '{}' does not exist or is not a directory",
                    name,
                    path.display()
                ),
            );
            continue;
        }

        results.push((name, path));
    }

    results
}

/// Resolves the full supervisor configuration by merging:
/// 1. File sessions (from base config)
/// 2. Legacy default session (if enabled)
/// 3. Generated workspace sessions (from CLAUDE_WORKSPACES)
///
/// Validates duplicate session names and duplicate session IDs, exiting with an error if found.
pub fn resolve_supervisor_config_with_env(
    mut base: SupervisorConfig,
    env: &EnvConfig,
) -> Result<SupervisorConfig, String> {
    // Validate supervisor-level durations
    base.validate_durations()?;

    // 1. File sessions: validate internal duplicates and durations
    let mut file_names = HashSet::new();
    let mut file_ids = HashSet::new();
    for sess in &base.sessions {
        sess.validate_durations()?;
        if !file_names.insert(sess.name.clone()) {
            return Err(format!("Duplicate session name '{}'", sess.name));
        }
        if !file_ids.insert(sess.session_id.clone()) {
            return Err(format!(
                "Duplicate session id '{}' in session '{}'",
                sess.session_id, sess.name
            ));
        }
    }

    let remote_prefix = env
        .remote_prefix
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("ai-server")
        .to_string();

    let extra_args: Vec<String> = env
        .session_args
        .as_deref()
        .map(|s| s.split_whitespace().map(String::from).collect())
        .unwrap_or_default();

    let env_max_crashes = env.max_crashes.unwrap_or(0);
    let generated_sandbox =
        parse_bool_value(env.sandbox.as_deref(), base.default_sandbox.unwrap_or(true));

    let mut generated_candidates = Vec::new();

    // 2. Default legacy session
    let default_enabled = parse_bool_value(env.default_session.as_deref(), true);
    if default_enabled {
        let home = env
            .home
            .as_ref()
            .filter(|p| !p.as_os_str().is_empty() && !p.to_string_lossy().trim().is_empty())
            .cloned()
            .unwrap_or_else(|| PathBuf::from("/home/coder"));
        let default_session_id = env
            .default_session_id
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "10000000-0000-4000-8000-000000000001".to_string());

        let mut args = vec![
            "--dangerously-skip-permissions".to_string(),
            "--remote-control".to_string(),
            remote_prefix.clone(),
        ];
        args.extend(extra_args.iter().cloned());

        let default_sess = SessionConfig {
            name: "default".to_string(),
            workdir: home,
            session_id: default_session_id,
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: args,
            agy_binary: PathBuf::from("agy"),
            env: HashMap::new(),
            session_file: None,
            transcript_path: None,
            memory_file: None,
            readiness_poll_secs: 30.0,
            hang_timeout_secs: 0.0,
            kill_grace_secs: 5.0,
            pty_eof_grace_secs: 10.0,
            kill_wait_secs: 15.0,
            sandbox: generated_sandbox,
            grants: None,
            base_backoff_secs: 2.0,
            max_backoff_secs: 60.0,
            healthy_run_secs: 30.0,
            max_crashes: env_max_crashes,
            crash_window_secs: 60.0,
            prefix_output: false,
            failed_cooldown_secs: 300.0,
            backoff_window_secs: 600.0,
            resume_fail_quarantine_after: 5,
            resume_fail_uptime_secs: 5.0,
        };
        generated_candidates.push(default_sess);
    }

    // 3. Workspace sessions
    if let Some(ref ws_var) = env.workspaces {
        let ws_trim = ws_var.trim();
        if !ws_trim.is_empty() {
            let ws_root = env
                .workspace_root
                .as_ref()
                .filter(|p| !p.as_os_str().is_empty() && !p.to_string_lossy().trim().is_empty())
                .cloned()
                .unwrap_or_else(|| PathBuf::from("/workspace"));

            let discovered = if ws_trim == "auto" {
                discover_auto_workspaces(&ws_root)
            } else {
                parse_explicit_workspaces(&ws_root, ws_trim)
            };

            for (ws_name, ws_path) in discovered {
                let session_id = generate_workspace_session_id(&ws_name);
                let remote_name = format!("{}-{}", remote_prefix, ws_name);
                let mut args = vec![
                    "--dangerously-skip-permissions".to_string(),
                    "--remote-control".to_string(),
                    remote_name,
                ];
                args.extend(extra_args.iter().cloned());

                let ws_sess = SessionConfig {
                    name: ws_name,
                    workdir: ws_path,
                    session_id,
                    engine: None,
                    claude_binary: PathBuf::from("claude"),
                    claude_args: args,
                    agy_binary: PathBuf::from("agy"),
                    env: HashMap::new(),
                    session_file: None,
                    transcript_path: None,
                    memory_file: None,
                    readiness_poll_secs: 30.0,
                    hang_timeout_secs: 0.0,
                    kill_grace_secs: 5.0,
                    pty_eof_grace_secs: 10.0,
                    kill_wait_secs: 15.0,
                    sandbox: generated_sandbox,
                    grants: None,
                    base_backoff_secs: 2.0,
                    max_backoff_secs: 60.0,
                    healthy_run_secs: 30.0,
                    max_crashes: env_max_crashes,
                    crash_window_secs: 60.0,
                    prefix_output: false,
                    failed_cooldown_secs: 300.0,
                    backoff_window_secs: 600.0,
                    resume_fail_quarantine_after: 5,
                    resume_fail_uptime_secs: 5.0,
                };
                generated_candidates.push(ws_sess);
            }
        }
    }

    // Fix #9: Filter out generated sessions that conflict with file sessions.
    // If an env-generated session has the same name OR session_id as a file session,
    // skip the generated one with a logged warning (file wins).
    let mut accepted_generated = Vec::new();
    let mut gen_names = HashSet::new();
    let mut gen_ids = HashSet::new();

    for gen in generated_candidates {
        if file_names.contains(&gen.name) || file_ids.contains(&gen.session_id) {
            log_event(
                "supervisor",
                &format!(
                    "Warning: Skipping generated session '{}' (session_id '{}'): conflicts with file-configured session (file wins)",
                    gen.name, gen.session_id
                ),
            );
            continue;
        }

        // Duplicates within generated sessions must still error
        if !gen_names.insert(gen.name.clone()) {
            return Err(format!("Duplicate session name '{}'", gen.name));
        }
        if !gen_ids.insert(gen.session_id.clone()) {
            return Err(format!(
                "Duplicate session id '{}' in session '{}'",
                gen.session_id, gen.name
            ));
        }

        gen.validate_durations()?;
        accepted_generated.push(gen);
    }

    // Merge: file sessions first, then accepted generated sessions
    let mut merged = std::mem::take(&mut base.sessions);
    merged.extend(accepted_generated);

    // Populate transcript_path and warn for symlinks
    for sess in &mut merged {
        warn_if_symlink_workdir(&sess.name, &sess.workdir);
        if sess.transcript_path.is_none() {
            let transcript = base.resolve_session_file(sess);
            sess.transcript_path = Some(transcript);
        }
        // HL-91 E5 cross-use: the supervisor-level agy path overrides every
        // session's agy binary (the per-session value only matters when no
        // supervisor-wide path is configured).
        if let Some(agy_path) = &base.agy_path {
            sess.agy_binary = agy_path.clone();
        }
    }

    base.sessions = merged;
    Ok(base)
}

/// Resolves supervisor config using real process environment variables.
pub fn resolve_supervisor_config(base: SupervisorConfig) -> Result<SupervisorConfig, String> {
    resolve_supervisor_config_with_env(base, &EnvConfig::from_process_env())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_rfc4122_uuid_v5_vector() {
        let dns_ns = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let id = Uuid::new_v5(&dns_ns, b"python.org");
        assert_eq!(id.to_string(), "886313e1-3b8a-5372-9b90-0c9aee199e5d");
    }

    #[test]
    fn test_uuid_v5_determinism() {
        let id1 = generate_workspace_session_id("homelab");
        let id2 = generate_workspace_session_id("homelab");
        assert_eq!(id1, id2);

        let id_nested = generate_workspace_session_id("nested-operator");
        assert_ne!(id1, id_nested);
    }

    #[test]
    fn test_name_validation() {
        // Valid
        assert!(is_valid_workspace_name("homelab"));
        assert!(is_valid_workspace_name("nested-operator"));
        assert!(is_valid_workspace_name("repo.v1"));
        assert!(is_valid_workspace_name("repo_2"));
        assert!(is_valid_workspace_name("123-repo"));

        // Invalid
        assert!(!is_valid_workspace_name(""));
        assert!(!is_valid_workspace_name("_bad"));
        assert!(!is_valid_workspace_name(".bad"));
        assert!(!is_valid_workspace_name("-bad"));
        assert!(!is_valid_workspace_name("BadCapital"));
        assert!(!is_valid_workspace_name("has space"));
        assert!(!is_valid_workspace_name("has/slash"));
    }

    #[test]
    fn test_auto_discovery() {
        let temp_dir = std::env::temp_dir().join(format!("test_auto_ws_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        // 1. Plain repo dir (with .git directory)
        let repo_plain = temp_dir.join("repo-plain");
        fs::create_dir_all(repo_plain.join(".git")).unwrap();

        // 2. Symlinked repo (target directory with .git file, like worktree)
        let target_repo = temp_dir.join("target-repo");
        fs::create_dir_all(&target_repo).unwrap();
        fs::write(target_repo.join(".git"), "gitdir: /some/path\n").unwrap();
        let symlink_repo = temp_dir.join("repo-symlink");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target_repo, &symlink_repo).unwrap();

        // 3. Dir without .git
        let no_git = temp_dir.join("no-git-dir");
        fs::create_dir_all(&no_git).unwrap();

        // 4. Hidden dir with .git
        let hidden_dir = temp_dir.join(".hidden-repo");
        fs::create_dir_all(hidden_dir.join(".git")).unwrap();

        // 5. Plain file
        let regular_file = temp_dir.join("file.txt");
        fs::write(&regular_file, "content").unwrap();

        let discovered = discover_auto_workspaces(&temp_dir);
        let names: Vec<String> = discovered.into_iter().map(|(n, _)| n).collect();

        // Target repo was not matched directly because its name is target-repo and has .git, wait:
        // target-repo is also in temp_dir!
        // So target-repo, repo-plain, and repo-symlink are in temp_dir.
        // Let's assert repo-plain and repo-symlink are present, and hidden / no-git / file are not!
        assert!(names.contains(&"repo-plain".to_string()));
        assert!(names.contains(&"repo-symlink".to_string()));
        assert!(!names.contains(&"no-git-dir".to_string()));
        assert!(!names.contains(&".hidden-repo".to_string()));
        assert!(!names.contains(&"file.txt".to_string()));

        // Check alphabetical sorting
        for i in 1..names.len() {
            assert!(names[i - 1] <= names[i]);
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_default_session_toggle() {
        let base = SupervisorConfig::default();

        let env_off = EnvConfig {
            default_session: Some("off".to_string()),
            ..Default::default()
        };
        let cfg_off = resolve_supervisor_config_with_env(base.clone(), &env_off).unwrap();
        assert!(cfg_off.sessions.is_empty());

        let env_on = EnvConfig {
            default_session: Some("1".to_string()),
            ..Default::default()
        };
        let cfg_on = resolve_supervisor_config_with_env(base, &env_on).unwrap();
        assert_eq!(cfg_on.sessions.len(), 1);
        assert_eq!(cfg_on.sessions[0].name, "default");
    }

    #[test]
    fn test_remote_prefix_and_session_args() {
        let temp_dir = std::env::temp_dir().join(format!("test_prefix_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let ws_dir = temp_dir.join("my-workspace");
        fs::create_dir_all(&ws_dir).unwrap();

        let base = SupervisorConfig::default();
        let env = EnvConfig {
            remote_prefix: Some("custom-agent".to_string()),
            session_args: Some("--model claude-3-5-haiku --verbose".to_string()),
            workspaces: Some(format!("{}", ws_dir.display())),
            ..Default::default()
        };

        let cfg = resolve_supervisor_config_with_env(base, &env).unwrap();
        assert_eq!(cfg.sessions.len(), 2); // default + my-workspace

        let def_sess = &cfg.sessions[0];
        assert_eq!(def_sess.name, "default");
        assert!(def_sess.claude_args.contains(&"custom-agent".to_string()));
        assert!(def_sess.claude_args.contains(&"--model".to_string()));
        assert!(def_sess
            .claude_args
            .contains(&"claude-3-5-haiku".to_string()));

        let ws_sess = &cfg.sessions[1];
        assert_eq!(ws_sess.name, "my-workspace");
        assert!(ws_sess
            .claude_args
            .contains(&"custom-agent-my-workspace".to_string()));
        assert!(ws_sess.claude_args.contains(&"--verbose".to_string()));

        // UUIDv5 must match ai-server-my-workspace regardless of custom-agent prefix
        let expected_uuid = generate_workspace_session_id("my-workspace");
        assert_eq!(ws_sess.session_id, expected_uuid);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_duplicate_name_error() {
        let temp_dir = std::env::temp_dir().join(format!("test_dup_name_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let ws_dir = temp_dir.join("default"); // Same name as default session!
        fs::create_dir_all(&ws_dir).unwrap();

        let base = SupervisorConfig::default();
        let env = EnvConfig {
            default_session: Some("true".to_string()),
            workspaces: Some(format!("{}", ws_dir.display())),
            ..Default::default()
        };

        let res = resolve_supervisor_config_with_env(base, &env);
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .contains("Duplicate session name 'default'"));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_file_wins_over_generated_session_id() {
        let mut base = SupervisorConfig::default();
        let file_sess = SessionConfig {
            name: "file-sess".to_string(),
            workdir: PathBuf::from("/tmp"),
            session_id: "10000000-0000-4000-8000-000000000001".to_string(), // Same ID as default!
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: std::path::PathBuf::from("agy"),
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
        };
        base.sessions.push(file_sess);

        let env = EnvConfig::default(); // has default session with same ID
                                        // File wins: generated default session is skipped with warning, no error
        let res = resolve_supervisor_config_with_env(base, &env).unwrap();
        assert_eq!(res.sessions.len(), 1);
        assert_eq!(res.sessions[0].name, "file-sess");
    }

    #[test]
    fn test_file_wins_over_generated_session_name() {
        let mut base = SupervisorConfig::default();
        let file_sess = SessionConfig {
            name: "default".to_string(), // Same name as default session!
            workdir: PathBuf::from("/custom/workdir"),
            session_id: "custom-file-id-123".to_string(),
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: std::path::PathBuf::from("agy"),
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
        };
        base.sessions.push(file_sess);

        let env = EnvConfig::default(); // has default session enabled
        let res = resolve_supervisor_config_with_env(base, &env).unwrap();
        assert_eq!(res.sessions.len(), 1);
        assert_eq!(res.sessions[0].session_id, "custom-file-id-123");
    }

    #[test]
    fn test_duplicate_name_within_file_sessions_errors() {
        let mut base = SupervisorConfig::default();
        let s1 = SessionConfig {
            name: "dup-file".to_string(),
            workdir: PathBuf::from("/tmp/1"),
            session_id: "id-1".to_string(),
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: std::path::PathBuf::from("agy"),
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
        };
        let mut s2 = s1.clone();
        s2.session_id = "id-2".to_string(); // Same name, different ID
        base.sessions.push(s1);
        base.sessions.push(s2);

        let env = EnvConfig {
            default_session: Some("false".to_string()),
            ..Default::default()
        };
        let res = resolve_supervisor_config_with_env(base, &env);
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .contains("Duplicate session name 'dup-file'"));
    }

    #[test]
    fn test_duplicate_id_within_file_sessions_errors() {
        let mut base = SupervisorConfig::default();
        let s1 = SessionConfig {
            name: "file-1".to_string(),
            workdir: PathBuf::from("/tmp/1"),
            session_id: "same-id".to_string(),
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: std::path::PathBuf::from("agy"),
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
        };
        let mut s2 = s1.clone();
        s2.name = "file-2".to_string(); // Different name, same ID
        base.sessions.push(s1);
        base.sessions.push(s2);

        let env = EnvConfig {
            default_session: Some("false".to_string()),
            ..Default::default()
        };
        let res = resolve_supervisor_config_with_env(base, &env);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("Duplicate session id 'same-id'"));
    }

    #[test]
    fn test_env_generated_sessions_default_max_crashes_and_readiness() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_env_defaults_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let ws_dir = temp_dir.join("test-ws");
        fs::create_dir_all(&ws_dir).unwrap();

        let base = SupervisorConfig::default();
        let env = EnvConfig {
            default_session: Some("true".to_string()),
            workspaces: Some(format!("{}", ws_dir.display())),
            ..Default::default()
        };

        let cfg = resolve_supervisor_config_with_env(base, &env).unwrap();
        assert_eq!(cfg.sessions.len(), 2);

        // Default session has max_crashes = 0 and readiness_poll_secs = 30.0
        assert_eq!(cfg.sessions[0].name, "default");
        assert_eq!(cfg.sessions[0].max_crashes, 0);
        assert_eq!(cfg.sessions[0].readiness_poll_secs, 30.0);

        // Workspace session has max_crashes = 0 and readiness_poll_secs = 30.0
        assert_eq!(cfg.sessions[1].name, "test-ws");
        assert_eq!(cfg.sessions[1].max_crashes, 0);
        assert_eq!(cfg.sessions[1].readiness_poll_secs, 30.0);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_env_max_crashes_override() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_env_override_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let ws_dir = temp_dir.join("test-ws");
        fs::create_dir_all(&ws_dir).unwrap();

        let base = SupervisorConfig::default();
        let env = EnvConfig {
            default_session: Some("true".to_string()),
            workspaces: Some(format!("{}", ws_dir.display())),
            max_crashes: Some(8),
            ..Default::default()
        };

        let cfg = resolve_supervisor_config_with_env(base, &env).unwrap();
        assert_eq!(cfg.sessions.len(), 2);
        assert_eq!(cfg.sessions[0].max_crashes, 8);
        assert_eq!(cfg.sessions[1].max_crashes, 8);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_empty_and_whitespace_env_vars_treated_as_unset() {
        let _guard = crate::test_env_lock();
        let keys = [
            "CLAUDE_WORKSPACES",
            "CLAUDE_WORKSPACE_ROOT",
            "CLAUDE_REMOTE_PREFIX",
            "CLAUDE_DEFAULT_SESSION",
            "CLAUDE_DEFAULT_SESSION_ID",
            "CLAUDE_SESSION_ARGS",
            "CLAUDE_MAX_CRASHES",
            "CLAUDE_SANDBOX",
            "HOME",
        ];
        let saved: Vec<(&str, Option<String>)> =
            keys.iter().map(|&k| (k, std::env::var(k).ok())).collect();

        std::env::set_var("CLAUDE_WORKSPACES", "   ");
        std::env::set_var("CLAUDE_WORKSPACE_ROOT", "   ");
        std::env::set_var("CLAUDE_REMOTE_PREFIX", " \t ");
        std::env::set_var("CLAUDE_DEFAULT_SESSION", "   ");
        std::env::set_var("CLAUDE_DEFAULT_SESSION_ID", " \n ");
        std::env::set_var("CLAUDE_SESSION_ARGS", "   ");
        std::env::set_var("CLAUDE_MAX_CRASHES", "   ");
        std::env::set_var("CLAUDE_SANDBOX", "   ");
        std::env::set_var("HOME", "   ");

        let env = EnvConfig::from_process_env();
        assert_eq!(env.workspaces, None);
        assert_eq!(env.workspace_root, None);
        assert_eq!(env.remote_prefix, None);
        assert_eq!(env.default_session, None);
        assert_eq!(env.default_session_id, None);
        assert_eq!(env.session_args, None);
        assert_eq!(env.max_crashes, None);
        assert_eq!(env.sandbox, None);
        assert_eq!(env.home, Some(PathBuf::from("/home/coder")));

        for &k in &keys {
            std::env::remove_var(k);
        }

        let env_unset = EnvConfig::from_process_env();
        assert_eq!(env_unset.home, Some(PathBuf::from("/home/coder")));

        for (k, v) in saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn test_generated_workspace_sessions_respect_sandbox_switch() {
        let temp_dir = std::env::temp_dir().join(format!("test_ws_sandbox_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        let ws_dir = temp_dir.join("repo-a");
        fs::create_dir_all(&ws_dir).unwrap();

        // 1. Default: sandbox is true for both default and generated workspace sessions.
        let env_default = EnvConfig {
            default_session: Some("true".to_string()),
            workspaces: Some(ws_dir.display().to_string()),
            ..Default::default()
        };
        let cfg_default =
            resolve_supervisor_config_with_env(SupervisorConfig::default(), &env_default).unwrap();
        assert_eq!(cfg_default.sessions.len(), 2);
        assert!(cfg_default.sessions.iter().all(|s| s.sandbox));

        // 2. SupervisorConfig.default_sandbox = Some(false) disables sandbox on generated sessions.
        let base_off = SupervisorConfig {
            default_sandbox: Some(false),
            ..Default::default()
        };
        let cfg_base_off =
            resolve_supervisor_config_with_env(base_off.clone(), &env_default).unwrap();
        assert!(cfg_base_off.sessions.iter().all(|s| !s.sandbox));

        // 3. EnvConfig.sandbox ("CLAUDE_SANDBOX") overrides base.default_sandbox.
        let env_override_off = EnvConfig {
            sandbox: Some("false".to_string()),
            ..env_default.clone()
        };
        let cfg_env_off =
            resolve_supervisor_config_with_env(SupervisorConfig::default(), &env_override_off)
                .unwrap();
        assert!(cfg_env_off.sessions.iter().all(|s| !s.sandbox));

        let env_override_on = EnvConfig {
            sandbox: Some("true".to_string()),
            ..env_default
        };
        let cfg_env_on = resolve_supervisor_config_with_env(base_off, &env_override_on).unwrap();
        assert!(cfg_env_on.sessions.iter().all(|s| s.sandbox));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_explicit_transcript_path_preserved_in_workspace_resolution() {
        let mut base = SupervisorConfig::default();
        let file_sess = SessionConfig {
            name: "explicit-session".to_string(),
            workdir: PathBuf::from("/tmp"),
            session_id: "id-explicit".to_string(),
            engine: None,
            claude_binary: PathBuf::from("claude"),
            claude_args: vec![],
            agy_binary: std::path::PathBuf::from("agy"),
            env: HashMap::new(),
            session_file: None,
            transcript_path: Some(PathBuf::from("/custom/explicit/file.jsonl")),
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
        base.sessions.push(file_sess);

        let env = EnvConfig {
            default_session: Some("false".to_string()),
            ..Default::default()
        };

        let resolved = resolve_supervisor_config_with_env(base, &env).unwrap();
        assert_eq!(resolved.sessions.len(), 1);
        assert_eq!(
            resolved.sessions[0].transcript_path,
            Some(PathBuf::from("/custom/explicit/file.jsonl"))
        );
    }
}

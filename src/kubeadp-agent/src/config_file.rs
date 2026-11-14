//! TOML configuration file support for the kubeadp-agent roles.
//!
//! Both long-lived roles of the `kubeadp-agent` binary (orchestrator and
//! worker) -- and the thin `agent-orchestrate` / `agent-run-api` wrappers --
//! can be configured from a single TOML file that maps 1:1 onto the
//! environment variables the code already reads. The pattern mirrors
//! `info-hub-mcp`'s `src/config.rs` (`config_file_map`, `parse_config_toml`,
//! `var_for_section_key`).
//!
//! ## File resolution
//!
//! - `KUBEADP_CONFIG_FILE` (env) selects the file; an empty value is an
//!   error.
//! - When unset, `/etc/kubeadp-agent/config.toml` is used only if it exists.
//! - An absent file means environment-only configuration (no error).
//!
//! ## Sections and keys
//!
//! The file has three sections -- `[common]`, `[orchestrator]`, `[worker]`.
//! Every key maps 1:1 to an environment variable the code ALREADY reads.
//! Keys are lower-case; a key in the wrong section, an unknown key, a
//! duplicate key or a non-scalar value is a hard error naming `section.key`.
//! Booleans are rendered `true`/`false`, numbers as decimal strings.
//!
//! Full key table (section `key` -> environment variable):
//!
//! ```text
//! [common]
//!   no_proxy                -> AGENT_RUN_NO_PROXY
//!   http_proxy              -> AGENT_RUN_HTTP_PROXY
//!   https_proxy             -> AGENT_RUN_HTTPS_PROXY
//!   api_token_file          -> KUBEADP_API_TOKEN_FILE
//!   api_allow_unauthenticated -> KUBEADP_API_ALLOW_UNAUTHENTICATED
//!   jobset_root_key_file    -> KUBEADP_JOBSET_ROOT_KEY_FILE
//!
//! [orchestrator]
//!   listen                  -> AGENT_ORCHESTRATE_LISTEN
//!   token_file              -> AGENT_ORCHESTRATE_TOKEN_FILE
//!   session_log_ring_bytes  -> AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES
//!   config                  -> AGENT_ORCHESTRATE_CONFIG
//!   workspaces              -> CLAUDE_WORKSPACES
//!   workspace_root          -> CLAUDE_WORKSPACE_ROOT
//!   remote_prefix           -> CLAUDE_REMOTE_PREFIX
//!   default_session         -> CLAUDE_DEFAULT_SESSION
//!   default_session_id      -> CLAUDE_DEFAULT_SESSION_ID
//!   session_args            -> CLAUDE_SESSION_ARGS
//!   claude_max_crashes      -> CLAUDE_MAX_CRASHES
//!
//! [worker]
//!   workspace_root          -> AGENT_RUN_WORKSPACE_ROOT
//!   jobs_dir_name           -> AGENT_RUN_JOBS_DIR_NAME
//!   agy_path                -> AGENT_RUN_AGY_PATH
//!   claude_path             -> AGENT_RUN_CLAUDE_PATH
//!   default_model           -> AGENT_RUN_DEFAULT_MODEL
//!   default_effort          -> AGENT_RUN_DEFAULT_EFFORT
//!   default_base_branch     -> AGENT_RUN_DEFAULT_BASE_BRANCH
//!   state_root              -> AGENT_RUN_STATE_ROOT
//!   repos_root              -> AGENT_RUN_REPOS_ROOT
//!   gc_max_age_days         -> AGENT_RUN_GC_MAX_AGE_DAYS
//!   max_parallel            -> AGENT_RUN_MAX_PARALLEL
//!   shutdown_grace_secs     -> AGENT_RUN_SHUTDOWN_GRACE
//!   launch_timeout_secs     -> AGENT_RUN_LAUNCH_TIMEOUT_SECS
//!   stall_minutes           -> AGENT_RUN_STALL_MINUTES
//!   max_runtime             -> AGENT_RUN_MAX_RUNTIME
//!   hindsight_enabled       -> AGENT_RUN_HINDSIGHT_ENABLED
//!   hindsight_bank          -> AGENT_RUN_HINDSIGHT_BANK
//!   hindsight_tag           -> AGENT_RUN_HINDSIGHT_TAG
//!   hindsight_path          -> AGENT_RUN_HINDSIGHT_PATH
//!   hindsight_auto_recall   -> AGENT_RUN_HINDSIGHT_AUTO_RECALL
//!   hindsight_auto_retain   -> AGENT_RUN_HINDSIGHT_AUTO_RETAIN
//!   hindsight_url           -> AGENT_RUN_HINDSIGHT_URL
//!   hindsight_profile       -> AGENT_RUN_HINDSIGHT_PROFILE
//!   remote_control          -> AGENT_RUN_REMOTE_CONTROL
//!   remote_control_autostart -> AGENT_RUN_REMOTE_CONTROL_AUTOSTART
//!   remote_control_check_secs -> AGENT_RUN_REMOTE_CONTROL_CHECK_SECS
//!   default_engine          -> AGENT_RUN_DEFAULT_ENGINE
//!   api_listen              -> AGENT_RUN_API_LISTEN
//!   agy_mcp_source          -> AGENT_RUN_AGY_MCP_SOURCE
//!   a2a_interrupt_min_secs  -> AGENT_RUN_A2A_INTERRUPT_MIN_SECS
//!   a2a_live_stdin          -> AGENT_RUN_A2A_LIVE_STDIN
//!   a2a_live_idle_secs      -> AGENT_RUN_A2A_LIVE_IDLE_SECS
//!   a2a_mcp_enabled         -> AGENT_RUN_A2A_MCP_ENABLED
//!   hindsight_api_key       -> (rejected: secret, set AGENT_RUN_HINDSIGHT_API_KEY from a Secret)
//! ```
//!
//! ## Engine selection defaults (HL-91 E1)
//!
//! The engine registry's per-role default (selection precedence level 3) is
//! configured with `[worker] default_engine` (jobs) and
//! `[orchestrator] default_session_engine` (sessions). Like every other key
//! these are lower-case scalars mapping 1:1 to environment variables; they
//! are unused until E3/E5 route jobs/sessions behind the engine trait.
//!
//! ## Secrets
//!
//! Secrets are never placed inline. The orchestrator bearer token accepts only
//! the `_FILE` form:
//!
//! - `[orchestrator] token_file` -> `AGENT_ORCHESTRATE_TOKEN_FILE`
//!   (the inline `AGENT_ORCHESTRATE_TOKEN` variable has no TOML key).
//! - The worker's Hindsight API key has no TOML key at all: the worker reads
//!   it from the environment variable `AGENT_RUN_HINDSIGHT_API_KEY`, which must
//!   come from a Secret, never from the ConfigMap.
//! - The shared operator API token (`[common] api_token_file` ->
//!   `KUBEADP_API_TOKEN_FILE`) likewise accepts only the `_FILE` form; an
//!   inline `[common] token` is rejected.
//! - The jobset root key (`[common] jobset_root_key_file` ->
//!   `KUBEADP_JOBSET_ROOT_KEY_FILE`) likewise accepts only the `_FILE` form;
//!   an inline `[common] jobset_root_key` is rejected.
//!
//! A key that would put one of these secrets inline is rejected with a message
//! saying how to supply it.
//!
//! ## Precedence and application
//!
//! The environment always wins. [`apply_to_env`] sets each variable ONLY if
//! it is not already set in the process environment; call it before the role
//! starts. The role applies `[common]` plus its own section; the other
//! role's section is parsed and validated but its values are not applied, so
//! one ConfigMap can serve both roles.
//!
//! Environment-only variables (loader directives, role selection) and the
//! test-only `*_TEST_*` hooks have no TOML key. `KUBEADP_ROLE` chooses the
//! role and is never read from the file.

use std::collections::HashMap;
use std::path::Path;

/// Environment variable selecting the configuration file.
pub const CONFIG_FILE_ENV: &str = "KUBEADP_CONFIG_FILE";

/// Fallback path when `KUBEADP_CONFIG_FILE` is unset: used only if it
/// exists; an absent file means environment-only configuration.
pub const DEFAULT_CONFIG_FILE: &str = "/etc/kubeadp-agent/config.toml";

/// Resolve the file map from the process environment, following the same
/// semantics as `config_file_map` in info-hub-mcp: an explicit path must
/// exist and be read (errors are fatal), an absent file yields the empty map.
pub fn config_file_map() -> Result<HashMap<String, String>, String> {
    let path = match std::env::var(CONFIG_FILE_ENV) {
        Ok(p) if !p.trim().is_empty() => p.trim().to_string(),
        Ok(_) => return Err(format!("{CONFIG_FILE_ENV} must not be empty")),
        Err(_) => {
            if !Path::new(DEFAULT_CONFIG_FILE).exists() {
                return Ok(HashMap::new());
            }
            DEFAULT_CONFIG_FILE.to_string()
        }
    };
    if !Path::new(&path).exists() {
        // An absent configuration file is environment-only configuration.
        return Ok(HashMap::new());
    }
    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read configuration file '{path}': {e}"))?;
    parse_config_toml(&path, &content)
}

/// Parses the TOML configuration file content into the variable map that
/// [`apply_to_env`] consumes. `path` is used only for error messages.
pub fn parse_config_toml(path: &str, content: &str) -> Result<HashMap<String, String>, String> {
    let parsed: toml::Value = toml::from_str(content).map_err(|e| {
        format!(
            "Invalid TOML in configuration file '{path}' at {}:{}: {}",
            e.span().map(|s| s.start).unwrap_or(0),
            e.span().map(|s| s.end).unwrap_or(0),
            e.message(),
        )
    })?;

    let mut vars: HashMap<String, String> = HashMap::new();
    let root = parsed.as_table().ok_or_else(|| {
        format!("Invalid configuration file '{path}': the document root must be a table")
    })?;
    for (section, value) in root {
        let table = value.as_table().ok_or_else(|| {
            format!("Invalid configuration file '{path}': section '{section}' must be a table")
        })?;
        // The `repo-sync` subcommand has its own dedicated configuration file
        // (KUBEADP_REPO_SYNC_CONFIG, see `core::reposync`); there are no env
        // variables here for this file to set, so a `[repo_sync]` section
        // must ERROR like any unknown key instead of being silently dropped.
        let section = if section == "repo_sync" {
            return Err(format!(
                "Invalid configuration file '{path}': section '[repo_sync]' is not supported here; configure the repo-sync subcommand in its own file (KUBEADP_REPO_SYNC_CONFIG)"
            ));
        } else {
            section
        };
        for (key, value) in table {
            if key.to_ascii_lowercase() != key.as_str() {
                return Err(format!(
                    "Invalid configuration file '{path}': key '{section}.{key}' must be lower-case"
                ));
            }
            if let Some(secret_var) = secret_inline_for_section_key(section, key) {
                return Err(format!(
                    "Invalid configuration file '{path}': secret '{section}.{key}' must not be set inline; supply '{secret_var}' through the environment from a Secret{}",
                    if section == "orchestrator" {
                        " or use '[orchestrator] token_file'"
                    } else {
                        ""
                    }
                ));
            }
            let resolved = var_for_section_key(section, key).ok_or_else(|| {
                format!("Invalid configuration file '{path}': unknown key '{section}.{key}'")
            })?;
            if vars.contains_key(&resolved) {
                return Err(format!(
                    "Invalid configuration file '{path}': duplicate key '{section}.{key}'"
                ));
            }
            let value = scalar_to_string(value).ok_or_else(|| {
                format!(
                    "Invalid configuration file '{path}': key '{section}.{key}' must be a scalar value"
                )
            })?;
            vars.insert(resolved, value);
        }
    }
    Ok(vars)
}

/// The secret environment variable a TOML `section.key` would set inline (for
/// the keys that are deliberately NOT in [`var_for_section_key`]). Returns
/// `Some(var)` for the inline spelling of a secret so the loader can reject
/// it with a `_FILE`-pointing message instead of "unknown key".
fn secret_inline_for_section_key(section: &str, key: &str) -> Option<&'static str> {
    match (section, key) {
        ("orchestrator", "token") => Some("AGENT_ORCHESTRATE_TOKEN"),
        // The operator token has no inline-TMPL key in either section: the
        // only spellings are the shared `[common] api_token_file`
        // (KUBEADP_API_TOKEN_FILE) and, for the orchestrator's legacy
        // `[orchestrator] token_file`. An inline `token` under either role
        // is rejected.
        ("common", "token") => Some("KUBEADP_API_TOKEN_FILE"),
        // The jobset root key likewise has no inline spelling: only the
        // shared `[common] jobset_root_key_file` (KUBEADP_JOBSET_ROOT_KEY_FILE).
        ("common", "jobset_root_key") => Some("KUBEADP_JOBSET_ROOT_KEY_FILE"),
        ("worker", "hindsight_api_key") => Some("AGENT_RUN_HINDSIGHT_API_KEY"),
        _ => None,
    }
}

/// Resolves a TOML `section.key` pair to the environment variable it stands
/// for, or `None` when the key is not a valid setting. This is the exact 1:1
/// mapping described in the module documentation; every variable the code
/// reads is listed explicitly (no prefix magic).
fn var_for_section_key(section: &str, key: &str) -> Option<String> {
    match (section, key) {
        // --- [common]: variables read by both roles (proxy settings) ---
        ("common", "no_proxy") => Some("AGENT_RUN_NO_PROXY".to_string()),
        ("common", "http_proxy") => Some("AGENT_RUN_HTTP_PROXY".to_string()),
        ("common", "https_proxy") => Some("AGENT_RUN_HTTPS_PROXY".to_string()),
        // The operator bearer token protecting the whole `/api/v1` and A2A
        // surface. It is a secret, supplied through a file (see
        // secret_inline_for_section_key); both roles read the same variable.
        ("common", "api_token_file") => Some("KUBEADP_API_TOKEN_FILE".to_string()),
        ("common", "api_allow_unauthenticated") => {
            Some("KUBEADP_API_ALLOW_UNAUTHENTICATED".to_string())
        }
        // The jobset root key: the file whose content is trimmed to become the
        // HMAC key deriving per-jobset secrets. Shared by both roles, a secret
        // supplied through a file only (see secret_inline_for_section_key).
        ("common", "jobset_root_key_file") => Some("KUBEADP_JOBSET_ROOT_KEY_FILE".to_string()),

        // --- [orchestrator]: the agent-orchestrate session supervisor ---
        ("orchestrator", "listen") => Some("AGENT_ORCHESTRATE_LISTEN".to_string()),
        ("orchestrator", "token_file") => Some("AGENT_ORCHESTRATE_TOKEN_FILE".to_string()),
        ("orchestrator", "session_log_ring_bytes") => {
            Some("AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES".to_string())
        }
        ("orchestrator", "config") => Some("AGENT_ORCHESTRATE_CONFIG".to_string()),
        ("orchestrator", "workspaces") => Some("CLAUDE_WORKSPACES".to_string()),
        ("orchestrator", "workspace_root") => Some("CLAUDE_WORKSPACE_ROOT".to_string()),
        ("orchestrator", "remote_prefix") => Some("CLAUDE_REMOTE_PREFIX".to_string()),
        ("orchestrator", "default_session") => Some("CLAUDE_DEFAULT_SESSION".to_string()),
        ("orchestrator", "default_session_id") => Some("CLAUDE_DEFAULT_SESSION_ID".to_string()),
        ("orchestrator", "session_args") => Some("CLAUDE_SESSION_ARGS".to_string()),
        ("orchestrator", "claude_max_crashes") => Some("CLAUDE_MAX_CRASHES".to_string()),
        ("orchestrator", "default_session_engine") => {
            Some("AGENT_ORCHESTRATE_DEFAULT_SESSION_ENGINE".to_string())
        }

        // --- [worker]: the agent-run API daemon ---
        ("worker", "workspace_root") => Some("AGENT_RUN_WORKSPACE_ROOT".to_string()),
        ("worker", "jobs_dir_name") => Some("AGENT_RUN_JOBS_DIR_NAME".to_string()),
        ("worker", "agy_path") => Some("AGENT_RUN_AGY_PATH".to_string()),
        ("worker", "claude_path") => Some("AGENT_RUN_CLAUDE_PATH".to_string()),
        ("worker", "default_model") => Some("AGENT_RUN_DEFAULT_MODEL".to_string()),
        ("worker", "default_effort") => Some("AGENT_RUN_DEFAULT_EFFORT".to_string()),
        ("worker", "default_base_branch") => Some("AGENT_RUN_DEFAULT_BASE_BRANCH".to_string()),
        ("worker", "state_root") => Some("AGENT_RUN_STATE_ROOT".to_string()),
        ("worker", "repos_root") => Some("AGENT_RUN_REPOS_ROOT".to_string()),
        ("worker", "gc_max_age_days") => Some("AGENT_RUN_GC_MAX_AGE_DAYS".to_string()),
        ("worker", "max_parallel") => Some("AGENT_RUN_MAX_PARALLEL".to_string()),
        ("worker", "shutdown_grace_secs") => Some("AGENT_RUN_SHUTDOWN_GRACE".to_string()),
        ("worker", "launch_timeout_secs") => Some("AGENT_RUN_LAUNCH_TIMEOUT_SECS".to_string()),
        ("worker", "stall_minutes") => Some("AGENT_RUN_STALL_MINUTES".to_string()),
        ("worker", "max_runtime") => Some("AGENT_RUN_MAX_RUNTIME".to_string()),
        ("worker", "hindsight_enabled") => Some("AGENT_RUN_HINDSIGHT_ENABLED".to_string()),
        ("worker", "hindsight_bank") => Some("AGENT_RUN_HINDSIGHT_BANK".to_string()),
        ("worker", "hindsight_tag") => Some("AGENT_RUN_HINDSIGHT_TAG".to_string()),
        ("worker", "hindsight_path") => Some("AGENT_RUN_HINDSIGHT_PATH".to_string()),
        ("worker", "hindsight_auto_recall") => Some("AGENT_RUN_HINDSIGHT_AUTO_RECALL".to_string()),
        ("worker", "hindsight_auto_retain") => Some("AGENT_RUN_HINDSIGHT_AUTO_RETAIN".to_string()),
        ("worker", "hindsight_url") => Some("AGENT_RUN_HINDSIGHT_URL".to_string()),
        ("worker", "hindsight_profile") => Some("AGENT_RUN_HINDSIGHT_PROFILE".to_string()),
        ("worker", "remote_control") => Some("AGENT_RUN_REMOTE_CONTROL".to_string()),
        ("worker", "remote_control_autostart") => {
            Some("AGENT_RUN_REMOTE_CONTROL_AUTOSTART".to_string())
        }
        ("worker", "remote_control_check_secs") => {
            Some("AGENT_RUN_REMOTE_CONTROL_CHECK_SECS".to_string())
        }
        ("worker", "default_engine") => Some("AGENT_RUN_DEFAULT_ENGINE".to_string()),
        ("worker", "api_listen") => Some("AGENT_RUN_API_LISTEN".to_string()),
        ("worker", "agy_mcp_source") => Some("AGENT_RUN_AGY_MCP_SOURCE".to_string()),
        ("worker", "a2a_interrupt_min_secs") => {
            Some("AGENT_RUN_A2A_INTERRUPT_MIN_SECS".to_string())
        }
        ("worker", "a2a_ack_secs") => Some("AGENT_RUN_A2A_ACK_SECS".to_string()),
        ("worker", "a2a_force_secs") => Some("AGENT_RUN_A2A_FORCE_SECS".to_string()),
        ("worker", "a2a_inbox_ttl_secs") => Some("AGENT_RUN_A2A_INBOX_TTL_SECS".to_string()),
        ("worker", "a2a_live_stdin") => Some("AGENT_RUN_A2A_LIVE_STDIN".to_string()),
        ("worker", "a2a_live_idle_secs") => Some("AGENT_RUN_A2A_LIVE_IDLE_SECS".to_string()),
        ("worker", "a2a_mcp_enabled") => Some("AGENT_RUN_A2A_MCP_ENABLED".to_string()),

        _ => None,
    }
}

/// Sets the given variables into the process environment ONLY if they are
/// not already set there (the environment always wins). Called before the
/// role starts. `map` is the parsed file map; `sections` selects which TOML
/// sections to apply (`common` plus the role's own section).
pub fn apply_to_env(map: &HashMap<String, String>, sections: &[&str]) -> usize {
    use std::collections::HashSet;
    let allowed: HashSet<&str> = sections.iter().copied().collect();
    let mut applied = 0usize;
    let resolved = resolve_section_filter(map, &allowed);
    for (var, value) in resolved {
        if std::env::var_os(&var).is_none() {
            // SAFETY: apply_to_env runs once, before any other thread of the
            // process may exist (it is called from the binary entry points
            // before the role starts).
            unsafe {
                std::env::set_var(var, value);
            }
            applied += 1;
        }
    }
    applied
}

/// Partitions a parsed file map by section: variables whose key belongs to
/// one of `allowed` sections are returned; every other mapped variable is
/// ignored (the other role's section is validated at parse time but not
/// applied). Returns the (variable, value) pairs of the allowed sections.
fn resolve_section_filter(
    map: &HashMap<String, String>,
    allowed: &std::collections::HashSet<&str>,
) -> Vec<(String, String)> {
    // Section membership is recovered through the invertible key table.
    let mut out = Vec::new();
    for (var, value) in map {
        if let Some((section, _)) = section_and_key_for_var(var) {
            if allowed.contains(section) {
                out.push((var.clone(), value.clone()));
            }
        }
    }
    out.sort();
    out
}

/// The canonical TOML `section` for a mapped variable (the inverse of
/// [`var_for_section_key`]). Returns `None` for variables without a TOML key.
fn section_and_key_for_var(var: &str) -> Option<(&'static str, String)> {
    const SECTIONS: &[&str] = &["common", "orchestrator", "worker"];
    for &section in SECTIONS {
        for key in section_keys(section) {
            if var_for_section_key(section, key) == Some(var.to_string()) {
                return Some((section, key.to_string()));
            }
        }
    }
    None
}

/// All valid TOML keys of one section (used by the inverse mapping).
fn section_keys(section: &str) -> Vec<&'static str> {
    match section {
        "common" => vec![
            "no_proxy",
            "http_proxy",
            "https_proxy",
            "api_token_file",
            "api_allow_unauthenticated",
            "jobset_root_key_file",
        ],
        "orchestrator" => vec![
            "listen",
            "token_file",
            "session_log_ring_bytes",
            "config",
            "workspaces",
            "workspace_root",
            "remote_prefix",
            "default_session",
            "default_session_id",
            "session_args",
            "claude_max_crashes",
            "default_session_engine",
        ],
        "worker" => vec![
            "workspace_root",
            "jobs_dir_name",
            "agy_path",
            "claude_path",
            "default_model",
            "default_effort",
            "default_base_branch",
            "state_root",
            "repos_root",
            "gc_max_age_days",
            "max_parallel",
            "shutdown_grace_secs",
            "launch_timeout_secs",
            "stall_minutes",
            "max_runtime",
            "hindsight_enabled",
            "hindsight_bank",
            "hindsight_tag",
            "hindsight_path",
            "hindsight_auto_recall",
            "hindsight_auto_retain",
            "hindsight_url",
            "hindsight_profile",
            "remote_control",
            "remote_control_autostart",
            "remote_control_check_secs",
            "api_listen",
            "agy_mcp_source",
            "default_engine",
            "a2a_interrupt_min_secs",
            "a2a_ack_secs",
            "a2a_force_secs",
            "a2a_inbox_ttl_secs",
            "a2a_live_stdin",
            "a2a_live_idle_secs",
            "a2a_mcp_enabled",
        ],
        _ => Vec::new(),
    }
}

/// Converts a TOML scalar to the string the env-based readers parse:
/// strings, integers, floats and booleans. Arrays and tables are not
/// configuration scalars here.
fn scalar_to_string(value: &toml::Value) -> Option<String> {
    match value {
        toml::Value::String(s) => Some(s.clone()),
        toml::Value::Integer(i) => Some(i.to_string()),
        toml::Value::Float(f) => Some(f.to_string()),
        toml::Value::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_file() -> &'static str {
        r#"
[common]
http_proxy = "http://proxy.example:3128"

[worker]
max_parallel = 4
hindsight_url = "https://hindsight.example"

[orchestrator]
listen = "0.0.0.0:8002"
claude_max_crashes = 7
"#
    }

    fn parse(s: &str) -> HashMap<String, String> {
        parse_config_toml("test.toml", s).expect("valid file")
    }

    #[test]
    fn test_valid_file_maps_all_three_sections() {
        let vars = parse(base_file());
        assert_eq!(
            vars.get("AGENT_RUN_HTTP_PROXY").map(|s| s.as_str()),
            Some("http://proxy.example:3128")
        );
        assert_eq!(
            vars.get("AGENT_RUN_MAX_PARALLEL").map(|s| s.as_str()),
            Some("4")
        );
        assert_eq!(
            vars.get("AGENT_RUN_HINDSIGHT_URL").map(|s| s.as_str()),
            Some("https://hindsight.example")
        );
        assert_eq!(
            vars.get("AGENT_ORCHESTRATE_LISTEN").map(|s| s.as_str()),
            Some("0.0.0.0:8002")
        );
        assert_eq!(
            vars.get("CLAUDE_MAX_CRASHES").map(|s| s.as_str()),
            Some("7")
        );
    }

    #[test]
    fn test_scalar_rendering_bool_and_number() {
        let toml = r#"
[worker]
max_parallel = 3
hindsight_enabled = true
remote_control = false
gc_max_age_days = 14
"#;
        let vars = parse(toml);
        assert_eq!(
            vars.get("AGENT_RUN_MAX_PARALLEL").map(|s| s.as_str()),
            Some("3")
        );
        assert_eq!(
            vars.get("AGENT_RUN_HINDSIGHT_ENABLED").map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(
            vars.get("AGENT_RUN_REMOTE_CONTROL").map(|s| s.as_str()),
            Some("false")
        );
        assert_eq!(
            vars.get("AGENT_RUN_GC_MAX_AGE_DAYS").map(|s| s.as_str()),
            Some("14")
        );
    }

    #[test]
    fn test_a2a_mcp_enabled_maps_to_env() {
        let toml = r#"
[worker]
a2a_mcp_enabled = true
"#;
        let vars = parse(toml);
        assert_eq!(
            vars.get("AGENT_RUN_A2A_MCP_ENABLED").map(|s| s.as_str()),
            Some("true")
        );
        // Inverse mapping recovers it too (sound and complete over the table).
        let (section, key) = section_and_key_for_var("AGENT_RUN_A2A_MCP_ENABLED").unwrap();
        assert_eq!(section, "worker");
        assert_eq!(key, "a2a_mcp_enabled");
        assert_eq!(
            var_for_section_key("worker", "a2a_mcp_enabled").as_deref(),
            Some("AGENT_RUN_A2A_MCP_ENABLED")
        );
    }

    #[test]
    fn test_a2a_live_keys_map_to_env() {
        let toml = r#"
[worker]
a2a_live_stdin = false
a2a_live_idle_secs = 300
"#;
        let vars = parse(toml);
        assert_eq!(
            vars.get("AGENT_RUN_A2A_LIVE_STDIN").map(|s| s.as_str()),
            Some("false")
        );
        assert_eq!(
            vars.get("AGENT_RUN_A2A_LIVE_IDLE_SECS").map(|s| s.as_str()),
            Some("300")
        );
        // Inverse mapping recovers both keys over the table.
        let (section, key) = section_and_key_for_var("AGENT_RUN_A2A_LIVE_STDIN").unwrap();
        assert_eq!(section, "worker");
        assert_eq!(key, "a2a_live_stdin");
        let (section, key) = section_and_key_for_var("AGENT_RUN_A2A_LIVE_IDLE_SECS").unwrap();
        assert_eq!(section, "worker");
        assert_eq!(key, "a2a_live_idle_secs");
        assert_eq!(
            var_for_section_key("worker", "a2a_live_stdin").as_deref(),
            Some("AGENT_RUN_A2A_LIVE_STDIN")
        );
        assert_eq!(
            var_for_section_key("worker", "a2a_live_idle_secs").as_deref(),
            Some("AGENT_RUN_A2A_LIVE_IDLE_SECS")
        );
    }

    #[test]
    fn test_unknown_key_is_error() {
        let err = parse_config_toml("test.toml", "[worker]\nbogus_key = 1\n").unwrap_err();
        assert!(err.contains("unknown key 'worker.bogus_key'"), "{err}");
        let err2 = parse_config_toml("test.toml", "[common]\nnope = 1\n").unwrap_err();
        assert!(err2.contains("unknown key 'common.nope'"), "{err2}");
    }

    #[test]
    fn test_wrong_section_is_error() {
        // max_parallel belongs to [worker], not [orchestrator].
        let err = parse_config_toml("test.toml", "[orchestrator]\nmax_parallel = 2\n").unwrap_err();
        assert!(
            err.contains("unknown key 'orchestrator.max_parallel'"),
            "{err}"
        );
        // listen belongs to [orchestrator], not [worker].
        let err2 = parse_config_toml("test.toml", "[worker]\nlisten = \"x\"\n").unwrap_err();
        assert!(err2.contains("unknown key 'worker.listen'"), "{err2}");
    }

    #[test]
    fn test_unknown_section_is_error() {
        let err = parse_config_toml("test.toml", "[bogus]\nx = 1\n").unwrap_err();
        assert!(err.contains("unknown key 'bogus.x'"), "{err}");
    }

    #[test]
    fn test_repo_sync_section_is_not_supported_here() {
        // The `repo-sync` subcommand configures through its own file
        // (KUBEADP_REPO_SYNC_CONFIG); a `[repo_sync]` section in this config
        // file must be a hard error, not silently dropped.
        let err = parse_config_toml("test.toml", "[repo_sync]\nrepos = []\n").unwrap_err();
        assert!(err.contains("repo_sync"), "{err}");
        assert!(err.contains("KUBEADP_REPO_SYNC_CONFIG"), "{err}");
    }

    #[test]
    fn test_upper_case_key_is_error() {
        let err = parse_config_toml("test.toml", "[worker]\nMax_parallel = 2\n").unwrap_err();
        assert!(err.contains("must be lower-case"), "{err}");
    }

    #[test]
    fn test_duplicate_key_is_error() {
        // toml 0.8 rejects duplicate keys in one table at parse time.
        let toml = "[worker]\nmax_parallel = 1\nmax_parallel = 2\n";
        let err = parse_config_toml("test.toml", toml).unwrap_err();
        assert!(err.contains("duplicate key"), "{err}");
    }

    #[test]
    fn test_cross_section_duplicate_is_error() {
        // http_proxy is only valid in [common]; stating it twice cannot happen
        // within one section, but two sections must not both define it either.
        let toml = "[common]\nhttp_proxy = \"a\"\n";
        let vars = parse(toml);
        assert_eq!(vars.len(), 1);
        // A duplicate of an existing resolved variable is caught by the map.
        let err = parse_config_toml(
            "test.toml",
            "[common]\nhttp_proxy = \"a\"\n[worker]\nhttp_proxy = \"b\"\n",
        )
        .unwrap_err();
        assert!(err.contains("unknown key 'worker.http_proxy'"), "{err}");
    }

    #[test]
    fn test_inline_secret_token_rejected() {
        let err = parse_config_toml("test.toml", "[orchestrator]\ntoken = \"abc\"\n").unwrap_err();
        assert!(err.contains("must not be set inline"), "{err}");
        assert!(err.contains("token_file"), "{err}");
    }

    #[test]
    fn test_inline_hindsight_api_key_rejected() {
        let err =
            parse_config_toml("test.toml", "[worker]\nhindsight_api_key = \"k\"\n").unwrap_err();
        assert!(err.contains("must not be set inline"), "{err}");
        assert!(err.contains("AGENT_RUN_HINDSIGHT_API_KEY"), "{err}");
    }

    #[test]
    fn test_token_file_form_accepted() {
        let vars = parse("[orchestrator]\ntoken_file = \"/run/secrets/token\"\n");
        assert_eq!(
            vars.get("AGENT_ORCHESTRATE_TOKEN_FILE").map(|s| s.as_str()),
            Some("/run/secrets/token")
        );
    }

    #[test]
    fn test_common_api_token_keys_accepted_and_inlined_rejected() {
        // api_token_file + api_allow_unauthenticated are [common] keys mapped
        // to their KUBEADP_* variables (applied for both roles).
        let vars = parse(
            "[common]\napi_token_file = \"/run/secrets/api-token\"\n\
             api_allow_unauthenticated = \"true\"\n\
             jobset_root_key_file = \"/run/secrets/jobset-root-key\"\n",
        );
        assert_eq!(
            vars.get("KUBEADP_API_TOKEN_FILE").map(|s| s.as_str()),
            Some("/run/secrets/api-token")
        );
        assert_eq!(
            vars.get("KUBEADP_API_ALLOW_UNAUTHENTICATED")
                .map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(
            vars.get("KUBEADP_JOBSET_ROOT_KEY_FILE").map(|s| s.as_str()),
            Some("/run/secrets/jobset-root-key")
        );
        // An inline token value is rejected like every other secret.
        let err = parse_config_toml("test.toml", "[common]\ntoken = \"abc\"\n").unwrap_err();
        assert!(err.contains("must not be set inline"), "{err}");
        assert!(err.contains("KUBEADP_API_TOKEN_FILE"), "{err}");
        // An inline jobset root key value is likewise rejected.
        let err =
            parse_config_toml("test.toml", "[common]\njobset_root_key = \"abc\"\n").unwrap_err();
        assert!(err.contains("must not be set inline"), "{err}");
        assert!(err.contains("KUBEADP_JOBSET_ROOT_KEY_FILE"), "{err}");
        // api_token_file is a scalar (a path); a non-scalar is rejected.
        let err =
            parse_config_toml("test.toml", "[common]\napi_token_file = [\"/a\"]\n").unwrap_err();
        assert!(err.contains("must be a scalar"), "{err}");
        // jobset_root_key_file is likewise a scalar path.
        let err = parse_config_toml("test.toml", "[common]\njobset_root_key_file = [\"/a\"]\n")
            .unwrap_err();
        assert!(err.contains("must be a scalar"), "{err}");
    }

    #[test]
    fn test_non_scalar_value_is_error() {
        let err = parse_config_toml("test.toml", "[worker]\nmax_parallel = [1, 2]\n").unwrap_err();
        assert!(err.contains("must be a scalar"), "{err}");
        let err2 = parse_config_toml("test.toml", "[worker]\nagy_path = { a = 1 }\n").unwrap_err();
        assert!(err2.contains("must be a scalar"), "{err2}");
    }

    #[test]
    fn test_absent_file_means_empty_map() {
        let dir = std::env::temp_dir().join(format!(
            "kubeadp-cfg-absent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let file = dir.join("config.toml");
        // No file anywhere: env-only configuration.
        std::env::set_var("KUBEADP_CONFIG_FILE", file.to_string_lossy().to_string());
        let map = config_file_map().expect("absent file is not an error");
        assert!(map.is_empty());
        std::env::remove_var("KUBEADP_CONFIG_FILE");
    }

    #[test]
    fn test_empty_path_value_is_error() {
        std::env::set_var("KUBEADP_CONFIG_FILE", "   ");
        let err = config_file_map().unwrap_err();
        assert!(err.contains("must not be empty"), "{err}");
        std::env::remove_var("KUBEADP_CONFIG_FILE");
    }

    #[test]
    fn test_apply_to_env_env_wins_and_only_allowed_sections() {
        // Scratch map: an unset variable is applied, a set one keeps its value.
        let vars = parse(base_file());
        std::env::remove_var("AGENT_RUN_MAX_PARALLEL");
        std::env::set_var("AGENT_RUN_HINDSIGHT_URL", "http://env.example/");

        let applied = apply_to_env(&vars, &["common", "worker"]);

        // [worker] max_parallel was unset -> applied.
        assert_eq!(std::env::var("AGENT_RUN_MAX_PARALLEL").unwrap(), "4");
        // [common] http_proxy was unset -> applied.
        assert_eq!(
            std::env::var("AGENT_RUN_HTTP_PROXY").unwrap(),
            "http://proxy.example:3128"
        );
        // env already set -> not overridden.
        assert_eq!(
            std::env::var("AGENT_RUN_HINDSIGHT_URL").unwrap(),
            "http://env.example/"
        );
        // [orchestrator] values are NOT applied when applying common+worker.
        assert!(std::env::var_os("AGENT_ORCHESTRATE_LISTEN").is_none());
        assert!(std::env::var_os("CLAUDE_MAX_CRASHES").is_none());

        // Three variables for common+worker existed in the file:
        // http_proxy, max_parallel, hindsight_url. env set => 2 applied.
        assert_eq!(applied, 2);

        // Now apply the orchestrator section: listen and claude_max_crashes.
        let applied2 = apply_to_env(&vars, &["orchestrator"]);
        assert_eq!(
            std::env::var("AGENT_ORCHESTRATE_LISTEN").unwrap(),
            "0.0.0.0:8002"
        );
        assert_eq!(std::env::var("CLAUDE_MAX_CRASHES").unwrap(), "7");
        assert_eq!(applied2, 2);

        std::env::remove_var("AGENT_RUN_MAX_PARALLEL");
        std::env::remove_var("AGENT_RUN_HTTP_PROXY");
        std::env::remove_var("AGENT_RUN_HINDSIGHT_URL");
        std::env::remove_var("AGENT_ORCHESTRATE_LISTEN");
        std::env::remove_var("CLAUDE_MAX_CRASHES");
    }

    #[test]
    fn test_mapping_table_is_sound_and_complete() {
        // Every key that var_for_section_key accepts must be recoverable by
        // the inverse mapping (no key silently dropped).
        let mut keys = 0;
        for &section in &["common", "orchestrator", "worker"] {
            for key in section_keys(section) {
                let var = var_for_section_key(section, key).unwrap();
                let (s2, k2) = section_and_key_for_var(&var).unwrap();
                assert_eq!(s2, section, "section mismatch for {var}");
                assert_eq!(k2, key, "key mismatch for {var}");
                keys += 1;
            }
        }
        assert!(keys >= 30, "key table unexpectedly small: {keys}");
    }

    #[test]
    fn test_no_env_only_or_test_vars_in_table() {
        // Loader directives and the test hooks must never be TOML keys.
        for &section in &["common", "orchestrator", "worker"] {
            for key in section_keys(section) {
                let var = var_for_section_key(section, key).unwrap();
                assert_ne!(var, "KUBEADP_CONFIG_FILE");
                assert_ne!(var, "KUBEADP_ROLE");
                assert!(!var.contains("TEST"), "{var} should not be a test var");
            }
        }
    }

    #[test]
    fn test_removed_sandbox_keys_rejected() {
        // [common] sandbox and sandbox_runtime were removed in contract v1.2/v1.3
        let toml = "[common]\nsandbox = \"auto\"\n";
        let err = parse_config_toml("test.toml", toml).unwrap_err();
        assert!(err.contains("unknown key 'common.sandbox'"), "{err}");

        let toml2 = "[common]\nsandbox_runtime = \"auto\"\n";
        let err2 = parse_config_toml("test.toml", toml2).unwrap_err();
        assert!(
            err2.contains("unknown key 'common.sandbox_runtime'"),
            "{err2}"
        );
    }

    #[test]
    fn test_engine_default_keys_map_to_env() {
        // HL-91 E1: per-role engine selection defaults (precedence level 3).
        let vars = parse(
            "[worker]\ndefault_engine = \"claude\"\n\
             [orchestrator]\ndefault_session_engine = \"agy\"\n",
        );
        assert_eq!(
            vars.get("AGENT_RUN_DEFAULT_ENGINE").map(|s| s.as_str()),
            Some("claude")
        );
        assert_eq!(
            vars.get("AGENT_ORCHESTRATE_DEFAULT_SESSION_ENGINE")
                .map(|s| s.as_str()),
            Some("agy")
        );
    }

    #[test]
    fn test_claude_path_key_maps_to_env() {
        // HL-91 E5: the worker's claude job engine binary path.
        let vars = parse("[worker]\nclaude_path = \"/opt/claude/bin/claude\"\n");
        assert_eq!(
            vars.get("AGENT_RUN_CLAUDE_PATH").map(|s| s.as_str()),
            Some("/opt/claude/bin/claude")
        );
    }
}

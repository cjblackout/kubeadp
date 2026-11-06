use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const DEFAULT_WORKSPACE_ROOT: &str = "/workspace";
pub const DEFAULT_JOBS_DIR_NAME: &str = ".agent-jobs";
/// Default root of the per-project private bare repositories that host job
/// worktrees: next to the job directories, under the workspace root. The
/// literal `{workspace_root}` is expanded against the resolved workspace
/// root when the config is loaded.
pub const DEFAULT_REPOS_ROOT_TEMPLATE: &str = "{workspace_root}/.agent-repos";
pub const DEFAULT_AGY_PATH: &str = "/usr/local/libexec/agy";
pub const DEFAULT_BASE_BRANCH: &str = "main";
pub const DEFAULT_STATE_ROOT: &str = "~/.agent-run";
pub const DEFAULT_GC_MAX_AGE_DAYS: u64 = 14;
pub const DEFAULT_MAX_PARALLEL: u32 = 3;
pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 25;
pub const DEFAULT_LAUNCH_TIMEOUT_SECS: u64 = 20;
pub const DEFAULT_STALL_MINUTES: u64 = 20;
/// Minimum interval (seconds) between two A2A `SendMessage` interrupts of the
/// SAME job: an incoming message that would interrupt a job whose previous
/// interrupt is younger than this is rejected as rate-limited (in addition to
/// the per-jobset hop-token budget). `0` disables the interval.
pub const DEFAULT_A2A_INTERRUPT_MIN_SECS: u64 = 20;
/// Whether a meshed job is launched in live mode: its `agy` process gets a
/// pipe as stdin (`--print "" --input-format stream-json`) and A2A messages
/// are delivered to the RUNNING job as NDJSON lines over that pipe instead of
/// killing and resuming it. See [`config::a2a_live_stdin`] for the full
/// contract; `false` forces the interrupt-and-resume path for every job.
pub const DEFAULT_A2A_LIVE_STDIN: bool = true;
/// How long (seconds) a live job's stdin stays open after its last delivered
/// message / log write before the watchdog closes it (EOF), letting agy finish
/// the turn and the job go `Done` through the normal classifier.
/// `0` means never close (the channel lives until the job ends on its own).
/// Non-zero values below [`MIN_A2A_LIVE_IDLE_SECS`] are clamped up.
pub const DEFAULT_A2A_LIVE_IDLE_SECS: u64 = 120;
/// Floor for a non-zero [`config::a2a_live_idle_secs`]: a live channel must
/// not be closed after a sub-second idle (agy needs time to flush its
/// response), so values below this are clamped up. `0` (never close) is
/// unaffected.
pub const MIN_A2A_LIVE_IDLE_SECS: u64 = 10;
pub const DEFAULT_HINDSIGHT_PATH: &str = "hindsight";
pub const DEFAULT_REMOTE_CONTROL_CHECK_SECS: u64 = 60;
pub const MIN_REMOTE_CONTROL_CHECK_SECS: u64 = 10;

/// Serialized representation of `agent-run` configuration file (e.g. `config.toml`).
///
/// Lives on the agent-run-api server (default `<state_root>/config.toml`, overridable via
/// `AGENT_RUN_CONFIG`). Managed via `agent-run init` / env vars on the deployment.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ConfigFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jobs_dir_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agy_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_base_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repos_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_max_age_days: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_parallel: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shutdown_grace_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launch_timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stall_minutes: Option<u64>,
    /// Server-wide default max runtime for jobs (same duration syntax as
    /// `wait --timeout`). UNSET BY DEFAULT: jobs run unbounded unless a
    /// per-job `--max-runtime` or this setting provides a limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_runtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_bank: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_auto_recall: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_auto_retain: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hindsight_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub https_proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_control: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_control_autostart: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_control_check_secs: Option<u64>,
    /// Default engine id for jobs (read by the worker role).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_engine: Option<String>,
    /// Default engine id for sessions (read by the orchestrator role).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_session_engine: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a2a_interrupt_min_secs: Option<u64>,
    /// Whether jobs that are members of an A2A mesh (jobset) are launched in
    /// live mode with a stdin pipe (see [`DEFAULT_A2A_LIVE_STDIN`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a2a_live_stdin: Option<bool>,
    /// Idle (seconds) after which a live job's stdin pipe is closed
    /// (see [`DEFAULT_A2A_LIVE_IDLE_SECS`]); `0` = never close.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a2a_live_idle_secs: Option<u64>,
}

/// Fully resolved, layered runtime configuration for the agent-run engine.
///
/// Precedence: environment variables (`AGENT_RUN_*`) > TOML config file > built-in defaults.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub config_path: Option<PathBuf>,
    pub workspace_root: PathBuf,
    pub jobs_dir_name: String,
    pub agy_path: PathBuf,
    pub default_model: Option<String>,
    pub default_effort: Option<String>,
    pub default_base_branch: String,
    pub state_root: PathBuf,
    /// Root of the per-project private bare repositories that own job
    /// worktrees (`<repos_root>/<project>.git`). Job worktrees are created
    /// from these repositories -- never from the git-sync-owned project
    /// checkout -- so a git-sync repo re-init cannot orphan them.
    /// Overridable via `AGENT_RUN_REPOS_ROOT`; defaults to
    /// `<workspace_root>/.agent-repos`.
    pub repos_root: PathBuf,
    /// Optional override of the home directory that holds agy's own state
    /// (`~/.gemini/antigravity-cli`). `None` (the production default) resolves
    /// the home directory at use time; set in tests to point conversation-id
    /// recovery at a temp dir without touching the process environment.
    pub agy_home: Option<PathBuf>,
    /// Path granted rw to the sandboxed agent (the policy's `shared_rw`
    /// entry, default `/home/agent`). The default is the production layout.
    /// Overridable so tests can point it at a creatable temp dir when the
    /// sandbox backend fail-closes on a writable path it cannot create.
    pub sandbox_home: PathBuf,
    pub gc_max_age_days: u64,
    pub max_parallel: u32,
    pub shutdown_grace_secs: u64,
    /// How long `start`/`retry`/`review` wait for the job monitor to confirm
    /// the spawned agy's PID before declaring the launch failed (seconds).
    /// A launch declared failed cancels its monitor, which kills the
    /// just-spawned agy (or never spawns it), so a failed launch never leaves
    /// a running agy behind. Invalid or non-positive values of
    /// `AGENT_RUN_LAUNCH_TIMEOUT_SECS` fall back to the file/default value
    /// with a warning (unlike the fail-loud `AGENT_RUN_GC_MAX_AGE_DAYS` et al).
    pub launch_timeout_secs: u64,
    /// Minutes of silence (no newer file mtime and no live child process)
    /// before a running job is flagged `possibly_stalled` by
    /// `status`/`list`. Display-only: never changes job state. Invalid or
    /// non-positive values of `AGENT_RUN_STALL_MINUTES` fall back with a
    /// warning (same pattern as `launch_timeout_secs`).
    pub stall_minutes: u64,
    /// Server-wide DEFAULT max job runtime (same syntax as `wait --timeout`),
    /// from `AGENT_RUN_MAX_RUNTIME` or config.toml `max_runtime`. UNSET BY
    /// DEFAULT: `None` means no default limit -- jobs run unbounded unless
    /// a per-job `--max-runtime` (which always wins) or this default applies.
    pub max_runtime: Option<chrono::Duration>,
    pub hindsight_enabled: bool,
    pub hindsight_bank: Option<String>,
    pub hindsight_tag: Option<String>,
    pub hindsight_path: PathBuf,
    pub hindsight_auto_recall: bool,
    pub hindsight_auto_retain: bool,
    pub hindsight_url: Option<String>,
    pub hindsight_api_key: Option<String>,
    pub hindsight_profile: Option<String>,
    pub http_proxy: Option<String>,
    pub https_proxy: Option<String>,
    pub no_proxy: Option<String>,
    pub remote_control: bool,
    pub remote_control_autostart: bool,
    pub remote_control_check_secs: u64,
    /// Default engine id for jobs (engine selection precedence level 3,
    /// per-role fallback; unused until E3 routes jobs behind the trait).
    pub default_engine: Option<String>,
    /// Default engine id for sessions (same precedence; unused until E2/E5).
    pub default_session_engine: Option<String>,
    /// Minimum seconds between two A2A `SendMessage` interrupts of the same
    /// job (see [`DEFAULT_A2A_INTERRUPT_MIN_SECS`]); `0` disables the interval.
    pub a2a_interrupt_min_secs: u64,
    /// Whether a job that belongs to an A2A mesh is launched in live mode: its
    /// `agy` process reads A2A messages from its stdin pipe (`--print ""
    /// --input-format stream-json`) and an inbound message is delivered to the
    /// RUNNING process as an NDJSON line instead of killing and resuming it.
    /// When this is on, a job is live-eligible only when the worker is itself
    /// a mesh member (its host is a jobset member) or the job was explicitly
    /// started into a jobset (`agent-run start --jobset`). `false` forces the
    /// interrupt-and-resume path for every job.
    pub a2a_live_stdin: bool,
    /// How long (seconds) a live job's stdin stays open after its last
    /// delivered message / job-log write before the watchdog closes it (EOF),
    /// letting agy finish the turn and the job go `Done` through the normal
    /// classifier. `0` means never close. Non-zero values below
    /// [`MIN_A2A_LIVE_IDLE_SECS`] are clamped up.
    pub a2a_live_idle_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        let workspace_root = PathBuf::from(DEFAULT_WORKSPACE_ROOT);
        let state_root = expand_path(DEFAULT_STATE_ROOT, &workspace_root);
        Self {
            config_path: default_config_file_path(),
            workspace_root: workspace_root.clone(),
            jobs_dir_name: DEFAULT_JOBS_DIR_NAME.to_string(),
            agy_path: PathBuf::from(DEFAULT_AGY_PATH),
            default_model: None,
            default_effort: None,
            default_base_branch: DEFAULT_BASE_BRANCH.to_string(),
            state_root,
            repos_root: expand_path(DEFAULT_REPOS_ROOT_TEMPLATE, &workspace_root),
            agy_home: None,
            sandbox_home: PathBuf::from("/home/agent"),
            gc_max_age_days: DEFAULT_GC_MAX_AGE_DAYS,
            max_parallel: DEFAULT_MAX_PARALLEL,
            shutdown_grace_secs: DEFAULT_SHUTDOWN_GRACE_SECS,
            launch_timeout_secs: DEFAULT_LAUNCH_TIMEOUT_SECS,
            stall_minutes: DEFAULT_STALL_MINUTES,
            max_runtime: None,
            hindsight_enabled: false,
            hindsight_bank: None,
            hindsight_tag: None,
            hindsight_path: PathBuf::from(DEFAULT_HINDSIGHT_PATH),
            hindsight_auto_recall: false,
            hindsight_auto_retain: false,
            hindsight_url: None,
            hindsight_api_key: None,
            hindsight_profile: None,
            http_proxy: None,
            https_proxy: None,
            no_proxy: None,
            remote_control: false,
            remote_control_autostart: false,
            remote_control_check_secs: DEFAULT_REMOTE_CONTROL_CHECK_SECS,
            default_engine: None,
            default_session_engine: None,
            a2a_interrupt_min_secs: DEFAULT_A2A_INTERRUPT_MIN_SECS,
            a2a_live_stdin: DEFAULT_A2A_LIVE_STDIN,
            a2a_live_idle_secs: DEFAULT_A2A_LIVE_IDLE_SECS,
        }
    }
}

impl Config {
    /// Loads configuration respecting the precedence hierarchy:
    /// Environment variables (`AGENT_RUN_*`) > TOML config file > Built-in defaults.
    pub fn load() -> Result<Self> {
        let env_vars: HashMap<String, String> = std::env::vars().collect();
        Self::load_with_env(&env_vars)
    }

    /// Resolves the config file path respecting `AGENT_RUN_CONFIG` > default path.
    pub fn resolve_config_path(env_vars: &HashMap<String, String>) -> Option<PathBuf> {
        env_vars
            .get("AGENT_RUN_CONFIG")
            .map(|s| expand_path(s, Path::new(DEFAULT_WORKSPACE_ROOT)))
            .or_else(default_config_file_path)
    }

    /// Internal loader accepting explicit environment map (for testing and isolation).
    pub fn load_with_env(env_vars: &HashMap<String, String>) -> Result<Self> {
        // 1. Determine config file path
        let config_path: Option<PathBuf> = Self::resolve_config_path(env_vars);

        // 2. Read config file if exists
        let file_cfg = if let Some(path) = &config_path {
            if path.exists() {
                let content = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read config file at {}", path.display()))?;
                match toml::from_str::<ConfigFile>(&content) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        anyhow::bail!(
                            "Failed to parse TOML config file at {}: {}",
                            path.display(),
                            e
                        );
                    }
                }
            } else {
                ConfigFile::default()
            }
        } else {
            ConfigFile::default()
        };

        // 3. Resolve workspace_root first (since state_root template can reference it)
        let workspace_root_raw = env_vars
            .get("AGENT_RUN_WORKSPACE_ROOT")
            .cloned()
            .or_else(|| file_cfg.workspace_root.clone())
            .unwrap_or_else(|| DEFAULT_WORKSPACE_ROOT.to_string());
        let workspace_root = expand_path(&workspace_root_raw, Path::new(DEFAULT_WORKSPACE_ROOT));

        // 4. Resolve jobs_dir_name
        let jobs_dir_name_raw = env_vars
            .get("AGENT_RUN_JOBS_DIR_NAME")
            .cloned()
            .or_else(|| file_cfg.jobs_dir_name.clone());
        let jobs_dir_name = jobs_dir_name_raw
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_JOBS_DIR_NAME.to_string());

        // 5. Resolve binary paths
        let agy_path_raw = env_vars
            .get("AGENT_RUN_AGY_PATH")
            .cloned()
            .or_else(|| file_cfg.agy_path.clone())
            .unwrap_or_else(|| DEFAULT_AGY_PATH.to_string());
        let agy_path = expand_path(&agy_path_raw, &workspace_root);

        // 6. Resolve model and effort
        let default_model_raw = env_vars
            .get("AGENT_RUN_DEFAULT_MODEL")
            .cloned()
            .or_else(|| file_cfg.default_model.clone());
        let default_model = default_model_raw.and_then(|m| {
            let trimmed = m.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let default_effort_raw = env_vars
            .get("AGENT_RUN_DEFAULT_EFFORT")
            .cloned()
            .or_else(|| file_cfg.default_effort.clone());
        let default_effort = default_effort_raw.and_then(|e| {
            let trimmed = e.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        // 7. Resolve default_base_branch
        let default_base_branch_raw = env_vars
            .get("AGENT_RUN_DEFAULT_BASE_BRANCH")
            .cloned()
            .or_else(|| file_cfg.default_base_branch.clone());
        let default_base_branch = default_base_branch_raw
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_BASE_BRANCH.to_string());

        // 8. Resolve state_root (config file, state database, and logs all live under it)
        let state_root_raw = env_vars
            .get("AGENT_RUN_STATE_ROOT")
            .cloned()
            .or_else(|| file_cfg.state_root.clone())
            .unwrap_or_else(|| DEFAULT_STATE_ROOT.to_string());
        let state_root = expand_path(&state_root_raw, &workspace_root);

        // 8b. Resolve repos_root (root of the per-project private bare
        // repositories that own job worktrees). Like state_root it may use
        // the `{workspace_root}` placeholder.
        let repos_root_raw = env_vars
            .get("AGENT_RUN_REPOS_ROOT")
            .cloned()
            .or_else(|| file_cfg.repos_root.clone());
        let repos_root = match repos_root_raw {
            Some(raw) if !raw.trim().is_empty() => expand_path(raw.trim(), &workspace_root),
            _ => expand_path(DEFAULT_REPOS_ROOT_TEMPLATE, &workspace_root),
        };

        // 9. Resolve gc_max_age_days
        let gc_max_age_days = if let Some(env_val) = env_vars.get("AGENT_RUN_GC_MAX_AGE_DAYS") {
            let clean = env_val.trim();
            clean.parse::<u64>().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid value for AGENT_RUN_GC_MAX_AGE_DAYS: '{}' (expected a positive integer)",
                    env_val
                )
            })?
        } else {
            file_cfg.gc_max_age_days.unwrap_or(DEFAULT_GC_MAX_AGE_DAYS)
        };

        // 10. Resolve max_parallel
        let max_parallel = if let Some(env_val) = env_vars.get("AGENT_RUN_MAX_PARALLEL") {
            let clean = env_val.trim();
            clean.parse::<u32>().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid value for AGENT_RUN_MAX_PARALLEL: '{}' (expected a positive integer)",
                    env_val
                )
            })?
        } else {
            file_cfg.max_parallel.unwrap_or(DEFAULT_MAX_PARALLEL)
        };

        // 10b. Resolve shutdown_grace_secs
        let shutdown_grace_secs = if let Some(env_val) = env_vars.get("AGENT_RUN_SHUTDOWN_GRACE") {
            let clean = env_val.trim();
            clean.parse::<u64>().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid value for AGENT_RUN_SHUTDOWN_GRACE: '{}' (expected a non-negative integer)",
                    env_val
                )
            })?
        } else {
            file_cfg
                .shutdown_grace_secs
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECS)
        };

        // 10c. Resolve launch_timeout_secs. Unlike the fail-loud numeric
        // settings above, an invalid or non-positive value here falls back to
        // the file/default with a warning: a mistyped launch timeout must not
        // be able to make every `agent-run start` fail during a deploy.
        let launch_timeout_secs = if let Some(env_val) = env_vars
            .get("AGENT_RUN_LAUNCH_TIMEOUT_SECS")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            match env_val.parse::<u64>() {
                Ok(secs) if secs > 0 => secs,
                _ => {
                    eprintln!(
                        "agent-run: ignoring invalid AGENT_RUN_LAUNCH_TIMEOUT_SECS value {env_val:?}; using the default"
                    );
                    file_cfg
                        .launch_timeout_secs
                        .unwrap_or(DEFAULT_LAUNCH_TIMEOUT_SECS)
                }
            }
        } else {
            file_cfg
                .launch_timeout_secs
                .unwrap_or(DEFAULT_LAUNCH_TIMEOUT_SECS)
        };

        // 10d. Resolve stall_minutes (same warn-and-fallback pattern as
        // launch_timeout_secs above: a mistyped stall threshold must not
        // break `status`/`list`/`info`).
        let stall_minutes = if let Some(env_val) = env_vars
            .get("AGENT_RUN_STALL_MINUTES")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            match env_val.parse::<u64>() {
                Ok(mins) if mins >= 1 => mins,
                _ => {
                    eprintln!(
                        "agent-run: ignoring invalid AGENT_RUN_STALL_MINUTES value {env_val:?}; using the default"
                    );
                    file_cfg.stall_minutes.unwrap_or(DEFAULT_STALL_MINUTES)
                }
            }
        } else {
            file_cfg.stall_minutes.unwrap_or(DEFAULT_STALL_MINUTES)
        };

        // 10e. Resolve a2a_interrupt_min_secs: the per-job minimum interval
        // between two A2A `SendMessage` interrupts. `0` (a deliberate
        // disabling of the interval) is valid; anything unparseable falls back
        // with a warning, so a mistyped value cannot disable it or make the
        // worker refuse to start.
        let a2a_interrupt_min_secs = if let Some(env_val) = env_vars
            .get("AGENT_RUN_A2A_INTERRUPT_MIN_SECS")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            match env_val.parse::<u64>() {
                Ok(secs) => secs,
                _ => {
                    eprintln!(
                        "agent-run: ignoring invalid AGENT_RUN_A2A_INTERRUPT_MIN_SECS value {env_val:?}; using the default"
                    );
                    file_cfg
                        .a2a_interrupt_min_secs
                        .unwrap_or(DEFAULT_A2A_INTERRUPT_MIN_SECS)
                }
            }
        } else {
            file_cfg
                .a2a_interrupt_min_secs
                .unwrap_or(DEFAULT_A2A_INTERRUPT_MIN_SECS)
        };

        // 10e2. Resolve a2a_live_stdin: whether a meshed job is launched in
        // live mode (stdin pipe for A2A messages) instead of always using the
        // interrupt-and-resume path. Boolean env values parse exactly like the
        // other boolean settings; an unparseable value fails loud (a mistyped
        // flag defaulting one way could silently change inter-A2A delivery
        // behaviour either direction, so there is no silent fallback).
        let a2a_live_stdin = if let Some(env_val) = env_vars.get("AGENT_RUN_A2A_LIVE_STDIN") {
            parse_bool_env("AGENT_RUN_A2A_LIVE_STDIN", env_val)?
        } else {
            file_cfg.a2a_live_stdin.unwrap_or(DEFAULT_A2A_LIVE_STDIN)
        };

        // 10e3. Resolve a2a_live_idle_secs: how long a live job's stdin stays
        // open after its last activity before the watchdog closes it (EOF).
        // `0` (never close) is valid; non-zero values below the floor are
        // clamped up so a live channel is never closed before agy can flush
        // its current turn; anything unparseable falls back with a warning.
        let a2a_live_idle_secs = if let Some(env_val) = env_vars
            .get("AGENT_RUN_A2A_LIVE_IDLE_SECS")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            match env_val.parse::<u64>() {
                Ok(0) => 0,
                Ok(secs) => secs.max(MIN_A2A_LIVE_IDLE_SECS),
                _ => {
                    eprintln!(
                        "agent-run: ignoring invalid AGENT_RUN_A2A_LIVE_IDLE_SECS value {env_val:?}; using the default"
                    );
                    let v = file_cfg
                        .a2a_live_idle_secs
                        .unwrap_or(DEFAULT_A2A_LIVE_IDLE_SECS);
                    if v == 0 {
                        0
                    } else {
                        v.max(MIN_A2A_LIVE_IDLE_SECS)
                    }
                }
            }
        } else {
            let v = file_cfg
                .a2a_live_idle_secs
                .unwrap_or(DEFAULT_A2A_LIVE_IDLE_SECS);
            if v == 0 {
                0
            } else {
                v.max(MIN_A2A_LIVE_IDLE_SECS)
            }
        };

        // 10f. Resolve max_runtime (the server-wide DEFAULT job runtime).
        // UNSET BY DEFAULT: absent/empty means no default limit, jobs run
        // unbounded -- unlike the settings above there is NO fallback value,
        // so `None` survives all the way down. An invalid non-empty value is
        // a loud error: it only affects newly started jobs, and a mistyped
        // limit silently disabling enforcement would be worse.
        let max_runtime = match env_vars
            .get("AGENT_RUN_MAX_RUNTIME")
            .cloned()
            .or_else(|| file_cfg.max_runtime.clone())
        {
            Some(raw) if !raw.trim().is_empty() => {
                Some(
                    crate::core::commands::gc::parse_duration_string(raw.trim()).with_context(|| {
                        format!(
                            "Invalid max_runtime value '{}': expected a duration like '30m', '2h', or '14d'",
                            raw
                        )
                    })?,
                )
            }
            _ => None,
        };

        // 11. Resolve hindsight settings
        let hindsight_enabled_env = env_vars
            .get("AGENT_RUN_HINDSIGHT")
            .or_else(|| env_vars.get("AGENT_RUN_HINDSIGHT_ENABLED"));
        let mut hindsight_enabled = if let Some(env_val) = hindsight_enabled_env {
            parse_bool_env("AGENT_RUN_HINDSIGHT", env_val)?
        } else {
            file_cfg.hindsight_enabled.unwrap_or(false)
        };

        let hindsight_bank_raw = env_vars
            .get("AGENT_RUN_HINDSIGHT_BANK")
            .cloned()
            .or_else(|| file_cfg.hindsight_bank.clone());
        let hindsight_bank = hindsight_bank_raw.and_then(|b| {
            let trimmed = b.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let hindsight_tag_raw = env_vars
            .get("AGENT_RUN_HINDSIGHT_TAG")
            .cloned()
            .or_else(|| file_cfg.hindsight_tag.clone());
        let hindsight_tag = hindsight_tag_raw.and_then(|t| {
            let trimmed = t.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let hindsight_path_raw = env_vars
            .get("AGENT_RUN_HINDSIGHT_PATH")
            .cloned()
            .or_else(|| file_cfg.hindsight_path.clone())
            .unwrap_or_else(|| DEFAULT_HINDSIGHT_PATH.to_string());
        let hindsight_path = expand_path(&hindsight_path_raw, &workspace_root);

        let hindsight_auto_recall =
            if let Some(env_val) = env_vars.get("AGENT_RUN_HINDSIGHT_AUTO_RECALL") {
                parse_bool_env("AGENT_RUN_HINDSIGHT_AUTO_RECALL", env_val)?
            } else {
                file_cfg.hindsight_auto_recall.unwrap_or(false)
            };

        let hindsight_auto_retain =
            if let Some(env_val) = env_vars.get("AGENT_RUN_HINDSIGHT_AUTO_RETAIN") {
                parse_bool_env("AGENT_RUN_HINDSIGHT_AUTO_RETAIN", env_val)?
            } else {
                file_cfg.hindsight_auto_retain.unwrap_or(false)
            };

        // If auto_recall or auto_retain is enabled while hindsight is disabled,
        // hindsight is implied enabled.
        if hindsight_auto_recall || hindsight_auto_retain {
            hindsight_enabled = true;
        }

        let hindsight_url_raw = env_vars
            .get("AGENT_RUN_HINDSIGHT_URL")
            .cloned()
            .or_else(|| file_cfg.hindsight_url.clone());
        let hindsight_url = hindsight_url_raw.and_then(|u| {
            let trimmed = u.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let hindsight_api_key_raw = env_vars
            .get("AGENT_RUN_HINDSIGHT_API_KEY")
            .cloned()
            .or_else(|| file_cfg.hindsight_api_key.clone());
        let hindsight_api_key = hindsight_api_key_raw.and_then(|k| {
            let trimmed = k.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let hindsight_profile_raw = env_vars
            .get("AGENT_RUN_HINDSIGHT_PROFILE")
            .cloned()
            .or_else(|| file_cfg.hindsight_profile.clone());
        let hindsight_profile = hindsight_profile_raw.and_then(|p| {
            let trimmed = p.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        // 12. Resolve proxy settings:
        // Precedence: env AGENT_RUN_*_PROXY > TOML *_proxy > standard env *_PROXY / *_proxy (case-insensitive)
        let http_proxy_raw = env_vars
            .get("AGENT_RUN_HTTP_PROXY")
            .cloned()
            .or_else(|| file_cfg.http_proxy.clone())
            .or_else(|| get_env_case_insensitive(env_vars, "http_proxy"));
        let http_proxy = http_proxy_raw.and_then(|s| {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let https_proxy_raw = env_vars
            .get("AGENT_RUN_HTTPS_PROXY")
            .cloned()
            .or_else(|| file_cfg.https_proxy.clone())
            .or_else(|| get_env_case_insensitive(env_vars, "https_proxy"));
        let https_proxy = https_proxy_raw.and_then(|s| {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let no_proxy_raw = env_vars
            .get("AGENT_RUN_NO_PROXY")
            .cloned()
            .or_else(|| file_cfg.no_proxy.clone())
            .or_else(|| get_env_case_insensitive(env_vars, "no_proxy"));
        let no_proxy = no_proxy_raw.and_then(|s| {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        // 13. Resolve remote_control
        let remote_control = if let Some(env_val) = env_vars.get("AGENT_RUN_REMOTE_CONTROL") {
            parse_bool_env("AGENT_RUN_REMOTE_CONTROL", env_val)?
        } else {
            file_cfg.remote_control.unwrap_or(false)
        };

        // 13b. Resolve remote_control_autostart
        let remote_control_autostart =
            if let Some(env_val) = env_vars.get("AGENT_RUN_REMOTE_CONTROL_AUTOSTART") {
                parse_bool_env("AGENT_RUN_REMOTE_CONTROL_AUTOSTART", env_val)?
            } else {
                file_cfg.remote_control_autostart.unwrap_or(false)
            };

        // 13c. Resolve remote_control_check_secs
        let remote_control_check_secs = if let Some(env_val) = env_vars
            .get("AGENT_RUN_REMOTE_CONTROL_CHECK_SECS")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            match env_val.parse::<u64>() {
                Ok(secs) if secs >= MIN_REMOTE_CONTROL_CHECK_SECS => secs,
                Ok(_secs) => {
                    eprintln!(
                        "agent-run: AGENT_RUN_REMOTE_CONTROL_CHECK_SECS value is below minimum {MIN_REMOTE_CONTROL_CHECK_SECS}; clamping to {MIN_REMOTE_CONTROL_CHECK_SECS}"
                    );
                    MIN_REMOTE_CONTROL_CHECK_SECS
                }
                Err(_) => {
                    eprintln!(
                        "agent-run: ignoring invalid AGENT_RUN_REMOTE_CONTROL_CHECK_SECS value {env_val:?}; using the default"
                    );
                    file_cfg
                        .remote_control_check_secs
                        .unwrap_or(DEFAULT_REMOTE_CONTROL_CHECK_SECS)
                        .max(MIN_REMOTE_CONTROL_CHECK_SECS)
                }
            }
        } else {
            file_cfg
                .remote_control_check_secs
                .unwrap_or(DEFAULT_REMOTE_CONTROL_CHECK_SECS)
                .max(MIN_REMOTE_CONTROL_CHECK_SECS)
        };

        // 13d. Resolve engine selection defaults (HL-91 E1). These feed the
        // engine registry selection precedence level 3 (per-role default)
        // once E3/E5 route jobs/sessions behind the engine trait; until then
        // they are unused configuration. Empty strings resolve to None.
        let default_engine = env_vars
            .get("AGENT_RUN_DEFAULT_ENGINE")
            .cloned()
            .or_else(|| file_cfg.default_engine.clone())
            .and_then(|s| {
                let trimmed = s.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            });

        let default_session_engine = env_vars
            .get("AGENT_ORCHESTRATE_DEFAULT_SESSION_ENGINE")
            .cloned()
            .or_else(|| file_cfg.default_session_engine.clone())
            .and_then(|s| {
                let trimmed = s.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            });

        // Ensure the state directory and SQLite state database exist so that any
        // command can auto-create the layout on first use (init is not required).
        crate::core::job::ensure_state_store(&state_root)?;

        Ok(Config {
            config_path,
            workspace_root,
            jobs_dir_name,
            agy_path,
            default_model,
            default_effort,
            default_base_branch,
            state_root,
            repos_root,
            // No config/env knob exists for this yet: production always
            // resolves the home directory at use time (dirs::home_dir), so
            // the override stays None outside of tests.
            agy_home: None,
            sandbox_home: PathBuf::from("/home/agent"),
            gc_max_age_days,
            max_parallel,
            shutdown_grace_secs,
            launch_timeout_secs,
            stall_minutes,
            max_runtime,
            hindsight_enabled,
            hindsight_bank,
            hindsight_tag,
            hindsight_path,
            hindsight_auto_recall,
            hindsight_auto_retain,
            hindsight_url,
            hindsight_api_key,
            hindsight_profile,
            http_proxy,
            https_proxy,
            no_proxy,
            remote_control,
            remote_control_autostart,
            remote_control_check_secs,
            default_engine,
            default_session_engine,
            a2a_interrupt_min_secs,
            a2a_live_stdin,
            a2a_live_idle_secs,
        })
    }
}

/// Atomically writes content to a target file via a temporary file and rename.
pub fn atomic_write(path: &Path, content: impl AsRef<[u8]>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent directory for {}", path.display()))?;
    }
    let rand_suffix: u32 = rand::random();
    let tmp_path = path.with_extension(format!("tmp.{}.{}", std::process::id(), rand_suffix));
    std::fs::write(&tmp_path, content.as_ref())
        .with_context(|| format!("Failed to write temporary file at {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "Failed to rename {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

/// Parses boolean environment variable values strictly, rejecting invalid strings with an error.
fn parse_bool_env(var_name: &str, val: &str) -> Result<bool> {
    let clean = val.trim();
    if clean.is_empty() {
        return Ok(false);
    }
    match clean.to_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => anyhow::bail!(
            "Invalid boolean value for environment variable {}: '{}' (expected 'true', 'false', '1', or '0')",
            var_name,
            other
        ),
    }
}

/// Case-insensitive environment variable lookup in the given map.
fn get_env_case_insensitive(env_vars: &HashMap<String, String>, key: &str) -> Option<String> {
    for (k, v) in env_vars {
        if k.eq_ignore_ascii_case(key) {
            return Some(v.clone());
        }
    }
    None
}

/// Expands `~` to home directory, and replaces `{workspace_root}` placeholder with `workspace_root`.
pub fn expand_path(raw: &str, workspace_root: &Path) -> PathBuf {
    let replaced = raw.replace("{workspace_root}", &workspace_root.to_string_lossy());
    if replaced.starts_with("~/") || replaced == "~" {
        if let Some(home) = dirs::home_dir() {
            if replaced == "~" {
                return home;
            }
            return home.join(&replaced[2..]);
        }
    }
    PathBuf::from(replaced)
}

/// Returns the standard config file path `~/.agent-run/config.toml` (under the default state root).
pub fn default_config_file_path() -> Option<PathBuf> {
    default_state_root().map(|dir| dir.join("config.toml"))
}

/// Returns the default state root directory `~/.agent-run`.
pub fn default_state_root() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join(".agent-run"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn env_with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_default_config() {
        let env = HashMap::new();
        // Isolate from any real ~/.agent-run/config.toml on the test host.
        let cfg = Config::load_with_env(&env).expect("load defaults");

        assert_eq!(cfg.workspace_root, PathBuf::from("/workspace"));
        assert_eq!(cfg.jobs_dir_name, ".agent-jobs");
        assert_eq!(cfg.agy_path, PathBuf::from("/usr/local/libexec/agy"));
        assert_eq!(cfg.default_base_branch, "main");
        assert_eq!(cfg.gc_max_age_days, 14);
        assert_eq!(cfg.max_parallel, 3);
        assert_eq!(cfg.shutdown_grace_secs, 25);
        assert_eq!(cfg.launch_timeout_secs, DEFAULT_LAUNCH_TIMEOUT_SECS);
        assert_eq!(cfg.stall_minutes, DEFAULT_STALL_MINUTES);
        assert!(!cfg.hindsight_enabled);
        assert_eq!(cfg.hindsight_bank, None);
        assert_eq!(cfg.hindsight_path, PathBuf::from("hindsight"));
        assert_eq!(cfg.a2a_live_stdin, DEFAULT_A2A_LIVE_STDIN);
        assert_eq!(cfg.a2a_live_idle_secs, DEFAULT_A2A_LIVE_IDLE_SECS);
    }

    #[test]
    fn test_a2a_live_keys_env_and_fallback() {
        // Env booleans parse like the other boolean settings.
        let env = env_with(&[("AGENT_RUN_A2A_LIVE_STDIN", "false")]);
        let cfg = Config::load_with_env(&env).expect("load bool env");
        assert!(!cfg.a2a_live_stdin);

        // Invalid boolean fails loud (a mistyped default must not silently
        // flip either way).
        let env = env_with(&[("AGENT_RUN_A2A_LIVE_STDIN", "maybe")]);
        assert!(Config::load_with_env(&env).is_err());

        // Idle: 0 = never close; a non-zero sub-floor value is clamped up.
        let env = env_with(&[("AGENT_RUN_A2A_LIVE_IDLE_SECS", "0")]);
        let cfg = Config::load_with_env(&env).expect("load idle 0");
        assert_eq!(cfg.a2a_live_idle_secs, 0);

        let env = env_with(&[("AGENT_RUN_A2A_LIVE_IDLE_SECS", "1")]);
        let cfg = Config::load_with_env(&env).expect("load idle 1");
        assert_eq!(cfg.a2a_live_idle_secs, MIN_A2A_LIVE_IDLE_SECS);

        let env = env_with(&[("AGENT_RUN_A2A_LIVE_IDLE_SECS", "333")]);
        let cfg = Config::load_with_env(&env).expect("load idle 333");
        assert_eq!(cfg.a2a_live_idle_secs, 333);

        // Unparseable idle falls back to the default (not fail-loud): a
        // mistyped idle must not refuse to start the worker.
        let env = env_with(&[("AGENT_RUN_A2A_LIVE_IDLE_SECS", "soon")]);
        let cfg = Config::load_with_env(&env).expect("load invalid idle");
        assert_eq!(cfg.a2a_live_idle_secs, DEFAULT_A2A_LIVE_IDLE_SECS);
    }

    #[test]
    fn test_layering_precedence_env_over_file_over_default() {
        let temp_dir = tempdir().unwrap();
        let config_file_path = temp_dir.path().join("config.toml");

        // 1. Write config file with base settings
        let toml_content = r#"
workspace_root = "/workspace/file"
jobs_dir_name = ".file-jobs"
default_base_branch = "file-branch"
gc_max_age_days = 30
shutdown_grace_secs = 30
launch_timeout_secs = 4
"#;
        fs::write(&config_file_path, toml_content).unwrap();

        // 2. Set environment variables that override some fields (and point at the file)
        let env = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("AGENT_RUN_WORKSPACE_ROOT", "/workspace/env"),
            ("AGENT_RUN_GC_MAX_AGE_DAYS", "20"),
            ("AGENT_RUN_MAX_PARALLEL", "7"),
            ("AGENT_RUN_SHUTDOWN_GRACE", "40"),
            ("AGENT_RUN_LAUNCH_TIMEOUT_SECS", "7"),
        ]);

        let cfg = Config::load_with_env(&env).expect("load layered config");

        // ENV wins over FILE
        assert_eq!(cfg.workspace_root, PathBuf::from("/workspace/env"));
        assert_eq!(cfg.gc_max_age_days, 20);
        assert_eq!(cfg.max_parallel, 7);
        assert_eq!(cfg.shutdown_grace_secs, 40);
        assert_eq!(cfg.launch_timeout_secs, 7);

        // FILE wins over DEFAULT (when no env variable is set)
        let file_only_env = env_with(&[("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap())]);
        let cfg = Config::load_with_env(&file_only_env).expect("load file-only config");
        assert_eq!(cfg.launch_timeout_secs, 4);

        // FILE wins over DEFAULT
        assert_eq!(cfg.jobs_dir_name, ".file-jobs");
        assert_eq!(cfg.default_base_branch, "file-branch");

        // DEFAULT used for anything untouched
        assert_eq!(cfg.agy_path, PathBuf::from("/usr/local/libexec/agy"));
    }

    #[test]
    fn test_hindsight_precedence() {
        let temp_dir = tempdir().unwrap();
        let config_file_path = temp_dir.path().join("config.toml");
        let toml_content = r#"
hindsight_enabled = true
hindsight_bank = "file-bank"
hindsight_tag = "file-tag"
hindsight_path = "file-hindsight"
hindsight_auto_recall = true
hindsight_auto_retain = true
hindsight_url = "https://file.example"
hindsight_api_key = "file-key"
hindsight_profile = "file-profile"
"#;
        fs::write(&config_file_path, toml_content).unwrap();

        let env = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("AGENT_RUN_HINDSIGHT_BANK", "env-bank"),
            ("AGENT_RUN_HINDSIGHT_AUTO_RETAIN", "false"),
        ]);

        let cfg = Config::load_with_env(&env).expect("load hindsight config");

        // ENV wins over FILE
        assert_eq!(cfg.hindsight_bank, Some("env-bank".to_string()));
        assert!(!cfg.hindsight_auto_retain);

        // FILE wins over DEFAULT
        assert!(cfg.hindsight_enabled);
        assert_eq!(cfg.hindsight_tag, Some("file-tag".to_string()));
        assert_eq!(cfg.hindsight_path, PathBuf::from("file-hindsight"));
        assert!(cfg.hindsight_auto_recall);
        assert_eq!(cfg.hindsight_url, Some("https://file.example".to_string()));
        assert_eq!(cfg.hindsight_api_key, Some("file-key".to_string()));
        assert_eq!(cfg.hindsight_profile, Some("file-profile".to_string()));
    }

    #[test]
    fn test_hindsight_empty_strings_are_none() {
        let env = env_with(&[
            ("AGENT_RUN_HINDSIGHT_BANK", "   "),
            ("AGENT_RUN_HINDSIGHT_URL", ""),
            ("AGENT_RUN_HINDSIGHT_ENABLED", ""),
        ]);
        let cfg = Config::load_with_env(&env).unwrap();
        assert_eq!(cfg.hindsight_bank, None);
        assert_eq!(cfg.hindsight_url, None);
        // empty string parses to false
        assert!(!cfg.hindsight_enabled);
    }

    #[test]
    fn test_config_file_serialization_omits_none() {
        let file = ConfigFile {
            workspace_root: Some("/custom".to_string()),
            gc_max_age_days: Some(7),
            ..Default::default()
        };
        let serialized = toml::to_string(&file).unwrap();
        assert!(serialized.contains(r#"workspace_root = "/custom""#));
        assert!(serialized.contains("gc_max_age_days = 7"));
        assert!(!serialized.contains("agy_path"));
        assert!(!serialized.contains("hindsight_bank"));
    }

    #[test]
    fn test_resolve_config_path_precedence() {
        let temp_dir = tempdir().unwrap();
        let custom = temp_dir.path().join("custom.toml");
        let env = env_with(&[("AGENT_RUN_CONFIG", custom.to_str().unwrap())]);
        assert_eq!(Config::resolve_config_path(&env), Some(custom));

        let env_empty = HashMap::new();
        // default path exists iff the host user has ~/.agent-run (informational, not asserted)
        let _ = Config::resolve_config_path(&env_empty);
    }

    #[test]
    fn test_malformed_toml_error_includes_path_and_parse_error() {
        let temp_dir = tempdir().unwrap();
        let bad_path = temp_dir.path().join("bad.toml");
        fs::write(&bad_path, "not valid toml [[[").unwrap();
        let env = env_with(&[("AGENT_RUN_CONFIG", bad_path.to_str().unwrap())]);
        let err = Config::load_with_env(&env).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(bad_path.to_str().unwrap()),
            "error should name the path: {msg}"
        );
    }

    #[test]
    fn test_nonexistent_config_uses_defaults() {
        let temp_dir = tempdir().unwrap();
        let missing = temp_dir.path().join("missing.toml");
        let env = env_with(&[("AGENT_RUN_CONFIG", missing.to_str().unwrap())]);
        let cfg = Config::load_with_env(&env).unwrap();
        assert_eq!(cfg.workspace_root, PathBuf::from("/workspace"));
        assert_eq!(cfg.gc_max_age_days, DEFAULT_GC_MAX_AGE_DAYS);
    }

    #[test]
    fn test_invalid_launch_timeout_env_falls_back_to_default() {
        // Unlike the fail-loud numeric settings (GC/MAX_PARALLEL/SHUTDOWN),
        // an invalid or non-positive AGENT_RUN_LAUNCH_TIMEOUT_SECS must fall
        // back to the default with a warning: a mistyped value must not be
        // able to break every `agent-run start`.
        assert_eq!(DEFAULT_LAUNCH_TIMEOUT_SECS, 20);
        assert_eq!(Config::default().launch_timeout_secs, 20);

        for bad in ["not-a-number", "0", "-5", "1.5"] {
            let env = env_with(&[("AGENT_RUN_LAUNCH_TIMEOUT_SECS", bad)]);
            let cfg = Config::load_with_env(&env).unwrap_or_else(|e| {
                panic!(
                    "invalid launch timeout env '{}' must fall back, not error: {e}",
                    bad
                )
            });
            assert_eq!(
                cfg.launch_timeout_secs, DEFAULT_LAUNCH_TIMEOUT_SECS,
                "invalid env value '{}' must fall back to the default",
                bad
            );
        }

        // A valid env value is used as-is.
        let env = env_with(&[("AGENT_RUN_LAUNCH_TIMEOUT_SECS", "7")]);
        let cfg = Config::load_with_env(&env).expect("load config with env launch timeout");
        assert_eq!(cfg.launch_timeout_secs, 7);
    }

    #[test]
    fn test_stall_minutes_env_and_fallback() {
        assert_eq!(DEFAULT_STALL_MINUTES, 20);
        assert_eq!(Config::default().stall_minutes, 20);

        // Valid env value is used as-is.
        let env = env_with(&[("AGENT_RUN_STALL_MINUTES", "7")]);
        let cfg = Config::load_with_env(&env).expect("load config with env stall minutes");
        assert_eq!(cfg.stall_minutes, 7);

        // Invalid or non-positive values fall back to the default (with a
        // warning), same as launch_timeout_secs.
        for bad in ["not-a-number", "0", "-5", ""] {
            let env = env_with(&[("AGENT_RUN_STALL_MINUTES", bad)]);
            let cfg = Config::load_with_env(&env).unwrap_or_else(|e| {
                panic!(
                    "invalid stall-minutes env '{}' must fall back, not error: {e}",
                    bad
                )
            });
            assert_eq!(
                cfg.stall_minutes, DEFAULT_STALL_MINUTES,
                "invalid env value '{}' must fall back to the default",
                bad
            );
        }
    }

    #[test]
    fn test_invalid_gc_max_age_days_env_fails_loudly() {
        let env = env_with(&[("AGENT_RUN_GC_MAX_AGE_DAYS", "not-a-number")]);
        let err = Config::load_with_env(&env).unwrap_err();
        assert!(err.to_string().contains("AGENT_RUN_GC_MAX_AGE_DAYS"));
    }

    #[test]
    fn test_invalid_boolean_env_var_fails_loudly() {
        let env = env_with(&[("AGENT_RUN_HINDSIGHT_AUTO_RECALL", "maybe")]);
        let err = Config::load_with_env(&env).unwrap_err();
        assert!(err.to_string().contains("AGENT_RUN_HINDSIGHT_AUTO_RECALL"));
    }

    #[test]
    fn test_trim_and_empty_fields_fall_back_to_defaults_or_none() {
        let env = env_with(&[
            ("AGENT_RUN_JOBS_DIR_NAME", "   "),
            ("AGENT_RUN_DEFAULT_BASE_BRANCH", "  "),
            ("AGENT_RUN_DEFAULT_MODEL", "  "),
        ]);
        let cfg = Config::load_with_env(&env).unwrap();
        assert_eq!(cfg.jobs_dir_name, DEFAULT_JOBS_DIR_NAME);
        assert_eq!(cfg.default_base_branch, DEFAULT_BASE_BRANCH);
        assert_eq!(cfg.default_model, None);
    }

    #[test]
    fn test_atomic_write_creates_and_overwrites_atomically() {
        let temp_dir = tempdir().unwrap();
        let target = temp_dir.path().join("nested/dir/file.txt");
        atomic_write(&target, b"first").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "first");
        atomic_write(&target, b"second").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "second");
    }

    #[test]
    fn test_agent_run_hindsight_env_name_and_invalid_fail_loud() {
        // AGENT_RUN_HINDSIGHT works
        let env_ok = env_with(&[("AGENT_RUN_HINDSIGHT", "true")]);
        let cfg = Config::load_with_env(&env_ok).expect("load config");
        assert!(cfg.hindsight_enabled);

        // Invalid AGENT_RUN_HINDSIGHT fails loud
        let env_bad = env_with(&[("AGENT_RUN_HINDSIGHT", "invalid_bool")]);
        let err = Config::load_with_env(&env_bad).unwrap_err();
        assert!(err.to_string().contains("AGENT_RUN_HINDSIGHT"));

        // Invalid AGENT_RUN_HINDSIGHT_AUTO_RETAIN fails loud
        let env_retain_bad = env_with(&[("AGENT_RUN_HINDSIGHT_AUTO_RETAIN", "bad_value")]);
        let err = Config::load_with_env(&env_retain_bad).unwrap_err();
        assert!(err.to_string().contains("AGENT_RUN_HINDSIGHT_AUTO_RETAIN"));
    }

    #[test]
    fn test_auto_recall_or_retain_implies_hindsight_enabled() {
        let env_recall = env_with(&[("AGENT_RUN_HINDSIGHT_AUTO_RECALL", "true")]);
        let cfg_recall = Config::load_with_env(&env_recall).unwrap();
        assert!(cfg_recall.hindsight_auto_recall);
        assert!(cfg_recall.hindsight_enabled);

        let env_retain = env_with(&[("AGENT_RUN_HINDSIGHT_AUTO_RETAIN", "true")]);
        let cfg_retain = Config::load_with_env(&env_retain).unwrap();
        assert!(cfg_retain.hindsight_auto_retain);
        assert!(cfg_retain.hindsight_enabled);
    }

    #[test]
    fn test_proxy_precedence_and_case_insensitivity() {
        let temp_dir = tempdir().unwrap();
        let config_file_path = temp_dir.path().join("config.toml");
        let toml_content = r#"
http_proxy = "http://file-http:8080"
https_proxy = "http://file-https:8443"
no_proxy = "file-no-proxy"
"#;
        fs::write(&config_file_path, toml_content).unwrap();

        // 1. Inherit from standard process env (case-insensitive) when AGENT_RUN_* and TOML unset
        let env_inherited = env_with(&[
            ("http_proxy", "http://process-http:8080"),
            ("HTTPS_PROXY", "http://process-https:8443"),
            ("No_Proxy", "process-no-proxy"),
        ]);
        let cfg = Config::load_with_env(&env_inherited).unwrap();
        assert_eq!(cfg.http_proxy, Some("http://process-http:8080".to_string()));
        assert_eq!(
            cfg.https_proxy,
            Some("http://process-https:8443".to_string())
        );
        assert_eq!(cfg.no_proxy, Some("process-no-proxy".to_string()));

        // 2. TOML beats process env
        let env_toml_beats_process = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("http_proxy", "http://process-http:8080"),
        ]);
        let cfg = Config::load_with_env(&env_toml_beats_process).unwrap();
        assert_eq!(cfg.http_proxy, Some("http://file-http:8080".to_string()));
        assert_eq!(cfg.https_proxy, Some("http://file-https:8443".to_string()));
        assert_eq!(cfg.no_proxy, Some("file-no-proxy".to_string()));

        // 3. AGENT_RUN_* beats TOML and process env
        let env_agent_run_wins = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("http_proxy", "http://process-http:8080"),
            ("AGENT_RUN_HTTP_PROXY", "http://env-override:8080"),
            ("AGENT_RUN_NO_PROXY", "env-no-proxy"),
        ]);
        let cfg = Config::load_with_env(&env_agent_run_wins).unwrap();
        assert_eq!(cfg.http_proxy, Some("http://env-override:8080".to_string()));
        assert_eq!(cfg.https_proxy, Some("http://file-https:8443".to_string()));
        assert_eq!(cfg.no_proxy, Some("env-no-proxy".to_string()));
    }

    #[test]
    fn test_max_runtime_config_unset_by_default() {
        // No env, no file: there is NO default max runtime -- jobs unbounded.
        let temp_dir = tempdir().unwrap();
        let env = env_with(&[(
            "AGENT_RUN_STATE_ROOT",
            temp_dir.path().join("state").to_str().unwrap(),
        )]);
        let cfg = Config::load_with_env(&env).unwrap();
        assert_eq!(cfg.max_runtime, None);
    }

    #[test]
    fn test_max_runtime_env_and_file_precedence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_file_path = temp_dir.path().join("config.toml");
        std::fs::write(&config_file_path, "max_runtime = \"45m\"\n").unwrap();

        // File value is honored.
        let env_file = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            (
                "AGENT_RUN_STATE_ROOT",
                temp_dir.path().join("state").to_str().unwrap(),
            ),
        ]);
        let cfg = Config::load_with_env(&env_file).unwrap();
        assert_eq!(cfg.max_runtime, Some(chrono::Duration::minutes(45)));

        // Env beats file (same duration syntax as wait --timeout).
        let env_both = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            (
                "AGENT_RUN_STATE_ROOT",
                temp_dir.path().join("state").to_str().unwrap(),
            ),
            ("AGENT_RUN_MAX_RUNTIME", "2h"),
        ]);
        let cfg = Config::load_with_env(&env_both).unwrap();
        assert_eq!(cfg.max_runtime, Some(chrono::Duration::hours(2)));

        // Invalid value is a hard error (no silent fallback; unset would be
        // indistinguishable from a mistake).
        let env_bad = env_with(&[
            (
                "AGENT_RUN_STATE_ROOT",
                temp_dir.path().join("state").to_str().unwrap(),
            ),
            ("AGENT_RUN_MAX_RUNTIME", "not-a-duration"),
        ]);
        assert!(Config::load_with_env(&env_bad).is_err());
    }

    #[test]
    fn test_remote_control_config_precedence_and_env_values() {
        let temp_dir = tempdir().unwrap();
        let state_root = temp_dir.path().join("state").to_str().unwrap().to_string();

        // 1. Unset by default -> false
        let env_unset = env_with(&[("AGENT_RUN_STATE_ROOT", &state_root)]);
        let cfg = Config::load_with_env(&env_unset).unwrap();
        assert!(!cfg.remote_control);

        // 2. Env values: "0", "1", "true", "TRUE"
        let env_zero = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL", "0"),
        ]);
        assert!(!Config::load_with_env(&env_zero).unwrap().remote_control);

        let env_one = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL", "1"),
        ]);
        assert!(Config::load_with_env(&env_one).unwrap().remote_control);

        let env_true = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL", "true"),
        ]);
        assert!(Config::load_with_env(&env_true).unwrap().remote_control);

        let env_caps = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL", "TRUE"),
        ]);
        assert!(Config::load_with_env(&env_caps).unwrap().remote_control);

        // 3. TOML file resolution and env precedence
        let config_file_path = temp_dir.path().join("config.toml");
        fs::write(&config_file_path, "remote_control = true\n").unwrap();

        let env_file = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("AGENT_RUN_STATE_ROOT", &state_root),
        ]);
        assert!(Config::load_with_env(&env_file).unwrap().remote_control);

        let env_override = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL", "0"),
        ]);
        assert!(!Config::load_with_env(&env_override).unwrap().remote_control);

        // 4. Invalid value fails
        let env_invalid = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL", "invalid_bool"),
        ]);
        assert!(Config::load_with_env(&env_invalid).is_err());
    }

    #[test]
    fn test_remote_control_autostart_and_check_secs() {
        let temp_dir = tempdir().unwrap();
        let state_root = temp_dir.path().join("state").to_str().unwrap().to_string();

        // 1. Defaults
        let env_unset = env_with(&[("AGENT_RUN_STATE_ROOT", &state_root)]);
        let cfg = Config::load_with_env(&env_unset).unwrap();
        assert!(!cfg.remote_control_autostart);
        assert_eq!(
            cfg.remote_control_check_secs,
            DEFAULT_REMOTE_CONTROL_CHECK_SECS
        );

        // 2. Autostart env values
        let env_as_true = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL_AUTOSTART", "true"),
        ]);
        assert!(
            Config::load_with_env(&env_as_true)
                .unwrap()
                .remote_control_autostart
        );

        let env_as_one = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL_AUTOSTART", "1"),
        ]);
        assert!(
            Config::load_with_env(&env_as_one)
                .unwrap()
                .remote_control_autostart
        );

        let env_as_zero = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL_AUTOSTART", "0"),
        ]);
        assert!(
            !Config::load_with_env(&env_as_zero)
                .unwrap()
                .remote_control_autostart
        );

        // 3. Check secs valid, clamped, fallback
        let env_cs_valid = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL_CHECK_SECS", "30"),
        ]);
        assert_eq!(
            Config::load_with_env(&env_cs_valid)
                .unwrap()
                .remote_control_check_secs,
            30
        );

        // Below minimum 10 clamps to 10
        let env_cs_small = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL_CHECK_SECS", "5"),
        ]);
        assert_eq!(
            Config::load_with_env(&env_cs_small)
                .unwrap()
                .remote_control_check_secs,
            10
        );

        // Invalid integer falls back to default 60
        let env_cs_invalid = env_with(&[
            ("AGENT_RUN_STATE_ROOT", &state_root),
            ("AGENT_RUN_REMOTE_CONTROL_CHECK_SECS", "abc"),
        ]);
        assert_eq!(
            Config::load_with_env(&env_cs_invalid)
                .unwrap()
                .remote_control_check_secs,
            60
        );

        // 4. File config resolution
        let config_file_path = temp_dir.path().join("config.toml");
        fs::write(
            &config_file_path,
            "remote_control_autostart = true\nremote_control_check_secs = 45\n",
        )
        .unwrap();

        let env_file = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            ("AGENT_RUN_STATE_ROOT", &state_root),
        ]);
        let cfg_file = Config::load_with_env(&env_file).unwrap();
        assert!(cfg_file.remote_control_autostart);
        assert_eq!(cfg_file.remote_control_check_secs, 45);
    }

    #[test]
    fn test_engine_defaults_unset_by_default() {
        // HL-91 E1: the engine selection defaults exist but are None unless
        // configured -- nothing changes for existing deployments.
        let temp_dir = tempdir().unwrap();
        let env = env_with(&[(
            "AGENT_RUN_STATE_ROOT",
            temp_dir.path().join("state").to_str().unwrap(),
        )]);
        let cfg = Config::load_with_env(&env).unwrap();
        assert_eq!(cfg.default_engine, None);
        assert_eq!(cfg.default_session_engine, None);
    }

    #[test]
    fn test_engine_defaults_env_and_file_precedence() {
        let temp_dir = tempdir().unwrap();
        let config_file_path = temp_dir.path().join("config.toml");
        fs::write(
            &config_file_path,
            "default_engine = \"file-engine\"\ndefault_session_engine = \"file-session\"\n",
        )
        .unwrap();

        // File values are honored.
        let env_file = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            (
                "AGENT_RUN_STATE_ROOT",
                temp_dir.path().join("state").to_str().unwrap(),
            ),
        ]);
        let cfg = Config::load_with_env(&env_file).unwrap();
        assert_eq!(cfg.default_engine, Some("file-engine".to_string()));
        assert_eq!(cfg.default_session_engine, Some("file-session".to_string()));

        // Env beats file, per the global precedence.
        let env_both = env_with(&[
            ("AGENT_RUN_CONFIG", config_file_path.to_str().unwrap()),
            (
                "AGENT_RUN_STATE_ROOT",
                temp_dir.path().join("state").to_str().unwrap(),
            ),
            ("AGENT_RUN_DEFAULT_ENGINE", "env-engine"),
            ("AGENT_ORCHESTRATE_DEFAULT_SESSION_ENGINE", "env-session"),
        ]);
        let cfg = Config::load_with_env(&env_both).unwrap();
        assert_eq!(cfg.default_engine, Some("env-engine".to_string()));
        assert_eq!(cfg.default_session_engine, Some("env-session".to_string()));

        // Whitespace-only values resolve to None.
        let env_blank = env_with(&[
            (
                "AGENT_RUN_STATE_ROOT",
                temp_dir.path().join("state").to_str().unwrap(),
            ),
            ("AGENT_RUN_DEFAULT_ENGINE", "   "),
        ]);
        let cfg = Config::load_with_env(&env_blank).unwrap();
        assert_eq!(cfg.default_engine, None);
    }
}

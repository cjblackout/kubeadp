//! Internal git-sync replacement: periodic, atomic, optionally-shallow sync
//! of one or more read-only repositories. One process handles all repos;
//! each `<root>` is the git repo itself (git-sync v4.7.1 layout), per-commit
//! worktrees live at `<root>/.worktrees/<sha>`, a read-only checkout is
//! exposed through an atomically-flipped relative symlink `link`, and stale
//! worktrees are pruned by [`cleanup`]. Port of kubernetes/git-sync v4.7.1
//! adapted to "one process for all repos from one TOML" (out of scope: hooks,
//! HTTP/metrics, submodules, sparse/partial clone, pid1, SSH/cookie/GitHub
//! app auth, `env.go` flags; see DOCS.md).
//!
//! One cycle per repo (under a flock): store credentials, [`init_repo`], read
//! the linked worktree, [`fetch_ref`], resolve `FETCH_HEAD^{}`, and if changed
//! publish a new worktree ([`ensure_worktree`]) and flip the link
//! ([`publish_symlink`]). `--once` runs one cycle per repo (first failure
//! exits); `--loop` retries each repo at its `period_secs` with git-sync
//! fail-counting (`max_failures`). Tokens come only from `token_env`/
//! `token_file` (re-read every cycle), never argv/config/logs; git runs with
//! `GIT_TERMINAL_PROMPT=0` and a private `GIT_CONFIG_GLOBAL` from
//! [`setup_git_defaults`]. Remote URLs are stored without userinfo.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

pub const DEFAULT_ROOT_TEMPLATE: &str = "/workspace/.roots/{name}";
pub const DEFAULT_REF: &str = "main";
pub const DEFAULT_PERIOD_SECS: u64 = 300;
pub const MIN_PERIOD_SECS: u64 = 30;
pub const DEFAULT_DEPTH: u32 = 0;
/// Non-current worktrees kept as a grace period before pruning.
pub const DEFAULT_KEEP_PREV: usize = 2;
/// Config file when the env var is unset (used only if it exists).
pub const DEFAULT_CONFIG_FILE: &str = "/etc/kubeadp-agent/repo-sync.toml";
pub const DEFAULT_FETCH_TIMEOUT_SECS: u64 = 300;

pub const CONFIG_FILE_ENV: &str = "KUBEADP_REPO_SYNC_CONFIG";
pub const TOKEN_ENV_ENV: &str = "KUBEADP_REPO_SYNC_TOKEN_ENV";
pub const TOKEN_FILE_ENV: &str = "KUBEADP_REPO_SYNC_TOKEN_FILE";
pub const PERIOD_ENV: &str = "KUBEADP_REPO_SYNC_PERIOD_SECS";
pub const FETCH_TIMEOUT_ENV: &str = "KUBEADP_REPO_SYNC_FETCH_TIMEOUT_SECS";
pub const MAX_FAILURES_ENV: &str = "KUBEADP_REPO_SYNC_MAX_FAILURES";

pub const GIT_USERNAME: &str = "git-sync";
const REDACTED: &str = "REDACTED";
pub const LOCK_FILE: &str = ".repo-sync.lock";
pub const WORKTREES_DIR: &str = ".worktrees";

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RepoSyncFile {
    #[serde(default)]
    pub repos: Vec<RepoFile>,
    /// Consecutive failures per repo before `run_loop` aborts (git-sync `max-failures`: 0 = any failure exits, negative = retry forever); overridden by `KUBEADP_REPO_SYNC_MAX_FAILURES`.
    #[serde(default)]
    pub max_failures: Option<i64>,
    /// Name of the env var holding the token (highest-precedence secret source; never a literal token).
    #[serde(default)]
    pub token_env: Option<String>,
    /// Path of a file whose trimmed content is the token.
    #[serde(default)]
    pub token_file: Option<String>,
}

impl RepoSyncFile {
    pub fn new() -> Self {
        RepoSyncFile {
            repos: Vec::new(),
            token_env: None,
            token_file: None,
            max_failures: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RepoFile {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub r#ref: Option<String>,
    #[serde(default)]
    pub root: Option<String>,
    /// Absolute path of the symlink the pod exposes (e.g. `/workspace/<name>`).
    pub link: Option<String>,
    #[serde(default)]
    pub period_secs: Option<u64>,
    #[serde(default)]
    pub depth: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Repo {
    pub name: String,
    pub url: String,
    pub reference: String,
    pub root: PathBuf,
    pub link: PathBuf,
    pub period: Duration,
    /// 0 = full history (an auto `--unshallow` is issued when a shallowed repo is no longer depth-limited).
    pub depth: u32,
    /// Per-fetch command timeout (env `KUBEADP_REPO_SYNC_FETCH_TIMEOUT_SECS` always wins; default [`DEFAULT_FETCH_TIMEOUT_SECS`]).
    pub fetch_timeout: Duration,
}

impl Repo {
    fn worktrees(&self) -> PathBuf {
        self.root.join(WORKTREES_DIR)
    }

    /// The worktree value for the given hash (git-sync `worktreeFor`).
    fn worktree_for(&self, hash: &str) -> Worktree {
        Worktree(self.worktrees().join(hash))
    }
}

/// A per-commit git worktree (git-sync `worktree`): a directory under `<root>/.worktrees` whose basename is the commit hash it checks out. It may or may not exist on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worktree(PathBuf);

impl Worktree {
    /// The (intended) commit hash this worktree checks out.
    pub fn hash(&self) -> String {
        self.0
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    /// The absolute on-disk path of this worktree.
    pub fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Clone, Default)]
pub struct RepoSyncConfig {
    pub repos: Vec<Repo>,
    /// Failures per repo before `run_loop` aborts (0 = any failure exits, negative = retry forever).
    pub max_failures: i64,
    pub token_env: Option<String>,
    pub token_file: Option<String>,
}

/// A resolved credential. The token string lives only in this struct (and on the stdin of a `git credential approve` child), never argv/config/logs.
#[derive(Clone)]
pub struct TokenCtx {
    value: String,
}

impl std::fmt::Debug for TokenCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenCtx")
            .field("value", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug)]
pub struct AnotherInstance(pub PathBuf);

impl std::fmt::Display for AnotherInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "another repo-sync instance is running for root {}",
            self.0.display()
        )
    }
}

impl std::error::Error for AnotherInstance {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CycleOutcome {
    /// The symlink was flipped to a new commit.
    Updated {
        old_sha: Option<String>,
        new_sha: String,
        elapsed: Duration,
    },
    /// The fetched commit already matches the symlink target.
    UpToDate { sha: String, elapsed: Duration },
}

/// RAII flock guard on a repo root. One process per root; a second acquisition returns immediately with [`AnotherInstance`] instead of blocking.
#[derive(Debug)]
pub struct RootLock {
    _file: fs::File,
}

impl RootLock {
    pub fn try_acquire(root: &Path) -> Result<Option<RootLock>> {
        fs::create_dir_all(root)
            .with_context(|| format!("failed to create repo-sync root {}", root.display()))?;
        let path = root.join(LOCK_FILE);
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open lock file {}", path.display()))?;
        // SAFETY: `flock` is safe to call on a valid fd we own; EWOULDBLOCK is the non-blocking "held elsewhere" signal.
        let rc = unsafe {
            libc::flock(
                std::os::unix::io::AsRawFd::as_raw_fd(&file),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        };
        if rc == 0 {
            Ok(Some(RootLock { _file: file }))
        } else {
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(e) if e == libc::EWOULDBLOCK || e == libc::EAGAIN => Ok(None),
                _ => Err(err).with_context(|| format!("failed to lock {}", path.display())),
            }
        }
    }
}

/// Resolves a repo root value (default, `~`, `{workspace_root}`, `{name}`) to an absolute path.
fn resolve_root(raw: &str, name: &str, workspace_root: &Path) -> PathBuf {
    let with_name = raw.replace("{name}", name);
    let expanded = crate::core::config::expand_path(&with_name, workspace_root);
    if expanded.is_absolute() {
        expanded
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(expanded)
    }
}

/// Parses a `[repo_sync]` TOML document; `source` is used only for messages.
pub fn parse_config_document(source: &str, content: &str) -> Result<RepoSyncConfig> {
    let parsed: toml::Value = toml::from_str(content).map_err(|e| {
        anyhow::anyhow!(
            "Invalid TOML in repo-sync config '{source}' at {}:{}: {}",
            e.span().map(|s| s.start).unwrap_or(0),
            e.span().map(|s| s.end).unwrap_or(0),
            e.message(),
        )
    })?;
    let table = parsed.as_table().ok_or_else(|| {
        anyhow::anyhow!("Invalid repo-sync config '{source}': the document root must be a table")
    })?;
    // The file may use either a bare top-level table (`repos = [...]`, `token_env = ...`) or the conventional `[repo_sync]` section wrapper. Only the `repo_sync` section name is accepted.
    let section = if let Some(envelope) = table.get("repo_sync") {
        if table.len() != 1 {
            bail!(
                "Invalid repo-sync config '{source}': a '[repo_sync]' section must be the only top-level table"
            );
        }
        for (key, _) in table {
            if key != "repo_sync" {
                bail!(
                    "Invalid repo-sync config '{source}': unknown top-level key '{key}' (expected nothing next to '[repo_sync]')"
                );
            }
        }
        envelope.as_table().ok_or_else(|| {
            anyhow::anyhow!("Invalid repo-sync config '{source}': '[repo_sync]' must be a table")
        })?
    } else {
        table
    };
    let parsed: RepoSyncFile = RepoSyncFile::deserialize(section.clone())
        .map_err(|e| anyhow::anyhow!("Invalid repo-sync config '{source}': {e}"))?;
    resolve_file_config(parsed)
}

/// Resolves a parsed file config into [`RepoSyncConfig`] with defaults/validation.
fn resolve_file_config(file: RepoSyncFile) -> Result<RepoSyncConfig> {
    let workspace_root = Path::new(crate::core::config::DEFAULT_WORKSPACE_ROOT);
    let mut repos = Vec::with_capacity(file.repos.len());
    let mut names: HashSet<String> = HashSet::new();
    for r in file.repos {
        let name = r.name.trim().to_string();
        if name.is_empty() {
            bail!("repo-sync config: a repo entry has an empty 'name'");
        }
        if !names.insert(name.clone()) {
            bail!("repo-sync config: duplicate repo name '{name}'");
        }
        let url = r.url.trim().to_string();
        if url.is_empty() {
            bail!("repo-sync config: repo '{name}' has an empty 'url'");
        }
        // Strip any embedded userinfo NOW, before the URL is ever logged or persisted: a credential in the URL must never reach argv, the repo config or an error message.
        let url = strip_url_userinfo(&url);
        let reference = r
            .r#ref
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_REF)
            .to_string();
        let root = resolve_root(
            r.root.as_deref().unwrap_or(DEFAULT_ROOT_TEMPLATE),
            &name,
            workspace_root,
        );
        let link_raw = r.link.as_deref().map(str::trim).filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "repo-sync config: repo '{name}' is missing its 'link' (absolute symlink path the pod exposes)"
                )
            })?;
        let link = PathBuf::from(link_raw);
        if !link.is_absolute() {
            bail!(
                "repo-sync config: repo '{name}' link '{}' must be an absolute path",
                link.display()
            );
        }
        let period_secs = r.period_secs.unwrap_or(DEFAULT_PERIOD_SECS);
        if period_secs < MIN_PERIOD_SECS {
            bail!(
                "repo-sync config: repo '{name}' period_secs {period_secs} is below the minimum {MIN_PERIOD_SECS}"
            );
        }
        // depth 0 = "full history", git-sync semantics (>= 0; an auto `--unshallow` un-shallows a repo that was fetched shallowly earlier).
        let depth = r.depth.unwrap_or(DEFAULT_DEPTH);
        repos.push(Repo {
            name,
            url,
            reference,
            root,
            link,
            period: Duration::from_secs(period_secs),
            depth,
            fetch_timeout: Duration::from_secs(DEFAULT_FETCH_TIMEOUT_SECS),
        });
    }
    if repos.is_empty() {
        bail!("repo-sync config: no repositories configured under [repo_sync] 'repos'");
    }
    Ok(RepoSyncConfig {
        repos,
        max_failures: file.max_failures.unwrap_or(0),
        token_env: file.token_env,
        token_file: file.token_file,
    })
}

/// Loads config from env + config file (env overrides > file > defaults).
pub fn load_config() -> Result<RepoSyncConfig> {
    load_config_impl(&std::env::vars().collect())
}

fn load_config_impl(env: &HashMap<String, String>) -> Result<RepoSyncConfig> {
    let path = match env.get(CONFIG_FILE_ENV) {
        Some(p) if !p.trim().is_empty() => Some(p.trim().to_string()),
        Some(_) => bail!("{CONFIG_FILE_ENV} must not be empty"),
        None => {
            if Path::new(DEFAULT_CONFIG_FILE).exists() {
                Some(DEFAULT_CONFIG_FILE.to_string())
            } else {
                None
            }
        }
    };
    let mut cfg = match path {
        Some(p) if Path::new(&p).exists() => {
            let content = fs::read_to_string(&p)
                .with_context(|| format!("failed to read repo-sync config file '{p}'"))?;
            parse_config_document(&p, &content)?
        }
        _ => RepoSyncConfig::default(),
    };
    // Environment overrides, same "environment always wins" pattern.
    if let Some(v) = env.get(TOKEN_ENV_ENV) {
        cfg.token_env = Some(v.trim().to_string());
    }
    if let Some(v) = env.get(TOKEN_FILE_ENV) {
        cfg.token_file = Some(v.trim().to_string());
    }
    if let Some(v) = env
        .get(MAX_FAILURES_ENV)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        cfg.max_failures = v.parse().with_context(|| {
            format!("invalid {MAX_FAILURES_ENV} value '{v}' (expected an integer)")
        })?;
    }
    if let Some(v) = env
        .get(PERIOD_ENV)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let secs: u64 = v.parse().with_context(|| {
            format!("invalid {PERIOD_ENV} value '{v}' (expected a positive integer)")
        })?;
        if secs < MIN_PERIOD_SECS {
            bail!("{PERIOD_ENV} value {secs} is below the minimum {MIN_PERIOD_SECS}");
        }
        for repo in &mut cfg.repos {
            repo.period = Duration::from_secs(secs);
        }
    }
    if let Some(v) = env
        .get(FETCH_TIMEOUT_ENV)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let secs: u64 = v.parse().with_context(|| {
            format!("invalid {FETCH_TIMEOUT_ENV} value '{v}' (expected a positive integer)")
        })?;
        if secs == 0 {
            bail!("{FETCH_TIMEOUT_ENV} value {secs} must be positive");
        }
        let timeout = Duration::from_secs(secs);
        for repo in &mut cfg.repos {
            repo.fetch_timeout = timeout;
        }
    }
    if cfg.repos.is_empty() {
        bail!(
            "repo-sync: no repositories configured (set {CONFIG_FILE_ENV} to a TOML file with a [repo_sync] section)"
        );
    }
    Ok(cfg)
}

/// Resolves the credential (env var or token file, trimmed); re-read every cycle.
pub fn refresh_token(cfg: &RepoSyncConfig) -> Result<Option<TokenCtx>> {
    let value = if let Some(env_name) = cfg.token_env.as_deref() {
        match std::env::var(env_name) {
            Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
            Ok(_) => None,
            Err(_) => {
                bail!(
                    "repo-sync: token_env names environment variable '{env_name}' which is not set"
                )
            }
        }
    } else {
        None
    };
    let value = match value {
        Some(v) => Some(v),
        None => match cfg.token_file.as_deref() {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|e| {
                    anyhow::anyhow!("repo-sync: failed to read token file '{}': {e}", path)
                })?;
                let trimmed = content.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            None => None,
        },
    };
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(Some(TokenCtx { value }))
}

fn strip_url_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after = &url[scheme_end + 3..];
    if let Some(at) = after.rfind('@') {
        return format!("{}{}", &url[..scheme_end + 3], &after[at + 1..]);
    }
    url.to_string()
}

/// Redacts the password but keeps the username for logging (git-sync `redactURL`).
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after = &url[scheme_end + 3..];
    let Some(at) = after.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &after[..at];
    let host_and_path = &after[at + 1..];
    let username = userinfo.split(':').next().unwrap_or("");
    if userinfo.contains(':') {
        format!(
            "{}://{}:{REDACTED}@{host_and_path}",
            &url[..scheme_end],
            username
        )
    } else {
        format!("{}://{username}@{host_and_path}", &url[..scheme_end])
    }
}

/// Path of the private per-process gitconfig (git-sync private config).
fn private_gitconfig_path() -> Result<PathBuf> {
    let dir = std::env::var("KUBEADP_REPO_SYNC_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!(".repo-sync-{}", std::process::id()))
        });
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let path = dir.join("gitconfig");
    if !path.exists() {
        fs::write(&path, "").with_context(|| format!("failed to create {}", path.display()))?;
    }
    Ok(path)
}

/// Writes git-sync's git defaults into the private gitconfig.
pub fn setup_git_defaults() -> Result<()> {
    let configs: [(&str, &str); 5] = [
        ("gc.autoDetach", "false"),
        ("gc.pruneExpire", "now"),
        ("credential.helper", "cache --timeout 3600"),
        ("core.askPass", "true"),
        ("safe.directory", "*"),
    ];
    for (key, val) in configs {
        run_git(
            git_base_command().args(["config", "--global", key, val]),
            &format!("config --global {key} {val}"),
        )
        .with_context(|| format!("error configuring git {key} {val}"))?;
    }
    Ok(())
}

/// Base `git` command: `GIT_TERMINAL_PROMPT=0` + private `GIT_CONFIG_GLOBAL`.
fn git_base_command() -> Command {
    let mut cmd = Command::new("git");
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    if let Ok(config) = private_gitconfig_path() {
        cmd.env("GIT_CONFIG_GLOBAL", config);
        cmd.env("GIT_CONFIG_NOSYSTEM", "true");
    }
    cmd
}

fn run_git(cmd: &mut Command, context: &str) -> Result<()> {
    let out = run_git_with_timeout(cmd, Duration::from_secs(600))
        .with_context(|| format!("failed to execute 'git {context}'"))?;
    if out.status.success() {
        Ok(())
    } else {
        bail!(
            "git {context} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
}

fn git_stdout(cmd: &mut Command) -> Option<String> {
    let out = run_git_with_timeout(cmd, Duration::from_secs(600)).ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// Runs a command with piped stdio, killing it on timeout.
fn run_git_with_timeout(
    cmd: &mut Command,
    timeout: Duration,
) -> std::io::Result<std::process::Output> {
    run_git_with_io(cmd, timeout, None)
}

/// Like [`run_git_with_timeout`], feeding `stdin` to the child.
fn run_git_with_stdin(
    cmd: &mut Command,
    timeout: Duration,
    stdin: &str,
) -> std::io::Result<std::process::Output> {
    run_git_with_io(cmd, timeout, Some(stdin))
}

fn run_git_with_io(
    cmd: &mut Command,
    timeout: Duration,
    stdin: Option<&str>,
) -> std::io::Result<std::process::Output> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    if stdin.is_some() {
        cmd.stdin(Stdio::piped());
    }
    let mut child = cmd.spawn()?;
    if let Some(input) = stdin {
        use std::io::Write;
        if let Some(mut si) = child.stdin.take() {
            let _ = si.write_all(input.as_bytes());
        }
    }
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("git command exceeded the {timeout:?} timeout"),
                    ));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut s) = child.stdout.take() {
        use std::io::Read;
        let _ = s.read_to_end(&mut stdout);
    }
    if let Some(mut s) = child.stderr.take() {
        use std::io::Read;
        let _ = s.read_to_end(&mut stderr);
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

fn store_credentials(url: &str, token: &TokenCtx) -> Result<()> {
    let creds = format!(
        "url={url}\nusername={GIT_USERNAME}\npassword={}\n",
        token.value
    );
    let out = run_git_with_stdin(
        git_base_command().args(["credential", "approve"]),
        Duration::from_secs(30),
        &creds,
    )
    .context("failed to execute 'git credential approve'")?;
    if out.status.success() {
        Ok(())
    } else {
        bail!(
            "can't configure git credentials: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
}

fn dir_is_empty(dir: &Path) -> Result<bool> {
    if !dir.exists() {
        return Ok(true);
    }
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("can't list repo directory {}", dir.display()))?;
    Ok(entries.next().is_none())
}

fn has_git_lock_file(dir: &Path) -> Result<Option<PathBuf>> {
    let lock_file = dir.join(".git").join("shallow.lock");
    match fs::metadata(&lock_file) {
        Ok(_) => Ok(Some(lock_file)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => {
            Err(e).with_context(|| format!("error calling stat on file {}", lock_file.display()))
        }
    }
}

fn sanity_check_repo(repo: &Repo) -> bool {
    match dir_is_empty(&repo.root) {
        Err(e) => {
            eprintln!("repo-sync: can't list repo directory: {e:#}");
            return false;
        }
        Ok(true) => return false,
        Ok(false) => {}
    }
    let root = repo.root.to_string_lossy();
    match git_stdout(git_base_command().args(["-C", &root, "rev-parse", "--show-toplevel"])) {
        Some(top) => {
            if top.trim() != repo.root.to_string_lossy().trim() {
                eprintln!(
                    "repo-sync: repo directory {} is under another repo (toplevel {})",
                    repo.root.display(),
                    top
                );
                return false;
            }
        }
        None => {
            eprintln!(
                "repo-sync: can't get repo toplevel for {}",
                repo.root.display()
            );
            return false;
        }
    }
    if run_git(
        git_base_command().args(["-C", &root, "fsck", "--no-progress", "--connectivity-only"]),
        "fsck --no-progress --connectivity-only",
    )
    .is_err()
    {
        return false;
    }
    match has_git_lock_file(&repo.root) {
        Err(e) => {
            eprintln!("repo-sync: {e:#}");
            false
        }
        Ok(Some(path)) => {
            eprintln!(
                "repo-sync: repo {} contains lock file {}",
                repo.root.display(),
                path.display()
            );
            false
        }
        Ok(None) => true,
    }
}

fn remove_dir_contents(dir: &Path) -> Result<()> {
    let entries = fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?;
    let mut first_error: Option<anyhow::Error> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        let res = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            remove_dir_all(&path)
        } else {
            fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))
        };
        if let Err(e) = res {
            eprintln!(
                "repo-sync: warning: failed to remove {}: {e:#}",
                path.display()
            );
            if first_error.is_none() {
                first_error = Some(e);
            }
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Ensures `<root>` is a usable git repo with `origin` set (git-sync `initRepo`).
fn init_repo(repo: &Repo) -> Result<()> {
    let mut need_git_init = false;

    match fs::metadata(&repo.root) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&repo.root)
                .with_context(|| format!("failed to create repo root {}", repo.root.display()))?;
            need_git_init = true;
        }
        Err(e) => return Err(e).with_context(|| format!("stat {} failed", repo.root.display())),
        Ok(m) if !m.is_dir() => {
            bail!(
                "repo root {} exists but is not a directory",
                repo.root.display()
            );
        }
        Ok(_) => {
            if sanity_check_repo(repo) {
                // already a valid repo, nothing to do.
            } else {
                eprintln!(
                    "repo-sync: repo directory {} is empty or failed checks; wiping it",
                    repo.root.display()
                );
                remove_dir_contents(&repo.root)?;
                need_git_init = true;
            }
        }
    }

    if need_git_init {
        // Running `git init` in an existing repo is safe (per git docs).
        let root = repo.root.to_string_lossy();
        run_git(
            git_base_command().args(["-C", &root, "init", "-b", "git-sync"]),
            "init -b git-sync",
        )
        .with_context(|| format!("failed to init repo {}", repo.root.display()))?;
        if !sanity_check_repo(repo) {
            bail!(
                "can't initialize git repo directory {}",
                repo.root.display()
            );
        }
    }

    // The "origin" remote has special meaning (e.g. in relative-path submodules), exactly like git-sync handles it.
    let url = strip_url_userinfo(&repo.url);
    let root = repo.root.to_string_lossy();
    let current = git_stdout(git_base_command().args(["-C", &root, "remote", "get-url", "origin"]));
    match current {
        None => {
            run_git(
                git_base_command().args(["-C", &root, "remote", "add", "origin", &url]),
                "remote add origin",
            )
            .with_context(|| format!("failed to add origin for repo {}", repo.root.display()))?;
        }
        Some(out) if out.trim() != url => {
            run_git(
                git_base_command().args(["-C", &root, "remote", "set-url", "origin", &url]),
                "remote set-url origin",
            )
            .with_context(|| format!("failed to set origin for repo {}", repo.root.display()))?;
        }
        Some(_) => {}
    }
    Ok(())
}

fn remove_dir_all(path: &Path) -> Result<()> {
    // Checkout trees are made read-only (write bits stripped); removing them requires write permission on the directories being emptied. Re-add the owner write bits first, then remove.
    if path.is_dir() {
        let _ = make_writable_recursively(path);
    }
    fs::remove_dir_all(path)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(e)
            }
        })
        .with_context(|| format!("failed to remove {}", path.display()))
}

fn make_writable_recursively(path: &Path) -> std::io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Ok(());
    }
    let mut perms = meta.permissions();
    perms.set_mode(meta.permissions().mode() | 0o700);
    fs::set_permissions(path, perms)?;
    if meta.is_dir() {
        for entry in fs::read_dir(path)? {
            let _ = make_writable_recursively(&entry?.path());
        }
    }
    Ok(())
}

fn strip_write_bits(path: &Path) -> std::io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Ok(());
    }
    let mut perms = meta.permissions();
    perms.set_mode(perms.mode() & !0o222);
    fs::set_permissions(path, perms)?;
    if meta.is_dir() {
        for entry in fs::read_dir(path)? {
            strip_write_bits(&entry?.path())?;
        }
    }
    Ok(())
}

/// Atomically points `link` at the relative `worktree` path (git-sync `publishSymlink`).
pub fn publish_symlink(repo: &Repo, worktree: &Worktree) -> Result<()> {
    let link_dir = repo
        .link
        .parent()
        .ok_or_else(|| anyhow::anyhow!("link '{}' has no parent", repo.link.display()))?;
    fs::create_dir_all(link_dir)
        .with_context(|| format!("failed to create symlink dir {}", link_dir.display()))?;

    let target = relative_path(link_dir, worktree.path()).with_context(|| {
        format!(
            "failed to compute relative link target for {}",
            worktree.path().display()
        )
    })?;

    const TMP_LINK: &str = "tmp-link";
    let tmp = link_dir.join(TMP_LINK);
    let _ = fs::remove_file(&tmp);
    std::os::unix::fs::symlink(&target, &tmp).with_context(|| {
        format!(
            "failed to create tmp symlink {} -> {}",
            tmp.display(),
            target.display()
        )
    })?;
    fs::rename(&tmp, &repo.link).with_context(|| {
        format!(
            "failed to flip symlink {} -> {}",
            repo.link.display(),
            target.display()
        )
    })?;
    Ok(())
}

/// Relative path of `target` from `base` (git-sync `filepath.Rel`).
pub fn relative_path(base: &Path, target: &Path) -> std::io::Result<PathBuf> {
    if base.is_absolute() != target.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot make relative path between absolute and relative paths",
        ));
    }
    let base = base.components().collect::<Vec<_>>();
    let target = target.components().collect::<Vec<_>>();
    let common = base
        .iter()
        .zip(target.iter())
        .take_while(|(b, t)| b == t)
        .count();
    let mut result = PathBuf::new();
    for _ in common..base.len() {
        result.push("..");
    }
    for c in &target[common..] {
        result.push(c.as_os_str());
    }
    Ok(result)
}

fn current_worktree(repo: &Repo) -> Result<Option<Worktree>> {
    let target = match fs::read_link(&repo.link) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read link {}", repo.link.display()))
        }
    };
    let resolved = if target.is_absolute() {
        target
    } else {
        let dir = repo
            .link
            .parent()
            .ok_or_else(|| anyhow::anyhow!("link '{}' has no parent", repo.link.display()))?;
        dir.join(&target)
    };
    // The resolved path may contain `..` components (the link target is relative); normalize it so the `starts_with` check is exact.
    let normalized = lexical_normalize(&resolved);
    if repo.worktrees().is_dir() && normalized.starts_with(repo.worktrees()) {
        Ok(Some(Worktree(normalized)))
    } else {
        Ok(None)
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                // Pop the last normal component if present; never cross the root or a prefix.
                let popped = out.pop();
                if !popped {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn current_linked_sha(repo: &Repo) -> Option<String> {
    current_worktree(repo).ok().flatten().map(|w| w.hash())
}

/// Prunes worktrees beyond the keep-count grace period; returns count removed.
fn remove_stale_worktrees(repo: &Repo, keep_prev: usize) -> usize {
    let Ok(entries) = fs::read_dir(repo.worktrees()) else {
        return 0;
    };
    let mut dirs: Vec<(String, std::time::SystemTime)> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        dirs.push((name_str.into_owned(), mtime));
    }
    dirs.sort_by_key(|(_, mtime)| std::cmp::Reverse(*mtime));
    let linked = current_linked_sha(repo);
    let mut kept = 0usize;
    let mut removed = 0usize;
    for (name, _) in dirs {
        if Some(&name) == linked.as_ref() {
            kept += 1;
            continue;
        }
        if kept < keep_prev {
            kept += 1;
            continue;
        }
        if let Err(e) = remove_worktree(repo, &repo.worktree_for(&name)) {
            eprintln!(
                "repo-sync: warning: failed to prune old worktree {}: {e:#}",
                repo.worktrees().join(&name).display()
            );
        } else {
            removed += 1;
        }
    }
    removed
}

fn remove_worktree(repo: &Repo, worktree: &Worktree) -> Result<()> {
    let path = worktree.path();
    if path.exists() {
        let _ = make_writable_recursively(path);
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove worktree directory {}", path.display()))?;
    }
    run_git(
        git_base_command().args([
            "-C",
            &repo.root.to_string_lossy(),
            "worktree",
            "prune",
            "--verbose",
        ]),
        "worktree prune --verbose",
    )?;
    Ok(())
}

/// Post-success housekeeping: prune worktrees, reflog expire, git gc (git-sync `cleanup`).
fn cleanup(repo: &Repo) {
    if remove_stale_worktrees(repo, DEFAULT_KEEP_PREV) == 0 {
        return;
    }
    if let Err(e) = run_git(
        git_base_command().args([
            "-C",
            &repo.root.to_string_lossy(),
            "worktree",
            "prune",
            "--verbose",
        ]),
        "worktree prune --verbose",
    ) {
        eprintln!("repo-sync: warning: worktree prune failed: {e:#}");
        return;
    }
    if let Err(e) = run_git(
        git_base_command().args([
            "-C",
            &repo.root.to_string_lossy(),
            "reflog",
            "expire",
            "--expire-unreachable=all",
            "--all",
        ]),
        "reflog expire --expire-unreachable=all --all",
    ) {
        eprintln!("repo-sync: warning: reflog expire failed: {e:#}");
        return;
    }
    if let Err(e) = run_git(
        git_base_command().args(["-C", &repo.root.to_string_lossy(), "gc"]),
        "gc",
    ) {
        eprintln!("repo-sync: warning: git gc failed: {e:#}");
    }
}

/// Fetches `ref` with depth/unshallow per config (git-sync `fetch`).
fn fetch_ref(repo: &Repo) -> Result<()> {
    let mut args: Vec<String> = vec![
        "-C".to_string(),
        repo.root.to_string_lossy().into_owned(),
        "fetch".to_string(),
        repo.url.clone(),
        repo.reference.clone(),
        "--verbose".to_string(),
        "--no-progress".to_string(),
        "--prune".to_string(),
        "--no-auto-gc".to_string(),
    ];
    if repo.depth > 0 {
        args.push("--depth".to_string());
        args.push(repo.depth.to_string());
    } else if is_shallow(repo)? {
        // The repo is shallow (a previous run used depth) but depth is off now: undo the shallow state.
        args.push("--unshallow".to_string());
    }
    let out = run_git_with_timeout(git_base_command().args(&args), repo.fetch_timeout)
        .with_context(|| {
            format!(
                "failed to fetch '{}' from {} ({}s timeout)",
                repo.reference,
                redact_url(&repo.url),
                repo.fetch_timeout.as_secs()
            )
        })?;
    if !out.status.success() {
        cleanup_git_locks(&repo.root);
        bail!(
            "fetch '{}' from {} failed: {}",
            repo.reference,
            redact_url(&repo.url),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    cleanup_git_locks(&repo.root);
    Ok(())
}

fn is_shallow(repo: &Repo) -> Result<bool> {
    let root = repo.root.to_string_lossy();
    let out =
        git_stdout(git_base_command().args(["-C", &root, "rev-parse", "--is-shallow-repository"]))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "can't determine repo shallowness for {}",
                    repo.root.display()
                )
            })?;
    match out.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => bail!("unparseable bool from git rev-parse --is-shallow-repository: {other:?}"),
    }
}

fn resolve_fetched_hash(repo: &Repo) -> Result<String> {
    let root = repo.root.to_string_lossy();
    git_stdout(git_base_command().args(["-C", &root, "rev-parse", "FETCH_HEAD^{}"])).ok_or_else(
        || {
            anyhow::anyhow!(
                "'{}' did not resolve to a commit after fetching from {}",
                repo.reference,
                redact_url(&repo.url)
            )
        },
    )
}

fn cleanup_git_locks(dir: &Path) {
    if !dir.is_dir() {
        return;
    }
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            if name.to_string_lossy().ends_with(".lock") {
                let _ = fs::remove_file(&path);
            } else if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                cleanup_git_locks(&path);
            }
        }
    }
}

fn ensure_worktree(repo: &Repo, sha: &str) -> Result<()> {
    let worktrees = repo.worktrees();
    fs::create_dir_all(&worktrees)
        .with_context(|| format!("failed to create {}", worktrees.display()))?;
    let worktree = repo.worktree_for(sha);
    // A leftover dir (partial or complete) must go: `worktree add` refuses to proceed otherwise ("'...' already exists"). Removing also prunes the git admin dir, so a fresh add works.
    remove_worktree(repo, &worktree)?;

    let root = repo.root.to_string_lossy();
    run_git(
        git_base_command().args([
            "-C",
            &root,
            "worktree",
            "add",
            "--force",
            "--detach",
            &worktree.path().to_string_lossy(),
            sha,
            "--no-checkout",
        ]),
        &format!(
            "worktree add --force --detach {} {sha} --no-checkout",
            worktree.path().display()
        ),
    )
    .with_context(|| {
        format!(
            "failed to create worktree {} for commit {sha}",
            worktree.path().display()
        )
    })?;

    configure_worktree(repo, sha, &worktree)?;
    strip_write_bits(worktree.path()).with_context(|| {
        format!(
            "failed to make checkout {} read-only",
            worktree.path().display()
        )
    })?;
    Ok(())
}

fn configure_worktree(repo: &Repo, sha: &str, worktree: &Worktree) -> Result<()> {
    let rel = relative_path(worktree.path(), &repo.root).with_context(|| {
        format!(
            "failed to compute relative path for {}",
            repo.root.display()
        )
    })?;
    // git stores a per-worktree admin dir under `<root>/.git/worktrees/<sha>` (git's internal name is `worktrees`, regardless of where the worktree directories themselves live on disk).
    let git_dir_ref = format!(
        "gitdir: {}\n",
        rel.join(".git").join("worktrees").join(sha).display()
    );
    fs::write(worktree.path().join(".git"), git_dir_ref).with_context(|| {
        format!(
            "failed to rewrite .git of worktree {}",
            worktree.path().display()
        )
    })?;

    let wt = worktree.path().to_string_lossy();
    run_git(
        git_base_command().args(["-C", &wt, "reset", "--hard", sha, "--"]),
        &format!("reset --hard {sha} --"),
    )
    .with_context(|| {
        format!(
            "failed to reset worktree {} to commit {sha}",
            worktree.path().display()
        )
    })?;
    Ok(())
}

fn status_line(repo: &Repo, old: Option<&str>, new: &str, elapsed: Duration) {
    eprintln!(
        "repo-sync {}: {} -> {} ({}ms)",
        repo.name,
        old.unwrap_or("-"),
        new,
        elapsed.as_millis()
    );
}

/// One sync cycle for one repo (git-sync `SyncRepo` order).
pub fn sync_one(repo: &Repo, token: Option<&TokenCtx>) -> Result<CycleOutcome> {
    let started = Instant::now();
    match RootLock::try_acquire(&repo.root)? {
        None => return Err(AnotherInstance(repo.root.clone()).into()),
        Some(_lock) => {}
    }
    // git-sync SyncRepo order: credentials, init, current-worktree, fetch, resolve. Credentials only apply to real URLs (http/https/ssh): a scheme-less local-path remote needs none and `git credential approve` would reject it.
    if let Some(tok) = token {
        if repo.url.contains("://") {
            store_credentials(&repo.url, &tok.clone())?;
        }
    }
    init_repo(repo)?;
    let current = current_worktree(repo)?;
    let old = current.as_ref().map(Worktree::hash);
    fetch_ref(repo)?;
    let sha = resolve_fetched_hash(repo)?;
    let elapsed = || started.elapsed();

    if old.as_deref() == Some(sha.as_str()) {
        // Already at the fetched commit; no update needed.
        status_line(repo, old.as_deref(), &sha, elapsed());
        return Ok(CycleOutcome::UpToDate {
            sha,
            elapsed: elapsed(),
        });
    }

    // Point the repo's own HEAD at the new ref so subsequent fetches are cheaper (git-sync `reset --soft <remoteHash> --`; the worktree itself is updated later).
    let root = repo.root.to_string_lossy();
    run_git(
        git_base_command().args(["-C", &root, "reset", "--soft", &sha, "--"]),
        &format!("reset --soft {sha} --"),
    )
    .with_context(|| {
        format!(
            "failed to reset repo {} to commit {sha}",
            repo.root.display()
        )
    })?;

    ensure_worktree(repo, &sha)?;
    let new_worktree = repo.worktree_for(&sha);
    publish_symlink(repo, &new_worktree).with_context(|| {
        format!(
            "failed to flip symlink {} to commit {sha}",
            repo.link.display()
        )
    })?;
    status_line(repo, old.as_deref(), &sha, elapsed());
    Ok(CycleOutcome::Updated {
        old_sha: old,
        new_sha: sha,
        elapsed: elapsed(),
    })
}

/// Runs one cycle per repo; first failure exits (`repo-sync --once`).
pub fn run_once(cfg: &RepoSyncConfig) -> Result<()> {
    setup_git_defaults()?;
    let token = refresh_token(cfg)?;
    for repo in &cfg.repos {
        match sync_one(repo, token.as_ref()) {
            Ok(_) => cleanup(repo),
            Err(e) if e.downcast_ref::<AnotherInstance>().is_some() => {
                eprintln!(
                    "repo-sync: another instance is running for root '{}'; exiting",
                    repo.root.display()
                );
                return Ok(());
            }
            Err(e) => {
                eprintln!("repo-sync: sync of '{}' failed: {:#}", repo.name, e);
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Periodic loop with git-sync fail-counting (`repo-sync --loop`).
pub fn run_loop(cfg: &RepoSyncConfig) -> Result<()> {
    setup_git_defaults()?;
    let mut last_sync: HashMap<String, Instant> = HashMap::new();
    let mut fail_count: HashMap<String, u32> = HashMap::new();
    loop {
        let token = refresh_token(cfg)?;
        let now = Instant::now();
        for repo in &cfg.repos {
            if last_sync
                .get(&repo.name)
                .map(|t| now.duration_since(*t) < repo.period)
                .unwrap_or(false)
            {
                continue;
            }
            match sync_one(repo, token.as_ref()) {
                Ok(_) => {
                    fail_count.remove(&repo.name);
                    cleanup(repo);
                }
                Err(e) if e.downcast_ref::<AnotherInstance>().is_some() => {
                    eprintln!(
                        "repo-sync: another instance is running for root '{}'; exiting",
                        repo.root.display()
                    );
                    return Ok(());
                }
                Err(e) => {
                    let n = fail_count.entry(repo.name.clone()).or_insert(0);
                    *n += 1;
                    eprintln!(
                        "repo-sync: warning: sync of '{}' failed (consecutive={}): {:#}",
                        repo.name, n, e
                    );
                    if cfg.max_failures >= 0 && *n as i64 >= cfg.max_failures {
                        return Err(anyhow::anyhow!(
                            "too many consecutive failures for '{}' ({n} >= {}), aborting",
                            repo.name,
                            cfg.max_failures
                        ));
                    }
                }
            }
            last_sync.insert(repo.name.clone(), Instant::now());
        }
        sleep_until_next_due(cfg, &last_sync);
    }
}

/// Sleeps until the next repo is due at its fixed `period_secs`.
fn sleep_until_next_due(cfg: &RepoSyncConfig, last_sync: &HashMap<String, Instant>) {
    let now = Instant::now();
    let mut wait = Duration::from_secs(3600);
    for repo in &cfg.repos {
        let last = last_sync.get(&repo.name).copied().unwrap_or(now);
        if let Some(remaining) = (last + repo.period).checked_duration_since(now) {
            wait = wait.min(remaining);
        }
    }
    if wait.is_zero() {
        wait = Duration::from_millis(10);
    }
    std::thread::sleep(wait);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_cfg(repos: Vec<RepoFile>) -> RepoSyncFile {
        RepoSyncFile {
            repos,
            ..RepoSyncFile::new()
        }
    }

    struct DirtyEnv;

    impl Drop for DirtyEnv {
        fn drop(&mut self) {
            std::env::remove_var("KUBEADP_REPO_SYNC_TOKEN");
            std::env::remove_var("GIT_TERMINAL_PROMPT");
            std::env::remove_var("KUBEADP_REPO_SYNC_RUNTIME_DIR");
            std::env::remove_var("KUBEADP_REPO_SYNC_TOKEN_ENV");
        }
    }

    fn token_for(value: &str) -> TokenCtx {
        TokenCtx {
            value: value.to_string(),
        }
    }

    fn assert_token_not_in_argv(cmd: &Command, token: &str) {
        use std::os::unix::ffi::OsStrExt;
        let argv = cmd.get_args().map(|a| a.as_bytes().to_vec());
        for arg in argv {
            assert!(
                !arg.windows(token.len()).any(|w| w == token.as_bytes()),
                "the token leaked into git argv: {:?}",
                cmd.get_args().collect::<Vec<_>>()
            );
        }
    }

    /// Sets up an isolated runtime dir so the private gitconfig lives under a
    /// temp dir (never the real user's config).
    ///
    /// The runtime dir is a process-global env var, so all tests that touch it
    /// are serialized through the shared test env mutex.
    fn with_runtime_dir<F: FnOnce(&Path)>(f: F) {
        let _guard = crate::core::lock_test_env();
        let _d = DirtyEnv;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var(
            "KUBEADP_REPO_SYNC_RUNTIME_DIR",
            dir.path().to_string_lossy().to_string(),
        );
        f(dir.path());
    }

    #[test]
    fn store_credentials_feeds_token_only_via_stdin() {
        with_runtime_dir(|_| {
            let token = "sekrit-token-xyz";
            let ctx = token_for(token);
            // The `git credential approve` command never carries the token in
            // argv or the environment.
            let mut cmd = git_base_command();
            cmd.args(["credential", "approve"]);
            assert_token_not_in_argv(&cmd, token);
            let envs = cmd.get_envs().collect::<Vec<_>>();
            for (k, v) in envs {
                assert!(
                    v.map(|v| v.to_string_lossy())
                        .map(|v| !v.contains(token))
                        .unwrap_or(true),
                    "the token leaked into the command environment via {k:?}"
                );
            }

            // Running it pipes the credential on stdin and succeeds.
            let url = "https://example.com/x.git";
            let res = store_credentials(url, &ctx);
            assert!(res.is_ok(), "credential approve must succeed: {res:?}");
        });
    }

    #[test]
    fn refresh_token_reads_file_each_call_and_trims() {
        let _lock = crate::core::lock_test_env();
        let _d = DirtyEnv;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, " first-secret \n").unwrap();
        let cfg = RepoSyncConfig {
            token_file: Some(file.to_string_lossy().to_string()),
            ..RepoSyncConfig::default()
        };
        let t1 = refresh_token(&cfg).unwrap().expect("token");
        assert_eq!(t1.value, "first-secret");

        // Rotation: the same configuration re-reads the file.
        std::fs::write(&file, "second-secret").unwrap();
        let t2 = refresh_token(&cfg).unwrap().expect("token");
        assert_eq!(t2.value, "second-secret");
    }

    #[test]
    fn refresh_token_reads_from_env_var() {
        let _lock = crate::core::lock_test_env();
        std::env::set_var("KUBEADP_REPO_SYNC_TEST_ONLY_REFRESH_TOKEN", "env-secret");
        let cfg = RepoSyncConfig {
            token_env: Some("KUBEADP_REPO_SYNC_TEST_ONLY_REFRESH_TOKEN".to_string()),
            ..RepoSyncConfig::default()
        };
        let t = refresh_token(&cfg).unwrap().expect("token");
        assert_eq!(t.value, "env-secret");
        assert!(
            !format!("{t:?}").contains("env-secret"),
            "Debug must not leak the token"
        );
        std::env::remove_var("KUBEADP_REPO_SYNC_TEST_ONLY_REFRESH_TOKEN");
    }

    #[test]
    fn redact_url_masks_passwords_but_keeps_usernames() {
        assert_eq!(
            redact_url("https://git:tok@example.com/x.git"),
            "https://git:REDACTED@example.com/x.git"
        );
        assert_eq!(
            redact_url("https://example.com/x.git"),
            "https://example.com/x.git"
        );
        assert_eq!(redact_url("/local/path"), "/local/path");
        assert_eq!(
            redact_url("https://plain@example.com/x"),
            "https://plain@example.com/x"
        );
    }

    #[test]
    fn redact_url_never_leaks_token_into_log_text() {
        let token = "sp3c-al-t0ken";
        let redacted = redact_url(&format!("https://user:{token}@example.com/x.git"));
        assert!(!redacted.contains(token), "redaction failed: {redacted}");
    }

    #[test]
    fn setup_git_defaults_writes_into_private_gitconfig() {
        with_runtime_dir(|dir| {
            setup_git_defaults().unwrap();
            let config = dir.join("gitconfig");
            let contents = std::fs::read_to_string(&config).unwrap();
            for needle in [
                "[gc]",
                "autoDetach = false",
                "pruneExpire = now",
                "helper = cache --timeout 3600",
                "askPass = true",
                "directory = *",
            ] {
                assert!(contents.contains(needle), "missing {needle} in {contents}");
            }
        });
    }

    #[test]
    fn init_repo_initializes_and_sets_stripped_origin() {
        with_runtime_dir(|dir| {
            let root = dir.join("roots").join("r");
            let repo = Repo {
                name: "r".to_string(),
                url: "https://git:tok@example.com/x.git".to_string(),
                reference: "main".to_string(),
                root: root.clone(),
                link: dir.join("out").join("r"),
                period: Duration::from_secs(300),
                depth: 0,
                fetch_timeout: Duration::from_secs(DEFAULT_FETCH_TIMEOUT_SECS),
            };
            init_repo(&repo).unwrap();
            assert!(root.join(".git").is_dir(), ".git must exist after init");
            assert!(sanity_check_repo(&repo), "fresh repo must pass sanity");
            let origin = git_stdout(git_base_command().args([
                "-C",
                &root.to_string_lossy(),
                "remote",
                "get-url",
                "origin",
            ]))
            .expect("origin");
            assert_eq!(origin, "https://example.com/x.git");

            // Idempotent: a second init leaves a valid repo with the same origin.
            init_repo(&repo).unwrap();
            assert!(sanity_check_repo(&repo));
        });
    }

    #[test]
    fn init_repo_wipes_unusable_root_and_reinitializes() {
        with_runtime_dir(|dir| {
            let root = dir.join("roots").join("r");
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("junk"), "not a repo").unwrap();
            let repo = Repo {
                name: "r".to_string(),
                url: "https://example.com/x.git".to_string(),
                reference: "main".to_string(),
                root: root.clone(),
                link: dir.join("out").join("r"),
                period: Duration::from_secs(300),
                depth: 0,
                fetch_timeout: Duration::from_secs(DEFAULT_FETCH_TIMEOUT_SECS),
            };
            assert!(!sanity_check_repo(&repo), "garbage dir must fail sanity");
            init_repo(&repo).unwrap();
            assert!(sanity_check_repo(&repo), "after wipe+init it must be valid");
            assert!(!root.join("junk").exists(), "the junk file must be gone");
        });
    }

    #[test]
    fn config_defaults_and_validation() {
        let cfg = resolve_file_config(file_cfg(vec![RepoFile {
            name: "proj".to_string(),
            url: "https://example.com/proj.git".to_string(),
            link: Some("/workspace/proj".to_string()),
            ..Default::default()
        }]))
        .unwrap();
        let r = &cfg.repos[0];
        assert_eq!(r.name, "proj");
        assert_eq!(r.reference, "main");
        assert_eq!(r.root, PathBuf::from("/workspace/.roots/proj"));
        assert_eq!(r.period, Duration::from_secs(300));
        assert_eq!(r.depth, 0);

        // period below the minimum is rejected.
        let err = resolve_file_config(file_cfg(vec![RepoFile {
            name: "p".to_string(),
            url: "u".to_string(),
            link: Some("/l".to_string()),
            period_secs: Some(5),
            ..Default::default()
        }]));
        assert!(err.is_err());

        // missing link is rejected.
        let err = resolve_file_config(file_cfg(vec![RepoFile {
            name: "p".to_string(),
            url: "u".to_string(),
            ..Default::default()
        }]));
        assert!(err.is_err());

        // an unknown key is a parse-time error (deny_unknown_fields).
        let err = parse_config_document(
            "t",
            "[repo_sync]\nrepos = [{ name = \"p\", url = \"u\", link = \"/l\", bogus = 1 }]\n",
        );
        assert!(err.is_err(), "unknown keys must error");
    }

    #[test]
    fn url_userinfo_is_stripped() {
        assert_eq!(
            strip_url_userinfo("https://git:tok@example.com/x.git"),
            "https://example.com/x.git"
        );
        assert_eq!(
            strip_url_userinfo("https://example.com/x.git"),
            "https://example.com/x.git"
        );
        assert_eq!(strip_url_userinfo("/local/path"), "/local/path");
    }

    #[test]
    fn lock_second_acquisition_reports_held() {
        let root = tempfile::tempdir().unwrap();
        let _first = RootLock::try_acquire(root.path()).expect("first lock");
        // A second acquisition from another handle must report held.
        match RootLock::try_acquire(root.path()) {
            Ok(None) => {}
            other => panic!("second acquisition while held must be None, got: {other:?}"),
        }
        drop(_first);
        // After release, acquisition succeeds again. Another test thread may
        // have forked a child that briefly inherits the released descriptor
        // (it is close-on-exec, so it is gone after the exec); retry for a
        // short while instead of failing on that window.
        let mut reacquired = false;
        for _ in 0..100 {
            if matches!(RootLock::try_acquire(root.path()), Ok(Some(_))) {
                reacquired = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            reacquired,
            "after releasing the lock, acquisition must succeed"
        );
    }

    #[test]
    fn publish_symlink_is_never_dangling_and_uses_relative_target() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // The worktrees live under the repo root (git-sync layout); the link
        // is elsewhere, so the relative target must point at the worktree
        // through `..` components.
        let repo_root = root.join("repo");
        let a = repo_root.join(".worktrees").join("aaa");
        let b = repo_root.join(".worktrees").join("bbb");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        let repo = Repo {
            name: "r".to_string(),
            url: "/dev/null".to_string(),
            reference: "main".to_string(),
            root: repo_root,
            link: root.join("out"),
            period: Duration::from_secs(300),
            depth: 0,
            fetch_timeout: Duration::from_secs(5),
        };
        // First publication is atomic too.
        publish_symlink(&repo, &Worktree(a.clone())).unwrap();
        // The target must be RELATIVE (volume-portable, git-sync).
        let target = fs::read_link(&repo.link).expect("the link must exist");
        assert!(
            !target.is_absolute(),
            "publish_symlink must target a relative path, got {target:?}"
        );
        let resolved = repo.link.parent().unwrap().join(&target);
        assert_eq!(resolved, a);

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = std::sync::Arc::clone(&stop);
        let link2 = repo.link.clone();
        let watch_a = a.clone();
        let watch_b = b.clone();
        let poller = std::thread::spawn(move || {
            while !stop2.load(std::sync::atomic::Ordering::SeqCst) {
                let target = fs::read_link(&link2).expect("the link must always exist");
                let resolved = link2.parent().unwrap().join(&target);
                assert!(
                    resolved == watch_a || resolved == watch_b,
                    "dangling or unexpected target: {resolved:?}"
                );
                std::thread::sleep(Duration::from_micros(50));
            }
        });
        for i in 0..200 {
            let target = if i % 2 == 0 { &b } else { &a };
            publish_symlink(&repo, &Worktree(target.clone())).unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        poller.join().unwrap();
    }

    #[test]
    fn strip_write_bits_clears_all_writers() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("f"), "x").unwrap();
        fs::write(dir.path().join("g"), "y").unwrap();
        strip_write_bits(dir.path()).unwrap();
        for p in [dir.path(), &sub, &sub.join("f"), &dir.path().join("g")] {
            let mode = fs::metadata(p).unwrap().permissions().mode();
            assert_eq!(mode & 0o222, 0, "{p:?} still writable: {mode:o}");
        }
    }
}

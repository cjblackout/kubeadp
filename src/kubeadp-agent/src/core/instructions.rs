//! Instruction-file hierarchy (stream 6, HL-125): per-run instruction files
//! for both engines, layered global -> per-session/per-project.
//!
//! A run (a session on the orchestrator, a job on the worker) materialises
//! the *same* instruction text into each engine's own file, so one operator
//! edit feeds claude and agy alike:
//!
//! - claude reads the global `~/.claude/CLAUDE.md` and the per-session
//!   `CLAUDE.md` in the run's working directory,
//! - agy reads the global `<agy-home>/.gemini/config/AGENTS.md` (the config
//!   file that already holds the MCP configuration) with the per-session text
//!   appended after it.
//!
//! ## Hierarchy (design 6.1)
//!
//! Merge order, highest wins last: **global -> per-session**. For a session
//! the per-session text is the private mounted `instructions_file` of that
//! session (or `<sessions-dir>/<session>.md`); for a worker job it is
//! `<sessions-dir>/<project>.md`, derived from `meta.project`.
//!
//! Content rules are enforced by the SPEC (§9.2 / the hierarchy section): a
//! per-level file may contain *rules only* -- no dates, no task ids, no
//! incident stories. The hierarchy is additive: later files append their
//! rules; they never replace a lower level.
//!
//! ## Read semantics (design 6.2)
//!
//! A **missing** configured instruction file is logged once (info) at run
//! start and skipped. A path that **exists but cannot be read** (a directory,
//! or a file without read permission) is an ERROR log line but never a start
//! failure: the run continues without that level.
//!
//! ## Witness files (design 6.3)
//!
//! Materialisation rewrites the engine's witness file on every run start (the
//! mounted sources are read again each time; the targets are written
//! read-only to the sandbox — the sandbox allows the runtime paths below).
//! The **legacy** per-project `memory_file` keeps working unchanged when the
//! new per-session file is absent; the new per-session file takes precedence
//! when present.
//!
//! This module is part of `core` (which must compile without the `server`
//! feature) and therefore contains its own small git helpers for the
//! tracked-`CLAUDE.md` refusal and the `.git/info/exclude` entry, mirroring
//! `orchestrate::memory` (the layer boundary forbids importing it here).

use std::io::Write;
use std::path::{Path, PathBuf};

/// Environment variable selecting the global instructions file
/// (design 6.2: `[common] instructions_file`).
pub const INSTRUCTIONS_FILE_ENV: &str = "KUBEADP_INSTRUCTIONS_FILE";
/// Default global instructions file (ConfigMap-mounted).
pub const DEFAULT_GLOBAL_INSTRUCTIONS: &str = "/etc/kubeadp-agent/instructions/global.md";
/// Environment variable overriding the directory that holds the per-session
/// / per-project instruction files (design 6.2; env-only, mirrors
/// `KUBEADP_INSTRUCTIONS_FILE`).
pub const SESSIONS_DIR_ENV: &str = "KUBEADP_INSTRUCTIONS_SESSIONS_DIR";
/// Default directory for the per-session / per-project instruction files.
pub const DEFAULT_SESSIONS_INSTRUCTIONS_DIR: &str = "/etc/kubeadp-agent/instructions/sessions";

/// Resolution of one instruction level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    /// The level is not configured at all.
    None,
    /// The level is configured but the path does not exist; `path` is the
    /// configured path (for one INFO log line at start).
    Missing(PathBuf),
    /// The level is configured and readable; `path` is the file and `text`
    /// its content.
    Content { path: PathBuf, text: String },
    /// The level is configured but cannot be read (directory, permission
    /// denied, ...); `reason` explains. This is an ERROR log line, never a
    /// start failure.
    Unreadable { path: PathBuf, reason: String },
}

/// The absolute path of the global instructions file:
/// `KUBEADP_INSTRUCTIONS_FILE` if set (and not blank), else the default.
pub fn global_instructions_path() -> PathBuf {
    std::env::var(INSTRUCTIONS_FILE_ENV)
        .map(|v| v.trim().to_string())
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_GLOBAL_INSTRUCTIONS))
}

/// The absolute path of the per-session instructions file for `name`:
/// `<sessions-dir>/<name>.md`. An explicit session `instructions_file`
/// overrides this; see [`per_session_path`].
pub fn session_instructions_path(name: &str) -> PathBuf {
    sessions_dir().join(format!("{name}.md"))
}

/// The absolute path of the per-project instructions file for a worker job on
/// `project`: `<sessions-dir>/<project>.md` (the per-session analog; there is
/// no per-job configuration, design 6.5).
pub fn project_instructions_path(project: &str) -> PathBuf {
    sessions_dir().join(format!("{project}.md"))
}

/// The per-session instructions directory:
/// `KUBEADP_INSTRUCTIONS_SESSIONS_DIR` if set (and not blank), else the
/// default `/etc/kubeadp-agent/instructions/sessions`.
pub fn sessions_dir() -> PathBuf {
    std::env::var(SESSIONS_DIR_ENV)
        .map(|v| v.trim().to_string())
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SESSIONS_INSTRUCTIONS_DIR))
}

/// The per-session instructions path for a session: an explicit
/// `instructions_file` wins, else `<sessions-dir>/<name>.md`.
pub fn per_session_path(explicit: Option<&Path>, name: &str) -> PathBuf {
    explicit
        .map(PathBuf::from)
        .unwrap_or_else(|| session_instructions_path(name))
}

/// Reads one instruction level, applying the design 6.2 semantics: `None`
/// when unconfigured, `Missing` for a configured-but-absent path,
/// `Unreadable` (never an error) for a path that exists but cannot be read,
/// `Content` otherwise. `path` is always the path that was examined.
pub fn read(path: &Path) -> ReadOutcome {
    if path.as_os_str().is_empty() {
        return ReadOutcome::None;
    }
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return ReadOutcome::Missing(path.to_path_buf());
        }
        Err(e) => {
            return ReadOutcome::Unreadable {
                path: path.to_path_buf(),
                reason: e.to_string(),
            }
        }
    };
    if !meta.is_file() {
        return ReadOutcome::Unreadable {
            path: path.to_path_buf(),
            reason: "not a regular file".to_string(),
        };
    }
    match std::fs::read_to_string(path) {
        Ok(text) => ReadOutcome::Content {
            path: path.to_path_buf(),
            text,
        },
        Err(e) => ReadOutcome::Unreadable {
            path: path.to_path_buf(),
            reason: e.to_string(),
        },
    }
}

/// Outcome of one materialisation call (a single run start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializeOutcome {
    /// Nothing materialised (no levels configured / readable).
    Noop,
    /// At least one level was (re)written.
    Written,
}

/// Materialises the full hierarchy for a run on an engine.
///
/// `engine` is one of `"claude"` or `"agy"` (anything else is treated as
/// `"claude"`, the historical driver). `home` is the effective run home
/// (the process `HOME` for a trusted run, the `<sandbox-home>` path the way
/// the sandbox exposes it for a sandboxed run). `workdir` is the run's
/// working directory. `global_text`/`per_session_text` are the already-read
/// hierarchy levels (see [`read`]); each is materialised when non-empty.
///
/// Returns `(outcome, log_lines)` where `log_lines` are the messages the
/// caller logs through its own channel (`log_event` for sessions,
/// `append_to_log_file` for jobs). Errors are never fatal: they become
/// `ERROR:` log lines exactly like the design requires.
pub fn materialize(
    engine: &str,
    home: &Path,
    workdir: &Path,
    global_text: Option<(PathBuf, String)>,
    per_session_text: Option<(PathBuf, String)>,
) -> (MaterializeOutcome, Vec<String>) {
    let mut log: Vec<String> = Vec::new();
    let mut written = false;
    if engine == "agy" {
        written |= materialize_agy(home, global_text, per_session_text, &mut log);
    } else {
        written |= materialize_claude(home, workdir, global_text, per_session_text, &mut log);
    }
    let outcome = if written {
        MaterializeOutcome::Written
    } else {
        MaterializeOutcome::Noop
    };
    (outcome, log)
}

/// Materialises the claude witness files: the global `~/.claude/CLAUDE.md`
/// and the per-session `CLAUDE.md` in `workdir`, which is excluded via
/// `.git/info/exclude` (tracked `CLAUDE.md` is never overwritten).
fn materialize_claude(
    home: &Path,
    workdir: &Path,
    global_text: Option<(PathBuf, String)>,
    per_session_text: Option<(PathBuf, String)>,
    log: &mut Vec<String>,
) -> bool {
    let mut written = false;
    if let Some((src, text)) = global_text {
        let target = home.join(".claude").join("CLAUDE.md");
        match write_atomic(&target, text.as_bytes()) {
            Ok(()) => {
                written = true;
                log.push(format!(
                    "instructions: claude global written: {} -> {}",
                    src.display(),
                    target.display()
                ));
            }
            Err(e) => log.push(format!(
                "ERROR: instructions: claude global write failed: {} (continuing without it)",
                e
            )),
        }
    }
    if let Some((src, text)) = per_session_text {
        let target = workdir.join("CLAUDE.md");
        match inject_entry(workdir, &target, text.as_bytes()) {
            Ok(()) => {
                written = true;
                log.push(format!(
                    "instructions: claude per-session written: {} -> {}",
                    src.display(),
                    target.display()
                ));
            }
            Err(e) => log.push(format!(
                "ERROR: instructions: claude per-session write failed: {} (continuing without it)",
                e
            )),
        }
    }
    written
}

/// Materialises the agy witness file: `<home>/.gemini/config/AGENTS.md`, the
/// global text first and the per-session text appended after it.
fn materialize_agy(
    home: &Path,
    global_text: Option<(PathBuf, String)>,
    per_session_text: Option<(PathBuf, String)>,
    log: &mut Vec<String>,
) -> bool {
    if global_text.is_none() && per_session_text.is_none() {
        return false;
    }
    let target = home.join(".gemini").join("config").join("AGENTS.md");
    let existing = std::fs::read_to_string(&target).unwrap_or_default();
    let mut combined = existing;
    for (src, text) in global_text.iter().chain(per_session_text.iter()) {
        if !text.is_empty() {
            if !combined.is_empty() && !combined.ends_with('\n') {
                combined.push('\n');
            }
            combined.push_str("\n## kubeadp-agent instructions (source: ");
            combined.push_str(&src.display().to_string());
            combined.push_str(")\n");
            combined.push_str(text);
            if !combined.ends_with('\n') {
                combined.push('\n');
            }
        }
    }
    match write_atomic(&target, combined.as_bytes()) {
        Ok(()) => {
            log.push(format!("instructions: agy written: {}", target.display()));
            true
        }
        Err(e) => {
            log.push(format!(
                "ERROR: instructions: agy write failed: {} (continuing without it)",
                e
            ));
            false
        }
    }
}

/// Writes `data` to `path` atomically (temp file + rename, mode 0644),
/// creating parent directories. Returns an error string on failure.
fn write_atomic(path: &Path, data: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {}", parent.display(), e))?;
    }
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        path.extension()
            .map(|e| e.to_string_lossy().to_string())
            .unwrap_or_else(|| "instructions".to_string()),
        std::process::id()
    ));
    let write = || -> Result<(), String> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| format!("failed to create temp file {}: {}", tmp.display(), e))?;
        file.write_all(data)
            .map_err(|e| format!("failed to write temp file {}: {}", tmp.display(), e))?;
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|e| format!("failed to set mode on {}: {}", tmp.display(), e))?;
        file.flush()
            .map_err(|e| format!("failed to flush temp file {}: {}", tmp.display(), e))?;
        drop(file);
        std::fs::rename(&tmp, path).map_err(|e| {
            format!(
                "failed to rename {} -> {}: {}",
                tmp.display(),
                path.display(),
                e
            )
        })
    };
    match write() {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// Injects the per-session entry into the workdir `CLAUDE.md`, applying the
/// git safety rules (never overwrite a tracked `CLAUDE.md`; exclude the
/// injected file via `.git/info/exclude`). A non-git `workdir` is written
/// directly.
fn inject_entry(workdir: &Path, target: &Path, data: &[u8]) -> Result<(), String> {
    let is_work_tree = git_in_workdir(workdir, &["rev-parse", "--is-inside-work-tree"])?
        .map(|s| s == "true")
        .unwrap_or(false);
    if is_work_tree {
        if git_in_workdir(workdir, &["ls-files", "--error-unmatch", "--", "CLAUDE.md"])?.is_some() {
            return Err(
                "CLAUDE.md is tracked in the repository; delete it from the repo before injecting"
                    .to_string(),
            );
        }
        if let Some(exclude) = exclude_file_path(workdir)? {
            ensure_exclude_entry(&exclude)?;
        }
    }
    if let Ok(existing) = std::fs::read(target) {
        if existing == data {
            return Ok(());
        }
    }
    write_atomic(target, data)
}

/// Runs `git <args>` in `workdir`, returning `Ok(Some(stdout))` on exit 0,
/// `Ok(None)` when git reports it is not inside a work tree, and `Err` on any
/// other failure.
fn git_in_workdir(workdir: &Path, args: &[&str]) -> Result<Option<String>, String> {
    let mut cmd = crate::core::git::hardened_git();
    cmd.arg("-C").arg(workdir);
    cmd.args(args);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());
    let output = cmd
        .output()
        .map_err(|e| format!("failed to run git in '{}': {}", workdir.display(), e))?;
    if output.status.success() {
        Ok(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_string(),
        ))
    } else {
        Ok(None)
    }
}

/// The absolute path of the repo's `info/exclude` file for `workdir`.
fn exclude_file_path(workdir: &Path) -> Result<Option<PathBuf>, String> {
    let out = git_in_workdir(workdir, &["rev-parse", "--git-path", "info/exclude"])?;
    let Some(raw) = out else {
        return Ok(None);
    };
    let p = PathBuf::from(&raw);
    let resolved = if p.is_absolute() { p } else { workdir.join(p) };
    Ok(Some(resolved))
}

/// Ensures the line `/CLAUDE.md` is present in the repository's
/// `info/exclude` file (idempotent, creates parents and the file itself).
fn ensure_exclude_entry(exclude: &Path) -> Result<(), String> {
    let contents = std::fs::read_to_string(exclude).unwrap_or_default();
    let has_entry = contents.lines().any(|l| l == "/CLAUDE.md");
    if has_entry {
        return Ok(());
    }
    if let Some(parent) = exclude.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {}", parent.display(), e))?;
    }
    let normalized = if contents.is_empty() || contents.ends_with('\n') {
        contents
    } else {
        format!("{}\n", contents)
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(exclude)
        .map_err(|e| format!("failed to open {}: {}", exclude.display(), e))?;
    file.write_all(format!("{}/CLAUDE.md\n", normalized).as_bytes())
        .map_err(|e| format!("failed to write {}: {}", exclude.display(), e))?;
    Ok(())
}

/// Materialises the instruction-file hierarchy for a worker job (design 6.5).
/// Jobs have no per-job configuration; the per-project analog is derived from
/// `meta.project`. Reads the levels (with the design 6.2 log semantics), calls
/// [`materialize`] with the engine-resolved home, and appends every resulting
/// line to the job's log file. Never a start failure.
pub fn job_materialize(
    config: &crate::core::config::Config,
    meta: &crate::core::job::JobMetadata,
    log_path: &Path,
) {
    let engine = meta.engine.as_deref().unwrap_or("claude");
    use crate::core::hindsight::append_to_log_file;
    let global = match read(&global_instructions_path()) {
        ReadOutcome::None => None,
        ReadOutcome::Content { path, text } => Some((path, text)),
        ReadOutcome::Missing(path) => {
            append_to_log_file(
                log_path,
                &format!(
                    "instructions: global file {} not present; continuing without it",
                    path.display()
                ),
            );
            None
        }
        ReadOutcome::Unreadable { path, reason } => {
            append_to_log_file(
                log_path,
                &format!(
                    "ERROR: instructions: global file {} cannot be read: {} (continuing without it)",
                    path.display(),
                    reason
                ),
            );
            None
        }
    };
    let per = match read(&project_instructions_path(&meta.project)) {
        ReadOutcome::None => None,
        ReadOutcome::Content { path, text } => Some((path, text)),
        ReadOutcome::Missing(path) => {
            append_to_log_file(
                log_path,
                &format!(
                    "instructions: per-project file {} not present; continuing without it",
                    path.display()
                ),
            );
            None
        }
        ReadOutcome::Unreadable { path, reason } => {
            append_to_log_file(
                log_path,
                &format!(
                    "ERROR: instructions: per-project file {} cannot be read: {} (continuing without it)",
                    path.display(),
                    reason
                ),
            );
            None
        }
    };
    // The job's home as the job child sees it: the launcher's process HOME
    // (the sandbox passes HOME through `env_allow`, so the witness lands at
    // exactly the path the engine reads). `sandbox_home` is only the sandbox's
    // writable-path grant, not the child's `$HOME`; it is the fallback when
    // HOME is unset.
    let home = std::env::var("HOME")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| config.sandbox_home.clone());
    let (_, lines) = materialize(engine, &home, &meta.worktree_path, global, per);
    for line in lines {
        append_to_log_file(log_path, &line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn unique_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kubeadp_instructions_{}_{}_{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn git_init(dir: &Path) {
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.email", "test@example.com"],
            vec!["config", "user.name", "test"],
        ] {
            let out = Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(&args)
                .output()
                .expect("git must be available");
            assert!(out.status.success(), "git {:?} failed", args);
        }
    }

    #[test]
    fn test_read_full_lifecycle() {
        let root = unique_temp_dir("read");
        let missing = root.join("missing.md");
        assert_eq!(read(&missing), ReadOutcome::Missing(missing.clone()));
        let dir = root.join("adir");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(matches!(
            read(&dir),
            ReadOutcome::Unreadable { ref path, .. } if *path == dir
        ));
        let file = root.join("f.md");
        std::fs::write(&file, "rule 1\n").unwrap();
        assert!(matches!(
            read(&file),
            ReadOutcome::Content { ref path, ref text } if *path == file && text == "rule 1\n"
        ));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_claude_materialize_writes_global_and_per_session_and_excludes() {
        let root = unique_temp_dir("claude");
        let home = root.join("home");
        let work = root.join("repo-workdir");
        std::fs::create_dir_all(&work).unwrap();
        git_init(&work);
        let (outcome, log) = materialize(
            "claude",
            &home,
            &work,
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            Some((PathBuf::from("/p.md"), "per rule\n".to_string())),
        );
        assert_eq!(outcome, MaterializeOutcome::Written);
        assert_eq!(
            std::fs::read_to_string(home.join(".claude/CLAUDE.md")).unwrap(),
            "global rule\n"
        );
        assert_eq!(
            std::fs::read_to_string(work.join("CLAUDE.md")).unwrap(),
            "per rule\n"
        );
        let exclude = exclude_file_path(&work).unwrap().unwrap();
        assert!(std::fs::read_to_string(&exclude)
            .unwrap()
            .lines()
            .any(|l| l == "/CLAUDE.md"));
        let status = Command::new("git")
            .arg("-C")
            .arg(&work)
            .args(["status", "--porcelain"])
            .output()
            .unwrap();
        assert_eq!(status.stdout, b"");
        assert!(log.iter().any(|l| l.contains("claude global written")));
        assert!(log.iter().any(|l| l.contains("claude per-session written")));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_claude_global_write_failure_is_error_line_not_fatal() {
        let root = unique_temp_dir("claude-fail");
        let home = root.join("home");
        std::fs::create_dir_all(home.join(".claude")).unwrap();
        // A directory at the target path makes the atomic rename fail even when
        // the test process is running as root (where mode 0o000 does not block).
        std::fs::create_dir(home.join(".claude/CLAUDE.md")).unwrap();

        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        let (outcome, log) = materialize(
            "claude",
            &home,
            &work,
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            None,
        );
        assert_eq!(outcome, MaterializeOutcome::Noop);
        assert!(log
            .iter()
            .any(|l| l.starts_with("ERROR: instructions: claude global write failed")));

        let _ = std::fs::remove_dir_all(home.join(".claude/CLAUDE.md"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_tracked_claude_md_not_overwritten() {
        let root = unique_temp_dir("tracked");
        let work = root.join("repo");
        std::fs::create_dir_all(&work).unwrap();
        git_init(&work);
        let tracked = work.join("CLAUDE.md");
        std::fs::write(&tracked, "original tracked content\n").unwrap();
        Command::new("git")
            .arg("-C")
            .arg(&work)
            .args(["add", "CLAUDE.md"])
            .status()
            .unwrap();
        Command::new("git")
            .arg("-C")
            .arg(&work)
            .args(["commit", "-q", "-m", "add"])
            .status()
            .unwrap();
        let home = root.join("home");
        let (outcome, log) = materialize(
            "claude",
            &home,
            &work,
            None,
            Some((PathBuf::from("/p.md"), "per rule\n".to_string())),
        );
        assert_eq!(outcome, MaterializeOutcome::Noop);
        assert_eq!(
            std::fs::read_to_string(&tracked).unwrap(),
            "original tracked content\n"
        );
        assert!(log
            .iter()
            .any(|l| l.contains("ERROR: instructions: claude per-session write failed")));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_appends_per_session_after_global() {
        let root = unique_temp_dir("agy");
        let home = root.join("home");
        let (outcome, log) = materialize(
            "agy",
            &home,
            &root.join("work"),
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            Some((PathBuf::from("/p.md"), "per rule\n".to_string())),
        );
        assert_eq!(outcome, MaterializeOutcome::Written);
        let combined = std::fs::read_to_string(home.join(".gemini/config/AGENTS.md")).unwrap();
        assert!(combined.contains("global rule"), "{combined}");
        assert!(combined.contains("per rule"), "{combined}");
        let gi = combined.find("global rule").unwrap();
        let pi = combined.find("per rule").unwrap();
        assert!(gi < pi, "per-session must follow global:\n{combined}");
        assert!(log.iter().any(|l| l.contains("agy written")));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_no_levels_is_noop() {
        let root = unique_temp_dir("agy-none");
        let home = root.join("home");
        let (outcome, log) = materialize("agy", &home, &root.join("work"), None, None);
        assert_eq!(outcome, MaterializeOutcome::Noop);
        assert!(!home.join(".gemini/config/AGENTS.md").exists());
        assert!(log.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_non_git_workdir_gets_entry() {
        let root = unique_temp_dir("nongit");
        let work = root.join("plain");
        std::fs::create_dir_all(&work).unwrap();
        let home = root.join("home");
        let (outcome, _log) = materialize(
            "claude",
            &home,
            &work,
            None,
            Some((PathBuf::from("/p.md"), "no git here\n".to_string())),
        );
        assert_eq!(outcome, MaterializeOutcome::Written);
        assert_eq!(
            std::fs::read_to_string(work.join("CLAUDE.md")).unwrap(),
            "no git here\n"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_path_resolvers_honor_env() {
        let _guard = crate::core::lock_test_env();
        std::env::remove_var(INSTRUCTIONS_FILE_ENV);
        std::env::remove_var(SESSIONS_DIR_ENV);
        assert_eq!(
            global_instructions_path(),
            PathBuf::from(DEFAULT_GLOBAL_INSTRUCTIONS)
        );
        assert_eq!(
            sessions_dir(),
            PathBuf::from(DEFAULT_SESSIONS_INSTRUCTIONS_DIR)
        );
        assert_eq!(
            session_instructions_path("sess"),
            PathBuf::from(DEFAULT_SESSIONS_INSTRUCTIONS_DIR).join("sess.md")
        );
        std::env::set_var(INSTRUCTIONS_FILE_ENV, "/tmp/glob.md");
        std::env::set_var(SESSIONS_DIR_ENV, "/tmp/sess");
        assert_eq!(global_instructions_path(), PathBuf::from("/tmp/glob.md"));
        assert_eq!(sessions_dir(), PathBuf::from("/tmp/sess"));
        assert_eq!(
            session_instructions_path("s"),
            PathBuf::from("/tmp/sess/s.md")
        );
        assert_eq!(
            per_session_path(Some(Path::new("/explicit.md")), "s"),
            PathBuf::from("/explicit.md")
        );
        assert_eq!(per_session_path(None, "s"), PathBuf::from("/tmp/sess/s.md"));
        assert_eq!(
            project_instructions_path("homelab"),
            PathBuf::from("/tmp/sess/homelab.md")
        );
    }
}

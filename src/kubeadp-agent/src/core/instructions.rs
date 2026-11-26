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
//!   file that already holds the MCP configuration),
//! - opencode reads the global `~/.config/opencode/AGENTS.md` and the
//!   per-session `AGENTS.md` in the run's working directory (KA-4: opencode
//!   auto-loads `AGENTS.md` walking up from the working directory, and also
//!   loads the global user `AGENTS.md`).
//!
//! The engine files may also carry *user* content that is not managed by the
//! agent (the agy `AGENTS.md` doubles as the MCP configuration file, and the
//! worker's agy home is persistent and shared by every job). The agent
//! therefore writes only a *managed block* — delimited by the
//! [`BLOCK_BEGIN`]/[`BLOCK_END`] marker lines — and every run *replaces* that
//! block (or removes it when no level exists), leaving all other content of
//! the file untouched. This keeps the materialisation idempotent (the same
//! inputs leave the file byte-identical) and project-safe (two different
//! projects in sequence never see each other's text).
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
//! Only the managed block (see [`BLOCK_BEGIN`]) is rewritten; user content
//! outside it is preserved.
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

/// Marker delimiting the managed block written into the engine witness files
/// (the agy `AGENTS.md` and, when present, a `CLAUDE.md` that already holds
/// user content). Every materialisation replaces the region between the
/// markers (inclusive) or removes it entirely; content outside the markers is
/// never touched.
pub const BLOCK_BEGIN: &str = "<!-- kubeadp-agent instructions: begin -->";
/// Closing marker of the managed block; see [`BLOCK_BEGIN`].
pub const BLOCK_END: &str = "<!-- kubeadp-agent instructions: end -->";

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
    /// Nothing materialised (no change to any witness file: no levels and no
    /// stale managed block to remove).
    Noop,
    /// At least one witness file was changed (a managed block written or
    /// removed).
    Written,
}

/// Materialises the full hierarchy for a run on an engine.
///
/// `engine` is one of `"claude"` or `"agy"` (anything else is treated as
/// `"claude"`, the historical driver). `home` is the effective run home
/// (the process `HOME` for a trusted run, the `<sandbox-home>` path the way
/// the sandbox exposes it for a sandboxed run). `workdir` is the run's
/// working directory. `global_text`/`per_session_text` are the already-read
/// hierarchy levels (see [`read`]).
///
/// Each witness file is written as a **managed block** (see [`BLOCK_BEGIN`]):
/// the levels present are rendered into the block (in hierarchy order), the
/// block is replaced on every run and removed when no level is present, and
/// any user content outside the block is left untouched. The materialisation
/// is therefore idempotent and project-safe.
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
    let core_config = crate::core::config::Config::load().unwrap_or_default();
    let registry = crate::engine::EngineRegistry::with_builtin(&core_config);
    let written = if let Ok(eng) = registry.resolve(crate::engine::RunKind::Job, Some(engine), None)
    {
        eng.materialize_instructions(home, workdir, global_text, per_session_text, &mut log)
    } else if let Ok(eng) = registry.resolve(crate::engine::RunKind::Session, Some(engine), None) {
        eng.materialize_instructions(home, workdir, global_text, per_session_text, &mut log)
    } else {
        materialize_claude(home, workdir, global_text, per_session_text, &mut log)
    };
    let outcome = if written {
        MaterializeOutcome::Written
    } else {
        MaterializeOutcome::Noop
    };
    (outcome, log)
}

/// Materialises the claude witness files: the global `~/.claude/CLAUDE.md` and
/// the per-session `CLAUDE.md` in `workdir`. Each holds a managed block (see
/// [`BLOCK_BEGIN`]) so user content outside the block is kept; the workdir
/// file is excluded via `.git/info/exclude` (a tracked `CLAUDE.md` is never
/// touched).
pub fn materialize_claude(
    home: &Path,
    workdir: &Path,
    global_text: Option<(PathBuf, String)>,
    per_session_text: Option<(PathBuf, String)>,
    log: &mut Vec<String>,
) -> bool {
    let mut written = false;
    // Global level -> ~/.claude/CLAUDE.md (a managed block; user content outside
    // the block is kept, the block is replaced each run or removed when absent).
    let global_target = home.join(".claude").join("CLAUDE.md");
    let global_src = global_text.as_ref().map(|(s, _)| s.display().to_string());
    let global_inner = block_content(render_inner(global_text.iter()));
    let global_set = global_inner.is_some();
    match write_managed(&global_target, global_inner) {
        Ok(true) => {
            written = true;
            log.push(if global_set {
                format!(
                    "instructions: claude global written: {} -> {}",
                    global_src.as_deref().unwrap_or_default(),
                    global_target.display()
                )
            } else {
                format!(
                    "instructions: claude global managed block removed: {}",
                    global_target.display()
                )
            });
        }
        Ok(false) => {}
        Err(e) => log.push(format!(
            "ERROR: instructions: claude global write failed: {} (continuing without it)",
            e
        )),
    }
    // Per-session level -> <workdir>/CLAUDE.md (managed block, git-excluded; a
    // tracked CLAUDE.md is never touched).
    let per_target = workdir.join("CLAUDE.md");
    let per_src = per_session_text
        .as_ref()
        .map(|(s, _)| s.display().to_string());
    let per_inner = block_content(render_inner(per_session_text.iter()));
    let per_set = per_inner.is_some();
    match inject_entry(workdir, &per_target, &per_inner) {
        Ok(InjectOutcome::Written) => {
            written = true;
            log.push(if per_set {
                format!(
                    "instructions: claude per-session written: {} -> {}",
                    per_src.as_deref().unwrap_or_default(),
                    per_target.display()
                )
            } else {
                format!(
                    "instructions: claude per-session managed block removed: {}",
                    per_target.display()
                )
            });
        }
        Ok(InjectOutcome::Unchanged) => {}
        Ok(InjectOutcome::Tracked) => {
            log.push(format!(
                "ERROR: instructions: per-session CLAUDE.md {} is tracked in the repository; not touched",
                per_target.display()
            ));
        }
        Err(e) => log.push(format!(
            "ERROR: instructions: claude per-session write failed: {} (continuing without it)",
            e
        )),
    }
    written
}

/// Materialises the agy witness file: `<home>/.gemini/config/AGENTS.md`. The
/// managed block holds the global text first and the per-session text after
/// it; the file may carry other user content (it doubles as the MCP
/// configuration) which is left untouched.
pub fn materialize_agy(
    home: &Path,
    global_text: Option<(PathBuf, String)>,
    per_session_text: Option<(PathBuf, String)>,
    log: &mut Vec<String>,
) -> bool {
    let target = home.join(".gemini").join("config").join("AGENTS.md");
    let inner = block_content(render_inner(
        global_text.iter().chain(per_session_text.iter()),
    ));
    let set = inner.is_some();
    match write_managed(&target, inner) {
        Ok(true) => {
            log.push(if set {
                format!("instructions: agy written: {}", target.display())
            } else {
                format!(
                    "instructions: agy managed block removed: {}",
                    target.display()
                )
            });
            true
        }
        Ok(false) => false,
        Err(e) => {
            log.push(format!(
                "ERROR: instructions: agy write failed: {} (continuing without it)",
                e
            ));
            false
        }
    }
}

/// Materialises the opencode witness files: the global `~/.config/opencode/
/// AGENTS.md` and the per-session `AGENTS.md` in the run's working directory
/// (KA-4, SPEC §4.10 targets). opencode auto-loads `AGENTS.md` walking up
/// from the working directory and also loads the global user `AGENTS.md`, so
/// the two levels map to the same global -> per-session hierarchy the other
/// engines use. Each holds a managed block (see [`BLOCK_BEGIN`]) so user
/// content outside the block is kept; the per-session file is git-excluded
/// like the claude `CLAUDE.md` (a tracked `AGENTS.md` is never touched).
pub fn materialize_opencode(
    home: &Path,
    workdir: &Path,
    global_text: Option<(PathBuf, String)>,
    per_session_text: Option<(PathBuf, String)>,
    log: &mut Vec<String>,
) -> bool {
    let mut written = false;
    // Global level -> ~/.config/opencode/AGENTS.md (a managed block).
    let global_target = home.join(".config").join("opencode").join("AGENTS.md");
    let global_src = global_text.as_ref().map(|(s, _)| s.display().to_string());
    let global_inner = block_content(render_inner(global_text.iter()));
    let global_set = global_inner.is_some();
    match write_managed(&global_target, global_inner) {
        Ok(true) => {
            written = true;
            log.push(if global_set {
                format!(
                    "instructions: opencode global written: {} -> {}",
                    global_src.as_deref().unwrap_or_default(),
                    global_target.display()
                )
            } else {
                format!(
                    "instructions: opencode global managed block removed: {}",
                    global_target.display()
                )
            });
        }
        Ok(false) => {}
        Err(e) => log.push(format!(
            "ERROR: instructions: opencode global write failed: {} (continuing without it)",
            e
        )),
    }
    // Per-session level -> <workdir>/AGENTS.md (managed block, git-excluded; a
    // tracked AGENTS.md is never touched).
    let per_target = workdir.join("AGENTS.md");
    let per_src = per_session_text
        .as_ref()
        .map(|(s, _)| s.display().to_string());
    let per_inner = block_content(render_inner(per_session_text.iter()));
    let per_set = per_inner.is_some();
    match inject_entry(workdir, &per_target, &per_inner) {
        Ok(InjectOutcome::Written) => {
            written = true;
            log.push(if per_set {
                format!(
                    "instructions: opencode per-session written: {} -> {}",
                    per_src.as_deref().unwrap_or_default(),
                    per_target.display()
                )
            } else {
                format!(
                    "instructions: opencode per-session managed block removed: {}",
                    per_target.display()
                )
            });
        }
        Ok(InjectOutcome::Unchanged) => {}
        Ok(InjectOutcome::Tracked) => {
            log.push(format!(
                "ERROR: instructions: per-session AGENTS.md {} is tracked in the repository; not touched",
                per_target.display()
            ));
        }
        Err(e) => log.push(format!(
            "ERROR: instructions: opencode per-session write failed: {} (continuing without it)",
            e
        )),
    }
    written
}

/// Renders the inner text of the managed block for the given hierarchy levels
/// (in order): one `## kubeadp-agent instructions (source: <path>)` section per
/// non-empty level. Empty when no level contributes content.
fn render_inner<'a>(levels: impl Iterator<Item = &'a (PathBuf, String)>) -> String {
    let mut out = String::new();
    for (src, text) in levels {
        if text.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("## kubeadp-agent instructions (source: ");
        out.push_str(&src.display().to_string());
        out.push_str(")\n");
        out.push_str(text);
        if !out.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

/// The inner content for the managed block, or `None` when no level
/// contributes content — `None` means "remove the block" rather than write it.
fn block_content(inner: String) -> Option<String> {
    if inner.is_empty() {
        None
    } else {
        Some(inner)
    }
}

/// Renders the full managed block (markers + inner text) for `inner`; `inner`
/// is normalised to end with a newline.
fn render_block(inner: &str) -> String {
    let inner = if inner.ends_with('\n') {
        inner.to_string()
    } else {
        format!("{inner}\n")
    };
    format!("{BLOCK_BEGIN}\n{inner}{BLOCK_END}\n")
}

/// The byte range `(start, end)` of the managed block inside `existing`, or
/// `None` when the file holds no managed block.
fn managed_block_span(existing: &str) -> Option<(usize, usize)> {
    let begin = existing.find(BLOCK_BEGIN)?;
    let end_begin = begin + BLOCK_BEGIN.len();
    let rel_end = existing[end_begin..].find(BLOCK_END)?;
    Some((begin, end_begin + rel_end + BLOCK_END.len()))
}

/// Splices the managed block into `existing`: it is replaced with `new_block`
/// (a full block including the marker lines) or removed when `new_block` is
/// `None`. Content outside the block is preserved, and the surrounding
/// whitespace is normalised to a single newline separator so that repeated
/// runs with the same inputs are byte-identical.
fn splice_block(existing: &str, new_block: Option<&str>) -> String {
    // No managed block present.
    let Some((start, end)) = managed_block_span(existing) else {
        return match new_block {
            // Nothing to write and nothing to remove: leave the file untouched.
            None => existing.to_string(),
            Some(block) => {
                if existing.is_empty() {
                    block.to_string()
                } else {
                    let base = if existing.ends_with('\n') {
                        existing.to_string()
                    } else {
                        format!("{existing}\n")
                    };
                    format!("{base}{block}")
                }
            }
        };
    };
    // A block is present. `before`/`after` are the content outside it; the
    // whitespace adjoing the block is normalised to a single newline, which is
    // what keeps repeated runs with the same inputs byte-identical.
    let before = existing[..start].trim_end_matches('\n');
    let after = existing[end..].trim_start_matches('\n');
    let mut out = String::new();
    if !before.is_empty() {
        out.push_str(before);
        out.push('\n');
    }
    if let Some(block) = new_block {
        out.push_str(block);
        if !after.is_empty() {
            out.push_str(after);
        }
    } else {
        out.push_str(after);
    }
    out
}

/// Reads the file at `path` (if present), splices the managed block in (or
/// removes it when `inner` is `None`), and writes the result atomically,
/// creating parent directories. Returns `Ok(true)` when the file was
/// (re)written, `Ok(false)` when nothing changed (the idempotent no-op), and
/// `Err` on a write failure. `inner` is the managed-block content (see
/// [`block_content`]); `None` removes any existing block and never creates a
/// file that does not exist.
fn write_managed(path: &Path, inner: Option<String>) -> Result<bool, String> {
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let new_block = inner.as_ref().map(|i| render_block(i));
    let out = splice_block(&existing, new_block.as_deref());
    if out == existing {
        return Ok(false);
    }
    write_atomic(path, out.as_bytes())?;
    Ok(true)
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

/// The result of applying the managed block to the workdir `CLAUDE.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InjectOutcome {
    /// The managed block was (re)written.
    Written,
    /// The file already held the correct content (idempotent no-op).
    Unchanged,
    /// The file is tracked in the workdir's git repository and was left
    /// untouched (neither injected nor removed).
    Tracked,
}

/// Applies the managed block to the workdir witness file (the claude
/// `CLAUDE.md` or the opencode `AGENTS.md`), keeping the git safety rules (a
/// tracked witness file is never touched; the injected file is excluded via
/// `.git/info/exclude`). A non-git `workdir` is written directly.
///
/// `inner` is the managed-block content (see [`block_content`]); `None`
/// removes any existing block. `Err` is returned only for a write failure.
fn inject_entry(
    workdir: &Path,
    target: &Path,
    inner: &Option<String>,
) -> Result<InjectOutcome, String> {
    let entry_name = target
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "CLAUDE.md".to_string());
    let is_work_tree = git_in_workdir(workdir, &["rev-parse", "--is-inside-work-tree"])?
        .map(|s| s == "true")
        .unwrap_or(false);
    if is_work_tree {
        // Never touch a tracked witness file: when one is tracked the file is
        // owned by the repository, so neither the block is injected nor
        // removed (the file is left byte-identical).
        if git_in_workdir(workdir, &["ls-files", "--error-unmatch", "--", &entry_name])?.is_some() {
            return Ok(InjectOutcome::Tracked);
        }
        // Only maintain the git-exclude entry when we actually write the
        // block; a removal must not touch the repository's state.
        if inner.is_some() {
            if let Some(exclude) = exclude_file_path(workdir)? {
                ensure_exclude_entry(&exclude, &entry_name)?;
            }
        }
    }
    let existing = std::fs::read_to_string(target).unwrap_or_default();
    let new_block = inner.as_ref().map(|i| render_block(i));
    let out = splice_block(&existing, new_block.as_deref());
    if out == existing {
        return Ok(InjectOutcome::Unchanged);
    }
    write_atomic(target, out.as_bytes())?;
    Ok(InjectOutcome::Written)
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

/// Ensures the line `/CLAUDE.md` (or the given witness entry name) is present
/// in the repository's `info/exclude` file (idempotent, creates parents and
/// the file itself).
fn ensure_exclude_entry(exclude: &Path, entry_name: &str) -> Result<(), String> {
    let contents = std::fs::read_to_string(exclude).unwrap_or_default();
    let has_entry = contents.lines().any(|l| l == format!("/{entry_name}"));
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
    file.write_all(format!("{normalized}/{entry_name}\n").as_bytes())
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

    /// The managed-block content of a file (between the marker lines), or
    /// `None` when the file holds no managed block.
    fn block_text(file: &str) -> Option<&str> {
        let begin = file.find(BLOCK_BEGIN)?;
        let end_begin = begin + BLOCK_BEGIN.len();
        let rel_end = file[end_begin..].find(BLOCK_END)?;
        Some(&file[end_begin..end_begin + rel_end])
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
        let gfile = std::fs::read_to_string(home.join(".claude/CLAUDE.md")).unwrap();
        let gblock = block_text(&gfile).expect("global managed block");
        assert!(gblock.contains("global rule"), "global block: {gblock}");
        assert!(gblock.contains("source: /g.md"), "global block: {gblock}");
        let pfile = std::fs::read_to_string(work.join("CLAUDE.md")).unwrap();
        let pblock = block_text(&pfile).expect("per-session managed block");
        assert!(pblock.contains("per rule"), "per-session block: {pblock}");
        assert!(
            pblock.contains("source: /p.md"),
            "per-session block: {pblock}"
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
            .any(|l| l.contains("is tracked in the repository")));
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
        let file = std::fs::read_to_string(work.join("CLAUDE.md")).unwrap();
        let block = block_text(&file).expect("managed block present");
        assert!(block.contains("no git here"), "block: {block}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_idempotent_same_inputs_byte_identical() {
        let root = unique_temp_dir("agy-idem");
        let home = root.join("home");
        let work = root.join("work");
        let levels = (
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            Some((PathBuf::from("/p.md"), "per rule\n".to_string())),
        );
        for _ in 0..3 {
            materialize("agy", &home, &work, levels.0.clone(), levels.1.clone());
        }
        let f = home.join(".gemini/config/AGENTS.md");
        let first = std::fs::read(&f).unwrap();
        // Two more runs with identical inputs must leave the file byte-identical.
        materialize("agy", &home, &work, levels.0.clone(), levels.1.clone());
        let second = std::fs::read(&f).unwrap();
        assert_eq!(first, second, "same inputs must be byte-identical");
        assert_eq!(
            std::fs::read_to_string(&f).unwrap(),
            format!(
                "{BLOCK_BEGIN}\n## kubeadp-agent instructions (source: /g.md)\nglobal rule\n\n## kubeadp-agent instructions (source: /p.md)\nper rule\n{BLOCK_END}\n"
            )
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_project_switch_no_cross_leak() {
        let root = unique_temp_dir("agy-switch");
        let home = root.join("home");
        let work = root.join("work");
        // Project A runs with its own per-project text.
        materialize(
            "agy",
            &home,
            &work,
            Some((PathBuf::from("/g.md"), "shared global\n".to_string())),
            Some((
                PathBuf::from("/proj-a.md"),
                "PROJECT-A-SECRET\n".to_string(),
            )),
        );
        // Project B runs with a different per-project text; the shared
        // persistent agy home must now show B's text and none of A's.
        materialize(
            "agy",
            &home,
            &work,
            Some((PathBuf::from("/g.md"), "shared global\n".to_string())),
            Some((
                PathBuf::from("/proj-b.md"),
                "PROJECT-B-SECRET\n".to_string(),
            )),
        );
        let text = std::fs::read_to_string(home.join(".gemini/config/AGENTS.md")).unwrap();
        assert!(text.contains("PROJECT-B-SECRET"), "expected B: {text}");
        assert!(
            !text.contains("PROJECT-A-SECRET"),
            "A leaked into B: {text}"
        );
        assert!(text.contains("shared global"), "{text}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_preserves_foreign_content_and_replaces_block() {
        let root = unique_temp_dir("agy-foreign");
        let home = root.join("home");
        let f = home.join(".gemini/config/AGENTS.md");
        // Pre-existing user content (the file doubles as the MCP config).
        let foreign = "mcpServers:\n  my-tool:\n    cmd: 'x'\nuser note (keep me)\n";
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(&f, foreign).unwrap();
        materialize(
            "agy",
            &home,
            &root.join("work"),
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            Some((PathBuf::from("/p.md"), "per rule\n".to_string())),
        );
        let after = std::fs::read_to_string(&f).unwrap();
        assert!(after.starts_with(foreign), "foreign prefix lost: {after}");
        assert!(
            after.contains("global rule") && after.contains("per rule"),
            "{after}"
        );
        // Replacing the block with different content keeps the foreign part.
        materialize(
            "agy",
            &home,
            &root.join("work"),
            None,
            Some((PathBuf::from("/p2.md"), "per rule two\n".to_string())),
        );
        let after2 = std::fs::read_to_string(&f).unwrap();
        assert!(
            after2.starts_with(foreign),
            "foreign prefix lost on replace: {after2}"
        );
        assert!(
            after2.contains("per rule two") && !after2.contains("global rule"),
            "{after2}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_no_levels_removes_block_keeps_foreign() {
        let root = unique_temp_dir("agy-remove");
        let home = root.join("home");
        let f = home.join(".gemini/config/AGENTS.md");
        let foreign = "mcpServers: {}\n";
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(&f, foreign).unwrap();
        // Run 1: with a level, the block is appended after the foreign content.
        materialize(
            "agy",
            &home,
            &root.join("work"),
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            None,
        );
        let with = std::fs::read_to_string(&f).unwrap();
        assert!(
            with.starts_with(foreign) && with.contains("global rule"),
            "{with}"
        );
        // Run 2: no levels -> the block is removed, foreign content survives.
        let (outcome, _log) = materialize("agy", &home, &root.join("work"), None, None);
        assert_eq!(outcome, MaterializeOutcome::Written);
        assert_eq!(std::fs::read_to_string(&f).unwrap(), foreign);
        // A third run (still no levels) is a no-op and byte-identical.
        let (outcome2, _log2) = materialize("agy", &home, &root.join("work"), None, None);
        assert_eq!(outcome2, MaterializeOutcome::Noop);
        assert_eq!(std::fs::read_to_string(&f).unwrap(), foreign);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_agy_no_levels_no_file_is_noop() {
        let root = unique_temp_dir("agy-remove-absent");
        let home = root.join("home");
        let f = home.join(".gemini/config/AGENTS.md");
        let (outcome, _log) = materialize("agy", &home, &root.join("work"), None, None);
        assert_eq!(outcome, MaterializeOutcome::Noop);
        assert!(!f.exists(), "no levels must not create the file");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_claude_global_preserves_foreign_content() {
        let root = unique_temp_dir("claude-foreign");
        let home = root.join("home");
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        let g = home.join(".claude/CLAUDE.md");
        std::fs::create_dir_all(g.parent().unwrap()).unwrap();
        let foreign = "# my user notes\nkeep this\n";
        std::fs::write(&g, foreign).unwrap();
        materialize(
            "claude",
            &home,
            &work,
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            None,
        );
        let after = std::fs::read_to_string(&g).unwrap();
        assert!(after.starts_with(foreign), "foreign prefix lost: {after}");
        assert!(after.contains("global rule"), "{after}");
        // Idempotent: same inputs again leave the file byte-identical.
        let before = std::fs::read(&g).unwrap();
        materialize(
            "claude",
            &home,
            &work,
            Some((PathBuf::from("/g.md"), "global rule\n".to_string())),
            None,
        );
        assert_eq!(before, std::fs::read(&g).unwrap());
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
            project_instructions_path("project-a"),
            PathBuf::from("/tmp/sess/project-a.md")
        );
    }

    #[test]
    fn test_instructions_hierarchy_full_lifecycle_and_invariants() {
        let root = unique_temp_dir("hierarchy-invariants");
        let home = root.join("home");
        let work = root.join("repo-workdir");
        std::fs::create_dir_all(&work).unwrap();
        git_init(&work);

        // Pre-create project's AGENTS.md in repo root
        let repo_agents = work.join("AGENTS.md");
        let original_repo_agents = "# User Project Docs\nDo not modify.\n";
        std::fs::write(&repo_agents, original_repo_agents).unwrap();

        // 1. Files exist -> global + per-session text materialised into witness files
        let (outcome1, _log1) = materialize(
            "claude",
            &home,
            &work,
            Some((PathBuf::from("/global.md"), "GLOBAL-RULE\n".to_string())),
            Some((PathBuf::from("/per.md"), "PER-RULE\n".to_string())),
        );
        assert_eq!(outcome1, MaterializeOutcome::Written);
        let gfile = std::fs::read_to_string(home.join(".claude/CLAUDE.md")).unwrap();
        assert!(gfile.contains("GLOBAL-RULE"));
        let pfile = std::fs::read_to_string(work.join("CLAUDE.md")).unwrap();
        assert!(pfile.contains("PER-RULE"));
        // Project's AGENTS.md is untouched
        assert_eq!(
            std::fs::read_to_string(&repo_agents).unwrap(),
            original_repo_agents
        );

        // For agy: global + per-session text combined in ~/.gemini/config/AGENTS.md
        let (outcome_agy, _log_agy) = materialize(
            "agy",
            &home,
            &work,
            Some((PathBuf::from("/global.md"), "GLOBAL-RULE\n".to_string())),
            Some((PathBuf::from("/per.md"), "PER-RULE\n".to_string())),
        );
        assert_eq!(outcome_agy, MaterializeOutcome::Written);
        let agy_agents = std::fs::read_to_string(home.join(".gemini/config/AGENTS.md")).unwrap();
        assert!(agy_agents.contains("GLOBAL-RULE"));
        assert!(agy_agents.contains("PER-RULE"));
        assert_eq!(
            std::fs::read_to_string(&repo_agents).unwrap(),
            original_repo_agents
        );

        // 2. Files do not exist (None / Missing) -> no error, witness managed block removed/noop
        let (outcome2, _log2) = materialize("claude", &home, &work, None, None);
        assert_eq!(outcome2, MaterializeOutcome::Written); // removed previously written block
        let pfile_after = std::fs::read_to_string(work.join("CLAUDE.md")).unwrap();
        assert_eq!(pfile_after, "");
        assert_eq!(
            std::fs::read_to_string(&repo_agents).unwrap(),
            original_repo_agents
        );

        // Missing file on read produces ReadOutcome::Missing (logged once as info line)
        let missing_path = root.join("nonexistent.md");
        assert_eq!(
            read(&missing_path),
            ReadOutcome::Missing(missing_path.clone())
        );

        // 3. Existing but unreadable file produces ReadOutcome::Unreadable (logged as error line)
        let unreadable_dir = root.join("unreadable-dir.md");
        std::fs::create_dir_all(&unreadable_dir).unwrap();
        let unreadable_outcome = read(&unreadable_dir);
        assert!(matches!(
            unreadable_outcome,
            ReadOutcome::Unreadable { ref path, ref reason } if *path == unreadable_dir && reason == "not a regular file"
        ));

        // 4. In agy job, verify repo workdir never gets CLAUDE.md created, repo AGENTS.md untouched
        let agy_work = root.join("agy-work");
        std::fs::create_dir_all(&agy_work).unwrap();
        git_init(&agy_work);
        let agy_repo_agents = agy_work.join("AGENTS.md");
        std::fs::write(&agy_repo_agents, "# AGY Repo Docs\n").unwrap();
        materialize(
            "agy",
            &home,
            &agy_work,
            Some((PathBuf::from("/global.md"), "RULE-A\n".to_string())),
            Some((PathBuf::from("/per.md"), "RULE-B\n".to_string())),
        );
        assert!(!agy_work.join("CLAUDE.md").exists());
        assert_eq!(
            std::fs::read_to_string(&agy_repo_agents).unwrap(),
            "# AGY Repo Docs\n"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}

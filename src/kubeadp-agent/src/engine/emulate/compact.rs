//! Compact emulation: summarise-then-restart (design HL-91 E4).
//!
//! A native compact (claude PTY's `/compact`) rewrites the harness's own
//! context window in place. Engines without a native compact get the
//! emulated path: the emulation layer summarises what the run has done so
//! far (from the run's own git worktree and checkpoint history), writes a
//! compact record file next to the worktree, and restarts the run through the
//! wrapped engine with a continuation prompt that carries the summary.
//!
//! The summary is derived entirely from the run's own repository: the commit
//! history on the run's branch since its base and the current uncommitted
//! worktree state, using the same hardened-git helpers as [`crate::core::git`].
//! No engine process is consulted -- the summarised context is the worktree.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::core::reaper::output_tracked;
use crate::engine::run::Run;

/// The file `compact` writes next to the worktree recording the emulated
/// compaction (summary + the continuation prompt it produced).
pub const COMPACT_META_FILE: &str = ".engine-compact.json";

/// The continuation prompt marker: whatever the engine receives after a
/// compaction is framed as the compacted context, not a new task.
pub const COMPACT_PROMPT_PREFIX: &str =
    "[engine compacted context follows; continue the same task from this summary]";

/// The result of an emulated compaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CompactSummary {
    /// The human/machine-readable summary of the run's work so far.
    pub summary: String,
    /// The continuation prompt to feed the wrapped engine on restart.
    pub next_prompt: String,
    /// The checkpoint sha the summary was taken at, when one exists.
    pub checkpoint: Option<String>,
}

/// The path of the compact record for `run`.
pub fn compact_meta_path(run: &Run) -> PathBuf {
    run.workdir.join(COMPACT_META_FILE)
}

fn git(workdir: &Path) -> std::process::Command {
    let mut cmd = crate::core::git::hardened_git();
    cmd.arg("-C").arg(workdir);
    cmd
}

/// Reads up to `limit` recent commit subjects from the run's worktree
/// (`git log --oneline`), newest first. Returns an empty list when the
/// worktree has no commits yet.
pub fn recent_commits(workdir: &Path, limit: usize) -> Result<Vec<String>> {
    let output = output_tracked(git(workdir).args(["log", "--oneline", "-n", &limit.to_string()]))
        .with_context(|| format!("git log failed in {}", workdir.display()))?;
    if !output.status.success() {
        // A worktree with no commits yet makes `git log` fail ("does not have
        // any commits yet"); that is an empty history for the summary, not an
        // error.
        if worktree_has_ref(workdir) {
            return Err(anyhow::Error::msg(format!(
                "git log failed in {}: {}",
                workdir.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

fn worktree_has_ref(workdir: &Path) -> bool {
    let out = output_tracked(git(workdir).args(["rev-parse", "--verify", "--quiet", "HEAD"]));
    out.map(|o| o.status.success()).unwrap_or(false)
}

/// The worktree's uncommitted state summary (`git status --porcelain` is
/// non-empty when there are changes), one short line per change.
pub fn worktree_changes(workdir: &Path, limit: usize) -> Result<Vec<String>> {
    let output =
        output_tracked(git(workdir).args(["status", "--porcelain", "--untracked-files=normal"]))
            .with_context(|| format!("git status failed in {}", workdir.display()))?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .take(limit)
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

/// Produces the emulated compact summary for `run` and writes the compact
/// record file. Returns the summary (including the next prompt).
pub fn summarise_and_write(run: &Run, restarted_prompt: Option<&str>) -> Result<CompactSummary> {
    let checkpoints = crate::engine::emulate::revert::checkpoint_sha(run)?;
    let commits = recent_commits(&run.workdir, 30)?;
    let changes = worktree_changes(&run.workdir, 50)?;

    let mut lines: Vec<String> = Vec::new();
    if let Some(ck) = &checkpoints {
        lines.push(format!("checkpoint: {ck}"));
    }
    if commits.is_empty() {
        lines.push("history: (no commits yet)".to_string());
    } else {
        lines.push(format!("history ({} commits):", commits.len()));
        for c in &commits {
            lines.push(format!("  {c}"));
        }
    }
    if changes.is_empty() {
        lines.push("worktree: clean".to_string());
    } else {
        lines.push(format!("worktree ({} changed entries):", changes.len()));
        for c in &changes {
            lines.push(format!("  {c}"));
        }
    }
    let summary = lines.join("\n");

    let continuation = restarted_prompt.unwrap_or("continue").trim();
    let next_prompt = format!("{COMPACT_PROMPT_PREFIX}\n\n{summary}\n\n{continuation}");

    let meta = serde_json::json!({
        "summarised_at": chrono::Utc::now().to_rfc3339(),
        "checkpoint": checkpoints,
        "summary": summary,
        "next_prompt": next_prompt,
    });
    let path = compact_meta_path(run);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(&meta)?)
        .with_context(|| format!("failed to write compact record {}", path.display()))?;

    Ok(CompactSummary {
        summary,
        next_prompt,
        checkpoint: checkpoints,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::run::{Run, RunKind};

    fn init_repo(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        let out = output_tracked(
            crate::core::git::hardened_git()
                .arg("init")
                .arg("-b")
                .arg("main")
                .arg(dir),
        )
        .unwrap();
        assert!(out.status.success());
        std::process::Command::new("git")
            .args(["-C", dir.to_string_lossy().as_ref()])
            .args(["config", "user.name", "compact-test"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args(["-C", dir.to_string_lossy().as_ref()])
            .args(["config", "user.email", "compact@example.com"])
            .status()
            .unwrap();
    }

    #[test]
    fn summarise_captures_history_and_writes_record() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("work");
        init_repo(&dir);
        std::fs::write(dir.join("a.txt"), "a\n").unwrap();
        crate::core::git::commit_uncommitted_changes(&dir, "summ", "add a").unwrap();

        let run = Run::new("compact-run", RunKind::Job, &dir);
        let summary = summarise_and_write(&run, Some("keep going")).unwrap();
        assert!(summary.summary.contains("add a"));
        assert!(summary.next_prompt.starts_with(COMPACT_PROMPT_PREFIX));
        assert!(summary.next_prompt.contains("keep going"));
        assert!(compact_meta_path(&run).exists());

        let record: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(compact_meta_path(&run)).unwrap())
                .unwrap();
        assert_eq!(record["summary"], summary.summary);
        assert_eq!(record["next_prompt"], summary.next_prompt);
    }

    #[test]
    fn summarise_empty_worktree_does_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("empty");
        std::fs::create_dir_all(&dir).unwrap();
        let run = Run::new("compact-empty", RunKind::Job, &dir);
        // No initial commit: the summary must state the empty history, not bail.
        let summary = summarise_and_write(&run, None).unwrap();
        assert!(summary.summary.contains("no commits"));
    }
}

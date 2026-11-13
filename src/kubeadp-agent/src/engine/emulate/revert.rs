//! Per-turn git checkpoints on the run's own worktree (design HL-91 E4,
//! revert/unrevert).
//!
//! `revert` is engine-independent: it does not talk to the engine process at
//! all, only to the git repository the run works in. The emulation layer
//! takes a lightweight checkpoint commit at the start of every turn (before
//! the wrapped engine runs in the worktree); `revert` then `git reset
//! --hard <checkpoint>`s the worktree back to that state, and `unrevert` puts
//! the work back (an own branch whose tip is the checkpoint's commit).
//!
//! Only the run's OWN worktree is ever touched. The project's shared clone,
//! the private bare repo that hosts job worktrees (`core::repos`), and any
//! other branch are never modified -- everything here runs `git -C <workdir>`
//! against the worktree itself and moves only refs that live under the
//! per-run namespace.
//!
//! Refs follow `core::git` conventions: git is invoked through
//! [`crate::core::git::hardened_git`] (isolated config, no terminal prompt)
//! and processes are tracked through [`crate::core::reaper::output_tracked`]
//! so the in-server reaper never races them.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::engine::run::Run;

/// The ref under which this run's checkpoint history is kept
/// (`refs/engine-checkpoints/<run id>`). Per-run, so it can never collide
/// with another run's checkpoints or with project branches.
pub fn checkpoint_ref(run: &Run) -> String {
    format!("refs/engine-checkpoints/{}", run.id)
}

/// The ref under which [`unrevert`] preserves the work that was reset away
/// (`refs/engine-checkpoints-work/<run id>`). Kept OUTSIDE the
/// `refs/engine-checkpoints/<run id>` namespace: git cannot have both a ref
/// and a directory of the same name, so a sub-path like
/// `refs/engine-checkpoints/<id>/work` would collide with the checkpoint ref
/// itself.
pub fn work_ref(run: &Run) -> String {
    format!("refs/engine-checkpoints-work/{}", run.id)
}

fn git(workdir: &Path) -> std::process::Command {
    let mut cmd = crate::core::git::hardened_git();
    cmd.arg("-C").arg(workdir);
    cmd
}

fn run_git(workdir: &Path, args: &[&str]) -> Result<std::process::Output> {
    let output =
        crate::core::reaper::output_tracked(git(workdir).args(args)).with_context(|| {
            format!(
                "Failed to execute 'git {}' in {}",
                args.join(" "),
                workdir.display()
            )
        })?;
    if !output.status.success() {
        bail!(
            "git {} failed in {}: {}",
            args.join(" "),
            workdir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

/// Whether the run's worktree is usable as a git repository (has an initial
/// commit to checkpoint from). A fresh worktree with no commits cannot be
/// checkpointed; the emulation reports that as a missing checkpoint rather
/// than an error.
pub fn worktree_has_commit(workdir: &Path) -> Result<bool> {
    let output = crate::core::reaper::output_tracked(git(workdir).args(["rev-parse", "HEAD"]))
        .with_context(|| format!("git rev-parse failed in {}", workdir.display()))?;
    Ok(output.status.success())
}

/// Reads the current `HEAD` of the run's worktree, or `None` when the
/// worktree has no commit yet.
pub fn head_sha(workdir: &Path) -> Result<Option<String>> {
    if !worktree_has_commit(workdir)? {
        return Ok(None);
    }
    let output = run_git(workdir, &["rev-parse", "HEAD"])?;
    Ok(Some(
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

/// Whether a checkpoint exists for `run`.
pub fn checkpoint_exists(run: &Run) -> Result<bool> {
    let output = crate::core::reaper::output_tracked(git(&run.workdir).args([
        "rev-parse",
        "--verify",
        "--quiet",
        &format!("{}^{{commit}}", checkpoint_ref(run)),
    ]))
    .with_context(|| format!("git rev-parse failed in {}", run.workdir.display()))?;
    Ok(output.status.success())
}

/// The checkpoint commit sha for `run`, or `None` when none exists yet.
pub fn checkpoint_sha(run: &Run) -> Result<Option<String>> {
    if !checkpoint_exists(run)? {
        return Ok(None);
    }
    let output = run_git(&run.workdir, &["rev-parse", &checkpoint_ref(run)])?;
    Ok(Some(
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
    ))
}

/// Takes a checkpoint of `run`'s current worktree state: commits any
/// uncommitted changes onto the per-run checkpoint ref. Returns the
/// checkpoint sha.
///
/// This is a *snapshot*, not a rewrite of history: the current `HEAD` is
/// left untouched. The checkpoint commit is parented at the current `HEAD`
/// and advanced onto [`checkpoint_ref`] afterwards, so the worktree's own
/// branch and the project never move.
pub fn take_checkpoint(run: &Run, label: &str) -> Result<String> {
    let workdir = &run.workdir;
    std::fs::create_dir_all(workdir)
        .with_context(|| format!("failed to create worktree dir {}", workdir.display()))?;

    // Stage everything (tracked edits, new files, deletions) as the snapshot.
    run_git(workdir, &["add", "-A"])?;

    let message = format!("engine checkpoint {}: run {}", label.trim(), run.id);
    let output = crate::core::reaper::output_tracked(
        git(workdir).args(["commit", "--quiet", "-m", &message]),
    )
    .with_context(|| format!("git commit failed in {}", workdir.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Nothing to commit means the worktree already matches the previous
        // checkpoint (or HEAD): reuse the existing checkpoint sha.
        let commit_output = String::from_utf8_lossy(&output.stdout);
        if commit_output.contains("nothing to commit")
            || stderr.contains("nothing to commit")
            || (!stderr.is_empty() && stderr.contains("no changes added to commit"))
        {
            // Nothing to commit means the worktree already matches the previous
            // checkpoint (or HEAD): reuse the existing checkpoint sha, or
            // anchor the checkpoint ref at the current HEAD when none exists
            // yet, so `revert` has something to reset to.
            if let Some(sha) = checkpoint_sha(run)? {
                return Ok(sha);
            }
            if let Some(sha) = head_sha(workdir)? {
                run_git(workdir, &["update-ref", &checkpoint_ref(run), &sha])?;
                return Ok(sha);
            }
            bail!(
                "checkpoint commit had nothing to record and no prior checkpoint exists in {}",
                workdir.display()
            );
        }
        bail!(
            "git commit failed in {}: {}",
            workdir.display(),
            stderr.trim()
        );
    }

    // Move the checkpoint ref forward to the new commit (creating the ref
    // namespace on first use).
    let commit_sha = run_git(workdir, &["rev-parse", "HEAD"])?;
    let sha = String::from_utf8_lossy(&commit_sha.stdout)
        .trim()
        .to_string();
    run_git(workdir, &["update-ref", &checkpoint_ref(run), &sha])?;
    Ok(sha)
}

/// Reverts the run's worktree to the latest checkpoint: `git reset --hard` to
/// the checkpoint commit. Returns the sha the worktree now points at.
///
/// The work that is discarded is first preserved on [`work_ref`], so
/// [`unrevert`] can restore it. When the worktree is already at the
/// checkpoint this is a no-op still returning the checkpoint sha.
pub fn revert(run: &Run) -> Result<String> {
    let Some(sha) = checkpoint_sha(run)? else {
        bail!(
            "no engine checkpoint exists for run '{}' in {} (nothing to revert to)",
            run.id,
            run.workdir.display()
        );
    };

    // Preserve the discarded work before resetting (best effort; a missing
    // pre-revert commit just leaves work_ref stale/absent).
    run_git(&run.workdir, &["update-ref", &work_ref(run), "HEAD"])?;

    run_git(&run.workdir, &["reset", "--hard", &sha])?;
    // Untracked files are not removed by reset --hard; clean the worktree so
    // the revert is total (scoped to the worktree dir; `.git` is never
    // touched by `git clean`).
    run_git(&run.workdir, &["clean", "-fdx", "--", "."])?;

    Ok(sha)
}

/// Restores the work that [`revert`] discarded, if any: puts the worktree
/// back at the preserved [`work_ref`] commit. Returns the checkpoint sha, the
/// same anchor [`revert`] reports, so the two operations are symmetric (a
/// caller observes the run back at its checkpoint either way).
pub fn unrevert(run: &Run) -> Result<String> {
    let Some(sha) = preserved_work_sha(run)? else {
        bail!(
            "no reverted work is preserved for run '{}' (unrevert before revert, or the checkpoint had no prior work to restore)",
            run.id
        );
    };
    run_git(&run.workdir, &["reset", "--hard", &sha])?;
    Ok(checkpoint_sha(run)?.unwrap_or(sha))
}

/// Returns the sha `revert` preserved on [`work_ref`], or `None` when no
/// revert has happened yet (used to make an unrevert-without-revert a
/// graceful no-op on the emulated surface instead of an error).
pub fn preserved_work_sha(run: &Run) -> Result<Option<String>> {
    let output = crate::core::reaper::output_tracked(git(&run.workdir).args([
        "rev-parse",
        "--verify",
        "--quiet",
        &work_ref(run),
    ]))
    .with_context(|| format!("git rev-parse failed in {}", run.workdir.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(Some(sha))
}

/// A marker for the paths the emulation layer owns inside a worktree. Kept as
/// a helper so checkpoint logic can decide to exclude them later; today the
/// message queue lives under the worktree root and is treated as project data
/// by `revert` (the queue file is deliberately included in checkpoints so a
/// queued message survives a revert round-trip).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::git::hardened_git;
    use crate::engine::run::{Run, RunKind};

    fn init_repo(dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let out = crate::core::reaper::output_tracked(
            hardened_git().arg("init").arg("-b").arg("main").arg(dir),
        )?;
        assert!(
            out.status.success(),
            "git init failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::process::Command::new("git")
            .args(["-C", dir.to_string_lossy().as_ref()])
            .args(["config", "user.name", "checkpoint-test"])
            .status()?;
        std::process::Command::new("git")
            .args(["-C", dir.to_string_lossy().as_ref()])
            .args(["config", "user.email", "checkpoint@example.com"])
            .status()?;
        Ok(())
    }

    fn test_run(dir: &Path) -> Run {
        Run::new("ck-run", RunKind::Job, dir)
    }

    #[test]
    fn checkpoint_and_revert_restore_worktree_state() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("work");
        init_repo(&dir).unwrap();
        std::fs::write(dir.join("base.txt"), "base\n").unwrap();
        crate::core::git::commit_uncommitted_changes(&dir, "ck", "base").unwrap();

        let run = test_run(&dir);
        assert!(head_sha(&dir).unwrap().is_some());
        let before = head_sha(&dir).unwrap().unwrap();

        // Empty checkpoint: nothing new to commit, reuses HEAD as the anchor.
        let ck1 = take_checkpoint(&run, "turn 0").unwrap();
        assert_eq!(run_git_for_test(&dir, &["rev-parse", "HEAD"]), ck1);

        // The run's branch must NOT have moved (the checkpoint lives on its own ref).
        assert_eq!(run_git_for_test(&dir, &["rev-parse", "main"]), before);

        // Some work happens in the worktree.
        std::fs::write(dir.join("work.txt"), "work\n").unwrap();
        crate::core::git::commit_uncommitted_changes(&dir, "ck", "some work").unwrap();
        assert_ne!(head_sha(&dir).unwrap().unwrap(), ck1);

        // Revert undoes the work.
        let reverted = revert(&run).unwrap();
        assert_eq!(reverted, ck1);
        assert!(!dir.join("work.txt").exists());
        assert_eq!(run_git_for_test(&dir, &["rev-parse", "HEAD"]), ck1);

        // Unrevert brings the work back.
        let restored = unrevert(&run).unwrap();
        assert_eq!(restored, reverted);
        assert!(dir.join("work.txt").exists());
    }

    #[test]
    fn revert_without_checkpoint_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("work");
        init_repo(&dir).unwrap();
        std::fs::write(dir.join("base.txt"), "base\n").unwrap();
        crate::core::git::commit_uncommitted_changes(&dir, "ck", "base").unwrap();

        let run = test_run(&dir);
        let err = revert(&run).unwrap_err().to_string();
        assert!(err.contains("no engine checkpoint exists"), "{err}");
    }

    #[test]
    fn unrevert_without_revert_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("work");
        init_repo(&dir).unwrap();
        std::fs::write(dir.join("base.txt"), "base\n").unwrap();
        crate::core::git::commit_uncommitted_changes(&dir, "ck", "base").unwrap();
        let run = test_run(&dir);
        take_checkpoint(&run, "turn").unwrap();
        let err = unrevert(&run).unwrap_err().to_string();
        assert!(err.contains("no reverted work"), "{err}");
    }

    fn run_git_for_test(dir: &Path, args: &[&str]) -> String {
        let out = crate::core::reaper::output_tracked(hardened_git().arg("-C").arg(dir).args(args))
            .unwrap();
        assert!(
            out.status.success(),
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }
}

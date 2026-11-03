use anyhow::{bail, Context, Result};
use serde_json::json;
use std::fs;

use crate::core::args::CleanArgs;
use crate::core::config::Config;
use crate::core::git::{delete_branch, remove_worktree};
use crate::core::job::{compute_status, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

pub fn run(config: &Config, args: &CleanArgs) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    let mut output = CommandOutput::new(json!({ "job_id": meta.job_id }));

    if status == JobStatus::Running {
        if !args.force {
            let pid_str = meta
                .pid
                .map(|p| format!(" (PID {})", p))
                .unwrap_or_default();
            return Err(crate::core::error::AgentRunError::invalid(
                "job_running",
                format!(
                    "Job '{}' is currently running{}. Terminate it first using 'agent-run kill {}', or pass --force.",
                    args.job_id,
                    pid_str,
                    args.job_id
                ),
            )
            .into());
        } else if let Some(pid) = meta.pid {
            crate::core::commands::kill::terminate_process_to(
                config,
                &meta.job_id,
                pid,
                meta.pid_start_time,
                Some(meta.retry_count),
                &mut |stderr, line| {
                    if stderr {
                        output.push_err(line);
                    } else {
                        output.push_out(line);
                    }
                },
            )?;
        }
    }

    // Job worktrees live in the project's private repository (agent-run's
    // own bare clone); pre-fix jobs whose private repo does not exist yet
    // still point at the project checkout.
    let repo_dir = crate::core::repos::project_git_repo(config, &meta.project);
    // A git-sync repo re-init can orphan this worktree's admin directory:
    // re-attach it first so the dirty check and the `git worktree remove`
    // below operate on a consistent pair (no-op when healthy).
    crate::core::repos::auto_repair_worktree(config, &meta);

    // Never run git in a worktree agent-run owns whose .git pointer does not
    // resolve inside the project's repos root / workspace checkout (a
    // tampered/stale pointer could redirect the dirty check / worktree removal
    // into an attacker-controlled repo). External (--from-worktree) worktrees
    // are user-supplied and are not validated against our roots.
    if !meta.external_worktree {
        crate::core::git::worktree_gitdir_in_expected_roots(
            &meta.worktree_path,
            &crate::core::git::expected_worktree_roots(config, &meta.project),
        )?;
    }

    // Re-read the row right before destroying anything: a concurrent `retry`
    // (which reuses this row, bumps retry_count and relaunches an agy in the
    // same worktree) can land between the load at the top of this function and
    // the removal below. Destroying the worktree/branch the new attempt just
    // launched would kill that attempt, so abort if the row changed.
    let fresh = JobMetadata::load(&config.state_root, &meta.job_id).with_context(|| {
        format!(
            "Failed to re-load job '{}' before removing its worktree",
            args.job_id
        )
    })?;
    let fresh_status = compute_status(&fresh, &config.state_root);
    if fresh.retry_count != meta.retry_count {
        bail!(
            "Job '{}' changed while this clean was running: retry_count moved from {} to {} (a concurrent retry may have launched a new attempt); aborting to avoid destroying it.",
            args.job_id,
            meta.retry_count,
            fresh.retry_count
        );
    }
    if fresh_status == JobStatus::Running && status != JobStatus::Running {
        bail!(
            "Job '{}' started running after this clean began (a concurrent retry may have launched); aborting - terminate it with 'agent-run kill {}' first.",
            args.job_id,
            args.job_id
        );
    }

    // Conservative-deletion checks:
    // Before removing ANYTHING, verify that worktree is clean and branch has no
    // unmerged/unpushed commits, unless --force is set.
    if !args.force {
        // 1. Check uncommitted worktree case. A worktree whose git state
        //    cannot be read (e.g. a dangling worktree whose admin dir was
        //    lost) is treated as dirty: it may hold unsaved files we cannot
        //    enumerate, so refuse to delete it without --force (the same
        //    conservative rule `gc` applies when it skips such jobs).
        let worktree_dirty = if meta.worktree_path.exists() && !meta.external_worktree {
            // When `git status` cannot run, we cannot prove the worktree is
            // clean, so treat an unreadable worktree as dirty.
            crate::core::git::is_worktree_dirty(&meta.worktree_path).unwrap_or(true)
        } else {
            false
        };
        if worktree_dirty {
            bail!(
                "Worktree at {} contains uncommitted modifications or untracked files. (Run 'agent-run clean {} --force' to discard uncommitted changes and remove the worktree)",
                meta.worktree_path.display(),
                args.job_id
            );
        }

        // 2. Check branch for unmerged/unpushed commits
        if !meta.branch.is_empty() && !meta.external_worktree && repo_dir.exists() {
            let unpushed_count = crate::core::git::count_unmerged_unpushed_commits(
                &repo_dir,
                &meta.branch,
                &meta.base_ref,
            )?;
            if unpushed_count > 0 {
                // Mirror the base validation the counter performed: when neither
                // the base ref nor origin/<base> exists, the "merged into base"
                // part of the count never ran, and the refusal must say so.
                let mut base_note = String::new();
                if !meta.base_ref.is_empty()
                    && !crate::core::git::git_ref_exists(&repo_dir, &meta.base_ref)?
                    && !crate::core::git::git_ref_exists(
                        &repo_dir,
                        &format!("origin/{}", meta.base_ref),
                    )?
                {
                    base_note = format!(
                        " Note: base ref '{}' was not found in the repository, so the 'already merged into base' part of this check was skipped (only the 'reachable from a remote' part was evaluated).",
                        meta.base_ref
                    );
                }
                bail!(
                    "Branch '{}' has {} commit(s) that are neither merged into base ref '{}' nor reachable from any remote.{} To proceed, push or merge the branch, or run 'agent-run clean {} --force' to discard them.",
                    meta.branch,
                    unpushed_count,
                    meta.base_ref,
                    base_note,
                    args.job_id
                );
            }
        }
    }

    let mut branch_tip_sha = None;
    if !meta.branch.is_empty() && !meta.external_worktree && repo_dir.exists() {
        if let Ok(sha) = crate::core::git::get_branch_tip_sha(&repo_dir, &meta.branch) {
            branch_tip_sha = Some(sha);
        }
    }

    if args.force {
        if let Some(ref sha) = branch_tip_sha {
            output.push_out(format!(
                "Discarding branch '{}' at commit {} (--force passed).",
                meta.branch, sha
            ));
        }
    }

    // Remove worktree via git worktree remove
    //
    // NOTE: `--force` is used when requested to delete worktrees that contain uncommitted
    // modifications, untracked generated files, or untracked dependencies created by the agent.
    // Jobs started with `--from-worktree` (external_worktree) own neither the worktree
    // nor its branch, so neither is removed.
    if meta.worktree_path.exists() {
        if meta.external_worktree {
            output.push_out(format!(
                "Keeping externally supplied worktree at {} (job '{}' was started with --from-worktree).",
                meta.worktree_path.display(),
                args.job_id
            ));
        } else {
            output.push_out(format!(
                "Removing git worktree at {}...",
                meta.worktree_path.display()
            ));
            if let Err(e) = remove_worktree(&repo_dir, &meta.worktree_path, args.force) {
                if !args.force {
                    return Err(crate::core::error::AgentRunError::invalid(
                        "worktree_removal_failed",
                        format!(
                            "Failed to remove worktree: {}. (Run 'agent-run clean {} --force' to discard uncommitted changes and remove the worktree)",
                            e,
                            args.job_id
                        ),
                    )
                    .into());
                }
                output.push_err(format!(
                    "Warning: git worktree remove returned error: {}",
                    e
                ));
                // `git worktree remove` failing usually means the worktree's
                // own bookkeeping is broken, so a blind remove_dir_all here
                // would destroy whatever files the agent (or a user) left
                // behind. Only sweep the leftover shell when it holds
                // nothing but git bookkeeping (or nothing at all).
                match crate::core::git::worktree_dir_can_be_swept(&meta.worktree_path, 20) {
                    Ok(()) => {
                        let _ = fs::remove_dir_all(&meta.worktree_path);
                    }
                    Err(entries) => {
                        bail!(
                            "Kept worktree at {} - it still contains files that are not git bookkeeping (up to 20 listed): {}. The directory was NOT deleted, so the job record was left in place. Once its contents are no longer needed, remove it manually, then re-run 'agent-run clean {} --force':\n  rm -rf '{}'",
                            meta.worktree_path.display(),
                            entries.join(", "),
                            args.job_id,
                            meta.worktree_path.display()
                        );
                    }
                }
            }
            output.push_out(format!(
                "Removed git worktree at {}.",
                meta.worktree_path.display()
            ));
        }
    }

    // Delete git branch associated with this job
    if !meta.branch.is_empty() && !meta.external_worktree && repo_dir.exists() {
        crate::core::git::prune_stale_worktree_registrations_to(&repo_dir, &mut |line| {
            output.push_err(line);
        });
        if let Err(e) = delete_branch(&repo_dir, &meta.branch) {
            output.push_err(format!(
                "Warning: failed to delete git branch '{}': {}",
                meta.branch, e
            ));
        } else {
            output.push_out(format!("Removed git branch '{}'.", meta.branch));
        }
    }

    // Purge the agent's agy conversation artifacts (best-effort; warns and continues).
    if let Some(conversation_id) = meta.conversation_id.as_deref() {
        match crate::core::agy_cleanup::default_antigravity_dir() {
            Some(root) => {
                crate::core::agy_cleanup::cleanup_conversation(&root, conversation_id);
            }
            None => {
                output.push_err(
                    "Warning: could not resolve home directory; skipping agy conversation cleanup"
                        .to_string(),
                );
            }
        }
    }

    // Remove the job record from the state database. A failure here is a
    // returned error, not a warning: the worktree/branch are already gone,
    // but the stale row would keep the job listed (and `retry`-able into a
    // nonexistent worktree) if clean reported success.
    if let Err(e) = JobMetadata::delete(&config.state_root, &meta.job_id) {
        bail!(
            "Job '{}' worktree/branch were handled, but deleting its job record failed: {}. The stale record still exists; run 'agent-run clean {}' again to finish.",
            meta.job_id,
            e,
            args.job_id
        );
    }
    output.push_out(format!("Removed job record for '{}'.", meta.job_id));

    // Purge log if requested
    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    if args.purge_log && log_path.exists() {
        match fs::remove_file(&log_path) {
            Ok(()) => output.push_out(format!("Removed log file at {}", log_path.display())),
            Err(e) => output.push_err(format!(
                "Failed to remove log file at {}: {}",
                log_path.display(),
                e
            )),
        }
    } else if log_path.exists() {
        output.push_out(format!(
            "Retained log file at {} (use --purge-log to remove).",
            log_path.display()
        ));
    }

    output.push_out(format!("Job '{}' cleaned successfully.", meta.job_id));
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    /// Initializes a minimal git repo with one commit (same recipe as the
    /// git module's tests).
    fn init_test_git_repo(path: &std::path::Path) {
        let run_cmd = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(path)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        std::fs::write(path.join("file.txt"), "hello").unwrap();
        run_cmd(&["add", "file.txt"]);
        run_cmd(&["commit", "-m", "initial commit"]);
    }

    fn save_job(
        state_root: &std::path::Path,
        job_id: &str,
        worktree: &std::path::Path,
        branch: &str,
    ) {
        crate::core::job::ensure_state_store(state_root).unwrap();
        let meta = JobMetadata {
            job_id: job_id.to_string(),
            project: "p".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: worktree.to_path_buf(),
            branch: branch.to_string(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(1),
            status: None,
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
        };
        meta.save(state_root).unwrap();
    }

    fn git(repo_dir: &std::path::Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(["-C", &repo_dir.to_string_lossy()])
            .args(args)
            .output()
            .expect("execute git command");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    #[test]
    fn test_clean_prunes_stale_worktree_before_branch_delete() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("ws").join("p");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        // Create the job's real worktree, then delete its directory
        // out-of-band: git keeps the (now stale) registration and refuses
        // `git branch -D` until it is pruned.
        let wt = temp.path().join("wt-j1");
        git(
            &repo_dir,
            &[
                "worktree",
                "add",
                "-b",
                "agent-run/j1",
                &wt.to_string_lossy(),
                "main",
            ],
        );
        std::fs::remove_dir_all(&wt).unwrap();
        assert!(
            git(&repo_dir, &["worktree", "list"]).contains(wt.to_str().unwrap()),
            "test fixture must leave a stale worktree registration"
        );

        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        save_job(&config.state_root, "j1", &wt, "agent-run/j1");

        run(
            &config,
            &CleanArgs {
                job_id: "j1".to_string(),
                force: false,
                purge_log: false,
            },
        )
        .unwrap();

        // Without the prune, `git branch -D` fails on the stale registration
        // and `clean` leaks the branch; with it, the branch is gone.
        assert_eq!(
            git(&repo_dir, &["branch", "--list", "agent-run/j1"]).trim(),
            "",
            "branch must be deleted even though the worktree dir was removed out-of-band"
        );
    }

    #[test]
    fn test_clean_reports_log_removal_failure() {
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        // Nonexistent worktree + empty branch: no destructive git steps run.
        save_job(
            &config.state_root,
            "j2",
            &temp.path().join("no-such-wt"),
            "",
        );

        // A DIRECTORY at the log path makes fs::remove_file fail with EISDIR
        // deterministically (even as root, where permission tricks do not).
        std::fs::create_dir_all(JobMetadata::log_path(&config.state_root, "j2")).unwrap();

        let out = run(
            &config,
            &CleanArgs {
                job_id: "j2".to_string(),
                force: false,
                purge_log: true,
            },
        )
        .unwrap();
        assert!(
            !out.stdout_lines
                .iter()
                .any(|l| l.contains("Removed log file")),
            "must not claim the log was removed: {:?}",
            out.stdout_lines
        );
        assert!(
            out.stderr_lines
                .iter()
                .any(|l| l.starts_with("Failed to remove log file") && l.contains("j2.log")),
            "must report the removal failure: {:?}",
            out.stderr_lines
        );
    }

    #[test]
    fn test_clean_refuses_on_unpushed_commits_and_removes_nothing() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("ws").join("p");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt = temp.path().join("wt-unpushed");
        git(
            &repo_dir,
            &[
                "worktree",
                "add",
                "-b",
                "agent-run/j-unpushed",
                &wt.to_string_lossy(),
                "main",
            ],
        );

        // Add a commit on the branch
        std::fs::write(wt.join("new-file.txt"), "new content").unwrap();
        git(&wt, &["add", "new-file.txt"]);
        git(&wt, &["commit", "-m", "work on branch"]);

        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        save_job(
            &config.state_root,
            "j-unpushed",
            &wt,
            "agent-run/j-unpushed",
        );
        let log_path = JobMetadata::log_path(&config.state_root, "j-unpushed");
        std::fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        std::fs::write(&log_path, "job log content").unwrap();

        let err = run(
            &config,
            &CleanArgs {
                job_id: "j-unpushed".to_string(),
                force: false,
                purge_log: true,
            },
        )
        .unwrap_err();

        let err_msg = err.to_string();
        assert!(err_msg.contains("1 commit(s)"));
        assert!(err_msg.contains("--force"));

        // Verify nothing was removed:
        assert!(wt.exists(), "worktree must still exist");
        assert!(
            !git(&repo_dir, &["branch", "--list", "agent-run/j-unpushed"]).is_empty(),
            "branch must still exist"
        );
        assert!(
            JobMetadata::load(&config.state_root, "j-unpushed").is_ok(),
            "job record must still exist"
        );
        assert!(log_path.exists(), "log must still exist");
    }

    #[test]
    fn test_clean_force_proceeds_and_prints_sha() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("ws").join("p");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt = temp.path().join("wt-force");
        git(
            &repo_dir,
            &[
                "worktree",
                "add",
                "-b",
                "agent-run/j-force",
                &wt.to_string_lossy(),
                "main",
            ],
        );

        std::fs::write(wt.join("new-file.txt"), "new content").unwrap();
        git(&wt, &["add", "new-file.txt"]);
        git(&wt, &["commit", "-m", "work on branch"]);
        let tip_sha = git(&wt, &["rev-parse", "HEAD"]).trim().to_string();

        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        save_job(&config.state_root, "j-force", &wt, "agent-run/j-force");

        let out = run(
            &config,
            &CleanArgs {
                job_id: "j-force".to_string(),
                force: true,
                purge_log: false,
            },
        )
        .unwrap();

        // Must print the tip sha
        assert!(
            out.stdout_lines.iter().any(|l| l.contains(&tip_sha)),
            "output must contain the branch tip sha {}: {:?}",
            tip_sha,
            out.stdout_lines
        );

        // Verify removal proceeded:
        assert!(!wt.exists(), "worktree must be removed");
        assert_eq!(
            git(&repo_dir, &["branch", "--list", "agent-run/j-force"]).trim(),
            "",
            "branch must be removed"
        );
        assert!(
            JobMetadata::load(&config.state_root, "j-force").is_err(),
            "job record must be deleted"
        );
    }

    #[test]
    fn test_clean_force_keeps_worktree_with_ignored_file_when_git_remove_fails() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("ws").join("p");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        // Commit a .gitignore so it checks out into the linked worktree and keeps
        // the agent's data file invisible to `git status`.
        std::fs::write(repo_dir.join(".gitignore"), "keepme.dat\n").unwrap();
        git(&repo_dir, &["add", ".gitignore"]);
        git(&repo_dir, &["commit", "-m", "add gitignore"]);

        let wt = temp.path().join("wt-keep");
        git(
            &repo_dir,
            &[
                "worktree",
                "add",
                "-b",
                "agent-run/j-keep",
                &wt.to_string_lossy(),
                "main",
            ],
        );
        std::fs::write(wt.join("keepme.dat"), "precious user data").unwrap();
        // Lock the worktree: `git worktree remove` (no force) fails while
        // `git status` stays clean, so the fallback sweep must keep the dir.
        git(&repo_dir, &["worktree", "lock", &wt.to_string_lossy()]);

        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        save_job(&config.state_root, "j-keep", &wt, "agent-run/j-keep");

        let err = run(
            &config,
            &CleanArgs {
                job_id: "j-keep".to_string(),
                force: true,
                purge_log: false,
            },
        )
        .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("Kept worktree at"),
            "must say the worktree was kept: {msg}"
        );
        assert!(
            msg.contains("keepme.dat"),
            "must list the file that was not deleted: {msg}"
        );
        assert!(
            msg.contains("rm -rf"),
            "must print the manual removal command: {msg}"
        );

        // Nothing destructive happened to user data or git state:
        assert!(
            wt.exists(),
            "worktree with git-ignored files must NOT be deleted"
        );
        assert!(
            wt.join("keepme.dat").exists(),
            "git-ignored file must survive a failed worktree removal"
        );
        assert!(
            !git(&repo_dir, &["branch", "--list", "agent-run/j-keep"]).is_empty(),
            "branch must remain while the worktree it checks out is kept"
        );
        assert!(
            JobMetadata::load(&config.state_root, "j-keep").is_ok(),
            "job record must be kept so the worktree is not orphaned"
        );
    }

    #[test]
    fn test_clean_force_sweeps_empty_leftover_when_git_remove_fails() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("ws").join("p");
        std::fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        // Empty commit: the worktree checks out nothing, so the only entry it
        // holds is its own `.git` pointer -- bookkeeping only, safe to sweep.
        run_cmd(&["commit", "-q", "--allow-empty", "-m", "init"]);

        let wt = temp.path().join("wt-shell");
        git(
            &repo_dir,
            &[
                "worktree",
                "add",
                "-b",
                "agent-run/j-shell",
                &wt.to_string_lossy(),
                "main",
            ],
        );
        assert_eq!(
            fs::read_dir(&wt)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            vec![".git".to_string()],
            "precondition: the worktree holds only its .git pointer"
        );
        // Drop the `.git` pointer: `git worktree remove` now fails for an
        // unrelated reason (validation error), leaving a shell that holds no
        // user data, so the fallback sweep may remove it. (A lock would not
        // do: `git worktree prune` keeps a locked worktree's registration, so
        // the branch could never be deleted afterwards.)
        fs::remove_file(wt.join(".git")).unwrap();

        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        save_job(&config.state_root, "j-shell", &wt, "agent-run/j-shell");

        let out = run(
            &config,
            &CleanArgs {
                job_id: "j-shell".to_string(),
                force: true,
                purge_log: false,
            },
        )
        .unwrap();

        assert!(
            out.stdout_lines
                .iter()
                .any(|l| l.contains("Removed git worktree at")),
            "bookkeeping-only leftover must be swept and reported removed: {:?}",
            out.stdout_lines
        );
        assert!(
            !wt.exists(),
            "worktree left holding only the .git pointer must be swept"
        );
        assert_eq!(
            git(&repo_dir, &["branch", "--list", "agent-run/j-shell"]).trim(),
            "",
            "branch must be removed after the stale registration is pruned"
        );
        assert!(
            JobMetadata::load(&config.state_root, "j-shell").is_err(),
            "job record must be deleted"
        );
    }
}

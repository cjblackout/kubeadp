use anyhow::{bail, Context, Result};
use serde_json::json;

use crate::core::args::CommitArgs;
use crate::core::config::Config;
use crate::core::git::{commit_selected, commit_uncommitted_changes};
use crate::core::job::{compute_status, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

/// Derives a commit summary from a job's prompt: the first non-blank line
/// (trimmed), truncated to 60 characters. When the prompt has no non-blank
/// line, falls back to a fixed placeholder.
pub fn derive_commit_summary(prompt_source: &str) -> String {
    let prompt_summary = prompt_source
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("agent-run job");
    if prompt_summary.chars().count() > 60 {
        let s: String = prompt_summary.chars().take(57).collect();
        format!("{}...", s)
    } else {
        prompt_summary.to_string()
    }
}

/// Commits uncommitted changes in a job's worktree as the INVOKING user.
///
/// NOTE: committing is a trusted operator action performed as the server's
/// own process user -- the engine has no privilege separation, so plain
/// `git` is always the right tool here.
pub fn run(config: &Config, args: &CommitArgs) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    // Refuse to commit a running job's worktree: it races the agent's own
    // git operations (a commit could interleave with the agent's edits and
    // be lost when a later tool run resets or recommits).
    if status == JobStatus::Running {
        let pid_str = meta
            .pid
            .map(|p| format!(" (PID {})", p))
            .unwrap_or_default();
        return Err(crate::core::error::AgentRunError::invalid(
            "job_running",
            format!(
                "Cannot commit job '{}': job is currently running{}. Terminate it first using 'agent-run kill {}'.",
                args.job_id,
                pid_str,
                args.job_id
            ),
        )
        .into());
    }

    if !meta.worktree_path.exists() {
        return Err(crate::core::error::AgentRunError::invalid(
            "worktree_not_found",
            format!(
                "Worktree directory for job '{}' does not exist at {} (it may have been deleted or cleaned)",
                args.job_id,
                meta.worktree_path.display()
            ),
        )
        .into());
    }

    // A git-sync repo re-init can orphan this worktree: re-attach it before
    // any git operation (silent no-op when healthy).
    crate::core::repos::auto_repair_worktree(config, &meta);

    // Never run git in a worktree agent-run owns whose .git pointer does not
    // resolve inside the project's repos root / workspace checkout (a
    // tampered/stale pointer could redirect the commit into an
    // attacker-controlled repo). External (--from-worktree) worktrees are
    // user-supplied and are not validated against our roots.
    if !meta.external_worktree {
        crate::core::git::worktree_gitdir_in_expected_roots(
            &meta.worktree_path,
            &crate::core::git::expected_worktree_roots(config, &meta.project),
        )?;
    }

    // Fast path: nothing to commit.
    let status_out = crate::core::reaper::output_tracked(&mut {
        let mut c = crate::core::git::hardened_git();
        c.args([
            "-C",
            &meta.worktree_path.to_string_lossy(),
            "status",
            "--porcelain",
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git status' in {} (ensure git is installed and in PATH)",
            meta.worktree_path.display()
        )
    })?;

    if !status_out.status.success() {
        bail!(
            "git status failed: {}",
            String::from_utf8_lossy(&status_out.stderr).trim()
        );
    }

    if status_out.stdout.is_empty() {
        let mut output = CommandOutput::new(json!({
            "job_id": meta.job_id,
            "committed": false,
            "message": args.message.clone(),
        }));
        output.push_out(format!(
            "Worktree for job '{}' is already clean (nothing to commit).",
            args.job_id
        ));
        return Ok(output);
    }

    let prompt_source = if !meta.resolved_prompt.trim().is_empty() {
        &meta.resolved_prompt
    } else {
        &meta.prompt
    };
    let message = args
        .message
        .clone()
        .unwrap_or_else(|| derive_commit_summary(prompt_source));

    let has_selection = !args.only.is_empty() || !args.exclude.is_empty();
    let selected_hint = if has_selection {
        " (selection: only matching paths are committed; everything else stays uncommitted)"
    } else {
        ""
    };
    let committed = if has_selection {
        commit_selected(
            &meta.worktree_path,
            &meta.job_id,
            &message,
            &args.only,
            &args.exclude,
        )?
    } else {
        commit_uncommitted_changes(&meta.worktree_path, &meta.job_id, &message)?
    };

    let output = if committed {
        let mut out = CommandOutput::new(json!({
            "job_id": meta.job_id,
            "committed": true,
            "message": message,
        }));
        out.push_out(format!(
            "Committed uncommitted changes in worktree {} (message: 'agent-run({}): {}'){}.",
            meta.worktree_path.display(),
            meta.job_id,
            message,
            selected_hint
        ));
        out
    } else {
        // The worktree became clean between our status check and the commit
        // (e.g. a concurrent process committed first): do not report a
        // commit that never happened.
        let mut out = CommandOutput::new(json!({
            "job_id": meta.job_id,
            "committed": false,
            "message": message,
        }));
        out.push_out(format!(
            "Worktree for job '{}' is clean: no new commit was made (changes were already committed).",
            args.job_id
        ));
        out
    };
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::JobStatus;
    use std::process::Command;
    use tempfile::tempdir;

    fn git(repo_dir: &std::path::Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(repo_dir)
            .args(args)
            .output()
            .expect("execute git");
        assert!(
            out.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn test_commit_commits_uncommitted_changes() {
        // A repo + worktree with uncommitted changes.
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        let repo_dir = ws_dir.join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        git(&repo_dir, &["init", "-b", "main"]);
        git(&repo_dir, &["config", "user.name", "Test"]);
        git(&repo_dir, &["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        git(&repo_dir, &["add", "file.txt"]);
        git(&repo_dir, &["commit", "-m", "initial commit"]);

        let wt = ws_dir.join("wt_commit");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/commitjob", "main").unwrap();
        std::fs::write(wt.join("changed.txt"), "change").unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        let meta = JobMetadata {
            job_id: "commitjob".to_string(),
            project: "myrepo".to_string(),
            prompt: "make a change".to_string(),
            resolved_prompt: "make a change".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: wt.clone(),
            branch: "agent-run/commitjob".to_string(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: None,
            status: Some(JobStatus::Done),
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
        meta.save(&state_dir).unwrap();

        // Commit succeeds as the server's own process user.
        let args = CommitArgs {
            job_id: "commitjob".to_string(),
            message: None,
            only: vec![],
            exclude: vec![],
        };
        let out = run(&config, &args).unwrap();
        assert_eq!(
            out.data["committed"],
            json!(true),
            "committed flag must be true"
        );
        let subject = git(&wt, &["log", "-1", "--format=%s"]);
        assert!(
            subject.starts_with("agent-run(commitjob): "),
            "commit subject should be agent-run-prefixed, got: {}",
            subject
        );

        // Second run hits the "already clean" fast path and still succeeds.
        run(&config, &args).unwrap();
    }

    /// Saves a job record (worktree `wt`, prompt as given, PID `pid`) and
    /// returns the job id, for tests that exercise `run` end-to-end.
    fn save_commit_job(
        state_dir: &std::path::Path,
        wt: &std::path::Path,
        prompt: &str,
        pid: Option<u32>,
    ) -> String {
        let meta = JobMetadata {
            job_id: "commitjob".to_string(),
            project: "myrepo".to_string(),
            prompt: prompt.to_string(),
            resolved_prompt: prompt.to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: wt.to_path_buf(),
            branch: "agent-run/commitjob".to_string(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid,
            pid_start_time: None,
            exit_code: None,
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
        meta.save(state_dir).unwrap();
        meta.job_id
    }

    #[test]
    fn test_commit_refuses_running_job() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        let repo_dir = ws_dir.join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        git(&repo_dir, &["init", "-b", "main"]);
        git(&repo_dir, &["config", "user.name", "Test"]);
        git(&repo_dir, &["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        git(&repo_dir, &["add", "file.txt"]);
        git(&repo_dir, &["commit", "-m", "initial commit"]);

        let wt = ws_dir.join("wt_running");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/commitjob", "main").unwrap();
        std::fs::write(wt.join("changed.txt"), "change").unwrap();

        // A live PID (the test harness's own) makes the job Running.
        let job_id = save_commit_job(&state_dir, &wt, "make a change", Some(std::process::id()));

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        // Record the commit count BEFORE: the worktree already contains
        // the initial commit, so the count is "1", not "0".
        let count_before = git(&wt, &["rev-list", "--count", "HEAD"]);
        assert!(!count_before.is_empty(), "could not read commit count");

        let args = CommitArgs {
            job_id: job_id.clone(),
            message: None,
            only: vec![],
            exclude: vec![],
        };
        let err = run(&config, &args).unwrap_err();
        assert!(
            err.to_string().contains("currently running"),
            "expected the running-job refusal, got: {}",
            err
        );

        // Nothing may have been committed: the count must be unchanged.
        let count_after = git(&wt, &["rev-list", "--count", "HEAD"]);
        assert_eq!(
            count_before, count_after,
            "refused commit must not create a commit"
        );
    }

    #[test]
    fn test_derive_commit_summary() {
        assert_eq!(
            derive_commit_summary("fix the login bug\nsecond line"),
            "fix the login bug"
        );
        assert_eq!(derive_commit_summary(""), "agent-run job");
        // Whitespace-only first line: the first non-blank line wins.
        assert_eq!(derive_commit_summary("  \nreal summary"), "real summary");
        // All lines blank: fixed fallback, not an empty string.
        assert_eq!(derive_commit_summary("  \n\n"), "agent-run job");
        let long = "x".repeat(100);
        let s = derive_commit_summary(&long);
        assert_eq!(s.chars().count(), 60);
        assert!(s.ends_with("..."));
    }
}

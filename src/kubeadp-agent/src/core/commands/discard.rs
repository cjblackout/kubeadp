use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::json;

use crate::core::args::DiscardArgs;
use crate::core::config::Config;
use crate::core::job::{compute_status, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

/// Validates one user-supplied discard path (lexical checks only).
///
/// `discard` is a destructive command that must never interpret shell
/// metacharacters or climb above the worktree, so the path must be a
/// relative, exact file name: no `*`, `?` or `[` (globs), no leading `/`,
/// no `..` component.
pub fn validate_discard_path(raw: &str) -> Result<String> {
    if raw.trim().is_empty() {
        bail!("discard path must not be empty");
    }
    if raw.starts_with('/') {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_refused",
            format!(
                "Refusing to discard '{}': absolute paths are not allowed (paths are relative to the worktree)",
                raw
            ),
        )
        .into());
    }
    if raw.chars().any(|c| matches!(c, '*' | '?' | '[')) {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_refused",
            format!(
                "Refusing to discard '{}': globs are not allowed (name each file explicitly)",
                raw
            ),
        )
        .into());
    }
    for component in Path::new(raw).components() {
        if component == std::path::Component::ParentDir {
            return Err(crate::core::error::AgentRunError::invalid(
                "discard_refused",
                format!(
                    "Refusing to discard '{}': '..' components are not allowed",
                    raw
                ),
            )
            .into());
        }
    }
    // Normalize leading `./` so downstream checks and messages are clean.
    let cleaned = raw.trim_start_matches("./").trim_end_matches('/');
    if cleaned.is_empty() {
        bail!("discard path must not be empty");
    }
    Ok(cleaned.to_string())
}

/// Checks that `worktree` holds the named untracked file, enforcing every
/// guard `discard` has: the path must exist, be a REGULAR file (not a
/// directory), traverse NO symlink component -- the final component or any
/// intermediate directory on the way to it -- stay inside the worktree after
/// the path resolves, and NOT be tracked by git. Returns the NAMED path that
/// is safe to delete: `discard` deletes exactly the path the user named and
/// never a resolved (symlink) target.
pub fn check_discardable(worktree: &Path, path: &str) -> Result<PathBuf> {
    let candidate = worktree.join(path);

    if !candidate.exists() {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_refused",
            format!(
                "Refusing to discard '{}': does not exist in the worktree (it was already deleted or the path is wrong)",
                path
            ),
        )
        .into());
    }

    let canonical_wt = worktree.canonicalize().with_context(|| {
        format!(
            "Failed to resolve worktree directory {} (ensure git is installed and in PATH)",
            worktree.display()
        )
    })?;

    // Resolve every component of the named path with symlink_metadata: a
    // symlink anywhere on the path -- the final component OR an intermediate
    // directory -- is refused. A symlink to a tracked file would otherwise
    // pass the tracked check below (the symlink name is untracked) and its
    // target -- not the symlink -- would be the file that gets deleted, and
    // an intermediate symlink to a tracked DIRECTORY makes the whole subtree
    // below it indistinguishable from real worktree files.
    let mut prefix = worktree.to_path_buf();
    for name in Path::new(path).components().map(|c| c.as_os_str()) {
        prefix.push(name);
        let meta = std::fs::symlink_metadata(&prefix).with_context(|| {
            format!(
                "Failed to stat '{}' in worktree {}",
                prefix.display(),
                worktree.display()
            )
        })?;
        if meta.file_type().is_symlink() {
            let comp = prefix
                .strip_prefix(worktree)
                .unwrap_or(&prefix)
                .to_string_lossy();
            return Err(crate::core::error::AgentRunError::invalid(
                "discard_refused",
                format!(
                    "Refusing to discard '{}': refusing to discard through a symlink: {}; remove it with git/rm yourself",
                    path,
                    comp
                ),
            )
            .into());
        }
    }

    // Every prefix is symlink-free, so canonicalizing the candidate resolves
    // the full on-disk location without following any link; it must stay
    // inside the worktree.
    let canonical_target = candidate.canonicalize().with_context(|| {
        format!(
            "Failed to resolve '{}' in worktree {}",
            path,
            worktree.display()
        )
    })?;
    if !canonical_target.starts_with(&canonical_wt) {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_refused",
            format!(
                "Refusing to discard '{}': its directory resolves outside the worktree ({})",
                path,
                canonical_target.display()
            ),
        )
        .into());
    }

    // A tracked file is git's business: refusing points the user at `git rm`,
    // and an untracked file is the only thing `discard` may delete. The check
    // runs on the canonicalized location expressed relative to the canonical
    // worktree root, so the answer is the same no matter which (already
    // validated, symlink-free) name the file was addressed by.
    let tracked_path = canonical_target
        .strip_prefix(&canonical_wt)
        .with_context(|| {
            format!(
                "Resolved path {} escaped the canonical worktree {}",
                canonical_target.display(),
                canonical_wt.display()
            )
        })?;
    let tracked_out = crate::core::reaper::output_tracked(&mut {
        let mut c = crate::core::git::hardened_git();
        c.args([
            "-C",
            &worktree.to_string_lossy(),
            "ls-files",
            "--error-unmatch",
            "--",
            &tracked_path.to_string_lossy(),
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git ls-files' in {} (ensure git is installed and in PATH)",
            worktree.display()
        )
    })?;
    if tracked_out.status.success() {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_refused",
            format!(
                "Refusing to discard '{}': tracked in the worktree's git index (use 'git rm -- <path>' from the worktree and commit if you meant to delete it tracked)",
                path
            ),
        )
        .into());
    }

    // Symlink components are already refused above, so is_dir() cannot be
    // fooled by a symlink-to-directory either.
    if candidate.is_dir() {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_refused",
            format!(
                "Refusing to discard '{}': is a directory (discard deletes single files -- name the files inside it, or remove it with git)",
                path
            ),
        )
        .into());
    }

    Ok(candidate)
}

/// Deletes the named untracked files from a job's worktree.
///
/// Only files the user named explicitly are ever deleted: globs are
/// rejected, and every guard (tracked, directory, symlink, running job)
/// must pass before anything is removed. Symlinks are refused outright, so
/// `discard` can never delete a resolved target the user did not name.
pub fn run(config: &Config, args: &DiscardArgs) -> Result<CommandOutput> {
    // `paths` is `#[serde(default)]` in the wire form, so an HTTP body that
    // omits it must also be an error -- an empty selection is a no-op, and
    // no-op successes would hide client bugs (the CLI cannot send zero
    // paths: clap requires them).
    if args.paths.is_empty() {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_no_paths",
            "Refusing to discard: discard needs at least one path (got none)",
        )
        .into());
    }
    if args.paths.iter().any(|p| p.trim().is_empty()) {
        return Err(crate::core::error::AgentRunError::invalid(
            "discard_no_paths",
            "Refusing to discard: at least one non-empty PATH is required",
        )
        .into());
    }

    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    // Deleting files from a running job's worktree would race the agent.
    if status == JobStatus::Running {
        let pid_str = meta
            .pid
            .map(|p| format!(" (PID {})", p))
            .unwrap_or_default();
        return Err(crate::core::error::AgentRunError::invalid(
            "job_running",
            format!(
                "Cannot discard from job '{}': job is currently running{}. Terminate it first using 'agent-run kill '{}'.",
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

    // Validate everything first: a guard failure must not leave a partial
    // deletion behind.
    let plan: Vec<(String, PathBuf)> = args
        .paths
        .iter()
        .map(|raw| {
            let cleaned = validate_discard_path(raw)?;
            let target = check_discardable(&meta.worktree_path, &cleaned)?;
            Ok((cleaned, target))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut removed: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for (path, target) in &plan {
        if !seen.insert(target.clone()) {
            continue;
        }
        std::fs::remove_file(target).with_context(|| {
            format!(
                "Failed to delete '{}' from worktree {}",
                target.display(),
                meta.worktree_path.display()
            )
        })?;
        removed.push((*path).clone());
    }

    let mut output = CommandOutput::new(json!({
        "job_id": meta.job_id,
        "removed": removed,
    }));
    for path in &removed {
        output.push_out(format!(
            "Deleted {} from the worktree of job '{}'.",
            path, args.job_id
        ));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(dir)
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

    /// Repo + worktree fixture: one tracked file, one untracked file, and a
    /// saved job record. Returns (state_dir, worktree).
    fn discard_fixture(temp: &Path, pid: Option<u32>) -> (PathBuf, PathBuf) {
        let state_dir = temp.join("state");
        let ws_dir = temp.join("ws");
        let repo_dir = ws_dir.join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        git(&repo_dir, &["init", "-b", "main"]);
        git(&repo_dir, &["config", "user.name", "Test"]);
        git(&repo_dir, &["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("tracked.txt"), "tracked\n").unwrap();
        git(&repo_dir, &["add", "tracked.txt"]);
        git(&repo_dir, &["commit", "-m", "initial commit"]);

        let wt = ws_dir.join("wt_discard");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/discardjob", "main").unwrap();
        std::fs::write(wt.join("scratch.txt"), "tmp\n").unwrap();

        let meta = JobMetadata {
            job_id: "discardjob".to_string(),
            project: "myrepo".to_string(),
            prompt: "scratch files".to_string(),
            resolved_prompt: "scratch files".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: wt.clone(),
            branch: "agent-run/discardjob".to_string(),
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
            engine: None,
        };
        meta.save(&state_dir).unwrap();
        (state_dir, wt)
    }

    fn disc_args(paths: &[&str]) -> DiscardArgs {
        DiscardArgs {
            job_id: "discardjob".to_string(),
            paths: paths.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_validate_discard_path_rejects_dangerous_input() {
        // Globs, absolute paths, `..` and the empty/blank name are all refused.
        for bad in [
            "*.log",
            "scr[0-9]at.txt",
            "s?atch.txt",
            "/etc/passwd",
            "a/../b.txt",
            "..",
            "   ",
        ] {
            let err = validate_discard_path(bad).unwrap_err();
            // Whitespace-only input is rejected up front as blank ("must not be
            // empty"), which carries no "Refusing to discard" prefix -- so the
            // exact wording is only asserted for the dangerous-path cases.
            if bad != "   " {
                assert!(
                    err.to_string().contains("Refusing to discard"),
                    "'{}' must be refused, got: {}",
                    bad,
                    err
                );
            }
        }
        // Plain relative paths pass (and leading `./` is normalized).
        assert_eq!(validate_discard_path("src/a.txt").unwrap(), "src/a.txt");
        assert_eq!(validate_discard_path("./a.txt").unwrap(), "a.txt");
    }

    #[test]
    fn test_check_discardable_guards() {
        let temp = tempdir().unwrap();

        let repo_dir = temp.path().join("repo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        git(&repo_dir, &["init", "-b", "main"]);
        git(&repo_dir, &["config", "user.name", "Test"]);
        git(&repo_dir, &["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("tracked.txt"), "t\n").unwrap();
        git(&repo_dir, &["add", "tracked.txt"]);
        git(&repo_dir, &["commit", "-m", "init"]);
        let wt = temp.path().join("wt");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/guards", "main").unwrap();
        std::fs::write(wt.join("untracked.txt"), "u\n").unwrap();
        std::fs::create_dir_all(wt.join("subdir")).unwrap();
        // A symlink escaping the worktree.
        let outside = temp.path().join("outside.txt");
        std::fs::write(&outside, "do not touch\n").unwrap();
        std::os::unix::fs::symlink(&outside, wt.join("escape.txt")).unwrap();
        // A symlink to a TRACKED file inside the worktree: the dangerous case
        // (deleting it would delete the tracked target).
        std::os::unix::fs::symlink("tracked.txt", wt.join("link.txt")).unwrap();

        // Valid: the untracked file resolves and is kept.
        assert!(check_discardable(&wt, "untracked.txt").is_ok());

        // Tracked file: refused, and the message points at git.
        let err = check_discardable(&wt, "tracked.txt").unwrap_err();
        assert!(err.to_string().contains("tracked"), "got: {}", err);

        // Directory: refused.
        let err = check_discardable(&wt, "subdir").unwrap_err();
        assert!(err.to_string().contains("is a directory"), "got: {}", err);

        // Missing file: refused.
        let err = check_discardable(&wt, "nope.txt").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {}", err);

        // Symlink escaping the worktree: refused, target untouched.
        let err = check_discardable(&wt, "escape.txt").unwrap_err();
        assert!(
            err.to_string()
                .contains("refusing to discard through a symlink"),
            "got: {}",
            err
        );
        assert!(outside.exists(), "symlink target must survive the refusal");

        // Symlink to a tracked file: refused (the symlink name is untracked,
        // but tracking must not matter to discard), tracked target survives.
        let err = check_discardable(&wt, "link.txt").unwrap_err();
        assert!(
            err.to_string()
                .contains("refusing to discard through a symlink"),
            "got: {}",
            err
        );
        assert!(
            wt.join("tracked.txt").exists(),
            "tracked target must survive"
        );
    }

    #[test]
    fn test_discard_refuses_symlinked_parent_directory() {
        // The high-severity repro: an UNTRACKED parent-directory symlink that
        // points at a TRACKED directory. The final component is a regular file
        // and the parent canonicalizes inside the worktree, so the old guards
        // let `discard linkdir/tracked-in-dir.txt` delete the tracked file.
        let temp = tempdir().unwrap();
        let (state_dir, wt) = discard_fixture(temp.path(), None);
        // realdir lives INSIDE the worktree and holds a committed file;
        // linkdir is an untracked symlink to it (the exact repro).
        let realdir = wt.join("realdir");
        std::fs::create_dir_all(&realdir).unwrap();
        std::fs::write(realdir.join("tracked-in-dir.txt"), "real\n").unwrap();
        git(&wt, &["add", "realdir/tracked-in-dir.txt"]);
        git(&wt, &["commit", "-m", "tracked dir"]);
        std::os::unix::fs::symlink("realdir", wt.join("linkdir")).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: state_dir
                .parent()
                .unwrap()
                .to_path_buf()
                .join(".agent-repos"),
            workspace_root: state_dir.parent().unwrap().to_path_buf(),
            ..Default::default()
        };
        let err = run(&config, &disc_args(&["linkdir/tracked-in-dir.txt"])).unwrap_err();
        assert!(
            err.to_string()
                .contains("refusing to discard through a symlink"),
            "got: {}",
            err
        );
        assert!(
            realdir.join("tracked-in-dir.txt").exists(),
            "the TRACKED file behind the symlinked parent must survive"
        );
        assert!(
            wt.join("scratch.txt").exists(),
            "all-or-nothing: nothing else touched"
        );
        assert!(
            wt.join("linkdir").symlink_metadata().is_ok(),
            "the symlink itself must remain for the user to remove"
        );
    }

    #[test]
    fn test_discard_refuses_nested_symlink_component() {
        // A symlink as an INTERMEDIATE component several levels deep
        // (a/b/link where link -> realdir holding c) must also be refused,
        // even though the final component is a regular file.
        let temp = tempdir().unwrap();
        let (_, wt) = discard_fixture(temp.path(), None);
        let realdir = temp.path().join("realdir");
        std::fs::create_dir_all(&realdir).unwrap();
        std::fs::write(realdir.join("c.txt"), "c\n").unwrap();
        std::fs::create_dir_all(wt.join("a/b")).unwrap();
        std::os::unix::fs::symlink(&realdir, wt.join("a/b/link")).unwrap();

        let err = check_discardable(&wt, "a/b/link/c.txt").unwrap_err();
        assert!(
            err.to_string()
                .contains("refusing to discard through a symlink"),
            "got: {}",
            err
        );
        assert!(realdir.join("c.txt").exists(), "target file must survive");
    }

    #[test]
    fn test_discard_refuses_symlinked_parent_pointing_outside() {
        // An untracked parent symlink pointing OUTSIDE the worktree (to a dir
        // that happens to hold a regular file) must be refused for the same
        // reason -- even though the old parent-canonicalize guard would catch
        // it too, the refusal must be the symlink refusal.
        let temp = tempdir().unwrap();
        let (state_dir, wt) = discard_fixture(temp.path(), None);
        let outside_dir = temp.path().join("outsidedir");
        std::fs::create_dir_all(&outside_dir).unwrap();
        std::fs::write(outside_dir.join("victim.txt"), "v\n").unwrap();
        std::os::unix::fs::symlink(&outside_dir, wt.join("linkout")).unwrap();
        let _ = state_dir;

        let err = check_discardable(&wt, "linkout/victim.txt").unwrap_err();
        assert!(
            err.to_string()
                .contains("refusing to discard through a symlink"),
            "got: {}",
            err
        );
        assert!(
            outside_dir.join("victim.txt").exists(),
            "outside file must survive"
        );
    }

    #[test]
    fn test_discard_refuses_symlink_to_tracked_file() {
        // End-to-end guard: discard through an untracked in-worktree symlink
        // must be refused, and the TRACKED target it points at must survive.
        let temp = tempdir().unwrap();
        let (state_dir, wt) = discard_fixture(temp.path(), None);
        std::os::unix::fs::symlink("tracked.txt", wt.join("link.txt")).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: state_dir
                .parent()
                .unwrap()
                .to_path_buf()
                .join(".agent-repos"),
            workspace_root: state_dir.parent().unwrap().to_path_buf(),
            ..Default::default()
        };
        let err = run(&config, &disc_args(&["link.txt"])).unwrap_err();
        assert!(
            err.to_string()
                .contains("refusing to discard through a symlink"),
            "got: {}",
            err
        );
        assert!(
            wt.join("tracked.txt").exists(),
            "TRACKED target must survive a refused symlink discard"
        );
        assert!(
            wt.join("scratch.txt").exists(),
            "all-or-nothing: nothing else touched"
        );
        assert!(
            wt.join("link.txt").symlink_metadata().is_ok(),
            "the symlink itself must remain for the user to remove"
        );
    }

    #[test]
    fn test_discard_removes_only_the_named_untracked_files() {
        let temp = tempdir().unwrap();
        let (state_dir, wt) = discard_fixture(temp.path(), None);
        std::fs::write(wt.join("keep.txt"), "keep\n").unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: state_dir
                .parent()
                .unwrap()
                .to_path_buf()
                .join(".agent-repos"),
            workspace_root: state_dir.parent().unwrap().to_path_buf(),
            ..Default::default()
        };
        let out = run(&config, &disc_args(&["scratch.txt", "keep.txt"])).unwrap();
        assert_eq!(
            out.data["removed"],
            json!(["scratch.txt", "keep.txt"]),
            "both named files must be reported removed"
        );
        assert!(!wt.join("scratch.txt").exists());
        assert!(!wt.join("keep.txt").exists());
    }

    #[test]
    fn test_discard_refuses_tracked_file_and_leaves_it() {
        let temp = tempdir().unwrap();
        let (state_dir, wt) = discard_fixture(temp.path(), None);

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: state_dir
                .parent()
                .unwrap()
                .to_path_buf()
                .join(".agent-repos"),
            workspace_root: state_dir.parent().unwrap().to_path_buf(),
            ..Default::default()
        };
        // The worktree's tracked file: refused, file survives.
        let err = run(&config, &disc_args(&["tracked.txt"])).unwrap_err();
        assert!(err.to_string().contains("tracked"), "got: {}", err);
        assert!(wt.join("tracked.txt").exists());

        // A second, untracked path in the same call must not be deleted
        // either (all-or-nothing validation).
        std::fs::write(wt.join("second.txt"), "s\n").unwrap();
        let err = run(&config, &disc_args(&["tracked.txt", "second.txt"])).unwrap_err();
        assert!(err.to_string().contains("tracked"), "got: {}", err);
        assert!(
            wt.join("second.txt").exists(),
            "validation must be all-or-nothing"
        );
    }

    #[test]
    fn test_discard_refuses_running_job() {
        let temp = tempdir().unwrap();
        let (state_dir, wt) = discard_fixture(temp.path(), Some(std::process::id()));

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: state_dir
                .parent()
                .unwrap()
                .to_path_buf()
                .join(".agent-repos"),
            workspace_root: state_dir.parent().unwrap().to_path_buf(),
            ..Default::default()
        };
        let err = run(&config, &disc_args(&["scratch.txt"])).unwrap_err();
        assert!(
            err.to_string().contains("currently running"),
            "got: {}",
            err
        );
        assert!(wt.join("scratch.txt").exists());
    }

    #[test]
    fn test_discard_refuses_glob_at_run_level() {
        let temp = tempdir().unwrap();
        let (state_dir, _wt) = discard_fixture(temp.path(), None);
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: state_dir
                .parent()
                .unwrap()
                .to_path_buf()
                .join(".agent-repos"),
            workspace_root: state_dir.parent().unwrap().to_path_buf(),
            ..Default::default()
        };
        let err = run(&config, &disc_args(&["*"])).unwrap_err();
        assert!(err.to_string().contains("globs"), "got: {}", err);
    }

    #[test]
    fn test_discard_empty_paths_is_error() {
        // An HTTP body that omits `paths` deserializes to an empty selection
        // (serde default); the command must refuse it instead of succeeding
        // as a no-op. The check runs BEFORE any job lookup, so an unknown job
        // id still surfaces the empty-paths error (not "No such job found").
        let err = run(&Config::default(), &disc_args(&[])).unwrap_err();
        assert!(
            err.to_string().contains("discard needs at least one path"),
            "got: {}",
            err
        );
    }
}

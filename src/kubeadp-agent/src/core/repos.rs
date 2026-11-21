//! Per-project private repositories and worktree repair.
//!
//! Job worktrees must never be registered in the git-sync-owned project
//! checkout: git-sync deletes and re-creates its repo directory on every
//! restart (`repo directory was empty or failed checks` -> `initializing
//! repo directory`), which orphans every worktree admin directory under
//! its `.git/worktrees/` and leaves each job worktree's `.git` file
//! dangling. This module gives agent-run its own bare repository per
//! project (`<repos_root>/<project>.git`, default
//! `<workspace_root>/.agent-repos`), keeps it current by shallow-fetching
//! from the same source the project checkout uses, and re-attaches
//! orphaned job worktrees to it (`repair_worktree`).

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{bail, Context, Result};

use crate::core::config::Config;
use crate::core::error::AgentRunError;
use crate::core::job::JobMetadata;

/// File lock (create_new + poll) serialising private-repo creation and
/// fetch within one project. A holder that dies with the lock file still
/// in place is stolen after [`STALE_LOCK_SECS`].
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(50);
const LOCK_TIMEOUT: Duration = Duration::from_secs(60);
const STALE_LOCK_SECS: u64 = 30;

/// The lock file name inside a private bare repo. It sits at the bare repo
/// root where git keeps no bookkeeping that could collide with it.
const LOCK_FILE: &str = "agent-run.lock";
const REPAIR_TMP_DIR: &str = ".wt-repair";
/// The canonical local branch of the private repo (all ref-based worktrees
/// start from it, mirroring `default_base_branch`).
const LOCAL_BASE_BRANCH: &str = "main";

/// RAII holder for the per-project private-repo lock (removes the lock
/// file on drop so the next start does not wait it out).
struct RepoLock {
    path: PathBuf,
}

impl RepoLock {
    fn acquire(repo_dir: &Path) -> Result<Self> {
        use std::io::Write;
        let path = repo_dir.join(LOCK_FILE);
        let deadline = Instant::now() + LOCK_TIMEOUT;
        loop {
            match fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
            {
                Ok(mut f) => {
                    let _ = writeln!(f, "{}", std::process::id());
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let holder_alive = fs::read_to_string(&path)
                        .ok()
                        .and_then(|s| s.trim().parse::<u32>().ok())
                        .is_some_and(crate::core::job::is_pid_alive);
                    let stale = !holder_alive
                        && fs::metadata(&path)
                            .and_then(|m| m.modified())
                            .ok()
                            .and_then(|t| SystemTime::now().duration_since(t).ok())
                            .is_some_and(|age| age.as_secs() > STALE_LOCK_SECS);
                    if stale {
                        let _ = fs::remove_file(&path);
                    } else if Instant::now() > deadline {
                        bail!(
                            "timed out waiting for the private repo lock {}",
                            path.display()
                        );
                    } else {
                        std::thread::sleep(LOCK_POLL_INTERVAL);
                    }
                }
                Err(e) => {
                    bail!(
                        "failed to acquire the private repo lock {}: {}",
                        path.display(),
                        e
                    )
                }
            }
        }
    }
}

impl Drop for RepoLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Runs git in `repo_dir` with `args`, mapping failure to a context-
/// carrying error. `quiet` suppresses stdout (stderr is always captured).
fn git_run(repo_dir: &Path, args: &[&str], quiet: bool) -> Result<()> {
    let mut cmd = crate::core::git::hardened_git();
    cmd.args(["-C", &repo_dir.to_string_lossy()]);
    for a in args {
        cmd.arg(a);
    }
    if quiet {
        cmd.stdout(std::process::Stdio::null());
    }
    let out = crate::core::reaper::output_tracked(&mut cmd).with_context(|| {
        format!(
            "failed to execute 'git {}' in {}",
            args.join(" "),
            repo_dir.display()
        )
    })?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        bail!(
            "git {} failed in {}: {}",
            args.join(" "),
            repo_dir.display(),
            stderr
        );
    }
    Ok(())
}

/// Captures the trimmed stdout of a git call; `None` on failure or empty
/// output.
fn git_stdout(repo_dir: &Path, args: &[&str]) -> Option<String> {
    let mut cmd = crate::core::git::hardened_git();
    cmd.args(["-C", &repo_dir.to_string_lossy()]).args(args);
    let out = crate::core::reaper::output_tracked(&mut cmd).ok()?;
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

/// Whether `ref_at` resolves to a commit in `repo_dir`.
fn ref_exists(repo_dir: &Path, ref_at: &str) -> bool {
    git_stdout(
        repo_dir,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{ref_at}^{{commit}}"),
        ],
    )
    .is_some()
}

/// Strips embedded userinfo (`scheme://user:token@host...`) from a remote
/// URL so a credential is never persisted into a repo's config. The pod
/// supplies its token through the git credential helper environment
/// instead; a clean URL keeps resolving to that helper.
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

/// The remote the private repo fetches from: the project checkout's
/// `origin`, or the checkout directory itself when it has no origin remote
/// (local test repos, and the fallback for checkouts whose origin is
/// unavailable).
pub fn repo_source(project_dir: &Path) -> String {
    if let Some(url) = git_stdout(project_dir, &["remote", "get-url", "origin"]) {
        strip_url_userinfo(&url)
    } else {
        project_dir.to_string_lossy().to_string()
    }
}

/// The private bare repository path for a project:
/// `<repos_root>/<project>.git`.
pub fn private_repo_path(config: &Config, project: &str) -> PathBuf {
    config.repos_root.join(format!("{}.git", project))
}

/// The git repository to use for a project's job-worktree bookkeeping:
/// the private repo when it exists (jobs started with the private-repo
/// layout), otherwise the legacy project checkout directory (jobs created
/// before the private repo existed still point their admin directories at
/// the git-sync repo).
pub fn project_git_repo(config: &Config, project: &str) -> PathBuf {
    let private = private_repo_path(config, project);
    if private.exists() {
        private
    } else {
        config.workspace_root.join(project)
    }
}

/// The git repository to use for the bookkeeping of `meta`'s worktree.
pub fn job_git_repo(config: &Config, meta: &JobMetadata) -> PathBuf {
    project_git_repo(config, &meta.project)
}

fn is_full_sha(base_ref: &str) -> bool {
    base_ref.len() == 40 && base_ref.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The fetch arguments for the private repo: shallow (`--depth=1`) by default,
/// but an already complete repo (not shallow, with fetched refs, e.g. one an
/// operator unshallowed because a job needs older history such as a fork
/// point) is fetched incrementally, so a start never cuts its history again.
fn fetch_args<'a>(repo: &Path, rest: &[&'a str]) -> Vec<&'a str> {
    let complete = git_stdout(repo, &["rev-parse", "--is-shallow-repository"]).as_deref()
        == Some("false")
        && git_stdout(repo, &["for-each-ref", "--count=1", "refs/remotes"])
            .map(|o| !o.trim().is_empty())
            .unwrap_or(false);
    let mut args = vec!["fetch", "-q"];
    if !complete {
        args.push("--depth=1");
    }
    args.extend_from_slice(rest);
    args
}

/// Creates (first use) and refreshes the project's private bare repository,
/// guaranteeing that `base_ref` (a branch name, "HEAD", or a full commit
/// SHA) is present locally and, for a ref-based base, that the repo's HEAD
/// resolves to the fetched tip. Shallow fetches (`--depth=1`) keep the
/// repo small. All work runs under a per-project file lock so concurrent
/// `start`s do not race. Returns the repo path.
pub fn ensure_private_repo(
    config: &Config,
    project: &str,
    project_dir: &Path,
    base_ref: &str,
) -> Result<PathBuf> {
    let repo = private_repo_path(config, project);
    fs::create_dir_all(&config.repos_root).with_context(|| {
        format!(
            "Failed to create private repos root {}",
            config.repos_root.display()
        )
    })?;
    // The lock file sits in the bare repo root, which first-use has not
    // created yet: make sure it exists before the lock can be dropped in.
    fs::create_dir_all(&repo)
        .with_context(|| format!("failed to create private repo {}", repo.display()))?;
    let _lock = RepoLock::acquire(&repo)?;

    let source = repo_source(project_dir);
    if !repo.join("HEAD").exists() {
        let repo_display = repo.display().to_string();
        fs::create_dir_all(&repo).with_context(|| format!("failed to create {}", repo_display))?;
        git_run(&repo, &["init", "-q", "--bare"], false)
            .with_context(|| format!("failed to 'git init --bare' {}", repo_display))?;
        if git_stdout(&repo, &["remote", "get-url", "origin"]).is_none() {
            git_run(&repo, &["remote", "add", "origin", &source], true)?;
        }
        git_run(
            &repo,
            &[
                "config",
                "remote.origin.fetch",
                "+refs/heads/*:refs/remotes/origin/*",
            ],
            true,
        )
        .context("failed to set the private repo fetch refspec")?;
    }

    if is_full_sha(base_ref) {
        // Concrete objects are immutable: fetch only when missing. Fetching
        // a reachable SHA works against a local source by default and
        // against hosts (e.g. GitHub) that allow it.
        if !ref_exists(&repo, base_ref) {
            let mut first_error: Option<String> = None;
            git_run(&repo, &fetch_args(&repo, &["origin", base_ref]), true).or_else(|e| {
                first_error = Some(e.to_string());
                git_run(&repo, &fetch_args(&repo, &[&source, base_ref]), true)
            })?;
            if !ref_exists(&repo, base_ref) {
                bail!(
                    "commit SHA '{base_ref}' is not present in the source {source} (fetch failed or the object is unreachable): {}",
                    first_error
                        .unwrap_or_else(|| "the fetch reported success but the object is still missing".to_string())
                );
            }
        }
    } else {
        // Ref-based base: always re-fetch (shallow) so the private repo
        // tracks the source, and point the canonical local branch at the
        // fetched tip so HEAD-based resolutions stay current.
        let target = if base_ref == "HEAD" { "HEAD" } else { base_ref };
        let mut first_error: Option<String> = None;
        let fetch_result =
            git_run(&repo, &fetch_args(&repo, &["origin", target]), true).or_else(|e| {
                first_error = Some(e.to_string());
                git_run(&repo, &fetch_args(&repo, &[&source, target]), true)
            });
        let first_error_detail = first_error.clone().unwrap_or_else(|| {
            "the fetch reported success but the object is still missing".to_string()
        });
        fetch_result.with_context(|| {
            format!(
                "failed to fetch '{base_ref}' into the private repo {} from source {} ({})",
                repo.display(),
                source,
                first_error_detail
            )
        })?;
        // Resolve the fetched tip: FETCH_HEAD (a local source always sets
        // it; `fetch <remote> HEAD`/`<branch>` over a URL also does), with
        // fallbacks onto the remote-tracking refs the fetch wrote.
        let origin_ref = if base_ref.starts_with("origin/") {
            base_ref.to_string()
        } else {
            format!("origin/{base_ref}")
        };
        let mut tip: Option<String> = git_stdout(
            &repo,
            &["rev-parse", "--verify", "--quiet", "FETCH_HEAD^{commit}"],
        );
        if tip.is_none() {
            tip = git_stdout(
                &repo,
                &[
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    &format!("{origin_ref}^{{commit}}"),
                ],
            );
        }
        if tip.is_none() && origin_ref != base_ref {
            tip = git_stdout(
                &repo,
                &[
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    &format!("{base_ref}^{{commit}}"),
                ],
            );
        }
        let tip = tip.ok_or_else(|| {
            anyhow::anyhow!(
                "base ref '{base_ref}' did not resolve in the private repo {} after fetching from source {} ({})",
                repo.display(),
                source,
                first_error.unwrap_or_else(|| "the fetch reported success but the object is still missing".to_string())
            )
        });
        let tip = tip?;
        git_run(
            &repo,
            &[
                "update-ref",
                &format!("refs/heads/{LOCAL_BASE_BRANCH}"),
                &tip,
            ],
            true,
        )?;
        git_run(
            &repo,
            &[
                "symbolic-ref",
                "HEAD",
                &format!("refs/heads/{LOCAL_BASE_BRANCH}"),
            ],
            true,
        )?;
    }

    Ok(repo)
}

/// Reads the `gitdir:` target of a worktree's `.git` file, if it is a file
/// whose first line has the expected shape.
fn gitdir_target(worktree_path: &Path) -> Option<String> {
    crate::core::git::worktree_gitdir_target(worktree_path)
}

/// Whether the worktree's link to its git admin data is dangling: the `.git`
/// file's `gitdir` directory is gone (the git-sync re-init signature), the
/// `.git` file is missing or malformed, or git itself cannot resolve the
/// worktree. A directory `.git` (the worktree IS a repo checkout) is never
/// dangling.
pub fn worktree_gitdir_dangling(worktree_path: &Path) -> bool {
    if !worktree_path.exists() {
        return false;
    }
    let dotgit = worktree_path.join(".git");
    if dotgit.is_dir() {
        return false;
    }
    let Some(target) = gitdir_target(worktree_path) else {
        return true;
    };
    if !Path::new(&target).exists() {
        return true;
    }
    // The gitdir exists but the pair may be inconsistent: probe it.
    let mut cmd = crate::core::git::hardened_git();
    cmd.args([
        "-C",
        &worktree_path.to_string_lossy(),
        "rev-parse",
        "--git-dir",
    ]);
    let out = crate::core::reaper::output_tracked(&mut cmd);
    !matches!(out, Ok(o) if o.status.success())
}

/// The result of [`repair_worktree`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairOutcome {
    /// The worktree already works (a no-op).
    Healthy,
    /// The worktree was re-attached to the private repository.
    Repaired,
    /// The job's worktree was supplied externally (`--from-worktree`): not
    /// ours to repair.
    External,
    /// The worktree directory does not exist on disk.
    Absent,
}

impl RepairOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RepairOutcome::Healthy => "healthy",
            RepairOutcome::Repaired => "repaired",
            RepairOutcome::External => "external",
            RepairOutcome::Absent => "absent",
        }
    }
}

/// Re-attaches a job's orphaned worktree to the project's private
/// repository.
///
/// Detection: the worktree's `.git` file points at a gitdir directory that
/// no longer exists (git-sync re-created its repo and dropped the admin
/// directory). Repair, without ever touching the files on disk:
///
/// 1. ensure the project's private repo exists and contains the job's base
///    commit (fetching `--depth=1` the exact ref/SHA from the source when
///    the private repo does not have it); a base that cannot be fetched is
///    a hard error naming the job and the base -- nothing is guessed,
/// 2. create a fresh admin directory by adding a throwaway detached,
///    un-checked-out worktree at a temp path whose basename is the job id,
/// 3. rewrite the job worktree's `.git` file to point at the new admin dir
///    and the admin dir's `gitdir` file back at the job worktree, and drop
///    the temp directory,
/// 4. run a MIXED `git reset` in the job worktree so its index matches the
///    base commit and the on-disk files show as the job's real changes
///    (modified tracked files as `M`, new files as `??`, deleted as `D`).
///
/// `git checkout`, `git clean` and `git reset --hard` are never run in a
/// job worktree; no file content is read from or written to the worktree.
/// Repairing a healthy worktree is a no-op.
pub fn repair_worktree(config: &Config, meta: &JobMetadata) -> Result<RepairOutcome> {
    if meta.external_worktree {
        return Ok(RepairOutcome::External);
    }
    if !meta.worktree_path.exists() {
        return Ok(RepairOutcome::Absent);
    }
    if !worktree_gitdir_dangling(&meta.worktree_path) {
        return Ok(RepairOutcome::Healthy);
    }

    let project_dir = config.workspace_root.join(&meta.project);
    if !project_dir.is_dir() {
        bail!(
            "cannot repair worktree for job '{}': project directory {} does not exist",
            meta.job_id,
            project_dir.display()
        );
    }

    let base = meta.base_ref.trim();
    if base.is_empty() {
        bail!(
            "cannot repair worktree for job '{}': the job row has no base ref to re-attach against",
            meta.job_id
        );
    }

    // Fetching the base from the source can fail when the object no longer
    // exists anywhere (the source branch was rewritten / force-pushed):
    // that must surface as a clear error naming the job and the base, never
    // as a guess at a different commit.
    let repo = match ensure_private_repo(config, &meta.project, &project_dir, base) {
        Ok(repo) => repo,
        Err(err) => {
            return Err(AgentRunError::invalid(
                "repair_base_not_found",
                format!(
                    "Cannot repair worktree for job '{}': base commit '{base}' could not be fetched into the private repository {} ({})",
                    meta.job_id,
                    private_repo_path(config, &meta.project).display(),
                    err
                ),
            )
            .into());
        }
    };
    let base_sha = git_stdout(
        &repo,
        &["rev-parse", "--verify", "--quiet", &format!("{base}^{{commit}}")],
    )
    .or_else(|| {
        git_stdout(
            &repo,
            &["rev-parse", "--verify", "--quiet", &format!("origin/{base}^{{commit}}")],
        )
    })
    .ok_or_else(|| {
        AgentRunError::invalid(
            "repair_base_not_found",
            format!(
                "Cannot repair worktree for job '{}': base commit '{base}' does not exist in the private repository {} and could not be fetched from the project source; refusing to guess a different commit",
                meta.job_id,
                repo.display()
            ),
        )
    })?;

    let admin_dir = repo.join("worktrees").join(&meta.job_id);
    // A half-finished previous repair may have left a stale admin entry.
    if admin_dir.exists() {
        fs::remove_dir_all(&admin_dir).with_context(|| {
            format!(
                "failed to remove stale worktree admin dir {}",
                admin_dir.display()
            )
        })?;
    }

    // Step 2: a throwaway worktree whose admin dir is exactly
    // <repo>/worktrees/<job_id>; --no-checkout keeps every working-tree
    // file (including the job's own) untouched.
    let tmp_root = repo.join(REPAIR_TMP_DIR);
    fs::create_dir_all(&tmp_root)
        .with_context(|| format!("failed to create {}", tmp_root.display()))?;
    let tmp_wt = tmp_root.join(&meta.job_id);
    if tmp_wt.exists() {
        fs::remove_dir_all(&tmp_wt).with_context(|| {
            format!(
                "failed to remove stale repair temp dir {}",
                tmp_wt.display()
            )
        })?;
    }
    if let Err(e) = git_run(
        &repo,
        &[
            "worktree",
            "add",
            "--no-checkout",
            "--detach",
            tmp_wt.to_string_lossy().as_ref(),
            &base_sha,
        ],
        true,
    ) {
        let _ = fs::remove_dir_all(&tmp_wt);
        return Err(e);
    }

    // Step 3: swap the gitdir pair, then drop the empty temp dir.
    let dotgit = meta.worktree_path.join(".git");
    fs::write(&dotgit, format!("gitdir: {}\n", admin_dir.display()))
        .with_context(|| format!("failed to rewrite {}", dotgit.display()))?;
    fs::write(admin_dir.join("gitdir"), format!("{}\n", dotgit.display()))
        .with_context(|| format!("failed to rewrite {}", admin_dir.join("gitdir").display()))?;
    if let Err(e) = fs::remove_dir_all(&tmp_wt) {
        let _ = fs::remove_file(&dotgit);
        return Err(e)
            .with_context(|| format!("failed to remove the repair temp dir {}", tmp_wt.display()));
    }

    // Step 4: rebuild the INDEX only (MIXED reset -- never --hard) so the
    // on-disk files show as changes against the base commit.
    git_run(&meta.worktree_path, &["reset", "-q"], true).with_context(|| {
        format!(
            "failed to re-index the repaired worktree {}",
            meta.worktree_path.display()
        )
    })?;
    git_run(&meta.worktree_path, &["status", "--porcelain"], true).with_context(|| {
        format!(
            "the repaired worktree {} still fails 'git status'",
            meta.worktree_path.display()
        )
    })?;

    Ok(RepairOutcome::Repaired)
}

/// Fire-and-forget auto-repair used once per request by the server-side
/// command paths: repairs a dangling worktree in place, logs a one-line
/// notice when it acts, and never turns a repair failure into a request
/// failure (the subsequent real git operation then reports the underlying
/// problem). Healthy, external and absent worktrees are silent no-ops.
pub fn auto_repair_worktree(config: &Config, meta: &JobMetadata) {
    if meta.external_worktree || !meta.worktree_path.exists() {
        return;
    }
    if !worktree_gitdir_dangling(&meta.worktree_path) {
        return;
    }
    match repair_worktree(config, meta) {
        Ok(RepairOutcome::Repaired) => {
            let repo = private_repo_path(config, &meta.project);
            eprintln!(
                "[agent-run] repaired orphaned git worktree for job '{}' (re-attached to private repo {})",
                meta.job_id,
                repo.display()
            );
        }
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "[agent-run] warning: automatic worktree repair failed for job '{}': {}",
                meta.job_id, e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::git::create_worktree;
    use crate::core::job::{ensure_state_store, JobMetadata};
    use chrono::Utc;
    use std::process::Command as StdCommand;

    /// A non-bare "source" checkout: `depth` commits on `main`, each adding
    /// a versioned line to `file.txt`. With `extra_files` a second tracked
    /// file (`extra.txt`) exists in every commit. When `with_origin` is
    /// set an `origin` remote pointing at a bare mirror is configured.
    fn make_source(
        root: &Path,
        name: &str,
        depth: usize,
        extra_files: bool,
        with_origin: bool,
    ) -> PathBuf {
        let src = root.join(name);
        fs::create_dir_all(&src).unwrap();
        let gitc = |dir: &Path, args: &[&str]| {
            let out = StdCommand::new("git")
                .current_dir(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        gitc(&src, &["init", "-q", "-b", "main"]);
        gitc(&src, &["config", "user.name", "Test"]);
        gitc(&src, &["config", "user.email", "test@test.com"]);
        for i in 0..depth.max(1) {
            fs::write(src.join("file.txt"), format!("v{i}\n")).unwrap();
            if extra_files {
                fs::write(src.join("extra.txt"), format!("x{i}\n")).unwrap();
            }
            gitc(&src, &["add", "."]);
            gitc(&src, &["commit", "-q", "-m", &format!("commit {i}")]);
        }
        if with_origin {
            let origin = root.join(format!("{name}-origin.git"));
            gitc(
                &src,
                &[
                    "clone",
                    "-q",
                    "--bare",
                    src.to_str().unwrap(),
                    origin.to_str().unwrap(),
                ],
            );
            gitc(&src, &["remote", "add", "origin", origin.to_str().unwrap()]);
        }
        src
    }

    fn git_stdout_at(dir: &Path, args: &[&str]) -> String {
        let out = StdCommand::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?} failed in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn head_sha(dir: &Path) -> String {
        git_stdout_at(dir, &["rev-parse", "HEAD"])
    }

    fn test_config(root: &Path) -> Config {
        let workspace_root = root.join("workspace");
        fs::create_dir_all(&workspace_root).unwrap();
        // Keep the private repos (and their lock files) inside the temp
        // dir instead of the default workspace root.
        let repos_root = workspace_root.join(".agent-repos");
        Config {
            workspace_root,
            repos_root,
            state_root: root.join("state"),
            ..Default::default()
        }
    }

    fn test_meta(job_id: &str, project: &str, worktree: PathBuf, base_ref: String) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            project: project.to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: worktree,
            branch: format!("agent-run/{job_id}"),
            base_ref,
            conversation_id: None,
            retry_count: 0,
            pid: None,
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
        }
    }

    /// The standard incident shape: a job worktree of the SOURCE checkout
    /// (as pre-fix jobs were created) whose admin directory has been
    /// deleted, with the job's on-disk changes in place.
    fn make_dangling_job_wt(config: &Config, src: &Path, base: &str, job_id: &str) -> PathBuf {
        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join(job_id);
        fs::create_dir_all(wt.parent().unwrap()).unwrap();
        let out = StdCommand::new("git")
            .current_dir(src)
            .args(["worktree", "add", "--detach", &wt.to_string_lossy(), base])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "setup worktree add failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        fs::write(wt.join("file.txt"), "job edit\n").unwrap();
        fs::write(wt.join("new-file.txt"), "brand new\n").unwrap();
        // The incident: the admin directory disappears.
        let admin_path = gitdir_target(&wt).unwrap();
        let admin = Path::new(&admin_path);
        fs::remove_dir_all(admin).unwrap();
        assert!(worktree_gitdir_dangling(&wt));
        wt
    }

    #[test]
    fn test_private_repo_created_and_fetched_from_source() {
        let root = tempfile::tempdir().unwrap();
        let src = make_source(root.path(), "src-repo", 2, false, false);
        let config = test_config(root.path());
        // A project checkout WITHOUT an origin remote: the fetch source
        // must fall back to the project directory itself.
        let project_dir = config.workspace_root.join("proj");
        let out = StdCommand::new("git")
            .args([
                "clone",
                "-q",
                src.to_str().unwrap(),
                project_dir.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(out.status.success(), "clone failed");
        git_stdout_at(&project_dir, &["remote", "remove", "origin"]);
        assert!(git_stdout(&project_dir, &["remote", "get-url", "origin"]).is_none());

        let repo = ensure_private_repo(&config, "proj", &project_dir, "HEAD").unwrap();
        assert_eq!(repo, private_repo_path(&config, "proj"));
        assert!(repo.join("HEAD").exists());
        let remote = git_stdout(&repo, &["remote", "get-url", "origin"]).unwrap();
        assert_eq!(
            remote,
            project_dir.to_string_lossy().to_string(),
            "without an origin remote the source must be the project dir"
        );
        assert!(ref_exists(&repo, "HEAD"));
        assert_eq!(
            git_stdout(&repo, &["rev-parse", "HEAD^{commit}"]),
            Some(head_sha(&project_dir))
        );

        // A second ensure with a concrete branch name must keep the repo
        // current and leave HEAD resolvable at the same (only) tip.
        ensure_private_repo(&config, "proj", &project_dir, "main").unwrap();
        assert_eq!(
            git_stdout(&repo, &["rev-parse", "HEAD^{commit}"]),
            Some(head_sha(&project_dir))
        );
        // A URL with embedded credentials must be stored without them.
    }

    #[test]
    fn test_credentials_are_stripped_from_stored_remote_url() {
        let root = tempfile::tempdir().unwrap();
        let origin = make_source(root.path(), "origin.git", 1, false, false);
        // Make the origin LOOK like a token-authenticated URL by giving the
        // source a token-bearing origin of its own (a local path with
        // userinfo is inert for git; we only test the strip logic here).
        let project_dir = config_project_with_origin(root.path(), &origin);
        assert_eq!(
            repo_source(&project_dir),
            origin.to_string_lossy().to_string()
        );
        assert_eq!(
            strip_url_userinfo("https://git:tok123@github.com/x/y.git"),
            "https://github.com/x/y.git"
        );
        assert_eq!(
            strip_url_userinfo("https://github.com/x/y.git"),
            "https://github.com/x/y.git"
        );
        assert_eq!(strip_url_userinfo("/local/path"), "/local/path");
    }

    fn config_project_with_origin(root: &Path, origin: &Path) -> PathBuf {
        let project_dir = root.join("checkout");
        let out = StdCommand::new("git")
            .args([
                "clone",
                "-q",
                origin.to_str().unwrap(),
                project_dir.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(out.status.success());
        project_dir
    }

    #[test]
    fn test_worktree_add_against_private_repo() {
        let root = tempfile::tempdir().unwrap();
        let src = make_source(root.path(), "proj", 1, false, false);
        let config = test_config(root.path());
        let repo = ensure_private_repo(&config, "proj", &src, "HEAD").unwrap();
        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("job1");
        fs::create_dir_all(wt.parent().unwrap()).unwrap();
        let base = create_worktree(&repo, &wt, "agent-run/job1", "HEAD").unwrap();
        assert!(wt.join(".git").is_file());
        let gitdir = gitdir_target(&wt).unwrap();
        assert!(
            gitdir.starts_with(repo.to_str().unwrap()),
            "the admin dir must live inside the private repo, got {gitdir}"
        );
        let list = git_stdout_at(&repo, &["worktree", "list"]);
        assert!(list.contains(wt.to_str().unwrap()));
        assert_eq!(head_sha(&wt), base);
    }

    #[test]
    fn test_job_worktrees_survive_source_side_reinit() {
        // The incident: the source checkout dir is deleted and recreated
        // from scratch (the git-sync restart). The private repo and the job
        // worktree must stay valid: the admin lives in the private repo,
        // and the job's base SHA was fetched into it at start time.
        let root = tempfile::tempdir().unwrap();
        let src = make_source(root.path(), "proj", 2, false, false);
        let config = test_config(root.path());

        let base_of_job = git_stdout_at(&src, &["rev-parse", "HEAD^"]);
        let repo = ensure_private_repo(&config, "proj", &src, &base_of_job).unwrap();
        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("job1");
        fs::create_dir_all(wt.parent().unwrap()).unwrap();
        create_worktree(&repo, &wt, "agent-run/job1", &base_of_job).unwrap();
        // The job "works": modify a tracked file, add a new one.
        fs::write(wt.join("file.txt"), "job edit\n").unwrap();
        fs::write(wt.join("new-file.txt"), "brand new\n").unwrap();

        // --- git-sync re-init: delete the ENTIRE source dir, recreate it
        // --- from scratch at the same path (depth 1 of a new history).
        fs::remove_dir_all(&src).unwrap();
        let fresh = make_source(root.path(), "proj", 1, false, false);

        // The old job worktree still works and shows its real changes.
        let status = git_stdout_at(&wt, &["status", "--porcelain"]);
        assert!(status.contains("M file.txt"), "got: {status}");
        assert!(status.contains("?? new-file.txt"), "got: {status}");
        assert_eq!(head_sha(&wt), base_of_job);
        assert_eq!(
            fs::read_to_string(wt.join("new-file.txt")).unwrap(),
            "brand new\n"
        );

        // And a NEW start fetches again from the re-created source.
        let new_tip = head_sha(&fresh);
        ensure_private_repo(&config, "proj", &fresh, &new_tip).unwrap();
        let wt2 = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("job2");
        fs::create_dir_all(wt2.parent().unwrap()).unwrap();
        create_worktree(&repo, &wt2, "agent-run/job2", &new_tip).unwrap();
        assert_eq!(head_sha(&wt2), new_tip);
    }

    /// The project checkout lives under the workspace root exactly like in
    /// production (`<workspace_root>/<project>`).
    fn make_workspace_source(root: &Path, depth: usize, extra_files: bool) -> (PathBuf, Config) {
        let config = test_config(root);
        let src = make_source(&config.workspace_root, "proj", depth, extra_files, false);
        (src, config)
    }

    #[test]
    fn test_repair_dangling_worktree_base_present_in_private() {
        let root = tempfile::tempdir().unwrap();
        // Two tracked files so the repair can show M (modified) and D
        // (deleted tracked) plus ?? (new).
        let (src, config) = make_workspace_source(root.path(), 2, true);
        let base_of_job = head_sha(&src);
        let repo = ensure_private_repo(&config, "proj", &src, "HEAD").unwrap();
        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("job1");
        create_worktree(&repo, &wt, "agent-run/job1", "HEAD").unwrap();

        // Job work: modify one tracked file, delete the other, add a new
        // file.
        fs::write(wt.join("file.txt"), "job edit\n").unwrap();
        fs::remove_file(wt.join("extra.txt")).unwrap();
        fs::write(wt.join("new-file.txt"), "brand new\n").unwrap();
        let before_modified = fs::read_to_string(wt.join("file.txt")).unwrap();

        // --- Incident: the admin directory disappears ---
        let admin_path = gitdir_target(&wt).unwrap();
        let admin = Path::new(&admin_path);
        fs::remove_dir_all(admin).unwrap();
        assert!(worktree_gitdir_dangling(&wt));

        let meta = test_meta("job1", "proj", wt.clone(), base_of_job.clone());
        assert_eq!(
            repair_worktree(&config, &meta).unwrap(),
            RepairOutcome::Repaired
        );

        // The worktree is valid again and shows the job's real changes.
        let status = git_stdout_at(&wt, &["status", "--porcelain"]);
        assert!(
            status.contains("M file.txt"),
            "a modified tracked file must show as modified: {status}"
        );
        assert!(
            status.contains("D extra.txt") || status.contains(" D extra.txt"),
            "a deleted tracked file must show as deleted: {status}"
        );
        assert!(
            status.contains("?? new-file.txt"),
            "a new file must show as untracked: {status}"
        );
        // The on-disk files are byte-identical (untouched by the repair).
        assert_eq!(
            fs::read_to_string(wt.join("file.txt")).unwrap(),
            before_modified
        );
        assert_eq!(
            fs::read_to_string(wt.join("new-file.txt")).unwrap(),
            "brand new\n"
        );
        assert!(!wt.join("extra.txt").exists());

        // The new admin dir lives in the private repo under the job id and
        // survives a prune (the gitdir pair is consistent).
        let new_gitdir = gitdir_target(&wt).unwrap();
        assert!(
            new_gitdir.starts_with(repo.to_str().unwrap()),
            "got {new_gitdir}"
        );
        assert!(new_gitdir.ends_with("worktrees/job1"), "got {new_gitdir}");
        git_run(&repo, &["worktree", "prune"], true).unwrap();
        assert!(
            Path::new(&new_gitdir).exists(),
            "prune must keep the repaired worktree"
        );
        let list = git_stdout_at(&repo, &["worktree", "list"]);
        assert!(list.contains(wt.to_str().unwrap()));
        // HEAD is the job's base commit (detached).
        assert_eq!(head_sha(&wt), base_of_job);

        // Idempotence: repairing a healthy worktree is a no-op.
        assert_eq!(
            repair_worktree(&config, &meta).unwrap(),
            RepairOutcome::Healthy
        );
        assert_eq!(gitdir_target(&wt).unwrap(), new_gitdir);
    }

    #[test]
    fn test_repair_fetches_missing_base_sha_from_source() {
        // The job's base is an OLD commit: the private repo was populated
        // shallowly with the current tip only, so it lacks the base -- but
        // the source still has it. Repair must fetch the EXACT SHA.
        let root = tempfile::tempdir().unwrap();
        let (src, config) = make_workspace_source(root.path(), 3, false);
        let base_of_job = git_stdout_at(&src, &["rev-parse", "HEAD^^"]);

        // First use: fetch the TIP only (shallow) -- the private repo lacks
        // the job's old base.
        let repo = ensure_private_repo(&config, "proj", &src, "HEAD").unwrap();
        assert!(
            !ref_exists(&repo, &base_of_job),
            "setup: the shallow private repo must lack the old base"
        );
        // Pre-fix flow: the job worktree lived in the SOURCE checkout at
        // the old base.
        let wt = make_dangling_job_wt(&config, &src, &base_of_job, "job1");

        let meta = test_meta("job1", "proj", wt.clone(), base_of_job.clone());
        assert_eq!(
            repair_worktree(&config, &meta).unwrap(),
            RepairOutcome::Repaired
        );
        assert!(
            ref_exists(&repo, &base_of_job),
            "repair must have fetched the missing base SHA from the source"
        );
        let status = git_stdout_at(&wt, &["status", "--porcelain"]);
        assert!(status.contains("M file.txt"), "got: {status}");
        assert!(status.contains("?? new-file.txt"), "got: {status}");
        assert_eq!(head_sha(&wt), base_of_job);
    }

    #[test]
    fn test_repair_base_sha_not_fetchable_is_a_clear_error() {
        // The base commit is unreachable from the source (its history was
        // rewritten / the branch force-pushed away): repair must fail
        // naming the job and the SHA, and must not guess another commit or
        // touch the worktree's files.
        let root = tempfile::tempdir().unwrap();
        // A project checkout whose (only) content has nothing to do with
        // the ghost SHA.
        let (src, config) = make_workspace_source(root.path(), 1, false);
        let project_dir = config.workspace_root.join("proj");
        assert_eq!(project_dir, src);
        // A commit that exists in NO repository reachable from the project.
        let other = root.path().join("unrelated");
        let _other_src = make_source(root.path(), "unrelated", 1, false, false);
        // make_source stamps its single commit with the same author, content
        // and message as the project checkout's commit, so both land in the
        // same second and collide into one identical SHA -- which is trivially
        // fetchable from the source. Commit distinct content so the ghost SHA
        // exists only in this unrelated repository.
        fs::write(other.join("file.txt"), "ghost content\n").unwrap();
        git_stdout_at(
            &other,
            &[
                "commit",
                "-q",
                "-am",
                "a commit that only this unrelated repo has",
            ],
        );
        let ghost_sha = head_sha(&other);

        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("job9");
        fs::create_dir_all(&wt).unwrap();
        fs::write(wt.join(".git"), "gitdir: /nonexistent/admin\n").unwrap();
        fs::write(wt.join("file.txt"), "work\n").unwrap();
        let meta = test_meta("job9", "proj", wt.clone(), ghost_sha.clone());

        let err = repair_worktree(&config, &meta).unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("job9") && chain.contains(&ghost_sha),
            "the error must name the job and the missing base: {chain}"
        );
        assert!(
            chain.contains("repair_base_not_found")
                || chain.to_lowercase().contains("could not be fetched"),
            "got: {chain}"
        );
        // The worktree's files were left exactly as they were.
        assert_eq!(fs::read_to_string(wt.join("file.txt")).unwrap(), "work\n");
        assert!(
            worktree_gitdir_dangling(&wt),
            "nothing must be half-repaired: the worktree stays dangling as found"
        );
    }

    #[test]
    fn test_repair_outcomes_for_external_absent_healthy() {
        let root = tempfile::tempdir().unwrap();
        let config = test_config(root.path());
        // Absent worktree directory.
        let meta = test_meta(
            "ghost",
            "proj",
            config
                .workspace_root
                .join(".agent-jobs")
                .join("proj")
                .join("ghost"),
            "HEAD".to_string(),
        );
        assert_eq!(
            repair_worktree(&config, &meta).unwrap(),
            RepairOutcome::Absent
        );
        // External worktree: never ours to touch.
        let ext = root.path().join("external-wt");
        fs::create_dir_all(&ext).unwrap();
        let mut meta_ext = test_meta("ext", "proj", ext, "HEAD".to_string());
        meta_ext.external_worktree = true;
        assert_eq!(
            repair_worktree(&config, &meta_ext).unwrap(),
            RepairOutcome::External
        );
        // A healthy worktree (a plain checkout with a `.git` directory)
        // never dangles and is never touched.
        let healthy = make_source(root.path(), "healthy", 1, false, false);
        let meta_h = test_meta("h", "proj", healthy.clone(), "HEAD".to_string());
        assert!(!worktree_gitdir_dangling(&healthy));
        assert_eq!(
            repair_worktree(&config, &meta_h).unwrap(),
            RepairOutcome::Healthy
        );
        // A normal worktree of the private repo (a `.git` file with a live
        // gitdir) is healthy too.
        let src = make_source(root.path(), "proj", 1, false, false);
        let repo = ensure_private_repo(&config, "proj", &src, "HEAD").unwrap();
        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("jobh");
        fs::create_dir_all(wt.parent().unwrap()).unwrap();
        create_worktree(&repo, &wt, "agent-run/jobh", "HEAD").unwrap();
        let meta_w = test_meta("jobh", "proj", wt.clone(), "HEAD".to_string());
        assert!(!worktree_gitdir_dangling(&wt));
        assert_eq!(
            repair_worktree(&config, &meta_w).unwrap(),
            RepairOutcome::Healthy
        );
        // auto_repair_worktree is a silent no-op on a healthy worktree and
        // logs exactly one line when it repairs.
        auto_repair_worktree(&config, &meta_w); // no-op, returns immediately
    }

    #[test]
    fn test_dangling_worktree_with_files_is_never_swept_without_force() {
        // A dangling worktree directory that still holds the job's files
        // must never be treated as stale: the sweep predicate reports the
        // files (so gc/clean keep it), gc skips the job, and `clean`
        // without --force refuses instead of deleting.
        let root = tempfile::tempdir().unwrap();
        let src = make_source(root.path(), "proj", 1, false, false);
        let config = test_config(root.path());
        let base = head_sha(&src);
        let repo = ensure_private_repo(&config, "proj", &src, "HEAD").unwrap();
        let wt = config
            .workspace_root
            .join(".agent-jobs")
            .join("proj")
            .join("job1");
        fs::create_dir_all(wt.parent().unwrap()).unwrap();
        create_worktree(&repo, &wt, "agent-run/job1", "HEAD").unwrap();
        fs::write(wt.join("file.txt"), "job edit\n").unwrap();
        fs::write(wt.join("artifact.bin"), b"payload").unwrap();
        let admin_path = gitdir_target(&wt).unwrap();
        let admin = Path::new(&admin_path);
        fs::remove_dir_all(admin).unwrap();
        assert!(worktree_gitdir_dangling(&wt));

        // 1. The sweep predicate itself refuses: it lists the non-`.git`
        //    files it would destroy.
        match crate::core::git::worktree_dir_can_be_swept(&wt, 20) {
            Err(entries) => {
                assert!(
                    entries.iter().any(|e| e == "file.txt"),
                    "entries: {entries:?}"
                );
                assert!(
                    entries.iter().any(|e| e == "artifact.bin"),
                    "entries: {entries:?}"
                );
            }
            Ok(()) => panic!("a dangling worktree with files must NOT be sweepable"),
        }

        // 2. gc keeps everything (the dirty check cannot pass on a broken
        //    worktree, and even auto-repair then reports it dirty).
        ensure_state_store(&config.state_root).unwrap();
        let mut meta = test_meta("job1", "proj", wt.clone(), base);
        meta.exit_code = Some(0);
        meta.save(&config.state_root).unwrap();
        let outcome = crate::core::commands::gc::prune_single_job(
            &config,
            &meta,
            crate::core::job::JobStatus::Done,
        )
        .unwrap();
        match outcome {
            crate::core::commands::gc::PruneOutcome::Skipped { reason, .. } => {
                assert!(
                    reason.contains("dirty worktree"),
                    "a dangling worktree must be skipped, got: {reason:?}"
                );
            }
            other => panic!("gc must skip the dangling worktree, got: {other:?}"),
        }
        assert!(
            wt.join("file.txt").exists(),
            "gc must not delete the worktree files"
        );
        assert!(wt.join("artifact.bin").exists());

        // 3. clean without --force refuses instead of deleting.
        let err = crate::core::commands::clean::run(
            &config,
            &crate::core::args::CleanArgs {
                job_id: "job1".to_string(),
                force: false,
                purge_log: false,
            },
        )
        .unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("uncommitted modifications or untracked files"),
            "clean without --force must refuse: {chain}"
        );
        assert!(wt.join("file.txt").exists());
        assert!(wt.join("artifact.bin").exists());
    }
}

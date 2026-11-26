use anyhow::{bail, Context, Result};
use std::ffi::OsStr;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Applies the hardened git configuration to `cmd`: the per-invocation `-c`
/// overrides that neutralise the code-execution vectors an agent can plant in
/// a repository's own config or hooks, plus an environment that strips the
/// ambient `GIT_*` variables a caller might carry. Every production git
/// invocation in the worker goes through this (or [`hardened_git`] /
/// [`diff_hardened`]) so the hardening cannot be forgotten at a call site.
///
/// The overrides:
/// - `core.fsmonitor=false` -- a repo (or the user/system config) could set
///   `core.fsmonitor` to a shell script that git runs to refresh the index;
///   disabling it stops that vector.
/// - `core.hooksPath=/dev/null` -- the repo's `.git/hooks` (or a configured
///   hooks path) could hold a `pre-commit`/`post-checkout` script that runs on
///   `git commit`/`checkout`; pointing the hooks dir at a non-directory
///   disables every hook.
/// - `protocol.ext.allow=never` -- refuses the `ext::` transport, which runs
///   an arbitrary command as part of a fetch/clone.
/// - `core.pager=cat` -- never pipes diff/log/show output through a pager
///   that could be a user/system `PAGER`.
///
/// The `diff.external` driver (a command the repo config could point at) is
/// neutralised separately, at the diff-family call sites, with `--no-ext-diff`
/// (see [`diff_hardened`] / [`diff_subcommand_index`]): an empty `-c
/// diff.external=` is NOT usable because git would try to *run* the empty
/// string as the external diff program and abort every diff.
///
/// Environment: the ambient `GIT_DIR`/`GIT_WORK_TREE`/`GIT_INDEX_FILE`/
/// `GIT_OBJECT_DIRECTORY`/`GIT_ALTERNATE_OBJECT_DIRECTORIES`/`GIT_COMMON_DIR`/
/// `GIT_CEILING_DIRECTORIES` are removed (they could redirect git at a
/// different repository than the `-C` path names), `GIT_CONFIG_GLOBAL` and
/// `GIT_CONFIG_SYSTEM` are pointed at `/dev/null` so the invoking user's and
/// the system's config cannot re-enable any of the vectors, and
/// `GIT_TERMINAL_PROMPT=0` stops a hung credential/SSH prompt.
fn harden_git(cmd: &mut Command) -> &mut Command {
    cmd.arg("-c")
        .arg("core.fsmonitor=false")
        .arg("-c")
        .arg("core.hooksPath=/dev/null")
        .arg("-c")
        .arg("protocol.ext.allow=never")
        .arg("-c")
        .arg("core.pager=cat");
    for var in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CEILING_DIRECTORIES",
    ] {
        cmd.env_remove(var);
    }
    cmd.env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0");
    cmd
}

/// Builds a hardened `git` command (see [`harden_git`]). Callers append
/// `-C <dir>` and the subcommand/arguments.
pub fn hardened_git() -> Command {
    let mut cmd = Command::new("git");
    harden_git(&mut cmd);
    cmd
}

/// Completes a `git diff` command so the diff is computed by git's built-in
/// diff: it appends the ref, the `--no-ext-diff --no-textconv` flags (so no
/// `diff.external` driver or `textconv` filter -- both of which the repo could
/// define in its config -- is ever invoked), and then the pathspec separator
/// and pathspecs.
///
/// The command must already carry `git diff` (built by the caller, which also
/// adds `-C <worktree>` and any other leading `diff` options). The two flags
/// are `diff` subcommand options and must come BEFORE the `--` separator:
/// anything after `--` is a pathspec, so a flag placed there would be read as
/// a (nonexistent) path and silently empty the diff. `pathspecs` may be empty
/// (no `--` separator is emitted).
pub fn diff_hardened<'c>(
    cmd: &'c mut Command,
    ref_arg: Option<&str>,
    pathspecs: &[String],
) -> &'c mut Command {
    if let Some(r) = ref_arg {
        cmd.arg(r);
    }
    cmd.arg("--no-ext-diff").arg("--no-textconv");
    if !pathspecs.is_empty() {
        cmd.arg("--").args(pathspecs);
    }
    cmd
}

/// Reads the `gitdir:` target of a worktree's `.git` file when it is a file
/// (a linked worktree) whose first line has the expected shape. Returns
/// `None` for a directory `.git`, a missing/malformed file, or a line that is
/// not a `gitdir:` pointer.
pub fn worktree_gitdir_target(worktree_path: &Path) -> Option<String> {
    let dotgit = worktree_path.join(".git");
    if !dotgit.is_file() {
        return None;
    }
    let content = std::fs::read_to_string(&dotgit).ok()?;
    content
        .lines()
        .next()?
        .trim()
        .strip_prefix("gitdir:")
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

/// Refuses a worktree whose `.git` file points OUTSIDE the repositories
/// agent-run manages for the project.
///
/// A job worktree is a linked worktree of the project's private bare
/// repository under `repos_root`; its `.git` file is
/// `gitdir: <repos_root>/<project>.git/worktrees/<...>`. Before any git
/// operation runs inside the worktree we verify that pointer resolves to a
/// location inside one of the allowed roots, so a tampered or stale `.git`
/// file cannot redirect our git calls at an arbitrary attacker-controlled
/// repository (whose config/hooks could then run code despite
/// [`harden_git`]).
///
/// A worktree with a directory `.git` is self-contained (its own repository
/// root, no `gitdir:` pointer); its executable config is neutralised by
/// [`harden_git`], so it is always accepted. A worktree with a `.git` FILE is a
/// linked worktree and its `gitdir:` target must canonicalize inside one of
/// `allowed_roots`.
///
/// `allowed_roots` are the repository locations agent-run uses for a project
/// (see [`crate::core::repos::project_git_repo`]): the private bare-repo root
/// (`config.repos_root`) for the current layout, plus the project's
/// workspace checkout (`config.workspace_root/<project>`) for legacy worktrees
/// still attached to the git-sync checkout. A pointer anywhere else is an
/// attacker-tampered link and is refused. Returns `Ok(())` in the accepted
/// cases and when the worktree is absent; a missing `gitdir:` line, a pointer
/// that does not resolve, or a target outside every allowed root is a hard
/// error.
pub fn worktree_gitdir_in_expected_roots(
    worktree_path: &Path,
    allowed_roots: &[PathBuf],
) -> Result<()> {
    if !worktree_path.exists() {
        return Ok(());
    }
    let dotgit = worktree_path.join(".git");
    if dotgit.is_dir() {
        // A directory .git means the worktree is its own repository root
        // (self-contained, not a linked worktree): it carries no `gitdir:`
        // pointer, and any executable config it holds (fsmonitor, hooks,
        // external diff) is neutralised by `harden_git`. There is no pointer
        // to validate.
        return Ok(());
    }
    if !dotgit.is_file() {
        // No .git entry at all: a broken/dangling worktree. Let the git call
        // itself fail with its own message rather than guessing.
        return Ok(());
    }
    let Some(target) = worktree_gitdir_target(worktree_path) else {
        return Err(crate::core::error::AgentRunError::invalid(
            "worktree_gitdir_outside_repos",
            format!(
                "Refusing to run git in worktree {}: its .git file has no readable 'gitdir:' pointer",
                worktree_path.display()
            ),
        )
        .into());
    };
    // Resolve the pointer to an absolute path WITHOUT following symlinks. A
    // lexical (component-wise) containment test needs no target to exist, so
    // a dangling worktree -- whose `.git` points at an admin directory that is
    // (re)created on repair -- is not rejected here; a tampered pointer at an
    // unrelated location is. `..` segments are folded lexically (via
    // `components`), so `../../elsewhere` cannot smuggle a target back out of
    // an allowed root.
    let raw = Path::new(&target);
    let resolved = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        worktree_path.join(raw)
    };
    let resolved = resolved.components().fold(PathBuf::new(), |mut acc, c| {
        acc.push(c);
        acc
    });

    // Each allowed root contributes its raw path and, when it exists, its
    // symlink-resolved (canonical) path.
    let mut roots: Vec<PathBuf> = Vec::new();
    for root in allowed_roots {
        roots.push(root.to_path_buf());
        if let Ok(c) = root.canonicalize() {
            roots.push(c);
        }
    }

    // Lexical containment: the raw pointer must point inside an allowed root.
    if roots.iter().any(|r| resolved.starts_with(r.as_path())) {
        // If the target exists, also confirm that symlinks do not resolve it
        // back OUT of every allowed root (a symlink planted at a path that is
        // lexically inside a root but points elsewhere would otherwise be
        // followed by git).
        if resolved.exists() {
            if let Ok(canonical) = resolved.canonicalize() {
                if roots.iter().any(|r| canonical.starts_with(r.as_path())) {
                    return Ok(());
                }
                return Err(crate::core::error::AgentRunError::invalid(
                    "worktree_gitdir_outside_repos",
                    format!(
                        "Refusing to run git in worktree {}: its .git points at {}, which resolves outside the repositories agent-run manages for this project",
                        worktree_path.display(),
                        canonical.display()
                    ),
                )
                .into());
            }
        }
        return Ok(());
    }

    Err(crate::core::error::AgentRunError::invalid(
        "worktree_gitdir_outside_repos",
        format!(
            "Refusing to run git in worktree {}: its .git points at {}, outside the repositories agent-run manages for this project",
            worktree_path.display(),
            resolved.display()
        ),
    )
    .into())
}

/// The roots a worktree's `.git` pointer is allowed to resolve inside for a
/// project (see [`worktree_gitdir_in_expected_roots`]): the private
/// bare-repo root, the `repo-sync` root `<workspace_root>/.roots/<project>`
/// (where `<workspace_root>/<project>` is a symlink into `.roots/<project>/.worktrees/<sha>`
/// and its `.git` points into `.roots/<project>/.git`), and, for legacy
/// worktrees still attached to the checkout, the project's workspace checkout.
pub fn expected_worktree_roots(
    config: &crate::core::config::Config,
    project: &str,
) -> Vec<PathBuf> {
    let mut roots = vec![config.repos_root.clone()];
    let checkout = config.workspace_root.join(project);
    if checkout != config.repos_root {
        // When `checkout` is itself a linked worktree (e.g. repo-sync's
        // `.roots/<project>/.worktrees/<sha>`), also allow the common `.git`
        // directory its `gitdir:` pointer resolves into.
        if let Some(target) = worktree_gitdir_target(&checkout) {
            let raw = Path::new(&target);
            let resolved = if raw.is_absolute() {
                raw.to_path_buf()
            } else {
                checkout.join(raw)
            };
            let folded = resolved.components().fold(PathBuf::new(), |mut acc, c| {
                acc.push(c);
                acc
            });
            // `<repo>/.git/worktrees/<name>` -> allow `<repo>/.git` and `<repo>`.
            if let Some(worktrees_dir) = folded.parent() {
                if let Some(git_dir) = worktrees_dir.parent() {
                    roots.push(git_dir.to_path_buf());
                    if let Some(repo_root) = git_dir.parent() {
                        roots.push(repo_root.to_path_buf());
                    }
                }
            }
        }
        roots.push(checkout);
    }
    let reposync_root = config.workspace_root.join(".roots").join(project);
    if !roots.contains(&reposync_root) {
        roots.push(reposync_root);
    }
    roots
}

/// Validates that `<workspace_root>/<project>` exists and is a valid git repository.
pub fn validate_project_repo(workspace_root: &Path, project: &str) -> Result<PathBuf> {
    let project_dir = workspace_root.join(project);
    if !project_dir.exists() {
        return Err(crate::core::error::AgentRunError::invalid(
            "project_not_found",
            format!(
                "Project directory does not exist: {}",
                project_dir.display()
            ),
        )
        .into());
    }
    if !project_dir.is_dir() {
        return Err(crate::core::error::AgentRunError::invalid(
            "project_not_a_directory",
            format!("Project path is not a directory: {}", project_dir.display()),
        )
        .into());
    }

    // Verify it is a git repo using `git rev-parse --is-inside-work-tree`
    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &project_dir.to_string_lossy(),
            "rev-parse",
            "--is-inside-work-tree",
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git' command for {} (ensure git is installed and in PATH)",
            project_dir.display()
        )
    })?;

    if !output.status.success() {
        return Err(crate::core::error::AgentRunError::invalid(
            "not_a_git_repository",
            format!(
                "Project '{}' at {} is not a valid git repository: {}",
                project,
                project_dir.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        )
        .into());
    }

    Ok(project_dir)
}

/// Validates that `worktree_path` is a git checkout of the SAME repository as
/// `project_dir` (used by `start --from-worktree`). Returns the canonicalized
/// worktree path.
pub fn validate_external_worktree(project_dir: &Path, worktree_path: &Path) -> Result<PathBuf> {
    let wt = worktree_path.canonicalize().with_context(|| {
        format!(
            "External worktree path does not exist: {}",
            worktree_path.display()
        )
    })?;

    // The git common dir (real .git of the main worktree) uniquely identifies a
    // repository across worktrees; both must resolve to the same location.
    let common_dir = |dir: &Path| -> Result<PathBuf> {
        let out = crate::core::reaper::output_tracked(&mut { let mut c = hardened_git(); c
            .args(["-C", &dir.to_string_lossy(), "rev-parse", "--git-common-dir"])
            ; c })
            .with_context(|| format!("Failed to execute 'git rev-parse --git-common-dir' in {} (ensure git is installed and in PATH)", dir.display()))?;
        if !out.status.success() {
            return Err(crate::core::error::AgentRunError::invalid(
                "not_a_git_checkout",
                format!(
                    "Not a git checkout: {} ({})",
                    dir.display(),
                    String::from_utf8_lossy(&out.stderr).trim()
                ),
            )
            .into());
        }
        let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let p = Path::new(&raw);
        let p = if p.is_absolute() {
            p.to_path_buf()
        } else {
            dir.join(p)
        };
        p.canonicalize()
            .with_context(|| format!("Failed to canonicalize git common dir '{}'", p.display()))
    };

    let project_common = common_dir(project_dir).with_context(|| {
        format!(
            "Project directory {} is not a git checkout",
            project_dir.display()
        )
    })?;
    let wt_common = common_dir(&wt)?;

    if project_common != wt_common {
        bail!(
            "External worktree {} does not belong to the same git repository as project {} (git common dirs {} vs {})",
            wt.display(),
            project_dir.display(),
            wt_common.display(),
            project_common.display()
        );
    }

    Ok(wt)
}

/// Returns true if the git error indicates index lock contention.
pub fn is_lock_contention_error(stderr: &str) -> bool {
    stderr.contains("index.lock")
        || (stderr.contains(".lock") && stderr.contains("Unable to create"))
}

/// Returns true when `ref_name` resolves to a commit in `repo_dir`.
pub fn git_ref_exists(repo_dir: &Path, ref_name: &str) -> Result<bool> {
    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &repo_dir.to_string_lossy(),
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{}^{{commit}}", ref_name),
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git rev-parse' in {} (ensure git is installed and in PATH)",
            repo_dir.display()
        )
    })?;
    Ok(output.status.success())
}

/// Creates a new git worktree with an isolated branch.
///
/// Returns the resolved, concrete commit SHA that the worktree branch was
/// actually created from. Callers store this in `JobMetadata.base_ref` so
/// later `diff` comparisons point at a fixed commit, not a moving branch
/// name or the worktree's own HEAD.
///
/// When `base_ref` does not resolve as-is (e.g. a bare branch name in a
/// git-sync shallow clone where only `origin/<name>` exists as a
/// remote-tracking ref), falls back to `origin/<base_ref>`.
pub fn create_worktree(
    repo_dir: &Path,
    worktree_path: &Path,
    branch_name: &str,
    base_ref: &str,
) -> Result<String> {
    // 1. Check if worktree destination path already exists (job ID collision or stale directory)
    if worktree_path.exists() {
        bail!(
            "Worktree destination path already exists: {} (stale worktree directory or job ID collision)",
            worktree_path.display()
        );
    }

    // 2. Validate that base_ref exists in repo_dir. A bare branch name may
    //    not resolve locally (git-sync shallow clones keep only the
    //    remote-tracking ref), so retry with an `origin/` prefix before
    //    giving up.
    let mut validated_ref = base_ref.to_string();
    if !git_ref_exists(repo_dir, base_ref)? {
        let fallback = format!("origin/{}", base_ref);
        if git_ref_exists(repo_dir, &fallback)? {
            validated_ref = fallback;
        } else {
            return Err(crate::core::error::AgentRunError::invalid(
                "git_ref_not_found",
                format!(
                    "Git reference '{}' does not exist in repository '{}'",
                    base_ref,
                    repo_dir.display()
                ),
            )
            .into());
        }
    }

    // 3. Ensure parent directory exists
    if let Some(parent) = worktree_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display()))?;
    }

    // 4. Resolve the validated ref to its concrete commit SHA.
    let sha_output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &repo_dir.to_string_lossy(),
            "rev-parse",
            &format!("{}^{{commit}}", validated_ref),
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git rev-parse' in {} (ensure git is installed and in PATH)",
            repo_dir.display()
        )
    })?;
    if !sha_output.status.success() {
        bail!(
            "Failed to resolve git reference '{}' to a commit SHA in repository '{}'",
            validated_ref,
            repo_dir.display()
        );
    }
    let resolved_sha = String::from_utf8_lossy(&sha_output.stdout)
        .trim()
        .to_string();

    let max_retries = 5;
    let mut attempt = 0;
    let output = loop {
        attempt += 1;
        let output = crate::core::reaper::output_tracked(&mut {
            let mut c = hardened_git();
            c.args([
                "-C",
                &repo_dir.to_string_lossy(),
                "worktree",
                "add",
                "-b",
                branch_name,
                &worktree_path.to_string_lossy(),
                &validated_ref,
            ]);
            c
        })
        .with_context(|| {
            format!(
                "Failed to execute 'git worktree add' for {} (ensure git is installed and in PATH)",
                worktree_path.display()
            )
        })?;

        if output.status.success() {
            break output;
        }

        let err = String::from_utf8_lossy(&output.stderr);
        if is_lock_contention_error(&err) && attempt <= max_retries {
            std::thread::sleep(std::time::Duration::from_millis(50 * (1 << (attempt - 1))));
            continue;
        }

        break output;
    };

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        let trimmed_err = err.trim();
        if trimmed_err.contains("already exists") {
            bail!(
                "Cannot create worktree: branch '{}' or destination '{}' already exists: {}",
                branch_name,
                worktree_path.display(),
                trimmed_err
            );
        } else if trimmed_err.contains("invalid reference")
            || trimmed_err.contains("not a valid object name")
        {
            return Err(crate::core::error::AgentRunError::invalid(
                "git_ref_not_found",
                format!(
                    "Git reference '{}' does not exist in repository '{}': {}",
                    base_ref,
                    repo_dir.display(),
                    trimmed_err
                ),
            )
            .into());
        } else {
            bail!(
                "git worktree add failed for '{}' from ref '{}': {}",
                worktree_path.display(),
                base_ref,
                trimmed_err
            );
        }
    }

    Ok(resolved_sha)
}

/// Removes a git worktree.
///
/// NOTE on `--force`: `git worktree remove` will refuse to delete a worktree if it
/// contains uncommitted modifications or untracked files left by the agent. `--force`
/// bypasses this check to tear down the worktree cleanly.
pub fn remove_worktree(repo_dir: &Path, worktree_path: &Path, force: bool) -> Result<()> {
    let repo_dir_str = repo_dir.to_string_lossy().to_string();
    let mut args = vec!["-C", &repo_dir_str, "worktree", "remove"];
    if force {
        args.push("--force");
    }
    let worktree_str = worktree_path.to_string_lossy().to_string();
    args.push(&worktree_str);

    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args(&args);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git worktree remove' for {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        let trimmed_err = err.trim();
        if trimmed_err.contains("contains modified or untracked files")
            || trimmed_err.contains("use --force")
        {
            bail!(
                "Worktree at {} contains uncommitted modifications or untracked files. Use --force to discard changes: {}",
                worktree_path.display(),
                trimmed_err
            );
        } else {
            bail!(
                "git worktree remove failed for {}: {}",
                worktree_path.display(),
                trimmed_err
            );
        }
    }

    // Ensure directory is cleaned up if git worktree remove left a directory shell
    if worktree_path.exists() {
        let _ = std::fs::remove_dir_all(worktree_path);
    }

    Ok(())
}

/// Checks whether a leftover worktree directory can be safely swept with
/// `remove_dir_all` after a failed `git worktree remove`.
///
/// A failed `git worktree remove` leaves the worktree's files behind, and those
/// may include files the agent (or a user) created that the repository does not
/// know about; the old fallback deleted the whole directory blindly. The only
/// content that is always safe to delete is the worktree's own git
/// bookkeeping -- for a linked worktree that is the `.git` file pointing into
/// the main repository (or nothing, when git already deleted it).
///
/// This is deliberate pure-filesystem inspection: it does not shell out to git,
/// because the worktree's bookkeeping may be exactly what is broken (e.g. the
/// `.git` file missing, which already makes `git worktree remove` fail).
/// Returns `Ok(())` when the directory is missing, empty, or holds nothing but
/// the `.git` entry; otherwise returns up to `max_entries` entry names (sorted,
/// for deterministic messages) as the reason to keep the directory.
pub fn worktree_dir_can_be_swept(
    worktree_path: &Path,
    max_entries: usize,
) -> Result<(), Vec<String>> {
    if !worktree_path.exists() {
        return Ok(());
    }
    let entries = std::fs::read_dir(worktree_path)
        .map_err(|e| {
            vec![format!(
                "Failed to read directory {}: {e}",
                worktree_path.display()
            )]
        })?
        .filter_map(|entry| {
            entry
                .ok()
                .map(|e| e.file_name().to_string_lossy().to_string())
        })
        .filter(|name| name.as_str() != ".git")
        .collect::<Vec<String>>();
    if entries.is_empty() {
        return Ok(());
    }
    let mut names = entries;
    names.sort();
    let mut shown = names;
    shown.truncate(max_entries);
    Err(shown)
}

/// Deletes a git branch in the repository.
pub fn delete_branch(repo_dir: &Path, branch_name: &str) -> Result<()> {
    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &repo_dir.to_string_lossy(),
            "branch",
            "-D",
            branch_name,
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git branch -D' in {}",
            repo_dir.display()
        )
    })?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!("git branch -D failed for '{}': {}", branch_name, err.trim());
    }

    Ok(())
}

/// Resolves a branch name to its tip commit SHA.
pub fn get_branch_tip_sha(repo_dir: &Path, branch_name: &str) -> Result<String> {
    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &repo_dir.to_string_lossy(),
            "rev-parse",
            &format!("{}^{{commit}}", branch_name),
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git rev-parse' in {} (ensure git is installed and in PATH)",
            repo_dir.display()
        )
    })?;

    if !output.status.success() {
        bail!(
            "Failed to resolve branch '{}' to a commit SHA in repository '{}': {}",
            branch_name,
            repo_dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Counts commits on `branch` that are neither merged into `base` nor reachable
/// from any remote-tracking ref (`git rev-list <branch> --not --remotes <base> --count`).
pub fn count_unmerged_unpushed_commits(repo_dir: &Path, branch: &str, base: &str) -> Result<usize> {
    if !git_ref_exists(repo_dir, branch)? {
        return Ok(0);
    }

    let repo_dir_str = repo_dir.to_string_lossy().to_string();
    let mut args = vec![
        "-C",
        &repo_dir_str,
        "rev-list",
        branch,
        "--not",
        "--remotes",
    ];

    let mut validated_base = None;
    if !base.is_empty() {
        if git_ref_exists(repo_dir, base)? {
            validated_base = Some(base.to_string());
        } else {
            let fallback = format!("origin/{}", base);
            if git_ref_exists(repo_dir, &fallback)? {
                validated_base = Some(fallback);
            }
        }
        if validated_base.is_none() {
            eprintln!(
                "Warning: base ref '{}' not found in repository '{}'; the 'merged into base' part of the unpushed-commit check was skipped, only the 'reachable from a remote' check was performed.",
                base,
                repo_dir.display()
            );
        }
    }

    if let Some(ref b) = validated_base {
        args.push(b);
    }
    args.push("--count");

    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args(&args);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git rev-list' in {} (ensure git is installed and in PATH)",
            repo_dir.display()
        )
    })?;

    if !output.status.success() {
        bail!(
            "git rev-list failed for '{}' in {}: {}",
            branch,
            repo_dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let count_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let count: usize = count_str.parse().with_context(|| {
        format!(
            "Failed to parse commit count from git rev-list output: '{}'",
            count_str
        )
    })?;

    Ok(count)
}

/// Checks whether a worktree contains uncommitted modifications or untracked files.
pub fn is_worktree_dirty(worktree_path: &Path) -> Result<bool> {
    let output = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &worktree_path.to_string_lossy(),
            "status",
            "--porcelain",
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git status' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !output.status.success() {
        bail!(
            "git status failed in {}: {}",
            worktree_path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    Ok(!output.stdout.is_empty())
}

/// Drops git worktree registrations whose directories no longer exist.
///
/// If a worktree directory was deleted out-of-band (e.g. `rm -rf`, or a
/// worktree-remove fallback path ran after the directory was already gone),
/// git still lists the worktree and `git branch -D` fails with "branch ... is
/// checked out at <missing path>" -- leaking the branch forever.
/// `git worktree prune` drops those stale registrations.
/// Best-effort: failures are reported on stderr and the (still-attempted)
/// branch deletion is left to warn on its own if it fails.
pub fn prune_stale_worktree_registrations(repo_dir: &Path) {
    let mut emit = |line: String| {
        eprintln!("{line}");
    };
    prune_stale_worktree_registrations_to(repo_dir, &mut emit)
}

/// Collected variant of [`prune_stale_worktree_registrations`]: warning lines
/// are routed through `emit(line)` instead of being printed directly, so the
/// run/execute split can record them (same pattern as gc's `*_to` variants).
pub fn prune_stale_worktree_registrations_to(repo_dir: &Path, emit: &mut dyn FnMut(String)) {
    match crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args(["-C", &repo_dir.to_string_lossy(), "worktree", "prune"]);
        c
    }) {
        Err(e) => emit(format!(
            "Warning: failed to run git worktree prune in {}: {}",
            repo_dir.display(),
            e
        )),
        Ok(out) if !out.status.success() => emit(format!(
            "Warning: git worktree prune in {} failed: {}",
            repo_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        )),
        Ok(_) => {}
    }
}

/// Builds the `git diff`/`git status`/`git ls-files`/`git add`/`git commit`
/// pathspec list for a selection: every `only` entry becomes a git
/// `:(literal)` pathspec -- a relative path taken VERBATIM (no pathspec magic
/// or globs, no leading-`/` root-anchoring), so a file literally named
/// `:foo` remains reachable; every `exclude` entry becomes a
/// `:(exclude,glob)` pattern (globs are the documented exclude behaviour).
///
/// ONE list feeds status, ls-files, diff, add and commit, so the five
/// commands can never disagree about what the selection covers.
pub fn build_diff_pathspecs(only: &[String], exclude: &[String]) -> Result<Vec<String>> {
    let mut specs: Vec<String> = Vec::with_capacity(only.len() + exclude.len());
    for entry in only {
        // An empty/blank value would become a bare `:(literal)` pathspec,
        // which matches the ENTIRE worktree, so `commit --only ""` (reachable
        // via the API, where the body is not forced non-empty the way the CLI
        // is) would commit everything. Refuse it, as discard refuses blank
        // paths.
        if entry.trim().is_empty() {
            return Err(crate::core::error::AgentRunError::invalid(
                "invalid_pathspec",
                "Refusing --only value: empty path in --only",
            )
            .into());
        }
        // `--only` values are LITERAL paths, not pathspecs: a leading `/`
        // would root-anchor (and thus select nothing) and a leading `:` is
        // git pathspec magic, so both are client errors, not selections.
        if entry.starts_with('/') {
            return Err(crate::core::error::AgentRunError::invalid(
                "invalid_pathspec",
                format!(
                    "Refusing --only value '{}': paths are relative to the worktree root",
                    entry
                ),
            )
            .into());
        }
        if entry.starts_with(":(literal,") {
            // A comma after `:(literal` does not close the magic group --
            // git would read `:(literal,top)` as combined magic. Extra
            // pathspec magic has no place in a --only value, so refuse the
            // variant with its own message instead of the generic
            // leading-colon one.
            return Err(crate::core::error::AgentRunError::invalid(
                "invalid_pathspec",
                format!(
                    "Refusing --only value '{}': a comma after ':(literal' extends the magic group (e.g. ':(literal,top)') and --only values carry no pathspec magic; --only values are literal relative paths, so use a plain path or ':(literal)X'",
                    entry
                ),
            )
            .into());
        }
        if entry.starts_with(":(literal)") {
            // The explicit escape hatch `:(literal)<path>`: kept verbatim,
            // with no second prefix (which would make `:(literal)` part of
            // the name). But the part after the prefix must be a real
            // worktree-relative path, re-validated like a bare --only entry:
            // nothing after the prefix is a bare `:(literal)` pathspec that
            // matches the ENTIRE worktree, a leading '/' would root-anchor,
            // a '..' component would escape the worktree, and further
            // pathspec magic is not allowed either.
            let rest = entry.strip_prefix(":(literal)").unwrap_or(entry);
            if rest.trim().is_empty() {
                return Err(crate::core::error::AgentRunError::invalid(
                    "invalid_pathspec",
                    format!(
                        "Refusing --only value '{}': nothing after the ':(literal)' prefix -- a bare ':(literal)' pathspec matches the entire worktree; put the path directly after the prefix, e.g. ':(literal)docs'",
                        entry
                    ),
                )
                .into());
            }
            if rest.starts_with('/') {
                return Err(crate::core::error::AgentRunError::invalid(
                    "invalid_pathspec",
                    format!(
                        "Refusing --only value '{}': paths are relative to the worktree root",
                        entry
                    ),
                )
                .into());
            }
            if rest.split('/').any(|component| component == "..") {
                return Err(crate::core::error::AgentRunError::invalid(
                    "invalid_pathspec",
                    format!(
                        "Refusing --only value '{}': the path after ':(literal)' must not contain a '..' component",
                        entry
                    ),
                )
                .into());
            }
            if rest.starts_with(":(") {
                return Err(crate::core::error::AgentRunError::invalid(
                    "invalid_pathspec",
                    format!(
                        "Refusing --only value '{}': further pathspec magic after the ':(literal)' prefix is not allowed",
                        entry
                    ),
                )
                .into());
            }
            specs.push(entry.clone());
        } else if entry.starts_with(':') {
            return Err(crate::core::error::AgentRunError::invalid(
                "invalid_pathspec",
                format!(
                    "Refusing --only value '{}': --only values are literal relative paths, not git pathspecs (a leading ':' is pathspec magic); to select a file whose name really starts with ':' use --only ':(literal){entry}'",
                    entry
                ),
            )
            .into());
        } else {
            // Plain relative path: force literal matching so glob characters
            // and any magic in the name are taken verbatim.
            specs.push(format!(":(literal){entry}"));
        }
    }
    for entry in exclude {
        // Same empty/blank guard as --only: a bare `:(exclude,glob)` pattern
        // must never be accepted from a blank API value (and the check keeps
        // the two flags symmetric).
        if entry.trim().is_empty() {
            return Err(crate::core::error::AgentRunError::invalid(
                "invalid_pathspec",
                "Refusing --exclude value: empty path in --exclude",
            )
            .into());
        }
        // Excludes stay GLOBS (the documented exclude behaviour), but a
        // leading `/` would root-anchor the pattern so it could never match
        // -- refuse it for the same reason as on --only.
        if entry.starts_with('/') {
            return Err(crate::core::error::AgentRunError::invalid(
                "invalid_pathspec",
                format!(
                    "Refusing --exclude value '{}': paths are relative to the worktree root",
                    entry
                ),
            )
            .into());
        }
        specs.push(format!(":(exclude,glob){entry}"));
    }
    Ok(specs)
}

/// Runs `git diff` inside the worktree directory.
///
/// In uncommitted mode (`base_ref` is `None`) the diff is taken against
/// `HEAD`, so the output covers everything that differs from HEAD: staged
/// changes, unstaged changes, and intent-to-add untracked files. Plain `git
/// diff` (worktree vs index) would silently omit anything already `git
/// add`ed, including staged deletions.
///
/// Untracked (new) files are marked intent-to-add (`git add -N`) first --
/// plain `git diff HEAD` never shows files that are not even in the index,
/// and `-N` makes new files appear as add-from-/dev/null hunks without
/// committing them. With no selection the historical `git add -N -A` (all
/// untracked files) is used; with a selection only the untracked files
/// still inside the selection are marked, so files outside `only`/`exclude`
/// are neither marked nor diffed. A failure here is returned as an error
/// (with stderr) instead of being silently ignored: ignoring it would
/// silently omit untracked files from the diff.
///
/// `only` and `exclude` are git pathspecs relative to the worktree root
/// (see [`build_diff_pathspecs`]); together they restrict the diff in both
/// uncommitted and base-ref modes.
pub fn run_diff(
    worktree_path: &Path,
    base_ref: Option<&str>,
    only: &[String],
    exclude: &[String],
) -> Result<String> {
    let worktree_str = worktree_path.to_string_lossy().to_string();
    // The same validated pathspec list drives the untracked scan below, the
    // diff itself, and (via the shared selection) status/add/commit, so the
    // "what is selected" answer is identical everywhere.
    let pathspecs = build_diff_pathspecs(only, exclude)?;
    let has_selection = !pathspecs.is_empty();

    let mut cmd = hardened_git();
    cmd.args(["-C", &worktree_str, "diff"]);
    match base_ref {
        // Uncommitted mode: everything that differs from HEAD (staged +
        // unstaged + intent-to-add untracked).
        None => {
            if !has_selection {
                let ita = crate::core::reaper::output_tracked(&mut { let mut c = hardened_git(); c
                    .args(["-C", &worktree_str, "add", "-N", "-A"])
                    ; c })
                    .with_context(|| {
                        format!(
                            "Failed to execute 'git add -N -A' in {} (ensure git is installed and in PATH)",
                            worktree_path.display()
                        )
                    })?;
                if !ita.status.success() {
                    bail!(
                        "git add -N -A failed in {}: {}",
                        worktree_path.display(),
                        String::from_utf8_lossy(&ita.stderr).trim()
                    );
                }
            } else {
                // Mark ONLY the untracked files the selection actually sees:
                // the selection's worktree status is read in full (read-only,
                // it never touches the index) and only the `A` entries are
                // marked, each as `:(literal)` + its raw on-disk bytes
                // (a raw leading `:` in a name is pathspec magic to git, so
                // the literal prefix is required). Passing the selection's
                // pathspecs to `add -N` directly would make git exit 128
                // ("pathspec did not match any files") for a staged-only
                // deletion or a directory with no untracked files, aborting
                // the diff even though `git diff HEAD` handles both fine.
                // An empty list skips `add -N` entirely: nothing to mark.
                let marked: Vec<std::ffi::OsString> =
                    worktree_status(worktree_path, only, exclude)?
                        .into_iter()
                        .filter(|e| e.status == "A")
                        .map(|e| {
                            let mut spec: Vec<u8> = b":(literal)".to_vec();
                            spec.extend_from_slice(e.path_bytes());
                            std::ffi::OsString::from_vec(spec)
                        })
                        .collect();
                if !marked.is_empty() {
                    let mut add_cmd = hardened_git();
                    add_cmd.args(["-C", &worktree_str, "add", "-N", "--"]);
                    for p in &marked {
                        add_cmd.arg(p);
                    }
                    let ita = crate::core::reaper::output_tracked(&mut add_cmd).with_context(|| {
                        format!(
                            "Failed to execute 'git add -N -- <selection>' in {} (ensure git is installed and in PATH)",
                            worktree_path.display()
                        )
                    })?;
                    if !ita.status.success() {
                        bail!(
                            "git add -N (selected untracked files) failed in {}: {}",
                            worktree_path.display(),
                            String::from_utf8_lossy(&ita.stderr).trim()
                        );
                    }
                }
            }
            diff_hardened(&mut cmd, Some("HEAD"), &pathspecs);
        }
        Some(r) => {
            diff_hardened(&mut cmd, Some(r), &pathspecs);
        }
    }

    let output = crate::core::reaper::output_tracked(&mut cmd).with_context(|| {
        format!(
            "Failed to execute 'git diff' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!("git diff failed: {}", err.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// If uncommitted changes exist in the worktree, stages and commits them (as
/// a single `agent-run(<job_id>): <summary>` commit).
///
/// Returns `true` if a commit was made, `false` when the worktree was
/// already clean. Callers must not report a commit as made when this
/// returns `false` (e.g. when a concurrent process committed first).
pub fn commit_uncommitted_changes(
    worktree_path: &Path,
    job_id: &str,
    prompt_summary: &str,
) -> Result<bool> {
    let status = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &worktree_path.to_string_lossy(),
            "status",
            "--porcelain",
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git status' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !status.status.success() {
        bail!(
            "git status failed in {}: {}",
            worktree_path.display(),
            String::from_utf8_lossy(&status.stderr).trim()
        );
    }

    if status.stdout.is_empty() {
        return Ok(false);
    }

    // Stage all changes
    let add_out = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args(["-C", &worktree_path.to_string_lossy(), "add", "-A"]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git add -A' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !add_out.status.success() {
        bail!(
            "git add failed: {}",
            String::from_utf8_lossy(&add_out.stderr)
        );
    }

    // Commit
    let commit_msg = format!("agent-run({}): {}", job_id, prompt_summary);
    let commit_out = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &worktree_path.to_string_lossy(),
            "commit",
            "-m",
            &commit_msg,
        ]);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git commit' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !commit_out.status.success() {
        // Between `git status` and `git commit` the worktree may have been
        // committed by another process (e.g. the agent itself): in that case
        // report that nothing was committed instead of erroring.
        if String::from_utf8_lossy(&commit_out.stdout).contains("nothing to commit") {
            return Ok(false);
        }
        bail!(
            "git commit failed: {}",
            String::from_utf8_lossy(&commit_out.stderr)
        );
    }

    Ok(true)
}

/// One changed entry of a worktree status, with the status letter collapsed
/// to the coarse classes users care about: `A` (new, i.e. untracked),
/// `M` (modified) and `D` (deleted).
#[derive(Debug, Clone)]
pub struct WorktreeFileStatus {
    /// `A` new, `M` modified or `D` deleted.
    pub status: &'static str,
    /// Worktree file size in bytes (0 for deleted files).
    pub size: u64,
    /// Path relative to the worktree root (forward slashes, no `./`).
    /// Lossy UTF-8 when the on-disk name is not valid UTF-8.
    pub path: String,
    /// The path's raw on-disk bytes, used for git round-trips (a `-z`
    /// porcelain path is raw bytes; re-encoding the lossy display form could
    /// differ from the actual name for non-UTF8 names).
    path_raw: Vec<u8>,
}

impl PartialEq for WorktreeFileStatus {
    fn eq(&self, other: &Self) -> bool {
        self.status == other.status && self.size == other.size && self.path_raw == other.path_raw
    }
}

impl Eq for WorktreeFileStatus {}

impl WorktreeFileStatus {
    /// The raw on-disk path bytes (see `path_raw`).
    pub fn path_bytes(&self) -> &[u8] {
        &self.path_raw
    }

    /// Builds an entry from the raw on-disk path bytes; the display path is
    /// the lossy UTF-8 form, so a non-UTF8 name still gets a usable entry.
    pub fn from_raw_path(status: &'static str, path_raw: &[u8]) -> Self {
        Self {
            status,
            size: 0,
            path: String::from_utf8_lossy(path_raw).into_owned(),
            path_raw: path_raw.to_vec(),
        }
    }
}

/// Lists the worktree's changed files: staged changes, unstaged changes and
/// untracked files (individually, never as a collapsed directory), restricted
/// by the same pathspec list [`build_diff_pathspecs`] feeds diff/add/commit,
/// so status and every other selection-bearing command agree.
///
/// Uses `git status --porcelain=v1 -z --untracked-files=all`, so it never
/// modifies the index (nothing is marked intent-to-add or otherwise staged).
/// Non-UTF8 on-disk names are kept (lossy display path, raw bytes for git):
/// a changed file is never silently dropped from the listing.
pub fn worktree_status(
    worktree_path: &Path,
    only: &[String],
    exclude: &[String],
) -> Result<Vec<WorktreeFileStatus>> {
    let pathspecs = build_diff_pathspecs(only, exclude)?;

    let mut cmd = hardened_git();
    cmd.args([
        "-C",
        &worktree_path.to_string_lossy(),
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    ]);
    if !pathspecs.is_empty() {
        cmd.arg("--").args(&pathspecs);
    }
    let out = crate::core::reaper::output_tracked(&mut cmd).with_context(|| {
        format!(
            "Failed to execute 'git status' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;
    if !out.status.success() {
        bail!(
            "git status failed in {}: {}",
            worktree_path.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    let mut entries = parse_porcelain_z(&out.stdout);
    // Porcelain can in principle surface the same path in more than one record
    // (a staged plus an unstaged change, or a rename name that collides with
    // another entry). The status list must name each path exactly once, so
    // drop later duplicates, keeping the first (staged) occurrence.
    //
    // Duplicates are compared on the RAW on-disk bytes, never on the lossy
    // display path: two different non-UTF8 names may share a lossy form, and
    // the bytes are anyway what the downstream git calls (ls-files/add/commit)
    // must receive.
    {
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        entries.retain(|e| seen.insert(e.path_raw.clone()));
    }
    for e in &mut entries {
        // The size comes from the raw bytes via OsStr::from_bytes: the lossy
        // display path may not be the file's actual (non-UTF8) name.
        e.size =
            std::fs::metadata(worktree_path.join(PathBuf::from(OsStr::from_bytes(&e.path_raw))))
                .map(|m| m.len())
                .unwrap_or(0);
    }
    Ok(entries)
}

/// The committed-delta variant of [`worktree_status`]: the changed files
/// relative to a fixed base commit (NOT `HEAD`), so both the changes the
/// agent already committed and the uncommitted ones show up. This is what a
/// `--base` view (patch and `--stat`) reports "against the base".
///
/// The list is **every path that differs between the worktree and the base
/// commit**, the union of two read-only commands that share one validated
/// pathspec list (so `--only`/`--exclude` mean the same thing here as in
/// every other selection-bearing command):
///
/// - `git diff --name-status -z --find-renames <base> -- <specs>`: the TRACKED
///   delta, comparing the worktree against the base commit, so it covers both
///   changes already committed past the base AND staged/unstaged worktree
///   changes. Regular records are one `<letter>\0<path>\0` pair (`A`/`M`/`D`).
///   A rename/copy record is `<R|C><similarity>\0<old path>\0<new path>\0`;
///   the NEW path is reported as an addition (`A`) and the OLD path as a
///   deletion (`D`), exactly how `worktree_status` surfaces a rename in the
///   plain `HEAD` view (so both views use the same `A`/`M`/`D` classes and a
///   rename yields the new file + the deleted source).
///   `git diff` never lists worktree paths that are missing from the index, so
///   UNTRACKED (never-added) files are NOT covered here and are added by the
///   next command (no intent-to-add marking: a `--stat` view must not mutate
///   the index).
/// - `git ls-files --others --exclude-standard -z -- <specs>`: the UNTRACKED
///   files only (raw path bytes, NUL-separated), each reported as an addition
///   (`A`). Ignored files are excluded, matching the untracked set the rest of
///   the selection sees.
///
/// Entries are deduplicated on the raw on-disk path bytes, keeping the STRONGER
/// status when the same path appears more than once (`D` over `M` over `A`),
/// and sizes are filled from the worktree exactly like [`worktree_status`]
/// (the on-disk size, or 0 for a deleted path).
///
/// One exception to the `D`-wins rule: a path that `git diff <base>` lists as
/// deleted but that STILL holds a file in the worktree (a file recreated
/// untracked after being dropped from the index, or a case-only rename) is a
/// changed file, not a deletion. It is reported once as `M` with its real
/// on-disk size instead of `D`.
///
/// `git status` is deliberately NOT used here: it compares against `HEAD`, so
/// it would miss committed-past-base changes and could not split a rename the
/// way the base diff does. The two commands above are the authoritative
/// "differs from `<base>`" source.
pub fn worktree_status_vs_base(
    worktree_path: &Path,
    base_ref: &str,
    only: &[String],
    exclude: &[String],
) -> Result<Vec<WorktreeFileStatus>> {
    let pathspecs = build_diff_pathspecs(only, exclude)?;
    let wt = worktree_path.to_string_lossy().to_string();

    let mut cmd = hardened_git();
    cmd.args(["-C", &wt, "diff", "--name-status", "-z", "--find-renames"]);
    // The ref, the `--no-ext-diff --no-textconv` flags, and the pathspec
    // separator are added by `diff_hardened` in the safe order (flags before
    // `--`).
    diff_hardened(&mut cmd, Some(base_ref), &pathspecs);
    let out = crate::core::reaper::output_tracked(&mut cmd).with_context(|| {
        format!(
            "Failed to execute 'git diff' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;
    if !out.status.success() {
        bail!(
            "git diff failed in {}: {}",
            worktree_path.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    // `--name-status -z` fields are NUL-separated: a regular record is a
    // status code followed by its path, while a rename/copy record is a code
    // `<R|C><digits>` followed by TWO paths (the old name first, then the new
    // one), so field lengths alone tell the two shapes apart. The new path is
    // kept as an addition and the old one as a deletion, matching how
    // `worktree_status` reports a rename in the plain `HEAD` view.
    let mut entries: Vec<WorktreeFileStatus> = Vec::new();
    let fields: Vec<&[u8]> = out
        .stdout
        .split(|b| *b == 0)
        .filter(|f| !f.is_empty())
        .collect();
    let mut i = 0;
    while i < fields.len() {
        let code = fields[i];
        let first = code.first().unwrap_or(&b' ');
        // A two-path record starts with R or C; every other code (A/M/D/T/...)
        // or the `?` a worktree-only path can show on its own is single-path.
        if matches!(first, b'R' | b'C') {
            if i + 2 < fields.len() {
                i += 1;
                let old_path = unprefix_slash_dot(fields[i]);
                i += 1;
                let new_path = unprefix_slash_dot(fields[i]);
                i += 1;
                if !new_path.is_empty() {
                    entries.push(WorktreeFileStatus::from_raw_path("A", new_path));
                }
                if !old_path.is_empty() {
                    entries.push(WorktreeFileStatus::from_raw_path("D", old_path));
                }
            } else {
                i += 1;
            }
        } else {
            if i + 1 < fields.len() {
                i += 1;
                let path = unprefix_slash_dot(fields[i]);
                if !path.is_empty() {
                    let letter = match first {
                        b'A' | b'?' => "A",
                        b'D' => "D",
                        _ => "M", // any other tracked change (M, T, typechange, ...)
                    };
                    entries.push(WorktreeFileStatus::from_raw_path(letter, path));
                }
                i += 1;
            } else {
                i += 1;
            }
        }
    }

    // Untracked files: the diff above never lists them (they are not in the
    // index), so they come from a worktree listing of the SAME selection.
    let mut untracked_cmd = hardened_git();
    untracked_cmd.args([
        "-C",
        &wt,
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
    ]);
    if !pathspecs.is_empty() {
        untracked_cmd.arg("--").args(&pathspecs);
    }
    let untracked_out =
        crate::core::reaper::output_tracked(&mut untracked_cmd).with_context(|| {
            format!(
                "Failed to execute 'git ls-files' in {} (ensure git is installed and in PATH)",
                worktree_path.display()
            )
        })?;
    if !untracked_out.status.success() {
        bail!(
            "git ls-files failed in {}: {}",
            worktree_path.display(),
            String::from_utf8_lossy(&untracked_out.stderr).trim()
        );
    }
    for field in untracked_out.stdout.split(|b| *b == 0) {
        if field.is_empty() {
            continue;
        }
        let path = unprefix_slash_dot(field);
        entries.push(WorktreeFileStatus::from_raw_path("A", path));
    }

    // One entry per path: when the same path shows up twice (e.g. inside one
    // diff scan and in the untracked listing, or duplicated by rename
    // detection) keep the STRONGER status, D over M over A.
    {
        // Deletion is the strongest signal, then modification, then addition.
        let rank = |s: &str| match s {
            "D" => 3,
            "M" => 2,
            _ => 1, // "A"
        };
        let mut by_path: std::collections::BTreeMap<Vec<u8>, WorktreeFileStatus> =
            std::collections::BTreeMap::new();
        for e in entries {
            // Two-phase borrow, so each arm borrows `by_path` separately:
            // a path seen before keeps the stronger status, a new path is
            // inserted.
            let present = by_path.contains_key(&e.path_raw);
            if present {
                if let Some(kept) = by_path.get_mut(&e.path_raw) {
                    if rank(e.status) > rank(kept.status) {
                        kept.status = e.status;
                    }
                }
            } else {
                by_path.insert(e.path_raw.clone(), e);
            }
        }
        entries = by_path.into_values().collect();
    }
    for e in &mut entries {
        let full = worktree_path.join(PathBuf::from(OsStr::from_bytes(&e.path_raw)));
        match std::fs::metadata(&full) {
            Ok(m) => {
                e.size = m.len();
                // A deletion in `git diff <base>` for a path that STILL holds
                // a file in the worktree (an untracked recreation after the
                // index drop, or a case-only rename) is a modification, not a
                // deletion: report it once as M with the real worktree size.
                if e.status == "D" {
                    e.status = "M";
                }
            }
            // A genuine deletion: the file is gone, so the size is 0 and the
            // status stays D.
            Err(_) => e.size = 0,
        }
    }
    Ok(entries)
}

/// Parses raw `git status --porcelain=v1 -z` stdout into worktree change
/// entries (sizes are filled in by the caller, which knows the worktree).
///
/// Record layout is `<XY> <path>`; a renamed/copied entry appends a second
/// path after the null (destination first, then the source that no longer
/// exists). Paths are kept raw by `-z`, so the fields are split on NUL
/// bytes and the `XY ` prefix -- no quoting to decode.
fn parse_porcelain_z(raw: &[u8]) -> Vec<WorktreeFileStatus> {
    let fields: Vec<&[u8]> = raw.split(|b| *b == 0).collect();
    let mut entries: Vec<WorktreeFileStatus> = Vec::new();
    let mut i = 0;
    while i < fields.len() {
        let record = fields[i];
        i += 1;
        let Some((xy, path_raw)) = split_porcelain_field(record) else {
            // Defensive: malformed (short) records carry no usable data.
            continue;
        };
        match xy.chars().next() {
            Some('R') | Some('C') => {
                // A `-z` rename/copy record lists the DESTINATION first (it is
                // the file that exists in the worktree), then the source name
                // as a BARE path -- no `XY ` prefix, because the source no
                // longer exists. Destination = new file, source = deletion.
                let path_raw = unprefix_slash_dot(path_raw);
                entries.push(WorktreeFileStatus::from_raw_path("A", path_raw));
                if i < fields.len() {
                    let old_bare = fields[i];
                    i += 1;
                    if !old_bare.is_empty() {
                        let old_bare = unprefix_slash_dot(old_bare);
                        entries.push(WorktreeFileStatus::from_raw_path("D", old_bare));
                    }
                }
            }
            _ => {
                let path_raw = unprefix_slash_dot(path_raw);
                entries.push(WorktreeFileStatus::from_raw_path(
                    porcelain_status_letter(xy),
                    path_raw,
                ));
            }
        }
    }
    entries
}

/// Splits one NUL-terminated `--porcelain=v1 -z` field into its two-byte
/// status code and the path. The `-z` form leaves paths raw (NOT C-styled
/// quoted), so no decoding is needed -- only the `XY ` prefix is stripped.
///
/// The path is returned as RAW BYTES: `-z` output keeps on-disk names as
/// bytes, and a non-UTF8 name is a perfectly changed file that must not be
/// dropped from the listing (it is surfaced with a lossy display path).
fn split_porcelain_field(raw: &[u8]) -> Option<(&str, &[u8])> {
    if raw.len() < 3 {
        return None;
    }
    let (xy, rest) = raw.split_at(2);
    let (space, path) = rest.split_first()?;
    if *space != b' ' {
        return None;
    }
    let xy = std::str::from_utf8(xy).ok()?;
    Some((xy, path))
}

/// Strips a leading `./` from raw `-z` path bytes (porcelain normally does
/// not add one, and a bare source-name field of a rename record carries no
/// `XY ` prefix -- it is kept for the rename branch's byte handling).
fn unprefix_slash_dot(raw: &[u8]) -> &[u8] {
    raw.strip_prefix(b"./").unwrap_or(raw)
}

/// Maps a porcelain XY code to a coarse status letter.
///
/// `D` deleted: the index or the worktree no longer holds the file. `A` new:
/// untracked (`??`), staged as an addition (`A` in the index), or marked
/// intent-to-add in the worktree (` A` -- the file exists but the index holds
/// only an `A` placeholder, as `run_diff` leaves it after `git add -N`), and
/// a rename destination also reads as new. A worktree 'A' is therefore "A"
/// too, so a file `run_diff` just intent-to-add marks is still reported as a
/// new file rather than a modification. Everything else (staged or unstaged
/// modifications) is `M`.
fn porcelain_status_letter(xy: &str) -> &'static str {
    let mut chars = xy.chars();
    let index = chars.next().unwrap_or(' ');
    let worktree = chars.next().unwrap_or(' ');
    if index == 'D' || worktree == 'D' {
        "D"
    } else if index == 'A' || index == '?' || worktree == 'A' {
        "A"
    } else {
        "M"
    }
}

/// Stages and commits ONLY the changes that still fall inside the given
/// selection; every other change -- including untracked files -- is left
/// uncommitted.
///
/// `only` and `exclude` are git pathspecs relative to the worktree root
/// (see [`build_diff_pathspecs`]). With no selection, historical behaviour
/// is preserved exactly (all changes are committed). Returns `true` if a
/// commit was made, `false` when there was nothing to commit in a clean
/// worktree (no-selection path), and an error when a SELECTION matched no
/// changes at all.
pub fn commit_selected(
    worktree_path: &Path,
    job_id: &str,
    message: &str,
    only: &[String],
    exclude: &[String],
) -> Result<bool> {
    if only.is_empty() && exclude.is_empty() {
        return commit_uncommitted_changes(worktree_path, job_id, message);
    }

    let entries = worktree_status(worktree_path, only, exclude)?;
    if entries.is_empty() {
        return Err(crate::core::error::AgentRunError::invalid(
            "selection_matched_nothing",
            format!(
                "Nothing to commit: the selection matched no changes in {} (check the --only/--exclude paths against `agent-run diff {job_id} --stat`)",
                worktree_path.display()
            ),
        )
        .into());
    }

    // Paths are handed to git as RAW BYTES (status came from `-z`, whose names
    // are raw on-disk bytes): the lossy display form must not be re-encoded,
    // since a non-UTF8 name would no longer round-trip to the real file.
    // Each per-entry pathspec is `:(literal)`-prefixed so a name that happens
    // to contain glob bytes or a leading `:` is matched verbatim, exactly like
    // the user's selection was.
    let raw_paths: Vec<Vec<u8>> = entries.iter().map(|e| e.path_bytes().to_vec()).collect();
    let literal: Vec<Vec<u8>> = raw_paths
        .iter()
        .map(|b| {
            let mut s = b":(literal)".to_vec();
            s.extend_from_slice(b);
            s
        })
        .collect();
    let literal_os: Vec<std::ffi::OsString> = literal
        .iter()
        .map(|b| std::ffi::OsString::from_vec(b.clone()))
        .collect();
    // The SAME `:(literal)`-prefixed pathspec list is what git ls-files, git
    // add and git commit below receive, so all selection-bearing calls see
    // exactly the paths the user selected -- never bare names (which git
    // would read as pathspec magic) and never the lossy display form.

    // Stage exactly the paths that carry a selected change. `git add -A --
    // <paths>` records the current state of those paths only -- including
    // untracked additions and file deletions -- and leaves every other path
    // untouched. A path whose deletion is ALREADY in the index is gone from
    // both the index and the worktree: `git add -A -- <path>` on it fails with
    // "pathspec did not match any files", so it takes no staging at all.
    // Detect those via one `git ls-files -z` batch (lists only index entries,
    // NUL-separated RAW bytes, and returns success even when a pathspec matches
    // nothing), so the comparison is byte-exact for every name.
    let ls_out = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &worktree_path.to_string_lossy(),
            "ls-files",
            "-z",
            "--",
        ])
        .args(&literal_os);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git ls-files' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;
    let index_paths: std::collections::HashSet<Vec<u8>> = ls_out
        .stdout
        .split(|b| *b == 0)
        .filter(|l| !l.is_empty())
        .map(|l| l.to_vec())
        .collect();

    let to_stage: Vec<&Vec<u8>> = raw_paths
        .iter()
        .filter(|p| {
            index_paths.contains(p.as_slice())
                || worktree_path
                    .join(PathBuf::from(OsStr::from_bytes(p)))
                    .exists()
        })
        .collect();

    if !to_stage.is_empty() {
        let stage_os: Vec<std::ffi::OsString> = to_stage
            .iter()
            .map(|p| {
                let mut s = b":(literal)".to_vec();
                s.extend_from_slice(p);
                std::ffi::OsString::from_vec(s)
            })
            .collect();
        let mut add = hardened_git();
        add.args(["-C", &worktree_path.to_string_lossy(), "add", "-A", "--"])
            .args(&stage_os);
        let add_out = crate::core::reaper::output_tracked(&mut add).with_context(|| {
            format!(
                "Failed to execute 'git add -A -- <selected paths>' in {} (ensure git is installed and in PATH)",
                worktree_path.display()
            )
        })?;
        if !add_out.status.success() {
            bail!(
                "git add (selected paths) failed in {}: {}",
                worktree_path.display(),
                String::from_utf8_lossy(&add_out.stderr).trim()
            );
        }
    }

    let commit_msg = format!("agent-run({}): {}", job_id, message);
    let commit_out = crate::core::reaper::output_tracked(&mut {
        let mut c = hardened_git();
        c.args([
            "-C",
            &worktree_path.to_string_lossy(),
            "commit",
            "-m",
            &commit_msg,
            "--",
        ])
        .args(&literal_os);
        c
    })
    .with_context(|| {
        format!(
            "Failed to execute 'git commit' in {} (ensure git is installed and in PATH)",
            worktree_path.display()
        )
    })?;

    if !commit_out.status.success() {
        // Between the status check and the commit the selected changes may
        // have been committed by another process (e.g. the agent itself):
        // report that nothing was committed instead of erroring.
        if String::from_utf8_lossy(&commit_out.stdout).contains("nothing to commit") {
            return Ok(false);
        }
        bail!(
            "git commit failed: {}",
            String::from_utf8_lossy(&commit_out.stderr).trim()
        );
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn init_test_git_repo(path: &Path) {
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

    #[test]
    fn test_validate_project_repo_nonexistent() {
        let temp = tempdir().unwrap();
        let err = validate_project_repo(temp.path(), "missing").unwrap_err();
        assert!(err.to_string().contains("Project directory does not exist"));
    }

    #[test]
    fn test_validate_project_repo_not_a_git_repo() {
        let temp = tempdir().unwrap();
        let not_git = temp.path().join("notgit");
        std::fs::create_dir_all(&not_git).unwrap();
        let err = validate_project_repo(temp.path(), "notgit").unwrap_err();
        assert!(err.to_string().contains("is not a valid git repository"));
    }

    #[test]
    fn test_validate_project_repo_success() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        let res = validate_project_repo(temp.path(), "myrepo");
        assert!(res.is_ok());
    }

    #[test]
    fn test_create_worktree_destination_already_exists() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt_path = temp.path().join("existing_wt");
        std::fs::create_dir_all(&wt_path).unwrap();

        let err = create_worktree(&repo_dir, &wt_path, "agent-run/branch1", "main").unwrap_err();
        assert!(err
            .to_string()
            .contains("Worktree destination path already exists"));
    }

    #[test]
    fn test_create_worktree_nonexistent_base_ref() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt_path = temp.path().join("new_wt");
        let err = create_worktree(
            &repo_dir,
            &wt_path,
            "agent-run/branch2",
            "nonexistent-ref-xyz",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Git reference 'nonexistent-ref-xyz' does not exist"),
            "Error was: {}",
            msg
        );
    }

    #[test]
    fn test_validate_external_worktree_same_repo_ok() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt = temp.path().join("ext_wt");
        create_worktree(&repo_dir, &wt, "feature/extern", "main").unwrap();

        let resolved = validate_external_worktree(&repo_dir, &wt).unwrap();
        assert_eq!(resolved, wt.canonicalize().unwrap());
    }

    #[test]
    fn test_validate_external_worktree_different_repo_rejected() {
        let temp = tempdir().unwrap();
        let repo_a = temp.path().join("repo_a");
        let repo_b = temp.path().join("repo_b");
        std::fs::create_dir_all(&repo_a).unwrap();
        std::fs::create_dir_all(&repo_b).unwrap();
        init_test_git_repo(&repo_a);
        init_test_git_repo(&repo_b);

        let err = validate_external_worktree(&repo_a, &repo_b).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not belong to the same git repository"),
            "Error was: {}",
            err
        );
    }

    #[test]
    fn test_validate_external_worktree_nonexistent_path() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let err = validate_external_worktree(&repo_dir, &temp.path().join("nope")).unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "Error was: {}",
            err
        );
    }

    #[test]
    fn test_remove_worktree_uncommitted_changes_requires_force() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt_path = temp.path().join("wt_changes");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_changes", "main").unwrap();

        // Write an uncommitted modification in the worktree
        std::fs::write(wt_path.join("uncommitted.txt"), "dirty worktree").unwrap();

        // Without force -> must fail and advise using --force
        let err = remove_worktree(&repo_dir, &wt_path, false).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--force"),
            "Error message should mention --force: {}",
            msg
        );
        assert!(
            msg.contains("uncommitted modifications or untracked files"),
            "Error message should identify uncommitted/untracked files: {}",
            msg
        );

        // With force -> must succeed
        assert!(remove_worktree(&repo_dir, &wt_path, true).is_ok());
    }

    #[test]
    fn test_worktree_dir_can_be_swept() {
        let temp = tempdir().unwrap();

        // Missing directory: nothing to protect.
        assert!(worktree_dir_can_be_swept(&temp.path().join("nope"), 20).is_ok());

        // Empty directory: safe.
        let empty = temp.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert!(worktree_dir_can_be_swept(&empty, 20).is_ok());

        // A .git file (linked-worktree pointer) or .git dir alone: safe.
        let only_dotgit = temp.path().join("only_dotgit");
        std::fs::create_dir_all(&only_dotgit).unwrap();
        std::fs::write(only_dotgit.join(".git"), "gitdir: /nowhere").unwrap();
        assert!(worktree_dir_can_be_swept(&only_dotgit, 20).is_ok());
        let only_dotgit_dir = temp.path().join("only_dotgit_dir");
        std::fs::create_dir_all(only_dotgit_dir.join(".git")).unwrap();
        assert!(worktree_dir_can_be_swept(&only_dotgit_dir, 20).is_ok());

        // Anything else: not safe, names reported (sorted, capped).
        let dirty = temp.path().join("dirty");
        std::fs::create_dir_all(&dirty).unwrap();
        std::fs::write(dirty.join(".git"), "gitdir: /nowhere").unwrap();
        std::fs::write(dirty.join("zeta.txt"), "x").unwrap();
        std::fs::write(dirty.join("alpha.txt"), "x").unwrap();
        let err = worktree_dir_can_be_swept(&dirty, 20).unwrap_err();
        assert_eq!(err, vec!["alpha.txt".to_string(), "zeta.txt".to_string()]);
        // The cap is honoured (and .git itself is never reported).
        let capped = worktree_dir_can_be_swept(&dirty, 1).unwrap_err();
        assert_eq!(capped, vec!["alpha.txt".to_string()]);
    }

    #[test]
    fn test_worktree_dir_can_be_swept_real_broken_worktree() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt_path = temp.path().join("wt_sweep");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_sweep", "main").unwrap();
        // Simulate the out-of-band breakage that makes `git worktree remove`
        // fail: drop the .git pointer and add an untracked file.
        std::fs::remove_file(wt_path.join(".git")).unwrap();
        std::fs::write(wt_path.join("out.dat"), "keep me").unwrap();

        let err = worktree_dir_can_be_swept(&wt_path, 20).unwrap_err();
        assert!(err.contains(&"out.dat".to_string()), "entries: {:?}", err);
        assert!(err.contains(&"file.txt".to_string()), "entries: {:?}", err);
    }

    #[test]
    fn test_count_unmerged_unpushed_with_and_without_base() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        // A branch at the same tip as main: no unpushed commits either way.
        let status = Command::new("git")
            .args(["-C", &repo_dir.to_string_lossy(), "branch", "agent-run/cnt"])
            .status()
            .unwrap();
        assert!(status.success());

        // Base ref exists -> counted against it, result is zero.
        let n = count_unmerged_unpushed_commits(&repo_dir, "agent-run/cnt", "main").unwrap();
        assert_eq!(n, 0);

        // Base ref missing (and no origin/main either) -> the check falls back
        // to remotes only, which finds nothing, so every commit is counted.
        let n = count_unmerged_unpushed_commits(&repo_dir, "agent-run/cnt", "nope").unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn test_delete_branch_success_and_missing() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        // Create a new branch
        let status = Command::new("git")
            .args([
                "-C",
                &repo_dir.to_string_lossy(),
                "branch",
                "test-to-delete",
            ])
            .status()
            .unwrap();
        assert!(status.success());

        // Deleting the branch should succeed
        assert!(delete_branch(&repo_dir, "test-to-delete").is_ok());

        // Deleting it again (now non-existent) should return Err
        let err = delete_branch(&repo_dir, "test-to-delete").unwrap_err();
        assert!(err.to_string().contains("git branch -D failed"));
    }

    #[test]
    fn test_is_lock_contention_error() {
        assert!(is_lock_contention_error(
            "fatal: Unable to create '/repo/.git/index.lock': File exists."
        ));
        assert!(is_lock_contention_error("error: cannot lock ref 'refs/heads/foo': Unable to create '/repo/.git/refs/heads/foo.lock'"));
        assert!(!is_lock_contention_error("fatal: invalid reference: main"));
        assert!(!is_lock_contention_error(
            "fatal: destination path already exists"
        ));
    }

    /// Resolves a git ref to its SHA inside `dir`.
    fn git_sha(dir: &Path, ref_name: &str) -> String {
        let out = Command::new("git")
            .args([
                "-C",
                &dir.to_string_lossy(),
                "rev-parse",
                &format!("{}^{{commit}}", ref_name),
            ])
            .output()
            .expect("rev-parse");
        assert!(
            out.status.success(),
            "rev-parse {} failed: {}",
            ref_name,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn test_create_worktree_resolves_head_to_concrete_sha() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let head_sha = git_sha(&repo_dir, "HEAD");

        let wt_path = temp.path().join("wt_head");
        let sha = create_worktree(&repo_dir, &wt_path, "agent-run/head", "HEAD").unwrap();

        // The stored base ref must be the repo's actual current commit SHA,
        // not the literal "HEAD" (which, in the fresh worktree, points at the
        // same commit forever and compares against itself).
        assert_eq!(sha, head_sha);
        assert_ne!(sha, "HEAD");
        // The worktree's branch sits at that same commit.
        assert_eq!(git_sha(&wt_path, "HEAD"), head_sha);
    }

    #[test]
    fn test_create_worktree_falls_back_to_origin_remote_tracking_ref() {
        let temp = tempdir().unwrap();

        // Bare remote standing in for `origin`.
        let bare = temp.path().join("origin.git");
        let bare_status = Command::new("git")
            .args(["init", "--bare", &bare.to_string_lossy()])
            .status()
            .expect("git init --bare");
        assert!(bare_status.success());

        // Working checkout that pushes to it, then drops its local branch to
        // simulate a git-sync shallow clone: detached HEAD, no local `main`,
        // only `origin/main` as a remote-tracking ref.
        let repo_dir = temp.path().join("clone");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run(&["remote", "add", "origin", &bare.to_string_lossy()]);
        run(&["push", "origin", "main"]);
        run(&["checkout", "--detach", "HEAD"]);
        run(&["branch", "-D", "main"]);

        // Sanity: the bare name must NOT resolve locally in this fixture.
        assert!(!git_ref_exists(&repo_dir, "main").unwrap());

        let wt_path = temp.path().join("wt_origin");
        let sha = create_worktree(&repo_dir, &wt_path, "agent-run/from-origin", "main").unwrap();
        assert_eq!(sha, git_sha(&repo_dir, "origin/main"));
    }

    #[test]
    fn test_run_diff_includes_untracked_files() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let wt_path = temp.path().join("wt_new");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_new", "main").unwrap();

        // Untracked (new) file: plain `git diff` never shows this by design;
        // run_diff must make it appear via intent-to-add.
        std::fs::write(wt_path.join("brand-new.txt"), "brand new content\n").unwrap();

        let diff = run_diff(&wt_path, None, &[], &[]).unwrap();
        assert!(
            diff.contains("brand-new.txt"),
            "diff must name the untracked file: {}",
            diff
        );
        assert!(
            diff.contains("brand new content"),
            "diff must include the untracked file's content: {}",
            diff
        );

        // Tracked-file modifications still show up alongside.
        std::fs::write(wt_path.join("file.txt"), "modified\n").unwrap();
        let diff2 = run_diff(&wt_path, None, &[], &[]).unwrap();
        assert!(
            diff2.contains("file.txt"),
            "diff must name the modified tracked file: {}",
            diff2
        );
        assert!(
            diff2.contains("modified"),
            "diff must show the tracked modification: {}",
            diff2
        );
    }

    /// Builds a worktree containing one of every kind of change relative to
    /// HEAD: an unstaged modification, a staged modification, a staged
    /// deletion, and an untracked file.
    fn build_worktree_full_diff_fixture(temp: &Path) -> (PathBuf, PathBuf) {
        let repo_dir = temp.join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        // Two more tracked files: `staged-mod.txt` (later staged-modified)
        // and `todelete.txt` (later staged-deleted).
        std::fs::write(repo_dir.join("staged-mod.txt"), "staged original\n").unwrap();
        std::fs::write(repo_dir.join("todelete.txt"), "to be deleted\n").unwrap();
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run(&["add", "-A"]);
        run(&["commit", "-m", "add second file"]);

        let wt_path = temp.join("wt_full");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_full", "main").unwrap();

        // 1. Unstaged modification of a tracked file (file.txt).
        std::fs::write(wt_path.join("file.txt"), "modified-unstaged\n").unwrap();
        // 2. Staged modification of a tracked file (staged-mod.txt).
        std::fs::write(wt_path.join("staged-mod.txt"), "modified-staged\n").unwrap();
        let add_status = Command::new("git")
            .args(["-C", &wt_path.to_string_lossy(), "add", "staged-mod.txt"])
            .status()
            .expect("git add");
        assert!(add_status.success());
        // 3. Staged deletion of a tracked file (todelete.txt).
        let rm_status = Command::new("git")
            .args(["-C", &wt_path.to_string_lossy(), "rm", "-f", "todelete.txt"])
            .status()
            .expect("git rm");
        assert!(rm_status.success());
        // 4. Untracked file.
        std::fs::write(wt_path.join("untracked-new.txt"), "untracked content\n").unwrap();
        (repo_dir, wt_path)
    }

    #[test]
    fn test_run_diff_uncommitted_mode_covers_staged_unstaged_deleted_and_untracked() {
        let temp = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_worktree_full_diff_fixture(temp.path());

        let diff = run_diff(&wt_path, None, &[], &[]).unwrap();

        // Unstaged modification appears.
        assert!(
            diff.contains("file.txt"),
            "unstaged modification missing: {}",
            diff
        );
        assert!(
            diff.contains("modified-unstaged"),
            "unstaged content missing: {}",
            diff
        );
        // Staged modification appears.
        assert!(
            diff.contains("staged-mod.txt"),
            "staged modification missing: {}",
            diff
        );
        assert!(
            diff.contains("modified-staged"),
            "staged content missing: {}",
            diff
        );
        // Staged deletion appears (plain `git diff` never shows it).
        assert!(
            diff.contains("todelete.txt"),
            "staged deletion missing: {}",
            diff
        );
        assert!(
            diff.contains("deleted file"),
            "deletion kind missing: {}",
            diff
        );
        // Untracked (intent-to-add) file appears with its content.
        assert!(
            diff.contains("untracked-new.txt"),
            "untracked file missing: {}",
            diff
        );
        assert!(
            diff.contains("untracked content"),
            "untracked content missing: {}",
            diff
        );
    }

    #[test]
    fn test_run_diff_base_ref_mode_uses_base_commit() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        let base_sha = git_sha(&repo_dir, "main");

        let wt_path = temp.path().join("wt_base");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_base", "main").unwrap();

        // A committed change on the worktree branch vs base.
        std::fs::write(wt_path.join("committed.txt"), "committed change\n").unwrap();
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(&wt_path)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run(&["add", "committed.txt"]);
        run(&["commit", "-m", "committed change"]);

        // Untracked files never appear in `git diff <commit>` output.
        std::fs::write(wt_path.join("untracked.txt"), "untracked\n").unwrap();

        let diff = run_diff(&wt_path, Some(&base_sha), &[], &[]).unwrap();
        assert!(
            diff.contains("committed.txt"),
            "committed diff missing: {}",
            diff
        );
        assert!(
            !diff.contains("untracked.txt"),
            "untracked file must not appear in base ref diff: {}",
            diff
        );
    }

    #[test]
    fn test_commit_uncommitted_changes_result_flag() {
        let temp = tempdir().unwrap();
        let repo_dir = temp.path().join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        // Dirty worktree -> commits and reports it.
        let wt_path = temp.path().join("wt_commit");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_commit", "main").unwrap();
        std::fs::write(wt_path.join("dirty.txt"), "dirty\n").unwrap();
        assert!(
            commit_uncommitted_changes(&wt_path, "job1", "summary").unwrap(),
            "commit on dirty worktree must report committed=true"
        );

        // Second call on the now-clean worktree -> no commit reported.
        assert!(
            !commit_uncommitted_changes(&wt_path, "job1", "summary").unwrap(),
            "clean worktree must report committed=false"
        );
    }

    #[test]
    fn test_commit_uncommitted_changes_propagates_status_failure() {
        let temp = tempdir().unwrap();
        let not_repo = temp.path().join("not-a-repo");
        std::fs::create_dir_all(&not_repo).unwrap();
        let err = commit_uncommitted_changes(&not_repo, "job1", "summary").unwrap_err();
        assert!(
            err.to_string().contains("git status failed"),
            "expected git status failure to propagate, got: {}",
            err
        );
    }

    #[test]
    fn test_build_diff_pathspecs_shape() {
        assert_eq!(
            build_diff_pathspecs(&[], &[]).unwrap(),
            Vec::<String>::new()
        );
        // Every --only entry is literal-qualified so glob/magic bytes in a
        // name cannot be interpreted as pathspec magic by git.
        assert_eq!(
            build_diff_pathspecs(&["docs/".to_string(), "src/main.rs".to_string()], &[]).unwrap(),
            vec![":(literal)docs/", ":(literal)src/main.rs"]
        );
        // --exclude entries stay globs (the documented exclude behaviour) but
        // are explicit `:(exclude,glob)` so they never inherit literal mode.
        assert_eq!(
            build_diff_pathspecs(&[], &["*.log".to_string()]).unwrap(),
            vec![":(exclude,glob)*.log"]
        );
        assert_eq!(
            build_diff_pathspecs(&["src/".to_string()], &["tests/fixtures/".to_string()]).unwrap(),
            vec![":(literal)src/", ":(exclude,glob)tests/fixtures/"]
        );

        // Leading '/' is refused: it would root-anchor and select nothing.
        let err = build_diff_pathspecs(&["/abs".to_string()], &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("relative to the worktree root"),
            "got: {}",
            err
        );
        let err = build_diff_pathspecs(&[], &["/abs".to_string()])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("relative to the worktree root"),
            "got: {}",
            err
        );

        // A leading ':' is pathspec magic: refused, with the explicit
        // escape-hatch spelled out (an already-literal entry passes through).
        let err = build_diff_pathspecs(&[":(top)foo".to_string()], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("pathspec magic"), "got: {}", err);
        assert!(err.contains(":(literal):(top)foo"), "escape-hatch: {}", err);
        assert_eq!(
            build_diff_pathspecs(&[":(literal):foo".to_string()], &[]).unwrap(),
            vec![":(literal):foo"]
        );
    }

    #[test]
    fn test_build_diff_pathspecs_rejects_empty_or_blank_values() {
        // A blank --only/--exclude value would become a bare `:(literal)` /
        // `:(exclude,glob)` pathspec that matches (excludes) EVERYTHING, so
        // `commit --only ""` via the API could commit the whole worktree.
        // Both flags bail on empty/whitespace-only entries, as discard bails
        // on blank paths.
        for (only, exclude, flag) in [
            (vec!["".to_string()], Vec::<String>::new(), "--only"),
            (vec!["   ".to_string()], Vec::<String>::new(), "--only"),
            (Vec::<String>::new(), vec!["".to_string()], "--exclude"),
            (Vec::<String>::new(), vec!["\t".to_string()], "--exclude"),
        ] {
            let err = build_diff_pathspecs(&only, &exclude)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("empty path in {flag}")),
                "blank {flag} must be refused: {err}"
            );
            // The "Refusing" prefix keeps the API mapping this a 400, not a 500.
            assert!(
                err.contains("Refusing"),
                "blank {flag} message must be 400-mapped: {err}"
            );
        }
        // A genuinely empty selection (no entries at all) is still fine:
        // no pathspecs, no error.
        assert_eq!(
            build_diff_pathspecs(&[], &[]).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_build_diff_pathspecs_literal_prefix_rules() {
        // The escape hatch keeps its meaning: `:(literal)X` is the literal
        // path X (a leading ':' in the name is exactly what it exists for).
        assert_eq!(
            build_diff_pathspecs(&[":(literal)docs/x".to_string()], &[]).unwrap(),
            vec![":(literal)docs/x"]
        );
        assert_eq!(
            build_diff_pathspecs(&[":(literal):foo".to_string()], &[]).unwrap(),
            vec![":(literal):foo"]
        );

        // A `:(literal` with a comma is combined magic (`:(literal,top)`),
        // never a prefix: refused on its own, with the 'Refusing' prefix
        // that keeps the API mapping this a 400.
        let err = build_diff_pathspecs(&[":(literal,x".to_string()], &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("comma after ':(literal' extends the magic group"),
            "got: {}",
            err
        );
        assert!(
            err.contains("Refusing"),
            "':(literal,' variant message must be 400-mapped: {err}"
        );

        // The rest after the prefix is re-validated like a bare --only
        // entry. Each of these would otherwise smuggle in pathspec magic or
        // select outside the worktree (a bare `:(literal)` alone matches
        // the ENTIRE worktree), so each shape must be refused.
        let bad_only = [
            ":(literal)",
            ":(literal)   ",
            ":(literal)/x",
            ":(literal,top)x",
            ":(literal)..",
            ":(literal)a/..",
            ":(literal)a/../b",
        ];
        for entry in bad_only {
            let err = build_diff_pathspecs(&[entry.to_string()], &[])
                .unwrap_err()
                .to_string();
            assert!(
                err.starts_with("Refusing"),
                "literal-prefix shape '{entry}' must be refused 400-style: {err}"
            );
        }
    }

    /// Tracked modified file + one untracked file + one tracked staged
    /// deletion: the selection fixture shared by the --only / --exclude /
    /// --stat tests below.
    fn build_selection_fixture(temp: &Path) -> (PathBuf, PathBuf) {
        let repo_dir = temp.join("myrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        std::fs::write(repo_dir.join("todelete2.txt"), "gone\n").unwrap();
        let run = |dir: &Path, args: &[&str]| {
            let status = Command::new("git")
                .current_dir(dir)
                .args(args)
                .status()
                .expect("execute git");
            assert!(status.success(), "git {:?} failed", args);
        };
        run(&repo_dir, &["add", "-A"]);
        run(&repo_dir, &["commit", "-m", "add todelete2"]);

        let wt_path = temp.join("wt_sel");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_sel", "main").unwrap();

        // M: modified tracked file (5-byte content)
        std::fs::write(wt_path.join("file.txt"), "mod!\n").unwrap();
        // D: staged deletion of a tracked file
        run(&wt_path, &["rm", "-f", "todelete2.txt"]);
        // A: two untracked files (distinct sizes)
        std::fs::write(wt_path.join("alpha.txt"), "aaaa\n").unwrap();
        std::fs::write(wt_path.join("beta.txt"), "bbbb\nbb\n").unwrap();
        (repo_dir, wt_path)
    }

    #[test]
    fn test_run_diff_only_restricts_to_selected_paths() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let only = vec!["alpha.txt".to_string()];

        let diff = run_diff(&wt_path, None, &only, &[]).unwrap();
        assert!(
            diff.contains("alpha.txt") && diff.contains("aaaa"),
            "selected file must appear: {}",
            diff
        );
        for absent in ["file.txt", "todelete2.txt", "beta.txt"] {
            assert!(
                !diff.contains(absent),
                "unselected file '{}' must not appear: {}",
                absent,
                diff
            );
        }
    }

    #[test]
    fn test_run_diff_exclude_drops_matching_paths() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let exclude = vec!["beta.txt".to_string()];

        let diff = run_diff(&wt_path, None, &[], &exclude).unwrap();
        assert!(
            diff.contains("alpha.txt"),
            "kept file must appear: {}",
            diff
        );
        assert!(
            diff.contains("file.txt"),
            "modified file must appear: {}",
            diff
        );
        assert!(
            diff.contains("todelete2.txt"),
            "staged deletion must appear: {}",
            diff
        );
        assert!(
            !diff.contains("beta.txt"),
            "excluded file must not appear: {}",
            diff
        );
    }

    #[test]
    fn test_run_diff_only_does_not_mark_unselected_untracked() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let only = vec!["alpha.txt".to_string()];

        run_diff(&wt_path, None, &only, &[]).unwrap();

        // Intent-to-add must have been applied to the selected untracked
        // file only: beta.txt must still be plain untracked.
        let status = Command::new("git")
            .args(["-C", &wt_path.to_string_lossy(), "status", "--porcelain"])
            .output()
            .expect("git status");
        let p = String::from_utf8_lossy(&status.stdout);
        assert!(p.lines().any(|l| l.contains("alpha.txt")), "status: {}", p);
        assert!(
            p.lines()
                .any(|l| l.starts_with("??") && l.contains("beta.txt")),
            "unselected untracked file must not be marked: {}",
            p
        );
    }

    #[test]
    fn test_run_diff_only_on_staged_deletion_returns_hunk() {
        // A staged-ONLY deletion (file absent from worktree AND index) must
        // not abort the diff with "pathspec did not match any files": the
        // untracked-mark list computed from status is empty for it, so
        // `git add -N` is skipped entirely and `git diff HEAD` still shows
        // the deletion.
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let only = vec!["todelete2.txt".to_string()];

        let diff = run_diff(&wt_path, None, &only, &[]).unwrap();
        assert!(
            diff.contains("todelete2.txt"),
            "deletion must appear: {}",
            diff
        );
        assert!(
            diff.contains("deleted file mode 100644"),
            "must be a deletion hunk: {}",
            diff
        );
        for absent in ["alpha.txt", "beta.txt", "file.txt"] {
            assert!(
                !diff.contains(absent),
                "unselected '{}' must not appear: {}",
                absent,
                diff
            );
        }
    }

    #[test]
    fn test_run_diff_only_on_unchanged_tracked_dir_is_empty() {
        // A directory that exists in the index but has NO changes is a
        // legitimate selection: the diff is empty and it must not error
        // (the old code passed the dir pathspec to `git add -N`, which can
        // refuse it; with the status-driven mark list there is nothing to
        // mark, so `add -N` is skipped).
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        // A directory present in the index and UNCHANGED in the worktree.
        // Commit it with a pathspec so the fixture's staged-only deletion
        // (todelete2.txt) stays out of the commit and still shows in the
        // full diff below.
        std::fs::create_dir_all(wt_path.join("committed_dir")).unwrap();
        std::fs::write(wt_path.join("committed_dir/inner.txt"), "x\n").unwrap();
        let run = |dir: &Path, args: &[&str]| {
            let status = Command::new("git")
                .current_dir(dir)
                .args(args)
                .status()
                .expect("execute git");
            assert!(status.success(), "git {:?} failed", args);
        };
        run(&wt_path, &["add", "committed_dir"]);
        run(
            &wt_path,
            &[
                "commit",
                "-m",
                "committed dir",
                "--",
                "committed_dir/inner.txt",
            ],
        );

        let only = vec!["committed_dir".to_string()];
        let diff = run_diff(&wt_path, None, &only, &[]).unwrap();
        assert!(
            diff.trim().is_empty(),
            "expected an empty diff, got: {}",
            diff
        );
        // The other, genuinely changed paths are untouched by the selection
        // and still present in the full uncommitted diff.
        let full = run_diff(&wt_path, None, &[], &[]).unwrap();
        assert!(full.contains("alpha.txt"), "full diff lost alpha: {}", full);
        assert!(
            full.contains("todelete2.txt"),
            "full diff lost the staged deletion: {}",
            full
        );
    }

    #[test]
    fn test_worktree_status_lists_all_change_kinds_with_sizes() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());

        let entries = worktree_status(
            &wt_path,
            &[
                "file.txt".to_string(),
                "todelete2.txt".to_string(),
                "alpha.txt".to_string(),
                "beta.txt".to_string(),
            ],
            &[],
        )
        .unwrap();
        let by_path: std::collections::HashMap<&str, &WorktreeFileStatus> =
            entries.iter().map(|e| (e.path.as_str(), e)).collect();
        assert_eq!(by_path.len(), 4, "expected 4 entries: {:?}", entries);

        let m = by_path.get("file.txt").expect("file.txt");
        assert_eq!(m.status, "M");
        assert_eq!(m.size, 5, "modified file keeps its worktree size");

        let d = by_path.get("todelete2.txt").expect("todelete2.txt");
        assert_eq!(d.status, "D");
        assert_eq!(d.size, 0, "deleted file reports size 0");

        let a1 = by_path.get("alpha.txt").expect("alpha.txt");
        assert_eq!(a1.status, "A");
        assert_eq!(a1.size, 5, "untracked file reports its worktree size");

        let a2 = by_path.get("beta.txt").expect("beta.txt");
        assert_eq!(a2.status, "A");
        assert_eq!(a2.size, 8);
    }

    #[test]
    fn test_worktree_status_honours_only_and_exclude() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());

        let only = worktree_status(&wt_path, &["alpha.txt".to_string()], &[]).unwrap();
        let paths: Vec<&str> = only.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["alpha.txt"]);

        let excl = worktree_status(&wt_path, &[], &["*.txt".to_string()]).unwrap();
        assert!(
            excl.is_empty(),
            "* must exclude every fixture file: {:?}",
            excl
        );

        // A selection matching nothing is an empty list, not an error.
        let none = worktree_status(&wt_path, &["no-such-dir/".to_string()], &[]).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn test_worktree_status_rename_shows_new_and_deleted_paths() {
        let temp_keep = tempdir().unwrap();
        let repo_dir = temp_keep.path().join("renamerepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        let wt_path = temp_keep.path().join("wt_rename");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_rename", "main").unwrap();

        // Rename the one tracked file (content moves with it).
        std::fs::rename(wt_path.join("file.txt"), wt_path.join("renamed.txt")).unwrap();

        let entries = worktree_status(&wt_path, &[], &[]).unwrap();
        let by_path: std::collections::HashMap<&str, &WorktreeFileStatus> =
            entries.iter().map(|e| (e.path.as_str(), e)).collect();
        // The -z rename record lists the destination first: it is the new
        // file; the old name is the deletion.
        let new = by_path
            .get("renamed.txt")
            .unwrap_or_else(|| panic!("missing renamed.txt in {:?}", entries));
        assert_eq!(new.status, "A");
        assert_eq!(new.size, 5, "content of the renamed file: {:?}", entries);
        let old = by_path
            .get("file.txt")
            .unwrap_or_else(|| panic!("missing file.txt in {:?}", entries));
        assert_eq!(old.status, "D");
        assert_eq!(old.size, 0);
    }

    /// Worktree with three kinds of changes relative to a base commit: one
    /// change the agent already COMMITTED on top of the base, one uncommitted
    /// modification of a base file, one untracked (never added) file, and one
    /// base file deleted (uncommitted). Returns (base sha, worktree).
    fn build_vs_base_fixture(temp: &Path) -> (String, PathBuf) {
        let repo_dir = temp.join("vsbaserepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        // A second base-tracked file, later deleted uncommitted.
        std::fs::write(repo_dir.join("todelete.txt"), "doomed\n").unwrap();
        let run = |dir: &Path, args: &[&str]| {
            let status = Command::new("git")
                .current_dir(dir)
                .args(args)
                .status()
                .expect("execute git");
            assert!(status.success(), "git {:?} failed", args);
        };
        run(&repo_dir, &["add", "todelete.txt"]);
        run(&repo_dir, &["commit", "-m", "add todelete"]);
        let base_sha = git_sha(&repo_dir, "HEAD");

        let wt_path = temp.join("wt_vsbase");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_vsbase", "main").unwrap();

        // Committed past the base: the agent added a file and committed it.
        std::fs::write(wt_path.join("committed.txt"), "committed\n").unwrap();
        run(&wt_path, &["add", "committed.txt"]);
        run(&wt_path, &["commit", "-m", "agent work"]);
        // Uncommitted: a modification of a base file...
        std::fs::write(wt_path.join("file.txt"), "changed!\n").unwrap();
        // ...the deletion of a base file...
        run(&wt_path, &["rm", "-f", "todelete.txt"]);
        // ...and an untracked file (not in the index, so `git diff <base>`
        // would not list it).
        std::fs::write(wt_path.join("untracked.txt"), "untracked\n").unwrap();
        (base_sha, wt_path)
    }

    #[test]
    fn test_worktree_status_vs_base_lists_committed_and_uncommitted() {
        let temp_keep = tempdir().unwrap();
        let (base_sha, wt_path) = build_vs_base_fixture(temp_keep.path());

        let entries = worktree_status_vs_base(&wt_path, &base_sha, &[], &[]).unwrap();
        let by_path: std::collections::HashMap<&str, &WorktreeFileStatus> =
            entries.iter().map(|e| (e.path.as_str(), e)).collect();

        // Step-by-step recount of `build_vs_base_fixture` and what it means for
        // "differs from base":
        //   - base = repo HEAD = {file.txt "hello", todelete.txt "doomed\n"}.
        //   - committed past the base: committed.txt created + committed
        //        -> differs: A committed.txt (worktree has it, base does not).
        //   - uncommitted modification of a base file: file.txt -> "changed!\n"
        //        -> differs: M file.txt (present in both, content changed).
        //   - uncommitted deletion of a base file: todelete.txt removed
        //        -> differs: D todelete.txt (base has it, worktree does not).
        //   - untracked file: untracked.txt written, never added
        //        -> differs: A untracked.txt (worktree has it, base does not;
        //           only the untracked scan sees it, `git diff <base>` alone
        //           never lists a path missing from the index).
        // Nothing else differs (the worktree branch is clean otherwise), so the
        // REAL number of differing paths is exactly 4.
        assert_eq!(entries.len(), 4, "expected 4 entries: {:?}", entries);

        // The committed change is listed (vs HEAD it would be clean).
        let c = by_path
            .get("committed.txt")
            .unwrap_or_else(|| panic!("missing committed.txt in {:?}", entries));
        assert_eq!(c.status, "A");
        assert_eq!(c.size, 10);

        // The uncommitted modification of a base file ("changed!\n" = 9 bytes).
        let m = by_path
            .get("file.txt")
            .unwrap_or_else(|| panic!("missing file.txt in {:?}", entries));
        assert_eq!(m.status, "M");
        assert_eq!(m.size, 9);

        // The uncommitted deletion reports size 0.
        let d = by_path
            .get("todelete.txt")
            .unwrap_or_else(|| panic!("missing todelete.txt in {:?}", entries));
        assert_eq!(d.status, "D");
        assert_eq!(d.size, 0);

        // The untracked file: `git diff <base>` alone never lists it; the
        // untracked scan (git ls-files --others) adds it with status A.
        let u = by_path
            .get("untracked.txt")
            .unwrap_or_else(|| panic!("missing untracked.txt in {:?}", entries));
        assert_eq!(u.status, "A");
        assert_eq!(u.size, 10);

        // Assert the EXACT (status, path) set, not just the count: the set must
        // be precisely the four differing paths above -- no more, no less.
        let got: std::collections::BTreeSet<(String, String)> = entries
            .iter()
            .map(|e| (e.status.to_string(), e.path.clone()))
            .collect();
        let want: std::collections::BTreeSet<(String, String)> = [
            ("A".to_string(), "committed.txt".to_string()),
            ("M".to_string(), "file.txt".to_string()),
            ("D".to_string(), "todelete.txt".to_string()),
            ("A".to_string(), "untracked.txt".to_string()),
        ]
        .iter()
        .cloned()
        .collect();
        assert_eq!(got, want, "exact (status,path) set mismatch: {:?}", entries);
    }

    #[test]
    fn test_worktree_status_vs_base_deleted_but_recreated_is_modified() {
        let temp_keep = tempdir().unwrap();
        let repo_dir = temp_keep.path().join("wt_replrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);

        let run = |dir: &Path, args: &[&str]| {
            let status = Command::new("git")
                .current_dir(dir)
                .args(args)
                .status()
                .expect("execute git");
            assert!(status.success(), "git {:?} failed", args);
        };

        // base: a single tracked file committed on the base branch.
        std::fs::write(repo_dir.join("a.txt"), "old content\n").unwrap();
        run(&repo_dir, &["add", "a.txt"]);
        run(&repo_dir, &["commit", "-m", "add a"]);
        let base_sha = git_sha(&repo_dir, "HEAD");

        let wt_path = temp_keep.path().join("wt_repl");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_repl", "main").unwrap();

        // Drop the file from the index and commit: it is gone from the
        // worktree branch's tree, but the base commit still holds it, so
        // `git diff <base>` reports it as a DELETION...
        run(&wt_path, &["rm", "--cached", "a.txt"]);
        run(&wt_path, &["commit", "-m", "drop a from index"]);
        // ...then recreate it as an UNTRACKED file with new content. The file
        // now EXISTS on disk even though the base diff says it was deleted.
        std::fs::write(wt_path.join("a.txt"), "replaced\n").unwrap();

        let entries = worktree_status_vs_base(&wt_path, &base_sha, &[], &[]).unwrap();
        let by_path: std::collections::HashMap<&str, &WorktreeFileStatus> =
            entries.iter().map(|e| (e.path.as_str(), e)).collect();

        // A recreated file is a MODIFICATION, not a deletion: one entry,
        // status M, the real on-disk size -- not a size-0 `D`.
        assert_eq!(
            entries.len(),
            1,
            "expected exactly one entry: {:?}",
            entries
        );
        let e = by_path
            .get("a.txt")
            .unwrap_or_else(|| panic!("missing a.txt in {:?}", entries));
        assert_eq!(
            e.status, "M",
            "recreated file must be reported M, not a deletion: {:?}",
            entries
        );
        assert_eq!(
            e.size, 9,
            "recreated file keeps its worktree size: {:?}",
            entries
        );
    }

    #[test]
    fn test_worktree_status_vs_base_honours_only_and_exclude() {
        let temp_keep = tempdir().unwrap();
        let (base_sha, wt_path) = build_vs_base_fixture(temp_keep.path());

        // --only restricts BOTH the tracked delta and the untracked scan.
        let only =
            worktree_status_vs_base(&wt_path, &base_sha, &["committed.txt".to_string()], &[])
                .unwrap();
        let paths: Vec<&str> = only.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["committed.txt"]);

        // The plain HEAD view of `worktree_status` must NOT see the committed
        // change -- this is the divergence `--base --stat` is fixing.
        let head_view: Vec<String> = worktree_status(&wt_path, &[], &[])
            .unwrap()
            .iter()
            .map(|e| e.path.clone())
            .collect();
        assert!(
            !head_view.iter().any(|p| p == "committed.txt"),
            "HEAD view must not list the committed file: {head_view:?}"
        );

        // --exclude drops matching entries from both directions.
        let excl =
            worktree_status_vs_base(&wt_path, &base_sha, &[], &["*.txt".to_string()]).unwrap();
        assert!(excl.is_empty(), "* must exclude everything: {:?}", excl);
    }

    #[test]
    fn test_commit_selected_commits_only_selected_paths() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let only = vec!["alpha.txt".to_string()];

        let committed = commit_selected(&wt_path, "job1", "pick alpha only", &only, &[]).unwrap();
        assert!(committed, "commit must report a commit");

        let log_out = Command::new("git")
            .args(["-C", &wt_path.to_string_lossy(), "log", "-1", "--format=%s"])
            .output()
            .expect("git log");
        let log_subject = String::from_utf8_lossy(&log_out.stdout);
        assert!(
            log_subject.starts_with("agent-run(job1): pick alpha only"),
            "commit subject: {}",
            log_subject
        );

        // Only the selected file was committed...
        let committed_files = String::from_utf8_lossy(
            &Command::new("git")
                .args([
                    "-C",
                    &wt_path.to_string_lossy(),
                    "show",
                    "--name-only",
                    "--format=",
                    "HEAD",
                ])
                .output()
                .expect("git show")
                .stdout,
        )
        .trim()
        .to_string();
        assert_eq!(
            committed_files, "alpha.txt",
            "committed files: {}",
            committed_files
        );

        // ...and the rest of the worktree is still dirty with the same kinds.
        let status_out = Command::new("git")
            .args(["-C", &wt_path.to_string_lossy(), "status", "--porcelain"])
            .output()
            .expect("git status");
        let status = String::from_utf8_lossy(&status_out.stdout);
        assert!(
            status.contains("file.txt"),
            "modification must remain: {}",
            status
        );
        assert!(
            status.contains("todelete2.txt"),
            "deletion must remain: {}",
            status
        );
        assert!(
            status.contains("?? beta.txt"),
            "untracked must remain: {}",
            status
        );
    }

    #[test]
    fn test_commit_selected_exclude_leaves_excluded_uncommitted() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let exclude = vec!["beta.txt".to_string()];

        let committed = commit_selected(&wt_path, "job1", "exclude beta", &[], &exclude).unwrap();
        assert!(committed);

        let show_out = Command::new("git")
            .args([
                "-C",
                &wt_path.to_string_lossy(),
                "show",
                "--name-only",
                "--format=",
                "HEAD",
            ])
            .output()
            .expect("git show");
        let committed_files = String::from_utf8_lossy(&show_out.stdout);
        assert!(
            !committed_files.contains("beta.txt"),
            "excluded file committed: {}",
            committed_files
        );
        assert!(
            committed_files.contains("alpha.txt"),
            "kept file must be committed: {}",
            committed_files
        );
        assert!(
            committed_files.contains("file.txt"),
            "kept file must be committed: {}",
            committed_files
        );

        let status_out = Command::new("git")
            .args(["-C", &wt_path.to_string_lossy(), "status", "--porcelain"])
            .output()
            .expect("git status");
        let status = String::from_utf8_lossy(&status_out.stdout);
        assert!(
            status.contains("?? beta.txt"),
            "excluded untracked must remain: {}",
            status
        );
    }

    #[test]
    fn test_commit_selected_matching_nothing_errors() {
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        let only = vec!["no-such-path/".to_string()];
        let err = commit_selected(&wt_path, "job1", "empty pick", &only, &[]).unwrap_err();
        assert!(
            err.to_string().contains("matched no changes"),
            "expected the matched-nothing error, got: {}",
            err
        );

        // Nothing may have been committed.
        let before_out = Command::new("git")
            .args([
                "-C",
                &wt_path.to_string_lossy(),
                "rev-list",
                "--count",
                "HEAD",
            ])
            .output()
            .expect("git rev-list");
        let count_before = String::from_utf8_lossy(&before_out.stdout);
        let after_out = Command::new("git")
            .args([
                "-C",
                &wt_path.to_string_lossy(),
                "rev-list",
                "--count",
                "HEAD",
            ])
            .output()
            .expect("git rev-list");
        let count_after = String::from_utf8_lossy(&after_out.stdout);
        assert_eq!(count_before.trim(), count_after.trim());
    }

    #[test]
    fn test_commit_selected_no_selection_clean_worktree_returns_false() {
        // A clean worktree without a selection keeps the historical
        // Ok(false) fast path (no error).
        let temp_keep = tempdir().unwrap();
        let (_repo_dir, wt_path) = build_selection_fixture(temp_keep.path());
        // Commit everything first, then the worktree is clean.
        assert!(commit_uncommitted_changes(&wt_path, "job1", "all").unwrap());
        assert!(!commit_selected(&wt_path, "job1", "again", &[], &[]).unwrap());
    }

    #[test]
    fn test_selection_sees_file_named_colon_foo() {
        let temp_keep = tempdir().unwrap();
        let repo_dir = temp_keep.path().join("colonrepo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        let wt_path = temp_keep.path().join("wt_colon");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_colon", "main").unwrap();

        // A file whose name is literally `:foo`: git pathspec magic must
        // never be applied to user choices, so selections address it through
        // the explicit `:(literal)` escape hatch.
        std::fs::write(wt_path.join(":foo"), ":foo!\n").unwrap();

        let entries = worktree_status(&wt_path, &[":(literal):foo".to_string()], &[]).unwrap();
        assert_eq!(
            entries.len(),
            1,
            "literal :foo must be listed: {:?}",
            entries
        );
        assert_eq!(entries[0].path, ":foo");
        assert_eq!(entries[0].status, "A");
        assert_eq!(entries[0].size, 6);

        // Bare magic is refused: a leading ':' without the (literal) prefix.
        let err = worktree_status(&wt_path, &[":foo".to_string()], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("pathspec magic"), "got: {}", err);

        // A leading '/' is refused for --only as well (root-anchoring).
        let err = run_diff(&wt_path, None, &["/abs".to_string()], &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("paths are relative to the worktree root"),
            "got: {}",
            err
        );

        // status / diff / ls-files / add / commit all take the SAME
        // `:(literal)` pathspec list, so the literal colon name survives the
        // full commit path too.
        let diff = run_diff(&wt_path, None, &[":(literal):foo".to_string()], &[]).unwrap();
        assert!(diff.contains(":foo"), "diff must show :foo: {}", diff);
        let committed = commit_selected(
            &wt_path,
            "jobc",
            "colon",
            &[":(literal):foo".to_string()],
            &[],
        )
        .unwrap();
        assert!(committed, "commit must succeed for a literal :name");
        let show_out = Command::new("git")
            .args([
                "-C",
                &wt_path.to_string_lossy(),
                "show",
                "--name-only",
                "--format=",
                "HEAD",
            ])
            .output()
            .expect("git show");
        assert_eq!(
            String::from_utf8_lossy(&show_out.stdout).trim(),
            ":foo",
            "committed name must be the literal name"
        );
    }

    #[test]
    fn test_status_keeps_non_utf8_file_names() {
        use std::os::unix::ffi::OsStringExt;
        let temp_keep = tempdir().unwrap();
        let repo_dir = temp_keep.path().join("utf8repo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        init_test_git_repo(&repo_dir);
        let wt_path = temp_keep.path().join("wt_utf8");
        create_worktree(&repo_dir, &wt_path, "agent-run/wt_utf8", "main").unwrap();

        // A changed file whose name is NOT valid UTF-8: it must be listed
        // (with a lossy display path), never silently dropped.
        std::fs::write(
            wt_path.join(std::ffi::OsString::from_vec(b"bad_\xff.png".to_vec())),
            b"bin\n",
        )
        .unwrap();
        std::fs::write(wt_path.join("file.txt"), "mod!\n").unwrap();

        let entries = worktree_status(&wt_path, &[], &[]).unwrap();
        let bad = entries
            .iter()
            .find(|e| e.path == "bad_\u{FFFD}.png")
            .unwrap_or_else(|| panic!("non-UTF8 name missing from status: {:?}", entries));
        assert_eq!(bad.status, "A");
        assert_eq!(bad.size, 4, "size must come from the on-disk name bytes");
        assert_eq!(bad.path_bytes(), b"bad_\xff.png");

        // The regular file is still listed alongside it.
        assert!(
            entries.iter().any(|e| e.path == "file.txt"),
            "regular entries must survive too: {:?}",
            entries
        );
    }

    #[test]
    fn test_parse_porcelain_z_keeps_raw_bytes() {
        let raw = b"?? bad_\xff.png\0 M file.txt\0";
        let entries = parse_porcelain_z(raw);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "bad_\u{FFFD}.png");
        assert_eq!(entries[0].path_bytes(), b"bad_\xff.png");
        assert_eq!(entries[0].status, "A");
        assert_eq!(entries[1].path, "file.txt");
        assert_eq!(entries[1].status, "M");
    }

    #[test]
    fn test_porcelain_status_letter_mapping() {
        // Untracked reads as new.
        assert_eq!(porcelain_status_letter("??"), "A");
        // Staged addition reads as new.
        assert_eq!(porcelain_status_letter("A "), "A");
        // Intent-to-add (run_diff's `git add -N` leaves exactly this) reads
        // as new, not modified -- the T47 failure case.
        assert_eq!(porcelain_status_letter(" A"), "A");
        // Staged addition + worktree changes on top of the placeholder still
        // reads as new.
        assert_eq!(porcelain_status_letter("AM"), "A");
        // Unstaged and staged modifications read as modified.
        assert_eq!(porcelain_status_letter(" M"), "M");
        assert_eq!(porcelain_status_letter("M "), "M");
        // A deletion on either side reads as deleted (D takes precedence).
        assert_eq!(porcelain_status_letter(" D"), "D");
        assert_eq!(porcelain_status_letter("D "), "D");
    }

    // ------------------------------------------------------------------
    // H2: hardened git -- the code-execution vectors an agent can plant in a
    // repository's own config or hooks must never run through our invocations.
    // Each test plants a marker-producing payload in the repo config and proves
    // the hardened path does NOT trigger it, while the same unhardened git call
    // DOES (the control arm proves the payload is live, not inert).
    // ------------------------------------------------------------------

    /// Builds a linked worktree of a private bare repo laid out exactly as the
    /// production job worktrees are: `<root>/<project>.git` (bare) plus the
    /// linked worktree at `<root>/<project>.git/worktrees/<id>` via its `.git`
    /// file. Returns `(bare_repo, worktree_path, root)`.
    fn linked_worktree_under_root(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
        // A real (non-bare) seed repo to create the bare from.
        let seed = root.join("seed");
        std::fs::create_dir_all(&seed).unwrap();
        let g = |args: &[&str], dir: &Path| {
            let st = Command::new("git")
                .current_dir(dir)
                .args(args)
                .status()
                .expect("execute git");
            assert!(st.success(), "git {args:?} in {} failed", dir.display());
        };
        g(&["init", "-b", "main"], &seed);
        g(&["config", "user.name", "T"], &seed);
        g(&["config", "user.email", "t@t.t"], &seed);
        std::fs::write(seed.join("f.txt"), "base\n").unwrap();
        g(&["add", "f.txt"], &seed);
        g(&["commit", "-m", "i"], &seed);

        let bare = root.join("proj.git");
        g(
            &[
                "clone",
                "-q",
                "--bare",
                seed.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
            root,
        );

        // A linked worktree of the bare repo.
        let wt = root.join("wt");
        g(
            &[
                "worktree",
                "add",
                "-q",
                wt.to_str().unwrap(),
                "-b",
                "agent-run/x",
            ],
            &bare,
        );
        // A linked worktree has its own git-dir config that does NOT inherit
        // the seed repo's identity, so give it one: commits (the hooks test's
        // control arm) would otherwise fail with "Author identity unknown"
        // before any hook runs.
        g(&["config", "user.name", "T"], &wt);
        g(&["config", "user.email", "t@t.t"], &wt);
        let _ = std::fs::remove_dir_all(&seed);
        (bare, wt, root.to_path_buf())
    }

    /// Writes `body` to `path` and marks it executable. Git runs planted
    /// `core.fsmonitor` / `diff.external` / hook scripts as *programs*, so a
    /// non-executable file is silently ignored and the control arm of a
    /// hardening test would be inert.
    fn write_exec(path: &Path, body: &str) {
        use std::fs::{OpenOptions, Permissions};
        use std::os::unix::fs::PermissionsExt;
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap();
        use std::io::Write as _;
        f.write_all(body.as_bytes()).unwrap();
        std::fs::set_permissions(path, Permissions::from_mode(0o755)).unwrap();
    }

    /// The index file of a worktree (in its git-dir), used to force a "cold"
    /// index so git re-stats the worktree -- the only situation where a planted
    /// `core.fsmonitor` is actually invoked.
    fn worktree_index(worktree: &Path) -> PathBuf {
        let out = Command::new("git")
            .current_dir(worktree)
            .args(["rev-parse", "--git-dir"])
            .output()
            .expect("rev-parse --git-dir");
        assert!(out.status.success());
        let git_dir = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let git_dir = Path::new(&git_dir);
        if git_dir.is_relative() {
            worktree.join(git_dir).join("index")
        } else {
            git_dir.join("index")
        }
    }

    #[test]
    fn test_harden_git_neutralises_fsmonitor() {
        let temp = tempdir().unwrap();
        let (_bare, wt, _root) = linked_worktree_under_root(temp.path());
        let marker = temp.path().join("m_fsmonitor");
        let fs = temp.path().join("fsmonitor.sh");
        write_exec(&fs, &format!("#!/bin/sh\ntouch \"{}\"\n", marker.display()));
        // Plant the fsmonitor in the worktree's own repo config.
        let cfg = |args: &[&str]| {
            let st = Command::new("git")
                .current_dir(&wt)
                .args(args)
                .status()
                .expect("git config");
            assert!(st.success());
        };
        cfg(&["config", "core.fsmonitor", fs.to_str().unwrap()]);
        std::fs::write(wt.join("f.txt"), "base\nchange\n").unwrap();

        // Control: a PLAIN `git add -N -A` on a COLD index runs the planted
        // fsmonitor (git re-stats the worktree and invokes the script).
        let idx = worktree_index(&wt);
        if idx.exists() {
            std::fs::remove_file(&idx).unwrap();
        }
        let _ = Command::new("git")
            .current_dir(&wt)
            .args(["add", "-N", "-A"])
            .output()
            .expect("plain add -N -A");
        assert!(
            marker.exists(),
            "control: the planted fsmonitor must be live for the test to mean anything"
        );

        // Hardened: `run_diff` (which runs `git add -N -A` then `git diff`) on
        // a COLD index must NOT run it.
        std::fs::remove_file(&marker).unwrap();
        if idx.exists() {
            std::fs::remove_file(&idx).unwrap();
        }
        let _ = run_diff(&wt, None, &[], &[]).expect("run_diff");
        assert!(
            !marker.exists(),
            "hardened git ran a planted fsmonitor script (code execution)"
        );
    }

    #[test]
    fn test_harden_git_neutralises_external_diff() {
        let temp = tempdir().unwrap();
        let (_bare, wt, _root) = linked_worktree_under_root(temp.path());
        let marker = temp.path().join("m_extdiff");
        let ext = temp.path().join("extdiff.sh");
        write_exec(
            &ext,
            &format!("#!/bin/sh\ntouch \"{}\"\n", marker.display()),
        );
        let cfg = |args: &[&str]| {
            let st = Command::new("git")
                .current_dir(&wt)
                .args(args)
                .status()
                .expect("git config");
            assert!(st.success());
        };
        cfg(&["config", "diff.external", ext.to_str().unwrap()]);
        std::fs::write(wt.join("f.txt"), "base\nchange\n").unwrap();

        // Control: a plain `git diff --ext-diff HEAD` runs the planted driver.
        let _ = Command::new("git")
            .current_dir(&wt)
            .args(["diff", "--ext-diff", "HEAD"])
            .output()
            .expect("plain diff");
        assert!(
            marker.exists(),
            "control: the planted diff.external driver must be live"
        );

        // Hardened: `run_diff` must NOT run it (--no-ext-diff).
        std::fs::remove_file(&marker).unwrap();
        let _ = run_diff(&wt, None, &[], &[]).expect("run_diff");
        assert!(
            !marker.exists(),
            "hardened git ran a planted diff.external driver (code execution)"
        );
    }

    #[test]
    fn test_harden_git_neutralises_hooks() {
        let temp = tempdir().unwrap();
        let (_bare, wt, _root) = linked_worktree_under_root(temp.path());
        let marker = temp.path().join("m_hook");
        let hooks_dir = temp.path().join("attacker-hooks");
        std::fs::create_dir_all(&hooks_dir).unwrap();
        for h in ["pre-commit", "post-commit"] {
            write_exec(
                &hooks_dir.join(h),
                &format!("#!/bin/sh\ntouch \"{}\"\n", marker.display()),
            );
        }
        let cfg = |args: &[&str]| {
            let st = Command::new("git")
                .current_dir(&wt)
                .args(args)
                .status()
                .expect("git config");
            assert!(st.success());
        };
        // Point the worktree's hooks at the planted directory.
        cfg(&["config", "core.hooksPath", hooks_dir.to_str().unwrap()]);
        std::fs::write(wt.join("f.txt"), "base\nchange\n").unwrap();
        let _ = Command::new("git")
            .current_dir(&wt)
            .args(["add", "f.txt"])
            .status()
            .expect("add");

        // Control: a PLAIN commit runs the planted hooks.
        let _ = Command::new("git")
            .current_dir(&wt)
            .args(["commit", "-m", "ctrl"])
            .output()
            .expect("plain commit");
        assert!(
            marker.exists(),
            "control: the planted hooks must be live for the test to mean anything"
        );
        // Reset the index state for the hardened commit.
        let _ = std::fs::write(wt.join("f.txt"), "base\nchange\nhardened\n");
        let _ = Command::new("git")
            .current_dir(&wt)
            .args(["add", "f.txt"])
            .status()
            .expect("add");

        // Hardened: `commit_uncommitted_changes` must NOT run them.
        std::fs::remove_file(&marker).unwrap();
        let committed = commit_uncommitted_changes(&wt, "job1", "hardened").expect("commit");
        assert!(committed, "the hardened commit should have made a commit");
        assert!(
            !marker.exists(),
            "hardened git ran planted hooks (code execution)"
        );
    }

    #[test]
    fn test_worktree_gitdir_pointer_outside_root_refused() {
        // Two independent roots: the legitimate one (under the repos root) and
        // a genuinely unrelated directory the attacker would point at.
        let root_temp = tempdir().unwrap();
        let outside_temp = tempdir().unwrap();
        let (bare, wt, root) = linked_worktree_under_root(root_temp.path());
        let _ = bare;
        // A healthy pointer is accepted.
        worktree_gitdir_in_expected_roots(&wt, std::slice::from_ref(&root))
            .expect("healthy pointer accepted");

        // A pointer smuggled OUTSIDE the root must be refused.
        let outside = outside_temp.path().to_path_buf();
        let dotgit = wt.join(".git");
        let original = std::fs::read_to_string(&dotgit).unwrap();
        std::fs::write(&dotgit, format!("gitdir: {}\n", outside.display())).unwrap();
        let err = worktree_gitdir_in_expected_roots(&wt, std::slice::from_ref(&root)).unwrap_err();
        assert!(
            err.to_string()
                .contains("outside the repositories agent-run manages for this project"),
            "expected an outside-root refusal, got: {}",
            err
        );
        // A garbage pointer (no gitdir line) is refused.
        std::fs::write(&dotgit, "not a git pointer\n").unwrap();
        assert!(worktree_gitdir_in_expected_roots(&wt, std::slice::from_ref(&root)).is_err());
        // Restore for cleanup.
        std::fs::write(&dotgit, &original).unwrap();
    }

    #[test]
    fn test_worktree_gitdir_directory_is_self_contained() {
        // A worktree that is its own repo root (directory .git) has no pointer
        // to validate and is always accepted.
        let temp = tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let g = |args: &[&str]| {
            let st = Command::new("git")
                .current_dir(&repo)
                .args(args)
                .status()
                .expect("git");
            assert!(st.success());
        };
        g(&["init", "-b", "main"]);
        let unrelated_root = temp.path().join("elsewhere");
        std::fs::create_dir_all(&unrelated_root).unwrap();
        worktree_gitdir_in_expected_roots(&repo, std::slice::from_ref(&unrelated_root))
            .expect("a self-contained (directory .git) worktree is accepted");
    }

    #[test]
    fn test_worktree_gitdir_target_parses() {
        let temp = tempdir().unwrap();
        let (_bare, wt, _root) = linked_worktree_under_root(temp.path());
        let Some(target) = worktree_gitdir_target(&wt) else {
            panic!("expected a gitdir: target in the worktree .git file");
        };
        assert!(target.contains(".git/worktrees/"), "got: {target}");
        // A self-contained repo has no .git file, so no target.
        let selfc = temp.path().join("selfc");
        std::fs::create_dir_all(&selfc).unwrap();
        let st = Command::new("git")
            .current_dir(&selfc)
            .args(["init", "-b", "main"])
            .status()
            .unwrap();
        assert!(st.success());
        assert!(worktree_gitdir_target(&selfc).is_none());
    }

    #[test]
    fn test_expected_worktree_roots_accepts_reposync_roots_layout() {
        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        let repos_root = temp.path().join("repos");
        let roots_proj = workspace_root.join(".roots").join("project-a");
        let roots_git_wt = roots_proj.join(".git").join("worktrees").join("job123");
        let roots_sha_wt = roots_proj.join(".worktrees").join("deadbeef");
        std::fs::create_dir_all(&roots_git_wt).unwrap();
        std::fs::create_dir_all(&roots_sha_wt).unwrap();
        std::fs::write(
            roots_sha_wt.join(".git"),
            format!(
                "gitdir: {}\n",
                roots_proj
                    .join(".git")
                    .join("worktrees")
                    .join("deadbeef")
                    .display()
            ),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            ".roots/project-a/.worktrees/deadbeef",
            workspace_root.join("project-a"),
        )
        .unwrap();

        let wt = workspace_root
            .join(".agent-jobs")
            .join("project-a")
            .join("job123");
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(
            wt.join(".git"),
            format!("gitdir: {}\n", roots_git_wt.display()),
        )
        .unwrap();

        let config = crate::core::config::Config {
            workspace_root,
            repos_root,
            ..Default::default()
        };
        let allowed = expected_worktree_roots(&config, "project-a");
        worktree_gitdir_in_expected_roots(&wt, &allowed).expect(
            "worktree attached to /workspace/.roots/<project>/.git/worktrees/<id> must be accepted",
        );
    }
}

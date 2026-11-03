use std::io::Write;
use std::path::{Path, PathBuf};

/// Outcome of a memory-file injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectOutcome {
    /// The target `CLAUDE.md` was written (or updated) in the workdir.
    Written,
    /// The target already existed with identical bytes; left untouched.
    Unchanged,
}

/// Runs `git <args>` in `workdir`, returning `Ok(Some(stdout))` on exit 0,
/// `Ok(None)` when git reports it is not inside a work tree, and `Err` on any
/// other failure (git missing, workdir unreadable, etc.).
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

/// Returns the absolute path of the repo's `info/exclude` file for `workdir`.
/// The path reported by `git rev-parse --git-path info/exclude` may be
/// relative (e.g. `.git/info/exclude`), in which case it is resolved relative
/// to `workdir`.
fn exclude_file_path(workdir: &Path) -> Result<Option<PathBuf>, String> {
    let out = git_in_workdir(workdir, &["rev-parse", "--git-path", "info/exclude"])?;
    let Some(raw) = out else {
        return Ok(None);
    };
    let p = PathBuf::from(&raw);
    let resolved = if p.is_absolute() { p } else { workdir.join(p) };
    Ok(Some(resolved))
}

/// Ensures the line `/CLAUDE.md` is present in the repository's `info/exclude`
/// file. Idempotent: appends the line only if no existing line equals it.
/// Creates parent directories and the file itself if missing.
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

/// Checks whether `workdir` is a git work tree with `CLAUDE.md` tracked in the
/// index. Returns `Ok(true)` when tracked, `Ok(false)` when not a work tree or
/// the file is untracked/ignored, `Err` on unexpected git failures.
fn is_tracked_in_git(workdir: &Path) -> Result<bool, String> {
    git_in_workdir(workdir, &["ls-files", "--error-unmatch", "--", "CLAUDE.md"])
        .map(|o| o.is_some())
}

/// Injects the per-project CLAUDE.md into `<workdir>/CLAUDE.md` before a
/// session spawn.
///
/// * Reads `src` (following symlinks, as in Kubernetes ConfigMap mounts).
/// * If `workdir` is inside a git work tree, refuses to overwrite a tracked
///   `CLAUDE.md`, and otherwise ensures `/CLAUDE.md` is in `info/exclude`.
/// * Writes the target atomically (temp file + rename, mode 0644) only when
///   the bytes differ; if identical the existing file (and its mtime) is kept.
pub fn inject_memory_file(workdir: &Path, src: &Path) -> Result<InjectOutcome, String> {
    let data = std::fs::read(src)
        .map_err(|e| format!("failed to read memory file '{}': {}", src.display(), e))?;
    if data.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(format!(
            "memory file '{}' is empty or whitespace-only; refusing to inject",
            src.display()
        ));
    }

    let target = workdir.join("CLAUDE.md");
    let is_work_tree = git_in_workdir(workdir, &["rev-parse", "--is-inside-work-tree"])?.is_some();

    if is_work_tree {
        if is_tracked_in_git(workdir)? {
            return Err(
                "CLAUDE.md is tracked in the repository; delete it from the repo before injecting"
                    .to_string(),
            );
        }
        if let Some(exclude) = exclude_file_path(workdir)? {
            ensure_exclude_entry(&exclude)?;
        }
    }

    if let Ok(existing) = std::fs::read(&target) {
        if existing == data {
            return Ok(InjectOutcome::Unchanged);
        }
    }

    let tmp = workdir.join(format!(".CLAUDE.md.tmp-{}", std::process::id()));
    let write_result = (|| -> Result<(), String> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| format!("failed to create temp file {}: {}", tmp.display(), e))?;
        file.write_all(&data)
            .map_err(|e| format!("failed to write temp file {}: {}", tmp.display(), e))?;
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|e| format!("failed to set mode on {}: {}", tmp.display(), e))?;
        file.flush()
            .map_err(|e| format!("failed to flush temp file {}: {}", tmp.display(), e))?;
        drop(file);
        std::fs::rename(&tmp, &target).map_err(|e| {
            format!(
                "failed to rename {} -> {}: {}",
                tmp.display(),
                target.display(),
                e
            )
        })?;
        Ok(())
    })();
    match write_result {
        Ok(()) => Ok(InjectOutcome::Written),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn unique_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cs_memory_test_{}_{}_{}",
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
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["init", "-q"])
            .output()
            .expect("git must be available");
        assert!(out.status.success(), "git init failed");
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["config", "user.email", "test@example.com"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["config", "user.name", "test"])
            .output()
            .unwrap();
        assert!(out.status.success());
    }

    fn write_src(dir: &Path, name: &str, content: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn test_inject_in_git_worktree_writes_and_exclude() {
        let root = unique_temp_dir("worktree");
        let workdir = root.join("repo");
        std::fs::create_dir_all(&workdir).unwrap();
        git_init(&workdir);
        let src = write_src(&root, "src.md", "# Project memory\n");

        let outcome = inject_memory_file(&workdir, &src).expect("injection succeeds");
        assert_eq!(outcome, InjectOutcome::Written);
        assert_eq!(
            std::fs::read_to_string(workdir.join("CLAUDE.md")).unwrap(),
            "# Project memory\n"
        );

        let exclude = exclude_file_path(&workdir).unwrap().unwrap();
        let contents = std::fs::read_to_string(&exclude).unwrap();
        assert_eq!(contents.lines().filter(|l| *l == "/CLAUDE.md").count(), 1);

        let status = Command::new("git")
            .arg("-C")
            .arg(&workdir)
            .args(["status", "--porcelain"])
            .output()
            .unwrap();
        assert_eq!(status.stdout, b"");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_second_call_unchanged_and_single_exclude() {
        let root = unique_temp_dir("unchanged");
        let workdir = root.join("repo");
        std::fs::create_dir_all(&workdir).unwrap();
        git_init(&workdir);
        let src = write_src(&root, "src.md", "hello\n");
        let src_path = src.clone();

        assert_eq!(
            inject_memory_file(&workdir, &src_path).unwrap(),
            InjectOutcome::Written
        );
        let mtime_first = std::fs::metadata(workdir.join("CLAUDE.md"))
            .unwrap()
            .modified()
            .unwrap();

        assert_eq!(
            inject_memory_file(&workdir, &src_path).unwrap(),
            InjectOutcome::Unchanged
        );
        let mtime_second = std::fs::metadata(workdir.join("CLAUDE.md"))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(mtime_first, mtime_second);

        let exclude = exclude_file_path(&workdir).unwrap().unwrap();
        let contents = std::fs::read_to_string(&exclude).unwrap();
        assert_eq!(contents.lines().filter(|l| *l == "/CLAUDE.md").count(), 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_tracked_claude_md_refused_content_untouched() {
        let root = unique_temp_dir("tracked");
        let workdir = root.join("repo");
        std::fs::create_dir_all(&workdir).unwrap();
        git_init(&workdir);
        let tracked = workdir.join("CLAUDE.md");
        std::fs::write(&tracked, "original tracked content\n").unwrap();
        Command::new("git")
            .arg("-C")
            .arg(&workdir)
            .args(["add", "CLAUDE.md"])
            .output()
            .unwrap();
        Command::new("git")
            .arg("-C")
            .arg(&workdir)
            .args(["commit", "-q", "-m", "add"])
            .output()
            .unwrap();
        let src = write_src(&root, "src.md", "injected content\n");

        let err = inject_memory_file(&workdir, &src).unwrap_err();
        assert!(
            err.contains("tracked"),
            "expected tracked error, got: {}",
            err
        );
        assert_eq!(
            std::fs::read_to_string(&tracked).unwrap(),
            "original tracked content\n"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_non_git_dir_writes_no_error() {
        let root = unique_temp_dir("nongit");
        let workdir = root.join("plain");
        std::fs::create_dir_all(&workdir).unwrap();
        let src = write_src(&root, "src.md", "no git here\n");

        let outcome = inject_memory_file(&workdir, &src).expect("writes without git");
        assert_eq!(outcome, InjectOutcome::Written);
        assert_eq!(
            std::fs::read_to_string(workdir.join("CLAUDE.md")).unwrap(),
            "no git here\n"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_empty_source_is_error() {
        let root = unique_temp_dir("empty");
        let workdir = root.join("repo");
        std::fs::create_dir_all(&workdir).unwrap();
        git_init(&workdir);
        let src = write_src(&root, "src.md", "   \n\t ");
        assert!(inject_memory_file(&workdir, &src).is_err());
        assert!(!workdir.join("CLAUDE.md").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_symlink_source_works() {
        let root = unique_temp_dir("symlink");
        let workdir = root.join("repo");
        std::fs::create_dir_all(&workdir).unwrap();
        git_init(&workdir);
        let real = write_src(&root, "real.md", "via symlink\n");
        let link = root.join("link.md");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let outcome = inject_memory_file(&workdir, &link).expect("symlink source works");
        assert_eq!(outcome, InjectOutcome::Written);
        assert_eq!(
            std::fs::read_to_string(workdir.join("CLAUDE.md")).unwrap(),
            "via symlink\n"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}

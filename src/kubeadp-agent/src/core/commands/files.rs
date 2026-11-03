//! Read-only inspection of a job's git worktree: `files` lists entries and
//! `cat` prints one file. Both refuse to touch anything outside the
//! canonicalized worktree: paths are checked lexically (no absolute paths,
//! no `..` or `.git` components) BEFORE any filesystem access, and symlinks
//! resolving outside the worktree are rejected by `cat` or listed without
//! being followed by `files`.

use std::cmp::Ordering;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};

use crate::core::args::{CatArgs, FilesArgs};
use crate::core::config::Config;
use crate::core::job::JobMetadata;
use crate::core::output::CommandOutput;

/// `--depth` is clamped into 1..=MAX_LIST_DEPTH.
const MAX_LIST_DEPTH: u32 = 4;
/// Listing stops after this many entries and appends a `... truncated` line.
const MAX_LISTED_ENTRIES: usize = 2000;
/// Default byte cap for `cat` output.
const DEFAULT_MAX_BYTES: u64 = 1_048_576;
/// Hard cap for `--max-bytes`.
const HARD_CAP_MAX_BYTES: u64 = 8_388_608;
/// A NUL byte within this many leading bytes marks the file as binary.
const BINARY_SNIFF_BYTES: usize = 8192;

fn rfc3339(ts: SystemTime) -> String {
    let secs = match ts.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => 0,
    };
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, 0).unwrap();
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Rejects unsafe paths lexically, before any filesystem access: absolute
/// paths, `..` components and `.git` components.
fn reject_unsafe_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    if p.is_absolute() {
        bail!(
            "path must be relative to the worktree, got absolute path '{}'",
            path
        );
    }
    for comp in p.components() {
        match comp {
            Component::ParentDir => {
                bail!("path must not contain '..' components, got '{}'", path)
            }
            Component::Normal(c) if c.to_str() == Some(".git") => {
                bail!("path must not include a '.git' component, got '{}'", path)
            }
            _ => {}
        }
    }
    Ok(())
}

struct ResolvedTarget {
    /// Canonicalized target with symlinks followed; `None` only for a
    /// dangling symlink.
    canon: Option<PathBuf>,
    /// Metadata of the last component as found (does not follow symlinks).
    link_meta: fs::Metadata,
    /// True when the last component itself is a symlink.
    is_link: bool,
    /// True when the resolved target leaves the worktree (or is dangling).
    escaped: bool,
    /// True when the canonical target is the worktree's `.git` directory or
    /// something inside it.
    dotgit: bool,
}

/// Joins `path` onto the canonicalized worktree and canonicalizes the
/// result, so any symlink resolving outside the worktree is detected. A
/// canonical target that is (or passes through) the worktree's `.git`
/// directory is rejected as well, because a symlink with any other name can
/// point into `.git` even though the requested path has no `.git` component.
fn resolve_target(worktree: &Path, path: &str) -> Result<ResolvedTarget> {
    reject_unsafe_path(path)?;
    let worktree_canon = worktree.canonicalize().map_err(|_| -> anyhow::Error {
        crate::core::error::AgentRunError::invalid(
            "worktree_not_found",
            format!("worktree does not exist: {}", worktree.display()),
        )
        .into()
    })?;
    let raw = worktree_canon.join(path);
    let link_meta = fs::symlink_metadata(&raw).map_err(|_| -> anyhow::Error {
        crate::core::error::AgentRunError::invalid(
            "path_not_found",
            format!(
                "path '{}' does not exist in worktree {}",
                path,
                worktree.display()
            ),
        )
        .into()
    })?;
    let is_link = link_meta.file_type().is_symlink();
    let canon = fs::canonicalize(&raw).ok();
    let (escaped, dotgit) = match &canon {
        Some(c) => {
            let outside = !c.starts_with(&worktree_canon);
            let dotgit = !outside
                && c.strip_prefix(&worktree_canon)
                    .map(|rel| {
                        rel.components()
                            .any(|comp| matches!(comp, Component::Normal(s) if s.to_str() == Some(".git")))
                    })
                    .unwrap_or(false);
            (outside || dotgit, dotgit)
        }
        None => (!raw.exists(), false),
    };
    Ok(ResolvedTarget {
        canon,
        link_meta,
        is_link,
        escaped,
        dotgit,
    })
}

fn bail_missing_worktree(job_id: &str, worktree: &Path) -> Result<()> {
    Err(crate::core::error::AgentRunError::invalid(
        "worktree_not_found",
        format!(
            "Worktree directory for job '{}' does not exist at {} (it may have been deleted outside agent-run or cleaned)",
            job_id,
            worktree.display()
        ),
    )
    .into())
}

struct Entry {
    /// Path relative to the listed directory (no trailing slash).
    name: String,
    ty: char,
    size: u64,
    mtime: String,
}

fn single_entry(
    path_str: &str,
    name: &str,
    ty: char,
    meta: &fs::Metadata,
) -> Result<CommandOutput> {
    let mtime = rfc3339(meta.modified()?);
    let size = meta.len();
    let entry = json!({
        "name": name,
        "type": ty.to_string(),
        "size": size,
        "mtime": mtime,
    });
    let mut output = CommandOutput::new(json!({
        "path": path_str,
        "entries": vec![entry],
    }));
    output.push_out(format!("{} {:>10}  {}  {}", ty, size, mtime, name));
    Ok(output)
}

/// Recursively lists `dir` (a canonical worktree subtree) into `out`,
/// descending at most `depth_left` levels below `dir`. Directories come
/// first, then names. Never lists `.git` and never follows symlinks.
fn list_dir(
    dir: &Path,
    rel: &str,
    depth_left: u32,
    out: &mut Vec<Entry>,
    truncated: &mut bool,
) -> Result<()> {
    if *truncated || depth_left == 0 {
        return Ok(());
    }
    let rd =
        fs::read_dir(dir).with_context(|| format!("cannot read directory {}", dir.display()))?;
    let mut children: Vec<(String, PathBuf, char, bool, u64, String)> = Vec::new();
    for entry in rd {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == ".git" {
            continue;
        }
        let ft = entry.file_type()?;
        let md = fs::symlink_metadata(entry.path())?;
        let ty = if ft.is_dir() {
            'd'
        } else if ft.is_symlink() {
            'l'
        } else {
            'f'
        };
        let mtime = rfc3339(md.modified()?);
        let rel_name = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        children.push((rel_name, entry.path(), ty, ft.is_dir(), md.len(), mtime));
    }
    children.sort_by(|a, b| match (a.3, b.3) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => a.0.cmp(&b.0),
    });
    let mut subdirs: Vec<(PathBuf, String)> = Vec::new();
    for (rel_name, full, ty, is_dir, size, mtime) in children {
        if out.len() >= MAX_LISTED_ENTRIES {
            *truncated = true;
            break;
        }
        if is_dir {
            subdirs.push((full.clone(), rel_name.clone()));
        }
        out.push(Entry {
            name: rel_name,
            ty,
            size,
            mtime,
        });
    }
    if !*truncated {
        for (full, rel_name) in subdirs {
            list_dir(&full, &rel_name, depth_left - 1, out, truncated)?;
            if *truncated {
                break;
            }
        }
    }
    Ok(())
}

/// Lists files under a path in the job's worktree (read-only).
pub fn files(config: &Config, args: &FilesArgs) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let worktree = &meta.worktree_path;
    if !worktree.exists() {
        bail_missing_worktree(&args.job_id, worktree)?;
    }
    // A git-sync repo re-init can orphan the worktree: re-attach it so
    // follow-up git operations (diff/commit) work; no-op when healthy.
    crate::core::repos::auto_repair_worktree(config, &meta);
    let path_str = match args.path.as_deref().map(str::trim) {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => ".".to_string(),
    };
    let depth = args.depth.unwrap_or(1).clamp(1, MAX_LIST_DEPTH);

    let rt = resolve_target(worktree, &path_str)?;
    if rt.is_link {
        // `files` never follows a symlink: whether it resolves inside or
        // outside the worktree, it is a single type-`l` entry -- except when
        // it points into the worktree's `.git` directory, which must not be
        // revealed at all.
        if rt.dotgit {
            bail!("`.git` is not accessible: '{}'", path_str);
        }
        return single_entry(&path_str, &path_str, 'l', &rt.link_meta);
    }
    if rt.dotgit {
        bail!("`.git` is not accessible: '{}'", path_str);
    }
    if rt.escaped {
        bail!("path escapes the worktree: '{}'", path_str);
    }
    let canon = rt.canon.expect("non-escaped target must be resolved");
    let fmeta = fs::metadata(&canon)?;
    if !fmeta.is_dir() {
        return single_entry(&path_str, &path_str, 'f', &fmeta);
    }

    let mut entries: Vec<Entry> = Vec::new();
    let mut truncated = false;
    list_dir(&canon, "", depth, &mut entries, &mut truncated)?;

    let entries_json: Vec<Value> = entries
        .iter()
        .map(|e| {
            json!({
                "name": e.name,
                "type": e.ty.to_string(),
                "size": e.size,
                "mtime": e.mtime,
            })
        })
        .collect();
    let mut output = CommandOutput::new(json!({
        "path": path_str,
        "entries": entries_json,
    }));
    for e in &entries {
        let indent = "  ".repeat(e.name.matches('/').count());
        let name = if e.ty == 'd' {
            format!("{}/", e.name)
        } else {
            e.name.clone()
        };
        output.push_out(format!(
            "{indent}{} {:>10}  {}  {}",
            e.ty, e.size, e.mtime, name
        ));
    }
    if truncated {
        output.push_out("... truncated".to_string());
    }
    Ok(output)
}

/// Parses a `--lines` spec: 1-based inclusive `A:B`, either side optional
/// (`"10:40"`, `"50:"`, `":20"`). Returns `(start, end)`.
fn parse_lines_spec(spec: &str) -> Result<(usize, Option<usize>)> {
    let parse_side = |s: &str, label: &str| -> Result<Option<u64>> {
        if s.is_empty() {
            Ok(None)
        } else {
            let v = s
                .parse::<u64>()
                .with_context(|| format!("invalid --lines {}: '{}'", label, s))?;
            if v < 1 {
                return Err(anyhow!(
                    "invalid --lines {}: must be >= 1, got {}",
                    label,
                    v
                ));
            }
            Ok(Some(v))
        }
    };
    let (a, b) = spec
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid --lines value '{}': expected 'A:B'", spec))?;
    let a = parse_side(a.trim(), "start")?;
    let b = parse_side(b.trim(), "end")?;
    if a.is_none() && b.is_none() {
        return Err(anyhow!(
            "invalid --lines value '{}': at least one side must be given",
            spec
        ));
    }
    if let (Some(s), Some(e)) = (a, b) {
        if e < s {
            return Err(anyhow!(
                "invalid --lines value '{}': end {} is before start {}",
                spec,
                e,
                s
            ));
        }
    }
    Ok((a.unwrap_or(1) as usize, b.map(|v| v as usize)))
}

/// Selects the 1-based inclusive line range `[start, end]` from `text`.
fn apply_lines(text: &str, start: usize, end: Option<usize>) -> String {
    let all: Vec<&str> = text.split('\n').collect();
    let total = all.len();
    if start > total {
        return String::new();
    }
    let end_idx = end.map(|e| e.min(total)).unwrap_or(total);
    all[start - 1..end_idx].join("\n")
}

/// Prints the contents of one file from the job's worktree (read-only).
pub fn cat(config: &Config, args: &CatArgs) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let worktree = &meta.worktree_path;
    if !worktree.exists() {
        bail_missing_worktree(&args.job_id, worktree)?;
    }
    // A git-sync repo re-init can orphan the worktree: re-attach it so
    // follow-up git operations (diff/commit) work; no-op when healthy.
    crate::core::repos::auto_repair_worktree(config, &meta);
    let path_str = args.path.trim();
    if path_str.is_empty() {
        bail!("path is empty");
    }

    let rt = resolve_target(worktree, path_str)?;
    if rt.dotgit {
        bail!("`.git` is not accessible: '{}'", path_str);
    }
    if rt.escaped {
        bail!("path escapes the worktree: '{}'", path_str);
    }
    let canon = rt.canon.expect("non-escaped target must be resolved");
    let fmeta = fs::metadata(&canon)?;
    if fmeta.is_dir() {
        bail!("'{}' is a directory; use `files` to list it", path_str);
    }
    if !fmeta.file_type().is_file() {
        bail!("'{}' is not a regular file and cannot be read", path_str);
    }
    let size = fmeta.len();
    let max_bytes = args
        .max_bytes
        .unwrap_or(DEFAULT_MAX_BYTES)
        .min(HARD_CAP_MAX_BYTES);

    // A NUL byte in the leading sniff window marks the file as binary.
    let sniff_len: u64 = BINARY_SNIFF_BYTES as u64;
    {
        let mut f =
            fs::File::open(&canon).with_context(|| format!("cannot open '{}'", path_str))?;
        let mut w = [0u8; 8192];
        let n = f
            .read(&mut w[..sniff_len as usize])
            .with_context(|| format!("cannot read '{}'", path_str))?;
        if w[..n].contains(&0) {
            let mut output = CommandOutput::new(json!({
                "path": path_str,
                "content": "",
                "truncated": false,
            }));
            output.exit_code = 1;
            output.push_err(format!("binary file ({} bytes) not shown", size));
            return Ok(output);
        }
    }

    // Never read more than the byte cap, even with `--lines` (the line
    // selection is applied to the window that was read).
    let take = size.min(max_bytes);
    let mut buf = Vec::new();
    {
        let f = fs::File::open(&canon).with_context(|| format!("cannot open '{}'", path_str))?;
        f.take(take)
            .read_to_end(&mut buf)
            .with_context(|| format!("cannot read '{}'", path_str))?;
    }
    let file_truncated = size > take;
    let text = String::from_utf8_lossy(&buf).into_owned();

    let content = if let Some(spec) = args.lines.as_deref() {
        let (start, end) = parse_lines_spec(spec)?;
        apply_lines(&text, start, end)
    } else {
        text
    };

    let truncated = file_truncated;
    let mut output = CommandOutput::new(json!({
        "path": path_str,
        "content": content,
        "truncated": truncated,
    }));
    // The client prints every line followed by '\n', so a file that ends with a newline must
    // not yield a trailing empty line (that would print one extra blank line).
    output.stdout_lines = if content.is_empty() {
        Vec::new()
    } else {
        content
            .strip_suffix('\n')
            .unwrap_or(&content)
            .split('\n')
            .map(String::from)
            .collect()
    };
    if file_truncated {
        output.push_err(format!(
            "output truncated at {} bytes (file is {} bytes)",
            take, size
        ));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    struct Fixture {
        #[allow(dead_code)]
        temp: TempDir,
        config: Config,
        job_id: String,
        worktree: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // TempDir would otherwise try to remove a non-empty directory.
            let _ = std::fs::remove_dir_all(self.temp.path());
        }
    }

    fn fixture(job_id: &str) -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let worktree = temp.path().join("worktree");
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::write(worktree.join("a.txt"), "alpha\n").unwrap();
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
            worktree_path: worktree.clone(),
            branch: "agent-run/x".to_string(),
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
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        meta.save(&config.state_root).unwrap();
        Fixture {
            temp,
            config,
            job_id: job_id.to_string(),
            worktree,
        }
    }

    fn files_args(job_id: &str, path: Option<&str>, depth: Option<u32>) -> FilesArgs {
        FilesArgs {
            job_id: job_id.to_string(),
            path: path.map(str::to_string),
            depth,
        }
    }

    fn cat_args(job_id: &str, path: &str) -> CatArgs {
        CatArgs {
            job_id: job_id.to_string(),
            path: path.to_string(),
            lines: None,
            max_bytes: None,
        }
    }

    #[test]
    fn test_files_rejects_dotdot_traversal_before_filesystem() {
        let fix = fixture("f1");
        std::fs::write(fix.temp.path().join("outside.txt"), "secret\n").unwrap();
        let err = files(
            &fix.config,
            &files_args(&fix.job_id, Some("../outside.txt"), None),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("'..'"),
            "expected a '..'-rejection, got: {err}"
        );
    }

    #[test]
    fn test_files_rejects_absolute_path() {
        let fix = fixture("f2");
        std::fs::write(fix.temp.path().join("outside.txt"), "secret\n").unwrap();
        let err = files(
            &fix.config,
            &files_args(
                &fix.job_id,
                Some(fix.temp.path().join("outside.txt").to_str().unwrap()),
                None,
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("absolute"),
            "expected an absolute-path rejection, got: {err}"
        );
    }

    #[test]
    fn test_files_rejects_git_component() {
        let fix = fixture("f3");
        std::fs::create_dir_all(fix.worktree.join(".git")).unwrap();
        std::fs::write(fix.worktree.join(".git").join("config"), "cfg\n").unwrap();
        let err = files(
            &fix.config,
            &files_args(&fix.job_id, Some(".git/config"), None),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(".git"),
            "expected a .git-component rejection, got: {err}"
        );
    }

    #[test]
    fn test_files_never_lists_git() {
        let fix = fixture("f4");
        std::fs::create_dir_all(fix.worktree.join(".git")).unwrap();
        std::fs::write(fix.worktree.join(".git").join("config"), "cfg\n").unwrap();
        let out = files(&fix.config, &files_args(&fix.job_id, None, None)).unwrap();
        let names: Vec<&str> = out.data["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        assert!(
            !names.contains(&".git"),
            ".git must not be listed: {names:?}"
        );
    }

    #[test]
    fn test_files_lists_outside_symlink_as_l() {
        let fix = fixture("f5");
        std::fs::write(fix.worktree.join("ext"), "/etc/passwd").unwrap();
        symlink("/etc/passwd", fix.worktree.join("link")).unwrap();
        let out = files(&fix.config, &files_args(&fix.job_id, Some("link"), None)).unwrap();
        let e = &out.data["entries"][0];
        assert_eq!(e["type"], "l");
        assert_eq!(out.stdout_lines.len(), 1);
        assert!(out.stdout_lines[0].contains("link"));
    }

    #[test]
    fn test_files_inside_symlink_is_fine() {
        let fix = fixture("f6");
        std::fs::create_dir_all(fix.worktree.join("sub")).unwrap();
        std::fs::write(fix.worktree.join("sub").join("real.txt"), "data\n").unwrap();
        // Relative link target (12 bytes) so the symlink's own size is stable.
        symlink("sub/real.txt", fix.worktree.join("link.txt")).unwrap();
        let out = files(
            &fix.config,
            &files_args(&fix.job_id, Some("link.txt"), None),
        )
        .unwrap();
        let e = &out.data["entries"][0];
        assert_eq!(e["type"], "l");
        assert_eq!(e["size"], 12);
    }

    #[test]
    fn test_cat_outside_symlink_errors() {
        let fix = fixture("f7");
        symlink("/etc/passwd", fix.worktree.join("link")).unwrap();
        let err = cat(&fix.config, &cat_args(&fix.job_id, "link")).unwrap_err();
        assert!(
            err.to_string().contains("path escapes the worktree"),
            "got: {err}"
        );
    }

    #[test]
    fn test_cat_directory_suggests_files() {
        let fix = fixture("f8");
        let err = cat(&fix.config, &cat_args(&fix.job_id, ".")).unwrap_err();
        assert!(err.to_string().contains("use `files`"), "got: {err}");
    }

    #[test]
    fn test_cat_binary_rejected_with_exit_code() {
        let fix = fixture("f9");
        std::fs::write(fix.worktree.join("bin.dat"), b"\x00\x01\x02").unwrap();
        let out = cat(&fix.config, &cat_args(&fix.job_id, "bin.dat")).unwrap();
        assert_eq!(out.exit_code, 1);
        assert!(
            out.stderr_lines
                .iter()
                .any(|l| l.contains("binary file (3 bytes) not shown")),
            "stderr: {:?}",
            out.stderr_lines
        );
    }

    #[test]
    fn test_cat_lines_variants() {
        let fix = fixture("fa");
        std::fs::write(fix.worktree.join("n.txt"), "1\n2\n3\n").unwrap();
        let make = |lines: Option<&str>| {
            let mut a = cat_args(&fix.job_id, "n.txt");
            a.lines = lines.map(str::to_string);
            a
        };
        assert_eq!(
            cat(&fix.config, &make(Some("1:2"))).unwrap().data["content"],
            "1\n2"
        );
        assert_eq!(
            cat(&fix.config, &make(Some("2:"))).unwrap().data["content"],
            "2\n3\n"
        );
        assert_eq!(
            cat(&fix.config, &make(Some(":2"))).unwrap().data["content"],
            "1\n2"
        );
        assert!(cat(&fix.config, &make(Some("1:2:3"))).is_err());
        assert!(cat(&fix.config, &make(Some("x:y"))).is_err());
        assert!(cat(&fix.config, &make(Some("2:1"))).is_err());
        assert!(cat(&fix.config, &make(Some(":"))).is_err());
        assert!(cat(&fix.config, &make(Some("0:2"))).is_err());
    }

    #[test]
    fn test_cat_truncates_at_max_bytes() {
        let fix = fixture("fb");
        let content = "x".repeat(20);
        std::fs::write(fix.worktree.join("big.txt"), &content).unwrap();
        let mut a = cat_args(&fix.job_id, "big.txt");
        a.max_bytes = Some(7);
        let out = cat(&fix.config, &a).unwrap();
        assert_eq!(out.data["content"], "xxxxxxx");
        assert_eq!(out.data["truncated"], true);
        let msg = out
            .stderr_lines
            .iter()
            .find(|l| l.contains("truncated"))
            .unwrap();
        assert!(
            msg.contains("at 7 bytes (file is 20 bytes)"),
            "got: {msg:?}"
        );
    }

    #[test]
    fn test_cat_stdout_lines_reproduce_file_bytes_when_printed() {
        let fix = fixture("fnl");
        std::fs::write(fix.worktree.join("nl.txt"), "a\nb\n").unwrap();
        let out = cat(&fix.config, &cat_args(&fix.job_id, "nl.txt")).unwrap();
        // The client writes each stdout line plus '\n': the printed bytes must equal the file.
        let printed: String = out.stdout_lines.iter().map(|l| format!("{l}\n")).collect();
        assert_eq!(printed, "a\nb\n");
        std::fs::write(fix.worktree.join("empty.txt"), "").unwrap();
        let out = cat(&fix.config, &cat_args(&fix.job_id, "empty.txt")).unwrap();
        assert!(out.stdout_lines.is_empty());
    }

    #[test]
    fn test_cat_returns_full_text_content() {
        // Guards the original zero-length `read(&mut buf)` bug: every cat must
        // actually return the file's bytes.
        let fix = fixture("fb0");
        std::fs::write(fix.worktree.join("hello.txt"), "hello\nworld").unwrap();
        let out = cat(&fix.config, &cat_args(&fix.job_id, "hello.txt")).unwrap();
        assert_eq!(out.data["content"], "hello\nworld");
        assert_eq!(out.data["truncated"], false);
        assert_eq!(
            out.stdout_lines,
            vec!["hello".to_string(), "world".to_string()]
        );
    }

    #[test]
    fn test_cat_lines_respects_hard_cap_on_large_file() {
        // A >HARD_CAP_MAX_BYTES file plus `--lines` must NOT make cat read the
        // whole file: the byte window is always capped at min(size, max_bytes),
        // and max_bytes is clamped to HARD_CAP_MAX_BYTES.
        let fix = fixture("fb1");
        let p = fix.worktree.join("big.txt");
        {
            let mut f = fs::File::create(&p).unwrap();
            // Fill the leading (sniff) window with non-NUL bytes so the file
            // counts as text; the rest is a sparse (zeroed) tail.
            f.write_all(b"a\nb\nc\n").unwrap();
            let pad = BINARY_SNIFF_BYTES - "a\nb\nc\n".len();
            f.write_all(&vec![b'x'; pad]).unwrap();
            f.set_len(20 * 1024 * 1024).unwrap();
        }
        let mut a = cat_args(&fix.job_id, "big.txt");
        a.lines = Some("1:2".to_string());
        // Force the cap to be the hard cap, proving --lines cannot bypass it.
        a.max_bytes = Some(100 * 1024 * 1024);
        let out = cat(&fix.config, &a).unwrap();
        assert_eq!(out.data["content"], "a\nb");
        assert_eq!(out.data["truncated"], true);
        // Only a tiny selection came back, never the full 20 MiB.
        let c = out.data["content"].as_str().unwrap();
        assert!(c.len() < 1024, "read too much: {c:?}");
        let msg = out
            .stderr_lines
            .iter()
            .find(|l| l.contains("truncated"))
            .unwrap()
            .clone();
        assert!(
            msg.contains("at 8388608 bytes (file is 20971520 bytes)"),
            "got: {msg:?}"
        );
    }

    #[test]
    fn test_cat_lines_under_cap_not_truncated() {
        // A file under the cap with --lines must report truncated=false (the
        // old code forced truncated whenever --lines was used).
        let fix = fixture("fb2");
        std::fs::write(fix.worktree.join("small.txt"), "a\nb\nc\nd\n").unwrap();
        let mut a = cat_args(&fix.job_id, "small.txt");
        a.lines = Some("1:2".to_string());
        let out = cat(&fix.config, &a).unwrap();
        assert_eq!(out.data["content"], "a\nb");
        assert_eq!(out.data["truncated"], false);
    }

    #[test]
    fn test_cat_git_symlink_rejected() {
        // A symlink named anything that resolves into .git must be rejected,
        // even though the requested path has no `.git` component.
        let fix = fixture("fb3");
        std::fs::create_dir_all(fix.worktree.join(".git")).unwrap();
        std::fs::write(fix.worktree.join(".git").join("config"), "cfg\n").unwrap();
        symlink(".git", fix.worktree.join("linkgit")).unwrap();
        let err = cat(&fix.config, &cat_args(&fix.job_id, "linkgit/config")).unwrap_err();
        assert!(err.to_string().contains(".git"), "got: {err}");
    }

    #[test]
    fn test_files_git_symlink_rejected_but_neighbors_ok() {
        let fix = fixture("fb4");
        std::fs::create_dir_all(fix.worktree.join(".git")).unwrap();
        std::fs::write(fix.worktree.join(".git").join("config"), "cfg\n").unwrap();
        symlink(".git", fix.worktree.join("linkgit")).unwrap();
        // Listing the symlinked .git errors out.
        let err = files(&fix.config, &files_args(&fix.job_id, Some("linkgit"), None)).unwrap_err();
        assert!(err.to_string().contains(".git"), "got: {err}");
        // A regular file next to it still lists and cats fine.
        let out = files(&fix.config, &files_args(&fix.job_id, Some("a.txt"), None)).unwrap();
        assert_eq!(out.data["entries"][0]["type"], "f");
        let out = cat(&fix.config, &cat_args(&fix.job_id, "a.txt")).unwrap();
        assert_eq!(out.data["content"], "alpha\n");
    }

    #[test]
    fn test_files_depth_clamp_and_recursion() {
        let fix = fixture("fc");
        std::fs::create_dir_all(fix.worktree.join("d2").join("d3")).unwrap();
        std::fs::write(fix.worktree.join("d2").join("d3").join("deep.txt"), "x").unwrap();
        // depth 1 (default): d2 shown, not its contents.
        let out = files(&fix.config, &files_args(&fix.job_id, None, None)).unwrap();
        let names: Vec<String> = out.data["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["d2".to_string(), "a.txt".to_string()]);
        // depth 40 is clamped to 4, which still covers the 3rd level here.
        let out = files(&fix.config, &files_args(&fix.job_id, None, Some(40))).unwrap();
        let names: Vec<String> = out.data["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"d2".to_string()), "{names:?}");
        assert!(names.contains(&"d2/d3".to_string()), "{names:?}");
        assert!(names.contains(&"d2/d3/deep.txt".to_string()), "{names:?}");
    }

    #[test]
    fn test_files_sorts_dirs_first_then_name() {
        let fix = fixture("fd");
        std::fs::create_dir_all(fix.worktree.join("zdir")).unwrap();
        std::fs::write(fix.worktree.join("a_file"), "a").unwrap();
        std::fs::write(fix.worktree.join("bfile.txt"), "b").unwrap();
        let out = files(&fix.config, &files_args(&fix.job_id, None, None)).unwrap();
        let names: Vec<String> = out.data["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["zdir", "a.txt", "a_file", "bfile.txt"]);
        // The trailing slash on directories is a stdout presentation detail.
        assert!(out.stdout_lines[0].contains("zdir/"));
    }

    #[test]
    fn test_files_missing_job_id_errors() {
        let fix = fixture("fe");
        let err = files(&fix.config, &files_args("nope-nope", None, None)).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("no such job"),
            "got: {err}"
        );
    }
}

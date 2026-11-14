//! `agent-run export`: pull a finished job's committed result out of the
//! server into the caller's machine (a `git format-patch` binary patch by
//! default, or a `tar` of the changed files).
//!
//! All git work happens on the server, in the job worktree; this module owns
//! that plumbing plus the typed errors. The API route returns the produced
//! bytes as a raw octet-stream with summary headers, and the remote CLI writes
//! them to a local file (the caller's cwd never needs to be a git repo).

use anyhow::{bail, Context, Result};
use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;
use std::process::Stdio;

use crate::core::args::ExportArgs;
use crate::core::commands::commit::derive_commit_summary;
use crate::core::config::Config;
use crate::core::git::{build_diff_pathspecs, commit_selected, worktree_status};
use crate::core::job::{compute_status, JobMetadata, JobStatus};

/// Default cap on an export body. Larger exports require `--allow-large`.
pub const MAX_EXPORT_BYTES: u64 = 50 * 1024 * 1024;

/// Response headers the export endpoint attaches so the client can print a
/// summary without parsing the (binary) body.
pub const EXPORT_COMMITS_HEADER: &str = "X-Agent-Run-Export-Commits";
pub const EXPORT_FILES_HEADER: &str = "X-Agent-Run-Export-Files";
pub const EXPORT_INSERTIONS_HEADER: &str = "X-Agent-Run-Export-Insertions";
pub const EXPORT_DELETIONS_HEADER: &str = "X-Agent-Run-Export-Deletions";
pub const EXPORT_DELETED_PATHS_HEADER: &str = "X-Agent-Run-Export-Deleted";
pub const EXPORT_COMMITTED_FILES_HEADER: &str = "X-Agent-Run-Export-Committed-Files";
pub const EXPORT_PROJECT_HEADER: &str = "X-Agent-Run-Export-Project";

/// The produced export body plus the summary the API server ships in response
/// headers (so the client can print a summary without parsing the body).
#[derive(Debug)]
pub struct ExportData {
    /// The raw body to stream to the client (patch text or tar bytes).
    pub bytes: Vec<u8>,
    /// The job's project name (for the client's "from your clone of <project>"
    /// hint).
    pub project: String,
    /// Commits after the base ref (within the selection).
    pub commits: u64,
    /// Files changed between the base ref and HEAD (within the selection).
    pub files: u64,
    /// Lines inserted.
    pub insertions: u64,
    /// Lines deleted.
    pub deletions: u64,
    /// How many paths the job deleted or renamed away (`D` entries plus the
    /// old side of `R` entries). For a tar export with any, they are shipped
    /// as the `.agent-run-deleted` manifest; always 0 for patch exports.
    pub deleted_paths: u64,
    /// How many files the initial commit captured (0 when nothing was
    /// committed). The client prints "Committed N file(s)" only when > 0.
    pub committed_files: u64,
}

/// Runs `git` in the worktree and returns its stdout bytes, with the
/// codebase's standard "git not found / failing git" error contexts.
///
/// `max` bounds how much of stdout is buffered in memory: at most
/// `max + 1` bytes are read. A return value longer than `max` therefore
/// means the output was cut short mid-stream and the child was killed (and
/// waited for); callers of body-producing commands feed that truncated
/// buffer to the `export_too_large` guard. `None` reads the stream in full
/// (small metadata commands, or `--allow-large` exports).
fn git_args(worktree: &Path, args: &[String], max: Option<u64>) -> Result<Vec<u8>> {
    let joined = args.join(" ");
    // Neutralise the diff drivers on the diff-family calls routed through here
    // (`diff --shortstat`, `--name-only`, `--name-status`). The two flags are
    // `diff` subcommand options, so they are spliced in right after the `diff`
    // subcommand (the first `args` entry that is not a `-c`/config argument).
    let effective: Vec<String> = if let Some(diff_pos) = diff_subcommand_index(args) {
        let mut v = args.to_vec();
        v.insert(diff_pos + 1, "--no-ext-diff".to_string());
        v.insert(diff_pos + 2, "--no-textconv".to_string());
        v
    } else {
        args.to_vec()
    };
    let mut child = crate::core::git::hardened_git()
        .arg("-C")
        .arg(worktree)
        .args(&effective)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to execute 'git {joined}' in {} (ensure git is installed and in PATH)",
                worktree.display()
            )
        })?;
    let mut stdout = child.stdout.take().expect("stdout is always piped here");
    // Register the child with the in-server zombie reaper for the whole
    // read+reap window: the reaper must not steal the git child's exit
    // status between its exit and the `wait_with_output` below (see
    // `TrackedChild`); on the rare TOCTOU ECHILD the status is recovered
    // from the reaper's stash.
    let tracked = crate::core::reaper::TrackedChild::new(child.id());
    let body = match max {
        Some(limit) => read_at_most(&mut stdout, limit as usize)?,
        // Unlimited (`--allow-large` / small metadata commands): read in full.
        None => {
            let mut body = Vec::new();
            stdout.read_to_end(&mut body)?;
            body
        }
    };
    let truncated = max.map(|m| body.len() > m as usize).unwrap_or(false);
    if truncated {
        // Stop buffering before the stream is held in memory at all: kill
        // the child and drain it (its remaining stderr/pipe buffer is
        // small; the process is already dying).
        let _ = child.kill();
    }
    let out = match child.wait_with_output() {
        Ok(out) => out,
        Err(e) => {
            let stashed = tracked.recover_echild(&e).map(|st| std::process::Output {
                status: st,
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
            match stashed {
                Some(out) => out,
                None => return Err(anyhow::anyhow!("failed to wait for 'git {joined}': {e}")),
            }
        }
    };
    if !out.status.success() && !truncated {
        bail!(
            "git {joined} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(body)
}

/// Reads at most `limit + 1` bytes from `r` and returns them. A result
/// longer than `limit` means `r` held more data than `limit` (the read was
/// cut short and more may follow); otherwise the returned bytes are the
/// whole stream.
pub fn read_at_most(r: &mut impl Read, limit: usize) -> std::io::Result<Vec<u8>> {
    let cap = limit.saturating_add(1);
    let mut out = Vec::with_capacity(cap.min(1 << 20));
    let mut chunk = [0u8; 64 * 1024];
    while out.len() < cap {
        let want = cap - out.len();
        let want = want.min(chunk.len());
        let n = r.read(&mut chunk[..want])?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&chunk[..n]);
    }
    Ok(out)
}

/// Appends the validated `-- <pathspecs>` selection to a git argument list.
fn with_selection(mut base: Vec<String>, pathspecs: &[String]) -> Vec<String> {
    if !pathspecs.is_empty() {
        base.push("--".to_string());
        base.extend(pathspecs.iter().cloned());
    }
    base
}

/// The index of the `diff` subcommand in a git argument list, if present.
///
/// The export calls build their arg list as an optional leading
/// `-c <key>=<value>` config pair followed by the subcommand and its options
/// (see the `git_args` call sites), so the subcommand is the first entry that
/// is not the `-c` flag and not the value of a preceding `-c`. Returns the
/// position of `diff` so the caller can splice the diff-only hardening flags
/// (`--no-ext-diff --no-textconv`) in right after it.
fn diff_subcommand_index(args: &[String]) -> Option<usize> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "-c" {
            // Skip the flag and its value.
            i += 2;
            continue;
        }
        if args[i] == "diff" {
            return Some(i);
        }
        // Any other non-config token means the subcommand is not `diff`.
        return None;
    }
    None
}

/// Parses a `git diff --shortstat` line into `(files, insertions, deletions)`.
/// An empty/blank shortstat (no changes) yields `(0, 0, 0)`.
fn parse_shortstat(stats: &str) -> (u64, u64, u64) {
    let mut files = 0u64;
    let mut ins = 0u64;
    let mut del = 0u64;
    let toks: Vec<&str> = stats.split_whitespace().collect();
    let mut i = 0;
    while i + 1 < toks.len() {
        let t = toks[i];
        if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
            let n = t.parse::<u64>().unwrap_or(0);
            let unit = toks[i + 1];
            if unit.contains("insertion") {
                ins = n;
            } else if unit.contains("deletion") {
                del = n;
            } else if unit.contains("file") {
                files = n;
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    (files, ins, del)
}

/// Builds the export for a job and returns the body plus summary stats.
///
/// Behavior, in order:
/// (a) refuse a running job;
/// (b) commit any uncommitted changes in the selection first when `-m` is
///     given (reusing [`commit_selected`]); without `-m` fail `export_uncommitted`;
/// (e) fail `export_empty` when there is no commit after the base ref;
/// (c) produce the body (patch or tar) restricted to the selection;
/// (d) fail `export_too_large` when the body exceeds [`MAX_EXPORT_BYTES`] and
///     `--allow-large` was not set.
pub fn run(config: &Config, args: &ExportArgs) -> Result<ExportData> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    if status == JobStatus::Running {
        let pid = meta.pid.map(|p| format!(" (PID {p})")).unwrap_or_default();
        return Err(crate::core::error::AgentRunError::invalid(
            "job_running",
            format!(
                "Cannot export job '{}': job is currently running{}. Terminate it first using 'agent-run kill {}'.",
                args.job_id, pid, args.job_id
            ),
        )
        .into());
    }

    if !meta.worktree_path.exists() {
        return Err(crate::core::error::AgentRunError::invalid(
            "worktree_not_found",
            format!(
                "Worktree directory for job '{}' does not exist at {} (it may have been deleted outside agent-run or cleaned)",
                meta.job_id,
                meta.worktree_path.display()
            ),
        )
        .into());
    }

    // Auto-repair a dangling worktree (e.g. a legacy worktree whose git-sync
    // admin dir was wiped) before validating its `.git` pointer.
    crate::core::repos::auto_repair_worktree(config, &meta);

    // Never run git in a worktree agent-run owns whose .git pointer does not
    // resolve inside the project's repos root / workspace checkout (a
    // tampered/stale pointer could redirect the export into an
    // attacker-controlled repo). External (--from-worktree) worktrees are
    // user-supplied and are not validated against our roots.
    if !meta.external_worktree {
        crate::core::git::worktree_gitdir_in_expected_roots(
            &meta.worktree_path,
            &crate::core::git::expected_worktree_roots(config, &meta.project),
        )?;
    }

    // The shared `:(literal)`/`:(exclude,glob)` pathspecs restrict every git
    // call below to the same selection (identical semantics to `diff`/`commit`).
    let pathspecs = build_diff_pathspecs(&args.only, &args.exclude)?;
    let wt = meta.worktree_path.clone();
    let base = meta.base_ref.clone();
    let range = format!("{base}..HEAD");

    // (b) uncommitted changes in the selection. `None` message means "no
    // commit was requested" -> refuse (never silently drop uncommitted work);
    // `Some(m)` commits via the shared selection path, deriving a default
    // summary when `m` is empty.
    let uncommitted = worktree_status(&wt, &args.only, &args.exclude)?;
    let committed_files = if uncommitted.is_empty() {
        0
    } else {
        match args.message.as_deref() {
            Some(message) => {
                let prompt_source = if !meta.resolved_prompt.trim().is_empty() {
                    &meta.resolved_prompt
                } else {
                    &meta.prompt
                };
                let message = if message.trim().is_empty() {
                    derive_commit_summary(prompt_source)
                } else {
                    message.trim().to_string()
                };
                // Reuse the commit-with-selection code path (the same one the
                // `commit` command uses); it also formats the message.
                let did = commit_selected(&wt, &args.job_id, &message, &args.only, &args.exclude)?;
                // Report the captured file count only when a commit was made.
                if did {
                    uncommitted.len() as u64
                } else {
                    0
                }
            }
            None => {
                return Err(crate::core::error::AgentRunError::invalid(
                    "export_uncommitted",
                    format!(
                        "Job '{}' has uncommitted changes: pass -m/--message <msg> to commit them before exporting, or run `agent-run commit {}` first",
                        args.job_id, args.job_id
                    ),
                )
                .into());
            }
        }
    };

    // (e) no commits after the base ref -> nothing to export.
    let count_raw = git_args(
        &wt,
        &with_selection(
            vec!["rev-list".into(), "--count".into(), range.clone()],
            &pathspecs,
        ),
        None,
    )?;
    let commits = String::from_utf8_lossy(&count_raw)
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    if commits == 0 {
        return Err(crate::core::error::AgentRunError::invalid(
            "export_empty",
            format!(
                "Nothing to export for job '{}': there are no commits after {} for this selection. Finish the job or commit changes first.",
                args.job_id, base
            ),
        )
        .into());
    }

    let (files, insertions, deletions) = {
        let shortstat = String::from_utf8_lossy(&git_args(
            &wt,
            &with_selection(
                vec![
                    "diff".into(),
                    "--shortstat".into(),
                    base.clone(),
                    "HEAD".into(),
                ],
                &pathspecs,
            ),
            None,
        )?)
        .to_string();
        parse_shortstat(&shortstat)
    };

    // (c) the body.
    let deleted_paths = if args.tar {
        // The tar list below (`--diff-filter=ACMR`) silently drops deletions
        // and a rename's old path, and the apply hint (`tar -xf` + `git add -A`
        // + `git commit`) cannot delete files from the caller's clone. When
        // the job deleted or renamed away anything, ship a
        // `.agent-run-deleted` manifest (one path per line) at the tar root
        // and count it in `X-Agent-Run-Export-Deleted`.
        deleted_paths(&wt, &base, &pathspecs)?
    } else {
        Vec::new()
    };
    // `None` cap = read the stream in full, which only `--allow-large`
    // permits; otherwise git's stdout is drained at most
    // `MAX_EXPORT_BYTES + 1` bytes and the child is killed.
    let cap = if args.allow_large {
        None
    } else {
        Some(MAX_EXPORT_BYTES)
    };
    let body = if args.tar {
        // `-z` + `core.quotePath=false` yields the changed paths as NUL-separated
        // raw bytes, so names with spaces, non-ASCII characters, or newlines
        // survive the round trip (without it, git C-style-quotes non-ASCII
        // names and a line-based split would mangle names containing newlines).
        let list = git_args(
            &wt,
            &with_selection(
                vec![
                    "-c".into(),
                    "core.quotePath=false".into(),
                    "diff".into(),
                    "--name-only".into(),
                    "-z".into(),
                    "--diff-filter=ACMR".into(),
                    base.clone(),
                    "HEAD".into(),
                ],
                &pathspecs,
            ),
            None,
        )?;
        let paths: Vec<String> = list
            .split(|&b| b == 0)
            .filter(|p| !p.is_empty())
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        let mut body = if paths.is_empty() {
            // A valid empty tar (two zero-filled 512-byte EOF blocks) rather
            // than `git archive ... --` (bare), which would dump the ENTIRE tree.
            vec![0u8; 10240]
        } else {
            // Prefix every path with `:(literal)` to disable pathspec magic
            // (wildcard/glob), so names such as `*` or a leading `:` are
            // matched verbatim.
            let mut archive = vec!["archive".into(), "--format=tar".into(), "HEAD".into()];
            archive.push("--".to_string());
            archive.extend(paths.into_iter().map(|p| format!(":(literal){p}")));
            git_args(&wt, &archive, cap)?
        };
        if !deleted_paths.is_empty() {
            append_tar_manifest(&mut body, &deleted_paths);
        }
        body
    } else {
        git_args(
            &wt,
            &with_selection(
                vec![
                    "format-patch".into(),
                    "--stdout".into(),
                    "--binary".into(),
                    range.clone(),
                ],
                &pathspecs,
            ),
            cap,
        )?
    };

    // (d) size guard.
    if (body.len() as u64) > MAX_EXPORT_BYTES && !args.allow_large {
        return Err(crate::core::error::AgentRunError::invalid(
            "export_too_large",
            format!(
                "Export for job '{}' is {} bytes, which exceeds the 50 MiB limit; re-run with --allow-large to export it anyway",
                args.job_id,
                body.len()
            ),
        )
        .into());
    }

    let project = meta.project.clone();
    Ok(ExportData {
        bytes: body,
        project,
        commits,
        files,
        insertions,
        deletions,
        deleted_paths: deleted_paths.len() as u64,
        committed_files,
    })
}

/// Filename of the tar manifest listing the paths a tar export cannot delete
/// (deleted files and the old side of renames); one path per line.
pub const DELETED_MANIFEST: &str = ".agent-run-deleted";

/// `git -c core.quotePath=false diff --name-status -z <base> HEAD` restricted
/// to the selection: the paths the selection deleted -- every `D` entry plus
/// the OLD side of every `R`/`C` record (a rename's new path stays, its old
/// path is what a tar of HEAD can no longer deliver). Deduplicated and sorted
/// so the manifest is deterministic.
fn deleted_paths(worktree: &Path, base: &str, pathspecs: &[String]) -> Result<Vec<String>> {
    let out = git_args(
        worktree,
        &with_selection(
            vec![
                "-c".into(),
                "core.quotePath=false".into(),
                "diff".into(),
                "--name-status".into(),
                "-z".into(),
                base.to_string(),
                "HEAD".into(),
            ],
            pathspecs,
        ),
        None,
    )?;
    let mut set: BTreeSet<String> = BTreeSet::new();
    let fields: Vec<&[u8]> = out.split(|b| *b == 0).filter(|f| !f.is_empty()).collect();
    let mut i = 0;
    while i < fields.len() {
        let first = fields[i].first().copied().unwrap_or(b' ');
        if matches!(first, b'R' | b'C') {
            // Rename/copy record: `<R|C><similarity>\0<old>\0<new>`.
            if i + 2 < fields.len() {
                set.insert(String::from_utf8_lossy(fields[i + 1]).into_owned());
                i += 3;
            } else {
                i += 1;
            }
        } else if first == b'D' && i + 1 < fields.len() {
            set.insert(String::from_utf8_lossy(fields[i + 1]).into_owned());
            i += 2;
        } else {
            i += 1;
        }
    }
    Ok(set.into_iter().collect())
}

const TAR_BLOCK: usize = 512;

/// Parses a ustar octal number field (e.g. the 11-digit size field), reading
/// while the bytes are octal digits.
fn tar_uoctal(field: &[u8]) -> usize {
    let mut value = 0usize;
    for &b in field {
        if !(b'0'..=b'7').contains(&b) {
            break;
        }
        value = value.wrapping_mul(8) + (b - b'0') as usize;
    }
    value
}

/// The offset just past the LAST member of a ustar stream, found by walking
/// the 512-byte header blocks (name + size field) and stopping at the first
/// all-zero block -- the EOF marker every writer (including `git archive`)
/// terminates with. An archive that is only EOF blocks (the empty export tar)
/// yields 0. Walking headers -- not trimming trailing zeros -- is what makes
/// this safe for members whose content itself ends in zero bytes.
fn tar_last_member_end(tar: &[u8]) -> usize {
    let mut off = 0usize;
    while off + TAR_BLOCK <= tar.len() {
        let header = &tar[off..off + TAR_BLOCK];
        if header.iter().all(|b| *b == 0) {
            break;
        }
        let size = tar_uoctal(&header[124..136]);
        off += TAR_BLOCK + size.div_ceil(TAR_BLOCK) * TAR_BLOCK;
    }
    off
}

/// Appends one regular-file member (`DELETED_MANIFEST`, mode 0644, one path
/// per line) to a ustar tar byte stream, after the last existing member (the
/// trailing zero padding/EOF blocks are discarded and re-emitted).
fn append_tar_manifest(tar: &mut Vec<u8>, paths: &[String]) {
    const TRAILER: usize = TAR_BLOCK * 20;
    let end = tar_last_member_end(tar);
    if end < tar.len() {
        tar.truncate(end);
    }
    let name = DELETED_MANIFEST.as_bytes();
    let mut content = String::new();
    for p in paths {
        content.push_str(p);
        content.push('\n');
    }
    let content = content.into_bytes();
    let mut header = [0u8; TAR_BLOCK];
    header[..name.len()].copy_from_slice(name);
    header[100..108].copy_from_slice(b"0000644\0");
    header[108..116].copy_from_slice(b"0000000\0");
    header[116..124].copy_from_slice(b"0000000\0");
    octal(&mut header[124..136], content.len());
    octal(&mut header[136..148], 0);
    header[156] = b'0'; // regular file
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    // ustar checksum: the byte sum with the 8-byte checksum field treated as
    // spaces (NUL + space as written is the same sum).
    header[148..156].fill(b' ');
    let sum: u32 = header.iter().map(|b| *b as u32).sum();
    header[148..154].copy_from_slice(format!("{sum:06o}").as_bytes());
    header[154] = b'\0';
    header[155] = b' ';
    tar.extend_from_slice(&header);
    tar.extend_from_slice(&content);
    let data_padded = content.len().div_ceil(TAR_BLOCK) * TAR_BLOCK;
    tar.resize(tar.len() + data_padded - content.len() + TRAILER, 0);
}

/// Writes `value` as an 11-digit zero-padded octal number NUL-terminated in a
/// 12-byte ustar field.
fn octal(field: &mut [u8], value: usize) {
    field[..11].copy_from_slice(format!("{value:011o}").as_bytes());
    field[11] = b'\0';
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn shortstat_multi() {
        assert_eq!(
            parse_shortstat(" 2 files changed, 3 insertions(+), 1 deletion(-)"),
            (2, 3, 1)
        );
    }

    #[test]
    fn shortstat_singular_and_deletion_only() {
        assert_eq!(
            parse_shortstat(" 1 file changed, 2 insertions(+)"),
            (1, 2, 0)
        );
        assert_eq!(parse_shortstat(" 1 file changed, 1 deletion(-)"), (1, 0, 1));
    }

    #[test]
    fn shortstat_empty_is_zero() {
        assert_eq!(parse_shortstat(""), (0, 0, 0));
        assert_eq!(parse_shortstat("   \n "), (0, 0, 0));
    }

    #[test]
    fn shortstat_large_numbers() {
        assert_eq!(
            parse_shortstat(" 250 files changed, 9000 insertions(+), 40 deletions(-)"),
            (250, 9000, 40)
        );
    }

    #[test]
    fn selection_adds_pathspecs() {
        let none = with_selection(vec!["diff".to_string()], &[]);
        let expected_none = vec!["diff".to_string()];
        assert_eq!(none, expected_none);

        let out = with_selection(
            vec!["diff".to_string()],
            &[":(literal)a".to_string(), ":(exclude,glob)b".to_string()],
        );
        let expected = vec![
            "diff".to_string(),
            "--".to_string(),
            ":(literal)a".to_string(),
            ":(exclude,glob)b".to_string(),
        ];
        assert_eq!(out, expected);
    }

    #[test]
    fn max_is_fifty_mib() {
        assert_eq!(MAX_EXPORT_BYTES, 50 * 1024 * 1024);
    }

    #[test]
    fn read_at_most_stops_at_limit_plus_one_on_endless_input() {
        // An endless reader must never be read further than limit + 1 bytes:
        // the returned slice is exactly one byte past the cap, proving the
        // caller can stop (and kill the child) without buffering more.
        let mut r = std::io::repeat(b'x');
        let out = read_at_most(&mut r, 1000).unwrap();
        assert_eq!(out.len(), 1001);
        assert!(out.iter().all(|&b| b == b'x'));
        // A bigger cap keeps the same invariant (and exercises the multi-
        // chunk path: the reader yields in bursts larger than one byte).
        let mut r = std::io::repeat(b'y');
        let out = read_at_most(&mut r, 700_000).unwrap();
        assert_eq!(out.len(), 700_001);
    }

    #[test]
    fn read_at_most_returns_whole_short_stream() {
        let mut r = b"abc".as_slice();
        assert_eq!(read_at_most(&mut r, 10).unwrap(), &b"abc"[..]);
        // Exactly at the limit: no truncation marker byte.
        let mut r = [1u8, 2, 3].as_slice();
        assert_eq!(read_at_most(&mut r, 3).unwrap(), &b"\x01\x02\x03"[..]);
        // Empty stream.
        let mut r: &[u8] = &[];
        assert!(read_at_most(&mut r, 0).unwrap().is_empty());
    }

    #[test]
    fn git_args_caps_output_and_stops_child() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        init_repo_at(&repo);
        std::fs::write(repo.join("a.txt"), "hello world\n").unwrap();
        git_out(&repo, &["add", "-A"]);
        git_out(&repo, &["commit", "-qm", "base"]);

        // A limit past the output returns the full stdout, unchanged.
        let full = git_args(&repo, &["rev-parse".into(), "HEAD".into()], Some(1024)).unwrap();
        let sha = git_out(&repo, &["rev-parse", "HEAD"]);
        assert_eq!(full, sha);

        // A limit below the output: exactly limit + 1 bytes come back, the
        // child is killed and waited for, and its (killed or broken-pipe)
        // non-success status must NOT surface as a "git failed" error.
        let cut = git_args(&repo, &["rev-parse".into(), "HEAD".into()], Some(5)).unwrap();
        assert_eq!(cut.len(), 6);
        assert_eq!(cut, &sha[..6]);
    }

    /// Builds a temporary git repo: a base commit with README.txt, then a
    /// second commit adding files whose names defeat naive parsing: a space,
    /// a non-ASCII character, a `*` (pathspec-glob magic) and a leading `:`.
    /// Returns the repo path and the SHA of the base commit.
    fn repo_with_odd_names(temp: &tempfile::TempDir) -> (std::path::PathBuf, String) {
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(&repo)
                .args(args)
                .status()
                .expect("execute git");
            assert!(status.success());
        };
        run(&["init", "-b", "main"]);
        run(&["config", "user.name", "Test"]);
        run(&["config", "user.email", "test@test.com"]);
        std::fs::write(repo.join("README.txt"), "base\n").unwrap();
        run(&["add", "README.txt"]);
        run(&["commit", "-m", "base"]);
        let base_sha = String::from_utf8(
            Command::new("git")
                .current_dir(&repo)
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("rev-parse")
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        for name in ["sp ace.txt", "naïve.txt", "star*star.txt", ":colon.txt"] {
            std::fs::write(repo.join(name), "x\n").unwrap();
            run(&["add", "."]);
        }
        run(&["commit", "-m", "odd names"]);
        (repo, base_sha)
    }

    /// Extracts the entry names from an in-memory ustar tar (512-byte
    /// headers; size is the octal field at offset 124). Pax global ('g')
    /// and extended ('x') header entries (typeflag at offset 156) are
    /// metadata, not members: their names are skipped, and every entry's
    /// data block is advanced past using its size field.
    fn tar_entry_names(bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut names = Vec::new();
        let mut off = 0usize;
        while off + 512 <= bytes.len() {
            let hdr = &bytes[off..off + 512];
            if hdr.iter().all(|&b| b == 0) {
                break;
            }
            let end = hdr.iter().position(|&b| b == 0).unwrap_or(100);
            let typeflag = hdr[156];
            if typeflag != b'g' && typeflag != b'x' {
                names.push(hdr[..end].to_vec());
            }
            // The octal size field may be space/NUL padded; collect only
            // the digit bytes so the parse cannot fail on padding.
            let mut size_text = String::new();
            for &b in &hdr[124..136] {
                match b {
                    b'0'..=b'7' => size_text.push(b as char),
                    b' ' | b'\x00' | b'\t' => {}
                    _ => break,
                }
            }
            let size = u64::from_str_radix(&size_text, 8).unwrap_or(0);
            let data_end = off + 512 + size as usize;
            off = data_end.div_ceil(512) * 512;
        }
        names
    }

    #[test]
    fn tar_export_survives_special_file_names() {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let (repo, base_sha) = repo_with_odd_names(&temp);
        let meta = JobMetadata {
            job_id: "expodd".to_string(),
            project: "proj".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: repo.clone(),
            branch: String::new(),
            base_ref: base_sha.clone(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(0),
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
            engine: None,
        };
        meta.save(&state_root).unwrap();

        let out = run(
            &Config {
                state_root: state_root.clone(),
                ..Default::default()
            },
            &ExportArgs {
                job_id: "expodd".to_string(),
                message: None,
                only: vec![],
                exclude: vec![],
                out: None,
                tar: true,
                allow_large: false,
                force: false,
            },
        )
        .unwrap();

        let mut names: Vec<String> = tar_entry_names(&out.bytes)
            .into_iter()
            .map(|n| String::from_utf8(n).unwrap())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                ":colon.txt".to_string(),
                "naïve.txt".to_string(),
                "sp ace.txt".to_string(),
                "star*star.txt".to_string(),
            ],
            "the tar must contain exactly the four committed files"
        );
        assert_eq!(out.files, 4);
    }

    fn git_out(dir: &Path, args: &[&str]) -> Vec<u8> {
        let out = Command::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .expect("execute git");
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    fn init_repo_at(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        git_out(path, &["init", "-q", "-b", "main"]);
        git_out(path, &["config", "user.name", "Test"]);
        git_out(path, &["config", "user.email", "test@test.com"]);
    }

    #[test]
    fn deleted_paths_lists_deletions_and_rename_old_sides() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_repo_at(repo);
        std::fs::write(repo.join("a.txt"), "alpha\n").unwrap();
        std::fs::write(repo.join("b.txt"), "beta beta beta beta\n").unwrap();
        git_out(repo, &["add", "-A"]);
        git_out(repo, &["commit", "-qm", "base"]);
        git_out(repo, &["rm", "a.txt"]);
        std::fs::rename(repo.join("b.txt"), repo.join("b2.txt")).unwrap();
        git_out(repo, &["add", "-A"]);
        git_out(repo, &["commit", "-qm", "delete and rename"]);

        // D entry (a.txt) plus the OLD side of the R entry (b.txt -> b2.txt).
        let got = deleted_paths(repo, "HEAD~1", &[]).unwrap();
        assert_eq!(got, vec!["a.txt".to_string(), "b.txt".to_string()]);

        // Nothing deleted between two equal-ish states -> empty.
        let none = deleted_paths(repo, "HEAD", &[]).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn deleted_manifest_roundtrip_in_real_tar() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        init_repo_at(&repo);
        std::fs::write(repo.join("keep.txt"), "keep\n").unwrap();
        git_out(&repo, &["add", "-A"]);
        git_out(&repo, &["commit", "-qm", "base"]);

        let mut tar = git_out(
            &repo,
            &["archive", "--format=tar", "HEAD", "--", "keep.txt"],
        );
        append_tar_manifest(&mut tar, &["gone.txt".to_string(), "b.txt".to_string()]);
        // The stream must still end only with zeroed EOF blocks.
        assert!(tar.len() > 10240);
        assert!(tar[tar.len() - 10240..].iter().all(|b| *b == 0));

        let out_dir = temp.path().join("extracted");
        std::fs::create_dir_all(&out_dir).unwrap();
        let mut child = Command::new("tar")
            .args(["-xf", "-", "-C"])
            .arg(&out_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn tar");
        {
            let stdin = child.stdin.as_mut().expect("tar stdin");
            use std::io::Write;
            stdin.write_all(&tar).unwrap();
        }
        let status = child.wait_with_output().expect("wait tar");
        assert!(
            status.status.success(),
            "tar -xf failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        let manifest = std::fs::read_to_string(out_dir.join(DELETED_MANIFEST)).unwrap();
        assert_eq!(manifest, "gone.txt\nb.txt\n");
        // Original members are untouched by the append.
        assert_eq!(
            std::fs::read_to_string(out_dir.join("keep.txt")).unwrap(),
            "keep\n"
        );
    }

    #[test]
    fn deleted_manifest_on_empty_export_tar() {
        let mut tar = vec![0u8; 10240];
        append_tar_manifest(&mut tar, &["gone.txt".to_string()]);

        // The single member starts at offset 0 with a valid header.
        let name = std::str::from_utf8(&tar[..100]).unwrap();
        assert_eq!(name.trim_end_matches('\0'), DELETED_MANIFEST);
        assert_eq!(&tar[100..108], b"0000644\0");
        assert_eq!(&tar[124..136], b"00000000011\0"); // 9 bytes
        assert_eq!(tar[156], b'0');
        assert_eq!(&tar[257..263], b"ustar\0");
        let stored =
            u64::from_str_radix(std::str::from_utf8(&tar[148..154]).unwrap(), 8).unwrap() as u32;
        let mut chk = tar[..512].to_vec();
        chk[148..156].fill(b' ');
        let recomputed: u32 = chk.iter().map(|b| *b as u32).sum();
        assert_eq!(stored, recomputed);
        // Header + 9-byte payload + trailing EOF blocks only.
        assert_eq!(tar.len(), 512 + 512 + 10240);
        assert!(tar[512 + 9..].iter().all(|b| *b == 0));
        assert_eq!(&tar[512..521], "gone.txt\n".as_bytes());
    }

    #[test]
    fn deleted_manifest_after_zero_ending_member() {
        // A member whose padded block is entirely zero (all-zero content)
        // merges with the EOF blocks; the trim must still end on a 512-boundary
        // just after that block and leave the content intact.
        let content = b"\0\0\0\0";
        let mut tar = Vec::new();
        let mut header = [0u8; TAR_BLOCK];
        header[..8].copy_from_slice(b"y.bin\0\0\0");
        header[100..108].copy_from_slice(b"0000644\0");
        header[108..116].copy_from_slice(b"0000000\0");
        header[116..124].copy_from_slice(b"0000000\0");
        octal(&mut header[124..136], content.len());
        octal(&mut header[136..148], 0);
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        header[148..156].fill(b' ');
        let sum: u32 = header.iter().map(|b| *b as u32).sum();
        header[148..154].copy_from_slice(format!("{sum:06o}").as_bytes());
        header[154] = 0;
        header[155] = b' ';
        tar.extend_from_slice(&header);
        tar.extend_from_slice(content);
        tar.resize(tar.len() + TAR_BLOCK - content.len() + TAR_BLOCK * 20, 0);

        append_tar_manifest(&mut tar, &["gone.txt".to_string()]);
        let out_dir = tempfile::tempdir().unwrap();
        let mut child = Command::new("tar")
            .args(["-xf", "-", "-C"])
            .arg(out_dir.path())
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn tar");
        {
            let stdin = child.stdin.as_mut().expect("tar stdin");
            use std::io::Write;
            stdin.write_all(&tar).unwrap();
        }
        let status = child.wait_with_output().expect("wait tar");
        assert!(
            status.status.success(),
            "tar -xf failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        assert_eq!(
            std::fs::read(out_dir.path().join("y.bin")).unwrap(),
            b"\0\0\0\0"
        );
        assert_eq!(
            std::fs::read_to_string(out_dir.path().join(DELETED_MANIFEST)).unwrap(),
            "gone.txt\n"
        );
    }
}

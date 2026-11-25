use anyhow::Result;
use serde_json::{json, Value};

use crate::core::args::DiffArgs;
use crate::core::config::Config;
use crate::core::git::{run_diff, worktree_status, worktree_status_vs_base, WorktreeFileStatus};
use crate::core::job::JobMetadata;
use crate::core::output::CommandOutput;

/// Shortens a git ref for display: a full (40+ char) hex SHA becomes its
/// first 7 characters; anything else (a branch name, a short SHA) is
/// returned unchanged.
pub fn short_sha(ref_: &str) -> String {
    let is_hex = !ref_.is_empty() && ref_.bytes().all(|b| b.is_ascii_hexdigit());
    if is_hex && ref_.len() > 7 {
        ref_[..7].to_string()
    } else {
        ref_.to_string()
    }
}

/// The `note` shown when the diff output is the committed fallback (there
/// were no uncommitted changes and the text is the diff against the base
/// ref). The CLI prints it to stderr so stdout stays a clean patch.
pub fn fallback_note(meta: &JobMetadata) -> String {
    format!(
        "no uncommitted changes; showing committed changes vs base {}",
        short_sha(&meta.base_ref)
    )
}

/// Computes the diff text for a job's worktree: uncommitted changes, or --
/// if there are none -- the committed diff against the job's base ref.
/// Returns the text and whether the committed fallback was used.
pub fn job_diff_text(meta: &JobMetadata) -> Result<(String, bool)> {
    let uncommitted = run_diff(&meta.worktree_path, None, &[], &[])?;
    if uncommitted.is_empty() {
        // If no uncommitted changes, check committed diff against base_ref
        Ok((
            run_diff(&meta.worktree_path, Some(&meta.base_ref), &[], &[])?,
            true,
        ))
    } else {
        Ok((uncommitted, false))
    }
}

/// Renders the per-file stat listing as aligned text columns:
/// `STATUS  SIZE  PATH`, with numeric sizes right-aligned.
pub fn format_stat_lines(files: &[WorktreeFileStatus]) -> Vec<String> {
    let mut lines = Vec::new();
    let size_width = files
        .iter()
        .map(|f| f.size.to_string().len().max(4))
        .max()
        .unwrap_or(4);
    for f in files {
        lines.push(format!(
            "{status}  {size:>w$}  {path}",
            status = f.status,
            size = f.size,
            w = size_width,
            path = f.path
        ));
    }
    lines
}

/// Serializes the per-file stat listing to the JSON wire shape
/// `{"files": [{"status", "size", "path"}, ...]}`.
pub fn files_json(files: &[WorktreeFileStatus]) -> Value {
    let items = files
        .iter()
        .map(|f| {
            json!({
                "status": f.status,
                "size": f.size,
                "path": f.path,
            })
        })
        .collect::<Vec<Value>>();
    json!({ "files": items })
}

pub fn run(config: &Config, args: &DiffArgs) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;

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

    // A git-sync repo re-init can orphan this worktree between requests:
    // re-attach it to the private repo before any git operation (no-op when
    // healthy; one line on stderr when it acts).
    crate::core::repos::auto_repair_worktree(config, &meta);

    // Never run git in a worktree agent-run owns whose .git pointer does not
    // resolve inside the project's repos root / workspace checkout (a
    // tampered/stale pointer could redirect the diff into an
    // attacker-controlled repo). External (--from-worktree) worktrees are
    // user-supplied and are not validated against our roots.
    if !meta.external_worktree {
        crate::core::git::worktree_gitdir_in_expected_roots(
            &meta.worktree_path,
            &crate::core::git::expected_worktree_roots(config, &meta.project),
        )?;
    }

    if args.stat {
        let files = if args.base {
            // A `--base` stat reports every file that differs from the base
            // commit (committed AND uncommitted), mirroring the `--base`
            // patch view. The plain stat stays uncommitted-only (vs HEAD).
            worktree_status_vs_base(
                &meta.worktree_path,
                &meta.base_ref,
                &args.only,
                &args.exclude,
            )?
        } else {
            worktree_status(&meta.worktree_path, &args.only, &args.exclude)?
        };
        // The `data["diff"]` field is a PATCH-ALIKE TEXT, so it is
        // newline-terminated exactly like the non-stat raw diff: one trailing
        // newline after the last line. An empty stat (no changed files) stays
        // the empty string -- not a bare newline -- so the JSON `diff` field
        // is well-formed even when there is nothing to report. `stdout_lines`
        // (what the human UI and the `--json`-less CLI render) is one line per
        // changed file and is unchanged by the trailing-newline rule.
        let mut text = format_stat_lines(&files).join("\n");
        if !files.is_empty() {
            text.push('\n');
        }
        let stdout_lines = text.lines().map(String::from).collect::<Vec<String>>();
        let mut output = CommandOutput::new(json!({
            "job_id": meta.job_id,
            "stat": true,
            // `diff` carries the same aligned text so the raw endpoint
            // (which prints data["diff"] verbatim) serves it unchanged.
            "diff": text,
            "files": files_json(&files)["files"],
        }));
        output.stdout_lines = stdout_lines;
        return Ok(output);
    }

    let (diff_output, fell_back) = if args.base {
        // An explicit --base diff is never the silent fallback: no note.
        (
            run_diff(
                &meta.worktree_path,
                Some(&meta.base_ref),
                &args.only,
                &args.exclude,
            )?,
            false,
        )
    } else if args.only.is_empty() && args.exclude.is_empty() {
        job_diff_text(&meta)?
    } else {
        // Selection: same fallback as the unselected path (empty uncommitted
        // diff -> the committed diff against the base ref), restricted to the
        // selection in both directions.
        let uncommitted = run_diff(&meta.worktree_path, None, &args.only, &args.exclude)?;
        if uncommitted.is_empty() {
            (
                run_diff(
                    &meta.worktree_path,
                    Some(&meta.base_ref),
                    &args.only,
                    &args.exclude,
                )?,
                true,
            )
        } else {
            (uncommitted, false)
        }
    };

    let note = if fell_back {
        Some(fallback_note(&meta))
    } else {
        None
    };

    // `diff` is a raw-text command: the diff is stored as its newline-split
    // lines so that joining them back reproduces the exact bytes
    // (an empty diff stays empty; no trailing newline is added). `note` is
    // always present (null when there is no fallback) so the wire shape is
    // stable; the raw endpoint transmits it via the DIFF_NOTE_HEADER
    // response header and the CLI prints it to stderr.
    let mut output = CommandOutput::new(json!({ "diff": diff_output, "note": note }));
    output.stdout_lines = diff_output.split('\n').map(String::from).collect();
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn test_short_sha_truncates_long_hex_only() {
        // A full 40-char hex SHA shortens to its first 7 chars.
        let sha = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(short_sha(sha), "0123456");
        // 41+ hex chars also shorten.
        assert_eq!(short_sha(&format!("{sha}8")), "0123456");
        // A short hex string (<=7) is unchanged.
        assert_eq!(short_sha("deadbeef"), "deadbee");
        assert_eq!(short_sha("abc1234"), "abc1234");
        // Non-hex refs (branch names) are unchanged.
        assert_eq!(short_sha("main"), "main");
        assert_eq!(short_sha("origin/main"), "origin/main");
        assert_eq!(short_sha(""), "");
    }

    /// Builds a temporary git repo: a base commit with README.txt, then a
    /// second commit adding one more line. Returns the repo path and the
    /// SHA of the base commit.
    fn repo_with_two_commits(temp: &tempfile::TempDir) -> (std::path::PathBuf, String) {
        // The repo lives in its own subdirectory so the agent-run state dir
        // (created at temp/state by the callers) stays OUTSIDE the worktree:
        // a state.sqlite inside the worktree would make it dirty and defeat
        // the clean/fallback assertions.
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
        std::fs::write(repo.join("README.txt"), "base\nmore\n").unwrap();
        run(&["commit", "-am", "second"]);
        (repo, base_sha)
    }

    fn diff_job_meta(
        state_root: &std::path::Path,
        job_id: &str,
        worktree: &std::path::Path,
        base_ref: &str,
    ) -> JobMetadata {
        let meta = JobMetadata {
            job_id: job_id.to_string(),
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
            worktree_path: worktree.to_path_buf(),
            branch: String::new(),
            base_ref: base_ref.to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(0),
            status: Some(crate::core::job::JobStatus::Done),
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
        meta.save(state_root).unwrap();
        meta
    }

    #[test]
    fn test_diff_uncommitted_has_no_note() {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let (repo, base_sha) = repo_with_two_commits(&temp);

        // Uncommitted change: the tracked README gains a line.
        let mut readme = std::fs::read_to_string(repo.join("README.txt")).unwrap();
        readme.push_str("dirty\n");
        std::fs::write(repo.join("README.txt"), readme).unwrap();

        diff_job_meta(&state_root, "diffu1", &repo, &base_sha);
        let out = run(
            &Config {
                state_root: state_root.clone(),
                ..Default::default()
            },
            &DiffArgs {
                job_id: "diffu1".to_string(),
                base: false,
                only: vec![],
                exclude: vec![],
                stat: false,
            },
        )
        .unwrap();
        assert_eq!(
            out.data["note"],
            Value::Null,
            "uncommitted diff has no note"
        );
        assert!(
            out.data["diff"].as_str().unwrap().contains("dirty"),
            "the uncommitted change must be in the diff: {}",
            out.data["diff"]
        );
    }

    #[test]
    fn test_diff_clean_worktree_falls_back_to_base_with_note() {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let (repo, base_sha) = repo_with_two_commits(&temp);

        // Clean worktree (HEAD == second commit): no uncommitted changes,
        // so the output is the committed diff against the base ref.
        diff_job_meta(&state_root, "diffc1", &repo, &base_sha);
        let out = run(
            &Config {
                state_root: state_root.clone(),
                ..Default::default()
            },
            &DiffArgs {
                job_id: "diffc1".to_string(),
                base: false,
                only: vec![],
                exclude: vec![],
                stat: false,
            },
        )
        .unwrap();
        let expected_note = format!(
            "no uncommitted changes; showing committed changes vs base {}",
            short_sha(&base_sha)
        );
        assert_eq!(
            out.data["note"].as_str(),
            Some(expected_note.as_str()),
            "fallback must carry the note with the short base sha: {}",
            out.data
        );
        assert!(
            out.data["diff"].as_str().unwrap().contains("more"),
            "the committed change must be in the diff: {}",
            out.data["diff"]
        );

        // An explicit --base never claims to be a fallback: no note.
        let out = run(
            &Config {
                state_root,
                ..Default::default()
            },
            &DiffArgs {
                job_id: "diffc1".to_string(),
                base: true,
                only: vec![],
                exclude: vec![],
                stat: false,
            },
        )
        .unwrap();
        assert_eq!(out.data["note"], Value::Null, "--base is explicit: no note");
    }

    #[test]
    fn test_diff_stat_base_lists_committed_change_plain_stat_stays_empty() {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_root).unwrap();
        // Clean worktree (HEAD == "second" commit); the only delta versus the
        // base ref is the COMMITTED "second" README change.
        let (repo, base_sha) = repo_with_two_commits(&temp);
        diff_job_meta(&state_root, "diffsb1", &repo, &base_sha);
        let cfg = Config {
            state_root: state_root.clone(),
            ..Default::default()
        };

        // Plain --stat is uncommitted-only: a clean worktree reports nothing.
        let plain = run(
            &cfg,
            &DiffArgs {
                job_id: "diffsb1".to_string(),
                base: false,
                only: vec![],
                exclude: vec![],
                stat: true,
            },
        )
        .unwrap();
        assert_eq!(
            plain.data["diff"],
            Value::String(String::new()),
            "plain stat of a clean worktree is the empty text: {}",
            plain.data
        );
        assert_eq!(plain.data["files"], json!([]), "no files: {}", plain.data);

        // --base --stat DOES see the committed change (status M, non-zero size).
        let base = run(
            &cfg,
            &DiffArgs {
                job_id: "diffsb1".to_string(),
                base: true,
                only: vec![],
                exclude: vec![],
                stat: true,
            },
        )
        .unwrap();
        let files = base.data["files"].as_array().unwrap();
        assert_eq!(
            files.len(),
            1,
            "one file differs from the base: {}",
            base.data
        );
        assert_eq!(files[0]["path"], "README.txt");
        assert_eq!(files[0]["status"], "M");
        assert_ne!(
            files[0]["size"], 0,
            "modified file keeps its size: {}",
            base.data
        );
        assert!(
            base.data["diff"].as_str().unwrap().contains("README.txt"),
            "the aligned text names the file: {}",
            base.data["diff"]
        );
    }

    #[test]
    fn test_diff_stat_text_is_newline_terminated_and_empty_when_no_files() {
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("state");
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let (repo, base_sha) = repo_with_two_commits(&temp);
        diff_job_meta(&state_root, "diffsn1", &repo, &base_sha);
        let cfg = Config {
            state_root: state_root.clone(),
            ..Default::default()
        };

        // Clean worktree -> empty stat: the data["diff"] text is the EMPTY
        // string (not a bare newline) and there are no stdout lines.
        let empty = run(
            &cfg,
            &DiffArgs {
                job_id: "diffsn1".to_string(),
                base: false,
                only: vec![],
                exclude: vec![],
                stat: true,
            },
        )
        .unwrap();
        assert_eq!(empty.data["diff"], Value::String(String::new()));
        assert!(empty.stdout_lines.is_empty());
        assert_eq!(empty.data["files"], json!([]));

        // One uncommitted change -> data["diff"] ends with EXACTLY one '\n'.
        let mut readme = std::fs::read_to_string(repo.join("README.txt")).unwrap();
        readme.push_str("dirty\n");
        std::fs::write(repo.join("README.txt"), readme).unwrap();
        let out = run(
            &cfg,
            &DiffArgs {
                job_id: "diffsn1".to_string(),
                base: false,
                only: vec![],
                exclude: vec![],
                stat: true,
            },
        )
        .unwrap();
        let diff = out.data["diff"].as_str().unwrap().to_string();
        assert!(
            diff.ends_with('\n'),
            "stat text must be newline-terminated: {diff:?}"
        );
        assert!(
            !diff.ends_with("\n\n"),
            "a single trailing newline only: {diff:?}"
        );
        let lines: Vec<&str> = diff.lines().collect();
        assert_eq!(lines.len(), 1, "exactly one stat line: {diff:?}");
        assert!(
            lines[0].starts_with("M"),
            "modified file is an M entry: {lines:?}"
        );
        assert!(
            lines[0].ends_with("README.txt"),
            "names the file: {lines:?}"
        );
        // The trailing newline is the ONLY byte past the last line.
        assert_eq!(diff.len(), lines[0].len() + 1);
    }
}

#[cfg(test)]
mod existing_tests {
    use super::*;

    #[test]
    fn test_format_stat_lines_alignment() {
        let mut alpha = WorktreeFileStatus::from_raw_path("A", b"alpha.txt");
        alpha.size = 5;
        let mut beta = WorktreeFileStatus::from_raw_path("M", b"beta.txt");
        beta.size = 1234;
        let files = vec![
            alpha,
            beta,
            WorktreeFileStatus::from_raw_path("D", b"gamma.txt"),
        ];
        let lines = format_stat_lines(&files);
        assert_eq!(lines[0], "A     5  alpha.txt");
        assert_eq!(lines[1], "M  1234  beta.txt");
        assert_eq!(lines[2], "D     0  gamma.txt");

        // Empty input stays empty.
        assert!(format_stat_lines(&[]).is_empty());

        // All-zero sizes still keep the SIZE header width (4).
        let zero = vec![WorktreeFileStatus::from_raw_path("D", b"x")];
        assert_eq!(format_stat_lines(&zero)[0], "D     0  x");
    }

    #[test]
    fn test_files_json_shape() {
        let mut alpha = WorktreeFileStatus::from_raw_path("A", b"alpha.txt");
        alpha.size = 5;
        let files = vec![alpha];
        assert_eq!(
            files_json(&files),
            json!({
                "files": [
                    { "status": "A", "size": 5, "path": "alpha.txt" }
                ]
            })
        );
        assert_eq!(files_json(&[]), json!({ "files": [] }));
    }
}

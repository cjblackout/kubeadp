//! `projects` -- read-only introspection of the project checkouts that
//! `start <PROJECT>` accepts: every direct entry of the workspace root that
//! is a git repository, with per-repo git freshness and jobs-database
//! counters.
//!
//! The command never fails because one field cannot be read: each `git`
//! call runs with a short hard timeout, and any spawn failure, timeout, or
//! non-zero exit is reported as `unknown` for that field only.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;

use crate::core::args::ProjectsArgs;
use crate::core::config::Config;
use crate::core::job::{compute_status, list_jobs, JobStatus};
use crate::core::output::CommandOutput;

/// Hard cap for one `git` lookup. The commands issue only local operations
/// (`rev-parse`, `log -1`), so a few seconds is a wide margin over any
/// healthy run while still bounding a hung git (network remote, NFS stall).
const GIT_TIMEOUT: Duration = Duration::from_secs(4);

const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Placeholder reported when a field cannot be determined.
const UNKNOWN: &str = "unknown";

const SHALLOW_NOTE: &str =
    "shallow clones: only the checked-out branch exists; --ref <other-branch> will not resolve";

/// Runs `git <args...>` in `dir` and returns the trimmed stdout when the
/// process exits zero within `GIT_TIMEOUT`. Returns `None` on spawn
/// failure, timeout, or non-zero exit -- the caller reports `unknown` for
/// the affected field and never fails the whole command.
fn git_stdout(dir: &Path, args: &[&str]) -> Option<String> {
    let mut child = Command::new("git")
        .current_dir(dir)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Register the child so the in-server zombie reaper (PID 1) can never
    // steal this git helper's exit status between the `try_wait()` polls; on
    // the rare TOCTOU ECHILD the status is handed back from the reaper's
    // stash and we use its real success flag instead of treating it as a failure.
    let tracked = crate::core::reaper::TrackedChild::new(child.id());
    let deadline = Instant::now() + GIT_TIMEOUT;
    let success = loop {
        // Recover an ECHILD (the reaper won the TOCTOU race) into the child's
        // real status; otherwise keep the original graceful degradation.
        let poll_status = match child.try_wait() {
            Ok(status) => status,
            Err(err) => match tracked.recover_echild(&err) {
                Some(status) => Some(status),
                None => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            },
        };
        if let Some(status) = poll_status {
            break status.success();
        } else {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    };
    if !success {
        return None;
    }
    let mut buf = Vec::new();
    let mut handle = child.stdout.take()?;
    handle.read_to_end(&mut buf).ok()?;
    Some(String::from_utf8_lossy(&buf).trim().to_string())
}

/// Checkout branch: the symbolic ref HEAD points at, `(detached)` when HEAD
/// itself is the result of `rev-parse --abbrev-ref HEAD`, `unknown` when the
/// lookup failed (e.g. a repo with no commits yet).
fn branch_name(dir: &Path) -> String {
    match git_stdout(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).as_deref() {
        Some("HEAD") => "(detached)".to_string(),
        Some(name) if !name.is_empty() => name.to_string(),
        _ => UNKNOWN.to_string(),
    }
}

/// `(short sha, committer date ISO, subject)` from `git log -1`, or
/// `unknown`/empty/empty when the lookup failed. The subject is the last
/// `|`-split piece so subjects containing `|` survive.
fn head_info(dir: &Path) -> (String, String, String) {
    let line = git_stdout(dir, &["log", "-1", "--format=%h|%cI|%s"]);
    let fields: Option<Vec<String>> = line
        .as_deref()
        .map(|l| -> Vec<String> { l.splitn(3, '|').map(str::to_string).collect() })
        .filter(|f| f.len() == 3);
    match fields {
        Some(f) => (f[0].clone(), f[1].clone(), f[2].clone()),
        None => (UNKNOWN.to_string(), String::new(), String::new()),
    }
}

fn is_shallow(dir: &Path) -> bool {
    git_stdout(dir, &["rev-parse", "--is-shallow-repository"]).as_deref() == Some("true")
}

/// mtime of the canonicalized checkout directory (git-sync replaces the
/// directory on each sync, so the mtime tracks freshness), formatted as a
/// UTC wall-clock string. `unknown` when it cannot be determined.
fn checked_out_at(dir: &Path) -> String {
    let mtime = fs::canonicalize(dir)
        .ok()
        .and_then(|canon| fs::metadata(&canon).and_then(|m| m.modified()).ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok());
    mtime
        .and_then(|d| {
            chrono::DateTime::<chrono::Utc>::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
        })
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| UNKNOWN.to_string())
}

struct ProjectRow {
    name: String,
    branch: String,
    head: String,
    head_date: String,
    head_subject: String,
    shallow: bool,
    checked_out_at: String,
    jobs_running: u64,
    jobs_total: u64,
}

/// `(running, total)` job counts per project from the jobs database.
fn job_counters(config: &Config) -> Result<HashMap<String, (u64, u64)>> {
    let mut counters: HashMap<String, (u64, u64)> = HashMap::new();
    for meta in list_jobs(&config.state_root, None)? {
        let running = compute_status(&meta, &config.state_root) == JobStatus::Running;
        let entry = counters.entry(meta.project.clone()).or_insert((0, 0));
        entry.1 += 1;
        if running {
            entry.0 += 1;
        }
    }
    Ok(counters)
}

pub fn run(config: &Config, _args: &ProjectsArgs) -> Result<CommandOutput> {
    let counters = job_counters(config)?;

    // Direct entries of the workspace root only: symlinks are followed
    // (`is_dir`/`exists` stat through them), dot-names are skipped, and a
    // project is a directory containing `.git` (dir or file).
    let mut rows: Vec<ProjectRow> = Vec::new();
    if let Ok(entries) = fs::read_dir(&config.workspace_root) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let dir = entry.path();
            if !dir.is_dir() || !dir.join(".git").exists() {
                continue;
            }
            let (head, head_date, head_subject) = head_info(&dir);
            let (jobs_running, jobs_total) = counters.get(&name).copied().unwrap_or((0, 0));
            rows.push(ProjectRow {
                name,
                branch: branch_name(&dir),
                head,
                head_date,
                head_subject,
                shallow: is_shallow(&dir),
                checked_out_at: checked_out_at(&dir),
                jobs_running,
                jobs_total,
            });
        }
    }
    rows.sort_by(|a, b| a.name.cmp(&b.name));

    let mut output = CommandOutput::new(json!({ "projects": [] }));
    if rows.is_empty() {
        output.push_out(format!(
            "No projects found under {}",
            config.workspace_root.display()
        ));
        return Ok(output);
    }

    let name_w = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0)
        .max(7);
    let branch_w = rows
        .iter()
        .map(|r| r.branch.chars().count())
        .max()
        .unwrap_or(0)
        .max(6);
    let head_w = rows
        .iter()
        .map(|r| r.head.chars().count())
        .max()
        .unwrap_or(0)
        .max(4);

    output.push_out(format!(
        "{:<nw$} {:<bw$} {:<hw$} {:<19} JOBS",
        "PROJECT",
        "BRANCH",
        "HEAD",
        "LAST SYNC",
        nw = name_w,
        bw = branch_w,
        hw = head_w
    ));
    for r in &rows {
        let synced = if r.head_date.is_empty() {
            UNKNOWN.to_string()
        } else {
            r.head_date.clone()
        };
        output.push_out(format!(
            "{:<nw$} {:<bw$} {:<hw$} {:<19} {}/{}",
            r.name,
            r.branch,
            r.head,
            synced,
            r.jobs_running,
            r.jobs_total,
            nw = name_w,
            bw = branch_w,
            hw = head_w
        ));
    }
    if rows.iter().any(|r| r.shallow) {
        output.push_out(SHALLOW_NOTE.to_string());
    }

    let entries_json: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            json!({
                "name": r.name,
                "branch": r.branch,
                "head": r.head,
                "head_date": r.head_date,
                "head_subject": r.head_subject,
                "shallow": r.shallow,
                "checked_out_at": r.checked_out_at,
                "jobs_running": r.jobs_running,
                "jobs_total": r.jobs_total,
            })
        })
        .collect();
    output.data["projects"] = json!(entries_json);

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{ensure_state_store, JobMetadata};
    use std::path::PathBuf;

    /// Initializes an empty git repo with one initial commit (same
    /// convention as `init_fixture_repo` in start.rs).
    fn init_repo(project_dir: &Path) {
        fs::create_dir_all(project_dir).unwrap();
        let git = |args: &[&str]| {
            let status = Command::new("git")
                .current_dir(project_dir)
                .args(args)
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?} failed");
        };
        git(&["init"]);
        git(&["config", "user.name", "Test"]);
        git(&["config", "user.email", "test@test.com"]);
        fs::write(project_dir.join("repo.txt"), "hello").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "init"]);
    }

    fn save_job(state_root: &Path, job_id: &str, project: &str) {
        let meta = JobMetadata {
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
            created_at: chrono::Utc::now(),
            worktree_path: PathBuf::from("/tmp/missing-wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
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
        };
        meta.save(state_root).unwrap();
    }

    fn make_config(temp: &tempfile::TempDir, workspace: &Path) -> Config {
        let state_dir = temp.path().join("state");
        ensure_state_store(&state_dir).unwrap();
        Config {
            repos_root: workspace.to_path_buf().join(".agent-repos"),
            workspace_root: workspace.to_path_buf(),
            state_root: state_dir,
            ..Default::default()
        }
    }

    fn entry<'a>(projects: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
        projects
            .iter()
            .find(|v| v["name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("no project entry named '{name}' in {:?}", projects))
    }

    #[test]
    fn test_projects_lists_repos_with_freshness_and_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();

        // Two real repos, one symlink to a repo, one plain directory, one
        // dot-directory containing a repo, one bare file. Only the repos
        // (and the symlink to one) are projects.
        init_repo(&workspace.join("alpha"));
        init_repo(&workspace.join("beta"));
        fs::create_dir_all(workspace.join(".hidden")).unwrap();
        init_repo(&workspace.join(".hidden").join("inner"));
        fs::create_dir_all(workspace.join("plain")).unwrap();
        fs::write(workspace.join("notes.txt"), "not a dir").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(workspace.join("beta"), workspace.join("beta-link")).unwrap();

        // Two jobs for alpha (no live PID -> none of them Running), none
        // for beta, one for a project that is not a checkout.
        let config = make_config(&temp, &workspace);
        save_job(&config.state_root, "prjjob01", "alpha");
        save_job(&config.state_root, "prjjob02", "alpha");
        save_job(&config.state_root, "prjjob03", "ghost");

        let out = run(&config, &ProjectsArgs {}).unwrap();

        let projects = out.data["projects"]
            .as_array()
            .unwrap_or_else(|| panic!("payload must carry a projects array, got {:?}", out.data));
        let names: Vec<&str> = projects
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            ["alpha", "beta", "beta-link"],
            "only dirs (or symlinks to dirs) containing .git are projects: {names:?}"
        );

        let alpha = entry(projects, "alpha");
        // Real commit made by init_repo: short sha, ISO committer date, subject "init".
        let head = alpha["head"].as_str().unwrap();
        assert!(
            head.len() >= 7 && !head.contains(' '),
            "short sha, got '{head}'"
        );
        assert_eq!(alpha["head_subject"].as_str().unwrap(), "init");
        assert!(
            alpha["head_date"].as_str().unwrap().contains('T'),
            "committer date must be ISO 8601, got {:?}",
            alpha["head_date"]
        );
        // Committed repo: branch is a real symbolic ref, not unknown.
        assert!(
            alpha["branch"].as_str().unwrap() != UNKNOWN,
            "branch of a committed repo must be resolved"
        );
        assert!(
            !alpha["shallow"].as_bool().unwrap(),
            "plain repos are not shallow"
        );
        assert_ne!(
            alpha["checked_out_at"].as_str().unwrap(),
            UNKNOWN,
            "checkout dir mtime must be resolvable"
        );
        assert_eq!(alpha["jobs_total"].as_u64().unwrap(), 2);
        assert_eq!(alpha["jobs_running"].as_u64().unwrap(), 0);

        let beta = entry(projects, "beta");
        assert_eq!(beta["jobs_total"].as_u64().unwrap(), 0);
        assert_eq!(beta["jobs_running"].as_u64().unwrap(), 0);

        // Human table: aligned header, then one row per project. No shallow
        // repos here, so no note line.
        let table = out.stdout_lines.join("\n");
        assert!(
            out.stdout_lines[0].starts_with("PROJECT"),
            "header line, got: {}",
            out.stdout_lines[0]
        );
        assert!(
            out.stdout_lines[0].contains("BRANCH")
                && out.stdout_lines[0].contains("HEAD")
                && out.stdout_lines[0].ends_with("JOBS")
        );
        assert!(
            table.contains("alpha") && table.contains("beta"),
            "table rows, got:\n{table}"
        );
        assert!(
            !table.contains("plain") && !table.contains("inner"),
            "non-repo and dot-dir entries must not appear:\n{table}"
        );
        assert!(
            !table.contains("shallow clones"),
            "no note without shallow repos"
        );
        assert_eq!(out.exit_code, 0);
    }

    #[test]
    fn test_projects_empty_workspace_reports_notice() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let config = make_config(&temp, &workspace);

        let out = run(&config, &ProjectsArgs {}).unwrap();
        assert_eq!(
            out.stdout_lines,
            [format!("No projects found under {}", workspace.display())],
            "exactly one notice line for an empty workspace"
        );
        assert!(out.data["projects"].as_array().unwrap().is_empty());
        assert_eq!(out.exit_code, 0);
    }

    #[test]
    fn test_projects_missing_workspace_reports_notice() {
        let temp = tempfile::tempdir().unwrap();
        let non_existent = temp.path().join("nowhere");
        let config = make_config(&temp, &non_existent);

        let out = run(&config, &ProjectsArgs {}).unwrap();
        assert!(
            out.stdout_lines[0].starts_with("No projects found under"),
            "unreadable workspace is an empty list, not an error, got: {:?}",
            out.stdout_lines
        );
        assert!(out.data["projects"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_projects_detached_head_and_shallow_note() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();

        // A full repo plus a shallow clone of it: the clone must be flagged
        // shallow and trigger the note line.
        init_repo(&workspace.join("full"));
        // `file://` is required: `--depth` on a plain local path is ignored.
        let status = Command::new("git")
            .args([
                "clone",
                "--depth",
                "1",
                &format!("file://{}", workspace.join("full").display()),
                &workspace.join("shallow-one").to_string_lossy(),
            ])
            .status()
            .unwrap();
        assert!(status.success(), "shallow clone failed");

        // Detach HEAD in the full repo.
        let status = Command::new("git")
            .current_dir(workspace.join("full"))
            .args(["checkout", "--detach", "HEAD"])
            .status()
            .unwrap();
        assert!(status.success(), "detached checkout failed");

        let config = make_config(&temp, &workspace);
        let out = run(&config, &ProjectsArgs {}).unwrap();

        let projects = out.data["projects"].as_array().unwrap();
        assert_eq!(
            entry(projects, "full")["branch"].as_str().unwrap(),
            "(detached)",
            "detached HEAD is reported as (detached)"
        );
        assert!(entry(projects, "shallow-one")["shallow"].as_bool().unwrap());
        assert!(!entry(projects, "full")["shallow"].as_bool().unwrap());
        let table = out.stdout_lines.join("\n");
        assert!(
            table.contains(SHALLOW_NOTE),
            "shallow clones must trigger the note line:\n{table}"
        );
    }
}

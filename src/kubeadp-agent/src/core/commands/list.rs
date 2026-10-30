use std::collections::HashSet;

use anyhow::Result;
use serde_json::json;

use crate::core::activity;
use crate::core::args::ListArgs;
use crate::core::config::Config;
use crate::core::job::{compute_status, list_jobs, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

/// Parses the `--status` values (repeatable, each value itself
/// comma-separated) into the set of allowed statuses. Blank/empty values
/// are ignored; an unrecognized name is a typed 400 error listing the valid
/// names.
fn parse_status_filter(elements: &[String]) -> Result<Option<HashSet<JobStatus>>> {
    let mut set: HashSet<JobStatus> = HashSet::new();
    for element in elements {
        for token in element.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            let Some(status) = JobStatus::parse(token) else {
                return Err(crate::core::error::AgentRunError::invalid(
                    "invalid_status",
                    format!(
                        "unknown status '{}': valid statuses are {}",
                        token,
                        JobStatus::ALL_NAMES.join(", ")
                    ),
                )
                .into());
            };
            set.insert(status);
        }
    }
    if set.is_empty() {
        Ok(None)
    } else {
        Ok(Some(set))
    }
}

/// Compact duration for the `age`/`activity` columns: `45s`, `5m`, `2h13m`,
/// `3d4h`. Whole units only, largest two non-zero units shown.
fn short_duration(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    let (days, hours, minutes) = (secs / 86_400, secs / 3600 % 24, secs / 60 % 60);
    if days > 0 {
        return if hours > 0 {
            format!("{days}d{hours}h")
        } else {
            format!("{days}d")
        };
    }
    if hours > 0 {
        return if minutes > 0 {
            format!("{hours}h{minutes}m")
        } else {
            format!("{hours}h")
        };
    }
    format!("{minutes}m")
}

pub fn run(config: &Config, args: &ListArgs) -> Result<CommandOutput> {
    // An empty/blank --project value means "no filter" rather than
    // "match rows with project = ''" (which exists for no job and would
    // report an empty list even when jobs are present).
    let project_filter: Option<&str> = args
        .project
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty());
    let jobs = list_jobs(&config.state_root, project_filter)?;

    // Compute each job's (live) status once: it drives both the --status
    // filter and the display (including the running-job stall markers).
    let rows: Vec<(&JobMetadata, JobStatus)> = jobs
        .iter()
        .map(|job| (job, compute_status(job, &config.state_root)))
        .collect();
    let status_filter = parse_status_filter(&args.status)?;
    let rows: Vec<(&JobMetadata, JobStatus)> = match &status_filter {
        Some(set) => rows.into_iter().filter(|(_, s)| set.contains(s)).collect(),
        None => rows,
    };

    if rows.is_empty() {
        let mut output = CommandOutput::new(json!({ "jobs": [] }));
        if let Some(p) = project_filter {
            output.push_out(format!("No jobs found for project '{}'.", p));
        } else {
            output.push_out("No jobs found.".to_string());
        }
        return Ok(output);
    }

    // One shared "now" for every row, in the wall clocks `age`/`activity`
    // are measured in.
    let now = std::time::SystemTime::now();
    let now_dt = chrono::Utc::now();

    let mut job_entries = Vec::with_capacity(rows.len());
    let mut output = CommandOutput::new(json!({ "jobs": job_entries }));

    // Print table header
    output.push_out(format!(
        "{:<10} {:<16} {:<16} {:<20} {:<8} {:<10} PROMPT",
        "JOB ID", "PROJECT", "STATUS", "CREATED", "AGE", "ACTIVITY"
    ));
    output.push_out(format!(
        "{:-<10} {:-<16} {:-<16} {:-<20} {:-<8} {:-<10} {:-<35}",
        "", "", "", "", "", "", ""
    ));

    for (job, status) in &rows {
        let status = *status;
        let created_str = job.created_at.format("%Y-%m-%d %H:%M:%S").to_string();

        let prompt_source = if !job.resolved_prompt.trim().is_empty() {
            &job.resolved_prompt
        } else {
            &job.prompt
        };
        // The stored prompt carries this job's per-turn reference line
        // ("[agent-run ref <job_id>#<n>]"): strip it so the table shows the
        // user's own text (rows are identified by job id anyway).
        let prompt_source =
            crate::core::agy_conversations::strip_ref_line(prompt_source, &job.job_id);
        let one_line_prompt = prompt_source.replace(['\n', '\r', '\t'], " ");
        let truncated_prompt = if one_line_prompt.chars().count() > 45 {
            let s: String = one_line_prompt.chars().take(42).collect();
            format!("{}...", s)
        } else {
            one_line_prompt
        };

        // Compact, informational-only stall marker for running jobs
        // (bounded scan: the number of running jobs is <= max_parallel, and
        // `status`/`list` already re-stat the worktrees). Never changes any
        // job state.
        let (running_status_str, stalled) = if status == JobStatus::Running {
            // The job's `created_at` as a `SystemTime` anchor.
            let fallback = std::time::SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(
                    job.created_at.timestamp().max(0) as u64,
                ))
                .unwrap_or(std::time::UNIX_EPOCH);
            let log_path = JobMetadata::log_path(&config.state_root, &job.job_id);
            let act = activity::collect_activity(&log_path, &job.worktree_path, job.pid, fallback);
            let threshold = std::time::Duration::from_secs(config.stall_minutes * 60);
            let stall = activity::assess_stall(
                act.last_activity,
                fallback,
                act.child_count,
                now,
                threshold,
            );
            (
                if stall.possibly_stalled {
                    "running *stall?"
                } else {
                    "running"
                },
                Some((stall, act)),
            )
        } else {
            (status.as_str(), None)
        };

        // `AGE` anchor: running jobs age from creation; every other job's
        // runtime ends when its log stops changing, so the log mtime is its
        // finish anchor (a job with no log at all has no anchor -> `?`).
        // Cheap stat only -- the worktree scan above runs for running jobs
        // alone, keeping `list` bounded over many jobs.
        let log_path = JobMetadata::log_path(&config.state_root, &job.job_id);
        let age: Option<u64> = if status == JobStatus::Running {
            Some(
                now_dt
                    .signed_duration_since(job.created_at)
                    .num_seconds()
                    .max(0) as u64,
            )
        } else {
            activity::log_mtime(&log_path)
                .and_then(|m| now.duration_since(m).ok())
                .map(|d| d.as_secs())
        };
        let age_secs = age.unwrap_or(0);
        let age_str = match age {
            Some(secs) => short_duration(secs),
            None => "?".to_string(),
        };

        // `ACTIVITY`: running jobs get the same idle-since-last-activity
        // signal `status` prints (with `!` when it assesses as possibly
        // stalled); non-running jobs have no live activity, so `-`.
        let (activity_str, last_activity_secs, possibly_stalled) = match &stalled {
            Some((stall, act)) => (
                format!(
                    "{}{}",
                    short_duration(stall.idle_secs),
                    if stall.possibly_stalled { "!" } else { "" }
                ),
                act.last_activity
                    .and_then(|t| now.duration_since(t).ok())
                    .map(|d| d.as_secs()),
                stall.possibly_stalled,
            ),
            None => (String::from("-"), None, false),
        };

        output.push_out(format!(
            "{:<10} {:<16} {:<16} {:<20} {:<8} {:<10} {}",
            job.job_id,
            job.project,
            running_status_str,
            created_str,
            age_str,
            activity_str,
            truncated_prompt
        ));

        let mut entry = serde_json::to_value(job)?;
        entry["status"] = json!(status.as_str());
        // Additive keys for every job (old clients ignore them): elapsed
        // since creation, seconds since the last activity anchor (null when
        // the job is not running or has no anchor), and the stall flag
        // (informational, false for non-running jobs).
        entry["elapsed_seconds"] = json!(age_secs);
        entry["last_activity_seconds"] = json!(last_activity_secs);
        entry["possibly_stalled"] = json!(possibly_stalled);
        if let Some((stall, act)) = &stalled {
            entry["idle_secs"] = json!(stall.idle_secs);
            entry["last_activity_time"] =
                json!(act.last_activity.and_then(activity::system_time_to_rfc3339));
            entry["child_process_count"] = json!(act.child_count);
        }
        job_entries.push(entry);
    }

    output.data["jobs"] = json!(job_entries);

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{ensure_state_store, JobMetadata};

    /// Builds a `JobMetadata` row (pid None, so `compute_status` classifies
    /// it from exit code + log, never as Running) and writes the given
    /// terminal-success `agy` line to its log when `Some`.
    fn save_job(
        state_root: &std::path::Path,
        job_id: &str,
        exit_code: i32,
        log_line: Option<&str>,
    ) {
        let meta = JobMetadata {
            job_id: job_id.to_string(),
            project: "proj-a".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: std::path::PathBuf::from("/tmp/missing-wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(exit_code),
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
        if let Some(line) = log_line {
            let log_dir = JobMetadata::log_dir(state_root);
            std::fs::create_dir_all(&log_dir).unwrap();
            std::fs::write(JobMetadata::log_path(state_root, job_id), line).unwrap();
        }
    }

    fn list_config(temp: &tempfile::TempDir) -> Config {
        let state_dir = temp.path().join("state");
        ensure_state_store(&state_dir).unwrap();
        save_one_job(&state_dir);
        Config {
            state_root: state_dir,
            ..Default::default()
        }
    }

    fn save_one_job(state_root: &std::path::Path) {
        save_job(
            state_root,
            "lstjob01",
            0,
            Some(r#"{"conversation_id":"c1","status":"SUCCESS","response":"All tests passed."}"#),
        );
    }

    #[test]
    fn test_short_duration_format() {
        assert_eq!(short_duration(0), "0s");
        assert_eq!(short_duration(45), "45s");
        assert_eq!(short_duration(59), "59s");
        assert_eq!(short_duration(60), "1m");
        assert_eq!(short_duration(300), "5m");
        assert_eq!(short_duration(3540), "59m");
        assert_eq!(short_duration(3600), "1h");
        assert_eq!(short_duration(3780), "1h3m");
        assert_eq!(short_duration(7980), "2h13m");
        assert_eq!(short_duration(86_399), "23h59m");
        assert_eq!(short_duration(86_400), "1d");
        assert_eq!(short_duration(100_800), "1d4h");
        assert_eq!(short_duration(273_600), "3d4h");
    }

    #[test]
    fn test_list_treats_blank_project_filter_as_no_filter() {
        let temp = tempfile::tempdir().unwrap();
        let config = list_config(&temp);

        // `--project ""` (and blank) must list the job instead of
        // reporting "No jobs found for project ''".
        for filter in [Some(String::new()), Some("   ".to_string()), None] {
            let out = run(
                &config,
                &ListArgs {
                    project: filter,
                    status: Vec::new(),
                },
            )
            .unwrap();
            let jobs = out.data["jobs"]
                .as_array()
                .expect("payload must carry a jobs array");
            assert_eq!(
                jobs.len(),
                1,
                "a blank/absent project filter must not hide existing jobs"
            );
            assert!(
                !out.stdout_lines.join("\n").contains("No jobs found"),
                "blank/absent filter must not print the no-jobs notice"
            );
        }

        // A real (non-matching) project filter still reports no jobs.
        let out = run(
            &config,
            &ListArgs {
                project: Some("otherproj".to_string()),
                status: Vec::new(),
            },
        )
        .unwrap();
        assert!(out.data["jobs"].as_array().unwrap().is_empty());
        assert!(
            out.stdout_lines
                .join("\n")
                .contains("No jobs found for project 'otherproj'"),
            "real filter must still print the project-specific no-jobs notice"
        );
    }

    #[test]
    fn test_list_status_filter_matches_and_rejects() {
        let temp = tempfile::tempdir().unwrap();
        let config = list_config(&temp);

        // The seeded job is Done. Matching statuses show it.
        for status in [
            vec!["done".into()],
            vec!["done,failed".into()],
            vec!["done,".into()],
        ] {
            let out = run(
                &config,
                &ListArgs {
                    project: None,
                    status,
                },
            )
            .unwrap();
            assert_eq!(
                out.data["jobs"].as_array().unwrap().len(),
                1,
                "a matching --status must keep the job"
            );
        }

        // A non-matching status hides it (and ignores blank tokens).
        let out = run(
            &config,
            &ListArgs {
                project: None,
                status: vec!["running".into(), ",".into()],
            },
        )
        .unwrap();
        assert!(out.data["jobs"].as_array().unwrap().is_empty());

        // --status ANDs with --project (project match + status miss -> empty).
        let out = run(
            &config,
            &ListArgs {
                project: Some("proj-a".into()),
                status: vec!["running".into()],
            },
        )
        .unwrap();
        assert!(out.data["jobs"].as_array().unwrap().is_empty());

        // An unknown status is a typed error listing the valid names.
        let err = run(
            &config,
            &ListArgs {
                project: None,
                status: vec!["bogus".into()],
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown status 'bogus'"),
            "error must name the bad token: {msg}"
        );
        for name in JobStatus::ALL_NAMES {
            assert!(msg.contains(name), "error must list {name}: {msg}");
        }
        // The typed error classifies as Invalid (400).
        let typed = err
            .chain()
            .find_map(|e| e.downcast_ref::<crate::core::error::AgentRunError>())
            .expect("typed error in the chain");
        assert_eq!(typed.code, "invalid_status");
        assert_eq!(typed.kind.status_code(), 400);
    }

    #[test]
    fn test_list_age_activity_columns_and_json_keys() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        ensure_state_store(&state_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            ..Default::default()
        };

        // A freshly finished job (created_at and its log are both "now"):
        // AGE is a short non-empty duration, ACTIVITY is `-`.
        save_job(
            &state_dir,
            "lstjob02",
            0,
            Some(r#"{"conversation_id":"c2","status":"SUCCESS","response":"ok"}"#),
        );
        // A failed job with NO log at all: no finish anchor -> AGE `?`,
        // ACTIVITY still `-`.
        save_job(&state_dir, "lstjob03", 1, None);

        let out = run(
            &config,
            &ListArgs {
                project: None,
                status: Vec::new(),
            },
        )
        .unwrap();

        // Header keeps the old columns in order and gains AGE/ACTIVITY
        // immediately before PROMPT.
        let header = out
            .stdout_lines
            .first()
            .expect("table header is the first line")
            .clone();
        let order: [&str; 7] = [
            "JOB ID", "PROJECT", "STATUS", "CREATED", "AGE", "ACTIVITY", "PROMPT",
        ];
        let mut pos = 0usize;
        for name in order {
            let at = header
                .get(pos..)
                .and_then(|rest| rest.find(name))
                .map(|i| pos + i)
                .unwrap_or_else(|| panic!("header missing {name} in order: {header}"));
            pos = at + name.len();
        }

        let text = out.stdout_lines.join("\n");
        // A row's cells: job-id, project, status(1), created(date, time),
        // age, activity, prompt... -> AGE is field 5, ACTIVITY field 6.
        fn fields(line: &str) -> Vec<&str> {
            line.split_whitespace().collect()
        }
        let row2 = text
            .lines()
            .find(|l| l.starts_with("lstjob02"))
            .expect("fresh finished job row");
        let row3 = text
            .lines()
            .find(|l| l.starts_with("lstjob03"))
            .expect("log-less failed job row");
        let (age2, act2) = (fields(row2)[5], fields(row2)[6]);
        let (age3, act3) = (fields(row3)[5], fields(row3)[6]);
        assert!(
            age2.ends_with('s') && !age2.is_empty(),
            "AGE of a finished job is a short duration: {age2:?} row: {row2}"
        );
        assert_eq!(act2, "-", "ACTIVITY is `-` for a non-running job: {row2}");
        assert_eq!(
            age3, "?",
            "a non-running job with no log has no age anchor: {row3}"
        );
        assert_eq!(act3, "-", "ACTIVITY is `-` even with no log at all: {row3}");
        assert!(
            !row2.contains("!"),
            "no `!` suffix for a non-stalled row: {row2}"
        );

        // JSON: the additive keys exist for EVERY job; `elapsed_seconds` is
        // a number, `possibly_stalled` is always a bool (false here -- no
        // running jobs), `last_activity_seconds` is null for non-running.
        let jobs = out.data["jobs"]
            .as_array()
            .expect("payload carries a jobs array");
        assert_eq!(jobs.len(), 2);
        for job in jobs {
            assert!(
                job["elapsed_seconds"].is_u64(),
                "elapsed_seconds must be a number: {job}"
            );
            assert!(
                job["possibly_stalled"].is_boolean(),
                "possibly_stalled must be a bool: {job}"
            );
            assert_eq!(job["possibly_stalled"], json!(false));
            assert!(
                job.get("last_activity_seconds")
                    .expect("key must exist")
                    .is_null(),
                "last_activity_seconds is null for non-running jobs: {job}"
            );
        }
    }
}

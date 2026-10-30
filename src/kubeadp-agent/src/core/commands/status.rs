use anyhow::Result;
use chrono::Utc;
use serde_json::json;

use crate::core::activity;
use crate::core::args::JobIdArg;
use crate::core::config::Config;
use crate::core::job::{compute_status, extract_conversation_id_from_log, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

pub fn run(config: &Config, args: &JobIdArg) -> Result<CommandOutput> {
    let mut meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    // The runner persists the id together with the final status; a read in
    // between must not hide an id the log already holds (same fallback as retry).
    if meta.conversation_id.is_none() {
        meta.conversation_id = extract_conversation_id_from_log(&JobMetadata::log_path(
            &config.state_root,
            &meta.job_id,
        ));
    }
    let status = compute_status(&meta, &config.state_root);
    // Re-attach an orphaned worktree (git-sync repo re-init) so the
    // worktree/branch line stays meaningful; silent no-op when healthy.
    crate::core::repos::auto_repair_worktree(config, &meta);

    let now = Utc::now();
    let elapsed_duration = now.signed_duration_since(meta.created_at);
    let total_secs = elapsed_duration.num_seconds().max(0);
    let elapsed_str = format_duration_secs(total_secs as u64);

    let mut output = CommandOutput::new(json!({
        "job": serde_json::to_value(&meta)?,
        "status": status.as_str(),
    }));
    output.push_out(format!("Job ID:       {}", meta.job_id));
    output.push_out(format!("Project:      {}", meta.project));
    output.push_out(format!("Status:       {}", status));
    output.push_out(format!("Elapsed:      {}", elapsed_str));
    // Informational-only activity signal: a long `agy -p` job writes nothing
    // to its log until it exits, so "Elapsed" alone could not distinguish
    // "working" from "stalled". Never touches job state; every missing
    // input (missing log/worktree, no /proc) degrades to a n/a value.
    if status == JobStatus::Running {
        fill_running_activity(&mut output, config, &meta);
    }
    if meta.worktree_path.exists() {
        output.push_out(format!("Worktree:     {}", meta.worktree_path.display()));
    } else {
        output.push_out(format!(
            "Worktree:     {} (directory missing on disk)",
            meta.worktree_path.display()
        ));
    }
    output.push_out(format!("Branch:       {}", meta.branch));
    output.push_out(format!("Base Ref:     {}", meta.base_ref));
    if let Some(pid) = meta.pid {
        output.push_out(format!("PID:          {}", pid));
    }
    if let Some(model) = &meta.model {
        output.push_out(format!("Model:        {}", model));
    }
    if let Some(effort) = &meta.effort {
        output.push_out(format!("Effort:       {}", effort));
    }
    // Max-runtime display + additive JSON keys. Both `*_seconds` keys are
    // ALWAYS present in the status JSON (`null` when none): `max_runtime_
    // seconds` is the limit in whole seconds (null when the job has no limit),
    // `remaining_seconds` is the time left (null when the job is not running
    // or the deadline has already passed). New clients read them; old clients
    // ignore them.
    let max_runtime_seconds = meta.max_runtime.map(|limit| limit.num_seconds());
    let remaining: Option<chrono::Duration> = if status == JobStatus::Running {
        meta.max_runtime.and_then(|limit| {
            crate::core::job::max_runtime_remaining(meta.started_at, Some(limit), now)
        })
    } else {
        None
    };
    let remaining_seconds = remaining.map(|r| r.num_seconds());
    if let Some(limit) = &meta.max_runtime {
        output.push_out(format!(
            "Max runtime:  {}",
            crate::core::job::format_max_runtime(limit)
        ));
        if let Some(rem) = &remaining {
            output.push_out(format!(
                "Remaining:    {}",
                crate::core::job::format_max_runtime(rem)
            ));
        }
    }
    output.data["max_runtime_seconds"] = json!(max_runtime_seconds);
    output.data["remaining_seconds"] = json!(remaining_seconds);
    if let Some(conv_id) = &meta.conversation_id {
        output.push_out(format!("Conversation: {}", conv_id));
    }
    output.push_out(format!("Memory:       {}", memory_display(&meta)));
    if let Some(query) = &meta.hindsight_query {
        output.push_out(format!("Hindsight query:  {}", query));
    }
    output.push_out(format!("Prompt:       {}", meta.prompt));

    Ok(output)
}

/// The `Memory:` status line, matching what `start` printed for the job:
/// `off` when Hindsight was disabled for it, otherwise the recall/retain
/// modes and bank actually used (a job stored before this display exists
/// reports `off`, never a fabricated "on").
fn memory_display(meta: &JobMetadata) -> String {
    match meta.hindsight {
        Some(true) => format!(
            "recall {}, retain {}, bank {}",
            if meta.auto_recall.unwrap_or(false) {
                "on"
            } else {
                "off"
            },
            if meta.auto_retain.unwrap_or(false) {
                "on"
            } else {
                "off"
            },
            meta.hindsight_bank.as_deref().unwrap_or("none")
        ),
        _ => "off".to_string(),
    }
}

/// Running-job-only extras for [`run`]: optional JSON fields (new clients
/// read them, old clients ignore them) + human lines. Purely informational
/// -- no job state is read-modified-written anywhere on this path.
fn fill_running_activity(output: &mut CommandOutput, config: &Config, meta: &JobMetadata) {
    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    // Anchor for "idle since" when neither the log nor the worktree has any
    // mtime yet: the job's own creation time, as a `SystemTime`.
    let fallback = std::time::SystemTime::UNIX_EPOCH
        .checked_add(std::time::Duration::from_secs(
            meta.created_at.timestamp().max(0) as u64,
        ))
        .unwrap_or(std::time::UNIX_EPOCH);
    let threshold = std::time::Duration::from_secs(config.stall_minutes * 60);

    let act = activity::collect_activity(&log_path, &meta.worktree_path, meta.pid, fallback);
    let stall = activity::assess_stall(
        act.last_activity,
        fallback,
        act.child_count,
        std::time::SystemTime::now(),
        threshold,
    );
    let last_rfc = act.last_activity.and_then(activity::system_time_to_rfc3339);

    let source = act.last_activity_source.unwrap_or("unknown");
    // The worktree scan runs under a traversal budget (see activity.rs); when
    // it is cut short the newest mtime is only an approximation, so say so in
    // the source hint. The untruncated format is byte-identical to before.
    let source_hint = if act.worktree_newest_approx {
        format!("{} (approx.)", source)
    } else {
        source.to_string()
    };
    output.push_out(format!(
        "Last activity:  {} ({})",
        last_rfc.clone().unwrap_or_else(|| "unknown".to_string()),
        source_hint
    ));
    match act.child_count {
        Some(n) if n > 0 => {
            output.push_out(format!("Children:       {n} live"));
            if let Some(cmd) = &act.newest_child_cmdline {
                output.push_out(format!("  newest child: {cmd}"));
            }
        }
        // Only reachable for Some(0): the guarded arm above covers n > 0.
        Some(_) => {
            output.push_out("Children:       0 live".to_string());
        }
        None => {
            output.push_out("Children:       n/a (/proc unavailable)".to_string());
        }
    }
    if stall.possibly_stalled {
        output.push_out(format!(
            "Idle for:       {} (possibly stalled; threshold {} min)",
            format_duration_secs(stall.idle_secs),
            config.stall_minutes
        ));
    } else {
        output.push_out(format!(
            "Idle for:       {}",
            format_duration_secs(stall.idle_secs)
        ));
    }

    // Optional, additive JSON fields (backward compatible: existing clients
    // keep the old shape, these keys simply did not exist before).
    output.data["last_activity_time"] = json!(last_rfc);
    output.data["last_activity_source"] = json!(act.last_activity_source);
    output.data["worktree_entries_scanned"] = json!(act.worktree_entries_scanned);
    output.data["worktree_hit_budget"] = json!(act.worktree_hit_budget);
    output.data["worktree_newest_approx"] = json!(act.worktree_newest_approx);
    output.data["child_process_count"] = json!(act.child_count);
    output.data["newest_child_cmdline"] = json!(act.newest_child_cmdline);
    output.data["idle_secs"] = json!(stall.idle_secs);
    output.data["idle_for"] = json!(format_duration_secs(stall.idle_secs));
    output.data["possibly_stalled"] = json!(stall.possibly_stalled);
    output.data["stall_threshold_minutes"] = json!(config.stall_minutes);
}

pub fn format_duration_secs(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        let mins = secs / 60;
        let rem_secs = secs % 60;
        format!("{}m {}s", mins, rem_secs)
    } else {
        let hours = secs / 3600;
        let rem = secs % 3600;
        let mins = rem / 60;
        let rem_secs = rem % 60;
        format!("{}h {}m {}s", hours, mins, rem_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn save_job(state_root: &std::path::Path, job_id: &str) {
        crate::core::job::ensure_state_store(state_root).unwrap();
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
            worktree_path: std::path::PathBuf::from("/nonexistent"),
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

    #[test]
    fn test_status_resolves_unique_job_id_prefix() {
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_job(&config.state_root, "abc12345");

        // The full ID works...
        assert!(run(
            &config,
            &JobIdArg {
                job_id: "abc12345".to_string()
            }
        )
        .is_ok());
        // ...and so does a unique prefix of it.
        let res = run(
            &config,
            &JobIdArg {
                job_id: "abc1".to_string(),
            },
        );
        assert!(
            res.is_ok(),
            "status with a unique ID prefix should resolve: {:?}",
            res.err()
        );
    }

    #[test]
    fn test_status_rejects_ambiguous_job_id_prefix() {
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_job(&config.state_root, "abc1x1");
        save_job(&config.state_root, "abc1y2");

        let err = run(
            &config,
            &JobIdArg {
                job_id: "abc1".to_string(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("ambiguous"), "got: {}", err);
    }

    #[test]
    fn test_status_memory_line_reports_enabled_job() {
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_job(&config.state_root, "memon1");
        let mut meta = JobMetadata::load(&config.state_root, "memon1").unwrap();
        meta.hindsight = Some(true);
        meta.auto_recall = Some(true);
        meta.auto_retain = Some(true);
        meta.hindsight_bank = Some("homelab".to_string());
        meta.save(&config.state_root).unwrap();

        let out = run(
            &config,
            &JobIdArg {
                job_id: "memon1".to_string(),
            },
        )
        .expect("status should succeed");
        assert!(
            out.stdout_lines
                .contains(&"Memory:       recall on, retain on, bank homelab".to_string()),
            "got: {:?}",
            out.stdout_lines
        );
    }

    #[test]
    fn test_status_memory_line_off_for_disabled_or_unrecorded_jobs() {
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };

        // A job started with --hindsight off ...
        save_job(&config.state_root, "memoff");
        let mut meta = JobMetadata::load(&config.state_root, "memoff").unwrap();
        meta.hindsight = Some(false);
        meta.auto_recall = Some(false);
        meta.auto_retain = Some(false);
        meta.save(&config.state_root).unwrap();
        let out = run(
            &config,
            &JobIdArg {
                job_id: "memoff".to_string(),
            },
        )
        .expect("status should succeed");
        assert!(
            out.stdout_lines.contains(&"Memory:       off".to_string()),
            "got: {:?}",
            out.stdout_lines
        );

        // ... and a job that never recorded its memory choice: off, never a
        // fabricated "on".
        save_job(&config.state_root, "memuns");
        let out = run(
            &config,
            &JobIdArg {
                job_id: "memuns".to_string(),
            },
        )
        .expect("status should succeed");
        assert!(
            out.stdout_lines.contains(&"Memory:       off".to_string()),
            "got: {:?}",
            out.stdout_lines
        );
    }

    #[test]
    fn test_status_hindsight_query_line_only_when_stored() {
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };

        // A job with a stored explicit recall query: one extra line, right
        // after the Memory: line.
        save_job(&config.state_root, "hinq1");
        let mut meta = JobMetadata::load(&config.state_root, "hinq1").unwrap();
        meta.hindsight = Some(true);
        meta.hindsight_query = Some("my recall query".to_string());
        meta.save(&config.state_root).unwrap();
        let out = run(
            &config,
            &JobIdArg {
                job_id: "hinq1".to_string(),
            },
        )
        .expect("status should succeed");
        let lines = &out.stdout_lines;
        let memory_idx = lines
            .iter()
            .position(|l| l.starts_with("Memory:"))
            .expect("Memory line present");
        let query_idx = lines
            .iter()
            .position(|l| l == "Hindsight query:  my recall query");
        assert!(
            query_idx.is_some(),
            "Hindsight query line present: {lines:?}"
        );
        let query_idx = query_idx.unwrap();
        assert!(
            query_idx > memory_idx,
            "query line must come after the Memory line: {:?}",
            lines
        );
        assert_eq!(query_idx, memory_idx + 1);

        // A job without a stored query: no such line at all.
        save_job(&config.state_root, "hinq2");
        let out = run(
            &config,
            &JobIdArg {
                job_id: "hinq2".to_string(),
            },
        )
        .expect("status should succeed");
        assert!(
            !out.stdout_lines
                .iter()
                .any(|l| l.starts_with("Hindsight query")),
            "no Hindsight query line for a job without one: {:?}",
            out.stdout_lines
        );
    }
}

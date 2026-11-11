use anyhow::{bail, Context, Result};
use chrono::Utc;
use rusqlite::params;
use serde_json::json;
use std::fs::OpenOptions;
use std::io::Write;

use crate::core::agy_conversations;
use crate::core::args::{HindsightFlag, RetryArgs};
use crate::core::commands::start::check_parallel_limit;
use crate::core::config::Config;
use crate::core::hindsight::{
    append_to_log_file, execute_recall, extract_prompt_text, parse_recall_response,
    prepend_recalled_context, validate_hindsight_config, validate_hindsight_query,
};
use crate::core::job::{
    compute_status, extract_conversation_id_from_log, restore_conversation_id, JobMetadata,
    JobStatus,
};
use crate::core::output::CommandOutput;
use crate::core::runner::{launch_background_job, PROMPT_ARGV_MAX_BYTES};

pub fn run(config: &Config, args: &RetryArgs) -> Result<CommandOutput> {
    let mut meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    // A follow-up turn: continue the same conversation with a NEW prompt
    // instead of re-running the stored one. `done` jobs become retryable
    // only together with this flag.
    let follow_up = args.prompt.is_some();

    // Per-job Hindsight switch. `off` wins over the stored per-job choice and
    // every server-side default: no recall, no retain, for this retry. `on`
    // re-enables Hindsight using the server's configured recall/retain
    // defaults (same as `start --hindsight on`); an absent flag keeps the
    // stored per-job choice (or the server default for jobs with none stored).
    let (hindsight_enabled, auto_recall, auto_retain) = match args.hindsight {
        Some(HindsightFlag::Off) => (false, false, false),
        Some(HindsightFlag::On) => (
            true,
            config.hindsight_auto_recall,
            config.hindsight_auto_retain,
        ),
        None => (
            meta.hindsight.unwrap_or(config.hindsight_enabled),
            meta.auto_recall.unwrap_or(config.hindsight_auto_recall),
            meta.auto_retain.unwrap_or(config.hindsight_auto_retain),
        ),
    };

    let resolved_bank = meta
        .hindsight_bank
        .clone()
        .or_else(|| {
            if !meta.project.is_empty() {
                Some(meta.project.clone())
            } else {
                None
            }
        })
        .or_else(|| config.hindsight_bank.clone());

    validate_hindsight_config(hindsight_enabled, &resolved_bank)?;

    // Explicit recall query: reject it when this retry explicitly disables
    // Hindsight (it would never be used), and validate it (trim, non-empty,
    // bounded length) before the attempt is claimed.
    if args.hindsight == Some(HindsightFlag::Off) && args.hindsight_query.is_some() {
        return Err(crate::core::error::AgentRunError::invalid(
            "hindsight_query_without_recall",
            "--hindsight-query cannot be used with --hindsight off (recall is disabled for this job)",
        )
        .into());
    }
    let hindsight_query = validate_hindsight_query(&args.hindsight_query)?;

    // Only valid for jobs whose status is self-terminated, failed, or killed
    match status {
        JobStatus::SelfTerminated
        | JobStatus::Failed
        | JobStatus::Killed
        | JobStatus::Incomplete
        | JobStatus::QuotaExhausted => {}
        JobStatus::Running => {
            let pid_str = meta
                .pid
                .map(|p| format!(" (PID {})", p))
                .unwrap_or_default();
            return Err(crate::core::error::AgentRunError::invalid(
                "job_running",
                format!(
                    "Cannot retry job '{}': job is currently running{}. Terminate it first using 'agent-run kill {}'.",
                    args.job_id,
                    pid_str,
                    args.job_id
                ),
            )
            .into());
        }
        JobStatus::Done if !follow_up => {
            return Err(crate::core::error::AgentRunError::invalid(
                "retry_already_completed",
                format!(
                    "Cannot retry job '{}': job has already completed successfully. Start a new job instead using 'agent-run start', or use --prompt to send a follow-up.",
                    args.job_id
                ),
            )
            .into());
        }
        JobStatus::Done => {
            // A follow-up prompt continues the finished conversation as a new
            // turn instead of re-running the stored prompt.
        }
    }

    if !meta.worktree_path.exists() {
        return Err(crate::core::error::AgentRunError::invalid(
            "retry_worktree_missing",
            format!(
                "Cannot retry job '{}': worktree directory no longer exists at {} (it may have been deleted or cleaned)",
                args.job_id,
                meta.worktree_path.display()
            ),
        )
        .into());
    }

    // A git-sync repo re-init can orphan this worktree: re-attach it before
    // the new agy run does any git work in it (no-op when healthy).
    crate::core::repos::auto_repair_worktree(config, &meta);

    // Validate the follow-up prompt before any job state changes: the size
    // bound is the same one `start` enforces (an oversized prompt would make
    // execve fail with E2BIG at spawn time), and an empty prompt must not
    // reach the launcher. Refusing here keeps the job row untouched -- no
    // claim bump, no banner, no launch.
    if let Some(raw) = &args.prompt {
        if raw.trim().is_empty() {
            return Err(crate::core::error::AgentRunError::invalid(
                "prompt_empty",
                "Prompt cannot be empty",
            )
            .into());
        }
        if raw.len() > PROMPT_ARGV_MAX_BYTES {
            bail!(
                "prompt is {} bytes, longer than the {} byte limit for prompts passed to agy as an argv element (execve would fail with E2BIG); provide the prompt via a file (@path) or shorten it",
                raw.len(),
                PROMPT_ARGV_MAX_BYTES
            );
        }
    }

    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    let mut output = CommandOutput::new(json!({
        "job_id": meta.job_id,
        "pid": serde_json::Value::Null,
    }));

    // Look for conversation ID from previous log if not already recorded
    if meta.conversation_id.is_none() {
        if let Some(conv_id) = extract_conversation_id_from_log(&log_path) {
            output.push_out(format!(
                "Discovered conversation ID '{}' from previous run log.",
                conv_id
            ));
            meta.conversation_id = Some(conv_id);
        }
    }

    // No conversation ID in the row and none in the log: the final JSON line
    // never landed (agy was killed mid-run). Before refusing, search agy's
    // own conversation store for the conversation this prompt started; a
    // recovered ID is only used when exactly one candidate conversation
    // contains the prompt (never a guess).
    if meta.conversation_id.is_none() {
        // The conversation on disk was started by the ORIGINAL prompt
        // (a follow-up prompt was never sent to agy), so the marker always
        // comes from the stored prompt, not from `--prompt`.
        //
        // Jobs whose stored prompt carries this job's reference line
        // (started since per-turn tokens were introduced) recover through
        // that token's bytes -- unique per job by construction, so two jobs
        // whose prompts share their opening bytes can never cross-match.
        // Rows stored before the change carry no line and fall back to the
        // legacy 80-byte prompt prefix (the same conservative
        // single-match/claimed/created-window scan as before).
        if let Some(marker) =
            agy_conversations::recovery_marker_for(&meta.resolved_prompt, &meta.job_id)
        {
            if let Some(dir) =
                agy_conversations::effective_conversations_dir(config.agy_home.as_deref())
            {
                let claimed = agy_conversations::claimed_conversation_ids(&config.state_root);
                if let Some(conv_id) = agy_conversations::find_conversation_id(
                    &dir,
                    &marker,
                    std::time::SystemTime::from(meta.created_at),
                    &claimed,
                ) {
                    output.push_out(format!(
                        "Recovered conversation ID '{}' from agy's conversation files.",
                        conv_id
                    ));
                    // Targeted single-column UPDATE (no-op if an ID was
                    // captured concurrently); the in-memory `meta` is carried
                    // through to the launch below either way.
                    if let Err(e) =
                        restore_conversation_id(&config.state_root, &meta.job_id, &conv_id)
                    {
                        output.push_err(format!(
                            "Warning: failed to persist recovered conversation ID '{}': {}",
                            conv_id, e
                        ));
                    }
                    meta.conversation_id = Some(conv_id);
                }
            }
        }
    }

    // When no conversation ID was captured, agent-run cannot automatically resume the session:
    // bare `agy --continue` (without -p) opens an interactive session picker requiring a TTY (which
    // fails in detached background runs), and agent-run does not currently implement headless session
    // resolution without an explicit conversation ID. Refuse cleanly here rather than launching
    // a background process that cannot proceed.
    if meta.conversation_id.is_none() {
        return Err(crate::core::error::AgentRunError::invalid(
            "retry_no_conversation",
            format!(
                "Cannot retry job '{}': no conversation ID was captured from the previous run \
                 (also searched agy's conversation files), and \
                 agent-run currently requires an explicit conversation ID to resume a session headlessly. \
                 This job cannot be resumed automatically; inspect its log with 'agent-run log {}' and \
                 start a fresh job with 'agent-run start' instead.",
                args.job_id,
                args.job_id
            ),
        )
        .into());
    }

    // Captured before the in-memory bump below: the conditional claim UPDATE
    // (run after the parallel-limit check) must compare against the value
    // this command read from the row. The bump happens here (before the
    // prompt text is built) so both the size check and the reference-line
    // stamp below can use `meta.retry_count` as this attempt's turn number.
    let expected_retry_count = meta.retry_count;
    meta.retry_count += 1;

    // The text the auto-recall below queries: for a follow-up turn that is
    // the NEW prompt, otherwise the text of the LAST attempt. When the last
    // attempt was a `retry --prompt` follow-up that text lives in
    // `retry_prompt` (a plain retry after a failed follow-up must not
    // silently re-send the ORIGINAL prompt); for jobs whose attempts never
    // replaced it, `retry_prompt` is the original resolved prompt anyway.
    // `extract_prompt_text` strips any leading `<recalled-context>` block
    // left by an earlier attempt (so re-recalls do not nest), and the ref
    // line is stripped on top of that so the recall query carries the
    // user's text only.
    // Both stored texts are already `@<file>`-resolved, and the shim
    // resolves the follow-up `@<file>` before it reaches the server, so no
    // second resolution.
    let prompt_text = match &args.prompt {
        Some(follow_up) => extract_prompt_text(follow_up, true),
        None => extract_prompt_text(
            agy_conversations::strip_ref_line(
                meta.retry_prompt
                    .as_deref()
                    .unwrap_or(&meta.resolved_prompt),
                &meta.job_id,
            ),
            true,
        ),
    };
    let mut retry_prompt: Option<String> = Some(prompt_text.clone());

    if hindsight_enabled && auto_recall {
        if let Some(bank) = &resolved_bank {
            // An explicit `--hindsight-query` for this retry wins; otherwise a
            // query stored on the job (from `start`) is reused; otherwise the
            // follow-up text itself is the query (today's behaviour). The
            // prompt is left untouched in every case.
            let recall_query = hindsight_query
                .as_deref()
                .or(meta.hindsight_query.as_deref())
                .unwrap_or(&prompt_text);
            match execute_recall(
                &config.hindsight_path,
                bank,
                recall_query,
                config.hindsight_tag.as_deref(),
                config.hindsight_url.as_deref(),
                config.hindsight_api_key.as_deref(),
                config.hindsight_profile.as_deref(),
                Some(config),
                meta.proxy.as_deref(),
            ) {
                Ok(json_output) => {
                    if let Some(recalled_text) = parse_recall_response(&json_output) {
                        let prepended = prepend_recalled_context(&recalled_text, &prompt_text);
                        retry_prompt = Some(prepended);
                        append_to_log_file(
                            &log_path,
                            &format!(
                                "[hindsight] Successfully recalled context from bank '{}'.",
                                bank
                            ),
                        );
                    } else {
                        let warn_msg = format!(
                            "Hindsight recall returned no usable memory items from bank '{}'; proceeding with original prompt.",
                            bank
                        );
                        append_to_log_file(&log_path, &format!("[hindsight warning] {}", warn_msg));
                        retry_prompt = Some(prompt_text);
                    }
                }
                Err(e) => {
                    let warn_msg = format!(
                        "Hindsight recall failed: {}; proceeding with original prompt.",
                        e
                    );
                    append_to_log_file(&log_path, &format!("[hindsight warning] {}", warn_msg));
                    output.push_err(format!("Warning: {}", warn_msg));
                    retry_prompt = Some(prompt_text);
                }
            }
        }
    }

    // Re-enforce the argv size limit on the FINAL prompt: the check above ran
    // on the user's follow-up text only, but auto-recall may have prepended
    // recalled context on top of it, and the per-turn reference line is
    // appended below. The launcher refuses at spawn time, which happens
    // AFTER `claim_retry_attempt` has already marked the row running --
    // bailing here, before the claim, keeps the row untouched.
    if let Some(final_prompt) = retry_prompt.as_deref() {
        let with_ref =
            agy_conversations::normalize_ref_prompt(final_prompt, &meta.job_id, meta.retry_count);
        if with_ref.len() > PROMPT_ARGV_MAX_BYTES {
            bail!(
                "prompt is {} bytes, longer than the {} byte limit for prompts passed to agy as an argv element (execve would fail with E2BIG); provide the prompt via a file (@path) or shorten it",
                with_ref.len(),
                PROMPT_ARGV_MAX_BYTES
            );
        }
    }

    // Stamp this attempt with its own turn number so every turn the engine
    // sends is uniquely attributable (and so a plain retry of an already
    // tokenized stored prompt refreshes the line instead of stacking it).
    // `meta.retry_count` holds this attempt's number (bumped above, ahead of
    // the claim).
    if let Some(text) = &mut retry_prompt {
        *text = agy_conversations::normalize_ref_prompt(text, &meta.job_id, meta.retry_count);
    }

    meta.status = Some(JobStatus::Running);
    meta.exit_code = None;
    // Clear the previous attempt's process identity. `launch_background_job`
    // polls the row until a pid appears and returns it; with the old pid still
    // stored it returned immediately (printing the ORIGINAL pid) before the
    // new attempt had published its own, and `kill` right after `retry` could
    // target the dead old pid.
    meta.pid = None;
    meta.pid_start_time = None;
    // A retry starts a FRESH runtime clock: reset the recorded start anchor
    // for the new attempt (the inherited `max_runtime` limit, if any, is
    // measured from this moment, not from the first attempt). The launcher
    // persists the in-memory row right before spawning the monitor.
    meta.started_at = Some(Utc::now());
    meta.hindsight = Some(hindsight_enabled);
    meta.auto_recall = Some(auto_recall);
    meta.auto_retain = Some(auto_retain);
    meta.retry_prompt = retry_prompt;
    if meta.hindsight_bank.is_none() {
        meta.hindsight_bank = resolved_bank;
    }
    // An explicit `--hindsight-query` for this retry replaces the stored
    // query (which later retries reuse); without the flag the stored query,
    // if any, is kept unchanged.
    if let Some(query) = hindsight_query {
        meta.hindsight_query = Some(query);
    }

    // Guaranteed Some: the bail! above refuses the retry otherwise.
    let conv_id = meta
        .conversation_id
        .as_ref()
        .expect("conversation_id checked above");
    output.push_out(format!(
        "Retrying job '{}' in worktree {} using conversation ID '{}'...",
        args.job_id,
        meta.worktree_path.display(),
        conv_id
    ));

    // Enforce the same max-parallel limit `start` applies before
    // launching the background retry process.
    check_parallel_limit(config)?;

    // Atomically claim the next attempt now that every non-mutating check has
    // passed, so a refused retry does not bump retry_count or leave the row
    // marked running with no process behind.
    claim_retry_attempt(&config.state_root, &meta.job_id, expected_retry_count)?;

    // Append retry delimiter to log file. The prefix `====================
    // RETRY AT` is part of the status classification contract (`last_attempt_log`
    // in job.rs splits on it): it must stay, and the follow-up annotation is
    // inserted before the closing `====` run instead. Written only AFTER the
    // parallel-limit check and the attempt claim succeeded: a refused retry
    // must not leave a banner in the log for an attempt that never started
    // (it would make `last_attempt_log` classify an empty latest attempt).
    let retry_banner_suffix = args
        .prompt
        .as_ref()
        .map(|p| format!(" (follow-up prompt, {} bytes)", p.len()))
        .unwrap_or_default();
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_path) {
        let retry_banner = format!(
            "\n\n==================== RETRY AT {}{} ====================\n\n",
            Utc::now().to_rfc3339(),
            retry_banner_suffix
        );
        let _ = file.write_all(retry_banner.as_bytes());
    }

    let pid = launch_background_job(config, &mut meta)
        .context("Failed to launch background retry process")?;

    output.push_out(format!(
        "Job '{}' retry launched in background (PID {}).",
        args.job_id, pid
    ));
    output.push_out(format!(
        "Run 'agent-run status {}' or 'agent-run tail {}' to monitor.",
        args.job_id, args.job_id
    ));

    output.data = json!({ "job_id": meta.job_id, "pid": pid });
    Ok(output)
}

/// Atomically claims the next retry attempt on the job row.
///
/// Two concurrent `retry` invocations both pass the terminal-status gate from
/// the same stale read of the row; left unchecked, each would bump
/// retry_count in memory and launch a monitor thread, so two agy processes
/// run on the same worktree (the last full-row save wins). The single-row
/// conditional UPDATE -- guarded on the retry_count the caller read -- makes
/// at most one claim succeed: a competing claim bumps retry_count, so the
/// loser matches no row and aborts before launching.
///
/// The claim deliberately does NOT inspect the row's status column: a job
/// orphaned by a server restart keeps `status = 'running'` in its row even
/// though its process is dead, and `retry` has already decided (via
/// `compute_status`) that the job is retryable, so a status guard would
/// wrongly refuse exactly that row. The retry_count guard alone already
/// makes the claim atomic.
fn claim_retry_attempt(
    state_root: &std::path::Path,
    job_id: &str,
    expected_retry_count: u32,
) -> Result<()> {
    let conn = crate::core::job::open_state_db(state_root)?;
    let updated = conn
        .execute(
            "UPDATE jobs SET retry_count = retry_count + 1, status = ?2 \
              WHERE job_id = ?1 AND retry_count = ?3",
            params![
                job_id,
                JobStatus::Running.as_str().to_string(),
                expected_retry_count
            ],
        )
        .with_context(|| {
            format!(
                "Failed to claim retry attempt for job '{}' in {}",
                job_id,
                crate::core::job::state_db_path(state_root).display()
            )
        })?;
    if updated == 0 {
        let what = match JobMetadata::load(state_root, job_id) {
            Ok(c) if c.retry_count != expected_retry_count => format!(
                "another retry already claimed a newer attempt (retry_count {} -> {})",
                expected_retry_count, c.retry_count
            ),
            Ok(_) => "the row was modified concurrently in a way this command cannot attribute to a newer claim (its retry_count is unchanged)".to_string(),
            Err(e) => format!("unknown: {e}"),
        };
        return Err(crate::core::error::AgentRunError::invalid(
            "retry_duplicate_attempt",
            format!(
                "Cannot retry job '{}': {} since this command started; not launching a duplicate attempt",
                job_id, what
            ),
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    fn save_job(
        state_root: &std::path::Path,
        job_id: &str,
        pid: Option<u32>,
        exit_code: Option<i32>,
        conversation_id: Option<&str>,
        worktree: &std::path::Path,
    ) {
        save_job_with_prompt(
            state_root,
            job_id,
            pid,
            exit_code,
            conversation_id,
            worktree,
            "p",
        );
    }

    /// Like `save_job`, but with an explicit prompt/resolved_prompt: the
    /// recovery test needs a prompt long enough to be a meaningful blob
    /// marker (a 1-byte marker would match too broadly).
    fn save_job_with_prompt(
        state_root: &std::path::Path,
        job_id: &str,
        pid: Option<u32>,
        exit_code: Option<i32>,
        conversation_id: Option<&str>,
        worktree: &std::path::Path,
        prompt: &str,
    ) {
        crate::core::job::ensure_state_store(state_root).unwrap();
        let meta = JobMetadata {
            job_id: job_id.to_string(),
            project: "p".to_string(),
            prompt: prompt.to_string(),
            resolved_prompt: prompt.to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: worktree.to_path_buf(),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: conversation_id.map(str::to_string),
            retry_count: 0,
            pid,
            pid_start_time: None,
            exit_code,
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
        meta.save(state_root).unwrap();
    }

    #[test]
    fn test_retry_refuses_at_parallel_limit() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            max_parallel: 1,
            ..Default::default()
        };

        // One running job (live PID of this test process) already fills the limit.
        save_job(
            &config.state_root,
            "runjob1",
            Some(std::process::id()),
            None,
            None,
            &worktree,
        );
        // A failed job eligible for retry (terminal + conversation ID).
        save_job(
            &config.state_root,
            "retryjob",
            None,
            Some(1),
            Some("conv-123"),
            &worktree,
        );

        let err = run(
            &config,
            &RetryArgs {
                job_id: "retryjob".to_string(),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("max-parallel"),
            "retry at the parallel limit must be refused, got: {}",
            err
        );
    }

    #[test]
    fn test_retry_refused_at_parallel_limit_leaves_log_unchanged() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            max_parallel: 1,
            ..Default::default()
        };

        // One running job (live PID of this test process) already fills the limit.
        save_job(
            &config.state_root,
            "runjob1",
            Some(std::process::id()),
            None,
            None,
            &worktree,
        );
        // A failed job eligible for retry (terminal + conversation ID).
        save_job(
            &config.state_root,
            "retryjob",
            None,
            Some(1),
            Some("conv-123"),
            &worktree,
        );

        // Seed the retried job's log with the previous attempt's output so the
        // byte-identical check below is meaningful.
        let log_path = JobMetadata::log_path(&config.state_root, "retryjob");
        std::fs::write(&log_path, "previous attempt output\n").unwrap();
        let log_before = std::fs::read(&log_path).unwrap();

        let err = run(
            &config,
            &RetryArgs {
                job_id: "retryjob".to_string(),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("max-parallel"),
            "retry at the parallel limit must be refused, got: {}",
            err
        );

        // A refused retry must not append the retry banner: the log stays
        // byte-identical, so `last_attempt_log` in job.rs keeps classifying
        // the real previous attempt instead of an empty "latest attempt".
        let log_after = std::fs::read(&log_path).unwrap();
        assert_eq!(
            log_after, log_before,
            "a retry refused by the parallel limit must leave the log untouched"
        );
        assert!(
            !String::from_utf8(log_after).unwrap().contains("RETRY AT"),
            "a refused retry must not leave a banner in the log"
        );
    }

    #[test]
    fn test_retry_claim_refuses_second_claim_at_same_retry_count() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            max_parallel: 4,
            ..Default::default()
        };
        // A failed job (terminal + conversation ID): eligible for retry.
        save_job(
            &config.state_root,
            "jr1",
            None,
            Some(1),
            Some("conv-1"),
            &worktree,
        );

        // First claim (from retry_count 0) succeeds: the row moves to
        // attempt 1 and is marked running.
        claim_retry_attempt(&config.state_root, "jr1", 0).unwrap();
        let claimed = JobMetadata::load(&config.state_root, "jr1").unwrap();
        assert_eq!(claimed.retry_count, 1);
        assert_eq!(claimed.status, Some(JobStatus::Running));

        // A second, overlapping claim still reading the stale retry_count
        // must be refused, not double-launch an attempt.
        let err = claim_retry_attempt(&config.state_root, "jr1", 0).unwrap_err();
        assert!(
            err.to_string().contains("another retry already claimed"),
            "the duplicate claim must be refused, got: {}",
            err
        );
        let after = JobMetadata::load(&config.state_root, "jr1").unwrap();
        assert_eq!(
            after.retry_count, 1,
            "a refused claim must not bump retry_count again"
        );

        // Once the attempt lands in a terminal state, the next retry claims
        // cleanly at the new retry_count.
        let mut terminal = after;
        terminal.status = Some(JobStatus::Failed);
        terminal.exit_code = Some(1);
        terminal.save(&config.state_root).unwrap();
        claim_retry_attempt(&config.state_root, "jr1", 1).unwrap();
    }

    #[test]
    fn test_retry_claim_ignores_stale_running_status() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            max_parallel: 4,
            ..Default::default()
        };
        // A failed job (terminal + conversation ID): eligible for retry.
        save_job(
            &config.state_root,
            "jr2",
            None,
            Some(1),
            Some("conv-2"),
            &worktree,
        );
        // Simulate an orphan: the process is dead but the row still carries
        // status 'running' (e.g. the server restarted before the monitor
        // finalized the attempt).
        let mut orphaned = JobMetadata::load(&config.state_root, "jr2").unwrap();
        orphaned.status = Some(JobStatus::Running);
        orphaned.save(&config.state_root).unwrap();

        // The claim must succeed at the row's current retry_count despite the
        // stale 'running' status -- the retry_count guard is what makes it
        // atomic, not the status column.
        claim_retry_attempt(&config.state_root, "jr2", 0).unwrap();
        let claimed = JobMetadata::load(&config.state_root, "jr2").unwrap();
        assert_eq!(claimed.retry_count, 1);
        assert_eq!(claimed.status, Some(JobStatus::Running));

        // A second, overlapping claim at the same expected retry_count must
        // fail exactly once.
        let err = claim_retry_attempt(&config.state_root, "jr2", 0).unwrap_err();
        assert!(
            err.to_string().contains("another retry already claimed"),
            "the duplicate claim must be refused, got: {}",
            err
        );
        let after = JobMetadata::load(&config.state_root, "jr2").unwrap();
        assert_eq!(
            after.retry_count, 1,
            "a refused claim must not bump retry_count again"
        );
    }

    // -- `retry --prompt` (follow-up turn) -----------------------------------

    fn retry_args(job_id: &str, prompt: Option<&str>) -> RetryArgs {
        RetryArgs {
            job_id: job_id.to_string(),
            prompt: prompt.map(str::to_string),
            ..Default::default()
        }
    }

    /// Writes an executable fake `agy` whose recorded argv lands in
    /// `worktree/argv.txt` (the runner sets the child's cwd to the worktree)
    /// and which then emits a structured SUCCESS line for its turn.
    fn fake_agy(bin: &std::path::Path, worktree: &std::path::Path, response: &str) {
        std::fs::write(
            bin,
            format!(
                "#!/bin/sh\n{{ printf '%s\\n' \"$@\" > {argv}; echo '{{\"status\":\"SUCCESS\",\"response\":\"{response}\"}}'\n}}\n",
                argv = worktree.join("argv.txt").display(),
                response = response,
            ),
        )
        .unwrap();
        std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Saves a job whose row classifies as `done` (exit 0 + structured
    /// SUCCESS line in the log) and returns its config.
    fn save_done_job(
        temp: &tempfile::TempDir,
        job_id: &str,
        worktree: &std::path::Path,
    ) -> (Config, std::path::PathBuf) {
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            ..Default::default()
        };
        save_job(
            temp.path().join("state").as_path(),
            job_id,
            None,
            Some(0),
            Some("550e8400-e29b-41d4-a716-446655440000"),
            worktree,
        );
        let log_path = JobMetadata::log_path(&config.state_root, job_id);
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &log_path,
            "{\"status\":\"SUCCESS\",\"response\":\"done ok\"}\n",
        )
        .unwrap();
        (config, log_path)
    }

    #[test]
    fn test_retry_done_without_prompt_refused_with_followup_hint() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let (config, _) = save_done_job(&temp, "jdone", &worktree);

        let err = run(&config, &retry_args("jdone", None)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("has already completed successfully"),
            "the original refusal must stay, got: {msg}"
        );
        assert!(
            msg.contains("use --prompt to send a follow-up"),
            "the refusal must point at --prompt, got: {msg}"
        );
        // Nothing was claimed: the row must be untouched.
        let after = JobMetadata::load(&config.state_root, "jdone").unwrap();
        assert_eq!(after.retry_count, 0);
        assert_eq!(after.status, None);
        assert_eq!(compute_status(&after, &config.state_root), JobStatus::Done);
    }

    #[test]
    fn test_retry_killed_remains_refused_even_with_prompt() {
        // A killed job never carries a conversation id (the cancelled turn's
        // JSON line was cut off), so a follow-up cannot resume it either.
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        // agy_home points at a dir without a conversation store: recovery
        // must find nothing and stay a no-op, and the test must not depend
        // on whatever lives in the real process home.
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_home: Some(temp.path().join("no-agy-home")),
            ..Default::default()
        };
        save_job(
            &config.state_root,
            "jkilled",
            None,
            Some(143),
            None,
            &worktree,
        );
        let mut killed = JobMetadata::load(&config.state_root, "jkilled").unwrap();
        killed.status = Some(JobStatus::Killed);
        killed.save(&config.state_root).unwrap();

        let err = run(&config, &retry_args("jkilled", Some("carry on"))).unwrap_err();
        assert!(
            err.to_string().contains("no conversation ID"),
            "killed jobs stay refused (missing conversation id), got: {err}"
        );
        let after = JobMetadata::load(&config.state_root, "jkilled").unwrap();
        assert_eq!(after.retry_count, 0);
        assert_eq!(after.status, Some(JobStatus::Killed));
    }

    #[test]
    fn test_retry_recovers_conversation_id_from_agy_files() {
        // Simulates the abrupt-kill rollout case: the row carries no
        // conversation id, the log is empty (agy never wrote its final JSON
        // line), but agy persisted the conversation to disk. The retry must
        // recover the ID from agy's conversation files and proceed.
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let home = temp.path().join("agy-home");
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            agy_home: Some(home.clone()),
            max_parallel: 4,
            ..Default::default()
        };
        // A failed job (exit 1) with no conversation id and an empty log.
        // The prompt doubles as the blob marker for recovery.
        let prompt = "lost-conversation recovery probe prompt text";
        save_job_with_prompt(
            &config.state_root,
            "jlost",
            None,
            Some(1),
            None,
            &worktree,
            prompt,
        );
        let log_path = JobMetadata::log_path(&config.state_root, "jlost");
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&log_path, "").unwrap();

        // agy's conversation store: the job's conversation (its steps payload
        // holds the prompt, framed like agy's protobuf blobs) plus a
        // distractor conversation from a different job.
        let conv_dir = home
            .join(".gemini")
            .join("antigravity-cli")
            .join("conversations");
        std::fs::create_dir_all(&conv_dir).unwrap();
        let recovered = "e5f6a7b8-1234-5678-9abc-def012345678";
        let other = "deadbeef-1234-5678-9abc-def012345678";
        let recovered_db =
            rusqlite::Connection::open(conv_dir.join(format!("{recovered}.db"))).unwrap();
        recovered_db
            .execute_batch("CREATE TABLE steps (idx INTEGER, step_payload BLOB);")
            .unwrap();
        // The payload frames the prompt like agy's protobuf blobs.
        recovered_db
            .execute(
                "INSERT INTO steps (idx, step_payload) VALUES (1, ?1)",
                rusqlite::params![format!("protobuf framing: {prompt} continues here")],
            )
            .unwrap();
        let other_db = rusqlite::Connection::open(conv_dir.join(format!("{other}.db"))).unwrap();
        other_db
            .execute_batch("CREATE TABLE steps (idx INTEGER, step_payload BLOB);")
            .unwrap();
        other_db
            .execute(
                "INSERT INTO steps (idx, step_payload) VALUES (1, ?1)",
                rusqlite::params!["an unrelated other conversation payload"],
            )
            .unwrap();

        fake_agy(&config.agy_path, &worktree, "recovered-turn");
        let out = run(&config, &retry_args("jlost", None))
            .expect("retry must succeed via the recovered conversation id");
        assert!(
            out.stdout_lines
                .iter()
                .any(|l| l.contains("Recovered conversation ID")),
            "the recovery must be reported, got: {:?}",
            out.stdout_lines
        );

        // The ID is persisted on the row and the claim was applied.
        let after = JobMetadata::load(&config.state_root, "jlost").unwrap();
        assert_eq!(
            after.conversation_id.as_deref(),
            Some(recovered),
            "the recovered id must be stored on the row"
        );
        assert_eq!(
            after.retry_count, 1,
            "the retry proceeds normally (claim applied)"
        );

        // And agy itself must be launched with the recovered id.
        let argv = worktree.join("argv.txt");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let text = loop {
            match std::fs::read_to_string(&argv) {
                Ok(text) if text.lines().any(|l| l == "--conversation") => break text,
                Ok(_) => {}
                Err(_) => {}
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the fake agy never recorded its argv"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        assert!(
            text.lines().any(|l| l == recovered),
            "agy must receive --conversation <recovered id>, argv: {text}"
        );
    }

    #[test]
    fn test_retry_running_refused_even_with_prompt() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        // A live PID (of this test process) with no exit code classifies as Running.
        save_job(
            &config.state_root,
            "jrun",
            Some(std::process::id()),
            None,
            None,
            &worktree,
        );

        let err = run(&config, &retry_args("jrun", Some("carry on"))).unwrap_err();
        assert!(
            err.to_string().contains("job is currently running"),
            "running jobs stay refused even with --prompt, got: {err}"
        );
        let after = JobMetadata::load(&config.state_root, "jrun").unwrap();
        assert_eq!(after.retry_count, 0);
    }

    #[test]
    fn test_retry_followup_refuses_empty_and_oversized_prompt() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            ..Default::default()
        };
        // A failed job with a conversation ID: every other gate would pass.
        save_job(
            &config.state_root,
            "jfailed",
            None,
            Some(1),
            Some("conv-f"),
            &worktree,
        );

        let err = run(&config, &retry_args("jfailed", Some("   "))).unwrap_err();
        assert_eq!(err.to_string(), "Prompt cannot be empty");

        let huge = "x".repeat(PROMPT_ARGV_MAX_BYTES + 1);
        let err = run(&config, &retry_args("jfailed", Some(huge.as_str()))).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("byte limit for prompts passed to agy as an argv element"),
            "the same size bound as start must apply, got: {msg}"
        );
        assert!(msg.contains("(execve would fail with E2BIG)"), "got: {msg}");

        // Both refusals happen before anything is claimed: row untouched.
        let after = JobMetadata::load(&config.state_root, "jfailed").unwrap();
        assert_eq!(
            after.retry_count, 0,
            "a refused follow-up must not bump retry_count"
        );
        assert_eq!(
            after.status, None,
            "a refused follow-up must not mark the job running"
        );
        assert!(
            !JobMetadata::log_path(&config.state_root, "jfailed").exists(),
            "no banner may be written for a refused retry"
        );
    }

    #[test]
    fn test_retry_followup_on_done_job_launches_new_turn() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let (config, log_path) = save_done_job(&temp, "jdone2", &worktree);
        fake_agy(&config.agy_path, &worktree, "follow-up ok");

        const CONV: &str = "550e8400-e29b-41d4-a716-446655440000";
        const PROMPT: &str = "now fix the flake";
        let output = run(&config, &retry_args("jdone2", Some(PROMPT)))
            .expect("a follow-up on a done job must launch");
        assert!(
            output.data["pid"].is_u64(),
            "the launch must report the agy pid, got: {}",
            output.data
        );

        // The row carries the NEW prompt in retry_prompt only (`status`
        // shows the incremented retry count); the original prompt is
        // unchanged. The stored follow-up prompt is stamped with this
        // attempt's reference line (turn = retry_count = 1).
        let after = JobMetadata::load(&config.state_root, "jdone2").unwrap();
        assert_eq!(after.retry_count, 1, "retry_count must be incremented");
        assert_eq!(
            after.retry_prompt.as_deref(),
            Some(format!("{PROMPT}\n\n[agent-run ref jdone2#1]").as_str()),
        );
        assert_eq!(after.prompt, "p", "the original prompt must stay unchanged");
        assert_eq!(
            after.resolved_prompt, "p",
            "the original resolved prompt must stay unchanged"
        );
        assert_eq!(after.conversation_id.as_deref(), Some(CONV));

        // Wait for the fake agy's turn to land in the log.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let log_content;
        loop {
            if let Ok(s) = std::fs::read_to_string(&log_path) {
                if s.contains("follow-up ok") {
                    log_content = s;
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the new turn's SUCCESS line never appeared in the log"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // The banner keeps the exact classifier prefix, with the follow-up
        // annotation (raw prompt byte size) before the closing ==== run.
        let banner = log_content
            .lines()
            .find(|l| l.contains("==================== RETRY AT"))
            .expect("the retry banner line is missing");
        assert!(
            banner.starts_with("==================== RETRY AT"),
            "the banner prefix must stay exact for last_attempt_log: {banner}"
        );
        assert!(
            banner.contains(&format!(" (follow-up prompt, {} bytes)", PROMPT.len())),
            "the banner must annotate the follow-up size: {banner}"
        );
        assert!(
            banner.ends_with("===================="),
            "the annotation must precede the closing ==== run: {banner}"
        );

        // `result` shows the response of the LAST turn: the new SUCCESS line
        // wins over the original "done ok" one.
        assert!(
            log_content.contains("done ok"),
            "the original attempt's log must be preserved"
        );
        assert_eq!(
            crate::core::job::extract_final_response(&log_content).as_deref(),
            Some("follow-up ok"),
            "result must show the last turn's response"
        );

        // Once the fake agy is reaped the job classifies from the new attempt.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if compute_status(&after, &config.state_root) == JobStatus::Done {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the follow-up turn never classified done"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // The fake agy received the conversation id and the NEW prompt text.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let argv = loop {
            if let Ok(s) = std::fs::read_to_string(worktree.join("argv.txt")) {
                break s;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the fake agy never recorded its argv"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        let lines: Vec<&str> = argv.lines().collect();
        assert!(lines.contains(&"--conversation"), "argv: {argv}");
        assert!(lines.contains(&CONV), "argv: {argv}");
        let pi = lines
            .iter()
            .position(|l| *l == "-p")
            .expect("the follow-up prompt must be passed with -p: {argv}");
        // The -p element is the follow-up text stamped with the attempt's
        // reference line (turn 1); the stamped element spans argv-log lines
        // (one arg per line), so check the joined text.
        assert_eq!(lines.get(pi + 1), Some(&PROMPT), "argv: {argv}");
        assert!(
            argv.contains(&format!("{PROMPT}\n\n[agent-run ref jdone2#1]")),
            "argv: {argv}"
        );
    }

    #[test]
    fn test_retry_without_prompt_keeps_stored_prompt_and_plain_banner() {
        // Regression guard: without --prompt the existing behavior (re-run
        // the stored prompt, plain banner) must be unchanged.
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            ..Default::default()
        };
        // A failed job with a conversation ID.
        save_job(
            &config.state_root,
            "jfail",
            None,
            Some(1),
            Some("conv-a"),
            &worktree,
        );
        let log_path = JobMetadata::log_path(&config.state_root, "jfail");
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        fake_agy(&config.agy_path, &worktree, "retried");

        run(&config, &retry_args("jfail", None))
            .expect("a plain retry of a failed job must launch");

        let after = JobMetadata::load(&config.state_root, "jfail").unwrap();
        assert_eq!(after.retry_count, 1);
        // The stored prompt is reused and re-stamped for this attempt (turn 1).
        assert_eq!(
            after.retry_prompt.as_deref(),
            Some("p\n\n[agent-run ref jfail#1]"),
            "the stored prompt must be reused (with this attempt's ref line)"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let log_content;
        loop {
            if let Ok(s) = std::fs::read_to_string(&log_path) {
                if s.contains("retried") {
                    log_content = s;
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the re-run's SUCCESS line never appeared in the log"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let banner = log_content
            .lines()
            .find(|l| l.contains("==================== RETRY AT"))
            .expect("the retry banner line is missing");
        assert!(
            banner.starts_with("==================== RETRY AT"),
            "got: {banner}"
        );
        assert!(
            !banner.contains("follow-up"),
            "a plain retry banner must carry no follow-up annotation: {banner}"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let argv = loop {
            if let Ok(s) = std::fs::read_to_string(worktree.join("argv.txt")) {
                break s;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the fake agy never recorded its argv"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        let lines: Vec<&str> = argv.lines().collect();
        let pi = lines
            .iter()
            .position(|l| *l == "-p")
            .expect("the stored prompt must be passed with -p: {argv}");
        // The -p element is the stored prompt re-stamped for this attempt
        // (turn 1); the stamped element spans argv-log lines (one arg per
        // line), so the line after -p is the prompt's first line and the
        // full element follows in the joined text.
        assert_eq!(lines.get(pi + 1), Some(&"p"), "argv: {argv}");
        assert!(
            argv.contains("p\n\n[agent-run ref jfail#1]"),
            "argv: {argv}"
        );
    }

    #[test]
    fn test_retry_without_prompt_after_failed_followup_reuses_retry_prompt() {
        // Regression guard: a row whose last attempt was a `retry --prompt`
        // follow-up stores that text in `retry_prompt`; a subsequent PLAIN
        // retry must launch the retry prompt, not silently re-send the
        // original prompt.
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            ..Default::default()
        };
        // A failed job with a conversation ID whose row still carries the
        // FAILED follow-up prompt from a previous `retry --prompt`.
        save_job(
            &config.state_root,
            "jfu",
            None,
            Some(1),
            Some("conv-fu"),
            &worktree,
        );
        let mut row = JobMetadata::load(&config.state_root, "jfu").unwrap();
        row.retry_prompt = Some("the failed follow-up".to_string());
        row.save(&config.state_root).unwrap();
        fake_agy(&config.agy_path, &worktree, "retried follow-up");

        run(&config, &retry_args("jfu", None))
            .expect("a plain retry after a failed follow-up must launch");

        let after = JobMetadata::load(&config.state_root, "jfu").unwrap();
        // The (legacy-shaped, never tokenized) stored retry prompt is launched
        // as-is, stamped with this attempt's reference line (turn 1).
        assert_eq!(
            after.retry_prompt.as_deref(),
            Some("the failed follow-up\n\n[agent-run ref jfu#1]"),
            "the launched (stored) prompt must be the retry prompt, not the original"
        );
        assert_eq!(after.retry_count, 1);

        // The fake agy must actually receive the retry prompt via -p.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let argv = loop {
            if let Ok(s) = std::fs::read_to_string(worktree.join("argv.txt")) {
                break s;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the fake agy never recorded its argv"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        let lines: Vec<&str> = argv.lines().collect();
        let pi = lines
            .iter()
            .position(|l| *l == "-p")
            .expect("the retry prompt must be passed with -p: {argv}");
        // Legacy-shaped stored retry prompt (never tokenized): the plain
        // retry still launches it, and the launcher stamps it for this
        // attempt (turn 1).
        assert_eq!(
            lines.get(pi + 1),
            Some(&"the failed follow-up"),
            "argv: {argv}"
        );
        assert!(
            argv.contains("the failed follow-up\n\n[agent-run ref jfu#1]"),
            "argv: {argv}"
        );
    }

    #[test]
    fn test_retry_followup_recall_operates_on_new_text() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            hindsight_bank: Some("bank-h".to_string()),
            hindsight_auto_recall: true,
            ..Default::default()
        };
        // A failed job with a conversation ID; the stored prompt is "p".
        save_job(
            &config.state_root,
            "jrecall",
            None,
            Some(1),
            Some("conv-r"),
            &worktree,
        );
        let mut jrec = JobMetadata::load(&config.state_root, "jrecall").unwrap();
        jrec.hindsight_bank = Some("bank-h".to_string());
        jrec.save(&config.state_root).unwrap();
        let log_path = JobMetadata::log_path(&config.state_root, "jrecall");
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        fake_agy(&config.agy_path, &worktree, "recalled turn");

        // Fake hindsight: records the query it was asked for and answers with
        // one recalled memory.
        let hindsight = temp.path().join("fake-hindsight");
        let state_root_str = config.state_root.display().to_string();
        std::fs::write(
            &hindsight,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > {q}\necho '{{\"results\":[{{\"text\":\"recalled fact\"}}]}}'\n",
                q = state_root_str + "/recall-query.txt"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&hindsight, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = Config {
            hindsight_path: hindsight.clone(),
            ..config
        };

        const PROMPT: &str = "second question";
        let mut args = retry_args("jrecall", Some(PROMPT));
        args.hindsight = Some(HindsightFlag::On);
        run(&config, &args).expect("the follow-up with recall must launch");

        // The recall query was built from the NEW prompt, not the stored one.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let query = loop {
            if let Ok(s) = std::fs::read_to_string(config.state_root.join("recall-query.txt")) {
                break s;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "hindsight never recorded the recall query"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        assert!(
            query.ends_with(format!("bank-h -- {PROMPT}\n").as_str()),
            "recall must operate on the new prompt text, got: {query:?}"
        );

        // The stored retry prompt is the recalled context prepended to the
        // NEW prompt (recalled-context wrapper from prepend_recalled_context),
        // stamped with this attempt's reference line (turn 1).
        let after = JobMetadata::load(&config.state_root, "jrecall").unwrap();
        let expected = prepend_recalled_context("recalled fact", PROMPT);
        let stamped = agy_conversations::normalize_ref_prompt(&expected, "jrecall", 1);
        assert_eq!(after.retry_prompt.as_deref(), Some(stamped.as_str()));
    }

    #[test]
    fn test_retry_oversized_prompt_after_recall_refused_before_claim() {
        // Regression guard: the size check on the user's follow-up text runs
        // BEFORE auto-recall; when the recalled context pushes the FINAL
        // prompt over the argv limit, the retry must be refused before the
        // row is claimed, not after (a post-claim bail would leave the row
        // marked running with no process behind).
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            hindsight_bank: Some("bank-big".to_string()),
            hindsight_auto_recall: true,
            ..Default::default()
        };
        // A failed job with a conversation ID: every other gate would pass.
        save_job(
            &config.state_root,
            "jbig",
            None,
            Some(1),
            Some("conv-big"),
            &worktree,
        );
        let log_path = JobMetadata::log_path(&config.state_root, "jbig");
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        // Fake hindsight: answers with ONE recalled memory large enough to
        // push the prepended final prompt over the limit (the user prompt
        // itself is well under it).
        let recall_json = temp.path().join("recall.json");
        std::fs::write(
            &recall_json,
            format!(
                "{{\"results\":[{{\"text\":\"{}\"}}]}}",
                "y".repeat(PROMPT_ARGV_MAX_BYTES)
            ),
        )
        .unwrap();
        let hindsight = temp.path().join("fake-hindsight");
        std::fs::write(
            &hindsight,
            format!("#!/bin/sh\ncat {}\n", recall_json.display()),
        )
        .unwrap();
        std::fs::set_permissions(&hindsight, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = Config {
            hindsight_path: hindsight,
            ..config
        };

        const PROMPT: &str = "small follow-up";
        let mut args = retry_args("jbig", Some(PROMPT));
        args.hindsight = Some(HindsightFlag::On);
        let err = run(&config, &args).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("byte limit for prompts passed to agy as an argv element"),
            "the FINAL prompt (with recalled context) must be size-checked, got: {msg}"
        );
        assert!(msg.contains("(execve would fail with E2BIG)"), "got: {msg}");

        // The refusal happens before the claim: the row is untouched.
        let after = JobMetadata::load(&config.state_root, "jbig").unwrap();
        assert_eq!(
            after.retry_count, 0,
            "a post-recall oversize refusal must not bump retry_count"
        );
        assert_eq!(
            after.status, None,
            "a post-recall oversize refusal must not mark the job running"
        );
        assert!(
            after.retry_prompt.is_none(),
            "a post-recall oversize refusal must not store the oversized prompt"
        );
        assert_eq!(after.exit_code, Some(1), "the row must be unchanged");
    }

    #[test]
    fn test_retry_hindsight_off_wins_over_stored_and_server_defaults() {
        // Regression guard for the precedence bug: a per-job `off` must win
        // over the stored per-job choice AND the server defaults (e.g. a
        // server started with AGENT_RUN_HINDSIGHT_AUTO_RECALL=true implies
        // hindsight on; `retry --hindsight off` must still run it without
        // any Hindsight activity).
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let (config, _) = save_done_job(&temp, "joff", &worktree);

        // The stored job opted into recall/retain ...
        let mut joff = JobMetadata::load(&config.state_root, "joff").unwrap();
        joff.hindsight = Some(true);
        joff.auto_recall = Some(true);
        joff.auto_retain = Some(true);
        joff.hindsight_bank = Some("bank-off".to_string());
        joff.save(&config.state_root).unwrap();

        // ... and so does the server.
        let config = Config {
            hindsight_enabled: true,
            hindsight_auto_recall: true,
            hindsight_auto_retain: true,
            ..config
        };
        fake_agy(&config.agy_path, &worktree, "follow-up ok");

        let mut args = retry_args("joff", Some("again"));
        args.hindsight = Some(HindsightFlag::Off);
        run(&config, &args).expect("the follow-up with --hindsight off must launch");

        let after = JobMetadata::load(&config.state_root, "joff").unwrap();
        assert_eq!(after.hindsight, Some(false), "off must force enabled=false");
        assert_eq!(
            after.auto_recall,
            Some(false),
            "off must force auto_recall=false"
        );
        assert_eq!(
            after.auto_retain,
            Some(false),
            "off must force auto_retain=false"
        );
        // The follow-up prompt is sent verbatim: no recalled context.
        assert!(
            after.retry_prompt.as_deref().unwrap().starts_with("again"),
            "no recalled-context wrapper may be prepended, got: {:?}",
            after.retry_prompt
        );
    }

    /// Sets up a failed job with a conversation ID, a bank, and a fake
    /// hindsight that records its recall argv; returns the config.
    fn recall_fixture(
        temp: &tempfile::TempDir,
        job_id: &str,
        bank: &str,
        stored_query: Option<&str>,
    ) -> (Config, std::path::PathBuf) {
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            repos_root: temp.path().join("ws").join(".agent-repos"),
            workspace_root: temp.path().join("ws"),
            agy_path: temp.path().join("fake-agy"),
            hindsight_bank: Some(bank.to_string()),
            hindsight_auto_recall: true,
            ..Default::default()
        };
        save_job(
            &config.state_root,
            job_id,
            None,
            Some(1),
            Some("conv-r"),
            &worktree,
        );
        let mut jrec = JobMetadata::load(&config.state_root, job_id).unwrap();
        jrec.hindsight_bank = Some(bank.to_string());
        if let Some(q) = stored_query {
            jrec.hindsight_query = Some(q.to_string());
        }
        jrec.save(&config.state_root).unwrap();
        let log_path = JobMetadata::log_path(&config.state_root, job_id);
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        fake_agy(&config.agy_path, &worktree, "recalled turn");

        let hindsight = temp.path().join("fake-hindsight");
        std::fs::write(
            &hindsight,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > {}/recall-query.txt\necho '{{\"results\":[{{\"text\":\"recalled fact\"}}]}}'\n",
                config.state_root.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&hindsight, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = Config {
            hindsight_path: hindsight,
            ..config
        };
        (config, worktree)
    }

    fn recorded_recall_query(config: &Config) -> String {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(s) = std::fs::read_to_string(config.state_root.join("recall-query.txt")) {
                return s;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "hindsight never recorded the recall query"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[test]
    fn test_retry_recall_uses_stored_hindsight_query() {
        // A query stored on the job (from `start --hindsight-query`) is reused
        // for the retry's recall instead of the follow-up text; the prompt
        // itself is still the follow-up text (recalled context is prepended to
        // it, exactly as before).
        let temp = tempdir().unwrap();
        let (config, _worktree) = recall_fixture(&temp, "jstored", "bank-h", Some("stored query"));

        const PROMPT: &str = "second question";
        let mut args = retry_args("jstored", Some(PROMPT));
        args.hindsight = Some(HindsightFlag::On);
        run(&config, &args).expect("the follow-up with recall must launch");

        let query = recorded_recall_query(&config);
        assert!(
            query.ends_with("bank-h -- stored query\n"),
            "the stored query (not the follow-up text) must be the recall query, got: {query:?}"
        );
        assert!(
            !query.contains(PROMPT),
            "the follow-up text must not appear in the recall argv: {query:?}"
        );

        let after = JobMetadata::load(&config.state_root, "jstored").unwrap();
        // The stored query is kept when the flag is not repeated.
        assert_eq!(after.hindsight_query.as_deref(), Some("stored query"));
        // The retry prompt is the recalled context prepended to the follow-up.
        let expected = prepend_recalled_context("recalled fact", PROMPT);
        let stamped = agy_conversations::normalize_ref_prompt(&expected, "jstored", 1);
        assert_eq!(after.retry_prompt.as_deref(), Some(stamped.as_str()));
    }

    #[test]
    fn test_retry_hindsight_query_flag_overrides_stored_and_is_persisted() {
        let temp = tempdir().unwrap();
        let (config, _worktree) = recall_fixture(&temp, "jrepl", "bank-h", Some("old query"));

        const PROMPT: &str = "third question";
        let mut args = retry_args("jrepl", Some(PROMPT));
        args.hindsight = Some(HindsightFlag::On);
        args.hindsight_query = Some("  new query ".to_string());
        run(&config, &args).expect("the follow-up with an explicit query must launch");

        let query = recorded_recall_query(&config);
        assert!(
            query.ends_with("bank-h -- new query\n"),
            "the explicit (trimmed) query must win over the stored one, got: {query:?}"
        );

        let after = JobMetadata::load(&config.state_root, "jrepl").unwrap();
        assert_eq!(
            after.hindsight_query.as_deref(),
            Some("new query"),
            "the explicit query must replace the stored one"
        );
    }

    #[test]
    fn test_retry_hindsight_query_validation_refuses_before_claim() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        let (config, _) = save_done_job(&temp, "jfail2", &worktree);
        // A done job is not retryable without a follow-up; give it one so the
        // query validation (which runs before the status gates' follow-up
        // check matters) is what produces each error.
        let mut args_off = retry_args("jfail2", Some("again"));
        args_off.hindsight = Some(HindsightFlag::Off);
        args_off.hindsight_query = Some("q".to_string());
        let err_off = run(&config, &args_off).unwrap_err();
        assert_eq!(
            err_off.to_string(),
            "--hindsight-query cannot be used with --hindsight off (recall is disabled for this job)"
        );

        let mut args_empty = retry_args("jfail2", Some("again"));
        args_empty.hindsight_query = Some("   ".to_string());
        let err_empty = run(&config, &args_empty).unwrap_err();
        assert_eq!(
            err_empty.to_string(),
            "--hindsight-query cannot be empty after trimming"
        );

        let mut args_long = retry_args("jfail2", Some("again"));
        args_long.hindsight_query = Some("x".repeat(2001));
        let err_long = run(&config, &args_long).unwrap_err();
        assert_eq!(
            err_long.to_string(),
            "--hindsight-query is 2001 characters, longer than the 2000 character limit"
        );

        // Every refusal happens before the claim: the row is untouched.
        let after = JobMetadata::load(&config.state_root, "jfail2").unwrap();
        assert_eq!(
            after.retry_count, 0,
            "a refused retry must not bump retry_count"
        );
        assert_eq!(
            after.status, None,
            "a refused retry must not mark the job running"
        );
        assert_eq!(after.exit_code, Some(0), "the row must be unchanged");
    }
}

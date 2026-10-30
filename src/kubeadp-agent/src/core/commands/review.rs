use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::json;

use crate::core::args::ReviewArgs;
use crate::core::commands::start::check_parallel_limit;
use crate::core::config::Config;
use crate::core::job::{generate_unique_job_id, JobMetadata};
use crate::core::output::CommandOutput;
use crate::core::runner::launch_background_job;

/// Hard cap on diff size embedded in the reviewer prompt (characters).
const MAX_DIFF_CHARS: usize = 24_000;

/// Best-effort removal of the review job row that this `review` inserted with
/// `insert_new` before the background launch: if the launch then fails the row
/// must not survive (it would show up in `list` as a stale job with no pid).
/// Must only be called for a row this review just created; failures are
/// reported on stderr and never replace the caller's error.
fn discard_new_job_row(config: &Config, job_id: &str) {
    if let Err(e) = JobMetadata::delete(&config.state_root, job_id) {
        eprintln!(
            "Warning: failed to delete job record '{}' of a failed review: {}",
            job_id, e
        );
    }
}

pub fn run(config: &Config, args: &ReviewArgs) -> Result<CommandOutput> {
    let target = JobMetadata::resolve(&config.state_root, &args.job_id)?;

    if !target.worktree_path.exists() {
        return Err(crate::core::error::AgentRunError::invalid(
            "worktree_not_found",
            format!(
                "Worktree directory for job '{}' does not exist at {} (it may have been deleted or cleaned)",
                args.job_id,
                target.worktree_path.display()
            ),
        )
        .into());
    }

    // A git-sync repo re-init can orphan this worktree: re-attach it before
    // the diff is computed (silent no-op when healthy).
    crate::core::repos::auto_repair_worktree(config, &target);

    let (diff_text, _fell_back) = crate::core::commands::diff::job_diff_text(&target)
        .context("Failed to compute diff for review")?;
    if diff_text.trim().is_empty() {
        return Err(crate::core::error::AgentRunError::invalid(
            "review_no_diff",
            format!(
                "No diff found in job '{}' worktree to review (worktree is clean against '{}').",
                args.job_id, target.base_ref
            ),
        )
        .into());
    }

    let diff_embedded = if diff_text.chars().count() > MAX_DIFF_CHARS {
        let head: String = diff_text.chars().take(MAX_DIFF_CHARS).collect();
        format!(
            "{}\n... (diff truncated at {} of {} characters) ...\n",
            head,
            MAX_DIFF_CHARS,
            diff_text.chars().count()
        )
    } else {
        diff_text
    };

    let original_prompt = if !target.prompt.trim().is_empty() {
        target.prompt.clone()
    } else {
        target.resolved_prompt.clone()
    };

    let prompt = format!(
        "You are reviewing changes produced by agent-run job '{job_id}' (project '{project}', base branch '{base_ref}').\n\
Original prompt of the producing job:\n>\n{original}\n\n\
Review the diff below. Do not modify any files. Report:\n\
1. Correctness issues or bugs introduced by the diff\n\
2. Security concerns\n\
3. Code quality / maintainability problems worth fixing\n\
4. Whether the changes appear to fulfill the original prompt\n\
Be specific: cite file and line. If the diff is fine, say so.\n\n\
=== DIFF (base: {base_ref}) ===\n{diff}",
        job_id = args.job_id,
        project = target.project,
        base_ref = target.base_ref,
        original = original_prompt.replace('\n', "\n> "),
        diff = diff_embedded,
    );

    let review_job_id = generate_unique_job_id(&config.state_root)?;
    let log_path = JobMetadata::log_path(&config.state_root, &review_job_id);

    // The review job runs in PLAN mode over the target job's existing
    // worktree: it owns no worktree or branch of its own (external_worktree),
    // so clean/gc will not remove it.
    let mut meta = JobMetadata {
        job_id: review_job_id.clone(),
        project: target.project.clone(),
        prompt: format!("Review changes for job {}", target.job_id),
        resolved_prompt: prompt,
        model: args
            .reviewer_model
            .clone()
            .or_else(|| config.default_model.clone()),
        effort: config.default_effort.clone(),
        sandbox: false,
        mode: "plan".to_string(),
        add_dirs: vec![],
        print_timeout: None,
        created_at: Utc::now(),
        worktree_path: target.worktree_path.clone(),
        branch: String::new(),
        base_ref: target.base_ref.clone(),
        conversation_id: None,
        retry_count: 0,
        pid: None,
        pid_start_time: None,
        exit_code: None,
        status: None,
        hindsight: Some(false),
        auto_recall: Some(false),
        auto_retain: Some(false),
        retry_prompt: None,
        read_only: false,
        external_worktree: true,
        hindsight_bank: target.hindsight_bank.clone(),
        proxy: target.proxy.clone(),
        max_runtime: None,
        started_at: None,
        hindsight_query: None,
    };

    // Enforce the same max-parallel limit `start` applies before
    // launching the background reviewer job.
    check_parallel_limit(config)?;

    // Create the review job row with a plain INSERT that hard-fails on an id
    // collision instead of upserting over an existing job (the first row
    // write otherwise happens in the shared launch path
    // (`launch_background_job_with_timeout` -> `save`, an upsert) which is
    // also used by `retry`, so that shared `save` stays untouched and the
    // later save remains a plain upsert).
    meta.insert_new(&config.state_root)?;

    let pid = match launch_background_job(config, &mut meta) {
        Ok(pid) => pid,
        Err(e) => {
            // The row this review inserted right before the launch must not
            // survive a failed launch (the review job owns no worktree or
            // branch of its own, so only the row needs removing).
            discard_new_job_row(config, &review_job_id);
            return Err(e);
        }
    };

    let mut output = CommandOutput::new(json!({
        "job_id": review_job_id,
        "pid": pid,
        "worktree_path": target.worktree_path.to_string_lossy().to_string(),
        "log_path": log_path.to_string_lossy().to_string(),
    }));
    output.push_out(format!(
        "Review job '{}' started in background (plan mode).",
        review_job_id
    ));
    output.push_out(format!("  PID:      {}", pid));
    output.push_out(format!("  Worktree: {}", target.worktree_path.display()));
    output.push_out(format!("  Log:      {}", log_path.display()));
    output.push_out(format!(
        "Run 'agent-run result {}' once it finishes to read the review.",
        review_job_id
    ));
    Ok(output)
}

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde_json::{json, Value};
use std::process::Command;

use crate::core::args::{HindsightFlag, StartArgs};
use crate::core::config::Config;
use crate::core::git::{
    create_worktree, delete_branch, prune_stale_worktree_registrations, remove_worktree,
    validate_external_worktree, validate_project_repo,
};
use crate::core::hindsight::{
    append_to_log_file, execute_recall, extract_prompt_text, parse_recall_response,
    prepend_recalled_context, resolve_bank, validate_hindsight_config, validate_hindsight_query,
};
use crate::core::job::{compute_status, generate_unique_job_id, list_jobs, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;
use crate::core::runner::{launch_background_job, resolve_prompt};

/// Determines the base ref for a new worktree branch.
///
/// When `--ref` is not provided (or empty), defaults to "HEAD" so it reliably resolves
/// regardless of whether the checkout is a normal clone, a git-sync shallow clone,
/// or a detached HEAD checkout.
pub fn resolve_base_ref(git_ref: Option<&str>) -> String {
    match git_ref {
        Some(r) if !r.trim().is_empty() => r.trim().to_string(),
        _ => "HEAD".to_string(),
    }
}

/// Rejects a new job when the number of currently running jobs already
/// reaches `config.max_parallel`.
pub fn check_parallel_limit(config: &Config) -> Result<()> {
    let running: Vec<String> = list_jobs(&config.state_root, None)?
        .iter()
        .filter(|j| compute_status(j, &config.state_root) == JobStatus::Running)
        .map(|j| j.job_id.clone())
        .collect();
    if (running.len() as u32) >= config.max_parallel {
        return Err(crate::core::error::AgentRunError::invalid(
            "parallel_limit",
            format!(
                "Cannot start new job: {} job(s) are already running, which reaches the max-parallel limit of {}. Running jobs: {}. Wait for jobs to finish ('agent-run wait <id>'), or raise the limit (--max-parallel / AGENT_RUN_MAX_PARALLEL / max_parallel in config.toml).",
                running.len(),
                config.max_parallel,
                running.join(", ")
            ),
        )
        .into());
    }
    Ok(())
}

/// Returns the branch name checked out in `worktree_path`, or an empty string
/// for a detached HEAD.
fn current_branch_name(worktree_path: &std::path::Path) -> String {
    crate::core::reaper::output_tracked(&mut {
        let mut c = Command::new("git");
        c.args([
            "-C",
            &worktree_path.to_string_lossy(),
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
        ]);
        c
    })
    .ok()
    .filter(|o| o.status.success())
    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    .filter(|b| !b.is_empty() && b != "HEAD")
    .unwrap_or_default()
}

/// Resolves the worktree's current HEAD to a concrete commit SHA, for jobs
/// started with `--from-worktree`.
///
/// The external worktree's existing commits are relative to the commit the
/// worktree sits on; storing the literal "HEAD" would make `diff`/`review`
/// later resolve HEAD to whatever the worktree's HEAD is at that time,
/// hiding all committed work. Returns "HEAD" as a fallback when the worktree
/// cannot be queried (the previous behavior).
pub fn resolve_external_base_ref(worktree_path: &std::path::Path) -> String {
    crate::core::reaper::output_tracked(&mut {
        let mut c = Command::new("git");
        c.args(["-C", &worktree_path.to_string_lossy(), "rev-parse", "HEAD"]);
        c
    })
    .ok()
    .filter(|o| o.status.success())
    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    .filter(|sha| !sha.is_empty())
    .unwrap_or_else(|| "HEAD".to_string())
}

/// Best-effort teardown of a worktree (and its branch) created by a `start`
/// that failed after `create_worktree` but before the job row was saved.
///
/// Without this the worktree directory and the `agent-run/<job-id>` branch
/// leak: no job record exists, so `list`/`clean`/`gc` cannot find or remove
/// them. Must only be called for a worktree this start created -- never for
/// an externally supplied `--from-worktree` path the job does not own.
/// Errors are reported on stderr; this never fails the caller.
pub fn cleanup_failed_start_worktree(
    repo_dir: &std::path::Path,
    worktree_path: &std::path::Path,
    branch_name: &str,
) {
    if let Err(e) = remove_worktree(repo_dir, worktree_path, true) {
        eprintln!(
            "Warning: failed to remove worktree {} of a failed start: {}",
            worktree_path.display(),
            e
        );
    }
    // Drop stale worktree registrations before the branch delete: if the
    // worktree directory was already gone when `remove_worktree` ran, git
    // still lists it and `git branch -D` refuses with "checked out at ...".
    prune_stale_worktree_registrations(repo_dir);
    if let Err(e) = delete_branch(repo_dir, branch_name) {
        eprintln!(
            "Warning: failed to delete branch '{}' of a failed start: {}",
            branch_name, e
        );
    }
}

/// Best-effort removal of the job row that this `start` inserted with
/// `insert_new` before the background launch.
///
/// If the launch then fails (e.g. the oversized-prompt check in
/// `launch_background_job_with_timeout`), the row must not survive: it would
/// otherwise show up in `list` as a stale `self-terminated` job with no pid --
/// exactly the row this helper removes. Must only be called for a row this
/// start just created; failures are reported on stderr and never replaced as
/// the caller's error.
fn discard_new_job_row(config: &Config, job_id: &str) {
    if let Err(e) = JobMetadata::delete(&config.state_root, job_id) {
        eprintln!(
            "Warning: failed to delete job record '{}' of a failed start: {}",
            job_id, e
        );
    }
}

/// Inserts the new job row, tearing down the worktree and branch this start
/// created when the insert fails (db locked, disk full, id collision).
///
/// Without the teardown the worktree and the `agent-run/<job-id>` branch leak
/// permanently: the failed insert means no job row exists, so `list`/`clean`/
/// `gc` can never find or remove them. Externally supplied
/// `--from-worktree` paths are never ours to remove. On success the worktree
/// stays (a later launch failure has its own teardown in `run`).
fn insert_new_or_cleanup(
    config: &Config,
    project_dir: &std::path::Path,
    meta: &JobMetadata,
    worktree_path: &std::path::Path,
    is_external_worktree: bool,
) -> Result<()> {
    if let Err(e) = meta.insert_new(&config.state_root) {
        if !is_external_worktree {
            cleanup_failed_start_worktree(project_dir, worktree_path, &meta.branch);
        }
        return Err(e);
    }
    Ok(())
}

pub fn run(config: &Config, args: &StartArgs) -> Result<CommandOutput> {
    // 1. Resolve bank upfront:
    // Precedence: explicit per-job override (--hindsight-bank) > project name > config/env hindsight_bank global fallback only when job has no project.
    // An explicit bank is validated and rejected when invalid (never silently
    // rewritten); derived values (project name, config/env fallback) are
    // normalised to a valid bank name.
    let resolved_bank = resolve_bank(
        args.hindsight_bank.as_deref(),
        &args.project,
        config.hindsight_bank.as_deref(),
    )?;

    // Per-job Hindsight switch. `off` wins over every server-side default:
    // no recall, no retain, for this job -- even when the server env/config
    // enables AUTO_RECALL or AUTO_RETAIN. `on` enables Hindsight using the
    // server's configured recall/retain defaults; an absent flag uses the
    // server's configured defaults as-is.
    let (hindsight_enabled, auto_recall, auto_retain) = match args.hindsight {
        Some(HindsightFlag::Off) => (false, false, false),
        Some(HindsightFlag::On) => (
            true,
            config.hindsight_auto_recall,
            config.hindsight_auto_retain,
        ),
        None => (
            config.hindsight_enabled,
            config.hindsight_auto_recall,
            config.hindsight_auto_retain,
        ),
    };
    validate_hindsight_config(hindsight_enabled, &resolved_bank)?;

    // Explicit recall query: reject it when this job explicitly disables
    // Hindsight (it would never be used), and validate it (trim, non-empty,
    // bounded length) before any worktree or job row is created.
    if args.hindsight == Some(HindsightFlag::Off) && args.hindsight_query.is_some() {
        return Err(crate::core::error::AgentRunError::invalid(
            "hindsight_query_without_recall",
            "--hindsight-query cannot be used with --hindsight off (recall is disabled for this job)",
        )
        .into());
    }
    let hindsight_query = validate_hindsight_query(&args.hindsight_query)?;

    // 2. Validate <project> directory and git repo
    let project_dir = validate_project_repo(&config.workspace_root, &args.project)?;

    // 2a. Enforce the max-parallel limit before creating any state.
    check_parallel_limit(config)?;

    let mut output = CommandOutput::new(Value::Null);

    // 2b. --from-worktree: adopt an existing worktree of the same repository.
    let external_worktree_path = if let Some(wt) = &args.from_worktree {
        let resolved = validate_external_worktree(&project_dir, wt)?;
        // Refuse to attach to a worktree that another job already owns.
        let existing = list_jobs(&config.state_root, None)?.into_iter().find(|j| {
            j.worktree_path
                .canonicalize()
                .map(|p| p == resolved)
                .unwrap_or(false)
        });
        if let Some(job) = existing {
            return Err(crate::core::error::AgentRunError::invalid(
                "worktree_in_use",
                format!(
                    "Refusing to start: worktree {} is already in use by job '{}'. Use a different worktree or clean that job first.",
                    resolved.display(),
                    job.job_id
                ),
            )
            .into());
        }
        Some(resolved)
    } else {
        None
    };

    // 2c. --plan forces plan mode (agy plan mode is the enforcement mechanism).
    let mode = if args.plan {
        "plan".to_string()
    } else {
        "accept-edits".to_string()
    };

    // 3. Resolve prompt
    let caller_cwd = std::env::current_dir().context("Failed to get current working directory")?;
    let resolved_prompt = resolve_prompt(&args.prompt, &caller_cwd)?;
    if resolved_prompt.trim().is_empty() {
        return Err(crate::core::error::AgentRunError::invalid(
            "prompt_empty",
            "Prompt cannot be empty",
        )
        .into());
    }

    // 4. Base ref for worktree: defaults to "HEAD" so it works on normal clones,
    // git-sync shallow clones, detached HEAD checkouts, etc.
    let base_ref = resolve_base_ref(args.git_ref.as_deref());

    // 5. Generate a collision-free job ID and log path
    let job_id = generate_unique_job_id(&config.state_root)?;
    let log_path = JobMetadata::log_path(&config.state_root, &job_id);

    // 6. Worktree: either adopt an externally supplied one (--from-worktree)
    //    or create a fresh one at <workspace_root>/<jobs_dir_name>/<project>/<job-id>
    //    (deliberately OUTSIDE the project checkout: on the server the project
    //    directory is a git-sync-managed path that can be deleted on sync).
    let is_external_worktree = external_worktree_path.is_some();
    let (worktree_path, branch_name, base_ref) = if let Some(wt) = external_worktree_path {
        // External worktree: agent-run owns neither it nor its branch.
        let branch = current_branch_name(&wt);
        output.push_out(format!(
            "Using existing worktree at {} (branch: '{}').",
            wt.display(),
            if branch.is_empty() {
                "(detached HEAD)"
            } else {
                &branch
            }
        ));
        // A concrete base for later `diff`/`review`: the worktree's current
        // HEAD as a SHA, since the literal "HEAD" would re-resolve to the
        // worktree's own (moving) HEAD and hide committed work.
        let resolved_base = resolve_external_base_ref(&wt);
        (wt, branch, resolved_base)
    } else {
        // Created under the workspace root (NOT inside the project checkout):
        // /workspace/<project> is a git-sync-managed symlink on the server and
        // is deleted/recreated on every upstream sync, which would take every
        // job worktree stored inside it (and its uncommitted work) with it.
        let worktree_path = config
            .workspace_root
            .join(&config.jobs_dir_name)
            .join(&args.project)
            .join(&job_id);
        let branch_name = format!("agent-run/{}", job_id);

        output.push_out(format!(
            "Creating git worktree at {} on branch '{}' (from '{}')...",
            worktree_path.display(),
            branch_name,
            base_ref
        ));

        // Create the parent directories before `git worktree add`, which
        // accepts paths outside the repository.
        if let Some(parent) = worktree_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory {}", parent.display()))?;
        }

        // The worktree is created from the project's PRIVATE repository
        // (agent-run's own bare clone), never from the git-sync-owned
        // project checkout: git-sync deletes and re-creates its repo on
        // every restart, which orphaned every job worktree registered
        // under its .git/worktrees/ (see crate::core::repos). ensure_private_repo
        // keeps a shallow mirror of the source (the checkout's origin, or
        // the checkout itself) current and fetches the base object.
        let git_repo = crate::core::repos::ensure_private_repo(
            config,
            &args.project,
            &project_dir,
            &base_ref,
        )?;

        // The worktree branch is created from this ref; store the resolved,
        // concrete commit SHA (not the literal ref name -- "HEAD" or a bare
        // branch name would compare against the worktree's own HEAD forever).
        let resolved_base = create_worktree(&git_repo, &worktree_path, &branch_name, &base_ref)?;

        // --from-patch: apply the patch on top of the fresh worktree; clean up
        // the worktree and abort if it does not apply.
        if let Some(patch) = &args.from_patch {
            let patch_path = if patch.is_absolute() {
                patch.clone()
            } else {
                caller_cwd.join(patch)
            };
            if !patch_path.exists() {
                let _ = remove_worktree(&git_repo, &worktree_path, true);
                if let Err(e) = delete_branch(&git_repo, &branch_name) {
                    output.push_err(format!(
                        "Warning: failed to delete branch '{}': {}",
                        branch_name, e
                    ));
                }
                return Err(crate::core::error::AgentRunError::invalid(
                    "patch_file_not_found",
                    format!("Patch file not found: {}", patch_path.display()),
                )
                .into());
            }
            let apply_out = match crate::core::reaper::output_tracked(&mut {
                let mut c = Command::new("git");
                c.args([
                    "-C",
                    &worktree_path.to_string_lossy(),
                    "apply",
                    "--whitespace=nowarn",
                    &patch_path.to_string_lossy(),
                ]);
                c
            }) {
                Ok(out) => out,
                Err(e) => {
                    cleanup_failed_start_worktree(&git_repo, &worktree_path, &branch_name);
                    return Err(e).context("Failed to execute 'git apply'");
                }
            };
            if !apply_out.status.success() {
                let err = String::from_utf8_lossy(&apply_out.stderr);
                let _ = remove_worktree(&git_repo, &worktree_path, true);
                if let Err(e) = delete_branch(&git_repo, &branch_name) {
                    output.push_err(format!(
                        "Warning: failed to delete branch '{}': {}",
                        branch_name, e
                    ));
                }
                return Err(crate::core::error::AgentRunError::invalid(
                    "patch_apply_failed",
                    format!(
                        "Failed to apply patch {} to the fresh worktree:\n{}\nWorktree removed.",
                        patch_path.display(),
                        err.trim()
                    ),
                )
                .into());
            }
            output.push_out(format!(
                "Applied patch {} to worktree.",
                patch_path.display()
            ));
        }
        (worktree_path, branch_name, resolved_base)
    };

    // 7. Resolve model and effort (CLI flag overrides config default)
    let model = args.model.clone().or_else(|| config.default_model.clone());
    let effort = args
        .effort
        .clone()
        .or_else(|| config.default_effort.clone());

    // 7b. Resolve the max-runtime limit: an explicit `--max-runtime` wins
    // over the server-wide `AGENT_RUN_MAX_RUNTIME`/config.toml default; when
    // neither is set the job is UNBOUNDED (the default state -- no limit).
    // The effective value is persisted on the job row so enforcement survives
    // a server restart and a per-job value always beats a later-changed
    // server default.
    let max_runtime: Option<chrono::Duration> = match &args.max_runtime {
        Some(raw) if !raw.trim().is_empty() => {
            let parsed = crate::core::commands::gc::parse_duration_string(raw.trim()).with_context(
                || {
                    format!(
                        "Invalid --max-runtime value '{}': expected a duration like '30m', '2h', or '14d' (same syntax as wait --timeout)",
                        raw
                    )
                },
            )?;
            // A zero (or negative) limit would kill the job the instant it
            // starts and has no useful meaning, so reject it explicitly (the
            // shared duration parser still allows `0` for `gc --older-than 0`
            // and waits, where zero is meaningful).
            if parsed.num_nanoseconds().is_some_and(|n| n <= 0) {
                bail!(
                    "Invalid --max-runtime value '{}': the limit must be greater than zero",
                    raw
                );
            }
            Some(parsed)
        }
        _ => config.max_runtime,
    };

    // 8. Auto-recall from Hindsight if enabled
    let mut agy_prompt = resolved_prompt.clone();

    if hindsight_enabled && auto_recall {
        if let Some(bank) = &resolved_bank {
            // `resolved_prompt` is already `@<file>`-resolved by
            // `resolve_prompt`; pass it through without a second resolution.
            let prompt_text = extract_prompt_text(&resolved_prompt, true);
            // An explicit `--hindsight-query` replaces the full prompt as the
            // recall query; the prompt itself is left untouched.
            let recall_query = hindsight_query.as_deref().unwrap_or(&prompt_text);
            match execute_recall(
                &config.hindsight_path,
                bank,
                recall_query,
                config.hindsight_tag.as_deref(),
                config.hindsight_url.as_deref(),
                config.hindsight_api_key.as_deref(),
                config.hindsight_profile.as_deref(),
                Some(config),
                args.proxy.as_deref(),
            ) {
                Ok(json_output) => {
                    if let Some(recalled_text) = parse_recall_response(&json_output) {
                        agy_prompt = prepend_recalled_context(&recalled_text, &prompt_text);
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
                    }
                }
                Err(e) => {
                    let warn_msg = format!(
                        "Hindsight recall failed: {}; proceeding with original prompt.",
                        e
                    );
                    append_to_log_file(&log_path, &format!("[hindsight warning] {}", warn_msg));
                    output.push_err(format!("Warning: {}", warn_msg));
                }
            }
        }
    }

    // 8a. Re-enforce the argv size limit on the FINAL prompt: it is measured
    // AFTER auto-recall, which may have prepended recalled context on top of
    // the (already validated) raw user prompt, and AFTER the per-turn
    // reference line is appended (see 8b). Refuse here, before `insert_new`
    // writes the row, and tear down the worktree created in step 6 so the
    // start leaves neither a row nor a worktree nor a branch behind -- the
    // same guarantee the failed-launch path provides.
    let final_prompt =
        crate::core::agy_conversations::normalize_ref_prompt(&agy_prompt, &job_id, 0);
    if final_prompt.len() > crate::core::runner::PROMPT_ARGV_MAX_BYTES {
        let message = format!(
            "prompt is {} bytes, longer than the {} byte limit for prompts passed to agy as an argv element (execve would fail with E2BIG); provide the prompt via a file (@path) or shorten it",
            final_prompt.len(),
            crate::core::runner::PROMPT_ARGV_MAX_BYTES
        );
        if !is_external_worktree {
            cleanup_failed_start_worktree(
                &crate::core::repos::project_git_repo(config, &args.project),
                &worktree_path,
                &branch_name,
            );
        }
        bail!(message);
    }

    // 8b. Stamp the prompt with this job's turn-0 reference line
    // (`[agent-run ref <job_id>#0]`) before it is stored and sent: the line
    // lands in agy's conversation db (agy stores the prompt it receives) and
    // is what the engine uses to recover this job's conversation id --
    // unique per job by construction, where an 80-byte prompt prefix is not.
    // The raw user prompt (`prompt`) is stored separately and is what
    // `status`/`list` show; the line only rides along for agy.
    agy_prompt = final_prompt;

    // 9. Assemble JobMetadata
    let bank_display = resolved_bank.as_deref().unwrap_or("none").to_string();
    let mut meta = JobMetadata {
        job_id: job_id.clone(),
        project: args.project.clone(),
        prompt: args.prompt.clone(),
        resolved_prompt: agy_prompt,
        model,
        effort,
        sandbox: args.sandbox,
        mode,
        add_dirs: args.add_dirs.clone(),
        print_timeout: args.print_timeout.clone(),
        created_at: Utc::now(),
        worktree_path: worktree_path.clone(),
        branch: branch_name,
        base_ref,
        conversation_id: None,
        retry_count: 0,
        pid: None,
        pid_start_time: None,
        exit_code: None,
        status: None,
        hindsight: Some(hindsight_enabled),
        auto_recall: Some(auto_recall),
        auto_retain: Some(auto_retain),
        retry_prompt: None,
        read_only: args.plan,
        external_worktree: is_external_worktree,
        hindsight_bank: resolved_bank,
        proxy: args.proxy.clone(),
        max_runtime,
        started_at: Some(Utc::now()),
        hindsight_query,
    };

    // 9a. Create the job row with a plain INSERT that hard-fails on an id
    //     collision instead of upserting over an existing job. The first row
    //     write otherwise happens in the shared launch path
    //     (`launch_background_job_with_timeout` -> `save`, an upsert) which is
    //     also used by `retry` (which legitimately re-saves an existing row),
    //     so that shared `save` is left untouched: this `insert_new` runs only
    //     for brand-new jobs (`start`), and the later `save` remains a plain
    //     upsert for every subsequent write, including retries. If the insert
    //     itself fails, `insert_new_or_cleanup` tears down the worktree and
    //     branch this start created before returning the error.
    insert_new_or_cleanup(
        config,
        &crate::core::repos::project_git_repo(config, &args.project),
        &meta,
        &worktree_path,
        is_external_worktree,
    )?;

    // 10. Launch detached in background
    let worktree_for_cleanup = worktree_path.clone();
    let branch_for_cleanup = meta.branch.clone();
    let pid = match launch_background_job(config, &mut meta) {
        Ok(pid) => pid,
        Err(e) => {
            // The row may exist (the monitor saves it before spawning) but no
            // process was ever confirmed: tear down what this start created
            // so it cannot leak (external worktrees are not ours to remove).
            if !is_external_worktree {
                cleanup_failed_start_worktree(
                    &crate::core::repos::project_git_repo(config, &args.project),
                    &worktree_for_cleanup,
                    &branch_for_cleanup,
                );
            }
            // The row this start inserted right before the launch must not
            // survive a failed launch; it is ours in every case (internal or
            // external worktree), unlike the worktree above.
            discard_new_job_row(config, &job_id);
            return Err(e);
        }
    };

    output.push_out(format!(
        "Memory: {}",
        if hindsight_enabled {
            format!(
                "recall {}, retain {}, bank {}",
                if auto_recall { "on" } else { "off" },
                if auto_retain { "on" } else { "off" },
                bank_display.as_str()
            )
        } else {
            "off".to_string()
        }
    ));
    output.push_out(format!("Job '{}' started in background.", job_id));
    output.push_out(format!("  PID:      {}", pid));
    if let Some(limit) = &meta.max_runtime {
        output.push_out(format!(
            "  Max runtime: {}",
            crate::core::job::format_max_runtime(limit)
        ));
    }
    output.push_out(format!("  Worktree: {}", worktree_path.display()));
    output.push_out(format!(
        "  Log:      {}",
        JobMetadata::log_path(&config.state_root, &job_id).display()
    ));
    output.push_out(format!(
        "Run 'agent-run status {}' or 'agent-run tail {}' to monitor.",
        job_id, job_id
    ));

    output.data = json!({
        "job_id": job_id,
        "pid": pid,
        "worktree_path": worktree_path.to_string_lossy().to_string(),
        "log_path": log_path.to_string_lossy().to_string(),
    });
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_base_ref_default_is_head() {
        assert_eq!(resolve_base_ref(None), "HEAD");
        assert_eq!(resolve_base_ref(Some("")), "HEAD");
        assert_eq!(resolve_base_ref(Some("   ")), "HEAD");
    }

    #[test]
    fn test_resolve_base_ref_explicit_ref_overrides_default() {
        assert_eq!(resolve_base_ref(Some("main")), "main");
        assert_eq!(resolve_base_ref(Some("feature/foo")), "feature/foo");
        assert_eq!(resolve_base_ref(Some("v1.0.0")), "v1.0.0");
        assert_eq!(resolve_base_ref(Some("abc1234")), "abc1234");
    }

    #[test]
    fn test_start_rejects_empty_and_whitespace_prompt() {
        use std::fs;
        use std::process::Command;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        fs::create_dir_all(&project_dir).unwrap();

        // Initialize git repo
        Command::new("git")
            .current_dir(&project_dir)
            .args(["init"])
            .status()
            .unwrap();
        Command::new("git")
            .current_dir(&project_dir)
            .args(["config", "user.name", "Test"])
            .status()
            .unwrap();
        Command::new("git")
            .current_dir(&project_dir)
            .args(["config", "user.email", "test@test.com"])
            .status()
            .unwrap();
        fs::write(project_dir.join("init.txt"), "hello").unwrap();
        Command::new("git")
            .current_dir(&project_dir)
            .args(["add", "."])
            .status()
            .unwrap();
        Command::new("git")
            .current_dir(&project_dir)
            .args(["commit", "-m", "init"])
            .status()
            .unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            ..Default::default()
        };

        // 1. Literal empty string prompt
        let args_empty = StartArgs {
            project: "test-repo".to_string(),
            prompt: "".to_string(),
            ..Default::default()
        };
        let err1 = run(&config, &args_empty).unwrap_err();
        assert_eq!(err1.to_string(), "Prompt cannot be empty");

        // 2. Whitespace-only string prompt
        let args_whitespace = StartArgs {
            project: "test-repo".to_string(),
            prompt: "   \t\n  ".to_string(),
            ..Default::default()
        };
        let err2 = run(&config, &args_whitespace).unwrap_err();
        assert_eq!(err2.to_string(), "Prompt cannot be empty");

        // 3. File-based prompt where file has only whitespace
        let empty_prompt_file = temp_dir.path().join("empty_prompt.txt");
        fs::write(&empty_prompt_file, "   \n\n\t  ").unwrap();
        let args_file = StartArgs {
            project: "test-repo".to_string(),
            prompt: format!("@{}", empty_prompt_file.display()),
            ..Default::default()
        };
        let err3 = run(&config, &args_file).unwrap_err();
        assert_eq!(err3.to_string(), "Prompt cannot be empty");

        // Crucial safety check: ensure no jobs directory or worktree was created
        let jobs_root = config.workspace_root.join(".agent-jobs");
        assert!(
            !jobs_root.exists(),
            "No worktree or jobs directory should be created for empty prompts"
        );
    }

    #[test]
    fn test_check_parallel_limit_rejects_at_capacity() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: temp_dir.path().join("ws").join(".agent-repos"),
            workspace_root: temp_dir.path().join("ws"),
            max_parallel: 2,
            ..Default::default()
        };

        // No jobs yet: allowed.
        check_parallel_limit(&config).unwrap();

        // Two running jobs (live PID of the test process).
        let now = chrono::Utc::now();
        for (i, id) in ["pjob1", "pjob2"].iter().enumerate() {
            let meta = JobMetadata {
                job_id: id.to_string(),
                project: "p".to_string(),
                prompt: "p".to_string(),
                resolved_prompt: "p".to_string(),
                model: None,
                effort: None,
                sandbox: false,
                mode: "accept-edits".to_string(),
                add_dirs: vec![],
                print_timeout: None,
                created_at: now,
                worktree_path: std::path::PathBuf::from(format!("/fake/{}", id)),
                branch: String::new(),
                base_ref: "main".to_string(),
                conversation_id: None,
                retry_count: i as u32,
                pid: Some(std::process::id()),
                pid_start_time: None,
                exit_code: None,
                status: None,
                hindsight: None,
                auto_recall: None,
                auto_retain: None,
                hindsight_bank: None,
                retry_prompt: None,
                read_only: false,
                external_worktree: false,
                proxy: None,
                max_runtime: None,
                started_at: None,
                hindsight_query: None,
            };
            meta.save(&state_dir).unwrap();
        }

        // At capacity: rejected, and the error names the running jobs.
        let err = check_parallel_limit(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("max-parallel"), "Error was: {}", msg);
        assert!(
            msg.contains("pjob1") && msg.contains("pjob2"),
            "Error was: {}",
            msg
        );

        // Terminating a job frees capacity (pid gone + exit code => terminal).
        let mut done = JobMetadata::load(&state_dir, "pjob1").unwrap();
        done.pid = None;
        done.exit_code = Some(0);
        done.save(&state_dir).unwrap();
        check_parallel_limit(&config).unwrap();
    }

    /// Initializes an empty git repo with one initial commit (same convention as
    /// `test_start_rejects_empty_and_whitespace_prompt`).
    fn init_fixture_repo(project_dir: &std::path::Path) {
        std::fs::create_dir_all(project_dir).unwrap();
        Command::new("git")
            .current_dir(project_dir)
            .args(["init"])
            .status()
            .unwrap();
        Command::new("git")
            .current_dir(project_dir)
            .args(["config", "user.name", "Test"])
            .status()
            .unwrap();
        Command::new("git")
            .current_dir(project_dir)
            .args(["config", "user.email", "test@test.com"])
            .status()
            .unwrap();
        std::fs::write(project_dir.join("init.txt"), "hello").unwrap();
        Command::new("git")
            .current_dir(project_dir)
            .args(["add", "."])
            .status()
            .unwrap();
        Command::new("git")
            .current_dir(project_dir)
            .args(["commit", "-m", "init"])
            .status()
            .unwrap();
    }

    /// Asserts no `agent-run/*` branch exists in the repo (the leak check).
    fn assert_no_agent_run_branches(project_dir: &std::path::Path) {
        let out = Command::new("git")
            .args([
                "-C",
                &project_dir.to_string_lossy(),
                "branch",
                "--list",
                "agent-run/*",
            ])
            .output()
            .unwrap();
        let branches = String::from_utf8_lossy(&out.stdout).trim().to_string();
        assert!(
            branches.is_empty(),
            "expected no leaked agent-run/* branches, found: {}",
            branches
        );
    }

    #[test]
    fn test_start_from_patch_not_found_cleans_up_worktree_and_branch() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            from_patch: Some(std::path::PathBuf::from("/nonexistent/missing.patch")),
            ..Default::default()
        };

        let err = run(&config, &args).unwrap_err();
        assert!(
            err.to_string().contains("Patch file not found"),
            "got: {}",
            err
        );

        // The worktree is gone, no job record was created, and -- crucially --
        // the `agent-run/<job-id>` branch created before the patch step is not leaked.
        let jobs_project_dir = config.workspace_root.join(".agent-jobs").join("test-repo");
        assert!(
            !jobs_project_dir.exists()
                || std::fs::read_dir(&jobs_project_dir).unwrap().count() == 0,
            "worktree directory should be cleaned up"
        );
        assert_no_agent_run_branches(&project_dir);
        assert!(
            list_jobs(&config.state_root, None).unwrap().is_empty(),
            "no job record should be created for a failed start"
        );
    }

    #[test]
    fn test_start_from_patch_apply_failure_cleans_up_worktree_and_branch() {
        use std::fs;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // A patch that exists on disk but modifies a file absent from the repo:
        // passes the existence check, so `git apply` itself is what fails.
        let patch_path = temp_dir.path().join("bad.patch");
        fs::write(
            &patch_path,
            "diff --git a/never-created.txt b/never-created.txt\n--- a/never-created.txt\n+++ b/never-created.txt\n@@ -1 +1,2 @@\n original-line\n+added-line\n",
        )
        .unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            from_patch: Some(patch_path),
            ..Default::default()
        };

        let err = run(&config, &args).unwrap_err();
        assert!(
            err.to_string().contains("Failed to apply patch"),
            "got: {}",
            err
        );

        let jobs_project_dir = config.workspace_root.join(".agent-jobs").join("test-repo");
        assert!(
            !jobs_project_dir.exists()
                || std::fs::read_dir(&jobs_project_dir).unwrap().count() == 0,
            "worktree directory should be cleaned up"
        );
        assert_no_agent_run_branches(&project_dir);
        assert!(
            list_jobs(&config.state_root, None).unwrap().is_empty(),
            "no job record should be created for a failed start"
        );
    }

    #[test]
    fn test_start_creates_worktree_outside_project_directory() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // Fake `agy`: exists so executable-path validation passes and the
        // job monitor has something to spawn; it exits immediately.
        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            ..Default::default()
        };

        run(&config, &args).expect("start should succeed");

        let jobs = list_jobs(&config.state_root, None).unwrap();
        assert_eq!(jobs.len(), 1, "exactly one job record should exist");
        let job = jobs[0].clone();

        // The worktree lives at <workspace_root>/<jobs_dir_name>/<project>/<job-id> --
        // outside the project directory (which, on the server, is a
        // git-sync-managed path that gets deleted on every upstream sync).
        let expected = config
            .workspace_root
            .join(&config.jobs_dir_name)
            .join("test-repo")
            .join(&job.job_id);
        assert_eq!(job.worktree_path, expected);
        assert!(!job.worktree_path.starts_with(&project_dir));
        assert!(job.worktree_path.is_dir());

        // Tidy up (worktree, branch, record) so the test leaves no state behind.
        // The worktree/branch live in the project's private repo now.
        let git_repo = crate::core::repos::project_git_repo(&config, "test-repo");
        let _ = remove_worktree(&git_repo, &job.worktree_path, true);
        let _ = delete_branch(&git_repo, &job.branch);
        let _ = JobMetadata::delete(&config.state_root, &job.job_id);
    }

    #[test]
    fn test_start_hindsight_off_wins_over_server_enabled_defaults() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        // The server has Hindsight fully enabled (as AGENT_RUN_HINDSIGHT
        // AUTO_RECALL=true would imply, including the enabled flag itself):
        // a per-job `off` must still win over all of it.
        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            hindsight_enabled: true,
            hindsight_auto_recall: true,
            hindsight_auto_retain: true,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight: Some(HindsightFlag::Off),
            ..Default::default()
        };

        let output = run(&config, &args).expect("start should succeed");
        assert!(
            output.stdout_lines.contains(&"Memory: off".to_string()),
            "the Memory line must report off, got: {:?}",
            output.stdout_lines
        );

        let jobs = list_jobs(&config.state_root, None).unwrap();
        assert_eq!(jobs.len(), 1, "exactly one job record should exist");
        assert_eq!(
            jobs[0].hindsight,
            Some(false),
            "off must force enabled=false"
        );
        assert_eq!(
            jobs[0].auto_recall,
            Some(false),
            "off must force auto_recall=false even when the server enables it"
        );
        assert_eq!(
            jobs[0].auto_retain,
            Some(false),
            "off must force auto_retain=false even when the server enables it"
        );

        // The worktree/branch live in the project's private repo now.
        let git_repo = crate::core::repos::project_git_repo(&config, "test-repo");
        let _ = remove_worktree(&git_repo, &jobs[0].worktree_path, true);
        let _ = delete_branch(&git_repo, &jobs[0].branch);
        let _ = JobMetadata::delete(&config.state_root, &jobs[0].job_id);
    }

    #[test]
    fn test_start_hindsight_on_uses_server_recall_retain_defaults() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        // The server enables recall by default but not retain; `on` picks up
        // exactly those defaults.
        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            hindsight_enabled: true,
            hindsight_auto_recall: true,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight: Some(HindsightFlag::On),
            ..Default::default()
        };

        let output = run(&config, &args).expect("start should succeed");
        assert!(
            output
                .stdout_lines
                .contains(&"Memory: recall on, retain off, bank test-repo".to_string()),
            "the Memory line must reflect the server defaults, got: {:?}",
            output.stdout_lines
        );

        let jobs = list_jobs(&config.state_root, None).unwrap();
        assert_eq!(jobs[0].hindsight, Some(true));
        assert_eq!(jobs[0].auto_recall, Some(true));
        assert_eq!(jobs[0].auto_retain, Some(false));

        // The worktree/branch live in the project's private repo now.
        let git_repo = crate::core::repos::project_git_repo(&config, "test-repo");
        let _ = remove_worktree(&git_repo, &jobs[0].worktree_path, true);
        let _ = delete_branch(&git_repo, &jobs[0].branch);
        let _ = JobMetadata::delete(&config.state_root, &jobs[0].job_id);
    }

    #[test]
    fn test_cleanup_failed_start_worktree_removes_worktree_and_branch() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // Simulate a worktree that `start` created but whose job row was
        // never saved (the leak scenario).
        let worktree_path = workspace_root
            .join(".agent-jobs")
            .join("test-repo")
            .join("leakjob1");
        create_worktree(&project_dir, &worktree_path, "agent-run/leakjob1", "HEAD").unwrap();
        assert!(
            worktree_path.is_dir(),
            "fixture worktree must exist before cleanup"
        );

        cleanup_failed_start_worktree(&project_dir, &worktree_path, "agent-run/leakjob1");

        assert!(
            !worktree_path.exists(),
            "worktree directory of a failed start must be removed"
        );
        assert_no_agent_run_branches(&project_dir);

        // Idempotent: a second call on the already-gone worktree must not panic.
        cleanup_failed_start_worktree(&project_dir, &worktree_path, "agent-run/leakjob1");
    }

    #[test]
    fn test_cleanup_failed_start_worktree_frees_branch_with_stale_registration() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // The worktree directory was deleted out-of-band between `start`
        // failing and the cleanup running: git keeps the stale registration,
        // and the helper must still free the branch.
        let worktree_path = workspace_root
            .join(".agent-jobs")
            .join("test-repo")
            .join("leakjob2");
        create_worktree(&project_dir, &worktree_path, "agent-run/leakjob2", "HEAD").unwrap();
        std::fs::remove_dir_all(&worktree_path).unwrap();
        let listing = Command::new("git")
            .args(["-C", &project_dir.to_string_lossy(), "worktree", "list"])
            .output()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&listing.stdout).contains("agent-run/leakjob2"),
            "fixture must leave a stale worktree registration"
        );

        cleanup_failed_start_worktree(&project_dir, &worktree_path, "agent-run/leakjob2");

        assert!(
            !worktree_path.exists(),
            "worktree directory must be gone after cleanup"
        );
        assert_no_agent_run_branches(&project_dir);
    }

    #[test]
    fn test_start_insert_new_failure_cleans_up_worktree_and_branch() {
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // Non-executable fake agy: a launch that gets this far would time out
        // confirming a PID (used for the end-to-end half of this test).
        let agy = temp_dir.path().join("fake-agy");
        std::fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&agy, std::fs::Permissions::from_mode(0o644)).unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            ..Default::default()
        };

        // Force `insert_new` to fail: pre-seed the exact row it would
        // insert, so the plain INSERT hard-fails on the id (the id generated
        // inside `run` is unpredictable, so the failure is triggered
        // directly against `insert_new_or_cleanup`, the same helper `start`
        // calls).
        crate::core::job::ensure_state_store(&config.state_root).unwrap();
        {
            let conn = crate::core::job::open_state_db(&config.state_root).unwrap();
            conn.execute_batch(
                "INSERT INTO jobs (
                    job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode,
                    add_dirs, print_timeout, created_at, worktree_path, branch, base_ref,
                    conversation_id, retry_count, pid, exit_code, status, hindsight,
                    auto_recall, auto_retain, retry_prompt, read_only, external_worktree,
                    pid_start_time
                ) VALUES (
                    'clashjob1', 'test-repo', 'p', 'p', NULL, NULL, 0, 'accept-edits',
                    '[]', NULL, 0, '/tmp/clash', 'agent-run/clashjob1', 'main',
                    NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, NULL
                )",
            )
            .unwrap();
            drop(conn);
        }

        // A worktree exactly like `start` creates when it is about to call
        // `insert_new`.
        let worktree_path = workspace_root
            .join(".agent-jobs")
            .join("test-repo")
            .join("clashjob1");
        create_worktree(&project_dir, &worktree_path, "agent-run/clashjob1", "HEAD").unwrap();
        assert!(
            worktree_path.is_dir(),
            "fixture worktree must exist before insert"
        );
        let meta = JobMetadata {
            job_id: "clashjob1".to_string(),
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            resolved_prompt: "do something".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: worktree_path.clone(),
            branch: "agent-run/clashjob1".to_string(),
            base_ref: "HEAD".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: None,
            status: None,
            hindsight: Some(false),
            auto_recall: Some(false),
            auto_retain: Some(false),
            hindsight_bank: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
        };

        // A failed insert of a worktree this start created must tear the
        // worktree and branch down: with no row, list/clean/gc could never
        // find them.
        let err =
            insert_new_or_cleanup(&config, &project_dir, &meta, &worktree_path, false).unwrap_err();
        let err_chain = format!("{:?}", err);
        assert!(
            err_chain.contains("job id 'clashjob1' already exists"),
            "the insert must fail with an id-collision error: {}",
            err_chain
        );
        assert!(
            !worktree_path.exists(),
            "a failed insert_new must not leak the worktree directory"
        );
        assert_no_agent_run_branches(&project_dir);

        // An externally supplied worktree is never ours to remove.
        let external_wt = temp_dir.path().join("external-wt");
        std::fs::create_dir_all(&external_wt).unwrap();
        let external_meta = JobMetadata {
            worktree_path: external_wt.clone(),
            external_worktree: true,
            ..meta
        };
        let err = insert_new_or_cleanup(&config, &project_dir, &external_meta, &external_wt, true)
            .unwrap_err();
        let err_chain = format!("{:?}", err);
        assert!(
            err_chain.contains("job id 'clashjob1' already exists"),
            "the insert must still fail: {}",
            err_chain
        );
        assert!(
            external_wt.exists(),
            "an external worktree must never be removed by a failed start"
        );

        // End-to-end: hold a write lock on the state database so `run` fails
        // at the job row write (right after creating the worktree) while the
        // earlier read (the parallel-limit check) still succeeds. The
        // worktree and branch must still be torn down even though no row
        // exists. `open_state_db` sets a 5s busy timeout, so this blocks ~5s.
        let locker = crate::core::job::open_state_db(&config.state_root).unwrap();
        locker.execute_batch("BEGIN IMMEDIATE;").unwrap();

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            ..Default::default()
        };
        let err2 = run(&config, &args).unwrap_err();
        let err_chain = format!("{:?}", err2);
        assert!(
            err_chain.contains("database is locked")
                && err_chain.contains("Failed to insert job metadata"),
            "start must fail at the job-row write, got: {}",
            err_chain
        );

        // Worktree and branch must be gone exactly like the other failed-
        // start cleanup tests assert.
        let jobs_project_dir = config.workspace_root.join(".agent-jobs").join("test-repo");
        assert!(
            !jobs_project_dir.exists()
                || std::fs::read_dir(&jobs_project_dir).unwrap().count() == 0,
            "worktree directory should be cleaned up"
        );
        assert_no_agent_run_branches(&project_dir);
        drop(locker);
    }

    #[test]
    fn test_start_launch_failure_cleans_up_worktree_and_branch() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // Non-executable fake agy: launch passes path validation but the
        // monitor cannot spawn it, so the PID poll times out and start bails
        // after the worktree has been created (the leak scenario).
        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o644)).unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            ..Default::default()
        };

        let err = run(&config, &args).unwrap_err();
        assert!(
            err.to_string().contains("did not confirm a PID"),
            "got: {}",
            err
        );

        // The worktree and the agent-run/* branch must be torn down on the
        // error path, exactly like the --from-patch failure tests assert.
        let jobs_project_dir = config.workspace_root.join(".agent-jobs").join("test-repo");
        assert!(
            !jobs_project_dir.exists()
                || std::fs::read_dir(&jobs_project_dir).unwrap().count() == 0,
            "worktree directory should be cleaned up"
        );
        assert_no_agent_run_branches(&project_dir);
    }

    #[test]
    fn test_start_oversized_prompt_failure_removes_new_job_row() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // Executable fake agy: nothing spawns, so the oversized-prompt check
        // in `launch_background_job_with_timeout` is what must refuse the job
        // -- AFTER `insert_new` has written the row.
        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "x".repeat(crate::core::runner::PROMPT_ARGV_MAX_BYTES + 1),
            ..Default::default()
        };

        let err = run(&config, &args).unwrap_err();
        assert!(
            err.to_string()
                .contains("longer than the 100000 byte limit"),
            "the oversized-prompt check must be the reason the launch failed: {}",
            err
        );

        // The row inserted by `insert_new` right before the launch must be
        // gone again: no job record at all (not even a stale self-terminated
        // one carrying the huge prompt).
        assert!(
            list_jobs(&config.state_root, None).unwrap().is_empty(),
            "no job record may remain after a launch failure"
        );

        // And the worktree directory plus the agent-run/* branch must be gone,
        // exactly like the other failed-start cleanup tests assert.
        let jobs_project_dir = config.workspace_root.join(".agent-jobs").join("test-repo");
        assert!(
            !jobs_project_dir.exists()
                || std::fs::read_dir(&jobs_project_dir).unwrap().count() == 0,
            "worktree directory should be cleaned up"
        );
        assert_no_agent_run_branches(&project_dir);
    }

    #[test]
    fn test_start_oversized_prompt_after_recall_refused_before_row_insert() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        // Executable fake agy: if the launch is ever reached it exits 0, so
        // the refusal must come from the post-recall size check BEFORE the
        // job row is inserted.
        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        // Fake hindsight: answers with ONE recalled memory large enough to
        // push the FINAL prompt (recalled context + raw prompt) over the
        // argv limit, while the raw prompt itself stays well under it.
        let recall_json = temp_dir.path().join("recall.json");
        fs::write(
            &recall_json,
            format!(
                "{{\"results\":[{{\"text\":\"{}\"}}]}}",
                "y".repeat(crate::core::runner::PROMPT_ARGV_MAX_BYTES)
            ),
        )
        .unwrap();
        let hindsight = temp_dir.path().join("fake-hindsight");
        fs::write(
            &hindsight,
            format!("#!/bin/sh\ncat {}\n", recall_json.display()),
        )
        .unwrap();
        fs::set_permissions(&hindsight, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            hindsight_bank: Some("bank-big".to_string()),
            hindsight_path: hindsight,
            hindsight_auto_recall: true,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "small prompt".to_string(),
            hindsight: Some(HindsightFlag::On),
            ..Default::default()
        };

        let err = run(&config, &args).unwrap_err();
        assert!(
            err.to_string()
                .contains("longer than the 100000 byte limit"),
            "the FINAL prompt (with recalled context) must be size-checked: {}",
            err
        );
        assert!(
            err.to_string().contains("(execve would fail with E2BIG)"),
            "got: {}",
            err
        );

        // The refusal must happen BEFORE the row insert: no job record at
        // all, and no leaked worktree or branch.
        assert!(
            list_jobs(&config.state_root, None).unwrap().is_empty(),
            "no job record may remain after a post-recall oversize refusal"
        );
        let jobs_project_dir = config.workspace_root.join(".agent-jobs").join("test-repo");
        assert!(
            !jobs_project_dir.exists()
                || std::fs::read_dir(&jobs_project_dir).unwrap().count() == 0,
            "worktree directory should be cleaned up"
        );
        assert_no_agent_run_branches(&project_dir);
    }

    #[test]
    fn test_start_hindsight_query_used_for_recall_not_prompt() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let state_root = temp_dir.path().join("state");
        // Fake hindsight: captures the recall argv and returns one memory.
        let hindsight = temp_dir.path().join("fake-hindsight");
        fs::write(
            &hindsight,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > {}/recall-query.txt\necho '{{\"results\":[{{\"text\":\"recalled fact\"}}]}}'\n",
                state_root.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&hindsight, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: state_root.clone(),
            agy_path: agy,
            hindsight_bank: Some("bank-q".to_string()),
            hindsight_path: hindsight,
            hindsight_auto_recall: true,
            ..Default::default()
        };

        let prompt = "a long prompt that must not be the recall query";
        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: prompt.to_string(),
            hindsight: Some(HindsightFlag::On),
            hindsight_bank: Some("bank-q".to_string()),
            hindsight_query: Some("  short query  ".to_string()),
            ..Default::default()
        };

        run(&config, &args).expect("start should succeed");

        // The fake hindsight records the recall arguments; poll for it.
        let query_file = state_root.join("recall-query.txt");
        let mut query = String::new();
        for _ in 0..200 {
            if let Ok(content) = fs::read_to_string(&query_file) {
                if content.contains("short query") {
                    query = content;
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(
            query.ends_with("bank-q short query\n"),
            "the trimmed explicit query (not the prompt) must be the recall query, got: {query:?}"
        );
        assert!(
            !query.contains("long prompt"),
            "the prompt must not appear in the recall argv: {query:?}"
        );

        let jobs = list_jobs(&config.state_root, None).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            jobs[0].prompt, prompt,
            "the stored prompt must be unchanged"
        );
        assert_eq!(
            jobs[0].hindsight_query,
            Some("short query".to_string()),
            "the trimmed query must be persisted on the job"
        );
        assert!(
            jobs[0].resolved_prompt.starts_with("<recalled-context>"),
            "recalled context must still be prepended: {:?}",
            jobs[0].resolved_prompt
        );
        assert!(
            jobs[0].resolved_prompt.contains(prompt),
            "the prompt text must still be part of the final prompt"
        );

        // The worktree/branch live in the project's private repo now.
        let git_repo = crate::core::repos::project_git_repo(&config, "test-repo");
        let _ = remove_worktree(&git_repo, &jobs[0].worktree_path, true);
        let _ = delete_branch(&git_repo, &jobs[0].branch);
        let _ = JobMetadata::delete(&config.state_root, &jobs[0].job_id);
    }

    #[test]
    fn test_start_hindsight_query_validation_refused_before_any_state() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            ..Default::default()
        };

        // 1. Whitespace-only query: typed empty error.
        let args_empty = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight_query: Some("   ".to_string()),
            ..Default::default()
        };
        let err1 = run(&config, &args_empty).unwrap_err();
        assert_eq!(
            err1.to_string(),
            "--hindsight-query cannot be empty after trimming"
        );

        // 2. Over-long query: typed too-long error stating the limit.
        let args_long = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight_query: Some("x".repeat(2001)),
            ..Default::default()
        };
        let err2 = run(&config, &args_long).unwrap_err();
        assert_eq!(
            err2.to_string(),
            "--hindsight-query is 2001 characters, longer than the 2000 character limit"
        );

        // 3. `--hindsight off` + query: typed conflict error.
        let args_off = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight: Some(HindsightFlag::Off),
            hindsight_query: Some("q".to_string()),
            ..Default::default()
        };
        let err3 = run(&config, &args_off).unwrap_err();
        assert_eq!(
            err3.to_string(),
            "--hindsight-query cannot be used with --hindsight off (recall is disabled for this job)"
        );

        // The refusals must happen before any worktree or job row exists.
        assert!(list_jobs(&config.state_root, None).unwrap().is_empty());
        let jobs_root = config.workspace_root.join(".agent-jobs");
        assert!(
            !jobs_root.exists(),
            "no jobs directory may be created for an invalid explicit query"
        );
        assert_no_agent_run_branches(&project_dir);
    }

    #[test]
    fn test_start_hindsight_query_accepted_but_unused_when_recall_disabled() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let agy = temp_dir.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        // Hindsight is disabled server-wide and the job does not enable it:
        // the flag is accepted and persisted, but never used for a recall.
        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            agy_path: agy,
            hindsight_enabled: false,
            hindsight_auto_recall: true,
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight_query: Some("q".to_string()),
            ..Default::default()
        };

        let output = run(&config, &args).expect("start should succeed");
        assert!(
            output.stdout_lines.contains(&"Memory: off".to_string()),
            "got: {:?}",
            output.stdout_lines
        );

        let jobs = list_jobs(&config.state_root, None).unwrap();
        assert_eq!(jobs[0].hindsight, Some(false));
        assert_eq!(
            jobs[0].hindsight_query,
            Some("q".to_string()),
            "the query is accepted and stored even when recall is off"
        );
        // No recall ran at all, so no recall-query capture file may exist.
        assert!(!config.state_root.join("recall-query.txt").exists());

        // The worktree/branch live in the project's private repo now.
        let git_repo = crate::core::repos::project_git_repo(&config, "test-repo");
        let _ = remove_worktree(&git_repo, &jobs[0].worktree_path, true);
        let _ = delete_branch(&git_repo, &jobs[0].branch);
        let _ = JobMetadata::delete(&config.state_root, &jobs[0].job_id);
    }

    #[test]
    fn test_resolve_external_base_ref_resolves_to_concrete_sha() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let project_dir = temp_dir.path().join("test-repo");
        init_fixture_repo(&project_dir);

        let wt = temp_dir.path().join("external_wt");
        create_worktree(&project_dir, &wt, "external/branch", "HEAD").unwrap();

        let head_sha = Command::new("git")
            .args(["-C", &wt.to_string_lossy(), "rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout;
        let head_sha = String::from_utf8_lossy(&head_sha).trim().to_string();

        let resolved = resolve_external_base_ref(&wt);
        assert_eq!(resolved, head_sha);
        assert_ne!(resolved, "HEAD");
        assert_eq!(resolved.len(), 40);
        assert!(
            resolved.chars().all(|c| c.is_ascii_hexdigit()),
            "expected a commit SHA, got: {}",
            resolved
        );

        // A path that is not a git checkout falls back to "HEAD"
        // (the previous behavior), rather than fabricating a SHA.
        assert_eq!(
            resolve_external_base_ref(&temp_dir.path().join("not-a-repo")),
            "HEAD"
        );
    }

    #[test]
    fn test_start_rejects_invalid_explicit_hindsight_bank() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let workspace_root = temp_dir.path().join("workspace");
        let project_dir = workspace_root.join("test-repo");
        init_fixture_repo(&project_dir);

        let config = Config {
            repos_root: workspace_root.clone().join(".agent-repos"),
            workspace_root: workspace_root.clone(),
            state_root: temp_dir.path().join("state"),
            ..Default::default()
        };

        let args = StartArgs {
            project: "test-repo".to_string(),
            prompt: "do something".to_string(),
            hindsight: Some(HindsightFlag::On),
            hindsight_bank: Some("bad/name".to_string()),
            ..Default::default()
        };

        let err = run(&config, &args).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Invalid --hindsight-bank"),
            "error should name the flag: {msg}"
        );
        assert!(
            msg.contains("bad/name"),
            "error should quote the offending value: {msg}"
        );

        // Bank resolution happens before any state is created, so nothing may
        // have leaked: no job row, no worktree registration, no branch.
        assert!(
            list_jobs(&config.state_root, None).unwrap().is_empty(),
            "no job record should be created for a rejected bank"
        );
        assert_no_agent_run_branches(&project_dir);
    }
}

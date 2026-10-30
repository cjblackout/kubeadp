use anyhow::Result;
use chrono::{Duration, Utc};
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::fs;

use crate::core::args::GcArgs;
use crate::core::config::Config;
use crate::core::git::{delete_branch, prune_stale_worktree_registrations, remove_worktree};
use crate::core::job::{compute_status, list_jobs, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

/// Minimum age for a job to be eligible for --max-jobs eviction.
pub const GC_MIN_AGE_FOR_MAX_JOBS_HOURS: i64 = 24;

/// Formats a job's age relative to `now`.
pub fn format_job_age(created_at: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> String {
    let dur = now - created_at;
    let days = dur.num_days();
    let hours = dur.num_hours();
    let minutes = dur.num_minutes();
    if days > 0 {
        format!("{}d", days)
    } else if hours > 0 {
        format!("{}h", hours)
    } else if minutes > 0 {
        format!("{}m", minutes)
    } else {
        format!("{}s", dur.num_seconds().max(0))
    }
}

/// Options controlling single job pruning in GC.
#[derive(Debug, Clone, Default)]
pub struct PruneOptions {
    pub dry_run: bool,
    pub purge_logs: bool,
    pub cutoff_time: Option<chrono::DateTime<Utc>>,
    pub prune_reason: Option<String>,
}

/// Result of evaluating or executing a single job prune.
#[derive(Debug, PartialEq, Eq)]
pub enum PruneOutcome {
    Pruned {
        job_id: String,
        status: JobStatus,
        age: String,
        removed: Vec<String>,
    },
    WouldPrune {
        job_id: String,
        status: JobStatus,
        age: String,
        items: Vec<String>,
        reason: String,
    },
    Skipped {
        job_id: String,
        reason: String,
        detail: String,
    },
}

/// Prunes a single job's worktree, branch, and metadata/log files.
pub fn prune_single_job(
    config: &Config,
    job: &JobMetadata,
    status: JobStatus,
) -> Result<PruneOutcome> {
    let mut emit = |stderr: bool, line: String| {
        if stderr {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    };
    prune_single_job_opts_to(config, job, status, &PruneOptions::default(), &mut emit)
}

/// Collected variant of [`prune_single_job`]: user-facing lines are routed
/// through `emit(stderr, line)` instead of being printed directly, so the
/// run/execute split can record them in a [`CommandOutput`].
pub fn prune_single_job_to(
    config: &Config,
    job: &JobMetadata,
    status: JobStatus,
    emit: &mut dyn FnMut(bool, String),
) -> Result<()> {
    prune_single_job_opts_to(config, job, status, &PruneOptions::default(), emit)?;
    Ok(())
}

/// Extended variant of [`prune_single_job_to`] supporting [`PruneOptions`].
pub fn prune_single_job_opts_to(
    config: &Config,
    job: &JobMetadata,
    status: JobStatus,
    opts: &PruneOptions,
    emit: &mut dyn FnMut(bool, String),
) -> Result<PruneOutcome> {
    // Job worktrees live in the project's private repository; pre-fix jobs
    // (no private repo yet) still point at the project checkout.
    let repo_dir = crate::core::repos::project_git_repo(config, &job.project);

    // Re-load the row before any destructive step: `run` snapshots terminal
    // jobs up front, and a concurrent `retry` (which reuses this row, bumps
    // retry_count, marks it running and relaunches agy in the same worktree)
    // can land between the snapshot and this prune. Destroying the
    // worktree/branch/row would kill the new attempt, so skip if the row is
    // already gone or its state changed (same re-check as `clean.rs`).
    let fresh = match JobMetadata::load(&config.state_root, &job.job_id) {
        Ok(fresh) => fresh,
        Err(_) => {
            return Ok(PruneOutcome::Skipped {
                job_id: job.job_id.clone(),
                reason: "job record gone".to_string(),
                detail: "job record no longer exists".to_string(),
            });
        }
    };
    if fresh.retry_count != job.retry_count
        || compute_status(&fresh, &config.state_root) == JobStatus::Running
    {
        emit(
            false,
            format!("Skipping job '{}': state changed concurrently", job.job_id),
        );
        return Ok(PruneOutcome::Skipped {
            job_id: job.job_id.clone(),
            reason: "state changed concurrently".to_string(),
            detail: "retry count changed or job running".to_string(),
        });
    }

    // A git-sync repo re-init can orphan this worktree's admin directory:
    // re-attach it to the private repo so the dirty check below is
    // evaluated against a consistent pair instead of erroring out on a
    // dangling one (no-op when healthy; external worktrees untouched).
    crate::core::repos::auto_repair_worktree(config, job);

    let worktree_exists = job.worktree_path.exists();

    // 1. Check worktree for uncommitted changes: stop force-removing dirty worktrees
    if worktree_exists && !job.external_worktree {
        match crate::core::git::is_worktree_dirty(&job.worktree_path) {
            Ok(true) => {
                emit(
                    false,
                    format!(
                        "Skipping job '{}': worktree at {} contains uncommitted modifications or untracked files",
                        job.job_id,
                        job.worktree_path.display()
                    ),
                );
                return Ok(PruneOutcome::Skipped {
                    job_id: job.job_id.clone(),
                    reason: "dirty worktree".to_string(),
                    detail: format!("worktree at {} is dirty", job.worktree_path.display()),
                });
            }
            Err(e) => {
                emit(
                    true,
                    format!(
                        "Warning: failed to check worktree status for job '{}' at {}: {}",
                        job.job_id,
                        job.worktree_path.display(),
                        e
                    ),
                );
                return Ok(PruneOutcome::Skipped {
                    job_id: job.job_id.clone(),
                    reason: "dirty worktree".to_string(),
                    detail: e.to_string(),
                });
            }
            Ok(false) => {}
        }
    }

    // 2. Check branch unmerged/unpushed commits:
    // "never delete a branch with unmerged/unpushed commits (skip the branch, keep it,
    // log a warning with the sha, still allow pruning the record/worktree only if the worktree
    // is clean and the branch tip is saved on a remote or merged -- otherwise skip the whole job and log why)"
    let mut branch_exists = false;
    if !job.branch.is_empty()
        && !job.external_worktree
        && repo_dir.exists()
        && crate::core::git::git_ref_exists(&repo_dir, &job.branch).unwrap_or(false)
    {
        branch_exists = true;
        let tip = crate::core::git::get_branch_tip_sha(&repo_dir, &job.branch)
            .unwrap_or_else(|_| "unknown".to_string());

        let unpushed = crate::core::git::count_unmerged_unpushed_commits(
            &repo_dir,
            &job.branch,
            &job.base_ref,
        )?;

        if unpushed > 0 {
            emit(
                true,
                format!(
                    "Warning: skipping branch '{}' (tip: {}): has {} unmerged/unpushed commit(s)",
                    job.branch, tip, unpushed
                ),
            );
            emit(
                false,
                format!(
                    "Skipping job '{}': branch '{}' has {} unmerged/unpushed commit(s) (tip: {})",
                    job.job_id, job.branch, unpushed, tip
                ),
            );
            return Ok(PruneOutcome::Skipped {
                job_id: job.job_id.clone(),
                reason: "unmerged branch".to_string(),
                detail: format!(
                    "branch '{}' has {} unmerged/unpushed commit(s) (tip: {})",
                    job.branch, unpushed, tip
                ),
            });
        }
    }

    // Determine what will be removed
    let log_path = JobMetadata::log_path(&config.state_root, &job.job_id);
    let log_exists = log_path.exists();

    // Log deletion rule:
    // keep the log of any job whose status is not Done unless it is older than the age cutoff
    // AND --purge-logs is passed (add that flag; default keeps logs)
    let should_delete_log = if status == JobStatus::Done {
        log_exists
    } else {
        let is_older_than_cutoff = opts
            .cutoff_time
            .is_some_and(|cutoff| job.created_at < cutoff);
        log_exists && is_older_than_cutoff && opts.purge_logs
    };

    let now = Utc::now();
    let age_str = format_job_age(job.created_at, now);

    let mut will_remove = Vec::new();
    if worktree_exists && !job.external_worktree {
        will_remove.push("worktree".to_string());
    }
    if branch_exists {
        will_remove.push("branch".to_string());
    }
    if job.conversation_id.is_some() {
        will_remove.push("conversation".to_string());
    }
    will_remove.push("record".to_string());
    if should_delete_log {
        will_remove.push("log".to_string());
    }

    if opts.dry_run {
        let reason = opts.prune_reason.as_deref().unwrap_or("eligible for GC");
        emit(
            false,
            format!(
                "[dry-run] Would remove job '{}' (status: {}, age: {}, removed: {}) ({})",
                job.job_id,
                status,
                age_str,
                will_remove.join(", "),
                reason
            ),
        );
        return Ok(PruneOutcome::WouldPrune {
            job_id: job.job_id.clone(),
            status,
            age: age_str,
            items: will_remove,
            reason: reason.to_string(),
        });
    }

    // Actual deletions:
    let mut actually_removed = Vec::new();

    // 1. Remove worktree (force: false)
    if worktree_exists {
        if job.external_worktree {
            emit(
                false,
                format!(
                    "Keeping externally supplied worktree {} (job '{}' was started with --from-worktree)",
                    job.worktree_path.display(),
                    job.job_id
                ),
            );
        } else {
            match remove_worktree(&repo_dir, &job.worktree_path, false) {
                Ok(()) => {
                    emit(
                        false,
                        format!(
                            "Removed worktree for job '{}' at {}",
                            job.job_id,
                            job.worktree_path.display()
                        ),
                    );
                    actually_removed.push("worktree".to_string());
                }
                Err(e) => {
                    emit(
                        true,
                        format!("Warning: git worktree remove returned error: {}", e),
                    );
                    // A failed `git worktree remove` usually means the
                    // worktree's own bookkeeping is broken, so a blind
                    // remove_dir_all here would destroy whatever files the
                    // agent (or a user) left behind. Only sweep the leftover
                    // shell when it holds nothing but git bookkeeping (or
                    // nothing at all). gc has no force flag, so a worktree
                    // that must be kept also keeps its record, branch and
                    // log: it is retried on a later gc run.
                    match crate::core::git::worktree_dir_can_be_swept(&job.worktree_path, 20) {
                        Ok(()) => {
                            let _ = fs::remove_dir_all(&job.worktree_path);
                            emit(
                                false,
                                format!(
                                    "Removed worktree for job '{}' at {}",
                                    job.job_id,
                                    job.worktree_path.display()
                                ),
                            );
                            actually_removed.push("worktree".to_string());
                        }
                        Err(entries) => {
                            emit(
                                false,
                                format!(
                                    "Kept git worktree for job '{}' at {} - it still contains files that are not git bookkeeping (up to 20 listed): {}. The directory was NOT deleted; once its contents are no longer needed, remove it manually:",
                                    job.job_id,
                                    job.worktree_path.display(),
                                    entries.join(", ")
                                ),
                            );
                            emit(false, format!("  rm -rf '{}'", job.worktree_path.display()));
                            return Ok(PruneOutcome::Skipped {
                                job_id: job.job_id.clone(),
                                reason: "worktree kept".to_string(),
                                detail: format!(
                                    "worktree at {} kept: {}",
                                    job.worktree_path.display(),
                                    entries.join(", ")
                                ),
                            });
                        }
                    }
                }
            }
        }
    }

    // 2. Delete branch
    if branch_exists {
        prune_stale_worktree_registrations(&repo_dir);
        match delete_branch(&repo_dir, &job.branch) {
            Ok(()) => {
                emit(
                    false,
                    format!(
                        "Removed git branch '{}' for job '{}'",
                        job.branch, job.job_id
                    ),
                );
                actually_removed.push("branch".to_string());
            }
            Err(e) => {
                emit(
                    true,
                    format!(
                        "Warning: failed to delete git branch '{}': {}",
                        job.branch, e
                    ),
                );
            }
        }
    }

    // 3. Conversation artifacts
    if let Some(conversation_id) = job.conversation_id.as_deref() {
        match crate::core::agy_cleanup::default_antigravity_dir() {
            Some(root) => {
                crate::core::agy_cleanup::cleanup_conversation(&root, conversation_id);
                actually_removed.push("conversation".to_string());
            }
            None => {
                emit(
                    true,
                    "Warning: could not resolve home directory; skipping agy conversation cleanup"
                        .to_string(),
                );
            }
        }
    }

    // 4. Delete job record
    match JobMetadata::delete(&config.state_root, &job.job_id) {
        Ok(()) => {
            emit(false, format!("Removed job record for '{}'", job.job_id));
            actually_removed.push("record".to_string());
        }
        Err(e) => {
            emit(
                true,
                format!(
                    "Warning: failed to delete job record '{}': {}",
                    job.job_id, e
                ),
            );
        }
    }

    // 5. Delete log if eligible
    if should_delete_log {
        match fs::remove_file(&log_path) {
            Ok(()) => {
                emit(
                    false,
                    format!(
                        "Removed log file for job '{}' at {}",
                        job.job_id,
                        log_path.display()
                    ),
                );
                actually_removed.push("log".to_string());
            }
            Err(e) => {
                emit(
                    true,
                    format!(
                        "Warning: failed to remove log file for job '{}' at {}: {}",
                        job.job_id,
                        log_path.display(),
                        e
                    ),
                );
            }
        }
    } else if log_exists {
        emit(
            false,
            format!(
                "Retained log file for job '{}' at {} (status: {}, purge_logs: {})",
                job.job_id,
                log_path.display(),
                status,
                opts.purge_logs
            ),
        );
    }

    // One line per deletion: (job id, status, age, what was removed)
    emit(
        false,
        format!(
            "Removed job '{}' (status: {}, age: {}, removed: {})",
            job.job_id,
            status,
            age_str,
            actually_removed.join(", ")
        ),
    );

    Ok(PruneOutcome::Pruned {
        job_id: job.job_id.clone(),
        status,
        age: age_str,
        removed: actually_removed,
    })
}

/// Removes orphaned job log files under `<state_root>/logs/`: `<job_id>.log`
/// files for which no job record exists in the state database any more (e.g.
/// the record was removed out-of-band, or a prune was interrupted between
/// deleting the record and the log). Returns the number of files removed.
///
/// Only `<job_id>.log` files are touched; anything else in the log directory is
/// left alone.
pub fn prune_orphan_logs(config: &Config) -> Result<u32> {
    let mut emit = |stderr: bool, line: String| {
        if stderr {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    };
    prune_orphan_logs_to(config, None, false, &mut emit)
}

/// Collected variant of [`prune_orphan_logs`].
/// Aborts the sweep (delete nothing, warn) if the job table is empty while log
/// files exist or if the database could not be read; only deletes orphan logs
/// whose mtime is older than 24h and older than the GC age cutoff; honours dry-run.
///
/// Returns the number of orphan log files removed, or, in dry-run mode (where
/// nothing is deleted), the number the sweep would have removed -- so callers
/// can still report the dry-run summary accurately.
pub fn prune_orphan_logs_to(
    config: &Config,
    cutoff_time: Option<chrono::DateTime<Utc>>,
    dry_run: bool,
    emit: &mut dyn FnMut(bool, String),
) -> Result<u32> {
    let log_dir = JobMetadata::log_dir(&config.state_root);
    if !log_dir.exists() {
        return Ok(0);
    }

    // 1. Read job table; abort if database could not be read
    let jobs = match list_jobs(&config.state_root, None) {
        Ok(j) => j,
        Err(e) => {
            emit(
                true,
                format!(
                    "Warning: aborting orphan log sweep: failed to read job database: {}",
                    e
                ),
            );
            return Ok(0);
        }
    };

    // 2. Read entries in log directory
    let dir_entries = match fs::read_dir(&log_dir) {
        Ok(entries) => entries,
        Err(e) => {
            emit(
                true,
                format!(
                    "Warning: failed to read log directory {}: {}",
                    log_dir.display(),
                    e
                ),
            );
            return Ok(0);
        }
    };

    let mut log_entries = Vec::new();
    for entry in dir_entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(job_id) = name.strip_suffix(".log") {
            log_entries.push((job_id.to_string(), entry));
        }
    }

    // 3. Abort the sweep (delete nothing, warn) if the job table is empty while log files exist
    if jobs.is_empty() && !log_entries.is_empty() {
        emit(
            true,
            "Warning: aborting orphan log sweep: job table is empty while log files exist (state database may be uninitialized or corrupted).".to_string(),
        );
        return Ok(0);
    }

    let tracked: HashSet<String> = jobs.into_iter().map(|j| j.job_id).collect();
    let now = Utc::now();
    let age_cutoff =
        cutoff_time.unwrap_or_else(|| now - Duration::days(config.gc_max_age_days as i64));
    let mut removed = 0u32;

    for (job_id, entry) in log_entries {
        if tracked.contains(&job_id) {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                emit(
                    true,
                    format!(
                        "Warning: failed to read metadata for log {}: {}",
                        entry.path().display(),
                        e
                    ),
                );
                continue;
            }
        };

        let mtime = match metadata.modified() {
            Ok(t) => chrono::DateTime::<Utc>::from(t),
            Err(e) => {
                emit(
                    true,
                    format!(
                        "Warning: failed to read mtime for log {}: {}",
                        entry.path().display(),
                        e
                    ),
                );
                continue;
            }
        };

        // Only delete orphan logs whose mtime is older than 24h and older than the GC age cutoff
        let older_than_24h = now - mtime > Duration::hours(24);
        let older_than_cutoff = mtime < age_cutoff;
        if !older_than_24h || !older_than_cutoff {
            continue;
        }

        if dry_run {
            emit(
                false,
                format!(
                    "[dry-run] Would remove orphaned log for unknown job '{}' ({}).",
                    job_id,
                    entry.path().display()
                ),
            );
            removed += 1;
        } else {
            match fs::remove_file(entry.path()) {
                Ok(()) => {
                    emit(
                        false,
                        format!(
                            "Removed orphaned log for unknown job '{}' ({}).",
                            job_id,
                            entry.path().display()
                        ),
                    );
                    removed += 1;
                }
                Err(e) => {
                    emit(
                        true,
                        format!(
                            "Warning: failed to remove orphaned log {}: {}",
                            entry.path().display(),
                            e
                        ),
                    );
                }
            }
        }
    }

    Ok(removed)
}

/// A job that `start` has just inserted (or a retry that `agent-run retry`
/// has just re-claimed) has no `pid` yet; `compute_status` then classifies
/// it from the empty log / missing exit code, ending up terminal
/// (e.g. `SelfTerminated`). Pruning such a row would destroy the state of a
/// still-starting job, so:
/// - any row with no pid and status `running` is protected regardless of age
///   (a freshly claimed retry has its pid cleared but a STALE created_at, so
///   the age-based check alone does not cover it; while the monitor has not
///   written the new pid the job may not be treated as terminal at all), and
/// - any other pidless job younger than 2 minutes is protected as well.
pub fn is_protected_just_inserted(job: &JobMetadata, now: chrono::DateTime<Utc>) -> bool {
    if job.pid.is_none() && job.status == Some(JobStatus::Running) {
        return true;
    }
    job.pid.is_none() && now - job.created_at < Duration::minutes(2)
}

pub fn run(config: &Config, args: &GcArgs) -> Result<CommandOutput> {
    let cutoff_duration = if let Some(raw) = &args.older_than {
        parse_duration_string(raw)?
    } else {
        Duration::days(config.gc_max_age_days as i64)
    };

    let cutoff_time = Utc::now() - cutoff_duration;
    let jobs = list_jobs(&config.state_root, args.project.as_deref())?;

    // 1. Filter to terminal jobs and sort ascending by created_at (oldest first)
    let now = Utc::now();
    let mut terminal_jobs: Vec<(JobMetadata, JobStatus)> = Vec::new();
    for job in jobs {
        if is_protected_just_inserted(&job, now) {
            continue;
        }
        let status = compute_status(&job, &config.state_root);
        if status.is_terminal() {
            terminal_jobs.push((job, status));
        }
    }
    terminal_jobs.sort_by_key(|(j, _)| j.created_at);

    // 2. Determine jobs to prune (job_id -> reason)
    let mut to_prune: BTreeMap<String, String> = BTreeMap::new();

    // Mode A: Age-based pruning
    for (job, _) in &terminal_jobs {
        if job.created_at < cutoff_time {
            to_prune.insert(job.job_id.clone(), "older than age cutoff".to_string());
        }
    }

    // Mode B: Max-jobs pruning (optional, purely additive)
    // --max-jobs eviction must never evict a job younger than 24h
    // (new constant GC_MIN_AGE_FOR_MAX_JOBS_HOURS=24) and never evicts non-terminal jobs
    if let Some(max_jobs) = args.max_jobs {
        let max_jobs_min_age_cutoff = now - Duration::hours(GC_MIN_AGE_FOR_MAX_JOBS_HOURS);
        let eligible: Vec<&JobMetadata> = terminal_jobs
            .iter()
            .map(|(j, _)| j)
            .filter(|j| {
                !to_prune.contains_key(&j.job_id) && j.created_at <= max_jobs_min_age_cutoff
            })
            .collect();

        let total_remaining_count = terminal_jobs
            .iter()
            .filter(|(j, _)| !to_prune.contains_key(&j.job_id))
            .count();

        if total_remaining_count > max_jobs {
            let excess = total_remaining_count - max_jobs;
            for job in eligible.iter().take(excess) {
                to_prune.insert(job.job_id.clone(), "max-jobs eviction".to_string());
            }
        }
    }

    let mut output = CommandOutput::new(json!({
        "removed_jobs": 0,
        "removed_orphan_logs": 0,
        "skipped_jobs": 0,
        "dry_run": args.dry_run,
    }));

    let mut removed_count = 0;
    let mut would_remove_count = 0;
    let mut skipped_counts: BTreeMap<String, usize> = BTreeMap::new();

    for (job, status) in &terminal_jobs {
        if let Some(reason) = to_prune.get(&job.job_id) {
            let opts = PruneOptions {
                dry_run: args.dry_run,
                purge_logs: args.purge_logs,
                cutoff_time: Some(cutoff_time),
                prune_reason: Some(reason.clone()),
            };

            let mut emit = |stderr: bool, line: String| {
                if stderr {
                    output.push_err(line);
                } else {
                    output.push_out(line);
                }
            };

            match prune_single_job_opts_to(config, job, *status, &opts, &mut emit)? {
                PruneOutcome::Pruned { .. } => {
                    removed_count += 1;
                }
                PruneOutcome::WouldPrune { .. } => {
                    would_remove_count += 1;
                }
                PruneOutcome::Skipped { reason, .. } => {
                    *skipped_counts.entry(reason).or_insert(0) += 1;
                }
            }
        }
    }

    // 3. Sweep orphaned log files (no matching job record in the state db).
    let mut sweep_emit = |stderr: bool, line: String| {
        if stderr {
            output.push_err(line);
        } else {
            output.push_out(line);
        }
    };
    let removed_orphan_logs =
        match prune_orphan_logs_to(config, Some(cutoff_time), args.dry_run, &mut sweep_emit) {
            Ok(0) => 0,
            Ok(n) => {
                if args.dry_run {
                    output.push_out(format!(
                        "[dry-run] Orphaned log sweep: would remove {} orphaned log file(s).",
                        n
                    ));
                } else {
                    output.push_out(format!("GC complete: removed {} orphaned log file(s).", n));
                }
                // In dry-run nothing was actually deleted, so the structured
                // result reports zero removed files (the count above is the
                // "would remove" number).
                if args.dry_run {
                    0
                } else {
                    n
                }
            }
            Err(e) => {
                output.push_err(format!("Warning: orphaned log sweep failed: {}", e));
                0
            }
        };

    let total_skipped: usize = skipped_counts.values().sum();
    let skipped_summary = if skipped_counts.is_empty() {
        "0 skipped".to_string()
    } else {
        let parts: Vec<String> = skipped_counts
            .iter()
            .map(|(reason, count)| format!("{} {}", count, reason))
            .collect();
        format!("{} skipped ({})", total_skipped, parts.join(", "))
    };

    if args.dry_run {
        output.push_out(format!(
            "[dry-run] GC complete: 0 removed ({} would be removed), {}.",
            would_remove_count, skipped_summary
        ));
    } else {
        output.push_out(format!(
            "GC complete: {} removed, {}.",
            removed_count, skipped_summary
        ));
    }

    output.data = json!({
        "removed_jobs": removed_count,
        "removed_orphan_logs": removed_orphan_logs,
        "skipped_jobs": total_skipped,
        "dry_run": args.dry_run,
    });
    Ok(output)
}

/// Parses strings like "14d", "48h", "30m", or raw number of days like "14".
pub fn parse_duration_string(raw: &str) -> Result<Duration> {
    let s = raw.trim();

    fn negative(raw: &str) -> anyhow::Error {
        crate::core::error::AgentRunError::invalid(
            "duration_negative",
            format!("Duration cannot be negative: '{}'", raw),
        )
        .into()
    }

    // Plain integer -> days
    if let Ok(days) = s.parse::<i64>() {
        if days < 0 {
            return Err(negative(raw));
        }
        return Ok(Duration::days(days));
    }

    if let Some(num_str) = s.strip_suffix('d') {
        if let Ok(days) = num_str.parse::<i64>() {
            if days < 0 {
                return Err(negative(raw));
            }
            return Ok(Duration::days(days));
        }
    }

    if let Some(num_str) = s.strip_suffix('h') {
        if let Ok(hours) = num_str.parse::<i64>() {
            if hours < 0 {
                return Err(negative(raw));
            }
            return Ok(Duration::hours(hours));
        }
    }

    if let Some(num_str) = s.strip_suffix('m') {
        if let Ok(mins) = num_str.parse::<i64>() {
            if mins < 0 {
                return Err(negative(raw));
            }
            return Ok(Duration::minutes(mins));
        }
    }

    if let Ok(std_dur) = humantime::parse_duration(s) {
        if let Ok(chrono_dur) = Duration::from_std(std_dur) {
            return Ok(chrono_dur);
        }
    }

    Err(crate::core::error::AgentRunError::invalid(
        "invalid_duration",
        format!(
            "Invalid duration format '{}'. Expected e.g. '14d', '48h', or number of days.",
            raw
        ),
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_parse_duration_string() {
        assert_eq!(parse_duration_string("14d").unwrap(), Duration::days(14));
        assert_eq!(parse_duration_string("7").unwrap(), Duration::days(7));
        assert_eq!(parse_duration_string("48h").unwrap(), Duration::hours(48));
        assert_eq!(
            parse_duration_string("120m").unwrap(),
            Duration::minutes(120)
        );
    }

    #[test]
    fn test_parse_duration_string_rejects_negative() {
        assert!(parse_duration_string("-5d").is_err());
        assert!(parse_duration_string("-10").is_err());
        assert!(parse_duration_string("-2h").is_err());
    }

    fn make_test_job(
        job_id: &str,
        project: &str,
        created_at: chrono::DateTime<Utc>,
        status: JobStatus,
    ) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            project: project.to_string(),
            prompt: "test prompt".to_string(),
            resolved_prompt: "test prompt".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at,
            worktree_path: PathBuf::from(format!("/fake/worktree/{}", job_id)),
            branch: format!("agent-run/{}", job_id),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(0),
            status: Some(status),
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
        }
    }

    #[test]
    fn test_prune_orphan_logs_removes_only_untracked() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            ..Default::default()
        };

        let log_dir = JobMetadata::log_dir(&state_dir);
        fs::create_dir_all(&log_dir).unwrap();

        // Tracked job with a log file.
        let tracked = make_test_job("tracked0", "p", Utc::now(), JobStatus::Done);
        tracked.save(&state_dir).unwrap();
        let tracked_log = JobMetadata::log_path(&state_dir, "tracked0");
        fs::write(&tracked_log, "tracked log").unwrap();

        // Orphaned log (no job record).
        let orphan_path = JobMetadata::log_path(&state_dir, "orphan01");
        fs::write(&orphan_path, "orphan log").unwrap();
        let file = fs::File::options().write(true).open(&orphan_path).unwrap();
        file.set_modified(
            std::time::SystemTime::now() - std::time::Duration::from_secs(15 * 86400),
        )
        .unwrap();

        // Non-<job_id>.log file in the same directory: must be untouched.
        fs::write(log_dir.join("notes.txt"), "not a job log").unwrap();

        let removed = prune_orphan_logs(&config).unwrap();
        assert_eq!(removed, 1);
        assert!(
            !JobMetadata::log_path(&state_dir, "orphan01").exists(),
            "orphaned log must be removed"
        );
        assert!(tracked_log.exists(), "log of a tracked job must be kept");
        assert!(
            log_dir.join("notes.txt").exists(),
            "non-job files in the log dir must be untouched"
        );
    }

    #[test]
    fn test_gc_execute_sweeps_orphan_logs() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&ws_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            ..Default::default()
        };

        // An old, genuinely Done terminal job (prunable), a fresh tracked
        // keeper job (keeps the job table non-empty so the post-prune orphan
        // sweep does not abort), and two orphaned logs: one backdated past
        // both the 24h and the GC cutoffs (swept) and one fresh (kept).
        let now = Utc::now();
        let old = make_test_job(
            "oldjob01",
            "p",
            now - chrono::Duration::days(3),
            JobStatus::Done,
        );
        old.save(&state_dir).unwrap();
        fs::create_dir_all(JobMetadata::log_dir(&state_dir)).unwrap();
        // A structured, complete SUCCESS is what `compute_status` classifies
        // as `Done` (a plain-text log with exit 0 would be `Incomplete`, and
        // the Done branch is exactly what removes a pruned job's log).
        fs::write(
            JobMetadata::log_path(&state_dir, "oldjob01"),
            r#"{"status":"SUCCESS","response":"done"}"#,
        )
        .unwrap();
        // Fresh tracked job: younger than the 1h cutoff, so it is NOT pruned
        // and the table stays non-empty when the orphan sweep runs.
        let keeper = make_test_job("keeperjob", "p", now, JobStatus::Done);
        keeper.save(&state_dir).unwrap();

        let ghost_path = JobMetadata::log_path(&state_dir, "ghostjob");
        fs::write(&ghost_path, "orphan").unwrap();
        let file = fs::File::options().write(true).open(&ghost_path).unwrap();
        file.set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(2 * 86400))
            .unwrap();
        // A freshly written orphan log: mtime = now, so it is younger than
        // 24h and must SURVIVE the sweep.
        let fresh_path = JobMetadata::log_path(&state_dir, "freshjob");
        fs::write(&fresh_path, "fresh orphan").unwrap();

        let args = GcArgs {
            project: None,
            older_than: Some("1h".to_string()),
            max_jobs: None,
            dry_run: false,
            purge_logs: false,
        };
        run(&config, &args).unwrap();

        assert!(
            JobMetadata::load(&state_dir, "oldjob01").is_err(),
            "old terminal job must be pruned"
        );
        assert!(
            !JobMetadata::log_path(&state_dir, "oldjob01").exists(),
            "a Done job's log is removed when the job is pruned"
        );
        assert!(
            JobMetadata::load(&state_dir, "keeperjob").is_ok(),
            "the fresh keeper job must not be pruned"
        );
        assert!(
            !JobMetadata::log_path(&state_dir, "ghostjob").exists(),
            "orphaned log must be swept by gc"
        );
        assert!(
            JobMetadata::log_path(&state_dir, "freshjob").exists(),
            "a fresh (younger than 24h) orphan log must be kept"
        );
    }

    #[test]
    fn test_prune_single_job_keeps_external_worktree() {
        let temp_dir = tempdir().unwrap();
        let ws_dir = temp_dir.path().join("ws");
        let state_dir = temp_dir.path().join("state");
        fs::create_dir_all(&ws_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        // A real worktree that belongs to the job (external flag set).
        let repo_dir = ws_dir.join("myrepo");
        fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        run_cmd(&["add", "file.txt"]);
        run_cmd(&["commit", "-m", "initial commit"]);
        crate::core::git::create_worktree(
            &repo_dir,
            &ws_dir.join("ext_wt"),
            "agent-run/extjob",
            "main",
        )
        .unwrap();

        let job = JobMetadata {
            worktree_path: ws_dir.join("ext_wt"),
            branch: "agent-run/extjob".to_string(),
            external_worktree: true,
            ..make_test_job("extjob", "myrepo", Utc::now(), JobStatus::Done)
        };
        job.save(&state_dir).unwrap();

        prune_single_job(&config, &job, JobStatus::Done).unwrap();

        assert!(
            job.worktree_path.exists(),
            "external worktree must NOT be removed by prune"
        );
        // The job's own record and log are still cleaned up.
        assert!(JobMetadata::load(&state_dir, "extjob").is_err());
        let branch = std::process::Command::new("git")
            .current_dir(&repo_dir)
            .args(["branch", "--list", "agent-run/extjob"])
            .output()
            .unwrap();
        assert!(
            !String::from_utf8_lossy(&branch.stdout).trim().is_empty(),
            "pre-existing branch of an external worktree must NOT be deleted"
        );
    }

    #[test]
    fn test_prune_single_job_keeps_worktree_with_ignored_file_when_git_remove_fails() {
        let temp_dir = tempdir().unwrap();
        let ws_dir = temp_dir.path().join("ws");
        let state_dir = temp_dir.path().join("state");
        fs::create_dir_all(&ws_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        let repo_dir = ws_dir.join("myrepo");
        fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        std::fs::write(repo_dir.join(".gitignore"), "ignored.dat\n").unwrap();
        run_cmd(&["add", "file.txt", ".gitignore"]);
        run_cmd(&["commit", "-m", "initial commit"]);
        let wt = ws_dir.join("kept_wt");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/keptjob", "main").unwrap();

        // An ignored file the agent produced in the worktree. It is invisible
        // to `git status --porcelain` (matched by the committed .gitignore), so
        // the pre-prune dirty check passes and the job reaches the removal path.
        std::fs::write(wt.join("ignored.dat"), "precious user data").unwrap();
        // Lock the worktree: `git worktree remove` now fails for this unrelated
        // reason (a single --force does not override a lock), while `git status`
        // still works clean, so the fallback sweep must KEEP the directory -- and
        // with it the agent's ignored data file -- rather than shred it.
        let lock = std::process::Command::new("git")
            .current_dir(&repo_dir)
            .args(["worktree", "lock", wt.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(lock.success(), "sanity: worktree must be lockable");

        let job = JobMetadata {
            worktree_path: wt.clone(),
            branch: "agent-run/keptjob".to_string(),
            ..make_test_job("keptjob", "myrepo", Utc::now(), JobStatus::Done)
        };
        job.save(&state_dir).unwrap();
        let log_path = JobMetadata::log_path(&state_dir, "keptjob");
        fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        fs::write(&log_path, "kept job log").unwrap();

        let mut out = Vec::new();
        let opts = PruneOptions::default();
        let outcome = prune_single_job_opts_to(
            &config,
            &job,
            JobStatus::Done,
            &opts,
            &mut |stderr, line| out.push((stderr, line)),
        )
        .unwrap();

        match outcome {
            PruneOutcome::Skipped { reason, .. } => {
                assert_eq!(reason, "worktree kept");
            }
            other => panic!("expected the job to be kept, got: {:?}", other),
        }
        assert!(
            wt.exists(),
            "worktree with git-ignored files must NOT be deleted when git worktree remove fails"
        );
        assert!(
            wt.join("ignored.dat").exists(),
            "git-ignored file must survive a failed worktree removal"
        );
        let listed = out
            .iter()
            .any(|(_, l)| l.contains("Kept git worktree for job 'keptjob'"));
        assert!(listed, "must explain the worktree was kept: {:?}", out);
        let listed_entry = out.iter().any(|(_, l)| l.contains("ignored.dat"));
        assert!(
            listed_entry,
            "kept warning must list the file that was not deleted: {:?}",
            out
        );
        assert!(
            JobMetadata::load(&state_dir, "keptjob").is_ok(),
            "job record must be kept so the worktree is not orphaned"
        );
        assert!(
            log_path.exists(),
            "the job's log must be retained while the worktree is kept"
        );
        assert!(
            git_ref_list(&repo_dir, "agent-run/keptjob"),
            "the branch must remain while the worktree it checks out is kept"
        );
    }

    fn git_ref_list(repo_dir: &std::path::Path, name: &str) -> bool {
        let out = std::process::Command::new("git")
            .current_dir(repo_dir)
            .args(["branch", "--list", name])
            .output()
            .unwrap();
        !String::from_utf8_lossy(&out.stdout).trim().is_empty()
    }

    #[test]
    fn test_prune_single_job_sweeps_empty_leftover_when_git_remove_fails() {
        let temp_dir = tempdir().unwrap();
        let ws_dir = temp_dir.path().join("ws");
        let state_dir = temp_dir.path().join("state");
        fs::create_dir_all(&ws_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        let repo_dir = ws_dir.join("myrepo");
        fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        // An empty commit: the worktree checks out NOTHING, so the only entry it
        // ever holds is the worktree's own `.git` pointer file -- i.e. git
        // bookkeeping only, safe to sweep.
        run_cmd(&["commit", "-q", "--allow-empty", "-m", "initial commit"]);
        let wt = ws_dir.join("shell_wt");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/shelljob", "main").unwrap();
        assert_eq!(
            fs::read_dir(&wt)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            vec![".git".to_string()],
            "precondition: the worktree holds only its .git pointer"
        );
        // Lock the worktree: `git worktree remove` now fails (a locked worktree
        // is never removed, even with --force) while `git status --porcelain`
        // still reports clean, so the job passes the step-1 dirt check and
        // reaches the failed-removal fallback. The leftover shell holds only
        // the worktree's `.git` pointer -- git bookkeeping -- so the sweep may
        // remove it. Deleting `.git` would not work here: `git status` (the
        // step-1 dirt check) would then fail, and gc would skip the job as
        // "dirty" before ever reaching the removal path.
        let lock = std::process::Command::new("git")
            .current_dir(&repo_dir)
            .args(["worktree", "lock", wt.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(lock.success(), "sanity: worktree must be lockable");

        let job = JobMetadata {
            worktree_path: wt.clone(),
            branch: "agent-run/shelljob".to_string(),
            ..make_test_job("shelljob", "myrepo", Utc::now(), JobStatus::Done)
        };
        job.save(&state_dir).unwrap();
        let log_path = JobMetadata::log_path(&state_dir, "shelljob");
        fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        fs::write(&log_path, "shell job log").unwrap();

        let mut out = Vec::new();
        let opts = PruneOptions::default();
        let outcome = prune_single_job_opts_to(
            &config,
            &job,
            JobStatus::Done,
            &opts,
            &mut |stderr, line| out.push((stderr, line)),
        )
        .unwrap();

        match outcome {
            PruneOutcome::Pruned { removed, .. } => {
                assert!(
                    removed.contains(&"worktree".to_string()),
                    "the empty leftover shell must be counted as removed: {:?}",
                    out
                );
                assert!(
                    removed.contains(&"record".to_string()),
                    "record removed: {:?}",
                    out
                );
                assert!(
                    removed.contains(&"log".to_string()),
                    "Done job log swept: {:?}",
                    out
                );
            }
            other => panic!("expected the job to be pruned, got: {:?}", other),
        }
        assert!(
            !wt.exists(),
            "empty leftover shell (tracked checkout only) must be swept away"
        );
        assert!(
            JobMetadata::load(&state_dir, "shelljob").is_err(),
            "record pruned"
        );
        assert!(!log_path.exists(), "Done job log pruned");
    }

    #[test]
    fn test_gc_age_based_pruning() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        let now = Utc::now();
        let old_job = make_test_job("old1", "proj1", now - Duration::days(20), JobStatus::Done);
        let fresh_job = make_test_job("fresh1", "proj1", now - Duration::days(5), JobStatus::Done);

        old_job.save(&state_dir).unwrap();
        fresh_job.save(&state_dir).unwrap();

        let args = GcArgs {
            older_than: None,
            project: None,
            max_jobs: None,
            dry_run: false,
            purge_logs: false,
        };

        run(&config, &args).unwrap();

        let remaining = list_jobs(&state_dir, None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].job_id, "fresh1");
    }

    #[test]
    fn test_prune_single_job_skips_when_row_changed_concurrently() {
        let temp_dir = tempdir().unwrap();
        let ws_dir = temp_dir.path().join("ws");
        let state_dir = temp_dir.path().join("state");
        fs::create_dir_all(&ws_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        // Snapshot as `gc::run` would: a finished job (fake worktree path,
        // which exists on disk so a buggy prune would destroy it).
        let mut job = make_test_job("racejob1", "proj1", Utc::now(), JobStatus::Done);
        let log_dir = JobMetadata::log_dir(&state_dir);
        fs::create_dir_all(&log_dir).unwrap();
        job.worktree_path = ws_dir.join("racejob1");
        fs::create_dir_all(&job.worktree_path).unwrap();
        fs::write(JobMetadata::log_path(&state_dir, "racejob1"), "log").unwrap();
        job.save(&state_dir).unwrap();

        // Simulate a concurrent `retry` landing between the snapshot and the
        // prune: same row, retry_count bumped, and a live process recorded
        // (matching start time) so compute_status reports Running.
        let pid = std::process::id();
        let mut fresh = JobMetadata::load(&state_dir, "racejob1").unwrap();
        fresh.retry_count += 1;
        fresh.status = Some(JobStatus::Running);
        fresh.pid = Some(pid);
        fresh.pid_start_time = crate::core::job::get_process_start_time(pid);
        fresh.exit_code = None;
        fresh.save(&state_dir).unwrap();
        assert_eq!(fresh.retry_count, 1);

        let mut out_lines: Vec<String> = Vec::new();
        let mut err_lines: Vec<String> = Vec::new();
        let mut emit = |stderr: bool, line: String| {
            if stderr {
                err_lines.push(line);
            } else {
                out_lines.push(line);
            }
        };
        prune_single_job_to(&config, &job, JobStatus::Done, &mut emit).unwrap();

        assert!(
            out_lines
                .iter()
                .any(|l| l.contains("Skipping job 'racejob1': state changed concurrently")),
            "prune must report the concurrent-state skip: {out_lines:?} / {err_lines:?}"
        );
        assert!(
            job.worktree_path.exists(),
            "worktree of a concurrently retried job must NOT be removed"
        );
        assert!(
            JobMetadata::log_path(&state_dir, "racejob1").exists(),
            "log of a concurrently retried job must NOT be removed"
        );
        let row = JobMetadata::load(&state_dir, "racejob1").unwrap();
        assert_eq!(
            row.retry_count, 1,
            "the retried job row must be left untouched"
        );
    }

    #[test]
    fn test_prune_single_job_deletes_branch_with_stale_worktree_registration() {
        let temp_dir = tempdir().unwrap();
        let ws_dir = temp_dir.path().join("ws");
        let state_dir = temp_dir.path().join("state");
        fs::create_dir_all(&ws_dir).unwrap();
        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            ..Default::default()
        };

        // Real repo + real worktree (same fixture as the sibling prune tests).
        let repo_dir = ws_dir.join("myrepo");
        fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |args: &[&str]| {
            std::process::Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command")
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        run_cmd(&["add", "file.txt"]);
        run_cmd(&["commit", "-m", "initial commit"]);
        let wt = ws_dir.join("stale_wt");
        crate::core::git::create_worktree(&repo_dir, &wt, "agent-run/stalejob", "main").unwrap();
        // Remove the worktree directory out-of-band: git keeps the (now
        // stale) registration and refuses `git branch -D` until it is pruned.
        fs::remove_dir_all(&wt).unwrap();
        let list = std::process::Command::new("git")
            .current_dir(&repo_dir)
            .args(["worktree", "list"])
            .output()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&list.stdout).contains(wt.to_str().unwrap()),
            "fixture must leave a stale worktree registration"
        );
        // Sanity: without the prune, git refuses the branch delete.
        delete_branch(&repo_dir, "agent-run/stalejob").unwrap_err();

        let job = JobMetadata {
            worktree_path: wt,
            branch: "agent-run/stalejob".to_string(),
            ..make_test_job("stalejob", "myrepo", Utc::now(), JobStatus::Done)
        };
        job.save(&state_dir).unwrap();

        prune_single_job(&config, &job, JobStatus::Done).unwrap();

        let branch = std::process::Command::new("git")
            .current_dir(&repo_dir)
            .args(["branch", "--list", "agent-run/stalejob"])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&branch.stdout).trim(),
            "",
            "branch must be deleted even when only a stale worktree registration of it remains"
        );
        assert!(
            JobMetadata::load(&state_dir, "stalejob").is_err(),
            "job record must be deleted"
        );
    }

    #[test]
    fn test_gc_max_jobs_pruning() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        // Create N+k jobs with varying ages (N=3, k=2, total=5)
        // All jobs are within gc_max_age_days (1-5 hours old)
        let now = Utc::now();
        let n = 3;
        let k = 2;
        let total = n + k;
        for i in 0..total {
            let job_id = format!("job{}", i);
            // i=0 is oldest (29 hours ago), i=4 is newest (25 hours ago)
            let created = now - Duration::hours(24 + (total - i) as i64);
            let job = make_test_job(&job_id, "proj1", created, JobStatus::Done);
            job.save(&state_dir).unwrap();
        }

        let initial_jobs = list_jobs(&state_dir, None).unwrap();
        assert_eq!(initial_jobs.len(), 5);

        // Run gc with --max-jobs 3
        let args = GcArgs {
            older_than: None,
            project: None,
            max_jobs: Some(n),
            dry_run: false,
            purge_logs: false,
        };

        run(&config, &args).unwrap();

        // Exactly the oldest k=2 jobs (job0, job1) must be pruned,
        // and exactly the newest N=3 jobs (job2, job3, job4) must remain.
        let remaining = list_jobs(&state_dir, None).unwrap();
        assert_eq!(remaining.len(), 3);
        let remaining_ids: Vec<String> = remaining.into_iter().map(|j| j.job_id).collect();
        assert!(
            !remaining_ids.contains(&"job0".to_string()),
            "job0 should have been pruned"
        );
        assert!(
            !remaining_ids.contains(&"job1".to_string()),
            "job1 should have been pruned"
        );
        assert!(
            remaining_ids.contains(&"job2".to_string()),
            "job2 should be retained"
        );
        assert!(
            remaining_ids.contains(&"job3".to_string()),
            "job3 should be retained"
        );
        assert!(
            remaining_ids.contains(&"job4".to_string()),
            "job4 should be retained"
        );
    }

    #[test]
    fn test_gc_does_not_prune_running_jobs() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        let now = Utc::now();
        // A running job with current process PID is definitely running
        let mut running_job = make_test_job(
            "running1",
            "proj1",
            now - Duration::days(30),
            JobStatus::Running,
        );
        running_job.pid = Some(std::process::id());
        running_job.exit_code = None;
        running_job.save(&state_dir).unwrap();

        let args = GcArgs {
            older_than: None,
            project: None,
            max_jobs: Some(0),
            dry_run: false,
            purge_logs: false,
        };

        run(&config, &args).unwrap();

        let remaining = list_jobs(&state_dir, None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].job_id, "running1");
    }

    #[test]
    fn test_is_protected_just_inserted() {
        let now = Utc::now();

        // Young job without a pid: protected from pruning.
        let fresh = make_test_job(
            "freshjob",
            "p",
            now - Duration::seconds(30),
            JobStatus::SelfTerminated,
        );
        assert!(fresh.pid.is_none(), "fixture must have no pid");
        assert!(is_protected_just_inserted(&fresh, now));

        // Same age but already launched (pid recorded): not protected.
        let mut spawned = make_test_job(
            "pidjob",
            "p",
            now - Duration::seconds(30),
            JobStatus::SelfTerminated,
        );
        spawned.pid = Some(std::process::id());
        assert!(!is_protected_just_inserted(&spawned, now));

        // Older job without a pid (a genuinely dead leftover): not protected.
        let stale = make_test_job(
            "stalejob",
            "p",
            now - Duration::minutes(5),
            JobStatus::SelfTerminated,
        );
        assert!(!is_protected_just_inserted(&stale, now));

        // A freshly claimed retry: the row was re-marked running with the
        // pid cleared, but created_at is STALE -- the age check alone would
        // let gc prune it while the new attempt starts. Any pidless row
        // with status running must be protected regardless of age.
        let retried = make_test_job(
            "retried1",
            "p",
            now - Duration::hours(6),
            JobStatus::Running,
        );
        assert!(retried.pid.is_none(), "fixture must have no pid");
        assert!(
            is_protected_just_inserted(&retried, now),
            "a retried job (status running, pid cleared, old created_at) must be protected"
        );

        // ...but only while the pid has not been written: once a pid is
        // recorded the row is judged by normal means again.
        let mut spawned_retried = make_test_job(
            "retried2",
            "p",
            now - Duration::hours(6),
            JobStatus::Running,
        );
        spawned_retried.pid = Some(std::process::id());
        assert!(
            !is_protected_just_inserted(&spawned_retried, now),
            "a running job with a recorded pid must not fall under the insert guard"
        );

        // An old pidless row that is NOT marked running stays unprotected,
        // so gc can still prune genuine leftovers.
        let stale_running_status_cleared = make_test_job(
            "stale2",
            "p",
            now - Duration::hours(6),
            JobStatus::SelfTerminated,
        );
        assert!(!is_protected_just_inserted(
            &stale_running_status_cleared,
            now
        ));
    }

    #[test]
    fn test_gc_does_not_prune_just_inserted_pidless_job() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        let now = Utc::now();
        // A job `start` just inserted: no pid, no exit code, no status yet.
        // compute_status calls it terminal (SelfTerminated), so without the
        // age guard an `--older-than 1m` gc would prune it while it launches
        // (90s old: past the 1m age cutoff, still within the 2m guard window).
        let mut fresh = make_test_job(
            "freshjob",
            "proj1",
            now - Duration::seconds(90),
            JobStatus::SelfTerminated,
        );
        fresh.exit_code = None;
        fresh.status = None;
        fresh.save(&state_dir).unwrap();

        // An old, genuinely finished job that must still be pruned.
        let stale = make_test_job(
            "stalejob",
            "proj1",
            now - Duration::hours(5),
            JobStatus::Done,
        );
        stale.save(&state_dir).unwrap();

        let args = GcArgs {
            older_than: Some("1m".to_string()),
            project: None,
            max_jobs: None,
            dry_run: false,
            purge_logs: false,
        };
        run(&config, &args).unwrap();

        let remaining = list_jobs(&state_dir, None).unwrap();
        let remaining_ids: Vec<String> = remaining.iter().map(|j| j.job_id.clone()).collect();
        assert!(
            remaining_ids.contains(&"freshjob".to_string()),
            "a just-inserted pidless job must not be pruned"
        );
        assert!(
            !remaining_ids.contains(&"stalejob".to_string()),
            "the guard must not blunt normal age-based pruning"
        );
    }

    #[test]
    fn test_gc_does_not_prune_retried_job_while_pid_is_waiting() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        let now = Utc::now();
        // The shape `agent-run retry` leaves behind right after claiming the
        // row: status running, pid/exit_code cleared, but created_at is the
        // ORIGINAL creation (hours old) and the log still carries attempt 1's
        // finished output, so compute_status classifies it terminal (Done).
        // Without the pidless-running guard, an age-based gc would prune the
        // job while its new attempt is still starting.
        let mut retried = make_test_job(
            "retried1",
            "proj1",
            now - Duration::hours(6),
            JobStatus::Running,
        );
        retried.pid = None;
        retried.exit_code = None;
        retried.save(&state_dir).unwrap();
        fs::create_dir_all(JobMetadata::log_dir(&state_dir)).unwrap();
        std::fs::write(
            JobMetadata::log_path(&state_dir, "retried1"),
            r#"{"status":"SUCCESS","response":"attempt one done"}"#,
        )
        .unwrap();

        // An old, genuinely finished job that must still be pruned.
        let stale = make_test_job(
            "stalejob",
            "proj1",
            now - Duration::hours(5),
            JobStatus::Done,
        );
        stale.save(&state_dir).unwrap();

        let args = GcArgs {
            older_than: Some("1h".to_string()),
            project: None,
            max_jobs: None,
            dry_run: false,
            purge_logs: false,
        };
        run(&config, &args).unwrap();

        let remaining = list_jobs(&state_dir, None).unwrap();
        let remaining_ids: Vec<String> = remaining.iter().map(|j| j.job_id.clone()).collect();
        assert!(
            remaining_ids.contains(&"retried1".to_string()),
            "a pidless running job with an old created_at must not be pruned while its new attempt starts"
        );
        assert!(
            !remaining_ids.contains(&"stalejob".to_string()),
            "the guard must not blunt normal age-based pruning"
        );
    }

    #[test]
    fn test_gc_dry_run_deletes_nothing() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            gc_max_age_days: 14,
            ..Default::default()
        };

        // Create an old job (15 days old) with a real git worktree and a log.
        // The worktree must be a genuine (clean) git worktree: the pre-prune
        // dirty check runs `git status`, which fails -- and the job is
        // conservatively skipped -- on a plain directory.
        let repo_dir = ws_dir.join("proj1");
        fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .current_dir(&repo_dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&["init", "-b", "main"]);
        run_cmd(&["config", "user.name", "Test"]);
        run_cmd(&["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        run_cmd(&["add", "file.txt"]);
        run_cmd(&["commit", "-m", "initial commit"]);

        let now = Utc::now();
        let old_time = now - Duration::days(15);
        let mut job = make_test_job("dryrunjob", "proj1", old_time, JobStatus::Done);
        job.worktree_path = ws_dir.join("dryrun_wt");
        crate::core::git::create_worktree(
            &repo_dir,
            &job.worktree_path,
            "agent-run/dryrunjob",
            "main",
        )
        .unwrap();
        job.save(&state_dir).unwrap();

        let log_dir = JobMetadata::log_dir(&state_dir);
        fs::create_dir_all(&log_dir).unwrap();
        let log_path = JobMetadata::log_path(&state_dir, "dryrunjob");
        fs::write(&log_path, "log content").unwrap();

        // Also an old orphan log (>24h and >14d)
        let orphan_path = JobMetadata::log_path(&state_dir, "orphan_old");
        fs::write(&orphan_path, "orphan log").unwrap();
        let file = fs::File::options().write(true).open(&orphan_path).unwrap();
        file.set_modified(
            std::time::SystemTime::now() - std::time::Duration::from_secs(15 * 86400),
        )
        .unwrap();

        let args = GcArgs {
            older_than: None,
            project: None,
            max_jobs: None,
            dry_run: true,
            purge_logs: false,
        };

        let out = run(&config, &args).unwrap();

        // Output mentions dry-run
        assert!(out
            .stdout_lines
            .iter()
            .any(|l| l.contains("[dry-run] Would remove job 'dryrunjob'")));
        assert!(out.stdout_lines.iter().any(
            |l| l.contains("[dry-run] Would remove orphaned log for unknown job 'orphan_old'")
        ));
        // The sweep summary reports the would-remove count even though nothing
        // was deleted (and the structured field stays at 0 removed).
        assert!(
            out.stdout_lines.iter().any(|l| l
                .contains("[dry-run] Orphaned log sweep: would remove 1 orphaned log file(s).")),
            "dry-run orphan sweep must report its would-remove summary: {:?}",
            out.stdout_lines
        );

        // Assert NOTHING was deleted
        assert!(
            JobMetadata::load(&state_dir, "dryrunjob").is_ok(),
            "job record must not be deleted"
        );
        assert!(job.worktree_path.exists(), "worktree must not be deleted");
        assert!(log_path.exists(), "log must not be deleted");
        assert!(orphan_path.exists(), "orphan log must not be deleted");
        assert_eq!(out.data["removed_jobs"], 0);
        assert_eq!(out.data["removed_orphan_logs"], 0);
    }

    #[test]
    fn test_gc_skips_dirty_worktree_and_unpushed_branch() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.clone().join(".agent-repos"),
            workspace_root: ws_dir.clone(),
            gc_max_age_days: 14,
            ..Default::default()
        };

        let repo_dir = ws_dir.join("myrepo");
        fs::create_dir_all(&repo_dir).unwrap();
        let run_cmd = |dir: &std::path::Path, args: &[&str]| {
            let status = std::process::Command::new("git")
                .current_dir(dir)
                .args(args)
                .status()
                .expect("execute git command");
            assert!(status.success());
        };
        run_cmd(&repo_dir, &["init", "-b", "main"]);
        run_cmd(&repo_dir, &["config", "user.name", "Test"]);
        run_cmd(&repo_dir, &["config", "user.email", "test@test.com"]);
        std::fs::write(repo_dir.join("file.txt"), "hello").unwrap();
        run_cmd(&repo_dir, &["add", "file.txt"]);
        run_cmd(&repo_dir, &["commit", "-m", "initial commit"]);

        let now = Utc::now();
        let old_time = now - Duration::days(20);

        // Job 1: Dirty worktree (uncommitted file)
        let wt_dirty = ws_dir.join("wt_dirty");
        crate::core::git::create_worktree(&repo_dir, &wt_dirty, "agent-run/j-dirty", "main")
            .unwrap();
        std::fs::write(wt_dirty.join("uncommitted.txt"), "dirty").unwrap();
        let job_dirty = JobMetadata {
            worktree_path: wt_dirty.clone(),
            branch: "agent-run/j-dirty".to_string(),
            base_ref: "main".to_string(),
            ..make_test_job("j-dirty", "myrepo", old_time, JobStatus::Done)
        };
        job_dirty.save(&state_dir).unwrap();

        // Job 2: Unpushed branch (clean worktree, but branch has unpushed commit)
        let wt_unpushed = ws_dir.join("wt_unpushed");
        crate::core::git::create_worktree(&repo_dir, &wt_unpushed, "agent-run/j-unpushed", "main")
            .unwrap();
        std::fs::write(wt_unpushed.join("committed.txt"), "committed").unwrap();
        run_cmd(&wt_unpushed, &["add", "committed.txt"]);
        run_cmd(&wt_unpushed, &["commit", "-m", "committed on branch"]);
        let unpushed_sha = std::process::Command::new("git")
            .current_dir(&wt_unpushed)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let unpushed_sha = String::from_utf8_lossy(&unpushed_sha.stdout)
            .trim()
            .to_string();
        let job_unpushed = JobMetadata {
            worktree_path: wt_unpushed.clone(),
            branch: "agent-run/j-unpushed".to_string(),
            base_ref: "main".to_string(),
            ..make_test_job("j-unpushed", "myrepo", old_time, JobStatus::Done)
        };
        job_unpushed.save(&state_dir).unwrap();

        // Job 3: Completely clean job (should be pruned)
        let wt_clean = ws_dir.join("wt_clean");
        crate::core::git::create_worktree(&repo_dir, &wt_clean, "agent-run/j-clean", "main")
            .unwrap();
        let job_clean = JobMetadata {
            worktree_path: wt_clean.clone(),
            branch: "agent-run/j-clean".to_string(),
            base_ref: "main".to_string(),
            ..make_test_job("j-clean", "myrepo", old_time, JobStatus::Done)
        };
        job_clean.save(&state_dir).unwrap();

        let args = GcArgs {
            older_than: Some("14d".to_string()),
            project: None,
            max_jobs: None,
            dry_run: false,
            purge_logs: false,
        };

        let out = run(&config, &args).unwrap();

        // Job 1 (dirty): skipped
        assert!(wt_dirty.exists(), "dirty worktree must be skipped");
        assert!(
            JobMetadata::load(&state_dir, "j-dirty").is_ok(),
            "dirty job record must be kept"
        );

        // Job 2 (unpushed): skipped and warned with tip sha
        assert!(
            wt_unpushed.exists(),
            "worktree of unpushed job must be skipped"
        );
        assert!(
            JobMetadata::load(&state_dir, "j-unpushed").is_ok(),
            "unpushed job record must be kept"
        );
        assert!(
            out.stderr_lines.iter().any(|l| l.contains(&unpushed_sha)),
            "warning must contain branch tip sha: {:?}",
            out.stderr_lines
        );

        // Job 3 (clean): pruned
        assert!(!wt_clean.exists(), "clean worktree must be removed");
        assert!(
            JobMetadata::load(&state_dir, "j-clean").is_err(),
            "clean job record must be deleted"
        );

        // Summary reports skipped jobs and reasons
        assert!(out
            .stdout_lines
            .iter()
            .any(|l| l.contains("1 removed") && l.contains("2 skipped")));
    }

    #[test]
    fn test_max_jobs_never_evicts_job_younger_than_24h() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        let now = Utc::now();
        // Create 3 jobs that are 5 hours old (<24h)
        for i in 0..3 {
            let job = make_test_job(
                &format!("young{}", i),
                "p",
                now - Duration::hours(5),
                JobStatus::Done,
            );
            job.save(&state_dir).unwrap();
        }

        // Run with max_jobs: Some(1)
        let args = GcArgs {
            older_than: None,
            project: None,
            max_jobs: Some(1),
            dry_run: false,
            purge_logs: false,
        };

        run(&config, &args).unwrap();

        // NONE of the 3 young jobs should have been pruned
        let remaining = list_jobs(&state_dir, None).unwrap();
        assert_eq!(
            remaining.len(),
            3,
            "jobs younger than 24h must never be evicted by max-jobs"
        );
    }

    #[test]
    fn test_orphan_sweep_aborts_on_empty_table() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            ..Default::default()
        };

        let log_dir = JobMetadata::log_dir(&state_dir);
        fs::create_dir_all(&log_dir).unwrap();
        let orphan_log = log_dir.join("orphan.log");
        fs::write(&orphan_log, "some log").unwrap();

        // Set mtime to 30 days ago
        let file = fs::File::options().write(true).open(&orphan_log).unwrap();
        file.set_modified(
            std::time::SystemTime::now() - std::time::Duration::from_secs(30 * 86400),
        )
        .unwrap();

        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut emit = |stderr: bool, line: String| {
            if stderr {
                err.push(line);
            } else {
                out.push(line);
            }
        };

        let removed = prune_orphan_logs_to(&config, None, false, &mut emit).unwrap();
        assert_eq!(
            removed, 0,
            "orphan sweep must delete nothing on empty job table"
        );
        assert!(
            orphan_log.exists(),
            "orphan log must not be deleted when job table is empty"
        );
        assert!(err
            .iter()
            .any(|l| l.contains("job table is empty while log files exist")));
    }

    #[test]
    fn test_logs_kept_for_non_done_jobs_by_default() {
        let temp_dir = tempdir().unwrap();
        let state_dir = temp_dir.path().join("state");
        let ws_dir = temp_dir.path().join("ws");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&ws_dir).unwrap();

        let config = Config {
            state_root: state_dir.clone(),
            repos_root: ws_dir.join(".agent-repos"),
            workspace_root: ws_dir,
            gc_max_age_days: 14,
            ..Default::default()
        };

        let now = Utc::now();
        let old_time = now - Duration::days(20);

        // Job 1: a genuinely Done job (a structured, complete SUCCESS is
        // what `compute_status` classifies as `Done`; a plain-text log with
        // exit 0 would be `Incomplete`) with a log.
        let job_done = make_test_job("j-done", "p", old_time, JobStatus::Done);
        job_done.save(&state_dir).unwrap();
        let log_done = JobMetadata::log_path(&state_dir, "j-done");
        fs::create_dir_all(log_done.parent().unwrap()).unwrap();
        fs::write(&log_done, r#"{"status":"SUCCESS","response":"done ok"}"#).unwrap();

        // Job 2: a genuinely Failed (non-Done) job -- a non-zero exit plus a
        // structured ERROR line -- with a log.
        let mut job_error = make_test_job("j-error", "p", old_time, JobStatus::Failed);
        job_error.exit_code = Some(1);
        job_error.save(&state_dir).unwrap();
        let log_error = JobMetadata::log_path(&state_dir, "j-error");
        fs::write(&log_error, r#"{"status":"ERROR","response":"boom"}"#).unwrap();

        // 1. Run GC without --purge-logs:
        let args = GcArgs {
            older_than: Some("14d".to_string()),
            project: None,
            max_jobs: None,
            dry_run: false,
            purge_logs: false,
        };

        run(&config, &args).unwrap();

        // Done job: record and log are removed
        assert!(
            JobMetadata::load(&state_dir, "j-done").is_err(),
            "done job record removed"
        );
        assert!(!log_done.exists(), "done job log removed");

        // Error job: record is removed, but log is RETAINED by default
        assert!(
            JobMetadata::load(&state_dir, "j-error").is_err(),
            "error job record removed"
        );
        assert!(
            log_error.exists(),
            "non-Done job log must be kept by default"
        );

        // 2. Re-create the (genuinely Failed) job and run with --purge-logs:
        let mut job_error2 = make_test_job("j-error2", "p", old_time, JobStatus::Failed);
        job_error2.exit_code = Some(1);
        job_error2.save(&state_dir).unwrap();
        let log_error2 = JobMetadata::log_path(&state_dir, "j-error2");
        fs::write(&log_error2, r#"{"status":"ERROR","response":"boom 2"}"#).unwrap();

        let args_purge = GcArgs {
            older_than: Some("14d".to_string()),
            project: None,
            max_jobs: None,
            dry_run: false,
            purge_logs: true,
        };

        run(&config, &args_purge).unwrap();

        assert!(JobMetadata::load(&state_dir, "j-error2").is_err());
        assert!(
            !log_error2.exists(),
            "with --purge-logs, non-Done log older than cutoff is removed"
        );
    }
}

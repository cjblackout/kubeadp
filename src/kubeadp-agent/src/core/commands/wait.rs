use anyhow::{bail, Context, Result};
use serde_json::json;
use std::time::Duration;

use crate::core::args::WaitArgs;
use crate::core::config::Config;
use crate::core::job::{compute_status, JobMetadata, JobStatus};
use crate::core::output::CommandOutput;

/// Polling interval while waiting for jobs to reach a terminal state.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitMode {
    /// Satisfied as soon as any of the tracked jobs is terminal.
    Any,
    /// Satisfied only when all tracked jobs are terminal.
    All,
}

/// Resolves the effective wait mode from the arguments.
///
/// Single ID: `Any` (identical semantics to `All` for one job).
/// Multiple IDs: `All` by default, `--any` opts into `Any`.
fn wait_mode(args: &WaitArgs) -> Result<WaitMode> {
    if args.any && args.all {
        bail!("--any and --all are mutually exclusive");
    }
    if args.job_ids.len() <= 1 || args.any {
        Ok(WaitMode::Any)
    } else {
        Ok(WaitMode::All)
    }
}

/// A job counts as "settled" for the wait condition when its row is gone
/// (`None` -- the job can never change state again) or its status is
/// terminal.
fn is_settled(s: &Option<JobStatus>) -> bool {
    match s {
        Some(st) => st.is_terminal(),
        None => true,
    }
}

/// True when the wait condition is met for the given status list.
fn wait_satisfied(mode: WaitMode, statuses: &[Option<JobStatus>]) -> bool {
    match mode {
        WaitMode::Any => statuses.iter().any(is_settled),
        WaitMode::All => statuses.iter().all(is_settled),
    }
}

/// Exit code once the condition is met:
/// `0` if the relevant job(s) are all/any `Done` respectively, `2` if they are
/// terminal but not Done. A vanished job row (`None`) is terminal but never
/// Done, so it can never drive the code to `0`.
fn wait_exit_code(mode: WaitMode, statuses: &[Option<JobStatus>]) -> u32 {
    match mode {
        WaitMode::Any => {
            if statuses.iter().any(|s| matches!(s, Some(JobStatus::Done))) {
                0
            } else {
                2
            }
        }
        WaitMode::All => {
            if statuses.iter().all(|s| matches!(s, Some(JobStatus::Done))) {
                0
            } else {
                2
            }
        }
    }
}

/// The machine-readable status token for a tracked job: its status string,
/// or `"missing"` when the job row has vanished.
fn status_str_for(s: &Option<JobStatus>) -> String {
    match s {
        Some(st) => st.as_str().to_string(),
        None => "missing".to_string(),
    }
}

/// User-facing per-job line for a job whose row vanished mid-wait (cleaned
/// or GC'd): a distinct message, not a terminal-state claim.
fn wait_missing_message(id: &str) -> String {
    format!(
        "Job '{}' record no longer exists (the job row was removed while waiting)",
        id
    )
}

/// Whether a load error means the job row simply no longer exists (the job
/// was cleaned/GC'd) as opposed to a real storage failure.
fn is_missing_job_error(err: &anyhow::Error) -> bool {
    err.to_string().starts_with("No such job found")
}

/// Resolves each requested job ID to its full job ID exactly once, before
/// the poll loop starts. An unknown ID or an ambiguous prefix is a hard
/// error here (same error texts as before); resolving once also prevents a
/// prefix from newly becoming ambiguous mid-wait.
fn resolve_wait_job_ids(config: &Config, job_ids: &[String]) -> Result<Vec<String>> {
    job_ids
        .iter()
        .map(|id| {
            JobMetadata::resolve(&config.state_root, id)
                .with_context(|| format!("Failed to load job '{}'", id))
                .map(|meta| meta.job_id)
        })
        .collect()
}

/// User-facing per-job line once the wait condition is satisfied.
///
/// A job that is still running when the condition is met (only possible in
/// `--any` mode) gets an accurate message instead of a false
/// "reached terminal state" claim.
fn wait_outcome_message(id: &str, s: &JobStatus) -> String {
    if s.is_terminal() {
        format!("Job '{}' reached terminal state: {}", id, s)
    } else {
        format!(
            "Job '{}' still running (wait condition already satisfied by another job)",
            id
        )
    }
}

pub fn run(config: &Config, args: &WaitArgs) -> Result<CommandOutput> {
    if args.job_ids.is_empty() {
        // Clap enforces `required = true` on the CLI, but the HTTP API can
        // deliver an empty list; without this guard `all(..)` would be
        // vacuously true (instant "all done", exit 0 for zero jobs) and
        // `--any` would wait forever on an empty set.
        bail!("No job IDs specified to wait for.");
    }

    let mode = wait_mode(args)?;

    let deadline = if let Some(raw) = &args.timeout {
        Some(chrono::Utc::now() + crate::core::commands::gc::parse_duration_string(raw)?)
    } else {
        None
    };

    let job_ids = resolve_wait_job_ids(config, &args.job_ids)?;

    // `None` marks a job whose row vanished (cleaned/GC'd) while waiting.
    let mut last: Vec<(String, Option<JobStatus>)> = Vec::new();
    loop {
        last.clear();
        for job_id in &job_ids {
            let status = match JobMetadata::load(&config.state_root, job_id) {
                Ok(meta) => Some(compute_status(&meta, &config.state_root)),
                // The row was removed (cleaned/GC'd) mid-wait: a terminal
                // "missing" outcome, not a hard error.
                Err(err) if is_missing_job_error(&err) => None,
                Err(err) => {
                    return Err(err).with_context(|| format!("Failed to load job '{}'", job_id))
                }
            };
            last.push((job_id.clone(), status));
        }
        let statuses: Vec<Option<JobStatus>> = last.iter().map(|(_, s)| *s).collect();

        if wait_satisfied(mode, &statuses) {
            let mut output = CommandOutput::new(json!({
                "job_ids": last.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
                "statuses": statuses.iter().map(status_str_for).collect::<Vec<_>>(),
            }));
            for (id, s) in &last {
                match s {
                    Some(st) => output.push_out(wait_outcome_message(id, st)),
                    None => output.push_out(wait_missing_message(id)),
                }
            }
            let code = wait_exit_code(mode, &statuses);
            if code != 0 {
                output.push_err(format!(
                    "Wait finished: condition met, but the relevant job(s) are not Done (exit code {}).",
                    code
                ));
            }
            output.exit_code = code as i32;
            return Ok(output);
        }

        if let Some(deadline) = deadline {
            if chrono::Utc::now() >= deadline {
                let mut output = CommandOutput::new(json!({
                    "job_ids": last.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
                    "statuses": statuses.iter().map(status_str_for).collect::<Vec<_>>(),
                }));
                for (id, s) in &last {
                    output.push_out(format!(
                        "Job '{}' status at timeout: {}",
                        id,
                        status_str_for(s)
                    ));
                }
                output.push_err(
                    "Timed out waiting for job(s); condition not met (exit code 3).".to_string(),
                );
                output.exit_code = 3;
                return Ok(output);
            }
        }

        std::thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wait_satisfied_modes() {
        let running = Some(JobStatus::Running);
        let done = Some(JobStatus::Done);
        let failed = Some(JobStatus::Failed);

        // Any: satisfied by a single terminal job among many.
        assert!(wait_satisfied(WaitMode::Any, &[running, running, failed]));
        assert!(!wait_satisfied(WaitMode::Any, &[running, running]));
        assert!(wait_satisfied(WaitMode::Any, &[done]));

        // All: requires every job to be terminal.
        assert!(!wait_satisfied(WaitMode::All, &[running, done, done]));
        assert!(wait_satisfied(WaitMode::All, &[done, failed]));
        assert!(!wait_satisfied(WaitMode::All, &[running]));
    }

    #[test]
    fn test_wait_missing_job_is_terminal_but_not_done() {
        // A vanished row (None) is terminal, but it must not, by itself,
        // satisfy All mode while another job is still running.
        assert!(wait_satisfied(
            WaitMode::Any,
            &[Some(JobStatus::Running), None]
        ));
        assert!(!wait_satisfied(
            WaitMode::All,
            &[Some(JobStatus::Running), None]
        ));
        assert!(wait_satisfied(
            WaitMode::All,
            &[Some(JobStatus::Done), None]
        ));
        assert!(!wait_satisfied(WaitMode::All, &[Some(JobStatus::Running)]));
        assert!(!wait_satisfied(WaitMode::Any, &[Some(JobStatus::Running)]));

        // But it never counts as Done: the exit code stays 2.
        assert_eq!(
            wait_exit_code(WaitMode::All, &[Some(JobStatus::Running), None]),
            2
        );
        assert_eq!(wait_exit_code(WaitMode::All, &[None]), 2);
        assert_eq!(
            wait_exit_code(WaitMode::Any, &[None, Some(JobStatus::Done)]),
            0
        );
        assert_eq!(wait_exit_code(WaitMode::Any, &[None]), 2);

        // Distinct, non-terminal-sounding message for the vanished row.
        let msg = wait_missing_message("abc12345");
        assert_eq!(
            msg,
            "Job 'abc12345' record no longer exists (the job row was removed while waiting)"
        );
        assert!(!msg.contains("reached terminal state"));
        assert!(!msg.is_empty() && msg.contains("abc12345"));

        // The machine-readable token for a vanished row.
        assert_eq!(status_str_for(&None), "missing");
        assert_eq!(status_str_for(&Some(JobStatus::Done)), "done");
    }

    #[test]
    fn test_wait_outcome_message_distinguishes_running() {
        // A terminal job keeps the "reached terminal state" wording.
        assert_eq!(
            wait_outcome_message("job1", &JobStatus::Done),
            "Job 'job1' reached terminal state: done"
        );
        assert_eq!(
            wait_outcome_message("job1", &JobStatus::Failed),
            "Job 'job1' reached terminal state: failed"
        );

        // A still-running job (only possible when the wait condition is
        // satisfied by another job in --any mode) must NOT be claimed to
        // have reached a terminal state.
        let msg = wait_outcome_message("job2", &JobStatus::Running);
        assert!(
            !msg.contains("reached terminal state"),
            "running job must not be reported as terminal: {}",
            msg
        );
        assert!(msg.contains("still running"));
        assert!(msg.contains("job2"));
    }

    #[test]
    fn test_wait_exit_code() {
        let done = Some(JobStatus::Done);
        let failed = Some(JobStatus::Failed);
        let incomplete = Some(JobStatus::Incomplete);
        let quota = Some(JobStatus::QuotaExhausted);

        assert_eq!(wait_exit_code(WaitMode::All, &[done]), 0);
        assert_eq!(wait_exit_code(WaitMode::All, &[done, done]), 0);
        assert_eq!(wait_exit_code(WaitMode::All, &[done, failed]), 2);
        assert_eq!(wait_exit_code(WaitMode::All, &[quota]), 2);

        assert_eq!(wait_exit_code(WaitMode::Any, &[incomplete, done]), 0);
        assert_eq!(wait_exit_code(WaitMode::Any, &[failed, incomplete]), 2);
    }

    #[test]
    fn test_wait_mode_resolution() {
        use crate::core::args::WaitArgs;

        // Single ID defaults to Any.
        let args = WaitArgs {
            job_ids: vec!["abc".into()],
            any: false,
            all: false,
            timeout: None,
        };
        assert_eq!(wait_mode(&args).unwrap(), WaitMode::Any);

        // Multiple IDs default to All.
        let args = WaitArgs {
            job_ids: vec!["abc".into(), "def".into()],
            any: false,
            all: false,
            timeout: None,
        };
        assert_eq!(wait_mode(&args).unwrap(), WaitMode::All);

        // --any wins for multiple IDs.
        let args = WaitArgs {
            job_ids: vec!["abc".into(), "def".into()],
            any: true,
            all: false,
            timeout: None,
        };
        assert_eq!(wait_mode(&args).unwrap(), WaitMode::Any);

        // --any and --all together is a user error (clap also enforces this).
        let args = WaitArgs {
            job_ids: vec!["abc".into()],
            any: true,
            all: true,
            timeout: None,
        };
        assert!(wait_mode(&args).is_err());
    }

    #[test]
    fn test_wait_run_rejects_empty_job_ids() {
        use crate::core::args::WaitArgs;

        // The HTTP API can deliver zero job IDs (clap's `required = true`
        // only guards the CLI). `run` must bail instead of exiting 0 with
        // "all done" (vacuous `all`) or waiting forever (`--any`).
        let config = Config::default();

        let args = WaitArgs {
            job_ids: vec![],
            any: false,
            all: false,
            timeout: None,
        };
        let res = run(&config, &args);
        assert!(
            res.is_err(),
            "wait with zero job IDs must error, got {:?}",
            res
        );
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("No job IDs specified to wait for."),
            "unexpected error message"
        );

        // --any mode: same guard, must not enter an infinite poll loop.
        let args = WaitArgs {
            job_ids: vec![],
            any: true,
            all: false,
            timeout: None,
        };
        let res = run(&config, &args);
        assert!(
            res.is_err(),
            "wait --any with zero job IDs must error, got {:?}",
            res
        );
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("No job IDs specified to wait for."),
            "unexpected error message"
        );
    }
}

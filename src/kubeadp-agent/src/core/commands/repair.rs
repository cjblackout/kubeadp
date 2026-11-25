use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::core::args::RepairArgs;
use crate::core::config::Config;
use crate::core::error::AgentRunError;
use crate::core::job::{list_jobs, JobMetadata};
use crate::core::output::CommandOutput;
use crate::core::repos::repair_worktree;

pub fn run(config: &Config, args: &RepairArgs) -> Result<CommandOutput> {
    if args.job_id.is_none() && !args.all {
        return Err(AgentRunError::invalid(
            "repair_missing_target",
            "Provide a job ID or --all to repair",
        )
        .into());
    }

    let mut output = CommandOutput::new(Value::Null);
    let mut results: Vec<Value> = Vec::new();

    if let Some(job_id) = &args.job_id {
        // Single job: the outcome is the whole answer; only `repaired` and
        // `healthy` succeed, everything else is a hard error (the server
        // maps the typed errors to 404/400/500).
        let meta = JobMetadata::resolve(&config.state_root, job_id)?;
        let outcome = repair_worktree(config, &meta)?;
        match outcome {
            crate::core::repos::RepairOutcome::Repaired => {
                output.push_out(format!("repaired {}", meta.job_id));
                results.push(json!({ "job_id": meta.job_id, "result": "repaired" }));
            }
            crate::core::repos::RepairOutcome::Healthy => {
                output.push_out(format!("healthy {}", meta.job_id));
                results.push(json!({ "job_id": meta.job_id, "result": "healthy" }));
            }
            crate::core::repos::RepairOutcome::External => {
                return Err(AgentRunError::invalid(
                    "repair_external_worktree",
                    format!(
                        "Job '{}' was started with --from-worktree: its worktree at {} is not managed by agent-run and cannot be repaired",
                        meta.job_id,
                        meta.worktree_path.display()
                    ),
                )
                .into());
            }
            crate::core::repos::RepairOutcome::Absent => {
                return Err(AgentRunError::not_found(
                    "worktree_not_found",
                    format!(
                        "Worktree directory for job '{}' does not exist at {} (nothing to repair)",
                        meta.job_id,
                        meta.worktree_path.display()
                    ),
                )
                .into());
            }
        }
        output.data = json!({ "results": results });
        return Ok(output);
    }

    // --all: one line per job (repairs only the dangling ones).
    let jobs = list_jobs(&config.state_root, None)
        .with_context(|| "failed to list jobs for repair --all".to_string())?;
    if jobs.is_empty() {
        output.push_out("No jobs found (nothing to repair).".to_string());
        output.data = json!({ "results": Vec::<Value>::new() });
        return Ok(output);
    }
    let mut failed = 0;
    for meta in &jobs {
        match repair_worktree(config, meta) {
            Ok(outcome) => {
                let word = match outcome {
                    crate::core::repos::RepairOutcome::Repaired => "repaired",
                    crate::core::repos::RepairOutcome::Healthy => "healthy",
                    crate::core::repos::RepairOutcome::External => {
                        output.push_out(format!(
                            "failed {}: external worktree (not managed by agent-run)",
                            meta.job_id
                        ));
                        failed += 1;
                        results.push(json!({
                            "job_id": meta.job_id,
                            "result": "failed",
                            "reason": "external worktree (not managed by agent-run)"
                        }));
                        continue;
                    }
                    crate::core::repos::RepairOutcome::Absent => {
                        output.push_out(format!(
                            "failed {}: worktree directory missing at {}",
                            meta.job_id,
                            meta.worktree_path.display()
                        ));
                        failed += 1;
                        results.push(json!({
                            "job_id": meta.job_id,
                            "result": "failed",
                            "reason": format!("worktree directory missing at {}", meta.worktree_path.display())
                        }));
                        continue;
                    }
                };
                output.push_out(format!("{word} {}", meta.job_id));
                results.push(json!({ "job_id": meta.job_id, "result": word }));
            }
            Err(err) => {
                let reason = err.to_string();
                output.push_out(format!("failed {}: {reason}", meta.job_id));
                failed += 1;
                results.push(json!({
                    "job_id": meta.job_id,
                    "result": "failed",
                    "reason": reason
                }));
            }
        }
    }
    if failed > 0 {
        output.exit_code = 1;
    }
    output.data = json!({ "results": results });
    Ok(output)
}

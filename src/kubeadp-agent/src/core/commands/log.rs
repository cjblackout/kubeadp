use anyhow::{Context, Result};
use serde_json::json;
use std::fs;

use crate::core::args::JobIdArg;
use crate::core::config::Config;
use crate::core::job::JobMetadata;
use crate::core::output::CommandOutput;

pub fn run(config: &Config, args: &JobIdArg) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    if !log_path.exists() {
        return Err(crate::core::error::AgentRunError::not_found(
            "log_not_found",
            format!(
                "No log file found for job '{}' at {} (the job may have terminated before logging started, or the log was purged)",
                meta.job_id,
                log_path.display()
            ),
        )
        .into());
    }

    let content = fs::read(&log_path)
        .with_context(|| format!("Failed to read log file at {}", log_path.display()))?;

    // `log` is a raw-bytes command: the log content is stored as its
    // newline-split lines so that joining them back reproduces
    // the exact bytes without adding a trailing newline.
    let text = String::from_utf8_lossy(&content).into_owned();
    let mut output = CommandOutput::new(json!({
        "log_path": log_path.to_string_lossy().to_string(),
    }));
    output.stdout_lines = text.split('\n').map(String::from).collect();
    Ok(output)
}

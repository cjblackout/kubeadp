//! `agent-run models` -- list the model names `start --model` accepts, by
//! asking the server's own `agy` to print them.
//!
//! The child inherits the server environment (including `HOME`) with the
//! job-runner proxy settings applied (`apply_proxy`), same as a job's agy.

use std::io::Read;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::core::args::ModelsArgs;
use crate::core::config::Config;
use crate::core::output::CommandOutput;

/// `agy models` is expected to finish quickly; 30 s is generous but
/// guarantees we never hang the caller forever.
const AGY_MODELS_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// `agy models` prints a progress line first (e.g. "Fetching available
/// models...") and then one line per model: `<id>\t<label>`. Lines without a
/// TAB are not model lines and are dropped; blank lines and CRLF endings are
/// tolerated.
pub fn parse_models(stdout: &str) -> Vec<(String, String)> {
    stdout
        .lines()
        .filter_map(|line| {
            let line = line.trim_end_matches('\r');
            let mut parts = line.splitn(2, '\t');
            let id = parts.next()?.trim();
            let label = parts.next()?.trim();
            if id.is_empty() || label.is_empty() {
                return None;
            }
            Some((id.to_string(), label.to_string()))
        })
        .collect()
}

/// Spawns `<config.agy_path> models` (with the job-runner proxy settings
/// applied via `apply_proxy`), waits up to `AGY_MODELS_TIMEOUT` (killing the
/// child on timeout) and returns `(stdout, trimmed stderr)`. Fails (never
/// panics) on spawn failure, timeout, or non-zero exit, including the trimmed
/// stderr in the error text.
fn run_agy_models(config: &Config) -> Result<(String, String)> {
    let agy = config.agy_path.to_string_lossy().to_string();
    let mut cmd = Command::new(&config.agy_path);
    cmd.arg("models")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    crate::core::proxy::apply_proxy(&mut cmd, Some(config), None);
    let mut child = crate::core::spawn::spawn_command_tracked(&mut cmd, "`agy models`")
        .with_context(|| format!("failed to spawn `{agy} models`"))?;
    // Register the child so the in-server zombie reaper (PID 1) can never
    // steal this helper's exit status between the `try_wait()` polls; on the
    // rare TOCTOU ECHILD the status is handed back from the reaper's stash.
    let tracked = crate::core::reaper::TrackedChild::new(child.id());
    let mut stdout_pipe = child.stdout.take().context("piped stdout")?;
    let mut stderr_pipe = child.stderr.take().context("piped stderr")?;

    let deadline = Instant::now() + AGY_MODELS_TIMEOUT;
    let (status, timed_out) = loop {
        // Recover an ECHILD (the reaper won the TOCTOU race) into its stashed
        // exit status; a genuine wait error still propagates unchanged.
        let poll_status = match child.try_wait() {
            Ok(status) => status,
            Err(e) => match tracked.recover_echild(&e) {
                Some(status) => Some(status),
                None => return Err(anyhow::anyhow!("`agy models` wait: {e}")),
            },
        };
        match poll_status {
            Some(status) => break (Some(status), false),
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break (None, true);
                }
                std::thread::sleep(POLL_INTERVAL);
            }
        }
    };

    let mut stdout_buf = Vec::new();
    stdout_pipe
        .read_to_end(&mut stdout_buf)
        .context("failed to read `agy models` stdout")?;
    let mut stderr_buf = Vec::new();
    stderr_pipe
        .read_to_end(&mut stderr_buf)
        .context("failed to read `agy models` stderr")?;
    let stdout = String::from_utf8_lossy(&stdout_buf).to_string();
    let stderr = String::from_utf8_lossy(&stderr_buf).trim().to_string();

    if timed_out {
        anyhow::bail!(
            "`{agy} models` timed out after {} s{}",
            AGY_MODELS_TIMEOUT.as_secs(),
            if stderr.is_empty() {
                String::new()
            } else {
                format!(" (stderr: {stderr})")
            }
        );
    }
    let status = status.ok_or_else(|| anyhow::anyhow!("`{agy} models` exited unexpectedly"))?;
    if !status.success() {
        anyhow::bail!(
            "`{agy} models` exited with code {}: {stderr}",
            crate::core::runner::exit_code_from_status(status)
        );
    }
    Ok((stdout, stderr))
}

pub fn run(config: &Config, args: &ModelsArgs) -> Result<CommandOutput> {
    let _ = args;
    let (stdout, stderr) = run_agy_models(config)?;
    let models = parse_models(&stdout);
    if models.is_empty() {
        anyhow::bail!(
            "`agy models` produced no model lines (stderr: {}): {}",
            if stderr.is_empty() {
                "<empty>".to_string()
            } else {
                stderr
            },
            stdout.trim()
        );
    }

    // Aligned table header: `MODEL  LABEL` (id column width from the data).
    let id_width = models
        .iter()
        .map(|(id, _)| id.chars().count())
        .max()
        .unwrap_or(0)
        .max("MODEL".len());

    let mut output = CommandOutput::new(json!({ "models": [] }));
    output.push_out(format!("{:<id_width$}  LABEL", "MODEL"));
    for (id, label) in &models {
        output.push_out(format!("{id:<id_width$}  {label}"));
    }

    let model_entries: Vec<Value> = models
        .iter()
        .map(|(id, label)| json!({ "id": id, "label": label }))
        .collect();
    output.data["models"] = json!(model_entries);

    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn write_fake_agy(dir: &std::path::Path, script: &str) -> std::path::PathBuf {
        let agy = dir.join("agy");
        crate::core::spawn::write_executable_script(&agy, script).unwrap();
        agy
    }

    fn config_with_agy(agy: std::path::PathBuf) -> Config {
        Config {
            agy_path: agy,
            ..Default::default()
        }
    }

    const SAMPLE: &str =
        "Fetching available models...\ngemini-3.8-flash-low\tGemini 3.8 Flash (Low)\n";

    #[test]
    fn test_parse_models_drops_non_model_lines() {
        let models = parse_models(SAMPLE);
        assert_eq!(
            models,
            vec![(
                "gemini-3.8-flash-low".to_string(),
                "Gemini 3.8 Flash (Low)".to_string()
            )]
        );
    }

    #[test]
    fn test_parse_models_handles_blank_lines_crlf_and_tabless_lines() {
        let sample = "Fetching available models...\r\n\r\nprogress without tab\r\nid-1\tLabel 1\r\n\r\nid-2\tLabel 2\r\n";
        let models = parse_models(sample);
        assert_eq!(
            models,
            vec![
                ("id-1".to_string(), "Label 1".to_string()),
                ("id-2".to_string(), "Label 2".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_models_no_tab_lines_is_empty() {
        assert!(parse_models("").is_empty());
        assert!(parse_models(SAMPLE.lines().next().unwrap()).is_empty());
        assert!(parse_models("\n\n").is_empty());
    }

    #[test]
    fn test_run_with_fake_agy_lists_models() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let script = concat!(
            "#!/bin/sh\n",
            "echo 'Fetching available models...'\n",
            "printf 'gemini-3.8-flash-low\\tGemini 3.8 Flash (Low)\\n'\n",
            "printf 'model-a\\tLabel A\\n'\n",
            "\n"
        );
        let agy = write_fake_agy(temp_dir.path(), script);
        let config = config_with_agy(agy);

        let output = run(&config, &ModelsArgs {}).unwrap();
        assert_eq!(output.data["models"].as_array().unwrap().len(), 2);
        assert_eq!(output.data["models"][0]["id"], "gemini-3.8-flash-low");
        assert_eq!(output.data["models"][0]["label"], "Gemini 3.8 Flash (Low)");
        assert_eq!(output.data["models"][1]["id"], "model-a");
        assert_eq!(output.data["models"][1]["label"], "Label A");
        assert_eq!(output.exit_code, 0);

        // Aligned table: id column width = longest id (20) + two spaces.
        assert_eq!(output.stdout_lines.len(), 3);
        let header = &output.stdout_lines[0];
        assert!(header.ends_with("LABEL"), "header: {header}");
        assert!(header.starts_with("MODEL"), "header: {header}");
        let first = output.stdout_lines[1].as_str();
        assert_eq!(first, "gemini-3.8-flash-low  Gemini 3.8 Flash (Low)");
        let second = output.stdout_lines[2].as_str();
        assert!(second.starts_with("model-a"));
        assert!(second.ends_with("Label A"));
        // Both labels start at the same column (id width 20 + 2 spaces).
        assert!(first[22..].starts_with("Gemini"));
        assert!(second[22..].starts_with("Label A"));
        for c in second[7..22].chars() {
            assert_eq!(c, ' ');
        }
    }

    #[test]
    fn test_run_with_fake_agy_nonzero_exit_errors_with_stderr() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let script = "#!/bin/sh\necho 'model fetch failed' >&2\nexit 1\n";
        let agy = write_fake_agy(temp_dir.path(), script);
        let config = config_with_agy(agy);

        let err = run(&config, &ModelsArgs {}).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("model fetch failed"),
            "error should include stderr, got: {msg}"
        );
    }

    #[test]
    fn test_run_with_fake_agy_zero_models_errors() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let script = "#!/bin/sh\necho 'Fetching available models...'\n";
        let agy = write_fake_agy(temp_dir.path(), script);
        let config = config_with_agy(agy);

        let err = run(&config, &ModelsArgs {}).unwrap_err();
        assert!(err.to_string().contains("no model lines"), "got: {}", err);
    }

    #[test]
    fn test_run_applies_config_proxy_env_to_agy_child() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        // The fake agy records the proxy variables it actually sees into an
        // absolutely-pathed file (its own stdout is consumed by
        // run_agy_models for the model lines), then prints a model line.
        let env_file = temp_dir.path().join("child.env");
        let script = format!(
            "#!/bin/sh\necho \"HTTPS_PROXY=${{HTTPS_PROXY}}\" > '{env}'\necho \"NO_PROXY=${{NO_PROXY}}\" >> '{env}'\nprintf 'model-a\\tLabel A\\n'\n",
            env = env_file.display()
        );
        let agy = write_fake_agy(temp_dir.path(), &script);
        let mut config = config_with_agy(agy);
        config.https_proxy = Some("http://models-proxy:8443".to_string());
        config.no_proxy = Some("corp.local".to_string());

        let output = run(&config, &ModelsArgs {}).unwrap();
        assert_eq!(output.data["models"].as_array().unwrap().len(), 1);
        let child_env = fs::read_to_string(&env_file).unwrap();
        assert!(
            child_env.contains("HTTPS_PROXY=http://models-proxy:8443"),
            "child did not receive HTTPS_PROXY: {child_env}"
        );
        assert!(
            child_env
                .contains("NO_PROXY=corp.local,localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc"),
            "NO_PROXY missing cluster defaults: {child_env}"
        );
    }
}

use anyhow::Result;
use serde_json::json;
use std::fs;

use crate::core::args::ResultArgs;
use crate::core::config::Config;
use crate::core::job::{
    compute_status, extract_final_response, structured_value, JobMetadata, JobStatus,
};
use crate::core::output::CommandOutput;

// NOTE: result is strictly read-only. Jobs, logs, worktrees and branches are
// removed ONLY by an explicit `agent-run clean <id>` or `agent-run gc`.

pub fn run(config: &Config, args: &ResultArgs) -> Result<CommandOutput> {
    let meta = JobMetadata::resolve(&config.state_root, &args.job_id)?;
    let status = compute_status(&meta, &config.state_root);

    if status == JobStatus::Running {
        let pid_str = meta
            .pid
            .map(|p| format!(" (PID {})", p))
            .unwrap_or_default();
        return Err(crate::core::error::AgentRunError::invalid(
            "job_running",
            format!(
                "Job '{}' is still running{}. Use 'agent-run wait {}' or 'agent-run tail {}' first.",
                meta.job_id,
                pid_str,
                meta.job_id,
                meta.job_id
            ),
        )
        .into());
    }

    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    // Read lossily (like `compute_status`): a stray non-UTF-8 byte in the raw
    // log capture must not blank the whole log and hide the final response.
    let log = match fs::read(&log_path) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(_) => String::new(),
    };

    let response = extract_final_response(&log);
    let error_text = final_error_text(&log);

    // The `data` payload is exactly the object the client pretty-prints for
    // `--json` (the server always runs result in human mode; the remote
    // client re-serializes `data` itself).
    let data = json!({
        "job_id": meta.job_id,
        "status": status.as_str(),
        "response": response,
        "error": error_text,
    });

    let mut output = CommandOutput::new(data);

    output.push_out(format!("Job:    {}", meta.job_id));
    output.push_out(format!("Status: {}", status));
    match &response {
        Some(r) => {
            output.push_out(String::new());
            output.push_out(r.clone());
        }
        None => {
            output.push_err(format!(
                "No final response captured for job '{}' (log has no structured SUCCESS line).",
                meta.job_id
            ));
        }
    }
    if let Some(err) = &error_text {
        if response
            .as_deref()
            .map(str::trim)
            .is_some_and(|r| !r.is_empty())
        {
            // The job ended in an error but a complete response was still
            // captured (e.g. a full answer followed by a transient upstream
            // 5xx on a later retry attempt). Surface the agent error as an
            // explicit warning WITHOUT hiding the response printed above.
            output.push_err(format!(
                "Warning: job's final status is '{}' and the agent reported an error, but a complete response was captured above. Agent error: {}",
                status, err
            ));
        } else {
            output.push_err(format!("Error reported by agent: {}", err));
        }
    }

    // Policy: `result` is strictly read-only. For a read-only job print one
    // informational hint so the caller knows how to clean it up when done.
    if meta.read_only {
        output.push_out(format!(
            "Read-only job '{}' is kept; remove it with: agent-run clean {}",
            meta.job_id, meta.job_id
        ));
    }

    Ok(output)
}

/// Returns the `error` field of the last recognized structured result
/// (terminal status: SUCCESS / ERROR / FAILED / FAILURE, case-insensitive),
/// if that result carries one.
///
/// Recognition goes through the shared [`crate::core::job::structured_value`]
/// helper, so BOTH structured shapes are covered: a legacy single-object
/// line with a top-level `status`, and the stream-json
/// `{"event":"result","result":{...}}` line (where `error` lives one level
/// down, inside the inner `result` object).
fn final_error_text(log: &str) -> Option<String> {
    let mut last: Option<Option<String>> = None;
    for line in log.lines() {
        // A claude stream-json terminal line (`result`/`system` with
        // `type`), which `structured_value`'s agy status-field recognition
        // would skip: its error text is the `error`/`message` payload.
        if let Some((st, text)) = crate::core::job::claude_structured_text(line) {
            last = Some(if matches!(st, crate::core::job::StructuredStatus::Error) {
                Some(text)
            } else {
                None
            });
            continue;
        }
        let Some((status, v)) = structured_value(line) else {
            continue;
        };
        // Terminal structured statuses: the success token plus the failure
        // spellings (case-insensitive). Anything else is skipped, keeping
        // the scan running at earlier lines.
        match status.to_ascii_uppercase().as_str() {
            "SUCCESS" | "ERROR" | "FAILED" | "FAILURE" => {}
            _ => continue,
        }
        last = Some(
            v.get("error")
                .and_then(|e| e.as_str())
                .map(|s| s.to_string()),
        );
    }
    let Some(err) = last.and_then(|e| e) else {
        // Defect 2 fallback (_HL-71 S1_): no terminal error line was found, but
        // a failed claude job may still carry its reason text in a synthetic
        // `assistant` message (`model:"<synthetic>"`) or in the plain-text
        // stderr tail of the log. Surface that instead of an empty `error`.
        return crate::core::job::claude_fallback_error_text(log);
    };
    if err.trim().is_empty() {
        return crate::core::job::claude_fallback_error_text(log);
    }
    Some(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_final_error_text() {
        let log = r#"some chatter
{"status":"processing","turn":1}
{"conversation_id":"c1","status":"ERROR","response":"","error":"quota exceeded","duration_seconds":2.0}
"#;
        assert_eq!(final_error_text(log).as_deref(), Some("quota exceeded"));

        // No error field on the last recognized line.
        let log2 = r#"{"conversation_id":"c1","status":"SUCCESS","response":"done"}
"#;
        assert_eq!(final_error_text(log2), None);

        // Unrecognized status lines are skipped.
        let log3 = r#"{"status":"weird","error":"ignored"}
"#;
        assert_eq!(final_error_text(log3), None);

        // FAILED / FAILURE spellings (and case variants) are recognized too.
        let log4 = r#"{"status":"FAILED","error":"build broke"}
"#;
        assert_eq!(final_error_text(log4).as_deref(), Some("build broke"));
        let log5 = r#"{"status":"failure","error":"disk full"}
"#;
        assert_eq!(final_error_text(log5).as_deref(), Some("disk full"));
        // A FAILED line without an error field still sets the scan window to
        // "no error" (like SUCCESS without an error field).
        let log6 = r#"{"status":"ERROR","error":"first"}
{"status":"FAILED"}"#;
        assert_eq!(final_error_text(log6), None);
    }

    #[test]
    fn test_final_error_text_claude_result_field() {
        // Defect 2 (HL-71 S1): a claude error `result` line with `is_error:
        // true` and the reason in the `result` field (NO `error` field) must
        // surface as the job's error text.
        let log = r#"{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"Failed to authenticate: OAuth session expired and could not be refreshed"}]},"session_id":"cl1"}
{"type":"result","subtype":"error","is_error":true,"result":"Failed to authenticate: OAuth session expired and could not be refreshed","session_id":"cl1"}
"#;
        assert_eq!(
            final_error_text(log).as_deref(),
            Some("Failed to authenticate: OAuth session expired and could not be refreshed"),
            "the claude error result's `result` text must be surfaced as the error"
        );

        // A succeeding claude result line carries no error -> None.
        let ok_log = r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"cl1"}
"#;
        assert_eq!(
            final_error_text(ok_log),
            None,
            "a claude success result must yield no error text"
        );
    }

    #[test]
    fn test_final_error_text_claude_fallback_chain() {
        // Defect 2 fallback (_HL-71 S1_): a failed claude job whose log has NO
        // terminal error `result` line still surfaces a reason -- the last
        // synthetic `assistant` message text, or the last non-JSON log line.
        // Without this, `agent-run result --json` reports an empty `error`.

        // No error-result line at all; the synthetic assistant carries the reason.
        let synthetic = r#"[claude-code:unrecognized_model] Failed to authenticate: OAuth session expired
{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"Failed to authenticate: OAuth session expired and could not be refreshed"}]},"session_id":"cl1"}
"#;
        assert_eq!(
            final_error_text(synthetic).as_deref(),
            Some("Failed to authenticate: OAuth session expired and could not be refreshed"),
            "the synthetic assistant text must be the fallback reason"
        );

        // A non-claude plain-chatter log must NOT surface a fallback reason.
        let chatter = "agent chatter only\nstill no structure\n";
        assert_eq!(
            final_error_text(chatter),
            None,
            "a non-claude log must not gain a fallback error"
        );
    }

    #[test]
    fn test_final_error_text_stream_json() {
        // A stream-json run that dies before the first turn: a single
        // `event:"result"` line. The `error` field lives one level DOWN,
        // inside the nested `result` object, so recognition must go through
        // the shared `structured_value` helper (legacy top-level `error`
        // would never be found).
        let stream_error = r#"{"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":"invalid model selection (--model \"no-such-model-xyz\"): model no-such-model-xyz is not recognized","duration_seconds":0,"num_turns":0,"usage":{"input_tokens":0,"output_tokens":0,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":0}}}
"#;
        let got = final_error_text(stream_error);
        assert!(
            got.as_deref()
                .is_some_and(|e| e.starts_with("invalid model selection")),
            "the nested result.error of a stream-json line must be surfaced, got: {:?}",
            got
        );

        // A stream-json SUCCESS line has no `error` field -> None (mirrors the
        // legacy SUCCESS case), and a step_update line is never a terminal
        // result (no `status`) so it is ignored.
        let stream_ok = r#"{"event":"step_update","step_update":{"conversation_id":"c1","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"all good"}}
{"event":"result","result":{"conversation_id":"c1","status":"SUCCESS","response":"done"}}
"#;
        assert_eq!(
            final_error_text(stream_ok),
            None,
            "a stream-json SUCCESS result (no error field) must yield None"
        );
    }

    fn save_read_only_job(state_root: &std::path::Path, job_id: &str, exit_code: Option<i32>) {
        save_read_only_job_with_log(
            state_root,
            job_id,
            exit_code,
            r#"{"conversation_id":"c1","status":"SUCCESS","response":"done"}"#,
        );
    }

    /// Like `save_read_only_job` but writes a caller-supplied log, so tests can
    /// build a terminal ERROR job (with or without a complete `response`)
    /// instead of the default terminal DONE job.
    fn save_read_only_job_with_log(
        state_root: &std::path::Path,
        job_id: &str,
        exit_code: Option<i32>,
        log: &str,
    ) {
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
            exit_code,
            status: None,
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: true,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(state_root).unwrap();
        // The log carries a complete structured terminal line, so the job is
        // terminal regardless of the recorded exit code.
        std::fs::write(JobMetadata::log_path(state_root, job_id), log).unwrap();
    }

    #[test]
    fn test_result_read_only_done_job_kept_after_result() {
        // Policy: result is strictly read-only. A done read-only job must still
        // be present (record + log) after `result` is called -- no auto-clean.
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        // exit_code None simulates a race where the monitor's final write has
        // not landed yet -- under the old code this used to defer; now the row
        // is simply kept in all cases.
        save_read_only_job(&config.state_root, "donekept1", None);

        let output = run(
            &config,
            &ResultArgs {
                job_id: "donekept1".to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            output.exit_code,
            0,
            "result must succeed: {:?}",
            output.stderr_lines.join(" | ")
        );
        // The row must still be present -- result never deletes.
        assert!(
            JobMetadata::load(&config.state_root, "donekept1").is_ok(),
            "done read-only job must be kept after result (no auto-clean)"
        );
        assert!(
            JobMetadata::log_path(&config.state_root, "donekept1").exists(),
            "log must be kept after result"
        );
        // The hint line must be printed.
        let stdout = output.stdout_lines.join("\n");
        assert!(
            stdout.contains("Read-only job 'donekept1' is kept"),
            "must print the keep-hint: {stdout}"
        );
        assert!(
            stdout.contains("agent-run clean donekept1"),
            "hint must name the clean command: {stdout}"
        );
    }

    #[test]
    fn test_result_read_only_done_job_kept_with_exit_code() {
        // Same as above but with exit_code recorded (monitor's final write
        // has landed). Under the old code this triggered auto-clean; now kept.
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_read_only_job(&config.state_root, "donekept2", Some(0));

        let output = run(
            &config,
            &ResultArgs {
                job_id: "donekept2".to_string(),
            },
        )
        .unwrap();
        assert_eq!(output.exit_code, 0);
        // Row and log must survive.
        assert!(
            JobMetadata::load(&config.state_root, "donekept2").is_ok(),
            "done read-only job with exit_code must still be kept after result"
        );
        assert!(JobMetadata::log_path(&config.state_root, "donekept2").exists());
        // The keep-hint must be present and must NOT say "auto-cleaned".
        let stdout = output.stdout_lines.join("\n");
        assert!(
            !stdout.contains("auto-cleaned"),
            "must not say auto-cleaned: {stdout}"
        );
        assert!(
            stdout.contains("agent-run clean donekept2"),
            "must print the clean hint: {stdout}"
        );
    }

    #[test]
    fn test_result_read_only_idempotent_second_read() {
        // Reading result twice must return the same output (idempotent) -- the
        // record and log must still exist after the first call so the second
        // call can succeed.
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_read_only_job(&config.state_root, "idem0001", Some(0));

        let out1 = run(
            &config,
            &ResultArgs {
                job_id: "idem0001".to_string(),
            },
        )
        .unwrap();
        let out2 = run(
            &config,
            &ResultArgs {
                job_id: "idem0001".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out1.exit_code, 0);
        assert_eq!(out2.exit_code, 0);
        assert_eq!(
            out1.stdout_lines.join("\n"),
            out2.stdout_lines.join("\n"),
            "second read must produce identical output"
        );
    }

    #[test]
    fn test_result_shows_response_despite_invalid_utf8_in_log() {
        // A stray non-UTF-8 byte in the raw log capture used to blank the
        // whole log (read_to_string -> unwrap_or_default), so `result`
        // printed "No final response captured" for a finished job.
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_read_only_job(&config.state_root, "lossyres1", Some(0));

        // Overwrite the log: one invalid byte (0xFF) BEFORE the valid final
        // JSON line, then the structured SUCCESS line.
        let mut log_bytes: Vec<u8> = vec![0xFF, b'\n'];
        log_bytes.extend_from_slice(
            br#"{"conversation_id":"c1","status":"SUCCESS","response":"done despite the bad byte"}"#,
        );
        std::fs::write(
            JobMetadata::log_path(&config.state_root, "lossyres1"),
            &log_bytes,
        )
        .unwrap();

        let output = run(
            &config,
            &ResultArgs {
                job_id: "lossyres1".to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            output.exit_code,
            0,
            "stdout: {:?}",
            output.stdout_lines.join(" ")
        );
        let stdout = output.stdout_lines.join("\n");
        assert!(
            stdout.contains("done despite the bad byte"),
            "the final response must survive a stray invalid byte in the log: {stdout}"
        );
        assert!(
            !stdout.contains("\"response\": null"),
            "response must not be null when the structured line is intact: {stdout}"
        );
        // Job must still be present (result never deletes).
        assert!(
            JobMetadata::load(&config.state_root, "lossyres1").is_ok(),
            "job must be kept after result even with bad-utf8 log"
        );
    }

    #[test]
    fn test_result_error_with_response_prints_response_and_warning() {
        // A read-only job whose terminal line is an ERROR but still carries a
        // complete response. `result` must (a) print the response, (b) surface
        // the agent error as a WARNING (not swallow it, not blank the body),
        // and (c) keep the record/log (no auto-clean for any status).
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        let err_log = r##"{"conversation_id":"c1","status":"ERROR","response":"# Research Report\n(14096 chars complete)","error":"API error (attempt 1): UNAVAILABLE (code 503): The service is currently unavailable.","duration_seconds":397.8,"num_turns":1}
"##;
        save_read_only_job_with_log(&config.state_root, "errjob01", Some(0), err_log);

        let output = run(
            &config,
            &ResultArgs {
                job_id: "errjob01".to_string(),
            },
        )
        .unwrap();
        // The command does not fail just because the job errored.
        assert_eq!(
            output.exit_code,
            0,
            "stderr: {:?}",
            output.stderr_lines.join(" | ")
        );

        let stdout = output.stdout_lines.join("\n");
        // The complete response is surfaced, not discarded.
        assert!(
            stdout.contains("(14096 chars complete)"),
            "the complete response must be printed: {stdout}"
        );
        assert!(
            stdout.contains("Status: failed"),
            "status must still reflect the error outcome: {stdout}"
        );
        // The agent error is delivered as an explicit warning line.
        let stderr = output.stderr_lines.join("\n");
        assert!(
            stderr.contains("Warning:") && stderr.contains("UNAVAILABLE (code 503)"),
            "the agent error must be surfaced as a warning: {stderr}"
        );
        // The row and the log are KEPT (result is strictly read-only).
        assert!(
            JobMetadata::load(&config.state_root, "errjob01").is_ok(),
            "read-only failed job must not be cleaned by result: record was destroyed"
        );
        assert!(
            JobMetadata::log_path(&config.state_root, "errjob01").exists(),
            "the log must be kept so the complete response can be re-read"
        );
        // The keep-hint must be printed for any read-only job.
        assert!(
            stdout.contains("Read-only job 'errjob01' is kept"),
            "the keep-hint must be printed even for failed jobs: {stdout}"
        );
    }

    #[test]
    fn test_result_error_empty_response_kept_and_error_line_shown() {
        // An ERROR terminal line with no (or blank) `response` must show the
        // "Error reported by agent:" line, keep the record/log, and still
        // print the keep-hint.
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        let err_empty = r#"{"conversation_id":"c1","status":"ERROR","response":"","error":"boom"}
"#;
        save_read_only_job_with_log(&config.state_root, "errjob02", Some(1), err_empty);

        let output = run(
            &config,
            &ResultArgs {
                job_id: "errjob02".to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            output.exit_code,
            0,
            "stderr: {:?}",
            output.stderr_lines.join(" | ")
        );

        // No complete response -> not the warning branch; the original
        // "Error reported by agent:" line is preserved (unchanged behaviour).
        let stderr = output.stderr_lines.join("\n");
        assert!(
            stderr.contains("Error reported by agent: boom"),
            "empty-response ERROR job keeps the original error line: {stderr}"
        );
        assert!(
            !stderr.contains("complete response was captured above"),
            "must not claim a complete response when none was captured: {stderr}"
        );
        // Record and log must be kept (result is strictly read-only).
        assert!(
            JobMetadata::load(&config.state_root, "errjob02").is_ok(),
            "read-only failed job (empty response) must not be cleaned by result"
        );
        assert!(
            JobMetadata::log_path(&config.state_root, "errjob02").exists(),
            "the log must be kept for a non-done outcome"
        );
        // Keep-hint must appear.
        let stdout = output.stdout_lines.join("\n");
        assert!(
            stdout.contains("agent-run clean errjob02"),
            "keep-hint must be printed for error jobs too: {stdout}"
        );
    }

    #[test]
    fn test_result_read_only_terminal_statuses_kept() {
        // Policy: `result` is strictly read-only. A read-only job in ANY
        // terminal classification must still be present (record + log) after
        // `result`, with the keep-hint printed. This covers the statuses not
        // exercised by the done/failed (ERROR) tests above: `incomplete`
        // (clean exit, no structured terminal line) and `quota-exhausted`
        // (quota signal in the log, distinct from a plain failed job).
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        // Exit 0 with no recognized structured line -> Incomplete.
        save_read_only_job_with_log(
            &config.state_root,
            "incompl01",
            Some(0),
            "agent chatter only, no structured terminal line\n",
        );
        // A quota-exhaustion marker in the terminal ERROR line -> QuotaExhausted.
        let quota_log = r#"{"conversation_id":"c1","status":"ERROR","response":"","error":"insufficient_quota","duration_seconds":1.0}
"#;
        save_read_only_job_with_log(&config.state_root, "quota0001", Some(1), quota_log);
        let cases = [
            ("incompl01", "incomplete"),
            ("quota0001", "quota-exhausted"),
        ];
        for (job_id, want_status) in cases {
            // Sanity-check the classification the scenario is supposed to produce.
            let meta = JobMetadata::load(&config.state_root, job_id).unwrap();
            assert_eq!(
                compute_status(&meta, &config.state_root).as_str(),
                want_status,
                "test setup for {job_id}"
            );

            let output = run(
                &config,
                &ResultArgs {
                    job_id: job_id.to_string(),
                },
            )
            .unwrap();
            assert_eq!(
                output.exit_code,
                0,
                "{job_id} stderr: {:?}",
                output.stderr_lines.join(" | ")
            );
            assert!(
                JobMetadata::load(&config.state_root, job_id).is_ok(),
                "{job_id} ({want_status}) must be kept after result (no auto-clean)"
            );
            assert!(
                JobMetadata::log_path(&config.state_root, job_id).exists(),
                "{job_id} ({want_status}) log must be kept after result"
            );
            let stdout = output.stdout_lines.join("\n");
            let status_line = format!("Status: {want_status}");
            let hint_line = format!("agent-run clean {job_id}");
            assert!(
                stdout.contains(&status_line),
                "{job_id} must report the terminal status: {stdout}"
            );
            assert!(
                stdout.contains(&hint_line),
                "keep-hint must be printed for {job_id} ({want_status}): {stdout}"
            );
        }
    }

    #[test]
    fn test_result_clean_removes_job_after_result() {
        // `result` keeps the job; `clean` must still be able to remove it.
        // This test verifies the clean command works on a read-only job that
        // was previously read via `result`.
        let temp = tempdir().unwrap();
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };
        save_read_only_job(&config.state_root, "cleanme1", Some(0));

        // Read the result first (must not delete the job).
        let out_result = run(
            &config,
            &ResultArgs {
                job_id: "cleanme1".to_string(),
            },
        )
        .unwrap();
        assert_eq!(out_result.exit_code, 0);
        assert!(
            JobMetadata::load(&config.state_root, "cleanme1").is_ok(),
            "job must still exist after result"
        );

        // Now clean it -- must succeed and remove the row.
        let clean_args = crate::core::args::CleanArgs {
            job_id: "cleanme1".to_string(),
            force: true,
            purge_log: true,
        };
        let out_clean = crate::core::commands::clean::run(&config, &clean_args).unwrap();
        assert_eq!(
            out_clean.exit_code,
            0,
            "clean must succeed: {:?}",
            out_clean.stderr_lines.join(" | ")
        );
        assert!(
            JobMetadata::load(&config.state_root, "cleanme1").is_err(),
            "job must be removed by clean"
        );
    }
}

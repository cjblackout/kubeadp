use anyhow::{bail, Context, Result};
use chrono::Utc;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::process::Command;

use crate::core::config::Config;
use crate::core::job::JobStatus;
use crate::core::proxy::apply_proxy;

/// Builds the argument list for `hindsight memory recall`.
///
/// Pattern: `<hindsight_path> -o json memory recall <bank> [--tags <tag>] -- "<query>"`
///
/// All options come BEFORE the query and the query is passed AFTER a `--`
/// separator, so a query that begins with a dash (e.g. an inbound A2A frame
/// such as `--- A2A INCOMING MESSAGE ...` or a literal `--help`) is treated as
/// a positional VALUE, never parsed as a flag by the hindsight CLI's clap
/// parser (`--` ends option parsing).
pub fn build_recall_args(bank: &str, query: &str, tag: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "-o".to_string(),
        "json".to_string(),
        "memory".to_string(),
        "recall".to_string(),
        bank.to_string(),
    ];
    if let Some(t) = tag {
        args.push("--tags".to_string());
        args.push(t.to_string());
    }
    args.push("--".to_string());
    args.push(query.to_string());
    args
}

/// Builds the argument list for `hindsight memory retain`.
///
/// Pattern: `<hindsight_path> -o json memory retain <bank> --document-tags <tag> --context "<context>" --async -- "<finding>"`
///
/// All options come BEFORE the finding and the finding is passed AFTER a `--`
/// separator so free text that begins with a dash is treated as a positional
/// VALUE, never parsed as a flag (same clap `--` safeguard as recall).
pub fn build_retain_args(
    bank: &str,
    finding: &str,
    tag: Option<&str>,
    context: &str,
) -> Vec<String> {
    let doc_tag = tag.unwrap_or("agent-run");
    vec![
        "-o".to_string(),
        "json".to_string(),
        "memory".to_string(),
        "retain".to_string(),
        bank.to_string(),
        "--document-tags".to_string(),
        doc_tag.to_string(),
        "--context".to_string(),
        context.to_string(),
        "--async".to_string(),
        "--".to_string(),
        finding.to_string(),
    ]
}

/// Parses the JSON output from `hindsight memory recall` defensively.
///
/// Traverses arrays of memories/results/items or objects containing text/content fields.
/// Returns `Some(plain_text)` if any non-empty memories were extracted, or `None` otherwise.
pub fn parse_recall_response(json_str: &str) -> Option<String> {
    let val: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let mut extracted = Vec::new();
    extract_text_from_value(&val, &mut extracted);
    if extracted.is_empty() {
        None
    } else {
        Some(extracted.join("\n\n"))
    }
}

fn extract_text_from_value(val: &serde_json::Value, out: &mut Vec<String>) {
    match val {
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_string());
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                extract_text_from_value(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            // Check known priority text fields on an individual memory/item
            for field in &["text", "content", "memory", "snippet", "finding"] {
                if let Some(serde_json::Value::String(s)) = map.get(*field) {
                    let trimmed = s.trim();
                    if !trimmed.is_empty() {
                        out.push(trimmed.to_string());
                        return;
                    }
                }
            }
            // Check container fields holding collections of memories
            for container in &["results", "memories", "items", "data", "documents"] {
                if let Some(child) = map.get(*container) {
                    extract_text_from_value(child, out);
                }
            }
        }
        _ => {}
    }
}

/// Prepends recalled memory context to the original prompt inside a `<recalled-context>` block.
///
/// If the prompt already starts with a `<recalled-context>` opening tag (a
/// well-formed block left behind by an earlier attempt, or a truncated one
/// whose closing tag is missing and `strip_recalled_context` deliberately
/// refuses to strip), it is returned unchanged: there is no documented marker
/// that would make everything after the opening tag safely treatable as one
/// block, so wrapping it would nest a new block inside the old one on every
/// retry. (Well-formed leading blocks never reach this path in practice:
/// `extract_prompt_text` strips them first.)
pub fn prepend_recalled_context(recalled: &str, original_prompt: &str) -> String {
    if original_prompt
        .trim_start()
        .starts_with("<recalled-context>")
    {
        return original_prompt.to_string();
    }
    format!(
        "<recalled-context>\n{}\n</recalled-context>\n\n{}",
        recalled.trim(),
        original_prompt
    )
}

/// Pure gating logic: auto-retain triggers ONLY when hindsight is enabled, auto-retain is on,
/// and the job status is terminal `Done`. Specifically returns false for `Failed`, `SelfTerminated`, `Killed`, or `Running`.
pub fn should_retain(hindsight_enabled: bool, auto_retain: bool, status: &JobStatus) -> bool {
    hindsight_enabled && auto_retain && matches!(status, JobStatus::Done)
}

/// Builds a short finding summary for auto-retain with the first ~200 characters of captured output.
pub fn build_retain_finding(job_id: &str, project: &str, captured_output: &str) -> String {
    let clean = captured_output.trim();
    let excerpt: String = if clean.chars().count() > 200 {
        clean.chars().take(200).collect()
    } else {
        clean.to_string()
    };
    format!(
        "agent-run job {} on project {} completed: {}",
        job_id, project, excerpt
    )
}

/// Builds the context payload for auto-retain with job ID, project, and tail excerpt of log.
pub fn build_retain_context(job_id: &str, project: &str, log_tail: &str) -> String {
    let clean = log_tail.trim();
    let count = clean.chars().count();
    let excerpt: String = if count > 2000 {
        clean.chars().skip(count - 2000).collect()
    } else {
        clean.to_string()
    };
    format!(
        "job_id: {}, project: {}, log_tail:\n{}",
        job_id, project, excerpt
    )
}

/// Strips any leading `<recalled-context>...</recalled-context>` block from a prompt.
///
/// Prevents nested/duplicate recalled-context blocks when a job is retried with auto-recall.
pub fn strip_recalled_context(prompt: &str) -> String {
    let trimmed = prompt.trim_start();
    if let Some(rest) = trimmed.strip_prefix("<recalled-context>") {
        if let Some(end_pos) = rest.find("</recalled-context>") {
            let after = &rest[end_pos + "</recalled-context>".len()..];
            return after.trim_start().to_string();
        }
    }
    prompt.to_string()
}

/// Resolves prompt text: if `already_resolved` is false and `prompt` starts
/// with `@`, reads file content; otherwise returns the string directly.
/// Also strips any existing `<recalled-context>` blocks so subsequent recalls
/// do not nest indefinitely.
///
/// Callers passing a prompt that is already resolved (e.g. the stored
/// `resolved_prompt`, or the content already read from an `@<file>` prompt)
/// MUST pass `already_resolved = true`: a resolved file content that itself
/// starts with `@` (a legitimate instruction line) must never be re-resolved
/// into the contents of another file.
pub fn extract_prompt_text(prompt: &str, already_resolved: bool) -> String {
    let raw = if !already_resolved {
        if let Some(path_str) = prompt.strip_prefix('@') {
            std::fs::read_to_string(path_str).unwrap_or_else(|_| prompt.to_string())
        } else {
            prompt.to_string()
        }
    } else {
        prompt.to_string()
    };
    strip_recalled_context(&raw)
}

/// Appends an informational or warning entry to the job's log file for auditability.
pub fn append_to_log_file(log_path: &Path, message: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log_path) {
        let timestamp = Utc::now().to_rfc3339();
        let _ = writeln!(file, "[{}] {}", timestamp, message);
    }
}

/// Validates that a hindsight bank is configured whenever hindsight is enabled.
pub fn validate_hindsight_config(hindsight_enabled: bool, bank: &Option<String>) -> Result<()> {
    if hindsight_enabled {
        match bank {
            Some(b) if !b.trim().is_empty() => Ok(()),
            _ => bail!(
                "Hindsight integration is enabled, but no hindsight bank is configured. Set 'hindsight_bank' in config or AGENT_RUN_HINDSIGHT_BANK environment variable."
            ),
        }
    } else {
        Ok(())
    }
}

/// Maximum length (in Unicode characters) of an explicit `--hindsight-query`
/// value. The Hindsight API rejects recall queries longer than ~500 tokens, so
/// a full job prompt routinely fails; callers that want a reliable recall
/// supply a short explicit query instead.
pub const HINDSIGHT_QUERY_MAX_CHARS: usize = 2000;

/// Validates and normalizes an explicit `--hindsight-query` value: it is
/// trimmed and must be non-empty and at most [`HINDSIGHT_QUERY_MAX_CHARS`]
/// Unicode characters long. `None` passes through unchanged.
pub fn validate_hindsight_query(query: &Option<String>) -> Result<Option<String>> {
    let Some(raw) = query else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(crate::core::error::AgentRunError::invalid(
            "hindsight_query_empty",
            "--hindsight-query cannot be empty after trimming",
        )
        .into());
    }
    let len = trimmed.chars().count();
    if len > HINDSIGHT_QUERY_MAX_CHARS {
        return Err(crate::core::error::AgentRunError::invalid(
            "hindsight_query_too_long",
            format!(
                "--hindsight-query is {len} characters, longer than the {HINDSIGHT_QUERY_MAX_CHARS} character limit"
            ),
        )
        .into());
    }
    Ok(Some(trimmed.to_string()))
}

/// Builds the `Command` for `hindsight memory recall`, setting environment variables if configured.
pub fn build_recall_command(
    hindsight_path: &Path,
    bank: &str,
    query: &str,
    tag: Option<&str>,
    url: Option<&str>,
    api_key: Option<&str>,
    profile: Option<&str>,
) -> Command {
    build_recall_command_with_proxy(
        hindsight_path,
        bank,
        query,
        tag,
        url,
        api_key,
        profile,
        None,
        None,
    )
}

/// Builds the `Command` for `hindsight memory recall` with proxy settings applied.
#[allow(clippy::too_many_arguments)]
pub fn build_recall_command_with_proxy(
    hindsight_path: &Path,
    bank: &str,
    query: &str,
    tag: Option<&str>,
    url: Option<&str>,
    api_key: Option<&str>,
    profile: Option<&str>,
    config: Option<&Config>,
    job_proxy: Option<&str>,
) -> Command {
    let args = build_recall_args(bank, query, tag);
    let mut cmd = Command::new(hindsight_path);
    cmd.args(&args);
    if let Some(u) = url {
        cmd.env("HINDSIGHT_API_URL", u);
    }
    if let Some(k) = api_key {
        cmd.env("HINDSIGHT_API_KEY", k);
    }
    if let Some(p) = profile {
        cmd.env("HINDSIGHT_PROFILE", p);
    }
    apply_proxy(&mut cmd, config, job_proxy);
    cmd
}

/// Executes `hindsight memory recall` and returns raw JSON stdout.
#[allow(clippy::too_many_arguments)]
pub fn execute_recall(
    hindsight_path: &Path,
    bank: &str,
    query: &str,
    tag: Option<&str>,
    url: Option<&str>,
    api_key: Option<&str>,
    profile: Option<&str>,
    config: Option<&Config>,
    job_proxy: Option<&str>,
) -> Result<String> {
    let mut cmd = build_recall_command_with_proxy(
        hindsight_path,
        bank,
        query,
        tag,
        url,
        api_key,
        profile,
        config,
        job_proxy,
    );
    let output = crate::core::reaper::output_tracked(&mut cmd).with_context(|| {
        format!(
            "Failed to execute Hindsight binary at '{}' (check that hindsight is installed and executable, or configure 'hindsight_path')",
            hindsight_path.display()
        )
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Hindsight binary at '{}' (memory recall) failed with status {}: {}",
            hindsight_path.display(),
            output.status,
            stderr.trim()
        );
    }

    let stdout =
        String::from_utf8(output.stdout).context("Invalid UTF-8 in hindsight recall output")?;
    Ok(stdout)
}

/// Builds the `Command` for `hindsight memory retain`, setting environment variables if configured.
#[allow(clippy::too_many_arguments)]
pub fn build_retain_command(
    hindsight_path: &Path,
    bank: &str,
    finding: &str,
    tag: Option<&str>,
    context: &str,
    url: Option<&str>,
    api_key: Option<&str>,
    profile: Option<&str>,
) -> Command {
    build_retain_command_with_proxy(
        hindsight_path,
        bank,
        finding,
        tag,
        context,
        url,
        api_key,
        profile,
        None,
        None,
    )
}

/// Builds the `Command` for `hindsight memory retain` with proxy settings applied.
#[allow(clippy::too_many_arguments)]
pub fn build_retain_command_with_proxy(
    hindsight_path: &Path,
    bank: &str,
    finding: &str,
    tag: Option<&str>,
    context: &str,
    url: Option<&str>,
    api_key: Option<&str>,
    profile: Option<&str>,
    config: Option<&Config>,
    job_proxy: Option<&str>,
) -> Command {
    let args = build_retain_args(bank, finding, tag, context);
    let mut cmd = Command::new(hindsight_path);
    cmd.args(&args);
    if let Some(u) = url {
        cmd.env("HINDSIGHT_API_URL", u);
    }
    if let Some(k) = api_key {
        cmd.env("HINDSIGHT_API_KEY", k);
    }
    if let Some(p) = profile {
        cmd.env("HINDSIGHT_PROFILE", p);
    }
    apply_proxy(&mut cmd, config, job_proxy);
    cmd
}

/// Executes `hindsight memory retain`.
#[allow(clippy::too_many_arguments)]
pub fn execute_retain(
    hindsight_path: &Path,
    bank: &str,
    finding: &str,
    tag: Option<&str>,
    context: &str,
    url: Option<&str>,
    api_key: Option<&str>,
    profile: Option<&str>,
    config: Option<&Config>,
    job_proxy: Option<&str>,
) -> Result<()> {
    let mut cmd = build_retain_command_with_proxy(
        hindsight_path,
        bank,
        finding,
        tag,
        context,
        url,
        api_key,
        profile,
        config,
        job_proxy,
    );
    let output = crate::core::reaper::output_tracked(&mut cmd).with_context(|| {
        format!(
            "Failed to execute Hindsight binary at '{}' (check that hindsight is installed and executable, or configure 'hindsight_path')",
            hindsight_path.display()
        )
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Hindsight binary at '{}' (memory retain) failed with status {}: {}",
            hindsight_path.display(),
            output.status,
            stderr.trim()
        );
    }

    Ok(())
}

/// Characters allowed in a Hindsight bank name.
fn is_allowed_bank_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}

/// Returns `true` when `name` is a usable Hindsight bank name.
///
/// A valid bank name is non-empty, at most 64 chars, uses only
/// `[A-Za-z0-9._-]`, does not start with `-` or `.`, and contains no `/`
/// or `..` sequence. Such names are safe to pass as a single argv element
/// to the `hindsight` CLI and to persist in job metadata.
pub fn is_valid_bank_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= 64
        && !name.starts_with('-')
        && !name.starts_with('.')
        && !name.contains('/')
        && !name.contains("..")
        && name.chars().all(is_allowed_bank_char)
}

/// Repairs an arbitrary string so it is a valid Hindsight bank name.
///
/// Each character outside `[A-Za-z0-9._-]` is replaced with `-`, leading
/// `-` and `.` characters are trimmed, consecutive runs of `-` and `.` are
/// collapsed, and the result is truncated to 64 chars. Names that
/// normalise to the empty string fall back to `"bank"`. The result always
/// satisfies [`is_valid_bank_name`].
pub fn normalize_bank_name(name: &str) -> String {
    let replaced: String = name
        .chars()
        .map(|c| if is_allowed_bank_char(c) { c } else { '-' })
        .collect();
    let trimmed = replaced.trim_start_matches(['-', '.']);
    let mut collapsed = String::with_capacity(trimmed.len());
    for c in trimmed.chars() {
        if matches!(c, '-' | '.') && collapsed.ends_with(c) {
            continue;
        }
        collapsed.push(c);
    }
    let cut: String = collapsed.chars().take(64).collect();
    if cut.is_empty() {
        "bank".to_string()
    } else {
        cut
    }
}

/// Resolves the Hindsight bank for a new job.
///
/// Precedence: explicit `--hindsight-bank` > project name >
/// config/env `hindsight_bank` fallback (only when the job has no project).
/// An explicit value is validated and rejected when invalid -- it is never
/// silently rewritten, because the operator chose it deliberately. Derived
/// values (project name, config/env fallback) are normalised instead.
/// `Ok(None)` means no bank was configured at all, which is fine as long as
/// Hindsight itself is disabled.
pub fn resolve_bank(
    explicit: Option<&str>,
    project: &str,
    fallback: Option<&str>,
) -> Result<Option<String>> {
    if let Some(bank) = explicit {
        if is_valid_bank_name(bank) {
            return Ok(Some(bank.to_string()));
        }
        bail!(
            "Invalid --hindsight-bank '{bank}': a Hindsight bank may only contain \
             [A-Za-z0-9._-], be at most 64 chars, and must not start with '-' or '.', \
             contain '/' or '..'. Omit the flag to derive the bank from the project \
             name, or fix the value."
        );
    }
    let base = if !project.trim().is_empty() {
        Some(project)
    } else {
        fallback
    };
    match base {
        Some(b) => Ok(Some(normalize_bank_name(b))),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_recall_args_with_tag() {
        let args = build_recall_args("my-bank", "Fix bug in api", Some("project:project-a"));
        assert_eq!(
            args,
            vec![
                "-o",
                "json",
                "memory",
                "recall",
                "my-bank",
                "--tags",
                "project:project-a",
                "--",
                "Fix bug in api"
            ]
        );
    }

    #[test]
    fn test_build_recall_args_without_tag() {
        let args = build_recall_args("my-bank", "Fix bug in api", None);
        assert_eq!(
            args,
            vec![
                "-o",
                "json",
                "memory",
                "recall",
                "my-bank",
                "--",
                "Fix bug in api"
            ]
        );
    }

    // HL-113: a recall query that starts with a dash (e.g. an inbound A2A
    // frame `--- A2A INCOMING MESSAGE ...`, or a literal `--help`) must be
    // passed as a positional VALUE after the `--` separator, never parsed as a
    // flag by the hindsight CLI (which would exit status 2 with
    // `unexpected argument`).
    #[test]
    fn test_build_recall_args_dash_prefixed_query_after_separator() {
        let query = "--- A2A INCOMING MESSAGE (claimed sender: task-1) ---\nhello";
        let args = build_recall_args("my-bank", query, Some("project:project-a"));
        let dash_pos = args
            .iter()
            .position(|a| a.starts_with("---"))
            .expect("the dash-prefixed query must be present");
        // The query is a value, so it must come AFTER the `--` separator (and
        // everything before the separator is options).
        let sep_pos = args.iter().position(|a| a == "--").unwrap();
        assert!(
            sep_pos < dash_pos,
            "the query must follow the '--' separator: {args:?}"
        );
        // The query is passed verbatim as a single argv element, and it is the
        // ONLY argument after the separator (it may begin with a dash - that
        // is exactly what `--` exists to protect).
        assert_eq!(args[sep_pos + 1], query);
        assert_eq!(
            args.len(),
            sep_pos + 2,
            "exactly one value after the separator"
        );

        // A literal `--help` query is a value, not a help invocation.
        let help_args = build_recall_args("my-bank", "--help", None);
        let help_pos = help_args.iter().position(|a| a == "--help").unwrap();
        let sep_pos = help_args.iter().position(|a| a == "--").unwrap();
        assert!(
            sep_pos < help_pos,
            "'--help' as a query must follow the '--' separator: {help_args:?}"
        );
    }

    fn error_code(err: &anyhow::Error) -> Option<&'static str> {
        err.chain()
            .find_map(|e| e.downcast_ref::<crate::core::error::AgentRunError>())
            .map(|t| t.code)
    }

    #[test]
    fn test_validate_hindsight_query_absent_passes_through() {
        assert_eq!(validate_hindsight_query(&None).unwrap(), None);
    }

    #[test]
    fn test_validate_hindsight_query_trims() {
        assert_eq!(
            validate_hindsight_query(&Some("  my query  ".to_string())).unwrap(),
            Some("my query".to_string())
        );
    }

    #[test]
    fn test_validate_hindsight_query_rejects_empty_after_trim() {
        for raw in ["", "   ", "\t\n"] {
            let err = validate_hindsight_query(&Some(raw.to_string())).unwrap_err();
            assert_eq!(
                err.to_string(),
                "--hindsight-query cannot be empty after trimming"
            );
            assert_eq!(error_code(&err), Some("hindsight_query_empty"));
        }
    }

    #[test]
    fn test_validate_hindsight_query_accepts_limit_and_rejects_over() {
        // Unicode (not byte) length decides: 2000 two-byte chars fit, 2001 do not.
        assert_eq!(
            validate_hindsight_query(&Some("é".repeat(HINDSIGHT_QUERY_MAX_CHARS))).unwrap(),
            Some("é".repeat(HINDSIGHT_QUERY_MAX_CHARS))
        );
        let over = "é".repeat(HINDSIGHT_QUERY_MAX_CHARS + 1);
        let err = validate_hindsight_query(&Some(over.clone())).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "--hindsight-query is {} characters, longer than the {} character limit",
                HINDSIGHT_QUERY_MAX_CHARS + 1,
                HINDSIGHT_QUERY_MAX_CHARS
            )
        );
        assert_eq!(error_code(&err), Some("hindsight_query_too_long"));
    }

    #[test]
    fn test_build_retain_args_with_custom_tag() {
        let args = build_retain_args(
            "my-bank",
            "finding text",
            Some("project:custom"),
            "context info",
        );
        assert_eq!(
            args,
            vec![
                "-o",
                "json",
                "memory",
                "retain",
                "my-bank",
                "--document-tags",
                "project:custom",
                "--context",
                "context info",
                "--async",
                "--",
                "finding text",
            ]
        );
        assert!(args.contains(&"--async".to_string()));
        // The finding is the only positional value, after the separator.
        assert_eq!(args[args.len() - 1], "finding text");
    }

    #[test]
    fn test_build_retain_args_defaults_tag_to_agent_run() {
        let args = build_retain_args("my-bank", "finding text", None, "context info");
        assert_eq!(
            args,
            vec![
                "-o",
                "json",
                "memory",
                "retain",
                "my-bank",
                "--document-tags",
                "agent-run",
                "--context",
                "context info",
                "--async",
                "--",
                "finding text",
            ]
        );
        assert!(args.contains(&"--async".to_string()));
        assert_eq!(args[args.len() - 1], "finding text");
    }

    // HL-113: a retain finding that begins with a dash must be passed as a
    // positional VALUE after the `--` separator (same clap safeguard as
    // recall).
    #[test]
    fn test_build_retain_args_dash_prefixed_finding_after_separator() {
        let finding = "--- A2A INCOMING MESSAGE (claimed sender: task-1) ---";
        let args = build_retain_args("my-bank", finding, None, "context info");
        let finding_pos = args
            .iter()
            .position(|a| a.starts_with("---"))
            .expect("the dash-prefixed finding must be present");
        let sep_pos = args.iter().position(|a| a == "--").unwrap();
        assert!(
            sep_pos < finding_pos,
            "the finding must follow the '--' separator: {args:?}"
        );
        // The finding is the single positional value (last element).
        assert_eq!(args.iter().filter(|a| **a == finding).count(), 1);
        assert_eq!(args[args.len() - 1], finding);
    }

    #[test]
    fn test_parse_recall_response_official_results_format() {
        let json = r#"{
            "results": [
                {
                    "text": "auth-proxy reloaded successfully",
                    "type": "experience"
                },
                {
                    "text": "Dry-run v4 executed successfully",
                    "type": "experience"
                }
            ]
        }"#;

        let parsed = parse_recall_response(json);
        assert!(parsed.is_some());
        let text = parsed.unwrap();
        assert!(text.contains("auth-proxy reloaded successfully"));
        assert!(text.contains("Dry-run v4 executed successfully"));
    }

    #[test]
    fn test_parse_recall_response_memories_and_content_fields() {
        let json = r#"{
            "memories": [
                { "content": "Learned that postgres port is 5432" },
                { "memory": "Redis caches sessions" }
            ]
        }"#;

        let parsed = parse_recall_response(json);
        assert!(parsed.is_some());
        let text = parsed.unwrap();
        assert!(text.contains("Learned that postgres port is 5432"));
        assert!(text.contains("Redis caches sessions"));
    }

    #[test]
    fn test_parse_recall_response_array_of_strings() {
        let json = r#"["Fact 1 about repo", "Fact 2 about repo"]"#;
        let parsed = parse_recall_response(json);
        assert!(parsed.is_some());
        let text = parsed.unwrap();
        assert_eq!(text, "Fact 1 about repo\n\nFact 2 about repo");
    }

    #[test]
    fn test_parse_recall_response_empty_or_invalid() {
        assert_eq!(parse_recall_response(r#"{"results": []}"#), None);
        assert_eq!(parse_recall_response(r#"[]"#), None);
        assert_eq!(parse_recall_response(r#"{}"#), None);
        assert_eq!(parse_recall_response("not valid json"), None);
    }

    #[test]
    fn test_prepend_recalled_context() {
        let recalled = "Remember to run cargo test before submitting.";
        let original = "Refactor payment service";
        let combined = prepend_recalled_context(recalled, original);

        let expected = "<recalled-context>\nRemember to run cargo test before submitting.\n</recalled-context>\n\nRefactor payment service";
        assert_eq!(combined, expected);
    }

    #[test]
    fn test_prepend_recalled_context_does_not_nest_when_block_already_leads() {
        let recalled = "Some fresh memory";
        let with_block = "<recalled-context>\nOld info\n</recalled-context>\n\nOriginal prompt";
        // A well-formed leading block is never wrapped again (it would nest on retries).
        assert_eq!(prepend_recalled_context(recalled, with_block), with_block);

        // A truncated block (opening tag, no closing tag) has no documented
        // marker, so it must not be wrapped either -- nesting is what made the
        // dedupe guarantee break.
        let truncated = "<recalled-context>\nOld info cut off mid-stream\nOriginal prompt";
        assert_eq!(prepend_recalled_context(recalled, truncated), truncated);

        // A plain prompt is still wrapped.
        assert_ne!(
            prepend_recalled_context(recalled, "Plain prompt"),
            "Plain prompt"
        );
        assert!(
            prepend_recalled_context(recalled, "Plain prompt").starts_with("<recalled-context>\n")
        );
    }

    #[test]
    fn test_strip_recalled_context() {
        let nested = "<recalled-context>\nOld info\n</recalled-context>\n\nOriginal prompt";
        assert_eq!(strip_recalled_context(nested), "Original prompt");

        let plain = "Original prompt without context";
        assert_eq!(
            strip_recalled_context(plain),
            "Original prompt without context"
        );

        let extracted = extract_prompt_text(nested, true);
        assert_eq!(extracted, "Original prompt");

        // An unclosing tag is not a documented marker for "everything is the
        // block": the input is left unchanged rather than silently eating the
        // user's prompt.
        let truncated = "<recalled-context>\nOld info only";
        assert_eq!(strip_recalled_context(truncated), truncated);
    }

    #[test]
    fn test_extract_prompt_text_resolves_at_file_exactly_once() {
        let tmp = tempfile::tempdir().unwrap();
        let inner = tmp.path().join("inner.txt");
        let outer = tmp.path().join("outer.txt");
        std::fs::write(&inner, "INSTRUCTIONS FROM INNER FILE").unwrap();
        // The outer file's content itself starts with `@` (an instruction
        // line): after the first resolution it must pass through untouched.
        std::fs::write(&outer, format!("@{}", inner.display())).unwrap();

        let first_pass = extract_prompt_text(&format!("@{}", outer.display()), false);
        assert_eq!(first_pass, format!("@{}", inner.display()));

        // The resolved content, treated as already resolved, is verbatim.
        assert_eq!(extract_prompt_text(&first_pass, true), first_pass);
    }

    #[test]
    fn test_extract_prompt_text_already_resolved_passes_verbatim() {
        // No @ handling at all for already-resolved prompts.
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("x.txt");
        std::fs::write(&f, "FILE CONTENT").unwrap();
        let resolved = format!("@{}", f.display());
        assert_eq!(extract_prompt_text(&resolved, true), resolved);
        // Plain text is unchanged.
        assert_eq!(extract_prompt_text("plain prompt", true), "plain prompt");
    }

    #[test]
    fn test_should_retain_gating_logic() {
        // Only Done with hindsight_enabled=true and auto_retain=true should retain
        assert!(should_retain(true, true, &JobStatus::Done));

        // Failed must NOT retain
        assert!(!should_retain(true, true, &JobStatus::Failed));

        // SelfTerminated must NOT retain
        assert!(!should_retain(true, true, &JobStatus::SelfTerminated));

        // Killed must NOT retain
        assert!(!should_retain(true, true, &JobStatus::Killed));

        // Running must NOT retain
        assert!(!should_retain(true, true, &JobStatus::Running));

        // Disabled hindsight must NOT retain
        assert!(!should_retain(false, true, &JobStatus::Done));

        // Auto-retain false must NOT retain
        assert!(!should_retain(true, false, &JobStatus::Done));
    }

    #[test]
    fn test_validate_hindsight_config() {
        // Disabled hindsight does not require bank
        assert!(validate_hindsight_config(false, &None).is_ok());

        // Enabled hindsight with bank succeeds
        assert!(validate_hindsight_config(true, &Some("project-a".to_string())).is_ok());

        // Enabled hindsight without bank fails
        assert!(validate_hindsight_config(true, &None).is_err());
        assert!(validate_hindsight_config(true, &Some("   ".to_string())).is_err());
    }

    #[test]
    fn test_build_retain_finding_truncation() {
        let short_output = "Finished successfully with 0 errors.";
        let finding = build_retain_finding("job123", "project-a", short_output);
        assert_eq!(
            finding,
            "agent-run job job123 on project project-a completed: Finished successfully with 0 errors."
        );

        let long_output = "a".repeat(300);
        let finding_long = build_retain_finding("job456", "project-a", &long_output);
        assert!(finding_long.contains(&"a".repeat(200)));
        assert!(!finding_long.contains(&"a".repeat(201)));
    }

    #[test]
    fn test_build_recall_command_env_vars_when_set() {
        let cmd = build_recall_command(
            Path::new("/usr/bin/hindsight"),
            "my-bank",
            "query text",
            Some("tag1"),
            Some("http://hindsight:8888"),
            Some("secret-key-123"),
            Some("custom-profile"),
        );
        let envs: std::collections::HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();
        assert_eq!(
            envs.get("HINDSIGHT_API_URL"),
            Some(&Some("http://hindsight:8888".to_string()))
        );
        assert_eq!(
            envs.get("HINDSIGHT_API_KEY"),
            Some(&Some("secret-key-123".to_string()))
        );
        assert_eq!(
            envs.get("HINDSIGHT_PROFILE"),
            Some(&Some("custom-profile".to_string()))
        );
    }

    #[test]
    fn test_build_recall_command_env_vars_when_unset() {
        let cmd = build_recall_command(
            Path::new("/usr/bin/hindsight"),
            "my-bank",
            "query text",
            None,
            None,
            None,
            None,
        );
        let envs: std::collections::HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();
        assert!(!envs.contains_key("HINDSIGHT_API_URL"));
        assert!(!envs.contains_key("HINDSIGHT_API_KEY"));
        assert!(!envs.contains_key("HINDSIGHT_PROFILE"));
    }

    #[test]
    fn test_build_retain_command_env_vars_when_set() {
        let cmd = build_retain_command(
            Path::new("/usr/bin/hindsight"),
            "my-bank",
            "finding",
            None,
            "context",
            Some("http://hindsight:8888"),
            Some("secret-key-123"),
            Some("custom-profile"),
        );
        let envs: std::collections::HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();
        assert_eq!(
            envs.get("HINDSIGHT_API_URL"),
            Some(&Some("http://hindsight:8888".to_string()))
        );
        assert_eq!(
            envs.get("HINDSIGHT_API_KEY"),
            Some(&Some("secret-key-123".to_string()))
        );
        assert_eq!(
            envs.get("HINDSIGHT_PROFILE"),
            Some(&Some("custom-profile".to_string()))
        );
    }

    #[test]
    fn test_build_retain_command_env_vars_when_unset() {
        let cmd = build_retain_command(
            Path::new("/usr/bin/hindsight"),
            "my-bank",
            "finding",
            None,
            "context",
            None,
            None,
            None,
        );
        let envs: std::collections::HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();
        assert!(!envs.contains_key("HINDSIGHT_API_URL"));
        assert!(!envs.contains_key("HINDSIGHT_API_KEY"));
        assert!(!envs.contains_key("HINDSIGHT_PROFILE"));
    }

    #[test]
    fn test_is_valid_bank_name_accepts_ordinary_names() {
        assert!(is_valid_bank_name("project-a"));
        assert!(is_valid_bank_name("nested-operator"));
        assert!(is_valid_bank_name("a.b_c-9"));
        assert!(is_valid_bank_name(&"a".repeat(64)));
    }

    #[test]
    fn test_is_valid_bank_name_rejects_invalid_names() {
        assert!(!is_valid_bank_name(""));
        assert!(!is_valid_bank_name("a/b"));
        assert!(!is_valid_bank_name("a..b"));
        assert!(!is_valid_bank_name("-abc"));
        assert!(!is_valid_bank_name(".abc"));
        assert!(!is_valid_bank_name("café"));
        assert!(!is_valid_bank_name("my project"));
        assert!(!is_valid_bank_name(&"a".repeat(65)));
    }

    #[test]
    fn test_normalize_bank_name_keeps_valid_names_unchanged() {
        assert_eq!(normalize_bank_name("project-a"), "project-a");
        assert_eq!(normalize_bank_name("nested-operator"), "nested-operator");
        assert_eq!(normalize_bank_name("a.b_c-9"), "a.b_c-9");
    }

    #[test]
    fn test_normalize_bank_name_repairs_invalid_names() {
        assert_eq!(normalize_bank_name("my app"), "my-app");
        assert_eq!(normalize_bank_name("a/b"), "a-b");
        // Consecutive dots collapse to a single dot.
        assert_eq!(normalize_bank_name("bad..name"), "bad.name");
        assert_eq!(normalize_bank_name(".hidden"), "hidden");
        assert_eq!(normalize_bank_name("-lead"), "lead");
        assert_eq!(normalize_bank_name("a--b"), "a-b");
        // Non-ASCII characters become '-'.
        assert_eq!(normalize_bank_name("café"), "caf-");
        // Trailing invalid chars become a single '-' (only LEADING -/. are
        // trimmed); the result is still a valid bank name.
        assert_eq!(normalize_bank_name("  spaced  "), "spaced-");
        // All-invalid input falls back to a usable name.
        assert_eq!(normalize_bank_name("---"), "bank");
        assert_eq!(normalize_bank_name(""), "bank");
        // Truncated to 64 chars.
        assert_eq!(normalize_bank_name(&"a".repeat(100)), "a".repeat(64));
    }

    #[test]
    fn test_normalize_bank_name_result_is_always_valid() {
        let long = "w".repeat(200);
        for name in [
            "",
            "a/b",
            "a..b",
            "-x",
            ".x",
            "café",
            "   ",
            long.as_str(),
            "-.a..b//c--d",
        ] {
            let normalized = normalize_bank_name(name);
            assert!(
                is_valid_bank_name(&normalized),
                "normalised {name:?} -> {normalized:?} is not a valid bank name"
            );
        }
    }

    #[test]
    fn test_resolve_bank_explicit_valid_passes_unchanged() {
        assert_eq!(
            resolve_bank(Some("project-a"), "my app", Some("f b")).unwrap(),
            Some("project-a".to_string())
        );
        // Explicit beats project and fallback.
        assert_eq!(
            resolve_bank(Some("bank-x"), "my project", Some("other")).unwrap(),
            Some("bank-x".to_string())
        );
    }

    #[test]
    fn test_resolve_bank_explicit_invalid_is_rejected() {
        let too_long = "b".repeat(65);
        for bad in [
            "bad/name",
            "a..b",
            "-x",
            ".x",
            "café",
            "my project",
            too_long.as_str(),
        ] {
            let err = resolve_bank(Some(bad), "my-project", None).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("Invalid --hindsight-bank"),
                "error should name the flag for {bad:?}: {msg}"
            );
            assert!(
                msg.contains(bad),
                "error should quote the offending value {bad:?}: {msg}"
            );
        }
    }

    #[test]
    fn test_resolve_bank_derived_values_are_normalised() {
        assert_eq!(
            resolve_bank(None, "my app", None).unwrap(),
            Some("my-app".to_string())
        );
        assert_eq!(
            resolve_bank(None, "bad..name", None).unwrap(),
            Some("bad.name".to_string())
        );
        // Empty (or blank) project falls through to the config/env fallback.
        assert_eq!(
            resolve_bank(None, "", Some("f b")).unwrap(),
            Some("f-b".to_string())
        );
        assert_eq!(
            resolve_bank(None, "   ", Some("f/b")).unwrap(),
            Some("f-b".to_string())
        );
        // Over-long derived names are truncated to 64 chars.
        assert_eq!(
            resolve_bank(None, &"a".repeat(100), None).unwrap(),
            Some("a".repeat(64))
        );
    }

    #[test]
    fn test_resolve_bank_nothing_configured_yields_none() {
        assert_eq!(
            resolve_bank(None, "", None).unwrap(),
            None,
            "hindsight disabled with no bank configured must stay Ok(None)"
        );
    }
}

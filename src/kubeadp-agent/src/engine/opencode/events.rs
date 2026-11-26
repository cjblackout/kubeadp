//! opencode JSON-event stream parser (KA-4).
//!
//! `opencode run --format json` emits one NDJSON event per line. This module
//! parses those lines into the engine-neutral [`crate::engine::run::EngineEvent`]
//! surface (verified live against opencode 1.19.4 in-cluster).
//!
//! Only the pure parts (line parse) are implemented here and unit tested; the
//! engine reads the job's log file and maps each line through this parser.

use crate::engine::run::{EngineEvent, Role, RunStatus};

/// Parses one opencode JSON-event line into zero or more [`EngineEvent`]s.
///
/// The typed surface the engine's `events()` returns. Line types:
/// - `step_start`: the harness started a step -> `Status(Running)`.
/// - `text`: a visible assistant text (`part.text`) -> `MessageDelta`.
/// - `tool_use`: a tool invocation (`part.tool`, `part.state.input`) ->
///   `ToolCall`.
/// - `step_finish`: a completed turn; only `reason:"stop"` finishes the run ->
///   `Done`.
/// - `error`: the terminal opencode error -> `Error` then `Done(Failed)`.
///
/// Non-JSON and unknown-typed lines are skipped (the raw log also carries
/// banner/progress text).
pub fn parse_opencode_line(line: &str) -> Vec<EngineEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() || !trimmed.starts_with('{') {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return Vec::new();
    };
    let mut events = Vec::new();
    match value.get("type").and_then(|t| t.as_str()) {
        Some("step_start") => events.push(EngineEvent::Status(RunStatus::Running)),
        Some("text") => {
            let text = value
                .get("part")
                .and_then(|p| p.get("text"))
                .and_then(|t| t.as_str())
                .unwrap_or_default();
            if !text.is_empty() {
                events.push(EngineEvent::MessageDelta {
                    role: Role::Assistant,
                    text: text.to_string(),
                });
            }
        }
        Some("tool_use") => {
            let part = value.get("part");
            let name = part
                .and_then(|p| p.get("tool"))
                .and_then(|t| t.as_str())
                .unwrap_or("tool")
                .to_string();
            let input = part
                .and_then(|p| p.get("state"))
                .and_then(|s| s.get("input"))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            events.push(EngineEvent::ToolCall { name, input });
        }
        Some("step_finish") => {
            // A completed turn (reason "stop") finishes the run.
            let reason = value
                .get("part")
                .and_then(|p| p.get("reason"))
                .and_then(|r| r.as_str())
                .unwrap_or("");
            if reason == "stop" {
                events.push(EngineEvent::Done {
                    status: RunStatus::Done,
                });
            }
        }
        Some("error") => {
            let message = value
                .get("error")
                .and_then(|e| e.get("data"))
                .and_then(|d| d.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("opencode error")
                .to_string();
            events.push(EngineEvent::Error { message });
            events.push(EngineEvent::Done {
                status: RunStatus::Failed,
            });
        }
        _ => {}
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real opencode 1.19.4 `run --format json` event lines (captured live
    // in-cluster): every field the parser reads is present.
    const STEP_START: &str = r#"{"type":"step_start","timestamp":1795701212743,"sessionID":"ses_abc","part":{"id":"prt_1","messageID":"msg_1","sessionID":"ses_abc","snapshot":"ced","type":"step-start"}}"#;
    const TEXT: &str = r#"{"type":"text","timestamp":1795701215581,"sessionID":"ses_abc","part":{"id":"prt_2","messageID":"msg_2","sessionID":"ses_abc","type":"text","text":"Done.","time":{"start":1795701215496,"end":1795701215577}}}"#;
    const TOOL_USE: &str = r#"{"type":"tool_use","timestamp":1795701214372,"sessionID":"ses_abc","part":{"type":"tool","tool":"bash","callID":"chatcmpl-tool-1","state":{"status":"completed","input":{"command":"echo hi"},"output":"(no output)"},"id":"prt_3","sessionID":"ses_abc","messageID":"msg_2"}}"#;
    const STEP_FINISH_STOP: &str = r#"{"type":"step_finish","timestamp":1795701215750,"sessionID":"ses_abc","part":{"id":"prt_4","reason":"stop","snapshot":"ced","messageID":"msg_2","sessionID":"ses_abc","type":"step-finish","tokens":{"total":16259,"input":127,"output":4,"reasoning":0,"cache":{"write":0,"read":16128}},"cost":0}}"#;
    const STEP_FINISH_TOOL_CALLS: &str = r#"{"type":"step_finish","timestamp":1795701214648,"sessionID":"ses_abc","part":{"id":"prt_5","reason":"tool-calls","snapshot":"ced","messageID":"msg_2","sessionID":"ses_abc","type":"step-finish","tokens":{"total":17697,"input":1506,"output":63,"reasoning":0,"cache":{"write":0,"read":16128}},"cost":0}}"#;
    const ERROR: &str = r#"{"type":"error","timestamp":1795699024851,"sessionID":"ses_err","error":{"name":"UnknownError","data":{"message":"Unexpected server error. Check server logs for details.","ref":"err_1"}}}"#;

    #[test]
    fn parses_full_lifecycle_into_typed_events() {
        let mut events = Vec::new();
        for line in [STEP_START, TEXT, TOOL_USE, STEP_FINISH_STOP] {
            events.extend(parse_opencode_line(line));
        }
        assert_eq!(events.len(), 4);
        assert!(matches!(events[0], EngineEvent::Status(RunStatus::Running)));
        assert!(matches!(
            &events[1],
            EngineEvent::MessageDelta {
                role: Role::Assistant,
                text,
            } if text == "Done."
        ));
        match &events[2] {
            EngineEvent::ToolCall { name, input } => {
                assert_eq!(name, "bash");
                assert_eq!(input["command"], "echo hi");
            }
            other => panic!("expected ToolCall, got {other:?}"),
        }
        assert!(matches!(events[3], EngineEvent::Done { .. }));
    }

    #[test]
    fn tool_call_step_does_not_finish_the_run() {
        // A `step_finish` with reason "tool-calls" is a mid-turn pause: the
        // harness continues with the tool result, so no Done is emitted.
        let events = parse_opencode_line(STEP_FINISH_TOOL_CALLS);
        assert!(
            events.is_empty(),
            "tool-calls finish must not end the run: {events:?}"
        );
    }

    #[test]
    fn error_emits_error_then_failed_done() {
        let events = parse_opencode_line(ERROR);
        assert_eq!(events.len(), 2);
        match &events[0] {
            EngineEvent::Error { message } => {
                assert!(message.contains("Unexpected server error"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
        assert!(matches!(
            events[1],
            EngineEvent::Done {
                status: RunStatus::Failed
            }
        ));
    }

    #[test]
    fn non_json_and_empty_lines_are_skipped() {
        assert!(parse_opencode_line("not json at all").is_empty());
        assert!(parse_opencode_line("   ").is_empty());
        assert!(parse_opencode_line("").is_empty());
        // A random-but-valid JSON object with no `type` is skipped.
        assert!(parse_opencode_line(r#"{"foo":42}"#).is_empty());
    }

    #[test]
    fn empty_text_part_is_not_surfaced() {
        let line =
            r#"{"type":"text","timestamp":1,"sessionID":"s","part":{"type":"text","text":""}}"#;
        assert!(parse_opencode_line(line).is_empty());
    }
}

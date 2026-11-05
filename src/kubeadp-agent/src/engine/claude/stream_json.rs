//! Headless stream-json driver (design HL-91 E2).
//!
//! Claude Code's headless job mode speaks newline-delimited JSON over stdio:
//! `claude -p --input-format stream-json --output-format stream-json`. This
//! module builds the argv, renders a user message request and parses the
//! NDJSON output stream into the engine-neutral [`crate::engine::run::RunStatus`]
//! / [`crate::engine::EngineEvent`] surface.
//!
//! Only the pure parts (argv, render, parse) are implemented here and unit
//! tested; the engine spans the actual child process in [`crate::engine::claude`].

use crate::engine::run::{EngineEvent, Role, RunStatus};

/// The claude transport modes this driver knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamJsonMode {
    /// Normal (non-interactive) run.
    Normal,
    /// `--mode plan`: plan-only turn.
    Plan,
}

/// Arguments for building a stream-json claude invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamJsonArgs {
    /// The claude binary path.
    pub binary: crate::engine::run::EngineId,
    /// The run's worktree (cwd for the child).
    pub workdir: std::path::PathBuf,
    /// The harness session id (used for `--resume` and transcript lookup).
    pub session_id: Option<crate::engine::run::ContextId>,
    /// The `--resume` target (a harness session to continue). Kept separate
    /// from `session_id` so a resume invocation can build an argv with exactly
    /// one `--resume` flag without repurposing the fresh-run fields.
    pub resume: Option<crate::engine::run::ContextId>,
    /// The `--mode` value, default `Normal`.
    pub mode: StreamJsonMode,
    /// Whether `--dangerously-skip-permissions` is passed (jobs default on).
    pub dangerous: bool,
}

impl StreamJsonArgs {
    /// Builds the claude argv.
    ///
    /// Layout:
    /// `<binary> [--dangerously-skip-permissions] [-p] [--mode <m>] --input-format
    /// stream-json --output-format stream-json [--resume <session_id|resume>]`
    pub fn argv(&self) -> Vec<String> {
        let mut argv = vec![self.binary.clone()];
        if self.dangerous {
            argv.push("--dangerously-skip-permissions".to_string());
        }
        argv.push("-p".to_string());
        argv.push("--mode".to_string());
        argv.push(match self.mode {
            StreamJsonMode::Normal => "normal".to_string(),
            StreamJsonMode::Plan => "plan".to_string(),
        });
        argv.push("--input-format".to_string());
        argv.push("stream-json".to_string());
        argv.push("--output-format".to_string());
        argv.push("stream-json".to_string());
        if let Some(ref session_id) = self.session_id {
            argv.push("--resume".to_string());
            argv.push(session_id.clone());
        }
        if let Some(ref resume_id) = self.resume {
            argv.push("--resume".to_string());
            argv.push(resume_id.clone());
        }
        argv
    }
}

/// Renders a single user message as one NDJSON request line for the stream
/// input format (the `user` type with a text content part, matching the
/// transcript line shape).
pub fn render_text_request(text: &str) -> String {
    let request = serde_json::json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{ "type": "text", "text": text }]
        }
    });
    serde_json::to_string(&request).expect("static request shape serialises")
}

/// Builds the spawn argv (`[launcher args..., <binary> <argv>]`) that mirrors
/// `core::runner::build_command`'s sandbox wrapping: production wraps the
/// claude command in `kubeadp-agent sandbox-exec --policy <file> -- <claude> ...`,
/// the `test-hooks` build passes the plain claude command through
/// ([`crate::sandbox::testhook::sandbox_passthrough`]) so hermetic unit tests
/// can drive a fake claude without a working sandbox runtime.
pub fn launch_argv(
    prog: &std::path::Path,
    args: &[String],
    policy_path: &std::path::Path,
) -> Vec<String> {
    let passthrough = {
        #[cfg(feature = "test-hooks")]
        {
            crate::sandbox::testhook::sandbox_passthrough()
        }
        #[cfg(not(feature = "test-hooks"))]
        {
            false
        }
    };
    if passthrough {
        let mut v = vec![prog.to_string_lossy().to_string()];
        v.extend_from_slice(args);
        v
    } else {
        let mut v = vec![crate::sandbox::find_launcher_exe()
            .to_string_lossy()
            .to_string()];
        v.push("sandbox-exec".to_string());
        v.push("--policy".to_string());
        v.push(policy_path.to_string_lossy().to_string());
        v.push("--".to_string());
        v.push(prog.to_string_lossy().to_string());
        v.extend_from_slice(args);
        v
    }
}

/// One parsed stream-json output line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamMessage {
    /// An `assistant` output message with text content.
    Assistant { text: String },
    /// A `user` output message with text content.
    User { text: String },
    /// A tool call from the assistant.
    ToolCall {
        name: String,
        input: serde_json::Value,
    },
    /// The run finished successfully (`event: result` / `result`).
    Done,
    /// The harness reported an error on this line.
    Error { message: String },
    /// A line this parser does not model (system messages, etc.).
    Ignored,
}

/// Parses one NDJSON output line into a [`StreamMessage`].
///
/// Handled shapes (per the stream-json contract): `assistant` / `user`
/// messages with a text content part, `tool_use` parts inside assistant
/// messages, `result` (success) and `result_is_error=true` / `system`
/// `subtype:"error"` (failure). Unknown lines parse to `Ignored`.
pub fn parse_stream_json_line(line: &str) -> Result<StreamMessage, String> {
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("invalid stream-json line: {e}"))?;
    let Some(ty) = value.get("type").and_then(|v| v.as_str()) else {
        return Ok(StreamMessage::Ignored);
    };

    if ty == "result" {
        if value.get("subtype").and_then(|v| v.as_str()) == Some("error")
            || value.get("is_error") == Some(&serde_json::Value::Bool(true))
        {
            let message = value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("stream-json result error")
                .to_string();
            return Ok(StreamMessage::Error { message });
        }
        return Ok(StreamMessage::Done);
    }

    if ty == "system" {
        if value.get("subtype").and_then(|v| v.as_str()) == Some("error") {
            let message = value
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("stream-json system error")
                .to_string();
            return Ok(StreamMessage::Error { message });
        }
        return Ok(StreamMessage::Ignored);
    }

    if ty != "assistant" && ty != "user" {
        // `stream_event` progress lines and anything else are not surfaced.
        return Ok(StreamMessage::Ignored);
    }

    let Some(message) = value.get("message") else {
        return Ok(StreamMessage::Ignored);
    };
    let Some(content) = message.get("content") else {
        return Ok(StreamMessage::Ignored);
    };
    let Some(parts) = content.as_array() else {
        return Ok(StreamMessage::Ignored);
    };

    let mut last: Option<StreamMessage> = None;
    for part in parts {
        let Some(obj) = part.as_object() else {
            continue;
        };
        match obj.get("type").and_then(|v| v.as_str()) {
            Some("text") => {
                last = Some(if ty == "assistant" {
                    StreamMessage::Assistant {
                        text: obj
                            .get("text")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    }
                } else {
                    StreamMessage::User {
                        text: obj
                            .get("text")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    }
                });
            }
            Some("tool_use") => {
                let name = obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let input = obj
                    .get("input")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
                last = Some(StreamMessage::ToolCall { name, input });
            }
            _ => {}
        }
    }

    Ok(last.unwrap_or(StreamMessage::Ignored))
}

/// Extracts the session id from a rendered stream-json message, when present
/// (the harness echoes `session_id` on some assistant/result lines).
pub fn parse_result_session_id(line: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    value
        .get("session_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Maps the parsed [`StreamMessage`] stream to events, skipping `Ignored`.
pub fn stream_events(messages: &[StreamMessage]) -> Vec<EngineEvent> {
    let mut events = Vec::new();
    for message in messages {
        match message {
            StreamMessage::Assistant { text } => {
                events.push(EngineEvent::MessageDelta {
                    role: Role::Assistant,
                    text: text.clone(),
                });
            }
            StreamMessage::User { text } => {
                events.push(EngineEvent::MessageDelta {
                    role: Role::User,
                    text: text.clone(),
                });
            }
            StreamMessage::ToolCall { name, input } => {
                events.push(EngineEvent::ToolCall {
                    name: name.clone(),
                    input: input.clone(),
                });
            }
            StreamMessage::Done => {
                events.push(EngineEvent::Done {
                    status: RunStatus::Done,
                });
            }
            StreamMessage::Error { message } => {
                events.push(EngineEvent::Error {
                    message: message.clone(),
                });
            }
            StreamMessage::Ignored => {}
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_default_job_shape() {
        let args = StreamJsonArgs {
            binary: "/usr/bin/claude".to_string(),
            workdir: std::path::PathBuf::from("/workspace/proj"),
            session_id: None,
            resume: None,
            mode: StreamJsonMode::Normal,
            dangerous: true,
        };
        let argv = args.argv();
        assert_eq!(argv[0], "/usr/bin/claude");
        assert!(argv.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(argv.contains(&"-p".to_string()));
        let mode_idx = argv.iter().position(|a| a == "--mode").unwrap();
        assert_eq!(argv[mode_idx + 1], "normal");
        let input_idx = argv.iter().position(|a| a == "--input-format").unwrap();
        assert_eq!(argv[input_idx + 1], "stream-json");
        let output_idx = argv.iter().position(|a| a == "--output-format").unwrap();
        assert_eq!(argv[output_idx + 1], "stream-json");
    }

    #[test]
    fn argv_resume_and_plan() {
        let args = StreamJsonArgs {
            binary: "claude".to_string(),
            workdir: std::path::PathBuf::from("/w"),
            session_id: Some("ctx-1".to_string()),
            resume: None,
            mode: StreamJsonMode::Plan,
            dangerous: false,
        };
        let argv = args.argv();
        let resume_idx = argv.iter().position(|a| a == "--resume").unwrap();
        assert_eq!(argv[resume_idx + 1], "ctx-1");
        let mode_idx = argv.iter().position(|a| a == "--mode").unwrap();
        assert_eq!(argv[mode_idx + 1], "plan");
        assert!(!argv.contains(&"--dangerously-skip-permissions".to_string()));
    }

    #[test]
    fn argv_resume_field_adds_resume_flag() {
        let args = StreamJsonArgs {
            binary: "claude".to_string(),
            workdir: std::path::PathBuf::from("/w"),
            session_id: None,
            resume: Some("res-1".to_string()),
            mode: StreamJsonMode::Normal,
            dangerous: false,
        };
        let argv = args.argv();
        let resume_idx = argv.iter().position(|a| a == "--resume").unwrap();
        assert_eq!(argv[resume_idx + 1], "res-1");
        assert!(argv.iter().filter(|a| a.as_str() == "--resume").count() == 1);
    }

    #[test]
    fn render_text_request_is_user_jsonl() {
        let line = render_text_request("hello");
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["type"], "user");
        assert_eq!(value["message"]["role"], "user");
        assert_eq!(value["message"]["content"][0]["text"], "hello");
    }

    fn assistant_text(text: &str) -> String {
        format!(
            r#"{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"{text}"}}]}},"session_id":"sid-9"}}"#
        )
    }

    fn assistant_with_tool() -> String {
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"listing"},{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/w/README.md"}}]}}"#
            .to_string()
    }

    #[test]
    fn parses_assistant_and_user_text() {
        assert_eq!(
            parse_stream_json_line(&assistant_text("hi")).unwrap(),
            StreamMessage::Assistant {
                text: "hi".to_string()
            }
        );
        assert_eq!(
            parse_stream_json_line(r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#)
                .unwrap(),
            StreamMessage::User {
                text: "hello".to_string()
            }
        );
    }

    #[test]
    fn parses_tool_call_part() {
        match parse_stream_json_line(&assistant_with_tool()).unwrap() {
            StreamMessage::ToolCall { name, input } => {
                assert_eq!(name, "Read");
                assert_eq!(input["file_path"], "/w/README.md");
            }
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    #[test]
    fn parses_result_and_error() {
        assert_eq!(
            parse_stream_json_line(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#
            )
            .unwrap(),
            StreamMessage::Done
        );
        assert_eq!(
            parse_stream_json_line(
                r#"{"type":"result","subtype":"error","is_error":true,"error":"boom"}"#
            )
            .unwrap(),
            StreamMessage::Error {
                message: "boom".to_string()
            }
        );
        assert_eq!(
            parse_stream_json_line(r#"{"type":"system","subtype":"error","message":"failed"}"#)
                .unwrap(),
            StreamMessage::Error {
                message: "failed".to_string()
            }
        );
    }

    #[test]
    fn ignores_progress_and_unknown() {
        assert_eq!(
            parse_stream_json_line(r#"{"type":"stream_event","event":{"type":"progress"}}"#)
                .unwrap(),
            StreamMessage::Ignored
        );
        assert!(parse_stream_json_line("not json").is_err());
        assert_eq!(
            parse_stream_json_line("42").unwrap(),
            StreamMessage::Ignored
        );
    }

    #[test]
    fn extracts_result_session_id() {
        assert_eq!(
            parse_result_session_id(&assistant_text("hi")),
            Some("sid-9".to_string())
        );
        assert_eq!(parse_result_session_id("42"), None);
    }

    #[test]
    fn stream_events_preserve_order() {
        let messages = [
            parse_stream_json_line(&assistant_text("hi")).unwrap(),
            StreamMessage::Done,
        ];
        let events = stream_events(&messages);
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            EngineEvent::MessageDelta {
                role: Role::Assistant,
                ..
            }
        ));
        assert!(matches!(events[1], EngineEvent::Done { .. }));
    }
}

//! Transcript parser (design HL-91 E2).
//!
//! Claude Code writes one newline-delimited JSON (jsonl) entry per turn to
//! `~/.claude/projects/<cwd-slug>/<session-id>.jsonl` (probe 398). This
//! module parses that file into the engine-neutral [`crate::engine::EngineEvent`]
//! surface the trait's `events()` returns.
//!
//! The parser is deliberately tolerant: lines it does not recognise (system
//! entries, unknown `type` values, messages without our known content parts,
//! tool entries without input) are skipped instead of failing the whole read,
//! so a transcript claude keeps appending to stays readable. A line that
//! cannot be parsed as JSON, or whose `message.content` holds a non-object
//! element, is a hard error so a corrupt file is surfaced rather than silently
//! dropped by [`read_transcript`].
//!
//! Line mapping (see [`parse_transcript_line`]):
//! - `type = "user"` / `"assistant"` with a `message` object -> one
//!   [`EngineEvent::MessageDelta`] per text part, one [`EngineEvent::ToolCall`]
//!   per `tool_use` part; a `tool_result` part is surfaced as a
//!   [`EngineEvent::MessageDelta`] with [`crate::engine::run::Role::Tool`].
//! - anything else -> skipped (tolerated).

use crate::engine::run::{EngineEvent, Role, TodoItem};
use std::path::Path;

/// A single parsed transcript entry, the decoded subset this module knows
/// about. Unknown entries are represented as `None` from
/// [`parse_transcript_line`]; [`parse_transcript`] filters them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptEntry {
    /// A message with text and/or tool parts, in file turn order.
    Message {
        session: String,
        role: Role,
        /// The message's own uuid; `None` when the line has none.
        uuid: Option<String>,
        /// The part index within the message (0-based, text + tool parts).
        part: usize,
        text: String,
        tool: Option<TodoItem>,
    },
}

/// Parses one jsonl line into [`TranscriptEntry`] values or tolerates it.
///
/// Returns `Ok(Vec::new())` (not an error) for lines this parser does not
/// model: unknown `type`, missing `message`, non-object message, a `message`
/// without content parts, or no matching content-part types. A line that is
/// not JSON, or whose `message.content` array holds a non-object element, is
/// a hard error so a corrupt transcript is not silently misread.
pub fn parse_transcript_line(line: &str) -> Result<Vec<TranscriptEntry>, String> {
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("invalid jsonl line: {e}"))?;
    let Some(ty) = value.get("type").and_then(|v| v.as_str()) else {
        return Ok(Vec::new());
    };
    if ty != "user" && ty != "assistant" {
        return Ok(Vec::new());
    }
    let Some(message) = value.get("message") else {
        return Ok(Vec::new());
    };
    let Some(content) = message.get("content") else {
        return Ok(Vec::new());
    };
    let Some(role) = message
        .get("role")
        .and_then(|v| v.as_str())
        .and_then(message_role)
    else {
        return Ok(Vec::new());
    };
    let session = value
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let uuid = value
        .get("uuid")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let Some(parts) = content.as_array() else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();
    for (idx, part) in parts.iter().enumerate() {
        let Some(obj) = part.as_object() else {
            return Err(format!(
                "transcript message content element {idx} is not an object"
            ));
        };
        match obj.get("type").and_then(|v| v.as_str()) {
            Some("text") => {
                let text = obj
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                out.push(TranscriptEntry::Message {
                    text,
                    tool: None,
                    role,
                    uuid: uuid.clone(),
                    session: session.clone(),
                    part: idx,
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
                out.push(TranscriptEntry::Message {
                    text: name,
                    tool: Some(TodoItem {
                        content: input.to_string(),
                        status: None,
                    }),
                    role,
                    uuid: uuid.clone(),
                    session: session.clone(),
                    part: idx,
                });
            }
            Some("tool_result") => {
                let text = obj
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                out.push(TranscriptEntry::Message {
                    text,
                    tool: None,
                    role: Role::Tool,
                    uuid: uuid.clone(),
                    session: session.clone(),
                    part: idx,
                });
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Maps a claude message `role` string to the engine-neutral [`Role`].
fn message_role(role: &str) -> Option<Role> {
    match role {
        "user" => Some(Role::User),
        "assistant" => Some(Role::Assistant),
        _ => None,
    }
}

/// Parses whole transcript content into engine events, skipping tolerated
/// entries. The returned events carry no cursor semantics themselves; the
/// caller assigns those (see [`crate::engine::claude`]).
pub fn parse_transcript(content: &str) -> Result<Vec<EngineEvent>, String> {
    let mut events = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        for entry in parse_transcript_line(line)? {
            match entry {
                TranscriptEntry::Message {
                    text, tool, role, ..
                } => {
                    if let Some(todo) = tool {
                        events.push(EngineEvent::Todo(todo));
                    } else {
                        events.push(EngineEvent::MessageDelta { role, text });
                    }
                }
            }
        }
    }
    Ok(events)
}

/// Reads a transcript file and parses it; a missing file is an empty event
/// list (a session that has not produced a transcript yet), any other read
/// or parse error is surfaced as a message.
pub fn read_transcript(path: &Path) -> Result<Vec<EngineEvent>, String> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(format!(
                "failed to read transcript '{}': {e}",
                path.display()
            ))
        }
    };
    parse_transcript(&content)
}

/// Locates a session's transcript file under a claude config directory.
///
/// Mirrors the resolution in `orchestrate::config::resolve_session_file`:
/// `~/.claude/projects/<cwd-slug>/<session-id>.jsonl`. The transcript is
/// keyed by the *canonical* workdir, so a session started in a symlinked
/// workdir still finds its file.
pub fn transcript_path(claude_dir: &Path, session_id: &str, workdir: &Path) -> std::path::PathBuf {
    let slug = crate::orchestrate::config::derive_project_slug(workdir);
    claude_dir
        .join("projects")
        .join(slug)
        .join(format!("{session_id}.jsonl"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::run::Role;

    fn line_user(text: &str) -> String {
        format!(
            r#"{{"parentUuid":null,"isSidechain":false,"userType":"external","cwd":"/workspace/proj","sessionId":"s1","version":"2.2.0","type":"user","message":{{"role":"user","content":[{{"type":"text","text":"{text}"}}]}},"uuid":"u1","timestamp":"2026-01-01T00:00:00.000Z"}}"#
        )
    }

    fn line_assistant_tool() -> String {
        r#"{"parentUuid":"u1","isSidechain":false,"cwd":"/workspace/proj","sessionId":"s1","version":"2.2.0","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"I will list files."},{"type":"tool_use","id":"toolu_1","name":"Read","input":{"file_path":"/workspace/proj/README.md"}}]},"uuid":"u2","timestamp":"2026-01-01T00:00:01.000Z"}"#
            .to_string()
    }

    fn line_tool_result() -> String {
        r##"{"parentUuid":"u2","isSidechain":false,"toolUseResult":true,"cwd":"/workspace/proj","sessionId":"s1","version":"2.2.0","type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"# proj\n"}]},"uuid":"u3","timestamp":"2026-01-01T00:00:02.000Z"}"##
            .to_string()
    }

    #[test]
    fn parses_user_text_line() {
        let entries = parse_transcript_line(&line_user("hello")).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            TranscriptEntry::Message {
                session,
                role,
                text,
                tool,
                ..
            } => {
                assert_eq!(session, "s1");
                assert_eq!(*role, Role::User);
                assert_eq!(text, "hello");
                assert!(tool.is_none());
            }
        }
    }

    #[test]
    fn parses_assistant_text_and_tool_call() {
        let entries = parse_transcript_line(&line_assistant_tool()).unwrap();
        assert_eq!(entries.len(), 2);
        match &entries[0] {
            TranscriptEntry::Message {
                role, text, tool, ..
            } => {
                assert_eq!(*role, Role::Assistant);
                assert_eq!(text, "I will list files.");
                assert!(tool.is_none());
            }
        }
        match &entries[1] {
            TranscriptEntry::Message {
                role, text, tool, ..
            } => {
                assert_eq!(*role, Role::Assistant);
                assert_eq!(text, "Read");
                let todo = tool.as_ref().expect("tool call becomes a todo");
                assert!(todo.content.contains("README.md"));
            }
        }
    }

    #[test]
    fn parses_tool_result_as_tool_role() {
        let entries = parse_transcript_line(&line_tool_result()).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            TranscriptEntry::Message {
                role, text, tool, ..
            } => {
                assert_eq!(*role, Role::Tool);
                assert_eq!(text, "# proj\n");
                assert!(tool.is_none());
            }
        }
    }

    #[test]
    fn tolerates_unknown_and_system_lines() {
        let system = r#"{"type":"system","subtype":"init","cwd":"/workspace/proj","sessionId":"s1","version":"2.2.0","message":{},"timestamp":"2026-01-01T00:00:00.000Z"}"#;
        assert_eq!(parse_transcript_line(system).unwrap(), Vec::new());
        assert!(parse_transcript_line("not json").is_err());
        assert_eq!(parse_transcript_line("42").unwrap(), Vec::new());
    }

    #[test]
    fn parse_transcript_aggregates_in_order() {
        let content = format!(
            "{}\n{}\n{}\n",
            line_user("hi"),
            line_assistant_tool(),
            line_tool_result()
        );
        let events = parse_transcript(&content).unwrap();
        assert_eq!(events.len(), 4);
        assert!(matches!(
            events[0],
            EngineEvent::MessageDelta {
                role: Role::User,
                ..
            }
        ));
        assert!(matches!(
            events[1],
            EngineEvent::MessageDelta {
                role: Role::Assistant,
                ..
            }
        ));
        assert!(matches!(events[2], EngineEvent::Todo(_)));
        assert!(matches!(
            events[3],
            EngineEvent::MessageDelta {
                role: Role::Tool,
                ..
            }
        ));
    }

    #[test]
    fn read_transcript_missing_file_is_empty() {
        let events = read_transcript(Path::new("/nonexistent/transcript.jsonl")).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn transcript_path_matches_session_resolution() {
        use std::path::PathBuf;
        let path = transcript_path(
            Path::new("/home/agent/.claude"),
            "sess-1",
            Path::new("/workspace/proj"),
        );
        assert_eq!(
            path,
            PathBuf::from("/home/agent/.claude/projects/-workspace-proj/sess-1.jsonl")
        );
    }
}

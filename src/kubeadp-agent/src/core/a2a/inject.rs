//! The A2A injection framework (design HL-113 §7.14, phase 1).
//!
//! The harness puts a queued A2A message into the run's model context at the
//! earliest safe boundary, through the engine's native context channel; the
//! injection is the acknowledgement. The channels are declared per engine by
//! the capability `a2a.inject` ([`InjectChannel`]); this module holds the
//! pieces every channel shares:
//!
//! - the framed-block join with the response byte cap (the injected text is
//!   always the framed `--- A2A INCOMING MESSAGE ---` block, untrusted data,
//!   size-capped);
//! - the hook JSON of the `hook` channel, printed by the
//!   `kubeadp-agent a2a-hook <event>` subcommand ([`run_a2a_hook`]): the
//!   subcommand reads the run's inbox, marks the messages injected and prints
//!   the hook output so the message lands in the model's context at the next
//!   boundary (nothing is killed).
//!
//! The `tool_result` channel lives in [`crate::mcp_a2a`] (the harness-owned
//! MCP server appends the queued messages to its tool results and
//! `wait_message` is a blocking receive). The `idle_turn` and `interrupt`
//! channels are the phase-2 delivery ladder.
//!
//! The hook output formats are the contract of the `hook` channel (design
//! §7.14): the Claude Code events PostToolUse and UserPromptSubmit return
//! `{"hookSpecificOutput":{"hookEventName":..,"additionalContext":..}}`,
//! Stop returns `{"decision":"block","reason":..}` so a session about to end
//! its turn continues with the message instead; agy's verified native
//! `pre-invocation` (the agy plugin `PreInvocation` hook, which runs before
//! every model call) returns
//! `{"injectSteps":[{"systemMessage":{"systemMessage":..}}]}`.

use std::io::Write;

use crate::core::a2a::inbox::{Inbox, QueuedMessage};
use crate::engine::capability::InjectChannel;

/// Cap on the injected text of one boundary. Well above the queue byte bound
/// (`crate::core::a2a::inbox::MAX_QUEUED_BYTES`, 8 KiB), so a bounded queue
/// can never produce an injection that does not fit, and well below the MCP
/// response cap so a tool result plus its injected messages stay bounded.
pub const MAX_INJECT_BYTES: usize = 16 * 1024;

/// Joins the framed texts of `messages` (oldest first, the queue order) into
/// one block, separated by a blank line. Pure (no I/O).
pub fn join_framed(messages: &[QueuedMessage]) -> String {
    messages
        .iter()
        .map(|m| m.framed.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The Claude Code / agy hook JSON for one event and the joined framed
/// messages.
///
/// - Claude Code `PostToolUse` / `UserPromptSubmit`: the message lands in the
///   model's context via `hookSpecificOutput.additionalContext`.
/// - Claude Code `Stop`: `decision: block` with the messages as the `reason`,
///   so a session about to end its turn continues with the message instead.
/// - agy `pre-invocation`: the native agy plugin `PreInvocation` hook (runs
///   before every model call, so right after each tool result); its stdout
///   `injectSteps` system message is put into the model's context.
///
/// An unknown event is an error (the contract events are the design's three
/// Claude Code events plus agy's verified `pre-invocation`).
pub fn hook_json(event: &str, framed: &str) -> Result<serde_json::Value, String> {
    match event {
        "PostToolUse" | "UserPromptSubmit" => Ok(serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": event,
                "additionalContext": framed,
            }
        })),
        "Stop" => Ok(serde_json::json!({
            "decision": "block",
            "reason": framed,
        })),
        "pre-invocation" => Ok(serde_json::json!({
            "injectSteps": [
                { "systemMessage": { "systemMessage": framed } }
            ]
        })),
        other => Err(format!(
            "unknown a2a-hook event '{other}' (expected PostToolUse, UserPromptSubmit, Stop or pre-invocation)"
        )),
    }
}

/// The `kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|
/// pre-invocation>` subcommand (the `hook` channel, design HL-113 §7.14):
/// reads the run's inbox directory (named by `KUBEADP_A2A_INBOX_DIR`), marks
/// the queued messages injected through the hook channel and prints the hook
/// JSON to stdout.
///
/// Empty inbox (or no inbox directory configured): no output, exit 0 -- a
/// hook that prints nothing injects nothing and the engine proceeds
/// normally. An unknown event or a write error: the reason on stderr, exit 2.
pub fn run_a2a_hook(args: &[String]) -> i32 {
    let Some(event) = args.first() else {
        eprintln!(
            "usage: kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation>"
        );
        return 2;
    };
    if args.len() > 1 {
        eprintln!(
            "usage: kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation>"
        );
        return 2;
    }
    // The event is validated before the inbox is touched: a wrong event is a
    // usage error even when nothing is queued.
    if !matches!(
        event.as_str(),
        "PostToolUse" | "UserPromptSubmit" | "Stop" | "pre-invocation"
    ) {
        eprintln!(
            "kubeadp-agent (a2a-hook): unknown event '{event}' (expected PostToolUse, UserPromptSubmit, Stop or pre-invocation)"
        );
        return 2;
    }
    let Some(inbox) = Inbox::from_env() else {
        // No inbox for this run (KUBEADP_A2A_INBOX_DIR unset): nothing to
        // inject, the hook is a no-op.
        return 0;
    };
    let messages = inbox.drain_capped(InjectChannel::Hook.as_str(), MAX_INJECT_BYTES);
    if messages.is_empty() {
        return 0;
    }
    let framed = join_framed(&messages);
    match hook_json(event, &framed) {
        Ok(json) => {
            let mut out = std::io::stdout();
            match (writeln!(out, "{json}"), out.flush()) {
                (Ok(()), Ok(())) => 0,
                (e, f) => {
                    if e.is_err() {
                        eprintln!(
                            "kubeadp-agent (a2a-hook): failed to write the hook output: {e:?}"
                        );
                    }
                    if f.is_err() {
                        eprintln!("kubeadp-agent (a2a-hook): failed to flush stdout: {f:?}");
                    }
                    2
                }
            }
        }
        Err(e) => {
            eprintln!("kubeadp-agent (a2a-hook): {e}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn msg(id: &str, text: &str) -> QueuedMessage {
        QueuedMessage {
            message_id: id.to_string(),
            sender: "peer".to_string(),
            hop: 1,
            context_id: None,
            task_id: None,
            framed: format!("--- A2A INCOMING MESSAGE ---\n{text}"),
            queued_at: crate::core::a2a::inbox::now_unix(),
        }
    }

    #[test]
    fn join_framed_keeps_queue_order() {
        let joined = join_framed(&[msg("m1", "first"), msg("m2", "second")]);
        let first = joined.find("first").unwrap();
        let second = joined.find("second").unwrap();
        assert!(first < second, "oldest first, got: {joined}");
        assert!(joined.contains("\n\n"));
    }

    #[test]
    fn hook_json_matches_the_design_formats() {
        // PostToolUse / UserPromptSubmit: additionalContext in the event's
        // hookSpecificOutput.
        for event in ["PostToolUse", "UserPromptSubmit"] {
            let v = hook_json(event, "MSG").unwrap();
            assert_eq!(
                v,
                serde_json::json!({
                    "hookSpecificOutput": {
                        "hookEventName": event,
                        "additionalContext": "MSG",
                    }
                })
            );
        }
        // Stop: block with the messages as the reason.
        let v = hook_json("Stop", "MSG").unwrap();
        assert_eq!(
            v,
            serde_json::json!({ "decision": "block", "reason": "MSG" })
        );
        // agy's verified native pre-invocation: the injectSteps system
        // message.
        let v = hook_json("pre-invocation", "MSG").unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "injectSteps": [
                    { "systemMessage": { "systemMessage": "MSG" } }
                ]
            })
        );
        // An unknown event is an error, never a guessed shape.
        assert!(hook_json("PreToolUse", "MSG").is_err());
    }

    #[test]
    fn run_a2a_hook_prints_the_hook_json_and_marks_injected() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        inbox.enqueue(msg("m1", "hello")).unwrap();

        std::env::set_var(crate::core::a2a::inbox::INBOX_ENV, tmp.path());
        let code = run_a2a_hook(&["PostToolUse".to_string()]);
        std::env::remove_var(crate::core::a2a::inbox::INBOX_ENV);
        assert_eq!(code, 0);
        // The message was injected through the hook channel.
        assert!(inbox.injected("m1"));
        assert!(inbox.pending().is_empty());
    }

    #[test]
    fn run_a2a_hook_is_silent_on_an_empty_or_absent_inbox() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // No inbox configured at all.
        std::env::remove_var(crate::core::a2a::inbox::INBOX_ENV);
        assert_eq!(run_a2a_hook(&["Stop".to_string()]), 0);
        // An existing, empty inbox.
        let tmp = tempdir().unwrap();
        std::env::set_var(crate::core::a2a::inbox::INBOX_ENV, tmp.path());
        let code = run_a2a_hook(&["UserPromptSubmit".to_string()]);
        std::env::remove_var(crate::core::a2a::inbox::INBOX_ENV);
        assert_eq!(code, 0);
        // A wrong event or extra arguments are usage errors.
        std::env::set_var(crate::core::a2a::inbox::INBOX_ENV, tmp.path());
        let bad_event = run_a2a_hook(&["Nope".to_string()]);
        let bad_args = run_a2a_hook(&["Stop".to_string(), "x".to_string()]);
        let no_args = run_a2a_hook(&[]);
        std::env::remove_var(crate::core::a2a::inbox::INBOX_ENV);
        assert_eq!(bad_event, 2);
        assert_eq!(bad_args, 2);
        assert_eq!(no_args, 2);
    }
}

//! The A2A injection framework (design HL-113 §7.14): the harness puts a
//! queued A2A message into the run's model context through the engine's
//! native channel; the injection is the acknowledgement. This module holds
//! the framed-block join with the response byte cap and the hook JSON of the
//! `hook` channel, printed by the `kubeadp-agent a2a-hook <event>`
//! subcommand ([`run_a2a_hook`]). The `tool_result` channel lives in
//! [`crate::mcp_a2a`]; the `idle_turn`/`interrupt` channels are driven by
//! the escalation ladder ([`crate::core::a2a::delivery`]).
//!
//! Hook formats: Claude PostToolUse/UserPromptSubmit return
//! `hookSpecificOutput.additionalContext`, Stop returns
//! `{"decision":"block",...}` (the session continues with the message); agy
//! `pre-invocation` returns `injectSteps` (or `{}` when empty),
//! `post-invocation` adds `terminationBehavior: force_continue` (the agy
//! `idle_turn` channel) and `stop` returns `{"decision":"continue",...}`.

use std::io::Write;

use crate::core::a2a::inbox::QueuedMessage;
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
/// - agy `post-invocation`: re-enters the loop with `injectSteps` plus
///   `terminationBehavior: force_continue` (the agy `idle_turn` channel).
/// - agy `stop`: `decision: continue` with the messages as the `reason`
///   (the agy idle-turn channel at the Stop boundary).
///
/// An unknown event is an error (the contract events are the design's three
/// Claude Code events plus agy's verified `pre-invocation`/`post-invocation`/
/// `stop`).
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
        "post-invocation" => Ok(serde_json::json!({
            "injectSteps": [
                { "systemMessage": { "systemMessage": framed } }
            ],
            "terminationBehavior": "force_continue",
        })),
        "stop" => Ok(serde_json::json!({
            "decision": "continue",
            "reason": framed,
        })),
        other => Err(format!(
            "unknown a2a-hook event '{other}' (expected PostToolUse, UserPromptSubmit, Stop, pre-invocation, post-invocation or stop)"
        )),
    }
}

/// The injection channel a hook event delivers through (the inbox marker's
/// `channel` field). The mid-turn hooks (PostToolUse, UserPromptSubmit,
/// agy's PreInvocation) are the `hook` channel; the turn-boundary hooks that
/// re-enter the loop (Claude Code's Stop, agy's PostInvocation/Stop) are the
/// `idle_turn` channel.
fn event_channel(event: &str) -> &'static str {
    match event {
        "Stop" | "post-invocation" | "stop" => InjectChannel::IdleTurn.as_str(),
        _ => InjectChannel::Hook.as_str(),
    }
}

/// Whether an event is an agy native plugin hook: agy expects a JSON object
/// on stdout for every invocation, so an empty inbox prints `{}` (which
/// injects nothing) instead of no output.
fn is_agy_event(event: &str) -> bool {
    matches!(event, "pre-invocation" | "post-invocation" | "stop")
}

/// The `kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|
/// pre-invocation|post-invocation|stop>` subcommand (the `hook` channel,
/// design HL-113 §7.14): reads the run's inbox directory (named by
/// `KUBEADP_A2A_INBOX_DIR`), marks the queued messages injected through the
/// event's channel and prints the hook JSON to stdout.
///
/// Claude Code events print nothing and exit 0 on an empty inbox (a hook
/// that prints nothing injects nothing). The agy native plugin events print
/// `{}` on an empty inbox (agy parses the stdout of every invocation; `{}`
/// injects nothing). An unknown event or a write error: the reason on
/// stderr, exit 2.
pub fn run_a2a_hook(args: &[String]) -> i32 {
    let Some(event) = args.first() else {
        eprintln!(
            "usage: kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation|post-invocation|stop>"
        );
        return 2;
    };
    if args.len() > 1 {
        eprintln!(
            "usage: kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation|post-invocation|stop>"
        );
        return 2;
    }
    // The event is validated before the inbox is touched: a wrong event is a
    // usage error even when nothing is queued.
    if !matches!(
        event.as_str(),
        "PostToolUse" | "UserPromptSubmit" | "Stop" | "pre-invocation" | "post-invocation" | "stop"
    ) {
        eprintln!(
            "kubeadp-agent (a2a-hook): unknown event '{event}' (expected PostToolUse, UserPromptSubmit, Stop, pre-invocation, post-invocation or stop)"
        );
        return 2;
    }
    // N4 (fail closed): inside a relay-sandboxed run the pod-local TCP inbox
    // API is unreachable (loopback-only netns, the relay refuses loopback).
    // Fail loudly instead of silently seeing an empty inbox, whatever the
    // receive side is.
    if let Some(reason) = crate::core::a2a::receive::inbox_unreachable_reason() {
        eprintln!("kubeadp-agent (a2a-hook): {reason}");
        return 2;
    }
    let Some(receive) = crate::core::a2a::receive::ReceiveInbox::from_env() else {
        // No inbox for this run (KUBEADP_A2A_INBOX_DIR unset): nothing to
        // inject. The agy events still print `{}` (agy parses the stdout of
        // every hook invocation).
        return if is_agy_event(event) {
            print_agy_empty()
        } else {
            0
        };
    };
    let channel = event_channel(event);
    // N10 (at-least-once receive): the hook CLAIMS (leases) the messages,
    // prints the hook JSON and only then acknowledges each one. A crash in
    // between re-delivers the message after its lease lapses; a duplicate
    // print is harmless. A write error hands the claim back.
    let messages = receive.claim_capped(MAX_INJECT_BYTES);
    if messages.is_empty() {
        return if is_agy_event(event) {
            print_agy_empty()
        } else {
            0
        };
    }
    let framed = join_framed(&messages);
    match hook_json(event, &framed) {
        Ok(json) => {
            let mut out = std::io::stdout();
            match (writeln!(out, "{json}"), out.flush()) {
                (Ok(()), Ok(())) => {
                    // The text reached stdout: acknowledge each claimed
                    // message afterwards.
                    for m in &messages {
                        receive.ack(channel, &m.message_id);
                    }
                    0
                }
                (e, f) => {
                    for m in &messages {
                        receive.release(&m.message_id);
                    }
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
            for m in &messages {
                receive.release(&m.message_id);
            }
            eprintln!("kubeadp-agent (a2a-hook): {e}");
            2
        }
    }
}

/// Prints the empty agy hook object (`{}`): agy parses the stdout of every
/// plugin hook invocation, and `{}` injects nothing.
fn print_agy_empty() -> i32 {
    let mut out = std::io::stdout();
    match (writeln!(out, "{{}}"), out.flush()) {
        (Ok(()), Ok(())) => 0,
        (e, f) => {
            if e.is_err() {
                eprintln!("kubeadp-agent (a2a-hook): failed to write the hook output: {e:?}");
            }
            if f.is_err() {
                eprintln!("kubeadp-agent (a2a-hook): failed to flush stdout: {f:?}");
            }
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::inbox::Inbox;
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
        // agy post-invocation: injectSteps plus the force_continue
        // termination behavior (the agy idle-turn channel).
        let v = hook_json("post-invocation", "MSG").unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "injectSteps": [
                    { "systemMessage": { "systemMessage": "MSG" } }
                ],
                "terminationBehavior": "force_continue",
            })
        );
        // agy stop: decision continue with the messages as the reason.
        let v = hook_json("stop", "MSG").unwrap();
        assert_eq!(
            v,
            serde_json::json!({ "decision": "continue", "reason": "MSG" })
        );
        // An unknown event is an error, never a guessed shape.
        assert!(hook_json("PreToolUse", "MSG").is_err());
        assert!(hook_json("SessionStart", "MSG").is_err());
    }

    #[test]
    fn run_a2a_hook_prints_the_hook_json_and_marks_injected() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        inbox.enqueue(msg("m1", "hello")).unwrap();

        let code = crate::core::a2a::testenv::with_env(
            &[(
                crate::core::a2a::inbox::INBOX_ENV,
                Some(tmp.path().to_str().unwrap()),
            )],
            || run_a2a_hook(&["PostToolUse".to_string()]),
        );
        assert_eq!(code, 0);
        // The message was injected through the hook channel.
        assert!(inbox.injected("m1"));
        assert!(inbox.pending().is_empty());
    }

    #[test]
    fn run_a2a_hook_is_silent_on_an_empty_or_absent_inbox() {
        // No inbox configured at all: the Claude events print nothing.
        crate::core::a2a::testenv::with_env(&[], || {
            assert_eq!(run_a2a_hook(&["Stop".to_string()]), 0);
        });
        // An existing, empty inbox.
        let tmp = tempdir().unwrap();
        crate::core::a2a::testenv::with_env(
            &[(
                crate::core::a2a::inbox::INBOX_ENV,
                Some(tmp.path().to_str().unwrap()),
            )],
            || {
                assert_eq!(run_a2a_hook(&["UserPromptSubmit".to_string()]), 0);
                // A wrong event or extra arguments are usage errors.
                assert_eq!(run_a2a_hook(&["Nope".to_string()]), 2);
                assert_eq!(run_a2a_hook(&["Stop".to_string(), "x".to_string()]), 2);
                assert_eq!(run_a2a_hook(&[]), 2);
            },
        );
    }

    #[test]
    fn run_a2a_hook_marks_injected_through_the_events_channel() {
        // The mid-turn hooks (PostToolUse, UserPromptSubmit, agy's
        // pre-invocation) inject through the `hook` channel; the
        // turn-boundary hooks that re-enter the loop (Claude's Stop, agy's
        // post-invocation/stop) inject through the `idle_turn` channel.
        assert_eq!(event_channel("PostToolUse"), "hook");
        assert_eq!(event_channel("UserPromptSubmit"), "hook");
        assert_eq!(event_channel("pre-invocation"), "hook");
        assert_eq!(event_channel("Stop"), "idle_turn");
        assert_eq!(event_channel("post-invocation"), "idle_turn");
        assert_eq!(event_channel("stop"), "idle_turn");
        assert!(!is_agy_event("Stop"));
        assert!(!is_agy_event("PostToolUse"));
        for event in ["pre-invocation", "post-invocation", "stop"] {
            assert!(is_agy_event(event), "{event}");
        }
    }
}

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
//! `idle_turn` channel) and `stop` returns `{"decision":"continue",...}`;
//! opencode `transform` prints the plain joined framed text (the harness
//! plugin splices it into the outgoing model messages, KA-7).
//!
//! User-visible echo (HL-125 3.7): the Claude Code hooks additionally answer
//! a top-level `systemMessage` -- a short visible line per incoming message
//! ("A2A from \<sender\> (hop n): \<first 200 chars\>") -- so the user sees
//! every message in the transcript while the full framed text stays model-only.
//! `systemMessage` is a universal Claude Code hook-output field Claude Code
//! shows to the user (a sibling of `hookSpecificOutput`, `continue`,
//! `stopReason`, `decision`, `reason`; nested inside `hookSpecificOutput` it
//! would be ignored). agy, which already delivers a model-visible
//! `systemMessage` through `injectSteps` and has no transcript the harness
//! reaches, is left unchanged. The whole echo is behind ONE switch
//! ([`A2A_ECHO_ENV`]/`KUBEADP_A2A_ECHO`, default on).

use std::io::Write;

use crate::core::a2a::inbox::QueuedMessage;
use crate::engine::capability::InjectChannel;

/// Cap on the injected text of one boundary. Well above the queue byte bound
/// (`crate::core::a2a::inbox::MAX_QUEUED_BYTES`, 8 KiB), so a bounded queue
/// can never produce an injection that does not fit, and well below the MCP
/// response cap so a tool result plus its injected messages stay bounded.
pub const MAX_INJECT_BYTES: usize = 16 * 1024;

/// The user-visible A2A echo (HL-125 3.7): in addition to the full framed text
/// in the model channel (`additionalContext`), the Claude Code hooks answer a
/// `systemMessage` line the transcript shows, so the user SEES every incoming
/// message ("A2A from \<sender\> (hop n): \<first 200 characters\>") and every
/// outgoing `send_message`/`reply` ("A2A to \<target\> (hop n): ...").
/// `systemMessage` is a TOP-LEVEL universal hook-output field Claude Code
/// shows to the user (a sibling of `hookSpecificOutput`, `continue`,
/// `stopReason`, `decision`, `reason`); inside `hookSpecificOutput` -- which
/// only carries `hookEventName` and event-specific fields such as
/// `additionalContext` -- it would be ignored. Even at the full byte cap of an
/// inbox message ([`crate::core::a2a::ingress::MAX_INGRESS_TEXT_BYTES`] +
/// framing) the echo stays a single short line.
///
/// Hard cap on the echoed message TEXT (characters), enforced on every echo
/// line including the outgoing tools: the echoed content never exceeds it.
pub const ECHO_CHAR_CAP: usize = 200;

/// The environment variable that turns the A2A echo ON or OFF inside a run
/// (`a2a-hook`, `mcp-a2a`). The launcher sets it from the resolved host config
/// (`[worker] a2a_echo` / `AGENT_RUN_A2A_ECHO`). The `KUBEADP_*` prefix keeps
/// it inside the default sandbox `env_allow` pattern, so the sandbox passes it
/// through.
pub const A2A_ECHO_ENV: &str = "KUBEADP_A2A_ECHO";

/// Joins the framed texts of `messages` (oldest first, the queue order) into
/// one block, separated by a blank line. Pure (no I/O).
pub fn join_framed(messages: &[QueuedMessage]) -> String {
    messages
        .iter()
        .map(|m| m.framed.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Whether the user-visible A2A echo is ON in this run. Reads
/// [`A2A_ECHO_ENV`] (`KUBEADP_A2A_ECHO`). Default ON. Recognized true values
/// are `1`/`true`/`yes`/`on`, false values `0`/`false`/`no`/`off`; an
/// unrecognized value (a garbled override) falls back to the default ON
/// rather than silently flipping delivery-visible behaviour either way.
pub fn a2a_echo_enabled() -> bool {
    match std::env::var(A2A_ECHO_ENV) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => true,
        },
        Err(_) => true,
    }
}

/// The echo line for ONE message: `A2A <direction> <peer> (hop <hop>): <text>`
/// (`direction` is "from" for incoming, "to" for outgoing; `text` is the
/// single-line, [`ECHO_CHAR_CAP`]-capped message text). Pure (no I/O).
pub fn a2a_echo_line(direction: &str, peer: &str, hop: u32, text: &str) -> String {
    format!("A2A {direction} {peer} (hop {hop}): {}", echo_text(text))
}

/// The user-visible echo block for the queued inbound `messages` (oldest
/// first, one line per message), so the transcript shows EVERY incoming
/// message. Empty when there is nothing to show. Pure (no I/O).
pub fn a2a_echo_block(messages: &[QueuedMessage]) -> String {
    messages
        .iter()
        .map(|m| a2a_echo_line("from", &m.sender, m.hop, &message_text(m.framed.as_str())))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The message text a framing carries without the frame: the content between
/// the `--- BEGIN CONTENT (nonce) ---` and `--- END CONTENT (nonce) ---`
/// delimiters of `core::a2a::ingress::frame_message`; for a frame without the
/// delimiters (the older `--- A2A INCOMING MESSAGE ---` test form) everything
/// after the header line. Falls back to the whole `framed` text when not even
/// a header is present. The caller sanitizes/caps it for the user-visible
/// echo. Pure (no I/O).
fn message_text(framed: &str) -> String {
    if let (Some(begin), Some(end)) = (
        framed.find("--- BEGIN CONTENT ("),
        framed.rfind("\n--- END CONTENT ("),
    ) {
        let start = framed[begin..]
            .find('\n')
            .map(|i| begin + i + 1)
            .unwrap_or(begin);
        return framed[start..end].to_string();
    }
    if let Some(rest) = framed.strip_prefix("--- A2A INCOMING MESSAGE") {
        if let Some(cs) = rest.strip_prefix(" (claimed sender: ") {
            if let Some(start) = cs.find(") ---\n") {
                return cs[start + ") ---\n".len()..].to_string();
            }
        }
        if let Some(text) = rest.strip_prefix(" ---\n") {
            return text.to_string();
        }
        return rest.to_string();
    }
    framed.to_string()
}

/// The capped single-line echo text: control characters are replaced by a
/// space (newline/tab) or dropped (the rest), so an echo can never smuggle
/// escape sequences or span transcript lines; then whitespace-collapsed and
/// trimmed, and cut at [`ECHO_CHAR_CAP`] characters with an explicit ellipsis
/// -- the echoed content NEVER exceeds the cap. Pure (no I/O).
fn echo_text(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        if c.is_control() {
            if matches!(c, '\n' | '\t' | '\r') {
                out.push(' ');
            }
        } else {
            out.push(c);
        }
    }
    let collapsed: String = out.split_whitespace().collect::<Vec<_>>().join(" ");
    let collapsed = collapsed.trim();
    if collapsed.chars().count() > ECHO_CHAR_CAP {
        let head: String = collapsed.chars().take(ECHO_CHAR_CAP - 1).collect();
        format!("{head}\u{2026}")
    } else {
        collapsed.to_string()
    }
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

/// The hook JSON with the user-visible echo included: [`hook_json`] with a
/// TOP-LEVEL editable `systemMessage` added (`systemMessage` is a universal
/// Claude Code hook-output field shown to the user, a sibling of
/// `hookSpecificOutput` / `continue` / `stopReason` / `decision` / `reason`;
/// nested inside `hookSpecificOutput` it would be ignored). The full framed
/// text stays ONLY in the model channel (`additionalContext` / `reason` /
/// `injectSteps`); `systemMessage` is the transcript-visible echo the Claude
/// Code hooks answer so the user SEES each incoming message ("A2A from
/// \<sender\> (hop n): ...").
///
/// - Claude Code `PostToolUse` / `UserPromptSubmit`: the echo line (or the
///   joined lines) as a top-level `systemMessage` next to the
///   `hookSpecificOutput.additionalContext` model channel.
/// - Claude Code `Stop`: `{"decision":"block","reason":<framed>}` plus the
///   echo line as a top-level `systemMessage` next to it (the turn re-enters
///   with `decision: block`, so the transcript shows the echo while the model
///   sees the full framed text).
/// - agy events are left unchanged: agy already delivers a *model-visible*
///   `systemMessage` through `injectSteps` and has no transcript the harness
///   reaches, so an echo there would only dilute the injected model context.
///
/// Returns None for the agy events.
pub fn hook_json_with_echo(
    event: &str,
    framed: &str,
    echo: &str,
) -> Result<Option<serde_json::Value>, String> {
    if is_agy_event(event) {
        return Ok(None);
    }
    let mut value = hook_json(event, framed)?;
    let echo_object = value
        .as_object_mut()
        .ok_or_else(|| "non-object hook JSON".to_string())?;
    echo_object.insert("systemMessage".to_string(), serde_json::json!(echo));
    Ok(Some(value))
}

/// The injection channel a hook event delivers through (the inbox marker's
/// `channel` field). The mid-turn hooks (PostToolUse, UserPromptSubmit,
/// agy's PreInvocation, opencode's `transform`) are the `hook` channel; the
/// turn-boundary hooks that re-enter the loop (Claude Code's Stop, agy's
/// PostInvocation/Stop) are the `idle_turn` channel.
fn event_channel(event: &str) -> &'static str {
    match event {
        "Stop" | "post-invocation" | "stop" => InjectChannel::IdleTurn.as_str(),
        _ => InjectChannel::Hook.as_str(),
    }
}

/// Whether an event is an agy native plugin hook: agy expects a JSON object
/// on stdout for every invocation, so an empty inbox prints `{}` (which
/// injects nothing) instead of no output. The opencode `transform` event
/// behaves the same way: the plugin reads stdout and expects a parseable
/// token (`{}` for empty) so it never mistakes "no output" for a failure.
fn is_agy_event(event: &str) -> bool {
    matches!(
        event,
        "pre-invocation" | "post-invocation" | "stop" | "transform"
    )
}

/// Whether an event prints the plain joined framed text (opencode's
/// `transform`) instead of a hook JSON object. The opencode plugin splices
/// the text directly into the model messages.
fn is_opencode_transform(event: &str) -> bool {
    matches!(event, "transform")
}

/// The `kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|
/// pre-invocation|post-invocation|stop|transform>` subcommand (the `hook`
/// channel, design HL-113 §7.14): reads the run's inbox directory (named by
/// `KUBEADP_A2A_INBOX_DIR`), marks the queued messages injected through the
/// event's channel and prints the hook JSON to stdout.
///
/// Claude Code events print nothing and exit 0 on an empty inbox (a hook
/// that prints nothing injects nothing). The agy native plugin events print
/// `{}` on an empty inbox (agy parses the stdout of every invocation; `{}`
/// injects nothing). The opencode `transform` event prints the JOINED FRAMED
/// TEXT on stdout (opencode's `experimental.chat.messages.transform` plugin
/// reads it and appends it to the model messages; `{}` when empty), never a
/// hook JSON.
///
/// Every failure (usage error, an unreachable/stuck inbox, a failed write)
/// is NON-blocking: the clear reason goes to stderr and the hook exits `1`,
/// for every event, so a hub/inbox outage never blocks a prompt or loops a
/// turn on Claude Code hooks (exit `2` would be a blocking hook error there;
/// the deliberate `decision: block` / `decision: continue` re-entry is
/// answered on stdout JSON, never on the exit code) and stays non-blocking
/// on the agy hooks. A hook never exits `2`.
pub fn run_a2a_hook(args: &[String]) -> i32 {
    let Some(event) = args.first() else {
        eprintln!(
            "usage: kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation|post-invocation|stop|transform>"
        );
        return 1;
    };
    if args.len() > 1 {
        eprintln!(
            "usage: kubeadp-agent a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation|post-invocation|stop|transform>"
        );
        return 1;
    }
    // The event is validated before the inbox is touched: a wrong event is a
    // usage error even when nothing is queued.
    if !matches!(
        event.as_str(),
        "PostToolUse"
            | "UserPromptSubmit"
            | "Stop"
            | "pre-invocation"
            | "post-invocation"
            | "stop"
            | "transform"
    ) {
        eprintln!(
            "kubeadp-agent (a2a-hook): unknown event '{event}' (expected PostToolUse, UserPromptSubmit, Stop, pre-invocation, post-invocation, stop or transform)"
        );
        return 1;
    }
    // N4 (fail closed): inside a relay-sandboxed run the pod-local TCP inbox
    // API is unreachable (loopback-only netns, the relay refuses loopback).
    // Fail loudly (exit 1, non-blocking) instead of silently seeing an empty
    // inbox, whatever the receive side is.
    if let Some(reason) = crate::core::a2a::receive::inbox_unreachable_reason() {
        eprintln!("kubeadp-agent (a2a-hook): {reason}");
        return 1;
    }
    let Some(receive) = crate::core::a2a::receive::ReceiveInbox::from_env() else {
        // No inbox for this run (KUBEADP_A2A_INBOX_DIR unset): nothing to
        // inject. The agy/opencode events still print `{}` (agy parses the
        // stdout of every hook invocation; the opencode plugin reads a token).
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
    // FAIL CLOSED, NON-BLOCKING: an unreachable or stuck pod-local inbox is
    // never silently read as empty -- the reason goes to stderr and the hook
    // exits 1 (any exit code other than 0 and 2 is a non-blocking error on
    // Claude Code hooks; a hub/inbox outage must never block a prompt or
    // loop a turn). The engine still notices the delivery is broken instead
    // of proceeding as if the inbox were empty, without blocking.
    let messages = match receive.claim_capped(MAX_INJECT_BYTES) {
        Ok(messages) => messages,
        Err(e) => {
            eprintln!("kubeadp-agent (a2a-hook): {e}");
            return 1;
        }
    };
    if messages.is_empty() {
        return if is_agy_event(event) {
            print_agy_empty()
        } else {
            0
        };
    }
    let framed = join_framed(&messages);
    let output = hook_stdout_payload(event, &framed, &messages);
    match output {
        Ok(json) => {
            let mut out = std::io::stdout();
            match (writeln!(out, "{json}"), out.flush()) {
                (Ok(()), Ok(())) => {
                    // The text reached stdout: acknowledge each claimed
                    // message afterwards. An ack that fails closed (the host
                    // went away between claim and ack) is NOT silent: the
                    // reason goes to stderr; the claim lease then lapses and
                    // the message is re-delivered later (at-least-once), so
                    // the hook still exits 0 -- the text DID reach the
                    // channel the moment stdout was written.
                    let mut ack_failed = false;
                    for m in &messages {
                        if let Err(e) = receive.ack(channel, &m.message_id) {
                            ack_failed = true;
                            eprintln!(
                                "kubeadp-agent (a2a-hook): ack for {} failed: {e}",
                                m.message_id
                            );
                        }
                    }
                    if ack_failed {
                        eprintln!(
                            "kubeadp-agent (a2a-hook): at least one ack failed; the message(s) will be re-delivered after the claim lease"
                        );
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
                    // Non-blocking: a hook that cannot deliver its own output
                    // must never block a prompt or loop a turn (exit 1).
                    1
                }
            }
        }
        Err(e) => {
            for m in &messages {
                receive.release(&m.message_id);
            }
            eprintln!("kubeadp-agent (a2a-hook): {e}");
            1
        }
    }
}

/// The stdio payload for one hook event and the joined framed messages.
///
/// - opencode `transform` prints the plain joined framed text (the plugin
///   splices it into the model messages); `{}` when nothing is queued (the
///   plugin reads a token). No echo: opencode has no transcript the harness
///   reaches.
/// - every other event delegates to [`hook_output`] (the Claude/agy JSON).
fn hook_stdout_payload(
    event: &str,
    framed: &str,
    messages: &[QueuedMessage],
) -> Result<String, String> {
    if is_opencode_transform(event) {
        return Ok(framed.to_string());
    }
    hook_output(event, framed, messages).map(|v| v.to_string())
}

/// The hook JSON for `event`/`framed` with the user-visible echo included:
/// [`hook_json`] with the top-level `systemMessage`
/// (one short line per incoming message) added for the Claude Code events,
/// driven by the one [`a2a_echo_enabled`] switch. agy events are left
/// unchanged. The full framed text always stays ONLY in the model channel.
/// Pure (no I/O), so tests can check the exact output without stdout capture.
fn hook_output(
    event: &str,
    framed: &str,
    messages: &[QueuedMessage],
) -> Result<serde_json::Value, String> {
    if !is_agy_event(event) && a2a_echo_enabled() {
        let echo = a2a_echo_block(messages);
        if !echo.is_empty() {
            return hook_json_with_echo(event, framed, &echo)
                .map(|v| v.expect("non-agy echo is Some"));
        }
    }
    hook_json(event, framed)
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
            // Non-blocking (exit 1), like every other hook failure.
            1
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
    fn a2a_echo_line_keeps_the_model_channel_and_caps_the_text() {
        // The full framed text stays in the model channel; the echo is the
        // short visible line (HL-125 3.7).
        let v = hook_json_with_echo("PostToolUse", "FULL", "A2A from peer (hop 1): hello")
            .unwrap()
            .unwrap();
        assert_eq!(
            v["hookSpecificOutput"]["additionalContext"], "FULL",
            "the full framed text stays in the model channel"
        );
        assert_eq!(v["systemMessage"], "A2A from peer (hop 1): hello");
        assert!(
            v["hookSpecificOutput"].get("systemMessage").is_none(),
            "systemMessage is a top-level field, never nested in hookSpecificOutput"
        );
        // Stop: block + top-level echo next to it.
        let v = hook_json_with_echo("Stop", "FULL", "A2A from peer (hop 2): hi")
            .unwrap()
            .unwrap();
        assert_eq!(v["decision"], "block");
        assert_eq!(v["reason"], "FULL");
        assert_eq!(v["systemMessage"], "A2A from peer (hop 2): hi");
        assert!(
            v["hookSpecificOutput"].get("systemMessage").is_none(),
            "Stop: no systemMessage inside hookSpecificOutput"
        );
        // agy events are left unchanged (no echo): None.
        for event in ["pre-invocation", "post-invocation", "stop"] {
            assert!(
                hook_json_with_echo(event, "FULL", "e").unwrap().is_none(),
                "{event}"
            );
        }
        // The echo line itself: direction, peer, hop and capped text.
        assert_eq!(
            a2a_echo_line("from", "peer", 1, "hello"),
            "A2A from peer (hop 1): hello"
        );
        assert_eq!(
            a2a_echo_line("to", "ops/peer", 3, "thanks"),
            "A2A to ops/peer (hop 3): thanks"
        );
        // The cap: at/under 200 the text survives; above it the line cuts at
        // exactly 200 chars with the ellipsis (never more than the cap).
        let at_cap = "x".repeat(ECHO_CHAR_CAP);
        assert_eq!(echo_text(&at_cap).chars().count(), ECHO_CHAR_CAP);
        let over = "y".repeat(ECHO_CHAR_CAP + 50);
        let cut = echo_text(&over);
        assert!(cut.ends_with('\u{2026}'));
        assert!(
            cut.chars().count() < ECHO_CHAR_CAP + 2,
            "cut stays within the cap"
        );
        // Control characters never reach the echo (escape sequences, CR, NUL
        // are dropped/collapsed; newline/tab become a space).
        assert!(!echo_text("a\u{1b}b\u{0}c").contains('\u{1b}'));
        assert!(!echo_text("a\r\nb\tc").contains('\n'));
        assert!(!echo_text("a\nb").contains('\n'));
        // The switch defaults ON and only the recognized values turn it off.
        let _g = crate::test_env_lock();
        std::env::remove_var(A2A_ECHO_ENV);
        assert!(a2a_echo_enabled(), "default ON");
        for off in ["0", "false", "no", "off"] {
            std::env::set_var(A2A_ECHO_ENV, off);
            assert!(!a2a_echo_enabled(), "{off} turns it off");
        }
        for (on, _) in [("1"), ("true"), ("yes"), ("on")]
            .into_iter()
            .map(|v| (v, ()))
        {
            std::env::set_var(A2A_ECHO_ENV, on);
            assert!(a2a_echo_enabled(), "{on} turns it on");
        }
        std::env::set_var(A2A_ECHO_ENV, "garbage");
        assert!(
            a2a_echo_enabled(),
            "a garbled override falls back to the default ON"
        );
        std::env::remove_var(A2A_ECHO_ENV);
        drop(_g);
    }

    #[test]
    fn a2a_echo_block_takes_the_message_text_not_the_frame() {
        // The classic production frame: the echo shows only the content, not
        // the nonce-delimited frame headers.
        let framed =
            "--- A2A INCOMING MESSAGE (claimed sender: peer, hop: 1) ---\nabc\n--- BEGIN CONTENT (deadbeef) ---\nthe actual message\n--- END CONTENT (deadbeef) ---";
        let m = QueuedMessage {
            message_id: "m1".to_string(),
            sender: "peer".to_string(),
            hop: 1,
            context_id: None,
            task_id: None,
            framed: framed.to_string(),
            queued_at: crate::core::a2a::inbox::now_unix(),
        };
        let block = a2a_echo_block(std::slice::from_ref(&m));
        assert_eq!(block, "A2A from peer (hop 1): the actual message");
        assert!(!block.contains("BEGIN CONTENT"));
        assert!(!block.contains("END CONTENT"));
        // The simpler framed test form without nonces.
        let m = QueuedMessage {
            message_id: "m2".to_string(),
            sender: "peer".to_string(),
            hop: 2,
            context_id: None,
            task_id: None,
            framed: "--- A2A INCOMING MESSAGE ---\nhello".to_string(),
            queued_at: crate::core::a2a::inbox::now_unix(),
        };
        assert_eq!(
            a2a_echo_block(std::slice::from_ref(&m)),
            "A2A from peer (hop 2): hello"
        );
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
                // A wrong event or extra arguments are usage errors. These
                // are NON-blocking (exit 1, never exit 2): a misconfigured
                // or mis-invoked hook must not block a prompt or loop a
                // turn on Claude Code hooks.
                assert_eq!(run_a2a_hook(&["Nope".to_string()]), 1);
                assert_eq!(run_a2a_hook(&["Stop".to_string(), "x".to_string()]), 1);
                assert_eq!(run_a2a_hook(&[]), 1);
            },
        );
    }

    #[test]
    fn run_a2a_hook_marks_injected_through_the_events_channel() {
        // The mid-turn hooks (PostToolUse, UserPromptSubmit, agy's
        // pre-invocation, opencode's transform) inject through the `hook`
        // channel; the turn-boundary hooks that re-enter the loop (Claude's
        // Stop, agy's post-invocation/stop) inject through the `idle_turn`
        // channel.
        assert_eq!(event_channel("PostToolUse"), "hook");
        assert_eq!(event_channel("UserPromptSubmit"), "hook");
        assert_eq!(event_channel("pre-invocation"), "hook");
        assert_eq!(event_channel("transform"), "hook");
        assert_eq!(event_channel("Stop"), "idle_turn");
        assert_eq!(event_channel("post-invocation"), "idle_turn");
        assert_eq!(event_channel("stop"), "idle_turn");
        assert!(!is_agy_event("Stop"));
        assert!(!is_agy_event("PostToolUse"));
        for event in ["pre-invocation", "post-invocation", "stop", "transform"] {
            assert!(is_agy_event(event), "{event}");
        }
        assert!(is_opencode_transform("transform"));
        assert!(!is_opencode_transform("pre-invocation"));
    }

    #[test]
    fn run_a2a_hook_transform_prints_plain_framed_text() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        inbox.enqueue(msg("t1", "hello transform")).unwrap();

        // The transform event prints the joined framed text (not a JSON hook
        // object) and marks the messages injected through the `hook` channel.
        let code = crate::core::a2a::testenv::with_env(
            &[(
                crate::core::a2a::inbox::INBOX_ENV,
                Some(tmp.path().to_str().unwrap()),
            )],
            || run_a2a_hook(&["transform".to_string()]),
        );
        assert_eq!(code, 0);
        assert!(inbox.injected("t1"));
        assert!(inbox.pending().is_empty());

        // On an empty inbox the transform event prints `{}` (the plugin
        // expects a parseable token), like the agy events.
        let code = crate::core::a2a::testenv::with_env(
            &[(
                crate::core::a2a::inbox::INBOX_ENV,
                Some(tmp.path().to_str().unwrap()),
            )],
            || run_a2a_hook(&["transform".to_string()]),
        );
        assert_eq!(code, 0);
    }

    #[test]
    fn transform_stdout_payload_is_plain_text() {
        // Unlike the Claude/agy events (hook JSON), the opencode transform
        // event delegates to the plain framed text with no echo wrapper.
        let m = msg("m1", "the message");
        let payload = hook_stdout_payload("transform", "FRAMED", std::slice::from_ref(&m))
            .expect("transform payload");
        assert_eq!(payload, "FRAMED");
        // Non-transform events still produce the JSON hook output.
        let payload = hook_stdout_payload("PostToolUse", "FRAMED", std::slice::from_ref(&m))
            .expect("PostToolUse payload");
        assert!(
            payload.contains("\"hookSpecificOutput\""),
            "PostToolUse keeps the JSON hook format: {payload}"
        );
    }

    /// An UNREACHABLE pod-local inbox must make EVERY hook event fail closed
    /// and NON-blocking: exit `1` (never `2`, which on Claude Code hooks is
    /// a blocking error that would block a prompt or loop a turn), for the
    /// Claude Code AND the agy events alike. (The stuck-not-unreachable case
    /// is covered at the receive layer; the hook code path is the same
    /// `claim_capped` error.)
    #[test]
    fn run_a2a_hook_fails_closed_nonblocking_on_an_unreachable_remote_inbox() {
        use crate::core::a2a::receive::{INBOX_TOKEN_ENV, INBOX_URL_ENV};
        use crate::core::a2a::testenv;

        // An address that is NOT listening (the listener is dropped): the
        // pod-local inbox API is unreachable, so the connect is refused fast.
        let addr = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            addr
        };

        let url = format!("http://{addr}");
        let pairs: &[(&str, Option<&str>)] = &[
            (
                crate::core::a2a::inbox::INBOX_ENV,
                Some("/x/a2a-inbox/run-unreachable"),
            ),
            (INBOX_TOKEN_ENV, Some("t0k3n")),
            (INBOX_URL_ENV, Some(&url)),
        ];
        for event in [
            "PostToolUse",
            "UserPromptSubmit",
            "Stop",
            "pre-invocation",
            "post-invocation",
            "stop",
            "transform",
        ] {
            // Each event fails closed, non-blocking (exit 1).
            let code = testenv::with_env(pairs, || run_a2a_hook(&[event.to_string()]));
            assert_eq!(
                code, 1,
                "{event}: an unreachable inbox fails the hook closed, non-blocking"
            );
        }
    }
}

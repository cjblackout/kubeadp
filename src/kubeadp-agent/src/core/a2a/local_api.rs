//! The wire protocol of the per-agent A2A delivery socket (design HL-125
//! stream 3 C4b, SPEC §6.9 / §7.14): a JSON-over-UnixStream request/response
//! API through which the SANDBOXED receive side (`a2a-hook`, `mcp-a2a`,
//! separate processes with NO database grant) reaches its run's inbox, just
//! as the pod-local HTTP API does today but over a per-agent Unix socket that
//! is reachable in EVERY network mode (`host` and `relay` alike -- a pathname
//! socket is not namespaced by a loopback-only netns, so the relay of §6.9
//! has no effect on it). The host process owns the shared database and serves
//! this socket; the socket addresses ONE agent's inbox and nothing else.
//!
//! # Framing
//!
//! Each message is a single line: one JSON object serialized compactly
//! (`serde_json::to_string` never emits a raw newline inside a string --
//! control characters are JSON-escaped), terminated by `\n`. A reader never
//! has to disambiguate framing from content. A request line is bounded
//! ([`MAX_REQUEST_BYTES`]); a client that exceeds it or sends bytes that are
//! not valid UTF-8/JSON fails closed with a clear error response.
//!
//! # Value-level protocol (mirrors the pod-local HTTP API of §7.14)
//!
//! Request (one `method`, plus the optional arguments that method uses):
//!
//! ```text
//! {"method":"peek"}
//! {"method":"claim","max_bytes":<optional>}
//! {"method":"drain","channel":"<name>","max_bytes":<optional>}
//! {"method":"ack","message_id":"<id>","channel":"<name>"}
//! ```
//!
//! Response:
//!
//! ```text
//! {"ok":true,"messages":[...],"count":<n>}        // peek / claim / drain
//! {"ok":true,"marked":<bool>}                     // ack
//! {"ok":false,"error":"<reason>"}                 // unknown method / bad args
//! ```
//!
//! - `peek` -- reports the pending messages a `claim` would return WITHOUT
//!   claiming or acknowledging (the sandboxed `wait_message` polls it).
//! - `claim` -- claims (LEASES, [`CLAIM_LEASE_SECS`]) the oldest pending
//!   messages up to the byte budget; the caller must `ack` each one it
//!   actually delivers.
//! - `drain` -- claim + `ack` (through `channel`) in one round trip: the
//!   messages returned are exactly the ones this call acknowledged (the
//!   injection is the acknowledgement, §7.14).
//! - `ack` -- the idempotent acknowledgement (`Ok(false)` when the message
//!   was already injected or is unknown -- a no-op).
//!
//! The byte budget counts the framed text plus one separator (`\n\n`,
//! 2 bytes) between messages, matching [`crate::core::a2a::inbox::Inbox`]'s
//! own budget accounting, so a `drain` never marks a message it could not
//! also return.
//!
//! # Isolation
//!
//! There is no bearer token and no run selection here: the socket lives in a
//! 0700 owner-checked per-agent directory ([`crate::core::a2a::delivery_sock`]),
//! only the addressing agent can reach it (its sandbox grants exactly its own
//! socket path, never a sibling's, R12), and the server is bound to ONE
//! inbox chosen by the launcher at bind time. The content of a message is
//! still untrusted DATA (framed by the ingress) and the client's request
//! values are bounded, but no client-selected path ever addresses a database:
//! the store is fixed at bind.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use serde::{Deserialize, Serialize};

use super::http::ClaimedMessage;
use super::inbox::{Inbox, QueuedMessage};

/// Upper bound of one request line, well above the queue byte bound
/// ([`crate::core::a2a::inbox::MAX_QUEUED_BYTES`]) so a bounded queue can
/// never produce a request that does not fit, and small enough that a broken
/// client cannot make the server buffer unbounded data.
pub const MAX_REQUEST_BYTES: usize = 16 * 1024;

/// The method names of the delivery socket protocol (§7.14).
pub const METH_PEEK: &str = "peek";
pub const METH_CLAIM: &str = "claim";
pub const METH_DRAIN: &str = "drain";
pub const METH_ACK: &str = "ack";

/// One request on the delivery socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalRequest {
    /// The method: `peek`, `claim`, `drain` or `ack`.
    pub method: String,
    /// Optional byte budget (like `drain_capped`); counts the framed text
    /// plus one separator between messages. Omitted = drain everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    /// The injection channel (`drain` and `ack`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// The message id to acknowledge (`ack`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

impl LocalRequest {
    /// A build-with-defaults helper for the receive side and tests.
    pub fn new(method: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            max_bytes: None,
            channel: None,
            message_id: None,
        }
    }
}

/// The response to one delivery-socket request. The `ok`/`error` shape makes
/// the receive side fail closed on a bad answer exactly like the HTTP API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<ClaimedMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marked: Option<bool>,
}

impl LocalResponse {
    fn ok_messages(messages: Vec<ClaimedMessage>) -> Self {
        let count = messages.len();
        Self {
            ok: true,
            error: None,
            messages: Some(messages),
            count: Some(count),
            marked: None,
        }
    }

    fn ok_marked(marked: bool) -> Self {
        Self {
            ok: true,
            error: None,
            messages: None,
            count: None,
            marked: Some(marked),
        }
    }

    /// A bad request (unknown method, missing/invalid arguments): a clear
    /// fail-closed error, never a guessed answer.
    fn error(reason: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(reason.into()),
            messages: None,
            count: None,
            marked: None,
        }
    }
}

/// Maps a stored message to its wire identity (the senders/acks see the same
/// fields the pod-local HTTP API returns).
fn claimed(msg: &QueuedMessage) -> ClaimedMessage {
    ClaimedMessage {
        message_id: msg.message_id.clone(),
        sender: msg.sender.clone(),
        framed: msg.framed.clone(),
        hop: msg.hop,
        context_id: msg.context_id.clone(),
        task_id: msg.task_id.clone(),
        queued_at: msg.queued_at,
    }
}

/// The pending messages a `claim` would return (oldest first), up to the byte
/// budget, WITHOUT claiming or acknowledging anything. Read-only.
fn peek_budgeted(inbox: &Inbox, max_bytes: usize) -> Vec<ClaimedMessage> {
    let mut out = Vec::new();
    let mut budget = max_bytes;
    for msg in inbox.pending() {
        let sep = if out.is_empty() { 0 } else { 2 };
        if msg.framed.len() + sep > budget {
            break;
        }
        budget -= msg.framed.len() + sep;
        out.push(claimed(&msg));
    }
    out
}

/// Dispatches one delivery-socket request against the server's inbox.
///
/// The store operations are the same public [`Inbox`] calls the host's own
/// delivery uses, so the claim/ack semantics (`claimed_at` lease, exactly
/// once by message id across all channels) hold identically for the socket
/// and the HTTP API. An unknown method or a missing/invalid argument fails
/// closed with a clear error instead of guessing.
pub fn dispatch(inbox: &Inbox, req: &LocalRequest) -> LocalResponse {
    match req.method.as_str() {
        METH_PEEK => {
            let budget = req.max_bytes.unwrap_or(usize::MAX);
            LocalResponse::ok_messages(peek_budgeted(inbox, budget))
        }
        METH_CLAIM => {
            let budget = req.max_bytes.unwrap_or(usize::MAX);
            let messages = inbox
                .claim_capped(budget)
                .iter()
                .map(claimed)
                .collect::<Vec<_>>();
            LocalResponse::ok_messages(messages)
        }
        METH_DRAIN => {
            let Some(channel) = req.channel.as_deref() else {
                return LocalResponse::error("drain requires a `channel` argument");
            };
            if channel.is_empty() {
                return LocalResponse::error("drain requires a non-empty `channel` argument");
            }
            let budget = req.max_bytes.unwrap_or(usize::MAX);
            let messages = inbox
                .drain_capped(channel, budget)
                .iter()
                .map(claimed)
                .collect::<Vec<_>>();
            LocalResponse::ok_messages(messages)
        }
        METH_ACK => {
            let (Some(message_id), Some(channel)) =
                (req.message_id.as_deref(), req.channel.as_deref())
            else {
                return LocalResponse::error("ack requires `message_id` and `channel` arguments");
            };
            if message_id.is_empty() || channel.is_empty() {
                return LocalResponse::error(
                    "ack requires non-empty `message_id` and `channel` arguments",
                );
            }
            LocalResponse::ok_marked(inbox.mark_injected(message_id, channel))
        }
        other => LocalResponse::error(format!(
            "unknown delivery socket method '{other}' (accepted: {METH_PEEK}, {METH_CLAIM}, \
             {METH_DRAIN}, {METH_ACK})"
        )),
    }
}

/// Reads one request line from the stream, bounded by [`MAX_REQUEST_BYTES`].
///
/// `Ok(None)` on a clean EOF before any byte (the peer closed an idle
/// connection). A framing violation (EOF mid-line, an over-long line, invalid
/// UTF-8 or invalid JSON) is an `Err` with the reason, which the caller
/// answers with a fail-closed error response.
fn read_request_line(stream: &mut UnixStream) -> Result<Option<LocalRequest>, String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .map_err(|e| format!("delivery socket read failed: {e}"))?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err("delivery socket: unexpected EOF inside a request".to_string());
        }
        if byte[0] == b'\n' {
            break;
        }
        buf.push(byte[0]);
        if buf.len() > MAX_REQUEST_BYTES {
            return Err(format!(
                "delivery socket: request exceeds {MAX_REQUEST_BYTES} bytes"
            ));
        }
    }
    // Strip a trailing CR ('\r\n' line endings are tolerated).
    while buf.last() == Some(&b'\r') {
        buf.pop();
    }
    let text = std::str::from_utf8(&buf)
        .map_err(|_| "delivery socket: request is not valid UTF-8".to_string())?;
    let req = serde_json::from_str::<LocalRequest>(text)
        .map_err(|e| format!("delivery socket: invalid request JSON: {e}"))?;
    Ok(Some(req))
}

/// Serves ONE request on `stream` and answers, then returns. The server's
/// inbox is the fixed store the socket was bound to.
///
/// A client that disconnects before writing anything (clean EOF) is ignored.
/// A client that sends a malformed request (over-long line, non-UTF-8,
/// invalid JSON, unknown method) FAILS CLOSED with a JSON error response, so
/// a broken or hostile sandboxed process can never make the server silently
/// guess or buffer unbounded data. The connection is not kept open across
/// requests: the receive side makes one connection per call, so a stuck
/// client can never hold a server thread.
pub fn serve_connection(stream: &mut UnixStream, inbox: &Inbox) {
    let req = match read_request_line(stream) {
        Ok(Some(req)) => req,
        Ok(None) => return, // clean EOF: nothing to answer
        Err(e) => {
            let _ = write_response(stream, &LocalResponse::error(e));
            return;
        }
    };
    let resp = dispatch(inbox, &req);
    let _ = write_response(stream, &resp);
}

/// Serializes `resp` as one newline-terminated JSON line. Best effort on the
/// write: a failed write only means the answering peer is gone; the server
/// has already done its work (including any acknowledgement the request
/// asked for -- injection already happened, so a lost response is re-delivery
/// semantics, not lost state).
fn write_response(stream: &mut UnixStream, resp: &LocalResponse) -> std::io::Result<()> {
    let line = serde_json::to_string(resp).unwrap_or_else(|_| {
        serde_json::json!({ "ok": false, "error": "failed to serialize the delivery socket response" })
            .to_string()
    });
    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::inbox::{now_unix, Inbox};
    use tempfile::tempdir;

    /// An in-memory inbox at a fresh temp dir with one queued message.
    fn seeded_inbox(id: &str, text: &str) -> (tempfile::TempDir, Inbox, QueuedMessage) {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        let msg = QueuedMessage {
            message_id: id.to_string(),
            sender: "peer-agent".to_string(),
            hop: 2,
            context_id: Some("ctx-1".to_string()),
            task_id: Some("task-1".to_string()),
            framed: format!("--- A2A INCOMING MESSAGE ---\n{text}"),
            queued_at: now_unix(),
        };
        inbox.enqueue(msg.clone()).unwrap();
        (tmp, inbox, msg)
    }

    #[test]
    fn peek_reports_pending_without_claiming_or_acking() {
        let (_tmp, inbox, _) = seeded_inbox("m1", "hello");
        // A peek returns the message but never claims or acknowledges it: the
        // message stays pending and claimable afterwards.
        let resp = dispatch(&inbox, &LocalRequest::new(METH_PEEK));
        assert!(resp.ok, "peek answers ok: {resp:?}");
        assert_eq!(resp.count, Some(1));
        assert_eq!(resp.messages.as_ref().unwrap()[0].message_id, "m1");
        assert!(resp.messages.as_ref().unwrap()[0].framed.contains("hello"));
        // Still pending, still claimable, not injected.
        assert_eq!(inbox.pending_count(), 1);
        let claim = dispatch(&inbox, &LocalRequest::new(METH_CLAIM));
        assert_eq!(claim.messages.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn drain_claims_and_acks_in_one_round_trip() {
        let (_tmp, inbox, msg) = seeded_inbox("m1", "hello");
        let mut req = LocalRequest::new(METH_DRAIN);
        req.channel = Some("tool_result".to_string());
        let resp = dispatch(&inbox, &req);
        assert!(resp.ok, "drain answers ok: {resp:?}");
        assert_eq!(resp.count, Some(1));
        assert_eq!(resp.messages.as_ref().unwrap()[0].message_id, "m1");
        // The message was acknowledged through the channel: injected, no
        // longer pending.
        assert!(inbox.injected("m1"));
        assert_eq!(inbox.pending_count(), 0);
        let _ = msg;
    }

    #[test]
    fn claim_leases_without_ack_then_releases_after_a_lease() {
        // Claim leases but does NOT acknowledge: the message stops being
        // pending/claimable for the lease, and a second claim (same or
        // another claimer) cannot take it. The lease is clamped to at least
        // one second ([`Inbox`]'s `claim_one`), so an immediate re-claim takes
        // nothing -- that is the point: a crashed claimer cannot double-take.
        let (_tmp, inbox, _) = seeded_inbox("m1", "hello");
        let c1 = dispatch(&inbox, &LocalRequest::new(METH_CLAIM));
        assert_eq!(c1.messages.as_ref().unwrap().len(), 1);
        let c2 = dispatch(&inbox, &LocalRequest::new(METH_CLAIM));
        assert_eq!(
            c2.messages.as_ref().unwrap().len(),
            0,
            "leased, not claimable"
        );
        // The claimer that leased the message can still ack it (mark it
        // injected through a channel), which is what the delivery drain
        // performs after the claim.
        let mut ack = LocalRequest::new(METH_ACK);
        ack.message_id = Some("m1".to_string());
        ack.channel = Some("hook".to_string());
        assert_eq!(dispatch(&inbox, &ack).marked, Some(true));
        assert_eq!(inbox.pending_count(), 0, "acknowledged, no longer pending");
    }

    #[test]
    fn ack_is_idempotent_and_reports_when_it_won() {
        let (_tmp, inbox, _) = seeded_inbox("m1", "hello");
        let mut ack = LocalRequest::new(METH_ACK);
        ack.message_id = Some("m1".to_string());
        ack.channel = Some("hook".to_string());
        assert_eq!(dispatch(&inbox, &ack).marked, Some(true));
        // A second ack is a no-op (another channel won / already injected).
        assert_eq!(dispatch(&inbox, &ack).marked, Some(false));
        // An unknown message id is a no-op, never an error.
        let mut unknown = LocalRequest::new(METH_ACK);
        unknown.message_id = Some("nope".to_string());
        unknown.channel = Some("hook".to_string());
        assert_eq!(dispatch(&inbox, &unknown).marked, Some(false));
    }

    #[test]
    fn byte_budget_stops_before_a_message_that_does_not_fit() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        for (i, text) in ["aa", "bbbb"].iter().enumerate() {
            inbox
                .enqueue(QueuedMessage {
                    message_id: format!("m{i}"),
                    sender: "peer".to_string(),
                    hop: 1,
                    context_id: None,
                    task_id: None,
                    framed: (*text).to_string(),
                    queued_at: now_unix(),
                })
                .unwrap();
        }
        // Budget 2 fits only "aa" (the separator counts 0 before the first).
        let mut req = LocalRequest::new(METH_PEEK);
        req.max_bytes = Some(2);
        let resp = dispatch(&inbox, &req);
        let ids = resp
            .messages
            .as_ref()
            .unwrap()
            .iter()
            .map(|m| m.message_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["m0".to_string()]);
        // A claim with the same budget returns (leases) the same single one.
        let mut claim = LocalRequest::new(METH_CLAIM);
        claim.max_bytes = Some(2);
        let resp = dispatch(&inbox, &claim);
        assert_eq!(resp.messages.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn unknown_method_and_missing_arguments_fail_closed() {
        let (_tmp, inbox, _) = seeded_inbox("m1", "hello");
        let resp = dispatch(&inbox, &LocalRequest::new("bogus"));
        assert!(!resp.ok);
        assert!(resp
            .error
            .as_deref()
            .unwrap()
            .contains("unknown delivery socket method"));
        // drain without a channel is an error, never a guess.
        let resp = dispatch(&inbox, &LocalRequest::new(METH_DRAIN));
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("channel"));
        // ack without message_id / channel is an error.
        let resp = dispatch(&inbox, &LocalRequest::new(METH_ACK));
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("message_id"));
    }

    #[test]
    fn request_parsing_tolerates_crlf_and_rejects_garbage() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        let sock = tmp.path().join("t.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();

        // Read one response line from a stream.
        fn read_line(stream: &mut UnixStream) -> String {
            let mut byte = [0u8; 1];
            let mut buf = Vec::new();
            loop {
                let n = stream.read(&mut byte).unwrap();
                if n == 0 {
                    break;
                }
                if byte[0] == b'\n' {
                    break;
                }
                buf.push(byte[0]);
            }
            String::from_utf8(buf).unwrap()
        }

        // CRLF request line is parsed (the trailing CR is stripped).
        let mut client = std::os::unix::net::UnixStream::connect(&sock).unwrap();
        let mut server = listener.accept().unwrap().0;
        client.write_all(b"{\"method\":\"peek\"}\r\n").unwrap();
        serve_connection(&mut server, &inbox);
        let text = read_line(&mut client);
        let resp: LocalResponse = serde_json::from_str(&text).unwrap();
        assert!(resp.ok, "the CRLF request parses: {resp:?}");
        drop(client);

        // Invalid JSON fails closed with a JSON error response.
        let mut client = std::os::unix::net::UnixStream::connect(&sock).unwrap();
        let mut server = listener.accept().unwrap().0;
        client.write_all(b"{not-json}\n").unwrap();
        serve_connection(&mut server, &inbox);
        let text = read_line(&mut client);
        let resp: LocalResponse = serde_json::from_str(&text).unwrap();
        assert!(!resp.ok);
        assert!(resp
            .error
            .as_deref()
            .unwrap()
            .contains("invalid request JSON"));
        drop(client);
    }

    #[test]
    fn overlong_request_fails_closed() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        let sock = tmp.path().join("t.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        let mut client = std::os::unix::net::UnixStream::connect(&sock).unwrap();
        let mut server = listener.accept().unwrap().0;
        // A request line far beyond the bound.
        let big = format!("{{\"method\":\"{}\"\n", "x".repeat(MAX_REQUEST_BYTES * 2));
        client.write_all(big.as_bytes()).unwrap();
        serve_connection(&mut server, &inbox);
        let mut byte = [0u8; 1];
        let mut buf = Vec::new();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        loop {
            let n = client.read(&mut byte).unwrap();
            if n == 0 {
                break;
            }
            if byte[0] == b'\n' {
                break;
            }
            buf.push(byte[0]);
        }
        let resp: LocalResponse = serde_json::from_str(&String::from_utf8(buf).unwrap()).unwrap();
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("exceeds"));
    }
}

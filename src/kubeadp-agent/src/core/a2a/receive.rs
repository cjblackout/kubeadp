//! The SANDBOXED receive side of the A2A inbox (design HL-113 §7.14, phase
//! 3): the `a2a-hook` subcommand and the `mcp-a2a` `wait_message` tool run
//! as separate processes inside the run's sandbox, which has NO database
//! grant (the shared `state.sqlite` is owned by the host). They reach their
//! run's inbox ONLY through the pod-local HTTP API ([`crate::core::a2a::http`])
//! under the per-run token.
//!
//! The token is derived on the HOST from the jobset root key and handed to
//! the sandboxed process in the environment (the sandbox must never see the
//! root key itself):
//!
//! - `KUBEADP_A2A_INBOX_DIR` -- the run's inbox dir (the shared identity).
//! - `KUBEADP_A2A_INBOX_TOKEN` -- the per-run bearer token (set by the
//!   launcher, which computes it from the root key; absent = no inbox
//!   access).
//! - `KUBEADP_A2A_INBOX_URL` -- optional base URL of the pod-local inbox API
//!   (default `http://127.0.0.1:8002`, the same listener the host serves).
//!
//! Everything here keys on the run id (the sanitized last segment of the
//! inbox dir), matching the server side. The receive side exposes the same
//! value-level API as the direct [`Inbox`] (`drain_capped`, `pending_count`)
//! so the senders and tests are unchanged; when the token is unset it falls
//! back to the direct inbox (local, trusted invocation).

use std::path::Path;
use std::time::Duration;

use crate::core::a2a::http::ClaimedMessage;
use crate::core::a2a::inbox::{Inbox, QueuedMessage};

/// A claimed (leased) message from the HTTP API becomes the same value as a
/// queued message from the direct inbox: they carry identical fields.
impl From<ClaimedMessage> for QueuedMessage {
    fn from(m: ClaimedMessage) -> Self {
        QueuedMessage {
            message_id: m.message_id,
            sender: m.sender,
            hop: m.hop,
            context_id: m.context_id,
            task_id: m.task_id,
            framed: m.framed,
            queued_at: m.queued_at,
        }
    }
}

/// Env var for the per-run inbox bearer token (host-derived, sandbox-side).
pub const INBOX_TOKEN_ENV: &str = "KUBEADP_A2A_INBOX_TOKEN";
/// Env var for the pod-local inbox API base URL.
pub const INBOX_URL_ENV: &str = "KUBEADP_A2A_INBOX_URL";
/// Default pod-local base URL (the same `127.0.0.1:8002` listener the host
/// serves; the sandboxed process shares the pod network namespace).
pub const DEFAULT_INBOX_URL: &str = "http://127.0.0.1:8002";

/// The pod-local inbox API base URL for a host listen address (N4): the
/// loopback form of the SAME listener the host serves. A launcher sets
/// `KUBEADP_A2A_INBOX_URL` to this so a changed listen port cannot break
/// delivery silently. `None` when `listen` is not a `host:port` address.
pub fn inbox_url_for_listen(listen: &str) -> Option<String> {
    let port = listen.rsplit_once(':')?.1.parse::<u16>().ok()?;
    Some(format!("http://127.0.0.1:{port}"))
}

/// Some(reason) when the pod-local inbox API is UNREACHABLE from this
/// process (N4): sandbox net mode `relay` gives the agent its own
/// loopback-only network namespace and the relay proxy refuses loopback
/// destinations, so the host's TCP inbox API cannot be reached and hooks and
/// `wait_message` would silently see an empty inbox. Detected from the relay
/// proxy env the sandbox launcher injects; the receiver FAILS CLOSED with
/// this reason instead. The Unix-socket inbox is the planned target (do not
/// build it here).
pub fn inbox_unreachable_reason() -> Option<String> {
    for var in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        if let Ok(value) = std::env::var(var) {
            if value.trim_end_matches('/') == crate::sandbox::relay::RELAY_PROXY_URL {
                return Some(
                    "A2A inbox API unreachable in sandbox net mode 'relay': the pod-local API listens on the host loopback, which the relay does not forward (the Unix-socket inbox is the planned target)".to_string(),
                );
            }
        }
    }
    None
}

/// Per-call HTTP timeout, well above the queue byte accounting and far below
/// the MCP call timeout, so a dead host never hangs a hook or a wait.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval of a blocking receive over the pod-local HTTP API.
pub const REMOTE_POLL: Duration = Duration::from_millis(500);

/// The receive side of a run's A2A inbox: either the pod-local HTTP API (a
/// sandboxed run) or the direct database inbox (a trusted/local invocation,
/// tests).
#[derive(Debug, Clone)]
pub enum ReceiveInbox {
    Local(Inbox),
    Remote(RemoteInbox),
}

impl ReceiveInbox {
    /// Builds the receive side from the environment: when the per-run token
    /// is set the sandbox uses the pod-local HTTP API; otherwise it falls
    /// back to the direct inbox (an unset `KUBEADP_A2A_INBOX_DIR` yields
    /// `None`).
    pub fn from_env() -> Option<Self> {
        let dir = std::env::var(crate::core::a2a::inbox::INBOX_ENV).ok()?;
        match std::env::var(INBOX_TOKEN_ENV) {
            Ok(token) if !token.is_empty() => Some(Self::Remote(RemoteInbox::new(
                dir,
                token,
                std::env::var(INBOX_URL_ENV).unwrap_or_else(|_| DEFAULT_INBOX_URL.to_string()),
            ))),
            _ => Inbox::from_env().map(Self::Local),
        }
    }

    /// How often a blocking receive should poll this inbox. The direct inbox
    /// is a local read (`local`, tens of milliseconds); the remote one costs
    /// an HTTP round trip plus a database open on the host per poll, so it
    /// is polled at [`REMOTE_POLL`] (still far inside the 2 s wake-up
    /// contract of `wait_message`).
    pub fn poll_interval(&self, local: Duration) -> Duration {
        match self {
            Self::Local(_) => local,
            Self::Remote(_) => REMOTE_POLL.max(local),
        }
    }

    /// The number of pending (queued, uninjected) messages.
    pub fn pending_count(&self) -> usize {
        match self {
            Self::Local(inbox) => inbox.pending_count(),
            Self::Remote(remote) => remote.pending_count(),
        }
    }

    /// Drains (claims, i.e. marks injected through `channel`) the oldest
    /// pending messages up to the byte budget. Exactly once: a message
    /// already injected by another channel is not returned.
    pub fn drain_capped(&self, channel: &str, max_bytes: usize) -> Vec<QueuedMessage> {
        match self {
            Self::Local(inbox) => inbox.drain_capped(channel, max_bytes),
            Self::Remote(remote) => remote.drain_capped(channel, max_bytes),
        }
    }

    /// Claims (leases) the oldest pending messages up to the byte budget,
    /// WITHOUT acknowledging them (N10, at-least-once receive): the caller
    /// hands the text to a channel first and acknowledges afterwards with
    /// [`ReceiveInbox::ack`]. A claimed-but-unacked message stops being
    /// claimable for the lease, then returns to the queue, so a crash between
    /// claim and ack re-delivers the message (a duplicate print is harmless).
    pub fn claim_capped(&self, max_bytes: usize) -> Vec<QueuedMessage> {
        match self {
            Self::Local(inbox) => inbox.claim_capped(max_bytes),
            Self::Remote(remote) => remote
                .claim(max_bytes)
                .into_iter()
                .map(QueuedMessage::from)
                .collect(),
        }
    }

    /// Acknowledges (marks injected through `channel`) one claimed message
    /// after the text reached the channel. Idempotent; the ack is deduped by
    /// message id.
    pub fn ack(&self, channel: &str, message_id: &str) -> bool {
        match self {
            Self::Local(inbox) => inbox.mark_injected(message_id, channel),
            Self::Remote(remote) => remote.mark_injected(channel, message_id),
        }
    }

    /// Hands a claim back after the text could NOT reach the channel. Local:
    /// the lease is released so another channel can claim at once. Remote:
    /// a no-op (the claim lease lapses and the message returns to the queue).
    pub fn release(&self, message_id: &str) {
        if let Self::Local(inbox) = self {
            inbox.release_claim(message_id);
        }
    }
}

/// A remote (pod-local HTTP) reference to one run's inbox.
#[derive(Debug, Clone)]
pub struct RemoteInbox {
    inbox: String,
    token: String,
    url: String,
}

impl RemoteInbox {
    pub fn new(inbox: String, token: String, url: String) -> Self {
        Self { inbox, token, url }
    }

    fn base(&self) -> String {
        self.url.trim_end_matches('/').to_string()
    }

    /// The number of pending (queued, uninjected, unclaimed) messages,
    /// without consuming anything (a `peek`).
    pub fn pending_count(&self) -> usize {
        let body = serde_json::json!({ "inbox": self.inbox }).to_string();
        let Some(text) = self.post("/a2a-inbox/peek", &body) else {
            return 0;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            return 0;
        };
        value.get("count").and_then(|v| v.as_u64()).unwrap_or(0) as usize
    }

    /// Claims (leases) the oldest pending messages up to the byte budget and
    /// acknowledges (marks injected through `channel`) every one delivered.
    /// The two-phase protocol is transparent to the caller: the messages
    /// returned are exactly the ones this call acknowledged.
    pub fn drain_capped(&self, channel: &str, max_bytes: usize) -> Vec<QueuedMessage> {
        let claimed = self.claim(max_bytes);
        // Acknowledge each claimed message we are about to return (the
        // injection is the acknowledgement); an already-injected row is a
        // no-op (another channel won), so never returned.
        claimed
            .into_iter()
            .filter(|m| self.mark_injected(channel, &m.message_id))
            .map(QueuedMessage::from)
            .collect()
    }

    /// Claims (leases) the oldest pending messages up to the byte budget.
    fn claim(&self, max_bytes: usize) -> Vec<ClaimedMessage> {
        let body = serde_json::json!({
            "inbox": self.inbox,
            "max_bytes": max_bytes,
        })
        .to_string();
        let Some(text) = self.post("/a2a-inbox/claim", &body) else {
            return Vec::new();
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            return Vec::new();
        };
        if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            return Vec::new();
        }
        let Some(messages) = value.get("messages").and_then(|v| v.as_array()) else {
            return Vec::new();
        };
        messages
            .iter()
            .filter_map(|m| serde_json::from_value::<ClaimedMessage>(m.clone()).ok())
            .collect()
    }

    /// Acknowledges (marks injected through `channel`) one message. Idempotent.
    fn mark_injected(&self, channel: &str, message_id: &str) -> bool {
        let body = serde_json::json!({
            "inbox": self.inbox,
            "message_id": message_id,
            "channel": channel,
        })
        .to_string();
        let Some(text) = self.post("/a2a-inbox/mark-injected", &body) else {
            return false;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            return false;
        };
        value.get("ok").and_then(|v| v.as_bool()) == Some(true)
            && value.get("marked").and_then(|v| v.as_bool()) == Some(true)
    }

    /// POSTs `body` to a relative endpoint with the bearer token, returning
    /// the response body text on success (HTTP 200). Nil on any error.
    ///
    /// Runs on a fresh OS thread: the `reqwest` blocking client creates (and
    /// later drops) its OWN tokio runtime, which is forbidden inside an
    /// already-running async context (the `mcp-a2a` tool handlers and the
    /// hook path all live inside a tokio runtime). The thread is as cheap as
    /// the nested-runtime construction it replaces and keeps the call
    /// strictly synchronous for the caller.
    fn post(&self, path: &str, body: &str) -> Option<String> {
        let url = format!("{}{}", self.base(), path);
        let token = self.token.clone();
        let body = body.to_string();
        std::thread::spawn(move || {
            let client = reqwest::blocking::Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .ok()?;
            let resp = client
                .post(url)
                .header("content-type", "application/json")
                .bearer_auth(&token)
                .body(body)
                .send()
                .ok()?;
            if !resp.status().is_success() {
                return None;
            }
            resp.text().ok()
        })
        .join()
        .ok()
        .flatten()
    }
}

/// The run id of a sandboxed inbox dir (the shared identity), mirroring the
/// server side.
pub fn run_id_of(inbox_dir: &Path) -> String {
    crate::core::a2a::http::run_id_of(inbox_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_prefers_the_remote_inbox_when_the_token_is_set() {
        // No token: falls back to the direct inbox (trusted/local invocation).
        crate::core::a2a::testenv::with_env(
            &[(
                crate::core::a2a::inbox::INBOX_ENV,
                Some("/x/a2a-inbox/run-1"),
            )],
            || match ReceiveInbox::from_env() {
                Some(ReceiveInbox::Local(inbox)) => {
                    assert_eq!(inbox.dir().to_string_lossy(), "/x/a2a-inbox/run-1");
                }
                _ => panic!("expected the local inbox fallback"),
            },
        );
        // With a token: the remote inbox (default URL, token, inbox path).
        crate::core::a2a::testenv::with_env(
            &[
                (
                    crate::core::a2a::inbox::INBOX_ENV,
                    Some("/x/a2a-inbox/run-1"),
                ),
                (INBOX_TOKEN_ENV, Some("t0k3n")),
            ],
            || match ReceiveInbox::from_env() {
                Some(ReceiveInbox::Remote(remote)) => {
                    assert_eq!(remote.url, DEFAULT_INBOX_URL);
                    assert!(remote.inbox.ends_with("run-1"));
                    assert_eq!(remote.token, "t0k3n");
                }
                _ => panic!("expected the remote inbox"),
            },
        );
    }

    #[test]
    fn inbox_url_follows_the_real_listen_address() {
        // N4: the launcher derives the inbox API base from the SAME listener
        // the host serves (loopback form); a changed listen port is picked
        // up, a non-address yields None.
        assert_eq!(
            inbox_url_for_listen("0.0.0.0:8002"),
            Some("http://127.0.0.1:8002".to_string())
        );
        assert_eq!(
            inbox_url_for_listen("127.0.0.1:9100"),
            Some("http://127.0.0.1:9100".to_string())
        );
        assert_eq!(inbox_url_for_listen("not-an-address"), None);
        assert_eq!(inbox_url_for_listen("host:noport"), None);
    }

    #[test]
    fn relay_sandbox_fails_the_inbox_closed() {
        // N4: with net: relay the agent's netns is loopback-only and the
        // relay proxy refuses loopback, so the TCP inbox API is unreachable.
        // The receiver announces it instead of silently seeing an empty
        // inbox; without the relay env nothing is announced.
        crate::core::a2a::testenv::with_env(&[], || {
            assert!(inbox_unreachable_reason().is_none());
        });
        crate::core::a2a::testenv::with_env(
            &[("HTTP_PROXY", Some(crate::sandbox::relay::RELAY_PROXY_URL))],
            || {
                let reason = inbox_unreachable_reason().expect("announced");
                assert!(reason.contains("relay"), "{reason}");
                // The hook subcommand fails closed with the same reason.
                let code = crate::core::a2a::inject::run_a2a_hook(&["PostToolUse".to_string()]);
                assert_eq!(code, 2, "the hook fails closed");
            },
        );
        // A non-relay proxy value is not announced.
        crate::core::a2a::testenv::with_env(
            &[("HTTP_PROXY", Some("http://proxy.example:8888"))],
            || {
                assert!(inbox_unreachable_reason().is_none());
            },
        );
    }
}

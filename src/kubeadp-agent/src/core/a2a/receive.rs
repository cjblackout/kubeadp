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
/// Env var naming the in-sandbox path of the agent's delivery socket (§6.9):
/// when the launcher set it (C5 wiring, all network modes) the receive side
/// prefers the socket over the pod-local HTTP API. Named via
/// [`crate::core::a2a::delivery_sock::SOCKET_ENV`].
pub const INBOX_SOCKET_ENV: &str = crate::core::a2a::delivery_sock::SOCKET_ENV;

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
/// destinations, so the host's TCP inbox API cannot be reached. This is NOT
/// a reason to fail closed when the receive side picks the per-agent delivery
/// socket (§6.9): a pathname unix socket is not namespaced by a loopback-only
/// netns, so with `KUBEADP_A2A_LOCAL_SOCKET` set the socket takes over and
/// the API is never consulted. The receiver FAILS CLOSED with the HTTP
/// reason only when it would actually use the HTTP API (no socket configured).
pub fn inbox_unreachable_reason() -> Option<String> {
    // With the delivery socket configured (C5 wiring) the HTTP API is never
    // used, so its unreachability in relay mode is irrelevant.
    if std::env::var(INBOX_SOCKET_ENV).is_ok() {
        return None;
    }
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
                    "A2A inbox API unreachable in sandbox net mode 'relay': the pod-local API listens on the host loopback, which the relay does not forward (the per-agent delivery socket is the preferred target)".to_string(),
                );
            }
        }
    }
    None
}

/// Connect (TCP) phase cap of the pod-local inbox HTTP client: an unreachable
/// host must fail the connect inside this, never hang a hook or a wait.
pub const INBOX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-request (connect + write + response body) deadline of the pod-local
/// inbox HTTP client: a stuck local inbox (a listener that accepts but never
/// answers) must FAIL CLOSED inside this with a clear error, never silently
/// read as an empty inbox. Far below the MCP call timeout so the deliverers
/// always get their answer in time.
pub const INBOX_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-run receive failure carrying the endpoint and the deadline, so a hook
/// or a wait can fail closed with a clear reason instead of silently treating
/// an unreachable inbox as empty.
#[derive(Debug, Clone)]
pub struct InboxApiError {
    /// The pod-local endpoint that failed (`<base>/<path>`).
    pub endpoint: String,
    /// The underlying request error (timeout, connect, transport).
    pub source: String,
    /// The per-request deadline the call ran under.
    pub deadline: Duration,
}

impl std::fmt::Display for InboxApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "inbox API request to {} failed after {:.1}s: {}",
            self.endpoint,
            self.deadline.as_secs_f64(),
            self.source
        )
    }
}

impl std::error::Error for InboxApiError {}

/// Poll interval of a blocking receive over the pod-local HTTP API.
pub const REMOTE_POLL: Duration = Duration::from_millis(500);

/// The receive side of a run's A2A inbox: either the per-agent delivery
/// socket (a sandboxed run with the socket configured, §6.9), the pod-local
/// HTTP API (a sandboxed run without the socket, legacy), or the direct
/// database inbox (a trusted/local invocation, tests).
#[derive(Debug, Clone)]
pub enum ReceiveInbox {
    /// The run's inbox over the per-agent delivery socket (the sandboxed
    /// receive side; reachable in every network mode).
    Socket(SocketInbox),
    Local(Inbox),
    Remote(RemoteInbox),
}

impl ReceiveInbox {
    /// Builds the receive side from the environment: when the per-agent
    /// delivery socket is configured (`KUBEADP_A2A_LOCAL_SOCKET`, set by the
    /// launcher in every net mode, §6.9) the sandbox uses it; otherwise,
    /// when the per-run HTTP token is set the pod-local HTTP API is used;
    /// otherwise it falls back to the direct inbox (an unset
    /// `KUBEADP_A2A_INBOX_DIR` yields `None`).
    pub fn from_env() -> Option<Self> {
        let dir = std::env::var(crate::core::a2a::inbox::INBOX_ENV).ok()?;
        // The delivery socket, when the launcher set its path (§6.9): the
        // preferred receive side in every network mode.
        if let Ok(socket_path) = std::env::var(INBOX_SOCKET_ENV) {
            if !socket_path.trim().is_empty() {
                return Some(Self::Socket(SocketInbox::new(
                    dir,
                    std::path::PathBuf::from(socket_path),
                )));
            }
        }
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
    /// is a local read (`local`, tens of milliseconds); the socket and the
    /// remote one cost a round trip plus a database open on the host per
    /// poll, so they are polled at [`REMOTE_POLL`] (still far inside the 2 s
    /// wake-up contract of `wait_message`).
    pub fn poll_interval(&self, local: Duration) -> Duration {
        match self {
            Self::Local(_) => local,
            Self::Socket(_) | Self::Remote(_) => REMOTE_POLL.max(local),
        }
    }

    /// The number of pending (queued, uninjected) messages. Fails closed with
    /// a clear [`InboxApiError`] when the delivery socket or the pod-local
    /// inbox API is unreachable or stuck (an unreachable inbox is never
    /// silently read as empty).
    pub fn pending_count(&self) -> Result<usize, InboxApiError> {
        match self {
            Self::Local(inbox) => Ok(inbox.pending_count()),
            Self::Socket(sock) => sock.pending_count(),
            Self::Remote(remote) => remote.pending_count(),
        }
    }

    /// Drains (claims, i.e. marks injected through `channel`) the oldest
    /// pending messages up to the byte budget. Exactly once: a message
    /// already injected by another channel is not returned. Fails closed with
    /// a clear error when the delivery socket or the pod-local inbox API is
    /// unreachable or stuck.
    pub fn drain_capped(
        &self,
        channel: &str,
        max_bytes: usize,
    ) -> Result<Vec<QueuedMessage>, InboxApiError> {
        match self {
            Self::Local(inbox) => Ok(inbox.drain_capped(channel, max_bytes)),
            Self::Socket(sock) => sock.drain_capped(channel, max_bytes),
            Self::Remote(remote) => remote.drain_capped(channel, max_bytes),
        }
    }

    /// Claims (leases) the oldest pending messages up to the byte budget,
    /// WITHOUT acknowledging them (N10, at-least-once receive): the caller
    /// hands the text to a channel first and acknowledges afterwards with
    /// [`ReceiveInbox::ack`]. A claimed-but-unacked message stops being
    /// claimable for the lease, then returns to the queue, so a crash between
    /// claim and ack re-delivers the message (a duplicate print is harmless).
    /// Fails closed with a clear error when the delivery socket or the
    /// pod-local inbox API is unreachable or stuck.
    pub fn claim_capped(&self, max_bytes: usize) -> Result<Vec<QueuedMessage>, InboxApiError> {
        match self {
            Self::Local(inbox) => Ok(inbox.claim_capped(max_bytes)),
            Self::Socket(sock) => sock
                .claim(max_bytes)
                .map(|claimed| claimed.into_iter().map(QueuedMessage::from).collect()),
            Self::Remote(remote) => remote
                .claim(max_bytes)
                .map(|claimed| claimed.into_iter().map(QueuedMessage::from).collect()),
        }
    }

    /// Acknowledges (marks injected through `channel`) one claimed message
    /// after the text reached the channel. Idempotent; the ack is deduped by
    /// message id. Fails closed with a clear error when the delivery socket
    /// or the pod-local inbox API is unreachable or stuck.
    pub fn ack(&self, channel: &str, message_id: &str) -> Result<bool, InboxApiError> {
        match self {
            Self::Local(inbox) => Ok(inbox.mark_injected(message_id, channel)),
            Self::Socket(sock) => sock.mark_injected(channel, message_id),
            Self::Remote(remote) => remote.mark_injected(channel, message_id),
        }
    }

    /// Hands a claim back after the text could NOT reach the channel. Local:
    /// the lease is released so another channel can claim at once. Socket /
    /// remote: a no-op (the claim lease lapses and the message returns to the
    /// queue).
    pub fn release(&self, message_id: &str) {
        if let Self::Local(inbox) = self {
            inbox.release_claim(message_id);
        }
    }
}

/// A receive side over the per-agent delivery socket ([`crate::core::a2a::delivery_sock`]):
/// a JSON-over-UnixStream reference to the run's inbox, bound by the host
/// before fork and reachable in every network mode (§6.9). The socket path
/// comes from `KUBEADP_A2A_LOCAL_SOCKET` (set by the launcher only); the run
/// key is the inbox dir, exactly like the HTTP API.
#[derive(Debug, Clone)]
pub struct SocketInbox {
    /// The run's inbox dir (`KUBEADP_A2A_INBOX_DIR`), the shared identity.
    inbox: String,
    /// The in-sandbox path of the delivery socket.
    socket: std::path::PathBuf,
}

/// The per-request deadline of a delivery-socket call: the socket is a local
/// service of the host process, so a hung listener must fail closed inside
/// this, never stall the receive side.
pub const SOCKET_TIMEOUT: Duration = Duration::from_secs(10);

impl SocketInbox {
    pub fn new(inbox: String, socket: std::path::PathBuf) -> Self {
        Self { inbox, socket }
    }

    /// The addressed run id (the sanitized inbox-dir identity), mirroring the
    /// server side.
    pub fn run_id(&self) -> String {
        crate::core::a2a::receive::run_id_of(std::path::Path::new(&self.inbox))
    }

    /// The socket path the receive side connects to.
    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket
    }

    fn api_error(&self, source: String) -> InboxApiError {
        InboxApiError {
            endpoint: self.socket.display().to_string(),
            source,
            deadline: SOCKET_TIMEOUT,
        }
    }

    /// The number of pending messages (a `peek`), without consuming anything.
    /// Fails closed with a clear error when the socket is unreachable or
    /// stuck.
    pub fn pending_count(&self) -> Result<usize, InboxApiError> {
        let resp = self.request(&crate::core::a2a::local_api::LocalRequest::new(
            crate::core::a2a::local_api::METH_PEEK,
        ))?;
        if !resp.ok {
            return Err(self.api_error(format!("host refused the peek: {:?}", resp.error)));
        }
        Ok(resp.count.unwrap_or(0))
    }

    /// Claims (leases) the oldest pending messages up to the byte budget and
    /// acknowledges (marks injected through `channel`) every one delivered.
    /// The two-phase protocol is transparent to the caller: the messages
    /// returned are exactly the ones this call acknowledged. Fails closed
    /// with a clear error when the socket is unreachable or stuck.
    pub fn drain_capped(
        &self,
        channel: &str,
        max_bytes: usize,
    ) -> Result<Vec<QueuedMessage>, InboxApiError> {
        let claimed = self.claim(max_bytes)?;
        let mut out = Vec::new();
        for m in claimed {
            if self.mark_injected(channel, &m.message_id)? {
                out.push(QueuedMessage::from(m));
            }
        }
        Ok(out)
    }

    /// Claims (leases) the oldest pending messages up to the byte budget.
    /// Fails closed with a clear error when the socket is unreachable or
    /// stuck.
    fn claim(&self, max_bytes: usize) -> Result<Vec<ClaimedMessage>, InboxApiError> {
        let mut req =
            crate::core::a2a::local_api::LocalRequest::new(crate::core::a2a::local_api::METH_CLAIM);
        req.max_bytes = Some(max_bytes);
        let resp = self.request(&req)?;
        if !resp.ok {
            return Err(self.api_error(format!("host refused the claim: {:?}", resp.error)));
        }
        Ok(resp.messages.unwrap_or_default())
    }

    /// Acknowledges (marks injected through `channel`) one message. Idempotent.
    /// Fails closed with a clear error when the socket is unreachable or
    /// stuck; `Ok(false)` means the host answered but did not mark
    /// (already injected by another channel -- a no-op).
    fn mark_injected(&self, channel: &str, message_id: &str) -> Result<bool, InboxApiError> {
        let mut req =
            crate::core::a2a::local_api::LocalRequest::new(crate::core::a2a::local_api::METH_ACK);
        req.channel = Some(channel.to_string());
        req.message_id = Some(message_id.to_string());
        let resp = self.request(&req)?;
        if !resp.ok {
            return Err(self.api_error(format!("host refused the mark-injected: {:?}", resp.error)));
        }
        Ok(resp.marked == Some(true))
    }

    /// Sends one request to the delivery socket and reads one response,
    /// under [`SOCKET_TIMEOUT`]. Runs on a fresh OS thread so a hung
    /// listener cannot stall the async `mcp-a2a` tool handlers (the same
    /// pattern as the HTTP client). Fails closed with a clear error on any
    /// transport/protocol failure: an unreachable or stuck local socket is
    /// NEVER silently read as empty.
    fn request(
        &self,
        req: &crate::core::a2a::local_api::LocalRequest,
    ) -> Result<crate::core::a2a::local_api::LocalResponse, InboxApiError> {
        let socket = self.socket.clone();
        let req = req.clone();
        let deadline = SOCKET_TIMEOUT;
        std::thread::spawn(
            move || -> Result<crate::core::a2a::local_api::LocalResponse, String> {
                let mut stream = std::os::unix::net::UnixStream::connect(&socket)
                    .map_err(|e| format!("connect to {socket:?}: {e}"))?;
                stream
                    .set_read_timeout(Some(deadline))
                    .map_err(|e| format!("set read timeout: {e}"))?;
                stream
                    .set_write_timeout(Some(deadline))
                    .map_err(|e| format!("set write timeout: {e}"))?;
                let mut line =
                    serde_json::to_string(&req).map_err(|e| format!("serialize request: {e}"))?;
                line.push('\n');
                use std::io::Write;
                stream
                    .write_all(line.as_bytes())
                    .map_err(|e| format!("write request: {e}"))?;
                stream.flush().map_err(|e| format!("flush request: {e}"))?;
                use std::io::Read;
                let mut buf: Vec<u8> = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    let n = stream
                        .read(&mut byte)
                        .map_err(|e| format!("read response: {e}"))?;
                    if n == 0 {
                        return Err("unexpected EOF in the delivery socket response".to_string());
                    }
                    if byte[0] == b'\n' {
                        break;
                    }
                    buf.push(byte[0]);
                }
                let text = std::str::from_utf8(&buf)
                    .map_err(|_| "delivery socket response is not valid UTF-8".to_string())?;
                serde_json::from_str(text)
                    .map_err(|e| format!("invalid delivery socket response: {e}"))
            },
        )
        .join()
        .map_err(|_| InboxApiError {
            endpoint: self.socket.display().to_string(),
            source: "the delivery socket request thread panicked".to_string(),
            deadline,
        })?
        .map_err(|source| InboxApiError {
            endpoint: self.socket.display().to_string(),
            source,
            deadline,
        })
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
    /// without consuming anything (a `peek`). Fails closed with a clear error
    /// when the pod-local inbox API is unreachable or stuck.
    pub fn pending_count(&self) -> Result<usize, InboxApiError> {
        let body = serde_json::json!({ "inbox": self.inbox }).to_string();
        let text = self.post("/a2a-inbox/peek", &body)?;
        let value = serde_json::from_str::<serde_json::Value>(&text).map_err(|e| {
            self.api_error("/a2a-inbox/peek", format!("invalid peek response: {e}"))
        })?;
        if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            return Err(self.api_error(
                "/a2a-inbox/peek",
                format!("host refused the peek: {}", value),
            ));
        }
        Ok(value.get("count").and_then(|v| v.as_u64()).unwrap_or(0) as usize)
    }

    /// Claims (leases) the oldest pending messages up to the byte budget and
    /// acknowledges (marks injected through `channel`) every one delivered.
    /// The two-phase protocol is transparent to the caller: the messages
    /// returned are exactly the ones this call acknowledged. Fails closed
    /// with a clear error when the pod-local inbox API is unreachable or
    /// stuck.
    pub fn drain_capped(
        &self,
        channel: &str,
        max_bytes: usize,
    ) -> Result<Vec<QueuedMessage>, InboxApiError> {
        let claimed = self.claim(max_bytes)?;
        // Acknowledge each claimed message we are about to return (the
        // injection is the acknowledgement); an already-injected row is a
        // no-op (another channel won), so never returned.
        let mut out = Vec::new();
        for m in claimed {
            if self.mark_injected(channel, &m.message_id)? {
                out.push(QueuedMessage::from(m));
            }
        }
        Ok(out)
    }

    /// Claims (leases) the oldest pending messages up to the byte budget.
    /// Fails closed with a clear error when the pod-local inbox API is
    /// unreachable or stuck.
    fn claim(&self, max_bytes: usize) -> Result<Vec<ClaimedMessage>, InboxApiError> {
        let body = serde_json::json!({
            "inbox": self.inbox,
            "max_bytes": max_bytes,
        })
        .to_string();
        let text = self.post("/a2a-inbox/claim", &body)?;
        let value = serde_json::from_str::<serde_json::Value>(&text).map_err(|e| {
            self.api_error("/a2a-inbox/claim", format!("invalid claim response: {e}"))
        })?;
        if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            return Err(self.api_error(
                "/a2a-inbox/claim",
                format!("host refused the claim: {}", value),
            ));
        }
        let Some(messages) = value.get("messages").and_then(|v| v.as_array()) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for m in messages {
            out.push(
                serde_json::from_value::<ClaimedMessage>(m.clone()).map_err(|e| {
                    self.api_error("/a2a-inbox/claim", format!("invalid claimed message: {e}"))
                })?,
            );
        }
        Ok(out)
    }

    /// Acknowledges (marks injected through `channel`) one message. Idempotent.
    /// Fails closed with a clear error when the pod-local inbox API is
    /// unreachable or stuck; `Ok(false)` means the host answered but did not
    /// mark (already injected by another channel -- a no-op).
    fn mark_injected(&self, channel: &str, message_id: &str) -> Result<bool, InboxApiError> {
        let body = serde_json::json!({
            "inbox": self.inbox,
            "message_id": message_id,
            "channel": channel,
        })
        .to_string();
        let text = self.post("/a2a-inbox/mark-injected", &body)?;
        let value = serde_json::from_str::<serde_json::Value>(&text).map_err(|e| {
            self.api_error(
                "/a2a-inbox/mark-injected",
                format!("invalid mark-injected response: {e}"),
            )
        })?;
        if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            return Err(self.api_error(
                "/a2a-inbox/mark-injected",
                format!("host refused the mark-injected: {}", value),
            ));
        }
        Ok(value.get("marked").and_then(|v| v.as_bool()) == Some(true))
    }

    /// A clear fail-closed error for an endpoint, with the deadline this
    /// process runs its inbox calls under.
    fn api_error(&self, path: &str, source: String) -> InboxApiError {
        InboxApiError {
            endpoint: format!("{}{}", self.base(), path),
            source,
            deadline: INBOX_REQUEST_TIMEOUT,
        }
    }

    /// POSTs `body` to a relative endpoint with the bearer token, returning
    /// the response body text on success (HTTP 200). Fails closed with a
    /// clear error on any transport/HTTP failure (connect, timeout, non-2xx):
    /// an unreachable or stuck local inbox is NEVER silently read as empty.
    ///
    /// Runs on a fresh OS thread: the `reqwest` blocking client uses its OWN
    /// internal tokio runtime, which is forbidden to block_on inside an
    /// already-running async context (the `mcp-a2a` tool handlers and the
    /// hook path all live inside a tokio runtime). The blocking client itself
    /// (and its connection pool + background runtime thread) is shared across
    /// calls via a `OnceLock` so `wait_message` polling does not construct a
    /// new runtime and TCP connection on every 500 ms tick.
    fn post(&self, path: &str, body: &str) -> Result<String, InboxApiError> {
        static CLIENT: std::sync::OnceLock<Option<reqwest::blocking::Client>> =
            std::sync::OnceLock::new();
        let url = format!("{}{}", self.base(), path);
        // The URL the worker thread posts to: cloned so the join below can
        // still name `url` on a panic (the thread `move`s its own copy).
        let thread_url = url.clone();
        let token = self.token.clone();
        let body = body.to_string();
        // The deadline used on the shared client (named on this error).
        let deadline = INBOX_REQUEST_TIMEOUT;
        std::thread::spawn(move || {
            let client = CLIENT.get_or_init(|| {
                reqwest::blocking::Client::builder()
                    .connect_timeout(INBOX_CONNECT_TIMEOUT)
                    .timeout(INBOX_REQUEST_TIMEOUT)
                    .build()
                    .ok()
            });
            let client = client.as_ref().ok_or_else(|| InboxApiError {
                endpoint: thread_url.clone(),
                source: "failed to build the inbox HTTP client".to_string(),
                deadline,
            })?;
            let resp = client
                .post(&thread_url)
                .header("content-type", "application/json")
                .bearer_auth(&token)
                .body(body)
                .send()
                .map_err(|e| InboxApiError {
                    endpoint: thread_url.clone(),
                    source: format!("{e}"),
                    deadline,
                })?;
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().unwrap_or_default();
                return Err(InboxApiError {
                    endpoint: thread_url.clone(),
                    source: format!("HTTP {status}: {text}"),
                    deadline,
                });
            }
            resp.text().map_err(|e| InboxApiError {
                endpoint: thread_url,
                source: format!("{e}"),
                deadline,
            })
        })
        .join()
        .map_err(|_| InboxApiError {
            endpoint: url,
            source: "the inbox request thread panicked".to_string(),
            deadline,
        })?
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
    fn from_env_prefers_the_delivery_socket_when_configured() {
        // The per-agent delivery socket (§6.9) wins over BOTH the direct
        // inbox fallback and the HTTP API, whenever the launcher set
        // KUBEADP_A2A_LOCAL_SOCKET (every net mode). The socket carries the
        // run key and the socket path.
        crate::core::a2a::testenv::with_env(
            &[
                (
                    crate::core::a2a::inbox::INBOX_ENV,
                    Some("/x/a2a-inbox/run-sock"),
                ),
                (INBOX_TOKEN_ENV, Some("t0k3n")),
                (
                    INBOX_SOCKET_ENV,
                    Some("/tmp/kubeadp-a2a-agent/delivery.sock"),
                ),
            ],
            || match ReceiveInbox::from_env() {
                Some(ReceiveInbox::Socket(sock)) => {
                    assert_eq!(
                        sock.socket_path(),
                        std::path::Path::new("/tmp/kubeadp-a2a-agent/delivery.sock")
                    );
                    assert_eq!(sock.run_id(), "run-sock");
                    assert!(sock.inbox.ends_with("run-sock"));
                }
                other => panic!("expected the socket inbox, got {other:?}"),
            },
        );
        // An empty socket env is treated as absent (falls back to the HTTP
        // API when a token is set).
        crate::core::a2a::testenv::with_env(
            &[
                (
                    crate::core::a2a::inbox::INBOX_ENV,
                    Some("/x/a2a-inbox/run-1"),
                ),
                (INBOX_TOKEN_ENV, Some("t0k3n")),
                (INBOX_SOCKET_ENV, Some("")),
            ],
            || match ReceiveInbox::from_env() {
                Some(ReceiveInbox::Remote(_)) => {}
                other => panic!("empty socket env falls back to the remote inbox, got {other:?}"),
            },
        );
    }

    #[test]
    fn relay_unreachability_is_moot_when_the_socket_is_configured() {
        // N4 with the delivery socket (§6.9): in relay mode the agent's netns
        // is loopback-only, but the per-agent delivery socket (a pathname unix
        // socket) is NOT namespaced by a netns, so it is reachable and the
        // HTTP API's relay unreachability is irrelevant. The receiver must NOT
        // announce a reason when the socket is configured.
        crate::core::a2a::testenv::with_env(
            &[
                ("HTTP_PROXY", Some(crate::sandbox::relay::RELAY_PROXY_URL)),
                (
                    INBOX_SOCKET_ENV,
                    Some("/tmp/kubeadp-a2a-agent/delivery.sock"),
                ),
            ],
            || {
                assert!(
                    inbox_unreachable_reason().is_none(),
                    "the relay reason is moot when the delivery socket is configured"
                );
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
                // The hook subcommand fails closed with the same reason, and
                // it does so NON-blockingly (exit 1): a hub/inbox outage must
                // never block a prompt or loop a turn (exit 2 on Claude Code
                // hooks would be a blocking error).
                let code = crate::core::a2a::inject::run_a2a_hook(&["PostToolUse".to_string()]);
                assert_eq!(code, 1, "the hook fails closed, non-blocking");
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

    /// A listener that accepts every connection and never responds (a STUCK
    /// local inbox, not just an unreachable one). Dropped (not joined) so the
    /// test ends even though the holder thread sleeps on.
    fn never_answering_listener() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let holder = std::thread::spawn(move || {
            // Accept and hold every connection without reading or responding;
            // the sockets stay open (in scope) until the process exits.
            for _ in 0..8 {
                let _ = listener.accept().ok();
            }
            std::thread::sleep(std::time::Duration::from_secs(3600));
        });
        (addr, holder)
    }

    /// A stuck (never-answering) pod-local inbox must FAIL CLOSED with a
    /// clear error within the request deadline -- never silently read as an
    /// empty inbox that makes a hook/wait proceed as if nothing were queued.
    #[test]
    fn stuck_remote_inbox_fails_closed_within_the_request_deadline() {
        let (addr, holder) = never_answering_listener();
        let remote = RemoteInbox::new(
            "/x/a2a-inbox/run-stuck".to_string(),
            "t0k3n".to_string(),
            format!("http://{addr}"),
        );
        let start = std::time::Instant::now();
        let err = remote
            .pending_count()
            .expect_err("a stuck inbox is an error, not an empty count");
        let elapsed = start.elapsed();
        assert!(
            elapsed < INBOX_REQUEST_TIMEOUT + std::time::Duration::from_secs(2),
            "failed closed within the deadline: {elapsed:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("inbox API request"), "{msg}");
        assert!(msg.contains(&addr.to_string()), "{msg}");
        // The ack path fails closed the same way.
        let err = remote
            .mark_injected("hook", "m1")
            .expect_err("a stuck inbox ack is an error, not a false no-op");
        assert!(err.to_string().contains("inbox API request"), "{err}");
        // The holder thread is left to sleep on; do not join it.
        let _ = holder;
    }
}

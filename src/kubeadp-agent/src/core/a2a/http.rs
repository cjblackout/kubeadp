//! Pod-local HTTP API for a SANDBOXED run's A2A inbox (design HL-113 §7.14,
//! phase 3).
//!
//! The worker/orchestrator process OWNS the shared state database, so a
//! sandboxed run (`a2a-hook`, `mcp-a2a`, both separate processes with no
//! database grant) reaches its inbox only through THIS API on `127.0.0.1` /
//! the pod's local listener, never the database file. Authentication is a
//! per-run HMAC token derived from the jobset root key and bound to the run's
//! start time (N3):
//!
//! ```text
//! token = hex(hmac_sha256(root_key,
//!             b"kubeadp-a2a-inbox-v2" || 0x00 || run_id || 0x00 || started_at))
//! ```
//!
//! The run id is the sanitized last segment of the run's inbox
//! ([`crate::core::a2a::inbox::run_id_from_dir`], exposed as
//! [`run_id_of`]); both sides derive it from the SAME root key file
//! (`resolve_root_key`) and the run's stored start time. A request is
//! authenticated only when its token equals the token of the LIVE run it
//! addresses (the run's metadata exists and its status is `Running`),
//! compared in constant time: the token expires the moment the run finished,
//! and a retried job's new attempt mints a fresh token the previous attempt
//! never had. Failed authentication answers 401 and is counted; the first
//! failure and then every 10th are logged (count only, no secrets). Without
//! a configured root key the endpoints stay closed (the sandbox gets no
//! token and cannot read its own inbox).
//!
//! The request's `inbox` value is used ONLY to name the run (its last path
//! segment, sanitized). The store is always this host's own state root: the
//! path a client sends never decides which database is opened, so an
//! authenticated run cannot make the host create or open a database (or
//! directories) at a location of its choosing.
//!
//! # Two-phase receive protocol
//!
//! Injection is the acknowledgement, so the sandboxed side reaches its inbox
//! in two HTTP calls:
//!
//! - [`claim`] (lease): claims the oldest pending messages, marking each row
//!   `claimed_at` for [`crate::core::a2a::inbox::CLAIM_LEASE_SECS`]. A
//!   claimed-but-not-injected message (the claimer crashed between `claim`
//!   and `mark-injected`) stops being claimable by others for the lease, then
//!   returns to the queue. Exactly once across concurrent claimers: two hooks
//!   of the same run never both claim the same message.
//! - [`mark_injected`] (ack): the idempotent acknowledgement. A message
//!   counts as delivered only when this runs; a message the claimer claimed
//!   but never acked is reclaimed after the lease.
//!
//! [`peek`] reports what a `claim` would return WITHOUT claiming or
//! acknowledging: the sandboxed `wait_message` tool polls it, so a blocked
//! `wait` wakes the moment a message arrives without consuming anything.
//!
//! # Endpoints
//!
//! All three require `Authorization: Bearer <token>` for the addressed run.
//!
//! - `POST /a2a-inbox/peek` -- body `{"inbox":"<run inbox dir>"}`. Returns
//!   the pending messages (oldest first) with `ok:true`; a `count` field is
//!   convenient for polling. Never claims or acks anything.
//! - `POST /a2a-inbox/claim` -- body
//!   `{"inbox":"<run inbox dir>","max_bytes":<optional>}`. Atomically CLAIMS
//!   (leases) the oldest pending messages up to the optional byte budget and
//!   returns them. The caller must `mark-injected` each one it actually
//!   delivers.
//! - `POST /a2a-inbox/mark-injected` -- body
//!   `{"inbox":"<run inbox dir>","message_id":"<id>","channel":"<name>"}`.
//!   Idempotently acknowledges one message. No-op when already injected (or
//!   unknown).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::inbox::{inbox_dir_for, run_id_from_dir, Inbox};
use crate::core::jobset::resolve_root_key;

/// The derivation label for the per-run inbox token (distinct from the
/// jobset-secret label so the two domains never collide). V2 binds the token
/// to the run's START TIME (a retried job's new attempt mints a fresh token
/// and the previous attempt's token stops verifying).
const INBOX_TOKEN_LABEL: &[u8] = b"kubeadp-a2a-inbox-v2";
const INBOX_TOKEN_SEP: u8 = 0x00;

/// The channel name recorded when `mark-injected` acknowledges a message
/// without an explicit channel (the ack the sandboxed receive side sends
/// after a `claim`).
pub const HTTP_CLAIM_CHANNEL: &str = "http";

/// State shared by every inbox HTTP route: the resolved root key bytes (or
/// `None` when no root key is configured, i.e. the endpoints stay closed)
/// and the host's own state root (the ONLY place a request's inbox is
/// resolved against).
#[derive(Clone)]
pub struct InboxHttpState {
    root_key: Option<Arc<[u8]>>,
    state_root: PathBuf,
}

impl InboxHttpState {
    /// Resolves the jobset root key once for the whole server. A configured
    /// but unusable key is a startup error naming only the path.
    ///
    /// `state_root` is the state root the host's delivery sink writes (the
    /// worker's `Config::state_root`, the orchestrator's
    /// [`crate::core::a2a::inbox::inbox_state_root`]).
    pub fn resolve(state_root: PathBuf) -> Result<Self, String> {
        Ok(Self {
            root_key: resolve_root_key()?.map(|key| Arc::from(key.as_bytes())),
            state_root,
        })
    }

    /// Test constructor with explicit key bytes and state root.
    #[cfg(test)]
    pub fn with_key(key: Vec<u8>, state_root: &Path) -> Self {
        Self {
            root_key: Some(Arc::from(key)),
            state_root: state_root.to_path_buf(),
        }
    }

    /// The host-side inbox of `run_id`: always under this host's own state
    /// root, whatever path the client named.
    fn inbox_for(&self, run_id: &str) -> Inbox {
        Inbox::new(inbox_dir_for(&self.state_root, run_id))
    }

    fn token_for(&self, run_id: &str, started_at: u64) -> Option<String> {
        let key = self.root_key.as_ref()?;
        let mut msg = Vec::with_capacity(INBOX_TOKEN_LABEL.len() + 2 + run_id.len() + 20);
        msg.extend_from_slice(INBOX_TOKEN_LABEL);
        msg.push(INBOX_TOKEN_SEP);
        msg.extend_from_slice(run_id.as_bytes());
        msg.push(INBOX_TOKEN_SEP);
        msg.extend_from_slice(started_at.to_string().as_bytes());
        let mac = crate::core::crypto::hmac_sha256(key, &msg);
        let mut out = String::with_capacity(mac.len() * 2);
        for b in mac {
            out.push_str(&format!("{b:02x}"));
        }
        Some(out)
    }

    /// The expected token for a LIVE run: derives it from the run's stored
    /// start time and refuses when the run is not live (no metadata, already
    /// finished, or never started). This is the expiry (N3): a token is valid
    /// only while the run it addresses is live, and it is rejected once the
    /// run finished.
    ///
    /// Serves both worker jobs (`jobs` table via `JobMetadata::load` — exact
    /// run id match, never a prefix match) and orchestrator sessions
    /// (`session_state` table via `load_supervisor_state`).
    fn live_run_token(&self, run_id: &str) -> Result<String, String> {
        let secs = match crate::core::job::JobMetadata::load(&self.state_root, run_id) {
            Ok(meta) => {
                if meta.status != Some(crate::core::job::JobStatus::Running) {
                    return Err(format!("run '{run_id}' is not live"));
                }
                let started_at = meta
                    .started_at
                    .ok_or_else(|| format!("run '{run_id}' never started"))?;
                u64::try_from(started_at.timestamp()).unwrap_or(0)
            }
            Err(_) => {
                let st = crate::orchestrate::state::load_supervisor_state(&self.state_root)
                    .ok()
                    .flatten()
                    .ok_or_else(|| format!("run '{run_id}' has no metadata"))?;
                let sess = st
                    .sessions
                    .get(run_id)
                    .ok_or_else(|| format!("run '{run_id}' has no metadata"))?;
                if sess.status != crate::orchestrate::state::SessionStatus::Running {
                    return Err(format!("run '{run_id}' is not live"));
                }
                sess.start_time_epoch
                    .ok_or_else(|| format!("run '{run_id}' never started"))?
            }
        };
        self.token_for(run_id, secs)
            .ok_or_else(|| "no A2A inbox key configured; the endpoint is closed".to_string())
    }
}

/// The run id of an inbox dir (the sanitized last segment) -- the shared
/// identity both sides derive the token and the rows from.
pub fn run_id_of(inbox_dir: &Path) -> String {
    run_id_from_dir(inbox_dir)
}

/// The bearer token a sandboxed run must present for `inbox_dir`. `None`
/// when the root key is not configured (the run has no inbox access) or the
/// run has no start time: the token is bound to the run's start time (N3), so
/// a run that has not started has no token.
pub fn token_for_run(inbox_dir: &Path, started_at: u64) -> Option<String> {
    // Only the key matters for the token; the state root is unused here.
    let state = InboxHttpState::resolve(PathBuf::new()).ok()?;
    state.token_for(&run_id_of(inbox_dir), started_at)
}

/// Count of repeated A2A inbox auth failures this process has seen. The
/// first failure and then every 10th are logged (count only, never the
/// presented token or any other secret).
static AUTH_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The body of the `claim` request.
#[derive(Debug, Deserialize, Serialize)]
pub struct ClaimRequest {
    /// The run's inbox dir (`KUBEADP_A2A_INBOX_DIR`), verbatim as the
    /// sandbox received it; the server derives the run id from it.
    pub inbox: String,
    /// Optional byte budget (like `drain_capped`); counts the framed text
    /// plus one separator between messages. Omitted = drain everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
}

/// The body of the `peek` request.
#[derive(Debug, Deserialize, Serialize)]
pub struct PeekRequest {
    /// The run's inbox dir, verbatim as the sandbox received it.
    pub inbox: String,
}

/// One claimed message (the framed text plus its identity).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedMessage {
    pub message_id: String,
    pub sender: String,
    pub framed: String,
    pub hop: u32,
    pub context_id: Option<String>,
    pub task_id: Option<String>,
    pub queued_at: u64,
}

/// The body of the `mark-injected` request.
#[derive(Debug, Deserialize, Serialize)]
pub struct MarkInjectedRequest {
    pub inbox: String,
    pub message_id: String,
    /// The channel to record (default `http`).
    #[serde(default = "default_channel")]
    pub channel: String,
}

fn default_channel() -> String {
    HTTP_CLAIM_CHANNEL.to_string()
}

/// The JSON error / ack body.
#[derive(Debug, Serialize)]
pub struct InboxHttpResponse<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<ClaimedMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marked: Option<bool>,
    #[serde(flatten)]
    pub data: std::marker::PhantomData<T>,
}

/// Builds the inbox HTTP router. The endpoints carry their own per-run token
/// check (NOT the operator bearer / API v1 layer), so the caller merges this
/// router AFTER the bearer layer exactly like the A2A routes.
pub fn inbox_http_router(state: InboxHttpState) -> Router {
    Router::new()
        .route("/a2a-inbox/peek", post(peek))
        .route("/a2a-inbox/claim", post(claim))
        .route("/a2a-inbox/mark-injected", post(mark_injected))
        .with_state(Arc::new(state))
}

/// Extracts the bearer token from `Authorization` (constant-time compare is
/// done by the caller against the derived token).
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Authenticates the request against the addressed run and hands back its
/// inbox. The token is the per-run HMAC bound to the run's start time, and it
/// is accepted only while the run is live (rejected once it finished, N3).
/// `Err` (with the error already written) on any failure; a failed
/// authentication answers 401 and is counted, the first failure and then
/// every 10th logged (count only, no secrets).
fn authed_inbox(
    headers: &HeaderMap,
    state: &InboxHttpState,
    inbox_dir: &str,
) -> Result<Inbox, StatusCode> {
    use std::sync::atomic::Ordering;
    let fail = |msg: &'static str| -> Result<Inbox, StatusCode> {
        let n = AUTH_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1 || n.is_multiple_of(10) {
            eprintln!("a2a inbox: repeated auth failures: {n} ({msg})");
        }
        Err(StatusCode::UNAUTHORIZED)
    };
    if bearer_token(headers).is_none() {
        return fail("missing Authorization: Bearer token");
    }
    let run_id = run_id_of(Path::new(inbox_dir));
    // The expected token: derived from the run's stored start time and
    // accepted only while the run is live.
    let Ok(stored) = state.live_run_token(&run_id) else {
        return fail("unknown or finished run");
    };
    let presented = bearer_token(headers).unwrap_or_default();
    if !crate::core::crypto::ct_eq(presented.as_bytes(), stored.as_bytes()) {
        return fail("invalid A2A inbox token");
    }
    Ok(state.inbox_for(&run_id))
}

/// `POST /a2a-inbox/peek`: reports the pending messages a `claim` would
/// return, WITHOUT claiming or acknowledging anything. The sandboxed
/// `wait_message` tool polls this, so a blocked wait wakes the moment a
/// message arrives without consuming (injecting) it.
async fn peek(
    State(state): State<Arc<InboxHttpState>>,
    headers: HeaderMap,
    Json(req): Json<PeekRequest>,
) -> Result<Json<InboxHttpResponse<ClaimedMessage>>, StatusCode> {
    let inbox = authed_inbox(&headers, &state, &req.inbox)?;
    // SQLite is blocking (and may wait on the write lock): never on a tokio
    // worker thread.
    let messages = tokio::task::spawn_blocking(move || peek_inbox(&inbox, usize::MAX))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let count = messages.len();
    Ok(Json(InboxHttpResponse {
        ok: true,
        error: None,
        messages: Some(messages),
        count: Some(count),
        marked: None,
        data: std::marker::PhantomData,
    }))
}

/// `POST /a2a-inbox/claim`: claims (LEASES) the oldest pending messages up to
/// the byte budget, atomically. The claim marks each row `claimed_at`, after
/// [`CLAIM_LEASE_SECS`] the row returns to the queue unless the claimer
/// acknowledges it with `mark-injected`. Injection is the acknowledgement.
async fn claim(
    State(state): State<Arc<InboxHttpState>>,
    headers: HeaderMap,
    Json(req): Json<ClaimRequest>,
) -> Result<Json<InboxHttpResponse<ClaimedMessage>>, StatusCode> {
    let inbox = authed_inbox(&headers, &state, &req.inbox)?;
    let budget = req.max_bytes.unwrap_or(usize::MAX);
    let claimed = tokio::task::spawn_blocking(move || claim_leased(&inbox, budget))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let count = claimed.len();
    Ok(Json(InboxHttpResponse {
        ok: true,
        error: None,
        messages: Some(claimed),
        count: Some(count),
        marked: None,
        data: std::marker::PhantomData,
    }))
}

/// `POST /a2a-inbox/mark-injected`: idempotently acknowledges one message.
async fn mark_injected(
    State(state): State<Arc<InboxHttpState>>,
    headers: HeaderMap,
    Json(req): Json<MarkInjectedRequest>,
) -> Result<Json<InboxHttpResponse<bool>>, StatusCode> {
    let inbox = authed_inbox(&headers, &state, &req.inbox)?;
    let marked =
        tokio::task::spawn_blocking(move || inbox.mark_injected(&req.message_id, &req.channel))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(InboxHttpResponse {
        ok: true,
        error: None,
        messages: None,
        count: None,
        marked: Some(marked),
        data: std::marker::PhantomData,
    }))
}

/// Lists the claimable (pending, unleased) messages for a run, oldest first,
/// filtered by the byte budget. Read-only: never claims or acks.
fn peek_inbox(inbox: &Inbox, max_bytes: usize) -> Vec<ClaimedMessage> {
    let run_id = inbox.run_id();
    let now = crate::core::a2a::inbox::now_unix();
    inbox
        .with_conn(|conn| internal_list_in_txn(conn, inbox, max_bytes, false, now, &run_id))
        .unwrap_or_default()
}

/// Atomically claims (leases) pending messages for the caller under the byte
/// budget: marks each returned row `claimed_at` (now), so exactly one
/// concurrent claimer wins each message. A claimed-but-unacked message stops
/// being claimable for [`CLAIM_LEASE_SECS`], then returns to the queue.
fn claim_leased(inbox: &Inbox, max_bytes: usize) -> Vec<ClaimedMessage> {
    let run_id = inbox.run_id();
    let now = crate::core::a2a::inbox::now_unix();
    inbox
        .with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE")
                .map_err(|e| format!("failed to begin the A2A claim transaction: {e}"))?;
            let result = internal_list_in_txn(conn, inbox, max_bytes, true, now, &run_id);
            match result {
                Ok(out) => {
                    conn.execute_batch("COMMIT")
                        .map_err(|e| format!("failed to commit the A2A claim transaction: {e}"))?;
                    Ok(out)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e)
                }
            }
        })
        .unwrap_or_default()
}

/// The shared internal list/claim implementation.
///
/// With `claim=true` runs inside the caller's `BEGIN IMMEDIATE` transaction
/// and marks each returned row `claimed_at`; with `claim=false` it is a
/// read-only peek outside any transaction. Claimable rows are queued
/// (`injected_at IS NULL`), not yet expired (`(?3 - queued_at) < ?4`), and
/// either never claimed or past their lease.
#[allow(clippy::too_many_arguments)]
fn internal_list_in_txn(
    conn: &rusqlite::Connection,
    inbox: &Inbox,
    max_bytes: usize,
    claim: bool,
    now: u64,
    run_id: &str,
) -> Result<Vec<ClaimedMessage>, String> {
    let ttl = inbox.ttl_secs().min(i64::MAX as u64) as i64;
    let lease = inbox.lease_secs() as i64;
    let mut stmt = conn
        .prepare(
            "SELECT message_id, sender, framed, hop, context_id, task_id, queued_at
             FROM a2a_message
             WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL
               AND (?3 - queued_at) < ?4
               AND (claimed_at IS NULL OR (?3 - claimed_at) >= ?5)
             ORDER BY queued_at, message_id",
        )
        .map_err(|e| format!("failed to prepare the A2A inbox query: {e}"))?;
    let rows = stmt
        .query_map(
            rusqlite::params![run_id, i64::from(inbox.fake()), now, ttl, lease],
            |row| {
                Ok(ClaimedMessage {
                    message_id: row.get(0)?,
                    sender: row.get(1)?,
                    framed: row.get(2)?,
                    hop: row.get(3)?,
                    context_id: row.get(4)?,
                    task_id: row.get(5)?,
                    queued_at: row.get(6)?,
                })
            },
        )
        .map_err(|e| format!("failed to read the A2A pending messages: {e}"))?;
    let mut out = Vec::new();
    let mut budget = max_bytes;
    for row in rows.flatten() {
        let sep = if out.is_empty() { 0 } else { 2 };
        if row.framed.len() + sep > budget {
            break;
        }
        budget -= row.framed.len() + sep;
        if claim {
            conn.execute(
                "UPDATE a2a_message SET claimed_at=?1
                 WHERE run_id=?2 AND message_id=?3 AND fake=?4 AND injected_at IS NULL",
                rusqlite::params![now, run_id, row.message_id, i64::from(inbox.fake())],
            )
            .map_err(|e| format!("failed to lease the A2A message: {e}"))?;
        }
        out.push(row);
    }
    Ok(out)
}

/// The host ensures a single message always fits a claim budget: the count
/// of a framed message is capped at the queue byte bound, and the byte
/// accounting here mirrors `drain_capped` exactly.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::inbox::QueuedMessage;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use serde_json::json;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn key() -> Vec<u8> {
        vec![b'k'; 32]
    }

    fn inbox_dir(root: &Path, run: &str) -> std::path::PathBuf {
        crate::core::a2a::inbox::inbox_dir_for(root, run)
    }

    /// Seeds a LIVE run (status Running, started now) in the test state root:
    /// the endpoints accept a token only while the addressed run is live
    /// (N3), so every authenticated request needs this row.
    fn seed_live_run(root: &Path, run: &str) {
        let meta = crate::core::job::JobMetadata {
            job_id: run.to_string(),
            project: "t".to_string(),
            prompt: "t".to_string(),
            resolved_prompt: "t".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "fast".to_string(),
            add_dirs: Vec::new(),
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: root.join(run),
            branch: format!("agent-run/{run}"),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            exit_code: None,
            status: Some(crate::core::job::JobStatus::Running),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            pid_start_time: None,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: Some(chrono::Utc::now()),
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.save(root).unwrap();
    }

    fn token(state: &InboxHttpState, run: &str) -> String {
        state.live_run_token(run).unwrap()
    }

    fn msg(id: &str, text: &str) -> QueuedMessage {
        QueuedMessage {
            message_id: id.to_string(),
            sender: "sender-agent".to_string(),
            hop: 1,
            context_id: None,
            task_id: None,
            framed: text.to_string(),
            queued_at: crate::core::a2a::inbox::now_unix(),
        }
    }

    async fn body_json(res: axum::response::Response) -> serde_json::Value {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn token_is_deterministic_domain_separated_and_start_bound() {
        let s1 = InboxHttpState::with_key(key(), Path::new("/root"));
        let s2 = InboxHttpState::with_key(key(), Path::new("/root"));
        assert_eq!(
            s1.token_for("run-1", 1_000_000),
            s2.token_for("run-1", 1_000_000)
        );
        // Different runs differ.
        assert_ne!(
            s1.token_for("run-1", 1_000_000),
            s1.token_for("run-2", 1_000_000)
        );
        // Different keys differ.
        let s3 = InboxHttpState::with_key(vec![b'j'; 32], Path::new("/root"));
        assert_ne!(
            s1.token_for("run-1", 1_000_000),
            s3.token_for("run-1", 1_000_000)
        );
        // N3: the token is bound to the run's start time - a new attempt
        // (a new started_at) mints a fresh token and the previous attempt's
        // token stops verifying.
        assert_ne!(
            s1.token_for("run-1", 1_000_000),
            s1.token_for("run-1", 1_000_001)
        );
        // roun id derives from the dir.
        assert_eq!(run_id_of(&inbox_dir(Path::new("/root"), "run-1")), "run-1");
        // A hostile run segment (a `..` escape) never forms a `..` path
        // element: each ambiguous byte is hex-encoded into a plain segment
        // used only as a row key, never a path.
        assert_eq!(
            run_id_of(Path::new("/root/a2a-inbox/../../etc/passwd")),
            "passwd"
        );
        assert_eq!(
            crate::core::a2a::inbox::inbox_dir_for(Path::new("/root"), "../etc/passwd"),
            Path::new("/root/a2a-inbox/2e2e2f6574632f706173737764")
        );
    }

    #[tokio::test]
    async fn claim_leases_and_mark_injected_acks_exactly_once_across_channels() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-a");
        let dir = inbox_dir(&root, "run-a");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = token(&state, "run-a");

        let inbox = Inbox::new(dir.clone());
        inbox.enqueue(msg("m1", "one")).unwrap();
        inbox.enqueue(msg("m2", "two")).unwrap();

        // A second claimer before the first acks must not see the leased msgs.
        let claim_body = json!({"inbox": dir.to_str().unwrap()}).to_string();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(claim_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_json(res).await;
        assert!(body["ok"].as_bool().unwrap());
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["message_id"], "m1");
        assert_eq!(msgs[1]["message_id"], "m2");

        // THE CLAIM IS A LEASE, NOT THE ACK: the messages are still not
        // injected, but no longer pending to ANYONE (a second claimer, the
        // host ladder, a `peek`) until the claimer acknowledges or the lease
        // lapses.
        assert_eq!(inbox.pending().len(), 0, "leased, not pending");
        assert!(!inbox.injected("m1"));
        // A peek also excludes the leased messages.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/peek")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(claim_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        assert_eq!(body["count"], 0, "peek does not see leased messages");

        // A second claim while the first holds the lease returns nothing.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(claim_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        assert!(body["messages"].as_array().unwrap().is_empty());

        // Only mark-injected is the ack: after it, the message is injected
        // and cannot be claimed or injected again.
        for mid in ["m1", "m2"] {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/a2a-inbox/mark-injected")
                        .header("content-type", "application/json")
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::from(
                            json!({"inbox": dir.to_str().unwrap(), "message_id": mid, "channel": "http"})
                                .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = body_json(res).await;
            assert_eq!(body["ok"], true);
            assert_eq!(body["marked"], true);
        }
        assert!(inbox.pending().is_empty());
        assert!(inbox.injected("m1") && inbox.injected("m2"));
    }

    #[tokio::test]
    async fn leased_message_returns_to_the_queue_after_the_lease() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-lease");
        let dir = inbox_dir(&root, "run-lease");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = token(&state, "run-lease");

        let inbox = Inbox::new(dir.clone());
        inbox.enqueue(msg("m1", "lease me")).unwrap();

        let claim_json = json!({"inbox": dir.to_str().unwrap()}).to_string();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(claim_json.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);

        // Age the claimed_at past the lease: the message becomes claimable again
        // (the claimer never acknowledged it -- e.g. the hook crashed).
        inbox
            .with_conn(|conn| {
                conn.execute(
                    "UPDATE a2a_message SET claimed_at=?1
                     WHERE run_id=?2 AND message_id='m1'",
                    rusqlite::params![
                        crate::core::a2a::inbox::now_unix()
                            .saturating_sub(crate::core::a2a::inbox::CLAIM_LEASE_SECS + 2),
                        inbox.run_id()
                    ],
                )
                .map_err(|e| format!("{e}"))?;
                Ok(())
            })
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(claim_json))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        assert_eq!(body["messages"].as_array().unwrap().len(), 1, "reclaimable");
    }

    #[tokio::test]
    async fn peek_reports_pending_without_claiming_or_acking() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-peek");
        let dir = inbox_dir(&root, "run-peek");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = token(&state, "run-peek");

        let inbox = Inbox::new(dir.clone());
        inbox.enqueue(msg("m1", "peek me")).unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/peek")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_json(res).await;
        assert_eq!(body["ok"], true);
        assert_eq!(body["count"], 1);
        assert_eq!(body["messages"][0]["message_id"], "m1");
        // Peeking consumes nothing: the message is still queued AND uninjected.
        assert_eq!(inbox.pending().len(), 1);
        assert!(!inbox.injected("m1"));
    }

    #[tokio::test]
    async fn claim_honours_the_byte_budget() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-b");
        let dir = inbox_dir(&root, "run-b");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = token(&state, "run-b");

        let inbox = Inbox::new(dir.clone());
        inbox.enqueue(msg("m1", "aaaa")).unwrap();
        inbox.enqueue(msg("m2", "bbbbbb")).unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap(), "max_bytes": 6}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        let msgs = body["messages"].as_array().unwrap();
        // "aaaa" (4) fits; "bbbbbb" (6) does not (4+2 separator+6 > 6).
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["message_id"], "m1");
        // m1 is leased (not pending), m2 stays queued and unclaimed.
        assert_eq!(inbox.pending().len(), 1, "only m2 remains pending");
        // The unreturned remainder is claimable next time (a second claim with
        // a bigger budget serves it).
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap(), "max_bytes": 100}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["messages"][0]["message_id"], "m2");
    }

    #[tokio::test]
    async fn wrong_or_missing_token_is_rejected_and_cross_run_denied() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-a");
        seed_live_run(&root, "run-b");
        let dir_a = inbox_dir(&root, "run-a");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token_a = token(&state, "run-a");
        let token_b = token(&state, "run-b");

        let inbox_a = Inbox::new(dir_a.clone());
        inbox_a.enqueue(msg("m1", "secret")).unwrap();

        // Missing token -> 401 (the auth failure is counted and throttled
        // logging keeps the count only, N3).
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"inbox": dir_a.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // Wrong token (run-a claims with run-b's token) -> 401.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token_b}"))
                    .body(Body::from(
                        json!({"inbox": dir_a.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        // Nothing was disclosed (still pending).
        assert_eq!(inbox_a.pending().len(), 1);

        // Correct token works.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token_a}"))
                    .body(Body::from(
                        json!({"inbox": dir_a.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(res).await;
        assert_eq!(body["ok"], true);
    }

    #[tokio::test]
    async fn finished_or_unknown_run_keeps_the_endpoint_closed() {
        // N3: the token is accepted only while the run is live. A finished
        // (Done) run and an unknown run are refused even with the token the
        // run had while it was live.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        seed_live_run(&root, "run-done");
        // Age the run: mark it Done in the stored metadata.
        let mut meta = crate::core::job::JobMetadata::resolve(&root, "run-done").unwrap();
        meta.status = Some(crate::core::job::JobStatus::Done);
        meta.save(&root).unwrap();
        let dir = inbox_dir(&root, "run-done");
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer whatever")
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // An unknown run: no metadata at all -> closed.
        let dir = inbox_dir(&root, "run-ghost");
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer whatever")
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sanitized_run_key_is_collision_free() {
        // N3: two different run ids never sanitize onto the same key
        // (`a.b` and `a_b` used to both become `a_b`).
        let a = crate::core::a2a::inbox::inbox_dir_for(Path::new("/root"), "a.b");
        let b = crate::core::a2a::inbox::inbox_dir_for(Path::new("/root"), "a_b");
        assert_ne!(a, b);
        // A hostile `..` escape stays a plain segment.
        let esc = crate::core::a2a::inbox::inbox_dir_for(Path::new("/root"), "../../etc/passwd");
        assert!(!esc.to_string_lossy().contains(".."));
        // Sanitization is idempotent: the sanitized segment re-sanitizes to
        // itself (the recovery path feeds sanitized ids back through).
        let seg = a.file_name().unwrap().to_str().unwrap().to_string();
        assert_eq!(
            crate::core::a2a::inbox::inbox_dir_for(Path::new("/root"), &seg),
            a
        );
    }

    #[tokio::test]
    async fn no_configured_root_key_keeps_the_endpoints_closed() {
        // With no root key the token derivation yields None -> authed request
        // is rejected even with an arbitrary token.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        let dir = inbox_dir(&root, "run-x");
        let state = InboxHttpState {
            root_key: None,
            state_root: root.clone(),
        };
        let app = inbox_http_router(state.clone());
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/claim")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer whatever")
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn mark_injected_is_idempotent_and_per_run() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-c");
        let dir = inbox_dir(&root, "run-c");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = token(&state, "run-c");

        let inbox = Inbox::new(dir.clone());
        inbox.enqueue(msg("m1", "one")).unwrap();

        let do_mark = |app: Router, msg_id: &str, ch: &str| {
            let t = token.clone();
            let dir = dir.to_str().unwrap().to_string();
            let json = json!({"inbox": dir, "message_id": msg_id, "channel": ch}).to_string();
            async move {
                app.clone()
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/a2a-inbox/mark-injected")
                            .header("content-type", "application/json")
                            .header("authorization", format!("Bearer {t}"))
                            .body(Body::from(json))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
            }
        };
        let res = do_mark(app.clone(), "m1", "tool_result").await;
        let body = body_json(res).await;
        assert_eq!(body["ok"], true);
        assert_eq!(body["marked"], true);
        // A second mark is a no-op (still ok, marked false).
        let res = do_mark(app.clone(), "m1", "tool_result").await;
        let body = body_json(res).await;
        assert_eq!(body["ok"], true);
        assert_eq!(body["marked"], false);
        assert!(inbox.injected("m1"));
    }

    // Multi-thread runtime on purpose: the sandboxed receive side calls the
    // API with a blocking client on its own OS thread (`RemoteInbox::post`),
    // which joins while this test thread waits; the axum server task must run
    // on a different worker or the request can never be served.
    #[tokio::test(flavor = "multi_thread")]
    async fn a2a_hook_subcommand_reaches_the_inbox_over_pod_local_http() {
        // The sandboxed `a2a-hook` subcommand runs as a SEPARATE process with
        // ONLY the env vars (INBOX_DIR + per-run token + URL): it must reach
        // its run's inbox through the local HTTP API and deliver (inject)
        // every event of both engines, exactly like the file-based path.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-hook");
        let dir = inbox_dir(&root, "run-hook");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let inbox = Inbox::new(dir.clone());

        // Serve the pod-local API on an ephemeral loopback port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // The launcher-derived env a sandboxed hook sees: the per-run token
        // (host-derived), its inbox dir, and the pod-local base URL. The
        // whole body below is synchronous (the hook and its assertions never
        // await), so the shared testenv guard can be scoped around it; the
        // hook's HTTP round trips run on their own threads.
        let token = state.live_run_token("run-hook").unwrap();
        let url = format!("http://{addr}");
        let dir_str = dir.to_str().unwrap().to_string();
        crate::core::a2a::testenv::with_env(
            &[
                (crate::core::a2a::inbox::INBOX_ENV, Some(&dir_str)),
                (crate::core::a2a::receive::INBOX_TOKEN_ENV, Some(&token)),
                (crate::core::a2a::receive::INBOX_URL_ENV, Some(&url)),
            ],
            || {
                // Every hook event of both engines, delivered one message each.
                let events = [
                    "PostToolUse",
                    "UserPromptSubmit",
                    "Stop",
                    "pre-invocation",
                    "post-invocation",
                    "stop",
                ];
                for (i, event) in events.iter().enumerate() {
                    let mid = format!("m-{i}");
                    inbox
                        .enqueue(msg(&mid, &format!("deliver {event}")))
                        .unwrap();
                    let code = crate::core::a2a::inject::run_a2a_hook(&[event.to_string()]);
                    assert_eq!(code, 0, "event {event}");
                    assert!(
                        inbox.injected(&mid),
                        "{event} injected (acknowledged over HTTP): {mid}"
                    );
                    assert!(!inbox.pending().iter().any(|m| m.message_id == mid));
                }

                // A second invocation with an empty inbox is a silent no-op for
                // the Claude events and `{}` for the agy events (same contract
                // as the direct path).
                for event in ["PostToolUse", "Stop"] {
                    assert_eq!(
                        crate::core::a2a::inject::run_a2a_hook(&[event.to_string()]),
                        0,
                        "{event} empty: exit 0"
                    );
                }
                for event in ["pre-invocation", "post-invocation", "stop"] {
                    assert_eq!(
                        crate::core::a2a::inject::run_a2a_hook(&[event.to_string()]),
                        0,
                        "{event} empty: exit 0"
                    );
                }
            },
        );
        server.abort();
    }

    /// The path a client sends only NAMES the run: the host never opens (or
    /// creates) a database at a client-chosen location, and a forged prefix
    /// still reads the run's real inbox under the host's own state root.
    #[tokio::test]
    async fn client_supplied_inbox_path_never_selects_the_database() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        seed_live_run(&root, "run-p");
        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = token(&state, "run-p");
        Inbox::new(inbox_dir(&root, "run-p"))
            .enqueue(msg("m1", "real"))
            .unwrap();

        // Same run id, hostile prefixes: a canonical-looking layout under a
        // directory the caller picked, and a non-canonical one.
        let evil_root = tmp.path().join("evil");
        for forged in [
            evil_root.join("a2a-inbox").join("run-p"),
            tmp.path().join("elsewhere").join("run-p"),
        ] {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/a2a-inbox/peek")
                        .header("content-type", "application/json")
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::from(
                            json!({"inbox": forged.to_str().unwrap()}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = body_json(res).await;
            assert_eq!(body["ok"], true);
            assert_eq!(body["count"], 1, "the run's real inbox is served");
        }
        assert!(
            !evil_root.exists(),
            "the host must not create a state database under a client-chosen path"
        );
    }

    #[tokio::test]
    async fn live_orchestrator_session_authenticates_and_expired_messages_are_hidden() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        crate::core::job::ensure_state_store(&root).unwrap();

        let now = crate::core::a2a::inbox::now_unix();
        let mut sup = crate::orchestrate::state::SupervisorState::new(1234);
        let mut sess = crate::orchestrate::state::SessionState::new("homelab".to_string());
        sess.status = crate::orchestrate::state::SessionStatus::Running;
        sess.start_time_epoch = Some(now);
        sup.sessions.insert("homelab".to_string(), sess);
        crate::orchestrate::state::save_supervisor_state(&root, &sup).unwrap();

        let state = InboxHttpState::with_key(key(), &root);
        let app = inbox_http_router(state.clone());
        let token = state
            .live_run_token("homelab")
            .expect("live orchestrator session token");
        let dir = inbox_dir(&root, "homelab");
        let inbox = Inbox::new(dir.clone());

        let mut expired = msg("m-expired", "too old");
        expired.queued_at = now.saturating_sub(crate::core::a2a::inbox::DEFAULT_TTL_SECS + 10);
        inbox.enqueue(expired).unwrap();
        inbox.enqueue(msg("m-fresh", "fresh")).unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a-inbox/peek")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        json!({"inbox": dir.to_str().unwrap()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_json(res).await;
        assert_eq!(body["count"], 1, "expired message must not be served");
        assert_eq!(body["messages"][0]["message_id"], "m-fresh");
    }
}

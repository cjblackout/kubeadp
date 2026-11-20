//! Engine-agnostic durable per-run A2A inbox (design HL-113 §7.14, phase 3;
//! design HL-125 stream 3 C2: refactored behind an [`InboxStore`] type).
//!
//! Every message accepted by the ingress (after the existing checks: framing
//! as untrusted data, idempotency by message id, hop limit, size caps) is
//! stored by the run's inbox in the shared per-pod state database
//! (`<state_root>/state.sqlite`, [`crate::core::job::open_state_db`]) in state
//! `queued`. Injecting a message into the model's context is the
//! acknowledgement: the row's `injected_at`/`channel` columns record the
//! injection, so the message state moves `queued -> injected`. The harness
//! (not the model) owns the inbox: the model never reads it, the injection
//! channels do ([`crate::core::a2a::inject`]).
//!
//! # Storage
//!
//! The AO (Always On) / orchestrator / worker pod process owns the database:
//! only THIS process writes and reads the inbox directly (the A2A sink and
//! the escalation ladder of `worker/a2a.rs` and `orchestrate/a2a.rs`). A
//! SANDBOXED run (`a2a-hook` and `mcp-a2a` are separate processes) reaches
//! its inbox ONLY through the pod's local HTTP API ([`crate::core::a2a::http`])
//! under a per-run token, NEVER the database file -- so the sandbox gets no
//! sqlite/wal/shm path and no file grants. The two sides agree on the run's
//! inbox via the ONE shared root helper ([`inbox_state_root`]).
//!
//! # Exactly-once
//!
//! The inbox row is the dedupe store: a message id already present as a
//! queued row (`injected_at IS NULL`) or as an injected row is a no-op on
//! re-enqueue (a sender retry is harmless), and injecting updates the row
//! itself, so two concurrent channels never both inject the same message.
//! Exactly once by message id across ALL channels.
//!
//! # Bounds
//!
//! The queue is bounded like the worker's coalescing buffer
//! (`MAX_QUEUED_MSGS` / `MAX_QUEUED_BYTES`, oldest dropped) and records
//! overflow with a durable note ([`DROPPED_NOTE`]). The note is stored as an
//! ordinary `a2a_message` row (sender [`NOTE_SENDER`]) with the newest
//! `queued_at`, so it counts toward the bound, is served by `pending`/`drain`
//! and is consumed by an injection exactly like the phase-1 note message --
//! and unlike the messages it reports it is never dropped (it is always the
//! oldest-drop target's last survivor). Uninjected messages expire after
//! `ttl_secs` (default [`DEFAULT_TTL_SECS`]); expired messages stop being
//! served and are pruned by the host.
//!
//! Injected rows are KEPT (the acknowledged state is the durable dedupe
//! store: a re-sent id is a no-op for the whole run, exactly like the phase-1
//! injection markers); they simply leave `pending`. A run's queued history is
//! therefore bounded, its acknowledged history is monotonic.
//!
//! # Audit and text deletion (SPEC §7.15)
//!
//! Every lifecycle event writes a metadata-only row to `a2a_audit` through
//! [`crate::core::a2a::audit`]: `queued` on accept, `injected` on injection,
//! `expired` on TTL expiry, `dropped` on queue overflow. Message text exists
//! only in `a2a_message.framed`; it is set to `NULL` 60 s after injection and
//! at once on expire/drop. The 14-day purge ([`purge`]) runs at startup
//! and at least hourly ([`start_purge_task`]).
//!
//! # Cross-process, crash-safe claims
//!
//! The inbox is safe for MANY threads AND MANY processes on the same
//! database: every mutation runs inside a `BEGIN IMMEDIATE` transaction on a
//! fresh read-write connection (SQLite WAL, busy timeout) so state
//! transitions (`queued -> injected`, claim + prune, lease steeling) take
//! effect under the database write lock. Concurrent writers simply wait for
//! the lock (busy_timeout), so a concurrent `enqueue` plus a concurrent
//! `mark_injected` on different messages never corrupt anything and both
//! win.
//!
//! # Layout (paths)
//!
//! ```text
//! <state_root>/state.sqlite   the shared per-pod db (migration v3; v7 for a2a_audit)
//! ```
//!
//! The run's inbox dir `<state_root>/a2a-inbox/<run_id>/` is NOT a store any
//! more: it survives only as the shared identity (the run key) both sides
//! derive, and no message or marker is written to it.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::core::a2a::audit::{outcome, AuditRow};

/// Environment variable naming the shared inbox state root, resolved EXACTLY
/// like the agent-run `state_root` ([`inbox_state_root`]).
///
/// This is the ONE definition both sides of a run's inbox resolve: the host
/// delivery (the A2A sink, `orchestrate/a2a.rs` / `http_server.rs`) and the
/// launcher that derives the run's inbox from it. Keeping it here
/// guarantees a session's inbox is the root the sink writes, never a
/// sibling root derived from the supervisor state file's parent (historically
/// `/tmp`, wiped at every pod restart).
pub const INBOX_STATE_ROOT_ENV: &str = "AGENT_RUN_STATE_ROOT";

/// Directory name of a run's inbox under its state root
/// (`<state_root>/a2a-inbox/<run_id>/`): the shared identity both sides of a
/// run's inbox derive. No message or marker is written here (the store is
/// the shared database); the path survives only as the run key.
pub const INBOX_DIR_NAME: &str = "a2a-inbox";

/// Environment variable naming the run's inbox for the injecting processes
/// (the `mcp-a2a` server and the `a2a-hook` subcommand). Its value is the
/// run's INBOX DIR (`<state_root>/a2a-inbox/<run>/`) exactly as before, so
/// neither process changes. The path is a run KEY / identity, not a writable
/// directory grant: the store is the shared database and a sandboxed process
/// reads through the local HTTP API ([`crate::core::a2a::http`]) keyed by
/// this path, never the filesystem.
pub const INBOX_ENV: &str = "KUBEADP_A2A_INBOX_DIR";

/// Maximum number of queued messages (oldest dropped; mirrors the worker's
/// `MAX_COALESCE_MSGS`).
pub const MAX_QUEUED_MSGS: usize = 8;

/// Maximum combined bytes of the queued messages (oldest dropped; mirrors the
/// worker's `MAX_COALESCE_BYTES`).
pub const MAX_QUEUED_BYTES: usize = 8 * 1024;

/// TTL for uninjected messages (seconds). An expired message stops being
/// served and is pruned by the host; the sender's task going `failed` on
/// expiry is the phase-2 escalation ladder.
pub const DEFAULT_TTL_SECS: u64 = 3600;

/// How long a sandboxed process may hold a CLAIMED (leased) message before it
/// becomes claimable again. The pod-local HTTP `claim` endpoint leases — it
/// does NOT acknowledge: a claimed-but-not-injected message (the claimer
/// crashed between claim and `mark-injected`) stops being visible to other
/// claimers for this long, then returns to the queue. Short enough that a
/// dead hook does not stall delivery, long enough that a live hook has time
/// to build its injection output and acknowledge.
pub const CLAIM_LEASE_SECS: u64 = 60;

/// How long an INJECTED (acknowledged) row is kept as the dedupe store
/// (seconds). A re-sent message id inside this window is a no-op; past it
/// the row is pruned so a long-lived run's acknowledged history (and the
/// shared database) stays bounded.
pub const INJECTED_RETENTION_SECS: u64 = 24 * 3600;

/// How long the message TEXT survives on an injected inbox row (SPEC §7.15):
/// 60 s after `injected_at` the `framed` column is set `NULL`, so a
/// long-lived run keeps only metadata (and the exactly-once dedupe key), not
/// the message body. Set `NULL` at once on `expired`/`dropped` (those rows are
/// removed anyway; the audit row carries the metadata).
pub const TEXT_NULL_AFTER_INJECTED_SECS: u64 = 60;

/// The retention of [`crate::core::a2a::audit`] rows and of text-less inbox
/// rows: 14 days after their timestamp (SPEC §7.15). Re-exported here so the
/// inbox purge and the audit purge agree on one constant.
pub use crate::core::a2a::audit::AUDIT_RETENTION_SECS as PURGE_RETENTION_SECS;

/// The durable note recorded when the bounded queue dropped messages.
pub const DROPPED_NOTE: &str = "[note] earlier message(s) dropped by the A2A inbox";

/// The sender recorded on the durable drop note (it is not from another
/// agent, so it must never look like A2A traffic).
pub const NOTE_SENDER: &str = "inbox";

/// Unix seconds now.
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A fresh message id for the durable drop note. The prefix never collides
/// with agent traffic (agent ids are `m-<sha256(message_id)>`).
fn note_message_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    format!(
        "inbox-note-{}-{}",
        nanos,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// One message stored in the inbox. `framed` is the already-framed text
/// produced by ingress (untrusted DATA, never instructions) or, for the
/// durable drop note, the note text itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedMessage {
    /// The A2A message id (the dedupe key).
    pub message_id: String,
    /// The claimed sender (for `reply`: the `to` argument).
    pub sender: String,
    /// Incoming hop count of the message (for `reply`: `incoming_hop`).
    pub hop: u32,
    /// Optional A2A context id the message addressed (for `reply`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    /// Optional A2A task id the message addressed (for `reply`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// The framed message text (untrusted data, stored verbatim).
    pub framed: String,
    /// Unix seconds the message was queued.
    pub queued_at: u64,
}

/// Outcome of an enqueue: `Stored` when the message is new, `Duplicate` when
/// the message id was already queued or already injected (a sender retry is a
/// harmless no-op).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Stored,
    Duplicate,
}

/// The storage class of an [`Inbox`]: a real repository on the shared per-pod
/// state database, or an in-memory repository for tests. Public because the
/// parity/black-box suites construct repositories explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    /// The shared per-pod database (`<state_root>/state.sqlite`).
    Repo,
    /// An in-memory database for tests when `Inbox` is built without a real
    /// state root path (or with the `fake` test flag).
    InMemory,
}

/// The root of a run's inbox. A real repo resolves the SHARED agent-run
/// state root; the run's inbox dir ([`dir`]) is kept as the shared identity
/// the injection processes key on, but the store is the database.
///
/// This is the stream-3 C2 `InboxStore` of DESIGN §3.3, built over the
/// existing migration-3 tables plus the migration-7 `a2a_audit`. The shorter
/// name [`Inbox`] remains as the compatibility alias so every existing caller
/// (worker sink, orchestrator ladder, injection channels) compiles unchanged.
#[derive(Debug, Clone)]
pub struct InboxStore {
    /// The run's inbox dir (`<state_root>/a2a-inbox/<run_id>/`), the identity
    /// both sides key on (`KUBEADP_A2A_INBOX_DIR`).
    dir: PathBuf,
    /// The resolved state root of the repo, or `None` for in-memory tests.
    state_root: Option<PathBuf>,
    ttl_secs: u64,
    /// The claim-lease window (seconds): how long a claimed-but-unacked
    /// message stays out of `pending` / claimable again (see
    /// [`CLAIM_LEASE_SECS`]). Mirrored per inbox so tests can shorten it.
    lease: u64,
    fake: bool,
    storage: StorageKind,
}

/// The compatibility name for [`InboxStore`]: every existing caller builds
/// and uses an `Inbox`, which is the same type.
pub type Inbox = InboxStore;

/// The run's inbox DIR for a run whose host keeps its state in the shared
/// `state_root`: `<state_root>/a2a-inbox/<run_id>/`. This is the identity the
/// injection side names in [`INBOX_ENV`] and the HTTP API keys on; the
/// newer storage lives in `state.sqlite`, but this DIRECTORY (which the
/// launcher already creates and grants) is kept so neither side's protocol
/// changes. Both sides resolve it through this one function, so the
/// sanitized run id can never disagree between them.
pub fn inbox_dir_for(state_root: &Path, run_id: &str) -> PathBuf {
    state_root
        .join(INBOX_DIR_NAME)
        .join(sanitize_run_id(run_id))
}

/// The shared root every run's A2A inbox lives under:
/// `<root>/a2a-inbox/<run>/`, where `root` is the agent-run `state_root`
/// resolved EXACTLY as the HTTP server derives it today
/// ([`crate::core::config::Config::load`] then `state_root`:
/// `AGENT_RUN_STATE_ROOT` > the config file's `state_root` > the built-in
/// default `~/.agent-run`, expanded against the workspace root and `~`).
///
/// This is the ONE definition both sides of a run's inbox resolve: the host
/// delivery side (the A2A sink, `orchestrate/a2a.rs` / `http_server.rs`) and
/// the launcher that names the run's inbox in [`INBOX_ENV`]
/// (`orchestrate/session.rs` and `core/runner.rs`; the worker always used the
/// same root). The supervisor session side must NOT derive the inbox from the
/// state file's parent (the default state file lives in `/tmp`, which the
/// sink never writes and the pod wipes at every restart): that was the
/// path-mismatch bug this helper removes -- a session's inbox is now
/// guaranteed to be the directory the sink writes. `INBOX_STATE_ROOT_ENV` is
/// kept as the documented variable the resolution honours.
pub fn inbox_state_root() -> PathBuf {
    crate::core::config::Config::load()
        .map(|c| c.state_root)
        .unwrap_or_else(|_| crate::core::config::Config::default().state_root)
}

/// Reduces a run id to a safe single path segment, collision-free for every
/// HONEST id (N3): a run id that is already a safe segment (ASCII
/// alphanumerics, `-`, `_`, at most 128 chars) passes verbatim, so two
/// generated job ids (8 hex chars) or session names can never collapse into
/// the same key. An id containing any other character (`a.b`, separators, a
/// hostile value) is hex-encoded (a hostile id can never form a `..`
/// segment), over-long ones are hashed instead, so the segment stays
/// bounded. The identity is only ever used as a row key and a path segment;
/// both sides derive it through this one function, so the sanitized run id
/// can never disagree between them.
fn sanitize_run_id(run_id: &str) -> String {
    if run_id.is_empty() {
        return "unknown".to_string();
    }
    if run_id.len() <= 128
        && run_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return run_id.to_string();
    }
    let encoded = hex_encode(run_id.as_bytes());
    if encoded.len() <= 128 {
        encoded
    } else {
        hex_encode(&crate::core::crypto::sha256(run_id.as_bytes()))
    }
}

/// Lowercase hex encoding of `bytes`.
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Derives the run id from an inbox dir: the last path segment. The
/// injection side sends its `KUBEADP_A2A_INBOX_DIR` value verbatim as the run
/// key; the host sanitizes it again before any database write, so a hostile
/// value can only ever select (or fail to select) a row set, never a path.
pub(crate) fn run_id_from_dir(dir: &Path) -> String {
    dir.file_name()
        .and_then(|n| n.to_str())
        .map(sanitize_run_id)
        .unwrap_or_else(|| "unknown".to_string())
}

/// The run ids that still hold uninjected messages in the shared database
/// under `state_root`. The delivery sinks call this once at startup: the
/// ladder watchers are in-memory threads, so after a pod restart the messages
/// queued before it would otherwise sit unwatched (never escalated, never
/// expired) until another message happens to arrive for the same run.
pub fn runs_with_uninjected(state_root: &Path) -> Vec<String> {
    let Ok(conn) = open_db(state_root) else {
        return Vec::new();
    };
    let Ok(mut stmt) = conn.prepare(
        "SELECT DISTINCT run_id FROM a2a_message WHERE fake=0 AND injected_at IS NULL ORDER BY run_id",
    ) else {
        return Vec::new();
    };
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map(|r| r.flatten().collect::<Vec<_>>())
        .unwrap_or_default();
    rows
}

/// Removes, for EVERY run under `state_root`, the acknowledged rows and the
/// expiry records older than [`INJECTED_RETENTION_SECS`]. Nothing else ever
/// deletes the rows of a run that receives no further message (a finished or
/// cleaned job), so the host calls this at startup. Best effort; returns the
/// number of rows removed.
pub fn sweep_retention(state_root: &Path) -> usize {
    let Ok(conn) = open_db(state_root) else {
        return 0;
    };
    let now = now_unix();
    let messages = conn
        .execute(
            "DELETE FROM a2a_message WHERE fake=0 AND injected_at IS NOT NULL AND (?1 - injected_at) >= ?2",
            rusqlite::params![now, INJECTED_RETENTION_SECS],
        )
        .unwrap_or(0);
    let expiries = conn
        .execute(
            "DELETE FROM a2a_expiry WHERE fake=0 AND (?1 - expired_at) >= ?2",
            rusqlite::params![now, INJECTED_RETENTION_SECS],
        )
        .unwrap_or(0);
    messages + expiries
}

/// Performs one **purge pass** for the pod's state root (SPEC §7.15):
///
/// - sets `framed` to `NULL` on injected `a2a_message` rows whose injection is
///   at least [`TEXT_NULL_AFTER_INJECTED_SECS`] (60 s) old -- the text is then
///   gone from the box; the row (and its exactly-once key) stays,
/// - deletes `a2a_audit` rows older than [`PURGE_RETENTION_SECS`] (14 days),
/// - deletes text-less `a2a_message` rows (the injected ones whose text was
///   nulled) and text-less `a2a_expiry` rows older than 14 days.
///
/// Runs at startup and then at least hourly ([`start_purge_task`]). Idempotent:
/// a second pass with the same `now` removes nothing. Best effort; returns the
/// number of rows changed (for tests and the startup log).
pub fn purge(state_root: &Path) -> usize {
    let Ok(conn) = open_db(state_root) else {
        return 0;
    };
    let now = now_unix();
    purge_at(&conn, now).unwrap_or(0)
}

/// The guts of [`purge`] with an injectable clock, so tests can drive the 60 s
/// text-null and the 14-day row deletion deterministically.
pub fn purge_at(conn: &Connection, now: u64) -> Result<usize, String> {
    let mut changed = 0usize;
    // 1. Null the text of injected rows past the 60 s window.
    changed += conn
        .execute(
            "UPDATE a2a_message
                SET framed=NULL
              WHERE fake=0 AND injected_at IS NOT NULL
                AND framed IS NOT NULL
                AND (?1 - injected_at) >= ?2",
            rusqlite::params![now as i64, TEXT_NULL_AFTER_INJECTED_SECS as i64],
        )
        .map_err(|e| format!("failed to null the A2A message text: {e}"))?;
    // 2. Delete expired audit rows (14-day retention).
    changed += crate::core::a2a::audit::AuditStore::purge_at(conn, now)?;
    // 3. Delete the text-less inbox rows past 14 days: injected rows whose
    //    text was nulled, and expiry records.
    changed += conn
        .execute(
            "DELETE FROM a2a_message
              WHERE fake=0 AND framed IS NULL AND injected_at IS NOT NULL
                AND (?1 - injected_at) >= ?2",
            rusqlite::params![now as i64, PURGE_RETENTION_SECS as i64],
        )
        .map_err(|e| format!("failed to purge the text-less A2A messages: {e}"))?;
    changed += conn
        .execute(
            "DELETE FROM a2a_expiry WHERE fake=0 AND (?1 - expired_at) >= ?2",
            rusqlite::params![now as i64, PURGE_RETENTION_SECS as i64],
        )
        .map_err(|e| format!("failed to purge the A2A expiry records: {e}"))?;
    Ok(changed)
}

/// The hourly purge task: runs one [`purge`] pass at startup and then at least
/// once per hour for the pod's state root, forever. The host (worker entry and
/// orchestrator entry) spawns this once at startup on a background thread.
pub fn start_purge_task(state_root: PathBuf) {
    std::thread::spawn(move || loop {
        let _ = purge(&state_root);
        std::thread::sleep(std::time::Duration::from_secs(3600));
    });
}

/// Expires and prunes every uninjected message whose TTL passed, for every run
/// in the pod's inbox (a startup + periodic sweep; the per-message `expire`
/// path audits each one). Mirrors the ladder's per-run expire but runs for ALL
/// runs so a run with no watcher cannot grow unbounded. Returns the number of
/// messages expired. Best effort, like the other sweeps.
pub fn expire_due(state_root: &Path, ttl_secs: u64) -> usize {
    let Ok(conn) = open_db(state_root) else {
        return 0;
    };
    let now = now_unix();
    let mut expired = 0usize;
    let Ok(mut stmt) = conn.prepare(
        "SELECT DISTINCT run_id FROM a2a_message
         WHERE fake=0 AND injected_at IS NULL AND (?1 - queued_at) >= ?2 ORDER BY run_id",
    ) else {
        return 0;
    };
    let runs: Vec<String> = stmt
        .query_map(rusqlite::params![now, ttl_secs], |r| r.get(0))
        .map(|r| r.flatten().collect())
        .unwrap_or_default();
    for run in runs {
        // The inbox with the real TTL (NOT `u64::MAX`, which the ladder uses
        // to apply the TTL itself): `pending_in` then filters to the expired
        // rows only, so the per-message `expire` audits exactly the lapsed
        // messages and never touches younger ones.
        let inbox = Inbox::with_ttl(inbox_dir_for(state_root, &run), ttl_secs);
        for msg in inbox.pending_in_expired() {
            if inbox.expire(&msg.message_id, "ttl") {
                expired += 1;
            }
        }
    }
    expired
}

/// Opens a connection to the shared state database (WAL + migrations).
fn open_db(state_root: &Path) -> Result<Connection, String> {
    crate::core::job::open_state_db(state_root)
        .map_err(|e| format!("failed to open the A2A state database: {e:#}"))
}

/// Builds an [`Inbox`] rooted at an inbox dir, resolving the real shared state
/// root. When `dir` is not under `a2a-inbox/` (or the root cannot be
/// resolved), an in-memory repository is used (tests keep working unchanged;
/// the real delivery paths always pass a `state_root/a2a-inbox/<run>` path).
fn inbox_at_dir(dir: impl Into<PathBuf>, ttl_secs: u64) -> Inbox {
    let dir = dir.into();
    let state_root = state_root_from_dir(&dir);
    match state_root {
        Some(root) => Inbox {
            dir,
            state_root: Some(root),
            ttl_secs,
            lease: CLAIM_LEASE_SECS,
            fake: false,
            storage: StorageKind::Repo,
        },
        None => Inbox {
            dir,
            state_root: None,
            // Tests build inboxes at arbitrary temp paths; those use the
            // in-memory repository (a real repo needs the shared state root).
            ttl_secs,
            lease: CLAIM_LEASE_SECS,
            fake: true,
            storage: StorageKind::InMemory,
        },
    }
}

/// Resolves the shared state root containing an inbox dir. Accepts only the
/// canonical layout `<state_root>/a2a-inbox/<run>`; under any other layout no
/// repo exists and the inbox falls back to the in-memory repository.
fn state_root_from_dir(dir: &Path) -> Option<PathBuf> {
    if dir
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        != Some(INBOX_DIR_NAME)
    {
        return None;
    }
    dir.parent()?.parent().map(Path::to_path_buf)
}

impl InboxStore {
    /// An inbox rooted at `dir` with the default TTL. Resolves the shared
    /// state root when `dir` has the canonical `<root>/a2a-inbox/<run>` shape;
    /// otherwise an in-memory repository (tests).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        inbox_at_dir(dir, DEFAULT_TTL_SECS)
    }

    /// An inbox rooted at `dir` with an explicit TTL (seconds).
    pub fn with_ttl(dir: impl Into<PathBuf>, ttl_secs: u64) -> Self {
        inbox_at_dir(dir, ttl_secs)
    }

    /// An inbox rooted at `dir` with an explicit claim-lease window (seconds),
    /// for tests that shorten the lease.
    pub fn with_lease(dir: impl Into<PathBuf>, lease_secs: u64) -> Self {
        let mut inbox = inbox_at_dir(dir, DEFAULT_TTL_SECS);
        inbox.lease = lease_secs;
        inbox
    }

    /// The inbox named by [`INBOX_ENV`] (set for the run by its launcher), or
    /// `None` when the variable is absent or empty: the injecting processes
    /// then have no receive side.
    pub fn from_env() -> Option<Self> {
        let dir = std::env::var(INBOX_ENV).ok()?;
        let trimmed = dir.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(Self::new(PathBuf::from(trimmed)))
    }

    /// The inbox directory (also the run key the HTTP API identifies the run
    /// by).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The storage class of this inbox.
    pub fn storage(&self) -> StorageKind {
        self.storage
    }

    /// The run id (sanitized last path segment).
    pub fn run_id(&self) -> String {
        run_id_from_dir(&self.dir)
    }

    /// Whether this inbox uses the in-memory store (a test inbox, `fake`).
    /// Not part of the public surface; the crate's own modules use it to
    /// run the same SQL against the shared in-memory store.
    pub fn fake(&self) -> bool {
        self.fake
    }

    /// The resolved state root, if any.
    pub fn state_root(&self) -> Option<&Path> {
        self.state_root.as_deref()
    }

    /// The TTL (seconds) for uninjected messages in this inbox.
    pub fn ttl_secs(&self) -> u64 {
        self.ttl_secs
    }

    /// The claim lease window (seconds) for this inbox.
    pub fn lease_secs(&self) -> u64 {
        self.lease
    }

    /// The dedupe key of a message: (run id, message id). Injected ids are
    /// deduplicated through the same key (the row's own `injected_at`).
    fn key(&self, message_id: &str) -> (String, String) {
        (self.run_id(), message_id.to_string())
    }

    /// Whether this inbox's store is the in-memory repository of the test
    /// suites (see [`StorageKind::InMemory`]).
    fn is_memory(&self) -> bool {
        self.storage == StorageKind::InMemory
    }

    /// Runs `f` with a connection to this inbox's store. For the in-memory
    /// repository, a SHARED connection per inbox dir is used (keyed by the
    /// dir path) so two handles on the same run share one store, mirroring
    /// the real shared database; the per-dir lock serializes access exactly
    /// like SQLite's own locking.
    pub fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, String>,
    ) -> Result<T, String> {
        if self.is_memory() {
            let shared = crate::core::a2a::mem::shared_conn(&self.dir)
                .ok_or_else(|| "failed to create the in-memory A2A store".to_string())?;
            let conn = shared.lock().unwrap_or_else(|e| e.into_inner());
            f(&conn)
        } else {
            let root = self
                .state_root
                .as_deref()
                .ok_or_else(|| "in-memory inbox has no state root".to_string())?;
            let conn = open_db(root)?;
            f(&conn)
        }
    }

    /// The audit store on the same state root as this inbox. For the
    /// in-memory test repo it writes to the shared in-memory `a2a_audit`
    /// table (mirrored in [`crate::core::a2a::mem`]).
    pub fn audit(&self) -> Option<crate::core::a2a::audit::AuditStore> {
        let root = self.state_root.clone()?;
        Some(crate::core::a2a::audit::AuditStore::new(root))
    }

    /// Records a metadata-only audit row for this run (SPEC §7.15), best
    /// effort: an audit failure must never fail message delivery, so errors
    /// are logged and swallowed. Never writes message text.
    fn record_audit(&self, msg: &QueuedMessage) {
        let Some(audit) = self.audit() else {
            return;
        };
        let _ = audit.record(AuditRow {
            run_id: self.run_id(),
            message_id: msg.message_id.clone(),
            from_agent_id: msg.sender.clone(),
            to_agent_id: crate::core::jobset::local_agent_id(),
            jobset_id: None,
            message_size: msg.framed.len() as i64,
            channel: None,
            delivered_at: Some(msg.queued_at),
            injected_at: None,
            outcome: outcome::QUEUED.to_string(),
            timestamp: msg.queued_at,
        });
    }

    /// The audit store for a state root, used by the purge and by tests.
    pub fn audit_store(state_root: &Path) -> crate::core::a2a::audit::AuditStore {
        crate::core::a2a::audit::AuditStore::new(state_root)
    }

    /// Appends `msg` to the inbox. A message id that is already queued or
    /// already injected is a no-op ([`EnqueueOutcome::Duplicate`]); the queue
    /// is pruned (injected + expired rows) and bounded (oldest dropped, a
    /// durable note recorded). The host calls this; the injecting side never
    /// does.
    pub fn enqueue(&self, msg: QueuedMessage) -> Result<EnqueueOutcome, String> {
        let (run_id, message_id) = self.key(&msg.message_id);
        let mut dropped = Vec::new();
        let outcome = self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE")
                .map_err(|e| format!("failed to begin enqueue transaction: {e}"))?;
            let outcome = (|| -> Result<EnqueueOutcome, String> {
                // A message id already present -- queued OR injected -- is a
                // no-op (the row is the durable dedupe store).
                let n: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM a2a_message WHERE run_id=?1 AND message_id=?2 AND fake=?3",
                        rusqlite::params![run_id, message_id, i64::from(self.fake)],
                        |row| row.get(0),
                    )
                    .map_err(|e| format!("failed to read the A2A inbox: {e}"))?;
                if n > 0 {
                    return Ok(EnqueueOutcome::Duplicate);
                }
                // Insert the new row. A duplicate PK (a concurrent sender won)
                // is a no-op for the loser.
                let inserted = conn
                    .execute(
                        "INSERT INTO a2a_message
                            (run_id, message_id, fake, sender, hop, context_id, task_id, framed, queued_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        rusqlite::params![
                            run_id,
                            message_id,
                            i64::from(self.fake),
                            &msg.sender,
                            msg.hop,
                            &msg.context_id,
                            &msg.task_id,
                            &msg.framed,
                            msg.queued_at,
                        ],
                    )
                    .map_err(|e| format!("failed to enqueue the A2A message: {e}"))?;
                if inserted == 0 {
                    return Ok(EnqueueOutcome::Duplicate);
                }
                self.prune_and_bound(conn, msg.queued_at, &mut dropped)?;
                Ok(EnqueueOutcome::Stored)
            })();
            match outcome {
                Ok(o) => {
                    conn.execute_batch("COMMIT")
                        .map_err(|e| format!("failed to commit enqueue transaction: {e}"))?;
                    Ok(o)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e)
                }
            }
        });
        // Audit (SPEC §7.15) AFTER the transaction commits: the audit store
        // opens its own connection, so it must never run while this inbox
        // holds the database write lock. Best effort.
        if outcome == Ok(EnqueueOutcome::Stored) {
            self.record_audit(&msg);
        }
        if !dropped.is_empty() {
            self.record_dropped_audits(dropped);
        }
        outcome
    }

    /// Records the `dropped` audit rows for messages the bounded queue
    /// removed, best effort.
    fn record_dropped_audits(&self, dropped: Vec<QueuedMessage>) {
        let Some(audit) = self.audit() else {
            return;
        };
        let to = crate::core::jobset::local_agent_id();
        for m in dropped {
            let _ = audit.record(AuditRow {
                run_id: self.run_id(),
                message_id: m.message_id.clone(),
                from_agent_id: m.sender.clone(),
                to_agent_id: to.clone(),
                jobset_id: None,
                message_size: m.framed.len() as i64,
                channel: None,
                delivered_at: Some(m.queued_at),
                injected_at: None,
                outcome: outcome::DROPPED.to_string(),
                timestamp: now_unix(),
            });
        }
    }

    /// Prunes (removes expired uninjected rows) and enforces the bounded
    /// queue inside the caller's transaction.
    ///
    /// Injected rows are KEPT: they are the acknowledged history and the
    /// durable dedupe store (a re-sent id is a no-op for the whole run),
    /// exactly like the phase-1 injection markers. Only expired uninjected
    /// rows are removed. Overflows record a durable note as an ordinary
    /// `a2a_message` row (sender [`NOTE_SENDER`], newest `queued_at`), which
    /// `pending`/`drain` serve and an injection consumes like any message. Any
    /// dropped (bounded-queue) messages are pushed into `dropped` so the
    /// caller can audit them (SPEC §7.15); their text is deleted at once.
    fn prune_and_bound(
        &self,
        conn: &Connection,
        now: u64,
        dropped: &mut Vec<QueuedMessage>,
    ) -> Result<(), String> {
        let (run_id,) = (self.run_id(),);
        conn.execute(
            "DELETE FROM a2a_message WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL AND (?3 - queued_at) >= ?4",
            rusqlite::params![run_id, i64::from(self.fake), now, self.ttl_secs],
        )
        .map_err(|e| format!("failed to prune the A2A inbox: {e}"))?;
        // Acknowledged rows are the dedupe store only for the retention
        // window: without this prune a long-lived run's injected history
        // grows without bound.
        conn.execute(
            "DELETE FROM a2a_message WHERE run_id=?1 AND fake=?2 AND injected_at IS NOT NULL AND (?3 - injected_at) >= ?4",
            rusqlite::params![
                run_id,
                i64::from(self.fake),
                now_unix(),
                INJECTED_RETENTION_SECS
            ],
        )
        .map_err(|e| format!("failed to prune the acknowledged A2A messages: {e}"))?;

        // Bounded queue: drop the oldest queued rows until the bounds hold.
        // Never drop the last remaining message. The drop is visible to the
        // injecting side as one durable note ([`DROPPED_NOTE`]); the note
        // counts toward the bound (it reserves one slot of the count and byte
        // budget once a drop happened), exactly like the phase-1 note message.
        // Every dropped message is collected into `dropped` for the audit row
        // (SPEC §7.15); its text is deleted at once.
        let mut dropped_any = false;
        loop {
            let queued: (i64, i64) = conn
                .query_row(
                    "SELECT COUNT(*), COALESCE(SUM(LENGTH(CAST(framed AS BLOB))), 0)
                     FROM a2a_message WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL",
                    rusqlite::params![run_id, i64::from(self.fake)],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|e| format!("failed to count the A2A queue: {e}"))?;
            let count = queued.0 as usize;
            let bytes = queued.1 as usize;
            let note_count = if dropped_any { 1usize } else { 0usize };
            let note_len = DROPPED_NOTE.len();
            if count + note_count <= MAX_QUEUED_MSGS && bytes + note_len <= MAX_QUEUED_BYTES {
                break;
            }
            let oldest: Option<(String, String, Option<String>)> = conn
                .query_row(
                    "SELECT message_id, sender, framed FROM a2a_message
                     WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL
                     ORDER BY queued_at, message_id LIMIT 1",
                    rusqlite::params![run_id, i64::from(self.fake)],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .ok();
            let Some((oldest_msg_id, oldest_sender, oldest_framed)) = oldest else {
                break;
            };
            // Keep at least one message even if it overflows the bound.
            let remaining: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM a2a_message
                     WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL AND message_id <> ?3",
                    rusqlite::params![run_id, i64::from(self.fake), oldest_msg_id],
                    |r| r.get(0),
                )
                .map_err(|e| format!("failed to count the A2A queue: {e}"))?;
            if remaining == 0 {
                break;
            }
            conn.execute(
                "DELETE FROM a2a_message WHERE run_id=?1 AND fake=?2 AND message_id=?3",
                rusqlite::params![run_id, i64::from(self.fake), oldest_msg_id],
            )
            .map_err(|e| format!("failed to drop the oldest A2A message: {e}"))?;
            // The text is deleted at once (the row is gone) and the metadata
            // is collected for the audit row.
            dropped.push(QueuedMessage {
                message_id: oldest_msg_id,
                sender: oldest_sender,
                hop: 0,
                context_id: None,
                task_id: None,
                framed: oldest_framed.unwrap_or_default(),
                queued_at: now,
            });
            dropped_any = true;
        }
        if dropped_any {
            // Record the durable note as an ordinary queued message with a
            // fresh id (never colliding with agent traffic). It has the
            // newest queued_at, so later overflows drop real messages first.
            conn.execute(
                "INSERT INTO a2a_message (run_id, message_id, fake, sender, hop, framed, queued_at)
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
                rusqlite::params![
                    run_id,
                    note_message_id(),
                    i64::from(self.fake),
                    NOTE_SENDER,
                    DROPPED_NOTE,
                    now_unix(),
                ],
            )
            .map_err(|e| format!("failed to record the A2A drop note: {e}"))?;
        }
        Ok(())
    }

    /// The queued (not yet injected, not expired) messages, oldest first.
    /// Read-only. Expired and already-injected messages are filtered here and
    /// removed by the host's next enqueue (the row state is the filter).
    pub fn pending(&self) -> Vec<QueuedMessage> {
        self.with_conn(|conn| Ok(self.pending_in(conn)))
            .unwrap_or_default()
    }

    fn pending_in(&self, conn: &Connection) -> Vec<QueuedMessage> {
        let now = now_unix();
        let (run_id,) = (self.run_id(),);
        // SQLite integers are signed 64-bit; a caller that disables the TTL
        // with `u64::MAX` (the ladder) must not overflow the bound value.
        let ttl = self.ttl_secs.min(i64::MAX as u64) as i64;
        // A message claimed (leased) by the sandboxed receive side within the
        // lease window is NOT pending: the claimer holds it for delivery (and
        // acknowledgement), so neither the host ladder nor another channel may
        // serve it. A lease that has lapsed (the claimer crashed without
        // acknowledging) returns the message to the queue.
        let mut stmt = match conn.prepare(
            "SELECT message_id, sender, hop, context_id, task_id, framed, queued_at
             FROM a2a_message
             WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL
               AND (?3 - queued_at) < ?4
               AND (claimed_at IS NULL OR (?3 - claimed_at) >= ?5)
             ORDER BY queued_at, message_id",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt
            .query_map(
                rusqlite::params![run_id, i64::from(self.fake), now, ttl, self.lease as i64],
                |row| {
                    Ok(QueuedMessage {
                        message_id: row.get(0)?,
                        sender: row.get(1)?,
                        hop: row.get::<_, i64>(2)? as u32,
                        context_id: row.get(3)?,
                        task_id: row.get(4)?,
                        framed: row.get(5)?,
                        queued_at: row.get(6)?,
                    })
                },
            )
            .map(|r| r.collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        rows
    }

    /// The messages whose TTL passed and that are still uninjected (the
    /// exact complement of [`InboxStore::pending`]): the rows this inbox
    /// would serve were it not for its own TTL. The expiry sweep uses it so
    /// the per-message [`InboxStore::expire`] audits exactly the lapsed
    /// messages. Claimed (leased) rows are included -- a claim that lapsed
    /// past the TTL is equally due. Oldest first.
    pub fn pending_in_expired(&self) -> Vec<QueuedMessage> {
        self.with_conn(|conn| {
            let now = now_unix();
            let (run_id,) = (self.run_id(),);
            let ttl = self.ttl_secs.min(i64::MAX as u64) as i64;
            let mut stmt = conn
                .prepare(
                    "SELECT message_id, sender, hop, context_id, task_id, framed, queued_at
                     FROM a2a_message
                     WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL
                       AND (?3 - queued_at) >= ?4
                     ORDER BY queued_at, message_id",
                )
                .map_err(|e| format!("failed to prepare the expired-message query: {e}"))?;
            let rows = stmt
                .query_map(
                    rusqlite::params![run_id, i64::from(self.fake), now, ttl],
                    |row| {
                        Ok(QueuedMessage {
                            message_id: row.get(0)?,
                            sender: row.get(1)?,
                            hop: row.get::<_, i64>(2)? as u32,
                            context_id: row.get(3)?,
                            task_id: row.get(4)?,
                            framed: row.get(5)?,
                            queued_at: row.get(6)?,
                        })
                    },
                )
                .map_err(|e| format!("failed to query the expired messages: {e}"))?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .unwrap_or_default()
    }

    /// The number of pending (queued, uninjected, unexpired) messages.
    pub fn pending_count(&self) -> usize {
        self.pending().len()
    }

    /// Marks `message_id` injected through `channel`: updates the row's
    /// `injected_at`/`channel`. Returns `true` when THIS call won the
    /// injection; `false` when the message was already injected (or is not
    /// queued). Exactly once across all channels: the atomic
    /// `injected_at IS NULL` guarded UPDATE means two concurrent channels
    /// never both win.
    pub fn mark_injected(&self, message_id: &str, channel: &str) -> bool {
        let (run_id, mid) = self.key(message_id);
        let now = now_unix();
        let won = self
            .with_conn(|conn| {
                let n = conn
                    .execute(
                        "UPDATE a2a_message SET injected_at=?1, channel=?2
                         WHERE run_id=?3 AND message_id=?4 AND fake=?5 AND injected_at IS NULL",
                        rusqlite::params![now, channel, run_id, mid, i64::from(self.fake)],
                    )
                    .map_err(|e| format!("failed to mark the A2A message injected: {e}"))?;
                Ok(n > 0)
            })
            .unwrap_or(false);
        if won {
            // Audit the injection (SPEC §7.15), best effort, after the row is
            // committed. Only the metadata of the acknowledged message: the
            // audit row carries the channel and timestamps, never the text.
            if let Some(audit) = self.audit() {
                let size = self
                    .with_conn(|conn| {
                        conn.query_row::<Option<String>, _, _>(
                            "SELECT framed FROM a2a_message WHERE run_id=?1 AND message_id=?2 AND fake=?3",
                            rusqlite::params![run_id, mid, i64::from(self.fake)],
                            |row| row.get(0),
                        )
                        .map_err(|e| e.to_string())
                        .map(|s| s.map(|t| t.len() as i64).unwrap_or(0))
                    })
                    .unwrap_or(0);
                let _ = audit.record(AuditRow {
                    run_id: self.run_id(),
                    message_id: message_id.to_string(),
                    from_agent_id: String::new(),
                    to_agent_id: crate::core::jobset::local_agent_id(),
                    jobset_id: None,
                    message_size: size,
                    channel: Some(channel.to_string()),
                    delivered_at: None,
                    injected_at: Some(now),
                    outcome: outcome::INJECTED.to_string(),
                    timestamp: now,
                });
            }
        }
        won
    }

    /// Atomically claims the oldest pending messages for a HOST-side resume
    /// (the escalation ladder's `idle_turn`/`interrupt` escalation): marks
    /// each claimed row `claimed_at` now, so exactly one concurrent escalation
    /// wins each message. Returns the messages THIS caller won, oldest first.
    /// A claim holds the message out of `pending` / out of every other
    /// channel for the lease window ([`CLAIM_LEASE_SECS`]); the winner must
    /// then either acknowledge it ([`Inbox::mark_injected`], the permanent
    /// inject) or release it ([`Inbox::release_claim`]) so the next escalation
    /// can retry. Without the atomic claim two concurrent ladder passes would
    /// both read the same pending message, both resume the job, and claim TWO
    /// attempts for ONE message.
    pub fn claim_for_resume(&self) -> Vec<QueuedMessage> {
        let run_id = self.run_id();
        let now = now_unix();
        let ttl = self.ttl_secs.min(i64::MAX as u64) as i64;
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE")
                .map_err(|e| format!("failed to begin the resume-claim transaction: {e}"))?;
            let outcome = (|| -> Result<Vec<QueuedMessage>, String> {
                let mut stmt = conn
                    .prepare(
                        "SELECT message_id, sender, hop, context_id, task_id, framed, queued_at
                         FROM a2a_message
                         WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL
                           AND (?3 - queued_at) < ?4
                           AND (claimed_at IS NULL OR (?3 - claimed_at) >= ?5)
                         ORDER BY queued_at, message_id",
                    )
                    .map_err(|e| format!("failed to prepare the resume-claim query: {e}"))?;
                let rows = stmt
                    .query_map(
                        rusqlite::params![
                            run_id,
                            i64::from(self.fake),
                            now,
                            ttl,
                            self.lease as i64
                        ],
                        |row| {
                            Ok(QueuedMessage {
                                message_id: row.get(0)?,
                                sender: row.get(1)?,
                                hop: row.get::<_, i64>(2)? as u32,
                                context_id: row.get(3)?,
                                task_id: row.get(4)?,
                                framed: row.get(5)?,
                                queued_at: row.get(6)?,
                            })
                        },
                    )
                    .map_err(|e| format!("failed to read the resumable A2A messages: {e}"))?;
                let mut out = Vec::new();
                for row in rows.flatten() {
                    conn.execute(
                        "UPDATE a2a_message SET claimed_at=?1
                         WHERE run_id=?2 AND message_id=?3 AND fake=?4 AND injected_at IS NULL",
                        rusqlite::params![now, run_id, row.message_id, i64::from(self.fake)],
                    )
                    .map_err(|e| format!("failed to claim the A2A message for resume: {e}"))?;
                    out.push(row);
                }
                Ok(out)
            })();
            match outcome {
                Ok(out) => {
                    conn.execute_batch("COMMIT").map_err(|e| {
                        format!("failed to commit the resume-claim transaction: {e}")
                    })?;
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

    /// Releases a resume claim ([`Inbox::claim_for_resume`]): clears the
    /// `claimed_at` marker so the message returns to `pending` immediately
    /// (a later escalation can retry it). Used when the resume could not be
    /// applied; best effort (the lease would release it anyway).
    pub fn release_claim(&self, message_id: &str) {
        let (run_id, mid) = self.key(message_id);
        let _ = self.with_conn(|conn| {
            conn.execute(
                "UPDATE a2a_message SET claimed_at=NULL
                 WHERE run_id=?1 AND message_id=?2 AND fake=?3",
                rusqlite::params![run_id, mid, i64::from(self.fake)],
            )
            .map_err(|e| format!("failed to release the A2A resume claim: {e}"))?;
            Ok(())
        });
    }

    /// The number of messages that are not injected yet, INCLUDING the ones a
    /// channel currently holds under a claim lease (which [`Inbox::pending`]
    /// hides). The ladder watcher keeps running while this is non-zero: a
    /// claimer that crashed between claim and acknowledgement returns its
    /// message to the queue when the lease lapses, and somebody must still
    /// be watching it then. Not TTL-filtered (the ladder applies the TTL).
    pub fn uninjected_count(&self) -> usize {
        let run_id = self.run_id();
        self.with_conn(|conn| {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM a2a_message
                     WHERE run_id=?1 AND fake=?2 AND injected_at IS NULL",
                    rusqlite::params![run_id, i64::from(self.fake)],
                    |row| row.get(0),
                )
                .map_err(|e| format!("failed to count the A2A inbox: {e}"))?;
            Ok(n.max(0) as usize)
        })
        .unwrap_or(0)
    }

    /// Expires one uninjected message: records the durable expiry (the
    /// sender's task fails with `reason`) AND removes the queued row, in one
    /// transaction. Removing the row is what keeps an expired message from
    /// being served later by a hook / `wait_message` (which would deliver a
    /// message whose sender was already told it failed) and from blocking the
    /// younger messages queued behind it. Returns `true` when THIS call
    /// expired the message; `false` when it was injected (or removed) first.
    pub fn expire(&self, message_id: &str, reason: &str) -> bool {
        let (run_id, mid) = self.key(message_id);
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE")
                .map_err(|e| format!("failed to begin the A2A expiry transaction: {e}"))?;
            let outcome = (|| -> Result<bool, String> {
                let removed = conn
                    .execute(
                        "DELETE FROM a2a_message
                         WHERE run_id=?1 AND message_id=?2 AND fake=?3 AND injected_at IS NULL",
                        rusqlite::params![run_id, mid, i64::from(self.fake)],
                    )
                    .map_err(|e| format!("failed to remove the expired A2A message: {e}"))?;
                if removed == 0 {
                    return Ok(false);
                }
                let (expired_at, expired_at_ns) = crate::core::a2a::delivery::now_expiry_pair();
                conn.execute(
                    "INSERT INTO a2a_expiry (run_id, fake, message_id, reason, expired_at, expired_at_ns)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(run_id, message_id)
                     DO UPDATE SET fake=excluded.fake, reason=excluded.reason,
                                   expired_at=excluded.expired_at, expired_at_ns=excluded.expired_at_ns",
                    rusqlite::params![
                        run_id,
                        i64::from(self.fake),
                        mid,
                        reason,
                        expired_at,
                        expired_at_ns
                    ],
                )
                .map_err(|e| format!("failed to record the A2A expiry: {e}"))?;
                Ok(true)
            })();
            match outcome {
                Ok(v) => {
                    conn.execute_batch("COMMIT")
                        .map_err(|e| format!("failed to commit the A2A expiry: {e}"))?;
                    Ok(v)
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(e)
                }
            }
        })
        .unwrap_or(false)
        && {
            // Audit the expiry (SPEC §7.15), best effort, after the row is
            // removed. The text is deleted at once (the row is gone).
            if let Some(audit) = self.audit() {
                let _ = audit.record(AuditRow {
                    run_id: self.run_id(),
                    message_id: message_id.to_string(),
                    from_agent_id: String::new(),
                    to_agent_id: crate::core::jobset::local_agent_id(),
                    jobset_id: None,
                    message_size: 0,
                    channel: None,
                    delivered_at: None,
                    injected_at: None,
                    outcome: outcome::EXPIRED.to_string(),
                    timestamp: now_unix(),
                });
            }
            true
        }
    }

    /// Whether `message_id` has been injected.
    pub fn injected(&self, message_id: &str) -> bool {
        let (run_id, mid) = self.key(message_id);
        self.with_conn(|conn| {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM a2a_message
                     WHERE run_id=?1 AND message_id=?2 AND fake=?3 AND injected_at IS NOT NULL",
                    rusqlite::params![run_id, mid, i64::from(self.fake)],
                    |row| row.get(0),
                )
                .map_err(|e| format!("failed to read the A2A inbox: {e}"))?;
            Ok(count > 0)
        })
        .unwrap_or(false)
    }

    /// Record that `message_id` expired uninjected (the durable `a2a_expiry`
    /// row that fails the sender's task; replaces the phase-2 `expired/`
    /// marker). Best effort.
    pub fn record_expired(&self, message_id: &str, reason: &str) {
        let (run_id, mid) = self.key(message_id);
        let _ = self.with_conn(|conn| {
            let (expired_at, expired_at_ns) =
                crate::core::a2a::delivery::now_expiry_pair();
            conn.execute(
                "INSERT INTO a2a_expiry (run_id, fake, message_id, reason, expired_at, expired_at_ns)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(run_id, message_id)
                 DO UPDATE SET fake=excluded.fake, reason=excluded.reason,
                               expired_at=excluded.expired_at, expired_at_ns=excluded.expired_at_ns",
                rusqlite::params![run_id, i64::from(self.fake), mid, reason, expired_at, expired_at_ns],
            )
            .map_err(|e| format!("failed to record the A2A expiry: {e}"))?;
            Ok(())
        });
    }

    /// The latest durable expiry record for the run, if any. "Latest" is
    /// deterministic: the largest `(expired_at, expired_at_ns)`, and on a
    /// full tie the larger message id (mirrors the phase-2 marker semantics;
    /// rows that cannot be read are skipped).
    pub fn read_expired(&self) -> Option<crate::core::a2a::delivery::ExpiryRecord> {
        let (run_id,) = (self.run_id(),);
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT message_id, reason, expired_at, expired_at_ns
                     FROM a2a_expiry WHERE run_id=?1 AND fake=?2
                     ORDER BY expired_at DESC, expired_at_ns DESC, message_id DESC LIMIT 1",
                )
                .map_err(|e| format!("failed to read the A2A expiry: {e}"))?;
            let record = stmt
                .query_row(rusqlite::params![run_id, i64::from(self.fake)], |row| {
                    Ok(crate::core::a2a::delivery::ExpiryRecord {
                        message_id: row.get(0)?,
                        reason: row.get(1)?,
                        expired_at: row.get(2)?,
                        expired_at_ns: row.get(3)?,
                    })
                })
                .ok();
            Ok(record)
        })
        .unwrap_or(None)
    }

    /// Removes every expiry record for the run (a fresh delivery supersedes
    /// the last failure). Best effort.
    pub fn clear_expired(&self) {
        let (run_id,) = (self.run_id(),);
        let _ = self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM a2a_expiry WHERE run_id=?1 AND fake=?2",
                rusqlite::params![run_id, i64::from(self.fake)],
            )
            .map_err(|e| format!("failed to clear the A2A expiry: {e}"))?;
            Ok(())
        });
    }

    /// Injects every pending message through `channel` (marks each one) and
    /// returns the messages THIS call won, oldest first. This is the
    /// acknowledgement: each returned message has its row marked. Exactly
    /// once: a message another channel claimed or injected first is not
    /// returned.
    pub fn drain(&self, channel: &str) -> Vec<QueuedMessage> {
        self.drain_capped(channel, usize::MAX)
    }

    /// Like [`Inbox::drain`], but stops before the first message that does
    /// not fit into the `max_bytes` budget (counting the framed text plus one
    /// separator between messages) and injects only the messages returned.
    /// Messages beyond the budget stay queued for the next boundary, so the
    /// bounded tool-result injection never drops a message it marked. A
    /// single message at or under the queue byte bound always fits a
    /// reasonable budget.
    pub fn drain_capped(&self, channel: &str, max_bytes: usize) -> Vec<QueuedMessage> {
        let mut out = Vec::new();
        for msg in self.claim_capped(max_bytes) {
            if self.mark_injected(&msg.message_id, channel) {
                out.push(msg);
            }
        }
        out
    }

    /// Claims (leases) the oldest pending messages up to the byte budget,
    /// atomically, WITHOUT acknowledging them (N10, at-least-once receive):
    /// the caller hands the text to a channel first and acknowledges
    /// afterwards. A claimed-but-unacked message stops being claimable for
    /// the claim lease, then returns to the queue, so a crash between claim
    /// and ack re-delivers the message (a duplicate is harmless; the inbox
    /// dedupes the ack by message id).
    pub fn claim_capped(&self, max_bytes: usize) -> Vec<QueuedMessage> {
        let mut out = Vec::new();
        let mut budget = max_bytes;
        let now = now_unix();
        for msg in self.pending() {
            let sep = if out.is_empty() { 0 } else { 2 };
            if msg.framed.len() + sep > budget {
                break;
            }
            budget -= msg.framed.len() + sep;
            if self.claim_one(&msg.message_id, now) {
                out.push(msg);
            }
        }
        out
    }

    /// Leases one message (marks `claimed_at`), exactly once across
    /// concurrent claimers. `true` when this caller won the message.
    fn claim_one(&self, message_id: &str, now: u64) -> bool {
        let (run_id, mid) = self.key(message_id);
        let lease = self.lease.max(1);
        let claimed = self
            .with_conn(|conn| {
                conn.execute(
                    "UPDATE a2a_message SET claimed_at=?1
                     WHERE run_id=?3 AND message_id=?4 AND fake=?5 AND injected_at IS NULL
                       AND (claimed_at IS NULL OR (?1 - claimed_at) >= ?2)",
                    rusqlite::params![now, lease, run_id, mid, i64::from(self.fake)],
                )
                .map_err(|e| format!("failed to claim the A2A message: {e}"))
            })
            .unwrap_or(0);
        claimed > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::audit::AuditStore;
    use rusqlite::params;
    use tempfile::tempdir;

    fn msg(id: &str, text: &str) -> QueuedMessage {
        QueuedMessage {
            message_id: id.to_string(),
            sender: "sender-agent".to_string(),
            hop: 1,
            context_id: Some("ctx-1".to_string()),
            task_id: Some("task-1".to_string()),
            framed: format!("--- A2A INCOMING MESSAGE ---\n{text}"),
            queued_at: now_unix(),
        }
    }

    /// A temp-dir inbox whose store is the real repo (canonical layout).
    fn repo_inbox(tmp: &tempfile::TempDir, run: &str) -> Inbox {
        let root = tmp.path().join("state-root");
        std::fs::create_dir_all(&root).unwrap();
        let dir = inbox_dir_for(&root, run);
        std::fs::create_dir_all(&dir).unwrap();
        Inbox::new(dir)
    }

    fn assert_repo_isolation(a: &Inbox, b: &Inbox) {
        // The in-memory repository is per-inbox; the repo is per-run but the
        // tests must not leak across runs: whatever `a` holds, `b` (a
        // different run in the same shared store) is empty.
        assert_eq!(a.storage(), b.storage());
        assert_eq!(a.run_id().is_empty(), b.run_id().is_empty());
        assert!(
            b.pending().is_empty(),
            "another run must not see our messages"
        );
    }

    #[test]
    fn repo_storage_detection() {
        let tmp = tempdir().unwrap();
        assert_eq!(Inbox::new(tmp.path()).storage(), StorageKind::InMemory);
        let root = tmp.path().join("state-root");
        let dir = inbox_dir_for(&root, "job-1");
        std::fs::create_dir_all(&dir).unwrap();
        let inbox = Inbox::new(dir);
        assert_eq!(inbox.storage(), StorageKind::Repo);
        assert_eq!(inbox.state_root().unwrap(), root);
        assert_eq!(inbox.run_id(), "job-1");
    }

    #[test]
    fn enqueue_pending_roundtrip() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        assert!(inbox.pending().is_empty());
        assert_eq!(
            inbox.enqueue(msg("m1", "hello")).unwrap(),
            EnqueueOutcome::Stored
        );
        let pending = inbox.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message_id, "m1");
        assert_eq!(pending[0].sender, "sender-agent");
        assert!(pending[0].framed.contains("hello"));
        assert!(!inbox.injected("m1"));
    }

    #[test]
    fn enqueue_dedupes_by_message_id() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        assert_eq!(
            inbox.enqueue(msg("m1", "hello")).unwrap(),
            EnqueueOutcome::Stored
        );
        // A sender retry (same id, different content) is a no-op.
        assert_eq!(
            inbox.enqueue(msg("m1", "changed")).unwrap(),
            EnqueueOutcome::Duplicate
        );
        assert_eq!(inbox.pending().len(), 1);
        assert!(
            inbox.pending()[0].framed.contains("hello"),
            "the first copy wins"
        );
        // Injected ids are also never re-queued.
        assert!(inbox.mark_injected("m1", "tool_result"));
        assert_eq!(
            inbox.enqueue(msg("m1", "again")).unwrap(),
            EnqueueOutcome::Duplicate
        );
        assert!(inbox.pending().is_empty());
    }

    #[test]
    fn mark_injected_records_marker_and_is_exactly_once() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        inbox.enqueue(msg("m1", "hello")).unwrap();
        assert!(inbox.mark_injected("m1", "hook"));
        assert!(inbox.injected("m1"));
        assert!(inbox.pending().is_empty());
        // A second injection is a no-op (not the winner).
        assert!(!inbox.mark_injected("m1", "tool_result"));
        // An unknown id never injects.
        assert!(!inbox.mark_injected("never-queued", "hook"));
    }

    #[test]
    fn drain_injects_every_message_exactly_once() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        for i in 0..3 {
            inbox
                .enqueue(msg(&format!("m{i}"), &format!("text {i}")))
                .unwrap();
        }
        let drained = inbox.drain("tool_result");
        assert_eq!(drained.len(), 3);
        assert!(drained.iter().all(|m| m.framed.contains("text")));
        // A second drain returns nothing (the injection is durable).
        assert!(inbox.drain("hook").is_empty());
        assert!(inbox.pending().is_empty());
    }

    #[test]
    fn concurrent_drains_inject_each_message_once() {
        // 4 threads drain the same inbox of 6 messages (under the queue
        // bound): every message is injected by exactly one channel (the
        // atomic row update).
        let tmp = tempdir().unwrap();
        let inbox = std::sync::Arc::new(Inbox::new(tmp.path()));
        for i in 0..6 {
            inbox
                .enqueue(msg(&format!("m{i}"), &format!("t{i}")))
                .unwrap();
        }
        let mut handles = Vec::new();
        for _ in 0..4 {
            let inbox = std::sync::Arc::clone(&inbox);
            handles.push(std::thread::spawn(move || {
                inbox
                    .drain("tool_result")
                    .iter()
                    .map(|m| m.message_id.clone())
                    .collect::<Vec<_>>()
            }));
        }
        let mut all: Vec<String> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 6, "each message must be injected exactly once");
    }

    #[test]
    fn drain_capped_never_marks_what_it_does_not_return() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        for i in 0..4 {
            let mut m = msg(&format!("m{i}"), &format!("text {i}"));
            m.queued_at = now_unix().saturating_sub(10 - i as u64);
            inbox.enqueue(m).unwrap();
        }
        let framed_len = inbox.pending()[0].framed.len();
        let drained = inbox.drain_capped("tool_result", 2 * framed_len + 2);
        assert_eq!(drained.len(), 2);
        assert_eq!(inbox.pending().len(), 2);
        assert!(inbox.injected("m0") && inbox.injected("m1"));
        assert!(!inbox.injected("m2") && !inbox.injected("m3"));
        // The next boundary injects the remainder.
        assert_eq!(inbox.drain_capped("hook", usize::MAX).len(), 2);
    }

    #[test]
    fn queue_is_bounded_with_durable_drop_note() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        for i in 0..20 {
            let mut m = msg(&format!("m{i}"), &format!("text {i}"));
            m.queued_at = now_unix().saturating_sub(100 - i as u64);
            inbox.enqueue(m).unwrap();
        }
        let pending = inbox.pending();
        assert!(
            pending.len() <= MAX_QUEUED_MSGS,
            "the queue must stay within {} entries, got {}",
            MAX_QUEUED_MSGS,
            pending.len()
        );
        assert!(
            !inbox.pending().iter().any(|m| m.message_id == "m0"),
            "the oldest message must have been dropped"
        );
        assert!(
            pending
                .iter()
                .any(|m| m.framed.contains("dropped by the A2A inbox")),
            "overflow must be recorded with a durable note"
        );
        let bytes: usize = pending.iter().map(|m| m.framed.len()).sum();
        assert!(bytes <= MAX_QUEUED_BYTES, "byte bound exceeded: {bytes}");
    }

    #[test]
    fn ttl_expires_uninjected_messages() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::with_ttl(tmp.path(), 10);
        inbox.enqueue(msg("m1", "hello")).unwrap();
        assert_eq!(inbox.pending().len(), 1);
        // Age the message past the TTL by writing it directly (the in-memory
        // table).
        inbox
            .with_conn(|conn| {
                conn.execute(
                    "UPDATE a2a_message SET queued_at=?1 WHERE run_id=?2 AND message_id=?3",
                    params![now_unix().saturating_sub(11), inbox.run_id(), "m1"],
                )
                .map_err(|e| format!("{e}"))?;
                Ok(())
            })
            .unwrap();
        assert!(inbox.pending().is_empty());
        assert!(inbox.pending_count() == 0);
        // The host's next enqueue prunes the expired row.
        inbox.enqueue(msg("m2", "next")).unwrap();
        let gone: i64 = inbox
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM a2a_message WHERE run_id=?1 AND message_id='m1'",
                    params![inbox.run_id()],
                    |r| r.get(0),
                )
                .map_err(|e| format!("{e}"))
            })
            .unwrap();
        assert_eq!(gone, 0, "expired message must be pruned");
        assert_eq!(inbox.pending().len(), 1);
    }

    #[test]
    fn real_run_isolated_from_other_run_in_shared_db() {
        let tmp = tempdir().unwrap();
        let a = repo_inbox(&tmp, "run-a");
        let b = repo_inbox(&tmp, "run-b");
        a.enqueue(msg("m1", "hello a")).unwrap();
        assert_eq!(a.pending().len(), 1);
        // The other run sees nothing (the run_id column separates them).
        assert_eq!(b.pending().len(), 0);
        assert!(!b.injected("m1"));
        assert_repo_isolation(&a, &b);
    }

    #[test]
    fn cross_process_enqueue_and_inject_on_the_real_db() {
        // Two inbox handles on the SAME run and repo (the worker sink path
        // and the hook reader path) operate on one database: enqueue writes
        // and mark_injected reads partition correctly across connections.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let dir = inbox_dir_for(&root, "job-1");
        std::fs::create_dir_all(&dir).unwrap();
        let writer = Inbox::new(dir.clone());
        assert_eq!(writer.storage(), StorageKind::Repo);
        writer.enqueue(msg("m1", "cross")).unwrap();
        // A second handle (same db, e.g. another table in the host) sees it.
        let reader = Inbox::new(dir);
        assert_eq!(reader.storage(), StorageKind::Repo);
        assert_eq!(reader.pending().len(), 1);
        assert!(reader.mark_injected("m1", "hook"));
        assert!(reader.injected("m1"));
        assert!(writer.pending().is_empty());
    }

    #[test]
    fn from_env_reads_the_inbox_variable() {
        crate::core::a2a::testenv::with_env(&[], || {
            assert!(Inbox::from_env().is_none());
        });
        crate::core::a2a::testenv::with_env(&[(INBOX_ENV, Some("/tmp/some-inbox"))], || {
            let inbox = Inbox::from_env().unwrap();
            assert_eq!(inbox.dir(), Path::new("/tmp/some-inbox"));
        });
        crate::core::a2a::testenv::with_env(&[(INBOX_ENV, Some("   "))], || {
            assert!(Inbox::from_env().is_none());
        });
    }

    #[test]
    fn inbox_state_root_follows_agent_run_state_root() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let root_str = root.to_str().unwrap().to_string();
        crate::core::a2a::testenv::with_env(&[("AGENT_RUN_STATE_ROOT", Some(&root_str))], || {
            let resolved = inbox_state_root();
            assert_eq!(
                resolved, root,
                "the shared inbox root must follow AGENT_RUN_STATE_ROOT exactly"
            );
            assert_eq!(
                inbox_dir_for(&resolved, "homelab"),
                root.join("a2a-inbox").join("homelab")
            );
        });
    }

    #[test]
    fn inbox_dir_for_sanitizes_the_run_id() {
        let root = Path::new("/state");
        assert_eq!(
            inbox_dir_for(root, "job-1"),
            PathBuf::from("/state/a2a-inbox/job-1")
        );
        let dir = inbox_dir_for(root, "../../etc");
        assert_eq!(dir, PathBuf::from("/state/a2a-inbox/2e2e2f2e2e2f657463"));
        assert!(!dir.to_string_lossy().contains(".."));
        assert_eq!(inbox_dir_for(root, "ab/cd"), inbox_dir_for(root, "ab/cd"));
    }

    #[test]
    fn message_rows_survive_a_garbled_peer_row() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        inbox.enqueue(msg("m1", "good")).unwrap();
        // A hostile/garbled row (unparseable framed content is fine; the SQL
        // path never parses the body) cannot wedge the inbox: the queue is
        // the row set, and a wrong run id or duplicate never affects m1.
        inbox
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO a2a_message
                        (run_id, message_id, fake, sender, hop, context_id, task_id, framed, queued_at)
                     VALUES ('other-run', 'probe', 1, 'x', 1, NULL, NULL, 'probe', 0)",
                    params![],
                )
                .map_err(|e| format!("{e}"))?;
                Ok(())
            })
            .unwrap();
        let pending = inbox.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message_id, "m1");
    }

    #[test]
    fn repo_messages_survive_a_reopen_and_honour_the_lease() {
        // A restart (a new `Inbox` handle / a reopened shared db) must see the
        // same rows: the per-run inbox is durable in `<state_root>/state.sqlite`,
        // not in a process-resident object.
        let tmp = tempdir().unwrap();
        let dir = inbox_dir_for(&tmp.path().join("state-root"), "run-reopen");
        std::fs::create_dir_all(&dir).unwrap();

        let first = Inbox::new(dir.clone());
        first.enqueue(msg("m1", "before restart")).unwrap();
        first.enqueue(msg("m2", "again")).unwrap();
        assert_eq!(first.pending().len(), 2);

        let second = Inbox::new(dir.clone());
        assert_eq!(second.pending().len(), 2, "rows survive the reopen");
        assert_eq!(second.pending()[0].message_id, "m1");

        // A lease set "before the crash" is likewise durable: reopen 1 sees
        // the messages as leased-and-unpending (a peer lands only past the
        // lease), exactly as it would after the pod restarted mid-hook.
        second
            .with_conn(|conn| {
                let now = now_unix();
                conn.execute(
                    "UPDATE a2a_message SET claimed_at=?1 WHERE run_id=?2 AND message_id='m1'",
                    params![now, second.run_id()],
                )
                .map_err(|e| format!("{e}"))?;
                Ok(())
            })
            .unwrap();
        let third = Inbox::new(dir.clone());
        assert_eq!(third.pending().len(), 1, "the leased row is not pending");
        assert_eq!(third.pending()[0].message_id, "m2");
        // The lease lapses and the leaked message becomes claimable again.
        third
            .with_conn(|conn| {
                conn.execute(
                    "UPDATE a2a_message SET claimed_at=?1 WHERE run_id=?2 AND message_id='m1'",
                    params![
                        now_unix().saturating_sub(CLAIM_LEASE_SECS + 1),
                        third.run_id()
                    ],
                )
                .map_err(|e| format!("{e}"))?;
                Ok(())
            })
            .unwrap();
        let fourth = Inbox::new(dir.clone());
        assert_eq!(fourth.pending().len(), 2, "the lease lapsed, back in queue");
    }

    #[test]
    fn expire_records_the_expiry_and_removes_the_row() {
        let tmp = tempdir().unwrap();
        let inbox = repo_inbox(&tmp, "run-expire");
        inbox.enqueue(msg("m1", "one")).unwrap();
        inbox.enqueue(msg("m2", "two")).unwrap();
        assert!(inbox.expire("m1", "ttl"));
        assert!(!inbox.expire("m1", "ttl"), "a second expiry is a no-op");
        assert_eq!(inbox.read_expired().unwrap().message_id, "m1");
        // The expired row is gone: no channel can serve it any more, and the
        // younger message behind it is still pending.
        let ids: Vec<String> = inbox.pending().into_iter().map(|m| m.message_id).collect();
        assert_eq!(ids, vec!["m2".to_string()]);
        // An injected message can no longer expire.
        assert!(inbox.mark_injected("m2", "hook"));
        assert!(!inbox.expire("m2", "ttl"));
        assert!(inbox.injected("m2"));
    }

    #[test]
    fn uninjected_count_includes_leased_messages() {
        let tmp = tempdir().unwrap();
        let inbox = repo_inbox(&tmp, "run-leased");
        inbox.enqueue(msg("m1", "one")).unwrap();
        assert_eq!(inbox.claim_for_resume().len(), 1);
        assert_eq!(inbox.pending_count(), 0, "leased: not pending");
        assert_eq!(inbox.uninjected_count(), 1, "leased: still uninjected");
        assert!(inbox.mark_injected("m1", "idle_turn"));
        assert_eq!(inbox.uninjected_count(), 0);
    }

    #[test]
    fn runs_with_uninjected_lists_only_runs_with_queued_messages() {
        let tmp = tempdir().unwrap();
        let queued = repo_inbox(&tmp, "run-queued");
        let acked = repo_inbox(&tmp, "run-acked");
        queued.enqueue(msg("m1", "one")).unwrap();
        acked.enqueue(msg("m1", "one")).unwrap();
        assert!(acked.mark_injected("m1", "hook"));
        let root = tmp.path().join("state-root");
        assert_eq!(runs_with_uninjected(&root), vec!["run-queued".to_string()]);
    }

    #[test]
    fn acknowledged_rows_are_pruned_after_the_retention_window() {
        let tmp = tempdir().unwrap();
        let inbox = repo_inbox(&tmp, "run-retention");
        let backdate = |id: &str| {
            inbox
                .with_conn(|conn| {
                    conn.execute(
                        "UPDATE a2a_message SET injected_at=?1 WHERE run_id=?2 AND message_id=?3",
                        params![now_unix() - INJECTED_RETENTION_SECS - 1, inbox.run_id(), id],
                    )
                    .map_err(|e| e.to_string())
                })
                .unwrap();
        };
        inbox.enqueue(msg("m1", "one")).unwrap();
        assert!(inbox.mark_injected("m1", "hook"));
        // Inside the window the acknowledged row is the dedupe store.
        assert_eq!(
            inbox.enqueue(msg("m1", "one")).unwrap(),
            EnqueueOutcome::Duplicate
        );
        backdate("m1");
        // The next enqueue of the run prunes it.
        inbox.enqueue(msg("m2", "two")).unwrap();
        assert!(!inbox.injected("m1"), "past the retention the row is gone");
        // The global sweep does the same for a run that gets no new message.
        assert!(inbox.mark_injected("m2", "hook"));
        backdate("m2");
        let root = tmp.path().join("state-root");
        assert_eq!(sweep_retention(&root), 1);
        assert!(!inbox.injected("m2"));
    }

    #[test]
    fn audit_records_accept_and_inject_rows() {
        // SPEC §7.15 / C2: every accepted message writes a metadata-only
        // `queued` audit row; injecting it adds an `injected` row. Never the
        // message text.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let dir = inbox_dir_for(&root, "run-audit-c2");
        std::fs::create_dir_all(&dir).unwrap();
        let inbox = Inbox::new(dir.clone());
        inbox.enqueue(msg("m1", "hello world")).unwrap();
        let audit = AuditStore::new(root.clone());
        let rows = audit.rows_for_run("run-audit-c2");
        assert_eq!(rows.len(), 1, "one queued row on accept");
        assert_eq!(rows[0].message_id, "m1");
        assert_eq!(rows[0].outcome, outcome::QUEUED);
        assert_eq!(
            rows[0].message_size,
            "--- A2A INCOMING MESSAGE ---\nhello world".len() as i64
        );
        assert!(!rows[0].to_agent_id.is_empty());
        assert!(rows[0].delivered_at.is_some());
        assert!(rows[0].injected_at.is_none());
        assert!(rows[0].channel.is_none());

        assert!(inbox.mark_injected("m1", "hook"));
        let rows = audit.rows_for_run("run-audit-c2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].outcome, outcome::INJECTED);
        assert_eq!(rows[1].channel.as_deref(), Some("hook"));
        assert!(
            rows[1].injected_at.is_some(),
            "injection records the injected-at timestamp"
        );
        assert_eq!(
            rows[1].message_size,
            "--- A2A INCOMING MESSAGE ---\nhello world".len() as i64
        );
        // The audit rows never carry the message text: `AuditRow` has no text
        // field at all, so nothing here can hold the body.
    }

    #[test]
    fn audit_records_expire_and_drop_rows() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let dir = inbox_dir_for(&root, "run-expire-drop");
        std::fs::create_dir_all(&dir).unwrap();
        let inbox = Inbox::new(dir);
        let audit = AuditStore::new(root);

        // Expire: the expired message's audit row appears.
        inbox.enqueue(msg("m1", "one")).unwrap();
        assert!(inbox.expire("m1", "ttl"));
        assert_eq!(
            audit
                .rows_for_run("run-expire-drop")
                .iter()
                .filter(|r| r.outcome == outcome::EXPIRED)
                .count(),
            1
        );

        // Drop: overflow the bounded queue so the oldest is dropped.
        inbox.enqueue(msg("m2", "two")).unwrap();
        for i in 3..(MAX_QUEUED_MSGS + 20) {
            inbox.enqueue(msg(&format!("m{i}"), "padding")).unwrap();
        }
        let rows = audit.rows_for_run("run-expire-drop");
        assert!(
            rows.iter().any(|r| r.outcome == outcome::DROPPED),
            "an overflow must record a dropped audit row"
        );
        // Audit rows are metadata-only: `AuditRow` has no text field.
    }

    #[test]
    fn purge_nulls_injected_text_after_60s_and_stays_idempotent() {
        // SPEC §7.15 / C2 timing: injected text survives < 60 s, is NULL at
        // >= 60 s, and a re-run with the same clock removes nothing more.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let dir = inbox_dir_for(&root, "run-text-null");
        std::fs::create_dir_all(&dir).unwrap();
        let inbox = Inbox::new(dir);
        let audit = AuditStore::new(root.clone());
        inbox.enqueue(msg("m1", "secret body")).unwrap();
        assert!(inbox.mark_injected("m1", "hook"));
        // The test drives the purge clock from the STORED injection time (the
        // one `mark_injected` wrote), so a wall-clock second boundary between
        // the calls can never shift the window.
        let injected_at: u64 = inbox
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT injected_at FROM a2a_message WHERE run_id=?1 AND message_id='m1'",
                    params![inbox.run_id()],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap();

        // Before the 60 s window the text is still present.
        let conn = open_db(&root).unwrap();
        purge_at(&conn, injected_at + 59).unwrap();
        drop(conn);
        let text: Option<String> = inbox
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT framed FROM a2a_message WHERE run_id=?1 AND message_id='m1'",
                    params![inbox.run_id()],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap();
        assert!(text.is_some(), "text survives inside the 60 s window");

        // At >= 60 s the text is NULL; the row (dedupe key) stays.
        let conn = open_db(&root).unwrap();
        purge_at(&conn, injected_at + 60).unwrap();
        drop(conn);
        let text: Option<String> = inbox
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT framed FROM a2a_message WHERE run_id=?1 AND message_id='m1'",
                    params![inbox.run_id()],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap();
        assert_eq!(text, None, "text is NULL past the 60 s window");
        assert!(inbox.injected("m1"), "the dedupe row survives");

        // Idempotent: a second purge with the same `now` changes nothing and
        // the audit row for the injection is untouched.
        let conn = open_db(&root).unwrap();
        let n = purge_at(&conn, injected_at + 60).unwrap();
        drop(conn);
        assert_eq!(n, 0, "a same-clock re-run is a no-op");
        assert_eq!(audit.count(), 2, "the audit rows are not purged at 60 s");
    }

    #[test]
    fn purge_deletes_audit_rows_after_retention() {
        // SPEC §7.15 / C2: audit rows older than AUDIT_RETENTION_SECS are
        // deleted; exactly-at-boundary rows and text are handled by
        // `purge_at` (see audit.rs purge test for the boundary semantics).
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        let dir = inbox_dir_for(&root, "run-audit-purge");
        std::fs::create_dir_all(&dir).unwrap();
        let inbox = Inbox::new(dir);
        let audit = AuditStore::new(root.clone());
        inbox.enqueue(msg("m1", "one")).unwrap();

        // Backdate the queued audit row past the retention, then purge.
        let old = now_unix() - PURGE_RETENTION_SECS - 1;
        inbox
            .with_conn(|conn| {
                conn.execute(
                    "UPDATE a2a_audit SET timestamp=?1, delivered_at=?1
                     WHERE run_id=?2 AND message_id='m1' AND outcome=?3",
                    params![old, inbox.run_id(), outcome::QUEUED],
                )
                .map_err(|e| e.to_string())
            })
            .unwrap();
        assert_eq!(audit.count(), 1);
        let now = now_unix();
        let conn = open_db(&root).unwrap();
        purge_at(&conn, now).unwrap();
        drop(conn);
        assert_eq!(audit.count(), 0, "audit rows past retention are removed");
    }
}

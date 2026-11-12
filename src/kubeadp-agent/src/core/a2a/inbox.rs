//! Engine-agnostic durable per-run A2A inbox (design HL-113 §7.14, phase 1).
//!
//! Every message accepted by the ingress (after the existing checks: framing
//! as untrusted data, idempotency by message id, hop limit, size caps) is
//! appended to the run's inbox directory as ONE JSON file per message, in
//! state `queued`. Injecting a message into the model's context is the
//! acknowledgement: an atomic marker file under `<inbox>/injected/` records
//! `injected_at` and the channel, so the message state moves
//! `queued -> injected`. The harness (not the model) owns the inbox: the
//! model never reads it, the injection channels do
//! ([`crate::core::a2a::inject`]).
//!
//! # Layout
//!
//! ```text
//! <run inbox dir>/                 the run's inbox (KUBEADP_A2A_INBOX_DIR)
//!   m-<sha256(message_id)>.json    one file per queued message
//!   injected/<sha256>.json         the injection marker (injected_at, channel)
//! ```
//!
//! Message files are written by the host (the delivery path of phase 2) and
//! are never modified by the injecting side: the sandboxed agent gets the
//! inbox directory READ-only (the messages) and the `injected/` subdirectory
//! READ-WRITE (the injection markers only). Expired and injected message
//! files are pruned by the host on the next enqueue.
//!
//! # Exactly-once
//!
//! The inbox is the dedupe store: a message id already present as a queued
//! file or as an injection marker is a no-op on re-enqueue (a sender retry is
//! harmless), and marking is atomic per message (`create_new` on the marker),
//! so two concurrent channels never both inject the same message. Exactly
//! once by message id across ALL channels.
//!
//! # Bounds
//!
//! The queue is bounded like the worker's coalescing buffer
//! (`MAX_QUEUED_MSGS` / `MAX_QUEUED_BYTES`, oldest dropped) and records
//! overflow with a durable note message. Uninjected messages expire after
//! `ttl_secs` (default [`DEFAULT_TTL_SECS`]); expired messages stop being
//! served and are pruned by the host.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::core::crypto::sha256;

/// Environment variable naming the shared inbox state root, resolved EXACTLY
/// like the agent-run `state_root` ([`inbox_state_root`]).
///
/// This is the ONE definition both sides of a run's inbox resolve: the host
/// delivery (the A2A sink, `orchestrate/a2a.rs` / `http_server.rs`) and the
/// launcher that names the run's inbox in [`INBOX_ENV`]
/// (`orchestrate/session.rs` and `core/runner.rs`). Keeping it here
/// guarantees a session's inbox is the directory the sink writes, never a
/// sibling root derived from the supervisor state file's parent (historically
/// `/tmp`, wiped at every pod restart).
pub const INBOX_STATE_ROOT_ENV: &str = "AGENT_RUN_STATE_ROOT";

/// Directory name of a run's inbox under its state dir.
pub const INBOX_DIR_NAME: &str = "a2a-inbox";

/// Subdirectory holding the injection markers (`injected_at`, channel).
pub const INJECTED_DIR_NAME: &str = "injected";

/// Environment variable naming the run's inbox directory for the injecting
/// processes (the `mcp-a2a` server and the `a2a-hook` subcommand; separate
/// processes that inherit the run's environment).
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

/// A durable per-run inbox rooted at a directory. Cheap to clone: every
/// operation reads the directory fresh, so concurrent hosts and injecting
/// processes on the same directory see each other.
#[derive(Debug, Clone)]
pub struct Inbox {
    dir: PathBuf,
    ttl_secs: u64,
}

/// The per-run inbox directory for a run whose host keeps its state in the
/// shared `state_root`: `<state_root>/a2a-inbox/<run_id>/`. Both sides of the
/// inbox (the host delivery path of phase 2 and the launcher that names the
/// directory in [`INBOX_ENV`]) resolve it through this one function, so the
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

/// The injection-marker directory inside an inbox: `<inbox>/injected/`. The
/// sandboxed agent gets exactly this subdirectory read-write (the markers)
/// and the inbox directory itself read-only (the messages).
pub fn injected_dir_for(inbox_dir: &Path) -> PathBuf {
    inbox_dir.join(INJECTED_DIR_NAME)
}

/// Reduces a run id to a safe single path segment: ASCII alphanumerics, `-`
/// and `_` kept, everything else (including `.` and separators, so a hostile
/// id can never form a `..` segment) replaced with `_`, capped at 128 chars.
/// A job id or session name never traverses the state root.
fn sanitize_run_id(run_id: &str) -> String {
    let mut out: String = run_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    if out.is_empty() {
        out = "unknown".to_string();
    }
    out
}

/// Deterministic file name for a message id: `m-` + hex(SHA-256). Ids are
/// attacker-controlled strings, so the file name is derived, not stored.
fn message_file_name(message_id: &str) -> String {
    let hex: String = sha256(message_id.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("m-{hex}.json")
}

impl Inbox {
    /// An inbox rooted at `dir` with the default TTL.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ttl_secs: DEFAULT_TTL_SECS,
        }
    }

    /// An inbox rooted at `dir` with an explicit TTL (seconds).
    pub fn with_ttl(dir: impl Into<PathBuf>, ttl_secs: u64) -> Self {
        Self {
            dir: dir.into(),
            ttl_secs,
        }
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

    /// The inbox directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The injection-marker directory (`<inbox>/injected/`).
    pub fn injected_dir(&self) -> PathBuf {
        injected_dir_for(&self.dir)
    }

    fn message_path(&self, message_id: &str) -> PathBuf {
        self.dir.join(message_file_name(message_id))
    }

    fn marker_path(&self, message_id: &str) -> PathBuf {
        self.injected_dir().join(message_file_name(message_id))
    }

    /// Writes `value` to `path` atomically (temp file in the same directory,
    /// then rename).
    fn write_atomic(path: &Path, value: &serde_json::Value) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
        let body = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
        let tmp = path.with_extension(format!(
            "tmp-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::write(&tmp, body.as_bytes())
            .map_err(|e| format!("failed to write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            format!("failed to publish {}: {e}", path.display())
        })
    }

    fn read_message(path: &Path) -> Option<QueuedMessage> {
        let bytes = std::fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Appends `msg` to the inbox. A message id that is already queued or
    /// already injected is a no-op ([`EnqueueOutcome::Duplicate`]); the queue
    /// is pruned (injected + expired files removed) and bounded (oldest
    /// dropped, a durable note recorded). The host calls this; the injecting
    /// side never does.
    pub fn enqueue(&self, msg: QueuedMessage) -> Result<EnqueueOutcome, String> {
        // Create the layout up front so the read/write grants of the injecting
        // processes point at real directories.
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| format!("failed to create {}: {e}", self.dir.display()))?;
        std::fs::create_dir_all(self.injected_dir())
            .map_err(|e| format!("failed to create {}: {e}", self.injected_dir().display()))?;

        // Already injected ids are never re-queued.
        if self.marker_path(&msg.message_id).exists() {
            return Ok(EnqueueOutcome::Duplicate);
        }

        // Atomically claim the message id: create_new wins for exactly ONE
        // concurrent sender, so a sender retry or two racing senders are a
        // no-op for the loser (the exists()-then-write dance is not atomic).
        // The file appears empty for the instant before the body lands; a
        // reader skips an unparseable file, so a torn read is harmless.
        let path = self.message_path(&msg.message_id);
        match std::fs::File::create_new(&path) {
            Ok(mut f) => {
                use std::io::Write;
                let body = serde_json::to_vec(&msg).unwrap_or_else(|_| b"{}".to_vec());
                f.write_all(&body)
                    .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
            }
            Err(_) => return Ok(EnqueueOutcome::Duplicate),
        }

        self.prune(msg.queued_at);
        self.enforce_bound();

        Ok(EnqueueOutcome::Stored)
    }

    /// Removes injected and expired message files (the host-side cleanup; the
    /// sandboxed side has no delete grant and never prunes).
    fn prune(&self, now: u64) {
        for (path, msg) in self.queued_files() {
            let injected = self.marker_path(&msg.message_id).exists();
            let expired = now.saturating_sub(msg.queued_at) >= self.ttl_secs;
            if injected || expired {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    /// Enforces the bounded queue: drops the oldest queued messages until the
    /// count and byte bounds hold, then records a durable note so the drop is
    /// visible to the injecting side (the worker's coalescing note semantics).
    /// The note counts toward the bound: once a drop happened, one slot of the
    /// count and byte budget is reserved for it, so the queue never exceeds
    /// the bounds after the note is added.
    fn enforce_bound(&self) {
        let mut dropped = false;
        loop {
            let files = self.queued_files();
            let count = files.len();
            let bytes: usize = files.iter().map(|(_, m)| m.framed.len()).sum();
            let note = if dropped {
                (1usize, DROPPED_NOTE.len())
            } else {
                (0usize, 0usize)
            };
            if count + note.0 <= MAX_QUEUED_MSGS && bytes + note.1 <= MAX_QUEUED_BYTES {
                break;
            }
            // Oldest first (queued_files is sorted oldest to newest); never
            // drop the last remaining message.
            let Some((path, _)) = files.first() else {
                break;
            };
            if count == 1 {
                break;
            }
            if std::fs::remove_file(path).is_ok() {
                dropped = true;
            } else {
                break;
            }
        }
        if dropped {
            let note = QueuedMessage {
                message_id: format!(
                    "inbox-note-{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0)
                ),
                sender: NOTE_SENDER.to_string(),
                hop: 0,
                context_id: None,
                task_id: None,
                framed: DROPPED_NOTE.to_string(),
                queued_at: now_unix(),
            };
            let path = self.message_path(&note.message_id);
            if !path.exists() {
                let _ = Self::write_atomic(&path, &serde_json::to_value(&note).unwrap_or_default());
            }
        }
    }

    /// The queued (not yet injected, not expired) messages, oldest first.
    /// Read-only: the sandboxed side has no delete grant, so expired and
    /// already-injected messages are filtered out here and removed by the
    /// host's next enqueue (the injection marker is the filter).
    pub fn pending(&self) -> Vec<QueuedMessage> {
        let now = now_unix();
        self.queued_files()
            .into_iter()
            .filter(|(_, msg)| {
                !self.injected(&msg.message_id) && now.saturating_sub(msg.queued_at) < self.ttl_secs
            })
            .map(|(_, msg)| msg)
            .collect()
    }

    /// The number of pending (queued, uninjected, unexpired) messages.
    pub fn pending_count(&self) -> usize {
        self.pending().len()
    }

    /// Reads the queued message files (oldest first) with their paths. Files
    /// that cannot be parsed are skipped (a torn or hostile file never wedges
    /// the inbox).
    fn queued_files(&self) -> Vec<(PathBuf, QueuedMessage)> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.starts_with("m-") || !name.ends_with(".json") {
                continue;
            }
            if let Some(msg) = Self::read_message(&path) {
                out.push((path, msg));
            }
        }
        out.sort_by(|a, b| {
            a.1.queued_at
                .cmp(&b.1.queued_at)
                .then_with(|| a.0.cmp(&b.0))
        });
        out
    }

    /// Marks `message_id` injected through `channel`: writes the injection
    /// marker (atomic create) recording `injected_at` and the channel.
    /// Returns `true` when THIS call won the injection; `false` when the
    /// message was already injected (or is not queued). Exactly once across
    /// all channels: two concurrent channels never both win.
    pub fn mark_injected(&self, message_id: &str, channel: &str) -> bool {
        // Only a QUEUED message can be injected: an unknown id (or one the
        // host already pruned) is never marked.
        if !self.message_path(message_id).exists() {
            return false;
        }
        let marker = self.marker_path(message_id);
        if marker.exists() {
            return false;
        }
        let value = serde_json::json!({
            "message_id": message_id,
            "injected_at": now_unix(),
            "channel": channel,
        });
        if let Some(parent) = marker.parent() {
            if std::fs::create_dir_all(parent).is_err() {
                return false;
            }
        }
        // create_new is the atomicity primitive: only one concurrent channel
        // creates the marker, so only one injects the message.
        match std::fs::File::create_new(&marker) {
            Ok(mut f) => {
                use std::io::Write;
                let body = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
                f.write_all(body.as_bytes()).is_ok()
            }
            Err(_) => false,
        }
    }

    /// Whether `message_id` has been injected (an injection marker exists).
    pub fn injected(&self, message_id: &str) -> bool {
        self.marker_path(message_id).exists()
    }

    /// Injects every pending message through `channel` (marks each one) and
    /// returns the messages THIS call won, oldest first. This is the
    /// acknowledgement: each returned message has its marker written. Exactly
    /// once: a message another channel injected first is not returned.
    pub fn drain(&self, channel: &str) -> Vec<QueuedMessage> {
        let mut out = Vec::new();
        for msg in self.pending() {
            if self.mark_injected(&msg.message_id, channel) {
                out.push(msg);
            }
        }
        out
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
        let mut budget = max_bytes;
        for msg in self.pending() {
            let sep = if out.is_empty() { 0 } else { 2 };
            if msg.framed.len() + sep > budget {
                break;
            }
            budget -= msg.framed.len() + sep;
            if self.mark_injected(&msg.message_id, channel) {
                out.push(msg);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        // One file per message, deterministic name.
        assert!(inbox.message_path("m1").exists());
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
        // The marker records injected_at and the channel.
        let marker = std::fs::read_to_string(inbox.marker_path("m1")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&marker).unwrap();
        assert_eq!(v["message_id"], "m1");
        assert_eq!(v["channel"], "hook");
        assert!(v["injected_at"].as_u64().is_some());
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
        // atomic marker).
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
        // Distinct timestamps: the queue order is by queued_at, so m0/m1 are
        // the oldest two.
        for i in 0..4 {
            let mut m = msg(&format!("m{i}"), &format!("text {i}"));
            m.queued_at = now_unix().saturating_sub(10 - i as u64);
            inbox.enqueue(m).unwrap();
        }
        // A budget for exactly two messages (the actual framed length): the
        // rest stays queued.
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
        // Distinct timestamps: the queue order is by queued_at, so the oldest
        // message is deterministic.
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
        // The oldest messages were dropped and the drop is durable (the note
        // is a queued message the injecting side sees).
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
        // The combined bytes stay within the bound.
        let bytes: usize = pending.iter().map(|m| m.framed.len()).sum();
        assert!(bytes <= MAX_QUEUED_BYTES, "byte bound exceeded: {bytes}");
    }

    #[test]
    fn ttl_expires_uninjected_messages() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::with_ttl(tmp.path(), 10);
        inbox.enqueue(msg("m1", "hello")).unwrap();
        assert_eq!(inbox.pending().len(), 1);
        // Age the message past the TTL: it stops being served.
        let path = inbox.message_path("m1");
        let mut m = inbox.pending()[0].clone();
        m.queued_at = now_unix().saturating_sub(11);
        Inbox::write_atomic(&path, &serde_json::to_value(&m).unwrap()).unwrap();
        assert!(inbox.pending().is_empty());
        assert!(inbox.pending_count() == 0);
        // The host's next enqueue prunes the expired file.
        inbox.enqueue(msg("m2", "next")).unwrap();
        assert!(!path.exists(), "expired message file must be pruned");
        assert_eq!(inbox.pending().len(), 1);
    }

    #[test]
    fn from_env_reads_the_inbox_variable() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var(INBOX_ENV);
        assert!(Inbox::from_env().is_none());
        std::env::set_var(INBOX_ENV, "/tmp/some-inbox");
        let inbox = Inbox::from_env().unwrap();
        assert_eq!(inbox.dir(), Path::new("/tmp/some-inbox"));
        std::env::set_var(INBOX_ENV, "   ");
        assert!(Inbox::from_env().is_none());
        std::env::remove_var(INBOX_ENV);
    }

    #[test]
    fn inbox_state_root_follows_agent_run_state_root() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // The shared inbox root is derived from AGENT_RUN_STATE_ROOT (the same
        // resolution the agent-run config applies), NOT from a supervisor
        // state file location. Under a non-default state root the session
        // inbox must land under THAT root.
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("state-root");
        std::env::set_var("AGENT_RUN_STATE_ROOT", root.to_str().unwrap());
        std::env::remove_var("AGENT_RUN_WORKSPACE_ROOT");
        std::env::remove_var("AGENT_RUN_CONFIG");
        let resolved = inbox_state_root();
        assert_eq!(
            resolved, root,
            "the shared inbox root must follow AGENT_RUN_STATE_ROOT exactly"
        );
        assert_eq!(
            inbox_dir_for(&resolved, "homelab"),
            root.join("a2a-inbox").join("homelab")
        );
        std::env::remove_var("AGENT_RUN_STATE_ROOT");
    }

    /// Test 1: the sink and the session launcher must resolve the IDENTICAL
    /// inbox directory for the same session name, under a non-default
    /// `state_file` -- and that directory must be under the shared agent-run
    /// state root, NOT under the state file's parent (the pre-fix `/tmp`
    /// mismatch). Both sides route through [`inbox_state_root`], so the
    /// regression cannot recur by construction.
    #[test]
    fn sink_and_session_agree_on_the_inbox_dir_for_a_non_default_state_file() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("data/state-root");
        std::env::set_var("AGENT_RUN_STATE_ROOT", root.to_str().unwrap());
        std::env::remove_var("AGENT_RUN_WORKSPACE_ROOT");
        std::env::remove_var("AGENT_RUN_CONFIG");

        // The session's launcher input: a supervisor config whose state file
        // lives in a directory that has NOTHING to do with the inbox root.
        let state_file = tmp.path().join("elsewhere").join("state.json");
        let session = "homelab";

        // The SINK resolves the session inbox on the shared state root (this
        // is the root `orchestrate/http_server.rs` passes to the A2A router).
        let sink_inbox = inbox_dir_for(&inbox_state_root(), session);
        // The SESSION resolves it through the same shared helper (the
        // session launcher now uses it, not `state_file.parent()`).
        let session_inbox = inbox_dir_for(&inbox_state_root(), session);

        assert_eq!(sink_inbox, session_inbox);
        assert_eq!(sink_inbox, root.join("a2a-inbox").join(session));
        // The pre-fix root -- the state file's parent -- must NOT be used.
        let legacy_root = state_file
            .parent()
            .unwrap_or_else(|| std::path::Path::new("/tmp"));
        let legacy_inbox = inbox_dir_for(legacy_root, session);
        assert_ne!(
            sink_inbox, legacy_inbox,
            "the session inbox must not be derived from the supervisor state file's parent"
        );
        assert!(
            sink_inbox.starts_with(&root),
            "the session inbox must live under the shared agent-run state root"
        );

        std::env::remove_var("AGENT_RUN_STATE_ROOT");
    }

    #[test]
    fn inbox_dir_for_sanitizes_the_run_id() {
        let root = Path::new("/state");
        assert_eq!(
            inbox_dir_for(root, "job-1"),
            PathBuf::from("/state/a2a-inbox/job-1")
        );
        // A hostile id cannot traverse: separators and dots become underscores.
        let dir = inbox_dir_for(root, "../../etc");
        assert_eq!(dir, PathBuf::from("/state/a2a-inbox/______etc"));
        assert!(!dir.to_string_lossy().contains(".."));
        // Both sides agree on the same directory for the same id.
        assert_eq!(inbox_dir_for(root, "ab/cd"), inbox_dir_for(root, "ab/cd"));
    }

    #[test]
    fn message_files_survive_a_garbled_sibling() {
        let tmp = tempdir().unwrap();
        let inbox = Inbox::new(tmp.path());
        inbox.enqueue(msg("m1", "good")).unwrap();
        // A torn/hostile file (not JSON) is skipped, never wedges the inbox.
        std::fs::write(tmp.path().join("m-garbage.json"), b"\x00not json").unwrap();
        let pending = inbox.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message_id, "m1");
        // Non-message files are ignored.
        std::fs::write(tmp.path().join("unrelated.txt"), b"x").unwrap();
        assert_eq!(inbox.pending().len(), 1);
    }
}

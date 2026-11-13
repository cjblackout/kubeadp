//! In-memory A2A inbox repository for the test suites (HL-113 §7.14).
//!
//! The real inbox is the shared per-pod SQLite store (`<state_root>/state.sqlite`,
//! migration v3, see [`crate::core::a2a::inbox`]). The parity/black-box tests
//! use [`StorageKind::InMemory`](crate::core::a2a::inbox::StorageKind::InMemory)
//! inboxes at arbitrary temp dirs (the shared state root is not available to a
//! unit test's temp path). To mirror the real shared-database behaviour (two
//! `Inbox` handles on the same inbox read and write ONE store), each in-memory
//! inbox keeps ONE shared `Connection` per inbox dir in a process-wide
//! registry. All tables are created so the same SQL that the repo path runs
//! works unchanged.
//!
//! The registry is keyed by the inbox dir path so separate tempdirs are
//! independent and two clones of the same `Inbox` share a store.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

use rusqlite::Connection;

/// The in-memory tables (mirror of migration v3).
const MEM_SQL: &str = "
CREATE TABLE IF NOT EXISTS a2a_message (
    run_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    fake INTEGER NOT NULL DEFAULT 0,
    sender TEXT NOT NULL,
    hop INTEGER NOT NULL,
    context_id TEXT,
    task_id TEXT,
    framed TEXT NOT NULL,
    queued_at INTEGER NOT NULL,
    claimed_at INTEGER,
    injected_at INTEGER,
    channel TEXT,
    PRIMARY KEY (run_id, message_id)
);
CREATE TABLE IF NOT EXISTS a2a_expiry (
    run_id TEXT NOT NULL,
    fake INTEGER NOT NULL DEFAULT 0,
    message_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    expired_at INTEGER NOT NULL,
    expired_at_ns INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, message_id)
);
";

static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<Connection>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Returns the shared in-memory connection for the inbox at `dir`, creating
/// it (with the tables) on first use, or `None` when the store cannot be
/// created. Serialized access is provided by the per-dir mutex the caller
/// locks.
pub fn shared_conn(dir: &std::path::Path) -> Option<Arc<Mutex<Connection>>> {
    let mut reg = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = reg.get(dir) {
        return Some(Arc::clone(conn));
    }
    let conn = Connection::open_in_memory().ok()?;
    conn.execute_batch(MEM_SQL).ok()?;
    let arc = Arc::new(Mutex::new(conn));
    reg.insert(dir.to_path_buf(), Arc::clone(&arc));
    // Best-effort bound: dropped tempdirs leave entries behind; release any
    // entry whose Arc has no live handles left (they are gone forever).
    if reg.len() > 512 {
        reg.retain(|_k, v| Arc::strong_count(v) > 1);
    }
    Some(arc)
}

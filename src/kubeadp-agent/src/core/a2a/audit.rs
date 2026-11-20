//! Metadata-only A2A audit store (design HL-125 stream 3 C2, SPEC §7.15).
//!
//! Every accepted, rejected-after-authorization, sent, injected, expired and
//! dropped message writes one metadata-only row to the `a2a_audit` table in
//! the shared per-pod state database (`<state_root>/state.sqlite`, migration
//! 7). An audit row carries from/to, the jobset, the message id, the message
//! size, the delivery channel, the delivery outcome and the timestamps --
//! NEVER the message text. Message text exists only in `a2a_message.framed`,
//! which [`crate::core::a2a::inbox`] sets to `NULL` 60 s after injection and
//! at once on expire/drop.
//!
//! Requests refused by §7.4 step 1 or 2 (an *unverified* sender) are logged,
//! not audited.
//!
//! # Purge
//!
//! [`purge`] deletes, for a given `now`, the `a2a_audit` rows whose timestamp
//! is at least [`AUDIT_RETENTION_SECS`] (14 days) old. The inbox half of the
//! purge (deleting text-less `a2a_message` rows past retention) lives in
//! [`crate::core::a2a::inbox::purge`]; the combined pass is
//! [`crate::core::a2a::inbox::purge`].
//!
//! A purge pass runs at startup and at least hourly
//! ([`crate::core::a2a::inbox::start_purge_task`]). The purge is idempotent:
//! re-running it with the same `now` is a no-op.
//!
//! The `a2a_audit` table is owned by this module; migration 7 creates it.

use std::path::Path;

use rusqlite::Connection;

/// The `a2a_audit` retention window: rows are deleted 14 days after their
/// timestamp (SPEC §7.15).
pub const AUDIT_RETENTION_SECS: u64 = 14 * 24 * 3600;

/// The outcome vocabulary of an audit row (SPEC §7.15): a message was
/// `queued` (accepted), `injected` (delivered to the model context),
/// `expired` (TTL reached uninjected), `dropped` (queue overflow), or
/// `rejected` (refused after authorization).
pub mod outcome {
    /// The message was accepted and stored in the inbox.
    pub const QUEUED: &str = "queued";
    /// The message was injected into the recipient's model context.
    pub const INJECTED: &str = "injected";
    /// The message expired uninjected after the TTL.
    pub const EXPIRED: &str = "expired";
    /// The message was dropped by the bounded queue (oldest dropped).
    pub const DROPPED: &str = "dropped";
    /// The message was refused after authorization.
    pub const REJECTED: &str = "rejected";
}

/// An A2A audit row (SPEC §7.15): metadata of one message lifecycle event.
/// Never contains message text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    /// The recipient run (job id or session name) the message addressed.
    pub run_id: String,
    /// The A2A message id (the dedupe key).
    pub message_id: String,
    /// The originating agent id that sent the message.
    pub from_agent_id: String,
    /// The addressed recipient agent id.
    pub to_agent_id: String,
    /// The jobset the exchange took place under, if known.
    pub jobset_id: Option<String>,
    /// The message size in bytes (framed text length).
    pub message_size: i64,
    /// The injection channel, when the event is an injection.
    pub channel: Option<String>,
    /// Unix seconds the message was delivered (enqueued).
    pub delivered_at: Option<u64>,
    /// Unix seconds the message was injected (acknowledged).
    pub injected_at: Option<u64>,
    /// The outcome: one of [`outcome`].
    pub outcome: String,
    /// Unix seconds of this audit event.
    pub timestamp: u64,
}

/// The audit store over the shared per-pod state database.
///
/// Only the pod's own process (the A2A sink and delivery ladder) writes and
/// reads it directly. It is cheap to construct: a handle holds merely the
/// state root and opens a fresh connection per operation, exactly like the
/// inbox store.
#[derive(Debug, Clone)]
pub struct AuditStore {
    state_root: std::path::PathBuf,
}

impl AuditStore {
    /// Opens the audit store for the given state root.
    pub fn new(state_root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            state_root: state_root.into(),
        }
    }

    /// The state root this store addresses.
    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T, String>) -> Result<T, String> {
        let conn = crate::core::job::open_state_db(&self.state_root)
            .map_err(|e| format!("failed to open the A2A state database: {e:#}"))?;
        f(&conn)
    }

    /// Records one audit row. Writes the metadata report of a message
    /// lifecycle event; a same `(run_id, message_id, outcome)` row is
    /// replaced (a re-send with the same outcome is idempotent). Best-effort
    /// by design: an audit failure never fails message delivery.
    pub fn record(&self, row: AuditRow) -> Result<(), String> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO a2a_audit
                    (run_id, message_id, from_agent_id, to_agent_id, jobset_id,
                     message_size, channel, delivered_at, injected_at, outcome, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(run_id, message_id, outcome)
                 DO UPDATE SET from_agent_id=excluded.from_agent_id,
                               to_agent_id=excluded.to_agent_id,
                               jobset_id=excluded.jobset_id,
                               message_size=excluded.message_size,
                               channel=excluded.channel,
                               delivered_at=excluded.delivered_at,
                               injected_at=excluded.injected_at,
                               timestamp=excluded.timestamp",
                rusqlite::params![
                    row.run_id,
                    row.message_id,
                    row.from_agent_id,
                    row.to_agent_id,
                    row.jobset_id,
                    row.message_size,
                    row.channel,
                    row.delivered_at,
                    row.injected_at,
                    row.outcome,
                    row.timestamp,
                ],
            )
            .map_err(|e| format!("failed to write the A2A audit row: {e}"))?;
            Ok(())
        })
    }

    /// Returns every audit row for a run (metadata only), oldest first.
    pub fn rows_for_run(&self, run_id: &str) -> Vec<AuditRow> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT run_id, message_id, from_agent_id, to_agent_id, jobset_id,
                            message_size, channel, delivered_at, injected_at, outcome, timestamp
                     FROM a2a_audit WHERE run_id=?1 ORDER BY timestamp, message_id",
                )
                .map_err(|e| format!("failed to read the A2A audit rows: {e}"))?;
            let rows = stmt
                .query_map(rusqlite::params![run_id], |row| {
                    Ok(AuditRow {
                        run_id: row.get(0)?,
                        message_id: row.get(1)?,
                        from_agent_id: row.get(2)?,
                        to_agent_id: row.get(3)?,
                        jobset_id: row.get(4)?,
                        message_size: row.get(5)?,
                        channel: row.get(6)?,
                        delivered_at: row.get(7)?,
                        injected_at: row.get(8)?,
                        outcome: row.get(9)?,
                        timestamp: row.get(10)?,
                    })
                })
                .map_err(|e| format!("failed to query the A2A audit rows: {e}"))?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .unwrap_or_default()
    }

    /// The total number of audit rows (for tests and the purge bookkeeping).
    pub fn count(&self) -> usize {
        self.with_conn(|conn| {
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM a2a_audit", [], |row| row.get(0))
                .map_err(|e| format!("failed to count the A2A audit rows: {e}"))?;
            Ok(n.max(0) as usize)
        })
        .unwrap_or(0)
    }

    /// Purges the expired audit rows: deletes every `a2a_audit` row whose
    /// `timestamp` is at least [`AUDIT_RETENTION_SECS`] before `now`. Returns
    /// the number of rows removed. Idempotent: a second purge with the same
    /// `now` removes nothing.
    pub fn purge(&self, now: u64) -> Result<usize, String> {
        self.with_conn(|conn| Self::purge_at(conn, now))
    }

    /// The SQL half of [`purge`] over a caller-provided connection, so the
    /// combined inbox+audit purge runs on one connection.
    pub fn purge_at(conn: &Connection, now: u64) -> Result<usize, String> {
        let removed = conn
            .execute(
                "DELETE FROM a2a_audit WHERE (?1 - timestamp) >= ?2",
                rusqlite::params![now as i64, AUDIT_RETENTION_SECS as i64],
            )
            .map_err(|e| format!("failed to purge the A2A audit rows: {e}"))?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn repo(tmp: &tempfile::TempDir) -> AuditStore {
        let root = tmp.path().join("state-root");
        std::fs::create_dir_all(&root).unwrap();
        AuditStore::new(root)
    }

    fn row(run: &str, mid: &str, outcome: &str, ts: u64) -> AuditRow {
        AuditRow {
            run_id: run.to_string(),
            message_id: mid.to_string(),
            from_agent_id: "sender".to_string(),
            to_agent_id: "receiver".to_string(),
            jobset_id: Some("jobset-1".to_string()),
            message_size: 42,
            channel: None,
            delivered_at: Some(ts),
            injected_at: None,
            outcome: outcome.to_string(),
            timestamp: ts,
        }
    }

    #[test]
    fn record_and_read_rows() {
        let tmp = tempdir().unwrap();
        let store = repo(&tmp);
        store
            .record(row("run-a", "m1", outcome::QUEUED, 100))
            .unwrap();
        store
            .record(row("run-a", "m1", outcome::INJECTED, 101))
            .unwrap();
        store
            .record(row("run-b", "m1", outcome::QUEUED, 102))
            .unwrap();
        let rows = store.rows_for_run("run-a");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].outcome, outcome::QUEUED);
        assert_eq!(rows[1].outcome, outcome::INJECTED);
        assert_eq!(store.count(), 3);
        // A duplicate (run, message, outcome) upserts, does not grow.
        store
            .record(row("run-a", "m1", outcome::QUEUED, 105))
            .unwrap();
        assert_eq!(store.count(), 3);
    }

    #[test]
    fn purge_removes_expired_rows_and_is_idempotent() {
        let tmp = tempdir().unwrap();
        let store = repo(&tmp);
        let ts_m1 = 10u64;
        let ts_m2 = ts_m1 + AUDIT_RETENTION_SECS;
        store
            .record(row("run-a", "m1", outcome::QUEUED, ts_m1))
            .unwrap();
        store
            .record(row("run-a", "m2", outcome::QUEUED, ts_m2))
            .unwrap();
        // At `ts_m1 + retention` m1 is exactly at the retention edge (age ==
        // retention, so `>=` purges it); m2 is brand new (age 0) and kept.
        assert_eq!(store.count(), 2);
        assert_eq!(store.purge(ts_m1 + AUDIT_RETENTION_SECS).unwrap(), 1);
        assert_eq!(store.count(), 1);
        // A second purge with the same `now` removes nothing (idempotent).
        assert_eq!(store.purge(ts_m1 + AUDIT_RETENTION_SECS).unwrap(), 0);
        assert_eq!(store.count(), 1);
        // m2 reaches the same edge only at `ts_m2 + retention`.
        assert_eq!(store.purge(ts_m2 + AUDIT_RETENTION_SECS).unwrap(), 1);
        assert_eq!(store.count(), 0);
    }
}

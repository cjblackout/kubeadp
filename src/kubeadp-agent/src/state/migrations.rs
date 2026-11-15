//! Numbered, additive schema migrations of the shared per-pod state database
//! (`<state_root>/state.sqlite`).
//!
//! This module is the ONE place where the database schema evolves: every
//! table owner appends its migration to [`MIGRATIONS`] and [`migrate`] brings
//! any database up to the highest declared version, exactly once per
//! database (tracked by `PRAGMA user_version`), one migration at a time.
//! Current table owners:
//! - the `jobs` table (the agent-run job store, version 1),
//! - the orchestrator's `supervisor_state` / `session_state` tables
//!   (version 2),
//! - the per-run A2A inbox (`a2a_message` / `a2a_expiry`,
//!   version 3). The inbox is the shared per-pod SQLite-backed store (HL-113
//!   phase 3): the pod's own process (the A2A sink and the escalation ladder)
//!   writes and reads it directly; a SANDBOXED run reaches it only through
//!   the pod's local HTTP API, never the database file.
//!
//! Rules for a new migration:
//! - additive only (new tables, new columns; never drop or rename),
//! - strictly ascending `version`, appended at the end of [`MIGRATIONS`],
//! - `apply` must be idempotent against databases created before the
//!   migration framework existed (they reach the first framework version
//!   with the old ad-hoc layout already in place).

use anyhow::{Context, Result};
use rusqlite::Connection;

/// One additive schema migration. `apply` runs at most once per database --
/// when the database's `user_version` is still below `version` -- and brings
/// the database to exactly that version.
pub struct Migration {
    /// The schema version the database reaches when this migration is
    /// applied. Migrations are declared in strictly ascending order.
    pub version: i64,
    /// Human-readable name for logs and error messages.
    pub name: &'static str,
    /// The migration body. Must be idempotent against pre-framework
    /// databases (see the module docs).
    pub apply: fn(&Connection) -> Result<()>,
}

/// The `jobs` table: the original agent-run job store (pre-framework layout,
/// kept as migration 1 so an old database migrates in place), followed by
/// the column upgrades that the old ad-hoc `open_state_db` code applied on
/// every open (CREATE TABLE IF NOT EXISTS never alters an existing table).
fn jobs_table_v1(conn: &Connection) -> Result<()> {
    conn.execute_batch(JOBS_TABLE_V1_SQL)
        .context("Failed to initialize jobs table")?;
    // Column upgrades for databases created before the columns existed. Each
    // missing column is added with an idempotent table_info check first; a
    // concurrent process may add the same column between the check and the
    // ALTER (that error is benign).
    let columns: Vec<String> = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(jobs)")
            .context("Failed to inspect the jobs table")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .context("Failed to read the jobs table columns")?;
        rows.filter_map(|r| r.ok()).collect()
    };
    for (col, decl) in JOBS_V1_COLUMNS {
        if !columns.iter().any(|c| c == *col) {
            if let Err(e) =
                conn.execute_batch(&format!("ALTER TABLE jobs ADD COLUMN {col} {decl};"))
            {
                if !e.to_string().contains("duplicate column") {
                    return Err(e).context(format!(
                        "Failed to migrate the jobs table (missing column '{col}')"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The original `jobs` table schema. `created_at` is stored as integer epoch
/// nanoseconds so ordering is exact; booleans as 0/1.
const JOBS_TABLE_V1_SQL: &str = "
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    project TEXT NOT NULL,
    prompt TEXT NOT NULL,
    resolved_prompt TEXT NOT NULL,
    model TEXT,
    effort TEXT,
    sandbox INTEGER NOT NULL,
    mode TEXT NOT NULL,
    add_dirs TEXT NOT NULL,
    print_timeout TEXT,
    created_at INTEGER NOT NULL,
    worktree_path TEXT NOT NULL,
    branch TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    conversation_id TEXT,
    retry_count INTEGER NOT NULL,
    pid INTEGER,
    exit_code INTEGER,
    status TEXT,
    hindsight INTEGER,
    auto_recall INTEGER,
    auto_retain INTEGER,
    retry_prompt TEXT
)
";

/// The column upgrades applied on top of [`JOBS_TABLE_V1_SQL`]: `(name, column
/// declaration)` pairs, added with a table_info check first so pre-framework
/// databases (which already carry some of them) migrate in place.
const JOBS_V1_COLUMNS: &[(&str, &str)] = &[
    ("read_only", "INTEGER NOT NULL DEFAULT 0"),
    ("external_worktree", "INTEGER NOT NULL DEFAULT 0"),
    ("pid_start_time", "INTEGER"),
    ("hindsight_bank", "TEXT"),
    ("proxy", "TEXT"),
    ("max_runtime", "INTEGER"),
    ("started_at", "INTEGER"),
    ("hindsight_query", "TEXT"),
    ("sandbox_info", "TEXT"),
    // Engine selection (HL-91 E5): an optional TEXT column defaulting to
    // NULL. NULL rows are the historical agy jobs (the agy engine was the
    // only driver before engine selection existed) -- `engine` resolves to
    // the configured/role default at read time, so NULL never changes
    // status/retry behaviour.
    ("engine", "TEXT"),
];

/// The orchestrator's supervisor and per-session state tables (one
/// `supervisor_state` row, one `session_state` row per session). Only the
/// supervisor process writes them; readers use read-only connections.
fn supervisor_session_state_v2(conn: &Connection) -> Result<()> {
    conn.execute_batch(SUPERVISOR_SESSION_STATE_V2_SQL)
        .context("Failed to initialize supervisor/session state tables")
}

const SUPERVISOR_SESSION_STATE_V2_SQL: &str = "
CREATE TABLE IF NOT EXISTS supervisor_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    supervisor_pid INTEGER NOT NULL,
    started TEXT NOT NULL,
    heartbeat TEXT NOT NULL,
    heartbeat_epoch INTEGER NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS session_state (
    name TEXT PRIMARY KEY,
    pid INTEGER,
    start_time TEXT,
    start_time_epoch INTEGER,
    uptime_secs REAL,
    restart_count INTEGER NOT NULL DEFAULT 0,
    last_exit_reason TEXT,
    status TEXT NOT NULL,
    last_health_result TEXT,
    engine TEXT,
    updated_at INTEGER NOT NULL
);
";

/// The per-run A2A inbox (HL-113 §7.14, phase 3): one shared SQLite store
/// (`<state_root>/state.sqlite`) replaces the phase-1/2 per-run directory
/// (`<state_root>/a2a-inbox/<run>/`).
///
/// - `a2a_message` holds every message ever accepted for a run, in states
///   `queued` -> `injected` (the phase-1 injection marker maps to the
///   `injected_at`/`channel` columns of the SAME row; the durable dedupe
///   store is the row itself, keyed by (run_id, message_id)). Injected rows
///   are kept (the acknowledgement is permanent, exactly like the phase-1
///   markers); expired uninjected rows are pruned by the host's next enqueue.
///   The bounded-queue drop note (design §7.14: `MAX_QUEUED_MSGS` /
///   `MAX_QUEUED_BYTES`, oldest dropped, durable note) is inserted as an
///   ordinary `a2a_message` row (sender `inbox`, the note text), exactly like
///   the phase-1 note message -- so the injecting side sees and consumes it.
/// - `a2a_expiry` replaces the phase-2 `expired/` marker directory: the
///   durable record that fails the sender's task after the TTL.
///
/// A `fake` flag exists for the parity/black-box tests that keep using the
/// in-memory/custom-rooted `Inbox` (any run whose messages were written with
/// `fake=1` is served from in-memory tables and never collides with the real
/// per-pod db). Only the pod's own process uses the module directly; a
/// sandboxed run reaches the inbox through the local HTTP API with a per-run
/// token, never this database.
fn a2a_inbox_v3(conn: &Connection) -> Result<()> {
    conn.execute_batch(A2A_INBOX_V3_SQL)
        .context("Failed to initialize the A2A inbox tables")
}

const A2A_INBOX_V3_SQL: &str = "
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
CREATE INDEX IF NOT EXISTS a2a_message_pending_idx ON a2a_message (run_id, injected_at, claimed_at, queued_at)
    WHERE fake = 0;
CREATE TABLE IF NOT EXISTS a2a_expiry (
    run_id TEXT NOT NULL,
    fake INTEGER NOT NULL DEFAULT 0,
    message_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    expired_at INTEGER NOT NULL,
    expired_at_ns INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, message_id)
);
CREATE INDEX IF NOT EXISTS a2a_expiry_run_idx ON a2a_expiry (run_id)
    WHERE fake = 0;
";

/// Every declared migration, in application order. Append new migrations at
/// the end (additive, ascending version); see the module docs.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "jobs table",
        apply: jobs_table_v1,
    },
    Migration {
        version: 2,
        name: "supervisor and session state tables",
        apply: supervisor_session_state_v2,
    },
    Migration {
        version: 3,
        name: "A2A inbox tables",
        apply: a2a_inbox_v3,
    },
];

/// The highest declared migration version (the schema version a fully
/// migrated database carries).
pub fn current_version() -> i64 {
    MIGRATIONS.last().map(|m| m.version).unwrap_or(0)
}

/// Brings the database up to [`current_version`]: applies every declared
/// migration above the database's `PRAGMA user_version`, in order, and
/// leaves the version recorded. Re-running on an up-to-date database is a
/// no-op; two concurrent processes migrating the same database are safe
/// (migrations are additive and idempotent). Refuses a database whose
/// `user_version` exceeds [`current_version`] (created by a newer binary).
pub fn migrate(conn: &Connection) -> Result<i64> {
    let mut version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let max_known = current_version();
    if version > max_known {
        anyhow::bail!(
            "state database schema user_version ({version}) is newer than the highest migration known to this binary ({max_known})"
        );
    }
    for migration in MIGRATIONS {
        if migration.version <= version {
            continue;
        }
        (migration.apply)(conn).with_context(|| {
            format!(
                "migration {} ({}) failed",
                migration.version, migration.name
            )
        })?;
        conn.pragma_update(None, "user_version", migration.version)
            .with_context(|| format!("failed to record schema version {}", migration.version))?;
        version = migration.version;
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    #[test]
    fn migrations_are_ascending_and_contiguous() {
        for (i, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(
                m.version,
                (i + 1) as i64,
                "migrations must be declared in strictly ascending, contiguous order"
            );
        }
    }

    #[test]
    fn migrate_creates_all_tables_on_a_fresh_database() {
        let conn = Connection::open_in_memory().unwrap();
        let version = migrate(&conn).unwrap();
        assert_eq!(version, current_version());

        for table in [
            "jobs",
            "supervisor_state",
            "session_state",
            "a2a_message",
            "a2a_expiry",
        ] {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                    params![table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "table {table} must exist after migrate");
        }
    }

    #[test]
    fn migrate_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        let first = migrate(&conn).unwrap();
        let second = migrate(&conn).unwrap();
        assert_eq!(first, second);
        assert_eq!(second, current_version());
    }

    #[test]
    fn migrate_upgrades_a_pre_framework_jobs_database() {
        let conn = Connection::open_in_memory().unwrap();
        // The OLD jobs schema (before read_only / engine et al existed),
        // written before the migration framework: user_version stays 0.
        conn.execute_batch(
            "CREATE TABLE jobs (
                job_id TEXT PRIMARY KEY, project TEXT NOT NULL, prompt TEXT NOT NULL,
                resolved_prompt TEXT NOT NULL, model TEXT, effort TEXT,
                sandbox INTEGER NOT NULL, mode TEXT NOT NULL, add_dirs TEXT NOT NULL,
                print_timeout TEXT, created_at INTEGER NOT NULL,
                worktree_path TEXT NOT NULL, branch TEXT NOT NULL, base_ref TEXT NOT NULL,
                conversation_id TEXT, retry_count INTEGER NOT NULL, pid INTEGER,
                exit_code INTEGER, status TEXT, hindsight INTEGER, auto_recall INTEGER,
                auto_retain INTEGER, retry_prompt TEXT
            );
            INSERT INTO jobs (job_id, project, prompt, resolved_prompt, sandbox, mode, add_dirs, created_at, worktree_path, branch, base_ref, retry_count)
            VALUES ('oldjob01', 'p', 'pr', 'pr', 0, 'plan', '[]', 0, '/wt', 'b', 'main', 0);",
        )
        .unwrap();

        migrate(&conn).unwrap();

        let engine: Option<String> = conn
            .query_row(
                "SELECT engine FROM jobs WHERE job_id = 'oldjob01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(engine, None, "the legacy column must be added and NULL");
        let read_only: i64 = conn
            .query_row(
                "SELECT read_only FROM jobs WHERE job_id = 'oldjob01'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(read_only, 0);
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, current_version());
    }

    #[test]
    fn migrate_refuses_future_user_version() {
        let conn = Connection::open_in_memory().unwrap();
        let future = current_version() + 5;
        conn.pragma_update(None, "user_version", future).unwrap();
        let err = migrate(&conn).unwrap_err().to_string();
        assert!(
            err.contains(&future.to_string()) && err.contains(&current_version().to_string()),
            "error must name both the database user_version and known max version, got: {err}"
        );
    }
}

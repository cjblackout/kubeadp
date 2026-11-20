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
//!   (versions 2 and 4),
//! - the per-run A2A inbox (`a2a_message` / `a2a_expiry`,
//!   version 3). The inbox is the shared per-pod SQLite-backed store (HL-113
//!   phase 3): the pod's own process (the A2A sink and the escalation ladder)
//!   writes and reads it directly; a SANDBOXED run reaches it only through
//!   the pod's local HTTP API, never the database file,
//! - the run metadata table (`run_meta`, version 5: paused flag, compact record,
//!   fork lineage, updated_at).
//!
//! Rules for a new migration:
//! - additive only (new tables, new columns; never drop or rename),
//! - strictly ascending `version`, appended at the end of [`MIGRATIONS`],
//! - `apply` must be idempotent against databases created before the
//!   migration framework existed (they reach the first framework version
//!   with the old ad-hoc layout already in place).

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

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

/// The supervisor heartbeat with sub-second precision (migration 4): the
/// heartbeat epoch in MILLISECONDS (`heartbeat_epoch_ms`). The whole-second
/// `heartbeat_epoch` column is kept in sync so readers of older binaries
/// keep working; rows written before this column existed read 0 and fall
/// back to the whole-second epoch at read time. Whole-second truncation on
/// both ends of the age computation loses up to two seconds and made a
/// strict `age > threshold` stale check miss its threshold (T13 flake).
fn supervisor_heartbeat_ms_v4(conn: &Connection) -> Result<()> {
    let columns: Vec<String> = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(supervisor_state)")
            .context("Failed to inspect the supervisor_state table")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .context("Failed to read the supervisor_state table columns")?;
        rows.filter_map(|r| r.ok()).collect()
    };
    if !columns.iter().any(|c| c == "heartbeat_epoch_ms") {
        if let Err(e) = conn.execute_batch(
            "ALTER TABLE supervisor_state ADD COLUMN heartbeat_epoch_ms INTEGER NOT NULL DEFAULT 0;",
        ) {
            if !e.to_string().contains("duplicate column") {
                return Err(e).context(
                    "Failed to migrate the supervisor_state table (missing column 'heartbeat_epoch_ms')",
                );
            }
        }
    }
    Ok(())
}

/// The `run_meta` table (migration 5, HL-91 §2.3 / HL-125 B3): stores per-run
/// metadata (paused flag, compact summary, compaction timestamp, fork lineage)
/// that supplements the authoritative `jobs` and `session_state` rows.
fn run_meta_v5(conn: &Connection) -> Result<()> {
    conn.execute_batch(RUN_META_V5_SQL)
        .context("Failed to initialize run_meta table")
}

const RUN_META_V5_SQL: &str = "
CREATE TABLE IF NOT EXISTS run_meta (
    run_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0,
    compact_summary TEXT,
    compacted_at INTEGER,
    forked_from TEXT,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS run_meta_kind_idx ON run_meta (kind);
";

/// The `jobset` / `jobset_member` tables (migration 6, HL-125 Stream 3 C1,
/// SPEC §2.3 / §7.2). Rows hold ids and epochs only -- never a secret, a
/// proof or the root key. In addition to the plain schema this migration
/// performs the ONE-TIME idempotent import of the legacy file-based
/// membership store (`jobsets.json` + the `job-jobsets/` directory) into
/// these tables. Imported jobsets carry creator `''` and cannot gain members
/// without a creator; they keep working for their existing members.
fn jobset_tables_v6(conn: &Connection) -> Result<()> {
    conn.execute_batch(JOBSET_TABLES_V6_SQL)
        .context("Failed to initialize jobset tables")?;
    jobset_import_from_files(conn)
}

const JOBSET_TABLES_V6_SQL: &str = "
CREATE TABLE IF NOT EXISTS jobset (
    jobset_id TEXT PRIMARY KEY,
    creator_agent_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    ended INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    ended_at INTEGER
);
CREATE TABLE IF NOT EXISTS jobset_member (
    jobset_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (jobset_id, agent_id)
);
CREATE INDEX IF NOT EXISTS jobset_member_agent_idx ON jobset_member (agent_id);
";

/// One-time idempotent import of the legacy `jobsets.json` membership store
/// and the `job-jobsets/` remember-files into the `jobset` /
/// `jobset_member` tables.
///
/// The state root the database lives under is derived from the connection's
/// main database file (`<state_root>/state.sqlite`), so `jobsets.json` and
/// `job-jobsets/` are the state-root siblings of the store. The `apply`
/// signature is `fn(&Connection)`; this helper reads the root from the
/// connection itself, which keeps the framework signature unchanged and makes
/// the migration testable against any database path.
///
/// Semantics (design §4, row "3"):
/// - every membership in `jobsets.json` becomes one `jobset_member` row;
/// - each distinct jobset becomes one `jobset(creator = '', epoch, ended)`;
/// - ended markers become tombstones (`ended = 1`);
/// - `job-jobsets/` entries re-associate a job id with the jobset it was
///   started into (the registry's `get_or_remembered` treats a remember-file
///   as membership), one `jobset_member` row per (jobset, job) pair.
///
/// The import is additive and `INSERT OR IGNORE`-based: an existing
/// `jobset`/`jobset_member` row is never overwritten, so re-running (or
/// running against a store that already holds rows, e.g. after the C1 mirror
/// hooks ran) is a no-op. Imported jobsets are created with `created_at`
/// `0`: the legacy file carries no timestamps.
///
/// The imported files are deliberately left in place: C1 keeps the in-memory
/// registry as the read/verification path and it still reloads from
/// `jobsets.json` / `job-jobsets/` at startup ([`load_memberships`]),
/// so renaming them away in C1 would regress restart persistence. The C5
/// flag-day removes those readers and renames the files to `*.migrated`
/// (design §4, "additive ... file renamed `*.migrated`"). The migration
/// itself runs exactly once per database (tracked by `user_version`), so the
/// import is one-time regardless of the files' presence afterwards.
fn jobset_import_from_files(conn: &Connection) -> Result<()> {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    let state_root = match conn
        .query_row("PRAGMA database_list", [], |row| row.get::<_, String>(2))
        .ok()
        .filter(|p| !p.is_empty())
    {
        Some(path) => PathBuf::from(path)
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default(),
        None => PathBuf::new(),
    };
    if state_root.as_os_str().is_empty() {
        // The in-memory test connections have an empty file name; there is
        // nothing to import for them.
        return Ok(());
    }

    let membership_path = state_root.join("jobsets.json");
    if !membership_path.exists() {
        return Ok(());
    }

    // A corrupt membership file is quarantine-territory of the registry load
    // ([`load_memberships`] renames it aside and starts empty); the import
    // matches that: a corrupt file skips the import instead of failing the
    // whole upgrade. The file is left for the registry's own handling.
    let doc: crate::core::jobset::MembershipDoc =
        match serde_json::from_slice(&fs::read(&membership_path)?) {
            Ok(d) => d,
            Err(_e) => return Ok(()),
        };

    let now = chrono::Utc::now().timestamp();
    let upsert_jobset =
        |conn: &Connection, jobset_id: &str, epoch: u64, ended: bool, now: i64| -> Result<()> {
            conn.execute(
                "INSERT INTO jobset (jobset_id, creator_agent_id, epoch, ended, created_at) \
             VALUES (?1, '', ?2, ?3, ?4) \
             ON CONFLICT(jobset_id) DO NOTHING",
                params![jobset_id, epoch, if ended { 1 } else { 0 }, now],
            )
            .context("Failed to insert an imported jobset row")?;
            Ok(())
        };

    // Members -> jobset_member rows; distinct jobsets -> jobset rows.
    for (agent_id, member) in doc
        .members
        .into_iter()
        .take(crate::core::jobset::MEMBERSHIP_MAX_ENTRIES)
    {
        upsert_jobset(conn, &member.jobset_id, member.epoch, false, now)?;
        conn.execute(
            "INSERT OR IGNORE INTO jobset_member (jobset_id, agent_id, joined_at) \
             VALUES (?1, ?2, ?3)",
            params![member.jobset_id, agent_id, now],
        )
        .context("Failed to insert an imported membership row")?;
    }

    // Ended markers -> tombstones.
    for (jobset_id, ended_epoch) in doc
        .ended
        .into_iter()
        .take(crate::core::jobset::ENDED_MAX_ENTRIES)
    {
        upsert_jobset(conn, &jobset_id, ended_epoch, true, now)?;
    }

    // The `job-jobsets/` directory: one `jobset_member` row per (jobset,
    // remembered job) pair.
    let job_jobsets_dir = state_root.join("job-jobsets");
    let mut remembered: HashMap<String, String> = HashMap::new();
    if let Ok(entries) = fs::read_dir(&job_jobsets_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if let Ok(content) = fs::read_to_string(entry.path()) {
                    let id = content.trim().to_string();
                    if !id.is_empty() {
                        remembered.insert(name.to_string(), id);
                    }
                }
            }
        }
    }
    for (job_id, jobset_id) in &remembered {
        upsert_jobset(conn, jobset_id, 0, false, now)?;
        conn.execute(
            "INSERT OR IGNORE INTO jobset_member (jobset_id, agent_id, joined_at) \
             VALUES (?1, ?2, ?3)",
            params![jobset_id, job_id, now],
        )
        .context("Failed to insert an imported remember-file membership")?;
    }

    Ok(())
}

/// The A2A audit table (`a2a_audit`, migration 7, HL-125 stream 3 C2; SPEC
/// §2.3/§7.15). One METADATA-ONLY row per delivered message: from/to, the
/// jobset, the message id, the message size, the delivery channel, the
/// delivery outcome and the timestamps. Message TEXT never lands here -- it
/// exists only in `a2a_message.framed`, which is set `NULL` 60 s after
/// injection and at once on expire/drop. Rows are deleted 14 days after their
/// timestamp by the purge pass (startup + at least hourly).
///
/// Migration 7 also drops the `NOT NULL` on `a2a_message.framed` (SPEC §7.15:
/// message text is set `NULL` 60 s after injection, so the column must accept
/// `NULL`). SQLite cannot relax `NOT NULL` in place, so the table is rebuilt
/// idempotently: the rebuild runs only when the column still says
/// `NOT NULL` (i.e. once), copies every existing row verbatim, re-creates the
/// partial `pending` index, and never touches another table or column. The
/// relax is a pure loosening -- existing rows and their meaning are
/// unchanged, which keeps it compatible with the additive-migration rule.
///
/// Note on numbering: the sibling stream-3 step C1 (JobsetStore) uses
/// migration version 6 for its `jobset`/`jobset_member` tables. This step
/// appends version 7, so the two additive migrations can land in any order on
/// the same crate without renumbering.
fn a2a_audit_v7(conn: &Connection) -> Result<()> {
    a2a_message_framed_nullable(conn)?;
    conn.execute_batch(A2A_AUDIT_V7_SQL)
        .context("Failed to initialize the A2A audit table")
}

/// Relaxes `a2a_message.framed` from `NOT NULL` to nullable, idempotently.
/// Runs at most once per database (guarded by a `PRAGMA table_info` check);
/// a concurrent process that already relaxed it is tolerated.
fn a2a_message_framed_nullable(conn: &Connection) -> Result<()> {
    let framed_not_null = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(a2a_message)")
            .context("Failed to inspect the a2a_message table")?;
        let cols: Vec<(String, i64)> = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let notnull: i64 = row.get(3)?;
                Ok((name, notnull))
            })
            .context("Failed to read the a2a_message table columns")?
            .filter_map(|r| r.ok())
            .collect();
        cols.iter()
            .find(|(name, _)| name == "framed")
            .map(|(_, notnull)| *notnull != 0)
            // The a2a_message table does not exist (a fresh pre-v3 schema or a
            // partial state): nothing to relax, migration 3 creates it later if
            // at all.
            .unwrap_or(false)
    };
    if !framed_not_null {
        return Ok(());
    }
    // The table rebuild must be atomic: a failure must not leave a partial
    // `a2a_message_new` behind (which would break the idempotent re-run from
    // the guard). Migration bodies only see `&Connection`, so the transaction
    // is opened and closed with explicit SQL inside the batch.
    let res = conn.execute_batch(
        "BEGIN IMMEDIATE;
        CREATE TABLE a2a_message_new (
            run_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            fake INTEGER NOT NULL DEFAULT 0,
            sender TEXT NOT NULL,
            hop INTEGER NOT NULL,
            context_id TEXT,
            task_id TEXT,
            framed TEXT,
            queued_at INTEGER NOT NULL,
            claimed_at INTEGER,
            injected_at INTEGER,
            channel TEXT,
            PRIMARY KEY (run_id, message_id)
        );
        INSERT INTO a2a_message_new
            (run_id, message_id, fake, sender, hop, context_id, task_id,
             framed, queued_at, claimed_at, injected_at, channel)
            SELECT run_id, message_id, fake, sender, hop, context_id, task_id,
                   framed, queued_at, claimed_at, injected_at, channel
            FROM a2a_message;
        DROP TABLE a2a_message;
        ALTER TABLE a2a_message_new RENAME TO a2a_message;
        CREATE INDEX IF NOT EXISTS a2a_message_pending_idx
            ON a2a_message (run_id, injected_at, claimed_at, queued_at) WHERE fake = 0;
        COMMIT;",
    );
    if let Err(e) = res {
        let _ = conn.execute_batch("ROLLBACK");
        return Err(anyhow::anyhow!(e))
            .context("Failed to relax a2a_message.framed to NULL (table rebuild)");
    }
    Ok(())
}

const A2A_AUDIT_V7_SQL: &str = "
CREATE TABLE IF NOT EXISTS a2a_audit (
    run_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    from_agent_id TEXT NOT NULL,
    to_agent_id TEXT NOT NULL,
    jobset_id TEXT,
    message_size INTEGER NOT NULL,
    channel TEXT,
    delivered_at INTEGER,
    injected_at INTEGER,
    outcome TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (run_id, message_id, outcome)
);
CREATE INDEX IF NOT EXISTS a2a_audit_timestamp_idx ON a2a_audit (timestamp) WHERE outcome IS NOT NULL;
CREATE INDEX IF NOT EXISTS a2a_audit_run_idx ON a2a_audit (run_id);
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
    Migration {
        version: 4,
        name: "supervisor heartbeat milliseconds",
        apply: supervisor_heartbeat_ms_v4,
    },
    Migration {
        version: 5,
        name: "run_meta table",
        apply: run_meta_v5,
    },
    Migration {
        version: 6,
        name: "jobset and jobset_member tables",
        apply: jobset_tables_v6,
    },
    Migration {
        version: 7,
        name: "A2A audit table",
        apply: a2a_audit_v7,
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
    fn migrations_are_ascending() {
        for w in MIGRATIONS.windows(2) {
            assert!(
                w[0].version < w[1].version,
                "migrations must be declared in strictly ascending order"
            );
        }
        // The parallel stream-3 C1 step owns version 6 (JobsetStore), so this
        // branch's array is expected to read [1..5, 7] until C1 lands. Every
        // version below the highest must be unique.
        let mut seen = std::collections::BTreeSet::new();
        for m in MIGRATIONS {
            assert!(
                seen.insert(m.version),
                "duplicate migration version {}",
                m.version
            );
        }
        assert_eq!(
            MIGRATIONS.last().map(|m| m.version),
            Some(7),
            "the highest migration must be 7 (a2a_audit)"
        );
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
            "run_meta",
            "jobset",
            "jobset_member",
            "a2a_audit",
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
    fn migrate_1_to_5_with_sample_rows_then_7() {
        let conn = Connection::open_in_memory().unwrap();
        // Apply the lower migrations explicitly (1 through 5; version 6 is
        // owned by the parallel C1 JobsetStore step and not present here).
        for m in MIGRATIONS.iter().filter(|m| m.version <= 5) {
            (m.apply)(&conn).unwrap();
            conn.pragma_update(None, "user_version", m.version).unwrap();
        }
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            5
        );

        // Insert sample job and session row
        conn.execute(
            "INSERT INTO jobs (job_id, project, prompt, resolved_prompt, sandbox, mode, add_dirs, created_at, worktree_path, branch, base_ref, retry_count)
             VALUES ('sample-job-1', 'test', 'do work', 'do work', 0, 'plan', '[]', 0, '/tmp/wt', 'main', 'main', 0)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO session_state (name, status, updated_at) VALUES ('sample-session-1', 'running', 123456)",
            [],
        ).unwrap();

        // Now run migrate(&conn), which applies migrations 5 and 6
        let version = migrate(&conn).unwrap();
        assert_eq!(version, current_version());
        assert!(
            version >= 6,
            "the jobset tables migration must have applied"
        );

        // Verify run_meta exists and accepts rows
        conn.execute(
            "INSERT INTO run_meta (run_id, kind, paused, compact_summary, compacted_at, forked_from, updated_at)
             VALUES ('sample-job-1', 'job', 1, 'compacted', 123, NULL, 456)",
            [],
        ).unwrap();

        let paused: i64 = conn
            .query_row(
                "SELECT paused FROM run_meta WHERE run_id = 'sample-job-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(paused, 1);

        // Verify the jobset tables exist and accept rows.
        conn.execute(
            "INSERT INTO jobset (jobset_id, creator_agent_id, epoch, ended, created_at) \
             VALUES ('sample-js', '', 0, 0, 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobset_member (jobset_id, agent_id, joined_at) \
             VALUES ('sample-js', 'sample-agent', 1)",
            [],
        )
        .unwrap();
        // The audit table accepts a metadata-only row.
        conn.execute(
            "INSERT INTO a2a_audit (run_id, message_id, from_agent_id, to_agent_id, jobset_id, message_size, channel, delivered_at, injected_at, outcome, timestamp)
             VALUES ('sample-job-1', 'm1', 'a', 'b', 'js', 42, 'hook', NULL, NULL, 'queued', 123)",
            [],
        ).unwrap();
        let outcome: String = conn
            .query_row(
                "SELECT outcome FROM a2a_audit WHERE run_id = 'sample-job-1' AND message_id = 'm1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome, "queued");
    }

    #[test]
    fn migration_6_imports_jobsets_json_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(root.join("job-jobsets")).unwrap();
        // A legacy membership file: two members of one jobset, one ended
        // marker, and a remember-file for a job.
        std::fs::write(
            root.join("jobsets.json"),
            r#"{
                "members": {
                    "job-a": {"jobset_id": "js-a", "epoch": 0},
                    "job-b": {"jobset_id": "js-a", "epoch": 0},
                    "job-c": {"jobset_id": "js-c", "epoch": 2}
                },
                "ended": {"js-d": 7}
            }"#,
        )
        .unwrap();
        std::fs::write(root.join("job-jobsets").join("job-b"), "js-a").unwrap();
        std::fs::write(root.join("job-jobsets").join("job-x"), "js-remembered").unwrap();

        let db_path = crate::state::StateDb::db_path(&root);
        let conn = Connection::open(&db_path).unwrap();
        let version = migrate(&conn).unwrap();
        assert_eq!(version, current_version());

        // Distinct jobsets created with creator '' and the right epochs.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobset", [], |r| r.get(0))
            .unwrap();
        // js-a, js-c, js-d, js-remembered
        assert_eq!(count, 4);

        let js_a: (u64, i64) = conn
            .query_row(
                "SELECT epoch, ended FROM jobset WHERE jobset_id = 'js-a'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(js_a, (0, 0));
        let js_d: (u64, i64) = conn
            .query_row(
                "SELECT epoch, ended FROM jobset WHERE jobset_id = 'js-d'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(js_d, (7, 1), "ended marker becomes a tombstone");

        // Members imported, job-b also from the remember-file (idempotent
        // union), job-x from the remember-file only.
        let members_of_js_a: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM jobset_member WHERE jobset_id = 'js-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(members_of_js_a, 2);
        let members_of_js_remembered: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM jobset_member WHERE jobset_id = 'js-remembered'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(members_of_js_remembered, 1);

        // The imported files stay in place (C1 keeps the file readers).
        assert!(root.join("jobsets.json").exists());
        assert!(root.join("job-jobsets").join("job-b").exists());
        assert!(!root.join("jobsets.json.migrated").exists());

        drop(conn);

        // Re-opening the database must not re-import (the migration runs
        // once), and the tables keep whatever they had.
        let conn = Connection::open(&db_path).unwrap();
        let again = migrate(&conn).unwrap();
        assert_eq!(again, version);
        let count_again: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobset", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count_again, 4);
    }

    #[test]
    fn migration_6_without_membership_file_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        std::fs::create_dir_all(&root).unwrap();
        let conn = Connection::open(crate::state::StateDb::db_path(&root)).unwrap();
        migrate(&conn).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobset", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
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

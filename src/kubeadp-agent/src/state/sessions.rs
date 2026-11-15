//! Session store for supervisor and session state.

#[cfg(feature = "server")]
use super::StateDb;
#[cfg(feature = "server")]
use rusqlite::{params, params_from_iter, Connection, OpenFlags};
#[cfg(feature = "server")]
use std::collections::HashMap;
#[cfg(feature = "server")]
use std::time::SystemTime;

#[cfg(feature = "server")]
pub use crate::orchestrate::state::{SessionState, SessionStatus, SupervisorState};

#[cfg(feature = "server")]
const SUPERVISOR_STATE_ROW_VERSION: i64 = 1;

/// Store handle for supervisor state and session states.
#[derive(Debug, Clone)]
pub struct SessionStore {
    #[cfg(feature = "server")]
    db: StateDb,
}

impl SessionStore {
    #[cfg(feature = "server")]
    pub fn new(db: StateDb) -> Self {
        Self { db }
    }

    #[cfg(not(feature = "server"))]
    pub fn new(_db: super::StateDb) -> Self {
        Self {}
    }

    /// Loads the supervisor state via a read-only connection.
    #[cfg(feature = "server")]
    pub fn load(&self) -> Result<Option<SupervisorState>, String> {
        let db_path = StateDb::db_path(self.db.state_root());
        if !db_path.exists() {
            return Ok(None);
        }
        let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| {
                format!(
                    "Failed to open state database {} read-only: {}",
                    db_path.display(),
                    e
                )
            })?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| format!("Failed to set busy timeout on {}: {}", db_path.display(), e))?;

        let (pid, heartbeat, heartbeat_epoch, status, version): (i64, String, i64, String, i64) =
            match conn.query_row(
                "SELECT supervisor_pid, heartbeat, heartbeat_epoch, status, version                  FROM supervisor_state WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            ) {
                Ok(row) => row,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
                Err(e) => {
                    return Err(format!(
                        "Failed to read the supervisor state from {} (is the file corrupted?): {}",
                        db_path.display(),
                        e
                    ));
                }
            };
        if version > SUPERVISOR_STATE_ROW_VERSION {
            return Err(format!(
                "Supervisor state in {} was written by a newer version (schema {} > {})",
                db_path.display(),
                version,
                SUPERVISOR_STATE_ROW_VERSION
            ));
        }

        let mut sessions = HashMap::new();
        {
            let mut stmt = conn
                .prepare(
                    "SELECT name, pid, start_time, start_time_epoch, uptime_secs, restart_count,                             last_exit_reason, status, last_health_result, engine                      FROM session_state ORDER BY name",
                )
                .map_err(|e| {
                    format!(
                        "Failed to read session state from {}: {}",
                        db_path.display(),
                        e
                    )
                })?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<f64>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                })
                .map_err(|e| {
                    format!(
                        "Failed to read session state from {}: {}",
                        db_path.display(),
                        e
                    )
                })?;
            for row in rows {
                let (
                    name,
                    pid,
                    start_time,
                    start_time_epoch,
                    uptime_secs,
                    restart_count,
                    last_exit_reason,
                    status_raw,
                    last_health_result,
                    engine,
                ) = row.map_err(|e| {
                    format!("Corrupted session state in {}: {}", db_path.display(), e)
                })?;
                let status = SessionStatus::parse(&status_raw).ok_or_else(|| {
                    format!(
                        "Corrupted session state in {}: unrecognized status '{}' for session '{}'",
                        db_path.display(),
                        status_raw,
                        name
                    )
                })?;
                sessions.insert(
                    name.clone(),
                    SessionState {
                        name,
                        pid: pid.map(|p| p as u32),
                        start_time,
                        start_time_epoch: start_time_epoch.map(|e| e as u64),
                        uptime_secs,
                        restart_count: restart_count as u32,
                        last_exit_reason,
                        status,
                        last_health_result,
                        engine,
                    },
                );
            }
        }

        Ok(Some(SupervisorState {
            supervisor_pid: pid as u32,
            heartbeat,
            heartbeat_epoch: heartbeat_epoch as u64,
            status,
            sessions,
        }))
    }

    /// Saves the supervisor state in one transaction.
    #[cfg(feature = "server")]
    pub fn save(&self, state: &SupervisorState) -> Result<(), String> {
        let db_path = StateDb::db_path(self.db.state_root());
        let conn = self.db.connection().map_err(|e| {
            format!(
                "Failed to open state database {}: {:#}",
                db_path.display(),
                e
            )
        })?;

        let now = now_epoch() as i64;
        let tx = conn.unchecked_transaction().map_err(|e| {
            format!(
                "Failed to begin a state transaction on {}: {}",
                db_path.display(),
                e
            )
        })?;

        tx.execute(
            "INSERT INTO supervisor_state (
                id, supervisor_pid, started, heartbeat, heartbeat_epoch, status, version, updated_at
            ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
                supervisor_pid = excluded.supervisor_pid,
                started = excluded.started,
                heartbeat = excluded.heartbeat,
                heartbeat_epoch = excluded.heartbeat_epoch,
                status = excluded.status,
                version = excluded.version,
                updated_at = excluded.updated_at",
            params![
                state.supervisor_pid,
                state.heartbeat,
                state.heartbeat,
                state.heartbeat_epoch as i64,
                state.status,
                SUPERVISOR_STATE_ROW_VERSION,
                now,
            ],
        )
        .map_err(|e| format!("Failed to save the supervisor state row: {}", e))?;

        for session in state.sessions.values() {
            tx.execute(
                "INSERT INTO session_state (
                    name, pid, start_time, start_time_epoch, uptime_secs, restart_count,
                    last_exit_reason, status, last_health_result, engine, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ON CONFLICT(name) DO UPDATE SET
                    pid = excluded.pid,
                    start_time = excluded.start_time,
                    start_time_epoch = excluded.start_time_epoch,
                    uptime_secs = excluded.uptime_secs,
                    restart_count = excluded.restart_count,
                    last_exit_reason = excluded.last_exit_reason,
                    status = excluded.status,
                    last_health_result = excluded.last_health_result,
                    engine = excluded.engine,
                    updated_at = excluded.updated_at",
                params![
                    session.name,
                    session.pid.map(|p| p as i64),
                    session.start_time,
                    session.start_time_epoch.map(|e| e as i64),
                    session.uptime_secs,
                    session.restart_count,
                    session.last_exit_reason,
                    session.status.to_string(),
                    session.last_health_result,
                    session.engine,
                    now,
                ],
            )
            .map_err(|e| format!("Failed to save session state for '{}': {}", session.name, e))?;
        }

        if state.sessions.is_empty() {
            tx.execute("DELETE FROM session_state", [])
                .map_err(|e| format!("Failed to clear session state rows: {}", e))?;
        } else {
            let names: Vec<String> = state.sessions.keys().cloned().collect();
            let placeholders: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
            let sql = format!(
                "DELETE FROM session_state WHERE name NOT IN ({})",
                placeholders.join(", ")
            );
            tx.execute(&sql, params_from_iter(names))
                .map_err(|e| format!("Failed to prune session state rows: {}", e))?;
        }

        tx.commit().map_err(|e| {
            format!(
                "Failed to commit the supervisor state to {}: {}",
                db_path.display(),
                e
            )
        })?;
        Ok(())
    }
}

#[cfg(feature = "server")]
fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

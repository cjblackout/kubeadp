//! RunRegistry view over jobs, sessions, and run_meta tables.
//!
//! Replaces the volatile in-memory registry: runs are resolved directly from
//! the authoritative `jobs` or `session_state` tables and merged with
//! optional metadata (`paused`, `compact_summary`, `forked_from`) stored in
//! the `run_meta` table (migration 5).

use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use super::StateDb;
use crate::engine::run::{Run, RunKind, RunStatus};

/// Metadata stored in `run_meta` for a run (job or session).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RunMeta {
    pub run_id: String,
    pub kind: String,
    pub paused: bool,
    pub compact_summary: Option<String>,
    pub compacted_at: Option<i64>,
    pub forked_from: Option<String>,
    pub updated_at: i64,
}

/// Store handle for querying and updating `run_meta` rows.
#[derive(Debug, Clone)]
pub struct RunMetaStore {
    db: StateDb,
}

impl RunMetaStore {
    pub fn new(db: StateDb) -> Self {
        Self { db }
    }

    /// Loads the `run_meta` for `run_id`, if it exists.
    pub fn get(&self, run_id: &str) -> Result<Option<RunMeta>> {
        let conn = self.db.connection()?;
        Self::get_with_conn(&conn, run_id)
    }

    pub(crate) fn get_with_conn(conn: &Connection, run_id: &str) -> Result<Option<RunMeta>> {
        let mut stmt = conn.prepare(
            "SELECT run_id, kind, paused, compact_summary, compacted_at, forked_from, updated_at \
             FROM run_meta WHERE run_id = ?1",
        )?;
        let mut rows = stmt.query(params![run_id])?;
        if let Some(row) = rows.next()? {
            let paused_int: i64 = row.get(2)?;
            Ok(Some(RunMeta {
                run_id: row.get(0)?,
                kind: row.get(1)?,
                paused: paused_int != 0,
                compact_summary: row.get(3)?,
                compacted_at: row.get(4)?,
                forked_from: row.get(5)?,
                updated_at: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// Sets the paused flag in `run_meta` (inserting or updating).
    pub fn set_paused(&self, run_id: &str, kind: RunKind, paused: bool) -> Result<()> {
        let conn = self.db.connection()?;
        let now = chrono::Utc::now().timestamp();
        let kind_str = match kind {
            RunKind::Job => "job",
            RunKind::Session => "session",
        };
        let paused_int = if paused { 1 } else { 0 };

        conn.execute(
            "INSERT INTO run_meta (run_id, kind, paused, updated_at) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(run_id) DO UPDATE SET \
                 paused = excluded.paused, \
                 updated_at = excluded.updated_at",
            params![run_id, kind_str, paused_int, now],
        )?;
        Ok(())
    }

    /// Records a compaction summary and timestamp in `run_meta`.
    pub fn record_compact(&self, run_id: &str, kind: RunKind, summary: &str) -> Result<()> {
        let conn = self.db.connection()?;
        let now = chrono::Utc::now().timestamp();
        let kind_str = match kind {
            RunKind::Job => "job",
            RunKind::Session => "session",
        };

        conn.execute(
            "INSERT INTO run_meta (run_id, kind, compact_summary, compacted_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(run_id) DO UPDATE SET \
                 compact_summary = excluded.compact_summary, \
                 compacted_at = excluded.compacted_at, \
                 updated_at = excluded.updated_at",
            params![run_id, kind_str, summary, now, now],
        )?;
        Ok(())
    }

    /// Records fork lineage (`new_id` was forked from `from_id`).
    pub fn record_fork(&self, new_id: &str, from_id: &str, kind: RunKind) -> Result<()> {
        let conn = self.db.connection()?;
        let now = chrono::Utc::now().timestamp();
        let kind_str = match kind {
            RunKind::Job => "job",
            RunKind::Session => "session",
        };

        conn.execute(
            "INSERT INTO run_meta (run_id, kind, forked_from, updated_at) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(run_id) DO UPDATE SET \
                 forked_from = excluded.forked_from, \
                 updated_at = excluded.updated_at",
            params![new_id, kind_str, from_id, now],
        )?;
        Ok(())
    }
}

/// The unified run registry view over jobs, sessions, and run_meta tables.
#[derive(Debug, Clone)]
pub struct RunRegistry {
    state_root: PathBuf,
    mem: Arc<RwLock<HashMap<String, Run>>>,
}

impl RunRegistry {
    pub fn new(db: StateDb) -> Self {
        Self {
            state_root: db.state_root().to_path_buf(),
            mem: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn lazy(state_root: &Path) -> Self {
        Self {
            state_root: state_root.to_path_buf(),
            mem: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn db(&self) -> Option<StateDb> {
        let db_file = StateDb::db_path(&self.state_root);
        if db_file.exists() {
            StateDb::open(&self.state_root).ok()
        } else {
            None
        }
    }

    fn ensure_db(&self) -> Result<StateDb> {
        StateDb::open(&self.state_root)
    }

    /// Looks up a run by id across jobs and sessions, merged with run_meta.
    pub fn get(&self, id: &str) -> Result<Option<Run>> {
        if let Some(db) = self.db() {
            // Try resolving as a job first (exact match or prefix match).
            if let Ok(job) = db.jobs().resolve(id) {
                let engine = match job.engine.as_deref() {
                    Some(e) => e.to_string(),
                    None => "agy".to_string(),
                };
                let mut run = Run::from_job(&job, &engine);
                // Lazy import from worktree files if present
                self.lazy_import_worktree_files(&run);

                if let Ok(Some(meta)) = db.runs_meta().get(&job.job_id) {
                    apply_meta_to_run(&mut run, &meta);
                }
                return Ok(Some(run));
            }

            // Next try resolving as a session.
            if let Ok(conn) = db.connection() {
                let mut stmt = conn.prepare(
                    "SELECT name, pid, start_time, start_time_epoch, uptime_secs, restart_count, \
                     last_exit_reason, status, last_health_result, engine \
                     FROM session_state WHERE name = ?1",
                )?;
                let mut rows = stmt.query(params![id])?;
                if let Some(row) = rows.next()? {
                    let name: String = row.get(0)?;
                    let status_raw: String = row.get(7)?;
                    let engine: Option<String> = row.get(9)?;
                    let status = match status_raw.as_str() {
                        "running" => RunStatus::Running,
                        "starting" => RunStatus::Starting,
                        "backoff" => RunStatus::Backoff,
                        "stopped" => RunStatus::Stopped,
                        "failed" => RunStatus::Failed,
                        _ => RunStatus::Pending,
                    };
                    let engine_str = engine.unwrap_or_else(|| "claude".to_string());
                    let mut run = Run::new(name.clone(), RunKind::Session, db.state_root());
                    run.engine = engine_str;
                    run.status = status;
                    run.meta = serde_json::json!({
                        "session_name": name,
                        "pid": row.get::<_, Option<i64>>(1)?,
                        "start_time": row.get::<_, Option<String>>(2)?,
                        "status": status_raw,
                    });

                    self.lazy_import_worktree_files(&run);

                    if let Ok(Some(meta)) = db.runs_meta().get(&name) {
                        apply_meta_to_run(&mut run, &meta);
                    }
                    return Ok(Some(run));
                }
            }
        }

        // Check if there is an in-memory registered run (e.g. from tests or standalone runs).
        if let Some(mut run) = self
            .mem
            .read()
            .map_err(|e| anyhow::anyhow!("mem lock poisoned: {e}"))?
            .get(id)
            .cloned()
        {
            if let Some(db) = self.db() {
                if let Ok(Some(meta)) = db.runs_meta().get(id) {
                    apply_meta_to_run(&mut run, &meta);
                }
            }
            return Ok(Some(run));
        }

        // Check if there is a run_meta row without job/session.
        if let Some(db) = self.db() {
            if let Ok(Some(meta)) = db.runs_meta().get(id) {
                let kind = match meta.kind.as_str() {
                    "session" => RunKind::Session,
                    _ => RunKind::Job,
                };
                let mut run = Run::new(meta.run_id.clone(), kind, db.state_root());
                apply_meta_to_run(&mut run, &meta);
                return Ok(Some(run));
            }
        }

        Ok(None)
    }

    /// Lists runs, optionally filtered by `kind`.
    pub fn list(&self, kind: Option<RunKind>) -> Result<Vec<Run>> {
        let mut runs = Vec::new();

        let include_jobs = kind.is_none_or(|k| k == RunKind::Job);
        let include_sessions = kind.is_none_or(|k| k == RunKind::Session);

        if let Some(db) = self.db() {
            if include_jobs {
                if let Ok(jobs) = db.jobs().list(None) {
                    for job in jobs {
                        let engine = match job.engine.as_deref() {
                            Some(e) => e.to_string(),
                            None => "agy".to_string(),
                        };
                        let mut run = Run::from_job(&job, &engine);
                        if let Ok(Some(meta)) = db.runs_meta().get(&job.job_id) {
                            apply_meta_to_run(&mut run, &meta);
                        }
                        runs.push(run);
                    }
                }
            }

            if include_sessions {
                if let Ok(conn) = db.connection() {
                    if let Ok(mut stmt) = conn.prepare(
                        "SELECT name, pid, start_time, start_time_epoch, uptime_secs, restart_count, \
                         last_exit_reason, status, last_health_result, engine \
                         FROM session_state ORDER BY name ASC",
                    ) {
                        let rows = stmt.query_map([], |row| {
                            let name: String = row.get(0)?;
                            let status_raw: String = row.get(7)?;
                            let engine: Option<String> = row.get(9)?;
                            Ok((name, status_raw, engine))
                        })?;
                        for row in rows.flatten() {
                            let (name, status_raw, engine) = row;
                            let status = match status_raw.as_str() {
                                "running" => RunStatus::Running,
                                "starting" => RunStatus::Starting,
                                "backoff" => RunStatus::Backoff,
                                "stopped" => RunStatus::Stopped,
                                "failed" => RunStatus::Failed,
                                _ => RunStatus::Pending,
                            };
                            let engine_str = engine.unwrap_or_else(|| "claude".to_string());
                            let mut run =
                                Run::new(name.clone(), RunKind::Session, db.state_root());
                            run.engine = engine_str;
                            run.status = status;
                            if let Ok(Some(meta)) = db.runs_meta().get(&name) {
                                apply_meta_to_run(&mut run, &meta);
                            }
                            runs.push(run);
                        }
                    }
                }
            }
        }

        // Also include any in-memory runs not already present in the list
        if let Ok(mem) = self.mem.read() {
            for run in mem.values() {
                if kind.is_none_or(|k| k == run.kind) && !runs.iter().any(|r| r.id == run.id) {
                    runs.push(run.clone());
                }
            }
        }

        Ok(runs)
    }

    /// Sets the paused flag on a run.
    pub fn set_paused(&self, id: &str, paused: bool) -> Result<()> {
        let kind = if let Ok(Some(run)) = self.get(id) {
            run.kind
        } else {
            RunKind::Job
        };
        let db = self.ensure_db()?;
        db.runs_meta().set_paused(id, kind, paused)
    }

    /// Records compact summary on a run.
    pub fn record_compact(&self, id: &str, summary: &str) -> Result<()> {
        let kind = if let Ok(Some(run)) = self.get(id) {
            run.kind
        } else {
            RunKind::Job
        };
        let db = self.ensure_db()?;
        db.runs_meta().record_compact(id, kind, summary)
    }

    /// Records fork lineage.
    pub fn record_fork(&self, new_id: &str, from_id: &str, kind: RunKind) -> Result<()> {
        let db = self.ensure_db()?;
        db.runs_meta().record_fork(new_id, from_id, kind)
    }

    /// Registers a run (in memory and in run_meta if DB exists, for test fixtures or standalone runs).
    pub fn register(&self, run: Run) -> Result<()> {
        if let Ok(mut mem) = self.mem.write() {
            mem.insert(run.id.clone(), run.clone());
        }

        if let Ok(db) = self.ensure_db() {
            let conn = db.connection()?;
            let now = chrono::Utc::now().timestamp();
            let kind_str = match run.kind {
                RunKind::Job => "job",
                RunKind::Session => "session",
            };
            let paused = if run.status == RunStatus::Paused {
                1
            } else {
                0
            };
            let forked_from = run.meta.get("forked_from").and_then(|v| v.as_str());

            conn.execute(
                "INSERT INTO run_meta (run_id, kind, paused, forked_from, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(run_id) DO UPDATE SET \
                     kind = excluded.kind, \
                     paused = excluded.paused, \
                     forked_from = excluded.forked_from, \
                     updated_at = excluded.updated_at",
                params![run.id, kind_str, paused, forked_from, now],
            )?;
        }
        Ok(())
    }

    /// Unregisters a run from memory and run_meta. Returns whether it was present.
    pub fn unregister(&self, id: &str) -> bool {
        let mut removed = false;
        if let Ok(mut mem) = self.mem.write() {
            if mem.remove(id).is_some() {
                removed = true;
            }
        }
        if let Some(db) = self.db() {
            if let Ok(conn) = db.connection() {
                if let Ok(n) = conn.execute("DELETE FROM run_meta WHERE run_id = ?1", params![id]) {
                    if n > 0 {
                        removed = true;
                    }
                }
            }
        }
        removed
    }

    /// Lazy import from worktree legacy files `.engine-compact.json` and `.engine-message-queue`.
    fn lazy_import_worktree_files(&self, run: &Run) {
        let compact_file = run.workdir.join(".engine-compact.json");
        if compact_file.is_file() {
            if let Ok(content) = std::fs::read_to_string(&compact_file) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
                    let summary = val.get("summary").and_then(|s| s.as_str()).unwrap_or("");
                    let _ = self.record_compact(&run.id, summary);
                }
            }
            let _ = std::fs::remove_file(compact_file);
        }

        let queue_file = run.workdir.join(".engine-message-queue");
        if queue_file.is_file() {
            let _ = std::fs::remove_file(queue_file);
        }
    }
}

fn apply_meta_to_run(run: &mut Run, meta: &RunMeta) {
    if meta.paused {
        run.status = RunStatus::Paused;
    }
    if let serde_json::Value::Object(ref mut map) = run.meta {
        map.insert("paused".to_string(), serde_json::json!(meta.paused));
        if let Some(ref cf) = meta.compact_summary {
            map.insert("compact_summary".to_string(), serde_json::json!(cf));
        }
        if let Some(ts) = meta.compacted_at {
            map.insert("compacted_at".to_string(), serde_json::json!(ts));
        }
        if let Some(ref f) = meta.forked_from {
            map.insert("forked_from".to_string(), serde_json::json!(f));
        }
    }
}

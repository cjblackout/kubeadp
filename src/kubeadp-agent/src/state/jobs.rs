//! Job store for managing job rows in `<state_root>/state.sqlite`.

use super::StateDb;
use anyhow::{bail, Context, Result};
use rusqlite::params;
use std::collections::HashSet;

pub(crate) use crate::core::job::row_to_job;
pub use crate::core::job::{JobMetadata, JobStatus};

/// Store handle for querying and updating job records.
#[derive(Debug, Clone)]
pub struct JobStore {
    db: StateDb,
}

impl JobStore {
    pub fn new(db: StateDb) -> Self {
        Self { db }
    }

    /// Saves (inserts or updates) a job's metadata.
    pub fn save(&self, job: &JobMetadata) -> Result<()> {
        let add_dirs_json =
            serde_json::to_string(&job.add_dirs).context("Failed to serialize add_dirs to JSON")?;
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        conn.execute(
            "INSERT INTO jobs (
                job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode,
                add_dirs, print_timeout, created_at, worktree_path, branch, base_ref,
                conversation_id, retry_count, pid, exit_code, status, hindsight,
                auto_recall, auto_retain, retry_prompt, read_only, external_worktree,
                pid_start_time, hindsight_bank, proxy, max_runtime, started_at,
                hindsight_query, sandbox_info, engine
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33)
            ON CONFLICT(job_id) DO UPDATE SET
                project = excluded.project,
                prompt = excluded.prompt,
                resolved_prompt = excluded.resolved_prompt,
                model = excluded.model,
                effort = excluded.effort,
                sandbox = excluded.sandbox,
                mode = excluded.mode,
                add_dirs = excluded.add_dirs,
                print_timeout = excluded.print_timeout,
                created_at = excluded.created_at,
                worktree_path = excluded.worktree_path,
                branch = excluded.branch,
                base_ref = excluded.base_ref,
                conversation_id = excluded.conversation_id,
                retry_count = excluded.retry_count,
                pid = excluded.pid,
                exit_code = excluded.exit_code,
                status = excluded.status,
                hindsight = excluded.hindsight,
                auto_recall = excluded.auto_recall,
                auto_retain = excluded.auto_retain,
                retry_prompt = excluded.retry_prompt,
                read_only = excluded.read_only,
                external_worktree = excluded.external_worktree,
                pid_start_time = excluded.pid_start_time,
                hindsight_bank = excluded.hindsight_bank,
                proxy = excluded.proxy,
                max_runtime = excluded.max_runtime,
                started_at = excluded.started_at,
                hindsight_query = excluded.hindsight_query,
                sandbox_info = excluded.sandbox_info,
                engine = excluded.engine",
            params![
                job.job_id,
                job.project,
                job.prompt,
                job.resolved_prompt,
                job.model,
                job.effort,
                job.sandbox as i64,
                job.mode,
                add_dirs_json,
                job.print_timeout,
                job.created_at.timestamp_nanos_opt().unwrap_or(0),
                job.worktree_path.to_string_lossy().to_string(),
                job.branch,
                job.base_ref,
                job.conversation_id,
                job.retry_count,
                job.pid,
                job.exit_code,
                job.status.map(|s| s.as_str().to_string()),
                job.hindsight.map(|b| b as i64),
                job.auto_recall.map(|b| b as i64),
                job.auto_retain.map(|b| b as i64),
                job.retry_prompt,
                job.read_only as i64,
                job.external_worktree as i64,
                job.pid_start_time.map(|t| t as i64),
                job.hindsight_bank,
                job.proxy,
                job.max_runtime.and_then(|d: chrono::Duration| d.num_nanoseconds()),
                job.started_at.and_then(|t| t.timestamp_nanos_opt()),
                job.hindsight_query,
                job.sandbox_info
                    .as_ref()
                    .and_then(|s| serde_json::to_string(s).ok()),
                job.engine,
            ],
        )
        .with_context(|| {
            format!(
                "Failed to save job metadata '{}' to {}",
                job.job_id,
                StateDb::db_path(state_root).display()
            )
        })?;
        Ok(())
    }

    /// Inserts a brand new job, failing if the job id already exists.
    pub fn insert_new(&self, job: &JobMetadata) -> Result<()> {
        let add_dirs_json =
            serde_json::to_string(&job.add_dirs).context("Failed to serialize add_dirs to JSON")?;
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        conn.execute(
            "INSERT INTO jobs (
                job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode,
                add_dirs, print_timeout, created_at, worktree_path, branch, base_ref,
                conversation_id, retry_count, pid, exit_code, status, hindsight,
                auto_recall, auto_retain, retry_prompt, read_only, external_worktree,
                pid_start_time, hindsight_bank, proxy, max_runtime, started_at,
                hindsight_query, sandbox_info, engine
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33)",
            params![
                job.job_id,
                job.project,
                job.prompt,
                job.resolved_prompt,
                job.model,
                job.effort,
                job.sandbox as i64,
                job.mode,
                add_dirs_json,
                job.print_timeout,
                job.created_at.timestamp_nanos_opt().unwrap_or(0),
                job.worktree_path.to_string_lossy().to_string(),
                job.branch,
                job.base_ref,
                job.conversation_id,
                job.retry_count,
                job.pid,
                job.exit_code,
                job.status.map(|s| s.as_str().to_string()),
                job.hindsight.map(|b| b as i64),
                job.auto_recall.map(|b| b as i64),
                job.auto_retain.map(|b| b as i64),
                job.retry_prompt,
                job.read_only as i64,
                job.external_worktree as i64,
                job.pid_start_time.map(|t| t as i64),
                job.hindsight_bank,
                job.proxy,
                job.max_runtime.and_then(|d: chrono::Duration| d.num_nanoseconds()),
                job.started_at.and_then(|t| t.timestamp_nanos_opt()),
                job.hindsight_query,
                job.sandbox_info
                    .as_ref()
                    .and_then(|s| serde_json::to_string(s).ok()),
                job.engine,
            ],
        )
        .map_err(|e| {
            if let rusqlite::Error::SqliteFailure(ref sqlite_err, _) = e {
                if sqlite_err.code == rusqlite::ErrorCode::ConstraintViolation {
                    return crate::core::error::AgentRunError::conflict(
                        "job_id_already_exists",
                        format!(
                            "Refusing to insert new job '{}': job id '{}' already exists in {} (a generated id collision must fail, never overwrite the existing job's row)",
                            job.job_id,
                            job.job_id,
                            StateDb::db_path(state_root).display()
                        ),
                    )
                    .into();
                }
            }
            anyhow::Error::from(e)
        })
        .with_context(|| {
            format!(
                "Failed to insert job metadata '{}' into {}",
                job.job_id,
                StateDb::db_path(state_root).display()
            )
        })?;
        Ok(())
    }

    /// Loads a job record by exact job id.
    pub fn load(&self, job_id: &str) -> Result<JobMetadata> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let mut stmt = conn
            .prepare("SELECT job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode, add_dirs, print_timeout, created_at, worktree_path, branch, base_ref, conversation_id, retry_count, pid, exit_code, status, hindsight, auto_recall, auto_retain, retry_prompt, read_only, external_worktree, pid_start_time, hindsight_bank, proxy, max_runtime, started_at, hindsight_query, sandbox_info, engine FROM jobs WHERE job_id = ?1")
            .with_context(|| format!("Failed to read job metadata for '{}' from {}", job_id, StateDb::db_path(state_root).display()))?;
        let mut rows = stmt
            .query_map(params![job_id], row_to_job)
            .with_context(|| {
                format!(
                    "Failed to query job '{}' from {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        match rows.next() {
            Some(Ok(meta)) => Ok(meta),
            Some(Err(e)) => bail!(
                "Corrupted or unreadable job metadata for '{}' in {}: {}",
                job_id,
                StateDb::db_path(state_root).display(),
                e
            ),
            None => {
                let err = crate::core::error::AgentRunError::not_found(
                    "job_not_found",
                    format!(
                        "No such job found: '{}' (no job record in {})",
                        job_id,
                        StateDb::db_path(state_root).display()
                    ),
                );
                Err(anyhow::Error::from(err))
            }
        }
    }

    /// Resolves a job id prefix (or exact match).
    pub fn resolve(&self, job_id: &str) -> Result<JobMetadata> {
        if let Ok(meta) = self.load(job_id) {
            return Ok(meta);
        }
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let mut stmt = conn
            .prepare(
                "SELECT job_id FROM jobs WHERE job_id LIKE ?1 ESCAPE '\\' ORDER BY length(job_id)",
            )
            .with_context(|| format!("Failed to resolve job prefix '{}'", job_id))?;
        let escaped_prefix = format!("{}%", crate::core::job::escape_like(job_id));
        let ids = stmt
            .query_map(params![escaped_prefix], |r| r.get(0))
            .with_context(|| {
                format!(
                    "Failed to query job prefix '{}' in {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?
            .collect::<std::result::Result<Vec<String>, _>>()
            .with_context(|| format!("Failed to read job prefix matches for '{}'", job_id))?;
        match ids.len() {
            0 => {
                let err = crate::core::error::AgentRunError::not_found(
                    "job_not_found",
                    format!(
                        "No such job found: '{}' (no job record in {})",
                        job_id,
                        StateDb::db_path(state_root).display()
                    ),
                );
                Err(anyhow::Error::from(err))
            }
            1 => self.load(&ids[0]),
            _ => {
                let err = crate::core::error::AgentRunError::invalid(
                    "job_id_ambiguous",
                    format!(
                        "Job ID prefix '{}' is ambiguous; matches: {}",
                        job_id,
                        ids.join(", ")
                    ),
                );
                Err(anyhow::Error::from(err))
            }
        }
    }

    /// Deletes a job record from the state database.
    pub fn delete(&self, job_id: &str) -> Result<()> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        conn.execute("DELETE FROM jobs WHERE job_id = ?1", params![job_id])
            .with_context(|| {
                format!(
                    "Failed to delete job record '{}' from {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        Ok(())
    }

    /// Checks whether a job id exists.
    pub fn exists(&self, job_id: &str) -> Result<bool> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = ?1)",
                params![job_id],
                |row| row.get(0),
            )
            .with_context(|| {
                format!(
                    "Failed to check job id '{}' in {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        Ok(exists)
    }

    /// Lists all jobs, optionally filtered by project, sorted newest first.
    pub fn list(&self, project_filter: Option<&str>) -> Result<Vec<JobMetadata>> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let mut stmt = conn
            .prepare(
                "SELECT job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode, add_dirs, print_timeout, created_at, worktree_path, branch, base_ref, conversation_id, retry_count, pid, exit_code, status, hindsight, auto_recall, auto_retain, retry_prompt, read_only, external_worktree, pid_start_time, hindsight_bank, proxy, max_runtime, started_at, hindsight_query, sandbox_info, engine \
                  FROM jobs WHERE ?1 IS NULL OR project = ?1 ORDER BY created_at DESC, job_id DESC",
            )
            .with_context(|| format!("Failed to prepare job list query in {}", StateDb::db_path(state_root).display()))?;

        let mut results = Vec::new();
        let rows = stmt
            .query_map(params![project_filter], row_to_job)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to query jobs from {}: {}",
                    StateDb::db_path(state_root).display(),
                    e
                )
            })?;
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Saves the final status for a job unless it was already marked killed.
    pub fn save_final_status_unless_killed(
        &self,
        job_id: &str,
        status: JobStatus,
        exit_code: Option<i32>,
        conversation_id: Option<&str>,
        expected_retry_count: Option<u32>,
    ) -> Result<bool> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();

        // conversation_id is persisted unconditionally (when the runner actually
        // extracted one) regardless of the status race below: it's discovered
        // from the log at the same moment as the exit code, a killed job can
        // still have a real conversation_id worth recording, and unlike
        // status/exit_code it was never the subject of the kill.rs/runner race
        // this function exists to close -- skipping it here would silently lose
        // it (clean/gc's agy conversation cleanup needs it to purge conversation
        // artifacts, and only extracts it from the log itself as a fallback in
        // `retry`, not in `clean`/`gc`).
        if let Some(conv_id) = conversation_id {
            conn.execute(
                "UPDATE jobs SET conversation_id = ?2 WHERE job_id = ?1",
                params![job_id, conv_id],
            )
            .with_context(|| {
                format!(
                    "Failed to save conversation_id for job '{}' in {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        }

        let updated = match expected_retry_count {
            Some(expected) => conn.execute(
                "UPDATE jobs SET status = ?2, exit_code = ?3 \
                 WHERE job_id = ?1 AND retry_count = ?4 \
                 AND (status IS NULL OR status != 'killed')",
                params![job_id, status.as_str().to_string(), exit_code, expected],
            ),
            None => conn.execute(
                "UPDATE jobs SET status = ?2, exit_code = ?3 \
                 WHERE job_id = ?1 AND (status IS NULL OR status != 'killed')",
                params![job_id, status.as_str().to_string(), exit_code],
            ),
        }
        .with_context(|| {
            format!(
                "Failed to save final status for job '{}' in {}",
                job_id,
                StateDb::db_path(state_root).display()
            )
        })?;
        Ok(updated > 0)
    }

    /// Marks a job killed.
    pub fn mark_job_killed(&self, job_id: &str, expected_retry_count: u32) -> Result<bool> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let updated = conn
            .execute(
                "UPDATE jobs SET status = 'killed', exit_code = 137 \
                 WHERE job_id = ?1 AND retry_count = ?2 \
                 AND (status IS NULL OR status NOT IN ('done', 'killed')) \
                 AND (exit_code IS NULL OR exit_code != 0)",
                params![job_id, expected_retry_count],
            )
            .with_context(|| {
                format!(
                    "Failed to mark job '{}' as killed in {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        Ok(updated > 0)
    }

    /// Updates sandbox_info for a job.
    pub fn set_job_sandbox(
        &self,
        job_id: &str,
        sandbox: &crate::sandbox::SandboxInfo,
    ) -> Result<()> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let json =
            serde_json::to_string(sandbox).context("Failed to serialize sandbox info to JSON")?;
        conn.execute(
            "UPDATE jobs SET sandbox_info = ?2 WHERE job_id = ?1",
            params![job_id, json],
        )
        .with_context(|| {
            format!(
                "Failed to update sandbox info for job '{}' in {}",
                job_id,
                StateDb::db_path(state_root).display()
            )
        })?;
        Ok(())
    }

    /// Restores a conversation ID if currently NULL.
    pub fn restore_conversation_id(&self, job_id: &str, conversation_id: &str) -> Result<bool> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let updated = conn
            .execute(
                "UPDATE jobs SET conversation_id = ?1 WHERE job_id = ?2 AND conversation_id IS NULL",
                params![conversation_id, job_id],
            )
            .with_context(|| {
                format!(
                    "Failed to record recovered conversation_id '{}' for job '{}' in {}",
                    conversation_id,
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        Ok(updated > 0)
    }

    /// Atomically claims a retry attempt by incrementing retry_count.
    pub fn claim_retry_attempt(&self, job_id: &str, expected_retry_count: u32) -> Result<()> {
        let conn = self.db.connection()?;
        let state_root = self.db.state_root();
        let updated = conn
            .execute(
                "UPDATE jobs SET retry_count = retry_count + 1, status = ?2 \
                  WHERE job_id = ?1 AND retry_count = ?3",
                params![
                    job_id,
                    JobStatus::Running.as_str().to_string(),
                    expected_retry_count
                ],
            )
            .with_context(|| {
                format!(
                    "Failed to claim retry attempt for job '{}' in {}",
                    job_id,
                    StateDb::db_path(state_root).display()
                )
            })?;
        if updated == 0 {
            let what = match self.load(job_id) {
                Ok(c) if c.retry_count != expected_retry_count => format!(
                    "another retry already claimed a newer attempt (retry_count {} -> {})",
                    expected_retry_count, c.retry_count
                ),
                Ok(c) => format!(
                    "the job's retry_count is {}, expected {}",
                    c.retry_count, expected_retry_count
                ),
                Err(_) => "the job no longer exists".to_string(),
            };
            let err = crate::core::error::AgentRunError::conflict(
                "retry_already_claimed",
                format!("Failed to claim retry attempt for job '{job_id}': {what}"),
            );
            return Err(anyhow::Error::from(err));
        }
        Ok(())
    }

    /// Returns all claimed non-null conversation IDs.
    pub fn claimed_conversation_ids(&self) -> HashSet<String> {
        let Ok(conn) = self.db.connection() else {
            return HashSet::new();
        };
        let Ok(mut stmt) =
            conn.prepare("SELECT conversation_id FROM jobs WHERE conversation_id IS NOT NULL")
        else {
            return HashSet::new();
        };
        stmt.query_map([], |row| row.get::<_, String>(0))
            .ok()
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }
}

//! SQLite-backed jobset store (HL-125 Stream 3, C1).
//!
//! The `jobset` and `jobset_member` tables (migration 6, SPEC §2.3 / §7.2)
//! hold ids and epochs only -- never a secret, a proof or the root key.
//! Secrets and member proofs are derived statelessly from the root key at
//! launch ([`crate::core::jobset::derive_jobset_secret`],
//! [`crate::core::jobset::derive_member_proof`]).
//!
//! C1 keeps the in-memory [`crate::core::jobset::JobsetRegistry`] as the read
//! and verification path (no authentication behaviour change). This store is
//! the persistence target: writes are mirrored in by the registry hooks and
//! it is what the C5 flag-day later reads. Migration 6 also performs the
//! one-time idempotent import of the legacy `jobsets.json` membership file
//! and the `job-jobsets/` directory into these tables.

use super::StateDb;
use anyhow::{bail, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::path::Path;

/// One jobset row: identity and epoch only (never a secret).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jobset {
    pub jobset_id: String,
    pub creator_agent_id: String,
    pub epoch: u64,
    pub ended: bool,
}

/// Store handle for the `jobset` / `jobset_member` tables.
#[derive(Debug, Clone)]
pub struct JobsetStore {
    db: StateDb,
}

impl JobsetStore {
    pub fn new(db: StateDb) -> Self {
        Self { db }
    }

    /// Opens the store for an explicit state root (used by the registry
    /// mirror hooks, which hold only the state root path).
    pub fn open_path(state_root: &Path) -> Result<Self> {
        Ok(Self::new(StateDb::open(state_root)?))
    }

    /// Ensures `agent` has its own default jobset (jobset id == agent id,
    /// creator == agent, SPEC §7.2 item 1): creates it on first call and is
    /// a no-op afterwards. Returns the jobset.
    pub fn ensure_default(&self, agent: &str) -> Result<Jobset> {
        let conn = self.db.connection()?;
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO jobset (jobset_id, creator_agent_id, epoch, ended, created_at) \
             VALUES (?1, ?1, 0, 0, ?2) \
             ON CONFLICT(jobset_id) DO NOTHING",
            params![agent, now],
        )
        .context("Failed to create the default jobset row")?;
        conn.execute(
            "INSERT INTO jobset_member (jobset_id, agent_id, joined_at) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(jobset_id, agent_id) DO NOTHING",
            params![agent, agent, now],
        )
        .context("Failed to add the default membership row")?;
        let jobset = conn
            .query_row(
                "SELECT jobset_id, creator_agent_id, epoch, ended \
                 FROM jobset WHERE jobset_id = ?1",
                params![agent],
                Self::row,
            )
            .optional()
            .with_context(|| format!("Failed to load default jobset '{agent}'"))?
            .ok_or_else(|| anyhow::anyhow!("default jobset '{agent}' missing after create"))?;
        Ok(jobset)
    }

    /// The jobset row for `jobset_id`, if any.
    pub fn get(&self, jobset_id: &str) -> Result<Option<Jobset>> {
        let conn = self.db.connection()?;
        let jobset = conn
            .query_row(
                "SELECT jobset_id, creator_agent_id, epoch, ended \
                 FROM jobset WHERE jobset_id = ?1",
                params![jobset_id],
                Self::row,
            )
            .optional()
            .with_context(|| format!("Failed to load jobset '{jobset_id}'"))?;
        Ok(jobset)
    }

    fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Jobset> {
        let ended: i64 = row.get(3)?;
        Ok(Jobset {
            jobset_id: row.get(0)?,
            creator_agent_id: row.get(1)?,
            epoch: row.get(2)?,
            ended: ended != 0,
        })
    }

    /// Creates (or re-adopts) a jobset this host does not have a creator row
    /// for, e.g. a caller-chosen foreign jobset (SPEC §7.2 item 5). Refuses a
    /// jobset that is already ended (`JobsetEnded`) or a row at a higher
    /// epoch than `epoch` (`JobsetEnded` semantics: the presented epoch is
    /// stale).
    pub fn upsert_foreign(
        &self,
        jobset_id: &str,
        creator: &str,
        epoch: u64,
    ) -> Result<Jobset, JoinError> {
        let conn = self
            .db
            .connection()
            .map_err(|e| JoinError::State(anyhow::anyhow!("{e:#}")))?;
        let now = chrono::Utc::now().timestamp();
        let existing: Option<(u64, i64, String)> = conn
            .query_row(
                "SELECT epoch, ended, creator_agent_id FROM jobset WHERE jobset_id = ?1",
                params![jobset_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| JoinError::State(anyhow::Error::from(e)))?;
        if let Some((existing_epoch, ended, existing_creator)) = existing {
            if ended != 0 {
                return Err(JoinError::JobsetEnded(jobset_id.to_string()));
            }
            if existing_epoch > epoch {
                return Err(JoinError::JobsetEnded(jobset_id.to_string()));
            }
            // Keep the existing row: a present, unended jobset is already
            // adoptable; a creator upgrade is a C3 concern.
            return Ok(Jobset {
                jobset_id: jobset_id.to_string(),
                creator_agent_id: existing_creator,
                epoch: existing_epoch,
                ended: false,
            });
        }
        conn.execute(
            "INSERT INTO jobset (jobset_id, creator_agent_id, epoch, ended, created_at) \
             VALUES (?1, ?2, ?3, 0, ?4)",
            params![jobset_id, creator, epoch, now],
        )
        .map_err(anyhow::Error::from)
        .map_err(|e| JoinError::State(anyhow::anyhow!("{e:#}")))?;
        Ok(Jobset {
            jobset_id: jobset_id.to_string(),
            creator_agent_id: creator.to_string(),
            epoch,
            ended: false,
        })
    }

    /// Adds `agent` as a member of `jobset_id` (idempotent: a second call is
    /// a no-op). Refuses when the jobset is ended or unknown is handled by
    /// the caller (this mirrors the in-memory join path).
    pub fn add_member(&self, jobset_id: &str, agent: &str) -> Result<()> {
        let conn = self.db.connection()?;
        let now = chrono::Utc::now().timestamp();
        let ended: bool = conn
            .query_row(
                "SELECT ended FROM jobset WHERE jobset_id = ?1",
                params![jobset_id],
                |row| Ok(row.get::<_, i64>(0)? != 0),
            )
            .optional()
            .context("Failed to check the jobset row")?
            .unwrap_or(false);
        if ended {
            bail!("refusing to join jobset '{jobset_id}': jobset has already been ended");
        }
        conn.execute(
            "INSERT INTO jobset_member (jobset_id, agent_id, joined_at) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(jobset_id, agent_id) DO NOTHING",
            params![jobset_id, agent, now],
        )
        .context("Failed to add the membership row")?;
        Ok(())
    }

    /// Removes every membership row of `agent` (SPEC §7.2 item 7: a member
    /// row is deleted when its agent is removed). Returns the id of each
    /// jobset that no longer has any member.
    pub fn remove_agent(&self, agent: &str) -> Result<Vec<String>> {
        let conn = self.db.connection()?;
        let affected: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT jobset_id FROM jobset_member WHERE agent_id = ?1")
                .context("Failed to prepare remove_agent query")?;
            let rows = stmt
                .query_map(params![agent], |row| row.get::<_, String>(0))
                .context("Failed to list the agent's jobset memberships")?;
            rows.filter_map(|r| r.ok()).collect()
        };
        conn.execute(
            "DELETE FROM jobset_member WHERE agent_id = ?1",
            params![agent],
        )
        .context("Failed to delete the agent's membership rows")?;
        let mut emptied = Vec::new();
        for jobset_id in affected {
            let remaining: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM jobset_member WHERE jobset_id = ?1",
                    params![jobset_id],
                    |row| row.get(0),
                )
                .context("Failed to count remaining members")?;
            if remaining == 0 {
                emptied.push(jobset_id);
            }
        }
        Ok(emptied)
    }

    /// The jobsets `agent` is a member of.
    pub fn memberships(&self, agent: &str) -> Result<Vec<Jobset>> {
        let conn = self.db.connection()?;
        let mut stmt = conn
            .prepare(
                "SELECT j.jobset_id, j.creator_agent_id, j.epoch, j.ended \
                 FROM jobset_member m JOIN jobset j ON j.jobset_id = m.jobset_id \
                 WHERE m.agent_id = ?1 ORDER BY m.joined_at",
            )
            .context("Failed to prepare memberships query")?;
        let rows = stmt
            .query_map(params![agent], Self::row)
            .context("Failed to load the agent's jobset memberships")?;
        rows.map(|r| r.context("Failed to read a membership row"))
            .collect()
    }

    /// The members of `jobset_id`.
    pub fn members(&self, jobset_id: &str) -> Result<Vec<String>> {
        let conn = self.db.connection()?;
        let mut stmt = conn
            .prepare("SELECT agent_id FROM jobset_member WHERE jobset_id = ?1 ORDER BY joined_at")
            .context("Failed to prepare members query")?;
        let rows = stmt
            .query_map(params![jobset_id], |row| row.get::<_, String>(0))
            .context("Failed to load the jobset members")?;
        rows.map(|r| r.context("Failed to read a member row"))
            .collect()
    }

    /// Whether `agent` is a member of `jobset_id`.
    pub fn is_member(&self, jobset_id: &str, agent: &str) -> Result<bool> {
        let conn = self.db.connection()?;
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM jobset_member WHERE jobset_id = ?1 AND agent_id = ?2",
                params![jobset_id, agent],
                |row| row.get(0),
            )
            .context("Failed to check jobset membership")?;
        Ok(n > 0)
    }

    /// Ends `jobset_id` in the store: bumps its epoch to `epoch + 1`, marks
    /// it ended and removes its membership rows (SPEC §7.16; mirrors the
    /// in-memory `JobsetRegistry::end_jobset`). Returns the new epoch. A
    /// no-op returning the stored epoch when already ended.
    pub fn end(&self, jobset_id: &str) -> Result<u64> {
        let conn = self.db.connection()?;
        let now = chrono::Utc::now().timestamp();
        let row: Option<(u64, i64)> = conn
            .query_row(
                "SELECT epoch, ended FROM jobset WHERE jobset_id = ?1",
                params![jobset_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("Failed to load the jobset row")?;
        let Some((epoch, ended)) = row else {
            // The C5 callers 404 on a genuinely unknown jobset before ending.
            bail!("jobset '{jobset_id}' not found in the state store");
        };
        if ended != 0 {
            return Ok(epoch);
        }
        let new_epoch = epoch + 1;
        conn.execute(
            "UPDATE jobset SET epoch = ?2, ended = 1, ended_at = ?3 WHERE jobset_id = ?1",
            params![jobset_id, new_epoch, now],
        )
        .context("Failed to end the jobset row")?;
        conn.execute(
            "DELETE FROM jobset_member WHERE jobset_id = ?1",
            params![jobset_id],
        )
        .context("Failed to remove memberships of the ended jobset")?;
        Ok(new_epoch)
    }
}

/// The join/upsert failure of the new-store path: an ended jobset, or a
/// presented epoch older than the stored one, is refused exactly like the
/// in-memory `register_agent_into` refuses an ended jobset.
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("jobset has already been ended ({0})")]
    JobsetEnded(String),
    #[error("jobset state store error: {0}")]
    State(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, JobsetStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = JobsetStore::open_path(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn ensure_default_creates_and_is_idempotent() {
        let (_dir, store) = store();
        let first = store.ensure_default("agent-a").unwrap();
        assert_eq!(first.jobset_id, "agent-a");
        assert_eq!(first.creator_agent_id, "agent-a");
        assert!(!first.ended);
        let again = store.ensure_default("agent-a").unwrap();
        assert_eq!(again, first);
        assert!(store.is_member("agent-a", "agent-a").unwrap());
        assert_eq!(
            store.members("agent-a").unwrap(),
            vec!["agent-a".to_string()]
        );
    }

    #[test]
    fn multi_jobset_membership() {
        let (_dir, store) = store();
        store.ensure_default("alice").unwrap();
        store.upsert_foreign("js-b", "", 0).unwrap();
        store.add_member("js-b", "alice").unwrap();
        store.upsert_foreign("js-c", "", 0).unwrap();
        store.add_member("js-c", "alice").unwrap();

        let memberships = store.memberships("alice").unwrap();
        let ids: Vec<String> = memberships.iter().map(|j| j.jobset_id.clone()).collect();
        assert!(ids.contains(&"alice".to_string()));
        assert!(ids.contains(&"js-b".to_string()));
        assert!(ids.contains(&"js-c".to_string()));

        assert_eq!(store.members("js-b").unwrap(), vec!["alice".to_string()]);
        assert!(store.is_member("js-b", "alice").unwrap());
        assert!(!store.is_member("js-b", "bob").unwrap());
    }

    #[test]
    fn upsert_foreign_refuses_ended_and_stale_epoch() {
        let (_dir, store) = store();
        store.upsert_foreign("js-x", "", 1).unwrap();
        assert!(matches!(
            store.upsert_foreign("js-x", "", 0),
            Err(JoinError::JobsetEnded(_))
        ));
        store.end("js-x").unwrap();
        assert!(matches!(
            store.upsert_foreign("js-x", "", 5),
            Err(JoinError::JobsetEnded(_))
        ));
    }

    #[test]
    fn epoch_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("state");
        {
            let store = JobsetStore::open_path(&root).unwrap();
            store.ensure_default("agent-r").unwrap();
            store.upsert_foreign("js-r", "", 0).unwrap();
            store.add_member("js-r", "agent-r").unwrap();
        }
        {
            // A fresh connection (a pod restart) reads the same rows.
            let store = JobsetStore::open_path(&root).unwrap();
            let jobset = store.get("js-r").unwrap().unwrap();
            assert!(!jobset.ended);
            assert_eq!(jobset.epoch, 0);
            assert!(store.is_member("js-r", "agent-r").unwrap());
        }
    }

    #[test]
    fn end_bumps_epoch_and_removes_members() {
        let (_dir, store) = store();
        store.ensure_default("agent-e").unwrap();
        store.upsert_foreign("js-e", "", 0).unwrap();
        store.add_member("js-e", "agent-e").unwrap();
        assert_eq!(store.end("js-e").unwrap(), 1);
        let jobset = store.get("js-e").unwrap().unwrap();
        assert!(jobset.ended);
        assert_eq!(jobset.epoch, 1);
        // A second end is a no-op at the stored epoch.
        assert_eq!(store.end("js-e").unwrap(), 1);
        assert!(store.members("js-e").unwrap().is_empty());
        // An ended jobset refuses new members.
        assert!(store.add_member("js-e", "other").is_err());
    }

    #[test]
    fn remove_agent_reports_emptied_jobsets() {
        let (_dir, store) = store();
        store.ensure_default("agent-f").unwrap();
        store.upsert_foreign("js-f", "", 0).unwrap();
        store.add_member("js-f", "agent-f").unwrap();
        let emptied = store.remove_agent("agent-f").unwrap();
        assert!(emptied.contains(&"js-f".to_string()));
        assert!(store.memberships("agent-f").unwrap().is_empty());
        assert!(store.members("js-f").unwrap().is_empty());
    }
}

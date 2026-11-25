//! Unified state store for kubeadp-agent (`<state_root>/state.sqlite`).
//!
//! Provides [`StateDb`], [`JobStore`], [`SessionStore`], [`RunMetaStore`],
//! [`RunRegistry`], and numbered schema migrations ([`migrations`]).
//!
//! # Concurrency and Durability
//! The state store opens with WAL journal mode, a 5-second busy timeout
//! and `synchronous=NORMAL`. Schema migrations are numbered and additive,
//! tracked by `PRAGMA user_version`. Databases with a newer `user_version`
//! than known to the binary are refused.

pub mod jobs;
pub mod jobsets;
pub mod migrations;
pub mod runs;
pub mod sessions;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use jobs::JobStore;
pub use jobsets::JobsetStore;
pub use runs::{RunMeta, RunMetaStore, RunRegistry};
pub use sessions::SessionStore;

/// The central state database handle for a pod.
#[derive(Debug, Clone)]
pub struct StateDb {
    state_root: PathBuf,
}

impl StateDb {
    /// Returns the path to `<state_root>/state.sqlite`.
    pub fn db_path(state_root: &Path) -> PathBuf {
        state_root.join("state.sqlite")
    }

    /// Opens the state database at `<state_root>/state.sqlite`.
    ///
    /// Creates the directory and database file if missing, sets permissions
    /// to 0600, configures a 5s busy timeout, enables WAL journal mode,
    /// sets `synchronous=NORMAL`, and applies pending migrations.
    pub fn open(state_root: &Path) -> Result<Self> {
        let path = Self::db_path(state_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create state directory {}", parent.display())
            })?;
        }
        let conn = Connection::open(&path)
            .with_context(|| format!("Failed to open state database at {}", path.display()))?;

        tighten_permissions(&path);

        conn.busy_timeout(Duration::from_secs(5))
            .with_context(|| format!("Failed to set busy timeout on {}", path.display()))?;

        set_wal_with_retry(&conn, &path)?;

        let _ = conn.pragma_update(None, "synchronous", "NORMAL");

        migrations::migrate(&conn).with_context(|| {
            format!(
                "Failed to migrate state database at {} (is the file corrupted?)",
                path.display()
            )
        })?;

        tighten_permissions(&path);

        Ok(Self {
            state_root: state_root.to_path_buf(),
        })
    }

    /// Opens a direct read-write connection to the state database.
    pub fn connection(&self) -> Result<Connection> {
        let path = Self::db_path(&self.state_root);
        let conn = Connection::open(&path)
            .with_context(|| format!("Failed to open connection to {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))
            .with_context(|| format!("Failed to set busy timeout on {}", path.display()))?;
        let _ = conn.pragma_update(None, "synchronous", "NORMAL");
        Ok(conn)
    }

    /// Opens a read-only connection to the state database.
    pub fn reader(&self) -> Result<Connection> {
        let path = Self::db_path(&self.state_root);
        if !path.exists() {
            anyhow::bail!("State database does not exist at {}", path.display());
        }
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| {
                format!(
                    "Failed to open state database read-only at {}",
                    path.display()
                )
            })?;
        conn.busy_timeout(Duration::from_secs(5))
            .with_context(|| format!("Failed to set busy timeout on {}", path.display()))?;
        Ok(conn)
    }

    /// Returns the state root directory path.
    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    /// Returns a [`JobStore`] handle for querying and modifying jobs.
    pub fn jobs(&self) -> JobStore {
        JobStore::new(self.clone())
    }

    /// Returns a [`SessionStore`] handle for supervisor and session state.
    pub fn sessions(&self) -> SessionStore {
        SessionStore::new(self.clone())
    }

    /// Returns a [`RunMetaStore`] handle for querying and modifying `run_meta`.
    pub fn runs_meta(&self) -> RunMetaStore {
        RunMetaStore::new(self.clone())
    }

    /// Returns a [`JobsetStore`] handle for the jobset/member tables.
    pub fn jobsets(&self) -> JobsetStore {
        JobsetStore::new(self.clone())
    }

    /// Returns a [`RunRegistry`] view over jobs, sessions, and `run_meta`.
    pub fn runs(&self) -> RunRegistry {
        RunRegistry::new(self.clone())
    }
}

/// N12: tightens the state database file (and its `-wal`/`-shm` siblings)
/// to mode 0600. Best effort.
fn tighten_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let tighten = |p: PathBuf| {
            if let Ok(meta) = fs::metadata(&p) {
                let mut perms = meta.permissions();
                if perms.mode() & 0o777 != 0o600 {
                    perms.set_mode(0o600);
                    let _ = fs::set_permissions(&p, perms);
                }
            }
        };
        tighten(path.to_path_buf());
        let mut sibling = path.as_os_str().to_owned();
        sibling.push("-wal");
        tighten(PathBuf::from(sibling));
        let mut sibling = path.as_os_str().to_owned();
        sibling.push("-shm");
        tighten(PathBuf::from(sibling));
    }
}

/// Runs `PRAGMA journal_mode = WAL` with a bounded retry on transient busy/locked errors.
fn set_wal_with_retry(conn: &Connection, path: &Path) -> Result<()> {
    const ATTEMPTS: usize = 8;
    const BACKOFF_MS: &[u64] = &[5, 10, 20, 40, 80, 160, 320];
    for attempt in 0..ATTEMPTS {
        match conn.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(e) if is_busy_locked(&e) && attempt + 1 < ATTEMPTS => {
                let ms = BACKOFF_MS
                    .get(attempt)
                    .copied()
                    .unwrap_or_else(|| BACKOFF_MS[BACKOFF_MS.len() - 1]);
                std::thread::sleep(Duration::from_millis(ms));
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "Failed to enable WAL journal mode on state database at {} (is the file corrupted?)",
                        path.display()
                    )
                });
            }
        }
    }
    unreachable!("the retry loop always returns")
}

fn is_busy_locked(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(e, _)
            if e.code == rusqlite::ErrorCode::DatabaseBusy
                || e.code == rusqlite::ErrorCode::DatabaseLocked
    )
}

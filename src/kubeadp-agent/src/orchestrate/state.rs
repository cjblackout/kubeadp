use crate::orchestrate::logging::rfc3339_now;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    Starting,
    Running,
    Backoff,
    Failed,
    Stopped,
}

impl std::fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionStatus::Starting => write!(f, "starting"),
            SessionStatus::Running => write!(f, "running"),
            SessionStatus::Backoff => write!(f, "backoff"),
            SessionStatus::Failed => write!(f, "failed"),
            SessionStatus::Stopped => write!(f, "stopped"),
        }
    }
}

impl SessionStatus {
    /// Parses a status string produced by `Display`/serde (lowercase);
    /// returns `None` for unrecognized values.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "starting" => Some(SessionStatus::Starting),
            "running" => Some(SessionStatus::Running),
            "backoff" => Some(SessionStatus::Backoff),
            "failed" => Some(SessionStatus::Failed),
            "stopped" => Some(SessionStatus::Stopped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionState {
    pub name: String,
    pub pid: Option<u32>,
    pub start_time: Option<String>,
    pub start_time_epoch: Option<u64>,
    pub uptime_secs: Option<f64>,
    pub restart_count: u32,
    pub last_exit_reason: Option<String>,
    pub status: SessionStatus,
    pub last_health_result: Option<String>,
    /// The engine this session runs on (`"claude"`, `"agy"`, ...). `None`
    /// for sessions created before engine selection existed (they are claude
    /// sessions by definition). Resolved from request > config > role default
    /// at start (HL-91 E1 §3.1) and persisted so the session survives a
    /// restart on the same engine (HL-91 E5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
}

impl SessionState {
    pub fn new(name: String) -> Self {
        Self {
            name,
            pid: None,
            start_time: None,
            start_time_epoch: None,
            uptime_secs: None,
            restart_count: 0,
            last_exit_reason: None,
            status: SessionStatus::Starting,
            last_health_result: None,
            engine: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SupervisorState {
    pub supervisor_pid: u32,
    pub heartbeat: String,
    pub heartbeat_epoch: u64,
    pub status: String,
    pub sessions: HashMap<String, SessionState>,
}

impl SupervisorState {
    pub fn new(pid: u32) -> Self {
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            supervisor_pid: pid,
            heartbeat: rfc3339_now(),
            heartbeat_epoch: now_epoch,
            status: "running".to_string(),
            sessions: HashMap::new(),
        }
    }

    pub fn update_heartbeat(&mut self) {
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.heartbeat = rfc3339_now();
        self.heartbeat_epoch = now_epoch;
    }
}

fn state_db_path(state_root: &Path) -> std::path::PathBuf {
    crate::core::job::state_db_path(state_root)
}

/// Persists the supervisor state into the shared per-pod state database
/// (`<state_root>/state.sqlite`): the single `supervisor_state` row plus one
/// `session_state` row per session, written in ONE transaction.
///
/// # Writers and readers
/// The supervisor process is the only writer (its session threads and the
/// heartbeat loop write through here; SQLite's WAL locking serializes
/// concurrent writers, the busy timeout waits briefly for one). Readers
/// (`agent-orchestrate status`, this module's load) use read-only
/// connections. `synchronous=NORMAL` keeps the existing heartbeat cadence
/// from hammering the disk: in WAL mode a crash never corrupts the database,
/// only a power loss may drop the most recent commits.
///
/// # Layout
/// `supervisor_state` is a single row (`id = 1`): pid, started, heartbeat,
/// status and a `version` for the row schema. `session_state` holds one row
/// per session with the fields of [`SessionState`]. Rows of sessions that
/// are no longer part of `state` (the in-memory map is authoritative) are
/// removed, so a config change also removes them from the store.
pub fn save_supervisor_state(state_root: &Path, state: &SupervisorState) -> Result<(), String> {
    let db = crate::state::StateDb::open(state_root).map_err(|e| e.to_string())?;
    db.sessions().save(state)
}

pub fn load_supervisor_state(state_root: &Path) -> Result<Option<SupervisorState>, String> {
    let db_path = state_db_path(state_root);
    if !db_path.exists() {
        return Ok(None);
    }
    let db = crate::state::StateDb::open(state_root).map_err(|e| e.to_string())?;
    db.sessions().load()
}

pub fn import_legacy_state(
    state_file: &Path,
    state_root: &Path,
) -> Result<Option<SupervisorState>, String> {
    if !state_file.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(state_file).map_err(|e| {
        format!(
            "Failed to read legacy state file {}: {}",
            state_file.display(),
            e
        )
    })?;
    let parsed: Option<SupervisorState> = serde_json::from_str(&content).ok();

    let migrated_path = state_file.with_file_name(format!(
        "{}.migrated",
        state_file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "state".to_string())
    ));
    fs::rename(state_file, &migrated_path).map_err(|e| {
        format!(
            "Failed to rename legacy state file {} to {}: {}",
            state_file.display(),
            migrated_path.display(),
            e
        )
    })?;

    let legacy = match parsed {
        Some(state) => state,
        None => {
            // Quarantined above; the caller logs and continues with the
            // database state (fresh on a new store).
            return Ok(None);
        }
    };

    match load_supervisor_state(state_root) {
        Ok(Some(_)) => Ok(None),
        Ok(None) => {
            save_supervisor_state(state_root, &legacy)?;
            Ok(Some(legacy))
        }
        Err(e) => Err(e),
    }
}

#[derive(Debug, PartialEq)]
pub enum StatusCheckError {
    StaleHeartbeat { age_secs: f64, threshold_secs: f64 },
    SessionFailed { failed_sessions: Vec<String> },
    Stopped { reason: String },
}

/// Validates heartbeat freshness and checks if supervisor or any session is in stopped/failed status.
pub fn check_status(
    state: &SupervisorState,
    stale_threshold_secs: f64,
) -> Result<(), StatusCheckError> {
    if state.status == "stopped" {
        return Err(StatusCheckError::Stopped {
            reason: "Supervisor state is stopped".to_string(),
        });
    }

    let mut stopped = Vec::new();
    for (name, session) in &state.sessions {
        if session.status == SessionStatus::Stopped {
            stopped.push(name.clone());
        }
    }
    if !stopped.is_empty() {
        stopped.sort();
        return Err(StatusCheckError::Stopped {
            reason: format!("session(s) in stopped state: {:?}", stopped),
        });
    }

    let now_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let age_secs = if now_epoch >= state.heartbeat_epoch {
        (now_epoch - state.heartbeat_epoch) as f64
    } else {
        0.0
    };

    if age_secs > stale_threshold_secs {
        return Err(StatusCheckError::StaleHeartbeat {
            age_secs,
            threshold_secs: stale_threshold_secs,
        });
    }

    let mut failed = Vec::new();
    for (name, session) in &state.sessions {
        if session.status == SessionStatus::Failed {
            failed.push(name.clone());
        }
    }

    if !failed.is_empty() {
        failed.sort();
        return Err(StatusCheckError::SessionFailed {
            failed_sessions: failed,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Arc;
    use std::thread;

    fn test_state_root(tag: &str) -> std::path::PathBuf {
        let dir = env::temp_dir().join(format!("test_state_db_{}_{}", tag, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_state(pid: u32) -> SupervisorState {
        let mut state = SupervisorState::new(pid);
        let mut sess = SessionState::new("sess1".to_string());
        sess.status = SessionStatus::Running;
        sess.pid = Some(1001);
        state.sessions.insert("sess1".to_string(), sess);
        state
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let root = test_state_root("roundtrip");

        let mut state = sample_state(42);
        let mut sess = SessionState::new("agy-sess".to_string());
        sess.status = SessionStatus::Backoff;
        sess.engine = Some("agy".to_string());
        sess.restart_count = 7;
        sess.last_exit_reason = Some("exit: 1".to_string());
        sess.start_time = Some("2026-01-01T00:00:00Z".to_string());
        sess.start_time_epoch = Some(1234);
        sess.uptime_secs = Some(42.5);
        state.sessions.insert("agy-sess".to_string(), sess);

        save_supervisor_state(&root, &state).expect("save must succeed");
        assert!(state_db_path(&root).exists());

        let read = load_supervisor_state(&root)
            .expect("load must succeed")
            .expect("a saved state must be present");
        assert_eq!(read.supervisor_pid, 42);
        assert_eq!(read.status, "running");
        assert_eq!(read.sessions["sess1"].status, SessionStatus::Running);
        assert_eq!(read.sessions["sess1"].pid, Some(1001));
        assert_eq!(read.sessions["agy-sess"].engine.as_deref(), Some("agy"));
        assert_eq!(read.sessions["agy-sess"].restart_count, 7);
        assert_eq!(
            read.sessions["agy-sess"].last_exit_reason.as_deref(),
            Some("exit: 1")
        );
        assert_eq!(read.sessions["agy-sess"].uptime_secs, Some(42.5));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_absent_store_loads_none() {
        let root = test_state_root("absent");
        let loaded = load_supervisor_state(&root).expect("absent db must not be an error");
        assert!(loaded.is_none(), "no supervisor state written yet");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_save_prunes_sessions_removed_from_the_state() {
        let root = test_state_root("prune");
        let mut state = sample_state(1);
        state
            .sessions
            .insert("extra".to_string(), SessionState::new("extra".to_string()));
        save_supervisor_state(&root, &state).unwrap();

        let mut next = sample_state(2);
        next.sessions.remove("extra");
        save_supervisor_state(&root, &next).unwrap();

        let read = load_supervisor_state(&root).unwrap().unwrap();
        assert!(!read.sessions.contains_key("extra"));
        let _ = fs::remove_dir_all(&root);
    }

    /// The real gain: the supervisor state survives a restart. Write state,
    /// close every connection (process exit), reopen and load again.
    #[test]
    fn test_restart_reopens_and_resumes() {
        let root = test_state_root("restart");

        let mut first = sample_state(111);
        first.sessions.get_mut("sess1").unwrap().restart_count = 3;
        first.sessions.get_mut("sess1").unwrap().last_exit_reason = Some("signal: 9".to_string());
        save_supervisor_state(&root, &first).unwrap();

        // Simulated process exit: a fresh process reopens the same database.
        let second = load_supervisor_state(&root)
            .expect("reopen must succeed")
            .expect("state must survive the restart");
        assert_eq!(second.supervisor_pid, 111);
        assert_eq!(second.sessions["sess1"].restart_count, 3);
        assert_eq!(
            second.sessions["sess1"].last_exit_reason.as_deref(),
            Some("signal: 9")
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_corrupt_database_is_a_clear_error() {
        let root = test_state_root("corrupt");
        let db_path = state_db_path(&root);
        fs::write(&db_path, b"this is not a sqlite database at all").unwrap();

        // The writer fails loud (WAL cannot be enabled on a corrupt file).
        let save_err = save_supervisor_state(&root, &sample_state(1)).unwrap_err();
        assert!(
            save_err.contains("state database"),
            "save error must name the database: {save_err}"
        );
        // The reader reports a clear error as well, never a silent empty state.
        let load_err = load_supervisor_state(&root).unwrap_err();
        assert!(
            load_err.contains("state database") || load_err.contains("corrupted"),
            "load error must name the database: {load_err}"
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_import_legacy_state_file_once() {
        let root = test_state_root("import");
        let legacy_file = root.join("claude-supervisor-state.json");
        let legacy = r#"{"supervisor_pid":4242,"heartbeat":"2026-01-01T00:00:00Z","heartbeat_epoch":100,"status":"running","sessions":{"old":{"name":"old","pid":55,"start_time":null,"start_time_epoch":null,"uptime_secs":null,"restart_count":4,"last_exit_reason":"exit: 2","status":"failed","last_health_result":null}}}"#;
        fs::write(&legacy_file, legacy).unwrap();

        let imported = import_legacy_state(&legacy_file, &root)
            .expect("import must succeed")
            .expect("state must be imported");
        assert_eq!(imported.supervisor_pid, 4242);
        assert_eq!(imported.sessions["old"].restart_count, 4);

        // The file is renamed to *.migrated exactly once.
        assert!(!legacy_file.exists());
        assert!(root.join("claude-supervisor-state.json.migrated").exists());

        // The imported state is readable from the store.
        let read = load_supervisor_state(&root).unwrap().unwrap();
        assert_eq!(read.sessions["old"].status, SessionStatus::Failed);

        // A second start (the file no longer exists) imports nothing.
        let again = import_legacy_state(&legacy_file, &root).unwrap();
        assert!(again.is_none());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_import_never_overwrites_newer_db_state() {
        let root = test_state_root("import-newer");
        save_supervisor_state(&root, &sample_state(1)).unwrap();

        let legacy_file = root.join("claude-supervisor-state.json");
        let legacy = r#"{"supervisor_pid":4242,"heartbeat":"2026-01-01T00:00:00Z","heartbeat_epoch":100,"status":"running","sessions":{}}"#;
        fs::write(&legacy_file, legacy).unwrap();

        let imported = import_legacy_state(&legacy_file, &root).unwrap();
        assert!(imported.is_none(), "the db state must not be overwritten");
        assert!(!legacy_file.exists(), "the file is still renamed");
        let read = load_supervisor_state(&root).unwrap().unwrap();
        assert_eq!(read.supervisor_pid, 1);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_import_corrupt_legacy_file_quarantines() {
        let root = test_state_root("import-corrupt");
        let legacy_file = root.join("claude-supervisor-state.json");
        fs::write(&legacy_file, b"{ not json").unwrap();

        let imported = import_legacy_state(&legacy_file, &root)
            .expect("a corrupt file is quarantined, not an error");
        assert!(imported.is_none());
        assert!(!legacy_file.exists());
        assert!(root.join("claude-supervisor-state.json.migrated").exists());
        assert!(load_supervisor_state(&root).unwrap().is_none());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_check_status_healthy() {
        let mut state = SupervisorState::new(100);
        let mut sess = SessionState::new("sess1".to_string());
        sess.status = SessionStatus::Running;
        state.sessions.insert("sess1".to_string(), sess);

        let res = check_status(&state, 10.0);
        assert_eq!(res, Ok(()));
    }

    #[test]
    fn test_check_status_stale_heartbeat() {
        let mut state = SupervisorState::new(100);
        state.heartbeat_epoch = 1000; // Old epoch
        let res = check_status(&state, 5.0);
        assert!(matches!(res, Err(StatusCheckError::StaleHeartbeat { .. })));
    }

    #[test]
    fn test_check_status_failed_session() {
        let mut state = SupervisorState::new(100);
        let mut sess = SessionState::new("sess1".to_string());
        sess.status = SessionStatus::Failed;
        state.sessions.insert("sess1".to_string(), sess);

        let res = check_status(&state, 15.0);
        assert_eq!(
            res,
            Err(StatusCheckError::SessionFailed {
                failed_sessions: vec!["sess1".to_string()]
            })
        );
    }

    #[test]
    fn test_check_status_supervisor_stopped() {
        let mut state = SupervisorState::new(100);
        state.status = "stopped".to_string();
        let res = check_status(&state, 15.0);
        assert!(matches!(res, Err(StatusCheckError::Stopped { .. })));
    }

    #[test]
    fn test_check_status_session_stopped() {
        let mut state = SupervisorState::new(100);
        let mut sess = SessionState::new("sess1".to_string());
        sess.status = SessionStatus::Stopped;
        state.sessions.insert("sess1".to_string(), sess);

        let res = check_status(&state, 15.0);
        assert!(matches!(res, Err(StatusCheckError::Stopped { .. })));
    }

    /// Concurrent writers (the supervisor's session threads + heartbeat loop)
    /// never lose writes and never corrupt the store, while a reader using a
    /// read-only connection sees a valid state on every read.
    #[test]
    fn test_concurrent_writes_with_read_only_reader() {
        let root = Arc::new(test_state_root("concurrent"));

        // Write an initial valid state so the reader can start immediately.
        save_supervisor_state(&root, &SupervisorState::new(999)).unwrap();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failures = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let writes = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // 8 concurrent writer threads.
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let root_c = Arc::clone(&root);
            let stop_c = Arc::clone(&stop);
            let writes_c = Arc::clone(&writes);
            handles.push(thread::spawn(move || {
                let mut state = SupervisorState::new(i);
                let sess = SessionState::new(format!("sess-{i}"));
                state.sessions.insert(format!("sess-{i}"), sess);
                let mut count = 0u64;
                while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
                    count += 1;
                    state
                        .sessions
                        .get_mut(&format!("sess-{i}"))
                        .unwrap()
                        .restart_count = count as u32;
                    if save_supervisor_state(&root_c, &state).is_ok() {
                        writes_c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }

        // Reader thread: load through a read-only connection in a loop.
        let root_r = Arc::clone(&root);
        let stop_r = Arc::clone(&stop);
        let failures_c = Arc::clone(&failures);
        let reader = thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
            while std::time::Instant::now() < deadline
                && !stop_r.load(std::sync::atomic::Ordering::Relaxed)
            {
                // An errored load during concurrent writes counts as a failure.
                if load_supervisor_state(&root_r).is_err() {
                    failures_c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        thread::sleep(std::time::Duration::from_millis(310));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);

        for h in handles {
            let _ = h.join();
        }
        let _ = reader.join();

        assert_eq!(
            failures.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a read-only reader must never see an error during concurrent writes"
        );
        assert!(
            writes.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "concurrent writers must have made progress"
        );

        // After the writers stop, the store is readable and holds the last
        // writer's session (each write replaces the session set, so exactly
        // one remains).
        let final_state = load_supervisor_state(&root).unwrap().unwrap();
        assert_eq!(final_state.sessions.len(), 1);

        let _ = fs::remove_dir_all(&*root);
    }
}

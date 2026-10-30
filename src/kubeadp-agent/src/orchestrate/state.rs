use crate::orchestrate::logging::rfc3339_now;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
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

/// Process-wide mutex that serializes all state-file writes.
/// This prevents concurrent threads from interleaving their temp-file writes
/// (the temp name includes pid+nanos+counter, so uniqueness is guaranteed even
/// without the mutex; the mutex additionally avoids a burst of redundant writes).
static WRITE_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn write_mutex() -> &'static Mutex<()> {
    WRITE_MUTEX.get_or_init(|| Mutex::new(()))
}

/// Process-wide counter for unique temp file names.
static WRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Atomically writes the state file by creating a unique temp file and renaming it.
///
/// # Thread-safety
/// Multiple threads can call this concurrently.  Each call:
/// 1. Clones the state data (caller must have already released any shared state lock).
/// 2. Serialises to JSON under the process-wide write mutex to avoid torn renames.
/// 3. Uses a temp path that is unique per-call (pid + nanoseconds + a monotone counter),
///    so two concurrent calls never share a temp path.
/// 4. Does NOT call `sync_all`; `rename(2)` atomicity is sufficient for this
///    `/tmp` state file (power-loss durability is not required).
pub fn write_state_atomic(state_file: &Path, state: &SupervisorState) -> Result<(), String> {
    // Serialize outside the lock – this is the expensive part.
    let json_bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| format!("Failed to serialize state: {}", e))?;

    // Build a unique temp path: .<name>.tmp.<pid>.<nanos>.<counter>
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let counter = WRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = state_file
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| "state".to_string());

    let temp_file = if let Some(parent) = state_file.parent() {
        let _ = fs::create_dir_all(parent);
        parent.join(format!(".{}.tmp.{}.{}.{}", file_name, pid, nanos, counter))
    } else {
        Path::new(&format!(
            "{}.tmp.{}.{}.{}",
            state_file.display(),
            pid,
            nanos,
            counter
        ))
        .to_path_buf()
    };

    // Serialise write + rename under the mutex so that a concurrent writer
    // cannot interleave its rename on top of ours.
    let _guard = write_mutex().lock().unwrap_or_else(|e| e.into_inner());

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_file)
        .map_err(|e| {
            format!(
                "Failed to create temp state file {}: {}",
                temp_file.display(),
                e
            )
        })?;

    file.write_all(&json_bytes)
        .map_err(|e| format!("Failed to write to temp state file: {}", e))?;
    // Intentionally omitting sync_all: rename(2) atomicity is sufficient for /tmp.
    drop(file);

    fs::rename(&temp_file, state_file).map_err(|e| {
        format!(
            "Failed to rename {} -> {}: {}",
            temp_file.display(),
            state_file.display(),
            e
        )
    })?;

    Ok(())
}

/// Reads and parses the state file.
pub fn read_state(state_file: &Path) -> Result<SupervisorState, String> {
    let content = fs::read_to_string(state_file)
        .map_err(|e| format!("Failed to read state file {}: {}", state_file.display(), e))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse state file {}: {}", state_file.display(), e))
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

    #[test]
    fn test_atomic_write_and_read() {
        let temp_dir = env::temp_dir().join(format!("test_state_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let state_path = temp_dir.join("state.json");

        let mut state = SupervisorState::new(42);
        let mut sess = SessionState::new("sess1".to_string());
        sess.status = SessionStatus::Running;
        sess.pid = Some(1001);
        state.sessions.insert("sess1".to_string(), sess);

        write_state_atomic(&state_path, &state).expect("Atomic write must succeed");
        assert!(state_path.exists());

        let read = read_state(&state_path).expect("Read state must succeed");
        assert_eq!(read.supervisor_pid, 42);
        assert_eq!(read.sessions["sess1"].status, SessionStatus::Running);
        assert_eq!(read.sessions["sess1"].pid, Some(1001));

        let _ = fs::remove_dir_all(&temp_dir);
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

    /// Fix #1: 8 concurrent writer threads + 1 reader parsing in a loop.
    /// Zero parse failures expected.
    #[test]
    fn test_concurrent_writes_no_torn_reads() {
        let temp_dir =
            env::temp_dir().join(format!("test_concurrent_state_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let state_path = Arc::new(temp_dir.join("concurrent_state.json"));

        // Write an initial valid state so the reader can start immediately.
        let initial = SupervisorState::new(999);
        write_state_atomic(&state_path, &initial).unwrap();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let parse_failures = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Spawn 8 writer threads.
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let path = Arc::clone(&state_path);
            let stop_c = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                let mut state = SupervisorState::new(i);
                let mut count = 0u32;
                while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
                    count = count.wrapping_add(1);
                    let mut sess = SessionState::new(format!("sess-{}", i));
                    sess.restart_count = count;
                    state.sessions.insert(format!("sess-{}", i), sess);
                    let _ = write_state_atomic(&path, &state);
                }
            }));
        }

        // Reader thread: parse state in a tight loop for ~200ms.
        let path_r = Arc::clone(&state_path);
        let stop_r = Arc::clone(&stop);
        let failures = Arc::clone(&parse_failures);
        let reader = thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
            while std::time::Instant::now() < deadline
                && !stop_r.load(std::sync::atomic::Ordering::Relaxed)
            {
                if let Ok(content) = fs::read_to_string(path_r.as_ref()) {
                    if !content.trim().is_empty()
                        && serde_json::from_str::<SupervisorState>(&content).is_err()
                    {
                        failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        // Let the test run for ~200ms then stop writers.
        thread::sleep(std::time::Duration::from_millis(210));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);

        for h in handles {
            let _ = h.join();
        }
        let _ = reader.join();

        let failures = parse_failures.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            failures, 0,
            "Expected zero JSON parse failures from concurrent writes, got {}",
            failures
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }
}

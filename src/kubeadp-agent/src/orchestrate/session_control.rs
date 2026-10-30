//! Shared per-session control handles so the sessions REST API can pause,
//! resume and restart a supervised session without writing to its PTY.
//!
//! The supervisor's main loop creates one [`SessionControl`] per configured
//! session and hands the live thread its `paused` flag. `main.rs`'s heartbeat
//! loop consumes `restart` and `running` to respawn a session thread after a
//! stop or crash, and the `/api/v1/sessions` handlers manipulate the same
//! flags. Deliberately tiny and lock-free on the hot paths: only the registry
//! map itself is behind a mutex.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::orchestrate::config::SessionConfig;
use crate::orchestrate::pty::PtyInput;

/// Live control handles for one supervised session.
pub struct SessionControl {
    /// The configuration used to (re)spawn this session's thread.
    pub config: SessionConfig,
    /// Pauses the currently running supervision thread (if any). Set by
    /// `stop`; cleared by `start`. Swapped for a fresh flag on respawn.
    pub paused: Arc<AtomicBool>,
    /// Set when the session thread is running (cleared by the heartbeat loop
    /// once the handle is observed finished).
    pub running: Arc<AtomicBool>,
    /// Set by the API to ask the heartbeat loop to respawn the session.
    pub restart: Arc<AtomicBool>,
    /// Slot for the live child PTY master, filled by the session thread while
    /// a child is spawned and drained before it closes the fd. The sessions
    /// API writes messages into it; a stale slot left behind by a dead thread
    /// is reclaimed on the next respawn.
    pub pty_input: Arc<Mutex<PtyInput>>,
}

impl SessionControl {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            paused: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            restart: Arc::new(AtomicBool::new(false)),
            pty_input: Arc::new(Mutex::new(PtyInput::new(-1))),
        }
    }
}

/// Registry of all supervised sessions, shared between the supervisor main
/// loop and the sessions API handlers.
#[derive(Clone, Default)]
pub struct SessionControlRegistry {
    inner: Arc<Mutex<HashMap<String, Arc<SessionControl>>>>,
}

impl SessionControlRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, control: Arc<SessionControl>) {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(control.config.name.clone(), control);
    }

    pub fn get(&self, name: &str) -> Option<Arc<SessionControl>> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(name)
    }

    /// Iterates a snapshot of all controls.
    pub fn all(&self) -> Vec<Arc<SessionControl>> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Sets a control's paused flag without touching any other field.
pub fn set_paused(control: &SessionControl, paused: bool) {
    control.paused.store(paused, Ordering::SeqCst);
}

/// Returns whether the control currently asks for a fresh thread.
pub fn take_restart(control: &SessionControl) -> bool {
    control.restart.swap(false, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config(name: &str) -> SessionConfig {
        SessionConfig {
            name: name.to_string(),
            workdir: std::path::PathBuf::from("/tmp"),
            session_id: format!("session-{name}"),
            claude_binary: std::path::PathBuf::from("/bin/true"),
            claude_args: vec![],
            env: Default::default(),
            session_file: None,
            memory_file: None,
            readiness_poll_secs: 1.0,
            hang_timeout_secs: 0.0,
            kill_grace_secs: 1.0,
            pty_eof_grace_secs: 1.0,
            kill_wait_secs: 2.0,
            base_backoff_secs: 1.0,
            max_backoff_secs: 10.0,
            healthy_run_secs: 30.0,
            max_crashes: 3,
            crash_window_secs: 60.0,
            prefix_output: true,
            transcript_path: None,
            failed_cooldown_secs: 1.0,
            backoff_window_secs: 60.0,
            resume_fail_quarantine_after: 3,
            resume_fail_uptime_secs: 60.0,
        }
    }

    #[test]
    fn test_registry_insert_get_all() {
        let reg = SessionControlRegistry::new();
        let c = Arc::new(SessionControl::new(sample_config("a")));
        reg.insert(c.clone());
        assert_eq!(reg.len(), 1);
        assert!(reg.get("a").is_some());
        assert!(reg.get("missing").is_none());
        assert_eq!(reg.all().len(), 1);
    }

    #[test]
    fn test_pause_and_restart_flags() {
        let c = SessionControl::new(sample_config("a"));
        assert!(!c.paused.load(Ordering::SeqCst));
        set_paused(&c, true);
        assert!(c.paused.load(Ordering::SeqCst));
        c.restart.store(true, Ordering::SeqCst);
        assert!(take_restart(&c));
        assert!(!take_restart(&c));
    }

    #[test]
    fn test_registry_is_empty() {
        assert!(SessionControlRegistry::new().is_empty());
    }
}

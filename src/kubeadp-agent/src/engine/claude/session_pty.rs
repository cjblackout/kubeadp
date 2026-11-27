//! PTY session adapter (design HL-91 E2).
//!
//! The claude `Engine` delegates its session-mode operations to the existing
//! supervisor machinery rather than moving the loop: [`ClaudePtyAdapter`]
//! wraps the pieces the sessions API already exposes
//! ([`crate::orchestrate::SessionsApiState`],
//! [`crate::orchestrate::sessions_api::deliver_message`],
//! [`crate::orchestrate::session_control::set_paused`]) so `start_session`,
//! `send_message` and `stop_session` stay thin adapters. Nothing routes
//! through the trait yet (E1 state), so this is the engine-side surface E3/E5
//! will wire up.
//!
//! Structured reads are served from the claude transcript
//! ([`crate::engine::claude::transcript`]) instead of the raw PTY log, using
//! the same monotone event cursor as [`crate::engine::run::Run::events_seq`].

use crate::engine::claude::transcript::{read_transcript, transcript_path};
use crate::engine::run::{EngineEvent, Run};
use crate::engine::EngineError;
use crate::orchestrate::session_control::{set_paused, SessionControl};
use crate::orchestrate::sessions_api::{deliver_message, DeliverOutcome, SessionsApiState};
use std::path::PathBuf;
use std::sync::Arc;

/// Maps a [`DeliverOutcome`] to the engine error surface.
pub fn outcome_to_error(outcome: DeliverOutcome, run: &Run) -> Result<(), EngineError> {
    match outcome {
        DeliverOutcome::Accepted { .. } => Ok(()),
        DeliverOutcome::NotFound => Err(EngineError::NotFound {
            run: run.id.clone(),
        }),
        DeliverOutcome::Empty => Err(EngineError::Conflict(
            "message text must not be empty".to_string(),
        )),
        DeliverOutcome::TooLarge => Err(EngineError::Internal(
            "message text exceeds the session delivery limit".to_string(),
        )),
        DeliverOutcome::RateLimited { retry_after_secs } => Err(EngineError::Conflict(format!(
            "message rate limit reached; retry after {retry_after_secs}s"
        ))),
        DeliverOutcome::NotReady => Err(EngineError::Conflict(
            "the session engine is still starting; retry the call".to_string(),
        )),
        DeliverOutcome::NotRunning => Err(EngineError::Internal(
            "session has no live PTY (stopped or paused)".to_string(),
        )),
    }
}

/// Engine-side handles for one supervised session, looked up per operation
/// from the shared registry the supervisor seeded.
pub struct ClaudePtyAdapter {
    /// The supervisor's shared session state (controls, log ring, limiter).
    pub state: Arc<SessionsApiState>,
    /// Claude config directory (`$HOME/.claude` by default) used to locate
    /// the transcript for structured reads.
    pub claude_dir: PathBuf,
}

impl ClaudePtyAdapter {
    /// Builds the adapter from the supervisor API state and claude config dir.
    pub fn new(state: Arc<SessionsApiState>, claude_dir: PathBuf) -> Self {
        Self { state, claude_dir }
    }

    /// Look up the live control for `name`.
    fn control(&self, name: &str) -> Option<Arc<SessionControl>> {
        self.state.controls.get(name)
    }

    /// Starts (or unpauses) a supervised session by name: clears the paused
    /// flag and, when no thread is running, arms the heartbeat loop's
    /// `restart` flag so a fresh thread is spawned. This is exactly the
    /// sessions API `start_session` behaviour, exposed for the engine.
    pub fn start(&self, name: &str) -> Result<(), EngineError> {
        let Some(control) = self.control(name) else {
            return Err(EngineError::NotFound {
                run: name.to_string(),
            });
        };
        set_paused(&control, false);
        if !control.running.load(std::sync::atomic::Ordering::SeqCst) {
            control
                .restart
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    /// Stops a supervised session: sets the paused flag (the loop exits and
    /// the bounded child-escalation path kills the process group). When
    /// `restart` is set, clears paused and arms the heartbeat loop to spawn a
    /// fresh thread, matching the sessions API `restart_session` semantics.
    pub fn stop(&self, name: &str, restart: bool) -> Result<(), EngineError> {
        let Some(control) = self.control(name) else {
            return Err(EngineError::NotFound {
                run: name.to_string(),
            });
        };
        set_paused(&control, true);
        if restart {
            set_paused(&control, false);
            control
                .restart
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    /// Delivers one message into the session PTY as a bracketed paste, using
    /// the same shared rate budget and sanitisation as the REST/A2A paths.
    pub fn send_message(&self, name: &str, text: &str) -> Result<(), EngineError> {
        let run = Run::new(name, crate::engine::run::RunKind::Session, PathBuf::new());
        let outcome = deliver_message(
            &self.state,
            name,
            crate::orchestrate::sessions_api::REST_SENDER,
            text,
            true,
        );
        outcome_to_error(outcome, &run)
    }

    /// The transcript path for `name` + `workdir`, using the adapter's claude
    /// config dir. Returns `None` when no session id is known.
    pub fn transcript_for(&self, name: &str, workdir: &std::path::Path) -> Option<PathBuf> {
        let control = self.control(name)?;
        let session_id = control.config.session_id.as_str();
        if session_id.is_empty() {
            return None;
        }
        Some(transcript_path(&self.claude_dir, session_id, workdir))
    }

    /// Structured events from the session's transcript after cursor `after`.
    ///
    /// The transcript is the durable record of the PTY conversation, so this
    /// is a pure file read (no live process). Events are indexed by
    /// [`EngineEvent`] position; `after` skips previously-surfaced events,
    /// mirroring [`crate::engine::run::Run::events_seq`].
    pub fn events(&self, run: &Run, after: u64) -> Result<Vec<EngineEvent>, EngineError> {
        let Some(path) = self.transcript_for(&run.id, &run.workdir) else {
            return Ok(Vec::new());
        };
        let events = read_transcript(&path).map_err(EngineError::Internal)?;
        let start = (after as usize).min(events.len());
        Ok(events[start..].to_vec())
    }
}

/// Filters a parsed event list to those after `after` and derives a fresh
/// cursor. Pure helper kept separate so the cursor logic is unit-testable
/// without a live adapter.
pub fn events_after_cursor(events: Vec<EngineEvent>, after: u64) -> Vec<EngineEvent> {
    let start = (after as usize).min(events.len());
    events[start..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_after_cursor_skips_earlier_and_bounds() {
        let events = vec![
            EngineEvent::MessageDelta {
                role: crate::engine::run::Role::User,
                text: "a".to_string(),
            },
            EngineEvent::MessageDelta {
                role: crate::engine::run::Role::Assistant,
                text: "b".to_string(),
            },
        ];
        assert_eq!(events_after_cursor(events.clone(), 0).len(), 2);
        assert_eq!(events_after_cursor(events.clone(), 1).len(), 1);
        assert_eq!(events_after_cursor(events.clone(), 2).len(), 0);
        // Cursor past the end saturates (no panic, empty tail).
        assert_eq!(events_after_cursor(events, 99).len(), 0);
    }

    #[test]
    fn parse_from_transcript_module_smokes() {
        let content = r#"{"type":"user","cwd":"/w","sessionId":"s","message":{"role":"user","content":[{"type":"text","text":"hi"}]},"uuid":"u1"}
{"type":"assistant","cwd":"/w","sessionId":"s","message":{"role":"assistant","content":[{"type":"text","text":"yo"}]},"uuid":"u2"}"#;
        let events = crate::engine::claude::transcript::parse_transcript(content).unwrap();
        assert_eq!(events.len(), 2);
        // The cursor helper composes with a parsed list.
        assert_eq!(events_after_cursor(events.clone(), 1).len(), 1);
    }

    #[test]
    fn derive_transcript_path_uses_session_id() {
        // regression guard: the adapter resolves through the same claude
        // config dir and slug rule as `resolve_session_file`.
        let dir = PathBuf::from("/home/agent/.claude");
        let p = transcript_path(&dir, "sess-9", std::path::Path::new("/workspace/proj"));
        assert!(p.ends_with("projects/-workspace-proj/sess-9.jsonl"));
    }

    #[test]
    fn start_unknown_session_is_not_found() {
        let registry = crate::orchestrate::session_control::SessionControlRegistry::new();
        let state = Arc::new(std::sync::Mutex::new(
            crate::orchestrate::state::SupervisorState::new(1),
        ));
        let adapter = ClaudePtyAdapter {
            state: crate::orchestrate::sessions_api::SessionsApiState::new(
                state,
                registry,
                Arc::new(crate::orchestrate::session_log::SessionLogs::new(1024)),
            ),
            claude_dir: PathBuf::from("/home/agent/.claude"),
        };
        let err = adapter.start("missing").unwrap_err();
        assert!(matches!(err, EngineError::NotFound { .. }));
    }
}

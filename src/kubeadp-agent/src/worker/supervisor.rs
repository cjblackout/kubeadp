//! Background supervisor for the agy remote-control daemon.
//!
//! Supervises the lifecycle of the remote-control background daemon when
//! `AGENT_RUN_REMOTE_CONTROL_AUTOSTART` is enabled. It ensures the daemon is started
//! at server boot, periodically polls status, and restarts the daemon if it exits
//! or crashes. Manual `disable` calls set an in-memory suppression flag that pauses
//! supervision until `enable` is called or the server restarts.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::core::commands;
use crate::core::config::Config;
use crate::core::output::CommandOutput;

/// Maximum backoff duration between retry attempts on failure (5 minutes).
pub const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Pure gate check: determines whether the supervisor should start, and returns an optional warning.
pub fn should_start_supervisor(
    remote_control: bool,
    autostart: bool,
) -> (bool, Option<&'static str>) {
    if autostart && !remote_control {
        (
            false,
            Some("AGENT_RUN_REMOTE_CONTROL_AUTOSTART is enabled but AGENT_RUN_REMOTE_CONTROL is not enabled; supervisor will not start"),
        )
    } else if autostart && remote_control {
        (true, None)
    } else {
        (false, None)
    }
}

/// Pure backoff calculation with exponential backoff capped at `max_interval`.
pub fn calculate_backoff(
    base_interval: Duration,
    consecutive_failures: u32,
    max_interval: Duration,
) -> Duration {
    if consecutive_failures == 0 {
        return base_interval;
    }
    let factor = 1u64
        .checked_shl(consecutive_failures.saturating_sub(1))
        .unwrap_or(u64::MAX);
    let backoff_secs = base_interval.as_secs().saturating_mul(factor);
    Duration::from_secs(backoff_secs).min(max_interval)
}

/// Action to be taken by the supervisor based on suppression and daemon active state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorAction {
    /// Inactive and not suppressed -> trigger enable.
    Restart,
    /// Active and not suppressed -> no action needed.
    DoNothingActive,
    /// Suppressed by manual disable -> do not start or restart.
    DoNothingSuppressed,
}

pub fn decide_action(suppressed: bool, active: bool) -> SupervisorAction {
    if suppressed {
        SupervisorAction::DoNothingSuppressed
    } else if !active {
        SupervisorAction::Restart
    } else {
        SupervisorAction::DoNothingActive
    }
}

/// Internal states of the supervisor state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorState {
    Initializing,
    Active,
    Suppressed,
    Error,
}

/// State transition events that warrant logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorEvent {
    Started,
    FoundInactiveAndRestarted,
    Error(String),
}

/// Evaluates state transitions from a status check or step result.
pub fn evaluate_state_transition(
    prev_state: SupervisorState,
    suppressed: bool,
    step_result: Result<bool, &str>,
) -> (SupervisorState, Option<SupervisorEvent>) {
    if suppressed {
        (SupervisorState::Suppressed, None)
    } else {
        match step_result {
            Ok(active) => {
                if active {
                    match prev_state {
                        SupervisorState::Initializing
                        | SupervisorState::Error
                        | SupervisorState::Suppressed => {
                            (SupervisorState::Active, Some(SupervisorEvent::Started))
                        }
                        SupervisorState::Active => (SupervisorState::Active, None),
                    }
                } else {
                    (prev_state, None)
                }
            }
            Err(err) => {
                if prev_state == SupervisorState::Error {
                    (SupervisorState::Error, None)
                } else {
                    (
                        SupervisorState::Error,
                        Some(SupervisorEvent::Error(err.to_string())),
                    )
                }
            }
        }
    }
}

/// Evaluates transition after a successful restart call.
pub fn transition_after_restart(prev_state: SupervisorState) -> (SupervisorState, SupervisorEvent) {
    match prev_state {
        SupervisorState::Active => (
            SupervisorState::Active,
            SupervisorEvent::FoundInactiveAndRestarted,
        ),
        _ => (SupervisorState::Active, SupervisorEvent::Started),
    }
}

fn log_transition(event: &SupervisorEvent, instance_name: &str) {
    match event {
        SupervisorEvent::Started => {
            if instance_name.is_empty() {
                eprintln!("agent-run-api: remote-control daemon started");
            } else {
                eprintln!(
                    "agent-run-api: remote-control daemon started (instance: {instance_name})"
                );
            }
        }
        SupervisorEvent::FoundInactiveAndRestarted => {
            if instance_name.is_empty() {
                eprintln!("agent-run-api: remote-control daemon found inactive; restarted");
            } else {
                eprintln!("agent-run-api: remote-control daemon found inactive; restarted (instance: {instance_name})");
            }
        }
        SupervisorEvent::Error(msg) => {
            eprintln!("agent-run-api: remote-control supervisor error: {msg}");
        }
    }
}

async fn run_enable_blocking(config: Arc<Config>) -> Option<anyhow::Result<CommandOutput>> {
    tokio::task::spawn_blocking(move || commands::remote_control::enable(&config))
        .await
        .ok()
}

async fn run_status_blocking(config: Arc<Config>) -> Option<anyhow::Result<CommandOutput>> {
    tokio::task::spawn_blocking(move || commands::remote_control::status(&config))
        .await
        .ok()
}

/// Starts the supervisor in a background task on the tokio runtime.
pub fn start_supervisor(
    config: Arc<Config>,
    suppressed: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        supervisor_loop(config, suppressed).await;
    })
}

async fn supervisor_loop(config: Arc<Config>, suppressed: Arc<AtomicBool>) {
    let check_interval = Duration::from_secs(config.remote_control_check_secs);
    let mut state = SupervisorState::Initializing;
    let mut consecutive_failures: u32 = 0;

    // Initial enable on startup
    if !suppressed.load(Ordering::SeqCst) {
        let enable_res = match run_enable_blocking(Arc::clone(&config)).await {
            Some(res) => res,
            None => return,
        };
        match enable_res {
            Ok(output) => {
                let inst = output
                    .data
                    .get("instance_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let (new_state, event) = evaluate_state_transition(state, false, Ok(true));
                state = new_state;
                consecutive_failures = 0;
                if let Some(SupervisorEvent::Started) = event {
                    log_transition(&SupervisorEvent::Started, inst);
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                let (new_state, event) = evaluate_state_transition(state, false, Err(&err_str));
                state = new_state;
                consecutive_failures = consecutive_failures.saturating_add(1);
                if let Some(ev) = event {
                    log_transition(&ev, "");
                }
            }
        }
    } else {
        state = SupervisorState::Suppressed;
    }

    loop {
        let sleep_dur = if consecutive_failures > 0 {
            calculate_backoff(check_interval, consecutive_failures, MAX_BACKOFF)
        } else {
            check_interval
        };

        tokio::time::sleep(sleep_dur).await;

        let is_suppressed = suppressed.load(Ordering::SeqCst);
        if is_suppressed {
            let (new_state, _) = evaluate_state_transition(state, true, Ok(false));
            state = new_state;
            consecutive_failures = 0;
            continue;
        }

        let status_res = match run_status_blocking(Arc::clone(&config)).await {
            Some(res) => res,
            None => return,
        };

        match status_res {
            Ok(status_output) => {
                let is_active = status_output
                    .data
                    .get("active")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let inst = status_output
                    .data
                    .get("instance_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                match decide_action(false, is_active) {
                    SupervisorAction::DoNothingActive => {
                        let (new_state, event) = evaluate_state_transition(state, false, Ok(true));
                        state = new_state;
                        consecutive_failures = 0;
                        if let Some(ev) = event {
                            log_transition(&ev, inst);
                        }
                    }
                    SupervisorAction::Restart => {
                        let enable_res = match run_enable_blocking(Arc::clone(&config)).await {
                            Some(res) => res,
                            None => return,
                        };
                        match enable_res {
                            Ok(enable_output) => {
                                let restart_inst = enable_output
                                    .data
                                    .get("instance_name")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(inst);
                                let (new_state, event) = transition_after_restart(state);
                                state = new_state;
                                consecutive_failures = 0;
                                log_transition(&event, restart_inst);
                            }
                            Err(e) => {
                                let err_str = e.to_string();
                                let (new_state, event) =
                                    evaluate_state_transition(state, false, Err(&err_str));
                                state = new_state;
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                if let Some(ev) = event {
                                    log_transition(&ev, "");
                                }
                            }
                        }
                    }
                    SupervisorAction::DoNothingSuppressed => {}
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                let (new_state, event) = evaluate_state_transition(state, false, Err(&err_str));
                state = new_state;
                consecutive_failures = consecutive_failures.saturating_add(1);
                if let Some(ev) = event {
                    log_transition(&ev, "");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gate_check_should_start_supervisor() {
        assert_eq!(should_start_supervisor(false, false), (false, None));
        assert_eq!(should_start_supervisor(true, false), (false, None));
        assert_eq!(should_start_supervisor(true, true), (true, None));

        let (should_start, warn) = should_start_supervisor(false, true);
        assert!(!should_start);
        assert!(warn.is_some());
        assert!(warn.unwrap().contains("AGENT_RUN_REMOTE_CONTROL_AUTOSTART"));
    }

    #[test]
    fn test_exponential_backoff_calculation() {
        let base = Duration::from_secs(60);
        let max = Duration::from_secs(300);

        // 0 failures -> base interval
        assert_eq!(calculate_backoff(base, 0, max), Duration::from_secs(60));

        // 1 failure -> base * 1 = 60s
        assert_eq!(calculate_backoff(base, 1, max), Duration::from_secs(60));

        // 2 failures -> base * 2 = 120s
        assert_eq!(calculate_backoff(base, 2, max), Duration::from_secs(120));

        // 3 failures -> base * 4 = 240s
        assert_eq!(calculate_backoff(base, 3, max), Duration::from_secs(240));

        // 4 failures -> base * 8 = 480s -> capped at 300s
        assert_eq!(calculate_backoff(base, 4, max), Duration::from_secs(300));

        // Many failures -> stays capped at 300s, no overflow
        assert_eq!(calculate_backoff(base, 20, max), Duration::from_secs(300));
        assert_eq!(calculate_backoff(base, 100, max), Duration::from_secs(300));

        // With smaller base interval (e.g. 10s in test)
        let base10 = Duration::from_secs(10);
        assert_eq!(calculate_backoff(base10, 0, max), Duration::from_secs(10));
        assert_eq!(calculate_backoff(base10, 1, max), Duration::from_secs(10));
        assert_eq!(calculate_backoff(base10, 2, max), Duration::from_secs(20));
        assert_eq!(calculate_backoff(base10, 3, max), Duration::from_secs(40));
        assert_eq!(calculate_backoff(base10, 4, max), Duration::from_secs(80));
    }

    #[test]
    fn test_decide_action_pure_logic() {
        // Suppressed always does nothing
        assert_eq!(
            decide_action(true, false),
            SupervisorAction::DoNothingSuppressed
        );
        assert_eq!(
            decide_action(true, true),
            SupervisorAction::DoNothingSuppressed
        );

        // Active does nothing
        assert_eq!(
            decide_action(false, true),
            SupervisorAction::DoNothingActive
        );

        // Inactive and not suppressed triggers restart
        assert_eq!(decide_action(false, false), SupervisorAction::Restart);
    }

    #[test]
    fn test_state_transitions_and_events() {
        // Initializing -> Active => Started event
        let (s, ev) = evaluate_state_transition(SupervisorState::Initializing, false, Ok(true));
        assert_eq!(s, SupervisorState::Active);
        assert_eq!(ev, Some(SupervisorEvent::Started));

        // Active -> Active => No event (never log every tick)
        let (s, ev) = evaluate_state_transition(SupervisorState::Active, false, Ok(true));
        assert_eq!(s, SupervisorState::Active);
        assert_eq!(ev, None);

        // Active -> Error => Error event
        let (s, ev) = evaluate_state_transition(SupervisorState::Active, false, Err("agy timeout"));
        assert_eq!(s, SupervisorState::Error);
        assert_eq!(ev, Some(SupervisorEvent::Error("agy timeout".to_string())));

        // Error -> Error => No event (do not repeat error log on every tick)
        let (s, ev) = evaluate_state_transition(SupervisorState::Error, false, Err("agy timeout"));
        assert_eq!(s, SupervisorState::Error);
        assert_eq!(ev, None);

        // Error -> Active => Started event (recovered)
        let (s, ev) = evaluate_state_transition(SupervisorState::Error, false, Ok(true));
        assert_eq!(s, SupervisorState::Active);
        assert_eq!(ev, Some(SupervisorEvent::Started));

        // Active -> Suppressed => Suppressed state, no event
        let (s, ev) = evaluate_state_transition(SupervisorState::Active, true, Ok(false));
        assert_eq!(s, SupervisorState::Suppressed);
        assert_eq!(ev, None);

        // Suppressed -> Suppressed => No event
        let (s, ev) = evaluate_state_transition(SupervisorState::Suppressed, true, Ok(false));
        assert_eq!(s, SupervisorState::Suppressed);
        assert_eq!(ev, None);

        // Suppressed -> Active => Started event
        let (s, ev) = evaluate_state_transition(SupervisorState::Suppressed, false, Ok(true));
        assert_eq!(s, SupervisorState::Active);
        assert_eq!(ev, Some(SupervisorEvent::Started));
    }

    #[test]
    fn test_transition_after_restart() {
        // Was previously active when found inactive and restarted
        let (s, ev) = transition_after_restart(SupervisorState::Active);
        assert_eq!(s, SupervisorState::Active);
        assert_eq!(ev, SupervisorEvent::FoundInactiveAndRestarted);

        // Was in other state (e.g. Error or Initializing)
        let (s, ev) = transition_after_restart(SupervisorState::Error);
        assert_eq!(s, SupervisorState::Active);
        assert_eq!(ev, SupervisorEvent::Started);
    }
}

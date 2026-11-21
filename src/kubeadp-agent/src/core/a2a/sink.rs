//! The shared A2A delivery-sink surface and the ONE escalation ladder used by
//! both roles (design HL-113 §7.14, stream-3 C3).
//!
//! [`AgentSink`] is the behaviour a delivery sink must expose for the shared
//! ladder to drive it: resolve the ladder configuration, the run's inbox, the
//! undeliverable/reason predicate, the §7.11 idle test and the two escalation
//! applications (`idle_turn` — never kills — and `interrupt`, the last
//! resort). [`ladder_pass`] is the single state machine over the InboxStore
//! that both [`crate::worker::a2a::JobMessageSink`] and
//! [`crate::orchestrate::a2a::SessionMessageSink`] run, so the worker (jobs)
//! and the orchestrator (sessions) can never drift apart:
//!
//! ```text
//! queued -> injection at the next boundary of any available channel (the
//!           hook channel; `tool_result`/`wait_message` drain in parallel)
//!        -> uninjected past `ack_secs` and the run idle:
//!           `Engine::is_idle`/`idle_turn` (the engine supplies the idle step;
//!           the harness starts the next turn with the message, never kills)
//!        -> `interrupt: true` or past `force_secs`: `interrupt` (the last
//!           resort: the kill-and-resume of §7.11)
//!        -> TTL expiry: the sender's task fails
//! ```
//! The clock is injectable through the `now` argument of [`ladder_pass`], which
//! is what makes the shared ladder deterministically testable (an aged
//! `queued_at` escalates to `idle_turn`/`interrupt`/`Expired` without any
//! wall-clock sleeping).

use std::path::PathBuf;

use crate::core::a2a::delivery::{next_step, read_expiry, LadderConfig, LadderInput, LadderStep};
use crate::core::a2a::inbox::{Inbox, QueuedMessage};
use crate::engine::capability::InjectChannel;

/// The delivery-sink surface the shared escalation ladder drives.
///
/// `run_id` is the addressed job id (worker) or session name (orchestrator).
/// The sink supplies everything the ladder cannot know generically: the ladder
/// knobs, the run's inbox path (the ladder opens it WITHOUT its own TTL filter
/// and applies the TTL itself, so an expiry is recorded when a message
/// lapses), the undeliverable predicate, the §7.11 idle test and the two
/// escalation applications. The engine supplies the idle step
/// ([`crate::engine::Engine::is_idle`] / `idle_turn`); a sink that has richer
/// knowledge (a live stdin channel, a session PTY) implements these hooks
/// directly.
pub trait AgentSink: Send + Sync {
    /// The ladder configuration this sink runs under.
    fn ladder_config(&self) -> LadderConfig;

    /// The run's inbox path (`<state_root>/a2a-inbox/<run>/`).
    fn inbox_dir(&self, run_id: &str) -> PathBuf;

    /// Why `run_id` is undeliverable right now, if it is. A message must never
    /// wake a killed/removed recipient: the ladder expires its pending
    /// messages with this reason and the sender's task fails.
    fn undeliverable(&self, run_id: &str) -> Option<String>;

    /// Whether the run is idle at a turn boundary (the `idle_turn` applies
    /// only past `ack_secs` AND when this is true). A finished run is idle; a
    /// RUNNING agy job is idle only with a live stdin channel; a running
    /// interactive session is idle between turns.
    fn is_idle(&self, run_id: &str) -> bool;

    /// Applies the `idle_turn` escalation to the ATOMICALLY claimed messages:
    /// starts the run's next turn with the joined framed text, never kills.
    /// Returns `true` when the messages were handed to the run (the ladder
    /// then acknowledges them through the `idle_turn` channel); `false` hands
    /// the claim back (still queued).
    fn apply_idle_turn(&self, run_id: &str, claimed: &[QueuedMessage]) -> bool;

    /// Applies the `interrupt` escalation (kill-and-resume, §7.11) to the
    /// ATOMICALLY claimed messages. Returns `true` when they were handed to
    /// the run (acknowledged through the `interrupt` channel); `false` hands
    /// the claim back.
    fn apply_interrupt(&self, run_id: &str, claimed: &[QueuedMessage]) -> bool;

    /// The step a ladder pass returns when the atomic claim came back empty
    /// (another channel or pass already claimed/injected the message). The
    /// worker treats that as `Injected` (nothing left for this pass to do);
    /// the orchestrator stays `Queued` (its watchers re-poll while a lease is
    /// held). Defaults to `Injected`.
    fn unclaimed_step(&self) -> LadderStep {
        LadderStep::Injected
    }
}

/// ONE ladder pass for `run_id`: computes the step for the oldest pending
/// message and applies it; the caller re-polls on a non-terminal step.
///
/// This is the shared state machine of §7.14, driven by BOTH sinks through
/// [`AgentSink`]. `now` is the unix-seconds clock input (injectable for the
/// deterministic unit tests); production passes
/// [`crate::core::a2a::inbox::now_unix`]().
///
/// The escalation (the `idle_turn`/`interrupt` application) is claimed
/// ATOMICALLY by [`Inbox::claim_for_resume`], so exactly one concurrent
/// escalation (a ladder watcher, a manual pass, the hook channel, or the
/// sandboxed HTTP receive side) resumes the run for a message; a loser gets an
/// empty claim and steps [`AgentSink::unclaimed_step`] without touching the
/// run. Without the claim two concurrent ladder passes for the same run would
/// both read the same pending message and both resume it, claiming TWO
/// attempts for ONE message.
pub fn ladder_pass<S>(sink: &S, run_id: &str, interrupt: bool, now: u64) -> LadderStep
where
    S: AgentSink + ?Sized,
{
    let ladder_cfg = sink.ladder_config();
    let inbox_dir = sink.inbox_dir(run_id);
    // The inbox is opened WITHOUT its own TTL filter: the ladder applies the
    // TTL itself so the expiry is recorded when a message lapses.
    let inbox = Inbox::with_ttl(&inbox_dir, u64::MAX);
    let pending = inbox.pending();
    if pending.is_empty() {
        if inbox.uninjected_count() > 0 {
            // A channel holds the rest under a claim lease: it either
            // acknowledges them or the lease lapses and they come back.
            return LadderStep::Queued;
        }
        // Nothing left to deliver: the run's last expiry (if any) is the
        // terminal state the sender sees, else everything was injected.
        return match read_expiry(&inbox_dir) {
            Some(record) => LadderStep::Expired {
                reason: record.reason,
            },
            None => LadderStep::Injected,
        };
    }
    // A message must never wake a killed/removed recipient (the ingress
    // refuses it up front, but the recipient can be killed while its message
    // is still queued): fail the sender's task instead of resuming or waiting.
    if let Some(reason) = sink.undeliverable(run_id) {
        for msg in &pending {
            inbox.expire(&msg.message_id, &reason);
        }
        return LadderStep::Expired { reason };
    }
    let step = next_step(&LadderInput {
        now,
        queued_at: pending[0].queued_at,
        interrupt,
        idle_turn_available: sink.is_idle(run_id),
        expired: false,
        config: &ladder_cfg,
    });
    match step {
        LadderStep::Expired { reason } => {
            // Record the expiry AND remove the row: an expired message must
            // not be served by a hook later, nor block the younger messages
            // behind it (the watcher keeps going for those).
            inbox.expire(&pending[0].message_id, &reason);
            LadderStep::Expired { reason }
        }
        LadderStep::IdleTurn => {
            let claimed = inbox.claim_for_resume();
            if claimed.is_empty() {
                // Another pass/channel already claimed (or injected) the
                // message: nothing left for THIS pass to resume.
                return sink.unclaimed_step();
            }
            if sink.apply_idle_turn(run_id, &claimed) {
                for msg in &claimed {
                    inbox.mark_injected(&msg.message_id, InjectChannel::IdleTurn.as_str());
                }
                LadderStep::IdleTurn
            } else {
                // The escalation did not apply: hand the claim back so a later
                // pass (or channel) can retry the same message.
                for msg in &claimed {
                    inbox.release_claim(&msg.message_id);
                }
                LadderStep::Queued
            }
        }
        LadderStep::Interrupt => {
            let claimed = inbox.claim_for_resume();
            if claimed.is_empty() {
                return sink.unclaimed_step();
            }
            if sink.apply_interrupt(run_id, &claimed) {
                for msg in &claimed {
                    inbox.mark_injected(&msg.message_id, InjectChannel::Interrupt.as_str());
                }
                LadderStep::Interrupt
            } else {
                for msg in &claimed {
                    inbox.release_claim(&msg.message_id);
                }
                LadderStep::Queued
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::inbox::{Inbox, QueuedMessage};
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// A fake sink for the shared-ladder unit tests: the inbox lives under a
    /// temp dir, the knobs are injectable, and the two escalation applications
    /// are recorded through closures. This is the injectable-clock half of the
    /// C3 ladder tests: the pass is driven with an explicit `now` (aging the
    /// stored `queued_at`), never with a wall-clock sleep.
    struct FakeSink {
        inbox_root: PathBuf,
        ladder: LadderConfig,
        undeliverable_reason: Option<String>,
        idle: bool,
        applied: std::sync::Mutex<Vec<(&'static str, String)>>,
        apply_ok: bool,
    }

    impl FakeSink {
        fn new(run_id: &str) -> (Self, Inbox) {
            let tmp = tempdir().unwrap();
            let root = tmp.path().to_path_buf();
            let inbox = Inbox::new(crate::core::a2a::inbox::inbox_dir_for(&root, run_id));
            let sink = Self {
                inbox_root: root,
                ladder: LadderConfig::default(),
                undeliverable_reason: None,
                idle: true,
                applied: std::sync::Mutex::new(Vec::new()),
                apply_ok: true,
            };
            (sink, inbox)
        }

        fn applied(&self) -> Vec<String> {
            self.applied
                .lock()
                .unwrap()
                .iter()
                .map(|(c, _)| (*c).to_string())
                .collect()
        }

        /// Ages the stored `queued_at` of `message_id` to `now - seconds`, so a
        /// later [`ladder_pass`] with the SAME `now` makes a deterministic
        /// escalation decision (no wall-clock boundary race between the aging
        /// and the pass).
        fn age(inbox: &Inbox, message_id: &str, now: u64, seconds: u64) {
            let _ = inbox.with_conn(|conn| {
                conn.execute(
                    "UPDATE a2a_message SET queued_at=?1 WHERE run_id=?2 AND message_id=?3 AND fake=?4",
                    rusqlite::params![now.saturating_sub(seconds), inbox.run_id(), message_id, i64::from(inbox.fake())],
                )
                .map_err(|e| format!("failed to age the A2A message: {e}"))?;
                Ok(())
            });
        }
    }

    impl AgentSink for FakeSink {
        fn ladder_config(&self) -> LadderConfig {
            self.ladder
        }
        fn inbox_dir(&self, run_id: &str) -> PathBuf {
            crate::core::a2a::inbox::inbox_dir_for(&self.inbox_root, run_id)
        }
        fn undeliverable(&self, _run_id: &str) -> Option<String> {
            self.undeliverable_reason.clone()
        }
        fn is_idle(&self, _run_id: &str) -> bool {
            self.idle
        }
        fn apply_idle_turn(&self, _run_id: &str, claimed: &[QueuedMessage]) -> bool {
            self.applied
                .lock()
                .unwrap()
                .push(("idle_turn", claimed[0].framed.clone()));
            self.apply_ok
        }
        fn apply_interrupt(&self, _run_id: &str, claimed: &[QueuedMessage]) -> bool {
            self.applied
                .lock()
                .unwrap()
                .push(("interrupt", claimed[0].framed.clone()));
            self.apply_ok
        }
    }

    fn msg(id: &str, text: &str) -> QueuedMessage {
        QueuedMessage {
            message_id: id.to_string(),
            sender: "peer".to_string(),
            hop: 1,
            context_id: None,
            task_id: None,
            framed: format!("--- A2A INCOMING MESSAGE ---\n{text}"),
            queued_at: crate::core::a2a::inbox::now_unix(),
        }
    }

    #[test]
    fn shared_ladder_injects_at_the_idle_turn_past_ack_secs_and_never_kills() {
        let (mut sink, inbox) = FakeSink::new("run-1");
        sink.ladder = LadderConfig {
            ack_secs: 45,
            force_secs: 0,
            ttl_secs: 3600,
        };
        inbox.enqueue(msg("m1", "hello")).unwrap();
        let now = crate::core::a2a::inbox::now_unix();
        // Inside the ack window: queued (nothing applied, nothing killed).
        FakeSink::age(&inbox, "m1", now, 44);
        assert_eq!(ladder_pass(&sink, "run-1", false, now), LadderStep::Queued);
        assert!(
            sink.applied().is_empty(),
            "inside the ack window nothing applies"
        );
        // Past the ack window, run idle: the idle_turn injects (never a kill).
        FakeSink::age(&inbox, "m1", now, 45);
        assert_eq!(
            ladder_pass(&sink, "run-1", false, now),
            LadderStep::IdleTurn
        );
        assert_eq!(sink.applied(), vec!["idle_turn"]);
        assert!(
            inbox.pending().is_empty(),
            "the idle_turn acknowledges the message"
        );
    }

    #[test]
    fn shared_ladder_interrupt_escalates_immediately_and_expires_on_the_ttl() {
        let (mut sink, inbox) = FakeSink::new("run-2");
        sink.ladder = LadderConfig {
            ack_secs: 45,
            force_secs: 0,
            ttl_secs: 3600,
        };
        inbox.enqueue(msg("m1", "urgent")).unwrap();
        let now = crate::core::a2a::inbox::now_unix();
        // interrupt:true escalates immediately (the only kill-and-resume path).
        assert_eq!(
            ladder_pass(&sink, "run-2", true, now),
            LadderStep::Interrupt
        );
        assert_eq!(sink.applied(), vec!["interrupt"]);
        // Past the TTL the message is never delivered.
        inbox.enqueue(msg("m2", "late")).unwrap();
        FakeSink::age(&inbox, "m2", now, 3600);
        assert!(matches!(
            ladder_pass(&sink, "run-2", false, now),
            LadderStep::Expired { .. }
        ));
        assert!(
            inbox.uninjected_count() == 0,
            "an expired message is removed"
        );
    }

    #[test]
    fn shared_ladder_expires_for_an_undeliverable_recipient() {
        let (mut sink, inbox) = FakeSink::new("run-3");
        sink.undeliverable_reason =
            Some("A2A message not delivered: job 'run-3' was killed by the operator".to_string());
        inbox.enqueue(msg("m1", "to a killed job")).unwrap();
        let now = crate::core::a2a::inbox::now_unix();
        assert!(matches!(
            ladder_pass(&sink, "run-3", false, now),
            LadderStep::Expired { .. }
        ));
        assert_eq!(
            sink.applied().len(),
            0,
            "an undeliverable recipient is never resumed"
        );
        assert!(inbox.uninjected_count() == 0);
    }

    #[test]
    fn shared_ladder_hands_a_failed_escalation_back_to_the_queue() {
        let (mut sink, inbox) = FakeSink::new("run-4");
        sink.ladder = LadderConfig {
            ack_secs: 45,
            force_secs: 0,
            ttl_secs: 3600,
        };
        sink.apply_ok = false;
        inbox.enqueue(msg("m1", "no live pty yet")).unwrap();
        let now = crate::core::a2a::inbox::now_unix();
        FakeSink::age(&inbox, "m1", now, 60);
        assert_eq!(
            ladder_pass(&sink, "run-4", false, now),
            LadderStep::Queued,
            "an unapplied escalation stays queued (the claim is handed back)"
        );
        assert_eq!(sink.applied(), vec!["idle_turn"]);
        assert_eq!(inbox.uninjected_count(), 1, "the claim was released");
    }
}

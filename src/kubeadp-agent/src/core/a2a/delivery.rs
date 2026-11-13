//! The shared escalation-ladder delivery state machine (design HL-113 §7.14,
//! phase 2).
//!
//! One state machine, used by [`crate::worker::a2a`] (agy) and
//! [`crate::orchestrate::a2a`] (Claude Code):
//!
//! ```text
//! queued -> injection at the next boundary of any available channel (the
//!           hook channel; `tool_result`/`wait_message` drain in parallel)
//!        -> uninjected past `ack_secs` and the run idle: `idle_turn`
//!           (the harness starts the next turn with the message)
//!        -> `interrupt: true` or past `force_secs`: `interrupt` (the last
//!           resort: the kill-and-resume of §7.11)
//!        -> TTL expiry: the sender's task fails
//! ```
//! Default: never kill -- the message stays queued until injected or the
//! TTL expires. The live stdin path of §7.11 stays off unless the
//! `idle_turn` step needs it. The pure half lives here ([`next_step`],
//! [`sender_task_state`]); the wiring applies the step and marks the message
//! injected. The inbox is the queue and the dedupe store; the durable
//! `a2a_expiry` record (in the shared state database, phase 3 of §7.14) is
//! what fails the sender's task. [`record_expiry`] / [`read_expiry`] /
//! [`clear_expiry`] keep their phase-2 `inbox_dir` signatures but now address
//! the database row for that run.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

use crate::core::a2a::inbox::Inbox;

/// `ack_secs` default: how long a message may stay queued before the ladder
/// escalates to `idle_turn` (only with the run idle at a turn boundary).
pub const DEFAULT_ACK_SECS: u64 = 45;

/// `force_secs` default: `0` = never kill. A non-zero value is how long a
/// message may stay queued before the ladder escalates to `interrupt`.
pub const DEFAULT_FORCE_SECS: u64 = 0;

/// Subdirectory of a run's inbox holding the durable expiry markers
/// (`<inbox>/expired/`); host-only (the sandboxed side has no grant).
pub const EXPIRED_DIR_NAME: &str = "expired";

/// Metadata key requesting the explicit `interrupt` escalation
/// (`interrupt: true`). Trusted request metadata, never the framed content.
pub const META_INTERRUPT: &str = "a2a.interrupt";

/// The escalation-ladder configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LadderConfig {
    /// Ack window (seconds) before the `idle_turn` escalation.
    pub ack_secs: u64,
    /// Force timeout (seconds, `0` = never) before the `interrupt` step.
    pub force_secs: u64,
    /// TTL for uninjected messages (the inbox TTL).
    pub ttl_secs: u64,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            ack_secs: DEFAULT_ACK_SECS,
            force_secs: DEFAULT_FORCE_SECS,
            ttl_secs: crate::core::a2a::inbox::DEFAULT_TTL_SECS,
        }
    }
}

/// The step the ladder wants taken for a run's oldest pending message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LadderStep {
    /// The message stays queued: the hook channel delivers it at the run's
    /// next boundary (nothing to do).
    Queued,
    /// Uninjected past `ack_secs` with the run idle: the harness starts the
    /// next turn with the message. Never kills.
    IdleTurn,
    /// The explicit last resort: `interrupt: true` or past `force_secs`.
    Interrupt,
    /// The message was injected through some channel (nothing pending).
    Injected,
    /// The message expired (TTL) uninjected: the sender's task fails.
    Expired { reason: String },
}

/// The inputs of one [`next_step`] decision; the engine wiring fills them
/// from the run's inbox and the run's own state.
#[derive(Debug, Clone, Copy)]
pub struct LadderInput<'a> {
    /// Unix seconds now.
    pub now: u64,
    /// Unix seconds the oldest pending message was queued.
    pub queued_at: u64,
    /// Whether an `interrupt: true` message is pending (trusted metadata).
    pub interrupt: bool,
    /// Whether the run is idle at a turn boundary (a finished run is idle; a
    /// RUNNING agy with a live stdin channel is idle between turns).
    pub idle_turn_available: bool,
    /// Whether a durable expiry marker is present for this run.
    pub expired: bool,
    /// The ladder configuration.
    pub config: &'a LadderConfig,
}

/// Computes the step the ladder wants for the run's oldest pending message.
/// Order (design §7.14): an already-expired message is never delivered;
/// `interrupt: true` or a force timeout wins over the idle turn; the idle
/// turn applies only past `ack_secs` with the run idle; else queued.
pub fn next_step(input: &LadderInput) -> LadderStep {
    if input.expired {
        return LadderStep::Expired {
            reason: expired_reason(input.config.ttl_secs),
        };
    }
    let age = input.now.saturating_sub(input.queued_at);
    if age >= input.config.ttl_secs {
        return LadderStep::Expired {
            reason: expired_reason(input.config.ttl_secs),
        };
    }
    if input.interrupt || (input.config.force_secs > 0 && age >= input.config.force_secs) {
        return LadderStep::Interrupt;
    }
    if input.idle_turn_available && age >= input.config.ack_secs {
        return LadderStep::IdleTurn;
    }
    LadderStep::Queued
}

/// The clear reason recorded when a message expires uninjected.
pub fn expired_reason(ttl_secs: u64) -> String {
    format!("A2A message expired after {ttl_secs}s without delivery: no channel acknowledged it")
}

/// How the SENDER sees the addressed task while its message is in flight
/// (design §7.14: `submitted` while queued, `working` once injected, `failed`
/// with a clear reason on TTL expiry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SenderTaskState {
    /// The message is queued (not yet injected).
    Submitted,
    /// The message was injected: the run is processing it.
    Working,
    /// The message expired undelivered.
    Failed { reason: String },
}

impl SenderTaskState {
    /// The stable wire name of the state.
    pub fn as_str(&self) -> &'static str {
        match self {
            SenderTaskState::Submitted => "submitted",
            SenderTaskState::Working => "working",
            SenderTaskState::Failed { .. } => "failed",
        }
    }
}

/// Maps the inbox state of a run to the sender's task state: queued ==
/// `submitted`, injected (no pending) == `working`, a durable expiry marker
/// fails the task with its reason.
pub fn sender_task_state(pending: usize, expiry: Option<&ExpiryRecord>) -> SenderTaskState {
    if let Some(record) = expiry {
        return SenderTaskState::Failed {
            reason: record.reason.clone(),
        };
    }
    if pending > 0 {
        SenderTaskState::Submitted
    } else {
        SenderTaskState::Working
    }
}

/// The durable expiry record: which message expired and why.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExpiryRecord {
    pub message_id: String,
    pub reason: String,
    /// Unix seconds the message was recorded expired.
    pub expired_at: u64,
    /// Nanoseconds since the Unix epoch the message was recorded expired,
    /// kept strictly increasing per process: it is the sub-second order of
    /// records taken within the same one-second `expired_at`. Absent on old
    /// markers, so it defaults to `0` when parsing them.
    #[serde(default)]
    pub expired_at_ns: u64,
}

/// The last `expired_at_ns` this process issued. Used to keep the sub-second
/// ordering strictly increasing per process: even two records taken in the
/// same clock nanosecond (or a backwards clock step) get a strictly larger
/// value than the one before them.
static LAST_EXPIRY_NS: AtomicU64 = AtomicU64::new(0);

/// Nanoseconds since the Unix epoch, kept strictly increasing within this
/// process: the value is `now`, but never below the last issued value + 1
/// (a CAS on `LAST_EXPIRY_NS` hands out distinct values to concurrent
/// callers).
fn next_expiry_ns() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let mut last = LAST_EXPIRY_NS.load(Ordering::Relaxed);
    loop {
        let next = now.max(last.saturating_add(1));
        match LAST_EXPIRY_NS.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return next,
            Err(current) => last = current,
        }
    }
}

/// The `(expired_at, expired_at_ns)` monotonic pair for a new expiry record:
/// one `expired_at` from the clock and one strictly-increasing sub-second
/// value, kept strictly increasing per process even for two records taken in
/// the same clock nanosecond. "Latest" among records therefore never depends
/// on insertion order.
pub fn now_expiry_pair() -> (u64, u64) {
    (crate::core::a2a::inbox::now_unix(), next_expiry_ns())
}

/// Records that `message_id` expired uninjected (durable `a2a_expiry` row in
/// the shared state database, atomic; best effort). `inbox_dir` is the run's
/// inbox path (`<state_root>/a2a-inbox/<run>/`); the shared database and the
/// run id are derived from it exactly like the inbox itself.
pub fn record_expiry(inbox_dir: &Path, message_id: &str, reason: &str) {
    Inbox::new(inbox_dir).record_expired(message_id, reason);
}

/// The latest durable expiry record for the run, if any. See [`Inbox::read_expired`].
pub fn read_expiry(inbox_dir: &Path) -> Option<ExpiryRecord> {
    Inbox::new(inbox_dir).read_expired()
}

/// Removes every expiry record for the run (a fresh delivery supersedes the
/// last failure). Best effort.
pub fn clear_expiry(inbox_dir: &Path) {
    Inbox::new(inbox_dir).clear_expired();
}

/// The poll interval of a ladder watcher between two non-terminal passes.
pub const LADDER_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// The ladder watcher threads of one delivery sink: at most one per run.
///
/// A watcher repeats the sink's ladder pass until the run has nothing left
/// to deliver. The exit is decided UNDER the registry lock together with a
/// fresh look at the inbox, which closes the lost-wakeup window of a plain
/// "is a watcher alive?" check: a message enqueued while the watcher is
/// about to exit either is seen by that final look (the watcher keeps
/// running) or finds the watcher already unregistered (a new one starts).
/// Callers enqueue first and call [`LadderWatchers::ensure`] afterwards.
#[derive(Default)]
pub struct LadderWatchers {
    active: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl LadderWatchers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a watcher is registered for `run` (tests).
    pub fn is_active(&self, run: &str) -> bool {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(run)
    }

    /// Ensures a watcher runs for `run`. `pass` is one ladder pass; `idle`
    /// reports whether the run has no uninjected message left (leased ones
    /// count as not idle: their lease may lapse). The watcher exits only when
    /// a pass returned a terminal step AND `idle` holds.
    pub fn ensure(
        &self,
        run: &str,
        mut pass: impl FnMut() -> LadderStep + Send + 'static,
        idle: impl Fn() -> bool + Send + 'static,
    ) {
        let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        if !active.insert(run.to_string()) {
            return;
        }
        let registry = std::sync::Arc::clone(&self.active);
        let name = run.to_string();
        let spawned = std::thread::Builder::new()
            .name(format!("a2a-ladder-{name}"))
            .spawn(move || loop {
                let step = pass();
                if matches!(step, LadderStep::Injected | LadderStep::Expired { .. }) {
                    let mut active = registry.lock().unwrap_or_else(|e| e.into_inner());
                    if idle() {
                        active.remove(&name);
                        return;
                    }
                }
                std::thread::sleep(LADDER_POLL);
            });
        if spawned.is_err() {
            // No thread: unregister so a later delivery can try again.
            active.remove(run);
        }
    }
}

/// Whether the message metadata requests the explicit `interrupt` escalation
/// (trusted request metadata; only exact truthy spellings count).
pub fn interrupt_requested(metadata: Option<&HashMap<String, Value>>) -> bool {
    let Some(value) = metadata.and_then(|m| m.get(META_INTERRUPT)) else {
        return false;
    };
    match value {
        Value::Bool(true) => true,
        Value::String(s) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn cfg(ack: u64, force: u64, ttl: u64) -> LadderConfig {
        LadderConfig {
            ack_secs: ack,
            force_secs: force,
            ttl_secs: ttl,
        }
    }

    fn input(
        queued_at: u64,
        now: u64,
        interrupt: bool,
        idle: bool,
        cfg: &LadderConfig,
    ) -> LadderInput<'_> {
        LadderInput {
            now,
            queued_at,
            interrupt,
            idle_turn_available: idle,
            expired: false,
            config: cfg,
        }
    }

    #[test]
    fn ladder_order_defaults_and_force_timeout() {
        let d = cfg(45, 0, 3600);
        // Queued inside the ack window; idle_turn past it (idle run). A busy
        // run stays queued however late (the default never kills).
        // interrupt:true escalates immediately; a configured force timeout
        // only past it (busy run included).
        assert_eq!(
            next_step(&input(1000, 1044, false, true, &d)),
            LadderStep::Queued
        );
        assert_eq!(
            next_step(&input(1000, 1045, false, true, &d)),
            LadderStep::IdleTurn
        );
        assert_eq!(
            next_step(&input(1000, 1000 + 3590, false, false, &d)),
            LadderStep::Queued,
            "a busy run stays queued however late inside the ttl"
        );
        assert_eq!(
            next_step(&input(1000, 1000, true, true, &d)),
            LadderStep::Interrupt
        );
        let forced = cfg(45, 300, 3600);
        assert_eq!(
            next_step(&input(1000, 1299, false, false, &forced)),
            LadderStep::Queued
        );
        assert_eq!(
            next_step(&input(1000, 1300, false, false, &forced)),
            LadderStep::Interrupt
        );
        // Past the TTL the message is never delivered (the sender's task
        // fails), even with interrupt:true; an already-recorded marker too.
        match next_step(&input(1000, 1000 + 3600, true, true, &d)) {
            LadderStep::Expired { reason } => {
                assert!(reason.contains("expired"), "{reason}");
                assert!(reason.contains("3600"), "{reason}");
            }
            other => panic!("expected Expired, got {other:?}"),
        }
        let mut i = input(1000, 1000, false, true, &d);
        i.expired = true;
        assert!(matches!(next_step(&i), LadderStep::Expired { .. }));
    }

    #[test]
    fn sender_task_state_maps_the_inbox() {
        assert_eq!(sender_task_state(1, None), SenderTaskState::Submitted);
        assert_eq!(sender_task_state(0, None), SenderTaskState::Working);
        assert_eq!(sender_task_state(1, None).as_str(), "submitted");
        assert_eq!(sender_task_state(0, None).as_str(), "working");
        let record = ExpiryRecord {
            message_id: "m1".to_string(),
            reason: "expired".to_string(),
            expired_at: 1,
            expired_at_ns: 0,
        };
        assert_eq!(
            sender_task_state(2, Some(&record)),
            SenderTaskState::Failed {
                reason: "expired".to_string()
            }
        );
        assert_eq!(sender_task_state(2, Some(&record)).as_str(), "failed");
    }

    #[test]
    fn expiry_records_roundtrip_and_clear() {
        let tmp = tempdir().unwrap();
        let inbox_dir = tmp.path().join("a2a-inbox/run-1");
        assert!(read_expiry(&inbox_dir).is_none());
        record_expiry(&inbox_dir, "m1", "first reason");
        record_expiry(&inbox_dir, "m2", "second reason");
        assert_eq!(read_expiry(&inbox_dir).unwrap().message_id, "m2");
        // A fresh delivery supersedes the failure.
        clear_expiry(&inbox_dir);
        assert!(read_expiry(&inbox_dir).is_none());
        // Overwrite the same message id: the newest reason wins.
        record_expiry(&inbox_dir, "m3", "ok");
        record_expiry(&inbox_dir, "m3", "newer");
        let rec = read_expiry(&inbox_dir).unwrap();
        assert_eq!(rec.message_id, "m3");
        assert_eq!(rec.reason, "newer");
    }

    #[test]
    fn expiry_last_recorded_wins_within_the_same_second() {
        // Ten records recorded back-to-back without sleeping: they all share
        // the one-second `expired_at`, so the monotonic sub-second ordering
        // must decide: the last recorded one always wins.
        for round in 0..3 {
            let tmp = tempdir().unwrap();
            let inbox_dir = tmp.path().join(format!("a2a-inbox/run-{round}"));
            for i in 0..10 {
                let id = format!("m{i}");
                record_expiry(&inbox_dir, &id, "reason");
            }
            let got = read_expiry(&inbox_dir).unwrap();
            assert_eq!(
                got.message_id, "m9",
                "round {round}: the last recorded marker must win"
            );
        }
        // Reversed id order too: the winner is the last recorded, not the
        // largest or smallest id.
        let tmp = tempdir().unwrap();
        let inbox_dir = tmp.path().join("a2a-inbox/reversed");
        for i in (0..10u32).rev() {
            record_expiry(&inbox_dir, &format!("m{i}"), "reason");
        }
        assert_eq!(read_expiry(&inbox_dir).unwrap().message_id, "m0");
    }

    #[test]
    fn interrupt_metadata_is_parsed_from_trusted_request_metadata_only() {
        let mut meta = HashMap::new();
        assert!(!interrupt_requested(None));
        assert!(!interrupt_requested(Some(&meta)));
        meta.insert(META_INTERRUPT.to_string(), serde_json::json!(true));
        assert!(interrupt_requested(Some(&meta)));
        for spell in ["true", "1", "yes", "on", "TRUE"] {
            meta.insert(META_INTERRUPT.to_string(), serde_json::json!(spell));
            assert!(interrupt_requested(Some(&meta)), "{spell}");
        }
        // Absent, null, false and non-truthy strings all count as "no".
        for not in [
            serde_json::json!(false),
            serde_json::json!("false"),
            serde_json::json!("0"),
            serde_json::json!("no"),
            serde_json::json!(""),
            serde_json::json!(1),
            serde_json::json!("garbage"),
        ] {
            meta.insert(META_INTERRUPT.to_string(), not.clone());
            assert!(!interrupt_requested(Some(&meta)), "{not}");
        }
    }
}

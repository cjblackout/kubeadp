//! Live delivery of A2A messages to a RUNNING job over its stdin pipe.
//!
//! Design (HL-113): instead of killing a running `agy` attempt and resuming
//! the job with the message as a follow-up turn (the interrupt-and-resume
//! path in `worker/a2a.rs`), a job that is a member of a real A2A mesh
//! (jobset) is launched in *live mode*: its `agy` process reads newline
//! delimited JSON from a pipe the worker holds as its stdin (`--print ""
//! --input-format stream-json`), and an inbound A2A message is delivered as a
//! single `{"event":"user","message":{"content":"..."}}` line over that pipe —
//! no process kill, no restart, the same conversation just continues.
//!
//! # Live eligibility (the mesh predicate)
//!
//! A job is live-eligible only when it belongs to a *real* mesh:
//! - the worker process is itself a jobset member (`host_jobset().is_some()`:
//!   its environment carries `KUBEADP_JOBSET_ID`), so the job joined the
//!   host's mesh, OR
//! - the job was explicitly started into a caller-chosen jobset
//!   (`agent-run start --jobset <id>`, the `register_agent_into` path).
//!
//! It is NOT enough that [`crate::core::jobset::agent_jobset`] /
//! [`crate::core::jobset::recall_job_jobset`] return something: the
//! `job-jobsets/<job_id>` marker is written for EVERY job by the launcher
//! (`runner.rs`, including fresh random jobsets an operator request gets), so
//! marker existence ≠ mesh membership. This module therefore maintains its own
//! per-job marker file `<state_root>/job-live/<job_id>` set by `commands::start`
//! (explicit `--jobset`, or `None` when the host is a member) and read by the
//! deliverer. It mirrors the `job-jobsets` marker lifecycle: persists across
//! retries, dropped/cleaned with the rest of the job's state.
//!
//! # The live channel
//!
//! The monitor (`runner::run_internal_runner`) creates the pipe, points the
//! spawned child's stdin at it (spawn happens only after the eligibility was
//! known) and registers a [`LiveChannel`] in this module's process-wide
//! registry holding the write end plus the process identity (pid + start
//! time). `worker/a2a.rs::deliver` looks the channel up, verifies the process
//! identity still matches (so a recycled pid can never be written into), and
//! writes ONE framed line under the channel's own mutex (messages can exceed
//! `PIPE_BUF`, so pipe atomicity is not relied on; the mutex serializes
//! concurrent deliveries). A failure / absent / stale channel is unregistered
//! and the delivery falls through to the existing interrupt-and-resume path.
//!
//! The channel is closed (its `Drop`-less cleanup) on EVERY monitor exit path
//! through [`LiveChannelGuard`]: taking the stored stdin handle and dropping
//! it sends EOF to `agy`, which in stream-json mode finishes its current turn
//! and exits normally, so the job goes `Done` through the ordinary classifier.
//!
//! # Idle close
//!
//! When `config.a2a_live_idle_secs > 0`, a per-channel watchdog thread closes
//! the stdin (EOF) once the channel has been idle for that long AND the job's
//! log file has not been written for that long either: no new message and no
//! turn output means `agy` finished the turn and is just waiting for input, so
//! EOF lets it exit instead of hanging. `0` (never close) is the "stay until
//! the job ends on its own" mode. Any watchdog errors are best-effort.

use std::collections::HashMap;
use std::io::{Error as IoError, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::ChildStdin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use crate::core::job::is_pid_alive;

/// The live-registry contents: job id -> its live channel.
pub type LiveMap = HashMap<String, Arc<LiveChannel>>;

/// Process-wide registry: job id -> its live channel (the stdin write end +
/// process identity). The `Arc` lets a delivery and the watchdog hold the same
/// channel while the monitor owns the registry slot. Created on first use; a
/// test swap mirrors `jobset`'s registry pattern.
static GLOBAL_LIVE: std::sync::Mutex<Option<Arc<Mutex<LiveMap>>>> = std::sync::Mutex::new(None);

#[cfg(test)]
thread_local! {
    static TEST_LOCAL_LIVE: std::cell::RefCell<Option<Arc<Mutex<LiveMap>>>> =
        const { std::cell::RefCell::new(None) };
}

/// The process-wide live registry (an owned clone), created on first use.
pub fn live_registry() -> Arc<Mutex<LiveMap>> {
    // Tests: a thread inside `with_isolated_live` sees its own registry, so
    // concurrently running tests on other threads never write into it.
    #[cfg(test)]
    if let Some(local) = TEST_LOCAL_LIVE.with(|c| c.borrow().clone()) {
        return local;
    }
    let mut slot = GLOBAL_LIVE.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_none() {
        *slot = Some(Arc::new(Mutex::new(HashMap::new())));
    }
    slot.as_ref().cloned().expect("registry initialized above")
}

/// Runs `f` with the live registry swapped for a fresh, empty one, then
/// restores it. Test-only. Only the CALLING thread sees the swap (others
/// share the process-wide registry), mirroring `jobset::with_isolated_registry`
/// so parallel tests never clobber each other's channels.
#[cfg(test)]
pub fn with_isolated_live<R>(f: impl FnOnce() -> R) -> R {
    let previous =
        TEST_LOCAL_LIVE.with(|c| c.borrow_mut().replace(Arc::new(Mutex::new(HashMap::new()))));
    let out = f();
    TEST_LOCAL_LIVE.with(|c| *c.borrow_mut() = previous);
    out
}

// ---------------------------------------------------------------------------
// Per-job live membership marker (`<state_root>/job-live/<job_id>`)
// ---------------------------------------------------------------------------

/// Directory (under the state root) holding the per-job live-membership
/// markers. Mirrors the `job-jobsets` marker store in structure and lifecycle.
pub const LIVE_MARKER_DIR: &str = "job-live";

fn live_marker_path(state_root: &Path, job_id: &str) -> PathBuf {
    state_root.join(LIVE_MARKER_DIR).join(job_id)
}

/// Records whether `job_id` is a member of a real A2A mesh and so live-mode
/// eligible (`[worker] a2a_live_stdin` permitting). Written by
/// `commands::start` when the job's jobset is decided: an explicit call
/// `--jobset <id>` (a real mesh) or a host that is itself a jobset member.
/// Best effort (a missing marker reads as not-eligible).
pub fn set_live_member(state_root: &Path, job_id: &str, member: bool) {
    let path = live_marker_path(state_root, job_id);
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if member {
        let _ = std::fs::write(path, b"1");
    } else {
        let _ = std::fs::write(path, b"0");
    }
}

/// Whether `job_id` carries a live-membership marker (`1`). Absent / unreadable
/// marker reads as false (not live-eligible).
pub fn live_member(state_root: &Path, job_id: &str) -> bool {
    std::fs::read_to_string(live_marker_path(state_root, job_id))
        .map(|s| s.trim() == "1")
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// The live channel
// ---------------------------------------------------------------------------

/// A running job's stdin-delivery channel: the pipe write end plus the process
/// identity the channel was registered with (used to refuse writing into a
/// recycled pid). Shared between the monitor (which registered it), the
/// deliverer and the idle watchdog through the registry's `Arc`.
pub struct LiveChannel {
    /// The agy process pid the channel was registered for.
    pub pid: u32,
    /// The process start time recorded at spawn (identity, like the job row).
    pub pid_start_time: Option<u64>,
    /// The stdin write end. `Some` while the channel is open; taking and
    /// dropping it sends EOF to the child. Guarded by the mutex so concurrent
    /// deliveries are serialized (one framed line at a time) and closing never
    /// races a write.
    stdin: Mutex<Option<ChildStdin>>,
    /// Last activity: set on register and on every successful delivery.
    last_activity: Mutex<Instant>,
    /// Path of the job's log file; the idle watchdog's second inactivity
    /// signal (a log line written recently = the turn is still producing
    /// output, so stdin stays open).
    log_path: PathBuf,
    /// Set once the channel is closed so the watchdog (and a racing
    /// deliverer) can stop without touching the pipe again.
    closed: AtomicBool,
}

impl LiveChannel {
    /// Whether the channel still refers to the live process described by the
    /// job row: the pid matches, the process is alive, and the recorded start
    /// time matches the row's (a reused pid with a different start time is
    /// refused). Mirrors how `compute_status` decides a job is `Running`.
    pub fn is_alive_for(&self, meta_pid: Option<u32>, meta_start: Option<u64>) -> bool {
        match meta_pid {
            Some(p) if p == self.pid && is_pid_alive(p) => {
                match (self.pid_start_time, meta_start) {
                    (Some(a), Some(b)) => a == b,
                    (None, None) => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    /// Writes one framed user message as a single NDJSON line followed by a
    /// newline. Serialized by the channel's mutex so two concurrent deliveries
    /// can never interleave bytes (a line longer than `PIPE_BUF` is written in
    /// several `write` calls, but never concurrently with another delivery).
    pub fn deliver_line(&self, text: &str) -> Result<(), IoError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(IoError::new(ErrorKind::BrokenPipe, "live channel closed"));
        }
        let line = frame_user_line(text);
        let mut guard = self.stdin.lock().expect("live stdin poisoned");
        match guard.as_mut() {
            Some(stdin) => {
                stdin.write_all(line.as_bytes())?;
                stdin.flush()
            }
            None => Err(IoError::new(
                ErrorKind::BrokenPipe,
                "live stdin already closed",
            )),
        }
    }

    /// Marks activity (called after a successful `deliver_line`); updates the
    /// watchdog's idle clock.
    pub fn touch(&self) {
        if let Ok(mut t) = self.last_activity.lock() {
            *t = Instant::now();
        }
    }

    /// Closes the channel: marks it closed and drops the stdin handle (EOF to
    /// the child). The registry entry is removed by the CALLER (`close_for` is
    /// the only mutator that touches the registry, to avoid a re-lock
    /// deadlock). Idempotent; safe from any thread. After this, a further
    /// `deliver_line` on the (now-unregistered) channel is refused.
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // Taking (and dropping) the handle closes the pipe -> EOF. Doing this
        // under our own mutex so a concurrent deliver_line cannot interleave.
        self.stdin.lock().expect("live stdin poisoned").take();
    }
}

/// Frames a delivery payload as the NDJSON line agy's stream-json input
/// expects: `{"event":"user","message":{"content":"<text>"}}` + newline. The
/// content is already-framed inbound A2A text (or, for the initial turn, the
/// job's normalized prompt); JSON-encoding makes it safe to embed arbitrary
/// text.
pub fn frame_user_line(text: &str) -> String {
    let value = serde_json::json!({ "event": "user", "message": { "content": text } });
    let mut s = serde_json::to_string(&value).expect("serializing a JSON string value cannot fail");
    s.push('\n');
    s
}

// ---------------------------------------------------------------------------
// Registration / lookup / close (used by the runner and the deliverer)
// ---------------------------------------------------------------------------

/// Registers the live channel for a running job. Called by the runner right
/// after the child's pid (and start time) were recorded, with the pipe write
/// end taken from the spawned child. Any pre-existing channel for the job is
/// replaced (a retry's own earlier attempt already cleaned its channel up).
pub fn register(
    job_id: &str,
    pid: u32,
    pid_start_time: Option<u64>,
    stdin: ChildStdin,
    log_path: PathBuf,
) {
    let channel = Arc::new(LiveChannel {
        pid,
        pid_start_time,
        stdin: Mutex::new(Some(stdin)),
        last_activity: Mutex::new(Instant::now()),
        log_path,
        closed: AtomicBool::new(false),
    });
    let reg = live_registry();
    let mut map = reg.lock().expect("live registry poisoned");
    if let Some(prev) = map.insert(job_id.to_string(), channel) {
        // A stale attempt's channel must be EOF'd before being replaced.
        prev.stdin.lock().expect("live stdin poisoned").take();
    }
}

/// Looks up the live channel for `job_id`, if one is registered.
pub fn lookup(job_id: &str) -> Option<Arc<LiveChannel>> {
    let reg = live_registry();
    let guard = reg.lock().expect("live registry poisoned");
    guard.get(job_id).cloned()
}

/// Unregisters and closes the live channel for `job_id`, if any (EOF to the
/// child). Called when a delivery is absent/stale/failed so the fallback
/// interrupt-and-resume path can take over without a half-open channel, and by
/// the guard on every monitor exit.
pub fn close_for(job_id: &str) {
    let reg = live_registry();
    let removed = reg.lock().expect("live registry poisoned").remove(job_id);
    if let Some(channel) = removed {
        channel.close();
    }
}

/// RAII guard that closes `job_id`'s live channel (EOF to the child) when it
/// drops. Held across a whole job/session lifetime (the monitor thread) so the
/// channel is cleaned up on every exit path -- normal completion, failure,
/// kill, a cancelled launch, a spawn error -- without each return site having
/// to call [`close_for`]. Mirrors [`crate::core::jobset::JobsetDropGuard`].
pub struct LiveChannelGuard {
    job_id: String,
}

impl LiveChannelGuard {
    /// Begins guarding `job_id`; its live channel is closed when this guard
    /// drops. Safe to create before the channel exists (close is a no-op).
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
        }
    }
}

impl Drop for LiveChannelGuard {
    fn drop(&mut self) {
        close_for(&self.job_id);
    }
}

// ---------------------------------------------------------------------------
// Idle watchdog
// ---------------------------------------------------------------------------

/// Poll interval of the idle watchdog.
const WATCHDOG_POLL: Duration = Duration::from_secs(1);

/// Spawns the idle watchdog for a live channel. When `idle_secs > 0`, the
/// watchdog closes the channel's stdin (EOf) once the channel has seen no
/// delivery AND the job's log file has been untouched for `idle_secs`: both
/// silent means `agy` finished the turn and is waiting for input, so EOF lets
/// it exit and the job classify `Done`. The watchdog stops when the channel is
/// closed/unregistered or the process is gone. Best effort: errors only warn.
///
/// `0` (never close) registers nothing and the channel lives until the job
/// ends on its own (or is killed/retried).
pub fn spawn_idle_watchdog(job_id: &str, idle_secs: u64) {
    if idle_secs == 0 {
        return;
    }
    let Some(channel) = lookup(job_id) else {
        return;
    };
    let job_id = job_id.to_string();
    let idle = Duration::from_secs(idle_secs);
    let _ = std::thread::Builder::new()
        .name(format!("agent-run-live-watchdog-{}", job_id))
        .spawn(move || {
            while !channel.closed.load(Ordering::SeqCst) && is_pid_alive(channel.pid) {
                let log_idle = channel
                    .log_path
                    .metadata()
                    .and_then(|m| m.modified())
                    .ok()
                    .map(|mtime| {
                        SystemTime::now()
                            .duration_since(mtime)
                            .unwrap_or(Duration::ZERO)
                    })
                    .unwrap_or(Duration::ZERO);
                let channel_idle = match channel.last_activity.lock() {
                    Ok(t) => t.elapsed(),
                    Err(_) => Duration::ZERO,
                };
                if channel_idle >= idle && log_idle >= idle {
                    // Both silent long enough: close stdin (EOF), let agy
                    // finish and exit; the job classifies Done normally.
                    close_for(&job_id);
                    break;
                }
                std::thread::sleep(WATCHDOG_POLL);
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn frame_user_line_is_single_ndjson_line() {
        let line = frame_user_line("plain text");
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1, "exactly one newline");
        let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(v["event"], "user");
        assert_eq!(v["message"]["content"], "plain text");
    }

    #[test]
    fn frame_user_line_json_encodes_embedded_newlines_and_quotes() {
        // The already-framed A2A text can contain newlines, quotes and the
        // nonce delimiters; it must arrive as ONE JSON-encoded line so agy's
        // NDJSON reader sees a single message.
        let text = "line one\n--- BEGIN CONTENT (abc) ---\n\"quoted\"\n--- END CONTENT (abc) ---";
        let line = frame_user_line(text);
        assert_eq!(
            line.matches('\n').count(),
            1,
            "content newlines must be escaped"
        );
        // JSON round-trip recovers the exact text.
        let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(v["message"]["content"], text);
    }

    #[test]
    fn live_member_marker_roundtrip() {
        with_isolated_live(|| {
            let tmp = tempdir().unwrap();
            assert!(!live_member(tmp.path(), "j1"));
            set_live_member(tmp.path(), "j1", true);
            assert!(live_member(tmp.path(), "j1"));
            set_live_member(tmp.path(), "j1", false);
            assert!(!live_member(tmp.path(), "j1"));
        });
    }

    #[test]
    fn register_lookup_close_roundtrip() {
        with_isolated_live(|| {
            // A real piped child gives us a ChildStdin to hold as the write end.
            let mut child = std::process::Command::new("/bin/cat")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn cat");
            let stdin = child.stdin.take().expect("piped stdin");
            let tmp = tempdir().unwrap();
            register(
                "job-live-1",
                child.id(),
                Some(7),
                stdin,
                tmp.path().join("j.log"),
            );
            assert!(lookup("job-live-1").is_some());
            close_for("job-live-1");
            assert!(lookup("job-live-1").is_none());
            // close for an unknown id is a no-op.
            close_for("never-registered");
            let _ = child.kill();
            let _ = child.wait();
        });
    }

    #[test]
    fn deliver_writes_framed_line_to_piped_stdin() {
        with_isolated_live(|| {
            // A child that appends its stdin to a file; the channel delivers
            // to it.
            let tmp = tempdir().unwrap();
            let out = tmp.path().join("received.txt");
            let out_str = out.to_string_lossy().into_owned();
            let mut child = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(format!("cat > {out_str}"))
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn cat");
            let stdin = child.stdin.take().expect("piped stdin");
            let pid = child.id();
            register("job-live-2", pid, None, stdin, tmp.path().join("j.log"));
            let ch = lookup("job-live-2").unwrap();
            assert!(ch.deliver_line("hello live").is_ok());
            ch.touch();
            drop(ch);
            close_for("job-live-2");
            let _ = child.wait();
            // The child read the line and wrote it to the file.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                let content = std::fs::read_to_string(&out).unwrap_or_default();
                if content.contains("hello live") {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "child never received the line; got: {content:?}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        });
    }

    #[test]
    fn close_sends_eof_and_deliver_then_fails() {
        with_isolated_live(|| {
            let tmp = tempdir().unwrap();
            let mut child = std::process::Command::new("/bin/cat")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn");
            let stdin = child.stdin.take().expect("piped stdin");
            register(
                "job-live-3",
                child.id(),
                None,
                stdin,
                tmp.path().join("j.log"),
            );
            close_for("job-live-3"); // drops write end -> EOF -> child exits
            let status = child.wait().expect("wait child");
            assert!(status.success(), "EOF must make the reader exit cleanly");
            // A delivery after close is refused (no channel).
            assert!(lookup("job-live-3").is_none());
        });
    }

    #[test]
    fn watchdog_closes_idle_channel() {
        // Unlike the other tests this one spawns a watchdog THREAD, which
        // shares the process-wide (global) registry -- it cannot see this
        // test thread's isolated registry. So register straight into the
        // global registry under a unique id (no other test touches the
        // global registry, they all isolate on their own thread) and let the
        // watchdog's cross-thread `close_for` find it there.
        let job_id = "job-live-watchdog-global";
        let tmp = tempdir().unwrap();
        let out = tmp.path().join("received.txt");
        let out_str = out.to_string_lossy().into_owned();
        // A stale job log (mtime far in the past): with no deliveries the
        // watchdog's "log silent" test passes immediately.
        let log = tmp.path().join("j.log");
        std::fs::write(&log, "old log line\n").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&log)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(3600))
            .unwrap();
        // The reader writes EOF (exits) when stdin closes; combined with the
        // stale log, the watchdog must close it.
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("cat > {out_str}"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn");
        let stdin = child.stdin.take().expect("piped stdin");
        let pid = child.id();
        register(job_id, pid, None, stdin, log);
        spawn_idle_watchdog(job_id, 1);
        // The child must exit (watchdog EOF) within a few seconds even though
        // nothing was ever delivered.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Ok(Some(st)) = child.try_wait() {
                break st;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "watchdog never closed the idle channel"
            );
            std::thread::sleep(Duration::from_millis(100));
        };
        assert!(status.success(), "EOF-driven exit must be clean");
        close_for(job_id);
    }
}

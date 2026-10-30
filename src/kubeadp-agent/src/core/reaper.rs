//! In-server zombie reaper for `agent-run-api` (which runs as PID 1 without
//! an init/tini in its pod).
//!
//! Why: every job monitor ([`crate::core::runner::run_internal_runner`]) owns its
//! agy child and always `wait()`s it, but agy (and the black-box/fake-agy
//! test harnesses) occasionally leave background children (`sleep 1 &`, ...)
//! that die while still reparented to the server (via
//! `PR_SET_CHILD_SUBREAPER`). With no init to reap them, they accumulate as
//! `[sleep] <defunct>` zombies for the pod's lifetime.
//!
//! Design ("least risky" of the two options considered -- the alternative
//! was a `tini` entrypoint, which would ALSO change signal delivery and the
//! container image, and cannot reap anything this server already owns):
//!
//! - A detached background thread calls `reap_round()` every 100 ms.
//! - `reap_round` first checks every TRACKED child (a pid the code itself is
//!   about to `Child::wait` on) in `/proc/<pid>/stat`; if any is a zombie,
//!   the whole round is skipped -- that owner is inside its own wait window
//!   and must not lose its exit status to a concurrent `waitpid(-1)`.
//! - Otherwise it drains `waitpid(-1, _, WNOHANG)` until empty. Any orphan
//!   (the normal case: agy's background children) is reaped and counted.
//!   If a TRACKED pid does exit inside the tiny scan-then-drain window
//!   (TOCTOU residue), its status is stashed and handed back to the owner
//!   through [`wait_stashed_exit`] instead of being lost.
//!
//! The plain sleep loop (no `SIGCHLD` handler) trades up to ~100 ms of
//! latency for zero interaction with tokio's signal registration and with
//! the graceful-shutdown path (`commands::shutdown`), which must be left
//! completely intact.

use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::process::ExitStatusExt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

#[cfg(test)]
pub static PROC_CHILD_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub fn lock_proc_child_tests() -> std::sync::MutexGuard<'static, ()> {
    // Poison-safe: a failed assertion in one test must not poison the /proc
    // lock for every other test that touches live process state.
    PROC_CHILD_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Test-only: when set to a pid, [`reap_round`]'s blind drain waits only for
/// that pid instead of `waitpid(-1)`. Under `cfg(test)` the suite runs many
/// tests in one process and other tests' `Command::output()`/`wait()`
/// children share that process; a `waitpid(-1)` drain would reap their
/// exited children and surface spurious ECHILD failures. Tests that spawn
/// a zombie they want [`reap_round`] to reap set this to that pid (see
/// [`set_test_reap_only`]). The production path never reads it.
///
/// An atomic (not a mutex): the guard returned by `set_test_reap_only`
/// lives across the `reap_round()` call that must still be able to read the
/// pid, so a reentrant lock would deadlock.
#[cfg(test)]
static TEST_REAP_ONLY: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// RAII guard that scopes a [`reap_round`] drain to one pid for the duration
/// of a test (see [`TEST_REAP_ONLY`]). Cleared on drop.
#[cfg(test)]
pub struct TestReapOnlyScope;

#[cfg(test)]
impl Drop for TestReapOnlyScope {
    fn drop(&mut self) {
        TEST_REAP_ONLY.store(0, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Restrict [`reap_round`]'s drain to `pid` until the returned guard is
/// dropped. See [`TEST_REAP_ONLY`].
#[cfg(test)]
pub fn set_test_reap_only(pid: u32) -> TestReapOnlyScope {
    TEST_REAP_ONLY.store(pid, std::sync::atomic::Ordering::SeqCst);
    TestReapOnlyScope
}

/// The pid (if any) a test asked [`reap_round`] to restrict its drain to.
#[cfg(test)]
fn test_reap_only() -> Option<i32> {
    let pid = TEST_REAP_ONLY.load(std::sync::atomic::Ordering::SeqCst);
    if pid == 0 {
        None
    } else {
        Some(pid as i32)
    }
}

#[derive(Clone)]
pub struct ReapedExit {
    pub code: Option<u32>,
    pub signal: Option<i32>,
    raw: i32,
}

impl ReapedExit {
    /// Recreate the `ExitStatus` the owner's `Child::wait` would have
    /// produced, so `exit_code_from_status` sees the same bits.
    pub fn into_exit_status(self) -> std::process::ExitStatus {
        std::process::ExitStatus::from_raw(self.raw)
    }
}

pub struct ReapRound {
    /// `true` when the round did nothing because a tracked child is
    /// currently a zombie (its owner must reap it first).
    pub skipped: bool,
    /// Tracked pids whose exit the reaper captured in the TOCTOU window
    /// (handed to the owner via [`wait_stashed_exit`]).
    pub stashed: Vec<u32>,
    /// Untracked (orphaned) children reaped this round.
    pub orphans: u32,
}

struct ReaperState {
    /// Pids owned by an in-flight `Child::wait` (registered by the runner).
    tracked: Mutex<BTreeSet<u32>>,
    /// Exits the reaper captured before their owner's `wait` could.
    stashed: Mutex<BTreeMap<u32, ReapedExit>>,
    stashed_cv: Condvar,
    orphans: AtomicU32,
}

fn state() -> &'static ReaperState {
    static STATE: OnceLock<ReaperState> = OnceLock::new();
    STATE.get_or_init(|| ReaperState {
        tracked: Mutex::new(BTreeSet::new()),
        stashed: Mutex::new(BTreeMap::new()),
        stashed_cv: Condvar::new(),
        orphans: AtomicU32::new(0),
    })
}

/// Register `pid` as owned by a `Child` handle the caller will `wait` on.
/// The reaper will never reap a tracked pid while it is a zombie.
pub fn track_child(pid: u32) {
    state().tracked.lock().unwrap().insert(pid);
}

pub fn untrack_child(pid: u32) {
    state().tracked.lock().unwrap().remove(&pid);
}

pub fn is_tracked(pid: u32) -> bool {
    state().tracked.lock().unwrap().contains(&pid)
}

fn stash_exit(pid: u32, status: i32) {
    let exit = ReapedExit {
        code: if (status & 0x7f) == 0 {
            Some(((status >> 8) & 0xff) as u32)
        } else {
            None
        },
        signal: if (status & 0x7f) != 0 {
            Some(status & 0x7f)
        } else {
            None
        },
        raw: status,
    };
    let (mut stashed, cv) = {
        let g = state().stashed.lock().unwrap();
        (g, &state().stashed_cv)
    };
    stashed.insert(pid, exit);
    drop(stashed);
    cv.notify_all();
}

/// Wait up to `timeout` for a stashed exit of `pid` (see the TOCTOU window
/// in [`reap_round`]'s docs). Returns `None` when it never arrived.
pub fn wait_stashed_exit(pid: u32, timeout: Duration) -> Option<ReapedExit> {
    let st = state();
    let started = Instant::now();
    loop {
        let guard = st.stashed.lock().unwrap();
        if let Some(exit) = guard.get(&pid).cloned() {
            drop(guard);
            st.stashed.lock().unwrap().remove(&pid);
            return Some(exit);
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return None;
        }
        drop(guard);
        let g = st.stashed.lock().unwrap();
        let _ = st.stashed_cv.wait_timeout(g, remaining);
    }
}

/// State char from `/proc/<pid>/stat` (`None` when the proc is gone or
/// `/proc` is unavailable).
fn proc_pid_state(pid: u32) -> Option<char> {
    let content = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = content.rfind(')')?;
    let rest = content.get(close + 1..)?.trim_start();
    let state = rest.chars().next()?;
    Some(state)
}

/// One synchronous reap pass. SAFE to call from any thread (the tests call
/// it directly; the background thread calls it in a loop). Never reaps a
/// tracked child while it is a zombie, and even in the TOCTOU residue window
/// hands the exit back to the owner (stashed) instead of losing it.
pub fn reap_round() -> ReapRound {
    let st = state();
    // Guard pass: any tracked zombie means an owner is inside its own
    // `Child::wait` window; stepping out of the way this round is what makes
    // the blind drain below race-free.
    let tracked: BTreeSet<u32> = st.tracked.lock().unwrap().clone();
    for pid in &tracked {
        if proc_pid_state(*pid) == Some('Z') {
            return ReapRound {
                skipped: true,
                stashed: Vec::new(),
                orphans: 0,
            };
        }
    }

    let mut stashed = Vec::new();
    let mut orphans = 0u32;
    loop {
        let mut status: i32 = 0;
        // Under `cfg(test)` the blind drain is either restricted to a single
        // pid a test registered (its own zombie, via `set_test_reap_only`) or
        // skipped entirely, so a test's `waitpid` can never steal the exited
        // children of other tests running in the same process. Production
        // always drains with -1 (any exitable child of this process).
        #[cfg(test)]
        let pid = match test_reap_only() {
            Some(pid) => {
                // SAFETY: `status` is in scope for the call; `pid` is the
                // test's own zombie child registered via `set_test_reap_only`.
                unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) }
            }
            None => break,
        };
        #[cfg(not(test))]
        let pid = unsafe {
            // SAFETY: `status` is in scope for the duration of the call;
            // `-1` with `WNOHANG` waits for any exitable child of this
            // process (children are process-wide on Linux, so this works
            // from a non-spawning thread).
            libc::waitpid(-1, &mut status, libc::WNOHANG)
        };
        if pid <= 0 {
            break;
        }
        let pid = pid as u32;
        if st.tracked.lock().unwrap().contains(&pid) {
            // TOCTOU residue: the child exited between the guard pass and
            // the drain. The owner's `Child::wait` will see ECHILD and take
            // the exit back from here.
            stash_exit(pid, status);
            stashed.push(pid);
        } else {
            orphans = orphans.saturating_add(1);
            st.orphans.fetch_add(1, Ordering::SeqCst);
        }
    }
    ReapRound {
        skipped: false,
        stashed,
        orphans,
    }
}

/// Total number of orphaned children this process has reaped since start.
pub fn orphan_reap_count() -> u32 {
    state().orphans.load(Ordering::SeqCst)
}

/// `true` when this process is PID 1 (the agent-run-api pod case: the
/// server is PID 1 with no init, so it must reap its own orphaned
/// children). Exposed here so binary crates do not need a direct `libc`
/// dependency for the check.
#[cfg(unix)]
pub fn is_pid1() -> bool {
    // SAFETY: no arguments.
    let pid = unsafe { libc::getpid() };
    pid == 1
}

#[cfg(not(unix))]
pub fn is_pid1() -> bool {
    false
}

/// `ECHILD` without a `libc` import at every call site.
const ECHILD: i32 = 10;

/// Remove a stash entry left unconsumed (only safe to call for a pid this
/// process's reaper stashed, i.e. a pid that was in the tracked set).
fn discard_stashed_exit(pid: u32) {
    state().stashed.lock().unwrap().remove(&pid);
}

/// RAII registration of a pid owned by an in-server `Command` whose exit the
/// caller will collect itself (via `try_wait`/`wait`/`wait_with_output`, NOT
/// a full `cmd.output()` poll-free path). While `pid` is tracked, the blind
/// `waitpid(-1)` drain in [`reap_round`] can never steal its exit status: it
/// skips the round while the child is a zombie, and if the child exits in the
/// tiny scan-then-drain TOCTOU window the status is stashed and handed back
/// through [`TrackedChild::recover_echild`] instead of being lost.
pub struct TrackedChild {
    pid: u32,
}

impl TrackedChild {
    /// Register `pid` with the reaper (see the type docs).
    pub fn new(pid: u32) -> Self {
        track_child(pid);
        TrackedChild { pid }
    }

    /// Take back the exit status when the owner's wait failed with ECHILD
    /// (the reaper won the TOCTOU race and stashed it in [`reap_round`]).
    /// Returns `None` when no stash entry exists -- then the ECHILD is
    /// genuine (e.g. the child was already fully reaped elsewhere) and the
    /// caller keeps its original error. The stash is consumed once, so
    /// repeated calls degrade to `None`.
    pub fn recover_echild(&self, err: &std::io::Error) -> Option<std::process::ExitStatus> {
        if err.raw_os_error() != Some(ECHILD) {
            return None;
        }
        let exit = wait_stashed_exit(self.pid, Duration::from_secs(2))?;
        Some(exit.into_exit_status())
    }
}

impl Drop for TrackedChild {
    fn drop(&mut self) {
        untrack_child(self.pid);
        // Remove any unconsumed stash entry so a later recycled pid cannot
        // pick up a stale status (no-op when the exit was already handed
        // back through `recover_echild`).
        discard_stashed_exit(self.pid);
    }
}

/// Drain one output pipe to EOF and return its bytes (runs on a reader
/// thread so the `try_wait` poll below is never blocked on pipe capacity).
fn read_pipe_to_vec(mut pipe: impl std::io::Read) -> Vec<u8> {
    let mut buf = Vec::new();
    // UFCS so the call does not depend on the `Read` trait being in scope.
    let _ = std::io::Read::read_to_end(&mut pipe, &mut buf);
    buf
}

/// Spawn `cmd` with stdout/stderr piped and wait for it to finish, with the
/// child registered in the reaper for the whole wait window (see
/// [`TrackedChild`]). Behaves like [`std::process::Command::output`], except
/// that if the in-server reaper wins the tiny scan-then-drain TOCTOU race and
/// `try_wait` fails with ECHILD, the exit status is taken back from the
/// reaper's stash instead of surfacing the spurious error. The pipes are
/// drained on reader threads and are complete in both outcomes.
pub fn output_tracked(cmd: &mut std::process::Command) -> std::io::Result<std::process::Output> {
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn()?;
    let tracked = TrackedChild::new(child.id());
    let stdout_pipe = child.stdout.take().expect("stdout is piped");
    let stderr_pipe = child.stderr.take().expect("stderr is piped");

    let stdout_reader = std::thread::spawn(move || read_pipe_to_vec(stdout_pipe));
    let stderr_reader = std::thread::spawn(move || read_pipe_to_vec(stderr_pipe));

    // Poll `try_wait` (the only std way to wait while inspecting each
    // iteration) so the ECHILD the reaper's drain can cause is caught and
    // translated into the stashed exit status.
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => match tracked.recover_echild(&e) {
                Some(status) => break status,
                None => return Err(e),
            },
        }
    };
    // The child is gone, so the write ends of both pipes are closed and the
    // reader threads reach EOF; join them to collect the full output.
    drop(child);
    let stdout = stdout_reader.join().expect("stdout reader thread panicked");
    let stderr = stderr_reader.join().expect("stderr reader thread panicked");
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

/// Spawn `cmd` and wait for it to finish, with the child registered in the
/// reaper for the whole wait window (see [`TrackedChild`]). Behaves like
/// [`std::process::Command::status`], except that an ECHILD caused by the
/// in-server reaper's TOCTOU race is recovered from the reaper's stash. Both
/// stdout and stderr are inherited (as [`std::process::Command::status`]
/// does), so callers that only need the exit status get it cheaply.
pub fn status_tracked(
    cmd: &mut std::process::Command,
) -> std::io::Result<std::process::ExitStatus> {
    let mut child = cmd.spawn()?;
    let tracked = TrackedChild::new(child.id());
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => match tracked.recover_echild(&e) {
                Some(status) => return Ok(status),
                None => return Err(e),
            },
        }
    }
}

/// Start the background reaper thread (idempotent; at most one per process).
/// The pod runs the server as PID 1 without init, so orphaned/defunct
/// children would otherwise stay zombies until the pod restarts. Intended to
/// be called from `agent-run-api` main when `getpid() == 1`.
///
/// DO NOT call this from test binaries: the loop would race
/// `Child::wait`-based tests in other modules. Unit tests exercise
/// [`reap_round`] directly instead.
pub fn start_reaper_thread() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.get().is_some() {
        return;
    }
    let _ = STARTED.set(());
    let handle = std::thread::Builder::new()
        .name("agent-run-zombie-reaper".to_string())
        .spawn(|| loop {
            let _ = reap_round();
            std::thread::sleep(Duration::from_millis(100));
        });
    // Detached: the reaper lives as long as the process, on purpose.
    if let Ok(handle) = handle {
        std::mem::forget(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::reaper::lock_proc_child_tests;

    /// Poll `/proc/<pid>/stat` until the process is a zombie (deadline 10 s;
    /// raised from 2 s -- a `sh -c true` child that exits immediately still
    /// needs the kernel to schedule its death on a loaded CI box, and the
    /// short deadline was the flaky part).
    fn wait_zombie(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if proc_pid_state(pid) == Some('Z') {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "pid {pid} did not become a zombie within 10s"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    // `clippy::zombie_processes` is suppressed intentionally: the whole point
    // of the helper is to leave the child unwaited (dropping the `Child`
    // handle) so it becomes a zombie the reaper must reap -- that is the
    // precondition of `reaper_reaps_untracked_zombie` and
    // `reaper_never_steals_a_tracked_child`. Both callers reap it in the
    // same test (via `reap_round` / an explicit `waitpid`), so no actual
    // zombie is left behind.
    #[allow(clippy::zombie_processes)]
    fn spawn_exiting_child() -> u32 {
        // A child that exits immediately; we deliberately do NOT wait on it
        // (dropping the `Child` handle) so it becomes a zombie owned by us.
        let child = std::process::Command::new("sh")
            .arg("-c")
            .arg("true")
            .spawn()
            .expect("spawn sh for reaper test");
        child.id()
    }

    #[test]
    fn reaper_reaps_untracked_zombie() {
        let _lock = lock_proc_child_tests();
        let before = orphan_reap_count();
        let pid = spawn_exiting_child();
        wait_zombie(pid);
        // Scope the drain to our own zombie so it cannot touch the exited
        // children of other tests running in this process.
        let _reap_only = set_test_reap_only(pid);

        let round = reap_round();
        assert!(
            !round.skipped,
            "no tracked child is a zombie, so the round must not skip"
        );
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "the reaper must have reaped the untracked zombie {pid}"
        );
        assert!(
            orphan_reap_count() >= before.saturating_add(1),
            "orphan counter must advance ({before} -> {})",
            orphan_reap_count()
        );
    }

    #[test]
    fn reaper_never_steals_a_tracked_child() {
        let _lock = lock_proc_child_tests();
        let pid = spawn_exiting_child();
        track_child(pid);
        wait_zombie(pid);
        // The drain is scoped to our own pid; the guard pass below still
        // must defer to the tracked owner regardless.
        let _reap_only = set_test_reap_only(pid);

        // The guard pass must defer to the (future) owner's wait.
        let round = reap_round();
        assert!(
            round.skipped,
            "a tracked zombie must make the round skip (orphans: {}, stashed: {:?})",
            round.orphans, round.stashed
        );
        assert_eq!(round.stashed, Vec::<u32>::new());

        // Prove the exit status is still the owner's to collect: a direct
        // waitpid for that pid succeeds with its status.
        let mut status: i32 = 0;
        let r = unsafe {
            // SAFETY: status is in scope for the call; pid is our own child.
            libc::waitpid(pid as i32, &mut status, 0)
        };
        assert_eq!(
            r, pid as i32,
            "the owner must still be able to waitpid {pid}"
        );
        assert_eq!(((status >> 8) & 0xff) as u32, 0, "sh -c true exits 0");
        untrack_child(pid);
    }

    #[test]
    fn stashed_exit_handoff_roundtrip() {
        // The TOCTOU hand-off mechanism in isolation: stash an exit for a
        // tracked pid and take it back the way the runner's ECHILD path
        // does. (The real TOCTOU window itself is timing-dependent and is
        // not asserted; the mechanism is.)
        let pid = 999_999u32; // cannot collide with a live pid in tests
        track_child(pid);
        // raw status for "exited with code 5": exit code lives in bits 8..16.
        stash_exit(pid, 5 << 8);
        let got = wait_stashed_exit(pid, Duration::from_secs(1)).expect("stashed exit was taken");
        assert_eq!(got.code, Some(5));
        assert_eq!(got.signal, None);
        assert_eq!(got.into_exit_status().code(), Some(5));
        assert!(
            wait_stashed_exit(pid, Duration::from_millis(50)).is_none(),
            "a stashed exit is consumed exactly once"
        );
        untrack_child(pid);
    }

    #[test]
    fn track_untrack_roundtrip() {
        assert!(!is_tracked(424_242));
        track_child(424_242);
        assert!(is_tracked(424_242));
        untrack_child(424_242);
        assert!(!is_tracked(424_242));
    }

    #[test]
    fn output_tracked_survives_concurrent_reap_round() {
        let _lock = lock_proc_child_tests();
        let before = orphan_reap_count();

        // First create the orphaned grandchild the drain below will focus on:
        // a `sh` parent that backgrounds `sleep 0.2`, prints its pid, and
        // exits. As a subreaper, the grandchild reparents here when `sh`
        // dies and is the drain's target for the rest of the test.
        crate::core::runner::mark_child_subreaper();
        let parent = std::process::Command::new("sh")
            .arg("-c")
            .arg("sleep 0.2 & echo $!")
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn parent for orphan test");
        let out = parent.wait_with_output().expect("wait for parent");
        let grandchild: u32 = String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .expect("sh must print the background grandchild pid");
        // Scope the drain to the grandchild for the whole test: the reaper
        // loop below then actively waits ONLY that pid, so it exercises the
        // concurrent reap while never stealing other tests' children.
        let _reap_only = set_test_reap_only(grandchild);

        // A reaper loop on another thread, exactly like the production
        // background thread (minus its 100 ms sleep: the tighter the loop, the
        // wider the window the tracked registration must cover).
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_flag = std::sync::Arc::clone(&stop);
        let reaper_thread = std::thread::spawn(move || {
            while !stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = reap_round();
            }
        });

        // 1) A tracked + waited helper child must get its exact exit status
        // back even though the reaper is draining concurrently. Repeat a few
        // times to widen the race window, as the full-suite runs do.
        for expected in [3, 5, 7] {
            let mut cmd = std::process::Command::new("sh");
            cmd.arg("-c").arg(format!("echo hi; exit {expected}"));
            let output = output_tracked(&mut cmd)
                .expect("output_tracked must not surface the reaper's TOCTOU ECHILD");
            assert_eq!(output.status.code(), Some(expected));
            assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hi");
        }

        // 2) The orphaned grandchild (untracked, reparented here) must STILL
        // be reaped by the loop, and the orphan counter must advance.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if orphan_reap_count() > before {
                stop.store(true, std::sync::atomic::Ordering::SeqCst);
                reaper_thread.join().expect("reaper test thread panicked");
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        reaper_thread.join().expect("reaper test thread panicked");
        panic!(
            "the concurrent reap_round loop must reap the orphaned grandchild {} (before={}, now={})",
            grandchild,
            before,
            orphan_reap_count()
        );
    }
}

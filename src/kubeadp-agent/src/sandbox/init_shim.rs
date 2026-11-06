//! Lightweight init shim for PID namespaces and tree-killing supervision.
//!
//! Handles:
//! - PID 1 reaping of adopted child processes.
//! - Signal forwarding (SIGTERM, SIGINT) to child process group / child tree.
//! - Propagating exit status (exit code or 128 + signal).
//! - Clean termination of remaining processes in the sandbox before exit.

use std::sync::atomic::{AtomicI32, Ordering};

static FORWARD_SIG: AtomicI32 = AtomicI32::new(0);

extern "C" fn sig_handler(sig: libc::c_int) {
    FORWARD_SIG.store(sig, Ordering::SeqCst);
}

/// Installs the SIGTERM/SIGINT handlers that record the signal for forwarding.
///
/// The gVisor-style launcher's shim runs as PID 1 of a fresh PID namespace; the
/// kernel does not deliver a signal with default disposition to a namespace
/// init, so a SIGTERM aimed at the shim before these handlers exist is silently
/// dropped. The shim therefore installs them as its very first action. Calling
/// it again later (from [`run_init_shim`]) re-installs the same handler and is
/// harmless.
pub fn install_shim_signal_handlers() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = sig_handler as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
    }
}

/// Waits for a child process while forwarding SIGTERM and SIGINT to it,
/// returning the child's exit code once it finishes.
pub fn wait_with_signal_forwarding(child_pid: libc::pid_t) -> i32 {
    install_shim_signal_handlers();

    loop {
        // Read (do not clear) the recorded signal and re-send it on every pass
        // until the child exits. A forward that lands in the window before the
        // namespace's PID 1 has installed its handler is dropped by the kernel;
        // retrying -- standard signals coalesce while pending -- makes the
        // forward survive that window instead of being given up after one shot.
        let sig = FORWARD_SIG.load(Ordering::SeqCst);
        if sig != 0 {
            unsafe {
                libc::kill(child_pid, sig);
            }
        }

        let mut status: libc::c_int = 0;
        let reaped = unsafe { libc::waitpid(child_pid, &mut status, libc::WNOHANG) };
        if reaped > 0 {
            return status_to_exit_code(status);
        } else if reaped == 0 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        } else {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            break;
        }
    }
    1
}

/// Runs the init shim loop around `target_pid`.
/// Reaps any adopted zombies, forwards signals to `target_pid`,
/// and when `target_pid` exits, kills all remaining processes (SIGTERM, then SIGKILL if needed)
/// and returns the exit status code (standard Unix: 0-255 or 128 + sig).
pub fn run_init_shim(target_pid: libc::pid_t) -> i32 {
    // Install the signal handler for SIGTERM and SIGINT (idempotent with the
    // early install the shim performs for the namespace-init case).
    install_shim_signal_handlers();

    let mut target_exit_code = None;

    loop {
        // Check if we received a signal to forward
        let sig = FORWARD_SIG.swap(0, Ordering::SeqCst);
        if sig != 0 && target_exit_code.is_none() {
            unsafe {
                // Forward signal to target process (or negative pid for its process group)
                libc::kill(target_pid, sig);
            }
        }

        // Wait for any child
        let mut status: libc::c_int = 0;
        let reaped = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };

        if reaped > 0 {
            if reaped == target_pid {
                let code = status_to_exit_code(status);
                target_exit_code = Some(code);
                // Target has exited! Now break and clean up any remaining children
                break;
            }
            // An adopted child was reaped; continue loop
            continue;
        } else if reaped == 0 {
            // No status change; sleep a tiny slice or wait for signal
            std::thread::sleep(std::time::Duration::from_millis(10));
        } else {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ECHILD) {
                // No more children at all
                break;
            }
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            // Other error
            break;
        }
    }

    let final_code = target_exit_code.unwrap_or(1);

    // Tree cleanup: kill all remaining processes in this namespace/tree
    kill_tree_and_reap();

    final_code
}

/// Maps raw libc waitpid status to exit code (0-255 or 128 + signal).
pub fn status_to_exit_code(status: libc::c_int) -> i32 {
    if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else if libc::WIFSIGNALED(status) {
        128 + libc::WTERMSIG(status)
    } else {
        1
    }
}

// The helpers below use raw syscalls and only stack memory. They must stay
// allocation-free: `kill_tree_and_reap` is executed inside a forked child
// (the subreaper / PID 1), where calling into the Rust heap is unsafe, and the
// unit test runs it in a forked child too.

/// Writes the NUL-terminated path `/proc/<pid>/stat` into `buf`. Uses only
/// stack memory (no allocation, no libc locks), so it is safe in a forked
/// child. Returns the number of bytes written, NUL included.
fn proc_stat_path(pid: i32, buf: &mut [i8]) -> usize {
    let prefix: &[u8] = b"/proc/";
    let suffix: &[u8] = b"/stat\0";
    let mut digits = [0i8; 10];
    let mut nd = 0usize;
    let mut v = pid as u32;
    loop {
        digits[nd] = (v % 10) as i8 + b'0' as i8;
        nd += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let mut i = 0usize;
    for &c in prefix {
        buf[i] = c as i8;
        i += 1;
    }
    for k in (0..nd).rev() {
        buf[i] = digits[k];
        i += 1;
    }
    for &c in suffix {
        buf[i] = c as i8;
        i += 1;
    }
    i
}

/// Reads the parent PID and zombie state from `/proc/<pid>/stat` via raw
/// syscalls (no allocation). Returns `Some((ppid, is_zombie))`.
///
/// The comm field (field 2) is wrapped in parentheses and may itself contain
/// spaces or `)`, so the ppid is parsed as the integer following the *last*
/// closing paren.
fn raw_ppid(pid: i32) -> Option<(i32, bool)> {
    let mut path = [0i8; 32];
    proc_stat_path(pid, &mut path);
    unsafe {
        let fd = libc::open(path.as_ptr(), libc::O_RDONLY);
        if fd < 0 {
            return None;
        }
        let mut buf = [0u8; 512];
        let r = libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len());
        libc::close(fd);
        if r <= 0 {
            return None;
        }
        let data = &buf[..r as usize];
        let _open = data.iter().position(|&b| b == b'(')?;
        let close = data.iter().rposition(|&b| b == b')')?;
        let after = &data[close + 1..];
        let mut i = 0;
        while i < after.len() && after[i] == b' ' {
            i += 1;
        }
        if i >= after.len() {
            return None;
        }
        let state = after[i];
        i += 1;
        while i < after.len() && after[i] == b' ' {
            i += 1;
        }
        let mut ppid = 0i64;
        let mut found = false;
        while i < after.len() {
            if !after[i].is_ascii_digit() {
                break;
            }
            ppid = ppid * 10 + (after[i] - b'0') as i64;
            found = true;
            i += 1;
        }
        if !found || ppid <= 0 {
            return None;
        }
        Some((ppid as i32, state == b'Z'))
    }
}

/// True if `pid` still exists and is not a zombie (raw, no allocation).
///
/// A process's `/proc/<pid>/stat` is gone once the process is fully reaped, and
/// carries a `Z` (zombie) state once it has exited but not yet been reaped;
/// both mean "not alive", so this is true only for a state other than `Z`.
fn raw_is_alive(pid: i32) -> bool {
    matches!(raw_ppid(pid), Some((_, false)))
}

/// True if `pid` exists and is in a stopped (`T`/`t`) state (raw, no
/// allocation). Used to verify the "buffer full" freeze (SIGSTOP) actually
/// took effect in the descendant-tree test.
#[cfg(test)]
fn raw_is_stopped(pid: i32) -> bool {
    let mut path = [0i8; 32];
    proc_stat_path(pid, &mut path);
    unsafe {
        let fd = libc::open(path.as_ptr(), libc::O_RDONLY);
        if fd < 0 {
            return false;
        }
        let mut buf = [0u8; 256];
        let r = libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len());
        libc::close(fd);
        if r <= 0 {
            return false;
        }
        let data = &buf[..r as usize];
        let close = match data.iter().rposition(|&b| b == b')') {
            Some(c) => c,
            None => return false,
        };
        let after = &data[close + 1..];
        let mut i = 0;
        while i < after.len() && after[i] == b' ' {
            i += 1;
        }
        i < after.len() && (after[i] == b'T' || after[i] == b't')
    }
}

/// The `getdents64` syscall number, per the running architecture.
const SYS_GETDENTS64: libc::c_long = if cfg!(target_arch = "aarch64") {
    61
} else if cfg!(target_arch = "riscv64") {
    28
} else {
    // x86 / x86_64
    217
};

// Fixed stack buffer sizes (bytes = N * 4 for an `i32` array). A pod's PID
// namespace is bounded by the kubelet `--pod-max-pids` (up to 10000 on the
// gVisor node), so `MAX_SCAN` comfortably covers the whole namespace even on a
// busy host; `MAX_DESC` bounds a single process's descendant tree. `MAX_DESC`
// is deliberately larger than the pod's pid cap so a whole tree fits in one
// scan (the old 4096 cap silently dropped the tail of a large tree, leaving
// survivors and spinning). If the buffer ever DOES fill, `raw_descendants`
// treats that as an explicit state and loops until no descendants remain or a
// deadline is reached instead of capping.
const MAX_SCAN: usize = 32768;
const MAX_DESC: usize = 16384;

/// Fills `out` with every numeric directory name (PID) present in `/proc`.
/// Uses `getdents64` via a raw syscall and only stack memory (no heap), so it
/// is safe to call from a forked child. Returns the number of PIDs written.
fn raw_list_pids(out: &mut [i32; MAX_SCAN]) -> usize {
    let mut n = 0usize;
    let mut buf = [0u8; 16384];
    let mut digits = [0u8; 24];
    unsafe {
        let fd = libc::open(c"/proc".as_ptr(), libc::O_RDONLY);
        if fd < 0 {
            return 0;
        }
        loop {
            let r = libc::syscall(SYS_GETDENTS64, fd, buf.as_mut_ptr(), buf.len());
            if r <= 0 {
                break;
            }
            let mut off = 0usize;
            while off + 20usize <= r as usize {
                let e = &buf[off..];
                // linux_dirent64: d_ino(8) d_off(8) d_reclen(2) d_type(1) name..
                let reclen = (e[16] as usize) | ((e[17] as usize) << 8);
                if reclen == 0 || off + reclen > r as usize {
                    break;
                }
                let name = &e[19..reclen];
                let mut nd = 0usize;
                while nd < name.len() && name[nd].is_ascii_digit() {
                    digits[nd] = name[nd];
                    nd += 1;
                }
                if nd > 0 {
                    let mut v: i32 = 0;
                    for &d in &digits[..nd] {
                        v = v * 10 + (d - b'0') as i32;
                    }
                    if v > 0 && n < out.len() {
                        out[n] = v;
                        n += 1;
                    }
                }
                off += reclen;
            }
        }
        libc::close(fd);
    }
    n
}

/// Collects the PIDs of every strict descendant of `root` into the buffer
/// `out`, returning the count. Does a breadth-first walk following the
/// `/proc/<pid>/stat` ppid links (a zombie still exposes its `/proc/<pid>/stat`
/// with its ppid, so the walk passes through already-dead intermediates).
/// Because `root` is a subreaper (or PID 1 of a namespace), orphaned descendants
/// re-parent to `root`, so the walk reaches exactly the processes that should
/// be torn down and nothing else.
///
/// The accumulator `out` doubles as the reached-set: a pid is reachable when
/// its ppid is `root` or is already in `out`. The walk loops until a pass adds
/// nothing (the set is stable) OR the `deadline` is reached, so a forked
/// descendant that keeps spawning children can never make the walk loop
/// forever. When `out` is full that is an EXPLICIT state (not a silent cap):
/// the collected pids are SIGSTOPped so a forker cannot outrun the snapshot,
/// and the walk keeps re-scanning until the deadline, returning however many
/// fit. `out` must be at least as large as the largest tree the caller cares
/// about (in production it is `MAX_DESC`, larger than the pod pid cap, so the
/// full state is only ever hit by a pathological tree). Uses only stack memory
/// (no heap), so it is safe in a forked child.
fn raw_descendants(root: i32, out: &mut [i32], deadline: std::time::Instant) -> usize {
    let mut count = 0usize;
    let mut scanned = [0i32; MAX_SCAN];
    loop {
        let scount = raw_list_pids(&mut scanned);
        let mut added = 0usize;
        for &pid in scanned.iter().take(scount) {
            if pid == root {
                continue;
            }
            if out[..count].contains(&pid) {
                continue;
            }
            if let Some((ppid, _)) = raw_ppid(pid) {
                let reachable = ppid == root || out[..count].contains(&ppid);
                if reachable && count < out.len() {
                    out[count] = pid;
                    count += 1;
                    added += 1;
                }
            }
        }
        // Stable: this pass found nothing new.
        if added == 0 {
            break;
        }
        // Explicit full-buffer state: freeze the collected pids so a forker
        // cannot outrun the snapshot, then keep scanning until the deadline.
        if count >= out.len() {
            for &p in out.iter().take(count) {
                if raw_is_alive(p) {
                    unsafe {
                        libc::kill(p, libc::SIGSTOP);
                    }
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        raw_nanosleep(20);
    }
    count
}

/// Reaps the caller's children until none remain. Uses `waitpid(-1)` which only
/// ever reaps the caller's own children (never other processes).
fn reap_children() {
    let mut iters = 0u32;
    loop {
        iters = iters.wrapping_add(1);
        if iters > 1000 {
            break;
        }
        let mut status: libc::c_int = 0;
        let r = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if r == 0 {
            // No child has changed state right now.
            return;
        }
        if r < 0 {
            let e = std::io::Error::last_os_error().raw_os_error();
            // EINTR: interrupted, try again. ECHILD (or other): no children
            // left to reap.
            if e != Some(libc::EINTR) {
                return;
            }
        }
    }
}

/// Lock-free sleep, to be used inside forked children (the subreaper / PID 1,
/// and the unit-test subreaper). After a `fork`, the child inherits the
/// parent's thread locks; calling `std::thread::sleep` there can block forever
/// on an inherited mutex, so we sleep with `nanosleep` instead. On `EINTR` the
/// kernel writes the remaining time back into `ts` (the same buffer is passed
/// as both the request and the remainder), so the sleep is retried for the
/// remainder and the total wait stays bounded; a safety cap prevents an
/// infinite spin if a signal fires on every attempt.
fn raw_nanosleep(ms: u32) {
    let mut ts: libc::timespec = libc::timespec {
        tv_sec: (ms / 1000) as libc::time_t,
        tv_nsec: ((ms % 1000) * 1_000_000) as libc::c_long,
    };
    let ts_ptr = &mut ts as *mut libc::timespec;
    for _ in 0..4000 {
        if unsafe { libc::nanosleep(ts_ptr as *const _, ts_ptr) } == 0 {
            return;
        }
    }
}

/// Terminates only the *descendants* of the calling process: walks the ppid
/// tree under `/proc` to collect them, sends SIGTERM to each one individually,
/// waits up to 2 s, SIGKILLs any survivors individually, then reaps until
/// `ECHILD`. A final bounded verification sweep repeats the SIGKILL+reap of
/// any descendants that (re)appeared after the first sweep, so the function
/// does not return with live descendants.
///
/// It NEVER signals the caller, its siblings, or any process outside the
/// descendant subtree -- no `kill(-1)`, no `kill(0)`, no `killpg`. The caller
/// must be the process whose children form the tree (the subreaper / PID 1).
pub fn kill_tree_and_reap() {
    let self_pid = unsafe { libc::getpid() };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut pids = [0i32; MAX_DESC];
    loop {
        let n = raw_descendants(self_pid, &mut pids, deadline);
        let mut any_live = false;
        for &pid in pids.iter().take(n) {
            if raw_is_alive(pid) {
                any_live = true;
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
            }
        }
        reap_children();
        if !any_live || std::time::Instant::now() >= deadline {
            break;
        }
        raw_nanosleep(20);
    }

    // SIGKILL any descendants that ignored SIGTERM (individually). A fresh
    // deadline so the final sweep is bounded in its own right; SIGKILL also
    // takes down descendants that were SIGSTOPped because the tree outgrew the
    // collection buffer.
    let kill_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let n = raw_descendants(self_pid, &mut pids, kill_deadline);
    for &pid in pids.iter().take(n) {
        if raw_is_alive(pid) {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }

    // Reap until there are no children left.
    reap_children();

    // Final verification sweep (F5): a descendant that (re)appeared after the
    // SIGKILL snapshot above (e.g. a forker that raced the first sweep, or a
    // process that only became our descendant after re-parenting) is still
    // alive here. Rescan and SIGKILL+reap in a bounded loop until none remain
    // or the sweep deadline is hit, so a forker cannot worm its way past the
    // teardown. The deadline keeps this bounded even against a process that
    // keeps forking.
    let verify_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let n = raw_descendants(self_pid, &mut pids, verify_deadline);
        let mut any_live = false;
        for &pid in pids.iter().take(n) {
            if raw_is_alive(pid) {
                any_live = true;
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
        reap_children();
        if !any_live || std::time::Instant::now() >= verify_deadline {
            break;
        }
        raw_nanosleep(20);
    }
}

/// Closes every open file descriptor greater than `2` (stdin/stdout/stderr).
///
/// Used by the gVisor shim (PID 1) and the agent root process right before the
/// target is exec'd, so that file descriptors inherited from the launcher --
/// in particular the secret fd and other launcher-internal descriptors -- do
/// not leak into the sandboxed process tree. `close_range` is used when the
/// kernel provides it; otherwise the range is closed with a fallback that
/// walks `/proc/self/fd`.
pub fn close_fds_above_2() {
    unsafe {
        // Prefer the kernel syscall: close every fd in [3, ~0].
        if libc::close_range(3, u32::MAX, 0) == 0 {
            return;
        }
    }
    // Fallback: read the open fd numbers from /proc/self/fd and close them.
    // (close_range returned an error, e.g. an older kernel without it.)
    let entries = match std::fs::read_dir("/proc/self/fd") {
        Ok(d) => d,
        Err(_) => return,
    };
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let fname = entry.file_name();
        let Some(name) = fname.to_str() else { continue };
        let Some(fd) = name.parse::<i32>().ok() else {
            continue;
        };
        if fd > 2 {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

/// Overwrites `buf` in place with zeros, using a volatile write so the store is
/// not optimized away, then forces a compiler barrier. Callers should invoke
/// this on a buffer that held a secret (e.g. the in-memory secret bytes) right
/// after the secret has been durably written to its destination, so the value
/// does not remain readable in the process heap.
pub fn zeroize_bytes(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        unsafe {
            std::ptr::write_volatile(b, 0u8);
        }
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

/// Arms `PR_SET_PDEATHSIG(signal)` on the calling process and closes the race
/// where the parent exits between the `fork` and the `prctl`.
///
/// The kernel only delivers the death signal for parents that die *after* the
/// `prctl`, so if the parent had already died we are re-parented to init / a
/// subreaper and the signal will never fire. To close that window, immediately
/// after arming the death signal the current parent is re-read with `getppid`;
/// if it no longer matches `parent_pid` (the process that spawned us), the
/// process raises `signal` on itself so it cannot outlive an abnormal parent
/// exit.
pub fn arm_pdeathsig(signal: libc::c_int, parent_pid: i32) {
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, signal as libc::c_ulong, 0, 0, 0);
        // Re-check the parent: if it is no longer the process that forked us,
        // it has already exited, so take ourselves down with the death signal.
        if libc::getppid() != parent_pid {
            libc::raise(signal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_to_exit_code() {
        // Normal exit code 42: in Unix waitpid status format, exit code is in bits 8..15 (status = code << 8)
        let status_exit_42 = 42 << 8;
        assert_eq!(status_to_exit_code(status_exit_42), 42);

        // Exit code 0
        assert_eq!(status_to_exit_code(0), 0);

        // Exit code 7
        let status_exit_7 = 7 << 8;
        assert_eq!(status_to_exit_code(status_exit_7), 7);

        // Signaled by SIGTERM (15): in Unix waitpid status format, term sig is in bits 0..6
        let status_sig_15 = libc::SIGTERM;
        assert_eq!(status_to_exit_code(status_sig_15), 128 + 15);

        // Signaled by SIGKILL (9) -> 128 + 9 = 137
        let status_sig_9 = libc::SIGKILL;
        assert_eq!(status_to_exit_code(status_sig_9), 137);
    }

    #[test]
    fn test_wait_forwards_term_to_child_handler() {
        // Mirrors R08.sigterm_launcher in-process (no PID namespace): a child
        // ("launcher") waits on a grandchild ("agent") via
        // `wait_with_signal_forwarding`; the launcher child installs the
        // SIGTERM handler itself. The test parent then SIGTERMs the launcher
        // child, exactly like the orchestrator does. The wait loop must
        // forward the TERM to the agent, the agent's own handler must run, and
        // the launcher must report the agent's clean exit (0). Everything
        // happens inside forked children so the test runner's signal state is
        // never touched.
        let launcher = unsafe { libc::fork() };
        assert!(launcher >= 0, "fork launcher child");
        if launcher == 0 {
            let agent = unsafe { libc::fork() };
            if agent == 0 {
                // Agent: install a SIGTERM handler that exits 0, mark that we
                // got the signal on a fd the launcher watches, then idle.
                extern "C" fn onterm(_: libc::c_int) {
                    unsafe {
                        libc::_exit(0);
                    }
                }
                let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
                sa.sa_sigaction = onterm as *const () as usize;
                unsafe {
                    libc::sigemptyset(&mut sa.sa_mask);
                    libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
                }
                loop {
                    raw_nanosleep(100);
                }
            }
            // Launcher: wait on the agent with signal forwarding. The SIGTERM
            // the test parent sends to THIS process is recorded by the handler
            // this function installs, forwarded to the agent, and the agent's
            // exit (0) is returned.
            let code = wait_with_signal_forwarding(agent);
            unsafe { libc::_exit(code) };
        }

        // Give the launcher time to fork the agent and install its handler,
        // then signal the launcher exactly like `case_R8_term` does. The wait
        // is bounded so a regression (no forward -> agent never exits -> the
        // launcher never exits) fails the test cleanly instead of hanging the
        // whole suite.
        std::thread::sleep(std::time::Duration::from_millis(100));
        unsafe {
            libc::kill(launcher, libc::SIGTERM);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let mut st = 0;
            let r = unsafe { libc::waitpid(launcher, &mut st, libc::WNOHANG) };
            if r == launcher {
                assert_eq!(
                    status_to_exit_code(st),
                    0,
                    "launcher must exit 0 after forwarding SIGTERM to the agent"
                );
                break;
            }
            if r < 0 {
                let e = std::io::Error::last_os_error();
                assert_eq!(
                    e.raw_os_error(),
                    Some(libc::EINTR),
                    "waitpid(launcher): {e}"
                );
            }
            assert!(
                std::time::Instant::now() < deadline,
                "launcher did not forward SIGTERM to the agent and exit within 5s"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    #[test]
    fn test_subreaper_tree_kill_logic() {
        // An unrelated sibling: a direct child of the test process, NOT a
        // descendant of the subreaper. It must SURVIVE the tree kill. Under the
        // old `kill(-1)` bug the subreaper's SIGKILL sweep would take it down
        // and this assertion would fail.
        let sib = unsafe { libc::fork() };
        assert!(sib >= 0, "fork sibling");
        if sib == 0 {
            // Sibling child: idle forever; the test process reaps it at the end.
            loop {
                raw_nanosleep(1000);
            }
        }

        // The subreaper: builds its own isolated tree, runs kill_tree_and_reap
        // INSIDE ITSELF (never in the test process), and exits 0 only if every
        // one of its descendants is gone.
        let sub = unsafe { libc::fork() };
        assert!(sub >= 0, "fork subreaper");
        if sub == 0 {
            unsafe {
                // Become a subreaper before any descendant can be orphaned, so
                // orphaned descendants re-parent to us.
                libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0);

                // Grandchild A: a direct child that ignores SIGTERM, so the
                // SIGKILL fallback path is exercised.
                let ga = libc::fork();
                if ga == 0 {
                    libc::signal(libc::SIGTERM, libc::SIG_IGN);
                    loop {
                        raw_nanosleep(200);
                    }
                }

                // Grandchild B forks the double-forked orphan (C) and exits, so
                // C is orphaned and re-parented to the subreaper.
                let gb = libc::fork();
                if gb == 0 {
                    let gc = libc::fork();
                    if gc == 0 {
                        // C: the double-forked orphan; idle forever.
                        loop {
                            raw_nanosleep(200);
                        }
                    }
                    // B exits immediately, orphaning C.
                    libc::_exit(0);
                }

                // Let the tree settle: B exits and C re-parents to the
                // subreaper. Poll (bounded) until both live descendants -- A
                // (which ignores SIGTERM) and C (the orphan) -- are visible. If
                // C had re-parented anywhere other than the subreaper this poll
                // would time out and the test would fail, so the orphan case is
                // genuinely exercised rather than silently leaking.
                let mut d = [0i32; MAX_DESC];
                let settle_deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(500);
                loop {
                    let scan_deadline =
                        std::time::Instant::now() + std::time::Duration::from_millis(200);
                    let n = raw_descendants(libc::getpid(), &mut d, scan_deadline);
                    let mut live = 0;
                    for &pid in d.iter().take(n) {
                        if raw_is_alive(pid) {
                            live += 1;
                        }
                    }
                    if live == 2 || std::time::Instant::now() >= settle_deadline {
                        break;
                    }
                    raw_nanosleep(10);
                }
                // Exit 3 if the expected two live descendants are not present.
                let scan_deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(200);
                let n = raw_descendants(libc::getpid(), &mut d, scan_deadline);
                let mut live_pre = 0;
                for &pid in d.iter().take(n) {
                    if raw_is_alive(pid) {
                        live_pre += 1;
                    }
                }
                if live_pre != 2 {
                    libc::_exit(3);
                }

                // Kill only our own descendants (never the sibling above).
                kill_tree_and_reap();

                // Verify inside the child: no descendant of the subreaper
                // remains (A is SIGKILLed, C is reaped; B was a zombie).
                let scan_deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(200);
                let n = raw_descendants(libc::getpid(), &mut d, scan_deadline);
                let mut live_post = 0;
                for &pid in d.iter().take(n) {
                    if raw_is_alive(pid) {
                        live_post += 1;
                    }
                }
                let ok = live_post == 0 && !raw_is_alive(ga);
                libc::_exit(if ok { 0 } else { 1 });
            }
        }

        // Parent: the subreaper exits 0 only if all of its descendants died.
        let mut st = 0;
        let r = unsafe { libc::waitpid(sub, &mut st, 0) };
        assert_eq!(r, sub, "waitpid(sub)");
        assert_eq!(
            status_to_exit_code(st),
            0,
            "subreaper reported a surviving descendant"
        );

        // The unrelated sibling must still be alive: it is not a descendant of
        // the subreaper, so the tree kill must not have touched it.
        assert!(
            raw_is_alive(sib),
            "unrelated sibling was killed by the tree kill"
        );

        // Clean up the sibling (we own it; a plain SIGTERM suffices).
        let _ = unsafe { libc::kill(sib, libc::SIGTERM) };
        let mut st2 = 0;
        let r2 = unsafe { libc::waitpid(sib, &mut st2, 0) };
        assert_eq!(r2, sib, "waitpid(sib)");
    }

    #[test]
    fn test_raw_descendants_more_than_one_scan_chunk() {
        // A descendant tree with more processes than a single scan buffer holds
        // must be handled without leaving survivors AND without looping
        // forever once the buffer fills. The subreaper forks NID idle
        // descendants (all direct children). Two walks exercise the two
        // regimes:
        //   * small buffer  -> the tree is bigger than the buffer, so the walk
        //     hits its EXPLICIT full-buffer state: it must SIGSTOP the collected
        //     pids (so a forker cannot outrun the snapshot) and then keep
        //     scanning ONLY until the deadline (bounded, not infinite).
        //   * large buffer  -> the whole tree fits, so the walk must converge to
        //     "no descendants remain" and collect every one (no survivors).
        // Only the processes we own are signalled/reaped, never anything else.
        const NID: usize = 60;
        const SMALL: usize = 8;

        let sub = unsafe { libc::fork() };
        assert!(sub >= 0, "fork subreaper");
        if sub == 0 {
            unsafe {
                libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0);

                // Fork NID idle descendants (we own them all).
                let mut ids = [0i32; NID];
                for id in ids.iter_mut() {
                    let c = libc::fork();
                    if c == 0 {
                        loop {
                            raw_nanosleep(500);
                        }
                    }
                    if c < 0 {
                        libc::_exit(5);
                    }
                    *id = c;
                }

                let root = libc::getpid();
                let mut ok = true;

                // --- Walk 1: buffer smaller than the tree ----------------
                let mut small = [0i32; SMALL];
                let start = std::time::Instant::now();
                let small_deadline = start + std::time::Duration::from_millis(300);
                let n_small = raw_descendants(root, &mut small, small_deadline);
                let bounded = start.elapsed() < std::time::Duration::from_millis(3000);
                // Filled the small buffer, and at least one collected pid was
                // SIGSTOPped (the explicit full-buffer freeze). SIGSTOP delivery
                // has scheduling latency, so poll briefly.
                let mut any_stopped = false;
                let stop_deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(500);
                while std::time::Instant::now() < stop_deadline {
                    any_stopped = small.iter().take(n_small).any(|&pid| raw_is_stopped(pid));
                    if any_stopped {
                        break;
                    }
                    raw_nanosleep(20);
                }
                ok &= bounded && n_small == SMALL && any_stopped;

                // --- Walk 2: buffer large enough for the whole tree -------
                let mut big = [0i32; 256];
                let big_deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(1000);
                // Re-run a few times to be robust against a transient /proc
                // listing gap; a fresh cleared buffer each time.
                let mut all = [0i32; NID];
                let mut found = 0usize;
                for _ in 0..10 {
                    big.fill(0);
                    let n = raw_descendants(root, &mut big, big_deadline);
                    for &pid in big.iter().take(n) {
                        if !all[..found].contains(&pid) && found < all.len() {
                            all[found] = pid;
                            found += 1;
                        }
                    }
                    if found == NID {
                        break;
                    }
                    raw_nanosleep(50);
                }
                // Every descendant collected: no survivors.
                ok &= found == NID && ids.iter().all(|&id| all[..found].contains(&id));

                // Clean up: thaw anything the freeze stopped, kill our own
                // descendants, and reap them all.
                for &id in &ids {
                    let _ = libc::kill(id, libc::SIGCONT);
                    let _ = libc::kill(id, libc::SIGKILL);
                }
                let mut w = 0;
                for _ in 0..NID {
                    let r = libc::waitpid(-1, &mut w, 0);
                    if r <= 0 {
                        break;
                    }
                }
                libc::_exit(if ok { 0 } else { 1 });
            }
        }

        let mut st = 0;
        let r = unsafe { libc::waitpid(sub, &mut st, 0) };
        assert_eq!(r, sub, "waitpid(sub)");
        assert_eq!(
            status_to_exit_code(st),
            0,
            "small-buffer walk was not bounded / did not freeze, or the large-buffer walk left survivors"
        );
    }

    #[test]
    fn test_zeroize_bytes_overwrites_in_place() {
        // A buffer that held a non-zero secret pattern must be fully zeroed
        // after zeroize_bytes, and the operation must act in place.
        let mut buf = vec![0xABu8; 257];
        zeroize_bytes(&mut buf);
        assert!(
            buf.iter().all(|&b| b == 0),
            "secret buffer was not fully zeroed"
        );
        assert_eq!(buf.len(), 257, "zeroize must not change the length");

        // An empty buffer is a no-op and must not panic.
        let mut empty: Vec<u8> = Vec::new();
        zeroize_bytes(&mut empty);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_close_fds_above_2_closes_inherited_fds() {
        // Open several extra descriptors in the test process; the forked child
        // (which inherits them) must close every fd > 2 while 0/1/2 stay open.
        let extra_a = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY) };
        let extra_b = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY) };
        let extra_c = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDWR) };
        assert!(
            extra_a > 2 && extra_b > 2 && extra_c > 2,
            "extra fds opened"
        );

        let child = unsafe { libc::fork() };
        assert!(child >= 0, "fork fd-check child");
        if child == 0 {
            // Inside the child: close every fd above 2, then report which of
            // the inherited fds are still open by fstat-ing them.
            close_fds_above_2();
            unsafe {
                // fds 0,1,2 must still be open (fstat succeeds).
                let mut sb: libc::stat = std::mem::zeroed();
                let std_open = (libc::fstat(0, &mut sb) == 0)
                    && (libc::fstat(1, &mut sb) == 0)
                    && (libc::fstat(2, &mut sb) == 0);
                // The inherited fds must now be closed (fstat fails with EBADF).
                let mut extra_closed = true;
                for &fd in &[extra_a, extra_b, extra_c] {
                    if libc::fstat(fd, &mut sb) == 0 {
                        extra_closed = false;
                    }
                }
                libc::_exit(if std_open && extra_closed { 0 } else { 1 });
            }
        }

        let mut st = 0;
        let r = unsafe { libc::waitpid(child, &mut st, 0) };
        assert_eq!(r, child, "waitpid(fd child)");
        assert_eq!(
            status_to_exit_code(st),
            0,
            "close_fds_above_2 must close inherited fds while keeping 0-2 open"
        );

        // The parent's own extra fds are untouched by the child's close.
        unsafe {
            libc::close(extra_a);
            libc::close(extra_b);
            libc::close(extra_c);
        }
    }
}

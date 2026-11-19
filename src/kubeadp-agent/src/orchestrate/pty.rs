use std::io;
use std::os::raw::c_int;
#[cfg(test)]
use std::os::unix::io::AsRawFd;
use std::os::unix::io::{FromRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct Pty {
    pub master: RawFd,
    pub slave: RawFd,
}

/// Test-only: `Pty::open` holds this for reading, and the fd-leak test holds it
/// for writing while it checks that a dropped pty's fd numbers are closed.
/// Without it, another test thread opening a pty in that window is handed the
/// just-freed fd number and the check mistakes it for a leak.
#[cfg(test)]
pub(crate) static OPEN_GUARD: std::sync::RwLock<()> = std::sync::RwLock::new(());

impl Pty {
    pub fn open() -> io::Result<Self> {
        #[cfg(test)]
        let _open_guard = OPEN_GUARD.read().unwrap_or_else(|e| e.into_inner());
        let mut master: c_int = -1;
        let mut slave: c_int = -1;
        let ret = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe {
            libc::fcntl(master, libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(slave, libc::F_SETFD, libc::FD_CLOEXEC);
        }
        Ok(Self { master, slave })
    }

    pub fn set_master_nonblocking(&self) -> io::Result<()> {
        let flags = unsafe { libc::fcntl(self.master, libc::F_GETFL, 0) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let ret = unsafe { libc::fcntl(self.master, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn close_slave(&mut self) {
        if self.slave >= 0 {
            unsafe {
                libc::close(self.slave);
            }
            self.slave = -1;
        }
    }

    pub fn close_master(&mut self) {
        if self.master >= 0 {
            unsafe {
                libc::close(self.master);
            }
            self.master = -1;
        }
    }
}

impl Drop for Pty {
    fn drop(&mut self) {
        self.close_slave();
        self.close_master();
    }
}

/// Spawns a child process with stdin, stdout, and stderr connected to the slave PTY,
/// in its own new process group and session.
///
/// Fix #8: Sets PR_SET_PDEATHSIG(SIGTERM) in the child's `pre_exec` closure so
/// that if the supervisor dies abnormally (crash / SIGKILL) the child receives
/// SIGTERM instead of being orphaned.  Includes the standard getppid() re-check
/// race guard (the parent may have already died between fork and pre_exec).
///
/// IMPORTANT: `spawn_in_pty` must be called from the long-lived session thread,
/// not from a transient thread, because PR_SET_PDEATHSIG is tied to the
/// THREAD that performed the fork.
pub fn spawn_in_pty(
    mut cmd: Command,
    pty: &Pty,
    secret_fd: Option<std::os::fd::OwnedFd>,
) -> io::Result<Child> {
    let slave_in = unsafe {
        let dup = libc::fcntl(pty.slave, libc::F_DUPFD_CLOEXEC, 0);
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        Stdio::from_raw_fd(dup)
    };
    let slave_out = unsafe {
        let dup = libc::fcntl(pty.slave, libc::F_DUPFD_CLOEXEC, 0);
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        Stdio::from_raw_fd(dup)
    };
    let slave_err = unsafe {
        let dup = libc::fcntl(pty.slave, libc::F_DUPFD_CLOEXEC, 0);
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        Stdio::from_raw_fd(dup)
    };

    // Record the supervisor's PID before forking so that the race guard in
    // pre_exec can verify the parent is still alive.
    let supervisor_pid = unsafe { libc::getpid() };

    unsafe {
        cmd.pre_exec(move || {
            // Jobset secret: dup the (already-written) memfd onto fd 3 for the
            // sandbox `secret` channel, then close the child's copy. The
            // `OwnedFd` is captured by value, so the fd is open in the child
            // (inherited by fork) when this runs and is closed in the parent
            // after `spawn()`.
            if let Some(secret_fd) = secret_fd.as_ref() {
                let src = std::os::fd::AsRawFd::as_raw_fd(secret_fd);
                let r = libc::dup2(src, 3);
                if src != 3 {
                    let _ = libc::close(src);
                }
                if r < 0 {
                    return Err(io::Error::last_os_error());
                }
                let _ = libc::fcntl(3, libc::F_SETFD, 0);
            }
            // Fix #8: Set PR_SET_PDEATHSIG so the child receives SIGTERM when
            // this supervisor thread exits (including abnormal death).
            //
            // Race guard: if the parent already exited between fork() and here,
            // getppid() returns 1 (init/systemd), not the supervisor pid.
            // In that case the signal is sent explicitly now.
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM, 0, 0, 0);
            if libc::getppid() != supervisor_pid {
                // Parent already gone; signal self to exit.
                libc::kill(libc::getpid(), libc::SIGTERM);
            }

            // Create a new session and become process group leader.
            libc::setsid();
            // Set the controlling terminal.
            libc::ioctl(0, libc::TIOCSCTTY, 0);
            Ok(())
        });
    }

    cmd.stdin(slave_in)
        .stdout(slave_out)
        .stderr(slave_err)
        .spawn()
}

/// Writes `secret` into an anonymous `memfd` (inaccessible by name) and returns
/// the owning `OwnedFd`. The caller hands it to [`spawn_in_pty`], which dups it
/// onto fd 3 in the child's `pre_exec` before exec (the sandbox `secret`
/// channel). The secret never appears in argv or the environment.
pub fn secret_memfd(secret: &str) -> std::os::fd::OwnedFd {
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    let bytes = secret.as_bytes();
    let name = CString::new("kubeadp-agent-jobset-secret").expect("static, no NUL");
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        panic!(
            "jobset secret memfd_create failed: {}",
            std::io::Error::last_os_error()
        );
    }
    let mut off = 0usize;
    while off < bytes.len() {
        let n = unsafe {
            libc::write(
                fd,
                bytes[off..].as_ptr() as *const libc::c_void,
                bytes.len() - off,
            )
        };
        if n < 0 {
            let _ = unsafe { libc::close(fd) };
            panic!(
                "jobset secret memfd write failed: {}",
                std::io::Error::last_os_error()
            );
        }
        off += n as usize;
    }
    // Rewind the shared file offset: the child reads the secret from fd 3
    // (a dup of this memfd — dup shares the open file description, including
    // its offset) via `read_secret_fd`, which reads from the current offset
    // to EOF. Without the rewind the child sees an empty file.
    assert_eq!(
        unsafe { libc::lseek(fd, 0, libc::SEEK_SET) },
        0,
        "jobset secret memfd lseek failed: {}",
        std::io::Error::last_os_error()
    );
    unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) }
}

/// A session-wide reference to the live child PTY master, shared so the REST
/// API can type text into the session without owning the supervision thread.
/// Per-attempt cap on the `poll(POLLOUT)` wait inside [`PtyInput::write_all`]:
/// the total write is also capped by its `deadline`, so a stuck child is
/// bounded in both dimensions.
pub const PTY_WRITE_POLL_TIMEOUT: Duration = Duration::from_millis(50);

/// Total budget for one bracketed-paste injection (all its write segments share
/// one `deadline` set to `now + PTY_WRITE_TOTAL_DEADLINE`). Generous enough for
/// a healthy child, bounded so a stuck child that never drains its input queue
/// cannot wedge the caller (and the `pty_input` mutex) indefinitely.
pub const PTY_WRITE_TOTAL_DEADLINE: Duration = Duration::from_secs(2);

pub struct PtyInput {
    master: RawFd,
}

impl PtyInput {
    pub fn new(master: RawFd) -> Self {
        Self { master }
    }

    pub fn taken(&self) -> bool {
        self.master >= 0
    }

    pub fn put(&mut self, master: RawFd) {
        self.master = master;
    }

    pub fn take(&mut self) -> Option<RawFd> {
        if self.master >= 0 {
            let fd = self.master;
            self.master = -1;
            Some(fd)
        } else {
            None
        }
    }

    /// Writes every byte of `buf` to the PTY master, bounded by `deadline`.
    ///
    /// The master is non-blocking, so a full slave input queue makes `write`
    /// return `EAGAIN` instead of blocking. Rather than spin on `EAGAIN` (which
    /// used to wedge the caller -- and the `pty_input` mutex it held -- for as
    /// long as a stuck child refused to drain), we wait for space with
    /// `poll(POLLOUT)`, capped per attempt by [`PTY_WRITE_POLL_TIMEOUT`] and in
    /// total by `deadline`. Reaching the deadline returns a clear
    /// [`io::ErrorKind::TimedOut`] error so the caller can release the lock and
    /// the session thread can proceed.
    pub fn write_all(&mut self, buf: &[u8], deadline: Instant) -> io::Result<()> {
        let mut written: usize = 0;
        while written < buf.len() {
            if deadline <= Instant::now() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "pty write timed out: {written} of {} bytes written",
                        buf.len()
                    ),
                ));
            }
            let n = unsafe {
                libc::write(
                    self.master,
                    buf[written..].as_ptr() as *const libc::c_void,
                    buf.len() - written,
                )
            };
            if n > 0 {
                written += n as usize;
                continue;
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                // Interrupted: re-check the deadline and retry.
                Some(libc::EINTR) => continue,
                // Slave input queue full: wait (bounded) for space, then retry.
                // (On Linux EWOULDBLOCK == EAGAIN.)
                Some(libc::EAGAIN) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let wait_ms = remaining.min(PTY_WRITE_POLL_TIMEOUT).as_millis().max(1) as c_int;
                    let mut pfd = libc::pollfd {
                        fd: self.master,
                        events: libc::POLLOUT,
                        revents: 0,
                    };
                    let ret = unsafe { libc::poll(&mut pfd, 1, wait_ms) };
                    if ret < 0 {
                        let perr = io::Error::last_os_error();
                        if perr.kind() != io::ErrorKind::Interrupted {
                            return Err(perr);
                        }
                        continue;
                    }
                    if (pfd.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL)) != 0 {
                        // Slave side gone: let the next write report the real error.
                        continue;
                    }
                    // Poll timed out (ret == 0) or is writable: loop; the
                    // deadline check at the top turns a stuck child into an error.
                }
                Some(_) => return Err(err),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "pty write returned 0",
                    ))
                }
            }
        }
        Ok(())
    }
}

/// Returns whether the OS process `pid` is still alive and -- when a
/// start-time anchor was recorded at spawn -- is still the exact same process
/// (`/proc/<pid>/stat` field 22 start ticks, robust to PID reuse).
///
/// With `start_time: None` (no anchor ever recorded) the check falls back to
/// requiring the live process's `/proc/<pid>/cmdline` to still look like a
/// supervised agent process (claude / agy / the sandbox-exec wrapper), so a
/// recycled PID held by an unrelated process is still refused rather than
/// signalled. Zombies are never considered alive (`core::job::is_pid_alive`).
pub fn is_same_process(pid: u32, start_time: Option<u64>) -> bool {
    if !crate::core::job::is_pid_alive(pid) {
        return false;
    }
    match start_time {
        Some(recorded) => crate::core::job::get_process_start_time(pid) == Some(recorded),
        None => crate::core::job::is_job_process_cmdline(pid),
    }
}

/// Identity-verified process-group signal: calls [`signal_process_group`]
/// only when `pid` is still the recorded target process (see
/// [`is_same_process`]; a recycled or exited PID is never signalled).
/// Returns `true` when a signal was actually sent.
pub fn signal_process_group_if_target(pid: u32, start_time: Option<u64>, sig: c_int) -> bool {
    if !is_same_process(pid, start_time) {
        return false;
    }
    let _ = signal_process_group(pid, sig);
    true
}

/// Signals the process group of `pid` with `sig`, falling back to signaling `pid` directly.
pub fn signal_process_group(pid: u32, sig: c_int) -> io::Result<()> {
    unsafe {
        let _ = libc::kill(-(pid as i32), sig);
        let _ = libc::kill(pid as i32, sig);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillResult {
    /// Child process was reaped. Contains waitpid raw status if captured.
    Reaped(Option<c_int>),
    /// Child process could not be reaped within deadline (uninterruptible / D-state).
    Unkillable,
}

impl KillResult {
    pub fn is_reaped(&self) -> bool {
        matches!(self, KillResult::Reaped(_))
    }

    pub fn status(&self) -> Option<c_int> {
        match self {
            KillResult::Reaped(s) => *s,
            KillResult::Unkillable => None,
        }
    }
}

/// Kills child and its process group: sends SIGTERM, waits up to `grace`,
/// and escalates to SIGKILL if still alive. After SIGKILL, waits up to `kill_wait`
/// (bounded poll with WNOHANG). Returns KillResult::Reaped if child is gone,
/// or KillResult::Unkillable if child cannot be reaped within deadline.
///
/// `start_time` is the process identity anchor recorded at spawn (see
/// [`is_same_process`]): before every signal the PID is re-verified, so a
/// recycled PID held by an unrelated process is left untouched (the signal
/// is skipped and the wait keeps polling; the caller's bounded deadlines
/// still terminate the loop).
pub fn kill_process_group_with_grace(
    pid: u32,
    start_time: Option<u64>,
    grace: Duration,
    kill_wait: Duration,
) -> KillResult {
    kill_process_group_with_grace_custom(pid, start_time, grace, kill_wait, |p, st, opt| unsafe {
        libc::waitpid(p, st, opt)
    })
}

/// Custom-waitpid version of `kill_process_group_with_grace` for unit-testing bounded waits.
pub fn kill_process_group_with_grace_custom<F>(
    pid: u32,
    start_time: Option<u64>,
    grace: Duration,
    kill_wait: Duration,
    mut waitpid_fn: F,
) -> KillResult
where
    F: FnMut(libc::pid_t, *mut libc::c_int, libc::c_int) -> libc::pid_t,
{
    // 1. Send SIGTERM to process group (only if still the recorded process)
    let _ = signal_process_group_if_target(pid, start_time, libc::SIGTERM);

    // 2. Wait up to grace
    let start = Instant::now();
    let mut exit_status = None;
    let mut reaped = false;
    while start.elapsed() < grace {
        let mut status: c_int = 0;
        let res = waitpid_fn(pid as i32, &mut status, libc::WNOHANG);
        if res > 0 {
            exit_status = Some(status);
            reaped = true;
            break;
        } else if res < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
            reaped = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // 3. Escalate to SIGKILL if not yet exited (identity re-verified)
    if !reaped {
        let _ = signal_process_group_if_target(pid, start_time, libc::SIGKILL);
        let kill_start = Instant::now();
        while kill_start.elapsed() < kill_wait {
            let mut status: c_int = 0;
            let res = waitpid_fn(pid as i32, &mut status, libc::WNOHANG);
            if res > 0 {
                exit_status = Some(status);
                reaped = true;
                break;
            } else if res < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
                reaped = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    // 4. Reap any other terminated processes in the process group if child was reaped
    if reaped {
        for _ in 0..1024 {
            let mut status: c_int = 0;
            let res = waitpid_fn(-(pid as i32), &mut status, libc::WNOHANG);
            if res <= 0 {
                break;
            }
        }
        KillResult::Reaped(exit_status)
    } else {
        KillResult::Unkillable
    }
}

/// Decodes raw waitpid status into (Option<exit_code>, Option<signal_number>).
pub fn decode_wait_status(status: c_int) -> (Option<i32>, Option<i32>) {
    let code = if libc::WIFEXITED(status) {
        Some(libc::WEXITSTATUS(status))
    } else {
        None
    };
    let signal = if libc::WIFSIGNALED(status) {
        Some(libc::WTERMSIG(status))
    } else {
        None
    };
    (code, signal)
}

/// Reads available bytes from master PTY with a timeout in milliseconds.
/// Returns Ok(None) on timeout, Ok(Some(bytes)) on read, Ok(Some(empty)) on EOF, or Err.
pub fn poll_and_read_pty(
    master_fd: RawFd,
    buf: &mut [u8],
    timeout_ms: c_int,
) -> io::Result<Option<usize>> {
    let mut pfd = libc::pollfd {
        fd: master_fd,
        events: libc::POLLIN,
        revents: 0,
    };

    let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            return Ok(None);
        }
        return Err(err);
    }
    if ret == 0 {
        return Ok(None);
    }

    if (pfd.revents & libc::POLLIN) != 0 {
        let n = unsafe { libc::read(master_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n > 0 {
            return Ok(Some(n as usize));
        } else if n == 0 {
            return Ok(Some(0)); // EOF
        } else {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EIO) {
                // EIO on Linux master PTY indicates slave side closed
                return Ok(Some(0));
            }
            if err.kind() == io::ErrorKind::Interrupted || err.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(err);
        }
    }

    if (pfd.revents & (libc::POLLHUP | libc::POLLERR)) != 0 {
        return Ok(Some(0)); // EOF
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_and_close_pty() {
        let pty = Pty::open().expect("Failed to open PTY");
        assert!(pty.master >= 0);
        assert!(pty.slave >= 0);

        // Verify FD_CLOEXEC is set on master and slave
        let m_flags = unsafe { libc::fcntl(pty.master, libc::F_GETFD) };
        assert!(m_flags >= 0 && (m_flags & libc::FD_CLOEXEC) != 0);
        let s_flags = unsafe { libc::fcntl(pty.slave, libc::F_GETFD) };
        assert!(s_flags >= 0 && (s_flags & libc::FD_CLOEXEC) != 0);

        pty.set_master_nonblocking().expect("Set non-blocking");
        // Keep other threads from opening a pty (and reusing these fd numbers)
        // between the drop and the checks below.
        let _no_concurrent_open = OPEN_GUARD.write().unwrap_or_else(|e| e.into_inner());
        let master_fd = pty.master;
        let slave_fd = pty.slave;
        drop(pty);

        // Verify Drop closed both fds. Other test threads of the same process
        // may reuse a just-freed number, so a still-open fd only counts as a
        // leak while it still points at a pty device.
        for fd in [master_fd, slave_fd] {
            let link = std::fs::read_link(format!("/proc/self/fd/{fd}"));
            if let Ok(target) = link {
                let target = target.to_string_lossy().into_owned();
                assert!(
                    !target.contains("/dev/pts/") && !target.contains("/dev/ptmx"),
                    "fd {fd} still open on {target} after drop"
                );
            }
        }
    }

    #[test]
    fn test_poll_and_read_pty_empty() {
        let pty = Pty::open().expect("open pty");
        pty.set_master_nonblocking().expect("nonblocking");
        let mut buf = [0u8; 128];
        let res = poll_and_read_pty(pty.master, &mut buf, 10).expect("read");
        assert_eq!(res, None);
    }

    #[test]
    fn test_spawn_in_pty_pdeathsig() {
        let pty = Pty::open().expect("open pty");
        let mut cmd = Command::new("python3");
        cmd.args([
            "-c",
            "import ctypes; s = ctypes.c_int(); ctypes.CDLL(None).prctl(2, ctypes.byref(s)); assert s.value == 15, f'got {s.value}'",
        ]);
        let mut child = spawn_in_pty(cmd, &pty, None).expect("spawn");
        let status = child.wait().expect("wait");
        assert!(status.success(), "child asserted pdeathsig == 15");
    }

    #[test]
    fn test_write_all_times_out_on_undrained_pipe() {
        use std::sync::{Arc, Mutex};

        // A pipe whose read end is kept open but never drained: the 64 KiB
        // pipe buffer fills, the non-blocking write then returns EAGAIN and
        // poll(POLLOUT) never reports space. write_all must give up at the
        // deadline (not spin) and leave the pty_input mutex free.
        let mut fds = [0 as libc::c_int; 2];
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) };
        assert_eq!(rc, 0, "pipe2");
        // pipe2: fds[0] = read end, fds[1] = write end. The write end is fed to
        // PtyInput; the read end stays open and is never drained, so the 64 KiB
        // pipe buffer fills and the non-blocking write returns EAGAIN.
        let read_fd = fds[0];
        let write_fd = fds[1];

        let input = Arc::new(Mutex::new(PtyInput::new(write_fd)));
        input.lock().unwrap().put(write_fd);

        let big = vec![b'a'; 1024 * 1024];
        let deadline = Instant::now() + Duration::from_millis(200);
        let start = Instant::now();
        let result = input.lock().unwrap().write_all(&big, deadline);
        let elapsed = start.elapsed();

        assert!(
            result.is_err(),
            "undrained pipe write should time out, got {result:?}"
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        // Bounded by the deadline (it waited, but did not spin forever).
        assert!(
            elapsed >= Duration::from_millis(150) && elapsed < Duration::from_millis(1200),
            "elapsed {elapsed:?} should track the ~200ms deadline"
        );
        // The caller's pty_input mutex is released once the write bails out.
        assert!(input.try_lock().is_ok(), "pty_input mutex must be free");

        unsafe {
            libc::close(write_fd);
            libc::close(read_fd);
        }
    }

    #[test]
    fn test_secret_memfd_rewinds_offset_for_child_read() {
        let secret = "jobset-secret-0123456789abcdef";
        let fd = secret_memfd(secret);
        let raw = fd.as_raw_fd();
        assert!(raw >= 0);

        let dup_fd = unsafe { libc::dup(raw) };
        assert!(dup_fd >= 0, "dup failed");
        let mut file = unsafe { std::fs::File::from_raw_fd(dup_fd) };
        use std::io::Read;
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .expect("read from dup'd secret memfd");
        assert_eq!(String::from_utf8_lossy(&data), secret);
    }

    #[test]
    fn test_kill_process_group_reaped_on_term() {
        let fake_pid = 999999;
        let grace = Duration::from_millis(50);
        let kill_wait = Duration::from_millis(50);

        // Child exits immediately on SIGTERM check
        let res = kill_process_group_with_grace_custom(
            fake_pid,
            None,
            grace,
            kill_wait,
            |p, status, _opt| {
                if p == fake_pid as libc::pid_t {
                    unsafe { *status = 0 };
                    fake_pid as libc::pid_t
                } else {
                    0
                }
            },
        );
        assert_eq!(res, KillResult::Reaped(Some(0)));
    }

    #[test]
    fn test_kill_process_group_reaped_on_kill() {
        let fake_pid = 999999;
        let grace = Duration::from_millis(40);
        let kill_wait = Duration::from_millis(100);

        let start = Instant::now();
        // Child resists SIGTERM (returns 0 during grace), then exits on SIGKILL
        let res = kill_process_group_with_grace_custom(
            fake_pid,
            None,
            grace,
            kill_wait,
            move |p, status, _opt| {
                if p == fake_pid as libc::pid_t {
                    if start.elapsed() >= grace {
                        unsafe { *status = 9 };
                        fake_pid as libc::pid_t
                    } else {
                        0
                    }
                } else {
                    0
                }
            },
        );
        assert_eq!(res, KillResult::Reaped(Some(9)));
    }

    #[test]
    fn test_kill_process_group_unkillable_returns_within_deadline() {
        let fake_pid = 999999;
        let grace = Duration::from_millis(40);
        let kill_wait = Duration::from_millis(40);

        let start = Instant::now();
        // Child never exits (waitpid returns 0 always)
        let res = kill_process_group_with_grace_custom(
            fake_pid,
            None,
            grace,
            kill_wait,
            |_p, _status, _opt| 0,
        );
        let elapsed = start.elapsed();

        assert_eq!(res, KillResult::Unkillable);
        // Assert it returned within bounded deadline: grace + kill_wait + small scheduling margin (< 300ms)
        assert!(
            elapsed >= Duration::from_millis(70),
            "elapsed {:?} should be at least grace + kill_wait",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(300),
            "elapsed {:?} exceeded bounded deadline",
            elapsed
        );
    }

    #[test]
    fn test_signal_reused_pid_is_refused_wrong_start_time() {
        // A live child is the identity anchor: a start time that does not
        // match the current /proc/<pid>/stat means the PID was (could only
        // have been) recycled by an unrelated process, so the signal must be
        // refused -- the "reused PID" of a real child is simulated with the
        // wrong start-tick value.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn child");
        let pid = child.id();
        let real_start = crate::core::job::get_process_start_time(pid)
            .expect("live child must have a start time");

        // Forged anchor (off-by-one): the PID is refused, signal NOT sent.
        let forged = signal_process_group_if_target(pid, Some(real_start + 1), libc::SIGTERM);
        assert!(
            !forged,
            "recycled PID with a mismatched start time must not be signalled"
        );
        assert!(
            crate::core::job::is_pid_alive(pid),
            "child must be untouched by the refused forged signal"
        );

        // Correct anchor: the signal IS sent and terminates the child.
        let sent = signal_process_group_if_target(pid, Some(real_start), libc::SIGTERM);
        assert!(sent, "the real child must be signalled");
        let status = child.wait().expect("reap child");
        assert!(
            !status.success(),
            "SIGTERM must terminate the child, got {status:?}"
        );

        // The PID must no longer be a viable signal target (dead).
        assert!(
            !signal_process_group_if_target(pid, Some(real_start), libc::SIGKILL),
            "a dead PID must never be signalled again"
        );
    }

    #[test]
    fn test_signal_target_fallback_cmdline_plausibility() {
        // With no recorded start time the helper falls back to a cmdline
        // plausibility check: a supervised-agent-shaped process (sleep here,
        // like the job tests) is signalled, while a mismatched/gone PID is not.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn child");
        let pid = child.id();

        // A start-time anchor that does not exist for any real process shape
        // still refuses a plausible cmdline (identity beats plausibility).
        assert!(
            !signal_process_group_if_target(pid, Some(u64::MAX), libc::SIGTERM),
            "a start-time mismatch must refuse even a plausible cmdline"
        );
        assert!(crate::core::job::is_pid_alive(pid));

        // No anchor: plausible cmdline ("sleep") is signalled.
        let sent = signal_process_group_if_target(pid, None, libc::SIGTERM);
        assert!(sent, "the no-anchor fallback must signal a plausible child");
        let status = child.wait().expect("reap child");
        assert!(!status.success());
    }

    #[test]
    fn test_kill_with_grace_refuses_recycled_pid() {
        // The bounded kill path keeps polling and leaves a PID with a forged
        // (recycled-PID) anchor untouched; by deadline it reports Unkillable
        // only to the caller, never signalling the wrong process.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let mut child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn child");
        let pid = child.id();
        let grace = Duration::from_millis(60);
        let kill_wait = Duration::from_millis(60);

        let mut wait_calls = 0_u32;
        let res = kill_process_group_with_grace_custom(
            pid,
            Some(crate::core::job::get_process_start_time(pid).unwrap() + 1),
            grace,
            kill_wait,
            |_p, _status, _opt| {
                wait_calls += 1;
                0
            },
        );
        // The forged signal was refused, nothing died, the child is untouched.
        assert_eq!(res, KillResult::Unkillable);
        assert!(wait_calls >= 2, "the bounded loop must keep polling");
        assert!(crate::core::job::is_pid_alive(pid));

        let _ = child.kill();
        let _ = child.wait();
    }
}

use std::io;
use std::os::raw::c_int;
use std::os::unix::io::{FromRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct Pty {
    pub master: RawFd,
    pub slave: RawFd,
}

impl Pty {
    pub fn open() -> io::Result<Self> {
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
pub fn spawn_in_pty(mut cmd: Command, pty: &Pty) -> io::Result<Child> {
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

/// A session-wide reference to the live child PTY master, shared so the REST
/// API can type text into the session without owning the supervision thread.
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

    /// Writes every byte of `buf` to the PTY master, retrying on EINTR and
    /// EAGAIN, until an error or EOF. A closed master only surfaces on the
    /// first write, so an idling slot used to be undetectable until now.
    pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut written: usize = 0;
        while written < buf.len() {
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
            if n < 0 && matches!(err.raw_os_error(), Some(libc::EINTR | libc::EAGAIN)) {
                continue;
            }
            return Err(if n < 0 {
                err
            } else {
                io::Error::new(io::ErrorKind::WriteZero, "pty write returned 0")
            });
        }
        Ok(())
    }
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
pub fn kill_process_group_with_grace(pid: u32, grace: Duration, kill_wait: Duration) -> KillResult {
    kill_process_group_with_grace_custom(pid, grace, kill_wait, |p, st, opt| unsafe {
        libc::waitpid(p, st, opt)
    })
}

/// Custom-waitpid version of `kill_process_group_with_grace` for unit-testing bounded waits.
pub fn kill_process_group_with_grace_custom<F>(
    pid: u32,
    grace: Duration,
    kill_wait: Duration,
    mut waitpid_fn: F,
) -> KillResult
where
    F: FnMut(libc::pid_t, *mut libc::c_int, libc::c_int) -> libc::pid_t,
{
    // 1. Send SIGTERM to process group
    let _ = signal_process_group(pid, libc::SIGTERM);

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

    // 3. Escalate to SIGKILL if not yet exited
    if !reaped {
        let _ = signal_process_group(pid, libc::SIGKILL);
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
        let mut child = spawn_in_pty(cmd, &pty).expect("spawn");
        let status = child.wait().expect("wait");
        assert!(status.success(), "child asserted pdeathsig == 15");
    }

    #[test]
    fn test_kill_process_group_reaped_on_term() {
        let fake_pid = 999999;
        let grace = Duration::from_millis(50);
        let kill_wait = Duration::from_millis(50);

        // Child exits immediately on SIGTERM check
        let res =
            kill_process_group_with_grace_custom(fake_pid, grace, kill_wait, |p, status, _opt| {
                if p == fake_pid as libc::pid_t {
                    unsafe { *status = 0 };
                    fake_pid as libc::pid_t
                } else {
                    0
                }
            });
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
        let res =
            kill_process_group_with_grace_custom(fake_pid, grace, kill_wait, |_p, _status, _opt| 0);
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
}

//! Kata Containers sandbox backend using Landlock and subreaper supervision.
//!
//! Kata cannot mount a fresh `/proc`, so all isolation here is Landlock-based.
//!
//! Mechanisms:
//! - Landlock domain per agent. The backend REQUIRES Landlock ABI >= 6 (the
//!   first ABI with `LANDLOCK_SCOPE_SIGNAL` and `LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET`);
//!   if the kernel offers less, the launch fails closed (exit 125) rather than
//!   silently running with weaker isolation.
//! - Filesystem access (default-deny: any path not explicitly allowed is
//!   inaccessible) -- what the policy ALLOWS:
//!   - RW: policy.rw, policy.shared_rw, the private tmp.
//!   - RO: policy.ro (/usr, /lib, /bin, /etc, /opt/tools, etc.), plus /proc and
//!     /dev (read-only, so the agent can read its own process info and the
//!     device tree), plus /dev/null, /dev/zero, /dev/urandom and /dev/tty
//!     (read/write, since those device nodes need to be written to).
//!   - `hide`: the hidden parent is simply NOT allowed (Landlock is
//!     default-deny), so only the agent's own subdirs remain reachable.
//!   - DENY (fail closed, never granted even if a policy lists them): the
//!     Kubernetes ServiceAccount token directory
//!     `/var/run/secrets/kubernetes.io`, and any `hide` entry that is a git
//!     repository (its object store must not be readable through a sibling).
//! - Access flags are chosen by path type: regular files get file-only flags,
//!   directories get the directory access set (see `access_flags_for_path`).
//! - Landlock scopes `LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET` and
//!   `LANDLOCK_SCOPE_SIGNAL` (both ABI >= 6, required here).
//! - Subreaper supervision (`prctl(PR_SET_CHILD_SUBREAPER, 1)`) + tree-kill on agent exit.
//! - Private per-session `/dev/shm`: mounted tmpfs (64 MiB, 65536 inodes, mode 0700)
//!   inside an unprivileged user and mount namespace before Landlock is applied,
//!   with Landlock granting read-write access to `/dev/shm`.
//! - Sets `PR_SET_NO_NEW_PRIVS`.
//! - Writes secret from fd to `<tmp>/<dest>` mode 0400.
//! - Opt-in `net: relay`: the launcher runs the outer HTTP CONNECT proxy in
//!   the pod's network namespace (see [`super::relay`]); the forked child
//!   unshares CLONE_NEWUSER (the caller's uid/gid mapped 1:1, unprivileged)
//!   and CLONE_NEWNET (allowed by the capabilities the fresh user namespace
//!   grants), brings loopback up and starts the `relay-inner` loopback relay
//!   before the Landlock domain is applied. The agent connects to the outer
//!   proxy socket by pathname directly (path-based unix sockets are not
//!   namespaced by network namespaces, and no new Landlock rule is needed:
//!   connect to a pathname socket is not Landlock-restricted below ABI 9,
//!   which this ruleset never handles). The namespace unshares need
//!   unprivileged user namespaces; if the unshare fails the launch aborts
//!   (exit 125) rather than running with weaker network isolation.
//!   `net: host` (the default) and the legacy `shared` alias keep today's
//!   behaviour byte-for-byte.

use super::detect::probe_landlock_abi;
use super::exec::exec_target;
use super::init_shim::{arm_pdeathsig, kill_tree_and_reap, zeroize_bytes};
use super::policy::SandboxPolicy;
use super::relay as relay_lib;
use std::ffi::CString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};

/// tmpfs mount options for the private per-session `/dev/shm`.
pub const KATA_SHM_PRIVATE_OPTS: &str = "size=64m,nr_inodes=65536,mode=0700";

/// Set once the private `/dev/shm` tmpfs is mounted in this process (the forked
/// launcher child); only then does the Landlock domain get a read-write rule for
/// `/dev/shm`, so the shared guest `/dev/shm` is never opened up.
static PRIVATE_SHM_MOUNTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Landlock syscall constants & access flags
const SYS_LANDLOCK_CREATE_RULESET: libc::c_long = 444;
const SYS_LANDLOCK_ADD_RULE: libc::c_long = 445;
const SYS_LANDLOCK_RESTRICT_SELF: libc::c_long = 446;

// Landlock rule type
const LANDLOCK_RULE_PATH_BENEATH: u32 = 1;

// Landlock access fs flags
pub const LANDLOCK_ACCESS_FS_EXECUTE: u64 = 1 << 0;
pub const LANDLOCK_ACCESS_FS_WRITE_FILE: u64 = 1 << 1;
pub const LANDLOCK_ACCESS_FS_READ_FILE: u64 = 1 << 2;
pub const LANDLOCK_ACCESS_FS_READ_DIR: u64 = 1 << 3;
pub const LANDLOCK_ACCESS_FS_REMOVE_DIR: u64 = 1 << 4;
pub const LANDLOCK_ACCESS_FS_REMOVE_FILE: u64 = 1 << 5;
pub const LANDLOCK_ACCESS_FS_MAKE_CHAR: u64 = 1 << 6;
pub const LANDLOCK_ACCESS_FS_MAKE_DIR: u64 = 1 << 7;
pub const LANDLOCK_ACCESS_FS_MAKE_REG: u64 = 1 << 8;
pub const LANDLOCK_ACCESS_FS_MAKE_SOCK: u64 = 1 << 9;
pub const LANDLOCK_ACCESS_FS_MAKE_FIFO: u64 = 1 << 10;
pub const LANDLOCK_ACCESS_FS_MAKE_BLOCK: u64 = 1 << 11;
pub const LANDLOCK_ACCESS_FS_MAKE_SYM: u64 = 1 << 12;
pub const LANDLOCK_ACCESS_FS_REFER: u64 = 1 << 13;
pub const LANDLOCK_ACCESS_FS_TRUNCATE: u64 = 1 << 14;
pub const LANDLOCK_ACCESS_FS_IOCTL_DEV: u64 = 1 << 15;

// Landlock scope flags (ABI >= 6)
pub const LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET: u64 = 1 << 0;
pub const LANDLOCK_SCOPE_SIGNAL: u64 = 1 << 1;

/// Minimum Landlock ABI the Kata backend runs on.
///
/// ABI 6 is the first release with `LANDLOCK_SCOPE_SIGNAL` and
/// `LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET`. The Kata backend needs both scopes
/// (Kata cannot mount a fresh `/proc`, so Landlock is the only scoping
/// available), so anything below ABI 6 must not be used: the backend fails
/// closed instead of silently running with weaker isolation.
pub const KATA_MIN_LANDLOCK_ABI: u32 = 6;

/// Returns `true` when a probed Landlock ABI is acceptable to the Kata
/// backend, i.e. at least [`KATA_MIN_LANDLOCK_ABI`].
pub fn kata_abi_ok(abi: u32) -> bool {
    abi >= KATA_MIN_LANDLOCK_ABI
}

/// Signal observed by the launcher (the `sandbox-exec` parent that supervises
/// the agent). Held in an atomic so the signal handler itself stays
/// async-signal-safe (it only records the signal number); the actual
/// `kill_tree_and_reap` + exit is performed later from the normal supervise
/// loop, where allocating and calling into libc is safe.
static KATA_LAUNCHER_SIG: AtomicI32 = AtomicI32::new(0);

/// Signal handler installed on the Kata launcher for SIGTERM, SIGINT and
/// SIGHUP. MUST remain async-signal-safe: it only stores the signal number and
/// does no allocation, no libc locks and no I/O.
extern "C" fn kata_launcher_signal_handler(sig: libc::c_int) {
    KATA_LAUNCHER_SIG.store(sig, Ordering::SeqCst);
}

/// Installs `kata_launcher_signal_handler` on the given signal. Returns the
/// previous disposition so the caller can restore it if a handler was already
/// present (e.g. the orchestrator installed its own before spawning
/// `sandbox-exec`).
fn install_launcher_signal(sig: libc::c_int) -> Option<libc::sigaction> {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = kata_launcher_signal_handler as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        let mut old: libc::sigaction = std::mem::zeroed();
        if libc::sigaction(sig, &sa, &mut old) != 0 {
            return None;
        }
        Some(old)
    }
}

/// Restores the previous signal dispositions captured by
/// `install_launcher_signal` (SIGTERM, SIGINT, SIGHUP). A `None` entry means the
/// install failed for that signal, so there is nothing to restore for it.
fn restore_launcher_signals(
    old: (
        Option<libc::sigaction>,
        Option<libc::sigaction>,
        Option<libc::sigaction>,
    ),
) {
    let entries = [
        (libc::SIGTERM, old.0),
        (libc::SIGINT, old.1),
        (libc::SIGHUP, old.2),
    ];
    for (sig, action) in entries
        .into_iter()
        .filter_map(|(sig, action)| action.map(|a| (sig, a)))
    {
        unsafe {
            libc::sigaction(sig, &action, std::ptr::null_mut());
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LandlockRulesetAttr {
    pub handled_access_fs: u64,
    pub handled_access_net: u64,
    pub handled_access_scope: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LandlockPathBeneathAttr {
    pub allowed_access: u64,
    pub parent_fd: i32,
}

/// Calculates handled FS flags for given Landlock ABI version (ABI 1 through 7).
pub fn landlock_handled_fs_flags(abi: u32) -> u64 {
    let mut flags = LANDLOCK_ACCESS_FS_EXECUTE
        | LANDLOCK_ACCESS_FS_WRITE_FILE
        | LANDLOCK_ACCESS_FS_READ_FILE
        | LANDLOCK_ACCESS_FS_READ_DIR
        | LANDLOCK_ACCESS_FS_REMOVE_DIR
        | LANDLOCK_ACCESS_FS_REMOVE_FILE
        | LANDLOCK_ACCESS_FS_MAKE_CHAR
        | LANDLOCK_ACCESS_FS_MAKE_DIR
        | LANDLOCK_ACCESS_FS_MAKE_REG
        | LANDLOCK_ACCESS_FS_MAKE_SOCK
        | LANDLOCK_ACCESS_FS_MAKE_FIFO
        | LANDLOCK_ACCESS_FS_MAKE_BLOCK
        | LANDLOCK_ACCESS_FS_MAKE_SYM;

    if abi >= 2 {
        flags |= LANDLOCK_ACCESS_FS_REFER;
    }
    if abi >= 3 {
        flags |= LANDLOCK_ACCESS_FS_TRUNCATE;
    }
    if abi >= 5 {
        flags |= LANDLOCK_ACCESS_FS_IOCTL_DEV;
    }
    flags
}

/// Read-only access for a DIRECTORY: list the directory (`READ_DIR`) and read
/// or execute the files beneath it (`READ_FILE`, `EXECUTE`).
///
/// A `path_beneath` rule on a directory grants its `allowed_access` both to
/// the directory and to the files beneath it, so `READ_FILE` and `EXECUTE` are
/// what let the agent read and run things under the directory; they are not
/// redundant with `READ_DIR`.
pub fn landlock_ro_fs_flags(_abi: u32) -> u64 {
    LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR
}

/// Read-write access for a DIRECTORY: the handled set for the given ABI, which
/// covers creating/removing entries in the directory (directory flags) and
/// reading/writing the files beneath it (file flags).
pub fn landlock_rw_fs_flags(abi: u32) -> u64 {
    landlock_handled_fs_flags(abi)
}

/// Read-only access for a single regular FILE (e.g. the `agy` binary or a
/// config file): read and execute it.
fn file_ro_flags() -> u64 {
    LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE
}

/// Read-write access for a single regular FILE: read, execute, write and
/// truncate it.
fn file_rw_flags() -> u64 {
    LANDLOCK_ACCESS_FS_EXECUTE
        | LANDLOCK_ACCESS_FS_READ_FILE
        | LANDLOCK_ACCESS_FS_WRITE_FILE
        | LANDLOCK_ACCESS_FS_TRUNCATE
}

/// Access for a character/block device node such as `/dev/null`: read and (for
/// a writable device) write it. `EXECUTE` and `TRUNCATE` are meaningless on a
/// device node and are deliberately omitted.
fn device_flags(writable: bool) -> u64 {
    let mut flags = LANDLOCK_ACCESS_FS_READ_FILE;
    if writable {
        flags |= LANDLOCK_ACCESS_FS_WRITE_FILE;
    }
    flags
}

/// Chooses the Landlock access flags for `path` by inspecting the path type:
/// a regular file (e.g. the `agy` binary or a single config file) gets
/// file-only access flags, while a directory (or a path that does not exist,
/// treated as a directory) gets the directory access set.
///
/// Passing directory-only flags such as `READ_DIR` to a regular file makes
/// `landlock_add_rule` fail or drop the rule; this helper keeps the two cases
/// distinct. `writable` selects the read-only vs read-write set.
pub fn access_flags_for_path(path: &Path, writable: bool) -> u64 {
    let dir_flags = if writable {
        landlock_rw_fs_flags(KATA_MIN_LANDLOCK_ABI)
    } else {
        landlock_ro_fs_flags(KATA_MIN_LANDLOCK_ABI)
    };
    match fs::metadata(path) {
        Ok(m) if m.is_dir() => dir_flags,
        Ok(_) => {
            // Regular file (or other non-directory path): file-only flags.
            if writable {
                file_rw_flags()
            } else {
                file_ro_flags()
            }
        }
        // Missing/unclassifiable: default to the directory set; `add_rule`
        // skips paths that do not exist, so the exact flags never matter here.
        Err(_) => dir_flags,
    }
}

/// Kubernetes ServiceAccount token directory, mounted by kubelet into every
/// pod that runs with a service account. An agent must never be able to read
/// it (it carries the cluster credential), so the Kata backend DENYs it even
/// if a policy lists it under `ro`/`rw`/`shared_rw`.
pub const K8S_SERVICEACCOUNT_TOKEN_DIR: &str = "/var/run/secrets/kubernetes.io";

/// Returns `true` when `path` is, or is under, the Kubernetes
/// ServiceAccount token directory. `Path::starts_with` is component-wise, so a
/// sibling such as `/var/run/secrets/kubernetes.io.bak` is NOT matched.
pub fn is_k8s_serviceaccount_token_path(path: &Path) -> bool {
    let base = std::path::Path::new(K8S_SERVICEACCOUNT_TOKEN_DIR);
    path == base || path.starts_with(base)
}

/// Returns `true` when `path` is a git repository (has a `.git` entry) or is
/// inside one (its nearest ancestor has a `.git` entry).
fn is_git_repository(path: &Path) -> bool {
    let mut cur = Some(path);
    while let Some(dir) = cur {
        if dir.join(".git").exists() {
            return true;
        }
        cur = dir.parent();
    }
    false
}

/// Resolves `path` through symlinks when it exists (`/var/run` -> `/run`,
/// `..data` secret-volume links); otherwise returns it unchanged. Deny checks
/// must compare resolved paths: Landlock rules attach to the resolved inode, so
/// a lexical check is bypassed by spelling the same directory another way.
fn resolve(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Returns `true` when granting `path` must be DENIED (never granted) by the
/// Kata Landlock domain:
/// - it is, is under, or CONTAINS the Kubernetes ServiceAccount token
///   directory (a parent grant exposes it just as well), unless
///   `allow_serviceaccount` lifts that deny; or
/// - it overlaps an EXISTING `hide` entry that is a git repository (so its
///   object store cannot be read through a sibling that is allowed).
///
/// All comparisons are on resolved paths, so symlinked spellings (`/run/...`
/// for `/var/run/...`) are denied too.
pub fn is_denied_path(path: &Path, hide: &[PathBuf], allow_serviceaccount: bool) -> bool {
    let p = resolve(path);
    if !allow_serviceaccount {
        let sa = resolve(Path::new(K8S_SERVICEACCOUNT_TOKEN_DIR));
        if is_k8s_serviceaccount_token_path(path)
            || p == sa
            || p.starts_with(&sa)
            || sa.starts_with(&p)
        {
            return true;
        }
    }
    // A `hide` entry that does not exist hides nothing, so it cannot be reached
    // through a sibling or parent grant either. (Already in main for the parent
    // walk; kept here with the resolved comparison.)
    hide.iter().any(|h| {
        let h = resolve(h);
        (p == h || p.starts_with(&h) || h.starts_with(&p)) && h.exists() && is_git_repository(&h)
    })
}

/// Drains every exited child other than `child_pid` (the agent root) so the
/// launcher does not accumulate `<defunct>` zombies, and reports whether the
/// agent root itself exited while draining.
///
/// The launcher is a child subreaper, so any orphaned descendant of the agent
/// (or of a process that reparented here) becomes a direct child of the
/// launcher. If the supervise loop only ever waited for `child_pid`, every
/// orphan that exited afterwards would stay a zombie of the launcher until the
/// launcher itself exited -- a long-lived session launcher would accumulate
/// them until the pod pid limit is hit, and `kill -0` on such a zombie still
/// succeeds, so liveness probes misread an exited process as alive.
///
/// Returns `Some(status)` when the drain reaped `child_pid` first (the agent
/// just exited in the gap between the targeted wait and the drain); the caller
/// must then run the normal exit path on that `status`, exactly as if the
/// targeted wait had seen it. Returns `None` when only other children (pure
/// orphans) were reaped, or when there is nothing left to reap.
///
/// `WNOHANG` keeps the caller's polling cadence (the short sleep) intact -- the
/// loop never busy-waits. Stops when no child has exited (`0`), when there are
/// no more children to reap (`ECHILD`), or when the agent is reaped here.
/// `EINTR` re-tries the drain.
///
/// `clippy::zombie_processes` is suppressed: the whole point of this helper is
/// to collect (reap) exited children rather than leave them unwaited, so a
/// linter that flags "a child is spawned and never waited" does not apply.
#[allow(clippy::zombie_processes)]
fn drain_adopted_orphans(child_pid: libc::pid_t) -> Option<libc::c_int> {
    let mut orphan_status: libc::c_int = 0;
    loop {
        let reaped = unsafe { libc::waitpid(-1, &mut orphan_status, libc::WNOHANG) };
        if reaped == 0 {
            return None;
        }
        if reaped < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EINTR) {
                return None; // ECHILD (or other): no other exited children left
            }
            continue;
        }
        if reaped == child_pid {
            // The agent exited and the drain reaped it first: hand its status
            // back so the caller takes the normal exit path on it.
            return Some(orphan_status);
        }
        // Reaped an adopted orphan; keep draining the rest.
    }
}

/// Returns the reason a GRANT path cannot be applied as a Landlock rule, or
/// `None` when it can. `add_rule` silently skips a path that does not exist or
/// cannot be opened (the base policy's long-standing behaviour), so the grant
/// path checks both itself and reports the grant as dropped with this reason
/// instead of skipping it quietly: the session starts with strictly less
/// access, one warning per dropped grant.
pub fn grant_apply_blocker(path: &Path) -> Option<String> {
    if !path.exists() {
        return Some(format!("path '{}' does not exist", path.display()));
    }
    let c_path = std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str());
    let Ok(c_path) = CString::new(c_path) else {
        // A NUL byte is reported by `add_rule` itself; nothing to pre-check.
        return None;
    };
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        return Some(format!("path '{}' cannot be opened: {err}", path.display()));
    }
    unsafe { libc::close(fd) };
    None
}

/// Executes a command in the Kata sandbox backend.
pub fn execute_kata(
    policy: &SandboxPolicy,
    cmd: &str,
    args: &[String],
    env: &[(String, String)],
) -> Result<i32, String> {
    // Network mode: fail closed on an unknown value, deterministically before
    // any probe or fork; `relay` opts into the private network namespace +
    // unix-socket relay (see super::relay). `host`/`shared` keep today's
    // behaviour byte-for-byte.
    let relay = kata_net_relay(&policy.net)?;
    let abi = probe_landlock_abi();
    if !kata_abi_ok(abi) {
        return Err(format!(
            "kata backend requires Landlock ABI >= {KATA_MIN_LANDLOCK_ABI} (for the signal and \
             abstract-unix-socket scopes); the kernel reported ABI {abi} -- refusing to run with \
             weaker isolation"
        ));
    }

    // Read secret fd if configured. The guard zeroizes the in-memory copy when
    // it is dropped: on the success path we drop it right after the secret file
    // is written (below), and on any earlier error return path it is dropped as
    // the function unwinds -- so the secret never lingers in the launcher's
    // heap. Mirrors the gVisor backend, which calls `zeroize_bytes` on its
    // in-memory secret right after the secret file is written.
    let secret = SecretBytesGuard(if let Some(ref sec) = policy.secret {
        read_secret_fd(sec.fd)?
    } else {
        Vec::new()
    });

    // Setup private tmp and secret file (writes the secret to a 0400 file).
    let private_tmp = setup_kata_private_tmp(policy, &secret.0)?;

    // The secret has now been durably written to its 0400 destination in the
    // private tmp (or there was no secret). Wipe the in-memory copy before the
    // fork/exec of the target so it cannot be recovered from the launcher's heap
    // via /proc/<pid>/mem / ptrace by a same-uid child. The guard's `Drop` does
    // the zeroizing; the target does not read these bytes (it reads the secret
    // file on disk), so dropping them now is safe in every path.
    drop(secret);

    // Contract v1.3 fail-closed: every writable policy path (rw, shared_rw and
    // the private tmp itself) must exist before the Landlock domain is built.
    // `landlock_add_rule` silently skips paths that do not exist, so a policy
    // rw/shared_rw path that cannot be created must fail the launch (exit 125)
    // rather than silently producing a weaker domain (R09). The private tmp was
    // just created (or already existed) so creating it again is idempotent.
    policy.ensure_writable_dirs(private_tmp.as_ref().map(|g| g.path.as_path()))?;

    // Relay pre-flight, in the launcher process: the private socket dir, the
    // outer proxy (one thread per connection) and the launcher exe path the
    // child needs. This MUST happen before the fork: the proxy itself must
    // run in the pod's network namespace (which this launcher process keeps
    // -- only the forked child unshares into the agent's private network
    // namespace below).
    let relay_setup = if relay {
        Some(relay_lib::setup_relay_launcher(policy)?)
    } else {
        None
    };

    // Set self as subreaper
    unsafe {
        if libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("prctl(PR_SET_CHILD_SUBREAPER) failed: {err}"));
        }
    }

    // Fork child
    let child_pid = unsafe { libc::fork() };
    if child_pid < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("fork failed in kata backend: {err}"));
    }

    if child_pid > 0 {
        // Parent (the launcher / `sandbox-exec`): supervise the agent root.
        //
        // The orchestrator stops a job by sending the launcher SIGTERM/SIGINT/
        // SIGHUP. With no handler the launcher's default action is to die,
        // which skips kill_tree_and_reap and leaves the agent and its
        // descendants running (setsid daemons). Install async-signal-safe
        // handlers that ONLY record the signal number; the supervise loop
        // below performs the actual kill_tree_and_reap and exits 128+signal.
        let old_term = install_launcher_signal(libc::SIGTERM);
        let old_int = install_launcher_signal(libc::SIGINT);
        let old_hup = install_launcher_signal(libc::SIGHUP);

        let mut status: libc::c_int = 0;
        loop {
            let mut r = unsafe { libc::waitpid(child_pid, &mut status, libc::WNOHANG) };
            if r == 0 {
                // The agent has not exited yet. Drain the exited children that
                // this subreaper adopted (orphans of the agent's subtree) so
                // they do not pile up as zombies of the launcher. If the agent
                // happened to exit in the gap after the targeted wait above,
                // the drain reaps it and reports its status; the exit path
                // below is then taken on that status, exactly as if the
                // targeted wait had seen it (no exit status is lost).
                if let Some(adopted) = drain_adopted_orphans(child_pid) {
                    status = adopted;
                    r = child_pid;
                }
            }
            if r == child_pid {
                // The agent exited (either seen by the targeted wait, or
                // reaped by the orphan drain above): clean up any adopted
                // descendants, the private tmp and the relay socket dir,
                // restore the previous signal dispositions and return the
                // agent's exit status. The cleanups must run even when the
                // agent crashed, so the next launch with the same agent_id
                // does not trip over a stale directory / socket / secret file
                // (F1).
                let code = if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status)
                } else if libc::WIFSIGNALED(status) {
                    128 + libc::WTERMSIG(status)
                } else {
                    1
                };
                kill_tree_and_reap();
                if let Some(g) = private_tmp.as_ref() {
                    g.cleanup();
                }
                if let Some(setup) = &relay_setup {
                    let _ = fs::remove_dir_all(&setup.dir);
                }
                restore_launcher_signals((old_term, old_int, old_hup));
                return Ok(code);
            }

            // Did the orchestrator signal us while we were waiting?
            let sig = KATA_LAUNCHER_SIG.swap(0, Ordering::SeqCst);
            if sig != 0 {
                // Tear down the whole tree (the agent is our direct child, so
                // the descendant walk reaches it and its subtree), clean up
                // the private tmp and the relay socket dir, and exit
                // 128+signal, mirroring a normal signal death of the launcher.
                // `_exit` bypasses Drop, so the cleanup must be explicit.
                kill_tree_and_reap();
                if let Some(g) = private_tmp.as_ref() {
                    g.cleanup();
                }
                if let Some(setup) = &relay_setup {
                    let _ = fs::remove_dir_all(&setup.dir);
                }
                unsafe {
                    libc::_exit(128 + sig);
                }
            }

            if r < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EINTR) {
                    return Err(format!("waitpid on agent root failed: {err}"));
                }
                continue;
            }

            // r == 0: the agent has not finished yet; poll again shortly.
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    // --- INSIDE CHILD PROCESS ---
    // Relay mode: the child (and everything forked from it -- the relay-inner
    // and the agent) gets its OWN network namespace, loopback only. The
    // launcher (our parent) keeps the pod's network namespace and runs the
    // outer proxy there. The unshares MUST happen before the Landlock domain
    // is applied: the domain allows /proc read-only, and the uid_map writes
    // the user-namespace setup performs would be denied behind it.
    if relay {
        if let Err(e) = kata_relay_unshare_private_net() {
            eprintln!("kubeadp-agent sandbox setup error: {e}");
            unsafe { libc::_exit(125) };
        }
    }

    // Private /dev/shm tmpfs: mounts a private tmpfs over /dev/shm inside a
    // dedicated mount namespace (unsharing CLONE_NEWUSER if not already unshared
    // for relay, then CLONE_NEWNS). Best effort: it is a convenience (POSIX
    // semaphores / shared memory), not a security boundary, and a session must
    // always start. If a step is not permitted (no unprivileged user namespaces,
    // no tmpfs mount), the launch goes on without it and /dev/shm stays
    // read-only under the Landlock policy (no rw rule for the shared /dev/shm).
    if let Err(e) = kata_prepare_private_shm(relay) {
        eprintln!("kubeadp-agent sandbox warning: private /dev/shm not available: {e}");
    }

    // Relay mode: start the inner relay (a subcommand of this binary) before
    // the Landlock domain is applied, so the relay stays unrestricted. It
    // listens on 127.0.0.1:3128 in the agent's network namespace and copies
    // each TCP connection to the OUTER proxy socket path (path-based unix
    // sockets are not namespaced by network namespaces, and the kata backend
    // has no mount plumbing to bind the socket into the private tmp). It dies
    // with the sandbox: the launcher's tree-kill reaps it when the agent
    // exits, and its parent-death signal fires when the agent root dies.
    if let (true, Some(setup)) = (relay, &relay_setup) {
        let socket_target = kata_relay_socket_target(setup);
        let argv = relay_lib::relay_inner_argv(&setup.exe, &socket_target);
        let inner_env = vec![("PATH".to_string(), super::exec::default_path().to_string())];
        let my_pid = unsafe { libc::getpid() };
        let relay_pid = unsafe { libc::fork() };
        if relay_pid < 0 {
            eprintln!("kubeadp-agent relay: fork relay-inner failed");
            unsafe { libc::_exit(125) };
        }
        if relay_pid == 0 {
            // --- INSIDE RELAY-INNER ---
            // If the agent root (our parent) dies, take ourselves down.
            arm_pdeathsig(libc::SIGKILL, my_pid);
            execute_target(&argv[0], &argv[1..], &inner_env);
        }
    }

    // Apply Landlock ruleset. Unchanged by the relay mode: the agent reaches
    // the outer proxy socket only through the relay-inner (a loopback TCP
    // connection to 127.0.0.1:3128), and no new Landlock rule is needed --
    // connect to a pathname unix socket is not Landlock-restricted below ABI 9
    // (`LANDLOCK_ACCESS_FS_RESOLVE_UNIX`), which this ruleset never handles,
    // and network sockets are unrestricted (`handled_access_net` is 0).
    if let Err(e) =
        apply_landlock_domain(policy, abi, private_tmp.as_ref().map(|g| g.path.as_path()))
    {
        eprintln!("kubeadp-agent landlock domain error: {e}");
        unsafe { libc::_exit(125) };
    }

    // Stop same-uid siblings from reading our /proc data via the ptrace gate.
    // Kata shares the pod's /proc (no fresh mount) and Landlock's read grant
    // on /proc does NOT gate the per-process data files (environ/maps/mem) --
    // those are filtered by ptrace_may_access, which a same-uid sibling
    // passes. Setting PR_SET_DUMPABLE(0) mirrors what the gVisor backend does
    // (its fresh /proc + dumpable=0) and closes the cross-agent data read on
    // the Landlock path too (R01: a sibling must not read the victim's
    // environ/maps/mem).
    unsafe {
        libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0);
    }

    // Environment filtering
    let mut final_env: Vec<(String, String)> =
        policy.filter_env(env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    if let Some(ref guard) = private_tmp {
        let tmp_path = &guard.path;
        final_env.retain(|(k, _)| k != "TMPDIR");
        final_env.push(("TMPDIR".to_string(), tmp_path.to_string_lossy().to_string()));
    }
    // With `net: relay` the loopback proxy env is injected on top of the
    // filtered set (a no-op for the host mode, which keeps the filtered set
    // byte-for-byte).
    let final_env = relay_lib::apply_relay_env(
        policy,
        final_env,
        relay_setup.as_ref().map(|s| s.exe.as_path()),
    );

    // Exec command.
    // Inside the user namespace the agent runs as an unprivileged UID (1000:1000
    // mapped 1:1), so execve clears all process capabilities; the agent has no
    // capabilities after exec. PR_SET_NO_NEW_PRIVS and Landlock restrict_self
    // remain in place.
    execute_target(cmd, args, &final_env);
}

/// Pure decision for the Kata backend's network mode: `true` when the policy
/// selects the relay mode, `false` for `host`/`shared` (today's behaviour,
/// byte-for-byte). Unknown values fail closed, mirroring the gVisor backend.
pub fn kata_net_relay(net: &str) -> Result<bool, String> {
    match net {
        "relay" => Ok(true),
        "host" | "shared" => Ok(false),
        other => Err(format!(
            "unknown net value '{other}' (accepted: host, shared, relay)"
        )),
    }
}

/// The namespace flags the Kata child unshares for the private network
/// namespace, in order: CLONE_NEWUSER first (the caller's uid/gid mapped 1:1,
/// unprivileged), then CLONE_NEWNET (allowed by the capabilities the fresh
/// user namespace grants). No mount/PID/IPC namespaces: Landlock is the kata
/// filesystem isolation and /proc stays shared.
fn kata_relay_unshare_flags() -> (libc::c_int, libc::c_int) {
    (libc::CLONE_NEWUSER, libc::CLONE_NEWNET)
}

/// The socket path the Kata relay-inner copies each connection to: the OUTER
/// proxy socket itself. Path-based unix sockets are not namespaced by network
/// namespaces, and the kata backend has no mount plumbing to bind the socket
/// into the private tmp (the gVisor backend bind-mounts it there instead).
fn kata_relay_socket_target(setup: &relay_lib::RelaySetup) -> PathBuf {
    setup.sock.clone()
}

/// Unshares a user namespace with the caller's uid and gid mapped 1:1,
/// denying additional groups. Fails closed with the caller context in the error.
fn kata_unshare_user_ns(context: &str) -> Result<(), String> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    unsafe {
        if libc::unshare(libc::CLONE_NEWUSER) != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "kubeadp-agent {context}: unshare(CLONE_NEWUSER) failed: {err} (the kata backend \
                 needs unprivileged user namespaces)"
            ));
        }
    }

    fs::write("/proc/self/setgroups", "deny\n")
        .map_err(|e| format!("kubeadp-agent {context}: write setgroups failed: {e}"))?;
    fs::write("/proc/self/uid_map", format!("{uid} {uid} 1\n"))
        .map_err(|e| format!("kubeadp-agent {context}: write uid_map failed: {e}"))?;
    fs::write("/proc/self/gid_map", format!("{gid} {gid} 1\n"))
        .map_err(|e| format!("kubeadp-agent {context}: write gid_map failed: {e}"))?;

    Ok(())
}

/// Prepares a private per-session `/dev/shm` tmpfs mount inside the Kata child.
///
/// Unshares `CLONE_NEWUSER` (if not already unshared by relay setup) mapped 1:1,
/// unshares `CLONE_NEWNS`, marks the root mount tree private (`MS_REC | MS_PRIVATE`),
/// and mounts a tmpfs over `/dev/shm` with options `size=64m,nr_inodes=65536,mode=0700`.
///
/// Fails closed on any error.
pub fn kata_prepare_private_shm(user_ns_already_unshared: bool) -> Result<(), String> {
    if !user_ns_already_unshared {
        kata_unshare_user_ns("/dev/shm")?;
    }

    unsafe {
        if libc::unshare(libc::CLONE_NEWNS) != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "kubeadp-agent /dev/shm: unshare(CLONE_NEWNS) failed: {err}"
            ));
        }

        if libc::mount(
            c"none".as_ptr(),
            c"/".as_ptr(),
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        ) != 0
        {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "kubeadp-agent /dev/shm: mount(/, MS_PRIVATE) failed: {err}"
            ));
        }

        let opts = CString::new(KATA_SHM_PRIVATE_OPTS).map_err(|e| e.to_string())?;
        if libc::mount(
            c"tmpfs".as_ptr(),
            c"/dev/shm".as_ptr(),
            c"tmpfs".as_ptr(),
            0,
            opts.as_ptr() as *const libc::c_void,
        ) != 0
        {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "kubeadp-agent /dev/shm: mount tmpfs on /dev/shm failed: {err}"
            ));
        }
    }

    PRIVATE_SHM_MOUNTED.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

/// Creates the agent's private network namespace in the Kata backend's forked
/// child: unshare(CLONE_NEWUSER) with the caller's own uid/gid mapped 1:1
/// (works unprivileged, as with the gVisor backend), then
/// unshare(CLONE_NEWNET) -- allowed by the capabilities the fresh user
/// namespace grants -- and bring loopback up inside the new namespace (no
/// external `ip` binary; see [`relay_lib::bring_loopback_up`]). Fails closed
/// with the missing capability named: without unprivileged user namespaces
/// the private network namespace cannot be created, so the launch aborts
/// (exit 125) rather than running with weaker network isolation.
fn kata_relay_unshare_private_net() -> Result<(), String> {
    let (_user_flag, net_flag) = kata_relay_unshare_flags();
    kata_unshare_user_ns("relay")?;

    unsafe {
        if libc::unshare(net_flag) != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "kubeadp-agent relay: unshare(CLONE_NEWNET) failed: {err}"
            ));
        }
    }

    relay_lib::bring_loopback_up()
}

/// Applies the Landlock domain ruleset to the current process.
pub fn apply_landlock_domain(
    policy: &SandboxPolicy,
    abi: u32,
    private_tmp: Option<&Path>,
) -> Result<(), String> {
    let handled_fs = landlock_handled_fs_flags(abi);
    let mut handled_scope = 0u64;
    if abi >= 6 {
        handled_scope |= LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET | LANDLOCK_SCOPE_SIGNAL;
    }

    let attr = LandlockRulesetAttr {
        handled_access_fs: handled_fs,
        handled_access_net: 0,
        handled_access_scope: handled_scope,
    };

    let ruleset_fd = unsafe {
        libc::syscall(
            SYS_LANDLOCK_CREATE_RULESET,
            &attr as *const _ as usize,
            std::mem::size_of::<LandlockRulesetAttr>(),
            0usize,
        )
    };

    if ruleset_fd < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("landlock_create_ruleset failed: {err}"));
    }
    let ruleset_fd = ruleset_fd as i32;

    let allow_sa = policy.role == crate::sandbox::policy::PolicyRole::Session
        && policy.grants.as_ref().is_some_and(|g| g.serviceaccount);

    // Helper to add a path-beneath rule. Denied paths (the kubernetes
    // ServiceAccount token dir, or a `hide` entry that is a git repository)
    // are refused even if a policy lists them: the backend fails closed.
    let add_rule = |path: &Path, allowed: u64| -> Result<(), String> {
        if is_denied_path(path, &policy.hide, allow_sa) {
            return Err(format!(
                "kata backend: refusing to grant access to denied path {path:?} (the \
                 kubernetes ServiceAccount token dir or a hidden git repository)"
            ));
        }
        if !path.exists() {
            return Ok(());
        }
        let c_path = std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str());
        let c_path = CString::new(c_path)
            .map_err(|_| format!("kata backend: path {path:?} contains a NUL byte"))?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        if fd < 0 {
            return Ok(()); // Skip if cannot open
        }
        let path_beneath = LandlockPathBeneathAttr {
            allowed_access: allowed,
            parent_fd: fd,
        };
        let res = unsafe {
            libc::syscall(
                SYS_LANDLOCK_ADD_RULE,
                ruleset_fd,
                LANDLOCK_RULE_PATH_BENEATH,
                &path_beneath as *const _ as usize,
                0usize,
            )
        };
        unsafe { libc::close(fd) };
        if res != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("landlock_add_rule on {path:?} failed: {err}"));
        }
        Ok(())
    };

    // Add a read-only rule whose access flags are chosen by the path type.
    let add_ro =
        |path: &Path| -> Result<(), String> { add_rule(path, access_flags_for_path(path, false)) };
    // Add a read-write rule whose access flags are chosen by the path type.
    // The ServiceAccount token directory can NEVER be granted read-write under any circumstance.
    let add_rw = |path: &Path| -> Result<(), String> {
        if is_k8s_serviceaccount_token_path(path) {
            return Err(format!(
                "kata backend: refusing to grant write access to ServiceAccount path {path:?}"
            ));
        }
        add_rule(path, access_flags_for_path(path, true))
    };
    // Add a read/write rule for a device node (no execute/truncate).
    let add_device_rw = |path: &Path| -> Result<(), String> { add_rule(path, device_flags(true)) };

    // 1. System read-only roots: the agent must always be able to read /proc
    //    (its own process info) and /dev (device nodes).
    add_ro(Path::new("/proc"))?;
    add_ro(Path::new("/dev"))?;
    // 2. The device nodes the agent needs, with read/write where required.
    add_device_rw(Path::new("/dev/null"))?;
    add_device_rw(Path::new("/dev/zero"))?;
    add_device_rw(Path::new("/dev/urandom"))?;
    add_device_rw(Path::new("/dev/tty"))?;

    // 3. RO allow-list (flags chosen by path type: files get file-only flags).
    for ro_path in &policy.ro {
        add_ro(ro_path)?;
    }

    // 4. RW allow-list.
    for rw_path in &policy.rw {
        add_rw(rw_path)?;
    }
    for shared_path in &policy.shared_rw {
        add_rw(shared_path)?;
    }
    if let Some(tmp) = private_tmp {
        add_rw(tmp)?;
    }
    // Private /dev/shm tmpfs is mounted inside the sandbox; Landlock requires an
    // explicit RW rule for /dev/shm so the agent can create semaphores and shared memory.
    // Only when the private tmpfs is really there: the shared guest /dev/shm is never opened up.
    if PRIVATE_SHM_MOUNTED.load(std::sync::atomic::Ordering::SeqCst) {
        add_rw(Path::new("/dev/shm"))?;
    }

    // 5. Grants allow-list (if session has grants). Grants are optional extras
    //    on top of the base policy, and a Landlock allow-list only ever adds
    //    access. A grant that cannot be applied (a forbidden/denied path, a
    //    write to the ServiceAccount dir, a missing/unopenable path, or the
    //    kernel rejecting the rule) is DROPPED with a warning and the session
    //    starts with less access: isolation is unaffected, whereas aborting
    //    would crash-loop the whole session over one optional path. The base
    //    policy above stays fatal.
    //
    //    Path grants may be directories or single files: `access_flags_for_path`
    //    picks the directory set (READ_DIR + file flags) for a directory and the
    //    file-only set for a regular file. A single-file grant exposes exactly
    //    that file: path lookup and `stat` are not Landlock-restricted, so the
    //    session reads, stats and names the file through its parent, but listing
    //    the parent needs READ_DIR on it and is NOT granted (Landlock has no
    //    metadata-only directory access; a partial parent grant would expose
    //    sibling names). Granting the directory is the supported way for tools
    //    that list the parent; the supervisor warns when a file grant's parent
    //    holds other entries.
    if let Some(ref g) = policy.grants {
        let grant = |what: &str, r: Result<(), String>| {
            if let Err(e) = r {
                eprintln!("kubeadp-agent: sandbox grant dropped ({what}): {e}");
            }
        };
        // `add_rule` silently skips a missing/unopenable path (base-policy
        // behaviour), so each grant is pre-checked and reported as dropped.
        let applicable = |path: &Path| -> Result<(), String> {
            match grant_apply_blocker(path) {
                Some(reason) => Err(reason),
                None => Ok(()),
            }
        };
        if g.serviceaccount {
            grant(
                "serviceaccount",
                applicable(Path::new(K8S_SERVICEACCOUNT_TOKEN_DIR))
                    .and_then(|()| add_ro(Path::new(K8S_SERVICEACCOUNT_TOKEN_DIR))),
            );
        }
        for ro_path in &g.ro_paths {
            grant(
                &format!("ro {}", ro_path.display()),
                applicable(ro_path).and_then(|()| add_ro(ro_path)),
            );
        }
        for rw_path in &g.rw_paths {
            grant(
                &format!("rw {}", rw_path.display()),
                applicable(rw_path).and_then(|()| add_rw(rw_path)),
            );
        }
    }

    // 5. Set PR_SET_NO_NEW_PRIVS before restrict_self
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        let err = std::io::Error::last_os_error();
        unsafe { libc::close(ruleset_fd) };
        return Err(format!("prctl(PR_SET_NO_NEW_PRIVS) failed: {err}"));
    }

    // 6. Enforce ruleset
    let res = unsafe { libc::syscall(SYS_LANDLOCK_RESTRICT_SELF, ruleset_fd, 0usize) };
    unsafe { libc::close(ruleset_fd) };

    if res != 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("landlock_restrict_self failed: {err}"));
    }

    Ok(())
}

/// Guard over the Kata backend's private tmp directory.
///
/// [`setup_kata_private_tmp`] creates the private tmp (and the secret file in
/// it) and hands back this guard. The launcher (parent process) must run
/// [`KataPrivateTmp::cleanup`] once the agent has exited -- and explicitly
/// before `libc::_exit` on the signal path, because `_exit` bypasses `Drop`.
/// `cleanup` removes the secret file(s) this launcher created and, when this
/// launcher owns the directory, the directory itself, so a subsequent launch
/// with the same `agent_id` does not fail with "File exists" (os error 17).
///
/// `Drop` also runs `cleanup`, covering every `?`-early-return path in
/// `execute_kata` after the tmp was created, while the forked child never
/// runs it (all child paths terminate in `_exit`).
struct KataPrivateTmp {
    path: PathBuf,
    remove_dir: bool,
    secret_files: Vec<PathBuf>,
}

impl KataPrivateTmp {
    /// Removes the secret files this launcher created and, when it owns the
    /// directory, the private tmp directory itself. Best-effort: a failed
    /// removal is logged but never propagates, so cleanup can never mask the
    /// agent's exit code.
    fn cleanup(&self) {
        for s in &self.secret_files {
            if let Err(e) = fs::remove_file(s) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    eprintln!(
                        "kubeadp-agent: failed to remove secret {}: {e}",
                        s.display()
                    );
                }
            }
        }
        if self.remove_dir {
            if let Err(e) = fs::remove_dir_all(&self.path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    eprintln!(
                        "kubeadp-agent: failed to remove private tmp {}: {e}",
                        self.path.display()
                    );
                }
            }
        }
    }
}

impl Drop for KataPrivateTmp {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn setup_kata_private_tmp(
    policy: &SandboxPolicy,
    secret_bytes: &[u8],
) -> Result<Option<KataPrivateTmp>, String> {
    if policy.tmp != "auto" && policy.tmp.is_empty() {
        return Ok(None);
    }

    let (path, existed) = if policy.tmp == "auto" {
        let agent_name = policy.agent_id.as_deref().unwrap_or("agent");
        let path = PathBuf::from(format!("/tmp/agent-sandbox-{agent_name}"));
        let existed = path.exists();
        create_owned_tmp_dir(&path)?;
        (path, existed)
    } else {
        let path = PathBuf::from(&policy.tmp);
        let existed = path.exists();
        create_owned_tmp_dir(&path)?;
        (path, existed)
    };
    // The auto tmp dir is always owned by this launcher (or left over from a
    // crashed launcher), so it is always removed on cleanup; a custom tmp is
    // removed only when this launcher freshly created it in this run.
    let remove_dir = policy.tmp == "auto" || !existed;

    let mut secret_files = Vec::new();
    if let Some(ref sec) = policy.secret {
        use std::io::Write;
        use std::os::unix::io::FromRawFd;
        let dest_path = path.join(&sec.dest);
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create secret parent dir: {e}"))?;
        }
        // A launcher that crashed before cleanup leaves the secret file behind;
        // the next launch must recover it (unlink a stale regular file owned by
        // us; refuse anything else -- fail closed).
        remove_stale_secret(&dest_path)?;
        let dest_c = CString::new(dest_path.to_str().unwrap()).unwrap();
        let fd = unsafe {
            libc::open(
                dest_c.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o400,
            )
        };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "failed to create secret file {dest_path:?} with O_EXCL mode 0400: {err}"
            ));
        }
        let mut file = unsafe { fs::File::from_raw_fd(fd) };
        file.write_all(secret_bytes)
            .map_err(|e| format!("failed to write secret to {dest_path:?}: {e}"))?;
        secret_files.push(dest_path);
    }

    Ok(Some(KataPrivateTmp {
        path,
        remove_dir,
        secret_files,
    }))
}

/// Ensures `path` exists as a real directory owned by the current user, with
/// mode 0700, creating it (and any missing parents) when needed. Fails closed
/// when the path exists but is a symlink or is owned by another user.
///
/// Symlink-safe: the path is inspected with `symlink_metadata` (lstat, which
/// never follows a symlink) BEFORE anything is created, so a pre-planted
/// symlink at the private-tmp path is refused instead of being followed by a
/// create/chmod/remove. When the path is absent the leaf is created with
/// `fs::DirBuilder::create` (i.e. `create_dir`, not `create_dir_all`) at mode
/// 0700 and re-verified as a real directory owned by the current uid.
/// Shared with the gVisor relay mode (the outer proxy socket dir).
pub(crate) fn create_owned_tmp_dir(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let euid = unsafe { libc::geteuid() };

    let existing = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Absent: create a fresh 0700 directory at `path`. Missing parents
            // may be created, but the leaf itself is made with `create` (not
            // `create_all`) so a symlink planted at the leaf is never followed.
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("failed to create private tmp parent: {e}"))?;
            }
            fs::DirBuilder::new()
                .mode(0o700)
                .create(path)
                .map_err(|e| format!("failed to create private tmp {path:?}: {e}"))?;
            return verify_owned_tmp_dir(path, euid);
        }
        Err(e) => return Err(format!("failed to stat private tmp {path:?}: {e}")),
    };

    if existing.file_type().is_symlink() {
        // Refuse rather than follow: removing/chmodding through the link would
        // act on the (possibly foreign) target. Fail closed.
        return Err(format!(
            "private tmp {path:?} is a symlink; refusing to follow it"
        ));
    }
    if !existing.is_dir() {
        return Err(format!("private tmp {path:?} is not a directory"));
    }
    if existing.uid() != euid {
        return Err(format!(
            "private tmp {path:?} is owned by uid {} (expected {euid})",
            existing.uid()
        ));
    }
    // A real directory we already own: tighten to 0700 (a crashed launcher may
    // have left it looser). Safe to reuse — no symlink was involved.
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|e| format!("failed to chmod private tmp {path:?} to 0700: {e}"))
}

/// Confirms `path` is a real directory (not a symlink) owned by `euid`, after
/// it was just created. Fails closed on any deviation.
fn verify_owned_tmp_dir(path: &Path, euid: u32) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path)
        .map_err(|e| format!("failed to stat private tmp {path:?}: {e}"))?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(format!("private tmp {path:?} is not a real directory"));
    }
    if meta.uid() != euid {
        return Err(format!(
            "private tmp {path:?} is owned by uid {} (expected {euid})",
            meta.uid()
        ));
    }
    Ok(())
}

/// Removes a stale secret file left by a launcher that crashed before cleanup.
/// Only a regular file owned by the current user is removed; a symlink,
/// directory, or a file owned by someone else is an error (fail closed --
/// touching such a path could be another agent's or an attacker's).
fn remove_stale_secret(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("failed to stat stale secret {path:?}: {e}")),
    };
    let euid = unsafe { libc::geteuid() };
    if meta.file_type().is_file() && meta.uid() == euid {
        fs::remove_file(path)
            .map_err(|e| format!("failed to remove stale secret {path:?}: {e}"))?;
        Ok(())
    } else {
        Err(format!(
            "refusing to remove stale secret {path:?}: not a regular file owned by the launcher"
        ))
    }
}

fn read_secret_fd(fd: i32) -> Result<Vec<u8>, String> {
    use std::io::Read;
    use std::os::unix::io::FromRawFd;

    let dup_fd = unsafe { libc::dup(fd) };
    if dup_fd < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("failed to dup secret fd {fd}: {err}"));
    }
    let mut file = unsafe { fs::File::from_raw_fd(dup_fd) };
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| format!("failed to read secret fd {fd}: {e}"))?;
    Ok(data)
}

/// RAII guard that zeroizes its in-memory secret bytes when dropped, so a copy
/// read from the secret fd is wiped on every return path (including early error
/// returns) after it was read. Mirrors the gVisor backend, which calls
/// `zeroize_bytes` on its in-memory secret right after the secret file is
/// written.
struct SecretBytesGuard(Vec<u8>);

impl Drop for SecretBytesGuard {
    fn drop(&mut self) {
        zeroize_bytes(&mut self.0);
    }
}

fn execute_target(cmd: &str, args: &[String], env: &[(String, String)]) -> ! {
    exec_target(cmd, args, env)
}

#[cfg(test)]
mod tests {
    #[test]
    fn serviceaccount_dir_is_denied_through_symlinked_spellings_and_parents() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("run/secrets/kubernetes.io");
        std::fs::create_dir_all(real.join("serviceaccount")).unwrap();
        // The check resolves symlinks: a link to the real dir is denied like the dir.
        let link = root.path().join("alias");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        // K8S_SERVICEACCOUNT_TOKEN_DIR is a fixed absolute path, so the symlink and
        // parent cases are exercised through the production path where it exists
        // and through `resolve` equivalence otherwise.
        assert_eq!(resolve(&link), std::fs::canonicalize(&real).unwrap());
        let sa = Path::new(K8S_SERVICEACCOUNT_TOKEN_DIR);
        if let Some(parent) = sa.parent() {
            // A parent grant exposes the token dir just as well.
            assert!(is_denied_path(parent, &[], false));
            assert!(!is_denied_path(parent, &[], true));
        }
    }

    #[test]
    fn missing_hide_entry_under_a_git_root_does_not_deny_the_parent_grant() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join(".git")).unwrap();
        let missing = root.path().join(".agent-jobs");
        // `.agent-jobs` is absent: nothing to hide, so granting the parent is fine.
        assert!(!is_denied_path(
            root.path(),
            std::slice::from_ref(&missing),
            false
        ));
        // Once it exists inside the repository it is hidden and the parent is refused.
        std::fs::create_dir(&missing).unwrap();
        assert!(is_denied_path(
            root.path(),
            std::slice::from_ref(&missing),
            false
        ));
    }

    use super::*;
    use crate::sandbox::policy::SecretConfig;

    #[test]
    fn test_landlock_flag_calculation() {
        let f1 = landlock_handled_fs_flags(1);
        assert_eq!(f1 & LANDLOCK_ACCESS_FS_REFER, 0);
        assert_eq!(f1 & LANDLOCK_ACCESS_FS_TRUNCATE, 0);

        let f2 = landlock_handled_fs_flags(2);
        assert_ne!(f2 & LANDLOCK_ACCESS_FS_REFER, 0);
        assert_eq!(f2 & LANDLOCK_ACCESS_FS_TRUNCATE, 0);

        let f3 = landlock_handled_fs_flags(3);
        assert_ne!(f3 & LANDLOCK_ACCESS_FS_TRUNCATE, 0);

        let f7 = landlock_handled_fs_flags(7);
        assert_ne!(f7 & LANDLOCK_ACCESS_FS_IOCTL_DEV, 0);
    }

    #[test]
    fn test_landlock_path_allowlist_builder() {
        let policy = SandboxPolicy {
            rw: vec![PathBuf::from("/workspace/job1")],
            ro: vec![PathBuf::from("/usr"), PathBuf::from("/lib")],
            shared_rw: vec![PathBuf::from("/home/agent")],
            ..Default::default()
        };

        let ro_access = landlock_ro_fs_flags(5);
        let rw_access = landlock_rw_fs_flags(5);

        assert_eq!(
            ro_access,
            LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR
        );
        assert_eq!(rw_access, landlock_handled_fs_flags(5));
        assert!(rw_access > ro_access);
        assert_eq!(policy.rw.len(), 1);
        assert_eq!(policy.ro.len(), 2);
    }

    // H6a: the Kata backend requires Landlock ABI >= 6.
    #[test]
    fn test_kata_abi_gate() {
        assert_eq!(KATA_MIN_LANDLOCK_ABI, 6);
        for abi in 0..6 {
            assert!(!kata_abi_ok(abi), "ABI {abi} must be rejected");
        }
        for abi in 6..=7 {
            assert!(kata_abi_ok(abi), "ABI {abi} must be accepted");
        }
    }

    // H6b: access flags are chosen by path type.
    #[test]
    fn test_access_flags_by_path_type() {
        let tmp = tempfile::tempdir().unwrap();

        // A real regular file gets file-only flags, no directory flag.
        let file = tmp.path().join("agy");
        std::fs::write(&file, b"bin").unwrap();
        let ro_file = access_flags_for_path(&file, false);
        assert_eq!(ro_file, file_ro_flags());
        assert_eq!(
            ro_file & LANDLOCK_ACCESS_FS_READ_DIR,
            0,
            "a file must not get the dir flag"
        );
        assert_ne!(ro_file & LANDLOCK_ACCESS_FS_READ_FILE, 0);
        assert_ne!(ro_file & LANDLOCK_ACCESS_FS_EXECUTE, 0);
        let rw_file = access_flags_for_path(&file, true);
        assert_eq!(rw_file, file_rw_flags());
        assert_ne!(rw_file & LANDLOCK_ACCESS_FS_WRITE_FILE, 0);
        assert_ne!(rw_file & LANDLOCK_ACCESS_FS_TRUNCATE, 0);
        assert_eq!(rw_file & LANDLOCK_ACCESS_FS_READ_DIR, 0);

        // A real directory keeps the directory flag (ro) / handled set (rw).
        let dir = tmp.path().join("dir");
        std::fs::create_dir(&dir).unwrap();
        assert_eq!(access_flags_for_path(&dir, false), landlock_ro_fs_flags(6));
        assert_eq!(access_flags_for_path(&dir, true), landlock_rw_fs_flags(6));
        assert_ne!(
            access_flags_for_path(&dir, false) & LANDLOCK_ACCESS_FS_READ_DIR,
            0
        );

        // A missing path is treated as a directory (add_rule skips it anyway).
        let missing = tmp.path().join("does-not-exist");
        assert_eq!(
            access_flags_for_path(&missing, false),
            landlock_ro_fs_flags(6)
        );
    }

    // H6c: the deny set covers the ServiceAccount token dir and hidden repos.
    #[test]
    fn test_denied_paths() {
        // ServiceAccount token dir and its children are denied; a sibling is not.
        assert!(is_k8s_serviceaccount_token_path(Path::new(
            K8S_SERVICEACCOUNT_TOKEN_DIR
        )));
        assert!(is_k8s_serviceaccount_token_path(Path::new(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )));
        assert!(!is_k8s_serviceaccount_token_path(Path::new(
            "/var/run/secrets/kubernetes.io.bak"
        )));
        assert!(!is_k8s_serviceaccount_token_path(Path::new(
            "/var/run/secrets"
        )));

        let no_hide: Vec<PathBuf> = Vec::new();
        assert!(is_denied_path(
            Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token"),
            &no_hide,
            false
        ));
        assert!(!is_denied_path(
            Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token"),
            &no_hide,
            true
        ));
        assert!(!is_denied_path(Path::new("/usr/bin"), &no_hide, false));

        // A `hide` entry that is a git repository is denied (and its subpaths).
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        let sibling = tmp.path().join("visible");
        std::fs::create_dir(&sibling).unwrap();
        let hide = vec![repo.clone()];
        assert!(is_denied_path(&repo, &hide, false));
        assert!(is_denied_path(&repo, &hide, true));
        assert!(is_denied_path(&repo.join("objects"), &hide, false));
        // The visible sibling is not a repo and is not hidden.
        assert!(!is_denied_path(&sibling, &hide, false));
        // A `hide` entry that is NOT a git repo does not trigger the deny.
        let plain = tmp.path().join("plain");
        std::fs::create_dir(&plain).unwrap();
        assert!(!is_denied_path(&plain, std::slice::from_ref(&plain), false));
    }

    // H6c (behavioral): a child that restricts only ITSELF must refuse to grant
    // a denied path (the ServiceAccount token dir). `apply_landlock_domain`
    // runs the deny check before any Landlock syscall, so this holds even on a
    // kernel without Landlock.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_apply_landlock_domain_in_child_refuses_denied() {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            let policy = SandboxPolicy {
                ro: vec![PathBuf::from(K8S_SERVICEACCOUNT_TOKEN_DIR)],
                ..Default::default()
            };
            let res = apply_landlock_domain(&policy, KATA_MIN_LANDLOCK_ABI, None);
            // Denied path => the domain application must fail (fail closed).
            unsafe {
                libc::_exit(if res.is_err() { 0 } else { 1 });
            }
        }
        let mut status = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
        }
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "applying a domain that grants the ServiceAccount token dir must fail"
        );
    }

    // HL-151: Test that:
    // (a) default sessions and worker jobs still cannot read ServiceAccount token dir
    // (b) a granted session CAN read it read-only and CANNOT write it
    // (c) a granted path outside allowed shapes is rejected
    #[cfg(target_os = "linux")]
    #[test]
    fn test_session_serviceaccount_grant_and_write_denial() {
        use crate::sandbox::policy::{GrantsConfig, PolicyRole};

        // (a) Worker job with SA token dir in ro is denied
        let worker_policy = SandboxPolicy {
            role: PolicyRole::Worker,
            ro: vec![PathBuf::from(K8S_SERVICEACCOUNT_TOKEN_DIR)],
            ..Default::default()
        };
        let pid1 = unsafe { libc::fork() };
        assert!(pid1 >= 0);
        if pid1 == 0 {
            let res = apply_landlock_domain(&worker_policy, KATA_MIN_LANDLOCK_ABI, None);
            unsafe {
                libc::_exit(if res.is_err() { 0 } else { 1 });
            }
        }
        let mut st1 = 0;
        unsafe {
            libc::waitpid(pid1, &mut st1, 0);
        }
        assert!(libc::WIFEXITED(st1) && libc::WEXITSTATUS(st1) == 0);

        // (a2) Default session (no grants) with SA token dir in ro is denied
        let session_no_grants = SandboxPolicy {
            role: PolicyRole::Session,
            ro: vec![PathBuf::from(K8S_SERVICEACCOUNT_TOKEN_DIR)],
            ..Default::default()
        };
        let pid2 = unsafe { libc::fork() };
        assert!(pid2 >= 0);
        if pid2 == 0 {
            let res = apply_landlock_domain(&session_no_grants, KATA_MIN_LANDLOCK_ABI, None);
            unsafe {
                libc::_exit(if res.is_err() { 0 } else { 1 });
            }
        }
        let mut st2 = 0;
        unsafe {
            libc::waitpid(pid2, &mut st2, 0);
        }
        assert!(libc::WIFEXITED(st2) && libc::WEXITSTATUS(st2) == 0);

        // (b) Session with serviceaccount: true grant in ro succeeds (or fails gracefully only if landlock unsupported)
        let session_granted = SandboxPolicy {
            role: PolicyRole::Session,
            grants: Some(GrantsConfig {
                serviceaccount: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let pid3 = unsafe { libc::fork() };
        assert!(pid3 >= 0);
        if pid3 == 0 {
            let res = apply_landlock_domain(&session_granted, KATA_MIN_LANDLOCK_ABI, None);
            // Must NOT fail with "refusing to grant access to denied path"
            if let Err(ref e) = res {
                if e.contains("refusing to grant access to denied path") {
                    unsafe {
                        libc::_exit(1);
                    }
                }
            }
            unsafe {
                libc::_exit(0);
            }
        }
        let mut st3 = 0;
        unsafe {
            libc::waitpid(pid3, &mut st3, 0);
        }
        assert!(libc::WIFEXITED(st3) && libc::WEXITSTATUS(st3) == 0);

        // (b2) A session asking for write access to the SA path has that grant dropped (not fatal)
        let session_write_sa = SandboxPolicy {
            role: PolicyRole::Session,
            grants: Some(GrantsConfig {
                serviceaccount: true,
                rw_paths: vec![PathBuf::from(K8S_SERVICEACCOUNT_TOKEN_DIR)],
                ..Default::default()
            }),
            ..Default::default()
        };
        let pid4 = unsafe { libc::fork() };
        assert!(pid4 >= 0);
        if pid4 == 0 {
            let res = apply_landlock_domain(&session_write_sa, KATA_MIN_LANDLOCK_ABI, None);
            // The write grant to the SA path is refused, but only that grant is
            // dropped: the domain is still applied (the session starts).
            // A kernel without Landlock (e.g. the Docker build of CI) cannot create any
            // ruleset: that is the environment, not a failed grant, so it passes.
            let applied = match res {
                Ok(_) => true,
                Err(e) => e.contains("landlock_create_ruleset failed"),
            };
            unsafe {
                libc::_exit(if applied { 0 } else { 1 });
            }
        }
        let mut st4 = 0;
        unsafe {
            libc::waitpid(pid4, &mut st4, 0);
        }
        assert!(libc::WIFEXITED(st4) && libc::WEXITSTATUS(st4) == 0);
    }

    // A grant whose path does not exist is DROPPED with a reason (never fatal),
    // while a grant whose path exists is kept: `add_rule` silently skips
    // missing/unopenable paths, so the grant path pre-check produces the
    // "grant dropped" warning text instead of skipping the grant quietly.
    #[test]
    fn test_grant_apply_blocker_drops_missing_and_keeps_existing_paths() {
        let missing = tempfile::tempdir().unwrap().path().join("does-not-exist");
        let reason = grant_apply_blocker(&missing);
        assert!(
            reason
                .as_deref()
                .is_some_and(|r| r.contains("does not exist")),
            "missing grant path must report a reason: {reason:?}"
        );

        // An existing, openable path is applicable: the grant is kept.
        let dir = tempfile::tempdir().unwrap();
        assert!(grant_apply_blocker(dir.path()).is_none());
        let file = dir.path().join("file");
        std::fs::write(&file, b"x").unwrap();
        assert!(grant_apply_blocker(&file).is_none());
    }

    // The operator API token file stays hidden from every sandboxed agent by
    // default; a single-FILE grant naming it explicitly (the documented
    // re-expose path) is NOT blocked by the kata deny checks, while the
    // ServiceAccount token dir keeps its deny.
    #[test]
    fn test_single_file_token_grant_not_denied() {
        let token_file = PathBuf::from("/var/run/kubeadp/api-token");
        assert!(
            !is_denied_path(&token_file, &[], false),
            "a single-file grant naming the token file must not be denied"
        );
        // A grant that CONTAINS the ServiceAccount dir stays denied.
        assert!(is_denied_path(
            &PathBuf::from("/var/run/secrets"),
            &[],
            false
        ));
        assert!(is_denied_path(
            &PathBuf::from(K8S_SERVICEACCOUNT_TOKEN_DIR),
            &[],
            false
        ));
    }

    // HL-117 (behavioral): a session with a grant whose path does not exist
    // still gets its domain applied -- the grant is dropped with a warning,
    // never fatal.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_apply_landlock_domain_in_child_drops_nonexistent_grant() {
        use crate::sandbox::policy::{GrantsConfig, PolicyRole};

        let missing = tempfile::tempdir().unwrap().path().join("gone");
        let policy = SandboxPolicy {
            role: PolicyRole::Session,
            grants: Some(GrantsConfig {
                ro_paths: vec![missing],
                rw_paths: vec![PathBuf::from("/workspace/definitely-not-here")],
                ..Default::default()
            }),
            ..Default::default()
        };
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            let res = apply_landlock_domain(&policy, KATA_MIN_LANDLOCK_ABI, None);
            // Nonexistent grant paths are dropped (warned), the session starts. A kernel
            // without Landlock (the CI Docker build) cannot create any ruleset: that is
            // the environment, not a failed grant.
            let applied = match res {
                Ok(_) => true,
                Err(e) => e.contains("landlock_create_ruleset failed"),
            };
            unsafe {
                libc::_exit(if applied { 0 } else { 1 });
            }
        }
        let mut status = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
        }
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "a nonexistent grant path must not abort the domain application"
        );
    }

    // HL-113 (behavioral): a single-FILE ro grant lets the child read and stat
    // the exact file through its parent (path lookup and stat are not
    // Landlock-restricted), while listing the parent stays denied -- the
    // documented gap that makes a directory grant the supported way for tools
    // that list the parent. Skipped (counts as pass) where the domain cannot
    // be applied at all (a kernel without Landlock).
    #[cfg(target_os = "linux")]
    #[test]
    fn test_file_grant_reads_and_stats_file_parent_list_denied() {
        use crate::sandbox::policy::{GrantsConfig, PolicyRole};

        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("secret-dir");
        std::fs::create_dir_all(&parent).unwrap();
        let file = parent.join("config");
        std::fs::write(&file, b"content").unwrap();
        // A sibling the file grant must NOT expose.
        std::fs::write(parent.join("sibling"), b"sibling").unwrap();

        let policy = SandboxPolicy {
            role: PolicyRole::Session,
            grants: Some(GrantsConfig {
                ro_paths: vec![file.clone()],
                ..Default::default()
            }),
            ..Default::default()
        };
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            let res = apply_landlock_domain(&policy, KATA_MIN_LANDLOCK_ABI, None);
            let applied = match res {
                Ok(()) => true,
                // No Landlock (or too old an ABI) in this environment: the
                // domain cannot be created, nothing to assert here.
                Err(e) => !e.contains("landlock_create_ruleset failed"),
            };
            if !applied {
                unsafe { libc::_exit(0) };
            }
            // The file itself is readable by path through its parent.
            let content = std::fs::read_to_string(&file).unwrap_or_default();
            if content != "content" {
                unsafe { libc::_exit(1) };
            }
            // stat of the file works (stat is not Landlock-restricted).
            let stat_ok = matches!(std::fs::metadata(&file), Ok(m) if m.is_file());
            if !stat_ok {
                unsafe { libc::_exit(1) };
            }
            // Listing the parent is denied: a file grant never widens to the
            // parent directory (sibling names stay hidden).
            if std::fs::read_dir(&parent).is_ok() {
                unsafe { libc::_exit(1) };
            }
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
        }
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "a single-file grant must read and stat the file while the parent list stays denied"
        );
    }

    // H6c (behavioral): a child that applies the FULL default domain (proc/dev
    // rules + ro/rw allow-list, all flags chosen by path type) and restricts
    // only itself must either succeed (kernel supports Landlock >= 6) or fail
    // cleanly (kernel without Landlock). It must never panic.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_apply_landlock_domain_in_child_default_policy() {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            let policy = SandboxPolicy::default();
            let _res = apply_landlock_domain(&policy, KATA_MIN_LANDLOCK_ABI, None);
            unsafe {
                libc::_exit(0);
            }
        }
        let mut status = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
        }
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "applying the default domain must not abort the child"
        );
    }

    // ---- F1: Kata private tmp guard ----------------------------------------
    //
    // A launcher that crashed before cleanup leaves the private tmp dir (with
    // mode 0700) and the mode-0400 secret file behind; the next launch with the
    // same agent_id must recover cleanly instead of failing with "File exists"
    // (os error 17) or running over a stale/foreign secret.

    fn policy_with_tmp(tmp: &str, secret: Option<SecretConfig>) -> SandboxPolicy {
        SandboxPolicy {
            agent_id: Some("job-f1".to_string()),
            tmp: tmp.to_string(),
            secret,
            ..Default::default()
        }
    }

    #[test]
    fn test_setup_kata_private_tmp_creates_owned_0700_fresh_auto() {
        let policy = policy_with_tmp("auto", None);
        let guard = setup_kata_private_tmp(&policy, b"").expect("auto tmp setup");
        let g = guard.expect("auto tmp always produced a guard");
        let expected = Path::new("/tmp/agent-sandbox-job-f1");
        assert_eq!(g.path, expected);
        assert!(
            g.remove_dir,
            "a fresh/auto tmp dir must be removed on cleanup"
        );
        let meta = std::fs::symlink_metadata(&g.path).unwrap();
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "private tmp must be 0700");
        assert_eq!(meta.uid(), unsafe { libc::geteuid() }, "owned by us");
        assert!(g.path.is_dir(), "private tmp must be a directory");
        // Drop removes it (auto => remove_dir) so a second launch is clean.
        g.cleanup();
        assert!(!g.path.exists(), "cleanup must remove the auto tmp dir");
    }

    #[test]
    fn test_setup_kata_private_tmp_custom_preexisting_kept_but_secret_removed() {
        let dir = tempfile::tempdir().unwrap();
        // Pre-existing custom tmp; the launcher must keep the dir on cleanup
        // (someone else may own it) but still drop the secret file it wrote.
        let sdir = dir.path().join("secret");
        std::fs::create_dir_all(&sdir).unwrap();
        let policy = policy_with_tmp(
            dir.path().to_str().unwrap(),
            Some(SecretConfig {
                fd: -1,
                dest: "secret/creds".to_string(),
            }),
        );
        let guard = setup_kata_private_tmp(&policy, b"top-secret").expect("custom tmp setup");
        let g = guard.expect("custom tmp produced a guard");
        assert!(!g.remove_dir, "custom pre-existing tmp must NOT be removed");
        let secret = dir.path().join("secret/creds");
        assert_eq!(std::fs::read(&secret).unwrap(), b"top-secret");
        use std::os::unix::fs::PermissionsExt;
        let smode = std::fs::metadata(&secret).unwrap().permissions().mode() & 0o777;
        assert_eq!(smode, 0o400, "secret must be 0400");
        g.cleanup();
        assert!(!secret.exists(), "secret must be removed on cleanup");
        assert!(
            dir.path().is_dir(),
            "pre-existing custom tmp dir must survive"
        );
    }

    #[test]
    fn test_setup_kata_private_tmp_replaces_stale_secret() {
        let dir = tempfile::tempdir().unwrap();
        // A crashed launcher left a stale regular secret file behind; the next
        // launch must unlink it (fail closed only for non-file/owned-by-others).
        let stale = dir.path().join("creds");
        std::fs::write(&stale, b"stale").unwrap();
        let policy = policy_with_tmp(
            dir.path().to_str().unwrap(),
            Some(SecretConfig {
                fd: -1,
                dest: "creds".to_string(),
            }),
        );
        let guard = setup_kata_private_tmp(&policy, b"fresh").expect("stale secret recovered");
        let g = guard.expect("stale-secret setup produced a guard");
        assert_eq!(std::fs::read(&stale).unwrap(), b"fresh");
        g.cleanup();
        assert!(!stale.exists());
    }

    #[test]
    fn test_setup_kata_private_tmp_refuses_symlink_secret() {
        let dir = tempfile::tempdir().unwrap();
        // A symlink at the secret destination is an attacker-controlled path:
        // the launch must fail closed, never follow the link.
        let target = dir.path().join("real-target");
        std::fs::write(&target, b"other").unwrap();
        let link = dir.path().join("creds");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let policy = policy_with_tmp(
            dir.path().to_str().unwrap(),
            Some(SecretConfig {
                fd: -1,
                dest: "creds".to_string(),
            }),
        );
        let err = setup_kata_private_tmp(&policy, b"top-secret")
            .err()
            .expect("a symlink secret destination must fail closed");
        assert!(
            err.contains("refusing to remove stale secret")
                || err.contains("failed to create secret"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_setup_kata_private_tmp_refuses_symlink_dir() {
        let dir = tempfile::tempdir().unwrap();
        // A symlink used as the private tmp must fail closed (never chmod or
        // write through the link).
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("tmp-link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let policy = policy_with_tmp(link.to_str().unwrap(), None);
        let err = setup_kata_private_tmp(&policy, b"")
            .err()
            .expect("a symlink private tmp must fail closed");
        assert!(err.contains("symlink"), "unexpected error: {err}");
    }

    #[test]
    fn test_create_owned_tmp_dir_does_not_follow_preplanted_symlink() {
        // A pre-planted symlink at the private-tmp path (pointing at another
        // directory) must NOT be followed: no write/chmod/remove may reach the
        // link's target, and the target and its contents must stay intact. The
        // result is either a clear error (fail closed) or a fresh real
        // directory — never a write through the link.
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("victim");
        std::fs::create_dir(&target).unwrap();
        let marker = target.join("sentinel");
        std::fs::write(&marker, b"keep-me").unwrap();
        let link = dir.path().join("tmp");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let res = create_owned_tmp_dir(&link);

        // The target was never followed: its contents are untouched.
        assert_eq!(
            std::fs::read(&marker).unwrap(),
            b"keep-me".to_vec(),
            "the symlink target must not be written through"
        );
        // No new entry was created inside the target (a write-through would add one).
        let entries: Vec<_> = std::fs::read_dir(&target)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(entries, vec!["sentinel"], "target must be unchanged");

        match res {
            Err(e) => {
                // Fail-closed: the error names the symlink.
                assert!(e.contains("symlink"), "expected a symlink error, got: {e}");
            }
            Ok(()) => {
                // Otherwise the path must now be a fresh real directory owned
                // by us, mode 0700 — and the symlink itself is gone (we did not
                // create the directory through it).
                let meta = std::fs::symlink_metadata(&link).unwrap();
                assert!(
                    !meta.file_type().is_symlink() && meta.is_dir(),
                    "result must be a real directory, not a symlink"
                );
                use std::os::unix::fs::{MetadataExt, PermissionsExt};
                assert_eq!(
                    meta.permissions().mode() & 0o777,
                    0o700,
                    "fresh private tmp must be 0700"
                );
                assert_eq!(meta.uid(), unsafe { libc::geteuid() }, "owned by us");
            }
        }
    }

    #[test]
    fn test_create_owned_tmp_dir_creates_fresh_0700_when_absent() {
        // When the path does not exist, `create_owned_tmp_dir` creates a fresh
        // real 0700 directory owned by the current uid (not a symlink), and a
        // missing parent is created first.
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("a/b/tmp");
        let res = create_owned_tmp_dir(&target);
        assert!(res.is_ok(), "fresh tmp must succeed: {res:?}");
        let meta = std::fs::symlink_metadata(&target).unwrap();
        assert!(meta.is_dir() && !meta.file_type().is_symlink());
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        assert_eq!(
            meta.permissions().mode() & 0o777,
            0o700,
            "fresh private tmp must be 0700"
        );
        assert_eq!(meta.uid(), unsafe { libc::geteuid() }, "owned by us");
    }

    #[test]
    fn test_setup_kata_private_tmp_disabled_when_tmp_empty() {
        let policy = policy_with_tmp("", None);
        let guard = setup_kata_private_tmp(&policy, b"").expect("empty tmp disables setup");
        assert!(guard.is_none(), "empty tmp must disable the private tmp");
    }

    // F6/F7: the kata agent root sets PR_SET_DUMPABLE(0), so a same-uid
    // sibling cannot read its /proc/<pid>/{environ,maps,mem} through the
    // ptrace gate even though kata shares the pod's /proc (no fresh mount).
    // Verify the EFFECTIVE result (the sibling's read of the hardened
    // process's /proc data is blocked) rather than a status field: gVisor's
    // procfs does not always expose the "Dumpable:" line, but the read gate
    // it controls is what the sandbox needs. The check is hermetic (fork +
    // same-uid probe, no privileges needed).
    #[cfg(target_os = "linux")]
    #[test]
    fn test_kata_agent_root_blocks_sameuid_proc_data_read() {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe {
                // The exact hardening the kata agent root applies.
                libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0);
            }
            // Stay alive so the parent can probe our /proc.
            loop {
                unsafe { libc::pause() };
            }
        }
        // Let the prctl land before probing.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let child_environ = format!("/proc/{pid}/environ");
        let read_ok = std::fs::read(&child_environ).is_ok();
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut st = 0;
            libc::waitpid(pid, &mut st, 0);
        }
        assert!(
            !read_ok,
            "a same-uid sibling must not read a non-dumpable agent root's /proc data"
        );
    }

    // ---- net: relay ----------------------------------------------------------
    //
    // `net: relay` on the kata backend gives the agent its own loopback-only
    // network namespace: the launcher runs the outer proxy (see `super::relay`),
    // and the forked child unshares CLONE_NEWUSER|CLONE_NEWNET, brings loopback
    // up and starts `relay-inner` inside the namespace before the Landlock
    // domain is applied. These tests are hermetic: they pin the pure decisions
    // of the kata relay path; the unshare plumbing itself is not exercised (no
    // real namespaces in unit tests).

    #[test]
    fn test_kata_net_mode_selects_relay() {
        // The pure net-mode decision: relay on, host/shared off (today's
        // behaviour byte-for-byte), unknown values fail closed.
        assert_eq!(kata_net_relay("relay"), Ok(true));
        assert_eq!(kata_net_relay("host"), Ok(false));
        assert_eq!(kata_net_relay("shared"), Ok(false));
        let err = kata_net_relay("bridged").unwrap_err();
        assert!(err.contains("unknown net value"), "err: {err}");
    }

    #[test]
    fn test_kata_refuses_unknown_net_before_probe_or_fork() {
        // execute_kata rejects an unknown net value deterministically before
        // any probe or fork (the net-mode match is the first check).
        let policy = crate::sandbox::policy::SandboxPolicy {
            net: "bridged".to_string(),
            ..crate::sandbox::policy::SandboxPolicy::default()
        };
        let err = super::execute_kata(&policy, "true", &[], &[]).unwrap_err();
        assert!(err.contains("unknown net value"), "err: {err}");
    }

    #[test]
    fn test_kata_relay_unshares_user_and_net_namespaces_only() {
        // The kata child creates the private network namespace with
        // unshare(CLONE_NEWUSER) + unshare(CLONE_NEWNET) only: no mount/PID/
        // IPC namespaces (Landlock is the kata filesystem isolation and /proc
        // stays shared).
        let (user, net) = kata_relay_unshare_flags();
        assert_eq!(user, libc::CLONE_NEWUSER);
        assert_eq!(net, libc::CLONE_NEWNET);
    }

    #[test]
    fn test_kata_relay_inner_uses_the_outer_socket_path() {
        // The kata backend has no mount plumbing: relay-inner copies to the
        // OUTER proxy socket path directly (path-based unix sockets are not
        // namespaced by network namespaces), never to a bind target under the
        // private tmp. Asserted on the built argv, no real namespaces.
        let setup = relay_lib::RelaySetup {
            dir: PathBuf::from("/tmp/kubeadp-relay-job1"),
            sock: PathBuf::from("/tmp/kubeadp-relay-job1/proxy.sock"),
            exe: PathBuf::from("/usr/local/bin/kubeadp-agent"),
        };
        let argv = relay_lib::relay_inner_argv(&setup.exe, &kata_relay_socket_target(&setup));
        assert_eq!(
            argv,
            vec![
                "/usr/local/bin/kubeadp-agent".to_string(),
                "relay-inner".to_string(),
                "/tmp/kubeadp-relay-job1/proxy.sock".to_string(),
            ]
        );
    }

    #[test]
    fn test_kata_relay_env_host_and_shared_unchanged() {
        // For host/shared the kata child env is exactly the filtered set plus
        // the private TMPDIR, byte-for-byte (apply_relay_env is a no-op): no
        // proxy variables are injected, no relay argv is built.
        use crate::sandbox::relay::apply_relay_env;
        let policy = crate::sandbox::policy::SandboxPolicy {
            net: "host".to_string(),
            ..crate::sandbox::policy::SandboxPolicy::default()
        };
        let filtered = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("TMPDIR".to_string(), "/tmp/agent-sandbox-job1".to_string()),
        ];
        let out = apply_relay_env(
            &policy,
            filtered.clone(),
            Some(Path::new("/usr/local/bin/kubeadp-agent")),
        );
        assert_eq!(out, filtered, "host env must be unchanged");
        let shared = crate::sandbox::policy::SandboxPolicy {
            net: "shared".to_string(),
            ..crate::sandbox::policy::SandboxPolicy::default()
        };
        let out = apply_relay_env(&shared, filtered.clone(), None);
        assert_eq!(out, filtered, "shared env must be unchanged");
    }

    // The Kata launcher is a child subreaper: any orphaned descendant of the
    // agent reparents to it and must be reaped as it exits, otherwise a
    // long-lived launcher accumulates `<defunct>` children (and `kill -0` on a
    // zombie still succeeds, so liveness checks misread an exited process as
    // alive). This is the defect the supervise loop's `drain_adopted_orphans`
    // step fixes. The test mirrors that step: a forked "launcher" child becomes
    // a subreaper, spawns an "agent" (its main child) plus a short-lived parent
    // that leaves an "orphan" grandchild; the launcher runs one supervise
    // iteration (the targeted wait for the agent + the orphan drain) and must
    // have reaped the orphan while the agent's exit status is still reported
    // correctly. Everything runs inside forked children so the test runner's
    // own signal state and children are never touched.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_drain_adopted_orphans_reaps_orphan_and_keeps_agent_status() {
        // Allocation-free raw /proc helpers (stack-only). The assertions below
        // run INSIDE a forked child, which must not touch the Rust heap or
        // std's locks (a fork of the multi-threaded test runner can deadlock on
        // an inherited lock) -- the same discipline as init_shim's raw_* helpers.
        const SYS_GETDENTS64: libc::c_long = if cfg!(target_arch = "aarch64") {
            61
        } else if cfg!(target_arch = "riscv64") {
            28
        } else {
            217
        };

        fn raw_sleep_ms(ms: u32) {
            let mut ts = libc::timespec {
                tv_sec: (ms / 1000) as libc::time_t,
                tv_nsec: ((ms % 1000) * 1_000_000) as libc::c_long,
            };
            unsafe {
                libc::nanosleep(&ts as *const _, &mut ts as *mut _);
            }
        }

        /// (ppid, is_zombie) from `/proc/<pid>/stat` via raw syscalls, or None.
        fn stat_ppid_state(pid: i32) -> Option<(i32, bool)> {
            let mut path = [0i8; 32];
            let mut i = 0usize;
            for &c in b"/proc/" {
                path[i] = c as i8;
                i += 1;
            }
            let mut digits = [0i8; 12];
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
            for k in (0..nd).rev() {
                path[i] = digits[k];
                i += 1;
            }
            for &c in b"/stat\0" {
                path[i] = c as i8;
                i += 1;
            }
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
                let close = data.iter().rposition(|&b| b == b')')?;
                let after = &data[close + 1..];
                // After the last ')' the fields are: state ppid pgrp ...
                let mut j = 0usize;
                while j < after.len() && after[j] == b' ' {
                    j += 1;
                }
                let state = after.get(j).copied();
                j += 1;
                while j < after.len() && after[j] == b' ' {
                    j += 1;
                }
                let mut ppid = 0i32;
                let mut found = false;
                while j < after.len() && after[j].is_ascii_digit() {
                    ppid = ppid * 10 + (after[j] - b'0') as i32;
                    found = true;
                    j += 1;
                }
                if !found {
                    return None;
                }
                Some((ppid, state == Some(b'Z')))
            }
        }

        /// Count this process's direct children currently in a zombie state.
        fn zombie_children() -> u32 {
            let mut count = 0u32;
            unsafe {
                let fd = libc::open(c"/proc".as_ptr(), libc::O_RDONLY);
                if fd < 0 {
                    return 0;
                }
                let self_pid = libc::getpid();
                let mut buf = [0u8; 16384];
                loop {
                    let r = libc::syscall(SYS_GETDENTS64, fd, buf.as_mut_ptr(), buf.len());
                    if r <= 0 {
                        break;
                    }
                    let mut off = 0usize;
                    while off + 20usize <= r as usize {
                        let reclen = (buf[off + 16] as usize) | ((buf[off + 17] as usize) << 8);
                        if reclen == 0 || off + reclen > r as usize {
                            break;
                        }
                        let name = &buf[off + 19..reclen];
                        let mut pid = 0i32;
                        let mut nd = 0usize;
                        while nd < name.len() && name[nd].is_ascii_digit() {
                            pid = pid * 10 + (name[nd] - b'0') as i32;
                            nd += 1;
                        }
                        if nd > 0 && pid > 0 {
                            if let Some((ppid, z)) = stat_ppid_state(pid) {
                                if ppid == self_pid && z {
                                    count += 1;
                                }
                            }
                        }
                        off += reclen;
                    }
                }
                libc::close(fd);
            }
            count
        }

        let launcher = unsafe { libc::fork() };
        assert!(launcher >= 0, "fork launcher child");
        if launcher == 0 {
            // INSIDE THE LAUNCHER CHILD: mark as subreaper so orphans of the
            // agent's subtree reparent here, exactly like the real launcher.
            // Die with the test runner: a launcher left behind (panic, timeout kill)
            // must never outlive the test process and hold its output pipe open.
            unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
            if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
                unsafe { libc::_exit(2) };
            }

            // The agent (the launcher's main child, the analogue of child_pid):
            // stays paused until we kill it, so we control when it leaves.
            let agent = unsafe { libc::fork() };
            if agent < 0 {
                unsafe { libc::_exit(3) };
            }
            if agent == 0 {
                // Never outlive the launcher: an agent left paused forever keeps the
                // CI step's output pipe open and the gate never finishes.
                unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
                loop {
                    unsafe { libc::pause() };
                }
            }

            // The orphan factory: a child that leaves one grandchild behind and
            // exits, orphaning the grandchild to this subreaper.
            let mid = unsafe { libc::fork() };
            if mid < 0 {
                unsafe { libc::_exit(3) };
            }
            if mid == 0 {
                let gc = unsafe { libc::fork() };
                if gc < 0 {
                    unsafe { libc::_exit(3) };
                }
                if gc == 0 {
                    // The orphan: exits once reparented to the launcher, where
                    // (without the fix) it would linger as a zombie.
                    unsafe { libc::_exit(9) };
                }
                unsafe { libc::_exit(0) }; // leave the grandchild orphaned
            }

            // Reap `mid` (it exits immediately), which is what triggers the
            // grandchild's re-parenting to this subreaper.
            let mut st = 0;
            if unsafe { libc::waitpid(mid, &mut st, 0) } != mid {
                unsafe { libc::_exit(4) };
            }

            // Wait (bounded) until the adopted orphan is actually a zombie
            // child of this launcher; that is the precondition the supervise
            // step's drain must clear.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while zombie_children() == 0 {
                if std::time::Instant::now() >= deadline {
                    unsafe { libc::_exit(5) };
                }
                raw_sleep_ms(20);
            }

            // Run the supervise step the launcher performs each loop
            // iteration, exactly as the fix wires it: the targeted wait for
            // the agent, then the adopted-orphan drain. The agent has not
            // exited yet, so the drain must reap the orphan (not the agent)
            // and report that the agent is still pending (None).
            let mut status = 0;
            let r = unsafe { libc::waitpid(agent, &mut status, libc::WNOHANG) };
            let drained = if r == 0 {
                drain_adopted_orphans(agent)
            } else {
                None
            };
            if drained.is_some() {
                // The agent exited in the gap -- unexpected for a paused
                // agent; treat it as a failure of the setup, not the fix.
                unsafe { libc::_exit(6) };
            }

            // The orphan must be fully reaped (no zombie child of this launcher
            // is left) and the agent must still be alive.
            let orphan_gone = zombie_children() == 0;
            // `kill(pid, 0)` instead of a /proc path built with `format!`: this runs in
            // a forked child of the multi-threaded test runner, where a heap
            // allocation can deadlock on a malloc lock inherited from another thread.
            let agent_alive = unsafe { libc::kill(agent, 0) } == 0;
            if !orphan_gone || !agent_alive {
                unsafe { libc::_exit(7) };
            }

            // Now signal the agent to exit 7; the next supervise iteration
            // (targeted wait) must reap it and report its status, proving the
            // drain did not consume the agent's exit status.
            unsafe {
                libc::kill(agent, libc::SIGKILL);
            }
            let mut status2 = 0;
            let r2 = unsafe { libc::waitpid(agent, &mut status2, 0) };
            let agent_reaped = r2 == agent;
            let status_ok = libc::WIFSIGNALED(status2) && libc::WTERMSIG(status2) == libc::SIGKILL;
            unsafe { libc::_exit(if agent_reaped && status_ok { 0 } else { 8 }) };
        }

        // INSIDE THE TEST RUNNER: the launcher exits 0 only if it reaped the
        // adopted orphan while still reporting the agent's exit status.
        // Bounded wait: if the forked launcher ever wedges, kill it and fail the
        // test instead of blocking the whole test binary (and the CI gate) forever.
        let mut st = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let r = loop {
            let r = unsafe { libc::waitpid(launcher, &mut st, libc::WNOHANG) };
            if r != 0 {
                break r;
            }
            if std::time::Instant::now() >= deadline {
                unsafe {
                    libc::kill(launcher, libc::SIGKILL);
                    libc::waitpid(launcher, &mut st, 0);
                }
                panic!("launcher child did not finish within 60 s (hung after fork)");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        assert_eq!(r, launcher, "waitpid(launcher)");
        assert_eq!(
            super::super::init_shim::status_to_exit_code(st),
            0,
            "launcher must reap the adopted orphan and still report the agent's exit status"
        );
    }

    // ---- Private /dev/shm ----------------------------------------------------

    #[test]
    fn test_kata_shm_private_opts_content() {
        assert_eq!(
            KATA_SHM_PRIVATE_OPTS, "size=64m,nr_inodes=65536,mode=0700",
            "shm options must match specification"
        );
        assert!(KATA_SHM_PRIVATE_OPTS.contains("size=64m"));
        assert!(KATA_SHM_PRIVATE_OPTS.contains("nr_inodes=65536"));
        assert!(KATA_SHM_PRIVATE_OPTS.contains("mode=0700"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_kata_private_shm_isolation_in_child() {
        // Forks child 1, runs kata_prepare_private_shm, writes a file to /dev/shm,
        // checks statfs (tmpfs and size cap), then exits.
        // Then forks child 2, runs kata_prepare_private_shm, and checks that child 1's
        // file is NOT visible.
        // Skips cleanly if unshare(CLONE_NEWUSER) or mount is not permitted in the environment.

        // Child 1
        let pid1 = unsafe { libc::fork() };
        assert!(pid1 >= 0, "fork child 1 failed");
        if pid1 == 0 {
            if let Err(e) = kata_prepare_private_shm(false) {
                // If unshare(CLONE_NEWUSER) or mount is not permitted in this test environment,
                // skip cleanly with exit code 77.
                eprintln!("kata_prepare_private_shm skipped in test env: {e}");
                unsafe { libc::_exit(77) };
            }

            // Verify statfs
            unsafe {
                let mut buf: libc::statfs = std::mem::zeroed();
                let path = CString::new("/dev/shm").unwrap();
                if libc::statfs(path.as_ptr(), &mut buf) != 0 {
                    libc::_exit(1);
                }
                // TMPFS_MAGIC is 0x01021994
                if buf.f_type != 0x01021994 {
                    libc::_exit(2);
                }
                // size cap: 64 MiB (67108864 bytes)
                let total_bytes = buf.f_blocks as u64 * buf.f_bsize as u64;
                if total_bytes != 64 * 1024 * 1024 {
                    libc::_exit(3);
                }
                // nr_inodes cap: 65536
                if buf.f_files != 65536 {
                    libc::_exit(4);
                }
            }

            // Write test file
            if fs::write("/dev/shm/kata_test_marker.txt", b"child1").is_err() {
                unsafe { libc::_exit(5) };
            }

            unsafe { libc::_exit(0) };
        }

        let mut st1 = 0;
        unsafe {
            libc::waitpid(pid1, &mut st1, 0);
        }
        let code1 = super::super::init_shim::status_to_exit_code(st1);
        if code1 == 77 {
            // Environment does not support unprivileged userns or mount; skip cleanly.
            return;
        }
        assert_eq!(code1, 0, "child 1 private /dev/shm setup failed");

        // Child 2
        let pid2 = unsafe { libc::fork() };
        assert!(pid2 >= 0, "fork child 2 failed");
        if pid2 == 0 {
            if let Err(e) = kata_prepare_private_shm(false) {
                eprintln!("kata_prepare_private_shm child 2 skipped in test env: {e}");
                unsafe { libc::_exit(77) };
            }

            // Check that child 1's marker file is NOT present
            if Path::new("/dev/shm/kata_test_marker.txt").exists() {
                unsafe { libc::_exit(6) };
            }

            unsafe { libc::_exit(0) };
        }

        let mut st2 = 0;
        unsafe {
            libc::waitpid(pid2, &mut st2, 0);
        }
        let code2 = super::super::init_shim::status_to_exit_code(st2);
        assert_eq!(
            code2, 0,
            "child 2 saw child 1's file or failed private /dev/shm setup"
        );
    }
}

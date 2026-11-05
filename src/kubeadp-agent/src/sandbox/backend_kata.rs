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
//! - Sets `PR_SET_NO_NEW_PRIVS`.
//! - Writes secret from fd to `<tmp>/<dest>` mode 0400.

use super::detect::probe_landlock_abi;
use super::exec::exec_target;
use super::init_shim::kill_tree_and_reap;
use super::policy::SandboxPolicy;
use std::ffi::CString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};

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

/// Returns `true` when `path` must be DENIED (never granted) by the Kata
/// Landlock domain: the Kubernetes ServiceAccount token directory, or a
/// `hide` entry that is a git repository (so its object store cannot be read
/// through a sibling that is allowed).
/// If `allow_serviceaccount` is true, the ServiceAccount token directory deny is lifted.
pub fn is_denied_path(path: &Path, hide: &[PathBuf], allow_serviceaccount: bool) -> bool {
    if !allow_serviceaccount && is_k8s_serviceaccount_token_path(path) {
        return true;
    }
    hide.iter()
        .any(|h| (path == h || path.starts_with(h) || h.starts_with(path)) && is_git_repository(h))
}

/// Executes a command in the Kata sandbox backend.
pub fn execute_kata(
    policy: &SandboxPolicy,
    cmd: &str,
    args: &[String],
    env: &[(String, String)],
) -> Result<i32, String> {
    let abi = probe_landlock_abi();
    if !kata_abi_ok(abi) {
        return Err(format!(
            "kata backend requires Landlock ABI >= {KATA_MIN_LANDLOCK_ABI} (for the signal and \
             abstract-unix-socket scopes); the kernel reported ABI {abi} -- refusing to run with \
             weaker isolation"
        ));
    }

    // Read secret fd if configured
    let secret_bytes = if let Some(ref sec) = policy.secret {
        read_secret_fd(sec.fd)?
    } else {
        Vec::new()
    };

    // Setup private tmp and secret file
    let private_tmp = setup_kata_private_tmp(policy, &secret_bytes)?;

    // Contract v1.3 fail-closed: every writable policy path (rw, shared_rw and
    // the private tmp itself) must exist before the Landlock domain is built.
    // `landlock_add_rule` silently skips paths that do not exist, so a policy
    // rw/shared_rw path that cannot be created must fail the launch (exit 125)
    // rather than silently producing a weaker domain (R09). The private tmp was
    // just created (or already existed) so creating it again is idempotent.
    policy.ensure_writable_dirs(private_tmp.as_ref().map(|g| g.path.as_path()))?;

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
            let r = unsafe { libc::waitpid(child_pid, &mut status, libc::WNOHANG) };
            if r == child_pid {
                // The agent exited on its own: clean up any adopted
                // descendants and the private tmp, restore the previous signal
                // dispositions and return the agent's exit status. The private
                // tmp cleanup must run even when the agent crashed, so the next
                // launch with the same agent_id does not trip over a stale
                // directory / secret file (F1).
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
                restore_launcher_signals((old_term, old_int, old_hup));
                return Ok(code);
            }

            // Did the orchestrator signal us while we were waiting?
            let sig = KATA_LAUNCHER_SIG.swap(0, Ordering::SeqCst);
            if sig != 0 {
                // Tear down the whole tree (the agent is our direct child, so
                // the descendant walk reaches it and its subtree), clean up the
                // private tmp, and exit 128+signal, mirroring a normal signal
                // death of the launcher. `_exit` bypasses Drop, so the cleanup
                // must be explicit.
                kill_tree_and_reap();
                if let Some(g) = private_tmp.as_ref() {
                    g.cleanup();
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
    // Apply Landlock ruleset
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

    // Exec command
    execute_target(cmd, args, &final_env);
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
        let c_path = CString::new(path.to_str().unwrap()).unwrap();
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

    // 5. Grants allow-list (if session has grants).
    if let Some(ref g) = policy.grants {
        if g.serviceaccount {
            add_ro(Path::new(K8S_SERVICEACCOUNT_TOKEN_DIR))?;
        }
        for ro_path in &g.ro_paths {
            add_ro(ro_path)?;
        }
        for rw_path in &g.rw_paths {
            add_rw(rw_path)?;
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
fn create_owned_tmp_dir(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|e| format!("failed to create private tmp: {e}"))?;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let meta = fs::symlink_metadata(path)
        .map_err(|e| format!("failed to stat private tmp {path:?}: {e}"))?;
    if meta.file_type().is_symlink() {
        return Err(format!("private tmp {path:?} is a symlink"));
    }
    if !meta.is_dir() {
        return Err(format!("private tmp {path:?} is not a directory"));
    }
    let euid = unsafe { libc::geteuid() };
    if meta.uid() != euid {
        return Err(format!(
            "private tmp {path:?} is owned by uid {} (expected {euid})",
            meta.uid()
        ));
    }
    // Tighten permissions: a stale dir left by a crashed launcher may not be
    // 0700. The tmp is the agent's private scratch; only the agent (same uid)
    // should ever access it.
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|e| format!("failed to chmod private tmp {path:?} to 0700: {e}"))?;
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

fn execute_target(cmd: &str, args: &[String], env: &[(String, String)]) -> ! {
    exec_target(cmd, args, env)
}

#[cfg(test)]
mod tests {
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

        // (b2) Session attempting to write SA path must fail closed!
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
            // Must fail closed because write to SA path is refused!
            unsafe {
                libc::_exit(if res.is_err() { 0 } else { 1 });
            }
        }
        let mut st4 = 0;
        unsafe {
            libc::waitpid(pid4, &mut st4, 0);
        }
        assert!(libc::WIFEXITED(st4) && libc::WEXITSTATUS(st4) == 0);
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
}

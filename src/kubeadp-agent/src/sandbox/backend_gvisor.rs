//! gVisor sandbox backend using unprivileged Linux namespaces.
//!
//! Mechanisms:
//! - unshare(CLONE_NEWUSER): uid/gid map 1000->1000, setgroups deny.
//! - unshare(CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWIPC | CLONE_NEWUTS).
//! - Fresh `/proc` mount.
//! - Full filesystem policy enforcement (the `plan_mounts` / `apply_mount_plan`
//!   pair): a private tmpfs over `/tmp` and `/dev/shm` (so programs that
//!   hard-code `/tmp` cannot share files with other agents), read-only
//!   bind-remounts for every `ro` path, and an empty-tmpfs overmount of every
//!   `hide` dir (policy-driven, including the derived state root, the agent
//!   job-dir root and the bare-repo root) that preserves the agent's own `rw`
//!   sub-paths. A policy `rw` path nested under a `ro` path (e.g. an
//!   `a2a-inbox/<job>/injected` marker dir under a read-only inbox) stays
//!   writable: after every ro bind-remount the plan re-binds each such nested
//!   `rw` path onto itself as its own writable mount, ordered by path depth so
//!   a deeper path is applied after its parent. EVERY mount is fail-closed:
//!   any failure aborts the sandbox with exit code 125.
//! - PID 1 = small init shim (reaps zombies, forwards SIGTERM/SIGINT, propagates exit code, kills descendants).
//! - Sets `PR_SET_NO_NEW_PRIVS`.
//! - Writes secret from fd to `<tmp>/<dest>` mode 0400.
//! - Opt-in `net: relay`: the shim unshares a fresh CLONE_NEWNET (loopback
//!   only) for the agent, bind-mounts the outer proxy's unix socket into the
//!   sandbox and starts the `relay-inner` loopback relay before the agent
//!   process; the launcher runs the outer HTTP CONNECT proxy in the pod's
//!   network namespace (see [`super::relay`]). `net: host` (the default) and
//!   the legacy `shared` alias keep today's behaviour byte-for-byte.

use super::exec::exec_target;
use super::init_shim::{
    arm_pdeathsig, close_fds_above_2, install_shim_signal_handlers, run_init_shim, zeroize_bytes,
};
use super::policy::{GrantsConfig, SandboxPolicy};
use super::relay as relay_lib;
use std::env;
use std::ffi::CString;
use std::fs;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};

// tmpfs mount data (size + mode) for the private scratch mounts. The agent's
// private tmp and `/dev/shm` are owner-only.
const AGENT_TMP_OPTS: &str = "size=64m,mode=0700";
const SHM_PRIVATE_OPTS: &str = "size=64m,mode=0700";

/// Returns one `(what, reason)` pair per path grant this backend cannot apply.
/// The gVisor namespace sandbox enforces the filesystem policy with mounts
/// (private tmp, hide overmounts, ro bind-remounts), not Landlock, so path
/// grants (`serviceaccount`, `ro_paths`, `rw_paths`) are not enforceable and
/// are dropped with a warning instead of aborting the session. Env grants are
/// not listed: `build_agent_env` applies them through `filter_env`.
fn gvisor_grant_warnings(g: &GrantsConfig) -> Vec<(String, String)> {
    const REASON: &str = "the gvisor backend cannot enforce path grants (no Landlock)";
    let mut warnings = Vec::new();
    if g.serviceaccount {
        warnings.push(("serviceaccount".to_string(), REASON.to_string()));
    }
    for p in &g.ro_paths {
        warnings.push((format!("ro {}", p.display()), REASON.to_string()));
    }
    for p in &g.rw_paths {
        warnings.push((format!("rw {}", p.display()), REASON.to_string()));
    }
    warnings
}

/// Executes a command in the gVisor sandbox backend.
/// Returns the exit code or error string.
pub fn execute_gvisor(
    policy: &SandboxPolicy,
    cmd: &str,
    args: &[String],
    env: &[(String, String)],
) -> Result<i32, String> {
    // Grants are optional extras on top of the base policy, and a dropped
    // grant only ever means LESS access: a grant this backend cannot apply
    // must never abort the session (that would crash-loop it over one
    // optional path). Path grants are dropped with one warning each; env
    // grants ARE applied (build_agent_env honours grants.env). The base
    // mount policy below stays fail-closed.
    if let Some(ref g) = policy.grants {
        for (what, reason) in gvisor_grant_warnings(g) {
            eprintln!("kubeadp-agent: sandbox grant dropped ({what}): {reason}");
        }
    }
    // Network mode: fail closed on an unknown value; `relay` opts into the
    // private network namespace + unix-socket relay (see super::relay).
    let relay = match policy.net.as_str() {
        "relay" => true,
        "host" | "shared" => false,
        other => {
            return Err(format!(
                "unknown net value '{other}' (accepted: host, shared, relay)"
            ))
        }
    };
    // 1. Prepare secret if configured: read secret bytes before unsharing if fd is inherited
    let mut secret_bytes = if let Some(ref sec) = policy.secret {
        read_secret_fd(sec.fd)?
    } else {
        Vec::new()
    };

    // 2. Unshare user namespace first (capture outer uid/gid before unsharing)
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    unsafe {
        if libc::unshare(libc::CLONE_NEWUSER) != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("gvisor unshare(CLONE_NEWUSER) failed: {err}"));
        }
    }

    // Write uid/gid maps
    fs::write("/proc/self/setgroups", "deny\n")
        .map_err(|e| format!("write setgroups failed: {e}"))?;
    fs::write("/proc/self/uid_map", format!("{uid} {uid} 1\n"))
        .map_err(|e| format!("write uid_map failed: {e}"))?;
    fs::write("/proc/self/gid_map", format!("{gid} {gid} 1\n"))
        .map_err(|e| format!("write gid_map failed: {e}"))?;

    // 3. Unshare Mount, PID, IPC, UTS namespaces
    unsafe {
        let flags =
            libc::CLONE_NEWNS | libc::CLONE_NEWPID | libc::CLONE_NEWIPC | libc::CLONE_NEWUTS;
        if libc::unshare(flags) != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("gvisor unshare(NS|PID|IPC|UTS) failed: {err}"));
        }
    }

    // 3b. Relay pre-flight, in the launcher process: the private socket dir,
    // the outer proxy (one thread per connection) and the launcher exe path
    // the shim needs. This MUST happen after the unshares above and before
    // the PID 1 fork: the proxy threads would make this process
    // multithreaded, and unshare(CLONE_NEWUSER) fails on a multithreaded
    // process, while the proxy itself must run in the pod's network namespace
    // (which this launcher process keeps -- only the shim below unshares
    // CLONE_NEWNET for the sandbox side).
    let relay_setup = if relay {
        Some(relay_lib::setup_relay_launcher(policy)?)
    } else {
        None
    };

    // 4. Fork child process which will be PID 1 in the new PID namespace
    let pid1 = unsafe { libc::fork() };
    if pid1 < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("gvisor fork PID 1 failed: {err}"));
    }

    if pid1 > 0 {
        // Parent outside PID ns: wait for PID 1 to exit with signal forwarding.
        let code = super::init_shim::wait_with_signal_forwarding(pid1);
        // Remove the relay socket dir (best-effort): a crashed launcher leaves
        // a stale dir behind, which the next launch reuses (owner-checked).
        if let Some(setup) = &relay_setup {
            let _ = fs::remove_dir_all(&setup.dir);
        }
        return Ok(code);
    }

    // --- INSIDE PID 1 of the new PID namespace (the shim) ---
    // The shim is a fork of the launcher, so it carries the launcher's full
    // (unfiltered) environment and the secret bytes on its heap. Because the
    // agent is a child of PID 1 in the same user namespace (same uid), the
    // agent could otherwise read /proc/1/environ (bypassing env_allow) or
    // /proc/1/mem / ptrace to recover the launcher environment and secret.
    // Harden PID 1 before anything agent-related runs:
    //
    // 1. If the launcher (our parent, outside the namespace) dies abnormally,
    //    take the whole sandbox down with us.
    //
    // Install the SIGTERM/SIGINT handlers FIRST, before any setup work or
    // entry into run_init_shim: as PID 1 of a fresh PID namespace the kernel
    // drops a default-disposition signal, so a SIGTERM forwarded by the
    // launcher before the handler exists would be consumed and never reach the
    // agent (R08.sigterm_launcher). The re-install inside run_init_shim is a
    // harmless no-op with the same handler.
    install_shim_signal_handlers();
    arm_pdeathsig(libc::SIGKILL, unsafe { libc::getppid() });

    // 2. Stop /proc/1/{mem,environ} and ptrace from exposing us to same-uid
    //    children. (The fresh /proc mount + non-root dumpable already blocks
    //    most paths; this makes it explicit.)
    unsafe {
        libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0);
    }

    // 3. Drop the launcher's environment: the shim keeps nothing, and the agent
    //    receives ONLY the env_allow-filtered set (built explicitly below), so
    //    no inherited variable can leak into the sandbox. Remember the shim's
    //    own (in-namespace) pid: it is PID 1 of the namespace and the parent of
    //    the agent, which the agent's death-signal re-check needs.
    let shim_pid = unsafe { libc::getpid() };
    unsafe {
        libc::clearenv();
    }

    // Relay mode: this shim (and everything forked from it -- the relay-inner
    // and the agent) gets its OWN network namespace, loopback only. The
    // unshare must happen here, in the forked shim: the launcher (our parent)
    // keeps the pod's network namespace and runs the outer proxy there. The
    // namespace is owned by the user namespace unshared above, where this
    // process holds the capabilities, so the loopback interface can be
    // brought up afterwards.
    if relay {
        unsafe {
            if libc::unshare(libc::CLONE_NEWNET) != 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("kubeadp-agent relay: unshare(CLONE_NEWNET) failed: {err}");
                libc::_exit(125);
            }
        }
        if let Err(e) = relay_lib::bring_loopback_up() {
            eprintln!("kubeadp-agent sandbox setup error: {e}");
            unsafe { libc::_exit(125) };
        }
    }

    // Ensure every writable policy path (rw / shared_rw / the private tmp
    // itself) exists before the mount plan is built and applied: the hide
    // overmounts preserve these paths by bind-back, so a missing writable dir
    // would silently vanish behind the hide and the agent would lose its own
    // rw / shared_rw access (R09: uncreatable rw must fail closed, exit 125).
    let private_tmp_hint = policy.private_tmp_path();
    if let Err(e) = policy.ensure_writable_dirs(private_tmp_hint.as_deref()) {
        eprintln!("kubeadp-agent sandbox setup error: {e}");
        unsafe { libc::_exit(125) };
    }

    // Perform mount setup: the full filesystem policy (private /tmp + /dev/shm,
    // ro binds, hide overmounts incl. the derived state/jobs/repos roots).
    // Any failure aborts the launch (exit 125) -- isolation is never partial.
    let spec = MountSpec::from_policy(policy.clone());
    let (plan, private_tmp) = plan_mounts(&spec);
    if let Err(e) = apply_mount_plan(&plan) {
        eprintln!("kubeadp-agent sandbox setup error: {e}");
        unsafe { libc::_exit(125) };
    }

    // Relay mode: bind-mount the outer proxy's unix socket into the sandbox,
    // under the agent's private tmpfs (fresh per sandbox, so the bind target
    // is created without touching the pod's shared filesystem). Fails closed.
    let relay_socket_target = if let (true, Some(setup)) = (relay, &relay_setup) {
        match private_tmp.as_ref() {
            Some(tmp) => {
                let target = relay_lib::relay_socket_target(tmp);
                if let Err(e) = fs::write(&target, b"") {
                    eprintln!("kubeadp-agent relay: cannot create socket bind target: {e}");
                    unsafe { libc::_exit(125) };
                }
                if let Err(e) = apply_mount_plan(&relay_mount_plan(&setup.sock, &target)) {
                    eprintln!("kubeadp-agent sandbox setup error: {e}");
                    unsafe { libc::_exit(125) };
                }
                Some(target)
            }
            None => {
                // The bind target lives under the private tmp; without one
                // there is no in-sandbox path for the outer socket.
                eprintln!(
                    "kubeadp-agent relay: net=relay requires a private tmp to hold the relay socket"
                );
                unsafe { libc::_exit(125) };
            }
        }
    } else {
        None
    };

    // Set PR_SET_NO_NEW_PRIVS
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            eprintln!("kubeadp-agent: prctl(PR_SET_NO_NEW_PRIVS) failed");
            libc::_exit(125);
        }
    }

    // Write the secret (if configured) into the (now private) tmp as a 0400
    // file. Fails closed: a secret that cannot be placed is a launch failure.
    if let Err(e) = write_secret(&private_tmp, &spec.policy, &secret_bytes) {
        eprintln!("kubeadp-agent private tmp secret setup error: {e}");
        unsafe { libc::_exit(125) };
    }

    // The secret has now been durably written to its 0400 destination file.
    // Wipe the in-memory copy so it cannot be recovered from the shim's heap
    // via /proc/1/mem / ptrace by a same-uid child.
    zeroize_bytes(&mut secret_bytes);

    // Close every fd > 2 (the secret fd and all other launcher fds) so the
    // shim -- and the agent forked from it -- inherit none of them.
    close_fds_above_2();

    // Relay mode: start the inner relay (a subcommand of this binary) before
    // the agent process. It listens on 127.0.0.1:3128 in the agent's network
    // namespace and copies each TCP connection to the bind-mounted outer
    // proxy socket. It dies with the namespace: the shim's tree-kill reaps it
    // when the agent exits.
    if let (true, Some(setup), Some(target)) = (relay, &relay_setup, &relay_socket_target) {
        let argv = relay_lib::relay_inner_argv(&setup.exe, target);
        let inner_env = vec![("PATH".to_string(), super::exec::default_path().to_string())];
        let relay_pid = unsafe { libc::fork() };
        if relay_pid < 0 {
            eprintln!("kubeadp-agent relay: fork relay-inner failed");
            unsafe { libc::_exit(125) };
        }
        if relay_pid == 0 {
            // --- INSIDE RELAY-INNER ---
            // If the shim (our parent, PID 1) dies, take ourselves down.
            arm_pdeathsig(libc::SIGKILL, shim_pid);
            execute_target(&argv[0], &argv[1..], &inner_env);
        }
    }

    // Fork the actual agent root process
    let agent_pid = unsafe { libc::fork() };
    if agent_pid < 0 {
        eprintln!("kubeadp-agent failed to fork agent process");
        unsafe { libc::_exit(125) };
    }

    if agent_pid == 0 {
        // --- INSIDE AGENT ROOT PROCESS ---
        // If the shim (our parent, PID 1 of the namespace) dies, take ourselves
        // down too: close the race by re-checking the parent after the prctl.
        arm_pdeathsig(libc::SIGKILL, shim_pid);
        // The shim's environment was already cleared and its fds closed; drop
        // any inherited fds once more here (belt and suspenders for the exec).
        close_fds_above_2();

        // The agent receives EXACTLY the env_allow-filtered variables (the shim
        // environment is empty), so nothing inherited can leak through. With
        // `net: relay` the loopback proxy env is injected on top (a no-op for
        // the host mode, which keeps the filtered set byte-for-byte).
        let final_env = relay_lib::apply_relay_env(
            policy,
            build_agent_env(policy, env, private_tmp.as_deref()),
            relay_setup.as_ref().map(|s| s.exe.as_path()),
        );
        // Exec the agent command
        execute_target(cmd, args, &final_env);
    }

    // PID 1 runs the init shim: reaps zombies, forwards signals, kills namespace on exit
    let exit_code = run_init_shim(agent_pid);
    unsafe { libc::_exit(exit_code) };
}

/// Pure relay mount plan: bind-mount the outer proxy's unix socket onto the
/// in-sandbox target path (under the agent's private tmpfs). Applied by the
/// shim after the main mount plan; any failure aborts the launch (exit 125).
pub fn relay_mount_plan(outer_socket: &Path, target: &Path) -> Vec<MountOp> {
    vec![MountOp::BindRec {
        src: outer_socket.to_path_buf(),
        target: target.to_path_buf(),
    }]
}

/// Pure delivery-socket mount plan (SPEC §6.6 / §6.9): bind-mount the per-
/// agent delivery socket the launcher bound before the fork onto its in-
/// sandbox target under the private tmp (next to the relay socket), exactly
/// like the relay socket. Applied by the shim after the main mount plan; any
/// failure aborts the launch (exit 125). A `host:target` pair is computed
/// from the policy by the a2a launcher; C5 wires it here.
pub fn delivery_socket_mount_plan(host_sock: &Path, target: &Path) -> Vec<MountOp> {
    vec![MountOp::BindRec {
        src: host_sock.to_path_buf(),
        target: target.to_path_buf(),
    }]
}

/// Pure environment builder for the gVisor agent root process.
///
/// Returns EXACTLY the `env_allow`-filtered variables, with `TMPDIR` forced to
/// the private tmp when one is configured. This is the only environment the
/// agent may see: the shim's own environment is cleared, so nothing inherited
/// can leak into the sandbox. Factored out (pure, no side effects) so the
/// filtering behaviour can be unit-tested without a live namespace.
fn build_agent_env(
    policy: &SandboxPolicy,
    env: &[(String, String)],
    private_tmp: Option<&std::path::Path>,
) -> Vec<(String, String)> {
    let mut final_env: Vec<(String, String)> =
        policy.filter_env(env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    if let Some(tmp_path) = private_tmp {
        final_env.retain(|(k, _)| k != "TMPDIR");
        final_env.push(("TMPDIR".to_string(), tmp_path.to_string_lossy().to_string()));
    }
    final_env
}

fn read_secret_fd(fd: i32) -> Result<Vec<u8>, String> {
    use std::io::Read;

    // Duplicate fd so we don't accidentally close the original during read
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

// --- Mount planning (pure) + execution (fail-closed) -----------------------
//
// The full filesystem policy of the gVisor sandbox is expressed as an ordered
// list of mount operations computed by the pure function [`plan_mounts`] (no
// I/O, fully unit-testable), then applied by [`apply_mount_plan`] which fails
// closed on the FIRST error so the sandbox never comes up with a weaker-than-
// policy mount set (isolation is always applied; a partial setup is a launch
// failure, exit 125).

/// A single mount operation in the plan, in execution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountOp {
    /// Make the root mount tree private so our mounts do not propagate out.
    RootPrivate,
    /// Mount a fresh procfs over `/proc` (only PIDs in our namespace are visible).
    Proc,
    /// Mount a private tmpfs over `target` (optionally capped with `size`).
    Tmpfs {
        target: PathBuf,
        size: Option<String>,
    },
    /// Bind-mount `src` onto `target`, recursively.
    BindRec { src: PathBuf, target: PathBuf },
    /// Remount the filesystem at `target` read-only (must follow a bind).
    RemountRo { target: PathBuf },
    /// Rebinds `target` (a policy `rw` path nested under a `ro` path) onto
    /// itself as its own writable mount and remounts it read-write, so the
    /// agent keeps write access to it even though a `ro` ancestor was
    /// remounted read-only. `nosuid`/`nodev` are preserved by the remount so
    /// the re-opened mount is never less restricted than its parent.
    /// (Must follow the `ro` remounts of every ancestor, so a deeper path is
    /// always applied after its parent.)
    RemountRw { target: PathBuf },
    /// Bind-mount the real `src` into a staging mountpoint `stage` (used to
    /// preserve an `rw` path that lives inside a hidden directory).
    PreserveBind { src: PathBuf, stage: PathBuf },
    /// Hide `target`'s real contents from the agent. When `target` is a
    /// directory (or does not exist yet) an empty tmpfs is mounted over it,
    /// then for each preserved entry `hide/rel` is recreated and the staging
    /// mount bound back so the agent keeps access to its own paths. When
    /// `target` is a regular file (e.g. the operator token file) the empty
    /// tmpfs cannot be mounted over it (a mountpoint must be a directory), so
    /// `/dev/null` is bind-mounted over the file instead; a file target must
    /// then carry no `preserved` entries (fail closed).
    Hide {
        target: PathBuf,
        preserved: Vec<Preserved>,
    },
    /// Detach (unmount) a staging mount that is no longer needed.
    Umount { target: PathBuf },
}

/// A policy `rw` path that lives inside a hidden directory and must stay
/// visible through the empty-tmpfs overmount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preserved {
    /// Real path (on the original mount tree) to bind from.
    pub src: PathBuf,
    /// Path, relative to the hidden directory, at which to re-expose it.
    pub rel: PathBuf,
    /// Staging mountpoint holding the `src` bind while the hide is built.
    pub stage: PathBuf,
}

/// Everything [`plan_mounts`] needs to compute the mount plan for one sandbox.
///
/// The `*_root` fields carry the derived agent roots that the launcher knows
/// from its (the agent's own) configuration. In production they are resolved
/// the same way the worker resolves them (`AGENT_RUN_*` env > defaults), so
/// the policy file itself never has to name them; in unit tests they are set
/// explicitly so the plan is fully hermetic.
#[derive(Debug, Clone)]
pub struct MountSpec {
    pub policy: SandboxPolicy,
    /// Agent state root (`~/.agent-run`), overmounted so other agents' state
    /// is hidden while this agent's own paths remain visible.
    pub state_root: PathBuf,
    /// Agent job-dir root (`<workspace>/.agent-jobs`), the root that owns this
    /// agent's `rw` worktree and every other agent's worktrees.
    pub jobs_root: PathBuf,
    /// Bare-repo root (`<workspace>/.agent-repos`).
    pub repos_root: PathBuf,
}

impl MountSpec {
    /// Builds a spec using the worker's own root resolution for the derived
    /// roots (env overrides, then the built-in defaults). Used by the backend
    /// at launch time.
    pub fn from_policy(policy: SandboxPolicy) -> Self {
        let state_root = env_path(
            "AGENT_RUN_STATE_ROOT",
            crate::core::config::DEFAULT_STATE_ROOT,
        );
        let jobs_root = env_path(
            "AGENT_RUN_JOBS_DIR_NAME",
            crate::core::config::DEFAULT_JOBS_DIR_NAME,
        );
        let repos_root = env_path(
            "AGENT_RUN_REPOS_ROOT",
            crate::core::config::DEFAULT_REPOS_ROOT_TEMPLATE,
        );
        Self {
            policy,
            state_root,
            jobs_root,
            repos_root,
        }
    }
}

/// Reads an env var (as a path template); falls back to `fallback` when the
/// variable is unset or empty, then expands `{workspace_root}` and `~` using
/// the workspace root. The workspace root itself is resolved from
/// `AGENT_RUN_WORKSPACE_ROOT` (or its built-in default).
fn env_path(var: &str, fallback: &str) -> PathBuf {
    let ws = env::var("AGENT_RUN_WORKSPACE_ROOT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| crate::core::config::DEFAULT_WORKSPACE_ROOT.to_string());
    let workspace_root = crate::core::config::expand_path(&ws, Path::new("/"));
    let raw = env::var(var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let template = raw.unwrap_or_else(|| fallback.to_string());
    crate::core::config::expand_path(&template, &workspace_root)
}

/// Computes the ordered mount plan for the sandbox. Pure: no syscalls, no
/// allocation of shared state, no clock reads -- so it can be unit-tested
/// hermetically. Returns the plan plus the private tmp path (used by the
/// secret writer and the agent `TMPDIR`).
pub fn plan_mounts(spec: &MountSpec) -> (Vec<MountOp>, Option<PathBuf>) {
    let private_tmp = spec.policy.private_tmp_path();
    let mut plan = Vec::new();
    let norm = |p: &Path| spec.policy.normalize_policy_path(p, private_tmp.as_deref());

    // 1. Root private + fresh /proc: the isolation baseline.
    plan.push(MountOp::RootPrivate);
    plan.push(MountOp::Proc);

    // 2. The agent's private tmp as a tmpfs (if it is not /tmp itself) so the
    //    agent's scratch is isolated from other agents. A fresh tmpfs over
    //    /tmp itself is deliberately NOT mounted: it would wipe every existing
    //    /tmp path (this agent's own rw worktree, the state root and the
    //    launched binary when they live under /tmp), and the preserve/hide
    //    steps that need those paths still present would then fail closed.
    //    /tmp keeps its real contents, which the ro/hide policy already hides
    //    as needed; only the agent's own private tmp is overmounted (owner-only).
    if let Some(pt) = private_tmp.as_ref() {
        if pt != Path::new("/tmp") {
            plan.push(MountOp::Tmpfs {
                target: pt.clone(),
                size: Some(AGENT_TMP_OPTS.to_string()),
            });
        }
    }

    // 3. Private /dev/shm (a failure here is a launch failure, not ignored).
    plan.push(MountOp::Tmpfs {
        target: PathBuf::from("/dev/shm"),
        size: Some(SHM_PRIVATE_OPTS.to_string()),
    });

    // 4. Read-only enforcement: bind each `ro` path, then remount it ro. A ro
    //    path that is itself (or an ancestor of) one of the agent's rw paths is
    //    skipped: remounting it read-only would revoke the write access the rw
    //    path needs (a rw worktree cannot live under a read-only remount), so
    //    enforcing ro there would defeat the rw. In production no default ro
    //    path is an ancestor of a worktree, so this only fires in the tests,
    //    where the launched binary and the worktree share a scratch dir.
    for ro in &spec.policy.ro {
        let p = norm(ro);
        let shadows_rw = spec.policy.rw.iter().any(|rw| {
            let r = norm(rw);
            r == p || r.starts_with(&p)
        });
        if shadows_rw {
            continue;
        }
        plan.push(MountOp::BindRec {
            src: p.clone(),
            target: p.clone(),
        });
        plan.push(MountOp::RemountRo { target: p });
    }

    // 5. Hide: policy hide + derived roots (state root, jobs root, repos root).
    let mut hide_targets: Vec<PathBuf> = spec.policy.hide.clone();
    for cand in [&spec.state_root, &spec.jobs_root, &spec.repos_root] {
        if !hide_targets.iter().any(|h| h == cand) {
            hide_targets.push(cand.clone());
        }
    }
    for hide in &hide_targets {
        let hide_real = norm(hide);
        // If ANY path the agent must keep (rw, shared_rw, or ro) equals the
        // hide target itself, the whole hide is skipped: overmounting it with
        // an empty tmpfs would remove the agent's own access to that path.
        let equals_hide = spec
            .policy
            .rw
            .iter()
            .chain(spec.policy.shared_rw.iter())
            .chain(spec.policy.ro.iter())
            .any(|p| norm(p) == hide_real);
        if equals_hide {
            continue;
        }
        // Paths under the hide that must stay visible through the empty-tmpfs
        // overmount: the agent's own rw paths (write access) plus its shared_rw
        // paths (writable by design) and its ro paths (readable by design).
        // Without the shared_rw/ro preservation, a shared or read-only path
        // living under a hidden parent would be wiped for this agent (F4).
        let mut preserved: Vec<Preserved> = Vec::new();
        let mut all: Vec<(usize, &PathBuf)> = spec
            .policy
            .rw
            .iter()
            .enumerate()
            .chain(
                spec.policy
                    .shared_rw
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (spec.policy.rw.len() + i, p)),
            )
            .chain(
                spec.policy
                    .ro
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (spec.policy.rw.len() + spec.policy.shared_rw.len() + i, p)),
            )
            .collect();
        // Stable order (rw first, then shared_rw, then ro). Preserve paths may
        // repeat across the lists; dedupe by source path keeping first.
        all.sort_by_key(|(idx, _)| *idx);
        let mut seen = std::collections::HashSet::new();
        for (idx, p) in all {
            let real = norm(p);
            if let Ok(rel) = real.strip_prefix(&hide_real) {
                if !rel.as_os_str().is_empty() && seen.insert(real.clone()) {
                    preserved.push(Preserved {
                        src: real.clone(),
                        rel: rel.to_path_buf(),
                        stage: staging_path(idx),
                    });
                }
            }
        }
        if !preserved.is_empty() {
            for pre in &preserved {
                plan.push(MountOp::PreserveBind {
                    src: pre.src.clone(),
                    stage: pre.stage.clone(),
                });
            }
        }
        plan.push(MountOp::Hide {
            target: hide.clone(),
            preserved: preserved.clone(),
        });
        // ro paths that were preserved through the hide must be re-mounted
        // read-only after the bind-back so the agent cannot write them.
        let ro_sources: std::collections::HashSet<PathBuf> =
            spec.policy.ro.iter().map(|p| norm(p)).collect();
        for pre in &preserved {
            if ro_sources.contains(&pre.src) {
                plan.push(MountOp::RemountRo {
                    target: hide.join(&pre.rel),
                });
            }
        }
        for pre in &preserved {
            plan.push(MountOp::Umount {
                target: pre.stage.clone(),
            });
        }
    }

    // 6. Writable-path rescue: a policy `rw` path nested under a `ro` path
    //    (e.g. `.../a2a-inbox/<job>/injected` under a read-only inbox dir)
    //    ends up inside a read-only bind-remount -- either the section-4
    //    self-bind of an unrelated `ro` path or the section-5 hide-preservation
    //    remount of an `ro` source that shadows it. The rw declaration is
    //    explicit policy, so the nested path must stay WRITABLE: rebind it
    //    onto itself as its own mount and remount it without MS_RDONLY
    //    (keeping nosuid/nodev). Applied shallowest-first so a deeper path is
    //    mounted on top of its re-opened parent. A rw path not under any ro
    //    path needs no extra mount (nothing read-only was applied to it).
    let nested_rw: Vec<PathBuf> = {
        let norm = |p: &Path| spec.policy.normalize_policy_path(p, private_tmp.as_deref());
        let ro_roots: Vec<PathBuf> = spec.policy.ro.iter().map(|p| norm(p)).collect();
        let mut out: Vec<PathBuf> = Vec::new();
        for rw in spec.policy.rw.iter() {
            let r = norm(rw);
            let under_ro = ro_roots.iter().any(|ro| r != *ro && r.starts_with(ro));
            if under_ro && !out.contains(&r) {
                out.push(r);
            }
        }
        // Shallowest first: the parent's writable bind must precede any mount
        // under it, so the deeper bind lands on top of the re-opened parent.
        out.sort_by_key(|p| p.components().count());
        out
    };
    for rw in &nested_rw {
        plan.push(MountOp::RemountRw { target: rw.clone() });
    }

    (plan, private_tmp)
}

/// A unique, per-process staging mountpoint under the (still-original) `/tmp`
/// used to hold `rw` binds while a hide overmount is being built.
fn staging_path(idx: usize) -> PathBuf {
    PathBuf::from(format!("/tmp/.kubeadp_staging_{}", std::process::id())).join(idx.to_string())
}

fn cstr(p: &Path) -> Result<CString, String> {
    CString::new(
        p.to_str()
            .ok_or_else(|| format!("non-UTF-8 path: {:?}", p))?,
    )
    .map_err(|e| format!("NUL byte in path {:?}: {e}", p))
}

/// Decides, at apply time, how a `hide` target is to be hidden. Returns
/// `true` when `target` exists on the current mount tree and is NOT a
/// directory (a regular file, the case of the operator token file behind
/// `KUBEADP_API_TOKEN_FILE`): a mountpoint must be a directory, so such a
/// target cannot be overmounted with an empty tmpfs and is instead hidden by
/// bind-mounting `/dev/null` over it. `false` for a directory or a target that
/// does not exist (the current empty-tmpfs behaviour applies). Uses
/// `symlink_metadata` so a symlinked dir is treated as a directory, never
/// followed.
fn hide_is_file(target: &Path) -> bool {
    match std::fs::symlink_metadata(target) {
        Ok(m) => !m.is_dir(),
        Err(_) => false,
    }
}

/// Applies the mount plan in order. Returns `Err` on the first failure so the
/// caller aborts the launch (exit 125) instead of running with a weaker-than-
/// policy mount set.
pub fn apply_mount_plan(plan: &[MountOp]) -> Result<(), String> {
    for op in plan {
        apply_mount_op(op)?;
    }
    Ok(())
}

/// Applies a single mount operation, failing closed on the first error.
fn apply_mount_op(op: &MountOp) -> Result<(), String> {
    match op {
        MountOp::RootPrivate => unsafe {
            if libc::mount(
                c"none".as_ptr(),
                c"/".as_ptr(),
                std::ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                std::ptr::null(),
            ) != 0
            {
                return Err(format!(
                    "mount(/, MS_PRIVATE) failed: {}",
                    std::io::Error::last_os_error()
                ));
            }
        },
        MountOp::Proc => unsafe {
            if libc::mount(
                c"proc".as_ptr(),
                c"/proc".as_ptr(),
                c"proc".as_ptr(),
                0,
                std::ptr::null(),
            ) != 0
            {
                return Err(format!(
                    "mount fresh /proc failed: {}",
                    std::io::Error::last_os_error()
                ));
            }
        },
        MountOp::Tmpfs { target, size } => {
            // Ensure the mountpoint exists on the current tree before
            // mounting (idempotent; the target is created on whatever the
            // parent mount currently is, so the ordering is robust).
            fs::create_dir_all(target)
                .map_err(|e| format!("prepare tmpfs target {target:?}: {e}"))?;
            let target_c = cstr(target)?;
            let data = match size {
                Some(s) => CString::new(s.clone()).map_err(|e| e.to_string())?,
                None => CString::new("").map_err(|e| e.to_string())?,
            };
            unsafe {
                if libc::mount(
                    c"tmpfs".as_ptr(),
                    target_c.as_ptr(),
                    c"tmpfs".as_ptr(),
                    0,
                    data.as_ptr() as *const libc::c_void,
                ) != 0
                {
                    return Err(format!(
                        "mount tmpfs over {} failed: {}",
                        target.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
        MountOp::BindRec { src, target } => {
            let src_c = cstr(src)?;
            let target_c = cstr(target)?;
            unsafe {
                if libc::mount(
                    src_c.as_ptr(),
                    target_c.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND | libc::MS_REC,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(format!(
                        "bind mount {} -> {} failed: {}",
                        src.display(),
                        target.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
        MountOp::RemountRo { target } => {
            let target_c = cstr(target)?;
            unsafe {
                if libc::mount(
                    std::ptr::null(),
                    target_c.as_ptr(),
                    std::ptr::null(),
                    libc::MS_REMOUNT | libc::MS_RDONLY,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(format!(
                        "remount read-only {} failed: {}",
                        target.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
        MountOp::RemountRw { target } => {
            // Ensure the mountpoint exists on the current (already remounted)
            // tree: the writable dirs were created by `ensure_writable_dirs`
            // before the plan ran, but on a path under a ro/hide ancestor the
            // overmount may hide a pre-existing dir -- re-creating it on the
            // ro tree is a no-op that never leaks (and a failure must fail
            // closed, matching every other mount).
            fs::create_dir_all(target)
                .map_err(|e| format!("prepare writable mount target {target:?}: {e}"))?;
            let target_c = cstr(target)?;
            // Rebind the path onto itself as its own mount so the read/write
            // remount below only ever touches this mount, never the (ro)
            // ancestor's filesystem.
            unsafe {
                if libc::mount(
                    target_c.as_ptr(),
                    target_c.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND | libc::MS_REC,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(format!(
                        "bind {} onto itself failed: {}",
                        target.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
            // Remount without MS_RDONLY to re-open the write path, keeping the
            // bind's nosuid/nodev so a writable nested path is never less
            // restricted than its ro parent (fail closed on any error).
            let data = CString::new("nosuid,nodev").map_err(|e| e.to_string())?;
            unsafe {
                if libc::mount(
                    std::ptr::null(),
                    target_c.as_ptr(),
                    std::ptr::null(),
                    libc::MS_REMOUNT | libc::MS_NOSUID | libc::MS_NODEV,
                    data.as_ptr() as *const libc::c_void,
                ) != 0
                {
                    return Err(format!(
                        "remount read-write {} failed: {}",
                        target.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
        MountOp::PreserveBind { src, stage } => {
            fs::create_dir_all(stage).map_err(|e| format!("create stage dir {stage:?}: {e}"))?;
            let src_c = cstr(src)?;
            let stage_c = cstr(stage)?;
            unsafe {
                if libc::mount(
                    src_c.as_ptr(),
                    stage_c.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND | libc::MS_REC,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(format!(
                        "preserve bind {} -> {} failed: {}",
                        src.display(),
                        stage.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
        MountOp::Hide { target, preserved } => {
            // A hide target that exists as a regular file (the operator token
            // file behind KUBEADP_API_TOKEN_FILE) cannot be overmounted with
            // an empty tmpfs: a mountpoint must be a directory, and
            // `fs::create_dir_all` on an existing file fails with EEXIST.
            // Hide it instead by bind-mounting /dev/null over it, which makes
            // any read of the file return EOF. Fail closed: the bind must
            // succeed or the launch is refused.
            if hide_is_file(target) {
                if !preserved.is_empty() {
                    return Err(format!(
                        "hide target {target:?} is a file but the plan preserves {preserved:?} \
                         sub-paths; a file cannot preserve children (fail closed)"
                    ));
                }
                let target_c = cstr(target)?;
                unsafe {
                    if libc::mount(
                        c"/dev/null".as_ptr(),
                        target_c.as_ptr(),
                        std::ptr::null(),
                        libc::MS_BIND,
                        std::ptr::null(),
                    ) != 0
                    {
                        return Err(format!(
                            "hide file {target:?}: bind /dev/null over it failed: {}",
                            std::io::Error::last_os_error()
                        ));
                    }
                }
                return Ok(());
            }
            let target_c = cstr(target)?;
            // Mount an empty tmpfs over the hidden dir, hiding its real
            // contents. (If the hide dir does not exist, `fs::create_dir_all`
            // on the original tree makes it so the mount has a target; the
            // mount then hides an empty dir, which is exactly what we want
            // -- we never silently skip a hide.)
            fs::create_dir_all(target).map_err(|e| format!("prepare hide dir {target:?}: {e}"))?;
            unsafe {
                if libc::mount(
                    c"tmpfs".as_ptr(),
                    target_c.as_ptr(),
                    c"tmpfs".as_ptr(),
                    0,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(format!(
                        "mount empty tmpfs over {target:?} failed: {}",
                        std::io::Error::last_os_error()
                    ));
                }
            }
            for pre in preserved {
                let dest = target.join(&pre.rel);
                fs::create_dir_all(&dest).map_err(|e| format!("recreate {dest:?}: {e}"))?;
                let stage_c = cstr(&pre.stage)?;
                let dest_c = cstr(&dest)?;
                unsafe {
                    if libc::mount(
                        stage_c.as_ptr(),
                        dest_c.as_ptr(),
                        std::ptr::null(),
                        libc::MS_BIND | libc::MS_REC,
                        std::ptr::null(),
                    ) != 0
                    {
                        return Err(format!(
                            "bind back preserved {dest:?} failed: {}",
                            std::io::Error::last_os_error()
                        ));
                    }
                }
            }
        }
        MountOp::Umount { target } => {
            let target_c = cstr(target)?;
            unsafe {
                if libc::umount2(target_c.as_ptr(), libc::MNT_DETACH) != 0 {
                    return Err(format!(
                        "umount staging {target:?} failed: {}",
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Writes the secret (already read from its fd) into the private tmp as a
/// 0400 file, after the private tmp has been mounted. Fails closed if no
/// private tmp is configured, if the destination already exists, or if the
/// write fails -- a secret that cannot be placed must never let the launch
/// proceed.
fn write_secret(
    private_tmp: &Option<PathBuf>,
    policy: &SandboxPolicy,
    secret_bytes: &[u8],
) -> Result<(), String> {
    let Some(sec) = &policy.secret else {
        return Ok(());
    };
    let Some(tmp_path) = private_tmp else {
        return Err(
            "policy configures a secret but no private tmp is available to hold it".to_string(),
        );
    };
    use std::io::Write;
    let dest_path = tmp_path.join(&sec.dest);
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create secret parent dir: {e}"))?;
    }
    let dest_c = CString::new(dest_path.to_str().unwrap()).unwrap();
    let fd = unsafe {
        libc::open(
            dest_c.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC,
            0o400,
        )
    };
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!(
            "failed to create secret file {dest_path:?} with O_EXCL mode 0400: {err}"
        ));
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.write_all(secret_bytes)
        .map_err(|e| format!("failed to write secret to {dest_path:?}: {e}"))?;
    Ok(())
}

fn execute_target(cmd: &str, args: &[String], env: &[(String, String)]) -> ! {
    exec_target(cmd, args, env)
}

#[cfg(test)]
mod tests {
    use super::super::init_shim::status_to_exit_code;
    use super::super::policy::SandboxPolicy;
    use super::{
        build_agent_env, delivery_socket_mount_plan, execute_gvisor, gvisor_grant_warnings,
        hide_is_file, plan_mounts, relay_mount_plan, MountOp, MountSpec, AGENT_TMP_OPTS,
        SHM_PRIVATE_OPTS,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_build_agent_env_is_exactly_the_filtered_set() {
        let policy = SandboxPolicy {
            env_allow: vec![
                "PATH".to_string(),
                "HOME".to_string(),
                "A2A_*".to_string(),
                "KUBEADP_*".to_string(),
            ],
            ..Default::default()
        };
        // A launcher environment full of sensitive variables the sandbox must
        // NOT receive (token, secret, etc.), mixed with the allowed ones.
        let launcher_env = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("HOME".to_string(), "/home/agent".to_string()),
            ("A2A_BASE".to_string(), "http://a2a".to_string()),
            ("KUBEADP_ROLE".to_string(), "worker".to_string()),
            ("VIKUNJA_API_TOKEN".to_string(), "supersecret".to_string()),
            ("SECRET_BYTES".to_string(), "abc".to_string()),
        ];
        let out = build_agent_env(&policy, &launcher_env, None);
        let got: BTreeMap<&str, &str> = out.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let want: BTreeMap<&str, &str> = [
            ("PATH", "/usr/bin"),
            ("HOME", "/home/agent"),
            ("A2A_BASE", "http://a2a"),
            ("KUBEADP_ROLE", "worker"),
        ]
        .iter()
        .copied()
        .collect();
        assert_eq!(
            got, want,
            "agent env must be exactly the env_allow-filtered set"
        );
        assert!(
            !out.iter().any(|(k, _)| k == "VIKUNJA_API_TOKEN"),
            "non-allowed variable leaked into agent env"
        );
        assert!(
            !out.iter().any(|(k, _)| k == "SECRET_BYTES"),
            "secret variable leaked into agent env"
        );
    }

    #[test]
    fn test_build_agent_env_forces_tmpdir_to_private_tmp() {
        let policy = SandboxPolicy {
            env_allow: vec!["PATH".to_string(), "TMPDIR".to_string()],
            ..Default::default()
        };
        // An inherited TMPDIR (a launcher value) must be replaced, not passed
        // through, when a private tmp is configured.
        let launcher_env = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("TMPDIR".to_string(), "/some/other/tmp".to_string()),
        ];
        let tmp = PathBuf::from("/tmp/agent-sandbox-job1");
        let out = build_agent_env(&policy, &launcher_env, Some(&tmp));
        let m: BTreeMap<&str, &str> = out.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(m.get("PATH"), Some(&"/usr/bin"));
        assert_eq!(m.get("TMPDIR"), Some(&"/tmp/agent-sandbox-job1"));
        assert_eq!(m.len(), 2, "unexpected extra variables in agent env");
    }

    // The process's C `environ` array (the in-memory environment). Accessing it
    // through a raw pointer is not safe in Rust, so it is exposed via FFI and
    // only ever read (never written) here. (A doc comment cannot be attached to
    // an extern block, so this uses plain comments.)
    extern "C" {
        static environ: *mut *mut std::ffi::c_char;
    }

    /// Walks the C `environ` array and reports whether any entry contains an
    /// `=` byte, i.e. at least one "KEY=VALUE" variable is present. Uses only
    /// raw pointers and stack work (no allocation), so it is safe inside a
    /// forked child.
    fn environ_has_entry() -> bool {
        let mut ptr = unsafe { environ };
        while !ptr.is_null() {
            let val = unsafe { *ptr };
            if val.is_null() {
                break;
            }
            let cstr = unsafe { std::ffi::CStr::from_ptr(val) };
            if cstr.to_bytes().contains(&b'=') {
                return true;
            }
            ptr = unsafe { ptr.add(1) };
        }
        false
    }

    #[test]
    fn test_shim_environment_is_cleared() {
        // Mirror the shim's own action (libc::clearenv) in a forked child and
        // assert its in-memory environment (C `environ`) is empty afterwards:
        // the shim is a fork of the launcher and must not retain the launcher's
        // variables. (The kernel's /proc/1/environ and /proc/1/mem, plus ptrace,
        // are separately blocked for a same-uid agent by PR_SET_DUMPABLE(0), set
        // by the shim in the same hardening block.) Everything in the child is
        // allocation-free.
        //
        // The child inherits the test process's (non-empty) environment, so we
        // first assert it actually has entries before clearing; otherwise the
        // "empty after" check would be a vacuous pass.
        let child = unsafe { libc::fork() };
        assert!(child >= 0, "fork clearenv child");
        if child == 0 {
            unsafe {
                if !environ_has_entry() {
                    libc::_exit(2); // child started empty: test is meaningless
                }
                libc::clearenv();
                libc::_exit(if environ_has_entry() { 1 } else { 0 });
            }
        }
        let mut st = 0;
        let r = unsafe { libc::waitpid(child, &mut st, 0) };
        assert_eq!(r, child, "waitpid(clearenv child)");
        assert_eq!(
            status_to_exit_code(st),
            0,
            "shim environment must be non-empty before, empty after clearenv"
        );
    }

    // ---- mount-plan tests (H1) ---------------------------------------------
    //
    // `plan_mounts` is pure (no syscalls), so these are fully hermetic. They
    // pin down the gVisor filesystem policy: private tmpfs over /tmp and
    // /dev/shm, read-only binds, and the hide overmounts (including the
    // derived state root, the job-dir root and the bare-repo root), each with
    // the agent's own `rw` paths preserved through the hide.

    fn spec(policy: SandboxPolicy) -> MountSpec {
        MountSpec {
            policy,
            state_root: PathBuf::from("/home/agent/.agent-run"),
            jobs_root: PathBuf::from("/workspace/.agent-jobs"),
            repos_root: PathBuf::from("/workspace/.agent-repos"),
        }
    }

    fn tmpfs_targets(plan: &[MountOp]) -> Vec<PathBuf> {
        plan.iter()
            .filter_map(|op| match op {
                MountOp::Tmpfs { target, .. } => Some(target.clone()),
                _ => None,
            })
            .collect()
    }

    fn ro_targets(plan: &[MountOp]) -> Vec<PathBuf> {
        plan.iter()
            .filter_map(|op| match op {
                MountOp::RemountRo { target } => Some(target.clone()),
                _ => None,
            })
            .collect()
    }

    fn hide_targets(plan: &[MountOp]) -> Vec<PathBuf> {
        plan.iter()
            .filter_map(|op| match op {
                MountOp::Hide { target, .. } => Some(target.clone()),
                _ => None,
            })
            .collect()
    }

    fn hide_preserved(plan: &[MountOp], hide: &Path) -> Vec<PathBuf> {
        plan.iter()
            .filter_map(|op| match op {
                MountOp::Hide { target, preserved } if target == hide => Some(
                    preserved
                        .iter()
                        .map(|p| target.join(&p.rel))
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .next()
            .unwrap_or_default()
    }

    #[test]
    fn test_plan_has_private_agent_tmp_and_shm() {
        let policy = SandboxPolicy {
            tmp: "auto".to_string(),
            agent_id: Some("job1".to_string()),
            ..Default::default()
        };
        let (plan, private_tmp) = plan_mounts(&spec(policy));
        assert_eq!(private_tmp, Some(PathBuf::from("/tmp/agent-sandbox-job1")));

        let t = tmpfs_targets(&plan);
        // The agent's own private tmp and /dev/shm are private tmpfs.
        assert!(
            t.contains(&PathBuf::from("/tmp/agent-sandbox-job1")),
            "no private agent tmp: {t:?}"
        );
        assert!(
            t.contains(&PathBuf::from("/dev/shm")),
            "no private /dev/shm: {t:?}"
        );
        // /tmp itself is NOT overmounted with a fresh tmpfs: doing so would wipe
        // every existing /tmp path (the agent's rw worktree, the state root and
        // the launched binary when they live under /tmp) and make the preserve/
        // hide steps that need those paths fail closed.
        assert!(
            !t.contains(&PathBuf::from("/tmp")),
            "a fresh tmpfs over /tmp would wipe existing /tmp paths: {t:?}"
        );

        // The private agent tmp and /dev/shm are both owner-only.
        let s = &plan[..];
        let tmp_opts = s
            .iter()
            .find_map(|op| match op {
                MountOp::Tmpfs { target, size }
                    if target == Path::new("/tmp/agent-sandbox-job1") =>
                {
                    size.clone()
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            tmp_opts.as_str(),
            AGENT_TMP_OPTS,
            "agent tmp opts: {tmp_opts:?}"
        );
        let shm_opts = s
            .iter()
            .find_map(|op| match op {
                MountOp::Tmpfs { target, size } if target == Path::new("/dev/shm") => size.clone(),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            shm_opts.as_str(),
            SHM_PRIVATE_OPTS,
            "shm opts: {shm_opts:?}"
        );
    }

    #[test]
    fn test_plan_enforces_ro_paths_with_bind_then_remount() {
        let policy = SandboxPolicy {
            ro: vec![
                PathBuf::from("/usr"),
                PathBuf::from("/etc"),
                PathBuf::from("/opt/tools"),
            ],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        // Every ro path is a read-only remount target.
        let ro = ro_targets(&plan);
        assert_eq!(
            ro,
            vec![
                PathBuf::from("/usr"),
                PathBuf::from("/etc"),
                PathBuf::from("/opt/tools"),
            ],
            "ro remount targets: {ro:?}"
        );
        // For each ro target there is a bind of the same path onto itself, and
        // the bind strictly precedes the remount (remount applies to the bind).
        for p in &ro {
            let bind_pos = plan
                .iter()
                .position(
                    |op| matches!(op, MountOp::BindRec { src, target } if src == p && target == p),
                )
                .unwrap_or_else(|| panic!("no self-bind for ro path {p:?}"));
            let ro_pos = plan
                .iter()
                .position(|op| matches!(op, MountOp::RemountRo { target } if target == p))
                .unwrap_or_else(|| panic!("no remount-ro for ro path {p:?}"));
            assert!(
                bind_pos < ro_pos,
                "bind ({bind_pos}) must precede remount-ro ({ro_pos}) for {p:?}"
            );
        }
    }

    #[test]
    fn test_plan_skips_ro_that_shadows_an_rw_path() {
        // A read-only path that is an ancestor of an rw path must NOT be
        // remounted read-only: doing so would revoke the write access the rw
        // path needs. A ro path that does not shadow an rw path is still
        // remounted read-only (the rule must not weaken the normal case).
        let policy = SandboxPolicy {
            ro: vec![
                // ancestor of the rw worktree -> must be skipped
                PathBuf::from("/scratch"),
                // unrelated ro path -> still enforced
                PathBuf::from("/usr"),
            ],
            rw: vec![PathBuf::from("/scratch/ws/.agent-jobs/job1")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        let ro = ro_targets(&plan);
        assert!(
            !ro.contains(&PathBuf::from("/scratch")),
            "a ro path that shadows an rw path must not be remounted ro: {ro:?}"
        );
        assert!(
            !plan.iter().any(
                |op| matches!(op, MountOp::BindRec { src, .. } if src == Path::new("/scratch"))
            ),
            "a ro path that shadows an rw path must not be bind-mounted either"
        );
        assert!(
            ro.contains(&PathBuf::from("/usr")),
            "an unrelated ro path must still be remounted ro: {ro:?}"
        );
    }

    fn rw_rescue_targets(plan: &[MountOp]) -> Vec<PathBuf> {
        plan.iter()
            .filter_map(|op| match op {
                MountOp::RemountRw { target } => Some(target.clone()),
                _ => None,
            })
            .collect()
    }

    // HL-113: a policy `rw` path nested under a `ro` path would stay inside a
    // read-only bind-remount (the section-4 self-bind or the section-5
    // hide-preservation remount). The rw declaration is explicit policy, so a
    // nested rw path must be re-opened writable by the plan.
    #[test]
    fn test_plan_reopens_nested_rw_under_ro_after_ro_remount() {
        // The a2a-inbox shape: the inbox dir is ro, the `injected` marker dir
        // is a rw CHILD of it.
        let policy = SandboxPolicy {
            ro: vec![PathBuf::from("/home/agent/.agent-run/a2a-inbox/job1")],
            rw: vec![PathBuf::from(
                "/home/agent/.agent-run/a2a-inbox/job1/injected",
            )],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        // The nested rw path gets a writable rescue mount...
        let rescue = rw_rescue_targets(&plan);
        assert_eq!(
            rescue,
            vec![PathBuf::from(
                "/home/agent/.agent-run/a2a-inbox/job1/injected"
            )],
            "nested rw must be re-opened writable: {rescue:?}"
        );
        // ...and it is emitted AFTER the ro remount of the inbox (the parent),
        // so the writable bind lands on top of the read-only mount.
        let ro_pos = match plan.iter().position(|op| {
            matches!(op, MountOp::RemountRo { target } if target == Path::new("/home/agent/.agent-run/a2a-inbox/job1"))
        }) {
            Some(p) => p,
            None => {
                // The inbox ro path may be preserved through the state-root
                // hide (see test below); accept either an explicit RemountRo
                // or a Hide-op remount that shadows it.
                let pos = plan.iter().position(|op| {
                    matches!(op, MountOp::Hide { target, .. } if target == Path::new("/home/agent/.agent-run"))
                });
                pos.expect("the ro inbox must be enforced one way or another")
            }
        };
        let rw_pos = plan
            .iter()
            .position(|op| {
                matches!(op, MountOp::RemountRw { target } if target == Path::new("/home/agent/.agent-run/a2a-inbox/job1/injected"))
            })
            .unwrap();
        assert!(
            ro_pos < rw_pos,
            "the ro enforcement ({ro_pos}) must precede the writable rescue ({rw_pos})"
        );
    }

    #[test]
    fn test_plan_rw_not_under_ro_produces_no_rescue_mount() {
        // A rw path with no ro ancestor (the normal worktree case) must NOT
        // get an extra writable mount: nothing was remounted read-only over it.
        let policy = SandboxPolicy {
            ro: vec![PathBuf::from("/usr"), PathBuf::from("/opt/tools")],
            rw: vec![PathBuf::from("/workspace/homelab")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        assert!(
            rw_rescue_targets(&plan).is_empty(),
            "a non-nested rw path must not get a rescue mount: {plan:?}"
        );
    }

    #[test]
    fn test_plan_two_levels_of_nesting_ordered_by_depth() {
        // Two nested rw paths under the same ro root: the shallowest is
        // re-opened first, the deeper one after it (so the deeper bind mounts
        // on top of the re-opened parent).
        let policy = SandboxPolicy {
            ro: vec![PathBuf::from("/data/ro-root")],
            rw: vec![
                PathBuf::from("/data/ro-root/a"),
                PathBuf::from("/data/ro-root/a/b"),
            ],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        let rescue = rw_rescue_targets(&plan);
        assert_eq!(
            rescue,
            vec![
                PathBuf::from("/data/ro-root/a"),
                PathBuf::from("/data/ro-root/a/b"),
            ],
            "nested rw rescues must be ordered shallowest-first: {rescue:?}"
        );
    }

    #[test]
    fn test_plan_nested_rw_under_hide_preserved_ro_gets_rescue() {
        // The a2a shape in a derived-root hide (the live failure): the state
        // root is auto-hidden, the inbox ro path is preserved through the
        // hide and re-mounted read-only, and the nested `injected` rw path
        // must then be re-opened writable AFTER that ro remount.
        let policy = SandboxPolicy {
            rw: vec![PathBuf::from(
                "/home/agent/.agent-run/a2a-inbox/job1/injected",
            )],
            ro: vec![PathBuf::from("/home/agent/.agent-run/a2a-inbox/job1")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        // The ro inbox is preserved through the state-root hide and remounted
        // read-only (the section-5 RemountRo path).
        let preserved_ro = plan.iter().any(|op| {
            matches!(op, MountOp::RemountRo { target } if target == Path::new("/home/agent/.agent-run/a2a-inbox/job1"))
        });
        assert!(
            preserved_ro,
            "the preserved ro inbox must be remounted read-only: {plan:?}"
        );
        // The nested rw path is rescued AFTER that remount.
        let ro_pos = plan
            .iter()
            .position(|op| {
                matches!(op, MountOp::RemountRo { target } if target == Path::new("/home/agent/.agent-run/a2a-inbox/job1"))
            })
            .unwrap();
        let rw_pos = plan
            .iter()
            .position(|op| {
                matches!(op, MountOp::RemountRw { target } if target == Path::new("/home/agent/.agent-run/a2a-inbox/job1/injected"))
            })
            .unwrap();
        assert!(
            ro_pos < rw_pos,
            "the preserved-ro remount ({ro_pos}) must precede the writable rescue ({rw_pos})"
        );
    }

    #[test]
    fn test_plan_hides_derived_roots_and_preserves_own_rw() {
        let policy = SandboxPolicy {
            rw: vec![
                // this agent's own worktree, inside the job-dir root
                PathBuf::from("/workspace/.agent-jobs/homelab/job1"),
            ],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        let hides = hide_targets(&plan);
        // The derived roots are hidden even though they are not in policy.hide.
        assert!(
            hides.contains(&PathBuf::from("/home/agent/.agent-run")),
            "state root not hidden: {hides:?}"
        );
        assert!(
            hides.contains(&PathBuf::from("/workspace/.agent-jobs")),
            "job-dir root not hidden: {hides:?}"
        );
        assert!(
            hides.contains(&PathBuf::from("/workspace/.agent-repos")),
            "repos root not hidden: {hides:?}"
        );
        // The policy-declared hide dir is hidden too.
        assert!(
            hides.contains(&PathBuf::from("/workspace/.agent-jobs")),
            "policy hide missing: {hides:?}"
        );
        // The agent's own rw worktree survives through the job-dir-root hide.
        let preserved = hide_preserved(&plan, Path::new("/workspace/.agent-jobs"));
        assert_eq!(
            preserved,
            vec![PathBuf::from("/workspace/.agent-jobs/homelab/job1")],
            "preserved under job root: {preserved:?}"
        );
        // A rw path that is a direct child of a hidden root must also bind back.
        // (state root has no rw child here, so it is an empty hide.)
        assert!(
            hide_preserved(&plan, Path::new("/home/agent/.agent-run")).is_empty(),
            "state root hide should have no preserved path here"
        );
    }

    #[test]
    fn test_plan_does_not_hide_a_whole_rw_path() {
        // When a hidden dir is itself one of the agent's rw paths, hiding it
        // would remove the agent's own write access, so it must be skipped.
        let policy = SandboxPolicy {
            rw: vec![PathBuf::from("/workspace/.agent-repos")],
            hide: vec![PathBuf::from("/workspace/.agent-repos")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        assert!(
            !hide_targets(&plan).contains(&PathBuf::from("/workspace/.agent-repos")),
            "a whole-rw hide dir must not be hidden: {plan:?}"
        );
    }

    // F4: a shared_rw path living under a hidden parent must be preserved
    // through the hide overmount (writable by design), not wiped for this
    // agent.
    #[test]
    fn test_plan_preserves_shared_rw_under_hide() {
        let policy = SandboxPolicy {
            rw: vec![PathBuf::from("/workspace/.agent-jobs/homelab/job1")],
            shared_rw: vec![PathBuf::from("/workspace/.agent-jobs/homelab/job1/shared")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        let preserved = hide_preserved(&plan, Path::new("/workspace/.agent-jobs"));
        for p in [
            PathBuf::from("/workspace/.agent-jobs/homelab/job1"),
            PathBuf::from("/workspace/.agent-jobs/homelab/job1/shared"),
        ] {
            assert!(
                preserved.contains(&p),
                "preserved missing {p:?}: {preserved:?}"
            );
        }
        // The shared_rw path is NOT in the ro list, so no RemountRo for it.
        let ro = ro_targets(&plan);
        assert!(
            !ro.contains(&PathBuf::from("/workspace/.agent-jobs/homelab/job1/shared")),
            "shared_rw preserved path must stay writable: {ro:?}"
        );
    }

    // F4: a ro path living under a hidden parent must be preserved through the
    // hide overmount AND re-mounted read-only after the bind-back, so the agent
    // can read it but never write it.
    #[test]
    fn test_plan_preserves_ro_under_hide_with_remount_ro() {
        let policy = SandboxPolicy {
            ro: vec![PathBuf::from("/workspace/.agent-jobs/tools/docs")],
            rw: vec![PathBuf::from("/workspace/.agent-jobs/homelab/job1")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        let hide = Path::new("/workspace/.agent-jobs");
        let preserved = hide_preserved(&plan, hide);
        assert!(
            preserved.contains(&PathBuf::from("/workspace/.agent-jobs/tools/docs")),
            "ro path under hide must be preserved: {preserved:?}"
        );
        // After the preserve-bind, the bind-back target must be remounted ro.
        let ro = ro_targets(&plan);
        assert!(
            ro.contains(&hide.join("tools/docs")),
            "preserved ro path must be remounted ro through the hide: {ro:?}"
        );
    }

    // F4 (guard): hiding is skipped entirely when a shared_rw path equals the
    // hide target itself (overmounting it would remove the agent's shared write
    // access).
    #[test]
    fn test_plan_does_not_hide_a_whole_shared_rw_path() {
        let policy = SandboxPolicy {
            shared_rw: vec![PathBuf::from("/workspace/.agent-jobs")],
            hide: vec![PathBuf::from("/workspace/.agent-jobs")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        assert!(
            !hide_targets(&plan).contains(&PathBuf::from("/workspace/.agent-jobs")),
            "a whole-shared_rw hide dir must not be hidden: {plan:?}"
        );
    }

    // F4 (guard): hiding is skipped entirely when a ro path equals the hide
    // target itself (overmounting it would remove the agent's read access).
    #[test]
    fn test_plan_does_not_hide_a_whole_ro_path() {
        let policy = SandboxPolicy {
            ro: vec![PathBuf::from("/workspace/.agent-jobs")],
            hide: vec![PathBuf::from("/workspace/.agent-jobs")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        assert!(
            !hide_targets(&plan).contains(&PathBuf::from("/workspace/.agent-jobs")),
            "a whole-ro hide dir must not be hidden: {plan:?}"
        );
    }

    #[test]
    fn test_plan_missing_hide_dir_still_hidden_not_skipped() {
        // A hide dir that does not exist on the original tree must still be
        // hidden (the executor creates it then mounts an empty tmpfs), never
        // silently skipped. The plan therefore contains a Hide for it.
        let policy = SandboxPolicy {
            hide: vec![PathBuf::from("/no/such/dir/anywhere")],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        assert!(
            hide_targets(&plan).contains(&PathBuf::from("/no/such/dir/anywhere")),
            "a missing hide dir must still be hidden, not skipped: {plan:?}"
        );
    }

    #[test]
    fn test_plan_file_hide_target_produces_hide_op() {
        // The operator token file (KUBEADP_API_TOKEN_FILE) is a regular FILE
        // on the hide list. `plan_mounts` is pure and must not assume the hide
        // target is a directory: a file target still yields a Hide op, and no
        // path under it can be preserved (a file has no children), so the
        // op carries an empty `preserved` list. Without this, apply_mount_op
        // fails with EEXIST (create_dir_all on a file) and every agent-run
        // job dies.
        let token = PathBuf::from("/var/run/kubeadp/api-token");
        let policy = SandboxPolicy {
            hide: vec![token.clone()],
            ..Default::default()
        };
        let (plan, _) = plan_mounts(&spec(policy));
        let hides = hide_targets(&plan);
        assert!(
            hides.contains(&token),
            "a file hide target must still produce a Hide op (never skipped): {plan:?}"
        );
        let op = plan
            .iter()
            .find_map(|op| match op {
                MountOp::Hide { target, preserved } if target == &token => Some(preserved),
                _ => None,
            })
            .expect("Hide op for the token file");
        assert!(
            op.is_empty(),
            "a file hide target cannot have preserved sub-paths: {op:?}"
        );
    }

    #[test]
    fn test_hide_is_file_classifier() {
        // The apply path branches on `hide_is_file`: true for a regular file
        // (the /dev/null bind), false for a directory and for a missing target
        // (the empty-tmpfs path). Hermetic, no mount involved.
        let dir = tempfile::tempdir().expect("tempdir");
        let f = dir.path().join("token");
        fs::write(&f, b"secret").expect("write token file");
        assert!(
            hide_is_file(&f),
            "a regular file must be classified as a file target"
        );
        assert!(
            !hide_is_file(dir.path()),
            "a directory must not be classified as a file target"
        );
        let missing = dir.path().join("does-not-exist");
        assert!(
            !hide_is_file(&missing),
            "a missing target must take the directory (tmpfs) path"
        );
    }

    #[test]
    fn test_plan_is_deterministic_and_ordered() {
        // Root private and fresh /proc must be the first two ops, so the
        // sandbox's mount/proc view is established before any policy mount.
        let (plan, _) = plan_mounts(&spec(SandboxPolicy::default()));
        assert!(matches!(plan[0], MountOp::RootPrivate));
        assert!(matches!(plan[1], MountOp::Proc));
        // Determinism: the same policy yields the same plan.
        let again = plan_mounts(&spec(SandboxPolicy::default())).0;
        assert_eq!(plan, again, "plan must be deterministic");
        // No hide target may be hidden more than once (dedup).
        let hides = hide_targets(&plan);
        assert_eq!(
            hides.len(),
            hides
                .iter()
                .map(|h| h.to_path_buf())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            "duplicate hide targets in plan: {hides:?}"
        );
    }

    #[test]
    fn test_mount_plan_fails_closed_on_mount_failure() {
        use super::apply_mount_plan;
        // Force a deterministic setup failure that needs no privileges and is
        // independent of the kernel's mount policy: target a mountpoint under a
        // regular file, so the mountpoint cannot be created (the target's path
        // component is a file, not a directory). apply_mount_plan must return
        // Err (never Ok, never panic) so the launch aborts with exit 125.
        let dir = tempfile::tempdir().expect("tempdir");
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, b"x").expect("write blocker file");
        let bad_target = blocker.join("sub"); // under a regular file

        let plan = vec![MountOp::Tmpfs {
            target: bad_target.clone(),
            size: Some("size=1m".to_string()),
        }];
        let err = apply_mount_plan(&plan).expect_err("mount failure must fail closed");
        assert!(
            err.contains("prepare tmpfs target")
                || err.contains(bad_target.to_string_lossy().as_ref()),
            "error must name the failing mount: {err}"
        );
    }

    #[test]
    fn test_plan_custom_tmp_and_no_auto_double_mount() {
        let p1 = SandboxPolicy {
            tmp: "/custom/scratch".to_string(),
            ..Default::default()
        };
        let (plan1, ptmp1) = plan_mounts(&spec(p1));
        assert_eq!(ptmp1, Some(PathBuf::from("/custom/scratch")));
        assert!(
            tmpfs_targets(&plan1).contains(&PathBuf::from("/custom/scratch")),
            "custom tmp not mounted: {plan1:?}"
        );

        let p2 = SandboxPolicy {
            tmp: String::new(),
            ..Default::default()
        };
        let (plan2, ptmp2) = plan_mounts(&spec(p2));
        assert!(ptmp2.is_none(), "empty tmp must disable private tmp");
        assert!(
            !tmpfs_targets(&plan2)
                .iter()
                .any(|t| t == Path::new("/tmp/agent-sandbox-agent")),
            "no agent tmp should be mounted when tmp is empty: {plan2:?}"
        );
        // /dev/shm is still private even without a private tmp (and /tmp is
        // never overmounted with a fresh tmpfs, so it is not a tmpfs target).
        assert!(tmpfs_targets(&plan2).contains(&PathBuf::from("/dev/shm")));
        assert!(!tmpfs_targets(&plan2).contains(&PathBuf::from("/tmp")));
    }

    #[test]
    fn test_relay_mount_plan_binds_outer_socket_into_sandbox() {
        // The relay mode binds the outer proxy's unix socket onto the
        // in-sandbox target (under the agent's private tmpfs) with a single
        // recursive bind. Asserted on the built plan, no real namespaces.
        let plan = relay_mount_plan(
            Path::new("/tmp/kubeadp-relay-agent/proxy.sock"),
            Path::new("/tmp/agent-sandbox-job1/.relay.sock"),
        );
        assert_eq!(
            plan,
            vec![MountOp::BindRec {
                src: PathBuf::from("/tmp/kubeadp-relay-agent/proxy.sock"),
                target: PathBuf::from("/tmp/agent-sandbox-job1/.relay.sock"),
            }]
        );
        // The host mount plan is unchanged by the relay mode: the relay bind
        // is an extra plan applied after the main one.
        let (plan, _) = plan_mounts(&spec(SandboxPolicy::default()));
        assert!(
            !plan
                .iter()
                .any(|op| matches!(op, MountOp::BindRec { src, .. }
                if src.as_os_str().to_string_lossy().contains("kubeadp-relay"))),
            "the host plan must carry no relay mount: {plan:?}"
        );
    }

    #[test]
    fn test_delivery_socket_mount_plan_binds_the_agent_socket_into_private_tmp() {
        // The delivery socket (the launcher's per-agent socket, SPEC §6.9) is
        // bind-mounted onto its in-sandbox target under the private tmp next
        // to the relay socket, with a single recursive bind -- the same shape
        // as the relay mount.
        let plan = delivery_socket_mount_plan(
            Path::new("/tmp/kubeadp-a2a-agent-7/delivery.sock"),
            Path::new("/tmp/agent-sandbox-job1/.delivery.sock"),
        );
        assert_eq!(
            plan,
            vec![MountOp::BindRec {
                src: PathBuf::from("/tmp/kubeadp-a2a-agent-7/delivery.sock"),
                target: PathBuf::from("/tmp/agent-sandbox-job1/.delivery.sock"),
            }]
        );
    }

    #[test]
    fn test_gvisor_refuses_unknown_net_value() {
        // An unknown net value fails closed before any namespace is unshared.
        let policy = SandboxPolicy {
            net: "bridged".to_string(),
            ..Default::default()
        };
        let err = execute_gvisor(&policy, "true", &[], &[]).unwrap_err();
        assert!(err.contains("unknown net value"), "err: {err}");
    }

    #[test]
    fn test_gvisor_drops_path_grants_with_one_warning_each() {
        // Path grants are not enforceable in the gVisor mount sandbox: each is
        // dropped with one warning naming the grant (the session starts with
        // strictly less access). Env grants are NOT listed -- build_agent_env
        // applies them.
        use super::super::policy::GrantsConfig;
        let grants = GrantsConfig {
            serviceaccount: true,
            ro_paths: vec![PathBuf::from("/opt/tools")],
            rw_paths: vec![PathBuf::from("/workspace"), PathBuf::from("/no/such/path")],
            env: vec!["GH_TOKEN".to_string(), "VIKUNJA_*".to_string()],
        };
        let warnings = gvisor_grant_warnings(&grants);
        assert_eq!(
            warnings.len(),
            4,
            "one warning per path grant: {warnings:?}"
        );
        let whats: Vec<&str> = warnings.iter().map(|(w, _)| w.as_str()).collect();
        assert!(whats.contains(&"serviceaccount"), "{whats:?}");
        assert!(whats.contains(&"ro /opt/tools"), "{whats:?}");
        assert!(whats.contains(&"rw /workspace"), "{whats:?}");
        assert!(whats.contains(&"rw /no/such/path"), "{whats:?}");
        assert!(
            warnings
                .iter()
                .all(|(_, r)| r.contains("cannot enforce path grants")),
            "{warnings:?}"
        );
        // No grants -> no warnings.
        assert!(gvisor_grant_warnings(&GrantsConfig::default()).is_empty());
    }

    #[test]
    fn test_gvisor_build_agent_env_keeps_env_grants() {
        // Env grants ARE applied by the gVisor backend: build_agent_env filters
        // through is_env_allowed, which honours grants.env on top of env_allow.
        use super::super::policy::GrantsConfig;
        let policy = SandboxPolicy {
            env_allow: vec!["PATH".to_string()],
            grants: Some(GrantsConfig {
                env: vec!["GH_TOKEN".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        let launcher_env = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("GH_TOKEN".to_string(), "tok".to_string()),
            ("SECRET_BYTES".to_string(), "abc".to_string()),
        ];
        let out = build_agent_env(&policy, &launcher_env, None);
        let got: BTreeMap<&str, &str> = out.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(got.get("PATH"), Some(&"/usr/bin"));
        assert_eq!(
            got.get("GH_TOKEN"),
            Some(&"tok"),
            "env grant must be applied"
        );
        assert_eq!(
            got.get("SECRET_BYTES"),
            None,
            "ungranted var must stay filtered"
        );
    }
}

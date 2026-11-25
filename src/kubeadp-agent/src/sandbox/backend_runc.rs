//! runc / standard container sandbox backend.
//!
//! The runc runtime provides no isolation of its own, so this backend is
//! strictly probe-driven: it reports and enforces ONLY the isolation the
//! probes actually confirmed, and it never claims a mechanism it does not apply.
//!
//! Decision (from the environment probe):
//! - If a fresh procfs can be mounted in a user namespace, run the
//!   namespace-based backend (gVisor-style: user/PID/mount/IPC/UTS namespaces,
//!   fresh `/proc`, private tmpfs, init shim). This path does NOT apply
//!   Landlock, and it must not be reported as if it did.
//! - Otherwise, if Landlock is available, run the Landlock backend (Kata-style:
//!   Landlock domain + subreaper tree-kill). This path does NOT create
//!   namespaces.
//! - If neither primitive is available, fail closed: the launch is refused
//!   (exit 125) rather than running the agent without isolation.
//!
//! The two mechanisms are alternatives, not a stack: whichever one is selected
//! is the ONLY extra isolation applied on top of the base container.
//!
//! Opt-in `net: relay`: the private network namespace is created through
//! unprivileged user namespaces -- by the namespace mechanism (the gVisor
//! backend unshares it itself, see [`super::backend_gvisor`]) or by the
//! Landlock mechanism (the kata backend's child unshares
//! CLONE_NEWUSER | CLONE_NEWNET, see [`super::backend_kata`]). Without user
//! namespaces neither can create the namespace, so relay fails closed with
//! the missing capability named. `net: host` (the default) and the legacy
//! `shared` alias keep today's behaviour byte-for-byte.

use super::backend_gvisor::execute_gvisor;
use super::backend_kata::execute_kata;
use super::detect::probe_environment;
use super::policy::SandboxPolicy;

/// Which isolation primitive the runc backend will enforce, decided purely
/// from the probe results. This is the exact claim the backend makes about
/// what it applies; it never reports a stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuncMechanism {
    /// Namespace-based isolation (gVisor-style). No Landlock is applied.
    Namespaces,
    /// Landlock domain + subreaper tree-kill (Kata-style). No namespaces.
    Landlock,
}

impl RuncMechanism {
    /// The single mechanism the backend enforces, reported exactly.
    pub fn description(self) -> &'static str {
        match self {
            Self::Namespaces => "namespaces (no Landlock)",
            Self::Landlock => "Landlock + subreaper (no namespaces)",
        }
    }
}

/// Chooses the runc isolation mechanism from the probe flags. Returns `None`
/// (fail closed) when neither required primitive is available.
pub fn runc_mechanism(
    landlock_abi: u32,
    user_namespace_ok: bool,
    fresh_procfs_ok: bool,
) -> Option<RuncMechanism> {
    if user_namespace_ok && fresh_procfs_ok {
        Some(RuncMechanism::Namespaces)
    } else if landlock_abi > 0 {
        Some(RuncMechanism::Landlock)
    } else {
        None
    }
}

/// Executes a command in the runc backend using only the isolation the probe
/// confirmed. Fails closed (returns `Err`) when no required primitive is
/// available, so the launcher exits 125 and the command never runs.
pub fn execute_runc(
    policy: &SandboxPolicy,
    cmd: &str,
    args: &[String],
    env: &[(String, String)],
) -> Result<i32, String> {
    let probe = probe_environment(None);

    let mechanism = runc_mechanism(
        probe.landlock_abi,
        probe.user_namespace.ok,
        probe.fresh_procfs_mount.ok,
    )
    .ok_or_else(|| {
        format!(
            "runc backend: neither namespaces nor Landlock available -- refusing to run without \
             isolation (userns: {}, fresh /proc: {}, landlock ABI: {})",
            probe.user_namespace.reason, probe.fresh_procfs_mount.reason, probe.landlock_abi
        )
    })?;

    // `net: relay` needs the agent's private network namespace, created
    // through unprivileged user namespaces (by either mechanism's delegate).
    // When the probe found user namespaces unavailable, neither delegate can
    // create the namespace, so the launch fails closed with the missing
    // capability named instead of running with weaker network isolation.
    if policy.net == "relay" {
        runc_relay_gate(probe.user_namespace.ok)?;
    }

    match mechanism {
        RuncMechanism::Namespaces => execute_gvisor(policy, cmd, args, env),
        RuncMechanism::Landlock => execute_kata(policy, cmd, args, env),
    }
}

/// Pure gate for the runc backend's `net: relay` support. The agent's private
/// network namespace is created through unprivileged user namespaces -- by the
/// namespace mechanism (the gVisor delegate) or by the kata delegate's child
/// unshare (CLONE_NEWUSER | CLONE_NEWNET). When user namespaces are
/// unavailable, neither can create the namespace, so the launch fails closed
/// with the missing capability named.
pub fn runc_relay_gate(user_namespace_ok: bool) -> Result<(), String> {
    if user_namespace_ok {
        Ok(())
    } else {
        Err(
            "net=relay requires unprivileged user namespaces (unshare(CLONE_NEWUSER)) to create \
             the agent's private network namespace; the runc backend's probe reports user \
             namespaces unavailable -- refusing to run with weaker network isolation"
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runc_mechanism_selection() {
        // Namespaces win when both primitives are available; Landlock is NOT
        // applied, and the backend must not report it as if it were.
        assert_eq!(
            runc_mechanism(7, true, true),
            Some(RuncMechanism::Namespaces)
        );
        // No namespaces but Landlock: Landlock path.
        assert_eq!(
            runc_mechanism(7, false, false),
            Some(RuncMechanism::Landlock)
        );
        // Landlock present but no fresh procfs: Landlock path.
        assert_eq!(
            runc_mechanism(6, true, false),
            Some(RuncMechanism::Landlock)
        );
        // Nothing available: fail closed.
        assert_eq!(runc_mechanism(0, false, false), None);
        // ABI 0 and no namespaces: fail closed even if procfs mount looks ok.
        assert_eq!(runc_mechanism(0, false, true), None);
    }

    #[test]
    fn test_runc_mechanism_reports_exactly_one_primitive() {
        // The description names exactly one mechanism and never claims a stack
        // or a mechanism that is not applied.
        let ns = RuncMechanism::Namespaces.description();
        assert!(ns.contains("no Landlock"));
        assert!(!ns.to_ascii_lowercase().contains("stack"));
        let ll = RuncMechanism::Landlock.description();
        assert!(ll.contains("no namespaces"));
        assert!(!ll.to_ascii_lowercase().contains("stack"));
    }

    #[test]
    fn test_runc_fails_closed_without_primitives() {
        // When neither primitive is available the backend refuses to run.
        assert!(runc_mechanism(0, false, false).is_none());
    }

    // ---- net: relay ----------------------------------------------------------
    //
    // The runc backend supports `net: relay` through its two mechanisms: the
    // namespace mechanism delegates to the gVisor backend (which unshares the
    // private network namespace itself, see `super::backend_gvisor`) and the
    // Landlock mechanism delegates to the kata backend (whose child unshares
    // CLONE_NEWUSER|CLONE_NEWNET, see `super::backend_kata`). These tests are
    // hermetic: they pin the pure gate and the (net-mode-independent)
    // mechanism selection; the end-to-end launch is not exercised (no real
    // namespaces in unit tests).

    #[test]
    fn test_runc_relay_gate_requires_user_namespaces() {
        // With user namespaces the relay namespace can be created (either
        // mechanism's delegate handles it); without them the launch fails
        // closed with the missing capability named.
        assert_eq!(runc_relay_gate(true), Ok(()));
        let err = runc_relay_gate(false).unwrap_err();
        assert!(err.contains("net=relay"), "err: {err}");
        assert!(err.contains("user namespaces"), "err: {err}");
    }

    #[test]
    fn test_runc_relay_delegates_through_both_mechanisms() {
        // The mechanism selection decides which delegate creates the private
        // network namespace for `net: relay`; it is unchanged by the network
        // mode (the same selection that routes host/shared, both of which
        // keep today's behaviour byte-for-byte).
        assert_eq!(
            runc_mechanism(6, true, true),
            Some(RuncMechanism::Namespaces)
        );
        assert_eq!(
            runc_mechanism(6, false, false),
            Some(RuncMechanism::Landlock)
        );
    }
}

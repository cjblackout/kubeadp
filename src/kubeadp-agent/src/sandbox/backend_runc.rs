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

    match mechanism {
        RuncMechanism::Namespaces => execute_gvisor(policy, cmd, args, env),
        RuncMechanism::Landlock => execute_kata(policy, cmd, args, env),
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
}

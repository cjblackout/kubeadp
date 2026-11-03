//! Per-agent process isolation ("sandbox").
//!
//! Implements SANDBOX CONTRACT v1:
//! - Three runtimes: gvisor, kata, runc (plus native for testing/overrides).
//! - Policy configuration (rw, ro, shared_rw, hide, tmp, secret, env_allow, net).
//! - Command execution via `sandbox-exec` and environment/runtime inspection via `sandbox-probe`.

pub mod backend_gvisor;
pub mod backend_kata;
pub mod backend_runc;
pub mod cli;
pub mod detect;
pub mod exec;
pub mod init_shim;
pub mod policy;

/// Test-only launch seam. Compiled only under the `test-hooks` feature so the
/// production build never carries the passthrough switch.
#[cfg(feature = "test-hooks")]
pub mod testhook;

pub use detect::{detect_runtime, probe_environment, EnvironmentProbe, SandboxRuntime};
pub use policy::SandboxPolicy;

use serde::{Deserialize, Serialize};

/// Sandbox attribution recorded on a job/session at every launch: the backend
/// `sandbox-exec` will use, the runtime the pod auto-detected (Contract v1.3
/// -- isolation is always applied and the runtime is NEVER user-overridable),
/// and the policy file that was written for this launch.
///
/// Persisted on the job row (as `sandbox` JSON) so `agent-run status` and
/// `agent-run info` can print it without re-reading the policy file; a retry
/// re-fills it through the same `build_command` path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxInfo {
    /// Backend executed by `sandbox-exec` (`gvisor`, `kata`, `runc`, ...).
    pub backend: String,
    /// Runtime auto-detected inside the pod (`gvisor`, `kata`, `runc`).
    pub runtime: String,
    /// Policy file handed to `sandbox-exec` for this launch.
    pub policy_path: String,
}

impl SandboxInfo {
    /// Builds the launch-time sandbox attribution: the backend follows the
    /// runtime the pod auto-detected (there is no override in Contract v1.3).
    pub fn detected(policy_path: &std::path::Path) -> Self {
        let runtime = detect_runtime();
        // `sandbox-exec` dispatches to the backend of the same name as the
        // auto-detected runtime, so both labels carry the same value here.
        let backend = runtime.to_string();
        Self {
            backend,
            runtime: runtime.to_string(),
            policy_path: policy_path.display().to_string(),
        }
    }
}

/// Locates the `kubeadp-agent` binary executable to run `sandbox-exec`.
///
/// Only `kubeadp-agent` implements `sandbox-exec`; the thin wrappers
/// (`agent-orchestrate`, `agent-run-api`) do not. So the launcher is the
/// `kubeadp-agent` binary next to `std::env::current_exe()` when that exists
/// (all binaries live in one directory in the image), and `current_exe()`
/// itself otherwise (the process already is `kubeadp-agent`). There is no
/// env-variable override and no `target/deps/` scanning, so the release binary
/// can never be talked into executing an unexpected binary.
///
/// In integration/unit tests or harness runs `current_exe()` is often a test
/// runner in `deps/` that does not handle `sandbox-exec`, so the test build
/// additionally honours `CARGO_BIN_EXE_kubeadp-agent` and scans `deps/` for
/// the compiled binary (see [`find_launcher_exe_test`]).
pub fn find_launcher_exe() -> std::path::PathBuf {
    #[cfg(not(test))]
    {
        // Production: resolve only via the process's own executable path.
        match std::env::current_exe() {
            Ok(cur) => sibling_launcher(&cur),
            Err(_) => std::path::PathBuf::from("kubeadp-agent"),
        }
    }
    #[cfg(test)]
    find_launcher_exe_test()
}

/// Maps the running executable to the binary that handles `sandbox-exec`: the
/// `kubeadp-agent` sibling when `cur` is a different binary (a wrapper), else
/// `cur` unchanged.
#[cfg_attr(test, allow(dead_code))]
fn sibling_launcher(cur: &std::path::Path) -> std::path::PathBuf {
    if cur.file_name().and_then(|n| n.to_str()) != Some("kubeadp-agent") {
        if let Some(cand) = cur.parent().map(|d| d.join("kubeadp-agent")) {
            if cand.is_file() {
                return cand;
            }
        }
    }
    cur.to_path_buf()
}

/// Test/harness-only launcher resolution: prefers `CARGO_BIN_EXE_kubeadp-agent`,
/// then a `kubeadp-agent` binary adjacent to (or in the same target dir as) the
/// test runner, and finally scans `deps/` for the compiled binary. Compiled
/// out of non-test builds so the release binary only ever uses
/// `std::env::current_exe`.
#[cfg(test)]
fn find_launcher_exe_test() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("CARGO_BIN_EXE_kubeadp-agent") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    if let Ok(cur) = std::env::current_exe() {
        if let Some(parent) = cur.parent() {
            // Check adjacent kubeadp-agent
            let cand = parent.join("kubeadp-agent");
            if cand.exists() {
                return cand;
            }
            // Check target/{debug,release}/kubeadp-agent if cur was in deps/
            if parent.file_name().and_then(|n| n.to_str()) == Some("deps") {
                if let Some(target_dir) = parent.parent() {
                    let cand = target_dir.join("kubeadp-agent");
                    if cand.exists() {
                        return cand;
                    }
                }
                // Under cargo test, find the binary in deps/ by reading its .d file
                if let Ok(entries) = std::fs::read_dir(parent) {
                    let mut bins: Vec<std::path::PathBuf> = Vec::new();
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                            if file_name.starts_with("kubeadp_agent-") && path.extension().is_none()
                            {
                                let d_file = path.with_extension("d");
                                if let Ok(d_content) = std::fs::read_to_string(&d_file) {
                                    if d_content.contains("src/bin/kubeadp-agent.rs") {
                                        bins.push(path);
                                    }
                                }
                            }
                        }
                    }
                    bins.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok());
                    if let Some(b) = bins.pop() {
                        return b;
                    }
                }
            }
        }
        return cur;
    }
    std::path::PathBuf::from("kubeadp-agent")
}

#[cfg(test)]
mod launcher_tests {
    use super::sibling_launcher;
    use std::fs;

    #[test]
    fn wrapper_resolves_to_kubeadp_agent_sibling() {
        let d = tempfile::tempdir().unwrap();
        let ka = d.path().join("kubeadp-agent");
        let wrap = d.path().join("agent-orchestrate");
        fs::write(&ka, b"").unwrap();
        fs::write(&wrap, b"").unwrap();
        assert_eq!(sibling_launcher(&wrap), ka);
        assert_eq!(sibling_launcher(&ka), ka);
    }

    #[test]
    fn wrapper_without_sibling_falls_back_to_current() {
        let d = tempfile::tempdir().unwrap();
        let wrap = d.path().join("agent-orchestrate");
        fs::write(&wrap, b"").unwrap();
        assert_eq!(sibling_launcher(&wrap), wrap);
    }
}

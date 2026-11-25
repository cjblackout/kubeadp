//! Test-only launch seam for the job launcher.
//!
//! This whole module compiles ONLY under the `test-hooks` cargo feature, so
//! the seam (and every use of it) is absent from production builds. The
//! production image is built with `--features a2a` only, which does not enable
//! `test-hooks`.
//!
//! The seam is a process-global switch, `SANDBOX_PASSTHROUGH`, that
//! `core::runner::build_command` consults. When it is set (the default under
//! `test-hooks`) the launcher returns the plain `agy` command instead of the
//! `kubeadp-agent sandbox-exec ... -- agy ...` wrapper. This is what lets the
//! unit tests that drive a fake `agy` script through the real launcher run
//! without a working sandbox runtime (a plain Docker build container cannot
//! isolate), while still exercising the launcher's spawn/PID/cancel machinery.
//!
//! It is a test affordance only: it has no config, env or CLI surface and
//! defaults to ON under `test-hooks` so the existing deterministic tests run
//! unsandboxed. A test that wants the real `sandbox-exec` wrapping sets it to
//! false and, when the platform cannot isolate, skips itself (see
//! `core::runner`'s end-to-end sandbox test).

use std::sync::atomic::{AtomicBool, Ordering};

/// When true, `build_command` launches the plain `agy` command instead of
/// wrapping it in `sandbox-exec`. Default ON under `test-hooks`.
static SANDBOX_PASSTHROUGH: AtomicBool = AtomicBool::new(true);

/// Returns whether the launcher currently passes `agy` through unwrapped.
pub fn sandbox_passthrough() -> bool {
    SANDBOX_PASSTHROUGH.load(Ordering::SeqCst)
}

/// Sets the passthrough switch. Tests use this to opt back into the real
/// `sandbox-exec` wrapping (set `false`) or restore the default (set `true`).
pub fn set_sandbox_passthrough(on: bool) {
    SANDBOX_PASSTHROUGH.store(on, Ordering::SeqCst);
}

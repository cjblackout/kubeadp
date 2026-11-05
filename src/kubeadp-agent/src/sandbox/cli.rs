//! CLI handling for sandbox commands: `sandbox-exec` and `sandbox-probe`.

use super::backend_gvisor::execute_gvisor;
use super::backend_kata::execute_kata;
use super::backend_runc::execute_runc;
use super::detect::{detect_runtime, probe_environment, SandboxRuntime};
use super::policy::SandboxPolicy;
use std::env;
use std::process::exit;

/// Dispatches `sandbox-probe` subcommand.
pub fn run_sandbox_probe(args: &[String]) {
    let json_mode = args.iter().any(|a| a == "--json");
    let probe = probe_environment(None);

    if json_mode {
        match serde_json::to_string_pretty(&probe) {
            Ok(s) => println!("{s}"),
            Err(e) => {
                eprintln!("failed to serialize probe to JSON: {e}");
                exit(1);
            }
        }
    } else {
        println!("Runtime: {}", probe.runtime);
        println!("Kernel: {}", probe.kernel);
        println!(
            "User Namespace: {} ({})",
            if probe.user_namespace.ok {
                "ok"
            } else {
                "failed"
            },
            probe.user_namespace.reason
        );
        println!(
            "PID Namespace: {} ({})",
            if probe.pid_namespace.ok {
                "ok"
            } else {
                "failed"
            },
            probe.pid_namespace.reason
        );
        println!(
            "Fresh Procfs Mount: {} ({})",
            if probe.fresh_procfs_mount.ok {
                "ok"
            } else {
                "failed"
            },
            probe.fresh_procfs_mount.reason
        );
        println!(
            "Tmpfs Mount: {} ({})",
            if probe.tmpfs_mount.ok { "ok" } else { "failed" },
            probe.tmpfs_mount.reason
        );
        println!(
            "Bind Mount: {} ({})",
            if probe.bind_mount.ok { "ok" } else { "failed" },
            probe.bind_mount.reason
        );
        println!(
            "Landlock ABI: {} (scopes: {:?})",
            probe.landlock_abi, probe.landlock_scopes
        );
        println!(
            "Subreaper: {} ({})",
            if probe.subreaper.ok { "ok" } else { "failed" },
            probe.subreaper.reason
        );
        println!("Chosen Backend: {}", probe.chosen_backend);
    }
}

/// Dispatches `relay-inner` (the in-sandbox loopback relay, started by the
/// init shim for `net: relay` policies).
pub fn run_relay_inner(args: &[String]) {
    let code = super::relay::relay_inner_main(args);
    exit(code);
}

/// Dispatches `relay-connect` (the ssh ProxyCommand helper for `net: relay`).
pub fn run_relay_connect(args: &[String]) {
    let code = super::relay::relay_connect_main(args);
    exit(code);
}

/// Dispatches `sandbox-exec` subcommand.
///
/// Syntax:
/// kubeadp-agent sandbox-exec --policy <file|-> -- <cmd> [args...]
pub fn run_sandbox_exec(args: &[String]) {
    let mut policy_path: Option<String> = None;
    let mut cmd_and_args: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--" {
            cmd_and_args.extend_from_slice(&args[i + 1..]);
            break;
        } else if arg == "--policy" {
            if i + 1 >= args.len() {
                eprintln!("kubeadp-agent sandbox-exec: missing argument for --policy");
                exit(125);
            }
            policy_path = Some(args[i + 1].clone());
            i += 2;
        } else if let Some(val) = arg.strip_prefix("--policy=") {
            policy_path = Some(val.to_string());
            i += 1;
        } else {
            eprintln!("kubeadp-agent sandbox-exec: unrecognized option '{arg}'");
            exit(125);
        }
    }

    if cmd_and_args.is_empty() {
        eprintln!("kubeadp-agent sandbox-exec: no command specified after '--'");
        exit(125);
    }

    let policy_arg = match policy_path {
        Some(p) => p,
        None => {
            eprintln!("kubeadp-agent sandbox-exec: --policy <file|-> is required");
            exit(125);
        }
    };

    let target_cmd = &cmd_and_args[0];
    let target_args = &cmd_and_args[1..];
    let current_env: Vec<(String, String)> = env::vars().collect();

    // Load policy
    let policy = match SandboxPolicy::load_from_file_or_stdin(&policy_arg) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kubeadp-agent sandbox-exec: failed to load policy: {e}");
            exit(125);
        }
    };
    let policy = finish_policy(policy);

    // Runtime is ALWAYS auto-detected inside the pod (Contract v1.3)
    let effective_runtime = detect_runtime();

    // Validate that the backend mechanisms actually work; if probe fails, exit 125.
    // The probe is asked to auto-detect (no runtime override passed).
    let probe = probe_environment(None);
    if let Err(probe_err) = validate_backend_probe(effective_runtime, &probe) {
        eprintln!(
            "kubeadp-agent sandbox-exec: backend '{effective_runtime}' probe failed: {probe_err}"
        );
        exit(125);
    }

    // Isolation is ALWAYS applied (Contract v1.3): the only runtimes
    // `detect_runtime()` can return are gvisor/kata/runc, and every other value
    // (e.g. the never-detected `Auto`) fails closed -- there is no native /
    // no-isolation fallback. The refusal is decided by a pure helper so the
    // "no sandbox, no run" invariant is unit-tested in isolation.
    if let Some(reason) = refusal_for_runtime(effective_runtime) {
        eprintln!(
            "kubeadp-agent sandbox-exec: no isolating backend for runtime '{effective_runtime}' ({reason})"
        );
        exit(125);
    }
    let res = match effective_runtime {
        SandboxRuntime::Gvisor => execute_gvisor(&policy, target_cmd, target_args, &current_env),
        SandboxRuntime::Kata => execute_kata(&policy, target_cmd, target_args, &current_env),
        SandboxRuntime::Runc => execute_runc(&policy, target_cmd, target_args, &current_env),
        // Refusal_for_runtime already exited for these; keep a fail-closed
        // fallback so the target never runs unsandboxed even if detection
        // changes to yield them.
        SandboxRuntime::Native | SandboxRuntime::Auto => {
            eprintln!(
                "kubeadp-agent sandbox-exec: no isolating backend for runtime '{effective_runtime}' ({})",
                refusal_for_runtime(effective_runtime).unwrap()
            );
            exit(125);
        }
    };

    match res {
        Ok(code) => exit(code),
        Err(err) => {
            eprintln!("kubeadp-agent sandbox-exec: {err}");
            exit(125);
        }
    }
}

/// Returns a refusal reason when a runtime has no isolating backend, so
/// `sandbox-exec` fails closed instead of running the target unsandboxed.
/// `gvisor`/`kata`/`runc` have a backend (None); `native` and `auto` do not
/// (Some) -- there is deliberately no no-isolation fallback.
fn refusal_for_runtime(runtime: SandboxRuntime) -> Option<&'static str> {
    match runtime {
        SandboxRuntime::Gvisor | SandboxRuntime::Kata | SandboxRuntime::Runc => None,
        SandboxRuntime::Native | SandboxRuntime::Auto => {
            Some("isolation is always applied; refusing to run without a sandbox")
        }
    }
}

/// Finalizes a hand-run `sandbox-exec` policy the same way the in-process
/// launchers (the job runner and the orchestrator session supervisor) do: it
/// hides the operator token file and the jobset root key file so a hand-run
/// sandbox cannot read either credential. Both builder methods are no-ops when
/// the corresponding env path (`KUBEADP_API_TOKEN_FILE` /
/// `KUBEADP_JOBSET_ROOT_KEY_FILE`) is not configured, so nothing else changes.
fn finish_policy(policy: SandboxPolicy) -> SandboxPolicy {
    policy
        .hide_operator_token_file()
        .hide_jobset_root_key_file()
}

/// Validates whether the chosen backend's required mechanisms passed probe.
pub fn validate_backend_probe(
    runtime: SandboxRuntime,
    probe: &super::detect::EnvironmentProbe,
) -> Result<(), String> {
    match runtime {
        SandboxRuntime::Gvisor => {
            if !probe.user_namespace.ok {
                return Err(format!(
                    "user namespace unsupported ({})",
                    probe.user_namespace.reason
                ));
            }
            if !probe.pid_namespace.ok {
                return Err(format!(
                    "pid namespace unsupported ({})",
                    probe.pid_namespace.reason
                ));
            }
            if !probe.fresh_procfs_mount.ok {
                return Err(format!(
                    "fresh procfs mount unsupported ({})",
                    probe.fresh_procfs_mount.reason
                ));
            }
            Ok(())
        }
        SandboxRuntime::Kata => {
            if probe.landlock_abi == 0 {
                return Err("landlock is unsupported (ABI 0)".to_string());
            }
            if !probe.subreaper.ok {
                return Err(format!(
                    "subreaper unsupported ({})",
                    probe.subreaper.reason
                ));
            }
            Ok(())
        }
        SandboxRuntime::Runc => {
            let has_landlock = probe.landlock_abi > 0;
            let has_namespaces = probe.user_namespace.ok && probe.fresh_procfs_mount.ok;
            if !has_landlock && !has_namespaces {
                return Err(format!(
                    "neither namespaces nor landlock available (userns: {}, landlock ABI: {})",
                    probe.user_namespace.reason, probe.landlock_abi
                ));
            }
            Ok(())
        }
        SandboxRuntime::Native => Ok(()),
        SandboxRuntime::Auto => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::detect::MechanismStatus;

    #[test]
    fn test_validate_backend_probe_fails_on_unsupported() {
        let fake_probe = super::super::detect::EnvironmentProbe {
            runtime: SandboxRuntime::Gvisor,
            kernel: "Linux 4.19".to_string(),
            user_namespace: MechanismStatus::failed("blocked by seccomp"),
            pid_namespace: MechanismStatus::ok(),
            fresh_procfs_mount: MechanismStatus::ok(),
            tmpfs_mount: MechanismStatus::ok(),
            bind_mount: MechanismStatus::ok(),
            landlock_abi: 0,
            landlock_scopes: Vec::new(),
            subreaper: MechanismStatus::ok(),
            chosen_backend: "gvisor".to_string(),
        };

        // Gvisor must fail if user_namespace failed
        let err = validate_backend_probe(SandboxRuntime::Gvisor, &fake_probe).unwrap_err();
        assert!(err.contains("user namespace unsupported"), "err: {err}");

        // Kata must fail if landlock ABI is 0
        let err_kata = validate_backend_probe(SandboxRuntime::Kata, &fake_probe).unwrap_err();
        assert!(
            err_kata.contains("landlock is unsupported"),
            "err: {err_kata}"
        );

        // Runc must fail if neither landlock nor namespaces are available
        let err_runc = validate_backend_probe(SandboxRuntime::Runc, &fake_probe).unwrap_err();
        assert!(
            err_runc.contains("neither namespaces nor landlock"),
            "err: {err_runc}"
        );
    }

    #[test]
    fn test_no_isolating_backend_refuses_native_and_auto() {
        // There is no native / no-isolation fallback: `native` and `auto`
        // carry no isolating backend, so the launcher must refuse to run
        // (fail closed) rather than exec the target unsandboxed. The three
        // real backends must NOT be refused.
        assert!(
            refusal_for_runtime(SandboxRuntime::Native).is_some(),
            "native must be refused (no isolating backend)"
        );
        assert!(
            refusal_for_runtime(SandboxRuntime::Auto).is_some(),
            "auto must be refused (no isolating backend)"
        );
        for rt in [
            SandboxRuntime::Gvisor,
            SandboxRuntime::Kata,
            SandboxRuntime::Runc,
        ] {
            assert!(
                refusal_for_runtime(rt).is_none(),
                "{rt:?} has a backend and must not be refused"
            );
        }
    }

    #[test]
    fn test_finish_policy_adds_both_hides_when_configured() {
        // `finish_policy` is the policy-finishing step `sandbox-exec` applies to
        // a hand-run policy: it hides the operator token file and the jobset
        // root key file (the same two builder methods the in-process launchers
        // call), so a hand-run sandbox cannot read either credential.
        //
        // We READ the env-resolved paths and never mutate the process env, so
        // this cannot race the CLI token tests or the jobset tests that set
        // these same vars (they use their own, per-module locks). For whatever
        // the host has configured, the finished policy must hide each of the
        // two paths when present, preserve the existing hide list, and change
        // nothing else. (The deterministic "adds both" mechanic, independent of
        // the host env, is proven by the `with_hidden_path` test below.)
        use crate::sandbox::policy::{jobset_root_key_file_path, operator_token_file_path};

        let base = SandboxPolicy::for_job(
            "job-cli",
            std::path::Path::new("/w/.agent-jobs/job-cli"),
            None,
            None,
            None,
        );
        let token = operator_token_file_path();
        let key = jobset_root_key_file_path();
        let finished = finish_policy(base.clone());

        // The finished policy must equal the base policy with the two
        // configured credential paths appended (each once, deduplicated by
        // `with_hidden_path`, in token-then-key order) and nothing else
        // changed. Build the expected vector exactly the way the builder
        // methods do and compare.
        let mut expected = base.hide.clone();
        if let Some(t) = &token {
            if !expected.iter().any(|h| h == t) {
                expected.push(t.clone());
            }
        }
        if let Some(k) = &key {
            if !expected.iter().any(|h| h == k) {
                expected.push(k.clone());
            }
        }
        assert_eq!(
            finished.hide, expected,
            "finish_policy must add each configured credential path exactly once"
        );

        // No other field may change.
        assert_eq!(finished.role, base.role);
        assert_eq!(finished.agent_id, base.agent_id);
        assert_eq!(finished.rw, base.rw);
        assert_eq!(finished.ro, base.ro);
        assert_eq!(finished.shared_rw, base.shared_rw);
        assert_eq!(finished.tmp, base.tmp);
        assert_eq!(finished.secret, base.secret);
        assert_eq!(finished.env_allow, base.env_allow);
        assert_eq!(finished.net, base.net);
    }

    #[test]
    fn test_finish_policy_adds_both_hides_mechanics() {
        // Deterministic proof of the "adds both hides" mechanic without touching
        // the (globally shared, race-prone) env. `finish_policy` composes the
        // two builder methods, each of which calls the pure `with_hidden_path`
        // on the configured path; `with_hidden_path` adds each distinct path
        // exactly once. With two distinct configured paths both appear and the
        // count grows by two; a repeat pass is idempotent.
        use std::path::Path;

        let base = SandboxPolicy::for_job(
            "job-cli2",
            Path::new("/w/.agent-jobs/job-cli2"),
            None,
            None,
            None,
        );
        let token_file = Path::new("/run/secrets/api-token");
        let key_file = Path::new("/run/secrets/jobset-root-key");

        // The exact composition `finish_policy` performs, with the two paths a
        // configured host would resolve to (two real, distinct files).
        let finished = base
            .clone()
            .with_hidden_path(Some(token_file))
            .with_hidden_path(Some(key_file));
        let hide: Vec<&Path> = finished.hide.iter().map(|h| h.as_path()).collect();
        assert!(hide.contains(&token_file), "token file must be hidden");
        assert!(hide.contains(&key_file), "root key file must be hidden");
        assert_eq!(
            finished.hide.len(),
            base.hide.len() + 2,
            "both configured paths added once each"
        );

        // Idempotent: re-applying both does not duplicate them.
        let again = finished
            .clone()
            .with_hidden_path(Some(token_file))
            .with_hidden_path(Some(key_file));
        assert_eq!(
            again.hide.len(),
            base.hide.len() + 2,
            "repeat must not duplicate"
        );
    }
}

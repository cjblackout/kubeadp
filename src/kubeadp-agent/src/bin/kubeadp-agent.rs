//! kubeadp-agent: role dispatcher.
//!
//! Selects the long-lived role from the `KUBEADP_ROLE` environment variable:
//! - `orchestrator` -> the agent-orchestrate process supervisor
//! - `worker`       -> the agent-run API daemon (remote job execution)
//!
//! `KUBEADP_ROLE` unset or set to any other value exits with code 2 and a
//! clear message. Each role runs exactly the code that the corresponding old
//! crate's `main()` ran, honouring the same env vars and CLI.

use kubeadp_agent::config_file::{apply_to_env, config_file_map};
use std::process::exit;

/// Applies the TOML configuration file for the given role: `[common]` plus
/// the role's own section. The environment always wins; an absent file is
/// environment-only configuration. Any configuration error is fatal.
fn apply_config_for(role: &str) {
    let sections: &[&str] = match role {
        "orchestrator" => &["common", "orchestrator"],
        "worker" => &["common", "worker"],
        _ => &["common"],
    };
    if let Err(e) = config_file_map().map(|map| apply_to_env(&map, sections)) {
        eprintln!("kubeadp-agent: configuration error: {e}");
        exit(1);
    }
}

fn main() {
    match std::env::var("KUBEADP_ROLE").as_deref() {
        Ok("orchestrator") => {
            apply_config_for("orchestrator");
            kubeadp_agent::orchestrate::entry::run();
        }
        Ok("worker") => {
            apply_config_for("worker");
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            if let Err(e) = rt.block_on(kubeadp_agent::worker::entry::run()) {
                eprintln!("kubeadp-agent (worker): {e:#}");
                exit(1);
            }
        }
        Ok(other) => {
            eprintln!(
                "kubeadp-agent: unknown KUBEADP_ROLE '{other}' (expected 'orchestrator' or 'worker')"
            );
            exit(2);
        }
        Err(_) if std::env::var_os("KUBEADP_ROLE").is_none() => {
            eprintln!(
                "kubeadp-agent: KUBEADP_ROLE is not set (expected 'orchestrator' or 'worker')"
            );
            exit(2);
        }
        Err(_) => {
            eprintln!(
                "kubeadp-agent: KUBEADP_ROLE is not a valid UTF-8 string (expected 'orchestrator' or 'worker')"
            );
            exit(2);
        }
    }
}

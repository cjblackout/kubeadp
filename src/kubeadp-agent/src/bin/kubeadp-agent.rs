//! kubeadp-agent: role dispatcher.
//!
//! Selects the long-lived role from the `KUBEADP_ROLE` environment variable:
//! - `orchestrator` -> the agent-orchestrate process supervisor
//! - `worker`       -> the agent-run API daemon (remote job execution)
//!
//! The `mcp-a2a` subcommand (first argument) runs the A2A **sending** side as
//! a stdio MCP server (feature `a2a`) and does not require `KUBEADP_ROLE`.
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
    let first_arg = std::env::args().nth(1);
    match first_arg.as_deref() {
        Some("sandbox-exec") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            kubeadp_agent::sandbox::cli::run_sandbox_exec(&args);
            return;
        }
        Some("sandbox-probe") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            kubeadp_agent::sandbox::cli::run_sandbox_probe(&args);
            return;
        }
        Some("mcp-a2a") => {
            run_mcp_a2a();
            return;
        }
        _ => {}
    }

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

/// Runs the `mcp-a2a` stdio MCP server (the A2A sending side, feature `a2a`).
#[cfg(feature = "a2a")]
fn run_mcp_a2a() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    if let Err(e) = rt.block_on(async {
        let server =
            kubeadp_agent::mcp_a2a::build_server(std::env::var("A2A_KUBE_API").ok()).await?;
        kubeadp_agent::mcp_a2a::run_stdio(server).await
    }) {
        eprintln!("kubeadp-agent (mcp-a2a): {e:#}");
        exit(1);
    }
}

/// Non-a2a build: the subcommand is not compiled in.
#[cfg(not(feature = "a2a"))]
fn run_mcp_a2a() {
    eprintln!("kubeadp-agent: the 'mcp-a2a' subcommand requires the 'a2a' cargo feature");
    exit(2);
}

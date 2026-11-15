//! kubeadp-agent: role dispatcher and subcommand router.
//!
//! Selects the long-lived role from the `KUBEADP_ROLE` environment variable
//! or a subcommand (`orchestrator` / `worker`):
//! - `orchestrator` -> the agent orchestrator process supervisor
//! - `worker`       -> the agent-run API daemon (remote job execution)
//!
//! The `mcp-a2a` subcommand (first argument) runs the A2A **sending** side as
//! a stdio MCP server (feature `a2a`) and does not require `KUBEADP_ROLE`.
//!
//! The `a2a-hook` subcommand (first argument, feature `a2a`) is the hook
//! entry of the A2A injection framework (design HL-113 §7.14):
//! `a2a-hook <PostToolUse|UserPromptSubmit|Stop|pre-invocation|post-invocation|stop>`
//! reads the run's durable A2A inbox (`KUBEADP_A2A_INBOX_DIR`), marks the
//! queued messages injected and prints the hook JSON that puts them into the
//! model's context.
//!
//! The `repo-sync` subcommand (first argument) runs the internal git-sync
//! replacement ([`kubeadp_agent::core::reposync`]) for all configured
//! repositories; it does not require `KUBEADP_ROLE`.
//!
//! The `relay-inner` / `relay-connect` subcommands (first argument) run the
//! in-sandbox loopback relay and the ssh ProxyCommand helper for the opt-in
//! `net: relay` sandbox policy (see [`kubeadp_agent::sandbox::relay`]); they
//! do not require `KUBEADP_ROLE`.
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

fn run_orchestrator(args: &[String]) -> ! {
    apply_config_for("orchestrator");
    kubeadp_agent::orchestrate::entry::run_with_args(args);
    exit(0);
}

fn run_worker() -> ! {
    apply_config_for("worker");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    if let Err(e) = rt.block_on(kubeadp_agent::worker::entry::run()) {
        eprintln!("kubeadp-agent (worker): {e:#}");
        exit(1);
    }
    exit(0);
}

fn main() {
    // Subcommands handled before the role dispatch: explicit, self-describing
    // entries that need no KUBEADP_ROLE.
    let first_arg = std::env::args().nth(1);
    match first_arg.as_deref() {
        Some("orchestrator") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            run_orchestrator(&args);
        }
        Some("worker") => {
            run_worker();
        }
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
        Some("relay-inner") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            kubeadp_agent::sandbox::cli::run_relay_inner(&args);
            return;
        }
        Some("relay-connect") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            kubeadp_agent::sandbox::cli::run_relay_connect(&args);
            return;
        }
        Some("mcp-a2a") => {
            run_mcp_a2a();
            return;
        }
        #[cfg(feature = "a2a")]
        Some("a2a-hook") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            let code = kubeadp_agent::core::a2a::inject::run_a2a_hook(&args);
            exit(code);
        }
        Some("repo-sync") => match run_repo_sync() {
            Ok(code) => exit(code),
            Err(e) => {
                eprintln!("kubeadp-agent (repo-sync): {e:#}");
                exit(1);
            }
        },
        _ => {}
    }

    match std::env::var("KUBEADP_ROLE").as_deref() {
        Ok("orchestrator") => {
            let args: Vec<String> = std::env::args().skip(1).collect();
            run_orchestrator(&args);
        }
        Ok("worker") => {
            run_worker();
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
        let kube_api = {
            #[cfg(feature = "test-hooks")]
            {
                std::env::var("A2A_KUBE_API").ok()
            }
            #[cfg(not(feature = "test-hooks"))]
            {
                None
            }
        };
        let server = kubeadp_agent::mcp_a2a::build_server(kube_api).await?;
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

/// Runs the `repo-sync` subcommand (the internal git-sync replacement, see
/// [`kubeadp_agent::core::reposync`]). Arguments: `--once` runs one cycle
/// over every configured repo (exits 1 on failure); `--loop` (the default)
/// runs the periodic loop. Exits 0 on success.
fn run_repo_sync() -> Result<i32, anyhow::Error> {
    use kubeadp_agent::core::reposync;
    let mut once = false;
    for arg in std::env::args().skip(2) {
        match arg.as_str() {
            "--once" => once = true,
            "--loop" => once = false,
            other => {
                anyhow::bail!("unknown repo-sync argument '{other}' (expected --once or --loop)")
            }
        }
    }
    let cfg = reposync::load_config()?;
    if once {
        reposync::run_once(&cfg)?;
        Ok(0)
    } else {
        reposync::run_loop(&cfg)?;
        Ok(0)
    }
}

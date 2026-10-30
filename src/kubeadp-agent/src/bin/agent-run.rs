//! agent-run: remote CLI client for the agent-run API server.
//!
//! Parses the command line, dispatches each command to the agent-run API
//! server over HTTP (ureq), and renders the server's `CommandOutput` with
//! the engine's own renderer. Flags and behaviour are unchanged from the old
//! `agent-run-cli` crate.

use clap::Parser;
use std::process::exit;

use kubeadp_agent::cli::{client, Cli};

fn main() {
    let cli = Cli::parse();

    // The shim is a pure remote client with no local execution engine, so a
    // reachable API server URL is mandatory (there is no local fallback).
    let Some(remote_url) = &cli.global_opts.remote else {
        eprintln!(
            "Error: agent-run is a remote-only client: --remote <URL> (or AGENT_RUN_REMOTE) is required.\n\
             Example: agent-run --remote http://agent-run-api.opencode.svc:8002 start <project> <prompt>"
        );
        exit(1);
    };

    client::dispatch(&cli, remote_url);
}

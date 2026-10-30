//! Thin wrapper that runs the agent-run-api worker role directly, kept so
//! the existing Dockerfiles and manifests keep working until phase 2.

use kubeadp_agent::config_file::{apply_to_env, config_file_map};
use std::process::exit;

fn main() {
    if let Err(e) = config_file_map().map(|map| apply_to_env(&map, &["common", "worker"])) {
        eprintln!("agent-run-api: configuration error: {e}");
        exit(1);
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    if let Err(e) = rt.block_on(kubeadp_agent::worker::entry::run()) {
        eprintln!("agent-run-api: {e:#}");
        exit(1);
    }
}

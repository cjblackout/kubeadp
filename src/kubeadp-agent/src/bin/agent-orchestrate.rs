//! Thin wrapper that runs the agent-orchestrate role directly, kept so the
//! existing Dockerfiles and manifests keep working until phase 2.

use kubeadp_agent::config_file::{apply_to_env, config_file_map};

fn main() {
    if let Err(e) = config_file_map().map(|map| apply_to_env(&map, &["common", "orchestrator"])) {
        eprintln!("agent-orchestrate: configuration error: {e}");
        std::process::exit(1);
    }
    kubeadp_agent::orchestrate::entry::run();
}

//! Thin wrapper that runs the agent-orchestrate role directly, kept so the
//! existing Dockerfiles and manifests keep working until phase 2.

fn main() {
    kubeadp_agent::orchestrate::entry::run();
}

# Development

This document covers source repository layout, compilation workflows, test suite execution, and documentation maintenance rules for agent-run.

## Repository Layout

The system is organized into three Rust crates located under `src/`:

- `src/kubeadp-agent/src/cli/` (package `src/kubeadp-agent`): Pure HTTP client shim (`agent-run`). Contains integration and black-box test harnesses in `tests/`.
- `src/kubeadp-agent/src/core/`: Common execution library containing Git worktree logic, process runners, SQLite persistence, activity monitoring, Hindsight client code, and (behind the `server` cargo feature) the axum-based HTTP server module (`agent_core::server`) with its router and SSE handlers.
- `src/kubeadp-agent/src/worker/`: HTTP API daemon (`agent-run-api`). Enables the `server` feature of `agent-core` and re-exports `agent_core::server`; it adds server initialization, config resolution, and zombie reaping.

Deployment and infrastructure definitions:
- `docker/agent-run-api/`: Container build definition.
- `apps/kubeadp-agent-worker/manifest.yaml`: Kubernetes Deployment, Service, PVCs, and CiliumNetworkPolicy.

## Source Build

Build both binaries locally:

```bash
# Debug builds
cargo build --manifest-path src/kubeadp-agent/Cargo.toml
cargo build --manifest-path src/kubeadp-agent/Cargo.toml

# Release builds
cargo build --release --manifest-path src/kubeadp-agent/Cargo.toml
cargo build --release --manifest-path src/kubeadp-agent/Cargo.toml
```

## Test Suites

### Complete Verification Suite
Run all crate unit tests followed by the black-box end-to-end suite:

```bash
bash src/kubeadp-agent/tests/cli/run-all.sh
```

### Unit Tests Only
Run standard Cargo test harnesses across all crates:

```bash
cargo test --manifest-path src/kubeadp-agent/Cargo.toml
cargo test --manifest-path src/kubeadp-agent/Cargo.toml
cargo test --manifest-path src/kubeadp-agent/Cargo.toml
```

### Black-Box Integration Tests
The suite in `src/kubeadp-agent/tests/cli/run-tests.sh` spins up an ephemeral `agent-run-api` instance on `127.0.0.1:18002` backed by a fake `agy` implementation (`fake-agy.sh`). It verifies full lifecycle behavior without requiring external network access or real credentials.

```bash
# Run black-box suite
bash src/kubeadp-agent/tests/cli/run-tests.sh

# Run specific tests with retained test artifacts
ONLY="T02 T07" KEEP=1 bash src/kubeadp-agent/tests/cli/run-tests.sh
```

Supported test environment variables:
- `BIN_DIR`: Directory containing prebuilt `agent-run` and `agent-run-api` binaries (skips cargo compilation).
- `PORT`: Port for the private mock API server (default 18002).
- `KEEP=1`: Retains temporary workspace, database, and log files upon test exit.
- `ONLY`: Space-separated list of test IDs to run (e.g. `ONLY="T01 T03"`).

## Documentation Synchronization Contract

Any pull request or commit modifying command-line options, flag arguments, defaults, HTTP API routes, request/response bodies, or error codes must atomically update:
1. The CLI `--help` strings in `src/kubeadp-agent/src/cli/cli.rs` and `src/kubeadp-agent/src/core/args.rs`.
2. The router and handlers in `src/kubeadp-agent/src/core/server.rs` (re-exported by `agent-run-api` via `src/kubeadp-agent/src/worker/server.rs`).
3. The reference documentation in `docs/agent-run/cli-reference.md` and `docs/agent-run/api-reference.md`.

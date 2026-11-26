# Installation

This document specifies build requirements, container image composition, Kubernetes deployment manifests, and agent authentication procedures for agent-run.

## Contents
- [Requirements](#requirements)
- [Source Build](#source-build)
- [Container Image](#container-image)
- [Kubernetes Deployment](#kubernetes-deployment)
- [agy Authentication](#agy-authentication)
- [Client Installation](#client-installation)
- [Upgrade Procedures](#upgrade-procedures)

## Requirements

- **Toolchain**: Rust stable compiler (edition 2021) and Cargo.
- **System libraries**: C compiler (`cc`), `pkg-config`, and `libsqlite3` development headers.
- **Runtime platform**: Linux x86_64 with `procfs` (`/proc/<pid>/stat`, `/proc/<pid>/cmdline`) and `prctl(PR_SET_CHILD_SUBREAPER)` support.
- **External tools**: Git 2.30+, `agy` executable, and optional `hindsight` CLI.

## Source Build

Build release artifacts for both crates:

```bash
# Build CLI client shim
cargo build --release --manifest-path src/kubeadp-agent/Cargo.toml

# Build API server daemon
cargo build --release --manifest-path src/kubeadp-agent/Cargo.toml
```

Compiled binaries:
- Client: `src/kubeadp-agent/target/release/agent-run`
- Server: `src/kubeadp-agent/target/release/agent-run-api`

## Container Image

The server container is defined in `docker/agent-run-api/Dockerfile` and published as `ghcr.io/cjblackout/kubeopencode/agent-run-api:<TAG>`.

Contents:
- Base system: Debian/Ubuntu Linux containing `git`, `curl`, `ca-certificates`, `procps`, `gcc`, and `libc6-dev`.
- Agent binary: `agy` installed at `/usr/local/libexec/agy`.
- Server binary: `agent-run-api` placed at `/usr/local/bin/agent-run-api`.
- Shared tools: Mounted at runtime from `ghcr.io/cjblackout/kubeopencode/modules:<TAG>` into `/opt/tools`.

## Kubernetes Deployment

The production daemon runs in namespace `opencode` via `apps/kubeadp-agent-worker/manifest.yaml`.

Key manifest specifications:
- **Runtime class**: `runtimeClassName: kata` (sandboxed VM isolation on home-region amd64 nodes).
- **Security context**: `runAsUser: 1000`, `runAsGroup: 1000`, `fsGroup: 1000`.
- **Termination grace**: `terminationGracePeriodSeconds: 60` (allows `AGENT_RUN_SHUTDOWN_GRACE` job drain).
- **Resources**: CPU request 250m, limit 3; memory request 512Mi, limit 4Gi.
- **Volumes**:
  - `kubeadp-agent-worker-home-bw`: 5Gi RWO PVC mounted at `/home/agent`.
  - `kubeadp-agent-worker-workspace-bw`: 50Gi RWO PVC mounted at `/workspace`.
  - `tools`: Read-only OCI image volume mounted at `/opt/tools`.
- **repo-sync sidecar**:
  - Repositories: `nested-operator`, `kubeopencode`, and `project-a` synced from GitHub into `/workspace/.roots/<project>` with symlinks to `/workspace/<project>`.
  - Credentials: `gh-kubeoc-token` secret (read-only token; Git push is prohibited).
- **Network policy**: `CiliumNetworkPolicy` restricts ingress on port 8002 to the `ai-server` pod (label and ServiceAccount); the worker API is independently authenticated (operator bearer or jobset identity on /api/v1, jobset secret + proof on A2A).

## agy Authentication

The `agy` CLI requires OAuth credentials stored under `/home/agent/.gemini/antigravity-cli`.

### Interactive OAuth Flow
1. Start authentication:
   ```bash
   agent-run auth
   ```
   The command queries `GET /api/v1/auth/status`. If unauthenticated, it invokes `POST /api/v1/auth` and outputs the OAuth login URL.
2. Complete verification:
   Visit the URL in a browser, grant permissions, and copy the authorization code.
3. Submit verification code:
   Enter the code into stdin when prompted, or pass directly:
   ```bash
   agent-run auth --code <AUTHORIZATION_CODE>
   ```
   The client relays the code via `POST /api/v1/auth/code`, and `agy` writes credential files directly to the server home volume.

### Flow Management
- **Reset parked flow**: If an auth flow is abandoned, clear the server slot:
  ```bash
  agent-run cancel
  ```
- **Automated setup**: Ensure directories exist and extract secret tokens from server environment:
  ```bash
  agent-run init --from-env SECRET_KEY --to /home/agent/.secrets/key.txt
  ```

## Client Installation

Copy the compiled `agent-run` binary to a path in `$PATH`:

```bash
install -m 0755 src/kubeadp-agent/target/release/agent-run /usr/local/bin/agent-run
```

Ensure `AGENT_RUN_REMOTE` points to the cluster service or gateway ingress URL.

## Upgrade Procedures

1. Build and publish a new container image via CI workflow `.github/workflows/build-agent-run-api.yml`.
2. Update the image tag in `apps/kubeadp-agent-worker/manifest.yaml`.
3. Apply the manifest through GitOps reconciliation. The Deployment uses `strategy: Recreate` due to RWO volume attachments.
4. Update local client binaries if command arguments or wire schemas have changed.

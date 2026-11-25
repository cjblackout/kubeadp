# kubeadp-agent image

One container image that replaces the two images built today from the same
cargo package (`docker/ai-server/Dockerfile` for the orchestrator pod,
`docker/agent-run-api/Dockerfile` for the worker pod). Built by
`.github/workflows/build-kubeadp-agent.yml` and pushed to
`ghcr.io/cjblackout/homelab/kubeadp-agent` (public GHCR, like the other
own-build images).

> Consumers: both agent Deployments in namespace `opencode` run this image: `kubeadp-agent-worker`
> (apps/kubeadp-agent-worker, `KUBEADP_ROLE=worker`, gVisor on bwcloud-worker1) and
> `kubeadp-agent-orchestrator` (apps/ai-server, `KUBEADP_ROLE=orchestrator`, Kata on a home node); both take the
> tag from the shared `kubeadp-agent` key in `versions/versions.env`, and `build-kubeadp-agent.yml` moves it on
> main. PR builds push an rc tag only, used for throwaway test pods (apps/kubeadp-agent-test).

## What is inside

The union of both old images, pinned the same way each old image pinned its
parts:

- The binaries of `src/kubeadp-agent` built with `--features a2a`:
  - `kubeadp-agent` — the role dispatcher; role chosen by `KUBEADP_ROLE`
    (`orchestrator` or `worker`) or subcommand (`orchestrator` / `worker`); unset or any other value exits with code 2.
  - `agent-run` — the remote CLI client.
- Both harness engines:
  - claude-code (npm `@anthropic-ai/claude-code`) plus its node runtime, built
    exactly as `docker/ai-server/Dockerfile`'s `tools-builder` stage does;
  - agy at the core's default `agy_path` `/usr/local/libexec/agy`, installed
    as `docker/agent-run-api/Dockerfile` does.
- ttyd `1.7.7`, pinned with its release checksum, as in `docker/ai-server/` —
  the orchestrator pod runs its interactive OAuth shell as a ttyd sidecar from
  this same image.
- Runtime packages: the union of both images' lists
  (`git curl ca-certificates procps rsync gcc libc6-dev make pkg-config`, plus `openssh-client patch zsh less psmisc wget nano vim-tiny htop git-lfs bash-completion` that the old ai-server image had), tini
  as PID 1, and `agent` (uid/gid 1000, `/bin/bash`) as the container user.
- The toolchains/CLIs are **not** baked in: they come from the read-only tools
  image volume `/opt/tools` (`docker/modules`), mounted by the pod manifests as
  today. The CA bundle (`SSL_CERT_FILE`/`REQUESTS_CA_BUNDLE`/
  `NODE_USE_SYSTEM_CA`) and the `/opt/tools` PATH entries are set in the pod
  manifests, exactly as for the old images.

## Base image

`debian:bookworm-slim`. The two old images disagreed (ai-server:
`codercom/code-server:4.138.0`, agent-run-api: `debian:bookworm-slim`);
`debian:bookworm-slim` is the code-server base too, it is what the worker runs
today, and nothing the orchestrator pod needs comes from code-server itself
(ttyd replaces code-server; claude/node are staged from the node base; tools
come from `/opt/tools`). The code-server uid/gid 1000 layout is reproduced by
the `agent` user, so the pod-facing `runAsUser/runAsGroup/fsGroup: 1000` of
both manifests is unchanged.

## Roles

One deployment per role, both from this image; `KUBEADP_ROLE` picks the role:

| Role | Deployment | Binary path | Notes |
| :--- | :--- | :--- | :--- |
| `orchestrator` | `kubeadp-agent-orchestrator` | `kubeadp-agent` → `orchestrate::entry` | claude engine + ttyd sidecar for the interactive shell; home/workspace PVCs as today |
| `worker` | `kubeadp-agent-worker` | `kubeadp-agent` → `worker::entry` | agy engine; listener/client env as the old agent-run-api pod |

Process model under the ENTRYPOINT (`tini -- kubeadp-agent`): tini is invoked
without `-g`, matching the signal contract both old pods documented; the worker
role's `agent-run-api` server still sets `PR_SET_CHILD_SUBREAPER` at startup, so
agy's session/child handling is unchanged from when the server was its own PID 1.

## Agent-to-Agent (A2A) Messaging Security & Ingress

The image binaries are compiled with `--features a2a`.
Incoming A2A messages are validated and framed before reaching session sinks:
- **Size limit**: Message text exceeding 20 KB (after trimming) is rejected with `A2AError::invalid_params("message text is too large")`.
- **Hop limit**: Metadata `a2a.hop` is parsed (defaulting to 0 if absent; non-numeric, negative or exceeding `MAX_HOP` maps to `MAX_HOP + 1`). If `hop > MAX_HOP` (where `MAX_HOP = 4`), the message is rejected before delivery to the sink.
- **Claimed sender & untrusted data framing**: Metadata `a2a.sender` is claimed, never authenticated. It is sanitized (control characters and newlines stripped, capped at 64 characters; defaults to `"unknown"` if absent or empty). Messages without any metadata are also framed with claimed sender `"unknown"` and hop 0. The incoming content is enclosed in a delimiter notice explicitly marking it as untrusted data from another agent that must not override owner instructions.
- **Access control**: In-cluster CiliumNetworkPolicies restrict which peers may reach the agent; the API layer itself authenticates callers (operator bearer or jobset identity on `/api/v1`, jobset secret + proof on A2A), so the network rules are not the only gate.

## How it is built

The workflow runs a `check` Docker stage (cargo fmt, clippy `-D warnings` for
the default, `a2a,test-hooks`, `cli`-only and `server`-only feature sets, and
the `a2a,test-hooks` test suite), builds the heavy stages one at a time
(`cli-tools`, `agent-builder`, `tools-builder`) then the final image, runs the
smoke test (`docker/kubeadp-agent/smoke-test.sh`, mounted read-only) before the
push, pushes to GHCR, tags `kubeadp-agent-<version>`, and on `main` commits the
new tag to the `kubeadp-agent` key in `versions/versions.env` (no manifest
consumes the image yet, so that is the only deploy-side change).

Version numbering follows the other own-build images: `YYYY.MM.<X>` with
`-rc<N>` prereleases; the Major in `src/kubeadp-agent/VERSION` appears in the rc
tags only. The `kubeadp-agent` key in `versions/versions.env` starts at the
placeholder `0000.0.0` and is rewritten by the first on-main build.

## Local build

```sh
git -C src/kubeadp-agent status   # repo-root build context
docker build -f docker/kubeadp-agent/Dockerfile \
  --build-arg BUILD_SHA=$(git rev-parse HEAD) -t kubeadp-agent .
```

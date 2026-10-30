# kubeadp-agent image

One container image that replaces the two images built today from the same
cargo package (`docker/ai-server/Dockerfile` for the orchestrator pod,
`docker/agent-run-api/Dockerfile` for the worker pod). Built by
`.github/workflows/build-kubeadp-agent.yml` and pushed to
`ghcr.io/cjblackout/homelab/kubeadp-agent` (public GHCR, like the other
own-build images).

> Nothing consumes this image yet: the cut-over (HL-70 phase 3) deploys test
> pods first, then the worker deployment, then the orchestrator. The two old
> images, their Dockerfiles, workflows and manifests stay untouched until then.

## What is inside

The union of both old images, pinned the same way each old image pinned its
parts:

- The four binaries of `src/kubeadp-agent`:
  - `kubeadp-agent` — the role dispatcher; role chosen by `KUBEADP_ROLE`
    (`orchestrator` or `worker`); unset or any other value exits with code 2.
  - `agent-run`, `agent-orchestrate`, `agent-run-api` — the thin wrappers, kept
    so existing commands/supervisors keep working unchanged.
- Both harness engines:
  - claude-code (npm `@anthropic-ai/claude-code`) plus its node runtime, built
    exactly as `docker/ai-server/Dockerfile`'s `tools-builder` stage does;
  - agy at the core's default `agy_path` `/usr/local/libexec/agy`, installed
    as `docker/agent-run-api/Dockerfile` does.
- ttyd `1.7.7`, pinned with its release checksum, as in `docker/ai-server/` —
  the orchestrator pod runs its interactive OAuth shell as a ttyd sidecar from
  this same image.
- Runtime packages: the union of both images' lists
  (`git curl ca-certificates procps rsync gcc libc6-dev make pkg-config`), tini
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

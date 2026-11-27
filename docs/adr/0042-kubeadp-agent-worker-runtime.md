# ADR 0042: Kubeadp-Agent Worker Runtime Integration

## Status

Accepted

## Date

2026-11-27

## Context

KubeOpenCode previously operated Agent Deployments using OpenCode's built-in server (`/tools/opencode serve`). While functional, this coupled agent lifecycle and execution to OpenCode's proprietary session model and internal server protocol.

With the evolution of the `kubeadp-agent` architecture, Agent instances transition from running `opencode serve` to hosting `kubeadp-agent worker`. The `kubeadp-agent worker` provides a robust, sandbox-aware REST and SSE API for agent execution across supported engines (including OpenCode, Claude, and Gemini engines).

## Decision

We update the Agent runtime architecture according to the following decisions:

1. **Pod Shape & Init Containers (D1)**:
   - Agent Deployments use a multi-init container pattern:
     - `kubeadp-init`: copies `/usr/local/bin/kubeadp-agent` and `/usr/local/bin/agent-run` into the shared `/tools` volume.
     - `opencode-init`: copies `/tools/opencode` binary into `/tools` so the OpenCode engine is available to the worker.
   - The worker container executes `/tools/kubeadp-agent worker`.

2. **Port & Health Probes (D2)**:
   - Default port remains `4096` for CRD backward compatibility, bound via `AGENT_RUN_API_LISTEN=0.0.0.0:4096`.
   - Liveness and startup probes use `/healthz`.
   - Readiness probe uses `/readyz`.

3. **Authentication & Secret Management (D3)**:
   - The Agent controller manages a dedicated Secret `<agent>-kubeadp` containing `api-token` and `jobset-root-key` (32-byte cryptographic random values).
   - Mounted to `/etc/kubeadp` read-only.
   - Worker environment variables configured:
     - `KUBEADP_API_TOKEN_FILE=/etc/kubeadp/api-token`
     - `KUBEADP_JOBSET_ROOT_KEY_FILE=/etc/kubeadp/jobset-root-key`
     - `KUBEADP_ROLE=worker`
     - `AGENT_RUN_WORKSPACE_ROOT=<workspaceDir>`
     - `AGENT_RUN_STATE_ROOT=/data/sessions`
     - `AGENT_RUN_OPENCODE_PATH=/tools/opencode`
     - `KUBEADP_SANDBOX_HOME=/home/agent`
     - `KUBEADP_SANDBOX_ENV_ALLOW=OPENCODE_*,<credential_env_names>`
     - `AGENT_RUN_API_LISTEN=0.0.0.0:<port>`

4. **Task Execution via task-submit (D4)**:
   - Tasks targeting an Agent (`agentRef`) invoke `kubeopencode task-submit`:
     - Submits job prompt via `POST /api/v1/jobs` with `{"project": project, "prompt": prompt, "engine": "opencode", "sandbox": false}`.
     - Streams real-time progress via `GET /api/v1/jobs/{id}/events` SSE stream.
     - Polls `/api/v1/jobs/{id}/result` until terminal status.
     - Maps terminal status (`done` -> exit 0; non-zero on failure/error).
     - Records job identifier to `/workspace/.kubeopencode/job-id`.

5. **Project Layout (D5)**:
   - Target project is derived from the first Git context name or defaults to `workspace`.

6. **Web Terminal & Reverse Proxy (D7)**:
   - Interactive Web Terminal exec launches `/bin/bash` inside the worker container.
   - API Reverse Proxy (`/proxy/*`) automatically injects the internal Bearer token from `<agent>-kubeadp` Secret.

## Consequences

- Clean separation between Kubernetes orchestration and runtime agent execution engines.
- Unified task submission protocol across all supported execution backends.

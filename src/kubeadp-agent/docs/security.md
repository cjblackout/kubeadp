# Security

This document specifies the security boundaries, network access controls, credential handling, process isolation, and hardening measures for agent-run.

## Trust Model

The `agent-run-api` server executes arbitrary code and terminal commands inside local Git worktrees via the `agy` agent running with `--dangerously-skip-permissions`. Callers with access to the API possess full execution privileges within the container environment.

## Authentication and Network Exposure

The HTTP API server is independently authenticated at the application layer: the operator bearer token or the jobset identity on the `/api/v1` route group, and the jobset secret + proof on the A2A routes. Network policy back this up, they are not the only gate.

- **Bearer token (optional)**: When a token is configured (agent-orchestrate via `AGENT_ORCHESTRATE_TOKEN` or `AGENT_ORCHESTRATE_TOKEN_FILE`), every `/api/v1/*` route -- the shared agent-run routes and the supervisor's `/api/v1/sessions` group alike -- requires `Authorization: Bearer <token>`, enforced once by a single router layer, so a new route added to the group is authenticated automatically. The comparison is constant-time and repeated failures are throttled per peer (`429 rate_limited` with `Retry-After`). The unauthenticated `/healthz` and `/readyz` probes are never gated.
- **No token for agent-run-api**: `agent-run-api` deliberately does not configure a bearer token of its own; it authenticates callers through the jobset identity (secret + proof) on the A2A routes, and its NetworkPolicy restricts who may reach it at all.
- **Network exposure**: Plaintext HTTP on port 8002. Not exposed to the LAN or public internet.
- **Cluster network policy**: Ingress to port 8002 is restricted by `CiliumNetworkPolicy` (`agent-run-api-ingress-from-ai-server`) exclusively to pods matching label `app: ai-server` in namespace `opencode` with ServiceAccount `ai-server`. The API layer itself is independently authenticated: the operator bearer token or the jobset identity on `/api/v1`, and the jobset secret + proof on the A2A routes. There is no gateway route.
- **Internal isolation**: Untrusted agent pods in the `opencode` namespace cannot access port 8002.

## Remote Control Daemon Exposure

The `remote-control` subcommand group controls the background daemon of the server's `agy` installation (`agy remote-control serve`). When active, this daemon exposes the host machine's `agy` instance—which runs with permissions auto-approved—to anyone logged in to the associated Google account on Antigravity (`https://antigravity.google.com`).

Because `agent-run-api` implements no authentication at the HTTP layer, enabling remote control is gated by the `AGENT_RUN_REMOTE_CONTROL` environment variable on the server. `POST /api/v1/remote-control/enable` (`agent-run remote-control enable`) is refused with HTTP 403 (`remote_control_disabled`) unless `AGENT_RUN_REMOTE_CONTROL` is explicitly set to `1` or `true` (case-insensitive). The `disable` and `status` operations remain available without the gate.

## Credential Handling

- **agy OAuth state**: Stored under `/home/agent/.gemini/antigravity-cli` on the persistent home volume. The client never transmits credential tokens over the HTTP API during `agent-run auth`; `agy` directly writes tokens to the server's filesystem upon receiving the authorization code.
- **Git credentials**: Read-only GitHub PAT supplied via the `GH_TOKEN` environment variable from Kubernetes secret `gh-kubeoc-token`. Git operations can clone and fetch from repositories but cannot push commits or create remote branches.
- **Environment extraction**: `agent-run init --from-env <VAR> --to <PATH>` extracts designated environment variables to disk with strict `0600` permissions.

## Isolation Mechanisms

- **Worktree isolation**: Each job operates within an independent Git worktree. Jobs cannot write to or mutate the primary project repository checkouts under `/workspace/<project>`.
- **Process groups**: Every agent process tree is launched in a new session via `setsid()`, isolating signals from the server daemon and other jobs.
- **Sandbox execution**: The `--sandbox` flag passes terminal restrictions to the underlying `agy` invocation.
- **Container boundary**: The server pod runs inside a hardware-isolated Kata Virtual Machine (`runtimeClassName: kata`), providing a kernel boundary between host and agent execution.

## Input Validation and Logs

- **Argument validation**: The server caps prompt arguments at 100,000 bytes (`PROMPT_ARGV_MAX_BYTES`) to prevent Linux `execve` `E2BIG` buffer overflow errors.
- **Path traversal prevention**: Path arguments for `discard`, `files`, and `cat` reject absolute paths, empty components, and directory escaping sequences (`..`).
- **Unvalidated parameters**: `init --from-env`/`--to` accept any environment variable name and any destination path, and `start`'s `project` is not restricted to a single path component under `workspace_root` (see limitations 9 and 10).
- **Log sanitization**: Logs in `<state_root>/logs/` capture raw process output, which may include source code and build output. Proxy credentials are automatically redacted in outputs.

## Hardening Recommendations

The following practices are recommended to mitigate risk:
1. Retain strict Kubernetes network policies blocking lateral access from other namespace workloads.
2. Maintain read-only GitHub token scopes on the server deployment.
3. Keep Kata VM isolation active to mitigate container escape risks from arbitrary code execution.
4. Periodically purge old log files containing sensitive code excerpts using `agent-run gc --purge-logs`.

# agent-run-cli

The `agent-run-cli` crate provides the `agent-run` command-line HTTP client shim for the `agent-run-api` daemon. It parses user arguments, dispatches requests over HTTP, and formats execution outputs. Build the binary with `cargo build --release --manifest-path src/agent-run-cli/Cargo.toml` and run tests with `cargo test --manifest-path src/agent-run-cli/Cargo.toml`. Complete command documentation, syntax, and options are specified in [docs/agent-run/cli-reference.md](../../docs/agent-run/cli-reference.md).

## `mcp-a2a` subcommand (A2A sending side, feature `a2a`)

`kubeadp-agent mcp-a2a` runs a stdio MCP server (no `KUBEADP_ROLE`) exposing `list_agents`, `send_message` and `get_task` for talking to peer A2A agents. Discovery is by Kubernetes Services labelled `a2a.homelab/agent=true` (own namespace + `A2A_NAMESPACES`), authenticated with the in-cluster service account; `A2A_KUBE_API` overrides the API base and `A2A_URL_OVERRIDES` redirects connections for tests/development. Messages are tagged `a2a.sender` (`A2A_SELF_NAME`) and `a2a.hop` (max 4). See `README.md`, section "`mcp-a2a` subcommand".

## A2A Ingress & Receiving Side (feature `a2a`)

When receiving messages via A2A (`ServerRequestHandler::send_message`), incoming payloads undergo strict ingress checks before delivery to the session/job sink:
- **Text size**: The size cap is applied to the WHOLE message (every text part) *before* framing, and over-limit messages are **rejected** (`A2AError::invalid_params("message text is too large")`) — never silently truncated. The cap is derived from the single PTY-delivery cap (`sessions_api::MESSAGE_MAX_BYTES`, 8 KB) minus the framing overhead, so any accepted message is guaranteed to fit the session PTY once framed.
- **Hop count**: `a2a.hop` metadata is checked (absent = 0; non-numeric, negative or huge = MAX_HOP+1). If hop > `MAX_HOP` (4), the message is rejected.
- **Untrusted data framing**: Claimed sender (`a2a.sender`) is sanitized (control chars/newlines stripped, max 64 chars, default "unknown"). The message content is wrapped in a framing block carrying the claimed sender, hop count and an explicit notice that the content is data from another agent, not an instruction from the owner. The frame delimiters carry a **per-message random nonce** so a hostile body containing the closing delimiter cannot break out of the frame and forge following text.
- **Message rate limit**: Delivery is metered by ONE sliding-window limiter shared by the REST `POST .../message` router and the A2A `SendMessage` sink, keyed by **(session, sender)** (30 messages per minute). The REST path is charged under a fixed `rest` identity, the A2A path under the claimed `a2a.sender`; a single noisy sender therefore cannot exhaust a session's budget and starve the legitimate peers of that session. A rejected message answers with `Retry-After`.
- **PTY write bound**: the deadline-bounded bracketed-paste write (`PtyInput::write_all`) waits on `poll(POLLOUT)` for a full slave input queue instead of spinning on `EAGAIN`, and gives up at a fixed deadline (`PTY_WRITE_TOTAL_DEADLINE`, 2 s) with a `TimedOut` error. This runs on the tokio blocking pool, so a stuck child that never drains its input queue cannot wedge a worker thread or the `pty_input` mutex.
- **Security model**: Metadata is claimed, never authenticated. True access control and isolation are enforced by network policy and mutual authentication.

## `repair`

Job worktrees are checkouts of a per-project **private** bare repository kept under the agent-run repos root (`<workspace_root>/.agent-repos/<project>.git` by default). When git-sync re-initialises the project checkout it can drop the `worktrees/<job_id>` **admin** directory of that private repo, leaving the worktree's `.git` file dangling: `git` commands in the worktree break until it is re-attached — the worktree is "orphaned". `repair` re-attaches it (recreating the admin directory at the job's recorded base ref); it is a no-op that reports `healthy` when the worktree is already attached.

Orphaned worktrees are also re-attached automatically, silently, at the start of the server-side `status`, `diff`, `commit`, `retry`, `files`, `cat`, `review`, and `gc` commands, so a one-off `repair` is only needed to verify or force the fix.

```bash
agent-run repair <JOB_ID>     # re-attach one job's worktree
agent-run repair --all         # sweep every job, repairing only the dangling ones
```

Output is one line per job: `repaired <id>`, `healthy <id>`, or `failed <id>: <reason>`. Exit status: `0` normally (and in `--all` when nothing failed), `1` when the single target is missing/unrepairable or when `--all` had at least one failure, `1` for API errors (e.g. `404 worktree_not_found` on a deleted worktree, `400 repair_external_worktree` for a `--from-worktree` job).

## Sandbox (`sandbox-exec` and `sandbox-probe`)

Per-agent process isolation ("sandbox") provides container-level isolation between agents running inside the same pod. It enforces SANDBOX CONTRACT v1 without requiring pod-level security context or capability changes.

### Commands

- `kubeadp-agent sandbox-probe [--json]`
  Probes host environment and container runtime mechanisms:
  - Runtime detection: `gvisor`, `kata`, `runc`, or `native`.
  - Kernel and mechanism checks: user namespace, PID namespace, fresh procfs mount, tmpfs mount, bind mount, Landlock ABI version (0 = unavailable), Landlock scopes, and child subreaper status.
  - Chosen backend.

- `kubeadp-agent sandbox-exec --policy <file|-> -- <cmd> [args...]`
  Executes `<cmd> [args...]` under the specified sandbox policy. Isolation is always active; runtime is auto-detected inside the pod. If no backend works on the pod's runtime, execution fails with exit code `125` and the command never runs.
  - Exit codes:
    - Target command's exit code is propagated.
    - Death by signal N propagates as `128 + N`.
    - `125`: sandbox setup failure or unsupported backend.
    - `126` / `127`: command not executable / not found.

### Job Status and Info Output

Job status (`agent-run status <id>`) and info (`agent-run info`) JSON outputs include a `sandbox` object:
```json
"sandbox": {
  "backend": "gvisor",
  "runtime": "gvisor",
  "policy_path": "/home/agent/.agent-run/logs/<id>.policy.json"
}
```
`agent-run status` and `agent-run info` print the active sandbox backend, runtime, and policy path.

**Trusted orchestrator sessions.** An orchestrator session configured with `"sandbox": false` (per session, in the supervisor config; default `true`) is NOT wrapped in `sandbox-exec`: it runs with the pod's full environment, `/workspace` and the ServiceAccount token. The kata backend never grants that token to a sandboxed agent, so this is the only way for a session to use `kubectl` as the pod's ServiceAccount. In `apps/ai-server` only the `homelab` session is trusted; `nested-operator` and `kubeopencode` stay sandboxed. Worker job sandboxing is unaffected.

A test-only launch seam that skips the `sandbox-exec` wrapper for fake-`agy` unit tests (so they run on a platform that cannot isolate) exists only in builds compiled with the `test-hooks` cargo feature and is absent from production builds.

### Policy Format

JSON document supplied via file or `-` (stdin):
```json
{
  "agent_id": "job-7",
  "jobset_id": "js-1",
  "rw": ["/workspace/.agent-jobs/homelab/ab12"],
  "ro": ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc", "/opt/tools", "/workspace/homelab"],
  "shared_rw": ["/home/agent"],
  "hide": ["/workspace/.agent-jobs"],
  "tmp": "auto",
  "secret": {"fd": 3, "dest": "secret"},
  "env_allow": ["PATH", "HOME", "LANG", "TERM", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "NODE_USE_SYSTEM_CA", "A2A_*", "KUBEADP_*"],
  "net": "shared"
}
```

### Backends

- **`gvisor`**: Unprivileged user (`1000:1000`), PID, Mount, IPC, and UTS namespaces. Mounts a fresh `/proc` (hiding processes of other agents), private tmpfs on `/tmp` and `/dev/shm`, overmounts `hide` directories with tmpfs, and re-binds the agent's own directories. PID 1 init shim reaps adopted zombies, forwards SIGTERM/SIGINT, and kills all residual descendants upon exit.
- **`kata`**: Landlock domain per agent. Kata cannot mount a fresh `/proc`, so Landlock is the only scoping available and the backend REQUIRES Landlock ABI >= 6 (the first ABI with the signal and abstract-unix-socket scopes); if the kernel offers less, the launch fails closed (exit `125`) instead of running with weaker isolation. What the policy allows (default-deny, only listed paths are reachable): RW on `rw`/`shared_rw`/the private tmp; RO on `ro` plus `/proc` and `/dev` (read-only) and `/dev/null`, `/dev/zero`, `/dev/urandom`, `/dev/tty` (read/write); the `hide` parent is simply not allowed. It never grants (fails closed if a policy lists them) the Kubernetes ServiceAccount token dir `/var/run/secrets/kubernetes.io` or a `hide` entry that is a git repository. Access flags are chosen by path type (regular files get file-only flags, directories get the directory set). Landlock signal & abstract-unix-socket scopes are applied. Process tree supervision uses `PR_SET_CHILD_SUBREAPER` with tree-wide termination within 5 seconds of agent exit.
- **`runc`**: Probe-driven backend that reports and enforces exactly the isolation the pod's probes confirm -- never more. It applies a single primitive, not a stack: a fresh-`/proc` user namespace (gVisor-style, no Landlock applied) when available, otherwise the Landlock domain with subreaper supervision (no namespaces). The chosen mechanism is reported precisely in `sandbox-probe` / the `sandbox` field. If neither primitive is available it fails closed (exit `125`) rather than running the agent without isolation.


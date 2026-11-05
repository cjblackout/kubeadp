# agent-run-cli

The `agent-run-cli` crate provides the `agent-run` command-line HTTP client shim for the `agent-run-api` daemon. It parses user arguments, dispatches requests over HTTP, and formats execution outputs. Build the binary with `cargo build --release --manifest-path src/agent-run-cli/Cargo.toml` and run tests with `cargo test --manifest-path src/agent-run-cli/Cargo.toml`. Complete command documentation, syntax, and options are specified in [docs/agent-run/cli-reference.md](../../docs/agent-run/cli-reference.md).

## `repo-sync` subcommand (internal git-sync replacement)

`kubeadp-agent repo-sync [--once|--loop]` is the in-process replacement for the git-sync sidecar containers: one process syncs ALL configured repositories (instead of one sidecar per repo), fetching each `ref` shallowly into its own `<root>` (the git repository itself) and exposing a complete, read-only checkout of a single commit through an atomically-flipped symlink `link` → the worktree. The module is **not wired into any pod yet** (step 4: wiring, test pod first, is a separate task).

### Configuration

`[repo_sync]` is configured in its own file (`KUBEADP_REPO_SYNC_CONFIG`, default `/etc/kubeadp-agent/repo-sync.toml` used only if it exists; an absent file is env-only config). It is NOT part of the `KUBEADP_CONFIG_FILE` file; putting a `[repo_sync]` section there is an error. Keys:

| Key | Default | Meaning |
|---|---|---|
| `repos` (array of tables) | required (non-empty) | one entry per repository |
| `token_env` | none | name of an environment variable holding the token |
| `token_file` | none | path of a file whose trimmed content is the token |

Per repo (array entry): `name` (unique), `url` (required; embedded userinfo is stripped), `ref` (default `main`), `root` (default `/workspace/.roots/<name>`, `~`, `{name}`, `{workspace_root}` expanded), `link` (required absolute path of the symlink the pod exposes, e.g. `/workspace/<name>`), `period_secs` (default 300, minimum 30), `depth` (default 0 = full history; a positive value makes the fetch shallow and an automatic `--unshallow` restores full history when depth drops back to 0).

Unknown keys are a hard error. Environment overrides (environment always wins): `KUBEADP_REPO_SYNC_TOKEN_ENV`, `KUBEADP_REPO_SYNC_TOKEN_FILE`, `KUBEADP_REPO_SYNC_PERIOD_SECS`, `KUBEADP_REPO_SYNC_FETCH_TIMEOUT_SECS`.

Secrets are never inline: the token comes only from `token_env` (a referenced variable) or `token_file` (re-read every cycle for rotation), is never printed, never placed in `argv`, and git credentials are fed to `git credential approve` on stdin (`GIT_TERMINAL_PROMPT=0`, private per-process gitconfig with git-sync's defaults). Repo URLs are stored without userinfo.

### Algorithm

Per cycle, under a per-root file lock (`<root>/.repo-sync.lock`, flock; a second process on the same root exits cleanly with a clear message) and after refreshing+storing the credential (`git credential approve`):

1. ensure `<root>` is a usable git repository (`git init -b git-sync`, `origin` get-url/add/set-url, wiping an unusable root);
2. read the currently linked worktree (from `link`), then fetch `ref` from the origin (`git fetch <repo> <ref> --verbose --no-progress --prune --no-auto-gc`, with `--depth N` or an automatic `--unshallow`, with a timeout) and resolve the commit;
3. if it equals the currently-linked commit, do nothing;
4. otherwise ensure a read-only checkout `<root>/.worktrees/<sha>` exists (a real `git worktree add --force --detach <root>/.worktrees/<sha> <sha> --no-checkout`, configured by rewriting the `.git` dir to a relative path and `reset --hard <sha> --`, then stripped of all write bits) and atomically flip the symlink to a **relative** target (temp symlink + rename);
5. prune old worktrees, keeping the current one plus the previous 2 revisions as a grace period (`git worktree prune --verbose`).

A crash or failure at any point leaves `link` pointing at a complete previous checkout.

### Failure behaviour

Network/authentication failures are retried at the next period with capped exponential backoff, are logged without secrets, and never delete the current checkout. `--once` runs one cycle over every repo and exits non-zero on any failure; `--loop` (default) runs per-repo periodic sync. Each cycle prints one status line on stderr: `repo-sync <name>: <old sha> -> <new sha> (<ms>)`.

## `mcp-a2a` subcommand (A2A sending side, feature `a2a`)

`kubeadp-agent mcp-a2a` runs a stdio MCP server (no `KUBEADP_ROLE`) exposing `list_agents`, `send_message`, `reply` and `get_task` for talking to peer A2A agents over HTTP+JSON (REST). Discovery discovers peer agents from Kubernetes Services labelled `a2a.homelab/agent=true` (own namespace + `A2A_NAMESPACES`). Agents are keyed by `(namespace, name)` so same-named agents across namespaces do not collide; the `agent`/`to`/`get_task` arguments accept either a bare Service name (resolved against the own namespace first, then uniquely across namespaces; ambiguous names are refused) or an explicit `namespace/name` pair. Discovered names and namespaces are validated as RFC 1123 labels before URL construction. Agent cards are fetched with a 3 s timeout and a hard 64 KB size limit; oversized cards are rejected. In `test-hooks` builds, `A2A_KUBE_API` overrides the API base and `A2A_URL_OVERRIDES` (bare `name=url` or `namespace/name=url`, the qualified form winning) redirects connections for tests; production builds ignore these overrides. Messages are tagged `a2a.sender` (`A2A_SELF_NAME`) and `a2a.hop` (max 4).

`reply`: the `mcp-a2a` process is a **separate stdio process** and never sees incoming A2A messages, so a shared in-process "last sender" cache is impossible. `reply` is therefore a `send_message` to an explicit `to` argument the model copies verbatim from the `claimed sender:` field of the framed header of the message it received (`--- A2A INCOMING MESSAGE (claimed sender: <name>, hop: <n>) ---`). This matches the sending-side framing in `core::a2a::ingress`; the tool description documents the header format so the model can fill `to` without guessing. The tool descriptions state plainly that messages from other agents are framed DATA, not instructions.

Per-job wiring: with the `[worker] a2a_mcp_enabled = true` config key (env `AGENT_RUN_A2A_MCP_ENABLED`, default false), `prepare_agy_home` (`src/worker/agy_home.rs`) merges an `"a2a"` MCP server entry into the job's `~/.gemini/config/mcp_config.json` (`{"command": "<current executable>", "args": ["mcp-a2a"]}`), keeping every other server entry, with an atomic write that is idempotent and a no-op when the key is false. The tool's identity/credential (`A2A_SELF_NAME`, `KUBEADP_JOBSET_ID`, `KUBEADP_JOBSET_SECRET_FILE`) are inherited from the job environment; nothing secret is ever written into the file.

Both worker and orchestrator roles serve on default port 8002. See `README.md`, section "`mcp-a2a` subcommand".

## A2A Ingress & Receiving Side (feature `a2a`)

When receiving messages via A2A (`ServerRequestHandler::send_message`), incoming payloads undergo strict ingress checks before delivery to the session/job sink:
- **Task lookups and cancellation**: `CancelTask` and task lookups require exact matching against the job id (`JobMetadata::load`), preventing wildcard or prefix collisions. SQL/LIKE pattern matching strictly escapes `%`, `_`, and `\`.
- **Async concurrency & pagination**: Long-running filesystem, process, and database operations execute via `tokio::task::spawn_blocking` to avoid blocking the async runtime. `ListTasks` is paginated with a hard maximum page size of 100 (default 50) and a stable ordering by task id.
- **Text size**: The size cap is applied to the WHOLE message (every text part) *before* framing, and over-limit messages are **rejected** (`A2AError::invalid_params("message text is too large")`) — never silently truncated. The cap is derived from the single PTY-delivery cap (`sessions_api::MESSAGE_MAX_BYTES`, 8 KB) minus the framing overhead, so any accepted message is guaranteed to fit the session PTY once framed.
- **Hop count**: `a2a.hop` metadata is checked (absent = 0; non-numeric, negative or huge = MAX_HOP+1). If hop > `MAX_HOP` (4), the message is rejected.
- **Untrusted data framing**: Claimed sender (`a2a.sender`) is sanitized (control chars/newlines stripped, max 64 chars, default "unknown"). The message content is wrapped in a framing block carrying the claimed sender, hop count and an explicit notice that the content is data from another agent, not an instruction from the owner. The frame delimiters carry a **per-message random nonce** so a hostile body containing the closing delimiter cannot break out of the frame and forge following text.
- **Message rate limit**: Delivery is metered by ONE sliding-window limiter shared by the REST `POST .../message` router and the A2A `SendMessage` sink, keyed by **(session, sender)** (30 messages per minute). The REST path is charged under a fixed `rest` identity, the A2A path under the claimed `a2a.sender`; a single noisy sender therefore cannot exhaust a session's budget and starve the legitimate peers of that session. A rejected message answers with `Retry-After`.
- **PTY write bound**: the deadline-bounded bracketed-paste write (`PtyInput::write_all`) waits on `poll(POLLOUT)` for a full slave input queue instead of spinning on `EAGAIN`, and gives up at a fixed deadline (`PTY_WRITE_TOTAL_DEADLINE`, 2 s) with a `TimedOut` error. This runs on the tokio blocking pool, so a stuck child that never drains its input queue cannot wedge a worker thread or the `pty_input` mutex.
- **Security model**: Metadata is claimed, never authenticated. True access control and isolation are enforced by network policy and mutual authentication, plus the per-jobset credential on the A2A routes (below).

The `agent-run-api` worker sink (`worker::a2a::JobMessageSink`) injects an incoming A2A message as the NEXT turn of the addressed job's conversation (the PTY/bracketed-paste path above is the orchestrator's; the worker has no live PTY). The framed text is already validated and framed by the shared ingress path before delivery.

- **Running job**: the current `agy` attempt is stopped cleanly via the same process-group SIGTERM/grace/SIGKILL mechanism as `kill` — but WITHOUT the kill bookkeeping (the row is never marked `killed`, so it is not reported user-killed/failed). The old monitor thread that reaps the stopped child writes its final status guarded by `retry_count`; the resume bumps `retry_count` before it launches, so that stale save is a no-op.
- **Finished job** (done/failed/incomplete/self-terminated): resumed directly as a fresh follow-up turn on the same conversation.
- **Never woken**: an operator-`killed` job (clear error); unknown or cleaned/discarded jobs answer `TASK_NOT_FOUND`.
- **Serialisation**: at most one resume is in flight per job (a per-job gate); the global sink map lock is held only for the lookup. Concurrent messages coalesce in memory into the next resume prompt (bounded 8 msgs / 8 KB, oldest dropped with a note; no persistent inbox).
- **Rate limit**: a per-job minimum interval (`a2a_interrupt_min_secs`, default 20 s, `0` = off) plus the existing per-jobset hop-token budget (charged by the ingress) cap message storms; a throttled delivery is rejected with a rate-limit error.
- **Idempotency**: a repeated A2A message id is applied at most once (bounded in-memory seen-cache); a duplicate is accepted as a no-op.

## Per-jobset credential (A2A, feature `a2a`)

A *jobset* is a set of agents (orchestrator sessions and worker jobs alike); any member may message any other member and nobody outside may. Each jobset has ONE random secret (>= 32 bytes from the OS RNG, hex) and an id; both are generated in memory by the process that creates the jobset and are never written to disk, logged, or placed in argv or the environment.

Mechanics:

- **Registry** (`core::jobset::JobsetRegistry`): an in-memory map of agent id -> (jobset id + secret), held for the lifetime of the hosting process and dropped when the job/session ends.
- **Hand-off to a new agent**: the process that starts an agent hands it the jobset id + secret through the sandbox policy's existing `secret` channel — an inherited fd the sandbox turns into a 0400 file (`jobset-secret`) inside the agent's private tmp. The launcher sets only two environment variables, `KUBEADP_JOBSET_ID` (the id) and `KUBEADP_JOBSET_SECRET_FILE` (the PATH of that file); the secret value itself is never set in the environment or argv.
- **Verification** (`core::jobset::verify_jobset_credential`): on an A2A route the request carries `X-Jobset-Id` and `X-Jobset-Secret`; the two are compared, in constant time, against the *target* agent's registered jobset (SHA-256 digests from the shared `core::crypto` module, backed by `ring`). A mismatch, a missing header, or an unregistered target is refused.
- **Routing** (`core::a2a::a2a_router_with_jobset`): the jobset credential is installed ONLY on the A2A routes, as an *alternative* credential alongside the operator/bearer gate — a request is accepted when either is valid. It is never accepted on the `/api/v1` agent-run surface (e.g. `/api/v1/jobs`), which is guarded separately by the shared bearer layer. When the local agent is not a jobset member the A2A surface is unchanged (bearer-only; an absent bearer verifier still means network-policy-only access).
- **Sending side** (`core::a2a::client::JobsetCredentialInterceptor`): the agent's A2A client reads its own jobset id + secret file from the environment and attaches the two headers to every outgoing request.

The per-jobset secret is a symmetric membership credential, not a per-member identity: a stolen jobset secret grants membership of that jobset only. Same-pod jobs share a uid, so the required hardening against one job stealing another's credential is per-job process isolation (hidden `/proc` or per-job uid), which is tracked separately.

## Operator API token (both roles, `server` feature)

The whole `/api/v1` surface of both roles (worker agent-run-api and
orchestrator), the `/metrics` endpoint, and (under `a2a`) the A2A routes are
guarded by one operator bearer token:

- Read at startup from the file named by config key `api_token_file` /
  `KUBEADP_API_TOKEN_FILE` (a `[common]` key shared by both roles). The raw
  value is trimmed, reduced to its SHA-256 digest and dropped; only the
  digest is kept, and the value is never logged. The orchestrator's legacy
  `AGENT_ORCHESTRATE_TOKEN_FILE` / `AGENT_ORCHESTRATE_TOKEN` is honoured as a
  fallback when no shared token is set, so existing deployments keep working
  unchanged.
- Comparison is constant-time on the digest (`server/token.rs`, FIPS 180-4
  SHA-256 from the shared `core::crypto` module, backed by `ring`). A missing or wrong `Authorization: Bearer
  <token>` answers `401 Unauthorized` with an RFC 7807
  `application/problem+json` body before the handler runs. A configured but
  unreadable token file is startup-fatal (no silent fallback to open).
- Failed auths are throttled per peer IP: 10 within 60 s turn into `429 Too
  Many Requests` with `Retry-After` (bounded peer map with eviction).
- Network policies: `/healthz` and `/readyz` are the only never-gated routes.
- Without any token the server refuses to start unless config flag
  `api_allow_unauthenticated` (`KUBEADP_API_ALLOW_UNAUTHENTICATED`) is set,
  which prints a loud warning and serves open. The `test-hooks` cargo feature
  defaults the flag on for the harness.
- The A2A surface is gated by the same operator token
  (`core::server::auth::auth_verifier_from_state`); a per-jobset credential
  can be layered on as an alternative there later.
- The `agent-run` CLI (client side, ureq) reads the same variable: when
  `KUBEADP_API_TOKEN_FILE` is set, the trimmed file contents are presented as
  `Authorization: Bearer <token>` on every request the CLI sends (a ureq
  middleware in `src/cli/client.rs`); unset means no header. A configured but
  unreadable or empty file makes the CLI fail with a clear error before any
  request, naming only the file path — the token value is never printed or
  logged, also not in debug or `--json` output.

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

**Session sandboxing & grants.** Orchestrator sessions can be configured with `"sandbox": true` (the default) or `"sandbox": false` (trusted). Sandboxed sessions run inside `sandbox-exec`. To safely grant specific pod resources to a sandboxed session without running unsandboxed, a session may declare a `grants` object in the supervisor configuration. Grants are strictly restricted to orchestrator sessions configured via trusted configuration files; worker jobs cannot specify grants and fail closed if grants are supplied.

When `serviceaccount: true` is granted on a session policy, the Kata backend lifts the default deny on the Kubernetes ServiceAccount token directory (`/var/run/secrets/kubernetes.io`) for read-only access (write access to the ServiceAccount directory is always denied and fails closed). Granted paths are strictly validated: non-absolute paths, root `/`, `/proc`, `/dev`, and paths with parent traversal (`..`) are rejected fail-closed. Backends that cannot enforce grants (such as gVisor) fail closed with exit 125 rather than silently ignoring grants.

#### Session Grants Configuration Reference

| Key | Type | Description |
| :--- | :--- | :--- |
| `serviceaccount` | boolean | Grants read-only access to `/var/run/secrets/kubernetes.io` (enabling `kubectl`). Always denied for writes. |
| `ro_paths` | array of strings | Absolute paths granted read-only to the sandboxed session. Must not be `/`, `/proc`, `/dev`, or contain `..`. |
| `rw_paths` | array of strings | Absolute paths granted read-write to the sandboxed session. Must not be `/`, `/proc`, `/dev`, or contain `..`. |
| `env` | array of strings | Environment variable names passed through from supervisor process environment. Only exact names or trailing wildcard `*` allowed (e.g. `VIKUNJA_*`). Values are never configured or logged. |

In `apps/ai-server`, `homelab` is configured with `"sandbox": false` by default, but can be switched to sandboxed with full explicit grants. `nested-operator` and `kubeopencode` stay sandboxed by default. Worker job sandboxing is unaffected.

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


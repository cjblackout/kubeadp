# kubeadp-agent

One cargo package for the whole agent stack. It consolidates the former crates `agent-core`, `agent-orchestrate`, `agent-run-api` and `agent-run-cli`; behaviour is unchanged.

## Layout

| Module | Former crate | Feature |
|---|---|---|
| `core` | agent-core (job engine, git worktrees, shared axum server, A2A) | `server` for `core::server`, `a2a` for `core::a2a` |
| `orchestrate` | agent-orchestrate (Claude Code session supervisor and REST API) | `server` |
| `worker` | agent-run-api (agy job API daemon) | `server` |
| `cli` | agent-run-cli (remote client) | `cli` |

## Binaries

- `kubeadp-agent`: role chosen by env `KUBEADP_ROLE=orchestrator|worker` (unset or any other value exits with code 2).
- `agent-run`: the remote CLI, flags and behaviour unchanged (see [DOCS.md](DOCS.md)).
- `agent-orchestrate` and `agent-run-api`: thin wrappers running one role directly, kept for the existing images and manifests.

## Features

`default = ["cli","server"]`, `cli`, `server`, `a2a` (implies `server`), `test-hooks` (implies `server`).

## Tests

`cargo test --features a2a,test-hooks`, `bash tests/orchestrate/run-tests.sh` (supervisor black-box suite), `bash tests/cli/run-all.sh` (CLI suite, never touches the real server; see tests/cli/README.md).

## Configuration file

Both long-lived roles of the `kubeadp-agent` binary (`KUBEADP_ROLE=orchestrator|worker`) and the thin `agent-orchestrate` / `agent-run-api` wrappers can be configured from a single TOML file that maps 1:1 onto the environment variables the code already reads. The pattern mirrors `info-hub-mcp`'s `src/config.rs`.

File resolution:

- `KUBEADP_CONFIG_FILE` selects the file; an empty value is an error.
- When unset, `/etc/kubeadp-agent/config.toml` is used **only if it exists**.
- An absent file means environment-only configuration (behaviour unchanged).

Sections: `[common]`, `[orchestrator]`, `[worker]`. Keys are lower-case; a key in the wrong section, an unknown key, a duplicate key or a non-scalar value is a hard error naming `section.key`. Booleans are written `true`/`false`, numbers as decimal strings.

**Precedence: the environment always wins.** The role applies `[common]` plus its own section; the other role's section is parsed and validated but its values are not applied, so one ConfigMap can serve both roles.

### Key table

| Section | Key | Environment variable |
|---|---|---|
| `[common]` | `http_proxy` | `AGENT_RUN_HTTP_PROXY` |
| `[common]` | `https_proxy` | `AGENT_RUN_HTTPS_PROXY` |
| `[common]` | `no_proxy` | `AGENT_RUN_NO_PROXY` |
| `[orchestrator]` | `listen` | `AGENT_ORCHESTRATE_LISTEN` |
| `[orchestrator]` | `token_file` | `AGENT_ORCHESTRATE_TOKEN_FILE` |
| `[orchestrator]` | `session_log_ring_bytes` | `AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES` |
| `[orchestrator]` | `config` | `AGENT_ORCHESTRATE_CONFIG` |
| `[orchestrator]` | `workspaces` | `CLAUDE_WORKSPACES` |
| `[orchestrator]` | `workspace_root` | `CLAUDE_WORKSPACE_ROOT` |
| `[orchestrator]` | `remote_prefix` | `CLAUDE_REMOTE_PREFIX` |
| `[orchestrator]` | `default_session` | `CLAUDE_DEFAULT_SESSION` |
| `[orchestrator]` | `default_session_id` | `CLAUDE_DEFAULT_SESSION_ID` |
| `[orchestrator]` | `session_args` | `CLAUDE_SESSION_ARGS` |
| `[orchestrator]` | `claude_max_crashes` | `CLAUDE_MAX_CRASHES` |
| `[worker]` | `workspace_root` | `AGENT_RUN_WORKSPACE_ROOT` |
| `[worker]` | `jobs_dir_name` | `AGENT_RUN_JOBS_DIR_NAME` |
| `[worker]` | `agy_path` | `AGENT_RUN_AGY_PATH` |
| `[worker]` | `default_model` | `AGENT_RUN_DEFAULT_MODEL` |
| `[worker]` | `default_effort` | `AGENT_RUN_DEFAULT_EFFORT` |
| `[worker]` | `default_base_branch` | `AGENT_RUN_DEFAULT_BASE_BRANCH` |
| `[worker]` | `state_root` | `AGENT_RUN_STATE_ROOT` |
| `[worker]` | `repos_root` | `AGENT_RUN_REPOS_ROOT` |
| `[worker]` | `gc_max_age_days` | `AGENT_RUN_GC_MAX_AGE_DAYS` |
| `[worker]` | `max_parallel` | `AGENT_RUN_MAX_PARALLEL` |
| `[worker]` | `shutdown_grace_secs` | `AGENT_RUN_SHUTDOWN_GRACE` |
| `[worker]` | `launch_timeout_secs` | `AGENT_RUN_LAUNCH_TIMEOUT_SECS` |
| `[worker]` | `stall_minutes` | `AGENT_RUN_STALL_MINUTES` |
| `[worker]` | `max_runtime` | `AGENT_RUN_MAX_RUNTIME` |
| `[worker]` | `hindsight_enabled` | `AGENT_RUN_HINDSIGHT_ENABLED` |
| `[worker]` | `hindsight_bank` | `AGENT_RUN_HINDSIGHT_BANK` |
| `[worker]` | `hindsight_tag` | `AGENT_RUN_HINDSIGHT_TAG` |
| `[worker]` | `hindsight_path` | `AGENT_RUN_HINDSIGHT_PATH` |
| `[worker]` | `hindsight_auto_recall` | `AGENT_RUN_HINDSIGHT_AUTO_RECALL` |
| `[worker]` | `hindsight_auto_retain` | `AGENT_RUN_HINDSIGHT_AUTO_RETAIN` |
| `[worker]` | `hindsight_url` | `AGENT_RUN_HINDSIGHT_URL` |
| `[worker]` | `hindsight_profile` | `AGENT_RUN_HINDSIGHT_PROFILE` |
| `[worker]` | `remote_control` | `AGENT_RUN_REMOTE_CONTROL` |
| `[worker]` | `remote_control_autostart` | `AGENT_RUN_REMOTE_CONTROL_AUTOSTART` |
| `[worker]` | `remote_control_check_secs` | `AGENT_RUN_REMOTE_CONTROL_CHECK_SECS` |
| `[worker]` | `api_listen` | `AGENT_RUN_API_LISTEN` |
| `[worker]` | `agy_mcp_source` | `AGENT_RUN_AGY_MCP_SOURCE` |

### Secrets

Secrets are never inline. Only the `_FILE` form is accepted:

- `[orchestrator] token_file` -> `AGENT_ORCHESTRATE_TOKEN_FILE` (the bearer token; the inline `AGENT_ORCHESTRATE_TOKEN` variable has no TOML key).
- The worker's Hindsight API key has no file key: set `AGENT_RUN_HINDSIGHT_API_KEY` in the environment from a Secret.

A key that would put either secret inline is rejected with a message pointing at the `_FILE` form.

Example (one ConfigMap for both roles):

```toml
[common]
http_proxy = "http://proxy.example:3128"

[orchestrator]
listen = "0.0.0.0:8003"
token_file = "/run/secrets/agent-token"
claude_max_crashes = 7

[worker]
max_parallel = 4
hindsight_url = "https://hindsight.example"
```

### Orchestrator session configuration as TOML

The orchestrator's JSON session configuration (`AGENT_ORCHESTRATE_CONFIG`, default `/etc/agent-orchestrate/config.json`) is also accepted as TOML, chosen by file extension (`.toml` vs `.json`) and deserialized through the same serde structs: the top-level fields (`state_file`, `heartbeat_interval_secs`, `stale_heartbeat_secs`, `shutdown_grace_secs`, `claude_config_dir`) plus an array of tables for `sessions` with the same field names as the JSON form. Sessions are not embedded in the main configuration file.

# Module notes: core
## agent-core

The `agent-core` crate is the shared execution engine and library backing `agent-run-cli`, `agent-run-api` and `agent-orchestrate`. It implements Git worktree isolation, SQLite job metadata persistence, process lifecycle management, activity monitoring, Hindsight memory integration, and (behind the `server` cargo feature) the axum-based HTTP API server module (`kubeadp_agent::core::server`) that both `agent-run-api` and `agent-orchestrate` re-export. The `server` module carries the shared `/api/v1` route group plus the observability/auth/error layers both servers build on: `server::obs::add_observability` wraps the authenticated sub-router in one bearer-auth layer and appends the never-auth `GET /healthz` and `GET /readyz` probes and the auth-gated `GET /metrics`; `server::reject` provides `ApiJson`/`ApiQuery`/`ApiPath` extractors whose rejections render as the standard `{"error","code"}` JSON shape (400/413/415/422). Build the library with `cargo build --manifest-path src/kubeadp-agent/Cargo.toml [--features server]` and run tests with `cargo test --manifest-path src/kubeadp-agent/Cargo.toml`. Architectural specifications and subsystem designs are detailed in [docs/agent-run/architecture.md](../../docs/agent-run/architecture.md).

The `agent-core` crate is the shared execution engine and library backing `agent-run-cli`, `agent-run-api` and `agent-orchestrate`. It implements Git worktree isolation, SQLite job metadata persistence, process lifecycle management, activity monitoring, Hindsight memory integration, and (behind the `server` cargo feature) the axum-based HTTP API server module (`kubeadp_agent::core::server`) that both `agent-run-api` and `agent-orchestrate` re-export. Build the library with `cargo build --manifest-path src/kubeadp-agent/Cargo.toml [--features server]` and run tests with `cargo test --manifest-path src/kubeadp-agent/Cargo.toml`. Architectural specifications and subsystem designs are detailed in [docs/agent-run/architecture.md](../../docs/agent-run/architecture.md).

### A2A agent protocol surface (feature `a2a`)

Behind the `a2a` cargo feature (which depends on `server`), `kubeadp_agent::core::a2a` provides the server side of the A2A v1 agent protocol: the public agent card at `/.well-known/agent-card.json` (name/description/version from a caller-supplied `A2aModel`; skills = a caller-passed list of project/agent names; `capabilities.streaming=true`; bearer security scheme) plus JSON-RPC (`POST /rpc`) and HTTP+JSON (REST) bindings for `GetTask`, `ListTasks` and `CancelTask` over the `TaskBackend` trait. `SendMessage`, `SendStreamingMessage`, `SubscribeToTask`, push-notification config and `GetExtendedAgentCard` answer HTTP 501 / `UNSUPPORTED_OPERATION` (`code: "unsupported"`), never 404, until later slices implement them.

The bindings use the official `a2a-lf` types crate (`a2aproject/a2a-rs`, Apache-2.0, pinned exactly) but not `a2a-server-lf`: its REST error envelope and its 400 mapping for unsupported operations do not match this crate's `{"error","code"}` error shape and the required 501 `code: "unsupported"` semantics, so the thin wire bindings are local handlers over the `a2a-lf` types.

Router construction: `a2a_router(model, backend, auth)` returns a standalone `axum::Router` that this crate does **not** mount; the wrappers (`agent-orchestrate`, `agent-run-api`) merge it into their own routers in later slices. All non-card routes require a bearer token supplied by the caller as an optional `AuthVerifier` (`Arc<dyn Fn(&HeaderMap) -> bool>`); the card is always public. `map_job_status` converts `kubeadp_agent::core::job::JobStatus` into A2A `TaskState` (`Running`→`working`, `Done`→`completed`, `Failed`/`Incomplete`/`QuotaExhausted`→`failed`, `SelfTerminated`/`Killed`→`canceled`).

Feature-gated integration tests live in `tests/a2a.rs`; run them with `cargo test --features a2a --manifest-path src/kubeadp-agent/Cargo.toml`.

# Module notes: orchestrate

## Agent Orchestrate

### Design
`agent-orchestrate` (formerly `claude-supervisor`) is a lightweight, robust Rust process supervisor for the `ai-server` pod's Claude Code CLI sessions.
It manages N independent interactive sessions concurrently without an asynchronous runtime, using minimal core dependencies (`libc`, `serde`, `serde_json`).

Each session runs in its own dedicated Unix pseudo-terminal (PTY) with an isolated process group (`setsid`, `TIOCSCTTY`).
PTY output is forwarded raw and immediately by default, preserving TUI screen drawing and ANSI escape sequences. Opt-in `prefix_output` buffers lines with `[<session>] ` and flushes partial lines after 100ms of inactivity.
Optional hang detection monitors PTY output activity (`hang_timeout_secs`). Empirical measurement with real Claude in a PTY showed that Claude emits ~1-2 KB within ~3.5s of launch and then remains completely silent while idle (observed silent for >2 minutes); output silence is normal behavior, so hang detection is OFF by default (`hang_timeout_secs: 0.0`).

Lifecycle state management:
1. Pre-launch Config: Thread-safe serialized (`static Mutex`) and atomic update of `~/.claude.json` and `~/.claude/settings.json`, skipping writes if disk contents are identical.
2. Readiness Check: Pre-launch validation of `claude auth status --json` with categorized logging. Aborts promptly on supervisor shutdown and reaps probe children. Non-zero exit code is recorded as `status-failed (rc=..)`, while `not-logged-in` is recorded only when rc == 0 and `loggedIn` is false.
3. Transcript Policy: Validates transcript files; a transcript is considered usable if at least one line parses as JSON (allowing Claude Code to resume successfully even if the last line is truncated or a middle line is corrupted). If a file exists but contains zero parseable JSON lines (empty, whitespace, or binary garbage), it is unusable and quarantined to `.corrupt.<unix-millis>.bak` to trigger a clean start with `--session-id`. Furthermore, if `--resume` exits non-zero within `resume_fail_uptime_secs` with `No conversation found with session ID` for `resume_fail_quarantine_after` consecutive launches with the same transcript size and mtime, it is quarantined to `.corrupt.<unix-millis>.bak` and starts fresh with `--session-id` (transient errors like network or API outages neither count toward quarantine nor reset an existing streak). Missing files start fresh without quarantine.
4. Launch Arguments: Dynamically selects `--resume <id>` if a usable transcript exists, or `--session-id <id>` for fresh starts.
5. Backoff Window & Cooldown: Crashes and caught panics increment restart counters. Restart delays are computed using a sliding window of all restarts within `backoff_window_secs` (`delay = min(base * 2^n, max_backoff)` with n = restarts already in the window, with +0..+25% jitter; the first crash gets `base`). If a session exceeds `max_crashes` within `crash_window_secs`, it enters `failed` state for `failed_cooldown_secs` (probes exit 2). Once the cooldown expires, the crash tracker is cleared and supervision automatically resumes.
6. Atomic State & Health Probing: State transitions and periodic heartbeats are committed via atomic tempfile rename with unique per-call names and dedicated write mutex serialization. State lock is released before disk writes. The `status` command serves as a Kubernetes probe (exit 1 if heartbeat is stale, exit 2 if any session failed or a thread died, exit 0 if healthy).
7. Bounded Shutdown, Reaping & Unkillable Handling: On SIGTERM, SIGINT, or SIGHUP, threads and child processes are stopped gracefully within `shutdown_grace_secs`. Reaps only tracked supervisor child PIDs/process groups (never `waitpid(-1)`). After SIGKILL escalation, child process wait is bounded by `kill_wait_secs` using a non-blocking poll. If a child process cannot be reaped (e.g. stuck in uninterruptible D-state I/O), the supervisor logs `child pid N unkillable (uninterruptible)`, does not block indefinitely, and marks the session as `failed` with last exit reason `unkillable` (causing `status` to exit 2). Once the stuck PID disappears, supervision resumes via the failed-cooldown path.
8. Orphan Prevention: Sets `PR_SET_PDEATHSIG(SIGTERM)` in child `pre_exec` (with `getppid()` race guard) when spawning from the long-lived session thread. If the supervisor terminates abruptly (SIGKILL/crash), child processes receive SIGTERM and exit instead of becoming orphaned.
9. Configuration Validation: All `f64` duration fields are validated at load time (must be finite, >= 0.0, <= 30 days, with `failed_cooldown_secs` >= 1.0). Invalid values exit 1 naming the offending field and session.
10. Session Precedence: File-configured sessions always take precedence over environment-generated sessions (default or `CLAUDE_WORKSPACES`). Conflicting generated sessions are skipped with a warning. Duplicates within the same source exit with code 1.

### CLI Commands
- `agent-orchestrate run [--config <path>]`: Run the supervisor. Resolves configuration (from `--config`, `AGENT_ORCHESTRATE_CONFIG` (legacy `CLAUDE_SUPERVISOR_CONFIG` still accepted), or `/etc/agent-orchestrate/config.json`) and environment variables (`CLAUDE_WORKSPACES`, etc.) and supervises all sessions.
- `agent-orchestrate status [--config <path>] [--state-file <path>]`: Probe supervisor health:
  - Exit `0`: All sessions healthy, heartbeat active.
  - Exit `1`: Heartbeat stale (> threshold).
  - Exit `2`: One or more sessions failed or a session thread terminated unexpectedly (triggers Kubernetes pod restart).
  - Exit `3`: State file could not be read or parsed.
  - Exit `4`: Supervisor is stopped or one or more sessions are in `stopped` status.
- `agent-orchestrate print-config [--config <path>]`: Resolve and print the active supervisor configuration as JSON to stdout without starting sessions or mutating files. Exits 1 on invalid duplicate names or session IDs.

### Configuration Reference
Base configuration is resolved in the following priority order:
1. `--config <path>` (CLI argument)
2. `AGENT_ORCHESTRATE_CONFIG` (environment variable path; legacy `CLAUDE_SUPERVISOR_CONFIG` still accepted)
3. `/etc/agent-orchestrate/config.json` (only if the file exists on disk)
4. Built-in defaults

> [!NOTE]
> Cwd-relative default config search (`agent-orchestrate.json` or `config.json` in the working directory) has been removed to avoid accidentally loading unrelated files in `/home/coder`.
> If an explicit path is provided via `--config` or the config environment variable and fails to load, the supervisor exits with code 1 immediately.

> [!WARNING]
> **Workdir Symlink Limitation**: Claude Code derives transcript paths using the canonical (symlink-resolved) real path of its working directory. If a session's `workdir` is a symlink whose target changes (such as git-sync checkout directories), the conversation transcript path will change and previous conversation history will be lost on retarget. Workspace sessions should point at stable real directories, not git-sync symlinks. (The default `/home/coder` session is a stable real directory and unaffected).

Example configuration JSON:

```json
{
  "state_file": "/tmp/claude-supervisor-state.json",
  "heartbeat_interval_secs": 2.0,
  "stale_heartbeat_secs": 15.0,
  "shutdown_grace_secs": 5.0,
  "claude_config_dir": null,
  "sessions": [
    {
      "name": "default",
      "workdir": "/home/coder",
      "session_id": "10000000-0000-4000-8000-000000000001",
      "claude_binary": "claude",
      "claude_args": [
        "--dangerously-skip-permissions",
        "--remote-control",
        "ai-server"
      ],
      "env": {},
      "session_file": null,
      "transcript_path": null,
      "readiness_poll_secs": 30.0,
      "hang_timeout_secs": 0.0,
      "kill_grace_secs": 5.0,
      "pty_eof_grace_secs": 10.0,
      "kill_wait_secs": 15.0,
      "base_backoff_secs": 2.0,
      "max_backoff_secs": 60.0,
      "healthy_run_secs": 30.0,
      "max_crashes": 0,
      "crash_window_secs": 60.0,
      "failed_cooldown_secs": 300.0,
      "backoff_window_secs": 600.0,
      "resume_fail_quarantine_after": 5,
      "resume_fail_uptime_secs": 5.0,
      "prefix_output": false
    }
  ]
}
```

#### Environment Variables for Workspace Auto-Generation
When running or inspecting config, environment variables dynamically inject sessions alongside any file-defined sessions. Empty or whitespace-only variables are treated as unset:
- `AGENT_ORCHESTRATE_CONFIG`: Path to base JSON configuration file (used if `--config` is not specified); legacy `CLAUDE_SUPERVISOR_CONFIG` is still accepted.
- `CLAUDE_WORKSPACES`: Comma and/or whitespace separated workspace names (e.g. `homelab,nested-operator`), or `auto`. In `auto` mode, scans `CLAUDE_WORKSPACE_ROOT` for non-hidden directories (or symlinks to directories) containing `.git` (file or directory) and sorts them alphabetically. Names must match `[a-z0-9][a-z0-9._-]*`.
- `CLAUDE_WORKSPACE_ROOT`: Base directory for relative workspace paths and `auto` scan (default: `/workspace`).
- `CLAUDE_REMOTE_PREFIX`: Remote-control name prefix for generated sessions (default: `ai-server`). Remote name is `<prefix>-<name>`.
- `CLAUDE_DEFAULT_SESSION`: Controls whether the legacy default session is included (`1`/`true`/`on` default; `0`/`false`/`no`/`off` disables).
- `CLAUDE_DEFAULT_SESSION_ID`: Session UUID for default session (default: `10000000-0000-4000-8000-000000000001`).
- `CLAUDE_SESSION_ARGS`: Optional whitespace-separated arguments appended to all generated sessions (e.g. `--model haiku`).
- `CLAUDE_MAX_CRASHES`: Optional integer override for maximum crashes within window for env-generated sessions. Defaults to `0` (unlimited retries, never mark failed).
- `HOME`: User home directory (if unset or empty, falls back to `/home/coder`).

Workspace session IDs are derived via deterministic UUIDv5 from `ai-server-<name>` within a fixed namespace UUID (`a689b7b9-3b60-449a-8b17-0d5b7a14e911`), ensuring transcript continuity even if `CLAUDE_REMOTE_PREFIX` changes.

#### Global Options
- `state_file`: Path to write the atomic JSON state (default: `/tmp/claude-supervisor-state.json`).
- `heartbeat_interval_secs`: Frequency of supervisor heartbeat timestamp updates (default: `2.0`).
- `stale_heartbeat_secs`: Threshold after which heartbeat is considered stale by probe (default: `15.0`).
- `shutdown_grace_secs`: Grace period before sending SIGKILL to remaining child processes on supervisor shutdown (default: `5.0`).
- `claude_config_dir`: Base Claude directory override (default: `$HOME/.claude`).

#### Session Options
- `name`: Unique name identifier for the session.
- `workdir`: Working directory path for the session child process.
- `session_id`: Unique Claude Code session UUID.
- `claude_binary`: Path or binary name for Claude executable (default: `"claude"`). If no path separator is present, searched in session `env.PATH` then system `PATH`.
- `claude_args`: Command-line arguments. `--resume` or `--session-id` is inserted appropriately.
- `env`: Key-value environment variables passed to the session.
- `session_file`: Legacy transcript file override.
- `transcript_path`: Explicit transcript file path override (takes precedence over derived `~/.claude/projects/<slug>/<session_id>.jsonl`).
- `readiness_poll_secs`: Interval between auth readiness checks (default: `30.0` for env-generated sessions, `5.0` for file-config sessions).
- `hang_timeout_secs`: Seconds of silent PTY inactivity before triggering kill & restart (default: `0.0` = disabled).
- `kill_grace_secs`: Grace period between SIGTERM and SIGKILL escalation (default: `5.0`).
- `pty_eof_grace_secs`: Grace period in seconds to wait for child to exit after PTY EOF/error before SIGTERM/SIGKILL escalation and restarting as normal exit (default: `10.0`).
- `kill_wait_secs`: Maximum duration in seconds to wait for child process reaping via non-blocking poll after SIGKILL before declaring the child unkillable (default: `15.0`).
- `base_backoff_secs`: Initial restart backoff duration in seconds (default: `2.0`).
- `max_backoff_secs`: Maximum exponential backoff cap (default: `60.0`).
- `healthy_run_secs`: Uptime threshold in seconds for a run to be considered healthy (default: `30.0`).
- `max_crashes`: Max allowed crashes within sliding crash window before marking session failed (default: `0` for env-generated sessions = unlimited retries; `5` for file-config sessions).
- `crash_window_secs`: Sliding window duration for crash threshold accounting (default: `60.0`).
- `failed_cooldown_secs`: Cooldown duration in seconds to keep a session in `failed` status before resetting the crash tracker and retrying (default: `300.0`, min: `1.0`).
- `backoff_window_secs`: Sliding window duration in seconds for exponential backoff accounting across all restarts (default: `600.0`).
- `resume_fail_quarantine_after`: Number of consecutive fast `--resume` failures printing `No conversation found with session ID` with identical transcript size and mtime before quarantining the file to start fresh with `--session-id` (default: `5`).
- `resume_fail_uptime_secs`: Maximum uptime in seconds to consider a failed `--resume` launch as a fast resume failure (default: `5.0`).
- `prefix_output`: When true, prefixes forwarded PTY output with `[<name>] ` and flushes partial lines after 100ms (default: `false`, raw immediate forwarding).

### REST API

The supervisor runs an HTTP server on its own tokio runtime in a background
thread, serving the REST API of the `worker` module (identical `/api/v1/...`
routes, request/response schemas, error format and SSE events, so the
`agent-run` CLI works against it unchanged) plus a supervisor-specific
`/api/v1/sessions` group for the sessions it manages (LIST, GET,
START / STOP / RESTART, redacted LOG tail, message injection). No endpoint
writes to a session's PTY except `message`, which is deliberately modelled on
what a user types into a terminal and never submits text as a command.

- Listen address: `AGENT_ORCHESTRATE_LISTEN`, default `127.0.0.1:8003`
  (reachable only from inside the cluster).
- Optional bearer token for the WHOLE `/api/v1` group (the shared agent-run
  routes and the `/api/v1/sessions` group alike):
  `AGENT_ORCHESTRATE_TOKEN_FILE` (a file whose trimmed contents are the token)
  or `AGENT_ORCHESTRATE_TOKEN`. The token is enforced once by a single router
  layer, compared in constant time, and failed attempts are throttled per peer
  IP (429 after 10 failures within 60 s). `/healthz` and `/readyz` are the only
  never-gated routes; `/metrics` requires the token like `/api/v1`.
- `GET /readyz`: unauthenticated readiness probe, `{"status": "ready"}` when
  the state store / sessions registry is readable, `503 {"error","code":
  "not_ready"}` otherwise.
- `GET /metrics`: Prometheus text format (`agent_process_uptime_seconds`,
  `agent_state{status="..."}` session counts by state,
  `agent_requests_total{class="..."}`), gated by the bearer token like
  `/api/v1`.
- Per-session log ring buffer: `AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES`
  (default 500000). `GET /api/v1/sessions/{name}/log?tail=N` serves the last
  `N` lines (default 5000) of the ring, with credential-looking values
  (`ghp_`/`gho_`/`ghu_`/`ghs_`/`ghr_`/`github_pat_`/`glpat-`/`Bearer
  `...`/`AGE-SECRET-KEY-`...) redacted before they reach a client.
- `POST /api/v1/sessions/{name}/message` injects one message into a running
  session. Body: `{"text": "<message>"}` (JSON). The text is trimmed and must
  be between 1 and 8192 bytes of valid UTF-8; control characters and escape
  sequences are stripped (a newline is kept and delivered inside the paste).
  The message is written to the session's PTY as a bracketed paste
  (`ESC[200~ ... ESC[201~`) followed by a single Enter, so pasted newlines
  never submit early and the text cannot run as a command; Claude Code queues
  text typed during a turn, so in-flight work is not interrupted.
  - `400 invalid_body`: body is not JSON, has no `text`, or the text is empty
    after trimming.
  - `413 too_large`: trimmed text exceeds 8192 bytes.
  - `404`: unknown session.
  - `409 not_running`: session is stopped/paused or has no live PTY.
  - `429 rate_limited` (with `Retry-After`): more than 30 messages per minute
    for one session (sliding window).
  - `202`: `{"session": "<name>", "accepted": true, "bytes": <n>}` where `n`
    is the number of sanitised text bytes pasted.
  Each accepted message writes exactly one line to the supervisor log with the
  session name, the byte count and a short hash prefix of the text (FNV-1a) —
  never the text itself — and the session log ring is never touched, so the
  log redaction of `session_log.rs` is not bypassed.

#### A2A Protocol Support (`--features a2a`)
When compiled with `--features a2a`, `agent-orchestrate` exposes the A2A v1 agent protocol surface powered by `agent-core::a2a`:
- Public Agent Card: `GET /.well-known/agent-card.json` serves the agent card (unauthenticated, public).
- Sessions as A2A Tasks:
  - Task ID = session name
  - Context ID = session name
  - State mapping: `Running` / `Starting` / `Backoff` -> `working`, `Stopped` -> `completed` (or `canceled` if paused), `Failed` -> `failed`.
- Endpoints:
  - JSON-RPC: `POST /rpc` supporting `GetTask`, `ListTasks`, and `CancelTask`.
  - REST: `GET /tasks`, `GET /tasks/{id}`, and `POST /tasks/{id}:cancel` (or legacy `POST /tasks/{id}/cancel`).
  - Unsupported operations (`SendMessage`, streaming, subscriptions, push configs) return standard 501 / `unsupported` responses.
- Authentication: Protected A2A routes are guarded by the same bearer token mechanism as `/api/v1` routes (`AGENT_ORCHESTRATE_TOKEN_FILE` or `AGENT_ORCHESTRATE_TOKEN`).

### How to Run Tests

#### 1. Cargo Unit Tests
Executes unit tests covering backoff math, jitter, sliding crash windows, configuration parsing, session file validation, corrupted file quarantine, concurrent config updates, UTF-8 safe tail formatting, atomic state serialization, UUIDv5 derivation, duration validation, and workspace discovery:
```bash
cargo test --locked
```

#### 2. Clippy & Linter
```bash
cargo clippy --all-targets --locked -- -D warnings
```

#### 3. Formatting Check
```bash
cargo fmt --check
```

#### 4. Black-Box Integration Suite
Executes the integration suite against `fake-claude.sh` in temporary, isolated directories (unsetting ambient supervisor environment variables to ensure hermetic runs):
```bash
tests/run-tests.sh
```

### Behavioral Notes

1. Crash Loop Protection & Failed State Cooldown: sliding crash window (`max_crashes` in `crash_window_secs`); once exceeded, the session enters `failed` status for `failed_cooldown_secs` (causing `status` to exit 2) while siblings keep running, then supervision resumes automatically.
2. Bounded Shutdown: each session runs in its own process group (`setsid`); the whole group is signaled with strict `shutdown_grace_secs` deadlines and SIGKILL escalation; only tracked supervisor children are reaped.
3. Orphan Prevention: child processes get `PR_SET_PDEATHSIG(SIGTERM)`, so they terminate if the supervisor dies abruptly.
4. Config is purely JSON (TOML support can be added if desired).

### Status

- Wired in as the default supervisor for the `ai-server` pod; see [apps/ai-server/DOCS.md](../../apps/ai-server/DOCS.md) for rollout status and configuration details.

# Module notes: worker

## agent-run-api

The `agent-run-api` crate provides the HTTP API daemon and job execution engine for `agent-run`. It manages background `agy` agent execution in Git worktrees, state persistence in SQLite, process supervision, and Server-Sent Events. Build the binary with `cargo build --release --manifest-path src/kubeadp-agent/Cargo.toml` and run tests with `cargo test --manifest-path src/kubeadp-agent/Cargo.toml`. Complete endpoint specifications, wire schemas, and error codes are documented in [docs/agent-run/api-reference.md](../../docs/agent-run/api-reference.md).

### A2A Protocol Surface

When built with the `a2a` feature (`--features a2a`), the daemon serves the Agent-to-Agent (A2A v1) protocol:
- `GET /.well-known/agent-card.json` -- public agent card describing the agent's capabilities and supported interfaces.
- `POST /rpc` -- JSON-RPC 2.0 interface supporting `GetTask`, `ListTasks`, and `CancelTask`.
- `GET /tasks`, `GET /tasks/{id}`, `POST /tasks/{id}:cancel` -- REST bindings for tasks.
- A2A tasks correspond directly to `agent-run` jobs (`task.id` = `job_id`, `task.context_id` = `job_id`), mapping job statuses (`running` -> `working`, `done` -> `completed`, `failed` -> `failed`, `killed` -> `canceled`) and exposing the final response and worktree diff as task artifacts.
- Methods not currently supported (such as `SendMessage`, `SubscribeToTask`, or push notifications) return HTTP 501 / JSON-RPC `UNSUPPORTED_OPERATION` (`code: "unsupported"`).


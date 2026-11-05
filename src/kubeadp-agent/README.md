# kubeadp-agent

Unified agent execution stack. It provides a process supervisor and REST API (`orchestrator`), a background job execution daemon (`worker`), a remote command-line interface (`agent-run`), an execution engine abstraction layer, and container-level process sandboxing.

## Module Layout

| Module | Purpose | Cargo Feature |
|---|---|---|
| `core` | Execution engine, Git worktree isolation, SQLite persistence, shared axum HTTP server, and A2A protocol support | `server` for `core::server`, `a2a` for `core::a2a` |
| `orchestrate` | Interactive session supervisor and REST management API | `server` |
| `worker` | Background job execution API daemon | `server` |
| `cli` | Remote HTTP command-line client (`agent-run`) | `cli` |
| `engine` | Pluggable engine layer (`Engine` trait, capability registry, emulation layer, and run-control routes) | `engine` (implies `server`) |
| `sandbox` | Container-level process isolation (`sandbox-exec`, `sandbox-probe`, `relay-inner`, `relay-connect`) | Built unconditionally |

## Binaries and Roles

The package provides the following binaries:

- `kubeadp-agent`: Dispatcher binary. When invoked without subcommands, it reads the `KUBEADP_ROLE` environment variable:
  - `KUBEADP_ROLE=orchestrator`: Runs the session supervisor and management API.
  - `KUBEADP_ROLE=worker`: Runs the background job API daemon.
  - An unset or invalid `KUBEADP_ROLE` exits with code 2.
- `agent-run`: Remote CLI client communicating with the API daemon over HTTP.
- `agent-orchestrate`: Direct binary entry point for the orchestrator role.
- `agent-run-api`: Direct binary entry point for the worker role.

### Subcommands

The `kubeadp-agent` binary supports subcommands that run directly without requiring `KUBEADP_ROLE`:

- `kubeadp-agent mcp-a2a`: Standard I/O Model Context Protocol (MCP) server providing client tools for talking to peer A2A agents (`list_agents`, `send_message`, `reply`, `get_task`).
- `kubeadp-agent repo-sync [--once|--loop]`: Synchronizes Git repositories into local checkouts using atomic worktrees and symlinks.
- `kubeadp-agent sandbox-exec --policy <file|-> -- <cmd> [args...]`: Executes `<cmd>` inside a sandboxed environment according to the specified policy.
- `kubeadp-agent sandbox-probe [--json]`: Detects container runtime capabilities and reports supported sandbox backends.
- `kubeadp-agent relay-inner <socket-path>`: Internal loopback TCP-to-Unix proxy for sandboxed network relay mode.
- `kubeadp-agent relay-connect <host> <port>`: Internal `ProxyCommand` helper for SSH egress through the relay proxy.

## Cargo Features

| Feature | Description |
|---|---|
| `default` | Includes `["cli", "server", "engine"]`. |
| `cli` | Builds client library and remote CLI dispatching logic. |
| `server` | Builds shared axum HTTP server routes, authentication, and supervisor/worker services. |
| `engine` | Enables pluggable `Engine` trait, run-control routes, and capability tables (implies `server`). |
| `a2a` | Enables A2A protocol server routes and the `mcp-a2a` tool suite (implies `server`). |
| `test-hooks` | Exposes internal test hooks and defaults `api_allow_unauthenticated` to true for harnesses (implies `server`). |

## Configuration File

The long-lived roles of `kubeadp-agent` (`orchestrator` and `worker`), as well as `agent-orchestrate` and `agent-run-api`, can be configured via a TOML file.

File resolution:
1. `KUBEADP_CONFIG_FILE` environment variable specifies an explicit path. An empty string is an error.
2. If unset, `/etc/kubeadp-agent/config.toml` is loaded if it exists.
3. If absent, configuration is read exclusively from environment variables.

Environment variables take precedence over settings in the configuration file.

The file contains three sections: `[common]`, `[orchestrator]`, and `[worker]`. Unknown, misplaced, or duplicate keys produce a configuration error and exit code 1.

### Configuration Keys

| Section | Key | Environment Variable | Default / Notes |
|---|---|---|---|
| `[common]` | `http_proxy` | `AGENT_RUN_HTTP_PROXY` | Proxy URL for HTTP traffic |
| `[common]` | `https_proxy` | `AGENT_RUN_HTTPS_PROXY` | Proxy URL for HTTPS traffic |
| `[common]` | `no_proxy` | `AGENT_RUN_NO_PROXY` | Comma-separated bypass host list |
| `[common]` | `api_token_file` | `KUBEADP_API_TOKEN_FILE` | Path to bearer token guarding `/api/v1` and A2A routes |
| `[common]` | `api_allow_unauthenticated` | `KUBEADP_API_ALLOW_UNAUTHENTICATED` | Boolean; permits starting without a token if `true` |
| `[common]` | `jobset_root_key_file` | `KUBEADP_JOBSET_ROOT_KEY_FILE` | Path to root HMAC key deriving per-jobset secrets |
| `[orchestrator]` | `listen` | `AGENT_ORCHESTRATE_LISTEN` | Listen address (default: `127.0.0.1:8002`) |
| `[orchestrator]` | `token_file` | `AGENT_ORCHESTRATE_TOKEN_FILE` | Path to orchestrator token file (fallback if `api_token_file` unset) |
| `[orchestrator]` | `session_log_ring_bytes` | `AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES` | Buffer size for session logs (default: 500,000 bytes) |
| `[orchestrator]` | `config` | `AGENT_ORCHESTRATE_CONFIG` | Path to session definition file (`.json` or `.toml`) |
| `[orchestrator]` | `workspaces` | `CLAUDE_WORKSPACES` | Comma/whitespace list of workspace names, or `auto` |
| `[orchestrator]` | `workspace_root` | `CLAUDE_WORKSPACE_ROOT` | Base directory for workspaces (default: `/workspace`) |
| `[orchestrator]` | `remote_prefix` | `CLAUDE_REMOTE_PREFIX` | Prefix for session remote names (default: `ai-server`) |
| `[orchestrator]` | `default_session` | `CLAUDE_DEFAULT_SESSION` | Boolean; controls auto-inclusion of default session |
| `[orchestrator]` | `default_session_id` | `CLAUDE_DEFAULT_SESSION_ID` | UUID for default session |
| `[orchestrator]` | `session_args` | `CLAUDE_SESSION_ARGS` | Extra CLI arguments appended to session processes |
| `[orchestrator]` | `claude_max_crashes` | `CLAUDE_MAX_CRASHES` | Maximum crashes within window before failing (default: 0 = unlimited) |
| `[orchestrator]` | `default_session_engine` | `AGENT_ORCHESTRATE_DEFAULT_SESSION_ENGINE` | Default engine for sessions (default: `claude`) |
| `[worker]` | `api_listen` | `AGENT_RUN_API_LISTEN` | Listen address (default: `0.0.0.0:8002`) |
| `[worker]` | `workspace_root` | `AGENT_RUN_WORKSPACE_ROOT` | Base directory for workspace operations |
| `[worker]` | `jobs_dir_name` | `AGENT_RUN_JOBS_DIR_NAME` | Subdirectory name for job worktrees |
| `[worker]` | `agy_path` | `AGENT_RUN_AGY_PATH` | Path to `agy` executable |
| `[worker]` | `default_model` | `AGENT_RUN_DEFAULT_MODEL` | Default model string passed to agent jobs |
| `[worker]` | `default_effort` | `AGENT_RUN_DEFAULT_EFFORT` | Default reasoning effort setting |
| `[worker]` | `default_base_branch` | `AGENT_RUN_DEFAULT_BASE_BRANCH` | Default base branch for repository checkouts |
| `[worker]` | `default_engine` | `AGENT_RUN_DEFAULT_ENGINE` | Default engine for jobs (default: `agy`) |
| `[worker]` | `state_root` | `AGENT_RUN_STATE_ROOT` | Path to persistent SQLite and runtime state |
| `[worker]` | `repos_root` | `AGENT_RUN_REPOS_ROOT` | Base path for bare Git cache repositories |
| `[worker]` | `gc_max_age_days` | `AGENT_RUN_GC_MAX_AGE_DAYS` | Threshold for automatic job garbage collection |
| `[worker]` | `max_parallel` | `AGENT_RUN_MAX_PARALLEL` | Concurrent job execution limit |
| `[worker]` | `shutdown_grace_secs` | `AGENT_RUN_SHUTDOWN_GRACE` | Grace period in seconds for termination |
| `[worker]` | `launch_timeout_secs` | `AGENT_RUN_LAUNCH_TIMEOUT_SECS` | Maximum seconds allowed for job launch |
| `[worker]` | `stall_minutes` | `AGENT_RUN_STALL_MINUTES` | Inactivity threshold before flagging stalled jobs |
| `[worker]` | `max_runtime` | `AGENT_RUN_MAX_RUNTIME` | Hard wall-clock limit for job execution |
| `[worker]` | `hindsight_enabled` | `AGENT_RUN_HINDSIGHT_ENABLED` | Boolean; enables Hindsight memory engine integration |
| `[worker]` | `hindsight_bank` | `AGENT_RUN_HINDSIGHT_BANK` | Bank identifier for Hindsight |
| `[worker]` | `hindsight_tag` | `AGENT_RUN_HINDSIGHT_TAG` | Project tag for memory indexing |
| `[worker]` | `hindsight_path` | `AGENT_RUN_HINDSIGHT_PATH` | Path to hindsight CLI binary |
| `[worker]` | `hindsight_auto_recall` | `AGENT_RUN_HINDSIGHT_AUTO_RECALL` | Boolean; automatically recall context for prompts |
| `[worker]` | `hindsight_auto_retain` | `AGENT_RUN_HINDSIGHT_AUTO_RETAIN` | Boolean; retain execution summary into memory |
| `[worker]` | `hindsight_url` | `AGENT_RUN_HINDSIGHT_URL` | Base URL for remote Hindsight API |
| `[worker]` | `hindsight_profile` | `AGENT_RUN_HINDSIGHT_PROFILE` | Profile name for Hindsight CLI calls |
| `[worker]` | `remote_control` | `AGENT_RUN_REMOTE_CONTROL` | Boolean; enables remote control supervisor |
| `[worker]` | `remote_control_autostart` | `AGENT_RUN_REMOTE_CONTROL_AUTOSTART` | Boolean; automatically starts remote control |
| `[worker]` | `remote_control_check_secs` | `AGENT_RUN_REMOTE_CONTROL_CHECK_SECS` | Health check interval for remote control |
| `[worker]` | `agy_mcp_source` | `AGENT_RUN_AGY_MCP_SOURCE` | Path to template MCP configuration file |
| `[worker]` | `a2a_interrupt_min_secs` | `AGENT_RUN_A2A_INTERRUPT_MIN_SECS` | Minimum seconds between incoming A2A job interrupts (default: 20) |
| `[worker]` | `a2a_mcp_enabled` | `AGENT_RUN_A2A_MCP_ENABLED` | Boolean; injects `mcp-a2a` server into job configuration (default: false) |

### Secret Management Rules

Secrets are never configured inline in the TOML file:
- Bearer tokens must use `api_token_file` or `token_file`. Inline `token` keys are rejected.
- Jobset root keys must use `jobset_root_key_file`. Inline `jobset_root_key` is rejected.
- The worker's Hindsight API key must be supplied via the `AGENT_RUN_HINDSIGHT_API_KEY` environment variable.

If no token is supplied, the server refuses to bind unless `api_allow_unauthenticated = true` is set. Raw secret values are hashed to SHA-256 upon reading and are never logged.

### Configuration Example

```toml
[common]
http_proxy = "http://proxy.example.internal:3128"
api_token_file = "/run/secrets/api-token"
jobset_root_key_file = "/run/secrets/jobset-root-key"

[orchestrator]
listen = "0.0.0.0:8002"
claude_max_crashes = 5
default_session_engine = "claude"

[worker]
max_parallel = 4
default_engine = "agy"
a2a_mcp_enabled = true
```

## Quick Usage

### Starting Services

Start the supervisor:
```bash
export KUBEADP_ROLE=orchestrator
kubeadp-agent
```

Start the worker daemon:
```bash
export KUBEADP_ROLE=worker
kubeadp-agent
```

### Running the Client

Start a background job via the remote CLI:
```bash
agent-run --remote http://agent-run-api:8002 start homelab "Update documentation"
```

Select an explicit execution engine:
```bash
agent-run --remote http://agent-run-api:8002 start --engine agy homelab "Run test suite"
```

Monitor job status:
```bash
agent-run --remote http://agent-run-api:8002 status <job-id>
```

Tail job execution logs:
```bash
agent-run --remote http://agent-run-api:8002 tail <job-id>
```

Inspect sandbox status on a host:
```bash
kubeadp-agent sandbox-probe --json
```

Execute a sandboxed command directly:
```bash
kubeadp-agent sandbox-exec --policy /path/to/policy.json -- /usr/bin/git status
```

# Configuration

This document specifies configuration sources, parameter resolution hierarchy, environment variables, proxy rules, and system limits for agent-run.

## Contents
- [Sources and precedence](#sources-and-precedence) · [Configuration reference table](#configuration-reference-table)
- [Path resolution](#path-resolution) · [Proxy configuration](#proxy-configuration) · [Hindsight settings](#hindsight-settings) · [Resource and operational limits](#resource-and-operational-limits)

## Sources and Precedence

Parameters are resolved in descending order of precedence:
1. Command-line invocation flags (e.g. `--model`, `--max-runtime`).
2. Environment variables (`AGENT_RUN_*`).
3. TOML configuration file located at `<state_root>/config.toml` (or path set by `AGENT_RUN_CONFIG`).
4. Built-in system defaults.

## Configuration Reference Table

| Name | Type | Default | Scope | Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `AGENT_RUN_REMOTE` | String | None | CLI | Base URL of the API server (`--remote`). |
| `AGENT_RUN_CONNECT_TIMEOUT_SECS` | Integer | `10` | CLI | TCP connection timeout in seconds. |
| `AGENT_RUN_HTTP_TIMEOUT_SECS` | Integer | `60` | CLI | Read/write timeout for standard API calls in seconds. |
| `AGENT_RUN_DEBUG` | String | None | CLI | If set and not `"0"`, logs HTTP status and error codes to stderr. |
| `AGENT_RUN_API_LISTEN` | String | `0.0.0.0:8001` | Server | TCP socket address for the server listener. |
| `AGENT_RUN_CONFIG` | Path | `~/.agent-run/config.toml` | Server/Core | Explicit path to server TOML configuration file. |
| `AGENT_RUN_WORKSPACE_ROOT` | Path | `/workspace` | Server/Core | Directory containing repository project checkouts. |
| `AGENT_RUN_JOBS_DIR_NAME` | String | `.agent-jobs` | Server/Core | Directory name inside workspace containing job worktrees. |
| `AGENT_RUN_STATE_ROOT` | Path | `~/.agent-run` | Server/Core | Directory holding `state.sqlite`, `logs/`, and `patches/`. |
| `AGENT_RUN_AGY_PATH` | Path | `/usr/local/libexec/agy` | Server/Core | Path to the `agy` executable binary. |
| `AGENT_RUN_DEFAULT_MODEL` | String | None | Server/Core | Default model identifier passed to `agy`. |
| `AGENT_RUN_DEFAULT_EFFORT` | String | None | Server/Core | Default reasoning effort (`low`, `medium`, `high`). |
| `AGENT_RUN_DEFAULT_BASE_BRANCH` | String | `main` | Server/Core | Default base Git branch for worktree creation. |
| `AGENT_RUN_GC_MAX_AGE_DAYS` | Integer | `14` | Server/Core | Default age threshold in days for job garbage collection. |
| `AGENT_RUN_MAX_PARALLEL` | Integer | `3` | Server/Core | Maximum concurrent running jobs permitted. |
| `AGENT_RUN_SHUTDOWN_GRACE` | Integer | `25` | Server/Core | Grace period (seconds) to wait for running jobs on server shutdown. |
| `AGENT_RUN_LAUNCH_TIMEOUT_SECS`| Integer | `20` | Server/Core | Maximum seconds to confirm job spawn before aborting launch. |
| `AGENT_RUN_STALL_MINUTES` | Integer | `20` | Server/Core | Inactivity minutes before flagging a running job `possibly_stalled`. |
| `AGENT_RUN_MAX_RUNTIME` | String | None | Server/Core | Server default job wall-clock runtime limit (e.g. `30m`, `2h`). |
| `AGENT_RUN_HINDSIGHT` | Boolean | `false` | Server/Core | Enable Hindsight memory recall and retention. |
| `AGENT_RUN_HINDSIGHT_ENABLED` | Boolean | `false` | Server/Core | Alias for `AGENT_RUN_HINDSIGHT`. |
| `AGENT_RUN_HINDSIGHT_BANK` | String | None | Server/Core | Default Hindsight memory bank (falls back to project name). |
| `AGENT_RUN_HINDSIGHT_TAG` | String | None | Server/Core | Document tag for retained memories (defaults to `agent-run`). |
| `AGENT_RUN_HINDSIGHT_PATH` | Path | `hindsight` | Server/Core | Path to the `hindsight` CLI executable. |
| `AGENT_RUN_HINDSIGHT_AUTO_RECALL` | Boolean | `false` | Server/Core | Recall memories into prompt prior to launch. Enables Hindsight if true. |
| `AGENT_RUN_HINDSIGHT_AUTO_RETAIN` | Boolean | `false` | Server/Core | Retain findings upon job completion in Done status. Enables Hindsight if true. |
| `AGENT_RUN_HINDSIGHT_URL` | String | None | Server/Core | Base URL of the upstream Hindsight API service. |
| `AGENT_RUN_HINDSIGHT_API_KEY` | String | None | Server/Core | Authentication token for Hindsight API service. |
| `AGENT_RUN_HINDSIGHT_PROFILE` | String | None | Server/Core | Profile name passed to the Hindsight CLI. |
| `AGENT_RUN_HTTP_PROXY` | String | None | Server/Core | Proxy URL for outbound HTTP requests from spawned processes. |
| `AGENT_RUN_HTTPS_PROXY` | String | None | Server/Core | Proxy URL for outbound HTTPS requests from spawned processes. |
| `AGENT_RUN_NO_PROXY` | String | None | Server/Core | Comma-separated list of destination domains bypassing the proxy. |
| `AGENT_RUN_REMOTE_CONTROL` | Boolean | `false` | Server/Core | Safety gate enabling agy remote-control daemon lifecycle management (`1`/`true`). The daemon instance name is the server hostname, picked up automatically. |
| `AGENT_RUN_REMOTE_CONTROL_AUTOSTART` | Boolean | `false` | Server/Core | Automatically start and supervise the agy remote-control daemon on server boot (`1`/`true`). Requires `AGENT_RUN_REMOTE_CONTROL`. |
| `AGENT_RUN_REMOTE_CONTROL_CHECK_SECS` | Integer | `60` | Server/Core | Interval in seconds between remote-control daemon health checks (minimum `10`). |

## Path Resolution

- Tilde (`~`) prefixes expand to the user's home directory.
- Relative paths for `state_root`, `agy_path`, and `hindsight_path` resolve against `workspace_root`.
- Job worktrees are positioned at `<workspace_root>/<jobs_dir_name>/<project>/<job_id>`.

## Proxy Configuration

Proxy environment variables apply to all child processes spawned by the server (`agy`, `git`, `gh`, `hindsight`).
- Precedence: `AGENT_RUN_*_PROXY` > TOML `*_proxy` > standard system `*_PROXY` / `*_proxy` (case-insensitive).
- In-cluster endpoints (`localhost`, `127.0.0.1`, `0.0.0.0`, `.cluster.local`, `.svc`) are appended to `NO_PROXY` automatically.
- Job overrides: The `--proxy <URL|none>` option on `start` overrides proxies for that specific job run (`"none"` disables all proxies).
- Redaction: Proxy credentials (`http://user:pass@host:port`) are scrubbed from CLI outputs and API JSON payloads.

## Hindsight Settings

- Auto-recall: When active, calls `hindsight memory recall` using the prompt as query and injects findings inside `<recalled-context>`.
- Auto-retain: When active, calls `hindsight memory retain` asynchronously upon terminal `Done` state only.
- Bank derivation: `start --hindsight-bank` > project directory name > `AGENT_RUN_HINDSIGHT_BANK`. Persisted in SQLite.

## Resource and Operational Limits

- **Parallelism limit**: Bounded by `AGENT_RUN_MAX_PARALLEL` (default 3). Submissions beyond this limit fail immediately with 400 `parallel_limit`.
- **Prompt argv limit**: Prompts passed to `execve` must not exceed 100,000 bytes (`PROMPT_ARGV_MAX_BYTES`).
- **File read limit**: `agent-run cat` limits file output to 1,048,576 bytes by default, with a hard maximum of 8,388,608 bytes.
- **SSE concurrency**: `agent-run-api` restricts concurrent event streams to 64 (`MAX_EVENT_STREAMS`). Subsequent requests receive HTTP 503.
- **Export limit**: Uncompressed patch or tar exports exceeding 50 MiB are refused unless `--allow-large` is passed.

## Build and Test Environment Variables

The following variables appear in the codebase for build-time metadata or test suite execution:
- `AGENT_RUN_BUILD_SHA`: Compile-time Git commit SHA captured via `option_env!("AGENT_RUN_BUILD_SHA")` and reported by `agent-run info`.
- `AGENT_RUN_API`: Test harness variable holding the path to the `agent-run-api` executable binary.
- `AGENT_RUN_INIT_TEST_SECRET`: Unit test variable used to verify secret ingestion in `agent-run init --from-env`.
- `AGENT_RUN_TEST_GH_TOKEN`: Unit test variable verifying credential masking during OAuth subprocess execution.
- `AGENT_RUN_TEST_HINDSIGHT_URL`: Unit test variable verifying credential masking during OAuth subprocess execution.

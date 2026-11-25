# Architecture

This document describes the architectural model, process lifecycle, worktree isolation boundaries, state storage, and integration pathways of agent-run.

## Contents
- [Components and Boundaries](#components-and-boundaries)
- [Job Lifecycle and State Machine](#job-lifecycle-and-state-machine)
- [Worktree and Branch Model](#worktree-and-branch-model)
- [On-Disk Layout and State Storage](#on-disk-layout-and-state-storage)
- [Process Supervision Model](#process-supervision-model)
- [Job Log Format](#job-log-format)
- [Hindsight Memory Integration](#hindsight-memory-integration)
- [Request Flow of `start`](#request-flow-of-start)

## Components and Boundaries

The system consists of the agent-run crates communicating over HTTP and POSIX process interfaces:

```text
+------------------+         HTTP (REST / SSE)         +--------------------+
|  agent-run (CLI) | --------------------------------> |   agent-run-api    |
+------------------+                                   +--------------------+
                                                                  |
                                                         fork / exec / setsid
                                                                  v
                                                       +--------------------+
                                                       |     agy agent      |
                                                       +--------------------+
                                                                  |
                                                         reads / writes / git
                                                                  v
                                                       +--------------------+
                                                       | Git Worktree (.git)|
                                                       +--------------------+
```

- `agent-run-cli`: Stateless remote command client that parses CLI arguments, sends HTTP requests, and formats server responses.
- `agent-run-api`: Long-running HTTP service that coordinates execution, manages SQLite state, and manages child processes. Its HTTP router and handlers live in `agent-core` (module `agent_core::server`, feature `server`) and are re-exported here.
- `agent-orchestrate`: Process orchestrator for the ai-server pod's agent sessions; it re-exports the same `agent_core::server` HTTP routes so the `agent-run` CLI works against it unchanged.
- `agent-core`: Shared engine implementing the job lifecycle, Git worktree operations, process spawning, Hindsight logic, and the axum-based HTTP server module.

## Job Lifecycle and State Machine

A job transitions through distinct operational and terminal states computed by `compute_status` based on process liveness, recorded database metadata, and captured logs.

| State | Type | Description and transition conditions |
| :--- | :--- | :--- |
| `running` | Active | The PID exists in `/proc/<pid>`, matches `pid_start_time`, and is not a zombie. |
| `done` | Terminal | Process exited with code 0, log contains structured JSON `status: "SUCCESS"` with non-empty `response`, and no pending background task signal. |
| `quota-exhausted` | Terminal | The latest attempt log contains provider quota or rate limit exhaustion strings. Wins over other non-done signals. |
| `incomplete` | Terminal | Process exited with code 0 without structured SUCCESS response text, OR log contains agy's background task cutoff notice ("terminating ... background task(s) on exit"). |
| `killed` | Terminal | Process terminated via `agent-run kill` or `max_runtime` enforcement; row marked `status = 'killed'` and `exit_code = 137`. |
| `self-terminated` | Terminal | Process exited with signal 137/143 without structured error JSON, or terminated unrecorded without structured error. |
| `failed` | Terminal | Process exited with non-zero exit code (other than bare 137/143) or log contains structured error JSON (`status: "ERROR"` / `"FAILED"`). |

Any terminal job can transition back to `running` via `agent-run retry`, which re-launches `agy` in the existing worktree with an incremented `retry_count`.

## Worktree and Branch Model

- **Location**: Worktrees reside at `<workspace_root>/.agent-jobs/<project>/<job_id>`. This keeps job directories completely outside the base project checkout (`<workspace_root>/<project>`).
- **Branch naming**: Each job operates on a dedicated branch named `agent-run/<job_id>`, branched from the specified `--ref` (defaulting to the project's base branch or `main`).
- **External worktrees**: When launched with `--from-worktree <PATH>`, `agent-run` skips worktree creation. External worktrees and branches are never deleted during cleanup.
- **Lifetime**: Created at `start`; preserved across `retry` and `review`; removed on `clean` or `gc`.

## On-Disk Layout and State Storage

State files are located under `state_root` (default `~/.agent-run`):

```text
~/.agent-run/
├── config.toml           # Server configuration file (optional)
├── state.sqlite          # SQLite database tracking job metadata
├── state.sqlite-wal      # Write-Ahead Log journal file
├── patches/              # Temporary patch files for --from-patch
└── logs/
    └── <job_id>.log      # Raw combined stdout/stderr log
```

### SQLite Schema Essentials
The `jobs` table maintains job records:
- `job_id` (TEXT PRIMARY KEY): 8-character hex string.
- `project`, `prompt`, `resolved_prompt`, `mode`, `worktree_path`, `branch`, `base_ref`: Job arguments and Git metadata.
- `created_at` (INTEGER): Epoch nanoseconds for unambiguous temporal ordering.
- `started_at` (INTEGER): Epoch seconds of the current attempt, reset by `retry`.
- `pid` (INTEGER), `pid_start_time` (INTEGER): Process ID and `/proc/<pid>/stat` start tick.
- `exit_code` (INTEGER), `status` (TEXT): Final execution metrics.
- `conversation_id` (TEXT): `agy` session UUID extracted from log output.
- `retry_count` (INTEGER): Attempt counter incremented on each retry.
- `hindsight` (INTEGER), `auto_recall` (INTEGER), `auto_retain` (INTEGER), `hindsight_bank` (TEXT): Memory settings.
- `max_runtime` (INTEGER): Configured wall-clock duration limit in nanoseconds.

Database access runs with SQLite WAL mode and a 5-second busy timeout.

## Process Supervision Model

- **Session isolation**: Spawned `agy` processes call `setsid()` in a `pre_exec` hook, creating an independent session and process group (`pgid == pid`).
- **Child subreaper**: The server designates itself as a child subreaper via `prctl(PR_SET_CHILD_SUBREAPER, 1)` to adopt background processes spawned by `agy`.
- **Zombie reaper**: When running as PID 1, a server background thread executes `waitpid(-1, WNOHANG)` every 100ms. Tracked child PIDs awaiting `Child::wait()` are skipped.
- **Launch handshake**: Coordination between launcher and monitor threads uses an atomic state machine (`LAUNCH_STATE_PENDING` -> `RUNNING` or `CANCELLED`). If PID confirmation exceeds `launch_timeout_secs` (default 20s), the launcher transitions to `CANCELLED`, kills the child process group, and cleans up.
- **Runtime enforcement**: An enforcer ticker evaluates active jobs every 10 seconds against `--max-runtime` or `AGENT_RUN_MAX_RUNTIME`. Exceeded jobs receive SIGTERM followed by SIGKILL.
- **Graceful shutdown**: Upon SIGTERM/SIGINT, the server sends SIGTERM to all running jobs and waits up to `shutdown_grace_secs` (default 25s) for jobs to write final state and exit.
- **Remote-control daemon and supervisor**: The background `agy remote-control serve` process outlives its `start` launcher and is adopted by the server (via subreaper or PID 1 adoption). When `AGENT_RUN_REMOTE_CONTROL_AUTOSTART` is active, an asynchronous supervisor task spawned after HTTP listener binding ensures daemon persistence. The supervisor executes non-blocking health checks and restarts off the async worker pool (`spawn_blocking`), polling at `AGENT_RUN_REMOTE_CONTROL_CHECK_SECS` intervals. Crashed daemons are restarted automatically, with failed calls retrying via exponential backoff (capped at 5 minutes). An in-memory suppression flag engaged by `disable` suppresses supervisor restarts until `enable` is called or the server restarts. State transitions (`started`, `found inactive and restarted`, `error`) are logged to stderr. Daemon processes remain exempt from SQLite job tracking and graceful shutdown drains, being reaped as adopted orphans upon exit.

## Job Log Format

`agent-run` invokes `agy` with `--output-format stream-json`. Captured job log files (`/home/agent/.agent-run/logs/<job>.log` on the server, viewed with `agent-run log <id>` and streamed with `agent-run tail <id>`) combine stdout and stderr into a single raw file. The log consists of two line types:

1. **`agent-run` lifecycle lines**: Prefixed with `[<RFC3339 timestamp>] [<component>] message` (for example, `[hindsight] Successfully recalled context from bank ...` or `[agy] Recovered conversation ID ...`).
2. **`agy` NDJSON event stream**: Single-line JSON objects emitted sequentially by `agy`:
   - `init`: Emitted first upon process startup: `{"event":"init","conversation_id":...,"init":{model,cwd,tools,...}}`.
   - `step_update`: Emitted continuously as agent turns execute: `{"event":"step_update","step_update":{conversation_id,step_index,state:ACTIVE|DONE,step_type:user_input|agent_response|tool,tool_name (tool steps),duration_seconds,usage,...}}`. Because these events stream incrementally during user prompts, tool executions, and assistant responses, the log remains live while the job is running.
   - `result`: Emitted upon execution completion: `{"event":"result","result":{conversation_id,status:SUCCESS|ERROR,response,error,...}}`.

An `agy` failure prior to executing any steps (such as invalid CLI options or model selection errors) produces a single `result` event with status `ERROR`, an empty `conversation_id`, and failure details in `error`, without preceding `init` or `step_update` events.

`agent-run` derives runtime state, metrics, and terminal artifacts directly from the stream:
- **Conversation tracking**: The session UUID is extracted from the top-level `conversation_id` field of the initial `init` event at the start of the log.
- **Terminal results**: `agent-run result` extracts the final response text from `response` on success, or the failure message from `error` on failure, directly from the terminal `result` event.
- **Status determination**: Terminal classification evaluates the nested `result.status` field (`SUCCESS` maps to `done`, `ERROR` maps to `failed`).
- **Background task notices**: `agy` background-task wait notices ("terminating ... background task(s) on exit") appear as plain text on stderr. All JSON lines—including `step_update` lines whose text deltas quote the notice—are ignored by background-task detection to prevent agent text from triggering an `incomplete` status.
- **Quota exhaustion filtering**: Upstream rate-limit and quota detection ignores all `step_update` lines. Agent dialogue, tool output, or documentation references that merely mention rate limits cannot flag the job as `quota-exhausted`.
- **Raw stream inspection**: Both `log` and `tail` print output lines raw without pretty rendering (none is planned). Structured step records can be filtered with `jq`:
  ```bash
  agent-run log <id> | grep '^{' | jq -c 'select(.event=="step_update") | .step_update | {step_index,state,step_type,tool_name}'
  ```

## Hindsight Memory Integration

- **Auto-recall**: Executed prior to launching `agy` when `auto_recall` is active. Calls `hindsight -o json memory recall <bank> "<query>"` with the recall query: the complete prompt text by default, or the `--hindsight-query` text when provided (stored per job and reused by `retry`). The response is wrapped in `<recalled-context>` blocks and prepended to the prompt; the prompt itself is not modified.
- **Query length limit**: The upstream Hindsight API rejects search queries longer than 500 tokens (about 2,000 characters). The query is the prompt by default, or the `--hindsight-query` text when provided; when the resulting query exceeds the limit, recall is skipped, the system logs a warning and proceeds with the unmodified prompt.
- **Auto-retain**: Executed asynchronously upon job completion only when `status` is `Done`. Sends a 200-character summary and the final 2000 characters of the log via `hindsight -o json memory retain <bank> "<finding>" --document-tags agent-run --context "<context>" --async`.

## Request Flow of `start`

1. **Client resolution**: `agent-run start` resolves local `@prompt` or `--from-patch` paths into payload strings and POSTs to `/api/v1/jobs`.
2. **Server admission**: Validates project existence, ensures prompt length <= 100,000 bytes (`PROMPT_ARGV_MAX_BYTES`), and checks active jobs against `max_parallel`.
3. **Worktree preparation**: Creates Git branch `agent-run/<job_id>` and worktree at `<workspace_root>/.agent-jobs/<project>/<job_id>`. Applies patch if present.
4. **Memory recall**: If enabled, invokes Hindsight recall and injects context into the prompt.
5. **Process execution**: Launches `agy` in detached process group, saves metadata row to SQLite, starts monitoring thread, and returns `CommandOutput`.

# API Reference

This document specifies the HTTP wire protocols, REST endpoints, Server-Sent Events schemas, and error definitions for agent-run-api.

## Contents
- [Transport and conventions](#transport-and-conventions) · [Error model and taxonomy](#error-model-and-taxonomy) · [Endpoint index](#endpoint-index)
- [Job lifecycle endpoints](#job-lifecycle-endpoints) · [Worktree endpoints](#worktree-endpoints) · [Server introspection endpoints](#server-introspection-endpoints) · [Remote control endpoints](#remote-control-endpoints) · [Authentication and health endpoints](#authentication-and-health-endpoints) · [Server-sent events](#server-sent-events) · [Concurrency guarantees](#concurrency-guarantees)

## Transport and Conventions

- **Base URL**: Default in-cluster endpoint `http://kubeadp-agent-worker-service.opencode.svc:8002`.
- **Content types**: Standard endpoints accept and return `application/json`. Log and diff return `text/plain; charset=utf-8`. Export returns `application/octet-stream`. Events stream as `text/event-stream`.
- **Asynchronous model**: `POST /api/v1/jobs` starts an agent process in the background and returns immediately with `CommandOutput`. Subsequent monitoring is handled via polling (`/status`, `/wait`, `/log`) or SSE (`/events`).
- **Response envelope**: Standard JSON responses serialize `CommandOutput`:
  ```json
  {"stdout_lines": ["..."], "stderr_lines": ["..."], "exit_code": 0, "data": {}}
  ```

## Error Model and Taxonomy

Failed requests return JSON with HTTP status 400, 404, 409, 500, or 503:
```json
{"error": "<human readable message>", "code": "<machine_readable_code>"}
```

### Rejection Shape (HL-51)

Every extractor rejection -- an invalid JSON body, a wrong content type, an
oversized body, or a malformed query/path parameter -- also answers the same
`{"error","code"}` JSON shape, keeping the status code axum chose:

| Status | Code | Cause |
| :--- | :--- | :--- |
| 400 | `invalid_body` | Body is not valid JSON or the request body was unreadable. |
| 400 | `invalid_query` | Query string fails to parse (bad percent-encoding, wrong types). |
| 400 | `invalid_path` | Path parameter fails to parse. |
| 413 | `too_large` | Request body exceeds the server's body limit. |
| 415 | `invalid_content_type` | Body is present without `application/json` content type. |
| 422 | `invalid_body` | Well-formed JSON whose fields do not match the handler. |

### Complete Error Code Reference

| Status | Code | Meaning |
| :--- | :--- | :--- |
| 400 | `discard_no_paths` | Discard request contained an empty paths list. |
| 400 | `discard_refused` | Named file is tracked, outside worktree, contains `..`, or is a directory. |
| 400 | `duration_negative` | Negative duration string provided for GC threshold. |
| 400 | `export_empty` | No changes found to export into patch or tar archive. |
| 400 | `export_too_large` | Uncompressed export exceeds 50 MiB without `allow_large = true`. |
| 400 | `export_uncommitted` | Worktree has uncommitted modifications and no commit message was supplied. |
| 400 | `git_ref_not_found` | Branch or ref specified by `--ref` does not exist in the repository. |
| 400 | `hindsight_query_empty` | `--hindsight-query` text is empty after trimming. |
| 400 | `hindsight_query_too_long` | `--hindsight-query` text exceeds 2,000 characters. |
| 400 | `hindsight_query_without_recall` | `--hindsight-query` is combined with `--hindsight off`. |
| 400 | `invalid_duration` | Malformed duration string syntax. |
| 400 | `invalid_pathspec` | Pathspec contains empty, absolute, or escaping `..` segments. |
| 400 | `invalid_query` | Query parameter contains malformed percent-encoding. |
| 400 | `invalid_body` | Request body is not valid JSON (or unreadable); also used for 422 well-formed-but-invalid fields. |
| 400 | `invalid_request` | Malformed JSON request body or unresolvable client payload. |
| 400 | `invalid_status` | Status filter value unrecognized by state machine. |
| 400 | `job_id_already_exists`| Generated or requested job ID collides with existing row. |
| 400 | `job_id_ambiguous` | Hexadecimal prefix matches multiple active or historical jobs. |
| 400 | `job_running` | Mutating operation refused because target job PID is active. |
| 400 | `must_be_provided_together` | `--from-env` and `--to` must be supplied simultaneously. |
| 400 | `not_a_git_checkout` | Project directory is not a working Git checkout. |
| 400 | `not_a_git_repository` | Project directory lacks a valid `.git` repository. |
| 400 | `parallel_limit` | Running jobs count reaches `max_parallel` limit. |
| 400 | `patch_apply_failed` | Git failed to apply `--from-patch` contents to clean worktree. |
| 400 | `patch_file_not_found` | Patch file path does not exist on disk. |
| 400 | `path_not_found` | Target path does not exist inside worktree. |
| 400 | `project_not_a_directory`| Project path exists but is not a directory. |
| 400 | `project_not_found` | Target project directory does not exist under workspace root. |
| 400 | `prompt_empty` | Prompt text contains only whitespace or is empty. |
| 400 | `prompt_file_not_found` | File referenced by `@<path>` does not exist. |
| 400 | `prompt_path_is_directory` | File referenced by `@<path>` is a directory. |
| 400 | `retry_already_completed` | Retry refused because job finished in `Done` state without follow-up prompt. |
| 400 | `retry_duplicate_attempt`| Retry collision: attempt counter was incremented concurrently. |
| 400 | `retry_no_conversation`| Job log contains no UUID conversation identifier to resume. |
| 400 | `retry_worktree_missing`| Worktree directory missing from disk; retry impossible. |
| 400 | `review_no_diff` | Target job worktree contains no diff to review. |
| 400 | `selection_matched_nothing` | Filter pathspecs matched no modified files. |
| 400 | `worktree_in_use` | Worktree path is currently checked out by another process. |
| 400 | `worktree_not_found` | Worktree directory does not exist. |
| 400 | `worktree_removal_failed`| Worktree deletion left non-bookkeeping files on disk. |
| 403 | `remote_control_disabled` | Remote control daemon management is disabled on the server (`AGENT_RUN_REMOTE_CONTROL`). |
| 404 | `auth_not_in_progress` | No interactive OAuth process is currently parked. |
| 404 | `job_not_found` | Job ID or prefix does not match any database record. |
| 404 | `log_not_found` | No log file exists on disk for the specified job. |
| 409 | `auth_in_progress` | An interactive OAuth flow is already active on the server. |
| 413 | `too_large` | Request body exceeds the server's body-size limit. |
| 415 | `invalid_content_type` | Body present without `application/json` content type. |
| 422 | `invalid_body` | Well-formed JSON with missing or invalid handler fields. |
| 429 | `rate_limited` | Bearer-auth failures exceeded the per-peer threshold within the window (with `Retry-After`). |
| 500 | `internal` | Unclassified I/O, database, or process failure. |
| 500 | `remote_control_failed` | Remote control daemon execution failed or timed out. |
| 503 | `not_ready` | `/readyz` reports the server cannot serve (`{"error","code":"not_ready"}`). |
| 503 | `too_many_streams` | Active SSE connections have reached the 64-stream capacity limit. |
| 401 | `unauthorized` | Bearer token missing or wrong when a token is configured. |

## Endpoint Index

| Method | Path | Purpose |
| :--- | :--- | :--- |
| `POST` | `/api/v1/jobs` | Launch a new background agent job. |
| `GET` | `/api/v1/jobs` | List historical and running jobs with optional filters. |
| `POST` | `/api/v1/jobs/wait` | Block until one or all jobs reach a terminal state. |
| `GET` | `/api/v1/jobs/{job_id}` | Retrieve status and metadata for a job. |
| `DELETE` | `/api/v1/jobs/{job_id}/clean` | Remove worktree, branch, and metadata row. |
| `POST` | `/api/v1/jobs/{job_id}/kill` | Terminate running job process group. |
| `POST` | `/api/v1/jobs/{job_id}/retry` | Resume or retry execution in existing worktree. |
| `POST` | `/api/v1/jobs/{job_id}/commit` | Commit worktree changes. |
| `POST` | `/api/v1/jobs/{job_id}/discard` | Delete named untracked files from worktree. |
| `GET` | `/api/v1/jobs/{job_id}/export` | Export committed changes as patch or tar stream. |
| `POST` | `/api/v1/jobs/{job_id}/review` | Spawn review agent over job worktree diff. |
| `GET` | `/api/v1/jobs/{job_id}/log` | Read raw captured stdout/stderr log bytes. |
| `GET` | `/api/v1/jobs/{job_id}/diff` | Retrieve unified diff or diffstat. |
| `GET` | `/api/v1/jobs/{job_id}/files` | List files and directories in worktree. |
| `GET` | `/api/v1/jobs/{job_id}/cat` | Stream text file content from worktree. |
| `GET` | `/api/v1/jobs/{job_id}/result` | Fetch structured result response text. |
| `GET` | `/api/v1/jobs/{job_id}/events` | Stream real-time Server-Sent Events. |
| `GET` | `/api/v1/projects` | List available workspace repository projects. |
| `POST` | `/api/v1/gc` | Garbage collect terminal jobs. |
| `POST` | `/api/v1/init` | Verify/initialize state and extract secret files. |
| `GET` | `/api/v1/auth/status` | Query validity of stored agent credentials. |
| `GET` | `/api/v1/info` | Snapshot server capability, config, and disk health. |
| `GET` | `/api/v1/models` | List supported agent model aliases. |
| `GET` | `/api/v1/quota` | Query provider token quotas and rate limits. |
| `POST` | `/api/v1/remote-control/enable` | Enable and start the agy remote-control daemon. |
| `POST` | `/api/v1/remote-control/disable` | Stop the agy remote-control daemon. |
| `GET` | `/api/v1/remote-control/status` | Query status of the agy remote-control daemon. |
| `POST` | `/api/v1/auth` | Initiate interactive OAuth login flow. |
| `POST` | `/api/v1/auth/code` | Submit OAuth verification code to complete login. |
| `POST` | `/api/v1/auth/cancel` | Abort active OAuth process and free lock. |
| `GET` | `/healthz` | Unauthenticated liveness probe. |
| `GET` | `/readyz` | Unauthenticated readiness probe: `{"status": "ready"}` when the server can serve, else `503` `not_ready`. |
| `GET` | `/metrics` | Prometheus text metrics. When a bearer token is configured it sits behind the same auth as `/api/v1`. |

---

## Job Lifecycle Endpoints

### `POST /api/v1/jobs`
Request fields (JSON): `project` (string, required), `prompt` (string, required), `git_ref` (string), `model` (string), `effort` (string), `sandbox` (bool), `add_dirs` (string[]), `print_timeout` (string), `hindsight` (`"on"`\|`"off"`), `hindsight_bank` (string), `hindsight_query` (string), `proxy` (string), `from_worktree` (string), `from_patch_content` (string), `plan` (bool), `max_runtime` (string).
Success: 200 OK with `CommandOutput.data` carrying `job_id`, `project`, `branch`, `worktree`, `status`, `pid`.
Errors: 400 `parallel_limit`, `prompt_empty`, `project_not_found`, `patch_apply_failed`.

### `GET /api/v1/jobs`
Query parameters: `project` (string), `status` (string, repeatable or comma-separated).
Success: 200 OK with `CommandOutput.data.jobs` array.
Errors: 400 `invalid_status`, `invalid_query`.

### `POST /api/v1/jobs/wait`
Request fields (JSON): `job_ids` (string[], required), `any` (bool), `all` (bool), `timeout` (string).
Success: 200 OK with `CommandOutput.data` mapping job IDs to final statuses (`exit_code`: 0 = all Done, 2 = terminal not Done, 3 = timeout).

### `GET /api/v1/jobs/{job_id}`
Path: `{job_id}` (exact ID or unique prefix).
Success: 200 OK with `CommandOutput.data` (`job_id`, `status`, `elapsed_seconds`, `worktree`, optional activity fields).
Errors: 404 `job_not_found`, 400 `job_id_ambiguous`.

### `DELETE /api/v1/jobs/{job_id}/clean`
Query parameters: `force` (bool), `purge_log` (bool).
Success: 200 OK with `CommandOutput.data.job_id`.
Errors: 400 `job_running`, `worktree_removal_failed`, 404 `job_not_found`.

### `POST /api/v1/jobs/{job_id}/kill`
Success: 200 OK with `CommandOutput.data.job_id`.
Errors: 404 `job_not_found`.

### `POST /api/v1/jobs/{job_id}/retry`
Request fields (JSON): `hindsight` (`"on"`\|`"off"`), `prompt` (string).
Success: 200 OK with `CommandOutput.data` (`job_id`, `retry_count`, `pid`, `status`).
Errors: 400 `job_running`, `retry_already_completed`, `retry_no_conversation`, `retry_worktree_missing`.

### `GET /api/v1/jobs/{job_id}/result`
Success: 200 OK with `CommandOutput.data` (`job_id`, `status`, `response`).
Errors: 404 `job_not_found`.

---

## Worktree Endpoints

### `POST /api/v1/jobs/{job_id}/commit`
Request fields (JSON): `message` (string), `only` (string[]), `exclude` (string[]).
Success: 200 OK with `CommandOutput.data` (`commit_sha`, `files_committed`).
Errors: 400 `job_running`, `invalid_pathspec`, `selection_matched_nothing`.

### `POST /api/v1/jobs/{job_id}/discard`
Request fields (JSON): `paths` (string[], required).
Success: 200 OK with `CommandOutput.data.discarded` string array.
Errors: 400 `discard_no_paths`, `discard_refused`, `job_running`.

### `GET /api/v1/jobs/{job_id}/export`
Query parameters: `format` (`"patch"`\|`"tar"`), `message` (string), `only` (repeatable), `exclude` (repeatable), `allow_large` (bool).
Success: 200 OK with `application/octet-stream` body and summary headers (`X-Agent-Run-Export-Commits`, `X-Agent-Run-Export-Files`, `X-Agent-Run-Export-Insertions`, `X-Agent-Run-Export-Deletions`, `X-Agent-Run-Export-Deleted`, `X-Agent-Run-Export-Committed-Files`, `X-Agent-Run-Export-Project`).
Errors: 400 `export_empty`, `export_too_large`, `export_uncommitted`, `job_running`.

### `POST /api/v1/jobs/{job_id}/review`
Request fields (JSON): `reviewer_model` (string).
Success: 200 OK with `CommandOutput.data` (`review_job_id`, `target_job_id`, `status`).
Errors: 400 `worktree_not_found`, `review_no_diff`.

### `GET /api/v1/jobs/{job_id}/log`
Query parameters: `offset` (integer, default 0).
Success: 200 OK with `text/plain; charset=utf-8` body and `Agent-Run-Log-Offset` header.
Errors: 404 `log_not_found`, `job_not_found`.

### `GET /api/v1/jobs/{job_id}/diff`
Query parameters: `base` (bool), `stat` (bool), `only` (repeatable), `exclude` (repeatable).
Success: 200 OK. If `stat=true`, returns JSON `CommandOutput`. Otherwise returns `text/plain; charset=utf-8` patch with optional `Agent-Run-Diff-Note` header.
Errors: 400 `invalid_pathspec`, 404 `job_not_found`.

### `GET /api/v1/jobs/{job_id}/files`
Query parameters: `path` (string), `depth` (integer, 1..=4).
Success: 200 OK with JSON array of `{path, is_dir, size}`.
Errors: 400 `path_not_found`, `worktree_not_found`.

### `GET /api/v1/jobs/{job_id}/cat`
Query parameters: `path` (string, required), `lines` (string `A:B`), `max_bytes` (integer).
Success: 200 OK with `CommandOutput.stdout_lines` containing requested file lines.
Errors: 400 `path_not_found`, `worktree_not_found`.

---

## Server Introspection Endpoints

- `GET /api/v1/projects`: Returns list of available Git repos with checkout branch, head commit, and job count.
- `GET /api/v1/info`: Returns server version, roots, memory status, disk space, and toolchain versions.
- `GET /api/v1/models`: Returns list of models supported by the server agent installation.
- `GET /api/v1/quota`: Query `?fresh=true|false`. Returns quota windows and remaining request percentages.
- `POST /api/v1/gc`: Request fields: `older_than`, `project`, `max_jobs`, `dry_run`, `purge_logs`. Deletes expired jobs.

---

## Remote Control Endpoints

- `POST /api/v1/remote-control/enable`: Starts and registers the `agy remote-control` background daemon under a static name matching the server hostname. Idempotent if already active. Returns 200 with `CommandOutput.data` carrying `active` (true), `pid` (integer), `instance_name` (string), `message` (string). Errors: 403 `remote_control_disabled` (when `AGENT_RUN_REMOTE_CONTROL` is unset or false), 500 `remote_control_failed`.
- `POST /api/v1/remote-control/disable`: Stops the `agy remote-control` background daemon and unregisters the instance. Idempotent if already inactive. Returns 200 with `CommandOutput.data` carrying `active` (false), optional `instance_name` (string), `message` (string). Errors: 500 `remote_control_failed`.
- `GET /api/v1/remote-control/status`: Queries the status of the `agy remote-control` background daemon. Returns 200 with `CommandOutput.data` carrying `active` (bool), optional `pid` (integer), optional `instance_name` (string, omitted when not assigned yet), `message` (string). Errors: 500 `remote_control_failed`.

---

## Authentication and Health Endpoints

- **Bearer security**: Every `/api/v1/*` route lives in one authenticated sub-router. When a bearer token is configured (agent-orchestrate via `AGENT_ORCHESTRATE_TOKEN` / `AGENT_ORCHESTRATE_TOKEN_FILE`), every `/api/v1` route -- read and write alike -- requires `Authorization: Bearer <token>` and answers `401 {"error":"unauthorized","code":"unauthorized"}` otherwise; comparison is constant-time and repeated failures are throttled per peer to `429 rate_limited` with `Retry-After`. When no token is configured (agent-run-api's network-policy-only model) the same routes are reachable without credentials and the responses are unchanged. The liveness and readiness probes are never gated.
- `GET /api/v1/auth/status`: Returns `{"authenticated": bool}` without spawning agent.
- `POST /api/v1/auth`: Starts OAuth flow. Returns 200 with `{authenticated: true}` or `{authenticated: false, oauth_url: "..."}`. Returns 409 `auth_in_progress` if a flow is already parked.
- `POST /api/v1/auth/code`: Body `{"code": "..."}`. Relays authorization code to parked process. Returns 200 on success. Returns 404 `auth_not_in_progress` if no flow is parked.
- `POST /api/v1/auth/cancel`: Aborts active OAuth child process. Returns 200 `{cancelled: true}`. Returns 404 `auth_not_in_progress` if no flow is active.
- `POST /api/v1/init`: Sets up directories and writes `--from-env` to `--to`.
- `GET /healthz`: Unauthenticated liveness probe. Returns `{"status": "ok"}`.
- `GET /readyz`: Unauthenticated readiness probe. Returns `{"status": "ready"}` when the server is ready to serve (for agent-run-api: the workspace/state dir is usable; for agent-orchestrate: the state store / sessions registry is readable). Returns `503` with `{"error":"not ready","code":"not_ready"}` otherwise.
- `GET /metrics`: Prometheus text format (`agent_process_uptime_seconds`, `agent_state{status="..."}` gauges of jobs by status / sessions by state, and `agent_requests_total{class="..."}` request counters by status class). When a bearer token is configured `/metrics` requires it, exactly like `/api/v1`.
- **Server-side paths never leak**: the `job_not_found` / `session_not_found` bodies carry only `{"error":"job not found","code":"job_not_found"}` / `{"error":"no session named '...'","code":"session_not_found"}`; the server-side `state.sqlite` path is logged server-side and never sent to the client.

---

## Server-Sent Events

`GET /api/v1/jobs/{job_id}/events` streams job progress as `text/event-stream`. Optional query `?offset=<n>` resumes log streaming from byte offset `n`.

Wire event types:
- `event: status`: Initial connection frame and any subsequent status change.
  ```text
  event: status
  data: {"status":"running","elapsed_seconds":12}
  ```
- `event: log`: Emitted every 500ms when new log bytes arrive (capped at 64 KiB per frame).
  ```text
  event: log
  data: {"offset":1048,"text":"Compiling agent-run v0.1.0\n"}
  ```
- `event: end`: Emitted once upon reaching a terminal state; connection closes immediately after.
  ```text
  event: end
  data: {"status":"done"}
  ```
- `: keepalive`: SSE comment sent every 15 seconds to prevent intermediate proxy timeouts.

Capacity limit: Server allows at most 64 concurrent streams. Excess requests receive 503 `too_many_streams`.

## Concurrency Guarantees

- State updates use atomic SQLite single-row conditional UPDATEs guarded by `retry_count` and state checks.
- Exactly one interactive OAuth flow can be in-progress simultaneously across the entire server (enforced by `AuthManager`).
- Job launches serialize `max_parallel` checks against active PID table rows.

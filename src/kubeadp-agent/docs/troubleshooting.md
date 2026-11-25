# Troubleshooting

This document provides operational failure diagnostics, resolution steps for common symptoms, and an error code index for agent-run.

## Symptom Resolution Matrix

| Symptom | Probable cause | Resolution |
| :--- | :--- | :--- |
| `start` fails with `parallel_limit` | Running jobs count reaches `max_parallel` (default 3). | Wait for jobs with `agent-run wait`, or raise `AGENT_RUN_MAX_PARALLEL`. |
| `start` fails with `Prompt exceeds maximum size` | Resolved prompt exceeds 100,000 bytes. | Shorten the prompt text or split instructions across multiple steps. |
| Job enters `quota-exhausted` | Upstream provider token limits or rate limits reached. | Inspect `agent-run quota --fresh` for reset times, or pass `--model <ALT>`. |
| Job enters `incomplete` | Process exited without structured JSON or cut off pending work. | Inspect `agent-run log <id>` and resume via `agent-run retry <id>`. |
| Job enters `self-terminated` | Process killed abruptly by SIGTERM/SIGKILL or node restart. | Check host memory and dmesg; resume via `agent-run retry <id>`. |
| `clean` or `commit` fails with `job_running` | Job PID is active in `/proc/<pid>`. | Terminate process with `agent-run kill <id>`, or pass `--force` to `clean`. |
| `clean` reports `worktree kept` | Non-bookkeeping files remained after `git worktree remove`. | Manually inspect paths listed in warning, then run `rm -rf <worktree_path>`. |
| `auth` fails with `auth_in_progress` (409) | Another OAuth authentication process is parked on server. | Execute `agent-run cancel` to terminate process and free the auth lock. |
| `watch` fails with `too_many_streams` (503) | Server reached 64 concurrent SSE connections. | Terminate unused `agent-run watch` streams, or monitor using `agent-run tail`. |
| HTTP connection refused or timeout | Daemon not listening, or network policy blocking client. | Verify pod liveness on port 8002 and inspect `CiliumNetworkPolicy` logs. |
| `remote-control start` or `status` slow on first call | Daemon cold start after an idle period can take up to 60 s (subsequent calls take 1–2 s). Command timeouts are raised to 180 s server-side (CLI read timeout 200 s). | Allow cold start to complete; verify daemon status with `agent-run remote-control status`. If persistently unresponsive due to stale state, exec into the container and restart the daemon. |

## Error Code Reference

The server returns stable snake_case identifiers in the `code` field of error responses:

- `auth_in_progress`: Active OAuth flow already running (HTTP 409).
- `auth_not_in_progress`: No OAuth flow currently parked on server (HTTP 404).
- `discard_no_paths` / `discard_refused`: Invalid paths supplied to `discard` (HTTP 400).
- `duration_negative` / `invalid_duration`: Unparseable duration format (HTTP 400).
- `export_empty` / `export_too_large` / `export_uncommitted`: Export validation failures (HTTP 400).
- `git_ref_not_found`: Requested Git ref does not exist in project repository (HTTP 400).
- `hindsight_query_empty` / `hindsight_query_too_long` / `hindsight_query_without_recall`: Invalid Hindsight recall query supplied to `start` (HTTP 400).
- `invalid_pathspec`: Escaping, absolute, or empty pathspec provided (HTTP 400).
- `invalid_query` / `invalid_request`: Malformed parameters or JSON payload (HTTP 400).
- `invalid_status`: Unrecognized status string in query filter (HTTP 400).
- `job_id_already_exists` / `job_id_ambiguous`: Job ID collision or ambiguous prefix (HTTP 400).
- `job_not_found` / `log_not_found`: Referenced job ID or log file missing (HTTP 404).
- `job_running`: Mutating command attempted on an active running job (HTTP 400).
- `must_be_provided_together`: `--from-env` and `--to` must both be supplied (HTTP 400).
- `not_a_git_checkout` / `not_a_git_repository`: Project lacks valid Git metadata (HTTP 400).
- `parallel_limit`: Max parallel job limit reached (HTTP 400).
- `patch_apply_failed` / `patch_file_not_found`: Git patch application failure (HTTP 400).
- `path_not_found`: Target file or directory missing in worktree (HTTP 400).
- `project_not_a_directory` / `project_not_found`: Target project path invalid (HTTP 400).
- `prompt_empty` / `prompt_file_not_found` / `prompt_path_is_directory`: Invalid prompt input (HTTP 400).
- `retry_already_completed`: Retry on `Done` job requires explicit follow-up prompt (HTTP 400).
- `retry_duplicate_attempt`: Concurrent retry attempted on same job (HTTP 400).
- `retry_no_conversation` / `retry_worktree_missing`: Missing state needed for resume (HTTP 400).
- `review_no_diff`: Target worktree diff is empty (HTTP 400).
- `selection_matched_nothing`: Diff or commit pathspec matched no modified files (HTTP 400).
- `too_many_streams`: SSE connection capacity exhausted (HTTP 503).
- `worktree_in_use`: Target worktree currently locked (HTTP 400).
- `worktree_not_found` / `worktree_removal_failed`: Worktree directory operations failed (HTTP 400).
- `internal`: Unhandled internal server error (HTTP 500).

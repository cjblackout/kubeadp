# Operations

This document covers operational health diagnostics, retention policies, state recovery, resource management, and deployment maintenance for agent-run.

## Health and Capacity Inspection

- `agent-run info`: Inspects server runtime status, active configuration values, disk space on `/workspace` and state roots, memory allocation, and toolchain versions.
- `agent-run projects`: Displays repository projects under `/workspace`, showing current tracking branch, HEAD commit hash, and active/total job counts.
- `agent-run quota`: Inspects remaining model provider quotas (Gemini / Claude+GPT) across 5-hour and weekly rate-limit windows. Pass `--fresh` to bypass the 60-second in-memory cache.
- `agent-run models`: Queries the underlying `agy` binary to verify model availability and system aliases.

## Logs and State Management

- **Logs**: Captured in `<state_root>/logs/<job_id>.log`. By default, logs for terminal non-`Done` jobs are preserved indefinitely for post-mortem auditing.
- **State database**: `<state_root>/state.sqlite` persists job metadata across daemon restarts. Backups can be taken live while in WAL mode (`sqlite3 state.sqlite ".backup state-backup.sqlite"`).

## Retention and Cleanup Policies

- **Explicit removal (`agent-run clean <id>`)**:
  - Deletes `<workspace_root>/.agent-jobs/<project>/<id>`, drops branch `agent-run/<id>`, and deletes the SQLite row.
  - Refusal: Refuses deletion if the job is `Running` or contains uncommitted modifications/untracked files, unless `--force` is supplied.
  - Kept-worktree rule: If `git worktree remove` fails and non-git files remain, the directory is preserved, a warning is emitted with manual `rm -rf` instructions, and the database record remains intact.
  - Log retention: Retains `<id>.log` unless `--purge-log` is specified.
- **Garbage collection (`agent-run gc`)**:
  - Scans terminal jobs older than `--older-than` (default 14 days).
  - Skips dirty worktrees without force.
  - Preserves logs of non-`Done` jobs unless `--purge-logs` is explicitly passed.
  - Supports `--dry-run` to preview deletions and `--max-jobs <N>` to cap total retained historical jobs (minimum retention floor of 24 hours).

## Recovery Procedures

- **Server restart during active jobs**:
  The server re-evaluates PIDs against `/proc/<pid>/stat` start times on startup. Jobs whose recorded process is gone transition to `SelfTerminated` or `Failed` upon the next status computation; worktrees remain intact for subsequent `retry`.
- **Orphaned worktrees and branches**:
  If a job row was removed without worktree cleanup, prune Git bookkeeping:
  ```bash
  git -C /workspace/<project> worktree prune
  rm -rf /workspace/.agent-jobs/<project>/<job_id>
  git -C /workspace/<project> branch -D agent-run/<job_id>
  ```
- **Hung or self-terminated jobs**:
  1. Terminate unresponsive execution: `agent-run kill <job_id>`. Sends SIGTERM, followed by SIGKILL if the process fails to exit within 5 seconds.
  2. Resume execution: `agent-run retry <job_id> --prompt "Continue execution"`. Relaunches `agy` in the existing worktree with incremented `retry_count`.

## Disk and Resource Considerations

- **Kata VM memory**: The Kata VM RAM backing `agent-run-api` resides in the host node's `/dev/shm`. The memory limit in `apps/kubeadp-agent-worker/manifest.yaml` is set to 4Gi to avoid filling `/dev/shm` on nodes hosting multiple sandboxes.
- **Disk growth**: Long-running test builds or uncleaned worktrees consume space on `/workspace` (50Gi PVC). Regularly run `agent-run gc --older-than 7d` to prune unneeded worktrees.

## Monitoring Signals

- Track active jobs against `AGENT_RUN_MAX_PARALLEL` capacity.
- Monitor log files for quota exhaustion warnings (`quota-exhausted` state).
- Running jobs idle longer than `AGENT_RUN_STALL_MINUTES` (default 20m) with zero active child processes are marked `possibly_stalled` in status output.

## Server Upgrade and Rollback

Deployments use `strategy: Recreate` due to ReadWriteOnce Longhorn volume attachments:
1. Update container image in `apps/kubeadp-agent-worker/manifest.yaml`.
2. Allow up to 60 seconds (`terminationGracePeriodSeconds`) for `AGENT_RUN_SHUTDOWN_GRACE` (default 25s) to signal active `agy` processes to persist conversation state before the pod terminates.
3. Verify new pod startup with `agent-run info`.

## Remote Control Operation

Remote control allows the `agent-run-api` host to appear in the Antigravity instance list:

- **Deployment configuration**: Enabled by setting `AGENT_RUN_REMOTE_CONTROL="true"` on the `agent-run-api` Deployment container in `apps/kubeadp-agent-worker/manifest.yaml`. The built-in code default remains `false`. Setting the environment variable permits remote control management.
- **Autostart and supervision**: When `AGENT_RUN_REMOTE_CONTROL_AUTOSTART="true"` is set alongside `AGENT_RUN_REMOTE_CONTROL`, `agent-run-api` launches an asynchronous background supervisor once the server listener is active. The supervisor executes the idempotent enable logic at boot and periodically polls daemon health every `AGENT_RUN_REMOTE_CONTROL_CHECK_SECS` (default 60s, minimum 10s). If the daemon crashes or terminates, the supervisor automatically restarts it. Command errors invoke exponential backoff capped at 5 minutes (300s). Log entries are emitted exclusively on state transitions (`started`, `found inactive and restarted`, `error`), suppressing per-tick output.
- **Suppression by disable**: A manual `agent-run remote-control disable` request sets an in-memory suppression flag on the API server. While suppressed, the supervisor leaves the daemon stopped. Calling `agent-run remote-control enable` clears the suppression flag and re-arms supervision. Container or server restarts clear the in-memory flag, allowing autostart to bring up the daemon upon reboot.
- **Cold-start latency**: Initial `agy remote-control start` and `status` invocations following an idle period incur cold-start latency between 30 and 60 seconds (subsequent invocations complete in 1–2 seconds). Internal execution timeouts are set to 180 seconds server-side, and CLI client read timeouts are enforced at a minimum of 200 seconds to prevent premature transport aborts.
- **Instance naming on rollout**: The instance name is derived from the `agent-run-api` pod hostname (e.g. `agent-run-api-7dd98d589f-9hbdd`) and changes whenever the pod is replaced on rollout. With autostart enabled, the supervisor automatically registers the new pod hostname upon startup.
- **Commands**:
  - `agent-run remote-control enable`: Starts and registers the background `agy remote-control` daemon under the current pod hostname, clearing supervisor suppression.
  - `agent-run remote-control disable`: Stops the background daemon and engages in-memory supervisor suppression.
  - `agent-run remote-control status`: Queries daemon status, reporting active state, PID, assigned instance name, and message.


## Task execution on the kubeadp-agent worker

An Agent's server Pod runs `kubeadp-agent worker` (supporting OpenCode, Antigravity (`agy`), and Claude Code engines). A Task that references the Agent runs `kubeopencode task-submit`, which submits a job to the worker's `/api/v1/jobs` API, streams the job log into the Pod log and exits with a code reflecting the final job status.

- **Where the work happens.** Every job runs in its own git worktree of a *project* under the Agent `workspaceDir`. The project is resolved from `spec.project`, Task annotations, or Agent git contexts. The patch is available via `/api/v1/jobs/{id}/diff` and mirrored in `Task.status.job.diff`.
- **Engine Selection.** Each Agent or AgentTemplate can specify `spec.engine` (`opencode`, `agy`, or `claude`).
- **Worker Configuration.** Agents and AgentTemplates define typed daemon configuration in `spec.worker` (e.g. `maxParallel`, `gcMaxAgeDays`, `stallMinutes`, `sandbox`, `hindsight`, `a2a`).
- **Interactive Actions.** Running tasks support actions (retry, kill, commit, discard, clean, review, message, interrupt, pause, compact, fork, revert) via operator REST endpoints authorized with Kubernetes RBAC.
- **Model and Effort.** Configured via `spec.model` and `spec.effort`, overriding worker defaults.
- **Timeout and stop.** `spec.timeout` is sent to the worker as `max_runtime`. Stopping a Task signals the worker to terminate the underlying job.
- **Live Status Mirroring.** `Task.status.job` reflects real-time job state, branch, commit, diff statistics, and todo items.
- **Web terminal.** The terminal opens a shell in the server container for direct workspace access.

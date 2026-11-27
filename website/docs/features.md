
## Task execution on the kubeadp-agent worker

An Agent's server Pod runs `kubeadp-agent worker` (the OpenCode binary stays available to it as an engine). A Task that references the Agent does not attach an OpenCode TUI to it: the Task Pod runs `kubeopencode task-submit`, which submits a job to the worker's `/api/v1/jobs` API, streams the job log into the Pod log and exits with a code that reflects the final job status (0 only for `done`).

- **Where the work happens.** Every job runs in its own git worktree of a *project*, a git repository directory directly under the Agent `workspaceDir`. The project is chosen in this order: the Task annotation `kubeopencode.io/project`; the first git context of the Agent (cloned into the server Pod at its mount path); the default project `workspace`, an empty repository the server Pod creates once on start. Edits therefore land in the job worktree, not in the project directory itself; the patch of a job is available from the worker (`/api/v1/jobs/{id}/diff`).
- **Model.** The model comes from the Agent's OpenCode configuration. The Task annotation `kubeopencode.io/model` (`provider/model`) overrides it for one Task.
- **Timeout and stop.** `spec.timeout` is also sent to the worker as the job's `max_runtime`. When a Task is stopped (annotation `kubeopencode.io/stop`) or times out, the Pod is deleted; `task-submit` receives SIGTERM and kills the job on the worker.
- **Session information.** `status.session.id` is the job ID. The Pod publishes it through its termination message; the controller reads it when the Pod ends. A Task that is stopped or times out has no published ID and keeps an empty `status.session.id`. When the worker reports usage in the job result, `status.session.summary` carries the token usage and cost; message counts and file change statistics are not reported for jobs.
- **Web terminal.** The terminal of an Agent opens a plain shell (`/bin/bash`) in the server container; there is no OpenCode TUI to attach to.

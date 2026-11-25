# Limitations

This document lists the architectural constraints, design limitations, and operational boundaries of the agent-run system.

1. **No HTTP authentication**
   - *Statement*: The HTTP API server implements no native authentication or authorization mechanisms.
   - *Impact*: Any process capable of routing traffic to port 8002 can submit commands and launch jobs.
   - *Workaround*: Enforce strict access control via Kubernetes `CiliumNetworkPolicy` and network segmentation.

2. **agy self-termination (`self-terminated` state)**
   - *Statement*: `agy` has been observed to exit cleanly after about 4.5-5 minutes while blocked on a long-running shell command, with no error and no signal received. A process that ends with exit status 137/143 and no structured error JSON is classified `self-terminated`, distinct from `failed`.
   - *Impact*: The job ends without a final answer; work already written to the worktree is retained.
   - *Workaround*: `agent-run retry <job_id> --prompt "<follow-up>"` resumes the same conversation in the same worktree. Jobs always run in the background for this reason.

3. **Single-server SQLite state repository**
   - *Statement*: Job metadata is stored in a local SQLite file (`state.sqlite`) using WAL mode.
   - *Impact*: The API daemon cannot be scaled horizontally across multiple active replicas; concurrent writes are serialized by file locks.
   - *Workaround*: Deploy `agent-run-api` as a single-replica Deployment with `strategy: Recreate`.

4. **Synchronous parallel capacity refusal**
   - *Statement*: The server does not maintain a submission queue for jobs exceeding `AGENT_RUN_MAX_PARALLEL`.
   - *Impact*: Submitting a job when active executions equal `max_parallel` fails immediately with HTTP 400 (`parallel_limit`).
   - *Workaround*: Block on running jobs using `agent-run wait` before submitting, or increase `AGENT_RUN_MAX_PARALLEL`.

5. **Read-only Git token constraints**
   - *Statement*: The server pod is provisioned with a read-only GitHub credential (`gh-kubeoc-token`).
   - *Impact*: Agents and scripts cannot push commits or create remote branches on GitHub from within the server worktrees.
   - *Workaround*: Export committed changes using `agent-run export <job_id>` and push from a host with write credentials.

6. **Quota exhaustion termination**
   - *Statement*: When upstream model rate limits or token quotas are depleted, `agy` fails and transitions jobs to `quota-exhausted`.
   - *Impact*: The job cannot progress until provider quota windows reset.
   - *Workaround*: Check quota windows via `agent-run quota --fresh` or launch jobs with an alternate model using `--model`.

7. **Hindsight 500-token prompt query limit**
   - *Statement*: The Hindsight API rejects recall queries longer than 500 tokens (roughly 2,000 characters of English prose; HTTP 400, `Query too long`). Automatic recall uses the complete job prompt as the query and does not truncate, summarise or split it.
   - *Impact*: For a longer prompt, recall is skipped for that job: no memory context is injected and no partial query is sent. Only a warning is written (to the `start` output and the job log). Auto-retain is unaffected.
   - *Workaround*: Pass `--hindsight-query "<query>"` with a short topic-focused query (trimmed, at most 2,000 characters) so recall targets the relevant context instead of the whole prompt, or pass `--hindsight off` to avoid the failed attempt.

8. **Linux host platform constraints**
   - *Statement*: The server process supervisor requires Linux procfs (`/proc/<pid>/stat`, `/proc/<pid>/cmdline`) and `prctl(PR_SET_CHILD_SUBREAPER)`.
   - *Impact*: The API daemon cannot execute on non-Linux operating systems.
   - *Workaround*: Run the API daemon inside Linux containers or virtual machines.

9. **Unrestricted `init` extraction parameters**
   - *Statement*: `POST /api/v1/init` (`agent-run init --from-env <VAR> --to <PATH>`) writes the value of any environment variable of the server process to any server-side path (parent directories are created, mode `0600`). Neither the variable name nor the destination is validated or restricted to an allowlist.
   - *Impact*: A caller with API access can read any environment variable of the server process, including credentials such as `GH_TOKEN`, into any file the server user can write.
   - *Workaround*: Restrict API access by network policy (see limitation 1).

10. **Unvalidated `project` path**
    - *Statement*: The `project` argument of `start` is joined to `workspace_root` without component validation; an absolute path or a `..` component resolves outside `workspace_root`. Only the existence of the directory and `git rev-parse --is-inside-work-tree` are checked.
    - *Impact*: A job can be created against any Git repository readable by the server user, and its worktree and branch are created relative to that repository.
    - *Workaround*: Restrict API access by network policy (see limitation 1).

11. **Untracked remote-control daemon sessions**
    - *Statement*: Sessions started through the `agy remote-control` daemon are not tracked by agent-run: they have no job record in SQLite, no dedicated worktree, and no captured log file. The daemon process does not automatically restart if it crashes or across container/pod restarts. Additionally, whether headless job conversations started via agent-run appear under the remote-control instance is unverified.
    - *Impact*: Remote-control interactions cannot be inspected, tailed, waited on, or cancelled via standard `agent-run` job commands (`status`, `log`, `tail`, `kill`, `result`).
    - *Workaround*: Manage the daemon lifecycle explicitly with `agent-run remote-control` commands and inspect the daemon logs on disk directly when needed.

12. **Raw NDJSON Log Output**
    - *Statement*: `agent-run log` and `agent-run tail` emit captured job logs as raw text combining `agent-run` prefixed lines and `agy` NDJSON event streams. Neither the CLI nor the server performs human-readable formatting or pretty-printing, and none is planned.
    - *Impact*: Reviewing execution progress directly from terminal output requires reading raw JSON records or piping the stream through external tools.
    - *Workaround*: Filter structured step updates using `jq`: `agent-run log <id> | grep '^{' | jq -c 'select(.event=="step_update") | .step_update | {step_index,state,step_type,tool_name}'`.

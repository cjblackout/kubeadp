# CLI Reference

This document specifies the command-line interface, global options, environment variables, exit codes, and subcommand syntax for agent-run.

## Contents
- [Global options](#global-options) · [Environment variables](#environment-variables) · [Exit status conventions](#exit-status-conventions) · [Output modes](#output-modes)
- Subcommands: [init](#init) · [start](#start) · [status](#status) · [log](#log) · [tail](#tail) · [diff](#diff) · [list](#list) · [projects](#projects) · [info](#info) · [models](#models) · [quota](#quota) · [clean](#clean) · [kill](#kill) · [retry](#retry) · [gc](#gc) · [wait](#wait) · [result](#result) · [review](#review) · [commit](#commit) · [files](#files) · [cat](#cat) · [discard](#discard) · [export](#export) · [auth](#auth) · [cancel](#cancel) · [watch](#watch) · [remote-control](#remote-control)

## Global Options

These options apply across all subcommands:

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--remote` | `<REMOTE>` | None | Base URL of the agent-run-api server (required, or `AGENT_RUN_REMOTE`). |
| `--json` | None | Disabled | Output JSON data payload on stdout; on failure, prints JSON error to stderr. |
| `-h, --help` | None | None | Print help information. |
| `-V, --version` | None | None | Print version string. |

## Environment Variables

- `AGENT_RUN_REMOTE`: Base URL of the target API server. Overridden by `--remote`.
- `AGENT_RUN_DEBUG`: When set and not `0`, prints HTTP status and machine error code to stderr on API error.
- `AGENT_RUN_CONNECT_TIMEOUT_SECS`: TCP connection timeout in seconds (default 10).
- `AGENT_RUN_HTTP_TIMEOUT_SECS`: Read/write timeout for standard API operations in seconds (default 60).

## Exit Status Conventions

- `0`: Success, or job terminated in `Done` state for `tail`, `watch`, and `wait`.
- `1`: General error, invalid argument, or communication failure.
- `2`: Job terminated in a state other than `Done` for `tail`, `watch`, and `wait`.
- `3`: The `--timeout` duration elapsed while the job remained running for `tail`, `watch`, and `wait`.

## Output Modes

By default, commands render formatted human-readable text. When `--json` is supplied, the command renders the server's `CommandOutput.data` JSON payload on stdout. Raw text commands (`log`, `cat`, raw `diff`, `tail`) ignore `--json` and emit bytes directly.

---

## `init`

```bash
agent-run init [OPTIONS]
```

Ensures server authentication and runtime state directories exist, optionally extracting secrets to files.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--from-env` | `<FROM_ENV>` | None | Read named environment variable from server environment (requires `--to`). |
| `--to` | `<TO>` | None | Destination path on server to write variable content with mode 0600. |
| `--hindsight-url` | `<HINDSIGHT_URL>` | None | Set Hindsight API base URL in server configuration. |
| `--hindsight-bank` | `<HINDSIGHT_BANK>` | None | Set default memory bank in server configuration. |
| `--hindsight-api-key` | `<HINDSIGHT_API_KEY>` | None | Set Hindsight API key in server configuration. |
| `--hindsight-profile` | `<HINDSIGHT_PROFILE>` | None | Set Hindsight profile name in server configuration. |

Exit status: 0 on success, 1 on error.
Example: `agent-run init --from-env SECRET_KEY --to /home/agent/.secrets/key`

## `start`

```bash
agent-run start [OPTIONS] <PROJECT> <PROMPT>
```

Creates an isolated Git worktree and launches an `agy` agent in the background.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--ref` | `<REF>` | `HEAD` | Git ref or branch to branch worktree from. |
| `--model` | `<MODEL>` | Server default | Model identifier passed to `agy`. |
| `--effort` | `<EFFORT>` | Server default | Reasoning effort (`low`, `medium`, `high`). |
| `--sandbox` | None | Disabled | Run `agy` in sandbox mode with restricted terminal access. |
| `--add-dir` | `<ADD_DIR>` | None | Additional directory to add to workspace (repeatable). |
| `--print-timeout` | `<PRINT_TIMEOUT>` | None | Optional time limit for print mode. |
| `--hindsight` | `<on\|off>` | Server default | Enable or disable Hindsight memory recall and retain for this job. |
| `--hindsight-bank` | `<HINDSIGHT_BANK>` | Project name | Explicit Hindsight bank override. |
| `--hindsight-query` | `<QUERY>` | Prompt text | Hindsight recall query text used instead of the full prompt; trimmed, max 2,000 characters, stored per job and reused by `retry`. Ignored when recall is disabled. |
| `--proxy` | `<PROXY>` | Server default | Proxy URL or `"none"` to disable proxy for child processes. |
| `--from-worktree` | `<FROM_WORKTREE>` | None | Use existing worktree path; skips worktree creation. |
| `--from-patch` | `<FROM_PATCH>` | None | Apply local Git patch file before starting agent. |
| `--plan` | None | Disabled | Read-only mode; agent is instructed not to modify files. |
| `--max-runtime` | `<MAX_RUNTIME>` | None (unlimited) unless `AGENT_RUN_MAX_RUNTIME` is set | Maximum wall-clock runtime (e.g. `30m`, `2h`) before SIGTERM/SIGKILL. |

Exit status: 0 on success, 1 on error.
Example: `agent-run start homelab "Fix tests" --model gemini-3.8-flash-medium --max-runtime 1h`
Example: `agent-run start homelab "Refactor the retry path to resume cleanly" --hindsight-query "retry resume worktree"`

## `status`

```bash
agent-run status [OPTIONS] <JOB_ID>
```

Prints the current execution status, elapsed time, worktree path, and metadata for a job.

Exit status: 0 on success, 1 on error.
Example: `agent-run status a1b2c3d4`

## `log`

```bash
agent-run log [OPTIONS] <JOB_ID>
```

Streams the entire captured stdout and stderr log of the job to stdout. Output is emitted raw without pretty rendering, mixing `agent-run` prefixed lines (`[<timestamp>] [<component>] ...`) with `agy` stream-json NDJSON event objects. The output updates live while the job is running as the agent executes steps. Structured step records can be extracted from the stream using `agent-run log <id> | grep '^{' | jq -c 'select(.event=="step_update") | .step_update | {step_index,state,step_type,tool_name}'`.

Exit status: 0 on success, 1 on error.
Example: `agent-run log a1b2c3d4`

## `tail`

```bash
agent-run tail [OPTIONS] <JOB_ID>
```

Polls and streams the job's log output live until execution reaches a terminal state. Output is emitted raw without pretty rendering, mixing `agent-run` prefixed lines with `agy` stream-json NDJSON events. Because `agy` emits step records incrementally as work occurs, the stream updates live during execution.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--timeout` | `<TIMEOUT>` | None | Maximum wait duration (e.g. `10m`, `1h`) before returning exit code 3. |

Exit status: 0 if job finished Done, 2 if terminated in another state, 3 on timeout, 1 on error.
Example: `agent-run tail a1b2c3d4 --timeout 30m`

## `diff`

```bash
agent-run diff [OPTIONS] <JOB_ID>
```

Outputs the unified diff of modifications made within the job's worktree.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--base` | None | Disabled | Diff against worktree base ref instead of worktree HEAD. |
| `--only` | `<ONLY>` | None | Restrict diff to pathspecs (repeatable). |
| `--exclude` | `<EXCLUDE>` | None | Exclude pathspecs from diff (repeatable). |
| `--stat` | None | Disabled | Print diffstat summary instead of full patch text. |

Exit status: 0 on success, 1 on error.
Example: `agent-run diff a1b2c3d4 --stat`

## `list`

```bash
agent-run list [OPTIONS]
```

Lists known jobs with project name, creation age, status, and prompt preview.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--project` | `<PROJECT>` | None | Filter displayed jobs by project name. |
| `--status` | `<STATUS>` | None | Filter by job status (repeatable, comma-separated). |

Exit status: 0 on success, 1 on error.
Example: `agent-run list --project homelab --status running,failed`

## `projects`

```bash
agent-run projects [OPTIONS]
```

Displays available repository projects under the server workspace root with sync state and job counts.

Exit status: 0 on success, 1 on error.
Example: `agent-run projects`

## `info`

```bash
agent-run info [OPTIONS]
```

Outputs server health, configuration parameters, disk capacity, auth state, and available toolchains.

Exit status: 0 on success, 1 on error.
Example: `agent-run info`

## `models`

```bash
agent-run models [OPTIONS]
```

Queries the server's `agy` binary and outputs supported model aliases and labels.

Exit status: 0 on success, 1 on error.
Example: `agent-run models`

## `quota`

```bash
agent-run quota [OPTIONS]
```

Displays remaining model provider request quotas and reset windows.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--fresh` | None | Disabled | Bypass server 60s quota cache and query provider live. |

### Daily Pacing

For each bucket whose limit name contains `weekly`, `quota` computes the daily pace after the table rows: one text line `pacing: <group> weekly: used <U>%, day <N> of 7, allowance <A>% -> OVER PACE` (or `-> within pace`, `A` with one decimal). The JSON payload gains a `pacing` array of `{group, used_percent, window_day, allowance_percent (one decimal), over_pace}` objects, one per weekly bucket. Each over-pace entry also emits the stderr warning `warning: <group> weekly quota is over its daily pace (used <U>%, allowance <A>% on day <N> of 7): delegate to a Qwen task instead`; the low-remaining (<=10%) warnings are unchanged.

The allowance is the day-of-window times 100/7 percent, where the day-of-window is `7 -` whole days until the reset time, clamped to 1..=7 (a missing or unparseable reset time yields no pacing entry; `used_percent` is `100 - remaining_percent`). A bucket is over pace when its used percent is at or above the allowance.

Exit status: 0 on success, 1 on error.
Example: `agent-run quota --fresh`

## `clean`

```bash
agent-run clean [OPTIONS] <JOB_ID>
```

Removes a job's Git worktree, deletes its branch, and purges the metadata row from SQLite.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--force` | None | Disabled | Force deletion if worktree is dirty or process is active. |
| `--purge-log` | None | Disabled | Delete `<job_id>.log` along with worktree resources. |

Exit status: 0 on success, 1 on error.
Example: `agent-run clean a1b2c3d4 --purge-log`

## `kill`

```bash
agent-run kill [OPTIONS] <JOB_ID>
```

Terminates an active job by sending SIGTERM, followed by SIGKILL if still running after 5 seconds.

Exit status: 0 on success, 1 on error.
Example: `agent-run kill a1b2c3d4`

## `retry`

```bash
agent-run retry [OPTIONS] <JOB_ID>
```

Relaunches `agy` in the existing worktree, resuming the conversation session.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--hindsight` | `<on\|off>` | Recorded/Default | Override Hindsight memory integration for this attempt. |
| `--prompt` | `<PROMPT>` | Recorded prompt | Follow-up instruction continuing the existing conversation. |
| `--hindsight-query` | `<QUERY>` | Stored query, else the follow-up text | Hindsight recall query for this attempt; replaces and updates the query stored with the job (trimmed, max 2,000 characters). Rejected with `--hindsight off`. |

Exit status: 0 on success, 1 on error.
Example: `agent-run retry a1b2c3d4 --prompt "Fix compilation error in main.rs"`

## `gc`

```bash
agent-run gc [OPTIONS]
```

Garbage collects terminal jobs older than the retention threshold.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--older-than` | `<OLDER_THAN>` | `14d` | Age cutoff duration (e.g. `14d`, `48h`). |
| `--project` | `<PROJECT>` | None | Restrict garbage collection to a specific project. |
| `--max-jobs` | `<MAX_JOBS>` | None | Retain at most N terminal jobs; prunes oldest beyond limit. |
| `--dry-run` | None | Disabled | Print candidate jobs without executing deletions. |
| `--purge-logs` | None | Disabled | Delete log files of terminal non-Done jobs. |

Exit status: 0 on success, 1 on error.
Example: `agent-run gc --older-than 7d --dry-run`

## `wait`

```bash
agent-run wait [OPTIONS] <JOB_IDS>...
```

Blocks synchronously until the specified jobs reach a terminal state.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--any` | None | Default (single) | Return when ANY specified job reaches terminal state. |
| `--all` | None | Default (multiple)| Return only when ALL specified jobs reach terminal state. |
| `--timeout` | `<TIMEOUT>` | Indefinite | Maximum wait duration (e.g. `30m`, `2h`). |

Exit status: 0 if all relevant jobs are Done, 2 if terminal but not Done, 3 on timeout, 1 on error.
Example: `agent-run wait a1b2c3d4 e5f6a7b8 --all --timeout 1h`

## `result`

```bash
agent-run result [OPTIONS] <JOB_ID>
```

Prints the final structured response text produced by a completed agent job.

Exit status: 0 on success, 1 on error.
Example: `agent-run result a1b2c3d4`

## `review`

```bash
agent-run review [OPTIONS] <JOB_ID>
```

Spawns a new plan-mode agent in the target job's worktree to review its diff.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--reviewer-model` | `<REVIEWER_MODEL>` | Server default | Model override for the reviewer agent. |

Exit status: 0 on success, 1 on error.
Example: `agent-run review a1b2c3d4 --reviewer-model claude-3-5-sonnet`

## `commit`

```bash
agent-run commit [OPTIONS] <JOB_ID>
```

Commits uncommitted changes inside the specified job worktree.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `-m, --message` | `<MESSAGE>` | Prompt summary | Commit message text. |
| `--only` | `<ONLY>` | All | Restrict commit to specific pathspecs (repeatable). |
| `--exclude` | `<EXCLUDE>` | None | Exclude pathspecs from commit (repeatable). |

Exit status: 0 on success, 1 on error.
Example: `agent-run commit a1b2c3d4 -m "feat: implement feature"`

## `files`

```bash
agent-run files [OPTIONS] <JOB_ID> [PATH]
```

Lists directories and files inside the job's worktree path.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--depth` | `<DEPTH>` | None | Maximum traversal depth clamped to 1..=4. |

Exit status: 0 on success, 1 on error.
Example: `agent-run files a1b2c3d4 src/ --depth 2`

## `cat`

```bash
agent-run cat [OPTIONS] <JOB_ID> <PATH>
```

Outputs the contents of a single text file from the job worktree.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--lines` | `<A:B>` | Whole file | 1-based inclusive line range (e.g. `10:50`, `20:`, `:30`). |
| `--max-bytes` | `<MAX_BYTES>` | 1048576 | Maximum bytes to output (hard limit 8388608). |

Exit status: 0 on success, 1 on error.
Example: `agent-run cat a1b2c3d4 src/main.rs --lines 1:100`

## `discard`

```bash
agent-run discard [OPTIONS] <JOB_ID> <PATH>...
```

Deletes named untracked files from the job's worktree.

Exit status: 0 on success, 1 on error.
Example: `agent-run discard a1b2c3d4 temp.txt debug.log`

## `export`

```bash
agent-run export [OPTIONS] <JOB_ID>
```

Downloads committed worktree changes to the client machine as a patch or tarball.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `-m, --message` | `<MESSAGE>` | None | Commit uncommitted changes prior to export. |
| `--only` | `<ONLY>` | All | Export only changes matching pathspecs (repeatable). |
| `--exclude` | `<EXCLUDE>` | None | Exclude changes matching pathspecs (repeatable). |
| `--out` | `<OUT>` | `./<id>.patch` | Output file path on client. |
| `--tar` | None | Disabled | Export tar archive of changed files instead of Git patch. |
| `--allow-large` | None | Disabled | Allow exports exceeding the 50 MiB threshold. |
| `--force` | None | Disabled | Overwrite destination file if already present. |

Exit status: 0 on success, 1 on error.
Example: `agent-run export a1b2c3d4 --out ./changes.patch`

## `auth`

```bash
agent-run auth [OPTIONS]
```

Initiates or completes an interactive OAuth flow for the server's `agy` binary.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--code` | `<CODE>` | Stdin | Verification code obtained from OAuth success page. |

Exit status: 0 on success, 1 on error.
Example: `agent-run auth --code 4/0AX4XfWh...`

## `cancel`

```bash
agent-run cancel [OPTIONS]
```

Terminates an in-progress server OAuth child process and clears the authentication slot.

Exit status: 0 on success, 1 on error.
Example: `agent-run cancel`

## `watch`

```bash
agent-run watch [OPTIONS] <JOB_ID>
```

Streams real-time Server-Sent Events (SSE) log output and status transitions until termination.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `--timeout` | `<TIMEOUT>` | None | Maximum wait duration (e.g. `30m`, `2h`) before exiting code 3. |

Exit status: 0 if job ended Done, 2 if terminated in another state, 3 on timeout, 1 on error.
Example: `agent-run watch a1b2c3d4 --timeout 45m`

## `remote-control`

```bash
agent-run remote-control <enable|disable|status>
```

Manages the lifecycle of the `agy` remote-control daemon on the API server.

The instance name is picked up automatically from the hostname of the `agent-run-api` pod (for example `agent-run-api-7dd98d589f-9hbdd`); it cannot be set per call. It changes whenever the pod is replaced, so run `enable` again after a rollout. The commands require `AGENT_RUN_REMOTE_CONTROL` to be enabled on the server (see [Configuration](configuration.md)); otherwise the server answers 403 `remote_control_disabled`.

| Option | Argument | Default | Meaning |
| :--- | :--- | :--- | :--- |
| *(none)* | — | — | None besides global options. |

Exit status: 0 on success, 1 on error.
Example: `agent-run remote-control status`


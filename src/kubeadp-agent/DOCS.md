# agent-run-cli

The `agent-run-cli` crate provides the `agent-run` command-line HTTP client shim for the `agent-run-api` daemon. It parses user arguments, dispatches requests over HTTP, and formats execution outputs. Build the binary with `cargo build --release --manifest-path src/agent-run-cli/Cargo.toml` and run tests with `cargo test --manifest-path src/agent-run-cli/Cargo.toml`. Complete command documentation, syntax, and options are specified in [docs/agent-run/cli-reference.md](../../docs/agent-run/cli-reference.md).

## `repair`

Job worktrees are checkouts of a per-project **private** bare repository kept under the agent-run repos root (`<workspace_root>/.agent-repos/<project>.git` by default). When git-sync re-initialises the project checkout it can drop the `worktrees/<job_id>` **admin** directory of that private repo, leaving the worktree's `.git` file dangling: `git` commands in the worktree break until it is re-attached — the worktree is "orphaned". `repair` re-attaches it (recreating the admin directory at the job's recorded base ref); it is a no-op that reports `healthy` when the worktree is already attached.

Orphaned worktrees are also re-attached automatically, silently, at the start of the server-side `status`, `diff`, `commit`, `retry`, `files`, `cat`, `review`, and `gc` commands, so a one-off `repair` is only needed to verify or force the fix.

```bash
agent-run repair <JOB_ID>     # re-attach one job's worktree
agent-run repair --all         # sweep every job, repairing only the dangling ones
```

Output is one line per job: `repaired <id>`, `healthy <id>`, or `failed <id>: <reason>`. Exit status: `0` normally (and in `--all` when nothing failed), `1` when the single target is missing/unrepairable or when `--all` had at least one failure, `1` for API errors (e.g. `404 worktree_not_found` on a deleted worktree, `400 repair_external_worktree` for a `--from-worktree` job).

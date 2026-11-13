//! Command-line surface of the `agent-run` shim.
//!
//! The shim is a pure remote client: it parses the command line, performs the
//! command against the agent-run API server over HTTP, and renders the
//! server's `CommandOutput` with the engine's own renderer (byte-identical to
//! the old local CLI). It has no local execution path of its own.
//!
//! The per-command argument structs live in `crate::core::args` -- shared
//! with the API server, where they are also the JSON wire format.

use clap::{Args, Parser, Subcommand};

pub use crate::core::args::{
    CatArgs, CleanArgs, CommitArgs, DiffArgs, DiscardArgs, ExportArgs, FilesArgs, GcArgs, InfoArgs,
    InitArgs, JobIdArg, JobsetEndArgs, ListArgs, MessageArgs, ModelsArgs, PauseArgs, ProjectsArgs,
    QuotaArgs, RemoteControlAction, RemoteControlArgs, RepairArgs, ResultArgs, RetryArgs,
    RevertArgs, ReviewArgs, StartArgs, WaitArgs,
};

/// agent-run: remote CLI client for the agent-run API server
#[derive(Debug, Parser)]
#[command(name = "agent-run", author, version, about = "Remote CLI client for the agent-run API server (agy AI coding agents)", long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub global_opts: GlobalOpts,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Args, Clone)]
pub struct GlobalOpts {
    /// Base URL of the agent-run API server (required). All execution happens
    /// server-side; this client renders the server's result. In-cluster:
    /// http://agent-run-api.opencode.svc:8002
    #[arg(global = true, long, env = "AGENT_RUN_REMOTE")]
    pub remote: Option<String>,

    /// Print the command's JSON data payload instead of human-readable
    /// text. Raw-text commands (log, tail, cat, and diff without --stat)
    /// ignore this flag and keep emitting text; on error, one JSON line
    /// with the "error" and "code" fields is also written to stderr.
    #[arg(global = true, long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)] // one parsed value per process; boxing would only churn the arg structs
pub enum Commands {
    /// Runtime setup: ensures the server is authenticated (interactive OAuth when its credentials are missing), then ensures job state directories exist and optionally extracts a secret env var to a 0600 file
    Init(InitArgs),

    /// Launch a new agy agent job in an isolated git worktree
    Start(StartArgs),

    /// Check status of a job (running, done, failed, self-terminated, killed, incomplete, quota-exhausted)
    Status(JobIdArg),

    /// Print captured log for a job
    Log(JobIdArg),

    /// Follow a job's log file live; exit codes: 0 = job ended Done, 2 = job
    /// ended in another terminal state, 3 = --timeout elapsed while still running
    Tail(TailOpts),

    /// Show git diff inside the job's worktree
    Diff(DiffArgs),

    /// List all known jobs
    List(ListArgs),

    /// List the projects `start <PROJECT>` accepts, with per-repo freshness and job counters
    Projects(ProjectsArgs),

    /// Show server health and job capability snapshot (version, config, capacity, disk, auth, toolchains)
    Info(InfoArgs),

    /// List the models agy can use (from the server's agy)
    Models(ModelsArgs),

    /// Show the agy quota (Gemini / Claude+GPT, weekly and five-hour limits)
    Quota(QuotaArgs),

    /// Remove a job's worktree and metadata
    Clean(CleanArgs),

    /// Terminate a running job process (SIGTERM then SIGKILL)
    Kill(JobIdArg),

    /// Re-launch agy in the same worktree continuing previous conversation
    Retry(RetryArgs),

    /// Repair a job's orphaned git worktree (re-attach it to agent-run's private repository), or repair every dangling worktree with --all
    Repair(RepairArgs),

    /// Garbage collect terminal jobs older than configured age
    Gc(GcArgs),

    /// Wait for job(s) to reach a terminal state (exit 0: done, 2: terminal but not done, 3: timeout)
    Wait(WaitArgs),

    /// Print the final structured result (response text) of a finished job
    Result(ResultArgs),

    /// Spawn a plan-mode review agent over another job's worktree diff
    Review(ReviewArgs),

    /// Commit uncommitted changes in a job's worktree
    Commit(CommitArgs),

    /// List files in a job's worktree
    Files(FilesArgs),

    /// Print a file from a job's worktree (text only, size-capped)
    Cat(CatArgs),

    /// Delete named untracked files from a job's worktree
    Discard(DiscardArgs),

    /// Export a job's committed result to the caller's machine (patch/tar)
    Export(ExportArgs),

    /// Authenticate the agy CLI (interactive OAuth via the server)
    Auth(AuthArgs),

    /// Cancel the in-progress auth flow (kill the server's parked agy
    /// child and free the slot for a fresh flow)
    Cancel,

    /// Stream a job's log and status changes live (SSE), until it ends
    Watch(WatchOpts),

    /// Manage the agy remote-control daemon on the API server
    RemoteControl(RemoteControlArgs),

    /// End a jobset: bump its epoch and mark it ended (old credentials stop
    /// verifying). Operator-only.
    Jobset(JobsetEndArgs),

    /// Interrupt a running job (emulated: process-group SIGTERM then SIGKILL,
    /// no kill bookkeeping; the job can be resumed with `retry`).
    Interrupt(JobIdArg),

    /// Pause or resume a job's E4 emulation flag (messages queue while paused).
    Pause(PauseArgs),

    /// Deliver a message to a job; queued in `.engine-message-queue` for the
    /// next turn when the engine has no live mid-run channel.
    Message(MessageArgs),

    /// Fork a job's worktree into a new run (emulated worktree copy).
    Fork(JobIdArg),

    /// Compact a job's context (emulated summarise-then-restart).
    Compact(JobIdArg),

    /// Revert a job's worktree to its latest engine checkpoint; an empty
    /// message unreverts.
    Revert(RevertArgs),

    /// Show a job's structured todo list (collected from its event stream).
    Todo(JobIdArg),
}

/// Arguments for `agent-run auth`.
///
/// Shim-only (no counterpart in `crate::core::args`): the server's auth
/// route takes the raw code string, and the code is either a flag or one
/// line of interactive stdin.
#[derive(Debug, Args, Clone)]
pub struct AuthArgs {
    /// The code shown on the auth-success page (otherwise read from stdin).
    #[arg(long)]
    pub code: Option<String>,
}

/// Arguments for `agent-run tail`.
///
/// Shim-only (no counterpart in `crate::core::args`): tail is a pure
/// client-side poll and the timeout is enforced locally.
#[derive(Debug, Args, Clone)]
pub struct TailOpts {
    /// The job to follow (full id or a unique prefix).
    pub job_id: String,

    /// Stop after this duration if the job is still running (exit code 3),
    /// e.g. 60s, 30m, 2h. Same duration syntax as `wait --timeout`.
    #[arg(long)]
    pub timeout: Option<String>,
}

/// Arguments for `agent-run watch`.
///
/// Shim-only (no counterpart in `crate::core::args`): watch is a pure
/// client-side SSE consumer and the timeout is enforced locally.
#[derive(Debug, Args, Clone)]
pub struct WatchOpts {
    /// The job to follow (full id or a unique prefix).
    pub job_id: String,

    /// Stop after this duration if the job is still running (exit code 3),
    /// e.g. 60s, 30m, 2h. Same duration syntax as `wait --timeout`.
    #[arg(long)]
    pub timeout: Option<String>,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Commands, StartArgs};

    fn start_argv(extra: &[&str]) -> Vec<String> {
        let mut argv = vec![
            "agent-run".to_string(),
            "start".to_string(),
            "proj".to_string(),
            "prompt".to_string(),
        ];
        argv.extend(extra.iter().map(|s| s.to_string()));
        argv
    }

    fn parsed_start(extra: &[&str]) -> StartArgs {
        let cli = Cli::try_parse_from(start_argv(extra)).expect("parse should succeed");
        match cli.command {
            Commands::Start(args) => args,
            other => panic!("expected the start subcommand, got: {other:?}"),
        }
    }

    fn rejected_start(extra: &[&str]) -> String {
        let err = Cli::try_parse_from(start_argv(extra)).expect_err("parse should fail");
        err.to_string()
    }

    fn parsed_retry(extra: &[&str]) -> super::RetryArgs {
        let mut argv = vec![
            "agent-run".to_string(),
            "retry".to_string(),
            "abc123".to_string(),
        ];
        argv.extend(extra.iter().map(|s| s.to_string()));
        let cli = Cli::try_parse_from(&argv).expect("parse should succeed");
        match cli.command {
            Commands::Retry(args) => args,
            other => panic!("expected the retry subcommand, got: {other:?}"),
        }
    }

    #[test]
    fn test_start_plan_and_effort_accept_documented_values() {
        let args = parsed_start(&["--plan", "--effort", "high"]);
        assert!(args.plan);
        assert_eq!(args.effort.as_deref(), Some("high"));

        // Without --plan the job runs in the normal (editing) mode.
        let args = parsed_start(&[]);
        assert!(!args.plan);
        assert_eq!(args.effort, None);
    }

    #[test]
    fn test_start_effort_rejects_undocumented_values() {
        for extra in [&["--effort", "ultra"], &["--effort", "Low"]] {
            // value sets are case-sensitive
            let msg = rejected_start(extra);
            assert!(
                msg.contains("invalid value"),
                "expected an invalid-value error for {extra:?}, got: {msg}"
            );
        }
    }

    #[test]
    fn test_retry_prompt_flag_parses_and_defaults_to_none() {
        use crate::core::args::HindsightFlag;
        let args = parsed_retry(&["--prompt", "now fix the flake"]);
        assert_eq!(args.job_id, "abc123");
        assert_eq!(args.prompt.as_deref(), Some("now fix the flake"));
        assert_eq!(args.hindsight, None);

        // Without --prompt the follow-up stays unset (a stored-prompt retry).
        let args = parsed_retry(&[]);
        assert_eq!(args.job_id, "abc123");
        assert_eq!(args.prompt, None);

        // --prompt combines with the hindsight flag, which parses its on/off
        // value.
        let args = parsed_retry(&["--prompt", "@prompt.txt", "--hindsight", "off"]);
        assert_eq!(args.prompt.as_deref(), Some("@prompt.txt"));
        assert_eq!(args.hindsight, Some(HindsightFlag::Off));
    }

    #[test]
    fn test_hindsight_query_flag_parses_for_start_and_retry() {
        let args = parsed_start(&["--hindsight-query", "short query"]);
        assert_eq!(args.hindsight_query.as_deref(), Some("short query"));
        assert_eq!(args.hindsight, None);
        assert_eq!(args.hindsight_bank, None);

        // Without the flag it stays unset (prompt-as-query behaviour).
        let args = parsed_start(&[]);
        assert_eq!(args.hindsight_query, None);

        let args = parsed_retry(&["--hindsight-query", "short query"]);
        assert_eq!(args.hindsight_query.as_deref(), Some("short query"));

        let args = parsed_retry(&[]);
        assert_eq!(args.hindsight_query, None);
    }

    #[test]
    fn test_files_and_cat_parse() {
        let cli = Cli::try_parse_from(["agent-run", "files", "abc123", "src", "--depth", "2"])
            .expect("parse should succeed");
        match cli.command {
            Commands::Files(args) => {
                assert_eq!(args.job_id, "abc123");
                assert_eq!(args.path.as_deref(), Some("src"));
                assert_eq!(args.depth, Some(2));
            }
            other => panic!("expected the files subcommand, got: {other:?}"),
        }

        let cli = Cli::try_parse_from([
            "agent-run",
            "cat",
            "abc123",
            "README.md",
            "--lines",
            "1:20",
            "--max-bytes",
            "1000",
        ])
        .expect("parse should succeed");
        match cli.command {
            Commands::Cat(args) => {
                assert_eq!(args.job_id, "abc123");
                assert_eq!(args.path, "README.md");
                assert_eq!(args.lines.as_deref(), Some("1:20"));
                assert_eq!(args.max_bytes, Some(1000));
            }
            other => panic!("expected the cat subcommand, got: {other:?}"),
        }
    }

    #[test]
    fn test_discard_parse() {
        let cli = Cli::try_parse_from(["agent-run", "discard", "abc123", "a.txt", "b/c.txt"])
            .expect("parse should succeed");
        match cli.command {
            Commands::Discard(args) => {
                assert_eq!(args.job_id, "abc123");
                assert_eq!(args.paths, vec!["a.txt", "b/c.txt"]);
            }
            other => panic!("expected the discard subcommand, got: {other:?}"),
        }

        // At least one PATH is required.
        let err =
            Cli::try_parse_from(["agent-run", "discard", "abc123"]).expect_err("parse should fail");
        assert!(err.to_string().contains("PATH"));
    }

    #[test]
    fn test_global_json_flag_parses_anywhere() {
        let cli = Cli::try_parse_from(["agent-run", "--json", "status", "abc123"])
            .expect("parse should succeed");
        assert!(cli.global_opts.json);

        // ... and after the subcommand, thanks to `global = true`.
        let cli =
            Cli::try_parse_from(["agent-run", "list", "--json"]).expect("parse should succeed");
        assert!(cli.global_opts.json);

        let cli =
            Cli::try_parse_from(["agent-run", "status", "abc123"]).expect("parse should succeed");
        assert!(!cli.global_opts.json);
    }

    #[test]
    fn test_tail_opts_parse() {
        let cli =
            Cli::try_parse_from(["agent-run", "tail", "abc123"]).expect("parse should succeed");
        match cli.command {
            Commands::Tail(args) => {
                assert_eq!(args.job_id, "abc123");
                assert_eq!(args.timeout, None);
            }
            other => panic!("expected the tail subcommand, got: {other:?}"),
        }

        let cli = Cli::try_parse_from(["agent-run", "tail", "abc123", "--timeout", "30m"])
            .expect("parse should succeed");
        match cli.command {
            Commands::Tail(args) => {
                assert_eq!(args.job_id, "abc123");
                assert_eq!(args.timeout.as_deref(), Some("30m"));
            }
            other => panic!("expected the tail subcommand, got: {other:?}"),
        }

        // --timeout still accepts the global flag on the same command.
        let cli = Cli::try_parse_from(["agent-run", "tail", "abc123", "--timeout", "2h", "--json"])
            .expect("parse should succeed");
        assert!(cli.global_opts.json);
    }

    #[test]
    fn test_start_max_runtime_flag_parses_and_defaults_to_none() {
        // Unset by default: no --max-runtime means no limit.
        let args = parsed_start(&[]);
        assert_eq!(args.max_runtime, None);

        // The value passes through verbatim (server validates the duration).
        let args = parsed_start(&["--max-runtime", "30m"]);
        assert_eq!(args.max_runtime.as_deref(), Some("30m"));
    }

    #[test]
    fn test_start_jobset_flag_parses_and_defaults_to_none() {
        // Unset by default: the job gets a fresh jobset (or the host's).
        let args = parsed_start(&[]);
        assert_eq!(args.jobset_id, None);

        // Explicit `--jobset <id>` joins the caller's jobset.
        let args = parsed_start(&["--jobset", "js-caller"]);
        assert_eq!(args.jobset_id.as_deref(), Some("js-caller"));
    }

    #[test]
    fn test_start_engine_flag_parses_and_defaults_to_none() {
        // Default: no --engine means agy (the server default).
        let args = parsed_start(&[]);
        assert_eq!(args.engine, None);

        // The value passes through verbatim (the server maps it to the
        // engine selection contract of HL-91 E3).
        let args = parsed_start(&["--engine", "agy"]);
        assert_eq!(args.engine.as_deref(), Some("agy"));
    }
}

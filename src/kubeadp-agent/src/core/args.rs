//! Per-command argument structs.
//!
//! These are the single source of truth for the CLI surface (the `agent-run`
//! shim parses them with clap) AND the API wire format (the `agent-run-api`
//! server deserializes them from JSON request bodies). Field names are the
//! wire contract -- do not rename.

use std::path::PathBuf;

use clap::{builder::PossibleValuesParser, Args, Subcommand};

/// Per-job Hindsight memory switch (`--hindsight on|off`).
///
/// `on` enables Hindsight for this job using the server's configured
/// recall/retain defaults; `off` disables ALL Hindsight activity for the job
/// (no recall, no retain) even when the server's env/config would enable it
/// (the per-job choice always wins); an absent flag defers to the server.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum HindsightFlag {
    On,
    Off,
}

/// Runtime setup: ensure job state directories exist and optionally extract a
/// secret env var to a 0600 file. Runs on the API server (server-side env).
#[derive(Debug, Args, Clone, Default, serde::Deserialize)]
pub struct InitArgs {
    /// Read named environment variable from the server's own environment
    #[arg(long, requires = "to")]
    #[serde(default)]
    pub from_env: Option<String>,

    /// Destination path to write environment variable value with mode 0600
    #[arg(long, requires = "from_env")]
    #[serde(default)]
    pub to: Option<PathBuf>,

    /// Configure Hindsight API URL in config file
    #[arg(long)]
    #[serde(default)]
    pub hindsight_url: Option<String>,

    /// Configure Hindsight memory bank in config file
    #[arg(long)]
    #[serde(default)]
    pub hindsight_bank: Option<String>,

    /// Configure Hindsight API key in config file
    #[arg(long)]
    #[serde(default)]
    pub hindsight_api_key: Option<String>,

    /// Configure Hindsight profile name in config file
    #[arg(long)]
    #[serde(default)]
    pub hindsight_profile: Option<String>,
}

/// Launch a new agy agent job in an isolated git worktree.
#[derive(Debug, Args, Clone, Default)]
pub struct StartArgs {
    /// Project directory name under workspace_root
    pub project: String,

    /// Prompt text (the CLI shim resolves `@<file>` prompts locally and sends
    /// the file contents as literal text).
    pub prompt: String,

    /// Branch or ref to create worktree from (default: HEAD)
    #[arg(long = "ref")]
    pub git_ref: Option<String>,

    /// Override model for agy session
    #[arg(long)]
    pub model: Option<String>,

    /// Reasoning effort (low|medium|high)
    #[arg(
        long,
        value_parser = PossibleValuesParser::new(["low", "medium", "high"])
    )]
    pub effort: Option<String>,

    /// Run agy in sandbox mode with terminal restrictions
    #[arg(long)]
    pub sandbox: bool,

    /// Additional directories to add to the workspace (repeatable; absolute
    /// paths on the server).
    #[arg(long = "add-dir")]
    pub add_dirs: Vec<PathBuf>,

    /// Optional time limit for print mode
    #[arg(long)]
    pub print_timeout: Option<String>,

    /// Hindsight long-term memory for this job: `on` enables it using the
    /// server's configured recall/retain defaults, `off` disables it entirely
    /// (no recall, no retain) even when the server config would enable it.
    /// Absent: the server's configured defaults apply.
    #[arg(long, value_name = "<on|off>")]
    pub hindsight: Option<HindsightFlag>,

    /// Explicit Hindsight memory bank override for this run (defaults to project name)
    #[arg(long)]
    pub hindsight_bank: Option<String>,

    /// Hindsight recall query for this job (replaces the full prompt as the query; at most 2000 characters)
    #[arg(long)]
    pub hindsight_query: Option<String>,

    /// Override proxy settings for this run (<url> or "none" to disable proxy)
    #[arg(long)]
    pub proxy: Option<String>,

    /// Launch the agent in an existing git worktree of the same repository
    /// (no new worktree is created; absolute path on the server).
    #[arg(long, conflicts_with = "from_patch", conflicts_with = "git_ref")]
    pub from_worktree: Option<PathBuf>,

    /// Create the worktree, then apply this git patch file to it before
    /// launching (absolute path on the server; the API server materializes
    /// `from_patch_content` from the request into a temp file).
    #[arg(long, conflicts_with = "from_worktree")]
    pub from_patch: Option<PathBuf>,

    /// Run a read-only investigation: the agent must not modify files (plan
    /// mode). The job and its log are kept after `agent-run result`; remove
    /// it explicitly with `agent-run clean <id>` or let `agent-run gc`
    /// reclaim it later.
    #[arg(long)]
    pub plan: bool,

    /// Maximum runtime for this job (same duration syntax as
    /// `wait --timeout`, e.g. `30m` or `2h`): when the job's wall time since
    /// start exceeds it, the server terminates it (job ends `killed`).
    /// Unset by default (no limit); wins over the server-wide
    /// `AGENT_RUN_MAX_RUNTIME` default.
    #[arg(long)]
    pub max_runtime: Option<String>,

    /// Join the job to an existing jobset instead of creating a fresh one:
    /// the job presents the jobset credential derived from the shared root
    /// key (`jobset_root_key_file` / `KUBEADP_JOBSET_ROOT_KEY_FILE`) for this
    /// jobset id, so the caller (e.g. an orchestrator session) and the job
    /// become members of the same A2A jobset. Requires a configured root key;
    /// a retried job keeps its original jobset.
    #[arg(long = "jobset")]
    pub jobset_id: Option<String>,
    /// Engine to run this job with (HL-91 E3, selection precedence level 1).
    /// `agy` (the default) uses the agy job driver; `claude` is a claude
    /// engine this pod does not host yet (the request answers 501); any other
    /// value is an unregistered engine the server refuses with 400.
    #[arg(long, value_name = "<engine>")]
    pub engine: Option<String>,
}

#[derive(serde::Deserialize)]
struct StartArgsDeserialize {
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub git_ref: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub sandbox: bool,
    #[serde(default)]
    pub add_dirs: Vec<PathBuf>,
    #[serde(default)]
    pub print_timeout: Option<String>,
    #[serde(default)]
    pub hindsight: Option<HindsightFlag>,
    #[serde(default)]
    pub hindsight_bank: Option<String>,
    #[serde(default)]
    pub hindsight_query: Option<String>,
    #[serde(default)]
    pub proxy: Option<String>,
    #[serde(default)]
    pub from_worktree: Option<PathBuf>,
    #[serde(default)]
    pub from_patch: Option<PathBuf>,
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    pub max_runtime: Option<String>,
    #[serde(default)]
    pub jobset_id: Option<String>,
    pub engine: Option<String>,
}

impl<'de> serde::Deserialize<'de> for StartArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = StartArgsDeserialize::deserialize(deserializer)?;
        Ok(StartArgs {
            project: raw.project,
            prompt: raw.prompt,
            git_ref: raw.git_ref,
            model: raw.model,
            effort: raw.effort,
            sandbox: raw.sandbox,
            add_dirs: raw.add_dirs,
            print_timeout: raw.print_timeout,
            hindsight: raw.hindsight,
            hindsight_bank: raw.hindsight_bank,
            hindsight_query: raw.hindsight_query,
            proxy: raw.proxy,
            from_worktree: raw.from_worktree,
            from_patch: raw.from_patch,
            plan: raw.plan,
            max_runtime: raw.max_runtime,
            jobset_id: raw.jobset_id,
            engine: raw.engine,
        })
    }
}

/// Re-launch agy in the same worktree continuing the previous conversation.
#[derive(Debug, Args, Clone, Default)]
pub struct RetryArgs {
    /// Short unique job ID
    pub job_id: String,

    /// Hindsight long-term memory for this retry: `on` re-enables it using the
    /// server's configured recall/retain defaults, `off` disables it entirely
    /// (no recall, no retain), absent: the stored per-job choice (or the
    /// server default) applies.
    #[arg(long, value_name = "<on|off>")]
    pub hindsight: Option<HindsightFlag>,

    /// Hindsight recall query for this job (replaces the full prompt as the query; at most 2000 characters)
    #[arg(long)]
    pub hindsight_query: Option<String>,

    /// Follow-up prompt for the new turn: continue the job's existing
    /// conversation with this prompt instead of re-running the stored one.
    /// The CLI shim resolves `@<file>` prompts locally (against the caller's
    /// cwd) and sends the file contents as literal text, exactly like `start`.
    /// Allowed on `done` jobs as well (a plain retry still refuses them).
    #[arg(long)]
    pub prompt: Option<String>,
}

#[derive(serde::Deserialize)]
struct RetryArgsDeserialize {
    #[serde(default)]
    pub job_id: String,
    #[serde(default)]
    pub hindsight: Option<HindsightFlag>,
    #[serde(default)]
    pub hindsight_query: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
}

impl<'de> serde::Deserialize<'de> for RetryArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RetryArgsDeserialize::deserialize(deserializer)?;
        Ok(RetryArgs {
            job_id: raw.job_id,
            hindsight: raw.hindsight,
            hindsight_query: raw.hindsight_query,
            prompt: raw.prompt,
        })
    }
}

/// Bare job ID (status/log/tail/kill).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct JobIdArg {
    /// Short unique job ID
    #[serde(default)]
    pub job_id: String,
}

/// Show git diff inside the job's worktree.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct DiffArgs {
    /// Short unique job ID
    #[serde(default)]
    pub job_id: String,

    /// Diff against base branch instead of worktree HEAD
    #[arg(long)]
    #[serde(default)]
    pub base: bool,

    /// Restrict the diff to these paths (repeatable; git pathspecs relative
    /// to the worktree root, so a directory name covers everything below it)
    #[arg(long)]
    #[serde(default)]
    pub only: Vec<String>,

    /// Exclude matching paths from the diff (repeatable; git pathspecs
    /// relative to the worktree root; matched as git `:(exclude)` patterns)
    #[arg(long)]
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Print one line per changed file (status, size in bytes, path) instead
    /// of the unified diff text
    #[arg(long)]
    #[serde(default)]
    pub stat: bool,
}

/// List all known jobs.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct ListArgs {
    /// Filter jobs by project name
    #[arg(long)]
    #[serde(default)]
    pub project: Option<String>,

    /// Only show jobs in this status (repeatable; each value may be
    /// comma-separated, e.g. --status running --status done,failed).
    /// Valid values: running, done, failed, self-terminated, killed,
    /// incomplete, quota-exhausted (an unknown status is an error).
    #[arg(
        long,
        num_args = 1..,
        value_delimiter = ',',
        value_name = "STATUS"
    )]
    #[serde(default)]
    pub status: Vec<String>,
}

/// Remove a job's worktree and metadata.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct CleanArgs {
    /// Short unique job ID
    #[serde(default)]
    pub job_id: String,

    /// Force removal even if worktree contains uncommitted/untracked files or
    /// is running
    #[arg(long)]
    #[serde(default)]
    pub force: bool,

    /// Also delete the job's log file
    #[arg(long)]
    #[serde(default)]
    pub purge_log: bool,
}

/// Repair a job's orphaned git worktree (re-attach it to agent-run's
/// private repository after a git-sync repo re-init dropped the admin
/// directory). Either one job ID or `--all`.
#[derive(Debug, Args, Clone, Default, serde::Deserialize)]
pub struct RepairArgs {
    /// The job to repair (full id or a unique prefix).
    #[arg(required_unless_present = "all")]
    #[serde(default)]
    pub job_id: Option<String>,

    /// Repair every job whose worktree is dangling.
    #[arg(long, conflicts_with = "job_id")]
    #[serde(default)]
    pub all: bool,
}

/// Garbage collect terminal jobs older than the configured age.
#[derive(Debug, Args, Clone, Default, serde::Deserialize)]
pub struct GcArgs {
    /// Age threshold duration (e.g. 14d, 48h, or number of days like 14)
    #[arg(long)]
    #[serde(default)]
    pub older_than: Option<String>,

    /// Filter GC to a specific project
    #[arg(long)]
    #[serde(default)]
    pub project: Option<String>,

    /// Maximum number of terminal jobs to retain; prunes oldest beyond this
    /// limit
    #[arg(long)]
    #[serde(default)]
    pub max_jobs: Option<usize>,

    /// Print what would be deleted without actually deleting anything
    #[arg(long)]
    #[serde(default)]
    pub dry_run: bool,

    /// Also delete job log files (default retains logs for non-Done jobs)
    #[arg(long)]
    #[serde(default)]
    pub purge_logs: bool,
}

/// Wait for job(s) to reach a terminal state (exit 0: done, 2: terminal but
/// not done, 3: timeout).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct WaitArgs {
    /// Job ID(s) to wait for (unique prefixes are resolved)
    #[arg(required = true)]
    pub job_ids: Vec<String>,

    /// Satisfied as soon as ANY of the given jobs is terminal (default for a
    /// single ID)
    #[arg(long, conflicts_with = "all")]
    #[serde(default)]
    pub any: bool,

    /// Satisfied only when ALL of the given jobs are terminal (default for
    /// multiple IDs)
    #[arg(long, conflicts_with = "any")]
    #[serde(default)]
    pub all: bool,

    /// Give up after a duration (e.g. 30m, 2h); by default wait indefinitely
    #[arg(long)]
    #[serde(default)]
    pub timeout: Option<String>,
}

/// Print the final structured result (response text) of a finished job.
///
/// JSON rendering is not part of the shared contract: the `data` payload
/// below the human lines is exactly the object the client pretty-prints
/// for `--json`, so the server always runs this command in human mode and
/// the client re-serializes `data` itself.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct ResultArgs {
    /// Job ID
    #[serde(default)]
    pub job_id: String,
}

/// Spawn a plan-mode review agent over another job's worktree diff.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct ReviewArgs {
    /// Job ID whose worktree diff should be reviewed
    #[serde(default)]
    pub job_id: String,

    /// Override the model for the reviewer agent
    #[arg(long)]
    #[serde(default)]
    pub reviewer_model: Option<String>,
}

/// Commit uncommitted changes in a job's worktree.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct CommitArgs {
    /// Job ID
    #[serde(default)]
    pub job_id: String,

    /// Commit message (defaults to a summary derived from the job's prompt)
    #[arg(short = 'm', long)]
    #[serde(default)]
    pub message: Option<String>,

    /// Commit only these paths (repeatable; git pathspecs relative to the
    /// worktree root); all other changes -- including untracked files -- are
    /// left uncommitted. Without the flag the whole worktree is committed.
    #[arg(long)]
    #[serde(default)]
    pub only: Vec<String>,

    /// Exclude matching paths from the commit (repeatable; git pathspecs
    /// relative to the worktree root; matched as git `:(exclude)` patterns)
    #[arg(long)]
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Delete named untracked files from a job's worktree.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct DiscardArgs {
    /// Short unique job ID
    #[arg(value_name = "JOB_ID")]
    #[serde(default)]
    pub job_id: String,

    /// Untracked file(s) to delete (repeatable; relative to the worktree
    /// root; explicit paths only -- no globs, no absolute paths, no `..`)
    #[arg(value_name = "PATH", required = true)]
    #[serde(default)]
    pub paths: Vec<String>,
}

/// Export a job's committed result to the caller's machine (patch or tar).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct ExportArgs {
    /// Short unique job ID
    #[arg(value_name = "JOB_ID")]
    #[serde(default)]
    pub job_id: String,

    /// Commit uncommitted (selected) changes first, with this message
    #[arg(short = 'm', long)]
    #[serde(default)]
    pub message: Option<String>,

    /// Export only the changes under these paths (repeatable; git pathspecs)
    #[arg(long)]
    #[serde(default)]
    pub only: Vec<String>,

    /// Exclude changes matching these globs (repeatable; git pathspecs)
    #[arg(long)]
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Local file to write (default `./<job-id>.patch` or `./<job-id>.tar`)
    #[arg(long)]
    #[serde(default)]
    pub out: Option<String>,

    /// Export a tar of the changed files instead of a git-format-patch
    #[arg(long)]
    #[serde(default)]
    pub tar: bool,

    /// Allow exports larger than the 50 MiB default cap
    #[arg(long)]
    #[serde(default)]
    pub allow_large: bool,

    /// Overwrite an existing `--out` file
    #[arg(long)]
    #[serde(default)]
    pub force: bool,
}

/// List files under a path in a job's worktree (read-only).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct FilesArgs {
    /// Short unique job ID
    #[arg(value_name = "JOB_ID")]
    #[serde(default)]
    pub job_id: String,

    /// Path to list (relative to the worktree; defaults to the worktree root)
    #[arg(value_name = "PATH")]
    #[serde(default)]
    pub path: Option<String>,

    /// How many levels deep to list (clamped to 1..=4)
    #[arg(long)]
    #[serde(default)]
    pub depth: Option<u32>,
}

/// Print one file from a job's worktree (read-only).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct CatArgs {
    /// Short unique job ID
    #[arg(value_name = "JOB_ID")]
    #[serde(default)]
    pub job_id: String,

    /// File to print (relative to the worktree)
    #[arg(value_name = "PATH")]
    #[serde(default)]
    pub path: String,

    /// 1-based inclusive line range `A:B`; either side may be omitted (`10:40`, `50:`, `:20`)
    #[arg(long, value_name = "A:B")]
    #[serde(default)]
    pub lines: Option<String>,

    /// Maximum number of file bytes to output (default 1048576, hard cap 8388608)
    #[arg(long)]
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

/// Show the agy quota (Gemini / Claude+GPT, weekly and five-hour limits).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct QuotaArgs {
    /// Bypass the 60 s in-memory cache and re-query agy
    #[arg(long)]
    #[serde(default)]
    pub fresh: bool,
}

/// List the models agy can use (from the server's agy).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct ModelsArgs {}

/// Server health + capability snapshot (`agent-run info`).
///
/// No flags: the snapshot is computed entirely from server state.
#[derive(Debug, Args, Clone, Default, serde::Deserialize)]
pub struct InfoArgs {}

/// List the projects `start <PROJECT>` accepts, with per-repo freshness
/// (branch, HEAD, shallow-clone flag, checkout mtime) and job counters.
/// Read-only server introspection; takes no arguments.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct ProjectsArgs {}

/// Lifecycle management of the agy remote-control daemon.
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct RemoteControlArgs {
    #[command(subcommand)]
    pub action: RemoteControlAction,
}

/// End a jobset: bump its epoch and mark it ended so its old credentials stop
/// verifying (operator-only, like every /api/v1 route).
#[derive(Debug, Args, Clone, serde::Deserialize)]
pub struct JobsetEndArgs {
    /// Jobset id (or a unique prefix is not accepted -- jobsets use exact ids).
    pub jobset_id: String,
}
#[derive(Debug, Subcommand, Clone, PartialEq, Eq, serde::Deserialize)]
pub enum RemoteControlAction {
    /// Enable and start the agy remote-control daemon
    Enable,
    /// Stop the agy remote-control daemon
    Disable,
    /// Show current status of the agy remote-control daemon
    Status,
}

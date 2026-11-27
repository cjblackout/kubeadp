use anyhow::{bail, Context, Result};
use std::fs::OpenOptions;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::core::agy_conversations;
use crate::core::config::Config;
use crate::core::hindsight::{
    append_to_log_file, build_retain_context, build_retain_finding, execute_retain, should_retain,
};
use crate::core::job::{
    compute_status, extract_conversation_id_from_log, get_process_start_time,
    restore_conversation_id, save_final_status_unless_killed, JobMetadata,
};
pub use crate::engine::{LaunchPlan, StdinMode, TurnInput};

/// Resolves a prompt argument.
///
/// If `prompt` starts with `@`, it indicates a file path (mirroring `agy`'s
/// `-p "@file"` convention). The file is read and its contents returned as
/// literal text. The `agent-run` CLI shim resolves `@file` prompts itself
/// (against the caller's cwd) before sending the request, so this path is
/// primarily a safety net for server-side callers.
pub fn resolve_prompt(raw_prompt: &str, caller_cwd: &Path) -> Result<String> {
    if let Some(rel_path) = raw_prompt.strip_prefix('@') {
        let path = Path::new(rel_path);
        let abs_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            caller_cwd.join(path)
        };

        if !abs_path.exists() {
            return Err(crate::core::error::AgentRunError::invalid(
                "prompt_file_not_found",
                format!("Prompt file does not exist: {}", abs_path.display()),
            )
            .into());
        }
        if abs_path.is_dir() {
            return Err(crate::core::error::AgentRunError::invalid(
                "prompt_path_is_directory",
                format!(
                    "Prompt path is a directory, not a file: {}",
                    abs_path.display()
                ),
            )
            .into());
        }

        let content = std::fs::read_to_string(&abs_path)
            .with_context(|| format!("Failed to read prompt file at {}", abs_path.display()))?;
        Ok(content)
    } else {
        Ok(raw_prompt.to_string())
    }
}

/// Mode of invocation for agy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationMode {
    Start {
        resolved_prompt: String,
    },
    Retry {
        conversation_id: Option<String>,
        prompt: Option<String>,
    },
}

/// Builds the underlying command path and argument list according to
/// configuration and job metadata.
///
/// The engine has no privilege separation: agy always runs as the process
/// user of the agent-run-api server, inheriting its environment (which is
/// how the pod's `GH_TOKEN`, the declarative git identity, and agy's
/// credential files in the home volume reach the job).
///
/// `<agy_path> --dangerously-skip-permissions --mode <m> --output-format stream-json ...`
///
/// `live` (a live-mode job, see [`crate::core::a2a_live`]) changes the
/// invocation: the prompt is no longer handed on `-p` but as the FIRST stdin
/// line (`--input-format stream-json`); agy still requires a prompt argument,
/// so `--print ""` is passed in its place. Every other element is unchanged,
/// and the caller writes the (normalized) prompt over the piped stdin.
pub fn build_command_and_args(
    config: &Config,
    meta: &JobMetadata,
    mode: &InvocationMode,
    live: bool,
) -> (std::path::PathBuf, Vec<String>) {
    let registry = crate::engine::EngineRegistry::with_builtin(config);
    if let Ok(eng) = registry.for_job(meta) {
        let turn = match mode {
            InvocationMode::Start { resolved_prompt } => TurnInput::Start {
                prompt: resolved_prompt,
                live,
            },
            InvocationMode::Retry {
                conversation_id,
                prompt,
            } => TurnInput::Resume {
                prompt: prompt.as_deref().unwrap_or_default(),
                conversation: conversation_id.as_deref(),
                live,
            },
        };
        if let Ok(plan) = eng.job_command(meta, turn) {
            return (plan.program, plan.args);
        }
    }

    // Build agy arguments:
    // ALWAYS pass --dangerously-skip-permissions, --mode, and --output-format stream-json
    // unconditionally. Stream-JSON emits NDJSON step events as they happen, so
    // the job log shows live progress while the final `event: result` object
    // keeps the structured status/result contract.
    let mut agy_args: Vec<String> = vec![
        "--dangerously-skip-permissions".to_string(),
        "--mode".to_string(),
        meta.mode.clone(),
        "--output-format".to_string(),
        "stream-json".to_string(),
    ];

    // Prompt or Retry continuation
    match mode {
        InvocationMode::Start { resolved_prompt } => {
            if live {
                // Live mode: the prompt travels as the first stdin line; agy
                // still requires a prompt arg, so supply an empty one here.
                agy_args.push("--print".to_string());
                agy_args.push("".to_string());
            } else {
                agy_args.push("-p".to_string());
                agy_args.push(resolved_prompt.clone());
            }
        }
        InvocationMode::Retry {
            conversation_id,
            prompt,
        } => {
            if let Some(conv_id) = conversation_id {
                agy_args.push("--conversation".to_string());
                agy_args.push(conv_id.clone());
            } else {
                // Bare `agy --continue` opens an interactive resume picker when run without -p,
                // which requires a TTY and cannot proceed in background execution.
                // `commands::retry::run` refuses retries without a conversation ID before
                // reaching here; kept so this function remains a total mapping over InvocationMode.
                agy_args.push("--continue".to_string());
            }
            if live {
                agy_args.push("--print".to_string());
                agy_args.push("".to_string());
            } else if let Some(p) = prompt {
                agy_args.push("-p".to_string());
                agy_args.push(p.clone());
            }
        }
    }

    // Live mode: A2A messages arrive as NDJSON over stdin, so switch the input
    // transport to stream-json (the prompt itself is the first stdin line).
    if live {
        agy_args.push("--input-format".to_string());
        agy_args.push("stream-json".to_string());
    }

    // Model
    if let Some(m) = &meta.model {
        agy_args.push("--model".to_string());
        agy_args.push(m.clone());
    }

    // Effort
    if let Some(e) = &meta.effort {
        agy_args.push("--effort".to_string());
        agy_args.push(e.clone());
    }

    // Sandbox
    if meta.sandbox {
        agy_args.push("--sandbox".to_string());
    }

    // Add dirs: the worktree path must ALWAYS be passed first unconditionally
    agy_args.push("--add-dir".to_string());
    agy_args.push(meta.worktree_path.to_string_lossy().to_string());

    for dir in &meta.add_dirs {
        agy_args.push("--add-dir".to_string());
        agy_args.push(dir.to_string_lossy().to_string());
    }

    // Print timeout
    if let Some(timeout) = &meta.print_timeout {
        agy_args.push("--print-timeout".to_string());
        agy_args.push(timeout.clone());
    }

    (config.agy_path.clone(), agy_args)
}

/// Builds the `std::process::Command` according to configuration and job metadata.
///
/// Every job (and every retry attempt, which goes through the same function)
/// is launched through `sandbox-exec` with a freshly written policy file in
/// the job's log directory. The sandbox isolation is ALWAYS applied and the
/// runtime is auto-detected inside the pod (`sandbox-exec` -- Contract v1.3):
/// there is no `--runtime` flag and no user-facing runtime override. A policy
/// file that cannot be written makes the launch fail here -- the command must
/// never run without isolation.
///
/// On success the returned `Command` wraps `sandbox-exec ... -- <agy> ...`;
/// the policy file path is also reported back in `policy_path` so callers can
/// surface it in job status/info output.
/// The private bare repository a job worktree is linked to: the `gitdir:`
/// pointer of its `.git` file (`<repos_root>/<project>.git/worktrees/<id>`)
/// resolved to `<repos_root>/<project>.git`, and only when that directory lies
/// directly under `repos_root`. `None` for a self-contained worktree, a missing
/// or malformed pointer, or a pointer outside the repositories agent-run
/// manages.
fn job_project_repo(
    worktree: &std::path::Path,
    repos_root: &std::path::Path,
) -> Option<std::path::PathBuf> {
    let target = crate::core::git::worktree_gitdir_target(worktree)?;
    let gitdir = std::fs::canonicalize(&target).ok()?;
    let repo = gitdir.parent()?.parent()?;
    if gitdir.parent()?.file_name()? != "worktrees" {
        return None;
    }
    let root = std::fs::canonicalize(repos_root).ok()?;
    (repo.parent()? == root).then(|| repo.to_path_buf())
}

/// Wraps a [`crate::engine::LaunchPlan`] with `sandbox-exec` in exactly one function,
/// for every engine and role.
pub fn sandboxed(
    plan: crate::engine::LaunchPlan,
    config: &Config,
    meta: &JobMetadata,
) -> Result<(Command, std::path::PathBuf, Option<String>)> {
    let mut base_policy = crate::sandbox::SandboxPolicy::for_job(
        &meta.job_id,
        &meta.worktree_path,
        None,
        Some(&plan.program),
        Some(vec![config.sandbox_home.clone()]),
    )
    .with_workspace_root(&config.workspace_root, Some(&config.jobs_dir_name))
    .with_extra_ro_paths(&config.sandbox_ro_paths);
    // A job worktree is a linked worktree of the project's private bare repo,
    // which the sandbox hides with every other repo under `repos_root`: without
    // this grant git inside the job fails ("not a git repository"). Only the
    // job's OWN project repo is re-exposed (never another project's).
    if let Some(repo) = job_project_repo(&meta.worktree_path, &config.repos_root) {
        base_policy.rw.push(repo);
    }

    let jobset = crate::core::jobset::agent_jobset(&meta.job_id);
    let policy = match &jobset {
        Some(_) => crate::core::jobset::build_jobset_policy(&base_policy),
        None => base_policy,
    };
    let policy = policy
        .hide_operator_token_file()
        .hide_jobset_root_key_file()
        .with_extra_env_allow(&config.sandbox_env_allow);
    let policy = if plan.program.file_name().is_some_and(|f| f == "claude") {
        policy.allow_claude_oauth_env()
    } else {
        policy
    };
    let policy_path =
        JobMetadata::log_dir(&config.state_root).join(format!("{}.policy.json", meta.job_id));
    if let Err(e) = std::fs::create_dir_all(JobMetadata::log_dir(&config.state_root)) {
        bail!(
            "failed to create job log directory for sandbox policy '{}': {e}",
            JobMetadata::log_dir(&config.state_root).display()
        );
    }
    crate::sandbox::SandboxPolicy::write_policy_file(&policy, &policy_path)
        .map_err(|e| anyhow::anyhow!("job '{}' cannot be launched sandboxed: {e}", meta.job_id))?;

    let passthrough = {
        #[cfg(feature = "test-hooks")]
        {
            crate::sandbox::testhook::sandbox_passthrough()
        }
        #[cfg(not(feature = "test-hooks"))]
        {
            false
        }
    };
    let argv = launch_argv(&plan.program, &plan.args, &policy_path, passthrough);
    let mut cmd = Command::new(argv[0].clone());
    cmd.args(&argv[1..]);

    for (k, v) in plan.env {
        cmd.env(k, v);
    }

    if let Some(cred) = &jobset {
        let private_tmp = policy
            .private_tmp_path()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
        for (k, v) in crate::core::jobset::build_jobset_env(cred, &private_tmp) {
            cmd.env(k, v);
        }
    }
    apply_a2a_self_name(&mut cmd, &meta.job_id);

    #[cfg(feature = "a2a")]
    {
        let a2a_inbox_dir =
            crate::core::a2a::inbox::inbox_dir_for(&config.state_root, &meta.job_id);
        cmd.env(crate::core::a2a::inbox::INBOX_ENV, &a2a_inbox_dir);
        // USER-VISIBLE A2A ECHO (HL-125 3.7): hand the resolved host config
        // down as `KUBEADP_A2A_ECHO` so the in-run harness (`a2a-hook`,
        // `mcp-a2a`) echoes every message to the transcript. `KUBEADP_*`
        // passes the default sandbox env filter. The child reads it verbatim
        // ("0"/"false" turns the echo off).
        cmd.env(
            crate::core::a2a::inject::A2A_ECHO_ENV,
            if config.a2a_echo { "1" } else { "0" },
        );
        if let Some(started_at) = meta.started_at {
            let secs = u64::try_from(started_at.timestamp()).unwrap_or(0);
            if let Some(token) = crate::core::a2a::http::token_for_run(&a2a_inbox_dir, secs) {
                cmd.env(crate::core::a2a::receive::INBOX_TOKEN_ENV, token);
            }
        }
        let listen =
            std::env::var("AGENT_RUN_API_LISTEN").unwrap_or_else(|_| "0.0.0.0:8002".to_string());
        if let Some(url) = crate::core::a2a::receive::inbox_url_for_listen(&listen) {
            cmd.env(crate::core::a2a::receive::INBOX_URL_ENV, url);
        }
    }

    crate::core::proxy::apply_proxy(&mut cmd, Some(config), meta.proxy.as_deref());
    // The secret channel carries the credential file (SPEC §6.5): the agent id,
    // the jobset id, epoch, secret and member proof as JSON.
    Ok((
        cmd,
        policy_path,
        crate::core::jobset::agent_credential_file_json(&meta.job_id).or_else(|| {
            jobset.map(|c| crate::core::jobset::credential_file_json(&meta.job_id, &c))
        }),
    ))
}

pub fn build_command(
    config: &Config,
    meta: &JobMetadata,
    mode: &InvocationMode,
    live: bool,
) -> Result<(Command, std::path::PathBuf, Option<String>), anyhow::Error> {
    let (prog, args) = build_command_and_args(config, meta, mode, live);
    let plan = crate::engine::LaunchPlan {
        program: prog,
        args,
        env: Vec::new(),
        stdin: crate::engine::StdinMode::Null,
        pty: false,
    };
    sandboxed(plan, config, meta)
}

/// Sets a launched job's `A2A_SELF_NAME` to `<prefix>/<job_id>` so every job
/// presents a unique, addressable A2A sender identity.
///
/// `<prefix>` is the discoverable agent prefix of THIS host: the `A2A_SELF_PREFIX`
/// env var (the Service name peers resolve, e.g. `kubeadp-agent-worker-service`,
/// set in the worker manifest); else the inherited `A2A_SELF_NAME`'s first
/// `/`-segment; else the pod's kernel comm name; else `kubeadp-agent`. The
/// `<job_id>` suffix lets a peer address this job as `<prefix>/<job_id>`: the
/// reply tool splits the single trailing `/`-segment into the A2A `task_id`
/// while the host's A2A surface (which serves tasks by job id) stays the
/// resolution target.
///
/// `A2A_SELF_NAME` is covered by the default `A2A_*` sandbox allow pattern, so
/// the value passes through into the sandboxed job.
pub fn apply_a2a_self_name(cmd: &mut Command, id: &str) {
    let prefix = std::env::var("A2A_SELF_PREFIX")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // Inherited `A2A_SELF_NAME`: take its first `/`-segment (whether
            // the incoming value is `prefix/job` or, when it has no slash, the
            // prefix is the whole bare name itself).
            std::env::var("A2A_SELF_NAME")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(|s| match s.split_once('/') {
                    Some((p, _)) => p.to_string(),
                    None => s,
                })
                .filter(|p| !p.is_empty())
        })
        .or_else(|| {
            std::fs::read_to_string("/proc/self/comm")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "kubeadp-agent".to_string());
    cmd.env("A2A_SELF_NAME", format!("{prefix}/{id}"));
}

/// Assembles the process `argv` for a launch. Pure (no process spawn) so the
/// always-on `sandbox-exec` wrapping can be asserted directly.
///
/// `passthrough == false` (production, always; or a test that opted back in):
/// the agy command is wrapped in `kubeadp-agent sandbox-exec --policy <file>
/// -- <agy> ...`. `passthrough == true` (only reachable under the `test-hooks`
/// feature, default on there): the plain agy argv, so fake-agy unit tests run
/// without a working sandbox runtime.
fn launch_argv(
    prog: &std::path::Path,
    args: &[String],
    policy_path: &std::path::Path,
    passthrough: bool,
) -> Vec<String> {
    if passthrough {
        let mut v = vec![prog.to_string_lossy().to_string()];
        v.extend_from_slice(args);
        v
    } else {
        let mut v = vec![crate::sandbox::find_launcher_exe()
            .to_string_lossy()
            .to_string()];
        v.push("sandbox-exec".to_string());
        v.push("--policy".to_string());
        v.push(policy_path.to_string_lossy().to_string());
        v.push("--".to_string());
        v.push(prog.to_string_lossy().to_string());
        v.extend_from_slice(args);
        v
    }
}

/// Upper bound for prompt text passed as a single argv element.
///
/// Unix `execve` rejects a single argument string longer than 32 pages
/// (128 KiB), so an oversized prompt surfaces as a cryptic
/// "Failed to spawn executable" (E2BIG) error. Checking before spawn turns
/// that into a clear error naming the limit. 100_000 bytes keeps the bound
/// below the OS limit with headroom for the rest of the argument vector.
pub(crate) const PROMPT_ARGV_MAX_BYTES: usize = 100_000;

/// Non-blocking drain of already-exited orphaned children (see the call site in
/// [`run_internal_runner`]).
///
/// Goes through the guarded [`crate::core::reaper::reap_round`] instead of a raw
/// `waitpid(-1, WNOHANG)`: other in-server commands (`quota`, `info`,
/// `projects`, `models`, `remote-control`, ...) spawn children whose exit
/// they collect themselves while a job monitor may finish concurrently. A raw
/// drain would reap such a child first and the owner would see `ECHILD` with
/// no recoverable status; `reap_round` skips the round while any tracked child
/// is a zombie and stashes the exit in the narrow residual window.
fn drain_orphans() {
    let _ = crate::core::reaper::reap_round();
}

/// Maps a child `ExitStatus` to the exit code recorded in the job row.
///
/// A normal exit passes the code through unchanged; a death by signal is
/// recorded as `128 + signal` (e.g. 137 for SIGKILL, 143 for SIGTERM),
/// matching the crate's existing 137/143 taxonomy and its status
/// classification (`determine_status_from_signals`). An `ExitStatus` with
/// neither a code nor a signal (only possible for exotic wait statuses)
/// falls back to -1, the previous "unknown exit" sentinel.
pub fn exit_code_from_status(status: std::process::ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        code
    } else {
        match ExitStatusExt::signal(&status) {
            Some(sig) => 128 + sig,
            None => -1,
        }
    }
}

/// Validates that configured binary paths exist and are executable when they
/// are absolute paths.
///
/// The existence check catches a missing binary. The executability check
/// catches one that exists but cannot be run: every job is launched through
/// the `sandbox-exec` wrapper (see [`build_command`]), which always spawns
/// successfully, so an unrunnable agy no longer surfaces as a `spawn()` EACCES
/// in the monitor (it would only show up later as the wrapper's exit code
/// 126, with the wrapper's PID already confirmed as a "successful" launch).
/// Failing fast here keeps an unrunnable agy from ever being reported as a
/// started job, so the caller can bail and tear down the worktree/branch it
/// just created. `access(X_OK)` mirrors exactly what the sandbox's `execvpe`
/// will enforce for the (same-uid) agent.
pub fn validate_executable_paths(config: &Config) -> Result<()> {
    validate_engine_executable_path(config, None)
}

/// Validates the configured engine executable path for `engine`.
/// Resolves the engine via [`crate::engine::EngineRegistry`] and validates
/// the program path reported by [`crate::engine::Engine::configured_program`].
pub fn validate_engine_executable_path(config: &Config, engine: Option<&str>) -> Result<()> {
    let registry = crate::engine::EngineRegistry::with_builtin(config);
    let resolved = registry.resolve(crate::engine::RunKind::Job, engine, None)?;
    let label = resolved.display_name();
    let path = resolved.configured_program(config);
    if path.is_absolute() {
        if !path.exists() {
            bail!(
                "Configured {label} binary does not exist at '{}'",
                path.display()
            );
        }
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
            bail!(
                "Configured {label} binary path '{}' contains an interior NUL byte",
                path.display()
            );
        };
        if unsafe { libc::access(c_path.as_ptr(), libc::X_OK) } != 0 {
            bail!(
                "Configured {label} binary is not executable at '{}'",
                path.display()
            );
        }
    }
    Ok(())
}

/// Puts a spawned child into its own new session and process group by
/// calling `setsid()` between fork and exec (async-signal-safe).
///
/// Applied to the agy job process and the auth child: the process is then
/// unreachable from any terminal session (SIGHUP does not kill it), and
/// kills can target the whole tree by the process group of its PID (a
/// group leader with `pgid == pid`).
pub(crate) fn detached_session(cmd: &mut Command) {
    // SAFETY: the pre_exec closure runs in the forked child before exec and
    // only calls `libc::setsid()` (async-signal-safe) plus errno inspection;
    // no allocation, locking, or non-reentrant calls in the child.
    unsafe {
        cmd.pre_exec(move || {
            if libc::setsid() == -1 {
                // EPERM: already a session/process-group leader -- nothing to do.
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EPERM) {
                    return Ok(());
                }
                return Err(err);
            }
            Ok(())
        });
    }
}

/// Sets the job spawn's `pre_exec` to (a) start a new session / process group
/// (`setsid`, exactly [`detached_session`]) and (b), when `secret` is `Some`,
/// hand the jobset secret to the child on file descriptor 3 (the conventional
/// `secret` channel the sandbox backends read via `read_secret_fd`).
///
/// One combined `pre_exec` is required because `Command::pre_exec` can hold
/// only a single closure. The secret is never in argv or the environment: it is
/// written into an anonymous `memfd` (inaccessible by name) whose `OwnedFd` is
/// captured by the closure, so it stays open in the parent until `spawn()` and
/// is dup'd onto fd 3 in the forked child before exec. The sandbox reads fd 3
/// and writes it to a 0400 file in the agent's private tmp; the agent reads
/// only the PATH of that file (`KUBEADP_JOBSET_CREDENTIALS_FILE`), never the secret.
/// A jobset member whose `memfd` cannot be created fails loudly (the launch is
/// aborted): it must not run without its secret channel.
fn attach_session_and_secret(cmd: &mut Command, mut secret: Option<String>) -> Result<()> {
    let secret_fd: Option<std::os::fd::OwnedFd> = match secret.as_mut() {
        Some(s) => {
            use std::ffi::CString;
            use std::os::fd::FromRawFd;
            let name = CString::new("kubeadp-agent-jobset-secret").expect("static, no NUL");
            let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
            if fd < 0 {
                let err = std::io::Error::last_os_error();
                // Zeroize the launcher's copy even on failure.
                for b in unsafe { s.as_bytes_mut() } {
                    unsafe { std::ptr::write_volatile(b, 0) };
                }
                std::sync::atomic::compiler_fence(Ordering::SeqCst);
                bail!("jobset secret memfd_create failed: {err}");
            }
            let bytes = s.as_bytes();
            let mut off = 0usize;
            while off < bytes.len() {
                let n = unsafe {
                    libc::write(
                        fd,
                        bytes[off..].as_ptr() as *const libc::c_void,
                        bytes.len() - off,
                    )
                };
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    let _ = unsafe { libc::close(fd) };
                    for b in unsafe { s.as_bytes_mut() } {
                        unsafe { std::ptr::write_volatile(b, 0) };
                    }
                    std::sync::atomic::compiler_fence(Ordering::SeqCst);
                    bail!("jobset secret memfd write failed: {err}");
                }
                off += n as usize;
            }
            // Zeroize the launcher's in-memory copy of the secret now that it
            // has been written to the anonymous memfd (SPEC §6.5 / §10.4).
            for b in unsafe { s.as_bytes_mut() } {
                unsafe { std::ptr::write_volatile(b, 0) };
            }
            std::sync::atomic::compiler_fence(Ordering::SeqCst);
            // Rewind the shared file offset: the child reads the secret from fd 3
            // (a dup of this memfd) via `read_secret_fd`, which reads from the
            // current offset to EOF. Without the rewind the child sees an empty
            // secret and cannot authenticate as a jobset member.
            if unsafe { libc::lseek(fd, 0, libc::SEEK_SET) } != 0 {
                let err = std::io::Error::last_os_error();
                let _ = unsafe { libc::close(fd) };
                bail!("jobset secret memfd lseek failed: {err}");
            }
            Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
        }
        None => None,
    };
    drop(secret);
    // SAFETY: the closure runs in the forked child between fork and exec and
    // only makes async-signal-safe calls (`dup2`/`close`/`fcntl`/`setsid` plus
    // errno inspection); no allocation, locking, or non-reentrant calls. The
    // memfd's `OwnedFd` is captured by value, so its fd is open in the child
    // (inherited by fork) when `dup2` runs; the child's extra copy is closed
    // right after the dup and, in the parent, is closed when `cmd` (holding the
    // closure) is dropped after `spawn()`. Only fd 3 (CLOEXEC cleared) survives
    // the exec into the sandbox.
    unsafe {
        cmd.pre_exec(move || {
            if let Some(secret_fd) = secret_fd.as_ref() {
                use std::os::fd::AsRawFd;
                let src = secret_fd.as_raw_fd();
                let r = libc::dup2(src, 3);
                if src != 3 {
                    let _ = libc::close(src);
                }
                if r < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // `dup2(src, src)` is a no-op that would leave fd 3's CLOEXEC
                // set; clear the FD flags so fd 3 survives the exec.
                if libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            if libc::setsid() == -1 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EPERM) {
                    return Ok(());
                }
                return Err(err);
            }
            Ok(())
        });
    }
    Ok(())
}

/// Marks this process as a child subreaper (`prctl(PR_SET_CHILD_SUBREAPER, 1)`).
///
/// `setsid()` only moves a process into its own session for
/// terminal/process-group signalling; it does NOT make the process a reaping
/// ancestor for its own (grand)children. As a subreaper, any descendant that
/// gets orphaned while this process is still alive is reparented here instead
/// of skipping past it to PID 1. That is exactly what happens when agy runs
/// `some-long-command &` and then exits its own turn: the background command's
/// process is orphaned the moment agy exits, and without a subreaper it would
/// land on PID 1, which in the api pod is the server process itself (single
/// container, no reaping init).
///
/// The flag is process-wide (the kernel keeps it in the shared signal
/// struct), so calling it once at server startup covers every runner thread.
/// Best effort: if `prctl` fails the job proceeds with the previous
/// (unsubreapered) behavior, and a warning is written to stderr.
pub fn mark_child_subreaper() {
    // SAFETY: `PR_SET_CHILD_SUBREAPER` passes only integer arguments -- the
    // kernel dereferences no pointer; no allocation, locking, or
    // non-reentrant calls.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == -1 {
        eprintln!(
            "agent-run: warning: prctl(PR_SET_CHILD_SUBREAPER, 1) failed: {} -- orphaned descendants of this job will reparent to PID 1 instead of the server",
            std::io::Error::last_os_error()
        );
    }
}

// =========================================================================
// Launch Handshake Protocol
// =========================================================================
// The launcher thread (`launch_background_job_with_timeout`) and the monitor
// thread (`run_internal_runner`) coordinate via an `Arc<AtomicU8>` state machine:
//
// 1. Initial state is LAUNCH_STATE_PENDING (0).
// 2. Monitor checks state before spawn; if CANCELLED (2), it exits without spawning agy.
// 3. Monitor spawns agy, then attempts CAS: compare_exchange(PENDING, RUNNING).
// 4. Launcher polls disk row for PID until timeout; on timeout, attempts CAS: compare_exchange(PENDING, CANCELLED).
// 5. If launcher wins CAS (PENDING -> CANCELLED): monitor's CAS fails, monitor immediately kills the
//    process group of the spawned child, reaps it, records no state, and launcher fails cleanly.
// 6. If monitor wins CAS (PENDING -> RUNNING): launcher's CAS fails, monitor proceeds to save PID row,
//    and launcher waits a bounded grace period (up to 5s) for the row to land on disk and returns Ok(pid).
//
// Exactly one side wins: a failed launch never leaves a running agy process or resurrected row.

pub const LAUNCH_STATE_PENDING: u8 = 0;
pub const LAUNCH_STATE_RUNNING: u8 = 1;
pub const LAUNCH_STATE_CANCELLED: u8 = 2;

#[cfg(test)]
type TestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
#[derive(Default, Clone)]
struct JobHooks {
    pre_spawn: Option<TestHook>,
    post_spawn: Option<TestHook>,
}

#[cfg(test)]
static TEST_HOOKS: std::sync::Mutex<Option<std::collections::HashMap<String, JobHooks>>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn register_test_pre_spawn_hook<F>(job_id: &str, hook: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let mut lock = TEST_HOOKS.lock().unwrap();
    let map = lock.get_or_insert_with(std::collections::HashMap::new);
    map.entry(job_id.to_string()).or_default().pre_spawn = Some(Arc::new(hook));
}

#[cfg(test)]
pub(crate) fn register_test_post_spawn_hook<F>(job_id: &str, hook: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let mut lock = TEST_HOOKS.lock().unwrap();
    let map = lock.get_or_insert_with(std::collections::HashMap::new);
    map.entry(job_id.to_string()).or_default().post_spawn = Some(Arc::new(hook));
}

#[cfg(test)]
pub(crate) fn clear_test_hooks(job_id: &str) {
    let mut lock = TEST_HOOKS.lock().unwrap();
    if let Some(map) = lock.as_mut() {
        map.remove(job_id);
    }
}

#[cfg(test)]
fn execute_test_pre_spawn_hook(job_id: &str) {
    let hook = {
        let mut lock = TEST_HOOKS.lock().unwrap();
        lock.as_mut()
            .and_then(|map| map.get(job_id).and_then(|h| h.pre_spawn.clone()))
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn execute_test_post_spawn_hook(job_id: &str) {
    let hook = {
        let mut lock = TEST_HOOKS.lock().unwrap();
        lock.as_mut()
            .and_then(|map| map.get(job_id).and_then(|h| h.post_spawn.clone()))
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(not(test))]
#[inline(always)]
fn execute_test_pre_spawn_hook(_job_id: &str) {}

#[cfg(not(test))]
#[inline(always)]
fn execute_test_post_spawn_hook(_job_id: &str) {}

/// Launches a background job by running the job monitor ([`run_internal_runner`])
/// on an in-process OS thread.
///
/// The agent-run-api server is the only execution engine and a long-lived
/// process, so the monitor lives in-process: the thread holds the `Child`
/// handle and `wait()`s, so the agy process is always reaped (no zombies, no
/// detached helper binary needed). The thread moves its own clone of the
/// `Config` (no env-var serialization of the config, no secrets in argv).
///
/// Returns the actual agy process PID after the monitor has recorded it (or
/// bails if the PID was not confirmed within `config.launch_timeout_secs`,
/// the confirmation window set by `AGENT_RUN_LAUNCH_TIMEOUT_SECS`, default
/// 20 s). A launch that times out is cancelled: the poll loop arbitrates with
/// the monitor via an atomic compare-exchange state machine (see
/// [`launch_background_job_with_timeout`]) so it never leaves a running agy
/// behind.
pub fn launch_background_job(config: &Config, meta: &mut JobMetadata) -> Result<u32> {
    launch_background_job_with_timeout(
        config,
        meta,
        Duration::from_secs(config.launch_timeout_secs),
        Duration::from_millis(50),
    )
}

/// Launches the in-process runner with configurable timeout and polling
/// interval (for testing).
pub fn launch_background_job_with_timeout(
    config: &Config,
    meta: &mut JobMetadata,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<u32> {
    // 0. Validate executable paths upfront before creating state files
    validate_engine_executable_path(config, meta.engine.as_deref())?;

    // 0a. The prompt travels as a single argv element to agy; a prompt longer
    // than PROMPT_ARGV_MAX_BYTES would make execve fail with E2BIG, surfacing
    // as a misleading spawn error. The spawned element follows `mode`
    // selection in `run_internal_runner` (resolved prompt for start
    // attempts, retry prompt for retries), each normalized with the
    // per-turn reference line, so check that same (tokenized) value and
    // return a clear error to the caller before any job row or state file
    // exists.
    let argv_prompt_len = if meta.retry_count > 0 {
        meta.retry_prompt.as_deref().map_or(0, |s| {
            agy_conversations::normalize_ref_prompt(s, &meta.job_id, meta.retry_count).len()
        })
    } else {
        agy_conversations::normalize_ref_prompt(&meta.resolved_prompt, &meta.job_id, 0).len()
    };
    if argv_prompt_len > PROMPT_ARGV_MAX_BYTES {
        bail!(
            "prompt is {} bytes, longer than the {} byte limit for prompts passed to agy as an argv element (execve would fail with E2BIG); provide the prompt via a file (@path) or shorten it",
            argv_prompt_len,
            PROMPT_ARGV_MAX_BYTES
        );
    }

    // 1. Ensure log directory and job state directory exist
    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create log directory at {}", parent.display()))?;
    }

    // 2. Save initial metadata
    meta.save(&config.state_root)?;

    // 2b. Register the agent in the process-wide jobset registry (idempotent:
    // a retry reuses the job's jobset; a brand-new job joins the host's jobset
    // if the host is a member, or gets a fresh random jobset if the host was
    // started by an operator request). Done just before the monitor thread
    // spawns so `build_command` can wire the sandbox `secret` channel + jobset
    // env and the A2A surface can accept the job's credential. The secret
    // exists in memory only and the entry is dropped on every end path
    // (monitor completion / kill / clean / gc / a failed start's row delete).
    // A job started with `--jobset` rejoins that jobset on every retry.
    match crate::core::jobset::recall_job_jobset(&config.state_root, &meta.job_id) {
        Some(jobset_id) => {
            if let Err(e) = crate::core::jobset::register_agent_into(&meta.job_id, &jobset_id) {
                eprintln!("jobset rejoin failed for job {}: {e}", meta.job_id);
                let _ = crate::core::jobset::register_agent(&meta.job_id);
            }
        }
        None => {
            let cred = crate::core::jobset::register_agent(&meta.job_id);
            crate::core::jobset::remember_job_jobset(
                &config.state_root,
                &meta.job_id,
                &cred.jobset_id,
            );
        }
    }

    // 3. Spawn the job monitor on an in-process thread. The handshake is
    // the atomic state machine coordinating launcher and monitor:
    // PENDING -> RUNNING (monitor wins) or PENDING -> CANCELLED (launcher wins).
    let handshake = Arc::new(AtomicU8::new(LAUNCH_STATE_PENDING));
    let thread_config = config.clone();
    let thread_job_id = meta.job_id.clone();
    let thread_handshake = Arc::clone(&handshake);
    thread::Builder::new()
        .name(format!("agent-run-job-{}", meta.job_id))
        .spawn(move || {
            if let Err(e) =
                run_internal_runner(&thread_config, &thread_job_id, Some(thread_handshake))
            {
                // run_internal_runner already appended a diagnostic to the job
                // log for the common case (agy spawn failure); this covers the
                // rest (state store I/O, ...) on the server's own stderr.
                eprintln!(
                    "agent-run: job monitor for '{}' exited with error: {}",
                    thread_job_id, e
                );
            }
        })
        .context("Failed to spawn job monitor thread")?;

    // 4. Poll for the PID recorded by the monitor
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(updated) = JobMetadata::load(&config.state_root, &meta.job_id) {
            if let Some(pid) = updated.pid {
                meta.pid = Some(pid);
                meta.pid_start_time = updated.pid_start_time;
                return Ok(pid);
            }
        }
        thread::sleep(poll_interval);
    }

    // 5. The confirmation window elapsed without a confirmed PID.
    // Attempt to transition PENDING -> CANCELLED via compare_exchange.
    match handshake.compare_exchange(
        LAUNCH_STATE_PENDING,
        LAUNCH_STATE_CANCELLED,
        Ordering::SeqCst,
        Ordering::SeqCst,
    ) {
        Ok(_) => {
            // Launcher won the CAS: state is CANCELLED.
            // Monitor is guaranteed to either not spawn agy or, if already spawned,
            // kill its process group, reap it, and record nothing.
            bail!(
                "job launch did not confirm a PID within {}ms -- the job monitor thread likely failed to spawn agy; check the job log and server output (job id: {})",
                timeout.as_millis(),
                meta.job_id
            );
        }
        Err(_) => {
            // Monitor won the CAS: state is RUNNING.
            // Monitor has spawned agy and is writing the PID row to disk.
            // Wait boundedly (up to 5s) for the row to become visible.
            let grace_deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < grace_deadline {
                if let Ok(updated) = JobMetadata::load(&config.state_root, &meta.job_id) {
                    if let Some(pid) = updated.pid {
                        meta.pid = Some(pid);
                        meta.pid_start_time = updated.pid_start_time;
                        return Ok(pid);
                    }
                }
                thread::sleep(poll_interval);
            }
            bail!(
                "job launch monitor confirmed RUNNING state but PID was not readable within grace period (job id: {})",
                meta.job_id
            );
        }
    }
}

/// Aborts a launch whose monitor has already spawned agy but whose launcher
/// has just declared the launch failed: signals the agy process group (agy
/// is a session/process-group leader via [`detached_session`], so the group
/// signal also reaches anything agy forked), reaps the child, and leaves a
/// cancellation note in the job log WITHOUT recording any job state -- so the
/// row the launcher is about to discard is never resurrected and no agy
/// process is left running (the orphan bug this whole handshake exists to
/// prevent).
fn abort_cancelled_launch(
    pid: u32,
    job_id: &str,
    log_path: &Path,
    child: &mut std::process::Child,
) {
    if let Err(e) = crate::core::commands::kill::send_signal(pid, "KILL") {
        eprintln!(
            "agent-run: warning: could not signal a cancelled agy (pid {pid}, job '{job_id}'): {e}"
        );
    }
    // Reap the child here: this monitor is the only reaper of the agy
    // process, and a killed-but-unreaped child would linger as a zombie.
    // ECHILD arm: the in-server zombie reaper took it first (TOCTOU
    // residue) -- its exit status is irrelevant on this cancellation path.
    if let Err(e) = child.wait() {
        if e.raw_os_error() == Some(libc::ECHILD) {
            let _ = crate::core::reaper::wait_stashed_exit(pid, Duration::from_secs(5));
        }
    }
    crate::core::reaper::untrack_child(pid);
    append_to_log_file(
        log_path,
        "[agent-run] launch cancelled: agy's process group was terminated, no job state recorded.",
    );
}

/// The job monitor: spawns the `agy` process for a job, records the PID,
/// waits for completion, records the exit code in the job record, extracts
/// the conversation ID, computes and persists the final status, and runs the
/// hindsight auto-retain hook.
///
/// Runs on the in-process monitor thread spawned by
/// [`launch_background_job_with_timeout`]. It is the only code that reaps
/// the agy child (it always `wait()`s), so the agy process is never left
/// orphaned or un-reaped.
///
/// `handshake` is the atomic state machine with the launcher:
/// - Before spawn: if CANCELLED, agy is not spawned.
/// - After spawn: CAS `PENDING -> RUNNING`. If failed (state is CANCELLED),
///   the spawned child's process group is killed, child reaped, and nothing recorded.
/// - `None` means direct invocation without a launcher (e.g. direct test runs).
pub fn run_internal_runner(
    config: &Config,
    job_id: &str,
    handshake: Option<Arc<AtomicU8>>,
) -> Result<()> {
    // Become the reparenting target for orphans this job's agy leaves
    // behind (e.g. agy's own `cmd &` background children) so they are
    // reaped here instead of leaking to a non-reaping PID 1.
    mark_child_subreaper();

    // Drop the job's jobset-registry entry when this monitor thread exits,
    // whatever the reason (completion, failure, kill, cancelled launch, spawn
    // error). A re-run (retry) re-registers a fresh entry, so the guard only
    // clears *this* attempt's registration.
    let _jobset_drop = crate::core::jobset::JobsetDropGuard::new(job_id);

    execute_test_pre_spawn_hook(job_id);

    let mut meta = JobMetadata::load(&config.state_root, job_id)?;

    // Resolve engine
    let registry = crate::engine::EngineRegistry::with_builtin(config);
    let engine = registry
        .for_job(&meta)
        .map_err(|e| anyhow::anyhow!("failed to resolve engine for job '{}': {e}", meta.job_id))?;

    // Live mode (an A2A mesh job): the process reads newline-delimited
    // JSON from its stdin (`--print "" --input-format stream-json`) instead of
    // taking the prompt on `-p`, and A2A messages are delivered to the running
    // process over that pipe rather than by killing and resuming it.
    let live = config.a2a_live_stdin
        && (crate::core::jobset::host_jobset().is_some()
            || crate::core::a2a_live::live_member(&config.state_root, job_id));

    // Construct TurnInput for the engine
    let prompt_text = if meta.retry_count > 0 {
        engine.normalize_prompt(
            meta.retry_prompt.as_deref().unwrap_or_default(),
            &meta.job_id,
            meta.retry_count,
        )
    } else {
        engine.normalize_prompt(&meta.resolved_prompt, &meta.job_id, 0)
    };

    let turn_input = if meta.retry_count > 0 {
        TurnInput::Resume {
            prompt: &prompt_text,
            conversation: meta.conversation_id.as_deref(),
            live,
        }
    } else {
        TurnInput::Start {
            prompt: &prompt_text,
            live,
        }
    };

    let plan = engine
        .job_command(&meta, turn_input)
        .map_err(|e| anyhow::anyhow!("engine failed to build job command: {e}"))?;

    let stdin_mode = plan.stdin.clone();
    let (cmd, policy_path, jobset_secret) = sandboxed(plan, config, &meta).with_context(|| {
        format!(
            "Failed to build sandboxed command for job '{}'",
            meta.job_id
        )
    })?;

    // Record sandbox info
    let sandbox = crate::sandbox::SandboxInfo::detected(&policy_path);
    if let Err(e) = crate::core::job::set_job_sandbox(&config.state_root, &meta.job_id, &sandbox) {
        eprintln!(
            "agent-run: job '{}': failed to record sandbox info: {}",
            meta.job_id, e
        );
    }
    meta.sandbox_info = Some(sandbox);

    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("Failed to open log file {}", log_path.display()))?;

    // Materialise the instruction-file hierarchy (stream 6, design 6.1/6.2/6.3)
    // before spawn. Jobs have no per-job configuration; the per-project analog
    // is `<sessions-dir>/<project>.md` derived from `meta.project`. FAIL-OPEN:
    // errors are log lines, never a start failure.
    crate::core::instructions::job_materialize(config, &meta, &log_path);

    let err_file = log_file
        .try_clone()
        .context("Failed to clone log file descriptor")?;

    let mut spawn_cmd = cmd;
    spawn_cmd.current_dir(&meta.worktree_path);
    attach_session_and_secret(&mut spawn_cmd, jobset_secret)?;

    let needs_piped_stdin = live || matches!(stdin_mode, StdinMode::Pipe { .. });
    if needs_piped_stdin {
        spawn_cmd.stdin(std::process::Stdio::piped());
    }

    // Cancellation check
    if let Some(ref state) = handshake {
        if state.load(Ordering::SeqCst) == LAUNCH_STATE_CANCELLED {
            append_to_log_file(
                &log_path,
                "[agent-run] launch cancelled: the launch was not confirmed in time, agy was not spawned.",
            );
            return Ok(());
        }
    }

    spawn_cmd.stdout(log_file);
    spawn_cmd.stderr(err_file);
    let mut child = match crate::core::spawn::spawn_command_tracked(&mut spawn_cmd, &engine.id()) {
        Ok(c) => c,
        Err(e) => {
            append_to_log_file(
                &log_path,
                &format!(
                    "Failed to spawn {}: {} (configured path: {})",
                    engine.display_name(),
                    e,
                    engine.configured_program(config).display()
                ),
            );
            bail!(
                "Failed to spawn executable (check that the binary exists and is executable, or check configured path): {}",
                e
            );
        }
    };

    let pid = child.id();

    // Register the pid with the in-server zombie reaper so its background
    // thread never reaps THIS child out from under our `Child::wait` below
    // (it skips rounds while a tracked child is a zombie, and hands back --
    // instead of losing -- an exit it captures in the tiny scan-then-drain
    // TOCTOU window). Untracked orphans (agy's `cmd &` children) are reaped.
    crate::core::reaper::track_child(pid);

    execute_test_post_spawn_hook(job_id);

    // Atomic CAS transition: PENDING -> RUNNING.
    // If this CAS fails, the launcher timed out and set state to CANCELLED.
    // The launcher won: kill agy's process group, reap it, and record nothing.
    if let Some(ref state) = handshake {
        if state
            .compare_exchange(
                LAUNCH_STATE_PENDING,
                LAUNCH_STATE_RUNNING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            abort_cancelled_launch(pid, job_id, &log_path, &mut child);
            return Ok(());
        }
    }

    // Record PID and launch start time immediately so status/tail/kill commands find it with race-free identity
    meta.pid = Some(pid);
    meta.pid_start_time = get_process_start_time(pid);
    let _ = meta.save(&config.state_root);

    // Live mode: hand the pipe write end to the live-channel registry (keyed by
    // the job id, with the process identity) so `JobMessageSink::deliver` can
    // write A2A messages into the RUNNING process, then deliver the initial
    // prompt as the first NDJSON line -- the exact same normalized text agy
    // would have received on `-p`, so conversation-id recovery still matches.
    // The `JobsetDropGuard`-style RAII guard below closes the channel (EOF to
    // agy) on EVERY exit path of this monitor.
    let _live_drop = crate::core::a2a_live::LiveChannelGuard::new(job_id);
    if live {
        if let Some(stdin) = child.stdin.take() {
            crate::core::a2a_live::register(
                job_id,
                pid,
                meta.pid_start_time,
                stdin,
                log_path.clone(),
            );
            // The initial prompt is the same normalized turn prompt computed
            // for `mode`: the start prompt (turn 0) or this attempt's retry
            // prompt. Writing it over stdin is best-effort -- a failure means
            // the pipe broke, which the fallback deliverer turns into an
            // interrupt-and-resume path.
            let initial = prompt_text.clone();
            if !initial.is_empty() {
                // The initial prompt is a normal first user message on the
                // live channel (it is the same NDJSON `user` envelope the
                // deliverer uses), so deliver it through the channel for the
                // same serialization + EOF-safety the A2A path gets.
                if let Some(ch) = crate::core::a2a_live::lookup(job_id) {
                    let _ = ch.deliver_line(&initial);
                    ch.touch();
                }
            }
            // Idle watchdog: after `a2a_live_idle_secs` of no delivery and no
            // log output, close stdin (EOF) so agy finishes the turn and the
            // job classifies Done instead of hanging.
            crate::core::a2a_live::spawn_idle_watchdog(job_id, config.a2a_live_idle_secs);
        }
    }

    // Deliver initial line if specified by StdinMode::Pipe { first_line }
    if let StdinMode::Pipe {
        first_line: Some(line),
    } = &stdin_mode
    {
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            let _ = stdin.write_all(line.as_bytes());
            let _ = stdin.flush();
            let _ = stdin;
        }
    }

    // While the child runs, the final JSON line (and with it the
    // conversation ID) is still in the future; if agy never writes it,
    // recovery would only work AFTER the fact (from `retry`). Recover it
    // up front instead: the prompt sent to agy is the same text that lands
    // in its conversation db, so a background poller can match it while
    // the run is live. The poller is best-effort (never blocks the wait,
    // never fails the job) and is stopped the moment the child exits.
    //
    // The lookup parameters (conversation dir, recovery marker bytes,
    // spawn-time not_before floor) are computed once and reused by BOTH the
    // live poller and the single synchronous post-exit attempt below: the
    // post-exit one must use the spawn-time floor, because a floor
    // recomputed after the child exits would exclude the conversation file
    // on long runs.
    //
    // The marker is the per-turn reference token of the job's stored start
    // prompt (its conversation-creating turn), or -- for rows stored before
    // per-turn tokens existed -- the legacy 80-byte prefix of the stored
    // prompt: [`agy_conversations::recovery_marker_for`] picks between them,
    // so a tokenized job can never be matched through another job's prefix.
    let conv_lookup: Option<(std::path::PathBuf, Vec<u8>, SystemTime)> =
        if meta.conversation_id.is_none() {
            match (
                agy_conversations::recovery_marker_for(&meta.resolved_prompt, &meta.job_id),
                SystemTime::now().checked_sub(Duration::from_secs(5)),
            ) {
                (Some(marker), Some(not_before)) => {
                    agy_conversations::effective_conversations_dir(config.agy_home.as_deref())
                        .map(|conv_dir| (conv_dir, marker, not_before))
                }
                _ => None,
            }
        } else {
            None
        };
    let conversation_poller: Option<(Arc<AtomicBool>, thread::JoinHandle<()>)> =
        match conv_lookup.as_ref() {
            Some((conv_dir, marker, not_before)) => {
                let stop = Arc::new(AtomicBool::new(false));
                let stop_flag = Arc::clone(&stop);
                let state_root = config.state_root.clone();
                let job_id = meta.job_id.clone();
                let conv_dir = conv_dir.clone();
                let marker = marker.clone();
                let not_before = *not_before;
                thread::Builder::new()
                    .name(format!("agent-run-conv-poll-{}", job_id))
                    .spawn(move || {
                        poll_conversation_id(
                            conv_dir, marker, not_before, state_root, job_id, &stop_flag,
                        )
                    })
                    .ok()
                    .map(|handle| (stop, handle))
            }
            None => None,
        };

    // Wait for the process to exit. Normally this reaps it; if the in-server
    // zombie reaper won the race for a tracked pid (its tiny scan-then-drain
    // TOCTOU window), `wait` returns ECHILD and we take the exit status back
    // from the reaper's stash instead.
    let exit_status = match child.wait() {
        Ok(s) => s,
        Err(e) if e.raw_os_error() == Some(libc::ECHILD) => {
            crate::core::reaper::untrack_child(pid);
            crate::core::reaper::wait_stashed_exit(pid, Duration::from_secs(10))
                .map(|reaped| reaped.into_exit_status())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "child {pid} was reaped by the concurrent zombie reaper before its exit status was recorded"
                    )
                })?
        }
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to wait on child process: {e}"));
        }
    };
    // Never keep polling after the child is gone: the run no longer writes
    // to its conversation db, and a late match could belong to a newer
    // attempt on the same row. The flag also bounds the thread's lifetime
    // if the join itself is skipped; the poller is panic-free.
    if let Some((stop, handle)) = conversation_poller {
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = handle.join();
    }
    // A signal death (TERM=143, KILL=137, ...) is recorded as 128+signal so
    // the job row matches the crate's 137/143 taxonomy and status
    // classification; a plain -1 would land in "no exit code" status logic.
    let exit_code = exit_code_from_status(exit_status);
    // Best-effort reap of any OTHER processes reparented to us during the
    // run: as a child subreaper (see `mark_child_subreaper`), orphans left
    // behind by agy (its own background children) reparent here instead of
    // to PID 1. Non-blocking drain of everything that has already exited
    // by now; anything still running is left behind and reparents to the
    // next reaping ancestor when this monitor exits.
    drain_orphans();
    // The monitor no longer owns this pid: from here on the reaper may reap
    // anything (idempotent if the ECHILD arm above already untracked it).
    crate::core::reaper::untrack_child(pid);

    // Update metadata with final results
    meta.exit_code = Some(exit_code);

    // Try extracting conversation ID from the log
    if let Some(conv_id) = extract_conversation_id_from_log(&log_path) {
        meta.conversation_id = Some(conv_id);
    }

    // The poller was stopped the moment the child exited: for a fast (or
    // killed) agy its first check may never have run, and no other code
    // recovers the id when the final JSON line is never written. Make ONE
    // synchronous recovery attempt before the final status row is written,
    // using the same marker/not_before the poller used. Bounded work,
    // best-effort: it can never panic or block the shutdown, and errors are
    // ignored -- an ambiguous or absent match leaves the id as None, which
    // is the same conservative outcome the poller produces.
    if meta.conversation_id.is_none() {
        if let Some(recovered) = conv_lookup.as_ref().and_then(|lookup| {
            attempt_conversation_id_recovery(
                &config.state_root,
                &meta.job_id,
                &log_path,
                &lookup.0,
                &lookup.1,
                lookup.2,
            )
        }) {
            meta.conversation_id = Some(recovered);
        }
    }

    // Compute the final status from the runner's own view of the job, then
    // persist it with a conditional update that is a no-op if `agent-run kill`
    // already wrote `killed` to the same row. The single-row conditional
    // UPDATE closes the TOCTOU race of a re-read-then-unconditional-save
    // atomically in the database.
    let computed_status = compute_status(&meta, &config.state_root);
    let applied = match save_final_status_unless_killed(
        &config.state_root,
        &meta.job_id,
        computed_status,
        Some(exit_code),
        meta.conversation_id.as_deref(),
        // This monitor belongs to attempt `meta.retry_count`; if `agent-run
        // retry` has since started a newer attempt on the same row, this
        // (late) write must not clobber it.
        Some(meta.retry_count),
    ) {
        Ok(applied) => applied,
        Err(e) => {
            // A real DB error (I/O, lock timeout, ...) is not the same as
            // "row already carries killed" -- treat it as unapplied, but keep
            // the real cause visible.
            eprintln!(
                "Warning: failed to save final status for job '{}': {}",
                meta.job_id, e
            );
            false
        }
    };
    // If the update was skipped, the row already carries `killed` (or a newer
    // retry attempt owns it) -- keep the in-memory status consistent with
    // what is on disk for the code below (retain gating, log messages). A
    // superseded attempt must never retain findings, which `Killed` ensures.
    let final_status = if applied {
        computed_status
    } else {
        crate::core::job::JobStatus::Killed
    };
    meta.status = Some(final_status);

    // Auto-retain hook: only if hindsight enabled, auto-retain on, and job reached Done
    let hindsight_enabled = meta.hindsight.unwrap_or(config.hindsight_enabled);
    let auto_retain = meta.auto_retain.unwrap_or(config.hindsight_auto_retain);
    if should_retain(hindsight_enabled, auto_retain, &final_status) {
        let bank_opt = meta
            .hindsight_bank
            .as_deref()
            .or(if !meta.project.is_empty() {
                Some(meta.project.as_str())
            } else {
                None
            })
            .or(config.hindsight_bank.as_deref());
        if let Some(bank) = bank_opt {
            let log_content = match std::fs::read(&log_path) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(_) => String::new(),
            };
            let finding = build_retain_finding(&meta.job_id, &meta.project, &log_content);
            let context = build_retain_context(&meta.job_id, &meta.project, &log_content);
            match execute_retain(
                &config.hindsight_path,
                bank,
                &finding,
                config.hindsight_tag.as_deref(),
                &context,
                config.hindsight_url.as_deref(),
                config.hindsight_api_key.as_deref(),
                config.hindsight_profile.as_deref(),
                Some(config),
                meta.proxy.as_deref(),
            ) {
                Ok(()) => {
                    append_to_log_file(
                        &log_path,
                        &format!(
                            "[hindsight] Retained finding into memory bank '{}' successfully.",
                            bank
                        ),
                    );
                }
                Err(e) => {
                    append_to_log_file(
                        &log_path,
                        &format!("[hindsight warning] Retain failed: {}", e),
                    );
                }
            }
        } else {
            append_to_log_file(
                &log_path,
                "[hindsight warning] Retain skipped: hindsight enabled but no bank configured.",
            );
        }
    }

    Ok(())
}

/// Performs ONE conversation-ID recovery attempt, exactly the lookup +
/// persist pass the poller ([`poll_conversation_id`]) runs on every tick,
/// factored out so the logic exists in one place. Intended for the single
/// synchronous post-exit fallback in [`run_internal_runner`]: it must be
/// safe to run inline on the monitor thread -- no threads, no sleeps, no
/// retries, and a fixed cost of at most one state read plus one read-only
/// scan of the conversation directory.
///
/// Returns `Some(id)` only when the job's row still has no conversation id
/// AND the scan found exactly one unambiguous conversation; in that case the
/// id is persisted with the same targeted UPDATE the poller uses
/// (`conversation_id IS NULL` guard). `None` -- zero matches, several
/// ambiguous matches, a row that already has an id, an unresolvable marker /
/// directory / spawn window, or any I/O or database error -- means "skip";
/// the caller leaves the row's id untouched and continues the normal final
/// save.
///
/// `marker` is the precomputed recovery bytes for the job's stored start
/// prompt (the per-turn reference token, or the legacy 80-byte prefix for
/// rows from before the token existed -- see
/// [`agy_conversations::recovery_marker_for`]).
fn attempt_conversation_id_recovery(
    state_root: &std::path::Path,
    job_id: &str,
    log_path: &std::path::Path,
    conv_dir: &std::path::Path,
    marker: &[u8],
    not_before: SystemTime,
) -> Option<String> {
    if marker.is_empty() {
        return None;
    }
    let Ok(cur) = JobMetadata::load(state_root, job_id) else {
        return None;
    };
    if cur.conversation_id.is_some() {
        return None;
    }
    let claimed = agy_conversations::claimed_conversation_ids(state_root);
    let conv_id = agy_conversations::find_conversation_id(conv_dir, marker, not_before, &claimed)?;
    let stored = match restore_conversation_id(state_root, job_id, &conv_id) {
        Ok(stored) => stored,
        Err(e) => {
            // Visible on the server's stderr, mirroring how the monitor
            // reports its own DB failures.
            eprintln!(
                "Warning: failed to persist recovered conversation ID '{}' for job '{}': {}",
                conv_id, job_id, e
            );
            false
        }
    };
    if stored {
        append_to_log_file(
            log_path,
            &format!(
                "[agy] Recovered conversation ID '{}' from agy's conversation files.",
                conv_id
            ),
        );
    }
    stored.then_some(conv_id)
}

/// The conversation-ID recovery poller for a live agy child (spawned by
/// [`run_internal_runner`]).
///
/// Each tick runs the shared [`attempt_conversation_id_recovery`] pass
/// (a `None` return is a silent skip, as in the post-exit fallback). The
/// first check runs immediately (before the first sleep); it then
/// re-checks every 3 s for the first 2 minutes and every 15 s afterwards.
/// Every step is best-effort: a failed state read, an unopenable database,
/// or a failed update is skipped and the loop continues. The thread never
/// panics and exits as soon as `stop` is set (after at most one remaining
/// sleep), so it can never wedge or outlive the monitor's wait loop.
fn poll_conversation_id(
    conv_dir: std::path::PathBuf,
    marker: Vec<u8>,
    not_before: SystemTime,
    state_root: std::path::PathBuf,
    job_id: String,
    stop: &AtomicBool,
) {
    if marker.is_empty() {
        return;
    }
    let start = Instant::now();
    loop {
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let log_path = JobMetadata::log_path(&state_root, &job_id);
        attempt_conversation_id_recovery(
            &state_root,
            &job_id,
            &log_path,
            &conv_dir,
            &marker,
            not_before,
        );
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let interval = if start.elapsed() < Duration::from_secs(120) {
            Duration::from_secs(3)
        } else {
            Duration::from_secs(15)
        };
        thread::sleep(interval);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn job_project_repo_resolves_only_the_projects_own_bare_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let repos = tmp.path().join("repos");
        let repo = repos.join("proj.git");
        std::fs::create_dir_all(repo.join("worktrees").join("j1")).unwrap();
        let wt = tmp.path().join("jobs").join("j1");
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(
            wt.join(".git"),
            format!("gitdir: {}\n", repo.join("worktrees").join("j1").display()),
        )
        .unwrap();
        let got = job_project_repo(&wt, &repos).unwrap();
        assert_eq!(got, std::fs::canonicalize(&repo).unwrap());
        // A pointer outside repos_root yields no grant.
        let other = tmp.path().join("elsewhere");
        std::fs::create_dir_all(other.join("x.git").join("worktrees").join("j2")).unwrap();
        let wt2 = tmp.path().join("jobs").join("j2");
        std::fs::create_dir_all(&wt2).unwrap();
        std::fs::write(
            wt2.join(".git"),
            format!(
                "gitdir: {}\n",
                other.join("x.git").join("worktrees").join("j2").display()
            ),
        )
        .unwrap();
        assert!(job_project_repo(&wt2, &repos).is_none());
        // A self-contained worktree (.git directory) yields none.
        let wt3 = tmp.path().join("jobs").join("j3");
        std::fs::create_dir_all(wt3.join(".git")).unwrap();
        assert!(job_project_repo(&wt3, &repos).is_none());
    }

    use super::*;
    use crate::engine::{EngineRegistry, StdinMode, TurnInput};
    use chrono::Utc;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::Stdio;
    use tempfile::tempdir;

    fn test_meta_overrides() -> JobMetadata {
        JobMetadata {
            job_id: "test1234".to_string(),
            project: "project-a".to_string(),
            prompt: "Test prompt".to_string(),
            resolved_prompt: "Test prompt".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: PathBuf::from("/workspace/project-a/.agent-jobs/test1234"),
            branch: "agent-run/test1234".to_string(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: None,
            status: None,
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        }
    }

    #[test]
    fn test_resolve_prompt_literal() {
        let tmp = tempdir().unwrap();
        assert_eq!(
            resolve_prompt("hello world", tmp.path()).unwrap(),
            "hello world"
        );
    }

    #[test]
    fn test_resolve_prompt_relative_file() {
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("prompt.txt");
        fs::write(&p, "file prompt body").unwrap();
        assert_eq!(
            resolve_prompt("@prompt.txt", tmp.path()).unwrap(),
            "file prompt body"
        );
    }

    #[test]
    fn test_resolve_prompt_file_content_verbatim() {
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("p.md");
        fs::write(&p, "# Heading\nline two\n").unwrap();
        assert_eq!(
            resolve_prompt("@p.md", tmp.path()).unwrap(),
            "# Heading\nline two\n"
        );
    }

    #[test]
    fn test_build_command_bare_agy_args() {
        let config = Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        meta.model = Some("gemini-pro".to_string());
        meta.effort = Some("high".to_string());
        meta.sandbox = true;
        meta.add_dirs = vec![PathBuf::from("/extra/dir")];
        meta.print_timeout = Some("30s".to_string());

        // `run_internal_runner` hands agy the prompt normalized with the
        // job's turn-0 reference line; the -p element carries exactly that.
        let start_prompt = agy_conversations::normalize_ref_prompt("Test prompt", &meta.job_id, 0);
        let mode = InvocationMode::Start {
            resolved_prompt: start_prompt.clone(),
        };

        let (prog, args) = build_command_and_args(&config, &meta, &mode, false);

        assert_eq!(prog, PathBuf::from("/usr/local/bin/agy"));
        assert!(args.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(args.contains(&"--mode".to_string()));
        assert!(args.contains(&"accept-edits".to_string()));
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"-p".to_string()));
        assert!(args.contains(&start_prompt));
        assert!(args.contains(&format!("Test prompt\n\n[agent-run ref {}#0]", meta.job_id)));
        assert!(args.contains(&"--model".to_string()));
        assert!(args.contains(&"gemini-pro".to_string()));
        assert!(args.contains(&"--effort".to_string()));
        assert!(args.contains(&"high".to_string()));
        assert!(args.contains(&"--sandbox".to_string()));
        assert!(args.contains(&"--add-dir".to_string()));
        assert!(args.contains(&"/workspace/project-a/.agent-jobs/test1234".to_string()));
        assert!(args.contains(&"/extra/dir".to_string()));
        assert!(args.contains(&"--print-timeout".to_string()));
        assert!(args.contains(&"30s".to_string()));
        // No privilege-separation remnants may leak into the command.
        assert!(!args.iter().any(|a| a.contains("env") || a == "gosu"));
    }

    #[test]
    fn test_build_command_live_agy_args() {
        // A live (meshed) job must invoke `agy --print "" --input-format
        // stream-json ...` (prompt travels over stdin, not `-p`), with every
        // other element identical to the non-live invocation.
        let config = Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        meta.model = Some("gemini-pro".to_string());
        meta.sandbox = true;
        let mode = InvocationMode::Start {
            resolved_prompt: "Test prompt".to_string(),
        };

        let (prog, args) = build_command_and_args(&config, &meta, &mode, true);

        assert_eq!(prog, PathBuf::from("/usr/local/bin/agy"));
        assert!(args.contains(&"--print".to_string()));
        // The empty-string prompt placeholder agy requires in live mode.
        assert!(args.contains(&"".to_string()));
        assert!(args.contains(&"--input-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        // The prompt itself must NOT travel as a -p argv element in live mode.
        assert!(!args.contains(&"Test prompt".to_string()));
        assert!(!args.contains(&"-p".to_string()));
        assert!(args.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(args.contains(&"--mode".to_string()));
        assert!(args.contains(&"accept-edits".to_string()));
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"--model".to_string()));
        assert!(args.contains(&"gemini-pro".to_string()));
        assert!(args.contains(&"--sandbox".to_string()));
        assert!(args.contains(&"--add-dir".to_string()));
        assert!(args.contains(&"/workspace/project-a/.agent-jobs/test1234".to_string()));
    }

    /// The claude job argv (HL-91 E5) must use the REAL `claude` CLI flags
    /// only. The live finding (claude 2.1.322 in the worker pod): the agy
    /// `--mode` flag made every claude job fail immediately with
    /// `error: unknown option '--mode'`. Plan mode maps to
    /// `--permission-mode plan`, normal mode passes no `--permission-mode`,
    /// and `--verbose` is required for `-p --output-format stream-json`.
    #[test]
    fn test_build_command_claude_args() {
        let config = Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            claude_path: PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        meta.engine = Some("claude".to_string());

        // A retry resumes the harness conversation via --resume.
        meta.conversation_id = Some("conv-uuid-999".to_string());
        meta.retry_count = 1;
        let mode = InvocationMode::Retry {
            conversation_id: Some("conv-uuid-999".to_string()),
            prompt: Some("retry prompt".to_string()),
        };

        // Normal mode (the job mode string is not `plan`).
        let (_, args) = build_command_and_args(&config, &meta, &mode, false);
        assert!(args.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(args.contains(&"-p".to_string()));
        assert!(args.contains(&"--input-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"--verbose".to_string()));
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"conv-uuid-999".to_string()));
        // The prompt travels as a stdin NDJSON line, never as an argv element.
        assert!(!args.contains(&"retry prompt".to_string()));
        // Normal mode: auto-approved by --dangerously-skip-permissions, so NO
        // --permission-mode flag at all.
        assert!(!args.contains(&"--permission-mode".to_string()));
        // No model configured -> no --model flag (test meta has model: None).
        assert!(!args.contains(&"--model".to_string()));
        // With a model set, the claude argv passes it through as
        // `--model <name>`, exactly like the agy builder.
        meta.model = Some("sonnet".to_string());
        let (_, args_model) = build_command_and_args(&config, &meta, &mode, false);
        let m_idx = args_model.iter().position(|a| a == "--model").unwrap();
        assert_eq!(args_model[m_idx + 1], "sonnet");

        // Plan mode: --permission-mode plan.
        let mut plan_meta = test_meta_overrides();
        plan_meta.engine = Some("claude".to_string());
        plan_meta.mode = "plan".to_string();
        let start_mode = InvocationMode::Start {
            resolved_prompt: "plan prompt".to_string(),
        };
        let (prog, plan_args) = build_command_and_args(&config, &plan_meta, &start_mode, false);
        assert_eq!(prog, PathBuf::from("/usr/local/bin/claude"));
        assert!(plan_args.contains(&"--permission-mode".to_string()));
        let idx = plan_args
            .iter()
            .position(|a| a == "--permission-mode")
            .unwrap();
        assert_eq!(plan_args[idx + 1], "plan");
        // A start attempt has no --resume yet.
        assert!(!plan_args.contains(&"--resume".to_string()));
    }

    /// Regression guard for the live `unknown option '--mode'` failure: no
    /// claude job argv element may ever start with `--mode` (that flag is
    /// agy-specific; the claude CLI's mode knob is `--permission-mode`).
    #[test]
    fn test_claude_args_never_carry_mode_flag() {
        let config = Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            claude_path: PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        let mut meta = test_meta_overrides();
        meta.engine = Some("claude".to_string());
        meta.conversation_id = Some("conv-1".to_string());
        meta.retry_count = 1;

        for job_mode in ["normal", "plan", "accept-edits"] {
            meta.mode = job_mode.to_string();
            for invocation in [
                InvocationMode::Start {
                    resolved_prompt: "p".to_string(),
                },
                InvocationMode::Retry {
                    conversation_id: Some("conv-1".to_string()),
                    prompt: Some("p".to_string()),
                },
            ] {
                for live in [false, true] {
                    let (_, args) = build_command_and_args(&config, &meta, &invocation, live);
                    assert!(
                        !args.iter().any(|a| a.starts_with("--mode")),
                        "claude job argv must not carry the agy-only --mode flag (mode={job_mode}, live={live}): {args:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn apply_a2a_self_name_sets_per_job_identity() {
        // Serialize env mutation.
        let _g = crate::test_env_lock();
        // Save/restore the env vars this test touches.
        let saved = ["A2A_SELF_PREFIX", "A2A_SELF_NAME", "HOME"]
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect::<Vec<_>>();

        let self_name_of = |c: &Command| -> Option<String> {
            c.get_envs()
                .find(|(k, _)| *k == std::ffi::OsStr::new("A2A_SELF_NAME"))
                .and_then(|(_, v)| v.map(|v| v.to_string_lossy().into_owned()))
        };

        // Explicit prefix wins.
        std::env::set_var("A2A_SELF_PREFIX", "kubeadp-agent-worker-service");
        std::env::remove_var("A2A_SELF_NAME");
        let mut c = Command::new("true");
        apply_a2a_self_name(&mut c, "job-abc");
        assert_eq!(
            self_name_of(&c).as_deref(),
            Some("kubeadp-agent-worker-service/job-abc")
        );
        std::env::set_var("A2A_SELF_NAME", "kubeadp-agent-worker-service");
        let mut c = Command::new("true");
        apply_a2a_self_name(&mut c, "job-abc");
        assert_eq!(
            self_name_of(&c).as_deref(),
            Some("kubeadp-agent-worker-service/job-abc")
        );

        // No prefix: the inherited A2A_SELF_NAME's first segment is used.
        std::env::remove_var("A2A_SELF_PREFIX");
        std::env::set_var("A2A_SELF_NAME", "kubeadp-agent-worker-service");
        let mut c = Command::new("true");
        apply_a2a_self_name(&mut c, "job-abc");
        assert_eq!(
            self_name_of(&c).as_deref(),
            Some("kubeadp-agent-worker-service/job-abc")
        );

        for (k, v) in saved {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn test_retry_mode_args() {
        let config = Config::default();
        let mut meta = test_meta_overrides();
        meta.conversation_id = Some("conv-uuid-999".to_string());
        meta.retry_count = 1;

        // Retry with conversation ID
        let mode1 = InvocationMode::Retry {
            conversation_id: Some("conv-uuid-999".to_string()),
            prompt: None,
        };
        let (_, args1) = build_command_and_args(&config, &meta, &mode1, false);
        assert!(args1.contains(&"--conversation".to_string()));
        assert!(args1.contains(&"conv-uuid-999".to_string()));
        assert!(args1.contains(&"--output-format".to_string()));
        assert!(args1.contains(&"stream-json".to_string()));
        assert!(!args1.contains(&"-p".to_string()));

        // Retry without conversation ID -> --continue
        let mode2 = InvocationMode::Retry {
            conversation_id: None,
            prompt: None,
        };
        let (_, args2) = build_command_and_args(&config, &meta, &mode2, false);
        assert!(args2.contains(&"--continue".to_string()));
        assert!(!args2.contains(&"--conversation".to_string()));
        assert!(!args2.contains(&"-p".to_string()));

        // Retry with recalled prompt -> passes -p, normalized with this
        // attempt's reference line (meta.retry_count is the turn number).
        let recalled = "<recalled-context>\nxyz\n</recalled-context>\n\nOld prompt";
        let mode3 = InvocationMode::Retry {
            conversation_id: Some("conv-uuid-999".to_string()),
            prompt: Some(agy_conversations::normalize_ref_prompt(
                recalled,
                &meta.job_id,
                meta.retry_count,
            )),
        };
        let (_, args3) = build_command_and_args(&config, &meta, &mode3, false);
        assert!(args3.contains(&"--conversation".to_string()));
        assert!(args3.contains(&"-p".to_string()));
        assert!(args3.contains(&format!(
            "{recalled}\n\n[agent-run ref {}#{}]",
            meta.job_id, meta.retry_count
        )));
    }

    #[test]
    fn test_worktree_path_passed_as_first_add_dir() {
        let config = Config::default();

        // 1. When meta.add_dirs is empty: worktree_path is passed as --add-dir
        let mut meta = test_meta_overrides();
        meta.job_id = "test_add_dir".to_string();
        meta.worktree_path = PathBuf::from("/workspace/project-a/.agent-jobs/test_add_dir");

        let mode = InvocationMode::Start {
            resolved_prompt: "Test".to_string(),
        };

        let (_, args_empty) = build_command_and_args(&config, &meta, &mode, false);
        let add_dir_indices: Vec<usize> = args_empty
            .iter()
            .enumerate()
            .filter(|(_, a)| *a == "--add-dir")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(add_dir_indices.len(), 1);
        assert_eq!(
            args_empty[add_dir_indices[0] + 1],
            "/workspace/project-a/.agent-jobs/test_add_dir"
        );

        // 2. When meta.add_dirs has items: worktree_path is passed FIRST unconditionally
        meta.add_dirs = vec![
            PathBuf::from("/workspace/extra1"),
            PathBuf::from("/workspace/extra2"),
        ];

        let (_, args_multi) = build_command_and_args(&config, &meta, &mode, false);
        let add_dir_indices_multi: Vec<usize> = args_multi
            .iter()
            .enumerate()
            .filter(|(_, a)| *a == "--add-dir")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(add_dir_indices_multi.len(), 3);
        assert_eq!(
            args_multi[add_dir_indices_multi[0] + 1],
            "/workspace/project-a/.agent-jobs/test_add_dir"
        );
        assert_eq!(
            args_multi[add_dir_indices_multi[1] + 1],
            "/workspace/extra1"
        );
        assert_eq!(
            args_multi[add_dir_indices_multi[2] + 1],
            "/workspace/extra2"
        );
    }

    #[test]
    fn test_validate_executable_paths_missing_agy() {
        let config = Config {
            agy_path: PathBuf::from("/nonexistent/agy-binary"),
            claude_path: std::path::PathBuf::from("claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        assert!(validate_executable_paths(&config).is_err());

        // A relative agy path is resolved at spawn time and is not validated.
        let rel_config = Config {
            agy_path: PathBuf::from("agy"),
            claude_path: std::path::PathBuf::from("claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        assert!(validate_executable_paths(&rel_config).is_ok());

        // An absolute path that exists passes validation.
        let ok_config = Config {
            agy_path: PathBuf::from("/bin/sh"),
            claude_path: std::path::PathBuf::from("claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        assert!(validate_executable_paths(&ok_config).is_ok());

        // A claude job checks claude_path, not agy_path.
        let claude_only_config = Config {
            agy_path: PathBuf::from("/nonexistent/agy-binary"),
            claude_path: PathBuf::from("/bin/sh"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        assert!(validate_engine_executable_path(&claude_only_config, Some("claude")).is_ok());
        assert!(validate_engine_executable_path(&config, Some("claude")).is_ok());
        let bad_claude_config = Config {
            agy_path: PathBuf::from("/bin/sh"),
            claude_path: PathBuf::from("/nonexistent/claude-binary"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        assert!(validate_engine_executable_path(&bad_claude_config, Some("claude")).is_err());
    }

    #[test]
    fn test_launch_background_job_in_process_thread() {
        // Spawns a real child (fake agy); take the process-child test lock so
        // it never races the zombie reaper tests (a tracked-zombie window
        // could otherwise make `reap_round` skip).
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        // Fake agy: prints a structured SUCCESS line and exits 0.
        let agy = tmp.path().join("fake-agy");
        fs::write(
            &agy,
            "#!/bin/sh\necho '{\"status\":\"SUCCESS\",\"response\":\"in-process-ok\",\"conversation_id\":\"conv-ipc-1\"}'\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy.clone(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        let job_id = format!("ipc{}", std::process::id());
        meta.job_id = job_id.clone();
        // worktree_path just needs to exist as the child's cwd
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        let pid = launch_background_job(&config, &mut meta).expect("launch should confirm a PID");
        assert!(pid > 1, "expected a real child PID");
        assert_eq!(meta.pid, Some(pid));

        // Wait for the monitor thread to finish the job (fake agy exits fast).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let updated = JobMetadata::load(&state_root, &job_id).unwrap();
            if updated.exit_code.is_some() {
                assert_eq!(updated.exit_code, Some(0));
                assert_eq!(updated.status, Some(crate::core::job::JobStatus::Done));
                assert_eq!(updated.conversation_id.as_deref(), Some("conv-ipc-1"));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "job did not finish in time"
            );
            thread::sleep(Duration::from_millis(50));
        }

        // The fake agy's output must be in the job log.
        let log_path = JobMetadata::log_path(&state_root, &job_id);
        let log = fs::read_to_string(&log_path).unwrap();
        assert!(
            log.contains("in-process-ok"),
            "log should contain agy output: {log}"
        );
    }

    #[test]
    fn test_runner_recovers_conversation_id_while_child_runs() {
        // Spawns a real child (fake agy); take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // The log never carries a conversation id (agy prints a plain
        // SUCCESS line without one); the monitor's background poller must
        // recover the ID from agy's conversation files while the child
        // runs, and persist it to the row.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();
        let home = tmp.path().join("agy-home");

        // Fake agy: SUCCESS but NO conversation_id in the JSON, so the only
        // way the row can get one is the poller.
        let agy = tmp.path().join("fake-agy");
        fs::write(
            &agy,
            "#!/bin/sh\necho '{\"status\":\"SUCCESS\",\"response\":\"no-conv-id-line\"}'\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            agy_home: Some(home.clone()),
            ..Default::default()
        };

        let recovered = "c0ffee00-1234-5678-9abc-def012345678";
        let job_id = format!("ipcrc{}", std::process::id());
        let prompt = "runner conv recovery marker-xyz-742 prompt";
        let conv_dir = home
            .join(".gemini")
            .join("antigravity-cli")
            .join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        // The stored (tokenized) prompt is what agy ran with: its bytes,
        // including the job's reference line, land in the step payload.
        let stored = agy_conversations::normalize_ref_prompt(prompt, &job_id, 0);
        let db = rusqlite::Connection::open(conv_dir.join(format!("{recovered}.db"))).unwrap();
        db.execute_batch("CREATE TABLE steps (idx INTEGER, step_payload BLOB);")
            .unwrap();
        db.execute(
            "INSERT INTO steps (idx, step_payload) VALUES (1, ?1)",
            rusqlite::params![format!("protobuf framing around {stored}")],
        )
        .unwrap();

        let mut meta = test_meta_overrides();
        meta.resolved_prompt = stored;
        meta.job_id = job_id.clone();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        crate::core::job::ensure_state_store(&state_root).unwrap();
        meta.save(&state_root).unwrap();

        let pid = launch_background_job(&config, &mut meta).expect("launch should confirm a PID");
        assert!(pid > 1);

        // Wait for the monitor to finalize the job; the poller's first check
        // happens before its first sleep, so the ID should already be on the
        // row by the time the fast fake agy exits.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let updated = JobMetadata::load(&state_root, &job_id).unwrap();
            if updated.exit_code.is_some() {
                assert_eq!(updated.exit_code, Some(0));
                assert_eq!(
                    updated.conversation_id.as_deref(),
                    Some(recovered),
                    "the poller must have persisted the recovered conversation id"
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "job did not finish in time"
            );
            thread::sleep(Duration::from_millis(50));
        }
        // The recovery is also surfaced in the job log for `agent-run tail`.
        let log = fs::read_to_string(JobMetadata::log_path(&state_root, &job_id)).unwrap();
        assert!(
            log.contains("Recovered conversation ID"),
            "recovery note should be in the log, got: {log}"
        );
    }

    #[test]
    fn test_runner_recovers_nothing_when_marker_is_ambiguous() {
        // Spawns a real child (fake agy); take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // Same setup as `test_runner_recovers_conversation_id_while_child_runs`
        // (fast fake agy, no conversation id in the log) EXCEPT that the
        // prompt marker matches TWO conversation databases. Recovery must
        // be conservative: neither the poller nor the post-exit fallback may
        // guess, so the row must end with conversation_id None.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();
        let home = tmp.path().join("agy-home");

        // Fake agy: SUCCESS but NO conversation_id in the JSON (same as the
        // single-match test).
        let agy = tmp.path().join("fake-agy");
        fs::write(
            &agy,
            "#!/bin/sh\necho '{\"status\":\"SUCCESS\",\"response\":\"no-conv-id-line\"}'\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            agy_home: Some(home.clone()),
            ..Default::default()
        };

        let prompt = "runner ambiguous recovery marker-xyz-743 prompt";
        let job_id = format!("ipcamb{}", std::process::id());
        let conv_dir = home
            .join(".gemini")
            .join("antigravity-cli")
            .join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        // The job's prompt is stored tokenized, so its recovery marker is the
        // job's own reference line. Two UUID-named conversation databases,
        // both created "now" and both embedding the same token text (as if
        // two agy runs were handed the identical prompt) -> the token
        // match is ambiguous and must be refused.
        let stored = agy_conversations::normalize_ref_prompt(prompt, &job_id, 0);
        for stem in [
            "a0000000-1111-5678-9aaa-def012345678",
            "b0000000-2222-5678-9bbb-def012345678",
        ] {
            let db = rusqlite::Connection::open(conv_dir.join(format!("{stem}.db"))).unwrap();
            db.execute_batch("CREATE TABLE steps (idx INTEGER, step_payload BLOB);")
                .unwrap();
            db.execute(
                "INSERT INTO steps (idx, step_payload) VALUES (1, ?1)",
                rusqlite::params![format!("protobuf framing around {stored}")],
            )
            .unwrap();
        }

        let mut meta = test_meta_overrides();
        meta.resolved_prompt = stored;
        meta.job_id = job_id.clone();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        crate::core::job::ensure_state_store(&state_root).unwrap();
        meta.save(&state_root).unwrap();

        let pid = launch_background_job(&config, &mut meta).expect("launch should confirm a PID");
        assert!(pid > 1);

        // Wait for the monitor to finalize the job: the row must END with no
        // conversation id, even though the marker exists in two DBs.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let updated = JobMetadata::load(&state_root, &job_id).unwrap();
            if updated.exit_code.is_some() {
                assert_eq!(updated.exit_code, Some(0));
                assert_eq!(
                    updated.conversation_id, None,
                    "an ambiguous match (two candidate DBs) must not be adopted"
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "job did not finish in time"
            );
            thread::sleep(Duration::from_millis(50));
        }
        // No recovery note may have been written: recovery never guessed.
        let log = fs::read_to_string(JobMetadata::log_path(&state_root, &job_id)).unwrap();
        assert!(
            !log.contains("Recovered conversation ID"),
            "no recovery note may be written for an ambiguous match, got: {log}"
        );
    }

    #[test]
    fn test_launch_background_job_bails_when_agy_unspawnable() {
        // Attempts a real spawn; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        // agy path exists but is not executable -> the launcher refuses it.
        // Every job now goes through the `sandbox-exec` wrapper, which always
        // spawns successfully, so an unrunnable agy would otherwise surface
        // only as the wrapper's exit code (126) with the wrapper's PID already
        // confirmed as a "successful" launch. The launcher therefore rejects
        // the binary up front, before any job row, worktree or process exists.
        let agy = tmp.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o644)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy.clone(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        let job_id = format!("bad{}", std::process::id());
        meta.job_id = job_id.clone();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        let err = launch_background_job_with_timeout(
            &config,
            &mut meta,
            Duration::from_millis(1500),
            Duration::from_millis(25),
        )
        .expect_err("unspawnable agy must make the launch bail");
        assert!(
            err.to_string().contains("not executable"),
            "the unrunnable agy must be refused by the launcher: {err}"
        );

        // The bail is pre-launch: no job row, no confirmed PID and no process
        // may have been left behind.
        assert!(
            JobMetadata::load(&state_root, &job_id).is_err(),
            "no job record may be written when the agy binary is not executable"
        );
        assert!(
            meta.pid.is_none(),
            "no PID may be confirmed for an unrunnable agy"
        );
    }

    #[test]
    fn test_detached_spawn_becomes_own_process_group_leader() {
        // Spawns and kills a real child; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // The agy child is spawned with setsid in the child. Verify the
        // mechanism the `agent-run kill` process-group signalling relies on:
        // the child must be the leader of its own new session/process group.
        let mut cmd = Command::new("sleep");
        cmd.arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        detached_session(&mut cmd);
        let mut child = cmd.spawn().expect("spawn sleep");
        let pid = child.id() as i32;

        let pgid = unsafe { libc::getpgid(pid) };
        assert_eq!(
            pgid, pid,
            "detached child must lead its own process group (pgid == pid) so kill(-pid) reaches it and its descendants; got pgid={pgid} for pid={pid}"
        );
        // The session id must also be its own pid (new session via setsid).
        let sid = unsafe { libc::getsid(pid) };
        assert_eq!(
            sid, pid,
            "detached child must be a session leader (sid == pid); got sid={sid} for pid={pid}"
        );

        // And the process-group signal must actually reach it.
        assert_eq!(unsafe { libc::kill(-pid, libc::SIGTERM) }, 0);
        let status = child.wait().expect("wait for child");
        assert!(
            !status.success(),
            "child should be terminated by SIGTERM to its process group: {status}"
        );
    }

    #[test]
    fn test_mark_child_subreaper_sets_subreaper_flag() {
        mark_child_subreaper();
        // PR_GET_CHILD_SUBREAPER must now report the flag as set.
        let mut val: libc::c_ulong = 0;
        // SAFETY: `val` is a valid, in-bounds c_ulong for prctl to write.
        let rc = unsafe {
            libc::prctl(
                libc::PR_GET_CHILD_SUBREAPER,
                &mut val as *mut libc::c_ulong as libc::c_ulong,
                0,
                0,
                0,
            )
        };
        assert_eq!(rc, 0, "prctl(PR_GET_CHILD_SUBREAPER) failed");
        assert_eq!(val, 1, "process must be marked as a child subreaper");
    }

    #[test]
    fn test_reparented_orphan_is_reaped_by_subreaper() {
        // Spawns real children; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // End-to-end: mark THIS test process a subreaper, spawn a child
        // that backgrounds a grandchild and exits, then confirm the
        // orphaned grandchild reparents here (not to PID 1) and is
        // reaped by the same non-blocking waitpid(pid, WNOHANG) drain the
        // monitor uses after its primary child.wait(). The drain is scoped
        // to the grandchild's pid so it cannot reap other tests' children.
        mark_child_subreaper();
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.3 & echo $!")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sh");
        let out = child.wait_with_output().expect("wait for sh");
        let grandchild: i32 = String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .expect("sh must print the background grandchild pid");

        let mut reaped = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            // SAFETY: WNOHANG with a null status pointer; no dereference.
            let pid = unsafe { libc::waitpid(grandchild, std::ptr::null_mut(), libc::WNOHANG) };
            if pid > 0 {
                reaped += 1;
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        assert!(
            reaped >= 1,
            "the orphaned background grandchild should have been reparented to this (subreaper) process and reaped; reaped={reaped}"
        );
    }

    #[test]
    fn test_drain_orphans_leaves_a_tracked_zombie_for_its_owner() {
        // Spawns a tracked child; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // Regression: the post-job drain used a raw `waitpid(-1, WNOHANG)`, which
        // reaped the exit status of a child that another in-server command
        // (quota, info, remote-control, ...) was about to collect itself, so the
        // owner saw ECHILD and failed. The drain must go through the guarded
        // reaper and leave a TRACKED zombie alone.
        mark_child_subreaper();
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("exit 7")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sh");
        let pid = child.id();
        let tracked = crate::core::reaper::TrackedChild::new(pid);
        // Scope the post-job drain to this test's own child so it can never
        // touch the exited children of other tests running in this process.
        let _reap_only = crate::core::reaper::set_test_reap_only(pid);

        // Wait until the child is a zombie (exited, not yet waited on).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let state = std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|c| {
                    let rest = c.get(c.rfind(')')? + 1..)?.trim_start().to_string();
                    rest.chars().next()
                });
            if state == Some('Z') {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "child never became a zombie (state {state:?})"
            );
            thread::sleep(Duration::from_millis(10));
        }

        drain_orphans();

        let status = match child.try_wait() {
            Ok(Some(status)) => status,
            Ok(None) => panic!("child must have exited"),
            Err(e) => tracked
                .recover_echild(&e)
                .unwrap_or_else(|| panic!("owner lost the exit status: {e}")),
        };
        assert_eq!(status.code(), Some(7));
    }

    #[test]
    fn test_exit_code_from_status_signal_and_plain_exit() {
        // Spawns real children; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // Plain exit: the code passes through unchanged.
        let status = Command::new("sh")
            .arg("-c")
            .arg("exit 3")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn exit 3")
            .wait()
            .expect("wait for exit 3");
        assert_eq!(exit_code_from_status(status), 3);

        // Signal death (SIGTERM): recorded as 128 + signal = 143, matching the
        // crate's 137/143 taxonomy instead of the old -1 sentinel.
        let status = Command::new("sh")
            .arg("-c")
            .arg("kill -TERM $$")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn kill -TERM")
            .wait()
            .expect("wait for TERM child");
        assert!(
            status.signal().is_some(),
            "child should have exited via SIGTERM, got {status}"
        );
        assert_eq!(exit_code_from_status(status), 143);
    }

    #[test]
    fn test_oversized_prompt_fails_before_any_job_state() {
        // Attempts a real spawn; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: PathBuf::from("/bin/sh"),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        let job_id = format!("big{}", std::process::id());
        meta.job_id = job_id.clone();
        meta.resolved_prompt = "x".repeat(PROMPT_ARGV_MAX_BYTES + 1);
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        let err = launch_background_job(&config, &mut meta)
            .expect_err("an over-limit prompt must be refused before spawn");
        assert!(
            err.to_string()
                .contains("longer than the 100000 byte limit"),
            "error must name the byte limit: {err}"
        );

        // The refusal happens before `meta.save`: no job row and no log file
        // may exist for this job.
        assert!(
            JobMetadata::load(&state_root, &job_id).is_err(),
            "no job row must be created for a refused oversized-prompt launch"
        );
        assert!(
            !JobMetadata::log_path(&state_root, &job_id).exists(),
            "no job log file may be created for a refused oversized-prompt launch"
        );
    }

    /// Counts the processes whose /proc/<pid>/cmdline contains `needle`.
    /// On non-Linux hosts /proc is absent and 0 is returned (which still
    /// satisfies the "no leftovers" assertions in these tests).
    fn count_processes_with_cmdline(needle: &str) -> usize {
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return 0;
        };
        let mut count = 0;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let Ok(cmdline) = std::fs::read(format!("/proc/{}/cmdline", name)) else {
                continue;
            };
            if String::from_utf8_lossy(&cmdline).contains(needle) {
                count += 1;
            }
        }
        count
    }

    fn wait_until(deadline: std::time::Instant, mut check: impl FnMut() -> bool) -> bool {
        loop {
            if check() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    fn test_cancelled_launch_fails_and_leaves_no_agy() {
        // Attempts a real spawn; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // (b) monitor delayed past the window, then cancelled: it never spawns.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        let pidfile = tmp.path().join("fake-agy.pid");
        fs::write(
            &agy,
            format!("#!/bin/sh\necho $$ > {}\nsleep 30\n", pidfile.display()),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy.clone(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        let job_id = format!("ipcnx{}", std::process::id());
        meta.job_id = job_id.clone();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        // Pre-spawn test hook: delay monitor well past the 200ms confirmation window.
        register_test_pre_spawn_hook(&job_id, || {
            thread::sleep(Duration::from_millis(600));
        });

        let err = launch_background_job_with_timeout(
            &config,
            &mut meta,
            Duration::from_millis(200),
            Duration::from_millis(25),
        )
        .expect_err("launch must not confirm a PID in time");
        assert!(
            err.to_string().contains("did not confirm a PID"),
            "unexpected error: {err}"
        );
        assert!(
            meta.pid.is_none(),
            "no PID may be recorded on a failed launch"
        );

        // Wait for the monitor to wake up and report cancellation in log
        assert!(
            wait_until(std::time::Instant::now() + Duration::from_secs(8), || {
                fs::read_to_string(JobMetadata::log_path(&state_root, &job_id))
                    .map(|l| l.contains("launch cancelled"))
                    .unwrap_or(false)
            }),
            "the monitor should have reported the cancellation in the job log"
        );
        let log = fs::read_to_string(JobMetadata::log_path(&state_root, &job_id)).unwrap();
        assert!(
            log.contains("agy was not spawned"),
            "the cancellation note must state that agy was not spawned: {log}"
        );

        // No PID recorded on row
        assert!(
            JobMetadata::load(&state_root, &job_id)
                .unwrap_or_else(|_| meta.clone())
                .pid
                .is_none(),
            "the cancelled monitor must not record a PID"
        );

        // The fake agy must never have run
        assert!(
            !pidfile.exists(),
            "the fake agy must never have been spawned"
        );
        assert_eq!(
            count_processes_with_cmdline(&agy.to_string_lossy()),
            0,
            "no agy process may remain after a cancelled launch"
        );

        clear_test_hooks(&job_id);
    }

    #[test]
    fn test_cancel_after_spawn_kills_agy_and_records_nothing() {
        // Spawns and kills a real agy; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // (a) launch times out and NO agy process remains (fake agy writes its
        // pid to a file; assert the pid is gone/killed within a short time).
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        let pidfile = tmp.path().join("fake-agy-cancelspawn.pid");
        fs::write(
            &agy,
            format!("#!/bin/sh\necho $$ > {}\nsleep 30\n", pidfile.display()),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy.clone(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let job_id = format!("ipcnxps{}", std::process::id());
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        let mut meta = test_meta_overrides();
        meta.job_id = job_id.clone();
        meta.worktree_path = wt;

        // Post-spawn hook: agy is spawned and running (writes pidfile), but the
        // monitor PARKS on a barrier before attempting CAS(PENDING, RUNNING).
        // The test releases it only after the agy has confirmed itself, so the
        // whole thing is deterministic instead of racing a fixed sleep: the
        // monitor is parked across the launcher's whole confirmation window,
        // the launcher therefore always times out (PENDING -> CANCELLED) while
        // the monitor writes no PID, and only then is the monitor released to
        // observe CANCELLED, kill the agy process group and record nothing.
        let gate = Arc::new(std::sync::Barrier::new(2));
        let gate_hook = Arc::clone(&gate);
        register_test_post_spawn_hook(&job_id, move || {
            let _ = gate_hook.wait();
        });

        let job_config = config.clone();
        let launcher = std::thread::spawn(move || {
            let res = launch_background_job_with_timeout(
                &job_config,
                &mut meta,
                Duration::from_millis(100),
                Duration::from_millis(20),
            );
            (res, meta)
        });

        // The monitor is parked at the barrier while the agy runs: wait until
        // the agy has started and recorded its PID (the "spawned and confirmed"
        // precondition), with a generous deadline for a loaded runner.
        assert!(
            wait_until(std::time::Instant::now() + Duration::from_secs(20), || {
                pidfile.exists()
            }),
            "fake agy should have started and recorded its PID"
        );

        // The launcher's confirmation window has now elapsed while the monitor
        // was parked, so the launch must have failed (PID never confirmed).
        let (res, meta) = launcher.join().unwrap();
        let err = res.expect_err("launch must fail when PID is not confirmed within timeout");
        assert!(
            err.to_string().contains("did not confirm a PID"),
            "unexpected error: {err}"
        );
        assert!(
            meta.pid.is_none(),
            "a cancelled launch must not record a PID on meta"
        );

        // Release the parked monitor: its CAS now fails (state is CANCELLED),
        // so it kills the agy process group and reaps the child.
        let _ = gate.wait();

        let agy_pid: u32 = fs::read_to_string(&pidfile)
            .unwrap_or_default()
            .trim()
            .parse()
            .expect("fake agy PID file must contain a number");
        assert!(
            agy_pid > 1,
            "fake agy must have recorded a real PID, got {agy_pid}"
        );

        // The monitor's post-spawn CAS failed (launcher set CANCELLED), so it
        // must have killed the wrapper's process group with SIGKILL and reaped
        // the child. The kill lands on the group the `sandbox-exec` wrapper led
        // (it is a setsid session/process-group leader), so the whole tree --
        // wrapper, its init-shim and the agy it forked/exec'd -- goes down
        // together. We therefore assert the invariant directly: NO process
        // still carrying the agy path in its cmdline remains. The monitor
        // parks in the post-spawn hook above before it reaches the kill, so
        // the teardown is not instantaneous; poll for it instead of assuming
        // it already happened. (The recorded `agy_pid` is the agy's
        // inner-PID-namespace PID, which has no `/proc/<pid>` entry in the
        // outer namespace, so a raw `is_pid_alive(agy_pid)` check is a no-op
        // under the wrapper and cannot serve as the "already killed" barrier.)
        assert!(
            wait_until(std::time::Instant::now() + Duration::from_secs(10), || {
                count_processes_with_cmdline(&agy.to_string_lossy()) == 0
            }),
            "no agy process may remain after the post-spawn cancellation"
        );

        // Nothing recorded on the row: no PID, no exit code, no final status
        let updated = JobMetadata::load(&state_root, &job_id).unwrap_or_else(|_| meta.clone());
        assert!(
            updated.pid.is_none(),
            "no PID may be recorded for a cancelled launch"
        );
        assert!(
            updated.exit_code.is_none(),
            "no exit code may be recorded for a cancelled launch"
        );
        assert!(
            updated.status.is_none(),
            "no final status may be recorded for a cancelled launch"
        );

        clear_test_hooks(&job_id);
    }

    #[test]
    fn test_boundary_race_never_leaves_orphaned_agy() {
        // Spawns real children; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // (c) the boundary race, made deterministic. The original version raced
        // a launch against a cancellation 200 times (tiny sleeps in the pre-spawn
        // hook) and ASSERTED both outcomes occurred at least once -- a
        // probabilistic requirement on timing: on a slow or loaded runner one
        // outcome is absent although the code is correct. Here each outcome is
        // instead driven explicitly via the test hooks and a bare Barrier (no
        // sleeps as the mechanism), and each scenario runs a fixed small number
        // of times. The INVARIANT is checked on every iteration:
        //
        //   Ok : launcher returns Ok(pid)   => agy alive, row holds the pid.
        //   Err: launcher returns Err       => no agy ever stays alive (no
        //                                      orphan), row holds no pid.
        //
        // Err arm: a pre-spawn hook parks the monitor (via a Barrier) before it
        // can spawn anything. The launcher's confirmation window always expires
        // PENDING->CANCELLED -- the monitor is parked and writes no PID, so
        // whatever the thread interleaving, the launcher wins the CAS and the
        // monitor, once released, observes CANCELLED and never spawns agy.
        // Ok arm: a post-spawn hook parks the monitor right after agy is spawned,
        // before the CAS. The test waits until agy has confirmed itself (its
        // pidfile appears, i.e. the PID is real), THEN releases the monitor so it
        // wins the CAS and records the row; with a wide launcher window the PID is
        // always confirmed before the cancellation could land.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\necho $$ > agy.pid\nsleep 30\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy.clone(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        // ------------------------------------------------------------------
        // Err arm: cancellation always wins; agy is never spawned.
        // ------------------------------------------------------------------
        const ERR_ITERATIONS: usize = 20;
        for i in 0..ERR_ITERATIONS {
            let job_id = format!("raceerr{}_{}", std::process::id(), i);
            let wt = tmp.path().join(format!("wterr_{}", i));
            fs::create_dir_all(&wt).unwrap();
            let mut meta = test_meta_overrides();
            meta.job_id = job_id.clone();
            meta.worktree_path = wt.clone();

            // Park the monitor before it can spawn agy; the test releases it
            // only after the launcher has already returned Err.
            let gate = Arc::new(std::sync::Barrier::new(2));
            let gate_hook = Arc::clone(&gate);
            register_test_pre_spawn_hook(&job_id, move || {
                let _ = gate_hook.wait();
            });

            let job_config = config.clone();
            let launcher = std::thread::spawn(move || {
                let res = launch_background_job_with_timeout(
                    &job_config,
                    &mut meta,
                    Duration::from_millis(200),
                    Duration::from_millis(20),
                );
                (res, meta)
            });

            // The confirmation window elapses while the monitor is parked: the
            // launcher wins PENDING->CANCELLED, so the launch is Err.
            let (res, meta) = launcher.join().unwrap();
            let err = res.expect_err("a launch with the monitor parked past the window must fail");
            assert!(
                err.to_string().contains("did not confirm a PID"),
                "unexpected error message: {err}"
            );
            assert!(meta.pid.is_none(), "failed launch must not set meta.pid");

            // Release the monitor: it must observe CANCELLED and skip the spawn.
            let _ = gate.wait();
            assert!(
                wait_until(std::time::Instant::now() + Duration::from_secs(8), || {
                    fs::read_to_string(JobMetadata::log_path(&state_root, &job_id))
                        .map(|l| l.contains("agy was not spawned"))
                        .unwrap_or(false)
                }),
                "the cancelled monitor must report in the job log that agy was not spawned"
            );

            // Invariants: no agy process may remain (never spawned) and the row
            // must not carry a PID.
            assert_eq!(
                count_processes_with_cmdline(&agy.to_string_lossy()),
                0,
                "no agy process may remain after a cancelled launch"
            );
            let row = JobMetadata::load(&state_root, &job_id).unwrap_or_else(|_| meta.clone());
            assert!(
                row.pid.is_none(),
                "a cancelled launch must not record a PID in the row"
            );

            clear_test_hooks(&job_id);
        }

        // ------------------------------------------------------------------
        // Ok arm: the PID is always confirmed before the cancellation lands.
        // ------------------------------------------------------------------
        const OK_ITERATIONS: usize = 20;
        for i in 0..OK_ITERATIONS {
            let job_id = format!("raceok{}_{}", std::process::id(), i);
            let wt = tmp.path().join(format!("wtok_{}", i));
            fs::create_dir_all(&wt).unwrap();
            let mut meta = test_meta_overrides();
            meta.job_id = job_id.clone();
            meta.worktree_path = wt.clone();

            // Park the monitor right after agy is spawned, before the CAS: the
            // test confirms the PID first, then releases it to record the row.
            let gate = Arc::new(std::sync::Barrier::new(2));
            let gate_hook = Arc::clone(&gate);
            register_test_post_spawn_hook(&job_id, move || {
                let _ = gate_hook.wait();
            });

            let job_config = config.clone();
            let launcher = std::thread::spawn(move || {
                let res = launch_background_job_with_timeout(
                    &job_config,
                    &mut meta,
                    Duration::from_secs(10),
                    Duration::from_millis(20),
                );
                (res, meta)
            });

            // Wait until agy has actually started (its pidfile appears), then
            // release the monitor to win the CAS and record the PID.
            let pidfile = wt.join("agy.pid");
            assert!(
                wait_until(std::time::Instant::now() + Duration::from_secs(8), || {
                    pidfile.exists()
                }),
                "fake agy should have started and recorded its pidfile"
            );
            let _ = gate.wait();

            let (res, meta) = launcher.join().unwrap();
            let pid = res.expect("a PID confirmed before the window must make the launch succeed");
            assert!(pid > 1);
            assert_eq!(meta.pid, Some(pid), "Ok launch must record the PID on meta");

            // Invariants: agy MUST be alive and the row MUST hold the PID.
            assert!(
                crate::core::job::is_pid_alive(pid),
                "successful launch (Ok) must have a live agy process"
            );
            let row = JobMetadata::load(&state_root, &job_id).unwrap();
            assert_eq!(
                row.pid,
                Some(pid),
                "Ok launch must record the PID in the row"
            );

            // Clean up the live process.
            let _ = crate::core::commands::kill::send_signal(pid, "KILL");
            let _ = wait_until(std::time::Instant::now() + Duration::from_secs(1), || {
                !crate::core::job::is_pid_alive(pid)
            });

            clear_test_hooks(&job_id);
        }
    }

    #[test]
    fn test_pid_confirmed_right_at_the_timeout_boundary_succeeds() {
        // Spawns a real child; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // A slow (but healthy) launch: the monitor reaches agy only late in
        // the confirmation window, so the PID is confirmed right at the
        // boundary. The launch must succeed with the confirmed PID and the
        // job must then run to completion.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        fs::write(
            &agy,
            "#!/bin/sh\necho '{\"status\":\"SUCCESS\",\"response\":\"boundary-ok\",\"conversation_id\":\"conv-boundary\"}'\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        let job_id = format!("ipcbc{}", std::process::id());
        meta.job_id = job_id.clone();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        register_test_pre_spawn_hook(&job_id, || {
            thread::sleep(Duration::from_millis(2000));
        });

        let pid = launch_background_job_with_timeout(
            &config,
            &mut meta,
            Duration::from_millis(5000),
            Duration::from_millis(25),
        )
        .expect("a PID confirmed late in the timeout window must make the launch succeed");
        assert!(pid > 1);
        assert_eq!(meta.pid, Some(pid));

        // And the job must then run to completion as an ordinary successful job.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let updated = JobMetadata::load(&state_root, &job_id).unwrap();
            if updated.exit_code.is_some() {
                assert_eq!(updated.exit_code, Some(0));
                assert_eq!(updated.status, Some(crate::core::job::JobStatus::Done));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "job did not finish in time"
            );
            thread::sleep(Duration::from_millis(50));
        }

        clear_test_hooks(&job_id);
    }

    #[test]
    fn test_launch_timeout_comes_from_config() {
        // Attempts a real spawn; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // `launch_background_job` must honor `config.launch_timeout_secs`
        // (the value fed by `AGENT_RUN_LAUNCH_TIMEOUT_SECS`, default 20 s)
        // instead of a hard-coded window: with a 1 s config value the
        // launch of a starved monitor must fail in ~1 s, not 20 s.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();

        let agy = tmp.path().join("fake-agy");
        fs::write(&agy, "#!/bin/sh\necho hi\nexit 0\n").unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root: state_root.clone(),
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            launch_timeout_secs: 1,
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        let job_id = format!("ipctc{}", std::process::id());
        meta.job_id = job_id.clone();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        meta.worktree_path = wt;

        register_test_pre_spawn_hook(&job_id, || {
            thread::sleep(Duration::from_millis(3000));
        });

        let started = std::time::Instant::now();
        let err = launch_background_job(&config, &mut meta)
            .expect_err("a starved monitor must time out at the configured 1 s");
        let elapsed = started.elapsed();
        assert!(
            err.to_string().contains("did not confirm a PID"),
            "unexpected error: {err}"
        );
        // ~1 s, clearly not the monitor's 3 s starvation (agy was never
        // spawned), the old hardcoded 2 s window, or the 20 s default: the
        // configured value must govern the wait.
        assert!(
            elapsed < Duration::from_millis(1800),
            "the configured 1 s launch_timeout_secs must govern the wait, not the old hardcoded 2 s or the 20 s default (waited {:?})",
            elapsed
        );

        clear_test_hooks(&job_id);
    }

    /// Pure-function check of the always-on `sandbox-exec` wrapping (no
    /// process spawn): with passthrough OFF, `build_command`'s argv must be
    /// exactly `[<kubeadp-agent exe>, sandbox-exec, --policy, <file>, --,
    /// <agy>, <agy args...>]`. This covers the wrapping invariant in every
    /// feature set, including production builds where isolation is always on.
    #[test]
    fn test_launch_argv_passthrough_off_is_sandboxed_wrapping() {
        let prog = PathBuf::from("/usr/local/bin/agy");
        let args = vec![
            "--dangerously-skip-permissions".to_string(),
            "--mode".to_string(),
            "accept-edits".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "-p".to_string(),
            "hello".to_string(),
            "--add-dir".to_string(),
            "/workspace/project-a/.agent-jobs/test1234".to_string(),
        ];
        let policy_path = PathBuf::from("/tmp/state/jobs/test1234.policy.json");

        let argv = launch_argv(&prog, &args, &policy_path, false);

        let launcher = crate::sandbox::find_launcher_exe();
        assert_eq!(argv[0], launcher.to_string_lossy().to_string());
        assert_eq!(argv[1], "sandbox-exec");
        assert_eq!(argv[2], "--policy");
        assert_eq!(argv[3], policy_path.to_string_lossy().to_string());
        assert_eq!(argv[4], "--");
        assert_eq!(argv[5], prog.to_string_lossy().to_string());
        assert_eq!(&argv[6..], &args);
        assert_eq!(argv.len(), 6 + args.len());
    }

    /// End-to-end: a fake `agy` runs to completion through the REAL launcher.
    /// `build_command` (the production launch path) is exercised with the
    /// test-hooks seam OFF, so it returns the real
    /// `kubeadp-agent sandbox-exec --policy <file> -- <agy> ...` command, which
    /// is spawned and waited on here -- exercising the actual sandbox backend.
    ///
    /// It probes first with the exact `sandbox-probe` library code
    /// `sandbox-exec` uses to pick a backend; when this platform cannot isolate
    /// (a plain Docker build container: no user namespaces / Landlock) it
    /// reports a clear skip and returns, so it runs on gVisor/Kata pods and is
    /// skipped in Docker CI. The seam default stays ON, so the other
    /// fake-agy launcher tests are never affected.
    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_fake_agy_runs_through_real_sandbox_wrapper() {
        // Spawns real children; take the process-child test lock.
        let _proc_lock = crate::core::reaper::lock_proc_child_tests();
        // Probe with the same code `sandbox-exec` uses to decide a backend; if
        // no mechanism is available here, isolation cannot run and the test is
        // meaningless (and the wrapper would fail closed with exit 125).
        let runtime = crate::sandbox::detect_runtime();
        let probe = crate::sandbox::probe_environment(None);
        if let Err(reason) = crate::sandbox::cli::validate_backend_probe(runtime, &probe) {
            eprintln!(
                "skipped: sandbox isolation unavailable on this platform ({reason}); \
                 the test-hooks passthrough seam keeps the launcher unit tests runnable here"
            );
            return;
        }

        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();

        // Fake agy records its argv in the (sandbox-writable) worktree and
        // emits a structured SUCCESS line on stdout.
        let agy = tmp.path().join("fake-agy");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\n{{ printf '%s\\n' \"$@\" > {argv}; echo '{{\"status\":\"SUCCESS\",\"response\":\"real-sbx-ok\",\"conversation_id\":\"conv-real-sbx\"}}'\n}}\n",
                argv = wt.join("argv.txt").display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = Config {
            workspace_root,
            state_root,
            agy_path: agy,
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            // Point the policy's writable shared path (the agent's home) at a
            // temp dir the sandbox backend can actually create; the production
            // default `/home/agent` is not creatable on this (or a CI) host.
            sandbox_home: tmp.path().join("sbx-home"),
            ..Default::default()
        };

        let mut meta = test_meta_overrides();
        meta.job_id = format!("sbxe2e{}", std::process::id());
        meta.worktree_path = wt.clone();
        let mode = InvocationMode::Start {
            resolved_prompt: "e2e sandbox probe".to_string(),
        };

        // Build the REAL wrapped command via the production `build_command`,
        // with the seam turned off just for this call. The flag is restored to
        // its default (on) immediately after, so the window during which any
        // other launcher test could observe the wrapped form is only this
        // synchronous build (and on an isolating platform such a concurrent
        // wrap is a working sandbox, not a failure).
        let (mut cmd, policy_path, _jobset_secret) = {
            crate::sandbox::testhook::set_sandbox_passthrough(false);
            let built = build_command(&config, &meta, &mode, false);
            crate::sandbox::testhook::set_sandbox_passthrough(true);
            built.expect("build_command must succeed with the seam off")
        };
        // Sanity: the production wrapper form is what we are about to run.
        let argv = launch_argv(
            &config.agy_path,
            &build_command_and_args(&config, &meta, &mode, false).1,
            &policy_path,
            false,
        );
        assert_eq!(argv[1], "sandbox-exec");
        assert_eq!(argv[3], policy_path.to_string_lossy().to_string());

        // Run the wrapper (the real `kubeadp-agent sandbox-exec ... -- agy`),
        // with the child's cwd at the worktree, capturing agy's stdout.
        let out_path = tmp.path().join("sbx-out.txt");
        let err_path = tmp.path().join("sbx-err.txt");
        let out_file = fs::File::create(&out_path).unwrap();
        let err_file = fs::File::create(&err_path).unwrap();
        let status = cmd
            .current_dir(&wt)
            .stdout(out_file)
            .stderr(err_file)
            .spawn()
            .expect("the sandbox wrapper must spawn")
            .wait()
            .expect("waiting on the sandbox wrapper must succeed");
        assert!(
            status.success(),
            "the sandboxed fake agy must exit 0; wrapper exit: {status}; stderr: {}",
            fs::read_to_string(&err_path).unwrap_or_default()
        );

        // The fake agy really ran inside the sandbox: its structured line is on
        // stdout and its argv was recorded in the sandbox-writable worktree.
        let out = fs::read_to_string(&out_path).unwrap();
        assert!(
            out.contains("conv-real-sbx") && out.contains("real-sbx-ok"),
            "the sandboxed agy's final line must reach stdout: {out}"
        );
        let argv_text = fs::read_to_string(wt.join("argv.txt")).unwrap();
        assert!(
            argv_text.contains("--output-format"),
            "the sandboxed agy must have executed its body: {argv_text}"
        );
    }

    #[test]
    fn test_claude_job_policy_allows_oauth_env_agy_does_not() {
        // HL-71: only a claude-engine job's policy lists the OAuth token env
        // NAME in `env_allow` (the pod's value then passes the sandbox-exec
        // env filter); agy jobs keep the default allowlist. The policy file
        // carries the NAME only -- never a token VALUE.
        let tmp = tempdir().unwrap();
        let state_root = tmp.path().join("state");
        let workspace_root = tmp.path().join("ws");
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&workspace_root).unwrap();
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        let config = Config {
            workspace_root,
            state_root,
            agy_path: std::path::PathBuf::from("/usr/local/bin/agy"),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            sandbox_home: tmp.path().join("sbx-home"),
            ..Default::default()
        };
        let mode = InvocationMode::Start {
            resolved_prompt: "policy probe".to_string(),
        };

        // claude job: the written policy lists the env NAME.
        let mut meta = test_meta_overrides();
        meta.engine = Some("claude".to_string());
        meta.job_id = "clj71claude".to_string();
        build_command(&config, &meta, &mode, false).expect("claude job builds");
        let claude_policy_path =
            JobMetadata::log_dir(&config.state_root).join(format!("{}.policy.json", meta.job_id));
        let claude_json = fs::read_to_string(&claude_policy_path).unwrap();
        assert!(
            claude_json.contains(crate::sandbox::CLAUDE_OAUTH_TOKEN_ENV),
            "the claude job policy must allow CLAUDE_CODE_OAUTH_TOKEN: {claude_json}"
        );
        assert!(
            !claude_json.contains("oat01-fake-token-value"),
            "the policy file must never carry a token value: {claude_json}"
        );

        // agy job (no engine): the default allowlist is kept.
        let mut meta = test_meta_overrides();
        meta.job_id = "clj71agy".to_string();
        build_command(&config, &meta, &mode, false).expect("agy job builds");
        let agy_policy_path =
            JobMetadata::log_dir(&config.state_root).join(format!("{}.policy.json", meta.job_id));
        let agy_json = fs::read_to_string(&agy_policy_path).unwrap();
        assert!(
            !agy_json.contains(crate::sandbox::CLAUDE_OAUTH_TOKEN_ENV),
            "the agy job policy must NOT allow CLAUDE_CODE_OAUTH_TOKEN: {agy_json}"
        );
    }

    #[test]
    fn test_exact_launch_plan_agy_start_and_resume() {
        let config = Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            claude_path: PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        let registry = EngineRegistry::with_builtin(&config);
        let mut meta = test_meta_overrides();
        meta.job_id = "test-job-agy".to_string();
        meta.mode = "accept-edits".to_string();
        meta.engine = Some("agy".to_string());

        let engine = registry.for_job(&meta).expect("engine for agy");

        // Start turn
        let start_turn = TurnInput::Start {
            prompt: "Test prompt [agent-run ref test-job-agy#0]",
            live: false,
        };
        let plan = engine.job_command(&meta, start_turn).expect("start plan");
        assert_eq!(plan.program, PathBuf::from("/usr/local/bin/agy"));
        assert_eq!(plan.stdin, StdinMode::Null);
        assert!(!plan.pty);
        assert!(plan
            .args
            .contains(&"--dangerously-skip-permissions".to_string()));
        assert!(plan.args.contains(&"--mode".to_string()));
        assert!(plan.args.contains(&"accept-edits".to_string()));
        assert!(plan.args.contains(&"--output-format".to_string()));
        assert!(plan.args.contains(&"stream-json".to_string()));
        assert!(plan.args.contains(&"-p".to_string()));
        assert!(plan
            .args
            .contains(&"Test prompt [agent-run ref test-job-agy#0]".to_string()));

        // Resume turn
        let resume_turn = TurnInput::Resume {
            prompt: "Next turn [agent-run ref test-job-agy#1]",
            conversation: Some("conv-1234"),
            live: false,
        };
        let plan_resume = engine.job_command(&meta, resume_turn).expect("resume plan");
        assert_eq!(plan_resume.program, PathBuf::from("/usr/local/bin/agy"));
        assert_eq!(plan_resume.stdin, StdinMode::Null);
        assert!(plan_resume.args.contains(&"--conversation".to_string()));
        assert!(plan_resume.args.contains(&"conv-1234".to_string()));
        assert!(plan_resume.args.contains(&"-p".to_string()));
        assert!(plan_resume
            .args
            .contains(&"Next turn [agent-run ref test-job-agy#1]".to_string()));
    }

    #[test]
    fn test_exact_launch_plan_claude_start_and_resume() {
        let config = Config {
            agy_path: PathBuf::from("/usr/local/bin/agy"),
            claude_path: PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        let registry = EngineRegistry::with_builtin(&config);
        let mut meta = test_meta_overrides();
        meta.job_id = "test-job-claude".to_string();
        meta.mode = "normal".to_string();
        meta.engine = Some("claude".to_string());

        let engine = registry.for_job(&meta).expect("engine for claude");

        // Start turn
        let start_turn = TurnInput::Start {
            prompt: "Hello claude",
            live: false,
        };
        let plan = engine.job_command(&meta, start_turn).expect("start plan");
        assert_eq!(plan.program, PathBuf::from("/usr/local/bin/claude"));
        assert!(plan.args.contains(&"--input-format".to_string()));
        assert!(plan.args.contains(&"stream-json".to_string()));
        assert!(plan.args.contains(&"--output-format".to_string()));
        assert!(plan.args.contains(&"stream-json".to_string()));
        assert!(plan.args.contains(&"--verbose".to_string()));

        match &plan.stdin {
            StdinMode::Pipe { first_line } => {
                let line = first_line.as_deref().expect("first line present");
                assert!(line.contains("Hello claude"));
                assert!(
                    line.ends_with('\n'),
                    "claude prompt line must end with newline"
                );
            }
            _ => panic!("claude must use StdinMode::Pipe"),
        }

        // Resume turn
        let resume_turn = TurnInput::Resume {
            prompt: "Continue work",
            conversation: Some("sess-9876"),
            live: false,
        };
        let plan_resume = engine.job_command(&meta, resume_turn).expect("resume plan");
        assert_eq!(plan_resume.program, PathBuf::from("/usr/local/bin/claude"));
        assert!(plan_resume.args.contains(&"--resume".to_string()));
        assert!(plan_resume.args.contains(&"sess-9876".to_string()));
        match &plan_resume.stdin {
            StdinMode::Pipe { first_line } => {
                let line = first_line.as_deref().expect("first line present");
                assert!(line.contains("Continue work"));
                assert!(
                    line.ends_with('\n'),
                    "claude resume prompt line must end with newline"
                );
            }
            _ => panic!("claude resume must use StdinMode::Pipe"),
        }
    }

    #[test]
    fn test_claude_job_command_preserves_prompt_with_fake_markers_and_cwd() {
        let config = Config {
            claude_path: PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        };
        let registry = EngineRegistry::with_builtin(&config);
        let mut meta = test_meta_overrides();
        meta.job_id = "test-job-markers".to_string();
        meta.engine = Some("claude".to_string());
        meta.worktree_path = PathBuf::from("/workspace/test-wt");

        let engine = registry.for_job(&meta).expect("engine for claude");

        let turn = TurnInput::Start {
            prompt: "FAKE_FAIL FAKE_WRITE=result.txt claude prompt",
            live: false,
        };
        let plan = engine.job_command(&meta, turn).expect("plan");

        match &plan.stdin {
            StdinMode::Pipe { first_line } => {
                let line = first_line.as_deref().expect("first line");
                assert!(line.contains("FAKE_FAIL"));
                assert!(line.contains("FAKE_WRITE=result.txt"));
                assert!(line.ends_with('\n'));
            }
            _ => panic!("expected StdinMode::Pipe"),
        }

        // Verify sandboxed execution plan wraps with worktree path preserved
        let (cmd, policy_path, _) = sandboxed(plan, &config, &meta).expect("sandboxed command");
        assert_eq!(cmd.get_current_dir(), None); // Command current_dir is applied on spawn_cmd in run_internal_runner
        assert!(policy_path.to_string_lossy().contains("test-job-markers"));
    }
}

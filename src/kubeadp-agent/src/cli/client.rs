//! Remote client: every command is performed by the agent-run API server over
//! HTTP. This client renders the server's [`CommandOutput`] with the engine's
//! own [`CommandOutput::print`] path, so remote UX is byte-identical to the
//! old local CLI by construction.
//!
//! Contract: `/tmp/agent-run-split-contract.md` (§3 client behavior, §4.1
//! endpoint map, §4.3 tail poll loop, §4.4 wait).

use std::io::{self, IsTerminal, Read, Write};
use std::path::Path;
use std::process::exit;
use std::time::{Duration, Instant};

use crate::core::job::JobStatus;
use crate::core::output::CommandOutput;
use crate::core::runner::resolve_prompt;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::cli::{
    Cli, Commands, CommitArgs, DiffArgs, DiscardArgs, ExportArgs, GcArgs, InitArgs,
    RemoteControlAction, RepairArgs, RetryArgs, ReviewArgs, StartArgs, WaitArgs,
};

pub(crate) const API_PREFIX: &str = "/api/v1";
/// Response header carrying the offset just past the bytes returned by a
/// `log?offset=` read.
const LOG_OFFSET_HEADER: &str = "Agent-Run-Log-Offset";
/// `tail`: cadence for reading new log bytes (mirrors the local tail loop).
const TAIL_POLL_INTERVAL: Duration = Duration::from_millis(150);
/// `tail`: wait for log file creation -- 100ms sleeps, up to 20 retries
/// (mirrors the local tail loop).
const TAIL_LOG_WAIT_INTERVAL: Duration = Duration::from_millis(100);
const TAIL_LOG_WAIT_RETRIES: u32 = 20;

/// Default connect timeout for every call.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Read timeout for ordinary (short-lived) calls.
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);
/// Read timeout for calls that may do git/worktree/agy work server-side
/// (`start`, `retry`, `clean`, `gc`, `commit`, `review`).
const LONG_READ_TIMEOUT: Duration = Duration::from_secs(300);
/// Extra seconds added on top of the user's `--timeout` for the `wait` call.
const WAIT_READ_TIMEOUT_EXTRA: u64 = 60;

/// Read a positive-integer env var; ignore and warn on invalid values.
fn read_timeout_env(var: &str) -> Option<u64> {
    let val = std::env::var(var).ok()?;
    match val.trim().parse::<u64>() {
        Ok(n) if n > 0 => Some(n),
        _ => {
            eprintln!(
                "agent-run: warning: ignoring invalid value for {var}={val:?} (must be a positive integer)"
            );
            None
        }
    }
}

/// Resolve the connect timeout: `AGENT_RUN_CONNECT_TIMEOUT_SECS` env var or
/// the compiled-in default.
fn connect_timeout() -> Duration {
    read_timeout_env("AGENT_RUN_CONNECT_TIMEOUT_SECS")
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_CONNECT_TIMEOUT)
}

/// Resolve the ordinary read timeout: `AGENT_RUN_HTTP_TIMEOUT_SECS` env var
/// or the compiled-in default.
fn ordinary_read_timeout() -> Duration {
    read_timeout_env("AGENT_RUN_HTTP_TIMEOUT_SECS")
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_READ_TIMEOUT)
}

/// Read timeout for remote-control operations (`enable`, `disable`, `status`).
/// Set to at least 200 s to accommodate cold starts (30-60 s) with safety headroom.
const REMOTE_CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(200);

/// Resolve the remote-control read timeout: at least 200 s, or `AGENT_RUN_HTTP_TIMEOUT_SECS`
/// if that variable specifies a larger value.
fn remote_control_read_timeout() -> Duration {
    let base = REMOTE_CONTROL_READ_TIMEOUT;
    if let Some(env_secs) = read_timeout_env("AGENT_RUN_HTTP_TIMEOUT_SECS") {
        let env_dur = Duration::from_secs(env_secs);
        if env_dur > base {
            return env_dur;
        }
    }
    base
}

/// Parse a human-readable duration string used by the `--timeout` flag
/// (e.g. "30m", "2h", "90s", or bare integer seconds).  Returns the number
/// of seconds, or `None` if the string cannot be parsed.
fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // bare integer: treat as seconds
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    // Try agent_core GC / humantime duration parser
    if let Ok(dur) = crate::core::commands::gc::parse_duration_string(s) {
        if let Ok(secs) = dur.num_seconds().try_into() {
            return Some(secs);
        }
    }
    // suffix form: trailing h / m / s / d
    let (digits, suffix) = s.split_at(s.len() - 1);
    let n: u64 = digits.parse().ok()?;
    match suffix {
        "s" => Some(n),
        "m" => Some(n * 60),
        "h" => Some(n * 3600),
        "d" => Some(n * 86400),
        _ => None,
    }
}

/// True when the user opted into diagnostic error output
/// (`AGENT_RUN_DEBUG=1`): an extra stderr line exposing the HTTP status
/// and the API error code behind a transport/HTTP failure.
fn debug_env_on() -> bool {
    std::env::var_os("AGENT_RUN_DEBUG")
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false)
}

/// Env var naming the operator token file (the same `[common] api_token_file`
/// -> `KUBEADP_API_TOKEN_FILE` mapping the server roles use; the file's
/// trimmed contents are presented as `Authorization: Bearer <token>` on every
/// request the CLI makes).
const API_TOKEN_FILE_ENV: &str = "KUBEADP_API_TOKEN_FILE";

/// Reads the operator token file, trimming surrounding whitespace. The CLI
/// build (`--no-default-features --features cli`) has no `server` feature, so
/// it cannot depend on `core::server::token::read_token_file` and keeps its
/// own tiny reader here. The raw token is never logged or shown anywhere: on
/// error the message names only the path.
fn api_token_from_file(env_value: &str) -> Result<String, String> {
    let path = Path::new(env_value);
    let bytes = std::fs::read(path).map_err(|e| {
        format!(
            "cannot read operator API token from {API_TOKEN_FILE_ENV} '{}': {e}",
            path.display()
        )
    })?;
    let token = String::from_utf8_lossy(&bytes).trim().to_string();
    if token.is_empty() {
        return Err(format!(
            "operator API token file {API_TOKEN_FILE_ENV} '{}' is empty",
            path.display()
        ));
    }
    Ok(token)
}

/// Resolves the operator token the CLI must present: the trimmed contents of
/// the file named by `KUBEADP_API_TOKEN_FILE` (config `api_token_file`). When
/// the variable is unset there is no token and no header is added. A
/// configured but unreadable or empty file is an error naming the path (the
/// token is never part of any message).
fn resolve_api_token() -> Result<Option<String>, String> {
    match std::env::var_os(API_TOKEN_FILE_ENV) {
        None => Ok(None),
        Some(value) => {
            let raw = value
                .into_string()
                .map_err(|_| format!("{API_TOKEN_FILE_ENV} is not valid Unicode"));
            api_token_from_file(&raw?).map(Some)
        }
    }
}

/// ureq middleware that stamps `Authorization: Bearer <token>` on every
/// request before it is sent. Only installed when a token file is configured
/// (see [`resolve_api_token`]); when it is set the bearer value must never
/// appear in debug output or error text, so the token is held in the struct
/// and the header is set exactly once per request.
#[allow(clippy::result_large_err)]
fn bearer_token_middleware(
    token: String,
) -> impl Fn(ureq::Request, ureq::MiddlewareNext) -> Result<ureq::Response, ureq::Error> {
    move |req, next| next.handle(req.set("Authorization", &format!("Bearer {token}")))
}

/// Build a ureq agent with timeouts appropriate for the given call type.
///
/// `read_timeout` of `None` means "no read timeout" (unbounded); used for
/// calls that wait on a human (auth flows) and for `wait` when the user
/// provided no `--timeout` argument.
///
/// When `KUBEADP_API_TOKEN_FILE` is set (config `[common] api_token_file`)
/// the built agent adds a middleware that presents the file's trimmed
/// contents as `Authorization: Bearer` on every request. A configured but
/// unreadable or empty file is a clear error (with the path, never the
/// token); this is the CLI's failure path here because the whole CLI
/// communicates with the API over `agents` built by this function.
fn build_agent(read_timeout: Option<Duration>) -> ureq::Agent {
    let ct = connect_timeout();
    let mut builder = ureq::AgentBuilder::new().timeout_connect(ct);
    if let Some(rt) = read_timeout {
        builder = builder.timeout_read(rt).timeout_write(rt);
    }
    match resolve_api_token() {
        Ok(Some(token)) => {
            builder = builder.middleware(bearer_token_middleware(token));
        }
        Ok(None) => {}
        Err(message) => {
            eprintln!("Error: {message}");
            exit(1);
        }
    }
    builder.build()
}

/// Performs `cli.command` against the agent-run API server at `base_url`
/// and never returns: it exits with the exit code the old local CLI would
/// use for the same result.
pub fn dispatch(cli: &Cli, base_url: &str) -> ! {
    let base = normalize_base_url(base_url);
    let json_mode = cli.global_opts.json;

    match &cli.command {
        Commands::Init(args) => {
            // init first makes sure the server is authenticated (agy owns
            // its credentials; see run_auth_flow), then does the setup.
            // Auth waits on a human -- no read timeout.
            let agent_auth = build_agent(None);
            if let Err(err) = run_auth_flow(&agent_auth, &base, None) {
                eprintln!("Error: {err:?}");
                exit(1);
            }
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/init"),
                    Some(&init_body(args)),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Start(args) => {
            let cwd =
                std::env::current_dir().expect("failed to read the current working directory");
            let body = match start_body(args, &cwd) {
                Ok(body) => body,
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    if json_mode {
                        let msg = format!("{:?}", err);
                        eprintln!("{}", json!({ "error": msg, "code": "invalid_request" }));
                    }
                    exit(1);
                }
            };
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/jobs"),
                    Some(&body),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Status(args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &job_url(&base, &args.job_id, ""),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Result(args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            // `result` keeps its existing --json behavior; it is the same flag
            // as the global one (ResultArgs no longer carries its own).
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &job_url(&base, &args.job_id, "/result"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Log(args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            raw_to_stdout(&agent, &job_url(&base, &args.job_id, "/log?offset=0"))
        }
        Commands::Tail(args) => {
            // A pure client-side poll; the local deadline (when --timeout is
            // given) plus WAIT_READ_TIMEOUT_EXTRA seconds of headroom bounds
            // each individual request.
            let tail_read_timeout = match &args.timeout {
                Some(raw) => match parse_duration_secs(raw) {
                    Some(secs) => Some(Duration::from_secs(
                        secs.saturating_add(WAIT_READ_TIMEOUT_EXTRA),
                    )),
                    None => fatal(
                        &format!(
                            "Invalid duration format '{}' for --timeout (e.g. 30m, 2h)",
                            raw
                        ),
                        false,
                    ),
                },
                None => Some(ordinary_read_timeout()),
            };
            let deadline = args
                .timeout
                .as_deref()
                .and_then(parse_duration_secs)
                .map(|secs| Instant::now() + Duration::from_secs(secs));
            let agent = build_agent(tail_read_timeout);
            // tail is raw-text: --json is ignored entirely (text output,
            // regular error format).
            tail(&agent, &base, &args.job_id, deadline)
        }
        Commands::Diff(args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            if args.stat {
                exit_output(
                    &call(
                        &agent,
                        "GET",
                        &job_url(&base, &args.job_id, &format!("/diff{}", diff_query(args))),
                        None,
                        json_mode,
                    ),
                    json_mode,
                )
            } else {
                raw_to_stdout(
                    &agent,
                    &job_url(&base, &args.job_id, &format!("/diff{}", diff_query(args))),
                )
            }
        }
        Commands::Info(_args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &format!("{base}{API_PREFIX}/info"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::List(args) => {
            let mut subpath = String::new();
            let mut first = true;
            if let Some(project) = &args.project {
                subpath.push_str(&format!("?project={}", urlencode(project)));
                first = false;
            }
            for status in &args.status {
                subpath.push_str(if first { "?" } else { "&" });
                subpath.push_str("status=");
                subpath.push_str(&urlencode(status));
                first = false;
            }
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &format!("{base}{API_PREFIX}/jobs{subpath}"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Projects(_args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &format!("{base}{API_PREFIX}/projects"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Models(_a) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &format!("{base}{API_PREFIX}/models"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Quota(args) => {
            let subpath = if args.fresh {
                "?fresh=true"
            } else {
                "?fresh=false"
            };
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &format!("{base}{API_PREFIX}/quota{subpath}"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Files(args) => {
            let mut subpath = String::from("/files");
            if let Some(path) = &args.path {
                subpath.push_str(&format!("?path={}", urlencode(path)));
            }
            if let Some(depth) = args.depth {
                subpath.push_str(&format!(
                    "{}depth={}",
                    if subpath.contains('?') { "&" } else { "?" },
                    depth
                ));
            }
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "GET",
                    &job_url(&base, &args.job_id, &subpath),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Cat(args) => {
            // `path` is required by the clap subcommand.
            let mut subpath = format!("/cat?path={}", urlencode(&args.path));
            if let Some(lines) = &args.lines {
                subpath.push_str(&format!("&lines={}", urlencode(lines)));
            }
            if let Some(max_bytes) = args.max_bytes {
                subpath.push_str(&format!("&max_bytes={}", max_bytes));
            }
            // The server's CommandOutput for cat carries the content in
            // `stdout_lines` (split on '\n'); `render_exit` writes every line
            // back with a trailing '\n' and then exits with `exit_code`, so
            // the bytes are faithful for newline-terminated files and the
            // binary-file error path still lands on stderr + exit 1.
            // cat is raw-text: --json is ignored entirely (text output,
            // regular error format).
            let agent = build_agent(Some(ordinary_read_timeout()));
            render_exit(&call(
                &agent,
                "GET",
                &job_url(&base, &args.job_id, &subpath),
                None,
                false,
            ))
        }
        Commands::Clean(args) => {
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "DELETE",
                    &job_url(
                        &base,
                        &args.job_id,
                        &format!("/clean?force={}&purge_log={}", args.force, args.purge_log),
                    ),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Kill(args) => {
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &job_url(&base, &args.job_id, "/kill"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Jobset(args) => {
            // `jobset end <id>` -> POST /api/v1/jobsets/<id>/end (operator
            // token gate is the same shared bearer layer as every /api/v1
            // route; the CLI attaches the bearer credential when configured).
            let encoded = urlencode(&args.jobset_id);
            let agent = build_agent(Some(ordinary_read_timeout()));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/jobsets/{encoded}/end"),
                    None,
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Retry(args) => {
            let cwd =
                std::env::current_dir().expect("failed to read the current working directory");
            let body = match retry_body(args, &cwd) {
                Ok(body) => body,
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    if json_mode {
                        let msg = format!("{:?}", err);
                        eprintln!("{}", json!({ "error": msg, "code": "invalid_request" }));
                    }
                    exit(1);
                }
            };
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &job_url(&base, &args.job_id, "/retry"),
                    Some(&body),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Repair(args) => {
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            let url = if args.all {
                format!("{base}{API_PREFIX}/jobs/repair")
            } else {
                job_url(
                    &base,
                    args.job_id.as_deref().expect("clap: job_id or --all"),
                    "/repair",
                )
            };
            exit_output(
                &call(&agent, "POST", &url, Some(&repair_body(args)), json_mode),
                json_mode,
            )
        }
        Commands::Gc(args) => {
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/gc"),
                    Some(&gc_body(args)),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Wait(args) => {
            // One long request: the server holds the connection until the
            // wait condition is met or the wait times out (§4.4).
            //
            // The read timeout is the user's --timeout (parsed to seconds)
            // plus WAIT_READ_TIMEOUT_EXTRA seconds of headroom. When no
            // --timeout is given the server waits indefinitely, so we also
            // set no read timeout (None).
            let wait_read_timeout = args
                .timeout
                .as_deref()
                .and_then(parse_duration_secs)
                .map(|secs| Duration::from_secs(secs.saturating_add(WAIT_READ_TIMEOUT_EXTRA)));
            let agent = build_agent(wait_read_timeout);
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/jobs/wait"),
                    Some(&wait_body(args)),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Review(args) => {
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &job_url(&base, &args.job_id, "/review"),
                    Some(&review_body(args)),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Commit(args) => {
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &job_url(&base, &args.job_id, "/commit"),
                    Some(&commit_body(args)),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Discard(args) => {
            let agent = build_agent(Some(LONG_READ_TIMEOUT));
            exit_output(
                &call(
                    &agent,
                    "POST",
                    &job_url(&base, &args.job_id, "/discard"),
                    Some(&discard_body(args)),
                    json_mode,
                ),
                json_mode,
            )
        }
        Commands::Export(args) => export(&base, args, json_mode),
        Commands::Auth(args) => {
            // Auth waits on a human -- no read timeout.
            let agent = build_agent(None);
            match run_auth_flow(&agent, &base, args.code.as_deref()) {
                Ok(()) => exit(0),
                Err(err) => {
                    eprintln!("Error: {err:?}");
                    exit(1);
                }
            }
        }
        Commands::Cancel => {
            // 200: the server killed the parked flow and cleared its slot.
            let agent = build_agent(Some(ordinary_read_timeout()));
            let output = auth_cancel(&agent, &base);
            println!("Authentication flow cancelled (the server's auth slot is free again).");
            render_exit(&output)
        }
        Commands::Watch(args) => {
            // A pure client-side SSE consumer; the local deadline (when
            // --timeout is given) is enforced on the main thread over an
            // unbounded recv, so no per-read socket timeout is needed (the
            // server's 15s keepalive keeps a live stream open).
            let deadline = match &args.timeout {
                Some(raw) => match parse_duration_secs(raw) {
                    Some(secs) => Some(Instant::now() + Duration::from_secs(secs)),
                    None => fatal(
                        &format!(
                            "Invalid duration format '{}' for --timeout (e.g. 30m, 2h)",
                            raw
                        ),
                        false,
                    ),
                },
                None => None,
            };
            let agent = build_agent(None);
            crate::cli::events_cli::watch(&agent, &base, &args.job_id, deadline)
        }
        Commands::RemoteControl(args) => {
            let agent = build_agent(Some(remote_control_read_timeout()));
            let output = match args.action {
                RemoteControlAction::Enable => call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/remote-control/enable"),
                    None,
                    json_mode,
                ),
                RemoteControlAction::Disable => call(
                    &agent,
                    "POST",
                    &format!("{base}{API_PREFIX}/remote-control/disable"),
                    None,
                    json_mode,
                ),
                RemoteControlAction::Status => call(
                    &agent,
                    "GET",
                    &format!("{base}{API_PREFIX}/remote-control/status"),
                    None,
                    json_mode,
                ),
            };
            exit_output(&output, json_mode)
        }
    }
}

// ---------------------------------------------------------------------------
// auth: the server drives the flow; agy owns the credentials
// ---------------------------------------------------------------------------

/// `agent-run auth` (and the first step of `init`): check the server's
/// auth status (`GET /auth/status`); when unauthenticated, start the flow
/// (`POST /auth`), print the OAuth URL + agy's prompt exactly once, get
/// the code (`--code` or one line of interactive stdin), and relay it
/// (`POST /auth/code`). agy itself writes the finished credentials into
/// the server's home -- the client installs nothing and no credential
/// bytes cross the wire. When the start is refused because a flow is
/// already parked on the server (409) and the code is already in hand,
/// the client skips the prompt (already printed by the client that
/// started the flow) and relays the code directly.
pub(crate) fn run_auth_flow(agent: &ureq::Agent, base: &str, code: Option<&str>) -> Result<()> {
    if auth_status(agent, base)? {
        println!("Already authenticated (the server holds valid agy credentials).");
        return Ok(());
    }
    let start = match auth_start(agent, base) {
        Ok(start) => Some(start),
        // A flow is already parked on the server (409) and the code is in
        // hand: skip the (already printed) prompt and relay the code.
        Err(AuthStartError::InProgress(_message)) if code.is_some() => None,
        Err(AuthStartError::InProgress(message)) | Err(AuthStartError::Fatal(message)) => {
            // Hard error with the exact rendering the http_error path
            // used (the server's message verbatim, no quoting).
            eprintln!("Error: {message}");
            exit(1);
        }
    };
    if let Some(start) = start {
        if start.authenticated {
            // Credentials became valid between the status check and the start.
            println!("Already authenticated (the server holds valid agy credentials).");
            return Ok(());
        }
        // Print the URL + agy's prompt exactly once (the prompt lines
        // carry the URL line agy printed; fall back to the raw URL if not)
        let url = start.oauth_url.clone().unwrap_or_default();
        if !url.is_empty() && !start.prompt_lines.iter().any(|l| l.contains(&url)) {
            println!("{url}");
        }
        for line in &start.prompt_lines {
            println!("{line}");
        }
    }
    let code = acquire_code(code, io::stdin().is_terminal())?;
    let output = auth_submit_code(agent, base, &code)?;
    for line in &output.stdout_lines {
        println!("{line}");
    }
    for line in &output.stderr_lines {
        eprintln!("{line}");
    }
    if output.exit_code != 0 {
        bail!("auth flow finished with exit code {}", output.exit_code);
    }
    if output.stdout_lines.is_empty() {
        println!("Authentication completed.");
    }
    Ok(())
}

/// `GET /auth/status` -- whether the server's home already holds valid agy
/// credentials (the server answers without spawning agy).
fn auth_status(agent: &ureq::Agent, base: &str) -> Result<bool> {
    let output = call(
        agent,
        "GET",
        &format!("{base}{API_PREFIX}/auth/status"),
        None,
        false,
    );
    output
        .data
        .get("authenticated")
        .and_then(Value::as_bool)
        .context("the server's auth status response carried no authenticated flag")
}

/// The machine-readable part of a `POST /auth` response: whether the
/// server is already authenticated, the OAuth URL when a flow was started,
/// and the prompt/URL lines agy printed so far (printed verbatim).
#[derive(Debug)]
pub(crate) struct AuthStartResponse {
    pub authenticated: bool,
    pub oauth_url: Option<String>,
    pub prompt_lines: Vec<String>,
}

/// Why a `POST /auth` start failed: a flow is already parked on the
/// server (the code can be relayed directly when already in hand), or a
/// fatal failure with the client-facing message the contract's error UX
/// would print.
#[derive(Debug)]
pub(crate) enum AuthStartError {
    /// 409: a flow is already in progress on the server.
    InProgress(String),
    /// Any other failure, with the client-facing message.
    Fatal(String),
}

/// `POST /auth` -- start (or no-op) the flow; the server blocks until the
/// OAuth URL is captured (its own deadline), so this call can take a
/// while. A 409 (a flow is already parked) is the typed in-progress error
/// so the caller can fall through to the code relay.
fn auth_start(agent: &ureq::Agent, base: &str) -> Result<AuthStartResponse, AuthStartError> {
    let url = format!("{base}{API_PREFIX}/auth");
    let request = agent
        .post(&url)
        .set("Content-Type", "application/json")
        .send_json(json!({}));
    match request {
        Ok(response) => match response.into_json::<CommandOutput>() {
            Ok(output) => {
                let authenticated =
                    output.data.get("authenticated").and_then(Value::as_bool) == Some(true);
                let oauth_url = output
                    .data
                    .get("oauth_url")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if !authenticated && oauth_url.is_none() {
                    return Err(AuthStartError::Fatal(
                        "the server's auth response carried no oauth_url".to_string(),
                    ));
                }
                Ok(AuthStartResponse {
                    authenticated,
                    oauth_url,
                    prompt_lines: output.stdout_lines,
                })
            }
            Err(err) => Err(AuthStartError::Fatal(format!(
                "invalid CommandOutput in response from agent-run API at {url}: {err}"
            ))),
        },
        Err(ureq::Error::Status(409, response)) => {
            let text = response.into_string().unwrap_or_default();
            Err(AuthStartError::InProgress(error_message_from_body(
                &text, &url,
            )))
        }
        Err(err) => Err(AuthStartError::Fatal(failure_message(&url, err).0)),
    }
}

/// `POST /auth/code` -- relay the user's code to the in-progress agy child
/// (fixed path, no session id) and wait for agy to finish.
fn auth_submit_code(agent: &ureq::Agent, base: &str, code: &str) -> Result<CommandOutput> {
    Ok(call(
        agent,
        "POST",
        &format!("{base}{API_PREFIX}/auth/code"),
        Some(&auth_code_body(code)),
        false,
    ))
}

/// The `POST /auth/code` body.
pub(crate) fn auth_code_body(code: &str) -> Value {
    json!({ "code": code })
}

/// `POST /auth/cancel` -- kill the parked agy child on the server and
/// clear its slot so a fresh flow can start. On 200 the response is the
/// server's CommandOutput with `data.cancelled == true`; non-2xx and
/// transport errors are fatal in the contract's error format (a 404 no
/// flow carries the server's exact error text).
fn auth_cancel(agent: &ureq::Agent, base: &str) -> CommandOutput {
    call(
        agent,
        "POST",
        &format!("{base}{API_PREFIX}/auth/cancel"),
        None,
        false,
    )
}

/// Gets the auth code: `--code` wins; otherwise one line from an
/// interactive stdin (the prompt goes to stderr to keep stdout clean, like
/// the old local CLI). Non-interactive stdin without `--code` is a clear
/// error.
fn acquire_code(code_flag: Option<&str>, stdin_is_tty: bool) -> Result<String> {
    if let Some(code) = code_flag {
        let trimmed = code.trim();
        if trimmed.is_empty() {
            bail!("no auth code provided (use --code or provide it on stdin)");
        }
        return Ok(trimmed.to_string());
    }
    if !stdin_is_tty {
        bail!("no auth code provided (use --code or provide it on stdin)");
    }
    eprint!("Paste the code: ");
    io::stderr().flush()?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .context("failed to read the auth code from stdin")?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        bail!("no auth code provided (use --code or provide it on stdin)");
    }
    Ok(trimmed.to_string())
}

// ---------------------------------------------------------------------------
// Request / response plumbing
// ---------------------------------------------------------------------------

/// Trims the user-supplied base URL and strips trailing slashes.
fn normalize_base_url(base: &str) -> String {
    let trimmed = base.trim();
    if trimmed.is_empty() {
        fatal(
            "--remote requires a non-empty base URL (e.g. http://127.0.0.1:8080)",
            false,
        );
    }
    trimmed.trim_end_matches('/').to_string()
}

/// Builds `/api/v1/jobs/<id><subpath>` with a percent-encoded job id.
fn job_url(base: &str, job_id: &str, subpath: &str) -> String {
    format!(
        "{base}{API_PREFIX}/jobs/{job_id}{subpath}",
        job_id = urlencode(job_id)
    )
}

/// Percent-encodes a string for safe use in a URL path segment or query
/// value (unreserved characters pass through).
pub(crate) fn urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Performs an HTTP request whose 2xx response body is a [`CommandOutput`].
/// Non-2xx and transport errors are fatal in the contract's error format.
fn call(
    agent: &ureq::Agent,
    method: &str,
    url: &str,
    body: Option<&Value>,
    json_mode: bool,
) -> CommandOutput {
    let request = match body {
        Some(value) => agent
            .request(method, url)
            .set("Content-Type", "application/json")
            .send_json(value),
        None => agent.request(method, url).call(),
    };

    match request {
        Ok(response) => match response.into_json::<CommandOutput>() {
            Ok(output) => output,
            Err(err) => fatal(
                &format!("invalid CommandOutput in response from agent-run API at {url}: {err}"),
                json_mode,
            ),
        },
        Err(err) => http_error(url, err, json_mode),
    }
}

/// Streams a response body to `writer` using `std::io::copy` and flushes at
/// the end. Avoids buffering the entire body in memory (e.g. large logs or diffs).
fn stream_response<W: Write>(response: ureq::Response, mut writer: W) -> io::Result<u64> {
    let mut reader = response.into_reader();
    let copied = io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    Ok(copied)
}

/// Streams a raw-text response body to stdout and exits 0 (used for the
/// `log` and `diff` endpoints, which return bytes rather than a
/// CommandOutput so the client can write them byte-identically).
///
/// Uses `stream_response` to avoid buffering the full body in memory (logs
/// and diffs can be many MiB on a live agent session). A broken pipe on
/// stdout (e.g. `agent-run log <id> | head`) is treated as a quiet exit 0.
///
/// If the response carries the diff fallback note header it is printed to
/// stderr after the body (the log endpoint never sets it).
fn raw_to_stdout(agent: &ureq::Agent, url: &str) -> ! {
    match agent.get(url).call() {
        Ok(response) => {
            let diff_note = response
                .header(crate::core::output::DIFF_NOTE_HEADER)
                .map(str::to_string);
            let stdout = io::stdout();
            match stream_response(response, stdout) {
                Ok(_) => {
                    if let Some(note) = diff_note {
                        eprintln!("{note}");
                    }
                    exit(0);
                }
                Err(err) if is_broken_pipe(&err) => {
                    // Consumer closed early (e.g. `head`): treat as success
                    // (the note is skipped: the consumer is gone).
                    exit(0);
                }
                Err(err) => fatal(
                    &format!("failed to stream response body from {url}: {err}"),
                    false,
                ),
            }
        }
        Err(err) => http_error(url, err, false),
    }
}

/// Returns `true` when `err` is a broken-pipe error (errno EPIPE / Windows
/// ERROR_BROKEN_PIPE).  Used to suppress the "failed to write to stdout"
/// panic when a downstream consumer (e.g. `head`) closes early.
fn is_broken_pipe(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}

/// Maps a failed request to the contract's error UX:
/// - non-2xx: `Error: <error field from the {"error": ...} body>` + exit 1
/// - transport failure: `Error: cannot reach agent-run API at <url>: <reason>` + exit 1
///
/// When `--json` is set an additional single-line JSON object
/// `{"error": ..., "code": ...}` is written to stderr (the "code" is the
/// API's machine-readable error code, or "internal" when absent). When
/// `AGENT_RUN_DEBUG` is set (any non-empty value except "0"), a diagnostic
/// line `agent-run: HTTP <status> (code <code>): <message>` is written to
/// stderr instead, exposing the status and code behind the failure.
pub(crate) fn http_error(url: &str, err: ureq::Error, json_mode: bool) -> ! {
    let (message, status, code) = failure_message(url, err);
    eprintln!("Error: {message}");
    if json_mode {
        eprintln!(
            "{}",
            json!({ "error": message, "code": code.unwrap_or_else(|| "internal".to_string()) })
        );
    } else if debug_env_on() {
        if let Some(status) = status {
            eprintln!(
                "agent-run: HTTP {status} (code {}): {message}",
                code.unwrap_or_else(|| "internal".to_string())
            );
        }
    }
    exit(1);
}

/// The client-facing message for a failed request plus the HTTP status
/// (when the server answered) and machine-readable error code (from the
/// response body's "code" field, when present). http_error prints the
/// message and exits 1; the auth fallthrough paths surface the message the
/// same way.
fn failure_message(url: &str, err: ureq::Error) -> (String, Option<u16>, Option<String>) {
    match err {
        ureq::Error::Status(code, response) => {
            let text = response.into_string().unwrap_or_default();
            let value = serde_json::from_str::<Value>(&text).ok();
            let body_code = value
                .as_ref()
                .and_then(|v| v.get("code"))
                .and_then(Value::as_str)
                .map(str::to_string);
            (error_message_from_body(&text, url), Some(code), body_code)
        }
        ureq::Error::Transport(transport) => (
            format!("cannot reach agent-run API at {url}: {transport}"),
            None,
            None,
        ),
    }
}

/// Extracts the human-facing error from a non-2xx response body: the
/// `error` field of an `{"error": ...}` JSON object when present,
/// otherwise the raw (trimmed) body text.
fn error_message_from_body(body: &str, url: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            let trimmed = body.trim();
            if trimmed.is_empty() {
                format!("agent-run API request to {url} failed")
            } else {
                trimmed.to_string()
            }
        })
}

/// Prints a [`CommandOutput`] exactly like the old local CLI and exits with
/// its exit code.
fn render_exit(output: &CommandOutput) -> ! {
    let code = output.print();
    exit(code);
}

/// Like `render_exit`, but under `--json` prints the command's JSON `data`
/// payload (pretty-printed) to stdout instead of the human-readable lines.
/// Exits with the same exit code either way, so `wait` keeps its 0/2/3
/// codes under --json.
fn exit_output(output: &CommandOutput, json_mode: bool) -> ! {
    let code = if json_mode {
        let pretty = serde_json::to_string_pretty(&output.data).expect("data is always valid JSON");
        write_stdout(pretty.as_bytes());
        write_stdout(b"\n");
        output.exit_code
    } else {
        output.print()
    };
    exit(code);
}

/// Writes bytes to stdout and flushes (the old local CLI flushed after every
/// chunk for `tail`-style streaming).
pub(crate) fn write_stdout(bytes: &[u8]) {
    let mut stdout = io::stdout();
    if let Err(err) = stdout.write_all(bytes) {
        fatal(&format!("failed to write to stdout: {err}"), false);
    }
    let _ = stdout.flush();
}

/// Prints `Error: <message>` and exits 1 (plus a JSON error line on stderr
/// in --json mode).
fn fatal(message: &str, json_mode: bool) -> ! {
    eprintln!("Error: {message}");
    if json_mode {
        eprintln!("{}", json!({ "error": message, "code": "internal" }));
    }
    exit(1);
}

// ---------------------------------------------------------------------------
// tail: client-side poll loop (§4.3)
// ---------------------------------------------------------------------------

/// Result of a `log?offset=` read.
enum LogRead {
    /// New bytes plus the offset to resume from (from the
    /// `Agent-Run-Log-Offset` response header).
    Bytes(Vec<u8>, u64),
    /// 404: log file does not (yet) exist. Carries the server's error text.
    Missing(String),
}

fn read_log(agent: &ureq::Agent, url: &str, json_mode: bool) -> LogRead {
    match agent.get(url).call() {
        Ok(response) => {
            let new_pos = response
                .header(LOG_OFFSET_HEADER)
                .and_then(|header| header.parse::<u64>().ok())
                .unwrap_or(0);
            let mut body = Vec::new();
            if let Err(err) = response.into_reader().read_to_end(&mut body) {
                fatal(
                    &format!("failed to read log response body from {url}: {err}"),
                    json_mode,
                );
            }
            LogRead::Bytes(body, new_pos)
        }
        Err(ureq::Error::Status(404, response)) => {
            let text = response.into_string().unwrap_or_default();
            LogRead::Missing(error_message_from_body(&text, url))
        }
        Err(err) => http_error(url, err, json_mode),
    }
}

/// Whether the tail poll loop should check the job's status on this tick.
/// Same rhythm as the old local tail loop: first tick plus every 7th tick
/// (~1s at the 150ms poll interval).
fn tail_status_check_due(poll_count: u64) -> bool {
    poll_count.is_multiple_of(7) || poll_count == 1
}

/// Renders a byte count in the export summary, e.g. `4.2 KiB`.
fn human_size(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = n as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// Anything occupying the export output path counts as occupied: a file, a
/// directory, or even a dangling symlink. `symlink_metadata` (unlike
/// `Path::exists()`) does not follow the link, so a dangling symlink is not
/// treated as missing -- which is exactly what let the old
/// exists-then-`File::create` flow follow the link and truncate the target.
fn out_target_occupied(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

/// Outcome of [`download_to_out`].
#[derive(Debug)]
enum DownloadOutcome {
    /// Full stream written and the temp file renamed onto the target.
    Wrote(u64),
    /// Existing target (re)appeared while streaming, without `--force`.
    /// The temp file has been removed and `out_path` is untouched.
    Refused,
}

/// Streams `source` into a sibling temp file `<out_path>.partial` opened
/// with `create_new` (so it can never truncate or follow an existing
/// target), and renames it over `out_path` only once the whole stream was
/// written and flushed. Without `force`, an existing target is refused both
/// up front and re-checked right before the rename (closing the TOCTOU
/// window of the caller's pre-request check); on any error -- or a refused
/// target -- the partial file is removed, so a mid-stream failure leaves
/// `out_path` intact and no `.partial` behind (this is what `--force` used
/// to break: the target was truncated before the download started).
fn download_to_out(
    out_path: &str,
    source: &mut impl Read,
    force: bool,
) -> io::Result<DownloadOutcome> {
    let out = Path::new(out_path);
    let partial = format!("{out_path}.partial");

    // Refuse an existing target immediately (a file, directory, or even a
    // dangling symlink) when --force is absent; the pre-request check in
    // `export` uses the same predicate, and the gate below re-checks it
    // again right before the rename.
    if !force && out_target_occupied(out) {
        return Ok(DownloadOutcome::Refused);
    }

    let mut file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)
    {
        Ok(f) => f,
        // A stale partial file (e.g. from a killed run) is a plain error;
        // the caller turns it into a user-facing message.
        Err(err) => {
            return Err(io::Error::other(format!(
                "failed to create '{partial}': {err}"
            )))
        }
    };

    let copied = match io::copy(source, &mut file) {
        Ok(copied) => copied,
        Err(err) => {
            let _ = std::fs::remove_file(&partial);
            return Err(err);
        }
    };
    if let Err(err) = file.flush() {
        let _ = std::fs::remove_file(&partial);
        return Err(err);
    }

    // Re-check right before the rename (closes the TOCTOU window of the
    // pre-request check and catches a racing writer).
    if !force && out_target_occupied(out) {
        let _ = std::fs::remove_file(&partial);
        return Ok(DownloadOutcome::Refused);
    }

    match std::fs::rename(&partial, out) {
        Ok(()) => Ok(DownloadOutcome::Wrote(copied)),
        Err(err) => {
            let _ = std::fs::remove_file(&partial);
            Err(err)
        }
    }
}

/// The user-facing refusal message (used when a target was refused instead
/// of overwritten).
fn refuse_message(out_path: &str) -> String {
    format!("refusing to overwrite existing file '{out_path}': pass --force to overwrite it")
}

/// `export`: fetch the job's committed result from the server (a raw
/// octet-stream on `/export`) and stream it to a local file. The caller's cwd
/// does NOT need to be a git repo -- all git work is server-side; this only
/// writes the downloaded bytes. Client-local `--out`/`--force` never reach the
/// server. `Committed N file(s)` is printed only when the server committed
/// uncommitted changes on the client's behalf (reported via the
/// `X-Agent-Run-Export-Committed-Files` header).
fn export(base: &str, args: &ExportArgs, json_mode: bool) -> ! {
    // Refuse to clobber an existing output file unless --force.
    let default_name = if args.tar {
        format!("{job_id}.tar", job_id = args.job_id)
    } else {
        format!("{job_id}.patch", job_id = args.job_id)
    };
    let path = match args.out.as_deref() {
        Some(p) => p.to_string(),
        None => default_name,
    };
    // Refuse to clobber an existing output file (file, directory, or even a
    // dangling symlink -- `out_target_occupied` uses `symlink_metadata`)
    // unless --force. The download re-checks this right before the rename.
    if !args.force && out_target_occupied(Path::new(&path)) {
        fatal(&refuse_message(&path), json_mode);
    }

    let agent = build_agent(Some(LONG_READ_TIMEOUT));
    let url = job_url(
        base,
        &args.job_id,
        &format!("/export{}", export_query(args)),
    );
    let response = match agent.get(&url).call() {
        Ok(response) => response,
        Err(err) => http_error(&url, err, json_mode),
    };

    let committed_files: u64 = response
        .header("X-Agent-Run-Export-Committed-Files")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let committed = committed_files > 0;
    let commits: u64 = response
        .header("X-Agent-Run-Export-Commits")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let files: u64 = response
        .header("X-Agent-Run-Export-Files")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let insertions: u64 = response
        .header("X-Agent-Run-Export-Insertions")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let deletions: u64 = response
        .header("X-Agent-Run-Export-Deletions")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let deleted_paths: u64 = response
        .header("X-Agent-Run-Export-Deleted")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let project = response
        .header("X-Agent-Run-Export-Project")
        .map(str::to_string)
        .unwrap_or_default();

    let mut reader = response.into_reader();
    let bytes = match download_to_out(&path, &mut reader, args.force) {
        Ok(DownloadOutcome::Wrote(b)) => b,
        Ok(DownloadOutcome::Refused) => fatal(&refuse_message(&path), json_mode),
        Err(err) => fatal(&format!("failed to write to {path}: {err}"), json_mode),
    };

    if json_mode {
        let payload = json!({
            "file": path,
            "commits": commits,
            "files": files,
            "insertions": insertions,
            "deletions": deletions,
            "deleted_paths": deleted_paths,
            "bytes": bytes,
            "committed": committed,
        });
        match serde_json::to_string_pretty(&payload) {
            Ok(s) => println!("{s}"),
            Err(_) => println!("{payload}"),
        }
        exit(0);
    }

    if committed {
        println!(
            "  Committed {} file(s) in job {}.",
            committed_files, args.job_id
        );
    }
    println!(
        "  Wrote {}  ({} commit(s), {} file(s), +{} -{}, {})",
        path,
        commits,
        files,
        insertions,
        deletions,
        human_size(bytes)
    );
    println!();
    println!("  To apply and push from your clone of {}:", project);
    println!("    git switch -c agent-run/{} origin/main", args.job_id);
    if args.tar {
        println!("    tar -xf {} && git add -A && git commit", path);
        if deleted_paths > 0 {
            println!(
                "    Note: a tar cannot delete files; also remove the {} path(s) listed in .agent-run-deleted",
                deleted_paths
            );
        }
    } else {
        println!("    git am {}", path);
    }
    println!("    git push -u origin HEAD");
    exit(0);
}

/// Follows a job's log until it reaches a terminal state (or the optional
/// deadline elapses). Exit codes, like `wait`:
/// - 0: the job ended Done
/// - 2: the job ended in another terminal state (failed, killed, ...)
/// - 3: `--timeout` elapsed while the job was still running
///
/// tail is raw-text: the `--json` flag is ignored entirely (text output,
/// regular error format).
fn tail(agent: &ureq::Agent, base: &str, job_id: &str, deadline: Option<Instant>) -> ! {
    let log_url = |offset: u64| -> String {
        format!(
            "{base}{API_PREFIX}/jobs/{}/log?offset={offset}",
            urlencode(job_id)
        )
    };

    // 1. Resolve the job: a 404 surfaces the server's "not found" message
    //    (same text as the old local CLI).
    call(agent, "GET", &job_url(base, job_id, ""), None, false);

    // 2. Wait for the log to appear (the job may not have written its first
    //    bytes yet): 100ms sleeps, up to 20 retries, then surface the
    //    server's "No log file found" error (same message as local).
    let mut pos: u64 = 0;
    let mut retries: u32 = 0;
    loop {
        match read_log(agent, &log_url(pos), false) {
            LogRead::Bytes(bytes, new_pos) => {
                write_stdout(&bytes);
                pos = new_pos;
                break;
            }
            LogRead::Missing(error) => {
                retries += 1;
                if retries >= TAIL_LOG_WAIT_RETRIES {
                    fatal(&error, false);
                }
                std::thread::sleep(TAIL_LOG_WAIT_INTERVAL);
            }
        }
    }

    // 3. Poll for new bytes; check the job status on the old local tail's
    //    cadence (first tick plus every 7th tick).
    let mut poll_count: u64 = 0;
    loop {
        if let LogRead::Bytes(bytes, new_pos) = read_log(agent, &log_url(pos), false) {
            write_stdout(&bytes);
            pos = new_pos;
        }
        // A 404 here means the log was purged mid-tail: no new data, keep
        // polling status.

        poll_count += 1;
        if tail_status_check_due(poll_count) {
            let status_output = call(agent, "GET", &job_url(base, job_id, ""), None, false);
            if let Some(status_str) = status_output.data.get("status").and_then(Value::as_str) {
                if let Some(status) = JobStatus::parse(status_str) {
                    if status != JobStatus::Running {
                        // Final drain of any bytes written just before termination.
                        if let LogRead::Bytes(bytes, _) = read_log(agent, &log_url(pos), false) {
                            write_stdout(&bytes);
                        }
                        eprintln!("\n[Job {job_id} transitioned to terminal state: {status}]");
                        // Exit like `wait`: 0 for Done, 2 for any other
                        // terminal state.
                        exit(if status == JobStatus::Done { 0 } else { 2 });
                    }
                    // The job is still running; if the user's --timeout
                    // deadline has passed, give up and report it.
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            eprintln!(
                                "\nTimed out waiting for the job to finish: job still running (exit code 3)."
                            );
                            exit(3);
                        }
                    }
                }
            }
        }

        std::thread::sleep(TAIL_POLL_INTERVAL);
    }
}

// ---------------------------------------------------------------------------
// Request bodies: mirror the arg structs field by field (snake_case),
// omitting unset (None / empty) fields.
// ---------------------------------------------------------------------------

/// `start` body. The `@file` prompt and `--from-patch` file are resolved to
/// literal text against the caller's cwd on the client side.
pub(crate) fn start_body(args: &StartArgs, caller_cwd: &Path) -> Result<Value> {
    let prompt = resolve_prompt(&args.prompt, caller_cwd)?;

    let from_patch_content: Option<String> = if let Some(patch) = &args.from_patch {
        let patch_path = if patch.is_absolute() {
            patch.clone()
        } else {
            caller_cwd.join(patch)
        };
        if !patch_path.exists() {
            bail!("Patch file not found: {}", patch_path.display());
        }
        Some(
            std::fs::read_to_string(&patch_path)
                .with_context(|| format!("Failed to read patch file {}", patch_path.display()))?,
        )
    } else {
        None
    };

    let mut body = json!({
        "project": args.project,
        "prompt": prompt,
        "sandbox": args.sandbox,
    });
    let obj = body.as_object_mut().expect("object literal");
    if args.plan {
        obj.insert("plan".to_string(), json!(true));
    }
    if let Some(v) = &args.hindsight {
        obj.insert("hindsight".to_string(), json!(v));
    }
    if let Some(v) = &args.hindsight_bank {
        obj.insert("hindsight_bank".to_string(), json!(v));
    }
    if let Some(v) = &args.hindsight_query {
        obj.insert("hindsight_query".to_string(), json!(v));
    }
    if let Some(v) = &args.proxy {
        obj.insert("proxy".to_string(), json!(v));
    }
    if let Some(v) = &args.git_ref {
        obj.insert("git_ref".to_string(), json!(v));
    }
    if let Some(v) = &args.model {
        obj.insert("model".to_string(), json!(v));
    }
    if let Some(v) = &args.effort {
        obj.insert("effort".to_string(), json!(v));
    }
    if !args.add_dirs.is_empty() {
        obj.insert(
            "add_dirs".to_string(),
            json!(args
                .add_dirs
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect::<Vec<_>>()),
        );
    }
    if let Some(v) = &args.print_timeout {
        obj.insert("print_timeout".to_string(), json!(v));
    }
    if let Some(v) = &args.max_runtime {
        obj.insert("max_runtime".to_string(), json!(v));
    }
    if let Some(v) = &args.from_worktree {
        obj.insert(
            "from_worktree".to_string(),
            json!(v.to_string_lossy().to_string()),
        );
    }
    if let Some(v) = from_patch_content {
        obj.insert("from_patch_content".to_string(), json!(v));
    }
    Ok(body)
}

/// `init` body: the subcommand flags verbatim (the shim carries no global
/// config overrides; server-side config lives in the API server's own
/// `AGENT_RUN_CONFIG` environment).
pub(crate) fn init_body(args: &InitArgs) -> Value {
    let mut body = json!({});
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.from_env {
        obj.insert("from_env".to_string(), json!(v));
    }
    if let Some(v) = &args.to {
        obj.insert("to".to_string(), json!(v.to_string_lossy().to_string()));
    }
    if let Some(v) = &args.hindsight_url {
        obj.insert("hindsight_url".to_string(), json!(v));
    }
    if let Some(v) = &args.hindsight_bank {
        obj.insert("hindsight_bank".to_string(), json!(v));
    }
    if let Some(v) = &args.hindsight_api_key {
        obj.insert("hindsight_api_key".to_string(), json!(v));
    }
    if let Some(v) = &args.hindsight_profile {
        obj.insert("hindsight_profile".to_string(), json!(v));
    }
    body
}

/// `retry` body. The `--prompt` follow-up is resolved to literal text
/// against the caller's cwd (`@<file>` -> file contents) exactly like the
/// `start` prompt before it crosses the wire; omitted when not given.
pub(crate) fn retry_body(args: &RetryArgs, caller_cwd: &Path) -> Result<Value> {
    let mut body = json!({
        "job_id": args.job_id,
    });
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.hindsight {
        obj.insert("hindsight".to_string(), json!(v));
    }
    if let Some(v) = &args.hindsight_query {
        obj.insert("hindsight_query".to_string(), json!(v));
    }
    if let Some(prompt) = &args.prompt {
        obj.insert(
            "prompt".to_string(),
            json!(resolve_prompt(prompt, caller_cwd)?),
        );
    }
    Ok(body)
}

pub(crate) fn repair_body(args: &RepairArgs) -> Value {
    let mut body = json!({});
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.job_id {
        obj.insert("job_id".to_string(), json!(v));
    }
    if args.all {
        obj.insert("all".to_string(), json!(true));
    }
    body
}

pub(crate) fn gc_body(args: &GcArgs) -> Value {
    let mut body = json!({});
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.older_than {
        obj.insert("older_than".to_string(), json!(v));
    }
    if let Some(v) = &args.project {
        obj.insert("project".to_string(), json!(v));
    }
    if let Some(v) = args.max_jobs {
        obj.insert("max_jobs".to_string(), json!(v));
    }
    if args.dry_run {
        obj.insert("dry_run".to_string(), json!(true));
    }
    if args.purge_logs {
        obj.insert("purge_logs".to_string(), json!(true));
    }
    body
}

pub(crate) fn wait_body(args: &WaitArgs) -> Value {
    let mut body = json!({
        "job_ids": args.job_ids,
        "any": args.any,
        "all": args.all,
    });
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.timeout {
        obj.insert("timeout".to_string(), json!(v));
    }
    body
}

pub(crate) fn review_body(args: &ReviewArgs) -> Value {
    let mut body = json!({ "job_id": args.job_id });
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.reviewer_model {
        obj.insert("reviewer_model".to_string(), json!(v));
    }
    body
}

pub(crate) fn commit_body(args: &CommitArgs) -> Value {
    let mut body = json!({ "job_id": args.job_id });
    let obj = body.as_object_mut().expect("object literal");
    if let Some(v) = &args.message {
        obj.insert("message".to_string(), json!(v));
    }
    if !args.only.is_empty() {
        obj.insert("only".to_string(), json!(args.only));
    }
    if !args.exclude.is_empty() {
        obj.insert("exclude".to_string(), json!(args.exclude));
    }
    body
}

pub(crate) fn discard_body(args: &DiscardArgs) -> Value {
    json!({ "job_id": args.job_id, "paths": args.paths })
}

/// Query string for the raw `/diff` endpoint (empty when no flags are set,
/// otherwise leading `?` included). Repeated `--only`/`--exclude` values are
/// sent as repeated query params.
pub(crate) fn diff_query(args: &DiffArgs) -> String {
    let mut params: Vec<String> = Vec::new();
    if args.base {
        params.push("base=true".to_string());
    }
    if args.stat {
        params.push("stat=true".to_string());
    }
    for value in &args.only {
        params.push(format!("only={}", urlencode(value)));
    }
    for value in &args.exclude {
        params.push(format!("exclude={}", urlencode(value)));
    }
    if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    }
}

/// Query string for the raw `/export` endpoint (empty when no flags are set,
/// otherwise leading `?` included). Repeated `--only`/`--exclude` values are
/// sent as repeated query params, like [`diff_query`]. `--out` and `--force`
/// are client-local (they are not sent to the server).
pub(crate) fn export_query(args: &ExportArgs) -> String {
    let mut params: Vec<String> = Vec::new();
    params.push(if args.tar {
        "format=tar".to_string()
    } else {
        "format=patch".to_string()
    });
    if let Some(message) = &args.message {
        params.push(format!("message={}", urlencode(message)));
    }
    for value in &args.only {
        params.push(format!("only={}", urlencode(value)));
    }
    for value in &args.exclude {
        params.push(format!("exclude={}", urlencode(value)));
    }
    if args.allow_large {
        params.push("allow_large=true".to_string());
    }
    if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start_args() -> StartArgs {
        StartArgs {
            project: "proj".to_string(),
            prompt: "add a test".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn start_body_maps_args_to_json_with_unset_fields_omitted() {
        use crate::core::args::HindsightFlag;
        let mut args = start_args();
        args.git_ref = Some("main".to_string());
        args.model = Some("gpt-test".to_string());
        args.effort = Some("high".to_string());
        args.add_dirs = vec![
            std::path::PathBuf::from("/tmp/a"),
            std::path::PathBuf::from("/tmp b"),
        ];
        args.print_timeout = Some("30m".to_string());
        args.from_worktree = Some(std::path::PathBuf::from("/wt/one"));
        args.sandbox = true;
        args.plan = true;
        args.hindsight = Some(HindsightFlag::On);
        args.hindsight_bank = Some("bank-proj".to_string());
        args.hindsight_query = Some("short query".to_string());
        args.proxy = Some("http://proxy:8080".to_string());

        let body = start_body(&args, std::path::Path::new("/tmp")).expect("start body");
        assert_eq!(
            body,
            json!({
                "project": "proj",
                "prompt": "add a test",
                "git_ref": "main",
                "model": "gpt-test",
                "effort": "high",
                "sandbox": true,
                "add_dirs": ["/tmp/a", "/tmp b"],
                "plan": true,
                "print_timeout": "30m",
                "hindsight": "on",
                "hindsight_bank": "bank-proj",
                "hindsight_query": "short query",
                "proxy": "http://proxy:8080",
                "from_worktree": "/wt/one",
            })
        );

        // When the flag is unset, the plan/hindsight/bank fields are omitted
        // entirely
        let base_body =
            start_body(&start_args(), std::path::Path::new("/tmp")).expect("start body");
        assert!(base_body.get("plan").is_none());
        assert!(base_body.get("hindsight").is_none());
        assert!(base_body.get("hindsight_bank").is_none());
        assert!(base_body.get("hindsight_query").is_none());
        assert!(base_body.get("proxy").is_none());

        // An explicit `off` is serialized as the string "off".
        let mut off_args = start_args();
        off_args.hindsight = Some(HindsightFlag::Off);
        let off_body = start_body(&off_args, std::path::Path::new("/tmp")).expect("start body");
        assert_eq!(off_body["hindsight"], json!("off"));
    }

    #[test]
    fn start_body_resolves_at_file_prompt_and_from_patch_against_cwd() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cwd = tmp.path();

        let prompt_file = cwd.join("prompt.txt");
        std::fs::write(&prompt_file, "do the thing\n").expect("write prompt file");
        let patch_file = cwd.join("change.patch");
        let patch_text = "diff --git a/x b/x\n+hello\n";
        std::fs::write(&patch_file, patch_text).expect("write patch file");

        let mut args = start_args();
        args.prompt = format!("@{}", prompt_file.display());
        args.from_patch = Some(std::path::PathBuf::from("change.patch"));

        let body = start_body(&args, cwd).expect("start body");
        assert_eq!(body["prompt"], json!("do the thing\n"));
        assert_eq!(body["from_patch_content"], json!(patch_text));
    }

    #[test]
    fn start_body_missing_patch_file_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut args = start_args();
        args.from_patch = Some(std::path::PathBuf::from("nope.patch"));

        let err = start_body(&args, tmp.path()).expect_err("must fail");
        let message = format!("{err:?}");
        assert!(
            message.contains("Patch file not found"),
            "unexpected error: {message}"
        );
    }

    fn retry_args(prompt: Option<&str>) -> RetryArgs {
        RetryArgs {
            job_id: "abc12345".to_string(),
            prompt: prompt.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn retry_body_maps_args_to_json_without_prompt() {
        use crate::core::args::HindsightFlag;
        let mut args = retry_args(None);
        args.hindsight = Some(HindsightFlag::On);
        args.hindsight_query = Some("short query".to_string());
        let body = retry_body(&args, std::path::Path::new("/tmp")).expect("retry body");
        assert_eq!(
            body,
            json!({
                "job_id": "abc12345",
                "hindsight": "on",
                "hindsight_query": "short query",
            })
        );
        assert!(
            body.get("prompt").is_none(),
            "an unset --prompt must be omitted from the body, got: {body}"
        );

        let mut off_args = retry_args(None);
        off_args.hindsight = Some(HindsightFlag::Off);
        let off_body = retry_body(&off_args, std::path::Path::new("/tmp")).expect("retry body");
        assert_eq!(off_body["hindsight"], json!("off"));
    }

    #[test]
    fn retry_body_embeds_literal_prompt() {
        let args = retry_args(Some("now fix the flake"));
        let body = retry_body(&args, std::path::Path::new("/tmp")).expect("retry body");
        assert_eq!(body["prompt"], json!("now fix the flake"));
    }

    #[test]
    fn retry_body_resolves_at_file_prompt_against_cwd() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let prompt_file = tmp.path().join("prompt.txt");
        std::fs::write(&prompt_file, "do the follow-up\n").expect("write prompt file");

        let args = retry_args(Some(format!("@{}", prompt_file.display()).as_str()));
        let body = retry_body(&args, tmp.path()).expect("retry body");
        assert_eq!(body["prompt"], json!("do the follow-up\n"));
    }

    #[test]
    fn retry_body_missing_prompt_file_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let args = retry_args(Some("@nope.txt"));
        let err = retry_body(&args, tmp.path()).expect_err("must fail");
        let message = format!("{err:?}");
        assert!(
            message.contains("Prompt file does not exist"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn init_body_maps_flags_verbatim_and_omits_unset() {
        assert_eq!(init_body(&InitArgs::default()), json!({}));

        let args = InitArgs {
            from_env: Some("MY_TOKEN".to_string()),
            to: Some(std::path::PathBuf::from("/etc/agent-run/token")),
            hindsight_url: Some("http://hindsight:8888".to_string()),
            hindsight_bank: Some("default-bank".to_string()),
            hindsight_api_key: Some("secret".to_string()),
            hindsight_profile: Some("homelab".to_string()),
        };
        assert_eq!(
            init_body(&args),
            json!({
                "from_env": "MY_TOKEN",
                "to": "/etc/agent-run/token",
                "hindsight_url": "http://hindsight:8888",
                "hindsight_bank": "default-bank",
                "hindsight_api_key": "secret",
                "hindsight_profile": "homelab",
            })
        );
    }

    #[test]
    fn wait_body_maps_args_to_json() {
        let args = WaitArgs {
            job_ids: vec!["abc12345".to_string(), "def67890".to_string()],
            any: true,
            all: false,
            timeout: Some("5m".to_string()),
        };
        assert_eq!(
            wait_body(&args),
            json!({
                "job_ids": ["abc12345", "def67890"],
                "any": true,
                "all": false,
                "timeout": "5m",
            })
        );

        let args = WaitArgs {
            job_ids: vec!["abc12345".to_string()],
            any: false,
            all: false,
            timeout: None,
        };
        assert_eq!(
            wait_body(&args),
            json!({
                "job_ids": ["abc12345"],
                "any": false,
                "all": false,
            })
        );
    }

    #[test]
    fn gc_body_maps_args_and_omits_defaults() {
        let args = GcArgs::default();
        assert_eq!(gc_body(&args), json!({}));

        let args_full = GcArgs {
            older_than: Some("14d".to_string()),
            project: Some("myproj".to_string()),
            max_jobs: Some(10),
            dry_run: true,
            purge_logs: true,
        };
        assert_eq!(
            gc_body(&args_full),
            json!({
                "older_than": "14d",
                "project": "myproj",
                "max_jobs": 10,
                "dry_run": true,
                "purge_logs": true,
            })
        );
    }

    #[test]
    fn repair_body_maps_args_and_omits_defaults() {
        // Single-job mode: only job_id is set (the path also carries it, but
        // the body mirrors the parsed args).
        let args = RepairArgs {
            job_id: Some("abc12345".to_string()),
            all: false,
        };
        assert_eq!(repair_body(&args), json!({ "job_id": "abc12345" }));

        // --all mode: no job_id, only the all flag.
        let args = RepairArgs {
            job_id: None,
            all: true,
        };
        assert_eq!(repair_body(&args), json!({ "all": true }));

        // Clap makes exactly one of the two required, so the empty body is
        // not produced in practice -- but the builder degrades to `{}`.
        assert_eq!(repair_body(&RepairArgs::default()), json!({}));
    }

    #[test]
    fn commit_body_maps_args_to_json_with_unset_fields_omitted() {
        let args = CommitArgs {
            job_id: "abc12345".to_string(),
            message: Some("fix the flake".to_string()),
            only: vec![],
            exclude: vec![],
        };
        assert_eq!(
            commit_body(&args),
            json!({ "job_id": "abc12345", "message": "fix the flake" })
        );

        let args = CommitArgs {
            job_id: "abc12345".to_string(),
            message: None,
            only: vec!["src/main.rs".to_string(), "README.md".to_string()],
            exclude: vec!["*.log".to_string()],
        };
        assert_eq!(
            commit_body(&args),
            json!({
                "job_id": "abc12345",
                "only": ["src/main.rs", "README.md"],
                "exclude": ["*.log"],
            })
        );
    }

    #[test]
    fn discard_body_maps_args_to_json() {
        let args = DiscardArgs {
            job_id: "abc12345".to_string(),
            paths: vec!["scratch.txt".to_string(), "build/out.o".to_string()],
        };
        assert_eq!(
            discard_body(&args),
            json!({
                "job_id": "abc12345",
                "paths": ["scratch.txt", "build/out.o"],
            })
        );
    }

    #[test]
    fn diff_query_builds_repeatable_params() {
        let mut args = DiffArgs {
            job_id: "abc12345".to_string(),
            base: false,
            only: vec![],
            exclude: vec![],
            stat: false,
        };
        assert_eq!(diff_query(&args), "");

        args.stat = true;
        args.only = vec!["src/main.rs".to_string(), "doc/ guide.md".to_string()];
        args.exclude = vec!["*.log".to_string()];
        assert_eq!(
            diff_query(&args),
            "?stat=true&only=src%2Fmain.rs&only=doc%2F%20guide.md&exclude=%2A.log"
        );

        args.base = true;
        assert_eq!(
            diff_query(&args),
            "?base=true&stat=true&only=src%2Fmain.rs&only=doc%2F%20guide.md&exclude=%2A.log"
        );
    }

    #[test]
    fn error_message_from_body_prefers_json_error_field() {
        assert_eq!(
            error_message_from_body(
                r#"{"error": "Job 'abc12345' not found in /var/lib/agent-run"}"#,
                "http://127.0.0.1:8080/api/v1/jobs/abc12345"
            ),
            "Job 'abc12345' not found in /var/lib/agent-run"
        );

        // Non-JSON bodies fall back to the trimmed raw text.
        assert_eq!(
            error_message_from_body("Internal Server Error\n", "http://x"),
            "Internal Server Error"
        );
        // Empty body falls back to a generic message naming the URL.
        assert_eq!(
            error_message_from_body("   ", "http://127.0.0.1:8080/api/v1/jobs"),
            "agent-run API request to http://127.0.0.1:8080/api/v1/jobs failed"
        );
    }

    #[test]
    fn tail_status_check_matches_local_cadence() {
        // First tick plus every 7th tick (same as the old local tail loop).
        assert!(tail_status_check_due(1));
        assert!(!tail_status_check_due(2));
        assert!(!tail_status_check_due(6));
        assert!(tail_status_check_due(7));
        assert!(!tail_status_check_due(8));
        assert!(tail_status_check_due(14));
    }

    #[test]
    fn wait_exit_code_two_propagates_through_dispatch_rendering() {
        // Canned `wait` CommandOutput as the server would return it for a
        // terminal-but-not-Done outcome: exactly these lines, then exit 2.
        let mut output = CommandOutput::new(json!({
            "job_ids": ["job12345"],
            "statuses": ["failed"],
        }));
        output.push_out("Job 'job12345' reached terminal state: failed".to_string());
        output.push_err(
            "Wait finished: condition met, but the relevant job(s) are not Done (exit code 2)."
                .to_string(),
        );
        output.exit_code = 2;

        // render_exit() does output.print() then exit(code); print() is the
        // same render_to path below, aimed at the process stdout/stderr.
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        assert_eq!(output.render_to(&mut stdout, &mut stderr), 2);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "Job 'job12345' reached terminal state: failed\n"
        );
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "Wait finished: condition met, but the relevant job(s) are not Done (exit code 2).\n"
        );
    }

    #[test]
    fn urlencode_round_trips_path_and_query_values() {
        assert_eq!(urlencode("abc12345"), "abc12345");
        assert_eq!(urlencode("my-project"), "my-project");
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
        assert_eq!(urlencode("a=b&c"), "a%3Db%26c");
    }

    // -- auth: code acquisition -------------------------------------------

    #[test]
    fn auth_code_body_maps_code() {
        assert_eq!(auth_code_body("ABC123"), json!({ "code": "ABC123" }));
    }

    #[test]
    fn acquire_code_prefers_the_flag_over_stdin() {
        assert_eq!(acquire_code(Some("  ABC123 "), false).unwrap(), "ABC123");
        assert_eq!(acquire_code(Some("  ABC123 "), true).unwrap(), "ABC123");
    }

    #[test]
    fn acquire_code_non_tty_without_flag_is_a_clear_error() {
        let err = acquire_code(None, false).expect_err("non-TTY without --code must fail");
        assert!(err
            .to_string()
            .contains("no auth code provided (use --code or provide it on stdin)"));
    }

    #[test]
    fn acquire_code_empty_flag_is_a_clear_error() {
        assert!(acquire_code(Some("   "), true).is_err());
    }

    // -- parse_duration_secs ----------------------------------------------

    #[test]
    fn parse_duration_secs_handles_common_formats() {
        assert_eq!(parse_duration_secs("30m"), Some(1800));
        assert_eq!(parse_duration_secs("2h"), Some(7200));
        assert_eq!(parse_duration_secs("90s"), Some(90));
        assert_eq!(parse_duration_secs("120"), Some(120));
        assert_eq!(parse_duration_secs("1d"), Some(86400));
        assert_eq!(parse_duration_secs("  30m  "), Some(1800));
        assert_eq!(parse_duration_secs(""), None);
        assert_eq!(parse_duration_secs("abc"), None);
        assert_eq!(parse_duration_secs("30x"), None);
    }

    // -- HTTP stub (localhost; canned responses, request recording) ------

    /// Reads one HTTP request (headers + Content-Length body) from the
    /// stream; None when the stream ends first.
    fn read_http_request(stream: &mut impl std::io::Read) -> Option<String> {
        let mut buf: Vec<u8> = Vec::new();
        let mut byte = [0u8; 1];
        while buf.len() < 16384 && !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            if stream.read(&mut byte).ok()? == 0 {
                break;
            }
            buf.push(byte[0]);
        }
        let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
        let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let content_length = header_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
            })
            .flatten()
            .unwrap_or(0);
        let mut body = vec![0u8; content_length];
        std::io::Read::read_exact(stream, &mut body).ok()?;
        buf.extend_from_slice(&body);
        Some(String::from_utf8_lossy(&buf).to_string())
    }

    /// The stub's observable state: base URL, recorded requests, and the
    /// server thread handle.
    type Stub = (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
        std::thread::JoinHandle<()>,
    );

    /// Spins up a throwaway HTTP server on 127.0.0.1:0. For every request
    /// it records the (request line, rest-of-request) pair and answers via
    /// `respond(request_line, rest)` -- (status, body). Each connection is
    /// closed after its response (the client therefore re-connects per
    /// request). Stops after a request line starting with `stop_after`, or
    /// `max_connections` connections.
    fn spawn_http_stub(
        respond: impl Fn(&str, &str) -> (u16, String) + Send + 'static,
        stop_after: &str,
        max_connections: u32,
    ) -> Stub {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let base = format!("http://{}", listener.local_addr().expect("stub addr"));
        let requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let stop_after = stop_after.to_string();
        let handle = std::thread::spawn(move || {
            let mut connections = 0u32;
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    break;
                };
                connections += 1;
                let Some(request) = read_http_request(&mut stream) else {
                    continue;
                };
                let (line, rest) = match request.split_once("\r\n") {
                    Some((l, r)) => (l.to_string(), r.to_string()),
                    None => (request.clone(), String::new()),
                };
                recorded.lock().unwrap().push((line.clone(), rest.clone()));
                let (status, body) = respond(&line, &rest);
                let reason = match status {
                    200 => "OK",
                    404 => "Not Found",
                    409 => "Conflict",
                    _ => "Error",
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                if line.starts_with(&stop_after) || connections >= max_connections {
                    break;
                }
            }
        });
        (base, requests, handle)
    }

    // -- cancel: request shape --------------------------------------------

    #[test]
    fn cancel_issues_post_to_auth_cancel() {
        let (base, requests, handle) = spawn_http_stub(
            |line, _rest| {
                if line.starts_with("POST /api/v1/auth/cancel") {
                    (
                        200,
                        r#"{"stdout_lines":[],"stderr_lines":[],"exit_code":0,"data":{"cancelled":true}}"#
                            .to_string(),
                    )
                } else {
                    (404, r#"{"error":"unexpected request"}"#.to_string())
                }
            },
            "POST /api/v1/auth/cancel",
            4,
        );
        let agent = build_agent(Some(ordinary_read_timeout()));
        let output = auth_cancel(&agent, &base);
        assert_eq!(output.data["cancelled"], true);
        let recorded = requests.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, "POST /api/v1/auth/cancel HTTP/1.1");
        drop(recorded);
        handle.join().expect("stub thread");
    }

    // -- auth: 409-in-progress fallthrough ---------------------------------

    #[test]
    fn auth_start_409_is_the_typed_in_progress_error() {
        let (base, _requests, handle) = spawn_http_stub(
            |_line, _rest| {
                (
                    409,
                    r#"{"error":"authentication is already in progress on this server"}"#
                        .to_string(),
                )
            },
            "POST /api/v1/auth ",
            2,
        );
        let agent = build_agent(None);
        match auth_start(&agent, &base) {
            Err(AuthStartError::InProgress(message)) => {
                assert_eq!(
                    message,
                    "authentication is already in progress on this server"
                );
            }
            other => panic!("expected a typed 409 in-progress error, got {other:?}"),
        }
        handle.join().expect("stub thread");
    }

    #[test]
    fn run_auth_flow_falls_through_to_code_relay_on_409_in_progress() {
        // Server: unauthenticated; start -> 409 (a flow is already
        // parked); code relay -> success.
        let (base, requests, handle) = spawn_http_stub(
            |line, _rest| {
                if line.starts_with("GET /api/v1/auth/status") {
                    (
                        200,
                        r#"{"stdout_lines":[],"stderr_lines":[],"exit_code":0,"data":{"authenticated":false}}"#
                            .to_string(),
                    )
                } else if line.starts_with("POST /api/v1/auth/code") {
                    (
                        200,
                        r#"{"stdout_lines":["authentication completed successfully"],"stderr_lines":[],"exit_code":0,"data":{"authenticated":true}}"#
                            .to_string(),
                    )
                } else if line.starts_with("POST /api/v1/auth ") {
                    (
                        409,
                        r#"{"error":"authentication is already in progress on this server"}"#
                            .to_string(),
                    )
                } else {
                    (500, r#"{"error":"unexpected request"}"#.to_string())
                }
            },
            "POST /api/v1/auth/code",
            6,
        );
        let agent = build_agent(None);
        run_auth_flow(&agent, &base, Some("ABC123"))
            .expect("a 409 with the code in hand must relay the code, not error");
        let recorded = requests.lock().unwrap().clone();
        let lines: Vec<&str> = recorded.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(
            lines,
            vec![
                "GET /api/v1/auth/status HTTP/1.1",
                "POST /api/v1/auth HTTP/1.1",
                "POST /api/v1/auth/code HTTP/1.1",
            ]
        );
        // The relayed body carries the code the client already had.
        assert!(
            recorded[2].1.contains(r#""code":"ABC123""#),
            "unexpected relay body: {}",
            recorded[2].1
        );
        drop(recorded);
        handle.join().expect("stub thread");
    }

    // -- timeout tests (no real network) ----------------------------------

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // -- operator-token file: the CLI middleware ----------------------------------

    /// Runs `f` with `KUBEADP_API_TOKEN_FILE` pointing at a fresh temp file
    /// containing exactly `contents` (a trailing newline is preserved
    /// verbatim, so callers pass e.g. `"T\n"`).
    fn with_token_file<R>(contents: &str, f: impl FnOnce() -> R) -> R {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("api-token");
        std::fs::write(&path, contents).unwrap();
        std::env::set_var("KUBEADP_API_TOKEN_FILE", &path);
        let result = f();
        std::env::remove_var("KUBEADP_API_TOKEN_FILE");
        result
    }

    #[test]
    fn bearer_token_from_file_is_presented_on_requests() {
        let _lock = ENV_MUTEX.lock().unwrap();
        with_token_file("T\n", || {
            let (base, requests, handle) = spawn_http_stub(
                |_line, _rest| (200, r#"{"ok":true}"#.to_string()),
                "GET /api/v1/info ",
                1,
            );
            let agent = build_agent(Some(ordinary_read_timeout()));
            let _ = agent.get(&format!("{base}/api/v1/info")).call();
            let recorded = requests.lock().unwrap().clone();
            assert!(
                recorded[0]
                    .1
                    .to_lowercase()
                    .contains("authorization: bearer t"),
                "no bearer header in {recorded:?}"
            );
            drop(recorded);
            handle.join().expect("stub thread");
        });
    }

    #[test]
    fn no_authorization_header_when_token_file_unset() {
        let _lock = ENV_MUTEX.lock().unwrap();
        std::env::remove_var("KUBEADP_API_TOKEN_FILE");
        let (base, requests, handle) = spawn_http_stub(
            |_line, _rest| (200, r#"{"ok":true}"#.to_string()),
            "GET /api/v1/info ",
            1,
        );
        let agent = build_agent(Some(ordinary_read_timeout()));
        let _ = agent.get(&format!("{base}/api/v1/info")).call();
        let recorded = requests.lock().unwrap().clone();
        assert!(
            !recorded[0].1.to_lowercase().contains("authorization:"),
            "unexpected authorization header in {recorded:?}"
        );
        drop(recorded);
        handle.join().expect("stub thread");
    }

    #[test]
    fn unreadable_token_file_is_a_clear_error_without_the_token() {
        let _lock = ENV_MUTEX.lock().unwrap();
        std::env::set_var("KUBEADP_API_TOKEN_FILE", "/nonexistent/token-file-xyzzy");
        let err = resolve_api_token().unwrap_err();
        std::env::remove_var("KUBEADP_API_TOKEN_FILE");
        assert!(
            err.contains("/nonexistent/token-file-xyzzy"),
            "error should name the path: {err}"
        );
        assert!(
            !err.contains("secret"),
            "token must never appear in the error: {err}"
        );
    }

    #[test]
    fn empty_token_file_is_a_clear_error_without_the_token() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-token");
        std::fs::write(&path, "   \n").unwrap();
        std::env::set_var("KUBEADP_API_TOKEN_FILE", &path);
        let err = resolve_api_token().unwrap_err();
        std::env::remove_var("KUBEADP_API_TOKEN_FILE");
        assert!(
            err.contains("empty-token"),
            "error should name the path: {err}"
        );
        assert!(!err.contains("secret"), "token must never appear: {err}");
        assert!(
            err.contains("empty"),
            "should mention the file is empty: {err}"
        );
    }

    /// Test that read_timeout_env parses positive integers and rejects non-positive or invalid strings.
    #[test]
    fn read_timeout_env_handles_valid_and_invalid_values() {
        let _lock = ENV_MUTEX.lock().unwrap();

        std::env::set_var("TEST_TIMEOUT_ENV", "42");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), Some(42));

        std::env::set_var("TEST_TIMEOUT_ENV", "  100  ");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), Some(100));

        std::env::set_var("TEST_TIMEOUT_ENV", "0");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), None);

        std::env::set_var("TEST_TIMEOUT_ENV", "-5");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), None);

        std::env::set_var("TEST_TIMEOUT_ENV", "invalid");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), None);

        std::env::set_var("TEST_TIMEOUT_ENV", "");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), None);

        std::env::remove_var("TEST_TIMEOUT_ENV");
        assert_eq!(read_timeout_env("TEST_TIMEOUT_ENV"), None);
    }

    /// Spawns a local TCP listener that accepts a connection but never sends
    /// any response (simulates a hung/wedged server). Returns the port and
    /// a join handle for the listener thread.
    fn spawn_silent_server() -> (u16, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind silent server");
        let port = listener.local_addr().expect("addr").port();
        let handle = std::thread::spawn(move || {
            // Accept one connection and just hold it open (never write).
            if let Ok((_stream, _)) = listener.accept() {
                // Keep the stream alive until the thread exits (drop at end of scope).
                std::thread::sleep(Duration::from_secs(30));
            }
        });
        (port, handle)
    }

    /// A status-style call to a server that accepts but never replies
    /// must fail within approximately the configured read timeout (set tiny via env).
    #[test]
    fn timeout_wedged_server_call_fails_within_configured_timeout() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Set ordinary timeout to 1 second via env var (tiny).
        std::env::set_var("AGENT_RUN_HTTP_TIMEOUT_SECS", "1");

        let (port, _handle) = spawn_silent_server();
        let url = format!("http://127.0.0.1:{port}/api/v1/jobs/testjob");

        // Uses the configured ordinary_read_timeout() resolved from AGENT_RUN_HTTP_TIMEOUT_SECS.
        let agent = build_agent(Some(ordinary_read_timeout()));

        let start = std::time::Instant::now();
        let result = agent.get(&url).call();
        let elapsed = start.elapsed();

        // Restore env.
        std::env::remove_var("AGENT_RUN_HTTP_TIMEOUT_SECS");

        // Must have failed (timeout or connection error).
        assert!(result.is_err(), "expected a timeout error, got Ok");
        let err = result.unwrap_err();
        assert!(
            matches!(err, ureq::Error::Transport(_)),
            "expected a transport error, got: {err:?}"
        );

        // Must have failed within ~the configured timeout (1s), not hung forever.
        assert!(
            elapsed >= Duration::from_millis(900),
            "call failed too fast ({elapsed:?}); expected ~1s timeout"
        );
        assert!(
            elapsed < Duration::from_secs(4),
            "call took too long ({elapsed:?}); expected a fast timeout (~1s)"
        );

        // Existing error UX verification: URL and timeout reason in failure_message.
        let (msg, status, body_code) = failure_message(&url, err);
        assert!(
            msg.contains(&url),
            "error message should contain URL: {msg}"
        );
        assert!(status.is_none(), "transport errors carry no HTTP status");
        assert!(body_code.is_none(), "transport errors carry no body code");
        assert!(
            msg.contains("timed out"),
            "error message should indicate timeout: {msg}"
        );
    }

    /// A wait-style call to a server that responds slowly (longer than the
    /// ordinary timeout, but within the extended wait timeout) must succeed.
    #[test]
    fn timeout_slow_wait_endpoint_succeeds_within_wait_timeout() {
        use std::io::Write as _;

        let _lock = ENV_MUTEX.lock().unwrap();
        // Set ordinary timeout to 1 second via env var.
        std::env::set_var("AGENT_RUN_HTTP_TIMEOUT_SECS", "1");

        // Server that waits 1300ms before responding.
        // 1300ms is longer than the ordinary timeout (1000ms = 1s),
        // but well shorter than the wait timeout (user timeout 2s + 60s headroom = 62s).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let body = r#"{"stdout_lines":["done"],"stderr_lines":[],"exit_code":0,"data":{}}"#;
        let body_owned = body.to_string();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                std::thread::sleep(Duration::from_millis(1300));
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body_owned.len(),
                    body_owned,
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        // Wait command specifies user timeout "2s".
        // wait_read_timeout = 2s + 60s = 62s (well above 1300ms).
        let wait_timeout_arg = Some("2s");
        let wait_read_timeout = wait_timeout_arg
            .and_then(parse_duration_secs)
            .map(|secs| Duration::from_secs(secs.saturating_add(WAIT_READ_TIMEOUT_EXTRA)));
        let wait_agent = build_agent(wait_read_timeout);

        let url = format!("http://127.0.0.1:{port}/api/v1/jobs/wait");
        let result = wait_agent
            .post(&url)
            .set("Content-Type", "application/json")
            .send_json(json!({"job_ids": ["x"], "any": true, "all": false, "timeout": "2s"}));

        // Clean up env.
        std::env::remove_var("AGENT_RUN_HTTP_TIMEOUT_SECS");

        assert!(
            result.is_ok(),
            "expected success from slow wait endpoint, got: {:?}",
            result.err()
        );
        let output: CommandOutput = result.unwrap().into_json().expect("valid CommandOutput");
        assert_eq!(output.stdout_lines, vec!["done"]);
    }

    /// Connecting to a closed (no listener) port must fail quickly with a
    /// transport/connection-refused error -- not hang.
    #[test]
    fn timeout_closed_port_fails_immediately() {
        // Bind then immediately drop the listener so the port is closed.
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.local_addr().expect("addr").port()
        };
        let url = format!("http://127.0.0.1:{port}/api/v1/jobs/testjob");

        let agent = build_agent(Some(ordinary_read_timeout()));

        let start = std::time::Instant::now();
        let result = agent.get(&url).call();
        let elapsed = start.elapsed();

        assert!(result.is_err(), "expected a connection-refused error");
        assert!(
            elapsed < Duration::from_secs(4),
            "connection to closed port took too long ({elapsed:?})"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, ureq::Error::Transport(_)),
            "expected a transport (connection refused) error"
        );
        let (msg, _, _) = failure_message(&url, err);
        assert!(
            msg.starts_with(&format!("cannot reach agent-run API at {url}:")),
            "expected existing error format, got: {msg}"
        );
    }

    // -- raw_to_stdout / streaming ----------------------------------------

    /// A local server that returns a multi-MB body: the bytes received on
    /// stdout must be byte-identical to what was sent.
    #[test]
    fn raw_to_stdout_streams_large_body_correctly() {
        use std::io::Write as _;

        // Build a ~3 MiB payload.
        let payload: Vec<u8> = (0u8..=255).cycle().take(3 * 1024 * 1024).collect();
        let payload_clone = payload.clone();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload_clone.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&payload_clone);
            }
        });

        let agent = build_agent(Some(Duration::from_secs(10)));

        let url = format!("http://127.0.0.1:{port}/data");
        let response = agent.get(&url).call().expect("response");
        let mut received = Vec::new();
        stream_response(response, &mut received).expect("stream_response");
        assert_eq!(received, payload, "streamed bytes must be byte-identical");
    }

    /// A closed stdout pipe does not panic during streaming and produces a broken pipe error.
    #[test]
    fn stream_response_closed_pipe_does_not_panic() {
        use std::io::Write as _;
        use std::os::unix::net::UnixStream;

        let payload: Vec<u8> = vec![b'a'; 1024 * 1024];
        let payload_clone = payload.clone();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload_clone.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&payload_clone);
            }
        });

        let agent = build_agent(Some(Duration::from_secs(10)));
        let url = format!("http://127.0.0.1:{port}/data");
        let response = agent.get(&url).call().expect("response");

        // Create an OS pipe / socket pair and drop the read end immediately.
        let (reader, writer) = UnixStream::pair().expect("unix stream pair");
        drop(reader); // Read end is closed; writing to `writer` produces EPIPE (broken pipe).

        let result = stream_response(response, writer);
        assert!(result.is_err(), "expected broken pipe error");
        let err = result.unwrap_err();
        assert!(
            is_broken_pipe(&err),
            "expected is_broken_pipe to be true, got err: {err:?}"
        );
    }

    /// Subprocess test: running agent-run against a streaming server with closed
    /// stdout pipe exits 0 cleanly without panicking.
    #[test]
    fn raw_to_stdout_closed_pipe_binary_exits_zero() {
        use std::io::Write as _;

        if let Some(bin) = option_env!("CARGO_BIN_EXE_agent-run") {
            let payload: Vec<u8> = vec![b'z'; 1024 * 1024];
            let payload_clone = payload.clone();

            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let port = listener.local_addr().expect("addr").port();
            std::thread::spawn(move || {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf);
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        payload_clone.len()
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(&payload_clone);
                }
            });

            let mut child = std::process::Command::new(bin)
                .args([
                    "--remote",
                    &format!("http://127.0.0.1:{port}"),
                    "log",
                    "testjob",
                ])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn agent-run");

            // Drop child stdout immediately to close the pipe on the reader side.
            drop(child.stdout.take());

            let status = child.wait().expect("wait for child");
            assert_eq!(
                status.code(),
                Some(0),
                "expected exit code 0 on broken stdout pipe"
            );
        }
    }

    /// Test that broken-pipe detection works for actual IO errors.
    #[test]
    fn is_broken_pipe_identifies_broken_pipe_errors() {
        use std::io::ErrorKind;
        let broken = io::Error::new(ErrorKind::BrokenPipe, "broken pipe");
        assert!(is_broken_pipe(&broken));
        let other = io::Error::new(ErrorKind::TimedOut, "timed out");
        assert!(!is_broken_pipe(&other));
    }

    /// Test that parse_duration_secs correctly calculates wait timeout headroom.
    #[test]
    fn wait_read_timeout_adds_extra_to_user_timeout() {
        // "30m" = 1800 s; add WAIT_READ_TIMEOUT_EXTRA (60 s) = 1860 s.
        let user_secs = parse_duration_secs("30m").unwrap();
        let total = user_secs.saturating_add(WAIT_READ_TIMEOUT_EXTRA);
        assert_eq!(total, 1860);

        // No user timeout -> wait_read_timeout is None (no read timeout).
        let wait_read_timeout: Option<Duration> = None::<&str>
            .and_then(parse_duration_secs)
            .map(|secs| Duration::from_secs(secs.saturating_add(WAIT_READ_TIMEOUT_EXTRA)));
        assert!(wait_read_timeout.is_none());
    }

    // -- error code extraction (--json / AGENT_RUN_DEBUG) ------------------

    /// A non-2xx response carrying `{"error": ..., "code": ...}` must yield
    /// the message, the HTTP status, and the machine-readable code.
    #[test]
    fn failure_message_extracts_status_and_code_from_typed_error_body() {
        let _lock = ENV_MUTEX.lock().unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let body = r#"{"error":"No such job found: 'abcd1234' (no job record in /state)","code":"job_not_found"}"#;
        let body_owned = body.to_string();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let response = format!(
                    "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body_owned.len(),
                    body_owned,
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let url = format!("http://127.0.0.1:{port}/api/v1/jobs/abcd1234");
        let agent = build_agent(Some(Duration::from_secs(5)));
        let err = agent
            .get(&url)
            .call()
            .expect_err("expected a 404 from the stub");

        let (message, status, code) = failure_message(&url, err);
        assert_eq!(
            message,
            "No such job found: 'abcd1234' (no job record in /state)"
        );
        assert_eq!(status, Some(404));
        assert_eq!(code.as_deref(), Some("job_not_found"));
    }

    /// A non-JSON error body still yields the trimmed text with no code.
    #[test]
    fn failure_message_without_code_field_carries_no_code() {
        let _lock = ENV_MUTEX.lock().unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let body = "Internal Server Error\n";
        let body_owned = body.to_string();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let response = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body_owned.len(),
                    body_owned,
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let url = format!("http://127.0.0.1:{port}/api/v1/jobs/x");
        let agent = build_agent(Some(Duration::from_secs(5)));
        let err = agent
            .get(&url)
            .call()
            .expect_err("expected a 500 from the stub");

        let (message, status, code) = failure_message(&url, err);
        assert_eq!(message, "Internal Server Error");
        assert_eq!(status, Some(500));
        assert_eq!(code, None);
    }

    // -- export: download-to-file safety (TOCTOU / temp-file / --force) --

    /// Yield `payload` on the first read, then either a clean EOF (`Ok(0)`)
    /// or a mid-stream error (simulating a truncated/failed response body),
    /// depending on `fail_after_payload`.
    struct StreamSource {
        payload: Vec<u8>,
        first: bool,
        fail_after_payload: bool,
    }

    impl std::io::Read for StreamSource {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.first {
                self.first = false;
                let n = std::cmp::min(buf.len(), self.payload.len());
                buf[..n].copy_from_slice(&self.payload[..n]);
                return Ok(n);
            }
            if self.fail_after_payload {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed mid-stream",
                ))
            } else {
                Ok(0)
            }
        }
    }

    /// A mid-stream failure (with `--force`, over an existing target) must
    /// leave the pre-existing target byte-for-byte intact, report an error,
    /// and leave no `<out>.partial` behind. Under the old `File::create` flow
    /// this exact scenario truncated the old file before the download began.
    #[test]
    fn export_download_mid_stream_failure_leaves_old_file_intact() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let out = tmp.path().join("job123.patch");
        std::fs::write(&out, b"OLD CONTENT").expect("write old");

        let mut source = StreamSource {
            payload: b"new patch body (truncated".to_vec(),
            first: true,
            fail_after_payload: true,
        };
        let out_path = out.to_string_lossy().into_owned();
        let res = download_to_out(&out_path, &mut source, /* force */ true);
        assert!(res.is_err(), "mid-stream failure must be an error");

        assert_eq!(
            std::fs::read(&out).expect("old file readable"),
            b"OLD CONTENT",
            "old file must be intact after a failed download"
        );
        assert!(
            !std::path::PathBuf::from(format!("{}.partial", out.display())).exists(),
            "no .partial file may be left behind"
        );
    }

    /// Without --force an already-existing target is refused (untouched, no
    /// partial written); with --force the same download replaces the target
    /// atomically.
    #[test]
    fn export_download_refuses_existing_target_without_force() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let out = tmp.path().join("job123.tar");
        let out_path = out.to_string_lossy().into_owned();

        std::fs::write(&out, b"existing").expect("seed target");
        let mut source = StreamSource {
            payload: b"replacement".to_vec(),
            first: true,
            fail_after_payload: false,
        };
        match download_to_out(&out_path, &mut source, /* force */ false) {
            Ok(DownloadOutcome::Refused) => {}
            other => panic!("expected Refused without --force, got {other:?}"),
        }
        assert_eq!(std::fs::read(&out).expect("target readable"), b"existing");
        assert!(
            !std::path::PathBuf::from(format!("{}.partial", out.display())).exists(),
            "no partial may appear"
        );

        // --force replaces the existing target atomically.
        let mut source = StreamSource {
            payload: b"replacement".to_vec(),
            first: true,
            fail_after_payload: false,
        };
        match download_to_out(&out_path, &mut source, /* force */ true) {
            Ok(DownloadOutcome::Wrote(11)) => {}
            other => panic!("expected Wrote(11) with --force, got {other:?}"),
        }
        assert_eq!(
            std::fs::read(&out).expect("target readable"),
            b"replacement"
        );
        assert!(
            !std::path::PathBuf::from(format!("{}.partial", out.display())).exists(),
            "no partial after success"
        );
    }

    /// A successful download writes the full stream, reports the byte count,
    /// and leaves no `<out>.partial` behind.
    #[test]
    fn export_download_success_writes_and_cleans_partial() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let out = tmp.path().join("job123.patch");
        let out_path = out.to_string_lossy().into_owned();

        let mut source = StreamSource {
            payload: b"full body here".to_vec(),
            first: true,
            fail_after_payload: false,
        };
        match download_to_out(&out_path, &mut source, /* force */ false) {
            Ok(DownloadOutcome::Wrote(n)) => assert_eq!(n, 14),
            other => panic!("expected Wrote, got {other:?}"),
        }
        assert_eq!(
            std::fs::read(&out).expect("target readable"),
            b"full body here"
        );
        assert!(
            !std::path::PathBuf::from(format!("{}.partial", out.display())).exists(),
            "no partial after success"
        );
    }

    #[test]
    fn test_remote_control_read_timeout_resolution() {
        let _lock = ENV_MUTEX.lock().unwrap();

        // 1. Unset -> base 200 s
        std::env::remove_var("AGENT_RUN_HTTP_TIMEOUT_SECS");
        assert_eq!(remote_control_read_timeout(), Duration::from_secs(200));

        // 2. Smaller than 200 s -> still base 200 s
        std::env::set_var("AGENT_RUN_HTTP_TIMEOUT_SECS", "60");
        assert_eq!(remote_control_read_timeout(), Duration::from_secs(200));

        // 3. Larger than 200 s -> env value wins
        std::env::set_var("AGENT_RUN_HTTP_TIMEOUT_SECS", "250");
        assert_eq!(remote_control_read_timeout(), Duration::from_secs(250));

        std::env::remove_var("AGENT_RUN_HTTP_TIMEOUT_SECS");
    }
}

//! HTTP API for agent-run: every command of the `agent-run` CLI, served over
//! HTTP for `agent-run --remote <url>` clients.
//!
//! Handlers deserialize requests into the core's clap arg structs and call
//! the same `commands::<x>::run(config, args)` the local CLI uses, so the job
//! lifecycle (worktree creation, agy invocation, status classification,
//! hindsight recall/retain, PR/commit/clean/gc semantics) is identical by
//! construction. `log` and `diff` return raw text (mirroring local stdout);
//! `log?offset=` additionally sends the `Agent-Run-Log-Offset` response
//! header so the remote client's tail loop can resume.

pub mod auth;
#[cfg(feature = "engine")]
pub mod capabilities;
mod events;
pub mod obs;
pub mod reject;
#[cfg(feature = "engine")]
pub mod runs;
pub mod token;

pub use reject::{ApiJson, ApiPath, ApiQuery};

use crate::core::args::{
    CatArgs, CleanArgs, CommitArgs, DiffArgs, DiscardArgs, ExportArgs, FilesArgs, GcArgs,
    HindsightFlag, InfoArgs, InitArgs, JobIdArg, ListArgs, ModelsArgs, ProjectsArgs, QuotaArgs,
    RepairArgs, ResultArgs, RetryArgs, ReviewArgs, StartArgs, WaitArgs,
};
use crate::core::auth::{server_credentials_valid, AuthManager, AuthStart};
use crate::core::commands;
use crate::core::config::Config;
use crate::core::job::JobMetadata;
use crate::core::output::CommandOutput;
#[cfg(feature = "engine")]
use crate::engine::emulate::EngineRunRegistry;
#[cfg(feature = "engine")]
use crate::engine::EngineRegistry;
use anyhow::{Context, Result};
use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Extension;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Response header carrying the offset just past the bytes returned by a
/// `log?offset=` read (the remote client resumes from it).
pub const LOG_OFFSET_HEADER: &str = "Agent-Run-Log-Offset";

type AppState = Arc<Config>;

/// In-memory state tracking for remote control supervision.
#[derive(Clone, Default)]
pub struct RemoteControlState {
    pub suppressed: Arc<AtomicBool>,
}

/// Builds the axum router for all authenticated API routes (contract §4.1
/// endpoint map), gated as one sub-router by
/// [`auth::BearerAuthLayer`] (see [`obs::add_observability`], which every
/// server builds on). The auth manager (one global, created at startup; at
/// most one in-progress OAuth flow) is shared as an extension alongside the
/// `Config` state.
///
/// The returned router carries the `/api/v1/*` routes plus their state; it
/// deliberately has NO `/healthz` (that is added by [`obs::add_observability`]
/// after the auth layer, so a liveness probe never needs a token). When no
/// bearer token is configured (only the explicit `api_allow_unauthenticated`
/// path) the auth layer passes requests through untouched.
#[allow(dead_code)]
pub fn router(state: AppState, auth: Arc<AuthManager>) -> Router {
    router_with_rc(state, auth, RemoteControlState::default())
}

/// Builds the router with an explicit remote control supervisor state extension.
///
/// All `/api/v1` routes live in ONE nested sub-router (with a JSON fallback
/// for unknown `/api/v1/*` paths). [`obs::add_observability`] wraps that
/// sub-router in a single bearer-auth layer, so a route added to this group
/// is authenticated automatically -- there is no per-handler check to
/// forget. The router carries NO `/healthz` (added by
/// [`obs::add_observability`] after the auth layer). With no bearer token
/// configured (only the explicit `api_allow_unauthenticated` path) the layer
/// passes requests through untouched.
pub fn router_with_rc(
    state: AppState,
    auth: Arc<AuthManager>,
    rc_state: RemoteControlState,
) -> Router {
    // E1: the engine registry defaults to empty (no engines registered yet);
    // the role entry points pass their configured registry through
    // `router_with_engines` in E3/E5.
    let api_v1 = Router::new()
        // start (POST body) / list (GET query)
        .route("/jobs", post(start_job).get(list_jobs))
        // wait -- registered before `{job_id}` so the static segment wins
        .route("/jobs/wait", post(wait_jobs))
        // repair --all, likewise before `{job_id}`
        .route("/jobs/repair", post(repair_all))
        .route("/jobs/{job_id}", get(job_status))
        .route("/jobs/{job_id}/clean", delete(clean_job))
        .route("/jobs/{job_id}/kill", post(kill_job))
        .route("/jobs/{job_id}/retry", post(retry_job))
        .route("/jobs/{job_id}/repair", post(repair_job))
        .route("/jobs/{job_id}/commit", post(commit_job))
        .route("/jobs/{job_id}/discard", post(discard_job))
        .route("/jobs/{job_id}/export", get(export_job))
        .route("/jobs/{job_id}/review", post(review_job))
        .route("/jobs/{job_id}/log", get(log_job))
        .route("/jobs/{job_id}/diff", get(diff_job))
        .route("/jobs/{job_id}/files", get(files_job))
        .route("/jobs/{job_id}/cat", get(cat_job))
        .route("/jobs/{job_id}/result", get(result_job))
        .route("/jobs/{job_id}/events", get(events::events_job))
        .route("/projects", get(projects_cmd))
        .route("/gc", post(gc_job))
        .route("/init", post(init_job))
        // Jobset lifecycle: ending a jobset bumps its epoch and marks it ended
        // (old credentials stop verifying). Same operator-token gate as every
        // other /api/v1 route (the shared bearer layer around this sub-router).
        .route("/jobsets/{jobset_id}/end", post(end_jobset_job))
        // auth (agy-owned home credentials; the manager is an extension)
        .route("/auth/status", get(auth_status))
        .route("/info", get(info_cmd))
        .route("/models", get(models_cmd))
        .route("/quota", get(quota_cmd))
        .route("/remote-control/enable", post(remote_control_enable))
        .route("/remote-control/disable", post(remote_control_disable))
        .route("/remote-control/status", get(remote_control_status))
        .route("/auth", post(start_auth))
        .route("/auth/code", post(submit_auth_code))
        .route("/auth/cancel", post(cancel_auth))
        .fallback(api_v1_not_found)
        .with_state(state)
        .layer(Extension(auth))
        .layer(Extension(rc_state));

    #[cfg(feature = "engine")]
    let api_v1 = api_v1
        .route("/capabilities", get(capabilities::capabilities_handler))
        .route("/runs/{run_id}/interrupt", post(runs::interrupt_run))
        .route("/runs/{run_id}/pause", post(runs::pause_run))
        .route("/runs/{run_id}/compact", post(runs::compact_run))
        .route("/runs/{run_id}/fork", post(runs::fork_run))
        .route("/runs/{run_id}/revert", post(runs::revert_run))
        .route("/runs/{run_id}/todo", get(runs::todo_run))
        .layer(Extension(EngineRunRegistry::global()))
        .layer(Extension(Arc::new(EngineRegistry::new())));

    Router::new().nest("/api/v1", api_v1)
}

/// Engine-aware variant of [`router_with_rc`]: the same `/api/v1` routes plus
/// `GET /api/v1/capabilities` carrying the caller's [`EngineRegistry`] (the
/// role entry points construct it from config in E3/E5).
///
/// When the crate builds without the `engine` feature this is behaviourally
/// identical to [`router_with_rc`] except for the registry.
#[cfg(feature = "engine")]
pub fn router_with_engines(
    state: AppState,
    auth: Arc<AuthManager>,
    rc_state: RemoteControlState,
    engines: Arc<EngineRegistry>,
) -> Router {
    let api_v1 = Router::new()
        .route("/jobs", post(start_job).get(list_jobs))
        .route("/jobs/wait", post(wait_jobs))
        .route("/jobs/repair", post(repair_all))
        .route("/jobs/{job_id}", get(job_status))
        .route("/jobs/{job_id}/clean", delete(clean_job))
        .route("/jobs/{job_id}/kill", post(kill_job))
        .route("/jobs/{job_id}/retry", post(retry_job))
        .route("/jobs/{job_id}/repair", post(repair_job))
        .route("/jobs/{job_id}/commit", post(commit_job))
        .route("/jobs/{job_id}/discard", post(discard_job))
        .route("/jobs/{job_id}/export", get(export_job))
        .route("/jobs/{job_id}/review", post(review_job))
        .route("/jobs/{job_id}/log", get(log_job))
        .route("/jobs/{job_id}/diff", get(diff_job))
        .route("/jobs/{job_id}/files", get(files_job))
        .route("/jobs/{job_id}/cat", get(cat_job))
        .route("/jobs/{job_id}/result", get(result_job))
        .route("/jobs/{job_id}/events", get(events::events_job))
        .route("/projects", get(projects_cmd))
        .route("/gc", post(gc_job))
        .route("/init", post(init_job))
        // Jobset lifecycle (A2A): same operator-token gate as the other
        // /api/v1 routes, carried over into the engine-aware router.
        .route("/jobsets/{jobset_id}/end", post(end_jobset_job))
        .route("/auth/status", get(auth_status))
        .route("/info", get(info_cmd))
        .route("/models", get(models_cmd))
        .route("/quota", get(quota_cmd))
        .route("/remote-control/enable", post(remote_control_enable))
        .route("/remote-control/disable", post(remote_control_disable))
        .route("/remote-control/status", get(remote_control_status))
        .route("/auth", post(start_auth))
        .route("/auth/code", post(submit_auth_code))
        .route("/auth/cancel", post(cancel_auth))
        .route("/capabilities", get(capabilities::capabilities_handler))
        // E4 run-control endpoints (design HL-91 2.5): interrupt/pause/compact/
        // fork/revert/todo, addressed by run id through the shared run
        // registry. In E4 the routes resolve no runs in production (the
        // drivers that populate the registry land in E3/E5); contracts are
        // exercised by tests that register runs directly.
        .route("/runs/{run_id}/interrupt", post(runs::interrupt_run))
        .route("/runs/{run_id}/pause", post(runs::pause_run))
        .route("/runs/{run_id}/compact", post(runs::compact_run))
        .route("/runs/{run_id}/fork", post(runs::fork_run))
        .route("/runs/{run_id}/revert", post(runs::revert_run))
        .route("/runs/{run_id}/todo", get(runs::todo_run))
        .layer(Extension(EngineRunRegistry::global()))
        .fallback(api_v1_not_found)
        .with_state(state)
        .layer(Extension(auth))
        .layer(Extension(rc_state))
        .layer(Extension(engines));

    Router::new().nest("/api/v1", api_v1)
}

/// Fallback for the `/api/v1` sub-router: an unknown API path answers the
/// standard JSON error shape instead of an empty 404. This fallback lives
/// INSIDE the authenticated sub-router, so an unauthenticated request to an
/// unknown `/api/v1/*` path is refused by the bearer layer before it reaches
/// this handler.
async fn api_v1_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "not found", "code": "not_found" })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// Finds the first `AgentRunError` in the anyhow error chain (the typed
/// classification the core produces at the originating call site).
fn typed_error(err: &anyhow::Error) -> Option<&crate::core::error::AgentRunError> {
    err.chain()
        .find_map(|e| e.downcast_ref::<crate::core::error::AgentRunError>())
}

/// Converts a core `Err` into the contract's error body
/// (`{"error": "<message>", "code": "<code>"}` + mapped status). The message
/// is the error's Display form (the top-level message) -- never the Debug
/// form -- so any `anyhow` "Caused by:" chain or embedded absolute path stays
/// out of the client-facing body. The FULL chain (including causes) is logged
/// server-side with `eprintln!` so nothing is lost for operators. Errors
/// carrying an [`AgentRunError`](crate::core::error::AgentRunError)
/// are classified by its `kind` (404/409/400/500) and carry its stable
/// `code`; anything else (unexpected I/O/spawn/DB failures, server
/// misconfiguration) is a 500 with `code: "internal"`.
pub(crate) fn error_to_response(err: anyhow::Error) -> Response {
    // Log the whole chain (Caused-by included) server-side. The body carries
    // the typed error's own user-facing message when there is one (that is
    // the text the CLI prints, e.g. "No such job found ..."), and only the
    // top-level Display message for an untyped (internal) error, so internal
    // causes and paths never reach a client.
    eprintln!("[agent-run-api] error: {err:#}");
    let Some(typed) = typed_error(&err) else {
        // Engine-layer errors carry their own response mapping (the 501
        // `{"error", "code":"unsupported", "engine"}` contract body). Nothing
        // routes through the engine trait in E1, so this arm is reached by
        // future endpoint code only; it is tested directly.
        #[cfg(feature = "engine")]
        if let Some(engine_err) = err
            .chain()
            .find_map(|e| e.downcast_ref::<crate::engine::errors::EngineError>())
        {
            return crate::engine::errors::to_response(engine_err);
        }
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string(), "code": "internal" })),
        )
            .into_response();
    };
    let message = strip_state_path(&typed.message);
    let status =
        StatusCode::from_u16(typed.kind.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        Json(json!({ "error": message, "code": typed.code })),
    )
        .into_response()
}

/// The core's "job not found" messages embed the server-side state database
/// path (`(no job record in <state_root>/state.sqlite)`) for operators. The
/// client body must not carry that path; the full message (path included) is
/// already logged above via `{err:#}`, so nothing is lost server-side.
fn strip_state_path(message: &str) -> String {
    match message.find(" (no job record in ") {
        Some(idx) => message[..idx].to_string(),
        None => message.to_string(),
    }
}

/// Runs a blocking core command off the tokio worker threads (the core does
/// file I/O, sqlite, process spawning, and 500ms poll loops).
async fn run_blocking<T>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .expect("spawn_blocking task panicked or runtime shut down")
}

// ---------------------------------------------------------------------------
// Request bodies: mirror the clap arg structs (snake_case, unset fields
// omitted by the client -- all optional fields therefore default).
// ---------------------------------------------------------------------------

/// `start` body. Differs from the core `StartArgs` only in `from_patch`:
/// the client sends the patch *content* (`from_patch_content`, resolved
/// against the caller's cwd), which the server materializes to a temp file
/// and passes as `from_patch`.
#[derive(Debug, Deserialize)]
struct StartRequest {
    project: String,
    prompt: String,
    #[serde(default)]
    git_ref: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default)]
    sandbox: bool,
    /// Wire form of `StartArgs.add_dirs` (paths as strings).
    #[serde(default)]
    add_dirs: Vec<String>,
    /// Read-only investigation (agy plan mode); the agent must not edit.
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    print_timeout: Option<String>,
    /// One flag for all Hindsight activity: `on`, `off`, or absent (server
    /// defaults). Replaces the old per-behaviour bools.
    #[serde(default)]
    pub hindsight: Option<HindsightFlag>,
    #[serde(default)]
    pub hindsight_bank: Option<String>,
    #[serde(default)]
    pub hindsight_query: Option<String>,
    #[serde(default)]
    pub proxy: Option<String>,
    /// Wire form of `StartArgs.from_worktree` (path as string).
    #[serde(default)]
    pub from_worktree: Option<String>,
    /// File contents for `--from-patch` (client resolves the file locally).
    #[serde(default)]
    pub from_patch_content: Option<String>,
    /// Opt-in max runtime (same syntax as `wait --timeout`), wins over the
    /// server-wide `AGENT_RUN_MAX_RUNTIME` default.
    #[serde(default)]
    pub max_runtime: Option<String>,
    /// Join the job to an existing jobset (see `StartArgs::jobset_id`).
    #[serde(default)]
    pub jobset_id: Option<String>,
    /// Engine to run this job with (HL-91 E3, selection precedence level 1).
    /// `agy` (the default) uses this pod's agy job driver; `claude` names the
    /// claude engine this pod does not host yet (501); any other value is an
    /// unregistered engine the server refuses with 400.
    #[serde(default)]
    pub engine: Option<String>,
}

/// Per-process sequence for unique temp patch file names.
static PATCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Writes the client-supplied patch content to a uniquely named temp file
/// under `<state_root>/patches/`. The caller MUST delete it after
/// `start::run` returns (success or error).
fn materialize_patch(config: &Config, content: &str) -> Result<PathBuf> {
    let dir = config.state_root.join("patches");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create patch directory {}", dir.display()))?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = PATCH_SEQ.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!(
        "incoming-{nanos}-{}-{seq}.patch",
        std::process::id()
    ));
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write temp patch file {}", path.display()))?;
    Ok(path)
}

/// `log?offset=` query (offset absent or 0 = full log).
#[derive(Debug, Deserialize)]
struct LogQuery {
    #[serde(default)]
    offset: Option<u64>,
}

/// Parsed `diff?base=true&stat=true&only=<p>&exclude=<g>` query (only/exclude
/// are repeatable pathspecs).
#[derive(Debug)]
struct DiffQuery {
    base: bool,
    stat: bool,
    only: Vec<String>,
    exclude: Vec<String>,
}

/// Parses the raw `?`-less diff query string. The CLI client sends
/// `only`/`exclude` as REPEATED keys (`?only=a&only=b`), which neither axum's
/// `Query<DiffQuery>` extractor nor a plain `serde` mapping into a struct can
/// express as a `Vec<String>` -- so the raw string is parsed as a flat list of
/// `(key, value)` pairs and the keys are folded by hand. Absent keys keep the
/// historical defaults (`base`/`stat` false, empty selections).
/// Decodes one percent-encoded query key/value. `%XX` is unescaped; a bare
/// `+` is kept as a literal plus (the client percent-encodes spaces as
/// `%20`). Malformed sequences (`%Z`, a trailing `%`) are an error, because
/// the raw bytes no longer describe a valid path selection.
fn percent_decode(segment: &str) -> Result<String, String> {
    fn hex_digit(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = segment.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len()
                || hex_digit(bytes[i + 1]).is_none()
                || hex_digit(bytes[i + 2]).is_none()
            {
                return Err(format!(
                    "invalid percent-encoding in query: cannot decode '{}'",
                    segment
                ));
            }
            let hi = hex_digit(bytes[i + 1]).unwrap();
            let lo = hex_digit(bytes[i + 2]).unwrap();
            out.push(hi * 16 + lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        format!(
            "invalid percent-encoding in query: '{}' is not valid UTF-8",
            segment
        )
    })
}

fn parse_diff_query(raw: Option<&str>) -> Result<DiffQuery, String> {
    fn truthy(value: &str) -> bool {
        matches!(value, "1" | "true" | "True" | "TRUE")
    }
    let mut base = false;
    let mut stat = false;
    let mut only: Vec<String> = Vec::new();
    let mut exclude: Vec<String> = Vec::new();
    if let Some(raw) = raw {
        let raw = raw.trim();
        if !raw.is_empty() {
            // Hand-folded over the raw `&`-separated segments (rather than a
            // struct decode) so that a key may be REPEATED in any position --
            // adjacent OR interleaved with other keys (`?only=a&x=1&only=b`) --
            // and a bare key (`?base`) reads as an empty value.
            for segment in raw.split('&') {
                if segment.is_empty() {
                    continue;
                }
                let (key_raw, value_raw) = match segment.split_once('=') {
                    Some((k, v)) => (k, v),
                    None => (segment, ""),
                };
                let key = percent_decode(key_raw)?;
                let value = percent_decode(value_raw)?;
                match key.as_str() {
                    "base" => base = truthy(&value),
                    "stat" => stat = truthy(&value),
                    "only" => only.push(value),
                    "exclude" => exclude.push(value),
                    // Unknown keys are ignored (an old client that sends no
                    // selection keys, or a new one adding reserved keys, keeps
                    // working).
                    _ => {}
                }
            }
        }
    }
    Ok(DiffQuery {
        base,
        stat,
        only,
        exclude,
    })
}

/// Parsed `export?format=patch|tar&message=&only=<p>&exclude=<g>&allow_large=`
/// query. `only`/`exclude` are repeatable pathspecs (like [`DiffQuery`]);
/// `message` is present only when the client sent a non-empty value, so the
/// server can commit uncommitted changes; an empty `message=` key is treated
/// as "derive a default message" (the core's commit path).
#[derive(Debug)]
struct ExportQuery {
    format: String,
    message: Option<String>,
    only: Vec<String>,
    exclude: Vec<String>,
    allow_large: bool,
}

/// Parses the raw `?`-less export query string like [`parse_diff_query`]
/// (repeatable keys, percent-encoding). Unknown/absent keys keep defaults
/// (`format=patch`, no selection, no size override).
fn parse_export_query(raw: Option<&str>) -> Result<ExportQuery, String> {
    fn truthy(value: &str) -> bool {
        matches!(value, "1" | "true" | "True" | "TRUE")
    }
    let mut format = "patch".to_string();
    let mut message: Option<String> = None;
    let mut saw_message = false;
    let mut only: Vec<String> = Vec::new();
    let mut exclude: Vec<String> = Vec::new();
    let mut allow_large = false;
    if let Some(raw) = raw {
        let raw = raw.trim();
        if !raw.is_empty() {
            for segment in raw.split('&') {
                if segment.is_empty() {
                    continue;
                }
                let (key_raw, value_raw) = match segment.split_once('=') {
                    Some((k, v)) => (k, v),
                    None => (segment, ""),
                };
                let key = percent_decode(key_raw)?;
                let value = percent_decode(value_raw)?;
                match key.as_str() {
                    "format" => format = value,
                    // A bare `message` key (no `=`) means "commit with a
                    // derived message"; `message=` (empty value) is
                    // intentionally treated the same, so the server commits
                    // with a derived summary rather than an empty one.
                    "message" => {
                        saw_message = true;
                        if value.is_empty() {
                            message.get_or_insert(String::new());
                        } else {
                            message = Some(value);
                        }
                    }
                    "only" => only.push(value),
                    "exclude" => exclude.push(value),
                    "allow_large" => allow_large = truthy(&value),
                    _ => {}
                }
            }
        }
    }
    if message.is_none() && saw_message {
        message = Some(String::new());
    }
    Ok(ExportQuery {
        format,
        message,
        only,
        exclude,
        allow_large,
    })
}

/// Parsed `jobs?project=<p>&status=<s>` list query. `status` is repeatable
/// and each value may itself be a comma-separated list (both shapes collapse
/// to one `Vec<String>`; the core's `--status` parser splits the commas).
#[derive(Debug)]
struct ListQuery {
    project: Option<String>,
    status: Vec<String>,
}

/// Parses the raw `?`-less list query string. The CLI client sends `status`
/// as REPEATED keys (`?status=done&status=failed`) and may also URL-encode a
/// comma-separated list (`?status=done%2Cfailed`), neither of which axum's
/// `Query<ListArgs>` extractor can fold into a `Vec<String>` -- so the raw
/// string is parsed as a flat list of `(key, value)` pairs (like
/// [`parse_diff_query`]). Absent keys keep the historical defaults (no
/// project filter, no status filter). A value's commas are preserved verbatim
/// in the collected vector: the core's status filter splits them.
fn parse_list_query(raw: Option<&str>) -> Result<ListQuery, String> {
    let mut project: Option<String> = None;
    let mut status: Vec<String> = Vec::new();
    if let Some(raw) = raw {
        let raw = raw.trim();
        if !raw.is_empty() {
            for segment in raw.split('&') {
                if segment.is_empty() {
                    continue;
                }
                let (key_raw, value_raw) = match segment.split_once('=') {
                    Some((k, v)) => (k, v),
                    None => (segment, ""),
                };
                let key = percent_decode(key_raw)?;
                let value = percent_decode(value_raw)?;
                match key.as_str() {
                    // The last occurrence wins (matching the single-valued
                    // extractor's behaviour for a repeated project key).
                    "project" => project = Some(value),
                    // Repeatable: every `status=` occurrence appends, so both
                    // `?status=a&status=b` and `?status=a,b` are honoured.
                    "status" => status.push(value),
                    // Unknown keys are ignored (forward-compat with the diff
                    // route), keeping an old client that sends no filter keys
                    // working.
                    _ => {}
                }
            }
        }
    }
    Ok(ListQuery { project, status })
}

/// `files?path=&depth=` query (path defaults to the worktree root, depth is
/// clamped to 1..=4 server-side).
#[derive(Debug, Deserialize)]
struct FilesQuery {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    depth: Option<u32>,
}

/// `cat?path=&lines=&max_bytes=` query (`path` is required -- a missing
/// parameter is a 400 from the query extractor, like `DiffQuery.base`).
#[derive(Debug, Deserialize)]
struct CatQuery {
    path: String,
    #[serde(default)]
    lines: Option<String>,
    #[serde(default)]
    max_bytes: Option<u64>,
}

/// `clean?force=&purge_log=` query.
#[derive(Debug, Deserialize)]
struct CleanQuery {
    #[serde(default)]
    force: bool,
    #[serde(default)]
    purge_log: bool,
}

/// `POST /api/v1/auth/code` body.
#[derive(Debug, Deserialize)]
struct AuthCodeRequest {
    code: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /api/v1/jobs` -- start.
async fn start_job(
    State(config): State<AppState>,
    ApiJson(req): ApiJson<StartRequest>,
) -> Response {
    // HL-91 E3: engine selection for this job. The agy driver is the only job
    // engine this pod hosts, so `agy` (and an absent engine) fall through to
    // the existing command path; `claude` is a real engine this pod does not
    // host yet (501, not a selection error); anything else is an engine the
    // pod never registered (400 `unknown_engine` -- an unregistered engine is
    // a selection error, never a 501; design 2.4). Under the reduced build
    // without the `engine` feature no engine field is accepted and an explicit
    // non-`agy` value answers the same contract through the trait mapping.
    if let Some(engine) = req.engine.as_deref() {
        match engine {
            "agy" => {}
            #[cfg(feature = "engine")]
            "claude" => {
                return crate::engine::errors::to_response(
                    &crate::engine::EngineError::unsupported("claude", "job.start"),
                )
            }
            #[cfg(not(feature = "engine"))]
            "claude" => {
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(json!({
                        "error": "engine 'claude' does not support endpoint 'job.start'",
                        "code": "unsupported",
                        "engine": "claude",
                    })),
                )
                    .into_response()
            }
            other => {
                #[cfg(feature = "engine")]
                {
                    return crate::engine::errors::to_response(
                        &crate::engine::EngineError::UnknownEngine {
                            engine: other.to_string(),
                        },
                    );
                }
                #[cfg(not(feature = "engine"))]
                {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": "unknown engine",
                            "code": "unknown_engine",
                            "engine": other,
                        })),
                    )
                        .into_response();
                }
            }
        }
    }
    // Materialize --from-patch content before running (the core applies the
    // patch file path, not content); delete it when run returns, success or
    // error.
    let temp_patch = match &req.from_patch_content {
        Some(content) => match materialize_patch(&config, content) {
            Ok(path) => Some(path),
            Err(err) => return error_to_response(err),
        },
        None => None,
    };
    let args = StartArgs {
        project: req.project,
        prompt: req.prompt,
        git_ref: req.git_ref,
        model: req.model,
        effort: req.effort,
        sandbox: req.sandbox,
        add_dirs: req.add_dirs.into_iter().map(PathBuf::from).collect(),
        print_timeout: req.print_timeout,
        hindsight: req.hindsight,
        hindsight_bank: req.hindsight_bank,
        hindsight_query: req.hindsight_query,
        proxy: req.proxy,
        from_worktree: req.from_worktree.map(PathBuf::from),
        from_patch: temp_patch.clone(),
        plan: req.plan,
        max_runtime: req.max_runtime,
        jobset_id: req.jobset_id,
        engine: req.engine,
    };
    let config = Arc::clone(&config);
    let output = run_blocking(move || commands::start::run(&config, &args)).await;
    if let Some(path) = &temp_patch {
        let _ = std::fs::remove_file(path);
    }
    match output {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/info` -- server health + capability snapshot.
async fn info_cmd(State(config): State<AppState>, ApiQuery(_args): ApiQuery<InfoArgs>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::info::run(&config, &_args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs?project=<p>&status=<s>` -- list.
///
/// The query is taken as `RawQuery` (rather than `Query<ListArgs>`) because
/// `status` is a repeatable key (optionally comma-separated per value), which
/// a struct-based extractor cannot fold into `Vec<String>`; see
/// [`parse_list_query`].
async fn list_jobs(State(config): State<AppState>, RawQuery(raw_query): RawQuery) -> Response {
    let query = match parse_list_query(raw_query.as_deref()) {
        Ok(query) => query,
        // A malformed query is a client error: 400 with the parse message
        // (an unparseable query must never silently fall back to no filter).
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": message, "code": "invalid_query" })),
            )
                .into_response()
        }
    };
    let args = ListArgs {
        project: query.project,
        status: query.status,
    };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::list::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/projects` -- projects.
async fn projects_cmd(State(config): State<AppState>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::projects::run(&config, &ProjectsArgs {})).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/models` -- list the models the server's agy can use.
async fn models_cmd(State(config): State<AppState>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::models::run(&config, &ModelsArgs {})).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/quota?fresh=true|false` -- show the agy quota.
async fn quota_cmd(
    State(config): State<AppState>,
    ApiQuery(args): ApiQuery<QuotaArgs>,
) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::quota::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/remote-control/enable` -- enable and start the remote-control daemon.
async fn remote_control_enable(
    State(config): State<AppState>,
    Extension(rc_state): Extension<RemoteControlState>,
) -> Response {
    let rc_enabled = config.remote_control;
    let config = Arc::clone(&config);
    match run_blocking(move || commands::remote_control::enable(&config)).await {
        Ok(output) => {
            rc_state.suppressed.store(false, Ordering::SeqCst);
            Json(output).into_response()
        }
        Err(err) => {
            if rc_enabled {
                rc_state.suppressed.store(false, Ordering::SeqCst);
            }
            error_to_response(err)
        }
    }
}

/// `POST /api/v1/remote-control/disable` -- stop the remote-control daemon.
async fn remote_control_disable(
    State(config): State<AppState>,
    Extension(rc_state): Extension<RemoteControlState>,
) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::remote_control::disable(&config)).await {
        Ok(output) => {
            rc_state.suppressed.store(true, Ordering::SeqCst);
            Json(output).into_response()
        }
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/remote-control/status` -- status of the remote-control daemon.
async fn remote_control_status(State(config): State<AppState>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::remote_control::status(&config)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs/{id}` -- status (`{id}` may be a unique prefix).
async fn job_status(State(config): State<AppState>, ApiPath(job_id): ApiPath<String>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::status::run(&config, &JobIdArg { job_id })).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `DELETE /api/v1/jobs/{id}/clean?force=&purge_log=` -- clean.
async fn clean_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiQuery(q): ApiQuery<CleanQuery>,
) -> Response {
    let args = CleanArgs {
        job_id,
        force: q.force,
        purge_log: q.purge_log,
    };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::clean::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/{id}/kill` -- kill (no body; the job ID comes from the
/// path, which wins over any body field).
async fn kill_job(State(config): State<AppState>, ApiPath(job_id): ApiPath<String>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::kill::run(&config, &JobIdArg { job_id })).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/{id}/retry` -- retry.
async fn retry_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(mut body): ApiJson<RetryArgs>,
) -> Response {
    body.job_id = job_id; // path is authoritative
    let config = Arc::clone(&config);
    match run_blocking(move || commands::retry::run(&config, &body)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/{id}/repair` -- repair one job's orphaned worktree.
async fn repair_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(mut body): ApiJson<RepairArgs>,
) -> Response {
    body.job_id = Some(job_id); // path is authoritative
    let config = Arc::clone(&config);
    match run_blocking(move || commands::repair::run(&config, &body)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/repair` -- repair every dangling worktree (--all).
async fn repair_all(
    State(config): State<AppState>,
    ApiJson(mut body): ApiJson<RepairArgs>,
) -> Response {
    body.job_id = None;
    body.all = true;
    let config = Arc::clone(&config);
    match run_blocking(move || commands::repair::run(&config, &body)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/{id}/commit` -- commit.
async fn commit_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(mut body): ApiJson<CommitArgs>,
) -> Response {
    body.job_id = job_id; // path is authoritative
    let config = Arc::clone(&config);
    match run_blocking(move || commands::commit::run(&config, &body)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/{id}/discard` -- discard.
async fn discard_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(mut body): ApiJson<DiscardArgs>,
) -> Response {
    body.job_id = job_id; // path is authoritative
    let config = Arc::clone(&config);
    match run_blocking(move || commands::discard::run(&config, &body)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs/{id}/export?format=patch|tar` -- raw patch/tar bytes
/// (application/octet-stream) plus the export summary headers. The query is
/// taken as [`RawQuery`] because `only`/`exclude` are repeatable keys (see
/// [`parse_export_query`]).
async fn export_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let q = match parse_export_query(raw_query.as_deref()) {
        Ok(q) => q,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": message, "code": "invalid_query" })),
            )
                .into_response()
        }
    };
    let args = ExportArgs {
        job_id,
        message: q.message,
        only: q.only,
        exclude: q.exclude,
        out: None,
        tar: q.format == "tar",
        allow_large: q.allow_large,
        force: false,
    };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::export::run(&config, &args)).await {
        Ok(payload) => {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                "Content-Type",
                "application/octet-stream"
                    .parse()
                    .expect("static header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_COMMITS_HEADER,
                payload
                    .commits
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_FILES_HEADER,
                payload
                    .files
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_INSERTIONS_HEADER,
                payload
                    .insertions
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_DELETIONS_HEADER,
                payload
                    .deletions
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_DELETED_PATHS_HEADER,
                payload
                    .deleted_paths
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_COMMITTED_FILES_HEADER,
                payload
                    .committed_files
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            let _ = headers.insert(
                crate::core::commands::export::EXPORT_PROJECT_HEADER,
                payload
                    .project
                    .to_string()
                    .parse()
                    .expect("ascii header value"),
            );
            (StatusCode::OK, headers, payload.bytes).into_response()
        }
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/{id}/review` -- review.
async fn review_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(mut body): ApiJson<ReviewArgs>,
) -> Response {
    body.job_id = job_id; // path is authoritative
    let config = Arc::clone(&config);
    match run_blocking(move || commands::review::run(&config, &body)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// Reads the job's log file from `offset` (seek) and returns the raw bytes
/// plus the new end offset. The missing-log-file error text is identical to
/// the local `log` command's.
pub(crate) fn read_log_bytes(config: &Config, job_id: &str, offset: u64) -> Result<(Vec<u8>, u64)> {
    let meta = JobMetadata::resolve(&config.state_root, job_id)?;
    let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
    if !log_path.exists() {
        return Err(crate::core::error::AgentRunError::not_found(
            "log_not_found",
            format!(
                "No log file found for job '{}' at {} (the job may have terminated before logging started, or the log was purged)",
                meta.job_id,
                log_path.display()
            ),
        )
        .into());
    }
    let file = std::fs::File::open(&log_path)
        .with_context(|| format!("Failed to open log file at {}", log_path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("Failed to stat log file at {}", log_path.display()))?
        .len();
    let start = offset.min(len);
    let mut reader = std::io::BufReader::new(file);
    reader
        .seek(std::io::SeekFrom::Start(start))
        .with_context(|| format!("Failed to seek in log file at {}", log_path.display()))?;
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("Failed to read log file at {}", log_path.display()))?;
    let new_offset = start + bytes.len() as u64;
    Ok((bytes, new_offset))
}

/// `GET /api/v1/jobs/{id}/log?offset=<n>` -- raw log bytes (NOT a
/// CommandOutput; the client writes them byte-identical to local stdout)
/// plus the `Agent-Run-Log-Offset` response header.
async fn log_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiQuery(q): ApiQuery<LogQuery>,
) -> Response {
    let offset = q.offset.unwrap_or(0);
    let config = Arc::clone(&config);
    match run_blocking(move || read_log_bytes(&config, &job_id, offset)).await {
        Ok((bytes, new_offset)) => {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                "Content-Type",
                "text/plain; charset=utf-8"
                    .parse()
                    .expect("static header value"),
            );
            headers.insert(
                LOG_OFFSET_HEADER,
                new_offset
                    .to_string()
                    .parse()
                    .expect("numeric header value"),
            );
            (StatusCode::OK, headers, bytes).into_response()
        }
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs/{id}/diff?base=true` -- raw diff text (NOT a
/// CommandOutput; mirrors local stdout).
///
/// The query is taken as `RawQuery` (rather than `Query<DiffQuery>`) because
/// `only`/`exclude` are repeatable keys, which a struct-based extractor cannot
/// fold into `Vec<String>` fields; see [`parse_diff_query`].
async fn diff_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let q = match parse_diff_query(raw_query.as_deref()) {
        Ok(q) => q,
        // A malformed query is a client error: 400 with the parse message
        // (an unparseable query must never silently fall back to defaults).
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": message, "code": "invalid_query" })),
            )
                .into_response()
        }
    };
    let args = DiffArgs {
        job_id,
        base: q.base,
        only: q.only,
        exclude: q.exclude,
        stat: q.stat,
    };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::diff::run(&config, &args)).await {
        Ok(output) => {
            // `--stat` is structured: the full CommandOutput (its `data`
            // carries job_id/stat/diff/files) so the CLI can render it
            // human-style or as JSON.
            if q.stat {
                return Json(output).into_response();
            }
            let text = output
                .data
                .get("diff")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            // With no uncommitted changes, the body is the committed diff
            // against the base: carry the explanatory note in a response
            // header so the body stays a clean, pipeable patch.
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                "Content-Type",
                "text/plain; charset=utf-8"
                    .parse()
                    .expect("static header value"),
            );
            if let Some(note) = output.data.get("note").and_then(Value::as_str) {
                if let Ok(value) = note.parse() {
                    headers.insert(crate::core::output::DIFF_NOTE_HEADER, value);
                }
            }
            (StatusCode::OK, headers, text).into_response()
        }
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs/{id}/files?path=&depth=` -- list files in the job's
/// worktree.
async fn files_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiQuery(q): ApiQuery<FilesQuery>,
) -> Response {
    let args = FilesArgs {
        job_id,
        path: q.path,
        depth: q.depth,
    };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::files::files(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs/{id}/cat?path=&lines=&max_bytes=` -- print one file
/// from the job's worktree (text only, size-capped).
async fn cat_job(
    State(config): State<AppState>,
    ApiPath(job_id): ApiPath<String>,
    ApiQuery(q): ApiQuery<CatQuery>,
) -> Response {
    let args = CatArgs {
        job_id,
        path: q.path,
        lines: q.lines,
        max_bytes: q.max_bytes,
    };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::files::cat(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/jobs/{id}/result` -- result. The `data` payload is exactly
/// the object local `result --json` prints; the remote client re-serializes
/// it itself when `--json` was given.
async fn result_job(State(config): State<AppState>, ApiPath(job_id): ApiPath<String>) -> Response {
    let args = ResultArgs { job_id };
    let config = Arc::clone(&config);
    match run_blocking(move || commands::result::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobs/wait` -- one long request: the core blocks (500ms poll
/// loop, deadline math via `gc::parse_duration_string`) until the wait
/// condition is met or the timeout expires. Runs off the worker threads.
async fn wait_jobs(State(config): State<AppState>, ApiJson(args): ApiJson<WaitArgs>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::wait::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/gc` -- gc.
async fn gc_job(State(config): State<AppState>, ApiJson(args): ApiJson<GcArgs>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::gc::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/init` -- init. Legacy ACL fields in the request body
/// (`share_auth_from`, `share_auth_user`) are simply ignored: the fields no
/// longer exist on `InitArgs` and serde skips unknown JSON fields.
async fn init_job(State(config): State<AppState>, ApiJson(args): ApiJson<InitArgs>) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || commands::init::run(&config, &args)).await {
        Ok(output) => Json(output).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/jobsets/{id}/end` — end a jobset (operator-only by the shared
/// bearer gate).
///
/// Bumps the jobset's epoch, marks it ended (old credentials stop verifying)
/// and persists the membership store; answers 200 with the new epoch. An
/// unknown jobset (no member in this process) answers 404 with the standard
/// JSON error shape. The body is empty; the request needs no body fields.
async fn end_jobset_job(ApiPath(jobset_id): ApiPath<String>) -> Response {
    let id = jobset_id.clone();
    match run_blocking(move || crate::core::jobset::end_jobset(&id).map_err(anyhow::Error::msg))
        .await
    {
        Ok(Some(epoch)) => {
            Json(json!({ "jobset_id": jobset_id, "ended": true, "epoch": epoch })).into_response()
        }
        Ok(None) => error_to_response(anyhow::Error::new(
            crate::core::error::AgentRunError::not_found(
                "jobset_not_found",
                format!("No such jobset: '{jobset_id}'"),
            ),
        )),
        Err(err) => error_to_response(err),
    }
}

/// `GET /api/v1/auth/status` -- whether the server's home already holds
/// valid agy credentials (no agy spawn).
async fn auth_status() -> Response {
    match run_blocking(|| Ok(server_credentials_valid())).await {
        Ok(authenticated) => Json(CommandOutput::new(
            json!({ "authenticated": authenticated }),
        ))
        .into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/auth` -- start (or no-op) the agy OAuth flow. Empty body:
/// the core decides between "already authenticated" (the server's home
/// already holds valid credentials -- no agy spawn) and an in-progress
/// interactive flow (`oauth_url` + the prompt/URL lines agy printed so
/// far). A second start while a flow is already in progress -> 409;
/// spawn failure / URL deadline -> 500 with the captured output.
async fn start_auth(
    State(config): State<AppState>,
    Extension(auth): Extension<Arc<AuthManager>>,
) -> Response {
    let config = Arc::clone(&config);
    match run_blocking(move || auth.start(&config)).await {
        Err(err) => error_to_response(err),
        Ok(AuthStart::AlreadyAuthenticated) => {
            let mut output = CommandOutput::new(json!({ "authenticated": true }));
            output.push_out("Already authenticated (valid agy credentials found).".to_string());
            Json(output).into_response()
        }
        Ok(AuthStart::Pending {
            oauth_url,
            prompt_lines,
        }) => {
            let mut output = CommandOutput::new(json!({
                "authenticated": false,
                "oauth_url": oauth_url,
            }));
            for line in prompt_lines {
                output.push_out(line);
            }
            Json(output).into_response()
        }
    }
}

/// `POST /api/v1/auth/code` -- relay the user's code to the in-progress
/// agy child and wait for it to finish (fixed path, no session id). On
/// success agy has written its own credential files into the server's
/// home: the post-code output tail is the confirmation, and nothing
/// comes back over the wire. No flow in progress -> 404; agy
/// failure/timeout -> 500 with its output surfaced.
async fn submit_auth_code(
    Extension(auth): Extension<Arc<AuthManager>>,
    ApiJson(req): ApiJson<AuthCodeRequest>,
) -> Response {
    let code = req.code;
    match run_blocking(move || auth.submit_code(&code)).await {
        Ok(result) => {
            let mut output = CommandOutput::new(json!({ "authenticated": true }));
            for line in result.stdout_lines {
                output.push_out(line);
            }
            for line in result.stderr_lines {
                output.push_err(line);
            }
            Json(output).into_response()
        }
        Err(err) => error_to_response(err),
    }
}

/// `POST /api/v1/auth/cancel` -- kill the parked agy child (the whole
/// process tree) and clear the slot so a fresh flow can start. The
/// success payload is `{cancelled: true}`; no flow in progress -> 404
/// (the exact error text the code relay reports).
async fn cancel_auth(Extension(auth): Extension<Arc<AuthManager>>) -> Response {
    let auth = Arc::clone(&auth);
    match run_blocking(move || auth.cancel()).await {
        Ok(()) => Json(CommandOutput::new(json!({ "cancelled": true }))).into_response(),
        Err(err) => error_to_response(err),
    }
}

/// `GET /healthz` -- liveness probe (added to the router by
/// [`obs::add_observability`], never-auth).
#[allow(dead_code)]
async fn healthz() -> Response {
    Json(json!({ "status": "ok" })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::job::{ensure_state_store, JobStatus};
    use chrono::Utc;
    use http_body_util::BodyExt;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;
    use tower::ServiceExt; // `oneshot`

    fn test_router() -> Router {
        // The full observable app (as every server builds it): the /api/v1
        // sub-router + an always-ready /readyz and the never-auth /healthz.
        // No bearer token is configured, so the auth layer passes through.
        obs::add_observability(
            router(Arc::new(Config::default()), Arc::new(AuthManager::new())),
            auth::AuthState::none(),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        )
    }

    /// A test app with a configured bearer token (for auth-gating tests).
    fn test_router_authed(token: &str) -> Router {
        obs::add_observability(
            router(Arc::new(Config::default()), Arc::new(AuthManager::new())),
            auth::AuthState::token(token.to_string()),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        )
    }

    async fn body_string(body: axum::body::Body) -> String {
        let bytes = body.collect().await.expect("body").to_bytes();
        String::from_utf8_lossy(&bytes).to_string()
    }

    fn get_request(uri: &str) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .expect("request")
    }

    fn get_request_auth(uri: &str, token: &str) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::builder()
            .uri(uri)
            .header("Authorization", format!("Bearer {token}"))
            .body(axum::body::Body::empty())
            .expect("request")
    }

    fn post_json_request(uri: &str, body: &str) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .expect("request")
    }

    /// A POST request carrying the bearer credential (the count for the gated
    /// /api/v1 group, used to prove a route is behind the token).
    fn post_request_auth(uri: &str, token: &str) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .header("Authorization", format!("Bearer {token}"))
            .body(axum::body::Body::empty())
            .expect("request")
    }

    #[tokio::test]
    async fn test_healthz_returns_ok() {
        let res = test_router()
            .oneshot(get_request("/healthz"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["status"], "ok");
    }

    #[tokio::test]
    async fn test_all_read_routes_gated_by_token_when_configured() {
        // HL-50: with a bearer token configured, EVERY /api/v1 route
        // (including the read endpoints /sessions, /jobs, /info) requires the
        // token -- the whole group sits behind one layer.
        let app = test_router_authed("sekret-token");
        for uri in [
            "/api/v1/jobs",
            "/api/v1/info",
            "/api/v1/projects",
            "/api/v1/models",
        ] {
            let res = app.clone().oneshot(get_request(uri)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "GET {uri} without token must be 401"
            );
        }
        // 200-with-token: routes whose handler needs no external binary (the
        // `agy` binary is absent in tests, so `/models`/`/quota` handlers 500
        // even with valid auth -- their gate is already proven by the 401 above).
        for uri in ["/api/v1/jobs", "/api/v1/info", "/api/v1/projects"] {
            let res_ok = app
                .clone()
                .oneshot(get_request_auth(uri, "sekret-token"))
                .await
                .unwrap();
            assert_eq!(
                res_ok.status(),
                StatusCode::OK,
                "GET {uri} with token must be 200"
            );
        }
    }

    #[tokio::test]
    async fn test_healthz_stays_open_with_token_configured() {
        // The liveness endpoint is exempt from the bearer layer.
        let app = test_router_authed("sekret-token");
        let res = app.clone().oneshot(get_request("/healthz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // /readyz likewise never requires the token.
        let res = app.oneshot(get_request("/readyz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_no_token_configured_serves_without_auth() {
        // agent-run-api's model: no token configured -> the auth layer passes
        // through, all routes reachable (responses identical to pre-token).
        let app = test_router();
        for uri in ["/api/v1/jobs", "/api/v1/info", "/api/v1/projects"] {
            let res = app.clone().oneshot(get_request(uri)).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK, "GET {uri} must stay open");
        }
    }

    #[tokio::test]
    async fn test_readyz_ready_and_not_ready() {
        // Ready probe Ok -> 200 {"status":"ready"}
        let app = obs::add_observability(
            router(Arc::new(Config::default()), Arc::new(AuthManager::new())),
            auth::AuthState::none(),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        );
        let res = app.oneshot(get_request("/readyz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["status"], "ready");

        // Ready probe Err -> 503 {"error","code":"not_ready"}
        let app = obs::add_observability(
            router(Arc::new(Config::default()), Arc::new(AuthManager::new())),
            auth::AuthState::none(),
            Arc::new(|| Err("state store unreadable".to_string())),
            Arc::new(Vec::new),
        );
        let res = app.oneshot(get_request("/readyz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "not_ready");
    }

    #[tokio::test]
    async fn test_readyz_open_with_token_configured() {
        let app = test_router_authed("sekret-token");
        let res = app.oneshot(get_request("/readyz")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_format_and_auth() {
        // Without a token (agent-run-api) /metrics is reachable and serves
        // the fixed Prometheus text set.
        let app = obs::add_observability(
            router(Arc::new(Config::default()), Arc::new(AuthManager::new())),
            auth::AuthState::none(),
            Arc::new(|| Ok(())),
            Arc::new(|| vec![("running".to_string(), 2), ("done".to_string(), 5)]),
        );
        let res = app.oneshot(get_request("/metrics")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_string(res.into_body()).await;
        assert!(body.contains("agent_process_uptime_seconds"), "{body}");
        assert!(body.contains("agent_state{status=\"done\"} 5"), "{body}");
        assert!(body.contains("agent_state{status=\"running\"} 2"), "{body}");
        assert!(
            body.contains("agent_requests_total{class=\"2xx\"}"),
            "{body}"
        );
        assert!(
            body.starts_with("# TYPE"),
            "metrics must start with TYPE/HELP: {body}"
        );

        // With a token configured /metrics is behind the same auth as /api/v1.
        let app = test_router_authed("sekret-token");
        let res = app.clone().oneshot(get_request("/metrics")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unauthorized");
        let res = app
            .oneshot(get_request_auth("/metrics", "sekret-token"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_parse_errors_are_json_shaped() {
        let app = test_router();
        // Invalid JSON -> 400 with the standard {error, code} JSON shape.
        let res = app
            .clone()
            .oneshot(post_json_request("/api/v1/jobs", "not json"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "invalid_body");
        assert!(value["error"].is_string());

        // Wrong content type -> 415 unsupported media type.
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/jobs")
            .header("Content-Type", "text/plain")
            .body(axum::body::Body::from("hello"))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "invalid_content_type");

        // Well-formed JSON with a missing required field -> 422.
        let res = app
            .clone()
            .oneshot(post_json_request("/api/v1/jobs", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "invalid_body");
    }

    #[tokio::test]
    async fn test_unknown_v1_route_is_404_json() {
        let app = test_router();
        let res = app
            .oneshot(get_request("/api/v1/definitely-not-a-route"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "not_found");
        assert_eq!(value["error"], "not found");
    }

    #[tokio::test]
    async fn test_remote_control_enable_disabled_returns_403() {
        let res = test_router()
            .oneshot(post_json_request("/api/v1/remote-control/enable", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = body_string(res.into_body()).await;
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["code"], "remote_control_disabled");
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains("AGENT_RUN_REMOTE_CONTROL"));
    }

    #[tokio::test]
    async fn test_remote_control_suppression_flag_toggle() {
        let rc_state = RemoteControlState::default();
        assert!(!rc_state.suppressed.load(Ordering::SeqCst));

        let app = router_with_rc(
            Arc::new(Config::default()),
            Arc::new(AuthManager::new()),
            rc_state.clone(),
        );
        let res = app
            .oneshot(post_json_request("/api/v1/remote-control/enable", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        assert!(!rc_state.suppressed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_unknown_route_is_404() {
        let res = test_router()
            .oneshot(get_request("/api/v1/nope"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_malformed_json_body_is_400() {
        let res = test_router()
            .oneshot(post_json_request("/api/v1/jobs", "not json"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_files_unknown_job_is_404_with_error_body() {
        // A fresh state dir (no jobs) in a temp root, so the unknown-job
        // error path is deterministic instead of touching the developer's
        // `~/.agent-run` database.
        let tmp = tempfile::tempdir().unwrap();
        let app = router(
            Arc::new(Config {
                workspace_root: tmp.path().join("ws"),
                state_root: tmp.path().join("state"),
                ..Default::default()
            }),
            Arc::new(AuthManager::new()),
        );
        let res = app
            .oneshot(get_request("/api/v1/jobs/nope1234/files?path=src&depth=2"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains("No such job found"));
    }

    #[tokio::test]
    async fn test_cat_unknown_job_is_404_with_error_body() {
        let tmp = tempfile::tempdir().unwrap();
        let app = router(
            Arc::new(Config {
                workspace_root: tmp.path().join("ws"),
                state_root: tmp.path().join("state"),
                ..Default::default()
            }),
            Arc::new(AuthManager::new()),
        );
        let res = app
            .oneshot(get_request(
                "/api/v1/jobs/nope1234/cat?path=README.md&lines=1:20&max_bytes=1000",
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains("No such job found"));
    }

    #[tokio::test]
    async fn test_repair_unknown_job_is_404_with_error_body() {
        // The per-job repair endpoint resolves the job by id/prefix before
        // touching any git state; an unknown id yields the standard 404.
        let tmp = tempfile::tempdir().unwrap();
        ensure_state_store(&tmp.path().join("state")).unwrap();
        let app = router(
            Arc::new(Config {
                workspace_root: tmp.path().join("ws"),
                state_root: tmp.path().join("state"),
                ..Default::default()
            }),
            Arc::new(AuthManager::new()),
        );
        let res = app
            .oneshot(post_json_request("/api/v1/jobs/nope1234/repair", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "job_not_found");
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains("No such job found"));
    }

    #[tokio::test]
    async fn test_repair_all_with_no_jobs_is_200_empty_results() {
        // `POST /api/v1/jobs/repair` with an empty state store: the static
        // route (registered before `{job_id}`) wins and reports nothing to
        // repair with the empty-results payload.
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        ensure_state_store(&state).unwrap();
        let app = router(
            Arc::new(Config {
                workspace_root: tmp.path().join("ws"),
                state_root: state,
                ..Default::default()
            }),
            Arc::new(AuthManager::new()),
        );
        let res = app
            .oneshot(post_json_request("/api/v1/jobs/repair", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["exit_code"], 0);
        assert_eq!(
            value["stdout_lines"],
            serde_json::json!(["No jobs found (nothing to repair)."])
        );
        assert_eq!(value["data"]["results"], serde_json::json!([]));
    }

    // -- error body shape ----------------------------------------------------

    #[tokio::test]
    async fn test_error_body_hides_cause_chain() {
        // A typed error buried under an anyhow context chain, built the way the
        // codebase layers context onto a failing call. The old Debug form
        // (`format!("{err:?}")`) turned this into "outer operation
        // failed\nCaused by:\n    0: reading job dir /abs/internal/state\n    1:
        // No such job found: 'x'" and sent that whole chain -- with the
        // internal absolute path -- to the client. The body must carry the
        // typed error's own user-facing message ("No such job found ...",
        // the text the CLI prints), never the context chain: no "Caused by",
        // no newline, no internal path.
        let inner: anyhow::Result<()> = Err(crate::core::error::AgentRunError::not_found(
            "job_not_found",
            "No such job found: 'x'",
        )
        .into());
        let err = inner
            .context("reading job dir /abs/internal/state")
            .context("outer operation failed")
            .expect_err("chained error present");

        let res = error_to_response(err);
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        let msg = value["error"].as_str().expect("error field");
        assert_eq!(
            msg, "No such job found: 'x'",
            "the body must carry the typed message, not the context chain: {msg:?}"
        );
        assert!(
            !msg.contains("Caused by"),
            "body leaked the cause chain: {msg:?}"
        );
        assert!(!msg.contains('\n'), "body leaked newlines: {msg:?}");
        assert!(
            !msg.contains("/abs/internal/state"),
            "body leaked an internal absolute path: {msg:?}"
        );
        // The typed `code` is preserved for programmatic consumers.
        assert_eq!(value["code"], "job_not_found");
    }

    // -- route -> arg mapping ------------------------------------------------

    #[test]
    fn test_start_request_maps_full_client_body() {
        // The exact shape client.rs::start_body emits: the base fields are
        // always present, optionals only when set.
        let body = json!({
            "project": "myrepo",
            "prompt": "Fix the bug",
            "sandbox": false,
            "plan": true,
            "hindsight": "off",
            "hindsight_query": "short query",
            "git_ref": "main",
            "model": "claude-sonnet-4-5",
            "effort": "high",
            "add_dirs": ["/workspace/other"],
            "print_timeout": "30m",
            "from_worktree": "/workspace/myrepo/.agent-jobs/existing",
            "from_patch_content": "diff --git a/x b/x",
        });
        let req: StartRequest = serde_json::from_value(body).expect("start body");
        assert_eq!(req.project, "myrepo");
        assert_eq!(req.prompt, "Fix the bug");
        assert_eq!(req.git_ref.as_deref(), Some("main"));
        assert_eq!(req.model.as_deref(), Some("claude-sonnet-4-5"));
        assert_eq!(req.effort.as_deref(), Some("high"));
        assert!(!req.sandbox);
        assert_eq!(req.add_dirs, vec!["/workspace/other".to_string()]);
        assert!(req.plan);
        assert_eq!(req.print_timeout.as_deref(), Some("30m"));
        assert_eq!(req.hindsight, Some(HindsightFlag::Off));
        assert_eq!(req.hindsight_bank, None);
        assert_eq!(req.hindsight_query.as_deref(), Some("short query"));
        assert_eq!(req.proxy, None);
        assert_eq!(
            req.from_worktree.as_deref(),
            Some("/workspace/myrepo/.agent-jobs/existing")
        );
        assert_eq!(
            req.from_patch_content.as_deref(),
            Some("diff --git a/x b/x")
        );
    }

    #[test]
    fn test_start_request_omitted_optionals_default_like_clap() {
        // Minimal client body (no optionals at all): every omitted field
        // must default exactly like the local clap flags do.
        let req: StartRequest =
            serde_json::from_value(json!({ "project": "p", "prompt": "hi" })).expect("body");
        assert!(!req.plan);
        assert!(!req.sandbox);
        assert!(req.add_dirs.is_empty());
        assert_eq!(req.hindsight, None);
        assert_eq!(req.hindsight_bank, None);
        assert_eq!(req.hindsight_query, None);
        assert_eq!(req.proxy, None);
        assert_eq!(req.git_ref, None);
        assert_eq!(req.model, None);
        assert_eq!(req.effort, None);
        assert_eq!(req.print_timeout, None);
        assert_eq!(req.from_worktree, None);
        assert_eq!(req.from_patch_content, None);
    }

    #[test]
    fn test_wait_gc_retry_bodies_map() {
        let w: WaitArgs = serde_json::from_value(
            json!({ "job_ids": ["a", "b"], "any": false, "all": true, "timeout": "5m" }),
        )
        .expect("wait body");
        assert_eq!(w.job_ids, vec!["a".to_string(), "b".to_string()]);
        assert!(!w.any);
        assert!(w.all);
        assert_eq!(w.timeout.as_deref(), Some("5m"));

        // wait body without timeout (client omits it).
        let w2: WaitArgs =
            serde_json::from_value(json!({ "job_ids": ["a"], "any": false, "all": false }))
                .expect("wait body");
        assert_eq!(w2.timeout, None);

        let g: GcArgs =
            serde_json::from_value(json!({ "older_than": "14d", "project": "p", "max_jobs": 5 }))
                .expect("gc body");
        assert_eq!(g.older_than.as_deref(), Some("14d"));
        assert_eq!(g.project.as_deref(), Some("p"));
        assert_eq!(g.max_jobs, Some(5));
        assert!(!g.dry_run);
        assert!(!g.purge_logs);

        let g2: GcArgs = serde_json::from_value(
            json!({ "older_than": "1d", "dry_run": true, "purge_logs": true }),
        )
        .expect("gc body with dry_run and purge_logs");
        assert_eq!(g2.older_than.as_deref(), Some("1d"));
        assert!(g2.dry_run);
        assert!(g2.purge_logs);

        let r: RetryArgs = serde_json::from_value(json!({ "job_id": "x", "hindsight": "on" }))
            .expect("retry body");
        assert_eq!(r.job_id, "x");
        assert_eq!(r.hindsight, Some(HindsightFlag::On));
        assert_eq!(r.hindsight_query, None);

        let r2: RetryArgs =
            serde_json::from_value(json!({ "job_id": "x", "hindsight_query": "short query" }))
                .expect("retry body with hindsight_query");
        assert_eq!(r2.hindsight_query.as_deref(), Some("short query"));

        let c: CommitArgs =
            serde_json::from_value(json!({ "job_id": "x", "message": "m" })).expect("commit body");
        assert_eq!(c.message.as_deref(), Some("m"));

        // Commit selection (only/exclude arrays; absent in old clients).
        let c2: CommitArgs = serde_json::from_value(
            json!({ "job_id": "x", "only": ["src/main.rs"], "exclude": ["*.log"] }),
        )
        .expect("commit body with selection");
        assert_eq!(c2.only, vec!["src/main.rs".to_string()]);
        assert_eq!(c2.exclude, vec!["*.log".to_string()]);

        let d: DiscardArgs =
            serde_json::from_value(json!({ "job_id": "x", "paths": ["a.txt", "b/c.txt"] }))
                .expect("discard body");
        assert_eq!(d.job_id, "x");
        assert_eq!(d.paths, vec!["a.txt".to_string(), "b/c.txt".to_string()]);

        // A body that OMITS `paths` still deserializes (serde default) -- the
        // wire form is unchanged; the refusal lives in the command, not the
        // deserializer, so this body is rejected at run() time (see the
        // empty-paths test below), not as a 400 from the Json extractor.
        let d_empty: DiscardArgs =
            serde_json::from_value(json!({ "job_id": "x" })).expect("discard body, no paths");
        assert!(d_empty.paths.is_empty());

        let rv: ReviewArgs =
            serde_json::from_value(json!({ "job_id": "x", "reviewer_model": "claude-opus-4-6" }))
                .expect("review body");
        assert_eq!(rv.reviewer_model.as_deref(), Some("claude-opus-4-6"));
    }

    #[test]
    fn test_repair_body_maps() {
        let r: RepairArgs =
            serde_json::from_value(json!({ "job_id": "x" })).expect("repair single body");
        assert_eq!(r.job_id.as_deref(), Some("x"));
        assert!(!r.all);

        // --all mode body (the client omits job_id; the route forces all).
        let a: RepairArgs =
            serde_json::from_value(json!({ "all": true })).expect("repair --all body");
        assert_eq!(a.job_id, None);
        assert!(a.all);

        // Minimal client body: both fields optional on the wire.
        let e: RepairArgs = serde_json::from_value(json!({})).expect("empty repair body");
        assert_eq!(e.job_id, None);
        assert!(!e.all);
    }

    #[test]
    fn test_clean_query_parses_bools() {
        // The client sends ?force=true&purge_log=false (Rust bool Display).
        let q: CleanQuery = serde_urlencoded_test("force=true&purge_log=false");
        assert!(q.force);
        assert!(!q.purge_log);
        let q2: CleanQuery = serde_urlencoded_test("force=false&purge_log=true");
        assert!(!q2.force);
        assert!(q2.purge_log);
        // Absent params default to false (like the local CLI flags).
        let q3: CleanQuery = serde_urlencoded_test("");
        assert!(!q3.force);
        assert!(!q3.purge_log);
    }

    fn serde_urlencoded_test<T: serde::de::DeserializeOwned>(query: &str) -> T {
        serde_urlencoded::from_str(query).expect("query")
    }

    #[test]
    fn test_diff_query_parses_selection_flags() {
        // The diff handler extracts the query as a RawQuery and folds the
        // `(key, value)` pairs by hand (see `parse_diff_query`); the tests
        // below drive that same parser with the raw string the `?`-less
        // extractor yields.

        // Old client: no new params at all.
        let q = parse_diff_query(None).unwrap();
        assert!(!q.base);
        assert!(!q.stat);
        assert!(q.only.is_empty());
        assert!(q.exclude.is_empty());
        let q = parse_diff_query(Some("")).unwrap();
        assert!(!q.base);
        assert!(!q.stat);
        assert!(q.only.is_empty());
        assert!(q.exclude.is_empty());

        // New client: repeated only/exclude values + both bools. The repeated
        // keys are the part a `Query<Struct>` extractor cannot express.
        let q = parse_diff_query(Some(
            "base=true&stat=true&only=src%2Fmain.rs&only=doc%2Fguide.md&exclude=%2A.log",
        ))
        .unwrap();
        assert!(q.base);
        assert!(q.stat);
        assert_eq!(
            q.only,
            vec!["src/main.rs".to_string(), "doc/guide.md".to_string()]
        );
        assert_eq!(q.exclude, vec!["*.log".to_string()]);

        // The client only sends `base`/`stat` when they are true, using Rust
        // bool Display; both `1` and `true` are accepted, and `false` is not
        // truthy.
        let q = parse_diff_query(Some("base=1&stat=True")).unwrap();
        assert!(q.base);
        assert!(q.stat);
        let q = parse_diff_query(Some("base=false&stat=false")).unwrap();
        assert!(!q.base);
        assert!(!q.stat);

        // Unknown keys are ignored; only/exclude are independent of base/stat.
        let q = parse_diff_query(Some("only=a.txt")).unwrap();
        assert!(!q.base);
        assert!(!q.stat);
        assert_eq!(q.only, vec!["a.txt".to_string()]);
        assert!(q.exclude.is_empty());
    }

    #[test]
    fn test_diff_query_accepts_interleaved_and_bare_keys() {
        // Repeated keys in NON-ADJACENT positions (interleaved with other
        // keys) must both survive -- a struct/adjacency-based decode dropped
        // the second occurrence. Unknown keys stay ignored.
        let q = parse_diff_query(Some("only=a&x=1&only=b")).unwrap();
        assert_eq!(q.only, vec!["a".to_string(), "b".to_string()]);
        assert!(!q.base && !q.stat);

        // A BARE key (no `=`) reads as an empty value, not an error: an old
        // client that sends `?base` without a value keeps working.
        let q = parse_diff_query(Some("base&stat=1")).unwrap();
        assert!(!q.base, "bare key is not truthy");
        assert!(q.stat);
    }

    #[test]
    fn test_diff_query_bad_percent_encoding_is_error() {
        // A malformed %-sequence must NOT be silently discarded (the old
        // behaviour reset the whole query to defaults) -- it is a client
        // error, which the handler turns into a 400.
        for bad in ["only=%ZZ", "x=%E0", "only=a&%"] {
            match parse_diff_query(Some(bad)) {
                Err(e) => assert!(e.contains("percent-encoding"), "{bad} -> {e}"),
                Ok(_) => panic!("{bad} should have failed to parse"),
            }
        }
    }

    #[test]
    fn test_list_query_parses_project_and_status() {
        // No query at all: old behaviour -- no project filter, no status
        // filter (the client that predates --status keeps working).
        let q = parse_list_query(None).unwrap();
        assert!(q.project.is_none());
        assert!(q.status.is_empty());
        for empty in ["", "   "] {
            let q = parse_list_query(Some(empty)).unwrap();
            assert!(q.project.is_none());
            assert!(q.status.is_empty());
        }

        // A single status value; the project filter is captured verbatim.
        let q = parse_list_query(Some("project=myrepo&status=done")).unwrap();
        assert_eq!(q.project.as_deref(), Some("myrepo"));
        assert_eq!(q.status, vec!["done".to_string()]);

        // `status` is REPEATED: every occurrence appends (the part a
        // `Query<ListArgs>` extractor cannot fold), in any position and
        // interleaved with other keys.
        let q = parse_list_query(Some("status=done&status=failed")).unwrap();
        assert_eq!(q.status, vec!["done".to_string(), "failed".to_string()]);
        let q = parse_list_query(Some("status=running&project=p&status=done")).unwrap();
        assert_eq!(q.project.as_deref(), Some("p"));
        assert_eq!(q.status, vec!["running".to_string(), "done".to_string()]);

        // A value may itself be comma-separated; the commas are PRESERVED (the
        // core's status filter splits them) and URL-encoding is undone.
        let q = parse_list_query(Some("status=done%2Cfailed")).unwrap();
        assert_eq!(q.status, vec!["done,failed".to_string()]);
        // ...as is a literal comma the client chose not to encode.
        let q = parse_list_query(Some("status=done,failed")).unwrap();
        assert_eq!(q.status, vec!["done,failed".to_string()]);

        // project only (no status) -- a pre-status client with a filter.
        let q = parse_list_query(Some("project=alpha")).unwrap();
        assert_eq!(q.project.as_deref(), Some("alpha"));
        assert!(q.status.is_empty());
    }

    #[test]
    fn test_list_query_bad_percent_encoding_is_error() {
        // A malformed %-sequence is a client error (the handler turns it into
        // a 400), never a silently-dropped filter.
        for bad in ["status=%ZZ", "project=%E0", "status=done&%"] {
            match parse_list_query(Some(bad)) {
                Err(e) => assert!(e.contains("percent-encoding"), "{bad} -> {e}"),
                Ok(_) => panic!("{bad} should have failed to parse"),
            }
        }
    }

    #[tokio::test]
    async fn test_discard_no_paths_is_400() {
        // A discard body that omits `paths` deserializes (serde default) but
        // the command refuses the empty selection -- and it does so BEFORE any
        // job lookup, so this is deterministic on a router with no jobs.
        let res = test_router()
            .oneshot(post_json_request(
                "/api/v1/jobs/some-job/discard",
                r#"{"job_id":"some-job"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("discard needs at least one path"),
            "error body: {value}"
        );
    }

    // -- list query: real router + seeded state store ------------------------

    fn fake_list_meta(job_id: &str, project: &str) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            project: project.to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: PathBuf::from("/tmp/missing-wt"),
            branch: String::new(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            pid_start_time: None,
            exit_code: Some(0),
            status: Some(JobStatus::Done),
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
        }
    }

    // Seeds one deterministic, terminal job: `compute_status` recomputes the
    // status from the (empty) PID + exit code + the structured log line, so
    // the log carries an explicit terminal SUCCESS/ERROR signal. Returns the
    // job id so the caller can assert on the filtered listing.
    fn seed_list_job(state_root: &Path, job_id: &str, project: &str, terminal: bool) {
        let mut meta = fake_list_meta(job_id, project);
        meta.exit_code = Some(if terminal { 0 } else { 1 });
        meta.save(state_root).unwrap();
        let log_dir = JobMetadata::log_dir(state_root);
        fs::create_dir_all(&log_dir).unwrap();
        let log = if terminal {
            r#"{"conversation_id":"c","status":"SUCCESS","response":"ok"}"#
        } else {
            r#"{"conversation_id":"c","status":"ERROR","response":"","error":"boom"}"#
        };
        fs::write(JobMetadata::log_path(state_root, job_id), log).unwrap();
    }

    // Builds a router over a fresh state store rooted in `tmp`, then applies
    // `seed` to it. The tempdir outlives the router within the test body.
    fn list_router_with(tmp: &TempDir, seed: &dyn Fn(&Path)) -> Router {
        let state = tmp.path().join("state");
        ensure_state_store(&state).unwrap();
        seed(&state);
        router(
            Arc::new(Config {
                workspace_root: tmp.path().join("ws"),
                state_root: state,
                ..Default::default()
            }),
            Arc::new(AuthManager::new()),
        )
    }

    fn list_count(value: &Value) -> usize {
        value["data"]["jobs"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(usize::MAX)
    }

    #[tokio::test]
    async fn test_list_status_query_filters_through_router() {
        let tmp = tempfile::tempdir().unwrap();
        let router = list_router_with(&tmp, &|state| {
            // 3 done + 2 failed (deterministic: terminal logs, no live PID).
            for (i, id) in ["lstjob01", "lstjob02", "lstjob03"].into_iter().enumerate() {
                seed_list_job(state, id, if i == 0 { "otherproj" } else { "proj-a" }, true);
            }
            for id in ["lstjob04", "lstjob05"] {
                seed_list_job(state, id, "proj-a", false);
            }
        });

        // No status key: old behaviour -- every job (all 5).
        let res = router
            .clone()
            .oneshot(get_request("/api/v1/jobs"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            list_count(&serde_json::from_str(&body_string(res.into_body()).await).unwrap()),
            5
        );

        // ?status=done keeps only the done jobs.
        let res = router
            .clone()
            .oneshot(get_request("/api/v1/jobs?status=done"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            list_count(&serde_json::from_str(&body_string(res.into_body()).await).unwrap()),
            3
        );

        // ?status=done,failed (one comma-joined value) -- both sets.
        let res = router
            .clone()
            .oneshot(get_request("/api/v1/jobs?status=done%2Cfailed"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            list_count(&serde_json::from_str(&body_string(res.into_body()).await).unwrap()),
            5
        );

        // ?status=done&status=failed (repeated keys) -- the shape a
        // `Query<ListArgs>` extractor cannot fold.
        let res = router
            .clone()
            .oneshot(get_request("/api/v1/jobs?status=done&status=failed"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            list_count(&serde_json::from_str(&body_string(res.into_body()).await).unwrap()),
            5
        );

        // ?status=failed alone keeps only the failed jobs.
        let res = router
            .clone()
            .oneshot(get_request("/api/v1/jobs?status=failed"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            list_count(&serde_json::from_str(&body_string(res.into_body()).await).unwrap()),
            2
        );

        // ?project ANDs with ?status: the 2 done jobs in proj-a (lstjob01 is
        // in otherproj), the 2 failed excluded.
        let res = router
            .clone()
            .oneshot(get_request("/api/v1/jobs?project=proj-a&status=done"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            list_count(&serde_json::from_str(&body_string(res.into_body()).await).unwrap()),
            2
        );
    }

    #[tokio::test]
    async fn test_list_status_query_unknown_is_400() {
        let tmp = tempfile::tempdir().unwrap();
        let router = list_router_with(&tmp, &|state| {
            seed_list_job(state, "lstjob01", "proj-a", true);
        });
        let res = router
            .oneshot(get_request("/api/v1/jobs?status=bogus"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("unknown status 'bogus'"),
            "error body: {value}"
        );
    }

    #[tokio::test]
    async fn test_list_bad_query_is_400_with_parse_error() {
        // Route-level: a malformed list query is a 400 with the parse message
        // (the job list is never even read).
        let res = test_router()
            .oneshot(get_request("/api/v1/jobs?status=%ZZ"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("percent-encoding"),
            "error body: {value}"
        );
    }

    #[tokio::test]
    async fn test_diff_bad_query_is_400_with_parse_error() {
        // Route-level: a malformed diff query is a 400 with the parse message,
        // not a silent default-args diff. (The job is never even resolved.)
        let res = test_router()
            .oneshot(get_request("/api/v1/jobs/some-job/diff?only=%ZZ"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("percent-encoding"),
            "error body: {value}"
        );
    }

    // -- error mapping -------------------------------------------------------
    //
    // Parity test: for every error the old `status_for_error`/
    // `VALIDATION_MARKERS` classified by TEXT, the equivalent TYPED error the
    // core now produces yields the SAME HTTP status (and a stable `code`).
    // Each row reconstructs the exact `anyhow::Error` the originating site
    // builds today, runs it through `error_to_response`, and asserts the
    // legacy status. The `expected_code` column pins the stable code.

    /// Builds a typed `anyhow::Error` (the same shape the originating core
    /// site builds) and asserts it maps to `status` with code `code`, and that
    /// the `error` field of the body is the untouched message.
    fn assert_typed(
        code: &'static str,
        kind: crate::core::error::ErrorKind,
        msg: &str,
        status: u16,
    ) {
        let err: anyhow::Error = match kind {
            crate::core::error::ErrorKind::NotFound => {
                crate::core::error::AgentRunError::not_found(code, msg).into()
            }
            crate::core::error::ErrorKind::Conflict => {
                crate::core::error::AgentRunError::conflict(code, msg).into()
            }
            crate::core::error::ErrorKind::Invalid => {
                crate::core::error::AgentRunError::invalid(code, msg).into()
            }
            crate::core::error::ErrorKind::Internal => {
                crate::core::error::AgentRunError::internal(code, msg).into()
            }
            crate::core::error::ErrorKind::Forbidden => {
                crate::core::error::AgentRunError::forbidden(code, msg).into()
            }
        };
        let res = error_to_response(err);
        assert_eq!(res.status().as_u16(), status, "status for '{code}'");
    }

    /// Asserts an UNTYPED error (no `AgentRunError` in the chain) maps to 500
    /// with code "internal" and the message preserved verbatim.
    fn assert_untyped(msg: &str) {
        let message = msg.to_string();
        let res = error_to_response(anyhow::anyhow!("{}", message));
        assert_eq!(res.status().as_u16(), 500, "untyped '{msg}' must stay 500");
    }

    #[test]
    fn test_error_status_mapping_parity() {
        use crate::core::error::ErrorKind;
        // Every row = a message the OLD status_for_error/VALIDATION_MARKERS
        // classified by text. The right-hand side is the TYPED error the core
        // now produces for it; the assertion is that it lands on the SAME
        // legacy status (404/409/400/500).
        // ---- 404 (NotFound) ----
        assert_typed(
            "job_not_found",
            ErrorKind::NotFound,
            "No such job found: 'abc' (no job record in /x/state.sqlite)",
            404,
        );
        assert_typed(
            "log_not_found",
            ErrorKind::NotFound,
            "No log file found for job 'abc' at /x/log.txt (the job may have terminated before logging started, or the log was purged)",
            404,
        );
        assert_typed(
            "auth_not_in_progress",
            ErrorKind::NotFound,
            "no authentication is in progress on this server",
            404,
        );
        // ---- 409 (Conflict) ----
        assert_typed(
            "auth_in_progress",
            ErrorKind::Conflict,
            "authentication is already in progress on this server",
            409,
        );
        // ---- 403 (Forbidden) ----
        assert_typed(
            "remote_control_disabled",
            ErrorKind::Forbidden,
            "remote control is disabled on the server (set AGENT_RUN_REMOTE_CONTROL=1 or true to enable)",
            403,
        );
        // ---- 400 (Invalid / validation refusals) ----
        assert_typed(
            "job_id_ambiguous",
            ErrorKind::Invalid,
            "Job ID prefix 'abc' is ambiguous; matches: ab1, ab2",
            400,
        );
        assert_typed(
            "parallel_limit",
            ErrorKind::Invalid,
            "Cannot start new job: 3 job(s) are already running, which reaches the max-parallel limit of 3. Running jobs: a, b, c. Wait for jobs to finish ('agent-run wait <id>'), or raise the limit (--max-parallel / AGENT_RUN_MAX_PARALLEL / max_parallel in config.toml).",
            400,
        );
        assert_typed(
            "job_running",
            ErrorKind::Invalid,
            "Cannot retry job 'x': job is currently running (PID 123). Terminate it first using 'agent-run kill x'.",
            400,
        );
        assert_typed(
            "job_running",
            ErrorKind::Invalid,
            "Job 'x' is still running (PID 123). Use 'agent-run wait x' or 'agent-run tail x' first.",
            400,
        );
        assert_typed(
            "selection_matched_nothing",
            ErrorKind::Invalid,
            "Nothing to commit: the selection matched no changes in wt (check the --only/--exclude paths against `agent-run diff x --stat`)",
            400,
        );
        assert_typed(
            "discard_no_paths",
            ErrorKind::Invalid,
            "Refusing to discard: discard needs at least one path (got none)",
            400,
        );
        assert_typed(
            "worktree_not_found",
            ErrorKind::Invalid,
            "Worktree directory for job 'x' does not exist at /workspace/r/.agent-jobs/x",
            400,
        );
        assert_typed(
            "invalid_duration",
            ErrorKind::Invalid,
            "Invalid duration format '14q'. Expected e.g. '14d', '48h', or number of days.",
            400,
        );
        assert_typed(
            "prompt_empty",
            ErrorKind::Invalid,
            "Prompt cannot be empty",
            400,
        );
        // ---- 500 (untyped): unexpected I/O/spawn/DB + misconfiguration ----
        assert_untyped("Failed to read log file at /x");
        assert_untyped("Configured agy binary does not exist at '/usr/local/libexec/agy'");
        assert_untyped("database is locked");
        assert_untyped(
            "Failed to spawn executable (check that the binary exists and is executable, or check configured path)",
        );
    }

    // The typed code carried in the JSON body (checked alongside the status).
    #[tokio::test]
    async fn test_error_body_carries_typed_code() {
        let res = error_to_response(crate_err_not_found(
            "job_not_found",
            "No such job found: 'abc' (no job record in /x/state.sqlite)",
        ));
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "job_not_found");
        // The server-side state path must never leak into the client body.
        assert_eq!(value["error"], "No such job found: 'abc'");
        assert!(!value["error"].as_str().unwrap().contains('/'));

        let res = error_to_response(crate_err_conflict(
            "auth_in_progress",
            "authentication is already in progress on this server",
        ));
        assert_eq!(res.status(), StatusCode::CONFLICT);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "auth_in_progress");
        assert_eq!(
            value["error"],
            "authentication is already in progress on this server"
        );

        let res = error_to_response(crate_err_invalid("prompt_empty", "Prompt cannot be empty"));
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "prompt_empty");
        assert_eq!(value["error"], "Prompt cannot be empty");

        // Untyped errors fall back to code "internal" with a 500.
        let res = error_to_response(anyhow::anyhow!("database is locked"));
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "internal");
        assert_eq!(value["error"], "database is locked");
    }

    fn crate_err_not_found(code: &'static str, msg: &str) -> anyhow::Error {
        crate::core::error::AgentRunError::not_found(code, msg).into()
    }
    fn crate_err_conflict(code: &'static str, msg: &str) -> anyhow::Error {
        crate::core::error::AgentRunError::conflict(code, msg).into()
    }
    fn crate_err_invalid(code: &'static str, msg: &str) -> anyhow::Error {
        crate::core::error::AgentRunError::invalid(code, msg).into()
    }

    // -- patch materialization ------------------------------------------------

    #[test]
    fn test_patch_materialization_unique_and_removable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = Config {
            state_root: temp.path().join("state"),
            ..Default::default()
        };

        let p1 = materialize_patch(&config, "patch one").expect("materialize 1");
        let p2 = materialize_patch(&config, "patch two").expect("materialize 2");
        assert_ne!(p1, p2, "consecutive patches must get unique names");
        assert!(
            p1.starts_with(&config.state_root),
            "patch must land under state_root"
        );
        assert_eq!(
            std::fs::read_to_string(&p1).expect("read"),
            "patch one",
            "content must round-trip verbatim"
        );

        // The start handler deletes the temp file after run returns (the
        // cleanup path itself is exercised end-to-end); simulate it here.
        std::fs::remove_file(&p1).expect("remove");
        assert!(!p1.exists());
        std::fs::remove_file(&p2).expect("remove 2");
    }

    // -- auth routes (fake agy scripts; HOME pointed at temp dirs) --------

    use std::os::unix::fs::PermissionsExt;

    /// The auth tests below mutate the process `HOME` env var; serialize
    /// them (the other suite tests don't read HOME).
    static AUTH_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn set_test_home(home: &std::path::Path) {
        std::env::set_var("HOME", home);
    }

    fn auth_test_config(tmp: &std::path::Path, agy: &std::path::Path) -> Arc<Config> {
        Arc::new(Config {
            workspace_root: tmp.join("ws"),
            state_root: tmp.join("state"),
            agy_path: agy.to_path_buf(),
            ..Default::default()
        })
    }

    fn fast_auth_manager() -> Arc<AuthManager> {
        Arc::new(AuthManager::with_timeouts(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        ))
    }

    fn json_lines(value: &Value, key: &str) -> Vec<String> {
        value[key]
            .as_array()
            .expect("line array")
            .iter()
            .map(|v| v.as_str().expect("line").to_string())
            .collect()
    }

    /// Fake `agy` for the happy-path auth flow: banner + OAuth URL + paste
    /// prompt, reads one line (the code) from stdin, prints the success
    /// markers, writes the three credential files into its own
    /// `$HOME/.gemini` (agy owns its credentials), then keeps running
    /// briefly (like the real agy's post-exchange `--print` prompt)
    /// before exiting 0.
    fn write_fake_agy_success(path: &std::path::Path) {
        std::fs::write(
            path,
            "#!/bin/sh\necho 'Opening the browser for authentication...'\necho 'Please visit https://antigravity.google/auth?session=fake123 and sign in'\necho 'After authenticating, copy the code displayed in the browser and paste it below:'\nread code\necho 'Completing authentication...'\necho 'authentication completed successfully'\nmkdir -p \"$HOME/.gemini/antigravity-cli\"\nprintf '{\"refresh_token\":\"rt-fake\",\"access_token\":\"at-fake\"}' > \"$HOME/.gemini/oauth_creds.json\"\nprintf '[{\"email\":\"dev@example.com\"}]' > \"$HOME/.gemini/google_accounts.json\"\nprintf 'token-fake-bytes' > \"$HOME/.gemini/antigravity-cli/antigravity-oauth-token\"\nsleep 0.2\nexit 0\n",
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Fake `agy` that prints the URL, reads the code, prints an error, and
    /// exits 1.
    fn write_fake_agy_failure(path: &std::path::Path) {
        std::fs::write(
            path,
            "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=fail123 and sign in'\nread code\necho 'ERROR: the code was rejected'\nexit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Fake `agy` that only sleeps (never prints a URL) and records its
    /// own PID first (the tests assert the kill actually reached it).
    fn write_fake_agy_sleeper(path: &std::path::Path, pid_file: &std::path::Path) {
        std::fs::write(
            path,
            format!("#!/bin/sh\necho $$ > {}\nsleep 30\n", pid_file.display()),
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Fake `agy` that prints the URL, records its PID, and sleeps
    /// (ignoring stdin) -- the in-progress conflict / code-timeout paths.
    fn write_fake_agy_url_sleeper(path: &std::path::Path, pid_file: &std::path::Path) {
        std::fs::write(
            path,
            format!(
                "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=sleep123 and sign in'\necho $$ > {}\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Takes `AUTH_TEST_LOCK` for the duration of the calling test, so the
    /// auth tests (which each mutate the global `HOME`) serialize. If a
    /// prior test panicked while holding the lock the mutex is poisoned;
    /// recover via `PoisonError::into_inner` and proceed rather than fail
    /// with a `PoisonError` (a std mutex cannot be re-locked once poisoned,
    /// so on that fallback path the returned guard is `None`). This keeps
    /// one flaky test from turning every later auth test into a
    /// `PoisonError` that masks the real failure.
    fn test_lock() -> Option<std::sync::MutexGuard<'static, ()>> {
        AUTH_TEST_LOCK
            .lock()
            .map_err(|poisoned| poisoned.into_inner())
            .ok()
    }

    /// Polls until `path` exists and is non-empty (the fake agy writes
    /// asynchronously and its writes may land after the flow call
    /// returned), up to 10 s in 25 ms steps.
    fn wait_for_file(path: &std::path::Path) -> String {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(content) = std::fs::read_to_string(path) {
                if !content.is_empty() {
                    return content.trim().to_string();
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "file {} never appeared (or stayed empty) within 10 s: the async agy write did not land",
                path.display()
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
    }

    /// Like `wait_for_file`, but additionally requires the file to be
    /// exactly `expected_len` bytes (the token blob is `printf`ed in one
    /// shot, so its final size is known): a partially written file must
    /// not be treated as complete.
    fn wait_for_token_file(path: &std::path::Path, expected_len: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(len) = std::fs::metadata(path).map(|m| m.len() as usize) {
                if len != 0 && len == expected_len {
                    return;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "token file {} did not reach {} bytes in time (async agy write race)",
                path.display(),
                expected_len
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
    }

    fn wait_for_pid_gone(pid: i32) {
        let proc = std::path::Path::new("/proc").join(pid.to_string());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while proc.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "child {pid} still alive after the kill"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[test]
    fn test_auth_status_reflects_home_credentials() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap(); // empty: no server creds
        set_test_home(&home);

        let agy = tmp.path().join("fake-agy");
        write_fake_agy_success(&agy);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Unauthenticated home -> false.
            let res = app
                .clone()
                .oneshot(get_request("/api/v1/auth/status"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["data"]["authenticated"], false);

            // Valid credentials in the home -> true.
            std::fs::create_dir_all(home.join(".gemini")).unwrap();
            std::fs::write(
                home.join(".gemini").join("oauth_creds.json"),
                r#"{"refresh_token":"rt"}"#,
            )
            .unwrap();
            let res = app
                .oneshot(get_request("/api/v1/auth/status"))
                .await
                .unwrap();
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["data"]["authenticated"], true);
        });
    }

    #[test]
    fn test_auth_already_authenticated_noop() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(home.join(".gemini")).unwrap();
        std::fs::write(
            home.join(".gemini").join("oauth_creds.json"),
            r#"{"refresh_token":"rt"}"#,
        )
        .unwrap();
        set_test_home(&home);

        // A sleeper agy proves no child is spawned on the no-op path.
        let pid_file = tmp.path().join("never.pid");
        let agy = tmp.path().join("fake-agy");
        write_fake_agy_sleeper(&agy, &pid_file);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let res = app
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["exit_code"], 0);
            assert_eq!(
                json_lines(&value, "stdout_lines"),
                vec!["Already authenticated (valid agy credentials found).".to_string()]
            );
            assert_eq!(value["data"]["authenticated"], true);
            assert!(value["data"].get("oauth_url").is_none());
            assert!(!pid_file.exists(), "no agy spawn on the no-op path");
            assert!(
                !tmp.path().join("state").exists(),
                "no state dir created on the no-op path"
            );
        });
    }

    #[test]
    fn test_auth_full_flow_no_credential_bytes_on_wire() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap(); // empty: no server creds
        set_test_home(&home);

        let agy = tmp.path().join("fake-agy");
        write_fake_agy_success(&agy);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // 1. Start: OAuth URL (+ the URL line verbatim), no session
            //    id, no credential bytes.
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let start_body = body_string(res.into_body()).await;
            let value: Value = serde_json::from_str(&start_body).unwrap();
            assert_eq!(value["exit_code"], 0);
            assert_eq!(value["data"]["authenticated"], false);
            assert_eq!(
                value["data"]["oauth_url"],
                "https://antigravity.google/auth?session=fake123"
            );
            assert!(
                value["data"].get("session_id").is_none(),
                "no session ids anymore"
            );
            assert!(
                value["data"].get("files").is_none(),
                "no credential files on the wire"
            );
            assert!(json_lines(&value, "stdout_lines")
                .iter()
                .any(|l| l.contains("https://antigravity.google/auth?session=fake123")));

            // 2. Relay the code to the fixed path: success confirmation.
            let res = app
                .clone()
                .oneshot(post_json_request(
                    "/api/v1/auth/code",
                    r#"{"code":"ABC123"}"#,
                ))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["exit_code"], 0);
            assert_eq!(value["data"]["authenticated"], true);
            assert!(json_lines(&value, "stdout_lines")
                .iter()
                .any(|l| l.contains("authentication completed successfully")));

            // 3. agy wrote its own credentials into the server's home;
            //    neither response carried the credential bytes. The child
            //    wrote them asynchronously (the flow call can return
            //    before the writes land), so wait for each file to be
            //    present and complete before asserting its content.
            let gemini = home.join(".gemini");
            let creds = gemini.join("oauth_creds.json");
            let _ = wait_for_file(&creds);
            assert_eq!(
                std::fs::read_to_string(&creds).unwrap(),
                r#"{"refresh_token":"rt-fake","access_token":"at-fake"}"#
            );
            let accounts = gemini.join("google_accounts.json");
            let _ = wait_for_file(&accounts);
            assert_eq!(
                std::fs::read_to_string(&accounts).unwrap(),
                r#"[{"email":"dev@example.com"}]"#
            );
            let token = gemini
                .join("antigravity-cli")
                .join("antigravity-oauth-token");
            // `printf 'token-fake-bytes'` is exactly 16 bytes: wait for the
            // final size so a partial write is never asserted on.
            wait_for_token_file(&token, 16);
            assert_eq!(std::fs::read(&token).unwrap(), b"token-fake-bytes");
            assert!(
                !start_body.contains("rt-fake") && !start_body.contains("token-fake-bytes"),
                "credential bytes must not cross the wire"
            );

            // 4. The flow only re-runs when the credentials go away:
            //    the home is authenticated now.
            let res = app
                .clone()
                .oneshot(get_request("/api/v1/auth/status"))
                .await
                .unwrap();
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["data"]["authenticated"], true);

            // 5. A second submit (no flow in progress) -> 404.
            let res = app
                .oneshot(post_json_request(
                    "/api/v1/auth/code",
                    r#"{"code":"AGAIN"}"#,
                ))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::NOT_FOUND);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(
                value["error"],
                "no authentication is in progress on this server"
            );
        });
    }

    #[test]
    fn test_auth_second_start_is_409_conflict() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        set_test_home(&home);

        let pid_file = tmp.path().join("sleeper.pid");
        let agy = tmp.path().join("fake-agy");
        write_fake_agy_url_sleeper(&agy, &pid_file);
        let auth = Arc::new(AuthManager::with_timeouts(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(500),
        ));
        let app = router(auth_test_config(tmp.path(), &agy), auth);

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // First start parks the child (URL captured, waiting for the
            // code the user would have gotten from the URL).
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            // Second start while the flow is in progress -> 409.
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::CONFLICT);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(
                value["error"],
                "authentication is already in progress on this server"
            );

            // End the flow: the sleeper ignores the code, so the code
            // deadline kills the parked child (reaped before the test
            // ends).
            let res = app
                .oneshot(post_json_request("/api/v1/auth/code", r#"{"code":"late"}"#))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        });
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();
        wait_for_pid_gone(pid);
    }

    #[tokio::test]
    async fn test_auth_submit_without_flow_is_404() {
        let tmp = tempfile::tempdir().unwrap();
        let agy = tmp.path().join("fake-agy");
        write_fake_agy_success(&agy);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());
        let res = app
            .oneshot(post_json_request("/api/v1/auth/code", r#"{"code":"ABC"}"#))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(
            value["error"],
            "no authentication is in progress on this server"
        );
    }

    #[test]
    fn test_auth_failure_exit_one_is_500_with_output() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        set_test_home(&home);

        let agy = tmp.path().join("fake-agy");
        write_fake_agy_failure(&agy);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let res = app
                .oneshot(post_json_request(
                    "/api/v1/auth/code",
                    r#"{"code":"WRONG"}"#,
                ))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            let error = value["error"].as_str().unwrap().to_string();
            assert!(error.contains("exited with code 1"), "unexpected: {error}");
            assert!(
                error.contains("ERROR: the code was rejected"),
                "agy's output must be surfaced: {error}"
            );
        });
    }

    #[test]
    fn test_auth_url_timeout_is_server_error() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        set_test_home(&home);

        // Fake agy that never prints a URL.
        let agy = tmp.path().join("fake-agy");
        std::fs::write(&agy, "#!/bin/sh\nsleep 30\n").unwrap();
        std::fs::set_permissions(&agy, std::fs::Permissions::from_mode(0o755)).unwrap();
        let auth = Arc::new(AuthManager::with_timeouts(
            std::time::Duration::from_millis(500),
            std::time::Duration::from_secs(5),
        ));
        let app = router(auth_test_config(tmp.path(), &agy), auth);

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let res = app
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert!(value["error"]
                .as_str()
                .unwrap()
                .contains("timed out waiting for the OAuth URL"));
        });
    }

    #[tokio::test]
    async fn test_auth_cancel_without_flow_is_404() {
        let tmp = tempfile::tempdir().unwrap();
        let agy = tmp.path().join("fake-agy");
        write_fake_agy_success(&agy);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());
        let res = app
            .oneshot(post_json_request("/api/v1/auth/cancel", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(
            value["error"],
            "no authentication is in progress on this server"
        );
    }

    #[test]
    fn test_auth_cancel_parked_flow_is_200_and_frees_the_slot() {
        let _lock = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        set_test_home(&home);

        let pid_file = tmp.path().join("sleeper.pid");
        let agy = tmp.path().join("fake-agy");
        write_fake_agy_url_sleeper(&agy, &pid_file);
        let app = router(auth_test_config(tmp.path(), &agy), fast_auth_manager());

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Park a flow.
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/auth", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            // Cancel it.
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/auth/cancel", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(value["data"]["cancelled"], true);

            // The slot is free: cancel again -> 404 with the exact
            // no-flow error text.
            let res = app
                .oneshot(post_json_request("/api/v1/auth/cancel", "{}"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::NOT_FOUND);
            let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
            assert_eq!(
                value["error"],
                "no authentication is in progress on this server"
            );
        });
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();
        wait_for_pid_gone(pid); // the parked child was killed
    }

    #[test]
    fn parse_export_query_folds_repeatable_keys_and_defaults() {
        // Absent query: patch, no selection, no commit, no override.
        let q = parse_export_query(None).unwrap();
        assert_eq!(q.format, "patch");
        assert!(q.message.is_none());
        assert!(q.only.is_empty());
        assert!(q.exclude.is_empty());
        assert!(!q.allow_large);

        // Full query: repeated only/exclude, tar format, message, allow_large.
        let q = parse_export_query(Some(
            "format=tar&message=fix%20it&only=a.txt&only=b/c.txt&exclude=x.log&allow_large=true",
        ))
        .unwrap();
        assert_eq!(q.format, "tar");
        assert_eq!(q.message.as_deref(), Some("fix it"));
        assert_eq!(q.only, vec!["a.txt", "b/c.txt"]);
        assert_eq!(q.exclude, vec!["x.log"]);
        assert!(q.allow_large);

        // An empty `message=` key still means "commit" (a derived message).
        let q = parse_export_query(Some("message=")).unwrap();
        assert_eq!(q.message.as_deref(), Some(""));

        // A malformed percent-encoding is an error, never a silent default.
        assert!(parse_export_query(Some("only=a%Zb")).is_err());

        // Unknown keys are ignored.
        let q = parse_export_query(Some("format=tar&bogus=1")).unwrap();
        assert_eq!(q.format, "tar");
        assert!(!q.allow_large);
    }

    // -------------------------------------------------------------------
    // GET /api/v1/capabilities (HL-91 E1): behind the same bearer layer as
    // every other /api/v1 route; serves the engine capability tables.
    // -------------------------------------------------------------------

    /// A capability router with one registered no-op engine ("testcfg") so
    /// the endpoint reports a real table, not just the empty list.
    #[cfg(feature = "engine")]
    fn capabilities_router(engines: std::sync::Arc<crate::engine::EngineRegistry>) -> Router {
        obs::add_observability(
            crate::core::server::router_with_engines(
                Arc::new(Config::default()),
                Arc::new(AuthManager::new()),
                RemoteControlState::default(),
                engines,
            ),
            auth::AuthState::none(),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        )
    }

    #[cfg(feature = "engine")]
    fn test_noop_engine(id: &'static str) -> std::sync::Arc<dyn crate::engine::Engine> {
        use crate::engine::capability::CapabilityTable;
        use crate::engine::{
            Engine, EngineCtx, EngineError, JobSpec, Run, RunId, SessionSpec, Transport,
        };
        struct TestCfgEngine {
            id: &'static str,
            capabilities: CapabilityTable,
        }
        #[::async_trait::async_trait]
        impl Engine for TestCfgEngine {
            fn id(&self) -> String {
                self.id.to_string()
            }
            async fn start_job(
                &self,
                _spec: JobSpec,
                _ctx: &mut dyn EngineCtx,
            ) -> Result<RunId, EngineError> {
                Ok("cfg-job".to_string())
            }
            async fn resume_job(&self, _run: &Run, _prompt: &str) -> Result<(), EngineError> {
                Ok(())
            }
            async fn job_status(&self, run: &Run) -> Result<crate::engine::RunStatus, EngineError> {
                Ok(run.status)
            }
            async fn start_session(
                &self,
                _spec: SessionSpec,
                _ctx: &mut dyn EngineCtx,
            ) -> Result<RunId, EngineError> {
                Ok("cfg-session".to_string())
            }
            async fn send_message(&self, _run: &Run, _text: &str) -> Result<(), EngineError> {
                Ok(())
            }
            async fn stop_session(&self, _run: &Run, _restart: bool) -> Result<(), EngineError> {
                Ok(())
            }
            fn capabilities(&self) -> CapabilityTable {
                self.capabilities.clone()
            }
        }
        let table = if id == "testcfg" {
            CapabilityTable::builder("testcfg")
                .transport(Transport::StreamJson)
                .native(&["job.start", "job.status", "session.start", "session.stop"])
                .build()
        } else {
            CapabilityTable::builder(id).build()
        };
        std::sync::Arc::new(TestCfgEngine {
            id,
            capabilities: table,
        }) as std::sync::Arc<dyn Engine>
    }

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn test_capabilities_returns_200_with_tables() {
        let mut registry = crate::engine::EngineRegistry::new();
        registry.register(test_noop_engine("testcfg"));
        let app = capabilities_router(std::sync::Arc::new(registry));
        let res = app
            .oneshot(get_request("/api/v1/capabilities"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert!(value["capabilities"].is_array());
        let list = value["capabilities"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["engine"], "testcfg");
        assert_eq!(list[0]["transport"], "stream_json");
        assert_eq!(list[0]["endpoints"]["job.start"], "native");
        assert_eq!(list[0]["endpoints"]["fork"], "unsupported");
    }

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn test_capabilities_empty_registry_returns_empty_list() {
        let reg = std::sync::Arc::new(crate::engine::EngineRegistry::new());
        let app = capabilities_router(reg);
        let res = app
            .oneshot(get_request("/api/v1/capabilities"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["capabilities"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn test_capabilities_requires_token_when_configured() {
        // /capabilities is registered inside the /api/v1 sub-router, so it is
        // behind the same bearer layer as /jobs and /info: 401 without the
        // token, 200 with it.
        let reg = std::sync::Arc::new(crate::engine::EngineRegistry::new());
        let app = obs::add_observability(
            crate::core::server::router_with_engines(
                Arc::new(Config::default()),
                Arc::new(AuthManager::new()),
                RemoteControlState::default(),
                reg,
            ),
            auth::AuthState::token("sekret-token".to_string()),
            Arc::new(|| Ok(())),
            Arc::new(Vec::new),
        );
        let res = app
            .clone()
            .oneshot(get_request("/api/v1/capabilities"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let res = app
            .oneshot(get_request_auth("/api/v1/capabilities", "sekret-token"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn engine_error_unsupported_maps_to_501_body_via_error_to_response() {
        // The error_to_response 501 arm (HL-91 2.6): an EngineError in the
        // anyhow chain answers 501 {"error","code":"unsupported","engine"}.
        let err = anyhow::Error::new(crate::engine::EngineError::unsupported("testcfg", "fork"));
        let res = super::error_to_response(err);
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unsupported");
        assert_eq!(value["engine"], "testcfg");
        assert!(
            value["error"].as_str().unwrap().contains("testcfg"),
            "501 body should name the engine: {value}"
        );
    }

    // -------------------------------------------------------------------
    // POST /api/v1/jobs engine field (HL-91 E3): the agy engine is the job
    // default; "claude" answers 501 (a real engine this pod does not host);
    // any other name answers 400 unknown_engine (a selection error).
    // -------------------------------------------------------------------

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn test_start_job_rejects_claude_with_501() {
        let app = test_router();
        let res = app
            .oneshot(post_json_request(
                "/api/v1/jobs",
                r#"{"project":"p","prompt":"hi","engine":"claude"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unsupported");
        assert_eq!(value["engine"], "claude");
        assert!(
            value["error"].as_str().unwrap().contains("claude"),
            "501 body should name the engine: {value}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn test_start_job_rejects_unknown_engine_with_400() {
        let app = test_router();
        let res = app
            .oneshot(post_json_request(
                "/api/v1/jobs",
                r#"{"project":"p","prompt":"hi","engine":"bogus"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "unknown_engine");
        assert_eq!(value["engine"], "bogus");
    }

    #[tokio::test]
    #[cfg(feature = "engine")]
    async fn test_start_job_absent_and_agy_engine_reach_the_command_path() {
        // `agy` (and an absent engine) are not rejected by engine selection:
        // the request proceeds to the real job handler, which answers 400
        // `project_not_found` for the missing project directory -- never the
        // 501 `unsupported` or 400 `unknown_engine` of engine selection.
        let tmp = tempfile::tempdir().unwrap();
        let app = router(
            Arc::new(Config {
                workspace_root: tmp.path().join("ws"),
                state_root: tmp.path().join("state"),
                agy_path: tmp.path().join("no-such-agy"),
                ..Default::default()
            }),
            Arc::new(AuthManager::new()),
        );
        for body in [
            r#"{"project":"p","prompt":"hi","engine":"agy"}"#,
            r#"{"project":"p","prompt":"hi"}"#,
        ] {
            let res = app
                .clone()
                .oneshot(post_json_request("/api/v1/jobs", body))
                .await
                .unwrap();
            // Engine selection accepted it: the request reached the command
            // layer, which rejects the missing project directory (not the
            // 501 `unsupported` or 400 `unknown_engine` of engine selection).
            let status = res.status();
            let text = body_string(res.into_body()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {body}: {text}");
            assert!(
                text.contains("project_not_found"),
                "body {body} must pass engine selection and fail later: {text}"
            );
        }
    }

    /// `POST /api/v1/jobsets/{id}/end` requires the operator bearer token (it
    /// is one of the whole /api/v1 group) and 404s on unknown jobset ids.
    #[tokio::test]
    async fn test_end_jobset_route_requires_token_and_404s_unknown() {
        // With a token configured, the route is gated like every /api/v1 route.
        let app = test_router_authed("sekret-token");
        let res = app
            .clone()
            .oneshot(post_json_request("/api/v1/jobsets/abc/end", "{}"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // With the token, an unknown jobset id 404s with the JSON error shape.
        let res = app
            .clone()
            .oneshot(post_request_auth(
                "/api/v1/jobsets/ghost/end",
                "sekret-token",
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["code"], "jobset_not_found");
    }

    /// `POST /api/v1/jobsets/{id}/end` on a KNOWN jobset answers 200 with the
    /// new (bumped) epoch and marks it ended (verify now rejects).
    #[tokio::test]
    async fn test_end_jobset_route_ends_known_jobset() {
        // The route runs its work on a blocking-pool thread and drives the
        // process-wide registry (like production), so the agent is registered
        // there under an id unique to this process. Tests that need an empty
        // registry use `with_isolated_registry` (a per-thread registry), which
        // this process-wide registration cannot affect.
        std::env::remove_var(crate::core::jobset::ROOT_KEY_FILE_ENV);
        let cred = crate::core::jobset::register_agent(&format!("job-end-{}", std::process::id()));
        let app = test_router();
        let uri = format!("/api/v1/jobsets/{}/end", cred.jobset_id);
        let res = app.oneshot(post_json_request(&uri, "{}")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "known jobset ends with 200");
        let value: Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(value["ended"], true);
        assert!(value["epoch"].as_u64().unwrap() > cred.epoch);
        // Ended in the registry -> verification now rejects.
        assert!(crate::core::jobset::global_registry().is_ended(&cred.jobset_id));
        crate::core::jobset::drop_agent(&format!("job-end-{}", std::process::id()));
    }

    /// The `start` body deserializes `jobset_id` (a `--jobset` start from the
    /// CLI) into `StartRequest`.
    #[test]
    fn start_request_deserializes_jobset_id() {
        let body = json!({
            "project": "proj",
            "prompt": "hi",
            "sandbox": true,
            "jobset_id": "js-caller",
        });
        let req: StartRequest = serde_json::from_value(body).expect("parse StartRequest");
        assert_eq!(req.jobset_id.as_deref(), Some("js-caller"));

        // Absent field defaults to None.
        let req: StartRequest = serde_json::from_value(json!({
            "project": "proj",
            "prompt": "hi",
        }))
        .expect("parse StartRequest");
        assert_eq!(req.jobset_id, None);
    }
}

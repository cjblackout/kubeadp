//! agy OAuth flow management (`agent-run auth`).
//!
//! The agent-run-api server owns agy's interactive OAuth flow on behalf of
//! remote `agent-run` clients. The API server runs as its own pod
//! (Deployment `agent-run-api` in namespace `opencode`, Service port 8002)
//! with its own writable home (`/home/agent` on the `agent-run-api-home`
//! PVC), so
//! agy **writes the finished credentials into its own home**
//! (`$HOME/.gemini/...`) as part of the exchange: the server never
//! transfers credential bytes over the wire and the CLI installs nothing.
//!
//! Auth is once-per-server, not per-session: at most ONE flow is in
//! progress at a time (a single slot in [`AuthManager`]); a second `start`
//! while one is running is a conflict. Credentials persist on the PVC, so
//! the flow only re-runs when they are missing/invalid:
//!
//! 1. [`AuthManager::start`] first checks the server's own
//!    `$HOME/.gemini/oauth_creds.json` (valid `refresh_token` =>
//!    [`AuthStart::AlreadyAuthenticated`], no agy spawn). Otherwise it
//!    spawns `agy --print "agent-run auth"` (print mode triggers the
//!    interactive OAuth flow) with the server's real home as `HOME` and
//!    working directory, a minimal environment (PATH/HOME/TERM/LANG plus
//!    the resolved proxy variables, if configured -- deliberately no git
//!    credentials, no hindsight), and its own
//!    session/process group (same pattern as the job runner's detached
//!    spawn, so the deadlines can kill the whole child tree). Its stdin is
//!    the **slave of a PTY**, not a plain pipe: agy refuses the interactive
//!    flow when stdin is a plain pipe (it polls for input, sees none, and
//!    bails with "authentication required. Run 'agy' to log in, then
//!    retry."), but a TTY on stdin makes it print the OAuth URL and block
//!    reading the code. The parent keeps the PTY **master** to deliver the
//!    code. stdout/stderr stay plain pipes (the line reader strips any
//!    stray terminal escapes). It then waits ([`URL_TIMEOUT`] by default)
//!    for the OAuth URL to appear on the child's stdout and hands back
//!    [`AuthStart::Pending`] with the URL and everything agy printed so far.
//! 2. [`AuthManager::submit_code`] writes the user's code to the child's
//!    stdin (via the PTY master), then waits ([`CODE_TIMEOUT`] by
//!    default) for the exchange to finish: either the child exits, or
//!    agy's own credential write lands in the flow's home. Success = the
//!    home now holds valid credentials (agy writes them itself), or exit
//!    0 AND the "authentication completed" marker (secondary signal, the
//!    local agy layout; the pod agy prints no marker and keeps running
//!    its `--print` prompt after the exchange, so a still-running child
//!    is killed the moment the credentials land). The slot is cleared on
//!    every exit path.
//!
//! A flow is **ended** the moment its child exits: if agy dies on its own
//! (its internal code timeout, a crash, an external kill) the slot is
//! cleared lazily on the next start/submit -- a second `start` sees a free
//! slot (no conflict), and `submit_code` bails immediately with the exit
//! code and the transcript instead of writing to a corpse. A flow parked
//! past [`PARKED_TIMEOUT`] with its child still alive is reaped (child
//! killed, slot cleared) the same lazy way, and [`AuthManager::cancel`]
//! kills a parked flow on demand, so an abandoned flow cannot block the
//! server's single slot.
//!
//! This module is engine-level: no HTTP, no CLI. The API server exposes it
//! as `GET /api/v1/auth/status` + `POST /api/v1/auth` +
//! `POST /api/v1/auth/code` + `POST /api/v1/auth/cancel`, and the
//! `agent-run` shim drives it.

use std::ffi::CStr;
use std::fs;
use std::io::{BufRead, BufReader, Error as IoError, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;

use crate::core::config::Config;

/// Default deadline for waiting on the OAuth URL on the child's stdout.
pub const URL_TIMEOUT: Duration = Duration::from_secs(120);
/// Default deadline for waiting on the child's exit after the code is
/// submitted.
pub const CODE_TIMEOUT: Duration = Duration::from_secs(120);
/// Default deadline for a flow parked with its URL captured but no code
/// submitted yet (the browser auth can legitimately take a while); an
/// abandoned flow is reaped lazily on the next start/submit so it cannot
/// block the server's single slot forever.
pub const PARKED_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// Poll cadence for the URL / exit waits.
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// A plain `http://` URL is only accepted as the OAuth URL (the fallback
/// in [`extract_url`]) once the transcript has been quiet this long: the
/// transcript is still growing while agy runs, and an early http line
/// (a proxy banner, a localhost callback) must not shadow the https URL
/// printed later.
const HTTP_FALLBACK_QUIET: Duration = Duration::from_millis(500);
/// Grace after the child exits, for in-flight output to reach the capture.
const DRAIN_INTERVAL: Duration = Duration::from_millis(100);

/// Result of starting the auth flow.
#[derive(Debug)]
pub enum AuthStart {
    /// The server already holds valid agy credentials: nothing to do.
    AlreadyAuthenticated,
    /// An interactive OAuth flow is in progress: the agy child is parked
    /// in the manager's single slot. The user opens `oauth_url` in a
    /// browser and returns the code shown on the auth-success page via
    /// [`AuthManager::submit_code`].
    Pending {
        /// The OAuth URL the user should open in a browser.
        oauth_url: String,
        /// Everything agy printed to stdout/stderr so far (up to and
        /// including the URL line; the "paste the code" prompt may arrive
        /// with it). The client prints this verbatim, exactly once.
        prompt_lines: Vec<String>,
    },
}

/// Result of a successful code submission: the tail of agy's post-code
/// output. agy has already written the credential files into its own home
/// -- the server (and this struct) carry no credential bytes.
#[derive(Debug)]
pub struct AuthResult {
    /// agy's stdout after the code was submitted (the start response
    /// already carried the earlier lines, so they are not repeated).
    pub stdout_lines: Vec<String>,
    /// agy's stderr after the code was submitted (may be empty).
    pub stderr_lines: Vec<String>,
}

/// One in-progress OAuth flow: the agy child, the PTY master we keep to
/// write the code into the child's stdin, the home the child ran with
/// (where agy writes the finished credentials), and the captured
/// stdout/stderr transcripts.
struct InProgress {
    child: Mutex<Child>,
    /// Keeps the auth child registered with the process-wide zombie reaper
    /// for the entire duration of the flow (including while parked awaiting
    /// `submit_code`).
    _tracked: crate::core::reaper::TrackedChild,
    /// Parent side of the PTY the child's stdin is attached to. Writing a
    /// line here delivers it to agy's stdin. Kept open for the flow's
    /// whole life (closing it would hang up agy's terminal); dropped when
    /// the flow ends.
    pty_master: OwnedFd,
    out_lines: Arc<Mutex<Vec<String>>>,
    err_lines: Arc<Mutex<Vec<String>>>,
    /// The home the child ran with (agy's `$HOME`): where agy writes the
    /// finished credentials (the success check in `submit_code`).
    home: PathBuf,
    /// When the flow was parked in the slot (URL captured, awaiting code).
    parked_at: Instant,
}

/// Owns the server process's at-most-one in-progress agy OAuth flow.
/// One global instance is created by the API server at startup and shared
/// as an axum extension.
pub struct AuthManager {
    slot: Mutex<Option<InProgress>>,
    url_timeout: Duration,
    code_timeout: Duration,
    parked_timeout: Duration,
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthManager {
    /// Creates a manager with the production deadlines
    /// ([`URL_TIMEOUT`], [`CODE_TIMEOUT`]).
    pub fn new() -> Self {
        Self::with_timeouts(URL_TIMEOUT, CODE_TIMEOUT)
    }

    /// Creates a manager with explicit deadlines (tests use short ones).
    /// Creates a manager with explicit URL/code deadlines (tests use short
    /// ones); the parked-flow deadline is [`PARKED_TIMEOUT`].
    pub fn with_timeouts(url_timeout: Duration, code_timeout: Duration) -> Self {
        Self {
            slot: Mutex::new(None),
            url_timeout,
            code_timeout,
            parked_timeout: PARKED_TIMEOUT,
        }
    }

    /// Clears the slot when the flow is over: the child has already
    /// exited (agy's own code timeout, a crash, an external kill -- the
    /// flow is ended the moment the child dies, regardless of age), or the
    /// child is still alive but parked past the parked deadline (the
    /// client abandoned it, e.g. bailed out before submitting the code):
    /// the child tree is killed. A fresh live flow is left untouched.
    fn reap_stale(&self) {
        let mut slot = self.slot.lock().unwrap();
        let Some(flow) = slot.take() else {
            return;
        };
        // Dead child: the flow is over; the slot is cleared by `take`
        // (try_wait reaped the zombie).
        if matches!(flow.child.lock().unwrap().try_wait(), Ok(Some(_))) {
            return;
        }
        if flow.parked_at.elapsed() < self.parked_timeout {
            *slot = Some(flow);
            return;
        }
        drop(slot);
        kill_child_tree(&mut flow.child.lock().unwrap());
    }

    /// Starts the auth flow on behalf of a client.
    ///
    /// No-op ([`AuthStart::AlreadyAuthenticated`]) when the server's own
    /// `$HOME/.gemini/oauth_creds.json` already carries a valid
    /// `refresh_token`; otherwise spawns the agy OAuth child and waits for
    /// its URL (see the module docs for the full flow).
    pub fn start(&self, config: &Config) -> Result<AuthStart> {
        let home = dirs::home_dir();
        self.start_with_home(config, home.as_deref())
    }

    /// Same as [`start`], with the "server home" made explicit (tests
    /// point it at a temp dir; `None` means there is no home to check and
    /// the child runs with the process's own `$HOME`).
    pub fn start_with_home(&self, config: &Config, home: Option<&Path>) -> Result<AuthStart> {
        self.reap_stale();
        if home.is_some_and(credentials_valid) {
            return Ok(AuthStart::AlreadyAuthenticated);
        }
        if self.slot.lock().unwrap().is_some() {
            return Err(in_progress_conflict());
        }

        let home_path = home
            .map(Path::to_path_buf)
            .or_else(dirs::home_dir)
            .context("could not determine the server home directory (HOME) for the auth flow")?;

        // agy refuses to run the interactive OAuth flow with a plain
        // pipe on stdin: it polls for input, sees none, and bails with
        // "authentication required. Run 'agy' to log in, then retry."
        // (verified against agy 1.2.13: TTY stdin or an immediately
        // readable stdin enters the flow; an idle pipe does not). The
        // child therefore gets a PTY slave as stdin -- it sees a TTY,
        // prints the OAuth URL and blocks reading the code. stdout and
        // stderr stay plain pipes so the transcripts have no terminal
        // rendering (the line reader still strips stray escapes).
        let (pty_master, pty_slave) = open_auth_pty()?;
        let mut cmd = Command::new(config.agy_path.clone());
        cmd.args(["--print", "agent-run auth"])
            .env_clear()
            .envs(auth_env(&home_path, config))
            .current_dir(&home_path)
            .stdin(Stdio::from(pty_slave))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Own session + process group (same pattern as the job runner's
        // detached spawn) so the deadline kills reach the whole child tree.
        crate::core::runner::detached_session(&mut cmd);
        let mut child =
            crate::core::spawn::spawn_command_tracked(&mut cmd, "agy").with_context(|| {
                format!(
                    "Failed to spawn agy for the auth flow (configured path: {})",
                    config.agy_path.display()
                )
            })?;
        // Register the child with the in-server zombie reaper for the whole
        // auth flow: while it is alive the reaper's guard pass steps aside,
        // and if it exits inside the tiny scan-then-drain TOCTOU window the
        // exit status is stashed and handed back through `recover_echild`
        // instead of being lost to a concurrent reaper tick (see
        // `TrackedChild`). The flow always reaps it via `kill_child_tree`.
        let _tracked = crate::core::reaper::TrackedChild::new(child.id());
        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take().expect("stderr was piped");

        // Dedicated reader threads: stdout and stderr are captured
        // line-buffered into separate transcripts (both phases read them).
        let out_lines = Arc::new(Mutex::new(Vec::new()));
        let err_lines = Arc::new(Mutex::new(Vec::new()));
        spawn_line_reader(stdout, out_lines.clone());
        spawn_line_reader(stderr, err_lines.clone());

        // Wait for the OAuth URL on the transcripts (deadline: url_timeout).
        // The transcript is still growing, so the http:// fallback of
        // `extract_url` is only trusted once it has been quiet for
        // HTTP_FALLBACK_QUIET (or the child exited, or the deadline hits):
        // an early plain-http line must not shadow the https URL that agy
        // prints later. An https:// URL is always accepted immediately.
        let deadline = Instant::now() + self.url_timeout;
        let mut last_len = 0;
        let mut len_changed = Instant::now();
        let url = loop {
            let all = transcript(&out_lines, &err_lines);
            if let Some(url) = extract_https_url(&all) {
                break url;
            }
            let quiet = Instant::now().saturating_duration_since(len_changed);
            match child.try_wait() {
                Ok(Some(status)) => {
                    // The child exited: nothing more is coming; take the
                    // http fallback if there is one, else fail as before.
                    thread::sleep(DRAIN_INTERVAL);
                    let all = transcript(&out_lines, &err_lines);
                    if let Some(url) = extract_url(&all) {
                        break url;
                    }
                    bail!(
                        "agy exited before printing the OAuth URL (exit code {}):\n{}",
                        status.code().unwrap_or(-1),
                        all.join("\n")
                    );
                }
                Ok(None) => {}
                Err(e) => bail!("Failed to poll the agy auth child: {e}"),
            }
            if all.len() != last_len {
                last_len = all.len();
                len_changed = Instant::now();
            } else if quiet >= HTTP_FALLBACK_QUIET {
                // Transcript has been quiet: if all we ever got is a plain
                // http URL, it is final (a later https is no longer coming
                // through this transcript); otherwise keep waiting for the
                // deadline.
                if let Some(url) = extract_url(&all) {
                    break url;
                }
            }
            if Instant::now() >= deadline {
                kill_child_tree(&mut child);
                thread::sleep(DRAIN_INTERVAL);
                match extract_url(&transcript(&out_lines, &err_lines)) {
                    Some(url) => break url,
                    None => bail!(
                        "Auth flow timed out waiting for the OAuth URL ({}s). agy output so far:\n{}",
                        self.url_timeout.as_secs(),
                        transcript(&out_lines, &err_lines).join("\n")
                    ),
                }
            }
            thread::sleep(POLL_INTERVAL);
        };

        let prompt_lines = transcript(&out_lines, &err_lines);

        // Publish the flow into the single slot. A parallel start that got
        // past the earlier check while this one waited for its URL wins
        // the slot: kill this child and report the conflict.
        let in_progress = InProgress {
            child: Mutex::new(child),
            _tracked,
            pty_master,
            out_lines,
            err_lines,
            home: home_path,
            parked_at: Instant::now(),
        };
        let mut slot = self.slot.lock().unwrap();
        if slot.is_some() {
            drop(slot);
            kill_child_tree(&mut in_progress.child.lock().unwrap());
            return Err(in_progress_conflict());
        }
        *slot = Some(in_progress);
        Ok(AuthStart::Pending {
            oauth_url: url,
            prompt_lines,
        })
    }

    /// Submits the user's code to the in-progress flow and waits for the
    /// exchange to finish (see the module docs). Success = agy's own
    /// credential write lands in the flow's home, or exit 0 AND the
    /// success marker (secondary signal); a still-running child is killed
    /// when the credentials land first. The slot is cleared on every exit
    /// path. A flow whose child has already exited errors immediately
    /// (exit code + transcript) before any code is written.
    pub fn submit_code(&self, code: &str) -> Result<AuthResult> {
        let in_progress = match self.slot.lock().unwrap().take() {
            Some(flow) => flow,
            None => return Err(no_flow_error()),
        };

        // A flow is ended the moment its child exits: probe before
        // touching the PTY so a dead child (agy's own code timeout, a
        // crash, an external kill) bails immediately with the exit code
        // and the transcript -- no CODE_TIMEOUT wait on a corpse.
        match in_progress.child.lock().unwrap().try_wait() {
            Ok(Some(status)) => {
                let exit_code = status.code().unwrap_or(-1);
                thread::sleep(DRAIN_INTERVAL);
                bail!(
                    "authentication flow already ended (agy exited with code {exit_code}): {}",
                    transcript(&in_progress.out_lines, &in_progress.err_lines).join("\n")
                );
            }
            Ok(None) => {}
            Err(e) => bail!("Failed to wait on the agy auth child: {e}"),
        }

        // Parked past the deadline while the child is still alive: the
        // client abandoned the flow; kill it and report no-flow (the
        // lazy-reap outcome).
        if in_progress.parked_at.elapsed() >= self.parked_timeout {
            kill_child_tree(&mut in_progress.child.lock().unwrap());
            return Err(no_flow_error());
        }

        // 1. Hand the code to agy: write the line to the PTY master,
        //    which delivers it to agy's stdin (canonical line discipline:
        //    agy's read returns the line at the newline). The master
        //    stays open afterwards -- agy reads one line and proceeds;
        //    closing the PTY would hang up its terminal. A failed write
        //    (agy already gave up) is fine: the exit status decides the
        //    outcome below. `pre_out`/`pre_err` mark where the
        //    client-visible transcript begins (everything before it was
        //    already sent in the start response).
        let pre_out = read_lines(&in_progress.out_lines).len();
        let pre_err = read_lines(&in_progress.err_lines).len();
        let code_line = format!("{code}\n");
        let mut written = 0usize;
        while written < code_line.len() {
            // SAFETY: `pty_master` is a valid, open fd and the buffer
            // outlives the call. n <= 0 (agy already hung up) breaks the
            // loop; the exit status below decides the outcome.
            let n = unsafe {
                libc::write(
                    in_progress.pty_master.as_raw_fd(),
                    code_line.as_ptr().add(written) as *const std::ffi::c_void,
                    code_line.len() - written,
                )
            };
            if n <= 0 {
                break;
            }
            written += n as usize;
        }

        // 2. Wait for the exchange to finish (deadline: code_timeout).
        // Two win conditions, polled each iteration: the child exits, or
        // the credential blob lands in the flow's home. The blob
        // (`antigravity-cli/antigravity-oauth-token`) is the pod agy's
        // only credential write (and the LAST write of fixtures that
        // write both layouts), so it is the signal that agy's write
        // burst is complete; breaking on the earlier `oauth_creds.json`
        // would kill the child mid-burst and leave the blob missing.
        // The local agy layout (oauth_creds.json, no blob) exits 0 on
        // its own, so the child-exit path still covers it via the
        // "authentication completed" marker / credentials check below.
        let deadline = Instant::now() + self.code_timeout;
        let child_exited = loop {
            match in_progress.child.lock().unwrap().try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) => {}
                Err(e) => bail!("Failed to wait on the agy auth child: {e}"),
            }
            if token_blob_present(&in_progress.home) {
                break None; // the credential blob (agy's final write) landed
            }
            if Instant::now() >= deadline {
                kill_child_tree(&mut in_progress.child.lock().unwrap());
                thread::sleep(DRAIN_INTERVAL);
                bail!(
                    "Auth flow timed out waiting for agy to finish ({}s)",
                    self.code_timeout.as_secs()
                );
            }
            thread::sleep(POLL_INTERVAL);
        };

        // 3. The credential write may have landed before the child
        //    exited: kill the still-running child (its remaining
        //    `--print` prompt is wasted work) and drain the transcript.
        let exit_code = match &child_exited {
            Some(status) => status.code().unwrap_or(-1),
            None => {
                kill_child_tree(&mut in_progress.child.lock().unwrap());
                // Credentials already landed: the exit code is
                // unreachable (success below never reads it).
                -1
            }
        };

        // Give the line readers a moment to deliver the last bytes agy
        // wrote before the pipe closed.
        thread::sleep(DRAIN_INTERVAL);
        let out_transcript = read_lines(&in_progress.out_lines);
        let err_transcript = read_lines(&in_progress.err_lines);

        // 4. Success = the home now holds valid credentials, or agy
        //    exited 0 with the success marker in the transcript.
        let success = match &child_exited {
            None => true,
            Some(status) => {
                status.code() == Some(0)
                    && (credentials_valid(&in_progress.home)
                        || out_transcript
                            .iter()
                            .chain(err_transcript.iter())
                            .any(|line| line.to_lowercase().contains("authentication completed")))
            }
        };

        // The client already printed everything before the code exchange:
        // report the tail.
        let skip_out = pre_out.min(out_transcript.len());
        let stdout_lines = out_transcript.split_at(skip_out).1.to_vec();
        let skip_err = pre_err.min(err_transcript.len());
        let stderr_lines = err_transcript.split_at(skip_err).1.to_vec();

        if !success {
            let mut tail = stdout_lines;
            tail.extend(stderr_lines);
            let mut message = if exit_code == 0 {
                "agy exited 0 without printing the success marker (authentication did not complete)"
                    .to_string()
            } else {
                format!("agy exited with code {exit_code} without completing authentication")
            };
            if !tail.is_empty() {
                message.push_str(&format!(
                    ". Output after the code was submitted:\n{}",
                    tail.join("\n")
                ));
            }
            bail!("{message}");
        }

        Ok(AuthResult {
            stdout_lines,
            stderr_lines,
        })
    }

    /// Cancels the in-progress flow: kills the agy child tree (SIGTERM,
    /// 2 s grace, SIGKILL) and clears the slot so a fresh flow can start.
    /// A flow whose child has already exited is simply cleared. No flow in
    /// progress -> the same error as [`submit_code`]'s no-flow case (the
    /// API maps it to 404).
    pub fn cancel(&self) -> Result<()> {
        let in_progress = match self.slot.lock().unwrap().take() {
            Some(flow) => flow,
            None => return Err(no_flow_error()),
        };
        kill_child_tree(&mut in_progress.child.lock().unwrap());
        Ok(())
    }
}

/// No auth flow in progress: the exact error text the API maps to 404.
fn no_flow_error() -> anyhow::Error {
    crate::core::error::AgentRunError::not_found(
        "auth_not_in_progress",
        "no authentication is in progress on this server",
    )
    .into()
}

/// A flow is already in progress: the exact error text the API maps to 409.
fn in_progress_conflict() -> anyhow::Error {
    crate::core::error::AgentRunError::conflict(
        "auth_in_progress",
        "authentication is already in progress on this server",
    )
    .into()
}

/// Whether `home` already holds valid agy OAuth credentials. Two
/// observed layouts:
/// - `$HOME/.gemini/oauth_creds.json` parses as JSON and carries a
///   non-empty `refresh_token` (the local-user agy layout), OR
/// - `$HOME/.gemini/antigravity-cli/antigravity-oauth-token` exists and
///   is non-empty (the pod agy layout: the OAuth exchange writes the
///   0600 token blob there and NO oauth_creds.json at all).
pub fn credentials_valid(home: &Path) -> bool {
    oauth_creds_valid(home) || token_blob_present(home)
}

/// The pod agy layout: the OAuth exchange writes the 0600 token blob to
/// `$HOME/.gemini/antigravity-cli/antigravity-oauth-token` (and no
/// oauth_creds.json at all). This is the LAST file agy writes, so its
/// presence (non-empty) doubles as a barrier on the whole write burst:
/// the wait loop in `submit_code` breaks on it before killing the child.
fn token_blob_present(home: &Path) -> bool {
    let token = home
        .join(".gemini")
        .join("antigravity-cli")
        .join("antigravity-oauth-token");
    std::fs::metadata(&token).is_ok_and(|meta| meta.is_file() && meta.len() > 0)
}

/// The `oauth_creds.json` layout: it exists, parses as JSON, and carries
/// a non-empty `refresh_token`.
fn oauth_creds_valid(home: &Path) -> bool {
    let path = home.join(".gemini").join("oauth_creds.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return false;
    };
    value
        .get("refresh_token")
        .and_then(Value::as_str)
        .is_some_and(|token| !token.is_empty())
}

/// Whether the server process's own home (`$HOME`) already holds valid agy
/// credentials (the `GET /auth/status` answer; no agy spawn).
pub fn server_credentials_valid() -> bool {
    dirs::home_dir().is_some_and(|home| credentials_valid(&home))
}

/// The minimal environment for the auth child: PATH/HOME/TERM/LANG plus the
/// resolved proxy variables (see [`crate::core::proxy::resolved_proxy_env`]).
/// Deliberately NO git credentials (no GH tokens) and NO hindsight vars --
/// the child may only do OAuth. The proxy variables are re-injected because
/// the child runs with `env_clear()`: without them the OAuth token exchange
/// could not reach an agy backend that sits behind a proxy, even though job
/// children in the same deployment get the proxy via their inherited env.
fn auth_env(home: &Path, config: &Config) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut env = Vec::new();
    for var in ["PATH", "TERM", "LANG"] {
        if let Ok(value) = std::env::var(var) {
            env.push((
                std::ffi::OsString::from(var),
                std::ffi::OsString::from(value),
            ));
        }
    }
    env.push((
        std::ffi::OsString::from("HOME"),
        std::ffi::OsString::from(home),
    ));
    for (key, value) in
        crate::core::proxy::resolved_proxy_env(Some(config), None).unwrap_or_default()
    {
        env.push((
            std::ffi::OsString::from(key),
            std::ffi::OsString::from(value),
        ));
    }
    env
}

/// Allocates a pseudoterminal and returns `(master, slave)`.
///
/// agy refuses to run its interactive OAuth flow when stdin is a plain
/// pipe (it polls stdin, sees no data, and bails with "authentication
/// required. Run 'agy' to log in, then retry."). Giving the child a real
/// TTY on stdin (the PTY slave) makes it enter the interactive flow: it
/// prints the OAuth URL and blocks reading the code. The parent keeps the
/// master to write the code back.
fn open_auth_pty() -> Result<(OwnedFd, OwnedFd)> {
    // SAFETY: `posix_openpt` returns a fresh, owned fd or -1.
    let raw_master = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY) };
    if raw_master < 0 {
        bail!(
            "could not allocate a PTY for the agy auth flow (posix_openpt: {}); \
             the container must expose /dev/ptmx",
            IoError::last_os_error()
        );
    }
    let master = unsafe { OwnedFd::from_raw_fd(raw_master) };
    // SAFETY: `master` is a valid, owned fd.
    if unsafe { libc::grantpt(master.as_raw_fd()) } < 0 {
        bail!("grantpt failed: {}", IoError::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::unlockpt(master.as_raw_fd()) } < 0 {
        bail!("unlockpt failed: {}", IoError::last_os_error());
    }
    // SAFETY: `ptsname` returns a pointer to a valid, NUL-terminated C
    // string that we copy out before returning.
    let name_ptr = unsafe { libc::ptsname(master.as_raw_fd()) };
    if name_ptr.is_null() {
        bail!("ptsname failed: {}", IoError::last_os_error());
    }
    let slave_name = unsafe { CStr::from_ptr(name_ptr) }
        .to_str()
        .map_err(|e| anyhow!("PTY slave path is not valid UTF-8: {e}"))?;
    let slave_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOCTTY)
        .open(slave_name)
        .with_context(|| format!("could not open PTY slave {slave_name}"))?;
    Ok((master, OwnedFd::from(slave_file)))
}

/// Strips terminal escape sequences (CSI color/cursor codes, OSC
/// sequences, and stray single-character escapes) from one captured line.
/// A child attached to a PTY may emit ANSI even to a non-TTY stdout;
/// this keeps the transcript and the extracted OAuth URL clean.
fn strip_terminal(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('[') => {
                // CSI: intermediates (0x20..=0x3F) then a final byte (0x40..=0x7E).
                for c2 in chars.by_ref() {
                    if (0x40u32..=0x7Eu32).contains(&(c2 as u32)) {
                        break;
                    }
                }
            }
            Some(']') => {
                // OSC: ends at BEL (\x07) or ST (ESC \).
                let osc = chars.by_ref();
                let mut prev_esc = false;
                for c2 in osc {
                    if prev_esc {
                        break; // ST (ESC \)
                    }
                    if c2 == '\u{7}' {
                        break;
                    }
                    prev_esc = c2 == '\u{1b}';
                }
            }
            _ => {
                // ESC + one character (or a lone trailing ESC).
            }
        }
    }
    out
}

/// Reads the shared transcript (the reader threads append under the lock).
fn read_lines(lines: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    lines.lock().unwrap().clone()
}

/// The combined transcript: stdout lines, then stderr lines.
fn transcript(
    out_lines: &Arc<Mutex<Vec<String>>>,
    err_lines: &Arc<Mutex<Vec<String>>>,
) -> Vec<String> {
    let mut all = read_lines(out_lines);
    all.extend(read_lines(err_lines));
    all
}

/// Maximum lines retained per stream (stdout/stderr) during an OAuth flow so
/// a misbehaving child cannot grow the in-memory transcript without bound.
const MAX_AUTH_TRANSCRIPT_LINES: usize = 512;

/// Captures one pipe line-by-line on a detached thread, appending each line
/// to the shared transcript up to [`MAX_AUTH_TRANSCRIPT_LINES`].
fn spawn_line_reader<R>(pipe: R, lines: Arc<Mutex<Vec<String>>>)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(pipe);
        for line in reader.lines() {
            match line {
                Ok(text) => {
                    let cleaned = strip_terminal(&text);
                    let mut guard = lines.lock().unwrap();
                    if guard.len() < MAX_AUTH_TRANSCRIPT_LINES {
                        guard.push(cleaned);
                    }
                }
                Err(_) => break,
            }
        }
    });
}

/// Kills the child's process group (SIGTERM, 2s grace, then SIGKILL) and
/// reaps it. The group signal reaches the child and any descendants when
/// the child is a session leader (the normal case via
/// `runner::detached_session`); when it is not, a direct pid signal is the
/// fallback.
fn kill_child_tree(child: &mut Child) {
    let pid = child.id() as i32;
    // SAFETY: `kill` takes only integer arguments; the kernel dereferences
    // nothing. ESRCH (group gone) is expected and ignored.
    if unsafe { libc::kill(-pid, libc::SIGTERM) } == -1 {
        // SAFETY: as above; single-pid signal.
        let _ = unsafe { libc::kill(pid, libc::SIGTERM) };
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(_) => return, // already reaped
        }
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    // SAFETY: as above.
    if unsafe { libc::kill(-pid, libc::SIGKILL) } == -1 {
        // SAFETY: as above; single-pid signal.
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
    }
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// URL extraction (std only -- no regex dependency)
// ---------------------------------------------------------------------------

/// The OAuth URL from the captured transcript: the first `https://` URL
/// seen anywhere, falling back to the first `http://` URL.
fn extract_url(lines: &[String]) -> Option<String> {
    let mut plain_http = None;
    for line in lines {
        for url in urls_in_line(line) {
            if url.starts_with("https://") {
                return Some(url);
            }
            if plain_http.is_none() {
                plain_http = Some(url);
            }
        }
    }
    plain_http
}

/// The first `https://` URL in the transcript (the only URL the wait loop
/// accepts immediately, before the transcript has settled).
fn extract_https_url(lines: &[String]) -> Option<String> {
    for line in lines {
        for url in &urls_in_line(line) {
            if url.starts_with("https://") {
                return Some(url.clone());
            }
        }
    }
    None
}

/// Every URL in `line`: the text from `http://` or `https://` to the next
/// whitespace (trailing sentence punctuation trimmed).
fn urls_in_line(line: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let mut from = 0;
    while let Some(start) = next_url_at(line, from) {
        let token = line[start..].split_whitespace().next().unwrap_or("");
        let url = token.trim_end_matches(['.', ',', ';', ':', ')', ']', '"', '\'', '!', '?']);
        if !url.is_empty() {
            urls.push(url.to_string());
        }
        from = start + 1;
    }
    urls
}

/// The next `http://`/`https://` occurrence at or after `from` (whichever
/// comes first).
fn next_url_at(line: &str, from: usize) -> Option<usize> {
    let plain = line[from..].find("http://").map(|i| from + i);
    let secure = line[from..].find("https://").map(|i| from + i);
    match (plain, secure) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn test_config(tmp: &Path, agy: &Path) -> Config {
        Config {
            repos_root: tmp.join("ws").join(".agent-repos"),
            workspace_root: tmp.join("ws"),
            state_root: tmp.join("state"),
            agy_path: agy.to_path_buf(),
            claude_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            opencode_path: std::path::PathBuf::from("opencode"),
            ..Default::default()
        }
    }

    fn manager(url: Duration, code: Duration) -> AuthManager {
        AuthManager::with_timeouts(url, code)
    }

    fn production_manager() -> AuthManager {
        manager(Duration::from_secs(5), Duration::from_secs(5))
    }

    /// Fake `agy` for the happy path: banner + OAuth URL + paste prompt,
    /// reads one line from stdin, prints the success markers, writes the
    /// three credential files into its own `$HOME/.gemini` (agy owns its
    /// credentials), then keeps running briefly (like the real agy's
    /// post-exchange `--print` prompt) before exiting 0.
    fn fake_agy_success(tmp: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-success");
        fs::write(
            &agy,
            "#!/bin/sh\necho 'Opening the browser for authentication...'\necho 'Please visit https://antigravity.google/auth?session=fake123 and sign in'\necho 'After authenticating, copy the code displayed in the browser and paste it below:'\nread code\necho 'Completing authentication...'\necho 'authentication completed successfully'\nmkdir -p \"$HOME/.gemini/antigravity-cli\"\nprintf '{\"refresh_token\":\"rt-fake\",\"access_token\":\"at-fake\",\"token_type\":\"Bearer\"}' > \"$HOME/.gemini/oauth_creds.json\"\nprintf '[{\"email\":\"dev@example.com\"}]' > \"$HOME/.gemini/google_accounts.json\"\nprintf 'token-fake-bytes' > \"$HOME/.gemini/antigravity-cli/antigravity-oauth-token\"\nsleep 0.2\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that reads the code, writes ONLY the pod-layout token
    /// file (32 bytes; no oauth_creds.json, no success marker), and then
    /// sleeps forever (the real agy would now run its `--print` prompt)
    /// -- the credentials-first success path.
    fn fake_agy_token_only_sleeper(tmp: &Path, pid_file: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-token-sleeper");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=token123 and sign in'\necho $$ > {}\nread code\nmkdir -p \"$HOME/.gemini/antigravity-cli\"\nprintf 'token-only-bytes-0123456789abcde' > \"$HOME/.gemini/antigravity-cli/antigravity-oauth-token\"\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that prints the URL, reads the code, prints an error, and
    /// exits 1.
    fn fake_agy_failure(tmp: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-failure");
        fs::write(
            &agy,
            "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=fail123 and sign in'\nread code\necho 'ERROR: the code was rejected'\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that prints the URL, reads the code, writes a credential
    /// file, exits 0 -- but WITHOUT the success marker (success must
    /// require the marker, not just exit 0).
    fn fake_agy_no_marker(tmp: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-no-marker");
        fs::write(
            &agy,
            "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=nomarker'\nread code\nmkdir -p \"$HOME/.gemini\"\nprintf '{\"refresh_token\":\"\"}' > \"$HOME/.gemini/oauth_creds.json\"\necho 'done, but no marker'\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that only sleeps (never prints a URL) and records its own
    /// PID first (the tests assert the kill actually reached it).
    fn fake_agy_sleeper(tmp: &Path, pid_file: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-sleeper");
        fs::write(
            &agy,
            format!("#!/bin/sh\necho $$ > {}\nsleep 30\n", pid_file.display()),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that prints the URL, records its PID, and sleeps
    /// (ignoring stdin) -- the in-progress / code-timeout kill paths.
    fn fake_agy_url_sleeper(tmp: &Path, pid_file: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-url-sleeper");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=sleep123 and sign in'\necho $$ > {}\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that dumps the git/hindsight-relevant environment it was
    /// spawned with, prints a URL, records its PID, and sleeps (the
    /// minimal-env assertion + a parked child to kill).
    fn fake_agy_env_sleeper(tmp: &Path, pid_file: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-env-sleeper");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho \"GH=[$(printenv AGENT_RUN_TEST_GH_TOKEN || true)]\"\necho \"HINDSIGHT=[$(printenv AGENT_RUN_TEST_HINDSIGHT_URL || true)]\"\necho \"HOME=$HOME\"\necho 'visit https://envcheck.example/auth now'\necho $$ > {}\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` for the URL-priority races: prints an early plain-http
    /// URL, then (optionally, when `delay_s` is set) prints the https URL
    /// only after `delay_s` seconds so any single early poll sees only the
    /// http line. Records its PID so the test can kill the parked child.
    fn fake_agy_mixed_urls(
        tmp: &Path,
        name: &str,
        pid_file: &Path,
        https_delay_s: Option<&str>,
    ) -> PathBuf {
        let agy = tmp.join(name);
        let https_line = match https_delay_s {
            Some(delay) => format!("sleep {delay}\necho 'visit https://secure.example/auth'"),
            None => "echo 'visit https://secure.example/auth'".to_string(),
        };
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho 'see http://plain.example/cb'\n{https_line}\necho $$ > {}\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Fake `agy` that prints ONLY a plain-http URL and then sleeps: the
    /// http fallback must be accepted once the transcript has been quiet
    /// for a settle window. Records its PID so the test can kill the
    /// parked child.
    fn fake_agy_http_only_sleeper(tmp: &Path, pid_file: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-http-only");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho 'visit http://plain.example/auth'\necho $$ > {}\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Polls until `path` exists and is non-empty (the fake agy writes
    /// asynchronously and its writes may land after the flow call
    /// returned), up to 10 s in 20 ms steps.
    fn wait_for_file(path: &Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(content) = fs::read_to_string(path) {
                if !content.is_empty() {
                    return content.trim().to_string();
                }
            }
            assert!(
                Instant::now() < deadline,
                "file {} never appeared (or stayed empty) within 10 s: the async agy write did not land",
                path.display()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Like `wait_for_file`, but additionally requires the file to be
    /// exactly `expected_len` bytes (the token blob is `printf`ed in one
    /// shot, so its final size is known): a partially written file must
    /// not be treated as complete.
    fn wait_for_token_file(path: &Path, expected_len: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(len) = fs::metadata(path).map(|m| m.len() as usize) {
                if len != 0 && len == expected_len {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "token file {} did not reach {} bytes in time (async agy write race)",
                path.display(),
                expected_len
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_pid_gone(pid: i32) {
        let proc = Path::new("/proc").join(pid.to_string());
        let deadline = Instant::now() + Duration::from_secs(5);
        while proc.exists() {
            assert!(
                Instant::now() < deadline,
                "child {pid} still alive after the kill"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// Fake `agy` that prints the URL, records its PID, sleeps briefly
    /// (so the URL is captured first), and exits non-zero on its own --
    /// agy's internal timeout / a crash: the child dies while the flow is
    /// parked.
    fn fake_agy_url_dead(tmp: &Path, pid_file: &Path) -> PathBuf {
        let agy = tmp.join("fake-agy-url-dead");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho 'Please visit https://antigravity.google/auth?session=dead123 and sign in'\necho $$ > {}\nsleep 0.3\nexit 1\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        agy
    }

    /// Waits until the process has exited: gone from /proc, or a zombie
    /// (exited, not yet reaped by its parent -- an unprobed dead child).
    fn wait_for_pid_exited(pid: i32) {
        let stat = Path::new("/proc").join(pid.to_string()).join("stat");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match fs::read_to_string(&stat) {
                Ok(content) => {
                    // "<pid> (comm) <state> ..." -- comm may contain
                    // spaces/parens, so the state follows the last ')'.
                    let state = content
                        .rsplit(')')
                        .next()
                        .unwrap_or_default()
                        .trim_start()
                        .chars()
                        .next()
                        .unwrap_or_default();
                    if state == 'Z' {
                        return;
                    }
                }
                Err(_) => return, // gone
            }
            assert!(Instant::now() < deadline, "child {pid} still running");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Starts a flow and unwraps the `Pending` result (panics on the
    /// no-op variant).
    fn start_pending(mgr: &AuthManager, config: &Config, home: &Path) -> (String, Vec<String>) {
        match mgr.start_with_home(config, Some(home)).expect("start") {
            AuthStart::Pending {
                oauth_url,
                prompt_lines,
            } => (oauth_url, prompt_lines),
            other => panic!("expected an in-progress flow, got {other:?}"),
        }
    }

    // -- pure helpers -------------------------------------------------------

    #[test]
    fn test_credentials_valid_variants() {
        let tmp = tempdir().unwrap();
        let gemini = tmp.path().join(".gemini");
        fs::create_dir_all(&gemini).unwrap();
        let creds = gemini.join("oauth_creds.json");

        assert!(!credentials_valid(tmp.path()), "missing file");
        fs::write(&creds, "not json").unwrap();
        assert!(!credentials_valid(tmp.path()), "malformed JSON");
        fs::write(&creds, r#"{"refresh_token":""}"#).unwrap();
        assert!(!credentials_valid(tmp.path()), "empty refresh_token");
        fs::write(&creds, r#"{"refresh_token":null}"#).unwrap();
        assert!(!credentials_valid(tmp.path()), "null refresh_token");
        fs::write(&creds, r#"{"access_token":"at","refresh_token":"rt"}"#).unwrap();
        assert!(credentials_valid(tmp.path()), "valid credentials");
    }

    #[test]
    fn test_extract_url_prefers_https_and_falls_back_to_http() {
        assert_eq!(
            extract_url(&[
                "visit http://plain.example/auth now".to_string(),
                "later https://secure.example/auth".to_string(),
            ]),
            Some("https://secure.example/auth".to_string())
        );
        assert_eq!(
            extract_url(&["only http://plain.example/auth here".to_string()]),
            Some("http://plain.example/auth".to_string())
        );
        assert_eq!(
            extract_url(&["https://bare.example/auth".to_string()]),
            Some("https://bare.example/auth".to_string())
        );
        assert_eq!(extract_url(&["no url on this line".to_string()]), None);
        assert_eq!(
            extract_url(&["url: https://x.example/a?b=c.".to_string()]),
            Some("https://x.example/a?b=c".to_string())
        );
        assert_eq!(
            extract_url(&["see https://one.example/a and https://two.example/b".to_string()]),
            Some("https://one.example/a".to_string())
        );
    }

    #[test]
    fn test_extract_https_url_only_accepts_secure() {
        assert_eq!(
            extract_https_url(&["see http://plain.example/cb".to_string()]),
            None
        );
        assert_eq!(
            extract_https_url(&[
                "see http://plain.example/cb".to_string(),
                "visit https://secure.example/auth".to_string(),
            ]),
            Some("https://secure.example/auth".to_string())
        );
        assert_eq!(
            extract_https_url(&["https://bare.example/auth".to_string()]),
            Some("https://bare.example/auth".to_string())
        );
        assert_eq!(
            extract_https_url(&["no url on this line".to_string()]),
            None
        );
        assert_eq!(
            extract_https_url(&["trailing punctuation https://x.example/a.".to_string()]),
            Some("https://x.example/a".to_string())
        );
    }

    #[test]
    fn test_strip_terminal_removes_ansi_and_preserves_text() {
        // Plain text (incl. non-ASCII) passes through unchanged.
        assert_eq!(strip_terminal("hello wörld"), "hello wörld");
        // CSI color codes around a URL are stripped, URL intact.
        assert_eq!(
            strip_terminal("\u{1b}[1;32mhttps://x.example/auth?state=abc\u{1b}[0m done"),
            "https://x.example/auth?state=abc done"
        );
        // Cursor-mode / screen-clear sequences (CSI ends at final byte;
        // the literal 'h' between them survives).
        assert_eq!(strip_terminal("a\u{1b}[?25lh\u{1b}[2Jb"), "ahb");
        // OSC with BEL terminator.
        assert_eq!(strip_terminal("\u{1b}]0;title\u{7}text"), "text");
        // OSC with ST (ESC \) terminator.
        assert_eq!(strip_terminal("\u{1b}]0;title\u{1b}\\text"), "text");
        // Lone trailing ESC.
        assert_eq!(strip_terminal("abc\u{1b}"), "abc");
        // ESC + single character.
        assert_eq!(strip_terminal("a\u{1b}7b"), "ab");
    }

    #[test]
    fn test_auth_env_includes_resolved_proxy_but_nothing_else() {
        let home = PathBuf::from("/home/server");
        let config = Config {
            http_proxy: Some("http://proxy.example:8080".to_string()),
            https_proxy: Some("http://proxy.example:8443".to_string()),
            no_proxy: Some("corp.local".to_string()),
            ..Default::default()
        };
        let env: std::collections::HashMap<String, String> = auth_env(&home, &config)
            .into_iter()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.to_string_lossy().to_string(),
                )
            })
            .collect();
        // The resolved proxy is re-injected (the child runs env_clear()).
        assert_eq!(
            env.get("HTTP_PROXY"),
            Some(&"http://proxy.example:8080".to_string())
        );
        assert_eq!(
            env.get("http_proxy"),
            Some(&"http://proxy.example:8080".to_string())
        );
        assert_eq!(
            env.get("HTTPS_PROXY"),
            Some(&"http://proxy.example:8443".to_string())
        );
        assert_eq!(
            env.get("https_proxy"),
            Some(&"http://proxy.example:8443".to_string())
        );
        assert_eq!(
            env.get("NO_PROXY"),
            Some(&"corp.local,localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc".to_string())
        );
        assert_eq!(
            env.get("no_proxy"),
            Some(&"corp.local,localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc".to_string())
        );
        // ...and nothing else beyond the minimal set + HOME.
        let unexpected = env
            .keys()
            .filter(|k| {
                !matches!(
                    k.as_str(),
                    "HTTP_PROXY"
                        | "http_proxy"
                        | "HTTPS_PROXY"
                        | "https_proxy"
                        | "NO_PROXY"
                        | "no_proxy"
                        | "PATH"
                        | "TERM"
                        | "LANG"
                        | "HOME"
                )
            })
            .count();
        assert_eq!(
            unexpected, 0,
            "only PATH/TERM/LANG/HOME + proxy vars: {env:?}"
        );
        assert_eq!(env.get("HOME"), Some(&"/home/server".to_string()));

        // No proxy configured -> no proxy vars at all.
        let env = auth_env(&home, &Config::default());
        let names: std::collections::HashSet<String> = env
            .into_iter()
            .map(|(k, _)| k.to_string_lossy().to_string())
            .collect();
        for k in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "NO_PROXY",
            "no_proxy",
        ] {
            assert!(!names.contains(k), "{k} must be absent, got {names:?}");
        }
        assert!(names.contains("HOME"));
    }

    #[test]
    fn test_auth_child_sees_config_proxy_despite_env_clear() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("proxy.pid");
        let agy = tmp.path().join("fake-agy-proxy");
        fs::write(
            &agy,
            format!(
                "#!/bin/sh\necho \"PROXY=[$(printenv HTTPS_PROXY || true)]\"\necho \"NOPROXY=[$(printenv NO_PROXY || true)]\"\necho 'visit https://proxycheck.example/auth now'\necho $$ > {}\nsleep 30\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let mut config = test_config(tmp.path(), &agy);
        config.https_proxy = Some("http://auth-proxy:8443".to_string());
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(300));

        let (url, prompt_lines) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://proxycheck.example/auth");
        // The child runs env_clear(); the config proxy reaches it only if the
        // explicit re-injection happens.
        assert!(
            prompt_lines
                .iter()
                .any(|l| l == "PROXY=[http://auth-proxy:8443]"),
            "the env_clear()ed auth child must carry the resolved proxy: {prompt_lines:?}"
        );
        assert!(
            prompt_lines
                .iter()
                .any(|l| l.starts_with("NOPROXY=[") && l.contains("localhost")),
            "the merged NO_PROXY (with cluster defaults) must reach the child: {prompt_lines:?}"
        );

        // Cleanup: kill the parked sleeper via the code-timeout path.
        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    // -- flow (fake agy only) -------------------------------------------------

    /// Regression: an early plain-http line must not be returned as the
    /// OAuth URL on an early poll while the transcript is still growing.
    /// The fake agy prints `http://plain.example/cb` first and only 0.3 s
    /// later prints the https URL -- with the 50 ms poll cadence (and
    /// especially under CPU load) an early poll is guaranteed to see only
    /// the http line, but the wait must keep going until the https URL
    /// arrives.
    #[test]
    fn test_https_url_wins_over_early_http_line() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("mixed.pid");
        let agy = fake_agy_mixed_urls(tmp.path(), "fake-agy-mixed", &pid_file, Some("0.3"));
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(300));

        let (url, _) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://secure.example/auth");

        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    /// A transcript with only a plain-http URL must still yield that URL:
    /// once the transcript has been quiet for the settle window the http
    /// fallback is final. The quiet window (500 ms) is well inside the
    /// 3 s url_timeout, and the child's output completes far before it,
    /// so no timing luck is involved.
    #[test]
    fn test_http_only_url_accepted_after_quiet_window() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("http-only.pid");
        let agy = fake_agy_http_only_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_secs(3), Duration::from_millis(300));

        let (url, _) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "http://plain.example/auth");

        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    /// Regression for the PTY gate: the auth child must see a TTY on
    /// stdin (a plain pipe makes agy refuse the OAuth flow entirely:
    /// "authentication required. Run 'agy' to log in, then retry."). Also
    /// pins that an ANSI-wrapped URL is extracted and reported cleanly,
    /// since a PTY invites terminal rendering.
    #[test]
    fn test_auth_flow_stdin_is_tty_and_ansi_url_is_clean() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let agy = tmp.path().join("fake-agy-tty");
        fs::write(
            &agy,
            "#!/bin/sh\necho \"STDIN_TTY=$([ -t 0 ] && echo yes || echo no)\"\nprintf '\\033[1;32mPlease visit https://tty.example/auth?session=t1\\033[0m and sign in\\n'\nread code\necho 'authentication completed successfully'\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();

        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();
        let (url, prompt) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://tty.example/auth?session=t1");
        let joined = prompt.join("\n");
        assert!(
            joined.contains("STDIN_TTY=yes"),
            "child stdin must be a TTY, transcript: {joined}"
        );
        assert!(
            !joined.contains('\u{1b}'),
            "terminal escapes leaked into the transcript: {joined:?}"
        );
        let result = mgr.submit_code("fake-code").expect("submit");
        let tail = result.stdout_lines.join("\n");
        assert!(tail.contains("authentication completed"), "{tail}");
    }

    #[test]
    fn test_auth_env_is_minimal_no_git_or_hindsight_vars() {
        let _g = crate::test_env_lock();
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        // Unique names: no other test or config loader reads these.
        std::env::set_var("AGENT_RUN_TEST_GH_TOKEN", "must-not-leak");
        std::env::set_var("AGENT_RUN_TEST_HINDSIGHT_URL", "http://hindsight.test");
        let pid_file = tmp.path().join("sleeper.pid");
        let agy = fake_agy_env_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(300));

        let (url, prompt_lines) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://envcheck.example/auth");
        assert!(
            prompt_lines.iter().any(|l| l == "GH=[]"),
            "git token leaked to the auth child: {prompt_lines:?}"
        );
        assert!(
            prompt_lines.iter().any(|l| l == "HINDSIGHT=[]"),
            "hindsight vars leaked to the auth child: {prompt_lines:?}"
        );
        assert!(
            prompt_lines
                .iter()
                .any(|l| l == &format!("HOME={}", home.display())),
            "the child HOME must be the configured home: {prompt_lines:?}"
        );

        // Cleanup: kill the parked sleeper via the code-timeout path.
        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
        std::env::remove_var("AGENT_RUN_TEST_GH_TOKEN");
        std::env::remove_var("AGENT_RUN_TEST_HINDSIGHT_URL");
    }

    #[test]
    fn test_start_already_authenticated_is_no_spawn() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(home.join(".gemini")).unwrap();
        fs::write(
            home.join(".gemini").join("oauth_creds.json"),
            r#"{"refresh_token":"rt"}"#,
        )
        .unwrap();

        // A sleeper agy proves no child is spawned on the no-op path.
        let pid_file = tmp.path().join("never.pid");
        let agy = fake_agy_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);

        let started = production_manager()
            .start_with_home(&config, Some(&home))
            .expect("start");
        assert!(matches!(started, AuthStart::AlreadyAuthenticated));
        assert!(!pid_file.exists(), "no agy child on the no-op path");
        assert!(
            !config.state_root.join("auth").exists(),
            "no state dir created on the no-op path"
        );
    }

    #[test]
    fn test_start_returns_pending_with_url_and_prompt_lines() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("sleeper.pid");
        let agy = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(300));

        let (url, prompt_lines) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=sleep123");
        assert!(
            prompt_lines
                .iter()
                .any(|l| l.contains("https://antigravity.google/auth?session=sleep123")),
            "the URL line must be in prompt_lines: {prompt_lines:?}"
        );

        // The child is parked in the slot: a second start conflicts.
        let err = mgr
            .start_with_home(&config, Some(&home))
            .expect_err("second start must conflict");
        assert_eq!(
            err.to_string(),
            "authentication is already in progress on this server"
        );

        // Cleanup: kill the parked sleeper via the code-timeout path.
        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    #[test]
    fn test_start_and_submit_success_flow() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home"); // empty: no server-side creds
        fs::create_dir_all(&home).unwrap();
        let agy = fake_agy_success(tmp.path());
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        let (url, prompt_lines) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=fake123");
        assert!(
            prompt_lines
                .iter()
                .any(|l| l.contains("https://antigravity.google/auth?session=fake123")),
            "the URL line must be in prompt_lines: {prompt_lines:?}"
        );

        let result = mgr.submit_code("ABC123").expect("submit");
        assert!(result
            .stdout_lines
            .iter()
            .any(|l| l.contains("authentication completed successfully")));
        assert!(
            !result
                .stdout_lines
                .iter()
                .chain(result.stderr_lines.iter())
                .any(|l| l.contains("https://antigravity.google/auth")),
            "start-phase lines must not repeat in the submit output: {result:?}"
        );

        // agy wrote the credential files ITSELF, into its own home -- the
        // result carries no file bytes (there is nowhere to put them).
        let gemini = home.join(".gemini");
        let creds_path = gemini.join("oauth_creds.json");
        let accounts_path = gemini.join("google_accounts.json");
        let token_path = gemini
            .join("antigravity-cli")
            .join("antigravity-oauth-token");
        // agy may write these AFTER submit returns; poll until present.
        wait_for_file(&creds_path);
        assert_eq!(
            fs::read_to_string(&creds_path).unwrap(),
            r#"{"refresh_token":"rt-fake","access_token":"at-fake","token_type":"Bearer"}"#
        );
        wait_for_file(&accounts_path);
        assert_eq!(
            fs::read_to_string(&accounts_path).unwrap(),
            r#"[{"email":"dev@example.com"}]"#
        );
        wait_for_token_file(&token_path, b"token-fake-bytes".len());
        assert_eq!(fs::read(&token_path).unwrap(), b"token-fake-bytes");

        // The slot is free again.
        assert!(mgr.slot.lock().unwrap().is_none());
    }

    #[test]
    fn test_flow_success_makes_home_authenticated() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home"); // empty: no server-side creds
        fs::create_dir_all(&home).unwrap();
        let agy = fake_agy_success(tmp.path());
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        start_pending(&mgr, &config, &home);
        mgr.submit_code("ABC123").expect("submit");

        // agy may still be writing the credential files when submit
        // returned; poll until they have landed before the check.
        wait_for_file(&home.join(".gemini").join("oauth_creds.json"));
        wait_for_token_file(
            &home
                .join(".gemini")
                .join("antigravity-cli")
                .join("antigravity-oauth-token"),
            b"token-fake-bytes".len(),
        );
        // agy's own write into the home is what authenticates the server:
        assert!(
            credentials_valid(&home),
            "agy's own credential write must make the home authenticated"
        );
        // ... and the flow is not re-run while the credentials are valid.
        let pid_file = tmp.path().join("second.pid");
        let sleeper = fake_agy_sleeper(tmp.path(), &pid_file);
        let sleeper_config = test_config(tmp.path(), &sleeper);
        let started = mgr
            .start_with_home(&sleeper_config, Some(&home))
            .expect("start");
        assert!(
            matches!(started, AuthStart::AlreadyAuthenticated),
            "valid credentials must short-circuit the flow"
        );
        assert!(
            !pid_file.exists(),
            "no agy child on the already-authenticated path"
        );
    }

    #[test]
    fn test_submit_failure_exit_nonzero_surfaces_output() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let agy = fake_agy_failure(tmp.path());
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        start_pending(&mgr, &config, &home);
        let err = mgr
            .submit_code("WRONG")
            .expect_err("failure exit must be an error");
        let msg = err.to_string();
        assert!(msg.contains("exited with code 1"), "unexpected: {msg}");
        assert!(
            msg.contains("ERROR: the code was rejected"),
            "agy's output must be surfaced: {msg}"
        );
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "slot cleared on failure"
        );
    }

    #[test]
    fn test_submit_requires_success_marker_even_on_exit_zero() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let agy = fake_agy_no_marker(tmp.path());
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        start_pending(&mgr, &config, &home);
        let err = mgr
            .submit_code("ANY")
            .expect_err("exit 0 without the success marker is not a success");
        assert!(
            err.to_string()
                .contains("exited 0 without printing the success marker"),
            "unexpected: {err}"
        );

        // The slot was cleared: a fresh flow can start.
        let pid_file = tmp.path().join("sleeper.pid");
        let sleeper = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let sleeper_config = test_config(tmp.path(), &sleeper);
        let (url, _) = start_pending(&mgr, &sleeper_config, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=sleep123");
        let err = mgr
            .submit_code("x")
            .expect_err("cleanup: sleeper must time out on the code deadline");
        assert!(err
            .to_string()
            .contains("timed out waiting for agy to finish"));
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    #[test]
    fn test_submit_without_in_progress_flow() {
        let mgr = production_manager();
        let err = mgr
            .submit_code("ABC")
            .expect_err("no flow in progress must fail");
        assert_eq!(
            err.to_string(),
            "no authentication is in progress on this server"
        );
    }

    #[test]
    fn test_second_start_conflicts_and_slot_clears_after_flow() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("sleeper1.pid");
        let agy = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(300));

        start_pending(&mgr, &config, &home);
        let err = mgr
            .start_with_home(&config, Some(&home))
            .expect_err("second start must conflict");
        assert_eq!(
            err.to_string(),
            "authentication is already in progress on this server"
        );

        // End the flow (the sleeper ignores the code; the code deadline
        // kills it) and confirm the slot is free again.
        let err = mgr
            .submit_code("late")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());

        let pid_file2 = tmp.path().join("sleeper2.pid");
        let agy2 = fake_agy_url_sleeper(tmp.path(), &pid_file2);
        let config2 = test_config(tmp.path(), &agy2);
        let (url, _) = start_pending(&mgr, &config2, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=sleep123");
        let _ = mgr.submit_code("x").expect_err("cleanup");
        wait_for_pid_gone(wait_for_file(&pid_file2).parse().unwrap());
    }

    #[test]
    fn test_start_child_exit_before_url_is_an_error() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let agy = tmp.path().join("fake-agy-no-url");
        fs::write(
            &agy,
            "#!/bin/sh\necho 'auth disabled in this build'\nexit 3\n",
        )
        .unwrap();
        fs::set_permissions(&agy, fs::Permissions::from_mode(0o755)).unwrap();
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        let err = mgr
            .start_with_home(&config, Some(&home))
            .expect_err("exit before URL must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("exited before printing the OAuth URL (exit code 3)"),
            "unexpected: {msg}"
        );
        assert!(
            msg.contains("auth disabled in this build"),
            "the child's output must be surfaced: {msg}"
        );
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "slot cleared after the failed start"
        );

        // A fresh flow can start after the failure.
        let pid_file = tmp.path().join("sleeper.pid");
        let sleeper = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let sleeper_config = test_config(tmp.path(), &sleeper);
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(300));
        let _ = start_pending(&mgr, &sleeper_config, &home);
        let _ = mgr.submit_code("x").expect_err("cleanup");
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    #[test]
    fn test_start_spawn_failure_reports_configured_path() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let config = test_config(tmp.path(), &tmp.path().join("no-such-agy"));

        let err = production_manager()
            .start_with_home(&config, Some(&home))
            .expect_err("missing agy must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to spawn agy for the auth flow"),
            "unexpected: {msg}"
        );
        assert!(msg.contains("no-such-agy"), "unexpected: {msg}");
    }

    #[test]
    fn test_start_url_timeout_kills_child() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("sleeper.pid");
        let agy = fake_agy_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_millis(500), Duration::from_secs(5));

        let err = mgr
            .start_with_home(&config, Some(&home))
            .expect_err("url wait must time out");
        let msg = err.to_string();
        assert!(
            msg.contains("timed out waiting for the OAuth URL"),
            "unexpected: {msg}"
        );
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "slot cleared after the url timeout"
        );
        wait_for_pid_gone(wait_for_file(&pid_file).parse().unwrap());
    }

    #[test]
    fn test_submit_code_timeout_kills_child() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("sleeper.pid");
        let agy = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = manager(Duration::from_secs(5), Duration::from_millis(500));

        start_pending(&mgr, &config, &home);
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();

        let err = mgr.submit_code("ABC").expect_err("code wait must time out");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "slot cleared after the code timeout"
        );
        wait_for_pid_gone(pid);
    }

    #[test]
    fn test_parked_flow_is_reaped_after_the_parked_deadline() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("sleeper1.pid");
        let agy = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        // URL/code deadlines generous; parked deadline tiny (test seam).
        let mut mgr = manager(Duration::from_secs(5), Duration::from_secs(5));
        mgr.parked_timeout = Duration::from_millis(300);

        start_pending(&mgr, &config, &home);
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();

        // A second start while the flow is still fresh conflicts.
        let err = mgr
            .start_with_home(&config, Some(&home))
            .expect_err("fresh flow must conflict");
        assert_eq!(
            err.to_string(),
            "authentication is already in progress on this server"
        );

        // Once the parked deadline elapses, the next start reaps the
        // abandoned flow and proceeds with a fresh one.
        thread::sleep(Duration::from_millis(400));
        let pid_file2 = tmp.path().join("sleeper2.pid");
        let agy2 = fake_agy_url_sleeper(tmp.path(), &pid_file2);
        let config2 = test_config(tmp.path(), &agy2);
        let (url, _) = start_pending(&mgr, &config2, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=sleep123");
        wait_for_pid_gone(pid); // the stale child was killed

        // Cleanup: kill the fresh parked sleeper via the code deadline.
        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(err
            .to_string()
            .contains("timed out waiting for agy to finish"));
        wait_for_pid_gone(wait_for_file(&pid_file2).parse().unwrap());
    }

    // -- flow lifecycle: dead child / cancel --------------------------------

    /// The parked child dies on its own (agy's own timeout / a crash): the
    /// slot must be cleared automatically, so a second start does NOT
    /// report in-progress -- regardless of the parked deadline.
    #[test]
    fn test_dead_child_cannot_block_a_fresh_start() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("dead.pid");
        let agy = fake_agy_url_dead(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        let (url, _) = start_pending(&mgr, &config, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=dead123");
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();
        wait_for_pid_exited(pid); // the child died on its own (exit 1)

        // A fresh flow starts cleanly on the auto-cleared slot.
        let pid_file2 = tmp.path().join("sleeper.pid");
        let sleeper = fake_agy_url_sleeper(tmp.path(), &pid_file2);
        let sleeper_config = test_config(tmp.path(), &sleeper);
        let (url2, _) = start_pending(&mgr, &sleeper_config, &home);
        assert_eq!(url2, "https://antigravity.google/auth?session=sleep123");

        // Cleanup: kill the parked sleeper via the code deadline.
        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file2).parse().unwrap());
    }

    /// `submit_code` on an already-ended flow bails IMMEDIATELY (no
    /// CODE_TIMEOUT wait) with an error carrying the exit code and the
    /// transcript.
    #[test]
    fn test_submit_code_on_ended_flow_bails_immediately_with_exit_code() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("dead.pid");
        let agy = fake_agy_url_dead(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        // Generous code deadline: the bail must not wait it out.
        let mgr = manager(Duration::from_secs(5), Duration::from_secs(10));

        start_pending(&mgr, &config, &home);
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();
        wait_for_pid_exited(pid); // the child died on its own (exit 1)

        let started = Instant::now();
        let err = mgr
            .submit_code("ABC")
            .expect_err("a dead child must bail, not succeed");
        let elapsed = started.elapsed();
        let msg = err.to_string();
        assert!(
            elapsed < Duration::from_secs(1),
            "must bail immediately (no code-deadline wait), took {elapsed:?}"
        );
        assert!(
            msg.contains("authentication flow already ended (agy exited with code 1)"),
            "the exit code must be surfaced: {msg}"
        );
        assert!(
            msg.contains("https://antigravity.google/auth?session=dead123"),
            "the transcript must be surfaced: {msg}"
        );
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "slot cleared on the ended-flow error"
        );
    }

    /// `cancel` kills the parked child and clears the slot; a fresh flow
    /// can start on the freed slot.
    #[test]
    fn test_cancel_kills_parked_child_and_clears_slot() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("sleeper.pid");
        let agy = fake_agy_url_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        start_pending(&mgr, &config, &home);
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();

        mgr.cancel().expect("cancel");
        wait_for_pid_gone(pid); // the parked child was killed
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "cancel must clear the slot"
        );

        // A fresh flow can start on the freed slot.
        let pid_file2 = tmp.path().join("sleeper2.pid");
        let agy2 = fake_agy_url_sleeper(tmp.path(), &pid_file2);
        let config2 = test_config(tmp.path(), &agy2);
        let (url, _) = start_pending(&mgr, &config2, &home);
        assert_eq!(url, "https://antigravity.google/auth?session=sleep123");

        // Cleanup: kill the fresh parked sleeper via the code deadline.
        let err = mgr
            .submit_code("x")
            .expect_err("sleeper must time out on the code deadline");
        assert!(
            err.to_string()
                .contains("timed out waiting for agy to finish"),
            "unexpected: {err}"
        );
        wait_for_pid_gone(wait_for_file(&pid_file2).parse().unwrap());
    }

    #[test]
    fn test_cancel_without_in_progress_flow() {
        let mgr = production_manager();
        let err = mgr.cancel().expect_err("no flow in progress must fail");
        assert_eq!(
            err.to_string(),
            "no authentication is in progress on this server"
        );
    }

    /// The pod agy writes ONLY the token file (no oauth_creds.json, no
    /// success marker) and then keeps running (its `--print` prompt): the
    /// submit must succeed via the credentials check and kill the
    /// still-running child.
    #[test]
    fn test_credentials_first_success_kills_still_running_child() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let pid_file = tmp.path().join("token-sleeper.pid");
        let agy = fake_agy_token_only_sleeper(tmp.path(), &pid_file);
        let config = test_config(tmp.path(), &agy);
        let mgr = production_manager();

        start_pending(&mgr, &config, &home);
        let pid: i32 = wait_for_file(&pid_file).parse().unwrap();

        let result = mgr
            .submit_code("ABC")
            .expect("the token file is the success signal (no marker)");
        assert!(
            !result
                .stdout_lines
                .iter()
                .chain(result.stderr_lines.iter())
                .any(|l| l.to_lowercase().contains("authentication completed")),
            "no marker was printed: {result:?}"
        );

        // The still-running child (its wasted --print prompt) was killed.
        wait_for_pid_gone(pid);
        assert!(
            mgr.slot.lock().unwrap().is_none(),
            "slot cleared on the credentials-first success"
        );
    }

    /// The pod-layout token file: every layout combination.
    #[test]
    fn test_credentials_valid_token_layout_variants() {
        let tmp = tempdir().unwrap();
        let gemini = tmp.path().join(".gemini");
        fs::create_dir_all(gemini.join("antigravity-cli")).unwrap();
        let token = gemini
            .join("antigravity-cli")
            .join("antigravity-oauth-token");
        let creds = gemini.join("oauth_creds.json");

        // Neither layout: invalid.
        assert!(!credentials_valid(tmp.path()), "neither layout");
        // Empty token file: invalid.
        fs::write(&token, "").unwrap();
        assert!(!credentials_valid(tmp.path()), "empty token file");
        // Token only (the pod layout): valid.
        fs::write(&token, "token-bytes").unwrap();
        assert!(credentials_valid(tmp.path()), "token only");
        // oauth_creds only (the local-user layout): valid.
        fs::remove_file(&token).unwrap();
        fs::write(&creds, r#"{"refresh_token":"rt"}"#).unwrap();
        assert!(credentials_valid(tmp.path()), "oauth_creds only");
        // Both present: valid.
        fs::write(&token, "token-bytes").unwrap();
        assert!(credentials_valid(tmp.path()), "both present");
    }
}

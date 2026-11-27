//! Server-backed opencode sessions (KA-14).
//!
//! `opencode run -i` is NOT a long-lived interactive process: probed against
//! opencode 1.19.4 in a PTY it answers its positional message and exits with
//! status 0, later input is only echoed by the terminal. A session therefore
//! supervises `opencode serve` (headless, loopback only) and the engine talks
//! to it over HTTP:
//!
//! - readiness: `GET /session/status` answers 200 (`{}` while idle, a running
//!   session shows up as `{"<id>":{"type":"busy"}}`);
//! - the engine creates one opencode session (`POST /session` with an allow
//!   ruleset, body shape verified against 1.19.4: `permission` is an array of
//!   `{permission, pattern, action}` rules, the old object form answers 400)
//!   and keeps its id so a restart continues the same conversation;
//! - a message is `POST /session/{id}/prompt_async` (204), which queues behind a
//!   running turn: the same call is the user-message channel, the A2A injection
//!   channel and the in-flight delivery channel.
//!
//! The port and the session id live in a small JSON state file per session
//! under the engine state root, so the supervisor thread (readiness, restart)
//! and the HTTP delivery path share them without any shared memory.
//!
//! The HTTP client is a minimal `std::net::TcpStream` HTTP/1.1 client
//! (`Connection: close`): the server is loopback-only and the calls are tiny.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Per-request connect/read/write timeout of the loopback HTTP client.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound for the response bytes the client reads (status JSON and
/// session objects are tiny; a runaway body is cut off).
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// How long readiness waits for `opencode serve` to answer.
pub const READY_DEADLINE: Duration = Duration::from_secs(90);

/// How long the background warm-up waits for the supervised `opencode serve` to
/// answer. A cold `opencode serve` (a first start installs into
/// `~/.config/opencode/node_modules`) can take minutes before its first request
/// is served, so the warm-up allows far longer than a single request may.
pub const WARMUP_DEADLINE: Duration = Duration::from_secs(600);

/// The persisted per-session serve state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServeState {
    /// Loopback port of the supervised `opencode serve` of the current start.
    pub port: u16,
    /// The opencode session id (`ses_...`), kept across restarts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Whether the background warm-up has finished (`wait_ready` +
    /// `ensure_session`) for the current start. Reset by the supervisor on a
    /// fresh start and set by the warm-up thread on success, so the sessions
    /// API can report `engine_ready` and message delivery can tell a cold
    /// server (retryable) from a dead one.
    #[serde(default, skip_serializing_if = "is_false")]
    pub ready: bool,
}

fn is_false(b: &bool) -> bool {
    !b
}

/// The allow-all permission ruleset a headless session needs (the shape opencode
/// 1.19.4 accepts; the old object form `{"*":"allow"}` is rejected with 400).
pub const ALLOW_ALL_SESSION_BODY: &str =
    r#"{"permission":[{"permission":"*","pattern":"*","action":"allow"}]}"#;

/// Path of the state file of a session: `<state_root>/opencode-serve/<name>.json`.
/// The name is reduced to a safe file stem so a session name can never escape
/// the directory.
pub fn state_path(state_root: &Path, session_name: &str) -> PathBuf {
    let stem: String = session_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let stem = if stem.is_empty() || stem.chars().all(|c| c == '.') {
        "session".to_string()
    } else {
        stem
    };
    state_root
        .join("opencode-serve")
        .join(format!("{stem}.json"))
}

/// Reads the state file; `None` when it is missing or unreadable.
pub fn read_state(path: &Path) -> Option<ServeState> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Writes the state file atomically (temp file in the same directory, rename).
pub fn write_state(path: &Path, state: &ServeState) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(
        &tmp,
        serde_json::to_vec(state).map_err(std::io::Error::other)?,
    )?;
    std::fs::rename(&tmp, path)
}

/// Picks a free loopback port by binding port 0 and releasing it. The tiny
/// window before `opencode serve` binds it is accepted: a collision makes the
/// start fail readiness and the supervisor restarts with a fresh port.
pub fn pick_free_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    Ok(listener.local_addr()?.port())
}

/// A decoded HTTP answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAnswer {
    /// Status code.
    pub status: u16,
    /// Body text (chunked transfer encoding already decoded).
    pub body: String,
}

/// Performs one HTTP/1.1 request against `127.0.0.1:<port>`.
pub fn http(port: u16, method: &str, path: &str, body: Option<&str>) -> Result<HttpAnswer, String> {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&addr, HTTP_TIMEOUT)
        .map_err(|e| format!("connect 127.0.0.1:{port}: {e}"))?;
    stream
        .set_read_timeout(Some(HTTP_TIMEOUT))
        .and_then(|_| stream.set_write_timeout(Some(HTTP_TIMEOUT)))
        .map_err(|e| format!("socket timeout: {e}"))?;

    let payload = body.unwrap_or("");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    if body.is_some() {
        request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            payload.len()
        ));
    }
    request.push_str("\r\n");
    request.push_str(payload);
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write request: {e}"))?;

    let mut raw = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                raw.extend_from_slice(&buf[..n]);
                if raw.len() > MAX_RESPONSE_BYTES {
                    return Err("response too large".to_string());
                }
            }
            // A server that closes without flushing a trailing read is fine as
            // long as the headers arrived; a timeout before that is an error.
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if raw.is_empty() {
                    return Err(format!("read response: {e}"));
                }
                break;
            }
            Err(e) => return Err(format!("read response: {e}")),
        }
    }
    parse_http_answer(&raw)
}

/// Parses a raw HTTP/1.1 response (status line, headers, optionally chunked body).
pub fn parse_http_answer(raw: &[u8]) -> Result<HttpAnswer, String> {
    let split = find_subslice(raw, b"\r\n\r\n")
        .ok_or_else(|| "malformed response: no header end".to_string())?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let body_bytes = &raw[split + 4..];

    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| "malformed response: empty".to_string())?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("malformed status line: {status_line}"))?;

    let chunked = lines.any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let body = if chunked {
        decode_chunked(body_bytes)?
    } else {
        body_bytes.to_vec()
    };
    Ok(HttpAnswer {
        status,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Decodes an HTTP/1.1 chunked body.
fn decode_chunked(mut data: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end =
            find_subslice(data, b"\r\n").ok_or_else(|| "malformed chunked body".to_string())?;
        let size_line = String::from_utf8_lossy(&data[..line_end]).to_string();
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| format!("bad chunk size: {size_hex}"))?;
        data = &data[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        if data.len() < size + 2 {
            return Err("truncated chunk".to_string());
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
}

/// Waits until `GET /session/status` answers 200 or the deadline/shutdown hits.
pub fn wait_ready(
    port: u16,
    poll_interval: Duration,
    shutdown: &AtomicBool,
    deadline: Duration,
) -> Result<(), String> {
    let end = Instant::now() + deadline;
    let mut last = String::from("no answer yet");
    while Instant::now() < end {
        if shutdown.load(Ordering::SeqCst) {
            return Err("shutdown requested".to_string());
        }
        match http(port, "GET", "/session/status", None) {
            Ok(a) if a.status == 200 => return Ok(()),
            Ok(a) => last = format!("status {}", a.status),
            Err(e) => last = e,
        }
        std::thread::sleep(poll_interval.max(Duration::from_millis(50)));
    }
    Err(format!(
        "opencode serve not ready within {deadline:?}: {last}"
    ))
}

/// Returns the id of the session to use: the stored one when the server still
/// knows it, else a freshly created session.
pub fn ensure_session(port: u16, existing: Option<&str>) -> Result<String, String> {
    if let Some(id) = existing {
        if valid_session_id(id) {
            if let Ok(a) = http(port, "GET", &format!("/session/{id}"), None) {
                if a.status == 200 {
                    return Ok(id.to_string());
                }
            }
        }
    }
    let answer = http(port, "POST", "/session", Some(ALLOW_ALL_SESSION_BODY))?;
    if answer.status != 200 {
        return Err(format!(
            "create session: status {}: {}",
            answer.status,
            truncate(&answer.body, 300)
        ));
    }
    let value: serde_json::Value =
        serde_json::from_str(&answer.body).map_err(|e| format!("create session: bad JSON: {e}"))?;
    value
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|id| valid_session_id(id))
        .map(str::to_string)
        .ok_or_else(|| "create session: no session id in the answer".to_string())
}

/// Warms up the supervised `opencode serve` recorded in `state_file`: polls
/// readiness with the long warm-up deadline and then creates (or reuses) the
/// opencode session, persisting its id, so the first user message never has to
/// wait for a cold server. `stop` aborts the warm-up when the session stops,
/// restarts, or the supervisor shuts down. Returns `Ok(true)` when the server
/// is warm and the session is persisted, `Ok(false)` when `stop` fired, and
/// `Err` when the deadline passed (the server stayed cold).
pub fn warm_up(
    state_file: &Path,
    stop: &AtomicBool,
    deadline: Duration,
    poll_interval: Duration,
) -> Result<bool, String> {
    let state = read_state(state_file).ok_or_else(|| {
        "opencode serve state is missing: the session was not started".to_string()
    })?;
    wait_ready(state.port, poll_interval, stop, deadline)?;
    let id = ensure_session(state.port, state.session_id.as_deref())?;
    let mut current = read_state(state_file).unwrap_or(state);
    current.session_id = Some(id);
    current.ready = true;
    write_state(state_file, &current)
        .map_err(|e| format!("cannot persist the warmed-up session: {e}"))?;
    Ok(true)
}

/// Whether the supervised `opencode serve` answered but has not finished its
/// warm-up yet (cold server, still starting): the delivery may be retried. A
/// truly dead server (nothing listening) is not `not_ready`.
pub fn is_not_ready(err: &str) -> bool {
    err.starts_with("opencode serve not ready")
}

/// Delivers `text` to the session recorded in `state_file`: waits for the
/// server of the current start, creates (or reuses) the opencode session and
/// queues the text as its next user message. The session id is persisted so a
/// restart continues the same conversation.
pub fn deliver(state_file: &Path, text: &str, ready_wait: Duration) -> Result<(), String> {
    let mut state = read_state(state_file).ok_or_else(|| {
        "opencode serve state is missing: the session was not started".to_string()
    })?;
    let never = AtomicBool::new(false);
    wait_ready(state.port, Duration::from_millis(200), &never, ready_wait)?;
    let id = ensure_session(state.port, state.session_id.as_deref())?;
    if state.session_id.as_deref() != Some(id.as_str()) {
        state.session_id = Some(id.clone());
        write_state(state_file, &state)
            .map_err(|e| format!("cannot persist the session id: {e}"))?;
    }
    prompt_async(state.port, &id, text)
}

/// Session ids are `ses_` plus alphanumerics; anything else never reaches a URL.
pub fn valid_session_id(id: &str) -> bool {
    id.starts_with("ses_")
        && id.len() <= 64
        && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Queues `text` as the next user message of the session (204 on success).
pub fn prompt_async(port: u16, session_id: &str, text: &str) -> Result<(), String> {
    if !valid_session_id(session_id) {
        return Err(format!("invalid session id: {session_id}"));
    }
    let body = serde_json::json!({ "parts": [{ "type": "text", "text": text }] }).to_string();
    let answer = http(
        port,
        "POST",
        &format!("/session/{session_id}/prompt_async"),
        Some(&body),
    )?;
    if answer.status == 204 || answer.status == 200 {
        Ok(())
    } else {
        Err(format!(
            "prompt_async: status {}: {}",
            answer.status,
            truncate(&answer.body, 300)
        ))
    }
}

/// Whether the session is running a turn (`/session/status` lists it as busy).
pub fn session_busy(port: u16, session_id: &str) -> Result<bool, String> {
    let answer = http(port, "GET", "/session/status", None)?;
    if answer.status != 200 {
        return Err(format!("session status: status {}", answer.status));
    }
    let value: serde_json::Value =
        serde_json::from_str(&answer.body).map_err(|e| format!("session status: bad JSON: {e}"))?;
    Ok(value.get(session_id).is_some())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// A tiny canned-answer HTTP server: one answer per accepted connection,
    /// recording every request (request line, body).
    struct Canned {
        port: u16,
        seen: Arc<Mutex<Vec<(String, String)>>>,
    }

    fn canned_server(answers: Vec<String>) -> Canned {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = Arc::clone(&seen);
        std::thread::spawn(move || {
            for answer in answers {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                reader.read_exact(&mut body).unwrap();
                seen2.lock().unwrap().push((
                    request_line.trim().to_string(),
                    String::from_utf8_lossy(&body).to_string(),
                ));
                let mut s = stream;
                s.write_all(answer.as_bytes()).unwrap();
            }
        });
        Canned { port, seen }
    }

    fn ok_json(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn state_path_cannot_escape_the_directory() {
        let root = Path::new("/state");
        assert_eq!(
            state_path(root, "sess-1"),
            PathBuf::from("/state/opencode-serve/sess-1.json")
        );
        assert_eq!(
            state_path(root, "../../etc/passwd"),
            PathBuf::from("/state/opencode-serve/.._.._etc_passwd.json")
        );
        assert_eq!(
            state_path(root, ""),
            PathBuf::from("/state/opencode-serve/session.json")
        );
        assert_eq!(
            state_path(root, ".."),
            PathBuf::from("/state/opencode-serve/session.json")
        );
    }

    #[test]
    fn state_round_trips_and_keeps_the_session_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(dir.path(), "s");
        assert!(read_state(&path).is_none());
        write_state(
            &path,
            &ServeState {
                port: 4242,
                session_id: Some("ses_abc".into()),
                ready: false,
            },
        )
        .unwrap();
        assert_eq!(
            read_state(&path),
            Some(ServeState {
                port: 4242,
                session_id: Some("ses_abc".into()),
                ready: false
            })
        );
        write_state(
            &path,
            &ServeState {
                port: 4243,
                session_id: None,
                ready: false,
            },
        )
        .unwrap();
        assert_eq!(read_state(&path).unwrap().port, 4243);
        std::fs::write(&path, b"not json").unwrap();
        assert!(read_state(&path).is_none());
    }

    #[test]
    fn pick_free_port_returns_a_bindable_port() {
        let p = pick_free_port().unwrap();
        assert!(p > 0);
        assert!(TcpListener::bind((Ipv4Addr::LOCALHOST, p)).is_ok());
    }

    #[test]
    fn parses_plain_and_chunked_answers() {
        let a = parse_http_answer(b"HTTP/1.1 204 No Content\r\n\r\n").unwrap();
        assert_eq!((a.status, a.body.as_str()), (204, ""));
        let a = parse_http_answer(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").unwrap();
        assert_eq!((a.status, a.body.as_str()), (200, "{}"));
        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n4;ext=1\r\ndefg\r\n0\r\n\r\n";
        let a = parse_http_answer(chunked).unwrap();
        assert_eq!(a.body, "abcdefg");
        assert!(parse_http_answer(b"garbage").is_err());
        assert!(parse_http_answer(b"HTTP/1.1 xyz\r\n\r\n").is_err());
        assert!(
            parse_http_answer(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
                .is_err()
        );
    }

    #[test]
    fn session_ids_are_validated() {
        assert!(valid_session_id("ses_de2f5ffceffeDzYn7wrDyMZru4"));
        assert!(!valid_session_id("../etc"));
        assert!(!valid_session_id("ses_a/b"));
        assert!(!valid_session_id("abc"));
        assert!(!valid_session_id(&format!("ses_{}", "a".repeat(80))));
    }

    #[test]
    fn wait_ready_succeeds_when_status_answers_200() {
        let srv = canned_server(vec![ok_json("{}")]);
        let shutdown = AtomicBool::new(false);
        assert!(wait_ready(
            srv.port,
            Duration::from_millis(50),
            &shutdown,
            Duration::from_secs(5)
        )
        .is_ok());
        assert_eq!(
            srv.seen.lock().unwrap()[0].0,
            "GET /session/status HTTP/1.1"
        );
    }

    #[test]
    fn wait_ready_gives_up_and_honours_shutdown() {
        let port = pick_free_port().unwrap(); // nothing listens
        let shutdown = AtomicBool::new(false);
        let err = wait_ready(
            port,
            Duration::from_millis(20),
            &shutdown,
            Duration::from_millis(200),
        )
        .unwrap_err();
        assert!(err.contains("not ready"), "{err}");
        shutdown.store(true, Ordering::SeqCst);
        let err = wait_ready(
            port,
            Duration::from_millis(20),
            &shutdown,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(err.contains("shutdown"), "{err}");
    }

    #[test]
    fn ensure_session_creates_with_the_allow_ruleset() {
        let srv = canned_server(vec![ok_json(r#"{"id":"ses_new1","slug":"x"}"#)]);
        assert_eq!(ensure_session(srv.port, None).unwrap(), "ses_new1");
        let seen = srv.seen.lock().unwrap();
        assert_eq!(seen[0].0, "POST /session HTTP/1.1");
        assert_eq!(seen[0].1, ALLOW_ALL_SESSION_BODY);
        assert!(
            seen[0].1.contains(r#""permission":[{"#),
            "the ruleset must be an array"
        );
    }

    #[test]
    fn ensure_session_reuses_a_known_session_and_recreates_a_lost_one() {
        // Known: GET answers 200, no create call.
        let srv = canned_server(vec![ok_json(r#"{"id":"ses_old"}"#)]);
        assert_eq!(
            ensure_session(srv.port, Some("ses_old")).unwrap(),
            "ses_old"
        );
        assert_eq!(srv.seen.lock().unwrap().len(), 1);

        // Lost (404 after a server data reset): a new session is created.
        let not_found =
            "HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                .to_string();
        let srv = canned_server(vec![not_found, ok_json(r#"{"id":"ses_fresh"}"#)]);
        assert_eq!(
            ensure_session(srv.port, Some("ses_gone")).unwrap(),
            "ses_fresh"
        );
        assert_eq!(srv.seen.lock().unwrap().len(), 2);
    }

    #[test]
    fn ensure_session_reports_a_rejected_create() {
        let bad = "HTTP/1.1 400 Bad Request\r\nContent-Length: 21\r\nConnection: close\r\n\r\n{\"_tag\":\"BadRequest\"}".to_string();
        let srv = canned_server(vec![bad]);
        let err = ensure_session(srv.port, None).unwrap_err();
        assert!(err.contains("400") && err.contains("BadRequest"), "{err}");
    }

    #[test]
    fn prompt_async_posts_text_parts_and_accepts_204() {
        let srv = canned_server(vec![
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".to_string()
        ]);
        prompt_async(srv.port, "ses_abc", "hello \"world\"\nline2").unwrap();
        let seen = srv.seen.lock().unwrap();
        assert_eq!(seen[0].0, "POST /session/ses_abc/prompt_async HTTP/1.1");
        let v: serde_json::Value = serde_json::from_str(&seen[0].1).unwrap();
        assert_eq!(v["parts"][0]["type"], "text");
        assert_eq!(v["parts"][0]["text"], "hello \"world\"\nline2");
    }

    #[test]
    fn prompt_async_rejects_bad_ids_and_server_errors() {
        assert!(prompt_async(1, "../x", "t")
            .unwrap_err()
            .contains("invalid session id"));
        let srv = canned_server(vec!["HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\nConnection: close\r\n\r\nboom".to_string()]);
        let err = prompt_async(srv.port, "ses_abc", "t").unwrap_err();
        assert!(err.contains("500") && err.contains("boom"), "{err}");
    }

    #[test]
    fn deliver_creates_the_session_once_persists_it_and_posts_the_message() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = state_path(dir.path(), "s1");
        let srv = canned_server(vec![
            ok_json("{}"),
            ok_json(r#"{"id":"ses_made"}"#),
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".to_string(),
        ]);
        write_state(
            &state_file,
            &ServeState {
                port: srv.port,
                session_id: None,
                ready: false,
            },
        )
        .unwrap();
        deliver(&state_file, "hi there", Duration::from_secs(5)).unwrap();
        let seen = srv.seen.lock().unwrap();
        assert_eq!(seen[0].0, "GET /session/status HTTP/1.1");
        assert_eq!(seen[1].0, "POST /session HTTP/1.1");
        assert_eq!(seen[2].0, "POST /session/ses_made/prompt_async HTTP/1.1");
        assert!(seen[2].1.contains("hi there"));
        assert_eq!(
            read_state(&state_file).unwrap().session_id.as_deref(),
            Some("ses_made")
        );
    }

    #[test]
    fn deliver_reports_a_missing_state_and_a_dead_server() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = state_path(dir.path(), "none");
        assert!(deliver(&state_file, "x", Duration::from_millis(100))
            .unwrap_err()
            .contains("not started"));
        write_state(
            &state_file,
            &ServeState {
                port: pick_free_port().unwrap(),
                session_id: None,
                ready: false,
            },
        )
        .unwrap();
        assert!(deliver(&state_file, "x", Duration::from_millis(200))
            .unwrap_err()
            .contains("not ready"));
    }

    #[test]
    fn session_busy_reads_the_status_map() {
        let srv = canned_server(vec![ok_json(r#"{"ses_a":{"type":"busy"}}"#), ok_json("{}")]);
        assert!(session_busy(srv.port, "ses_a").unwrap());
        assert!(!session_busy(srv.port, "ses_a").unwrap());
    }

    /// A canned-answer server that sleeps `first_delay` before it answers its
    /// FIRST request and answers every later request immediately: the
    /// cold-start shape of a real `opencode serve` (one slow first request,
    /// instant after).
    fn late_canned_server(first_delay: Duration, answers: Vec<String>) -> u16 {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let mut answers = answers.into_iter();
            let mut first = true;
            for answer in &mut answers {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                reader.read_exact(&mut body).unwrap();
                if first {
                    first = false;
                    std::thread::sleep(first_delay);
                }
                let mut s = stream;
                s.write_all(answer.as_bytes()).unwrap();
            }
        });
        port
    }

    #[test]
    fn warm_up_waits_for_a_late_server_and_persists_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = state_path(dir.path(), "warm");
        let port = late_canned_server(
            Duration::from_millis(1200),
            vec![ok_json("{}"), ok_json(r#"{"id":"ses_late"}"#)],
        );
        write_state(
            &state_file,
            &ServeState {
                port,
                session_id: None,
                ready: false,
            },
        )
        .unwrap();
        let stop = AtomicBool::new(false);
        let started = Instant::now();
        let warmed = warm_up(
            &state_file,
            &stop,
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .unwrap();
        assert!(warmed, "warm-up must complete once the server answers");
        assert!(
            started.elapsed() >= Duration::from_millis(1100),
            "warm-up must wait out the cold first request"
        );
        let state = read_state(&state_file).unwrap();
        assert!(state.ready, "the ready flag must be set by the warm-up");
        assert!(
            state.session_id.as_deref() == Some("ses_late"),
            "the opencode session id must be persisted by the warm-up"
        );
    }

    #[test]
    fn warm_up_aborts_on_stop_before_warming() {
        let dir = tempfile::tempdir().unwrap();
        let state_file = state_path(dir.path(), "warm-stop");
        let port = pick_free_port().unwrap(); // nothing listens
        write_state(
            &state_file,
            &ServeState {
                port,
                session_id: None,
                ready: false,
            },
        )
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let signaller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            stop2.store(true, Ordering::SeqCst);
        });
        let res = warm_up(
            &state_file,
            &stop,
            Duration::from_secs(30),
            Duration::from_millis(50),
        );
        let err = res.unwrap_err();
        assert!(err.contains("shutdown"), "{err}");
        signaller.join().unwrap();
        // A deadline pass (server never answers and no stop) is a plain error.
        let stop3 = AtomicBool::new(false);
        let err = warm_up(
            &state_file,
            &stop3,
            Duration::from_millis(250),
            Duration::from_millis(50),
        )
        .unwrap_err();
        assert!(err.contains("not ready"), "{err}");
        assert!(is_not_ready(&err));
    }

    #[test]
    fn is_not_ready_classifies_only_a_cold_server() {
        assert!(is_not_ready(
            "opencode serve not ready within 30s: read response: Resource temporarily unavailable"
        ));
        assert!(!is_not_ready("connect 127.0.0.1:1: Connection refused"));
        assert!(!is_not_ready(
            "opencode serve state is missing: the session was not started"
        ));
    }
}

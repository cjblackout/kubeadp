//! `GET /api/v1/jobs/{id}/events` -- Server-Sent-Events stream of a job's log
//! and status, consumed by `agent-run watch` (replaces the client's 150ms
//! `log?offset` polling).
//!
//! Wire format (plain SSE; no `id`/`retry` fields):
//! - `event: status` with `data: {"status":"...","elapsed_seconds":n}` --
//!   once on connect and on every change of the job's status
//! - `event: log` with `data: {"offset":n,"text":"<new log text>"}` -- the
//!   log bytes appended since the previous `log` event; `offset` is the file
//!   offset just past those bytes (a client resuming passes it back as
//!   `?offset=`)
//! - `event: end` with `data: {"status":"<terminal status>"}` -- sent when
//!   the job reaches a terminal state, then the stream closes
//! - `: keepalive` SSE comment every 15s so proxies do not drop the idle
//!   connection
//!
//! The log file is polled every 500ms, the job status every 1s. An unknown
//! job is NOT a stream: the route resolves the job before responding and
//! answers with the same typed 404 JSON error body as every other route (see
//! [`super::error_to_response`]). Status classification and log reading reuse
//! the exact helpers the other routes use (`job::compute_status`,
//! [`super::read_log_bytes`]).

use crate::core::config::Config;
use crate::core::job::{compute_status, JobMetadata, JobStatus};
use axum::body::Body;
use axum::extract::State;
use axum::http::header::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response as AxumResponse;
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::reject::{ApiPath, ApiQuery};

use super::{error_to_response, LOG_OFFSET_HEADER};

/// The SSE route handler. Resolves the job BEFORE streaming so an unknown id
/// returns the same typed 404 JSON as every other route (never a stream).
pub async fn events_job(
    State(config): State<Arc<Config>>,
    ApiPath(job_id): ApiPath<String>,
    ApiQuery(query): ApiQuery<EventsQuery>,
) -> AxumResponse {
    events_response(&config, &job_id, &query)
}

/// SSE content type (event streams are never compressed or cached).
pub const EVENTS_CONTENT_TYPE: &str = "text/event-stream";
/// `log` events: cadence of the log-file poll.
pub const LOG_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// `status` events: cadence of the status poll.
pub const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// `: keepalive` comments: the fixed wire cadence. It is NOT publicly
/// tunable: the old `?debug_fast_keepalive=` query knob let ANY client force
/// 100ms pump ticks -- a CPU amplifier. Tests that need a faster cadence
/// drive `pump` directly (it takes the interval as a parameter).
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// Minimum pump naps until the next due tick (the cadences are 500ms/1s, so
/// 25ms keeps the thread mostly sleeping without missing a tick by much).
const PUMP_SLICE: Duration = Duration::from_millis(25);
/// Hard cap on the raw bytes a single `log` event may carry (per tick). Any
/// bytes beyond it wait for the next tick (the offset only advances past
/// what was actually emitted).
pub const LOG_SLICE_CAP: u64 = 64 * 1024;
/// Upper bound on concurrent event streams; beyond it a request gets a 503.
pub const MAX_EVENT_STREAMS: usize = 64;

/// `events?offset=` query: resume log delivery from byte offset N.
#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub offset: Option<u64>,
}

type PumpErr = String;
type Frame = Result<Vec<u8>, PumpErr>;

/// Global cap on concurrent event streams. Each active stream holds one
/// [`StreamSlot`] (an RAII guard that releases its slot on drop, whether the
/// client disconnects, the job ends, or the slot is dropped out from under a
/// test), so the counter can never leak past the streams that actually exist.
static ACTIVE_STREAMS: AtomicUsize = AtomicUsize::new(0);

/// RAII guard for one active event stream: reserves a slot on creation and
/// releases it on drop. The pump thread owns one, so the slot is released the
/// moment the pump notices the client is gone, even if the HTTP response is
/// still being held open elsewhere.
pub struct StreamSlot;

impl StreamSlot {
    /// Reserves a slot, or `None` when all `MAX_EVENT_STREAMS` are taken.
    pub fn try_acquire() -> Option<Self> {
        loop {
            let active = ACTIVE_STREAMS.load(Ordering::Acquire);
            if active >= MAX_EVENT_STREAMS {
                return None;
            }
            match ACTIVE_STREAMS.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self),
                Err(_) => continue,
            }
        }
    }

    /// Number of slots currently held (unit tests only).
    #[cfg(test)]
    pub(crate) fn held() -> usize {
        ACTIVE_STREAMS.load(Ordering::Acquire)
    }
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        ACTIVE_STREAMS.fetch_sub(1, Ordering::Release);
    }
}

/// One wire event, in the exact order it should reach the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// `event: status` + the compact status JSON as `data`.
    Status(String),
    /// `event: log` + the compact `{"offset":n,"text":"..."}` JSON as `data`.
    Log(String),
    /// `event: end` + the compact `{"status":"..."}` JSON as `data`.
    End(String),
    /// A `: keepalive` SSE comment.
    Keepalive,
}

/// Formats one event into its SSE bytes (the client parses exactly this).
pub fn format_event(e: &Event) -> String {
    match e {
        Event::Status(data) => sse_field("status", data),
        Event::Log(data) => sse_field("log", data),
        Event::End(data) => sse_field("end", data),
        // The comment is its own SSE block: it terminates with the blank
        // line, or the next frame glues onto it (`: keepaliveevent: log`).
        Event::Keepalive => ": keepalive\n\n".to_string(),
    }
}

/// `event: <name>\ndata: <data>\n\n` -- the SSE framing for every named
/// event. Data payloads are single-line JSON (serde_json never emits raw
/// newlines), so one `data:` field always carries the whole payload.
fn sse_field(event: &str, data: &str) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

/// `event: status` payload: `{"status":"...","elapsed_seconds":n}`. A struct
/// (not `json!`) so the documented key order -- `status` first -- is stable:
/// `serde_json::Value` would sort the keys alphabetically.
#[derive(Serialize)]
struct StatusPayload<'a> {
    status: &'a str,
    elapsed_seconds: u64,
}

/// The job's elapsed wall-clock seconds, computed the same way the status
/// command's `Elapsed` line is: since the stored `started_at`, falling back
/// to `created_at` when the runner never recorded a start (never negative).
fn job_elapsed_secs(meta: &JobMetadata) -> u64 {
    let anchor = meta.started_at.unwrap_or(meta.created_at);
    Utc::now()
        .signed_duration_since(anchor)
        .num_seconds()
        .max(0) as u64
}

pub fn format_status_data(status: &JobStatus, elapsed_secs: u64) -> String {
    serde_json::to_string(&StatusPayload {
        status: status.as_str(),
        elapsed_seconds: elapsed_secs,
    })
    .expect("the status payload only holds JSON-serializable fields")
}

/// `event: log` payload: `{"offset":n,"text":"<new log text>"}`. The offset
/// is the log-file position just past the appended text (byte-accurate: the
/// text is decoded from exactly those bytes, so resuming with it never
/// re-sends or drops bytes, lossy-decoded or not).
#[derive(Serialize)]
struct LogPayload<'a> {
    offset: u64,
    text: &'a str,
}

pub fn format_log_data(offset: u64, text: &str) -> String {
    serde_json::to_string(&LogPayload { offset, text })
        .expect("the log payload only holds JSON-serializable fields")
}

/// `event: end` payload: `{"status":"<terminal status>"}`.
#[derive(Serialize)]
struct EndPayload<'a> {
    status: &'a str,
}

pub fn format_end_data(status: &JobStatus) -> String {
    serde_json::to_string(&EndPayload {
        status: status.as_str(),
    })
    .expect("the end payload only holds JSON-serializable fields")
}

/// Reads up to `LOG_SLICE_CAP` bytes of the log past `offset`, stopping at a
/// UTF-8 character boundary, and returns the decodable bytes plus the offset
/// just past them.
///
/// Two guards keep one `log` event bounded and intact:
/// - the raw-bytes cap: anything beyond the slice waits for the next tick
///   (the offset only advances past the bytes actually emitted);
/// - the UTF-8 boundary: if the slice ends inside a multibyte sequence, the
///   incomplete tail bytes are held back the same way (and decode losslessly
///   on the next tick) instead of becoming `U+FFFD`, so a character
///   straddling the cap still arrives exactly once over two events.
///
/// A mid-stream 404 (log purged / job cleaned) is not fatal: the caller
/// skips the round and `end` will still arrive from the terminal status.
fn read_log_slice(config: &Config, job_id: &str, offset: u64) -> anyhow::Result<(Vec<u8>, u64)> {
    let (bytes, end) = super::read_log_bytes(config, job_id, offset)?;
    if bytes.len() <= LOG_SLICE_CAP as usize {
        return Ok((bytes, end));
    }
    // The slice would exceed the cap: walk back from the cut over continuation
    // bytes (0b10xxxxxx) until it lands on a sequence start, i.e. the last
    // COMPLETE character boundary strictly within the cap.
    let mut cut = LOG_SLICE_CAP as usize;
    while cut > 0 && (bytes[cut] & 0xC0) == 0x80 {
        cut -= 1;
    }
    if std::str::from_utf8(&bytes[..cut]).is_err() {
        // The cap fell inside a malformed sequence: keep backing off until the
        // emitted prefix decodes losslessly.
        while cut > 0 && std::str::from_utf8(&bytes[..cut]).is_err() {
            cut -= 1;
        }
    }
    if cut == 0 {
        return Ok((Vec::new(), offset)); // nothing complete within the cap
    }
    Ok((bytes[..cut].to_vec(), offset + cut as u64))
}

/// One poll round:
/// 1. a `Log` with the new log text (offset just past it) when the file grew
///    (at most `LOG_SLICE_CAP` raw bytes per round, cut on a UTF-8 boundary)
/// 2. when `check_status` and the status changed: a `Status` (with the
///    job's fresh elapsed time since its start, not this stream's age)
/// 3. when the newly checked status is terminal: a final `Log` (drain of
///    anything written right before termination, same cap) and the `End`
///    event, then `None` to stop the pump loop
///
/// This is the unit under test: the pump loop only decides what is due,
/// drives this, and sleeps between rounds.
pub fn poll_once(
    config: &Config,
    meta: &JobMetadata,
    check_status: bool,
    offset: &mut u64,
    last_status: &mut JobStatus,
) -> Option<Vec<Event>> {
    let mut out: Vec<Event> = Vec::new();
    // Log slice since the last event (missing file or no growth: nothing).
    if let Ok((bytes, new_offset)) = read_log_slice(config, &meta.job_id, *offset) {
        if !bytes.is_empty() {
            out.push(Event::Log(format_log_data(
                new_offset,
                &String::from_utf8_lossy(&bytes),
            )));
            *offset = new_offset;
        }
    }
    if !check_status {
        return Some(out);
    }
    let status = compute_status(meta, &config.state_root);
    if status != *last_status {
        out.push(Event::Status(format_status_data(
            &status,
            job_elapsed_secs(meta),
        )));
    }
    *last_status = status;
    if status.is_terminal() {
        // Drain any bytes written between the grow-check above and the
        // terminal classification (same per-round cap and boundary rules).
        if let Ok((bytes, new_offset)) = read_log_slice(config, &meta.job_id, *offset) {
            if !bytes.is_empty() {
                out.push(Event::Log(format_log_data(
                    new_offset,
                    &String::from_utf8_lossy(&bytes),
                )));
                *offset = new_offset;
            }
        }
        out.push(Event::End(format_end_data(&status)));
        return Some(out);
    }
    Some(out)
}

/// Builds the response for `GET /api/v1/jobs/{id}/events`.
///
/// Unknown job / non-unique prefix: the same typed JSON error as the other
/// routes (a 404 with `{"error","code"}`), never a stream, because the job
/// is resolved before any body is produced.
pub fn events_response(config: &Arc<Config>, job_id: &str, query: &EventsQuery) -> AxumResponse {
    let meta = match JobMetadata::resolve(&config.state_root, job_id) {
        Ok(meta) => meta,
        Err(err) => return error_to_response(err),
    };
    // Cap on concurrent streams: a rejected request never resolves to a pump
    // (no thread, no slot), so the counter only grows for live streams.
    let slot = match StreamSlot::try_acquire() {
        Some(slot) => slot,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "too many event streams",
                    "code": "too_many_streams"
                })),
            )
                .into_response();
        }
    };
    let initial_status = compute_status(&meta, &config.state_root);
    let started = Instant::now();
    let config = Arc::clone(config);
    let resume_from = query.offset.unwrap_or(0);

    let (tx, rx) = mpsc::channel::<Frame>(64);
    let handle = tokio::runtime::Handle::current();
    std::thread::spawn(move || {
        handle.block_on(pump(
            tx,
            config,
            meta,
            initial_status,
            started,
            resume_from,
            KEEPALIVE_INTERVAL,
            slot,
        ));
    });

    // `ReceiverStream<Item = Result<Vec<u8>, String>>` satisfies
    // `TryStream` via the futures-core blanket impl: `Vec<u8>` coerces into
    // `Bytes` and `String` into `Box<dyn Error + Send + Sync>`, so no
    // `.map` (and no `StreamExt` import) is needed for `Body::from_stream`.
    let stream = ReceiverStream::new(rx);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(EVENTS_CONTENT_TYPE),
    );
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    // The server's resume offset for this stream: what a reconnecting client
    // should pass as `?offset=` to continue exactly where this one started.
    headers.insert(
        LOG_OFFSET_HEADER,
        resume_from
            .to_string()
            .parse()
            .expect("numeric header value"),
    );
    (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
}

#[allow(clippy::too_many_arguments)]
async fn pump(
    tx: mpsc::Sender<Frame>,
    config: Arc<Config>,
    mut meta: JobMetadata,
    mut last_status: JobStatus,
    started: Instant,
    mut offset: u64,
    keepalive: Duration,
    // RAII guard: held (alive) for the whole pump, released on return.
    _slot: StreamSlot,
) {
    // The on-connect status event precedes every log/keepalive check. Its
    // elapsed time is the job's own age (its record's start, not this
    // stream's), matching every later `status` frame.
    if !send(
        &tx,
        &Event::Status(format_status_data(&last_status, job_elapsed_secs(&meta))),
    )
    .await
    {
        return;
    }
    // Seed the first round as due: even a job that is already terminal at
    // connect time must deliver its pre-existing log and `end` on the first
    // tick, not after the first cadence has elapsed. Subtracting the full
    // cadence makes `last + CADENCE <= now` hold immediately (seeding from
    // slightly before `started` would leave the first tick NOT due and a
    // terminal job's log-only tick would end the stream without `end`).
    let mut last_log = started - LOG_POLL_INTERVAL;
    let mut last_status_check = started - STATUS_POLL_INTERVAL;
    let mut last_keepalive = started;
    let mut keepalive_due = false;
    loop {
        // The client stopped reading (request cancelled / stream dropped):
        // the channel closes the instant the receiver side goes away, so
        // every tick notices it -- not just when a keepalive happens to fire.
        if tx.is_closed() {
            return;
        }
        if !keepalive_due && last_keepalive + keepalive <= Instant::now() {
            keepalive_due = true;
        }
        let log_due = last_log + LOG_POLL_INTERVAL <= Instant::now();
        let status_due = last_status_check + STATUS_POLL_INTERVAL <= Instant::now();
        if !log_due && !status_due && !keepalive_due {
            tokio::time::sleep(PUMP_SLICE).await;
            continue;
        }
        if keepalive_due {
            if !send(&tx, &Event::Keepalive).await {
                return;
            }
            last_keepalive = Instant::now();
            keepalive_due = false;
        }
        if status_due {
            match JobMetadata::load(&config.state_root, &meta.job_id) {
                Ok(fresh) => {
                    meta = fresh;
                }
                // The record disappeared mid-stream (clean/gc raced): end
                // the stream as `missing` -- the client's contract treats
                // "record no longer exists" as non-done, mirroring the core
                // `wait` behavior.
                Err(_) => {
                    send(&tx, &Event::End(json!({ "status": "missing" }).to_string())).await;
                    return;
                }
            }
        }
        if let Some(events) = poll_once(&config, &meta, status_due, &mut offset, &mut last_status) {
            for event in &events {
                if !send(&tx, event).await {
                    return; // client disconnected: drop the pump
                }
            }
        }
        if log_due {
            last_log = Instant::now();
        }
        if status_due {
            last_status_check = Instant::now();
        }
        if last_status.is_terminal() {
            return; // `end` was flushed: the stream is complete
        }
    }
}

async fn send(tx: &mpsc::Sender<Frame>, event: &Event) -> bool {
    tx.send(Ok(format_event(event).into_bytes())).await.is_ok()
}

// ---------------------------------------------------------------------------
// Unit tests: the format functions, the resume offset, the wire round-trip
// (parsing the formatter's output the way the client does), and the
// router-level reads (unknown job -> typed 404 JSON, not a stream; finished
// job -> first events read off the body; ?offset= resume).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::auth::AuthManager;
    use chrono::Utc;
    use http_body_util::BodyExt;
    use tempfile::TempDir;
    use tower::ServiceExt; // `oneshot`

    fn seed_job(tmp: &TempDir, job_id: &str) -> (Config, JobMetadata) {
        let state_root = tmp.path().join("state");
        crate::core::job::ensure_state_store(&state_root).unwrap();
        let config = Config {
            workspace_root: tmp.path().join("ws"),
            state_root: state_root.clone(),
            ..Default::default()
        };
        let meta = JobMetadata {
            job_id: job_id.to_string(),
            project: "proj".to_string(),
            prompt: "p".to_string(),
            resolved_prompt: "p".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "ask".to_string(),
            add_dirs: Vec::new(),
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: tmp.path().join("wt"),
            branch: "b".to_string(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            exit_code: None,
            status: Some(JobStatus::Running),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            pid_start_time: None,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
            engine: None,
        };
        meta.insert_new(&state_root).unwrap();
        (config, meta)
    }

    /// Parses raw SSE bytes the way the client does: split on blank lines,
    /// keep non-blank lines per event.
    fn parse_sse(bytes: &[u8]) -> Vec<Vec<String>> {
        let s = String::from_utf8_lossy(bytes);
        let mut events: Vec<Vec<String>> = Vec::new();
        let mut current: Vec<String> = Vec::new();
        for line in s.lines() {
            if line.is_empty() {
                if !current.is_empty() {
                    events.push(std::mem::take(&mut current));
                }
            } else {
                current.push(line.to_string());
            }
        }
        if !current.is_empty() {
            events.push(current);
        }
        events
    }

    fn event_data(event: &[String]) -> String {
        event
            .iter()
            .filter(|l| l.starts_with("data:"))
            .map(|l| l["data:".len()..].trim_start_matches(' '))
            .collect::<Vec<_>>()
            .concat()
    }

    #[test]
    fn status_data_is_compact_json() {
        assert_eq!(
            format_status_data(&JobStatus::Running, 42),
            "{\"status\":\"running\",\"elapsed_seconds\":42}"
        );
        assert_eq!(
            format_status_data(&JobStatus::Done, 0),
            "{\"status\":\"done\",\"elapsed_seconds\":0}"
        );
    }

    #[test]
    fn log_data_carries_offset_and_text() {
        assert_eq!(
            format_log_data(17, "hello\nworld"),
            "{\"offset\":17,\"text\":\"hello\\nworld\"}"
        );
        assert_eq!(format_log_data(0, ""), "{\"offset\":0,\"text\":\"\"}");
    }

    #[test]
    fn end_data_is_the_terminal_status() {
        assert_eq!(
            format_end_data(&JobStatus::Killed),
            "{\"status\":\"killed\"}"
        );
        assert_eq!(
            format_end_data(&JobStatus::Failed),
            "{\"status\":\"failed\"}"
        );
    }

    #[test]
    fn wire_framing_is_sse_fields() {
        assert_eq!(
            format_event(&Event::Status(
                "{\"status\":\"running\",\"elapsed_seconds\":0}".into()
            )),
            "event: status\ndata: {\"status\":\"running\",\"elapsed_seconds\":0}\n\n"
        );
        assert_eq!(
            format_event(&Event::Log("{\"offset\":3,\"text\":\"x\"}".into())),
            "event: log\ndata: {\"offset\":3,\"text\":\"x\"}\n\n"
        );
        assert_eq!(
            format_event(&Event::End("{\"status\":\"done\"}".into())),
            "event: end\ndata: {\"status\":\"done\"}\n\n"
        );
        assert_eq!(format_event(&Event::Keepalive), ": keepalive\n\n");
    }

    /// A keepalive frame must be a self-contained SSE block: concatenated
    /// with the following frames it must split into exactly one block per
    /// frame, none of them glued to a neighbor.
    #[test]
    fn keepalive_frame_is_its_own_sse_block() {
        let wire = format_event(&Event::Keepalive)
            + &format_event(&Event::Status(
                "{\"status\":\"running\",\"elapsed_seconds\":0}".into(),
            ))
            + &format_event(&Event::Log("{\"offset\":3,\"text\":\"x\"}".into()));
        let blocks: Vec<&str> = wire.split("\n\n").filter(|b| !b.is_empty()).collect();
        assert_eq!(blocks.len(), 3, "frames must not glue: {wire:?}");
        assert!(blocks[1].starts_with("event: "));
        assert!(blocks[2].starts_with("event: "));
    }

    /// Re-parses a formatted `log` event the way the client does (SSE fields
    /// -> JSON) and checks the payload decodes back to the original fields.
    #[test]
    fn round_trip_log_event_preserves_offset_and_text() {
        let text = "line one\nline \"two\"\r\nend";
        let wire = format_event(&Event::Log(format_log_data(11, text)));
        let lines: Vec<&str> = wire.lines().collect();
        assert_eq!(lines[0], "event: log");
        assert!(lines.iter().any(|l| l.starts_with("data:")));
        let data = lines
            .iter()
            .filter(|l| l.starts_with("data:"))
            .map(|l| &l["data:".len()..])
            .map(|l| l.strip_prefix(' ').unwrap_or(l))
            .collect::<Vec<_>>()
            .concat();
        let value: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(value["offset"], 11);
        assert_eq!(value["text"], text);
    }

    /// Resume offset: a `log` event's `offset` is the position to pass back
    /// as `?offset=` so the next slice is exactly the following bytes.
    #[test]
    fn resume_offset_continues_past_appended_bytes() {
        let tmp = TempDir::new().unwrap();
        let (config, meta) = seed_job(&tmp, "ab12cd34");
        let job_id = &meta.job_id;
        let log_path = JobMetadata::log_path(&config.state_root, job_id);

        std::fs::write(&log_path, b"hello").unwrap();
        let (bytes, offset) = crate::server::read_log_bytes(&config, job_id, 0).unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(offset, 5);

        // Resume from the event's offset: only the appended bytes.
        std::fs::write(&log_path, b"hello world").unwrap();
        let (bytes, offset) = crate::server::read_log_bytes(&config, job_id, offset).unwrap();
        assert_eq!(bytes, b" world");
        assert_eq!(offset, 11);

        // An offset at/after EOF yields no text (nothing re-sent).
        let (bytes, offset) = crate::server::read_log_bytes(&config, job_id, 11).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(offset, 11);
        let (bytes, _) = crate::server::read_log_bytes(&config, job_id, 999).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn poll_once_is_quiet_when_nothing_changed() {
        let tmp = TempDir::new().unwrap();
        let (config, mut meta) = seed_job(&tmp, "cd34ef56");
        std::fs::write(JobMetadata::log_path(&config.state_root, "cd34ef56"), b"").unwrap();
        meta.status = Some(JobStatus::Running);
        meta.save(&config.state_root).unwrap();
        let meta = JobMetadata::load(&config.state_root, "cd34ef56").unwrap();

        let mut offset: u64 = 0;
        let mut last_status = JobStatus::Running;
        let tick = poll_once(&config, &meta, true, &mut offset, &mut last_status).unwrap();
        // Empty log (no growth), unchanged running status.
        assert!(tick.is_empty());
    }

    #[test]
    fn poll_once_drains_log_and_sends_end_when_terminal() {
        let tmp = TempDir::new().unwrap();
        let (config, mut meta) = seed_job(&tmp, "ef56ab78");
        std::fs::write(
            JobMetadata::log_path(&config.state_root, "ef56ab78"),
            b"working...",
        )
        .unwrap();
        meta.status = Some(JobStatus::Running);
        meta.save(&config.state_root).unwrap();
        let meta = JobMetadata::load(&config.state_root, "ef56ab78").unwrap();
        let mut offset: u64 = 0;
        let mut last_status = JobStatus::Running;

        // First tick: the pump already sent the on-connect status event, so
        // an unchanged status yields only the new log slice.
        let first = poll_once(&config, &meta, true, &mut offset, &mut last_status).unwrap();
        assert_eq!(first, vec![Event::Log(format_log_data(10, "working..."))]);
        assert_eq!(offset, 10);

        // The job now finishes: new slice, the status change, then `end`, in
        // that order.
        let finished = "working...\n{\"status\":\"SUCCESS\",\"response\":\"ok\"}";
        std::fs::write(
            JobMetadata::log_path(&config.state_root, "ef56ab78"),
            finished.as_bytes(),
        )
        .unwrap();
        let mut meta = meta;
        meta.exit_code = Some(0);
        meta.status = Some(JobStatus::Done);
        meta.save(&config.state_root).unwrap();
        let meta = JobMetadata::load(&config.state_root, "ef56ab78").unwrap();
        let tick = poll_once(&config, &meta, true, &mut offset, &mut last_status).unwrap();
        assert_eq!(
            tick[0],
            Event::Log(format_log_data(finished.len() as u64, &finished[10..]))
        );
        assert_eq!(
            tick[1],
            Event::Status(format_status_data(
                &JobStatus::Done,
                job_elapsed_secs(&meta)
            ))
        );
        assert_eq!(tick[2], Event::End(format_end_data(&JobStatus::Done)));
        assert_eq!(last_status, JobStatus::Done);
        assert!(JobStatus::Done.is_terminal());
    }

    #[test]
    fn poll_once_skips_status_when_not_checked() {
        let tmp = TempDir::new().unwrap();
        let (config, meta) = seed_job(&tmp, "56ef78ab");
        std::fs::write(JobMetadata::log_path(&config.state_root, "56ef78ab"), b"").unwrap();
        // The record IS terminal on disk, but a round that is only polling
        // the log must not report it (status checks happen on their own
        // cadence).
        let mut meta = meta;
        meta.exit_code = Some(0);
        meta.status = Some(JobStatus::Done);
        meta.save(&config.state_root).unwrap();

        let mut offset: u64 = 0;
        let mut last_status = JobStatus::Running;
        let tick = poll_once(&config, &meta, false, &mut offset, &mut last_status).unwrap();
        assert!(tick.is_empty());
        assert_eq!(last_status, JobStatus::Running);

        // The status event reports the JOB's elapsed time (its stored
        // `started_at`, falling back to `created_at` -- the same anchor the
        // status command's Elapsed line uses), never this stream's age: a
        // job that started 100 s ago must report at least 100 s even though
        // the stream is brand new.
        meta.started_at = Some(Utc::now() - chrono::Duration::seconds(100));
        meta.save(&config.state_root).unwrap();
        let meta = JobMetadata::load(&config.state_root, "56ef78ab").unwrap();
        // The exit code was recorded: compute_status now classifies from
        // the (empty) log, so the status changed from `Running` and this
        // round emits the status event.
        let mut last_status = JobStatus::Running;
        let tick = poll_once(&config, &meta, true, &mut offset, &mut last_status).unwrap();
        let status = tick
            .iter()
            .find(|e| matches!(e, Event::Status(_)))
            .expect("a status change must emit a status event");
        let Event::Status(data) = status else {
            panic!("expected a `status` event, got: {status:?}");
        };
        let value: serde_json::Value = serde_json::from_str(data).unwrap();
        assert!(
            value["elapsed_seconds"].as_u64().unwrap() >= 100,
            "elapsed_seconds must come from the job's started_at: {data}"
        );
    }

    #[tokio::test]
    async fn events_unknown_job_is_typed_404_not_a_stream() {
        let tmp = TempDir::new().unwrap();
        let config = Config {
            workspace_root: tmp.path().join("ws"),
            state_root: tmp.path().join("state"),
            ..Default::default()
        };
        let app = crate::server::router(Arc::new(config), Arc::new(AuthManager::new()));
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/jobs/nope1234/events")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_ne!(
            res.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(EVENTS_CONTENT_TYPE),
            "an unknown job must NOT open a stream"
        );
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value =
            serde_json::from_str(&String::from_utf8_lossy(&bytes)).unwrap();
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains("No such job found"));
        assert!(value.get("code").is_some());
    }

    #[tokio::test]
    async fn events_stream_emits_status_log_end_for_a_finished_job() {
        let tmp = TempDir::new().unwrap();
        let (config, mut meta) = seed_job(&tmp, "12ab34cd");
        let success_line = "{\"status\":\"SUCCESS\",\"response\":\"R\"}";
        let full = format!("hello log\n{success_line}");
        std::fs::write(
            JobMetadata::log_path(&config.state_root, &meta.job_id),
            full.as_bytes(),
        )
        .unwrap();
        meta.exit_code = Some(0);
        meta.status = Some(JobStatus::Done);
        meta.save(&config.state_root).unwrap();

        let app = crate::server::router(Arc::new(config), Arc::new(AuthManager::new()));
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/v1/jobs/{}/events", meta.job_id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
            EVENTS_CONTENT_TYPE
        );

        // Read the body to the end (the stream closes after `end`).
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let events = parse_sse(&bytes);

        // The first event is the on-connect status event.
        assert_eq!(events[0][0], "event: status");
        let first: serde_json::Value = serde_json::from_str(&event_data(&events[0])).unwrap();
        assert_eq!(first["status"], "done");
        assert!(first["elapsed_seconds"].is_number());

        // Then: the full log as `log` event(s), and finally `end`.
        let mut log_text = String::new();
        let mut ended = String::new();
        for event in &events[1..] {
            if event[0].starts_with(':') {
                continue; // keepalive comments
            }
            let name = event[0].strip_prefix("event: ").unwrap();
            match name {
                "log" => {
                    let v: serde_json::Value = serde_json::from_str(&event_data(event)).unwrap();
                    log_text.push_str(v["text"].as_str().unwrap());
                }
                "end" => ended = event_data(event),
                other => panic!("unexpected event {other:?}"),
            }
        }
        assert_eq!(log_text, full);
        assert_eq!(ended, "{\"status\":\"done\"}");

        // The last named event is `end` (the stream closes after it).
        let last_named = events
            .iter()
            .rev()
            .find(|e| e[0].starts_with("event: "))
            .unwrap();
        assert_eq!(last_named[0], "event: end");
    }

    /// Resume through the real endpoint: connecting with `?offset=` set to
    /// the log's current size receives NO `log` events (nothing left to
    /// send) but still gets `end` for the finished job.
    #[tokio::test]
    async fn events_offset_query_resumes_mid_log() {
        let tmp = TempDir::new().unwrap();
        let (config, mut meta) = seed_job(&tmp, "34cd56ef");
        std::fs::write(
            JobMetadata::log_path(&config.state_root, &meta.job_id),
            b"first half ",
        )
        .unwrap();
        meta.exit_code = Some(0);
        meta.status = Some(JobStatus::Done);
        meta.save(&config.state_root).unwrap();
        let resume = 11; // == the length written above

        let app = crate::server::router(Arc::new(config), Arc::new(AuthManager::new()));
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/v1/jobs/{}/events?offset={resume}",
                        meta.job_id
                    ))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get(crate::server::LOG_OFFSET_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            resume.to_string()
        );
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let events = parse_sse(&bytes);
        // Nothing left to send: no `log` events at all.
        let log_events = events
            .iter()
            .filter(|e| e.first().map(|l| l.as_str()) == Some("event: log"));
        assert_eq!(log_events.count(), 0);
        // ...but the finished job still ends the stream.
        let last_named = events
            .iter()
            .rev()
            .find(|e| e[0].starts_with("event: "))
            .unwrap();
        assert_eq!(last_named[0], "event: end");
    }

    /// Reads the next frame of the body (the pump sends exactly one channel
    /// frame per SSE frame, so one body chunk is one wire frame).
    async fn next_body_chunk(body: &mut Body) -> Option<String> {
        let frame = body.frame().await?.expect("no stream error");
        frame
            .into_data()
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    }

    /// The on-connect status frame reports the JOB's elapsed time (its
    /// stored `started_at`, falling back to `created_at`), not the age of
    /// this brand-new stream: a job that started 100 s ago must report at
    /// least 100 s the moment a client connects.
    #[tokio::test]
    async fn on_connect_status_elapsed_is_the_job_age() {
        let tmp = TempDir::new().unwrap();
        let (config, mut meta) = seed_job(&tmp, "ef79abcd");
        meta.started_at = Some(Utc::now() - chrono::Duration::seconds(100));
        meta.save(&config.state_root).unwrap();

        let app = crate::server::router(Arc::new(config), Arc::new(AuthManager::new()));
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/v1/jobs/{}/events", meta.job_id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let mut body = res.into_body();
        // The first chunk is the on-connect status frame (a running job's
        // stream never ends on its own, so it is read one frame at a time).
        let s = next_body_chunk(&mut body)
            .await
            .expect("the on-connect status frame");
        assert!(
            s.starts_with("event: status"),
            "the on-connect frame must be the status frame: {s:?}"
        );
        let data = s.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
        let value: serde_json::Value = serde_json::from_str(data).unwrap();
        assert!(
            value["elapsed_seconds"].as_u64().unwrap() >= 100,
            "on-connect elapsed must come from the job's started_at: {s:?}"
        );
        // Drop the body: the running job's stream ends with its client.
        drop(body);
    }

    /// A client-tunable keepalive cadence is a public CPU amplifier: the
    /// cadence on the wire is the fixed 15s, and the old
    /// `?debug_fast_keepalive=true` knob must no longer shorten it (a job
    /// that is idle would otherwise tick a pump thread every 100ms at any
    /// client's request).
    #[tokio::test]
    async fn keepalive_cadence_is_not_publicly_tunable() {
        let tmp = TempDir::new().unwrap();
        let (config, meta) = seed_job(&tmp, "79abcd00");
        std::fs::write(JobMetadata::log_path(&config.state_root, &meta.job_id), b"").unwrap();

        let app = crate::server::router(Arc::new(config), Arc::new(AuthManager::new()));
        // The hostile knob: parsed by an older server, unknown to this one.
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/v1/jobs/{}/events?debug_fast_keepalive=true",
                        meta.job_id
                    ))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let mut body = res.into_body();
        let s = next_body_chunk(&mut body)
            .await
            .expect("the on-connect status frame");
        assert!(s.starts_with("event: status"), "on-connect frame: {s:?}");
        // The job is idle: nothing but the keepalive can ever arrive on
        // this stream next. With the old per-client 100ms knob a keepalive
        // frame would be here within 500ms; with the fixed 15s wire
        // cadence, no frame may arrive at all.
        let early =
            tokio::time::timeout(Duration::from_millis(500), next_body_chunk(&mut body)).await;
        match early {
            Ok(Some(chunk)) => {
                panic!("a public query knob must not shorten the keepalive cadence: {chunk:?}")
            }
            Ok(None) => panic!("the stream ended before the keepalive"),
            Err(_) => {} // nothing within 500ms: the fixed cadence holds
        }
        // Drop the body: a running job's stream never ends on its own.
        drop(body);
    }

    /// A multibyte character straddling the 64 KiB slice boundary must arrive
    /// intact over two `log` events, with NO `U+FFFD` and NO byte
    /// re-sent/dropped: the first event holds the complete prefix (offset just
    /// past it) and the second event starts exactly at the remaining tail.
    #[test]
    fn log_slice_caps_at_boundary_and_survives_multibyte_straddle() {
        let tmp = TempDir::new().unwrap();
        let (config, meta) = seed_job(&tmp, "aa11bb22");
        let job_id = &meta.job_id;
        let log_path = JobMetadata::log_path(&config.state_root, job_id);

        // 'é' (2 bytes) straddles byte 65536: first byte inside the cap,
        // second byte the first byte of the tail.
        let mut content = vec![b'x'; (LOG_SLICE_CAP - 1) as usize];
        content.extend_from_slice(b"\xC3\xA9");
        content.push(b'?');
        std::fs::write(&log_path, &content).unwrap();

        let (mut offset, mut last_status): (u64, JobStatus) = (0, JobStatus::Running);
        let mut parts: Vec<String> = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        loop {
            let tick = poll_once(&config, &meta, false, &mut offset, &mut last_status).unwrap();
            if tick.is_empty() {
                break; // all bytes emitted
            }
            match &tick[0] {
                Event::Log(data) => {
                    let v: serde_json::Value = serde_json::from_str(data).unwrap();
                    let text = v["text"].as_str().unwrap();
                    assert!(
                        text.chars().all(|c| c != '\u{FFFD}'),
                        "chunking must never insert U+FFFD"
                    );
                    parts.push(text.to_string());
                    offsets.push(v["offset"].as_u64().unwrap());
                }
                other => panic!("unexpected event in log-only round: {other:?}"),
            }
        }
        // Two events: the 64 KiB cap cut at byte 65535 (just before the
        // 2-byte 'é'), and the second event carries 'é' intact plus the tail.
        assert_eq!(parts.len(), 2, "boundary char belongs to two events");
        assert_eq!(parts[0], "x".repeat(65535));
        assert_eq!(parts[1], "\u{e9}?");
        assert_eq!(offsets, vec![65535, content.len() as u64]);

        // The drain path (terminal round) obeys the same cap: the first slice
        // stops BEFORE the straddling char, and the drain slice is capped too.
        let (config, mut meta) = seed_job(&tmp, "cc33dd44");
        let job_id = &meta.job_id;
        let mut content = vec![b'x'; (LOG_SLICE_CAP - 1) as usize];
        content.extend_from_slice(b"\xC3\xA9");
        content.extend_from_slice(&vec![b'y'; (LOG_SLICE_CAP + 3) as usize]);
        std::fs::write(JobMetadata::log_path(&config.state_root, job_id), &content).unwrap();
        meta.exit_code = Some(0);
        meta.status = Some(JobStatus::Done);
        meta.save(&config.state_root).unwrap();
        let meta = JobMetadata::load(&config.state_root, job_id).unwrap();
        let total = content.len() as u64;
        let (mut offset, mut last_status) = (0, JobStatus::Running);
        let round = poll_once(&config, &meta, true, &mut offset, &mut last_status).unwrap();
        let logs: Vec<(u64, String)> = round
            .iter()
            .filter_map(|e| match e {
                Event::Log(d) => {
                    let v: serde_json::Value = serde_json::from_str(d).unwrap();
                    Some((
                        v["offset"].as_u64().unwrap(),
                        v["text"].as_str().unwrap().to_string(),
                    ))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            logs.len(),
            2,
            "drain round: capped slice + capped drain + end"
        );
        assert_eq!(logs[0].0, LOG_SLICE_CAP - 1);
        assert_eq!(logs[0].1, "x".repeat(65535));
        assert_eq!(logs[1].0, LOG_SLICE_CAP - 1 + LOG_SLICE_CAP);
        assert_eq!(logs[1].1, format!("\u{e9}{}", "y".repeat(65534)));
        assert_eq!(offset, LOG_SLICE_CAP - 1 + LOG_SLICE_CAP);
        assert!(round.iter().any(|e| matches!(e, Event::End(_))));
        // The follow-up round delivers the final 5 bytes losslessly.
        let mut last2 = JobStatus::Done;
        let again = poll_once(&config, &meta, true, &mut offset, &mut last2).unwrap();
        let again_logs: Vec<(u64, String)> = again
            .iter()
            .filter_map(|e| match e {
                Event::Log(d) => {
                    let v: serde_json::Value = serde_json::from_str(d).unwrap();
                    Some((
                        v["offset"].as_u64().unwrap(),
                        v["text"].as_str().unwrap().to_string(),
                    ))
                }
                _ => None,
            })
            .collect();
        assert_eq!(again_logs, vec![(total, "yyyyy".to_string())]);
        assert_eq!(offset, total);
    }

    /// The stream cap, tested on the guard type directly: acquisitions can
    /// never exceed `MAX_EVENT_STREAMS`, once the pool is exhausted
    /// `try_acquire` is rejected (-> the router answers 503), and dropping a
    /// guard frees its slot immediately. (Other in-flight streams' pumps also
    /// hold slots, so the loop stops early instead of panicking when some of
    /// the cap is already taken.)
    #[test]
    fn stream_slot_caps_concurrent_streams() {
        let mut slots = Vec::new();
        for _ in 0..=MAX_EVENT_STREAMS {
            match StreamSlot::try_acquire() {
                Some(s) => slots.push(s),
                None => break, // cap reached (fully or by background streams)
            }
        }
        assert!(
            slots.len() <= MAX_EVENT_STREAMS,
            "more than the cap were acquired"
        );
        assert!(!slots.is_empty(), "must have acquired at least one slot");
        assert!(
            StreamSlot::held() >= slots.len(),
            "our slots are part of the held count"
        );
        let next = StreamSlot::try_acquire();
        if let Some(extra) = next {
            drop(extra);
        } else {
            // The pool is exhausted right now: `cap` holders exist (ours plus
            // any background streams), so dropping ONE held guard must bring
            // the held count below the cap and make an acquire succeed.
            assert!(
                StreamSlot::held() <= MAX_EVENT_STREAMS,
                "never over the cap"
            );
            let before = StreamSlot::held();
            drop(slots.pop().unwrap());
            assert!(StreamSlot::held() < before, "drop freed a slot");
            let refilled = StreamSlot::try_acquire();
            assert!(refilled.is_some(), "a freed slot must be reusable");
            slots.push(refilled.unwrap());
        }
        drop(slots); // RAII: dropping every guard releases every slot
        match StreamSlot::try_acquire() {
            Some(s) => drop(s), // free slots are reusable
            None => panic!("free slots must be reusable"),
        }
    }
}

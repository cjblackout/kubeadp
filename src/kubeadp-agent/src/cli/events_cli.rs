//! `agent-run watch`: reads the server's SSE job-events stream
//! (`GET /api/v1/jobs/{id}/events`) line by line with the existing blocking
//! ureq client, prints log text to stdout as it arrives (like `tail`) and a
//! one-line status banner to stderr on every change.
//!
//! Exit codes, like `wait`:
//! - `0`: the job ended `done` (an `end` event with `{"status":"done"}`)
//! - `2`: the job ended in another terminal state (or its record vanished,
//!   which the server ends as `missing`)
//! - `3`: `--timeout` elapsed while the job was still running
//! - `1`: the connection dropped before the `end` event
//!
//! The response body is read on a background thread (ureq's reader is
//! `Send + Sync`) while the main thread owns the `--timeout` deadline: it
//! exits `3` the moment the deadline passes, no matter where the blocking
//! read is. `: keepalive` comments are ignored (they only matter to idle
//! proxies); a partial event is reassembled across `read_line` calls.

use std::io::{BufRead, Lines, Read};
use std::time::{Duration, Instant};

use crate::core::job::JobStatus;
use serde_json::Value;

type SseLines = Lines<std::io::BufReader<Box<dyn Read + Send + Sync + 'static>>>;

/// One named event decoded from the SSE wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseEvent {
    /// `{"status":"...","elapsed_seconds":n}`.
    Status { status: JobStatus, elapsed: u64 },
    /// `{"offset":n,"text":"..."}` -- raw log text to write to stdout.
    Log { offset: u64, text: String },
    /// `{"status":"..."}` -- the stream is complete.
    End { status: JobStatus },
}

/// Decodes the payload of a named event. `None` (comment lines only, an
/// empty block, an unknown event type, or malformed JSON) means "nothing to
/// deliver".
fn decode_event(name: Option<&str>, data: Option<&str>) -> Option<SseEvent> {
    let (Some(name), Some(data)) = (name, data) else {
        return None;
    };
    let value: Value = match serde_json::from_str(data) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("watch: dropping malformed '{name}' event: {err}");
            return None;
        }
    };
    match name {
        "status" => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .and_then(JobStatus::parse);
            let elapsed = value
                .get("elapsed_seconds")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            status.map(|status| SseEvent::Status { status, elapsed })
        }
        "log" => {
            let offset = value.get("offset").and_then(Value::as_u64);
            let text = value.get("text").and_then(Value::as_str);
            match (offset, text) {
                (Some(offset), Some(text)) => Some(SseEvent::Log {
                    offset,
                    text: text.to_string(),
                }),
                _ => None,
            }
        }
        "end" => value
            .get("status")
            .and_then(Value::as_str)
            .and_then(JobStatus::parse)
            .map(|status| SseEvent::End { status }),
        // A future server event type: skip it rather than fail the stream.
        other => {
            eprintln!("watch: ignoring unknown SSE event type '{other}'");
            None
        }
    }
}

/// Parses ONE complete event from the buffer: the consecutive lines ending
/// at the first empty line (the SSE event terminator).
///
/// Returns `Ok((event, rest))` where `rest` holds everything after the
/// terminator (possibly holding further whole events), and `Err(())` when
/// the buffer does not contain a terminator yet -- keep feeding lines.
///
/// This is the unit under test: [`read_events`] only feeds it the wire.
#[allow(clippy::result_unit_err)]
pub fn parse_event(buf: &str) -> Result<(Option<SseEvent>, String), ()> {
    let mut pos = 0usize;
    let mut terminated = false;
    let mut event_name: Option<&str> = None;
    let mut data: Option<&str> = None;
    for line in buf.lines() {
        // `lines()` yields the line without its terminator, stripping a
        // trailing '\r' when the ending is CRLF: count the terminator bytes
        // ('\r' if present, plus the '\n') back into `pos` so `rest` begins
        // exactly where this line's terminator ends.
        let end = pos
            + line.len()
            + usize::from(buf.as_bytes().get(pos + line.len()) == Some(&b'\r'))
            + 1;
        if line.is_empty() {
            // A blank line terminates the event; everything after it is `rest`.
            pos = end;
            terminated = true;
            break;
        }
        if line.starts_with(':') {
            // A comment (`: keepalive`): carries no payload.
            pos = end;
            continue;
        }
        match line.split_once(':') {
            Some(("event", value)) => event_name = Some(value.trim()),
            Some(("data", value)) => data = Some(value.trim_start_matches(' ')),
            // A line outside the `field: value` shape (e.g. a non-SSE
            // error body): stop parsing rather than guess.
            _ => return Err(()),
        }
        pos = end; // consume the line and its terminator
    }
    if !terminated {
        return Err(());
    }
    Ok((decode_event(event_name, data), buf[pos..].to_string()))
}

/// The banner + exit-code decision for an `end` event (a pure function of the
/// state the client already holds: the last status it printed and the terminal
/// status the `end` event carries).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndDecision {
    /// The exit code the process exits with for this terminal status. Set by
    /// [`decide_end`] and asserted by the tests; the production path derives
    /// the code from the terminal status via [`finish_watch_exit`].
    #[allow(dead_code)]
    pub exit: i32,
    /// `Some(status)` when a `[Job status: <status>]` banner must still be
    /// printed for the terminal status, `None` when it was already printed by
    /// a `status` event (the `end` event must not repeat it).
    pub banner: Option<JobStatus>,
}

/// The `wait`-style exit code for a terminal status: `0` for `done`, `2` for
/// any other terminal state (the timeout exit `3` is set by the caller before
/// the `end` event is considered, and a dropped stream exits `1`).
pub fn finish_watch_exit(status: JobStatus) -> i32 {
    if status == JobStatus::Done {
        0
    } else {
        2
    }
}

/// Decides the terminal banner and exit code for an `end` event.
///
/// `last_printed` is the status of the most recent `[Job status: ...]` banner
/// (the on-connect status event or a later `status` event), or `None` if the
/// `end` arrives before any status banner was printed. The terminal status
/// from `end` is always bannered unless it already was, and the exit code is
/// `0` for `done`, `2` otherwise.
pub fn decide_end(last_printed: Option<JobStatus>, end_status: JobStatus) -> EndDecision {
    EndDecision {
        exit: finish_watch_exit(end_status),
        banner: if last_printed == Some(end_status) {
            None
        } else {
            Some(end_status)
        },
    }
}

/// Result of streaming one `watch` session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchOutcome {
    /// The `end` event arrived with this terminal status.
    Terminal(JobStatus),
    /// The connection ended (EOF, read error, or dropped socket) before
    /// `end` reached the client.
    Dropped,
}

/// Reads the SSE body of `response` on a background thread and resolves
/// `outcome` with the final [`WatchOutcome`] when the stream ends. The
/// deadline does not live here: the CALLER checks it on its own thread and
/// stops waiting when it passes (this thread is a detached best-effort that
/// dies with the process).
fn spawn_reader(response: ureq::Response) -> std::sync::mpsc::Receiver<WatchOutcome> {
    let (otx, orx) = std::sync::mpsc::channel::<WatchOutcome>();
    let mut lines: SseLines = std::io::BufReader::new(response.into_reader()).lines();
    std::thread::spawn(move || {
        let _ = otx.send(read_events(&mut lines));
    });
    orx
}

/// Drives the read loop: accumulate wire lines, hand each complete event to
/// the output sinks (log -> stdout, status -> stderr banner), and stop at
/// the `end` event, EOF, or a read error.
fn read_events(lines: &mut SseLines) -> WatchOutcome {
    let mut buf = String::new();
    // The last status whose `[Job status: ...]` banner was printed to stderr.
    // The `end` event carries the terminal status independently of any `status`
    // event, so it is bannered here too -- but only when it differs from the
    // last printed one (the terminal transition is usually already announced by
    // a `status` event, in which case `end` must not repeat the banner).
    let mut last_printed: Option<JobStatus> = None;
    loop {
        let line = match lines.next() {
            Some(Ok(line)) => line,
            None => return WatchOutcome::Dropped,
            Some(Err(err)) => {
                eprintln!("watch: the connection to the events stream dropped: {err}");
                return WatchOutcome::Dropped;
            }
        };
        buf.push_str(&line);
        buf.push('\n');
        match parse_event(&buf) {
            Err(()) => {
                // Incomplete event (or a non-SSE body): keep reading; the
                // body closing turns this into `Dropped`.
            }
            Ok((event, rest)) => {
                buf = rest;
                if let Some(event) = event {
                    match &event {
                        SseEvent::Log { text, .. } => {
                            crate::cli::client::write_stdout(text.as_bytes())
                        }
                        SseEvent::Status { status, .. } => {
                            eprintln!("[Job status: {status}]");
                            last_printed = Some(*status);
                        }
                        SseEvent::End { status } => {
                            let decision = decide_end(last_printed, *status);
                            if let Some(banner) = decision.banner {
                                eprintln!("[Job status: {banner}]");
                            }
                            return WatchOutcome::Terminal(*status);
                        }
                    }
                }
            }
        }
    }
}

/// `agent-run watch <job>`: connects with the existing blocking HTTP client
/// and streams the job's events. Never returns: exits with the `wait` exit
/// codes (0 done / 2 other terminal / 3 timeout) or 1 when the connection
/// drops before `end`.
pub fn watch(agent: &ureq::Agent, base: &str, job_id: &str, deadline: Option<Instant>) -> ! {
    let url = format!(
        "{base}{}/jobs/{}/events",
        crate::cli::client::API_PREFIX,
        crate::cli::client::urlencode(job_id)
    );
    let response = match agent.get(&url).call() {
        Ok(response) => response,
        Err(err) => crate::cli::client::http_error(&url, err, false),
    };
    let content_type = response
        .header("content-type")
        .map(ToString::to_string)
        .unwrap_or_default();
    if !content_type.starts_with("text/event-stream") {
        eprintln!(
            "watch: the server did not open an events stream for the job \
             (content-type: '{content_type}'); the job may be unknown"
        );
        std::process::exit(1);
    }
    let orx = spawn_reader(response);

    if let Some(deadline) = deadline {
        watch_with_deadline(orx, deadline)
    } else {
        watch_forever(orx)
    }
}

/// Polls the outcome channel until either result lands: the final outcome or
/// the deadline. The short poll sleeps keep the exit within ~50ms of the
/// deadline instead of waiting for the next stream event.
fn watch_with_deadline(orx: std::sync::mpsc::Receiver<WatchOutcome>, deadline: Instant) -> ! {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            eprintln!("Timed out waiting for the job to finish: job still running (exit code 3).");
            std::process::exit(3);
        }
        match orx.try_recv() {
            Ok(outcome) => finish_outcome(outcome),
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                std::thread::sleep(left.min(Duration::from_millis(50)));
            }
            // The reader thread finished without resolving an outcome:
            // treat it like a dropped connection.
            Err(std::sync::mpsc::TryRecvError::Disconnected) => finish_dropped(),
        }
    }
}

fn watch_forever(orx: std::sync::mpsc::Receiver<WatchOutcome>) -> ! {
    match orx.recv() {
        Ok(outcome) => finish_outcome(outcome),
        _ => finish_dropped(),
    }
}

fn finish_outcome(outcome: WatchOutcome) -> ! {
    match outcome {
        WatchOutcome::Terminal(status) => finish(&status),
        WatchOutcome::Dropped => finish_dropped(),
    }
}

fn finish(status: &JobStatus) -> ! {
    std::process::exit(finish_watch_exit(*status));
}

fn finish_dropped() -> ! {
    eprintln!(
        "watch: the connection to the job-event stream was closed before the \
         job reached a terminal state -- the job status is unknown"
    );
    std::process::exit(1);
}

// ---------------------------------------------------------------------------
// Unit tests for the SSE parser (the wire the server's `format_event` emits).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    const STATUS_RUN: &str =
        "event: status\ndata: {\"status\":\"running\",\"elapsed_seconds\":4}\n\n";
    const LOG_TEXT: &str = "line one\nline \"two\"\r\nend";
    const LOG_WIRE: &str =
        "event: log\ndata: {\"offset\":11,\"text\":\"line one\\nline \\\"two\\\"\\r\\nend\"}\n\n";

    #[test]
    fn parse_status_event() {
        assert_eq!(
            parse_event(STATUS_RUN),
            Ok((
                Some(SseEvent::Status {
                    status: JobStatus::Running,
                    elapsed: 4
                }),
                String::new()
            ))
        );
    }

    #[test]
    fn parse_log_event_with_multiline_and_quoted_text() {
        assert_eq!(
            parse_event(LOG_WIRE),
            Ok((
                Some(SseEvent::Log {
                    offset: 11,
                    text: LOG_TEXT.into()
                }),
                String::new()
            ))
        );
    }

    #[test]
    fn parse_end_event() {
        assert_eq!(
            parse_event("event: end\ndata: {\"status\":\"failed\"}\n\n"),
            Ok((
                Some(SseEvent::End {
                    status: JobStatus::Failed
                }),
                String::new()
            ))
        );
    }

    #[test]
    fn keepalive_comment_before_an_event_is_consumed_with_it() {
        let buf = format!(": keepalive\n{STATUS_RUN}");
        assert_eq!(
            parse_event(&buf),
            Ok((
                Some(SseEvent::Status {
                    status: JobStatus::Running,
                    elapsed: 4
                }),
                String::new()
            ))
        );
    }

    #[test]
    fn incomplete_buffer_is_not_terminated() {
        assert_eq!(parse_event("event: status\ndata: {\"status\":"), Err(()));
        assert_eq!(
            parse_event("event: status\ndata: {\"status\":\"running\"}"),
            Err(())
        );
        assert_eq!(parse_event("e"), Err(()));
    }

    #[test]
    fn two_events_in_one_buffer_yield_first_plus_rest() {
        let buf = format!("{STATUS_RUN}event: end\ndata: {{\"status\":\"killed\"}}\n\n");
        let (first, rest) = parse_event(&buf).unwrap();
        assert_eq!(
            first,
            Some(SseEvent::Status {
                status: JobStatus::Running,
                elapsed: 4
            })
        );
        assert_eq!(
            parse_event(&rest),
            Ok((
                Some(SseEvent::End {
                    status: JobStatus::Killed
                }),
                String::new()
            ))
        );
    }

    #[test]
    fn empty_block_is_a_noop() {
        assert_eq!(parse_event("\n"), Ok((None, String::new())));
    }

    #[test]
    fn unknown_event_type_is_skipped() {
        let buf = "event: ping\ndata: {\"x\":1}\n\n";
        assert_eq!(parse_event(buf), Ok((None, String::new())));
    }

    #[test]
    fn malformed_line_is_an_error() {
        assert_eq!(parse_event("not an sse line\n\n"), Err(()));
    }

    #[test]
    fn end_banners_and_exits_for_each_terminal_status() {
        // `end` already announced by a `status` event: no repeated banner.
        assert_eq!(
            decide_end(Some(JobStatus::Killed), JobStatus::Killed),
            EndDecision {
                exit: 2,
                banner: None
            }
        );
        // `end` arrives as the first terminal info (no prior status banner):
        // the killed banner must still print, and the exit is 2.
        assert_eq!(
            decide_end(None, JobStatus::Killed),
            EndDecision {
                exit: 2,
                banner: Some(JobStatus::Killed)
            }
        );
        // `done` always exits 0; a status event already printed `done` means
        // no banner from `end`.
        assert_eq!(
            decide_end(Some(JobStatus::Done), JobStatus::Done),
            EndDecision {
                exit: 0,
                banner: None
            }
        );
        // A terminal status the stream never announced before: banner + exit 2.
        assert_eq!(
            decide_end(Some(JobStatus::Running), JobStatus::Failed),
            EndDecision {
                exit: 2,
                banner: Some(JobStatus::Failed)
            }
        );
    }
}

use std::io::Write;
use std::sync::Mutex;
use std::time::SystemTime;

static LOG_MUTEX: Mutex<()> = Mutex::new(());

/// Returns current UTC timestamp in ISO 8601 / RFC 3339 format (e.g. "2026-10-01T07:12:34Z").
pub fn rfc3339_now() -> String {
    let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    };
    let mut tm = libc::tm {
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: 0,
        tm_gmtoff: 0,
        tm_zone: std::ptr::null(),
    };
    unsafe {
        let t = now as libc::time_t;
        libc::gmtime_r(&t, &mut tm);
    }
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    )
}

/// Logs a message to stderr with timestamp and session name.
/// Example output: `2026-10-01T07:12:34Z [default] Starting Claude Code...`
pub fn log_event(session_name: &str, message: &str) {
    let timestamp = rfc3339_now();
    let _lock = LOG_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let mut stderr = std::io::stderr();
    let _ = writeln!(stderr, "{} [{}] {}", timestamp, session_name, message);
    let _ = stderr.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc3339_format() {
        let ts = rfc3339_now();
        assert_eq!(ts.len(), 20);
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[10..11], "T");
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
    }

    #[test]
    fn test_log_event_does_not_panic() {
        log_event("test-session", "test message");
    }
}

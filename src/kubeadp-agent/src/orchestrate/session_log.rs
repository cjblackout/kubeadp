//! Per-session output ring buffer and secret redaction for the sessions API.
//!
//! Each session's forwarded stdout/stderr bytes are appended into a bounded
//! per-session ring buffer (size from `AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES`,
//! default 500_000 bytes). The API serves a redacted tail of that buffer, so
//! known credential shapes in the output are masked before they reach a
//! client.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

const DEFAULT_RING_CAP_BYTES: usize = 500_000;

fn ring_cap_from_env() -> usize {
    let Some(raw) = std::env::var("AGENT_ORCHESTRATE_SESSION_LOG_RING_BYTES")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    else {
        return DEFAULT_RING_CAP_BYTES;
    };
    raw.parse::<usize>().unwrap_or(DEFAULT_RING_CAP_BYTES)
}

/// Bounded per-session byte ring buffers shared with the stdout writer thread.
pub struct SessionLogs {
    inner: Mutex<HashMap<String, VecDeque<u8>>>,
    cap: usize,
}

impl SessionLogs {
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            cap: cap_bytes.max(1),
        }
    }

    pub fn from_env() -> Self {
        Self::new(ring_cap_from_env())
    }

    /// Appends `data` to the session ring, trimming the front excess so the
    /// ring never exceeds its byte cap.
    pub fn append(&self, session_name: &str, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let deque = map
            .entry(session_name.to_string())
            .or_insert_with(|| VecDeque::with_capacity(self.cap.min(4096)));
        deque.extend(data.iter().copied());
        if deque.len() > self.cap {
            let excess = deque.len() - self.cap;
            deque.drain(..excess);
        }
    }

    /// Whether any bytes have been collected for the session.
    pub fn has(&self, session_name: &str) -> bool {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.get(session_name)
            .map(|d| !d.is_empty())
            .unwrap_or(false)
    }

    /// Returns the last `tail_bytes` bytes for the session (whole ring if the
    /// ring is smaller), or `None` when the session is unknown/empty.
    pub fn tail(&self, session_name: &str, tail_bytes: usize) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let deque = map.get(session_name)?;
        if deque.is_empty() {
            return None;
        }
        let len = deque.len();
        let start = len.saturating_sub(tail_bytes.max(1).min(len));
        Some(deque.range(start..).copied().collect())
    }

    /// `tail` then `redact_secrets`.
    pub fn tail_redacted(&self, session_name: &str, tail_bytes: usize) -> Option<Vec<u8>> {
        self.tail(session_name, tail_bytes)
            .map(|b| redact_secrets(&b))
    }

    /// Returns the last `n` newline-terminated lines for the session (fewer if
    /// the ring holds less). The returned slice starts at a line boundary so
    /// the API can serve whole lines without cutting mid-line; an unterminated
    /// trailing segment counts as one line.
    pub fn tail_lines(&self, session_name: &str, n: usize) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let deque = map.get(session_name)?;
        if deque.is_empty() {
            return None;
        }
        let len = deque.len();
        let n = n.max(1);
        // Walk backwards counting newlines to find the start of the last `n`
        // lines. An unterminated trailing segment counts as an extra line, so
        // start with `newlines = 1` when the buffer does not end with `\n`.
        let mut newlines = if deque[len - 1] == b'\n' { 0 } else { 1 };
        let mut start = 0;
        for idx in (0..len).rev() {
            if deque[idx] == b'\n' {
                newlines += 1;
                if newlines > n {
                    start = idx + 1;
                    break;
                }
            }
        }
        if newlines < n && len > 0 {
            // The buffer holds fewer than `n` lines: nudge `start` past the
            // leading newline (if any) so the first line is kept whole.
            while start < len && deque[start] == b'\n' {
                start += 1;
            }
        }
        Some(deque.range(start..).copied().collect())
    }
}

/// Known secret prefixes. Only the bytes following a matched prefix are
/// masked, so the shape (e.g. `ghp_`) stays visible while the token value is
/// replaced with `*`.
const KNOWN_SECRET_PREFIXES: &[&[u8]] = &[
    b"ghp_",
    b"gho_",
    b"ghu_",
    b"ghs_",
    b"ghr_",
    b"github_pat_",
    b"glpat-",
    b"Bearer ",
    b"AGE-SECRET-KEY-",
];

fn is_token_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'+' | b'/' | b'=' | b'.')
}

/// Returns a redacted copy of `buf` where credential-looking values are fully
/// masked. Any non-token byte (whitespace, punctuation, newline) terminates a
/// masked run. Byte-safe: non-ASCII bytes are copied through unchanged.
pub fn redact_secrets(buf: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(buf.len());
    let mut i = 0;
    while i < buf.len() {
        let mut matched_prefix: Option<&[u8]> = None;
        for prefix in KNOWN_SECRET_PREFIXES {
            if buf[i..].starts_with(prefix) {
                matched_prefix = Some(prefix);
                break;
            }
        }
        match matched_prefix {
            Some(prefix) => {
                out.extend_from_slice(prefix);
                i += prefix.len();
                let mut masked_any = false;
                while i < buf.len() && is_token_char(buf[i]) {
                    out.push(b'*');
                    masked_any = true;
                    i += 1;
                }
                if !masked_any {
                    out.push(b'*');
                }
            }
            None => {
                out.push(buf[i]);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_append_and_tail_bounded() {
        let logs = SessionLogs::new(10);
        logs.append("s", b"0123456789");
        assert!(logs.has("s"));
        // Append 5 more: ring keeps the last 10 bytes.
        logs.append("s", b"ABCDE");
        assert_eq!(logs.tail("s", 10), Some(b"56789ABCDE".to_vec()));
        // tail smaller than ring takes the tail slice.
        assert_eq!(logs.tail("s", 4), Some(b"BCDE".to_vec()));
        // Unknown session.
        assert_eq!(logs.tail("nope", 10), None);
        assert!(!logs.has("nope"));
    }

    #[test]
    fn test_ring_empty_session() {
        let logs = SessionLogs::new(10);
        logs.append("s", b"");
        assert!(!logs.has("s"));
        assert_eq!(logs.tail("s", 5), None);
    }

    #[test]
    fn test_ring_multiple_sessions_isolated() {
        let logs = SessionLogs::new(100);
        logs.append("a", b"aaaa");
        logs.append("b", b"bbbb");
        assert_eq!(logs.tail("a", 100), Some(b"aaaa".to_vec()));
        assert_eq!(logs.tail("b", 100), Some(b"bbbb".to_vec()));
    }

    #[test]
    fn test_tail_lines_last_n() {
        let logs = SessionLogs::new(10_000);
        logs.append("s", b"line1\nline2\nline3\nline4\n");
        assert_eq!(logs.tail_lines("s", 2), Some(b"line3\nline4\n".to_vec()));
        assert_eq!(
            logs.tail_lines("s", 10),
            Some(b"line1\nline2\nline3\nline4\n".to_vec())
        );
        assert_eq!(logs.tail_lines("s", 1), Some(b"line4\n".to_vec()));
        assert_eq!(logs.tail_lines("nope", 5), None);
    }

    #[test]
    fn test_tail_lines_no_trailing_newline() {
        let logs = SessionLogs::new(10_000);
        logs.append("s", b"a\nb\nc");
        assert_eq!(logs.tail_lines("s", 2), Some(b"b\nc".to_vec()));
        assert_eq!(logs.tail_lines("s", 1), Some(b"c".to_vec()));
    }

    #[test]
    fn test_redact_github_token() {
        let out = redact_secrets(b"token ghp_1234567890abcdef done");
        assert_eq!(out, b"token ghp_**************** done");
    }

    #[test]
    fn test_redact_glpat() {
        let out = redact_secrets(b"glpat-abcdef1234567890");
        assert_eq!(out, b"glpat-****************");
    }

    #[test]
    fn test_redact_bearer() {
        let out = redact_secrets(b"Authorization: Bearer abc.def.ghi");
        assert_eq!(out, b"Authorization: Bearer ***********");
    }

    #[test]
    fn test_redact_age_secret_key() {
        let out = redact_secrets(b"AGE-SECRET-KEY-1ABcDE+Fg/=");
        assert_eq!(out, b"AGE-SECRET-KEY-***********");
    }

    #[test]
    fn test_redact_github_fine_grained() {
        let out = redact_secrets(b"github_pat_11ABCxyz_1234567890abcdefgh");
        assert_eq!(out, b"github_pat_***************************");
    }

    #[test]
    fn test_redact_no_match_unchanged() {
        let input = b"no secrets here, just progress: 42% done\n";
        assert_eq!(redact_secrets(input), input);
    }

    #[test]
    fn test_redact_multibyte_safe() {
        let input = "🤖 hello ghp_tok123".as_bytes().to_vec();
        let out = redact_secrets(&input);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with('🤖'));
        assert!(text.contains("ghp_******"));
    }

    #[test]
    fn test_tail_redacted_applies_redaction() {
        let logs = SessionLogs::new(100);
        logs.append("s", b"login with Bearer supersecret now");
        let tail = logs.tail_redacted("s", 100).unwrap();
        assert_eq!(tail, b"login with Bearer *********** now");
    }
}

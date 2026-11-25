use serde::de::{Deserialize, IgnoredAny};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Transcript usability result.
#[derive(Debug, PartialEq)]
pub enum TranscriptStatus {
    /// File does not exist – start fresh with --session-id.
    Missing,
    /// At least one line parses as JSON – safe to resume.
    Usable,
    /// File exists but zero lines parse as JSON (empty, whitespace-only, binary garbage).
    /// Caller must quarantine and start fresh.
    Unusable,
}

/// Determines whether a transcript file is suitable for `--resume`.
///
/// Policy (verified with real claude 2.1.286):
/// * Missing file → `Missing` (fresh start).
/// * At least one line parses as any JSON value → `Usable` (resume ok; corrupt/truncated
///   other lines are tolerated by claude).
/// * Zero parseable lines → `Unusable` (empty file / garbage-only).
///
/// Scanning stops at the FIRST parseable line, so huge files cost O(1).
/// Uses `serde_json::Deserializer` with `IgnoredAny` — no DOM allocation.
pub fn classify_transcript(path: &Path) -> TranscriptStatus {
    if !path.exists() {
        return TranscriptStatus::Missing;
    }
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return TranscriptStatus::Unusable,
    };
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line_str = match line {
            Ok(s) => s,
            Err(_) => continue, // I/O error on a line → skip
        };
        let trimmed = line_str.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Try parsing as any JSON value using IgnoredAny (zero allocation).
        let mut de = serde_json::Deserializer::from_str(trimmed);
        if IgnoredAny::deserialize(&mut de).is_ok() {
            return TranscriptStatus::Usable;
        }
        // Non-parseable line: keep scanning (may be a truncated last line).
    }
    // Scanned all lines and none parsed.
    TranscriptStatus::Unusable
}

/// Backward-compat wrapper used by `session.rs`.
/// Returns `true` if the transcript is Missing or Usable (i.e. no quarantine needed).
pub fn session_file_is_valid(path: &Path) -> bool {
    matches!(
        classify_transcript(path),
        TranscriptStatus::Missing | TranscriptStatus::Usable
    )
}

/// Moves an unusable session transcript file aside to
/// `{path}.corrupt.{epoch_millis}.bak`.
/// Millisecond resolution ensures two quarantines never overwrite each other.
pub fn quarantine_corrupt_session_file(path: &Path) -> Result<PathBuf, String> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let backup_path = PathBuf::from(format!("{}.corrupt.{}.bak", path.display(), millis));
    fs::rename(path, &backup_path).map_err(|e| {
        format!(
            "Failed to move corrupt session file {} -> {}: {}",
            path.display(),
            backup_path.display(),
            e
        )
    })?;
    Ok(backup_path)
}

/// The resume-specific error printed by Claude CLI when a conversation cannot be resumed.
pub const RESUME_NOT_FOUND_ERROR: &str = "No conversation found with session ID";

/// Returns true if the captured output tail contains the Claude CLI resume error (case-insensitive).
pub fn output_contains_resume_not_found(tail: &str) -> bool {
    tail.to_lowercase()
        .contains(&RESUME_NOT_FOUND_ERROR.to_lowercase())
}

/// Identifies the size and modification timestamp of a transcript file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFingerprint {
    pub size: u64,
    pub mtime: Option<SystemTime>,
}

/// Captures the size and mtime of the transcript file if it exists.
pub fn get_transcript_fingerprint(path: &Path) -> Option<TranscriptFingerprint> {
    fs::metadata(path).ok().map(|m| TranscriptFingerprint {
        size: m.len(),
        mtime: m.modified().ok(),
    })
}

/// Tracks consecutive fast `--resume` failures where the transcript file has
/// the same size and mtime across failures.
#[derive(Debug, Default)]
pub struct ResumeFailTracker {
    last_fingerprint: Option<TranscriptFingerprint>,
    consecutive_failures: usize,
}

impl ResumeFailTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.last_fingerprint = None;
        self.consecutive_failures = 0;
    }

    pub fn last_fingerprint(&self) -> Option<&TranscriptFingerprint> {
        self.last_fingerprint.as_ref()
    }

    pub fn record_failure(&mut self, fingerprint: Option<TranscriptFingerprint>) -> usize {
        match (self.last_fingerprint.as_ref(), fingerprint.as_ref()) {
            (Some(last), Some(curr)) if last == curr => {
                self.consecutive_failures += 1;
            }
            (_, Some(curr)) => {
                self.last_fingerprint = Some(curr.clone());
                self.consecutive_failures = 1;
            }
            _ => {
                self.reset();
            }
        }
        self.consecutive_failures
    }

    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_nonexistent_file_is_missing() {
        let path = Path::new("/tmp/does_not_exist_session_cs_123456.jsonl");
        assert_eq!(classify_transcript(path), TranscriptStatus::Missing);
        assert!(session_file_is_valid(path));
    }

    #[test]
    fn test_valid_jsonl_session_file() {
        let temp_dir = env::temp_dir().join(format!("test_sess_valid_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("valid.jsonl");

        let content = "{\"type\": \"user\", \"message\": \"hello\"}\n\n{\"type\": \"assistant\", \"message\": \"hi\"}\n";
        fs::write(&path, content).unwrap();

        assert_eq!(classify_transcript(&path), TranscriptStatus::Usable);
        assert!(session_file_is_valid(&path));
        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Fix #2: a corrupt line in the MIDDLE must NOT quarantine a usable transcript.
    #[test]
    fn test_corrupt_middle_line_still_usable() {
        let temp_dir = env::temp_dir().join(format!("test_sess_mid_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("mid_corrupt.jsonl");

        // First line is valid JSON → Usable, even though second line is garbage.
        let content = "{\"type\": \"user\"}\nBROKEN JSON NOT VALID\n{\"type\": \"assistant\"}\n";
        fs::write(&path, content).unwrap();

        assert_eq!(classify_transcript(&path), TranscriptStatus::Usable);
        assert!(session_file_is_valid(&path));
        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Fix #2: truncated last line (no closing brace) must not quarantine when other lines parse.
    #[test]
    fn test_truncated_last_line_still_usable() {
        let temp_dir = env::temp_dir().join(format!("test_sess_trunc_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("truncated.jsonl");

        let content = "{\"type\": \"user\"}\n{\"type\": \"truncated\""; // no closing brace
        fs::write(&path, content).unwrap();

        assert_eq!(classify_transcript(&path), TranscriptStatus::Usable);
        assert!(session_file_is_valid(&path));
        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Fix #2: an empty file is Unusable (not Usable and not Missing).
    #[test]
    fn test_empty_file_is_unusable() {
        let temp_dir = env::temp_dir().join(format!("test_sess_empty_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("empty.jsonl");

        fs::write(&path, b"").unwrap();

        assert_eq!(classify_transcript(&path), TranscriptStatus::Unusable);
        assert!(!session_file_is_valid(&path));
        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Fix #2: a file containing only whitespace / newlines is Unusable.
    #[test]
    fn test_whitespace_only_file_is_unusable() {
        let temp_dir = env::temp_dir().join(format!("test_sess_ws_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("whitespace.jsonl");

        fs::write(&path, b"   \n  \n\t\n").unwrap();

        assert_eq!(classify_transcript(&path), TranscriptStatus::Unusable);
        assert!(!session_file_is_valid(&path));
        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Fix #2: a file with only garbage is Unusable.
    #[test]
    fn test_garbage_only_file_is_unusable() {
        let temp_dir = env::temp_dir().join(format!("test_sess_garbage_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("garbage.jsonl");

        fs::write(&path, b"NOT JSON AT ALL\nMORE GARBAGE\n\x00\xff").unwrap();

        assert_eq!(classify_transcript(&path), TranscriptStatus::Unusable);
        assert!(!session_file_is_valid(&path));
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_quarantine_moves_file_and_creates_backup() {
        let temp_dir = env::temp_dir().join(format!("test_sess_quar_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("bad.jsonl");
        fs::write(&path, b"GARBAGE").unwrap();

        let backup = quarantine_corrupt_session_file(&path).expect("Quarantine must succeed");
        assert!(!path.exists(), "original must be gone");
        assert!(backup.exists(), "backup must exist");
        // Backup name must contain milliseconds (not just seconds).
        let bname = backup.to_string_lossy();
        assert!(bname.contains(".corrupt."), "name format: {}", bname);
        assert!(bname.ends_with(".bak"), "name format: {}", bname);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Fix #2: empty file quarantined → fresh session started with --session-id.
    /// (Functional behaviour is tested in the black-box suite T14/T42/T43.)
    /// Here we just verify classify + quarantine + classify round-trip.
    #[test]
    fn test_empty_quarantine_then_missing() {
        let temp_dir = env::temp_dir().join(format!("test_sess_eq_{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let path = temp_dir.join("session.jsonl");

        fs::write(&path, b"").unwrap();
        assert_eq!(classify_transcript(&path), TranscriptStatus::Unusable);

        quarantine_corrupt_session_file(&path).unwrap();
        // After quarantine the original path no longer exists → Missing.
        assert_eq!(classify_transcript(&path), TranscriptStatus::Missing);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_output_contains_resume_not_found() {
        assert!(output_contains_resume_not_found(
            "Error: No conversation found with session ID 12345"
        ));
        assert!(output_contains_resume_not_found(
            "error: no conversation found with session id 12345"
        ));
        assert!(output_contains_resume_not_found(
            "PREFIX NO CONVERSATION FOUND WITH SESSION ID SUFFIX"
        ));
        assert!(!output_contains_resume_not_found("Error: API unavailable"));
        assert!(!output_contains_resume_not_found("Segmentation fault"));
        assert!(!output_contains_resume_not_found(""));
    }

    #[test]
    fn test_resume_fail_tracker_consecutive_same_fingerprint() {
        let mut tracker = ResumeFailTracker::new();
        let fp1 = Some(TranscriptFingerprint {
            size: 100,
            mtime: Some(SystemTime::UNIX_EPOCH),
        });

        assert_eq!(tracker.record_failure(fp1.clone()), 1);
        assert_eq!(tracker.record_failure(fp1.clone()), 2);
        assert_eq!(tracker.record_failure(fp1.clone()), 3);
        assert_eq!(tracker.consecutive_failures(), 3);
        assert_eq!(tracker.last_fingerprint(), fp1.as_ref());

        // Different fingerprint resets count to 1 with the new fingerprint
        let fp2 = Some(TranscriptFingerprint {
            size: 200,
            mtime: Some(SystemTime::UNIX_EPOCH),
        });
        assert_eq!(tracker.record_failure(fp2.clone()), 1);
        assert_eq!(tracker.record_failure(fp2.clone()), 2);
        assert_eq!(tracker.last_fingerprint(), fp2.as_ref());

        // Reset clears everything
        tracker.reset();
        assert_eq!(tracker.consecutive_failures(), 0);
        assert_eq!(tracker.last_fingerprint(), None);
        assert_eq!(tracker.record_failure(fp2), 1);
    }
}

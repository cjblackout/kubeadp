//! Per-run message queue file for the agy send-message mid-run emulation
//! (design HL-91 E4).
//!
//! The claude PTY engines deliver a message into a live session directly; a
//! batch engine such as agy has no live prompt channel once a turn is
//! running. The emulation layer therefore keeps a per-run queue as a plain
//! append-only text file (one line per message) inside the run's worktree,
//! named by the [`Run`] id. Messages written while the run is paused are held
//! there; resuming (or the start of the next turn) drains the queue and feeds
//! it to the wrapped engine as one continuation prompt.
//!
//! A queue file is plain text (never an index), so a crash mid-append is
//! safe: only whole lines are ever read and a torn tail line is left in
//! place for the next drain. Lines are newline-separated; any newlines inside
//! a message are escaped so one message always maps to one line
//! (`\n` -> `\\n`, a literal `\` doubled). Messages are stored verbatim
//! otherwise -- the file is data plumbing, never framed as agent input.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::engine::run::Run;

/// The queue file name (per worktree, keyed by run id).
pub const QUEUE_FILE_NAME: &str = ".engine-message-queue";

/// Escape a message into a single line: newlines become the two characters
/// `\n`, existing backslashes are doubled so the decoding is unambiguous.
pub fn escape_message(message: &str) -> String {
    message.replace('\\', "\\\\").replace('\n', "\\n")
}

/// The inverse of [`escape_message`]: restores newlines and single backslashes.
pub fn unescape_message(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('\\') => out.push('\\'),
                _ => out.push(c),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The queue file path for `run`: `<workdir>/.engine-message-queue`.
pub fn queue_path(run: &Run) -> PathBuf {
    run.workdir.join(QUEUE_FILE_NAME)
}

/// Appends `message` to the run's queue file (creating it and its directory
/// when absent). Returns the number of messages now queued (1 implies the
/// previous queue was empty).
pub fn enqueue(run: &Run, message: &str) -> Result<usize, std::io::Error> {
    let path = queue_path(run);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let before = len(run).unwrap_or(0);
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    writeln!(file, "{}", escape_message(message))?;
    file.flush()?;
    Ok(before + 1)
}

/// The number of messages currently queued for `run` (0 when the queue does
/// not exist or is empty).
pub fn len(run: &Run) -> Result<usize, std::io::Error> {
    let path = queue_path(run);
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut n = 0usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            n += 1;
        }
    }
    Ok(n)
}

/// The messages currently queued for `run`, in order (an empty vector when
/// the queue does not exist).
pub fn peek(run: &Run) -> Result<Vec<String>, std::io::Error> {
    let path = queue_path(run);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut messages = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            messages.push(unescape_message(line.trim_end()));
        }
    }
    Ok(messages)
}

/// Drains the queue: removes the file and returns the collected messages, so
/// a subsequent [`peek`]/[`len`] observes an empty queue. An absent queue
/// yields an empty vector (no error).
pub fn drain(run: &Run) -> Result<Vec<String>, std::io::Error> {
    let messages = peek(run)?;
    let path = queue_path(run);
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(messages)
}

/// Whether `path` is a queue file of this module (used by tests and to keep
/// `revert` from treating an abandoned queue as project work).
pub fn is_queue_path(path: &Path) -> bool {
    path.file_name()
        .map(|name| name == QUEUE_FILE_NAME)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::run::{Run, RunKind};

    fn test_run(dir: &Path) -> Run {
        Run::new("q-run", RunKind::Job, dir)
    }

    #[test]
    fn escape_round_trips_newlines_and_backslashes() {
        for text in [
            "plain",
            "line one\nline two",
            "back\\slash",
            "\nleading and trailing\n",
            "mixed \\n and \\\\ and real\nnewlines",
        ] {
            assert_eq!(unescape_message(&escape_message(text)), text);
        }
        assert_ne!(
            escape_message("a\nb"),
            "a\nb",
            "an encoded message must stay on a single line"
        );
    }

    #[test]
    fn enqueue_peek_drain_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let run = test_run(dir.path());
        assert_eq!(len(&run).unwrap(), 0);

        assert_eq!(enqueue(&run, "first message").unwrap(), 1);
        assert_eq!(enqueue(&run, "multi\nline").unwrap(), 2);
        assert_eq!(len(&run).unwrap(), 2);

        // The queue must be a plain file with one line per message.
        let raw = std::fs::read_to_string(queue_path(&run)).unwrap();
        assert_eq!(raw.lines().count(), 2);

        assert_eq!(
            peek(&run).unwrap(),
            vec!["first message".to_string(), "multi\nline".to_string()]
        );

        // Draining clears the file and returns the messages.
        assert_eq!(
            drain(&run).unwrap(),
            vec!["first message".to_string(), "multi\nline".to_string()]
        );
        assert_eq!(len(&run).unwrap(), 0);
        assert!(peek(&run).unwrap().is_empty());
        assert!(!queue_path(&run).exists());
    }

    #[test]
    fn drain_on_absent_queue_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let run = test_run(dir.path());
        assert!(drain(&run).unwrap().is_empty());
    }
}

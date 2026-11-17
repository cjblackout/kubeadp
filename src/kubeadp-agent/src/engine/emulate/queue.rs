//! Per-run message queue for the agy send-message mid-run emulation
//! (design HL-91 E4, HL-125 B3).
//!
//! The claude PTY engines deliver a message into a live session directly; a
//! batch engine such as agy has no live prompt channel once a turn is
//! running. The emulation layer therefore keeps a per-run queue stored in
//! `a2a_message` table (origin/sender 'api') or lazily imported from legacy
//! worktree files.
//! Messages written while the run is paused are held there; resuming (or
//! the start of the next turn) drains the queue and feeds it to the wrapped
//! engine as one continuation prompt.

use rusqlite::params;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::engine::run::Run;

/// The queue file name (per worktree, keyed by run id, legacy).
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

/// Appends `message` to the run's queue (in `a2a_message` table in state.sqlite if available,
/// falling back to file if no state database is present). Returns the number of messages now queued.
pub fn enqueue(run: &Run, message: &str) -> Result<usize, std::io::Error> {
    // If a state.sqlite exists in workdir or state_root, use table
    if let Some(state_root) = find_state_root(run) {
        if let Ok(db) = crate::state::StateDb::open(&state_root) {
            if let Ok(conn) = db.connection() {
                let msg_id = format!("msg-{}", uuid::Uuid::new_v4());
                let now = chrono::Utc::now().timestamp();
                let _ = conn.execute(
                    "INSERT INTO a2a_message (run_id, message_id, fake, sender, hop, framed, queued_at) \
                     VALUES (?1, ?2, 0, 'api', 0, ?3, ?4)",
                    params![run.id, msg_id, message, now],
                );
                return len(run);
            }
        }
    }

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

/// The number of messages currently queued for `run`.
pub fn len(run: &Run) -> Result<usize, std::io::Error> {
    let mut total = 0;
    if let Some(state_root) = find_state_root(run) {
        if let Ok(db) = crate::state::StateDb::open(&state_root) {
            if let Ok(conn) = db.connection() {
                if let Ok(n) = conn.query_row(
                    "SELECT COUNT(*) FROM a2a_message WHERE run_id = ?1 AND sender = 'api' AND injected_at IS NULL",
                    params![run.id],
                    |row| row.get::<_, usize>(0),
                ) {
                    total += n;
                }
            }
        }
    }

    let path = queue_path(run);
    if path.exists() {
        if let Ok(file) = File::open(&path) {
            let reader = BufReader::new(file);
            for l in reader.lines().map_while(Result::ok) {
                if !l.trim().is_empty() {
                    total += 1;
                }
            }
        }
    }
    Ok(total)
}

/// The messages currently queued for `run`, in order.
pub fn peek(run: &Run) -> Result<Vec<String>, std::io::Error> {
    let mut messages = Vec::new();
    if let Some(state_root) = find_state_root(run) {
        if let Ok(db) = crate::state::StateDb::open(&state_root) {
            if let Ok(conn) = db.connection() {
                if let Ok(mut stmt) = conn.prepare(
                    "SELECT framed FROM a2a_message \
                     WHERE run_id = ?1 AND sender = 'api' AND injected_at IS NULL \
                     ORDER BY queued_at ASC, message_id ASC",
                ) {
                    if let Ok(rows) = stmt.query_map(params![run.id], |row| row.get::<_, String>(0))
                    {
                        for msg in rows.flatten() {
                            messages.push(msg);
                        }
                    }
                }
            }
        }
    }

    let path = queue_path(run);
    if path.exists() {
        if let Ok(file) = File::open(&path) {
            let reader = BufReader::new(file);
            for l in reader.lines().map_while(Result::ok) {
                if !l.trim().is_empty() {
                    messages.push(unescape_message(l.trim_end()));
                }
            }
        }
    }
    Ok(messages)
}

/// Drains the queue: marks table rows injected or deletes them, removes the legacy file,
/// and returns the collected messages.
pub fn drain(run: &Run) -> Result<Vec<String>, std::io::Error> {
    let messages = peek(run)?;
    if let Some(state_root) = find_state_root(run) {
        if let Ok(db) = crate::state::StateDb::open(&state_root) {
            if let Ok(conn) = db.connection() {
                let now = chrono::Utc::now().timestamp();
                let _ = conn.execute(
                    "UPDATE a2a_message SET injected_at = ?1, channel = 'drain' \
                     WHERE run_id = ?2 AND sender = 'api' AND injected_at IS NULL",
                    params![now, run.id],
                );
            }
        }
    }

    let path = queue_path(run);
    if path.exists() {
        let _ = std::fs::remove_file(&path);
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

fn find_state_root(run: &Run) -> Option<PathBuf> {
    if run.workdir.join("state.sqlite").exists() {
        return Some(run.workdir.clone());
    }
    if let Some(parent) = run.workdir.parent() {
        if parent.join("state.sqlite").exists() {
            return Some(parent.to_path_buf());
        }
        if let Some(gp) = parent.parent() {
            if gp.join("state.sqlite").exists() {
                return Some(gp.to_path_buf());
            }
        }
    }
    None
}

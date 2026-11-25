//! Best-effort cleanup of agy (Antigravity CLI) conversation artifacts.
//!
//! When a job recorded an agy conversation ID (extracted from the agent's
//! `--json` output), `agent-run clean`/`gc` should also purge that
//! conversation's artifacts under `~/.gemini/antigravity-cli`:
//!
//! - the `conversation_summaries` row in `database/conversation_summary.db`
//!   (kept, with a hint to `agy conversation prune`, when other summaries
//!   still reference the conversation as parent or winning conversation),
//! - the raw per-conversation files (`conversations/<id>.db`,
//!   `presence/<id>.lock`, `annotations/<id>.pbtxt`),
//! - the `brain/<id>/` directory, and
//! - entries in `cache/last_conversations.json` whose value is the ID.
//!
//! The engine has no privilege separation: agy runs as the server's own
//! process user, so all reads/writes/removals here are direct. Every step is
//! best-effort: failures print a warning and cleanup continues with the next
//! step.

use std::fs;
use std::path::{Path, PathBuf};

/// Returns the antigravity-cli state directory `<home>/.gemini/antigravity-cli`.
pub fn default_antigravity_dir() -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    Some(home.join(".gemini").join("antigravity-cli"))
}

/// A conversation ID is used verbatim in path joins and `remove_dir_all`, so
/// it must be a single, safe path component: only ASCII alphanumerics, `-` and
/// `_` (UUID-like), and never `.` / `..`. Anything else is refused.
fn valid_conversation_id(id: &str) -> bool {
    id != "."
        && id != ".."
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Cleans up all artifacts for one agy conversation under `antigravity_root`.
///
/// Best-effort throughout: any single failure (file already gone, permission
/// denied, unreadable cache) prints a warning and does not abort the rest.
pub fn cleanup_conversation(antigravity_root: &Path, conversation_id: &str) {
    // An empty conversation_id must never reach the per-file/dir joins below:
    // `Path::join("")` is a no-op in Rust, so `root.join("brain").join("")`
    // resolves to `root/brain` itself -- a `Some("")` conversation_id (from a
    // corrupted record or an edge case in extraction) would otherwise cause
    // `cleanup_brain_dir`'s `rm -rf` to wipe every conversation's brain data,
    // not just one.
    if conversation_id.trim().is_empty() {
        eprintln!("Warning: refusing to clean up an empty conversation_id (would match the whole antigravity-cli directory)");
        return;
    }
    if !valid_conversation_id(conversation_id) {
        eprintln!(
            "Warning: refusing to clean up conversation_id '{}' (a conversation ID must be a single path component of ASCII alphanumerics, '-' and '_', like a UUID; no slashes, dots or other characters)",
            conversation_id
        );
        return;
    }
    if !antigravity_root.exists() {
        return;
    }
    cleanup_summary_db_row(antigravity_root, conversation_id);
    cleanup_raw_conversation_files(antigravity_root, conversation_id);
    cleanup_brain_dir(antigravity_root, conversation_id);
    cleanup_last_conversations_cache(antigravity_root, conversation_id);
}

/// Deletes the `conversation_summaries` row for the conversation, unless other
/// rows still reference it (as parent or winning conversation), in which case
/// the row is kept and the user is pointed at `agy conversation prune`.
///
/// Best-effort: a failure here prints a warning and does not abort the rest
/// of the cleanup.
fn cleanup_summary_db_row(root: &Path, conversation_id: &str) {
    let db_path = root.join("database").join("conversation_summary.db");
    if !db_path.exists() {
        return;
    }
    if let Err(e) = delete_summary_row(&db_path, conversation_id) {
        eprintln!(
            "Warning: failed to clean up conversation_summary row for '{}' in {}: {}",
            conversation_id,
            db_path.display(),
            e
        );
    }
}

/// Outcome of attempting to delete one conversation's summary row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SummaryRowOutcome {
    /// The row existed and was deleted.
    Deleted,
    /// The row exists but other summaries reference it; it was kept.
    Kept,
    /// Nothing to do (no db, no table, or no such row).
    Noop,
}

/// Opens `db_path` in-process and deletes the conversation's
/// `conversation_summaries` row unless other rows still reference it (as
/// parent or winning conversation).
fn delete_summary_row(db_path: &Path, conversation_id: &str) -> Result<SummaryRowOutcome, String> {
    // Open READ_WRITE without the CREATE flag: the caller already checked
    // `db_path.exists()`, and if the file vanished in the meantime a plain
    // `Connection::open` would silently create an empty stray database.
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|e| format!("could not open {}: {}", db_path.display(), e))?;
    let table_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'conversation_summaries'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("could not inspect {}: {}", db_path.display(), e))?;
    if table_exists == 0 {
        return Ok(SummaryRowOutcome::Noop);
    }
    let referenced: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM conversation_summaries WHERE parent_conversation_id = ?1 OR winning_conversation_id = ?1",
            rusqlite::params![conversation_id],
            |row| row.get(0),
        )
        .map_err(|e| format!("could not read {}: {}", db_path.display(), e))?;
    if referenced > 0 {
        eprintln!(
            "Warning: keeping conversation_summary row for '{}' because {} other conversation(s) still reference it; run `agy conversation prune {}` to clean those up",
            conversation_id, referenced, conversation_id
        );
        return Ok(SummaryRowOutcome::Kept);
    }
    let deleted = conn
        .execute(
            "DELETE FROM conversation_summaries WHERE conversation_id = ?1",
            rusqlite::params![conversation_id],
        )
        .map_err(|e| format!("delete failed: {}", e))?;
    Ok(if deleted > 0 {
        SummaryRowOutcome::Deleted
    } else {
        SummaryRowOutcome::Noop
    })
}

/// Removes the raw per-conversation files; best-effort per file.
fn cleanup_raw_conversation_files(root: &Path, conversation_id: &str) {
    for (dir, ext) in [
        ("conversations", "db"),
        ("presence", "lock"),
        ("annotations", "pbtxt"),
    ] {
        let path = root.join(dir).join(format!("{}.{}", conversation_id, ext));
        if !path.exists() {
            continue;
        }
        if let Err(e) = fs::remove_file(&path) {
            eprintln!("Warning: failed to remove {}: {}", path.display(), e);
        }
    }
}

/// Removes the `brain/<conversation_id>/` directory.
fn cleanup_brain_dir(root: &Path, conversation_id: &str) {
    let brain_dir = root.join("brain").join(conversation_id);
    if !brain_dir.exists() {
        return;
    }
    if let Err(e) = fs::remove_dir_all(&brain_dir) {
        eprintln!(
            "Warning: failed to remove brain directory {}: {}",
            brain_dir.display(),
            e
        );
    }
}

/// Removes entries from `cache/last_conversations.json` whose value is the
/// conversation ID; if the read/write/parse fails, warn and continue.
fn cleanup_last_conversations_cache(root: &Path, conversation_id: &str) {
    let cache_path = root.join("cache").join("last_conversations.json");
    if !cache_path.exists() {
        return;
    }
    let content = match fs::read_to_string(&cache_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!(
                "Warning: could not read {} ({}); skipping cache update",
                cache_path.display(),
                e
            );
            return;
        }
    };
    let mut value: serde_json::Value = match serde_json::from_str(&content) {
        Ok(value) => value,
        Err(e) => {
            eprintln!(
                "Warning: could not parse {} ({}); leaving it untouched",
                cache_path.display(),
                e
            );
            return;
        }
    };
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let target = serde_json::Value::String(conversation_id.to_string());
    let to_remove: Vec<String> = obj
        .iter()
        .filter(|(_, v)| *v == &target)
        .map(|(k, _)| k.clone())
        .collect();
    if to_remove.is_empty() {
        return;
    }
    for key in to_remove {
        obj.remove(&key);
    }
    let new_content = match serde_json::to_string_pretty(&value) {
        Ok(content) => content,
        Err(e) => {
            eprintln!(
                "Warning: could not serialize updated last_conversations.json: {}",
                e
            );
            return;
        }
    };
    if let Err(e) = fs::write(&cache_path, new_content) {
        eprintln!("Warning: could not update {}: {}", cache_path.display(), e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture_root() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join(".gemini").join("antigravity-cli");
        fs::create_dir_all(root.join("conversations")).unwrap();
        fs::create_dir_all(root.join("presence")).unwrap();
        fs::create_dir_all(root.join("annotations")).unwrap();
        fs::create_dir_all(root.join("brain").join("conv-1")).unwrap();
        fs::create_dir_all(root.join("cache")).unwrap();
        (tmp, root)
    }

    #[test]
    fn test_default_antigravity_dir_under_home() {
        let dir = default_antigravity_dir().expect("home dir should exist in CI/dev");
        let components: Vec<_> = dir.components().collect();
        assert_eq!(
            components[components.len() - 2..]
                .iter()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"),
            ".gemini/antigravity-cli"
        );
    }

    #[test]
    fn test_cleanup_conversation_removes_artifacts() {
        let (_tmp, root) = fixture_root();
        fs::write(root.join("conversations").join("conv-1.db"), b"x").unwrap();
        fs::write(root.join("presence").join("conv-1.lock"), b"x").unwrap();
        fs::write(root.join("annotations").join("conv-1.pbtxt"), b"x").unwrap();
        fs::write(
            root.join("cache").join("last_conversations.json"),
            r#"{"a":"conv-1","b":"conv-2"}"#,
        )
        .unwrap();

        cleanup_conversation(&root, "conv-1");

        assert!(!root.join("conversations").join("conv-1.db").exists());
        assert!(!root.join("presence").join("conv-1.lock").exists());
        assert!(!root.join("annotations").join("conv-1.pbtxt").exists());
        assert!(!root.join("brain").join("conv-1").exists());
        let cache: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(root.join("cache").join("last_conversations.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(cache.get("a"), None);
        assert_eq!(cache.get("b").unwrap(), "conv-2");
    }

    #[test]
    fn test_cleanup_empty_conversation_id_is_refused() {
        let (_tmp, root) = fixture_root();
        // If the guard were broken, `brain` would be wiped by the empty join.
        cleanup_conversation(&root, "");
        assert!(root.join("brain").exists());
    }

    #[test]
    fn test_summary_row_deleted_when_unreferenced() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("agy");
        fs::create_dir_all(root.join("database")).unwrap();
        let db = root.join("database").join("conversation_summary.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE conversation_summaries (
                    conversation_id TEXT PRIMARY KEY,
                    parent_conversation_id TEXT,
                    winning_conversation_id TEXT
                 );
                 INSERT INTO conversation_summaries VALUES ('conv-1', NULL, NULL);
                 INSERT INTO conversation_summaries VALUES ('conv-2', 'conv-1', NULL);",
            )
            .unwrap();
        }
        // conv-1 is referenced by conv-2 -> kept
        assert_eq!(
            delete_summary_row(&db, "conv-1").unwrap(),
            SummaryRowOutcome::Kept
        );
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute(
                "DELETE FROM conversation_summaries WHERE conversation_id = 'conv-2'",
                [],
            )
            .unwrap();
        }
        // now unreferenced -> deleted
        assert_eq!(
            delete_summary_row(&db, "conv-1").unwrap(),
            SummaryRowOutcome::Deleted
        );
        // missing row -> noop
        assert_eq!(
            delete_summary_row(&db, "conv-1").unwrap(),
            SummaryRowOutcome::Noop
        );
    }

    #[test]
    fn test_summary_row_noop_without_db_or_table() {
        let tmp = tempdir().unwrap();
        // no db at all: refused (no CREATE flag), and NO stray empty db is created
        let missing = tmp.path().join("missing.db");
        assert!(delete_summary_row(&missing, "c").is_err());
        assert!(!missing.exists());
        // db without the table
        let db = tmp.path().join("empty.db");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("CREATE TABLE other (x)")
            .unwrap();
        assert_eq!(
            delete_summary_row(&db, "c").unwrap(),
            SummaryRowOutcome::Noop
        );
    }

    #[test]
    fn test_cleanup_refuses_invalid_conversation_ids() {
        let bad_ids = ["", ".", "..", "../x", "a/b", "c d", "a\\b"];
        for id in bad_ids {
            let (_tmp, root) = fixture_root();
            fs::write(root.join("conversations").join("conv-1.db"), b"x").unwrap();
            cleanup_conversation(&root, id);
            // Nothing must be touched: no joins may escape per-conversation paths.
            assert!(
                root.join("conversations").join("conv-1.db").exists(),
                "id {id:?}"
            );
            assert!(root.join("brain").join("conv-1").exists(), "id {id:?}");
            assert!(root.join("brain").exists(), "id {id:?}");
            // No stray database may have been created.
            assert!(
                !root
                    .join("database")
                    .join("conversation_summary.db")
                    .exists(),
                "id {id:?}"
            );
        }
    }

    #[test]
    fn test_cleanup_accepts_uuid_like_conversation_id() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let (_tmp, root) = fixture_root();
        fs::create_dir_all(root.join("brain").join(uuid)).unwrap();
        fs::write(root.join("conversations").join(format!("{uuid}.db")), b"x").unwrap();
        cleanup_conversation(&root, uuid);
        assert!(!root.join("brain").join(uuid).exists());
        assert!(!root
            .join("conversations")
            .join(format!("{uuid}.db"))
            .exists());
        // Other conversations are untouched.
        assert!(root.join("brain").join("conv-1").exists());
    }
}

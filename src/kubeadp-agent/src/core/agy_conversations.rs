//! Recovers agy conversation IDs from agy's own on-disk conversation files.
//!
//! When a job's agy process dies before printing its final JSON line (e.g. the
//! server pod is rolled out mid-run), the job log carries no conversation ID
//! and `retry` refuses: nothing was captured. agy itself, however, persists
//! every conversation under `<home>/.gemini/antigravity-cli/conversations` as
//! `<conversation-id>.db` (SQLite, plus `-wal`/`-shm` side files): the prompt
//! text the job ran with appears verbatim inside `steps.step_payload`, so the
//! conversation that belongs to a prompt can be found by blob-scanning the
//! candidate databases.
//!
//! The recovery is deliberately conservative: a conversation ID is only
//! returned when EXACTLY ONE candidate database (recent, unclaimed,
//! UUID-named) contains the prompt marker. Zero or several matches both
//! yield `None` -- agent-run never guesses a conversation to resume.
//!
//! Uniqueness by construction: every prompt the runner hands agy carries a
//! per-job, per-turn reference line (`[agent-run ref <job_id>#<turn>]`, see
//! [`ref_token`]). The marker for such a job is that line's bytes, not the
//! prompt's 80-byte prefix: two jobs started in the same second with prompts
//! that share their opening bytes (e.g. continuation jobs with the same
//! boilerplate) therefore never collide, because each job's line names its
//! own (globally unique) job id and turn. The 80-byte prefix survives only as
//! the marker for job rows created before this change, whose stored prompts
//! carry no reference line and whose conversation files therefore contain
//! none either ([`recovery_marker_for`] picks between the two).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use rusqlite::{params, Connection, OpenFlags};

/// The antigravity-cli conversation store under `home`:
/// `<home>/.gemini/antigravity-cli/conversations`. Mirrors
/// `agy_cleanup::default_antigravity_dir` (same home lookup, deeper path).
pub fn conversations_dir(home: &Path) -> PathBuf {
    home.join(".gemini")
        .join("antigravity-cli")
        .join("conversations")
}

/// Resolves the conversation directory, using `home` when given and falling
/// back to the process home directory otherwise (`Config::agy_home` is the
/// test hook; production passes `None`).
pub fn effective_conversations_dir(home: Option<&Path>) -> Option<PathBuf> {
    home.map(PathBuf::from)
        .or_else(dirs::home_dir)
        .map(|home_dir| conversations_dir(&home_dir))
}

/// Builds the legacy blob marker: the first up to 80 bytes of the prompt,
/// used ONLY for job rows created before per-turn reference tokens existed
/// (their stored prompts carry no `[agent-run ref ...]` line, and neither do
/// the conversation files their agy runs wrote). See [`recovery_marker_for`].
///
/// agy stores the prompt sent to the model inside `steps.step_payload`
/// (protobuf), so the first bytes of the exact text the runner handed agy are
/// a reliable in-blob signature. Trims whitespace, strips a leading
/// `<recalled-context>...</recalled-context>` block (retries with
/// auto-recall prepend one: the recalled bytes belong to the memory bank, not
/// to the prompt that started the conversation), and returns the first up to
/// 80 bytes.
///
/// The 80-byte cut happens on a character boundary (cutting mid-sequence
/// would corrupt the UTF-8 the blob stores and could match the wrong
/// conversation). Returns `None` for empty (or all-recalled-context) prompts:
/// without a marker there is nothing to match on, and an empty marker would
/// match every row in every candidate.
///
/// CAVEAT (why the token marker replaced this one): the prefix is not unique
/// per job -- two jobs whose prompts share their first 80 bytes both match.
/// This is acceptable for the legacy fallback only, because it is gated to
/// rows whose conversations predate the tokens and it keeps the
/// single-match/`claimed`/spawn-window rules of [`find_conversation_id`]:
/// an ambiguous match still returns `None` (never a guess).
pub fn prompt_marker(prompt: &str) -> Option<Vec<u8>> {
    let stripped = crate::core::hindsight::strip_recalled_context(prompt);
    let text = stripped.trim().to_string();
    if text.is_empty() {
        return None;
    }
    let mut bytes = Vec::with_capacity(80);
    for ch in text.chars() {
        // A char that would not fit in full must be dropped whole: the
        // result is always at most 80 bytes and always ends on a char
        // boundary (UTF-8 safe for the blob comparison).
        if bytes.len() + ch.len_utf8() > 80 {
            break;
        }
        bytes.extend_from_slice(ch.encode_utf8(&mut [0u8; 4]).as_bytes());
    }
    Some(bytes)
}

/// Builds the per-job, per-turn reference line appended to every prompt the
/// runner hands agy: `[agent-run ref <job_id>#<turn>]`.
///
/// `<job_id>` is the job's own 8-hex id (unique across the state database)
/// and `<turn>` is 0 for the start prompt and the attempt number for each
/// later turn, so EVERY turn of every job yields a different token. The line
/// is pure ASCII, so its bytes are always a valid marker for the blob scan.
pub fn ref_token(job_id: &str, turn: u32) -> String {
    format!("[agent-run ref {job_id}#{turn}]")
}

/// Strips the tail of `prompt` that starts at this job's own reference line
/// (the `\n\n[agent-run ref <job_id>#<turn>]` suffix any turn appended),
/// keeping the line that precedes it. Returns `prompt` unchanged when no
/// line for THIS job appears: user text that merely mentions the phrase for
/// another job is never modified. Purely textual (character based), so it is
/// always valid UTF-8 for any input.
///
/// Also used to keep engine-side views of a tokenized stored prompt clean
/// (e.g. the recall query built from the last attempt's text).
pub fn strip_ref_line<'a>(prompt: &'a str, job_id: &str) -> &'a str {
    let needle = format!("[agent-run ref {job_id}#");
    if let Some(pos) = prompt.rfind(&needle) {
        if pos == 0 {
            return "";
        }
        // The suffix always starts with `\n\n` (see the appenders); drop the
        // line separator along with the tail. `pos >= 2` because `rfind`
        // found a non-empty needle at or after index 0 only when the string
        // holds it; guard anyway so a pathological offset cannot underflow.
        if prompt[..pos].ends_with('\n') {
            let without_leading_newlines = prompt[..pos].trim_end_matches(['\r', '\n']);
            return without_leading_newlines;
        }
    }
    prompt
}

/// Returns the prompt `prompt` is normalized to before it is stored or
/// passed to agy for attempt `turn` of job `job_id`: any reference line this
/// job already carries is replaced by this turn's line, so a prompt always
/// carries exactly one token, for its current turn.
///
/// Prompts that are empty apart from (a leading) `<recalled-context>` block
/// are returned unchanged: the engine refuses them earlier or the turn has
/// no searchable text of its own, and a bare token line would be the only
/// content in the prompt.
pub fn normalize_ref_prompt(prompt: &str, job_id: &str, turn: u32) -> String {
    if crate::core::hindsight::strip_recalled_context(prompt)
        .trim()
        .is_empty()
    {
        return prompt.to_string();
    }
    format!(
        "{}\n\n{}",
        strip_ref_line(prompt, job_id).trim_end(),
        ref_token(job_id, turn)
    )
}

/// The recovery marker for a prompt as stored on a job row.
///
/// Jobs whose stored prompt carries this job's reference line (created since
/// per-turn tokens became part of the stored prompt) recover through the
/// line's BYTES: the token the start attempt appended (turn 0, the turn that
/// created the conversation) is present in the conversation's first step as
/// soon as the file exists, and it names this job's id, so a scan for it can
/// never match another job's conversation -- even one whose payload embeds
/// this same prompt text (the 80-byte-prefix collision the legacy marker
/// used to suffer).
///
/// Rows without a reference line for this job (created before the change)
/// fall back to [`prompt_marker`], the conservative 80-byte prefix, so
/// existing conversations remain recoverable.
pub fn recovery_marker_for(prompt: &str, job_id: &str) -> Option<Vec<u8>> {
    if prompt.contains(&format!("[agent-run ref {job_id}#")) {
        return Some(ref_token(job_id, 0).into_bytes());
    }
    prompt_marker(prompt)
}

/// Returns `true` for a 36-character UUID-shaped name: 8-4-4-4-12 ASCII hex
/// digits separated by hyphens. agy names conversation files after the
/// (UUID) conversation ID; anything else in the directory (temp files,
/// non-UUID experiments, stray copies) must never be treated as a candidate.
fn is_uuid_like(name: &str) -> bool {
    let b = name.as_bytes();
    if b.len() != 36 {
        return false;
    }
    b.iter().enumerate().all(|(i, c)| {
        if matches!(i, 8 | 13 | 18 | 23) {
            *c == b'-'
        } else {
            c.is_ascii_hexdigit()
        }
    })
}

/// Finds the conversation ID whose database contains `marker`, scanning
/// `dir` for `<uuid>.db` files.
///
/// Candidate rules -- anything failing one is skipped, never an error:
/// - the file name stem is UUID-shaped (agy's conversation ID format),
/// - its mtime is at least `not_before - 5s` (the -5s slack absorbs clock
///   skew between the moment the runner spawned agy and the file mtime),
/// - its stem is not in `claimed` (a conversation ID another job row already
///   owns must not be re-adopted).
///
/// Each candidate is opened READ-ONLY; open and query failures are ignored
/// because side files may be locked by a running agy, truncated by a kill,
/// or not be SQLite at all. The probe is the same one verified against live
/// agy databases: a `steps` row whose `step_payload` blob contains the
/// marker.
///
/// Returns `Some(id)` ONLY when exactly one candidate matches; zero or
/// several matches both return `None`.
pub fn find_conversation_id(
    dir: &Path,
    marker: &[u8],
    not_before: SystemTime,
    claimed: &HashSet<String>,
) -> Option<String> {
    if marker.is_empty() {
        return None;
    }
    let entries = std::fs::read_dir(dir).ok()?;
    let floor = not_before.checked_sub(Duration::from_secs(5));

    let mut candidates: Vec<(String, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if !is_uuid_like(stem) || claimed.contains(stem) {
            continue;
        }
        if let Some(floor) = floor {
            let Ok(mtime) = std::fs::metadata(&path).and_then(|m| m.modified()) else {
                continue;
            };
            // mtime < floor means the file predates the job's spawn window.
            // (A mtime before the UNIX epoch is not representable here --
            // `duration_since` Errs for it -- and counts as "old".)
            let Ok(_) = mtime.duration_since(floor) else {
                continue;
            };
        }
        candidates.push((stem.to_string(), path));
    }
    // Deterministic scan order regardless of filesystem enumeration order.
    candidates.sort();

    let mut matches: Vec<String> = Vec::new();
    for (id, path) in &candidates {
        let Ok(conn) = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY) else {
            // WAL-locked, incomplete, or not a SQLite file at all: skip.
            continue;
        };
        let mut stmt = match conn
            .prepare("SELECT 1 FROM steps WHERE instr(step_payload, CAST(?1 AS BLOB)) > 0 LIMIT 1")
        {
            Ok(stmt) => stmt,
            Err(_) => continue,
        };
        let hit = stmt
            .query_row(params![marker], |row| row.get::<_, i64>(0))
            .map(|_| true)
            .unwrap_or(false);
        if !hit {
            continue;
        }

        // A job must never adopt a conversation whose payload contains a
        // DIFFERENT job's token.
        let mut check_stmt = match conn.prepare(
            "SELECT step_payload FROM steps WHERE instr(step_payload, CAST(?1 AS BLOB)) > 0",
        ) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let expected_job_id = extract_job_id_from_marker(marker);
        let has_foreign = match check_stmt
            .query_map(params![b"[agent-run ref "], |row| row.get::<_, Vec<u8>>(0))
        {
            Ok(rows) => {
                let mut found_foreign = false;
                for payload in rows.flatten() {
                    if contains_foreign_ref_token(&payload, expected_job_id) {
                        found_foreign = true;
                        break;
                    }
                }
                found_foreign
            }
            Err(_) => false,
        };
        if has_foreign {
            continue;
        }

        matches.push(id.clone());
    }
    // Only a single match is trustworthy; zero or several candidates both
    // mean "cannot tell" -> None.
    (matches.len() == 1).then(|| matches.pop().expect("len == 1 checked"))
}

/// Extracts the job ID from a reference marker if it has the format
/// `[agent-run ref <job_id>#<turn>]` (or bytes containing that token).
pub fn extract_job_id_from_marker(marker: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(marker).ok()?;
    let needle = "[agent-run ref ";
    let pos = s.find(needle)?;
    let rest = &s[pos + needle.len()..];
    let hash_pos = rest.find('#')?;
    let job_id = &rest[..hash_pos];
    if !job_id.is_empty()
        && job_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        Some(job_id)
    } else {
        None
    }
}

/// Checks whether `payload` contains a reference token `[agent-run ref <job_id>#`
/// for a job other than `expected_job_id` (if `expected_job_id` is None, ANY reference token
/// is considered a different job's token).
pub fn contains_foreign_ref_token(payload: &[u8], expected_job_id: Option<&str>) -> bool {
    let needle = b"[agent-run ref ";
    let mut search_from = 0;
    while search_from + needle.len() <= payload.len() {
        if let Some(rel_pos) = payload[search_from..]
            .windows(needle.len())
            .position(|w| w == needle)
        {
            let token_start = search_from + rel_pos + needle.len();
            if let Some(hash_rel) = payload[token_start..].iter().position(|&b| b == b'#') {
                let job_id_bytes = &payload[token_start..token_start + hash_rel];
                if let Ok(job_id) = std::str::from_utf8(job_id_bytes) {
                    if !job_id.is_empty()
                        && job_id
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                    {
                        match expected_job_id {
                            Some(expected) => {
                                if job_id != expected {
                                    return true;
                                }
                            }
                            None => {
                                return true;
                            }
                        }
                    }
                }
            }
            search_from = token_start;
        } else {
            break;
        }
    }
    false
}

/// All non-null `conversation_id` values currently recorded in the `jobs`
/// table: conversations other job rows already own.
pub fn claimed_conversation_ids(state_root: &Path) -> HashSet<String> {
    let Ok(conn) = crate::core::job::open_state_db(state_root) else {
        return HashSet::new();
    };
    let Ok(mut stmt) =
        conn.prepare("SELECT conversation_id FROM jobs WHERE conversation_id IS NOT NULL")
    else {
        return HashSet::new();
    };
    stmt.query_map([], |row| row.get::<_, String>(0))
        .ok()
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Creates `<stem>.db` with a `steps` table and one row whose payload
    /// embeds `marker` inside protobuf-ish framing (like agy's blobs: the
    /// prompt bytes sit inside a larger payload), so matches are found the
    /// same way as on live databases.
    fn insert_conv_db(dir: &Path, stem: &str, marker: &str) {
        let path = dir.join(format!("{stem}.db"));
        let conn = Connection::open(&path).expect("test: open conversation db");
        conn.execute_batch("CREATE TABLE steps (idx INTEGER, step_payload BLOB);")
            .unwrap();
        let framed = format!("proto-prefix-{marker}-proto-suffix");
        conn.execute(
            "INSERT INTO steps (idx, step_payload) VALUES (1, ?1)",
            params![framed.as_bytes()],
        )
        .unwrap();
    }

    fn new_dir() -> tempfile::TempDir {
        tempdir().unwrap()
    }

    fn recent() -> SystemTime {
        SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap()
    }

    #[test]
    fn marker_basic_and_cap() {
        let marker = prompt_marker("just plain prompt text").unwrap();
        assert_eq!(
            marker,
            "just plain prompt text".as_bytes(),
            "short prompts pass through unchanged"
        );
        let long = "a".repeat(100);
        let marker = prompt_marker(&long).unwrap();
        assert_eq!(marker.len(), 80, "marker is capped at 80 bytes");
        assert_eq!(&marker[..], "a".repeat(80).as_bytes());
    }

    #[test]
    fn marker_multibyte_cuts_on_char_boundary() {
        // 25 'é' = 50 bytes; adding 'héllo ' (8 bytes) keeps 58, then a 4-byte
        // character at byte 79+ must be dropped whole to keep <= 80 bytes on a
        // boundary.
        let prompt = format!("{}\u{1F680}", "é".repeat(40));
        // 40*2 + 4 = 84 bytes; the rocket (4 bytes) starts at byte 80 and must
        // be cut, leaving the 80-byte UTF-8 prefix.
        let marker = prompt_marker(&prompt).unwrap();
        assert!(
            marker.len() <= 80,
            "marker must not exceed 80 bytes, got {}",
            marker.len()
        );
        assert!(
            std::str::from_utf8(&marker).is_ok(),
            "marker must be valid UTF-8 (cut on a char boundary)"
        );
        assert_eq!(marker, "é".repeat(40).as_bytes());
    }

    #[test]
    fn marker_strips_recalled_context_prefix() {
        let prompt = "<recalled-context>\nsome old memory item\n</recalled-context>\n\nfix the bug";
        let marker = prompt_marker(prompt).unwrap();
        assert_eq!(
            marker,
            "fix the bug".as_bytes(),
            "the recalled block is agy-internal and must not be the marker"
        );
    }

    #[test]
    fn marker_empty_and_unclosed_recalled_block() {
        assert_eq!(prompt_marker("   "), None, "blank prompt has no marker");
        assert_eq!(prompt_marker(""), None);
        assert_eq!(
            prompt_marker("<recalled-context>\nonly recalled text\n</recalled-context>"),
            None,
            "a prompt that IS only a recalled block has no marker"
        );
        // Unclosed opening tag: strip_recalled_context refuses to strip
        // (no safe end) -- the whole text becomes the marker.
        let marker = prompt_marker("<recalled-context>truncated").unwrap();
        assert_eq!(marker, "<recalled-context>truncated".as_bytes());
    }

    #[test]
    fn find_returns_single_match() {
        let dir = new_dir();
        let conv_dir = dir.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        insert_conv_db(
            &conv_dir,
            "11111111-2222-3333-4444-555555555555",
            "needle text",
        );
        let marker = "needle text".as_bytes();
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(&conv_dir, marker, recent(), &claimed),
            Some("11111111-2222-3333-4444-555555555555".to_string())
        );
    }

    #[test]
    fn find_refuses_ambiguous_matches() {
        let dir = new_dir();
        let conv_dir = dir.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        insert_conv_db(
            &conv_dir,
            "aaaaaaaa-1111-3333-4444-555555555555",
            "needle text",
        );
        insert_conv_db(
            &conv_dir,
            "bbbbbbbb-1111-3333-4444-555555555555",
            "needle text",
        );
        let marker = "needle text".as_bytes();
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(&conv_dir, marker, recent(), &claimed),
            None,
            "two matching candidates: never guess"
        );
    }

    #[test]
    fn find_ignores_claimed_conversations() {
        let dir = new_dir();
        let conv_dir = dir.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        insert_conv_db(
            &conv_dir,
            "aaaaaaaa-1111-3333-4444-555555555555",
            "needle text",
        );
        let marker = "needle text".as_bytes();
        let claimed = HashSet::from(["aaaaaaaa-1111-3333-4444-555555555555".to_string()]);
        assert_eq!(
            find_conversation_id(&conv_dir, marker, recent(), &claimed),
            None,
            "a conversation already owned by another job row is not re-adopted"
        );
    }

    #[test]
    fn find_ignores_old_candidates() {
        let dir = new_dir();
        let conv_dir = dir.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        insert_conv_db(
            &conv_dir,
            "aaaaaaaa-1111-3333-4444-555555555555",
            "needle text",
        );
        let marker = "needle text".as_bytes();
        let claimed = HashSet::new();
        // not_before is in the future: not_before - 5s is still past the file's
        // just-created mtime, so the candidate is too old.
        let not_before = SystemTime::now()
            .checked_add(Duration::from_secs(30))
            .unwrap();
        assert_eq!(
            find_conversation_id(&conv_dir, marker, not_before, &claimed),
            None,
            "candidates older than the spawn window are not this job's"
        );
    }

    #[test]
    fn find_ignores_non_uuid_and_stray_files() {
        let dir = new_dir();
        let conv_dir = dir.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        fs::write(conv_dir.join("notes.db"), b"needle text here").unwrap();
        fs::write(conv_dir.join("my-conversation.db"), b"needle text here").unwrap();
        fs::write(
            conv_dir.join("aaaaaaaa-1111-3333-4444"),
            b"needle text here",
        )
        .unwrap();
        // Short hex, wrong grouping.
        fs::write(
            conv_dir.join("aaaaaaaa-111-3333-4444-555555555555.db"),
            b"needle text here",
        )
        .unwrap();
        let marker = "needle text".as_bytes();
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(&conv_dir, marker, recent(), &claimed),
            None,
            "non-UUID file names are never candidates"
        );
    }

    #[test]
    fn find_ignores_corrupt_and_locked_dbs() {
        let dir = new_dir();
        let conv_dir = dir.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        // Garbage, not SQLite: open must fail and be ignored.
        fs::write(
            conv_dir.join("aaaaaaaa-1111-3333-4444-555555555555.db"),
            b"this is not a sqlite database at all, just garbage bytes",
        )
        .unwrap();
        // A real db without the `steps` table: the query must fail and be ignored.
        Connection::open(conv_dir.join("bbbbbbbb-1111-3333-4444-555555555555.db"))
            .unwrap()
            .execute_batch("CREATE TABLE other (x INTEGER);")
            .unwrap();
        let marker = "needle text".as_bytes();
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(&conv_dir, marker, recent(), &claimed),
            None,
            "corrupt/incomplete databases are skipped, not fatal"
        );
    }

    #[test]
    fn find_missing_dir_is_none_not_error() {
        let dir = new_dir();
        let marker = "needle text".as_bytes();
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(
                &dir.path().join("does-not-exist"),
                marker,
                recent(),
                &claimed
            ),
            None
        );
    }

    #[test]
    fn claimed_ids_read_from_state_db() {
        let dir = new_dir();
        crate::core::job::ensure_state_store(dir.path()).unwrap();
        let conn = crate::core::job::open_state_db(dir.path()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, project, prompt, resolved_prompt, sandbox, mode, add_dirs, created_at, worktree_path, branch, base_ref, conversation_id, retry_count)
             VALUES ('j1', 'p', 'p', 'p', 0, 'm', '[]', 1, '/wt', '', 'main', 'owned-conv', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, project, prompt, resolved_prompt, sandbox, mode, add_dirs, created_at, worktree_path, branch, base_ref, conversation_id, retry_count)
             VALUES ('j2', 'p', 'p', 'p', 0, 'm', '[]', 1, '/wt', '', 'main', NULL, 0)",
            [],
        )
        .unwrap();
        let claimed = claimed_conversation_ids(dir.path());
        assert_eq!(claimed, HashSet::from(["owned-conv".to_string()]));
    }

    // -- per-turn reference tokens (uniqueness by construction) --------------

    #[test]
    fn token_format_differs_per_turn_and_job() {
        assert_eq!(ref_token("deadbeef", 0), "[agent-run ref deadbeef#0]");
        assert_eq!(ref_token("deadbeef", 3), "[agent-run ref deadbeef#3]");
        assert_ne!(ref_token("deadbeef", 0), ref_token("deadbeef", 1));
        assert_ne!(ref_token("deadbeef", 0), ref_token("beefdead", 0));
    }

    #[test]
    fn normalize_appends_token_and_refreshes_on_retries() {
        let turn0 = normalize_ref_prompt("do the thing", "j1234", 0);
        assert_eq!(turn0, "do the thing\n\n[agent-run ref j1234#0]");
        // A later turn of the same job replaces the old line instead of
        // stacking tokens: exactly one line, for the current turn.
        let turn1 = normalize_ref_prompt(&turn0, "j1234", 1);
        assert_eq!(turn1, "do the thing\n\n[agent-run ref j1234#1]");
        // Plain retries re-run an already tokenized stored prompt: the turn
        // number refreshes, the prompt body stays.
        let turn2 = normalize_ref_prompt(&turn1, "j1234", 2);
        assert_eq!(turn2, "do the thing\n\n[agent-run ref j1234#2]");
    }

    #[test]
    fn normalize_leaves_foreign_ref_lines_alone() {
        // User text that mentions ANOTHER job's line must not be stripped or
        // rewritten; this job's own line is added on top.
        let user_text = "compare with [agent-run ref otherjob#0] from the log";
        let normalized = normalize_ref_prompt(user_text, "j1234", 0);
        assert!(normalized.contains("[agent-run ref otherjob#0]"));
        assert!(normalized.ends_with("[agent-run ref j1234#0]"));
        // Stripping this job's line again recovers the original text.
        assert_eq!(strip_ref_line(&normalized, "j1234"), user_text);
        // ...and stripping ANOTHER job's line leaves everything untouched.
        assert_eq!(strip_ref_line(&normalized, "otherjob"), normalized.as_str());
    }

    #[test]
    fn normalize_noop_for_empty_and_recalled_only_prompts() {
        assert_eq!(normalize_ref_prompt("   ", "j1234", 0), "   ");
        assert_eq!(normalize_ref_prompt("", "j1234", 0), "");
        let only_recalled = "<recalled-context>\nonly memories\n</recalled-context>";
        assert_eq!(
            normalize_ref_prompt(only_recalled, "j1234", 0),
            only_recalled,
            "a prompt with no text of its own must not become a bare token"
        );
        // A recalled block followed by real text does get the token.
        let with_text = format!("{only_recalled}\n\nactual work");
        assert!(normalize_ref_prompt(with_text.as_str(), "j1234", 0)
            .ends_with("[agent-run ref j1234#0]"));
    }

    #[test]
    fn recovery_marker_prefers_token_over_prefix() {
        let tokenized = "same opening boilerplate".repeat(10);
        let stored = normalize_ref_prompt(&tokenized, "j1234", 0);
        // A tokenized row recovers through the start turn's token bytes, not
        // the 80-byte prefix (which the repeat shares with a sibling job).
        assert_eq!(
            recovery_marker_for(&stored, "j1234"),
            Some(ref_token("j1234", 0).into_bytes())
        );
        // A legacy row (no token line for the job) keeps the old 80-byte
        // prefix marker so pre-change conversations still recover.
        let legacy_prompt = format!("{} legacy tail text", "x".repeat(100));
        assert_eq!(
            recovery_marker_for(&legacy_prompt, "j1234"),
            prompt_marker(&legacy_prompt)
        );
        // Empty prompts have no marker at all.
        assert_eq!(recovery_marker_for("  ", "j1234"), None);
    }

    /// The incident shape: two jobs whose prompts are identical for the first
    /// 200 bytes (well past the old 80-byte prefix) and differ only at the
    /// end; each conversation db holds the full prompt its agy ran with,
    /// framed the way agy protobuf step payloads frame it (via
    /// `insert_conv_db`).
    fn twin_jobs_dir(dir: &Path, job_a: &str, job_b: &str, stem_a: &str, stem_b: &str) {
        let common = "c".repeat(200);
        let prompt_a = format!("{common} alpha tail of job A");
        let prompt_b = format!("{common} beta tail of job B");
        fs::create_dir_all(dir).unwrap();
        insert_conv_db(
            dir,
            stem_a,
            &format!("{prompt_a}\n\n{}", ref_token(job_a, 0)),
        );
        insert_conv_db(
            dir,
            stem_b,
            &format!("{prompt_b}\n\n{}", ref_token(job_b, 0)),
        );
    }

    #[test]
    fn token_marker_matches_only_own_file_for_identical_prefix_jobs() {
        let tmp = new_dir();
        let conv_dir = tmp.path().join("conversations");
        twin_jobs_dir(
            &conv_dir,
            "aaaa1111",
            "bbbb2222",
            "aaaaaaaa-1111-3333-4444-555555555555",
            "bbbbbbbb-2222-3333-4444-555555555555",
        );
        let claimed = HashSet::new();
        // Each job's token bytes match exactly one (its own) file, even
        // though the 80-byte prefixes of both payloads are identical.
        assert_eq!(
            find_conversation_id(
                &conv_dir,
                ref_token("aaaa1111", 0).as_bytes(),
                recent(),
                &claimed
            ),
            Some("aaaaaaaa-1111-3333-4444-555555555555".to_string())
        );
        assert_eq!(
            find_conversation_id(
                &conv_dir,
                ref_token("bbbb2222", 0).as_bytes(),
                recent(),
                &claimed
            ),
            Some("bbbbbbbb-2222-3333-4444-555555555555".to_string())
        );
    }

    #[test]
    fn token_marker_never_adopts_another_job_conversation() {
        // The live incident: job A polls before its own conversation file
        // exists; only B's file does, and B's file contains A's 80-byte
        // prompt prefix. A's legacy-style prefix scan would adopt B -- the
        // token scan must not.
        let tmp = new_dir();
        let conv_dir = tmp.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        let common = "c".repeat(200);
        let prompt_b = format!("{common} beta tail of job B");
        let stem_b = "bbbbbbbb-2222-3333-4444-555555555555";
        insert_conv_db(
            &conv_dir,
            stem_b,
            &format!("{prompt_b}\n\n{}", ref_token("bbbb2222", 0)),
        );

        let claimed = HashSet::new();
        // (a) the token marker finds nothing foreign:
        assert_eq!(
            find_conversation_id(&conv_dir, ref_token("aaaa1111", 0).as_bytes(), recent(), &claimed),
            None,
            "job A must not adopt job B's conversation even though B's payload embeds A's prompt prefix"
        );
        // (b) once A's own file appears, the same scan finds A's file (and
        // only A's, since B's file also carries A's prefix):
        let stem_a = "aaaaaaaa-1111-3333-4444-555555555555";
        insert_conv_db(
            &conv_dir,
            stem_a,
            &format!(
                "{} alpha tail of job A\n\n{}",
                common,
                ref_token("aaaa1111", 0)
            ),
        );
        assert_eq!(
            find_conversation_id(
                &conv_dir,
                ref_token("aaaa1111", 0).as_bytes(),
                recent(),
                &claimed
            ),
            Some(stem_a.to_string())
        );
        // (c) the legacy prefix marker on the same directory is ambiguous
        // (both files share the 80-byte prefix): it must refuse, which is why
        // tokenized jobs never fall back to it.
        let prefix = prompt_marker(&format!("{common} alpha tail of job A")).unwrap();
        assert_eq!(
            find_conversation_id(&conv_dir, &prefix, recent(), &claimed),
            None,
            "the old prefix scan stays conservative (ambiguity -> None)"
        );
    }

    #[test]
    fn legacy_row_without_token_recovers_via_prefix_marker() {
        // A row created before per-turn tokens existed: its stored prompt has
        // no ref line, and its conversation db therefore has none either.
        let tmp = new_dir();
        let conv_dir = tmp.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        let prompt = "legacy job prompt written before the token change";
        insert_conv_db(
            &conv_dir,
            "cccccccc-3333-3333-4444-555555555555",
            &format!("{prompt} continues"),
        );
        // `recovery_marker_for` selects the legacy prefix for this row, and
        // the prefix still finds the single conversation.
        let marker =
            recovery_marker_for(prompt, "jlegacy1").expect("legacy prompt has a prefix marker");
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(&conv_dir, &marker, recent(), &claimed),
            Some("cccccccc-3333-3333-4444-555555555555".to_string())
        );
    }

    #[test]
    fn legacy_row_never_adopts_conversation_with_foreign_token() {
        // A legacy row matching on prefix must never adopt a conversation
        // whose payload contains another job's reference token.
        let tmp = new_dir();
        let conv_dir = tmp.path().join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();
        let common = "common prefix text shared between legacy job and new job";
        let stem_b = "bbbbbbbb-2222-3333-4444-555555555555";
        insert_conv_db(
            &conv_dir,
            stem_b,
            &format!(
                "{common} beta tail of job B\n\n{}",
                ref_token("bbbb2222", 0)
            ),
        );

        let marker = prompt_marker(common).unwrap();
        let claimed = HashSet::new();
        assert_eq!(
            find_conversation_id(&conv_dir, &marker, recent(), &claimed),
            None,
            "legacy row must not adopt a conversation created by another tokenized job"
        );
    }

    #[test]
    fn uuid_shape_checks() {
        assert!(is_uuid_like("aaaaaaaa-1111-3333-4444-555555555555"));
        assert!(is_uuid_like("abcdef12-34ab-56cd-78ef-90abcdef1234"));
        assert!(!is_uuid_like("aaaaaaaa-1111-3333-4444-55555555555"));
        assert!(!is_uuid_like("aaaaaaaaa-111-3333-4444-5555555555"));
        assert!(!is_uuid_like("aaaaaaaa_1111_3333_4444_5555555555"));
        assert!(!is_uuid_like("zzzzzzzz-1111-3333-4444-555555555555"));
        assert!(!is_uuid_like(""));
    }
}

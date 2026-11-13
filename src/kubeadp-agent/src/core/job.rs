use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, Row};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

/// Status of an agent-run job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobStatus {
    Running,
    Done,
    Failed,
    SelfTerminated,
    Killed,
    /// The process exited cleanly but agy never produced a structured
    /// SUCCESS result with non-empty response text.
    Incomplete,
    /// The log shows a quota/rate-limit exhaustion signal from the upstream
    /// model provider (classified instead of waiting for the print timeout).
    QuotaExhausted,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Running => "running",
            JobStatus::Done => "done",
            JobStatus::Failed => "failed",
            JobStatus::SelfTerminated => "self-terminated",
            JobStatus::Killed => "killed",
            JobStatus::Incomplete => "incomplete",
            JobStatus::QuotaExhausted => "quota-exhausted",
        }
    }

    /// Parses a status string produced by `as_str`; returns `None` for unrecognized values.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(JobStatus::Running),
            "done" => Some(JobStatus::Done),
            "failed" => Some(JobStatus::Failed),
            "self-terminated" => Some(JobStatus::SelfTerminated),
            "killed" => Some(JobStatus::Killed),
            "incomplete" => Some(JobStatus::Incomplete),
            "quota-exhausted" => Some(JobStatus::QuotaExhausted),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Done
                | JobStatus::Failed
                | JobStatus::SelfTerminated
                | JobStatus::Killed
                | JobStatus::Incomplete
                | JobStatus::QuotaExhausted
        )
    }

    /// The valid status names, in enum order, for error messages.
    pub const ALL_NAMES: [&'static str; 7] = [
        "running",
        "done",
        "failed",
        "self-terminated",
        "killed",
        "incomplete",
        "quota-exhausted",
    ];
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Metadata for a single agent-run job, stored as one row in the `jobs` table
/// of `<state_root>/state.sqlite`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct JobMetadata {
    pub job_id: String,
    pub project: String,
    pub prompt: String,
    pub resolved_prompt: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub sandbox: bool,
    pub mode: String,
    pub add_dirs: Vec<PathBuf>,
    pub print_timeout: Option<String>,
    pub created_at: DateTime<Utc>,
    pub worktree_path: PathBuf,
    pub branch: String,
    pub base_ref: String,
    pub conversation_id: Option<String>,
    pub retry_count: u32,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub status: Option<JobStatus>,
    pub hindsight: Option<bool>,
    pub auto_recall: Option<bool>,
    pub auto_retain: Option<bool>,
    pub retry_prompt: Option<String>,
    /// Started with `--plan`: a read-only investigation where the agent must
    /// not modify files. Jobs are never removed by `result`; they are
    /// reclaimed only by an explicit `agent-run clean <id>` or
    /// `agent-run gc`.
    pub read_only: bool,
    /// The worktree was supplied externally via `--from-worktree`: cleanup
    /// must not remove the worktree or its pre-existing branch.
    pub external_worktree: bool,
    /// The process start time (in clock ticks since boot, from /proc/<pid>/stat)
    /// captured at the exact moment the process was spawned. Used to verify process
    /// identity before sending signals, guarding against PID reuse.
    pub pid_start_time: Option<u64>,
    /// Resolved Hindsight memory bank for this job. Persisted so retry, review,
    /// and auto-retain always retain into the same bank.
    pub hindsight_bank: Option<String>,
    /// Per-job proxy override (`<url>` or `"none"` to disable).
    ///
    /// Serialized in REDACTED form in every JSON output (credentials are
    /// stripped by [`crate::core::proxy::redact_proxy_url`]) so a URL such as
    /// `http://user:secret@host:8080` never reaches `status --json`,
    /// `list --json`, or an API response. The raw value stays in the state
    /// DB, where retry/re-execution needs it.
    #[serde(serialize_with = "serialize_proxy_redacted")]
    pub proxy: Option<String>,
    /// Opt-in maximum wall-clock runtime for this job (same duration syntax
    /// as `wait --timeout`), persisted so enforcement survives a server
    /// restart. `None` = unbounded (no default limit exists).
    pub max_runtime: Option<Duration>,
    /// Wall-clock time this attempt started, reset by `retry`. The start
    /// anchor for `max_runtime` enforcement; the `created_at` clock is
    /// shared by other consumers (gc age, status Elapsed) and must not
    /// change meaning.
    pub started_at: Option<DateTime<Utc>>,
    /// Explicit Hindsight recall query for this job (trimmed). Replaces the
    /// prompt as the recall query and is persisted so `retry` reuses it.
    pub hindsight_query: Option<String>,
    /// Sandbox attribution ({backend, runtime, policy_path}) recorded at every
    /// launch -- start and each retry, which goes through the same
    /// `build_command` path. `None` only for rows never launched (or written
    /// before the field existed). Serialized in every JSON output so status
    /// and info can print it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_info: Option<crate::sandbox::SandboxInfo>,
    /// The engine this job runs on (`"agy"`, `"claude"`, ...). `None` for
    /// rows created before engine selection existed (they are agy jobs by
    /// definition). Resolved from request > config > role default at start
    /// (HL-91 E1 §3.1), persisted so status/retry/kill re-derive the right
    /// driver after a server restart (HL-91 E5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
}

/// Serde hook for [`JobMetadata::proxy`]: every JSON rendering carries the
/// redacted URL (see [`crate::core::proxy::redact_proxy_url`]) instead of the raw
/// stored value. Affects serialization only; the DB write/read paths are
/// plain field access and always keep the raw URL.
fn serialize_proxy_redacted<S: serde::Serializer>(
    proxy: &Option<String>,
    ser: S,
) -> std::result::Result<S::Ok, S::Error> {
    let redacted: Option<String> = proxy.as_deref().map(crate::core::proxy::redact_proxy_url);
    serde::Serialize::serialize(&redacted, ser)
}

/// Returns the path of the state database: `<state_root>/state.sqlite`.
pub fn state_db_path(state_root: &Path) -> PathBuf {
    state_root.join("state.sqlite")
}

/// Opens the state database, creating the parent directory, the database file,
/// and the schema (via the numbered migrations in
/// [`crate::core::migrations`], applied exactly once per database) if they do
/// not exist yet.
///
/// WAL journal mode is enabled so multiple concurrent `agent-run` processes
/// (e.g. a running internal runner plus status/tail/kill commands) can safely
/// share the database with SQLite's own locking.
pub fn open_state_db(state_root: &Path) -> Result<Connection> {
    let path = state_db_path(state_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create state directory {}", parent.display()))?;
    }
    let conn = Connection::open(&path)
        .with_context(|| format!("Failed to open state database at {}", path.display()))?;
    // N12: the state database holds message text (and job state) and must
    // not be readable by other users. Tighten to 0600 right after creation;
    // the -wal/-shm siblings SQLite creates alongside are tightened after the
    // migrations below (the WAL is created by the journal-mode switch).
    tighten_state_db_permissions(&path);
    // The busy timeout MUST be set before any other pragma: two connections
    // that open the same NEW database at the same time (e.g. parallel tests,
    // or the worker and a CLI process) race the first `PRAGMA journal_mode =
    // WAL` on a fresh file, and without a timeout that pragma fails
    // immediately with SQLITE_BUSY instead of waiting for the other opener to
    // finish. A plain busy timeout of several seconds absorbs the initial
    // creation window and also the normal WAL writer contention
    // (e.g. `start` inserting a job while a job monitor thread updates
    // another job's PID/exit code).
    conn.busy_timeout(StdDuration::from_secs(5))
        .with_context(|| {
            format!(
                "Failed to set busy timeout on state database at {}",
                path.display()
            )
        })?;
    // Enable WAL with a short bounded retry: the busy timeout above lets a
    // concurrent opener's exclusive file-lock window pass, but on some SQLite
    // builds/configs `journal_mode` reports SQLITE_BUSY against a lock that a
    // repeated attempt clears immediately. Real corruption is never a busy
    // error -- SQLITE_NOTADB / malformed-header errors surface unchanged on
    // the last attempt.
    set_wal_with_retry(&conn, &path)?;
    crate::core::migrations::migrate(&conn).with_context(|| {
        format!(
            "Failed to migrate state database at {} (is the file corrupted?)",
            path.display()
        )
    })?;
    // N12: the migrations above created the -wal/-shm siblings (WAL mode) --
    // tighten those too, best effort.
    tighten_state_db_permissions(&path);
    Ok(conn)
}

/// N12: tightens the state database file (and its `-wal`/`-shm` siblings,
/// when present) to mode 0600. Best effort: a filesystem that does not
/// support permission bits leaves the files untouched.
fn tighten_state_db_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let tighten = |p: PathBuf| {
        if let Ok(meta) = fs::metadata(&p) {
            let mut perms = meta.permissions();
            if perms.mode() & 0o777 != 0o600 {
                perms.set_mode(0o600);
                let _ = fs::set_permissions(&p, perms);
            }
        }
    };
    tighten(path.to_path_buf());
    let mut sibling = path.as_os_str().to_owned();
    sibling.push("-wal");
    tighten(PathBuf::from(sibling));
    let mut sibling = path.as_os_str().to_owned();
    sibling.push("-shm");
    tighten(PathBuf::from(sibling));
}

/// Whether a `rusqlite` error is a transient lock (busy/locked) rather than a
/// real database problem (corruption, schema mismatch, ...).
fn is_busy_locked(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(e, _)
            if e.code == rusqlite::ErrorCode::DatabaseBusy
                || e.code == rusqlite::ErrorCode::DatabaseLocked
    )
}

/// Runs `PRAGMA journal_mode = WAL` with a bounded retry on transient
/// SQLITE_BUSY / SQLITE_LOCKED (a concurrent opener of a brand-new database
/// can hold the exclusive creation lock). Keeps the corrupt-file detection
/// for real corruption: the error is only reported once the retry budget is
/// exhausted.
fn set_wal_with_retry(conn: &Connection, path: &Path) -> Result<()> {
    const ATTEMPTS: usize = 8;
    const BACKOFF_MS: &[u64] = &[5, 10, 20, 40, 80, 160, 320];
    for attempt in 0..ATTEMPTS {
        match conn.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(e) if is_busy_locked(&e) && attempt + 1 < ATTEMPTS => {
                let ms = BACKOFF_MS
                    .get(attempt)
                    .copied()
                    .unwrap_or_else(|| BACKOFF_MS[BACKOFF_MS.len() - 1]);
                std::thread::sleep(StdDuration::from_millis(ms));
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "Failed to enable WAL journal mode on state database at {} (is the file corrupted?)",
                        path.display()
                    )
                });
            }
        }
    }
    unreachable!("the retry loop always returns")
}

/// Ensures the full state directory layout exists under `state_root`: the root
/// directory itself, the `logs/` subdirectory, and the SQLite state database
/// with its `jobs` table. Idempotent -- safe to run repeatedly.
pub fn ensure_state_store(state_root: &Path) -> Result<()> {
    fs::create_dir_all(state_root)
        .with_context(|| format!("Failed to create state directory {}", state_root.display()))?;
    fs::create_dir_all(JobMetadata::log_dir(state_root)).with_context(|| {
        format!(
            "Failed to create log directory {}",
            JobMetadata::log_dir(state_root).display()
        )
    })?;
    let _conn = open_state_db(state_root)?;
    Ok(())
}

/// Maps one database row to a `JobMetadata` value. Column order must match the
/// SELECT list in `JobMetadata::load`.
fn row_to_job(row: &Row<'_>) -> rusqlite::Result<JobMetadata> {
    let add_dirs_json: String = row.get(8)?;
    let created_at_ns: i64 = row.get(10)?;
    let status: Option<String> = row.get(18)?;
    Ok(JobMetadata {
        job_id: row.get(0)?,
        project: row.get(1)?,
        prompt: row.get(2)?,
        resolved_prompt: row.get(3)?,
        model: row.get(4)?,
        effort: row.get(5)?,
        sandbox: row.get::<_, i64>(6)? != 0,
        mode: row.get(7)?,
        add_dirs: serde_json::from_str(&add_dirs_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
        })?,
        print_timeout: row.get(9)?,
        created_at: DateTime::from_timestamp_nanos(created_at_ns),
        worktree_path: PathBuf::from(row.get::<_, String>(11)?),
        branch: row.get(12)?,
        base_ref: row.get(13)?,
        conversation_id: row.get(14)?,
        retry_count: row.get(15)?,
        pid: row.get(16)?,
        exit_code: row.get(17)?,
        status: status.and_then(|s| JobStatus::parse(&s)),
        hindsight: row.get::<_, Option<i64>>(19)?.map(|b| b != 0),
        auto_recall: row.get::<_, Option<i64>>(20)?.map(|b| b != 0),
        auto_retain: row.get::<_, Option<i64>>(21)?.map(|b| b != 0),
        retry_prompt: row.get(22)?,
        read_only: row.get::<_, i64>(23)? != 0,
        external_worktree: row.get::<_, i64>(24)? != 0,
        pid_start_time: row.get::<_, Option<i64>>(25)?.map(|v| v as u64),
        hindsight_bank: row.get(26)?,
        proxy: row.get(27)?,
        max_runtime: row.get::<_, Option<i64>>(28)?.map(Duration::nanoseconds),
        started_at: row.get::<_, Option<i64>>(29)?.and_then(|v: i64| {
            DateTime::from_timestamp(
                v.div_euclid(1_000_000_000),
                (v.rem_euclid(1_000_000_000)) as u32,
            )
        }),
        hindsight_query: row.get(30)?,
        sandbox_info: row
            .get::<_, Option<String>>(31)?
            .and_then(|s| serde_json::from_str::<crate::sandbox::SandboxInfo>(&s).ok()),
        engine: row.get(32)?,
    })
}

/// Escapes SQLite LIKE special characters (`%`, `_`, and `\`) using `\` as the escape character.
pub fn escape_like(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '%' || c == '_' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

impl JobMetadata {
    pub fn log_dir(state_root: &Path) -> PathBuf {
        state_root.join("logs")
    }

    pub fn log_path(state_root: &Path, job_id: &str) -> PathBuf {
        Self::log_dir(state_root).join(format!("{}.log", job_id))
    }

    pub fn save(&self, state_root: &Path) -> Result<()> {
        let add_dirs_json = serde_json::to_string(&self.add_dirs)
            .context("Failed to serialize add_dirs to JSON")?;
        let conn = open_state_db(state_root)?;
        conn.execute(
            "INSERT INTO jobs (
                job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode,
                add_dirs, print_timeout, created_at, worktree_path, branch, base_ref,
                conversation_id, retry_count, pid, exit_code, status, hindsight,
                auto_recall, auto_retain, retry_prompt, read_only, external_worktree,
                pid_start_time, hindsight_bank, proxy, max_runtime, started_at,
                hindsight_query, sandbox_info, engine
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33
            )
            ON CONFLICT(job_id) DO UPDATE SET
                project = excluded.project,
                prompt = excluded.prompt,
                resolved_prompt = excluded.resolved_prompt,
                model = excluded.model,
                effort = excluded.effort,
                sandbox = excluded.sandbox,
                mode = excluded.mode,
                add_dirs = excluded.add_dirs,
                print_timeout = excluded.print_timeout,
                created_at = excluded.created_at,
                worktree_path = excluded.worktree_path,
                branch = excluded.branch,
                base_ref = excluded.base_ref,
                conversation_id = excluded.conversation_id,
                retry_count = excluded.retry_count,
                pid = excluded.pid,
                exit_code = excluded.exit_code,
                status = excluded.status,
                hindsight = excluded.hindsight,
                auto_recall = excluded.auto_recall,
                auto_retain = excluded.auto_retain,
                retry_prompt = excluded.retry_prompt,
                read_only = excluded.read_only,
                external_worktree = excluded.external_worktree,
                pid_start_time = excluded.pid_start_time,
                hindsight_bank = excluded.hindsight_bank,
                proxy = excluded.proxy,
                max_runtime = excluded.max_runtime,
                started_at = excluded.started_at,
                hindsight_query = excluded.hindsight_query,
                sandbox_info = excluded.sandbox_info,
                engine = excluded.engine",
            params![
                self.job_id,
                self.project,
                self.prompt,
                self.resolved_prompt,
                self.model,
                self.effort,
                self.sandbox as i64,
                self.mode,
                add_dirs_json,
                self.print_timeout,
                self.created_at.timestamp_nanos_opt().unwrap_or(0),
                self.worktree_path.to_string_lossy().to_string(),
                self.branch,
                self.base_ref,
                self.conversation_id,
                self.retry_count,
                self.pid,
                self.exit_code,
                self.status.map(|s| s.as_str().to_string()),
                self.hindsight.map(|b| b as i64),
                self.auto_recall.map(|b| b as i64),
                self.auto_retain.map(|b| b as i64),
                self.retry_prompt,
                self.read_only as i64,
                self.external_worktree as i64,
                self.pid_start_time.map(|t| t as i64),
                self.hindsight_bank,
                self.proxy,
                self.max_runtime.and_then(|d: chrono::Duration| d.num_nanoseconds()),
                self.started_at.and_then(|t| t.timestamp_nanos_opt()),
                self.hindsight_query,
                self.sandbox_info
                    .as_ref()
                    .and_then(|s| serde_json::to_string(s).ok()),
                self.engine,
            ],
        )
        .with_context(|| {
            format!(
                "Failed to save job metadata '{}' to {}",
                self.job_id,
                state_db_path(state_root).display()
            )
        })?;
        Ok(())
    }

    /// Inserts this job as a NEW row using a plain `INSERT` that fails on an
    /// existing `job_id` (instead of upserting like `save`).
    ///
    /// `save` is an upsert: a colliding job id silently overwrites whatever
    /// job already holds that row. For the initial creation of a new job,
    /// a collision must be a hard error, not a silent destroy of the other
    /// job's state. All subsequent updates of an existing job keep using
    /// `save`.
    pub fn insert_new(&self, state_root: &Path) -> Result<()> {
        let add_dirs_json = serde_json::to_string(&self.add_dirs)
            .context("Failed to serialize add_dirs to JSON")?;
        let conn = open_state_db(state_root)?;
        conn.execute(
            "INSERT INTO jobs (
                job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode,
                add_dirs, print_timeout, created_at, worktree_path, branch, base_ref,
                conversation_id, retry_count, pid, exit_code, status, hindsight,
                auto_recall, auto_retain, retry_prompt, read_only, external_worktree,
                pid_start_time, hindsight_bank, proxy, max_runtime, started_at,
                hindsight_query, sandbox_info, engine
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33
            )",
            params![
                self.job_id,
                self.project,
                self.prompt,
                self.resolved_prompt,
                self.model,
                self.effort,
                self.sandbox as i64,
                self.mode,
                add_dirs_json,
                self.print_timeout,
                self.created_at.timestamp_nanos_opt().unwrap_or(0),
                self.worktree_path.to_string_lossy().to_string(),
                self.branch,
                self.base_ref,
                self.conversation_id,
                self.retry_count,
                self.pid,
                self.exit_code,
                self.status.map(|s| s.as_str().to_string()),
                self.hindsight.map(|b| b as i64),
                self.auto_recall.map(|b| b as i64),
                self.auto_retain.map(|b| b as i64),
                self.retry_prompt,
                self.read_only as i64,
                self.external_worktree as i64,
                self.pid_start_time.map(|t| t as i64),
                self.hindsight_bank,
                self.proxy,
                self.max_runtime.and_then(|d: chrono::Duration| d.num_nanoseconds()),
                self.started_at.and_then(|t| t.timestamp_nanos_opt()),
                self.hindsight_query,
                self.sandbox_info
                    .as_ref()
                    .and_then(|s| serde_json::to_string(s).ok()),
                self.engine,
            ],
        )
        .map_err(|e| -> anyhow::Error {
            if e.to_string().contains("UNIQUE constraint failed") {
                use crate::core::error::AgentRunError;
                AgentRunError::invalid(
                    "job_id_already_exists",
                    format!(
                        "Refusing to insert new job '{}': job id '{}' already exists in {} (a generated id collision must fail, never overwrite the existing job's row)",
                        self.job_id,
                        self.job_id,
                        state_db_path(state_root).display()
                    ),
                )
                .into()
            } else {
                anyhow::Error::from(e)
            }
        })
        .with_context(|| {
            format!(
                "Failed to insert job metadata '{}' into {}",
                self.job_id,
                state_db_path(state_root).display()
            )
        })?;
        Ok(())
    }

    pub fn load(state_root: &Path, job_id: &str) -> Result<Self> {
        let conn = open_state_db(state_root)?;
        let mut stmt = conn
            .prepare("SELECT job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode, add_dirs, print_timeout, created_at, worktree_path, branch, base_ref, conversation_id, retry_count, pid, exit_code, status, hindsight, auto_recall, auto_retain, retry_prompt, read_only, external_worktree, pid_start_time, hindsight_bank, proxy, max_runtime, started_at, hindsight_query, sandbox_info, engine FROM jobs WHERE job_id = ?1")
            .with_context(|| format!("Failed to read job metadata for '{}' from {}", job_id, state_db_path(state_root).display()))?;
        let mut rows = stmt
            .query_map(params![job_id], row_to_job)
            .with_context(|| {
                format!(
                    "Failed to query job '{}' from {}",
                    job_id,
                    state_db_path(state_root).display()
                )
            })?;
        match rows.next() {
            Some(Ok(meta)) => Ok(meta),
            Some(Err(e)) => bail!(
                "Corrupted or unreadable job metadata for '{}' in {}: {}",
                job_id,
                state_db_path(state_root).display(),
                e
            ),
            None => {
                let err = crate::core::error::AgentRunError::not_found(
                    "job_not_found",
                    format!(
                        "No such job found: '{}' (no job record in {})",
                        job_id,
                        state_db_path(state_root).display()
                    ),
                );
                Err(anyhow::Error::from(err))
            }
        }
    }

    /// Loads a job record, resolving a unique ID prefix if the given ID is not
    /// an exact match. Ambiguous prefixes are rejected with the matching IDs.
    pub fn resolve(state_root: &Path, job_id: &str) -> Result<Self> {
        if let Ok(meta) = Self::load(state_root, job_id) {
            return Ok(meta);
        }
        let conn = open_state_db(state_root)?;
        let mut stmt = conn
            .prepare(
                "SELECT job_id FROM jobs WHERE job_id LIKE ?1 ESCAPE '\\' ORDER BY length(job_id)",
            )
            .with_context(|| format!("Failed to resolve job prefix '{}'", job_id))?;
        let escaped_prefix = format!("{}%", escape_like(job_id));
        let ids: Vec<String> = stmt
            .query_map(params![escaped_prefix], |r| r.get(0))
            .with_context(|| {
                format!(
                    "Failed to query job prefix '{}' in {}",
                    job_id,
                    state_db_path(state_root).display()
                )
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("Failed to read job prefix matches for '{}'", job_id))?;
        match ids.len() {
            0 => Err(anyhow::Error::from(
                crate::core::error::AgentRunError::not_found(
                    "job_not_found",
                    format!(
                        "No such job found: '{}' (no job record in {})",
                        job_id,
                        state_db_path(state_root).display()
                    ),
                ),
            )),
            1 => Self::load(state_root, &ids[0]),
            _ => Err(anyhow::Error::from(
                crate::core::error::AgentRunError::invalid(
                    "job_id_ambiguous",
                    format!(
                        "Job ID prefix '{}' is ambiguous; matches: {}",
                        job_id,
                        ids.join(", ")
                    ),
                ),
            )),
        }
    }

    /// Deletes the job's record from the state database. A missing job is not an error.
    pub fn delete(state_root: &Path, job_id: &str) -> Result<()> {
        let conn = open_state_db(state_root)?;
        conn.execute("DELETE FROM jobs WHERE job_id = ?1", params![job_id])
            .with_context(|| {
                format!(
                    "Failed to delete job record '{}' from {}",
                    job_id,
                    state_db_path(state_root).display()
                )
            })?;
        Ok(())
    }
}

/// Persists the sandbox attribution of the current launch attempt onto a job
/// row without touching any other column. Called by the runner after every
/// `build_command` (a retry goes through the same path), so status/info can
/// print the active backend/runtime/policy file.
pub fn set_job_sandbox(
    state_root: &Path,
    job_id: &str,
    sandbox: &crate::sandbox::SandboxInfo,
) -> Result<()> {
    let conn = open_state_db(state_root)?;
    let json =
        serde_json::to_string(sandbox).context("Failed to serialize sandbox info to JSON")?;
    conn.execute(
        "UPDATE jobs SET sandbox_info = ?2 WHERE job_id = ?1",
        params![job_id, json],
    )
    .with_context(|| {
        format!(
            "Failed to update sandbox info for job '{}' in {}",
            job_id,
            state_db_path(state_root).display()
        )
    })?;
    Ok(())
}

/// Atomically writes a job's final status, exit code, and conversation ID,
/// but only if the row's stored status is not already `killed`.
///
/// This exists for the background runner's final-status save, which races
/// `agent-run kill` (a separate process) writing `status = 'killed'` to the
/// same row while the runner is still handling the child's exit. An
/// unconditional save there could land after kill's write and clobber
/// `killed` with a status computed from the raw SIGKILL exit code. The
/// single-row conditional UPDATE makes the check-and-write atomic: if
/// `killed` is already in the database, the update matches no row.
///
/// `expected_retry_count` guards a second race: `agent-run retry` reuses the
/// same job row, bumps `retry_count` and sets `status = 'running'`, while the
/// PREVIOUS attempt's monitor thread may still be finalising (reaping the
/// killed child, reading its log). Its late write would find `status !=
/// 'killed'` and clobber the retried attempt's `running` with the old
/// attempt's failure. When `Some(n)` is given, the update only applies to a
/// row whose `retry_count` is still `n` (the attempt the caller belongs to);
/// a superseded attempt's write is a no-op. `None` keeps the unguarded
/// behaviour.
///
/// Returns `true` if the row was updated, `false` if it was skipped because
/// it already carries `killed` or belongs to a newer retry.
pub fn save_final_status_unless_killed(
    state_root: &Path,
    job_id: &str,
    status: JobStatus,
    exit_code: Option<i32>,
    conversation_id: Option<&str>,
    expected_retry_count: Option<u32>,
) -> Result<bool> {
    let conn = open_state_db(state_root)?;

    // conversation_id is persisted unconditionally (when the runner actually
    // extracted one) regardless of the status race below: it's discovered
    // from the log at the same moment as the exit code, a killed job can
    // still have a real conversation_id worth recording, and unlike
    // status/exit_code it was never the subject of the kill.rs/runner race
    // this function exists to close -- skipping it here would silently lose
    // it (clean/gc's agy conversation cleanup needs it to purge conversation
    // artifacts, and only extracts it from the log itself as a fallback in
    // `retry`, not in `clean`/`gc`).
    if let Some(conv_id) = conversation_id {
        conn.execute(
            "UPDATE jobs SET conversation_id = ?2 WHERE job_id = ?1",
            params![job_id, conv_id],
        )
        .with_context(|| {
            format!(
                "Failed to save conversation_id for job '{}' in {}",
                job_id,
                state_db_path(state_root).display()
            )
        })?;
    }

    let updated = match expected_retry_count {
        Some(expected) => conn.execute(
            "UPDATE jobs SET status = ?2, exit_code = ?3 \
             WHERE job_id = ?1 AND retry_count = ?4 \
             AND (status IS NULL OR status != 'killed')",
            params![job_id, status.as_str().to_string(), exit_code, expected],
        ),
        None => conn.execute(
            "UPDATE jobs SET status = ?2, exit_code = ?3 \
             WHERE job_id = ?1 AND (status IS NULL OR status != 'killed')",
            params![job_id, status.as_str().to_string(), exit_code],
        ),
    }
    .with_context(|| {
        format!(
            "Failed to save final status for job '{}' in {}",
            job_id,
            state_db_path(state_root).display()
        )
    })?;
    Ok(updated > 0)
}

/// Atomically marks a job row as killed: a targeted UPDATE of `status` and
/// `exit_code` only, guarded by the expected `retry_count` AND by the row
/// still being in the state a kill was aimed at.
///
/// The old `kill::run` flow loaded the row, set
/// `status = Some(Killed)` / `exit_code = Some(137)` in memory and saved the
/// whole row back. That full-row write raced the background runner's own
/// save, which records the job's `conversation_id` at (almost) the same
/// moment: a stale snapshot re-saved after the runner's write silently
/// clobbered it. Updating only `status`/`exit_code` cannot touch any other
/// column. The `retry_count` guard mirrors
/// `save_final_status_unless_killed`: a job that has moved on to a newer
/// attempt is not rewritten by a delayed kill.
///
/// The status/exit_code guard closes the same class of race on the terminal
/// end: between deciding to terminate a job and actually writing the killed
/// state, the job may have FINISHED on its own (the runner recorded its
/// terminal status plus the real exit code, and the process exited, so a
/// "terminate" call found the PID already gone and succeeded). Writing
/// `killed`/137 over that final state would report a completed job as
/// killed and fabricate an exit code the process never had. So the row is
/// marked killed unless the job finished SUCCESSFULLY on its own, the one
/// outcome a kill must never overwrite: a row whose stored status says
/// `done` (finished on its own) or `killed` (already marked) is untouched,
/// as is any row the runner finalised with a clean `exit_code` of 0.
///
/// Every other recorded state is a kill target, even one that already
/// carries an exit code: after WE send SIGTERM the process exits on its own
/// and the runner/reaper may record its signal-derived exit code (143) --
/// and occasionally a terminal status -- BEFORE this UPDATE runs. Such a row
/// is ours to take: the `killed`/137 state is what the job actually is, the
/// SIGTERM we sent is exactly how it exited.
///
/// The caller is expected to pair this with a termination path that reports
/// whether IT killed the process or found it already gone
/// (`crate::core::commands::kill::terminate_process`) and with a post-termination
/// re-read of the row; this guard is the last line of defense that holds
/// even if both callers race.
///
/// Returns `true` if the row was marked killed, `false` if it belongs to a
/// different retry attempt, is gone, or finished successfully on its own
/// (status `done`/`killed` or a recorded exit code of 0).
pub fn mark_job_killed(state_root: &Path, job_id: &str, expected_retry_count: u32) -> Result<bool> {
    let conn = open_state_db(state_root)?;
    let updated = conn
        .execute(
            "UPDATE jobs SET status = 'killed', exit_code = 137 \
             WHERE job_id = ?1 AND retry_count = ?2 \
             AND (status IS NULL OR status NOT IN ('done', 'killed')) \
             AND (exit_code IS NULL OR exit_code <> 0)",
            params![job_id, expected_retry_count],
        )
        .with_context(|| {
            format!(
                "Failed to mark job '{}' as killed in {}",
                job_id,
                state_db_path(state_root).display()
            )
        })?;
    Ok(updated > 0)
}

/// Records a recovered `conversation_id` on a job row that has none.
///
/// Mirrors `mark_job_killed`: a single targeted UPDATE instead of a full-row
/// save. A full-row save of the in-memory snapshot could clobber columns
/// written concurrently (the job monitor records PID/exit code at the same
/// moment recovery runs, and `claim_retry_attempt` bumps `retry_count`),
/// while this touches exactly one column. The `IS NULL` guard makes the
/// write a no-op if a conversation ID was captured in the meantime (e.g.
/// the runner extracted one from the log right before this call) -- a
/// real captured ID must never be overwritten by a recovered one.
///
/// Returns `true` if the ID was stored, `false` if the row already had one
/// (or is gone).
pub fn restore_conversation_id(
    state_root: &Path,
    job_id: &str,
    conversation_id: &str,
) -> Result<bool> {
    let conn = open_state_db(state_root)?;
    let updated = conn
        .execute(
            "UPDATE jobs SET conversation_id = ?1 WHERE job_id = ?2 AND conversation_id IS NULL",
            params![conversation_id, job_id],
        )
        .with_context(|| {
            format!(
                "Failed to record recovered conversation_id '{}' for job '{}' in {}",
                conversation_id,
                job_id,
                state_db_path(state_root).display()
            )
        })?;
    Ok(updated > 0)
}

/// Generates a short, unique, filesystem-safe job id (8-char lowercase hex).
pub fn generate_job_id() -> String {
    let rand_bytes: [u8; 4] = rand::random();
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        rand_bytes[0], rand_bytes[1], rand_bytes[2], rand_bytes[3]
    )
}

/// Returns whether a job with this exact `job_id` already exists in the
/// state database. 4-byte random ids have a real birthday-collision
/// probability at any non-trivial job count, and `JobMetadata::save` is an
/// upsert, so a colliding id must be detected BEFORE the first write, not
/// silently absorbed into another job's row.
pub fn job_id_exists(state_root: &Path, job_id: &str) -> Result<bool> {
    let conn = open_state_db(state_root)?;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = ?1)",
            params![job_id],
            |row| row.get(0),
        )
        .with_context(|| {
            format!(
                "Failed to check job id '{}' in {}",
                job_id,
                state_db_path(state_root).display()
            )
        })?;
    Ok(exists)
}

/// Generates a job id that is guaranteed (at call time) not to exist in the
/// state database: loops [`generate_job_id`] until a free id is found.
///
/// Pair this with `JobMetadata::insert_new` for first-time creation of a job
/// row: the loop removes the practical collision risk and `insert_new` turns
/// the theoretical remaining race (two concurrent `start`s generating the
/// same id between check and insert) into a hard error instead of a
/// silent overwrite of the other job's row.
pub fn generate_unique_job_id(state_root: &Path) -> Result<String> {
    // 2^32 possible ids: a few hundred tries make a collision astronomically
    // unlikely in an already-crowded store; bail out well before that.
    for _ in 0..1024 {
        let id = generate_job_id();
        if !job_id_exists(state_root, &id)? {
            return Ok(id);
        }
    }
    bail!(
        "Could not generate a collision-free job id in {} attempts in {}",
        1024,
        state_db_path(state_root).display()
    )
}

/// Checks whether an OS process ID is currently alive on Linux via `/proc/<pid>`.
pub fn is_pid_alive(pid: u32) -> bool {
    let proc_path = PathBuf::from(format!("/proc/{}", pid));
    if !proc_path.exists() {
        return false;
    }

    // Check status file to avoid counting zombie processes as running
    let status_path = proc_path.join("status");
    if let Ok(content) = fs::read_to_string(status_path) {
        for line in content.lines() {
            if line.starts_with("State:") {
                // 'Z (zombie)' means process terminated and is awaiting reap
                if line.contains('Z') {
                    return false;
                }
                break;
            }
        }
    }

    true
}

/// Extracts the start time (field 22, in clock ticks since system boot) from `/proc/<pid>/stat`.
///
/// In Linux procfs, `/proc/<pid>/stat` contains space-separated fields where field 2 is the process comm
/// in parentheses (which may itself contain spaces or closing parentheses). To parse robustly, locate the
/// last ')' and tokenize the remainder: field 3 (state) is index 0, so field 22 (starttime) is index 19.
pub fn get_process_start_time(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{}/stat", pid)).ok()?;
    let rparen = stat.rfind(')')?;
    let rest = stat[rparen + 1..].trim_start();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() > 19 {
        fields[19].parse::<u64>().ok()
    } else {
        None
    }
}

/// Conservative process-identity check for rows that have NO recorded
/// `pid_start_time` (rows written before that column existed): the PID is
/// only accepted as the job's own if its `/proc/<pid>/cmdline` still looks
/// like a job process (the `agy`/`claude` engine, the always-on
/// `sandbox-exec` wrapper they run under, the engine itself, or a test
/// runner process such as `sleep`).
///
/// Rows without a start time must not pass the identity check
/// unconditionally: a reused PID would make a dead job look `Running`
/// (inflating the max-parallel count) or let a kill target an unrelated
/// process.
pub(crate) fn is_job_process_cmdline(pid: u32) -> bool {
    let bytes = match fs::read(format!("/proc/{}/cmdline", pid)) {
        Ok(b) if !b.is_empty() => b,
        _ => return false,
    };
    let s = String::from_utf8_lossy(&bytes);
    s.contains("agy")
        || s.contains("claude")
        || s.contains("sandbox-exec")
        || s.contains("agent-run")
        || s.contains("agent_core")
        || s.contains("kubeadp_agent")
        || s.contains("sleep")
}

/// Computes the live job status.
///
/// Precedence:
/// 1. If PID is recorded and alive in `/proc/<pid>`, status is `JobStatus::Running`.
/// 2. If the job was explicitly marked as `JobStatus::Killed`, status is `JobStatus::Killed`.
/// 3. Otherwise (PID dead or unrecorded), the status is derived from the
///    recorded exit code and the captured log (`determine_status_from_signals`):
///    - A structured SUCCESS with non-empty response text is the only thing
///      that counts as `JobStatus::Done`; it wins over any other signal,
///      except a background-task cutoff in the log (bullet below).
///    - A quota/rate-limit exhaustion signal anywhere in the log
///      (`JobStatus::QuotaExhausted`) beats all remaining signals, so a quota
///      job is classified immediately instead of waiting for the print timeout.
///    - A background-task cutoff signal in the log forces `JobStatus::Incomplete`
///      instead of `JobStatus::Done`: agy cut the turn off with pending
///      background work, so the response is a "will wait" promise, not a
///      final answer. A quota signal present alongside it still wins.
///    - Exit code 0 with no complete SUCCESS (empty response or no structured
///      output at all) is `JobStatus::Incomplete`.
///    - Non-zero exit (137/143 without a structured error) is
///      `JobStatus::SelfTerminated`; other non-zero exits `JobStatus::Failed`.
///    - No exit code recorded: structured error is `JobStatus::Failed`,
///      anything else `JobStatus::SelfTerminated`.
pub fn compute_status(meta: &JobMetadata, state_root: &Path) -> JobStatus {
    // 1. Check if PID is actively running and matches recorded start time.
    //    When no start time was recorded for the row, fall back to a
    //    conservative cmdline check instead of trusting the PID blindly:
    //    a recycled PID must not make a dead job report as Running.
    if let Some(pid) = meta.pid {
        if is_pid_alive(pid) {
            let is_same = match meta.pid_start_time {
                Some(stored) => get_process_start_time(pid) == Some(stored),
                None => is_job_process_cmdline(pid),
            };
            if is_same {
                return JobStatus::Running;
            }
        }
    }

    // 2. If the job was explicitly marked as killed, retain that distinct status
    if meta.status == Some(JobStatus::Killed) {
        return JobStatus::Killed;
    }

    // 2b. A freshly created (or freshly re-claimed) job has no pid yet -- the
    //     monitor writes it only after the process actually starts. During
    //     that gap the row would otherwise classify from the empty log /
    //     missing exit code and look terminal (SelfTerminated), which makes
    //     `wait` exit early and lets `gc` prune the starting job. New rows
    //     only (created < 2 min ago): an OLD pidless 'running' row is an
    //     orphan of a dead server and must still classify from the log so
    //     `retry` can resume it (and so gc can consider it).
    if meta.pid.is_none()
        && meta.exit_code.is_none()
        && meta.status == Some(JobStatus::Running)
        && Utc::now() - meta.created_at < Duration::minutes(2)
    {
        return JobStatus::Running;
    }

    // 3. Use the recorded exit code from the job record.
    //
    // The log is a raw capture of agy's stdout/stderr (plus appended retry
    // banners and diagnostics) and is not guaranteed to be valid UTF-8: a
    // single stray byte (e.g. from a crashed tool inside the job) must not
    // make the whole read fail and the log come back EMPTY, which would
    // classify a cleanly-finished job as Incomplete. Decode lossily instead.
    let log_path = JobMetadata::log_path(state_root, &meta.job_id);
    let log_content = match fs::read(&log_path) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(_) => String::new(),
    };

    // 3b. Engine-aware classification. The recorded `engine` selects the
    //     status taxonomy whose stream the log carries: claude runs stream
    //     NDJSON `{"type":"result",...}` lines (a different shape from agy's
    //     `{"event":"result",...}` / `status` lines), so the claude job driver
    //     classifies them with the claude rules. A NULL/absent engine row is
    //     a historical agy job (agy was the only driver before engine
    //     selection), so the agy path stays the unconditional default and is
    //     byte-identical to the pre-E5 behaviour.
    if meta.engine.as_deref() == Some("claude") {
        determine_status_from_claude(meta.exit_code, &log_content)
    } else {
        determine_status_from_signals(meta.exit_code, &log_content)
    }
}

/// Status classification from structured agy JSON output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredStatus {
    Success,
    Error,
}

/// Scans `log` from the end for the last structured result (legacy
/// single-object line or stream `event:"result"` line) with a recognized
/// `status` field. Returns the recognized status plus the `response` text of
/// that same result (empty string when the field is missing).
///
/// Recognizes "SUCCESS", "DONE", "OK" (case-insensitive) as
/// `StructuredStatus::Success` and "ERROR", "FAILED" (case-insensitive) as
/// `StructuredStatus::Error`. Unrecognized status values are skipped, keeping
/// the scan going at earlier lines.
///
/// An ERROR/FAILED line carries its own `response` field (empty string when
/// absent): agy can finish a turn with a complete answer and still report a
/// transient upstream error afterwards, so a recognized failure line can hold
/// finished work worth surfacing.
fn scan_final_structured(log: &str) -> Option<(StructuredStatus, String)> {
    for line in log.lines().rev() {
        // A claude stream-json terminal line has no agy `status` field, so
        // `structured_value` would skip it; recognize it first (its text is
        // the `result`/`error`/`message` payload).
        if let Some((outcome, text)) = claude_structured_text(line) {
            return Some((outcome, text));
        }
        let Some((status, val)) = structured_value(line) else {
            continue;
        };
        match status.to_ascii_uppercase().as_str() {
            "SUCCESS" | "DONE" | "OK" => {
                let response = val
                    .get("response")
                    .and_then(|r| r.as_str())
                    .unwrap_or("")
                    .to_string();
                return Some((StructuredStatus::Success, response));
            }
            "ERROR" | "FAILED" => {
                // The terminal ERROR/FAILED line may still carry a complete
                // `response` (e.g. agy finished its turn and produced a full
                // answer, then reported a transient upstream 5xx on a later
                // retry attempt). Preserve that text so the `result` command
                // can surface it instead of discarding the finished work. The
                // status variant is unchanged, so `determine_status_from_signals`
                // (which only looks at the variant, not the text) still
                // classifies the job `Failed`.
                let response = val
                    .get("response")
                    .and_then(|r| r.as_str())
                    .unwrap_or("")
                    .to_string();
                return Some((StructuredStatus::Error, response));
            }
            _ => {}
        }
    }
    None
}

/// Returns the `response` text of the last recognized structured JSON line in
/// the LAST attempt of `log`, for BOTH a terminal SUCCESS and a terminal
/// ERROR/FAILED line (empty string when the field is missing). `None` when
/// that attempt contains no recognized structured line.
///
/// Like `determine_status_from_signals`, only the text after the last retry
/// banner is scanned: a SUCCESS line left behind by a superseded attempt must
/// not be surfaced as the result of a later attempt that crashed without any
/// structured output.
pub fn extract_final_response(log: &str) -> Option<String> {
    scan_final_structured(last_attempt_log(log)).map(|(_, response)| response)
}

/// Case-insensitive markers that indicate the upstream model provider rejected
/// the request because of quota or rate limiting.
const QUOTA_MARKERS: &[&str] = &[
    "resource_exhausted",
    "insufficient_quota",
    "quota exceeded",
    "quota-exhausted",
    "quota limit",
    "exceeded your quota",
    "rate limit",
    "too many requests",
];

/// Returns true when `log` contains a quota/rate-limit exhaustion signal.
///
/// Lines that are stream-json `step_update` events are EXCLUDED from the scan:
/// they carry the agent's own free text (`text_delta`, tool output), which can
/// legitimately contain words like "rate limit" (for example when the agent
/// reads or quotes documentation about rate limiting) without the upstream
/// provider actually having rejected the request. Every other line -- plain
/// text, `event:"result"` lines (including their `error` text), and stderr
/// output captured in the log -- keeps matching exactly as before, so logs
/// without any `event` lines behave identically to the old whole-text scan.
pub fn has_quota_exhaustion_signal(log: &str) -> bool {
    for line in log.lines() {
        if is_stream_step_update(line) {
            continue;
        }
        let lower = line.to_ascii_lowercase();
        if QUOTA_MARKERS.iter().any(|m| lower.contains(m)) {
            return true;
        }
    }
    false
}

/// The `agent-run retry` banner written to the job log before each new
/// attempt (see `commands::retry`): `==================== RETRY AT <ts>
/// ====================`.
const RETRY_AT_MARKER: &str = "==================== RETRY AT";

/// Returns the portion of `log` after the LAST retry banner line, i.e. the
/// text of the most recent attempt only. Returns `log` unchanged when no
/// retry banner is present.
///
/// A retried job accumulates every attempt in one log file. Status signals
/// that are searched anywhere in the text (currently the quota/rate-limit
/// scan) must only look at the latest attempt, otherwise a signal from a
/// superseded attempt (e.g. a rate-limit error in attempt 1 that attempt 2
/// then overcame) misclassifies the whole job.
fn last_attempt_log(log: &str) -> &str {
    let Some(marker) = log.rfind(RETRY_AT_MARKER) else {
        return log;
    };
    // Skip past the banner line itself.
    let rest = &log[marker..];
    match rest.find('\n') {
        Some(nl) => &log[marker + nl + 1..],
        None => &log[marker..],
    }
}

/// Parses a log line as a JSON object, trimming surrounding whitespace.
/// Returns `None` when the line is not a JSON object (does not start with
/// `{` / end with `}`, or fails to parse, or parses to a non-object). This is
/// the generic "is this an NDJSON event line" check the stream-json aware
/// scans build on.
fn json_object_line(line: &str) -> Option<serde_json::Value> {
    let trimmed = line.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return None;
    }
    let val = serde_json::from_str::<serde_json::Value>(trimmed).ok()?;
    val.is_object().then_some(val)
}

/// Recognizes one of agy's structured JSON result objects in EITHER output
/// format. Returns the `status` string and the structured object that
/// `response` / `error` are read from.
///
/// Two shapes are recognized:
/// 1. Legacy (`--output-format json`): a line that is itself a JSON object
///    with a string top-level `status` field. The whole line object is the
///    structured value.
/// 2. Stream (`--output-format stream-json`): the final
///    `{"event":"result","result":{...}}` line, whose `result` field is an
///    object carrying a string `status`. The INNER `result` object is the
///    structured value, so `response` / `error` (nested one level down) are
///    read from it.
///
/// The structured-line recognition rules live ONLY here; every structured
/// consumer (`scan_final_structured`, the status determination,
/// `extract_final_response`, and `commands::result::final_error_text`) goes
/// through this single helper, so both shapes work unchanged.
pub(crate) fn structured_value(line: &str) -> Option<(String, serde_json::Value)> {
    let val = json_object_line(line)?;
    // Stream shape (checked first: it is the more specific of the two): an
    // `event:"result"` line whose `result` payload is an object with a string
    // `status`.
    if val.get("event").and_then(|e| e.as_str()) == Some("result") {
        let inner = val.get("result")?;
        let status = inner.get("status")?.as_str()?;
        return Some((status.to_string(), inner.clone()));
    }
    // Legacy shape: a top-level string `status` on the line object itself.
    let status = val.get("status")?.as_str()?;
    Some((status.to_string(), val))
}

/// True when `line` is a stream-json `step_update` event line
/// (`{"event":"step_update",...}`). Those lines carry the agent's own free
/// text (`text_delta`, tool output) and are excluded from the
/// quota/rate-limit scan (see `has_quota_exhaustion_signal`).
fn is_stream_step_update(line: &str) -> bool {
    let Some(val) = json_object_line(line) else {
        return false;
    };
    val.get("event").and_then(|e| e.as_str()) == Some("step_update")
}

/// Returns true when `log` contains agy's own exit message for terminating
/// still-pending background tasks (e.g. "terminating 1 background task(s) on
/// exit"). agy prints this when it deliberately ends a turn while background
/// work is still running, so the "SUCCESS" response of that turn is a "will
/// wait" promise rather than a final answer.
///
/// Matching is case-insensitive and line-based: a single log line must
/// contain "background task" (covers "task(s)" and "tasks"), a "terminat*"
/// verb form ("terminating", "terminated", "termination"), AND "on exit" --
/// this last anchor is required (not optional) because agy's structured
/// JSON response line is itself emitted as a single line, so a genuinely
/// completed turn whose own response text happens to mention both
/// "background task" and "terminat*" (e.g. describing work it just did)
/// would otherwise false-trigger this check and downgrade a real `Done` to
/// `Incomplete`. Requiring "on exit" too keeps the match anchored to agy's
/// own specific exit notice rather than arbitrary narrative text, while
/// still tolerating wording variance in the "background task(s)" phrasing
/// itself (count, plural, punctuation).
///
/// One further anchor, required because the JSON `response` field is free
/// text: a line that parses as a JSON object is never that notice -- agy
/// prints the notice as plain text, so a completed turn whose response merely
/// QUOTES the notice ("terminating 1 background task(s) on exit") inside a
/// JSON line must not false-trigger this check.
///
/// In stream-json mode the exclusion is broadened to EVERY JSON object line
/// (not only structured results): the agent's free text also appears in
/// `step_update` lines (`text_delta`, tool output), and those lines have NO
/// top-level `status` field. Skipping any `event` line keeps a quoted notice
/// inside ANY JSON line from triggering the signal; plain-text lines keep
/// matching exactly as before.
pub fn has_pending_background_task_signal(log: &str) -> bool {
    let lower = log.to_ascii_lowercase();
    lower.lines().any(|line| {
        json_object_line(line).is_none()
            && line.contains("background task")
            && line.contains("terminat")
            && line.contains("on exit")
    })
}

/// Pure logic to determine status from recorded exit code (if any) and log output.
///
/// Anchors status detection specifically to agy's structured JSON output
/// (`--output-format stream-json`, and legacy `--output-format json`) by
/// finding the last valid JSON result object with a recognized status field,
/// avoiding false positives from substring matches within the AI model's
/// generated text.
///
/// `JobStatus::Done` is reserved for a structured SUCCESS whose `response`
/// field carries non-empty text; a process that exits cleanly without that is
/// `JobStatus::Incomplete`. A background-task cutoff signal in the log forces
/// `JobStatus::Incomplete` even over such a SUCCESS: agy cut the turn off with
/// pending background work, so the response is a "will wait" promise, not a
/// final answer. A quota/rate-limit signal anywhere in the log is
/// `JobStatus::QuotaExhausted` (checked before the exit-code fallbacks, so a
/// quota job is classified as soon as it dies, not after the print timeout),
/// and it also wins over the background-task cutoff signal.
pub fn determine_status_from_signals(exit_code: Option<i32>, log: &str) -> JobStatus {
    // A retried job accumulates every attempt in one log. ALL signals here
    // (structured status, background-task cutoff, quota) describe the outcome
    // of the most recent attempt only: a notice/SUCCESS left behind by a
    // superseded attempt must not misclassify a later attempt (an old
    // pending-task notice would pin a job Incomplete forever, and a SUCCESS in
    // an earlier attempt would be re-reported once a later attempt crashes
    // without JSON).
    let current = last_attempt_log(log);
    let structured = scan_final_structured(current);
    let has_error = matches!(structured, Some((StructuredStatus::Error, _)));
    let success_complete = matches!(
        structured,
        Some((StructuredStatus::Success, ref response)) if !response.trim().is_empty()
    );
    let pending_bg_task = has_pending_background_task_signal(current);

    // A complete structured SUCCESS is the only thing that counts as Done --
    // unless agy cut the turn off with background tasks still pending, in
    // which case the non-empty response is a "will wait" promise, not a
    // final answer.
    if success_complete && !pending_bg_task {
        return JobStatus::Done;
    }

    // Quota/rate-limit exhaustion from the upstream provider is a distinct,
    // diagnosable terminal state -- and the more actionable of the two
    // "didn't finish" signals, so it wins over the background-task cutoff.
    //
    // Scanned only over the LAST attempt's text: a retried job accumulates
    // all attempts in one log, and a quota error from a superseded attempt
    // must not misclassify a later attempt that got further.
    if has_quota_exhaustion_signal(current) {
        return JobStatus::QuotaExhausted;
    }

    // agy terminated pending background tasks on exit: the work is not
    // actually done, so the job is Incomplete regardless of the recorded
    // exit code or structured status.
    if pending_bg_task {
        return JobStatus::Incomplete;
    }

    if let Some(code) = exit_code {
        if code == 0 {
            // Exited cleanly but without a complete SUCCESS.
            if has_error {
                JobStatus::Failed
            } else {
                JobStatus::Incomplete
            }
        } else {
            // Non-zero exit code
            // Check if killed abruptly by SIGKILL / SIGTERM without failure explanation
            if (code == 137 || code == 143) && !has_error {
                JobStatus::SelfTerminated
            } else {
                JobStatus::Failed
            }
        }
    } else {
        // No exit code recorded
        if has_error {
            JobStatus::Failed
        } else {
            JobStatus::SelfTerminated
        }
    }
}

/// A claude stream-json terminal outcome, read from a `result` line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeOutcome {
    /// A `result` line that is not an error (`subtype` != `"error"`, `is_error`
    /// false or absent): the claude job completed.
    Done,
    /// A `result` line with `subtype:"error"` / `is_error:true`, or a `system`
    /// `subtype:"error"` line: the claude job failed.
    Failed,
}

/// Recognizes a claude stream-json terminal line in `line` and returns its
/// outcome. The shapes mirror `engine/claude/stream_json.rs`:
/// - `{"type":"result","subtype":"success"|...,"is_error":false|absent,...}` -> Done
/// - `{"type":"result","subtype":"error","is_error":true,...}` -> Failed
/// - `{"type":"system","subtype":"error","message":"..."}` -> Failed
///
/// Any other line (a `stream_event`, an `assistant`/`user` message, an
/// unparseable blob) is `None`, so the scan keeps going.
fn claude_line_outcome(line: &str) -> Option<ClaudeOutcome> {
    let value = json_object_line(line)?;
    let ty = value.get("type").and_then(|t| t.as_str())?;
    match ty {
        "result" => {
            let is_err = value.get("subtype").and_then(|s| s.as_str()) == Some("error")
                || value.get("is_error") == Some(&serde_json::Value::Bool(true));
            Some(if is_err {
                ClaudeOutcome::Failed
            } else {
                ClaudeOutcome::Done
            })
        }
        "system" => {
            if value.get("subtype").and_then(|s| s.as_str()) == Some("error") {
                Some(ClaudeOutcome::Failed)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The last claude terminal outcome in `log` (scanning backwards), restricted
/// to the LAST attempt like the agy scan, so a stale `result` from a
/// superseded attempt never misclassifies a later attempt.
fn scan_claude_outcome(log: &str) -> Option<ClaudeOutcome> {
    for line in last_attempt_log(log).lines().rev() {
        if let Some(outcome) = claude_line_outcome(line) {
            return Some(outcome);
        }
    }
    None
}

/// Returns the `StructuredStatus` + text payload of a claude stream-json
/// terminal line, for the `result`/error surfacing path (`result` command):
/// a success `result` line yields its `result` text; an error `result` yields
/// its `error` text; a `system:error` line yields its `message`.
pub(crate) fn claude_structured_text(line: &str) -> Option<(StructuredStatus, String)> {
    let value = json_object_line(line)?;
    let ty = value.get("type").and_then(|t| t.as_str())?;
    match ty {
        "result" => {
            let is_err = value.get("subtype").and_then(|s| s.as_str()) == Some("error")
                || value.get("is_error") == Some(&serde_json::Value::Bool(true));
            // Defect 2 (HL-71 S1): an error `result` line carries its reason in
            // the `result` field (the live claude 2.1.322 error shape):
            //   {"type":"result","subtype":"error","is_error":true,
            //    "result":"Failed to authenticate: OAuth session expired ..."}
            // with no `error` field at all. Surface that text in preference to
            // the empty `error` field so `agent-run result` reports the reason.
            let text = if is_err {
                value
                    .get("result")
                    .and_then(|r| r.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .or_else(|| value.get("error").and_then(|e| e.as_str()))
                    .unwrap_or("")
            } else {
                value.get("result").and_then(|r| r.as_str()).unwrap_or("")
            }
            .to_string();
            Some(if is_err {
                (StructuredStatus::Error, text)
            } else {
                (StructuredStatus::Success, text)
            })
        }
        "system" => {
            if value.get("subtype").and_then(|s| s.as_str()) == Some("error") {
                let text = value
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("")
                    .to_string();
                Some((StructuredStatus::Error, text))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// True when `log` carries a claude stream-json shape: a `[claude-code:...]`
/// marker line or any `assistant`/`result`/`system` JSON event line. The
/// reason-fallback scan (_HL-71 S1_) only applies to such logs, so an agy
/// job's unstructured tail (agent chatter) can never be mistaken for an
/// error reason.
fn looks_like_claude_stream(log: &str) -> bool {
    log.lines().any(|line| {
        if line.trim_start().starts_with("[claude-code:") {
            return true;
        }
        json_object_line(line).is_some_and(|value| {
            matches!(
                value.get("type").and_then(|t| t.as_str()),
                Some("assistant" | "result" | "system")
            )
        })
    })
}

/// Returns the concatenated text content of the LAST synthetic `assistant`
/// message in `log` -- the claude message with `model:"<synthetic>"` that a
/// failing run emits to carry its reason text without a real model
/// attribution (e.g. "Failed to authenticate: OAuth session expired ...").
/// `None` when no such message carries any text.
fn last_synthetic_assistant_text(log: &str) -> Option<String> {
    for line in log.lines().rev() {
        let Some(value) = json_object_line(line) else {
            continue;
        };
        if value.get("type").and_then(|t| t.as_str()) != Some("assistant") {
            continue;
        }
        let message = value.get("message");
        if message
            .and_then(|m| m.get("model"))
            .and_then(|m| m.as_str())
            != Some("<synthetic>")
        {
            continue;
        }
        let content = message
            .and_then(|m| m.get("content"))
            .or_else(|| value.get("content"));
        let Some(blocks) = content.and_then(|c| c.as_array()) else {
            continue;
        };
        let text: String = blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
            .collect();
        if !text.trim().is_empty() {
            return Some(text);
        }
    }
    None
}

/// Returns the last line of `log` that is neither blank nor a JSON-object
/// event line -- the plain-text stderr tail of a failed run (e.g. a
/// `[claude-code:...]` marker or a raw error line).
fn last_non_json_line(log: &str) -> Option<String> {
    log.lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty() && json_object_line(l).is_none())
        .map(ToOwned::to_owned)
}

/// Defect 2 fallback chain (_HL-71 S1_): when a claude job failed but its log
/// carries NO terminal error `result` line (the `result` command's
/// `final_error_text` scan finds nothing), surface a reason instead of an
/// empty `error`:
///
/// 1. the text of the last synthetic `assistant` message
///    (`model:"<synthetic>"` -- claude's reason-carrying message), else
/// 2. the last non-JSON log line (the `[claude-code:...]` marker / stderr
///    tail).
///
/// Only applies to claude-shaped logs; returns `None` otherwise so agy and
/// quota-classified jobs are untouched.
pub(crate) fn claude_fallback_error_text(log: &str) -> Option<String> {
    let last_attempt = last_attempt_log(log);
    if !looks_like_claude_stream(last_attempt) {
        return None;
    }
    if let Some(text) = last_synthetic_assistant_text(last_attempt) {
        if !text.trim().is_empty() {
            return Some(text);
        }
    }
    last_non_json_line(last_attempt)
}

/// Status classification for a claude stream-json job log (HL-91 E5).
///
/// Rules, mirroring the agy taxonomy on the claude contract:
/// - A terminal `result` success (the claude analogue of a complete agy
///   SUCCESS) is `Done`. claude exits 0 on a successful `result`, so the
///   outcome carries the same weight as agy's structured SUCCESS.
/// - A `result`/`system` error line is `Failed` -- but a 137/143 kill without
///   an error line stays `SelfTerminated` (the external-kill taxonomy).
/// - A quota/rate-limit exhaustion signal anywhere in the log is
///   `QuotaExhausted` (claude surfaces upstream rejection in its error text,
///   including on `system`/`result` lines, which the generic scan matches).
/// - Exit 0 with no terminal success line is `Incomplete` (the harness
///   finished without reporting a result).
/// - Non-zero exit with no error line is `Failed` (137/143 unless the log
///   shows an error, which the killed-job paths write explicitly).
/// - No exit code with no error line is `SelfTerminated` (the row was cut
///   short without an outcome, e.g. an orphan of a dead server).
pub fn determine_status_from_claude(exit_code: Option<i32>, log: &str) -> JobStatus {
    let outcome = scan_claude_outcome(log);
    let has_error = outcome == Some(ClaudeOutcome::Failed);
    let done = outcome == Some(ClaudeOutcome::Done);

    if done {
        return JobStatus::Done;
    }

    // Quota/rate-limit exhaustion (same markers and last-attempt scoping as
    // the agy scan; claude emits the upstream error text on its error lines,
    // which the marker scan catches).
    if has_quota_exhaustion_signal(last_attempt_log(log)) {
        return JobStatus::QuotaExhausted;
    }

    match exit_code {
        Some(0) => {
            // Clean exit but no terminal success result.
            if has_error {
                JobStatus::Failed
            } else {
                JobStatus::Incomplete
            }
        }
        Some(code) => {
            // Non-zero exit: kill signals (137/143) without an error line are
            // SelfTerminated, anything else Failed.
            if (code == 137 || code == 143) && !has_error {
                JobStatus::SelfTerminated
            } else {
                JobStatus::Failed
            }
        }
        None => {
            if has_error {
                JobStatus::Failed
            } else {
                JobStatus::SelfTerminated
            }
        }
    }
}

/// Strips ANSI escape sequences (CSI codes like `\x1b[32m`, OSC codes, etc.) from `input`.
pub fn strip_ansi_escapes(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            match chars.peek() {
                Some('[') => {
                    chars.next(); // consume '['
                                  // Parameters / intermediate bytes: 0x20-0x3F (e.g. '0'-'9', ';', '?', ' ')
                    while let Some(&p) = chars.peek() {
                        if (0x20..=0x3F).contains(&(p as u32)) {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    // Final byte: 0x40-0x7E (e.g. 'm', 'H', 'J', etc.)
                    if let Some(&f) = chars.peek() {
                        if (0x40..=0x7E).contains(&(f as u32)) {
                            chars.next();
                        }
                    }
                }
                Some(']') => {
                    chars.next(); // consume ']'
                                  // OSC sequence: consume until BEL (\x07) or ST (\x1b\)
                    while let Some(osc_c) = chars.next() {
                        if osc_c == '\x07' {
                            break;
                        }
                        if osc_c == '\x1b' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                Some('(') | Some(')') => {
                    chars.next();
                    chars.next(); // consume charset designator
                }
                _ => {}
            }
        } else {
            output.push(c);
        }
    }
    output
}

/// Searches for a 36-character UUID token (8-4-4-4-12 hex digits with hyphens) in `line`.
pub fn find_uuid_in_line(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    if bytes.len() < 36 {
        return None;
    }
    for i in 0..=bytes.len() - 36 {
        // Ensure word boundary before the candidate: not alphanumeric or hyphen
        if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'-') {
            continue;
        }
        // Ensure word boundary after the candidate: not alphanumeric or hyphen
        if i + 36 < bytes.len() && (bytes[i + 36].is_ascii_alphanumeric() || bytes[i + 36] == b'-')
        {
            continue;
        }
        if bytes[i + 8] == b'-'
            && bytes[i + 13] == b'-'
            && bytes[i + 18] == b'-'
            && bytes[i + 23] == b'-'
        {
            // Byte-aligned windows do not guarantee a char-boundary slice
            // when the line contains multibyte characters; `line.get`
            // validates the range and rejects it instead of panicking.
            let Some(candidate) = line.get(i..i + 36) else {
                continue;
            };
            let is_valid_uuid = candidate.char_indices().all(|(idx, c)| {
                if idx == 8 || idx == 13 || idx == 18 || idx == 23 {
                    c == '-'
                } else {
                    c.is_ascii_hexdigit()
                }
            });
            if is_valid_uuid {
                return Some(candidate.to_string());
            }
        }
    }
    None
}

/// Attempts to parse an agy conversation ID from the job log.
pub fn extract_conversation_id_from_log(log_path: &Path) -> Option<String> {
    if !log_path.exists() {
        return None;
    }
    // The log is a raw capture of agy's stdout/stderr and is not guaranteed to
    // be valid UTF-8: a single stray byte must not make the whole read fail
    // and the conversation id go uncaptured (compute_status already decodes
    // the same file lossily).
    let bytes = fs::read(log_path).ok()?;
    let content = String::from_utf8_lossy(&bytes);
    extract_conversation_id(&content)
}

/// Extracts conversation ID using regex-like substring patterns and robust UUID scanning.
pub fn extract_conversation_id(log: &str) -> Option<String> {
    for raw_line in log.lines() {
        let clean_line = strip_ansi_escapes(raw_line);
        let line = clean_line.as_str();

        // Pattern 0: the stream-json `init` event carries the conversation_id
        // as a JSON field at the very start of the run (first line of the
        // log), so pick it up here before the pattern 1-4 fallbacks. The
        // first (i.e. lowest) line in the log holding it wins, and the
        // remaining patterns still run as before on every line, so all
        // existing priority/fallback rules are unchanged.
        if let Some(val) = json_object_line(line) {
            if val.get("event").and_then(|e| e.as_str()) == Some("init") {
                if let Some(id) = val.get("conversation_id").and_then(|c| c.as_str()) {
                    if !id.is_empty() {
                        return Some(id.to_string());
                    }
                }
            }
        }

        // Pattern 1: Conversation ID: <id>
        if let Some(pos) = line.find("Conversation ID:") {
            let rest = line[pos + "Conversation ID:".len()..].trim();
            let token = rest.split_whitespace().next().unwrap_or("");
            let clean = token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
            if !clean.is_empty() {
                return Some(clean.to_string());
            }
        }

        // Pattern 2: "conversation_id": "<id>" or "conversationId": "<id>"
        for key in &["\"conversation_id\"", "\"conversationId\""] {
            if let Some(pos) = line.find(key) {
                let rest = line[pos + key.len()..].trim_start();
                if let Some(after_colon) = rest.strip_prefix(':') {
                    let rest = after_colon.trim_start();
                    if let Some(after_quote) = rest.strip_prefix('"') {
                        if let Some(end) = after_quote.find('"') {
                            let id = &after_quote[..end];
                            if !id.is_empty() {
                                return Some(id.to_string());
                            }
                        }
                    }
                }
            }
        }

        // Pattern 2b: claude stream-json carries the harness session id on
        // `"session_id"` (assistant/result lines). This is the resume key for
        // a claude job, so it is captured exactly like an agy conversation id:
        // `retry` / resume reuse the same `conversation_id` field.
        if let Some(pos) = line.find("\"session_id\"") {
            let rest = line[pos + "\"session_id\"".len()..].trim_start();
            if let Some(after_colon) = rest.strip_prefix(':') {
                let rest = after_colon.trim_start();
                if let Some(after_quote) = rest.strip_prefix('"') {
                    if let Some(end) = after_quote.find('"') {
                        let id = &after_quote[..end];
                        if !id.is_empty() {
                            return Some(id.to_string());
                        }
                    }
                }
            }
        }

        // Pattern 3: Early agy notice emitted before first turn completes:
        // "Forwarding user message to conversation <id>"
        // Real agy format: "Forwarding user message to conversation %s (items=%d, media=%d)"
        if let Some(pos) = line.find("Forwarding user message to conversation ") {
            let rest = line[pos + "Forwarding user message to conversation ".len()..].trim();
            let token = rest.split_whitespace().next().unwrap_or("");
            let clean = token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
            if !clean.is_empty() {
                return Some(clean.to_string());
            }
        }

        // Pattern 4: Robust fallback: any UUID-v4-shaped token appearing anywhere on a line
        // containing "conversation" case-insensitively.
        if line.to_ascii_lowercase().contains("conversation") {
            if let Some(uuid) = find_uuid_in_line(line) {
                return Some(uuid);
            }
        }
    }
    None
}

/// Returns all job records from the state database, optionally filtered by
/// project name, sorted newest first.
pub fn list_jobs(state_root: &Path, project_filter: Option<&str>) -> Result<Vec<JobMetadata>> {
    let conn = open_state_db(state_root)?;
    let mut stmt = conn
        .prepare(
            "SELECT job_id, project, prompt, resolved_prompt, model, effort, sandbox, mode, add_dirs, print_timeout, created_at, worktree_path, branch, base_ref, conversation_id, retry_count, pid, exit_code, status, hindsight, auto_recall, auto_retain, retry_prompt, read_only, external_worktree, pid_start_time, hindsight_bank, proxy, max_runtime, started_at, hindsight_query, sandbox_info, engine \
              FROM jobs WHERE ?1 IS NULL OR project = ?1 ORDER BY created_at DESC, job_id DESC",
        )
        .with_context(|| format!("Failed to prepare job list query in {}", state_db_path(state_root).display()))?;

    let mut results = Vec::new();
    let rows = stmt
        .query_map(params![project_filter], row_to_job)
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to query jobs from {}: {}",
                state_db_path(state_root).display(),
                e
            )
        })?;
    for row in rows {
        results.push(row?);
    }
    Ok(results)
}

/// The effective max-runtime limit for a job: the per-job value wins over
/// the server-wide default; when neither is set there is NO limit (both
/// default to `None` -- a limit only ever exists when explicitly set).
pub fn effective_max_runtime(
    per_job: Option<Duration>,
    server_default: Option<Duration>,
) -> Option<Duration> {
    per_job.or(server_default)
}

/// The absolute deadline at which a job exceeds its max runtime: the
/// attempt's recorded start time plus the effective limit. `None` when the
/// job has no limit or no recorded start time (rows without a start time
/// never expire).
pub fn max_runtime_deadline(
    started_at: Option<DateTime<Utc>>,
    limit: Option<Duration>,
) -> Option<DateTime<Utc>> {
    Some(started_at? + limit?)
}

/// `true` when the job's max runtime has been exceeded: it has a limit, a
/// recorded start time, and the deadline is now in the past.
pub fn max_runtime_exceeded(
    started_at: Option<DateTime<Utc>>,
    limit: Option<Duration>,
    now: DateTime<Utc>,
) -> bool {
    match max_runtime_deadline(started_at, limit) {
        Some(deadline) => now >= deadline,
        None => false,
    }
}

/// Pure enforcement DECISION: given a job's current status, its attempt start
/// anchor, its max-runtime limit, and the current time, SHOULD the server
/// terminate it for exceeding its max runtime?
///
/// `true` only when ALL of the following hold:
///   * the job is currently `Running` (terminal/other states are never
///     terminated here -- they already have a final status),
///   * the job HAS a limit (`None` limit = unbounded = never, by definition),
///   * the deadline (`started_at + limit`) is at or behind `now`.
///
/// Intentionally pure (no I/O, no DB): the periodic server scan
/// ([`crate::core::max_runtime::enforce_once`]) calls this to decide, then performs
/// the actual termination through the shared `kill` path. Unit-tested in
/// isolation because it is the whole "should we kill?" policy.
pub fn max_runtime_kill_due(
    status: JobStatus,
    started_at: Option<DateTime<Utc>>,
    limit: Option<Duration>,
    now: DateTime<Utc>,
) -> bool {
    if status != JobStatus::Running {
        return false;
    }
    max_runtime_exceeded(started_at, limit, now)
}

/// Time remaining until the job's max runtime deadline; `None` when the job
/// has no deadline (no limit, no start time) or it has already passed.
pub fn max_runtime_remaining(
    started_at: Option<DateTime<Utc>>,
    limit: Option<Duration>,
    now: DateTime<Utc>,
) -> Option<Duration> {
    let deadline = max_runtime_deadline(started_at, limit)?;
    let remaining = deadline - now;
    // `remaining <= 0` (not yet started relative to now, or already past the
    // deadline) yields `None`; any positive remainder is returned.
    if remaining.num_nanoseconds().map(|n| n > 0).unwrap_or(false) {
        Some(remaining)
    } else {
        None
    }
}

/// Human form of a max-runtime duration for `status`/`start` output
/// (`30s`, `5m 10s`, `2h 5m`; days for >= 24h).
pub fn format_max_runtime(d: &Duration) -> String {
    let total = d.num_seconds().max(0);
    if total < 60 {
        format!("{total}s")
    } else if total < 3600 {
        format!("{}m {}s", total / 60, total % 60)
    } else if total < 86400 {
        format!("{}h {}m", total / 3600, (total % 3600) / 60)
    } else {
        format!("{}d {}h", total / 86400, (total % 86400) / 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_job_id_uniqueness_and_format() {
        let count = 100; // 32-bit ids: 1000 draws collide in ~1 of 8000 runs (birthday); production retries via job_id_exists
        let mut ids = HashSet::with_capacity(count);

        for _ in 0..count {
            let id = generate_job_id();
            assert_eq!(id.len(), 8, "Job ID must be exactly 8 characters long");
            assert!(
                id.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "Job ID must be lowercase hex characters: {}",
                id
            );
            ids.insert(id);
        }

        assert_eq!(
            ids.len(),
            count,
            "All generated job IDs must be unique (no collisions)"
        );
    }

    #[test]
    fn test_status_determination_signals() {
        // Case 1: Exit code 0 with structured SUCCESS line -> Done
        let status = determine_status_from_signals(
            Some(0),
            r#"{"conversation_id":"c1","status":"SUCCESS","response":"All tests passed."}"#,
        );
        assert_eq!(status, JobStatus::Done);

        // Case 2: Exit code 1 with structured ERROR line -> Failed
        let status = determine_status_from_signals(
            Some(1),
            r#"{"conversation_id":"c1","status":"ERROR","error":"file not found"}"#,
        );
        assert_eq!(status, JobStatus::Failed);

        // Case 3: Exit code 0 but empty log -> Incomplete (exited cleanly
        // without ever producing a structured SUCCESS with response text)
        let status = determine_status_from_signals(Some(0), "");
        assert_eq!(status, JobStatus::Incomplete);

        // Case 4: Process disappeared without exit code and no structured status -> SelfTerminated
        let status = determine_status_from_signals(None, "Starting tool execution...");
        assert_eq!(status, JobStatus::SelfTerminated);

        // Case 5: Process disappeared without exit code but structured SUCCESS line present -> Done
        let status = determine_status_from_signals(
            None,
            r#"{"conversation_id":"c1","status":"SUCCESS","response":"File saved."}"#,
        );
        assert_eq!(status, JobStatus::Done);

        // Case 6: Process disappeared without exit code but structured ERROR line present -> Failed
        let status = determine_status_from_signals(
            None,
            r#"{"conversation_id":"c1","status":"ERROR","error":"git connection refused"}"#,
        );
        assert_eq!(status, JobStatus::Failed);

        // Case 7: Signal 137 (SIGKILL) with no error explanation -> SelfTerminated
        let status = determine_status_from_signals(Some(137), "Running background build...");
        assert_eq!(status, JobStatus::SelfTerminated);

        // Case 8: Exit code 0, non-empty log with progress narration but no structured status line -> Incomplete
        let progress_log = "Running tests with go test -race ./...\nRunning background task...\nChecking compiler output...";
        let status = determine_status_from_signals(Some(0), progress_log);
        assert_eq!(status, JobStatus::Incomplete);
    }
    #[test]
    fn test_status_incomplete_and_quota_exhausted() {
        // SUCCESS with empty response, exit 0 -> Incomplete (NOT Done)
        let empty_response = r#"{"conversation_id":"c1","status":"SUCCESS","response":""}"#;
        assert_eq!(
            determine_status_from_signals(Some(0), empty_response),
            JobStatus::Incomplete,
            "SUCCESS with empty response must not count as Done"
        );
        // Whitespace-only response -> Incomplete
        let ws_response = r#"{"status":"SUCCESS","response":"  \n"}"#;
        assert_eq!(
            determine_status_from_signals(Some(0), ws_response),
            JobStatus::Incomplete
        );
        // SUCCESS with empty response, no exit code recorded -> SelfTerminated
        // (process outcome unknown; not a clean exit)
        assert_eq!(
            determine_status_from_signals(None, empty_response),
            JobStatus::SelfTerminated
        );

        // Quota signals -> QuotaExhausted regardless of exit code or shape
        let quota_grpc = "upstream error: grpc: RESOURCE_EXHAUSTED: monthly spend limit exceeded";
        assert_eq!(
            determine_status_from_signals(Some(1), quota_grpc),
            JobStatus::QuotaExhausted
        );
        let quota_error_json =
            r#"{"status":"ERROR","error":"429 Too Many Requests: rate limit exceeded"}"#;
        assert_eq!(
            determine_status_from_signals(Some(0), quota_error_json),
            JobStatus::QuotaExhausted,
            "quota signal in an ERROR line must be classified as QuotaExhausted, not Failed"
        );
        assert_eq!(
            determine_status_from_signals(Some(137), "request rejected: quota exceeded"),
            JobStatus::QuotaExhausted,
            "quota signal must beat the 137/143 SelfTerminated fallback"
        );

        // A complete SUCCESS with response text wins over quota text elsewhere
        // in the log (e.g. quota error hit earlier in the run, retried fine).
        let recovered =
            "upstream: rate limit exceeded, retrying...\n{\"status\":\"SUCCESS\",\"response\":\"done ok\"}";
        assert_eq!(
            determine_status_from_signals(Some(0), recovered),
            JobStatus::Done
        );

        // has_quota_exhaustion_signal unit behavior
        assert!(has_quota_exhaustion_signal(
            "got grpc.status=RESOURCE_EXHAUSTED back"
        ));
        assert!(has_quota_exhaustion_signal(
            "You have exceeded your quota for this month."
        ));
        assert!(!has_quota_exhaustion_signal(
            "All tests passed. No problems found."
        ));
    }

    #[test]
    fn test_status_pending_background_task_override() {
        // Regression: a real complete SUCCESS with a substantive response and
        // no background-task marker must still be Done.
        let complete_success = r#"
Running integration tests...
{"conversation_id":"conv-789","status":"SUCCESS","response":"All tests passed, changes committed to the branch.","duration_seconds":12.34,"num_turns":3,"usage":{"input_tokens":50000,"output_tokens":1200,"total_tokens":51200}}
"#;
        assert_eq!(
            determine_status_from_signals(Some(0), complete_success),
            JobStatus::Done,
            "complete SUCCESS without the background-task marker must still be Done"
        );
        assert_eq!(
            determine_status_from_signals(None, complete_success),
            JobStatus::Done,
            "complete SUCCESS without the background-task marker must still be Done (None exit code)"
        );

        // agy cut the turn off with a background task still pending: the
        // non-empty "will wait" response must NOT count as Done.
        let bg_task_log = r#"
Kicking off the long build in the background...
terminating 1 background task(s) on exit
{"conversation_id":"conv-789","status":"SUCCESS","response":"The build is running in the background, I will wait for it and report the result.","duration_seconds":8.1,"num_turns":2,"usage":{"input_tokens":40000,"output_tokens":800,"total_tokens":40800}}
"#;
        assert_eq!(
            determine_status_from_signals(Some(0), bg_task_log),
            JobStatus::Incomplete,
            "SUCCESS with a 'will wait' response plus the background-task cutoff signal must be Incomplete, not Done"
        );
        assert_eq!(
            determine_status_from_signals(None, bg_task_log),
            JobStatus::Incomplete,
            "background-task cutoff signal must force Incomplete even without a recorded exit code"
        );

        // Marker unit behavior: wording variance, and no false positives.
        assert!(has_pending_background_task_signal(
            "terminating 1 background task(s) on exit"
        ));
        assert!(has_pending_background_task_signal(
            "Terminating 3 background tasks on exit"
        ));
        assert!(has_pending_background_task_signal(
            "terminated background task on exit"
        ));
        assert!(!has_pending_background_task_signal(
            "Running background task..."
        ));
        assert!(!has_pending_background_task_signal(
            "terminating the docker daemon gracefully\nRunning background task for the build..."
        ));
        // "on exit" must be required, not optional: a genuinely completed
        // turn whose own response text happens to mention both other words
        // (describing work it just did, not agy's own exit notice) must not
        // false-trigger. Single-line because the structured JSON response is
        // itself emitted as one line -- exactly where this false positive
        // would actually occur.
        assert!(!has_pending_background_task_signal(
            r#"{"status":"SUCCESS","response":"Terminating background tasks is now handled cleanly."}"#
        ));

        // Quota exhaustion is the more actionable signal and wins when both
        // are present.
        let quota_bg_log =
            "upstream: rate limit exceeded\nterminating 1 background task(s) on exit\n{\"status\":\"SUCCESS\",\"response\":\"I will wait for the retry\"}";
        assert_eq!(
            determine_status_from_signals(Some(0), quota_bg_log),
            JobStatus::QuotaExhausted,
            "quota signal must win over the background-task cutoff signal"
        );
    }

    #[test]
    fn test_status_background_task_notice_quoted_in_structured_json() {
        // Regression (false positive 2026-09-30): a job that finished
        // normally with a complete structured SUCCESS was demoted to
        // Incomplete because the agy notice "terminating N background
        // task(s) on exit" appeared only as a QUOTE inside the `response`
        // text of the structured JSON result line -- the review answer was
        // explaining that bug. The notice match must be anchored to agy's
        // own plain-text exit notice, not narrative text (even text
        // embedded inside a JSON line).
        let quoted_in_success = r#"
Reviewing the agent's logs...
{"conversation_id":"conv-fp260930","status":"SUCCESS","response":"Fixed the job status classification. The notice (\"terminating 1 background task(s) on exit\") was matched even when the agent's own answer quoted it while explaining the bug; the fix anchors the match to agy's plain-text exit notice, so this completed run is now classified Done.","duration_seconds":74.2,"num_turns":9,"usage":{"input_tokens":120000,"output_tokens":3400,"total_tokens":123400}}
"#;
        assert!(
            !has_pending_background_task_signal(quoted_in_success),
            "the notice quoted inside the structured SUCCESS response must not false-trigger"
        );
        assert_eq!(
            determine_status_from_signals(Some(0), quoted_in_success),
            JobStatus::Done,
            "complete SUCCESS whose response QUOTES the notice must be Done, not Incomplete"
        );
        assert_eq!(
            determine_status_from_signals(None, quoted_in_success),
            JobStatus::Done,
            "complete SUCCESS whose response QUOTES the notice must be Done (None exit code)"
        );

        // But agy's own plain-text notice is still the cutoff signal: notice
        // line followed by a SUCCESS JSON line must remain Incomplete.
        let notice_then_success = "terminating 1 background task(s) on exit\n{\"conversation_id\":\"conv-789\",\"status\":\"SUCCESS\",\"response\":\"The build is running in the background, I will wait for it and report the result.\"}";
        assert!(has_pending_background_task_signal(notice_then_success));
        assert_eq!(
            determine_status_from_signals(Some(0), notice_then_success),
            JobStatus::Incomplete,
            "agy's own plain-text notice must still force Incomplete over the SUCCESS"
        );

        // And a quote inside an ERROR JSON line must not flip a Failed job
        // into Incomplete.
        let quoted_in_error = "{\"conversation_id\":\"conv-err\",\"status\":\"ERROR\",\"error\":\"panic: the log said (\\\"terminating 1 background task(s) on exit\\\") before the process died\"}";
        assert!(
            !has_pending_background_task_signal(quoted_in_error),
            "the notice quoted inside the structured ERROR response must not false-trigger"
        );
        assert_eq!(
            determine_status_from_signals(Some(1), quoted_in_error),
            JobStatus::Failed,
            "ERROR json with a quoted notice must be Failed, not Incomplete"
        );
    }

    #[test]
    fn test_extract_final_response() {
        let log = format!(
            "line one\n{}\nnoise\n{}",
            r#"{"status":"SUCCESS","response":"first answer"}"#,
            r#"{"conversation_id":"c2","status":"SUCCESS","response":"second answer"}"#
        );
        assert_eq!(
            extract_final_response(&log).as_deref(),
            Some("second answer")
        );
        // Missing response field -> empty string (line is still recognized)
        assert_eq!(
            extract_final_response(r#"{"status":"SUCCESS"}"#).as_deref(),
            Some("")
        );
        // No recognized line -> None
        assert_eq!(extract_final_response("no json here"), None);
        // Final recognized line is ERROR -> its (absent) response, not the earlier success
        let log2 = r#"{"status":"SUCCESS","response":"ok"}
{"status":"ERROR","error":"boom"}"#;
        assert_eq!(extract_final_response(log2).as_deref(), Some(""));
        // Unrecognized status values are skipped
        assert_eq!(
            extract_final_response(
                r#"{"status":"processing"}
{"status":"SUCCESS","response":"real"}"#
            )
            .as_deref(),
            Some("real")
        );
    }

    #[test]
    fn test_extract_final_response_error_carries_response() {
        // The terminal ERROR line carries a complete `response`: it must be
        // surfaced, not discarded (the incident behind fixing BUG 1).
        let log = r##"{"conversation_id":"c1","status":"ERROR","response":"# Research Report\nfull body","error":"API error (attempt 1): UNAVAILABLE (code 503): The service is currently unavailable.","duration_seconds":397.8,"num_turns":1}
"##;
        assert_eq!(
            extract_final_response(log).as_deref(),
            Some("# Research Report\nfull body")
        );

        // A later SUCCESS line is superseded by a terminal ERROR line: the
        // final recognized line is the ERROR one, and its response wins.
        let log2 = r#"{"status":"SUCCESS","response":"earlier ok"}
{"status":"ERROR","response":"captured before the 5xx","error":"503 unavailable"}"#;
        assert_eq!(
            extract_final_response(log2).as_deref(),
            Some("captured before the 5xx")
        );

        // Empty response field on the ERROR line -> empty string (field
        // present but blank), still a recognized line.
        assert_eq!(
            extract_final_response(r#"{"status":"ERROR","response":"","error":"boom"}"#).as_deref(),
            Some("")
        );

        // Missing response field on the ERROR line -> empty string as well.
        assert_eq!(
            extract_final_response(r#"{"status":"ERROR","error":"boom"}"#).as_deref(),
            Some("")
        );
    }

    #[test]
    fn test_status_still_failed_when_error_carries_response() {
        // Carrying a complete response on a terminal ERROR line must NOT
        // promote the job to Done: the status variant is still `Failed`
        // regardless of exit code.
        let err_with_response = r#"{"conversation_id":"c1","status":"ERROR","response":"full body","error":"503 unavailable"}"#;
        assert_eq!(
            determine_status_from_signals(Some(0), err_with_response),
            JobStatus::Failed,
            "ERROR with a complete response is still Failed (exit 0)"
        );
        assert_eq!(
            determine_status_from_signals(Some(1), err_with_response),
            JobStatus::Failed,
            "ERROR with a complete response is still Failed (exit 1)"
        );
        assert_eq!(
            determine_status_from_signals(None, err_with_response),
            JobStatus::Failed,
            "ERROR with a complete response is still Failed (no exit code)"
        );
    }

    #[test]
    fn test_determine_status_no_misclassification_from_model_narrative() {
        // 1. Model text contains "Error: ..." or "failed to ..." in its analysis,
        // but the structured status is SUCCESS.
        let code_review_success = r#"
Reviewing codebase...
Line 42: Error: potential null pointer dereference found in function parse_config().
Build failed to link with libssl.
{"conversation_id":"conv-123","status":"SUCCESS","response":"Code review complete. Found 1 issue.","duration_seconds":2.5,"num_turns":1}
"#;
        assert_eq!(
            determine_status_from_signals(Some(0), code_review_success),
            JobStatus::Done,
            "Log with model output containing 'Error:' must NOT be misclassified as Failed when structured status is SUCCESS"
        );
        assert_eq!(
            determine_status_from_signals(None, code_review_success),
            JobStatus::Done,
            "Log with model output containing 'Error:' must NOT be misclassified as Failed when structured status is SUCCESS (None exit code)"
        );

        // 2. Model text contains "completed successfully" in narrative,
        // but the structured status is ERROR.
        let narrative_with_failure = r#"
Starting deployment...
Step 1 completed successfully.
Step 2 completed successfully.
{"conversation_id":"conv-456","status":"ERROR","error":"Deployment failed at step 3","duration_seconds":5.0,"num_turns":2}
"#;
        assert_eq!(
            determine_status_from_signals(Some(1), narrative_with_failure),
            JobStatus::Failed,
            "Log with model output containing 'completed successfully' must NOT be misclassified as Done when structured status is ERROR"
        );
        assert_eq!(
            determine_status_from_signals(None, narrative_with_failure),
            JobStatus::Failed,
            "Log with model output containing 'completed successfully' must NOT be misclassified as Done when structured status is ERROR (None exit code)"
        );

        // 3. Raw unstructured text with "Error:" or "completed successfully" without any structured JSON
        // should NOT be treated as structured Done or Failed.
        let unstructured_text =
            "I analyzed the repo and completed successfully without any problem.";
        assert_eq!(
            determine_status_from_signals(Some(0), unstructured_text),
            JobStatus::Incomplete,
            "Exit 0 with unstructured text should be Incomplete (clean exit, no complete SUCCESS)"
        );
        // With no exit code recorded the process outcome is unknown -> SelfTerminated
        assert_eq!(
            determine_status_from_signals(None, unstructured_text),
            JobStatus::SelfTerminated
        );
    }

    #[test]
    fn test_extract_conversation_id() {
        let log_sample = r#"
2026-09-23T10:00:00Z INFO agy: starting
Conversation ID: 29376801-94e5-48b6-9c02-f7a10b6d32b1
{"status":"ok"}
"#;
        assert_eq!(
            extract_conversation_id(log_sample),
            Some("29376801-94e5-48b6-9c02-f7a10b6d32b1".to_string())
        );

        let json_sample = r#"{"type":"init","conversation_id":"conv-abc-123","agent":"test"}"#;
        assert_eq!(
            extract_conversation_id(json_sample),
            Some("conv-abc-123".to_string())
        );
    }

    #[test]
    fn test_extract_conversation_id_real_agy_json_output() {
        // Real success JSON output from agy
        let success_json = r#"{"conversation_id":"9fa0bf8a-807e-4cb8-838a-f52a724e532c","status":"SUCCESS","response":"Hello there, friend!\n","duration_seconds":3.6133411730000002,"num_turns":1,"usage":{"input_tokens":11849,"output_tokens":219,"thinking_tokens":214,"cache_read_tokens":0,"total_tokens":12068}}"#;
        assert_eq!(
            extract_conversation_id(success_json),
            Some("9fa0bf8a-807e-4cb8-838a-f52a724e532c".to_string()),
            "Success line must extract the exact conversation ID"
        );

        // Real error JSON output from agy with empty conversation_id
        let error_json = r#"{"conversation_id":"","status":"ERROR","response":"","error":"authentication failed or timed out","duration_seconds":0,"num_turns":0,"usage":{"input_tokens":0,"output_tokens":0,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":0}}"#;
        assert_eq!(
            extract_conversation_id(error_json),
            None,
            "Error line with empty conversation_id must return None"
        );
    }

    #[test]
    fn test_extract_conversation_id_early_forwarding_line() {
        // Job killed before first turn completes produces only the early line:
        // no "Conversation ID:" and no JSON status output.
        let early_log =
            "Forwarding user message to conversation 8900031f-fe31-4dea-95e9-0f0f89ba8929\n";
        assert_eq!(
            extract_conversation_id(early_log),
            Some("8900031f-fe31-4dea-95e9-0f0f89ba8929".to_string()),
            "Early forwarding line must extract the exact conversation ID"
        );

        // Also handles variations with timestamp, brackets, punctuation, or 'conversation:'
        let early_log_prefixed = "2026-09-25T04:20:00Z INFO [agy] Forwarding user message to conversation 29376801-94e5-48b6-9c02-f7a10b6d32b1...\nProcess terminated with signal SIGTERM";
        assert_eq!(
            extract_conversation_id(early_log_prefixed),
            Some("29376801-94e5-48b6-9c02-f7a10b6d32b1".to_string()),
            "Prefixed early forwarding line with trailing dots must extract clean ID"
        );
    }

    #[test]
    fn test_strip_ansi_escapes() {
        assert_eq!(strip_ansi_escapes("plain text"), "plain text");
        assert_eq!(
            strip_ansi_escapes("\x1b[32mgreen text\x1b[0m"),
            "green text"
        );
        assert_eq!(
            strip_ansi_escapes("\x1b[1;31;42mbold red on green\x1b[0m"),
            "bold red on green"
        );
        assert_eq!(strip_ansi_escapes("\x1b[2K\x1b[1Acleared"), "cleared");
        assert_eq!(
            strip_ansi_escapes("\x1b]0;Title\x07window title"),
            "window title"
        );
    }

    #[test]
    fn test_extract_conversation_id_real_agy_wording_and_ansi() {
        // Real literal format string and line from agy binary / cli.log:
        // "Forwarding user message to conversation %s (items=%d, media=%d)"
        let real_line = "I0923 12:01:03.585938       1 conversation_manager.go:699] Forwarding user message to conversation 9b9e1255-014d-4eff-9db9-a2979b17e900 (items=1, media=0)\n";
        assert_eq!(
            extract_conversation_id(real_line),
            Some("9b9e1255-014d-4eff-9db9-a2979b17e900".to_string()),
            "Real agy log line format must extract conversation UUID"
        );

        // Line with ANSI color codes wrapping the phrase and conversation ID
        let ansi_wrapped = "\x1b[32mI0923 12:01:03.585938 1 conversation_manager.go:699]\x1b[0m \x1b[1mForwarding user message to conversation\x1b[0m \x1b[36m9b9e1255-014d-4eff-9db9-a2979b17e900\x1b[0m (items=1, media=0)\n";
        assert_eq!(
            extract_conversation_id(ansi_wrapped),
            Some("9b9e1255-014d-4eff-9db9-a2979b17e900".to_string()),
            "ANSI escape codes must be stripped before matching"
        );

        // Different phrasing: "Forwarding request to conversation"
        let request_phrasing = "2026-09-25T10:00:00Z Forwarding request to conversation 9b9e1255-014d-4eff-9db9-a2979b17e900";
        assert_eq!(
            extract_conversation_id(request_phrasing),
            Some("9b9e1255-014d-4eff-9db9-a2979b17e900".to_string()),
            "Broadened match must handle 'Forwarding request to conversation'"
        );

        // Different phrasing: "Forwarding message to conversation"
        let message_phrasing =
            "Forwarding message to conversation 9b9e1255-014d-4eff-9db9-a2979b17e900";
        assert_eq!(
            extract_conversation_id(message_phrasing),
            Some("9b9e1255-014d-4eff-9db9-a2979b17e900".to_string()),
            "Broadened match must handle 'Forwarding message to conversation'"
        );

        // Print mode log line from session.go:
        let print_mode_log = "session.go:177] Print mode: conversation=9b9e1255-014d-4eff-9db9-a2979b17e900, sending message";
        assert_eq!(
            extract_conversation_id(print_mode_log),
            Some("9b9e1255-014d-4eff-9db9-a2979b17e900".to_string()),
            "Print mode conversation assignment must extract UUID"
        );

        // Server created conversation line:
        let created_conv_log =
            "server.go:1239] Created conversation 9b9e1255-014d-4eff-9db9-a2979b17e900";
        assert_eq!(
            extract_conversation_id(created_conv_log),
            Some("9b9e1255-014d-4eff-9db9-a2979b17e900".to_string()),
            "Created conversation log line must extract UUID"
        );
    }

    #[test]
    fn test_determine_status_real_json_and_setfacl_warnings() {
        let success_json = r#"{"conversation_id":"9fa0bf8a-807e-4cb8-838a-f52a724e532c","status":"SUCCESS","response":"Hello there, friend!\n","duration_seconds":3.6133411730000002,"num_turns":1,"usage":{"input_tokens":11849,"output_tokens":219,"thinking_tokens":214,"cache_read_tokens":0,"total_tokens":12068}}"#;
        let error_json = r#"{"conversation_id":"","status":"ERROR","response":"","error":"authentication failed or timed out","duration_seconds":0,"num_turns":0,"usage":{"input_tokens":0,"output_tokens":0,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":0}}"#;

        let mut setfacl_lines = String::new();
        for i in 0..30 {
            setfacl_lines.push_str(&format!(
                "setfacl: /path/to/file_{}.txt: Operation not supported\n",
                i
            ));
        }

        let mixed_success_log = format!("{}{}\n", setfacl_lines, success_json);
        let mixed_error_log = format!("{}{}\n", setfacl_lines, error_json);

        // Success JSON with setfacl warnings -> Done
        assert_eq!(
            determine_status_from_signals(Some(0), &mixed_success_log),
            JobStatus::Done,
            "Log with setfacl warnings followed by success JSON must resolve to Done with exit code 0"
        );
        assert_eq!(
            determine_status_from_signals(None, &mixed_success_log),
            JobStatus::Done,
            "Log with setfacl warnings followed by success JSON and None exit code must resolve to Done"
        );

        // Error JSON with setfacl warnings -> Failed
        assert_eq!(
            determine_status_from_signals(Some(0), &mixed_error_log),
            JobStatus::Failed,
            "Log with setfacl warnings followed by error JSON must resolve to Failed even if exit code is 0"
        );
        assert_eq!(
            determine_status_from_signals(Some(1), &mixed_error_log),
            JobStatus::Failed,
            "Log with setfacl warnings followed by error JSON and non-zero exit code must resolve to Failed"
        );
        assert_eq!(
            determine_status_from_signals(None, &mixed_error_log),
            JobStatus::Failed,
            "Log with setfacl warnings followed by error JSON and None exit code must resolve to Failed"
        );
    }

    #[test]
    fn test_job_metadata_load_missing_job_returns_clear_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let err = JobMetadata::load(temp_dir.path(), "nonexistent123").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("No such job found: 'nonexistent123'"));
        assert!(msg.contains("no job record in"));
    }

    #[test]
    fn test_job_metadata_load_corrupt_db_returns_clear_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Overwrite the state database with garbage to simulate a corrupt/truncated file.
        fs::write(
            state_db_path(temp_dir.path()),
            "this is definitely not a sqlite database",
        )
        .unwrap();

        let err = JobMetadata::load(temp_dir.path(), "corruptjob1").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Failed to enable WAL journal mode on state database at"));
        assert!(msg.contains("state.sqlite"));
    }

    #[test]
    fn test_list_jobs_sorted_newest_first_and_filtered() {
        let temp_dir = tempfile::tempdir().unwrap();
        for (i, (id, project)) in [
            ("job-a", "proj-x"),
            ("job-b", "proj-y"),
            ("job-c", "proj-x"),
        ]
        .iter()
        .enumerate()
        {
            let base = make_test_metadata(id, project, 0);
            // Spread timestamps so ordering is unambiguous.
            let meta = JobMetadata {
                created_at: Utc::now() - chrono::Duration::seconds(i as i64 * 100),
                ..base
            };
            meta.save(temp_dir.path()).unwrap();
        }

        let all = list_jobs(temp_dir.path(), None).unwrap();
        let ids: Vec<&str> = all.iter().map(|m| m.job_id.as_str()).collect();
        assert_eq!(ids, vec!["job-a", "job-b", "job-c"]);

        let filtered = list_jobs(temp_dir.path(), Some("proj-x")).unwrap();
        let ids: Vec<&str> = filtered.iter().map(|m| m.job_id.as_str()).collect();
        assert_eq!(ids, vec!["job-a", "job-c"]);

        // A fresh state root yields an empty list, not an error.
        let fresh = tempfile::tempdir().unwrap();
        assert!(list_jobs(fresh.path(), None).unwrap().is_empty());
    }

    #[test]
    fn test_job_metadata_save_load_round_trip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let meta = JobMetadata {
            job_id: "roundtrip".to_string(),
            project: "testproj".to_string(),
            prompt: "do something".to_string(),
            resolved_prompt: "do something with --config".to_string(),
            model: Some("model-x".to_string()),
            effort: Some("high".to_string()),
            sandbox: true,
            mode: "accept-edits".to_string(),
            add_dirs: vec![PathBuf::from("/data/a"), PathBuf::from("/data/b")],
            print_timeout: Some("2h".to_string()),
            created_at: Utc::now(),
            worktree_path: PathBuf::from("/workspace/wt"),
            branch: "agent-run/roundtrip".to_string(),
            base_ref: "main".to_string(),
            conversation_id: Some("conv-123".to_string()),
            retry_count: 3,
            pid: Some(12345),
            exit_code: None,
            status: Some(JobStatus::Running),
            hindsight: Some(true),
            auto_recall: Some(false),
            auto_retain: Some(true),
            retry_prompt: None,
            read_only: true,
            external_worktree: false,
            pid_start_time: Some(67890),
            hindsight_bank: Some("custom-bank".to_string()),
            proxy: Some("http://proxy.example.com:8080".to_string()),
            max_runtime: None,
            started_at: None,
            hindsight_query: Some("remember the migration".to_string()),
            sandbox_info: Some(crate::sandbox::SandboxInfo {
                backend: "gvisor".to_string(),
                runtime: "gvisor".to_string(),
                policy_path: "/tmp/roundtrip.policy.json".to_string(),
            }),
            engine: Some("claude".to_string()),
        };

        meta.save(temp_dir.path()).unwrap();
        assert!(state_db_path(temp_dir.path()).exists());

        let loaded = JobMetadata::load(temp_dir.path(), "roundtrip").unwrap();
        assert_eq!(loaded, meta);

        // Re-saving (as runner does when recording PID/exit code) updates the row in place.
        let updated = JobMetadata {
            exit_code: Some(0),
            status: Some(JobStatus::Done),
            ..meta.clone()
        };
        updated.save(temp_dir.path()).unwrap();
        let reloaded = JobMetadata::load(temp_dir.path(), "roundtrip").unwrap();
        assert_eq!(reloaded.exit_code, Some(0));
        assert_eq!(reloaded.status, Some(JobStatus::Done));
        assert_eq!(reloaded.job_id, "roundtrip");
        // Still exactly one row.
        assert_eq!(list_jobs(temp_dir.path(), None).unwrap().len(), 1);
    }

    #[test]
    fn test_set_job_sandbox_updates_row_and_persists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let meta = make_test_metadata("sandbox01", "p", 0);
        meta.save(temp_dir.path()).unwrap();
        assert_eq!(
            JobMetadata::load(temp_dir.path(), "sandbox01")
                .unwrap()
                .sandbox_info,
            None
        );

        let info = crate::sandbox::SandboxInfo {
            backend: "gvisor".to_string(),
            runtime: "gvisor".to_string(),
            policy_path: "/tmp/sandbox01.policy.json".to_string(),
        };
        set_job_sandbox(temp_dir.path(), "sandbox01", &info).unwrap();

        let loaded = JobMetadata::load(temp_dir.path(), "sandbox01").unwrap();
        assert_eq!(loaded.sandbox_info, Some(info.clone()));

        // Re-writing on a retry replaces the record, it does not append or error.
        let retry_info = crate::sandbox::SandboxInfo {
            backend: "gvisor".to_string(),
            runtime: "gvisor".to_string(),
            policy_path: "/tmp/sandbox01-retry.policy.json".to_string(),
        };
        set_job_sandbox(temp_dir.path(), "sandbox01", &retry_info).unwrap();
        let reloaded = JobMetadata::load(temp_dir.path(), "sandbox01").unwrap();
        assert_eq!(reloaded.sandbox_info, Some(retry_info));

        // A missing job id is not an error (mirrors `save`'s upsert semantics).
        set_job_sandbox(temp_dir.path(), "missing-job", &info).unwrap();
    }

    #[test]
    fn test_job_proxy_serializes_redacted_but_db_keeps_raw() {
        let temp_dir = tempfile::tempdir().unwrap();
        let raw = "http://user:secret@proxy.example.com:8080";
        let mut meta = make_test_metadata("proxyred01", "p", 0);
        meta.proxy = Some(raw.to_string());

        // The state DB must keep the RAW value (retry re-executes through it).
        meta.save(temp_dir.path()).unwrap();
        let loaded = JobMetadata::load(temp_dir.path(), "proxyred01").unwrap();
        assert_eq!(loaded.proxy.as_deref(), Some(raw));

        // Every JSON rendering of the job must be redacted: no password, and
        // no `user:` userinfo remnant either. Mirror the two real call sites
        // (status: `to_value(&meta)`, list: `to_value(job)`).
        let value = serde_json::to_value(&meta).unwrap();
        let rendered = value.to_string();
        assert!(
            !rendered.contains("secret"),
            "JSON leaked password: {rendered}"
        );
        assert!(
            !rendered.contains("user:"),
            "JSON leaked userinfo: {rendered}"
        );
        assert_eq!(
            value.get("proxy").and_then(|v| v.as_str()),
            Some("http://***:***@proxy.example.com:8080")
        );

        // A job without a proxy still serializes `proxy: null`.
        let plain = make_test_metadata("noproxy01", "p", 0);
        let plain_value = serde_json::to_value(plain).unwrap();
        match plain_value.get("proxy") {
            None | Some(serde_json::Value::Null) => {}
            Some(other) => panic!("expected proxy null, got {other}"),
        }
    }

    #[test]
    fn test_save_final_status_unless_killed_noop_when_already_killed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("condkill01", "p", 0);
        meta.exit_code = Some(137);
        meta.status = Some(JobStatus::Killed);
        meta.conversation_id = Some("conv-old".to_string());
        meta.save(temp_dir.path()).unwrap();

        // A runner-computed status must not clobber a row already marked
        // `killed` (the kill/runner race) -- but conversation_id is NOT
        // part of that race (kill.rs never sets it) and is persisted
        // unconditionally whenever a new one is given, since clean/gc need
        // it to purge agy's conversation artifacts even for a killed job.
        let applied = save_final_status_unless_killed(
            temp_dir.path(),
            "condkill01",
            JobStatus::Failed,
            Some(1),
            Some("conv-new"),
            None,
        )
        .unwrap();
        assert!(
            !applied,
            "status/exit_code update must be a no-op when the row is already `killed`"
        );
        let reloaded = JobMetadata::load(temp_dir.path(), "condkill01").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));
        assert_eq!(
            reloaded.conversation_id.as_deref(),
            Some("conv-new"),
            "conversation_id must still be persisted even when the status update is skipped"
        );
    }

    #[test]
    fn test_save_final_status_applies_when_not_killed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let meta = make_test_metadata("condok01", "p", 0);
        meta.save(temp_dir.path()).unwrap();

        // NULL status: the update applies (and persists the conversation ID).
        let applied = save_final_status_unless_killed(
            temp_dir.path(),
            "condok01",
            JobStatus::Done,
            Some(0),
            Some("conv-new"),
            None,
        )
        .unwrap();
        assert!(applied);
        let reloaded = JobMetadata::load(temp_dir.path(), "condok01").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Done));
        assert_eq!(reloaded.exit_code, Some(0));
        assert_eq!(reloaded.conversation_id.as_deref(), Some("conv-new"));

        // A non-terminal stored status (e.g. `running`) is also updated.
        // Passing `None` for conversation_id skips that column entirely
        // (rather than nulling it out) -- the previously-persisted
        // "conv-new" from above must survive.
        let mut running = reloaded;
        running.status = Some(JobStatus::Running);
        running.save(temp_dir.path()).unwrap();
        let applied = save_final_status_unless_killed(
            temp_dir.path(),
            "condok01",
            JobStatus::Failed,
            Some(1),
            None,
            None,
        )
        .unwrap();
        assert!(applied);
        let reloaded = JobMetadata::load(temp_dir.path(), "condok01").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Failed));
        assert_eq!(reloaded.exit_code, Some(1));
        assert_eq!(
            reloaded.conversation_id.as_deref(),
            Some("conv-new"),
            "conversation_id must not be nulled out when None is passed (only skipped)"
        );
    }

    #[test]
    fn test_save_final_status_skips_superseded_retry_attempt() {
        // Attempt 0's monitor finalises late, after `retry` already bumped the
        // row to retry_count 1 / status running. Its write must be a no-op.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("retryguard01", "p", 0);
        meta.retry_count = 1;
        meta.status = Some(JobStatus::Running);
        meta.exit_code = None;
        meta.save(temp_dir.path()).unwrap();

        let applied = save_final_status_unless_killed(
            temp_dir.path(),
            "retryguard01",
            JobStatus::Failed,
            Some(143),
            None,
            Some(0),
        )
        .unwrap();
        assert!(
            !applied,
            "a superseded attempt's final status must be skipped"
        );
        let reloaded = JobMetadata::load(temp_dir.path(), "retryguard01").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Running));
        assert_eq!(reloaded.exit_code, None);
    }

    #[test]
    fn test_save_final_status_applies_for_current_retry_attempt() {
        // The CURRENT attempt (retry_count matches) still finalises normally,
        // including from a `running` row.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("retryguard02", "p", 0);
        meta.retry_count = 2;
        meta.status = Some(JobStatus::Running);
        meta.save(temp_dir.path()).unwrap();

        let applied = save_final_status_unless_killed(
            temp_dir.path(),
            "retryguard02",
            JobStatus::Done,
            Some(0),
            Some("conv-1"),
            Some(2),
        )
        .unwrap();
        assert!(applied);
        let reloaded = JobMetadata::load(temp_dir.path(), "retryguard02").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Done));
        assert_eq!(reloaded.exit_code, Some(0));
    }

    #[test]
    fn test_save_final_status_retry_guard_still_respects_killed() {
        // Matching retry_count does not override the existing kill protection.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("retryguard03", "p", 0);
        meta.retry_count = 1;
        meta.status = Some(JobStatus::Killed);
        meta.exit_code = Some(137);
        meta.save(temp_dir.path()).unwrap();

        let applied = save_final_status_unless_killed(
            temp_dir.path(),
            "retryguard03",
            JobStatus::Failed,
            Some(1),
            None,
            Some(1),
        )
        .unwrap();
        assert!(!applied);
        let reloaded = JobMetadata::load(temp_dir.path(), "retryguard03").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));
    }

    #[test]
    fn test_job_metadata_concurrent_writes_no_corruption() {
        let temp_dir = tempfile::tempdir().unwrap();
        let meta = make_test_metadata("concurrent", "testproj", 0);
        meta.save(temp_dir.path()).unwrap();

        // Two processes simulating concurrent agent-run invocations: one updates
        // the same job's PID repeatedly, the other writes a different job.
        let root_a = temp_dir.path().to_path_buf();
        let handle_a = std::thread::spawn(move || {
            for i in 0..10 {
                let mut m = JobMetadata::load(&root_a, "concurrent").unwrap();
                m.pid = Some(1000 + i);
                m.save(&root_a).unwrap();
            }
        });
        let root_b = temp_dir.path().to_path_buf();
        let handle_b = std::thread::spawn(move || {
            let mut m = make_test_metadata("other-job", "otherproj", 0);
            m.exit_code = Some(1);
            m.save(&root_b).unwrap();
        });
        handle_a.join().unwrap();
        handle_b.join().unwrap();

        let a = JobMetadata::load(temp_dir.path(), "concurrent").unwrap();
        assert!(a.pid.unwrap() >= 1000);
        assert_eq!(a.project, "testproj");
        let b = JobMetadata::load(temp_dir.path(), "other-job").unwrap();
        assert_eq!(b.exit_code, Some(1));
        assert_eq!(list_jobs(temp_dir.path(), None).unwrap().len(), 2);
    }

    #[test]
    fn test_job_metadata_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        make_test_metadata("doomed", "testproj", 0)
            .save(temp_dir.path())
            .unwrap();

        JobMetadata::delete(temp_dir.path(), "doomed").unwrap();
        assert!(JobMetadata::load(temp_dir.path(), "doomed").is_err());
        // Deleting an already-gone job is a no-op, not an error.
        JobMetadata::delete(temp_dir.path(), "doomed").unwrap();
    }

    fn make_test_metadata(job_id: &str, project: &str, _age: u8) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            project: project.to_string(),
            prompt: "do something".to_string(),
            resolved_prompt: "do something".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: PathBuf::from("/workspace/wt"),
            branch: format!("agent-run/{}", job_id),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            exit_code: None,
            status: None,
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
        }
    }

    #[test]
    fn test_schema_migration_adds_new_columns_to_old_db() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = state_db_path(temp_dir.path());
        fs::create_dir_all(db_path.parent().unwrap()).unwrap();

        // Create a database with the OLD schema (no read_only / external_worktree / pid_start_time).
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE jobs (
                job_id TEXT PRIMARY KEY, project TEXT NOT NULL, prompt TEXT NOT NULL,
                resolved_prompt TEXT NOT NULL, model TEXT, effort TEXT,
                sandbox INTEGER NOT NULL, mode TEXT NOT NULL, add_dirs TEXT NOT NULL,
                print_timeout TEXT, created_at INTEGER NOT NULL,
                worktree_path TEXT NOT NULL, branch TEXT NOT NULL, base_ref TEXT NOT NULL,
                conversation_id TEXT, retry_count INTEGER NOT NULL, pid INTEGER,
                exit_code INTEGER, status TEXT, hindsight INTEGER, auto_recall INTEGER,
                auto_retain INTEGER, retry_prompt TEXT
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, project, prompt, resolved_prompt, sandbox, mode, add_dirs, created_at, worktree_path, branch, base_ref, retry_count)
             VALUES ('oldjob01', 'p', 'pr', 'pr', 0, 'plan', '[]', 0, '/wt', 'b', 'main', 0)",
            [],
        )
        .unwrap();
        conn.close().unwrap();

        // Opening through agent-run must migrate the table and load the row
        // with the new fields defaulted.
        let meta = JobMetadata::load(temp_dir.path(), "oldjob01").unwrap();
        assert_eq!(meta.job_id, "oldjob01");
        assert!(!meta.read_only);
        assert!(!meta.external_worktree);
        assert_eq!(meta.pid_start_time, None);
        assert_eq!(meta.hindsight_query, None);
        assert_eq!(meta.sandbox_info, None);
        assert_eq!(meta.engine, None);

        // The migrated row can also be round-tripped through save().
        let mut meta = meta;
        meta.read_only = true;
        meta.external_worktree = true;
        meta.pid_start_time = Some(987654);
        meta.hindsight_query = Some("stored query".to_string());
        meta.sandbox_info = Some(crate::sandbox::SandboxInfo {
            backend: "runc".to_string(),
            runtime: "runc".to_string(),
            policy_path: "/tmp/migrated.policy.json".to_string(),
        });
        meta.engine = Some("claude".to_string());
        meta.save(temp_dir.path()).unwrap();
        let reloaded = JobMetadata::load(temp_dir.path(), "oldjob01").unwrap();
        assert!(reloaded.read_only);
        assert!(reloaded.external_worktree);
        assert_eq!(reloaded.pid_start_time, Some(987654));
        assert_eq!(reloaded.hindsight_query, Some("stored query".to_string()));
        assert_eq!(
            reloaded.sandbox_info,
            Some(crate::sandbox::SandboxInfo {
                backend: "runc".to_string(),
                runtime: "runc".to_string(),
                policy_path: "/tmp/migrated.policy.json".to_string(),
            })
        );
        assert_eq!(reloaded.engine, Some("claude".to_string()));
    }

    #[test]
    fn test_job_status_strings_and_terminal() {
        assert_eq!(JobStatus::Running.as_str(), "running");
        assert_eq!(JobStatus::Done.as_str(), "done");
        assert_eq!(JobStatus::Failed.as_str(), "failed");
        assert_eq!(JobStatus::SelfTerminated.as_str(), "self-terminated");
        assert_eq!(JobStatus::Killed.as_str(), "killed");
        assert_eq!(JobStatus::Incomplete.as_str(), "incomplete");
        assert_eq!(JobStatus::QuotaExhausted.as_str(), "quota-exhausted");

        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Done.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::SelfTerminated.is_terminal());
        assert!(JobStatus::Killed.is_terminal());
        assert!(JobStatus::Incomplete.is_terminal());
        assert!(JobStatus::QuotaExhausted.is_terminal());

        // parse round-trips for the new statuses
        assert_eq!(JobStatus::parse("incomplete"), Some(JobStatus::Incomplete));
        assert_eq!(
            JobStatus::parse("quota-exhausted"),
            Some(JobStatus::QuotaExhausted)
        );
    }

    #[test]
    fn test_compute_status_killed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let meta = JobMetadata {
            job_id: "testkilled".to_string(),
            project: "testproj".to_string(),
            prompt: "test".to_string(),
            resolved_prompt: "test".to_string(),
            model: None,
            effort: None,
            sandbox: false,
            mode: "accept-edits".to_string(),
            add_dirs: vec![],
            print_timeout: None,
            created_at: Utc::now(),
            worktree_path: PathBuf::from("/tmp/dummy"),
            branch: "agent-run/testkilled".to_string(),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            exit_code: Some(137),
            status: Some(JobStatus::Killed),
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

        let status = compute_status(&meta, temp_dir.path());
        assert_eq!(status, JobStatus::Killed);
    }

    #[test]
    fn test_resolve_prefix_matching() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Distinct ids sharing the prefix "abc".
        make_test_metadata("abc1", "p", 0)
            .save(temp_dir.path())
            .unwrap();
        make_test_metadata("abc2", "p", 0)
            .save(temp_dir.path())
            .unwrap();
        make_test_metadata("xyz1", "p", 0)
            .save(temp_dir.path())
            .unwrap();

        // Exact match works.
        let m = JobMetadata::resolve(temp_dir.path(), "abc1").unwrap();
        assert_eq!(m.job_id, "abc1");

        // Unique prefix resolves.
        let m = JobMetadata::resolve(temp_dir.path(), "xyz").unwrap();
        assert_eq!(m.job_id, "xyz1");
        let m = JobMetadata::resolve(temp_dir.path(), "abc1").unwrap();
        assert_eq!(m.job_id, "abc1");

        // Ambiguous prefix is rejected with both matches listed.
        let err = JobMetadata::resolve(temp_dir.path(), "abc").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"), "Error was: {}", msg);
        assert!(
            msg.contains("abc1") && msg.contains("abc2"),
            "Error was: {}",
            msg
        );

        // Unknown id.
        let err = JobMetadata::resolve(temp_dir.path(), "zzz").unwrap_err();
        assert!(
            err.to_string().contains("No such job"),
            "Error was: {}",
            err
        );

        // Escape characters: % and _ and \ in job IDs must not behave as wildcards
        make_test_metadata("job%1", "p", 0)
            .save(temp_dir.path())
            .unwrap();
        make_test_metadata("job_1", "p", 0)
            .save(temp_dir.path())
            .unwrap();
        make_test_metadata("jobx1", "p", 0)
            .save(temp_dir.path())
            .unwrap();

        // Querying "job%" should resolve ONLY "job%1", NOT "job_1" or "jobx1"
        let m = JobMetadata::resolve(temp_dir.path(), "job%").unwrap();
        assert_eq!(m.job_id, "job%1");

        // Querying "job_" should resolve ONLY "job_1", NOT "job%1" or "jobx1"
        let m = JobMetadata::resolve(temp_dir.path(), "job_").unwrap();
        assert_eq!(m.job_id, "job_1");
    }

    #[test]
    fn test_mark_job_killed_updates_only_status_and_exit_code() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Row carrying fields a full-row save from a stale snapshot would
        // clobber (conversation_id written concurrently by the runner).
        let mut meta = make_test_metadata("kmk01", "p", 0);
        meta.retry_count = 1;
        meta.conversation_id = Some("conv-9".to_string());
        meta.pid = Some(999);
        meta.retry_prompt = Some("do it again".to_string());
        meta.save(temp_dir.path()).unwrap();

        let applied = mark_job_killed(temp_dir.path(), "kmk01", 1).unwrap();
        assert!(applied, "helper must apply for the expected retry_count");
        let reloaded = JobMetadata::load(temp_dir.path(), "kmk01").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));
        assert_eq!(
            reloaded.conversation_id.as_deref(),
            Some("conv-9"),
            "conversation_id must survive the targeted kill update"
        );
        assert_eq!(reloaded.pid, Some(999));
        assert_eq!(reloaded.retry_prompt.as_deref(), Some("do it again"));
        assert_eq!(reloaded.retry_count, 1);

        // A delayed kill for a superseded attempt is a no-op: the row is
        // neither re-killed nor touched at all.
        let mut other = make_test_metadata("kmk02", "p", 0);
        other.retry_count = 5;
        other.conversation_id = Some("conv-7".to_string());
        other.save(temp_dir.path()).unwrap();
        let applied = mark_job_killed(temp_dir.path(), "kmk02", 6).unwrap();
        assert!(!applied, "a newer retry_count must skip the kill update");
        let reloaded = JobMetadata::load(temp_dir.path(), "kmk02").unwrap();
        assert_eq!(reloaded.status, None);
        assert_eq!(reloaded.exit_code, None);
        assert_eq!(reloaded.conversation_id.as_deref(), Some("conv-7"));
    }

    #[test]
    fn test_mark_job_killed_only_protects_finished_success() {
        // (a) The done-race: the job finished SUCCESSFULLY on its own
        // between the decision to terminate it and the mark (the runner
        // recorded `done` with a clean exit code, the PID is already gone,
        // so terminate succeeded as "already dead"). Such a row must KEEP
        // its done status and exit code, never be rewritten to killed/137.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut done = make_test_metadata("kmkdone", "p", 0);
        done.status = Some(JobStatus::Done);
        done.exit_code = Some(0);
        done.save(temp_dir.path()).unwrap();

        let applied = mark_job_killed(temp_dir.path(), "kmkdone", 0).unwrap();
        assert!(
            !applied,
            "mark_job_killed must refuse a row that is already done"
        );
        let reloaded = JobMetadata::load(temp_dir.path(), "kmkdone").unwrap();
        assert_eq!(
            reloaded.status,
            Some(JobStatus::Done),
            "a done job must not be overwritten to killed"
        );
        assert_eq!(reloaded.exit_code, Some(0));

        // (b) The SIGTERM race: WE sent the signal, the process died from
        // it, and the runner already recorded the signal-derived exit code
        // 143 -- with a status of `failed` from the error line the process
        // wrote on interrupt, or with no status stamped yet. Either way the
        // row is ours: it must become killed/137 (a job that ended on our
        // SIGTERM is a killed job, whatever the reaper wrote first).
        let mut signalled = make_test_metadata("kmksig", "p", 0);
        signalled.status = Some(JobStatus::Failed);
        signalled.exit_code = Some(143);
        signalled.save(temp_dir.path()).unwrap();
        assert!(
            mark_job_killed(temp_dir.path(), "kmksig", 0).unwrap(),
            "a row carrying our signal's exit 143 must become killed"
        );
        let reloaded = JobMetadata::load(temp_dir.path(), "kmksig").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));

        let mut bare = make_test_metadata("kmkbare", "p", 0);
        bare.exit_code = Some(143);
        bare.save(temp_dir.path()).unwrap();
        assert!(
            mark_job_killed(temp_dir.path(), "kmkbare", 0).unwrap(),
            "an exit-143 row with no status stamped must become killed"
        );
        let reloaded = JobMetadata::load(temp_dir.path(), "kmkbare").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));

        // (c) A row already marked killed is left exactly as-is.
        let mut killed = make_test_metadata("kmkdone2", "p", 0);
        killed.status = Some(JobStatus::Killed);
        killed.exit_code = Some(137);
        killed.save(temp_dir.path()).unwrap();
        assert!(!mark_job_killed(temp_dir.path(), "kmkdone2", 0).unwrap());
        let reloaded = JobMetadata::load(temp_dir.path(), "kmkdone2").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));

        // A running attempt with no exit code yet is still markable (the
        // normal kill path).
        let mut running = make_test_metadata("kmkrund", "p", 0);
        running.status = Some(JobStatus::Running);
        running.pid = Some(999);
        running.save(temp_dir.path()).unwrap();
        assert!(mark_job_killed(temp_dir.path(), "kmkrund", 0).unwrap());
        let reloaded = JobMetadata::load(temp_dir.path(), "kmkrund").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Killed));
        assert_eq!(reloaded.exit_code, Some(137));

        // (d) A delayed kill aimed at a superseded attempt is a no-op
        // regardless of the row's state: the retry_count guard keeps it
        // exactly as the newer attempt left it.
        let mut stale = make_test_metadata("kmkstale", "p", 0);
        stale.retry_count = 3;
        stale.status = Some(JobStatus::Running);
        stale.save(temp_dir.path()).unwrap();
        assert!(
            !mark_job_killed(temp_dir.path(), "kmkstale", 2).unwrap(),
            "a stale retry_count must skip the kill update"
        );
        let reloaded = JobMetadata::load(temp_dir.path(), "kmkstale").unwrap();
        assert_eq!(reloaded.status, Some(JobStatus::Running));
        assert_eq!(reloaded.exit_code, None);
    }

    #[test]
    fn test_insert_new_fails_on_existing_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let meta = make_test_metadata("ins01", "p", 0);
        meta.insert_new(temp_dir.path()).unwrap();
        assert_eq!(
            JobMetadata::load(temp_dir.path(), "ins01").unwrap().job_id,
            "ins01"
        );

        // A second insert of the SAME id must error, not overwrite:
        // a generated-id collision (upsert via save) would silently
        // destroy the first job's row.
        let other_row = make_test_metadata("ins01", "OVERRUN", 0);
        let err = other_row.insert_new(temp_dir.path()).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("already exists"),
            "collision must be reported, got: {}",
            msg
        );
        let untouched = JobMetadata::load(temp_dir.path(), "ins01").unwrap();
        assert_eq!(
            untouched.project, "p",
            "existing row must not be overwritten"
        );
    }

    #[test]
    fn test_job_id_exists_and_generate_unique_job_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(!job_id_exists(temp_dir.path(), "aaaabbbb").unwrap());
        make_test_metadata("aaaabbbb", "p", 0)
            .insert_new(temp_dir.path())
            .unwrap();
        assert!(job_id_exists(temp_dir.path(), "aaaabbbb").unwrap());
        assert!(!job_id_exists(temp_dir.path(), "ffffffff").unwrap());

        // The collision-checking generator never hands out a taken id.
        let id = generate_unique_job_id(temp_dir.path()).unwrap();
        assert_eq!(id.len(), 8);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(id, "aaaabbbb");
        assert!(!job_id_exists(temp_dir.path(), &id).unwrap());
    }

    #[test]
    fn test_find_uuid_in_line_multibyte_does_not_panic() {
        // Byte window: a 2-byte 'é' (0xC3 0xA9) followed by 7 'a's and
        // '-' at byte offsets 9/14/19/24. For i=1 the window [1..37)
        // starts INSIDE the first char: the old `&line[i..i + 36]`
        // panicked there, the range-checked `line.get` skips it.
        let mut bytes: Vec<u8> = vec![0xC3, 0xA9];
        bytes.extend(std::iter::repeat_n(b'a', 7)); // idx 2..=8
        bytes.push(b'-'); // idx 9
        bytes.extend(std::iter::repeat_n(b'a', 4)); // idx 10..=13
        bytes.push(b'-'); // idx 14
        bytes.extend(std::iter::repeat_n(b'a', 4)); // idx 15..=18
        bytes.push(b'-'); // idx 19
        bytes.extend(std::iter::repeat_n(b'a', 4)); // idx 20..=23
        bytes.push(b'-'); // idx 24
        bytes.extend(std::iter::repeat_n(b'a', 12)); // idx 25..=36
        bytes.push(b' '); // idx 37
        let line = String::from_utf8(bytes).unwrap();
        assert_eq!(line.len(), 38);
        assert!(
            !line.is_char_boundary(1),
            "test setup: byte 1 must sit inside a multibyte char"
        );
        assert_eq!(find_uuid_in_line(&line), None);

        // A well-aligned uuid right after multibyte text is still found.
        let line2 = "é 8f14e45f-ceea-467f-a96f-4208246a6a34";
        assert_eq!(
            find_uuid_in_line(line2),
            Some("8f14e45f-ceea-467f-a96f-4208246a6a34".to_string())
        );
    }

    #[test]
    fn test_compute_status_tolerates_invalid_utf8_log() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("lossy01", "p", 0);
        meta.exit_code = Some(0);
        meta.save(temp_dir.path()).unwrap();

        // One invalid byte anywhere in the log used to abort the whole
        // read (unwrap_or_default -> ""), misclassifying the finished job
        // as Incomplete. Lossy decoding must keep the SUCCESS line.
        fs::create_dir_all(JobMetadata::log_dir(temp_dir.path())).unwrap();
        let mut log_bytes: Vec<u8> = vec![0xFF, 0xFE, b'\n'];
        log_bytes
            .extend_from_slice(br#"{"conversation_id":"c1","status":"SUCCESS","response":"done"}"#);
        fs::write(
            JobMetadata::log_path(temp_dir.path(), "lossy01"),
            &log_bytes,
        )
        .unwrap();

        assert_eq!(
            compute_status(&meta, temp_dir.path()),
            JobStatus::Done,
            "a log with one invalid UTF-8 byte must not be treated as empty"
        );
    }

    #[test]
    fn test_quota_exhaustion_scan_covered_by_retry_marker() {
        // `agent-run retry` appends this banner between attempts.
        let banner = "==================== RETRY AT 2026-01-01T00:00:00+00:00 ====================";

        // A quota error in the SUPERSEDED attempt must not mark the job
        // QuotaExhausted once attempt 2 succeeds.
        let recovered = format!(
            "upstream: rate limit exceeded\n{}\n\n{{\"status\":\"SUCCESS\",\"response\":\"done\"}}\n",
            banner
        );
        assert_eq!(
            determine_status_from_signals(Some(0), &recovered),
            JobStatus::Done,
            "quota signal before the last retry marker must not win over attempt 2's SUCCESS"
        );

        // A quota signal in the LAST attempt still classifies the job.
        let quota_last = format!(
            "attempt one fine\n{}\n\nupstream error: too many requests\n",
            banner
        );
        assert_eq!(
            determine_status_from_signals(Some(1), &quota_last),
            JobStatus::QuotaExhausted,
            "quota signal after the last retry marker must still win"
        );

        // The discriminating case: attempt 1 hit the provider's quota,
        // attempt 2 ran but ended with a structured error. The quota
        // signal is in the SUPERSEDED attempt, so the job is Failed, not
        // QuotaExhausted (the old whole-log scan returned QuotaExhausted).
        let error_after_quota = format!(
            "gRPC: RESOURCE_EXHAUSTED insufficient_quota\n{}\n\n{{\"status\":\"ERROR\",\"error\":\"tool failed\"}}\n",
            banner
        );
        assert_eq!(
            determine_status_from_signals(Some(1), &error_after_quota),
            JobStatus::Failed,
            "a quota signal only in the superseded attempt must not win over the last attempt's structured error"
        );

        // No marker (single attempt): whole-log behavior unchanged.
        assert_eq!(
            determine_status_from_signals(Some(137), "request rejected: quota exceeded"),
            JobStatus::QuotaExhausted
        );
        // Unit behavior of the windowing helper.
        assert!(last_attempt_log("no markers at all rate limit").contains("rate limit"));
        let windowed = format!("old rate limit\n{}\n\nnew attempt\n", banner);
        let tail = last_attempt_log(&windowed);
        assert!(tail.contains("new attempt"));
        assert!(!tail.contains("old rate limit"));
    }

    #[test]
    fn test_status_signals_are_scoped_to_last_attempt() {
        // Banner written by `agent-run retry` between attempts (same exact
        // shape as in `last_attempt_log` tests above).
        let banner = "==================== RETRY AT 2026-01-01T00:00:00+00:00 ====================";

        // Scenario A: attempt 1 was cut off with a pending background task,
        // attempt 2 then succeeds with a structured SUCCESS. The notice left
        // behind by the SUPERSEDED attempt must not pin the job Incomplete
        // forever (the old whole-log scan picked it up).
        let recovered_bg = format!(
            "Kicking off the long build in the background...\nterminating 1 background task(s) on exit\n{{\"status\":\"SUCCESS\",\"response\":\"will report once the build finishes\"}}\n{}\n\nRunning integration tests...\n{{\"status\":\"SUCCESS\",\"response\":\"All tests passed.\"}}\n",
            banner
        );
        assert_eq!(
            determine_status_from_signals(Some(0), &recovered_bg),
            JobStatus::Done,
            "a pending-background-task notice only in the superseded attempt must not pin the job Incomplete"
        );
        assert_eq!(
            extract_final_response(&recovered_bg).as_deref(),
            Some("All tests passed."),
            "the final response must come from the last attempt, not the old 'will wait' promise"
        );

        // Scenario B: attempt 1 ends with a structured SUCCESS, follow-up
        // attempt 2 crashes without any JSON at all. The scan must not go
        // back past the retry banner and re-report attempt 1's SUCCESS as the
        // current outcome.
        let crashed_after_success = format!(
            "{{\"status\":\"SUCCESS\",\"response\":\"first attempt done\"}}\n{}\n\nattempt two started\nsegfault (core dumped)\n",
            banner
        );
        assert_eq!(
            determine_status_from_signals(Some(137), &crashed_after_success),
            JobStatus::SelfTerminated,
            "a SUCCESS from the superseded attempt must not be re-reported once the last attempt crashed (137, no JSON)"
        );
        assert_eq!(
            determine_status_from_signals(None, &crashed_after_success),
            JobStatus::SelfTerminated,
            "a SUCCESS from the superseded attempt must not be re-reported once the last attempt crashed (no exit code)"
        );
        assert_eq!(
            extract_final_response(&crashed_after_success),
            None,
            "no structured line in the last attempt means no final response"
        );

        // Signals in the LAST attempt must still win (no over-windowing).
        let still_pending = format!(
            "plain attempt one\n{}\n\nterminating 1 background task(s) on exit\n{{\"status\":\"SUCCESS\",\"response\":\"I will wait\"}}\n",
            banner
        );
        assert_eq!(
            determine_status_from_signals(Some(0), &still_pending),
            JobStatus::Incomplete,
            "a pending-background-task notice in the LAST attempt must still force Incomplete"
        );
        assert_eq!(
            extract_final_response(&still_pending).as_deref(),
            Some("I will wait")
        );
    }

    // ---------------------------------------------------------------------
    // stream-json (`--output-format stream-json`) log handling. The fixtures
    // mirror /workspace/in/stream-success.ndjson and
    // /workspace/in/stream-error.ndjson (real agy stream output); only the
    // very long `tools` array in the real init event is trimmed down.
    // ---------------------------------------------------------------------
    const STREAM_SUCCESS_LOG: &str = r#"{"event":"init","conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","init":{"model":"gemini-3.8-flash-low","cwd":"/tmp/sj","tools":["run_command","read_url_content","send_message"],"permission_mode":"always-proceed"}}
{"event":"step_update","step_update":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","step_index":0,"state":"DONE","step_type":"user_input"}}
{"event":"step_update","step_update":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","step_index":1,"state":"DONE","step_type":"agent_response","duration_seconds":2.6094502090000002,"usage":{"input_tokens":11757,"output_tokens":55,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":11812}}}
{"event":"step_update","step_update":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","step_index":2,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"name":"run_command","parameters":{"CommandLine":"echo hello"}}}}
{"event":"step_update","step_update":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","step_index":2,"state":"DONE","step_type":"tool","tool_name":"run_command","duration_seconds":0.075699115,"tool_info":{"name":"run_command","parameters":{"CommandLine":"echo hello"},"output":"hello\r\n"}}}
{"event":"step_update","step_update":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","step_index":3,"state":"DONE","step_type":"agent_response","text_delta":"done\n","duration_seconds":2.389726535,"usage":{"input_tokens":11889,"output_tokens":1,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":11890}}}
{"event":"result","result":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","status":"SUCCESS","response":"done\n","duration_seconds":5.481872132,"num_turns":1,"usage":{"input_tokens":23646,"output_tokens":56,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":23702}}}
"#;

    const STREAM_ERROR_LOG: &str = r#"{"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":"invalid model selection (--model \"no-such-model-xyz\" --effort \"\"): model no-such-model-xyz is not recognized as a known model or custom model in settings\nAvailable models:\n  Gemini 3.8 Flash (High)\n  GPT-OSS 120B (Medium)","duration_seconds":0,"num_turns":0,"usage":{"input_tokens":0,"output_tokens":0,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":0}}}
"#;

    #[test]
    fn test_stream_json_success_log() {
        // A full `--output-format stream-json` run: an init event, several
        // step_update events, and a terminal `event:"result"` SUCCESS line.
        // The status must be derived from the NESTED `result.status`, and the
        // final response from the nested `result.response` ("done\n").
        assert_eq!(
            determine_status_from_signals(Some(0), STREAM_SUCCESS_LOG),
            JobStatus::Done,
            "a stream-json SUCCESS result line must classify the job Done"
        );
        assert_eq!(
            extract_final_response(STREAM_SUCCESS_LOG).as_deref(),
            Some("done\n"),
            "the final response must come from the nested result.response"
        );
        // The init event carries the conversation id at the TOP level (Pattern 0),
        // which the nested result line (Pattern 2) would also recover.
        assert_eq!(
            extract_conversation_id(STREAM_SUCCESS_LOG).as_deref(),
            Some("67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8"),
            "the init event's top-level conversation_id must be extracted"
        );
    }

    #[test]
    fn test_stream_json_error_log() {
        // A run that dies before the first turn: a single `event:"result"`
        // line with `status:"ERROR"` and an `error` string, and an EMPTY
        // conversation_id.
        assert_eq!(
            determine_status_from_signals(Some(1), STREAM_ERROR_LOG),
            JobStatus::Failed,
            "a stream-json ERROR result line must classify the job Failed"
        );
        assert_eq!(
            determine_status_from_signals(Some(0), STREAM_ERROR_LOG),
            JobStatus::Failed,
            "a stream-json ERROR result line is Failed regardless of exit code"
        );
        // An empty conversation_id must not yield a bogus id.
        assert_eq!(
            extract_conversation_id(STREAM_ERROR_LOG),
            None,
            "an empty conversation_id in the stream result line must not yield a spurious id"
        );
    }

    #[test]
    fn test_stream_json_background_task_signal_skips_step_update() {
        // A `step_update` line whose `text_delta` QUOTES agy's exit notice must
        // NOT trigger the pending-background-task signal: the text is the
        // agent's own narrative inside a JSON event, not agy's plain-text
        // notice (the JSON line is skipped wholesale).
        let quoted_in_step = r#"{"event":"step_update","step_update":{"conversation_id":"c1","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"The log said: terminating 1 background task(s) on exit, so the build was cut off."}}"#;
        assert!(
            !has_pending_background_task_signal(quoted_in_step),
            "a quoted notice inside a step_update text_delta must not trigger"
        );

        // But agy's own plain-text notice (a non-JSON line) still triggers.
        let plain_notice = "terminating 1 background task(s) on exit";
        assert!(
            has_pending_background_task_signal(plain_notice),
            "agy's plain-text notice must still trigger"
        );
    }

    #[test]
    fn test_stream_json_quota_signal_skips_step_update() {
        // A `step_update` line quoting rate-limit wording (the agent reading
        // docs) must NOT be a quota signal.
        let quoted_in_step = r#"{"event":"step_update","step_update":{"conversation_id":"c1","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"Note: you will hit the rate limit if you send too many requests."}}"#;
        assert!(
            !has_quota_exhaustion_signal(quoted_in_step),
            "rate-limit wording inside a step_update must not trigger the quota signal"
        );

        // But a real `event:"result"` ERROR line carrying a quota marker does.
        let quota_result = r#"{"event":"result","result":{"conversation_id":"c1","status":"ERROR","response":"","error":"quota exceeded"}}"#;
        assert!(
            has_quota_exhaustion_signal(quota_result),
            "a quota marker in the result error text must trigger the quota signal"
        );
    }

    #[test]
    fn test_stream_json_retry_scoping() {
        let banner = "==================== RETRY AT 2026-01-01T00:00:00+00:00 ====================";

        // Attempt 1 (superseded) ends with a stream SUCCESS; attempt 2 ends
        // with a stream ERROR. Only attempt 2 counts: the job is Failed, and
        // the final response must NOT leak from the superseded attempt.
        let mixed = format!(
            "{}\n{}\n\n{}\n",
            r#"{"event":"result","result":{"conversation_id":"c1","status":"SUCCESS","response":"first attempt done"}}"#,
            banner,
            r#"{"event":"result","result":{"conversation_id":"c1","status":"ERROR","response":"","error":"tool failed"}}"#
        );
        assert_eq!(
            determine_status_from_signals(Some(1), &mixed),
            JobStatus::Failed,
            "a stream ERROR in the last attempt must win over a superseded stream SUCCESS"
        );
        assert_eq!(
            extract_final_response(&mixed).as_deref(),
            Some(""),
            "the final response must not leak from the superseded attempt"
        );

        // The reverse: a step_update quoting the pending-task notice in attempt
        // 1 must not pin the job Incomplete once attempt 2 succeeds.
        let recovered = format!(
            "{}\n{}\n\n{}\n",
            r#"{"event":"step_update","step_update":{"conversation_id":"c1","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"terminating 1 background task(s) on exit"}}"#,
            banner,
            r#"{"event":"result","result":{"conversation_id":"c1","status":"SUCCESS","response":"all good"}}"#
        );
        assert_eq!(
            determine_status_from_signals(Some(0), &recovered),
            JobStatus::Done,
            "a quoted notice in the superseded attempt must not pin the job Incomplete"
        );
        assert_eq!(
            extract_final_response(&recovered).as_deref(),
            Some("all good")
        );
    }

    #[test]
    fn test_stream_json_conversation_id_sources() {
        // With no init line present, the id nested in the result line
        // (result.conversation_id) is still recovered by Pattern 2.
        let result_only = r#"{"event":"result","result":{"conversation_id":"67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8","status":"SUCCESS","response":"done"}}"#;
        assert_eq!(
            extract_conversation_id(result_only).as_deref(),
            Some("67b3c7c6-9bd7-48e8-8eb6-6cecf92565a8"),
            "the conversation id nested in the result line must still be recovered"
        );
    }

    #[test]
    fn test_extract_conversation_id_from_log_tolerates_invalid_utf8() {
        let temp_dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(JobMetadata::log_dir(temp_dir.path())).unwrap();
        let log_path = JobMetadata::log_path(temp_dir.path(), "lossy02");

        // An invalid UTF-8 byte (0xFF) before the line carrying the
        // conversation id: the read must not fail and lose the id just
        // because of that one byte (compute_status decodes the same file
        // lossily; `read_to_string` used to return Err here).
        let mut bytes: Vec<u8> = vec![0xFF];
        bytes.extend_from_slice(b"noise from a crashed tool\n");
        bytes.extend_from_slice(b"Conversation ID: 12345678-1234-1234-1234-123456789abc\n");
        fs::write(&log_path, &bytes).unwrap();

        assert_eq!(
            extract_conversation_id_from_log(&log_path).as_deref(),
            Some("12345678-1234-1234-1234-123456789abc"),
            "a stray invalid byte must not make the conversation-id extraction fail"
        );

        // A missing log still yields None (no accidental panic/unwrap).
        assert_eq!(
            extract_conversation_id_from_log(&temp_dir.path().join("missing.log")),
            None
        );
    }

    #[test]
    fn test_compute_status_running_before_monitor_writes_pid() {
        // A freshly created (or freshly re-claimed) job has no pid yet -- the
        // monitor writes it only after the process starts. During that gap
        // the row must still report Running, not a terminal status derived
        // from the empty log (which made `wait` exit early and let `gc`
        // prune the starting job).
        let temp_dir = tempfile::tempdir().unwrap();

        let mut fresh = make_test_metadata("freshrun01", "p", 0);
        fresh.status = Some(JobStatus::Running);
        fresh.save(temp_dir.path()).unwrap();
        assert_eq!(
            compute_status(&fresh, temp_dir.path()),
            JobStatus::Running,
            "a fresh job with no pid recorded yet must still be Running during the pid-write gap"
        );

        // The same shape but created long ago is an orphaned row of a dead
        // server: it must still classify from the log (empty log ->
        // SelfTerminated) so `retry` can resume it and gc can consider it --
        // the freshness guard must not run forever.
        let mut orphan = make_test_metadata("orphaned01", "p", 0);
        orphan.created_at = Utc::now() - Duration::minutes(30);
        orphan.status = Some(JobStatus::Running);
        orphan.save(temp_dir.path()).unwrap();
        assert_eq!(
            compute_status(&orphan, temp_dir.path()),
            JobStatus::SelfTerminated,
            "a stale pidless 'running' row must still classify from the log"
        );
    }

    #[test]
    fn test_compute_status_pid_without_start_time_uses_cmdline_fallback() {
        use std::process::Command;

        // Rows without a recorded pid_start_time must not trust the PID
        // blindly: the process must still look like a job process.
        /// Reaps the child in `Drop` (kill + wait) so both test processes are
        /// cleaned up even when an assertion in the middle of the test panics.
        struct ChildGuard(std::process::Child);

        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        /// Polls `/proc/<pid>/cmdline` until it starts with `prefix` (at most
        /// `timeout`, one check every 10 ms). Returns the observed cmdline.
        fn poll_cmdline_prefix(pid: u32, prefix: &str, timeout: std::time::Duration) -> String {
            let deadline = std::time::Instant::now() + timeout;
            loop {
                if let Ok(bytes) = std::fs::read(format!("/proc/{}/cmdline", pid)) {
                    let s = String::from_utf8_lossy(&bytes).to_string();
                    if s.starts_with(prefix) {
                        return s;
                    }
                }
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "timed out waiting for /proc/{}/cmdline to start with '{}'",
                        pid, prefix
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        //
        // `exec -a` is a bash-ism (the `sh` on this system is dash, which
        // dies immediately on it), so the renamed process must be spawned
        // with bash explicitly; the children live behind `ChildGuard`s so
        // they are killed and reaped on any early panic.
        let job_like = ChildGuard(
            Command::new("sleep")
                .arg("30")
                .spawn()
                .expect("spawn sleep"),
        );
        let not_a_job = ChildGuard(
            Command::new("bash")
                .arg("-c")
                .arg("exec -a zzz sleep 30")
                .spawn()
                .expect("spawn renamed sleep"),
        );

        let sleep_pid = job_like.0.id();
        let zzz_pid = not_a_job.0.id();

        // Wait until both children's cmdlines are actually observable
        // (bash may briefly exist before the exec replaces it).
        let sleep_cmd = poll_cmdline_prefix(sleep_pid, "sleep", std::time::Duration::from_secs(5));
        let zzz_cmd = poll_cmdline_prefix(zzz_pid, "zzz", std::time::Duration::from_secs(5));
        assert!(
            sleep_cmd.contains("sleep"),
            "test process 'sleep 30' cmdline must be observable: {:?}",
            sleep_cmd
        );
        assert!(
            zzz_cmd.starts_with("zzz"),
            "renamed process cmdline must start with 'zzz': {:?}",
            zzz_cmd
        );

        assert!(
            is_job_process_cmdline(sleep_pid),
            "test process 'sleep 30' must match the job-process pattern"
        );
        assert!(
            !is_job_process_cmdline(zzz_pid),
            "a renamed process (cmdline 'zzz 30') must not match"
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("nofbst01", "p", 0);
        meta.pid = Some(sleep_pid);
        meta.pid_start_time = None;
        meta.save(temp_dir.path()).unwrap();
        assert_eq!(
            compute_status(&meta, temp_dir.path()),
            JobStatus::Running,
            "plausible cmdline with unknown start time still counts as Running"
        );

        let mut other = make_test_metadata("nofbst02", "p", 0);
        other.pid = Some(zzz_pid);
        other.pid_start_time = None;
        other.save(temp_dir.path()).unwrap();
        assert_eq!(
            compute_status(&other, temp_dir.path()),
            JobStatus::SelfTerminated,
            "a reused PID whose cmdline no longer looks like a job must not report Running"
        );
    }

    #[test]
    fn test_max_runtime_precedence_and_deadline() {
        use chrono::Duration;
        // Parsing goes through the shared duration syntax (same as wait --timeout).
        let limit: Duration = crate::core::commands::gc::parse_duration_string("90s").unwrap();
        assert_eq!(limit, Duration::seconds(90));

        // Precedence: per-job wins over the server default; either one alone
        // is used; neither set -> no limit (the default state).
        assert_eq!(
            effective_max_runtime(Some(limit), Some(Duration::days(1))),
            Some(limit)
        );
        assert_eq!(effective_max_runtime(None, Some(limit)), Some(limit));
        assert_eq!(effective_max_runtime(None, None), None);

        let start = Utc::now();
        // With a limit and a start time: the deadline is start + limit.
        assert_eq!(
            max_runtime_deadline(Some(start), Some(limit)),
            Some(start + limit)
        );
        // No limit -> never expires, regardless of start time.
        assert_eq!(max_runtime_deadline(Some(start), None), None);
        // No recorded start time -> never expires, regardless of limit.
        assert_eq!(max_runtime_deadline(None, Some(limit)), None);

        // Not yet expired (90s limit, 45s in)...
        assert!(!max_runtime_exceeded(
            Some(start),
            Some(limit),
            start + Duration::seconds(45)
        ));
        // ...at the deadline...
        assert!(max_runtime_exceeded(
            Some(start),
            Some(limit),
            start + limit
        ));
        // ...and no limit / no start never expire.
        assert!(!max_runtime_exceeded(
            Some(start),
            None,
            start + Duration::days(3650)
        ));
        assert!(!max_runtime_exceeded(
            None,
            Some(limit),
            start + Duration::days(3650)
        ));

        // Remaining time counts down to (but never below) zero.
        assert_eq!(
            max_runtime_remaining(Some(start), Some(limit), start),
            Some(limit)
        );
        assert_eq!(
            max_runtime_remaining(Some(start), Some(limit), start + Duration::seconds(45)),
            Some(Duration::seconds(45))
        );
        assert_eq!(
            max_runtime_remaining(Some(start), Some(limit), start + limit),
            None
        );
        assert_eq!(max_runtime_remaining(Some(start), None, start), None);
        assert_eq!(max_runtime_remaining(None, Some(limit), start), None);
    }

    #[test]
    fn test_max_runtime_kill_due_decision() {
        use chrono::Duration;
        let limit = Duration::minutes(2);
        let start = Utc::now();

        // A running job with a limit, past its deadline: kill it.
        assert!(max_runtime_kill_due(
            JobStatus::Running,
            Some(start),
            Some(limit),
            start + limit
        ));
        assert!(max_runtime_kill_due(
            JobStatus::Running,
            Some(start),
            Some(limit),
            start + limit + Duration::seconds(30)
        ));

        // A running job that has NOT yet hit the deadline: no kill.
        assert!(!max_runtime_kill_due(
            JobStatus::Running,
            Some(start),
            Some(limit),
            start + Duration::seconds(30)
        ));

        // No limit (None): never killed, no matter how old -- unbounded is
        // exactly today's behavior and must never produce a kill.
        assert!(!max_runtime_kill_due(
            JobStatus::Running,
            Some(start),
            None,
            start + Duration::days(3650)
        ));

        // No recorded start time: never killed even with a limit.
        assert!(!max_runtime_kill_due(
            JobStatus::Running,
            None,
            Some(limit),
            start + Duration::days(3650)
        ));

        // A terminal / non-running job is NEVER killed, even past its deadline:
        // it already has a final status and the enforcer must not touch it.
        assert!(!max_runtime_kill_due(
            JobStatus::Killed,
            Some(start),
            Some(limit),
            start + limit + Duration::hours(1)
        ));
        assert!(!max_runtime_kill_due(
            JobStatus::Done,
            Some(start),
            Some(limit),
            start + limit + Duration::hours(1)
        ));
        assert!(!max_runtime_kill_due(
            JobStatus::Failed,
            Some(start),
            Some(limit),
            start + limit + Duration::hours(1)
        ));
    }

    #[test]
    fn test_max_runtime_metadata_roundtrip() {
        use chrono::Duration;
        let temp_dir = tempfile::tempdir().unwrap();
        let mut meta = make_test_metadata("mxbound1", "p", 0);
        meta.max_runtime = Some(Duration::minutes(5));
        meta.started_at = Some(Utc::now());
        meta.save(temp_dir.path()).unwrap();

        let reloaded = JobMetadata::load(temp_dir.path(), "mxbound1").unwrap();
        assert_eq!(reloaded.max_runtime, Some(Duration::minutes(5)));
        assert!(reloaded.started_at.is_some());

        // And unbounded fields round-trip as None (the default state).
        let mut unbounded = make_test_metadata("mxunbd01", "p", 0);
        unbounded.started_at = Some(Utc::now());
        unbounded.max_runtime = None;
        unbounded.save(temp_dir.path()).unwrap();
        let reloaded = JobMetadata::load(temp_dir.path(), "mxunbd01").unwrap();
        assert_eq!(reloaded.max_runtime, None);
        assert_eq!(
            reloaded.started_at.and_then(|t| t.timestamp_nanos_opt()),
            unbounded.started_at.and_then(|t| t.timestamp_nanos_opt())
        );
    }

    // ---------------------------------------------------------------------
    // claude engine classification (HL-71 slice S1): `compute_status` on a
    // claude row goes through `determine_status_from_claude`, whose rules are
    // unit-tested here on stream-json log shapes (the black-box suite T172+
    // drives the same paths end to end through the HTTP/CLI surface).
    // ---------------------------------------------------------------------

    /// The canonical claude stream-json terminal shapes the fake harness
    /// emits (see `tests/cli/fake-claude.sh`): a non-error `result` line
    /// carries `subtype` + `result`; an error `result` carries
    /// `subtype:"error"` + `error`; a quota failure surfaces the upstream text
    /// on the error line.
    const CLAUDE_RESULT_OK: &str = r#"{"type":"result","subtype":"success","is_error":false,"result":"CLAUDE-DONE","session_id":"cl1"}"#;
    const CLAUDE_RESULT_ERR: &str =
        r#"{"type":"result","subtype":"error","is_error":true,"error":"boom","session_id":"cl1"}"#;
    const CLAUDE_SYSTEM_ERR: &str =
        r#"{"type":"system","subtype":"error","message":"boom","session_id":"cl1"}"#;
    const CLAUDE_QUOTA: &str = r#"{"type":"result","subtype":"error","is_error":true,"error":"upstream 429 resource_exhausted: rate limit exceeded","session_id":"cl1"}"#;

    #[test]
    fn test_determine_status_from_claude_done() {
        // A non-error terminal `result` is Done regardless of the exit code.
        assert_eq!(
            determine_status_from_claude(Some(0), CLAUDE_RESULT_OK),
            JobStatus::Done
        );
        assert_eq!(
            determine_status_from_claude(Some(1), CLAUDE_RESULT_OK),
            JobStatus::Done,
            "a success result line outranks a stray non-zero exit"
        );
    }

    #[test]
    fn test_determine_status_from_claude_failed() {
        // An error `result` line is Failed for any exit code, and so is a
        // `system`/error line.
        assert_eq!(
            determine_status_from_claude(Some(1), CLAUDE_RESULT_ERR),
            JobStatus::Failed
        );
        assert_eq!(
            determine_status_from_claude(Some(0), CLAUDE_RESULT_ERR),
            JobStatus::Failed,
            "an error result line is Failed regardless of exit code"
        );
        assert_eq!(
            determine_status_from_claude(None, CLAUDE_SYSTEM_ERR),
            JobStatus::Failed
        );
    }

    #[test]
    fn test_determine_status_from_claude_quota() {
        // A quota/rate-limit signal on the terminal error line wins over the
        // plain Failed classification.
        assert_eq!(
            determine_status_from_claude(Some(1), CLAUDE_QUOTA),
            JobStatus::QuotaExhausted
        );
    }

    #[test]
    fn test_determine_status_from_claude_killed() {
        // A kill (137/143) with no error line is SelfTerminated, mirroring
        // the agy taxonomy: `kill` marks the row Killed via mark_job_killed,
        // but the signal-based fall-through must agree.
        assert_eq!(
            determine_status_from_claude(Some(137), ""),
            JobStatus::SelfTerminated
        );
        assert_eq!(
            determine_status_from_claude(Some(143), ""),
            JobStatus::SelfTerminated
        );
        // A kill whose log DOES show an error line is Failed, not
        // SelfTerminated.
        assert_eq!(
            determine_status_from_claude(Some(137), CLAUDE_RESULT_ERR),
            JobStatus::Failed
        );
    }

    #[test]
    fn test_determine_status_from_claude_incomplete_vs_self_terminated() {
        // Exit 0 with no terminal success line is Incomplete.
        assert_eq!(
            determine_status_from_claude(Some(0), "no result line here"),
            JobStatus::Incomplete
        );
        // No exit code and no error line is SelfTerminated (cut short).
        assert_eq!(
            determine_status_from_claude(None, ""),
            JobStatus::SelfTerminated
        );
        // Non-zero exit (non-kill signal) with no error line is Failed.
        assert_eq!(determine_status_from_claude(Some(3), ""), JobStatus::Failed);
    }

    #[test]
    fn test_determine_status_from_claude_rejects_stale_attempt() {
        // A terminal `result` from a superseded attempt must not classify the
        // current one (last_attempt scoping, as in the agy scan).
        let stale_then_fail = format!(
            "{}\n==================== RETRY AT 2024-01-01T00:00:00Z ====================\n{}",
            CLAUDE_RESULT_OK, CLAUDE_RESULT_ERR
        );
        assert_eq!(
            determine_status_from_claude(Some(1), &stale_then_fail),
            JobStatus::Failed,
            "a stale success from a previous attempt must not win over the latest error"
        );
        let stale_err_then_ok = format!(
            "{}\n==================== RETRY AT 2024-01-01T00:00:00Z ====================\n{}",
            CLAUDE_RESULT_ERR, CLAUDE_RESULT_OK
        );
        assert_eq!(
            determine_status_from_claude(Some(0), &stale_err_then_ok),
            JobStatus::Done,
            "a stale error from a previous attempt must not override the latest success"
        );
    }

    #[test]
    fn test_claude_structured_text_success() {
        // The `result` command surfaces the text payload: a success `result`
        // line yields its `result` text as Success.
        let (status, text) = claude_structured_text(CLAUDE_RESULT_OK).unwrap();
        assert_eq!(status, StructuredStatus::Success);
        assert_eq!(text, "CLAUDE-DONE");
        assert!(
            claude_structured_text(
                r#"{"type":"assistant","content":[{"type":"text","text":"mid"}]}"#
            )
            .is_none(),
            "assistant lines are not terminal result lines"
        );
    }

    #[test]
    fn test_claude_structured_text_error() {
        // An error `result` line yields its `error` text as Error; a
        // `system`/error line yields its `message`.
        let (status, text) = claude_structured_text(CLAUDE_RESULT_ERR).unwrap();
        assert_eq!(status, StructuredStatus::Error);
        assert_eq!(text, "boom");
        let (status, text) = claude_structured_text(CLAUDE_SYSTEM_ERR).unwrap();
        assert_eq!(status, StructuredStatus::Error);
        assert_eq!(text, "boom");
    }

    #[test]
    fn test_claude_structured_text_error_carries_result_field() {
        // Defect 2 (HL-71 S1): the real claude error shape carries the reason
        // in the `result` field with NO `error` field at all (an
        // authentication/session failure, e.g. "Failed to authenticate: OAuth
        // session expired ..."). The text surfacing must prefer `result` so
        // `agent-run result` reports the reason instead of the empty string.
        let authfail = r#"{"type":"result","subtype":"error","is_error":true,"result":"Failed to authenticate: OAuth session expired and could not be refreshed","session_id":"cl1"}"#;
        let (status, text) = claude_structured_text(authfail).unwrap();
        assert_eq!(status, StructuredStatus::Error);
        assert_eq!(
            text,
            "Failed to authenticate: OAuth session expired and could not be refreshed"
        );
        assert_eq!(
            determine_status_from_claude(Some(1), authfail),
            JobStatus::Failed,
            "the auth/expired-session error shape must classify the job failed"
        );

        // An error `result` that carries BOTH fields keeps the `result` text
        // (it is the run's terminal message) over `error`.
        let both = r#"{"type":"result","subtype":"error","is_error":true,"result":"model no-such-model-xyz is not recognized","error":"x","session_id":"cl1"}"#;
        let (status, text) = claude_structured_text(both).unwrap();
        assert_eq!(status, StructuredStatus::Error);
        assert_eq!(text, "model no-such-model-xyz is not recognized");

        // A blank `result` on an error line falls back to `error` (unchanged
        // behaviour).
        let blank = r#"{"type":"result","subtype":"error","is_error":true,"result":"","error":"boom","session_id":"cl1"}"#;
        let (_, text) = claude_structured_text(blank).unwrap();
        assert_eq!(text, "boom");
    }

    #[test]
    fn test_extract_final_response_error_shapes() {
        // The full failed-claude log from the live bug: a synthetic assistant
        // message (model "<synthetic>") carrying the reason text, a
        // `[claude-code:...]` log line, and a final error `result` whose
        // `result` field holds the same text. `extract_final_response` must
        // surface that text (so `agent-run result --json` reports it), and the
        // job must classify `failed`.
        let authfail_log = r#"[claude-code:unrecognized_model] Failed to authenticate: OAuth session expired and could not be refreshed
{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"Failed to authenticate: OAuth session expired and could not be refreshed"}]},"session_id":"cl1"}
{"type":"result","subtype":"error","is_error":true,"result":"Failed to authenticate: OAuth session expired and could not be refreshed","session_id":"cl1"}
"#;
        assert_eq!(
            extract_final_response(authfail_log).as_deref(),
            Some("Failed to authenticate: OAuth session expired and could not be refreshed"),
            "the auth-failure reason must be surfaced from the error result"
        );
        assert_eq!(
            determine_status_from_claude(Some(1), authfail_log),
            JobStatus::Failed,
            "the auth-failure log must classify the job failed"
        );

        // The unrecognized-model variant (same shape, different message).
        let model_log = r#"[claude-code:unrecognized_model] Error: model not recognized
{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"Error: model not recognized"}]},"session_id":"cl1"}
{"type":"result","subtype":"error","is_error":true,"result":"Error: model not recognized","session_id":"cl1"}
"#;
        assert_eq!(
            extract_final_response(model_log).as_deref(),
            Some("Error: model not recognized")
        );
        assert_eq!(
            determine_status_from_claude(Some(1), model_log),
            JobStatus::Failed
        );
    }

    #[test]
    fn test_claude_fallback_error_text_chain() {
        // Defect 2 fallback chain (_HL-71 S1_). When a claude job fails but
        // the log has NO terminal error `result` line, the error reason must
        // still surface: link 1 = the last synthetic `assistant` message text,
        // link 2 = the last non-JSON log line. A non-claude log must NOT match
        // (an agy tail of plain chatter is not an error reason).

        // Link 1: only the synthetic assistant message carries the reason; no
        // agent_error `result` line at all.
        let synthetic_only = r#"[claude-code:unrecognized_model] Failed to authenticate: OAuth session expired
{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"Failed to authenticate: OAuth session expired and could not be refreshed"}]},"session_id":"cl1"}
"#;
        assert_eq!(
            claude_fallback_error_text(synthetic_only).as_deref(),
            Some("Failed to authenticate: OAuth session expired and could not be refreshed"),
            "synthetic assistant text must be the reason fallback"
        );

        // Link 1 must pick the LAST synthetic message when several exist.
        let multi = r#"{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"first reason"}]},"session_id":"cl1"}
{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"final reason"}]},"session_id":"cl1"}
"#;
        assert_eq!(
            claude_fallback_error_text(multi).as_deref(),
            Some("final reason"),
            "the last synthetic assistant text wins"
        );

        // Link 2: claude-shaped log (a `[claude-code:...]` marker), no
        // synthetic message -> the last non-JSON line is the reason. The
        // JSON event itself is excluded.
        let marker_only = "[claude-code:unrecognized_model] Error: model not recognized\n{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"x\",\"session_id\":\"cl1\"}\n";
        assert_eq!(
            claude_fallback_error_text(marker_only).as_deref(),
            Some("[claude-code:unrecognized_model] Error: model not recognized"),
            "the last non-JSON line is the marker text"
        );

        // A claude log whose last non-JSON line is chatter still surfaces that
        // line (no better signal; the job did fail).
        let chatter = "some plain agent chatter\n{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"ok\",\"session_id\":\"cl1\"}\n";
        assert_eq!(
            claude_fallback_error_text(chatter).as_deref(),
            Some("some plain agent chatter")
        );

        // Non-claude logs must never match: no marker and no claude JSON event
        // (an agy run, or pure chatter) -> None.
        let agy_tail = "agent chatter only\nno structured terminal line\n";
        assert_eq!(
            claude_fallback_error_text(agy_tail),
            None,
            "an agy-chatter tail must never be surfaced as a claude reason"
        );
        let agy_structured = r#"{"conversation_id":"c1","status":"SUCCESS","response":"done"}
"#;
        assert_eq!(
            claude_fallback_error_text(agy_structured),
            None,
            "an agy structured log must not match the claude fallback"
        );

        // The fallback respects the last-attempt boundary (a synthetic failed
        // message in a superseded attempt must not be surfaced).
        let retried = format!(
            "{}\n==================== RETRY AT 2026-01-01T00:00:00Z ====================\n",
            r#"{"type":"assistant","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"first attempt failed"}]},"session_id":"cl1"}"#
        );
        assert_eq!(
            claude_fallback_error_text(&retried),
            None,
            "a synthetic reason from a superseded attempt is not surfaced"
        );
        let retried2 = "first attempt chatter\n==================== RETRY AT 2026-01-01T00:00:00Z ====================\n[claude-code:unrecognized_model] Error: second attempt model\n".to_owned();
        assert_eq!(
            claude_fallback_error_text(&retried2).as_deref(),
            Some("[claude-code:unrecognized_model] Error: second attempt model"),
            "the last non-JSON line of the LAST attempt is the reason"
        );
    }

    #[test]
    fn test_compute_status_dispatches_on_engine_field() {
        // The recorded `engine` selects the status taxonomy whose stream the
        // job log carries (agy's classifier also recognises claude's
        // stream-json `result` lines via the shared `scan_final_structured`,
        // so the divergence is the OTHER direction): a claude row is
        // classified ONLY by claude terminal shapes, so an agy-shaped log on
        // a claude row is Incomplete, while the same log on an agy row is
        // Done. This proves `compute_status` dispatches on `meta.engine`.
        let temp_dir = tempfile::tempdir().unwrap();

        let agy_done = r#"{"conversation_id":"c1","status":"SUCCESS","response":"ok"}"#;

        let mut claude_meta = make_test_metadata("cledsp01", "proj", 0);
        claude_meta.engine = Some("claude".to_string());
        claude_meta.pid = None;
        claude_meta.exit_code = Some(0);
        claude_meta.status = None;
        claude_meta.save(temp_dir.path()).unwrap();
        std::fs::create_dir_all(JobMetadata::log_dir(temp_dir.path())).unwrap();
        let log_path = JobMetadata::log_path(temp_dir.path(), "cledsp01");
        std::fs::write(&log_path, agy_done).unwrap();
        assert_eq!(
            compute_status(&claude_meta, temp_dir.path()),
            JobStatus::Incomplete,
            "a claude row must not take the agy taxonomy (agy-shaped log -> Incomplete)"
        );

        // The same agy-shaped log on an agy row is Done (agy path unchanged).
        let mut agy_meta = make_test_metadata("agedsp01", "proj", 0);
        agy_meta.pid = None;
        agy_meta.exit_code = Some(0);
        agy_meta.status = None;
        agy_meta.save(temp_dir.path()).unwrap();
        let log_path = JobMetadata::log_path(temp_dir.path(), "agedsp01");
        std::fs::write(&log_path, agy_done).unwrap();
        assert_eq!(
            compute_status(&agy_meta, temp_dir.path()),
            JobStatus::Done,
            "an agy/default row keeps the agy taxonomy (agy-shaped log -> Done)"
        );

        // And the claude taxonomy's own happy path (a claude stream-json
        // success on a claude row) is Done.
        let mut claude_meta2 = make_test_metadata("cledsp02", "proj", 0);
        claude_meta2.engine = Some("claude".to_string());
        claude_meta2.pid = None;
        claude_meta2.exit_code = Some(0);
        claude_meta2.status = None;
        claude_meta2.save(temp_dir.path()).unwrap();
        let log_path = JobMetadata::log_path(temp_dir.path(), "cledsp02");
        std::fs::write(&log_path, CLAUDE_RESULT_OK).unwrap();
        assert_eq!(
            compute_status(&claude_meta2, temp_dir.path()),
            JobStatus::Done,
            "a claude row with a claude stream-json success is Done"
        );
    }

    #[test]
    fn concurrent_opens_of_a_fresh_state_db_all_succeed() {
        // Regression (HL-113 gates): several openings of a brand-new
        // state.sqlite at the same time (parallel tests, or the worker and a
        // CLI) raced the first `PRAGMA journal_mode = WAL` on the fresh file
        // and one of them failed with "database is locked" (SQLITE_BUSY).
        // The busy timeout set BEFORE the pragma plus the bounded retry on
        // busy/locked must make every opener succeed: each open runs the
        // (idempotent) migrations on first use and returns an immediately
        // usable connection.
        let temp = tempfile::tempdir().unwrap();
        let state_root = temp.path().join("s");
        let mut handles = Vec::new();
        for _ in 0..8 {
            let root = state_root.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..30 {
                    let conn = open_state_db(&root)
                        .expect("concurrent open of a fresh state db must succeed");
                    // The connection must be usable: migrations ran, the
                    // jobs table exists, WAL is active.
                    let wal: String = conn
                        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
                        .expect("read journal_mode");
                    assert_eq!(wal, "wal", "WAL journal mode must be enabled");
                    let n: i64 = conn
                        .query_row("SELECT count(*) FROM jobs", [], |r| r.get(0))
                        .expect("jobs table must exist after a concurrent open");
                    assert_eq!(n, 0);
                }
            }));
        }
        for h in handles {
            h.join().expect("concurrent open thread panicked");
        }
    }
}

//! Read-only "is this RUNNING job actually working" signals, built from the
//! job log mtime, a bounded scan of the job worktree, and the job's live
//! child processes (`/proc`).
//!
//! Everything here is display-only: the caller (the `status`/`list`
//! commands) embeds the results as OPTIONAL JSON fields and human lines for
//! `running` jobs. Nothing in this module ever signals, kills, marks failed,
//! or mutates a job row, and an unavailable `/proc` degrades to `None`
//! (unknown) rather than an error.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

/// Traversal budget for [`scan_worktree`]: a scan stops descending (and stops
/// reading further directory entries) once this many entries have been
/// counted or [`WORKTREE_SCAN_TIME_CAP`] has elapsed, so a huge worktree with
/// vendored/dependency trees stays cheap. 4096 entries cover a typical job
/// worktree at a bounded cost.
pub const WORKTREE_SCAN_BUDGET: u32 = 4096;

/// Wall-clock cap for [`scan_worktree`]: a scan is strictly bounded in time
/// as well as in entry count, so a pathologically slow filesystem cannot make
/// a `status` call hang.
pub const WORKTREE_SCAN_TIME_CAP: Duration = Duration::from_millis(200);

/// Directory names skipped by [`scan_worktree`] (any depth): git internals.
pub const EXCLUDED_DIR_GIT: &str = ".git";
/// Directory names skipped by [`scan_worktree`] (any depth): build output.
pub const EXCLUDED_DIR_TARGET: &str = "target";

/// Directory names skipped by [`scan_worktree`] at any depth: VCS internals,
/// build output, and dependency/derived trees whose churn carries no activity
/// signal. Checking entry names against this list keeps the traversal itself
/// bounded on trees whose size is dominated by dependencies.
pub const EXCLUDED_SCAN_DIRS: &[&str] = &[
    EXCLUDED_DIR_GIT,
    EXCLUDED_DIR_TARGET,
    "node_modules",
    ".cache",
    ".venv",
    "__pycache__",
    "dist-newstyle",
    ".gradle",
];

/// Max length (in chars) of the newest child command line reported by
/// [`live_children`].
pub const CMDLINE_TRUNCATE: usize = 80;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorktreeScan {
    pub newest: Option<SystemTime>,
    pub entries_scanned: u32,
    /// `true` when the traversal budget (entry count or wall-clock cap) was
    /// exhausted before the scan completed; kept from the pre-traversal-budget
    /// API (whose meaning was "entry-count budget reached").
    pub hit_budget: bool,
    /// `true` when the traversal budget (entry count or wall-clock cap) was
    /// exhausted: the scan stopped descending, so `newest` covers only the
    /// part of the tree that was walked and is therefore an approximation.
    pub worktree_newest_approx: bool,
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    let m = std::fs::metadata(path).ok()?;
    m.modified().ok()
}

/// Newest mtime of files under `root`, bounded to `budget` entries, skipping
/// entries whose names are in [`EXCLUDED_SCAN_DIRS`] at any depth (`.git`,
/// `target`, and the dependency/derived trees listed there; this also keeps
/// the `.git` FILE of a `git worktree` checkout out of the mtime race, as
/// before).
/// Returns `None` when `root` does not exist or is not a directory.
///
/// The budget is a TRAVERSAL budget: the walk is iterative (explicit stack,
/// no recursion) and never follows symlinked directories, and once the
/// budget is exhausted -- either `budget` entries have been counted or
/// [`WORKTREE_SCAN_TIME_CAP`] has elapsed relative to the scan start -- the
/// scan stops descending AND stops reading further directory entries. The
/// returned `newest` mtime therefore covers only the part of the tree that
/// was walked; [`WorktreeScan::worktree_newest_approx`] is `true` in that
/// case so callers can present it as an approximation.
pub fn scan_worktree(root: &Path, budget: u32) -> Option<WorktreeScan> {
    if !root.is_dir() {
        return None;
    }
    let started = Instant::now();
    let mut newest: Option<SystemTime> = None;
    let mut entries: u32 = 0;
    let mut hit_budget = false;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        // Strict bound: stop descending (and stop reading any further
        // directory entries) once the entry budget is exhausted or the
        // wall-clock cap has elapsed. Queued directories are intentionally
        // left unvisited: the scan must stay cheap no matter what is queued.
        if entries >= budget || started.elapsed() >= WORKTREE_SCAN_TIME_CAP {
            hit_budget = true;
            break;
        }
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            // The budget is a TRAVERSAL budget: once exhausted we stop reading
            // the rest of this directory's entries too (a `read_dir` is
            // materialised lazily, so this `break` is what actually stops the
            // reads, not just the descent).
            if entries >= budget || started.elapsed() >= WORKTREE_SCAN_TIME_CAP {
                hit_budget = true;
                break;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if EXCLUDED_SCAN_DIRS.contains(&name.as_ref()) {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                // Do not follow symlinked directories (a symlink loop in a
                // worktree must not hang or explode the scan).
                if !path.is_symlink() {
                    stack.push(path);
                }
                continue;
            }
            // Only regular files count toward the entry budget: directories
            // are traversal containers, not content that signals activity.
            if !hit_budget {
                entries = entries.saturating_add(1);
                if entries >= budget {
                    hit_budget = true;
                }
            }
            if let Some(m) = file_mtime(&path) {
                newest = Some(match newest {
                    Some(n) if n >= m => n,
                    _ => m,
                });
            }
        }
    }
    Some(WorktreeScan {
        newest,
        entries_scanned: entries,
        hit_budget,
        worktree_newest_approx: hit_budget,
    })
}

/// Newest mtime of a single file (`None` when it is missing/unreadable).
pub fn log_mtime(path: &Path) -> Option<SystemTime> {
    file_mtime(path)
}

/// Parse one `/proc/<pid>/stat` line into `(pid, ppid, state, start_ticks)`.
/// The comm field is parenthesized and may contain spaces/parens, so the
/// fields after it start after the LAST `)` (same approach the crate already
/// uses in `job::get_process_start_time`).
pub fn parse_proc_stat(line: &str) -> Option<(u32, u32, char, u64)> {
    let open = line.find('(')?;
    let close = line.rfind(')')?;
    if close <= open + 1 {
        return None;
    }
    let before = line.get(..open)?;
    let pid: u32 = before.trim().parse().ok()?;
    let rest = line.get(close + 1..)?.trim_start();
    let mut f = rest.split_whitespace();
    let state = f.next()?.chars().next()?;
    let ppid: u32 = f.next()?.parse().ok()?;
    // Fields after the state: ppid(4) pgrp(5) session(6) tty_nr(7) tpgid(8)
    // flags(9) minflt(10) cminflt(11) majflt(12) cmajflt(13) utime(14)
    // stime(15) cutime(16) cstime(17) priority(18) nice(19) num_threads(20)
    // itrealvalue(21) starttime(22).
    for _ in 0..17 {
        f.next()?;
    }
    let start_ticks: u64 = f.next()?.parse().ok()?;
    Some((pid, ppid, state, start_ticks))
}

/// Build a `pid -> (ppid, state, start ticks)` map from a `/proc`-like root
/// (the parent dir holding numeric `<pid>/stat` files). Returns `None` when
/// the root cannot be read (e.g. no `/proc` on the platform).
pub fn proc_snapshot_in(proc_root: &Path) -> Option<BTreeMap<u32, (u32, char, u64)>> {
    let read = std::fs::read_dir(proc_root).ok()?;
    let mut map = BTreeMap::new();
    for entry in read.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let pid: u32 = match name.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let Ok(content) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        if let Some((_, ppid, state, ticks)) = parse_proc_stat(&content) {
            map.insert(pid, (ppid, state, ticks));
        }
    }
    Some(map)
}

/// Truncate a command line (bytes read from `/proc/<pid>/cmdline`, NUL-
/// separated) to at most `CMDLINE_TRUNCATE` chars.
pub fn truncate_cmdline(raw: &str, max_chars: usize) -> String {
    let mut s: String = raw.chars().take(max_chars).collect();
    if raw.chars().count() > max_chars {
        s.push('…');
    }
    s
}

/// Live (non-zombie) descendants of `job_pid`, found through a
/// `/proc`-like root: `(count, newest child's command line)`. "Newest" is the
/// descendant with the largest start time (start ticks from its `stat`).
/// Returns `None` when the `/proc` root is unavailable; an unknown or already
/// gone `job_pid` yields `Some(0, None)`.
pub fn live_children_in(job_pid: u32, proc_root: &Path) -> Option<(u32, Option<String>)> {
    let snap = proc_snapshot_in(proc_root)?;
    let mut children_of: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for (&pid, &(ppid, _, _)) in &snap {
        children_of.entry(ppid).or_default().push(pid);
    }
    let mut queue = VecDeque::new();
    let mut seen = BTreeSet::new();
    for &c in children_of.get(&job_pid).into_iter().flatten() {
        if seen.insert(c) {
            queue.push_back(c);
        }
    }
    let mut count: u32 = 0;
    let mut newest: Option<(u64, u32)> = None;
    while let Some(pid) = queue.pop_front() {
        let entry = &snap[&pid];
        if entry.1 != 'Z' {
            count = count.saturating_add(1);
            if newest.is_none_or(|(t, _)| entry.2 > t) {
                newest = Some((entry.2, pid));
            }
        }
        for &c in children_of.get(&pid).into_iter().flatten() {
            if seen.insert(c) {
                queue.push_back(c);
            }
        }
    }
    let cmdline = newest.and_then(|(_, pid)| {
        let raw = std::fs::read(proc_root.join(pid.to_string()).join("cmdline")).ok()?;
        let text: Vec<String> = raw
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect();
        Some(truncate_cmdline(&text.join(" "), CMDLINE_TRUNCATE))
    });
    Some((count, cmdline))
}

/// Live descendants from the real `/proc` (degrades to `None` off-Linux).
#[cfg(unix)]
pub fn live_children(job_pid: u32) -> Option<(u32, Option<String>)> {
    live_children_in(job_pid, Path::new("/proc"))
}

#[cfg(not(unix))]
pub fn live_children(_job_pid: u32) -> Option<(u32, Option<String>)> {
    None
}

#[derive(Debug, Clone)]
pub struct Activity {
    pub last_activity: Option<SystemTime>,
    /// `"log"` | `"worktree"` | `"job start"` (the job's `created_at` is used
    /// when neither the log nor any worktree file has an mtime), or `None`
    /// when no anchor exists at all.
    pub last_activity_source: Option<&'static str>,
    pub worktree_entries_scanned: u32,
    pub worktree_hit_budget: bool,
    /// `true` when `last_activity_source` is `"worktree"` and the scan was
    /// cut short by the traversal budget: `last_activity` is then the newest
    /// mtime found SO FAR, flagged approximate.
    pub worktree_newest_approx: bool,
    pub child_count: Option<u32>,
    pub newest_child_cmdline: Option<String>,
}

/// Combine the job log mtime and the bounded worktree scan into a single
/// "last activity" signal plus the live-child summary. `fallback_start` is
/// the job's `created_at`, used only when neither source produced an mtime.
/// Never fails: every missing piece degrades to `None`.
pub fn collect_activity(
    log_path: &Path,
    worktree_path: &Path,
    job_pid: Option<u32>,
    fallback_start: SystemTime,
) -> Activity {
    let log_m = log_mtime(log_path);
    let worktree = scan_worktree(worktree_path, WORKTREE_SCAN_BUDGET);
    let children = job_pid.and_then(live_children);

    let (last, source) = match (log_m, worktree.as_ref().and_then(|w| w.newest)) {
        (Some(l), Some(w)) => {
            if w > l {
                (Some(w), Some("worktree"))
            } else {
                (Some(l), Some("log"))
            }
        }
        (Some(l), None) => (Some(l), Some("log")),
        (None, Some(w)) => (Some(w), Some("worktree")),
        (None, None) => (Some(fallback_start), Some("job start")),
    };

    Activity {
        last_activity: last,
        last_activity_source: source,
        worktree_entries_scanned: worktree.as_ref().map_or(0, |w| w.entries_scanned),
        worktree_hit_budget: worktree.as_ref().is_some_and(|w| w.hit_budget),
        worktree_newest_approx: source == Some("worktree")
            && worktree.as_ref().is_some_and(|w| w.worktree_newest_approx),
        child_count: children.as_ref().map(|(c, _)| *c),
        newest_child_cmdline: children.as_ref().and_then(|(_, cmd)| cmd.clone()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StallAssessment {
    pub idle_secs: u64,
    pub possibly_stalled: bool,
}

/// Pure stall decision (injectable clock + threshold): the job looks stalled
/// when its last activity is at least `threshold` old AND it has NO live
/// child process. `child_count == None` means `/proc` was unavailable
/// (unknown), which is treated conservatively as "not stalled"; `false` is
/// therefore only informational and never mutates job state.
pub fn assess_stall(
    last_activity: Option<SystemTime>,
    fallback_start: SystemTime,
    child_count: Option<u32>,
    now: SystemTime,
    threshold: Duration,
) -> StallAssessment {
    let anchor = last_activity.unwrap_or(fallback_start);
    let idle = now
        .duration_since(anchor)
        .unwrap_or_else(|_| anchor.duration_since(now).unwrap_or_default());
    let idle_secs = idle.as_secs();
    let possibly_stalled = idle >= threshold && child_count == Some(0);
    StallAssessment {
        idle_secs,
        possibly_stalled,
    }
}

/// RFC3339-seconds (`...Z`) wall-clock rendering of a `SystemTime`.
pub fn system_time_to_rfc3339(t: SystemTime) -> Option<String> {
    let secs = t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64;
    let dt = chrono::DateTime::from_timestamp(secs, 0)?;
    Some(dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set a file's mtime (Linux, the platform this crate targets).
    fn set_mtime(path: &Path, secs: i64) {
        std::fs::write(path, b"x").unwrap();
        use std::os::unix::ffi::OsStrExt;
        let c: &[u8] = path.as_os_str().as_bytes();
        let c = std::ffi::CString::new(c).unwrap();
        let times = [libc::timespec {
            tv_sec: secs,
            tv_nsec: 0,
        }; 2];
        // utimensat (not `utimes`, which takes `timeval`) takes a `timespec`
        // pair and resolves the path against the current working directory.
        // SAFETY: raw FFI; `c` is a valid NUL-terminated path, `times` a
        // 2-element array valid for the duration of the call, and the path
        // exists (created by the `touch`/`set_mtime` caller above).
        let r = unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(r, 0, "utimensat failed for {}", path.display());
    }

    fn touch(path: &Path, secs: i64) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        set_mtime(path, secs);
    }

    #[test]
    fn scan_prefers_newest_file_across_subdirs() {
        let t = tempfile::tempdir().unwrap();
        touch(&t.path().join("a.txt"), 100);
        touch(&t.path().join("sub/old.txt"), 50);
        touch(&t.path().join("sub2/deep/new.txt"), 200);
        let scan = scan_worktree(t.path(), WORKTREE_SCAN_BUDGET).unwrap();
        assert_eq!(
            scan.newest,
            Some(std::time::UNIX_EPOCH + Duration::from_secs(200))
        );
        assert!(!scan.hit_budget);
        assert!(!scan.worktree_newest_approx);
        assert_eq!(scan.entries_scanned, 3);
    }

    #[test]
    fn scan_skips_git_and_target_dirs() {
        let t = tempfile::tempdir().unwrap();
        touch(&t.path().join("gitfile"), 100);
        // A bare repo's `.git` is a directory full of files; a worktree's is
        // a file. Both must be skipped so they never win the "newest" race.
        touch(&t.path().join(".git/HEAD"), 5000);
        touch(&t.path().join(".git/config"), 5000);
        touch(&t.path().join("target/release/bin"), 6000);
        touch(&t.path().join("target/deep/x/y"), 6000);
        // Dependency/derived trees: every excluded name at a few depths must
        // be pruned from the traversal, so their (newer) mtimes never win.
        touch(&t.path().join("node_modules/pkg/index.js"), 9000);
        touch(&t.path().join("src/node_modules/deep/more/a.js"), 9000);
        touch(&t.path().join(".venv/bin/python"), 9000);
        touch(&t.path().join("app/__pycache__/mod.cpython-312.pyc"), 9000);
        touch(&t.path().join("cache/.cache/pytest/v/cache"), 9000);
        touch(&t.path().join("dist-newstyle/build/x/lib"), 9000);
        touch(&t.path().join("build/.gradle/7/files.bin"), 9000);
        touch(&t.path().join("src/main.rs"), 300);
        let scan = scan_worktree(t.path(), WORKTREE_SCAN_BUDGET).unwrap();
        assert_eq!(
            scan.newest,
            Some(std::time::UNIX_EPOCH + Duration::from_secs(300)),
            "newest must not come from .git/, target/ or any other excluded dir"
        );
        assert!(!scan.hit_budget);
        assert!(!scan.worktree_newest_approx);
    }

    #[test]
    fn scan_stops_traversal_at_budget_and_marks_newest_approximate() {
        let t = tempfile::tempdir().unwrap();
        // More entries than a small budget. `read_dir` order is
        // OS-dependent, so with a traversal budget the first files seen are
        // an unknown subset -- the newest mtime is only approximate.
        for i in 0..50 {
            touch(&t.path().join(format!("f{i:02}.txt")), 1000 + i as i64);
        }
        let budget = 10;
        let scan = scan_worktree(t.path(), budget).unwrap();
        assert!(scan.hit_budget, "the entry budget must be reported as hit");
        assert!(
            scan.worktree_newest_approx,
            "a budget-stopped scan must flag its newest mtime as approximate"
        );
        assert_eq!(
            scan.entries_scanned, budget,
            "the scan counts exactly `budget` entries and then stops reading"
        );
        let secs = scan
            .newest
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .expect("some of the first files must have been mtime-checked");
        assert!(
            (1000..1050).contains(&secs),
            "newest is the mtime of one of the walked files, got {secs}"
        );
    }

    #[test]
    fn scan_budget_exhausted_is_approximate_and_does_not_hang() {
        let t = tempfile::tempdir().unwrap();
        // Deep tree with many more entries than the budget: the scan must
        // stop promptly (traversal budget), flag the result approximate, and
        // still report the newest mtime found so far.
        for i in 0..40 {
            touch(&t.path().join(format!("d{i}/deep/f.txt")), 2000 + i as i64);
        }
        let started = std::time::Instant::now();
        let scan = scan_worktree(t.path(), 5).unwrap();
        // Loose bound: on any plausible filesystem a 5-entry scan of a small
        // temp tree completes in milliseconds; 2 s only rules out a hang
        // (e.g. unbounded descent on a huge or symlink-loop-prone tree).
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "an exhausted budget must not make the scan hang"
        );
        assert!(scan.hit_budget);
        assert!(scan.worktree_newest_approx);
        assert_eq!(scan.entries_scanned, 5, "stopping is strict at the budget");
        assert!(
            scan.newest.is_some(),
            "the newest mtime found so far must be reported as approximate, not None"
        );
    }

    #[test]
    fn scan_missing_or_non_dir_root_is_none() {
        let t = tempfile::tempdir().unwrap();
        assert!(scan_worktree(&t.path().join("nope"), WORKTREE_SCAN_BUDGET).is_none());
        let f = t.path().join("file");
        std::fs::write(&f, b"x").unwrap();
        assert!(scan_worktree(&f, WORKTREE_SCAN_BUDGET).is_none());
    }

    #[test]
    fn collect_activity_prefers_newer_of_log_and_worktree() {
        let t = tempfile::tempdir().unwrap();
        let log = t.path().join("job.log");
        touch(&log, 100);
        touch(&t.path().join("wt/file.rs"), 300);
        let anchor = std::time::UNIX_EPOCH + Duration::from_secs(1);
        let act = collect_activity(&log, &t.path().join("wt"), None, anchor);
        assert_eq!(
            act.last_activity,
            Some(std::time::UNIX_EPOCH + Duration::from_secs(300))
        );
        assert_eq!(act.last_activity_source, Some("worktree"));
        assert!(
            !act.worktree_newest_approx,
            "a scan under budget is not approximate"
        );

        touch(&log, 400);
        let act = collect_activity(&log, &t.path().join("wt"), None, anchor);
        assert_eq!(
            act.last_activity,
            Some(std::time::UNIX_EPOCH + Duration::from_secs(400))
        );
        assert_eq!(act.last_activity_source, Some("log"));
    }

    #[test]
    fn collect_activity_missing_worktree_still_reports_log() {
        let t = tempfile::tempdir().unwrap();
        let log = t.path().join("job.log");
        touch(&log, 100);
        let anchor = std::time::UNIX_EPOCH + Duration::from_secs(1);
        let act = collect_activity(&log, &t.path().join("missing-wt"), None, anchor);
        assert_eq!(
            act.last_activity,
            Some(std::time::UNIX_EPOCH + Duration::from_secs(100))
        );
        assert_eq!(act.last_activity_source, Some("log"));
        assert_eq!(act.worktree_entries_scanned, 0);
        assert!(!act.worktree_hit_budget);
        assert_eq!(act.child_count, None);
    }

    #[test]
    fn collect_activity_no_sources_falls_back_to_job_start() {
        let t = tempfile::tempdir().unwrap();
        let anchor = std::time::UNIX_EPOCH + Duration::from_secs(42);
        let act = collect_activity(
            &t.path().join("absent.log"),
            &t.path().join("absent-wt"),
            None,
            anchor,
        );
        assert_eq!(act.last_activity, Some(anchor));
        assert_eq!(act.last_activity_source, Some("job start"));
    }

    #[test]
    fn live_children_counts_live_descendants_and_nearest_cmdline() {
        // `sh` spawns `sleep`; after sh exits, sleep is reparented upward --
        // so run the check while sh is still alive: sh is the child, sleep is
        // the grandchild.
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg("sleep 30")
            .spawn()
            .unwrap();
        let pid = child.id();
        // Poll (up to ~5 s in 25 ms steps) until sh has actually forked its
        // `sleep` child: a fixed short sleep is race-prone on a loaded box,
        // and the whole point of the test is to count live descendants.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some((count, _)) = live_children(pid) {
                if count >= 1 {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "`sh -c 'sleep 30'` did not show a live descendant within 5 s"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let (count, cmdline) = live_children(pid).expect("/proc unavailable in test env");
        assert!(
            count >= 1,
            "expected at least one live descendant, got {count}"
        );
        assert!(
            cmdline.as_deref().is_none_or(|c| c.contains("sleep")),
            "newest descendant cmdline should be the sleep, got {cmdline:?}"
        );
        // Clean up: kill the child, then reap what we own (its `sleep`
        // grandchild is reaped by the test process once `sh` dies).
        // SAFETY: pid is our own child's pid.
        let _ = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        let _ = child.wait();
    }

    #[test]
    fn live_children_unknown_pid_is_zero() {
        // A pid that does not exist yields an empty descendant set, not an
        // error (None is reserved for "no /proc at all").
        let (count, cmdline) = live_children_in(999_999_999, Path::new("/proc"))
            .expect("/proc unavailable in test env");
        assert_eq!(count, 0);
        assert_eq!(cmdline, None);
    }

    #[test]
    fn live_children_synthetic_proc_root() {
        let t = tempfile::tempdir().unwrap();
        // Process tree: 100 (job) -> 101 -> 102 (live), 101 has zombie child 103.
        // Also an unrelated process 200 (not a descendant).
        let mk = |pid: u32, ppid: u32, state: char, ticks: u64, cmdline: &[u8]| {
            let d = t.path().join(pid.to_string());
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(
                d.join("stat"),
                format!("{pid} (comm) {state} {ppid} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {ticks}"),
            )
            .unwrap();
            std::fs::write(d.join("cmdline"), cmdline).unwrap();
        };
        mk(100, 1, 'S', 10, b"agy\x00-r\x00");
        mk(101, 100, 'S', 20, b"sh\x00-c\x00cmd\x00");
        mk(102, 101, 'R', 30, b"sleep\x0099\x00");
        mk(103, 101, 'Z', 40, b"sleep\x00\x00");
        mk(200, 1, 'S', 5, b"unrelated\x00");

        let (count, cmdline) = live_children_in(100, t.path()).unwrap();
        assert_eq!(
            count, 2,
            "live descendants: sh + sleep (zombie 103 excluded)"
        );
        assert_eq!(
            cmdline.as_deref(),
            Some("sleep 99"),
            "newest is 102 (start ticks 30)"
        );

        let (count, _) = live_children_in(102, t.path()).unwrap();
        assert_eq!(count, 0);

        assert!(live_children_in(100, &t.path().join("nope")).is_none());
    }

    #[test]
    fn parse_proc_stat_handles_parens_in_comm() {
        let line = "42 (a b (c)) S 7 42 42 0 -1 4194304 100 0 0 0 1 2 0 0 20 0 3 0 999 1024 5 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0";
        let (pid, ppid, state, ticks) = parse_proc_stat(line).unwrap();
        assert_eq!(pid, 42);
        assert_eq!(ppid, 7);
        assert_eq!(state, 'S');
        assert_eq!(ticks, 999);
    }

    #[test]
    fn truncate_cmdline_caps_at_limit() {
        let long = "a".repeat(200);
        let s = truncate_cmdline(&long, 80);
        assert_eq!(s.chars().count(), 81, "80 chars + the ellipsis marker");
        assert!(s.starts_with(&"a".repeat(80)));
        let short = truncate_cmdline("sh -c", 80);
        assert_eq!(short, "sh -c");
    }

    #[test]
    fn assess_stall_matrix() {
        use std::time::UNIX_EPOCH;
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let fresh = UNIX_EPOCH + Duration::from_secs(9_900);
        let old = UNIX_EPOCH + Duration::from_secs(1_000);
        let threshold = Duration::from_secs(20 * 60);
        let fallback = UNIX_EPOCH + Duration::from_secs(5_000);

        // Fresh activity: never stalled.
        let a = assess_stall(Some(fresh), fallback, Some(0), now, threshold);
        assert!(!a.possibly_stalled);
        assert_eq!(a.idle_secs, 100);

        // Old activity and NO live children: stalled.
        let a = assess_stall(Some(old), fallback, Some(0), now, threshold);
        assert!(a.possibly_stalled);
        assert_eq!(a.idle_secs, 9_000);

        // Old activity but children running: not stalled.
        let a = assess_stall(Some(old), fallback, Some(3), now, threshold);
        assert!(!a.possibly_stalled);

        // Old activity, /proc unavailable (unknown children): conservatively
        // not stalled.
        let a = assess_stall(Some(old), fallback, None, now, threshold);
        assert!(!a.possibly_stalled);

        // No activity source at all: the job start (fallback) anchors the idle time.
        let a = assess_stall(None, fallback, Some(0), now, threshold);
        // idle = 10_000 - 5_000 = 5_000 >= 1200 and no live children.
        assert!(a.possibly_stalled);
        assert_eq!(a.idle_secs, 5_000);

        // Threshold right at the boundary: idle == threshold is "at least".
        let at_edge = now - threshold;
        let a = assess_stall(Some(at_edge), fallback, Some(0), now, threshold);
        assert!(a.possibly_stalled);
        assert_eq!(a.idle_secs, 1200);
    }

    #[test]
    fn system_time_to_rfc3339_renders_seconds_z() {
        let t = std::time::UNIX_EPOCH + Duration::from_secs(0);
        assert_eq!(
            system_time_to_rfc3339(t),
            Some("1970-01-01T00:00:00Z".to_string())
        );
        let t = std::time::UNIX_EPOCH + Duration::from_secs(1_760_000_000);
        let s = system_time_to_rfc3339(t).unwrap();
        assert!(s.ends_with('Z'));
        assert_eq!(
            s.len(),
            20,
            "rfc3339 with whole seconds: YYYY-MM-DDTHH:MM:SSZ"
        );
    }
}

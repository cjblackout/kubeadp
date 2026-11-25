//! Shared helpers for writing an executable script and spawning it, hardened
//! against the "Text file busy" (ETXTBSY) race that flaked the fake-`agy` unit
//! tests under the parallel test harness.
//!
//! Why the race happens: the test harness runs every `#[test]` in one process
//! on many threads. One thread writes an executable script (`fs::write` holds
//! an open write descriptor to the inode) while another thread forks a child.
//! `fork` copies the whole fd table, so the child inherits that write
//! descriptor; the kernel refuses to `exec` an inode that still has a write-
//! mode descriptor open anywhere (`ETXTBSY`, os error 26), so the forked
//! child's `exec` of the just-written script fails. The descriptor stays open
//! in the child until it exits, so the inode is text-busy for the child's
//! whole lifetime, not just the writer's `write()` call.
//!
//! Three mitigations, applied together:
//!
//! 1. [`write_executable_script`] opens its temp file (the standard library always sets `O_CLOEXEC`), so a
//!    concurrently forked child drops the write descriptor at its own `exec`
//!    -- the inherited-fd window then shrinks to the child's fork-to-exec gap
//!    (microseconds) instead of its lifetime.
//! 2. The same helper writes to a UNIQUE temp name in the target's directory,
//!    syncs the file, closes the write descriptor, chmods it, and only then
//!    atomically renames it into place: the target name only ever exists fully
//!    written and 0o755, and the writer's own descriptor is closed before
//!    anything can exec it.
//! 3. [`spawn_command_tracked`] retries a `spawn` a bounded number of times on
//!    `ETXTBSY` (short, capped backoff between attempts), so a concurrent
//!    child that is still holding the inherited descriptor is waited out and
//!    the exec is retried. This is the deterministic half of the fix -- a real
//!    failure (missing file, not executable) still propagates unchanged.

use std::fs::{OpenOptions, Permissions};
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// `ETXTBSY` without a `libc` import at every call site (os error 26).
const ETXTBSY: i32 = 26;

/// Whether `err` is the "Text file busy" the inherited-descriptor race yields.
pub fn is_txt_busy(err: &io::Error) -> bool {
    err.raw_os_error() == Some(ETXTBSY)
}

/// Monotonic counter for unique temp names; combined with the process id it is
/// unique across every writer in this process and across processes.
static SCRIPT_TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Writes `contents` to `path` as an executable (0o755) file, atomically and
/// race-free: a unique temp file in the same directory is written, synced and
/// closed (opened `O_CLOEXEC` so concurrent children cannot inherit the
/// descriptor), then chmodded and renamed onto `path`.
///
/// On a failed rename the temp is removed and the error returned.
pub fn write_executable_script(path: &Path, contents: &str) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("the target script has no parent directory"))?;
    let stem = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("script");
    let tmp = dir.join(format!(
        ".{stem}.{}.{}.tmp",
        std::process::id(),
        SCRIPT_TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    drop(file); // close the write descriptor before chmod / rename
    let _ = std::fs::set_permissions(&tmp, Permissions::from_mode(0o755));
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Bounded spawn retry window: a handful of attempts with short, capped
/// backoff, so a concurrent child still holding the inherited write
/// descriptor is waited out. Worst case is ~5 s, far under the caller's own
/// launch/timeout budgets (20-30 s), and is only ever paid when `exec`
/// actually fails with `ETXTBSY` -- a normal spawn returns on the first try.
const MAX_TXT_BUSY_ATTEMPTS: u32 = 100;
const TXT_BUSY_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const TXT_BUSY_MAX_BACKOFF: Duration = Duration::from_millis(50);

/// Spawns `cmd`, retrying a bounded number of times when `exec` fails with
/// `ETXTBSY` (os error 26) -- the "Text file busy" the inherited-descriptor
/// race yields when this process's just-written script is still open-for-write
/// in a concurrently forked child. Other spawn errors propagate unchanged.
///
/// `label` names the command in the (rare) error that is returned after every
/// attempt still fails with `ETXTBSY`.
pub fn spawn_command_tracked(cmd: &mut Command, label: &str) -> io::Result<std::process::Child> {
    let mut backoff = TXT_BUSY_INITIAL_BACKOFF;
    for attempt in 0..MAX_TXT_BUSY_ATTEMPTS {
        match cmd.spawn() {
            Ok(child) => return Ok(child),
            Err(e) if is_txt_busy(&e) => {
                // A concurrent child is still holding the descriptor: wait a
                // little for it to exit and release it, then re-exec. The
                // last attempt does not sleep -- its error is reported below.
                if attempt + 1 < MAX_TXT_BUSY_ATTEMPTS {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(TXT_BUSY_MAX_BACKOFF);
                }
            }
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other(format!(
        "{label}: still text-busy after {MAX_TXT_BUSY_ATTEMPTS} spawn attempts"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Spawns the given worker script `SPAWNS_PER_THREAD` times from
    /// `THREADS` threads at once; every iteration rewrites the thread's OWN
    /// script (a fresh unique temp + rename, the writer side of the race) and
    /// then spawns it. Every spawn must succeed -- this is exactly the
    /// concurrent write/exec contention that previously surfaced as
    /// `Text file busy (os error 26)`.
    ///
    /// Kept deliberately small (8 threads x 6 spawns, no sleeps): it finishes
    /// in well under a second, so the stress test stays under the 5 s budget.
    #[test]
    fn stress_concurrent_script_writes_and_spawns() {
        let root = tempdir().unwrap();
        const THREADS: usize = 8;
        const SPAWNS_PER_THREAD: usize = 6;

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let base = root.path().join(format!("t{t}"));
            handles.push(std::thread::spawn(move || {
                std::fs::create_dir_all(&base).unwrap();
                let bin = base.join("worker");
                for i in 0..SPAWNS_PER_THREAD {
                    let script = format!("#!/bin/sh\nprintf 't{t} i{i} ok\\n'\n");
                    write_executable_script(&bin, &script).unwrap();
                    let mut cmd = Command::new(&bin);
                    cmd.stdout(std::process::Stdio::piped())
                        .stderr(std::process::Stdio::piped());
                    let child = spawn_command_tracked(&mut cmd, "stress worker").unwrap();
                    let out = child.wait_with_output().unwrap();
                    assert!(out.status.success(), "worker {t}/{i} must exit 0");
                    assert!(
                        String::from_utf8_lossy(&out.stdout).contains(&format!("t{t} i{i}")),
                        "worker {t}/{i} must print its marker"
                    );
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    /// A plain (non-txt-busy) spawn error must pass through unchanged -- the
    /// retry must not mask a real failure.
    #[test]
    fn spawn_command_tracked_propagates_real_errors() {
        let mut cmd = Command::new("/nonexistent/binary/that/cannot/exist");
        let err = spawn_command_tracked(&mut cmd, "missing").unwrap_err();
        assert!(!is_txt_busy(&err), "a missing binary is not text-busy");
        assert!(err.kind() == io::ErrorKind::NotFound);
    }

    /// The unique-temp + rename writer must land a fully-written, 0o755 file
    /// at the target path and leave no `.tmp` litter behind.
    #[test]
    fn write_executable_script_lands_executable_and_leaves_no_temp() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("prog");
        write_executable_script(&target, "#!/bin/sh\necho hi\n").unwrap();
        let meta = std::fs::metadata(&target).unwrap();
        assert_eq!(
            meta.permissions().mode() & 0o777,
            0o755,
            "the script must be executable"
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "#!/bin/sh\necho hi\n"
        );
        // No stray temp files in the directory (only the target itself).
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp file may be left: {leftovers:?}"
        );
    }
}

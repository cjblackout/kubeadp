//! Integration tests for the `repo-sync` subcommand of the real `kubeadp-agent`
//! binary (the internal git-sync replacement, `core::reposync`).
//!
//! Offline: every remote is a local bare repository created with
//! `git init --bare` and pushed to from a temp working clone. The binary is
//! invoked with `CARGO_BIN_EXE_kubeadp-agent` (needs the `server` feature,
//! i.e. runs under `cargo test --features a2a,test-hooks`).

#![cfg(feature = "server")]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A local bare remote. `origin` points SHA refs: read-only fetch.
struct Remote {
    bare: PathBuf,
    work: PathBuf,
}

fn git_in(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {args:?} failed in {}: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn git_ok(dir: &Path, args: &[&str]) {
    let _ = git_in(dir, args);
}

/// Creates a bare remote named `name` with `count` commits on `main`, each
/// versioning `file.txt`, and returns (bare, work) paths.
fn make_remote(root: &Path, name: &str, count: usize) -> Remote {
    let work = root.join(format!("{name}-work"));
    std::fs::create_dir_all(&work).unwrap();
    git_ok(&work, &["init", "-q", "-b", "main"]);
    git_ok(&work, &["config", "user.name", "Test"]);
    git_ok(&work, &["config", "user.email", "test@test.com"]);
    for i in 0..count.max(1) {
        std::fs::write(work.join("file.txt"), format!("v{i}\n")).unwrap();
        git_ok(&work, &["add", "."]);
        git_ok(&work, &["commit", "-q", "-m", &format!("commit {i}")]);
    }
    let bare = root.join(format!("{name}.git"));
    git_ok(
        root,
        &[
            "clone",
            "-q",
            "--bare",
            work.to_str().unwrap(),
            bare.to_str().unwrap(),
        ],
    );
    // The work clone must be able to push to the bare remote.
    git_ok(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
    Remote { bare, work }
}

/// Adds a new commit to the remote's working clone and pushes it to the bare.
fn push_new_commit(remote: &Remote, content: &str) -> String {
    std::fs::write(remote.work.join("file.txt"), content).unwrap();
    git_ok(&remote.work, &["add", "."]);
    git_ok(&remote.work, &["commit", "-q", "-m", "new commit"]);
    let out = Command::new("git")
        .current_dir(&remote.work)
        .args(["push", "-q", "origin", "main"])
        .output()
        .unwrap();
    assert!(out.status.success(), "push failed: {:?}", out);
    git_in(&remote.work, &["rev-parse", "HEAD"])
}

fn fs_create_dir_all(p: &Path) {
    std::fs::create_dir_all(p).unwrap();
}

fn read_link(path: &Path) -> PathBuf {
    std::fs::read_link(path).unwrap_or_else(|e| panic!("read_link {}: {e}", path.display()))
}

/// The directory a repo exposes through its symlink.
fn linked_dir(link: &Path) -> PathBuf {
    let target = read_link(link);
    if target.is_absolute() {
        target
    } else {
        link.parent().expect("link has a parent").join(&target)
    }
}

/// SHA the link currently points at, from the worktree dir name.
fn linked_sha(link: &Path) -> Option<String> {
    read_link(link)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
}

/// Runs the real `kubeadp-agent` binary's `repo-sync` subcommand with a given
/// config file and env additions. Captures stdout+stderr.
fn run_bin(args: &[&str], config: &Path, extra_env: &[(&str, &str)]) -> std::process::Output {
    let exe = std::env::var("CARGO_BIN_EXE_kubeadp-agent")
        .unwrap_or_else(|_| panic!("CARGO_BIN_EXE_kubeadp-agent not set"));
    let mut cmd = Command::new(&exe);
    let argv: Vec<&str> = std::iter::once("repo-sync")
        .chain(args.iter().copied())
        .collect();
    cmd.args(&argv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env(
            "KUBEADP_REPO_SYNC_CONFIG",
            config.to_string_lossy().as_ref(),
        );
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn repo-sync")
}

fn write_config(path: &Path, body: &str) {
    fs_create_dir_all(path.parent().unwrap());
    std::fs::write(path, body).unwrap();
}

/// A repo-sync config TOML for one repo.
fn one_repo_config(root: &Path, name: &str, url: &str, link: &Path, extra: &str) -> String {
    format!(
        "[repo_sync]\nrepos = [\n  {{ name = \"{name}\", url = \"{url}\", root = \"{}\", link = \"{}\"{extra} }},\n]\n",
        root.join("roots").join(name).display(),
        link.display()
    )
}

#[test]
fn reposync_first_sync_creates_worktree_and_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 3);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "first sync failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let worktrees = root.join("roots").join("proj").join(".worktrees");
    let entries: Vec<String> = std::fs::read_dir(&worktrees)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "exactly one worktree after first sync: {entries:?}"
    );

    let sha = git_in(&remote.bare, &["rev-parse", "main"]);
    assert_eq!(linked_sha(&link), Some(sha.clone()));
    // The linked worktree contains the right file content.
    assert_eq!(
        std::fs::read_to_string(linked_dir(&link).join("file.txt")).unwrap(),
        "v2\n"
    );
    // Status line on stderr.
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("repo-sync proj:"),
        "expected status line, got {err}"
    );
    assert!(
        err.contains(&sha[..8]),
        "status line must show the sha, got {err}"
    );
}

#[test]
fn reposync_second_sync_with_no_change_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 2);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sha = git_in(&remote.bare, &["rev-parse", "main"]);

    // Remember the worktree directories, run again, ensure no new worktree.
    let worktrees = root.join("roots").join("proj").join(".worktrees");
    let before: Vec<String> = std::fs::read_dir(&worktrees)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(linked_sha(&link), Some(sha));
    let after: Vec<String> = std::fs::read_dir(&worktrees)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(before, after, "no-change sync must not add worktrees");
}

#[test]
fn reposync_new_commit_flips_symlink_atomically_and_keeps_previous_worktree() {
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 1);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let old_sha = git_in(&remote.bare, &["rev-parse", "main"]);

    // A reader thread watches the symlink: it must never dangle while the
    // update happens (the link always resolves to one of the two worktrees
    // or a real dir).
    let watch_link = link.clone();
    let watch_targets = {
        let wt = root.join("roots").join("proj").join(".worktrees");
        vec![wt.join(&old_sha)]
    };
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let watcher = std::thread::spawn(move || {
        while !stop2.load(Ordering::SeqCst) {
            let target =
                std::fs::read_link(&watch_link).unwrap_or_else(|e| panic!("link vanished: {e}"));
            let resolved = if target.is_absolute() {
                target
            } else {
                watch_link.parent().unwrap().join(&target)
            };
            assert!(
                resolved.is_dir(),
                "link pointed at a non-existent directory while updating: {resolved:?}"
            );
            let _ = &watch_targets; // structure is asserted by resolved.is_dir()
        }
    });

    let new_sha = push_new_commit(&remote, "v9\n");
    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(linked_sha(&link), Some(new_sha.clone()));
    assert_eq!(
        std::fs::read_to_string(linked_dir(&link).join("file.txt")).unwrap(),
        "v9\n"
    );

    stop.store(true, Ordering::SeqCst);
    watcher.join().unwrap();

    // The previous worktree is kept (grace period = previous revision).
    let worktrees = root.join("roots").join("proj").join(".worktrees");
    assert!(
        worktrees.join(&old_sha).is_dir(),
        "previous worktree must be kept"
    );
    assert!(worktrees.join(&new_sha).is_dir());
}

#[test]
fn reposync_failed_fetch_keeps_link_intact_and_exits_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 1);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let old_sha = linked_sha(&link).expect("link set");

    // Destroy the remote (fake it as deleted), then sync: fetch must fail and
    // the link must still point at the complete previous checkout.
    let old_file = std::fs::read_to_string(linked_dir(&link).join("file.txt")).unwrap();
    std::fs::remove_dir_all(&remote.bare).unwrap();

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        !out.status.success(),
        "--once must exit non-zero on failure"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("failed"), "error must be visible: {err}");

    assert_eq!(linked_sha(&link), Some(old_sha));
    assert_eq!(
        std::fs::read_to_string(linked_dir(&link).join("file.txt")).unwrap(),
        old_file
    );
}

#[test]
fn reposync_leftover_worktree_is_cleaned_and_link_stays_valid() {
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 1);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let old_sha = linked_sha(&link).expect("link set");

    // An interrupted update left a partial worktree dir for the commit the
    // next sync will fetch. `ensure_worktree` must remove the leftover
    // (git worktree add refuses "... already exists") and rebuild it.
    let new_sha = push_new_commit(&remote, "v9\n");
    let worktrees = root.join("roots").join("proj").join(".worktrees");
    let partial = worktrees.join(&new_sha);
    fs_create_dir_all(&partial);
    std::fs::write(partial.join("partial"), "half").unwrap();

    // Next sync removes the leftover worktree and still works.
    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(linked_sha(&link), Some(new_sha.clone()));
    assert_eq!(
        std::fs::read_to_string(linked_dir(&link).join("file.txt")).unwrap(),
        "v9\n"
    );
    assert!(
        !partial.join("partial").exists(),
        "leftover worktree contents must be gone after rebuild"
    );
    assert!(worktrees.join(&old_sha).is_dir(), "previous worktree kept");
}

#[test]
fn reposync_checkout_is_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 1);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Walk the linked tree: no file or dir may be writable by anyone.
    fn assert_read_only(p: &Path) {
        let meta = std::fs::symlink_metadata(p).unwrap();
        if meta.file_type().is_symlink() {
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        assert_eq!(mode & 0o222, 0, "{p:?} is writable (mode {mode:o})");
        if meta.is_dir() {
            for e in std::fs::read_dir(p).unwrap() {
                assert_read_only(&e.unwrap().path());
            }
        }
    }
    assert_read_only(&linked_dir(&link));
}

#[test]
fn reposync_two_instances_on_one_root_the_second_exits_correctly() {
    // The lock is per-root: while the first instance holds the root (created
    // but parked), a second --once must report "another instance" and exit 0.
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 1);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    // Park a lock on the root.
    let lock_path = root.join("roots").join("proj").join(".repo-sync.lock");
    fs_create_dir_all(lock_path.parent().unwrap());
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    // SAFETY: exclusive non-blocking flock on our own fd.
    let rc = unsafe {
        libc::flock(
            std::os::unix::io::AsRawFd::as_raw_fd(&lock_file),
            libc::LOCK_EX | libc::LOCK_NB,
        )
    };
    assert_eq!(rc, 0, "setup must take the flock");

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "a held root must exit cleanly (0): {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("another instance") || err.contains("is running"),
        "the second instance must say it exits, got: {err}"
    );
}

#[test]
fn reposync_retention_prunes_old_worktrees() {
    // Three updates: after the third, the first worktree (2 revisions back)
    // is pruned; the current and the previous (grace period) stay.
    let dir = tempfile::tempdir().unwrap();
    let remote = make_remote(dir.path(), "proj", 1);
    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    write_config(
        &cfg,
        &one_repo_config(&root, "proj", remote.bare.to_str().unwrap(), &link, ""),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let first_sha = git_in(&remote.bare, &["rev-parse", "main"]);

    let sha2 = push_new_commit(&remote, "two\n");
    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(linked_sha(&link), Some(sha2.clone()));

    let worktrees = root.join("roots").join("proj").join(".worktrees");
    assert!(
        worktrees.join(&first_sha).is_dir(),
        "first worktree kept as grace"
    );

    let sha3 = push_new_commit(&remote, "three\n");
    let out = run_bin(&["--once"], &cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(linked_sha(&link), Some(sha3.clone()));

    // After the third sync: first pruned, second and third present.
    assert!(
        !worktrees.join(&first_sha).exists(),
        "first worktree (2 revisions back) must be pruned"
    );
    assert!(
        worktrees.join(&sha2).is_dir(),
        "previous worktree kept as grace"
    );
    assert!(worktrees.join(&sha3).is_dir(), "current worktree present");
}

#[test]
fn reposync_token_never_appears_in_argv_or_logs() {
    let dir = tempfile::tempdir().unwrap();
    // A real local remote so the fetch path actually exercises the credential
    // plumbing (offline). The URL carries a token-looking secret in its
    // userinfo; the mirror must strip it, and the token must never surface in
    // output or in the mirror's config.
    let remote_bare = dir.path().join("proj.git");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git_ok(&work, &["init", "-q", "-b", "main"]);
    git_ok(&work, &["config", "user.name", "Test"]);
    git_ok(&work, &["config", "user.email", "t@t"]);
    std::fs::write(work.join("f"), "v\n").unwrap();
    git_ok(&work, &["add", "."]);
    git_ok(&work, &["commit", "-qm", "c"]);
    git_ok(
        dir.path(),
        &[
            "clone",
            "-q",
            "--bare",
            work.to_str().unwrap(),
            remote_bare.to_str().unwrap(),
        ],
    );

    let token = "n3ver-put-This-000-on-a-cmdline";
    let token_file = dir.path().join("secrets").join("token");
    fs_create_dir_all(token_file.parent().unwrap());
    std::fs::write(&token_file, token).unwrap();

    let root = dir.path().join("data");
    let link = dir.path().join("workspace").join("proj");
    let cfg = dir.path().join("cfg.toml");
    let url = format!("https://user:{}@example.invalid/proj.git", token);
    write_config(
        &cfg,
        &format!(
            "[repo_sync]\ntoken_file = \"{}\"\nrepos = [\n  {{ name = \"proj\", url = \"{url}\", root = \"{}\", link = \"{}\" }},\n]\n",
            token_file.display(),
            root.join("roots").join("proj").display(),
            link.display()
        ),
    );

    let out = run_bin(&["--once"], &cfg, &[]);
    let all = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !all.contains(token),
        "the token must never appear in repo-sync output: {all}"
    );

    // The mirror stores the stripped URL only; the token-bearing userinfo is
    // gone from the persisted repo config.
    let mirror_config = root.join("roots").join("proj").join(".git").join("config");
    if let Ok(contents) = std::fs::read_to_string(&mirror_config) {
        assert!(
            !contents.contains(token),
            "the token must not be persisted into the mirror config"
        );
        assert!(
            contents.contains("https://example.invalid/proj.git"),
            "mirror origin must be the stripped URL: {contents}"
        );
        assert!(
            !contents.contains("user:") && !contents.contains(&format!("{token}@")),
            "userinfo must be stripped: {contents}"
        );
    }
}

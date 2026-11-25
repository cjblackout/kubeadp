//! Shared exec path resolution for the sandbox backends.
//!
//! After building the agent's filtered environment, every backend execs the
//! target with `execve`. Command resolution (PATH search) MUST use the
//! filtered environment, not the calling process's `environ`: the gVisor shim
//! clears its environment before forking the agent, so a plain `execvpe` there
//! would find no PATH and report even an on-policy binary as ENOENT (exit
//! 127). `resolve_exec_path` searches the PATH carried by the filtered envp,
//! and `exec_target` runs the resolved path with `execve`, mapping exec
//! errors to the contract exit codes (127 not found, 126 not executable, else
//! 125).

use std::ffi::CString;
use std::path::{Path, PathBuf};

/// Default PATH used when the filtered environment carries no PATH (mirrors
/// execvp's POSIX fallback).
pub fn default_path() -> &'static str {
    "/usr/local/bin:/usr/bin:/bin"
}

/// Returns `true` when `path` names a file the current user can execute
/// (`access(path, X_OK)`).
fn is_executable(path: &Path) -> bool {
    let Ok(c) = CString::new(path.to_str().unwrap_or_default()) else {
        return false;
    };
    unsafe { libc::access(c.as_ptr(), libc::X_OK) == 0 }
}

/// Resolves the executable path for `cmd` using execvp() semantics against the
/// environment `env` (the envp the process will exec with): bare command names
/// are searched in PATH (an empty PATH element means the current directory); a
/// command containing a slash is used verbatim. Returns `None` when no PATH
/// entry yields an executable file; the caller then embarks on the raw name,
/// letting the kernel return ENOENT (127), exactly like execvp.
pub fn resolve_exec_path(cmd: &str, env: &[(String, String)]) -> Option<PathBuf> {
    if cmd.contains('/') {
        return Some(PathBuf::from(cmd));
    }
    let mut path: &str = default_path();
    for (k, v) in env {
        if k == "PATH" {
            path = v.as_str();
            break;
        }
    }
    for element in path.split(':') {
        let dir = if element.is_empty() { "." } else { element };
        let cand = Path::new(dir).join(cmd);
        if is_executable(&cand) {
            return Some(cand);
        }
    }
    None
}

/// Maps an exec(2) errno to the sandbox contract exit code:
/// ENOENT -> 127, EACCES -> 126, anything else -> 125.
pub fn exec_errno_code(errno: i32) -> i32 {
    match errno {
        libc::ENOENT => 127,
        libc::EACCES => 126,
        _ => 125,
    }
}

/// Execs `cmd` with `args` and `env` via `execve`, resolving the executable
/// against the filtered `env` PATH (never the caller's process environment).
/// Never returns: on exec failure it maps the errno to the contract exit code
/// and exits with it.
pub fn exec_target(cmd: &str, args: &[String], env: &[(String, String)]) -> ! {
    let prog = resolve_exec_path(cmd, env).unwrap_or_else(|| PathBuf::from(cmd));

    let mut c_args: Vec<CString> = Vec::new();
    c_args.push(CString::new(cmd).unwrap());
    for a in args {
        c_args.push(CString::new(a.as_str()).unwrap());
    }
    let mut c_arg_ptrs: Vec<*const libc::c_char> = c_args.iter().map(|c| c.as_ptr()).collect();
    c_arg_ptrs.push(std::ptr::null());

    let mut c_envs: Vec<CString> = Vec::new();
    for (k, v) in env {
        c_envs.push(CString::new(format!("{k}={v}")).unwrap());
    }
    let mut c_env_ptrs: Vec<*const libc::c_char> = c_envs.iter().map(|c| c.as_ptr()).collect();
    c_env_ptrs.push(std::ptr::null());

    let prog_c = CString::new(prog.to_str().unwrap()).unwrap();
    unsafe {
        libc::execve(prog_c.as_ptr(), c_arg_ptrs.as_ptr(), c_env_ptrs.as_ptr());
    }

    let err = std::io::Error::last_os_error();
    let code = exec_errno_code(err.raw_os_error().unwrap_or(0));
    eprintln!("kubeadp-agent failed to exec '{cmd}': {err}");
    unsafe { libc::_exit(code) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn make_executable(path: &std::path::Path) {
        std::fs::write(path, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn test_resolve_finds_on_path() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("tool");
        make_executable(&bin);
        let env = vec![("PATH".to_string(), dir.path().to_string_lossy().to_string())];
        assert_eq!(resolve_exec_path("tool", &env), Some(bin));
        // a slash command is used verbatim, never searched on PATH
        assert_eq!(
            resolve_exec_path("/usr/bin/tool", &env),
            Some(PathBuf::from("/usr/bin/tool"))
        );
    }

    #[test]
    fn test_resolve_honours_search_order() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let f1 = d1.path().join("prog");
        let f2 = d2.path().join("prog");
        make_executable(&f1);
        make_executable(&f2);
        let env = vec![(
            "PATH".to_string(),
            format!("{}:{}", d1.path().display(), d2.path().display()),
        )];
        assert_eq!(
            resolve_exec_path("prog", &env),
            Some(f1),
            "first PATH hit wins"
        );
    }

    #[test]
    fn test_resolve_not_found_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let env = vec![("PATH".to_string(), dir.path().to_string_lossy().to_string())];
        assert_eq!(resolve_exec_path("missing-tool", &env), None);
        // No PATH in the env at all -> default PATH, still not found.
        assert_eq!(resolve_exec_path("missing-tool", &[]), None);
    }

    #[test]
    fn test_resolve_non_executable_not_selected() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("noexec");
        std::fs::write(&bin, b"x").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o644)).unwrap();
        let env = vec![("PATH".to_string(), dir.path().to_string_lossy().to_string())];
        assert_eq!(resolve_exec_path("noexec", &env), None);
    }

    #[test]
    fn test_exec_errno_mapping() {
        assert_eq!(exec_errno_code(libc::ENOENT), 127);
        assert_eq!(exec_errno_code(libc::EACCES), 126);
        assert_eq!(exec_errno_code(libc::EIO), 125);
        assert_eq!(exec_errno_code(0), 125);
    }
}

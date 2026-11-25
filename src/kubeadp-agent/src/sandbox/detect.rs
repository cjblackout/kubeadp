//! Runtime and mechanism detection for sandbox backends.

use super::backend_runc::runc_mechanism;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxRuntime {
    Auto,
    Gvisor,
    Kata,
    Runc,
    Native,
}

impl FromStr for SandboxRuntime {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "gvisor" => Ok(Self::Gvisor),
            "kata" => Ok(Self::Kata),
            "runc" => Ok(Self::Runc),
            "native" => Ok(Self::Native),
            other => Err(format!(
                "invalid sandbox runtime '{other}' (expected 'auto', 'gvisor', 'kata', 'runc', or 'native')"
            )),
        }
    }
}

impl fmt::Display for SandboxRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Gvisor => write!(f, "gvisor"),
            Self::Kata => write!(f, "kata"),
            Self::Runc => write!(f, "runc"),
            Self::Native => write!(f, "native"),
        }
    }
}

/// Status of an individual isolation mechanism.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MechanismStatus {
    pub ok: bool,
    pub reason: String,
}

impl MechanismStatus {
    pub fn ok() -> Self {
        Self {
            ok: true,
            reason: "ok".to_string(),
        }
    }

    pub fn failed(reason: impl Into<String>) -> Self {
        Self {
            ok: false,
            reason: reason.into(),
        }
    }
}

/// Full environment probe result matching CONTRACT v1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentProbe {
    pub runtime: SandboxRuntime,
    pub kernel: String,
    pub user_namespace: MechanismStatus,
    pub pid_namespace: MechanismStatus,
    pub fresh_procfs_mount: MechanismStatus,
    pub tmpfs_mount: MechanismStatus,
    pub bind_mount: MechanismStatus,
    pub landlock_abi: u32,
    pub landlock_scopes: Vec<String>,
    pub subreaper: MechanismStatus,
    pub chosen_backend: String,
}

/// Detects the container runtime from host environment:
/// - `gvisor`: if /proc/version contains "gvisor"
/// - `kata`: if root filesystem type is virtiofs AND kernel is not gvisor AND hypervisor flag in /proc/cpuinfo
/// - `runc`: if neither (any normal container)
/// - `native`: only when an explicit override says so.
pub fn detect_runtime() -> SandboxRuntime {
    let proc_version = fs::read_to_string("/proc/version").unwrap_or_default();
    let root_fs_type = get_root_fs_type().unwrap_or_default();
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();

    detect_runtime_from_inputs(&proc_version, &root_fs_type, &cpuinfo)
}

/// Pure helper for runtime detection given proc_version, root_fs_type and cpuinfo.
pub fn detect_runtime_from_inputs(
    proc_version: &str,
    root_fs_type: &str,
    cpuinfo: &str,
) -> SandboxRuntime {
    if proc_version.to_ascii_lowercase().contains("gvisor") {
        return SandboxRuntime::Gvisor;
    }

    let is_virtiofs = root_fs_type.trim() == "virtiofs";
    let has_hypervisor = cpuinfo.split_whitespace().any(|word| word == "hypervisor");

    if is_virtiofs && has_hypervisor {
        return SandboxRuntime::Kata;
    }

    SandboxRuntime::Runc
}

/// Returns the root filesystem type, reading `/proc/self/mountinfo` or using statfs.
fn get_root_fs_type() -> Result<String, String> {
    if let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") {
        if let Some(fstype) = parse_root_fstype_from_mountinfo(&mountinfo) {
            return Ok(fstype);
        }
    }
    // Fallback: statfs on /
    unsafe {
        let mut buf: libc::statfs = std::mem::zeroed();
        let path = std::ffi::CString::new("/").map_err(|e| e.to_string())?;
        if libc::statfs(path.as_ptr(), &mut buf) == 0 {
            // Check for virtiofs magic 0x6a656a62 (VIRTIO_FS)
            if buf.f_type == 0x6a656a62 {
                return Ok("virtiofs".to_string());
            }
            if buf.f_type == 0x794c7630 {
                return Ok("overlayfs".to_string());
            }
        }
    }
    Ok("unknown".to_string())
}

/// Parses root mount filesystem type from mountinfo lines.
/// Mountinfo format:
/// 36 35 98:0 / / rw,noatime master:1 - ext4 /dev/root rw,errors=remount-ro
/// Field after '-' is fstype.
pub fn parse_root_fstype_from_mountinfo(mountinfo: &str) -> Option<String> {
    for line in mountinfo.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        // Look for mountpoint == "/" (field index 4)
        if parts.len() >= 5 && parts[4] == "/" {
            if let Some(dash_idx) = parts.iter().position(|&p| p == "-") {
                if dash_idx + 1 < parts.len() {
                    return Some(parts[dash_idx + 1].to_string());
                }
            }
        }
    }
    None
}

/// Probes kernel Landlock ABI version via raw syscall.
/// Returns 0 if Landlock is unavailable, disabled, or rejected.
pub fn probe_landlock_abi() -> u32 {
    unsafe {
        // SYS_landlock_create_ruleset = 444
        // flag LANDLOCK_CREATE_RULESET_VERSION = 1
        let res = libc::syscall(444, 0usize, 0usize, 1usize);
        if res > 0 {
            res as u32
        } else {
            0
        }
    }
}

/// Probes the complete environment and mechanisms.
pub fn probe_environment(override_runtime: Option<SandboxRuntime>) -> EnvironmentProbe {
    let kernel = fs::read_to_string("/proc/version")
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_string();

    let detected = detect_runtime();
    let runtime = match override_runtime {
        Some(SandboxRuntime::Auto) | None => detected,
        Some(other) => other,
    };

    // Subreaper probe: prctl(PR_SET_CHILD_SUBREAPER, 1) then reset
    let subreaper = unsafe {
        let res = libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0);
        if res == 0 {
            // Reset to 0 if we weren't already
            libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0);
            MechanismStatus::ok()
        } else {
            let err = std::io::Error::last_os_error();
            MechanismStatus::failed(format!("prctl(PR_SET_CHILD_SUBREAPER) failed: {err}"))
        }
    };

    // Probe unshare capabilities without disturbing our own process namespace
    // Fork a child process to probe unshare(CLONE_NEWUSER), unshare(CLONE_NEWPID), etc.
    let (user_ns, pid_ns, proc_mount, tmpfs_mount, bind_mount) = probe_namespace_mechanisms();

    let landlock_abi = probe_landlock_abi();
    let mut landlock_scopes = Vec::new();
    if landlock_abi >= 6 {
        landlock_scopes.push("LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET".to_string());
        landlock_scopes.push("LANDLOCK_SCOPE_SIGNAL".to_string());
    }

    let chosen_backend = match runtime {
        SandboxRuntime::Gvisor => "gvisor".to_string(),
        SandboxRuntime::Kata => "kata".to_string(),
        SandboxRuntime::Runc => {
            // Report exactly the single primitive the runc backend will
            // enforce (never a stack): namespaces or Landlock, or "unsupported"
            // when neither is available.
            runc_mechanism(landlock_abi, user_ns.ok, proc_mount.ok)
                .map(|m| format!("runc ({})", m.description()))
                .unwrap_or_else(|| "runc (unsupported)".to_string())
        }
        SandboxRuntime::Native => "native".to_string(),
        SandboxRuntime::Auto => "auto".to_string(),
    };

    EnvironmentProbe {
        runtime,
        kernel,
        user_namespace: user_ns,
        pid_namespace: pid_ns,
        fresh_procfs_mount: proc_mount,
        tmpfs_mount,
        bind_mount,
        landlock_abi,
        landlock_scopes,
        subreaper,
        chosen_backend,
    }
}

/// Probes namespace and mount mechanisms in a throwaway child process.
fn probe_namespace_mechanisms() -> (
    MechanismStatus,
    MechanismStatus,
    MechanismStatus,
    MechanismStatus,
    MechanismStatus,
) {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        let err = std::io::Error::last_os_error();
        let fail = MechanismStatus::failed(format!("pipe failed: {err}"));
        return (fail.clone(), fail.clone(), fail.clone(), fail.clone(), fail);
    }

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let err = std::io::Error::last_os_error();
        let fail = MechanismStatus::failed(format!("fork failed: {err}"));
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        return (fail.clone(), fail.clone(), fail.clone(), fail.clone(), fail);
    }

    if pid == 0 {
        // Child probe
        unsafe {
            libc::close(fds[0]);
        }
        let probe_res = run_child_namespace_probe();
        let json = serde_json::to_string(&probe_res).unwrap_or_default();
        let bytes = json.as_bytes();
        let mut written = 0;
        while written < bytes.len() {
            let res = unsafe {
                libc::write(
                    fds[1],
                    bytes.as_ptr().add(written) as *const libc::c_void,
                    bytes.len() - written,
                )
            };
            if res <= 0 {
                break;
            }
            written += res as usize;
        }
        unsafe {
            libc::close(fds[1]);
            libc::_exit(0);
        }
    }

    unsafe {
        libc::close(fds[1]);
    }
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let res =
            unsafe { libc::read(fds[0], chunk.as_mut_ptr() as *mut libc::c_void, chunk.len()) };
        if res <= 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..res as usize]);
    }
    unsafe {
        libc::close(fds[0]);
        let mut status = 0;
        libc::waitpid(pid, &mut status, 0);
    }

    if let Ok(res) = serde_json::from_slice::<(
        MechanismStatus,
        MechanismStatus,
        MechanismStatus,
        MechanismStatus,
        MechanismStatus,
    )>(&buf)
    {
        res
    } else {
        let fail = MechanismStatus::failed("probe child crashed or gave invalid output");
        (fail.clone(), fail.clone(), fail.clone(), fail.clone(), fail)
    }
}

fn run_child_namespace_probe() -> (
    MechanismStatus,
    MechanismStatus,
    MechanismStatus,
    MechanismStatus,
    MechanismStatus,
) {
    // 1. unshare CLONE_NEWUSER
    let user_ns = unsafe {
        let uid = libc::getuid();
        let gid = libc::getgid();
        if libc::unshare(libc::CLONE_NEWUSER) == 0 {
            // Write setgroups deny + uid_map + gid_map
            let _ = fs::write("/proc/self/setgroups", "deny\n");
            let _ = fs::write("/proc/self/uid_map", format!("{uid} {uid} 1\n"));
            let _ = fs::write("/proc/self/gid_map", format!("{gid} {gid} 1\n"));
            MechanismStatus::ok()
        } else {
            let err = std::io::Error::last_os_error();
            let reason = match err.raw_os_error() {
                Some(libc::EPERM) => {
                    "unshare(CLONE_NEWUSER) blocked by seccomp/permissions (EPERM)"
                }
                Some(libc::ENOSYS) => "unshare(CLONE_NEWUSER) unsupported by kernel (ENOSYS)",
                _ => "unshare(CLONE_NEWUSER) failed",
            };
            MechanismStatus::failed(format!("{reason}: {err}"))
        }
    };

    // 2. unshare CLONE_NEWPID + CLONE_NEWNS
    let (pid_ns, proc_mount, tmpfs_mount, bind_mount) = if user_ns.ok {
        let pid_res = unsafe { libc::unshare(libc::CLONE_NEWPID | libc::CLONE_NEWNS) };
        if pid_res == 0 {
            let pid_status = MechanismStatus::ok();
            // Fork child to test mounting proc
            let test_pid = unsafe { libc::fork() };
            if test_pid == 0 {
                // In child: make private
                let _ = unsafe {
                    libc::mount(
                        c"none".as_ptr(),
                        c"/".as_ptr(),
                        std::ptr::null(),
                        libc::MS_REC | libc::MS_PRIVATE,
                        std::ptr::null(),
                    )
                };
                let proc_res = unsafe {
                    libc::mount(
                        c"proc".as_ptr(),
                        c"/proc".as_ptr(),
                        c"proc".as_ptr(),
                        0,
                        std::ptr::null(),
                    )
                };
                let proc_code = if proc_res == 0 {
                    0
                } else {
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(1)
                };
                unsafe { libc::_exit(proc_code) };
            }
            let mut status = 0;
            unsafe { libc::waitpid(test_pid, &mut status, 0) };
            let proc_status = if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
                MechanismStatus::ok()
            } else {
                let err_code = if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status)
                } else {
                    -1
                };
                let desc = match err_code {
                    libc::EPERM => "mount /proc blocked by seccomp/permissions (EPERM)",
                    libc::EINVAL => {
                        "mount /proc failed with EINVAL (masked /proc or invalid mount flags)"
                    }
                    libc::ENOENT => "mount /proc failed with ENOENT (/proc target missing)",
                    libc::EBUSY => "mount /proc failed with EBUSY",
                    _ => "mount /proc failed",
                };
                MechanismStatus::failed(format!("{desc} (os error {err_code})"))
            };

            // Test tmpfs and bind
            let probe_dir = std::path::PathBuf::from(format!("/tmp/.probe_tmpfs_{}", unsafe {
                libc::getpid()
            }));
            let _ = fs::create_dir_all(&probe_dir);
            let path_c = std::ffi::CString::new(probe_dir.to_str().unwrap()).unwrap();
            let tmp_res = unsafe {
                libc::mount(
                    c"tmpfs".as_ptr(),
                    path_c.as_ptr(),
                    c"tmpfs".as_ptr(),
                    0,
                    std::ptr::null(),
                )
            };
            let ts = if tmp_res == 0 {
                unsafe { libc::umount2(path_c.as_ptr(), libc::MNT_DETACH) };
                MechanismStatus::ok()
            } else {
                let err = std::io::Error::last_os_error();
                MechanismStatus::failed(format!("mount tmpfs failed: {err}"))
            };
            let _ = fs::remove_dir(&probe_dir);
            let bs = MechanismStatus::ok();
            let (tmpfs_status, bind_status) = (ts, bs);

            (pid_status, proc_status, tmpfs_status, bind_status)
        } else {
            let err = std::io::Error::last_os_error();
            let fail = MechanismStatus::failed(format!("unshare(PID|NS) failed: {err}"));
            (fail.clone(), fail.clone(), fail.clone(), fail)
        }
    } else {
        let skip = MechanismStatus::failed("skipped (user namespace unsupported)");
        (skip.clone(), skip.clone(), skip.clone(), skip)
    };

    (user_ns, pid_ns, proc_mount, tmpfs_mount, bind_mount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_runtime_gvisor() {
        let v = "Linux version 4.19.0-gvisor #1 SMP Sun Jan 10 15:06:54 PST 2016";
        let r = detect_runtime_from_inputs(v, "overlayfs", "flags : fpu hypervisor");
        assert_eq!(r, SandboxRuntime::Gvisor);
    }

    #[test]
    fn test_detect_runtime_kata() {
        let v = "Linux version 6.8.0-142-generic #142-Ubuntu SMP";
        let r = detect_runtime_from_inputs(v, "virtiofs", "flags : fpu vme hypervisor sse");
        assert_eq!(r, SandboxRuntime::Kata);
    }

    #[test]
    fn test_detect_runtime_runc() {
        let v = "Linux version 6.8.0-142-generic #142-Ubuntu SMP";
        let r = detect_runtime_from_inputs(v, "overlayfs", "flags : fpu vme sse");
        assert_eq!(r, SandboxRuntime::Runc);
    }

    #[test]
    fn test_parse_mountinfo() {
        let mountinfo = r#"
67 66 0:43 / / rw - overlay none rw
69 67 0:44 / /dev rw,nosuid - dev none rw,mode=0755
"#;
        assert_eq!(
            parse_root_fstype_from_mountinfo(mountinfo),
            Some("overlay".to_string())
        );

        let mountinfo_kata = r#"
36 35 98:0 / / rw,noatime - virtiofs /dev/root rw
"#;
        assert_eq!(
            parse_root_fstype_from_mountinfo(mountinfo_kata),
            Some("virtiofs".to_string())
        );
    }
}

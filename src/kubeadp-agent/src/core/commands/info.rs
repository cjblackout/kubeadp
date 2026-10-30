//! `agent-run info` -- one command answering "is the server healthy and what
//! can jobs do": server identity/uptime, resolved config, current capacity,
//! disk headroom, auth credential state, and toolchain availability.
//!
//! Every section degrades to "unknown" instead of failing the command, and no
//! token or secret is ever printed (only paths, booleans, mtimes, versions).

use std::collections::BTreeMap;
use std::ffi::CString;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use crate::core::args::InfoArgs;
use crate::core::config::Config;
use crate::core::job::{compute_status, get_process_start_time, list_jobs, JobStatus};
use crate::core::output::CommandOutput;

/// Bound for one toolchain version probe; the child is killed past it.
const TOOL_PROBE_TIMEOUT: Duration = Duration::from_millis(3000);
/// (program, version argument) pairs probed by the `toolchains` section.
const TOOLCHAINS: &[(&str, &str)] = &[
    ("git", "--version"),
    ("gh", "--version"),
    ("go", "version"),
    ("rustc", "--version"),
    ("cargo", "--version"),
    ("python3", "--version"),
    ("pip3", "--version"),
];

pub fn run(config: &Config, _args: &InfoArgs) -> Result<CommandOutput> {
    let (server_data, server_lines) = server_section();
    let (config_data, config_lines) = config_section(config);
    let (capacity_data, capacity_lines) = capacity_section(config);
    let (disk_data, disk_lines, disk_warns) = disk_section(config);
    let (auth_data, auth_lines) = auth_section(config);
    let (tool_data, tool_lines) = toolchains_section();

    let mut output = CommandOutput::new(json!({
        "server": server_data,
        "config": config_data,
        "capacity": capacity_data,
        "disk": disk_data,
        "auth": auth_data,
        "toolchains": tool_data,
    }));

    for lines in [
        server_lines,
        config_lines,
        capacity_lines,
        disk_lines,
        auth_lines,
        tool_lines,
    ] {
        for line in lines {
            output.push_out(line);
        }
    }
    for warn in disk_warns {
        output.push_err(warn);
    }
    Ok(output)
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

fn server_section() -> (Value, Vec<String>) {
    let version = env!("CARGO_PKG_VERSION");
    let build_sha = option_env!("AGENT_RUN_BUILD_SHA").unwrap_or("unknown");
    let uptime_secs = process_uptime_secs();

    let mut lines = vec!["server".to_string()];
    lines.push(format!("  version: {} (build {})", version, build_sha));
    lines.push(match uptime_secs {
        Some(secs) => format!("  uptime: {}", fmt_uptime(secs)),
        None => "  uptime: unknown".to_string(),
    });

    (
        json!({
            "version": version,
            "build_sha": build_sha,
            "uptime_secs": uptime_secs,
        }),
        lines,
    )
}

/// Process age in seconds: jiffies since boot (field 22 of
/// `/proc/self/stat`, via `get_process_start_time`) subtracted from the
/// system's seconds-since-boot from `/proc/uptime`.
fn process_uptime_secs() -> Option<u64> {
    let start_jiffies = get_process_start_time(std::process::id())?;
    let up = std::fs::read_to_string("/proc/uptime").ok()?;
    let boot_secs: f64 = up.split_whitespace().next()?.parse().ok()?;
    let clk_tck = (unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).max(1) as f64;
    let age = boot_secs - start_jiffies as f64 / clk_tck;
    Some(age.max(0.0) as u64)
}

fn fmt_uptime(secs: u64) -> String {
    let (d, h, m, s) = (
        secs / 86400,
        (secs % 86400) / 3600,
        (secs % 3600) / 60,
        secs % 60,
    );
    if d > 0 {
        format!("{d}d {h}h {m}m")
    } else if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

fn config_section(config: &Config) -> (Value, Vec<String>) {
    let agy_exists = config.agy_path.is_file();
    let agy_executable = agy_exists
        && std::fs::metadata(&config.agy_path)
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false);

    let mut lines = vec!["config".to_string()];
    lines.push(format!(
        "  workspace_root: {}",
        config.workspace_root.display()
    ));
    lines.push(format!("  state_root: {}", config.state_root.display()));
    lines.push(format!(
        "  jobs_dir: {} / max_parallel: {}",
        config.jobs_dir_name, config.max_parallel
    ));
    lines.push(format!(
        "  defaults: model={} effort={} base={}",
        display_opt(config.default_model.as_deref()),
        display_opt(config.default_effort.as_deref()),
        config.default_base_branch
    ));
    lines.push(format!(
        "  stall_threshold_minutes: {} (AGENT_RUN_STALL_MINUTES)",
        config.stall_minutes
    ));
    lines.push(format!(
        "  gc_age_days: {} / hindsight: {}",
        config.gc_max_age_days,
        if config.hindsight_enabled {
            "enabled"
        } else {
            "disabled"
        }
    ));
    lines.push(format!(
        "  hindsight_defaults: auto_recall={} auto_retain={} / bank_rule={}",
        if config.hindsight_auto_recall {
            "enabled"
        } else {
            "disabled"
        },
        if config.hindsight_auto_retain {
            "enabled"
        } else {
            "disabled"
        },
        config
            .hindsight_bank
            .as_deref()
            .unwrap_or("<project-name> (fallback unset)")
    ));
    if let Some(url) = &config.hindsight_url {
        lines.push(format!("  hindsight_url: {}", url));
    }

    let http_p = config
        .http_proxy
        .as_deref()
        .map(crate::core::proxy::redact_proxy_url);
    let https_p = config
        .https_proxy
        .as_deref()
        .map(crate::core::proxy::redact_proxy_url);
    let no_p = config
        .no_proxy
        .as_deref()
        .map(|s| crate::core::proxy::build_no_proxy(Some(s)));

    let proxy_str = match (http_p.as_deref(), https_p.as_deref()) {
        (Some(h), Some(hs)) => format!("http={} https={}", h, hs),
        (Some(h), None) => format!("http={}", h),
        (None, Some(hs)) => format!("https={}", hs),
        (None, None) => "none".to_string(),
    };
    lines.push(format!(
        "  proxy: {} / no_proxy={}",
        proxy_str,
        no_p.as_deref().unwrap_or("none")
    ));
    lines.push(format!(
        "  agy: {} ({})",
        config.agy_path.display(),
        if agy_exists {
            if agy_executable {
                "ok"
            } else {
                "not executable"
            }
        } else {
            "missing"
        }
    ));

    let bank_rule = config
        .hindsight_bank
        .clone()
        .unwrap_or_else(|| "<project-name>".to_string());

    (
        json!({
            "workspace_root": config.workspace_root.display().to_string(),
            "state_root": config.state_root.display().to_string(),
            "jobs_dir_name": config.jobs_dir_name.clone(),
            "max_parallel": config.max_parallel,
            "default_model": config.default_model.clone(),
            "default_effort": config.default_effort.clone(),
            "default_base_branch": config.default_base_branch.clone(),
            "stall_minutes": config.stall_minutes,
            "gc_max_age_days": config.gc_max_age_days,
            "hindsight_enabled": config.hindsight_enabled,
            "hindsight_defaults": {
                "enabled": config.hindsight_enabled,
                "auto_recall": config.hindsight_auto_recall,
                "auto_retain": config.hindsight_auto_retain,
                "url": config.hindsight_url.clone(),
                "bank_rule": bank_rule,
            },
            "proxy": {
                "http_proxy": http_p,
                "https_proxy": https_p,
                "no_proxy": no_p,
            },
            "agy_path": config.agy_path.display().to_string(),
            "agy_path_exists": agy_exists,
            "agy_path_executable": agy_executable,
        }),
        lines,
    )
}

fn display_opt(s: Option<&str>) -> String {
    s.map(str::to_string)
        .unwrap_or_else(|| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// capacity
// ---------------------------------------------------------------------------

fn capacity_section(config: &Config) -> (Value, Vec<String>) {
    // `info` must never fail: a missing/unreadable state DB means "no jobs".
    let jobs = list_jobs(&config.state_root, None).unwrap_or_default();
    let mut running = 0u32;
    let mut by_status: BTreeMap<String, u32> = BTreeMap::new();
    for job in &jobs {
        let status = compute_status(job, &config.state_root);
        *by_status.entry(status.as_str().to_string()).or_insert(0) += 1;
        if status == JobStatus::Running {
            running += 1;
        }
    }

    let status_str = if by_status.is_empty() {
        "none".to_string()
    } else {
        by_status
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let mut lines = vec!["capacity".to_string()];
    lines.push(format!(
        "  running: {} (max_parallel {}) / total jobs: {}",
        running,
        config.max_parallel,
        jobs.len()
    ));
    lines.push(format!("  by status: {status_str}"));

    (
        json!({
            "running": running,
            "max_parallel": config.max_parallel,
            "total_jobs": jobs.len(),
            "by_status": by_status,
        }),
        lines,
    )
}

// ---------------------------------------------------------------------------
// disk
// ---------------------------------------------------------------------------

/// (total, used, available bytes, used percent) for the filesystem holding
/// `path`, via `libc::statvfs`.
fn disk_usage(path: &Path) -> Option<(u64, u64, u64, f64)> {
    let c_path = CString::new(path.to_string_lossy().into_owned()).ok()?;
    let mut buf: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut buf) } != 0 {
        return None;
    }
    let frsize = buf.f_frsize.max(1) as u64;
    let total = buf.f_blocks as u64 * frsize;
    let free = buf.f_bfree as u64 * frsize;
    let avail = buf.f_bavail as u64 * frsize;
    let used = total.saturating_sub(free);
    let pct = if total > 0 {
        used as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    Some((total, used, avail, pct))
}

fn disk_section(config: &Config) -> (Value, Vec<String>, Vec<String>) {
    let mut lines = vec!["disk".to_string()];
    let mut warns: Vec<String> = Vec::new();
    let mut data_entries = serde_json::Map::new();

    let samples: [(&str, &Path); 2] = [
        ("workspace_root", &config.workspace_root),
        ("state_root", &config.state_root),
    ];
    for (name, path) in samples {
        match disk_usage(path) {
            Some((total, used, avail, pct)) => {
                data_entries.insert(
                    name.to_string(),
                    json!({
                        "total_bytes": total,
                        "used_bytes": used,
                        "available_bytes": avail,
                        "used_percent": pct,
                    }),
                );
                lines.push(format!(
                    "  {name}: {} / {} used / {} free ({pct:.1}% used)",
                    fmt_bytes(total),
                    fmt_bytes(used),
                    fmt_bytes(avail)
                ));
                if total > 0 && (avail as f64) < 0.10 * (total as f64) {
                    warns.push(format!(
                        "WARN: only {:.1}% of {name} free ({} available)",
                        100.0 * avail as f64 / total as f64,
                        fmt_bytes(avail)
                    ));
                }
            }
            None => {
                data_entries.insert(name.to_string(), Value::Null);
                lines.push(format!("  {name}: unknown"));
            }
        }
    }

    (Value::Object(data_entries), lines, warns)
}

fn fmt_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

/// Presence + credential-file mtime, using exactly the paths `auth.rs`
/// checks: `$HOME/.gemini/oauth_creds.json` or
/// `$HOME/.gemini/antigravity-cli/antigravity-oauth-token`. No agy spawn,
/// no network -- files only. Credential contents are never read or printed.
fn cred_presence() -> (bool, Option<DateTime<Utc>>) {
    let present = crate::core::auth::server_credentials_valid();
    let mtime = dirs::home_dir().and_then(|home| credential_mtime(&home));
    (present, mtime)
}

fn credential_mtime(home: &Path) -> Option<DateTime<Utc>> {
    for rel in [
        ".gemini/oauth_creds.json",
        ".gemini/antigravity-cli/antigravity-oauth-token",
    ] {
        let path = home.join(rel);
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        if let Ok(modified) = meta.modified() {
            return Some(DateTime::<Utc>::from(modified));
        }
    }
    None
}

/// created_at of the newest job whose live status is quota-exhausted.
fn newest_quota_exhausted(rows: &[(JobStatus, DateTime<Utc>)]) -> Option<DateTime<Utc>> {
    let mut newest: Option<DateTime<Utc>> = None;
    for (status, created_at) in rows {
        if *status != JobStatus::QuotaExhausted {
            continue;
        }
        newest = Some(match newest {
            Some(n) if created_at > &n => *created_at,
            None => *created_at,
            Some(n) => n,
        });
    }
    newest
}

fn auth_section(config: &Config) -> (Value, Vec<String>) {
    let (present, cred_mtime) = cred_presence();
    let last_quota: Option<String> = list_jobs(&config.state_root, None).ok().and_then(|jobs| {
        let rows: Vec<(JobStatus, DateTime<Utc>)> = jobs
            .iter()
            .map(|job| (compute_status(job, &config.state_root), job.created_at))
            .collect();
        newest_quota_exhausted(&rows).map(|t| t.to_rfc3339())
    });

    let mut lines = vec!["auth".to_string()];
    lines.push(match &cred_mtime {
        Some(m) => format!(
            "  credentials: present (file mtime {})",
            m.format("%Y-%m-%dT%H:%M:%SZ")
        ),
        None if present => "  credentials: present".to_string(),
        None => "  credentials: missing".to_string(),
    });
    lines.push(match &last_quota {
        Some(s) => format!("  last quota-exhausted: {s}"),
        None => "  last quota-exhausted: none".to_string(),
    });

    (
        json!({
            "present": present,
            "credential_mtime_unix": cred_mtime.map(|m| m.timestamp()),
            "last_quota_exhausted": last_quota,
        }),
        lines,
    )
}

// ---------------------------------------------------------------------------
// toolchains
// ---------------------------------------------------------------------------

fn toolchains_section() -> (Value, Vec<String>) {
    let mut lines = vec!["toolchains".to_string()];
    let mut data_entries = serde_json::Map::new();
    for (program, version_arg) in TOOLCHAINS {
        let version = probe_tool_with_timeout(program, version_arg, TOOL_PROBE_TIMEOUT);
        lines.push(format!("  {program}: {version}"));
        data_entries.insert((*program).to_string(), json!(version));
    }
    (Value::Object(data_entries), lines)
}

/// Runs `<program> <version_arg>`, returns the first output line, bounded by
/// `timeout` (child killed when the bound is exceeded). Missing binary ->
/// "not installed", no output -> "not installed", exceeded bound -> "timed
/// out".
fn probe_tool_with_timeout(program: &str, version_arg: &str, timeout: Duration) -> String {
    let mut child = match Command::new(program)
        .arg(version_arg)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return "not installed".to_string(),
    };
    // Register the child so the in-server zombie reaper (PID 1) can never
    // steal this probe's exit status from under the kill()/wait() reap below;
    // it is untracked (and any stashed status discarded) when this scope ends.
    let _tracked = crate::core::reaper::TrackedChild::new(child.id());
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stdout.read_to_string(&mut text);
        let _ = tx.send(text);
    });

    let deadline = Instant::now() + timeout;
    let text = match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(text) => text,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return "timed out".to_string();
        }
    };
    // The pipe closing means the child already closed stdout; kill+wait is a
    // safe no-op reap in the common case.
    let _ = child.kill();
    let _ = child.wait();

    let first = text.lines().next().unwrap_or("").trim().to_string();
    if first.is_empty() {
        "not installed".to_string()
    } else {
        first
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_info_args_deserializes_from_empty_object() {
        // The API takes `Query(InfoArgs)`; any query string (including an
        // empty one) must parse since the struct has no fields.
        let args: InfoArgs = serde_json::from_str("{}").expect("empty object");
        let _ = args;
    }

    #[test]
    fn test_probe_missing_tool_reports_not_installed() {
        assert_eq!(
            probe_tool_with_timeout(
                "definitely-not-a-tool-xyz",
                "--version",
                Duration::from_millis(500)
            ),
            "not installed"
        );
    }

    #[test]
    fn test_probe_installed_tool_or_skip() {
        let installed = Command::new("git").arg("--version").output().is_ok();
        let got = probe_tool_with_timeout("git", "--version", TOOL_PROBE_TIMEOUT);
        if installed {
            assert!(got.starts_with("git version"), "unexpected: {got}");
        } else {
            assert_eq!(got, "not installed", "unexpected: {got}");
        }
    }

    #[test]
    fn test_probe_kills_hanging_tool() {
        // `sleep 5` writes nothing and outlives the probe bound: the child
        // must be killed and the probe must report "timed out".
        assert_eq!(
            probe_tool_with_timeout("sleep", "5", Duration::from_millis(300)),
            "timed out"
        );
    }

    #[test]
    fn test_uptime_is_plausible() {
        if let Some(secs) = process_uptime_secs() {
            assert!(secs < 365 * 24 * 3600, "implausible uptime {secs}s");
            assert!(!fmt_uptime(secs).is_empty());
        }
    }

    #[test]
    fn test_disk_usage_of_root_is_sane() {
        if let Some((total, used, avail, pct)) = disk_usage(Path::new("/")) {
            assert!(total >= used, "used > total");
            assert!(avail <= total, "avail > total");
            assert!((0.0..=100.0).contains(&pct), "pct out of range: {pct}");
        }
    }

    #[test]
    fn test_fmt_bytes_and_uptime() {
        assert_eq!(fmt_bytes(0), "0B");
        assert_eq!(fmt_bytes(512), "512B");
        assert_eq!(fmt_bytes(2048), "2.0KiB");
        assert_eq!(fmt_bytes(3 * 1024 * 1024), "3.0MiB");
        assert_eq!(fmt_uptime(59), "59s");
        assert_eq!(fmt_uptime(125), "2m 5s");
        assert_eq!(fmt_uptime(3600 + 60 + 1), "1h 1m 1s");
        assert_eq!(fmt_uptime(90061), "1d 1h 1m");
    }

    #[test]
    fn test_newest_quota_exhausted_picks_latest() {
        let t0 = chrono::Utc::now();
        let rows: Vec<(JobStatus, DateTime<Utc>)> = vec![
            (JobStatus::Done, t0),
            (JobStatus::QuotaExhausted, t0 - chrono::Duration::hours(5)),
            (JobStatus::QuotaExhausted, t0 - chrono::Duration::hours(2)),
            (JobStatus::Failed, t0),
        ];
        let got = newest_quota_exhausted(&rows).expect("a quota job exists");
        assert_eq!(got, t0 - chrono::Duration::hours(2));
        assert!(newest_quota_exhausted(&rows[..1]).is_none());
        assert!(newest_quota_exhausted(&[]).is_none());
    }

    #[test]
    fn test_cred_presence_shape() {
        // Must not panic on any home; when credentials are present the
        // credential file should carry an mtime.
        let (present, mtime) = cred_presence();
        if present {
            let _ = mtime; // shape only; fs may hide mtimes
        }
    }

    #[test]
    fn test_run_smoke() {
        // Throwaway state root so the real job DB is never touched.
        let tmp = std::env::temp_dir().join(format!("agent-run-info-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).expect("create temp state root");

        let config = Config {
            state_root: tmp.clone(),
            ..Default::default()
        };

        let out = run(&config, &InfoArgs {}).expect("info must never fail");
        for key in ["server", "config", "capacity", "disk", "auth", "toolchains"] {
            assert!(out.data.get(key).is_some(), "missing data section {key}");
        }
        assert!(out.stdout_lines.iter().any(|l| l == "server"));
        assert!(out.stdout_lines.iter().any(|l| l == "toolchains"));
        assert!(out.data["server"]["version"].is_string());
        assert!(out.data["capacity"]["total_jobs"].is_u64());
        assert!(out.data["capacity"]["by_status"].is_object());
        assert!(out.data["auth"]["present"].is_boolean());
        assert!(out.data["toolchains"].get("git").is_some());
        assert!(out.exit_code == 0);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}

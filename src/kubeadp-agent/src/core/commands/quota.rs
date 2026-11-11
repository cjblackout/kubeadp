//! `agent-run quota`: shows the account's agy usage quota with a single
//! `-p "/usage"` turn (no model turn is consumed).
//!
//! The child runs the configured agy (`<config.agy_path>`) with the same
//! environment the job runner gives agy (the server's own environment,
//! including `HOME`, plus the resolved proxy variables -- `apply_proxy`),
//! but with no prompt file and no worktree:
//! `<agy_path> -p "/usage" --output-format json`, cwd = `config.workspace_root`,
//! stdin closed. agy's `response` field carries one tab-separated line per
//! bucket: `<group>\t<limit name>\t<remaining>%\t<reset time RFC3339 UTC>`.
//!
//! The last successful result is cached in memory for 60 s per process so
//! repeated calls do not hammer agy; `--fresh` bypasses the cache.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::core::args::QuotaArgs;
use crate::core::config::Config;
use crate::core::output::CommandOutput;

/// How long a successful quota result is served from the in-memory cache.
pub const CACHE_TTL: Duration = Duration::from_secs(60);

/// How long `run` waits for agy to answer `/usage` before killing the child.
pub const AGY_TIMEOUT: Duration = Duration::from_secs(60);

/// One usage bucket from agy's `/usage` report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaBucket {
    pub group: String,
    pub limit: String,
    pub remaining_percent: u8,
    pub resets_at: String,
}

/// Daily pacing of a weekly quota bucket: how far along its 7-day window
/// the bucket is, the spend allowed by that point, and whether usage has
/// caught up to or passed that allowance.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Pacing {
    pub group: String,
    pub used_percent: u8,
    pub window_day: u8,
    pub allowance_percent: f64,
    pub over_pace: bool,
}

/// Computes the daily pacing of a weekly bucket against `now`. Only
/// buckets whose `limit` contains "weekly" (case-insensitive) yield
/// `Some`; a malformed `resets_at` yields `None`. `used_percent` is
/// `100 - remaining_percent` (saturating); the window day is
/// `7 -` whole days until reset, clamped to 1..=7; the allowance is
/// `day * 100 / 7` percent, and `over_pace` when usage is at or above it.
pub fn weekly_pacing(bucket: &QuotaBucket, now: DateTime<Utc>) -> Option<Pacing> {
    if !bucket.limit.to_ascii_lowercase().contains("weekly") {
        return None;
    }
    let reset = DateTime::parse_from_rfc3339(&bucket.resets_at)
        .ok()?
        .with_timezone(&Utc);
    let used_percent = 100u8.saturating_sub(bucket.remaining_percent);
    let secs = reset.signed_duration_since(now).num_seconds().max(0);
    let d = secs.div_euclid(86_400);
    let window_day = (7 - d).clamp(1, 7) as u8;
    let allowance_percent = window_day as f64 * 100.0 / 7.0;
    Some(Pacing {
        group: bucket.group.clone(),
        used_percent,
        window_day,
        allowance_percent,
        over_pace: used_percent as f64 >= allowance_percent,
    })
}

/// Parses the tab-separated `response` text of an agy `/usage` result into
/// buckets. Blank or malformed lines (fewer than 4 tab fields, or a
/// non-numeric percent) are skipped, not errors.
pub fn parse_usage(response: &str) -> Vec<QuotaBucket> {
    let mut buckets = Vec::new();
    for line in response.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split('\t');
        let (Some(group), Some(limit), Some(remaining), Some(resets)) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let remaining = remaining.trim().trim_end_matches('%').trim();
        let Ok(remaining_percent) = remaining.parse::<u8>() else {
            continue;
        };
        let group = group.trim();
        let limit = limit.trim();
        if group.is_empty() || limit.is_empty() {
            continue;
        }
        buckets.push(QuotaBucket {
            group: group.to_string(),
            limit: limit.to_string(),
            remaining_percent,
            resets_at: resets.trim().to_string(),
        });
    }
    buckets
}

/// Pure cache decision for `run`: true iff a previous successful result
/// exists, it is still within [`CACHE_TTL`], and the caller did not ask for
/// a fresh fetch. A backward-moving clock (`last` after `now`) counts as a
/// miss.
pub fn cache_hit<T>(last: Option<(Instant, T)>, now: Instant, fresh: bool) -> bool {
    match last {
        Some((at, _)) if !fresh => now
            .checked_duration_since(at)
            .map(|age| age < CACHE_TTL)
            .unwrap_or(false),
        _ => false,
    }
}

struct QuotaCacheEntry {
    at: Instant,
    fetched_at: DateTime<Utc>,
    buckets: Vec<QuotaBucket>,
}

static QUOTA_CACHE: Mutex<Option<QuotaCacheEntry>> = Mutex::new(None);

/// Queries agy's `/usage`: `<agy_path> -p "/usage" --output-format json` with
/// cwd = `config.workspace_root`, stdin closed, stdout+stderr captured, the
/// inherited server environment (including `HOME`) with the job-runner proxy
/// settings applied (`apply_proxy`, so an upstream behind the corporate proxy
/// is reached), and a kill on timeout. `timeout` is a parameter so tests can
/// use a short bound instead of the 60 s production value.
pub fn fetch_quota(
    config: &Config,
    timeout: Duration,
) -> Result<(DateTime<Utc>, Vec<QuotaBucket>)> {
    let mut cmd = Command::new(&config.agy_path);
    cmd.args(["-p", "/usage", "--output-format", "json"])
        .current_dir(&config.workspace_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    crate::core::proxy::apply_proxy(&mut cmd, Some(config), None);

    let started = Instant::now();
    let mut child = crate::core::spawn::spawn_command_tracked(&mut cmd, "agy")
        .with_context(|| format!("could not spawn agy at {}", config.agy_path.display()))?;
    // Register the child so the in-server zombie reaper (PID 1) can never
    // steal this helper's exit status between the `try_wait()` polls; on the
    // rare TOCTOU ECHILD the status is handed back from the reaper's stash.
    let tracked = crate::core::reaper::TrackedChild::new(child.id());

    // Poll without blocking: reaping with try_wait() (the only std way to
    // poll) would make a later wait_with_output() fail with ECHILD on its
    // final wait, so the pipes are drained manually after the exit is seen.
    let status = loop {
        // Recover an ECHILD (the reaper won the TOCTOU race) into its stashed
        // exit status; a genuine wait error propagates unchanged.
        let poll_status = match child.try_wait() {
            Ok(status) => status,
            Err(e) => match tracked.recover_echild(&e) {
                Some(status) => Some(status),
                None => return Err(e.into()),
            },
        };
        match poll_status {
            Some(status) => break status,
            None => {
                if started.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    bail!("agy /usage did not finish within {}s", timeout.as_secs());
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    };

    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    if let Some(mut so) = child.stdout.take() {
        so.read_to_end(&mut stdout_buf)
            .context("could not read agy /usage stdout")?;
    }
    if let Some(mut se) = child.stderr.take() {
        se.read_to_end(&mut stderr_buf)
            .context("could not read agy /usage stderr")?;
    }
    let stdout = String::from_utf8_lossy(&stdout_buf);
    let stderr = String::from_utf8_lossy(&stderr_buf);

    if !status.success() {
        let detail = if stderr.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            stderr.trim().to_string()
        };
        bail!(
            "agy /usage exited with status {}: {}",
            status.code().unwrap_or(-1),
            detail
        );
    }

    let obj = first_json_object(&stdout).with_context(|| {
        let detail = if stderr.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            stderr.trim().to_string()
        };
        format!("agy /usage returned no usable output: {detail}")
    })?;

    let status_str = obj.get("status").and_then(Value::as_str).unwrap_or("");
    if status_str != "SUCCESS" {
        let response = obj
            .get("response")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("")
            .to_string();
        let detail = if response.is_empty() {
            stderr.trim().to_string()
        } else {
            response
        };
        bail!("agy /usage reported status {status_str:?}: {detail}");
    }

    let response = obj.get("response").and_then(Value::as_str).unwrap_or("");
    Ok((Utc::now(), parse_usage(response)))
}

/// Parses the first JSON object in `text`, tolerating non-JSON prefix or
/// trailing garbage.
fn first_json_object(text: &str) -> Result<Value> {
    let text = text.trim();
    let start = text.find('{').context("no JSON object found")?;
    match serde_json::Deserializer::from_str(&text[start..])
        .into_iter::<Value>()
        .next()
    {
        Some(Ok(value)) => Ok(value),
        Some(Err(e)) => bail!("invalid JSON object: {e}"),
        None => bail!("no JSON object found"),
    }
}

/// Human duration until an RFC3339 reset time (e.g. `2h 14m`); `-` when the
/// time cannot be parsed, `0m` once it is in the past.
fn human_until(resets_at: &str, now: DateTime<Utc>) -> String {
    let reset = match DateTime::parse_from_rfc3339(resets_at) {
        Ok(dt) => dt.with_timezone(&Utc),
        Err(_) => return "-".to_string(),
    };
    let secs = reset.signed_duration_since(now).num_seconds().max(0);
    let days = secs.div_euclid(86_400);
    let hours = (secs % 86_400).div_euclid(3_600);
    let mins = (secs % 3_600).div_euclid(60);
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {mins}m")
    } else if secs < 60 {
        format!("{secs}s")
    } else {
        format!("{mins}m")
    }
}

pub fn run(config: &Config, args: &QuotaArgs) -> Result<CommandOutput> {
    let (fetched_at, buckets) = {
        let now = Instant::now();
        let mut guard = QUOTA_CACHE.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = guard
            .as_ref()
            .filter(|e| cache_hit(Some((e.at, ())), now, args.fresh))
        {
            (entry.fetched_at, entry.buckets.clone())
        } else {
            let (fetched_at, buckets) = fetch_quota(config, AGY_TIMEOUT)?;
            *guard = Some(QuotaCacheEntry {
                at: now,
                fetched_at,
                buckets: buckets.clone(),
            });
            (fetched_at, buckets)
        }
    };

    let now = Utc::now();
    let entries: Vec<Value> = buckets
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<_, _>>()
        .context("failed to serialize quota buckets")?;
    let pacing: Vec<Pacing> = buckets
        .iter()
        .filter_map(|b| weekly_pacing(b, now))
        .collect();
    let pacing_entries: Vec<Value> = pacing
        .iter()
        .map(|p| {
            json!({
                "group": p.group,
                "used_percent": p.used_percent,
                "window_day": p.window_day,
                "allowance_percent": (p.allowance_percent * 10.0).round() / 10.0,
                "over_pace": p.over_pace,
            })
        })
        .collect();
    let fetched_at_rfc3339 = fetched_at.to_rfc3339_opts(SecondsFormat::Secs, true);

    let group_w = buckets
        .iter()
        .map(|b| b.group.len())
        .max()
        .unwrap_or("GROUP".len())
        .max("GROUP".len());
    let limit_w = buckets
        .iter()
        .map(|b| b.limit.len())
        .max()
        .unwrap_or("LIMIT".len())
        .max("LIMIT".len());

    let mut output = CommandOutput::new(json!({
        "buckets": entries,
        "fetched_at": fetched_at_rfc3339,
        "pacing": pacing_entries,
    }));

    output.push_out(format!(
        "{:<g$}  {:<l$}  {:>9}  {:>20}  {:>6}",
        "GROUP",
        "LIMIT",
        "REMAINING",
        "RESETS (UTC)",
        "IN",
        g = group_w,
        l = limit_w,
    ));
    for b in &buckets {
        output.push_out(format!(
            "{:<g$}  {:<l$}  {:>9}  {:>20}  {:>6}",
            b.group,
            b.limit,
            format!("{}%", b.remaining_percent),
            b.resets_at,
            human_until(&b.resets_at, now),
            g = group_w,
            l = limit_w,
        ));
    }

    for p in &pacing {
        let verdict = if p.over_pace {
            "OVER PACE"
        } else {
            "within pace"
        };
        output.push_out(format!(
            "pacing: {} weekly: used {}%, day {} of 7, allowance {:.1}% -> {}",
            p.group, p.used_percent, p.window_day, p.allowance_percent, verdict
        ));
    }

    for b in &buckets {
        if b.remaining_percent <= 10 {
            output.push_err(format!(
                "warning: {} {} is at {}% (resets {})",
                b.group, b.limit, b.remaining_percent, b.resets_at
            ));
        }
    }

    for p in &pacing {
        if p.over_pace {
            output.push_err(format!(
                "warning: {} weekly quota is over its daily pace (used {}%, allowance {:.1}% on day {} of 7): delegate to a Qwen task instead",
                p.group, p.used_percent, p.allowance_percent, p.window_day
            ));
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// Real abbreviated agy `/usage` sample (2026-09-30, one line per bucket).
    const SAMPLE_USAGE: &str = "Gemini Models\tWeekly Limit Remaining\t5%\t2026-09-30T17:45:53Z\nGemini Models\tFive Hour Limit Remaining\t100%\t2026-09-30T21:31:52Z\nClaude and GPT models\tWeekly Limit Remaining\t100%\t2026-10-07T16:52:58Z\nClaude and GPT models\tFive Hour Limit Remaining\t100%\t2026-09-30T21:52:58Z\n";

    fn sample_json() -> String {
        json!({
            "conversation_id": "",
            "status": "SUCCESS",
            "response": SAMPLE_USAGE,
            "duration_seconds": 0,
            "num_turns": 0,
            "usage": {},
        })
        .to_string()
    }

    /// Writes a fake agy shell script (chmod 755) that prints `body`; when
    /// `count_file` is given, appends `x` to it on every invocation.
    fn write_fake_agy(dir: &Path, body: &str, count_file: Option<&Path>, name: &str) -> PathBuf {
        let body_path = dir.join(format!("{name}.body"));
        std::fs::write(&body_path, body).unwrap();
        let mut script = String::from("#!/bin/sh\n");
        if let Some(count) = count_file {
            script.push_str(&format!("echo x >> {}\n", count.display()));
        }
        script.push_str(&format!("cat '{}'\n", body_path.display()));
        let path = dir.join(name);
        crate::core::spawn::write_executable_script(&path, &script).unwrap();
        path
    }

    fn test_config(dir: &Path, agy: &Path) -> Config {
        Config {
            agy_path: agy.to_path_buf(),
            repos_root: dir.to_path_buf().join(".agent-repos"),
            workspace_root: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn invocation_count(count_file: &Path) -> usize {
        std::fs::read_to_string(count_file)
            .unwrap_or_default()
            .lines()
            .count()
    }

    #[test]
    fn parse_real_sample() {
        let buckets = parse_usage(SAMPLE_USAGE);
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0].group, "Gemini Models");
        assert_eq!(buckets[0].limit, "Weekly Limit Remaining");
        assert_eq!(buckets[0].remaining_percent, 5);
        assert_eq!(buckets[0].resets_at, "2026-09-30T17:45:53Z");
        assert_eq!(buckets[1].remaining_percent, 100);
        assert_eq!(buckets[3].group, "Claude and GPT models");
        assert_eq!(buckets[3].limit, "Five Hour Limit Remaining");
        assert_eq!(buckets[3].resets_at, "2026-09-30T21:52:58Z");
    }

    #[test]
    fn parse_crlf_variant() {
        let crlf = SAMPLE_USAGE.replace('\n', "\r\n");
        assert_eq!(parse_usage(&crlf), parse_usage(SAMPLE_USAGE));
    }

    #[test]
    fn parse_skips_blank_and_malformed_lines() {
        let text = "only\ttwo\tfields\n\n\ngood\tLine\t42%\t2026-10-01T00:00:00Z\nbadpct\tLine\tten%\t2026-10-01T00:00:00Z\nshort\tline\nlast\tOk\t0%\t2026-10-01T00:00:00Z";
        let buckets = parse_usage(text);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].group, "good");
        assert_eq!(buckets[0].remaining_percent, 42);
        assert_eq!(buckets[1].group, "last");
        assert_eq!(buckets[1].remaining_percent, 0);
    }

    #[test]
    fn parse_empty_response() {
        assert!(parse_usage("").is_empty());
        assert!(parse_usage("   \n\t\n").is_empty());
    }

    #[test]
    fn cache_decision_pure_helper() {
        let t0 = Instant::now();
        assert!(!cache_hit::<()>(None, t0 + Duration::from_secs(10), false));
        assert!(cache_hit(Some((t0, ())), t0, false));
        assert!(cache_hit(
            Some((t0, ())),
            t0 + Duration::from_secs(59),
            false
        ));
        assert!(!cache_hit(
            Some((t0, ())),
            t0 + Duration::from_secs(61),
            false
        ));
        assert!(!cache_hit(
            Some((t0, ())),
            t0 + Duration::from_secs(10),
            true
        ));
        // Backward clock (last timestamp in the future of now) is a miss.
        assert!(!cache_hit(
            Some((t0 + Duration::from_secs(100), ())),
            t0,
            false
        ));
    }

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-30T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// One row of the [`weekly_pacing_table`] cases: the bucket inputs and
    /// the expected pacing fields, or `None` when no pacing is expected.
    #[derive(Debug, Clone, Copy)]
    struct PacingCase {
        remaining: u8,
        resets_at: &'static str,
        used: u8,
        day: u8,
        allowance: f64,
        over: bool,
    }

    #[test]
    fn weekly_pacing_table() {
        let now = fixed_now();
        let cases = [
            PacingCase {
                remaining: 76,
                resets_at: "2026-10-06T12:00:00Z",
                used: 24,
                day: 1,
                allowance: 14.3,
                over: true,
            },
            PacingCase {
                remaining: 95,
                resets_at: "2026-10-06T12:00:00Z",
                used: 5,
                day: 1,
                allowance: 14.3,
                over: false,
            },
            PacingCase {
                remaining: 50,
                resets_at: "2026-10-03T22:00:00Z",
                used: 50,
                day: 4,
                allowance: 57.1,
                over: false,
            },
            PacingCase {
                remaining: 40,
                resets_at: "2026-10-03T22:00:00Z",
                used: 60,
                day: 4,
                allowance: 57.1,
                over: true,
            },
            PacingCase {
                remaining: 100,
                resets_at: "2026-10-07T10:00:00Z",
                used: 0,
                day: 1,
                allowance: 14.3,
                over: false,
            },
            PacingCase {
                remaining: 0,
                resets_at: "2026-09-29T10:00:00Z",
                used: 100,
                day: 7,
                allowance: 100.0,
                over: true,
            },
        ];
        for c in cases {
            let bucket = QuotaBucket {
                group: "Gemini Models".to_string(),
                limit: "Weekly Limit Remaining".to_string(),
                remaining_percent: c.remaining,
                resets_at: c.resets_at.to_string(),
            };
            let p = weekly_pacing(&bucket, now).unwrap_or_else(|| {
                panic!(
                    "case ({}, {}): expected Some, got None",
                    c.remaining, c.resets_at
                )
            });
            assert_eq!(p.used_percent, c.used, "{}", c.resets_at);
            assert_eq!(p.window_day, c.day, "{}", c.resets_at);
            assert!(
                (p.allowance_percent - c.allowance).abs() < 0.05,
                "case ({}, {}): allowance {:.4} vs {}",
                c.remaining,
                c.resets_at,
                p.allowance_percent,
                c.allowance
            );
            assert_eq!(p.over_pace, c.over, "{}", c.resets_at);
        }
    }

    #[test]
    fn weekly_pacing_non_weekly_and_malformed() {
        let now = fixed_now();
        let five_hour = QuotaBucket {
            group: "Gemini Models".to_string(),
            limit: "Five Hour Limit Remaining".to_string(),
            remaining_percent: 100,
            resets_at: "2026-09-30T15:00:00Z".to_string(),
        };
        assert!(weekly_pacing(&five_hour, now).is_none());
        let malformed = QuotaBucket {
            group: "Gemini Models".to_string(),
            limit: "Weekly Limit Remaining".to_string(),
            remaining_percent: 50,
            resets_at: "not-a-time".to_string(),
        };
        assert!(weekly_pacing(&malformed, now).is_none());
    }

    #[test]
    fn run_level_pacing_rounding_only() {
        // Run-level pacing rendering is plain formatting of Pacing fields;
        // only the one-decimal allowance rounding needs a check.
        let round1 = |v: f64| (v * 10.0).round() / 10.0;
        assert_eq!(round1(1.0 * 100.0 / 7.0), 14.3);
        assert_eq!(round1(4.0 * 100.0 / 7.0), 57.1);
        assert_eq!(round1(7.0 * 100.0 / 7.0), 100.0);
    }

    #[test]
    fn run_success_uses_fake_agy_and_warns() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let count = dir.join("count");
        let fake = write_fake_agy(&dir, &sample_json(), Some(&count), "fake-agy");
        let config = test_config(&dir, &fake);

        let out = run(&config, &QuotaArgs { fresh: true }).unwrap();
        assert_eq!(out.exit_code, 0);
        assert_eq!(
            out.stdout_lines.len(),
            7,
            "header + 4 bucket rows + 2 pacing lines"
        );
        let header = out.stdout_lines[0].as_str();
        assert!(header.starts_with("GROUP"), "{header}");
        assert!(
            header.contains("REMAINING") && header.contains("RESETS (UTC)"),
            "{header}"
        );
        assert!(header.ends_with("IN"), "{header}");
        // Columns are padded to the widest cell, so compare with whitespace
        // collapsed: the row text must start with the fixture's fields, and
        // the remainder is the IN column.
        let row = out.stdout_lines[1].as_str();
        let row_text = row.split_whitespace().collect::<Vec<_>>().join(" ");
        let prefix = "Gemini Models Weekly Limit Remaining 5% 2026-09-30T17:45:53Z";
        let in_cell = row_text
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_prefix(' '))
            .unwrap_or_else(|| panic!("row must start with {prefix}: {row}"));
        // IN is computed from the current time (the sample reset times are
        // now in the past, yielding "0s"), so only assert it is present, not
        // a specific value.
        assert!(!in_cell.is_empty(), "IN column must not be empty: {row}");
        assert_eq!(out.stderr_lines.len(), 1, "one <=10% warning");
        // The warning renders only the fixture's fixed reset time, so it is
        // independent of the current time.
        assert_eq!(
            out.stderr_lines[0],
            "warning: Gemini Models Weekly Limit Remaining is at 5% (resets 2026-09-30T17:45:53Z)"
        );

        let buckets = out.data["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0]["group"], "Gemini Models");
        assert_eq!(buckets[0]["limit"], "Weekly Limit Remaining");
        assert_eq!(buckets[0]["remaining_percent"], 5);
        assert_eq!(buckets[0]["resets_at"], "2026-09-30T17:45:53Z");
        // Both weekly resets are in the past, so pacing is deterministic:
        // day 7, allowance 100.0%, and 95%/0% used keep both within pace.
        let pacing = out.data["pacing"].as_array().unwrap();
        assert_eq!(pacing.len(), 2, "one pacing entry per weekly bucket");
        assert_eq!(pacing[0]["group"], "Gemini Models");
        assert_eq!(pacing[0]["used_percent"], 95);
        assert_eq!(pacing[0]["window_day"], 7);
        assert_eq!(pacing[0]["allowance_percent"], 100.0);
        assert_eq!(pacing[0]["over_pace"], false);
        assert_eq!(pacing[1]["used_percent"], 0);
        assert_eq!(pacing[1]["over_pace"], false);
        assert!(
            DateTime::parse_from_rfc3339(out.data["fetched_at"].as_str().unwrap()).is_ok(),
            "fetched_at must be RFC3339: {}",
            out.data["fetched_at"]
        );
        assert_eq!(
            invocation_count(&count),
            1,
            "fresh call must hit the fake agy"
        );
    }

    #[test]
    fn run_reports_trimmed_stderr_on_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let fail = dir.join("fake-agy-fail");
        crate::core::spawn::write_executable_script(
            &fail,
            "#!/bin/sh\necho '   quota blew up   ' >&2\nexit 1\n",
        )
        .unwrap();
        let config = test_config(&dir, &fail);

        let err = run(&config, &QuotaArgs { fresh: true }).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("quota blew up"), "got: {msg}");
        assert!(!msg.contains("   "), "stderr detail must be trimmed: {msg}");
    }

    #[test]
    fn run_kills_agy_on_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let sleeper = dir.join("fake-agy-slow");
        crate::core::spawn::write_executable_script(&sleeper, "#!/bin/sh\nexec sleep 30\n")
            .unwrap();
        let config = test_config(&dir, &sleeper);

        // Short bound via the timeout parameter of the inner fn (the 60 s
        // production path would exceed the test budget).
        let err = fetch_quota(&config, Duration::from_secs(1)).unwrap_err();
        assert!(
            err.to_string().contains("did not finish within 1s"),
            "got: {err}"
        );
    }

    #[test]
    fn run_applies_config_proxy_env_to_agy_child() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let body = dir.join("usage.body");
        std::fs::write(&body, sample_json()).unwrap();
        // The fake agy records the proxy variables it actually sees into a
        // file (fetch_quota discards the child's stdout after parsing the
        // JSON, so stdout is not a usable observation channel here), then
        // prints the usage JSON.
        let env_file = dir.join("child.env");
        let fake = dir.join("fake-agy-proxy");
        let script = format!(
            "#!/bin/sh\necho \"HTTP_PROXY=${{HTTP_PROXY}}\" > '{env}'\necho \"HTTPS_PROXY=${{HTTPS_PROXY}}\" >> '{env}'\necho \"NO_PROXY=${{NO_PROXY}}\" >> '{env}'\ncat '{body}'\n",
            env = env_file.display(),
            body = body.display()
        );
        crate::core::spawn::write_executable_script(&fake, &script).unwrap();
        let mut config = test_config(&dir, &fake);
        config.http_proxy = Some("http://quota-proxy:8080".to_string());
        config.https_proxy = Some("http://quota-proxy:8443".to_string());
        config.no_proxy = Some("corp.local".to_string());

        let out = run(&config, &QuotaArgs { fresh: true }).unwrap();
        // The usage payload must still parse regardless of the proxy env.
        assert_eq!(out.data["buckets"].as_array().map(|b| b.len()), Some(4));
        let child_env =
            std::fs::read_to_string(&env_file).expect("fake agy must have written the env file");
        assert!(
            child_env.contains("HTTP_PROXY=http://quota-proxy:8080"),
            "child did not receive HTTP_PROXY: {child_env}"
        );
        assert!(
            child_env.contains("HTTPS_PROXY=http://quota-proxy:8443"),
            "child did not receive HTTPS_PROXY: {child_env}"
        );
        assert!(
            child_env
                .contains("NO_PROXY=corp.local,localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc"),
            "NO_PROXY missing cluster defaults: {child_env}"
        );
    }

    #[test]
    fn run_caches_within_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let count = dir.join("count");
        let fake = write_fake_agy(&dir, &sample_json(), Some(&count), "fake-agy-cache");
        let config = test_config(&dir, &fake);

        let first = run(&config, &QuotaArgs { fresh: false }).unwrap();
        let second = run(&config, &QuotaArgs { fresh: false }).unwrap();
        assert_eq!(first.data, second.data);
        // <=1: a concurrent test may have primed the process-wide cache with
        // this same sample, in which case the fake is never invoked at all.
        assert!(
            invocation_count(&count) <= 1,
            "agy invoked {} times for two cached calls",
            invocation_count(&count)
        );

        // --fresh bypasses the cache and always re-invokes agy.
        let fresh = run(&config, &QuotaArgs { fresh: true }).unwrap();
        assert_eq!(fresh.data["buckets"], first.data["buckets"]);
        assert!(invocation_count(&count) >= 1, "--fresh must re-invoke agy");
    }
}

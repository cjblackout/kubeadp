use std::process::Command;

use crate::core::config::Config;

/// Default hosts and domains that must always be excluded from proxying
/// whenever a proxy is active.
pub const DEFAULT_NO_PROXY_ENTRIES: &[&str] = &[
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    ".cluster.local",
    ".svc",
];

/// Redacts username and password credentials from a proxy URL.
///
/// The userinfo/host boundary is the LAST `'@'` of the authority, so a
/// password that itself contains `'@'` (or `':'`) cannot push part of the
/// credential onto the host side and leak it.
///
/// For example:
/// - `http://user:pass@proxy.example.com:8080` -> `http://***:***@proxy.example.com:8080`
/// - `http://user@proxy.example.com:8080` -> `http://***@proxy.example.com:8080`
/// - `http://user:pa@ss@proxy.example.com:8080` -> `http://***:***@proxy.example.com:8080`
/// - `http://proxy.example.com:8080` -> `http://proxy.example.com:8080`
pub fn redact_proxy_url(raw: &str) -> String {
    let trimmed = raw.trim();
    let (scheme, rest) = if let Some(pos) = trimmed.find("://") {
        let (s, r) = trimmed.split_at(pos + 3);
        (s, r)
    } else {
        ("", trimmed)
    };

    let authority = match rest.find('/') {
        Some(slash_idx) => &rest[..slash_idx],
        None => rest,
    };
    let path_and_query = &rest[authority.len()..];

    if let Some(at_idx) = authority.rfind('@') {
        let userinfo = &authority[..at_idx];
        let host_port = &authority[at_idx + 1..];
        let redacted_userinfo = if userinfo.contains(':') {
            "***:***"
        } else {
            "***"
        };
        format!("{scheme}{redacted_userinfo}@{host_port}{path_and_query}")
    } else {
        trimmed.to_string()
    }
}

/// Appends standard homelab/cluster exclusions (`localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc`)
/// to an existing `NO_PROXY` string, deduplicating case-insensitively.
pub fn build_no_proxy(existing: Option<&str>) -> String {
    let mut entries: Vec<String> = Vec::new();
    if let Some(s) = existing {
        for item in s.split(',') {
            let trimmed = item.trim();
            if !trimmed.is_empty()
                && !entries
                    .iter()
                    .any(|e: &String| e.eq_ignore_ascii_case(trimmed))
            {
                entries.push(trimmed.to_string());
            }
        }
    }
    for default in DEFAULT_NO_PROXY_ENTRIES {
        if !entries
            .iter()
            .any(|e: &String| e.eq_ignore_ascii_case(default))
        {
            entries.push(default.to_string());
        }
    }
    entries.join(",")
}

/// Configures proxy environment variables on a child process `Command`.
///
/// Precedence:
/// 1. `job_proxy == Some("none")` -> Proxy is disabled for this job. All proxy
///    environment variables are explicitly removed so process-level settings
///    do not leak into the child.
/// 2. `job_proxy == Some("<url>")` -> Both `HTTP_PROXY` and `HTTPS_PROXY` (and their
///    lowercase counterparts) are set to `<url>`. `NO_PROXY` is constructed with
///    the cluster defaults appended.
/// 3. `job_proxy == None` -> The settings from `Config` (`http_proxy`, `https_proxy`,
///    `no_proxy`) are applied if either HTTP or HTTPS proxy is set. If proxy is set,
///    cluster exclusions are appended to `NO_PROXY`. Both upper and lower case
///    variants are set.
pub fn apply_proxy(cmd: &mut Command, config: Option<&Config>, job_proxy: Option<&str>) {
    if let Some(proxy_val) = job_proxy {
        let clean = proxy_val.trim();
        if clean.eq_ignore_ascii_case("none") {
            // Explicitly disable proxy for this child process
            cmd.env_remove("HTTP_PROXY")
                .env_remove("http_proxy")
                .env_remove("HTTPS_PROXY")
                .env_remove("https_proxy")
                .env_remove("ALL_PROXY")
                .env_remove("all_proxy")
                .env_remove("NO_PROXY")
                .env_remove("no_proxy");
            return;
        } else if !clean.is_empty() {
            if let Some(vars) = resolved_proxy_env(config, Some(clean)) {
                set_env_vars(cmd, Some(&vars));
            }
            return;
        }
    }

    // Fall back to config settings or process environment (the process
    // environment branch only makes sense for children that inherit it; the
    // `env_clear`d children use [`resolved_proxy_env`], which skips it).
    if let Some(vars) = resolved_proxy_env(config, None) {
        set_env_vars(cmd, Some(&vars));
        return;
    }
    if config.is_none() {
        let env_vars = (
            std::env::var("AGENT_RUN_HTTP_PROXY")
                .ok()
                .or_else(|| get_process_env_case_insensitive("HTTP_PROXY")),
            std::env::var("AGENT_RUN_HTTPS_PROXY")
                .ok()
                .or_else(|| get_process_env_case_insensitive("HTTPS_PROXY")),
            std::env::var("AGENT_RUN_NO_PROXY")
                .ok()
                .or_else(|| get_process_env_case_insensitive("NO_PROXY")),
        );
        let (http, https, no) = env_vars;
        if let Some(vars) = env_proxy_vars(http, https, no) {
            set_env_vars(cmd, Some(&vars));
        }
    }
}

/// Applies a resolved proxy variable list to a child `Command`.
fn set_env_vars(cmd: &mut Command, vars: Option<&[(String, String)]>) {
    if let Some(vars) = vars {
        for (key, value) in vars {
            cmd.env(key, value);
        }
    }
}

/// The proxy environment variables implied by explicit `http_proxy`/
/// `https_proxy`/`no_proxy` values (both case variants, cluster bypasses
/// appended); `None` when no proxy is set.
fn env_proxy_vars(
    http_proxy: Option<String>,
    https_proxy: Option<String>,
    no_proxy: Option<String>,
) -> Option<Vec<(String, String)>> {
    let has_http = http_proxy
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let has_https = https_proxy
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if has_http.is_none() && has_https.is_none() {
        return None;
    }
    let no_proxy = build_no_proxy(no_proxy.as_deref());
    let mut vars = Vec::new();
    if let Some(http) = &has_http {
        vars.push(("HTTP_PROXY".to_string(), http.to_string()));
        vars.push(("http_proxy".to_string(), http.to_string()));
    }
    if let Some(https) = &has_https {
        vars.push(("HTTPS_PROXY".to_string(), https.to_string()));
        vars.push(("https_proxy".to_string(), https.to_string()));
    }
    vars.push(("NO_PROXY".to_string(), no_proxy.clone()));
    vars.push(("no_proxy".to_string(), no_proxy));
    Some(vars)
}

/// The proxy environment variables for a child spawned with a
/// NON-INHERITING (`env_clear`) environment, resolved with the same rules as
/// [`apply_proxy`]: a job proxy of `"<url>"` wins over the config, a job
/// proxy of `"none"` (or empty) sets nothing, and otherwise the config's
/// `http_proxy`/`https_proxy`/`no_proxy` are used. The process environment
/// is deliberately NOT consulted (that fallback only makes sense for
/// inherited-environment children, where [`apply_proxy`] still applies it).
///
/// Returns `None` when nothing should be set. Otherwise a list covering both
/// upper- and lower-case variants of the proxy variables, with the
/// always-appended cluster-bypass `NO_PROXY`/`no_proxy` values.
pub fn resolved_proxy_env(
    config: Option<&Config>,
    job_proxy: Option<&str>,
) -> Option<Vec<(String, String)>> {
    if let Some(proxy_val) = job_proxy {
        let clean = proxy_val.trim();
        if clean.is_empty() || clean.eq_ignore_ascii_case("none") {
            return None;
        }
        let no_proxy = build_no_proxy(config.and_then(|c| c.no_proxy.as_deref()));
        return Some(vec![
            ("HTTP_PROXY".to_string(), clean.to_string()),
            ("http_proxy".to_string(), clean.to_string()),
            ("HTTPS_PROXY".to_string(), clean.to_string()),
            ("https_proxy".to_string(), clean.to_string()),
            ("NO_PROXY".to_string(), no_proxy.clone()),
            ("no_proxy".to_string(), no_proxy),
        ]);
    }

    let cfg = config?;
    let http_proxy = cfg
        .http_proxy
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let https_proxy = cfg
        .https_proxy
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    if http_proxy.is_none() && https_proxy.is_none() {
        return None;
    }
    let no_proxy = build_no_proxy(cfg.no_proxy.as_deref());
    let mut vars = Vec::new();
    if let Some(http_proxy) = http_proxy {
        vars.push(("HTTP_PROXY".to_string(), http_proxy.clone()));
        vars.push(("http_proxy".to_string(), http_proxy));
    }
    if let Some(https_proxy) = https_proxy {
        vars.push(("HTTPS_PROXY".to_string(), https_proxy.clone()));
        vars.push(("https_proxy".to_string(), https_proxy));
    }
    vars.push(("NO_PROXY".to_string(), no_proxy.clone()));
    vars.push(("no_proxy".to_string(), no_proxy));
    Some(vars)
}

fn get_process_env_case_insensitive(key: &str) -> Option<String> {
    for (k, v) in std::env::vars() {
        if k.eq_ignore_ascii_case(key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_redact_proxy_url() {
        assert_eq!(
            redact_proxy_url("http://user:password@proxy.example.com:8080"),
            "http://***:***@proxy.example.com:8080"
        );
        assert_eq!(
            redact_proxy_url("http://user@proxy.example.com:8080"),
            "http://***@proxy.example.com:8080"
        );
        assert_eq!(
            redact_proxy_url("https://proxy.example.com:8080/path?q=1"),
            "https://proxy.example.com:8080/path?q=1"
        );
        assert_eq!(
            redact_proxy_url("http://bob:s3cr3t@10.0.0.1:3128/"),
            "http://***:***@10.0.0.1:3128/"
        );
        assert_eq!(redact_proxy_url("none"), "none");
    }

    #[test]
    fn test_redact_proxy_url_at_in_password_and_edge_cases() {
        // The split is at the LAST '@' of the authority, so a password that
        // contains '@' stays fully inside the redacted userinfo.
        assert_eq!(
            redact_proxy_url("http://user:pa@ss@host:8080"),
            "http://***:***@host:8080"
        );
        assert_eq!(
            redact_proxy_url("https://alice:s3cr3t@corp@gate.example.com:3128/socks"),
            "https://***:***@gate.example.com:3128/socks"
        );
        // Username without password: userinfo has no ':' -> single ***.
        assert_eq!(redact_proxy_url("http://user@host"), "http://***@host");
        // '@'-only userinfo edge (empty username, no password).
        assert_eq!(redact_proxy_url("http://@host"), "http://***@host");
        // No credentials: returned unchanged.
        assert_eq!(
            redact_proxy_url("http://host:8080/path"),
            "http://host:8080/path"
        );
        // No scheme: authority is still redacted.
        assert_eq!(redact_proxy_url("proxy:8080/user"), "proxy:8080/user");
        assert_eq!(
            redact_proxy_url("http://bob:pw@proxy:8080"),
            "http://***:***@proxy:8080"
        );
        // IPv6 host with userinfo.
        assert_eq!(
            redact_proxy_url("http://user:secret@[::1]:8080"),
            "http://***:***@[::1]:8080"
        );
        assert_eq!(
            redact_proxy_url("http://user@[fe80::1%eth0]:3128"),
            "http://***@[fe80::1%eth0]:3128"
        );
    }

    #[test]
    fn test_build_no_proxy_dedupes_and_appends() {
        let empty = build_no_proxy(None);
        assert_eq!(empty, "localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc");

        let custom = build_no_proxy(Some("127.0.0.1,internal.corp,localhost"));
        assert_eq!(
            custom,
            "127.0.0.1,internal.corp,localhost,0.0.0.0,.cluster.local,.svc"
        );
    }

    #[test]
    fn test_apply_proxy_per_job_override() {
        let mut cmd = Command::new("echo");
        let cfg = Config {
            http_proxy: Some("http://global-proxy:8080".to_string()),
            ..Default::default()
        };

        apply_proxy(&mut cmd, Some(&cfg), Some("http://job-proxy:9090"));
        let envs: HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();

        assert_eq!(
            envs.get("HTTP_PROXY"),
            Some(&Some("http://job-proxy:9090".to_string()))
        );
        assert_eq!(
            envs.get("http_proxy"),
            Some(&Some("http://job-proxy:9090".to_string()))
        );
        assert_eq!(
            envs.get("HTTPS_PROXY"),
            Some(&Some("http://job-proxy:9090".to_string()))
        );
        assert_eq!(
            envs.get("https_proxy"),
            Some(&Some("http://job-proxy:9090".to_string()))
        );
        assert!(envs.contains_key("NO_PROXY"));
        assert!(envs.contains_key("no_proxy"));
    }

    #[test]
    fn test_apply_proxy_disabled_with_none() {
        let mut cmd = Command::new("echo");
        cmd.env("HTTP_PROXY", "http://inherited:8080");
        let cfg = Config {
            http_proxy: Some("http://global-proxy:8080".to_string()),
            ..Default::default()
        };

        apply_proxy(&mut cmd, Some(&cfg), Some("none"));
        let envs: HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();

        assert_eq!(envs.get("HTTP_PROXY"), Some(&None));
        assert_eq!(envs.get("http_proxy"), Some(&None));
        assert_eq!(envs.get("HTTPS_PROXY"), Some(&None));
        assert_eq!(envs.get("https_proxy"), Some(&None));
    }

    #[test]
    fn test_apply_proxy_from_config() {
        let mut cmd = Command::new("echo");
        let cfg = Config {
            http_proxy: Some("http://global-http:8080".to_string()),
            https_proxy: Some("http://global-https:8443".to_string()),
            no_proxy: Some("corp.local".to_string()),
            ..Default::default()
        };

        apply_proxy(&mut cmd, Some(&cfg), None);
        let envs: HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|s| s.to_string_lossy().to_string()),
                )
            })
            .collect();

        assert_eq!(
            envs.get("HTTP_PROXY"),
            Some(&Some("http://global-http:8080".to_string()))
        );
        assert_eq!(
            envs.get("http_proxy"),
            Some(&Some("http://global-http:8080".to_string()))
        );
        assert_eq!(
            envs.get("HTTPS_PROXY"),
            Some(&Some("http://global-https:8443".to_string()))
        );
        assert_eq!(
            envs.get("https_proxy"),
            Some(&Some("http://global-https:8443".to_string()))
        );
        assert_eq!(
            envs.get("NO_PROXY"),
            Some(&Some(
                "corp.local,localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc".to_string()
            ))
        );
        assert_eq!(
            envs.get("no_proxy"),
            Some(&Some(
                "corp.local,localhost,127.0.0.1,0.0.0.0,.cluster.local,.svc".to_string()
            ))
        );
    }
}

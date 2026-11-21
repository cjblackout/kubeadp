//! Self-healing setup of agy's home directory.
//!
//! A ConfigMap file mounted with a `subPath` under `~/.gemini/config` makes the kubelet
//! create the missing parent directories as root, after which agy (running as the server's
//! uid) cannot create `~/.gemini/config/projects` or `~/.gemini/antigravity-cli`. When the
//! MCP config is mounted at a neutral path (`AGENT_RUN_AGY_MCP_SOURCE`) this module creates
//! the `.gemini` tree itself, as the server's uid, and installs the config there. It only
//! creates missing directories and rewrites `mcp_config.json`; agy's credentials under
//! `~/.gemini/antigravity-cli` are never touched.
//!
//! When the config key `a2a_mcp_enabled` (`AGENT_RUN_A2A_MCP_ENABLED`) is set, an `a2a`
//! MCP server entry (`kubeadp-agent mcp-a2a`) is merged into the installed
//! `mcp_config.json`, keeping every other server entry. The merge is idempotent (rerunning
//! with the same exe leaves the file unchanged) and atomic. The tool reads its own
//! identity/credential from the job environment (`A2A_SELF_NAME`, `KUBEADP_JOBSET_ID`,
//! `KUBEADP_JOBSET_CREDENTIALS_FILE`); nothing secret is ever written into this file.

use std::fs;
use std::io::Write;
use std::path::Path;

/// Environment variable that turns on the per-job `a2a` MCP server merge.
pub const A2A_MCP_ENABLED_ENV: &str = "AGENT_RUN_A2A_MCP_ENABLED";

/// Computes the `a2a` MCP server entry command (`kubeadp-agent mcp-a2a`) from the
/// current executable. `current_exe()` is the launcher in production (`kubeadp-agent`),
/// exactly like [`crate::sandbox::find_launcher_exe`]; the fallback keeps the config
/// usable in tests and environments where `current_exe()` is a test runner.
pub fn a2a_mcp_command() -> Vec<String> {
    let exe = crate::sandbox::find_launcher_exe();
    vec![exe.to_string_lossy().into_owned(), "mcp-a2a".to_string()]
}

/// Creates `<home>/.gemini/config` and, when `mcp_source` is given, installs the
/// sourced content to `<home>/.gemini/config/mcp_config.json` (atomically, on every
/// start, so the mounted ConfigMap stays the source of truth). When the `a2a` MCP
/// server is enabled (`a2a_mcp_enabled`, see [`A2A_MCP_ENABLED_ENV`]), the sourced
/// servers are merged with an `a2a` entry pointing at the current executable before the
/// write (keep other servers, idempotent, nothing secret).
pub fn prepare_agy_home(home: &Path, mcp_source: Option<&Path>) -> Result<(), String> {
    let config_dir = home.join(".gemini").join("config");
    fs::create_dir_all(&config_dir).map_err(|e| {
        format!(
            "cannot create {} ({e}); a root-owned directory in the home volume needs `chown 1000:1000`",
            config_dir.display()
        )
    })?;
    let Some(source) = mcp_source else {
        return Ok(());
    };
    let bytes = fs::read(source)
        .map_err(|e| format!("cannot read MCP config source {}: {e}", source.display()))?;
    let mut installed = bytes.to_vec();
    if a2a_mcp_enabled() {
        installed = merge_a2a_entry(&bytes, &a2a_mcp_command())?;
    }
    let target = config_dir.join("mcp_config.json");
    let tmp = config_dir.join(format!(
        ".mcp_config.json.tmp.{}.{}",
        std::process::id(),
        rand::random::<u32>()
    ));
    let write = || -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&installed)?;
        f.sync_all()?;
        fs::rename(&tmp, &target)
    };
    write().map_err(|e| {
        let _ = fs::remove_file(&tmp);
        format!("cannot install {}: {e}", target.display())
    })
}

/// Whether the per-job `a2a` MCP server entry is enabled (`[worker] a2a_mcp_enabled`,
/// `AGENT_RUN_A2A_MCP_ENABLED`). Stays false when unset.
fn a2a_mcp_enabled() -> bool {
    match std::env::var(A2A_MCP_ENABLED_ENV) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// Merges an `a2a` MCP server entry into the sourced `mcp_config.json` document.
///
/// The document is `{"mcpServers": { ... }}`; every existing server is kept and an
/// `a2a` entry (`command` + `args: ["mcp-a2a"]`) is inserted. The merge is idempotent:
/// when the source already carries the identical fresh `a2a` entry, the original bytes
/// are returned unchanged. A non-JSON or non-object source is an error -- a job must
/// never end up with a broken config.
fn merge_a2a_entry(source: &[u8], command: &[String]) -> Result<Vec<u8>, String> {
    let mut doc: serde_json::Value = serde_json::from_slice(source)
        .map_err(|e| format!("mcp_config.json source is not valid JSON: {e}"))?;
    let servers = doc
        .get_mut("mcpServers")
        .and_then(|v| v.as_object_mut())
        .ok_or_else(|| "mcp_config.json source has no 'mcpServers' object".to_string())?;

    let fresh = serde_json::json!({
        "command": command[0],
        "args": ["mcp-a2a"],
    });
    if servers.get("a2a") == Some(&fresh) {
        // Already merged; leave the document untouched so the check is idempotent.
        return Ok(source.to_vec());
    }
    servers.insert("a2a".to_string(), fresh);
    serde_json::to_vec_pretty(&doc)
        .map_err(|e| format!("cannot serialize merged mcp_config.json: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The merge tests that touch the process env are serialized so parallel
    /// Acquires the crate-wide test env lock (recovering from poison) and removes
    /// `AGENT_RUN_A2A_MCP_ENABLED` so tests start with a clean environment.
    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        let guard = crate::test_env_lock();
        std::env::remove_var(A2A_MCP_ENABLED_ENV);
        guard
    }

    fn command() -> Vec<String> {
        vec![
            "/usr/local/bin/kubeadp-agent".to_string(),
            "mcp-a2a".to_string(),
        ]
    }

    #[test]
    fn creates_dirs_and_copies_the_config() {
        let _guard = lock_env();
        let t = tempfile::tempdir().unwrap();
        let src = t.path().join("src.json");
        fs::write(&src, b"{\"mcpServers\":{}}").unwrap();
        let home = t.path().join("home");
        prepare_agy_home(&home, Some(&src)).unwrap();
        let got = fs::read(home.join(".gemini/config/mcp_config.json")).unwrap();
        assert_eq!(got, b"{\"mcpServers\":{}}");
    }

    #[test]
    fn overwrites_a_stale_config() {
        let _guard = lock_env();
        let t = tempfile::tempdir().unwrap();
        let home = t.path().join("home");
        fs::create_dir_all(home.join(".gemini/config")).unwrap();
        fs::write(home.join(".gemini/config/mcp_config.json"), b"stale").unwrap();
        let src = t.path().join("src.json");
        fs::write(&src, b"fresh").unwrap();
        prepare_agy_home(&home, Some(&src)).unwrap();
        assert_eq!(
            fs::read(home.join(".gemini/config/mcp_config.json")).unwrap(),
            b"fresh"
        );
    }

    #[test]
    fn unset_source_only_creates_the_dirs() {
        let _guard = lock_env();
        let t = tempfile::tempdir().unwrap();
        prepare_agy_home(t.path(), None).unwrap();
        assert!(t.path().join(".gemini/config").is_dir());
        assert!(!t.path().join(".gemini/config/mcp_config.json").exists());
    }

    #[test]
    fn missing_source_is_an_error_but_dirs_exist() {
        let _guard = lock_env();
        let t = tempfile::tempdir().unwrap();
        let err = prepare_agy_home(t.path(), Some(&t.path().join("nope.json"))).unwrap_err();
        assert!(err.contains("cannot read MCP config source"));
        assert!(t.path().join(".gemini/config").is_dir());
    }

    #[test]
    fn leaves_agy_credentials_alone() {
        let _guard = lock_env();
        let t = tempfile::tempdir().unwrap();
        let cli = t.path().join(".gemini/antigravity-cli");
        fs::create_dir_all(&cli).unwrap();
        fs::write(cli.join("antigravity-oauth-token"), b"secret").unwrap();
        prepare_agy_home(t.path(), None).unwrap();
        assert_eq!(
            fs::read(cli.join("antigravity-oauth-token")).unwrap(),
            b"secret"
        );
    }

    #[test]
    fn merge_keeps_existing_servers_and_adds_a2a() {
        let source =
            br#"{"mcpServers":{"playwright":{"disabled":false,"serverUrl":"http://pw/mcp"}}}"#;
        let merged = merge_a2a_entry(source, &command()).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        let servers = doc["mcpServers"].as_object().unwrap();
        assert!(servers.contains_key("playwright"), "existing server kept");
        assert_eq!(
            servers["playwright"]["serverUrl"], "http://pw/mcp",
            "existing server untouched"
        );
        assert_eq!(
            doc["mcpServers"]["a2a"],
            serde_json::json!({"command": "/usr/local/bin/kubeadp-agent", "args": ["mcp-a2a"]})
        );
        // The entry never carries credentials.
        let serialized = serde_json::to_string(&merged).unwrap();
        assert!(!serialized.contains("KUBEADP_JOBSET"));
        assert!(!serialized.contains("A2A_SELF_NAME"));
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn merge_is_idempotent() {
        let source =
            br#"{"mcpServers":{"playwright":{"disabled":false,"serverUrl":"http://pw/mcp"}}}"#;
        let once = merge_a2a_entry(source, &command()).unwrap();
        let twice = merge_a2a_entry(&once, &command()).unwrap();
        assert_eq!(once, twice, "a second merge leaves the file unchanged");
        // And an already-merged document is returned byte-for-byte unchanged.
        assert_eq!(twice, once);
    }

    #[test]
    fn merge_replaces_a_stale_a2a_entry() {
        let source = br#"{"mcpServers":{"a2a":{"command":"/old/binary","args":["mcp-a2a"]}}}"#;
        let merged = merge_a2a_entry(source, &command()).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        assert_eq!(
            doc["mcpServers"]["a2a"]["command"],
            "/usr/local/bin/kubeadp-agent"
        );
    }

    #[test]
    fn merge_rejects_non_json_source() {
        let err = merge_a2a_entry(b"not json", &command()).unwrap_err();
        assert!(err.contains("not valid JSON"), "{err}");
        let err2 = merge_a2a_entry(b"{\"foo\":1}", &command()).unwrap_err();
        assert!(err2.contains("no 'mcpServers' object"), "{err2}");
    }

    #[test]
    fn enable_flag_defaults_false_and_parses_bools() {
        let _guard = lock_env();
        assert!(!a2a_mcp_enabled());
        std::env::set_var(A2A_MCP_ENABLED_ENV, "true");
        assert!(a2a_mcp_enabled());
        std::env::set_var(A2A_MCP_ENABLED_ENV, "1");
        assert!(a2a_mcp_enabled());
        std::env::set_var(A2A_MCP_ENABLED_ENV, "false");
        assert!(!a2a_mcp_enabled());
        std::env::set_var(A2A_MCP_ENABLED_ENV, "0");
        assert!(!a2a_mcp_enabled());
        std::env::set_var(A2A_MCP_ENABLED_ENV, "garbage");
        assert!(!a2a_mcp_enabled());
        std::env::remove_var(A2A_MCP_ENABLED_ENV);
    }

    #[test]
    fn prepare_agy_home_merges_a2a_when_enabled_and_is_noop_when_disabled() {
        let _guard = lock_env();
        let t = tempfile::tempdir().unwrap();
        let src = t.path().join("src.json");
        let source_json = r#"{"mcpServers":{"keep":{"serverUrl":"http://k/mcp"}}}"#;
        fs::write(&src, source_json).unwrap();

        // Disabled (default): the source is copied verbatim, no a2a entry.
        let home_off = t.path().join("home-off");
        prepare_agy_home(&home_off, Some(&src)).unwrap();
        let off = fs::read(home_off.join(".gemini/config/mcp_config.json")).unwrap();
        assert_eq!(off, source_json.as_bytes());

        // Enabled: the a2a entry is merged in, other servers kept, no secret.
        std::env::set_var(A2A_MCP_ENABLED_ENV, "true");
        let home_on = t.path().join("home-on");
        prepare_agy_home(&home_on, Some(&src)).unwrap();
        let on = fs::read(home_on.join(".gemini/config/mcp_config.json")).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&on).unwrap();
        assert_eq!(doc["mcpServers"]["keep"]["serverUrl"], "http://k/mcp");
        assert!(doc["mcpServers"].as_object().unwrap().contains_key("a2a"));
        // Re-run with the same config is a no-op (byte-identical file).
        let tmp = home_on.join(".gemini/config/mcp_config.json");
        let before = fs::read(&tmp).unwrap();
        prepare_agy_home(&home_on, Some(&src)).unwrap();
        let after = fs::read(&tmp).unwrap();
        assert_eq!(before, after, "idempotent write");
        std::env::remove_var(A2A_MCP_ENABLED_ENV);
    }
}

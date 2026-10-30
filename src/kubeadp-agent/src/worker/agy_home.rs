//! Self-healing setup of agy's home directory.
//!
//! A ConfigMap file mounted with a `subPath` under `~/.gemini/config` makes the kubelet
//! create the missing parent directories as root, after which agy (running as the server's
//! uid) cannot create `~/.gemini/config/projects` or `~/.gemini/antigravity-cli`. When the
//! MCP config is mounted at a neutral path (`AGENT_RUN_AGY_MCP_SOURCE`) this module creates
//! the `.gemini` tree itself, as the server's uid, and installs the config there. It only
//! creates missing directories and rewrites `mcp_config.json`; agy's credentials under
//! `~/.gemini/antigravity-cli` are never touched.

use std::fs;
use std::io::Write;
use std::path::Path;

/// Creates `<home>/.gemini/config` and, when `mcp_source` is given, copies it to
/// `<home>/.gemini/config/mcp_config.json` (atomically, on every start, so the mounted
/// ConfigMap stays the source of truth).
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
    let target = config_dir.join("mcp_config.json");
    let tmp = config_dir.join(".mcp_config.json.tmp");
    let write = || -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, &target)
    };
    write().map_err(|e| {
        let _ = fs::remove_file(&tmp);
        format!("cannot install {}: {e}", target.display())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_dirs_and_copies_the_config() {
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
        let t = tempfile::tempdir().unwrap();
        prepare_agy_home(t.path(), None).unwrap();
        assert!(t.path().join(".gemini/config").is_dir());
        assert!(!t.path().join(".gemini/config/mcp_config.json").exists());
    }

    #[test]
    fn missing_source_is_an_error_but_dirs_exist() {
        let t = tempfile::tempdir().unwrap();
        let err = prepare_agy_home(t.path(), Some(&t.path().join("nope.json"))).unwrap_err();
        assert!(err.contains("cannot read MCP config source"));
        assert!(t.path().join(".gemini/config").is_dir());
    }

    #[test]
    fn leaves_agy_credentials_alone() {
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
}

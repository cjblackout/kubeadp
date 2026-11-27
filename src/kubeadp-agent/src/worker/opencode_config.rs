//! Self-healing setup of opencode's A2A injection channels (design HL-113
//! §7.14, KA-7).
//!
//! opencode has no Claude-format hooks and no plugin "install" CLI: it loads
//! plugins from the `plugin` array of its config file and MCP servers from the
//! `mcp` object of the same file (verified live against opencode 1.19.4, whose
//! config schema has a `plugin: string[]` and an `mcp: {name: {type:"local"|
//! "remote", command, environment, ...}}`). This module therefore materialises
//! both into the per-run opencode home:
//!
//! - `~/.config/opencode/opencode.json`: the harness's A2A wiring, merged
//!   WITHOUT clobbering the operator's own config (every existing key and the
//!   existing `plugin`/`mcp` entries are kept; only missing entries are
//!   inserted), idempotent and atomic.
//!   - `plugin`: an entry for the harness's bundled `a2a` opencode plugin
//!     (`~/.config/opencode/plugin/a2a/`), whose `plugin/index.js` implements
//!     the `hook` channel (see below).
//!   - `mcp.a2a`: a `{type:"local", command:[<exe>,"mcp-a2a"]}` entry pointing
//!     at the harness-owned `mcp-a2a` stdio server, the `tool_result` channel
//!     (exactly like the claude `--mcp-config` and the agy `mcp_config.json`).
//! - `~/.config/opencode/plugin/a2a/plugin/index.js`: the harness's opencode
//!   plugin (see the module doc of [`plugin`]).
//!
//! opencode resolves its global config at `$XDG_CONFIG_HOME/opencode` (default
//! `~/.config/opencode`); the module targets that directory through the
//! caller-supplied home. The write is best-effort (an error is logged; the
//! `tool_result` channel still delivers), exactly like the claude/agy setup.
//!
//! A RUNNING opencode job gets its `idle_turn` from the finished-resume path
//! (the engine's `idle_turn`); the plugin additionally exposes the `hook`
//! mid-turn channel and the `mcp-a2a` the `tool_result` channel, so opencode
//! reaches the full §7.14 ladder (SPEC §3.4, §3.5).

use std::path::{Path, PathBuf};

/// The plugin directory under the opencode config root
/// (`<config_dir>/plugin/a2a/`).
pub const PLUGIN_DIR_NAME: &str = "a2a";
/// The plugin subdirectory opencode loads as the plugin module root
/// (a `<root>/plugin/index.js` entry point, like the example plugins).
pub const PLUGIN_ROOT_SUBDIR: &str = "plugin";

/// The opencode config file name (global config, `$XDG_CONFIG_HOME/opencode`).
pub const CONFIG_FILE_NAME: &str = "opencode.json";

/// The plugin name opencode uses in the `plugin` array (a directory path is
/// the ``node'' form opencode resolves; keep the name stable for the merge).
pub const PLUGIN_NAME: &str = "a2a";

/// The config root for an opencode home: `<home>/.config/opencode/` (the
/// default `Global.Path.config`; `$XDG_CONFIG_HOME` overrides it, but the
/// harness runs opencode with a plain `HOME`, so the default applies).
pub fn config_dir_for(home: &Path) -> PathBuf {
    home.join(".config").join("opencode")
}

/// The harness plugin root: `<config_dir>/plugin/a2a/`.
pub fn plugin_dir_for(home: &Path) -> PathBuf {
    config_dir_for(home).join("plugin").join(PLUGIN_DIR_NAME)
}

/// The plugin module root opencode loads: `<plugin_dir>/plugin/`.
pub fn plugin_root_for(home: &Path) -> PathBuf {
    plugin_dir_for(home).join(PLUGIN_ROOT_SUBDIR)
}

/// The config file path: `<config_dir>/opencode.json`.
pub fn config_file_for(home: &Path) -> PathBuf {
    config_dir_for(home).join(CONFIG_FILE_NAME)
}

/// Whether the run-enabled `a2a` MCP server entry is on
/// (`KUBEADP_A2A_MCP_ENABLED`, a run-agnostic host default). Stays true when
/// unset, mirroring the claude job driver which always passes `--mcp-config`.
pub fn a2a_mcp_enabled() -> bool {
    match std::env::var("KUBEADP_A2A_MCP_ENABLED") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
}

/// The `mcp.a2a` server entry command: the current executable with the
/// `mcp-a2a` subcommand (the harness-owned stdio MCP server, the
/// `tool_result` channel). `current_exe()` is the launcher in production,
/// exactly like [`crate::sandbox::find_launcher_exe`].
pub fn a2a_mcp_command() -> Vec<String> {
    let exe = crate::sandbox::find_launcher_exe();
    vec![exe.to_string_lossy().into_owned(), "mcp-a2a".to_string()]
}

/// The default `plugin` entry pattern (the node form opencode resolves as a
/// directory with a `plugin/index.js` entry point).
pub fn plugin_entry(home: &Path) -> String {
    plugin_root_for(home).to_string_lossy().into_owned()
}

/// The `plugin/index.js` entry point of the harness's opencode A2A plugin.
///
/// opencode's `experimental.chat.messages.transform` hook fires right before
/// the messages are sent to the model (verified live: appending a text part
/// to the last user message reaches the model context). The plugin uses it as
/// the `hook` channel: it drains the run's A2A inbox (named by
/// `KUBEADP_A2A_INBOX_DIR`) through the harness `a2a-hook transform`
/// subcommand (which prints the joined framed text and marks the messages
/// injected once stdout is written) and appends that text to the last user
/// part of the outgoing messages, so every queued message lands in the model's
/// context at the turn boundary.
///
/// The `a2a-hook` subcommand is spawned as a child process (the plugin runs
/// inside the opencode process and has no other way to reach the harness);
/// a missing inbox, an empty drain or a failed child is a graceful no-op -- a
/// hook must never block a prompt. The `event` hook is registered but unused
/// (a placeholder for `session.idle`-driven idling in future phases).
pub fn plugin_js() -> &'static str {
    r#"import { spawnSync } from "node:child_process";

// The harness `a2a-hook transform` event drains the run's A2A inbox
// (KUBEADP_A2A_INBOX_DIR) and prints the joined framed text; the messages are
// acknowledged once the text reaches stdout (N10 at-least-once: a crash before
// ack re-delivers the message after the claim lease).
function drainInbox() {
  const exe = process.env.KUBEADP_A2A_HOOK_BIN || "kubeadp-agent";
  const r = spawnSync(exe, ["a2a-hook", "transform"], {
    encoding: "utf8",
    timeout: 5000,
    maxBuffer: 128 * 1024,
  });
  if (r.status !== 0 || !r.stdout) return "";
  const out = r.stdout.trim();
  return out === "{}" ? "" : out;
}

export default async function kubeadpA2A(input, options) {
  return {
    "experimental.chat.messages.transform": async (input, output) => {
      const framed = drainInbox();
      if (!framed) return;
      const msgs = output.messages || [];
      // Append into the LAST user message so the model sees it as user (not
      // system) input at the current turn boundary.
      const lastUser = [...msgs].reverse().find((m) => m.info && m.info.role === "user");
      if (!lastUser) return;
      if (!Array.isArray(lastUser.parts)) lastUser.parts = [];
      lastUser.parts.push({ type: "text", text: "\n" + framed });
    },
    // Placeholder for idle-driven turns (future phases); opencode's session
    // status/idle events land here.
    event: async () => {},
  };
}
"#
}

/// Writes the harness opencode plugin into `<config_dir>/plugin/a2a/plugin/
/// index.js` (idempotent: an identical rewrite leaves the file unchanged).
fn write_plugin(home: &Path) -> Result<(), String> {
    let root = plugin_root_for(home);
    std::fs::create_dir_all(&root)
        .map_err(|e| format!("failed to create {}: {e}", root.display()))?;
    let entry = root.join("index.js");
    write_if_changed(&entry, plugin_js().as_bytes())
}

/// Writes `content` to `path` only when the current content differs (so an
/// identical rewrite never bumps the mtime), via a temp file and rename.
fn write_if_changed(path: &Path, content: &[u8]) -> Result<(), String> {
    if let Ok(existing) = std::fs::read(path) {
        if existing == content {
            return Ok(());
        }
    }
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    ));
    std::fs::write(&tmp, content).map_err(|e| format!("failed to write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("failed to publish {}: {e}", path.display())
    })
}

/// Merges the harness's A2A `plugin` entry and `mcp.a2a` server into the
/// opencode config document. Every existing key and every existing
/// `plugin`/`mcp` entry is kept; only a MISSING A2A entry is inserted. Returns
/// `None` when the document already carries both, `Some(serialized)` when it
/// was (or needed to be) rewritten.
fn merge_a2a_into_config(home: &Path, doc: &mut serde_json::Value) -> Option<String> {
    let entry = plugin_entry(home);
    let command = a2a_mcp_command();
    let a2a_server = serde_json::json!({
        "type": "local",
        "command": command,
    });

    let obj = match doc.as_object_mut() {
        Some(o) => o,
        None => {
            *doc = serde_json::json!({});
            doc.as_object_mut().unwrap()
        }
    };

    let mut changed = false;
    // plugin: only insert when the harness entry is absent.
    let plugins = obj.entry("plugin").or_insert_with(|| serde_json::json!([]));
    let array = match plugins.as_array_mut() {
        Some(a) => a,
        None => {
            *plugins = serde_json::json!([]);
            plugins.as_array_mut().unwrap()
        }
    };
    let has_plugin = array.iter().any(|p| match p {
        serde_json::Value::String(s) => s == &entry,
        _ => false,
    });
    if !has_plugin {
        array.push(serde_json::Value::String(entry));
        changed = true;
    }

    // mcp.a2a: only insert when absent (or identical -- the entry is idempotent).
    let mcp = obj.entry("mcp").or_insert_with(|| serde_json::json!({}));
    let servers = match mcp.as_object_mut() {
        Some(o) => o,
        None => {
            *mcp = serde_json::json!({});
            mcp.as_object_mut().unwrap()
        }
    };
    if servers.get("a2a") != Some(&a2a_server) {
        servers.insert("a2a".to_string(), a2a_server);
        changed = true;
    }

    if !changed {
        return None;
    }
    serde_json::to_string_pretty(doc).ok()
}

/// Prepares the opencode home for A2A injection: writes the harness plugin
/// and merges the `plugin` + `mcp.a2a` entries into `~/.config/opencode/
/// opencode.json` (idempotent, atomic, never clobbers operator config). The
/// plugin write and the merge are best-effort: an error names the failing
/// step (the `tool_result` channel still delivers through `mcp-a2a`).
pub fn prepare_opencode_home(home: &Path) -> Result<(), String> {
    write_plugin(home)?;
    let config_path = config_file_for(home);
    // Read the existing config (absent -> empty object).
    let mut doc: serde_json::Value = if config_path.is_file() {
        std::fs::read_to_string(&config_path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
            .unwrap_or_else(|| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };
    if let Some(updated) = merge_a2a_into_config(home, &mut doc) {
        write_if_changed(&config_path, updated.as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        crate::test_env_lock()
    }

    #[test]
    fn paths_resolve_under_the_opencode_home() {
        let home = Path::new("/home/test");
        assert_eq!(
            config_dir_for(home),
            PathBuf::from("/home/test/.config/opencode")
        );
        assert_eq!(
            plugin_dir_for(home),
            PathBuf::from("/home/test/.config/opencode/plugin/a2a")
        );
        assert_eq!(
            plugin_root_for(home),
            PathBuf::from("/home/test/.config/opencode/plugin/a2a/plugin")
        );
        assert_eq!(
            config_file_for(home),
            PathBuf::from("/home/test/.config/opencode/opencode.json")
        );
    }

    #[test]
    fn plugin_entry_is_the_plugin_root() {
        let home = Path::new("/home/test");
        assert_eq!(
            plugin_entry(home),
            "/home/test/.config/opencode/plugin/a2a/plugin"
        );
    }

    #[test]
    fn prepare_opencode_home_writes_plugin_and_merges_config() {
        let _guard = lock_env();
        std::env::remove_var("KUBEADP_A2A_MCP_ENABLED");
        let t = tempfile::tempdir().unwrap();
        let home = t.path().join("home");
        prepare_opencode_home(&home).unwrap();

        // The plugin entry point exists.
        let entry = plugin_root_for(&home).join("index.js");
        assert!(entry.is_file(), "plugin entry point: {}", entry.display());
        let js = std::fs::read_to_string(&entry).unwrap();
        assert!(js.contains("a2a-hook"));
        assert!(js.contains("messages.transform"));

        // The config file carries the plugin entry + the a2a MCP server.
        let config_path = config_file_for(&home);
        assert!(config_path.is_file());
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        let plugins = doc["plugin"].as_array().unwrap();
        assert!(
            plugins
                .iter()
                .any(|p| p.as_str() == Some(&plugin_entry(&home))),
            "plugin entry present: {doc}"
        );
        let a2a = &doc["mcp"]["a2a"];
        assert_eq!(a2a["type"], "local");
        assert_eq!(a2a["command"][1], "mcp-a2a");
        // The first element is a non-empty launcher path (the test build
        // resolves it through the serial test env, so only the shape is
        // asserted, not the exact binary name).
        assert!(matches!(a2a["command"][0].as_str(), Some(c) if !c.is_empty()));
        // Never a credential in the config.
        let serialized = serde_json::to_string(&doc).unwrap();
        assert!(!serialized.contains("KUBEADP_JOBSET"));
        assert!(!serialized.contains("A2A_SELF_NAME"));
    }

    #[test]
    fn prepare_opencode_home_is_idempotent() {
        let _guard = lock_env();
        std::env::remove_var("KUBEADP_A2A_MCP_ENABLED");
        let t = tempfile::tempdir().unwrap();
        let home = t.path().join("home");
        prepare_opencode_home(&home).unwrap();
        let config_path = config_file_for(&home);
        let entry_path = plugin_root_for(&home).join("index.js");
        let before_cfg = std::fs::read(&config_path).unwrap();
        let before_js = std::fs::read(&entry_path).unwrap();
        #[cfg(unix)]
        let mtime = std::fs::metadata(&config_path).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        prepare_opencode_home(&home).unwrap();
        assert_eq!(std::fs::read(&config_path).unwrap(), before_cfg);
        assert_eq!(std::fs::read(&entry_path).unwrap(), before_js);
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(&config_path).unwrap().modified().unwrap(),
            mtime,
            "an identical rewrite must not bump the config mtime"
        );
    }

    #[test]
    fn prepare_opencode_home_keeps_operator_config() {
        let _guard = lock_env();
        std::env::remove_var("KUBEADP_A2A_MCP_ENABLED");
        let t = tempfile::tempdir().unwrap();
        let home = t.path().join("home");
        let config_path = config_file_for(&home);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            r#"{
  "model": "provider/model",
  "plugin": ["/user/plugin"],
  "mcp": {"other": {"type": "remote", "url": "http://x/mcp"}}
}"#,
        )
        .unwrap();

        prepare_opencode_home(&home).unwrap();
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(doc["model"], "provider/model", "operator model kept");
        let plugins: Vec<&str> = doc["plugin"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(plugins, vec!["/user/plugin", &plugin_entry(&home)]);
        assert_eq!(doc["mcp"]["other"]["type"], "remote");
        assert_eq!(doc["mcp"]["a2a"]["type"], "local");
    }

    #[test]
    fn merge_replaces_a_stale_a2a_server_entry() {
        let _guard = lock_env();
        std::env::remove_var("KUBEADP_A2A_MCP_ENABLED");
        let t = tempfile::tempdir().unwrap();
        let home = t.path().join("home");
        let config_path = config_file_for(&home);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            r#"{"mcp":{"a2a":{"type":"local","command":["/old/binary","mcp-a2a"]}}}"#,
        )
        .unwrap();
        prepare_opencode_home(&home).unwrap();
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert!(
            !doc["mcp"]["a2a"]["command"][0]
                .as_str()
                .unwrap()
                .contains("old"),
            "stale a2a entry replaced: {doc}"
        );
        assert_eq!(doc["mcp"]["a2a"]["command"][1], "mcp-a2a");
    }

    #[test]
    fn a2a_mcp_enabled_default_true_and_parses_bools() {
        let _guard = lock_env();
        std::env::remove_var("KUBEADP_A2A_MCP_ENABLED");
        assert!(a2a_mcp_enabled(), "default true");
        for on in ["1", "true", "yes", "on"] {
            std::env::set_var("KUBEADP_A2A_MCP_ENABLED", on);
            assert!(a2a_mcp_enabled(), "{on}");
        }
        for off in ["0", "false", "no", "off"] {
            std::env::set_var("KUBEADP_A2A_MCP_ENABLED", off);
            assert!(!a2a_mcp_enabled(), "{off}");
        }
        std::env::set_var("KUBEADP_A2A_MCP_ENABLED", "garbage");
        assert!(a2a_mcp_enabled(), "garbled override falls back to true");
        std::env::remove_var("KUBEADP_A2A_MCP_ENABLED");
    }
}

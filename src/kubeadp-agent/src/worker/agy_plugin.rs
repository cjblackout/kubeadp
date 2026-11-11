//! The NATIVE agy plugin carrying the A2A hook channel (design HL-113 §7.14,
//! phase 2; verified live against agy 1.5.5). agy rejects Claude-format
//! plugins for hooks (a converted plugin's `PreInvocation` is dropped) and
//! rejects `--settings` hooks, so the plugin is native: a plugin ROOT with
//! `.antigravity-plugin/plugin.json` plus `hooks.json` AT THE PLUGIN ROOT,
//! flat handler lists (no Claude `matcher`/`hooks` nesting):
//!
//! ```json
//! {"a2a":{"PreInvocation":[{"type":"command","command":"<abs> a2a-hook pre-invocation"}],
//!         "PostInvocation":[...],"Stop":[...]}}
//! ```
//!
//! Hook commands run via `sh -c` with cwd = the plugin dir, so the binary
//! path is ABSOLUTE. Each hook reads the run's inbox from the environment
//! (`KUBEADP_A2A_INBOX_DIR`, set per run by the launcher): `PreInvocation`
//! injects before every model call (the `hook` channel), `PostInvocation`
//! re-enters the loop with `terminationBehavior: force_continue` and `Stop`
//! returns `{"decision":"continue",...}` (the agy `idle_turn` channel). The
//! worker installs the plugin (`agy plugin install <dir>`); it never touches
//! the user's other plugins.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Name of the plugin under the run state root (`<state_root>/a2a-plugin/`).
pub const PLUGIN_DIR_NAME: &str = "a2a-plugin";

/// The plugin name (also the `hooks.json` top-level key agy matches under).
pub const PLUGIN_NAME: &str = "a2a";

/// How long the worker waits for `agy plugin install` (best effort: the
/// plugin files stay on disk and the hook channel simply never fires until
/// it is installed).
pub const INSTALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The hook command for one event: the absolute launcher binary path plus
/// the a2a-hook event name (sh -c runs hooks with cwd = the plugin dir, so
/// the path must be absolute).
pub fn hook_command(event: &str) -> String {
    let exe = crate::sandbox::find_launcher_exe();
    format!("{} a2a-hook {}", exe.to_string_lossy(), event)
}

/// The plugin directory for a run state root: `<state_root>/a2a-plugin/`.
pub fn plugin_dir_for(state_root: &Path) -> PathBuf {
    state_root.join(PLUGIN_DIR_NAME)
}

/// The `hooks.json` document: the plugin name matched against flat handler
/// lists, one per event, each a single `command` handler. Pure (no I/O).
pub fn hooks_json() -> serde_json::Value {
    serde_json::json!({
        PLUGIN_NAME: {
            "PreInvocation": [
                { "type": "command", "command": hook_command("pre-invocation") }
            ],
            "PostInvocation": [
                { "type": "command", "command": hook_command("post-invocation") }
            ],
            "Stop": [
                { "type": "command", "command": hook_command("stop") }
            ],
        }
    })
}

/// The `plugin.json` manifest inside `.antigravity-plugin/`. Pure (no I/O).
pub fn plugin_json() -> serde_json::Value {
    serde_json::json!({
        "name": PLUGIN_NAME,
        "version": env!("CARGO_PKG_VERSION"),
    })
}

/// Writes the NATIVE agy plugin into `dir`:
/// `<dir>/.antigravity-plugin/plugin.json` plus `hooks.json` AT THE PLUGIN
/// ROOT (next to the manifest), with the flat handler-list structure above.
/// Idempotent: an identical rewrite leaves the files unchanged.
pub fn write_plugin(dir: &Path) -> Result<(), String> {
    let marker = dir.join(".antigravity-plugin");
    std::fs::create_dir_all(&marker)
        .map_err(|e| format!("failed to create {}: {e}", marker.display()))?;
    let manifest = marker.join("plugin.json");
    write_if_changed(
        &manifest,
        &serde_json::to_string_pretty(&plugin_json()).unwrap_or_default(),
    )?;
    let hooks = dir.join("hooks.json");
    write_if_changed(
        &hooks,
        &serde_json::to_string_pretty(&hooks_json()).unwrap_or_default(),
    )?;
    Ok(())
}

/// Writes `content` to `path` only when the current content differs (so an
/// identical rewrite never bumps the mtime), via a temp file and rename.
fn write_if_changed(path: &Path, content: &str) -> Result<(), String> {
    if let Ok(existing) = std::fs::read_to_string(path) {
        if existing == content {
            return Ok(());
        }
    }
    let tmp = path.with_extension(format!("tmp-{}", now_unix()));
    std::fs::write(&tmp, content.as_bytes())
        .map_err(|e| format!("failed to write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("failed to publish {}: {e}", path.display()))
}

/// Installs the plugin for the worker's runs (the content is
/// run-independent -- each hook reads the run's inbox from the environment
/// -- so one install covers every run): writes it under
/// `<state_root>/a2a-plugin/` and runs `<agy> plugin install <dir>` with the
/// job's agy home (`HOME`). Best effort: a missing agy binary or a failed
/// install is an error for the caller to log (the hook channel then never
/// fires; `tool_result` still delivers) -- never aborts the worker.
pub fn install(state_root: &Path, agy_path: &Path, agy_home: &Path) -> Result<(), String> {
    let dir = plugin_dir_for(state_root);
    write_plugin(&dir)?;
    if !agy_path.is_file() {
        return Err(format!(
            "agy binary {} not found: the a2a plugin is written ({}) but not installed",
            agy_path.display(),
            dir.display()
        ));
    }
    let mut cmd = Command::new(agy_path);
    cmd.arg("plugin")
        .arg("install")
        .arg(&dir)
        .env("HOME", agy_home)
        .env_remove("ANTIGRAVITY_API_KEY")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to run `agy plugin install {dir:?}`: {e}"))?;
    match wait_with_timeout(child, INSTALL_TIMEOUT) {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(format!(
            "`agy plugin install` failed (exit {status}); the plugin is written at {}",
            dir.display()
        )),
        Err(_) => Err(format!(
            "`agy plugin install` timed out after {}s; the plugin is written at {}",
            INSTALL_TIMEOUT.as_secs(),
            dir.display()
        )),
    }
}

/// Unix seconds now (local: the plugin writer is not a2a-gated).
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Waits for `child` up to `timeout`, killing it on expiry.
fn wait_with_timeout(
    mut child: std::process::Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus, ()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) => {}
            Err(_) => return Err(()),
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hooks_json_structure_matches_the_design_exactly() {
        let v = hooks_json();
        // The plugin name is the ONLY top-level key, with flat handler lists
        // (no matcher/hooks nesting) for the three events.
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 1, "exactly one plugin entry: {v}");
        let a2a = obj.get(PLUGIN_NAME).unwrap().as_object().unwrap();
        assert_eq!(
            a2a.keys().cloned().collect::<Vec<_>>(),
            vec![
                "PreInvocation".to_string(),
                "PostInvocation".to_string(),
                "Stop".to_string()
            ],
            "flat handler lists for the three events: {v}"
        );
        // Each handler is a command handler carrying the ABSOLUTE binary path
        // (sh -c runs with cwd = the plugin dir) plus the a2a-hook event.
        for (event, key) in [
            ("pre-invocation", "PreInvocation"),
            ("post-invocation", "PostInvocation"),
            ("stop", "Stop"),
        ] {
            let handlers = a2a[key].as_array().unwrap();
            assert_eq!(handlers.len(), 1, "{key}: {v}");
            assert_eq!(handlers[0]["type"], "command", "{key}: {v}");
            let command = handlers[0]["command"].as_str().unwrap();
            assert!(command.starts_with('/'), "absolute path: {command}");
            assert_eq!(command, hook_command(event), "{key}: {v}");
            assert!(
                command.ends_with(&format!("a2a-hook {event}")),
                "{key}: {v}"
            );
        }
        // No Claude-format fields anywhere.
        let serialized = serde_json::to_string(&v).unwrap();
        assert!(!serialized.contains("hookSpecificOutput"));
        assert!(!serialized.contains("matcher"));
        assert!(!serialized.contains("additionalContext"));
    }

    #[test]
    fn write_plugin_creates_the_layout_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("a2a-plugin");
        write_plugin(&dir).unwrap();
        // The manifest lives inside .antigravity-plugin/, the hooks at the
        // plugin ROOT next to it.
        assert!(dir.join(".antigravity-plugin/plugin.json").is_file());
        assert!(dir.join("hooks.json").is_file());
        let parsed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("hooks.json")).unwrap())
                .unwrap();
        assert_eq!(
            parsed,
            hooks_json(),
            "hooks.json structure exactly as designed"
        );
        let parsed: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(".antigravity-plugin/plugin.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(parsed, plugin_json());
        // Idempotent: an identical rewrite leaves the files unchanged.
        let before = std::fs::read(dir.join("hooks.json")).unwrap();
        write_plugin(&dir).unwrap();
        assert_eq!(std::fs::read(dir.join("hooks.json")).unwrap(), before);
    }

    #[test]
    fn hook_command_uses_the_absolute_launcher_path() {
        let cmd = hook_command("pre-invocation");
        assert!(cmd.starts_with('/'), "absolute binary path, got: {cmd}");
        assert!(cmd.ends_with(" a2a-hook pre-invocation"), "got: {cmd}");
        assert_eq!(
            plugin_dir_for(Path::new("/state")),
            PathBuf::from("/state/a2a-plugin")
        );
    }
}

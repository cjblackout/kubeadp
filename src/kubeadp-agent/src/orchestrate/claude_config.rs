use serde_json::{json, Value};
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Mutex;
use std::time::SystemTime;

static CONFIG_MUTEX: Mutex<()> = Mutex::new(());

/// Atomically writes `content` to `target_path` via tempfile + rename.
/// Preserves the original file's permission bits if it already exists, or sets 0600 for new files.
fn write_atomic_preserving_mode(target_path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create directory {}: {}", parent.display(), e))?;
    }

    #[cfg(unix)]
    let mode = fs::metadata(target_path)
        .map(|m| m.permissions().mode() & 0o7777)
        .unwrap_or(0o600);

    let file_name = target_path
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".to_string());
    let parent_dir = target_path.parent().unwrap_or_else(|| Path::new("."));

    let nanos = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_nanos(),
        Err(_) => 0,
    };
    let temp_path = parent_dir.join(format!(
        ".{}.tmp.{}_{}",
        file_name,
        std::process::id(),
        nanos
    ));

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)
        .map_err(|e| format!("Failed to open temp file {}: {}", temp_path.display(), e))?;

    file.write_all(content.as_bytes())
        .map_err(|e| format!("Failed to write to temp file: {}", e))?;
    file.sync_all()
        .map_err(|e| format!("Failed to sync temp file: {}", e))?;
    drop(file);

    #[cfg(unix)]
    fs::set_permissions(&temp_path, fs::Permissions::from_mode(mode))
        .map_err(|e| format!("Failed to set permissions on temp file: {}", e))?;

    fs::rename(&temp_path, target_path).map_err(|e| {
        let _ = fs::remove_file(&temp_path);
        format!(
            "Failed to rename temp file {} -> {}: {}",
            temp_path.display(),
            target_path.display(),
            e
        )
    })?;

    Ok(())
}

fn check_cfg_satisfied(map: &serde_json::Map<String, Value>, workdirs: &[&Path]) -> bool {
    if map.get("hasCompletedOnboarding") != Some(&Value::Bool(true)) {
        return false;
    }
    if !map.contains_key("theme") {
        return false;
    }
    if !map.contains_key("lastOnboardingVersion") {
        return false;
    }
    if map.get("bypassPermissionsModeAccepted") != Some(&Value::Bool(true)) {
        return false;
    }
    if let Some(Value::Object(ref projects)) = map.get("projects") {
        for dir in workdirs {
            let dir_key = dir.to_string_lossy().to_string();
            if let Some(Value::Object(ref proj_entry)) = projects.get(&dir_key) {
                if proj_entry.get("hasTrustDialogAccepted") != Some(&Value::Bool(true)) {
                    return false;
                }
            } else {
                return false;
            }
        }
    } else if !workdirs.is_empty() {
        return false;
    }
    true
}

fn update_cfg_map(map: &mut serde_json::Map<String, Value>, workdirs: &[&Path]) {
    if map.get("hasCompletedOnboarding") != Some(&Value::Bool(true)) {
        map.insert("hasCompletedOnboarding".to_string(), json!(true));
    }
    if !map.contains_key("theme") {
        map.insert("theme".to_string(), json!("dark"));
    }
    if !map.contains_key("lastOnboardingVersion") {
        map.insert("lastOnboardingVersion".to_string(), json!("2.1.278"));
    }
    if map.get("bypassPermissionsModeAccepted") != Some(&Value::Bool(true)) {
        map.insert("bypassPermissionsModeAccepted".to_string(), json!(true));
    }
    if !map.contains_key("projects") || !map["projects"].is_object() {
        map.insert("projects".to_string(), json!({}));
    }
    if let Some(Value::Object(ref mut projects)) = map.get_mut("projects") {
        for dir in workdirs {
            let dir_key = dir.to_string_lossy().to_string();
            if !projects.contains_key(&dir_key) || !projects[&dir_key].is_object() {
                projects.insert(dir_key.clone(), json!({}));
            }
            if let Some(Value::Object(ref mut proj_entry)) = projects.get_mut(&dir_key) {
                if proj_entry.get("hasTrustDialogAccepted") != Some(&Value::Bool(true)) {
                    proj_entry.insert("hasTrustDialogAccepted".to_string(), json!(true));
                }
            }
        }
    }
}

fn check_settings_satisfied(map: &serde_json::Map<String, Value>) -> bool {
    if map.get("skipDangerousModePermissionPrompt") != Some(&Value::Bool(true)) {
        return false;
    }
    if let Some(Value::Object(ref perms)) = map.get("permissions") {
        if perms.get("defaultMode") != Some(&Value::String("bypassPermissions".to_string())) {
            return false;
        }
    } else {
        return false;
    }
    true
}

fn update_settings_map(map: &mut serde_json::Map<String, Value>) {
    if map.get("skipDangerousModePermissionPrompt") != Some(&Value::Bool(true)) {
        map.insert("skipDangerousModePermissionPrompt".to_string(), json!(true));
    }
    if !map.contains_key("permissions") || !map["permissions"].is_object() {
        map.insert("permissions".to_string(), json!({}));
    }
    if let Some(Value::Object(ref mut perm_entry)) = map.get_mut("permissions") {
        if perm_entry.get("defaultMode") != Some(&Value::String("bypassPermissions".to_string())) {
            perm_entry.insert("defaultMode".to_string(), json!("bypassPermissions"));
        }
    }
}

/// Ensures ~/.claude.json and ~/.claude/settings.json exist with necessary onboarding,
/// theme, workspace trust, and bypass permissions so Claude runs headlessly without prompts.
///
/// Semantically idempotent:
/// Reads the files; if every required key/value is already present:
///   - hasCompletedOnboarding=true
///   - theme (present with any value)
///   - bypassPermissionsModeAccepted=true
///   - projects[<each workdir>].hasTrustDialogAccepted=true
///   - settings: skipDangerousModePermissionPrompt=true
///   - settings: permissions.defaultMode="bypassPermissions"
///
/// it does NOT write at all (preserving file mtime and any concurrent edits from running Claude).
///
/// Otherwise, inserts only missing or different keys into the parsed document, preserving existing
/// key order (via serde_json preserve_order) and permissions (copying existing mode; new files 0600).
/// Does not overwrite lastOnboardingVersion or theme if already present with any value.
pub fn ensure_claude_config(
    cfg_path: &Path,
    settings_path: &Path,
    workdirs: &[&Path],
) -> Result<(), String> {
    let _lock = CONFIG_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

    // 1. Update ~/.claude.json
    let cfg_satisfied = if cfg_path.is_file() {
        if let Ok(content) = fs::read_to_string(cfg_path) {
            if let Ok(Value::Object(ref map)) = serde_json::from_str(&content) {
                check_cfg_satisfied(map, workdirs)
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    if !cfg_satisfied {
        let mut map: serde_json::Map<String, Value> = if cfg_path.is_file() {
            fs::read_to_string(cfg_path)
                .ok()
                .and_then(|content| serde_json::from_str(&content).ok())
                .and_then(|val| match val {
                    Value::Object(m) => Some(m),
                    _ => None,
                })
                .unwrap_or_default()
        } else {
            serde_json::Map::new()
        };

        update_cfg_map(&mut map, workdirs);
        let content = serde_json::to_string_pretty(&Value::Object(map))
            .map_err(|e| format!("Failed to serialize claude config: {}", e))?;
        write_atomic_preserving_mode(cfg_path, &content)?;
    }

    // 2. Update ~/.claude/settings.json
    let settings_satisfied = if settings_path.is_file() {
        if let Ok(content) = fs::read_to_string(settings_path) {
            if let Ok(Value::Object(ref map)) = serde_json::from_str(&content) {
                check_settings_satisfied(map)
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    if !settings_satisfied {
        let mut map: serde_json::Map<String, Value> = if settings_path.is_file() {
            fs::read_to_string(settings_path)
                .ok()
                .and_then(|content| serde_json::from_str(&content).ok())
                .and_then(|val| match val {
                    Value::Object(m) => Some(m),
                    _ => None,
                })
                .unwrap_or_default()
        } else {
            serde_json::Map::new()
        };

        update_settings_map(&mut map);
        let content = serde_json::to_string_pretty(&Value::Object(map))
            .map_err(|e| format!("Failed to serialize settings: {}", e))?;
        write_atomic_preserving_mode(settings_path, &content)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_ensure_claude_config_creates_fresh_files() {
        let temp_dir = env::temp_dir().join(format!("test_claude_cfg_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let cfg_path = temp_dir.join(".claude.json");
        let settings_path = temp_dir.join(".claude").join("settings.json");
        let workdirs = vec![Path::new("/home/coder"), Path::new("/workspace/test")];

        ensure_claude_config(&cfg_path, &settings_path, &workdirs).expect("Must succeed");

        assert!(cfg_path.exists());
        assert!(settings_path.exists());

        // Validate cfg content
        let cfg_str = fs::read_to_string(&cfg_path).unwrap();
        let cfg_json: Value = serde_json::from_str(&cfg_str).unwrap();
        assert_eq!(cfg_json["hasCompletedOnboarding"], true);
        assert_eq!(cfg_json["theme"], "dark");
        assert_eq!(cfg_json["lastOnboardingVersion"], "2.1.278");
        assert_eq!(cfg_json["bypassPermissionsModeAccepted"], true);
        assert_eq!(
            cfg_json["projects"]["/home/coder"]["hasTrustDialogAccepted"],
            true
        );
        assert_eq!(
            cfg_json["projects"]["/workspace/test"]["hasTrustDialogAccepted"],
            true
        );

        // Validate settings content
        let settings_str = fs::read_to_string(&settings_path).unwrap();
        let settings_json: Value = serde_json::from_str(&settings_str).unwrap();
        assert_eq!(settings_json["skipDangerousModePermissionPrompt"], true);
        assert_eq!(
            settings_json["permissions"]["defaultMode"],
            "bypassPermissions"
        );

        #[cfg(unix)]
        {
            let cfg_mode = fs::metadata(&cfg_path).unwrap().permissions().mode() & 0o777;
            let settings_mode = fs::metadata(&settings_path).unwrap().permissions().mode() & 0o777;
            assert_eq!(cfg_mode, 0o600, "fresh file must have 0600 mode");
            assert_eq!(settings_mode, 0o600, "fresh file must have 0600 mode");
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_ensure_claude_config_handles_corrupt_files() {
        let temp_dir =
            env::temp_dir().join(format!("test_claude_cfg_corrupt_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(temp_dir.join(".claude")).unwrap();

        let cfg_path = temp_dir.join(".claude.json");
        let settings_path = temp_dir.join(".claude").join("settings.json");

        // Write corrupt garbage into files
        fs::write(&cfg_path, "{ broken json not valid").unwrap();
        fs::write(&settings_path, "[ 1, 2, 3 not object").unwrap();

        let workdirs = vec![Path::new("/workspace")];
        ensure_claude_config(&cfg_path, &settings_path, &workdirs)
            .expect("Must rewrite corrupt files");

        let cfg_str = fs::read_to_string(&cfg_path).unwrap();
        let cfg_json: Value = serde_json::from_str(&cfg_str).expect("Rewritten must be valid JSON");
        assert_eq!(cfg_json["hasCompletedOnboarding"], true);
        assert_eq!(
            cfg_json["projects"]["/workspace"]["hasTrustDialogAccepted"],
            true
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_ensure_claude_config_concurrent_threads() {
        let temp_dir =
            env::temp_dir().join(format!("test_claude_cfg_threads_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let cfg_path = Arc::new(temp_dir.join(".claude.json"));
        let settings_path = Arc::new(temp_dir.join(".claude").join("settings.json"));

        let num_threads = 12;
        let mut handles = Vec::new();

        for i in 0..num_threads {
            let cp = Arc::clone(&cfg_path);
            let sp = Arc::clone(&settings_path);
            handles.push(thread::spawn(move || {
                let dir_str = format!("/workspace/project-{}", i);
                let workdirs = vec![Path::new(&dir_str)];
                ensure_claude_config(&cp, &sp, &workdirs).expect("Concurrent call must succeed");
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify that all keys are present and files are valid JSON
        let cfg_str = fs::read_to_string(&*cfg_path).unwrap();
        let cfg_json: Value = serde_json::from_str(&cfg_str).unwrap();
        assert_eq!(cfg_json["hasCompletedOnboarding"], true);

        for i in 0..num_threads {
            let key = format!("/workspace/project-{}", i);
            assert_eq!(
                cfg_json["projects"][&key]["hasTrustDialogAccepted"], true,
                "Missing key: {}",
                key
            );
        }

        let settings_str = fs::read_to_string(&*settings_path).unwrap();
        let settings_json: Value = serde_json::from_str(&settings_str).unwrap();
        assert_eq!(
            settings_json["permissions"]["defaultMode"],
            "bypassPermissions"
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_ensure_claude_config_preserves_mtime_when_identical() {
        let temp_dir =
            env::temp_dir().join(format!("test_claude_cfg_mtime_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let cfg_path = temp_dir.join(".claude.json");
        let settings_path = temp_dir.join(".claude").join("settings.json");
        let workdirs = vec![Path::new("/workspace/unchanged")];

        ensure_claude_config(&cfg_path, &settings_path, &workdirs).unwrap();

        let mtime_cfg_1 = fs::metadata(&cfg_path).unwrap().modified().unwrap();
        let mtime_settings_1 = fs::metadata(&settings_path).unwrap().modified().unwrap();

        // Sleep to ensure time difference if rewritten
        thread::sleep(std::time::Duration::from_millis(50));

        // Call again with identical data
        ensure_claude_config(&cfg_path, &settings_path, &workdirs).unwrap();

        let mtime_cfg_2 = fs::metadata(&cfg_path).unwrap().modified().unwrap();
        let mtime_settings_2 = fs::metadata(&settings_path).unwrap().modified().unwrap();

        assert_eq!(
            mtime_cfg_1, mtime_cfg_2,
            "mtime of .claude.json should not change on identical rewrite"
        );
        assert_eq!(
            mtime_settings_1, mtime_settings_2,
            "mtime of settings.json should not change on identical rewrite"
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_semantic_idempotence_preserves_mtime_even_if_formatting_differs() {
        let temp_dir =
            env::temp_dir().join(format!("test_claude_cfg_sem_idem_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(temp_dir.join(".claude")).unwrap();

        let cfg_path = temp_dir.join(".claude.json");
        let settings_path = temp_dir.join(".claude").join("settings.json");
        let workdirs = vec![Path::new("/workspace/custom")];

        // Custom formatting: minified single-line JSON, custom theme, custom onboarding version, extra keys
        let custom_cfg = r#"{"extraKey":"val","theme":"my-solarized","hasCompletedOnboarding":true,"bypassPermissionsModeAccepted":true,"lastOnboardingVersion":"0.9.0","projects":{"/workspace/custom":{"hasTrustDialogAccepted":true,"customProjectMeta":123}}}"#;
        let custom_settings = r#"{"unrelated":true,"skipDangerousModePermissionPrompt":true,"permissions":{"defaultMode":"bypassPermissions","extra":1}}"#;

        fs::write(&cfg_path, custom_cfg).unwrap();
        fs::write(&settings_path, custom_settings).unwrap();

        let mtime_cfg_1 = fs::metadata(&cfg_path).unwrap().modified().unwrap();
        let mtime_settings_1 = fs::metadata(&settings_path).unwrap().modified().unwrap();

        thread::sleep(std::time::Duration::from_millis(50));

        ensure_claude_config(&cfg_path, &settings_path, &workdirs).unwrap();

        let mtime_cfg_2 = fs::metadata(&cfg_path).unwrap().modified().unwrap();
        let mtime_settings_2 = fs::metadata(&settings_path).unwrap().modified().unwrap();

        assert_eq!(
            mtime_cfg_1, mtime_cfg_2,
            "mtime of .claude.json must not change when semantically satisfied"
        );
        assert_eq!(
            mtime_settings_1, mtime_settings_2,
            "mtime of settings.json must not change when semantically satisfied"
        );

        // Verify content was not rewritten with standard formatter
        let content_after = fs::read_to_string(&cfg_path).unwrap();
        assert_eq!(
            content_after, custom_cfg,
            "content must remain byte-identical"
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_missing_keys_inserted_order_preserved_and_no_overwrite() {
        let temp_dir =
            env::temp_dir().join(format!("test_claude_cfg_order_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(temp_dir.join(".claude")).unwrap();

        let cfg_path = temp_dir.join(".claude.json");
        let settings_path = temp_dir.join(".claude").join("settings.json");
        let workdirs = vec![Path::new("/workspace/project")];

        // An existing document with custom keys and specific order.
        // theme is present ("gruvbox"), lastOnboardingVersion is present ("1.2.3").
        // Missing: bypassPermissionsModeAccepted, hasCompletedOnboarding, projects.
        let initial_cfg = r#"{
  "zebra_unrelated": 1,
  "theme": "gruvbox",
  "apple_unrelated": 2,
  "lastOnboardingVersion": "1.2.3",
  "banana_unrelated": 3
}"#;
        fs::write(&cfg_path, initial_cfg).unwrap();

        ensure_claude_config(&cfg_path, &settings_path, &workdirs).unwrap();

        let content_after = fs::read_to_string(&cfg_path).unwrap();
        let val_after: Value = serde_json::from_str(&content_after).unwrap();

        // 1. theme and lastOnboardingVersion must NOT have been overwritten
        assert_eq!(val_after["theme"], "gruvbox");
        assert_eq!(val_after["lastOnboardingVersion"], "1.2.3");

        // 2. Missing keys must be inserted
        assert_eq!(val_after["hasCompletedOnboarding"], true);
        assert_eq!(val_after["bypassPermissionsModeAccepted"], true);
        assert_eq!(
            val_after["projects"]["/workspace/project"]["hasTrustDialogAccepted"],
            true
        );

        // 3. Existing key order must be preserved: zebra_unrelated before theme before apple_unrelated before lastOnboardingVersion before banana_unrelated
        if let Value::Object(map) = val_after {
            let keys: Vec<&String> = map.keys().collect();
            assert_eq!(keys[0], "zebra_unrelated");
            assert_eq!(keys[1], "theme");
            assert_eq!(keys[2], "apple_unrelated");
            assert_eq!(keys[3], "lastOnboardingVersion");
            assert_eq!(keys[4], "banana_unrelated");
        } else {
            panic!("Expected Value::Object");
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_mode_preserved_on_update() {
        let temp_dir = env::temp_dir().join(format!("test_claude_cfg_mode_{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(temp_dir.join(".claude")).unwrap();

        let cfg_path = temp_dir.join(".claude.json");
        let settings_path = temp_dir.join(".claude").join("settings.json");
        let workdirs = vec![Path::new("/workspace")];

        // Create cfg_path with incomplete config and specific permissions mode: 0640
        fs::write(&cfg_path, r#"{"unrelated": 1}"#).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o640)).unwrap();

        ensure_claude_config(&cfg_path, &settings_path, &workdirs).unwrap();

        #[cfg(unix)]
        {
            let cfg_mode = fs::metadata(&cfg_path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                cfg_mode, 0o640,
                "original file permissions must be preserved"
            );

            // settings_path was newly created, so it must have mode 0600
            let settings_mode = fs::metadata(&settings_path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                settings_mode, 0o600,
                "newly created file must have 0600 mode"
            );
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }
}

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use crate::core::args::InitArgs;
use crate::core::config::{atomic_write, Config, ConfigFile};
use crate::core::output::CommandOutput;

/// Reads existing config file or starts from empty `ConfigFile`, updates hindsight fields
/// provided in `args`, writes the file back out, and prints a summary.
pub fn persist_hindsight_config(config_path: &Path, args: &InitArgs) -> Result<Vec<String>> {
    let mut emit = |line: String| println!("{line}");
    persist_hindsight_config_to(config_path, args, &mut emit)
}

/// Collected variant of [`persist_hindsight_config`]: summary lines are routed
/// through `emit(line)` instead of being printed directly, so the result can
/// be recorded in a [`CommandOutput`].
pub fn persist_hindsight_config_to(
    config_path: &Path,
    args: &InitArgs,
    emit: &mut dyn FnMut(String),
) -> Result<Vec<String>> {
    let mut file_cfg = if config_path.exists() {
        let content = fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file at {}", config_path.display()))?;
        toml::from_str::<ConfigFile>(&content).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse TOML config file at {}: {}",
                config_path.display(),
                e
            )
        })?
    } else {
        ConfigFile::default()
    };

    let mut updated_fields = Vec::new();
    if let Some(url) = &args.hindsight_url {
        file_cfg.hindsight_url = Some(url.clone());
        updated_fields.push(format!("hindsight_url = {:?}", url));
    }
    if let Some(bank) = &args.hindsight_bank {
        file_cfg.hindsight_bank = Some(bank.clone());
        updated_fields.push(format!("hindsight_bank = {:?}", bank));
    }
    if let Some(key) = &args.hindsight_api_key {
        file_cfg.hindsight_api_key = Some(key.clone());
        updated_fields.push(format!(
            "hindsight_api_key = <redacted, {} chars>",
            key.chars().count()
        ));
    }
    if let Some(profile) = &args.hindsight_profile {
        file_cfg.hindsight_profile = Some(profile.clone());
        updated_fields.push(format!("hindsight_profile = {:?}", profile));
    }

    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "Failed to create parent directory for {}",
                config_path.display()
            )
        })?;
    }

    let toml_str =
        toml::to_string_pretty(&file_cfg).context("Failed to serialize updated config to TOML")?;
    atomic_write(config_path, toml_str)
        .with_context(|| format!("Failed to write config file at {}", config_path.display()))?;

    emit(format!(
        "Updated Hindsight configuration in {}:",
        config_path.display()
    ));
    for field in &updated_fields {
        emit(format!("  {}", field));
    }

    Ok(updated_fields)
}

/// Server-side runtime setup: persist hindsight configuration (when the flags
/// are set), materialize a secret env var to a 0600 file (when `--from-env`/
/// `--to` are set), and ensure the state store layout exists.
///
/// This runs on the agent-run-api server: `--from-env`/`--to` read the
/// server's own environment, and the config file it writes is the server's.
pub fn run(config: &Config, args: &InitArgs) -> Result<CommandOutput> {
    let mut output = CommandOutput::new(serde_json::json!({}));

    // 1. Handle hindsight configuration flags if any were provided
    let has_hindsight_flags = args.hindsight_url.is_some()
        || args.hindsight_bank.is_some()
        || args.hindsight_api_key.is_some()
        || args.hindsight_profile.is_some();

    if has_hindsight_flags {
        let config_path = config
            .config_path
            .clone()
            .or_else(|| Config::resolve_config_path(&HashMap::new()))
            .context("Failed to determine config file path (could not resolve config directory)")?;

        let mut emit = |line: String| output.push_out(line);
        persist_hindsight_config_to(&config_path, args, &mut emit)?;
    }

    // 2. Handle --from-env and --to if provided (server-side env extraction)
    if let (Some(var_name), Some(to_path)) = (&args.from_env, &args.to) {
        let val = std::env::var(var_name).with_context(|| {
            format!(
                "Environment variable '{}' not found in current environment",
                var_name
            )
        })?;

        if let Some(parent) = to_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create parent directory for {}",
                    to_path.display()
                )
            })?;
        }

        // Open with mode 0600 so permissions are set atomically at creation
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(to_path)
            .with_context(|| {
                format!("Failed to create destination file at {}", to_path.display())
            })?;

        file.write_all(val.as_bytes())
            .with_context(|| format!("Failed to write content to {}", to_path.display()))?;

        output.push_out(format!(
            "Wrote environment variable '{}' to {} (mode 0600)",
            var_name,
            to_path.display()
        ));
    } else if args.from_env.is_some() || args.to.is_some() {
        return Err(crate::core::error::AgentRunError::invalid(
            "must_be_provided_together",
            "Both --from-env and --to must be provided together",
        )
        .into());
    }

    // 3. Ensure the state directory layout exists (root, logs/, state.sqlite with jobs table)
    crate::core::job::ensure_state_store(&config.state_root).with_context(|| {
        format!(
            "Failed to initialize state store at {}",
            config.state_root.display()
        )
    })?;
    output.push_out(format!(
        "Ensured state directory exists: {} (state database: {})",
        config.state_root.display(),
        crate::core::job::state_db_path(&config.state_root).display()
    ));

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn test_config() -> Config {
        let tmp = tempdir().unwrap();
        // Keep the tempdir alive for the test via the state_root path string.
        Config {
            config_path: Some(tmp.path().join("config.toml")),
            state_root: tmp.path().join("state"),
            ..Default::default()
        }
    }

    #[test]
    fn test_init_from_env_writes_mode_0600() {
        let _env_lock = crate::core::lock_test_env();
        let tmp = tempdir().unwrap();
        let to_path = tmp.path().join("secret.txt");
        std::env::set_var("AGENT_RUN_INIT_TEST_SECRET", "s3cret-value");
        let config = test_config();
        let args = InitArgs {
            from_env: Some("AGENT_RUN_INIT_TEST_SECRET".to_string()),
            to: Some(to_path.clone()),
            ..Default::default()
        };
        let out = run(&config, &args).unwrap();
        assert!(out
            .stdout_lines
            .iter()
            .any(|l| l.contains("Wrote environment variable")));
        let content = fs::read_to_string(&to_path).unwrap();
        assert_eq!(content, "s3cret-value");
        let metadata = fs::metadata(&to_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o600);
        std::env::remove_var("AGENT_RUN_INIT_TEST_SECRET");
    }

    #[test]
    fn test_init_from_env_and_to_must_be_paired() {
        let config = test_config();
        let args = InitArgs {
            from_env: Some("SOME_VAR".to_string()),
            to: None,
            ..Default::default()
        };
        let err = run(&config, &args).unwrap_err();
        assert!(err.to_string().contains("Both --from-env and --to"));

        let args = InitArgs {
            from_env: None,
            to: Some(PathBuf::from("/tmp/x")),
            ..Default::default()
        };
        let err = run(&config, &args).unwrap_err();
        assert!(err.to_string().contains("Both --from-env and --to"));
    }

    #[test]
    fn test_init_hindsight_config_merge_not_overwrite() {
        let tmp = tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let config = Config {
            config_path: Some(config_path.clone()),
            state_root: tmp.path().join("state"),
            ..Default::default()
        };
        fs::write(&config_path, "workspace_root = \"/ws\"\n").unwrap();

        let args = InitArgs {
            hindsight_url: Some("https://api.example".to_string()),
            ..Default::default()
        };
        let out = run(&config, &args).unwrap();
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(
            content.contains(r#"workspace_root = "/ws""#),
            "existing key must survive: {content}"
        );
        assert!(content.contains(r#"hindsight_url = "https://api.example""#));
        assert!(out.stdout_lines.iter().any(|l| l.contains("hindsight_url")));
    }

    #[test]
    fn test_init_without_hindsight_flags_skips_config_file() {
        let tmp = tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let config = Config {
            config_path: Some(config_path.clone()),
            state_root: tmp.path().join("state"),
            ..Default::default()
        };
        let args = InitArgs::default();
        run(&config, &args).unwrap();
        assert!(
            !config_path.exists(),
            "no hindsight flags -> config file untouched"
        );
    }

    #[test]
    fn test_init_hindsight_creates_new_config_file_with_multiple_flags() {
        let tmp = tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let config = Config {
            config_path: Some(config_path.clone()),
            state_root: tmp.path().join("state"),
            ..Default::default()
        };
        let args = InitArgs {
            hindsight_url: Some("https://api.example".to_string()),
            hindsight_bank: Some("my-bank".to_string()),
            hindsight_profile: Some("my-profile".to_string()),
            ..Default::default()
        };
        run(&config, &args).unwrap();
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains(r#"hindsight_url = "https://api.example""#));
        assert!(content.contains(r#"hindsight_bank = "my-bank""#));
        assert!(content.contains(r#"hindsight_profile = "my-profile""#));
    }

    #[test]
    fn test_init_hindsight_api_key_redacted_in_summary() {
        let tmp = tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let config = Config {
            config_path: Some(config_path.clone()),
            state_root: tmp.path().join("state"),
            ..Default::default()
        };
        let args = InitArgs {
            hindsight_api_key: Some("sk-supersecretkey".to_string()),
            ..Default::default()
        };
        let out = run(&config, &args).unwrap();
        assert!(
            out.stdout_lines
                .iter()
                .any(|l| l.contains("<redacted, 17 chars>")),
            "summary must redact the key: {:?}",
            out.stdout_lines
        );
        assert!(!out
            .stdout_lines
            .iter()
            .any(|l| l.contains("sk-supersecretkey")));
        // but the file itself must carry the real key
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains(r#"hindsight_api_key = "sk-supersecretkey""#));
    }

    #[test]
    fn test_init_ensures_state_store() {
        let tmp = tempdir().unwrap();
        let config = Config {
            config_path: Some(tmp.path().join("config.toml")),
            state_root: tmp.path().join("state"),
            ..Default::default()
        };
        let out = run(&config, &InitArgs::default()).unwrap();
        assert!(config.state_root.is_dir());
        assert!(crate::core::job::state_db_path(&config.state_root).exists());
        assert!(out
            .stdout_lines
            .iter()
            .any(|l| l.contains("Ensured state directory exists")));
    }
}

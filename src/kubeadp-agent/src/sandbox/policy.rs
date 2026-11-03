//! Policy definitions and parsing for the sandbox launcher.

use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Secret injection specification: inherited file descriptor written to `<tmp>/dest` with mode 0400.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretConfig {
    pub fd: i32,
    pub dest: String,
}

/// Sandbox policy matching CONTRACT v1:
/// Unknown policy keys are an error (`deny_unknown_fields`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPolicy {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub jobset_id: Option<String>,
    #[serde(default)]
    pub rw: Vec<PathBuf>,
    #[serde(default)]
    pub ro: Vec<PathBuf>,
    #[serde(default)]
    pub shared_rw: Vec<PathBuf>,
    #[serde(default)]
    pub hide: Vec<PathBuf>,
    #[serde(default = "default_tmp")]
    pub tmp: String,
    #[serde(default)]
    pub secret: Option<SecretConfig>,
    #[serde(default = "default_env_allow")]
    pub env_allow: Vec<String>,
    #[serde(default = "default_net")]
    pub net: String,
}

fn default_tmp() -> String {
    "auto".to_string()
}

fn default_env_allow() -> Vec<String> {
    vec![
        "PATH".to_string(),
        "HOME".to_string(),
        "LANG".to_string(),
        "TERM".to_string(),
        "SSL_CERT_FILE".to_string(),
        "REQUESTS_CA_BUNDLE".to_string(),
        "NODE_USE_SYSTEM_CA".to_string(),
        "A2A_*".to_string(),
        "KUBEADP_*".to_string(),
    ]
}

fn default_net() -> String {
    "shared".to_string()
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            agent_id: None,
            jobset_id: None,
            rw: Vec::new(),
            ro: vec![
                PathBuf::from("/usr"),
                PathBuf::from("/lib"),
                PathBuf::from("/lib64"),
                PathBuf::from("/bin"),
                PathBuf::from("/sbin"),
                PathBuf::from("/etc"),
                PathBuf::from("/opt/tools"),
                PathBuf::from("/workspace/homelab"),
            ],
            shared_rw: vec![PathBuf::from("/home/agent")],
            hide: vec![PathBuf::from("/workspace/.agent-jobs")],
            tmp: default_tmp(),
            secret: None,
            env_allow: default_env_allow(),
            net: default_net(),
        }
    }
}

impl SandboxPolicy {
    /// Builds a default policy for a worker job.
    /// `shared_rw` overrides the production default writable shared path
    /// (`/home/agent`); `None` keeps that default.
    pub fn for_job(
        job_id: &str,
        worktree_path: &Path,
        project_root: Option<&Path>,
        agy_path: Option<&Path>,
        shared_rw: Option<Vec<PathBuf>>,
    ) -> Self {
        let mut policy = Self {
            agent_id: Some(job_id.to_string()),
            rw: vec![worktree_path.to_path_buf()],
            ..Default::default()
        };
        if let Some(srw) = shared_rw {
            policy.shared_rw = srw;
        }
        if let Some(proj) = project_root {
            if !policy.ro.contains(&proj.to_path_buf()) {
                policy.ro.push(proj.to_path_buf());
            }
        }
        if let Some(agy) = agy_path {
            let agy_buf = agy.to_path_buf();
            if !policy.ro.contains(&agy_buf) {
                policy.ro.push(agy_buf);
            }
            if let Some(parent) = agy.parent() {
                let parent_buf = parent.to_path_buf();
                if !policy.ro.contains(&parent_buf) {
                    policy.ro.push(parent_buf);
                }
            }
        }
        policy
    }

    /// Builds a default policy for an orchestrator session.
    pub fn for_session(session_id: &str, workdir: &Path) -> Self {
        Self {
            agent_id: Some(session_id.to_string()),
            rw: vec![workdir.to_path_buf()],
            ..Default::default()
        }
    }

    /// Loads a policy from a file path or stdin if path is "-".
    pub fn load_from_file_or_stdin(path: &str) -> Result<Self, String> {
        let content = if path == "-" {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| format!("failed to read policy from stdin: {e}"))?;
            buf
        } else {
            std::fs::read_to_string(path)
                .map_err(|e| format!("failed to read policy file '{path}': {e}"))?
        };
        Self::parse_json(&content)
    }

    /// Parses policy from JSON string. Unknown keys produce an error.
    pub fn parse_json(json_str: &str) -> Result<Self, String> {
        serde_json::from_str(json_str).map_err(|e| format!("invalid policy JSON: {e}"))
    }

    /// Materializes a policy document on disk at `path` as pretty JSON.
    ///
    /// Isolation is always applied (Contract v1.3), so a policy that cannot
    /// be written must abort the launch: callers must NOT ignore the returned
    /// error (there is no fallback that would run the command without the
    /// policy file). Returns a descriptive message on failure.
    pub fn write_policy_file(policy: &SandboxPolicy, path: &Path) -> Result<(), String> {
        let json = serde_json::to_string_pretty(policy)
            .map_err(|e| format!("failed to serialize sandbox policy: {e}"))?;
        std::fs::write(path, json)
            .map_err(|e| format!("failed to write sandbox policy '{}': {e}", path.display()))?;
        Ok(())
    }

    /// Checks whether an environment variable name matches any pattern in `env_allow`.
    /// Patterns support simple trailing `*` wildcard (e.g. `A2A_*`).
    pub fn is_env_allowed(&self, var_name: &str) -> bool {
        for pattern in &self.env_allow {
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if var_name.starts_with(prefix) {
                    return true;
                }
            } else if pattern == var_name {
                return true;
            }
        }
        false
    }

    /// Filters the given environment variables, retaining only allowed variables.
    pub fn filter_env<'a, I>(&self, env_iter: I) -> Vec<(String, String)>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        env_iter
            .into_iter()
            .filter(|(k, _)| self.is_env_allowed(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Computes the private tmp directory (under `/tmp`) without touching the
    /// filesystem. `tmp == "auto"` names it after the agent; `tmp == ""`
    /// disables the private tmp; otherwise `tmp` is used verbatim.
    pub fn private_tmp_path(&self) -> Option<PathBuf> {
        if self.tmp.is_empty() {
            return None;
        }
        if self.tmp == "auto" {
            let agent = self.agent_id.as_deref().unwrap_or("agent");
            Some(PathBuf::from(format!("/tmp/agent-sandbox-{agent}")))
        } else {
            Some(PathBuf::from(self.tmp.as_str()))
        }
    }

    /// Normalizes a policy path: absolute paths are used as-is, relative paths
    /// are resolved against the private tmp (the agent's working scratch).
    /// Shared paths are expected to be absolute already.
    pub fn normalize_policy_path(&self, path: &Path, private_tmp: Option<&Path>) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else if let Some(tmp) = private_tmp {
            tmp.join(path)
        } else {
            path.to_path_buf()
        }
    }

    /// Ensures every writable policy path exists as a real directory so the
    /// sandbox can actually grant write access to it. Shared between the
    /// backends because the isolation step that needs the directory to exist
    /// differs per backend:
    /// - gVisor: the private-tmp/mount planning needs the normalized writable
    ///   dirs to exist before the hide overmounts are built.
    /// - Kata/runc-Landlock: `landlock_add_rule` silently skips non-existent
    ///   paths, so a writable path that cannot be created would be silently
    ///   dropped from the domain instead of failing the launch. Contract
    ///   v1.3 requires fail-closed: a policy rw/shared_rw path that cannot be
    ///   created is a launch failure (exit 125), not a quietly-weaker domain.
    ///
    /// Relative writable paths are resolved against `private_tmp` (the agent's
    /// scratch), matching `plan_mounts`; if no private tmp is configured they
    /// are used as-is. Absolute paths are used verbatim. Returns an error
    /// naming the first path that could not be created or is not a directory.
    pub fn ensure_writable_dirs(&self, private_tmp: Option<&Path>) -> Result<(), String> {
        let mut paths: Vec<PathBuf> = Vec::new();
        paths.extend(self.rw.iter().cloned());
        paths.extend(self.shared_rw.iter().cloned());
        if let Some(pt) = private_tmp {
            paths.push(pt.to_path_buf());
        }
        for p in paths {
            let norm = self.normalize_policy_path(&p, private_tmp);
            std::fs::create_dir_all(&norm).map_err(|e| {
                format!(
                    "sandbox policy writable path {} cannot be created: {e}",
                    norm.display()
                )
            })?;
            if let Ok(m) = std::fs::metadata(&norm) {
                if !m.is_dir() {
                    return Err(format!(
                        "sandbox policy writable path {} is not a directory",
                        norm.display()
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_policy() {
        let json = r#"{
            "agent_id": "job-7",
            "jobset_id": "js-1",
            "rw": ["/workspace/.agent-jobs/homelab/ab12"],
            "ro": ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc", "/opt/tools", "/workspace/homelab"],
            "shared_rw": ["/home/agent"],
            "hide": ["/workspace/.agent-jobs"],
            "tmp": "auto",
            "secret": {"fd": 3, "dest": "secret"},
            "env_allow": ["PATH", "HOME", "LANG", "TERM", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "NODE_USE_SYSTEM_CA", "A2A_*", "KUBEADP_*"],
            "net": "shared"
        }"#;

        let policy = SandboxPolicy::parse_json(json).expect("valid policy should parse");
        assert_eq!(policy.agent_id.as_deref(), Some("job-7"));
        assert_eq!(policy.jobset_id.as_deref(), Some("js-1"));
        assert_eq!(
            policy.rw,
            vec![PathBuf::from("/workspace/.agent-jobs/homelab/ab12")]
        );
        assert_eq!(policy.shared_rw, vec![PathBuf::from("/home/agent")]);
        assert_eq!(
            policy.secret,
            Some(SecretConfig {
                fd: 3,
                dest: "secret".to_string()
            })
        );
        assert_eq!(policy.net, "shared");
        assert!(policy.is_env_allowed("PATH"));
        assert!(policy.is_env_allowed("A2A_SOMETHING"));
        assert!(policy.is_env_allowed("KUBEADP_ROLE"));
        assert!(!policy.is_env_allowed("VIKUNJA_API_TOKEN"));
    }

    #[test]
    fn test_unknown_key_rejected() {
        let json = r#"{
            "rw": ["/workspace"],
            "unknown_field": 123
        }"#;
        let err = SandboxPolicy::parse_json(json).unwrap_err();
        assert!(err.contains("unknown field"), "error was: {err}");
    }

    #[test]
    fn test_write_policy_file_roundtrip_and_failure() {
        let dir = tempfile::tempdir().unwrap();
        let policy = SandboxPolicy::for_job(
            "job-write1",
            std::path::Path::new("/workspace/.agent-jobs/homelab/job-write1"),
            None,
            None,
            None,
        );

        let path = dir.path().join("policy.json");
        SandboxPolicy::write_policy_file(&policy, &path).expect("policy write must succeed");
        let on_disk = std::fs::read_to_string(&path).unwrap();
        let reparsed = SandboxPolicy::parse_json(&on_disk).expect("written policy must reparse");
        assert_eq!(reparsed, policy);

        // A path under a missing directory must fail with a descriptive error.
        let bad_path = dir.path().join("no-such-dir").join("policy.json");
        let err = SandboxPolicy::write_policy_file(&policy, &bad_path).unwrap_err();
        assert!(
            err.contains("failed to write sandbox policy"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_hide_calculation_and_preservation() {
        let policy = SandboxPolicy {
            rw: vec![
                PathBuf::from("/workspace/.agent-jobs/homelab/job1"),
                PathBuf::from("/tmp/other"),
            ],
            hide: vec![PathBuf::from("/workspace/.agent-jobs")],
            ..Default::default()
        };

        // Check which rw entries are under the hide path
        let hide_dir = &policy.hide[0];
        let sub_rws: Vec<_> = policy
            .rw
            .iter()
            .filter(|rw| rw.starts_with(hide_dir) && *rw != hide_dir)
            .collect();
        assert_eq!(sub_rws.len(), 1);
        assert_eq!(
            sub_rws[0].strip_prefix(hide_dir).unwrap(),
            Path::new("homelab/job1")
        );
    }

    // F2 (R09): every writable policy path (rw, shared_rw and the private tmp)
    // must exist as a real directory, or the launch fails closed (exit 125).
    #[test]
    fn test_ensure_writable_dirs_creates_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let policy = SandboxPolicy {
            rw: vec![dir.path().join("rw/deep/nested")],
            shared_rw: vec![dir.path().join("shared")],
            tmp: dir.path().join("tmp").to_string_lossy().to_string(),
            ..Default::default()
        };
        let private_tmp = policy.private_tmp_path();
        policy
            .ensure_writable_dirs(private_tmp.as_deref())
            .expect("all writable paths must be created");
        for p in [
            dir.path().join("rw/deep/nested"),
            dir.path().join("shared"),
            dir.path().join("tmp"),
        ] {
            assert!(p.is_dir(), "expected dir {p:?} to exist");
        }
    }

    // F2 (R09): an rw path rooted at an uncreatable location (system paths,
    // or a regular file used as a parent) must fail the launch, never silently
    // drop the writable grant.
    #[test]
    fn test_ensure_writable_dirs_fails_closed_on_uncreatable_path() {
        let dir = tempfile::tempdir().unwrap();
        let policy = SandboxPolicy {
            rw: vec![PathBuf::from("/proc/hl86-nonexistent/deeply/nested")],
            ..Default::default()
        };
        let err = policy
            .ensure_writable_dirs(None)
            .expect_err("an uncreatable rw path must fail closed");
        assert!(
            err.contains("cannot be created") || err.contains("not a directory"),
            "error must explain the writable-path problem: {err}"
        );

        // A regular file used as a parent (file-as-directory) also fails.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let policy2 = SandboxPolicy {
            rw: vec![blocker.join("sub")], // under a regular file
            ..Default::default()
        };
        let err2 = policy2
            .ensure_writable_dirs(None)
            .expect_err("a writable path under a file must fail closed");
        assert!(
            err2.contains("cannot be created") || err2.contains("not a directory"),
            "error must explain the writable-path problem: {err2}"
        );
    }

    // F2: a relative writable path is resolved against the private tmp, so it
    // lands inside the agent's own scratch (matching plan_mounts).
    #[test]
    fn test_ensure_writable_dirs_resolves_relative_against_private_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let policy = SandboxPolicy {
            rw: vec![PathBuf::from("work")],
            shared_rw: Vec::new(),
            tmp: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let private_tmp = policy.private_tmp_path();
        policy
            .ensure_writable_dirs(private_tmp.as_deref())
            .expect("relative writable path resolved to private tmp must exist");
        assert!(
            private_tmp.unwrap().join("work").is_dir(),
            "relative rw path must be created under the private tmp"
        );
    }
}

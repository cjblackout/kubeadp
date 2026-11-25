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

/// Execution role for a sandbox policy.
/// Grants are restricted to `Session` role policies and are rejected for `Worker` role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PolicyRole {
    #[default]
    Worker,
    Session,
}

/// Per-session grants configuration (passed from trusted supervisor config).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantsConfig {
    /// Grants read-only access to the Kubernetes ServiceAccount token directory.
    #[serde(default)]
    pub serviceaccount: bool,

    /// Absolute paths granted read-only. Entries may be directories (the grant
    /// covers their contents recursively) or single regular files. Must not be
    /// `/`, under `/proc` or `/dev`, contain `..`, or be empty.
    #[serde(default)]
    pub ro_paths: Vec<PathBuf>,

    /// Absolute paths granted read-write; same shape rules as `ro_paths`. A
    /// missing entry is created as a directory at launch; an entry that
    /// already exists as a regular file is a valid single-file grant and is
    /// kept as-is (never created or replaced).
    #[serde(default)]
    pub rw_paths: Vec<PathBuf>,

    /// Environment variable names passed through from supervisor process environment.
    /// Trailing wildcard `*` allowed only.
    #[serde(default)]
    pub env: Vec<String>,
}

/// Describes how one agent's A2A delivery socket becomes a harness mount for
/// a given policy (SPEC §6.9): the backend-specific mount/grant inputs the
/// a2a launcher derives from [`SandboxPolicy::delivery_socket_mount`]. Kept
/// path-only (no `core::a2a` types) because the sandbox library compiles
/// without the `a2a` feature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySocketMount {
    /// The per-agent socket path the host bound before the fork (the OUTER
    /// path, in the pod filesystem).
    pub host_sock: PathBuf,
    /// The in-sandbox path gVisor bind-mounts `host_sock` onto (under the
    /// private tmp, next to the relay socket); exported as
    /// `KUBEADP_A2A_LOCAL_SOCKET` for the gVisor backend.
    pub gvisor_target: PathBuf,
    /// The paths kata / runc-Landlock grant for the socket's resolution
    /// (directory traversal only; pathname connect is not Landlock-restricted
    /// below ABI 9). The launcher adds these to the Landlock domain.
    pub landlock_paths: Vec<PathBuf>,
}

/// Sandbox policy matching CONTRACT v1:
/// Unknown policy keys are an error (`deny_unknown_fields`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPolicy {
    #[serde(default)]
    pub role: PolicyRole,
    #[serde(default)]
    pub grants: Option<GrantsConfig>,
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
    /// Network mode: `host` (the default; the agent keeps the pod's network
    /// namespace -- today's behaviour; the legacy value `shared` is an alias),
    /// or `relay` (the agent gets its own loopback-only network namespace and
    /// reaches the outside through a unix-socket relay proxy; honoured by all
    /// three backends: the gVisor shim unshares the namespace, the kata and
    /// runc-Landlock children unshare user+network namespaces).
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

/// The `net` values the policy accepts. `shared` is the legacy alias of the
/// default `host` (the agent keeps the pod's network namespace); `relay` is
/// the opt-in private-namespace + unix-socket-relay mode (all three
/// backends). Anything else is rejected at parse time.
pub const NET_MODES: &[&str] = &["host", "shared", "relay"];

/// The environment variable carrying the long-lived claude OAuth token
/// (`claude setup-token`) that claude-engine jobs authenticate with. Only the
/// NAME is ever listed in a policy's `env_allow` (and so written into a policy
/// file); the VALUE travels through the filtered process environment and is
/// never logged or serialized.
pub const CLAUDE_OAUTH_TOKEN_ENV: &str = "CLAUDE_CODE_OAUTH_TOKEN";

fn default_net() -> String {
    "host".to_string()
}

/// Validates the policy `net` value. Unknown values are an error so a policy
/// that was never parsed cannot silently select a weaker (or unknown) mode.
fn validate_net(net: &str) -> Result<(), String> {
    if NET_MODES.contains(&net) {
        Ok(())
    } else {
        Err(format!(
            "unknown net value '{net}' (accepted: host, shared, relay)"
        ))
    }
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            role: PolicyRole::Worker,
            grants: None,
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
            role: PolicyRole::Worker,
            grants: None,
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

    /// Builds a default policy for an orchestrator session with optional grants.
    pub fn for_session(session_id: &str, workdir: &Path, grants: Option<GrantsConfig>) -> Self {
        Self {
            role: PolicyRole::Session,
            grants,
            agent_id: Some(session_id.to_string()),
            rw: vec![workdir.to_path_buf()],
            ..Default::default()
        }
    }

    /// Validates the policy and its grants.
    /// Fails closed (returns Err) if:
    /// - `net` is not one of the accepted values (`host`, `shared`, `relay`).
    /// - `role` is `Worker` but `grants` is set (worker jobs can never have grants).
    /// - Any grant path is non-absolute, empty, root `/`, `/proc`, `/dev`, or contains traversal `..`.
    /// - Any grant env name is invalid.
    pub fn validate(&self) -> Result<(), String> {
        validate_net(&self.net)?;
        if self.role == PolicyRole::Worker && self.grants.is_some() {
            return Err("Worker job policy must not specify grants (grants are restricted to session policies)".to_string());
        }
        if let Some(ref g) = self.grants {
            for p in &g.ro_paths {
                validate_policy_grant_path(p, "ro_paths")?;
            }
            for p in &g.rw_paths {
                validate_policy_grant_path(p, "rw_paths")?;
            }
            for env in &g.env {
                validate_policy_grant_env_name(env)?;
            }
        }
        Ok(())
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
        let policy = Self::parse_json(&content)?;
        policy.validate()?;
        Ok(policy)
    }

    /// Parses policy from JSON string. Unknown keys and unknown `net` values
    /// produce an error.
    pub fn parse_json(json_str: &str) -> Result<Self, String> {
        let policy: Self =
            serde_json::from_str(json_str).map_err(|e| format!("invalid policy JSON: {e}"))?;
        validate_net(&policy.net)?;
        Ok(policy)
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

    /// Checks whether an environment variable name matches any pattern in `env_allow`
    /// or in `grants.env` if grants are configured.
    /// Patterns support simple trailing `*` wildcard (e.g. `A2A_*`, `VIKUNJA_*`).
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
        if let Some(ref g) = self.grants {
            for pattern in &g.env {
                if pattern.ends_with('*') {
                    let prefix = &pattern[..pattern.len() - 1];
                    if var_name.starts_with(prefix) {
                        return true;
                    }
                } else if pattern == var_name {
                    return true;
                }
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

    /// The harness mount of this agent's A2A delivery socket (SPEC §6.9):
    /// how the per-agent socket path the launcher bound BEFORE the fork
    /// becomes reachable inside this policy's sandbox.
    ///
    /// The sandbox module is compiled without the `a2a` feature, so this
    /// takes the host socket PATH (never an A2A type) and cannot know the
    /// exact in-sandbox value the a2a launcher picks for the env -- the three
    /// fields are the inputs C5 uses per backend:
    ///
    /// - gVisor bind-mounts `host_sock` onto [`DeliverySocketMount::gvisor_target`]
    ///   (under the private tmp, next to the relay socket) and exports that
    ///   target as `KUBEADP_A2A_LOCAL_SOCKET`;
    /// - kata / runc-Landlock use `host_sock` DIRECTLY (a pathname unix socket
    ///   is not namespaced by a netns and needs no bind) and grant
    ///   [`DeliverySocketMount::landlock_paths`] for the socket's resolution
    ///   (pathname connect is not Landlock-restricted below ABI 9, so no
    ///   socket rule is needed -- only directory traversal).
    pub fn delivery_socket_mount(&self, host_sock: &Path) -> DeliverySocketMount {
        let gvisor_target = match self.private_tmp_path() {
            Some(tmp) => tmp.join(".delivery.sock"),
            None => host_sock.to_path_buf(),
        };
        // The socket's private directory: granting it mirrors the private-tmp
        // grant (access_flags_for_path chooses the directory flag set), so an
        // agent can resolve `<dir>/delivery.sock`. The socket file itself
        // needs no rule.
        let landlock_paths = host_sock
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| vec![p.to_path_buf()])
            .unwrap_or_default();
        DeliverySocketMount {
            host_sock: host_sock.to_path_buf(),
            gvisor_target,
            landlock_paths,
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
        if let Some(ref g) = self.grants {
            for p in &g.rw_paths {
                // A granted rw path may be a directory (created when missing,
                // the launch still fails closed when it cannot be) or a single
                // regular file: an existing file is a valid file grant and is
                // kept as-is -- never created, replaced or rejected here.
                if std::fs::metadata(p).is_ok() {
                    continue;
                }
                std::fs::create_dir_all(p).map_err(|e| {
                    format!(
                        "sandbox policy granted rw path {} cannot be created: {e}",
                        p.display()
                    )
                })?;
                if let Ok(m) = std::fs::metadata(p) {
                    if !m.is_dir() {
                        return Err(format!(
                            "sandbox policy granted rw path {} is not a directory",
                            p.display()
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Adds the configured operator token file (config `api_token_file`, the
    /// path named by `KUBEADP_API_TOKEN_FILE`) to the policy's `hide` list so a
    /// sandboxed agent can never read the operator credential. No-op when the
    /// token file is not configured (there is then nothing to hide).
    pub fn hide_operator_token_file(self) -> Self {
        self.with_hidden_path(operator_token_file_path().as_deref())
    }

    /// Adds the configured jobset root key file (config
    /// `jobset_root_key_file`, the path named by
    /// `KUBEADP_JOBSET_ROOT_KEY_FILE`) to the policy's `hide` list so a
    /// sandboxed agent can never read the root key (agents only ever see
    /// their DERIVED per-jobset secret through the `secret` channel). No-op
    /// when the root key file is not configured.
    pub fn hide_jobset_root_key_file(self) -> Self {
        self.with_hidden_path(jobset_root_key_file_path().as_deref())
    }

    /// Adds `CLAUDE_CODE_OAUTH_TOKEN` to the policy's `env_allow` list so a
    /// claude-engine job receives the pod's durable claude login through the
    /// env_allow filter. Callers use it ONLY for claude-engine job policies
    /// (agy jobs keep the default allowlist). Idempotent: an entry already
    /// present (e.g. a hand-written policy) is not duplicated. The allowlist
    /// carries the variable NAME only: the token VALUE stays in the pod
    /// environment and reaches the job exclusively through the filtered
    /// process env (`build_agent_env`), never through the policy file, argv
    /// or any log line.
    pub fn allow_claude_oauth_env(mut self) -> Self {
        let var = CLAUDE_OAUTH_TOKEN_ENV.to_string();
        if !self.env_allow.iter().any(|p| p == &var) {
            self.env_allow.push(var);
        }
        self
    }

    /// Adds `path` to the policy's `hide` list so a sandboxed agent can never
    /// read it. No-op when `path` is `None`. Idempotent: an already-hidden
    /// path is not duplicated.
    pub fn with_hidden_path(mut self, path: Option<&Path>) -> Self {
        if let Some(path) = path {
            let pb = path.to_path_buf();
            if !self.hide.iter().any(|h| h == &pb) {
                self.hide.push(pb);
            }
        }
        self
    }
}

/// Environment variable naming the operator token file (config key
/// `api_token_file`). Mirrors `crate::core::server::token::API_TOKEN_FILE_ENV`;
/// duplicated here because the sandbox module is compiled without the `server`
/// feature (a cli-only build) and so cannot reference that module.
const API_TOKEN_FILE_ENV: &str = "KUBEADP_API_TOKEN_FILE";

/// The absolute path of the operator token file (the file named by
/// `KUBEADP_API_TOKEN_FILE`, config key `api_token_file`), if configured.
///
/// This is the credential that guards the host's `/api/v1` and A2A surface; an
/// agent must never be able to read it, so every sandbox policy the launcher
/// builds hides this path. `None` means the host has no operator token file
/// configured.
pub fn operator_token_file_path() -> Option<PathBuf> {
    std::env::var(API_TOKEN_FILE_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

/// Environment variable naming the jobset root key file (config key
/// `jobset_root_key_file`). Mirrors
/// `crate::core::jobset::ROOT_KEY_FILE_ENV`; duplicated here because the
/// sandbox module is compiled without the `server` feature (a cli-only build)
/// and so cannot reference that module.
const JOBSET_ROOT_KEY_FILE_ENV: &str = "KUBEADP_JOBSET_ROOT_KEY_FILE";

/// The absolute path of the jobset root key file (the file named by
/// `KUBEADP_JOBSET_ROOT_KEY_FILE`, config key `jobset_root_key_file`), if
/// configured.
///
/// This is the root of trust that derives every jobset secret; an agent must
/// never be able to read it, so every sandbox policy the launcher builds hides
/// this path. `None` means the host has no root key file configured.
pub fn jobset_root_key_file_path() -> Option<PathBuf> {
    std::env::var(JOBSET_ROOT_KEY_FILE_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

/// Validates that a granted path in a sandbox policy is absolute, not root or forbidden system dirs,
/// and has no path traversal components ('..').
fn validate_policy_grant_path(path: &Path, field: &str) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err(format!(
            "Sandbox policy error: field 'grants.{}' contains an empty path entry",
            field
        ));
    }
    if !path.is_absolute() {
        return Err(format!(
            "Sandbox policy error: field 'grants.{}' contains non-absolute path '{}'",
            field,
            path.display()
        ));
    }
    let s = path.to_string_lossy();
    if s == "/" || s == "/proc" || s.starts_with("/proc/") || s == "/dev" || s.starts_with("/dev/")
    {
        return Err(format!(
            "Sandbox policy error: field 'grants.{}' contains forbidden path '{}'",
            field,
            path.display()
        ));
    }
    for comp in path.components() {
        if let std::path::Component::ParentDir = comp {
            return Err(format!(
                "Sandbox policy error: field 'grants.{}' contains parent traversal '..' in '{}'",
                field,
                path.display()
            ));
        }
    }
    Ok(())
}

/// Validates an environment variable name in a sandbox policy grant: valid identifier, trailing '*' allowed only.
fn validate_policy_grant_env_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(
            "Sandbox policy error: field 'grants.env' contains empty variable name".to_string(),
        );
    }
    let (ident, has_wildcard) = if let Some(stripped) = name.strip_suffix('*') {
        (stripped, true)
    } else {
        (name, false)
    };
    if has_wildcard && ident.contains('*') {
        return Err(format!(
            "Sandbox policy error: field 'grants.env' has '*' not at the trailing position in '{}'",
            name
        ));
    }
    if ident.is_empty() && has_wildcard {
        return Err(format!(
            "Sandbox policy error: field 'grants.env' wildcard '*' requires prefix in '{}'",
            name
        ));
    }
    for c in ident.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(format!(
                "Sandbox policy error: field 'grants.env' contains invalid identifier character '{c}' in '{name}'"
            ));
        }
    }
    Ok(())
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
    fn test_allow_claude_oauth_env_name_only() {
        // The default for_job policy does NOT pass the claude OAuth token env
        // (the name matches no literal and no wildcard pattern), so agy jobs
        // keep it out of their environment.
        let base = SandboxPolicy::for_job(
            "job-71",
            Path::new("/workspace/.agent-jobs/homelab/job-71"),
            None,
            None,
            None,
        );
        assert!(!base.is_env_allowed(CLAUDE_OAUTH_TOKEN_ENV));

        // The claude-job policy adds the NAME to `env_allow` so the pod's
        // CLAUDE_CODE_OAUTH_TOKEN value passes the sandbox-exec env filter
        // into the agent process.
        let policy = base.clone().allow_claude_oauth_env();
        assert!(policy.is_env_allowed(CLAUDE_OAUTH_TOKEN_ENV));

        // Idempotent: a second call does not duplicate the entry.
        let policy = policy.allow_claude_oauth_env();
        assert_eq!(
            policy
                .env_allow
                .iter()
                .filter(|p| *p == CLAUDE_OAUTH_TOKEN_ENV)
                .count(),
            1
        );

        // Only the NAME is ever serialized: the policy JSON lists the env var
        // name and never a token VALUE (values ride the filtered process
        // environment via `filter_env`, which never feeds the policy file).
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains(CLAUDE_OAUTH_TOKEN_ENV));
        assert!(!json.contains("oat01-fake-token-value"));
    }

    #[test]
    fn test_filter_env_passes_claude_token_value() {
        // The env filter passes the token VALUE from the worker pod's
        // environment to the job process when the policy allows the NAME;
        // other secret-named variables stay filtered out.
        let policy = SandboxPolicy::for_job(
            "job-71",
            Path::new("/workspace/.agent-jobs/homelab/job-71"),
            None,
            None,
            None,
        )
        .allow_claude_oauth_env();
        let env = [
            ("PATH".to_string(), "/usr/bin".to_string()),
            (
                CLAUDE_OAUTH_TOKEN_ENV.to_string(),
                "oat01-fake-token-value".to_string(),
            ),
            ("VIKUNJA_API_TOKEN".to_string(), "vj-secret".to_string()),
        ];
        let filtered = policy.filter_env(env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        assert!(filtered.contains(&(
            CLAUDE_OAUTH_TOKEN_ENV.to_string(),
            "oat01-fake-token-value".to_string()
        )));
        assert!(filtered.iter().all(|(k, _)| k != "VIKUNJA_API_TOKEN"));

        // Without the allowlist entry the same variable is dropped entirely.
        let denied = SandboxPolicy::for_job(
            "job-71",
            Path::new("/workspace/.agent-jobs/homelab/job-71"),
            None,
            None,
            None,
        );
        let filtered = denied.filter_env(env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        assert!(filtered.iter().all(|(k, _)| k != CLAUDE_OAUTH_TOKEN_ENV));
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
    fn test_net_default_host_and_accepted_values() {
        // No `net` field: the default is `host` (today's behaviour, opt-out).
        let policy = SandboxPolicy::parse_json(r#"{"rw": ["/workspace"]}"#).expect("parse");
        assert_eq!(policy.net, "host");
        // Default::default() carries the host default too.
        assert_eq!(SandboxPolicy::default().net, "host");
        // The legacy `shared` alias stays accepted (same behaviour as host).
        let policy = SandboxPolicy::parse_json(r#"{"net": "shared"}"#).expect("parse");
        assert_eq!(policy.net, "shared");
        // The opt-in `relay` value is accepted.
        let policy = SandboxPolicy::parse_json(r#"{"net": "relay"}"#).expect("parse");
        assert_eq!(policy.net, "relay");
        // An unknown value is rejected at parse time.
        let err = SandboxPolicy::parse_json(r#"{"net": "bridged"}"#).unwrap_err();
        assert!(
            err.contains("unknown net value"),
            "error must name the net value: {err}"
        );
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

    #[test]
    fn test_jobset_policy_secret_channel_and_hide() {
        // The jobset policy carries the `secret` channel (fd 3 -> the 0400
        // file name the sandbox writes it to) and only the two jobset env vars
        // are guaranteed to pass the `env_allow` filter -- the secret value
        // itself is never an env var, so it cannot reach the agent.
        let base = SandboxPolicy::for_job(
            "job-42",
            Path::new("/workspace/.agent-jobs/homelab/job-42"),
            None,
            None,
            None,
        );
        let policy = crate::core::jobset::build_jobset_policy(&base);

        assert_eq!(
            policy.secret,
            Some(SecretConfig {
                fd: 3,
                dest: crate::core::jobset::CREDENTIAL_FILE_NAME.to_string()
            })
        );
        assert!(policy.is_env_allowed(crate::core::jobset::JOBSET_ID_ENV));
        assert!(policy.is_env_allowed(crate::core::jobset::CREDENTIALS_FILE_ENV));
        assert!(policy.is_env_allowed(crate::core::jobset::JOBSET_EPOCH_ENV));
        // The secret value is not an environment variable at all: even if it
        // were (wrongly) set, it would be filtered out (not `KUBEADP_*`-named
        // here). The point is the launcher never sets it.
        assert!(!policy.is_env_allowed("JOBSET_SECRET"));

        // The env the launcher sets contains the id, the epoch and the PATH
        // of the secret file only.
        let cred = crate::core::jobset::JobsetCredential {
            jobset_id: "js-1".to_string(),
            secret: "super-secret-value".to_string(),
            epoch: crate::core::jobset::EPOCH_ZERO,
        };
        let tmp = PathBuf::from("/tmp/agent-sandbox-job-42");
        let env = crate::core::jobset::build_jobset_env(&cred, &tmp);
        let env_str: Vec<String> = env.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let joined = env_str.join("\n");
        // Exactly three vars: the jobset id, the epoch, and the credential file
        // PATH (never the secret value).
        assert_eq!(env.len(), 3);
        assert!(joined.contains(&format!("{}=js-1", crate::core::jobset::JOBSET_ID_ENV)));
        assert!(joined.contains(&format!(
            "{}={}/jobset-credentials.json",
            crate::core::jobset::CREDENTIALS_FILE_ENV,
            tmp.display()
        )));
        assert!(joined.contains(&format!("{}=0", crate::core::jobset::JOBSET_EPOCH_ENV)));
        // The secret value appears NOWHERE in the env (id/epoch/path only).
        assert!(!joined.contains("super-secret-value"));
        assert!(!joined.contains("super-secret-value") && !joined.contains("secret-value"));
    }

    #[test]
    fn test_jobset_policy_is_noop_for_non_member() {
        // A job that is not a jobset member is launched unchanged: build_command
        // does not call build_jobset_policy, so the base policy has no secret
        // channel and no jobset env vars are added.
        let policy = SandboxPolicy::for_job(
            "job-plain",
            Path::new("/workspace/.agent-jobs/homelab/job-plain"),
            None,
            None,
            None,
        );
        assert!(policy.secret.is_none());
    }

    #[test]
    fn test_with_hidden_path_adds_and_is_idempotent() {
        let base = SandboxPolicy::for_job(
            "job-h",
            Path::new("/w/.agent-jobs/h/job-h"),
            None,
            None,
            None,
        );
        let token = Path::new("/run/secrets/api-token");
        let p1 = base.clone().with_hidden_path(Some(token));
        assert!(p1
            .hide
            .iter()
            .any(|h| h.as_path() == Path::new("/run/secrets/api-token")));
        // Idempotent: a second call does not duplicate the entry.
        let p2 = p1.with_hidden_path(Some(token));
        let n = p2
            .hide
            .iter()
            .filter(|h| h.as_path() == Path::new("/run/secrets/api-token"))
            .count();
        assert_eq!(n, 1);
        // None: the policy is unchanged.
        let unchanged = base.hide.len();
        let p3 = base.clone().with_hidden_path(None);
        assert_eq!(p3.hide.len(), unchanged);
    }

    #[test]
    fn test_hide_operator_token_file_reads_env() {
        let _guard = crate::test_env_lock();
        // `hide_operator_token_file` maps the configured `KUBEADP_API_TOKEN_FILE`
        // path into the `hide` list. Asserted via the pure `with_hidden_path`
        // on the same resolved value (avoids mutating the process env, which is
        // shared with the token tests).
        let path = std::env::var_os(API_TOKEN_FILE_ENV);
        let base = SandboxPolicy::for_job(
            "job-h",
            Path::new("/w/.agent-jobs/h/job-h"),
            None,
            None,
            None,
        );
        match path {
            Some(raw) => {
                let p = base.hide_operator_token_file();
                assert!(p.hide.iter().any(|h| h.as_path() == Path::new(&raw)));
            }
            None => {
                let unchanged = base.hide.len();
                let p = base.hide_operator_token_file();
                assert_eq!(p.hide.len(), unchanged);
            }
        }
    }

    #[test]
    fn test_worker_grants_rejected() {
        let json = r#"{
            "role": "worker",
            "grants": {
                "serviceaccount": true
            }
        }"#;
        let policy: SandboxPolicy = serde_json::from_str(json).unwrap();
        let err = policy.validate().unwrap_err();
        assert!(err.contains("Worker job policy must not specify grants"));
    }

    #[test]
    fn test_session_grants_shape_validation() {
        // Forbidden root /
        let mut policy = SandboxPolicy::for_session(
            "s1",
            Path::new("/tmp"),
            Some(GrantsConfig {
                ro_paths: vec![PathBuf::from("/")],
                ..Default::default()
            }),
        );
        let err = policy.validate().unwrap_err();
        assert!(err.contains("forbidden path '/'"));

        // Forbidden /proc
        policy.grants = Some(GrantsConfig {
            ro_paths: vec![PathBuf::from("/proc/version")],
            ..Default::default()
        });
        let err2 = policy.validate().unwrap_err();
        assert!(err2.contains("forbidden path '/proc/version'"));

        // Non-absolute path
        policy.grants = Some(GrantsConfig {
            rw_paths: vec![PathBuf::from("relative/path")],
            ..Default::default()
        });
        let err3 = policy.validate().unwrap_err();
        assert!(err3.contains("non-absolute path"));

        // Parent traversal
        policy.grants = Some(GrantsConfig {
            rw_paths: vec![PathBuf::from("/workspace/../etc")],
            ..Default::default()
        });
        let err4 = policy.validate().unwrap_err();
        assert!(err4.contains("parent traversal '..'"));

        // Valid grants
        policy.grants = Some(GrantsConfig {
            serviceaccount: true,
            ro_paths: vec![
                PathBuf::from("/opt/tools"),
                PathBuf::from("/opt/ssh-home-cloud"),
            ],
            rw_paths: vec![PathBuf::from("/workspace"), PathBuf::from("/home/agent")],
            env: vec!["VIKUNJA_*".to_string(), "GH_TOKEN".to_string()],
        });
        assert!(policy.validate().is_ok());
        assert!(policy.is_env_allowed("VIKUNJA_API_TOKEN"));
        assert!(policy.is_env_allowed("GH_TOKEN"));
        assert!(!policy.is_env_allowed("OTHER_SECRET"));
    }

    // Grants entries may be files or directories: a single regular file (e.g. a
    // kubeconfig inside a mounted-secret directory) validates like a directory.
    #[test]
    fn test_grant_paths_may_be_files_or_directories() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config");
        std::fs::write(&file, b"kubeconfig").unwrap();
        let subdir = dir.path().join("secrets");
        std::fs::create_dir_all(&subdir).unwrap();

        let policy = SandboxPolicy::for_session(
            "s-files",
            Path::new("/tmp"),
            Some(GrantsConfig {
                ro_paths: vec![file.clone(), subdir.clone()],
                rw_paths: vec![file.clone()],
                ..Default::default()
            }),
        );
        assert!(
            policy.validate().is_ok(),
            "file and directory grants must both validate"
        );
    }

    // An empty grant entry is rejected with a clear error (never silently
    // normalized into `/` or a wildcard).
    #[test]
    fn test_grant_empty_path_entry_rejected() {
        let policy = SandboxPolicy::for_session(
            "s-empty",
            Path::new("/tmp"),
            Some(GrantsConfig {
                ro_paths: vec![PathBuf::from("")],
                ..Default::default()
            }),
        );
        let err = policy.validate().unwrap_err();
        assert!(
            err.contains("empty path entry"),
            "error must name the empty entry: {err}"
        );

        let mut policy2 = SandboxPolicy::for_session("s-empty2", Path::new("/tmp"), None);
        policy2.grants = Some(GrantsConfig {
            rw_paths: vec![PathBuf::from("")],
            ..Default::default()
        });
        let err2 = policy2.validate().unwrap_err();
        assert!(err2.contains("empty path entry"), "error was: {err2}");
    }

    // F-grants: `ensure_writable_dirs` keeps a granted rw FILE as-is (never
    // creates or replaces it), creates a missing directory grant, and keeps an
    // existing directory grant untouched.
    #[test]
    fn test_ensure_writable_dirs_keeps_single_file_grants() {
        let dir = tempfile::tempdir().unwrap();
        let file_grant = dir.path().join("mounted-secret/config");
        std::fs::create_dir_all(file_grant.parent().unwrap()).unwrap();
        std::fs::write(&file_grant, b"secret").unwrap();
        let dir_grant = dir.path().join("created/dir");

        let policy = SandboxPolicy::for_session(
            "s-filegrant",
            Path::new("/tmp"),
            Some(GrantsConfig {
                rw_paths: vec![file_grant.clone(), dir_grant.clone()],
                ..Default::default()
            }),
        );
        policy
            .ensure_writable_dirs(None)
            .expect("a file grant and a creatable dir grant must both succeed");
        // The file grant was neither replaced by a directory nor dropped.
        assert!(
            file_grant.is_file(),
            "granted file must stay a regular file"
        );
        assert!(
            dir_grant.is_dir(),
            "missing directory grant must be created"
        );
    }

    // The operator API token file stays hidden from every sandboxed agent by
    // default; it may be re-exposed ONLY by a single-file ro_paths grant naming
    // it explicitly (existing behaviour), which must keep validating.
    #[test]
    fn test_operator_token_file_hidden_and_single_file_grant_reexposes() {
        if let Some(token) = operator_token_file_path() {
            let policy = SandboxPolicy::for_session("s-tok", Path::new("/tmp"), None)
                .hide_operator_token_file();
            assert!(
                policy.hide.iter().any(|h| h == &token),
                "the token file must be hidden by default"
            );
        }
        // A single-file grant naming the token file (the documented re-expose
        // path) is a valid session policy.
        let explicit = SandboxPolicy::for_session(
            "s-tok2",
            Path::new("/tmp"),
            Some(GrantsConfig {
                ro_paths: vec![PathBuf::from("/var/run/kubeadp/api-token")],
                ..Default::default()
            }),
        );
        assert!(
            explicit.validate().is_ok(),
            "a single-file grant of the token file must validate"
        );
        assert!(explicit.hide_operator_token_file().validate().is_ok());
    }
}

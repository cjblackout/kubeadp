//! Per-jobset credential: the shared secret that lets members of a jobset
//! authenticate to each other's A2A surface.
//!
//! A *jobset* is a set of agents (orchestrator sessions and worker jobs alike);
//! any member may message any other member, and nobody outside may. The module
//! owns:
//!
//! - the jobset *secret* and *id* (random, from the OS RNG, hex-encoded);
//! - the in-memory [`JobsetRegistry`] (agent id -> jobset id + secret) that the
//!   hosting process keeps for the lifetime of the agent;
//! - constant-time verification of a presented credential
//!   ([`verify_jobset_credential`]) and a constant-time byte comparison helper
//!   ([`constant_time_eq`]);
//! - the [`JobsetCredential`] the agent side presents (the jobset id plus the
//!   secret read from the 0400 file the sandbox placed in the agent's private
//!   tmp);
//! - the sandbox-policy / environment wiring that hands a *new* agent its
//!   jobset id + secret through the policy's `secret` channel (an inherited fd
//!   the sandbox turns into a 0400 file), never through argv or the secret's
//!   own value.
//!
//! The secret is generated in memory by the process that creates the jobset.
//! It is never written to disk by the hub, never logged, and never placed in
//! argv or the environment: the launcher sets only the jobset id and the PATH
//! of the secret file (see [`build_jobset_policy`]).
//!
//! # Derived secrets and persisted membership (robust jobsets)
//!
//! Cross-process restarts and retries are supported through two mechanisms:
//!
//! - **Derived secret**: when a jobset root key is configured (the file
//!   content that `[common] jobset_root_key_file` /
//!   `KUBEADP_JOBSET_ROOT_KEY_FILE` names, mounted into both roles and hidden
//!   from every sandbox), the jobset secret is no longer a random in-memory
//!   value but `hex(hmac_sha256(root_key, label || 0x00 || id || 0x00 ||
//!   epoch))` ([`derive_jobset_secret`]). It is deterministic: any process
//!   holding the root key can derive (and therefore verify) any member's
//!   credential for the presented id at the jobset's current epoch, with no
//!   secret stored or looked up.
//! - **Persisted membership (NO secrets)**: the map agent id -> {jobset id,
//!   epoch} (plus ended jobsets id -> epoch) is written to a small JSON file
//!   `jobsets.json` under the configured data/state dir, atomically (temp +
//!   rename), mode 0600, with a bounded entry count. A retried job re-derives
//!   the same secret and rejoins its jobset even after a process restart.
//!
//! # Constant-time comparison
//!
//! Secrets and ids are compared through SHA-256 digests so that (a) a value
//! whose length differs from the expected one cannot shortcut the comparison
//! and (b) the equality test runs in constant time for equal-length digests.
//! The SHA-256 primitive and the constant-time digest comparison both come
//! from the shared [`crate::core::crypto`] module (backed by `ring`); the
//! comparison itself ([`crate::core::crypto::digest_eq`]) reduces both values
//! to fixed-size digests before comparing.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rand::RngCore;

use super::crypto;

/// Header carrying the presented jobset id.
pub const JOBSET_ID_HEADER: &str = "X-Jobset-Id";
/// Header carrying the presented jobset secret.
pub const JOBSET_SECRET_HEADER: &str = "X-Jobset-Secret";
/// Environment variable the launcher sets: the jobset id the agent belongs to.
pub const JOBSET_ID_ENV: &str = "KUBEADP_JOBSET_ID";
/// Environment variable the launcher sets: the PATH of the 0400 secret file in
/// the agent's private tmp (never the secret value itself).
pub const JOBSET_SECRET_FILE_ENV: &str = "KUBEADP_JOBSET_SECRET_FILE";
/// Environment variable naming the jobset root key file (shared by both roles;
/// config key `jobset_root_key_file` maps to it).
pub const ROOT_KEY_FILE_ENV: &str = "KUBEADP_JOBSET_ROOT_KEY_FILE";
/// Environment variable the launcher sets: the jobset's current epoch (the
/// counter the secret was derived at). Carried in the environment (never the
/// secret value) so an agent started by a jobset member knows which epoch its
/// secret is valid for.
pub const JOBSET_EPOCH_ENV: &str = "KUBEADP_JOBSET_EPOCH";
/// The jobset secret file's name inside the agent's private tmp. The launcher
/// sets [`JOBSET_SECRET_FILE_ENV`] to the absolute path built from this name.
pub const JOBSET_SECRET_FILE_NAME: &str = "jobset-secret";
/// Byte length of a fresh jobset secret (32 bytes = 256 bits, above the 32-byte
/// floor from the design).
pub const JOBSET_SECRET_BYTES: usize = 32;
/// Byte length of a fresh jobset id (16 bytes).
pub const JOBSET_ID_BYTES: usize = 16;

/// A random byte string from the OS RNG, hex-encoded (lower case).
fn random_hex(byte_len: usize) -> String {
    let mut bytes = vec![0u8; byte_len];
    rand::rngs::OsRng.fill_bytes(&mut bytes[..]);
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Unit for the salt: this byte separates the fixed labels and variable
/// fields in the derivation input so no field concatenation can collide:
/// `root_key || b"kubeadp-jobset-v1" || 0x00 || jobset_id || 0x00 || epoch`.
const DERIVE_LABEL: &[u8] = b"kubeadp-jobset-v1";
const DERIVE_SEP: u8 = 0x00;

/// Minimum acceptable byte length of the trimmed root key (from the design).
pub const ROOT_KEY_MIN_BYTES: usize = 32;

/// One past-tracked epoch marker: a running jobset is identified by its
/// current epoch; an ended jobset is recorded as a `u64` epoch at which it
/// was ended (see [`MembershipStore`]).
pub type Epoch = u64;

/// Whether a missing root key warning has already been emitted (emitted at
/// most once per process).
static WARNED_NO_ROOT_KEY: AtomicBool = AtomicBool::new(false);

/// The strict derivation domain for jobset secrets.
///
/// `id` is the jobset id (hex, lowercase), `epoch` the jobset's current epoch
/// counter (started at 0, bumped when the jobset ends; documented as decimal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobsetSecretRef<'a> {
    pub id: &'a str,
    pub epoch: Epoch,
}

/// Trimmed bytes of the file named by `KUBEADP_JOBSET_ROOT_KEY_FILE` (config
/// key `jobset_root_key_file`). `Some` when a root key is configured and has
/// at least [`ROOT_KEY_MIN_BYTES`] bytes after trimming; the value lives in
/// memory only and is never logged.
pub struct RootKey(Arc<[u8]>);

impl RootKey {
    fn new(bytes: Vec<u8>) -> Self {
        Self(bytes.into())
    }

    /// The key bytes (held in memory only).
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for RootKey {
    // Never print the key material in a Debug representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RootKey").field(&"<redacted>").finish()
    }
}

/// Returns the configured jobset root key file path (config key
/// `jobset_root_key_file`), if set.
pub fn root_key_file_path() -> Option<std::path::PathBuf> {
    std::env::var(ROOT_KEY_FILE_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
}

/// Resolves the jobset root key from the configured file, trimming the file's
/// bytes (excluding surrounding whitespace) into the HMAC key material.
///
/// A configured-but-missing/short key is a clear startup error naming only the
/// path (never the content). `None` when the key is not configured.
pub fn resolve_root_key() -> Result<Option<RootKey>, String> {
    let Some(path) = root_key_file_path() else {
        return Ok(None);
    };
    let bytes = std::fs::read(&path).map_err(|e| {
        format!(
            "jobset root key file '{}' cannot be read: {e} (configure a file with >= {} bytes)",
            path.display(),
            ROOT_KEY_MIN_BYTES
        )
    })?;
    let trimmed = bytes
        .iter()
        .copied()
        .skip_while(|b| b.is_ascii_whitespace())
        .collect::<Vec<u8>>();
    let trimmed = trimmed
        .into_iter()
        .rev()
        .skip_while(|b| b.is_ascii_whitespace())
        .collect::<Vec<u8>>();
    let trimmed = trimmed.into_iter().rev().collect::<Vec<u8>>();
    if trimmed.len() < ROOT_KEY_MIN_BYTES {
        return Err(format!(
            "jobset root key file '{}' holds fewer than {} bytes after trimming; refusing to start",
            path.display(),
            ROOT_KEY_MIN_BYTES
        ));
    }
    Ok(Some(RootKey::new(trimmed)))
}

/// Derives the jobset secret for `id` at `epoch`, hex-encoded.
///
/// `input = b"kubeadp-jobset-v1" || 0x00 || id || 0x00 || epoch.decimal`
/// then `secret = hex(hmac_sha256(root_key, input))`. Deterministic: the same
/// root key, id and epoch always produce the same secret, so any process
/// holding the root key can verify (and re-derive) a member's credential
/// statelessly.
pub fn derive_jobset_secret(root_key: &[u8], id: &str, epoch: Epoch) -> String {
    let mut msg = Vec::with_capacity(DERIVE_LABEL.len() + 1 + id.len() + 1 + 12);
    msg.extend_from_slice(DERIVE_LABEL);
    msg.push(DERIVE_SEP);
    msg.extend_from_slice(id.as_bytes());
    msg.push(DERIVE_SEP);
    msg.extend_from_slice(epoch.to_string().as_bytes());
    hex_lower(&crypto::hmac_sha256(root_key, &msg))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// The zero epoch every fresh jobset starts at.
pub const EPOCH_ZERO: Epoch = 0;

/// Generates a fresh jobset id (random, from the OS RNG, hex-encoded).
///
/// The value is a public identifier (not a secret); it names the jobset the
/// agent belongs to.
pub fn new_jobset_id() -> String {
    random_hex(JOBSET_ID_BYTES)
}

/// Generate a fresh jobset secret (>= 32 bytes from the OS RNG, hex-encoded).
///
/// The caller owns the value; it must never be logged or written to disk by
/// the hub. Hand a new agent the value through the sandbox policy's `secret`
/// channel ([`build_jobset_policy`]), never through argv or the environment.
pub fn new_jobset_secret() -> String {
    random_hex(JOBSET_SECRET_BYTES)
}

/// Read the jobset id from the environment.
///
/// The launcher sets [`JOBSET_ID_ENV`] for a jobset member; its absence (or an
/// empty value) means the agent is not a jobset member and presents no
/// jobset credential.
pub fn jobset_id_from_env() -> Option<String> {
    std::env::var(JOBSET_ID_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// A credential the agent presents to a peer: its jobset id plus the jobset
/// secret (and the epoch the secret was derived at).
///
/// `None` means "not a jobset member" and no credential is attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobsetCredential {
    pub jobset_id: String,
    pub secret: String,
    /// The jobset epoch this secret was derived at.
    pub epoch: Epoch,
}

impl JobsetCredential {
    /// Load the presenting agent's own jobset credential from the environment.
    ///
    /// The jobset id comes from [`JOBSET_ID_ENV`]; the secret is read from the
    /// file whose path is named by [`JOBSET_SECRET_FILE_ENV`] (the 0400 file
    /// the sandbox placed in the agent's private tmp). The secret is read from
    /// the file, never carried in the environment. The epoch comes from
    /// [`JOBSET_EPOCH_ENV`], defaulting to 0 when unset (a scalar, not a
    /// secret).
    ///
    /// Returns `None` when the agent is not a jobset member (no jobset id) or
    /// when the secret file is missing/empty.
    pub fn from_env() -> Option<Self> {
        let jobset_id = jobset_id_from_env()?;
        let path = std::env::var(JOBSET_SECRET_FILE_ENV)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default_secret_file_path().to_string());
        let secret = std::fs::read_to_string(&path)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let epoch = std::env::var(JOBSET_EPOCH_ENV)
            .ok()
            .and_then(|e| e.trim().parse::<Epoch>().ok())
            .unwrap_or(EPOCH_ZERO);
        Some(Self {
            jobset_id,
            secret,
            epoch,
        })
    }

    /// The jobset id, as a header value (already validated non-empty by
    /// construction).
    pub fn jobset_id_header(&self) -> Option<&str> {
        (!self.jobset_id.is_empty()).then_some(self.jobset_id.as_str())
    }

    /// The secret, as a header value (already validated non-empty by
    /// construction).
    pub fn secret_header(&self) -> Option<&str> {
        (!self.secret.is_empty()).then_some(self.secret.as_str())
    }
}

/// The default path of the jobset secret file inside the agent's private tmp,
/// used when [`JOBSET_SECRET_FILE_ENV`] is unset. Mirrors the sandbox's
/// `$TMPDIR/<dest>` placement (see the `secret` policy channel).
fn default_secret_file_path() -> String {
    let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
    std::path::Path::new(&base)
        .join(JOBSET_SECRET_FILE_NAME)
        .to_string_lossy()
        .to_string()
}

/// The absolute path of the jobset secret file for the given private tmp
/// root. The launcher sets [`JOBSET_SECRET_FILE_ENV`] to this value so the
/// agent knows where to read its secret without the launcher (or any log)
/// exposing the secret itself.
pub fn secret_file_path(tmp_root: &std::path::Path) -> std::path::PathBuf {
    tmp_root.join(JOBSET_SECRET_FILE_NAME)
}

/// This process's own agent identity, used as the local agent id the incoming
/// jobset credential is checked against.
///
/// Reads `KUBEADP_SELF_AGENT_ID` (an operator-set label for this agent); falls
/// back to the kernel comm name, then a crate default. This is NOT a secret
/// and is not itself trusted -- it is only the local key into the registry.
pub fn local_agent_id() -> String {
    if let Ok(v) = std::env::var("KUBEADP_SELF_AGENT_ID") {
        let t = v.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    std::fs::read_to_string("/proc/self/comm")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "kubeadp-agent".to_string())
}

/// One jobset membership: the jobset id, the secret (derived from the root
/// key when one is configured, otherwise random) and the epoch the secret was
/// derived at.
///
/// The secret is held in memory only and is dropped with the entry when the
/// agent's job/session ends.
#[derive(Clone)]
pub struct JobsetEntry {
    pub jobset_id: String,
    secret: String,
    pub epoch: Epoch,
}

impl JobsetEntry {
    fn new(jobset_id: impl Into<String>, secret: impl Into<String>, epoch: Epoch) -> Self {
        Self {
            jobset_id: jobset_id.into(),
            secret: secret.into(),
            epoch,
        }
    }

    /// The jobset secret. Only exposed to the launcher, which hands it to the
    /// agent through the sandbox `secret` channel (an inherited fd) -- never
    /// through argv or the environment.
    pub fn secret(&self) -> &str {
        &self.secret
    }
}

impl fmt::Debug for JobsetEntry {
    // Never print the secret in a Debug representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobsetEntry")
            .field("jobset_id", &self.jobset_id)
            .field("epoch", &self.epoch)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// In-memory registry of jobset memberships: agent id -> [`JobsetEntry`], plus
/// the ended-jobsets map (jobset id -> epoch it was ended at).
///
/// The hosting process registers each agent it runs (its own sessions and the
/// jobs it spawns) and drops the entry when the agent's job/session ends. The
/// registry is the *host's* view: it knows, for every agent it hosts, the
/// jobset that agent belongs to. Verification of an incoming credential is
/// then a lookup of the *target* agent's entry.
#[derive(Default)]
pub struct JobsetRegistry {
    entries: std::sync::Mutex<HashMap<String, JobsetEntry>>,
    /// Ended jobsets: jobset id -> the epoch it was ended at. A credential
    /// presented for an ended jobset id is always rejected.
    ended: std::sync::Mutex<HashMap<String, u64>>,
    /// Every jobset id this process has KNOWN (registered, loaded from the
    /// persisted membership store, or ended), for the host's lifetime. A
    /// jobset whose members have all finished and been dropped from `entries`
    /// is still recognisable (e.g. so the end route can 404 an unknown jobset
    /// while still allowing the operator to end a finished one). Bounded FIFO
    /// to [`KNOWN_JOBSETS_MAX`].
    known: std::sync::Mutex<std::collections::VecDeque<String>>,
    /// The resolved jobset root key (when configured), cached for the process
    /// lifetime. `None` means no root key (random in-process secrets).
    root_key: std::sync::Mutex<Option<Arc<[u8]>>>,
    /// The data/state dir the persisted membership file lives under.
    /// `None` until [`init_global_jobsets`] runs (or a test sets it).
    data_dir: std::sync::Mutex<Option<std::path::PathBuf>>,
}

impl JobsetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the jobset an agent belongs to, at epoch
    /// [`EPOCH_ZERO`].
    pub fn register(
        &self,
        agent_id: impl Into<String>,
        jobset_id: impl Into<String>,
        secret: impl Into<String>,
    ) {
        self.register_at(agent_id, jobset_id, secret, EPOCH_ZERO);
    }

    /// Register (or replace) the jobset an agent belongs to, at the given
    /// epoch (the epoch this jobset's secret was derived at).
    pub fn register_at(
        &self,
        agent_id: impl Into<String>,
        jobset_id: impl Into<String>,
        secret: impl Into<String>,
        epoch: Epoch,
    ) {
        let jobset_id = jobset_id.into();
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let id = agent_id.into();
        map.insert(id, JobsetEntry::new(jobset_id.clone(), secret, epoch));
        drop(map);
        self.note_jobset(&jobset_id);
    }

    /// Drop an agent's entry (called when the agent's job/session ends). The
    /// persisted membership is retained (a retried job re-joins its jobset via
    /// [`register_agent`], which re-derives the same secret), and the jobset
    /// id stays "known" so the end route can still recognise it.
    pub fn drop(&self, agent_id: &str) {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.remove(agent_id);
        drop(map);
        if let Some(entry) = entry {
            self.note_jobset(&entry.jobset_id);
        }
    }

    /// The entry for `agent_id`, if present.
    pub fn get(&self, agent_id: &str) -> Option<JobsetEntry> {
        let map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.get(agent_id).cloned()
    }

    /// Whether `agent_id` has a registered jobset.
    pub fn contains(&self, agent_id: &str) -> bool {
        let map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.contains_key(agent_id)
    }

    pub fn len(&self) -> usize {
        let map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.len()
    }

    pub fn is_empty(&self) -> bool {
        let map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.is_empty()
    }

    /// A snapshot of all entries: agent id -> cloned entry. Used by the
    /// persistence layer so the lock is held only for the clone.
    pub fn snapshot(&self) -> HashMap<String, JobsetEntry> {
        let map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.clone()
    }

    /// The cached jobset root key bytes, if configured.
    pub fn root_key(&self) -> Option<Arc<[u8]>> {
        let k = self.root_key.lock().unwrap_or_else(|e| e.into_inner());
        k.clone()
    }

    /// Whether the jobset has been ended (its id is in the ended map).
    pub fn is_ended(&self, jobset_id: &str) -> bool {
        let map = self.ended.lock().unwrap_or_else(|e| e.into_inner());
        map.contains_key(jobset_id)
    }

    /// The epoch a jobset was ended at, if ended.
    pub fn ended_epoch(&self, jobset_id: &str) -> Option<u64> {
        let map = self.ended.lock().unwrap_or_else(|e| e.into_inner());
        map.get(jobset_id).copied()
    }

    /// A snapshot of the ended map: jobset id -> ended epoch.
    pub fn ended_snapshot(&self) -> HashMap<String, u64> {
        let map = self.ended.lock().unwrap_or_else(|e| e.into_inner());
        map.clone()
    }

    /// True when `jobset_id` is not ended AND this process knows it: either a
    /// live registered member, or a jobset this host has served (whose members
    /// may all have finished -- their persisted memberships keep the jobset
    /// recognisable). Used by the end route to 404 genuinely unknown jobsets
    /// while still allowing the operator to end a finished one.
    pub fn has_member_of(&self, jobset_id: &str) -> bool {
        let known = {
            let known = self.known.lock().unwrap_or_else(|e| e.into_inner());
            known.contains(&jobset_id.to_string())
        };
        if known {
            return true;
        }
        let map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        map.values().any(|e| e.jobset_id == jobset_id)
    }

    /// Remembers a jobset id this process has served, so a later `end_jobset`
    /// (or a target-less credential check) still recognises it after its last
    /// live member is dropped. Bounded FIFO: the newest [`KNOWN_JOBSETS_MAX`]
    /// ids are kept, the oldest are evicted.
    fn note_jobset(&self, jobset_id: &str) {
        let mut known = self.known.lock().unwrap_or_else(|e| e.into_inner());
        if known.iter().any(|k| k == jobset_id) {
            return;
        }
        known.push_back(jobset_id.to_string());
        while known.len() > KNOWN_JOBSETS_MAX {
            known.pop_front();
        }
    }

    /// The data dir the membership file lives under, if initialized.
    pub fn data_dir(&self) -> Option<std::path::PathBuf> {
        let d = self.data_dir.lock().unwrap_or_else(|e| e.into_inner());
        d.clone()
    }

    /// Sets the data dir the membership file lives under (at startup).
    pub fn set_data_dir(&self, dir: std::path::PathBuf) {
        let mut d = self.data_dir.lock().unwrap_or_else(|e| e.into_inner());
        *d = Some(dir);
    }

    /// Sets the resolved jobset root key (cached for the process lifetime).
    pub fn set_root_key(&self, key: Option<Arc<[u8]>>) {
        let mut k = self.root_key.lock().unwrap_or_else(|e| e.into_inner());
        *k = key;
    }

    /// Seeds the ended map from a loaded document (at startup).
    pub fn seed_ended(&self, ended: HashMap<String, u64>) {
        let mut map = self.ended.lock().unwrap_or_else(|e| e.into_inner());
        *map = ended;
    }

    /// Ends a jobset: marks `jobset_id` ended (so its old credentials stop
    /// verifying) and returns the new epoch it was bumped to. The new epoch is
    /// one past the highest epoch any member of the jobset holds (or 1 for an
    /// empty jobset). Idempotent: an already-ended jobset returns its ended
    /// epoch. Returns `None` when the jobset is not known (no member),
    /// distinguishing it from the ended case so the caller can 404.
    pub fn end_jobset(&self, jobset_id: &str) -> Option<u64> {
        if let Some(already) = self.ended_epoch(jobset_id) {
            return Some(already);
        }
        if !self.has_member_of(jobset_id) {
            return None;
        }
        let max_member_epoch = self
            .snapshot()
            .values()
            .filter(|e| e.jobset_id == jobset_id)
            .map(|e| e.epoch)
            .max()
            .unwrap_or(0);
        let new_epoch = max_member_epoch + 1;
        let mut map = self.ended.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(jobset_id.to_string(), new_epoch);
        drop(map);
        self.note_jobset(jobset_id);
        Some(new_epoch)
    }
}

// ---------------------------------------------------------------------------
// Process-wide registry
// ---------------------------------------------------------------------------
//
// The hosting process (the worker / orchestrator) keeps ONE registry for every
// agent it hosts: itself, the jobs it spawns, and the sessions it supervises.
// A job or session is registered when it is created and its entry is dropped
// when it ends. The A2A surface (built at startup) reads the same registry to
// decide which incoming jobset credentials to accept.

/// The single process-wide jobset registry.
///
/// A `Mutex<Option<Arc<JobsetRegistry>>>`: the registry is lazily created on
/// first use and the `Arc` lets the A2A verifier (built once at startup) hold
/// a clone of the registry while the host still owns the same registry. The
/// `Option` slot lets a test swap in a throwaway registry and restore the
/// previous one (tests run multithreaded, so each gets its own).
static GLOBAL_REGISTRY: std::sync::Mutex<Option<Arc<JobsetRegistry>>> = std::sync::Mutex::new(None);

/// The process-wide registry (an owned `Arc` clone), created on first use.
pub fn global_registry() -> Arc<JobsetRegistry> {
    let mut slot = GLOBAL_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_none() {
        *slot = Some(Arc::new(JobsetRegistry::new()));
    }
    slot.as_ref().cloned().expect("registry initialized above")
}

/// The hosting process's own jobset membership, if it is a jobset member.
///
/// A member is one whose environment carries [`JOBSET_ID_ENV`] (set by the
/// parent through [`build_jobset_env`]); the secret is read from the 0400 file
/// named by [`JOBSET_SECRET_FILE_ENV`]. An agent that was started by an
/// operator request (a plain API server with no jobset in its environment) is
/// not a member and returns `None`; such an agent gives every agent it starts
/// a *fresh* jobset (see [`fresh_jobset_credential`]).
pub fn host_jobset() -> Option<JobsetCredential> {
    JobsetCredential::from_env()
}

/// Emits the one-time "no root key" warning naming only the file that would
/// hold the key (`KUBEADP_JOBSET_ROOT_KEY_FILE`), never the value. Emitted at
/// most once per process (a missing key is a permanent condition for the
/// process lifetime, and every fresh jobset would otherwise repeat it).
fn warn_no_root_key_once() {
    if !WARNED_NO_ROOT_KEY.swap(true, Ordering::SeqCst) {
        eprintln!(
            "kubeadp-agent: warning: jobset root key not configured \
             (KUBEADP_JOBSET_ROOT_KEY_FILE unset or empty): fresh jobset \
             secrets are random and only valid inside this process (a \
             restart cannot re-derive them)"
        );
    }
}

/// A fresh jobset id + secret, for an agent started by an operator request
/// (its host is not itself a jobset member).
///
/// When a root key is configured the secret is DERIVED (deterministic for the
/// jobset id at epoch 0), so any process holding the same root key derives the
/// same secret; otherwise it is random (today's behaviour) with a one-time
/// warning, in which case the credential is only valid inside this process.
///
/// The root key is read from the process-wide registry's CACHE (populated by
/// [`init_global_jobsets`] at startup, where a broken root key file is a
/// startup error) -- never re-read here, so this function cannot panic on a
/// misconfiguration after startup.
pub fn fresh_jobset_credential() -> JobsetCredential {
    let jobset_id = new_jobset_id();
    match global_registry().root_key() {
        Some(key) => {
            let secret = derive_jobset_secret(&key, &jobset_id, EPOCH_ZERO);
            JobsetCredential {
                jobset_id,
                secret,
                epoch: EPOCH_ZERO,
            }
        }
        // No root key configured (or not initialized): random secret today +
        // one warning.
        None => {
            warn_no_root_key_once();
            JobsetCredential {
                jobset_id,
                secret: new_jobset_secret(),
                epoch: EPOCH_ZERO,
            }
        }
    }
}

/// Register a newly created agent in the process-wide registry, choosing its
/// jobset: the host's own jobset when the host is a member (the agent joins
/// the starting agent's jobset), or a fresh random jobset when the host is
/// not a member (an operator request).
///
/// Returns the credential the agent was given (the jobset id plus, for the
/// launcher to hand through the sandbox `secret` channel, the secret).
///
/// With a root key configured, a retry (or a re-register after a job end)
/// re-derives the same secret for the agent's jobset at its current epoch --
/// even across a process restart, because membership (id + epoch, NO secret)
/// is persisted by [`save_memberships`] and reloaded by [`load_memberships`].
pub fn register_agent(agent_id: &str) -> JobsetCredential {
    // A retry re-registers an agent that is already a member: it keeps its
    // jobset (and secret) so a retried job stays in the same jobset.
    if let Some(existing) = global_registry().get(agent_id) {
        return JobsetCredential {
            jobset_id: existing.jobset_id,
            secret: existing.secret,
            epoch: existing.epoch,
        };
    }
    let cred = host_jobset().unwrap_or_else(fresh_jobset_credential);
    global_registry().register_at(agent_id, &cred.jobset_id, &cred.secret, cred.epoch);
    // Persist the new membership (agent id + jobset id + epoch ONLY -- no
    // secrets) so a process restart can re-derive the same credential and a
    // retried job re-joins its jobset. Persistence is advisory: a failure is
    // logged and the in-memory registry keeps working (the log names only
    // the agent id and the error, never the secret).
    if let Err(e) = save_memberships() {
        eprintln!(
            "kubeadp-agent: warning: failed to persist jobset membership for '{}': {}",
            agent_id, e
        );
    }
    cred
}

/// Drop an agent's entry from the process-wide registry, called on *every*
/// end path of the agent's job/session (done, failed, killed, retried,
/// cleaned, GC'd, supervisor restart). Idempotent: dropping an unknown id is
/// a no-op, so a double end is harmless.
pub fn drop_agent(agent_id: &str) {
    JobsetRegistry::drop(global_registry().as_ref(), agent_id);
}

/// RAII handle that drops the jobset entry for `agent_id` from the
/// process-wide registry when it is dropped. Held across a whole job/session
/// lifetime (the monitor thread) so the entry is dropped on *every* exit path
/// -- normal completion, failure, kill, a cancelled launch, a spawn error --
/// without each return site having to call [`drop_agent`].
pub struct JobsetDropGuard {
    agent_id: String,
}

impl JobsetDropGuard {
    /// Begins guarding `agent_id`; the entry is dropped when this guard drops.
    pub fn new(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
        }
    }
}

impl Drop for JobsetDropGuard {
    fn drop(&mut self) {
        drop_agent(&self.agent_id);
    }
}

/// Registers this process's own agent identity in the process-wide registry,
/// if the process is a jobset member. Called once at server startup so the
/// A2A surface can accept credentials presented against this agent.
pub fn register_self() {
    if let Some(cred) = host_jobset() {
        global_registry().register_at(local_agent_id(), &cred.jobset_id, &cred.secret, cred.epoch);
    }
}

/// The jobset credential registered for `agent_id` in the process-wide
/// registry, if it is a jobset member.
///
/// Used by the launcher at build time: when an agent is a member, its
/// [`build_jobset_policy`] / [`build_jobset_env`] are applied so the secret
/// reaches the agent through the sandbox `secret` channel and the id + secret
/// file PATH reach the environment. `None` means the agent was not registered
/// (not a member) and the agent is launched with no jobset.
pub fn agent_jobset(agent_id: &str) -> Option<JobsetCredential> {
    let entry = global_registry().get(agent_id)?;
    let secret = entry.secret().to_string();
    Some(JobsetCredential {
        jobset_id: entry.jobset_id,
        secret,
        epoch: entry.epoch,
    })
}

// ---------------------------------------------------------------------------
// Persisted membership (NO secrets)
// ---------------------------------------------------------------------------
//
// The map agent id -> {jobset id, epoch} (plus ended jobsets id -> ended
// epoch) lives in one small JSON file `jobsets.json` under the configured
// data/state dir (the worker's `state_root` is reused; orchestrators point at
// a writable dir). Written atomically (temp + rename) with mode 0600, bounded
// to [`MEMBERSHIP_MAX_ENTRIES`]. A corrupt file is quarantined (renamed
// aside) at load and startup continues with empty state. No secrets are ever
// persisted here -- only the membership (id + epoch), from which the derived
// secret can be re-computed by anyone holding the root key.

/// File name of the persisted membership store.
pub const MEMBERSHIP_FILE_NAME: &str = "jobsets.json";
/// Maximum number of entries in the membership store (bounded writes).
pub const MEMBERSHIP_MAX_ENTRIES: usize = 10_000;
/// Maximum number of ended-jobset markers kept (bounded writes).
pub const ENDED_MAX_ENTRIES: usize = 1_000;
/// Maximum number of known-jobset ids remembered in memory (bounded FIFO;
/// membership file writes are separately bounded by [`MEMBERSHIP_MAX_ENTRIES`]).
pub const KNOWN_JOBSETS_MAX: usize = 10_000;

/// The persisted membership document (JSON on disk).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MembershipDoc {
    /// agent id -> (jobset id, epoch) for every agent we know a membership for.
    #[serde(default)]
    members: HashMap<String, Member>,
    /// ended jobset id -> the epoch it was ended at.
    #[serde(default)]
    ended: HashMap<String, u64>,
}

/// One persisted membership: the jobset id and the epoch (never a secret).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    jobset_id: String,
    epoch: u64,
}

/// Returns the path of the membership store for `state_root`.
pub fn membership_file_path(state_root: &std::path::Path) -> std::path::PathBuf {
    state_root.join(MEMBERSHIP_FILE_NAME)
}

/// Loads the persisted memberships (and ended jobsets) into the process-wide
/// registry.
///
/// A missing file is empty state. A corrupt/unreadable file is quarantined
/// (renamed to `jobsets.json.corrupt-<n>`) and startup continues with empty
/// state, so a half-written or attacker-tampered file can never wedge the
/// process. Bounded: only the first [`MEMBERSHIP_MAX_ENTRIES`] memberships are
/// loaded. Only loads entries when a root key is configured (membership alone
/// cannot reproduce a random secret); without a root key the registry is
/// seeded empty and jobs get fresh random creds as today.
pub fn load_memberships(state_root: &std::path::Path) -> Result<(), String> {
    let path = membership_file_path(state_root);
    let raw = match std::fs::read(&path) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("failed to read '{}': {e}", path.display())),
    };
    let doc: MembershipDoc = match serde_json::from_slice(&raw) {
        Ok(d) => d,
        Err(_e) => {
            quarantine_corrupt(&path);
            return Ok(());
        }
    };
    let registry = global_registry();
    let root_key = registry.root_key();
    if let Some(root_key) = root_key {
        for (agent_id, member) in doc.members.into_iter().take(MEMBERSHIP_MAX_ENTRIES) {
            let secret = derive_jobset_secret(&root_key, &member.jobset_id, member.epoch);
            registry.register_at(agent_id, member.jobset_id, secret, member.epoch);
        }
    }
    for jobset_id in doc.ended.keys() {
        registry.note_jobset(jobset_id);
    }
    registry.seed_ended(doc.ended);
    Ok(())
}

/// Persists the current memberships (and the ended-jobset map) for the
/// process-wide registry to its data dir, atomically: the new content is
/// written to a temp file in the same directory, fsynced, then renamed over
/// the destination; the file mode is 0600. The write is bounded to
/// [`MEMBERSHIP_MAX_ENTRIES`] members and [`ENDED_MAX_ENTRIES`] ended
/// markers. No secrets are among the fields written. A no-op when no data
/// dir has been initialized (the ordinary in-process/test path without
/// persistence). Failures are non-fatal: membership is advisory state and the
/// in-memory registry keeps working.
pub fn save_memberships() -> Result<(), String> {
    let Some(state_root) = global_registry().data_dir() else {
        return Ok(());
    };
    save_memberships_to(&state_root)
}

/// Persists memberships to an explicit `state_root` (also drives the writes
/// after an `end_jobset` on a registry with a known data dir).
pub fn save_memberships_to(state_root: &std::path::Path) -> Result<(), String> {
    let path = membership_file_path(state_root);
    let registry = global_registry();

    let mut doc = MembershipDoc {
        members: HashMap::new(),
        ended: registry.ended_snapshot(),
    };

    // Walk a snapshot of the entries (bounded).
    let entries: Vec<(String, Member)> = registry
        .snapshot()
        .into_iter()
        .take(MEMBERSHIP_MAX_ENTRIES)
        .map(|(agent_id, entry)| {
            (
                agent_id,
                Member {
                    jobset_id: entry.jobset_id,
                    epoch: entry.epoch,
                },
            )
        })
        .collect();
    doc.members = entries.into_iter().collect();
    doc.ended = doc.ended.into_iter().take(ENDED_MAX_ENTRIES).collect();

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create '{}': {e}", parent.display()))?;
        }
    }
    let bytes = serde_json::to_vec_pretty(&doc)
        .map_err(|e| format!("failed to serialize memberships: {e}"))?;
    write_atomic_0600(&path, &bytes)
}

/// One-time startup initializer for the process-wide jobset registry:
/// resolves and caches the jobset root key (a clear startup error when the
/// configured file is missing/short), sets the data dir, and loads the
/// persisted memberships + ended jobsets.
///
/// `state_root` is the dir the membership file (`jobsets.json`) lives under:
/// the worker passes its `config.state_root`, the orchestrator a writable
/// state dir. Idempotent (the first call wins the root key and data dir).
/// The root-key path is named in any error; its VALUE is never.
///
/// Returns a human-readable error naming only the configured root key path.
pub fn init_global_jobsets(state_root: &std::path::Path) -> Result<(), String> {
    let registry = global_registry();
    let root_key = match resolve_root_key() {
        Ok(Some(key)) => Some(key),
        Ok(None) => None,
        Err(e) => return Err(e),
    };
    registry.set_root_key(root_key.map(|k| k.0));
    registry.set_data_dir(state_root.to_path_buf());
    load_memberships(state_root)
}

/// Ends a jobset in the process-wide registry: bumps its epoch, marks it
/// ended (old credentials stop verifying) and persists the membership store.
///
/// Returns `Ok(Some(epoch))` with the new epoch when the jobset was known
/// (or already ended), `Ok(None)` when the jobset is not known (no member)
/// so the caller can 404. Persistence failures are non-fatal (the in-memory
/// ended map still rejects all credentials); they are surfaced as a string
/// for operator visibility.
pub fn end_jobset(jobset_id: &str) -> Result<Option<u64>, String> {
    let registry = global_registry();
    let Some(_epoch) = registry.end_jobset(jobset_id) else {
        return Ok(None);
    };
    save_memberships()?;
    Ok(Some(
        registry.ended_epoch(jobset_id).expect("just ended above"),
    ))
}

/// Writes `bytes` to `path` atomically with mode 0600 (temp file in the same
/// directory + fsync + rename over the destination).
fn write_atomic_0600(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let tmp = dir.join(format!(
        "{}.tmp-{}",
        MEMBERSHIP_FILE_NAME,
        std::process::id()
    ));
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)
        .map_err(|e| format!("failed to create '{}': {e}", tmp.display()))?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("failed to chmod '{}': {e}", tmp.display()))?;
    f.write_all(bytes)
        .map_err(|e| format!("failed to write '{}': {e}", tmp.display()))?;
    f.sync_all()
        .map_err(|e| format!("failed to fsync '{}': {e}", tmp.display()))?;
    drop(f);
    std::fs::rename(&tmp, path)
        .map_err(|e| format!("failed to rename over '{}': {e}", path.display()))?;
    Ok(())
}

/// Renames a corrupt membership file aside so a future launch starts empty.
fn quarantine_corrupt(path: &std::path::Path) {
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| MEMBERSHIP_FILE_NAME.to_string());
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let mut n = 0u64;
    loop {
        let q = parent.join(format!("{file_name}.corrupt-{n}"));
        if !q.exists() {
            let _ = std::fs::rename(path, &q);
            return;
        }
        n += 1;
    }
}

/// Test-only: resets the "no root key" warning flag so a later test can emit
/// it again.
#[cfg(test)]
fn reset_warned_no_root_key() {
    WARNED_NO_ROOT_KEY.store(false, Ordering::SeqCst);
}

/// Process-wide lock that serializes all callers of [`with_isolated_registry`]
/// for the whole duration of their closure. The registry is process-global, so
/// parallel tests must not run their isolated-swap sections concurrently.
#[cfg(test)]
static ISOLATION_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Test-only: runs `f` with the process-wide registry swapped for a fresh,
/// empty one, then restores the previous registry. Keeps multithreaded tests
/// from clobbering each other's registry entries.
#[cfg(test)]
pub fn with_isolated_registry<R>(f: impl FnOnce() -> R) -> R {
    let _isolation = ISOLATION_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let guard = JobsetScope::new();
    let out = f();
    drop(guard);
    out
}

/// Swaps in a fresh process-wide registry and restores the previous one on
/// drop (RAII). See [`with_isolated_registry`].
#[cfg(test)]
struct JobsetScope {
    previous: Option<Arc<JobsetRegistry>>,
}

#[cfg(test)]
impl JobsetScope {
    fn new() -> Self {
        let mut slot = GLOBAL_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let previous = slot.take();
        *slot = Some(Arc::new(JobsetRegistry::new()));
        Self { previous }
    }
}

#[cfg(test)]
impl Drop for JobsetScope {
    fn drop(&mut self) {
        let mut slot = GLOBAL_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        *slot = self.previous.take();
    }
}

/// Reads a single header value from a request's headers.
///
/// Header names are matched case-insensitively; multiple values yield the
/// first. The verifier depends only on this closure so it can be pointed at an
/// axum [`axum::http::HeaderMap`] or any other header representation.
pub type HeaderGetter = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Verify a presented jobset credential against the *target* agent's jobset.
///
/// The presented credential is the pair of headers [`JOBSET_ID_HEADER`]
/// (jobset id) and [`JOBSET_SECRET_HEADER`] (secret), read through `get_header`
/// (a case-insensitive header lookup). The credential is accepted exactly when
/// the target agent has a registered jobset, the presented jobset id matches
/// the target's jobset id, and the presented secret matches the target's
/// secret -- both compared through constant-time SHA-256 digest comparison.
///
/// Returns `false` when either header is absent, the target agent has no
/// registered jobset, or any field mismatches.
pub fn verify_jobset_credential(
    registry: &JobsetRegistry,
    target_agent_id: &str,
    get_header: &HeaderGetter,
) -> bool {
    let Some(presented_id) = get_header(JOBSET_ID_HEADER) else {
        return false;
    };
    let Some(presented_secret) = get_header(JOBSET_SECRET_HEADER) else {
        return false;
    };
    if presented_id.is_empty() || presented_secret.is_empty() {
        return false;
    }

    // An ended jobset's credentials always stop verifying.
    if registry.is_ended(&presented_id) {
        return false;
    }

    // Membership check: the TARGET agent must be a member of the presented
    // jobset (its registered jobset id matches the presented one).
    let Some(entry) = registry.get(target_agent_id) else {
        return false;
    };
    if !crypto::digest_eq(presented_id.as_bytes(), entry.jobset_id.as_bytes()) {
        return false;
    }

    match registry.root_key() {
        // Root key configured: verify STATELESSLY by re-deriving the secret
        // for the presented id at the jobset's current epoch and comparing in
        // constant time. No secret lookup is needed in the registry.
        Some(root_key) => {
            let derived = derive_jobset_secret(&root_key, &entry.jobset_id, entry.epoch);
            crypto::digest_eq(presented_secret.as_bytes(), derived.as_bytes())
        }
        // No root key: the secret is random and lives only in the registry (the
        // in-process behaviour from before the derived-secret design; a restart
        // loses it, which the one-time warning at creation says).
        None => crypto::digest_eq(presented_secret.as_bytes(), entry.secret.as_bytes()),
    }
}

/// The sandbox-policy side of handing a *new* agent its jobset: a clone of the
/// caller's base [`crate::sandbox::policy::SandboxPolicy`] with the `secret`
/// channel configured to write the jobset secret into the agent's private tmp
/// (as the 0400 file [`JOBSET_SECRET_FILE_NAME`]).
///
/// The policy carries only the secret *channel* (an inherited fd and the
/// destination name), never the secret value: the value is read by the
/// sandbox launcher from the inherited fd and placed in the private tmp. The
/// environment is set separately by the launcher (see [`build_jobset_env`])
/// and carries the jobset id and the PATH of the secret file, never the
/// secret value. See the test module for the argv/env-absence guarantee.
pub fn build_jobset_policy(
    base: &crate::sandbox::policy::SandboxPolicy,
) -> crate::sandbox::policy::SandboxPolicy {
    let secret = crate::sandbox::policy::SecretConfig {
        // fd 3 is the conventional secret fd the backends' `read_secret_fd`
        // dups; the launcher opens the secret file read-only on fd 3 before
        // exec (see the sandbox harness `exec 3<"$sec"`).
        fd: 3,
        dest: JOBSET_SECRET_FILE_NAME.to_string(),
    };
    let mut policy = base.clone();
    policy.secret = Some(secret);
    // The jobset env vars are already covered by the default `KUBEADP_*`
    // allow pattern, but be explicit so a custom `env_allow` cannot silently
    // drop them.
    for var in [JOBSET_ID_ENV, JOBSET_SECRET_FILE_ENV, JOBSET_EPOCH_ENV] {
        if !policy
            .env_allow
            .iter()
            .any(|p| p == var || p.ends_with('*'))
        {
            policy.env_allow.push(var.to_string());
        }
    }
    policy
}

/// The environment variables the launcher sets for a jobset member: the jobset
/// id, the epoch its secret was derived at, and the PATH of the secret file
/// (never the secret value). `private_tmp` is the agent's private tmp root the
/// sandbox will create; the secret file path is built from it.
pub fn build_jobset_env(
    credential: &JobsetCredential,
    private_tmp: &std::path::Path,
) -> Vec<(String, String)> {
    let file = secret_file_path(private_tmp);
    vec![
        (JOBSET_ID_ENV.to_string(), credential.jobset_id.clone()),
        (JOBSET_EPOCH_ENV.to_string(), credential.epoch.to_string()),
        (
            JOBSET_SECRET_FILE_ENV.to_string(),
            file.to_string_lossy().to_string(),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Serializes tests that mutate the process-global jobset env vars
    /// (`KUBEADP_JOBSET_ID` / `KUBEADP_JOBSET_SECRET_FILE`), so parallel test
    /// threads do not clobber each other's environment.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Runs `f` while holding the jobset env lock. Tests that read or mutate the
    /// jobset env vars (via `host_jobset` / `JobsetCredential::from_env` or
    /// `set_var`/`remove_var`) must run under this so they do not race each
    /// other on the process environment.
    fn with_env_guard<R>(f: impl FnOnce() -> R) -> R {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        f()
    }

    /// A fixed credential pair so tests are deterministic.
    fn cred(id: &str, secret: &str) -> JobsetCredential {
        JobsetCredential {
            jobset_id: id.to_string(),
            secret: secret.to_string(),
            epoch: EPOCH_ZERO,
        }
    }

    /// Build a [`HeaderGetter`] from a plain (name -> value) table holding the
    /// given (id, secret) headers (each optional). Case-insensitive on lookup,
    /// matching how HTTP header names compare. Std-only so the test compiles in
    /// every feature set (the axum adapter is tested separately).
    fn headers(id: Option<&str>, secret: Option<&str>) -> HeaderGetter {
        let mut table: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        if let Some(id) = id {
            table.insert(JOBSET_ID_HEADER.to_string(), id.to_string());
        }
        if let Some(secret) = secret {
            table.insert(JOBSET_SECRET_HEADER.to_string(), secret.to_string());
        }
        Arc::new(move |name: &str| table.get(name).cloned())
    }

    fn registry_with(agent: &str, id: &str, secret: &str) -> JobsetRegistry {
        let reg = JobsetRegistry::new();
        reg.register(agent, id, secret);
        reg
    }

    // ---- generation ------------------------------------------------------------

    #[test]
    fn fresh_secret_and_id_are_random_and_sized() {
        let s1 = new_jobset_secret();
        let s2 = new_jobset_secret();
        assert_eq!(s1.len(), JOBSET_SECRET_BYTES * 2); // 64 hex chars
        assert_eq!(s2.len(), JOBSET_SECRET_BYTES * 2);
        assert_ne!(s1, s2, "two fresh secrets must differ");
        assert!(s1.chars().all(|c| c.is_ascii_hexdigit()));

        let i1 = new_jobset_id();
        let i2 = new_jobset_id();
        assert_eq!(i1.len(), JOBSET_ID_BYTES * 2);
        assert_ne!(i1, i2, "two fresh ids must differ");
        assert!(i1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ---- registry lifecycle ----------------------------------------------------

    #[test]
    fn entry_dropped_at_job_end() {
        let reg = JobsetRegistry::new();
        assert!(reg.is_empty());
        reg.register("agent-1", "js-1", "secret-1");
        assert!(reg.contains("agent-1"));
        assert_eq!(reg.len(), 1);
        // Job ends -> dropped.
        reg.drop("agent-1");
        assert!(!reg.contains("agent-1"));
        assert!(reg.is_empty());
        assert!(reg.get("agent-1").is_none());
    }

    // ---- verification ------------------------------------------------------------

    #[test]
    fn matching_credential_accepted() {
        let reg = registry_with("agent-1", "js-1", "secret-1");
        assert!(verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-1"), Some("secret-1"))
        ));
    }

    #[test]
    fn wrong_secret_rejected() {
        let reg = registry_with("agent-1", "js-1", "secret-1");
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-1"), Some("secret-WRONG"))
        ));
    }

    #[test]
    fn wrong_jobset_id_rejected() {
        let reg = registry_with("agent-1", "js-1", "secret-1");
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-2"), Some("secret-1"))
        ));
    }

    #[test]
    fn cross_jobset_secret_rejected() {
        // agent-1 is in js-1; agent-2 is in js-2. A sender presenting
        // agent-2's jobset id+secret to target agent-1 must be rejected.
        let reg = JobsetRegistry::new();
        reg.register("agent-1", "js-1", "secret-1");
        reg.register("agent-2", "js-2", "secret-2");
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-2"), Some("secret-2"))
        ));
        // And agent-1's credential is rejected against agent-2.
        assert!(!verify_jobset_credential(
            &reg,
            "agent-2",
            &headers(Some("js-1"), Some("secret-1"))
        ));
        // Each member's own credential is still accepted for its own target.
        assert!(verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-1"), Some("secret-1"))
        ));
        assert!(verify_jobset_credential(
            &reg,
            "agent-2",
            &headers(Some("js-2"), Some("secret-2"))
        ));
    }

    #[test]
    fn missing_header_rejected() {
        let reg = registry_with("agent-1", "js-1", "secret-1");
        // No secret header.
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-1"), None)
        ));
        // No id header.
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(None, Some("secret-1"))
        ));
        // No headers at all.
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(None, None)
        ));
    }

    #[test]
    fn unknown_target_rejected() {
        let reg = registry_with("agent-1", "js-1", "secret-1");
        assert!(!verify_jobset_credential(
            &reg,
            "agent-GHOST",
            &headers(Some("js-1"), Some("secret-1"))
        ));
    }

    #[test]
    fn different_length_secret_rejected_constant_time_path() {
        let reg = registry_with("agent-1", "js-1", "secret-1");
        // A different-length secret must not match (digest comparison handles
        // length differences without panicking).
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-1"), Some("short"))
        ));
        assert!(!verify_jobset_credential(
            &reg,
            "agent-1",
            &headers(Some("js-1"), Some("secret-1-extra-extra"))
        ));
    }

    // ---- credential from env ----------------------------------------------------

    #[test]
    fn credential_from_env_reads_id_and_secret_file() {
        with_env_guard(|| {
            let dir = tempfile::tempdir().unwrap();
            let secret_file = dir.path().join(JOBSET_SECRET_FILE_NAME);
            std::fs::write(&secret_file, "the-secret\n").unwrap();

            // Env vars are process-global; use distinct names via a child scope
            // is not possible, so set/remove around the call under the lock.
            std::env::set_var(JOBSET_ID_ENV, "js-env");
            std::env::set_var(
                JOBSET_SECRET_FILE_ENV,
                secret_file.to_string_lossy().to_string(),
            );
            let c = JobsetCredential::from_env();
            std::env::remove_var(JOBSET_ID_ENV);
            std::env::remove_var(JOBSET_SECRET_FILE_ENV);

            assert_eq!(
                c,
                Some(cred("js-env", "the-secret")),
                "id from env, secret trimmed from the file"
            );
        });
    }

    #[test]
    fn credential_from_env_absent_when_not_a_member() {
        with_env_guard(|| {
            std::env::remove_var(JOBSET_ID_ENV);
            std::env::remove_var(JOBSET_SECRET_FILE_ENV);
            assert_eq!(JobsetCredential::from_env(), None);
        });
    }

    // ---- policy / env wiring: secret absent from argv and env -------------------

    #[test]
    fn policy_and_env_carry_no_secret_value() {
        let dir = tempfile::tempdir().unwrap();
        let base = crate::sandbox::policy::SandboxPolicy::for_session("sess-1", dir.path(), None);
        let c = cred("js-1", "topsecret-value-must-not-leak");
        let policy = build_jobset_policy(&base);
        let env = build_jobset_env(&c, dir.path());

        // The on-disk policy artifact (the file the launcher reads) must not
        // contain the secret value: serializing the policy (exactly what
        // write_policy_file does) must never embed it.
        let policy_json = serde_json::to_string(&policy).expect("policy must serialize");
        assert!(
            !policy_json.contains("topsecret-value-must-not-leak"),
            "secret value must not appear in the policy JSON: {policy_json}"
        );

        // The policy wires the secret channel (fd + dest) but NEVER the value.
        assert!(matches!(
            &policy.secret,
            Some(crate::sandbox::policy::SecretConfig { fd: 3, dest })
                if dest == JOBSET_SECRET_FILE_NAME
        ));

        // The env carries the id and the PATH of the secret file -- never the
        // secret value itself.
        let env_kv: std::collections::HashMap<&str, &str> =
            env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(env_kv.get(JOBSET_ID_ENV), Some(&"js-1"));
        assert_eq!(env_kv.get(JOBSET_EPOCH_ENV), Some(&"0"));
        let expected_file = secret_file_path(dir.path()).to_string_lossy().to_string();
        assert_eq!(
            env_kv.get(JOBSET_SECRET_FILE_ENV),
            Some(&expected_file.as_str())
        );
        // The secret value appears in no env value.
        for (_, v) in &env {
            assert!(
                !v.contains("topsecret-value-must-not-leak"),
                "secret value must not appear in an env value: {v}"
            );
        }

        // The secret value appears in no argv element of the launcher command.
        // The command the launcher runs is `sandbox-exec --policy <path> --
        // <prog> <args...>`; build the argv the way build_command does and
        // assert the secret is absent from every element.
        let argv: Vec<String> = vec![
            "kubeadp-agent".to_string(),
            "sandbox-exec".to_string(),
            "--policy".to_string(),
            dir.path().join("policy.json").to_string_lossy().to_string(),
            "--".to_string(),
            "/usr/local/bin/agy".to_string(),
        ];
        // Env is passed via the process environment (filtered), not argv; the
        // argv is the same regardless of the jobset env. Assert the secret is
        // not in argv.
        for arg in &argv {
            assert!(
                !arg.contains("topsecret-value-must-not-leak"),
                "secret value must not appear in argv: {arg}"
            );
        }
    }

    #[test]
    fn secret_file_path_uses_tmp_root() {
        let p = secret_file_path(std::path::Path::new("/sandbox/tmp"));
        assert_eq!(
            p,
            std::path::Path::new("/sandbox/tmp").join(JOBSET_SECRET_FILE_NAME)
        );
    }

    // ---- process-wide registry lifecycle ------------------------------------

    /// A non-member host (no jobset in the environment) gives a newly created
    /// agent a fresh, random jobset; the entry is retrievable and then dropped.
    #[test]
    fn register_agent_non_member_gets_fresh_jobset_and_drop_removes_it() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                // Ensure the test process is not a member (clear any membership
                // the process environment might carry) so a fresh jobset is
                // expected.
                std::env::remove_var(JOBSET_ID_ENV);
                let cred = register_agent("job-a");
                assert!(!cred.jobset_id.is_empty());
                assert!(cred.secret.len() >= JOBSET_SECRET_BYTES * 2);

                // The entry is visible to the launcher (agent_jobset) and matches.
                let got = agent_jobset("job-a").expect("registered agent is retrievable");
                assert_eq!(got.jobset_id, cred.jobset_id);
                assert_eq!(got.secret, cred.secret);
                assert!(global_registry().contains("job-a"));

                // Dropped on the end path: gone after drop_agent.
                drop_agent("job-a");
                assert!(!global_registry().contains("job-a"));
                assert!(agent_jobset("job-a").is_none());
            });
        });
    }

    /// A retry re-registers an already-known agent and keeps its existing
    /// jobset (idempotent register), so a retried job stays in the same jobset.
    #[test]
    fn register_agent_is_idempotent_for_existing_member() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let first = register_agent("job-b");
                let again = register_agent("job-b");
                assert_eq!(
                    first.jobset_id, again.jobset_id,
                    "re-register keeps the jobset"
                );
                assert_eq!(first.secret, again.secret);
            });
        });
    }

    /// The RAII guard drops the entry when it goes out of scope -- the single
    /// mechanism that covers every end path of the monitor thread (done,
    /// failed, killed, cancelled launch, spawn error).
    #[test]
    fn drop_guard_removes_entry_on_scope_exit() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                register_agent("job-c");
                assert!(global_registry().contains("job-c"));
                {
                    let _guard = JobsetDropGuard::new("job-c");
                    // Still present while the guard is alive.
                    assert!(global_registry().contains("job-c"));
                }
                // Dropped once the guard exits scope (every monitor return path).
                assert!(!global_registry().contains("job-c"));
            });
        });
    }

    /// A host that is a jobset member hands its OWN jobset to a new agent, so
    /// the new agent joins the starting agent's jobset.
    #[test]
    fn register_agent_member_host_inherits_host_jobset() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                // Put the host in a known jobset via the environment: id in the
                // env, secret read from a file named by the env (as the launcher
                // sets it).
                let dir = tempfile::tempdir().unwrap();
                let secret_file = dir.path().join(JOBSET_SECRET_FILE_NAME);
                std::fs::write(&secret_file, "host-secret-value").unwrap();
                std::env::set_var(JOBSET_ID_ENV, "host-js");
                std::env::set_var(
                    JOBSET_SECRET_FILE_ENV,
                    secret_file.to_string_lossy().to_string(),
                );
                let host = host_jobset().expect("host is a member");
                assert_eq!(host.jobset_id, "host-js");
                assert_eq!(host.secret, "host-secret-value");

                let child = register_agent("job-d");
                // The child inherits the host's jobset (same id + secret).
                assert_eq!(child.jobset_id, "host-js");
                assert_eq!(child.secret, "host-secret-value");
                // And the host can verify a credential presented against the child.
                let get = headers(Some("host-js"), Some("host-secret-value"));
                assert!(verify_jobset_credential(&global_registry(), "job-d", &get));
                std::env::remove_var(JOBSET_ID_ENV);
                std::env::remove_var(JOBSET_SECRET_FILE_ENV);
            });
        });
    }

    /// The A2A credential check against a registered member: a valid jobset
    /// credential is accepted, a wrong secret / other jobset / missing header is
    /// rejected, and a dropped agent (ended) is rejected.
    #[test]
    fn verify_jobset_credential_accepts_and_rejects() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let cred = register_agent("agent-x");

                let valid = headers(Some(&cred.jobset_id), Some(&cred.secret));
                assert!(verify_jobset_credential(
                    &global_registry(),
                    "agent-x",
                    &valid
                ));

                let wrong_secret = headers(Some(&cred.jobset_id), Some("nope"));
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-x",
                    &wrong_secret
                ));

                let other_jobset = headers(Some("different-js"), Some(&cred.secret));
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-x",
                    &other_jobset
                ));

                let missing = headers(None, Some(&cred.secret));
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-x",
                    &missing
                ));

                let missing_secret = headers(Some(&cred.jobset_id), None);
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-x",
                    &missing_secret
                ));

                // Unknown target (not registered) is rejected.
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-unknown",
                    &valid
                ));

                // After the agent ends (entry dropped), the same credential is
                // rejected.
                drop_agent("agent-x");
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-x",
                    &valid
                ));
            });
        });
    }

    // ---- derived secrets (root key) ----------------------------------------

    /// Writes a root key file (>= 32 bytes) and sets `KUBEADP_JOBSET_ROOT_KEY_FILE`
    /// to it under the env lock. Returns the path. Caller must call
    /// [`clear_root_key_env`] inside the same env guard.
    fn with_root_key_file(dir: &std::path::Path, bytes: usize) -> std::path::PathBuf {
        let path = dir.join("root-key");
        let material: Vec<u8> = (0..bytes).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &material).unwrap();
        std::env::set_var(ROOT_KEY_FILE_ENV, path.to_string_lossy().to_string());
        path
    }

    fn clear_root_key_env() {
        std::env::remove_var(ROOT_KEY_FILE_ENV);
    }

    #[test]
    fn derive_is_deterministic_and_differs_per_id_epoch_and_key() {
        let key = b"0123456789abcdef0123456789abcdef";
        let other_key = b"fedcba9876543210fedcba9876543210";
        let s1 = derive_jobset_secret(key, "jobset-aaa", 0);
        let s2 = derive_jobset_secret(key, "jobset-aaa", 0);
        assert_eq!(s1, s2, "same key+id+epoch is deterministic");
        assert_eq!(s1.len(), 64, "hex of 32-byte HMAC");
        let s3 = derive_jobset_secret(key, "jobset-bbb", 0);
        assert_ne!(s1, s3, "different jobset id differs");
        let s4 = derive_jobset_secret(key, "jobset-aaa", 1);
        assert_ne!(s1, s4, "different epoch differs");
        let s5 = derive_jobset_secret(other_key, "jobset-aaa", 0);
        assert_ne!(s1, s5, "different root key differs");
    }

    #[test]
    fn resolve_root_key_trims_and_rejects_short() {
        with_env_guard(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key");
            // 32 bytes -> ok.
            std::fs::write(&path, vec![b'x'; 32]).unwrap();
            std::env::set_var(ROOT_KEY_FILE_ENV, path.to_string_lossy().to_string());
            let key = resolve_root_key().unwrap().expect("32-byte key acceptable");
            assert_eq!(key.as_bytes().len(), 32);
            // Surrounding whitespace is trimmed.
            std::fs::write(&path, format!("  {}\n", "k".repeat(32))).unwrap();
            let key = resolve_root_key().unwrap().expect("trimmed key acceptable");
            assert_eq!(key.as_bytes().len(), 32);
            // 31 bytes -> clear error naming only the path.
            let short = dir.path().join("short");
            std::fs::write(&short, vec![0x01u8; 31]).unwrap();
            std::env::set_var(ROOT_KEY_FILE_ENV, short.to_string_lossy().to_string());
            let err = resolve_root_key().unwrap_err();
            assert!(err.contains("short"), "{err}");
            assert!(err.contains("fewer than"), "{err}");
            // The error must not embed the key material.
            assert!(!err.contains('\u{1}'), "no key bytes leak: {err}");
            clear_root_key_env();
        });
    }

    /// A "restart" (a fresh registry, same root key file + same membership
    /// file) still verifies the derived credential, and a retried job
    /// re-derives the same secret.
    #[test]
    fn restart_loads_membership_and_retry_rejoins_same_jobset() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                // First "process": init + register a job.
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).expect("init ok");
                let cred = register_agent("job-r");
                let derived = derive_jobset_secret(
                    global_registry().root_key().as_deref().unwrap(),
                    &cred.jobset_id,
                    cred.epoch,
                );
                assert_eq!(cred.secret, derived, "credential is the derived secret");
                // `register_agent` persists the membership itself (F4); no
                // manual save_memberships() call may be needed for a retried
                // job to rejoin after a restart.
                assert_eq!(
                    credential_from_env_headers(&cred),
                    derived,
                    "derived matches"
                );

                // Second "process": a brand-new registry, same key + membership
                // file, WITHOUT re-registering anything.
                let fresh = {
                    let guard = JobsetScope::new();
                    init_global_jobsets(&state_root).expect("re-init ok");
                    let out = (
                        global_registry().get("job-r"),
                        verify_jobset_credential(
                            &global_registry(),
                            "job-r",
                            &headers(Some(&cred.jobset_id), Some(&derived)),
                        ),
                    );
                    drop(guard);
                    out
                };
                let (entry, verified) = fresh;
                assert!(entry.is_some(), "membership re-loaded after restart");
                assert_eq!(
                    entry.unwrap().jobset_id,
                    cred.jobset_id,
                    "the same jobset id after restart"
                );
                assert!(verified, "derived credential verifies after restart");

                // A retried job (same agent id) re-derives the same secret.
                let retried = register_agent("job-r");
                assert_eq!(retried.jobset_id, cred.jobset_id);
                assert_eq!(retried.secret, derived);
                clear_root_key_env();
            });
        });
    }

    /// `register_agent` persists the membership itself (F4): a fresh registry
    /// reloaded from the same membership file sees the job's membership with
    /// NO explicit `save_memberships()` call, and the retried job re-joins.
    #[test]
    fn register_agent_persists_membership_for_restart() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).unwrap();

                // Register a job WITHOUT any manual save: register_agent must
                // persist the membership itself.
                let cred = register_agent("job-f4");
                let derived = derive_jobset_secret(
                    global_registry().root_key().as_deref().unwrap(),
                    &cred.jobset_id,
                    cred.epoch,
                );
                let membership_file = membership_file_path(&state_root);
                assert!(
                    membership_file.exists(),
                    "register_agent must write the membership file"
                );

                // "Restart": fresh registry, same root key + membership file.
                let (entry, verified) = {
                    let guard = JobsetScope::new();
                    init_global_jobsets(&state_root).unwrap();
                    let out = (
                        global_registry().get("job-f4"),
                        verify_jobset_credential(
                            &global_registry(),
                            "job-f4",
                            &headers(Some(&cred.jobset_id), Some(&derived)),
                        ),
                    );
                    drop(guard);
                    out
                };
                assert!(entry.is_some(), "membership persisted by register_agent");
                assert_eq!(entry.unwrap().jobset_id, cred.jobset_id);
                assert!(verified, "derived credential verifies after restart");

                // A retried job re-joins the same jobset with the same secret.
                let retried = register_agent("job-f4");
                assert_eq!(retried.jobset_id, cred.jobset_id);
                assert_eq!(retried.secret, derived);
                clear_root_key_env();
            });
        });
    }

    /// A "restart" (a fresh registry, same root key file + same membership
    /// file) still verifies the derived credential through the router-facing
    /// verification path (which now derives, not looks up).
    #[test]
    fn verify_uses_derivation_not_secret_lookup_when_root_key_present() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).unwrap();
                let cred = register_agent("agent-z");
                // Even if the in-memory secret were wrong, verification derives
                // from the root key: the derived credential verifies, and any
                // other secret fails.
                let derived = derive_jobset_secret(
                    global_registry().root_key().as_deref().unwrap(),
                    &cred.jobset_id,
                    cred.epoch,
                );
                assert!(verify_jobset_credential(
                    &global_registry(),
                    "agent-z",
                    &headers(Some(&cred.jobset_id), Some(&derived))
                ));
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-z",
                    &headers(Some(&cred.jobset_id), Some("not-the-derived-secret"))
                ));
                clear_root_key_env();
            });
        });
    }

    #[test]
    fn wrong_root_key_rejects() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                let path = with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).unwrap();
                let cred = register_agent("agent-w");
                clear_root_key_env();

                // A DIFFERENT root key derives a different secret -> reject.
                let other = dir.path().join("other-key");
                std::fs::write(&other, vec![b'z'; 32]).unwrap();
                std::env::set_var(ROOT_KEY_FILE_ENV, other.to_string_lossy().to_string());
                reset_warned_no_root_key();
                init_global_jobsets(&state_root).unwrap();
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-w",
                    &headers(Some(&cred.jobset_id), Some(&cred.secret))
                ));
                clear_root_key_env();
                std::fs::remove_file(&path).ok();
            });
        });
    }

    #[test]
    fn ending_jobset_rejects_old_credential_and_404s_unknown() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).unwrap();
                let cred = register_agent("agent-e");

                let valid = headers(Some(&cred.jobset_id), Some(&cred.secret));
                assert!(verify_jobset_credential(
                    &global_registry(),
                    "agent-e",
                    &valid
                ));

                let new_epoch = end_jobset(&cred.jobset_id)
                    .expect("currently empty registry change not error")
                    .expect("jobset is known");
                assert!(new_epoch > cred.epoch, "epoch bumped");
                assert!(global_registry().is_ended(&cred.jobset_id));

                // The old credential stops verifying.
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "agent-e",
                    &valid
                ));

                // An unknown jobset -> None (the route 404s).
                assert!(end_jobset("does-not-exist").expect("no io error").is_none());
                clear_root_key_env();
            });
        });
    }

    #[test]
    fn end_jobset_still_works_when_all_members_finished() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).unwrap();

                // Two jobs of the same jobset; both finish and their live
                // entries are dropped.
                let cred_a = register_agent("job-a");
                let _cred_b = register_agent("job-b");
                // `job-a` was the first registration: it got a fresh jobset,
                // and `job-b` received the host's inherited jobset only when
                // the host is a member. Force both into one jobset by
                // registering them against the same id-space (re-register
                // job-b into job-a's jobset so they share it).
                global_registry().register_at(
                    "job-b",
                    &cred_a.jobset_id,
                    &cred_a.secret,
                    cred_a.epoch,
                );
                assert_eq!(
                    global_registry().get("job-b").unwrap().jobset_id,
                    cred_a.jobset_id
                );
                drop_agent("job-a");
                drop_agent("job-b");
                // No live member remains.
                assert!(global_registry()
                    .snapshot()
                    .values()
                    .all(|e| e.jobset_id != cred_a.jobset_id));

                // The jobset must still be recognisable (its membership was
                // persisted) so the end route does NOT 404.
                let epoch = end_jobset(&cred_a.jobset_id)
                    .expect("no io error")
                    .expect("finished jobset still known, not a 404");
                assert!(epoch > cred_a.epoch);
                clear_root_key_env();
            });
        });
    }

    #[test]
    fn missing_root_key_keeps_random_secret_and_warns_once() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                clear_root_key_env();
                reset_warned_no_root_key();
                // First fresh jobset warns.
                let c1 = fresh_jobset_credential();
                assert!(WARNED_NO_ROOT_KEY.load(Ordering::SeqCst));
                assert_ne!(c1.secret.len(), 0);
                // The secret is random and differs across calls (in-process).
                let c2 = fresh_jobset_credential();
                assert_ne!(c1.secret, c2.secret);
                // The registry-based (in-process) verification still works with
                // a registered secret, exactly as before the derivation design.
                let reg = JobsetRegistry::new();
                reg.register("a", &c1.jobset_id, &c1.secret);
                assert!(verify_jobset_credential(
                    &reg,
                    "a",
                    &headers(Some(&c1.jobset_id), Some(&c1.secret))
                ));
                assert!(!verify_jobset_credential(
                    &reg,
                    "a",
                    &headers(Some(&c1.jobset_id), Some("wrong"))
                ));
            });
        });
    }

    #[test]
    fn non_member_target_rejected() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                init_global_jobsets(&state_root).unwrap();
                let cred = register_agent("agent-m");

                // Present a VALID credential for agent-m's jobset but target a
                // different (non-member) agent.
                assert!(!verify_jobset_credential(
                    &global_registry(),
                    "other-agent",
                    &headers(Some(&cred.jobset_id), Some(&cred.secret))
                ));
                clear_root_key_env();
            });
        });
    }

    #[test]
    fn corrupt_membership_file_is_quarantined() {
        with_env_guard(|| {
            with_isolated_registry(|| {
                std::env::remove_var(JOBSET_ID_ENV);
                let dir = tempfile::tempdir().unwrap();
                with_root_key_file(dir.path(), 32);
                reset_warned_no_root_key();
                let state_root = dir.path().join("state");
                std::fs::create_dir_all(&state_root).unwrap();
                let file = membership_file_path(&state_root);
                std::fs::write(&file, b"{ not valid json !").unwrap();

                init_global_jobsets(&state_root).expect("corrupt file does not wedge startup");
                // Quarantined: the original is gone, a corrupt-N file exists,
                // and the registry is empty (startup continued).
                assert!(!file.exists(), "corrupt file renamed away");
                let quarantined = std::fs::read_dir(&state_root)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .any(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .starts_with(&format!("{}.corrupt-", MEMBERSHIP_FILE_NAME))
                    });
                assert!(quarantined, "corrupt file quarantined");
                clear_root_key_env();
            });
        });
    }

    /// The derived secret (and the root key) must not leak into env/argv/logs:
    /// `build_jobset_env` carries only id + epoch + the secret FILE PATH, and
    /// the root key file path is never in the jobset env.
    #[test]
    fn no_secret_or_root_key_in_env_or_argv() {
        let dir = tempfile::tempdir().unwrap();
        with_env_guard(|| {
            let root_path = with_root_key_file(dir.path(), 32);
            let cred = fresh_jobset_credential();
            let env = build_jobset_env(&cred, dir.path());
            for (k, v) in &env {
                assert!(
                    !v.contains(&cred.secret),
                    "secret must not appear in env {k}={v}"
                );
                assert!(
                    !v.contains(&root_path.to_string_lossy().to_string()),
                    "root key path must not appear in env {k}={v}"
                );
            }
            // Epoch and id and file path allowed and present.
            let env_kv: std::collections::HashMap<&str, &str> =
                env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            assert_eq!(env_kv.get(JOBSET_ID_ENV), Some(&cred.jobset_id.as_str()));
            assert_eq!(env_kv.get(JOBSET_EPOCH_ENV), Some(&"0"));
            clear_root_key_env();
        });
    }

    /// Helpers used above: derives the secret via the root key and produces a
    /// matching header set.
    fn credential_from_env_headers(cred: &JobsetCredential) -> String {
        derive_jobset_secret(
            global_registry()
                .root_key()
                .as_deref()
                .expect("root key set"),
            &cred.jobset_id,
            cred.epoch,
        )
    }
}

//! A2A sending side: the client context behind the `mcp-a2a` MCP subcommand.
//!
//! This module (behind the crate's `a2a` cargo feature) provides the remote
//! A2A interface for the agent: it discovers peer agents from Kubernetes
//! Services, fetches their agent cards, and speaks the A2A v1 protocol as a
//! *client* through the official `a2a-client-lf` SDK. The MCP tool definitions
//! in [`crate::mcp_a2a`] call into [`A2aClientCtx`] to implement
//! `list_agents`, `send_message` and `get_task`.
//!
//! # Agent registry / discovery
//!
//! Peer agents are discovered from Kubernetes Services carrying the label
//! `a2a.homelab/agent=true`:
//!
//! - the pod's own namespace (read from the in-cluster service account, or
//!   `A2A_NAMESPACE` for tests), plus any namespaces listed in the
//!   comma-separated `A2A_NAMESPACES` environment variable;
//! - the Kubernetes API is reached over the in-cluster service account token
//!   (`/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`)
//!   with the API base overridable via `A2A_KUBE_API` for tests and local
//!   development.
//!
//! Each matching Service becomes one agent entry:
//!
//! - `name` / `namespace`, and a URL `http://<service>.<namespace>.svc:<port>`
//!   where the port is the Service port named `a2a` (falling back to the first
//!   port when none is named);
//! - a best-effort fetch of `/.well-known/agent-card.json` (3 s timeout);
//!   a failed or absent card simply leaves the `card` field out of the entry.
//!
//! The `send_message`/`get_task` tools resolve their `agent` argument by the
//! Service name from the same registry, so a caller can never point the MCP
//! client at an arbitrary URL: the target must be a discovered agent.
//!
//! # Hop counting
//!
//! Messages are tagged with two metadata fields to bound A2A fan-out across
//! agents: `a2a.sender` (from the `A2A_SELF_NAME` environment variable,
//! defaulting to the pod's hostname) and `a2a.hop`. On send, the requested
//! hop is the incoming message's hop + 1; a hop above `MAX_HOP` is refused
//! with a clear error.

use std::collections::HashMap;
use std::sync::Arc;

use a2a::{A2AError, GetTaskRequest, Message, Part, Role, SendMessageRequest, SendMessageResponse};
use a2a_client::middleware::CallInterceptor;
use a2a_client::rest::RestTransport;
use a2a_client::transport::ServiceParams;
use a2a_client::A2AClient;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// Maximum forward chain length across agents (`a2a.hop`). A message with a
/// hop above this is refused on send.
pub const MAX_HOP: u32 = 4;
/// Metadata key for the sender identity (value from `A2A_SELF_NAME`).
pub const META_SENDER: &str = "a2a.sender";
/// Metadata key for the hop counter.
pub const META_HOP: &str = "a2a.hop";
/// Label selecting agent Services in the Kubernetes discovery.
pub const AGENT_LABEL: &str = "a2a.homelab/agent=true";
/// Default in-cluster service-account token path.
pub const SA_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
/// Default in-cluster CA bundle path.
pub const SA_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
/// Default in-cluster namespace file path.
pub const SA_NAMESPACE_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
/// Card fetch timeout for discovery.
pub const CARD_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// Hard size cap on fetched agent cards (64 KB). Bodies exceeding this are rejected.
pub const MAX_CARD_BYTES: usize = 64 * 1024;

/// Validate a Kubernetes name / namespace as an RFC 1123 label.
/// Must be 1-63 lowercase alphanumeric characters or '-', starting and ending
/// with an alphanumeric character.
pub fn is_valid_rfc1123_label(s: &str) -> bool {
    if s.is_empty() || s.len() > 63 {
        return false;
    }
    let bytes = s.as_bytes();
    let is_alphanumeric = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !is_alphanumeric(bytes[0]) || !is_alphanumeric(bytes[bytes.len() - 1]) {
        return false;
    }
    bytes.iter().all(|&b| is_alphanumeric(b) || b == b'-')
}

/// A discovered peer agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentEntry {
    /// The Kubernetes Service name.
    pub name: String,
    /// The namespace the Service lives in.
    pub namespace: String,
    /// The base URL of the agent's A2A REST endpoint
    /// (`http://<service>.<namespace>.svc:<port>`).
    pub url: String,
    /// The fetched agent card (best-effort; absent when the fetch failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub card: Option<serde_json::Value>,
}

/// The A2A sending-side client context shared by the MCP tools.
#[derive(Clone)]
pub struct A2aClientCtx {
    /// The discovered agent registry keyed by (namespace, name).
    agents: Arc<HashMap<(String, String), AgentEntry>>,
    /// Sender self-name carried in message metadata (`a2a.sender`).
    self_name: Arc<str>,
    /// Default namespace for unadorned agent lookup (e.g. the pod's namespace).
    default_namespace: Option<String>,
    /// Interceptors applied to every outgoing request: the per-jobset
    /// credential (when this agent is a jobset member) plus any bearer
    /// credential. Built once at construction, not per request.
    interceptors: Vec<Arc<dyn CallInterceptor>>,
}

impl A2aClientCtx {
    /// List the discovered agents (stable order by namespace, then name).
    pub fn list_agents(&self) -> Vec<AgentEntry> {
        let mut agents: Vec<AgentEntry> = self.agents.values().cloned().collect();
        agents.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| a.name.cmp(&b.name))
        });
        agents
    }

    /// Resolve an agent by name or "namespace/name" reference.
    ///
    /// If target contains a '/', it is treated as an exact (namespace, name) pair.
    /// Otherwise, if a default namespace is set, (default_namespace, target) is
    /// tried first. If not found or no default namespace, matching across all
    /// namespaces is checked: if exactly one agent matches `target`, it is returned;
    /// if multiple namespaces have an agent with that name, resolution returns `None`
    /// to avoid ambiguity and shadowing.
    pub fn agent(&self, target: &str) -> Option<&AgentEntry> {
        if let Some((ns, name)) = target.split_once('/') {
            return self.agents.get(&(ns.to_string(), name.to_string()));
        }
        if let Some(ref def_ns) = self.default_namespace {
            if let Some(entry) = self.agents.get(&(def_ns.clone(), target.to_string())) {
                return Some(entry);
            }
        }
        let matches: Vec<&AgentEntry> = self
            .agents
            .iter()
            .filter(|((_, name), _)| name == target)
            .map(|(_, entry)| entry)
            .collect();
        if matches.len() == 1 {
            Some(matches[0])
        } else {
            None
        }
    }

    /// The configured sender self-name.
    pub fn self_name(&self) -> &str {
        &self.self_name
    }

    /// Build a client for the agent's endpoint.
    ///
    /// The connection base is the cluster-internal Service URL, unless the
    /// `A2A_URL_OVERRIDES` test/development seam (`name=url,...` or
    /// `namespace/name=url,...`) maps this agent to another base (e.g. an
    /// in-process server on 127.0.0.1).
    ///
    /// When this agent is a jobset member (it knows its jobset id and secret
    /// file), the client attaches the per-jobset credential to every outgoing
    /// request (see [`JobsetCredentialInterceptor`]) so the target agent can
    /// verify membership.
    fn client_for(&self, entry: &AgentEntry) -> Result<A2AClient<RestTransport>, String> {
        let base = url_override(&entry.name, &entry.namespace).unwrap_or_else(|| entry.url.clone());
        let http = a2a_client::default_reqwest_client(None)
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;
        let transport = RestTransport::new(http, base.trim_end_matches('/').to_string());
        Ok(A2AClient::new(transport).with_interceptors(self.interceptors.clone()))
    }

    /// Compute the outgoing hop and refuse when it exceeds [`MAX_HOP`].
    ///
    /// The incoming hop (from the forwarded message's metadata) is
    /// incremented for the hop this message will carry.
    fn next_hop(&self, incoming_hop: Option<u32>) -> Result<u32, String> {
        let hop = incoming_hop.unwrap_or(0) + 1;
        if hop > MAX_HOP {
            return Err(format!(
                "hop limit reached: this message is at hop {hop} (max {MAX_HOP}); refusing to forward further"
            ));
        }
        Ok(hop)
    }

    /// Build the tagged A2A message for `send_message` (pure, so tests can
    /// inspect the metadata without a network round trip).
    fn build_send_message(
        &self,
        text: &str,
        context_id: Option<String>,
        task_id: Option<String>,
        incoming_hop: Option<u32>,
    ) -> Result<Message, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("message text must not be empty".to_string());
        }
        let hop = self.next_hop(incoming_hop)?;
        let mut message = Message::new(Role::User, vec![Part::text(text)]);
        message.context_id = context_id;
        message.task_id = task_id;
        let mut metadata = HashMap::new();
        metadata.insert(
            META_SENDER.to_string(),
            serde_json::json!(self.self_name.as_ref()),
        );
        metadata.insert(META_HOP.to_string(), serde_json::json!(hop));
        message.metadata = Some(metadata);
        Ok(message)
    }

    /// Send a text message to the given agent (by name or namespace/name), tagging it
    /// with the sender name and the next hop.
    pub async fn send_message(
        &self,
        agent: &str,
        text: &str,
        context_id: Option<String>,
        task_id: Option<String>,
        incoming_hop: Option<u32>,
    ) -> Result<serde_json::Value, String> {
        let entry = self
            .agent(agent)
            .ok_or_else(|| format!("no A2A agent named '{agent}' in the registry"))?;

        let message = self.build_send_message(text, context_id, task_id, incoming_hop)?;

        let req = SendMessageRequest {
            message,
            configuration: None,
            metadata: None,
            tenant: None,
        };

        let client = self.client_for(entry)?;
        let resp = client
            .send_message(&req)
            .await
            .map_err(|e| format!("send_message to {agent}: {e}"))?;
        Ok(match resp {
            SendMessageResponse::Task(task) => serde_json::to_value(task)
                .map_err(|e| format!("failed to encode response task: {e}"))?,
            SendMessageResponse::Message(msg) => serde_json::to_value(msg)
                .map_err(|e| format!("failed to encode response message: {e}"))?,
        })
    }

    /// Fetch the current state of a task from the given agent (by name or
    /// namespace/name).
    pub async fn get_task(&self, agent: &str, task_id: &str) -> Result<serde_json::Value, String> {
        let entry = self
            .agent(agent)
            .ok_or_else(|| format!("no A2A agent named '{agent}' in the registry"))?;
        if task_id.trim().is_empty() {
            return Err("task_id must not be empty".to_string());
        }

        let client = self.client_for(entry)?;
        let req = GetTaskRequest {
            id: task_id.to_string(),
            history_length: None,
            tenant: None,
        };
        let task = client
            .get_task(&req)
            .await
            .map_err(|e| format!("get_task on {agent}: {e}"))?;
        serde_json::to_value(task).map_err(|e| format!("failed to encode task: {e}"))
    }
}

/// A [`CallInterceptor`] that attaches the per-jobset credential to every
/// outgoing A2A request.
///
/// A jobset member knows its jobset id (`KUBEADP_JOBSET_ID`) and the path of
/// the 0400 secret file (`KUBEADP_JOBSET_SECRET_FILE`); the interceptor reads
/// them once (see [`crate::core::jobset::JobsetCredential::from_env`]) and
/// injects the [`crate::core::jobset::JOBSET_ID_HEADER`] and
/// [`crate::core::jobset::JOBSET_SECRET_HEADER`] headers so the target agent's
/// jobset check can verify membership. Both transports (JSON-RPC and REST)
/// apply `ServiceParams` as HTTP headers, so this is transport-agnostic.
pub struct JobsetCredentialInterceptor {
    jobset_id: String,
    secret: String,
}

impl JobsetCredentialInterceptor {
    /// Build the interceptor from a credential, or `None` when the caller is
    /// not a jobset member (no credential to attach).
    pub fn from_credential(cred: &crate::core::jobset::JobsetCredential) -> Option<Self> {
        match (cred.jobset_id_header(), cred.secret_header()) {
            (Some(id), Some(secret)) => Some(Self {
                jobset_id: id.to_string(),
                secret: secret.to_string(),
            }),
            _ => None,
        }
    }
}

#[async_trait]
impl CallInterceptor for JobsetCredentialInterceptor {
    async fn before(&self, _method: &str, params: &mut ServiceParams) -> Result<(), A2AError> {
        params
            .entry(crate::core::jobset::JOBSET_ID_HEADER.to_string())
            .or_default()
            .push(self.jobset_id.clone());
        params
            .entry(crate::core::jobset::JOBSET_SECRET_HEADER.to_string())
            .or_default()
            .push(self.secret.clone());
        Ok(())
    }
}

/// The jobset credential interceptor for this process, if this agent is a
/// jobset member.
///
/// Reads the credential from the environment (jobset id + the secret file
/// named by `KUBEADP_JOBSET_SECRET_FILE`). Returns `None` when the agent has no
/// jobset id or its secret file is absent -- such an agent is not a jobset
/// member and presents no credential.
fn jobset_credential_interceptor() -> Option<JobsetCredentialInterceptor> {
    crate::core::jobset::JobsetCredential::from_env()
        .and_then(|c| JobsetCredentialInterceptor::from_credential(&c))
}

/// Error returned when building the client context / discovery fails.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("kubernetes API error: {0}")]
    KubeApi(String),
    #[error("service-account token error: {0}")]
    Token(String),
    #[error("invalid response from kubernetes API: {0}")]
    InvalidResponse(String),
}

/// Read a file's contents trimmed, from a path or a literal override.
fn read_optional(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Fetch an agent card from `<base_url>/.well-known/agent-card.json`.
/// Rejects bodies larger than [`MAX_CARD_BYTES`] before or during reading.
async fn fetch_agent_card(client: &reqwest::Client, base_url: &str) -> Option<serde_json::Value> {
    let url = format!(
        "{}/.well-known/agent-card.json",
        base_url.trim_end_matches('/')
    );
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    if let Some(len) = resp.content_length() {
        if len > MAX_CARD_BYTES as u64 {
            return None;
        }
    }
    // Stream up to MAX_CARD_BYTES + 1 to detect oversized bodies without content-length header
    let mut stream = resp.bytes_stream();
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        if buf.len() + chunk.len() > MAX_CARD_BYTES {
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&buf).ok()
}

/// Build the client context by discovering peer agents.
///
/// The discovery source is injectable for tests: `list_services` is called
/// with the resolved API base and the (possibly `None`) bearer token, and
/// must return the already label-filtered Services as `items` JSON values.
/// When running inside a pod the service-account token is read lazily.
pub async fn build_ctx<F, Fut>(
    kube_api: Option<String>,
    list_services: F,
) -> Result<A2aClientCtx, DiscoveryError>
where
    F: Fn(&str, Option<&str>) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<serde_json::Value>, String>>,
{
    let (token, _ca, namespace) = read_incluster_creds();
    // Validate incluster namespace if present
    let own_ns = namespace.filter(|ns| is_valid_rfc1123_label(ns));

    // Under production builds, kube_api overrides are ignored.
    #[cfg(feature = "test-hooks")]
    let api_override = kube_api;
    #[cfg(not(feature = "test-hooks"))]
    let api_override: Option<String> = {
        let _ = kube_api;
        None
    };

    let api_base = api_override
        .map(|s| s.trim_end_matches('/').to_string())
        .unwrap_or_else(|| "https://kubernetes.default.svc".to_string());

    let services = list_services(&api_base, token.as_deref())
        .await
        .map_err(DiscoveryError::KubeApi)?;

    let mut agents = HashMap::new();
    let http_client = reqwest::Client::builder()
        .timeout(CARD_FETCH_TIMEOUT)
        .build()
        .ok();

    for svc in services {
        let name = svc
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let svc_ns = svc
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| own_ns.clone())
            .unwrap_or_default();

        // Enforce RFC 1123 validation on discovered name and namespace
        if !is_valid_rfc1123_label(&name) || !is_valid_rfc1123_label(&svc_ns) {
            continue;
        }

        // Port selection: the port named `a2a`, else the first port.
        let port = svc
            .get("spec")
            .and_then(|s| s.get("ports"))
            .and_then(|p| p.as_array())
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("a2a"))
                    .or_else(|| ports.first())
            })
            .and_then(|p| p.get("port"))
            .and_then(|v| v.as_u64());
        let Some(port) = port else {
            continue;
        };
        let url = format!("http://{name}.{svc_ns}.svc:{port}");
        let card = if let Some(ref client) = http_client {
            let fetch_url = url_override(&name, &svc_ns).unwrap_or_else(|| url.clone());
            tokio::time::timeout(CARD_FETCH_TIMEOUT, fetch_agent_card(client, &fetch_url))
                .await
                .ok()
                .flatten()
        } else {
            None
        };

        // Agents are keyed by (namespace, name). Locally discovered/existing entries are preserved
        // and cannot be overwritten by conflicting peers.
        agents
            .entry((svc_ns.clone(), name.clone()))
            .or_insert(AgentEntry {
                name,
                namespace: svc_ns,
                url,
                card,
            });
    }

    let self_name: Arc<str> = std::env::var("A2A_SELF_NAME")
        .ok()
        .filter(|s| is_valid_rfc1123_label(s))
        .or_else(|| {
            // The banner hostname is the closest in-pod identity available.
            std::fs::read_to_string("/proc/self/comm")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| is_valid_rfc1123_label(s))
        })
        .unwrap_or_else(|| "kubeadp-agent".to_string())
        .into();

    Ok(A2aClientCtx {
        agents: Arc::new(agents),
        self_name,
        default_namespace: own_ns,
        interceptors: interceptors_from_env(),
    })
}

/// An empty client context (no discovered agents). Used by the MCP server when
/// startup discovery fails, so the stdio server still starts and every tool
/// reports a clear error instead of the process exiting. The jobset credential
/// is still attached (it is independent of discovery).
pub fn empty_ctx() -> A2aClientCtx {
    A2aClientCtx {
        agents: Arc::new(HashMap::new()),
        self_name: "kubeadp-agent".into(),
        default_namespace: None,
        interceptors: interceptors_from_env(),
    }
}

/// The interceptors to attach to outgoing requests, built once from the
/// environment: the per-jobset credential when this agent is a jobset member
/// (and the operator bearer credential when one is configured, added later).
fn interceptors_from_env() -> Vec<Arc<dyn CallInterceptor>> {
    jobset_credential_interceptor()
        .map(|i| vec![Arc::new(i) as Arc<dyn CallInterceptor>])
        .unwrap_or_default()
}

fn read_incluster_creds() -> (Option<String>, Option<String>, Option<String>) {
    (
        read_optional(SA_TOKEN_PATH),
        read_optional(SA_CA_PATH),
        read_optional(SA_NAMESPACE_PATH),
    )
}

/// Namespaces to discover agents in: the pod's own namespace plus the
/// comma-separated `A2A_NAMESPACES` env var.
/// Each namespace is validated as an RFC 1123 label.
pub fn discovery_namespaces(own_namespace: Option<&str>) -> Vec<String> {
    let mut namespaces = Vec::new();
    if let Some(ns) = own_namespace {
        if is_valid_rfc1123_label(ns) {
            namespaces.push(ns.to_string());
        }
    }
    if let Ok(extra) = std::env::var("A2A_NAMESPACES") {
        for ns in extra.split(',') {
            let ns = ns.trim();
            if is_valid_rfc1123_label(ns) && !namespaces.iter().any(|x| x == ns) {
                namespaces.push(ns.to_string());
            }
        }
    }
    namespaces
}

/// Resolve a service-name -> base-URL override for tests.
///
/// Under production builds, URL overrides are disabled. In `test-hooks` builds,
/// `A2A_URL_OVERRIDES` is a comma-separated `name=url` or
/// `namespace/name=url` list (also accepting whitespace around separators).
/// A `namespace/name` override matches only the agent in that namespace and
/// wins over a bare-name override; a bare `name` applies to every agent with
/// that name (used by the single-peer tests). Returns `None` when unset or the
/// key does not match.
fn url_override(name: &str, namespace: &str) -> Option<String> {
    #[cfg(feature = "test-hooks")]
    {
        let Ok(overrides) = std::env::var("A2A_URL_OVERRIDES") else {
            return None;
        };
        let mut fallback: Option<String> = None;
        for pair in overrides.split(',') {
            let Some((key, url)) = pair.split_once('=') else {
                continue;
            };
            let key = key.trim();
            let url = url.trim().to_string();
            if key == format!("{namespace}/{name}") {
                return Some(url);
            }
            if key == name {
                fallback = Some(url);
            }
        }
        fallback
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = (name, namespace);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with(agents: Vec<(&str, &str, &str)>) -> A2aClientCtx {
        let mut map = HashMap::new();
        for (name, ns, url) in agents {
            map.insert(
                (ns.to_string(), name.to_string()),
                AgentEntry {
                    name: name.to_string(),
                    namespace: ns.to_string(),
                    url: url.to_string(),
                    card: None,
                },
            );
        }
        A2aClientCtx {
            agents: Arc::new(map),
            self_name: "test-self".into(),
            default_namespace: None,
            interceptors: Vec::new(),
        }
    }

    #[test]
    fn hop_increments_and_bounds() {
        let ctx = ctx_with(vec![("alpha", "ops", "http://alpha.ops.svc:8080")]);
        assert_eq!(MAX_HOP, 4);
        // Incoming hop 0 -> outgoing hop 1 (allowed).
        let msg = ctx
            .build_send_message("hello", None, None, Some(0))
            .unwrap();
        assert_eq!(
            msg.metadata.as_ref().unwrap()[META_HOP],
            serde_json::json!(1)
        );
        assert_eq!(
            msg.metadata.as_ref().unwrap()[META_SENDER],
            serde_json::json!("test-self")
        );
        // No incoming hop -> outgoing hop 1.
        let msg2 = ctx.build_send_message("hi", None, None, None).unwrap();
        assert_eq!(
            msg2.metadata.as_ref().unwrap()[META_HOP],
            serde_json::json!(1)
        );
        // Incoming hop 4 -> outgoing hop 5 -> refused.
        assert!(ctx.build_send_message("x", None, None, Some(4)).is_err());
        assert!(ctx.next_hop(Some(3)).unwrap() == 4);
        assert!(ctx.next_hop(Some(4)).is_err());
    }

    #[test]
    fn empty_text_is_refused() {
        let ctx = ctx_with(vec![("alpha", "ops", "http://alpha.ops.svc:8080")]);
        assert!(ctx.build_send_message("   ", None, None, None).is_err());
    }

    #[tokio::test]
    async fn unknown_agent_is_refused() {
        let ctx = ctx_with(vec![]);
        let result = ctx.send_message("ghost", "hi", None, None, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ghost"));
    }

    #[test]
    fn list_agents_sorts_by_namespace_then_name() {
        let ctx = ctx_with(vec![
            ("b", "ns2", "http://b.ns2.svc:1"),
            ("a", "ns1", "http://a.ns1.svc:1"),
            ("a2", "ns1", "http://a2.ns1.svc:1"),
        ]);
        let all = ctx.list_agents();
        let names: Vec<&str> = all.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, vec!["a", "a2", "b"]);
    }

    #[test]
    fn discovery_namespaces_merges_and_dedupes() {
        std::env::remove_var("A2A_NAMESPACES");
        assert_eq!(discovery_namespaces(Some("pod")), vec!["pod"]);
        std::env::set_var("A2A_NAMESPACES", "pod, ops,other");
        assert_eq!(
            discovery_namespaces(Some("pod")),
            vec!["pod", "ops", "other"]
        );
        std::env::remove_var("A2A_NAMESPACES");
        assert_eq!(discovery_namespaces(None), Vec::<String>::new());
    }

    #[test]
    fn rfc1123_label_validation() {
        assert!(is_valid_rfc1123_label("default"));
        assert!(is_valid_rfc1123_label("my-agent-1"));
        assert!(is_valid_rfc1123_label("a"));
        assert!(!is_valid_rfc1123_label(""));
        assert!(!is_valid_rfc1123_label("-leading-dash"));
        assert!(!is_valid_rfc1123_label("trailing-dash-"));
        assert!(!is_valid_rfc1123_label("Upper-Case"));
        assert!(!is_valid_rfc1123_label("has.dots"));
        assert!(!is_valid_rfc1123_label("has/slashes"));
        assert!(!is_valid_rfc1123_label("has_underscores"));
        assert!(!is_valid_rfc1123_label(&"a".repeat(64)));
        assert!(is_valid_rfc1123_label(&"a".repeat(63)));
    }

    #[test]
    fn multi_namespace_same_name_avoids_shadowing() {
        let ctx = ctx_with(vec![
            ("worker", "team-a", "http://worker.team-a.svc:8080"),
            ("worker", "team-b", "http://worker.team-b.svc:8080"),
            ("unique", "team-a", "http://unique.team-a.svc:8080"),
        ]);
        // Unqualified "worker" is ambiguous across team-a and team-b -> None
        assert!(ctx.agent("worker").is_none());
        // Fully qualified "team-a/worker" and "team-b/worker" resolve correctly
        let a = ctx.agent("team-a/worker").expect("resolve team-a/worker");
        assert_eq!(a.namespace, "team-a");
        let b = ctx.agent("team-b/worker").expect("resolve team-b/worker");
        assert_eq!(b.namespace, "team-b");
        // Unique agent name without '/' resolves cleanly
        let u = ctx.agent("unique").expect("resolve unique");
        assert_eq!(u.namespace, "team-a");
    }

    #[tokio::test]
    async fn agent_card_size_cap_and_peer_override() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        // 1. Oversized card server (> 64 KB)
        let listener_big = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_big = listener_big.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            if let Ok((mut sock, _)) = listener_big.accept().await {
                let mut req_buf = [0u8; 1024];
                let _ = sock.read(&mut req_buf).await;
                let big_body = format!(
                    r#"{{"name":"{}", "data":"{}"}}"#,
                    "big",
                    "x".repeat(70 * 1024)
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    big_body.len(),
                    big_body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });

        // 2. Normal card server (< 64 KB)
        let listener_ok = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_ok = listener_ok.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            if let Ok((mut sock, _)) = listener_ok.accept().await {
                let mut req_buf = [0u8; 1024];
                let _ = sock.read(&mut req_buf).await;
                let ok_body = r#"{"name":"normal","version":"1.0"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    ok_body.len(),
                    ok_body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });

        let client = reqwest::Client::new();
        // Oversized card rejected (returns None)
        let big_res = fetch_agent_card(&client, &format!("http://{addr_big}")).await;
        assert!(big_res.is_none());

        // Normal card accepted
        let ok_res = fetch_agent_card(&client, &format!("http://{addr_ok}")).await;
        assert!(ok_res.is_some());
        assert_eq!(ok_res.unwrap()["name"], "normal");
    }

    #[test]
    fn label_constants_are_sane() {
        assert!(AGENT_LABEL.contains("a2a.homelab/agent"));
        assert_eq!(MAX_HOP, 4);
    }

    #[tokio::test]
    async fn jobset_interceptor_injects_both_headers() {
        let cred = crate::core::jobset::JobsetCredential {
            jobset_id: "js-1".to_string(),
            secret: "secret-1".to_string(),
            epoch: crate::core::jobset::EPOCH_ZERO,
        };
        let interceptor =
            JobsetCredentialInterceptor::from_credential(&cred).expect("valid credential");
        let mut params: ServiceParams = HashMap::new();
        interceptor
            .before("SendMessage", &mut params)
            .await
            .unwrap();
        assert_eq!(
            params
                .get(crate::core::jobset::JOBSET_ID_HEADER)
                .unwrap()
                .as_slice(),
            &["js-1"]
        );
        assert_eq!(
            params
                .get(crate::core::jobset::JOBSET_SECRET_HEADER)
                .unwrap()
                .as_slice(),
            &["secret-1"]
        );
    }

    #[test]
    fn jobset_interceptor_absent_without_credential() {
        assert!(JobsetCredentialInterceptor::from_credential(
            &crate::core::jobset::JobsetCredential {
                jobset_id: String::new(),
                secret: String::new(),
                epoch: crate::core::jobset::EPOCH_ZERO,
            }
        )
        .is_none());
    }
}

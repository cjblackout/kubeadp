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

use a2a::{GetTaskRequest, Message, Part, Role, SendMessageRequest, SendMessageResponse};
use a2a_client::agent_card::AgentCardResolver;
use a2a_client::jsonrpc::JsonRpcTransport;
use a2a_client::A2AClient;
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

/// A discovered peer agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentEntry {
    /// The Kubernetes Service name.
    pub name: String,
    /// The namespace the Service lives in.
    pub namespace: String,
    /// The base URL of the agent's A2A JSON-RPC endpoint
    /// (`http://<service>.<namespace>.svc:<port>`).
    pub url: String,
    /// The fetched agent card (best-effort; absent when the fetch failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub card: Option<serde_json::Value>,
}

/// The A2A sending-side client context shared by the MCP tools.
#[derive(Clone)]
pub struct A2aClientCtx {
    /// The discovered agent registry (name -> entry).
    agents: Arc<HashMap<String, AgentEntry>>,
    /// Sender self-name carried in message metadata (`a2a.sender`).
    self_name: Arc<str>,
}

impl A2aClientCtx {
    /// List the discovered agents (stable order by name).
    pub fn list_agents(&self) -> Vec<AgentEntry> {
        let mut agents: Vec<AgentEntry> = self.agents.values().cloned().collect();
        agents.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| a.name.cmp(&b.name))
        });
        agents
    }

    /// Resolve an agent by Service name.
    pub fn agent(&self, name: &str) -> Option<&AgentEntry> {
        self.agents.get(name)
    }

    /// The configured sender self-name.
    pub fn self_name(&self) -> &str {
        &self.self_name
    }

    /// Build a client for the agent's JSON-RPC endpoint.
    ///
    /// The connection base is the cluster-internal Service URL, unless the
    /// `A2A_URL_OVERRIDES` test/development seam (`name=url,...`) maps this
    /// agent to another base (e.g. an in-process server on 127.0.0.1).
    fn client_for(&self, entry: &AgentEntry) -> Result<A2AClient<JsonRpcTransport>, String> {
        let base = url_override(&entry.name).unwrap_or_else(|| entry.url.clone());
        let http = a2a_client::default_reqwest_client(None)
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;
        let transport = JsonRpcTransport::new(http, format!("{}/rpc", base.trim_end_matches('/')));
        Ok(A2AClient::new(transport))
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

    /// Send a text message to the given agent (by Service name), tagging it
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
            .agents
            .get(agent)
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

    /// Fetch the current state of a task from the given agent (by Service
    /// name).
    pub async fn get_task(&self, agent: &str, task_id: &str) -> Result<serde_json::Value, String> {
        let entry = self
            .agents
            .get(agent)
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
    let api_base = kube_api
        .map(|s| s.trim_end_matches('/').to_string())
        .unwrap_or_else(|| "https://kubernetes.default.svc".to_string());

    let services = list_services(&api_base, token.as_deref())
        .await
        .map_err(DiscoveryError::KubeApi)?;

    let mut agents = HashMap::new();
    let mut card_resolver = AgentCardResolver::new(None);
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
            .unwrap_or_else(|| namespace.clone().unwrap_or_default());
        if name.is_empty() {
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
        let card_resolver = &mut card_resolver;
        let card = tokio::time::timeout(
            CARD_FETCH_TIMEOUT,
            card_resolver.resolve(&url_override(&name).unwrap_or_else(|| url.clone())),
        )
        .await
        .ok()
        .and_then(|r| r.ok())
        .map(|card| serde_json::to_value(card).unwrap_or_default());
        agents.insert(
            name.clone(),
            AgentEntry {
                name,
                namespace: svc_ns,
                url,
                card,
            },
        );
    }

    let self_name: Arc<str> = std::env::var("A2A_SELF_NAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            // The banner hostname is the closest in-pod identity available.
            std::fs::read_to_string("/proc/self/comm")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "kubeadp-agent".to_string())
        .into();

    // If no namespace is configured, treat it as "not in-cluster": in that
    // case a registry discovered externally is still usable, but the empty
    // namespace is a strong signal.
    let _ = namespace;

    Ok(A2aClientCtx {
        agents: Arc::new(agents),
        self_name,
    })
}

/// An empty client context (no discovered agents). Used by the MCP server when
/// startup discovery fails, so the stdio server still starts and every tool
/// reports a clear error instead of the process exiting.
pub fn empty_ctx() -> A2aClientCtx {
    A2aClientCtx {
        agents: Arc::new(HashMap::new()),
        self_name: "kubeadp-agent".into(),
    }
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
pub fn discovery_namespaces(own_namespace: Option<&str>) -> Vec<String> {
    let mut namespaces = Vec::new();
    if let Some(ns) = own_namespace {
        if !ns.is_empty() {
            namespaces.push(ns.to_string());
        }
    }
    if let Ok(extra) = std::env::var("A2A_NAMESPACES") {
        for ns in extra.split(',') {
            let ns = ns.trim();
            if !ns.is_empty() && !namespaces.iter().any(|x| x == ns) {
                namespaces.push(ns.to_string());
            }
        }
    }
    namespaces
}

/// Resolve a service-name -> base-URL override for tests.
///
/// `A2A_URL_OVERRIDES` is a comma-separated `name=url` list (also accepting
/// whitespace around separators). Returns `None` when unset or the name is not
/// listed.
fn url_override(name: &str) -> Option<String> {
    let Ok(overrides) = std::env::var("A2A_URL_OVERRIDES") else {
        return None;
    };
    for pair in overrides.split(',') {
        let Some((n, url)) = pair.split_once('=') else {
            continue;
        };
        if n.trim() == name {
            return Some(url.trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with(agents: Vec<(&str, &str, &str)>) -> A2aClientCtx {
        let mut map = HashMap::new();
        for (name, ns, url) in agents {
            map.insert(
                name.to_string(),
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
    fn label_constants_are_sane() {
        assert!(AGENT_LABEL.contains("a2a.homelab/agent"));
        assert_eq!(MAX_HOP, 4);
    }
}

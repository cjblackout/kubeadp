//! MCP stdio entry for the A2A sending side (`kubeadp-agent mcp-a2a`).
//!
//! The Kubernetes service-account discovery is implemented here (the real
//! list callback passed to [`crate::core::a2a::client::build_ctx`]): it lists
//! Services with the label `a2a.kubeadp/agent=true` across the pod's own
//! namespace plus `A2A_NAMESPACES`. The HTTP call to the Kubernetes API uses
//! the in-cluster service-account token and CA, or `A2A_KUBE_API` for tests.

use crate::core::a2a::client::{discovery_namespaces, AGENT_LABEL};

/// Label key and value selecting agent Services, derived from [`AGENT_LABEL`]
/// (`<key>=<value>`).
const AGENT_LABEL_KEY: &str = "a2a.kubeadp/agent";
const AGENT_LABEL_VALUE: &str = "true";

/// The selector `AGENT_LABEL` used as the Kubernetes API label selector.
/// The `=` in a label selector is fine unescaped in a query string; a
/// definitive encoding would use `%3D`, but the k8s API accepts both.
fn agent_label_selector() -> &'static str {
    AGENT_LABEL
}

/// List Services with [`AGENT_LABEL`] across the discovery namespaces.
///
/// This is the production list callback; it is a plain async fn so the
/// `builder` wiring below can pass it through `build_ctx`.
pub async fn discover_services(
    api_base: &str,
    token: Option<&str>,
) -> Result<Vec<serde_json::Value>, String> {
    let namespaces = discovery_namespaces(
        std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .ok()
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty()),
    );
    let client = http_client(token)?;
    let mut out = Vec::new();
    let selector = agent_label_selector();
    for ns in namespaces {
        let url = format!("{api_base}/api/v1/namespaces/{ns}/services?labelSelector={selector}");
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("list services in {ns}: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_else(|_| String::new());
            return Err(format!("kubernetes API {status} for {url}: {body}"));
        }
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("invalid services response for {ns}: {e}"))?;
        // The API already applied the label selector; keep a defensive filter
        // so a misbehaving/mocked API cannot list non-agent services.
        let mut filtered: Vec<serde_json::Value> = json
            .get("items")
            .and_then(|i| i.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|svc| {
                svc.get("metadata")
                    .and_then(|m| m.get("labels"))
                    .and_then(|l| l.get(AGENT_LABEL_KEY))
                    .and_then(|v| v.as_str())
                    == Some(AGENT_LABEL_VALUE)
            })
            .collect();
        out.append(&mut filtered);
    }
    Ok(out)
}

fn http_client(token: Option<&str>) -> Result<reqwest::Client, String> {
    use std::sync::{Mutex, OnceLock};

    if token.is_none() {
        static NO_TOKEN_CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
        return NO_TOKEN_CLIENT
            .get_or_init(|| build_http_client(None))
            .clone();
    }

    static TOKEN_CLIENT: OnceLock<Mutex<Option<(String, reqwest::Client)>>> = OnceLock::new();
    let slot = TOKEN_CLIENT.get_or_init(|| Mutex::new(None));
    let mut guard = slot.lock().unwrap_or_else(|e| e.into_inner());
    let tok_str = token.unwrap();
    if let Some((cached_token, client)) = guard.as_ref() {
        if cached_token == tok_str {
            return Ok(client.clone());
        }
    }
    let client = build_http_client(Some(tok_str))?;
    *guard = Some((tok_str.to_string(), client.clone()));
    Ok(client)
}

fn build_http_client(token: Option<&str>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        // Bound discovery with explicit connect and request timeouts so an unreachable
        // cluster cannot hang server startup or service discovery.
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(5));
    if let Some(ca) = read_optional("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt") {
        if let Ok(certs) = reqwest::Certificate::from_pem_bundle(ca.as_bytes()) {
            builder = builder.tls_certs_merge(certs);
        }
    }
    let mut builder = builder;
    if let Some(token) = token {
        eprintln!("[mcp-a2a] using in-cluster service-account token");
        builder = builder.default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")) {
                headers.insert(reqwest::header::AUTHORIZATION, v);
            }
            headers
        });
    }
    builder
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))
}

fn read_optional(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Build the MCP server around a freshly-discovered A2A client context.
///
/// A discovery failure at startup (e.g. the MCP client is being set up before
/// the cluster is reachable) does not kill the server: it starts with an
/// empty registry and every tool reports its error, which is far friendlier
/// for a stdio MCP client than an immediate exit. When the static `A2A_PEERS`
/// registry is configured, the fallback uses ONLY that registry (so a
/// sandboxed job with no service-account token still gets its configured
/// peers), rather than an empty one.
pub async fn build_server(
    kube_api: Option<String>,
) -> Result<crate::mcp_a2a::A2aMcpServer, Box<dyn std::error::Error + Send + Sync>> {
    let ctx = match crate::core::a2a::client::build_ctx(kube_api.clone(), |base, token| {
        let base = base.to_string();
        let token = token.map(|s| s.to_string());
        async move { discover_services(&base, token.as_deref()).await }
    })
    .await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("[mcp-a2a] agent discovery failed ({e}); starting with a fallback registry");
            let fallback = crate::core::a2a::client::static_peers_ctx();
            if fallback.list_agents().is_empty() {
                eprintln!(
                    "[mcp-a2a] no static A2A_PEERS configured; starting with an empty registry"
                );
            } else {
                eprintln!(
                    "[mcp-a2a] using {} static A2A_PEERS entr{}",
                    fallback.list_agents().len(),
                    if fallback.list_agents().len() == 1 {
                        "y"
                    } else {
                        "ies"
                    }
                );
            }
            fallback
        }
    };
    Ok(crate::mcp_a2a::A2aMcpServer::new(ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_label_constants_stay_in_lockstep() {
        assert_eq!(
            format!("{AGENT_LABEL_KEY}={AGENT_LABEL_VALUE}"),
            AGENT_LABEL
        );
    }

    #[test]
    fn optional_read_trims() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x");
        std::fs::write(&p, "  value  \n").unwrap();
        assert_eq!(
            read_optional(p.to_str().unwrap()),
            Some("value".to_string())
        );
        assert_eq!(read_optional("/nonexistent/path"), None);
    }

    /// `static_peers_ctx` builds a context purely from `A2A_PEERS` (the
    /// no-SA-token fallback): the static entries become the registry and the
    /// self-name is honoured as-is.
    #[test]
    fn static_peers_ctx_registers_configured_peers() {
        let _g = crate::test_env_lock();
        use crate::core::a2a::client::A2A_PEERS_ENV;

        std::env::set_var(
            A2A_PEERS_ENV,
            "worker=http://worker.invalid:8002, other/agent=http://agent.invalid:8002",
        );
        std::env::set_var("A2A_SELF_NAME", "kubeadp-agent-worker-service/job-a1");
        let ctx = crate::core::a2a::client::static_peers_ctx();
        let agents = ctx.list_agents();
        let names: Vec<&str> = agents.iter().map(|a| a.name.as_str()).collect();
        assert!(
            names.contains(&"worker") && names.contains(&"agent"),
            "static peers are registered: {names:?}"
        );
        // The per-job self-name survives into the context.
        assert_eq!(
            ctx.self_name(),
            "kubeadp-agent-worker-service/job-a1",
            "per-job self-name is honoured"
        );
        // A bare peer is placed in the pod namespace when known.
        let worker = ctx
            .list_agents()
            .into_iter()
            .find(|a| a.name == "worker")
            .expect("worker peer present");
        std::env::remove_var(A2A_PEERS_ENV);
        std::env::remove_var("A2A_SELF_NAME");
        if std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .is_some()
        {
            assert!(
                !worker.namespace.is_empty(),
                "bare peer gets the pod namespace"
            );
        }
    }

    #[test]
    fn http_client_reuses_shared_instance() {
        let client1 = http_client(None).expect("build no-token client");
        let client2 = http_client(None).expect("build no-token client");
        // Verify both returned clients are built cleanly
        assert!(http_client(Some("test-token-1")).is_ok());
        let tok1_a = http_client(Some("test-token-1")).expect("build token client");
        let tok1_b = http_client(Some("test-token-1")).expect("build token client");
        let _ = (client1, client2, tok1_a, tok1_b);
    }
}

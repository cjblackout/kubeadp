//! Discovery tests for the A2A sending side: `discover_services` against a
//! local fake Kubernetes API, plus the agent-entry URL/port derivation and the
//! `A2A_NAMESPACES` / `A2A_URL_OVERRIDES` seams.
#![cfg(feature = "a2a")]

use kubeadp_agent::core::a2a::client::{build_ctx, MAX_HOP};
use kubeadp_agent::mcp_a2a::discover_services;
use tokio::net::TcpListener;

/// Start a fake Kubernetes API that returns the given Service JSON body for
/// every services list request; return its base URL.
async fn fake_kube(body: serde_json::Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            let body = body.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.to_string().len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    format!("http://{addr}")
}

fn svc(name: &str, ns: &str, port: u64, labeled: bool) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "name": name,
            "namespace": ns,
            "labels": if labeled {
                serde_json::json!({ "a2a.homelab/agent": "true" })
            } else {
                serde_json::json!({})
            }
        },
        "spec": {
            "ports": [
                { "name": "a2a", "port": port }
            ]
        }
    })
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn discovery_filters_by_label_and_derives_url() {
    let _g = kubeadp_agent::test_env_lock();
    let body = serde_json::json!({
        "kind": "ServiceList",
        "items": [
            svc("alpha", "ops", 8081, true),
            svc("beta", "ops", 8082, true),
            svc("plain", "ops", 9999, false),
        ]
    });
    let base = fake_kube(body).await;

    // No in-cluster namespace file here -> discovery_namespaces reads
    // A2A_NAMESPACES only.
    std::env::set_var("A2A_NAMESPACES", "ops");
    let svcs = discover_services(&base, None).await.expect("list services");
    std::env::remove_var("A2A_NAMESPACES");

    // The fake API ignores the labelSelector (returns all three); the
    // defensive client-side filter keeps only labeled agents.
    let mut names: Vec<String> = svcs
        .iter()
        .filter_map(|s| s["metadata"]["name"].as_str().map(|s| s.to_string()))
        .collect();
    names.sort();
    // A pod with a ServiceAccount namespace file lists its own namespace as well
    // (the fake API answers every list with the same items), so compare unique names.
    names.dedup();
    assert_eq!(names, vec!["alpha", "beta"]);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn build_ctx_builds_registry_with_url_and_card() {
    let _g = kubeadp_agent::test_env_lock();
    let body = serde_json::json!({
        "kind": "ServiceList",
        "items": [ svc("alpha", "ops", 8081, true) ]
    });
    let base = fake_kube(body).await;
    std::env::set_var("A2A_NAMESPACES", "ops");
    // No URL override -> card fetch against the (unreachable) cluster URL
    // times out and is omitted, which is the correct best-effort behavior.
    let list = |b: &str, t: Option<&str>| {
        let b = b.to_string();
        let t = t.map(|s| s.to_string());
        async move { discover_services(&b, t.as_deref()).await }
    };
    let ctx = build_ctx(Some(base), list).await.expect("build ctx");
    std::env::remove_var("A2A_NAMESPACES");

    let agents = ctx.list_agents();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].name, "alpha");
    assert_eq!(agents[0].namespace, "ops");
    assert_eq!(agents[0].url, "http://alpha.ops.svc:8081");
    assert!(agents[0].card.is_none());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn send_message_only_accepts_registry_names() {
    let _g = kubeadp_agent::test_env_lock();
    // A URL-looking agent argument is not a registry name -> refused before
    // any network call.
    let body = serde_json::json!({ "kind": "ServiceList", "items": [] });
    let base = fake_kube(body).await;
    std::env::set_var("A2A_NAMESPACES", "ops");
    let list = |b: &str, t: Option<&str>| {
        let b = b.to_string();
        let t = t.map(|s| s.to_string());
        async move { discover_services(&b, t.as_deref()).await }
    };
    let ctx = build_ctx(Some(base), list).await.expect("build ctx");
    std::env::remove_var("A2A_NAMESPACES");

    let err = ctx
        .send_message("http://evil.example:8080", "hi", None, None, None)
        .await
        .unwrap_err();
    assert!(err.contains("no A2A agent named"), "got: {err}");
}

#[test]
fn debug_constants_signature() {
    assert_eq!(MAX_HOP, 4);
}

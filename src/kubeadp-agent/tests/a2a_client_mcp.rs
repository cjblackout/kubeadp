//! End-to-end test for the A2A sending-side MCP tools (`mcp-a2a`): starts an
//! in-process A2A server (the SDK-backed router over axum, exactly like
//! `tests/a2a_client.rs`), builds the MCP server's client context around it
//! (via `build_ctx` with a fake discovery callback + `A2A_URL_OVERRIDES`),
//! and drives `list_agents -> send_message -> get_task` through the rmcp
//! in-memory transport.
//!
//! Gated behind the `a2a` feature like the rest of the A2A surface.
#![cfg(feature = "a2a")]

use a2a::TaskState;
use kubeadp_agent::core::a2a::{a2a_router, A2aModel, A2aTask, TaskBackend};
use kubeadp_agent::mcp_a2a::A2aMcpServer;
use rmcp::model::CallToolRequestParams;
use rmcp::ServiceExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

/// In-memory fake backend plus a recorded sink, mirroring tests/a2a_client.rs.
struct FakeBackend {
    tasks: Mutex<HashMap<String, A2aTask>>,
}

impl FakeBackend {
    fn with(tasks: Vec<A2aTask>) -> Arc<Self> {
        Arc::new(Self {
            tasks: Mutex::new(tasks.into_iter().map(|t| (t.id.clone(), t)).collect()),
        })
    }
}

impl TaskBackend for FakeBackend {
    fn get_task(&self, task_id: &str) -> Result<A2aTask, kubeadp_agent::core::a2a::A2AError> {
        self.tasks
            .lock()
            .unwrap()
            .get(task_id)
            .cloned()
            .ok_or_else(|| kubeadp_agent::core::a2a::A2AError::task_not_found(task_id))
    }

    fn list_tasks(
        &self,
        context_id: Option<&str>,
        state: Option<&TaskState>,
    ) -> Result<Vec<A2aTask>, kubeadp_agent::core::a2a::A2AError> {
        Ok(self
            .tasks
            .lock()
            .unwrap()
            .values()
            .filter(|t| context_id.is_none_or(|c| t.context_id.as_deref() == Some(c)))
            .filter(|t| state.is_none_or(|s| &t.state == s))
            .cloned()
            .collect())
    }

    fn cancel_task(&self, task_id: &str) -> Result<A2aTask, kubeadp_agent::core::a2a::A2AError> {
        let mut guard = self.tasks.lock().unwrap();
        let task = guard
            .get_mut(task_id)
            .ok_or_else(|| kubeadp_agent::core::a2a::A2AError::task_not_found(task_id))?;
        task.state = TaskState::Canceled;
        Ok(task.clone())
    }
}

fn task(id: &str, context: &str, state: TaskState) -> A2aTask {
    A2aTask {
        id: id.to_string(),
        context_id: Some(context.to_string()),
        state,
        status_message: Some("some result".to_string()),
        error: None,
        artifacts: None,
    }
}

/// Recording sink so the peer server accepts SendMessage and records the
/// delivered text (mirroring tests/a2a_client.rs).
#[derive(Default)]
struct RecordingSink {
    delivered: Mutex<Vec<(String, String)>>,
}

impl kubeadp_agent::core::a2a::MessageSink for RecordingSink {
    fn deliver(&self, task_id: &str, text: &str) -> Result<(), a2a::A2AError> {
        self.delivered
            .lock()
            .unwrap()
            .push((task_id.to_string(), text.to_string()));
        Ok(())
    }
}

/// Start the SDK A2A server on an ephemeral port; return the base URL.
async fn serve(backend: Arc<FakeBackend>, sink: Arc<RecordingSink>) -> String {
    let model = A2aModel {
        name: "peer-agent".to_string(),
        description: "e2e peer".to_string(),
        version: "0.0.0".to_string(),
        skills: vec![],
        json_rpc_url: "http://127.0.0.1/unused".to_string(),
    };
    let app = a2a_router(model, backend, Some(sink), None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Build the MCP server with the client context pointed at the live in-process
/// A2A server via a fake discovery callback returning one agent Service.
async fn mcp_server(peer_port: u64) -> A2aMcpServer {
    let list_services = |_base: &str, _token: Option<&str>| {
        let peer = serde_json::json!({
            "metadata": { "name": "peer", "namespace": "testns", "labels": {
                "a2a.homelab/agent": "true" } },
            "spec": { "ports": [ { "name": "a2a", "port": peer_port } ] }
        });
        async move { Ok(vec![peer]) }
    };
    let ctx = kubeadp_agent::core::a2a::client::build_ctx(None, list_services)
        .await
        .expect("build ctx");
    A2aMcpServer::new(ctx)
}

/// The e2e tests mutate process-global env vars (`A2A_URL_OVERRIDES`,
/// `A2A_SELF_NAME`). A shared mutex serializes them so parallel runners cannot
/// clobber each other's overrides.
fn env_mutex() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static M: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    M.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test]
async fn mcp_a2a_tools_round_trip() {
    let _guard = env_mutex().lock().await;
    // The in-process A2A server (peer) on an ephemeral port.
    let backend = FakeBackend::with(vec![task("t-1", "ctx-1", TaskState::Working)]);
    let base = serve(backend.clone(), Arc::new(RecordingSink::default())).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();

    // Override the discovery URL so the client reaches the in-process server.
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));
    std::env::set_var("A2A_SELF_NAME", "sender-test");

    let server = mcp_server(port).await;

    // In-memory duplex transport in the MCP server.
    let (server_transport, client_transport) = tokio::io::duplex(8192);
    let _server_task = tokio::spawn(async move {
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    let client = ().serve(client_transport).await.expect("client handshake");

    // 1. list_agents: the discovered peer (with card if the A2A server serves it).
    let res = client
        .call_tool(CallToolRequestParams::new("list_agents"))
        .await
        .expect("call list_agents");
    assert_eq!(res.is_error, Some(false));
    let text = res.content[0].as_text().unwrap().text.clone();
    let val: serde_json::Value = serde_json::from_str(&text).unwrap();
    let agents = val["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["name"], "peer");
    assert_eq!(agents[0]["namespace"], "testns");
    // The advertised URL is the cluster-internal Service URL (production
    // behavior); the A2A_URL_OVERRIDES seam only redirects the *connection*.
    assert_eq!(agents[0]["url"], format!("http://peer.testns.svc:{port}"));
    // The card was fetched at discovery time (best-effort, no auth).
    assert_eq!(agents[0]["card"]["name"], "peer-agent");

    // 2. send_message to the peer (tagging metadata: sender + hop 1).
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({
                    "agent": "peer",
                    "text": "hello peer",
                    "task_id": "t-1"
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("call send_message");
    assert_eq!(
        res.is_error,
        Some(false),
        "send_message error: {:?}",
        res.content
            .iter()
            .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
            .collect::<Vec<_>>()
    );
    let text = res.content[0].as_text().unwrap().text.clone();
    let val: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(val["id"], "t-1");
    assert_eq!(val["status"]["state"], "TASK_STATE_WORKING");

    // 3. get_task on the peer.
    let res = client
        .call_tool(
            CallToolRequestParams::new("get_task").with_arguments(
                serde_json::json!({ "agent": "peer", "task_id": "t-1" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call get_task");
    assert_eq!(res.is_error, Some(false));
    let text = res.content[0].as_text().unwrap().text.clone();
    let val: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(val["id"], "t-1");
    assert_eq!(val["status"]["state"], "TASK_STATE_WORKING");

    // 4. Error path: unknown agent and unknown task surface as MCP tool errors.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "ghost", "text": "hi" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message ghost");
    assert_eq!(res.is_error, Some(true));

    let res = client
        .call_tool(
            CallToolRequestParams::new("get_task").with_arguments(
                serde_json::json!({ "agent": "peer", "task_id": "nope" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call get_task missing");
    assert_eq!(res.is_error, Some(true));

    std::env::remove_var("A2A_URL_OVERRIDES");
    std::env::remove_var("A2A_SELF_NAME");
}

/// The `max_hop` limit surfaces as a tool error on send, not a panic.
#[tokio::test]
async fn mcp_a2a_send_refuses_beyond_max_hop() {
    let _guard = env_mutex().lock().await;
    let backend = FakeBackend::with(vec![task("t-1", "ctx-1", TaskState::Working)]);
    let base = serve(backend, Arc::new(RecordingSink::default())).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));

    let server = mcp_server(port).await;
    let (server_transport, client_transport) = tokio::io::duplex(8192);
    let _server_task = tokio::spawn(async move {
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    let client = ().serve(client_transport).await.expect("client handshake");

    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "x", "incoming_hop": 4 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message over hop");
    assert_eq!(res.is_error, Some(true));
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.contains("hop limit"), "got: {text}");

    std::env::remove_var("A2A_URL_OVERRIDES");
}

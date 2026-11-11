//! End-to-end test for the A2A sending-side MCP tools (`mcp-a2a`): starts an
//! in-process A2A server (the SDK-backed router over axum, exactly like
//! `tests/a2a_client.rs`), builds the MCP server's client context around it
//! (via `build_ctx` with a fake discovery callback + `A2A_URL_OVERRIDES`),
//! and drives `list_agents -> send_message / reply -> get_task` through the
//! rmcp in-memory transport.
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
/// delivered text (mirroring tests/a2a_client.rs) plus the claimed sender.
#[derive(Default)]
struct RecordingSink {
    delivered: Mutex<Vec<(String, String, String)>>,
}

impl kubeadp_agent::core::a2a::MessageSink for RecordingSink {
    fn deliver_with_meta(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        _message_id: &str,
        _meta: kubeadp_agent::core::a2a::DeliveryMeta,
    ) -> Result<(), a2a::A2AError> {
        self.delivered.lock().unwrap().push((
            task_id.to_string(),
            sender.to_string(),
            text.to_string(),
        ));
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
        url: "http://127.0.0.1/unused".to_string(),
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

/// Build the MCP server with a caller-supplied discovery callback so tests can
/// register several peers (e.g. same name in two namespaces).
async fn mcp_server_with<F, Fut>(list_services: F) -> A2aMcpServer
where
    F: Fn(&str, Option<&str>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Vec<serde_json::Value>, String>> + Send,
{
    let ctx = kubeadp_agent::core::a2a::client::build_ctx(None, list_services)
        .await
        .expect("build ctx");
    A2aMcpServer::new(ctx)
}

/// Drive the in-memory MCP transport around a server; returns the client as a
/// type-erased function that keeps the `RunningService` type out of the
/// helper's signature.
async fn mcp_client(
    server: A2aMcpServer,
) -> rmcp::service::RunningService<rmcp::service::RoleClient, ()> {
    let (server_transport, client_transport) = tokio::io::duplex(8192);
    tokio::spawn(async move {
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    ().serve(client_transport).await.expect("client handshake")
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
    let sink = Arc::new(RecordingSink::default());
    let base = serve(backend.clone(), sink.clone()).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();

    // Override the discovery URL so the client reaches the in-process server.
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));
    std::env::set_var("A2A_SELF_NAME", "sender-test");

    let server = mcp_server(port).await;

    let (server_transport, client_transport) = tokio::io::duplex(8192);
    let _server_task = tokio::spawn(async move {
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    let client = ().serve(client_transport).await.expect("client handshake");

    // 0. The tool set contains `reply`.
    let tools_list = client.list_tools(None).await.expect("list tools");
    let names: Vec<&str> = tools_list.tools.iter().map(|t| t.name.as_ref()).collect();
    for expected in ["list_agents", "send_message", "reply", "get_task"] {
        assert!(
            names.contains(&expected),
            "tool {expected} must be in the list, got {names:?}"
        );
    }
    // 0b. The reply tool documents the explicit-to mechanism and the frame.
    let reply_tool = tools_list
        .tools
        .iter()
        .find(|t| t.name.as_ref() == "reply")
        .expect("reply tool present");
    let reply_desc = reply_tool.description.clone().unwrap_or_default();
    assert!(
        reply_desc.contains("claimed sender") && reply_desc.contains("--- A2A INCOMING MESSAGE"),
        "reply must document the framed sender header: {reply_desc}"
    );

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

    // The peer delivered it with the sender from A2A_SELF_NAME.
    {
        let delivered = sink.delivered.lock().unwrap();
        assert_eq!(delivered.len(), 1);
        // claimed sender of the outgoing message is `sender-test`.
        assert_eq!(delivered[0].1, "sender-test");
        assert!(delivered[0].2.contains("hello peer"));
    }

    // 3. reply: send back to the peer that would have sent us a message. In
    // production the framed header's `claimed sender:` equals the sending
    // agent's Service name (= its A2A_SELF_NAME), so the model copies it
    // verbatim into `to` and it resolves in the registry. Here the peer's
    // Service name is `peer`; it receives our reply tagged with our own
    // sender (`sender-test`).
    let res = client
        .call_tool(
            CallToolRequestParams::new("reply").with_arguments(
                serde_json::json!({
                    "to": "peer", // copied from "claimed sender:" in the frame
                    "text": "thanks, back to you",
                    "task_id": "t-1"
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("call reply");
    assert_eq!(
        res.is_error,
        Some(false),
        "reply error: {:?}",
        res.content
            .iter()
            .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
            .collect::<Vec<_>>()
    );
    // The reply arrived at the peer from us (sender-test) with the reply text.
    {
        let delivered = sink.delivered.lock().unwrap();
        assert_eq!(delivered.len(), 2, "send + reply both delivered");
        assert_eq!(delivered[1].1, "sender-test");
        assert!(delivered[1].2.contains("thanks, back to you"));
    }

    // 4. get_task on the peer.
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

    // 5. Error path: unknown agent and unknown task surface as MCP tool errors.
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

    // reply to an unknown sender is likewise an MCP tool error.
    let res = client
        .call_tool(
            CallToolRequestParams::new("reply").with_arguments(
                serde_json::json!({ "to": "ghost", "text": "hi" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call reply ghost");
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
    let client = mcp_client(server).await;

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

    // The same limit applies to reply (its `incoming_hop` is the incoming
    // message's hop; the reply would be one above).
    let res = client
        .call_tool(
            CallToolRequestParams::new("reply").with_arguments(
                serde_json::json!({ "to": "peer", "text": "x", "incoming_hop": 4 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call reply over hop");
    assert_eq!(res.is_error, Some(true));
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.contains("hop limit"), "got: {text}");

    std::env::remove_var("A2A_URL_OVERRIDES");
}

/// `send_message`/`reply` accept a `namespace/name` pair to disambiguate
/// same-named agents across namespaces; a bare ambiguous name is an error.
#[tokio::test]
async fn mcp_a2a_name_vs_namespace_name_resolution() {
    let _guard = env_mutex().lock().await;
    // Two peer servers, one per namespace, both named "worker".
    let backend_a = FakeBackend::with(vec![task("t-a", "ctx-a", TaskState::Working)]);
    let base_a = serve(backend_a, Arc::new(RecordingSink::default())).await;
    let port_a: u64 = base_a.rsplit(':').next().unwrap().parse().unwrap();
    let backend_b = FakeBackend::with(vec![task("t-b", "ctx-b", TaskState::Working)]);
    let base_b = serve(backend_b, Arc::new(RecordingSink::default())).await;
    let port_b: u64 = base_b.rsplit(':').next().unwrap().parse().unwrap();

    std::env::set_var(
        "A2A_URL_OVERRIDES",
        format!("ns-a/worker={base_a},ns-b/worker={base_b}"),
    );

    let services_a = port_a;
    let services_b = port_b;
    let list_services = move |_base: &str, _token: Option<&str>| {
        let peers = vec![
            serde_json::json!({
                "metadata": { "name": "worker", "namespace": "ns-a", "labels": {
                    "a2a.homelab/agent": "true" } },
                "spec": { "ports": [ { "name": "a2a", "port": services_a } ] }
            }),
            serde_json::json!({
                "metadata": { "name": "worker", "namespace": "ns-b", "labels": {
                    "a2a.homelab/agent": "true" } },
                "spec": { "ports": [ { "name": "a2a", "port": services_b } ] }
            }),
        ];
        async move { Ok(peers) }
    };
    let server = mcp_server_with(list_services).await;
    let client = mcp_client(server).await;

    // Ambiguous bare name is a tool error.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "worker", "text": "hi" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message ambiguous");
    assert_eq!(res.is_error, Some(true), "ambiguous name must error");

    // Fully qualified namespace/name reaches ns-b's worker.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "ns-b/worker", "text": "hi b", "task_id": "t-b" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message ns-b/worker");
    assert_eq!(
        res.is_error,
        Some(false),
        "qualified send_message error: {:?}",
        res.content
            .iter()
            .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
            .collect::<Vec<_>>()
    );
    let val: serde_json::Value =
        serde_json::from_str(&res.content[0].as_text().unwrap().text).unwrap();
    assert_eq!(val["id"], "t-b");

    // reply accepts the same qualified form.
    let res = client
        .call_tool(
            CallToolRequestParams::new("reply").with_arguments(
                serde_json::json!({ "to": "ns-a/worker", "text": "hi a", "task_id": "t-a" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call reply ns-a/worker");
    assert_eq!(
        res.is_error,
        Some(false),
        "qualified reply error: {:?}",
        res.content
            .iter()
            .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
            .collect::<Vec<_>>()
    );
    let val: serde_json::Value =
        serde_json::from_str(&res.content[0].as_text().unwrap().text).unwrap();
    assert_eq!(val["id"], "t-a");

    std::env::remove_var("A2A_URL_OVERRIDES");
}

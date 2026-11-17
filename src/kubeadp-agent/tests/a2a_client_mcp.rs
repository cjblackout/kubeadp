//! End-to-end test for the A2A sending-side MCP tools (`mcp-a2a`): starts an
//! in-process A2A server (the SDK-backed router over axum, exactly like
//! `tests/a2a_client.rs`), builds the MCP server's client context around it
//! (via `build_ctx` with a fake discovery callback + `A2A_URL_OVERRIDES`),
//! and drives `list_agents -> send_message / reply -> get_task` through the
//! rmcp in-memory transport.
//!
//! Gated behind the `a2a` feature like the rest of the A2A surface.
#![cfg(feature = "a2a")]

use a2a::{Artifact, Part, TaskState};
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

/// A task whose result artifact carries `messages` conversation messages of
/// `chars` characters each (hundreds of long texts -- the bloat a long-running
/// peer job reports, result text and diff): the response the MCP tools must
/// keep bounded.
fn long_task(id: &str, context: &str, state: TaskState, messages: usize, chars: usize) -> A2aTask {
    let conversation: String = (0..messages)
        .map(|i| format!("message {i}: {}\n", "x".repeat(chars)))
        .collect();
    A2aTask {
        id: id.to_string(),
        context_id: Some(context.to_string()),
        state,
        status_message: Some(conversation.clone()),
        error: None,
        artifacts: Some(vec![Artifact {
            artifact_id: format!("{id}-result"),
            name: Some("result.txt".to_string()),
            description: Some("Job result text".to_string()),
            parts: vec![Part::text(conversation)],
            metadata: None,
            extensions: None,
        }]),
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
/// `A2A_SELF_NAME`). The crate-wide test env lock serializes them so parallel runners cannot
/// clobber each other's overrides.
fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    kubeadp_agent::test_env_lock()
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_tools_round_trip() {
    let _guard = env_lock();
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
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_send_refuses_beyond_max_hop() {
    let _guard = env_lock();
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
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_name_vs_namespace_name_resolution() {
    let _guard = env_lock();
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

/// A peer task with a VERY long result (hundreds of long messages, result
/// text and artifact): `send_message` and `reply` still return the small
/// fixed-shape success result (`delivered: true`, well below the response
/// cap) and `get_task` returns a bounded result -- a delivered message is
/// never replaced by the response-cap error.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_tools_stay_bounded_on_long_task_responses() {
    let _guard = env_lock();
    // The peer task's response carries ~300 KB of result text (300 messages
    // of 1000 characters each), far above the 20480-byte response cap.
    let backend = FakeBackend::with(vec![long_task(
        "t-big",
        "ctx-big",
        TaskState::Working,
        300,
        1000,
    )]);
    let sink = Arc::new(RecordingSink::default());
    let base = serve(backend, sink.clone()).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));
    std::env::set_var("A2A_SELF_NAME", "sender-test");

    let server = mcp_server(port).await;
    let client = mcp_client(server).await;

    // 1. send_message: success, delivered flag, bounded size, no oversize
    //    error -- although the peer's response task is far above the cap.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "hello peer", "task_id": "t-big" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message on a long task");
    assert_eq!(
        res.is_error,
        Some(false),
        "a delivered send must never be an error: {:?}",
        res.content
            .iter()
            .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
            .collect::<Vec<_>>()
    );
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(
        text.len() < 20480,
        "the send result stays below the cap: {} bytes",
        text.len()
    );
    assert!(
        !text.contains("response exceeds"),
        "never the oversize error"
    );
    let val: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(val["id"], "t-big");
    assert_eq!(val["status"]["state"], "TASK_STATE_WORKING");
    assert_eq!(val["delivered"], true, "the delivered flag");
    assert_eq!(val["delivery"], "injected");
    assert_eq!(val["hop"], 1, "the outgoing hop is in the summary");
    assert!(
        val.get("history").is_none(),
        "no history in the send result"
    );
    assert!(
        val.get("artifacts").is_none(),
        "no artifacts in the send result"
    );
    assert_eq!(
        sink.delivered.lock().unwrap().len(),
        1,
        "the message WAS delivered"
    );

    // 2. reply: the same fixed-shape success result.
    let res = client
        .call_tool(
            CallToolRequestParams::new("reply").with_arguments(
                serde_json::json!({ "to": "peer", "text": "thanks", "task_id": "t-big" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call reply on a long task");
    assert_eq!(
        res.is_error,
        Some(false),
        "reply must succeed: {:?}",
        res.content
    );
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.len() < 20480, "bounded: {} bytes", text.len());
    assert!(
        !text.contains("response exceeds"),
        "never the oversize error"
    );
    let val: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(val["id"], "t-big");
    assert_eq!(val["delivered"], true);

    // 3. get_task: a bounded result for any history size -- the state plus
    //    truncated texts, never the response-cap error.
    let res = client
        .call_tool(
            CallToolRequestParams::new("get_task").with_arguments(
                serde_json::json!({ "agent": "peer", "task_id": "t-big" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call get_task on a long task");
    assert_eq!(
        res.is_error,
        Some(false),
        "get_task must succeed: {:?}",
        res.content
    );
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.len() < 20480, "bounded: {} bytes", text.len());
    assert!(
        !text.contains("response exceeds"),
        "never the oversize error"
    );
    let val: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(val["id"], "t-big");
    assert_eq!(val["status"]["state"], "TASK_STATE_WORKING");
    // The long texts are truncated with the explicit marker.
    let artifact_text = val["artifacts"][0]["parts"][0]["text"].as_str().unwrap();
    assert!(
        artifact_text.contains("characters omitted"),
        "truncation marker: {artifact_text}"
    );
    let status_message = val["metadata"]["statusMessage"].as_str().unwrap();
    assert!(status_message.contains("characters omitted"));

    // 4. history_length: accepted as the argument of get_task (0 = none).
    let res = client
        .call_tool(
            CallToolRequestParams::new("get_task").with_arguments(
                serde_json::json!({ "agent": "peer", "task_id": "t-big", "history_length": 0 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call get_task with history_length");
    assert_eq!(res.is_error, Some(false));
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.len() < 20480, "bounded: {} bytes", text.len());

    // 5. history_length above the cap is clamped, not an error.
    let res = client
        .call_tool(
            CallToolRequestParams::new("get_task").with_arguments(
                serde_json::json!({ "agent": "peer", "task_id": "t-big", "history_length": 500 })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call get_task with a clamped history_length");
    assert_eq!(res.is_error, Some(false));
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.len() < 20480, "bounded: {} bytes", text.len());

    std::env::remove_var("A2A_URL_OVERRIDES");
    std::env::remove_var("A2A_SELF_NAME");
}

/// The argument names are unmistakable: missing or unknown arguments are
/// answered with an error that names the parameter and lists the valid ones,
/// and the obvious case variants (`text`/`message`/`Message`,
/// `task_id`/`taskId`) are accepted as aliases of the canonical names.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_argument_names_and_aliases() {
    let _guard = env_lock();
    let backend = FakeBackend::with(vec![task("t-1", "ctx-1", TaskState::Working)]);
    let sink = Arc::new(RecordingSink::default());
    let base = serve(backend, sink.clone()).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));

    let server = mcp_server(port).await;
    let client = mcp_client(server).await;

    // 1. The schema publishes ONLY the canonical names, with the required
    //    flags and the parameter descriptions.
    let tools_list = client.list_tools(None).await.expect("list tools");
    let schema_of = |name: &str| {
        tools_list
            .tools
            .iter()
            .find(|t| t.name.as_ref() == name)
            .unwrap_or_else(|| panic!("tool {name} present"))
            .input_schema
            .clone()
    };
    let send_schema = schema_of("send_message");
    let properties = send_schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("schema properties");
    let mut names: Vec<&str> = properties.keys().map(|k| k.as_str()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["agent", "context_id", "incoming_hop", "task_id", "text"],
        "only canonical names are published: {names:?}"
    );
    let required: Vec<&str> = send_schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert_eq!(
        required.len(),
        2,
        "agent and text are required: {required:?}"
    );
    for name in ["agent", "text", "context_id", "task_id", "incoming_hop"] {
        let description = properties[name]
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or_default();
        assert!(
            description.contains(name) && !description.is_empty(),
            "{name} description states the parameter: {description}"
        );
    }

    // 2. The case variants are accepted as aliases.
    for variant in ["Message", "message"] {
        let mut args = serde_json::Map::new();
        args.insert("agent".to_string(), serde_json::json!("peer"));
        args.insert("task_id".to_string(), serde_json::json!("t-1"));
        args.insert(variant.to_string(), serde_json::json!("hello via alias"));
        let res = client
            .call_tool(CallToolRequestParams::new("send_message").with_arguments(args))
            .await
            .expect("call send_message with a text alias");
        assert_eq!(
            res.is_error,
            Some(false),
            "{variant} is an accepted alias of text: {:?}",
            res.content
                .iter()
                .map(|c| c.as_text().map(|t| t.text.clone()).unwrap_or_default())
                .collect::<Vec<_>>()
        );
    }
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "hi", "taskId": "t-1" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message with a taskId alias");
    assert_eq!(res.is_error, Some(false), "taskId is an accepted alias");
    // reply accepts the same aliases.
    let res = client
        .call_tool(
            CallToolRequestParams::new("reply").with_arguments(
                serde_json::json!({ "to": "peer", "Message": "hi", "taskId": "t-1" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call reply with aliases");
    assert_eq!(res.is_error, Some(false), "reply accepts the aliases");

    // 3. A missing required argument is named.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message without text");
    assert_eq!(res.is_error, Some(true), "missing text must error");
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(
        text.contains("missing required argument `text`"),
        "the missing parameter is named: {text}"
    );
    assert!(
        text.contains("valid arguments") && text.contains("`agent`") && text.contains("`text`"),
        "the valid arguments are listed: {text}"
    );

    // 4. An unknown argument is named and the valid ones are listed.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "hi", "payload": "hi again" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message with an unknown argument");
    assert_eq!(res.is_error, Some(true), "an unknown argument must error");
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(
        text.contains("unknown argument `payload`"),
        "the unknown parameter is named: {text}"
    );
    assert!(
        text.contains("valid arguments"),
        "the valid ones are listed: {text}"
    );

    // 5. A wrongly typed argument is named.
    let res = client
        .call_tool(
            CallToolRequestParams::new("get_task").with_arguments(
                serde_json::json!({ "agent": "peer", "task_id": "t-1", "history_length": "five" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call get_task with a mistyped history_length");
    assert_eq!(res.is_error, Some(true), "a mistyped argument must error");
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(
        text.contains("`history_length`") && text.contains("must be a integer"),
        "the parameter and its type are named: {text}"
    );

    // 6. A missing `agent` is named the same way.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "text": "hi" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message without agent");
    assert_eq!(res.is_error, Some(true));
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(text.contains("missing required argument `agent`"), "{text}");

    std::env::remove_var("A2A_URL_OVERRIDES");
}

/// An inline message above the A2A ingress size cap fails with a clear error
/// that names the limit -- it is never silently truncated and never a generic
/// failure.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_oversize_inline_message_is_refused_with_the_limit_named() {
    let _guard = env_lock();
    let backend = FakeBackend::with(vec![task("t-1", "ctx-1", TaskState::Working)]);
    let sink = Arc::new(RecordingSink::default());
    let base = serve(backend, sink).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));

    let server = mcp_server(port).await;
    let client = mcp_client(server).await;

    let cap = kubeadp_agent::core::a2a::client::MAX_INGRESS_TEXT_BYTES;
    let over = "x".repeat(cap + 1);
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": over })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("call send_message with an oversize inline message");
    assert_eq!(
        res.is_error,
        Some(true),
        "an oversize message must be refused"
    );
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(
        text.contains("too large"),
        "the refusal names the size: {text}"
    );
    assert!(
        text.contains(&cap.to_string()),
        "the error names the byte limit: {text}"
    );
    // Nothing was sent (the refusal happens before any network round trip).
    // A second, valid send shows the connection still works.
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "still alive", "task_id": "t-1" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("send after the refusal");
    assert_eq!(res.is_error, Some(false));

    std::env::remove_var("A2A_URL_OVERRIDES");
}

/// The send result tells the sender how the message was accepted:
/// `delivery` is `injected` (live input channel) for a working task and
/// `queued` (durable inbox) for a task whose peer reports the submitted
/// state; a terminal/unknown target refuses the send with the peer's reason.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn mcp_a2a_delivery_field_and_terminal_refusal() {
    let _guard = env_lock();
    let backend = FakeBackend::with(vec![
        task("t-running", "ctx-a", TaskState::Working),
        task("t-queued", "ctx-b", TaskState::Submitted),
    ]);
    let base = serve(backend, Arc::new(RecordingSink::default())).await;
    let port: u64 = base.rsplit(':').next().unwrap().parse().unwrap();
    std::env::set_var("A2A_URL_OVERRIDES", format!("peer={base}"));

    let server = mcp_server(port).await;
    let client = mcp_client(server).await;

    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "hi", "task_id": "t-running" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("send to a working task");
    let val: serde_json::Value =
        serde_json::from_str(&res.content[0].as_text().unwrap().text).unwrap();
    assert_eq!(val["delivery"], "injected", "live input channel: {val}");

    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "hi", "task_id": "t-queued" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("send to a submitted task");
    let val: serde_json::Value =
        serde_json::from_str(&res.content[0].as_text().unwrap().text).unwrap();
    assert_eq!(val["delivery"], "queued", "durable inbox: {val}");

    // A terminal/unknown target: the send fails with the peer's reason (the
    // message was not delivered and will never be).
    let res = client
        .call_tool(
            CallToolRequestParams::new("send_message").with_arguments(
                serde_json::json!({ "agent": "peer", "text": "hi", "task_id": "gone" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("send to an unknown task");
    assert_eq!(res.is_error, Some(true), "an unknown target must error");
    let text = res.content[0].as_text().unwrap().text.clone();
    assert!(
        text.contains("delivery") && text.contains("terminal"),
        "the terminal refusal is hinted: {text}"
    );

    std::env::remove_var("A2A_URL_OVERRIDES");
}

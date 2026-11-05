//! End-to-end test using the official `a2a-client-lf` SDK client against the
//! SDK-backed A2A server (HL-78). Gated behind the `a2a` feature like the
//! rest of the A2A surface.
#![cfg(feature = "a2a")]

use a2a::{
    A2AError, CancelTaskRequest, GetTaskRequest, ListTasksRequest, Message, Part, Role,
    SendMessageRequest, TaskState,
};
use a2a_client::auth::AuthInterceptor;
use a2a_client::client::A2AClient;
use a2a_client::rest::RestTransport;
use kubeadp_agent::core::a2a::{
    a2a_router, constant_time_eq, A2aModel, A2aTask, AuthVerifier, TaskBackend,
    WELL_KNOWN_AGENT_CARD_PATH,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

/// An in-memory fake backend plus a recorded delivery sink, so the test can
/// observe SendMessage reaching the "session".
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
        if task.state.is_terminal() {
            return Err(kubeadp_agent::core::a2a::A2AError::task_not_cancelable(
                task_id,
            ));
        }
        task.state = TaskState::Canceled;
        Ok(task.clone())
    }
}

/// Test sink recording delivered (task_id, text) pairs.
#[derive(Default)]
struct RecordingSink {
    delivered: Mutex<Vec<(String, String)>>,
}

impl kubeadp_agent::core::a2a::MessageSink for RecordingSink {
    fn deliver(
        &self,
        task_id: &str,
        _sender: &str,
        text: &str,
        _message_id: &str,
    ) -> Result<(), A2AError> {
        self.delivered
            .lock()
            .unwrap()
            .push((task_id.to_string(), text.to_string()));
        Ok(())
    }
}

fn good_secret() -> &'static str {
    "test-secret"
}

fn verifier_good() -> AuthVerifier {
    let secret = good_secret().as_bytes().to_vec();
    Arc::new(move |headers: &axum::http::HeaderMap| {
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.strip_prefix("Bearer ")
                    .is_some_and(|t| constant_time_eq(t.as_bytes(), &secret))
            })
    })
}

/// Start the SDK A2A server on an ephemeral port; return the REST base URL.
async fn serve(
    backend: Arc<FakeBackend>,
    sink: Arc<RecordingSink>,
    auth: Option<AuthVerifier>,
) -> (String, tokio::task::JoinHandle<()>) {
    let model = A2aModel {
        name: "client-test-agent".to_string(),
        description: "HL-78 client test".to_string(),
        version: "0.0.0".to_string(),
        skills: vec!["proj-a".to_string()],
        url: "http://127.0.0.1/unused".to_string(),
    };
    let app = a2a_router(model, backend, Some(sink), auth);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

/// Build an authenticated SDK client bound to the server's REST endpoint.
fn client(endpoint: &str) -> A2AClient<RestTransport> {
    let http = reqwest::Client::new();
    let transport = RestTransport::new(http, endpoint.trim_end_matches('/').to_string());
    A2AClient::new(transport)
        .with_interceptors(vec![Arc::new(AuthInterceptor::bearer(good_secret()))])
}

fn task(id: &str, context: &str, state: TaskState) -> A2aTask {
    A2aTask {
        id: id.to_string(),
        context_id: Some(context.to_string()),
        state,
        status_message: None,
        error: None,
        artifacts: None,
    }
}

#[tokio::test]
async fn sdk_client_get_task() {
    let backend = FakeBackend::with(vec![task("t1", "c1", TaskState::Working)]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink, Some(verifier_good())).await;

    let cli = client(&base);
    let got = cli
        .get_task(&GetTaskRequest {
            id: "t1".to_string(),
            history_length: None,
            tenant: None,
        })
        .await
        .expect("get_task");
    assert_eq!(got.id, "t1");
    assert_eq!(got.context_id, "c1");
    assert_eq!(got.status.state, TaskState::Working);

    handle.abort();
}

#[tokio::test]
async fn sdk_client_list_tasks() {
    let backend = FakeBackend::with(vec![
        task("t1", "c1", TaskState::Working),
        task("t2", "c1", TaskState::Completed),
        task("t3", "c2", TaskState::Working),
    ]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink, Some(verifier_good())).await;

    let cli = client(&base);
    let all = cli
        .list_tasks(&ListTasksRequest {
            context_id: None,
            status: None,
            page_size: None,
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
            tenant: None,
        })
        .await
        .expect("list_tasks");
    assert_eq!(all.tasks.len(), 3);

    // contextId filter
    let c1 = cli
        .list_tasks(&ListTasksRequest {
            context_id: Some("c1".to_string()),
            status: None,
            page_size: None,
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
            tenant: None,
        })
        .await
        .expect("list_tasks c1");
    assert_eq!(c1.tasks.len(), 2);

    handle.abort();
}

#[tokio::test]
async fn sdk_client_cancel_task() {
    let backend = FakeBackend::with(vec![task("t1", "c1", TaskState::Working)]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink, Some(verifier_good())).await;

    let cli = client(&base);
    let canceled = cli
        .cancel_task(&CancelTaskRequest {
            id: "t1".to_string(),
            metadata: None,
            tenant: None,
        })
        .await
        .expect("cancel_task");
    assert_eq!(canceled.status.state, TaskState::Canceled);

    // Canceling a terminal task -> TaskNotCancelable surfaced from the SDK
    // client as an A2AError with the JSON-RPC code -32002. The client maps the
    // ErrorInfo reason (TASK_NOT_CANCELABLE) back to the canonical code.
    let err = cli
        .cancel_task(&CancelTaskRequest {
            id: "t1".to_string(),
            metadata: None,
            tenant: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, -32002); // TASK_NOT_CANCELABLE;

    handle.abort();
}

#[tokio::test]
async fn sdk_client_send_message_delivers_to_sink() {
    let backend = FakeBackend::with(vec![task("sess-1", "sess-1", TaskState::Working)]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink.clone(), Some(verifier_good())).await;

    let cli = client(&base);
    let req = SendMessageRequest {
        message: Message::new(Role::User, vec![Part::text("hello server")]),
        configuration: None,
        metadata: None,
        tenant: None,
    };
    // Address the long-lived session via the task id on the message.
    let mut req = req;
    req.message.task_id = Some("sess-1".to_string());

    let resp = cli.send_message(&req).await.expect("send_message");
    match resp {
        a2a::SendMessageResponse::Task(t) => {
            assert_eq!(t.id, "sess-1");
            assert_eq!(t.status.state, TaskState::Working);
        }
        a2a::SendMessageResponse::Message(_) => panic!("expected Task response"),
    }

    {
        let delivered = sink.delivered.lock().unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].0, "sess-1");
        assert!(delivered[0].1.contains("hello server"));
        assert!(delivered[0].1.contains("claimed sender: unknown, hop: 0"));
    }

    // Session keeps running: GetTask still reports Working.
    let got = cli
        .get_task(&GetTaskRequest {
            id: "sess-1".to_string(),
            history_length: None,
            tenant: None,
        })
        .await
        .unwrap();
    assert_eq!(got.status.state, TaskState::Working);

    handle.abort();
}

#[tokio::test]
async fn sdk_client_unauthenticated_is_rejected() {
    let backend = FakeBackend::with(vec![task("t1", "c1", TaskState::Working)]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink, Some(verifier_good())).await;

    // No auth interceptor.
    let http = reqwest::Client::new();
    let transport = RestTransport::new(http, base.trim_end_matches('/').to_string());
    let cli = A2AClient::new(transport);

    let err = cli
        .get_task(&GetTaskRequest {
            id: "t1".to_string(),
            history_length: None,
            tenant: None,
        })
        .await
        .unwrap_err();
    // The server authorizer answers INVALID_REQUEST; the SDK maps the REST
    // error envelope to an A2AError with code -32600.
    assert_eq!(err.code, -32600); // INVALID_REQUEST

    handle.abort();
}

#[tokio::test]
async fn sdk_client_streaming_is_unsupported() {
    let backend = FakeBackend::with(vec![]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink, Some(verifier_good())).await;

    let cli = client(&base);
    let req = SendMessageRequest {
        message: Message::new(Role::User, vec![Part::text("hi")]),
        configuration: None,
        metadata: None,
        tenant: None,
    };
    let err = match cli.send_streaming_message(&req).await {
        Ok(_) => panic!("streaming must be unsupported"),
        Err(e) => e,
    };
    assert_eq!(err.code, -32004); // UNSUPPORTED_OPERATION

    handle.abort();
}

#[tokio::test]
async fn sdk_client_card_is_served_without_auth() {
    let backend = FakeBackend::with(vec![]);
    let sink = Arc::new(RecordingSink::default());
    let (base, handle) = serve(backend, sink, Some(verifier_good())).await;

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{base}{WELL_KNOWN_AGENT_CARD_PATH}"))
        .send()
        .await
        .expect("card request");
    assert!(resp.status().is_success());
    let card: HashMap<String, serde_json::Value> = resp.json().await.expect("card json");
    assert_eq!(card["name"], "client-test-agent");
    assert_eq!(card["version"], "0.0.0");

    handle.abort();
}

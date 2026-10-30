//! Feature-gated integration tests for the A2A module (`--features a2a`).
#![cfg(feature = "a2a")]

use a2a::TaskState;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::Router;
use kubeadp_agent::core::a2a::{
    a2a_router, constant_time_eq, map_job_status, A2aModel, A2aTask, AuthVerifier, TaskBackend,
    WELL_KNOWN_AGENT_CARD_PATH,
};
use kubeadp_agent::core::job::JobStatus;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

/// An in-memory fake backend used by the binding tests.
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

fn test_model() -> A2aModel {
    A2aModel {
        name: "test-agent".to_string(),
        description: "A test agent".to_string(),
        version: "1.0.0".to_string(),
        skills: vec!["proj-a".to_string(), "proj-b".to_string()],
        json_rpc_url: "http://localhost:8080".to_string(),
    }
}

fn verifier_ok() -> AuthVerifier {
    Arc::new(|headers: &HeaderMap| {
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.strip_prefix("Bearer ")
                    .is_some_and(|t| constant_time_eq(t.as_bytes(), b"good-token"))
            })
    })
}

fn test_router(backend: Arc<dyn TaskBackend>, auth: Option<AuthVerifier>) -> Router {
    a2a_router(test_model(), backend, auth)
}

async fn body_string(body: Body) -> String {
    let bytes = http_body_util::BodyExt::collect(body)
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8_lossy(&bytes).to_string()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn get_auth(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("authorization", "Bearer good-token")
        .body(Body::empty())
        .expect("request")
}

fn post_auth(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", "Bearer good-token")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

// ---------------------------------------------------------------------------
// Agent card
// ---------------------------------------------------------------------------

#[tokio::test]
async fn card_is_served_without_auth_and_has_expected_content() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));

    let res = app
        .clone()
        .oneshot(get(WELL_KNOWN_AGENT_CARD_PATH))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_str(&body_string(res.into_body()).await).unwrap();

    assert_eq!(value["name"], "test-agent");
    assert_eq!(value["description"], "A test agent");
    assert_eq!(value["version"], "1.0.0");
    // capabilities.streaming = true
    assert_eq!(value["capabilities"]["streaming"], true);
    // skills = caller-passed project/agent names
    let skills = value["skills"].as_array().unwrap();
    assert_eq!(skills.len(), 2);
    assert_eq!(skills[0]["name"], "proj-a");
    assert_eq!(skills[1]["name"], "proj-b");
    // securitySchemes carries a bearer scheme (nested under the SDK's
    // httpAuthSecurityScheme discriminator, matching the A2A wire shape).
    assert_eq!(
        value["securitySchemes"]["bearer"]["httpAuthSecurityScheme"]["scheme"],
        "bearer"
    );
    // supportedInterfaces advertises the JSON-RPC binding
    assert_eq!(
        value["supportedInterfaces"][0]["url"],
        "http://localhost:8080"
    );
    assert_eq!(
        value["supportedInterfaces"][0]["protocolBinding"],
        "JSONRPC"
    );
}

#[tokio::test]
async fn card_requires_no_token_even_when_auth_configured() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));
    let res = app.oneshot(get(WELL_KNOWN_AGENT_CARD_PATH)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Auth on the non-card routes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn protected_routes_require_bearer_token() {
    let task = A2aTask {
        id: "t1".to_string(),
        context_id: Some("c1".to_string()),
        state: TaskState::Completed,
        status_message: None,
        error: None,
        artifacts: None,
    };
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![task]);
    let app = test_router(backend, Some(verifier_ok()));

    // No auth header on the JSON-RPC binding -> 401 JSON.
    let req = Request::builder()
        .method("POST")
        .uri("/rpc")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"t1"}}"#.to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let value: serde_json::Value =
        serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(value["code"], "unauthorized");
    assert!(value["error"].is_string());

    // No auth header on a REST route -> 401.
    let res = app.clone().oneshot(get("/tasks")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let value: serde_json::Value =
        serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(value["code"], "unauthorized");

    // Wrong token -> 401.
    let req = Request::builder()
        .uri("/tasks")
        .header("authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// GetTask / ListTasks / CancelTask through BOTH bindings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_task_through_rest_and_jsonrpc() {
    let task = A2aTask {
        id: "t1".to_string(),
        context_id: Some("c1".to_string()),
        state: TaskState::Completed,
        status_message: Some("done".to_string()),
        error: None,
        artifacts: None,
    };
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![task]);
    let app = test_router(backend, Some(verifier_ok()));

    // REST GET /tasks/<id>
    let res = app.clone().oneshot(get_auth("/tasks/t1")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["id"], "t1");
    assert_eq!(v["contextId"], "c1");
    assert_eq!(v["status"]["state"], "TASK_STATE_COMPLETED");

    // JSON-RPC GetTask
    let body = r#"{"jsonrpc":"2.0","id":"req-1","method":"GetTask","params":{"id":"t1"}}"#;
    let res = app.clone().oneshot(post_auth("/rpc", body)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], "req-1");
    assert_eq!(v["result"]["id"], "t1");
    assert_eq!(v["result"]["status"]["state"], "TASK_STATE_COMPLETED");
}

#[tokio::test]
async fn list_tasks_through_rest_and_jsonrpc_with_filters() {
    let tasks = vec![
        A2aTask {
            id: "t1".to_string(),
            context_id: Some("c1".to_string()),
            state: TaskState::Completed,
            status_message: None,
            error: None,
            artifacts: None,
        },
        A2aTask {
            id: "t2".to_string(),
            context_id: Some("c2".to_string()),
            state: TaskState::Working,
            status_message: None,
            error: None,
            artifacts: None,
        },
    ];
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(tasks);
    let app = test_router(backend, Some(verifier_ok()));

    // REST GET /tasks?contextId=c1
    let res = app
        .clone()
        .oneshot(get_auth("/tasks?contextId=c1"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    let list = v["tasks"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], "t1");
    assert_eq!(v["totalSize"], 1);

    // REST GET /tasks?status=working
    let res = app
        .clone()
        .oneshot(get_auth("/tasks?status=working"))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    let list = v["tasks"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], "t2");

    // JSON-RPC ListTasks filters by contextId
    let body = r#"{"jsonrpc":"2.0","id":2,"method":"ListTasks","params":{"contextId":"c1"}}"#;
    let res = app.clone().oneshot(post_auth("/rpc", body)).await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    let list = v["result"]["tasks"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], "t1");
}

#[tokio::test]
async fn cancel_task_through_rest_and_jsonrpc() {
    let tasks = vec![
        A2aTask {
            id: "t1".to_string(),
            context_id: Some("c1".to_string()),
            state: TaskState::Working,
            status_message: None,
            error: None,
            artifacts: None,
        },
        A2aTask {
            id: "t2".to_string(),
            context_id: Some("c2".to_string()),
            state: TaskState::Completed,
            status_message: None,
            error: None,
            artifacts: None,
        },
    ];
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(tasks);
    let app = test_router(backend, Some(verifier_ok()));

    // REST POST /tasks/t1:cancel
    let res = app
        .clone()
        .oneshot(post_auth("/tasks/t1:cancel", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["status"]["state"], "TASK_STATE_CANCELED");

    // Cancel again (already canceled) -> taskNotCancelable (400), never 404.
    let res = app
        .clone()
        .oneshot(post_auth("/tasks/t1:cancel", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["code"], "taskNotCancelable");

    // REST legacy POST /tasks/t1/cancel (already canceled) -> taskNotCancelable.
    let res = app
        .clone()
        .oneshot(post_auth("/tasks/t1/cancel", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["code"], "taskNotCancelable");

    // JSON-RPC CancelTask on a completed task -> TaskNotCancelable error
    let body = r#"{"jsonrpc":"2.0","id":3,"method":"CancelTask","params":{"id":"t2"}}"#;
    let res = app.clone().oneshot(post_auth("/rpc", body)).await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert!(v["error"].is_object());
    assert_eq!(v["error"]["code"], -32002); // TASK_NOT_CANCELABLE
}

// ---------------------------------------------------------------------------
// Unknown task -> A2A TaskNotFound (404 for REST, code in JSON-RPC)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_task_returns_task_not_found() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));

    // REST: 404 with the A2A error code.
    let res = app.clone().oneshot(get_auth("/tasks/nope")).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["code"], "taskNotFound");

    // JSON-RPC: TASK_NOT_FOUND (-32001) error object.
    let body = r#"{"jsonrpc":"2.0","id":4,"method":"GetTask","params":{"id":"nope"}}"#;
    let res = app.clone().oneshot(post_auth("/rpc", body)).await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["error"]["code"], -32001);
}

// ---------------------------------------------------------------------------
// 501 stubs for the not-yet-implemented methods (never 404)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn send_message_rest_is_501_unsupported() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));

    // JSON-RPC method names surface as UNSUPPORTED_OPERATION.
    let body = r#"{"jsonrpc":"2.0","id":5,"method":"SendMessage","params":{}}"#;
    let res = app.clone().oneshot(post_auth("/rpc", body)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["error"]["code"], -32004); // UNSUPPORTED_OPERATION
    assert_eq!(v["error"]["data"]["code"], "unsupported");

    // REST -> HTTP 501 with code "unsupported".
    let res = app
        .clone()
        .oneshot(post_auth("/message:send", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["code"], "unsupported");
}

#[tokio::test]
async fn subscribe_streaming_push_and_extended_card_are_501() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));

    let cases: Vec<(&str, &str)> = vec![
        // (REST path, HTTP method)
        ("/message:stream", "POST"),
        ("/message/send", "POST"),
        ("/message/stream", "POST"),
        ("/extendedAgentCard", "GET"),
        ("/agent-card/extended", "GET"),
        ("/tasks/t1/subscribe", "GET"),
        ("/tasks/t1:subscribe", "GET"),
        ("/tasks/t1/pushNotificationConfigs", "GET"),
        ("/tasks/t1/pushNotificationConfigs/push-1", "GET"),
    ];
    for (path, method) in cases {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", "Bearer good-token")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED, "path {path}");
        let v: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(v["code"], "unsupported", "path {path}");
    }

    // JSON-RPC SubscribeToTask and push config methods answer UNSUPPORTED.
    for method in [
        "SendStreamingMessage",
        "SubscribeToTask",
        "CreateTaskPushNotificationConfig",
        "GetTaskPushNotificationConfig",
        "ListTaskPushNotificationConfigs",
        "DeleteTaskPushNotificationConfig",
        "GetExtendedAgentCard",
    ] {
        let body = format!(r#"{{"jsonrpc":"2.0","id":6,"method":"{method}","params":{{}}}}"#);
        let res = app.clone().oneshot(post_auth("/rpc", &body)).await.unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&body_string(res.into_body()).await).unwrap();
        assert_eq!(v["error"]["code"], -32004, "method {method}");
        assert_eq!(v["error"]["data"]["code"], "unsupported", "method {method}");
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC envelope robustness (SDK-compatible wire format)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jsonrpc_malformed_body_is_parse_error_in_envelope() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));
    let res = app
        .clone()
        .oneshot(post_auth("/rpc", "{not json"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["error"]["code"], -32700); // PARSE_ERROR
}

#[tokio::test]
async fn jsonrpc_wrong_version_is_invalid_request() {
    let backend: Arc<dyn TaskBackend> = FakeBackend::with(vec![]);
    let app = test_router(backend, Some(verifier_ok()));
    let body = r#"{"jsonrpc":"1.0","id":7,"method":"GetTask","params":{"id":"t1"}}"#;
    let res = app.oneshot(post_auth("/rpc", body)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res.into_body()).await).unwrap();
    assert_eq!(v["error"]["code"], -32600); // INVALID_REQUEST
}

// ---------------------------------------------------------------------------
// State mapping table
// ---------------------------------------------------------------------------

#[test]
fn job_status_to_a2a_state_mapping_table() {
    let cases: Vec<(JobStatus, TaskState)> = vec![
        (JobStatus::Running, TaskState::Working),
        (JobStatus::Done, TaskState::Completed),
        (JobStatus::Failed, TaskState::Failed),
        (JobStatus::Incomplete, TaskState::Failed),
        (JobStatus::QuotaExhausted, TaskState::Failed),
        (JobStatus::SelfTerminated, TaskState::Canceled),
        (JobStatus::Killed, TaskState::Canceled),
    ];
    for (job, expected) in cases {
        assert_eq!(map_job_status(job), expected, "mapping {job:?}");
    }
}

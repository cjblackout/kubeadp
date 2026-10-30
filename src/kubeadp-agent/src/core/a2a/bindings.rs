//! JSON-RPC and HTTP+JSON (REST) bindings over the [`TaskBackend`] trait.
//!
//! The JSON-RPC binding follows the official SDK wire format (`a2a-lf`'s
//! `JsonRpcRequest`/`JsonRpcResponse`): one `POST` endpoint routed by the
//! `method` field, JSON-RPC method names `GetTask`/`ListTasks`/`CancelTask`
//! etc., an error carries `code` + `message` (+ `data`) and HTTP stays 200.
//! The REST binding follows the A2A spec paths, including the `:cancel` /
//! `:subscribe` action suffixes. `SendMessage`, `SendStreamingMessage`,
//! `SubscribeToTask`, push-notification config and `GetExtendedAgentCard`
//! all answer `unsupported` (HTTP 501 for REST), never 404.

use a2a::jsonrpc::{methods, JsonRpcError, JsonRpcId, JsonRpcResponse};
use a2a::{CancelTaskRequest, GetTaskRequest, ListTasksRequest, Task, TaskState};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{A2AError, A2aState};

/// JSON-RPC endpoint path (the official SDK serves it at `/` of its own
/// router; here it is mounted at this fixed path inside the crate's router).
pub const JSONRPC_PATH: &str = "/rpc";

/// A2A JSON-RPC error code for an unsupported method; mirrors the official
/// SDK's `error_code::UNSUPPORTED_OPERATION`.
pub const UNSUPPORTED_OPERATION: i32 = -32004;

/// A2A JSON-RPC error code for a missing task; mirrors the official SDK's
/// `error_code::TASK_NOT_FOUND`.
pub const TASK_NOT_FOUND: i32 = -32001;

/// A2A JSON-RPC error code for a cancel on a non-cancelable task.
pub const TASK_NOT_CANCELABLE: i32 = -32002;

/// A2A JSON-RPC error code for invalid params; standard JSON-RPC.
pub const INVALID_PARAMS: i32 = -32602;

/// The reason string emitted for an unsupported operation, matching the
/// official SDK's `error_reason`.
pub const UNSUPPORTED_OPERATION_REASON: &str = "UNSUPPORTED_OPERATION";

// ---------------------------------------------------------------------------
// JSON-RPC
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct JsonRpcEnvelope {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

/// Parses the JSON-RPC params: the SDK accepts a bare object (or `null`).
/// This crate accepts only the object form (the SDK's own bindings send
/// named params as one object).
fn jsonrpc_params(value: Option<Value>) -> Value {
    value.unwrap_or(Value::Null)
}

pub async fn jsonrpc_handler(State(state): State<A2aState>, body: Bytes) -> Response {
    // Match the official SDK: a malformed body is a JSON-RPC parse error in
    // the envelope (HTTP 200), never an axum 400 rejection. Reuse the SDK's
    // response types for the envelope; the params parse below is validated
    // per-method.
    let envelope: JsonRpcEnvelope = match serde_json::from_slice(&body) {
        Ok(e) => e,
        Err(e) => {
            return jsonrpc_error_response(
                Value::Null,
                JsonRpcError {
                    code: -32700,
                    message: format!("parse error: {e}"),
                    data: None,
                },
                StatusCode::OK,
            );
        }
    };
    let id = envelope.id.clone();
    if envelope.jsonrpc != "2.0" {
        return jsonrpc_error_response(
            id,
            JsonRpcError {
                code: -32600,
                message: "invalid request: invalid jsonrpc version".to_string(),
                data: None,
            },
            StatusCode::OK,
        );
    }

    let params = jsonrpc_params(envelope.params);
    let result: Result<Value, A2AError> = match envelope.method.as_str() {
        methods::GET_TASK => {
            let req = match serde_json::from_value::<GetTaskRequest>(params) {
                Ok(req) => req,
                Err(e) => {
                    return jsonrpc_error_response(
                        id,
                        a2a_error_to_jsonrpc(&A2AError::invalid_params(e.to_string())),
                        StatusCode::OK,
                    )
                }
            };
            match state.backend.get_task(&req.id) {
                Ok(task) => {
                    let wire: Task = task.into();
                    Ok(serde_json::to_value(wire).unwrap_or_default())
                }
                Err(e) => Err(e),
            }
        }
        methods::LIST_TASKS => {
            let req = match serde_json::from_value::<ListTasksRequest>(params) {
                Ok(req) => req,
                Err(e) => {
                    return jsonrpc_error_response(
                        id,
                        a2a_error_to_jsonrpc(&A2AError::invalid_params(e.to_string())),
                        StatusCode::OK,
                    )
                }
            };
            match state
                .backend
                .list_tasks(req.context_id.as_deref(), req.status.as_ref())
            {
                Ok(tasks) => {
                    let tasks: Vec<Task> = tasks.into_iter().map(Task::from).collect();
                    Ok(serde_json::to_value(json!({
                        "tasks": tasks,
                        "nextPageToken": "",
                        "pageSize": tasks.len(),
                        "totalSize": tasks.len(),
                    }))
                    .unwrap_or_default())
                }
                Err(e) => Err(e),
            }
        }
        methods::CANCEL_TASK => {
            let req = match serde_json::from_value::<CancelTaskRequest>(params) {
                Ok(req) => req,
                Err(e) => {
                    return jsonrpc_error_response(
                        id,
                        a2a_error_to_jsonrpc(&A2AError::invalid_params(e.to_string())),
                        StatusCode::OK,
                    )
                }
            };
            match state.backend.cancel_task(&req.id) {
                Ok(task) => {
                    let wire: Task = task.into();
                    Ok(serde_json::to_value(wire).unwrap_or_default())
                }
                Err(e) => Err(e),
            }
        }
        // Methods this slice deliberately does not implement. They answer a
        // JSON-RPC `UNSUPPORTED_OPERATION` error (HTTP 200, matching the
        // official SDK's JSON-RPC behavior), with the crate's
        // {"error","code"} shape inside `data`.
        methods::SEND_MESSAGE
        | methods::SEND_STREAMING_MESSAGE
        | methods::SUBSCRIBE_TO_TASK
        | methods::CREATE_PUSH_CONFIG
        | methods::GET_PUSH_CONFIG
        | methods::LIST_PUSH_CONFIGS
        | methods::DELETE_PUSH_CONFIG
        | methods::GET_EXTENDED_AGENT_CARD => Err(A2AError::unsupported(&envelope.method)),
        other => Err(A2AError {
            code: "methodNotFound",
            message: format!("method not found: {other}"),
        }),
    };

    match result {
        Ok(value) => {
            let resp = JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: jsonrpc_id(&id),
                result: Some(value),
                error: None,
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            let jsonrpc_err = a2a_error_to_jsonrpc(&e);
            jsonrpc_error_response(id, jsonrpc_err, StatusCode::OK)
        }
    }
}

/// Converts a crate [`A2AError`] to the A2A JSON-RPC error object. The
/// `code` mirrors the official SDK's numeric codes; the `data` object carries
/// the crate's `{error, code}` shape plus the A2A reason string.
fn a2a_error_to_jsonrpc(e: &A2AError) -> JsonRpcError {
    match e.code {
        "taskNotFound" => JsonRpcError {
            code: TASK_NOT_FOUND,
            message: e.message.clone(),
            data: jsonrpc_error_data(e),
        },
        "taskNotCancelable" => JsonRpcError {
            code: TASK_NOT_CANCELABLE,
            message: e.message.clone(),
            data: jsonrpc_error_data(e),
        },
        "unsupported" => JsonRpcError {
            code: UNSUPPORTED_OPERATION,
            message: e.message.clone(),
            data: jsonrpc_error_data(e),
        },
        "invalidParams" => JsonRpcError {
            code: INVALID_PARAMS,
            message: e.message.clone(),
            data: jsonrpc_error_data(e),
        },
        _ => JsonRpcError {
            code: -32603,
            message: e.message.clone(),
            data: jsonrpc_error_data(e),
        },
    }
}

fn jsonrpc_error_data(e: &A2AError) -> Option<Value> {
    Some(json!({
        "error": e.message,
        "code": e.code,
        "reason": UNSUPPORTED_OPERATION_REASON,
    }))
}

fn jsonrpc_error_response(id: Value, error: JsonRpcError, status: StatusCode) -> Response {
    let resp = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id: jsonrpc_id(&id),
        result: None,
        error: Some(error),
    };
    (status, Json(resp)).into_response()
}

/// The SDK's `JsonRpcId` preserves string vs number. Map the raw envelope id
/// through; anything unusual becomes `Null` (matching the SDK's fallback).
fn jsonrpc_id(value: &Value) -> JsonRpcId {
    match value {
        Value::String(s) => JsonRpcId::String(s.clone()),
        Value::Number(n) => n.as_i64().map(JsonRpcId::Number).unwrap_or(JsonRpcId::Null),
        Value::Null => JsonRpcId::Null,
        _ => JsonRpcId::Null,
    }
}

// ---------------------------------------------------------------------------
// REST (HTTP+JSON)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ListTasksQuery {
    #[serde(default, rename = "contextId")]
    pub context_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, rename = "pageSize")]
    pub page_size: Option<i32>,
    #[serde(default, rename = "pageToken")]
    pub page_token: Option<String>,
}

pub async fn rest_list_tasks(
    State(state): State<A2aState>,
    Query(query): Query<ListTasksQuery>,
) -> Response {
    let _ = query.page_token; // echoed back; no backend cursor yet
    let status = match query.status.as_deref() {
        None => None,
        Some(raw) => match parse_task_state(raw) {
            Some(s) => Some(s),
            None => {
                return rest_error_response(&A2AError::invalid_params(format!(
                    "invalid status filter: {raw}"
                )))
            }
        },
    };
    let tasks = match state
        .backend
        .list_tasks(query.context_id.as_deref(), status.as_ref())
    {
        Ok(t) => t,
        Err(e) => return rest_error_response(&e),
    };
    // No backend pagination yet: the requested page size/token are echoed, the
    // full filtered list is returned with an exhausted token.
    let tasks: Vec<Task> = tasks.into_iter().map(Task::from).collect();
    let total = tasks.len();
    let page_size = query.page_size.unwrap_or(total as i32).max(1);
    (
        StatusCode::OK,
        Json(json!({
            "tasks": tasks,
            "nextPageToken": "",
            "pageSize": page_size,
            "totalSize": total,
        })),
    )
        .into_response()
}

pub async fn rest_get_task_or_subscribe(
    State(state): State<A2aState>,
    Path(id): Path<String>,
) -> Response {
    // The `:subscribe` action suffix routes to a 501 stub.
    if let Some(task_id) = id.strip_suffix(":subscribe") {
        let _ = state;
        return rest_error_response(&A2AError::unsupported(&format!(
            "SubscribeToTask({task_id})"
        )));
    }
    rest_get_task(axum::extract::State(state), id).await
}

async fn rest_get_task(State(state): State<A2aState>, id: String) -> Response {
    match state.backend.get_task(&id) {
        Ok(task) => {
            let wire: Task = task.into();
            (
                StatusCode::OK,
                Json(serde_json::to_value(wire).unwrap_or_default()),
            )
                .into_response()
        }
        Err(e) => rest_error_response(&e),
    }
}

pub async fn rest_cancel_task_legacy(
    State(state): State<A2aState>,
    Path(id): Path<String>,
) -> Response {
    rest_cancel_task(axum::extract::State(state), id).await
}

pub async fn rest_task_action(State(state): State<A2aState>, Path(id): Path<String>) -> Response {
    if let Some(task_id) = id.strip_suffix(":cancel") {
        return rest_cancel_task(axum::extract::State(state), task_id.to_string()).await;
    }
    if let Some(task_id) = id.strip_suffix(":subscribe") {
        return rest_error_response(&A2AError::unsupported(&format!(
            "SubscribeToTask({task_id})"
        )));
    }
    rest_error_response(&A2AError::invalid_params("unsupported task action"))
}

async fn rest_cancel_task(State(state): State<A2aState>, id: String) -> Response {
    match state.backend.cancel_task(&id) {
        Ok(task) => {
            let wire: Task = task.into();
            (
                StatusCode::OK,
                Json(serde_json::to_value(wire).unwrap_or_default()),
            )
                .into_response()
        }
        Err(e) => rest_error_response(&e),
    }
}

// 501 stubs ---------------------------------------------------------------

pub async fn rest_send_message_unsupported(State(state): State<A2aState>) -> Response {
    let _ = state;
    rest_error_response(&A2AError::unsupported("SendMessage"))
}

pub async fn rest_stream_message_unsupported(State(state): State<A2aState>) -> Response {
    let _ = state;
    rest_error_response(&A2AError::unsupported("SendStreamingMessage"))
}

pub async fn rest_subscribe_unsupported(
    State(state): State<A2aState>,
    Path(_id): Path<String>,
) -> Response {
    let _ = state;
    rest_error_response(&A2AError::unsupported("SubscribeToTask"))
}

pub async fn rest_extended_card_unsupported(State(state): State<A2aState>) -> Response {
    let _ = state;
    rest_error_response(&A2AError::unsupported("GetExtendedAgentCard"))
}

pub async fn rest_push_configs_unsupported(
    State(state): State<A2aState>,
    Path(_id): Path<String>,
) -> Response {
    let _ = state;
    rest_error_response(&A2AError::unsupported("TaskPushNotificationConfig"))
}

pub async fn rest_push_config_unsupported(
    State(state): State<A2aState>,
    Path(_params): Path<(String, String)>,
) -> Response {
    let _ = state;
    rest_error_response(&A2AError::unsupported("TaskPushNotificationConfig"))
}

/// Parses an A2A task-state string as it appears on the wire (both the
/// canonical `TASK_STATE_*` and the friendly lowercased forms clients use).
fn parse_task_state(s: &str) -> Option<TaskState> {
    match s {
        "TASK_STATE_SUBMITTED" | "submitted" => Some(TaskState::Submitted),
        "TASK_STATE_WORKING" | "working" => Some(TaskState::Working),
        "TASK_STATE_COMPLETED" | "completed" => Some(TaskState::Completed),
        "TASK_STATE_FAILED" | "failed" => Some(TaskState::Failed),
        "TASK_STATE_CANCELED" | "canceled" => Some(TaskState::Canceled),
        "TASK_STATE_INPUT_REQUIRED" | "input-required" => Some(TaskState::InputRequired),
        "TASK_STATE_REJECTED" | "rejected" => Some(TaskState::Rejected),
        "TASK_STATE_AUTH_REQUIRED" | "auth-required" => Some(TaskState::AuthRequired),
        _ => None,
    }
}

/// Serializes the crate's `{"error","code"}` shape (matching the rest of the
/// server module) with the A2A error code; unsupported maps to HTTP 501, a
/// missing task to 404, everything else to 400 (`invalidParams`) / 500.
fn rest_error_response(e: &A2AError) -> Response {
    let status = match e.code {
        "taskNotFound" => StatusCode::NOT_FOUND,
        "unsupported" => StatusCode::NOT_IMPLEMENTED,
        "taskNotCancelable" | "invalidParams" => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": e.message, "code": e.code }))).into_response()
}

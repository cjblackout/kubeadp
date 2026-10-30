//! A2A `RequestHandler` implementation bridging the official `a2a-server-lf`
//! SDK routers to the crate's [`TaskBackend`].
//!
//! The SDK's [`DefaultRequestHandler`](a2a_server::DefaultRequestHandler)
//! mints fresh task ids and drives them through a `TaskStore` + `AgentExecutor`
//! lifecycle. That model does not fit this crate's tasks, which are
//! pre-existing records (supervised sessions / agent-run jobs) owned by the
//! backend:
//!
//! - a long-lived session never reaches the terminal events that
//!   `DefaultRequestHandler`'s non-streaming `send_message` waits for, and
//! - `TaskStore::create` would mint a *new* id instead of addressing the
//!   existing session.
//!
//! So this module implements [`RequestHandler`](a2a_server::RequestHandler)
//! directly over [`TaskBackend`], keeping the GetTask/ListTasks/CancelTask
//! semantics identical to the previous hand-written bindings, and wiring
//! `SendMessage` to a caller-supplied [`MessageSink`] (the orchestrator
//! delivers into the session PTY as a bracketed paste).
//!
//! Each method first runs the optional bearer verifier (the SDK's
//! `RequestAuthorizer` equivalent), then delegates to the backend; the card
//! route is handled separately by the SDK's public `agent_card_router` and is
//! never authorized.
//!
//! `SubscribeToTask`, the push-notification config methods and
//! `GetExtendedAgentCard` answer the SDK's dedicated errors: the REST binding
//! maps them per `A2AError::http_status_code` and the JSON-RPC binding to the
//! matching numeric codes (`UNSUPPORTED_OPERATION` -32004,
//! `PUSH_NOTIFICATION_NOT_SUPPORTED` -32003, `EXTENDED_CARD_NOT_CONFIGURED`
//! -32007).

use std::sync::Arc;

use a2a::{
    A2AError, AgentCard, CancelTaskRequest, DeleteTaskPushNotificationConfigRequest,
    GetExtendedAgentCardRequest, GetTaskPushNotificationConfigRequest, GetTaskRequest,
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    ListTasksRequest, ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task, TaskPushNotificationConfig,
};
use a2a_server::{RequestHandler, ServiceParams};
use async_trait::async_trait;
use futures::stream::BoxStream;

use super::backend::{A2AError as CrateError, TaskBackend};
use crate::core::a2a::AuthVerifier;

/// Converts a crate [`CrateError`] into the SDK's A2A error.
fn to_sdk_error(e: CrateError) -> A2AError {
    use a2a::error_code;
    match e.code {
        "taskNotFound" => A2AError::new(error_code::TASK_NOT_FOUND, e.message),
        "taskNotCancelable" => A2AError::new(error_code::TASK_NOT_CANCELABLE, e.message),
        "unsupported" => A2AError::new(error_code::UNSUPPORTED_OPERATION, e.message),
        "invalidParams" => A2AError::new(error_code::INVALID_PARAMS, e.message),
        _ => A2AError::internal(e.message),
    }
}

/// Destination for a `SendMessage` payload.
///
/// The orchestrator delivers into the live session PTY (bracketed paste);
/// the worker has no interactive input channel for a running `agy` process
/// and installs no sink, so its `SendMessage` answers `UNSUPPORTED_OPERATION`.
pub trait MessageSink: Send + Sync {
    /// Deliver `text` to the task (session) identified by `task_id`.
    ///
    /// Returning `Err` means the text was NOT delivered (e.g. the session has
    /// no live PTY or the message budget is exhausted).
    fn deliver(&self, task_id: &str, text: &str) -> Result<(), A2AError>;
}
/// A2A request handler directly over a [`TaskBackend`] and an optional
/// [`MessageSink`].
pub struct ServerRequestHandler {
    backend: Arc<dyn TaskBackend>,
    sink: Option<Arc<dyn MessageSink>>,
    verifier: Option<AuthVerifier>,
}

impl ServerRequestHandler {
    pub fn new(
        backend: Arc<dyn TaskBackend>,
        sink: Option<Arc<dyn MessageSink>>,
        verifier: Option<AuthVerifier>,
    ) -> Self {
        Self {
            backend,
            sink,
            verifier,
        }
    }

    fn authorize(&self, params: &ServiceParams) -> Result<(), A2AError> {
        let Some(verifier) = &self.verifier else {
            return Ok(());
        };
        // The SDK's ServiceParams is the request's header map keyed
        // lowercased; reconstruct a HeaderMap so the caller's verifier (built
        // for axum) keeps working unmodified.
        let mut headers = axum::http::HeaderMap::new();
        for (name, values) in params {
            let Ok(header_name) = axum::http::header::HeaderName::from_bytes(name.as_bytes())
            else {
                continue;
            };
            for value in values {
                if let Ok(header_value) = axum::http::header::HeaderValue::from_str(value) {
                    headers.append(header_name.clone(), header_value);
                }
            }
        }
        if verifier(&headers) {
            Ok(())
        } else {
            Err(A2AError::invalid_request("authentication required"))
        }
    }
}

fn task_from_backend(backend: &Arc<dyn TaskBackend>, id: &str) -> Result<Task, A2AError> {
    backend.get_task(id).map(Task::from).map_err(to_sdk_error)
}

#[async_trait]
impl RequestHandler for ServerRequestHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        self.authorize(params)?;
        // This role either has an interactive input channel (successful
        // delivery) or it has none at all (SendMessage is unsupported),
        // independent of the message shape, so check capability first.
        if self.sink.is_none() {
            return Err(A2AError::unsupported_operation(
                "SendMessage is not supported by this agent role (no interactive input channel)"
                    .to_string(),
            ));
        }
        // Resolve the addressed task id: SendMessage may name the task in the
        // message or arrive on an existing context.
        let task_id = req
            .message
            .task_id
            .as_deref()
            .or(req.message.context_id.as_deref())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| A2AError::invalid_params("message must address a task or context"))?;

        // Existence and state are validated up front so an unknown session or
        // job answers TaskNotFound, never a deliverer-specific error.
        let task = task_from_backend(&self.backend, task_id)?;

        let text = req.message.text().map(str::trim).unwrap_or("").to_string();
        if text.is_empty() {
            return Err(A2AError::invalid_params("message text must not be empty"));
        }

        self.sink
            .as_ref()
            .expect("checked above")
            .deliver(task_id, &text)?;

        // Non-streaming delivery per A2A §7.4.2: return the (long-lived) task
        // in its current state; the session keeps running. The client then
        // observes terminal events via GetTask.
        Ok(SendMessageResponse::Task(task))
    }

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        _req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.authorize(params)?;
        Err(A2AError::unsupported_operation(
            "SendStreamingMessage is not supported: this agent only serves the non-streaming binding",
        ))
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.authorize(params)?;
        let _ = req.history_length; // no history is served
        task_from_backend(&self.backend, &req.id)
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.authorize(params)?;
        let raw: Vec<crate::core::a2a::A2aTask> = self
            .backend
            .list_tasks(req.context_id.as_deref(), req.status.as_ref())
            .map_err(to_sdk_error)?;
        let tasks: Vec<Task> = raw.into_iter().map(Task::from).collect();
        let total = tasks.len() as i32;
        Ok(ListTasksResponse {
            tasks,
            next_page_token: String::new(),
            page_size: total.max(1),
            total_size: total,
        })
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        self.authorize(params)?;
        self.backend
            .cancel_task(&req.id)
            .map(Task::from)
            .map_err(to_sdk_error)
    }

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        _req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.authorize(params)?;
        Err(A2AError::unsupported_operation(
            "SubscribeToTask is not supported by this agent",
        ))
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        _req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.authorize(params)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        _req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.authorize(params)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        _req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.authorize(params)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        _req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.authorize(params)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        _req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        self.authorize(params)?;
        Err(A2AError::extended_card_not_configured())
    }
}

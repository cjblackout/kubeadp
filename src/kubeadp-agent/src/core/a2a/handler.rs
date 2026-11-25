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
//! Authentication is enforced by the router-level
//! [`super::auth::A2aAuthLayer`] (installed on the SDK REST sub-router by
//! [`super::a2a_router`] / [`super::a2a_router_with_jobset`]): it runs BEFORE
//! the SDK parses or validates the body, answers a failed check with HTTP `401`
//! (`UNAUTHENTICATED`), and is never applied to the public agent card route
//! (served separately by the SDK's `agent_card_router`). The methods below
//! therefore assume an already-authorized request and delegate to the backend.
//!
//! `SubscribeToTask`, the push-notification config methods and
//! `GetExtendedAgentCard` answer the SDK's dedicated errors: the REST binding
//! maps them per `A2AError::http_status_code` (`UNSUPPORTED_OPERATION`,
//! `PUSH_NOTIFICATION_NOT_SUPPORTED`, `EXTENDED_CARD_NOT_CONFIGURED`).

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
#[cfg(feature = "a2a")]
use super::ingress::process_ingress;

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

/// Builds the A2A error for a rate-limited (too-often) `SendMessage`. The
/// SDK's `A2AError` has no dedicated rate-limit variant / 429 status, so a
/// throttled delivery is reported as `INVALID_PARAMS` with an explicit
/// rate-limit message (the SDK maps `INVALID_PARAMS` to HTTP 400).
pub fn a2a_rate_limited(msg: impl Into<String>) -> A2AError {
    A2AError::invalid_params(msg)
}

/// Trusted delivery metadata the ingress extracted from a `SendMessage`
/// request (design HL-113 §7.14, phase 2): the receiver-verified hop count
/// and the `interrupt: true` escalation request. Never derived from the
/// framed content.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeliveryMeta {
    /// The hop count of the incoming message (0..=MAX_HOP).
    pub hop: u32,
    /// Whether the message requests the explicit `interrupt` escalation
    /// (kill-and-resume) instead of waiting for a boundary.
    pub interrupt: bool,
}

/// Destination for a `SendMessage` payload.
///
/// The orchestrator delivers into the live session PTY (bracketed paste); the
/// worker resumes the addressed job's conversation with the framed text as the
/// next turn (interrupt-and-resume). The handler runs `deliver` on the tokio
/// blocking pool (a PTY write or a job interrupt/resume is blocking), so it
/// never occupies a worker thread.
pub trait MessageSink: Send + Sync {
    /// Deliver with the trusted delivery metadata the ingress extracted: the
    /// verified hop count and the `interrupt: true` request (design §7.14).
    /// Never derived from the framed content. `message_id` is the A2A message
    /// id; a sink that deduplicates on it applies a repeated id only once.
    /// Returning `Err` means the text was NOT delivered (the task is gone,
    /// killed by the operator, the budget exhausted, or rate-limited).
    fn deliver_with_meta(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
        meta: DeliveryMeta,
    ) -> Result<(), A2AError>;

    /// Deliver without the trusted metadata (zero hop, no interrupt); the
    /// default delegates to [`MessageSink::deliver_with_meta`].
    fn deliver(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
    ) -> Result<(), A2AError> {
        self.deliver_with_meta(task_id, sender, text, message_id, DeliveryMeta::default())
    }
}

/// A2A request handler directly over a [`TaskBackend`] and an optional
/// [`MessageSink`].
///
/// Authentication (operator/bearer + per-jobset credential) is enforced by the
/// router-level [`super::auth::A2aAuthLayer`] installed by
/// [`super::a2a_router`] / [`super::a2a_router_with_jobset`] on the SDK REST
/// sub-router. The layer runs before the SDK parses or validates the request
/// body and answers a failed check with HTTP `401` (`UNAUTHENTICATED`). The
/// handler methods below therefore assume the request is already authorized
/// and no longer re-check credentials.
pub struct ServerRequestHandler {
    backend: Arc<dyn TaskBackend>,
    sink: Option<Arc<dyn MessageSink>>,
}

impl ServerRequestHandler {
    pub fn new(backend: Arc<dyn TaskBackend>, sink: Option<Arc<dyn MessageSink>>) -> Self {
        Self { backend, sink }
    }
}

/// Maximum page size allowed for ListTasks requests.
pub const MAX_PAGE_SIZE: i32 = 100;
/// Default page size for ListTasks requests when unspecified.
pub const DEFAULT_PAGE_SIZE: i32 = 50;

fn task_from_backend(backend: &Arc<dyn TaskBackend>, id: &str) -> Result<Task, A2AError> {
    backend.get_task(id).map(Task::from).map_err(to_sdk_error)
}

async fn task_from_backend_blocking(
    backend: Arc<dyn TaskBackend>,
    id: String,
) -> Result<Task, A2AError> {
    tokio::task::spawn_blocking(move || task_from_backend(&backend, &id))
        .await
        .expect("spawn_blocking task panicked or runtime shut down")
}

#[async_trait]
impl RequestHandler for ServerRequestHandler {
    async fn send_message(
        &self,
        _params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        // This role either has an interactive input channel (successful
        // delivery) or it has none at all (SendMessage is unsupported),
        // independent of the message shape, so check capability first and
        // answer the unsupported error BEFORE any addressing/shape validation.
        if self.sink.is_none() {
            return Err(A2AError::unsupported_operation(
                "SendMessage is not supported by this agent role (no interactive input channel)"
                    .to_string(),
            ));
        }

        // A sink exists: resolve the addressed task id. SendMessage may name
        // the task in the message or arrive on an existing context.
        let task_id = req
            .message
            .task_id
            .as_deref()
            .or(req.message.context_id.as_deref())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| A2AError::invalid_params("message must address a task or context"))?;

        // Existence and state are validated up front so an unknown session or
        // job answers TaskNotFound, never a deliverer-specific error.
        let task = task_from_backend_blocking(self.backend.clone(), task_id.to_string()).await?;

        #[cfg(feature = "a2a")]
        let (sender, text_to_deliver, delivery_meta) = {
            // Validates the WHOLE message (all parts: empty, size and hop) and
            // frames it with a per-message nonce delimiter. For a
            // jobset-authenticated send the auth layer has already verified
            // the signed hop token and rewritten the metadata, so the hop
            // here is the receiver-verified one, never the sender's claimed
            // `a2a.hop` integer; the interrupt escalation request is the
            // operator's only (stripped by the layer for any other sender).
            let ingress = process_ingress(&req.message.parts, req.message.metadata.as_ref())?;
            let meta = DeliveryMeta {
                hop: ingress.hop,
                interrupt: ingress.interrupt,
            };
            (ingress.claimed_sender, ingress.framed_text, meta)
        };
        #[cfg(not(feature = "a2a"))]
        let (sender, text_to_deliver, delivery_meta) = {
            let text = req.message.text().map(str::trim).unwrap_or("").to_string();
            if text.is_empty() {
                return Err(A2AError::invalid_params("message text must not be empty"));
            }
            // No ingress in the non-a2a build, so there is no claimed sender:
            // deliver under the same "unknown" identity ingress would use.
            (String::from("unknown"), text, DeliveryMeta::default())
        };

        // The sink does a (deadline-bounded) blocking PTY write; run it on the
        // tokio blocking pool so a stuck child cannot hold a worker thread (and
        // the pty_input mutex) and starve /healthz.
        let sink = self.sink.as_ref().expect("checked above").clone();
        let delivery_task = task_id.to_string();
        let delivery_sender = sender.clone();
        let delivery_text = text_to_deliver.clone();
        let delivery_message_id = req.message.message_id.clone();
        tokio::task::spawn_blocking(move || {
            sink.deliver_with_meta(
                &delivery_task,
                &delivery_sender,
                &delivery_text,
                &delivery_message_id,
                delivery_meta,
            )
        })
        .await
        .expect("spawn_blocking task panicked or runtime shut down")?;

        // Non-streaming delivery per A2A §7.4.2: return the (long-lived) task
        // in its current state; the session keeps running. The client then
        // observes terminal events via GetTask.
        // SPEC §7.9, §7.14: populate metadata.delivery with "queued" or "injected".
        let mut task = task;
        let delivery = if task.status.state == a2a::TaskState::Submitted {
            "queued"
        } else {
            "injected"
        };
        let meta = task
            .metadata
            .get_or_insert_with(std::collections::HashMap::new);
        meta.insert("delivery".to_string(), serde_json::json!(delivery));

        Ok(SendMessageResponse::Task(task))
    }

    async fn send_streaming_message(
        &self,
        _params: &ServiceParams,
        _req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        Err(A2AError::unsupported_operation(
            "SendStreamingMessage is not supported: this agent only serves the non-streaming binding",
        ))
    }

    async fn get_task(
        &self,
        _params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        let _ = req.history_length; // no history is served
        task_from_backend_blocking(self.backend.clone(), req.id).await
    }

    async fn list_tasks(
        &self,
        _params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        let backend = self.backend.clone();
        let context_id = req.context_id.clone();
        let status = req.status.clone();
        let mut raw: Vec<crate::core::a2a::A2aTask> = tokio::task::spawn_blocking(move || {
            backend.list_tasks(context_id.as_deref(), status.as_ref())
        })
        .await
        .expect("spawn_blocking task panicked or runtime shut down")
        .map_err(to_sdk_error)?;

        // Enforce stable ordering by task id
        raw.sort_by(|a, b| a.id.cmp(&b.id));

        let total_size = raw.len() as i32;

        let limit = req
            .page_size
            .map(|s| s.clamp(1, MAX_PAGE_SIZE))
            .unwrap_or(DEFAULT_PAGE_SIZE) as usize;

        let offset = if let Some(token) = &req.page_token {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                0
            } else {
                trimmed.parse::<usize>().map_err(|_| {
                    A2AError::invalid_params("invalid page_token: expected offset number")
                })?
            }
        } else {
            0
        };

        if offset > raw.len() {
            return Err(A2AError::invalid_params(format!(
                "page_token offset {offset} out of bounds (total {})",
                raw.len()
            )));
        }

        let slice = &raw[offset..];
        let page_items: Vec<Task> = slice.iter().take(limit).cloned().map(Task::from).collect();
        let next_offset = offset + page_items.len();
        let next_page_token = if next_offset < raw.len() {
            next_offset.to_string()
        } else {
            String::new()
        };

        Ok(ListTasksResponse {
            page_size: page_items.len() as i32,
            total_size,
            tasks: page_items,
            next_page_token,
        })
    }

    async fn cancel_task(
        &self,
        _params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        let backend = self.backend.clone();
        let task_id = req.id;
        tokio::task::spawn_blocking(move || {
            backend
                .cancel_task(&task_id)
                .map(Task::from)
                .map_err(to_sdk_error)
        })
        .await
        .expect("spawn_blocking task panicked or runtime shut down")
    }

    async fn subscribe_to_task(
        &self,
        _params: &ServiceParams,
        _req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        Err(A2AError::unsupported_operation(
            "SubscribeToTask is not supported by this agent",
        ))
    }

    async fn create_push_config(
        &self,
        _params: &ServiceParams,
        _req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        Err(A2AError::push_notification_not_supported())
    }

    async fn get_push_config(
        &self,
        _params: &ServiceParams,
        _req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        Err(A2AError::push_notification_not_supported())
    }

    async fn list_push_configs(
        &self,
        _params: &ServiceParams,
        _req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        Err(A2AError::push_notification_not_supported())
    }

    async fn delete_push_config(
        &self,
        _params: &ServiceParams,
        _req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        Err(A2AError::push_notification_not_supported())
    }

    async fn get_extended_agent_card(
        &self,
        _params: &ServiceParams,
        _req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        Err(A2AError::extended_card_not_configured())
    }
}

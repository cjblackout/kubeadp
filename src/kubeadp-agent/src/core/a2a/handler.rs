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

/// Builds the A2A error for a rate-limited (too-often) `SendMessage`. The
/// SDK's `A2AError` has no dedicated rate-limit variant / 429 status, so a
/// throttled delivery is reported as `INVALID_PARAMS` with an explicit
/// rate-limit message (the SDK maps `INVALID_PARAMS` to HTTP 400).
pub fn a2a_rate_limited(msg: impl Into<String>) -> A2AError {
    A2AError::invalid_params(msg)
}

/// Destination for a `SendMessage` payload.
///
/// The orchestrator delivers into the live session PTY (bracketed paste); the
/// worker resumes the addressed job's conversation with the framed text as the
/// next turn (interrupt-and-resume). The handler runs `deliver` on the tokio
/// blocking pool (a PTY write or a job interrupt/resume is blocking), so it
/// never occupies a worker thread.
pub trait MessageSink: Send + Sync {
    /// Deliver `text` to the task (session) identified by `task_id`, charged
    /// to the `sender` (the claimed `a2a.sender`) for the per-(session, sender)
    /// message budget.
    ///
    /// `message_id` is the A2A message id; a sink that deduplicates on it
    /// applies a repeated id only once (idempotency).
    ///
    /// Returning `Err` means the text was NOT delivered (e.g. the task is gone,
    /// killed by the operator, the sender's budget is exhausted, or the delivery
    /// was rate-limited).
    fn deliver(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
    ) -> Result<(), A2AError>;
}

/// A2A request handler directly over a [`TaskBackend`] and an optional
/// [`MessageSink`].
pub struct ServerRequestHandler {
    backend: Arc<dyn TaskBackend>,
    sink: Option<Arc<dyn MessageSink>>,
    verifier: Option<AuthVerifier>,
    jobset: Option<super::JobsetCred>,
}

impl ServerRequestHandler {
    pub fn new(
        backend: Arc<dyn TaskBackend>,
        sink: Option<Arc<dyn MessageSink>>,
        verifier: Option<AuthVerifier>,
    ) -> Self {
        Self::with_jobset(backend, sink, verifier, None)
    }

    /// Builds the handler with both the operator/bearer verifier AND the
    /// per-jobset credential context. The jobset check is target-aware: a
    /// request that presents a jobset credential is verified against the
    /// *target* task/agent of the request (see [`Self::authorize`]), never
    /// against this process's own identity.
    pub fn with_jobset(
        backend: Arc<dyn TaskBackend>,
        sink: Option<Arc<dyn MessageSink>>,
        verifier: Option<AuthVerifier>,
        jobset: Option<super::JobsetCred>,
    ) -> Self {
        Self {
            backend,
            sink,
            verifier,
            jobset,
        }
    }

    /// Reads one header value (case-insensitive) from the SDK's
    /// `ServiceParams` (header names are lowercased there). Multiple values
    /// yield the first.
    fn get_header(params: &ServiceParams, name: &str) -> Option<String> {
        params
            .get(&name.to_ascii_lowercase())
            .and_then(|values| values.first())
            .cloned()
    }

    /// The presented jobset credential header pair, if BOTH the id and the
    /// secret headers are present and non-empty.
    fn presented_jobset_credential(params: &ServiceParams) -> Option<()> {
        let id = Self::get_header(params, crate::core::jobset::JOBSET_ID_HEADER)
            .filter(|s| !s.is_empty());
        let secret = Self::get_header(params, crate::core::jobset::JOBSET_SECRET_HEADER)
            .filter(|s| !s.is_empty());
        match (id, secret) {
            (Some(_), Some(_)) => Some(()),
            _ => None,
        }
    }

    /// Authorizes the request against `target_agent_id` -- the addressed
    /// task/agent of THIS request (the `send_message` recipient, or the task
    /// id on `/tasks/{id}`). `None` is used for target-less operations
    /// (e.g. `list_tasks` without a context), where the presented jobset
    /// credential is accepted when the host serves a member of that jobset.
    ///
    /// The rules:
    ///
    /// - a request that PRESENTS a jobset credential header pair is verified
    ///   against the TARGET's registered jobset (constant-time). A valid
    ///   credential for the target's own jobset is accepted; a credential for
    ///   ANY other jobset, or a target with no registered membership, is
    ///   rejected with NO bearer fallback.
    /// - a request that presents NO jobset credential is accepted only when
    ///   the operator/bearer verifier accepts it (or no verifier is
    ///   configured -- the network-policy-only model).
    fn authorize(
        &self,
        params: &ServiceParams,
        target_agent_id: Option<&str>,
    ) -> Result<(), A2AError> {
        if Self::presented_jobset_credential(params).is_some() {
            let getter: crate::core::jobset::HeaderGetter = {
                let params = params.clone();
                Arc::new(move |name: &str| {
                    params
                        .get(&name.to_ascii_lowercase())
                        .and_then(|values| values.first())
                        .cloned()
                })
            };
            let Some(jobset) = &self.jobset else {
                // No jobset context on this router: a presented jobset
                // credential cannot be verified, so it is refused (never
                // downgraded to the operator token).
                return Err(A2AError::invalid_request("authentication required"));
            };
            let ok = match target_agent_id {
                // Target resolved: the presented id+secret must belong to the
                // TARGET's jobset.
                Some(target) => {
                    crate::core::jobset::verify_jobset_credential(&jobset.registry, target, &getter)
                }
                // No single target (list_tasks without a context): the
                // jobset credential is accepted when the host serves a
                // member of the presented jobset.
                None => jobset.registry.has_member_of(
                    &Self::get_header(params, crate::core::jobset::JOBSET_ID_HEADER)
                        .unwrap_or_default(),
                ),
            };
            if ok {
                return Ok(());
            }
            return Err(A2AError::invalid_request("authentication required"));
        }
        // No jobset credential presented: the operator/bearer gate decides.
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
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        // This role either has an interactive input channel (successful
        // delivery) or it has none at all (SendMessage is unsupported),
        // independent of the message shape, so check capability first and
        // answer the unsupported error BEFORE any addressing/shape validation.
        if self.sink.is_none() {
            self.authorize(params, None)?;
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

        // The A2A credential is verified against the send_message RECIPIENT
        // (the task/session the message is addressed to).
        self.authorize(params, Some(task_id))?;

        // Existence and state are validated up front so an unknown session or
        // job answers TaskNotFound, never a deliverer-specific error.
        let task = task_from_backend_blocking(self.backend.clone(), task_id.to_string()).await?;

        #[cfg(feature = "a2a")]
        let (sender, text_to_deliver) = {
            // Validates the WHOLE message (all parts: empty, size and hop) and
            // frames it with a per-message nonce delimiter.
            let ingress = process_ingress(&req.message.parts, req.message.metadata.as_ref())?;
            (ingress.claimed_sender, ingress.framed_text)
        };
        #[cfg(not(feature = "a2a"))]
        let (sender, text_to_deliver) = {
            let text = req.message.text().map(str::trim).unwrap_or("").to_string();
            if text.is_empty() {
                return Err(A2AError::invalid_params("message text must not be empty"));
            }
            // No ingress in the non-a2a build, so there is no claimed sender:
            // deliver under the same "unknown" identity ingress would use.
            (String::from("unknown"), text)
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
            sink.deliver(
                &delivery_task,
                &delivery_sender,
                &delivery_text,
                &delivery_message_id,
            )
        })
        .await
        .expect("spawn_blocking task panicked or runtime shut down")?;

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
        self.authorize(params, None)?;
        Err(A2AError::unsupported_operation(
            "SendStreamingMessage is not supported: this agent only serves the non-streaming binding",
        ))
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        // The A2A credential is verified against the addressed task.
        self.authorize(params, Some(&req.id))?;
        let _ = req.history_length; // no history is served
        task_from_backend_blocking(self.backend.clone(), req.id).await
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        // No single target: a presented jobset credential is accepted when the
        // host serves a member of the presented jobset; otherwise the bearer
        // gate applies.
        self.authorize(params, req.context_id.as_deref())?;
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
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        // The A2A credential is verified against the addressed task.
        self.authorize(params, Some(&req.id))?;
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
        params: &ServiceParams,
        _req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.authorize(params, None)?;
        Err(A2AError::unsupported_operation(
            "SubscribeToTask is not supported by this agent",
        ))
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        _req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.authorize(params, None)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        _req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.authorize(params, None)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        _req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.authorize(params, None)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        _req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.authorize(params, None)?;
        Err(A2AError::push_notification_not_supported())
    }

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        _req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        self.authorize(params, None)?;
        Err(A2AError::extended_card_not_configured())
    }
}

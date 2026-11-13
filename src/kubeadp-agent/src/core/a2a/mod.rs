//! A2A v1 agent protocol surface (agent card + HTTP+JSON REST binding),
//! served by the official `a2a-server-lf` SDK.
//!
//! This module (behind the crate's `a2a` cargo feature, which itself depends
//! on the `server` feature) provides the server side of the A2A v1 protocol
//! for the agent wrappers. It is mounted by the wrapper roles: the
//! orchestrator merges it into its HTTP server in
//! [`crate::orchestrate::http_server::build_router`], the worker in
//! [`crate::worker::entry`].
//!
//! # SDK decision
//!
//! Since the A2A plan (Vikunja HL-43/HL-56) was written, the official
//! [`a2a-server-lf`](https://docs.rs/a2a-server-lf) SDK
//! (`a2aproject/a2a-rs`, Apache-2.0) has matured past the "not a server
//! framework" caveat: its `rest_router`/`agent_card_router`
//! are plain axum 0.8 routers that merge into ours, and its REST error
//! envelope is the A2A-spec shape (`{"error":{"code","status","message",
//! "details"}}`, `application/problem+json`).
//!
//! The one deliberate SDK delta, documented here so it is not re-litigated:
//! the SDK maps `UNSUPPORTED_OPERATION` to HTTP 400 while the previous
//! hand-written bindings answered 501. The A2A REST spec (§9.6.2) does not
//! reserve 501 for unsupported operations.
//!
//! # Surface
//!
//! - `GET /.well-known/agent-card.json` -- the public agent card (no auth,
//!   CORS for discovery) served by the SDK's `agent_card_router`.
//! - SDK `rest_router` (`/message:send`, `/tasks`, `/tasks/{id}`,
//!   `/tasks/{id}/cancel`, ...) over a [`ServerRequestHandler`] that
//!   implements `GetTask`, `ListTasks`, `CancelTask` over the [`TaskBackend`]
//!   trait and `SendMessage` over an optional [`MessageSink`].
//! - `SendStreamingMessage`, `SubscribeToTask`, push-notification config and
//!   `GetExtendedAgentCard` answer the SDK's dedicated errors.
//!
//! Authorization is enforced inside [`ServerRequestHandler`] from the
//! caller's [`AuthVerifier`] (each `RequestHandler` method receives the
//! request's `ServiceParams`); the agent card stays public.

mod auth;
mod backend;
mod card;
mod handler;
#[cfg(feature = "a2a")]
pub mod ingress;

#[cfg(feature = "a2a")]
pub mod inbox;

#[cfg(feature = "a2a")]
pub mod inject;

#[cfg(feature = "a2a")]
pub mod delivery;

#[cfg(feature = "a2a")]
pub mod mem;

#[cfg(feature = "a2a")]
pub mod http;

#[cfg(feature = "a2a")]
pub mod client;

#[cfg(feature = "a2a")]
pub mod receive;

pub use auth::A2aAuthLayer;
pub use backend::{map_job_status, A2AError, A2aTask, TaskBackend};
pub use card::{
    advertised_url, pod_name, role_description, role_skills, A2aModel, A2A_URL_ENV,
    ROLE_ORCHESTRATOR, ROLE_WORKER, WELL_KNOWN_AGENT_CARD_PATH,
};
pub use handler::{a2a_rate_limited, DeliveryMeta, MessageSink, ServerRequestHandler};

use std::sync::Arc;

use a2a_server::agent_card::agent_card_router;
use a2a_server::rest::rest_router;
use a2a_server::StaticAgentCard;
use axum::http::HeaderMap;
use axum::Router;

/// Caller-supplied request authorizer for the protected routes.
///
/// Returns `true` when the request's headers carry an accepted bearer token.
/// The wrapper serving the router builds this from its configured token
/// (constant-time compare; see [`constant_time_eq`]). A `None` value disables
/// the check entirely -- the wrapper then relies on network policy to keep
/// the endpoint unreachable, matching the agent-run-api / agent-orchestrate
/// "no token configured" model.
pub type AuthVerifier = Arc<dyn Fn(&HeaderMap) -> bool + Send + Sync>;

/// Builds the A2A axum router from the SDK's sub-routers.
///
/// - the public agent card router (no auth)
/// - the SDK REST router (`/message:send`, `/tasks`, ...)
///
/// The REST sub-router uses [`ServerRequestHandler`] carrying the optional
/// bearer verifier and [`MessageSink`]; an absent verifier lets everything
/// through (network-policy-only access control). The agent card stays public.
pub fn a2a_router(
    model: A2aModel,
    backend: Arc<dyn TaskBackend>,
    sink: Option<Arc<dyn MessageSink>>,
    auth: Option<AuthVerifier>,
) -> Router {
    let handler = Arc::new(ServerRequestHandler::new(backend, sink));

    // Authentication is applied as ONE router-level layer on the SDK REST
    // sub-router, so it runs for every A2A route before the SDK parses or
    // validates the body; the public card router is merged separately and is
    // never gated.
    let rest = rest_router(handler).layer(A2aAuthLayer::new(auth));

    let card_router = agent_card_router(Arc::new(StaticAgentCard::new(model.to_agent_card())));

    card_router.merge(rest)
}

/// The per-jobset credential context for the A2A routes, built once at
/// router-build time.
///
/// Carries the shared [`crate::core::jobset::JobsetRegistry`] used to verify a
/// presented jobset credential against the *target* agent of each request, and
/// the local agent id (this process's own addressable identity).
pub struct JobsetCred {
    pub registry: Arc<crate::core::jobset::JobsetRegistry>,
    pub local_agent_id: String,
}

impl JobsetCred {
    /// Build the context from the environment: reads the agent's jobset id and
    /// the secret file named by `KUBEADP_JOBSET_SECRET_FILE`, registers this
    /// agent in a fresh registry, and returns `None` when the agent is not a
    /// jobset member (no jobset id, or the secret file is absent).
    ///
    /// `local_agent_id` is this process's own agent identity.
    pub fn from_env(local_agent_id: String) -> Option<Self> {
        let cred = crate::core::jobset::JobsetCredential::from_env()?;
        let registry = Arc::new(crate::core::jobset::JobsetRegistry::new());
        registry.register(
            local_agent_id.clone(),
            cred.jobset_id.clone(),
            cred.secret.clone(),
        );
        Some(Self {
            registry,
            local_agent_id,
        })
    }

    /// Build the context for a *live* host using the process-wide registry.
    ///
    /// Always returns `Some` (never the bearer-only fallback router): the
    /// registry is the process-wide one that holds every agent the host
    /// serves (its own session/job membership plus every session/job it
    /// registered via [`crate::core::jobset::register_agent`]). A request that
    /// presents a jobset credential is verified against the TARGET of that
    /// request, which is authoritative regardless of whether this host
    /// process itself is a jobset member. This is what lets a worker or
    /// orchestrator daemon that does not itself carry a jobset id accept a
    /// jobset credential targeted at one of its hosted jobs/sessions. A
    /// request that presents no jobset credential falls back to the
    /// operator/bearer gate in [`ServerRequestHandler`].
    pub fn from_host() -> Option<Self> {
        let local_agent_id = crate::core::jobset::local_agent_id();
        Some(Self {
            registry: crate::core::jobset::global_registry(),
            local_agent_id,
        })
    }
}

/// Builds the A2A axum router with the per-jobset credential wired in as an
/// *alternative* credential on the A2A routes.
///
/// This is the single hook where the per-jobset check and the operator /
/// bearer gate combine. The combination lives in
/// [`ServerRequestHandler::authorize`]: a request that presents a jobset
/// credential is verified against the TARGET task/agent of the request (the
/// `send_message` recipient, or the task on `/tasks/{id}`), and a request that
/// presents no jobset credential is accepted only when the operator/bearer
/// verifier accepts it (or no verifier is configured -- the
/// network-policy-only model).
///
/// The check is installed ONLY on the A2A routes (the SDK JSON-RPC + REST
/// sub-routers share one [`ServerRequestHandler`]), so the jobset credential
/// is never accepted on the `/api/v1` agent-run surface (e.g.
/// `/api/v1/jobs`), which is guarded separately by the shared bearer layer.
pub fn a2a_router_with_jobset(
    model: A2aModel,
    backend: Arc<dyn TaskBackend>,
    sink: Option<Arc<dyn MessageSink>>,
    auth: Option<AuthVerifier>,
    jobset: Option<JobsetCred>,
) -> Router {
    let handler = Arc::new(ServerRequestHandler::new(backend, sink));

    // Authentication is ONE router-level layer on the SDK REST sub-router
    // (run before the body is parsed); the per-jobset credential and the
    // operator/bearer verifier combine in the layer's target-aware check.
    let rest = rest_router(handler).layer(A2aAuthLayer::with_jobset(auth, jobset.as_ref()));

    let card_router = agent_card_router(Arc::new(StaticAgentCard::new(model.to_agent_card())));

    card_router.merge(rest)
}

/// Constant-time comparison of two byte strings, used to validate a bearer
/// token (the hop-token MAC check in [`ingress`]) without leaking the
/// length-independent comparison timing.
///
/// Thin re-export of the shared [`crate::core::crypto::ct_eq`] so the
/// `a2a::constant_time_eq` call site is unchanged.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    crate::core::crypto::ct_eq(a, b)
}

/// Test-only helper: mutate the process environment (which the card helpers,
/// the receive-side inbox helpers and the state-root resolution read) under a
/// process-wide lock so env-touching unit tests never race each other (cargo
/// runs unit tests on parallel threads). Sets the given (var, value) pairs --
/// clearing any of the known `VARS` not listed first -- and restores the
/// previous values on drop (while still holding the lock).
///
/// Every test in the crate that touches one of these variables MUST go
/// through this guard (or the `testenv::with_env` closure form): a
/// module-local `Mutex` does not serialize against a sibling module, so two
/// tests could otherwise read each other's half-applied environment.
#[cfg(test)]
pub(crate) mod testenv {
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    /// Every env var the crate's tests touch. A guard clears all of these and
    /// restores them on drop, so a test can never leak a variable into a
    /// sibling test (e.g. `KUBEADP_A2A_INBOX_TOKEN` leaking into the direct
    /// `a2a-hook` path and silently switching it to the remote inbox).
    const VARS: &[&str] = &[
        "KUBEADP_A2A_NAME",
        "A2A_SELF_NAME",
        "POD_NAME",
        "HOSTNAME",
        "KUBEADP_A2A_URL",
        "KUBEADP_A2A_INBOX_DIR",
        "KUBEADP_A2A_INBOX_TOKEN",
        "KUBEADP_A2A_INBOX_URL",
        "AGENT_RUN_STATE_ROOT",
        "AGENT_RUN_WORKSPACE_ROOT",
        "AGENT_RUN_CONFIG",
    ];

    pub struct Guard {
        _lock: MutexGuard<'static, ()>,
        prior: Vec<(&'static str, Option<String>)>,
    }

    impl Guard {
        /// Takes the env lock, clears the known vars, applies `pairs`. The
        /// returned guard restores the previous values on drop (still holding
        /// the lock). Scope it tightly (a block) so it is dropped before the
        /// next guard.
        pub fn set(pairs: &[(&str, Option<&str>)]) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prior = VARS
                .iter()
                .map(|var| (*var, std::env::var(*var).ok()))
                .collect::<Vec<_>>();
            for var in VARS {
                std::env::remove_var(var);
            }
            for (var, value) in pairs {
                if let Some(v) = value {
                    std::env::set_var(var, v);
                }
            }
            Self { _lock: lock, prior }
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            for (var, value) in self.prior.drain(..) {
                match value {
                    Some(v) => std::env::set_var(var, v),
                    None => std::env::remove_var(var),
                }
            }
        }
    }

    /// Runs `f` under the env lock with the known vars cleared and the given
    /// pairs applied. Helpful when the body is a closure (e.g. async tests
    /// that must not hold the lock across an await).
    pub fn with_env<T>(pairs: &[(&str, Option<&str>)], f: impl FnOnce() -> T) -> T {
        let guard = Guard::set(pairs);
        let out = f();
        drop(guard);
        out
    }
}

#[cfg(test)]
mod jobset_router_tests {
    use super::*;
    use a2a::TaskState;
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tower::ServiceExt;

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
        fn get_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
            self.tasks
                .lock()
                .unwrap()
                .get(task_id)
                .cloned()
                .ok_or_else(|| A2AError::task_not_found(task_id))
        }
        fn list_tasks(
            &self,
            _context_id: Option<&str>,
            _state: Option<&TaskState>,
        ) -> Result<Vec<A2aTask>, A2AError> {
            Ok(self.tasks.lock().unwrap().values().cloned().collect())
        }
        fn cancel_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
            Err(A2AError::task_not_cancelable(task_id))
        }
    }

    fn task(id: &str) -> A2aTask {
        A2aTask {
            id: id.to_string(),
            context_id: Some(id.to_string()),
            state: TaskState::Working,
            status_message: None,
            error: None,
            artifacts: None,
        }
    }

    /// A JobsetCred whose registry registers the given (target agent, jobset,
    /// secret) memberships, built directly (bypassing the environment) so the
    /// test is deterministic. Verification is target-aware, so the tests
    /// register the TASK/AGENT addressed by each request.
    fn cred_with_members(members: &[(&str, &str, &str)]) -> JobsetCred {
        let registry = Arc::new(crate::core::jobset::JobsetRegistry::new());
        for (agent, jobset_id, secret) in members {
            registry.register(*agent, *jobset_id, *secret);
        }
        JobsetCred {
            registry,
            local_agent_id: "local-agent".to_string(),
        }
    }

    fn model() -> A2aModel {
        A2aModel {
            name: "js-test-agent".to_string(),
            description: "jobset router test".to_string(),
            version: "0.0.0".to_string(),
            skills: vec![],
            url: "http://127.0.0.1/unused".to_string(),
        }
    }

    fn get_task_req(uri: &str, headers: &[(&str, &str)]) -> Request<Body> {
        let mut b = Request::builder().uri(uri);
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn jobset_credential_accepted_on_a2a_route() {
        // The task itself (t1) is a member of jobset js-1; the presented
        // credential matches the TARGET task's jobset -> accepted.
        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, Some(cred));
        let res = app
            .oneshot(get_task_req(
                "/tasks/t1",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "secret-1"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn jobset_credential_accepted_even_when_host_is_not_a_member() {
        // F2: the daemon that hosts the task does not itself carry a jobset
        // id (its own identity is not registered), yet a presented jobset
        // credential targeted at one of its hosted tasks must verify --
        // against the TARGET, via the global registry -- instead of falling
        // back to bearer-only and rejecting.
        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, Some(cred));
        let res = app
            .oneshot(get_task_req(
                "/tasks/t1",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "secret-1"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn jobset_cross_jobset_access_rejected() {
        // F3: an agent in jobset js-1 must NOT be able to access a task that
        // belongs to a DIFFERENT jobset (js-2), even with a correct js-1
        // credential -- the presented credential is verified against the
        // target task's jobset.
        let backend = FakeBackend::with(vec![task("t2")]);
        let cred = cred_with_members(&[("t2", "js-2", "secret-2")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, Some(cred));
        let res = app
            .oneshot(get_task_req(
                "/tasks/t2",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "secret-1"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn jobset_unknown_target_rejected() {
        // F3: a target with no registered membership must be refused even when
        // the presented jobset/secret belongs to a VALID jobset the host
        // serves.
        let backend = FakeBackend::with(vec![task("unknown")]);
        let cred = cred_with_members(&[("known", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, Some(cred));
        let res = app
            .oneshot(get_task_req(
                "/tasks/unknown",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "secret-1"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    /// A request that names no target (`GET /tasks`) still has to present the
    /// jobset's secret: the id alone is not a credential.
    #[tokio::test]
    async fn jobset_targetless_request_verifies_the_secret() {
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(
            model(),
            FakeBackend::with(vec![task("t1")]),
            None,
            None,
            Some(cred),
        );
        let wrong = app
            .clone()
            .oneshot(get_task_req(
                "/tasks",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "not-the-secret"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        let right = app
            .oneshot(get_task_req(
                "/tasks",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "secret-1"),
                ],
            ))
            .await
            .unwrap();
        assert_ne!(right.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn jobset_wrong_secret_rejected() {
        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, Some(cred));
        let res = app
            .oneshot(get_task_req(
                "/tasks/t1",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "wrong-secret"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn jobset_missing_header_open_when_no_verifier_configured() {
        // No credential headers, and NO bearer verifier configured (the
        // api_allow_unauthenticated / network-policy-only model): a request
        // with no jobset credential passes (the surface is open by design).
        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, Some(cred));
        let res = app.oneshot(get_task_req("/tasks/t1", &[])).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn jobset_missing_header_rejected_when_verifier_configured() {
        // No credential headers, but a bearer verifier IS configured: the
        // request must present a valid credential (jobset or bearer).
        let bearer: AuthVerifier = Arc::new(|_headers: &HeaderMap| false);
        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, Some(bearer), Some(cred));
        let res = app.oneshot(get_task_req("/tasks/t1", &[])).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn jobset_downgrade_from_invalid_jobset_to_bearer_prevented() {
        // F2: when a jobset credential IS presented but is invalid (wrong
        // secret), the request must NOT be downgraded to the bearer gate --
        // an invalid jobset credential is refused outright.
        let secret = b"op-token";
        let bearer: AuthVerifier = Arc::new(move |headers: &HeaderMap| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.strip_prefix("Bearer ").is_some_and(|t| t == "op-token"))
        });
        let _ = secret;

        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, Some(bearer), Some(cred));

        // Jobset header presented but wrong secret + VALID bearer token:
        // the invalid jobset credential must win and the request is refused
        // (the bearer token is NOT a fallback for a presented-but-wrong
        // jobset credential).
        let res = app
            .oneshot(get_task_req(
                "/tasks/t1",
                &[
                    (header::AUTHORIZATION.as_str(), "Bearer op-token"),
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "wrong-secret"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn jobset_combines_with_bearer() {
        // A configured bearer verifier: a request is accepted with EITHER the
        // jobset credential OR the bearer token (bearer only when no jobset
        // credential is presented).
        let secret = b"op-token";
        let bearer: AuthVerifier = Arc::new(move |headers: &HeaderMap| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.strip_prefix("Bearer ").is_some_and(|t| t == "op-token"))
        });
        let _ = secret;

        let backend = FakeBackend::with(vec![task("t1")]);
        let cred = cred_with_members(&[("t1", "js-1", "secret-1")]);
        let app = a2a_router_with_jobset(model(), backend, None, Some(bearer), Some(cred));

        // Valid bearer alone (no jobset headers) -> accepted.
        let res = app
            .clone()
            .oneshot(get_task_req(
                "/tasks/t1",
                &[(header::AUTHORIZATION.as_str(), "Bearer op-token")],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Valid jobset credential alone -> accepted.
        let res = app
            .clone()
            .oneshot(get_task_req(
                "/tasks/t1",
                &[
                    (crate::core::jobset::JOBSET_ID_HEADER, "js-1"),
                    (crate::core::jobset::JOBSET_SECRET_HEADER, "secret-1"),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Neither valid -> refused.
        let res = app.oneshot(get_task_req("/tasks/t1", &[])).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn not_a_jobset_member_preserves_bearer_only() {
        // No jobset cred: the router is the plain bearer-only one; an absent
        // bearer verifier passes everything through (network-policy model).
        let backend = FakeBackend::with(vec![task("t1")]);
        let app = a2a_router_with_jobset(model(), backend, None, None, None);
        let res = app.oneshot(get_task_req("/tasks/t1", &[])).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}

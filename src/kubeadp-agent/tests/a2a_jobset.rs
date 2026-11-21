//! Feature-gated integration tests for the C4a JOB SET-ONLY A2A layer
//! (JobsetAuthLayer, `/a2a/agents`, `POST /a2a/jobsets/{id}:end`, uniform
//! 429 + Retry-After).
//!
//! These tests build `a2a_router_with_jobset_only` (the flag-day target
//! router) directly; it is NOT mounted in production until C5.
#![cfg(feature = "a2a")]

use a2a::A2AError as SdkA2AError;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::Router;
use kubeadp_agent::core::a2a::{
    a2a_router_with_jobset_only, A2AError, A2aModel, A2aTask, DeliveryMeta, JobsetCred,
    MessageSink, PeerEndNotifier, TaskBackend, AGENT_ID_HEADER, AGENT_PROOF_HEADER,
};
use kubeadp_agent::core::jobset::{
    derive_jobset_secret, derive_member_proof, JobsetRegistry, JOBSET_ID_HEADER,
    JOBSET_SECRET_HEADER,
};
use std::sync::Arc;
use std::sync::Mutex;
use tower::ServiceExt;

const ROOT: &[u8] = b"test-root-key";

/// A backend that always answers task-not-found, enough for auth tests (the
/// layer refuses before the handler for bad credentials).
#[derive(Clone)]
struct NoTasks;

impl TaskBackend for NoTasks {
    fn get_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        Err(A2AError::task_not_found(task_id))
    }
    fn list_tasks(
        &self,
        _context_id: Option<&str>,
        _state: Option<&a2a::TaskState>,
    ) -> Result<Vec<A2aTask>, A2AError> {
        Ok(vec![])
    }
    fn cancel_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        Err(A2AError::task_not_found(task_id))
    }
}

/// A backend that returns a (long-lived) task for any known agent id, so a
/// valid send resolves its addressed task and answers 200.
#[derive(Clone)]
struct KnownTasks {
    known: Arc<std::collections::HashSet<String>>,
}

impl KnownTasks {
    fn with(ids: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            known: Arc::new(ids.iter().map(|s| s.to_string()).collect()),
        })
    }
}

impl TaskBackend for KnownTasks {
    fn get_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        if self.known.contains(task_id) {
            Ok(A2aTask {
                id: task_id.to_string(),
                context_id: None,
                state: a2a::TaskState::Working,
                status_message: None,
                error: None,
                artifacts: None,
            })
        } else {
            Err(A2AError::task_not_found(task_id))
        }
    }
    fn list_tasks(
        &self,
        _context_id: Option<&str>,
        _state: Option<&a2a::TaskState>,
    ) -> Result<Vec<A2aTask>, A2AError> {
        Ok(vec![])
    }
    fn cancel_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        self.get_task(task_id)
    }
}

/// One recorded delivery (four string fields).
type Delivery = (String, String, String, String);

/// A sink that records every delivery.
#[derive(Clone, Default)]
struct RecordingSink {
    deliveries: Arc<Mutex<Vec<Delivery>>>,
}

impl MessageSink for RecordingSink {
    fn deliver_with_meta(
        &self,
        task_id: &str,
        sender: &str,
        text: &str,
        message_id: &str,
        _meta: DeliveryMeta,
    ) -> Result<(), SdkA2AError> {
        self.deliveries.lock().unwrap().push((
            task_id.to_string(),
            sender.to_string(),
            text.to_string(),
            message_id.to_string(),
        ));
        Ok(())
    }
}

/// Builds a registry with two agents of one jobset registered under a root
/// key, so the layer can derive (and verify) real secrets/proofs.
fn registry_with_jobset() -> (Arc<JobsetRegistry>, String, String, String) {
    let registry = Arc::new(JobsetRegistry::new());
    registry.set_root_key(Some(Arc::from(ROOT)));
    let jobset_id = "js-1";
    // Agent A is a member and the creator (creator == jobset id by default);
    // agent B is a second member.
    registry.register(
        "agent-a",
        jobset_id,
        derive_jobset_secret(ROOT, jobset_id, 0),
    );
    registry.register(
        "agent-b",
        jobset_id,
        derive_jobset_secret(ROOT, jobset_id, 0),
    );
    (
        registry,
        jobset_id.to_string(),
        "agent-a".to_string(),
        "agent-b".to_string(),
    )
}

fn jobset_cred(registry: Arc<JobsetRegistry>) -> JobsetCred {
    JobsetCred {
        registry,
        local_agent_id: "agent-a".to_string(),
    }
}

fn notifier_none() -> Arc<dyn PeerEndNotifier> {
    Arc::new(NoopNotifier)
}

#[derive(Debug)]
struct NoopNotifier;

#[async_trait::async_trait]
impl PeerEndNotifier for NoopNotifier {
    async fn notify(&self, _jobset_id: &str, _headers: &HeaderMap) -> usize {
        0
    }
}

fn test_model() -> A2aModel {
    A2aModel {
        name: "test-agent".to_string(),
        description: "A test agent".to_string(),
        version: "1.0.0".to_string(),
        skills: vec![],
        url: "http://localhost:8080".to_string(),
    }
}

fn router(
    registry: Arc<JobsetRegistry>,
    backend: Arc<dyn TaskBackend>,
    sink: RecordingSink,
) -> Router {
    a2a_router_with_jobset_only(
        test_model(),
        backend,
        Some(Arc::new(sink)),
        jobset_cred(registry),
        notifier_none(),
    )
}

/// Headers of a send request from `sender` for a jobset/epoch. The proof must
/// be the member proof of the SENDER.
fn member_headers(jobset_id: &str, epoch: u64, sender: &str) -> HeaderMap {
    let secret = derive_jobset_secret(ROOT, jobset_id, epoch);
    let proof = derive_member_proof(ROOT, jobset_id, epoch, sender);
    let mut h = HeaderMap::new();
    h.insert(JOBSET_ID_HEADER, jobset_id.parse().unwrap());
    h.insert(JOBSET_SECRET_HEADER, secret.parse().unwrap());
    h.insert(AGENT_ID_HEADER, sender.parse().unwrap());
    h.insert(AGENT_PROOF_HEADER, proof.parse().unwrap());
    h
}

async fn body_string(body: Body) -> String {
    let bytes = http_body_util::BodyExt::collect(body)
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8_lossy(&bytes).to_string()
}

/// Builds a request with a full header map. The `http` builder only has the
/// per-key `header` setter, so the map is assigned through `headers_mut`.
fn request_with_headers(uri: &str, method: &str, headers: HeaderMap, body: Body) -> Request<Body> {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .expect("request");
    *req.headers_mut() = headers;
    req
}

/// A well-formed send message body addressed to `task_id`, carrying a valid
/// one-time hop token minted with `jobset_secret` (the receiver-authoritative
/// hop guard needs it; SPEC §7.11, N1).
fn send_body(task_id: &str, message_id: &str, jobset_secret: &str) -> String {
    let token = kubeadp_agent::core::a2a::ingress::issue_hop_token(
        1,
        message_id,
        jobset_secret.as_bytes(),
        kubeadp_agent::core::a2a::inbox::now_unix(),
    );
    // Serialized with the SDK's own types, the shape its REST router parses.
    let mut message = a2a::Message::new(a2a::Role::User, vec![a2a::Part::text("hello")]);
    message.message_id = message_id.to_string();
    message.task_id = Some(task_id.to_string());
    message.metadata = Some(std::collections::HashMap::from([(
        "a2a.hop_token".to_string(),
        serde_json::json!(token),
    )]));
    serde_json::to_string(&a2a::SendMessageRequest {
        message,
        configuration: None,
        metadata: None,
        tenant: None,
    })
    .unwrap()
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn card_stays_open_on_jobset_only_router() {
    let (reg, _, _, _) = registry_with_jobset();
    let app = router(reg.clone(), Arc::new(NoTasks), RecordingSink::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/.well-known/agent-card.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn bearer_only_request_is_refused_by_jobset_only_router() {
    let (reg, _, _, _) = registry_with_jobset();
    let app = router(reg, Arc::new(NoTasks), RecordingSink::default());
    // A valid bearer token (as the old operator layer would accept) is NOT a
    // jobset credential: the jobset-only layer must refuse it (SPEC §7.5).
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/message:send")
                .header("authorization", "Bearer good-token")
                .header("a2a-version", "1.0")
                .header("content-type", "application/json")
                .body(Body::from(send_body("agent-b", "m-1", "")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("UNAUTHENTICATED"), "body: {body}");
}

#[tokio::test]
async fn malformed_body_with_bad_credential_answers_401_before_body_validation() {
    let (reg, _, _, _) = registry_with_jobset();
    let app = router(reg, Arc::new(NoTasks), RecordingSink::default());
    // The credential is invalid AND the body is not valid JSON. The layer must
    // refuse the credential FIRST (401), never surface a body-validation
    // error from the SDK (SPEC §7.5: auth before body validation).
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/message:send")
                .header(JOBSET_ID_HEADER, "js-1")
                .header(JOBSET_SECRET_HEADER, "wrong")
                .header(AGENT_ID_HEADER, "agent-a")
                .header(AGENT_PROOF_HEADER, "wrong")
                .header("a2a-version", "1.0")
                .header("content-type", "application/json")
                .body(Body::from("{not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("UNAUTHENTICATED"), "body: {body}");
}

#[tokio::test]
async fn send_with_valid_jobset_credential_is_forwarded_to_sink() {
    let (reg, jobset_id, _, recipient) = registry_with_jobset();
    let sink = RecordingSink::default();
    let backend = KnownTasks::with(&["agent-a", "agent-b"]);
    let app = router(reg, backend, sink.clone());
    let mut headers = member_headers(&jobset_id, 0, "agent-a");
    headers.insert("a2a-version", "1.0".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());
    let resp = app
        .oneshot(request_with_headers(
            "/message:send",
            "POST",
            headers,
            Body::from(send_body(
                &recipient,
                "m-1",
                &derive_jobset_secret(ROOT, &jobset_id, 0),
            )),
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "body: {}",
        body_string(resp.into_body()).await
    );
    let deliveries = sink.deliveries.lock().unwrap();
    assert_eq!(deliveries.len(), 1);
}

#[tokio::test]
async fn recipient_not_in_jobset_answers_403_no_shared_jobset() {
    let (reg, jobset_id, sender, _) = registry_with_jobset();
    let mut headers = member_headers(&jobset_id, 0, &sender);
    headers.insert("a2a-version", "1.0".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());
    let app = router(reg, Arc::new(NoTasks), RecordingSink::default());
    let resp = app
        .oneshot(request_with_headers(
            "/message:send",
            "POST",
            headers,
            Body::from(send_body("outside-jobset", "m-2", "")),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body = body_string(resp.into_body()).await;
    assert!(
        body.to_ascii_lowercase().contains("no_shared_jobset"),
        "body: {body}"
    );
}

// ---------------------------------------------------------------------------
// GET /a2a/agents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agents_list_is_scoped_to_the_presented_jobset() {
    let (reg, jobset_id, sender, _) = registry_with_jobset();
    // A second jobset with one member, so the list must NOT include it.
    reg.register(
        "agent-c",
        "other-js",
        derive_jobset_secret(ROOT, "other-js", 0),
    );
    let mut headers = member_headers(&jobset_id, 0, &sender);
    headers.insert("a2a-version", "1.0".parse().unwrap());
    let app = router(reg, Arc::new(NoTasks), RecordingSink::default());
    let resp = app
        .oneshot(request_with_headers(
            "/a2a/agents",
            "GET",
            headers,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    let agents = value.get("agents").unwrap().as_array().unwrap();
    let ids: Vec<&str> = agents
        .iter()
        .filter_map(|a| a.get("agent_id").and_then(|v| v.as_str()))
        .collect();
    assert!(
        ids.contains(&"agent-a") && ids.contains(&"agent-b"),
        "jobset members missing: {ids:?}"
    );
    assert!(
        !ids.contains(&"agent-c"),
        "other-jobset agent leaked into the list: {ids:?}"
    );
    for a in agents {
        assert_eq!(a.get("jobset_id").and_then(|v| v.as_str()), Some("js-1"));
        assert!(a.get("kind").and_then(|v| v.as_str()).is_some());
        assert!(a.get("state").and_then(|v| v.as_str()).is_some());
    }
}

#[tokio::test]
async fn agents_list_without_credential_is_refused() {
    let (reg, _, _, _) = registry_with_jobset();
    let app = router(reg, Arc::new(NoTasks), RecordingSink::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/a2a/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// POST /a2a/jobsets/{id}:end
// ---------------------------------------------------------------------------

/// A notifier that records the received headers and answers a fixed count.
#[derive(Debug)]
struct RecordingNotifier {
    calls: Mutex<Vec<(String, HeaderMap)>>,
    answer: usize,
}

#[async_trait::async_trait]
impl PeerEndNotifier for RecordingNotifier {
    async fn notify(&self, jobset_id: &str, headers: &HeaderMap) -> usize {
        self.calls
            .lock()
            .unwrap()
            .push((jobset_id.to_string(), headers.clone()));
        self.answer
    }
}

#[tokio::test]
async fn end_requires_creator_proof_and_applies_local_end() {
    let (reg, jobset_id, _, _) = registry_with_jobset();
    // In the per-pod default jobset the creator's agent id EQUALS the jobset
    // id (SPEC §7.2 item 1); register that identity so the creator
    // verification (which compares X-Agent-Id against the store/registry
    // creator, defaulting to the jobset id) can pass.
    reg.register(
        jobset_id.as_str(),
        jobset_id.as_str(),
        derive_jobset_secret(ROOT, &jobset_id, 0),
    );
    assert!(!reg.is_ended(&jobset_id));

    // A plain member (agent-b) is NOT the creator: 403.
    let mut member = member_headers(&jobset_id, 0, "agent-b");
    member.insert("a2a-version", "1.0".parse().unwrap());
    let app = router(reg.clone(), Arc::new(NoTasks), RecordingSink::default());
    let resp = app
        .clone()
        .oneshot(request_with_headers(
            &format!("/a2a/jobsets/{jobset_id}:end"),
            "POST",
            member,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // The creator (agent id == jobset id) can end it.
    let notifier = Arc::new(RecordingNotifier {
        calls: Mutex::new(Vec::new()),
        answer: 1,
    });
    let app = a2a_router_with_jobset_only(
        test_model(),
        Arc::new(NoTasks),
        Some(Arc::new(RecordingSink::default())),
        jobset_cred(reg.clone()),
        notifier.clone(),
    );
    let mut creator = member_headers(&jobset_id, 0, &jobset_id);
    creator.insert("a2a-version", "1.0".parse().unwrap());
    let resp = app
        .clone()
        .oneshot(request_with_headers(
            &format!("/a2a/jobsets/{jobset_id}:end"),
            "POST",
            creator,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "body: {}",
        body_string(resp.into_body()).await
    );
    assert!(reg.is_ended(&jobset_id));
    {
        let calls = notifier.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, jobset_id);
    }

    // A late `:end` with a STALE (pre-end) credential is refused 401: the
    // end bumped the epoch and marked the jobset ended, so the old credential
    // no longer verifies (SPEC §7.16: the end is permanent). Handler-level
    // idempotency (an already-ended jobset applies end again without error)
    // is covered by the unit test `apply_end_is_idempotent_...`.
    let mut creator2 = member_headers(&jobset_id, 0, &jobset_id);
    creator2.insert("a2a-version", "1.0".parse().unwrap());
    let resp = app
        .clone()
        .oneshot(request_with_headers(
            &format!("/a2a/jobsets/{jobset_id}:end"),
            "POST",
            creator2,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Uniform 429 + Retry-After
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sixth_send_between_same_sender_recipient_answers_429_with_retry_after() {
    let (reg, jobset_id, sender, recipient) = registry_with_jobset();
    let app = router(
        reg,
        KnownTasks::with(&["agent-a", "agent-b"]),
        RecordingSink::default(),
    );
    for i in 0..5 {
        let mut headers = member_headers(&jobset_id, 0, &sender);
        headers.insert("a2a-version", "1.0".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let resp = app
            .clone()
            .oneshot(request_with_headers(
                "/message:send",
                "POST",
                headers,
                Body::from(send_body(
                    &recipient,
                    &format!("m-{i}"),
                    &derive_jobset_secret(ROOT, &jobset_id, 0),
                )),
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "request {i}: {}",
            body_string(resp.into_body()).await
        );
    }
    // The 6th send in the refill window (capacity 5) is rate-limited.
    let mut headers = member_headers(&jobset_id, 0, &sender);
    headers.insert("a2a-version", "1.0".parse().unwrap());
    headers.insert("content-type", "application/json".parse().unwrap());
    let resp = app
        .oneshot(request_with_headers(
            "/message:send",
            "POST",
            headers,
            Body::from(send_body(
                &recipient,
                "m-5",
                &derive_jobset_secret(ROOT, &jobset_id, 0),
            )),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after = resp
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .expect("Retry-After header");
    let secs: u64 = retry_after.to_str().unwrap().parse().unwrap();
    assert!(secs >= 1);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("RATE_LIMITED"), "body: {body}");
}

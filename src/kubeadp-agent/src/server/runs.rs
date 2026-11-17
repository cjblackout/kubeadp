//! E4 run-control endpoints (design HL-91 section 2.5): `interrupt`, `pause`,
//! `compact`, `fork`, `revert` and `todo`, all addressed by run id.
//!
//! A request resolves the [`Run`] through [`EngineRunRegistry`] and the run's
//! engine through the [`EngineRegistry`]; the engine's reported capability for
//! the endpoint then decides the answer (design 2.4 rule, 501 only where
//! genuinely impossible):
//!
//! - `Unsupported` -- 501 `{"code":"unsupported","engine":<id>}` (the E1
//!   contract body via [`crate::engine::errors::to_response`]),
//! - `Native`/`Emulated` -- dispatched through the [`EmulatingEngine`]
//!   wrapper, which delegates a native endpoint to the inner engine and fronts
//!   an emulated one itself.
//!
//! Nothing registers runs into [`EngineRunRegistry::global`] during E4 (the
//! drivers land in E3/E5), so in production these routes answer 404 for any
//! run id; the contract is exercised by tests that register runs directly
//! through the same registry.

use axum::extract::{Extension, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::core::config::Config;
use crate::core::job::JobMetadata;
use crate::core::output::CommandOutput;
use crate::engine::capability::{CapEndpointKey, Capability};
use crate::engine::emulate::{EmulatingEngine, EngineRunRegistry};
use crate::engine::errors::EngineError;
use crate::engine::run::Run;
use crate::engine::{Engine, EngineRegistry, RunKind};
use crate::server::{ApiJson, ApiPath, AppState};

/// Resolves `run` and its emulation wrapper, gating `key` on the engine's
/// reported capability (an `Unsupported` endpoint answers 501).
fn resolve_run_control(
    registry: &EngineRegistry,
    runs: &EngineRunRegistry,
    run_id: &str,
    key: CapEndpointKey,
) -> Result<(Run, Arc<EmulatingEngine>), EngineError> {
    let run = runs.lookup(run_id).ok_or_else(|| EngineError::NotFound {
        run: run_id.to_string(),
    })?;
    let engine = registry
        .emulating(&run.engine)
        .ok_or_else(|| EngineError::UnknownEngine {
            engine: run.engine.clone(),
        })?;
    if engine.capability(key) == Capability::Unsupported {
        return Err(EngineError::unsupported(run.engine.clone(), key));
    }
    Ok((run, engine))
}

/// Resolves a JOB to its `Run` view and emulation wrapper, gating `key` on
/// the engine's reported capability (an `Unsupported` endpoint answers 501).
///
/// The job surface (`/api/v1/jobs/{id}/...`) resolves the job row on demand
/// ([`crate::core::job::JobMetadata::resolve`], prefix-tolerant) and builds
/// the trait `Run` from it; the engine is the row's recorded engine (falling
/// back to the config's job default, then "agy" for rows written before
/// engine selection existed). No in-memory registry is consulted, so the
/// routes keep working across process restarts and agree with `status`/
/// `retry`/`kill`, which all resolve the row the same way.
fn resolve_job_run_control(
    config: &Config,
    registry: &EngineRegistry,
    job_id: &str,
    key: CapEndpointKey,
) -> Result<(Run, Arc<EmulatingEngine>), EngineError> {
    let meta = JobMetadata::resolve(&config.state_root, job_id).map_err(|e| {
        if let Some(err) = e.downcast_ref::<crate::core::error::AgentRunError>() {
            if matches!(err.kind, crate::core::error::ErrorKind::NotFound) {
                return EngineError::NotFound {
                    run: job_id.to_string(),
                };
            }
        }
        let msg = e.to_string();
        let clean = match msg.find(" (no job record in ") {
            Some(idx) => &msg[..idx],
            None => msg.as_str(),
        };
        EngineError::Internal(format!("failed to resolve job '{job_id}': {clean}"))
    })?;
    let engine = match meta.engine.as_deref() {
        Some(e) => e.to_string(),
        None => registry.default_for(RunKind::Job).clone(),
    };
    let run = Run::from_job(&meta, &engine);
    let engine = registry
        .emulating(&run.engine)
        .ok_or_else(|| EngineError::UnknownEngine {
            engine: run.engine.clone(),
        })?;
    if engine.capability(key) == Capability::Unsupported {
        return Err(EngineError::unsupported(run.engine.clone(), key));
    }
    Ok((run, engine))
}

/// `POST /api/v1/runs/{id}/interrupt` -- interrupt a run (native or emulated).
pub async fn interrupt_run(
    Extension(registry): Extension<Arc<EngineRegistry>>,
    Extension(runs): Extension<Arc<EngineRunRegistry>>,
    ApiPath(run_id): ApiPath<String>,
) -> Response {
    let result = match resolve_run_control(&registry, &runs, &run_id, "interrupt") {
        Ok((run, engine)) => engine.interrupt(&run).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => Json(json!({ "run_id": run_id, "interrupted": true })).into_response(),
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// The `pause`/`resume` request body.
#[derive(Debug, Deserialize)]
pub struct PauseBody {
    #[serde(default)]
    paused: bool,
}

/// `POST /api/v1/runs/{id}/pause` -- pause/resume a run. `{"paused": true}`
/// pauses, `{"paused": false}` resumes.
pub async fn pause_run(
    Extension(registry): Extension<Arc<EngineRegistry>>,
    Extension(runs): Extension<Arc<EngineRunRegistry>>,
    ApiPath(run_id): ApiPath<String>,
    ApiJson(body): ApiJson<PauseBody>,
) -> Response {
    let result = match resolve_run_control(&registry, &runs, &run_id, "pause") {
        Ok((run, engine)) => engine.pause(&run, body.paused).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => Json(json!({ "run_id": run_id, "paused": body.paused })).into_response(),
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `POST /api/v1/runs/{id}/compact` -- compact a run's context (native or
/// emulated summarise-then-restart).
pub async fn compact_run(
    Extension(registry): Extension<Arc<EngineRegistry>>,
    Extension(runs): Extension<Arc<EngineRunRegistry>>,
    ApiPath(run_id): ApiPath<String>,
) -> Response {
    let result = match resolve_run_control(&registry, &runs, &run_id, "compact") {
        Ok((run, engine)) => engine.compact(&run).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => Json(json!({ "run_id": run_id, "compacted": true })).into_response(),
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `POST /api/v1/runs/{id}/fork` -- fork a run into a new run (native or
/// emulated worktree copy), returning the new run id.
pub async fn fork_run(
    Extension(registry): Extension<Arc<EngineRegistry>>,
    Extension(runs): Extension<Arc<EngineRunRegistry>>,
    ApiPath(run_id): ApiPath<String>,
) -> Response {
    let result = match resolve_run_control(&registry, &runs, &run_id, "fork") {
        Ok((from, engine)) => {
            // The E4 emulated fork seeds itself from the source run; the
            // placeholder run is the kubeadp-agent (new) run the caller
            // pre-created, which native forks may populate.
            let placeholder = Run::new(
                format!("{}:pending", from.id),
                from.kind,
                from.workdir.clone(),
            );
            engine.fork(&placeholder, &from).await
        }
        Err(err) => Err(err),
    };
    match result {
        Ok(new_id) => Json(json!({ "run_id": new_id, "forked_from": run_id })).into_response(),
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// The `revert` request body. An empty commit means *unrevert* (restore the
/// work `revert` discarded).
#[derive(Debug, Deserialize)]
pub struct RevertBody {
    #[serde(default)]
    commit: String,
}

/// `POST /api/v1/runs/{id}/revert` -- revert a run's worktree to its latest
/// checkpoint (`{"commit": ""}` unreverts).
pub async fn revert_run(
    Extension(registry): Extension<Arc<EngineRegistry>>,
    Extension(runs): Extension<Arc<EngineRunRegistry>>,
    ApiPath(run_id): ApiPath<String>,
    ApiJson(body): ApiJson<RevertBody>,
) -> Response {
    let result = match resolve_run_control(&registry, &runs, &run_id, "revert") {
        Ok((run, engine)) => engine.revert(&run, &body.commit).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => Json(json!({
            "run_id": run_id,
            "reverted": !body.commit.is_empty(),
            "commit": body.commit,
        }))
        .into_response(),
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `GET /api/v1/runs/{id}/todo` -- the run's structured todo list (collected
/// from its event stream by the emulation layer).
pub async fn todo_run(
    Extension(registry): Extension<Arc<EngineRegistry>>,
    Extension(runs): Extension<Arc<EngineRunRegistry>>,
    ApiPath(run_id): ApiPath<String>,
) -> Response {
    let result = match resolve_run_control(&registry, &runs, &run_id, "todo") {
        Ok((run, engine)) => Ok(engine.todo_for(&run)),
        Err(err) => Err(err),
    };
    match result {
        Ok(todos) => Json(json!({ "run_id": run_id, "todos": todos })).into_response(),
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

// ---------------------------------------------------------------------------
// Job-scoped run control (`/api/v1/jobs/{job_id}/...`, HL-71 S2).
//
// The job surface resolves the job row on demand (prefix-tolerant) and gates
// on the recorded engine's reported capability; the same emulation wrapper
// the run surface uses fronts the endpoint, so claude/agy jobs answer
// interrupt/pause/message/fork/compact/revert/todo with real behaviour --
// never 501 because of the engine. `/api/v1/runs/{id}/*` resolves no runs in
// production (the run registry is populated only by tests); the job-scoped
// routes are the production surface.
// ---------------------------------------------------------------------------

/// `POST /api/v1/jobs/{id}/interrupt` -- interrupt a running job (emulated:
/// process-group SIGTERM -> SIGKILL, no kill bookkeeping).
pub async fn interrupt_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
) -> Response {
    let result = match resolve_job_run_control(&config, &registry, &job_id, "interrupt") {
        Ok((run, engine)) => engine.interrupt(&run).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "interrupted": true,
            }));
            output.push_out(format!("Job '{job_id}' interrupted."));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `POST /api/v1/jobs/{id}/pause` -- pause/resume a job. `{"paused": true}`
/// pauses (the E4 per-run flag; messages queue), `false` resumes.
pub async fn pause_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(body): ApiJson<PauseBody>,
) -> Response {
    let paused = body.paused;
    let result = match resolve_job_run_control(&config, &registry, &job_id, "pause") {
        Ok((run, engine)) => engine.pause(&run, paused).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "paused": paused,
            }));
            let verb = if paused { "paused" } else { "resumed" };
            output.push_out(format!("Job '{job_id}' {verb}."));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// The `message` request body for `POST /api/v1/jobs/{id}/message`.
#[derive(Debug, Deserialize)]
pub struct MessageBody {
    #[serde(default)]
    pub message: String,
}

/// `POST /api/v1/jobs/{id}/message` -- deliver a message to a running job.
/// Engines without a native mid-run channel (the batch harnesses) queue it to
/// the job worktree's `.engine-message-queue`, delivered at the next turn.
pub async fn message_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(body): ApiJson<MessageBody>,
) -> Response {
    let message = body.message;
    let result = match resolve_job_run_control(&config, &registry, &job_id, "a2a.send_message") {
        Ok((run, engine)) => engine.send_message(&run, &message).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "queued": true,
            }));
            output.push_out(format!("Message queued for job '{job_id}'."));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `POST /api/v1/jobs/{id}/compact` -- compact a job's context (emulated
/// summarise-then-restart).
pub async fn compact_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
) -> Response {
    let result = match resolve_job_run_control(&config, &registry, &job_id, "compact") {
        Ok((run, engine)) => engine.compact(&run).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "compacted": true,
            }));
            output.push_out(format!("Job '{job_id}' compacted."));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `POST /api/v1/jobs/{id}/fork` -- fork a job into a new run (emulated
/// worktree copy seeded from the source job's worktree), returning the new
/// run id.
pub async fn fork_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
) -> Response {
    let result = match resolve_job_run_control(&config, &registry, &job_id, "fork") {
        Ok((from, engine)) => {
            let placeholder = Run::new(
                format!("{}:pending", from.id),
                from.kind,
                from.workdir.clone(),
            );
            engine
                .fork(&placeholder, &from)
                .await
                .map(|new_id| (from, new_id))
        }
        Err(err) => Err(err),
    };
    match result {
        Ok((from, new_id)) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "run_id": new_id,
                "forked_from": from.id,
            }));
            output.push_out(format!("Forked job '{job_id}' as run '{new_id}'."));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `POST /api/v1/jobs/{id}/revert` -- revert a job's worktree to its latest
/// engine checkpoint (`{"commit": ""}` unreverts).
pub async fn revert_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
    ApiJson(body): ApiJson<RevertBody>,
) -> Response {
    let commit = body.commit.clone();
    let result = match resolve_job_run_control(&config, &registry, &job_id, "revert") {
        Ok((run, engine)) => engine.revert(&run, &commit).await,
        Err(err) => Err(err),
    };
    match result {
        Ok(()) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "reverted": !commit.is_empty(),
                "commit": commit,
            }));
            let verb = if commit.is_empty() {
                "unreverted"
            } else {
                "reverted"
            };
            output.push_out(format!("Job '{job_id}' {verb}."));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

/// `GET /api/v1/jobs/{id}/todo` -- the job's structured todo list (collected
/// from its event stream by the emulation layer).
pub async fn todo_job(
    State(config): State<AppState>,
    Extension(registry): Extension<Arc<EngineRegistry>>,
    ApiPath(job_id): ApiPath<String>,
) -> Response {
    let result = match resolve_job_run_control(&config, &registry, &job_id, "todo") {
        Ok((run, engine)) => Ok(engine.todo_for(&run)),
        Err(err) => Err(err),
    };
    match result {
        Ok(todos) => {
            let mut output = CommandOutput::new(json!({
                "job_id": job_id,
                "todos": todos,
            }));
            output.push_out(format!("Job '{job_id}' has {} todo item(s).", todos.len()));
            Json(output).into_response()
        }
        Err(err) => crate::engine::errors::to_response(&err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::auth::AuthManager;
    use crate::core::config::Config;
    use crate::engine::capability::CapabilityTable;
    use crate::engine::run::{RunKind, RunStatus, TodoItem};
    use crate::engine::Engine;
    use crate::server::{obs, RemoteControlState};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    /// A minimal test engine with a table the run-control routes can dispatch
    /// against (todos are surfaced by the emulation layer, not the inner
    /// engine).
    struct TestRunEngine {
        id: &'static str,
        table: CapabilityTable,
    }

    #[::async_trait::async_trait]
    impl Engine for TestRunEngine {
        fn id(&self) -> crate::engine::EngineId {
            self.id.to_string()
        }
        async fn start_job(
            &self,
            _spec: crate::engine::JobSpec,
            _ctx: &mut dyn crate::engine::EngineCtx,
        ) -> Result<crate::engine::RunId, EngineError> {
            Ok("run-engine-job".to_string())
        }
        async fn resume_job(&self, _run: &Run, _prompt: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn job_status(&self, run: &Run) -> Result<RunStatus, EngineError> {
            Ok(run.status)
        }
        async fn start_session(
            &self,
            _spec: crate::engine::SessionSpec,
            _ctx: &mut dyn crate::engine::EngineCtx,
        ) -> Result<crate::engine::RunId, EngineError> {
            Ok("run-engine-session".to_string())
        }
        async fn send_message(&self, _run: &Run, _text: &str) -> Result<(), EngineError> {
            Ok(())
        }
        async fn stop_session(&self, _run: &Run, _restart: bool) -> Result<(), EngineError> {
            Ok(())
        }
        fn job_command(
            &self,
            job: &crate::core::job::JobMetadata,
            _turn: crate::engine::TurnInput<'_>,
        ) -> Result<crate::engine::LaunchPlan, EngineError> {
            Ok(crate::engine::LaunchPlan {
                program: std::path::PathBuf::from("/bin/true"),
                args: vec![job.job_id.clone()],
                env: Vec::new(),
                stdin: crate::engine::StdinMode::Null,
                pty: false,
            })
        }
        fn classify_exit(
            &self,
            exit: crate::engine::run::ExitInfo,
            _log_tail: &[u8],
        ) -> crate::core::job::JobStatus {
            match exit.code {
                Some(0) => crate::core::job::JobStatus::Done,
                _ => crate::core::job::JobStatus::Failed,
            }
        }
        fn conversation_id(
            &self,
            job: &crate::core::job::JobMetadata,
        ) -> Option<crate::engine::ContextId> {
            job.conversation_id.clone()
        }
        async fn pause(&self, _run: &Run, _paused: bool) -> Result<(), EngineError> {
            Ok(())
        }
        fn capabilities(&self) -> CapabilityTable {
            self.table.clone()
        }
    }

    /// The observable test app with one registered engine and the extensions
    /// the run-control routes need.
    fn run_control_app() -> Router {
        let mut registry = crate::engine::EngineRegistry::new();
        let inner: std::sync::Arc<dyn Engine> = std::sync::Arc::new(TestRunEngine {
            id: "rterm",
            table: CapabilityTable::builder("rterm").native(&["pause"]).build(),
        });
        registry.register(inner);
        run_control_app_with(std::sync::Arc::new(registry))
    }

    fn run_control_app_with(registry: std::sync::Arc<crate::engine::EngineRegistry>) -> Router {
        obs::add_observability(
            crate::server::router_with_engines(
                std::sync::Arc::new(Config::default()),
                std::sync::Arc::new(AuthManager::new()),
                RemoteControlState::default(),
                registry,
            ),
            crate::server::auth::AuthState::none(),
            std::sync::Arc::new(|| Ok(())),
            std::sync::Arc::new(Vec::new),
        )
    }

    fn register_run(run: Run) {
        EngineRunRegistry::global().register(run);
    }

    async fn body_json(res: axum::response::Response) -> (StatusCode, Value) {
        let status = res.status();
        let bytes = res.into_body().collect().await.expect("body").to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn post(uri: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn run(id: &str, kind: RunKind, workdir: &str, engine: &str) -> Run {
        let mut r = Run::new(id, kind, workdir);
        r.engine = engine.to_string();
        r
    }

    #[tokio::test]
    async fn pause_on_native_engine_returns_200() {
        // The engine reports pause native; dispatching through the wrapper must
        // reach the inner engine (which records nothing but answers Ok).
        let app = run_control_app();
        register_run(run("p-run", RunKind::Job, "/workspace/proj", "rterm"));
        let res = app
            .oneshot(post("/api/v1/runs/p-run/pause", r#"{"paused":true}"#))
            .await
            .unwrap();
        let (status, body) = body_json(res).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["paused"], true);
        assert_eq!(body["run_id"], "p-run");
    }

    #[tokio::test]
    async fn compact_unsupported_answers_501() {
        // rterm declares no native compact and nothing is emulated -> 501 with
        // the E1 contract body.
        let app = run_control_app();
        register_run(run("c-run", RunKind::Job, "/workspace/proj", "rterm"));
        let res = app
            .oneshot(post("/api/v1/runs/c-run/compact", "{}"))
            .await
            .unwrap();
        let (status, body) = body_json(res).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body["code"], "unsupported");
        assert_eq!(body["engine"], "rterm");
    }

    #[tokio::test]
    async fn todo_returns_recorded_todos() {
        // The emulation's todo list is fed by the event stream; the route
        // surfaces what was recorded. Feed it through the same wrapper the app
        // resolves (the registry's emulating() accessor reaches it).
        let mut registry = crate::engine::EngineRegistry::new();
        let inner: std::sync::Arc<dyn Engine> = std::sync::Arc::new(TestRunEngine {
            id: "tterm",
            table: CapabilityTable::builder("tterm").build(),
        });
        registry.register(inner);
        let registry = std::sync::Arc::new(registry);
        let app = run_control_app_with(Arc::clone(&registry));

        let run = run("t-run", RunKind::Job, "/workspace/proj", "tterm");
        register_run(run.clone());
        let wrap = registry.emulating("tterm").expect("registered wrapper");
        wrap.record_todo(
            &run,
            TodoItem {
                content: "do a thing".into(),
                status: None,
            },
        );

        let res = app.oneshot(get("/api/v1/runs/t-run/todo")).await.unwrap();
        let (status, body) = body_json(res).await;
        assert_eq!(status, StatusCode::OK);
        let todos = body["todos"].as_array().cloned().unwrap_or_default();
        assert!(
            todos.iter().any(|t| t["content"] == "do a thing"),
            "todo list missing the recorded item: {body}"
        );
    }

    #[tokio::test]
    async fn unknown_run_answers_404() {
        let app = run_control_app();
        register_run(run("exists", RunKind::Job, "/workspace/proj", "rterm"));
        let res = app
            .oneshot(post("/api/v1/runs/nope/interrupt", "{}"))
            .await
            .unwrap();
        let (status, body) = body_json(res).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "not_found");
    }

    #[tokio::test]
    async fn unknown_job_on_run_control_route_answers_404_not_500() {
        let app = run_control_app();
        let res = app
            .oneshot(post("/api/v1/jobs/nope-job/interrupt", "{}"))
            .await
            .unwrap();
        let (status, body) = body_json(res).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "not_found");
    }
}

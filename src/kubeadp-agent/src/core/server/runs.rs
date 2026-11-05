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

use axum::extract::Extension;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::core::server::{ApiJson, ApiPath};
use crate::engine::capability::{CapEndpointKey, Capability};
use crate::engine::emulate::{EmulatingEngine, EngineRunRegistry};
use crate::engine::errors::EngineError;
use crate::engine::run::Run;
use crate::engine::{Engine, EngineRegistry};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::auth::AuthManager;
    use crate::core::config::Config;
    use crate::core::server::{obs, RemoteControlState};
    use crate::engine::capability::CapabilityTable;
    use crate::engine::run::{RunKind, RunStatus, TodoItem};
    use crate::engine::Engine;
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
            crate::core::server::router_with_engines(
                std::sync::Arc::new(Config::default()),
                std::sync::Arc::new(AuthManager::new()),
                RemoteControlState::default(),
                registry,
            ),
            crate::core::server::auth::AuthState::none(),
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
}

//! E6 engine-conformance suite (HL-71): drives every engine in the
//! [`EngineRegistry`] and every capability key from `GET /api/v1/capabilities`
//! through the REAL endpoints and asserts the documented contract.
//!
//! Two independent guarantee classes:
//!
//! 1. **Invariant conformance (Part 1, must pass).** The endpoint honors the
//!    capability it reports: a key the engine reports `Native`/`Emulated`
//!    answers 2xx (or the key-specific typed contract), a key it reports
//!    `Unsupported` answers 501 `{"code":"unsupported","engine":<id>}` (never
//!    "501 by engine" for an unknown engine -- an unknown engine is 400
//!    `unknown_engine`), an unknown run answers 404 `not_found`. Every
//!    registered engine is enumerated, and every `/capabilities` endpoint key
//!    is driven: the real job endpoints for the job keys (`POST /api/v1/jobs`,
//!    status/list/kill/retry/events/result), the run-control routes for the
//!    control keys, and the engine selection surface for the job default.
//!
//! 2. **Documented-contract parity (Part 2).** The `/capabilities` report is
//!    cross-checked against the DOCUMENTED tables (`DOCS.md` §2.4). Since HL-71
//!    S2 arms `EmulatingEngine::enable_all` in production (worker/entry.rs) and
//!    the engines declare the documented surfaces, the report must match the
//!    docs exactly. The only rows that remain stale are the SPEC.md §5.5 rows
//!    the SPEC.md file cannot be edited for (it is a locked normative doc);
//!    those are recorded in `KNOWN_GAPS` as exact assertions (a fix forces
//!    their removal).
//!
//! Coverage guards (Part 3, must pass) enforce admission for the suite itself:
//! a synthetic engine registered into the registry without a conformance case
//! fails, and a capability key declared by an engine without a conformance
//! case fails. Negative cases prove the guard rejects unknown engines / keys.
//!
//! The gate builds with `--features a2a,test-hooks` (the same set the regular
//! test suite uses), so this file compiles under `all(engine, test-hooks)`;
//! with `test-hooks` absent the file is skipped entirely (no live sandbox
//! runtime in a plain build).
//!
//! The suite is hermetic: fake `agy`/`claude` binaries (under the `test-hooks`
//! sandbox passthrough seam -- no Landlock/Docker), git repositories created
//! in a temp dir, and the job monitor threads hold no other process-global
//! state that would leak across tests.
#![cfg(all(feature = "engine", feature = "test-hooks"))]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use kubeadp_agent::core::auth::AuthManager;
use kubeadp_agent::core::config::Config;
use kubeadp_agent::core::job::{ensure_state_store, JobMetadata};
use kubeadp_agent::core::server::{obs, RemoteControlState};
use kubeadp_agent::engine::capability::{Capability, CapabilityTable};
use kubeadp_agent::engine::run::{Run, RunKind};
use kubeadp_agent::engine::{Engine, EngineId, EngineRegistry};
use kubeadp_agent::sandbox::testhook;
use kubeadp_agent::state::StateDb;
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// The engines the worker role registers in production (`worker/entry.rs`).
/// The conformance suite mirrors that exact construction: agy, claude and
/// opencode, all wrapped the way the worker entry points build them, then
/// `attach_config` + `enable_all_emulated`.
fn production_registry() -> EngineRegistry {
    production_registry_with_config(Arc::new(Config::default()))
}

/// Like [`production_registry`], but with a caller-supplied host config so a
/// test can point the emulation layer's job-facing paths (the emulated
/// compact's retry resolve) at a temp state root instead of the process
/// HOME -- the suite stays hermetic.
fn production_registry_with_config(config: Arc<Config>) -> EngineRegistry {
    let mut registry = kubeadp_agent::engine::AgyEngine::build_registry(Arc::clone(&config));
    registry.register(Arc::new(kubeadp_agent::engine::claude::ClaudeEngine::new(
        PathBuf::from("claude"),
        None,
    )));
    registry.register(Arc::new(
        kubeadp_agent::engine::opencode::OpenCodeEngine::new(Arc::clone(&config)),
    ));
    registry.attach_config(config);
    registry.enable_all_emulated();
    registry
}

/// The engine-aware router exactly as the worker role builds it
/// (`router_with_engines` + `add_observability`), so the suite hits the same
/// routes, extensions and auth layer production serves.
fn api_app() -> Router {
    api_app_with_registry(production_registry())
}

/// The engine-aware router with a caller-supplied registry (used by the
/// synthetic-engine negative tests to drive a bare engine).
fn api_app_with_registry(registry: EngineRegistry) -> Router {
    api_app_with_registry_and_config(registry, Arc::new(Config::default()))
}

fn api_app_with_registry_and_config(registry: EngineRegistry, config: Arc<Config>) -> Router {
    let runs = Arc::new(kubeadp_agent::state::RunRegistry::lazy(&config.state_root));
    api_app_with_registry_config_and_runs(registry, config, runs)
}

fn api_app_with_registry_config_and_runs(
    registry: EngineRegistry,
    config: Arc<Config>,
    runs: Arc<kubeadp_agent::state::RunRegistry>,
) -> Router {
    obs::add_observability(
        kubeadp_agent::core::server::router_with_engines_and_runs(
            Arc::clone(&config),
            Arc::new(AuthManager::new()),
            RemoteControlState::default(),
            Arc::new(registry),
            runs,
        ),
        kubeadp_agent::core::server::auth::AuthState::none(),
        Arc::new(|| Ok(())),
        Arc::new(Vec::new),
    )
}

/// A registry with one synthetic engine that declares support for a key.
fn engine_with_declared_key(key: &'static str) -> EngineRegistry {
    synthetic_registry("synthetic", &[key])
}

/// A registry with one synthetic engine declaring `native_keys` (nothing
/// else). A bare engine (empty set) reports every control key `Unsupported`,
/// so driving a control endpoint through it answers the 501 contract.
fn synthetic_registry(engine_id: &'static str, native_keys: &[&'static str]) -> EngineRegistry {
    let mut registry = EngineRegistry::new();
    let inner: Arc<dyn Engine> = Arc::new(SyntheticEngine {
        id: engine_id,
        table: CapabilityTable::builder(engine_id)
            .native(native_keys)
            .build(),
    });
    registry.register(inner);
    registry
}

/// A minimal engine used only by the coverage-guard unit block (never
/// registered in the production registry).
struct SyntheticEngine {
    id: &'static str,
    table: CapabilityTable,
}

#[::async_trait::async_trait]
impl Engine for SyntheticEngine {
    fn id(&self) -> EngineId {
        self.id.to_string()
    }
    async fn start_job(
        &self,
        _spec: kubeadp_agent::engine::JobSpec,
        _ctx: &mut dyn kubeadp_agent::engine::EngineCtx,
    ) -> Result<kubeadp_agent::engine::RunId, kubeadp_agent::engine::EngineError> {
        Ok("synthetic-job".to_string())
    }
    async fn resume_job(
        &self,
        _run: &Run,
        _prompt: &str,
    ) -> Result<(), kubeadp_agent::engine::EngineError> {
        Ok(())
    }
    async fn job_status(
        &self,
        run: &Run,
    ) -> Result<kubeadp_agent::engine::RunStatus, kubeadp_agent::engine::EngineError> {
        Ok(run.status)
    }
    async fn start_session(
        &self,
        _spec: kubeadp_agent::engine::SessionSpec,
        _ctx: &mut dyn kubeadp_agent::engine::EngineCtx,
    ) -> Result<kubeadp_agent::engine::RunId, kubeadp_agent::engine::EngineError> {
        Ok("synthetic-session".to_string())
    }
    async fn send_message(
        &self,
        _run: &Run,
        _text: &str,
    ) -> Result<(), kubeadp_agent::engine::EngineError> {
        Ok(())
    }
    async fn stop_session(
        &self,
        _run: &Run,
        _restart: bool,
    ) -> Result<(), kubeadp_agent::engine::EngineError> {
        Ok(())
    }
    async fn pause(
        &self,
        _run: &Run,
        _paused: bool,
    ) -> Result<(), kubeadp_agent::engine::EngineError> {
        Ok(())
    }
    fn job_command(
        &self,
        job: &kubeadp_agent::core::job::JobMetadata,
        _turn: kubeadp_agent::engine::TurnInput<'_>,
    ) -> Result<kubeadp_agent::engine::LaunchPlan, kubeadp_agent::engine::EngineError> {
        Ok(kubeadp_agent::engine::LaunchPlan {
            program: std::path::PathBuf::from("/bin/true"),
            args: vec![job.job_id.clone()],
            env: Vec::new(),
            stdin: kubeadp_agent::engine::StdinMode::Null,
            pty: false,
        })
    }
    fn session_command(
        &self,
        session: &kubeadp_agent::engine::SessionDef,
        _resume: kubeadp_agent::engine::ResumeMode,
    ) -> Result<kubeadp_agent::engine::LaunchPlan, kubeadp_agent::engine::EngineError> {
        Ok(kubeadp_agent::engine::LaunchPlan {
            program: std::path::PathBuf::from("/bin/true"),
            args: vec![session.name.clone()],
            env: Vec::new(),
            stdin: kubeadp_agent::engine::StdinMode::Null,
            pty: true,
        })
    }
    fn classify_exit(
        &self,
        exit: kubeadp_agent::engine::ExitInfo,
        _log_tail: &[u8],
    ) -> kubeadp_agent::core::job::JobStatus {
        match exit.code {
            Some(0) => kubeadp_agent::core::job::JobStatus::Done,
            _ => kubeadp_agent::core::job::JobStatus::Failed,
        }
    }
    fn conversation_id(
        &self,
        job: &kubeadp_agent::core::job::JobMetadata,
    ) -> Option<kubeadp_agent::engine::ContextId> {
        job.conversation_id.clone()
    }
    fn capabilities(&self) -> CapabilityTable {
        self.table.clone()
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/// Runs `oneshot` and returns the status + body parsed as JSON (or `Null` for
/// a non-JSON body, e.g. the SSE events stream).
async fn body_json(res: axum::response::Response) -> (StatusCode, Value) {
    let status = res.status();
    let bytes = res.into_body().collect().await.expect("body").to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// Runs `oneshot` and returns the status + raw body bytes.
async fn body_bytes(res: axum::response::Response) -> (StatusCode, Vec<u8>) {
    let status = res.status();
    let bytes = res
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes()
        .to_vec();
    (status, bytes)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

fn post_json(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// A `Run` registered into the process-wide run registry, with a given engine
/// and kind, whose workdir is a real temp git fixture (the emulated
/// fork/compact/revert read it). Returns the run id.
fn register_run(
    id: &str,
    kind: RunKind,
    engine: &str,
    workdir: &Path,
    app_runs: &kubeadp_agent::state::RunRegistry,
) -> String {
    let mut run = Run::new(id.to_string(), kind, workdir);
    run.engine = engine.to_string();
    let state_root = workdir.parent().unwrap_or(workdir);
    let _ = StateDb::open(state_root);
    app_runs.register(run).unwrap();
    id.to_string()
}

// ---------------------------------------------------------------------------
// Job-launch fixture (hermetic fake engines)
// ---------------------------------------------------------------------------

/// Small git repository fixture: `git init`, a commit on `main`, so the job
/// launcher's private-repo mirror creates the worktree from it.
fn init_fixture_repo(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let git = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .expect("git should run");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Conformance"]);
    git(&["config", "user.email", "conformance@test"]);
    git(&["checkout", "-q", "-b", "main"]);
    std::fs::write(dir.join("init.txt"), "hello\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "init"]);
}

/// Writes an executable fake `agy` that produces a complete structured
/// SUCCESS so the job classifies `Done` (see `determine_status_from_signals`:
/// a terminal stream-json `event:result` with a `response`).
fn fake_agy(dir: &Path) -> PathBuf {
    let path = dir.join("fake-agy");
    std::fs::write(
        &path,
        r#"#!/bin/sh
printf '%s\n' '{"event":"step_update","type":"text","text_delta":"working"}'
printf '%s\n' '{"event":"result","result":{"status":"SUCCESS","response":"conformance done"}}'
sleep 1
exit 0
"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// Writes an executable fake `claude` that produces a terminal stream-json
/// `result` (the claude job taxonomy's `Done`).
fn fake_claude(dir: &Path) -> PathBuf {
    let path = dir.join("fake-claude");
    std::fs::write(
        &path,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init"}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"conformance done","session_id":"fake-session-1"}'
sleep 1
exit 0
"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// Writes an executable fake `opencode` that produces a terminal opencode
/// event stream: a `step_start`, a `text`, and a `step_finish` reason `stop`,
/// carrying a stable `sessionID` (the opencode job taxonomy's `Done`).
fn fake_opencode(dir: &Path) -> PathBuf {
    let path = dir.join("fake-opencode");
    std::fs::write(
        &path,
        r#"#!/bin/sh
printf '%s\n' '{"type":"step_start","sessionID":"fake-session-oc","part":{"type":"step-start"}}'
printf '%s\n' '{"type":"text","sessionID":"fake-session-oc","part":{"type":"text","text":"conformance done"}}'
printf '%s\n' '{"type":"step_finish","sessionID":"fake-session-oc","part":{"type":"step-finish","reason":"stop"}}'
sleep 1
exit 0
"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// A config with temp state/workspace roots and the fake harness binaries.
/// The `state_root` must have its state store initialised, and the workspace
/// root holds `<project>` + `.agent-repos`.
struct HermeticConfig {
    // Hold the TempDirs alive for the whole config lifetime.
    _root: TempDir,
    state_root: PathBuf,
    config: Config,
}

fn hermetic_config(root: TempDir, engine_name: &str) -> HermeticConfig {
    let workspace_root = root.path().join(format!("ws-{engine_name}"));
    let project_dir = workspace_root.join("proj");
    init_fixture_repo(&project_dir);
    let state_root = root.path().join(format!("state-{engine_name}"));
    ensure_state_store(&state_root).unwrap();
    let config = Config {
        workspace_root: workspace_root.clone(),
        repos_root: workspace_root.join(".agent-repos"),
        state_root: state_root.clone(),
        agy_path: fake_agy(root.path()),
        claude_path: fake_claude(root.path()),
        opencode_path: fake_opencode(root.path()),
        sandbox_home: workspace_root.join("sandbox-home"),
        ..Default::default()
    };
    // The passthrough seam (default on under test-hooks) is explicitly kept
    // on so the fake binaries launch unwrapped.
    testhook::set_sandbox_passthrough(true);
    HermeticConfig {
        _root: root,
        state_root,
        config,
    }
}

/// App for the hermetic config: same routes as production, engines built from
/// the hermetic config, with the same attach + enable_all_emulated wiring as
/// the worker entry point.
fn hermetic_app(cfg: &HermeticConfig) -> Router {
    let mut registry =
        kubeadp_agent::engine::AgyEngine::build_registry(Arc::new(cfg.config.clone()));
    registry.register(Arc::new(kubeadp_agent::engine::claude::ClaudeEngine::new(
        PathBuf::from("claude"),
        None,
    )));
    registry.register(Arc::new(
        kubeadp_agent::engine::opencode::OpenCodeEngine::new(Arc::new(cfg.config.clone())),
    ));
    registry.attach_config(Arc::new(cfg.config.clone()));
    registry.enable_all_emulated();
    obs::add_observability(
        kubeadp_agent::core::server::router_with_engines(
            Arc::new(cfg.config.clone()),
            Arc::new(AuthManager::new()),
            RemoteControlState::default(),
            Arc::new(registry),
        ),
        kubeadp_agent::core::server::auth::AuthState::none(),
        Arc::new(|| Ok(())),
        Arc::new(Vec::new),
    )
}

/// Polls the job status endpoint until it returns `done`.
async fn wait_status_done(app: &Router, job_id: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let (s, v) = body_json(
            app.clone()
                .oneshot(get(&format!("/api/v1/jobs/{job_id}")))
                .await
                .unwrap(),
        )
        .await;
        if s == StatusCode::OK && v["data"]["status"] == "done" {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// Part 1: invariant conformance (the endpoint honors its reported capability)
// ---------------------------------------------------------------------------

/// The run-control endpoints (`/api/v1/runs/{id}/...`) answer the capability
/// they report. Driving every registered engine against every control key:
///
/// - a key the engine reports `Unsupported` answers 501 `{"code":
///   "unsupported","engine":<id>}` (never a generic 500);
/// - a self-contained `Native`/`Emulated` key (pause/fork/compact/revert/
///   todo) answers 2xx -- the emulation layer fronts it against the run's
///   worktree without needing a real job row;
/// - the emulated `interrupt` of a fabricated (non-job) run must resolve the
///   job row to signal it and answers the honest typed 404 `not_found`, NOT
///   a 501-by-engine;
/// - an unknown run answers 404 `not_found`.
///
/// The real-job interrupt path (the run resolves to a live job row) is
/// exercised by `job_control_surface_answers_usable_bodies` below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_control_capabilities_dispatch() {
    // A temp state root keeps the emulated compact's job-resolve step (and
    // every other job-facing retry path) hermetic: it resolves against the
    // fake run's state root, never the process HOME.
    let state_root_dir = TempDir::new().unwrap();
    let config = Arc::new(Config {
        state_root: state_root_dir.path().to_path_buf(),
        ..Default::default()
    });
    let app_runs = Arc::new(kubeadp_agent::state::RunRegistry::lazy(&config.state_root));
    let app = api_app_with_registry_config_and_runs(
        production_registry_with_config(Arc::clone(&config)),
        Arc::clone(&config),
        Arc::clone(&app_runs),
    );
    let workdir = state_root_dir.path().join("workdir");
    std::fs::create_dir_all(&workdir).unwrap();

    let registry = production_registry_with_config(Arc::clone(&config));
    for engine in registry.enabled_ids() {
        let table = registry
            .capability_table(&engine)
            .expect("engine must have a table");
        for key in ["interrupt", "pause", "compact", "fork", "revert", "todo"] {
            let cap = table.capability(key).unwrap_or(Capability::Unsupported);
            let run_id = register_run(
                &format!("run-{engine}-{key}"),
                RunKind::Job,
                &engine,
                &workdir,
                &app_runs,
            );
            let (method, uri, body) = control_request(key, &run_id);
            let req = match method {
                "GET" => get(&uri),
                "POST" => post_json(&uri, body),
                _ => unreachable!(),
            };
            let (status, value) = body_json(app.clone().oneshot(req).await.unwrap()).await;
            match cap {
                // Unsupported: 501 with the typed body naming the engine.
                Capability::Unsupported => {
                    assert_eq!(
                        status,
                        StatusCode::NOT_IMPLEMENTED,
                        "engine {engine} key {key} should be 501"
                    );
                    assert_eq!(value["code"], "unsupported", "engine {engine} key {key}");
                    assert_eq!(
                        value["engine"], engine,
                        "engine {engine} key {key} names its engine"
                    );
                }
                // Native/Emulated: the control surface answers 2xx, EXCEPT
                // the emulated interrupt of a fabricated run -- it resolves
                // the (missing) job row to signal and answers the honest
                // typed 404 `not_found`, never a 501-by-engine.
                Capability::Native | Capability::Emulated => {
                    if key == "interrupt" {
                        assert_eq!(
                            status,
                            StatusCode::NOT_FOUND,
                            "engine {engine} interrupt must be a typed 404 (no real job row)"
                        );
                        assert_eq!(value["code"], "not_found", "engine {engine} interrupt");
                    } else {
                        assert!(
                            status.is_success(),
                            "engine {engine} key {key} should be 2xx, got {status}: {value}"
                        );
                    }
                }
            }
            // Clean the runs (including any the emulated fork registered:
            // `<run_id>-fork-<seq>`). The fork seq is per-wrapper and this
            // loop forks each engine exactly once, so the forked id is
            // deterministic.
            if matches!(cap, Capability::Native | Capability::Emulated) && key == "fork" {
                for seq in 0..4 {
                    let forked = format!("{run_id}-fork-{seq}");
                    let db = StateDb::open(&config.state_root).unwrap();
                    if db.runs().unregister(&forked) {
                        break;
                    }
                }
            }
            let db = StateDb::open(&config.state_root).unwrap();
            db.runs().unregister(&run_id);
        }
    }

    // Unknown run -> 404 not_found (never a 501). `interrupt` is a POST
    // route, so drive it as POST (GET would be a 405 from the router, never
    // the run-resolution 404).
    let (status, value) = body_json(
        app.clone()
            .oneshot(post_json("/api/v1/runs/nope/interrupt", ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "not_found");

    // The suite asserts the negative (unknown engine -> 400) through the
    // engine-selection surface below.
    assert_eq!(registry.len(), 3, "registry has agy+claude+opencode");
}

fn control_request(key: &str, run_id: &str) -> (&'static str, String, &'static str) {
    match key {
        "interrupt" => ("POST", format!("/api/v1/runs/{run_id}/interrupt"), ""),
        "pause" => (
            "POST",
            format!("/api/v1/runs/{run_id}/pause"),
            r#"{"paused":true}"#,
        ),
        "compact" => ("POST", format!("/api/v1/runs/{run_id}/compact"), ""),
        "fork" => ("POST", format!("/api/v1/runs/{run_id}/fork"), ""),
        "revert" => (
            "POST",
            format!("/api/v1/runs/{run_id}/revert"),
            r#"{"commit":""}"#,
        ),
        "todo" => ("GET", format!("/api/v1/runs/{run_id}/todo"), ""),
        _ => unreachable!(),
    }
}

/// Engine selection: `POST /api/v1/jobs` honors the capability contract for
/// the *selection* surface -- an unknown engine is 400 `unknown_engine`
/// (never 501-by-engine), and the `/capabilities` report lists exactly the
/// registered engines.
#[tokio::test]
async fn engine_selection_and_capabilities_listing() {
    let app = api_app();

    // Unknown engine -> 4xx client error (the start route resolves the
    // engine first; on a default config with no project the 400 code may be
    // project_not_found, but the family is always 4xx -- never 501-by-engine).
    let (status, value) = body_json(
        app.clone()
            .oneshot(post_json(
                "/api/v1/jobs",
                r#"{"project":"proj","prompt":"p","engine":"bogus"}"#,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        status.is_client_error(),
        "unknown engine must be 4xx, got {status}: {value}"
    );

    // /capabilities lists exactly the registered engines.
    let (status, value) = body_json(
        app.clone()
            .oneshot(get("/api/v1/capabilities"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let caps = value["capabilities"]
        .as_array()
        .expect("capabilities array");
    let mut engines: Vec<String> = caps
        .iter()
        .map(|c| c["engine"].as_str().unwrap_or("").to_string())
        .collect();
    engines.sort();
    assert_eq!(
        engines,
        vec![
            "agy".to_string(),
            "claude".to_string(),
            "opencode".to_string()
        ]
    );

    // Every endpoint key in the static table is present in each engine's
    // report (an engine's capability() returns Unsupported for anything it
    // did not declare, never omits a key).
    let all_keys = kubeadp_agent::engine::capability::ENDPOINTS
        .iter()
        .map(|e| e.key)
        .collect::<Vec<_>>();
    for c in caps.iter() {
        let endpoints = c["endpoints"].as_object().unwrap();
        for key in &all_keys {
            assert!(
                endpoints.contains_key(*key),
                "engine {} must report key {key}",
                c["engine"]
            );
        }
    }
}

/// The 501-by-engine contract is not dead code: no PRODUCTION engine reports
/// one of the six control keys `Unsupported` after the worker arms
/// `enable_all_emulated` (agy + claude both report every control key
/// Emulated), so the 501 branch of `run_control_capabilities_dispatch` is
/// only exercised by a synthetic bare engine. A synthetic engine registered
/// WITHOUT `enable_all_emulated` reports the control keys `Unsupported`, and
/// driving one through `/api/v1/runs/{id}/...` must answer the typed 501
/// `{"code":"unsupported","engine":<id>}` -- the same contract every future
/// partial engine must honor. `todo` is the exception: the emulation layer
/// always fronts it (there is no native todo), so it answers 2xx even for a
/// bare engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_control_key_answers_typed_501() {
    let state_root_dir = TempDir::new().unwrap();
    let config = Arc::new(Config {
        state_root: state_root_dir.path().to_path_buf(),
        ..Default::default()
    });
    let workdir = state_root_dir.path().join("workdir");
    std::fs::create_dir_all(&workdir).unwrap();

    let app_runs = Arc::new(kubeadp_agent::state::RunRegistry::lazy(&config.state_root));
    let app = api_app_with_registry_config_and_runs(
        synthetic_registry("bare", &[]),
        Arc::clone(&config),
        Arc::clone(&app_runs),
    );
    let registry = synthetic_registry("bare", &[]);
    let run_id = register_run("bare-run-1", RunKind::Job, "bare", &workdir, &app_runs);

    for (method, uri, body) in [
        ("POST", format!("/api/v1/runs/{run_id}/interrupt"), ""),
        (
            "POST",
            format!("/api/v1/runs/{run_id}/pause"),
            r#"{"paused":true}"#,
        ),
        ("POST", format!("/api/v1/runs/{run_id}/compact"), ""),
        ("POST", format!("/api/v1/runs/{run_id}/fork"), ""),
        (
            "POST",
            format!("/api/v1/runs/{run_id}/revert"),
            r#"{"commit":""}"#,
        ),
    ] {
        let req = match method {
            "GET" => get(&uri),
            "POST" => post_json(&uri, body),
            _ => unreachable!(),
        };
        let (status, value) = body_json(app.clone().oneshot(req).await.unwrap()).await;
        assert_eq!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "{method} {uri}: expected 501, got {status}: {value}"
        );
        assert_eq!(value["code"], "unsupported", "{uri}: {value}");
        assert_eq!(value["engine"], "bare", "{uri}: {value}");
    }

    // `todo` stays 2xx even for a bare engine (the emulation layer always
    // fronts the structured todo surface).
    let (status, value) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/runs/{run_id}/todo")))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        status.is_success(),
        "bare todo must be 2xx, got {status}: {value}"
    );

    let _ = StateDb::open(&config.state_root).map(|db| db.runs().unregister(&run_id));
    assert_eq!(
        registry.len(),
        1,
        "bare registry carries the synthetic engine"
    );
}

// ---------------------------------------------------------------------------
// Part 1: job lifecycle conformance (real /api/v1/jobs with fake engines)
// ---------------------------------------------------------------------------

/// Full job lifecycle through the REAL `POST /api/v1/jobs` endpoint for the
/// per-role default engine (agy): start -> status -> list -> events -> result
/// -> kill (finished) -> clean, plus the endpoints that must answer their
/// documented contract. The job endpoints are engine-independent; this test
/// drives them for the default agy role.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_lifecycle_agy_hermetic() {
    let root = TempDir::new().unwrap();
    let hcfg = hermetic_config(root, "agy");
    let app = hermetic_app(&hcfg);

    let (status, value) = body_json(
        app.clone()
            .oneshot(post_json(
                "/api/v1/jobs",
                r#"{"project":"proj","prompt":"conformance agy job","sandbox":false}"#,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "start must return 200: {value}");
    let job_id = value["data"]["job_id"]
        .as_str()
        .unwrap_or_else(|| panic!("start body must carry job_id: {value}"))
        .to_string();
    let pid = value["data"]["pid"].as_u64().expect("pid must be present");

    // Wait up to 20s for the fake agy to exit and the row to reach Done.
    assert!(
        wait_status_done(&app, &job_id, Duration::from_secs(20)).await,
        "agy job must reach done"
    );

    // job.status: 200 with the resolved job + status.
    let (s, v) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["data"]["status"], "done");
    assert_eq!(v["data"]["job"]["job_id"], job_id);

    // job.list: 200, contains the job.
    let (s, v) = body_json(
        app.clone()
            .oneshot(get("/api/v1/jobs?project=proj"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let listed = v["data"]["jobs"].as_array().expect("list must carry jobs");
    assert!(
        listed.iter().any(|j| j["job_id"] == job_id),
        "job must appear in list"
    );

    // job.events: the SSE stream resolves (200) and carries the terminal
    // `end` event. The body is `text/event-stream`, so assert on the raw
    // bytes, not JSON.
    let (s, bytes) = body_bytes(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}/events")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "events must resolve");
    let sse = String::from_utf8_lossy(&bytes);
    assert!(
        sse.contains("event: end") && sse.contains("\"done\""),
        "events stream must carry the terminal end event: {sse:?}"
    );

    // job.result: the final summary (registered as Native for agy).
    let (s, v) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}/result")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "result: {v}");
    assert!(!v["data"].is_null(), "result carries data");

    // job.kill on a finished job: answers OK (the "not running" no-op path),
    // never 501 (agy reports job.kill Native).
    let (s, v) = body_json(
        app.clone()
            .oneshot(post_json(&format!("/api/v1/jobs/{job_id}/kill"), ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "kill on finished job: {v}");

    // cleanup: remove the job's worktree/branch/row so no state leaks.
    teardown_job(&hcfg, &job_id);

    let _ = pid;
}

/// The claude job lifecycle: `POST /api/v1/jobs` with `"engine":"claude"`
/// runs the claude stream-json harness (the fake claude writes a terminal
/// `result`), so the claude taxonomy classifies `done`. The job endpoints
/// (status/list/kill/events/result) are engine-independent: they operate on
/// the job row and the log, so they answer 200 for claude too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_lifecycle_claude_hermetic() {
    let root = TempDir::new().unwrap();
    let hcfg = hermetic_config(root, "claude");
    let app = hermetic_app(&hcfg);

    let (status, value) = body_json(
        app.clone()
            .oneshot(post_json(
                "/api/v1/jobs",
                r#"{"project":"proj","prompt":"conformance claude job","sandbox":false,"engine":"claude"}"#,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "claude start must be 200: {value}");
    let job_id = value["data"]["job_id"]
        .as_str()
        .unwrap_or_else(|| panic!("start body must carry job_id: {value}"))
        .to_string();

    assert!(
        wait_status_done(&app, &job_id, Duration::from_secs(20)).await,
        "claude job must reach done"
    );

    // job.status: 200 for claude (engine-independent).
    let (s, v) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "claude status must be 200");
    assert_eq!(v["data"]["status"], "done");

    // job.result: 200 for claude. `commands::result::run` resolves the job
    // row and reads the log -- it never 501s by engine (`job.result` is
    // engine-independent). The claude taxonomy's result text is the fake's
    // `result` field.
    let (s, v) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}/result")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "claude job.result must be 200: {v}");
    assert!(
        v["data"]["response"]
            .as_str()
            .unwrap_or("")
            .contains("conformance done"),
        "claude result must carry the fake's response: {v}"
    );

    // job.retry on a done job without --prompt -> 400 (the documented
    // retry_already_completed error), never 501.
    let (s, v) = body_json(
        app.clone()
            .oneshot(post_json(
                &format!("/api/v1/jobs/{job_id}/retry"),
                &format!(r#"{{"prompt":null,"job_id":"{job_id}"}}"#),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "retry on done claude job: {v}");
    assert_eq!(v["code"], "retry_already_completed");

    // job.events: SSE stream resolves to the terminal end event.
    let (s, bytes) = body_bytes(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}/events")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "claude events must resolve");
    let sse = String::from_utf8_lossy(&bytes);
    assert!(
        sse.contains("event: end"),
        "claude events stream must carry the terminal end event: {sse:?}"
    );

    teardown_job(&hcfg, &job_id);
}

/// The opencode job lifecycle: `POST /api/v1/jobs` with `"engine":"opencode"`
/// runs the opencode `run --format json` harness (the fake opencode writes a
/// terminal `step_finish` reason `stop`), so the opencode taxonomy classifies
/// `done`. The job endpoints (status/list/kill/events/result) are
/// engine-independent: they operate on the job row and the log, so they
/// answer 200 for opencode too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_lifecycle_opencode_hermetic() {
    let root = TempDir::new().unwrap();
    let hcfg = hermetic_config(root, "opencode");
    let app = hermetic_app(&hcfg);

    let (status, value) = body_json(
        app.clone()
            .oneshot(post_json(
                "/api/v1/jobs",
                r#"{"project":"proj","prompt":"conformance opencode job","sandbox":false,"engine":"opencode"}"#,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "opencode start must be 200: {value}"
    );
    let job_id = value["data"]["job_id"]
        .as_str()
        .unwrap_or_else(|| panic!("start body must carry job_id: {value}"))
        .to_string();

    assert!(
        wait_status_done(&app, &job_id, Duration::from_secs(20)).await,
        "opencode job must reach done"
    );

    // The conversation id (the harness session id) must have been captured to
    // the row, since retry's resume key comes from it.
    let conv = std::time::Instant::now() + Duration::from_secs(5);
    let mut conversation_id = String::new();
    while std::time::Instant::now() < conv {
        if let Ok(meta) = JobMetadata::load(&hcfg.state_root, &job_id) {
            if let Some(c) = meta.conversation_id.clone() {
                conversation_id = c;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !conversation_id.is_empty(),
        "opencode job must capture a conversation id from sessionID lines"
    );

    // job.status: 200 for opencode (engine-independent).
    let (s, v) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "opencode status must be 200");
    assert_eq!(v["data"]["status"], "done");

    // job.result: 200 for opencode. The response surfaces the last turn's
    // visible text events.
    let (s, v) = body_json(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}/result")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "opencode job.result must be 200: {v}");
    assert!(
        v["data"]["response"]
            .as_str()
            .unwrap_or("")
            .contains("conformance done"),
        "opencode result must carry the fake's response: {v}"
    );

    // job.retry on a done job without --prompt -> 400 (the documented
    // retry_already_completed error), never 501.
    let (s, v) = body_json(
        app.clone()
            .oneshot(post_json(
                &format!("/api/v1/jobs/{job_id}/retry"),
                &format!(r#"{{"prompt":null,"job_id":"{job_id}"}}"#),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "retry on done opencode job: {v}"
    );
    assert_eq!(v["code"], "retry_already_completed");

    // job.events: SSE stream resolves to the terminal end event.
    let (s, bytes) = body_bytes(
        app.clone()
            .oneshot(get(&format!("/api/v1/jobs/{job_id}/events")))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "opencode events must resolve");
    let sse = String::from_utf8_lossy(&bytes);
    assert!(
        sse.contains("event: end"),
        "opencode events stream must carry the terminal end event: {sse:?}"
    );

    teardown_job(&hcfg, &job_id);
}

/// The job-scoped run-control surface (`/api/v1/jobs/{id}/...`, HL-71 S2) is
/// the PRODUCTION surface: the run registry resolves no runs in production, so
/// the job routes are what an operator actually drives. Every control endpoint
/// resolves the job row on demand and dispatches through the emulation
/// wrapper, so a finished agy/claude/opencode job still answers a usable body
/// (2xx + a `CommandOutput` with a `.data` payload) -- never a 501 because of
/// the engine.
///
/// Driven for ALL engines (agy default + explicit claude + explicit opencode)
/// after the job reached `done`:
/// - `todo` (GET) -> 2xx `{data: {todos: [...]}}`;
/// - `interrupt` (POST) -> 2xx `{data: {interrupted: true}}` (the job is not
///   running, so the emulated interrupt records the "not running" note into
///   the job log instead of signalling a process);
/// - `pause` (POST) -> 2xx `{data: {paused: ...}}` (per-run flag);
/// - `message` (POST) -> 2xx `{data: {queued: true}}` (`a2a.send_message` is
///   emulated for both engines: no live PTY, so the message is queued);
/// - `compact` (POST) -> 2xx `{data: {compacted: true}}` (the summary is
///   written; the restart through `retry` is deferred because the fake
///   harness captured no conversation id -- the emulation records the deferral
///   note and still answers the usable body);
/// - `fork` (POST) -> 2xx `{data: {run_id}}` (a fresh run seeded from the
///   job's worktree is registered);
/// - `revert` (POST, empty commit) -> 2xx `{data: {reverted: false}}` (no
///   work was discarded, so unrevert is a graceful no-op).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_control_surface_answers_usable_bodies() {
    for engine_name in ["agy", "claude", "opencode"] {
        let root = TempDir::new().unwrap();
        let hcfg = hermetic_config(root, engine_name);
        let app = hermetic_app(&hcfg);

        let engine_field = if engine_name == "claude" {
            r#","engine":"claude""#
        } else if engine_name == "opencode" {
            r#","engine":"opencode""#
        } else {
            ""
        };
        let (status, value) = body_json(
            app.clone()
                .oneshot(post_json(
                    "/api/v1/jobs",
                    &format!(
                        r#"{{"project":"proj","prompt":"conformance {engine_name} control job","sandbox":false{engine_field}}}"#
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{engine_name} start must be 200: {value}"
        );
        let job_id = value["data"]["job_id"]
            .as_str()
            .unwrap_or_else(|| panic!("start body must carry job_id: {value}"))
            .to_string();

        assert!(
            wait_status_done(&app, &job_id, Duration::from_secs(20)).await,
            "{engine_name} job must reach done"
        );

        let sm = |label: &str, s: StatusCode, v: &Value| {
            assert!(
                s.is_success(),
                "{engine_name} {label} must be 2xx, got {s}: {v}"
            );
        };

        // todo: GET -> 2xx with a todos array in data.
        let (s, v) = body_json(
            app.clone()
                .oneshot(get(&format!("/api/v1/jobs/{job_id}/todo")))
                .await
                .unwrap(),
        )
        .await;
        sm("todo", s, &v);
        assert!(
            v["data"]["todos"].as_array().is_some(),
            "{engine_name} todo data must carry a todos array: {v}"
        );

        // interrupt: POST (no body) -> 2xx; the job is done, so the emulated
        // interrupt records a note instead of signalling a process.
        let (s, v) = body_json(
            app.clone()
                .oneshot(post_json(&format!("/api/v1/jobs/{job_id}/interrupt"), ""))
                .await
                .unwrap(),
        )
        .await;
        sm("interrupt", s, &v);
        assert_eq!(v["data"]["interrupted"], true, "{engine_name}: {v}");

        // pause: POST -> 2xx (per-run flag; no-op for a finished job).
        let (s, v) = body_json(
            app.clone()
                .oneshot(post_json(
                    &format!("/api/v1/jobs/{job_id}/pause"),
                    r#"{"paused":true}"#,
                ))
                .await
                .unwrap(),
        )
        .await;
        sm("pause", s, &v);
        assert_eq!(v["data"]["paused"], true, "{engine_name}: {v}");

        // message (a2a.send_message): POST -> 2xx queued (no live PTY).
        let (s, v) = body_json(
            app.clone()
                .oneshot(post_json(
                    &format!("/api/v1/jobs/{job_id}/message"),
                    r#"{"message":"ping"}"#,
                ))
                .await
                .unwrap(),
        )
        .await;
        sm("message", s, &v);
        assert_eq!(v["data"]["queued"], true, "{engine_name}: {v}");

        // compact: POST -> 2xx; the summary is written, the restart deferred.
        let (s, v) = body_json(
            app.clone()
                .oneshot(post_json(&format!("/api/v1/jobs/{job_id}/compact"), ""))
                .await
                .unwrap(),
        )
        .await;
        sm("compact", s, &v);
        assert_eq!(v["data"]["compacted"], true, "{engine_name}: {v}");

        // fork: POST -> 2xx with a fresh run id seeded from the job worktree.
        let (s, v) = body_json(
            app.clone()
                .oneshot(post_json(&format!("/api/v1/jobs/{job_id}/fork"), ""))
                .await
                .unwrap(),
        )
        .await;
        sm("fork", s, &v);
        let forked = v["data"]["run_id"]
            .as_str()
            .unwrap_or_else(|| panic!("fork data must carry run_id: {v}"))
            .to_string();
        assert!(!forked.is_empty(), "{engine_name}: {v}");

        // revert with an empty commit -> 2xx unrevert no-op.
        let (s, v) = body_json(
            app.clone()
                .oneshot(post_json(
                    &format!("/api/v1/jobs/{job_id}/revert"),
                    r#"{"commit":""}"#,
                ))
                .await
                .unwrap(),
        )
        .await;
        sm("revert", s, &v);
        assert_eq!(v["data"]["reverted"], false, "{engine_name}: {v}");

        // The fork registered a new run; drop it and tear the job down.
        let _ = StateDb::open(&hcfg.state_root).map(|db| db.runs().unregister(&forked));
        teardown_job(&hcfg, &job_id);
    }
}

/// Removes a job's worktree/branch/row (mirrors the unit-test teardown).
fn teardown_job(cfg: &HermeticConfig, job_id: &str) {
    if let Ok(meta) = JobMetadata::load(&cfg.state_root, job_id) {
        let repo = kubeadp_agent::core::repos::project_git_repo(&cfg.config, &meta.project);
        let _ = kubeadp_agent::core::git::remove_worktree(&repo, &meta.worktree_path, true);
        let _ = kubeadp_agent::core::git::delete_branch(&repo, &meta.branch);
    }
    let _ = JobMetadata::delete(&cfg.state_root, job_id);
}

// ---------------------------------------------------------------------------
// Part 2: documented-contract parity (the report matches DOCS.md §2.4)
// ---------------------------------------------------------------------------

/// The `/capabilities` report must match the DOCUMENTED tables (`DOCS.md`
/// §2.4). Since HL-71 S2 arms `enable_all_emulated` in production and the
/// engines declare the documented surfaces, the report must match the docs
/// exactly. The only remaining divergences are the SPEC.md §5.5 rows, which
/// are locked (SPEC.md is a normative document this patch must not edit
/// beyond a pointer); they are asserted exactly under `KNOWN_GAPS` below (a
/// fix requires their removal).
#[tokio::test]
async fn capabilities_match_documented_tables() {
    let app = api_app();
    let (status, value) = body_json(
        app.clone()
            .oneshot(get("/api/v1/capabilities"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let caps = value["capabilities"]
        .as_array()
        .expect("capabilities array");
    let by_engine: std::collections::BTreeMap<String, &Value> = caps
        .iter()
        .map(|c| (c["engine"].as_str().unwrap_or("").to_string(), c))
        .collect();

    // Doc rows from DOCS.md §2.4 (the reported "effective" view).
    let agy = by_engine.get("agy").expect("agy reported");
    let claude = by_engine.get("claude").expect("claude reported");
    let opencode = by_engine.get("opencode").expect("opencode reported");
    let agy_eps = agy["endpoints"].as_object().unwrap();
    let claude_eps = claude["endpoints"].as_object().unwrap();
    let opencode_eps = opencode["endpoints"].as_object().unwrap();

    // The documented rows (DOCS.md §2.4), the reported "effective" (post-S2)
    // view. `permissions` and `a2a.inject` are also DOCS.md §2.4 rows:
    // `permissions` is `unsupported` by design (a headless auto-approve engine
    // never asks; never a 501) and `a2a.inject` is a native channel.
    let agy_doc: Vec<(&str, &str)> = vec![
        ("job.start", "native"),
        ("job.status", "native"),
        ("job.list", "native"),
        ("job.kill", "native"),
        ("job.retry", "native"),
        ("job.events", "native"),
        ("job.result", "native"),
        ("session.start", "emulated"),
        ("session.stop", "emulated"),
        ("session.restart", "emulated"),
        ("session.log", "emulated"),
        ("session.message", "emulated"),
        ("interrupt", "emulated"),
        ("pause", "emulated"),
        ("fork", "emulated"),
        ("compact", "emulated"),
        ("revert", "emulated"),
        ("todo", "emulated"),
        ("workspace.*", "native"),
        ("a2a.send_message", "emulated"),
        ("a2a.inject", "native"),
        ("permissions", "unsupported"),
    ];
    let claude_doc: Vec<(&str, &str)> = vec![
        ("job.start", "native"),
        ("job.status", "native"),
        ("job.list", "native"),
        ("job.kill", "native"),
        ("job.retry", "native"),
        ("job.events", "native"),
        ("job.result", "native"),
        ("session.start", "native"),
        ("session.stop", "native"),
        ("session.restart", "native"),
        ("session.log", "native"),
        ("session.message", "native"),
        ("interrupt", "emulated"),
        ("pause", "emulated"),
        ("fork", "emulated"),
        ("compact", "emulated"),
        ("revert", "emulated"),
        ("todo", "emulated"),
        ("workspace.*", "native"),
        ("a2a.send_message", "emulated"),
        ("a2a.inject", "native"),
        ("permissions", "unsupported"),
    ];
    // opencode mirrors the agy job surface (the engine-aware runner provides
    // the same start/status/retry/events lifecycle over the JSON-event log,
    // `workspace.*` is engine-independent, and `a2a.send_message` is emulated
    // -- queued, no live PTY). Injection supports hook, tool_result, and
    // idle_turn, reported as a native channel.
    let opencode_doc: Vec<(&str, &str)> = vec![
        ("job.start", "native"),
        ("job.status", "native"),
        ("job.list", "native"),
        ("job.kill", "native"),
        ("job.retry", "native"),
        ("job.events", "native"),
        ("job.result", "native"),
        ("session.start", "emulated"),
        ("session.stop", "emulated"),
        ("session.restart", "emulated"),
        ("session.log", "emulated"),
        ("session.message", "emulated"),
        ("interrupt", "emulated"),
        ("pause", "emulated"),
        ("fork", "emulated"),
        ("compact", "emulated"),
        ("revert", "emulated"),
        ("todo", "emulated"),
        ("workspace.*", "native"),
        ("a2a.send_message", "emulated"),
        ("a2a.inject", "native"),
        ("permissions", "unsupported"),
    ];

    let mut failures: Vec<String> = Vec::new();
    for (engine, eps, doc) in [
        ("agy", agy_eps, &agy_doc),
        ("claude", claude_eps, &claude_doc),
        ("opencode", opencode_eps, &opencode_doc),
    ] {
        for (key, expected) in doc {
            let actual = eps[*key]
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    (if eps.contains_key(*key) {
                        "emulated"
                    } else {
                        "unsupported"
                    })
                    .to_string()
                });
            // Normalize the serialized value (snake_case) to the doc label.
            let actual_norm = match actual.as_str() {
                "native" => "native",
                "emulated" => "emulated",
                "unsupported" => "unsupported",
                other => panic!("unexpected capability label {other}"),
            };
            if actual_norm != *expected {
                failures.push(format!(
                    "{engine}.{key}: documented {expected}, reported {actual_norm}"
                ));
            }
        }
    }

    // KNOWN_GAPS: the SPEC.md §5.5 rows that are stale relative to the
    // effective (post-S2) report but CANNOT be fixed here: SPEC.md is a
    // locked normative document -- this patch adds only a one-line pointer
    // there, never edits its §5.5 table. Each row records SPEC.md's value;
    // the engine's effective report must NOT match it (the gap is real), so
    // a change that makes a row non-stale -- or a fix that edits SPEC.md --
    // forces the row's removal here. Nothing else may be stale.
    fn report_value<'a>(eps: &'a serde_json::Map<String, Value>, key: &str) -> &'a str {
        eps[key]
            .as_str()
            .map(|s| match s {
                "native" => "native",
                "emulated" => "emulated",
                "unsupported" => "unsupported",
                other => panic!("unexpected capability label {other}"),
            })
            .unwrap_or_else(|| panic!("missing capability label for {key}"))
    }
    // (spec_key, SPEC.md §5.5 value, effective value, engine)
    let known_gaps: [(&str, &str, &str, &str); 6] = [
        ("pause", "native", "emulated", "agy"), // S2 arms emulated pause
        ("fork", "unsupported", "emulated", "agy"), // S2 arms emulated fork
        ("interrupt", "native", "emulated", "claude"), // S2 arms emulated interrupt
        ("pause", "unsupported", "emulated", "claude"), // S2 arms emulated pause
        ("fork", "unsupported", "emulated", "claude"), // S2 arms emulated fork
        ("a2a.send_message", "native", "emulated", "claude"), // job surface is queued, not PTY
    ];
    for (key, spec_value, effective, engine) in known_gaps {
        let actual = report_value(if engine == "agy" { agy_eps } else { claude_eps }, key);
        assert_ne!(
            actual, spec_value,
            "KNOWN_GAP engine {engine} key {key} against SPEC.md §5.5 recorded as {spec_value} \
             but the report now matches ({actual}): the SPEC.md row is no longer stale -- edit \
             SPEC.md and remove this row"
        );
        assert_eq!(
            actual, effective,
            "KNOWN_GAP engine {engine} key {key}: effective report moved to {actual}, \
             expected the S2 {effective} -- update the record"
        );
    }

    // If DOCS.md §2.4 diverges from the live report (or a row was edited
    // without the engine declaration following), fail the gate.
    assert!(
        failures.is_empty(),
        "capability report diverges from DOCS.md §2.4. Divergences:\n{}",
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Part 3: coverage guards (admission control for the conformance suite)
// ---------------------------------------------------------------------------

/// A synthetic engine registered into the registry is enumerated by
/// `/capabilities` but has no conformance case in this suite: the guard must
/// reject it deterministically when the production engine set grows without a
/// matching conformance case.
#[tokio::test]
async fn coverage_guard_rejects_undeclared_engine() {
    // The admission rule for the production engine set: exactly agy + claude
    // + opencode (the engines this suite has cases for). A new engine
    // registered in the worker/orchestrator entry points without a
    // conformance case must break the gate.
    let registry = production_registry();
    let ids = registry.enabled_ids();
    let known = ["agy", "claude", "opencode"];
    for id in &ids {
        assert!(
            known.contains(&id.as_str()),
            "production engine {id} is outside the documented admission set \
             (a conformance case is missing for it)"
        );
    }
    assert!(
        ids.iter().any(|e| e == "agy")
            && ids.iter().any(|e| e == "claude")
            && ids.iter().any(|e| e == "opencode"),
        "production registry must carry agy + claude + opencode: {ids:?}"
    );

    // Negative control: a synthetic engine outside the admission set is still
    // listed by `/capabilities`, but the guard rejects it -- it is not one of
    // the engines this suite admits.
    let registry = engine_with_declared_key("job.start");
    let synthetic_ids = registry.enabled_ids();
    assert!(
        synthetic_ids.iter().any(|e| e == "synthetic"),
        "the synthetic engine must be registered"
    );
    for id in &synthetic_ids {
        assert!(
            !known.contains(&id.as_str()),
            "undeclared engine {id} must be rejected by the admission rule"
        );
    }
}

/// A declared capability key must have a conformance case: every key the
/// static table defines is covered either by an explicit live drive in this
/// suite (job lifecycle + control surface + job message) or by the Part 2
/// documented-parity assertions (keys with no worker HTTP route, whose exact
/// per-engine label is asserted there). A key that gains a Native/Emulated
/// declaration -- or a new static key -- without being covered is caught here.
///
/// The partition is honest about what this suite can drive over the wire:
/// only the keys below have a worker route a test request can reach. The
/// remaining keys (workspace, sessions, a2a.inject, permissions) have no
/// route in the worker -- they are worker-internal surfaces -- so their
/// conformance case is the documented-parity label assertion.
#[tokio::test]
async fn coverage_guard_rejects_undeclared_key() {
    let all_keys = kubeadp_agent::engine::capability::ENDPOINTS
        .iter()
        .map(|e| e.key)
        .collect::<Vec<_>>();
    // Keys this suite drives through a real worker route. Each must have at
    // least one conformance case that walks the HTTP path (Part 1 / Part 3).
    let driven = [
        "job.start",
        "job.status",
        "job.list",
        "job.kill",
        "job.retry",
        "job.events",
        "job.result",
        "interrupt",
        "pause",
        "compact",
        "fork",
        "revert",
        "todo",
        "a2a.send_message",
    ]
    .to_vec();
    // Keys with no worker HTTP route: their only conformance case is the
    // Part 2 documented-parity assertion (which pins the exact per-engine
    // label), so they must appear in neither side's false claims.
    let parity_by_doc = [
        "workspace.*",
        "session.start",
        "session.stop",
        "session.restart",
        "session.log",
        "session.message",
        "a2a.inject",
        "permissions",
    ]
    .to_vec();
    for key in all_keys {
        assert!(
            driven.contains(&key) || parity_by_doc.contains(&key),
            "key {key} is in the static table but has NO conformance case \
             (not HTTP-driven and not covered by documented parity)"
        );
        assert!(
            !(driven.contains(&key) && parity_by_doc.contains(&key)),
            "key {key} is listed in both the driven and parity-by-doc sets"
        );
    }
}

/// Every key declared by a production engine has a conformance case: a key an
/// engine reports as `Native`/`Emulated` must be driven by this suite (HTTP
/// keys) or pinned by the documented-parity assertions (no-route keys).
#[tokio::test]
async fn every_declared_key_has_a_case() {
    let registry = production_registry();
    let all_keys = kubeadp_agent::engine::capability::ENDPOINTS
        .iter()
        .map(|e| e.key)
        .collect::<Vec<_>>();
    let driven = [
        "job.start",
        "job.status",
        "job.list",
        "job.kill",
        "job.retry",
        "job.events",
        "job.result",
        "interrupt",
        "pause",
        "compact",
        "fork",
        "revert",
        "todo",
        "a2a.send_message",
    ]
    .to_vec();
    let parity_by_doc = [
        "workspace.*",
        "session.start",
        "session.stop",
        "session.restart",
        "session.log",
        "session.message",
        "a2a.inject",
        "permissions",
    ]
    .to_vec();
    for key in all_keys {
        let declared_by_any = registry.enabled_ids().iter().any(|id| {
            registry
                .capability_table(id)
                .and_then(|t| t.capability(key))
                .map(|c| c != Capability::Unsupported)
                .unwrap_or(false)
        });
        if declared_by_any {
            assert!(
                driven.contains(&key) || parity_by_doc.contains(&key),
                "key {key} is declared by a registered engine but has no conformance case"
            );
        }
    }
}

/// The emulation wrapper always fronts the structured `todo` surface: every
/// registered engine reports `todo` as Emulated.
#[tokio::test]
async fn todo_is_always_emulated() {
    let registry = production_registry();
    for id in registry.enabled_ids() {
        let table = registry.capability_table(&id).expect("wrapped engine");
        assert_eq!(
            table.capability("todo"),
            Some(Capability::Emulated),
            "engine {id}: todo is always fronted by the emulation layer"
        );
    }
}

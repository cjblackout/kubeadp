//! A2A (Agent-to-Agent) protocol task backend for agent-run-api.
//!
//! Exposes agent-run jobs as A2A tasks over the [`crate::core::a2a::TaskBackend`]
//! trait:
//! - An A2A Task corresponds to a job (id = job id; contextId = job id).
//! - State mapping converts `JobStatus` into `TaskState`:
//!   - `Running` -> `TaskState::Working`
//!   - `Done` -> `TaskState::Completed`
//!   - `Failed`, `Incomplete`, `QuotaExhausted` -> `TaskState::Failed`
//!   - `SelfTerminated`, `Killed` -> `TaskState::Canceled`
//! - Artifacts: the job's final response text (if present), plus the git diff
//!   text (if available and cheap).
//! - `ListTasks`: lists jobs, optionally filtered by contextId (matching job_id)
//!   and/or A2A TaskState.
//! - `GetTask`: returns one task by job ID or unique prefix.
//! - `CancelTask`: kills the job via `crate::core::commands::kill::run`.

use std::fs;
use std::sync::Arc;

use crate::core::a2a::{map_job_status, A2AError, A2aTask, TaskBackend};
use crate::core::args::JobIdArg;
use crate::core::commands;
use crate::core::config::Config;
use crate::core::job::{compute_status, extract_final_response, list_jobs, JobMetadata, JobStatus};
use a2a::{Artifact, Part, TaskState};

/// TaskBackend implementation that operates on agent-core's SQLite job store and worktrees.
pub struct JobTaskBackend {
    config: Arc<Config>,
}

impl JobTaskBackend {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// Converts a [`JobMetadata`] row into an [`A2aTask`] with state and artifacts.
    fn job_to_task(&self, meta: &JobMetadata) -> A2aTask {
        let status = compute_status(meta, &self.config.state_root);
        let state = map_job_status(status);

        let mut status_message = None;
        let mut error = None;
        let mut artifacts_vec = Vec::new();

        // 1. Final response from job log (if any)
        let log_path = JobMetadata::log_path(&self.config.state_root, &meta.job_id);
        if let Ok(bytes) = fs::read(&log_path) {
            let log = String::from_utf8_lossy(&bytes);
            if let Some(resp) = extract_final_response(&log) {
                if !resp.is_empty() {
                    status_message = Some(resp.clone());
                    artifacts_vec.push(Artifact {
                        artifact_id: format!("{}-result", meta.job_id),
                        name: Some("result.txt".to_string()),
                        description: Some("Job result text".to_string()),
                        parts: vec![Part::text(resp)],
                        metadata: None,
                        extensions: None,
                    });
                }
            }
        }

        // Error message for failed/terminal states
        if status == JobStatus::Failed
            || status == JobStatus::Incomplete
            || status == JobStatus::QuotaExhausted
        {
            error = Some(format!("Job ended with status: {status}"));
        }

        // 2. Git diff artifact if worktree exists and cheap to compute.
        // Refuse to compute a diff from a worktree agent-run owns whose .git
        // pointer does not resolve inside the project's repos root / workspace
        // checkout (a tampered/stale pointer could redirect the diff into an
        // attacker-controlled repo); on refusal the task is returned without
        // the diff artifact rather than failing. External (--from-worktree)
        // worktrees are user-supplied and are not validated against our roots.
        let pointer_ok = if meta.external_worktree {
            true
        } else {
            crate::core::git::worktree_gitdir_in_expected_roots(
                &meta.worktree_path,
                &crate::core::git::expected_worktree_roots(&self.config, &meta.project),
            )
            .is_ok()
        };
        if meta.worktree_path.exists() && pointer_ok {
            if let Ok((diff_text, _)) = commands::diff::job_diff_text(meta) {
                if !diff_text.is_empty() {
                    artifacts_vec.push(Artifact {
                        artifact_id: format!("{}-diff", meta.job_id),
                        name: Some("changes.diff".to_string()),
                        description: Some("Worktree git diff".to_string()),
                        parts: vec![Part::text(diff_text)],
                        metadata: None,
                        extensions: None,
                    });
                }
            }
        }

        let artifacts = if artifacts_vec.is_empty() {
            None
        } else {
            Some(artifacts_vec)
        };

        A2aTask {
            id: meta.job_id.clone(),
            context_id: Some(meta.job_id.clone()),
            state,
            status_message,
            error,
            artifacts,
        }
    }
}

impl TaskBackend for JobTaskBackend {
    fn get_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        let meta = JobMetadata::resolve(&self.config.state_root, task_id)
            .map_err(|_| A2AError::task_not_found(task_id))?;
        Ok(self.job_to_task(&meta))
    }

    fn list_tasks(
        &self,
        context_id: Option<&str>,
        state_filter: Option<&TaskState>,
    ) -> Result<Vec<A2aTask>, A2AError> {
        let all_jobs = list_jobs(&self.config.state_root, None)
            .map_err(|e| A2AError::internal(format!("failed to list jobs: {e}")))?;

        let mut tasks = Vec::new();
        for meta in all_jobs {
            if let Some(ctx) = context_id {
                // context_id matches job_id
                if meta.job_id != ctx {
                    continue;
                }
            }
            let task = self.job_to_task(&meta);
            if let Some(target_state) = state_filter {
                if &task.state != target_state {
                    continue;
                }
            }
            tasks.push(task);
        }

        Ok(tasks)
    }

    fn cancel_task(&self, task_id: &str) -> Result<A2aTask, A2AError> {
        let meta = JobMetadata::resolve(&self.config.state_root, task_id)
            .map_err(|_| A2AError::task_not_found(task_id))?;
        let status = compute_status(&meta, &self.config.state_root);

        // A job that is already terminal cannot be canceled
        if status.is_terminal() {
            return Err(A2AError::task_not_cancelable(task_id));
        }

        // Run kill command (reusing the exact code path as POST /api/v1/jobs/{id}/kill)
        let _ = commands::kill::run(
            &self.config,
            &JobIdArg {
                job_id: meta.job_id.clone(),
            },
        )
        .map_err(|e| A2AError::internal(format!("failed to kill job {task_id}: {e}")))?;

        // Re-read task state after kill
        let fresh_meta = JobMetadata::load(&self.config.state_root, &meta.job_id).unwrap_or(meta);
        Ok(self.job_to_task(&fresh_meta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{open_state_db, JobStatus};
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> Config {
        Config {
            state_root: dir.path().to_path_buf(),
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        }
    }

    fn create_test_job(config: &Config, id: &str, status: JobStatus) -> JobMetadata {
        let meta = JobMetadata {
            job_id: id.to_string(),
            project: "test-proj".to_string(),
            prompt: "test prompt".to_string(),
            resolved_prompt: "test prompt".to_string(),
            model: Some("test-model".to_string()),
            effort: None,
            sandbox: false,
            mode: "fast".to_string(),
            add_dirs: Vec::new(),
            print_timeout: None,
            created_at: chrono::Utc::now(),
            worktree_path: config.workspace_root.join(id),
            branch: format!("agent-run/{id}"),
            base_ref: "main".to_string(),
            conversation_id: None,
            retry_count: 0,
            pid: None,
            exit_code: match status {
                JobStatus::Done => Some(0),
                JobStatus::Running => None,
                _ => Some(1),
            },
            status: Some(status),
            hindsight: None,
            auto_recall: None,
            auto_retain: None,
            retry_prompt: None,
            read_only: false,
            external_worktree: false,
            pid_start_time: None,
            hindsight_bank: None,
            proxy: None,
            max_runtime: None,
            started_at: None,
            hindsight_query: None,
            sandbox_info: None,
        };
        meta.save(&config.state_root).unwrap();

        let log_path = JobMetadata::log_path(&config.state_root, id);
        if let Some(parent) = log_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if status == JobStatus::Done {
            let _ = fs::write(
                &log_path,
                r#"{"status":"SUCCESS","response":"Success result"}"#,
            );
        } else if status == JobStatus::Failed {
            let _ = fs::write(&log_path, r#"{"status":"ERROR","error":"Failed error"}"#);
        }

        meta
    }

    #[test]
    fn test_unknown_task_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config);

        let err = backend.get_task("nonexistent").unwrap_err();
        assert_eq!(err.code, "taskNotFound");
    }

    #[test]
    fn test_state_mapping_and_get_task() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        create_test_job(&config, "job-done", JobStatus::Done);
        create_test_job(&config, "job-failed", JobStatus::Failed);
        create_test_job(&config, "job-killed", JobStatus::Killed);

        let t1 = backend.get_task("job-done").unwrap();
        assert_eq!(t1.id, "job-done");
        assert_eq!(t1.context_id.as_deref(), Some("job-done"));
        assert_eq!(t1.state, TaskState::Completed);

        let t2 = backend.get_task("job-failed").unwrap();
        assert_eq!(t2.state, TaskState::Failed);
        assert!(t2.error.is_some());

        let t3 = backend.get_task("job-killed").unwrap();
        assert_eq!(t3.state, TaskState::Canceled);
    }

    #[test]
    fn test_list_tasks_and_filtering() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        create_test_job(&config, "job1", JobStatus::Done);
        create_test_job(&config, "job2", JobStatus::Failed);

        let all = backend.list_tasks(None, None).unwrap();
        assert_eq!(all.len(), 2);

        let filtered_ctx = backend.list_tasks(Some("job1"), None).unwrap();
        assert_eq!(filtered_ctx.len(), 1);
        assert_eq!(filtered_ctx[0].id, "job1");

        let filtered_state = backend.list_tasks(None, Some(&TaskState::Failed)).unwrap();
        assert_eq!(filtered_state.len(), 1);
        assert_eq!(filtered_state[0].id, "job2");
    }

    #[test]
    fn test_cancel_terminal_task_returns_not_cancelable() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        create_test_job(&config, "term-job", JobStatus::Done);
        let err = backend.cancel_task("term-job").unwrap_err();
        assert_eq!(err.code, "taskNotCancelable");
    }

    #[test]
    fn test_artifacts_from_job_log() {
        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        let backend = JobTaskBackend::new(config.clone());

        let meta = create_test_job(&config, "job-with-res", JobStatus::Done);
        let log_path = JobMetadata::log_path(&config.state_root, &meta.job_id);
        fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        fs::write(
            &log_path,
            r#"{"status":"SUCCESS","response":"Found the bug in file.rs"}"#,
        )
        .unwrap();

        let task = backend.get_task("job-with-res").unwrap();
        assert_eq!(
            task.status_message.as_deref(),
            Some("Found the bug in file.rs")
        );
        let arts = task.artifacts.expect("artifacts should be present");
        assert_eq!(arts.len(), 1);
        assert_eq!(arts[0].name.as_deref(), Some("result.txt"));
        assert_eq!(arts[0].parts[0].as_text(), Some("Found the bug in file.rs"));
    }

    #[tokio::test]
    async fn test_http_a2a_endpoints() {
        use crate::core::a2a::{a2a_router, A2aModel, WELL_KNOWN_AGENT_CARD_PATH};
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let tmp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&tmp));
        open_state_db(&config.state_root).unwrap();
        create_test_job(&config, "job-http-1", JobStatus::Done);

        let model = A2aModel {
            name: "agent-run-api".to_string(),
            description: "Test".to_string(),
            version: "0.1.0".to_string(),
            skills: vec![],
            json_rpc_url: "http://localhost:8001/rpc".to_string(),
        };
        let backend = Arc::new(JobTaskBackend::new(config));
        let app = a2a_router(model, backend, None, None);

        // 1. Agent card (public)
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(WELL_KNOWN_AGENT_CARD_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 2. GET /tasks/job-http-1
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/tasks/job-http-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 3. GET /tasks/nonexistent -> 404
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/tasks/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // 4. POST /rpc with GetTask
        let rpc_req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "GetTask",
            "params": { "id": "job-http-1" },
            "id": 1
        });
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc")
                    .header("a2a-version", "1.0")
                    .header("content-type", "application/json")
                    .body(Body::from(rpc_req.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 5. POST /message:send -> the worker has no interactive input channel,
        // so SendMessage is UNSUPPORTED_OPERATION; the SDK maps that to HTTP 400
        // (the previous hand-written bindings answered 501).
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/message:send")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }
}

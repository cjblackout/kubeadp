//! `core`: the job-execution engine (all of the former `agent-core` crate).
//!
//! This is the ONLY execution engine in the system. It owns:
//! - the job state store (`state.sqlite`) and log files,
//! - git worktree lifecycle (`<workspace>/<jobs_dir_name>/<project>/<job-id>`,
//!   outside the project checkout),
//! - the job monitor (`runner::run_internal_runner`), which runs on an
//!   in-process OS thread spawned by [`runner::launch_background_job`],
//! - hindsight recall/retain integration and agy conversation cleanup,
//! - the uniform [`output::CommandOutput`] contract that both the
//!   `agent-run` CLI shim (which renders it) and the `agent-run-api`
//!   server (which returns it as JSON) build on.
//!
//! The axum-based HTTP API server (`core::server`) that the `orchestrate` and
//! `worker` roles re-export lives in this module behind the `server` cargo
//! feature, so the `cli` module (which does not serve HTTP) stays free of the
//! HTTP dependency tree.

pub mod a2a_live;
pub mod activity;
pub mod agy_cleanup;
pub mod agy_conversations;
pub mod args;
pub mod auth;
pub mod commands;
pub mod config;
pub mod crypto;
pub mod error;
pub mod git;
pub mod hindsight;
pub mod job;
pub mod jobset;
pub mod max_runtime;
pub mod migrations;
pub mod output;
pub mod proxy;
pub mod reaper;
pub mod repos;
pub mod reposync;
pub mod runner;
pub mod spawn;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "a2a")]
pub mod a2a;

/// Shared process-wide mutex for unit tests that mutate or depend on
/// process-global environment variables (`std::env::set_var` / `remove_var`).
#[cfg(test)]
pub fn lock_test_env() -> std::sync::MutexGuard<'static, ()> {
    static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

//! kubeadp-agent: the unified agent package (consolidation of agent-core,
//! agent-orchestrate, agent-run-api and agent-run-cli).
//!
//! Module map:
//! - [`core`] -- the job-execution engine (state store, git worktrees, job
//!   lifecycle, hindsight, and -- behind the `server` feature -- the axum
//!   HTTP API server). All of the former `agent-core` crate.
//! - [`orchestrate`] -- the process orchestrator for the ai-server pod's
//!   agent sessions, with an agent-run-compatible REST API. All of the former
//!   `agent-orchestrate` crate (lib modules).
//! - [`worker`] -- the HTTP API server that executes agent-run jobs remotely
//!   (the same core as the agent-run CLI). Former `agent-run-api` modules
//!   except its `main`.
//! - [`cli`] -- the remote CLI client modules for the `agent-run` binary.
//!   Former `agent-run-cli` modules except its `main`.
//!
//! Feature-gating (mirrors the old crates):
//! - `server` gates `core::server` (and the modules of `orchestrate` and
//!   `worker` that need the axum stack).
//! - `cli` gates this crate's `cli` module (the ureq-based client).
//! - `a2a` (implies `server`) gates the A2A module of `core` and the per-role
//!   `a2a` integration modules.
//! - `test-hooks` is used by the orchestrator session loop tests.
//! - `engine` (implies `server`) gates the pluggable engine layer:
//!   the [`engine`] module tree (trait, run model, capability table,
//!   registry, 501 plumbing).

#[cfg(feature = "cli")]
pub mod cli;
pub mod config_file;
pub mod core;
#[cfg(feature = "engine")]
pub mod engine;
#[cfg(feature = "a2a")]
pub mod mcp_a2a;
#[cfg(feature = "server")]
pub mod orchestrate;
pub mod sandbox;
#[cfg(feature = "server")]
pub mod worker;

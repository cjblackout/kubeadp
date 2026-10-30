//! `orchestrate`: the process orchestrator for the ai-server pod's agent
//! sessions (all of the former `agent-orchestrate` crate lib modules).

#[cfg(feature = "a2a")]
pub mod a2a;
pub mod backoff;
pub mod claude_config;
pub mod config;
pub mod entry;
pub mod http_server;
pub mod logging;
pub mod memory;
pub mod pty;
pub mod readiness;
pub mod server;
pub mod session;
pub mod session_control;
pub mod session_file;
pub mod session_log;
pub mod sessions_api;
pub mod state;
pub mod stdout_writer;
pub mod workspaces;

//! `cli`: the remote CLI client modules for the `agent-run` binary (former
//! `agent-run-cli` modules, except its `main`).

#[allow(clippy::module_inception)]
pub mod cli;
pub mod client;
pub mod events_cli;

pub use cli::*;

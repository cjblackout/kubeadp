//! `worker`: the HTTP API server that executes agent-run jobs remotely
//! (former `agent-run-api` modules, except its `main`).

pub mod agy_home;
pub mod agy_plugin;
pub mod entry;
pub mod server;
pub mod supervisor;

#[cfg(feature = "a2a")]
pub mod a2a;

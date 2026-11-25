//! Typed error classification for agent-run.
//!
//! The API server maps errors to HTTP statuses by inspecting this type
//! (via `downcast_ref` on the `anyhow` chain) instead of matching on
//! error message text. The `message` field keeps the exact human-readable
//! text the CLI prints; `code` is a stable machine-readable identifier.

use std::error::Error as StdError;
use std::fmt;

/// The class of failure, mapped to an HTTP status by the API server:
/// `NotFound` -> 404, `Conflict` -> 409, `Invalid` -> 400,
/// `Internal` -> 500.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    Conflict,
    Invalid,
    Internal,
    Forbidden,
}

impl ErrorKind {
    /// The HTTP status code this error kind maps to.
    pub fn status_code(self) -> u16 {
        match self {
            ErrorKind::NotFound => 404,
            ErrorKind::Conflict => 409,
            ErrorKind::Invalid => 400,
            ErrorKind::Internal => 500,
            ErrorKind::Forbidden => 403,
        }
    }
}

/// A classified agent-run error. The `message` is the exact text printed
/// to the user (byte-identical to the pre-typed-error text); the `code`
/// is a stable snake_case identifier for programmatic consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunError {
    pub kind: ErrorKind,
    pub code: &'static str,
    pub message: String,
}

impl fmt::Display for AgentRunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for AgentRunError {}

impl AgentRunError {
    fn new(kind: ErrorKind, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
        }
    }

    /// Unknown job / missing log / no in-progress auth flow (HTTP 404).
    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, code, message)
    }

    /// A second auth start while one is already in progress (HTTP 409).
    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, code, message)
    }

    /// Validation refusals (HTTP 400).
    pub fn invalid(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, code, message)
    }

    /// Unexpected internal failure (HTTP 500).
    pub fn internal(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, code, message)
    }

    /// Forbidden operations (HTTP 403).
    pub fn forbidden(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Forbidden, code, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_exactly_the_message() {
        // The CLI prints the message verbatim (`Error: <message>`), so Display
        // must be byte-identical to the stored message -- no code prefix.
        let e = AgentRunError::invalid("prompt_empty", "Prompt cannot be empty");
        assert_eq!(e.to_string(), "Prompt cannot be empty");
        let e = AgentRunError::not_found(
            "job_not_found",
            "No such job found: 'abc' (no job record in /x)",
        );
        assert_eq!(
            e.to_string(),
            "No such job found: 'abc' (no job record in /x)"
        );
    }

    #[test]
    fn kind_maps_to_status_code() {
        assert_eq!(ErrorKind::NotFound.status_code(), 404);
        assert_eq!(ErrorKind::Conflict.status_code(), 409);
        assert_eq!(ErrorKind::Invalid.status_code(), 400);
        assert_eq!(ErrorKind::Internal.status_code(), 500);
        assert_eq!(ErrorKind::Forbidden.status_code(), 403);
    }

    #[test]
    fn constructors_set_kind_and_code() {
        assert_eq!(AgentRunError::not_found("a", "m").kind, ErrorKind::NotFound);
        assert_eq!(AgentRunError::conflict("a", "m").kind, ErrorKind::Conflict);
        assert_eq!(AgentRunError::invalid("a", "m").kind, ErrorKind::Invalid);
        assert_eq!(AgentRunError::internal("a", "m").kind, ErrorKind::Internal);
        assert_eq!(
            AgentRunError::forbidden("a", "m").kind,
            ErrorKind::Forbidden
        );
        assert_eq!(AgentRunError::not_found("a", "m").code, "a");
        assert_eq!(AgentRunError::forbidden("a", "m").code, "a");
    }

    #[test]
    fn downcast_ref_finds_typed_error_in_chain() {
        // The server classifies by walking the anyhow chain; a typed error
        // wrapped (even under .with_context) must still be discoverable.
        let err: anyhow::Error =
            AgentRunError::invalid("job_running", "Job 'x' is currently running.").into();
        assert_eq!(
            err.chain()
                .find_map(|e| e.downcast_ref::<AgentRunError>())
                .map(|t| t.code),
            Some("job_running")
        );
        // An untyped error has no AgentRunError anywhere in the chain.
        let other: anyhow::Error = anyhow::anyhow!("database is locked");
        assert_eq!(
            other
                .chain()
                .find_map(|e| e.downcast_ref::<AgentRunError>()),
            None
        );
    }

    #[test]
    fn debug_form_is_unwrapped_message() {
        // format!("{err:?}") on an anyhow::Error from AgentRunError must NOT
        // append a "Caused by:" trailer: AgentRunError has no source(), so a
        // single-message error stays byte-identical to the bare message.
        let e: anyhow::Error = AgentRunError::invalid("x", "Prompt cannot be empty").into();
        assert_eq!(format!("{e:?}"), "Prompt cannot be empty");
    }
}

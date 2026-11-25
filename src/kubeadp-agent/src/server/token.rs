//! Operator-token handling for the agent-run API surface.
//!
//! The [`crate::server::auth`] layer compares bearer credentials in
//! constant time. Instead of comparing raw token strings (whose lengths
//! differ legitimately and would give away the token length in the timing
//! of a length short-circuit), both the configured token and the presented
//! credential are reduced to their SHA-256 digest and the 32-byte digests
//! are compared byte by byte. A wrong-length presented credential hashes
//! to a different digest, so the comparison never reveals anything about
//! the secret -- and the raw token value lives only for the instant it is
//! hashed at startup.
//!
//! SHA-256 and the constant-time digest comparison come from the shared
//! [`crate::core::crypto`] module (backed by `ring`), which is a direct,
//! non-optional dependency and therefore reachable from every feature set
//! (including `--no-default-features --features server`). The vectors that
//! pin the primitive live in that module's test suite.
//!
//! The token is read at startup from the file named by `KUBEADP_API_TOKEN_FILE`
//! (config key `api_token_file`, shared by both roles). If no token is
//! configured the server refuses to start in production builds unless
//! `KUBEADP_API_ALLOW_UNAUTHENTICATED=true` (config key
//! `api_allow_unauthenticated`) is set -- see [`startup_permitted`]. The
//! token value itself is never logged or stored outside the digest.

use std::io;
use std::path::{Path, PathBuf};

use crate::core::crypto;

/// Environment variable naming the operator token file (shared by both roles;
/// config key `api_token_file` maps to it).
pub const API_TOKEN_FILE_ENV: &str = "KUBEADP_API_TOKEN_FILE";
/// Environment variable allowing a startup without any token (config key
/// `api_allow_unauthenticated` maps to it).
pub const ALLOW_UNAUTHENTICATED_ENV: &str = "KUBEADP_API_ALLOW_UNAUTHENTICATED";
/// Environment variable explicitly allowing the open A2A surface (no config
/// key: the A2A routes stay open without an operator token only when this is
/// set true; otherwise the server refuses to mount them, see
/// [`a2a_open_permitted`]).
pub const ALLOW_OPEN_ENV: &str = "KUBEADP_API_ALLOW_OPEN";

/// Length of a SHA-256 digest in bytes.
pub const DIGEST_LEN: usize = 32;

/// Computes the SHA-256 digest (FIPS 180-4) of `data`.
///
/// Thin re-export of the shared [`crate::core::crypto::sha256`] so the auth
/// layer's existing `token::sha256` call site is unchanged.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    crypto::sha256(data)
}

/// Constant-time comparison of a presented credential against the stored
/// 32-byte SHA-256 digest. The presented value is hashed first, so both
/// sides are always `DIGEST_LEN` bytes and the comparison runs in constant
/// time independent of input length.
///
/// Thin re-export of the shared [`crate::core::crypto`] helpers over the
/// fixed-size digests.
pub fn constant_time_digest_eq(presented: &[u8], expected_digest: &[u8; DIGEST_LEN]) -> bool {
    crypto::ct_eq(&crypto::sha256(presented), expected_digest)
}

/// Reads the operator token from `path`, trimming surrounding whitespace.
/// A missing or unreadable file is an error: the caller must not silently
/// fall back to "no token" for a configured but unreadable path, because
/// that would start an unauthenticated server while the operator believes
/// one is protected.
pub fn read_token_file(path: &Path) -> io::Result<String> {
    let bytes = std::fs::read(path)?;
    let s = String::from_utf8_lossy(&bytes);
    Ok(s.trim().to_string())
}

/// Resolves the operator token from the environment, shared by both roles.
///
/// The token comes from the file named by `KUBEADP_API_TOKEN_FILE` (config
/// key `api_token_file`; the inline `KUBEADP_API_TOKEN` variable has no
/// config key). The value is trimmed; an empty or absent file means "no
/// token configured".
///
/// Returns `Err` when the file is configured but cannot be read (that is a
/// startup-fatal misconfiguration: silently falling back to an
/// unauthenticated server would be worse).
pub fn resolve_operator_token() -> Result<Option<String>, String> {
    let Some(path) = std::env::var_os(API_TOKEN_FILE_ENV) else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    let token = read_token_file(&path).map_err(|e| {
        format!(
            "cannot read operator api token from {API_TOKEN_FILE_ENV} '{}': {e}",
            path.display()
        )
    })?;
    if token.is_empty() {
        Ok(None)
    } else {
        Ok(Some(token))
    }
}

/// Whether the current process is allowed to serve without a token:
/// `api_allow_unauthenticated` (config `api_allow_unauthenticated`,
/// env `KUBEADP_API_ALLOW_UNAUTHENTICATED`) set true. The value is parsed
/// leniently (any of `1/true/yes/on` case-insensitively).
pub fn allow_unauthenticated_from_env() -> bool {
    std::env::var(ALLOW_UNAUTHENTICATED_ENV)
        .ok()
        .map(|v| parse_bool(&v))
        .unwrap_or(false)
}

/// Whether the A2A routes may stay open without an operator token:
/// `KUBEADP_API_ALLOW_OPEN` set true (parsed leniently like the other flag).
pub fn allow_open_from_env() -> bool {
    std::env::var(ALLOW_OPEN_ENV)
        .ok()
        .map(|v| parse_bool(&v))
        .unwrap_or(false)
}

fn parse_bool(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether unauthenticated operation is permitted for THIS startup: the env
/// flag `KUBEADP_API_ALLOW_UNAUTHENTICATED` if set, else the test-hooks
/// default (test-hooks builds are for the supervisor black-box harness and
/// start servers without a token; the flag still overrides either way).
fn unauthenticated_permitted() -> bool {
    #[cfg(feature = "test-hooks")]
    {
        match std::env::var(ALLOW_UNAUTHENTICATED_ENV) {
            Ok(v) => parse_bool(&v),
            Err(_) => true,
        }
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        allow_unauthenticated_from_env()
    }
}

/// `true` when the server may start without a configured operator token
/// (i.e. when `api_allow_unauthenticated` is set). This is a pure decision
/// predicate so it is table-testable; the caller prints a loud warning and
/// refuses to serve when it returns `false`.
pub fn startup_permitted(token_configured: bool, allow_unauthenticated: bool) -> bool {
    token_configured || allow_unauthenticated
}

/// `true` when the A2A routes may be mounted with the given operator-token
/// state: a token configured (the A2A routes sit behind the same bearer as
/// `/api/v1`), or `KUBEADP_API_ALLOW_OPEN` set true (the explicit open path).
/// Pure decision predicate so it is table-testable; the mount sites refuse to
/// start with a clear error naming `KUBEADP_API_ALLOW_OPEN` when it returns
/// `false` -- a `None` operator verifier must never mean an open A2A surface
/// by accident.
pub fn a2a_open_permitted(token_configured: bool, allow_open: bool) -> bool {
    token_configured || allow_open
}

/// The result of resolving the operator-token configuration at startup:
/// either the (optional) token to protect the API with, or a fatal startup
/// error the server must refuse to continue past.
#[derive(Debug)]
pub enum StartupAuth {
    /// Serve with this bearer token guarding `/api/v1` and the A2A surface.
    Token(String),
    /// No token configured and the unauthenticated flag is set: the `/api/v1`
    /// surface is open (the operator took the explicit unsafe path); the A2A
    /// routes stay unmounted unless `KUBEADP_API_ALLOW_OPEN` is set too.
    Unauthenticated,
    /// No token configured and the flag is absent: startup refused. The
    /// string is the message to log before refusing.
    Refused(String),
}

/// Resolves the operator-token configuration for a role at startup, applying
/// the "no token requires `api_allow_unauthenticated`" rule.
///
/// The token is read from `KUBEADP_API_TOKEN_FILE` (shared by both roles).
pub fn startup_auth() -> Result<StartupAuth, String> {
    match resolve_operator_token()? {
        Some(token) => Ok(StartupAuth::Token(token)),
        None if unauthenticated_permitted() => Ok(StartupAuth::Unauthenticated),
        None => Ok(StartupAuth::Refused(
            "no operator api token configured (KUBEADP_API_TOKEN_FILE) and \
             KUBEADP_API_ALLOW_UNAUTHENTICATED is not set; refusing to start an \
             unauthenticated API server -- supply the token (api_token_file) or \
             explicitly set api_allow_unauthenticated"
                .to_string(),
        )),
    }
}

/// Hex-encodes a digest (used by `status`-style output and tests; never
/// logs a token, only a digest).
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Acquires the shared crate-wide env lock for the duration of `f`.
    fn with_env_lock<R>(f: impl FnOnce() -> R) -> R {
        let _guard = crate::test_env_lock();
        f()
    }

    #[test]
    fn test_constant_time_digest_eq() {
        let digest = sha256(b"sekret");
        assert!(constant_time_digest_eq(b"sekret", &digest));
        assert!(!constant_time_digest_eq(b"sekrett", &digest));
        assert!(!constant_time_digest_eq(b"wrong", &digest));
        // Different lengths still compare (the presented digest differs).
        assert!(!constant_time_digest_eq(b"", &digest));
        assert!(constant_time_digest_eq(b"", &sha256(b"")));
    }

    #[test]
    fn test_startup_permitted() {
        assert!(startup_permitted(true, false));
        assert!(startup_permitted(true, true));
        assert!(startup_permitted(false, true));
        assert!(!startup_permitted(false, false));
    }

    #[test]
    fn test_a2a_open_permitted() {
        assert!(a2a_open_permitted(true, false));
        assert!(a2a_open_permitted(true, true));
        assert!(a2a_open_permitted(false, true));
        // No token and no explicit open flag: the A2A routes refuse to mount.
        assert!(!a2a_open_permitted(false, false));
    }

    #[test]
    fn test_read_token_file_trims() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "  from-file-token  \n").unwrap();
        assert_eq!(read_token_file(&path).unwrap(), "from-file-token");
        assert!(read_token_file(&dir.path().join("missing")).is_err());
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(b""), "");
        assert_eq!(hex_encode(&[0xde, 0xad]), "dead");
        assert_eq!(hex_encode(&[0x10, 0xff]), "10ff");
    }

    #[test]
    fn test_resolve_operator_token_from_file() {
        with_env_lock(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("api-token");
            std::fs::write(&path, "  op-token-1  \n").unwrap();
            unsafe {
                std::env::set_var(API_TOKEN_FILE_ENV, &path);
            }
            let got = resolve_operator_token();
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
            }
            assert_eq!(got.unwrap().as_deref(), Some("op-token-1"));
        });
    }

    #[test]
    fn test_resolve_operator_token_unset_or_empty() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
            }
            assert_eq!(resolve_operator_token().unwrap(), None);

            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("api-token");
            std::fs::write(&path, "  \n").unwrap();
            unsafe {
                std::env::set_var(API_TOKEN_FILE_ENV, &path);
            }
            let got = resolve_operator_token();
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
            }
            assert_eq!(got.unwrap(), None);
        });
    }

    #[test]
    fn test_resolve_operator_token_unreadable_is_error() {
        with_env_lock(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("missing-token");
            unsafe {
                std::env::set_var(API_TOKEN_FILE_ENV, &path);
            }
            let got = resolve_operator_token();
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
            }
            assert!(
                got.is_err(),
                "a configured-but-unreadable file must be fatal"
            );
        });
    }

    #[test]
    fn test_allow_unauthenticated_parsing() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(ALLOW_UNAUTHENTICATED_ENV);
            }
            assert!(!allow_unauthenticated_from_env());
            for val in ["1", "true", "TRUE", "yes", "on"] {
                unsafe {
                    std::env::set_var(ALLOW_UNAUTHENTICATED_ENV, val);
                }
                assert!(allow_unauthenticated_from_env(), "value {val:?}");
            }
            for val in ["0", "false", "no", "off", "banana"] {
                unsafe {
                    std::env::set_var(ALLOW_UNAUTHENTICATED_ENV, val);
                }
                assert!(!allow_unauthenticated_from_env(), "value {val:?}");
            }
            unsafe {
                std::env::remove_var(ALLOW_UNAUTHENTICATED_ENV);
            }
        });
    }

    #[test]
    fn test_allow_open_parsing() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(ALLOW_OPEN_ENV);
            }
            assert!(!allow_open_from_env());
            for val in ["1", "true", "TRUE", "yes", "on"] {
                unsafe {
                    std::env::set_var(ALLOW_OPEN_ENV, val);
                }
                assert!(allow_open_from_env(), "value {val:?}");
            }
            for val in ["0", "false", "no", "off", "banana"] {
                unsafe {
                    std::env::set_var(ALLOW_OPEN_ENV, val);
                }
                assert!(!allow_open_from_env(), "value {val:?}");
            }
            unsafe {
                std::env::remove_var(ALLOW_OPEN_ENV);
            }
        });
    }

    #[test]
    fn test_startup_auth_token_present() {
        with_env_lock(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("api-token");
            std::fs::write(&path, "startup-token").unwrap();
            unsafe {
                std::env::set_var(API_TOKEN_FILE_ENV, &path);
                std::env::remove_var(ALLOW_UNAUTHENTICATED_ENV);
            }
            let got = startup_auth();
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
            }
            match got.unwrap() {
                StartupAuth::Token(t) => assert_eq!(t, "startup-token"),
                _ => panic!("expected Token"),
            }
        });
    }

    #[test]
    fn test_startup_auth_unauthenticated_flag() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
                std::env::set_var(ALLOW_UNAUTHENTICATED_ENV, "true");
            }
            let got = startup_auth();
            unsafe {
                std::env::remove_var(ALLOW_UNAUTHENTICATED_ENV);
            }
            assert!(matches!(got.unwrap(), StartupAuth::Unauthenticated));
        });
    }

    #[test]
    fn test_startup_auth_refused_without_token_and_flag() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
                std::env::remove_var(ALLOW_UNAUTHENTICATED_ENV);
            }
            let got = startup_auth();
            match got.unwrap() {
                // test-hooks builds default the flag on (see unauthenticated_permitted),
                // so only assert refusal when the flag is genuinely off.
                StartupAuth::Refused(msg) => assert!(!msg.is_empty()),
                StartupAuth::Unauthenticated if cfg!(feature = "test-hooks") => {}
                other => panic!("unexpected startup_auth result: {other:?}"),
            }
        });
    }

    #[test]
    fn test_startup_auth_flag_false_refused() {
        with_env_lock(|| {
            unsafe {
                std::env::remove_var(API_TOKEN_FILE_ENV);
                std::env::set_var(ALLOW_UNAUTHENTICATED_ENV, "false");
            }
            let got = startup_auth();
            unsafe {
                std::env::remove_var(ALLOW_UNAUTHENTICATED_ENV);
            }
            assert!(matches!(got.unwrap(), StartupAuth::Refused(_)));
        });
    }
}

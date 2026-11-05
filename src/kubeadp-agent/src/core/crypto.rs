//! Shared cryptographic primitives for the whole crate, backed by the
//! `ring` crate.
//!
//! This is the single source for the SHA-256 / HMAC-SHA256 / constant-time
//! comparison helpers that the jobset credential check
//! ([`super::jobset`]), the operator-token gate ([`super::server::token`])
//! and the A2A hop-token signing ([`super::a2a::ingress`]) all need. Each of
//! those modules previously carried its own hand-rolled copy; those copies
//! are gone and every caller now goes through this module.
//!
//! `ring` is a direct, non-optional dependency, so the module (and therefore
//! every feature set -- `cli`, `server`, `a2a`) reaches it.
//!
//! Correctness is pinned by NIST SHA-256 vectors and RFC 4231 HMAC vectors in
//! the test module below (they now exercise `ring` through these wrappers).

// `ring::constant_time::verify_slices_are_equal` is the crate's only
// constant-time byte-slice comparison; in 0.17.14 it is marked `deprecated`
// (ring intends to drop it and leave only its own fixed-size digest/tag
// comparisons). The call sites here compare two internally computed
// fixed-size digests -- never attacker-controlled bytes -- so the side-channel
// guarantee the deprecation points at is not what we rely on. Kept with an
// explicit `#[allow(deprecated)]` until ring ships a public replacement.
#[allow(deprecated)]
use ring::constant_time::verify_slices_are_equal;
use ring::digest::{digest, SHA256};
use ring::hmac::{self, Key as HmacKey};

/// Computes the SHA-256 digest (FIPS 180-4) of `data`.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let d = digest(&SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

/// Computes HMAC-SHA256 (RFC 2104) keyed by `key` over `data`.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mac_key = HmacKey::new(hmac::HMAC_SHA256, key);
    let sig = hmac::sign(&mac_key, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(sig.as_ref());
    out
}

/// Constant-time comparison of two byte slices.
///
/// Returns `false` on a length mismatch (the length difference is not a
/// secret in the call sites that use this helper) and compares the whole
/// input in constant time otherwise.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    // `verify_slices_are_equal` is deprecated (see the rationale on the `use`
    // above); suppress the lint just around the call.
    #[allow(deprecated)]
    {
        verify_slices_are_equal(a, b).is_ok()
    }
}

/// True when the two byte slices have equal SHA-256 digests, compared in
/// constant time. Reducing both values to a fixed 32-byte digest first lets a
/// value whose raw length differs from the expected one still take the same
/// constant-time comparison path.
pub fn digest_eq(a: &[u8], b: &[u8]) -> bool {
    ct_eq(&sha256(a), &sha256(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        let mut s = String::with_capacity(2 * b.len());
        for x in b {
            s.push_str(&format!("{x:02x}"));
        }
        s
    }

    // ---- SHA-256 correctness (NIST vectors, consolidated) --------------------

    #[test]
    fn sha256_known_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // 56-byte input (two 64-byte blocks once padded).
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // 1,000,000 'a' (multi-block stress).
        let million = vec![b'a'; 1_000_000];
        assert_eq!(
            hex(&sha256(&million)),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
        assert_eq!(
            hex(&sha256(b"The quick brown fox jumps over the lazy dog")),
            "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
        );
        // 1 MiB of 'a'.
        let mi = vec![b'a'; 1_048_576];
        assert_eq!(
            hex(&sha256(&mi)),
            "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
        );
        // A 55-byte message hits the padding boundary; it must not equal a
        // shorter single character.
        let msg55 = b"a".repeat(55);
        assert_ne!(hex(&sha256(&msg55)), hex(&sha256(b"a")));
    }

    // ---- HMAC-SHA256 correctness (RFC 4231 vectors) --------------------------

    #[test]
    fn hmac_sha256_known_vectors() {
        // RFC 4231 test case 1.
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // RFC 4231 test case 2.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    // ---- constant-time comparison -------------------------------------------

    #[test]
    fn ct_eq_equal_and_unequal_lengths() {
        // Equal values, equal length.
        assert!(ct_eq(b"abc", b"abc"));
        assert!(ct_eq(b"", b""));
        // Unequal values, equal length.
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"xyz"));
        // Unequal lengths always false (and does not panic).
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abc"));
        assert!(!ct_eq(b"", b"a"));
    }

    #[test]
    fn digest_eq_ignores_length_and_content() {
        // Equal content -> equal digest.
        assert!(digest_eq(b"abc", b"abc"));
        assert!(digest_eq(b"", b""));
        // Different content -> different digest, even when the raw lengths
        // happen to differ (the whole point of digesting first).
        assert!(!digest_eq(b"abc", b"abd"));
        assert!(!digest_eq(b"abc", b"abcd"));
        assert!(!digest_eq(b"", b"a"));
        // Two different-length inputs with the same content match.
        assert!(digest_eq(b"", b""));
    }
}

//! Prompt → `task_digest`.
//!
//! The ledger stores a digest of the prompt, never the body: run accounting
//! must be observable without silently archiving possibly-sensitive task text
//! (see `RunRecord::task_digest`). The format is fixed by the core schema —
//! SHA-256, lower-case hex, first 16 characters — so it must not be
//! hand-rolled from `std`.

use sha2::{Digest, Sha256};

/// Number of leading hex characters kept from the SHA-256 digest.
///
/// Sixteen hex chars = 64 bits, enough to make accidental collisions across a
/// personal-scale ledger negligible while staying short in logs.
const DIGEST_HEX_LEN: usize = 16;

/// Compute the `task_digest` for a prompt: SHA-256 of the UTF-8 bytes, encoded
/// as lower-case hex, truncated to the first [`DIGEST_HEX_LEN`] characters.
///
/// Deterministic and dependency-pinned so a given prompt always yields the same
/// digest across machines and releases.
pub fn task_digest(prompt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prompt.as_bytes());
    let full = hasher.finalize();
    // Render the whole digest to hex, then keep the leading window. Encoding
    // then truncating (rather than truncating bytes) keeps the boundary on a
    // hex-character edge and matches the "hex, first 16 chars" spec exactly.
    let mut hex = String::with_capacity(full.len() * 2);
    for byte in full {
        use std::fmt::Write as _;
        // Writing to a String is infallible; ignore the formatter Result.
        let _ = write!(hex, "{byte:02x}");
    }
    hex.truncate(DIGEST_HEX_LEN);
    hex
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_sixteen_lowercase_hex_chars() {
        let d = task_digest("hello world");
        assert_eq!(d.len(), DIGEST_HEX_LEN);
        assert!(
            d.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn digest_matches_known_sha256_prefix() {
        // Independently verifiable: SHA-256("abc") begins ba7816bf8f01cfea.
        assert_eq!(task_digest("abc"), "ba7816bf8f01cfea");
    }

    #[test]
    fn digest_is_deterministic() {
        assert_eq!(task_digest("same prompt"), task_digest("same prompt"));
    }

    #[test]
    fn different_prompts_differ() {
        assert_ne!(task_digest("prompt a"), task_digest("prompt b"));
    }

    #[test]
    fn empty_prompt_hashes_to_sha256_empty_prefix() {
        // SHA-256("") begins e3b0c44298fc1c14.
        assert_eq!(task_digest(""), "e3b0c44298fc1c14");
    }
}

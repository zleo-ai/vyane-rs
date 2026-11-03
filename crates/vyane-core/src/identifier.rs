//! Shared rules for caller-supplied identifiers.
//!
//! Owners, ids, routes, keys and similar values travel between stores, the
//! run ledger, logs and terminals. Every store that accepts one applies the
//! same rule, so a value that is legal in one store is legal in its
//! neighbours and a value that is rejected is rejected everywhere:
//!
//! * not blank;
//! * at most `max_bytes` UTF-8 bytes;
//! * no leading or trailing whitespace (so `"local"` and `" local"` cannot
//!   become two namespaces that look the same);
//! * no control character (C0 including NUL, DEL, or C1), so a value cannot
//!   forge a log line, a terminal escape or a C-string boundary.
//!
//! Errors carry only the field name and the reason, never the value: a
//! rejected value may be a secret pasted by mistake, or bytes meant to be
//! echoed into an operator log.

use std::fmt;

/// Why an identifier was rejected. The value itself is never retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierProblem {
    /// Empty, or whitespace only.
    Blank,
    /// Longer than the field's byte limit.
    TooLong { max_bytes: usize },
    /// Starts or ends with whitespace.
    SurroundingWhitespace,
    /// Contains a control character (including NUL).
    ControlCharacter,
}

impl fmt::Display for IdentifierProblem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blank => formatter.write_str("must not be blank"),
            Self::TooLong { max_bytes } => write!(formatter, "must be at most {max_bytes} bytes"),
            Self::SurroundingWhitespace => {
                formatter.write_str("must not start or end with whitespace")
            }
            Self::ControlCharacter => formatter.write_str("must not contain control characters"),
        }
    }
}

/// A rejected identifier: the field name and the reason, without the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{field} {problem}")]
pub struct InvalidIdentifier {
    pub field: &'static str,
    pub problem: IdentifierProblem,
}

/// Check `value` against the shared identifier rule (see the module docs).
///
/// `field` names the value in the error; the value itself is never echoed.
pub fn validate_identifier(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), InvalidIdentifier> {
    identifier_problem(value, max_bytes)
        .map_or(Ok(()), |problem| Err(InvalidIdentifier { field, problem }))
}

/// The first problem with `value`, or `None` when it is a valid identifier.
#[must_use]
pub fn identifier_problem(value: &str, max_bytes: usize) -> Option<IdentifierProblem> {
    if value.trim().is_empty() {
        return Some(IdentifierProblem::Blank);
    }
    if value.len() > max_bytes {
        return Some(IdentifierProblem::TooLong { max_bytes });
    }
    if value.trim() != value {
        return Some(IdentifierProblem::SurroundingWhitespace);
    }
    if value.chars().any(char::is_control) {
        return Some(IdentifierProblem::ControlCharacter);
    }
    None
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_identifiers() {
        for value in ["local", "a", "team ops", "run-01.alpha_β", "routing.stage"] {
            assert_eq!(validate_identifier("id", value, 64), Ok(()), "{value:?}");
        }
        assert_eq!(validate_identifier("id", "abcd", 4), Ok(()));
    }

    #[test]
    fn rejects_blank_values() {
        for value in ["", " ", "\t", "\n", "  \u{3000} "] {
            assert_eq!(
                identifier_problem(value, 64),
                Some(IdentifierProblem::Blank),
                "{value:?}"
            );
        }
    }

    #[test]
    fn rejects_values_over_the_byte_limit() {
        assert_eq!(
            identifier_problem("abcde", 4),
            Some(IdentifierProblem::TooLong { max_bytes: 4 })
        );
        // The limit counts UTF-8 bytes, not characters.
        assert_eq!(
            identifier_problem("βββ", 5),
            Some(IdentifierProblem::TooLong { max_bytes: 5 })
        );
    }

    #[test]
    fn rejects_surrounding_whitespace() {
        for value in [" local", "local ", "local\n", "\tlocal", "local\u{a0}"] {
            assert_eq!(
                identifier_problem(value, 64),
                Some(IdentifierProblem::SurroundingWhitespace),
                "{value:?}"
            );
        }
    }

    #[test]
    fn rejects_control_characters_anywhere() {
        for value in [
            "lo\0cal",
            "lo\u{1b}[2Jcal",
            "lo\ncal",
            "lo\tcal",
            "lo\u{7f}cal",
            "lo\u{85}cal",
        ] {
            assert_eq!(
                identifier_problem(value, 64),
                Some(IdentifierProblem::ControlCharacter),
                "{value:?}"
            );
        }
    }

    #[test]
    fn errors_name_the_field_but_never_the_value() {
        const CANARY: &str = "IDENTIFIER_CANARY";
        for value in [
            format!(" {CANARY}"),
            format!("{CANARY}\n"),
            format!("{CANARY}\u{1b}"),
            CANARY.repeat(8),
        ] {
            let error = validate_identifier("owner", &value, 64).unwrap_err();
            let message = error.to_string();
            assert!(message.starts_with("owner "), "{message}");
            assert!(!message.contains(CANARY), "{message}");
            assert!(!format!("{error:?}").contains(CANARY));
        }
    }
}

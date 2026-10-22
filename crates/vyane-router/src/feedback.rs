//! Explicit feedback notes a caller may pass into routing.
//!
//! A note is an input value, not a file or a cache. Scoring copies it onto the
//! route result only when the caller selects [`FeedbackScoring::Enabled`]. The
//! copy is a record: it is not an input to the complexity score or the chosen
//! target. [`FeedbackScoring::Disabled`] is the default and drops the notes.

use serde::{Deserialize, Serialize};

/// Whether caller-supplied [`FeedbackNote`] values may enter route scoring.
///
/// The default is [`FeedbackScoring::Disabled`], which keeps routing on the
/// deterministic score alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FeedbackScoring {
    /// Ignore feedback notes. They are not recorded and cannot change a route.
    #[default]
    Disabled,
    /// Record feedback notes. The chosen target stays the deterministic one.
    Enabled,
}

/// Closed outcome on a [`FeedbackNote`]. No prompt or free-form comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackOutcome {
    Succeeded,
    Failed,
}

impl FeedbackOutcome {
    /// Stable snake_case token matching the serde rename for this outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            FeedbackOutcome::Succeeded => "succeeded",
            FeedbackOutcome::Failed => "failed",
        }
    }
}

impl std::fmt::Display for FeedbackOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Explicit feedback for one opaque target.
///
/// `target_key` is a caller-owned identity. Scoring does not resolve it, and
/// this type has no field for prompt or comment text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedbackNote {
    pub target_key: String,
    pub outcome: FeedbackOutcome,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn outcome_token_matches_serde_name() {
        assert_eq!(FeedbackOutcome::Succeeded.as_str(), "succeeded");
        assert_eq!(FeedbackOutcome::Failed.to_string(), "failed");
        assert_eq!(FeedbackScoring::default(), FeedbackScoring::Disabled);
    }

    #[test]
    fn note_json_is_target_and_outcome_only() {
        let note = FeedbackNote {
            target_key: "profile-a".into(),
            outcome: FeedbackOutcome::Succeeded,
        };
        let json = serde_json::to_value(&note).unwrap();
        assert_eq!(json["target_key"], "profile-a");
        assert_eq!(json["outcome"], "succeeded");
        assert_eq!(json.as_object().unwrap().len(), 2);
    }
}

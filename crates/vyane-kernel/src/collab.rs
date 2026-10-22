//! One collaboration round, recorded without running it.
//!
//! [`RoundRecord`] stores a caller-supplied [`Target`] and a closed outcome:
//! continue, consensus, or stopped. [`Target`] carries no credential and no
//! endpoint. Constructing a record does not dispatch, call a model, or
//! contact GitHub.

use std::fmt;

use serde::{Deserialize, Serialize};
use vyane_core::Target;

/// Closed outcome of one collaboration round.
///
/// These three variants are the whole set. An unrecognized token does not
/// deserialize into this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundOutcome {
    /// Another round may be recorded later. This variant starts nothing.
    Continue,
    /// The round ended because the participants agreed.
    Consensus,
    /// The round ended because collaboration stopped.
    Stopped,
}

impl RoundOutcome {
    /// Stable snake_case token matching the serde rename for this outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Consensus => "consensus",
            Self::Stopped => "stopped",
        }
    }
}

impl fmt::Display for RoundOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One recorded collaboration round.
///
/// Audit data only. `target` is an identity the caller already has, including
/// a fake target in tests. Nothing here resolves an endpoint or performs I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundRecord {
    /// Caller-chosen round number. The kernel does not allocate or advance it.
    pub round: u32,
    /// Loggable target identity. No credential and no endpoint.
    pub target: Target,
    pub outcome: RoundOutcome,
}

impl RoundRecord {
    /// Record `outcome` for `round` against `target`.
    ///
    /// This only builds the record. It does not call the target.
    #[must_use]
    pub const fn new(round: u32, target: Target, outcome: RoundOutcome) -> Self {
        Self {
            round,
            target,
            outcome,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{RoundOutcome, RoundRecord};
    use vyane_core::{ModelId, Protocol, ProviderId, Target};

    fn fake_target() -> Target {
        Target {
            provider: ProviderId::new("fake"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("fake-target"),
        }
    }

    #[test]
    fn fake_target_round_is_continue_consensus_or_stopped() {
        let target = fake_target();
        let cases = [
            (0, RoundOutcome::Continue, "continue"),
            (1, RoundOutcome::Consensus, "consensus"),
            (2, RoundOutcome::Stopped, "stopped"),
        ];

        for (round, outcome, token) in cases {
            let record = RoundRecord::new(round, target.clone(), outcome);
            assert_eq!(record.round, round);
            assert_eq!(record.target, target);
            assert_eq!(record.target.provider.as_str(), "fake");
            assert_eq!(record.target.model.as_str(), "fake-target");
            assert!(record.target.harness.is_none());
            assert_eq!(record.outcome, outcome);
            assert_eq!(record.outcome.as_str(), token);
            assert_eq!(record.outcome.to_string(), token);

            let json = serde_json::to_string(&record).unwrap();
            let back: RoundRecord = serde_json::from_str(&json).unwrap();
            assert_eq!(back, record);
            assert_eq!(
                serde_json::to_string(&outcome).unwrap(),
                format!("\"{token}\"")
            );
        }

        assert!(serde_json::from_str::<RoundOutcome>("\"retry\"").is_err());
        assert!(serde_json::from_str::<RoundOutcome>("\"github\"").is_err());

        let mut value =
            serde_json::to_value(RoundRecord::new(0, target, RoundOutcome::Stopped)).unwrap();
        value.as_object_mut().unwrap().insert(
            "channel".to_string(),
            serde_json::Value::String("github".to_string()),
        );
        assert!(serde_json::from_value::<RoundRecord>(value).is_err());
    }
}

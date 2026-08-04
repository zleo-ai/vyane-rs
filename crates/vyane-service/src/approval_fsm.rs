//! Pure approval resume state machine for the kernel integration pilot.
//!
//! Durable decisions live in [`crate::kernel_store::KernelStore`]. This module
//! only encodes legal transitions so truth probes can target shipped logic
//! without OS I/O.

use serde::{Deserialize, Serialize};

/// Delivery phase for Process-lane autonomous delivery under approval resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPhase {
    Running,
    ApprovalRequired,
    Approved,
    Denied,
    Resuming,
    Verified,
    Completed,
    Failed,
    Cancelled,
}

impl DeliveryPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::ApprovalRequired => "approval_required",
            Self::Approved => "approved",
            Self::Denied => "denied",
            Self::Resuming => "resuming",
            Self::Verified => "verified",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "approval_required" => Some(Self::ApprovalRequired),
            "approved" => Some(Self::Approved),
            "denied" => Some(Self::Denied),
            "resuming" => Some(Self::Resuming),
            "verified" => Some(Self::Verified),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Denied
        )
    }

    /// Deny is terminal for approval: grant must never recover it.
    #[must_use]
    pub const fn blocks_grant(self) -> bool {
        matches!(
            self,
            Self::Denied | Self::Completed | Self::Failed | Self::Cancelled
        )
    }

    /// Effects may only run while actively executing (not waiting on approval).
    #[must_use]
    pub const fn allows_effect(self) -> bool {
        matches!(
            self,
            Self::Running | Self::Resuming | Self::Approved | Self::Verified
        )
    }
}

/// Event that may advance the approval/delivery FSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryEvent {
    /// Permission ceiling returned `ask` while phase is Running.
    AskRequired,
    /// Bound grant accepted (or idempotent re-grant).
    GrantAccepted,
    /// Explicit deny.
    DenyAccepted,
    /// Execution continues after grant.
    ResumeStarted,
    /// Truth probe + artifact verified.
    Verified,
    /// Terminal success.
    Complete,
    /// Terminal failure (not deny).
    Fail,
    /// Cancel.
    Cancel,
}

/// Typed FSM error — always fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTransitionError {
    IllegalTransition,
    TerminalImmutable,
    DenyIsFinal,
    GrantRequiresAsk,
}

impl std::fmt::Display for DeliveryTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IllegalTransition => f.write_str("illegal delivery transition"),
            Self::TerminalImmutable => f.write_str("terminal delivery phase is immutable"),
            Self::DenyIsFinal => f.write_str("denied delivery cannot be granted"),
            Self::GrantRequiresAsk => {
                f.write_str("grant only valid from approval_required (or idempotent approved)")
            }
        }
    }
}

impl std::error::Error for DeliveryTransitionError {}

/// Pure transition. Does not touch durable storage.
pub fn transition(
    current: DeliveryPhase,
    event: DeliveryEvent,
) -> Result<DeliveryPhase, DeliveryTransitionError> {
    if current.is_terminal() && !matches!(event, DeliveryEvent::GrantAccepted) {
        // Grant on Denied is a special fail-closed case below.
        if matches!(
            event,
            DeliveryEvent::Complete
                | DeliveryEvent::Fail
                | DeliveryEvent::Cancel
                | DeliveryEvent::Verified
                | DeliveryEvent::ResumeStarted
                | DeliveryEvent::AskRequired
                | DeliveryEvent::DenyAccepted
        ) {
            return Err(DeliveryTransitionError::TerminalImmutable);
        }
    }

    match (current, event) {
        (DeliveryPhase::Running, DeliveryEvent::AskRequired) => Ok(DeliveryPhase::ApprovalRequired),
        (DeliveryPhase::Running, DeliveryEvent::Verified) => Ok(DeliveryPhase::Verified),
        (DeliveryPhase::Running, DeliveryEvent::Complete) => Ok(DeliveryPhase::Completed),
        (DeliveryPhase::Running, DeliveryEvent::Fail) => Ok(DeliveryPhase::Failed),
        (DeliveryPhase::Running, DeliveryEvent::Cancel) => Ok(DeliveryPhase::Cancelled),
        (DeliveryPhase::Running, DeliveryEvent::DenyAccepted) => Ok(DeliveryPhase::Denied),

        (DeliveryPhase::ApprovalRequired, DeliveryEvent::GrantAccepted) => {
            Ok(DeliveryPhase::Approved)
        }
        (DeliveryPhase::ApprovalRequired, DeliveryEvent::DenyAccepted) => Ok(DeliveryPhase::Denied),
        (DeliveryPhase::ApprovalRequired, DeliveryEvent::Cancel) => Ok(DeliveryPhase::Cancelled),
        (DeliveryPhase::ApprovalRequired, DeliveryEvent::Fail) => Ok(DeliveryPhase::Failed),

        // Idempotent re-grant while already approved.
        (DeliveryPhase::Approved, DeliveryEvent::GrantAccepted) => Ok(DeliveryPhase::Approved),
        (DeliveryPhase::Approved, DeliveryEvent::ResumeStarted) => Ok(DeliveryPhase::Resuming),
        // Complete may follow grant even if Resuming was not persisted (crash window).
        (DeliveryPhase::Approved, DeliveryEvent::Complete) => Ok(DeliveryPhase::Completed),
        (DeliveryPhase::Approved, DeliveryEvent::Verified) => Ok(DeliveryPhase::Verified),
        (DeliveryPhase::Approved, DeliveryEvent::Cancel) => Ok(DeliveryPhase::Cancelled),
        (DeliveryPhase::Approved, DeliveryEvent::Fail) => Ok(DeliveryPhase::Failed),

        (DeliveryPhase::Resuming, DeliveryEvent::Verified) => Ok(DeliveryPhase::Verified),
        (DeliveryPhase::Resuming, DeliveryEvent::Complete) => Ok(DeliveryPhase::Completed),
        (DeliveryPhase::Resuming, DeliveryEvent::Fail) => Ok(DeliveryPhase::Failed),
        (DeliveryPhase::Resuming, DeliveryEvent::Cancel) => Ok(DeliveryPhase::Cancelled),
        // Allow continuing work after resume without forcing Verified first.
        (DeliveryPhase::Resuming, DeliveryEvent::ResumeStarted) => Ok(DeliveryPhase::Resuming),

        (DeliveryPhase::Verified, DeliveryEvent::Complete) => Ok(DeliveryPhase::Completed),
        (DeliveryPhase::Verified, DeliveryEvent::Fail) => Ok(DeliveryPhase::Failed),
        (DeliveryPhase::Verified, DeliveryEvent::Cancel) => Ok(DeliveryPhase::Cancelled),

        // Deny is never recoverable by grant.
        (DeliveryPhase::Denied, DeliveryEvent::GrantAccepted) => {
            Err(DeliveryTransitionError::DenyIsFinal)
        }

        // Grant outside ask/approved is illegal.
        (_, DeliveryEvent::GrantAccepted) => Err(DeliveryTransitionError::GrantRequiresAsk),

        _ => Err(DeliveryTransitionError::IllegalTransition),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn ask_grant_resume_complete() {
        let mut p = DeliveryPhase::Running;
        p = transition(p, DeliveryEvent::AskRequired).unwrap();
        assert_eq!(p, DeliveryPhase::ApprovalRequired);
        p = transition(p, DeliveryEvent::GrantAccepted).unwrap();
        assert_eq!(p, DeliveryPhase::Approved);
        p = transition(p, DeliveryEvent::ResumeStarted).unwrap();
        assert_eq!(p, DeliveryPhase::Resuming);
        p = transition(p, DeliveryEvent::Verified).unwrap();
        p = transition(p, DeliveryEvent::Complete).unwrap();
        assert_eq!(p, DeliveryPhase::Completed);
    }

    #[test]
    fn deny_then_grant_fails_closed() {
        let p = transition(DeliveryPhase::Running, DeliveryEvent::AskRequired).unwrap();
        let p = transition(p, DeliveryEvent::DenyAccepted).unwrap();
        assert_eq!(p, DeliveryPhase::Denied);
        let err = transition(p, DeliveryEvent::GrantAccepted).unwrap_err();
        assert_eq!(err, DeliveryTransitionError::DenyIsFinal);
    }

    #[test]
    fn grant_without_ask_fails() {
        let err = transition(DeliveryPhase::Running, DeliveryEvent::GrantAccepted).unwrap_err();
        assert_eq!(err, DeliveryTransitionError::GrantRequiresAsk);
    }

    #[test]
    fn repeated_grant_is_idempotent() {
        let p = transition(DeliveryPhase::Running, DeliveryEvent::AskRequired).unwrap();
        let p = transition(p, DeliveryEvent::GrantAccepted).unwrap();
        let p2 = transition(p, DeliveryEvent::GrantAccepted).unwrap();
        assert_eq!(p2, DeliveryPhase::Approved);
    }

    #[test]
    fn effects_blocked_in_approval_required_and_denied() {
        assert!(!DeliveryPhase::ApprovalRequired.allows_effect());
        assert!(!DeliveryPhase::Denied.allows_effect());
        assert!(DeliveryPhase::Resuming.allows_effect());
        assert!(DeliveryPhase::Running.allows_effect());
    }
}

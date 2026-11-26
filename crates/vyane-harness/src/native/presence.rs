//! In-process presence for the native harness.
//!
//! Session start, post-tool heartbeat, and stop call [`crate::presence`]
//! directly. They do not spawn a process, and they have no parameter for a
//! prompt or a tool result. Claude Code and Codex settings are untouched.

use std::sync::Mutex;
use std::time::Duration;

use crate::presence::{
    CheckIn, CheckInOutcome, DEFAULT_HEARTBEAT_INTERVAL, HARNESS_NATIVE, Heartbeat,
    HeartbeatOutcome, LeaveOutcome, ListQuery, PresenceError, PresenceList, PresenceStore,
    validate_check_in,
};

/// Identity stored for one native session. Card and status are metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeIdentity {
    /// Session key.
    pub session_id: String,
    /// Display label.
    pub label: String,
    /// Repository label.
    pub repo: String,
    /// Opaque card id.
    pub card: String,
    /// One-line status.
    pub status: String,
    /// Harness name. Empty becomes [`HARNESS_NATIVE`].
    pub harness: String,
}

/// In-process presence session.
///
/// The methods below are the native lifecycle. None of them starts a process.
#[derive(Debug)]
pub struct NativePresence {
    store: PresenceStore,
    identity: NativeIdentity,
    heartbeat_interval: Duration,
    last_beat: Mutex<Option<f64>>,
}

impl NativePresence {
    /// Bind a session to a table. This does not write and does not spawn.
    pub fn open(store: PresenceStore, mut identity: NativeIdentity) -> Result<Self, PresenceError> {
        if identity.harness.trim().is_empty() {
            identity.harness = HARNESS_NATIVE.to_owned();
        }
        validate_check_in(&CheckIn {
            session_id: identity.session_id.clone(),
            label: identity.label.clone(),
            repo: identity.repo.clone(),
            card: identity.card.clone(),
            status: identity.status.clone(),
            harness: identity.harness.clone(),
        })?;
        Ok(Self {
            store,
            identity,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            last_beat: Mutex::new(None),
        })
    }

    /// Replace the post-tool heartbeat gap.
    pub fn set_heartbeat_interval(&mut self, interval: Duration) {
        self.heartbeat_interval = interval;
    }

    /// Full check-in for session start, including the opaque card and one-line status.
    pub fn on_session_start(&self, now: f64) -> Result<CheckInOutcome, PresenceError> {
        let outcome = self.store.check_in(&self.check_in_update(), now)?;
        if outcome.written {
            self.remember_beat(now);
        }
        Ok(outcome)
    }

    /// Throttled heartbeat after a tool call.
    ///
    /// Returns `Ok(None)` when the previous beat is still inside the interval.
    /// The tool name and output are not arguments and are not written.
    pub fn on_after_tool(&self, now: f64) -> Result<Option<HeartbeatOutcome>, PresenceError> {
        if self.throttled(now) {
            return Ok(None);
        }
        let outcome = self.store.heartbeat(&self.heartbeat_update(), now)?;
        if outcome.written {
            self.remember_beat(now);
        }
        Ok(Some(outcome))
    }

    /// Remove this session when the native run stops.
    pub fn on_stop(&self, now: f64) -> Result<LeaveOutcome, PresenceError> {
        self.store.leave(&self.identity.session_id, now)
    }

    /// Other online sessions. This session is excluded.
    pub fn peers(&self, now: f64) -> Result<PresenceList, PresenceError> {
        self.store.list(
            &ListQuery {
                session_id: self.identity.session_id.clone(),
                include_self: false,
                include_offline: false,
            },
            now,
        )
    }

    fn check_in_update(&self) -> CheckIn {
        CheckIn {
            session_id: self.identity.session_id.clone(),
            label: self.identity.label.clone(),
            repo: self.identity.repo.clone(),
            card: self.identity.card.clone(),
            status: self.identity.status.clone(),
            harness: self.identity.harness.clone(),
        }
    }

    fn heartbeat_update(&self) -> Heartbeat {
        Heartbeat {
            session_id: self.identity.session_id.clone(),
            label: self.identity.label.clone(),
            repo: self.identity.repo.clone(),
            harness: self.identity.harness.clone(),
        }
    }

    fn throttled(&self, now: f64) -> bool {
        let last = self
            .last_beat
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        matches!(
            *last,
            Some(last) if now >= last && now - last < self.heartbeat_interval.as_secs_f64()
        )
    }

    fn remember_beat(&self, now: f64) {
        let mut last = self
            .last_beat
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *last = Some(now);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::presence::{HARNESS_NATIVE, PresenceStore};
    use serde_json::Value;

    fn identity(session: &str, card: &str, status: &str) -> NativeIdentity {
        NativeIdentity {
            session_id: session.to_owned(),
            label: session.to_owned(),
            repo: "vyane-rs".to_owned(),
            card: card.to_owned(),
            status: status.to_owned(),
            harness: String::new(),
        }
    }

    #[test]
    fn two_native_sessions_see_each_other_without_spawning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let mut alpha = NativePresence::open(
            PresenceStore::open(&path),
            identity("alpha", "card-a", "editing"),
        )
        .unwrap();
        let beta = NativePresence::open(
            PresenceStore::open(&path),
            identity("beta", "card-b", "reading"),
        )
        .unwrap();
        alpha.on_session_start(1_000.0).unwrap();
        beta.on_session_start(1_001.0).unwrap();

        let seen_by_alpha = alpha.peers(1_002.0).unwrap();
        assert_eq!(seen_by_alpha.agents.len(), 1);
        assert_eq!(seen_by_alpha.agents[0].session_id, "beta");
        assert_eq!(seen_by_alpha.agents[0].card, "card-b");
        assert_eq!(seen_by_alpha.agents[0].harness, HARNESS_NATIVE);

        let seen_by_beta = beta.peers(1_002.0).unwrap();
        assert_eq!(seen_by_beta.agents[0].session_id, "alpha");
        assert_eq!(seen_by_beta.agents[0].status, "editing");

        alpha.set_heartbeat_interval(std::time::Duration::from_secs(30));
        assert!(alpha.on_after_tool(1_010.0).unwrap().is_none());
        let heartbeat = alpha.on_after_tool(1_040.0).unwrap().unwrap();
        let agent = heartbeat.agent.unwrap();
        assert_eq!(agent.card, "card-a");
        assert_eq!(agent.status, "editing");
        assert_eq!(agent.last_checkin, Some(1_040.0));

        let stored: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let record = stored["agents"]["alpha"].as_object().unwrap();
        assert!(record.get("prompt").is_none());
        assert!(record.get("tool_output").is_none());
        assert_eq!(record["status"], "editing");

        assert!(alpha.on_stop(1_050.0).unwrap().removed);
        assert!(
            alpha
                .peers(1_051.0)
                .unwrap()
                .agents
                .iter()
                .all(|peer| peer.session_id != "alpha")
        );
        assert_eq!(beta.peers(1_051.0).unwrap().agents.len(), 0);
    }

    #[test]
    fn open_rejects_tool_output_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let mut identity = identity("alpha", "card", "status");
        identity.status = format!("tool output\n{}", "x".repeat(400));
        let error = NativePresence::open(PresenceStore::open(&path), identity).unwrap_err();
        assert_eq!(error.as_str(), "invalid_field");
        assert!(!path.exists());
    }
}

//! One owner-scoped EventLog fact for a terminal dispatch.
//!
//! The kernel appends exactly one [`RunRecord`] after a local dispatch reaches
//! a terminal status. This wrapper forwards that write to the run ledger, then
//! appends one durable event on the existing [`EventLog`]. It does not buffer
//! events, open a subscription, or start a resident loop.

use std::collections::BTreeMap;

use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use vyane_core::{Ledger, RunQuery, RunRecord};
use vyane_ledger::{
    EventCategory, EventDurability, EventLog, EventLogError, EventSource, JsonlLedger, NewEvent,
};

pub(crate) const DISPATCH_EVENT_STREAM: &str = "dispatch";
const DISPATCH_TERMINAL_EVENT_TYPE: &str = "dispatch.terminal";
const EVENT_ID_PREFIX: &str = "dispatch.terminal.";
const MAX_REFERENCE_BYTES: usize = 256;

pub(crate) struct DispatchEventLedger {
    inner: JsonlLedger,
    events: EventLog,
}

impl DispatchEventLedger {
    pub(crate) fn new(inner: JsonlLedger, events: EventLog) -> Self {
        Self { inner, events }
    }
}

#[async_trait]
impl Ledger for DispatchEventLedger {
    async fn append(&self, record: &RunRecord) -> vyane_core::Result<()> {
        self.inner.append(record).await?;
        if let Err(error) = append_terminal_dispatch(&self.events, record).await {
            // Kind only. A failed projection must not hide the run record or
            // invite the caller to repeat the finished dispatch.
            tracing::warn!(
                error = error.as_str(),
                "dispatch terminal event append failed"
            );
        }
        Ok(())
    }

    async fn query(&self, query: RunQuery) -> vyane_core::Result<Vec<RunRecord>> {
        self.inner.query(query).await
    }
}

/// Stable identity for one terminal dispatch. Short run ids stay readable;
/// anything that would not fit an EventLog reference is a domain-separated hash.
pub(crate) fn dispatch_terminal_event_id(run_id: &str) -> String {
    let direct = format!("{EVENT_ID_PREFIX}{run_id}");
    if fits_reference(&direct) {
        direct
    } else {
        let mut digest = Sha256::new();
        digest.update(b"vyane.dispatch.terminal.v1\0");
        digest.update(run_id.as_bytes());
        let encoded = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!("{EVENT_ID_PREFIX}{encoded}")
    }
}

fn fits_reference(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_REFERENCE_BYTES && !value.contains('\0')
}

async fn append_terminal_dispatch(
    events: &EventLog,
    record: &RunRecord,
) -> Result<(), EventLogError> {
    let mut payload = BTreeMap::new();
    payload.insert(
        "run_id".to_string(),
        serde_json::Value::String(record.run_id.clone()),
    );
    payload.insert(
        "status".to_string(),
        serde_json::Value::String(record.status.as_str().to_string()),
    );
    let mut event = NewEvent::new(
        record.owner.clone(),
        EventCategory::Lifecycle,
        DISPATCH_TERMINAL_EVENT_TYPE,
        EventSource::Kernel,
    );
    event.event_id = dispatch_terminal_event_id(&record.run_id);
    if fits_reference(&record.run_id) {
        event.correlation_id = Some(record.run_id.clone());
    }
    event.payload = payload;
    events
        .append(DISPATCH_EVENT_STREAM, event, EventDurability::Durable)
        .await
        .map(|_| ())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use vyane_config::ResolvedConfig;
    use vyane_core::{
        AdapterTransport, Ledger, ModelId, Protocol, ProviderId, RunQuery, RunRecord, RunStatus,
        Sandbox, Target,
    };
    use vyane_ledger::{EventCategory, EventLog, EventSource, JsonlLedger};

    use super::{DISPATCH_EVENT_STREAM, DispatchEventLedger, dispatch_terminal_event_id};
    use crate::config::{Runtime, StoragePaths};

    fn target() -> Target {
        Target {
            provider: ProviderId::new("provider"),
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("model"),
        }
    }

    fn terminal_record(owner: &str, run_id: &str, status: RunStatus) -> RunRecord {
        let now = chrono::Utc::now();
        RunRecord {
            run_id: run_id.into(),
            owner: owner.into(),
            started_at: now,
            finished_at: now,
            task_digest: "CANARY_DIGEST".into(),
            task_preview: Some("CANARY_PROMPT".into()),
            workdir: Some("/CANARY_WORKDIR".into()),
            sandbox: Sandbox::ReadOnly,
            target: target(),
            transport: AdapterTransport::DirectHttp,
            attempts: Vec::new(),
            status,
            usage: None,
            cost_usd: None,
            session_id: Some("CANARY_SESSION".into()),
            output_chars: Some(9),
            error: Some("CANARY_ERROR".into()),
            labels: BTreeMap::from([("CANARY_LABEL".into(), "CANARY_VALUE".into())]),
        }
    }

    fn runtime() -> (tempfile::TempDir, Runtime) {
        let directory = crate::test_support::private_tempdir();
        let paths = StoragePaths::from_data_dir(directory.path());
        let runtime = Runtime::new(ResolvedConfig::default(), paths).unwrap();
        (directory, runtime)
    }

    async fn events_for(root: &std::path::Path, owner: &str) -> Vec<vyane_ledger::EventRecord> {
        EventLog::new(root.join("events"))
            .read_after(owner, DISPATCH_EVENT_STREAM, 0, 10)
            .await
            .unwrap()
            .events
    }

    #[test]
    fn terminal_event_id_is_stable_and_bounded() {
        assert_eq!(
            dispatch_terminal_event_id("run-1"),
            "dispatch.terminal.run-1"
        );
        assert_eq!(
            dispatch_terminal_event_id("run-1"),
            dispatch_terminal_event_id("run-1")
        );
        assert_ne!(
            dispatch_terminal_event_id("run-1"),
            dispatch_terminal_event_id("run-2")
        );
        let long = "x".repeat(300);
        let hashed = dispatch_terminal_event_id(&long);
        assert_eq!(hashed, dispatch_terminal_event_id(&long));
        assert!(hashed.len() <= 256);
        assert!(!hashed.contains(&long));
    }

    #[tokio::test]
    async fn runtime_appends_one_owner_scoped_terminal_event() {
        let (directory, runtime) = runtime();
        runtime
            .ledger
            .append(&terminal_record("alice", "run-a", RunStatus::Success))
            .await
            .unwrap();
        runtime
            .ledger
            .append(&terminal_record("bob", "run-b", RunStatus::Error))
            .await
            .unwrap();

        let alice = events_for(directory.path(), "alice").await;
        let bob = events_for(directory.path(), "bob").await;
        assert_eq!(alice.len(), 1);
        assert_eq!(bob.len(), 1);
        assert_eq!(alice[0].owner, "alice");
        assert_eq!(alice[0].event_id, dispatch_terminal_event_id("run-a"));
        assert_eq!(alice[0].correlation_id.as_deref(), Some("run-a"));
        assert_eq!(alice[0].category, EventCategory::Lifecycle);
        assert_eq!(alice[0].event_type, "dispatch.terminal");
        assert_eq!(alice[0].source, EventSource::Kernel);
        assert!(alice[0].summary.is_none());
        assert!(alice[0].trace_id.is_none());
        assert_eq!(
            alice[0]
                .payload
                .get("status")
                .and_then(|value| value.as_str()),
            Some("success")
        );
        assert_eq!(
            alice[0]
                .payload
                .get("run_id")
                .and_then(|value| value.as_str()),
            Some("run-a")
        );
        assert_eq!(alice[0].payload.len(), 2);
        assert_eq!(
            bob[0]
                .payload
                .get("status")
                .and_then(|value| value.as_str()),
            Some("error")
        );
        let wire =
            serde_json::to_string(&alice[0]).unwrap() + &serde_json::to_string(&bob[0]).unwrap();
        for canary in [
            "CANARY_DIGEST",
            "CANARY_PROMPT",
            "CANARY_WORKDIR",
            "CANARY_SESSION",
            "CANARY_ERROR",
            "CANARY_LABEL",
            "CANARY_VALUE",
        ] {
            assert!(!wire.contains(canary), "leaked {canary}");
        }

        let queried = runtime
            .ledger
            .query(RunQuery {
                owner: Some("alice".into()),
                ..RunQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].run_id, "run-a");
        assert_eq!(events_for(directory.path(), "alice").await.len(), 1);
        assert!(events_for(directory.path(), "local").await.is_empty());
    }

    #[tokio::test]
    async fn repeated_terminal_append_reuses_the_stable_id() {
        let (directory, runtime) = runtime();
        let record = terminal_record("local", "run-same", RunStatus::Cancelled);
        runtime.ledger.append(&record).await.unwrap();
        runtime.ledger.append(&record).await.unwrap();
        let events = events_for(directory.path(), "local").await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id, events[1].event_id);
        assert_eq!(events[0].event_id, dispatch_terminal_event_id("run-same"));
        assert_eq!(
            events[0]
                .payload
                .get("status")
                .and_then(|value| value.as_str()),
            Some("cancelled")
        );
    }

    #[tokio::test]
    async fn event_log_rejection_keeps_the_run_record() {
        let directory = crate::test_support::private_tempdir();
        let blocked = directory.path().join("events");
        std::fs::write(&blocked, b"blocked").unwrap();
        let ledger = Arc::new(DispatchEventLedger::new(
            JsonlLedger::new(directory.path().join("ledger.jsonl")),
            EventLog::new(&blocked),
        ));
        ledger
            .append(&terminal_record("alice", "run-timeout", RunStatus::Timeout))
            .await
            .unwrap();
        let stored = ledger.query(RunQuery::default()).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].run_id, "run-timeout");
        assert_eq!(stored[0].status, RunStatus::Timeout);
        assert_eq!(std::fs::read(&blocked).unwrap(), b"blocked");
    }
}

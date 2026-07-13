use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use vyane_agent::{AgentEvent, AgentStore};
use vyane_ledger::{EventCategory, EventDurability, EventLog, EventSource, NewEvent};

use crate::{BrokerScope, ProjectionReport, Result};

pub const DEFAULT_AGENT_EVENT_PROJECTOR: &str = "vyane.event-log.agent-lifecycle.v1";
pub const DEFAULT_AGENT_EVENT_STREAM: &str = "agent-lifecycle";

/// Projects one bounded page of the transactional, body-free AgentRun outbox
/// into an at-least-once EventLog stream.
///
/// The source event id is reused as the stable projection identity. Each
/// append is made durable before its source outbox row is acknowledged, so a
/// crash in between can append the same event id again. EventLog consumers
/// must therefore deduplicate by event id.
#[derive(Clone)]
pub struct AgentEventProjector {
    scope: BrokerScope,
    store: Arc<dyn AgentStore>,
    event_log: EventLog,
    projector_id: String,
    stream_id: String,
}

impl AgentEventProjector {
    #[must_use]
    pub fn new(scope: BrokerScope, store: Arc<dyn AgentStore>, event_log: EventLog) -> Self {
        Self::with_identity(
            scope,
            store,
            event_log,
            DEFAULT_AGENT_EVENT_PROJECTOR,
            DEFAULT_AGENT_EVENT_STREAM,
        )
    }

    #[must_use]
    pub fn with_identity(
        scope: BrokerScope,
        store: Arc<dyn AgentStore>,
        event_log: EventLog,
        projector_id: impl Into<String>,
        stream_id: impl Into<String>,
    ) -> Self {
        Self {
            scope,
            store,
            event_log,
            projector_id: projector_id.into(),
            stream_id: stream_id.into(),
        }
    }

    #[must_use]
    pub fn projector_id(&self) -> &str {
        &self.projector_id
    }

    #[must_use]
    pub fn scope(&self) -> &BrokerScope {
        &self.scope
    }

    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Project at most `limit` source events and return without starting a
    /// resident loop. A failed append leaves the event unacknowledged.
    pub async fn project_once(&self, limit: usize) -> Result<ProjectionReport> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner().to_string();
        let projector = self.projector_id.clone();
        let page = tokio::task::spawn_blocking(move || {
            store.unprojected_events(&owner, &projector, limit)
        })
        .await??;
        let read = page.items.len();
        let mut projected = 0;
        for item in page.items {
            self.event_log
                .append(&self.stream_id, map_event(&item), EventDurability::Durable)
                .await?;
            let store = Arc::clone(&self.store);
            let owner = self.scope.owner().to_string();
            let projector = self.projector_id.clone();
            let event_id = item.event_id;
            tokio::task::spawn_blocking(move || {
                store.mark_projected(&owner, &projector, &event_id)
            })
            .await??;
            projected += 1;
        }
        Ok(ProjectionReport {
            read,
            projected,
            has_more: page.has_more,
        })
    }
}

fn map_event(event: &AgentEvent) -> NewEvent {
    let mut payload = BTreeMap::new();
    payload.insert("agent_event_sequence".into(), json!(event.sequence));
    payload.insert("worker_id".into(), json!(event.worker_id));
    payload.insert("worker_revision".into(), json!(event.worker_revision));
    payload.insert(
        "worker_lifecycle".into(),
        json!(event.worker_lifecycle.to_string()),
    );
    if let Some(run_id) = &event.run_id {
        payload.insert("run_id".into(), json!(run_id));
    }
    if let Some(run_revision) = event.run_revision {
        payload.insert("run_revision".into(), json!(run_revision));
    }
    if let Some(run_state) = event.run_state {
        payload.insert("run_state".into(), json!(run_state.to_string()));
    }
    NewEvent {
        event_id: event.event_id.clone(),
        owner: event.owner.clone(),
        category: EventCategory::Lifecycle,
        event_type: format!("agent.{}", event.kind),
        source: EventSource::Daemon,
        trace_id: None,
        correlation_id: event
            .run_id
            .clone()
            .or_else(|| Some(event.worker_id.clone())),
        summary: None,
        payload,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use chrono::Utc;
    use tempfile::TempDir;
    use vyane_agent::{NewAgentRun, NewWorker, RunMode, SqliteAgentStore};

    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn add_root(store: &dyn AgentStore, owner: &str) {
        store
            .create_root(
                owner,
                &NewWorker {
                    id: "worker".into(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: "run".into(),
                    worker_id: "worker".into(),
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    mode: RunMode::Autonomous,
                    target_key: "native/default".into(),
                    prompt_digest: digest('a'),
                    policy_digest: digest('b'),
                    available_at: Utc::now(),
                    timeout_seconds: 60,
                    max_resume_attempts: 1,
                },
            )
            .unwrap();
    }

    #[tokio::test]
    async fn restart_after_append_retries_the_same_stable_identity() {
        let directory = TempDir::new().unwrap();
        let concrete =
            Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
        add_root(concrete.as_ref(), "owner");
        let store: Arc<dyn AgentStore> = concrete;
        let source = store
            .unprojected_events("owner", "restart-projector", 1)
            .unwrap()
            .items
            .remove(0);
        let mapped = map_event(&source);
        let event_root = directory.path().join("events");
        let event_log = EventLog::new(&event_root);

        // Simulate a process stopping after durable append and before the
        // outbox acknowledgement becomes visible.
        event_log
            .append("restart-stream", mapped.clone(), EventDurability::Durable)
            .await
            .unwrap();

        let restarted = AgentEventProjector::with_identity(
            BrokerScope::new("owner").unwrap(),
            Arc::clone(&store),
            EventLog::new(&event_root),
            "restart-projector",
            "restart-stream",
        );
        assert_eq!(restarted.project_once(1).await.unwrap().projected, 1);

        let page = EventLog::new(&event_root)
            .read_after("owner", "restart-stream", 0, 10)
            .await
            .unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.events[0].event_id, source.event_id);
        assert_eq!(page.events[1].event_id, source.event_id);
        assert_eq!(page.events[0].category, page.events[1].category);
        assert_eq!(page.events[0].event_type, page.events[1].event_type);
        assert_eq!(page.events[0].source, page.events[1].source);
        assert_eq!(page.events[0].correlation_id, page.events[1].correlation_id);
        assert_eq!(page.events[0].payload, page.events[1].payload);

        let remaining = store
            .unprojected_events("owner", "restart-projector", 10)
            .unwrap();
        assert!(
            remaining
                .items
                .iter()
                .all(|event| event.event_id != source.event_id)
        );
    }
}

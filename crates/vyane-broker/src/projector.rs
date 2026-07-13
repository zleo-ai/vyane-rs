use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use vyane_ledger::{EventCategory, EventDurability, EventLog, EventSource, NewEvent};
use vyane_message::{MessageEvent, MessageStore};

use crate::{BrokerScope, Result};

pub const DEFAULT_MESSAGE_EVENT_PROJECTOR: &str = "vyane.event-log.message-lifecycle.v1";
pub const DEFAULT_MESSAGE_EVENT_STREAM: &str = "message-lifecycle";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionReport {
    pub read: usize,
    pub projected: usize,
    pub has_more: bool,
}

/// Projects the transactional body-free outbox into an at-least-once EventLog
/// stream. A successful append is acknowledged to the outbox afterwards, so a
/// crash between the two may append the same stable event id again.
#[derive(Clone)]
pub struct MessageEventProjector {
    scope: BrokerScope,
    store: Arc<dyn MessageStore>,
    event_log: EventLog,
    projector_id: String,
    stream_id: String,
}

impl MessageEventProjector {
    #[must_use]
    pub fn new(scope: BrokerScope, store: Arc<dyn MessageStore>, event_log: EventLog) -> Self {
        Self::with_identity(
            scope,
            store,
            event_log,
            DEFAULT_MESSAGE_EVENT_PROJECTOR,
            DEFAULT_MESSAGE_EVENT_STREAM,
        )
    }

    #[must_use]
    pub fn with_identity(
        scope: BrokerScope,
        store: Arc<dyn MessageStore>,
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

    pub(crate) fn store(&self) -> &Arc<dyn MessageStore> {
        &self.store
    }

    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

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

fn map_event(event: &MessageEvent) -> NewEvent {
    let mut payload = BTreeMap::new();
    payload.insert("message_id".into(), json!(event.message_id));
    payload.insert("delivery_id".into(), json!(event.delivery_id));
    payload.insert("delivery_revision".into(), json!(event.delivery_revision));
    payload.insert(
        "conversation_sequence".into(),
        json!(event.conversation_sequence),
    );
    payload.insert("to_status".into(), json!(event.to_status.to_string()));
    if let Some(status) = event.from_status {
        payload.insert("from_status".into(), json!(status.to_string()));
    }
    payload.insert("lease_generation".into(), json!(event.lease_generation));
    payload.insert("target_kind".into(), json!(event.target.kind.to_string()));
    payload.insert("direction".into(), json!(event.direction.to_string()));
    if let Some(reply_to) = &event.reply_to {
        payload.insert("reply_to".into(), json!(reply_to));
    }
    NewEvent {
        event_id: event.event_id.clone(),
        owner: event.owner.clone(),
        category: EventCategory::Collaboration,
        event_type: format!("message.{}", event.kind),
        source: EventSource::Broker,
        trace_id: None,
        // Keep only an internal opaque correlation key. Caller-controlled
        // conversation, route, endpoint, trace, and receipt values stay out.
        correlation_id: Some(event.message_id.clone()),
        summary: None,
        payload,
    }
}

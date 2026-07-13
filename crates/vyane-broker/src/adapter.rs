use std::fmt;

use async_trait::async_trait;
use serde_json::Value;
use tokio::time::Instant;
use vyane_message::{
    EndpointRef, MessageDirection, MessageRecord, NewMessage, NewTransportReceipt,
};

/// How an adapter prevents a delivery from becoming a duplicate after an
/// uncertain result. The broker rejects [`ReplaySafety::Unsupported`] before
/// it claims work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySafety {
    /// Repeating the same stable delivery key returns the same remote effect.
    Idempotent,
    /// The adapter reconciles the stable key before it creates a new effect.
    Reconciles,
    /// The adapter has no safe crash-recovery story.
    Unsupported,
}

#[derive(Debug, Clone)]
pub struct AdapterContext {
    owner: String,
    deadline: Instant,
    transport_idempotency_key: String,
}

impl AdapterContext {
    pub(crate) fn new(owner: String, deadline: Instant, transport_idempotency_key: String) -> Self {
        Self {
            owner,
            deadline,
            transport_idempotency_key,
        }
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    #[must_use]
    pub fn transport_idempotency_key(&self) -> &str {
        &self.transport_idempotency_key
    }
}

/// Adapter-visible delivery data. It intentionally excludes the owner-bound
/// lease receipt and its token.
#[derive(Clone, PartialEq)]
pub struct DeliveryEnvelope {
    pub message_id: String,
    pub delivery_id: String,
    pub conversation_id: String,
    pub session_id: Option<String>,
    pub direction: MessageDirection,
    pub kind: String,
    pub sender: EndpointRef,
    pub body: String,
    pub payload: Value,
    pub reply_to: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub route: String,
    pub target: EndpointRef,
    pub attempt_count: u32,
}

impl DeliveryEnvelope {
    pub(crate) fn from_records(
        message: &MessageRecord,
        delivery: &vyane_message::DeliveryRecord,
    ) -> Self {
        Self {
            message_id: message.id.clone(),
            delivery_id: delivery.id.clone(),
            conversation_id: message.conversation_id.clone(),
            session_id: message.session_id.clone(),
            direction: message.direction,
            kind: message.kind.clone(),
            sender: message.sender.clone(),
            body: message.body.clone(),
            payload: message.payload.clone(),
            reply_to: message.reply_to.clone(),
            trace_id: message.trace_id.clone(),
            correlation_id: message.correlation_id.clone(),
            route: delivery.route.clone(),
            target: delivery.target.clone(),
            attempt_count: delivery.attempt_count,
        }
    }
}

impl fmt::Debug for DeliveryEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryEnvelope")
            .field("message_id", &self.message_id)
            .field("delivery_id", &self.delivery_id)
            .field("conversation_id", &self.conversation_id)
            .field("session_id", &self.session_id)
            .field("direction", &self.direction)
            .field("kind", &self.kind)
            .field("sender", &self.sender)
            .field("body", &"[REDACTED]")
            .field("body_bytes", &self.body.len())
            .field("payload", &"[REDACTED]")
            .field("reply_to", &self.reply_to)
            .field("trace_id", &self.trace_id)
            .field("correlation_id", &self.correlation_id)
            .field("route", &self.route)
            .field("target", &self.target)
            .field("attempt_count", &self.attempt_count)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub enum AdapterOutcome {
    /// No untracked durable effect was created outside the message store. A
    /// handler that creates a remote effect must use `TransportDelivered`.
    LocalHandled,
    /// Enqueue the reply and acknowledge the input in one SQLite transaction.
    Reply(Box<NewMessage>),
    /// A remote effect was observed and can now be recorded and acknowledged.
    TransportDelivered(NewTransportReceipt),
}

impl fmt::Debug for AdapterOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalHandled => formatter.write_str("LocalHandled"),
            Self::Reply(reply) => formatter.debug_tuple("Reply").field(reply).finish(),
            Self::TransportDelivered(receipt) => formatter
                .debug_tuple("TransportDelivered")
                .field(receipt)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterFailure {
    /// The adapter proved that no effect was created. A retry may safely use
    /// the same stable delivery key after the requested delay.
    Retry {
        reason_code: String,
        delay_seconds: u64,
    },
    /// The adapter proved that no effect was created and retry cannot help.
    Permanent { failure_code: String },
    /// The adapter cannot prove whether the effect happened. The broker leaves
    /// the lease untouched; a replay-safe adapter reconciles on the next claim.
    Uncertain { reason_code: String },
}

#[async_trait]
pub trait DeliveryAdapter: Send + Sync {
    /// Return a stable, non-secret adapter identity.
    ///
    /// Adapter methods must never include message bodies, payloads,
    /// credentials, or endpoint secrets in a panic payload. The broker catches
    /// unwinds to isolate lifecycle failure, but Rust invokes the process panic
    /// hook before the unwind can be caught.
    fn name(&self) -> &str;

    fn replay_safety(&self) -> ReplaySafety;

    /// Perform one bounded delivery attempt.
    ///
    /// Implementations must not detach an unowned side-effecting task. The
    /// future can be dropped on timeout or caller cancellation, so every
    /// effect must remain recoverable through the stable idempotency key or
    /// reconciliation contract declared by [`Self::replay_safety`].
    async fn deliver(
        &self,
        context: AdapterContext,
        delivery: DeliveryEnvelope,
    ) -> std::result::Result<AdapterOutcome, AdapterFailure>;
}

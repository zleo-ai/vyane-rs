use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::{MessageStoreError, Result};

pub const MAX_BODY_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_JSON_DEPTH: usize = 32;
pub const MAX_JSON_NODES: usize = 4_096;
pub const MAX_DELIVERIES: usize = 128;
pub const MAX_CLAIM_MAILBOXES: usize = 32;
pub const MAX_PAGE_SIZE: usize = 1_000;
pub const MAX_LEASE_SECONDS: i64 = 24 * 60 * 60;
pub const MAX_RETRY_SECONDS: u64 = 7 * 24 * 60 * 60;

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal,)+ }) => {
        impl $name {
            pub(crate) const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $value,)+ }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = MessageStoreError;

            fn from_str(value: &str) -> Result<Self> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(MessageStoreError::CorruptData(format!(
                        "unknown stored {} value",
                        stringify!($name)
                    ))),
                }
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageDirection {
    Ingress,
    Egress,
    Internal,
}

string_enum!(MessageDirection {
    Ingress => "ingress",
    Egress => "egress",
    Internal => "internal",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    User,
    Agent,
    Worker,
    Channel,
    Service,
    External,
}

string_enum!(EndpointKind {
    User => "user",
    Agent => "agent",
    Worker => "worker",
    Channel => "channel",
    Service => "service",
    External => "external",
});

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointRef {
    pub kind: EndpointKind,
    pub id: String,
}

impl EndpointRef {
    pub(crate) fn validate(&self, field: &str) -> Result<()> {
        validate_text(field, &self.id, 512)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeliveryMailbox {
    pub route: String,
    pub target: EndpointRef,
}

impl DeliveryMailbox {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("delivery mailbox route", &self.route, 256)?;
        self.target.validate("delivery mailbox target id")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    Leased,
    Delivered,
    Acknowledged,
    DeadLettered,
    Expired,
    Cancelled,
}

impl DeliveryStatus {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Acknowledged | Self::DeadLettered | Self::Expired | Self::Cancelled
        )
    }

    #[must_use]
    pub fn holds_lease(self) -> bool {
        matches!(self, Self::Leased | Self::Delivered)
    }
}

string_enum!(DeliveryStatus {
    Pending => "pending",
    Leased => "leased",
    Delivered => "delivered",
    Acknowledged => "acknowledged",
    DeadLettered => "dead_lettered",
    Expired => "expired",
    Cancelled => "cancelled",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageEventKind {
    Enqueued,
    Leased,
    LeaseRenewed,
    Delivered,
    Acknowledged,
    Nacked,
    Reclaimed,
    DeadLettered,
    Expired,
    Cancelled,
}

string_enum!(MessageEventKind {
    Enqueued => "enqueued",
    Leased => "leased",
    LeaseRenewed => "lease_renewed",
    Delivered => "delivered",
    Acknowledged => "acknowledged",
    Nacked => "nacked",
    Reclaimed => "reclaimed",
    DeadLettered => "dead_lettered",
    Expired => "expired",
    Cancelled => "cancelled",
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyKey {
    pub producer: String,
    pub key: String,
}

impl IdempotencyKey {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("idempotency producer", &self.producer, 256)?;
        validate_text("idempotency key", &self.key, 256)
    }
}

/// Canonical SHA-256 identity of one complete [`NewMessage`] request.
///
/// The digest covers the body, payload, routing, references, idempotency key,
/// and every requested delivery. It is safe to persist as reconciliation
/// metadata, but it is not an authorization capability or proof that a
/// message was committed.
#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct MessageRequestDigest(String);

impl MessageRequestDigest {
    /// Parse a previously persisted canonical request digest.
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.len() != 64 || !value.bytes().all(is_lower_hex_byte) {
            return Err(MessageStoreError::InvalidInput(
                "message request digest must be 64 lowercase hexadecimal characters".into(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagePublicationStatus {
    Staged,
    Published,
    Discarded,
}

string_enum!(MessagePublicationStatus {
    Staged => "staged",
    Published => "published",
    Discarded => "discarded",
});

impl fmt::Debug for MessageRequestDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("MessageRequestDigest")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for MessageRequestDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for MessageRequestDigest {
    fn deserialize<Deserializer>(
        deserializer: Deserializer,
    ) -> std::result::Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewDelivery {
    pub route: String,
    pub target: EndpointRef,
    pub available_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_attempts: u32,
}

impl NewDelivery {
    fn validate_structure(&self) -> Result<()> {
        validate_text("delivery route", &self.route, 256)?;
        self.target.validate("delivery target id")?;
        if !(1..=100).contains(&self.max_attempts) {
            return Err(MessageStoreError::InvalidInput(
                "delivery max_attempts must be between 1 and 100".into(),
            ));
        }
        let available_at = self.available_at.map(normalize_timestamp).transpose()?;
        let expires_at = self.expires_at.map(normalize_timestamp).transpose()?;
        if let (Some(available_at), Some(expires_at)) = (available_at, expires_at) {
            if expires_at <= available_at {
                return Err(MessageStoreError::InvalidInput(
                    "delivery expiry must be later than availability".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_temporal_admission(&self, now: DateTime<Utc>) -> Result<()> {
        if let Some(expires_at) = self.expires_at {
            let expires_at = normalize_timestamp(expires_at)?;
            if expires_at <= now {
                return Err(MessageStoreError::InvalidInput(
                    "delivery expiry must be later than enqueue time".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct NewMessage {
    pub conversation_id: String,
    pub session_id: Option<String>,
    pub direction: MessageDirection,
    pub kind: String,
    pub sender: EndpointRef,
    pub body: String,
    #[serde(default)]
    pub payload: Value,
    pub reply_to: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub idempotency: IdempotencyKey,
    pub deliveries: Vec<NewDelivery>,
}

impl fmt::Debug for NewMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewMessage")
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
            .field("idempotency", &self.idempotency)
            .field("deliveries", &self.deliveries)
            .finish()
    }
}

impl NewMessage {
    pub(crate) fn validate_structure(&self) -> Result<()> {
        validate_text("conversation id", &self.conversation_id, 256)?;
        validate_optional_text("session id", self.session_id.as_deref(), 256)?;
        validate_text("message kind", &self.kind, 128)?;
        self.sender.validate("sender id")?;
        if self.body.len() > MAX_BODY_BYTES || self.body.contains('\0') {
            return Err(MessageStoreError::InvalidInput(
                "message body is oversized or contains NUL".into(),
            ));
        }
        validate_payload(&self.payload)?;
        if self.body.is_empty() && self.payload.is_null() {
            return Err(MessageStoreError::InvalidInput(
                "message body and payload must not both be empty".into(),
            ));
        }
        validate_optional_text("reply_to", self.reply_to.as_deref(), 64)?;
        validate_optional_text("trace id", self.trace_id.as_deref(), 256)?;
        validate_optional_text("correlation id", self.correlation_id.as_deref(), 256)?;
        self.idempotency.validate()?;
        if self.deliveries.is_empty() || self.deliveries.len() > MAX_DELIVERIES {
            return Err(MessageStoreError::InvalidInput(format!(
                "message must contain between 1 and {MAX_DELIVERIES} deliveries"
            )));
        }
        let mut routes = BTreeSet::new();
        for delivery in &self.deliveries {
            delivery.validate_structure()?;
            let identity = (
                delivery.route.as_str(),
                delivery.target.kind.as_str(),
                delivery.target.id.as_str(),
            );
            if !routes.insert(identity) {
                return Err(MessageStoreError::InvalidInput(
                    "message contains a duplicate delivery route and target".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate(&self, now: DateTime<Utc>) -> Result<()> {
        self.validate_structure()?;
        for delivery in &self.deliveries {
            delivery.validate_temporal_admission(now)?;
        }
        Ok(())
    }

    /// Compute the canonical request digest used by the message store for
    /// idempotency conflict detection.
    ///
    /// Invalid message structure is rejected before hashing. Store-assigned
    /// identifiers and timestamps are intentionally not part of the digest.
    pub fn request_digest(&self) -> Result<MessageRequestDigest> {
        self.validate_structure()?;
        self.canonical_digest()
    }

    pub(crate) fn canonical_digest(&self) -> Result<MessageRequestDigest> {
        let mut value = serde_json::to_value(self).map_err(|_| {
            MessageStoreError::InvalidInput("message request cannot be serialized".into())
        })?;
        canonicalize_json(&mut value);
        let bytes = serde_json::to_vec(&value).map_err(|_| {
            MessageStoreError::InvalidInput("message request cannot be serialized".into())
        })?;
        Ok(MessageRequestDigest(hex_lower(&Sha256::digest(bytes))))
    }
}

/// Body-free proof that an exact idempotent message request is committed.
///
/// Resolution is owner-scoped and requires the caller's expected digest, so a
/// reused producer/key pair with different content fails closed rather than
/// being mistaken for the requested publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageIdempotencyResolution {
    pub owner: String,
    pub message_id: String,
    pub request_digest: MessageRequestDigest,
    pub status: MessagePublicationStatus,
    pub created_at: DateTime<Utc>,
}

/// Body-free result of staging or transitioning one exact publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagePublicationOutcome {
    pub resolution: MessageIdempotencyResolution,
    /// True when this exact request identity already existed before the call.
    ///
    /// `resolution.status` always reports its current publication state.
    pub existing: bool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub id: String,
    pub owner: String,
    pub conversation_id: String,
    pub conversation_sequence: u64,
    pub session_id: Option<String>,
    pub direction: MessageDirection,
    pub kind: String,
    pub sender: EndpointRef,
    pub body: String,
    pub payload: Value,
    pub reply_to: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub idempotency: IdempotencyKey,
    pub request_digest: String,
    pub created_at: DateTime<Utc>,
}

impl fmt::Debug for MessageRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageRecord")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("conversation_id", &self.conversation_id)
            .field("conversation_sequence", &self.conversation_sequence)
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
            .field("idempotency", &self.idempotency)
            .field("request_digest", &self.request_digest)
            .field("created_at", &self.created_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub id: String,
    pub owner: String,
    pub message_id: String,
    pub route: String,
    pub target: EndpointRef,
    pub status: DeliveryStatus,
    pub available_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub revision: u64,
    pub lease_generation: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub first_delivered_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub dead_lettered_at: Option<DateTime<Utc>>,
    pub failure_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl DeliveryRecord {
    /// Stable provider-side idempotency key for external sends.
    ///
    /// The key intentionally excludes lease generation: reclaiming a delivery
    /// must reconcile the same remote side effect instead of creating another.
    #[must_use]
    pub fn transport_idempotency_key(&self) -> String {
        format!("vyane:{}", self.id)
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageBundle {
    pub message: MessageRecord,
    pub deliveries: Vec<DeliveryRecord>,
}

impl fmt::Debug for MessageBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageBundle")
            .field("message", &self.message)
            .field("deliveries", &self.deliveries)
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EnqueueOutcome {
    pub bundle: MessageBundle,
    pub existing: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTransportReceipt {
    pub transport: String,
    pub account_scope: String,
    pub destination_scope: String,
    pub external_ids: Vec<String>,
}

impl fmt::Debug for NewTransportReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewTransportReceipt")
            .field("transport", &self.transport)
            .field("account_scope", &"[REDACTED]")
            .field("destination_scope", &"[REDACTED]")
            .field("external_id_count", &self.external_ids.len())
            .finish()
    }
}

impl NewTransportReceipt {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("transport receipt transport", &self.transport, 64)?;
        if !self.transport.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        }) {
            return Err(MessageStoreError::InvalidInput(
                "transport receipt transport must use lowercase ASCII identity characters".into(),
            ));
        }
        validate_text("transport receipt account scope", &self.account_scope, 512)?;
        validate_text(
            "transport receipt destination scope",
            &self.destination_scope,
            512,
        )?;
        if self.external_ids.is_empty() || self.external_ids.len() > MAX_DELIVERIES {
            return Err(MessageStoreError::InvalidInput(format!(
                "transport receipt must contain between 1 and {MAX_DELIVERIES} external ids"
            )));
        }
        let mut external_ids = HashSet::with_capacity(self.external_ids.len());
        for external_id in &self.external_ids {
            validate_text("transport receipt external id", external_id, 512)?;
            if !external_ids.insert(external_id) {
                return Err(MessageStoreError::InvalidInput(
                    "transport receipt contains a duplicate external id".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn canonical_digest(&self) -> Result<String> {
        self.validate()?;
        let mut hasher = Sha256::new();
        hasher.update(b"vyane-transport-receipt-v1\0");
        for value in [
            &self.transport,
            &self.account_scope,
            &self.destination_scope,
        ] {
            let length = u64::try_from(value.len()).map_err(|_| {
                MessageStoreError::InvalidInput("transport receipt field is too large".into())
            })?;
            hasher.update(length.to_be_bytes());
            hasher.update(value.as_bytes());
        }
        for external_id in &self.external_ids {
            let length = u64::try_from(external_id.len()).map_err(|_| {
                MessageStoreError::InvalidInput("transport receipt field is too large".into())
            })?;
            hasher.update(length.to_be_bytes());
            hasher.update(external_id.as_bytes());
        }
        Ok(hex_lower(&hasher.finalize()))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportReceiptRecord {
    pub owner: String,
    pub delivery_id: String,
    pub generation: u64,
    pub ordinal: u32,
    pub transport: String,
    pub account_scope: String,
    pub destination_scope: String,
    pub external_id: String,
    pub recorded_at: DateTime<Utc>,
}

impl fmt::Debug for TransportReceiptRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportReceiptRecord")
            .field("owner", &self.owner)
            .field("delivery_id", &self.delivery_id)
            .field("generation", &self.generation)
            .field("ordinal", &self.ordinal)
            .field("transport", &self.transport)
            .field("account_scope", &"[REDACTED]")
            .field("destination_scope", &"[REDACTED]")
            .field("external_id", &"[REDACTED]")
            .field("recorded_at", &self.recorded_at)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MarkTransportDeliveredOutcome {
    pub delivery: DeliveryRecord,
    pub receipts: Vec<TransportReceiptRecord>,
    pub existing: bool,
}

impl fmt::Debug for MarkTransportDeliveredOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarkTransportDeliveredOutcome")
            .field("delivery", &self.delivery)
            .field("receipts", &self.receipts)
            .field("existing", &self.existing)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct TransportReceiptResolution {
    pub receipt: TransportReceiptRecord,
    pub delivery: DeliveryRecord,
    pub message: MessageRecord,
}

impl fmt::Debug for TransportReceiptResolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportReceiptResolution")
            .field("receipt", &self.receipt)
            .field("delivery", &self.delivery)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Debug for EnqueueOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueueOutcome")
            .field("bundle", &self.bundle)
            .field("existing", &self.existing)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LeaseReceipt {
    pub delivery_id: String,
    pub generation: u64,
    pub mailbox: DeliveryMailbox,
    pub consumer: String,
    pub token: String,
}

impl fmt::Debug for LeaseReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseReceipt")
            .field("delivery_id", &self.delivery_id)
            .field("generation", &self.generation)
            .field("mailbox", &self.mailbox)
            .field("consumer", &self.consumer)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl LeaseReceipt {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("receipt delivery id", &self.delivery_id, 64)?;
        self.mailbox.validate()?;
        validate_text("receipt consumer", &self.consumer, 256)?;
        if self.generation == 0
            || self.token.len() != 64
            || !self
                .token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(MessageStoreError::InvalidReceipt {
                delivery_id: self.delivery_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimQuery {
    pub mailboxes: Vec<DeliveryMailbox>,
    pub limit: usize,
}

impl ClaimQuery {
    /// Validate a claim without opening a store or mutating delivery state.
    ///
    /// Resident supervisors use this before they admit a polling lane, so a
    /// malformed mailbox set can never reach the transactional claim path.
    pub fn validate(&self) -> Result<()> {
        if self.mailboxes.is_empty() || self.mailboxes.len() > MAX_CLAIM_MAILBOXES {
            return Err(MessageStoreError::InvalidInput(format!(
                "claim must contain between 1 and {MAX_CLAIM_MAILBOXES} mailboxes"
            )));
        }
        let mut seen = HashSet::with_capacity(self.mailboxes.len());
        for mailbox in &self.mailboxes {
            mailbox.validate()?;
            if !seen.insert(mailbox) {
                return Err(MessageStoreError::InvalidInput(
                    "claim contains a duplicate mailbox".into(),
                ));
            }
        }
        validate_limit(self.limit, "claim limit")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRequest {
    pub consumer: String,
    pub lease_seconds: u64,
}

impl LeaseRequest {
    /// Validate lease identity and duration without touching persistent state.
    pub fn validate(&self) -> Result<()> {
        validate_text("lease consumer", &self.consumer, 256)?;
        if self.lease_seconds == 0
            || self.lease_seconds > u64::try_from(MAX_LEASE_SECONDS).unwrap_or(u64::MAX)
        {
            return Err(MessageStoreError::InvalidInput(
                "lease duration must be between 1 second and 24 hours".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq)]
pub struct LeasedDelivery {
    pub message: MessageRecord,
    pub delivery: DeliveryRecord,
    pub receipt: LeaseReceipt,
}

impl fmt::Debug for LeasedDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeasedDelivery")
            .field("message", &self.message)
            .field("delivery", &self.delivery)
            .field("receipt", &self.receipt)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NackDisposition {
    RetryAfter { delay_seconds: u64 },
    Permanent { failure_code: String },
}

impl NackDisposition {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::RetryAfter { delay_seconds } => {
                if *delay_seconds > MAX_RETRY_SECONDS {
                    return Err(MessageStoreError::InvalidInput(
                        "nack retry delay exceeds seven days".into(),
                    ));
                }
                Ok(())
            }
            Self::Permanent { failure_code } => validate_text("failure code", failure_code, 128),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageCursor {
    pub conversation_id: String,
    pub sequence: u64,
    pub id: String,
}

impl MessageCursor {
    pub(crate) fn validate_for(&self, conversation_id: &str) -> Result<()> {
        if self.conversation_id != conversation_id {
            return Err(MessageStoreError::InvalidInput(
                "message cursor belongs to a different conversation".into(),
            ));
        }
        validate_text("cursor message id", &self.id, 64)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessagePage {
    pub items: Vec<MessageBundle>,
    pub next_cursor: Option<MessageCursor>,
}

/// Bounded filters for one exact delivery mailbox.
///
/// Terminal failures and cancelled or expired deliveries are never returned.
/// Acknowledged deliveries and not-yet-due deliveries are opt-in so the
/// ordinary inbox remains an actionable view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxQuery {
    pub include_acknowledged: bool,
    pub include_future: bool,
    pub limit: usize,
}

impl Default for MailboxQuery {
    fn default() -> Self {
        Self {
            include_acknowledged: false,
            include_future: false,
            limit: 100,
        }
    }
}

impl MailboxQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_limit(self.limit, "mailbox page limit")
    }
}

/// One immutable message paired with its mailbox-specific delivery state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MailboxMessage {
    pub message: MessageRecord,
    pub delivery: DeliveryRecord,
}

/// A stable, bounded mailbox page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MailboxPage {
    pub items: Vec<MailboxMessage>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEvent {
    pub sequence: u64,
    pub event_id: String,
    pub owner: String,
    pub message_id: String,
    pub delivery_id: String,
    pub delivery_revision: u64,
    pub conversation_id: String,
    pub conversation_sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub kind: MessageEventKind,
    pub from_status: Option<DeliveryStatus>,
    pub to_status: DeliveryStatus,
    pub lease_generation: u64,
    pub route: String,
    pub target: EndpointRef,
    pub direction: MessageDirection,
    pub reply_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxPage {
    pub items: Vec<MessageEvent>,
    pub has_more: bool,
}

#[derive(Clone, PartialEq)]
pub struct ReplyAndAckOutcome {
    pub reply: EnqueueOutcome,
    pub acknowledged: DeliveryRecord,
}

impl fmt::Debug for ReplyAndAckOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyAndAckOutcome")
            .field("reply", &self.reply)
            .field("acknowledged", &self.acknowledged)
            .finish()
    }
}

pub(crate) fn validate_owner(owner: &str) -> Result<()> {
    validate_text("owner", owner, 256)
}

pub(crate) fn validate_limit(limit: usize, field: &str) -> Result<()> {
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(MessageStoreError::InvalidInput(format!(
            "{field} must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_text(field: &str, value: &str, max: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        return Err(MessageStoreError::InvalidInput(format!(
            "{field} is empty, oversized, or contains NUL"
        )));
    }
    Ok(())
}

pub(crate) fn validate_optional_text(field: &str, value: Option<&str>, max: usize) -> Result<()> {
    value.map_or(Ok(()), |value| validate_text(field, value, max))
}

pub(crate) fn normalize_timestamp(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis()).ok_or_else(|| {
        MessageStoreError::InvalidInput("timestamp is outside SQLite millisecond range".into())
    })
}

pub(crate) fn validate_payload(payload: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(payload).map_err(|_| {
        MessageStoreError::InvalidInput("message payload cannot be serialized".into())
    })?;
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(MessageStoreError::InvalidInput(
            "message payload exceeds its byte limit".into(),
        ));
    }
    let mut nodes = 0usize;
    validate_json_shape(payload, 1, &mut nodes)
}

fn validate_json_shape(value: &Value, depth: usize, nodes: &mut usize) -> Result<()> {
    *nodes = nodes.saturating_add(1);
    if depth > MAX_JSON_DEPTH || *nodes > MAX_JSON_NODES {
        return Err(MessageStoreError::InvalidInput(
            "message payload exceeds its depth or node limit".into(),
        ));
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_json_shape(value, depth + 1, nodes)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if key.len() > 256 || key.contains('\0') {
                    return Err(MessageStoreError::InvalidInput(
                        "message payload contains an invalid object key".into(),
                    ));
                }
                validate_json_shape(value, depth + 1, nodes)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn canonicalize_json(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        Value::Object(values) => {
            let mut ordered = BTreeMap::new();
            for (key, mut value) in std::mem::take(values) {
                canonicalize_json(&mut value);
                ordered.insert(key, value);
            }
            values.extend(ordered);
        }
        _ => {}
    }
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn is_lower_hex_byte(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

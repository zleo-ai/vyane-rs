use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt as _, StreamExt as _, stream};
use tokio::time::{Instant, timeout};
use vyane_message::{
    ClaimQuery, DeliveryMailbox, EnqueueOutcome, IdempotencyKey, LeaseRequest,
    MessageIdempotencyResolution, MessagePublicationOutcome, MessageRequestDigest, MessageStore,
    NackDisposition, NewMessage,
};

use crate::{
    AdapterContext, AdapterFailure, AdapterOutcome, BrokerError, DeliveryAdapter, DeliveryEnvelope,
    ReplaySafety, Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerScope {
    owner: String,
}

impl BrokerScope {
    pub fn new(owner: impl Into<String>) -> Result<Self> {
        let owner = owner.into();
        if owner.is_empty()
            || owner.len() > 256
            || owner.contains('\0')
            || owner.trim() != owner
            || owner.chars().any(char::is_control)
        {
            return Err(BrokerError::InvalidConfig(
                "owner must contain between 1 and 256 canonical non-control bytes".into(),
            ));
        }
        Ok(Self { owner })
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }
}

#[derive(Debug, Clone)]
pub struct PumpOptions {
    pub adapter_timeout: Duration,
    pub settlement_margin: Duration,
    pub max_in_flight: usize,
}

impl Default for PumpOptions {
    fn default() -> Self {
        Self {
            adapter_timeout: Duration::from_secs(20),
            settlement_margin: Duration::from_secs(5),
            max_in_flight: 8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PumpItemStatus {
    Acknowledged,
    ReplyEnqueued {
        message_id: String,
    },
    RetryScheduled,
    DeadLettered,
    /// The actual lease was shorter than the bounded adapter window, so the
    /// adapter was not called and no external effect was attempted.
    InsufficientLeaseWindow,
    Uncertain,
    TimedOut,
    AdapterPanicked,
    SettlementFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpItemResult {
    pub delivery_id: String,
    pub status: PumpItemStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpReport {
    pub claimed: usize,
    pub items: Vec<PumpItemResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub expired: usize,
    pub reclaimed: usize,
}

/// Owner-bound facade over the synchronous transactional message store.
#[derive(Clone)]
pub struct MessageBroker {
    scope: BrokerScope,
    store: Arc<dyn MessageStore>,
}

impl MessageBroker {
    #[must_use]
    pub fn new(scope: BrokerScope, store: Arc<dyn MessageStore>) -> Self {
        Self { scope, store }
    }

    #[must_use]
    pub fn scope(&self) -> &BrokerScope {
        &self.scope
    }

    pub(crate) fn store(&self) -> &Arc<dyn MessageStore> {
        &self.store
    }

    /// Persist an idempotent message request. If this future is cancelled, the
    /// blocking commit may still complete; retry the same producer/key pair.
    pub async fn publish(&self, message: NewMessage) -> Result<EnqueueOutcome> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        Ok(tokio::task::spawn_blocking(move || store.enqueue(&owner, &message)).await??)
    }

    /// Persist a message behind the store publication gate. No ordinary read,
    /// claim, event, or outbox surface can observe its content until release.
    pub async fn stage(&self, message: NewMessage) -> Result<MessagePublicationOutcome> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        Ok(tokio::task::spawn_blocking(move || store.stage(&owner, &message)).await??)
    }

    /// Atomically make an exact staged request visible and emit its initial
    /// enqueue events. Replaying the same terminal transition is idempotent.
    pub async fn publish_staged(
        &self,
        idempotency: IdempotencyKey,
        expected_digest: MessageRequestDigest,
    ) -> Result<Option<MessagePublicationOutcome>> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        Ok(tokio::task::spawn_blocking(move || {
            store.publish_staged(&owner, &idempotency, &expected_digest)
        })
        .await??)
    }

    /// Atomically discard an exact staged request without exposing its body or
    /// creating enqueue events. Replaying the discard is idempotent.
    pub async fn discard_staged(
        &self,
        idempotency: IdempotencyKey,
        expected_digest: MessageRequestDigest,
    ) -> Result<Option<MessagePublicationOutcome>> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        Ok(tokio::task::spawn_blocking(move || {
            store.discard_staged(&owner, &idempotency, &expected_digest)
        })
        .await??)
    }

    /// Resolve an exact published request without returning its body, payload,
    /// or delivery destinations. Hidden publication states appear absent.
    pub async fn resolve_idempotency(
        &self,
        idempotency: IdempotencyKey,
        expected_digest: MessageRequestDigest,
    ) -> Result<Option<MessageIdempotencyResolution>> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        Ok(tokio::task::spawn_blocking(move || {
            store.resolve_idempotency(&owner, &idempotency, &expected_digest)
        })
        .await??)
    }

    /// Staged-aware body-free resolution for recovery/control-plane callers.
    pub async fn resolve_publication(
        &self,
        idempotency: IdempotencyKey,
        expected_digest: MessageRequestDigest,
    ) -> Result<Option<MessageIdempotencyResolution>> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        Ok(tokio::task::spawn_blocking(move || {
            store.resolve_publication(&owner, &idempotency, &expected_digest)
        })
        .await??)
    }

    /// Claim at most `min(query.limit, max_in_flight)` rows and settle them.
    /// No resident loop or unbounded channel is hidden inside this call.
    /// Results are completion-ordered. Dropping this future cancels adapter
    /// futures, while already-started blocking settlements may still finish.
    pub async fn pump_once(
        &self,
        mut query: ClaimQuery,
        lease: LeaseRequest,
        adapter: Arc<dyn DeliveryAdapter>,
        options: PumpOptions,
    ) -> Result<PumpReport> {
        let adapter_name = validate_adapter(adapter.as_ref())?;
        let required_window = validate_pump_options(&lease, &options)?;
        query.limit = query.limit.min(options.max_in_flight);

        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        let claimed =
            tokio::task::spawn_blocking(move || store.claim(&owner, &query, &lease)).await??;
        let claimed_count = claimed.len();
        let owner = self.scope.owner.clone();
        let store = Arc::clone(&self.store);
        let adapter_timeout = options.adapter_timeout;
        let max_in_flight = options.max_in_flight;

        let items = stream::iter(claimed.into_iter().map(|leased| {
            let owner = owner.clone();
            let store = Arc::clone(&store);
            let adapter = Arc::clone(&adapter);
            let adapter_name = adapter_name.clone();
            async move {
                process_one(
                    owner,
                    store,
                    adapter,
                    adapter_name,
                    leased,
                    adapter_timeout,
                    required_window,
                )
                .await
            }
        }))
        .buffer_unordered(max_in_flight)
        .collect::<Vec<_>>()
        .await;

        Ok(PumpReport {
            claimed: claimed_count,
            items,
        })
    }

    /// Expire TTL-bound rows, then reclaim leases whose workers disappeared.
    pub async fn maintenance_once(&self, limit: usize) -> Result<MaintenanceReport> {
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        let expired =
            tokio::task::spawn_blocking(move || store.expire_due(&owner, limit)).await??;
        let store = Arc::clone(&self.store);
        let owner = self.scope.owner.clone();
        let reclaimed =
            tokio::task::spawn_blocking(move || store.reclaim_expired(&owner, limit)).await??;
        Ok(MaintenanceReport { expired, reclaimed })
    }
}

pub(crate) fn validate_adapter(adapter: &dyn DeliveryAdapter) -> Result<String> {
    let name = adapter.name();
    if name.is_empty()
        || name.len() > 64
        || !name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(BrokerError::InvalidConfig(
            "adapter name must use lowercase ASCII identity characters".into(),
        ));
    }
    if adapter.replay_safety() == ReplaySafety::Unsupported {
        return Err(BrokerError::UnsafeAdapter {
            adapter: name.to_string(),
        });
    }
    Ok(name.to_string())
}

pub(crate) fn validate_pump_options(
    lease: &LeaseRequest,
    options: &PumpOptions,
) -> Result<Duration> {
    if options.max_in_flight == 0 {
        return Err(BrokerError::InvalidConfig(
            "max_in_flight must be greater than zero".into(),
        ));
    }
    if options.adapter_timeout.is_zero() {
        return Err(BrokerError::InvalidConfig(
            "adapter timeout must be greater than zero".into(),
        ));
    }
    let lease_duration = Duration::from_secs(lease.lease_seconds);
    let required = options
        .adapter_timeout
        .checked_add(options.settlement_margin)
        .ok_or_else(|| BrokerError::InvalidConfig("adapter timeout overflow".into()))?;
    if required >= lease_duration {
        return Err(BrokerError::InvalidConfig(
            "adapter timeout plus settlement margin must be shorter than the lease".into(),
        ));
    }
    Ok(required)
}

async fn process_one(
    owner: String,
    store: Arc<dyn MessageStore>,
    adapter: Arc<dyn DeliveryAdapter>,
    adapter_name: String,
    leased: vyane_message::LeasedDelivery,
    adapter_timeout: Duration,
    required_window: Duration,
) -> PumpItemResult {
    let delivery_id = leased.delivery.id.clone();
    let has_window = leased
        .delivery
        .lease_expires_at
        .and_then(|expires_at| {
            expires_at
                .signed_duration_since(chrono::Utc::now())
                .to_std()
                .ok()
        })
        .is_some_and(|remaining| remaining > required_window);
    if !has_window {
        return PumpItemResult {
            delivery_id,
            status: PumpItemStatus::InsufficientLeaseWindow,
        };
    }
    let context = AdapterContext::new(
        owner.clone(),
        Instant::now() + adapter_timeout,
        leased.delivery.transport_idempotency_key(),
    );
    let envelope = DeliveryEnvelope::from_records(&leased.message, &leased.delivery);
    let delivered = AssertUnwindSafe(adapter.deliver(context, envelope)).catch_unwind();
    let status = match timeout(adapter_timeout, delivered).await {
        Err(_) => PumpItemStatus::TimedOut,
        Ok(Err(_)) => PumpItemStatus::AdapterPanicked,
        Ok(Ok(Err(AdapterFailure::Uncertain { .. }))) => PumpItemStatus::Uncertain,
        Ok(Ok(Err(AdapterFailure::Retry { delay_seconds, .. }))) => {
            settle_nack(
                store,
                owner,
                leased.receipt.mailbox.clone(),
                leased.receipt,
                NackDisposition::RetryAfter { delay_seconds },
            )
            .await
        }
        Ok(Ok(Err(AdapterFailure::Permanent { failure_code }))) => {
            settle_nack(
                store,
                owner,
                leased.receipt.mailbox.clone(),
                leased.receipt,
                NackDisposition::Permanent { failure_code },
            )
            .await
        }
        Ok(Ok(Ok(outcome))) => settle_success(owner, store, &adapter_name, leased, outcome).await,
    };
    PumpItemResult {
        delivery_id,
        status,
    }
}

async fn settle_nack(
    store: Arc<dyn MessageStore>,
    owner: String,
    mailbox: DeliveryMailbox,
    receipt: vyane_message::LeaseReceipt,
    disposition: NackDisposition,
) -> PumpItemStatus {
    match tokio::task::spawn_blocking(move || store.nack(&owner, &mailbox, &receipt, &disposition))
        .await
    {
        Ok(Ok(delivery)) => match delivery.status {
            vyane_message::DeliveryStatus::Pending => PumpItemStatus::RetryScheduled,
            vyane_message::DeliveryStatus::DeadLettered => PumpItemStatus::DeadLettered,
            _ => PumpItemStatus::SettlementFailed,
        },
        Ok(Err(_)) | Err(_) => PumpItemStatus::SettlementFailed,
    }
}

async fn settle_success(
    owner: String,
    store: Arc<dyn MessageStore>,
    adapter_name: &str,
    leased: vyane_message::LeasedDelivery,
    outcome: AdapterOutcome,
) -> PumpItemStatus {
    let mailbox = leased.receipt.mailbox.clone();
    match outcome {
        AdapterOutcome::Reply(reply) => {
            let marked_store = Arc::clone(&store);
            let marked_owner = owner.clone();
            let marked_mailbox = mailbox.clone();
            let marked_receipt = leased.receipt.clone();
            let marked = tokio::task::spawn_blocking(move || {
                marked_store.mark_delivered(&marked_owner, &marked_mailbox, &marked_receipt)
            })
            .await;
            if !matches!(marked, Ok(Ok(_))) {
                return PumpItemStatus::SettlementFailed;
            }
            let call = tokio::task::spawn_blocking(move || {
                store.reply_and_ack(&owner, &mailbox, &leased.receipt, &reply)
            })
            .await;
            match call {
                Ok(Ok(outcome)) => PumpItemStatus::ReplyEnqueued {
                    message_id: outcome.reply.bundle.message.id,
                },
                Ok(Err(_)) | Err(_) => PumpItemStatus::SettlementFailed,
            }
        }
        AdapterOutcome::LocalHandled => {
            settle_delivered_then_ack(owner, store, mailbox, leased.receipt).await
        }
        AdapterOutcome::TransportDelivered(transport_receipt) => {
            if transport_receipt.transport != adapter_name {
                return PumpItemStatus::SettlementFailed;
            }
            let receipt = leased.receipt;
            let delivered_store = Arc::clone(&store);
            let delivered_owner = owner.clone();
            let delivered_mailbox = mailbox.clone();
            let delivered_receipt = receipt.clone();
            let marked = tokio::task::spawn_blocking(move || {
                delivered_store.mark_transport_delivered(
                    &delivered_owner,
                    &delivered_mailbox,
                    &delivered_receipt,
                    &transport_receipt,
                )
            })
            .await;
            match marked {
                Ok(Ok(_)) => acknowledge(owner, store, mailbox, receipt).await,
                Ok(Err(_)) | Err(_) => PumpItemStatus::SettlementFailed,
            }
        }
    }
}

async fn settle_delivered_then_ack(
    owner: String,
    store: Arc<dyn MessageStore>,
    mailbox: DeliveryMailbox,
    receipt: vyane_message::LeaseReceipt,
) -> PumpItemStatus {
    let delivered_store = Arc::clone(&store);
    let delivered_owner = owner.clone();
    let delivered_mailbox = mailbox.clone();
    let delivered_receipt = receipt.clone();
    let marked = tokio::task::spawn_blocking(move || {
        delivered_store.mark_delivered(&delivered_owner, &delivered_mailbox, &delivered_receipt)
    })
    .await;
    match marked {
        Ok(Ok(_)) => acknowledge(owner, store, mailbox, receipt).await,
        Ok(Err(_)) | Err(_) => PumpItemStatus::SettlementFailed,
    }
}

async fn acknowledge(
    owner: String,
    store: Arc<dyn MessageStore>,
    mailbox: DeliveryMailbox,
    receipt: vyane_message::LeaseReceipt,
) -> PumpItemStatus {
    match tokio::task::spawn_blocking(move || store.acknowledge(&owner, &mailbox, &receipt)).await {
        Ok(Ok(_)) => PumpItemStatus::Acknowledged,
        Ok(Err(_)) | Err(_) => PumpItemStatus::SettlementFailed,
    }
}

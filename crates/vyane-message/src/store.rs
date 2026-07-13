use crate::{
    ClaimQuery, DeliveryMailbox, DeliveryRecord, EnqueueOutcome, IdempotencyKey, LeaseReceipt,
    LeaseRequest, LeasedDelivery, MarkTransportDeliveredOutcome, MessageBundle, MessageCursor,
    MessageEvent, MessageIdempotencyResolution, MessagePage, MessagePublicationOutcome,
    MessageRequestDigest, NackDisposition, NewMessage, NewTransportReceipt, OutboxPage,
    ReplyAndAckOutcome, Result, TransportReceiptRecord, TransportReceiptResolution,
};

/// Synchronous transactional message source of truth.
///
/// `owner` is explicit authority on every operation. Implementations must make
/// an unauthorized identifier indistinguishable from an absent one. Runtime
/// services should call this synchronous trait from a blocking worker pool.
pub trait MessageStore: Send + Sync {
    fn enqueue(&self, owner: &str, message: &NewMessage) -> Result<EnqueueOutcome>;

    /// Persist an exact request behind the publication gate. Staged content is
    /// absent from every ordinary read, claim, event, and outbox surface.
    fn stage(&self, owner: &str, message: &NewMessage) -> Result<MessagePublicationOutcome>;

    /// Atomically expose a staged request and create its initial delivery
    /// events. An absent key returns `None`; digest drift fails closed.
    fn publish_staged(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessagePublicationOutcome>>;

    /// Permanently close a staged request without exposing its content.
    fn discard_staged(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessagePublicationOutcome>>;

    fn get(&self, owner: &str, message_id: &str) -> Result<Option<MessageBundle>>;

    /// Resolve an exact published idempotent request without returning its
    /// body, payload, or delivery destinations. Staged and discarded rows are
    /// indistinguishable from absent rows on this ordinary surface.
    ///
    /// An existing producer/key pair with a different digest returns
    /// `IdempotencyConflict`. Another owner's row is indistinguishable from an
    /// absent row.
    fn resolve_idempotency(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessageIdempotencyResolution>>;

    /// Control-plane resolution of an exact request in any publication state.
    /// This is the staged-aware recovery surface; ordinary callers should use
    /// [`Self::resolve_idempotency`], which exposes only published truth.
    fn resolve_publication(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessageIdempotencyResolution>>;

    fn list_conversation(
        &self,
        owner: &str,
        conversation_id: &str,
        cursor: Option<&MessageCursor>,
        limit: usize,
    ) -> Result<MessagePage>;

    fn events(&self, owner: &str, message_id: &str) -> Result<Vec<MessageEvent>>;

    fn claim(
        &self,
        owner: &str,
        query: &ClaimQuery,
        lease: &LeaseRequest,
    ) -> Result<Vec<LeasedDelivery>>;

    fn renew(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        operation_id: &str,
        lease_seconds: u64,
    ) -> Result<DeliveryRecord>;

    fn mark_delivered(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
    ) -> Result<DeliveryRecord>;

    /// Records an already-observed external side effect with the delivery
    /// transition in one local transaction.
    ///
    /// This is not a distributed exactly-once primitive. Adapters must send
    /// with [`DeliveryRecord::transport_idempotency_key`] (or an equivalent
    /// provider reconciliation key), then record every returned external id
    /// as one immutable batch. Providers without idempotency/reconciliation
    /// remain at-least-once across a crash between remote success and this call.
    fn mark_transport_delivered(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        transport_receipt: &NewTransportReceipt,
    ) -> Result<MarkTransportDeliveredOutcome>;

    fn transport_receipts(
        &self,
        owner: &str,
        delivery_id: &str,
    ) -> Result<Vec<TransportReceiptRecord>>;

    fn resolve_transport_receipt(
        &self,
        owner: &str,
        transport: &str,
        account_scope: &str,
        destination_scope: &str,
        external_id: &str,
    ) -> Result<Option<TransportReceiptResolution>>;

    fn acknowledge(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
    ) -> Result<DeliveryRecord>;

    fn nack(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        disposition: &NackDisposition,
    ) -> Result<DeliveryRecord>;

    fn reclaim_expired(&self, owner: &str, limit: usize) -> Result<usize>;

    fn expire_due(&self, owner: &str, limit: usize) -> Result<usize>;

    fn cancel(&self, owner: &str, delivery_id: &str) -> Result<DeliveryRecord>;

    fn reply_and_ack(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        reply: &NewMessage,
    ) -> Result<ReplyAndAckOutcome>;

    fn unprojected_events(&self, owner: &str, projector: &str, limit: usize) -> Result<OutboxPage>;

    fn mark_projected(&self, owner: &str, projector: &str, event_id: &str) -> Result<()>;
}

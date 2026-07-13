//! Owner-safe transactional message persistence for Vyane.
//!
//! Immutable message content and mutable delivery state have separate rows.
//! Every delivery transition and its body-free projection outbox event commit
//! in the same SQLite transaction. EventLog and external brokers are downstream
//! projections, never the source of truth.
//!
//! External transport receipts prove effects that have already been observed;
//! they cannot make a remote API call and SQLite commit atomic. Adapters must
//! use the stable delivery idempotency key and reconcile uncertain sends.

mod error;
mod model;
mod sqlite;
mod store;

pub use error::{MessageStoreError, Result};
pub use model::{
    ClaimQuery, DeliveryMailbox, DeliveryRecord, DeliveryStatus, EndpointKind, EndpointRef,
    EnqueueOutcome, IdempotencyKey, LeaseReceipt, LeaseRequest, LeasedDelivery, MailboxMessage,
    MailboxPage, MailboxQuery, MarkTransportDeliveredOutcome, MessageBundle, MessageCursor,
    MessageDirection, MessageEvent, MessageEventKind, MessageIdempotencyResolution, MessagePage,
    MessagePublicationOutcome, MessagePublicationStatus, MessageRecord, MessageRequestDigest,
    NackDisposition, NewDelivery, NewMessage, NewTransportReceipt, OutboxPage, ReplyAndAckOutcome,
    TransportReceiptRecord, TransportReceiptResolution,
};
pub use sqlite::{MessageClock, SCHEMA_VERSION, SqliteMessageStore, SystemMessageClock};
pub use store::MessageStore;

use std::fs::File;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use fs4::fs_std::FileExt as _;
use rusqlite::limits::Limit;
use rusqlite::types::{Type, Value};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension as _, Row, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::model::{
    hex_lower, normalize_timestamp, validate_limit, validate_optional_text, validate_owner,
    validate_payload, validate_text,
};
use crate::{
    ClaimQuery, DeliveryMailbox, DeliveryRecord, DeliveryStatus, EndpointKind, EndpointRef,
    EnqueueOutcome, IdempotencyKey, LeaseReceipt, LeaseRequest, LeasedDelivery, MailboxMessage,
    MailboxPage, MailboxQuery, MarkTransportDeliveredOutcome, MessageBundle, MessageCursor,
    MessageEvent, MessageEventKind, MessageIdempotencyResolution, MessagePage,
    MessagePublicationOutcome, MessagePublicationStatus, MessageRecord, MessageRequestDigest,
    MessageStore, MessageStoreError, NackDisposition, NewMessage, NewTransportReceipt, OutboxPage,
    ReplyAndAckOutcome, Result, TransportReceiptRecord, TransportReceiptResolution,
};

pub const SCHEMA_VERSION: u32 = 2;
const RECORD_SCHEMA: u32 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const DATABASE_CREATE_ATTEMPTS: usize = 128;
#[cfg(unix)]
const DATABASE_CREATE_PREFIX: &str = ".vyane-message-db-create";
#[cfg(unix)]
static DATABASE_CREATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const SQLITE_VALUE_LIMIT: i32 = 512 * 1024;
const MIGRATION_0001: &str = include_str!("../migrations/0001_messages.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_publication_gate.sql");

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaObject {
    kind: String,
    name: String,
    table_name: String,
    sql: String,
}

static EXPECTED_SCHEMA: OnceLock<std::result::Result<Vec<SchemaObject>, String>> = OnceLock::new();

const MESSAGE_COLUMNS: &str = "\
    id, owner, conversation_id, conversation_sequence, session_id, direction, kind, \
    sender_kind, sender_id, body, payload_json, reply_to, trace_id, correlation_id, \
    producer, idempotency_key, request_digest, created_at_ms, record_schema";
const PUBLIC_MESSAGE_COLUMNS: &str = "\
    m.id, m.owner, m.conversation_id, p.conversation_sequence, m.session_id, m.direction, m.kind, \
    m.sender_kind, m.sender_id, m.body, m.payload_json, m.reply_to, m.trace_id, m.correlation_id, \
    m.producer, m.idempotency_key, m.request_digest, m.created_at_ms, m.record_schema";
const DELIVERY_COLUMNS: &str = "\
    id, owner, message_id, route, target_kind, target_id, status, available_at_ms, \
    expires_at_ms, attempt_count, max_attempts, revision, lease_generation, lease_owner, \
    lease_expires_at_ms, first_delivered_at_ms, acknowledged_at_ms, dead_lettered_at_ms, \
    failure_code, created_at_ms, updated_at_ms, record_schema";
const EVENT_COLUMNS: &str = "\
    sequence, event_id, owner, message_id, delivery_id, delivery_revision, conversation_id, \
    conversation_sequence, occurred_at_ms, event_type, from_status, to_status, \
    lease_generation, route, target_kind, target_id, direction, reply_to";
const TRANSPORT_RECEIPT_COLUMNS: &str = "\
    owner, delivery_id, generation, ordinal, transport, account_scope, destination_scope, \
    external_id, recorded_at_ms, receipt_digest, record_schema";

/// Store-owned time source. Production callers cannot supply lifecycle time.
pub trait MessageClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemMessageClock;

impl MessageClock for SystemMessageClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct SqliteMessageStore {
    path: PathBuf,
    clock: Arc<dyn MessageClock>,
}

struct WriteTransaction<'connection> {
    transaction: Transaction<'connection>,
    lock: File,
}

impl<'connection> Deref for WriteTransaction<'connection> {
    type Target = Transaction<'connection>;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl WriteTransaction<'_> {
    fn commit(self) -> Result<()> {
        let Self { transaction, lock } = self;
        let commit = transaction.commit().map_err(MessageStoreError::from);
        drop(lock);
        commit
    }
}

impl std::fmt::Debug for SqliteMessageStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteMessageStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl SqliteMessageStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_clock(path, Arc::new(SystemMessageClock))
    }

    pub fn open_with_clock(path: impl Into<PathBuf>, clock: Arc<dyn MessageClock>) -> Result<Self> {
        let store = Self {
            path: path.into(),
            clock,
        };
        store.initialize()?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn now(&self) -> Result<DateTime<Utc>> {
        normalize_timestamp(self.clock.now())
    }

    fn initialize(&self) -> Result<()> {
        prepare_database_path(&self.path)?;
        let mut connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        let found = user_version(&connection)?;
        if found > SCHEMA_VERSION {
            return Err(MessageStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        connection.pragma_update(None, "journal_mode", "WAL")?;
        configure_connection(&connection)?;
        validate_database_files(&self.path)?;
        let transaction = self.begin_locked_transaction(&mut connection)?;
        let found = user_version(&transaction)?;
        if found > SCHEMA_VERSION {
            return Err(MessageStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        if found == 0 {
            transaction.execute_batch(MIGRATION_0001)?;
            transaction.execute_batch(MIGRATION_0002)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        } else if found == 1 {
            transaction.execute_batch(MIGRATION_0002)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        validate_schema_definition(&transaction)?;
        audit_database_integrity(&transaction)?;
        transaction.commit()?;
        validate_database_files(&self.path)
    }

    fn connection(&self) -> Result<Connection> {
        let connection = open_database(&self.path)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        configure_connection(&connection)?;
        let found = user_version(&connection)?;
        if found != SCHEMA_VERSION {
            return Err(MessageStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        validate_schema_definition(&connection)?;
        validate_database_files(&self.path)?;
        Ok(connection)
    }

    fn begin_locked_transaction<'connection>(
        &self,
        connection: &'connection mut Connection,
    ) -> Result<WriteTransaction<'connection>> {
        let lock = acquire_write_lock(&self.path)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Ok(WriteTransaction { transaction, lock })
    }

    fn write_transaction<'connection>(
        &self,
        connection: &'connection mut Connection,
    ) -> Result<WriteTransaction<'connection>> {
        let transaction = self.begin_locked_transaction(connection)?;
        let found = user_version(&transaction)?;
        if found != SCHEMA_VERSION {
            return Err(MessageStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        validate_schema_definition(&transaction)?;
        Ok(transaction)
    }

    fn transition_staged(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
        target: MessagePublicationStatus,
    ) -> Result<Option<MessagePublicationOutcome>> {
        validate_owner(owner)?;
        idempotency.validate()?;
        if !matches!(
            target,
            MessagePublicationStatus::Published | MessagePublicationStatus::Discarded
        ) {
            return Err(MessageStoreError::InvalidInput(
                "staged publication target is invalid".into(),
            ));
        }
        let now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let outcome = transition_staged_in_transaction(
            &transaction,
            owner,
            idempotency,
            expected_digest,
            target,
            now,
        )?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Runs the expensive whole-database consistency audit on demand.
    pub fn audit_integrity(&self) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = self.begin_locked_transaction(&mut connection)?;
        audit_database_integrity(&transaction)?;
        transaction.commit()
    }
}

impl MessageStore for SqliteMessageStore {
    fn enqueue(&self, owner: &str, message: &NewMessage) -> Result<EnqueueOutcome> {
        let now = self.now()?;
        validate_owner(owner)?;
        message.validate_structure()?;
        let digest = message.canonical_digest()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let outcome = enqueue_in_transaction(&transaction, owner, message, digest.as_str(), now)?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn stage(&self, owner: &str, message: &NewMessage) -> Result<MessagePublicationOutcome> {
        let now = self.now()?;
        validate_owner(owner)?;
        message.validate_structure()?;
        let digest = message.canonical_digest()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let outcome = stage_in_transaction(&transaction, owner, message, &digest, now)?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn publish_staged(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessagePublicationOutcome>> {
        self.transition_staged(
            owner,
            idempotency,
            expected_digest,
            MessagePublicationStatus::Published,
        )
    }

    fn discard_staged(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessagePublicationOutcome>> {
        self.transition_staged(
            owner,
            idempotency,
            expected_digest,
            MessagePublicationStatus::Discarded,
        )
    }

    fn get(&self, owner: &str, message_id: &str) -> Result<Option<MessageBundle>> {
        validate_owner(owner)?;
        validate_text("message id", message_id, 64)?;
        let connection = self.connection()?;
        if publication_status(&connection, owner, message_id)?
            != Some(MessagePublicationStatus::Published)
        {
            return Ok(None);
        }
        get_bundle(&connection, owner, message_id)
    }

    fn resolve_idempotency(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessageIdempotencyResolution>> {
        validate_owner(owner)?;
        idempotency.validate()?;
        let connection = self.connection()?;
        let resolved: Option<(String, String, DateTime<Utc>, String)> = connection
            .query_row(
                "SELECT m.id, m.request_digest, m.created_at_ms, p.status FROM messages m \
                 JOIN message_publications p ON p.owner = m.owner AND p.message_id = m.id \
                 WHERE m.owner = ?1 AND m.producer = ?2 \
                 AND m.idempotency_key = ?3 AND p.status = 'published'",
                params![owner, idempotency.producer, idempotency.key],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        stored_timestamp(row, 2, "message created_at")?,
                        row.get(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((message_id, request_digest, created_at, status)) = resolved else {
            return Ok(None);
        };
        validate_uuid_v7("message id", &message_id)
            .map_err(|_| MessageStoreError::CorruptData("message id is invalid".into()))?;
        let request_digest = MessageRequestDigest::parse(request_digest).map_err(|_| {
            MessageStoreError::CorruptData("message request digest is invalid".into())
        })?;
        if request_digest != *expected_digest {
            return Err(MessageStoreError::IdempotencyConflict);
        }
        Ok(Some(MessageIdempotencyResolution {
            owner: owner.to_string(),
            message_id,
            request_digest,
            status: MessagePublicationStatus::from_str(&status)?,
            created_at,
        }))
    }

    fn resolve_publication(
        &self,
        owner: &str,
        idempotency: &IdempotencyKey,
        expected_digest: &MessageRequestDigest,
    ) -> Result<Option<MessageIdempotencyResolution>> {
        validate_owner(owner)?;
        idempotency.validate()?;
        let connection = self.connection()?;
        let Some(stored) = publication_by_key(&connection, owner, idempotency)? else {
            return Ok(None);
        };
        if stored.request_digest != *expected_digest {
            return Err(MessageStoreError::IdempotencyConflict);
        }
        Ok(Some(stored.into_resolution(owner)))
    }

    fn list_conversation(
        &self,
        owner: &str,
        conversation_id: &str,
        cursor: Option<&MessageCursor>,
        limit: usize,
    ) -> Result<MessagePage> {
        validate_owner(owner)?;
        validate_text("conversation id", conversation_id, 256)?;
        validate_limit(limit, "message page limit")?;
        if let Some(cursor) = cursor {
            cursor.validate_for(conversation_id)?;
        }
        let connection = self.connection()?;
        let fetch = limit.saturating_add(1);
        let mut messages = if let Some(cursor) = cursor {
            let sql = format!(
                "SELECT {PUBLIC_MESSAGE_COLUMNS} FROM messages m \
                 JOIN message_publications p ON p.owner = m.owner AND p.message_id = m.id \
                 WHERE m.owner = ?1 AND m.conversation_id = ?2 AND p.status = 'published' \
                   AND (p.conversation_sequence > ?3 OR \
                        (p.conversation_sequence = ?3 AND m.id > ?4)) \
                 ORDER BY p.conversation_sequence ASC, m.id ASC LIMIT ?5"
            );
            query_messages(
                &connection,
                &sql,
                params![
                    owner,
                    conversation_id,
                    u64_to_i64(cursor.sequence, "cursor sequence")?,
                    cursor.id,
                    usize_to_i64(fetch, "message page limit")?
                ],
            )?
        } else {
            let sql = format!(
                "SELECT {PUBLIC_MESSAGE_COLUMNS} FROM messages m \
                 JOIN message_publications p ON p.owner = m.owner AND p.message_id = m.id \
                 WHERE m.owner = ?1 AND m.conversation_id = ?2 AND p.status = 'published' \
                 ORDER BY p.conversation_sequence ASC, m.id ASC LIMIT ?3"
            );
            query_messages(
                &connection,
                &sql,
                params![
                    owner,
                    conversation_id,
                    usize_to_i64(fetch, "message page limit")?
                ],
            )?
        };
        let has_more = messages.len() > limit;
        messages.truncate(limit);
        let mut items = Vec::with_capacity(messages.len());
        for message in &messages {
            items.push(MessageBundle {
                message: message.clone(),
                deliveries: deliveries_for_message(&connection, owner, &message.id)?,
            });
        }
        let next_cursor = has_more
            .then(|| {
                messages.last().map(|message| MessageCursor {
                    conversation_id: message.conversation_id.clone(),
                    sequence: message.conversation_sequence,
                    id: message.id.clone(),
                })
            })
            .flatten();
        Ok(MessagePage { items, next_cursor })
    }

    fn list_mailbox(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        query: &MailboxQuery,
    ) -> Result<MailboxPage> {
        validate_owner(owner)?;
        mailbox.validate()?;
        query.validate()?;
        let now = self.now()?;
        let connection = self.connection()?;
        let status_predicate = if query.include_acknowledged {
            "d.status IN ('pending', 'leased', 'delivered', 'acknowledged')"
        } else {
            "d.status IN ('pending', 'leased', 'delivered')"
        };
        let availability_predicate = if query.include_future {
            "1 = 1"
        } else {
            "d.available_at_ms <= ?5"
        };
        let sql = format!(
            "SELECT d.id FROM deliveries d \
             WHERE d.owner = ?1 AND d.route = ?2 AND d.target_kind = ?3 \
               AND d.target_id = ?4 AND {status_predicate} \
               AND {availability_predicate} \
               AND (d.expires_at_ms IS NULL OR d.expires_at_ms > ?5) \
               AND EXISTS (SELECT 1 FROM message_publications p \
                   WHERE p.owner = d.owner AND p.message_id = d.message_id \
                     AND p.status = 'published') \
             ORDER BY d.available_at_ms ASC, ( \
                 SELECT sequence FROM message_events e \
                 WHERE e.owner = d.owner AND e.delivery_id = d.id \
                   AND e.delivery_revision = 0 \
             ) ASC, d.id ASC LIMIT ?6"
        );
        let fetch = query.limit.saturating_add(1);
        let mut statement = connection.prepare(&sql)?;
        let ids = statement
            .query_map(
                params![
                    owner,
                    mailbox.route,
                    mailbox.target.kind.as_str(),
                    mailbox.target.id,
                    now.timestamp_millis(),
                    usize_to_i64(fetch, "mailbox page limit")?
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);

        let has_more = ids.len() > query.limit;
        let mut items = Vec::with_capacity(query.limit.min(ids.len()));
        for delivery_id in ids.into_iter().take(query.limit) {
            let delivery = get_delivery(&connection, owner, &delivery_id)?
                .ok_or(MessageStoreError::NotFound)?;
            let message = get_message(&connection, owner, &delivery.message_id)?
                .ok_or(MessageStoreError::NotFound)?;
            items.push(MailboxMessage { message, delivery });
        }
        Ok(MailboxPage { items, has_more })
    }

    fn events(&self, owner: &str, message_id: &str) -> Result<Vec<MessageEvent>> {
        validate_owner(owner)?;
        validate_text("message id", message_id, 64)?;
        let connection = self.connection()?;
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM message_events \
             WHERE owner = ?1 AND message_id = ?2 ORDER BY sequence ASC"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params![owner, message_id], row_to_event)?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn claim(
        &self,
        owner: &str,
        query: &ClaimQuery,
        lease: &LeaseRequest,
    ) -> Result<Vec<LeasedDelivery>> {
        validate_owner(owner)?;
        query.validate()?;
        lease.validate()?;
        let now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        expire_due_in_transaction(&transaction, owner, now, 1_000)?;
        reclaim_expired_in_transaction(&transaction, owner, now, 1_000)?;

        let mailbox_predicates = std::iter::repeat_n(
            "(route = ? AND target_kind = ? AND target_id = ?)",
            query.mailboxes.len(),
        )
        .collect::<Vec<_>>()
        .join(" OR ");
        let sql = format!(
            "SELECT id FROM deliveries \
             WHERE owner = ? AND ({mailbox_predicates}) \
               AND status = 'pending' AND available_at_ms <= ? \
               AND (expires_at_ms IS NULL OR expires_at_ms > ?) \
               AND attempt_count < max_attempts \
               AND EXISTS (SELECT 1 FROM message_publications p \
                   WHERE p.owner = deliveries.owner AND p.message_id = deliveries.message_id \
                     AND p.status = 'published') \
               AND NOT EXISTS (SELECT 1 FROM delivery_transport_receipts \
                   WHERE owner = deliveries.owner AND delivery_id = deliveries.id) \
             ORDER BY available_at_ms ASC, ( \
                 SELECT sequence FROM message_events \
                 WHERE owner = deliveries.owner AND delivery_id = deliveries.id \
                   AND delivery_revision = 0 \
             ) ASC, id ASC LIMIT ?"
        );
        let mut values = Vec::with_capacity(2 + (query.mailboxes.len() * 3) + 2);
        values.push(Value::Text(owner.to_string()));
        for mailbox in &query.mailboxes {
            values.push(Value::Text(mailbox.route.clone()));
            values.push(Value::Text(mailbox.target.kind.as_str().to_string()));
            values.push(Value::Text(mailbox.target.id.clone()));
        }
        values.push(Value::Integer(now.timestamp_millis()));
        values.push(Value::Integer(now.timestamp_millis()));
        values.push(Value::Integer(usize_to_i64(query.limit, "claim limit")?));
        let mut statement = transaction.prepare(&sql)?;
        let ids = statement
            .query_map(params_from_iter(values.iter()), |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);

        let mut claimed = Vec::with_capacity(ids.len());
        for delivery_id in ids {
            claimed.push(lease_delivery_in_transaction(
                &transaction,
                owner,
                &delivery_id,
                lease,
                now,
            )?);
        }
        transaction.commit()?;
        Ok(claimed)
    }

    fn claim_message(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        message_id: &str,
        lease: &LeaseRequest,
    ) -> Result<Option<LeasedDelivery>> {
        validate_owner(owner)?;
        mailbox.validate()?;
        validate_text("message id", message_id, 64)?;
        lease.validate()?;
        let now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        expire_due_in_transaction(&transaction, owner, now, 1_000)?;
        reclaim_expired_in_transaction(&transaction, owner, now, 1_000)?;
        let delivery_id = transaction
            .query_row(
                "SELECT id FROM deliveries \
                 WHERE owner = ?1 AND route = ?2 AND target_kind = ?3 AND target_id = ?4 \
                   AND message_id = ?5 AND status = 'pending' AND available_at_ms <= ?6 \
                   AND (expires_at_ms IS NULL OR expires_at_ms > ?6) \
                   AND attempt_count < max_attempts \
                   AND EXISTS (SELECT 1 FROM message_publications p \
                       WHERE p.owner = deliveries.owner AND p.message_id = deliveries.message_id \
                         AND p.status = 'published') \
                   AND NOT EXISTS (SELECT 1 FROM delivery_transport_receipts \
                       WHERE owner = deliveries.owner AND delivery_id = deliveries.id) \
                 LIMIT 1",
                params![
                    owner,
                    mailbox.route,
                    mailbox.target.kind.as_str(),
                    mailbox.target.id,
                    message_id,
                    now.timestamp_millis()
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(delivery_id) = delivery_id else {
            transaction.commit()?;
            return Ok(None);
        };
        let claimed = lease_delivery_in_transaction(&transaction, owner, &delivery_id, lease, now)?;
        transaction.commit()?;
        Ok(Some(claimed))
    }

    fn renew(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        operation_id: &str,
        lease_seconds: u64,
    ) -> Result<DeliveryRecord> {
        validate_text("renew operation id", operation_id, 128)?;
        if lease_seconds == 0 || lease_seconds > 86_400 {
            return Err(MessageStoreError::InvalidInput(
                "lease duration must be between 1 second and 24 hours".into(),
            ));
        }
        let operation_digest = hex_lower(&Sha256::digest(operation_id.as_bytes()));
        self.receipt_transition(
            owner,
            mailbox,
            receipt,
            &format!("renew:{operation_digest}:{lease_seconds}"),
            ReceiptAction::Renew {
                lease_seconds,
                operation_digest,
            },
        )
    }

    fn mark_delivered(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
    ) -> Result<DeliveryRecord> {
        self.receipt_transition(
            owner,
            mailbox,
            receipt,
            "delivered",
            ReceiptAction::MarkDelivered,
        )
    }

    fn mark_transport_delivered(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        transport_receipt: &NewTransportReceipt,
    ) -> Result<MarkTransportDeliveredOutcome> {
        validate_owner(owner)?;
        mailbox.validate()?;
        receipt.validate()?;
        transport_receipt.validate()?;
        if &receipt.mailbox != mailbox {
            return Err(MessageStoreError::InvalidReceipt {
                delivery_id: receipt.delivery_id.clone(),
            });
        }
        let receipt_digest = transport_receipt.canonical_digest()?;
        let mut now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let before = authenticate_receipt(&transaction, owner, mailbox, receipt)?;
        if let Some((stored, stored_digest)) =
            get_transport_receipts(&transaction, owner, &receipt.delivery_id)?
        {
            if stored
                .first()
                .is_none_or(|stored| stored.generation != receipt.generation)
                || stored_digest != receipt_digest
            {
                return Err(MessageStoreError::TransportReceiptConflict {
                    delivery_id: receipt.delivery_id.clone(),
                });
            }
            let delivery = get_delivery(&transaction, owner, &receipt.delivery_id)?
                .ok_or(MessageStoreError::NotFound)?;
            transaction.commit()?;
            return Ok(MarkTransportDeliveredOutcome {
                delivery,
                receipts: stored,
                existing: true,
            });
        }
        require_current_receipt(&before, receipt)?;
        now = now.max(before.updated_at);
        if transport_identity_exists(&transaction, owner, transport_receipt)? {
            return Err(MessageStoreError::TransportReceiptConflict {
                delivery_id: receipt.delivery_id.clone(),
            });
        }
        let delivered =
            mark_transport_delivered_in_transaction(&transaction, &before, receipt, now)?;
        for (ordinal, external_id) in transport_receipt.external_ids.iter().enumerate() {
            transaction.execute(
                "INSERT INTO delivery_transport_receipts (record_schema, owner, delivery_id, \
                    generation, ordinal, transport, account_scope, destination_scope, external_id, \
                    receipt_digest, recorded_at_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    i64::from(RECORD_SCHEMA),
                    owner,
                    receipt.delivery_id,
                    u64_to_i64(receipt.generation, "lease generation")?,
                    usize_to_i64(ordinal, "transport receipt ordinal")?,
                    transport_receipt.transport,
                    transport_receipt.account_scope,
                    transport_receipt.destination_scope,
                    external_id,
                    receipt_digest,
                    now.timestamp_millis()
                ],
            )?;
        }
        let (stored, _) = get_transport_receipts(&transaction, owner, &receipt.delivery_id)?
            .ok_or_else(|| MessageStoreError::CorruptData("transport receipt is absent".into()))?;
        transaction.commit()?;
        Ok(MarkTransportDeliveredOutcome {
            delivery: delivered,
            receipts: stored,
            existing: false,
        })
    }

    fn transport_receipts(
        &self,
        owner: &str,
        delivery_id: &str,
    ) -> Result<Vec<TransportReceiptRecord>> {
        validate_owner(owner)?;
        validate_text("delivery id", delivery_id, 64)?;
        let connection = self.connection()?;
        Ok(get_transport_receipts(&connection, owner, delivery_id)?
            .map_or_else(Vec::new, |(receipts, _)| receipts))
    }

    fn resolve_transport_receipt(
        &self,
        owner: &str,
        transport: &str,
        account_scope: &str,
        destination_scope: &str,
        external_id: &str,
    ) -> Result<Option<TransportReceiptResolution>> {
        validate_owner(owner)?;
        let locator = NewTransportReceipt {
            transport: transport.to_string(),
            account_scope: account_scope.to_string(),
            destination_scope: destination_scope.to_string(),
            external_ids: vec![external_id.to_string()],
        };
        locator.validate()?;
        let connection = self.connection()?;
        let delivery_id: Option<String> = connection
            .query_row(
                "SELECT delivery_id FROM delivery_transport_receipts \
                 WHERE owner = ?1 AND transport = ?2 AND account_scope = ?3 \
                   AND destination_scope = ?4 AND external_id = ?5",
                params![
                    owner,
                    transport,
                    account_scope,
                    destination_scope,
                    external_id
                ],
                |row| row.get(0),
            )
            .optional()?;
        let Some(delivery_id) = delivery_id else {
            return Ok(None);
        };
        let (receipts, _) =
            get_transport_receipts(&connection, owner, &delivery_id)?.ok_or_else(|| {
                MessageStoreError::CorruptData("transport receipt batch is absent".into())
            })?;
        let receipt = receipts
            .into_iter()
            .find(|receipt| receipt.external_id == external_id)
            .ok_or_else(|| {
                MessageStoreError::CorruptData("transport receipt locator is absent".into())
            })?;
        let delivery =
            get_delivery(&connection, owner, &delivery_id)?.ok_or(MessageStoreError::NotFound)?;
        let message = get_message(&connection, owner, &delivery.message_id)?
            .ok_or(MessageStoreError::NotFound)?;
        Ok(Some(TransportReceiptResolution {
            receipt,
            delivery,
            message,
        }))
    }

    fn acknowledge(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
    ) -> Result<DeliveryRecord> {
        self.receipt_transition(
            owner,
            mailbox,
            receipt,
            "acknowledged",
            ReceiptAction::Acknowledge,
        )
    }

    fn nack(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        disposition: &NackDisposition,
    ) -> Result<DeliveryRecord> {
        disposition.validate()?;
        let operation_key = match disposition {
            NackDisposition::RetryAfter { delay_seconds } => format!("nack:retry:{delay_seconds}"),
            NackDisposition::Permanent { failure_code } => format!(
                "nack:permanent:{}",
                hex_lower(&Sha256::digest(failure_code.as_bytes()))
            ),
        };
        self.receipt_transition(
            owner,
            mailbox,
            receipt,
            &operation_key,
            ReceiptAction::Nack(disposition.clone()),
        )
    }

    fn reclaim_expired(&self, owner: &str, limit: usize) -> Result<usize> {
        validate_owner(owner)?;
        validate_limit(limit, "reclaim limit")?;
        let now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let changed = reclaim_expired_in_transaction(&transaction, owner, now, limit)?;
        transaction.commit()?;
        Ok(changed)
    }

    fn expire_due(&self, owner: &str, limit: usize) -> Result<usize> {
        validate_owner(owner)?;
        validate_limit(limit, "expiry limit")?;
        let now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let changed = expire_due_in_transaction(&transaction, owner, now, limit)?;
        transaction.commit()?;
        Ok(changed)
    }

    fn cancel(&self, owner: &str, delivery_id: &str) -> Result<DeliveryRecord> {
        validate_owner(owner)?;
        validate_text("delivery id", delivery_id, 64)?;
        let mut now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let before =
            get_delivery(&transaction, owner, delivery_id)?.ok_or(MessageStoreError::NotFound)?;
        if publication_status(&transaction, owner, &before.message_id)?
            != Some(MessagePublicationStatus::Published)
        {
            return Err(MessageStoreError::NotFound);
        }
        now = now.max(before.updated_at);
        if before.status == DeliveryStatus::Cancelled {
            transaction.commit()?;
            return Ok(before);
        }
        if before.status.is_terminal() {
            return Err(MessageStoreError::InvalidState {
                delivery_id: delivery_id.to_string(),
                operation: "cancel",
                state: before.status,
            });
        }
        if has_transport_receipt(&transaction, owner, delivery_id)? {
            return Err(MessageStoreError::InvalidState {
                delivery_id: delivery_id.to_string(),
                operation: "cancel an externally delivered delivery",
                state: before.status,
            });
        }
        update_delivery_state(
            &transaction,
            &before,
            DeliveryStatus::Cancelled,
            now,
            now,
            None,
        )?;
        let after =
            get_delivery(&transaction, owner, delivery_id)?.ok_or(MessageStoreError::NotFound)?;
        let message = get_message(&transaction, owner, &before.message_id)?
            .ok_or(MessageStoreError::NotFound)?;
        insert_event(
            &transaction,
            &message,
            &after,
            MessageEventKind::Cancelled,
            Some(before.status),
            now,
        )?;
        transaction.commit()?;
        Ok(after)
    }

    fn reply_and_ack(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        reply: &NewMessage,
    ) -> Result<ReplyAndAckOutcome> {
        validate_owner(owner)?;
        mailbox.validate()?;
        receipt.validate()?;
        let mut now = self.now()?;
        reply.validate_structure()?;
        let reply_digest = reply.canonical_digest()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let before = authenticate_receipt(&transaction, owner, mailbox, receipt)?;
        let operation_key = format!("reply_and_ack:{reply_digest}");
        if let Some(stored) = stored_reply_operation(&transaction, owner, receipt, &operation_key)?
        {
            let reply_bundle = get_bundle(&transaction, owner, &stored.reply_message_id)?
                .ok_or_else(|| MessageStoreError::CorruptData("stored reply is absent".into()))?;
            transaction.commit()?;
            return Ok(ReplyAndAckOutcome {
                reply: EnqueueOutcome {
                    bundle: reply_bundle,
                    existing: true,
                },
                acknowledged: stored.delivery,
            });
        }
        require_current_receipt(&before, receipt)?;
        now = now.max(before.updated_at);
        let original = get_message(&transaction, owner, &before.message_id)?
            .ok_or(MessageStoreError::NotFound)?;
        let expected_reply_to = Some(before.message_id.as_str());
        if reply.reply_to.as_deref() != expected_reply_to {
            return Err(MessageStoreError::InvalidInput(
                "reply_and_ack requires reply_to to match the leased message".into(),
            ));
        }
        if reply.conversation_id != original.conversation_id {
            return Err(MessageStoreError::InvalidInput(
                "reply_and_ack requires the same logical conversation".into(),
            ));
        }
        reject_conflicting_terminal_operation(&transaction, owner, receipt, "ack")?;
        require_live_state(&before, receipt, now, "reply and acknowledge", true)?;
        let reply_outcome =
            enqueue_in_transaction(&transaction, owner, reply, reply_digest.as_str(), now)?;
        let acknowledged = acknowledge_in_transaction(&transaction, &before, now)?;
        store_reply_operation(
            &transaction,
            owner,
            receipt,
            &operation_key,
            &acknowledged,
            &reply_outcome.bundle.message.id,
            now,
        )?;
        transaction.commit()?;
        Ok(ReplyAndAckOutcome {
            reply: reply_outcome,
            acknowledged,
        })
    }

    fn unprojected_events(&self, owner: &str, projector: &str, limit: usize) -> Result<OutboxPage> {
        validate_owner(owner)?;
        validate_text("outbox projector", projector, 128)?;
        validate_limit(limit, "outbox limit")?;
        let connection = self.connection()?;
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM message_events \
             WHERE owner = ?1 AND NOT EXISTS ( \
                 SELECT 1 FROM message_event_projections \
                 WHERE owner = message_events.owner AND projector = ?2 \
                   AND event_sequence = message_events.sequence \
             ) ORDER BY sequence ASC LIMIT ?3"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                owner,
                projector,
                usize_to_i64(limit.saturating_add(1), "outbox limit")?
            ],
            row_to_event,
        )?;
        let mut items = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        Ok(OutboxPage { items, has_more })
    }

    fn mark_projected(&self, owner: &str, projector: &str, event_id: &str) -> Result<()> {
        validate_owner(owner)?;
        validate_text("outbox projector", projector, 128)?;
        validate_text("event id", event_id, 64)?;
        let now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let changed = transaction.execute(
            "INSERT INTO message_event_projections (owner, projector, event_sequence, projected_at_ms) \
             SELECT owner, ?1, sequence, MAX(?2, occurred_at_ms) FROM message_events \
             WHERE owner = ?3 AND event_id = ?4 \
             ON CONFLICT(owner, projector, event_sequence) DO NOTHING",
            params![projector, now.timestamp_millis(), owner, event_id],
        )?;
        if changed == 1 {
            transaction.commit()?;
            return Ok(());
        }
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM message_events e \
             JOIN message_event_projections p \
               ON p.owner = e.owner AND p.event_sequence = e.sequence \
             WHERE e.owner = ?1 AND e.event_id = ?2 AND p.projector = ?3)",
            params![owner, event_id, projector],
            |row| row.get(0),
        )?;
        if exists {
            transaction.commit()?;
            Ok(())
        } else {
            Err(MessageStoreError::ProjectionConflict)
        }
    }
}

impl SqliteMessageStore {
    fn receipt_transition(
        &self,
        owner: &str,
        mailbox: &DeliveryMailbox,
        receipt: &LeaseReceipt,
        operation_key: &str,
        action: ReceiptAction,
    ) -> Result<DeliveryRecord> {
        validate_owner(owner)?;
        mailbox.validate()?;
        receipt.validate()?;
        if &receipt.mailbox != mailbox {
            return Err(MessageStoreError::InvalidReceipt {
                delivery_id: receipt.delivery_id.clone(),
            });
        }
        let mut now = self.now()?;
        let mut connection = self.connection()?;
        let transaction = self.write_transaction(&mut connection)?;
        let before = authenticate_receipt(&transaction, owner, mailbox, receipt)?;
        if let Some(result) =
            stored_delivery_operation(&transaction, owner, receipt, operation_key)?
        {
            transaction.commit()?;
            return Ok(result);
        }
        if let ReceiptAction::Renew {
            operation_digest, ..
        } = &action
        {
            reject_renew_operation_drift(
                &transaction,
                owner,
                receipt,
                operation_digest,
                operation_key,
            )?;
        }
        if matches!(&action, ReceiptAction::Acknowledge)
            && before.status == DeliveryStatus::Acknowledged
            && has_transport_receipt(&transaction, owner, &receipt.delivery_id)?
        {
            now = now.max(before.updated_at);
            store_delivery_operation(&transaction, owner, receipt, operation_key, &before, now)?;
            transaction.commit()?;
            return Ok(before);
        }
        require_current_receipt(&before, receipt)?;
        now = now.max(before.updated_at);
        match action {
            ReceiptAction::Acknowledge | ReceiptAction::Nack(_) => {
                reject_conflicting_terminal_operation(
                    &transaction,
                    owner,
                    receipt,
                    if matches!(action, ReceiptAction::Acknowledge) {
                        "ack"
                    } else {
                        "nack"
                    },
                )?;
            }
            _ => {}
        }
        let result = match action {
            ReceiptAction::Renew { lease_seconds, .. } => {
                renew_in_transaction(&transaction, &before, receipt, now, lease_seconds)?
            }
            ReceiptAction::MarkDelivered => {
                mark_delivered_in_transaction(&transaction, &before, receipt, now)?
            }
            ReceiptAction::Acknowledge => acknowledge_in_transaction(&transaction, &before, now)?,
            ReceiptAction::Nack(disposition) => {
                nack_in_transaction(&transaction, &before, receipt, now, &disposition)?
            }
        };
        store_delivery_operation(&transaction, owner, receipt, operation_key, &result, now)?;
        transaction.commit()?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
enum ReceiptAction {
    Renew {
        lease_seconds: u64,
        operation_digest: String,
    },
    MarkDelivered,
    Acknowledge,
    Nack(NackDisposition),
}

fn lease_delivery_in_transaction(
    connection: &Connection,
    owner: &str,
    delivery_id: &str,
    lease: &LeaseRequest,
    now: DateTime<Utc>,
) -> Result<LeasedDelivery> {
    let before =
        get_delivery(connection, owner, delivery_id)?.ok_or(MessageStoreError::NotFound)?;
    let message =
        get_message(connection, owner, &before.message_id)?.ok_or(MessageStoreError::NotFound)?;
    let claim_now = now.max(before.updated_at);
    let requested_lease_expires_at = add_seconds(claim_now, lease.lease_seconds, "lease duration")?;
    let claimed_mailbox = DeliveryMailbox {
        route: before.route.clone(),
        target: before.target.clone(),
    };
    let lease_expires_at = before.expires_at.map_or(requested_lease_expires_at, |ttl| {
        requested_lease_expires_at.min(ttl)
    });
    if lease_expires_at <= claim_now {
        return Err(MessageStoreError::CorruptData(
            "claim selected a delivery with no live lease window".into(),
        ));
    }
    let token = random_token()?;
    let token_hash = Sha256::digest(token.as_bytes()).to_vec();
    let generation = next_u64(before.lease_generation, "lease generation")?;
    let attempt_count = before.attempt_count.checked_add(1).ok_or_else(|| {
        MessageStoreError::CorruptData("delivery attempt counter overflow".into())
    })?;
    let revision = next_u64(before.revision, "delivery revision")?;
    let changed = connection.execute(
        "UPDATE deliveries SET status = 'leased', attempt_count = ?1, revision = ?2, \
            lease_generation = ?3, lease_owner = ?4, lease_token_hash = ?5, \
            lease_expires_at_ms = ?6, updated_at_ms = ?7, failure_code = NULL \
         WHERE owner = ?8 AND id = ?9 AND status = 'pending' AND revision = ?10",
        params![
            i64::from(attempt_count),
            u64_to_i64(revision, "delivery revision")?,
            u64_to_i64(generation, "lease generation")?,
            lease.consumer,
            token_hash,
            lease_expires_at.timestamp_millis(),
            claim_now.timestamp_millis(),
            owner,
            delivery_id,
            u64_to_i64(before.revision, "delivery revision")?
        ],
    )?;
    if changed != 1 {
        return Err(MessageStoreError::CorruptData(
            "claim lost its write lock invariant".into(),
        ));
    }
    connection.execute(
        "INSERT INTO delivery_attempts (owner, delivery_id, generation, route, \
            target_kind, target_id, consumer, token_hash, claimed_at_ms, initial_expires_at_ms) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            owner,
            delivery_id,
            u64_to_i64(generation, "lease generation")?,
            claimed_mailbox.route,
            claimed_mailbox.target.kind.as_str(),
            claimed_mailbox.target.id,
            lease.consumer,
            Sha256::digest(token.as_bytes()).to_vec(),
            claim_now.timestamp_millis(),
            lease_expires_at.timestamp_millis()
        ],
    )?;
    let after = get_delivery(connection, owner, delivery_id)?.ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        MessageEventKind::Leased,
        Some(before.status),
        claim_now,
    )?;
    Ok(LeasedDelivery {
        message,
        delivery: after,
        receipt: LeaseReceipt {
            delivery_id: delivery_id.to_string(),
            generation,
            mailbox: claimed_mailbox,
            consumer: lease.consumer.clone(),
            token,
        },
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredReplyOperation {
    delivery: DeliveryRecord,
    reply_message_id: String,
}

#[derive(Debug)]
struct StoredPublication {
    message_id: String,
    conversation_id: String,
    request_digest: MessageRequestDigest,
    created_at: DateTime<Utc>,
    origin: String,
    status: MessagePublicationStatus,
}

impl StoredPublication {
    fn into_resolution(self, owner: &str) -> MessageIdempotencyResolution {
        MessageIdempotencyResolution {
            owner: owner.to_string(),
            message_id: self.message_id,
            request_digest: self.request_digest,
            status: self.status,
            created_at: self.created_at,
        }
    }
}

fn publication_by_key(
    connection: &Connection,
    owner: &str,
    idempotency: &IdempotencyKey,
) -> Result<Option<StoredPublication>> {
    let stored: Option<(String, String, String, DateTime<Utc>, String, String)> = connection
        .query_row(
            "SELECT m.id, m.conversation_id, m.request_digest, m.created_at_ms, p.origin, p.status \
             FROM messages m JOIN message_publications p \
               ON p.owner = m.owner AND p.message_id = m.id \
             WHERE m.owner = ?1 AND m.producer = ?2 AND m.idempotency_key = ?3",
            params![owner, idempotency.producer, idempotency.key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    stored_timestamp(row, 3, "message created_at")?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    stored
        .map(
            |(message_id, conversation_id, request_digest, created_at, origin, status)| {
                validate_uuid_v7("message id", &message_id).map_err(|_| {
                    MessageStoreError::CorruptData("publication message id is invalid".into())
                })?;
                if !matches!(origin.as_str(), "ordinary" | "staged") {
                    return Err(MessageStoreError::CorruptData(
                        "message publication origin is invalid".into(),
                    ));
                }
                Ok(StoredPublication {
                    message_id,
                    conversation_id,
                    request_digest: MessageRequestDigest::parse(request_digest).map_err(|_| {
                        MessageStoreError::CorruptData("message request digest is invalid".into())
                    })?,
                    created_at,
                    origin,
                    status: MessagePublicationStatus::from_str(&status)?,
                })
            },
        )
        .transpose()
}

fn publication_status(
    connection: &Connection,
    owner: &str,
    message_id: &str,
) -> Result<Option<MessagePublicationStatus>> {
    let status: Option<String> = connection
        .query_row(
            "SELECT status FROM message_publications WHERE owner = ?1 AND message_id = ?2",
            params![owner, message_id],
            |row| row.get(0),
        )
        .optional()?;
    status
        .map(|status| MessagePublicationStatus::from_str(&status))
        .transpose()
}

fn transition_staged_in_transaction(
    connection: &Connection,
    owner: &str,
    idempotency: &IdempotencyKey,
    expected_digest: &MessageRequestDigest,
    target: MessagePublicationStatus,
    now: DateTime<Utc>,
) -> Result<Option<MessagePublicationOutcome>> {
    let Some(mut stored) = publication_by_key(connection, owner, idempotency)? else {
        return Ok(None);
    };
    if stored.request_digest != *expected_digest || stored.origin != "staged" {
        return Err(MessageStoreError::IdempotencyConflict);
    }
    if stored.status == target {
        return Ok(Some(MessagePublicationOutcome {
            resolution: stored.into_resolution(owner),
            existing: true,
        }));
    }
    if stored.status != MessagePublicationStatus::Staged {
        return Err(MessageStoreError::PublicationConflict);
    }
    let transition_now = now.max(stored.created_at);
    let publication_sequence = if target == MessagePublicationStatus::Published {
        Some(allocate_publication_sequence(
            connection,
            owner,
            &stored.conversation_id,
        )?)
    } else {
        None
    };
    let (published_at, discarded_at) = match target {
        MessagePublicationStatus::Published => (Some(transition_now.timestamp_millis()), None),
        MessagePublicationStatus::Discarded => (None, Some(transition_now.timestamp_millis())),
        MessagePublicationStatus::Staged => {
            return Err(MessageStoreError::InvalidInput(
                "staged publication target is invalid".into(),
            ));
        }
    };
    let changed = connection.execute(
        "UPDATE message_publications SET status = ?1, published_at_ms = ?2, \
            discarded_at_ms = ?3, conversation_sequence = COALESCE(?4, conversation_sequence), \
            revision = 1 \
         WHERE owner = ?5 AND message_id = ?6 AND origin = 'staged' \
           AND status = 'staged' AND revision = 0",
        params![
            target.as_str(),
            published_at,
            discarded_at,
            publication_sequence,
            owner,
            stored.message_id
        ],
    )?;
    if changed != 1 {
        return Err(MessageStoreError::CorruptData(
            "staged publication lost its write-lock invariant".into(),
        ));
    }
    if target == MessagePublicationStatus::Published {
        let message = get_message(connection, owner, &stored.message_id)?
            .ok_or_else(|| MessageStoreError::CorruptData("staged message is absent".into()))?;
        let deliveries = deliveries_for_message(connection, owner, &stored.message_id)?;
        for before in deliveries {
            let event_now = transition_now.max(before.updated_at);
            let changed = connection.execute(
                "UPDATE deliveries SET updated_at_ms = ?1 \
                 WHERE owner = ?2 AND id = ?3 AND revision = 0 AND status = 'pending'",
                params![event_now.timestamp_millis(), owner, before.id],
            )?;
            if changed != 1 {
                return Err(MessageStoreError::CorruptData(
                    "staged delivery lost its publication invariant".into(),
                ));
            }
            let after = get_delivery(connection, owner, &before.id)?.ok_or_else(|| {
                MessageStoreError::CorruptData("staged delivery is absent".into())
            })?;
            insert_event(
                connection,
                &message,
                &after,
                MessageEventKind::Enqueued,
                None,
                event_now,
            )?;
        }
    }
    stored.status = target;
    Ok(Some(MessagePublicationOutcome {
        resolution: stored.into_resolution(owner),
        existing: false,
    }))
}

fn enqueue_in_transaction(
    connection: &Connection,
    owner: &str,
    request: &NewMessage,
    request_digest: &str,
    now: DateTime<Utc>,
) -> Result<EnqueueOutcome> {
    if let Some(existing) = publication_by_key(connection, owner, &request.idempotency)? {
        if existing.request_digest.as_str() != request_digest || existing.origin != "ordinary" {
            return Err(MessageStoreError::IdempotencyConflict);
        }
        if existing.status != MessagePublicationStatus::Published {
            return Err(MessageStoreError::CorruptData(
                "ordinary publication is not published".into(),
            ));
        }
        let bundle = get_bundle(connection, owner, &existing.message_id)?
            .ok_or_else(|| MessageStoreError::CorruptData("idempotent message is absent".into()))?;
        return Ok(EnqueueOutcome {
            bundle,
            existing: true,
        });
    }

    let bundle = insert_message_in_transaction(
        connection,
        owner,
        request,
        request_digest,
        now,
        "ordinary",
        MessagePublicationStatus::Published,
    )?;
    Ok(EnqueueOutcome {
        bundle,
        existing: false,
    })
}

fn stage_in_transaction(
    connection: &Connection,
    owner: &str,
    request: &NewMessage,
    request_digest: &MessageRequestDigest,
    now: DateTime<Utc>,
) -> Result<MessagePublicationOutcome> {
    if let Some(existing) = publication_by_key(connection, owner, &request.idempotency)? {
        if existing.request_digest != *request_digest || existing.origin != "staged" {
            return Err(MessageStoreError::IdempotencyConflict);
        }
        return Ok(MessagePublicationOutcome {
            resolution: existing.into_resolution(owner),
            existing: true,
        });
    }

    let bundle = insert_message_in_transaction(
        connection,
        owner,
        request,
        request_digest.as_str(),
        now,
        "staged",
        MessagePublicationStatus::Staged,
    )?;
    Ok(MessagePublicationOutcome {
        resolution: MessageIdempotencyResolution {
            owner: owner.to_string(),
            message_id: bundle.message.id,
            request_digest: request_digest.clone(),
            status: MessagePublicationStatus::Staged,
            created_at: bundle.message.created_at,
        },
        existing: false,
    })
}

fn allocate_publication_sequence(
    connection: &Connection,
    owner: &str,
    conversation_id: &str,
) -> Result<i64> {
    let sequence: i64 = connection.query_row(
        "INSERT INTO publication_conversation_sequences (owner, conversation_id, next_sequence) \
         VALUES (?1, ?2, 2) \
         ON CONFLICT(owner, conversation_id) DO UPDATE \
             SET next_sequence = next_sequence + 1 \
         RETURNING next_sequence - 1",
        params![owner, conversation_id],
        |row| row.get(0),
    )?;
    Ok(sequence)
}

#[allow(clippy::too_many_arguments)]
fn insert_message_in_transaction(
    connection: &Connection,
    owner: &str,
    request: &NewMessage,
    request_digest: &str,
    now: DateTime<Utc>,
    origin: &str,
    publication_status: MessagePublicationStatus,
) -> Result<MessageBundle> {
    if !matches!(
        (origin, publication_status),
        ("ordinary", MessagePublicationStatus::Published)
            | ("staged", MessagePublicationStatus::Staged)
    ) {
        return Err(MessageStoreError::CorruptData(
            "new message publication mode is invalid".into(),
        ));
    }

    request.validate(now)?;

    if let Some(reply_to) = request.reply_to.as_deref() {
        let reply_conversation: Option<String> = connection
            .query_row(
                "SELECT m.conversation_id FROM messages m \
                 JOIN message_publications p ON p.owner = m.owner AND p.message_id = m.id \
                 WHERE m.owner = ?1 AND m.id = ?2 AND p.status = 'published'",
                params![owner, reply_to],
                |row| row.get(0),
            )
            .optional()?;
        if reply_conversation.as_deref() != Some(request.conversation_id.as_str()) {
            return Err(MessageStoreError::InvalidInput(
                "reply_to must exist in the same authorized conversation".into(),
            ));
        }
    }

    let sequence: i64 = connection.query_row(
        "INSERT INTO conversation_sequences (owner, conversation_id, next_sequence) \
         VALUES (?1, ?2, 2) \
         ON CONFLICT(owner, conversation_id) DO UPDATE \
             SET next_sequence = next_sequence + 1 \
         RETURNING next_sequence - 1",
        params![owner, request.conversation_id],
        |row| row.get(0),
    )?;
    let public_sequence = if publication_status == MessagePublicationStatus::Published {
        allocate_publication_sequence(connection, owner, &request.conversation_id)?
    } else {
        sequence
    };
    let message_id = Uuid::now_v7().to_string();
    let payload_json = serde_json::to_string(&request.payload).map_err(|_| {
        MessageStoreError::InvalidInput("message payload is not serializable".into())
    })?;
    connection.execute(
        "INSERT INTO messages (id, record_schema, owner, conversation_id, \
            conversation_sequence, session_id, direction, kind, sender_kind, sender_id, body, \
            payload_json, reply_to, trace_id, correlation_id, producer, idempotency_key, \
            request_digest, created_at_ms) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                 ?15, ?16, ?17, ?18, ?19)",
        params![
            message_id,
            i64::from(RECORD_SCHEMA),
            owner,
            request.conversation_id,
            sequence,
            request.session_id,
            request.direction.as_str(),
            request.kind,
            request.sender.kind.as_str(),
            request.sender.id,
            request.body,
            payload_json,
            request.reply_to,
            request.trace_id,
            request.correlation_id,
            request.idempotency.producer,
            request.idempotency.key,
            request_digest,
            now.timestamp_millis()
        ],
    )?;
    connection.execute(
        "INSERT INTO message_publications (owner, message_id, conversation_id, \
            conversation_sequence, origin, status, published_at_ms, discarded_at_ms, \
            revision, record_schema) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 0, 1)",
        params![
            owner,
            message_id,
            request.conversation_id,
            public_sequence,
            origin,
            publication_status.as_str(),
            (publication_status == MessagePublicationStatus::Published)
                .then_some(now.timestamp_millis())
        ],
    )?;
    let message = get_message(connection, owner, &message_id)?
        .ok_or_else(|| MessageStoreError::CorruptData("new message is absent".into()))?;
    let mut deliveries = Vec::with_capacity(request.deliveries.len());
    for requested in &request.deliveries {
        let delivery_id = Uuid::now_v7().to_string();
        let available_at = requested.available_at.unwrap_or(now);
        connection.execute(
            "INSERT INTO deliveries (id, record_schema, owner, message_id, route, target_kind, \
                target_id, status, available_at_ms, expires_at_ms, attempt_count, max_attempts, \
                revision, lease_generation, created_at_ms, updated_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', ?8, ?9, 0, ?10, 0, 0, ?11, ?11)",
            params![
                delivery_id,
                i64::from(RECORD_SCHEMA),
                owner,
                message_id,
                requested.route,
                requested.target.kind.as_str(),
                requested.target.id,
                available_at.timestamp_millis(),
                requested.expires_at.map(|value| value.timestamp_millis()),
                i64::from(requested.max_attempts),
                now.timestamp_millis()
            ],
        )?;
        let delivery = get_delivery(connection, owner, &delivery_id)?
            .ok_or_else(|| MessageStoreError::CorruptData("new delivery is absent".into()))?;
        if publication_status == MessagePublicationStatus::Published {
            insert_event(
                connection,
                &message,
                &delivery,
                MessageEventKind::Enqueued,
                None,
                now,
            )?;
        }
        deliveries.push(delivery);
    }
    Ok(MessageBundle {
        message,
        deliveries,
    })
}

fn get_bundle(
    connection: &Connection,
    owner: &str,
    message_id: &str,
) -> Result<Option<MessageBundle>> {
    let Some(message) = get_message(connection, owner, message_id)? else {
        return Ok(None);
    };
    let deliveries = deliveries_for_message(connection, owner, message_id)?;
    Ok(Some(MessageBundle {
        message,
        deliveries,
    }))
}

fn get_message(
    connection: &Connection,
    owner: &str,
    message_id: &str,
) -> Result<Option<MessageRecord>> {
    let sql = format!(
        "SELECT {PUBLIC_MESSAGE_COLUMNS} FROM messages m \
         JOIN message_publications p ON p.owner = m.owner AND p.message_id = m.id \
         WHERE m.owner = ?1 AND m.id = ?2"
    );
    Ok(connection
        .query_row(&sql, params![owner, message_id], row_to_message)
        .optional()?)
}

fn get_delivery(
    connection: &Connection,
    owner: &str,
    delivery_id: &str,
) -> Result<Option<DeliveryRecord>> {
    let sql = format!("SELECT {DELIVERY_COLUMNS} FROM deliveries WHERE owner = ?1 AND id = ?2");
    Ok(connection
        .query_row(&sql, params![owner, delivery_id], row_to_delivery)
        .optional()?)
}

fn get_transport_receipts(
    connection: &Connection,
    owner: &str,
    delivery_id: &str,
) -> Result<Option<(Vec<TransportReceiptRecord>, String)>> {
    let sql = format!(
        "SELECT {TRANSPORT_RECEIPT_COLUMNS} FROM delivery_transport_receipts \
         WHERE owner = ?1 AND delivery_id = ?2 ORDER BY ordinal ASC"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![owner, delivery_id], row_to_transport_receipt)?;
    let stored = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    if stored.is_empty() {
        return Ok(None);
    }
    let digest = stored[0].1.clone();
    let first = stored[0].0.clone();
    let mut records = Vec::with_capacity(stored.len());
    for (index, (record, row_digest)) in stored.into_iter().enumerate() {
        if usize::try_from(record.ordinal).ok() != Some(index)
            || record.owner != first.owner
            || record.delivery_id != first.delivery_id
            || record.generation != first.generation
            || record.transport != first.transport
            || record.account_scope != first.account_scope
            || record.destination_scope != first.destination_scope
            || record.recorded_at != first.recorded_at
            || row_digest != digest
        {
            return Err(MessageStoreError::CorruptData(
                "transport receipt batch rows disagree".into(),
            ));
        }
        records.push(record);
    }
    let request = NewTransportReceipt {
        transport: first.transport.clone(),
        account_scope: first.account_scope.clone(),
        destination_scope: first.destination_scope.clone(),
        external_ids: records
            .iter()
            .map(|record| record.external_id.clone())
            .collect(),
    };
    if request.canonical_digest()?.as_str() != digest {
        return Err(MessageStoreError::CorruptData(
            "transport receipt batch digest does not match its identities".into(),
        ));
    }
    Ok(Some((records, digest)))
}

fn transport_identity_exists(
    connection: &Connection,
    owner: &str,
    receipt: &NewTransportReceipt,
) -> Result<bool> {
    for external_id in &receipt.external_ids {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM delivery_transport_receipts \
             WHERE owner = ?1 AND transport = ?2 AND account_scope = ?3 \
               AND destination_scope = ?4 AND external_id = ?5)",
            params![
                owner,
                receipt.transport,
                receipt.account_scope,
                receipt.destination_scope,
                external_id
            ],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_transport_receipt(connection: &Connection, owner: &str, delivery_id: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM delivery_transport_receipts \
         WHERE owner = ?1 AND delivery_id = ?2)",
        params![owner, delivery_id],
        |row| row.get(0),
    )?)
}

fn deliveries_for_message(
    connection: &Connection,
    owner: &str,
    message_id: &str,
) -> Result<Vec<DeliveryRecord>> {
    let sql = format!(
        "SELECT {DELIVERY_COLUMNS} FROM deliveries \
         WHERE owner = ?1 AND message_id = ?2 ORDER BY created_at_ms ASC, id ASC"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![owner, message_id], row_to_delivery)?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_messages<P: rusqlite::Params>(
    connection: &Connection,
    sql: &str,
    parameters: P,
) -> Result<Vec<MessageRecord>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(parameters, row_to_message)?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn insert_event(
    connection: &Connection,
    message: &MessageRecord,
    delivery: &DeliveryRecord,
    kind: MessageEventKind,
    from_status: Option<DeliveryStatus>,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let sequence: i64 = connection.query_row(
        "INSERT INTO owner_event_sequences (owner, next_sequence) VALUES (?1, 2) \
         ON CONFLICT(owner) DO UPDATE SET next_sequence = next_sequence + 1 \
         RETURNING next_sequence - 1",
        params![message.owner],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT INTO message_events (sequence, event_id, owner, message_id, delivery_id, \
            delivery_revision, conversation_id, conversation_sequence, occurred_at_ms, \
            event_type, from_status, to_status, lease_generation, route, target_kind, \
            target_id, direction, reply_to) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                 ?15, ?16, ?17, ?18)",
        params![
            sequence,
            Uuid::now_v7().to_string(),
            message.owner,
            message.id,
            delivery.id,
            u64_to_i64(delivery.revision, "delivery revision")?,
            message.conversation_id,
            u64_to_i64(message.conversation_sequence, "conversation sequence")?,
            occurred_at.timestamp_millis(),
            kind.as_str(),
            from_status.map(DeliveryStatus::as_str),
            delivery.status.as_str(),
            u64_to_i64(delivery.lease_generation, "lease generation")?,
            delivery.route,
            delivery.target.kind.as_str(),
            delivery.target.id,
            message.direction.as_str(),
            message.reply_to
        ],
    )?;
    Ok(())
}

fn authenticate_receipt(
    connection: &Connection,
    owner: &str,
    mailbox: &DeliveryMailbox,
    receipt: &LeaseReceipt,
) -> Result<DeliveryRecord> {
    if &receipt.mailbox != mailbox {
        return Err(MessageStoreError::InvalidReceipt {
            delivery_id: receipt.delivery_id.clone(),
        });
    }
    let delivery = get_delivery(connection, owner, &receipt.delivery_id)?
        .ok_or(MessageStoreError::NotFound)?;
    if delivery.route != mailbox.route || delivery.target != mailbox.target {
        return Err(MessageStoreError::InvalidReceipt {
            delivery_id: receipt.delivery_id.clone(),
        });
    }
    let attempt: Option<(String, String, String, String, Vec<u8>)> = connection
        .query_row(
            "SELECT route, target_kind, target_id, consumer, token_hash FROM delivery_attempts \
             WHERE owner = ?1 AND delivery_id = ?2 AND generation = ?3",
            params![
                owner,
                receipt.delivery_id,
                u64_to_i64(receipt.generation, "lease generation")?
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((route, target_kind, target_id, consumer, token_hash)) = attempt else {
        return Err(MessageStoreError::InvalidReceipt {
            delivery_id: receipt.delivery_id.clone(),
        });
    };
    let candidate = Sha256::digest(receipt.token.as_bytes());
    if route != mailbox.route
        || target_kind != mailbox.target.kind.as_str()
        || target_id != mailbox.target.id
        || consumer != receipt.consumer
        || !constant_time_eq(&token_hash, candidate.as_slice())
    {
        return Err(MessageStoreError::InvalidReceipt {
            delivery_id: receipt.delivery_id.clone(),
        });
    }
    Ok(delivery)
}

fn require_current_receipt(delivery: &DeliveryRecord, receipt: &LeaseReceipt) -> Result<()> {
    if delivery.lease_generation == receipt.generation {
        Ok(())
    } else {
        Err(MessageStoreError::InvalidReceipt {
            delivery_id: receipt.delivery_id.clone(),
        })
    }
}

fn require_live_state(
    delivery: &DeliveryRecord,
    receipt: &LeaseReceipt,
    now: DateTime<Utc>,
    operation: &'static str,
    delivered_only: bool,
) -> Result<()> {
    let valid_state = if delivered_only {
        delivery.status == DeliveryStatus::Delivered
    } else {
        delivery.status.holds_lease()
    };
    if !valid_state {
        return Err(MessageStoreError::InvalidState {
            delivery_id: receipt.delivery_id.clone(),
            operation,
            state: delivery.status,
        });
    }
    if delivery
        .lease_expires_at
        .is_none_or(|expires_at| expires_at <= now)
    {
        return Err(MessageStoreError::LeaseExpired {
            delivery_id: receipt.delivery_id.clone(),
        });
    }
    if delivery
        .expires_at
        .is_some_and(|expires_at| expires_at <= now)
    {
        return Err(MessageStoreError::LeaseExpired {
            delivery_id: receipt.delivery_id.clone(),
        });
    }
    Ok(())
}

fn renew_in_transaction(
    connection: &Connection,
    before: &DeliveryRecord,
    receipt: &LeaseReceipt,
    now: DateTime<Utc>,
    lease_seconds: u64,
) -> Result<DeliveryRecord> {
    require_live_state(before, receipt, now, "renew", false)?;
    let expires_at = add_seconds(now, lease_seconds, "lease duration")?;
    if before.expires_at.is_some_and(|ttl| expires_at > ttl) {
        return Err(MessageStoreError::InvalidInput(
            "renewed lease must end before delivery expiry".into(),
        ));
    }
    let current_expiry = before.lease_expires_at.ok_or_else(|| {
        MessageStoreError::CorruptData("leased delivery lacks lease expiry".into())
    })?;
    if expires_at <= current_expiry {
        return Ok(before.clone());
    }
    let revision = next_u64(before.revision, "delivery revision")?;
    let changed = connection.execute(
        "UPDATE deliveries SET lease_expires_at_ms = ?1, revision = ?2, updated_at_ms = ?3 \
         WHERE owner = ?4 AND id = ?5 AND revision = ?6 AND lease_generation = ?7 \
           AND status IN ('leased', 'delivered')",
        params![
            expires_at.timestamp_millis(),
            u64_to_i64(revision, "delivery revision")?,
            now.timestamp_millis(),
            before.owner,
            before.id,
            u64_to_i64(before.revision, "delivery revision")?,
            u64_to_i64(before.lease_generation, "lease generation")?
        ],
    )?;
    ensure_one_change(changed, "renew delivery")?;
    let after =
        get_delivery(connection, &before.owner, &before.id)?.ok_or(MessageStoreError::NotFound)?;
    let message = get_message(connection, &before.owner, &before.message_id)?
        .ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        MessageEventKind::LeaseRenewed,
        Some(before.status),
        now,
    )?;
    Ok(after)
}

fn mark_delivered_in_transaction(
    connection: &Connection,
    before: &DeliveryRecord,
    receipt: &LeaseReceipt,
    now: DateTime<Utc>,
) -> Result<DeliveryRecord> {
    require_live_state(before, receipt, now, "mark delivered", false)?;
    if before.status != DeliveryStatus::Leased {
        return Err(MessageStoreError::InvalidState {
            delivery_id: before.id.clone(),
            operation: "mark delivered",
            state: before.status,
        });
    }
    let revision = next_u64(before.revision, "delivery revision")?;
    let changed = connection.execute(
        "UPDATE deliveries SET status = 'delivered', revision = ?1, updated_at_ms = ?2, \
            first_delivered_at_ms = COALESCE(first_delivered_at_ms, ?2) \
         WHERE owner = ?3 AND id = ?4 AND revision = ?5 AND status = 'leased'",
        params![
            u64_to_i64(revision, "delivery revision")?,
            now.timestamp_millis(),
            before.owner,
            before.id,
            u64_to_i64(before.revision, "delivery revision")?
        ],
    )?;
    ensure_one_change(changed, "mark delivery delivered")?;
    let after =
        get_delivery(connection, &before.owner, &before.id)?.ok_or(MessageStoreError::NotFound)?;
    let message = get_message(connection, &before.owner, &before.message_id)?
        .ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        MessageEventKind::Delivered,
        Some(before.status),
        now,
    )?;
    Ok(after)
}

fn mark_transport_delivered_in_transaction(
    connection: &Connection,
    before: &DeliveryRecord,
    receipt: &LeaseReceipt,
    now: DateTime<Utc>,
) -> Result<DeliveryRecord> {
    let reclaimed_pending = if before.status == DeliveryStatus::Pending {
        let latest_event: Option<String> = connection
            .query_row(
                "SELECT event_type FROM message_events \
                 WHERE owner = ?1 AND delivery_id = ?2 \
                 ORDER BY sequence DESC LIMIT 1",
                params![before.owner, before.id],
                |row| row.get(0),
            )
            .optional()?;
        latest_event.as_deref() == Some(MessageEventKind::Reclaimed.as_str())
    } else {
        false
    };
    if before.status != DeliveryStatus::Leased && !reclaimed_pending {
        return Err(MessageStoreError::InvalidState {
            delivery_id: before.id.clone(),
            operation: "record external transport delivery",
            state: before.status,
        });
    }
    let revision = next_u64(before.revision, "delivery revision")?;
    let changed = connection.execute(
        "UPDATE deliveries SET status = 'delivered', revision = ?1, updated_at_ms = ?2, \
            first_delivered_at_ms = COALESCE(first_delivered_at_ms, ?2), \
            lease_owner = COALESCE(lease_owner, ( \
                SELECT consumer FROM delivery_attempts \
                WHERE owner = ?3 AND delivery_id = ?4 AND generation = ?7 \
            )), \
            lease_token_hash = COALESCE(lease_token_hash, ( \
                SELECT token_hash FROM delivery_attempts \
                WHERE owner = ?3 AND delivery_id = ?4 AND generation = ?7 \
            )), \
            lease_expires_at_ms = COALESCE(lease_expires_at_ms, ( \
                SELECT initial_expires_at_ms FROM delivery_attempts \
                WHERE owner = ?3 AND delivery_id = ?4 AND generation = ?7 \
            )) \
         WHERE owner = ?3 AND id = ?4 AND revision = ?5 AND lease_generation = ?6 \
           AND status IN ('leased', 'pending')",
        params![
            u64_to_i64(revision, "delivery revision")?,
            now.timestamp_millis(),
            before.owner,
            before.id,
            u64_to_i64(before.revision, "delivery revision")?,
            u64_to_i64(before.lease_generation, "lease generation")?,
            u64_to_i64(receipt.generation, "receipt generation")?
        ],
    )?;
    ensure_one_change(changed, "mark transport delivery delivered")?;
    let after =
        get_delivery(connection, &before.owner, &before.id)?.ok_or(MessageStoreError::NotFound)?;
    let message = get_message(connection, &before.owner, &before.message_id)?
        .ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        MessageEventKind::Delivered,
        Some(before.status),
        now,
    )?;
    Ok(after)
}

fn acknowledge_in_transaction(
    connection: &Connection,
    before: &DeliveryRecord,
    now: DateTime<Utc>,
) -> Result<DeliveryRecord> {
    if before.status == DeliveryStatus::Delivered
        && has_transport_receipt(connection, &before.owner, &before.id)?
    {
        return acknowledge_transport_delivery(connection, before, now);
    }
    let synthetic_receipt = LeaseReceipt {
        delivery_id: before.id.clone(),
        generation: before.lease_generation,
        mailbox: DeliveryMailbox {
            route: before.route.clone(),
            target: before.target.clone(),
        },
        consumer: before.lease_owner.clone().unwrap_or_default(),
        token: "0".repeat(64),
    };
    require_live_state(before, &synthetic_receipt, now, "acknowledge", true)?;
    update_delivery_state(
        connection,
        before,
        DeliveryStatus::Acknowledged,
        now,
        before.available_at,
        None,
    )?;
    connection.execute(
        "UPDATE deliveries SET acknowledged_at_ms = ?1 WHERE owner = ?2 AND id = ?3",
        params![now.timestamp_millis(), before.owner, before.id],
    )?;
    let after =
        get_delivery(connection, &before.owner, &before.id)?.ok_or(MessageStoreError::NotFound)?;
    let message = get_message(connection, &before.owner, &before.message_id)?
        .ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        MessageEventKind::Acknowledged,
        Some(before.status),
        now,
    )?;
    Ok(after)
}

fn acknowledge_transport_delivery(
    connection: &Connection,
    before: &DeliveryRecord,
    now: DateTime<Utc>,
) -> Result<DeliveryRecord> {
    if before.status != DeliveryStatus::Delivered
        || !has_transport_receipt(connection, &before.owner, &before.id)?
    {
        return Err(MessageStoreError::CorruptData(
            "transport acknowledgement requires an externally delivered record".into(),
        ));
    }
    let now = now.max(before.updated_at);
    update_delivery_state(
        connection,
        before,
        DeliveryStatus::Acknowledged,
        now,
        before.available_at,
        None,
    )?;
    connection.execute(
        "UPDATE deliveries SET acknowledged_at_ms = ?1 WHERE owner = ?2 AND id = ?3",
        params![now.timestamp_millis(), before.owner, before.id],
    )?;
    let after =
        get_delivery(connection, &before.owner, &before.id)?.ok_or(MessageStoreError::NotFound)?;
    let message = get_message(connection, &before.owner, &before.message_id)?
        .ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        MessageEventKind::Acknowledged,
        Some(before.status),
        now,
    )?;
    Ok(after)
}

fn nack_in_transaction(
    connection: &Connection,
    before: &DeliveryRecord,
    receipt: &LeaseReceipt,
    now: DateTime<Utc>,
    disposition: &NackDisposition,
) -> Result<DeliveryRecord> {
    require_live_state(before, receipt, now, "nack", false)?;
    if has_transport_receipt(connection, &before.owner, &before.id)? {
        return Err(MessageStoreError::InvalidState {
            delivery_id: before.id.clone(),
            operation: "nack an externally delivered delivery",
            state: before.status,
        });
    }
    let (status, available_at, failure_code, event_kind) = match disposition {
        NackDisposition::RetryAfter { delay_seconds }
            if before.attempt_count < before.max_attempts
                && before.expires_at.is_none_or(|expires| expires > now) =>
        {
            (
                DeliveryStatus::Pending,
                add_seconds(now, *delay_seconds, "retry delay")?,
                None,
                MessageEventKind::Nacked,
            )
        }
        NackDisposition::RetryAfter { .. } => (
            DeliveryStatus::DeadLettered,
            before.available_at,
            Some("attempts_exhausted".to_string()),
            MessageEventKind::DeadLettered,
        ),
        NackDisposition::Permanent { failure_code } => (
            DeliveryStatus::DeadLettered,
            before.available_at,
            Some(failure_code.clone()),
            MessageEventKind::DeadLettered,
        ),
    };
    if before
        .expires_at
        .is_some_and(|expires| available_at >= expires)
        && status == DeliveryStatus::Pending
    {
        return Err(MessageStoreError::InvalidInput(
            "retry delay reaches or exceeds delivery expiry".into(),
        ));
    }
    update_delivery_state(
        connection,
        before,
        status,
        now,
        available_at,
        failure_code.as_deref(),
    )?;
    if status == DeliveryStatus::DeadLettered {
        connection.execute(
            "UPDATE deliveries SET dead_lettered_at_ms = ?1 WHERE owner = ?2 AND id = ?3",
            params![now.timestamp_millis(), before.owner, before.id],
        )?;
    }
    let after =
        get_delivery(connection, &before.owner, &before.id)?.ok_or(MessageStoreError::NotFound)?;
    let message = get_message(connection, &before.owner, &before.message_id)?
        .ok_or(MessageStoreError::NotFound)?;
    insert_event(
        connection,
        &message,
        &after,
        event_kind,
        Some(before.status),
        now,
    )?;
    Ok(after)
}

fn update_delivery_state(
    connection: &Connection,
    before: &DeliveryRecord,
    status: DeliveryStatus,
    now: DateTime<Utc>,
    available_at: DateTime<Utc>,
    failure_code: Option<&str>,
) -> Result<()> {
    let revision = next_u64(before.revision, "delivery revision")?;
    let changed = connection.execute(
        "UPDATE deliveries SET status = ?1, available_at_ms = ?2, revision = ?3, \
            lease_owner = NULL, lease_token_hash = NULL, lease_expires_at_ms = NULL, \
            updated_at_ms = ?4, failure_code = ?5 \
         WHERE owner = ?6 AND id = ?7 AND revision = ?8",
        params![
            status.as_str(),
            available_at.timestamp_millis(),
            u64_to_i64(revision, "delivery revision")?,
            now.timestamp_millis(),
            failure_code,
            before.owner,
            before.id,
            u64_to_i64(before.revision, "delivery revision")?
        ],
    )?;
    ensure_one_change(changed, "transition delivery")
}

fn reclaim_expired_in_transaction(
    connection: &Connection,
    owner: &str,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<usize> {
    let mut statement = connection.prepare(
        "SELECT id FROM deliveries WHERE owner = ?1 AND status IN ('leased', 'delivered') \
         AND lease_expires_at_ms <= ?2 AND EXISTS (SELECT 1 FROM message_publications p \
             WHERE p.owner = deliveries.owner AND p.message_id = deliveries.message_id \
               AND p.status = 'published') \
         ORDER BY lease_expires_at_ms ASC, id ASC LIMIT ?3",
    )?;
    let ids = statement
        .query_map(
            params![
                owner,
                now.timestamp_millis(),
                usize_to_i64(limit, "reclaim limit")?
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    for id in &ids {
        let before = get_delivery(connection, owner, id)?.ok_or(MessageStoreError::NotFound)?;
        if before.status == DeliveryStatus::Delivered
            && has_transport_receipt(connection, owner, id)?
        {
            acknowledge_transport_delivery(connection, &before, now)?;
            continue;
        }
        let transition_now = now.max(before.updated_at);
        let (status, failure_code, kind) = if before.expires_at.is_some_and(|at| at <= now) {
            (DeliveryStatus::Expired, None, MessageEventKind::Expired)
        } else if before.attempt_count >= before.max_attempts {
            (
                DeliveryStatus::DeadLettered,
                Some("attempts_exhausted"),
                MessageEventKind::DeadLettered,
            )
        } else {
            (DeliveryStatus::Pending, None, MessageEventKind::Reclaimed)
        };
        update_delivery_state(
            connection,
            &before,
            status,
            transition_now,
            transition_now,
            failure_code,
        )?;
        if status == DeliveryStatus::DeadLettered {
            connection.execute(
                "UPDATE deliveries SET dead_lettered_at_ms = ?1 WHERE owner = ?2 AND id = ?3",
                params![transition_now.timestamp_millis(), owner, id],
            )?;
        }
        let after = get_delivery(connection, owner, id)?.ok_or(MessageStoreError::NotFound)?;
        let message = get_message(connection, owner, &before.message_id)?
            .ok_or(MessageStoreError::NotFound)?;
        insert_event(
            connection,
            &message,
            &after,
            kind,
            Some(before.status),
            transition_now,
        )?;
    }
    Ok(ids.len())
}

fn expire_due_in_transaction(
    connection: &Connection,
    owner: &str,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<usize> {
    let mut statement = connection.prepare(
        "SELECT id FROM deliveries WHERE owner = ?1 \
         AND status IN ('pending', 'leased', 'delivered') \
         AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?2 \
         AND EXISTS (SELECT 1 FROM message_publications p \
             WHERE p.owner = deliveries.owner AND p.message_id = deliveries.message_id \
               AND p.status = 'published') \
         ORDER BY expires_at_ms ASC, id ASC LIMIT ?3",
    )?;
    let ids = statement
        .query_map(
            params![
                owner,
                now.timestamp_millis(),
                usize_to_i64(limit, "expiry limit")?
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    for id in &ids {
        let before = get_delivery(connection, owner, id)?.ok_or(MessageStoreError::NotFound)?;
        if before.status == DeliveryStatus::Delivered
            && has_transport_receipt(connection, owner, id)?
        {
            acknowledge_transport_delivery(connection, &before, now)?;
            continue;
        }
        let transition_now = now.max(before.updated_at);
        update_delivery_state(
            connection,
            &before,
            DeliveryStatus::Expired,
            transition_now,
            before.available_at,
            None,
        )?;
        let after = get_delivery(connection, owner, id)?.ok_or(MessageStoreError::NotFound)?;
        let message = get_message(connection, owner, &before.message_id)?
            .ok_or(MessageStoreError::NotFound)?;
        insert_event(
            connection,
            &message,
            &after,
            MessageEventKind::Expired,
            Some(before.status),
            transition_now,
        )?;
    }
    Ok(ids.len())
}

fn stored_delivery_operation(
    connection: &Connection,
    owner: &str,
    receipt: &LeaseReceipt,
    operation_key: &str,
) -> Result<Option<DeliveryRecord>> {
    let raw: Option<String> = connection
        .query_row(
            "SELECT result_json FROM receipt_operations \
             WHERE owner = ?1 AND delivery_id = ?2 AND generation = ?3 AND operation_key = ?4",
            params![
                owner,
                receipt.delivery_id,
                u64_to_i64(receipt.generation, "lease generation")?,
                operation_key
            ],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|value| {
        let result = serde_json::from_str::<DeliveryRecord>(&value).map_err(|_| {
            MessageStoreError::CorruptData("receipt operation result is invalid".into())
        })?;
        validate_stored_delivery(&result).map_err(|_| {
            MessageStoreError::CorruptData(
                "receipt operation result violates the delivery contract".into(),
            )
        })?;
        if result.owner != owner
            || result.id != receipt.delivery_id
            || result.lease_generation != receipt.generation
        {
            return Err(MessageStoreError::CorruptData(
                "receipt operation result has mismatched authority or generation".into(),
            ));
        }
        Ok(result)
    })
    .transpose()
}

fn store_delivery_operation(
    connection: &Connection,
    owner: &str,
    receipt: &LeaseReceipt,
    operation_key: &str,
    result: &DeliveryRecord,
    now: DateTime<Utc>,
) -> Result<()> {
    let result_json = serde_json::to_string(result)
        .map_err(|_| MessageStoreError::CorruptData("cannot serialize receipt result".into()))?;
    connection.execute(
        "INSERT INTO receipt_operations (owner, delivery_id, generation, operation_key, \
            result_json, completed_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            owner,
            receipt.delivery_id,
            u64_to_i64(receipt.generation, "lease generation")?,
            operation_key,
            result_json,
            now.timestamp_millis()
        ],
    )?;
    Ok(())
}

fn stored_reply_operation(
    connection: &Connection,
    owner: &str,
    receipt: &LeaseReceipt,
    operation_key: &str,
) -> Result<Option<StoredReplyOperation>> {
    let raw: Option<String> = connection
        .query_row(
            "SELECT result_json FROM receipt_operations \
             WHERE owner = ?1 AND delivery_id = ?2 AND generation = ?3 AND operation_key = ?4",
            params![
                owner,
                receipt.delivery_id,
                u64_to_i64(receipt.generation, "lease generation")?,
                operation_key
            ],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|value| {
        let result = serde_json::from_str::<StoredReplyOperation>(&value).map_err(|_| {
            MessageStoreError::CorruptData("reply receipt operation is invalid".into())
        })?;
        validate_stored_delivery(&result.delivery).map_err(|_| {
            MessageStoreError::CorruptData(
                "reply receipt operation violates the delivery contract".into(),
            )
        })?;
        validate_uuid_v7("reply receipt message id", &result.reply_message_id)?;
        if result.delivery.owner != owner
            || result.delivery.id != receipt.delivery_id
            || result.delivery.lease_generation != receipt.generation
        {
            return Err(MessageStoreError::CorruptData(
                "reply receipt operation has mismatched authority or generation".into(),
            ));
        }
        Ok(result)
    })
    .transpose()
}

#[allow(clippy::too_many_arguments)]
fn store_reply_operation(
    connection: &Connection,
    owner: &str,
    receipt: &LeaseReceipt,
    operation_key: &str,
    delivery: &DeliveryRecord,
    reply_message_id: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let result_json = serde_json::to_string(&StoredReplyOperation {
        delivery: delivery.clone(),
        reply_message_id: reply_message_id.to_string(),
    })
    .map_err(|_| MessageStoreError::CorruptData("cannot serialize reply receipt".into()))?;
    connection.execute(
        "INSERT INTO receipt_operations (owner, delivery_id, generation, operation_key, \
            result_json, completed_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            owner,
            receipt.delivery_id,
            u64_to_i64(receipt.generation, "lease generation")?,
            operation_key,
            result_json,
            now.timestamp_millis()
        ],
    )?;
    Ok(())
}

fn reject_renew_operation_drift(
    connection: &Connection,
    owner: &str,
    receipt: &LeaseReceipt,
    operation_digest: &str,
    requested_key: &str,
) -> Result<()> {
    let pattern = format!("renew:{operation_digest}:*");
    let conflict: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM receipt_operations \
         WHERE owner = ?1 AND delivery_id = ?2 AND generation = ?3 \
           AND operation_key GLOB ?4 AND operation_key <> ?5)",
        params![
            owner,
            receipt.delivery_id,
            u64_to_i64(receipt.generation, "lease generation")?,
            pattern,
            requested_key
        ],
        |row| row.get(0),
    )?;
    if conflict {
        Err(MessageStoreError::ReceiptOperationConflict {
            delivery_id: receipt.delivery_id.clone(),
        })
    } else {
        Ok(())
    }
}

fn reject_conflicting_terminal_operation(
    connection: &Connection,
    owner: &str,
    receipt: &LeaseReceipt,
    requested: &str,
) -> Result<()> {
    let conflict: bool = if requested == "ack" {
        connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM receipt_operations \
             WHERE owner = ?1 AND delivery_id = ?2 AND generation = ?3 \
               AND operation_key LIKE 'nack:%')",
            params![
                owner,
                receipt.delivery_id,
                u64_to_i64(receipt.generation, "lease generation")?
            ],
            |row| row.get(0),
        )?
    } else {
        connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM receipt_operations \
             WHERE owner = ?1 AND delivery_id = ?2 AND generation = ?3 \
               AND (operation_key = 'acknowledged' OR operation_key LIKE 'reply_and_ack:%'))",
            params![
                owner,
                receipt.delivery_id,
                u64_to_i64(receipt.generation, "lease generation")?
            ],
            |row| row.get(0),
        )?
    };
    if conflict {
        Err(MessageStoreError::InvalidState {
            delivery_id: receipt.delivery_id.clone(),
            operation: "apply a conflicting terminal receipt operation",
            state: get_delivery(connection, owner, &receipt.delivery_id)?
                .ok_or(MessageStoreError::NotFound)?
                .status,
        })
    } else {
        Ok(())
    }
}

fn row_to_message(row: &Row<'_>) -> rusqlite::Result<MessageRecord> {
    let payload_raw: String = row.get(10)?;
    let payload: serde_json::Value = serde_json::from_str(&payload_raw)
        .map_err(|_| data_error(10, Type::Text, "invalid message payload JSON"))?;
    let direction_raw: String = row.get(5)?;
    let sender_kind_raw: String = row.get(7)?;
    let schema: i64 = row.get(18)?;
    if schema != i64::from(RECORD_SCHEMA) {
        return Err(data_error(
            18,
            Type::Integer,
            "unsupported message record schema",
        ));
    }
    let record = MessageRecord {
        id: row.get(0)?,
        owner: row.get(1)?,
        conversation_id: row.get(2)?,
        conversation_sequence: stored_u64(row, 3, "conversation sequence")?,
        session_id: row.get(4)?,
        direction: parse_enum(5, &direction_raw)?,
        kind: row.get(6)?,
        sender: EndpointRef {
            kind: parse_enum(7, &sender_kind_raw)?,
            id: row.get(8)?,
        },
        body: row.get(9)?,
        payload,
        reply_to: row.get(11)?,
        trace_id: row.get(12)?,
        correlation_id: row.get(13)?,
        idempotency: IdempotencyKey {
            producer: row.get(14)?,
            key: row.get(15)?,
        },
        request_digest: row.get(16)?,
        created_at: stored_timestamp(row, 17, "message created_at")?,
    };
    validate_stored_message(&record).map_err(|_| {
        data_error(
            0,
            Type::Text,
            "stored message violates its bounded contract",
        )
    })?;
    Ok(record)
}

fn row_to_delivery(row: &Row<'_>) -> rusqlite::Result<DeliveryRecord> {
    let target_kind_raw: String = row.get(4)?;
    let status_raw: String = row.get(6)?;
    let schema: i64 = row.get(21)?;
    if schema != i64::from(RECORD_SCHEMA) {
        return Err(data_error(
            21,
            Type::Integer,
            "unsupported delivery record schema",
        ));
    }
    let record = DeliveryRecord {
        id: row.get(0)?,
        owner: row.get(1)?,
        message_id: row.get(2)?,
        route: row.get(3)?,
        target: EndpointRef {
            kind: parse_enum(4, &target_kind_raw)?,
            id: row.get(5)?,
        },
        status: parse_enum(6, &status_raw)?,
        available_at: stored_timestamp(row, 7, "delivery available_at")?,
        expires_at: optional_timestamp(row, 8, "delivery expires_at")?,
        attempt_count: stored_u32(row, 9, "attempt count")?,
        max_attempts: stored_u32(row, 10, "max attempts")?,
        revision: stored_u64(row, 11, "delivery revision")?,
        lease_generation: stored_u64(row, 12, "lease generation")?,
        lease_owner: row.get(13)?,
        lease_expires_at: optional_timestamp(row, 14, "lease expires_at")?,
        first_delivered_at: optional_timestamp(row, 15, "first delivered_at")?,
        acknowledged_at: optional_timestamp(row, 16, "acknowledged_at")?,
        dead_lettered_at: optional_timestamp(row, 17, "dead_lettered_at")?,
        failure_code: row.get(18)?,
        created_at: stored_timestamp(row, 19, "delivery created_at")?,
        updated_at: stored_timestamp(row, 20, "delivery updated_at")?,
    };
    validate_stored_delivery(&record).map_err(|_| {
        data_error(
            0,
            Type::Text,
            "stored delivery violates its lifecycle contract",
        )
    })?;
    Ok(record)
}

fn row_to_event(row: &Row<'_>) -> rusqlite::Result<MessageEvent> {
    let event_type_raw: String = row.get(9)?;
    let from_status_raw: Option<String> = row.get(10)?;
    let to_status_raw: String = row.get(11)?;
    let target_kind_raw: String = row.get(14)?;
    let direction_raw: String = row.get(16)?;
    let event = MessageEvent {
        sequence: stored_u64(row, 0, "event sequence")?,
        event_id: row.get(1)?,
        owner: row.get(2)?,
        message_id: row.get(3)?,
        delivery_id: row.get(4)?,
        delivery_revision: stored_u64(row, 5, "event delivery revision")?,
        conversation_id: row.get(6)?,
        conversation_sequence: stored_u64(row, 7, "event conversation sequence")?,
        occurred_at: stored_timestamp(row, 8, "event occurred_at")?,
        kind: parse_enum(9, &event_type_raw)?,
        from_status: from_status_raw
            .as_deref()
            .map(|value| parse_enum(10, value))
            .transpose()?,
        to_status: parse_enum(11, &to_status_raw)?,
        lease_generation: stored_u64(row, 12, "event lease generation")?,
        route: row.get(13)?,
        target: EndpointRef {
            kind: parse_enum(14, &target_kind_raw)?,
            id: row.get(15)?,
        },
        direction: parse_enum(16, &direction_raw)?,
        reply_to: row.get(17)?,
    };
    validate_stored_event(&event)
        .map_err(|_| data_error(0, Type::Text, "stored event violates its contract"))?;
    Ok(event)
}

fn row_to_transport_receipt(row: &Row<'_>) -> rusqlite::Result<(TransportReceiptRecord, String)> {
    let digest: String = row.get(9)?;
    let schema: i64 = row.get(10)?;
    if schema != i64::from(RECORD_SCHEMA) {
        return Err(data_error(
            10,
            Type::Integer,
            "unsupported transport receipt record schema",
        ));
    }
    let record = TransportReceiptRecord {
        owner: row.get(0)?,
        delivery_id: row.get(1)?,
        generation: stored_u64(row, 2, "transport receipt generation")?,
        ordinal: stored_u32(row, 3, "transport receipt ordinal")?,
        transport: row.get(4)?,
        account_scope: row.get(5)?,
        destination_scope: row.get(6)?,
        external_id: row.get(7)?,
        recorded_at: stored_timestamp(row, 8, "transport receipt recorded_at")?,
    };
    validate_stored_transport_receipt(&record, &digest).map_err(|_| {
        data_error(
            0,
            Type::Text,
            "stored transport receipt violates its contract",
        )
    })?;
    Ok((record, digest))
}

fn validate_stored_message(record: &MessageRecord) -> Result<()> {
    validate_uuid_v7("message id", &record.id)?;
    validate_owner(&record.owner)?;
    validate_text("conversation id", &record.conversation_id, 256)?;
    if record.conversation_sequence == 0 {
        return Err(MessageStoreError::CorruptData(
            "conversation sequence is zero".into(),
        ));
    }
    validate_optional_text("session id", record.session_id.as_deref(), 256)?;
    validate_text("message kind", &record.kind, 128)?;
    record.sender.validate("sender id")?;
    if record.body.len() > crate::model::MAX_BODY_BYTES || record.body.contains('\0') {
        return Err(MessageStoreError::CorruptData(
            "stored body violates its bound".into(),
        ));
    }
    validate_payload(&record.payload)?;
    if record.body.is_empty() && record.payload.is_null() {
        return Err(MessageStoreError::CorruptData(
            "stored message body and payload are both empty".into(),
        ));
    }
    if let Some(reply_to) = record.reply_to.as_deref() {
        validate_uuid_v7("message reply_to", reply_to)?;
    }
    validate_optional_text("trace id", record.trace_id.as_deref(), 256)?;
    validate_optional_text("correlation id", record.correlation_id.as_deref(), 256)?;
    record.idempotency.validate()?;
    if record.request_digest.len() != 64
        || !record
            .request_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MessageStoreError::CorruptData(
            "stored request digest is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_stored_delivery(record: &DeliveryRecord) -> Result<()> {
    validate_uuid_v7("delivery id", &record.id)?;
    validate_owner(&record.owner)?;
    validate_uuid_v7("delivery message id", &record.message_id)?;
    validate_text("delivery route", &record.route, 256)?;
    record.target.validate("delivery target id")?;
    if record.max_attempts == 0
        || record.max_attempts > 100
        || record.attempt_count > record.max_attempts
    {
        return Err(MessageStoreError::CorruptData(
            "stored delivery attempt counters are invalid".into(),
        ));
    }
    if u64::from(record.attempt_count) != record.lease_generation {
        return Err(MessageStoreError::CorruptData(
            "stored delivery attempt count and lease generation diverge".into(),
        ));
    }
    validate_optional_text("delivery lease owner", record.lease_owner.as_deref(), 256)?;
    if record.updated_at < record.created_at {
        return Err(MessageStoreError::CorruptData(
            "stored delivery updated_at predates creation".into(),
        ));
    }
    for (label, value) in [
        ("first delivered_at", record.first_delivered_at),
        ("acknowledged_at", record.acknowledged_at),
        ("dead_lettered_at", record.dead_lettered_at),
    ] {
        if value.is_some_and(|value| value < record.created_at || value > record.updated_at) {
            return Err(MessageStoreError::CorruptData(format!(
                "stored delivery {label} is outside its lifecycle"
            )));
        }
    }
    let has_lease = record.lease_owner.is_some() && record.lease_expires_at.is_some();
    if record.status.holds_lease() != has_lease {
        return Err(MessageStoreError::CorruptData(
            "stored delivery lease fields contradict status".into(),
        ));
    }
    if record.status == DeliveryStatus::Leased
        && record
            .lease_expires_at
            .is_none_or(|expires_at| expires_at <= record.updated_at)
    {
        return Err(MessageStoreError::CorruptData(
            "stored delivery lease is not live at its update time".into(),
        ));
    }
    if record.status.holds_lease()
        && record.expires_at.is_some_and(|ttl| {
            record
                .lease_expires_at
                .is_some_and(|lease_expires_at| lease_expires_at > ttl)
        })
    {
        return Err(MessageStoreError::CorruptData(
            "stored delivery lease exceeds delivery expiry".into(),
        ));
    }
    if matches!(
        record.status,
        DeliveryStatus::Delivered | DeliveryStatus::Acknowledged
    ) && record.first_delivered_at.is_none()
    {
        return Err(MessageStoreError::CorruptData(
            "delivered lifecycle lacks first delivery timestamp".into(),
        ));
    }
    if (record.status == DeliveryStatus::Acknowledged) != record.acknowledged_at.is_some() {
        return Err(MessageStoreError::CorruptData(
            "acknowledged delivery timestamp contradicts status".into(),
        ));
    }
    if (record.status == DeliveryStatus::DeadLettered) != record.dead_lettered_at.is_some() {
        return Err(MessageStoreError::CorruptData(
            "dead-lettered delivery timestamp contradicts status".into(),
        ));
    }
    if record.status == DeliveryStatus::DeadLettered {
        validate_text(
            "failure code",
            record.failure_code.as_deref().unwrap_or_default(),
            128,
        )?;
    } else if record.failure_code.is_some() {
        return Err(MessageStoreError::CorruptData(
            "failure code is present outside dead-letter state".into(),
        ));
    }
    Ok(())
}

fn validate_stored_event(event: &MessageEvent) -> Result<()> {
    if event.sequence == 0 || event.conversation_sequence == 0 {
        return Err(MessageStoreError::CorruptData(
            "stored event sequence is zero".into(),
        ));
    }
    validate_uuid_v7("event id", &event.event_id)?;
    validate_owner(&event.owner)?;
    validate_uuid_v7("event message id", &event.message_id)?;
    validate_uuid_v7("event delivery id", &event.delivery_id)?;
    validate_text("event route", &event.route, 256)?;
    event.target.validate("event target id")?;
    if let Some(reply_to) = event.reply_to.as_deref() {
        validate_uuid_v7("event reply_to", reply_to)?;
    }
    let transition_is_valid = match (event.kind, event.from_status, event.to_status) {
        (MessageEventKind::Enqueued, None, DeliveryStatus::Pending) => true,
        (MessageEventKind::Leased, Some(DeliveryStatus::Pending), DeliveryStatus::Leased) => true,
        (MessageEventKind::LeaseRenewed, Some(from), to) => from == to && to.holds_lease(),
        (
            MessageEventKind::Delivered,
            Some(DeliveryStatus::Leased | DeliveryStatus::Pending),
            DeliveryStatus::Delivered,
        ) => true,
        (
            MessageEventKind::Acknowledged,
            Some(DeliveryStatus::Delivered),
            DeliveryStatus::Acknowledged,
        ) => true,
        (
            MessageEventKind::Nacked | MessageEventKind::Reclaimed,
            Some(DeliveryStatus::Leased | DeliveryStatus::Delivered),
            DeliveryStatus::Pending,
        ) => true,
        (
            MessageEventKind::DeadLettered,
            Some(DeliveryStatus::Leased | DeliveryStatus::Delivered),
            DeliveryStatus::DeadLettered,
        ) => true,
        (
            MessageEventKind::Expired,
            Some(DeliveryStatus::Pending | DeliveryStatus::Leased | DeliveryStatus::Delivered),
            DeliveryStatus::Expired,
        ) => true,
        (
            MessageEventKind::Cancelled,
            Some(DeliveryStatus::Pending | DeliveryStatus::Leased | DeliveryStatus::Delivered),
            DeliveryStatus::Cancelled,
        ) => true,
        _ => false,
    };
    if !transition_is_valid {
        return Err(MessageStoreError::CorruptData(
            "event kind contradicts its lifecycle statuses".into(),
        ));
    }
    Ok(())
}

fn validate_stored_transport_receipt(record: &TransportReceiptRecord, digest: &str) -> Result<()> {
    validate_owner(&record.owner)?;
    validate_uuid_v7("transport receipt delivery id", &record.delivery_id)?;
    if record.generation == 0 {
        return Err(MessageStoreError::CorruptData(
            "transport receipt generation is zero".into(),
        ));
    }
    if record.ordinal >= 128 {
        return Err(MessageStoreError::CorruptData(
            "transport receipt ordinal exceeds its bound".into(),
        ));
    }
    let request = NewTransportReceipt {
        transport: record.transport.clone(),
        account_scope: record.account_scope.clone(),
        destination_scope: record.destination_scope.clone(),
        external_ids: vec![record.external_id.clone()],
    };
    request.validate()?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MessageStoreError::CorruptData(
            "transport receipt digest is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_uuid_v7(label: &str, value: &str) -> Result<()> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| MessageStoreError::CorruptData(format!("stored {label} is not a UUID")))?;
    if parsed.get_version_num() != 7 || parsed.to_string() != value {
        return Err(MessageStoreError::CorruptData(format!(
            "stored {label} is not a canonical UUIDv7"
        )));
    }
    Ok(())
}

fn parse_enum<T>(index: usize, raw: &str) -> rusqlite::Result<T>
where
    T: FromStr<Err = MessageStoreError>,
{
    raw.parse()
        .map_err(|_| data_error(index, Type::Text, "unknown stored enum value"))
}

fn stored_timestamp(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<DateTime<Utc>> {
    let value: i64 = row.get(index)?;
    DateTime::<Utc>::from_timestamp_millis(value)
        .ok_or_else(|| data_error(index, Type::Integer, label))
}

fn optional_timestamp(
    row: &Row<'_>,
    index: usize,
    label: &str,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let value: Option<i64> = row.get(index)?;
    value
        .map(|value| {
            DateTime::<Utc>::from_timestamp_millis(value)
                .ok_or_else(|| data_error(index, Type::Integer, label))
        })
        .transpose()
}

fn stored_u64(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| data_error(index, Type::Integer, label))
}

fn stored_u32(row: &Row<'_>, index: usize, label: &str) -> rusqlite::Result<u32> {
    let value: i64 = row.get(index)?;
    u32::try_from(value).map_err(|_| data_error(index, Type::Integer, label))
}

fn data_error(index: usize, value_type: Type, message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        value_type,
        Box::new(MessageStoreError::CorruptData(message.into())),
    )
}

fn configure_connection(connection: &Connection) -> Result<()> {
    connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, SQLITE_VALUE_LIMIT)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    Ok(())
}

fn user_version(connection: &Connection) -> Result<u32> {
    let value: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    u32::try_from(value)
        .map_err(|_| MessageStoreError::CorruptData("database user_version is out of range".into()))
}

fn validate_schema_definition(connection: &Connection) -> Result<()> {
    let actual = schema_manifest(connection)?;
    let expected = expected_schema()?;
    if actual != expected {
        return Err(MessageStoreError::CorruptData(
            "database schema differs from the supported migration manifest".into(),
        ));
    }
    Ok(())
}

fn audit_database_integrity(connection: &Connection) -> Result<()> {
    let quick_check: String =
        connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick_check != "ok" {
        return Err(MessageStoreError::CorruptData(
            "SQLite quick_check failed".into(),
        ));
    }
    let foreign_key_errors: i64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        return Err(MessageStoreError::CorruptData(
            "database contains foreign-key violations".into(),
        ));
    }
    audit_row_contracts(connection)?;
    validate_relational_integrity(connection)?;
    Ok(())
}

fn audit_row_contracts(connection: &Connection) -> Result<()> {
    let sql = format!("SELECT {MESSAGE_COLUMNS} FROM messages ORDER BY owner, id");
    let mut statement = connection.prepare(&sql)?;
    for row in statement.query_map([], row_to_message)? {
        row?;
    }
    drop(statement);

    let mut statement = connection.prepare(
        "SELECT owner, message_id, conversation_id, conversation_sequence, origin, status, \
            published_at_ms, discarded_at_ms, revision, record_schema \
         FROM message_publications ORDER BY owner, message_id",
    )?;
    let publications = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            stored_u64(row, 3, "publication conversation sequence")?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            optional_timestamp(row, 6, "publication published_at")?,
            optional_timestamp(row, 7, "publication discarded_at")?,
            stored_u64(row, 8, "publication revision")?,
            row.get::<_, i64>(9)?,
        ))
    })?;
    for publication in publications {
        let (
            owner,
            message_id,
            conversation_id,
            conversation_sequence,
            origin,
            status,
            published_at,
            discarded_at,
            revision,
            schema,
        ) = publication?;
        validate_owner(&owner).map_err(|_| {
            MessageStoreError::CorruptData("publication owner violates its bound".into())
        })?;
        validate_uuid_v7("publication message id", &message_id)?;
        validate_text("publication conversation id", &conversation_id, 256)?;
        let status = MessagePublicationStatus::from_str(&status)?;
        let valid = conversation_sequence > 0
            && schema == i64::from(RECORD_SCHEMA)
            && matches!(origin.as_str(), "ordinary" | "staged")
            && match (origin.as_str(), status) {
                ("ordinary", MessagePublicationStatus::Published) => {
                    published_at.is_some() && discarded_at.is_none() && revision == 0
                }
                ("staged", MessagePublicationStatus::Staged) => {
                    published_at.is_none() && discarded_at.is_none() && revision == 0
                }
                ("staged", MessagePublicationStatus::Published) => {
                    published_at.is_some() && discarded_at.is_none() && revision == 1
                }
                ("staged", MessagePublicationStatus::Discarded) => {
                    published_at.is_none() && discarded_at.is_some() && revision == 1
                }
                _ => false,
            };
        if !valid {
            return Err(MessageStoreError::CorruptData(
                "message publication violates its lifecycle contract".into(),
            ));
        }
    }
    drop(statement);

    let sql = format!("SELECT {DELIVERY_COLUMNS} FROM deliveries ORDER BY owner, id");
    let mut statement = connection.prepare(&sql)?;
    for row in statement.query_map([], row_to_delivery)? {
        row?;
    }
    drop(statement);

    let sql = format!("SELECT {EVENT_COLUMNS} FROM message_events ORDER BY owner, sequence");
    let mut statement = connection.prepare(&sql)?;
    for row in statement.query_map([], row_to_event)? {
        row?;
    }
    drop(statement);

    let mut statement = connection.prepare(
        "SELECT owner, delivery_id, generation, route, target_kind, target_id, consumer, \
            token_hash, claimed_at_ms, initial_expires_at_ms \
         FROM delivery_attempts ORDER BY owner, delivery_id, generation",
    )?;
    let attempts = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            stored_u64(row, 2, "attempt generation")?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Vec<u8>>(7)?,
            stored_timestamp(row, 8, "attempt claimed_at")?,
            stored_timestamp(row, 9, "attempt initial expiry")?,
        ))
    })?;
    for attempt in attempts {
        let (
            owner,
            delivery_id,
            generation,
            route,
            target_kind,
            target_id,
            consumer,
            token_hash,
            claimed_at,
            initial_expiry,
        ) = attempt?;
        validate_owner(&owner).map_err(|_| {
            MessageStoreError::CorruptData("attempt owner violates its bound".into())
        })?;
        validate_uuid_v7("attempt delivery id", &delivery_id)?;
        validate_text("attempt route", &route, 256).map_err(|_| {
            MessageStoreError::CorruptData("attempt route violates its bound".into())
        })?;
        let kind = EndpointKind::from_str(&target_kind)?;
        EndpointRef {
            kind,
            id: target_id,
        }
        .validate("attempt target id")
        .map_err(|_| MessageStoreError::CorruptData("attempt target violates its bound".into()))?;
        validate_text("attempt consumer", &consumer, 256).map_err(|_| {
            MessageStoreError::CorruptData("attempt consumer violates its bound".into())
        })?;
        if generation == 0 || token_hash.len() != 32 || initial_expiry <= claimed_at {
            return Err(MessageStoreError::CorruptData(
                "attempt token, generation, or time range is invalid".into(),
            ));
        }
    }

    let mut statement = connection.prepare(
        "SELECT owner, delivery_id, generation, operation_key, result_json, completed_at_ms \
         FROM receipt_operations ORDER BY owner, delivery_id, generation, operation_key",
    )?;
    let operations = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            stored_u64(row, 2, "receipt operation generation")?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            stored_timestamp(row, 5, "receipt operation completed_at")?,
        ))
    })?;
    for operation in operations {
        let (owner, delivery_id, generation, operation_key, result_json, completed_at) = operation?;
        audit_receipt_operation(
            connection,
            &owner,
            &delivery_id,
            generation,
            &operation_key,
            &result_json,
            completed_at,
        )?;
    }
    Ok(())
}

fn audit_receipt_operation(
    connection: &Connection,
    owner: &str,
    delivery_id: &str,
    generation: u64,
    operation_key: &str,
    result_json: &str,
    completed_at: DateTime<Utc>,
) -> Result<()> {
    validate_owner(owner).map_err(|_| {
        MessageStoreError::CorruptData("receipt operation owner violates its bound".into())
    })?;
    validate_uuid_v7("receipt operation delivery id", delivery_id)?;
    validate_text("receipt operation key", operation_key, 256).map_err(|_| {
        MessageStoreError::CorruptData("receipt operation key violates its bound".into())
    })?;
    if generation == 0 {
        return Err(MessageStoreError::CorruptData(
            "receipt operation generation is zero".into(),
        ));
    }
    let mut reply_message_id = None;
    let delivery = if operation_key.starts_with("reply_and_ack:") {
        let stored = serde_json::from_str::<StoredReplyOperation>(result_json).map_err(|_| {
            MessageStoreError::CorruptData("reply receipt operation JSON is invalid".into())
        })?;
        validate_uuid_v7("reply receipt message id", &stored.reply_message_id)?;
        reply_message_id = Some(stored.reply_message_id);
        if stored.delivery.status != DeliveryStatus::Acknowledged {
            return Err(MessageStoreError::CorruptData(
                "reply receipt operation is not acknowledged".into(),
            ));
        }
        stored.delivery
    } else {
        serde_json::from_str::<DeliveryRecord>(result_json).map_err(|_| {
            MessageStoreError::CorruptData("receipt operation JSON is invalid".into())
        })?
    };
    validate_stored_delivery(&delivery).map_err(|_| {
        MessageStoreError::CorruptData("receipt operation delivery is invalid".into())
    })?;
    if delivery.owner != owner
        || delivery.id != delivery_id
        || delivery.lease_generation != generation
        || completed_at < delivery.updated_at
    {
        return Err(MessageStoreError::CorruptData(
            "receipt operation result contradicts its authority or completion time".into(),
        ));
    }
    let status_matches = if operation_key == "delivered" {
        delivery.status == DeliveryStatus::Delivered
    } else if operation_key == "acknowledged" || operation_key.starts_with("reply_and_ack:") {
        delivery.status == DeliveryStatus::Acknowledged
    } else if operation_key.starts_with("nack:retry:") {
        matches!(
            delivery.status,
            DeliveryStatus::Pending | DeliveryStatus::DeadLettered
        )
    } else if operation_key.starts_with("nack:permanent:") {
        delivery.status == DeliveryStatus::DeadLettered
    } else if operation_key.starts_with("renew:") {
        delivery.status.holds_lease()
    } else {
        false
    };
    if !status_matches {
        return Err(MessageStoreError::CorruptData(
            "receipt operation key contradicts its result state".into(),
        ));
    }
    let current = get_delivery(connection, owner, delivery_id)?
        .ok_or_else(|| MessageStoreError::CorruptData("receipt delivery is absent".into()))?;
    if delivery.message_id != current.message_id
        || delivery.route != current.route
        || delivery.target != current.target
        || delivery.max_attempts != current.max_attempts
        || delivery.created_at != current.created_at
        || delivery.revision > current.revision
        || delivery.lease_generation > current.lease_generation
        || delivery.updated_at > current.updated_at
    {
        return Err(MessageStoreError::CorruptData(
            "receipt operation snapshot contradicts the authoritative delivery".into(),
        ));
    }
    let event_status: Option<(String, u64)> = connection
        .query_row(
            "SELECT to_status, lease_generation FROM message_events \
             WHERE owner = ?1 AND delivery_id = ?2 AND delivery_revision = ?3",
            params![
                owner,
                delivery_id,
                u64_to_i64(delivery.revision, "receipt delivery revision")?
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    stored_u64(row, 1, "receipt event lease generation")?,
                ))
            },
        )
        .optional()?;
    let Some((event_status, event_generation)) = event_status else {
        return Err(MessageStoreError::CorruptData(
            "receipt operation has no matching lifecycle event".into(),
        ));
    };
    if DeliveryStatus::from_str(&event_status)? != delivery.status
        || event_generation != delivery.lease_generation
    {
        return Err(MessageStoreError::CorruptData(
            "receipt operation snapshot contradicts its lifecycle event".into(),
        ));
    }
    if let Some(reply_message_id) = reply_message_id {
        let reply = get_message(connection, owner, &reply_message_id)?.ok_or_else(|| {
            MessageStoreError::CorruptData("receipt reply message is absent".into())
        })?;
        let original = get_message(connection, owner, &delivery.message_id)?.ok_or_else(|| {
            MessageStoreError::CorruptData("receipt original message is absent".into())
        })?;
        if reply.reply_to.as_deref() != Some(original.id.as_str())
            || reply.conversation_id != original.conversation_id
        {
            return Err(MessageStoreError::CorruptData(
                "receipt reply is not linked to its original conversation".into(),
            ));
        }
    }
    Ok(())
}

fn validate_relational_integrity(connection: &Connection) -> Result<()> {
    let invalid_publication_relation: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM messages m \
             LEFT JOIN message_publications p \
               ON p.owner = m.owner AND p.message_id = m.id \
             WHERE p.message_id IS NULL \
                OR p.conversation_id <> m.conversation_id \
                OR (p.published_at_ms IS NOT NULL AND p.published_at_ms < m.created_at_ms) \
                OR (p.discarded_at_ms IS NOT NULL AND p.discarded_at_ms < m.created_at_ms) \
                OR (p.origin = 'ordinary' AND p.published_at_ms <> m.created_at_ms) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_publication_relation {
        return Err(MessageStoreError::CorruptData(
            "message publication contradicts its immutable message".into(),
        ));
    }

    let invalid_publication_event: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_publications p \
             JOIN deliveries d ON d.owner = p.owner AND d.message_id = p.message_id \
             WHERE p.status = 'published' AND ( \
                    p.published_at_ms IS NULL \
                 OR (SELECT COUNT(*) FROM message_events e \
                     WHERE e.owner = d.owner AND e.delivery_id = d.id \
                       AND e.delivery_revision = 0 AND e.event_type = 'enqueued' \
                       AND e.from_status IS NULL AND e.to_status = 'pending' \
                       AND e.occurred_at_ms = p.published_at_ms \
                       AND e.conversation_id = p.conversation_id \
                       AND e.conversation_sequence = p.conversation_sequence) <> 1 \
             ) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_publication_event {
        return Err(MessageStoreError::CorruptData(
            "published message is not bound to its exact initial enqueue events".into(),
        ));
    }

    let invalid_hidden_delivery: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_publications p \
             JOIN deliveries d ON d.owner = p.owner AND d.message_id = p.message_id \
             WHERE p.status <> 'published' AND ( \
                    d.status <> 'pending' OR d.revision <> 0 OR d.attempt_count <> 0 \
                 OR d.lease_generation <> 0 \
                 OR EXISTS (SELECT 1 FROM message_events e \
                     WHERE e.owner = d.owner AND e.delivery_id = d.id) \
                 OR EXISTS (SELECT 1 FROM delivery_attempts a \
                     WHERE a.owner = d.owner AND a.delivery_id = d.id) \
                 OR EXISTS (SELECT 1 FROM delivery_transport_receipts r \
                     WHERE r.owner = d.owner AND r.delivery_id = d.id) \
             ) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_hidden_delivery {
        return Err(MessageStoreError::CorruptData(
            "hidden publication has visible delivery lifecycle state".into(),
        ));
    }

    let invalid_current_attempt: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM deliveries d \
             LEFT JOIN delivery_attempts a \
               ON a.owner = d.owner AND a.delivery_id = d.id \
              AND a.generation = d.lease_generation \
             WHERE d.status IN ('leased', 'delivered') AND ( \
                    a.delivery_id IS NULL \
                 OR d.attempt_count <> d.lease_generation \
                 OR d.route <> a.route OR d.target_kind <> a.target_kind \
                 OR d.target_id <> a.target_id OR d.lease_owner IS NOT a.consumer \
                 OR d.lease_token_hash IS NOT a.token_hash \
             ) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_current_attempt {
        return Err(MessageStoreError::CorruptData(
            "active delivery is not backed by its current authenticated attempt".into(),
        ));
    }
    let invalid_attempt_history: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM deliveries d \
             LEFT JOIN ( \
                 SELECT owner, delivery_id, COUNT(*) AS count_rows, \
                        MIN(generation) AS min_generation, MAX(generation) AS max_generation \
                 FROM delivery_attempts GROUP BY owner, delivery_id \
             ) a ON a.owner = d.owner AND a.delivery_id = d.id \
             WHERE COALESCE(a.count_rows, 0) <> d.attempt_count \
                OR COALESCE(a.max_generation, 0) <> d.lease_generation \
                OR (a.count_rows > 0 AND a.min_generation <> 1) \
                OR EXISTS ( \
                    SELECT 1 FROM delivery_attempts history \
                    WHERE history.owner = d.owner AND history.delivery_id = d.id \
                      AND (history.route <> d.route \
                           OR history.target_kind <> d.target_kind \
                           OR history.target_id <> d.target_id) \
                ) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_attempt_history {
        return Err(MessageStoreError::CorruptData(
            "delivery attempt history is incomplete or contradicts its route".into(),
        ));
    }

    let invalid_transport_receipt: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM delivery_transport_receipts r \
             JOIN deliveries d ON d.owner = r.owner AND d.id = r.delivery_id \
             WHERE r.generation <> d.lease_generation \
                OR d.status NOT IN ('delivered', 'acknowledged') \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_transport_receipt {
        return Err(MessageStoreError::CorruptData(
            "transport receipt contradicts the authoritative delivery lifecycle".into(),
        ));
    }
    let mut statement = connection.prepare(
        "SELECT DISTINCT owner, delivery_id FROM delivery_transport_receipts \
         ORDER BY owner, delivery_id",
    )?;
    let receipt_batches = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for receipt_batch in receipt_batches {
        let (owner, delivery_id) = receipt_batch?;
        if get_transport_receipts(connection, &owner, &delivery_id)?.is_none() {
            return Err(MessageStoreError::CorruptData(
                "transport receipt batch disappeared during audit".into(),
            ));
        }
    }

    let invalid_event_relation: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_events e \
             JOIN deliveries d ON d.owner = e.owner AND d.id = e.delivery_id \
             JOIN messages m ON m.owner = e.owner AND m.id = e.message_id \
             JOIN message_publications p ON p.owner = e.owner AND p.message_id = e.message_id \
             WHERE d.message_id <> e.message_id \
                OR p.status <> 'published' \
                OR p.conversation_id <> e.conversation_id \
                OR p.conversation_sequence <> e.conversation_sequence \
                OR m.direction <> e.direction OR m.reply_to IS NOT e.reply_to \
                OR d.route <> e.route OR d.target_kind <> e.target_kind \
                OR d.target_id <> e.target_id \
                OR e.delivery_revision > d.revision \
                OR e.lease_generation > d.lease_generation \
                OR e.occurred_at_ms < m.created_at_ms \
                OR e.occurred_at_ms > d.updated_at_ms \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_event_relation {
        return Err(MessageStoreError::CorruptData(
            "message event contradicts its message or delivery source".into(),
        ));
    }
    let invalid_event_revision_chain: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM deliveries d \
             JOIN message_publications p ON p.owner = d.owner AND p.message_id = d.message_id \
             LEFT JOIN message_events e ON e.owner = d.owner AND e.delivery_id = d.id \
             GROUP BY d.owner, d.id, d.revision, p.status \
             HAVING (p.status = 'published' AND ( \
                    COUNT(e.sequence) <> d.revision + 1 \
                 OR MIN(e.delivery_revision) <> 0 \
                 OR MAX(e.delivery_revision) <> d.revision)) \
                 OR (p.status <> 'published' AND COUNT(e.sequence) <> 0) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_event_revision_chain {
        return Err(MessageStoreError::CorruptData(
            "delivery revision history is incomplete or contradictory".into(),
        ));
    }
    let invalid_event_transition_chain: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM ( \
                 SELECT owner, delivery_id, delivery_revision, sequence, occurred_at_ms, \
                        event_type, from_status, lease_generation, \
                        LAG(to_status) OVER chain AS previous_status, \
                        LAG(event_type) OVER chain AS previous_event_type, \
                        LAG(sequence) OVER chain AS previous_sequence, \
                        LAG(occurred_at_ms) OVER chain AS previous_occurred_at, \
                        LAG(lease_generation) OVER chain AS previous_generation \
                 FROM message_events \
                 WINDOW chain AS (PARTITION BY owner, delivery_id ORDER BY delivery_revision) \
             ) ordered \
             WHERE delivery_revision > 0 AND ( \
                    from_status IS NOT previous_status \
                 OR sequence <= previous_sequence \
                 OR occurred_at_ms < previous_occurred_at \
                 OR (event_type = 'leased' \
                     AND lease_generation <> previous_generation + 1) \
                 OR (event_type <> 'leased' \
                     AND lease_generation <> previous_generation) \
                 OR (event_type = 'delivered' AND from_status = 'pending' \
                     AND previous_event_type <> 'reclaimed') \
             ) \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_event_transition_chain {
        return Err(MessageStoreError::CorruptData(
            "delivery event transitions do not form a monotonic lifecycle chain".into(),
        ));
    }

    let invalid_projection_time: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_event_projections p \
             JOIN message_events e ON e.owner = p.owner AND e.sequence = p.event_sequence \
             WHERE p.projected_at_ms < e.occurred_at_ms \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_projection_time {
        return Err(MessageStoreError::CorruptData(
            "event projection predates its source event".into(),
        ));
    }

    let invalid_conversation_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM messages m \
             LEFT JOIN conversation_sequences s \
               ON s.owner = m.owner AND s.conversation_id = m.conversation_id \
             WHERE s.owner IS NULL OR s.next_sequence <> ( \
                 SELECT MAX(peer.conversation_sequence) + 1 FROM messages peer \
                 WHERE peer.owner = m.owner AND peer.conversation_id = m.conversation_id \
             ) \
         )",
        [],
        |row| row.get(0),
    )?;
    let invalid_event_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_events e \
             LEFT JOIN owner_event_sequences s ON s.owner = e.owner \
             WHERE s.owner IS NULL OR s.next_sequence <> ( \
                 SELECT MAX(peer.sequence) + 1 FROM message_events peer \
                 WHERE peer.owner = e.owner \
             ) \
         )",
        [],
        |row| row.get(0),
    )?;
    let invalid_publication_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_publications p \
             LEFT JOIN publication_conversation_sequences s \
               ON s.owner = p.owner AND s.conversation_id = p.conversation_id \
             WHERE p.status = 'published' AND (s.owner IS NULL OR s.next_sequence <> ( \
                 SELECT MAX(peer.conversation_sequence) + 1 FROM message_publications peer \
                 WHERE peer.owner = p.owner AND peer.conversation_id = p.conversation_id \
                   AND peer.status = 'published')) \
         )",
        [],
        |row| row.get(0),
    )?;
    let gapped_conversation_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM messages \
             GROUP BY owner, conversation_id \
             HAVING MIN(conversation_sequence) <> 1 \
                 OR COUNT(*) <> MAX(conversation_sequence) \
         )",
        [],
        |row| row.get(0),
    )?;
    let gapped_event_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_events GROUP BY owner \
             HAVING MIN(sequence) <> 1 OR COUNT(*) <> MAX(sequence) \
         )",
        [],
        |row| row.get(0),
    )?;
    let gapped_publication_sequence: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM message_publications WHERE status = 'published' \
             GROUP BY owner, conversation_id \
             HAVING MIN(conversation_sequence) <> 1 \
                 OR COUNT(*) <> MAX(conversation_sequence) \
         )",
        [],
        |row| row.get(0),
    )?;
    let orphan_allocator: bool = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM conversation_sequences s \
             WHERE NOT EXISTS (SELECT 1 FROM messages m \
                 WHERE m.owner = s.owner AND m.conversation_id = s.conversation_id) \
             UNION ALL \
             SELECT 1 FROM owner_event_sequences s \
             WHERE NOT EXISTS (SELECT 1 FROM message_events e WHERE e.owner = s.owner) \
             UNION ALL \
             SELECT 1 FROM publication_conversation_sequences s \
             WHERE NOT EXISTS (SELECT 1 FROM message_publications p \
                 WHERE p.owner = s.owner AND p.conversation_id = s.conversation_id \
                   AND p.status = 'published') \
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_conversation_sequence
        || invalid_publication_sequence
        || invalid_event_sequence
        || gapped_conversation_sequence
        || gapped_publication_sequence
        || gapped_event_sequence
        || orphan_allocator
    {
        return Err(MessageStoreError::CorruptData(
            "stored sequence allocator trails committed records".into(),
        ));
    }
    Ok(())
}

fn expected_schema() -> Result<&'static [SchemaObject]> {
    match EXPECTED_SCHEMA.get_or_init(|| {
        let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0001)
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(MIGRATION_0002)
            .map_err(|error| error.to_string())?;
        schema_manifest(&connection).map_err(|error| error.to_string())
    }) {
        Ok(manifest) => Ok(manifest.as_slice()),
        Err(error) => Err(MessageStoreError::CorruptData(format!(
            "cannot construct the supported schema manifest: {error}"
        ))),
    }
}

fn schema_manifest(connection: &Connection) -> Result<Vec<SchemaObject>> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema \
         WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name",
    )?;
    let rows = statement.query_map([], |row| {
        let sql: String = row.get(3)?;
        Ok(SchemaObject {
            kind: row.get(0)?,
            name: row.get(1)?,
            table_name: row.get(2)?,
            sql: normalize_schema_sql(&sql),
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut quote_end = None;
    let mut pending_space = false;
    for character in sql.trim().trim_end_matches(';').chars() {
        if let Some(end) = quote_end {
            normalized.push(character);
            if character == end {
                quote_end = None;
            }
            continue;
        }
        if character.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space && !normalized.is_empty() {
            normalized.push(' ');
        }
        pending_space = false;
        normalized.push(character.to_ascii_lowercase());
        quote_end = match character {
            '\'' | '"' | '`' => Some(character),
            '[' => Some(']'),
            _ => None,
        };
    }
    normalized
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| {
        MessageStoreError::InvalidInput("secure lease entropy is unavailable".into())
    })?;
    Ok(hex_lower(&bytes))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn add_seconds(value: DateTime<Utc>, seconds: u64, label: &str) -> Result<DateTime<Utc>> {
    let seconds = i64::try_from(seconds)
        .map_err(|_| MessageStoreError::InvalidInput(format!("{label} is too large")))?;
    value
        .checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or_else(|| MessageStoreError::InvalidInput(format!("{label} exceeds timestamp range")))
}

fn next_u64(value: u64, label: &str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| MessageStoreError::CorruptData(format!("{label} overflow")))
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| MessageStoreError::InvalidInput(format!("{label} exceeds SQLite range")))
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| MessageStoreError::InvalidInput(format!("{label} exceeds SQLite range")))
}

fn ensure_one_change(changed: usize, operation: &str) -> Result<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(MessageStoreError::CorruptData(format!(
            "{operation} did not update exactly one row"
        )))
    }
}

fn open_database(path: &Path) -> Result<Connection> {
    #[cfg(unix)]
    {
        reject_symlink_components(path)?;
        reject_existing_sidecar_symlinks(path)?;
        // Validate before SQLite opens the files. SQLite may normalize sidecar
        // modes during open, which would hide an unsafe at-rest state.
        validate_database_files(path)?;
    }
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Ok(Connection::open_with_flags(path, flags)?)
}

fn acquire_write_lock(path: &Path) -> Result<File> {
    let lock_path = companion_path(path, ".write-lock");
    #[cfg(unix)]
    let lock = {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file
    };
    #[cfg(not(unix))]
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    let deadline = Instant::now() + BUSY_TIMEOUT;
    loop {
        if lock.try_lock_exclusive()? {
            return Ok(lock);
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out acquiring message store write lock",
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn companion_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
fn prepare_database_path(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_existed = parent.try_exists()?;
    if !parent_existed {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    validate_private_directory(parent)?;
    reject_symlink_components(path)?;
    reject_existing_sidecar_symlinks(path)?;
    // Never expose the final SQLite path while this process still owns a raw
    // descriptor for its inode. Closing any descriptor for an inode discards
    // this process's POSIX locks, including locks acquired by another SQLite
    // connection after observing the final path.
    match std::fs::symlink_metadata(path) {
        Ok(_) => validate_database_files(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let candidate = create_closed_database_candidate(parent)?;
            publish_database_candidate(&candidate, path)?;
            validate_database_files(path)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn create_closed_database_candidate(parent: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    for _ in 0..DATABASE_CREATE_ATTEMPTS {
        let sequence = DATABASE_CREATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            "{DATABASE_CREATE_PREFIX}-{}-{sequence}",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&candidate)
        {
            Ok(file) => {
                let configured = (|| -> std::io::Result<()> {
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
                    let metadata = file.metadata()?;
                    if !metadata.is_file() || metadata.permissions().mode() & 0o7777 != 0o600 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "new message database candidate is not a private regular file",
                        ));
                    }
                    Ok(())
                })();
                drop(file);
                if let Err(error) = configured {
                    let _ = std::fs::remove_file(&candidate);
                    return Err(error.into());
                }
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not reserve a unique message database candidate",
    )
    .into())
}

#[cfg(unix)]
fn publish_database_candidate(candidate: &Path, path: &Path) -> Result<()> {
    let published = std::fs::hard_link(candidate, path);
    let _ = std::fs::remove_file(candidate);
    match published {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_database_files(path)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn prepare_database_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    Ok(())
}

#[cfg(unix)]
fn validate_database_files(path: &Path) -> Result<()> {
    // Keep this metadata-only. SQLite owns the descriptors for these files;
    // opening and dropping a second descriptor can silently destroy its
    // process-wide POSIX locks while a connection or transaction is live.
    validate_database_file(path, true)?;
    validate_database_file(&sqlite_sidecar(path, "-wal"), false)?;
    validate_database_file(&sqlite_sidecar(path, "-shm"), false)?;
    Ok(())
}

#[cfg(not(unix))]
fn validate_database_files(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sqlite_sidecar(path: &Path, suffix: &str) -> PathBuf {
    companion_path(path, suffix)
}

#[cfg(unix)]
fn validate_private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(MessageStoreError::InvalidInput(
            "message database parent must be a real directory".into(),
        ));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(MessageStoreError::InvalidInput(
            "message database parent must not be group- or world-writable".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(MessageStoreError::InvalidInput(
                    "message database path must not traverse a symlink".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn reject_existing_sidecar_symlinks(path: &Path) -> Result<()> {
    for candidate in [sqlite_sidecar(path, "-wal"), sqlite_sidecar(path, "-shm")] {
        match std::fs::symlink_metadata(candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(MessageStoreError::InvalidInput(
                    "message database sidecars must not be symlinks".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_database_file(path: &Path, required: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(
            MessageStoreError::InvalidInput("message database files must not be symlinks".into()),
        ),
        Ok(metadata) if !metadata.is_file() => Err(MessageStoreError::InvalidInput(
            "message database files must be regular files".into(),
        )),
        Ok(metadata) if metadata.permissions().mode() & 0o7777 != 0o600 => {
            Err(MessageStoreError::InvalidInput(
                "message database file permissions must be 0600; repair them while the store is offline"
                    .into(),
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::process::Command;
    use std::sync::{Arc, Barrier};

    use rusqlite::ErrorCode;

    use super::*;

    const LOCK_PROBE_DATABASE: &str = "VYANE_MESSAGE_LOCK_PROBE_DATABASE";

    #[test]
    fn first_creation_closes_candidates_before_atomic_publication() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("first-create.sqlite");
        let candidates = (0..8)
            .map(|_| create_closed_database_candidate(directory.path()).unwrap())
            .collect::<Vec<_>>();
        assert!(
            !path.exists(),
            "the final path must stay hidden while candidates are prepared"
        );
        for candidate in &candidates {
            let metadata = std::fs::symlink_metadata(candidate).unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        }

        let barrier = Arc::new(Barrier::new(candidates.len()));
        let path = Arc::new(path);
        let publishers = candidates
            .into_iter()
            .map(|candidate| {
                let barrier = Arc::clone(&barrier);
                let path = Arc::clone(&path);
                std::thread::spawn(move || {
                    barrier.wait();
                    publish_database_candidate(&candidate, &path).unwrap();
                })
            })
            .collect::<Vec<_>>();
        for publisher in publishers {
            publisher.join().unwrap();
        }

        validate_database_files(&path).unwrap();
        assert_eq!(
            std::fs::symlink_metadata(&*path)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(DATABASE_CREATE_PREFIX)
        }));

        let rejected = create_closed_database_candidate(directory.path()).unwrap();
        let unsafe_winner = directory.path().join("unsafe-winner.sqlite");
        std::fs::write(&unsafe_winner, b"unsafe winner").unwrap();
        std::fs::set_permissions(&unsafe_winner, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            publish_database_candidate(&rejected, &unsafe_winner),
            Err(MessageStoreError::InvalidInput(_))
        ));
        assert!(!rejected.exists());
    }

    #[test]
    fn metadata_validation_does_not_release_a_wal_writer_lock() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lock-probe.sqlite");
        prepare_database_path(&path).unwrap();

        let mut connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .execute_batch("CREATE TABLE lock_probe (value INTEGER NOT NULL);")
            .unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute("INSERT INTO lock_probe (value) VALUES (1)", [])
            .unwrap();

        validate_database_files(&path).unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("sqlite::tests::wal_writer_lock_probe")
            .arg("--exact")
            .arg("--ignored")
            .arg("--nocapture")
            .env(LOCK_PROBE_DATABASE, &path)
            .status()
            .unwrap();
        assert!(status.success(), "external WAL writer acquired the lock");

        transaction.commit().unwrap();
    }

    #[test]
    #[ignore = "subprocess-only WAL writer probe"]
    fn wal_writer_lock_probe() {
        let Some(path) = std::env::var_os(LOCK_PROBE_DATABASE) else {
            return;
        };
        let connection = Connection::open(path).unwrap();
        connection.busy_timeout(Duration::from_millis(100)).unwrap();
        let error = connection
            .execute_batch("BEGIN IMMEDIATE")
            .expect_err("external WAL writer unexpectedly acquired the lock");
        assert!(matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        ));
    }
}

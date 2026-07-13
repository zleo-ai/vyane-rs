CREATE TABLE conversation_sequences (
    owner             TEXT NOT NULL,
    conversation_id   TEXT NOT NULL,
    next_sequence     INTEGER NOT NULL CHECK (next_sequence >= 2),
    PRIMARY KEY (owner, conversation_id)
);

CREATE TABLE owner_event_sequences (
    owner         TEXT PRIMARY KEY,
    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 2)
);

CREATE TABLE messages (
    id                    TEXT NOT NULL,
    record_schema         INTEGER NOT NULL,
    owner                 TEXT NOT NULL,
    conversation_id       TEXT NOT NULL,
    conversation_sequence INTEGER NOT NULL CHECK (conversation_sequence > 0),
    session_id            TEXT,
    direction             TEXT NOT NULL CHECK (direction IN ('ingress', 'egress', 'internal')),
    kind                  TEXT NOT NULL,
    sender_kind           TEXT NOT NULL,
    sender_id             TEXT NOT NULL,
    body                  TEXT NOT NULL,
    payload_json          TEXT NOT NULL,
    reply_to              TEXT,
    trace_id              TEXT,
    correlation_id        TEXT,
    producer              TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL,
    request_digest        TEXT NOT NULL,
    created_at_ms         INTEGER NOT NULL,
    PRIMARY KEY (owner, id),
    UNIQUE (owner, conversation_id, conversation_sequence),
    UNIQUE (owner, conversation_id, id),
    UNIQUE (owner, producer, idempotency_key),
    FOREIGN KEY (owner, conversation_id, reply_to)
        REFERENCES messages(owner, conversation_id, id)
);

CREATE INDEX messages_owner_conversation_idx
    ON messages(owner, conversation_id, conversation_sequence, id);

CREATE TRIGGER messages_immutable_update
BEFORE UPDATE ON messages
BEGIN
    SELECT RAISE(ABORT, 'messages are immutable');
END;

CREATE TRIGGER messages_immutable_delete
BEFORE DELETE ON messages
BEGIN
    SELECT RAISE(ABORT, 'messages are immutable');
END;

CREATE TABLE deliveries (
    id                     TEXT NOT NULL,
    record_schema          INTEGER NOT NULL,
    owner                  TEXT NOT NULL,
    message_id             TEXT NOT NULL,
    route                  TEXT NOT NULL,
    target_kind            TEXT NOT NULL,
    target_id              TEXT NOT NULL,
    status                 TEXT NOT NULL CHECK (status IN (
                               'pending', 'leased', 'delivered', 'acknowledged',
                               'dead_lettered', 'expired', 'cancelled')),
    available_at_ms        INTEGER NOT NULL,
    expires_at_ms          INTEGER,
    attempt_count          INTEGER NOT NULL CHECK (attempt_count >= 0),
    max_attempts           INTEGER NOT NULL CHECK (max_attempts > 0 AND max_attempts <= 100),
    revision               INTEGER NOT NULL CHECK (revision >= 0),
    lease_generation       INTEGER NOT NULL CHECK (lease_generation >= 0),
    lease_owner            TEXT,
    lease_token_hash       BLOB,
    lease_expires_at_ms    INTEGER,
    first_delivered_at_ms  INTEGER,
    acknowledged_at_ms     INTEGER,
    dead_lettered_at_ms    INTEGER,
    failure_code           TEXT,
    created_at_ms          INTEGER NOT NULL,
    updated_at_ms          INTEGER NOT NULL,
    PRIMARY KEY (owner, id),
    UNIQUE (owner, message_id, route, target_kind, target_id),
    FOREIGN KEY (owner, message_id) REFERENCES messages(owner, id),
    CHECK (attempt_count <= max_attempts),
    CHECK (
        (status IN ('leased', 'delivered')
            AND lease_owner IS NOT NULL
            AND lease_token_hash IS NOT NULL
            AND length(lease_token_hash) = 32
            AND lease_expires_at_ms IS NOT NULL)
        OR
        (status NOT IN ('leased', 'delivered')
            AND lease_owner IS NULL
            AND lease_token_hash IS NULL
            AND lease_expires_at_ms IS NULL)
    )
);

CREATE INDEX deliveries_claim_idx
    ON deliveries(owner, route, target_kind, target_id, status, available_at_ms, created_at_ms, id);
CREATE INDEX deliveries_lease_idx
    ON deliveries(owner, status, lease_expires_at_ms);
CREATE INDEX deliveries_expiry_idx
    ON deliveries(owner, status, expires_at_ms);
CREATE INDEX deliveries_message_idx
    ON deliveries(owner, message_id, created_at_ms, id);

CREATE TABLE delivery_attempts (
    owner                 TEXT NOT NULL,
    delivery_id           TEXT NOT NULL,
    generation            INTEGER NOT NULL CHECK (generation > 0),
    route                 TEXT NOT NULL,
    target_kind           TEXT NOT NULL,
    target_id             TEXT NOT NULL,
    consumer              TEXT NOT NULL,
    token_hash            BLOB NOT NULL CHECK (length(token_hash) = 32),
    claimed_at_ms         INTEGER NOT NULL,
    initial_expires_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, delivery_id, generation),
    FOREIGN KEY (owner, delivery_id) REFERENCES deliveries(owner, id)
);

CREATE TRIGGER delivery_attempts_immutable_update
BEFORE UPDATE ON delivery_attempts
BEGIN
    SELECT RAISE(ABORT, 'delivery attempts are immutable');
END;

CREATE TRIGGER delivery_attempts_immutable_delete
BEFORE DELETE ON delivery_attempts
BEGIN
    SELECT RAISE(ABORT, 'delivery attempts are immutable');
END;

CREATE TABLE receipt_operations (
    owner          TEXT NOT NULL,
    delivery_id    TEXT NOT NULL,
    generation     INTEGER NOT NULL,
    operation_key  TEXT NOT NULL,
    result_json    TEXT NOT NULL,
    completed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, delivery_id, generation, operation_key),
    FOREIGN KEY (owner, delivery_id, generation)
        REFERENCES delivery_attempts(owner, delivery_id, generation)
);

CREATE TRIGGER receipt_operations_immutable_update
BEFORE UPDATE ON receipt_operations
BEGIN
    SELECT RAISE(ABORT, 'receipt operations are immutable');
END;

CREATE TRIGGER receipt_operations_immutable_delete
BEFORE DELETE ON receipt_operations
BEGIN
    SELECT RAISE(ABORT, 'receipt operations are immutable');
END;

CREATE INDEX receipt_operations_attempt_idx
    ON receipt_operations(owner, delivery_id, generation);

CREATE TABLE delivery_transport_receipts (
    record_schema       INTEGER NOT NULL CHECK (record_schema = 1),
    owner               TEXT NOT NULL,
    delivery_id         TEXT NOT NULL,
    generation          INTEGER NOT NULL CHECK (generation > 0),
    ordinal             INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 128),
    transport           TEXT NOT NULL,
    account_scope       TEXT NOT NULL,
    destination_scope   TEXT NOT NULL,
    external_id         TEXT NOT NULL,
    receipt_digest      TEXT NOT NULL CHECK (
                            length(receipt_digest) = 64
                            AND receipt_digest NOT GLOB '*[^0-9a-f]*'
                        ),
    recorded_at_ms      INTEGER NOT NULL,
    PRIMARY KEY (owner, delivery_id, ordinal),
    UNIQUE (owner, delivery_id, external_id),
    UNIQUE (owner, transport, account_scope, destination_scope, external_id),
    FOREIGN KEY (owner, delivery_id, generation)
        REFERENCES delivery_attempts(owner, delivery_id, generation)
) WITHOUT ROWID;

CREATE TRIGGER delivery_transport_receipts_immutable_update
BEFORE UPDATE ON delivery_transport_receipts
BEGIN
    SELECT RAISE(ABORT, 'transport receipts are immutable');
END;

CREATE TRIGGER delivery_transport_receipts_immutable_delete
BEFORE DELETE ON delivery_transport_receipts
BEGIN
    SELECT RAISE(ABORT, 'transport receipts are immutable');
END;

CREATE TRIGGER delivery_transport_receipts_require_delivered
BEFORE INSERT ON delivery_transport_receipts
WHEN NOT EXISTS (
    SELECT 1 FROM deliveries
    WHERE owner = NEW.owner
      AND id = NEW.delivery_id
      AND lease_generation = NEW.generation
      AND status = 'delivered'
)
BEGIN
    SELECT RAISE(ABORT, 'transport receipt requires matching delivered attempt');
END;

CREATE TRIGGER delivery_transport_receipts_no_reopen
BEFORE UPDATE OF status, lease_generation ON deliveries
WHEN EXISTS (
    SELECT 1 FROM delivery_transport_receipts
    WHERE owner = OLD.owner AND delivery_id = OLD.id
)
AND (
    NEW.status NOT IN ('delivered', 'acknowledged')
    OR NEW.lease_generation <> OLD.lease_generation
)
BEGIN
    SELECT RAISE(ABORT, 'externally delivered delivery cannot be reopened');
END;

CREATE TABLE message_events (
    sequence                 INTEGER NOT NULL CHECK (sequence > 0),
    event_id                 TEXT NOT NULL,
    owner                    TEXT NOT NULL,
    message_id               TEXT NOT NULL,
    delivery_id              TEXT NOT NULL,
    delivery_revision        INTEGER NOT NULL CHECK (delivery_revision >= 0),
    conversation_id          TEXT NOT NULL,
    conversation_sequence    INTEGER NOT NULL CHECK (conversation_sequence > 0),
    occurred_at_ms           INTEGER NOT NULL,
    event_type               TEXT NOT NULL,
    from_status              TEXT,
    to_status                TEXT NOT NULL,
    lease_generation         INTEGER NOT NULL CHECK (lease_generation >= 0),
    route                    TEXT NOT NULL,
    target_kind              TEXT NOT NULL,
    target_id                TEXT NOT NULL,
    direction                TEXT NOT NULL,
    reply_to                 TEXT,
    PRIMARY KEY (owner, sequence),
    UNIQUE (owner, event_id),
    UNIQUE (owner, delivery_id, delivery_revision),
    FOREIGN KEY (owner, message_id) REFERENCES messages(owner, id),
    FOREIGN KEY (owner, delivery_id) REFERENCES deliveries(owner, id)
);

CREATE INDEX message_events_message_idx
    ON message_events(owner, message_id, sequence);

CREATE TRIGGER message_events_immutable_update
BEFORE UPDATE ON message_events
BEGIN
    SELECT RAISE(ABORT, 'message events are immutable');
END;

CREATE TRIGGER message_events_immutable_delete
BEFORE DELETE ON message_events
BEGIN
    SELECT RAISE(ABORT, 'message events are immutable');
END;

CREATE TABLE message_event_projections (
    owner             TEXT NOT NULL,
    projector         TEXT NOT NULL,
    event_sequence    INTEGER NOT NULL CHECK (event_sequence > 0),
    projected_at_ms   INTEGER NOT NULL,
    PRIMARY KEY (owner, projector, event_sequence),
    FOREIGN KEY (owner, event_sequence) REFERENCES message_events(owner, sequence)
) WITHOUT ROWID;

CREATE INDEX message_event_projections_event_idx
    ON message_event_projections(owner, event_sequence, projector);

CREATE TRIGGER message_event_projections_immutable_update
BEFORE UPDATE ON message_event_projections
BEGIN
    SELECT RAISE(ABORT, 'message event projections are immutable');
END;

CREATE TRIGGER message_event_projections_immutable_delete
BEFORE DELETE ON message_event_projections
BEGIN
    SELECT RAISE(ABORT, 'message event projections are immutable');
END;

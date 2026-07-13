CREATE TABLE message_publications (
    owner           TEXT NOT NULL,
    message_id      TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    conversation_sequence INTEGER NOT NULL CHECK (conversation_sequence > 0),
    origin          TEXT NOT NULL CHECK (origin IN ('ordinary', 'staged')),
    status          TEXT NOT NULL CHECK (status IN ('staged', 'published', 'discarded')),
    published_at_ms INTEGER,
    discarded_at_ms INTEGER,
    revision        INTEGER NOT NULL CHECK (revision >= 0),
    record_schema   INTEGER NOT NULL CHECK (record_schema = 1),
    PRIMARY KEY (owner, message_id),
    FOREIGN KEY (owner, message_id) REFERENCES messages(owner, id) ON DELETE RESTRICT,
    CHECK (
        (origin = 'ordinary' AND status = 'published' AND published_at_ms IS NOT NULL
            AND discarded_at_ms IS NULL AND revision = 0)
        OR
        (origin = 'staged' AND status = 'staged' AND published_at_ms IS NULL
            AND discarded_at_ms IS NULL AND revision = 0)
        OR
        (origin = 'staged' AND status = 'published' AND published_at_ms IS NOT NULL
            AND discarded_at_ms IS NULL AND revision = 1)
        OR
        (origin = 'staged' AND status = 'discarded' AND published_at_ms IS NULL
            AND discarded_at_ms IS NOT NULL AND revision = 1)
    )
) WITHOUT ROWID;

CREATE INDEX message_publications_owner_status_idx
    ON message_publications(owner, status, message_id);

CREATE UNIQUE INDEX message_publications_conversation_sequence_idx
    ON message_publications(owner, conversation_id, conversation_sequence)
    WHERE status = 'published';

CREATE TABLE publication_conversation_sequences (
    owner             TEXT NOT NULL,
    conversation_id   TEXT NOT NULL,
    next_sequence     INTEGER NOT NULL CHECK (next_sequence >= 2),
    PRIMARY KEY (owner, conversation_id)
);

INSERT INTO message_publications (
    owner, message_id, conversation_id, conversation_sequence, origin, status,
    published_at_ms, discarded_at_ms, revision, record_schema
)
SELECT owner, id, conversation_id, conversation_sequence, 'ordinary', 'published',
       created_at_ms, NULL, 0, 1
FROM messages;

INSERT INTO publication_conversation_sequences(owner, conversation_id, next_sequence)
SELECT owner, conversation_id, MAX(conversation_sequence) + 1
FROM messages
GROUP BY owner, conversation_id;

CREATE TRIGGER message_publications_guard_update
BEFORE UPDATE ON message_publications
WHEN NOT (
    OLD.origin = 'staged'
    AND OLD.status = 'staged'
    AND NEW.owner = OLD.owner
    AND NEW.message_id = OLD.message_id
    AND NEW.conversation_id = OLD.conversation_id
    AND NEW.origin = OLD.origin
    AND NEW.status IN ('published', 'discarded')
    AND NEW.revision = OLD.revision + 1
    AND NEW.record_schema = OLD.record_schema
)
BEGIN
    SELECT RAISE(ABORT, 'invalid message publication transition');
END;

CREATE TRIGGER message_publications_immutable_delete
BEFORE DELETE ON message_publications
BEGIN
    SELECT RAISE(ABORT, 'message publications are immutable after insertion');
END;

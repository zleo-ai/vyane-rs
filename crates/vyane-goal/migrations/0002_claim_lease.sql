-- Schema v2: worker claim/lease columns on goals, plus new audited event kinds.

ALTER TABLE goals ADD COLUMN claimed_by TEXT;
ALTER TABLE goals ADD COLUMN claim_expires_at_ms INTEGER;
ALTER TABLE goals ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0
    CHECK (claim_generation >= 0);

-- Rebuild goal_events to widen the kind CHECK constraint.
DROP TRIGGER goal_events_immutable_update;
DROP TRIGGER goal_events_immutable_delete;

CREATE TABLE goal_events_v2 (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    owner           TEXT NOT NULL,
    goal_id         TEXT NOT NULL,
    revision        INTEGER NOT NULL CHECK (revision >= 0),
    occurred_at_ms  INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN (
                        'created', 'started', 'claimed', 'lease_renewed',
                        'reclaimed', 'progress', 'criterion_satisfied',
                        'criteria_waived', 'paused', 'resumed', 'completed',
                        'failed', 'cancelled')),
    from_status     TEXT,
    to_status       TEXT NOT NULL,
    stage           TEXT,
    detail          TEXT,
    UNIQUE (owner, event_id),
    UNIQUE (owner, goal_id, revision),
    FOREIGN KEY (owner, goal_id) REFERENCES goals(owner, id)
);

INSERT INTO goal_events_v2 (sequence, event_id, owner, goal_id, revision,
    occurred_at_ms, kind, from_status, to_status, stage, detail)
SELECT sequence, event_id, owner, goal_id, revision,
    occurred_at_ms, kind, from_status, to_status, stage, detail
FROM goal_events;

DROP INDEX goal_events_owner_goal_idx;
DROP TABLE goal_events;
ALTER TABLE goal_events_v2 RENAME TO goal_events;

CREATE INDEX goal_events_owner_goal_idx
    ON goal_events(owner, goal_id, revision);

CREATE TRIGGER goal_events_immutable_update
BEFORE UPDATE ON goal_events
BEGIN
    SELECT RAISE(ABORT, 'goal events are immutable');
END;

CREATE TRIGGER goal_events_immutable_delete
BEFORE DELETE ON goal_events
BEGIN
    SELECT RAISE(ABORT, 'goal events are immutable');
END;

CREATE TABLE goals (
    owner               TEXT NOT NULL,
    id                  TEXT NOT NULL,
    record_schema       INTEGER NOT NULL CHECK (record_schema = 1),
    title               TEXT NOT NULL,
    description         TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN (
                            'queued', 'in_progress', 'paused',
                            'completed', 'failed', 'cancelled')),
    priority            INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 4),
    parent_goal_id      TEXT,
    acceptance_json     TEXT NOT NULL,
    created_at_ms       INTEGER NOT NULL,
    started_at_ms       INTEGER,
    updated_at_ms       INTEGER NOT NULL,
    finished_at_ms      INTEGER,
    revision            INTEGER NOT NULL CHECK (revision >= 0),
    completion_summary  TEXT,
    failure_reason      TEXT,
    pause_reason        TEXT,
    cancel_reason       TEXT,
    PRIMARY KEY (owner, id)
);

CREATE INDEX goals_owner_queue_idx
    ON goals(owner, status, priority, created_at_ms, id);
CREATE INDEX goals_owner_updated_idx
    ON goals(owner, priority, updated_at_ms DESC, id);
CREATE INDEX goals_owner_parent_idx
    ON goals(owner, parent_goal_id, priority, updated_at_ms DESC, id)
    WHERE parent_goal_id IS NOT NULL;

CREATE TABLE goal_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    owner           TEXT NOT NULL,
    goal_id         TEXT NOT NULL,
    revision        INTEGER NOT NULL CHECK (revision >= 0),
    occurred_at_ms  INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN (
                        'created', 'started', 'progress', 'paused',
                        'resumed', 'completed', 'failed', 'cancelled')),
    from_status     TEXT,
    to_status       TEXT NOT NULL,
    stage           TEXT,
    detail          TEXT,
    UNIQUE (owner, event_id),
    UNIQUE (owner, goal_id, revision),
    FOREIGN KEY (owner, goal_id) REFERENCES goals(owner, id)
);

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

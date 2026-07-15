CREATE TABLE goal_verifications (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    verification_id TEXT NOT NULL UNIQUE,
    owner           TEXT NOT NULL,
    goal_id         TEXT NOT NULL,
    recorded_at_ms  INTEGER NOT NULL,
    worker_id       TEXT,
    payload_json    TEXT NOT NULL,
    payload_sha256  TEXT NOT NULL,
    FOREIGN KEY (owner, goal_id) REFERENCES goals(owner, id)
);

CREATE INDEX goal_verifications_owner_goal_idx
    ON goal_verifications(owner, goal_id, recorded_at_ms, verification_id);

CREATE TRIGGER goal_verifications_immutable_update
BEFORE UPDATE ON goal_verifications
BEGIN
    SELECT RAISE(ABORT, 'goal verification artifacts are immutable');
END;

CREATE TRIGGER goal_verifications_immutable_delete
BEFORE DELETE ON goal_verifications
BEGIN
    SELECT RAISE(ABORT, 'goal verification artifacts are immutable');
END;

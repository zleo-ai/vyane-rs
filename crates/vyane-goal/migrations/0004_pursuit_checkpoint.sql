CREATE TABLE goal_pursuit_checkpoints (
    owner                TEXT NOT NULL,
    goal_id              TEXT NOT NULL,
    checkpoint_revision  INTEGER NOT NULL CHECK (checkpoint_revision >= 0),
    claim_generation     INTEGER NOT NULL CHECK (claim_generation >= 0),
    updated_at_ms        INTEGER NOT NULL,
    payload_json         TEXT NOT NULL,
    payload_sha256       TEXT NOT NULL,
    PRIMARY KEY (owner, goal_id),
    FOREIGN KEY (owner, goal_id) REFERENCES goals(owner, id)
);

CREATE INDEX goal_pursuit_checkpoints_owner_updated_idx
    ON goal_pursuit_checkpoints(owner, updated_at_ms DESC, goal_id);

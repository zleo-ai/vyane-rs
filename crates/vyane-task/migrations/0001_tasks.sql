CREATE TABLE tasks (
    id                           TEXT PRIMARY KEY,
    record_schema                INTEGER NOT NULL,
    owner                        TEXT NOT NULL,
    kind                         TEXT NOT NULL,
    origin                       TEXT NOT NULL,
    state                        TEXT NOT NULL,
    task_digest                  TEXT NOT NULL,
    target_key                   TEXT NOT NULL,
    created_at_ms                INTEGER NOT NULL,
    started_at_ms                INTEGER,
    updated_at_ms                INTEGER NOT NULL,
    finished_at_ms               INTEGER,
    revision                     INTEGER NOT NULL,
    executor_epoch               INTEGER NOT NULL,
    controller_kind              TEXT,
    controller_instance_id       TEXT,
    controller_pid               INTEGER,
    controller_pgid              INTEGER,
    controller_started_at_ms     INTEGER,
    controller_birth_fingerprint TEXT,
    lease_owner                  TEXT,
    lease_expires_at_ms          INTEGER,
    ledger_run_id                TEXT,
    failure_code                 TEXT,
    CHECK (revision >= 0),
    CHECK (executor_epoch >= 0),
    CHECK (
        (controller_kind IS NULL
            AND controller_instance_id IS NULL
            AND controller_pid IS NULL
            AND controller_pgid IS NULL
            AND controller_started_at_ms IS NULL
            AND controller_birth_fingerprint IS NULL)
        OR
        (controller_kind = 'in_process'
            AND controller_instance_id IS NOT NULL
            AND controller_pid IS NULL
            AND controller_pgid IS NULL
            AND controller_started_at_ms IS NULL
            AND controller_birth_fingerprint IS NULL)
        OR
        (controller_kind = 'process_group'
            AND controller_instance_id IS NULL
            AND controller_pid IS NOT NULL
            AND controller_pgid IS NOT NULL
            AND controller_pid > 0
            AND controller_pgid > 0
            AND controller_started_at_ms IS NOT NULL)
    ),
    CHECK (
        (lease_owner IS NULL AND lease_expires_at_ms IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
    )
);

CREATE INDEX tasks_owner_created_idx
    ON tasks(owner, created_at_ms DESC, id DESC);
CREATE INDEX tasks_state_lease_idx
    ON tasks(state, lease_expires_at_ms);
CREATE INDEX tasks_ledger_run_idx
    ON tasks(ledger_run_id) WHERE ledger_run_id IS NOT NULL;

CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    revision        INTEGER NOT NULL,
    occurred_at_ms  INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    from_state      TEXT,
    to_state        TEXT NOT NULL,
    actor_instance  TEXT,
    executor_epoch  INTEGER NOT NULL,
    UNIQUE(task_id, revision)
);

CREATE INDEX task_events_task_idx
    ON task_events(task_id, revision);

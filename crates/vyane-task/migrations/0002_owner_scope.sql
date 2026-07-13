DROP INDEX tasks_owner_created_idx;
DROP INDEX tasks_state_lease_idx;
DROP INDEX tasks_ledger_run_idx;
DROP INDEX task_events_task_idx;
ALTER TABLE task_events RENAME TO task_events_v1;
ALTER TABLE tasks RENAME TO tasks_v1;

CREATE TABLE tasks (
    owner                        TEXT NOT NULL,
    id                           TEXT NOT NULL,
    record_schema                INTEGER NOT NULL,
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
    PRIMARY KEY (owner, id),
    CHECK (revision >= 0),
    CHECK (executor_epoch >= 0),
    CHECK (
        (controller_kind IS NULL AND controller_instance_id IS NULL
            AND controller_pid IS NULL AND controller_pgid IS NULL
            AND controller_started_at_ms IS NULL AND controller_birth_fingerprint IS NULL)
        OR (controller_kind = 'in_process' AND controller_instance_id IS NOT NULL
            AND controller_pid IS NULL AND controller_pgid IS NULL
            AND controller_started_at_ms IS NULL AND controller_birth_fingerprint IS NULL)
        OR (controller_kind = 'process_group' AND controller_instance_id IS NULL
            AND controller_pid IS NOT NULL AND controller_pgid IS NOT NULL
            AND controller_pid > 0 AND controller_pgid > 0
            AND controller_started_at_ms IS NOT NULL)
    ),
    CHECK ((lease_owner IS NULL AND lease_expires_at_ms IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL))
);

CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    owner           TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    occurred_at_ms  INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    from_state      TEXT,
    to_state        TEXT NOT NULL,
    actor_instance  TEXT,
    executor_epoch  INTEGER NOT NULL,
    UNIQUE(owner, task_id, revision),
    FOREIGN KEY(owner, task_id) REFERENCES tasks(owner, id) ON DELETE CASCADE
);

INSERT INTO tasks (
    owner, id, record_schema, kind, origin, state, task_digest, target_key,
    created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision, executor_epoch,
    controller_kind, controller_instance_id, controller_pid, controller_pgid,
    controller_started_at_ms, controller_birth_fingerprint, lease_owner,
    lease_expires_at_ms, ledger_run_id, failure_code
)
SELECT owner, id, record_schema, kind, origin, state, task_digest, target_key,
    created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision, executor_epoch,
    controller_kind, controller_instance_id, controller_pid, controller_pgid,
    controller_started_at_ms, controller_birth_fingerprint, lease_owner,
    lease_expires_at_ms, ledger_run_id, failure_code
FROM tasks_v1;

INSERT INTO task_events (
    sequence, owner, task_id, revision, occurred_at_ms, kind, from_state, to_state,
    actor_instance, executor_epoch
)
SELECT e.sequence, t.owner, e.task_id, e.revision, e.occurred_at_ms, e.kind,
    e.from_state, e.to_state, e.actor_instance, e.executor_epoch
FROM task_events_v1 AS e JOIN tasks_v1 AS t ON t.id = e.task_id;

DROP TABLE task_events_v1;
DROP TABLE tasks_v1;

CREATE INDEX tasks_owner_created_idx ON tasks(owner, created_at_ms DESC, id DESC);
CREATE INDEX tasks_owner_state_lease_idx ON tasks(owner, state, lease_expires_at_ms);
CREATE INDEX tasks_owner_ledger_run_idx ON tasks(owner, ledger_run_id)
    WHERE ledger_run_id IS NOT NULL;
CREATE INDEX task_events_owner_task_idx ON task_events(owner, task_id, revision);

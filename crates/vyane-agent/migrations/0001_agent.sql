CREATE TABLE workers (
    owner TEXT NOT NULL,
    id TEXT NOT NULL,
    parent_id TEXT,
    logical_session_id TEXT,
    lifecycle TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    released_at_ms INTEGER,
    revision INTEGER NOT NULL,
    record_schema INTEGER NOT NULL,
    PRIMARY KEY (owner, id),
    FOREIGN KEY (owner, parent_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    CHECK (parent_id IS NULL OR parent_id <> id),
    CHECK (revision >= 0),
    CHECK (record_schema = 1)
) STRICT;

CREATE INDEX workers_owner_parent_idx
ON workers(owner, parent_id, created_at_ms, id);

CREATE INDEX workers_owner_lifecycle_idx
ON workers(owner, lifecycle, created_at_ms, id);

CREATE TABLE agent_runs (
    owner TEXT NOT NULL,
    id TEXT NOT NULL,
    queue_sequence INTEGER NOT NULL UNIQUE,
    worker_id TEXT NOT NULL,
    task_id TEXT,
    trace_id TEXT,
    parent_run_id TEXT,
    resume_of_run_id TEXT,
    state TEXT NOT NULL,
    mode TEXT NOT NULL,
    target_key TEXT NOT NULL,
    prompt_digest TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    resume_binding_digest TEXT,
    available_at_ms INTEGER NOT NULL,
    deadline_at_ms INTEGER,
    timeout_ms INTEGER NOT NULL,
    max_resume_attempts INTEGER NOT NULL,
    resume_attempt INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    revision INTEGER NOT NULL,
    worker_generation INTEGER NOT NULL,
    controller_kind TEXT,
    controller_id TEXT,
    controller_fingerprint TEXT,
    lease_owner TEXT,
    lease_expires_at_ms INTEGER,
    lease_token_hash TEXT,
    last_heartbeat_at_ms INTEGER,
    last_activity_at_ms INTEGER,
    failure_code TEXT,
    record_schema INTEGER NOT NULL,
    PRIMARY KEY (owner, id),
    FOREIGN KEY (owner, worker_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, parent_run_id) REFERENCES agent_runs(owner, id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, resume_of_run_id) REFERENCES agent_runs(owner, id) ON DELETE RESTRICT,
    CHECK (revision >= 0),
    CHECK (worker_generation >= 0),
    CHECK (max_resume_attempts >= 0),
    CHECK (resume_attempt >= 0),
    CHECK (timeout_ms > 0),
    CHECK (record_schema = 1),
    CHECK ((controller_kind IS NULL) = (controller_id IS NULL)),
    CHECK ((lease_owner IS NULL) = (lease_expires_at_ms IS NULL)),
    CHECK ((lease_owner IS NULL) = (lease_token_hash IS NULL))
) STRICT;

CREATE UNIQUE INDEX agent_runs_owner_resume_of_idx
ON agent_runs(owner, resume_of_run_id)
WHERE resume_of_run_id IS NOT NULL;

CREATE INDEX agent_runs_owner_due_idx
ON agent_runs(owner, state, available_at_ms, queue_sequence);

CREATE INDEX agent_runs_owner_worker_idx
ON agent_runs(owner, worker_id, created_at_ms, id);

CREATE UNIQUE INDEX agent_runs_owner_worker_generation_idx
ON agent_runs(owner, worker_id, worker_generation)
WHERE worker_generation > 0;

CREATE INDEX agent_runs_owner_task_idx
ON agent_runs(owner, task_id, created_at_ms, id)
WHERE task_id IS NOT NULL;

CREATE UNIQUE INDEX agent_runs_one_active_per_worker_idx
ON agent_runs(owner, worker_id)
WHERE state IN ('starting', 'running', 'cancelling');

CREATE TABLE cancel_tree_operations (
    owner TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    root_worker_id TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    worker_count INTEGER NOT NULL,
    run_count INTEGER NOT NULL,
    lease_owner TEXT NOT NULL,
    lease_seconds INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    record_schema INTEGER NOT NULL,
    PRIMARY KEY (owner, operation_id),
    FOREIGN KEY (owner, root_worker_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    CHECK (worker_count > 0),
    CHECK (run_count >= 0),
    CHECK (lease_seconds > 0),
    CHECK (record_schema = 1)
) STRICT;

CREATE TABLE cancel_tree_operation_workers (
    owner TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (owner, operation_id, worker_id),
    UNIQUE (owner, operation_id, ordinal),
    FOREIGN KEY (owner, operation_id)
      REFERENCES cancel_tree_operations(owner, operation_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, worker_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    CHECK (ordinal >= 0)
) STRICT;

CREATE TABLE cancel_tree_operation_runs (
    owner TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    action TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (owner, operation_id, run_id),
    UNIQUE (owner, operation_id, ordinal),
    FOREIGN KEY (owner, operation_id)
      REFERENCES cancel_tree_operations(owner, operation_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, run_id) REFERENCES agent_runs(owner, id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, worker_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    CHECK (action IN ('queued_cancel', 'controller_cancel')),
    CHECK (ordinal >= 0)
) STRICT;

CREATE TABLE run_control_operations (
    owner TEXT NOT NULL,
    run_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_generation INTEGER NOT NULL,
    run_revision INTEGER NOT NULL,
    controller_kind TEXT,
    controller_id TEXT,
    controller_fingerprint TEXT,
    token_hash TEXT NOT NULL,
    lease_owner TEXT NOT NULL,
    lease_expires_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    settled_at_ms INTEGER,
    record_schema INTEGER NOT NULL,
    PRIMARY KEY (owner, run_id, operation_id),
    FOREIGN KEY (owner, run_id) REFERENCES agent_runs(owner, id) ON DELETE RESTRICT,
    CHECK ((controller_kind IS NULL) = (controller_id IS NULL)),
    CHECK (worker_generation > 0),
    CHECK (run_revision > 0),
    CHECK (record_schema = 1)
) STRICT;

CREATE UNIQUE INDEX run_control_operations_one_active_idx
ON run_control_operations(owner, run_id)
WHERE status = 'active';

CREATE INDEX run_control_operations_expiry_idx
ON run_control_operations(owner, status, lease_expires_at_ms, run_id);

CREATE TABLE owner_agent_event_sequences (
    owner TEXT PRIMARY KEY,
    next_sequence INTEGER NOT NULL,
    CHECK (next_sequence > 0)
) STRICT;

CREATE TABLE agent_events (
    owner TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    run_id TEXT,
    occurred_at_ms INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    worker_revision INTEGER NOT NULL,
    run_revision INTEGER,
    run_state TEXT,
    worker_lifecycle TEXT NOT NULL,
    PRIMARY KEY (owner, sequence),
    UNIQUE (owner, event_id),
    FOREIGN KEY (owner, worker_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, run_id) REFERENCES agent_runs(owner, id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX agent_events_owner_sequence_idx
ON agent_events(owner, sequence);

CREATE INDEX agent_events_owner_worker_idx
ON agent_events(owner, worker_id, sequence);

CREATE TABLE agent_projector_progress (
    owner TEXT NOT NULL,
    projector TEXT NOT NULL,
    event_sequence INTEGER NOT NULL,
    projected_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, projector, event_sequence),
    FOREIGN KEY (owner, event_sequence)
      REFERENCES agent_events(owner, sequence) ON DELETE RESTRICT
) STRICT;

CREATE INDEX agent_projector_progress_owner_projector_idx
ON agent_projector_progress(owner, projector, projected_at_ms, event_sequence);

CREATE UNIQUE INDEX agent_runs_owner_id_worker_generation_idx
ON agent_runs(owner, id, worker_id, worker_generation);

CREATE TABLE agent_run_completions (
    owner TEXT NOT NULL,
    run_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_generation INTEGER NOT NULL,
    completion_id TEXT NOT NULL,
    sink_kind TEXT NOT NULL,
    publication_key TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    content_bytes INTEGER NOT NULL,
    status TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    prepared_at_ms INTEGER NOT NULL,
    prepared_run_revision INTEGER NOT NULL,
    committed_at_ms INTEGER,
    committed_run_revision INTEGER,
    abandoned_at_ms INTEGER,
    abandoned_run_revision INTEGER,
    committed_by_operation_id TEXT,
    revision INTEGER NOT NULL,
    record_schema INTEGER NOT NULL,
    PRIMARY KEY (owner, run_id),
    UNIQUE (owner, completion_id),
    UNIQUE (owner, sink_kind, publication_key),
    FOREIGN KEY (owner, run_id, worker_id, worker_generation)
      REFERENCES agent_runs(owner, id, worker_id, worker_generation) ON DELETE RESTRICT,
    FOREIGN KEY (owner, worker_id) REFERENCES workers(owner, id) ON DELETE RESTRICT,
    FOREIGN KEY (owner, run_id, committed_by_operation_id)
      REFERENCES run_control_operations(owner, run_id, operation_id) ON DELETE RESTRICT,
    CHECK (worker_generation > 0),
    CHECK (content_bytes >= 0),
    CHECK (status IN ('prepared', 'committed', 'abandoned')),
    CHECK (length(content_digest) = 64 AND content_digest NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(token_hash) = 64 AND token_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (revision >= 0),
    CHECK (prepared_run_revision > 0),
    CHECK (committed_run_revision IS NULL OR committed_run_revision > 0),
    CHECK (abandoned_run_revision IS NULL OR abandoned_run_revision > 0),
    CHECK (record_schema = 1),
    CHECK ((status = 'prepared' AND committed_at_ms IS NULL AND abandoned_at_ms IS NULL
            AND committed_run_revision IS NULL AND abandoned_run_revision IS NULL
            AND committed_by_operation_id IS NULL)
        OR (status = 'committed' AND committed_at_ms IS NOT NULL AND abandoned_at_ms IS NULL
            AND committed_run_revision IS NOT NULL AND abandoned_run_revision IS NULL)
        OR (status = 'abandoned' AND committed_at_ms IS NULL AND abandoned_at_ms IS NOT NULL
            AND committed_run_revision IS NULL AND abandoned_run_revision IS NOT NULL
            AND committed_by_operation_id IS NULL))
) STRICT;

CREATE INDEX agent_run_completions_owner_status_idx
ON agent_run_completions(owner, status, prepared_at_ms, run_id);

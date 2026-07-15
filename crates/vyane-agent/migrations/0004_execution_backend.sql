ALTER TABLE agent_runs ADD COLUMN execution_backend TEXT NOT NULL
DEFAULT 'legacy_unassigned'
CHECK (execution_backend IN ('legacy_unassigned', 'cli_harness_process', 'native_in_process', 'remote'));

CREATE INDEX agent_runs_owner_backend_due_idx
ON agent_runs(owner, execution_backend, state, available_at_ms, queue_sequence);

ALTER TABLE agent_run_completions ADD COLUMN execution_backend TEXT NOT NULL
DEFAULT 'legacy_unassigned'
CHECK (execution_backend IN ('legacy_unassigned', 'cli_harness_process', 'native_in_process', 'remote'));

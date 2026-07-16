-- Schema v6: durable owner-scoped takeover approval queue (WP-74).
--
-- Records an explicit, unconsumed approval to execute one current ready
-- takeover / start_takeover step. The queue never dispatches: it binds the
-- exact typed target axes, canonical workdir, sandbox, bounded timeout and a
-- goal/plan snapshot so a later explicit decision and one-shot execution can
-- re-validate the boundary before any runtime effect.

CREATE TABLE goal_takeover_approvals (
    approval_id        TEXT NOT NULL PRIMARY KEY,
    owner              TEXT NOT NULL,
    goal_id            TEXT NOT NULL,
    step_id            TEXT NOT NULL,
    step_kind          TEXT NOT NULL,
    quota_event_id     TEXT NOT NULL,
    snapshot_digest    TEXT NOT NULL,
    target_profile     TEXT,
    target_provider    TEXT NOT NULL,
    target_protocol    TEXT NOT NULL,
    target_harness     TEXT NOT NULL,
    target_model       TEXT NOT NULL,
    workdir            TEXT NOT NULL,
    sandbox            TEXT NOT NULL CHECK (sandbox IN (
                            'read_only', 'write', 'full')),
    timeout_seconds    INTEGER NOT NULL CHECK (timeout_seconds > 0),
    goal_revision      INTEGER NOT NULL CHECK (goal_revision >= 0),
    plan_snapshot_json TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN (
                            'pending', 'approved', 'rejected',
                            'in_flight', 'done', 'blocked')),
    decided_by         TEXT,
    decision_reason    TEXT,
    run_id             TEXT,
    run_status         TEXT,
    blocker_reason     TEXT,
    created_at_ms      INTEGER NOT NULL,
    decided_at_ms      INTEGER,
    updated_at_ms      INTEGER NOT NULL,
    UNIQUE (owner, snapshot_digest),
    FOREIGN KEY (owner, goal_id) REFERENCES goals(owner, id)
);

CREATE INDEX goal_takeover_approvals_owner_goal_idx
    ON goal_takeover_approvals(owner, goal_id, status, created_at_ms, approval_id);
CREATE INDEX goal_takeover_approvals_owner_status_idx
    ON goal_takeover_approvals(owner, status, updated_at_ms DESC, approval_id);

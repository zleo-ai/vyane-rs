CREATE TABLE task_dispatch_persistence (
    owner         TEXT NOT NULL,
    task_id       TEXT NOT NULL,
    ledger_state  TEXT NOT NULL,
    ledger_kind   TEXT,
    session_state TEXT NOT NULL,
    session_kind  TEXT,
    PRIMARY KEY (owner, task_id),
    FOREIGN KEY (owner, task_id) REFERENCES tasks(owner, id) ON DELETE CASCADE,
    CHECK (ledger_state IN ('saved', 'not_requested', 'failed')),
    CHECK (session_state IN ('saved', 'not_requested', 'failed')),
    CHECK (
        (ledger_state = 'failed' AND ledger_kind IS NOT NULL)
        OR (ledger_state != 'failed' AND ledger_kind IS NULL)
    ),
    CHECK (
        (session_state = 'failed' AND session_kind IS NOT NULL)
        OR (session_state != 'failed' AND session_kind IS NULL)
    )
);

-- Schema v3 records the post-run ledger and session result beside a settlement.
-- Absence of a row means the completion did not record persistence. That is
-- unknown, not a successful write and not a failed write. Old v2 rows stay
-- without a row. The columns store only the step state and, for a failure,
-- the stable error-kind token.

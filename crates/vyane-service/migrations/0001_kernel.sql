-- Kernel durable store schema v1 (integration pilot).
-- Multi-process authority for receipt / effect / approval / artifact / lease fence / delivery phase.
-- AgentRun claim/lease remains in agent.sqlite (SqliteAgentStore); this store does not fork it.

CREATE TABLE kernel_meta (
    key TEXT NOT NULL PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE kernel_receipts (
    owner TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    final_status TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, receipt_id),
    CHECK (revision >= 1),
    CHECK (final_status IN (
        'open', 'completed', 'failed', 'cancelled', 'unresolved'
    ))
) STRICT;

CREATE INDEX kernel_receipts_owner_status_idx
ON kernel_receipts(owner, final_status, updated_at_ms);

CREATE TABLE kernel_effects (
    owner TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    run_id TEXT,
    receipt_id TEXT,
    applied_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, effect_id),
    CHECK (length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE INDEX kernel_effects_owner_run_idx
ON kernel_effects(owner, run_id);

CREATE TABLE kernel_approvals (
    owner TEXT NOT NULL,
    approval_id TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    decision TEXT NOT NULL,
    decided_by TEXT,
    bound_revision INTEGER NOT NULL,
    bound_lease_owner TEXT,
    bound_generation INTEGER,
    decided_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, approval_id),
    UNIQUE (owner, receipt_id, request_digest),
    CHECK (decision IN ('pending', 'approved', 'denied')),
    CHECK (bound_revision >= 0),
    CHECK (length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE INDEX kernel_approvals_owner_receipt_idx
ON kernel_approvals(owner, receipt_id, decision, updated_at_ms);

CREATE TABLE kernel_artifacts (
    owner TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    path TEXT NOT NULL,
    content_bytes INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, receipt_id, digest),
    CHECK (content_bytes >= 0),
    CHECK (length(digest) = 64 AND digest NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE TABLE kernel_lease_fences (
    owner TEXT NOT NULL,
    run_id TEXT NOT NULL,
    lease_owner TEXT NOT NULL,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    token TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    expires_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, run_id),
    CHECK (generation >= 0),
    CHECK (revision >= 0)
) STRICT;

CREATE TABLE kernel_delivery (
    owner TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    revision INTEGER NOT NULL,
    approval_id TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, receipt_id),
    CHECK (revision >= 0),
    CHECK (phase IN (
        'running',
        'approval_required',
        'approved',
        'denied',
        'resuming',
        'verified',
        'completed',
        'failed',
        'cancelled'
    ))
) STRICT;

-- Schema v8: bind review execution to exact successful takeover evidence.
--
-- The original approval table remains the single durable queue for continuity
-- execution. Review rows bind the terminal takeover approval and run they are
-- reviewing; takeover rows keep these columns NULL.

ALTER TABLE goal_takeover_approvals
    ADD COLUMN upstream_approval_id TEXT;
ALTER TABLE goal_takeover_approvals
    ADD COLUMN upstream_run_id TEXT;
ALTER TABLE goal_takeover_approvals
    ADD COLUMN upstream_run_status TEXT CHECK (
        upstream_run_status IS NULL OR upstream_run_status = 'success');

CREATE INDEX goal_takeover_approvals_upstream_idx
    ON goal_takeover_approvals(owner, upstream_approval_id);

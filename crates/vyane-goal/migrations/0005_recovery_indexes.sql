CREATE INDEX goals_owner_worker_lease_idx
    ON goals(owner, status, claimed_by, claim_expires_at_ms);

CREATE INDEX goals_owner_lease_idx
    ON goals(owner, status, claim_expires_at_ms);

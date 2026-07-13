CREATE TABLE agent_projector_dispositions (
    owner TEXT NOT NULL,
    projector TEXT NOT NULL,
    event_sequence INTEGER NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('deferred', 'quarantined')),
    reason TEXT NOT NULL,
    not_before_ms INTEGER,
    recorded_at_ms INTEGER NOT NULL,
    PRIMARY KEY (owner, projector, event_sequence),
    FOREIGN KEY (owner, event_sequence)
      REFERENCES agent_events(owner, sequence) ON DELETE RESTRICT,
    CHECK (
        (disposition = 'deferred'
            AND reason IN ('sink_unavailable', 'missing_sink')
            AND not_before_ms IS NOT NULL
            AND not_before_ms > recorded_at_ms)
        OR
        (disposition = 'quarantined'
            AND reason IN ('invalid_event', 'sink_conflict')
            AND not_before_ms IS NULL)
    )
) STRICT;

CREATE INDEX agent_projector_dispositions_due_idx
ON agent_projector_dispositions(owner, projector, disposition, not_before_ms, event_sequence);

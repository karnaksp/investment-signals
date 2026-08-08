-- A small BRIN index supports bounded retention without recreating the large
-- per-row timestamp B-tree removed in 0109.  Rows arrive in time order, which
-- is the ideal BRIN access pattern.
CREATE INDEX IF NOT EXISTS processed_events_processed_at_brin_idx
    ON processed_events USING BRIN (processed_at)
    WITH (pages_per_range = 32);

-- Supports both the retention anti-join and PostgreSQL's foreign-key check
-- when an unreferenced inbox row is removed.
CREATE INDEX IF NOT EXISTS detector_observation_outbox_source_event_idx
    ON detector_observation_outbox (source_event_id);

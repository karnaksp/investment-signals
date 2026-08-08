-- Published detector observations have already been durably acknowledged by
-- ClickHouse. Keep one day for operational inspection while retaining every
-- pending, failed, and dead-letter row indefinitely.
CREATE INDEX IF NOT EXISTS detector_observation_outbox_published_retention_idx
    ON detector_observation_outbox (published_at, observation_id)
    WHERE status = 'published';

CREATE OR REPLACE FUNCTION reject_detector_observation_outbox_delete()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status <> 'published'
       OR OLD.published_at IS NULL
       OR OLD.published_at > now() - INTERVAL '24 hours' THEN
        RAISE EXCEPTION 'only published detector observations past the safety window may be purged';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Snapshot rows are updated for every accepted broker event. Vacuuming after
-- ten row versions caused near-continuous maintenance and repeatedly scanned
-- the TOAST relation. The compressed v2 payload keeps the bounded interval
-- below small-machine disk limits while these thresholds avoid vacuum storms.
ALTER TABLE detector_state_snapshots SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_analyze_scale_factor = 0.10,
    autovacuum_analyze_threshold = 50000,
    toast.autovacuum_vacuum_scale_factor = 0.05,
    toast.autovacuum_vacuum_threshold = 10000
);

-- The detector snapshot is addressed by instrument_id.  The two additional
-- UNIQUE constraints changed on every event and prevented HOT updates, while
-- adding no integrity beyond the monotonic-advance trigger.
ALTER TABLE detector_state_snapshots
    DROP CONSTRAINT IF EXISTS detector_state_snapshots_source_event_id_key,
    DROP CONSTRAINT IF EXISTS detector_state_snapshots_topic_partition_id_offset_id_key;

-- No production query uses the per-event timestamp index.  The primary key
-- and broker-position unique index continue to protect idempotent processing.
DROP INDEX IF EXISTS processed_events_processed_at_idx;

ALTER TABLE detector_state_snapshots SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 10,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 10,
    toast.autovacuum_vacuum_scale_factor = 0.01,
    toast.autovacuum_vacuum_threshold = 10
);

ALTER TABLE detector_observation_outbox SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 100,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold = 100,
    toast.autovacuum_vacuum_scale_factor = 0.02,
    toast.autovacuum_vacuum_threshold = 100
);

ALTER TABLE processed_events SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold = 1000
);

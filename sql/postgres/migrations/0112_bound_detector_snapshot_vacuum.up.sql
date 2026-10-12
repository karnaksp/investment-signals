-- Migration 0111 may already be installed with the pre-compression thresholds.
-- Retune the hot snapshot table now that checkpoint payloads use zlib.
ALTER TABLE detector_state_snapshots SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 50000,
    toast.autovacuum_vacuum_scale_factor = 0.0,
    toast.autovacuum_vacuum_threshold = 5000,
    autovacuum_vacuum_cost_delay = 10,
    autovacuum_vacuum_cost_limit = 500
);

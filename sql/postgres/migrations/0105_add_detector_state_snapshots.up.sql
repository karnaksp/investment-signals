CREATE TABLE detector_state_snapshots (
    instrument_id TEXT PRIMARY KEY,
    source_event_id TEXT NOT NULL UNIQUE
        REFERENCES processed_events(event_id) ON DELETE RESTRICT,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    offset_id BIGINT NOT NULL CHECK (offset_id >= 0),
    state_schema_version TEXT NOT NULL
        CHECK (length(state_schema_version) BETWEEN 1 AND 64),
    detector_config_version TEXT NOT NULL
        CHECK (length(detector_config_version) BETWEEN 1 AND 128),
    snapshot_payload BYTEA NOT NULL,
    snapshot_sha256 BYTEA NOT NULL CHECK (octet_length(snapshot_sha256) = 32),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, partition_id, offset_id)
);

CREATE OR REPLACE FUNCTION validate_detector_state_snapshot_advance()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.instrument_id IS DISTINCT FROM OLD.instrument_id THEN
        RAISE EXCEPTION 'detector state instrument identity is immutable';
    END IF;
    IF NEW.topic IS DISTINCT FROM OLD.topic
       OR NEW.partition_id IS DISTINCT FROM OLD.partition_id
       OR NEW.offset_id <= OLD.offset_id THEN
        RAISE EXCEPTION 'detector state snapshot must advance one broker partition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER detector_state_snapshots_advance_guard
BEFORE UPDATE ON detector_state_snapshots
FOR EACH ROW EXECUTE FUNCTION validate_detector_state_snapshot_advance();

CREATE OR REPLACE FUNCTION reject_detector_state_snapshot_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'detector state snapshots may not be deleted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER detector_state_snapshots_delete_guard
BEFORE DELETE ON detector_state_snapshots
FOR EACH ROW EXECUTE FUNCTION reject_detector_state_snapshot_delete();

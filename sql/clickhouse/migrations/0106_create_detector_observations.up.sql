CREATE TABLE IF NOT EXISTS signal_engine.detector_observations
(
    signal_type LowCardinality(String),
    instrument_id LowCardinality(String),
    session_date Date,
    observed_at DateTime64(6, 'UTC'),
    observation_id UUID,
    source_event_id String,
    detector_config_version LowCardinality(String),
    expectation_catalog_version LowCardinality(String),
    metric_value Float64,
    threshold_value Float64,
    threshold_passed UInt8,
    sample_weight Float64,
    features_json String,
    payload_fingerprint FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_date)
ORDER BY (signal_type, instrument_id, session_date, observed_at, observation_id)
TTL toDateTime(observed_at) + toIntervalDay(35)
SETTINGS index_granularity = 8192;

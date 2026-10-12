CREATE DATABASE IF NOT EXISTS signal_engine;

CREATE TABLE IF NOT EXISTS signal_engine.market_signals
(
    signal_id UUID,
    detected_at DateTime64(3, 'UTC'),
    instrument_id LowCardinality(String),
    ticker LowCardinality(String),
    class_code LowCardinality(String),
    alias LowCardinality(String),
    source_event_type LowCardinality(String),
    signal_type LowCardinality(String),
    severity UInt8,
    metric_value Float64,
    baseline_value Float64,
    z_score Float64,
    window_seconds UInt32,
    summary String,
    payload_json String,
    source_event_id String DEFAULT '',
    source_event_at DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC'),
    signal_schema_version LowCardinality(String) DEFAULT '1.0.0',
    expectation_catalog_version LowCardinality(String) DEFAULT '',
    detector_config_version LowCardinality(String) DEFAULT '',
    delivery_config_version LowCardinality(String) DEFAULT '',
    cost_model_version LowCardinality(String) DEFAULT '',
    provenance_status LowCardinality(String) DEFAULT 'legacy'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(detected_at)
ORDER BY (signal_type, instrument_id, toDate(detected_at), detected_at, signal_id)
TTL toDateTime(detected_at) + INTERVAL 365 DAY DELETE;

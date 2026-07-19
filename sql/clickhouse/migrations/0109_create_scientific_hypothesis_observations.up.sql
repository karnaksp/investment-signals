CREATE TABLE IF NOT EXISTS signal_engine.scientific_hypothesis_observations
(
    observation_id String,
    hypothesis_id LowCardinality(String),
    hypothesis_version LowCardinality(String),
    policy_version LowCardinality(String),
    formula_version LowCardinality(String),
    formula_fingerprint String,
    scientific_source_ids Array(String),
    instrument_id LowCardinality(String),
    ticker LowCardinality(String),
    trading_day Date,
    observed_at DateTime64(6, 'UTC'),
    feature_max_observed_at DateTime64(6, 'UTC'),
    model_trained_until Nullable(DateTime64(6, 'UTC')),
    market_phase LowCardinality(String),
    phase_bucket LowCardinality(String),
    decision Enum8('matched' = 1, 'not_matched' = 2, 'abstain' = 3),
    reason_code LowCardinality(String),
    expected_direction Int8,
    forecast_value Nullable(Float64),
    target_metric LowCardinality(String),
    effect_unit LowCardinality(String),
    claim_scope LowCardinality(String),
    horizon_seconds UInt32,
    target_at DateTime64(6, 'UTC'),
    feature_values_json String,
    thresholds_json String,
    input_window_start DateTime64(6, 'UTC'),
    input_window_end DateTime64(6, 'UTC'),
    source_kind Enum8('stream' = 1, 'historical_backfill' = 2),
    source_max_observed_at DateTime64(6, 'UTC'),
    has_gap UInt8,
    source_event_ids Array(String),
    input_fingerprint String,
    dataset_fingerprint String,
    config_fingerprint String,
    payload_fingerprint String,
    recorded_at DateTime64(6, 'UTC'),
    record_version UInt64
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(trading_day)
ORDER BY (
    hypothesis_id,
    hypothesis_version,
    instrument_id,
    trading_day,
    observed_at,
    observation_id
)
TTL toDateTime(observed_at) + toIntervalDay(365)
SETTINGS index_granularity = 8192;

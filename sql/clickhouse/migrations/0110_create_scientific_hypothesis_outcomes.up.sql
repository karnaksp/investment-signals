CREATE TABLE IF NOT EXISTS signal_engine.scientific_hypothesis_outcomes
(
    outcome_id String,
    observation_id String,
    hypothesis_id LowCardinality(String),
    hypothesis_version LowCardinality(String),
    instrument_id LowCardinality(String),
    trading_day Date,
    target_at DateTime64(6, 'UTC'),
    observed_range_start DateTime64(6, 'UTC'),
    observed_range_end DateTime64(6, 'UTC'),
    available UInt8,
    reason_code LowCardinality(String),
    actual_value Nullable(Float64),
    cost_adjusted_value Nullable(Float64),
    model_loss Nullable(Float64),
    benchmark_loss Nullable(Float64),
    supported Nullable(UInt8),
    target_metric LowCardinality(String),
    effect_unit LowCardinality(String),
    outcome_policy_version LowCardinality(String),
    source_event_ids Array(String),
    source_window_start DateTime64(6, 'UTC'),
    source_window_end DateTime64(6, 'UTC'),
    source_max_observed_at DateTime64(6, 'UTC'),
    input_fingerprint String,
    evaluated_at DateTime64(6, 'UTC'),
    payload_fingerprint String,
    record_version UInt64
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(trading_day)
ORDER BY (
    hypothesis_id,
    hypothesis_version,
    instrument_id,
    trading_day,
    target_at,
    observation_id,
    outcome_policy_version
)
TTL toDateTime(target_at) + toIntervalDay(365)
SETTINGS index_granularity = 8192;

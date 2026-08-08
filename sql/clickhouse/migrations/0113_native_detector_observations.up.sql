-- Core owns detector-observation persistence. Keep the legacy physical table
-- readable during the transition: ClickHouse DDL and the PostgreSQL migration
-- ledger do not share a transaction, so an in-place rename/swap is not safely
-- retryable after an ambiguous runner failure.
CREATE TABLE IF NOT EXISTS signal_engine.detector_observations_v2
(
    signal_type LowCardinality(String),
    instrument_id LowCardinality(String),
    session_date Date,
    observed_at DateTime64(9, 'UTC'),
    observation_id UUID,
    source_event_id String,
    detector_config_version LowCardinality(String),
    expectation_catalog_version LowCardinality(String),
    metric_value Float64,
    threshold_value Float64,
    threshold_passed Bool,
    sample_weight Float64 DEFAULT 1,
    features_json String DEFAULT '',
    payload_fingerprint FixedString(64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY
(
    signal_type,
    instrument_id,
    session_date,
    observed_at,
    observation_id
)
TTL toDateTime(observed_at) + INTERVAL 35 DAY DELETE
SETTINGS
    index_granularity = 8192,
    old_parts_lifetime = 60;

-- The anti join makes a retry idempotent. DISTINCT removes only byte-identical
-- transport replays; conflicting fingerprints remain visible to evidence
-- readers and therefore cannot be silently canonicalised.
INSERT INTO signal_engine.detector_observations_v2
(
    signal_type,
    instrument_id,
    session_date,
    observed_at,
    observation_id,
    source_event_id,
    detector_config_version,
    expectation_catalog_version,
    metric_value,
    threshold_value,
    threshold_passed,
    sample_weight,
    features_json,
    payload_fingerprint
)
SELECT
    source.signal_type,
    source.instrument_id,
    source.session_date,
    source.observed_at,
    source.observation_id,
    source.source_event_id,
    source.detector_config_version,
    source.expectation_catalog_version,
    source.metric_value,
    source.threshold_value,
    source.threshold_passed,
    source.sample_weight,
    source.features_json,
    source.payload_fingerprint
FROM
(
    SELECT DISTINCT
        signal_type,
        instrument_id,
        session_date,
        parseDateTime64BestEffort(toString(observed_at), 9, 'UTC') AS observed_at,
        observation_id,
        source_event_id,
        detector_config_version,
        expectation_catalog_version,
        metric_value,
        threshold_value,
        toBool(threshold_passed) AS threshold_passed,
        sample_weight,
        features_json,
        payload_fingerprint
    FROM signal_engine.detector_observations
) AS source
LEFT ANTI JOIN
(
    SELECT observation_id, payload_fingerprint
    FROM signal_engine.detector_observations_v2
    GROUP BY observation_id, payload_fingerprint
) AS existing
    ON existing.observation_id = source.observation_id
   AND existing.payload_fingerprint = source.payload_fingerprint;

-- Bound the legacy table while a release is still reading it. New writes go
-- only to detector_observations_v2 after this migration succeeds.
ALTER TABLE signal_engine.detector_observations
    MODIFY TTL
        toDateTime(
            parseDateTime64BestEffort(toString(observed_at), 9, 'UTC')
        ) + INTERVAL 35 DAY DELETE;

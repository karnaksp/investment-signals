-- Raw market stream: Kafka Engine -> Materialized View -> MergeTree (analytics / backtest).
-- ``kafka_format = 'JSONAsString'`` ожидает JSON в Kafka. При ``KAFKA_RAW_VALUE_FORMAT=protobuf``
-- этот пайплайн не сможет парсить сообщения — оставьте JSON для ClickHouse или вынесите
-- сырой поток в отдельный JSON-топик (см. README).
-- ORDER BY: low-to-high cardinality per ClickHouse best practices (event_type, day, instrument, time, id).
-- PARTITION: monthly bounds partition count (schema-partition-low-cardinality).

CREATE DATABASE IF NOT EXISTS signal_engine;

CREATE TABLE IF NOT EXISTS signal_engine.market_raw_kafka_queue
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'marketdata.raw',
    kafka_group_name = 'clickhouse_signal_engine_raw_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_poll_max_batch_size = 65536,
    kafka_flush_interval_ms = 5000;

CREATE TABLE IF NOT EXISTS signal_engine.market_raw_events
(
    event_id String,
    event_type LowCardinality(String),
    instrument_id String,
    ticker LowCardinality(String),
    class_code LowCardinality(String),
    alias LowCardinality(String),
    figi String,
    uid String,
    source_time DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    payload_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(source_time))
ORDER BY (event_type, toDate(source_time), instrument_id, source_time, event_id)
-- CH 24.8+: TTL must be Date/DateTime, not DateTime64 (BAD_TTL_EXPRESSION).
TTL toDateTime(source_time) + toIntervalDay(3)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS signal_engine.market_raw_consumer
TO signal_engine.market_raw_events
AS
SELECT
    JSONExtractString(message, 'event_id') AS event_id,
    JSONExtractString(message, 'event_type') AS event_type,
    JSONExtractString(message, 'instrument_id') AS instrument_id,
    JSONExtractString(message, 'ticker') AS ticker,
    JSONExtractString(message, 'class_code') AS class_code,
    JSONExtractString(message, 'alias') AS alias,
    JSONExtractString(message, 'figi') AS figi,
    JSONExtractString(message, 'uid') AS uid,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(message, 'source_time'), 3, 'UTC'),
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC')
    ) AS source_time,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(message, 'received_at'), 3, 'UTC'),
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC')
    ) AS received_at,
    ifNull(JSONExtractRaw(message, 'payload'), '{}') AS payload_json
FROM signal_engine.market_raw_kafka_queue;

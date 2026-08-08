-- Kafka-engine settings cannot be changed with ALTER TABLE on ClickHouse
-- 25.3. Recreate only the disposable Kafka transport and its materialized
-- view; the durable market_raw_events MergeTree and the broker offsets remain
-- intact. Every statement is retry-safe after a partially completed run.
DROP TABLE IF EXISTS signal_engine.market_raw_consumer SYNC;

DROP TABLE IF EXISTS signal_engine.market_raw_kafka_queue SYNC;

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
    kafka_flush_interval_ms = 30000;

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
        parseDateTime64BestEffortOrNull(
            JSONExtractString(message, 'source_time'),
            3,
            'UTC'
        ),
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC')
    ) AS source_time,
    coalesce(
        parseDateTime64BestEffortOrNull(
            JSONExtractString(message, 'received_at'),
            3,
            'UTC'
        ),
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC')
    ) AS received_at,
    ifNull(JSONExtractRaw(message, 'payload'), '{}') AS payload_json
FROM signal_engine.market_raw_kafka_queue;

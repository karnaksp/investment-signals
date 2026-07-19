CREATE TABLE IF NOT EXISTS signal_engine.scientific_candles_1m
(
    instrument_id LowCardinality(String),
    ticker LowCardinality(String),
    exchange LowCardinality(String),
    trading_day Date,
    candle_at DateTime64(6, 'UTC'),
    open_price Decimal(18, 9),
    high_price Decimal(18, 9),
    low_price Decimal(18, 9),
    close_price Decimal(18, 9),
    volume UInt64,
    is_complete UInt8,
    source_kind Enum8('backfill' = 1, 'stream' = 2),
    source_at DateTime64(6, 'UTC'),
    received_at DateTime64(6, 'UTC'),
    source_event_id String,
    payload_fingerprint FixedString(64),
    has_gap UInt8 DEFAULT 0,
    schema_version LowCardinality(String),
    record_version UInt64
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(trading_day)
ORDER BY (instrument_id, trading_day, candle_at)
TTL toDateTime(candle_at) + toIntervalDay(365)
SETTINGS index_granularity = 8192;

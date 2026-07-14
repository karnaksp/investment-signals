CREATE TABLE IF NOT EXISTS signal_engine.market_reference_ticks
(
    instrument_id LowCardinality(String),
    event_at DateTime64(9, 'UTC'),
    received_at DateTime64(9, 'UTC'),
    event_id UUID,
    source_kind LowCardinality(String),
    bid_price Decimal(18, 9) DEFAULT 0,
    ask_price Decimal(18, 9) DEFAULT 0,
    last_price Decimal(18, 9) DEFAULT 0,
    trade_price Decimal(18, 9) DEFAULT 0,
    bid_quantity UInt64 DEFAULT 0,
    ask_quantity UInt64 DEFAULT 0,
    has_valid_book UInt8 DEFAULT 0,
    has_last_price UInt8 DEFAULT 0,
    has_trade UInt8 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(event_at))
ORDER BY (instrument_id, toDate(event_at), event_at, event_id)
TTL toDateTime(event_at) + toIntervalDay(35)
SETTINGS index_granularity = 8192;

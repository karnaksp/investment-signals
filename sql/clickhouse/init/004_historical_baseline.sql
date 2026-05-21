-- Historical slot baselines: per-instrument distributions by minute-of-day (UTC)
-- built from trade feature bars. Longer TTL than raw events (schema-partition-lifecycle).
-- ORDER BY aligns with typical filter: instrument_id + timeframe + metric + slot.

-- Долгоживущие дневные факты по слоту UTC: наполняются из bars (сид) и из сырья за последние N дней
-- (Dagster). Сырой market_raw_events — короткий TTL; эта таблица держит историю (TTL 200 дн.).
CREATE TABLE IF NOT EXISTS signal_engine.trade_slot_daily
(
    trading_day Date,
    instrument_id String,
    timeframe LowCardinality(String),
    slot_minute UInt16,
    sum_qty Float64,
    n_trades UInt64,
    sum_pv Float64,
    open_px Float64,
    max_px Float64,
    min_px Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trading_day)
ORDER BY (instrument_id, timeframe, trading_day, slot_minute)
TTL trading_day + toIntervalDay(200)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS signal_engine.historical_baseline_slot_stats
(
    computed_at DateTime('UTC'),
    instrument_id String,
    timeframe LowCardinality(String),
    slot_minute UInt16,
    metric LowCardinality(String),
    median Float64,
    p90 Float64,
    p95 Float64,
    p99 Float64,
    sample_days UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (instrument_id, timeframe, metric, slot_minute, computed_at)
TTL computed_at + toIntervalDay(200)
SETTINGS index_granularity = 8192;

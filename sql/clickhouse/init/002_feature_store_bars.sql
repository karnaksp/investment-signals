-- Feature-store bars: incremental MVs from market_raw_events (per query-mv-incremental).
-- SummingMergeTree merges partial inserts; query via vw_* views with GROUP BY.
-- VWAP = sum(price * qty) / sum(qty); tempo = n_trades / window_seconds.

CREATE TABLE IF NOT EXISTS signal_engine.features_trade_bar_1m
(
    instrument_id LowCardinality(String),
    bucket DateTime,
    sum_pv Float64,
    sum_qty Float64,
    n_trades UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (instrument_id, bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS signal_engine.mv_features_trade_bar_1m
TO signal_engine.features_trade_bar_1m
AS
SELECT
    instrument_id,
    toStartOfMinute(source_time) AS bucket,
    qty * px AS sum_pv,
    qty AS sum_qty,
    toUInt64(1) AS n_trades
FROM
(
    SELECT
        instrument_id,
        source_time,
        JSONExtractFloat(payload_json, 'quantity') AS qty,
        toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'units'), 0))
            + toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'nano'), 0)) / 1000000000. AS px
    FROM signal_engine.market_raw_events
    WHERE event_type = 'trade'
      AND JSONHas(payload_json, 'quantity')
      AND JSONHas(payload_json, 'price')
) AS decoded
WHERE qty > 0 AND px > 0 AND isFinite(qty) AND isFinite(px);

CREATE TABLE IF NOT EXISTS signal_engine.features_trade_bar_5m
(
    instrument_id LowCardinality(String),
    bucket DateTime,
    sum_pv Float64,
    sum_qty Float64,
    n_trades UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (instrument_id, bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS signal_engine.mv_features_trade_bar_5m
TO signal_engine.features_trade_bar_5m
AS
SELECT
    instrument_id,
    toStartOfFiveMinutes(source_time) AS bucket,
    qty * px AS sum_pv,
    qty AS sum_qty,
    toUInt64(1) AS n_trades
FROM
(
    SELECT
        instrument_id,
        source_time,
        JSONExtractFloat(payload_json, 'quantity') AS qty,
        toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'units'), 0))
            + toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'nano'), 0)) / 1000000000. AS px
    FROM signal_engine.market_raw_events
    WHERE event_type = 'trade'
      AND JSONHas(payload_json, 'quantity')
      AND JSONHas(payload_json, 'price')
) AS decoded
WHERE qty > 0 AND px > 0 AND isFinite(qty) AND isFinite(px);

CREATE TABLE IF NOT EXISTS signal_engine.features_trade_bar_15m
(
    instrument_id LowCardinality(String),
    bucket DateTime,
    sum_pv Float64,
    sum_qty Float64,
    n_trades UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (instrument_id, bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS signal_engine.mv_features_trade_bar_15m
TO signal_engine.features_trade_bar_15m
AS
SELECT
    instrument_id,
    toStartOfFifteenMinutes(source_time) AS bucket,
    qty * px AS sum_pv,
    qty AS sum_qty,
    toUInt64(1) AS n_trades
FROM
(
    SELECT
        instrument_id,
        source_time,
        JSONExtractFloat(payload_json, 'quantity') AS qty,
        toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'units'), 0))
            + toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'nano'), 0)) / 1000000000. AS px
    FROM signal_engine.market_raw_events
    WHERE event_type = 'trade'
      AND JSONHas(payload_json, 'quantity')
      AND JSONHas(payload_json, 'price')
) AS decoded
WHERE qty > 0 AND px > 0 AND isFinite(qty) AND isFinite(px);

CREATE VIEW IF NOT EXISTS signal_engine.vw_trade_bar_1m_vwap AS
SELECT
    instrument_id,
    bucket,
    vwap,
    trades_total AS n_trades,
    trades_total / 60. AS trades_per_sec
FROM
(
    SELECT
        instrument_id,
        bucket,
        sum(sum_pv) / sum(sum_qty) AS vwap,
        sum(n_trades) AS trades_total
    FROM signal_engine.features_trade_bar_1m
    GROUP BY instrument_id, bucket
    HAVING sum(sum_qty) > 0
);

CREATE VIEW IF NOT EXISTS signal_engine.vw_trade_bar_5m_vwap AS
SELECT
    instrument_id,
    bucket,
    vwap,
    trades_total AS n_trades,
    trades_total / 300. AS trades_per_sec
FROM
(
    SELECT
        instrument_id,
        bucket,
        sum(sum_pv) / sum(sum_qty) AS vwap,
        sum(n_trades) AS trades_total
    FROM signal_engine.features_trade_bar_5m
    GROUP BY instrument_id, bucket
    HAVING sum(sum_qty) > 0
);

CREATE VIEW IF NOT EXISTS signal_engine.vw_trade_bar_15m_vwap AS
SELECT
    instrument_id,
    bucket,
    vwap,
    trades_total AS n_trades,
    trades_total / 900. AS trades_per_sec
FROM
(
    SELECT
        instrument_id,
        bucket,
        sum(sum_pv) / sum(sum_qty) AS vwap,
        sum(n_trades) AS trades_total
    FROM signal_engine.features_trade_bar_15m
    GROUP BY instrument_id, bucket
    HAVING sum(sum_qty) > 0
);

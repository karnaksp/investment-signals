-- Standard profile has at most 25 instruments. Dictionary encoding avoids
-- repeating instrument identifiers in each time-series row.
ALTER TABLE signal_engine.market_raw_events
    MODIFY COLUMN instrument_id LowCardinality(String);

ALTER TABLE signal_engine.market_signals
    MODIFY COLUMN instrument_id LowCardinality(String);

ALTER TABLE signal_engine.features_trade_bar_1m
    MODIFY COLUMN instrument_id LowCardinality(String);

ALTER TABLE signal_engine.features_trade_bar_5m
    MODIFY COLUMN instrument_id LowCardinality(String);

ALTER TABLE signal_engine.features_trade_bar_15m
    MODIFY COLUMN instrument_id LowCardinality(String);

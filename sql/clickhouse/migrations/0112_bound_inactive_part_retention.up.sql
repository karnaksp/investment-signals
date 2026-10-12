-- A single-node live installation does not need ClickHouse's eight-minute
-- rollback window for parts replaced by merges. A shorter window bounds disk
-- amplification without changing active rows, table TTLs, or query semantics.
ALTER TABLE signal_engine.detector_observations
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.features_trade_bar_15m
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.features_trade_bar_1m
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.features_trade_bar_5m
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.market_raw_events
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.market_reference_ticks
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.market_signals
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.scientific_candles_1m
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.scientific_hypothesis_observations
    MODIFY SETTING old_parts_lifetime = 60;
ALTER TABLE signal_engine.scientific_hypothesis_outcomes
    MODIFY SETTING old_parts_lifetime = 60;

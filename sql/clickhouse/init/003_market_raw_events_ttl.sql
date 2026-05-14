-- Поджать TTL для уже существующих инсталляций (001 мог создать таблицу со старым сроком).
-- Сырые события в CH — короткий буфер; долгая история сигналов в Postgres / Kafka marketdata.signals.
ALTER TABLE signal_engine.market_raw_events
    MODIFY TTL toDateTime(source_time) + toIntervalDay(7);

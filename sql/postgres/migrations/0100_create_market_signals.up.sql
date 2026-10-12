CREATE TABLE IF NOT EXISTS market_signals (
    signal_id UUID PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL,
    instrument_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    class_code TEXT NOT NULL,
    alias TEXT NOT NULL,
    source_event_type TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    severity SMALLINT NOT NULL CHECK (severity BETWEEN 1 AND 3),
    metric_value DOUBLE PRECISION NOT NULL,
    baseline_value DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    window_seconds INTEGER NOT NULL,
    summary TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS market_signals_detected_at_idx
    ON market_signals (detected_at DESC);

CREATE INDEX IF NOT EXISTS market_signals_instrument_detected_idx
    ON market_signals (instrument_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS market_signals_type_detected_idx
    ON market_signals (signal_type, detected_at DESC);

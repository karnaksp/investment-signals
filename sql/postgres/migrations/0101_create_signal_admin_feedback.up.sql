-- Ручная разметка сигналов в админке (полезно / шум / не уверен).
CREATE TABLE IF NOT EXISTS signal_admin_feedback (
    signal_id UUID PRIMARY KEY
        REFERENCES market_signals (signal_id) ON DELETE CASCADE,
    label TEXT NOT NULL
        CHECK (label IN ('useful', 'noise', 'unsure')),
    note TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS signal_admin_feedback_label_idx
    ON signal_admin_feedback (label);

CREATE INDEX IF NOT EXISTS signal_admin_feedback_updated_idx
    ON signal_admin_feedback (updated_at DESC);

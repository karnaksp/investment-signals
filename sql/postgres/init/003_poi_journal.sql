CREATE TABLE IF NOT EXISTS poi_journal (
    poi_id UUID PRIMARY KEY,
    instrument_id TEXT NOT NULL DEFAULT '',
    ticker TEXT NOT NULL DEFAULT '',
    setup_type TEXT NOT NULL DEFAULT '',
    bias TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL
        CHECK (
            action IN (
                'watch',
                'dismiss',
                'paper_long',
                'paper_short',
                'missed',
                'useful',
                'noise',
                'unsure'
            )
        ),
    note TEXT NOT NULL DEFAULT '',
    entry_price DOUBLE PRECISION,
    exit_price DOUBLE PRECISION,
    result TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS poi_journal_updated_idx
    ON poi_journal (updated_at DESC);

CREATE INDEX IF NOT EXISTS poi_journal_action_idx
    ON poi_journal (action);

CREATE INDEX IF NOT EXISTS poi_journal_ticker_idx
    ON poi_journal (ticker);

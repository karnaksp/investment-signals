ALTER TABLE market_signals
    ADD COLUMN IF NOT EXISTS source_event_id TEXT,
    ADD COLUMN IF NOT EXISTS source_event_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_schema_version TEXT NOT NULL DEFAULT '1.0.0',
    ADD COLUMN IF NOT EXISTS expectation_catalog_version TEXT,
    ADD COLUMN IF NOT EXISTS detector_config_version TEXT,
    ADD COLUMN IF NOT EXISTS delivery_config_version TEXT,
    ADD COLUMN IF NOT EXISTS cost_model_version TEXT,
    ADD COLUMN IF NOT EXISTS provenance_status TEXT NOT NULL DEFAULT 'legacy';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'market_signals_provenance_status_check'
          AND conrelid = 'market_signals'::regclass
    ) THEN
        ALTER TABLE market_signals
            ADD CONSTRAINT market_signals_provenance_status_check
            CHECK (provenance_status IN ('complete', 'legacy'));
    END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS market_signals_source_event_type_uq
    ON market_signals (source_event_id, signal_type)
    WHERE source_event_id IS NOT NULL;

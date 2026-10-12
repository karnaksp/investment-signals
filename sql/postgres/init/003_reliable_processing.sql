-- Development compatibility. Product upgrades use migration 0103.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    offset_id BIGINT NOT NULL CHECK (offset_id >= 0),
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, partition_id, offset_id)
);

CREATE INDEX IF NOT EXISTS processed_events_processed_at_idx
    ON processed_events (processed_at DESC);

CREATE TABLE IF NOT EXISTS delivery_outbox (
    outbox_id UUID PRIMARY KEY,
    signal_id UUID NOT NULL
        REFERENCES market_signals(signal_id) ON DELETE CASCADE,
    destination_type TEXT NOT NULL
        CHECK (destination_type IN ('telegram', 'webhook')),
    destination_key_hash BYTEA NOT NULL
        CHECK (octet_length(destination_key_hash) = 32),
    payload_json JSONB NOT NULL
        CHECK (jsonb_typeof(payload_json) = 'object'),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (
            status IN (
                'pending', 'delivering', 'delivered', 'failed', 'dead_letter'
            )
        ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    UNIQUE (signal_id, destination_type, destination_key_hash)
);

CREATE INDEX IF NOT EXISTS delivery_outbox_ready_idx
    ON delivery_outbox (next_attempt_at, outbox_id)
    WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS delivery_outbox_reclaim_idx
    ON delivery_outbox (next_attempt_at, outbox_id)
    WHERE status = 'delivering';

CREATE INDEX IF NOT EXISTS delivery_outbox_signal_idx
    ON delivery_outbox (signal_id);

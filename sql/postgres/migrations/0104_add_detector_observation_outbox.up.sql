CREATE TABLE IF NOT EXISTS detector_observation_outbox (
    observation_id UUID PRIMARY KEY,
    source_event_id TEXT NOT NULL
        REFERENCES processed_events(event_id) ON DELETE RESTRICT,
    payload_json JSONB NOT NULL
        CHECK (jsonb_typeof(payload_json) = 'object'),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'publishing', 'published', 'failed', 'dead_letter')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CHECK ((status = 'publishing') = (claimed_at IS NOT NULL)),
    CHECK ((status = 'published') = (published_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS detector_observation_outbox_ready_idx
    ON detector_observation_outbox (next_attempt_at, observation_id)
    WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS detector_observation_outbox_reclaim_idx
    ON detector_observation_outbox (claimed_at, observation_id)
    WHERE status = 'publishing';

CREATE OR REPLACE FUNCTION protect_detector_observation_outbox()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.observation_id IS DISTINCT FROM OLD.observation_id
       OR NEW.source_event_id IS DISTINCT FROM OLD.source_event_id
       OR NEW.payload_json IS DISTINCT FROM OLD.payload_json
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'detector observation identity and payload are immutable';
    END IF;
    IF OLD.status IN ('published', 'dead_letter') THEN
        RAISE EXCEPTION 'terminal detector observation outbox row is immutable';
    END IF;
    IF NOT (
        (OLD.status IN ('pending', 'failed') AND NEW.status = 'publishing')
        OR (OLD.status = 'publishing' AND NEW.status IN ('published', 'failed', 'dead_letter'))
    ) THEN
        RAISE EXCEPTION 'invalid detector observation outbox transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER detector_observation_outbox_state_guard
BEFORE UPDATE ON detector_observation_outbox
FOR EACH ROW EXECUTE FUNCTION protect_detector_observation_outbox();

CREATE OR REPLACE FUNCTION reject_detector_observation_outbox_delete()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status <> 'published'
       OR OLD.published_at IS NULL
       OR OLD.published_at > now() - INTERVAL '7 days' THEN
        RAISE EXCEPTION 'only published detector observations past the safety window may be purged';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER detector_observation_outbox_delete_guard
BEFORE DELETE ON detector_observation_outbox
FOR EACH ROW EXECUTE FUNCTION reject_detector_observation_outbox_delete();

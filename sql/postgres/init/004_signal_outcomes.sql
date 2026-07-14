-- Development compatibility. Product upgrades use migrations 0107-0108.
CREATE TABLE IF NOT EXISTS core_directional_signal_outcomes (
    outcome_id UUID PRIMARY KEY,
    signal_id UUID NOT NULL
        REFERENCES market_signals(signal_id) ON DELETE CASCADE,
    instrument_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    source_event_at TIMESTAMPTZ NOT NULL,
    horizon_seconds INTEGER NOT NULL CHECK (horizon_seconds > 0),
    verdict TEXT NOT NULL
        CHECK (
            verdict IN (
                'confirmed', 'contradicted', 'insignificant', 'inconclusive'
            )
        ),
    reason_code TEXT NOT NULL CHECK (length(reason_code) > 0),
    expected_direction SMALLINT NOT NULL CHECK (expected_direction IN (-1, 1)),
    anchor_price NUMERIC,
    forward_price NUMERIC,
    raw_return_bps NUMERIC,
    net_expected_bps NUMERIC,
    net_reverse_bps NUMERIC,
    materiality_bps NUMERIC NOT NULL CHECK (materiality_bps >= 0),
    cost_model_version TEXT NOT NULL CHECK (length(cost_model_version) > 0),
    policy_version TEXT NOT NULL CHECK (length(policy_version) > 0),
    inverse_hypothesis_candidate BOOLEAN NOT NULL DEFAULT false,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(payload_json) = 'object'),
    UNIQUE (signal_id, horizon_seconds, policy_version, cost_model_version)
);

CREATE INDEX IF NOT EXISTS core_directional_signal_outcomes_signal_idx
    ON core_directional_signal_outcomes (signal_id, horizon_seconds);

CREATE INDEX IF NOT EXISTS core_directional_signal_outcomes_verdict_idx
    ON core_directional_signal_outcomes (verdict, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS core_directional_signal_outcomes_inverse_candidate_idx
    ON core_directional_signal_outcomes (signal_type, evaluated_at DESC)
    WHERE inverse_hypothesis_candidate;

CREATE INDEX IF NOT EXISTS core_directional_signal_outcomes_instrument_source_idx
    ON core_directional_signal_outcomes (instrument_id, source_event_at DESC);

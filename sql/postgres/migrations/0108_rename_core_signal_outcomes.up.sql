ALTER TABLE IF EXISTS signal_outcomes
    RENAME TO core_directional_signal_outcomes;

ALTER INDEX IF EXISTS signal_outcomes_signal_idx
    RENAME TO core_directional_signal_outcomes_signal_idx;

ALTER INDEX IF EXISTS signal_outcomes_verdict_idx
    RENAME TO core_directional_signal_outcomes_verdict_idx;

ALTER INDEX IF EXISTS signal_outcomes_inverse_candidate_idx
    RENAME TO core_directional_signal_outcomes_inverse_candidate_idx;

ALTER INDEX IF EXISTS signal_outcomes_instrument_source_idx
    RENAME TO core_directional_signal_outcomes_instrument_source_idx;

ALTER TABLE signal_engine.scientific_hypothesis_observations
    ADD COLUMN IF NOT EXISTS record_schema_version LowCardinality(String)
    DEFAULT 'prospective-scientific-v1';

ALTER TABLE signal_engine.scientific_hypothesis_outcomes
    ADD COLUMN IF NOT EXISTS record_schema_version LowCardinality(String)
    DEFAULT 'prospective-scientific-v1';

ALTER TABLE signal_engine.scientific_hypothesis_outcomes
    ADD COLUMN IF NOT EXISTS measurements_json String DEFAULT '{}';

ALTER TABLE signal_engine.scientific_hypothesis_outcomes
    ADD COLUMN IF NOT EXISTS evidence_fingerprint String DEFAULT '';

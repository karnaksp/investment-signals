CREATE TABLE detector_config_acknowledgements (
    detector_instance_id TEXT NOT NULL,
    detector_config_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('loaded', 'failed')),
    failure_reason_code TEXT,
    configured_instruments_count INTEGER NOT NULL DEFAULT 0
        CHECK (configured_instruments_count >= 0),
    loaded_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (detector_instance_id, detector_config_version, loaded_at),
    CHECK (
        (status = 'loaded' AND failure_reason_code IS NULL)
        OR (status = 'failed' AND failure_reason_code IS NOT NULL)
    )
);

CREATE INDEX detector_config_acknowledgements_latest_idx
ON detector_config_acknowledgements (loaded_at DESC, detector_config_version);

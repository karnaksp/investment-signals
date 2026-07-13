"""Framework-independent detector evaluation observations."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid5


_OBSERVATION_NAMESPACE = UUID("be07e46a-0cbd-5a48-9868-ad4244845b37")
HISTORY_SAMPLING_POLICY_VERSION = "history-evaluation-v1"


@dataclass(frozen=True)
class DetectorObservation:
    """One threshold evaluation, including evaluations that did not alert."""

    observation_id: str
    source_event_id: str
    observed_at: datetime
    instrument_id: str
    source_event_type: str
    signal_type: str
    metric_value: float
    baseline_value: float
    z_score: float
    threshold_value: float
    threshold_passed: bool
    detector_passed: bool
    signal_emitted: bool
    window_seconds: int
    sampling_policy_version: str
    detector_config_version: str
    expectation_catalog_version: str | None
    provenance_status: str

    def __post_init__(self) -> None:
        required = (
            self.observation_id,
            self.source_event_id,
            self.instrument_id,
            self.source_event_type,
            self.signal_type,
            self.sampling_policy_version,
            self.detector_config_version,
            self.provenance_status,
        )
        if any(not value for value in required):
            raise ValueError("detector observation identifiers must not be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        values = (
            self.metric_value,
            self.baseline_value,
            self.z_score,
            self.threshold_value,
        )
        if any(not math.isfinite(value) for value in values):
            raise ValueError("detector observation values must be finite")
        if self.window_seconds < 0:
            raise ValueError("window_seconds must be non-negative")
        if self.detector_passed and not self.threshold_passed:
            raise ValueError("detector_passed requires threshold_passed")
        if self.signal_emitted and not self.detector_passed:
            raise ValueError("signal_emitted requires detector_passed")


def deterministic_observation_id(
    *,
    source_event_id: str,
    signal_type: str,
    detector_config_version: str,
    sampling_policy_version: str = HISTORY_SAMPLING_POLICY_VERSION,
) -> str:
    """Return the stable identity for one detector-family event evaluation."""

    parts = (
        source_event_id,
        signal_type,
        detector_config_version,
        sampling_policy_version,
    )
    if any(not part for part in parts):
        raise ValueError("observation identity components must not be empty")
    return str(uuid5(_OBSERVATION_NAMESPACE, "\x1f".join(parts)))

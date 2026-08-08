"""Immutable inputs for daily H10 live-shadow persistence."""

from __future__ import annotations

from dataclasses import dataclass

from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Feature,
    R2Outcome,
)


@dataclass(frozen=True, slots=True)
class R2LiveShadowInput:
    instrument_id: str
    feature: R2Feature
    outcome: R2Outcome
    dataset_fingerprint: str
    source_event_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("R2 live-shadow instrument_id is required")
        if self.feature.observation_id != self.outcome.observation_id:
            raise ValueError("R2 live-shadow feature and outcome must align")
        if not self.dataset_fingerprint.startswith("sha256:"):
            raise ValueError("R2 live-shadow dataset fingerprint must use sha256")
        if not self.source_event_ids or any(
            not item.strip() for item in self.source_event_ids
        ):
            raise ValueError("R2 live-shadow source event ids are required")
        if len(self.source_event_ids) != len(set(self.source_event_ids)):
            raise ValueError("R2 live-shadow source event ids must be unique")

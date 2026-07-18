"""Boundary mapper from research records to typed hypothesis features."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping

from tinvest_signal_engine.domain.hypothesis_formulas import (
    FeatureName,
    HypothesisFeatureSet,
    ObservedFeature,
)


class ResearchHypothesisFeatureAdapter:
    @staticmethod
    def from_records(records: tuple[Mapping[str, object], ...]) -> HypothesisFeatureSet:
        return HypothesisFeatureSet.from_iterable(
            ResearchHypothesisFeatureAdapter._feature(record) for record in records
        )

    @staticmethod
    def _feature(record: Mapping[str, object]) -> ObservedFeature:
        try:
            return ObservedFeature(
                name=FeatureName(str(record["name"])),
                value=float(record["value"]),
                observed_at=_timestamp(record["observed_at"]),
                window_start=_timestamp(record["window_start"]),
                window_end=_timestamp(record["window_end"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid research feature record: {exc}") from exc


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("feature timestamp must be RFC3339 text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("feature timestamp must be timezone-aware")
    return parsed

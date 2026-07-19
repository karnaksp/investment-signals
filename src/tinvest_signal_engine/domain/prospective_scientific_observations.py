"""Immutable contracts for prospective scientific observations and outcomes.

The records in this module seal what was known at an observation cutoff and
what became known only after its outcome horizon.  They intentionally contain
no storage, broker, or framework concerns.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from hashlib import sha256
import json
import math
from typing import Mapping

from tinvest_signal_engine.domain.scientific_candle_models import (
    AbstentionReason,
    CausalFeatureVector,
    ScientificCandleHypothesis,
    ScientificModelOutcome,
    ScientificTarget,
)


class ProspectiveSourceKind(str, Enum):
    STREAM = "stream"
    HISTORICAL_BACKFILL = "historical_backfill"


class PersistenceDisposition(str, Enum):
    INSERTED = "inserted"
    REPLAYED = "replayed"


class ProspectiveEvidenceConflict(RuntimeError):
    """Raised when a deterministic identity is reused with different content."""


@dataclass(frozen=True, slots=True)
class ProspectiveObservationProvenance:
    """Causal lineage for the data sealed into one feature vector."""

    source_kind: ProspectiveSourceKind
    source_event_ids: tuple[str, ...]
    source_window_start: datetime
    source_window_end: datetime
    source_max_observed_at: datetime
    input_fingerprint: str
    dataset_fingerprint: str
    scientific_source_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_aware(self.source_window_start, "source_window_start")
        _require_aware(self.source_window_end, "source_window_end")
        _require_aware(self.source_max_observed_at, "source_max_observed_at")
        if self.source_window_start > self.source_window_end:
            raise ValueError("source window start must not be after its end")
        if self.source_max_observed_at < self.source_window_end:
            raise ValueError("source_max_observed_at must cover the source window")
        if not self.source_event_ids or any(
            not item.strip() for item in self.source_event_ids
        ):
            raise ValueError("source_event_ids must contain non-empty identities")
        if len(set(self.source_event_ids)) != len(self.source_event_ids):
            raise ValueError("source_event_ids must be unique")
        if not self.scientific_source_ids or any(
            not item.strip() for item in self.scientific_source_ids
        ):
            raise ValueError("scientific_source_ids must contain non-empty identities")
        if len(set(self.scientific_source_ids)) != len(self.scientific_source_ids):
            raise ValueError("scientific_source_ids must be unique")
        _require_fingerprint(self.input_fingerprint, "input_fingerprint")
        _require_fingerprint(self.dataset_fingerprint, "dataset_fingerprint")


@dataclass(frozen=True, slots=True)
class ProspectiveScientificObservation:
    """One immutable feature evaluation, including non-events and abstentions."""

    observation_id: str
    instrument_id: str
    feature: CausalFeatureVector
    policy_version: str
    formula_version: str
    provenance: ProspectiveObservationProvenance
    recorded_at: datetime
    payload_fingerprint: str

    def __post_init__(self) -> None:
        if self.observation_id != deterministic_prospective_observation_id(
            hypothesis=self.feature.hypothesis,
            hypothesis_version=self.feature.hypothesis_version,
            instrument_id=self.instrument_id,
            observed_at=self.feature.observed_at,
            policy_version=self.policy_version,
            formula_version=self.formula_version,
        ):
            raise ValueError("observation_id does not match its deterministic identity")
        if not self.instrument_id.strip():
            raise ValueError("instrument_id must not be empty")
        if not self.policy_version.strip() or not self.formula_version.strip():
            raise ValueError("policy and formula versions must not be empty")
        _require_aware(self.recorded_at, "recorded_at")
        if self.recorded_at < self.feature.observed_at:
            raise ValueError("recorded_at must not precede observed_at")
        if self.feature.feature_max_observed_at > self.feature.observed_at:
            raise ValueError("feature_max_observed_at must not exceed observed_at")
        if self.provenance.source_max_observed_at > self.feature.observed_at:
            raise ValueError("observation provenance uses future source data")
        if self.provenance.source_window_end > self.feature.observed_at:
            raise ValueError("observation source window extends past observed_at")
        _require_fingerprint(self.payload_fingerprint, "payload_fingerprint")
        expected = prospective_observation_payload_fingerprint(
            instrument_id=self.instrument_id,
            feature=self.feature,
            policy_version=self.policy_version,
            formula_version=self.formula_version,
            provenance=self.provenance,
        )
        if self.payload_fingerprint != expected:
            raise ValueError("observation payload_fingerprint does not match content")

    @property
    def target_at(self) -> datetime:
        return self.feature.observed_at + timedelta(
            seconds=self.feature.horizon_seconds
        )

    def is_mature(self, now: datetime, *, grace_seconds: int) -> bool:
        _require_aware(now, "now")
        if grace_seconds < 0:
            raise ValueError("grace_seconds must not be negative")
        return now >= self.target_at + timedelta(seconds=grace_seconds)


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeEvidence:
    """Post-event market evidence returned by an outer-layer data source."""

    observation_id: str
    target_at: datetime
    available: bool
    reason: AbstentionReason
    actual_value: float | None
    source_event_ids: tuple[str, ...]
    source_window_start: datetime
    source_window_end: datetime
    source_max_observed_at: datetime
    input_fingerprint: str

    def __post_init__(self) -> None:
        if not self.observation_id.startswith("sha256:"):
            raise ValueError("observation_id must use sha256")
        for name, value in (
            ("target_at", self.target_at),
            ("source_window_start", self.source_window_start),
            ("source_window_end", self.source_window_end),
            ("source_max_observed_at", self.source_max_observed_at),
        ):
            _require_aware(value, name)
        if self.source_window_start > self.source_window_end:
            raise ValueError("outcome source window is inverted")
        if self.source_max_observed_at < self.source_window_end:
            raise ValueError("source_max_observed_at must cover outcome source window")
        if self.source_max_observed_at > self.target_at:
            raise ValueError("outcome evidence uses data after target_at")
        if self.available:
            if self.actual_value is None or not math.isfinite(self.actual_value):
                raise ValueError("available outcome evidence requires a finite value")
            if not self.source_event_ids:
                raise ValueError("available outcome evidence requires source events")
        elif self.actual_value is not None:
            raise ValueError("unavailable outcome evidence must not contain a value")
        if any(not item.strip() for item in self.source_event_ids):
            raise ValueError("outcome source_event_ids must not contain empty values")
        if len(set(self.source_event_ids)) != len(self.source_event_ids):
            raise ValueError("outcome source_event_ids must be unique")
        _require_fingerprint(self.input_fingerprint, "input_fingerprint")


@dataclass(frozen=True, slots=True)
class ProspectiveScientificOutcome:
    """A sealed, versioned and idempotent result for one mature observation."""

    outcome_id: str
    observation_id: str
    hypothesis: ScientificCandleHypothesis
    target: ScientificTarget
    target_at: datetime
    result: ScientificModelOutcome
    outcome_policy_version: str
    source_event_ids: tuple[str, ...]
    source_window_start: datetime
    source_window_end: datetime
    source_max_observed_at: datetime
    input_fingerprint: str
    evaluated_at: datetime
    payload_fingerprint: str

    def __post_init__(self) -> None:
        if self.outcome_id != deterministic_prospective_outcome_id(
            observation_id=self.observation_id,
            outcome_policy_version=self.outcome_policy_version,
        ):
            raise ValueError("outcome_id does not match its deterministic identity")
        if self.result.observation_id != self.observation_id:
            raise ValueError("outcome result belongs to a different observation")
        if self.result.target_at != self.target_at:
            raise ValueError("outcome result target_at differs from sealed target")
        if not self.outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        for name, value in (
            ("target_at", self.target_at),
            ("source_window_start", self.source_window_start),
            ("source_window_end", self.source_window_end),
            ("source_max_observed_at", self.source_max_observed_at),
            ("evaluated_at", self.evaluated_at),
        ):
            _require_aware(value, name)
        if self.evaluated_at < self.target_at:
            raise ValueError("outcome must not be evaluated before target_at")
        if self.source_window_start > self.source_window_end:
            raise ValueError("outcome source window is inverted")
        if self.source_max_observed_at > self.target_at:
            raise ValueError("outcome uses source data after target_at")
        if self.result.available and not self.source_event_ids:
            raise ValueError("available outcome requires source events")
        _require_fingerprint(self.input_fingerprint, "input_fingerprint")
        _require_fingerprint(self.payload_fingerprint, "payload_fingerprint")
        expected = prospective_outcome_payload_fingerprint(
            observation_id=self.observation_id,
            hypothesis=self.hypothesis,
            target=self.target,
            result=self.result,
            outcome_policy_version=self.outcome_policy_version,
            source_event_ids=self.source_event_ids,
            source_window_start=self.source_window_start,
            source_window_end=self.source_window_end,
            source_max_observed_at=self.source_max_observed_at,
            input_fingerprint=self.input_fingerprint,
        )
        if self.payload_fingerprint != expected:
            raise ValueError("outcome payload_fingerprint does not match content")


def deterministic_prospective_observation_id(
    *,
    hypothesis: ScientificCandleHypothesis,
    hypothesis_version: str,
    instrument_id: str,
    observed_at: datetime,
    policy_version: str,
    formula_version: str,
) -> str:
    _require_aware(observed_at, "observed_at")
    components = (
        hypothesis.value,
        hypothesis_version,
        instrument_id,
        observed_at.isoformat(),
        policy_version,
        formula_version,
    )
    if any(not item.strip() for item in components):
        raise ValueError("observation identity components must not be empty")
    return "sha256:" + sha256("\x1f".join(components).encode("utf-8")).hexdigest()


def deterministic_prospective_outcome_id(
    *, observation_id: str, outcome_policy_version: str
) -> str:
    if not observation_id.startswith("sha256:"):
        raise ValueError("observation_id must use sha256")
    if not outcome_policy_version.strip():
        raise ValueError("outcome_policy_version must not be empty")
    value = f"{observation_id}\x1f{outcome_policy_version}"
    return "sha256:" + sha256(value.encode("utf-8")).hexdigest()


def prospective_observation_payload_fingerprint(
    *,
    instrument_id: str,
    feature: CausalFeatureVector,
    policy_version: str,
    formula_version: str,
    provenance: ProspectiveObservationProvenance,
) -> str:
    return _fingerprint(
        {
            "feature": _feature_payload(feature),
            "formula_version": formula_version,
            "instrument_id": instrument_id,
            "policy_version": policy_version,
            "provenance": _provenance_payload(provenance),
        }
    )


def prospective_outcome_payload_fingerprint(
    *,
    observation_id: str,
    hypothesis: ScientificCandleHypothesis,
    target: ScientificTarget,
    result: ScientificModelOutcome,
    outcome_policy_version: str,
    source_event_ids: tuple[str, ...],
    source_window_start: datetime,
    source_window_end: datetime,
    source_max_observed_at: datetime,
    input_fingerprint: str,
) -> str:
    return _fingerprint(
        {
            "hypothesis": hypothesis.value,
            "input_fingerprint": input_fingerprint,
            "observation_id": observation_id,
            "outcome_policy_version": outcome_policy_version,
            "result": _outcome_result_payload(result),
            "source_event_ids": source_event_ids,
            "source_max_observed_at": source_max_observed_at.isoformat(),
            "source_window_end": source_window_end.isoformat(),
            "source_window_start": source_window_start.isoformat(),
            "target": target.value,
        }
    )


def build_prospective_observation(
    *,
    instrument_id: str,
    feature: CausalFeatureVector,
    policy_version: str,
    formula_version: str,
    provenance: ProspectiveObservationProvenance,
    recorded_at: datetime,
) -> ProspectiveScientificObservation:
    observation_id = deterministic_prospective_observation_id(
        hypothesis=feature.hypothesis,
        hypothesis_version=feature.hypothesis_version,
        instrument_id=instrument_id,
        observed_at=feature.observed_at,
        policy_version=policy_version,
        formula_version=formula_version,
    )
    return ProspectiveScientificObservation(
        observation_id=observation_id,
        instrument_id=instrument_id,
        feature=feature,
        policy_version=policy_version,
        formula_version=formula_version,
        provenance=provenance,
        recorded_at=recorded_at,
        payload_fingerprint=prospective_observation_payload_fingerprint(
            instrument_id=instrument_id,
            feature=feature,
            policy_version=policy_version,
            formula_version=formula_version,
            provenance=provenance,
        ),
    )


def _feature_payload(feature: CausalFeatureVector) -> Mapping[str, object]:
    return {
        "decision": feature.decision.value,
        "expected_direction": feature.expected_direction,
        "feature_max_observed_at": feature.feature_max_observed_at.isoformat(),
        "feature_values": feature.feature_values,
        "forecast_value": feature.forecast_value,
        "horizon_seconds": feature.horizon_seconds,
        "hypothesis": feature.hypothesis.value,
        "hypothesis_version": feature.hypothesis_version,
        "model_trained_until": (
            feature.model_trained_until.isoformat()
            if feature.model_trained_until is not None
            else None
        ),
        "observation_id": feature.observation_id,
        "observed_at": feature.observed_at.isoformat(),
        "reason": feature.reason.value,
        "target": feature.target.value,
        "ticker": feature.ticker,
        "trading_day": feature.trading_day.isoformat(),
    }


def _provenance_payload(
    provenance: ProspectiveObservationProvenance,
) -> Mapping[str, object]:
    return {
        "dataset_fingerprint": provenance.dataset_fingerprint,
        "input_fingerprint": provenance.input_fingerprint,
        "scientific_source_ids": provenance.scientific_source_ids,
        "source_event_ids": provenance.source_event_ids,
        "source_kind": provenance.source_kind.value,
        "source_max_observed_at": provenance.source_max_observed_at.isoformat(),
        "source_window_end": provenance.source_window_end.isoformat(),
        "source_window_start": provenance.source_window_start.isoformat(),
    }


def _outcome_result_payload(result: ScientificModelOutcome) -> Mapping[str, object]:
    return {
        "actual_value": result.actual_value,
        "available": result.available,
        "benchmark_loss": result.benchmark_loss,
        "cost_adjusted_value": result.cost_adjusted_value,
        "model_loss": result.model_loss,
        "observation_id": result.observation_id,
        "reason": result.reason.value,
        "supported": result.supported,
        "target_at": result.target_at.isoformat(),
    }


def _fingerprint(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _require_fingerprint(value: str, name: str) -> None:
    if not value.startswith("sha256:") or len(value) != 71:
        raise ValueError(f"{name} must be a sha256 fingerprint")
    try:
        int(value.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise ValueError(f"{name} must be a sha256 fingerprint") from exc


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")

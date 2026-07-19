"""Versioned records and aggregates for prospective live-shadow evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
import json
from typing import Iterable, Mapping

from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    TargetMetric,
)


LIVE_SHADOW_RECORD_VERSION = "prospective-live-shadow-v1"


@dataclass(frozen=True, slots=True)
class ProspectiveLiveObservation:
    observation_id: str
    record_version: str
    instrument_id: str
    policy_version: str
    feature: ProspectiveFeature
    source_event_ids: tuple[str, ...]
    dataset_fingerprint: str
    input_fingerprint: str
    recorded_at: datetime
    payload_fingerprint: str

    def __post_init__(self) -> None:
        if self.record_version != LIVE_SHADOW_RECORD_VERSION:
            raise ValueError("unsupported prospective live observation version")
        if self.observation_id != deterministic_live_observation_id(
            instrument_id=self.instrument_id,
            feature=self.feature,
            policy_version=self.policy_version,
        ):
            raise ValueError("live observation identity does not match content")
        if not self.instrument_id.strip() or not self.policy_version.strip():
            raise ValueError("instrument and policy versions are required")
        if not self.source_event_ids or any(
            not item.strip() for item in self.source_event_ids
        ):
            raise ValueError("source_event_ids must contain non-empty identities")
        if len(self.source_event_ids) != len(set(self.source_event_ids)):
            raise ValueError("source_event_ids must be unique")
        _require_fingerprint(self.dataset_fingerprint, "dataset_fingerprint")
        _require_fingerprint(self.input_fingerprint, "input_fingerprint")
        _require_aware(self.recorded_at, "recorded_at")
        if self.recorded_at < self.feature.observed_at:
            raise ValueError("recorded_at must not precede the observation")
        expected = live_observation_payload_fingerprint(
            instrument_id=self.instrument_id,
            policy_version=self.policy_version,
            feature=self.feature,
            source_event_ids=self.source_event_ids,
            dataset_fingerprint=self.dataset_fingerprint,
            input_fingerprint=self.input_fingerprint,
        )
        if self.payload_fingerprint != expected:
            raise ValueError("live observation fingerprint does not match content")

    @property
    def target_at(self) -> datetime:
        return self.feature.observed_at + timedelta(
            seconds=self.feature.horizon_seconds
        )


@dataclass(frozen=True, slots=True)
class ProspectiveLiveOutcome:
    outcome_id: str
    record_version: str
    observation_id: str
    outcome_policy_version: str
    outcome: ProspectiveOutcome
    evidence_fingerprint: str
    evaluated_at: datetime
    payload_fingerprint: str

    def __post_init__(self) -> None:
        if self.record_version != LIVE_SHADOW_RECORD_VERSION:
            raise ValueError("unsupported prospective live outcome version")
        if self.outcome_id != deterministic_live_outcome_id(
            observation_id=self.observation_id,
            outcome_policy_version=self.outcome_policy_version,
        ):
            raise ValueError("live outcome identity does not match content")
        if not self.outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        _require_fingerprint(self.evidence_fingerprint, "evidence_fingerprint")
        _require_aware(self.evaluated_at, "evaluated_at")
        if self.evaluated_at < self.outcome.target_at:
            raise ValueError("live outcome must not be evaluated before target_at")
        expected = live_outcome_payload_fingerprint(
            observation_id=self.observation_id,
            outcome_policy_version=self.outcome_policy_version,
            outcome=self.outcome,
            evidence_fingerprint=self.evidence_fingerprint,
        )
        if self.payload_fingerprint != expected:
            raise ValueError("live outcome fingerprint does not match content")


@dataclass(frozen=True, slots=True)
class LiveShadowReasonCount:
    reason_code: str
    count: int

    def __post_init__(self) -> None:
        if not self.reason_code.strip() or self.count <= 0:
            raise ValueError("live-shadow reason counts must be named and positive")


@dataclass(frozen=True, slots=True)
class LiveShadowHypothesisStatistics:
    hypothesis: ProspectiveHypothesis
    hypothesis_version: str
    horizon_seconds: int
    observation_count: int
    matched_count: int
    not_matched_count: int
    abstained_count: int
    mature_outcome_count: int
    available_outcome_count: int
    matched_outcome_count: int
    positive_effect_count: int
    mean_effect: float | None
    data_coverage: float | None
    reasons_histogram: tuple[LiveShadowReasonCount, ...]
    descriptive_only: bool = True

    def __post_init__(self) -> None:
        if self.hypothesis_version != self.hypothesis.version:
            raise ValueError("live-shadow hypothesis version does not match hypothesis")
        counts = (
            self.observation_count,
            self.matched_count,
            self.not_matched_count,
            self.abstained_count,
            self.mature_outcome_count,
            self.available_outcome_count,
            self.matched_outcome_count,
            self.positive_effect_count,
        )
        if self.horizon_seconds <= 0 or any(value < 0 for value in counts):
            raise ValueError("live-shadow horizons and counts must be non-negative")
        if (
            self.matched_count + self.not_matched_count + self.abstained_count
            != self.observation_count
        ):
            raise ValueError("live-shadow decisions must cover every observation")
        if self.available_outcome_count > self.mature_outcome_count:
            raise ValueError("available outcomes cannot exceed mature outcomes")
        if self.mature_outcome_count > self.observation_count:
            raise ValueError("mature outcomes cannot exceed observations")
        if self.matched_outcome_count > self.matched_count:
            raise ValueError("matched outcomes cannot exceed matched observations")
        if self.positive_effect_count > self.matched_outcome_count:
            raise ValueError("positive effects cannot exceed matched outcomes")
        if self.data_coverage is not None and not 0.0 <= self.data_coverage <= 1.0:
            raise ValueError("live-shadow data coverage must be in [0, 1]")
        if not self.descriptive_only:
            raise ValueError("live-shadow statistics must remain descriptive-only")
        reason_codes = tuple(item.reason_code for item in self.reasons_histogram)
        if reason_codes != tuple(sorted(reason_codes)):
            raise ValueError("live-shadow reasons must be sorted")


@dataclass(frozen=True, slots=True)
class ProspectiveLiveShadowStatistics:
    version: str
    generated_at: datetime
    rows: tuple[LiveShadowHypothesisStatistics, ...]
    descriptive_only: bool = True

    def __post_init__(self) -> None:
        if self.version != LIVE_SHADOW_RECORD_VERSION:
            raise ValueError("unsupported prospective live statistics version")
        _require_aware(self.generated_at, "generated_at")
        if not self.descriptive_only:
            raise ValueError("prospective live statistics must remain descriptive-only")
        identities = tuple(
            (item.hypothesis.value, item.horizon_seconds) for item in self.rows
        )
        if identities != tuple(sorted(identities)) or len(identities) != len(
            set(identities)
        ):
            raise ValueError("live-shadow statistic rows must be unique and sorted")


def build_live_observation(
    *,
    instrument_id: str,
    policy_version: str,
    feature: ProspectiveFeature,
    source_event_ids: tuple[str, ...],
    dataset_fingerprint: str,
    input_fingerprint: str,
    recorded_at: datetime,
) -> ProspectiveLiveObservation:
    observation_id = deterministic_live_observation_id(
        instrument_id=instrument_id,
        feature=feature,
        policy_version=policy_version,
    )
    payload_fingerprint = live_observation_payload_fingerprint(
        instrument_id=instrument_id,
        policy_version=policy_version,
        feature=feature,
        source_event_ids=source_event_ids,
        dataset_fingerprint=dataset_fingerprint,
        input_fingerprint=input_fingerprint,
    )
    return ProspectiveLiveObservation(
        observation_id=observation_id,
        record_version=LIVE_SHADOW_RECORD_VERSION,
        instrument_id=instrument_id,
        policy_version=policy_version,
        feature=feature,
        source_event_ids=source_event_ids,
        dataset_fingerprint=dataset_fingerprint,
        input_fingerprint=input_fingerprint,
        recorded_at=recorded_at,
        payload_fingerprint=payload_fingerprint,
    )


def build_live_outcome(
    *,
    observation: ProspectiveLiveObservation,
    outcome: ProspectiveOutcome,
    outcome_policy_version: str,
    evidence_fingerprint: str,
    evaluated_at: datetime,
) -> ProspectiveLiveOutcome:
    if outcome.observation_id != observation.feature.observation_id:
        raise ValueError("outcome belongs to a different feature")
    if outcome.target_at != observation.target_at:
        raise ValueError("outcome target differs from sealed observation target")
    outcome_id = deterministic_live_outcome_id(
        observation_id=observation.observation_id,
        outcome_policy_version=outcome_policy_version,
    )
    fingerprint = live_outcome_payload_fingerprint(
        observation_id=observation.observation_id,
        outcome_policy_version=outcome_policy_version,
        outcome=outcome,
        evidence_fingerprint=evidence_fingerprint,
    )
    return ProspectiveLiveOutcome(
        outcome_id=outcome_id,
        record_version=LIVE_SHADOW_RECORD_VERSION,
        observation_id=observation.observation_id,
        outcome_policy_version=outcome_policy_version,
        outcome=outcome,
        evidence_fingerprint=evidence_fingerprint,
        evaluated_at=evaluated_at,
        payload_fingerprint=fingerprint,
    )


def aggregate_live_shadow_statistics(
    observations: Iterable[ProspectiveLiveObservation],
    outcomes: Iterable[ProspectiveLiveOutcome],
    *,
    generated_at: datetime,
) -> ProspectiveLiveShadowStatistics:
    observation_rows = tuple(observations)
    outcome_by_observation = {item.observation_id: item for item in outcomes}
    grouped: defaultdict[
        tuple[ProspectiveHypothesis, int], list[ProspectiveLiveObservation]
    ] = defaultdict(list)
    for item in observation_rows:
        grouped[(item.feature.hypothesis, item.feature.horizon_seconds)].append(item)
    rows: list[LiveShadowHypothesisStatistics] = []
    for (hypothesis, horizon), group in sorted(
        grouped.items(), key=lambda item: (item[0][0].value, item[0][1])
    ):
        decisions = Counter(item.feature.decision for item in group)
        reasons = Counter(item.feature.reason.value for item in group)
        mature = tuple(
            outcome_by_observation[item.observation_id]
            for item in group
            if item.observation_id in outcome_by_observation
        )
        available = tuple(item for item in mature if item.outcome.available)
        reasons.update(
            item.outcome.reason.value for item in mature if not item.outcome.available
        )
        effects = tuple(
            effect
            for item in group
            if item.feature.decision is ProspectiveDecision.MATCHED
            and (stored := outcome_by_observation.get(item.observation_id)) is not None
            and (effect := _descriptive_effect(item.feature, stored.outcome))
            is not None
        )
        rows.append(
            LiveShadowHypothesisStatistics(
                hypothesis=hypothesis,
                hypothesis_version=hypothesis.version,
                horizon_seconds=horizon,
                observation_count=len(group),
                matched_count=decisions[ProspectiveDecision.MATCHED],
                not_matched_count=decisions[ProspectiveDecision.NOT_MATCHED],
                abstained_count=decisions[ProspectiveDecision.ABSTAIN],
                mature_outcome_count=len(mature),
                available_outcome_count=len(available),
                matched_outcome_count=len(effects),
                positive_effect_count=sum(value > 0.0 for value in effects),
                mean_effect=(sum(effects) / len(effects) if effects else None),
                data_coverage=(len(available) / len(mature) if mature else None),
                reasons_histogram=tuple(
                    LiveShadowReasonCount(reason_code=reason, count=count)
                    for reason, count in sorted(reasons.items())
                ),
            )
        )
    return ProspectiveLiveShadowStatistics(
        version=LIVE_SHADOW_RECORD_VERSION,
        generated_at=generated_at,
        rows=tuple(rows),
    )


def deterministic_live_observation_id(
    *,
    instrument_id: str,
    feature: ProspectiveFeature,
    policy_version: str,
) -> str:
    return _fingerprint(
        {
            "feature_observation_id": feature.observation_id,
            "instrument_id": instrument_id,
            "policy_version": policy_version,
            "record_version": LIVE_SHADOW_RECORD_VERSION,
        }
    )


def deterministic_live_outcome_id(
    *, observation_id: str, outcome_policy_version: str
) -> str:
    return _fingerprint(
        {
            "observation_id": observation_id,
            "outcome_policy_version": outcome_policy_version,
            "record_version": LIVE_SHADOW_RECORD_VERSION,
        }
    )


def live_observation_payload_fingerprint(
    *,
    instrument_id: str,
    policy_version: str,
    feature: ProspectiveFeature,
    source_event_ids: tuple[str, ...],
    dataset_fingerprint: str,
    input_fingerprint: str,
) -> str:
    return _fingerprint(
        {
            "dataset_fingerprint": dataset_fingerprint,
            "feature": _feature_payload(feature),
            "input_fingerprint": input_fingerprint,
            "instrument_id": instrument_id,
            "policy_version": policy_version,
            "record_version": LIVE_SHADOW_RECORD_VERSION,
            "source_event_ids": source_event_ids,
        }
    )


def live_outcome_payload_fingerprint(
    *,
    observation_id: str,
    outcome_policy_version: str,
    outcome: ProspectiveOutcome,
    evidence_fingerprint: str,
) -> str:
    return _fingerprint(
        {
            "evidence_fingerprint": evidence_fingerprint,
            "observation_id": observation_id,
            "outcome": _outcome_payload(outcome),
            "outcome_policy_version": outcome_policy_version,
            "record_version": LIVE_SHADOW_RECORD_VERSION,
        }
    )


def _descriptive_effect(
    feature: ProspectiveFeature,
    outcome: ProspectiveOutcome,
) -> float | None:
    if not outcome.available:
        return None
    if feature.target is TargetMetric.FORWARD_RETURN:
        return _metric(outcome, "cost_adjusted_directional_return")
    if feature.target is TargetMetric.FUTURE_VARIANCE_UPLIFT:
        return _metric(outcome, "future_variance_uplift")
    har = _metric(outcome, "har_qlike")
    ewma = _metric(outcome, "ewma_qlike")
    phase = _metric(outcome, "phase_qlike")
    if har is None or ewma is None or phase is None:
        return None
    return min(ewma, phase) - har


def _metric(outcome: ProspectiveOutcome, name: str) -> float | None:
    try:
        return outcome.metric(name).value
    except KeyError:
        return None


def _feature_payload(feature: ProspectiveFeature) -> Mapping[str, object]:
    return {
        "decision": feature.decision.value,
        "expected_direction": feature.expected_direction,
        "feature_max_observed_at": feature.feature_max_observed_at.isoformat(),
        "feature_values": tuple(
            (item.name, item.unit.value, item.value) for item in feature.feature_values
        ),
        "forecast": (
            (feature.forecast.name, feature.forecast.unit.value, feature.forecast.value)
            if feature.forecast is not None
            else None
        ),
        "history_observed_until": (
            feature.history_observed_until.isoformat()
            if feature.history_observed_until is not None
            else None
        ),
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


def _outcome_payload(outcome: ProspectiveOutcome) -> Mapping[str, object]:
    return {
        "available": outcome.available,
        "measurements": tuple(
            (item.name, item.unit.value, item.value) for item in outcome.measurements
        ),
        "observation_id": outcome.observation_id,
        "reason": outcome.reason.value,
        "target": outcome.target.value,
        "target_at": outcome.target_at.isoformat(),
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

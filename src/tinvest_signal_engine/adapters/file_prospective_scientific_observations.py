"""Durable local-file adapters for prospective scientific evidence.

These adapters provide a production-usable single-host boundary while the root
integrator owns the eventual PostgreSQL/ClickHouse migration contract.
"""

from __future__ import annotations

from datetime import date, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping

from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveEvidenceConflict,
    ProspectiveObservationProvenance,
    ProspectiveOutcomeEvidence,
    ProspectiveScientificObservation,
    ProspectiveScientificOutcome,
    ProspectiveSourceKind,
    deterministic_prospective_outcome_id,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    AbstentionReason,
    CausalFeatureVector,
    FeatureDecision,
    ScientificCandleHypothesis,
    ScientificModelOutcome,
    ScientificTarget,
)


_SCHEMA_VERSION = "prospective-scientific-evidence-v1"


class ImmutableFileProspectiveScientificStore:
    """Persist immutable observations/outcomes as deterministic JSON files."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._observations = self._root / "observations"
        self._outcomes = self._root / "outcomes"
        self._observations.mkdir(parents=True, exist_ok=True)
        self._outcomes.mkdir(parents=True, exist_ok=True)

    def persist_observation(
        self, observation: ProspectiveScientificObservation
    ) -> PersistenceDisposition:
        return _persist_immutable(
            self._path(self._observations, observation.observation_id),
            _observation_to_payload(observation),
            payload_fingerprint=observation.payload_fingerprint,
        )

    def pending_observations(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[ProspectiveScientificObservation, ...]:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")
        pending: list[ProspectiveScientificObservation] = []
        for path in sorted(self._observations.glob("*.json")):
            observation = _observation_from_payload(_read_object(path))
            outcome_id = deterministic_prospective_outcome_id(
                observation_id=observation.observation_id,
                outcome_policy_version=outcome_policy_version,
            )
            if not self._path(self._outcomes, outcome_id).exists():
                pending.append(observation)
        pending.sort(key=lambda item: (item.target_at, item.observation_id))
        return tuple(pending[:limit])

    def persist_outcome(
        self, outcome: ProspectiveScientificOutcome
    ) -> PersistenceDisposition:
        return _persist_immutable(
            self._path(self._outcomes, outcome.outcome_id),
            _outcome_to_payload(outcome),
            payload_fingerprint=outcome.payload_fingerprint,
        )

    def load_observation(self, observation_id: str) -> ProspectiveScientificObservation:
        return _observation_from_payload(
            _read_object(self._path(self._observations, observation_id))
        )

    def load_outcome(self, outcome_id: str) -> ProspectiveScientificOutcome:
        return _outcome_from_payload(
            _read_object(self._path(self._outcomes, outcome_id))
        )

    @staticmethod
    def _path(directory: Path, identity: str) -> Path:
        digest = identity.removeprefix("sha256:")
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("file evidence identity must use lowercase sha256")
        return directory / f"{digest}.json"


class ImmutableFileProspectiveOutcomeEvidenceSource:
    """Read one externally sealed post-event evidence record per observation."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def load(
        self, observation: ProspectiveScientificObservation
    ) -> ProspectiveOutcomeEvidence:
        digest = observation.observation_id.removeprefix("sha256:")
        path = self._root / f"{digest}.json"
        if not path.exists():
            missing = (
                "sha256:"
                + sha256(
                    (
                        f"{observation.observation_id}\x1f"
                        f"{observation.target_at.isoformat()}\x1fmissing"
                    ).encode("utf-8")
                ).hexdigest()
            )
            return ProspectiveOutcomeEvidence(
                observation_id=observation.observation_id,
                target_at=observation.target_at,
                available=False,
                reason=AbstentionReason.OUTCOME_UNAVAILABLE,
                actual_value=None,
                source_event_ids=(),
                source_window_start=observation.feature.observed_at,
                source_window_end=observation.target_at,
                source_max_observed_at=observation.target_at,
                input_fingerprint=missing,
            )
        payload = _read_object(path)
        if payload.get("schema_version") != _SCHEMA_VERSION:
            raise ValueError("unsupported prospective evidence schema")
        return ProspectiveOutcomeEvidence(
            observation_id=str(payload["observation_id"]),
            target_at=_datetime(payload["target_at"]),
            available=bool(payload["available"]),
            reason=AbstentionReason(str(payload["reason"])),
            actual_value=_optional_float(payload.get("actual_value")),
            source_event_ids=tuple(str(item) for item in payload["source_event_ids"]),
            source_window_start=_datetime(payload["source_window_start"]),
            source_window_end=_datetime(payload["source_window_end"]),
            source_max_observed_at=_datetime(payload["source_max_observed_at"]),
            input_fingerprint=str(payload["input_fingerprint"]),
        )


def _persist_immutable(
    path: Path,
    payload: Mapping[str, object],
    *,
    payload_fingerprint: str,
) -> PersistenceDisposition:
    canonical = _canonical(payload)
    if path.exists():
        existing = _read_object(path)
        if existing.get("payload_fingerprint") != payload_fingerprint:
            raise ProspectiveEvidenceConflict(
                f"immutable evidence identity was reused: {path.stem}"
            )
        return PersistenceDisposition.REPLAYED
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        mode="wb",
        dir=path.parent,
        prefix=f".{path.stem}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(canonical)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = _read_object(path)
            if existing.get("payload_fingerprint") != payload_fingerprint:
                raise ProspectiveEvidenceConflict(
                    f"immutable evidence identity was reused: {path.stem}"
                )
            return PersistenceDisposition.REPLAYED
    finally:
        temporary.unlink(missing_ok=True)
    return PersistenceDisposition.INSERTED


def _observation_to_payload(
    observation: ProspectiveScientificObservation,
) -> Mapping[str, object]:
    feature = observation.feature
    provenance = observation.provenance
    return {
        "schema_version": _SCHEMA_VERSION,
        "observation_id": observation.observation_id,
        "instrument_id": observation.instrument_id,
        "policy_version": observation.policy_version,
        "formula_version": observation.formula_version,
        "recorded_at": observation.recorded_at.isoformat(),
        "payload_fingerprint": observation.payload_fingerprint,
        "feature": {
            "observation_id": feature.observation_id,
            "hypothesis": feature.hypothesis.value,
            "hypothesis_version": feature.hypothesis_version,
            "ticker": feature.ticker,
            "trading_day": feature.trading_day.isoformat(),
            "observed_at": feature.observed_at.isoformat(),
            "feature_max_observed_at": feature.feature_max_observed_at.isoformat(),
            "model_trained_until": (
                feature.model_trained_until.isoformat()
                if feature.model_trained_until is not None
                else None
            ),
            "horizon_seconds": feature.horizon_seconds,
            "target": feature.target.value,
            "decision": feature.decision.value,
            "reason": feature.reason.value,
            "expected_direction": feature.expected_direction,
            "forecast_value": feature.forecast_value,
            "feature_values": feature.feature_values,
        },
        "provenance": {
            "source_kind": provenance.source_kind.value,
            "source_event_ids": provenance.source_event_ids,
            "source_window_start": provenance.source_window_start.isoformat(),
            "source_window_end": provenance.source_window_end.isoformat(),
            "source_max_observed_at": provenance.source_max_observed_at.isoformat(),
            "input_fingerprint": provenance.input_fingerprint,
            "dataset_fingerprint": provenance.dataset_fingerprint,
            "scientific_source_ids": provenance.scientific_source_ids,
        },
    }


def _observation_from_payload(
    payload: Mapping[str, object],
) -> ProspectiveScientificObservation:
    _require_schema(payload)
    raw_feature = _mapping(payload["feature"])
    raw_provenance = _mapping(payload["provenance"])
    feature = CausalFeatureVector(
        observation_id=str(raw_feature["observation_id"]),
        hypothesis=ScientificCandleHypothesis(str(raw_feature["hypothesis"])),
        hypothesis_version=str(raw_feature["hypothesis_version"]),
        ticker=str(raw_feature["ticker"]),
        trading_day=date.fromisoformat(str(raw_feature["trading_day"])),
        observed_at=_datetime(raw_feature["observed_at"]),
        feature_max_observed_at=_datetime(raw_feature["feature_max_observed_at"]),
        model_trained_until=(
            _datetime(raw_feature["model_trained_until"])
            if raw_feature.get("model_trained_until") is not None
            else None
        ),
        horizon_seconds=int(raw_feature["horizon_seconds"]),
        target=ScientificTarget(str(raw_feature["target"])),
        decision=FeatureDecision(str(raw_feature["decision"])),
        reason=AbstentionReason(str(raw_feature["reason"])),
        expected_direction=int(raw_feature["expected_direction"]),
        forecast_value=_optional_float(raw_feature.get("forecast_value")),
        feature_values=tuple(
            (str(item[0]), float(item[1]))
            for item in _sequence(raw_feature["feature_values"])
        ),
    )
    provenance = ProspectiveObservationProvenance(
        source_kind=ProspectiveSourceKind(str(raw_provenance["source_kind"])),
        source_event_ids=tuple(
            str(item) for item in _sequence(raw_provenance["source_event_ids"])
        ),
        source_window_start=_datetime(raw_provenance["source_window_start"]),
        source_window_end=_datetime(raw_provenance["source_window_end"]),
        source_max_observed_at=_datetime(raw_provenance["source_max_observed_at"]),
        input_fingerprint=str(raw_provenance["input_fingerprint"]),
        dataset_fingerprint=str(raw_provenance["dataset_fingerprint"]),
        scientific_source_ids=tuple(
            str(item) for item in _sequence(raw_provenance["scientific_source_ids"])
        ),
    )
    return ProspectiveScientificObservation(
        observation_id=str(payload["observation_id"]),
        instrument_id=str(payload["instrument_id"]),
        feature=feature,
        policy_version=str(payload["policy_version"]),
        formula_version=str(payload["formula_version"]),
        provenance=provenance,
        recorded_at=_datetime(payload["recorded_at"]),
        payload_fingerprint=str(payload["payload_fingerprint"]),
    )


def _outcome_to_payload(outcome: ProspectiveScientificOutcome) -> Mapping[str, object]:
    result = outcome.result
    return {
        "schema_version": _SCHEMA_VERSION,
        "outcome_id": outcome.outcome_id,
        "observation_id": outcome.observation_id,
        "hypothesis": outcome.hypothesis.value,
        "target": outcome.target.value,
        "target_at": outcome.target_at.isoformat(),
        "outcome_policy_version": outcome.outcome_policy_version,
        "source_event_ids": outcome.source_event_ids,
        "source_window_start": outcome.source_window_start.isoformat(),
        "source_window_end": outcome.source_window_end.isoformat(),
        "source_max_observed_at": outcome.source_max_observed_at.isoformat(),
        "input_fingerprint": outcome.input_fingerprint,
        "evaluated_at": outcome.evaluated_at.isoformat(),
        "payload_fingerprint": outcome.payload_fingerprint,
        "result": {
            "observation_id": result.observation_id,
            "target_at": result.target_at.isoformat(),
            "available": result.available,
            "reason": result.reason.value,
            "actual_value": result.actual_value,
            "cost_adjusted_value": result.cost_adjusted_value,
            "model_loss": result.model_loss,
            "benchmark_loss": result.benchmark_loss,
            "supported": result.supported,
        },
    }


def _outcome_from_payload(
    payload: Mapping[str, object],
) -> ProspectiveScientificOutcome:
    _require_schema(payload)
    raw_result = _mapping(payload["result"])
    result = ScientificModelOutcome(
        observation_id=str(raw_result["observation_id"]),
        target_at=_datetime(raw_result["target_at"]),
        available=bool(raw_result["available"]),
        reason=AbstentionReason(str(raw_result["reason"])),
        actual_value=_optional_float(raw_result.get("actual_value")),
        cost_adjusted_value=_optional_float(raw_result.get("cost_adjusted_value")),
        model_loss=_optional_float(raw_result.get("model_loss")),
        benchmark_loss=_optional_float(raw_result.get("benchmark_loss")),
        supported=(
            bool(raw_result["supported"])
            if raw_result.get("supported") is not None
            else None
        ),
    )
    return ProspectiveScientificOutcome(
        outcome_id=str(payload["outcome_id"]),
        observation_id=str(payload["observation_id"]),
        hypothesis=ScientificCandleHypothesis(str(payload["hypothesis"])),
        target=ScientificTarget(str(payload["target"])),
        target_at=_datetime(payload["target_at"]),
        result=result,
        outcome_policy_version=str(payload["outcome_policy_version"]),
        source_event_ids=tuple(
            str(item) for item in _sequence(payload["source_event_ids"])
        ),
        source_window_start=_datetime(payload["source_window_start"]),
        source_window_end=_datetime(payload["source_window_end"]),
        source_max_observed_at=_datetime(payload["source_max_observed_at"]),
        input_fingerprint=str(payload["input_fingerprint"]),
        evaluated_at=_datetime(payload["evaluated_at"]),
        payload_fingerprint=str(payload["payload_fingerprint"]),
    )


def _read_object(path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    if not isinstance(payload, dict):
        raise ValueError(f"evidence file must contain an object: {path}")
    return payload


def _require_schema(payload: Mapping[str, object]) -> None:
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("unsupported prospective evidence schema")


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected object in evidence payload")
    return value


def _sequence(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("expected array in evidence payload")
    return value


def _datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("expected ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _canonical(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

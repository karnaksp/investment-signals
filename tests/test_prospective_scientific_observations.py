from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json

import pytest

from tinvest_signal_engine.adapters.file_prospective_scientific_observations import (
    ImmutableFileProspectiveScientificStore,
)
from tinvest_signal_engine.application.prospective_scientific_observations import (
    ProcessMatureProspectiveScientificOutcomes,
    RecordProspectiveScientificObservation,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveEvidenceConflict,
    ProspectiveObservationProvenance,
    ProspectiveOutcomeEvidence,
    ProspectiveSourceKind,
    deterministic_prospective_observation_id,
    deterministic_prospective_outcome_id,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    AbstentionReason,
    FeatureDecision,
    ScientificCandlePolicy,
    opening_gap_feature,
    relative_volume_activity_feature,
)
from tinvest_signal_engine.services.prospective_scientific_observation_worker import (
    build_file_prospective_scientific_runtime,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 20, 9, 15, tzinfo=UTC)
FINGERPRINT_A = "sha256:" + "a" * 64
FINGERPRINT_B = "sha256:" + "b" * 64
POLICY = ScientificCandlePolicy()


def _feature(
    current_volume: float,
    *,
    history_days: int = 20,
    observed_at: datetime = OBSERVED_AT,
):
    return relative_volume_activity_feature(
        ticker="SBER",
        trading_day=date(2026, 7, 20),
        observed_at=observed_at,
        current_volume=current_volume,
        historical_phase_volumes=tuple(float(value) for value in range(history_days)),
        baseline_future_variance=2.0,
        policy=POLICY,
    )


def _provenance(
    *,
    observed_at: datetime = OBSERVED_AT,
    source_max_observed_at: datetime | None = None,
) -> ProspectiveObservationProvenance:
    maximum = source_max_observed_at or observed_at
    return ProspectiveObservationProvenance(
        source_kind=ProspectiveSourceKind.STREAM,
        source_event_ids=("candle-1", "candle-2"),
        source_window_start=observed_at - timedelta(minutes=14),
        source_window_end=observed_at,
        source_max_observed_at=maximum,
        input_fingerprint=FINGERPRINT_A,
        dataset_fingerprint=FINGERPRINT_B,
        scientific_source_ids=("Heston-Korajczyk-Sadka-2010",),
    )


class _MemoryStore:
    def __init__(self) -> None:
        self.observations = {}
        self.outcomes = {}

    def persist_observation(self, observation):
        existing = self.observations.get(observation.observation_id)
        if existing is not None:
            if existing.payload_fingerprint != observation.payload_fingerprint:
                raise ProspectiveEvidenceConflict("observation conflict")
            return PersistenceDisposition.REPLAYED
        self.observations[observation.observation_id] = observation
        return PersistenceDisposition.INSERTED

    def pending_observations(self, *, outcome_policy_version, limit):
        pending = tuple(
            item
            for item in self.observations.values()
            if deterministic_prospective_outcome_id(
                observation_id=item.observation_id,
                outcome_policy_version=outcome_policy_version,
            )
            not in self.outcomes
        )
        return tuple(sorted(pending, key=lambda item: item.target_at))[:limit]

    def persist_outcome(self, outcome):
        existing = self.outcomes.get(outcome.outcome_id)
        if existing is not None:
            if existing.payload_fingerprint != outcome.payload_fingerprint:
                raise ProspectiveEvidenceConflict("outcome conflict")
            return PersistenceDisposition.REPLAYED
        self.outcomes[outcome.outcome_id] = outcome
        return PersistenceDisposition.INSERTED


class _EvidenceSource:
    def __init__(self) -> None:
        self.by_observation = {}
        self.load_calls = 0

    def load(self, observation):
        self.load_calls += 1
        return self.by_observation[observation.observation_id]


def _record(store, feature, *, recorded_at=None):
    sealed_at = recorded_at or feature.observed_at + timedelta(seconds=1)
    return RecordProspectiveScientificObservation(store).execute(
        instrument_id="SBER_TQBR",
        feature=feature,
        policy_version=POLICY.version,
        formula_version="relative-volume-activity-v2",
        provenance=_provenance(observed_at=feature.observed_at),
        recorded_at=sealed_at,
    )


def _evidence(observation, *, available=True, actual_value=3.0):
    return ProspectiveOutcomeEvidence(
        observation_id=observation.observation_id,
        target_at=observation.target_at,
        available=available,
        reason=(
            AbstentionReason.CONDITIONS_MATCHED
            if available
            else AbstentionReason.OUTCOME_UNAVAILABLE
        ),
        actual_value=actual_value if available else None,
        source_event_ids=("future-candle-1",) if available else (),
        source_window_start=observation.feature.observed_at,
        source_window_end=observation.target_at,
        source_max_observed_at=observation.target_at,
        input_fingerprint=FINGERPRINT_B,
    )


def test_deterministic_identity_includes_sealed_policy_and_formula() -> None:
    feature = _feature(100.0)
    first = deterministic_prospective_observation_id(
        hypothesis=feature.hypothesis,
        hypothesis_version=feature.hypothesis_version,
        instrument_id="SBER_TQBR",
        observed_at=feature.observed_at,
        policy_version=POLICY.version,
        formula_version="relative-volume-activity-v2",
    )
    assert first == deterministic_prospective_observation_id(
        hypothesis=feature.hypothesis,
        hypothesis_version=feature.hypothesis_version,
        instrument_id="SBER_TQBR",
        observed_at=feature.observed_at,
        policy_version=POLICY.version,
        formula_version="relative-volume-activity-v2",
    )
    assert first != deterministic_prospective_observation_id(
        hypothesis=feature.hypothesis,
        hypothesis_version=feature.hypothesis_version,
        instrument_id="SBER_TQBR",
        observed_at=feature.observed_at,
        policy_version=POLICY.version,
        formula_version="relative-volume-activity-v3",
    )


def test_recording_preserves_matched_not_matched_and_abstain() -> None:
    store = _MemoryStore()
    observations = (
        _record(store, _feature(100.0))[0],
        _record(
            store,
            _feature(0.0, observed_at=OBSERVED_AT + timedelta(minutes=15)),
        )[0],
        _record(
            store,
            _feature(
                100.0,
                history_days=10,
                observed_at=OBSERVED_AT + timedelta(minutes=30),
            ),
        )[0],
    )
    assert tuple(item.feature.decision for item in observations) == (
        FeatureDecision.MATCHED,
        FeatureDecision.NOT_MATCHED,
        FeatureDecision.ABSTAIN,
    )
    assert len(store.observations) == 3


def test_future_source_data_is_rejected_before_persistence() -> None:
    with pytest.raises(ValueError, match="future source data"):
        RecordProspectiveScientificObservation(_MemoryStore()).execute(
            instrument_id="SBER_TQBR",
            feature=_feature(100.0),
            policy_version=POLICY.version,
            formula_version="relative-volume-activity-v2",
            provenance=_provenance(
                source_max_observed_at=OBSERVED_AT + timedelta(microseconds=1)
            ),
            recorded_at=OBSERVED_AT + timedelta(seconds=1),
        )


def test_outcome_worker_never_reads_market_evidence_before_maturity() -> None:
    store = _MemoryStore()
    evidence = _EvidenceSource()
    observation, _ = _record(store, _feature(100.0))
    evidence.by_observation[observation.observation_id] = _evidence(observation)
    worker = ProcessMatureProspectiveScientificOutcomes(
        store=store,
        evidence=evidence,
        policy=POLICY,
        outcome_policy_version="outcome-v1",
        grace_seconds=30,
    )
    result = worker.run_once(
        now=observation.target_at + timedelta(seconds=29),
        limit=10,
    )
    assert result.pending == 1
    assert result.stored == 0
    assert evidence.load_calls == 0


def test_mature_activity_outcome_is_sealed_once_and_is_idempotent() -> None:
    store = _MemoryStore()
    evidence = _EvidenceSource()
    observation, _ = _record(store, _feature(100.0))
    evidence.by_observation[observation.observation_id] = _evidence(observation)
    worker = ProcessMatureProspectiveScientificOutcomes(
        store=store,
        evidence=evidence,
        policy=POLICY,
        outcome_policy_version="outcome-v1",
        grace_seconds=30,
    )
    matured_at = observation.target_at + timedelta(seconds=30)
    first = worker.run_once(now=matured_at, limit=10)
    second = worker.run_once(now=matured_at + timedelta(seconds=1), limit=10)
    assert first.stored == 1
    assert first.unavailable == 0
    assert second.scanned == 0
    assert len(store.outcomes) == 1
    outcome = next(iter(store.outcomes.values()))
    assert outcome.result.actual_value == pytest.approx(1.5)
    assert outcome.result.supported is True


def test_mature_directional_outcome_uses_existing_scientific_formula() -> None:
    store = _MemoryStore()
    evidence = _EvidenceSource()
    feature = opening_gap_feature(
        ticker="SBER",
        trading_day=date(2026, 7, 20),
        observed_at=OBSERVED_AT,
        previous_close=100.0,
        opening_price=101.0,
        policy=POLICY,
    )
    observation, _ = RecordProspectiveScientificObservation(store).execute(
        instrument_id="SBER_TQBR",
        feature=feature,
        policy_version=POLICY.version,
        formula_version="opening-gap-reversion-v1",
        provenance=_provenance(),
        recorded_at=OBSERVED_AT + timedelta(seconds=1),
    )
    evidence.by_observation[observation.observation_id] = _evidence(
        observation,
        actual_value=-50.0,
    )
    result = ProcessMatureProspectiveScientificOutcomes(
        store=store,
        evidence=evidence,
        policy=POLICY,
        outcome_policy_version="outcome-v1",
        grace_seconds=0,
    ).run_once(now=observation.target_at, limit=10)
    assert result.stored == 1
    outcome = next(iter(store.outcomes.values()))
    assert outcome.result.actual_value == -50.0
    assert outcome.result.cost_adjusted_value == 40.0
    assert outcome.result.supported is True


def test_mature_missing_market_data_is_sealed_with_stable_reason() -> None:
    store = _MemoryStore()
    evidence = _EvidenceSource()
    observation, _ = _record(store, _feature(100.0))
    evidence.by_observation[observation.observation_id] = _evidence(
        observation,
        available=False,
        actual_value=None,
    )
    result = ProcessMatureProspectiveScientificOutcomes(
        store=store,
        evidence=evidence,
        policy=POLICY,
        outcome_policy_version="outcome-v1",
        grace_seconds=0,
    ).run_once(now=observation.target_at, limit=10)
    assert result.unavailable == 1
    outcome = next(iter(store.outcomes.values()))
    assert outcome.result.available is False
    assert outcome.result.reason is AbstentionReason.OUTCOME_UNAVAILABLE


def test_file_store_replays_semantically_identical_observation(tmp_path) -> None:
    store = ImmutableFileProspectiveScientificStore(tmp_path / "state")
    feature = _feature(100.0)
    observation, first = _record(store, feature)
    replayed, second = _record(
        store,
        feature,
        recorded_at=OBSERVED_AT + timedelta(seconds=10),
    )
    assert first is PersistenceDisposition.INSERTED
    assert second is PersistenceDisposition.REPLAYED
    assert replayed.payload_fingerprint == observation.payload_fingerprint
    assert store.load_observation(observation.observation_id) == observation


def test_file_store_rejects_same_identity_with_changed_provenance(tmp_path) -> None:
    store = ImmutableFileProspectiveScientificStore(tmp_path / "state")
    feature = _feature(100.0)
    _record(store, feature)
    with pytest.raises(ProspectiveEvidenceConflict):
        RecordProspectiveScientificObservation(store).execute(
            instrument_id="SBER_TQBR",
            feature=feature,
            policy_version=POLICY.version,
            formula_version="relative-volume-activity-v2",
            provenance=ProspectiveObservationProvenance(
                source_kind=ProspectiveSourceKind.STREAM,
                source_event_ids=("different-source",),
                source_window_start=OBSERVED_AT - timedelta(minutes=14),
                source_window_end=OBSERVED_AT,
                source_max_observed_at=OBSERVED_AT,
                input_fingerprint=FINGERPRINT_B,
                dataset_fingerprint=FINGERPRINT_B,
                scientific_source_ids=("Heston-Korajczyk-Sadka-2010",),
            ),
            recorded_at=OBSERVED_AT + timedelta(seconds=1),
        )


def test_file_runtime_wires_recorder_store_and_mature_worker(tmp_path) -> None:
    runtime = build_file_prospective_scientific_runtime(
        state_dir=tmp_path / "state",
        evidence_dir=tmp_path / "evidence",
        policy=POLICY,
        outcome_policy_version="outcome-v1",
        grace_seconds=0,
    )
    observation, disposition = runtime.recorder.execute(
        instrument_id="SBER_TQBR",
        feature=_feature(100.0),
        policy_version=POLICY.version,
        formula_version="relative-volume-activity-v2",
        provenance=_provenance(),
        recorded_at=OBSERVED_AT + timedelta(seconds=1),
    )
    assert disposition is PersistenceDisposition.INSERTED
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    evidence = _evidence(observation)
    (
        evidence_dir / f"{observation.observation_id.removeprefix('sha256:')}.json"
    ).write_text(
        json.dumps(
            {
                "schema_version": "prospective-scientific-evidence-v1",
                "observation_id": evidence.observation_id,
                "target_at": evidence.target_at.isoformat(),
                "available": evidence.available,
                "reason": evidence.reason.value,
                "actual_value": evidence.actual_value,
                "source_event_ids": evidence.source_event_ids,
                "source_window_start": evidence.source_window_start.isoformat(),
                "source_window_end": evidence.source_window_end.isoformat(),
                "source_max_observed_at": evidence.source_max_observed_at.isoformat(),
                "input_fingerprint": evidence.input_fingerprint,
            }
        ),
        encoding="utf-8",
    )
    result = runtime.outcome_worker.run_once(now=observation.target_at, limit=10)
    assert result.stored == 1
    outcome_id = deterministic_prospective_outcome_id(
        observation_id=observation.observation_id,
        outcome_policy_version="outcome-v1",
    )
    assert runtime.store.load_outcome(outcome_id).result.supported is True

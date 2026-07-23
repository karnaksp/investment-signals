from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from math import log

import pytest

from tinvest_signal_engine.adapters.in_memory_prospective_live_shadow import (
    InMemoryProspectiveLiveShadowStore,
)
from tinvest_signal_engine.application.prospective_live_shadow import (
    HarFeatureInput,
    JumpFeatureInput,
    LIVE_SHADOW_HYPOTHESES,
    ProcessProspectiveLiveOutcomes,
    ProspectiveLiveOutcomeEvidence,
    ProspectivePortfolioSnapshot,
    RecordProspectivePortfolioSnapshot,
    RelativeVolumeFeatureInput,
    SemivarianceFeatureInput,
    VolatilityJumpFeatureInput,
)
from tinvest_signal_engine.domain.prospective_live_shadow import (
    LIVE_SHADOW_RECORD_VERSION,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    HarV2Parameters,
    JumpHistoryPoint,
    ProspectiveHypothesis,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    TargetMetric,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    ProspectiveEvidenceConflict,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 20, 9, 30, tzinfo=UTC)
HISTORY_AT = OBSERVED_AT - timedelta(days=1)
DATASET = "sha256:" + "a" * 64
INPUT = "sha256:" + "b" * 64
EVIDENCE = "sha256:" + "c" * 64
POLICY = ProspectiveScientificPolicy(
    version="prospective-live-test-v1",
    jump_history_days=2,
    jump_percentile=0.75,
    jump_low_volume_percentile=0.50,
    jump_high_volume_percentile=0.75,
    jump_high_range_percentile=0.75,
    jump_high_illiquidity_percentile=0.75,
    volume_history_days=2,
    volume_percentile=0.75,
    har_minimum_training_points=1,
    semivariance_history_days=2,
    semivariance_percentile=0.75,
    jump_variance_history_days=2,
    jump_variance_percentile=0.75,
)


def _snapshot(*, sufficient: bool = True) -> ProspectivePortfolioSnapshot:
    history_count = 2 if sufficient else 0
    history_at = HISTORY_AT if sufficient else None
    return ProspectivePortfolioSnapshot(
        instrument_id="SBER_TQBR",
        ticker="SBER",
        trading_day=date(2026, 7, 20),
        observed_at=OBSERVED_AT,
        recorded_at=OBSERVED_AT + timedelta(seconds=1),
        source_event_ids=("candle-1", "candle-2"),
        dataset_fingerprint=DATASET,
        input_fingerprint=INPUT,
        trading_gap=False,
        jump=JumpFeatureInput(
            signed_return_bps=100.0,
            volume=1.0,
            range_bps=100.0,
            illiquidity=100.0,
            prior_history=tuple(
                JumpHistoryPoint(
                    absolute_return_bps=float(index + 1),
                    volume=float((index + 1) * 10),
                    range_bps=float(index + 1),
                    illiquidity=float(index + 1),
                )
                for index in range(history_count)
            ),
            history_observed_until=history_at,
        ),
        relative_volume=RelativeVolumeFeatureInput(
            current_volume=100.0,
            historical_volumes=tuple(
                float(index + 1) for index in range(history_count)
            ),
            baseline_future_variance=2.0 if sufficient else 0.0,
            history_observed_until=history_at,
        ),
        har=HarFeatureInput(
            short_variance=1.0,
            medium_variance=1.0,
            long_variance=1.0,
            parameters=(
                HarV2Parameters(
                    intercept=log(3.0),
                    short_weight=0.0,
                    medium_weight=0.0,
                    long_weight=0.0,
                    training_points=100,
                    trained_until=HISTORY_AT,
                )
                if sufficient
                else None
            ),
        ),
        semivariance=SemivarianceFeatureInput(
            downside_share=0.9,
            historical_downside_shares=tuple(
                0.1 * (index + 1) for index in range(history_count)
            ),
            baseline_future_variance=2.0 if sufficient else 0.0,
            history_observed_until=history_at,
        ),
        volatility_jump=VolatilityJumpFeatureInput(
            jump_share=0.9,
            continuous_variance=1.0,
            historical_jump_shares=tuple(
                0.1 * (index + 1) for index in range(history_count)
            ),
            baseline_future_variance=2.0 if sufficient else 0.0,
            history_observed_until=history_at,
        ),
    )


class _OutcomeSource:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.calls = 0

    def load(self, observation, *, as_of):
        self.calls += 1
        assert as_of >= observation.target_at
        feature = observation.feature
        actual = None
        ewma = phase = None
        if self.available:
            if feature.target is TargetMetric.FORWARD_RETURN:
                actual = (
                    -20.0
                    if feature.hypothesis
                    is ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2
                    else 20.0
                )
            elif feature.target is TargetMetric.FUTURE_REALIZED_VARIANCE:
                actual = 2.0
                ewma = phase = 10.0
            else:
                actual = 3.0
        return ProspectiveLiveOutcomeEvidence(
            observation_id=observation.observation_id,
            target_at=observation.target_at,
            available=self.available,
            actual_value=actual,
            evidence_fingerprint=EVIDENCE,
            ewma_baseline=ewma,
            phase_baseline=phase,
        )


def _row(result, hypothesis, horizon):
    return next(
        item
        for item in result.event.statistics.rows
        if item.hypothesis is hypothesis and item.horizon_seconds == horizon
    )


def test_full_portfolio_is_built_once_versioned_and_idempotent() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    recorder = RecordProspectivePortfolioSnapshot(store=store, policy=POLICY)

    first = recorder.execute(_snapshot())
    second = recorder.execute(_snapshot())

    assert first.stored == 8
    assert first.replayed == 0
    assert second.stored == 0
    assert second.replayed == 8
    assert first.observation_ids == second.observation_ids
    assert len(store.observations()) == 8
    assert {
        item.feature.hypothesis for item in store.observations()
    } == LIVE_SHADOW_HYPOTHESES
    assert all(
        item.record_version == LIVE_SHADOW_RECORD_VERSION
        and item.policy_version == POLICY.version
        and item.feature.hypothesis_version == item.feature.hypothesis.version
        for item in store.observations()
    )
    assert len(first.event.statistics.rows) == 8
    assert first.event.statistics.descriptive_only is True


def test_insufficient_history_is_persisted_as_explicit_abstention() -> None:
    result = RecordProspectivePortfolioSnapshot(
        store=InMemoryProspectiveLiveShadowStore(), policy=POLICY
    ).execute(_snapshot(sufficient=False))

    assert all(row.abstained_count == 1 for row in result.event.statistics.rows)
    reasons = {
        item.reason_code
        for row in result.event.statistics.rows
        for item in row.reasons_histogram
    }
    assert reasons == {
        ProspectiveReason.INSUFFICIENT_PRIOR_DAYS.value,
        ProspectiveReason.MODEL_NOT_TRAINED.value,
    }
    assert all(row.matched_count == 0 for row in result.event.statistics.rows)


def test_mature_worker_never_loads_future_outcomes() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    RecordProspectivePortfolioSnapshot(store=store, policy=POLICY).execute(_snapshot())
    source = _OutcomeSource()
    worker = ProcessProspectiveLiveOutcomes(
        store=store,
        source=source,
        policy=POLICY,
        outcome_policy_version="live-outcome-v1",
    )

    result = worker.run_once(
        now=OBSERVED_AT + timedelta(seconds=min(POLICY.jump_horizons_seconds) - 1),
        limit=20,
    )

    assert result.scanned == 8
    assert result.pending == 8
    assert result.stored == 0
    assert source.calls == 0


def test_mature_outcomes_accumulate_descriptive_live_shadow_statistics() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    RecordProspectivePortfolioSnapshot(store=store, policy=POLICY).execute(_snapshot())
    source = _OutcomeSource()
    worker = ProcessProspectiveLiveOutcomes(
        store=store,
        source=source,
        policy=POLICY,
        outcome_policy_version="live-outcome-v1",
    )
    now = OBSERVED_AT + timedelta(seconds=POLICY.volume_horizon_seconds)

    first = worker.run_once(now=now, limit=20)
    second = worker.run_once(now=now + timedelta(seconds=1), limit=20)

    assert first.stored == 8
    assert first.unavailable == 0
    assert second.scanned == 0
    assert len(store.outcomes(outcome_policy_version="live-outcome-v1")) == 8
    h3 = _row(
        first,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        POLICY.jump_horizons_seconds[0],
    )
    assert h3.matched_count == 1
    assert h3.matched_outcome_count == 1
    assert h3.positive_effect_count == 1
    assert h3.mean_effect == pytest.approx(10.0)
    h4 = _row(
        first,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        POLICY.jump_horizons_seconds[0],
    )
    assert h4.not_matched_count == 1
    assert h4.matched_outcome_count == 0
    assert h4.mean_effect is None
    h7 = _row(
        first,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        POLICY.volume_horizon_seconds,
    )
    assert h7.mean_effect == pytest.approx(0.5)
    h15 = _row(
        first,
        ProspectiveHypothesis.HAR_VOLATILITY_V2,
        POLICY.har_horizon_seconds,
    )
    assert h15.mean_effect is not None and h15.mean_effect > 0.0
    assert all(row.descriptive_only for row in first.event.statistics.rows)
    assert first.event.event_type == "prospective_live_shadow_updated"


def test_temporarily_unavailable_mature_outcomes_are_retried_during_grace() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    RecordProspectivePortfolioSnapshot(store=store, policy=POLICY).execute(_snapshot())
    source = _OutcomeSource(available=False)
    worker = ProcessProspectiveLiveOutcomes(
        store=store,
        source=source,
        policy=POLICY,
        outcome_policy_version="live-outcome-v1",
    )

    first = worker.run_once(
        now=OBSERVED_AT + timedelta(seconds=min(POLICY.jump_horizons_seconds)),
        limit=20,
    )
    source.available = True
    second = worker.run_once(
        now=OBSERVED_AT
        + timedelta(seconds=min(POLICY.jump_horizons_seconds) + 1),
        limit=20,
    )

    assert first.stored == 0
    assert first.pending == 8
    assert first.unavailable == 0
    assert second.stored == 2
    assert second.pending == 6
    assert second.unavailable == 0
    assert len(store.outcomes(outcome_policy_version="live-outcome-v1")) == 2


def test_unavailable_outcomes_are_sealed_after_availability_grace() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    RecordProspectivePortfolioSnapshot(store=store, policy=POLICY).execute(_snapshot())
    result = ProcessProspectiveLiveOutcomes(
        store=store,
        source=_OutcomeSource(available=False),
        policy=POLICY,
        outcome_policy_version="live-outcome-v1",
    ).run_once(
        now=OBSERVED_AT
        + timedelta(seconds=POLICY.volume_horizon_seconds, minutes=5),
        limit=20,
    )

    assert result.stored == 8
    assert result.pending == 0
    assert result.unavailable == 8
    assert all(row.data_coverage == 0.0 for row in result.event.statistics.rows)
    assert all(row.mean_effect is None for row in result.event.statistics.rows)


def test_next_ingest_event_retains_accumulated_outcome_statistics() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    recorder = RecordProspectivePortfolioSnapshot(
        store=store,
        policy=POLICY,
        outcome_policy_version="live-outcome-v1",
    )
    recorder.execute(_snapshot())
    ProcessProspectiveLiveOutcomes(
        store=store,
        source=_OutcomeSource(),
        policy=POLICY,
        outcome_policy_version="live-outcome-v1",
    ).run_once(
        now=OBSERVED_AT + timedelta(seconds=POLICY.volume_horizon_seconds),
        limit=20,
    )
    next_at = OBSERVED_AT + timedelta(hours=1)
    result = recorder.execute(
        replace(
            _snapshot(),
            observed_at=next_at,
            recorded_at=next_at + timedelta(seconds=1),
            source_event_ids=("candle-3", "candle-4"),
            input_fingerprint="sha256:" + "d" * 64,
        )
    )

    h7 = _row(
        result,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        POLICY.volume_horizon_seconds,
    )
    assert h7.observation_count == 2
    assert h7.mature_outcome_count == 1
    assert h7.available_outcome_count == 1


def test_same_identity_with_changed_source_payload_is_rejected() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    recorder = RecordProspectivePortfolioSnapshot(store=store, policy=POLICY)
    recorder.execute(_snapshot())

    with pytest.raises(ProspectiveEvidenceConflict):
        recorder.execute(replace(_snapshot(), input_fingerprint="sha256:" + "d" * 64))

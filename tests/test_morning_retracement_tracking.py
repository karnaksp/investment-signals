from __future__ import annotations

from datetime import date, datetime
from hashlib import sha256
import json
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.morning_retracement_signals import (
    MorningRetracementMarketSeries,
)
from tinvest_signal_engine.application.morning_retracement_tracking import (
    OUTCOME_POLICY_VERSION,
    ProcessMorningRetracementOutcomes,
    StoredMorningRetracementAssessment,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import (
    MorningSnapshot,
    RetracementDirection,
)
from tinvest_signal_engine.domain.morning_retracement_signal import (
    LinearProbabilityModel,
    MorningRetracementLiveAssessment,
    MorningRetracementRecommendation,
    MorningRetracementRuntimePolicy,
    MorningRetracementTrackedOutcome,
)


MOSCOW = ZoneInfo("Europe/Moscow")


class MemoryTrackingStore:
    def __init__(self, assessment: MorningRetracementLiveAssessment) -> None:
        self.assessment = assessment
        self.outcomes: list[MorningRetracementTrackedOutcome] = []

    def pending_assessments(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[StoredMorningRetracementAssessment, ...]:
        assert outcome_policy_version == OUTCOME_POLICY_VERSION
        assert limit > 0
        return (
            StoredMorningRetracementAssessment(
                observation_id="observation-1",
                assessment=self.assessment,
            ),
        )

    def persist_outcome(
        self,
        outcome: MorningRetracementTrackedOutcome,
        *,
        assessment: MorningRetracementLiveAssessment,
    ) -> None:
        assert assessment is self.assessment
        self.outcomes.append(outcome)


def _policy() -> MorningRetracementRuntimePolicy:
    model_payload = {
        "schema": "linear-probability-model-v1",
        "link": "logit",
        "positive_class": 1,
        "feature_names": ["ticker=SBER"],
        "coefficients": [0.0],
        "intercept": 0.0,
    }
    model_fingerprint = "sha256:" + sha256(
        json.dumps(
            model_payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return MorningRetracementRuntimePolicy(
        policy_version="test-policy",
        hypothesis_id="h1-morning-low-volume-reversion",
        hypothesis_version="2.2.0",
        model=LinearProbabilityModel(
            feature_names=("ticker=SBER",),
            coefficients=(0.0,),
            intercept=0.0,
            fingerprint=model_fingerprint,
        ),
        target_fraction=0.5,
        default_probability_threshold=0.5,
        stop_extension_fraction=0.4,
        break_even_trigger_fraction=0.33,
        deadline_local_minute=11 * 60,
        round_trip_cost_bps=0.0,
        require_volume_baseline=False,
        default_maximum_relative_volume=1.0,
        default_minimum_active_minute_ratio=0.0,
        historical_target_probability=0.72,
        historical_target_probability_lower=0.58,
        historical_non_loss_probability=0.72,
        historical_non_loss_probability_lower=0.60,
        historical_sample_count=72,
        historical_trading_days=22,
        expected_hit_minutes_p25=11,
        expected_hit_minutes_median=31,
        expected_hit_minutes_p75=76,
    )


def _assessment() -> MorningRetracementLiveAssessment:
    observed_at = datetime(2026, 7, 28, 9, 0, tzinfo=MOSCOW)
    snapshot = MorningSnapshot(
        ticker="SBER",
        observed_at=observed_at,
        previous_close=100.0,
        current_price=101.8,
        running_extreme=102.0,
        extreme_at=observed_at,
        direction=RetracementDirection.RETURN_DOWN,
        excursion_bps=200.0,
        tick_size=0.01,
    )
    return MorningRetracementLiveAssessment(
        instrument_id="instrument-sber",
        ticker="SBER",
        trading_day="2026-07-28",
        recommendation=MorningRetracementRecommendation(
            snapshot=snapshot,
            model_probability=0.8,
            target_price=101.0,
            initial_stop_price=102.8,
            break_even_trigger_price=101.34,
            break_even_stop_price=101.79,
            relative_volume=0.4,
            active_minute_ratio=0.9,
            observed_at=observed_at,
        ),
        eligible_for_signal=True,
        reason_codes=(),
        settings_revision=1,
        policy_version="test-policy",
        hypothesis_version="2.2.0",
        model_fingerprint="sha256:test",
        probability_threshold=0.7,
        maximum_relative_volume=0.5,
        minimum_excursion_bps=40.0,
        minimum_remaining_move_bps=20.0,
        remaining_move_bps=78.0,
        deadline_local_minute=11 * 60,
        expected_hit_minutes_p25=11,
        expected_hit_minutes_median=31,
        expected_hit_minutes_p75=76,
        training_window_ended=False,
    )


def _candle(at: datetime, *, open_price: float, low: float) -> HistoricalCandle:
    return HistoricalCandle(
        ticker="SBER",
        at=at,
        open=open_price,
        high=open_price + 0.1,
        low=low,
        close=low + 0.05,
        volume=100.0,
        complete=True,
    )


def test_background_tracker_seals_each_entry_minute_after_deadline() -> None:
    assessment = _assessment()
    store = MemoryTrackingStore(assessment)
    market = MorningRetracementMarketSeries(
        instrument_id=assessment.instrument_id,
        ticker="SBER",
        class_code="TQBR",
        alias="SBER_TQBR",
        trading_day=date(2026, 7, 28),
        previous_session=(),
        current_session=(
            _candle(
                datetime(2026, 7, 28, 9, 1, tzinfo=MOSCOW),
                open_price=101.7,
                low=100.9,
            ),
        ),
        historical_cumulative_volume=None,
    )

    batch = ProcessMorningRetracementOutcomes(
        store=store,
        policy=_policy(),
    ).execute(
        now=datetime(2026, 7, 28, 11, 2, tzinfo=MOSCOW),
        market=(market,),
    )

    assert batch.stored == 1
    assert batch.unavailable == 0
    assert store.outcomes[0].target_hit is True
    assert store.outcomes[0].exit_reason == "target"
    assert store.outcomes[0].minutes_to_exit == 0.0

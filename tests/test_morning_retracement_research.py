from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.application.morning_retracement_research import (
    BuildMorningRetracementResearch,
    PreviousSignalEvent,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import (
    RetracementDirection,
    TradeExitReason,
    TradePolicy,
    build_snapshot,
    observe_retracements,
    simulate_trade,
)
from scripts.research_morning_retracement import (
    _clustered_day_bootstrap_interval,
    _passes_operational_filter,
    _product_gates,
    _recommendation_summary,
    _select_first_episode_signal,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def test_operational_filter_rejects_sparse_or_overactive_mornings() -> None:
    specification = {
        "require_volume_baseline": True,
        "maximum_relative_volume": 0.5,
        "minimum_active_minute_ratio": 0.75,
    }
    accepted = SimpleNamespace(
        feature_values=lambda _family: {
            "morning_volume_baseline_available": 1.0,
            "morning_relative_volume": 0.45,
            "morning_active_minute_ratio": 0.80,
        }
    )
    sparse = SimpleNamespace(
        feature_values=lambda _family: {
            "morning_volume_baseline_available": 1.0,
            "morning_relative_volume": 0.30,
            "morning_active_minute_ratio": 0.40,
        }
    )
    high_volume = SimpleNamespace(
        feature_values=lambda _family: {
            "morning_volume_baseline_available": 1.0,
            "morning_relative_volume": 0.80,
            "morning_active_minute_ratio": 0.90,
        }
    )

    assert _passes_operational_filter(accepted, specification) is True
    assert _passes_operational_filter(sparse, specification) is False
    assert _passes_operational_filter(high_volume, specification) is False


def _candle(
    at: datetime,
    price: float,
    *,
    high: float | None = None,
    low: float | None = None,
    volume: float = 100.0,
    ticker: str = "SBER",
) -> HistoricalCandle:
    return HistoricalCandle(
        ticker=ticker,
        at=at,
        open=price,
        high=high if high is not None else price,
        low=low if low is not None else price,
        close=price,
        volume=volume,
    )


def _snapshot() -> tuple[HistoricalCandle, ...]:
    start = datetime(2026, 7, 20, 7, 0, tzinfo=MOSCOW)
    return (
        _candle(start, 100.5, high=100.6, low=100.4),
        _candle(start + timedelta(minutes=5), 101.0, high=101.1, low=100.5),
        _candle(start + timedelta(minutes=10), 102.0, high=102.0, low=100.9),
        _candle(start + timedelta(minutes=15), 101.8, high=101.9, low=101.7),
    )


def test_build_snapshot_uses_only_running_extreme_and_path_first_passage() -> None:
    rows = _snapshot()
    snapshot = build_snapshot(
        ticker="SBER",
        observed_at=rows[-1].at,
        previous_close=100.0,
        observed_candles=rows,
        analytical_floor_bps=10.0,
        tick_size=0.01,
    )

    assert snapshot is not None
    assert snapshot.running_extreme == 102.0
    assert snapshot.direction is RetracementDirection.RETURN_DOWN
    assert snapshot.target_price(0.5) == 101.0

    future = (
        _candle(rows[-1].at + timedelta(minutes=1), 101.7, high=101.8, low=101.5),
        _candle(rows[-1].at + timedelta(minutes=2), 101.1, high=101.6, low=100.9),
    )
    labels = observe_retracements(snapshot, future)
    half = next(item for item in labels if item.fraction == 0.5)
    assert half.reached is True
    assert half.first_reached_at == future[-1].at
    assert half.minutes_to_target == 2.0


def test_trade_simulator_resolves_same_candle_target_and_stop_adversely() -> None:
    rows = _snapshot()
    snapshot = build_snapshot(
        ticker="SBER",
        observed_at=rows[-1].at,
        previous_close=100.0,
        observed_candles=rows,
        analytical_floor_bps=10.0,
        tick_size=0.01,
    )
    assert snapshot is not None
    future = (
        _candle(
            rows[-1].at + timedelta(minutes=1),
            101.8,
            high=102.6,
            low=100.9,
        ),
    )
    policy = TradePolicy(
        target_fraction=0.5,
        stop_extension_fraction=0.25,
        break_even_trigger_fraction=0.25,
        deadline_local_minute=11 * 60,
        round_trip_cost_bps=10.0,
    )

    result = simulate_trade(snapshot, future, policy)

    assert result.exit_reason is TradeExitReason.INITIAL_STOP
    assert result.net_result_bps is not None
    assert result.net_result_bps < 0.0


def test_break_even_trigger_produces_non_loss_after_modeled_costs() -> None:
    rows = _snapshot()
    snapshot = build_snapshot(
        ticker="SBER",
        observed_at=rows[-1].at,
        previous_close=100.0,
        observed_candles=rows,
        analytical_floor_bps=10.0,
        tick_size=0.01,
    )
    assert snapshot is not None
    future = (
        _candle(
            rows[-1].at + timedelta(minutes=1),
            101.8,
            high=101.9,
            low=101.2,
        ),
    )
    policy = TradePolicy(
        target_fraction=0.75,
        stop_extension_fraction=0.25,
        break_even_trigger_fraction=0.25,
        deadline_local_minute=11 * 60,
        round_trip_cost_bps=10.0,
    )

    result = simulate_trade(snapshot, future, policy)

    assert result.exit_reason is TradeExitReason.BREAK_EVEN
    assert result.break_even_armed_at == future[0].at
    assert result.net_result_bps is not None
    assert result.net_result_bps > 0.0


def test_break_even_can_follow_remaining_entry_to_target_progress() -> None:
    rows = _snapshot()
    snapshot = build_snapshot(
        ticker="SBER",
        observed_at=rows[-1].at,
        previous_close=100.0,
        observed_candles=rows,
        analytical_floor_bps=10.0,
        tick_size=0.01,
    )
    assert snapshot is not None
    future = (
        _candle(
            rows[-1].at + timedelta(minutes=1),
            101.8,
            high=101.9,
            low=101.3,
        ),
    )
    policy = TradePolicy(
        target_fraction=0.5,
        stop_extension_fraction=0.25,
        break_even_trigger_fraction=0.25,
        deadline_local_minute=11 * 60,
        round_trip_cost_bps=10.0,
        break_even_target_progress_fraction=0.5,
    )

    result = simulate_trade(snapshot, future, policy)

    assert result.break_even_trigger_price == pytest.approx(101.4)
    assert result.exit_reason is TradeExitReason.BREAK_EVEN
    assert result.net_result_bps is not None
    assert result.net_result_bps > 0.0


def test_gap_through_target_is_not_counted_as_an_executable_trade() -> None:
    rows = _snapshot()
    snapshot = build_snapshot(
        ticker="SBER",
        observed_at=rows[-1].at,
        previous_close=100.0,
        observed_candles=rows,
        analytical_floor_bps=10.0,
        tick_size=0.01,
    )
    assert snapshot is not None
    future = (
        _candle(
            rows[-1].at + timedelta(minutes=1),
            100.8,
            high=100.9,
            low=100.7,
        ),
    )
    policy = TradePolicy(
        target_fraction=0.5,
        stop_extension_fraction=0.25,
        break_even_trigger_fraction=0.25,
        deadline_local_minute=11 * 60,
        round_trip_cost_bps=10.0,
    )

    result = simulate_trade(snapshot, future, policy)

    assert result.exit_reason is TradeExitReason.UNAVAILABLE
    assert result.entry_price is None
    assert result.net_result_bps is None


def test_dataset_includes_previous_session_without_immature_outcome_leakage() -> None:
    prior_start = datetime(2026, 7, 17, 10, 0, tzinfo=MOSCOW)
    prior = tuple(
        _candle(
            prior_start + timedelta(minutes=index),
            99.0 + index / 480.0,
            high=99.1 + index / 480.0,
            low=98.9 + index / 480.0,
            volume=100.0 + index,
        )
        for index in range(481)
    )
    morning_start = datetime(2026, 7, 20, 7, 0, tzinfo=MOSCOW)
    morning = tuple(
        _candle(
            morning_start + timedelta(minutes=index),
            102.0 - index / 240.0,
            high=102.05 - index / 240.0,
            low=101.95 - index / 240.0,
            volume=50.0 + index,
        )
        for index in range(241)
    )
    event = PreviousSignalEvent(
        ticker="SBER",
        event_at=datetime(2026, 7, 17, 17, 30, tzinfo=MOSCOW),
        signal_type="price_jump",
        direction=1,
        outcome_ready_at=datetime(2026, 7, 20, 12, 0, tzinfo=MOSCOW),
        outcome_confirmed=True,
    )

    examples = BuildMorningRetracementResearch().execute(
        prior + morning,
        previous_signals=(event,),
    )

    assert examples
    example = examples[0]
    assert example.label_available is True
    assert all(
        feature.observed_at <= example.feature_cutoff_at for feature in example.features
    )
    previous = example.feature_values("previous_session")
    assert previous["prior_signal_count"] == 1.0
    assert previous["prior_mature_signal_count"] == 0.0
    assert previous["prior_confirmed_signal_count"] == 0.0
    assert example.label_for(0.5).reached is True


def test_current_morning_signal_is_visible_only_after_its_event_time() -> None:
    prior_start = datetime(2026, 7, 17, 10, 0, tzinfo=MOSCOW)
    prior = tuple(
        _candle(prior_start + timedelta(minutes=index), 100.0) for index in range(481)
    )
    morning_start = datetime(2026, 7, 20, 7, 0, tzinfo=MOSCOW)
    morning = tuple(
        _candle(
            morning_start + timedelta(minutes=index),
            102.0 - index / 240.0,
            high=102.05 - index / 240.0,
            low=101.95 - index / 240.0,
        )
        for index in range(241)
    )
    event = PreviousSignalEvent(
        ticker="SBER",
        event_at=morning_start + timedelta(minutes=30),
        signal_type="volume_spike",
        direction=-1,
    )

    examples = BuildMorningRetracementResearch().execute(
        prior + morning,
        previous_signals=(event,),
    )
    before = max(
        (item for item in examples if item.snapshot.observed_at < event.event_at),
        key=lambda item: item.snapshot.observed_at,
    )
    after = min(
        (item for item in examples if item.snapshot.observed_at >= event.event_at),
        key=lambda item: item.snapshot.observed_at,
    )

    assert "morning_signal_count" not in before.feature_values("morning")
    assert after.feature_values("morning")["morning_signal_count"] == 1.0


def test_relative_volume_uses_only_earlier_trading_days() -> None:
    rows: list[HistoricalCandle] = []
    for day, volume in (
        (datetime(2026, 7, 16, 7, 0, tzinfo=MOSCOW), 100.0),
        (datetime(2026, 7, 17, 7, 0, tzinfo=MOSCOW), 200.0),
        (datetime(2026, 7, 20, 7, 0, tzinfo=MOSCOW), 150.0),
    ):
        rows.extend(
            _candle(
                day + timedelta(minutes=index),
                100.0 if index == 0 else 101.0,
                high=101.2,
                low=99.9,
                volume=volume,
            )
            for index in range(241)
        )

    examples = BuildMorningRetracementResearch().execute(tuple(rows))
    second_day = next(
        item
        for item in examples
        if item.trading_day == date(2026, 7, 17)
        and item.snapshot.observed_at.hour == 7
        and item.snapshot.observed_at.minute == 15
    )
    third_day = next(
        item
        for item in examples
        if item.trading_day == date(2026, 7, 20)
        and item.snapshot.observed_at.hour == 7
        and item.snapshot.observed_at.minute == 15
    )

    assert second_day.feature_values("morning")["morning_relative_volume"] == 2.0
    assert third_day.feature_values("morning")["morning_relative_volume"] == 1.0
    assert (
        third_day.feature_values("morning")["morning_volume_baseline_available"] == 1.0
    )


def test_snapshot_rejects_future_extreme() -> None:
    rows = _snapshot()
    with pytest.raises(ValueError, match="feature leakage"):
        from tinvest_signal_engine.application.morning_retracement_research import (
            MorningRetracementExample,
            ResearchFeature,
        )

        snapshot = build_snapshot(
            ticker="SBER",
            observed_at=rows[-1].at,
            previous_close=100.0,
            observed_candles=rows,
            analytical_floor_bps=10.0,
            tick_size=0.01,
        )
        assert snapshot is not None
        MorningRetracementExample(
            episode_id="SBER:2026-07-20",
            row_id="row",
            trading_day=rows[-1].at.date(),
            snapshot=snapshot,
            feature_cutoff_at=snapshot.observed_at,
            features=(
                ResearchFeature(
                    "future",
                    1.0,
                    snapshot.observed_at + timedelta(minutes=1),
                    "morning",
                ),
            ),
            labels=(),
            future_candles=(),
            maximum_retracement_fraction=0.0,
            maximum_adverse_extension_fraction=0.0,
            label_available=False,
        )


def test_probability_selection_emits_only_first_signal_per_episode() -> None:
    prior_start = datetime(2026, 7, 17, 10, 0, tzinfo=MOSCOW)
    prior = tuple(
        _candle(prior_start + timedelta(minutes=index), 100.0) for index in range(481)
    )
    morning_start = datetime(2026, 7, 20, 7, 0, tzinfo=MOSCOW)
    morning = tuple(
        _candle(
            morning_start + timedelta(minutes=index),
            102.0 - index / 240.0,
            high=102.05 - index / 240.0,
            low=101.95 - index / 240.0,
        )
        for index in range(241)
    )
    examples = BuildMorningRetracementResearch().execute(prior + morning)
    assert len(examples) > 1

    selected = _select_first_episode_signal(
        examples,
        [0.99] * len(examples),
        0.90,
        0.50,
        10.0,
    )

    assert len(selected) == 1
    assert selected[0][0] == min(
        (item for item in examples if item.snapshot.excursion_bps >= 40.0),
        key=lambda item: item.snapshot.observed_at,
    )


def test_product_gate_requires_safety_and_evidence_not_only_positive_median() -> None:
    holdout = {
        "trades": 299,
        "trading_days": 29,
        "target_hit_rate": 0.95,
        "target_wilson_lower": 0.90,
        "non_loss_rate": 0.99,
        "non_loss_wilson_lower": 0.95,
        "median_net_bps": 1.0,
        "tickers": 5,
        "maximum_instrument_share": 0.30,
    }
    stress = {"median_net_bps": 0.5}

    gates = _product_gates(holdout, stress)

    assert {item["gate"] for item in gates if not item["passed"]} == {
        "minimum_episodes_300",
        "minimum_trading_days_30",
    }


def test_day_clustered_confidence_interval_is_reproducible() -> None:
    examples = [
        SimpleNamespace(trading_day=date(2026, 7, 20)),
        SimpleNamespace(trading_day=date(2026, 7, 20)),
        SimpleNamespace(trading_day=date(2026, 7, 21)),
        SimpleNamespace(trading_day=date(2026, 7, 21)),
    ]
    values = [1.0, 1.0, 0.0, 0.0]

    first = _clustered_day_bootstrap_interval(examples, values)
    second = _clustered_day_bootstrap_interval(examples, values)

    assert first == second
    assert first[0] <= 0.5 <= first[1]


def test_product_gate_does_not_require_control_or_independent_holdout() -> None:
    holdout = {
        "trades": 300,
        "trading_days": 30,
        "target_hit_rate": 0.95,
        "target_day_bootstrap_lower": 0.85,
        "non_loss_rate": 0.99,
        "non_loss_day_bootstrap_lower": 0.95,
        "median_net_bps": 1.0,
        "tickers": 5,
        "maximum_instrument_share": 0.30,
    }
    stress = {"median_net_bps": 0.5}

    gates = _product_gates(holdout, stress)

    assert all(item["passed"] for item in gates)
    assert {item["gate"] for item in gates}.isdisjoint(
        {
            "matched_control_target_lift_lower_above_zero",
            "previous_session_incremental_value_when_used",
            "independent_holdout_not_previously_opened",
        }
    )


def test_observed_recommendation_preserves_probability_and_sample_size() -> None:
    recommendation = _recommendation_summary(
        {
            "trades": 79,
            "trading_days": 17,
            "target_hit_rate": 0.608,
            "target_day_bootstrap_lower": 0.50,
            "non_loss_rate": 0.81,
            "non_loss_day_bootstrap_lower": 0.71,
        },
        target_fraction=0.75,
    )

    assert recommendation == {
        "status": "observed",
        "validated": False,
        "target_fraction": 0.75,
        "target_probability": 0.608,
        "target_probability_lower": 0.50,
        "non_loss_probability": 0.81,
        "non_loss_probability_lower": 0.71,
        "sample_count": 79,
        "trading_days": 17,
        "disclaimer_code": "historical_observation_not_guarantee",
    }

from __future__ import annotations

from datetime import date, datetime, timedelta
from math import exp
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificRequest,
    build_prospective_scientific_research,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_scientific_models import (
    JumpVarianceContrastHistoryPoint,
    ProspectiveDecision,
    ProspectiveHypothesis,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    SemivarianceContrastHistoryPoint,
    downside_semivariance_contrast_v2_feature,
    volatility_jump_contrast_v2_feature,
)


MOSCOW = ZoneInfo("Europe/Moscow")
DATASET = "sha256:" + "d" * 64


def _observed_at() -> datetime:
    return datetime(2026, 7, 24, 12, 0, tzinfo=MOSCOW)


def _semivariance_history(
    observed_at: datetime, count: int = 40
) -> tuple[SemivarianceContrastHistoryPoint, ...]:
    return tuple(
        SemivarianceContrastHistoryPoint(
            downside_variance=2.0,
            upside_variance=8.0,
            future_variance=12.0 + index,
            target_at=observed_at - timedelta(days=count - index),
        )
        for index in range(count)
    )


def _jump_history(
    observed_at: datetime, count: int = 60
) -> tuple[JumpVarianceContrastHistoryPoint, ...]:
    return tuple(
        JumpVarianceContrastHistoryPoint(
            jump_variance=2.0,
            continuous_variance=8.0,
            future_variance=20.0 + index,
            target_at=observed_at - timedelta(days=count - index),
        )
        for index in range(count)
    )


def test_h16v2_compares_negative_to_positive_same_scale_without_direction() -> None:
    observed_at = _observed_at()
    history = _semivariance_history(observed_at)
    policy = ProspectiveScientificPolicy(
        semivariance_v2_minimum_comparables=5,
    )

    feature = downside_semivariance_contrast_v2_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        downside_variance=6.0,
        upside_variance=4.0,
        prior_same_phase=history,
        history_observed_until=history[-1].target_at,
        trading_gap=False,
        policy=policy,
    )

    assert feature.hypothesis is (
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2
    )
    assert feature.hypothesis_version == "2.0.0"
    assert feature.decision is ProspectiveDecision.MATCHED
    assert feature.expected_direction == 0
    assert feature.value("comparable_positive_window_count") == 40.0
    assert feature.value("baseline_future_variance") == pytest.approx(31.5)
    assert feature.history_observed_until < feature.observed_at


def test_h17v2_compares_jump_to_continuous_same_scale_without_direction() -> None:
    observed_at = _observed_at()
    history = _jump_history(observed_at)
    policy = ProspectiveScientificPolicy(
        jump_variance_v2_minimum_comparables=5,
    )

    feature = volatility_jump_contrast_v2_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        jump_variance=6.0,
        continuous_variance=4.0,
        prior_same_phase=history,
        history_observed_until=history[-1].target_at,
        trading_gap=False,
        policy=policy,
    )

    assert feature.hypothesis is ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2
    assert feature.hypothesis_version == "2.0.0"
    assert feature.decision is ProspectiveDecision.MATCHED
    assert feature.expected_direction == 0
    assert feature.value("comparable_continuous_window_count") == 60.0
    assert feature.value("baseline_future_variance") == pytest.approx(49.5)
    assert feature.history_observed_until < feature.observed_at


@pytest.mark.parametrize(
    ("builder", "history"),
    (
        ("semivariance", _semivariance_history),
        ("jump", _jump_history),
    ),
)
def test_v2_risk_features_reject_future_or_falsified_history_boundaries(
    builder: str,
    history,
) -> None:
    observed_at = _observed_at()
    points = history(observed_at)
    future = replace_last_target(points, observed_at + timedelta(minutes=1))
    common = {
        "ticker": "SBER",
        "trading_day": observed_at.date(),
        "observed_at": observed_at,
        "history_observed_until": future[-1].target_at,
        "trading_gap": False,
        "policy": ProspectiveScientificPolicy(),
    }

    with pytest.raises(ValueError, match="must precede the observation"):
        if builder == "semivariance":
            downside_semivariance_contrast_v2_feature(
                downside_variance=6.0,
                upside_variance=4.0,
                prior_same_phase=future,
                **common,
            )
        else:
            volatility_jump_contrast_v2_feature(
                jump_variance=6.0,
                continuous_variance=4.0,
                prior_same_phase=future,
                **common,
            )

    with pytest.raises(ValueError, match="boundary does not match"):
        if builder == "semivariance":
            downside_semivariance_contrast_v2_feature(
                downside_variance=6.0,
                upside_variance=4.0,
                prior_same_phase=points,
                **(common | {"history_observed_until": points[-2].target_at}),
            )
        else:
            volatility_jump_contrast_v2_feature(
                jump_variance=6.0,
                continuous_variance=4.0,
                prior_same_phase=points,
                **(common | {"history_observed_until": points[-2].target_at}),
            )


def replace_last_target(points, target_at):
    last = points[-1]
    if isinstance(last, SemivarianceContrastHistoryPoint):
        replacement = SemivarianceContrastHistoryPoint(
            downside_variance=last.downside_variance,
            upside_variance=last.upside_variance,
            future_variance=last.future_variance,
            target_at=target_at,
        )
    else:
        replacement = JumpVarianceContrastHistoryPoint(
            jump_variance=last.jump_variance,
            continuous_variance=last.continuous_variance,
            future_variance=last.future_variance,
            target_at=target_at,
        )
    return (*points[:-1], replacement)


def _candles(days: int) -> tuple[HistoricalCandle, ...]:
    rows: list[HistoricalCandle] = []
    start = date(2026, 1, 1)
    for day_index in range(days):
        trading_day = start + timedelta(days=day_index)
        price = 100.0
        for minute in range(95):
            at = datetime.combine(
                trading_day,
                datetime.min.time(),
                tzinfo=MOSCOW,
            ) + timedelta(hours=10, minutes=minute)
            # Deterministic signed variation supplies both semivariances and a
            # small number of jump-like windows without any random state.
            signed_bps = -8.0 if minute % 7 == 0 else 5.0 if minute % 2 == 0 else -4.0
            close = price * exp(signed_bps / 10_000.0)
            rows.append(
                HistoricalCandle(
                    ticker="SBER",
                    at=at,
                    open=price,
                    high=max(price, close),
                    low=min(price, close),
                    close=close,
                    volume=100.0 + minute,
                )
            )
            price = close
    return tuple(rows)


def test_h16v2_h17v2_historical_portfolio_is_deterministic_and_causal() -> None:
    policy = ProspectiveScientificPolicy(
        semivariance_v2_minimum_comparables=1,
        jump_variance_v2_minimum_comparables=1,
    )
    request = ProspectiveScientificRequest(
        selected_hypotheses=(
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2,
            ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2,
        ),
        policy=policy,
    )
    candles = _candles(65)

    first = build_prospective_scientific_research(
        candles,
        dataset_fingerprint=DATASET,
        request=request,
    )
    second = build_prospective_scientific_research(
        reversed(candles),
        dataset_fingerprint=DATASET,
        request=request,
    )

    assert first == second
    assert first.report_fingerprint == second.report_fingerprint
    assert {item.hypothesis for item in first.features} == set(
        request.selected_hypotheses
    )
    assert all(item.expected_direction == 0 for item in first.features)
    assert all(
        item.history_observed_until is None
        or item.history_observed_until < item.observed_at
        for item in first.features
    )


def test_v2_historical_portfolio_abstains_when_phase_history_is_incomplete() -> None:
    request = ProspectiveScientificRequest(
        selected_hypotheses=(
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2,
            ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2,
        )
    )

    report = build_prospective_scientific_research(
        _candles(10),
        dataset_fingerprint=DATASET,
        request=request,
    )

    assert report.features
    assert all(item.decision is ProspectiveDecision.ABSTAIN for item in report.features)
    assert {item.reason for item in report.features} == {
        ProspectiveReason.INSUFFICIENT_PRIOR_DAYS
    }

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificRequest,
    build_prospective_scientific_research,
)
from tinvest_signal_engine.application.relative_value_live_shadow import (
    BuildRelativeValueLiveFeatures,
    MarketResidualLiveInput,
    PairResidualLiveInput,
    RelativeValueLiveSnapshot,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_scientific_models import (
    FrozenMarketResidualParameters,
    FrozenPairParameters,
    ProspectiveDecision,
    ProspectiveHypothesis,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    pair_residual_reversion_v2_feature,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def _at(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=MOSCOW)


def _candle(ticker: str, at: datetime, price: float) -> HistoricalCandle:
    return HistoricalCandle(
        ticker=ticker,
        at=at,
        open=price,
        high=price * 1.0002,
        low=price * 0.9998,
        close=price,
        volume=1_000.0,
    )


def _candles(*, days: int = 10) -> tuple[HistoricalCandle, ...]:
    start = date(2026, 6, 1)
    tickers = ("AAA", "BBB", "CCC", "DDD", "PAIR", "PAIRP")
    rows: list[HistoricalCandle] = []
    for day_offset in range(days):
        trading_day = start + timedelta(days=day_offset)
        for minute in range(90):
            at = _at(trading_day, 10 + minute // 60, minute % 60)
            market_move = day_offset * 0.20 + minute * 0.015
            for ticker_index, ticker in enumerate(tickers[:4]):
                residual = ((day_offset + ticker_index) % 3 - 1) * minute * 0.003
                rows.append(
                    _candle(
                        ticker,
                        at,
                        100.0 + ticker_index * 10.0 + market_move + residual,
                    )
                )
            pair_base = 80.0 + day_offset * 0.10 + minute * 0.01
            rows.append(_candle("PAIRP", at, pair_base))
            rows.append(
                _candle(
                    "PAIR",
                    at,
                    5.0 + 1.15 * pair_base + ((day_offset % 3) - 1) * 0.02,
                )
            )
    return tuple(rows)


def _policy() -> ProspectiveScientificPolicy:
    return ProspectiveScientificPolicy(
        market_residual_history_days=2,
        market_residual_percentile=0.80,
        market_residual_horizons_seconds=(900,),
        pair_history_days=2,
        pair_min_training_points=4,
        pair_min_correlation=0.10,
        pair_horizons_seconds=(900,),
    )


def test_relative_value_replay_is_deterministic_and_causal() -> None:
    candles = _candles()
    request = ProspectiveScientificRequest(
        selected_hypotheses=(
            ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
            ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
        ),
        policy=_policy(),
        market_universe=("AAA", "BBB", "CCC", "DDD"),
        pair_candidates=(("PAIR", "PAIRP"),),
    )

    first = build_prospective_scientific_research(
        candles,
        dataset_fingerprint="sha256:first",
        request=request,
    )
    repeated = build_prospective_scientific_research(
        tuple(reversed(candles)),
        dataset_fingerprint="sha256:first",
        request=request,
    )

    assert first.report_fingerprint == repeated.report_fingerprint
    assert first.features == repeated.features
    assert {item.hypothesis for item in first.features} == {
        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
    }
    assert first.pair_parameters
    assert all(
        item.model_trained_until is None or item.model_trained_until < item.observed_at
        for item in first.features
    )

    cutoff = _at(date(2026, 6, 8), 11, 0)
    changed = tuple(
        _candle(item.ticker, item.at, item.close * 1.5) if item.at > cutoff else item
        for item in candles
    )
    later_mutation = build_prospective_scientific_research(
        changed,
        dataset_fingerprint="sha256:changed",
        request=request,
    )
    assert tuple(
        item for item in first.features if item.observed_at <= cutoff
    ) == tuple(item for item in later_mutation.features if item.observed_at <= cutoff)


def test_market_residual_abstains_when_fixed_basket_is_incomplete() -> None:
    candles = _candles(days=6)
    damaged_day = date(2026, 6, 6)
    damaged = tuple(
        item
        for item in candles
        if not (
            item.ticker in {"BBB", "CCC", "DDD"}
            and _at(damaged_day, 10, 25) <= item.at <= _at(damaged_day, 10, 29)
        )
    )
    report = build_prospective_scientific_research(
        damaged,
        dataset_fingerprint="sha256:damaged",
        request=ProspectiveScientificRequest(
            selected_hypotheses=(ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,),
            policy=_policy(),
            market_universe=("AAA", "BBB", "CCC", "DDD"),
            pair_candidates=(("PAIR", "PAIRP"),),
        ),
    )

    feature = next(
        item
        for item in report.features
        if item.ticker == "AAA" and item.observed_at == _at(damaged_day, 10, 30)
    )
    assert feature.decision is ProspectiveDecision.ABSTAIN
    assert feature.reason is ProspectiveReason.BASKET_COVERAGE_BELOW_MINIMUM


def test_pair_v2_abstains_for_unstable_past_relationship() -> None:
    observed_at = _at(date(2026, 7, 24), 12, 0)
    policy = ProspectiveScientificPolicy(
        pair_min_training_points=4,
        pair_min_correlation=0.70,
    )
    parameters = FrozenPairParameters(
        left_ticker="SBER",
        right_ticker="SBERP",
        intercept=0.0,
        hedge_ratio=1.0,
        spread_mean=0.0,
        spread_std=0.01,
        correlation=0.20,
        training_points=500,
        trained_until=observed_at - timedelta(days=1),
    )

    feature = pair_residual_reversion_v2_feature(
        left_ticker="SBER",
        right_ticker="SBERP",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        left_price=100.0,
        right_price=95.0,
        parameters=parameters,
        corporate_action_suspected=False,
        liquid=True,
        policy=policy,
        horizon_seconds=900,
    )

    assert feature.decision is ProspectiveDecision.ABSTAIN
    assert feature.reason is ProspectiveReason.PAIR_RELATIONSHIP_UNSTABLE


def test_live_contract_reuses_frozen_h11v2_and_h12v2_formulas() -> None:
    observed_at = _at(date(2026, 7, 24), 12, 0)
    policy = ProspectiveScientificPolicy(
        market_residual_horizons_seconds=(900,),
        pair_horizons_seconds=(900,),
        pair_min_training_points=4,
    )
    market_parameters = FrozenMarketResidualParameters(
        ticker="SBER",
        beta=1.1,
        absolute_residual_threshold_bps=10.0,
        training_points=200,
        trained_until=observed_at - timedelta(days=1),
        basket_members=("GAZP", "LKOH"),
    )
    pair_parameters = FrozenPairParameters(
        left_ticker="SBER",
        right_ticker="SBERP",
        intercept=0.0,
        hedge_ratio=1.0,
        spread_mean=0.0,
        spread_std=0.01,
        correlation=0.90,
        training_points=500,
        trained_until=observed_at - timedelta(days=1),
    )
    use_case = BuildRelativeValueLiveFeatures(policy)

    features = use_case.execute(
        RelativeValueLiveSnapshot(
            trading_day=observed_at.date(),
            observed_at=observed_at,
            market=MarketResidualLiveInput(
                ticker="SBER",
                stock_return_bps=35.0,
                basket_return_bps=5.0,
                basket_coverage=1.0,
                parameters=market_parameters,
            ),
            pair=PairResidualLiveInput(
                left_ticker="SBER",
                right_ticker="SBERP",
                left_price=105.0,
                right_price=100.0,
                parameters=pair_parameters,
            ),
        )
    )

    assert tuple(item.hypothesis for item in features) == (
        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
    )
    with pytest.raises(ValueError, match="precede"):
        use_case.execute(
            RelativeValueLiveSnapshot(
                trading_day=observed_at.date(),
                observed_at=observed_at,
                market=MarketResidualLiveInput(
                    ticker="SBER",
                    stock_return_bps=35.0,
                    basket_return_bps=5.0,
                    basket_coverage=1.0,
                    parameters=FrozenMarketResidualParameters(
                        ticker="SBER",
                        beta=1.1,
                        absolute_residual_threshold_bps=10.0,
                        training_points=200,
                        trained_until=observed_at,
                        basket_members=("GAZP", "LKOH"),
                    ),
                ),
            )
        )

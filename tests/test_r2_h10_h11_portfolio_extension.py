from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from tinvest_signal_engine.adapters.scientific_hypothesis_registry import (
    VersionedScientificRegistry,
)
from tinvest_signal_engine.application.hypothesis_portfolio_runner import (
    EvidenceGatePolicyReference,
    PortfolioHypothesisRegistration,
    RunHypothesisPortfolioRequest,
)
from tinvest_signal_engine.application.prospective_portfolio_extensions import (
    R2ExtensionRequest,
    build_extended_prospective_scientific_research,
    build_partitioned_r2_extension_research,
    build_r2_extension_research,
)
from tinvest_signal_engine.application.prospective_scientific_models import (
    build_prospective_scientific_research,
)
from tinvest_signal_engine.application.scientific_portfolio_versions import (
    ScientificPortfolioVersion,
    build_versioned_portfolio_plan,
    scientific_portfolio_definition,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Decision,
    R2ExtensionHypothesis,
    R2ExtensionPolicy,
    R2Reason,
)
from tinvest_signal_engine.services.hypothesis_portfolio_runtime import (
    build_file_hypothesis_portfolio_runtime,
)


MOSCOW = ZoneInfo("Europe/Moscow")
DATASET = "sha256:" + "d" * 64
REGISTRY = Path("config/scientific_hypotheses/registry-v1.yaml")


def _at(trading_day: date, minute: int) -> datetime:
    return datetime(
        trading_day.year,
        trading_day.month,
        trading_day.day,
        10 + minute // 60,
        minute % 60,
        tzinfo=MOSCOW,
    )


def _candle(ticker: str, at: datetime, start: float, end: float) -> HistoricalCandle:
    return HistoricalCandle(
        ticker=ticker,
        at=at,
        open=start,
        high=max(start, end),
        low=min(start, end),
        close=end,
        volume=1_000.0,
    )


def _research_candles() -> tuple[HistoricalCandle, ...]:
    candles: list[HistoricalCandle] = []
    sber_previous = 100.0
    gazp_previous = 200.0
    start = date(2026, 6, 1)
    for day_index in range(23):
        trading_day = start + timedelta(days=day_index)
        market_gap_bps = (-8.0, -2.0, 4.0, 10.0)[day_index % 4]
        stock_gap_bps = (
            120.0
            if day_index == 22
            else (-20.0 + float((day_index * 7) % 41))
        )
        sber = sber_previous * (1.0 + stock_gap_bps / 10_000.0)
        gazp = gazp_previous * (1.0 + market_gap_bps / 10_000.0)
        market_step = ((day_index % 5) - 2) * 0.00003
        for minute in range(65):
            gazp_next = gazp * (1.0 + market_step)
            residual_step = 0.00001 * (1 if (day_index + minute) % 2 else -1)
            if day_index == 22 and minute < 5:
                residual_step += 0.0015
            sber_next = sber * (1.0 + 1.4 * market_step + residual_step)
            candles.append(_candle("SBER", _at(trading_day, minute), sber, sber_next))
            candles.append(_candle("GAZP", _at(trading_day, minute), gazp, gazp_next))
            sber = sber_next
            gazp = gazp_next
        sber_previous = sber
        gazp_previous = gazp
    return tuple(candles)


def _request(candles: tuple[HistoricalCandle, ...]) -> R2ExtensionRequest:
    days = tuple(sorted({item.at.date() for item in candles}))
    return R2ExtensionRequest(
        market_universe=("GAZP",),
        exchange_schedule_known_days=days,
    )


def test_r2_h10_h11_are_deterministic_causal_and_keep_both_horizons() -> None:
    candles = _research_candles()
    request = _request(candles)
    first = build_r2_extension_research(
        candles, dataset_fingerprint=DATASET, request=request
    )
    repeated = build_r2_extension_research(
        reversed(candles), dataset_fingerprint=DATASET, request=request
    )

    assert first == repeated
    assert first.report_fingerprint == repeated.report_fingerprint
    assert {item.hypothesis for item in first.features} == set(R2ExtensionHypothesis)
    assert {
        item.horizon_seconds
        for item in first.features
        if item.hypothesis is R2ExtensionHypothesis.OPENING_GAP_REVERSION
    } == {1800, 3600}
    assert {
        item.horizon_seconds
        for item in first.features
        if item.hypothesis is R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION
    } == {900, 1800}
    assert all(
        item.feature_source_available_at <= item.available_at
        and (item.history_available_at is None or item.history_available_at < item.event_at)
        and (item.model_available_at is None or item.model_available_at < item.event_at)
        for item in first.features
    )
    assert any(
        item.decision is R2Decision.MATCHED
        and item.hypothesis is R2ExtensionHypothesis.OPENING_GAP_REVERSION
        for item in first.features
    )
    assert any(
        item.decision is R2Decision.MATCHED
        and item.hypothesis is R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION
        for item in first.features
    )


def test_partitioned_r2_is_exactly_equivalent_without_materializing_candles() -> None:
    candles = _research_candles()
    request = _request(candles)
    expected = build_r2_extension_research(
        candles,
        dataset_fingerprint=DATASET,
        request=request,
    )
    partitions = tuple(
        tuple(item for item in candles if item.ticker == ticker)
        for ticker in sorted({item.ticker for item in candles})
    )

    class PartitionedCache:
        def load(self) -> tuple[HistoricalCandle, ...]:
            raise AssertionError("partitioned R2 must not materialize the cache")

        def describe(self) -> object:
            raise AssertionError("builder receives the sealed fingerprint")

        def iter_ticker_partitions(
            self,
        ) -> tuple[tuple[HistoricalCandle, ...], ...]:
            return partitions

    actual = build_partitioned_r2_extension_research(
        PartitionedCache(),
        dataset_fingerprint=DATASET,
        request=request,
    )

    assert actual == expected
    assert actual.report_fingerprint == expected.report_fingerprint

    scoped_request = replace(request, target_universe=("SBER",))
    scoped = build_partitioned_r2_extension_research(
        PartitionedCache(),
        dataset_fingerprint=DATASET,
        request=scoped_request,
    )
    scoped_batch = build_r2_extension_research(
        candles,
        dataset_fingerprint=DATASET,
        request=scoped_request,
    )
    assert scoped == scoped_batch
    assert {item.ticker for item in scoped.features} == {"SBER"}


def test_future_price_change_does_not_change_already_available_features() -> None:
    candles = _research_candles()
    request = _request(candles)
    first = build_r2_extension_research(
        candles, dataset_fingerprint=DATASET, request=request
    )
    latest_day = max(item.at.date() for item in candles)
    cutoff = _at(latest_day, 5)
    before = tuple(
        item
        for item in first.features
        if item.available_at <= cutoff
    )
    assert before

    changed = tuple(
        replace(
            item,
            high=item.high * 1.2,
            close=item.close * 1.2,
        )
        if item.ticker == "SBER" and item.at > cutoff
        else item
        for item in candles
    )
    second = build_r2_extension_research(
        changed,
        dataset_fingerprint="sha256:" + "e" * 64,
        request=request,
    )
    after = tuple(item for item in second.features if item.available_at <= cutoff)
    assert tuple(item.fingerprint for item in before) == tuple(
        item.fingerprint for item in after
    )


def test_explicit_refusals_are_sealed_in_features() -> None:
    candles = _research_candles()
    latest_day = max(item.at.date() for item in candles)
    request = replace(
        _request(candles),
        shortened_session_days=(latest_day,),
        corporate_action_ticker_days=(("SBER", latest_day),),
        trading_gap_ticker_days=(("GAZP", latest_day),),
    )
    report = build_r2_extension_research(
        candles, dataset_fingerprint=DATASET, request=request
    )
    latest = tuple(item for item in report.features if item.trading_day == latest_day)
    assert any(item.reason is R2Reason.SHORTENED_SESSION for item in latest)
    assert any(
        item.reason is R2Reason.CORPORATE_ACTION_SUSPECTED for item in latest
    )
    assert any(item.reason is R2Reason.TRADING_GAP for item in latest)
    assert all(
        item.expected_direction == 0
        for item in latest
        if item.decision is R2Decision.ABSTAIN
    )


def test_observed_regular_open_resolves_schedule_without_future_data() -> None:
    candles = _research_candles()
    report = build_r2_extension_research(
        candles,
        dataset_fingerprint=DATASET,
        request=R2ExtensionRequest(
            selected_hypotheses=(
                R2ExtensionHypothesis.OPENING_GAP_REVERSION,
            ),
            market_universe=("GAZP",),
            observed_exchange_open_is_schedule_evidence=True,
        ),
    )

    assert report.features
    assert all(item.reason is not R2Reason.EXCHANGE_SCHEDULE_UNKNOWN for item in report.features)
    assert any(item.decision is R2Decision.MATCHED for item in report.features)
    assert any(
        feature.decision is R2Decision.NOT_MATCHED and outcome.available
        for feature, outcome in zip(report.features, report.outcomes, strict=True)
    )


def test_h10_uses_first_completed_candle_within_five_minutes_of_horizon() -> None:
    candles = _research_candles()
    latest_day = max(item.at.date() for item in candles)
    with_missing_exact_targets = tuple(
        item
        for item in candles
        if not (
            item.at.date() == latest_day
            and item.ticker == "SBER"
            and (
                item.at.astimezone(MOSCOW).hour,
                item.at.astimezone(MOSCOW).minute,
            )
            in {(10, 30), (11, 0)}
        )
    )
    report = build_r2_extension_research(
        with_missing_exact_targets,
        dataset_fingerprint=DATASET,
        request=R2ExtensionRequest(
            selected_hypotheses=(
                R2ExtensionHypothesis.OPENING_GAP_REVERSION,
            ),
            market_universe=("GAZP",),
            target_universe=("SBER",),
            observed_exchange_open_is_schedule_evidence=True,
        ),
    )
    latest = tuple(
        (feature, outcome)
        for feature, outcome in zip(report.features, report.outcomes, strict=True)
        if feature.trading_day == latest_day
        and feature.decision is not R2Decision.ABSTAIN
    )

    assert len(latest) == 2
    assert all(outcome.available for _, outcome in latest)
    assert all(
        outcome.available_at == outcome.target_at + timedelta(minutes=1)
        for _, outcome in latest
    )


def test_sealed_eleven_report_is_unchanged_by_opt_in_extension() -> None:
    candles = _research_candles()
    sealed = build_prospective_scientific_research(
        candles, dataset_fingerprint=DATASET
    )
    combined = build_extended_prospective_scientific_research(
        candles,
        dataset_fingerprint=DATASET,
        extension_request=_request(candles),
    )
    assert combined.sealed_report == sealed
    assert combined.sealed_report.report_fingerprint == sealed.report_fingerprint
    assert len(combined.sealed_report.selected_hypotheses) == 11
    assert combined.extension_report.portfolio_version == "r2-h10-h11-v1.0.1"


def test_r2_policy_matches_sealed_registry_contract() -> None:
    registry = VersionedScientificRegistry.from_file(REGISTRY)
    policy = R2ExtensionPolicy()
    h10 = registry.get_hypothesis("h10-positive-main-open-gap-reversion", "1.0.0")
    h11 = registry.get_hypothesis("h11-residual-move-reversion", "1.0.0")
    assert h10 is not None and h10.preregistration is not None
    assert h11 is not None and h11.preregistration is not None
    assert h10.horizon_seconds == policy.opening_gap_horizons_seconds
    assert h11.horizon_seconds == policy.residual_horizons_seconds
    assert h10.preregistration.cost_model_version == policy.cost_model_version
    assert h11.preregistration.cost_model_version == policy.cost_model_version
    h10_thresholds = dict(h10.preregistration.thresholds)
    h11_thresholds = dict(h11.preregistration.thresholds)
    assert float(h10_thresholds["positive_main_open_gap_z_min"]) == (
        policy.opening_gap_z_min
    )
    assert float(h10_thresholds["market_gap_z_abstention_min"]) == (
        policy.market_gap_z_abstention_min
    )
    assert int(h11_thresholds["beta_lookback_trading_days"]) == (
        policy.residual_beta_lookback_days
    )
    assert float(h11_thresholds["basket_coverage_min"]) == (
        policy.residual_basket_coverage_min
    )
    assert float(h11_thresholds["absolute_residual_return_5m_percentile_min"]) == (
        policy.residual_percentile_min
    )


def test_local_runner_requires_explicit_extended_version_and_fingerprint() -> None:
    sealed = scientific_portfolio_definition(
        ScientificPortfolioVersion.SEALED_ELEVEN_V1
    )
    extended = scientific_portfolio_definition(
        ScientificPortfolioVersion.EXTENDED_H10_H11_V1
    )
    assert len(sealed.hypothesis_ids) == 11
    assert len(extended.hypothesis_ids) == 13
    assert extended.hypothesis_ids[:11] == sealed.hypothesis_ids
    assert extended.hypothesis_ids[-2:] == (
        "h10-positive-main-open-gap-reversion",
        "h11-residual-move-reversion",
    )
    assert sealed.fingerprint != extended.fingerprint

    registrations = _portfolio_registrations()
    plan = build_versioned_portfolio_plan(
        version=ScientificPortfolioVersion.EXTENDED_H10_H11_V1,
        registrations=registrations,
        dataset_fingerprint=DATASET,
        replay_engine_version="r2-replay-v1",
    )
    assert len(plan.requests) == 2
    assert sum(len(item.hypotheses) for item in plan.requests) == 13
    assert all(
        item.portfolio_definition_fingerprint == extended.fingerprint
        for item in plan.requests
    )
    assert plan.fingerprint.startswith("sha256:")

    request = next(item for item in plan.requests if item.cost_model_version == "1.0.0")
    legacy = RunHypothesisPortfolioRequest(
        dataset_fingerprint=DATASET,
        cost_model_version="1.0.0",
        replay_engine_version="r2-replay-v1",
        hypotheses=request.hypotheses,
    )
    assert legacy.input_fingerprint != request.input_fingerprint
    assert replace(legacy).input_fingerprint == legacy.input_fingerprint
    assert legacy.input_fingerprint == (
        "sha256:86d99d2e9d19383ca573eccfe2cf68834d123b65e86726de9948da4775e61a8e"
    )


def test_local_file_runtime_defaults_to_sealed_and_accepts_only_explicit_opt_in(
    tmp_path: Path,
) -> None:
    default = build_file_hypothesis_portfolio_runtime(
        state_dir=tmp_path / "default",
        replay=object(),  # type: ignore[arg-type]
        evidence_gates=object(),  # type: ignore[arg-type]
    )
    extended = build_file_hypothesis_portfolio_runtime(
        state_dir=tmp_path / "extended",
        replay=object(),  # type: ignore[arg-type]
        evidence_gates=object(),  # type: ignore[arg-type]
        portfolio_version=ScientificPortfolioVersion.EXTENDED_H10_H11_V1,
    )
    assert default.portfolio_version is ScientificPortfolioVersion.SEALED_ELEVEN_V1
    assert (
        extended.portfolio_version
        is ScientificPortfolioVersion.EXTENDED_H10_H11_V1
    )


def _portfolio_registrations() -> tuple[PortfolioHypothesisRegistration, ...]:
    registry = VersionedScientificRegistry.from_file(REGISTRY)
    versions = {
        "h1-morning-low-volume-reversion": "1.0.0",
        "h2-morning-high-volume-continuation": "1.0.0",
        "h3-jump-low-activity-reversal": "2.0.0",
        "h4-jump-high-activity-continuation": "2.0.0",
        "h5-same-phase-return-recurrence": "1.0.0",
        "h6-open-close-market-continuation": "1.0.0",
        "h7-relative-volume-future-activity": "3.0.0",
        "h10-positive-main-open-gap-reversion": "1.0.0",
        "h11-residual-move-reversion": "1.0.0",
        "h12-pair-residual-reversion": "1.0.0",
        "h15-multi-window-volatility-forecast": "2.0.0",
        "h16-negative-semivariance-future-risk": "1.0.0",
        "h17-volatility-jump-persistence": "1.0.0",
    }
    result: list[PortfolioHypothesisRegistration] = []
    for hypothesis_id, version in versions.items():
        hypothesis = registry.get_hypothesis(hypothesis_id, version)
        assert hypothesis is not None and hypothesis.preregistration is not None
        result.append(
            PortfolioHypothesisRegistration(
                replay_key=hypothesis_id,
                hypothesis=hypothesis,
                family_id="scientific-r2",
                primary_metric="cost_adjusted_return_bps",
                primary_horizon_seconds=(
                    hypothesis.preregistration.horizon_seconds[0]
                ),
                intermediate_gate=EvidenceGatePolicyReference(
                    policy_id="intermediate",
                    version="1.0.0",
                    fingerprint="sha256:intermediate",
                ),
                strict_gate=EvidenceGatePolicyReference(
                    policy_id="strict",
                    version="1.0.0",
                    fingerprint="sha256:strict",
                ),
            )
        )
    return tuple(result)

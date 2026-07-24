"""Causal construction of the opt-in H10/H11 R2 research portfolio.

The existing prospective portfolio is deliberately left untouched.  This
module produces a separately versioned report whose features become available
only after their final input candle and whose fitted history ends on a prior
trading day.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from hashlib import sha256
import json
from math import log
from statistics import fmean, pstdev
from typing import Iterable, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
    ProspectiveScientificRequest,
    build_prospective_scientific_research,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    HistoricalCandle,
)
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Decision,
    R2ExtensionHypothesis,
    R2ExtensionPolicy,
    R2Feature,
    R2Metric,
    R2Outcome,
    R2Reason,
    feature_identity,
)


MOSCOW = ZoneInfo("Europe/Moscow")
_MAIN_OPEN = time(10, 0)
_MAIN_CLOSE = time(18, 40)


@dataclass(frozen=True, slots=True)
class R2ExtensionRequest:
    """Explicit inputs whose truth cannot be inferred safely from candles."""

    policy: R2ExtensionPolicy = R2ExtensionPolicy()
    selected_hypotheses: tuple[R2ExtensionHypothesis, ...] = tuple(
        R2ExtensionHypothesis
    )
    market_universe: tuple[str, ...] = (
        "SBER",
        "GAZP",
        "LKOH",
        "YDEX",
        "T",
        "ROSN",
        "NVTK",
        "GMKN",
        "MOEX",
        "TATN",
    )
    target_universe: tuple[str, ...] = ()
    exchange_schedule_known_days: tuple[date, ...] = ()
    shortened_session_days: tuple[date, ...] = ()
    corporate_action_ticker_days: tuple[tuple[str, date], ...] = ()
    trading_gap_ticker_days: tuple[tuple[str, date], ...] = ()

    def __post_init__(self) -> None:
        if not self.selected_hypotheses:
            raise ValueError("R2 extension requires at least one hypothesis")
        if len(set(self.selected_hypotheses)) != len(self.selected_hypotheses):
            raise ValueError("R2 extension hypotheses must be unique")
        if not self.market_universe or any(
            not ticker.strip() for ticker in self.market_universe
        ):
            raise ValueError("R2 market universe must contain named tickers")
        if len(set(self.market_universe)) != len(self.market_universe):
            raise ValueError("R2 market universe must be unique")
        if any(not ticker.strip() for ticker in self.target_universe):
            raise ValueError("R2 target universe must contain named tickers")
        if len(set(self.target_universe)) != len(self.target_universe):
            raise ValueError("R2 target universe must be unique")
        _require_unique(self.exchange_schedule_known_days, "known schedule days")
        _require_unique(self.shortened_session_days, "shortened session days")
        _require_unique(
            self.corporate_action_ticker_days, "corporate action ticker-days"
        )
        _require_unique(self.trading_gap_ticker_days, "trading-gap ticker-days")

    @property
    def fingerprint(self) -> str:
        payload = {
            "corporate_action_ticker_days": _ticker_day_payload(
                self.corporate_action_ticker_days
            ),
            "exchange_schedule_known_days": tuple(
                item.isoformat() for item in sorted(self.exchange_schedule_known_days)
            ),
            "market_universe": self.market_universe,
            "policy_fingerprint": self.policy.fingerprint,
            "selected_hypotheses": tuple(
                (item.value, item.version) for item in self.selected_hypotheses
            ),
            "shortened_session_days": tuple(
                item.isoformat() for item in sorted(self.shortened_session_days)
            ),
            "trading_gap_ticker_days": _ticker_day_payload(
                self.trading_gap_ticker_days
            ),
        }
        if self.target_universe:
            payload["target_universe"] = self.target_universe
        return _fingerprint(payload)


@dataclass(frozen=True, slots=True)
class R2ExtensionReport:
    portfolio_version: str
    dataset_fingerprint: str
    request_fingerprint: str
    report_fingerprint: str
    features: tuple[R2Feature, ...]
    outcomes: tuple[R2Outcome, ...]

    def __post_init__(self) -> None:
        identities = tuple(item.observation_id for item in self.features)
        if not self.portfolio_version.strip():
            raise ValueError("R2 portfolio version must not be empty")
        if len(identities) != len(set(identities)):
            raise ValueError("R2 feature identities must be unique")
        if len(self.features) != len(self.outcomes):
            raise ValueError("R2 features and outcomes must remain aligned")
        if any(
            feature.observation_id != outcome.observation_id
            for feature, outcome in zip(self.features, self.outcomes, strict=True)
        ):
            raise ValueError("R2 feature and outcome identities must remain aligned")
        for value in (
            self.dataset_fingerprint,
            self.request_fingerprint,
            self.report_fingerprint,
        ):
            if not value.startswith("sha256:"):
                raise ValueError("R2 report fingerprints must use sha256")


@dataclass(frozen=True, slots=True)
class ExtendedProspectiveScientificReport:
    """Envelope proving that R2 is additive to the unchanged sealed report."""

    sealed_report: ProspectiveScientificReport
    extension_report: R2ExtensionReport


class R2ExtensionCandleCachePort(Protocol):
    """Application-owned boundary for immutable one-minute candle history."""

    def describe(self) -> CandleCacheDescriptor: ...

    def load(self) -> tuple[HistoricalCandle, ...]: ...


class PartitionedR2ExtensionCandleCachePort(R2ExtensionCandleCachePort, Protocol):
    """Repeatable ticker-bounded source for the memory-safe R2 replay."""

    def iter_ticker_partitions(
        self,
    ) -> Iterable[tuple[HistoricalCandle, ...]]: ...


class BuildR2ExtensionReplay:
    """Build the causal H10/H11 extension from an injected local cache.

    Evidence assessment and transport serialization deliberately remain
    outside this use case.  A composition root may only publish the resulting
    observations after a separate evidence gate has assessed them.
    """

    def __init__(self, candle_cache: R2ExtensionCandleCachePort) -> None:
        self._candle_cache = candle_cache

    def execute(self, request: R2ExtensionRequest) -> R2ExtensionReport:
        descriptor = self._candle_cache.describe()
        partition_reader = getattr(
            self._candle_cache,
            "iter_ticker_partitions",
            None,
        )
        if callable(partition_reader):
            return build_partitioned_r2_extension_research(
                self._candle_cache,  # type: ignore[arg-type]
                dataset_fingerprint=descriptor.dataset_fingerprint,
                request=request,
            )
        return build_r2_extension_research(
            self._candle_cache.load(),
            dataset_fingerprint=descriptor.dataset_fingerprint,
            request=request,
        )


def build_r2_extension_research(
    candles: Iterable[HistoricalCandle],
    *,
    dataset_fingerprint: str,
    request: R2ExtensionRequest | None = None,
) -> R2ExtensionReport:
    """Build deterministic H10/H11 features from complete one-minute candles."""

    request = request or R2ExtensionRequest()
    if not dataset_fingerprint.startswith("sha256:"):
        raise ValueError("dataset_fingerprint must use sha256")
    complete = tuple(
        sorted(
            (item for item in candles if item.complete),
            key=lambda item: (item.ticker, item.at),
        )
    )
    if not complete:
        raise ValueError("R2 extension research requires complete candles")
    if len({(item.ticker, item.at) for item in complete}) != len(complete):
        raise ValueError("R2 extension candles must be unique")
    by_ticker: defaultdict[str, list[HistoricalCandle]] = defaultdict(list)
    for candle in complete:
        by_ticker[candle.ticker].append(candle)
    ordered = {ticker: tuple(rows) for ticker, rows in sorted(by_ticker.items())}

    rows: list[tuple[R2Feature, R2Outcome]] = []
    selected = frozenset(request.selected_hypotheses)
    if R2ExtensionHypothesis.OPENING_GAP_REVERSION in selected:
        rows.extend(_opening_gap_rows(ordered, request))
    if R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION in selected:
        rows.extend(_residual_rows(ordered, request))
    rows.sort(
        key=lambda item: (
            item[0].available_at,
            item[0].ticker,
            item[0].hypothesis.value,
            item[0].horizon_seconds,
        )
    )
    features = tuple(item[0] for item in rows)
    outcomes = tuple(item[1] for item in rows)
    fingerprint = _fingerprint(
        {
            "dataset_fingerprint": dataset_fingerprint,
            "features": tuple(item.fingerprint for item in features),
            "outcomes": tuple(item.fingerprint for item in outcomes),
            "portfolio_version": request.policy.version,
            "request_fingerprint": request.fingerprint,
        }
    )
    return R2ExtensionReport(
        portfolio_version=request.policy.version,
        dataset_fingerprint=dataset_fingerprint,
        request_fingerprint=request.fingerprint,
        report_fingerprint=fingerprint,
        features=features,
        outcomes=outcomes,
    )


def build_partitioned_r2_extension_research(
    cache: PartitionedR2ExtensionCandleCachePort,
    *,
    dataset_fingerprint: str,
    request: R2ExtensionRequest,
) -> R2ExtensionReport:
    """Build byte-equivalent R2 results while retaining one ticker at a time.

    The fixed basket is reduced to daily and five-minute returns in the first
    pass.  The second pass evaluates each ticker against those compact market
    summaries.  No global candle graph is constructed, and every input remains
    causal and identical to the materialised formula path.
    """

    if not dataset_fingerprint.startswith("sha256:"):
        raise ValueError("dataset_fingerprint must use sha256")
    selected = frozenset(request.selected_hypotheses)
    requested_universe = frozenset(request.market_universe)
    present_universe: set[str] = set()
    universe_daily: dict[str, dict[date, float]] = {}
    universe_windows: dict[str, dict[tuple[date, time], float]] = {}
    universe_closes: dict[str, dict[date, datetime]] = {}
    market_gap_values: defaultdict[date, list[float]] = defaultdict(list)
    seen_first_pass: set[str] = set()

    for raw_partition in cache.iter_ticker_partitions():
        validated = _validated_ticker_partition(
            raw_partition,
            seen_first_pass,
        )
        if validated is None:
            continue
        ticker, candles = validated
        if ticker not in requested_universe:
            continue
        present_universe.add(ticker)
        by_day = _group_by_day(candles)
        universe_daily[ticker] = {
            trading_day: _return_bps(rows[0].open, rows[-1].close)
            for trading_day, rows in by_day.items()
        }
        universe_windows[ticker] = {
            (
                trading_day,
                window[0].at.astimezone(MOSCOW).time(),
            ): _return_bps(window[0].open, window[-1].close)
            for trading_day, rows in by_day.items()
            for window in _complete_windows(
                rows,
                request.policy.residual_window_minutes,
            )
        }
        universe_closes[ticker] = {
            trading_day: _candle_available_at(rows[-1])
            for trading_day, rows in by_day.items()
        }
        if R2ExtensionHypothesis.OPENING_GAP_REVERSION in selected:
            for candidate in _opening_candidates_for_ticker(
                ticker,
                candles,
                request,
            ):
                if candidate.gap_z is not None:
                    market_gap_values[candidate.trading_day].append(candidate.gap_z)

    if not seen_first_pass:
        raise ValueError("R2 extension research requires complete candles")
    universe = tuple(
        ticker for ticker in request.market_universe if ticker in present_universe
    )
    basket_daily = _basket_daily_returns(universe_daily, universe)
    basket_windows = _basket_window_returns_from_compact(
        universe_windows,
        universe,
    )
    market_gap_z = {
        trading_day: fmean(values)
        for trading_day, values in market_gap_values.items()
        if values
    }

    feature_outcomes: list[tuple[R2Feature, R2Outcome]] = []
    seen_second_pass: set[str] = set()
    target_universe = frozenset(request.target_universe)
    for raw_partition in cache.iter_ticker_partitions():
        validated = _validated_ticker_partition(
            raw_partition,
            seen_second_pass,
        )
        if validated is None:
            continue
        ticker, candles = validated
        if target_universe and ticker not in target_universe:
            continue
        if R2ExtensionHypothesis.OPENING_GAP_REVERSION in selected:
            feature_outcomes.extend(
                _partitioned_opening_gap_rows(
                    ticker,
                    candles,
                    market_gap_z=market_gap_z,
                    request=request,
                )
            )
        if R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION in selected:
            feature_outcomes.extend(
                _partitioned_residual_rows(
                    ticker,
                    candles,
                    basket_daily=basket_daily,
                    basket_windows=basket_windows,
                    universe_closes=universe_closes,
                    universe=universe,
                    request=request,
                )
            )

    feature_outcomes.sort(
        key=lambda item: (
            item[0].available_at,
            item[0].ticker,
            item[0].hypothesis.value,
            item[0].horizon_seconds,
        )
    )
    features = tuple(item[0] for item in feature_outcomes)
    outcomes = tuple(item[1] for item in feature_outcomes)
    fingerprint = _fingerprint(
        {
            "dataset_fingerprint": dataset_fingerprint,
            "features": tuple(item.fingerprint for item in features),
            "outcomes": tuple(item.fingerprint for item in outcomes),
            "portfolio_version": request.policy.version,
            "request_fingerprint": request.fingerprint,
        }
    )
    return R2ExtensionReport(
        portfolio_version=request.policy.version,
        dataset_fingerprint=dataset_fingerprint,
        request_fingerprint=request.fingerprint,
        report_fingerprint=fingerprint,
        features=features,
        outcomes=outcomes,
    )


def build_extended_prospective_scientific_research(
    candles: Iterable[HistoricalCandle],
    *,
    dataset_fingerprint: str,
    sealed_request: ProspectiveScientificRequest | None = None,
    extension_request: R2ExtensionRequest | None = None,
) -> ExtendedProspectiveScientificReport:
    """Build the sealed eleven and opt-in R2 without combining fingerprints."""

    materialized = tuple(candles)
    sealed_request = sealed_request or ProspectiveScientificRequest()
    extension_request = extension_request or R2ExtensionRequest()
    return ExtendedProspectiveScientificReport(
        sealed_report=build_prospective_scientific_research(
            materialized,
            dataset_fingerprint=dataset_fingerprint,
            request=sealed_request,
        ),
        extension_report=build_r2_extension_research(
            materialized,
            dataset_fingerprint=dataset_fingerprint,
            request=extension_request,
        ),
    )


def _opening_gap_rows(
    by_ticker: Mapping[str, tuple[HistoricalCandle, ...]],
    request: R2ExtensionRequest,
) -> list[tuple[R2Feature, R2Outcome]]:
    policy = request.policy
    candidates_by_day: defaultdict[date, list[_OpeningGapCandidate]] = defaultdict(list)
    for ticker, candles in by_ticker.items():
        for candidate in _opening_candidates_for_ticker(ticker, candles, request):
            candidates_by_day[candidate.trading_day].append(candidate)

    result: list[tuple[R2Feature, R2Outcome]] = []
    universe = frozenset(request.market_universe)
    target_universe = frozenset(request.target_universe)
    for trading_day, candidates in sorted(candidates_by_day.items()):
        market_z_values = tuple(
            item.gap_z
            for item in candidates
            if item.ticker in universe and item.gap_z is not None
        )
        market_gap_z = fmean(market_z_values) if market_z_values else 0.0
        for candidate in candidates:
            if target_universe and candidate.ticker not in target_universe:
                continue
            for horizon in policy.opening_gap_horizons_seconds:
                decision, reason = _opening_decision(
                    candidate, market_gap_z, request
                )
                feature = _opening_feature(
                    candidate,
                    horizon=horizon,
                    market_gap_z=market_gap_z,
                    decision=decision,
                    reason=reason,
                    policy=policy,
                )
                result.append(
                    (
                        feature,
                        _directional_outcome(
                            feature,
                            anchor_price=candidate.opening.close,
                            candles=candidate.candles,
                            policy=policy,
                        ),
                    )
                )
    return result


def _opening_candidates_for_ticker(
    ticker: str,
    candles: tuple[HistoricalCandle, ...],
    request: R2ExtensionRequest,
) -> tuple[_OpeningGapCandidate, ...]:
    policy = request.policy
    by_day = _group_by_day(candles)
    raw_history: list[tuple[date, float, datetime]] = []
    previous_close: HistoricalCandle | None = None
    candidates: list[_OpeningGapCandidate] = []
    known_days = frozenset(request.exchange_schedule_known_days)
    shortened_days = frozenset(request.shortened_session_days)
    corporate_actions = frozenset(request.corporate_action_ticker_days)
    for trading_day, current in by_day.items():
        opening = next(
            (
                item
                for item in current
                if item.at.astimezone(MOSCOW).time() == _MAIN_OPEN
            ),
            None,
        )
        if opening is None:
            previous_close = current[-1]
            continue
        available_at = _candle_available_at(opening)
        history = tuple(raw_history[-policy.opening_gap_history_days :])
        gap_bps = (
            _return_bps(previous_close.close, opening.close)
            if previous_close is not None
            else None
        )
        candidates.append(
            _OpeningGapCandidate(
                ticker=ticker,
                trading_day=trading_day,
                opening=opening,
                available_at=available_at,
                previous_close=previous_close,
                gap_bps=gap_bps,
                gap_z=_z_score(gap_bps, tuple(item[1] for item in history)),
                history=history,
                candles=current,
            )
        )
        if (
            gap_bps is not None
            and trading_day in known_days
            and trading_day not in shortened_days
            and (ticker, trading_day) not in corporate_actions
        ):
            raw_history.append((trading_day, gap_bps, available_at))
        previous_close = current[-1]
    return tuple(candidates)


def _partitioned_opening_gap_rows(
    ticker: str,
    candles: tuple[HistoricalCandle, ...],
    *,
    market_gap_z: Mapping[date, float],
    request: R2ExtensionRequest,
) -> list[tuple[R2Feature, R2Outcome]]:
    result: list[tuple[R2Feature, R2Outcome]] = []
    for candidate in _opening_candidates_for_ticker(ticker, candles, request):
        market_value = market_gap_z.get(candidate.trading_day, 0.0)
        decision, reason = _opening_decision(candidate, market_value, request)
        for horizon in request.policy.opening_gap_horizons_seconds:
            feature = _opening_feature(
                candidate,
                horizon=horizon,
                market_gap_z=market_value,
                decision=decision,
                reason=reason,
                policy=request.policy,
            )
            result.append(
                (
                    feature,
                    _directional_outcome(
                        feature,
                        anchor_price=candidate.opening.close,
                        candles=candidate.candles,
                        policy=request.policy,
                    ),
                )
            )
    return result


def _opening_decision(
    candidate: _OpeningGapCandidate,
    market_gap_z: float,
    request: R2ExtensionRequest,
) -> tuple[R2Decision, R2Reason]:
    day_key = (candidate.ticker, candidate.trading_day)
    if candidate.trading_day not in frozenset(request.exchange_schedule_known_days):
        return R2Decision.ABSTAIN, R2Reason.EXCHANGE_SCHEDULE_UNKNOWN
    if candidate.trading_day in frozenset(request.shortened_session_days):
        return R2Decision.ABSTAIN, R2Reason.SHORTENED_SESSION
    if day_key in frozenset(request.corporate_action_ticker_days):
        return R2Decision.ABSTAIN, R2Reason.CORPORATE_ACTION_SUSPECTED
    if candidate.previous_close is None:
        return R2Decision.ABSTAIN, R2Reason.MISSING_PREVIOUS_CLOSE
    if len(candidate.history) < request.policy.opening_gap_history_days:
        return R2Decision.ABSTAIN, R2Reason.INSUFFICIENT_HISTORY
    if candidate.gap_z is None:
        return R2Decision.ABSTAIN, R2Reason.INSUFFICIENT_HISTORY
    if market_gap_z >= request.policy.market_gap_z_abstention_min:
        return R2Decision.ABSTAIN, R2Reason.MARKET_WIDE_POSITIVE_GAP
    if candidate.gap_z >= request.policy.opening_gap_z_min:
        return R2Decision.MATCHED, R2Reason.CONDITIONS_MATCHED
    return R2Decision.NOT_MATCHED, R2Reason.CONDITIONS_NOT_MET


def _opening_feature(
    candidate: _OpeningGapCandidate,
    *,
    horizon: int,
    market_gap_z: float,
    decision: R2Decision,
    reason: R2Reason,
    policy: R2ExtensionPolicy,
) -> R2Feature:
    history_at = candidate.history[-1][2] if candidate.history else None
    observation_id = feature_identity(
        hypothesis=R2ExtensionHypothesis.OPENING_GAP_REVERSION,
        ticker=candidate.ticker,
        available_at=candidate.available_at,
        horizon_seconds=horizon,
        policy_fingerprint=policy.fingerprint,
    )
    return R2Feature(
        observation_id=observation_id,
        hypothesis=R2ExtensionHypothesis.OPENING_GAP_REVERSION,
        ticker=candidate.ticker,
        trading_day=candidate.trading_day,
        event_at=candidate.opening.at,
        available_at=candidate.available_at,
        feature_source_available_at=candidate.available_at,
        history_available_at=history_at,
        model_available_at=history_at,
        horizon_seconds=horizon,
        decision=decision,
        reason=reason,
        expected_direction=-1 if decision is R2Decision.MATCHED else 0,
        values=tuple(
            R2Metric(name, value)
            for name, value in (
                ("opening_gap_bps", candidate.gap_bps or 0.0),
                ("opening_gap_z", candidate.gap_z or 0.0),
                ("market_gap_z", market_gap_z),
                ("history_days", float(len(candidate.history))),
            )
        ),
    )


def _residual_rows(
    by_ticker: Mapping[str, tuple[HistoricalCandle, ...]],
    request: R2ExtensionRequest,
) -> list[tuple[R2Feature, R2Outcome]]:
    policy = request.policy
    universe = tuple(
        ticker for ticker in request.market_universe if ticker in by_ticker
    )
    daily_returns = _daily_returns(by_ticker)
    basket_daily = _basket_daily_returns(daily_returns, universe)
    by_ticker_day = {
        ticker: _group_by_day(candles) for ticker, candles in by_ticker.items()
    }
    basket_windows = _basket_window_returns(
        by_ticker_day,
        universe,
        policy.residual_window_minutes,
    )
    result: list[tuple[R2Feature, R2Outcome]] = []
    target_universe = frozenset(request.target_universe)
    for ticker, daily in sorted(by_ticker_day.items()):
        if target_universe and ticker not in target_universe:
            continue
        days = tuple(sorted(daily))
        for trading_day in days:
            beta_history = _prior_daily_pairs(
                trading_day,
                daily_returns.get(ticker, {}),
                basket_daily,
                policy.residual_beta_lookback_days,
                excluded_ticker_days=(
                    frozenset(request.corporate_action_ticker_days)
                    | frozenset(request.trading_gap_ticker_days)
                ),
                ticker=ticker,
            )
            beta = _beta(beta_history)
            history_at = _basket_model_available_at(
                beta_history[-1][0] if beta_history else None,
                by_ticker_day,
                universe,
            )
            historical_residuals = _prior_residuals(
                ticker=ticker,
                before_day=trading_day,
                by_ticker_day=by_ticker_day,
                basket_windows=basket_windows,
                beta=beta,
                window_minutes=policy.residual_window_minutes,
                excluded_ticker_days=(
                    frozenset(request.corporate_action_ticker_days)
                    | frozenset(request.trading_gap_ticker_days)
                ),
            )
            for window in _complete_windows(
                daily[trading_day], policy.residual_window_minutes
            ):
                stock_return = _return_bps(window[0].open, window[-1].close)
                key = (trading_day, window[0].at.astimezone(MOSCOW).time())
                market_return, coverage = basket_windows.get(key, (0.0, 0.0))
                residual = (
                    stock_return - beta * market_return if beta is not None else 0.0
                )
                percentile = _absolute_percentile(residual, historical_residuals)
                available_at = _candle_available_at(window[-1])
                decision, reason = _residual_decision(
                    ticker=ticker,
                    trading_day=trading_day,
                    beta=beta,
                    coverage=coverage,
                    residual=residual,
                    market_return=market_return,
                    percentile=percentile,
                    beta_history_days=len(beta_history),
                    request=request,
                )
                for horizon in policy.residual_horizons_seconds:
                    observation_id = feature_identity(
                        hypothesis=R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION,
                        ticker=ticker,
                        available_at=available_at,
                        horizon_seconds=horizon,
                        policy_fingerprint=policy.fingerprint,
                    )
                    feature = R2Feature(
                        observation_id=observation_id,
                        hypothesis=(
                            R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION
                        ),
                        ticker=ticker,
                        trading_day=trading_day,
                        event_at=available_at,
                        available_at=available_at,
                        feature_source_available_at=available_at,
                        history_available_at=history_at,
                        model_available_at=history_at,
                        horizon_seconds=horizon,
                        decision=decision,
                        reason=reason,
                        expected_direction=(
                            -1 if residual > 0.0 else 1
                        )
                        if decision is R2Decision.MATCHED
                        else 0,
                        values=tuple(
                            R2Metric(name, value)
                            for name, value in (
                                ("stock_return_5m_bps", stock_return),
                                ("market_return_5m_bps", market_return),
                                ("market_beta", beta or 0.0),
                                ("residual_return_5m_bps", residual),
                                ("absolute_residual_percentile", percentile),
                                ("basket_coverage", coverage),
                                ("beta_history_days", float(len(beta_history))),
                            )
                        ),
                    )
                    result.append(
                        (
                            feature,
                            _directional_outcome(
                                feature,
                                anchor_price=window[-1].close,
                                candles=daily[trading_day],
                                policy=policy,
                            ),
                        )
                    )
    return result


def _partitioned_residual_rows(
    ticker: str,
    candles: tuple[HistoricalCandle, ...],
    *,
    basket_daily: Mapping[date, float],
    basket_windows: Mapping[tuple[date, time], tuple[float, float]],
    universe_closes: Mapping[str, Mapping[date, datetime]],
    universe: Sequence[str],
    request: R2ExtensionRequest,
) -> list[tuple[R2Feature, R2Outcome]]:
    policy = request.policy
    daily = _group_by_day(candles)
    daily_returns = {
        trading_day: _return_bps(rows[0].open, rows[-1].close)
        for trading_day, rows in daily.items()
    }
    compact_windows = {
        trading_day: tuple(
            (
                window[0].at.astimezone(MOSCOW).time(),
                _return_bps(window[0].open, window[-1].close),
            )
            for window in _complete_windows(
                rows,
                policy.residual_window_minutes,
            )
        )
        for trading_day, rows in daily.items()
    }
    excluded = (
        frozenset(request.corporate_action_ticker_days)
        | frozenset(request.trading_gap_ticker_days)
    )
    result: list[tuple[R2Feature, R2Outcome]] = []
    for trading_day, day_candles in daily.items():
        beta_history = _prior_daily_pairs(
            trading_day,
            daily_returns,
            basket_daily,
            policy.residual_beta_lookback_days,
            excluded_ticker_days=excluded,
            ticker=ticker,
        )
        beta = _beta(beta_history)
        history_at = _compact_basket_model_available_at(
            beta_history[-1][0] if beta_history else None,
            universe_closes,
            universe,
        )
        historical_residuals = _prior_compact_residuals(
            ticker=ticker,
            before_day=trading_day,
            windows_by_day=compact_windows,
            basket_windows=basket_windows,
            beta=beta,
            excluded_ticker_days=excluded,
        )
        for window in _complete_windows(
            day_candles,
            policy.residual_window_minutes,
        ):
            stock_return = _return_bps(window[0].open, window[-1].close)
            key = (trading_day, window[0].at.astimezone(MOSCOW).time())
            market_return, coverage = basket_windows.get(key, (0.0, 0.0))
            residual = (
                stock_return - beta * market_return if beta is not None else 0.0
            )
            percentile = _absolute_percentile(residual, historical_residuals)
            available_at = _candle_available_at(window[-1])
            decision, reason = _residual_decision(
                ticker=ticker,
                trading_day=trading_day,
                beta=beta,
                coverage=coverage,
                residual=residual,
                market_return=market_return,
                percentile=percentile,
                beta_history_days=len(beta_history),
                request=request,
            )
            for horizon in policy.residual_horizons_seconds:
                observation_id = feature_identity(
                    hypothesis=R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION,
                    ticker=ticker,
                    available_at=available_at,
                    horizon_seconds=horizon,
                    policy_fingerprint=policy.fingerprint,
                )
                feature = R2Feature(
                    observation_id=observation_id,
                    hypothesis=R2ExtensionHypothesis.MARKET_RESIDUAL_REVERSION,
                    ticker=ticker,
                    trading_day=trading_day,
                    event_at=available_at,
                    available_at=available_at,
                    feature_source_available_at=available_at,
                    history_available_at=history_at,
                    model_available_at=history_at,
                    horizon_seconds=horizon,
                    decision=decision,
                    reason=reason,
                    expected_direction=(
                        -1 if residual > 0.0 else 1
                    )
                    if decision is R2Decision.MATCHED
                    else 0,
                    values=tuple(
                        R2Metric(name, value)
                        for name, value in (
                            ("stock_return_5m_bps", stock_return),
                            ("market_return_5m_bps", market_return),
                            ("market_beta", beta or 0.0),
                            ("residual_return_5m_bps", residual),
                            ("absolute_residual_percentile", percentile),
                            ("basket_coverage", coverage),
                            ("beta_history_days", float(len(beta_history))),
                        )
                    ),
                )
                result.append(
                    (
                        feature,
                        _directional_outcome(
                            feature,
                            anchor_price=window[-1].close,
                            candles=day_candles,
                            policy=policy,
                        ),
                    )
                )
    return result


def _residual_decision(
    *,
    ticker: str,
    trading_day: date,
    beta: float | None,
    coverage: float,
    residual: float,
    market_return: float,
    percentile: float,
    beta_history_days: int,
    request: R2ExtensionRequest,
) -> tuple[R2Decision, R2Reason]:
    ticker_day = (ticker, trading_day)
    if ticker_day in frozenset(request.corporate_action_ticker_days):
        return R2Decision.ABSTAIN, R2Reason.CORPORATE_ACTION_SUSPECTED
    if ticker_day in frozenset(request.trading_gap_ticker_days):
        return R2Decision.ABSTAIN, R2Reason.TRADING_GAP
    if (
        beta is None
        or beta_history_days < request.policy.residual_beta_lookback_days
    ):
        return R2Decision.ABSTAIN, R2Reason.MARKET_BETA_UNAVAILABLE
    if coverage < request.policy.residual_basket_coverage_min:
        return R2Decision.ABSTAIN, R2Reason.BASKET_COVERAGE_BELOW_MINIMUM
    if residual == 0.0:
        return R2Decision.ABSTAIN, R2Reason.DIRECTION_UNAVAILABLE
    if residual * market_return > 0.0 and abs(market_return) >= abs(residual):
        return R2Decision.ABSTAIN, R2Reason.MARKET_WIDE_MOVE_SAME_DIRECTION
    if percentile >= request.policy.residual_percentile_min:
        return R2Decision.MATCHED, R2Reason.CONDITIONS_MATCHED
    return R2Decision.NOT_MATCHED, R2Reason.CONDITIONS_NOT_MET


def _directional_outcome(
    feature: R2Feature,
    *,
    anchor_price: float,
    candles: Sequence[HistoricalCandle],
    policy: R2ExtensionPolicy,
) -> R2Outcome:
    target_at = feature.available_at + timedelta(seconds=feature.horizon_seconds)
    if feature.decision is not R2Decision.MATCHED:
        return R2Outcome(
            observation_id=feature.observation_id,
            target_at=target_at,
            available_at=target_at,
            available=False,
            reason=feature.reason,
            forward_return_bps=None,
            cost_adjusted_signed_return_bps=None,
        )
    target = next(
        (
            item
            for item in candles
            if _candle_available_at(item) == target_at and item.complete
        ),
        None,
    )
    if target is None:
        return R2Outcome(
            observation_id=feature.observation_id,
            target_at=target_at,
            available_at=target_at,
            available=False,
            reason=R2Reason.OUTCOME_UNAVAILABLE,
            forward_return_bps=None,
            cost_adjusted_signed_return_bps=None,
        )
    gross = _return_bps(anchor_price, target.close)
    return R2Outcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available_at=_candle_available_at(target),
        available=True,
        reason=R2Reason.CONDITIONS_MATCHED,
        forward_return_bps=gross,
        cost_adjusted_signed_return_bps=(
            gross * feature.expected_direction - policy.round_trip_cost_bps
        ),
    )


@dataclass(frozen=True, slots=True)
class _OpeningGapCandidate:
    ticker: str
    trading_day: date
    opening: HistoricalCandle
    available_at: datetime
    previous_close: HistoricalCandle | None
    gap_bps: float | None
    gap_z: float | None
    history: tuple[tuple[date, float, datetime], ...]
    candles: tuple[HistoricalCandle, ...]


def _group_by_day(
    candles: Sequence[HistoricalCandle],
) -> dict[date, tuple[HistoricalCandle, ...]]:
    grouped: defaultdict[date, list[HistoricalCandle]] = defaultdict(list)
    for candle in candles:
        grouped[candle.at.astimezone(MOSCOW).date()].append(candle)
    return {day: tuple(rows) for day, rows in sorted(grouped.items())}


def _complete_windows(
    candles: Sequence[HistoricalCandle], window_minutes: int
) -> tuple[tuple[HistoricalCandle, ...], ...]:
    main = tuple(
        item
        for item in candles
        if _MAIN_OPEN <= item.at.astimezone(MOSCOW).time() < _MAIN_CLOSE
    )
    by_at = {item.at: item for item in main}
    result: list[tuple[HistoricalCandle, ...]] = []
    for start in main:
        local = start.at.astimezone(MOSCOW)
        elapsed = local.hour * 60 + local.minute - 10 * 60
        if elapsed < 0 or elapsed % window_minutes:
            continue
        window = tuple(
            by_at.get(start.at + timedelta(minutes=offset))
            for offset in range(window_minutes)
        )
        if any(item is None for item in window):
            continue
        result.append(tuple(item for item in window if item is not None))
    return tuple(result)


def _daily_returns(
    by_ticker: Mapping[str, tuple[HistoricalCandle, ...]],
) -> dict[str, dict[date, float]]:
    result: dict[str, dict[date, float]] = {}
    for ticker, candles in by_ticker.items():
        result[ticker] = {
            trading_day: _return_bps(rows[0].open, rows[-1].close)
            for trading_day, rows in _group_by_day(candles).items()
        }
    return result


def _basket_daily_returns(
    daily: Mapping[str, Mapping[date, float]], universe: Sequence[str]
) -> dict[date, float]:
    values: defaultdict[date, list[float]] = defaultdict(list)
    for ticker in universe:
        for trading_day, value in daily.get(ticker, {}).items():
            values[trading_day].append(value)
    return {day: fmean(rows) for day, rows in values.items() if rows}


def _basket_window_returns(
    by_ticker_day: Mapping[str, Mapping[date, tuple[HistoricalCandle, ...]]],
    universe: Sequence[str],
    window_minutes: int,
) -> dict[tuple[date, time], tuple[float, float]]:
    values: defaultdict[tuple[date, time], list[float]] = defaultdict(list)
    for ticker in universe:
        for trading_day, candles in by_ticker_day.get(ticker, {}).items():
            for window in _complete_windows(candles, window_minutes):
                key = (trading_day, window[0].at.astimezone(MOSCOW).time())
                values[key].append(_return_bps(window[0].open, window[-1].close))
    denominator = len(universe)
    return {
        key: (fmean(rows), len(rows) / denominator)
        for key, rows in values.items()
    }


def _basket_window_returns_from_compact(
    by_ticker: Mapping[str, Mapping[tuple[date, time], float]],
    universe: Sequence[str],
) -> dict[tuple[date, time], tuple[float, float]]:
    values: defaultdict[tuple[date, time], list[float]] = defaultdict(list)
    for ticker in universe:
        for key, value in by_ticker.get(ticker, {}).items():
            values[key].append(value)
    denominator = len(universe)
    return {
        key: (fmean(rows), len(rows) / denominator)
        for key, rows in values.items()
    }


def _prior_daily_pairs(
    before_day: date,
    stock: Mapping[date, float],
    market: Mapping[date, float],
    lookback: int,
    excluded_ticker_days: frozenset[tuple[str, date]],
    ticker: str,
) -> tuple[tuple[date, float, float], ...]:
    days = tuple(
        sorted(
            day
            for day in stock.keys() & market.keys()
            if day < before_day and (ticker, day) not in excluded_ticker_days
        )
    )
    return tuple((day, stock[day], market[day]) for day in days[-lookback:])


def _basket_model_available_at(
    trading_day: date | None,
    by_ticker_day: Mapping[str, Mapping[date, tuple[HistoricalCandle, ...]]],
    universe: Sequence[str],
) -> datetime | None:
    if trading_day is None:
        return None
    latest = tuple(
        rows[trading_day][-1]
        for ticker in universe
        if (rows := by_ticker_day.get(ticker)) is not None
        and trading_day in rows
        and rows[trading_day]
    )
    if not latest:
        return None
    return max(_candle_available_at(item) for item in latest)


def _compact_basket_model_available_at(
    trading_day: date | None,
    closes: Mapping[str, Mapping[date, datetime]],
    universe: Sequence[str],
) -> datetime | None:
    if trading_day is None:
        return None
    latest = tuple(
        by_day[trading_day]
        for ticker in universe
        if (by_day := closes.get(ticker)) is not None
        and trading_day in by_day
    )
    return max(latest) if latest else None


def _beta(history: Sequence[tuple[date, float, float]]) -> float | None:
    if len(history) < 2:
        return None
    stock = tuple(item[1] for item in history)
    market = tuple(item[2] for item in history)
    market_mean = fmean(market)
    denominator = sum((value - market_mean) ** 2 for value in market)
    if denominator <= 0.0:
        return None
    stock_mean = fmean(stock)
    return sum(
        (market_value - market_mean) * (stock_value - stock_mean)
        for stock_value, market_value in zip(stock, market, strict=True)
    ) / denominator


def _prior_residuals(
    *,
    ticker: str,
    before_day: date,
    by_ticker_day: Mapping[str, Mapping[date, tuple[HistoricalCandle, ...]]],
    basket_windows: Mapping[tuple[date, time], tuple[float, float]],
    beta: float | None,
    window_minutes: int,
    excluded_ticker_days: frozenset[tuple[str, date]],
) -> tuple[float, ...]:
    if beta is None:
        return ()
    result: list[float] = []
    for trading_day, candles in by_ticker_day.get(ticker, {}).items():
        if (
            trading_day >= before_day
            or (ticker, trading_day) in excluded_ticker_days
        ):
            continue
        for window in _complete_windows(candles, window_minutes):
            key = (trading_day, window[0].at.astimezone(MOSCOW).time())
            market = basket_windows.get(key)
            if market is None:
                continue
            stock = _return_bps(window[0].open, window[-1].close)
            result.append(stock - beta * market[0])
    return tuple(result)


def _prior_compact_residuals(
    *,
    ticker: str,
    before_day: date,
    windows_by_day: Mapping[date, Sequence[tuple[time, float]]],
    basket_windows: Mapping[tuple[date, time], tuple[float, float]],
    beta: float | None,
    excluded_ticker_days: frozenset[tuple[str, date]],
) -> tuple[float, ...]:
    if beta is None:
        return ()
    result: list[float] = []
    for trading_day, windows in windows_by_day.items():
        if (
            trading_day >= before_day
            or (ticker, trading_day) in excluded_ticker_days
        ):
            continue
        for local_time, stock_return in windows:
            market = basket_windows.get((trading_day, local_time))
            if market is not None:
                result.append(stock_return - beta * market[0])
    return tuple(result)


def _validated_ticker_partition(
    raw_partition: Sequence[HistoricalCandle],
    seen_tickers: set[str],
) -> tuple[str, tuple[HistoricalCandle, ...]] | None:
    candles = tuple(item for item in raw_partition if item.complete)
    if not candles:
        return None
    ticker = candles[0].ticker
    if ticker in seen_tickers or any(item.ticker != ticker for item in candles):
        raise ValueError("partitioned R2 requires one unique ticker partition")
    if any(left.at >= right.at for left, right in zip(candles, candles[1:])):
        raise ValueError("partitioned R2 candles must be strictly time ordered")
    seen_tickers.add(ticker)
    return ticker, candles


def _z_score(value: float | None, history: Sequence[float]) -> float | None:
    if value is None or len(history) < 2:
        return None
    deviation = pstdev(history)
    if deviation <= 0.0:
        return None
    return (value - fmean(history)) / deviation


def _absolute_percentile(value: float, history: Sequence[float]) -> float:
    if not history:
        return 0.0
    absolute = abs(value)
    return sum(abs(item) <= absolute for item in history) / len(history)


def _return_bps(start: float, end: float) -> float:
    return log(end / start) * 10_000.0


def _candle_available_at(candle: HistoricalCandle) -> datetime:
    return candle.at + timedelta(minutes=1)


def _ticker_day_payload(values: Sequence[tuple[str, date]]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((ticker, day.isoformat()) for ticker, day in values))


def _require_unique(values: Sequence[object], label: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"R2 {label} must be unique")


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()

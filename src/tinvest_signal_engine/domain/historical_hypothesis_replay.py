"""Immutable values produced by causal historical hypothesis replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    EvidenceBundle,
)
from tinvest_signal_engine.domain.hypothesis_formulas import (
    ExpectedEffect,
    HypothesisId,
    ObservationReason,
    ObservationVerdict,
    OutcomeAnchor,
)
from tinvest_signal_engine.domain.trading_phases import TradingPhase


@dataclass(frozen=True, slots=True)
class HistoricalCandle:
    ticker: str
    at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    complete: bool = True

    def __post_init__(self) -> None:
        if not self.ticker.strip():
            raise ValueError("candle ticker must not be empty")
        if self.at.tzinfo is None or self.at.utcoffset() is None:
            raise ValueError("candle timestamp must be timezone-aware")
        if min(self.open, self.high, self.low, self.close) <= 0.0:
            raise ValueError("candle prices must be positive")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("candle OHLC bounds are inconsistent")
        if self.volume < 0.0:
            raise ValueError("candle volume must not be negative")


@dataclass(frozen=True, slots=True)
class CandleCacheDescriptor:
    dataset_fingerprint: str
    partition_count: int
    tickers: tuple[str, ...]
    start_day: date
    end_day: date

    def __post_init__(self) -> None:
        if not self.dataset_fingerprint.startswith("sha256:"):
            raise ValueError("dataset_fingerprint must use sha256")
        if self.partition_count <= 0 or not self.tickers:
            raise ValueError("cache descriptor must contain partitions and tickers")
        if self.end_day < self.start_day:
            raise ValueError("cache end_day must not precede start_day")


@dataclass(frozen=True, slots=True)
class ReplayCostModel:
    version: str
    commission_bps: float
    slippage_bps: float
    half_spread_entry_bps: float
    half_spread_exit_bps: float

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("cost model version must not be empty")
        if min(
            self.commission_bps,
            self.slippage_bps,
            self.half_spread_entry_bps,
            self.half_spread_exit_bps,
        ) < 0.0:
            raise ValueError("cost model components must not be negative")

    @property
    def round_trip_bps(self) -> float:
        return (
            self.commission_bps
            + self.slippage_bps
            + self.half_spread_entry_bps
            + self.half_spread_exit_bps
        )


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    observation_id: str
    hypothesis_id: HypothesisId
    hypothesis_version: str
    ticker: str
    event_at: datetime
    trading_day: date
    phase: TradingPhase
    verdict: ObservationVerdict
    reason: ObservationReason
    expected_effect: ExpectedEffect
    expected_direction: int
    outcome_anchor: OutcomeAnchor
    horizon_seconds: int
    feature_cutoff_at: datetime
    gross_effect_bps: float | None
    net_effect_bps: float | None
    label_available: bool

    def __post_init__(self) -> None:
        if self.feature_cutoff_at > self.event_at:
            raise ValueError("replay outcome contains feature leakage")
        if self.label_available != (self.net_effect_bps is not None):
            raise ValueError("label availability must match net effect presence")


@dataclass(frozen=True, slots=True)
class HypothesisReplaySummary:
    hypothesis_id: HypothesisId
    hypothesis_version: str
    evaluated_observations: int
    matched_observations: int
    abstained_observations: int
    available_labels: int
    holdout_eligible_events: int


@dataclass(frozen=True, slots=True)
class HistoricalReplayReport:
    run_id: str
    engine_version: str
    dataset_fingerprint: str
    cache_partition_count: int
    selected_hypotheses: tuple[HypothesisId, ...]
    cost_model: ReplayCostModel
    split: ChronologicalSplit | None
    summaries: tuple[HypothesisReplaySummary, ...]
    outcomes: tuple[ReplayOutcome, ...]
    evidence: tuple[EvidenceBundle, ...]


@dataclass(frozen=True, slots=True)
class CompletedReplay:
    run_id: str
    artifact_fingerprint: str
    dataset_fingerprint: str
    selected_hypotheses: tuple[HypothesisId, ...]
    resumed: bool

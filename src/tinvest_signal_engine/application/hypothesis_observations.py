"""Application use case for deterministic scientific hypothesis observations."""

from __future__ import annotations

from datetime import datetime

from tinvest_signal_engine.domain.hypothesis_formulas import (
    HypothesisEvent,
    HypothesisFeatureSet,
    HypothesisId,
    HypothesisObservation,
    HypothesisRule,
    default_rule,
    evaluate_hypothesis_rule,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
    TradingPhaseSchedule,
)


class EvaluateHypothesisObservation:
    """Classify one event without reading storage or future market state."""

    def __init__(
        self,
        schedule: TradingPhaseSchedule = MOEX_EQUITY_PHASE_SCHEDULE_V1,
    ) -> None:
        self._schedule = schedule

    def execute(
        self,
        *,
        hypothesis_id: HypothesisId,
        ticker: str,
        event_at: datetime,
        features: HypothesisFeatureSet,
        has_trading_gap: bool = False,
        rule: HypothesisRule | None = None,
    ) -> HypothesisObservation:
        selected_rule = rule or default_rule(hypothesis_id)
        if selected_rule.hypothesis_id is not hypothesis_id:
            raise ValueError("rule identity does not match requested hypothesis")
        event = HypothesisEvent(
            ticker=ticker,
            event_at=event_at,
            phase=self._schedule.phase_at(event_at),
            has_trading_gap=has_trading_gap,
        )
        return evaluate_hypothesis_rule(selected_rule, event, features)

"""Pure rules for clean-price convergence of bonds towards par at maturity."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


POLICY_VERSION = "bond-par-convergence-v1.0.0"
EVIDENCE_VERSION = "tinvest-bonds-2026-07-v1"
PAR_PRICE = Decimal("100")
TARGET_TRADING_SESSIONS = 60
MINIMUM_DEVIATION_POINTS = Decimal("0.3")
ROUND_TRIP_COST_BPS = Decimal("20")
ALLOWED_RISK_LEVELS = frozenset({"low", "moderate"})


@dataclass(frozen=True)
class BondConvergenceEvidence:
    eligible_observations: int = 189
    successful_observations: int = 177
    distinct_maturities: int = 142
    success_rate: Decimal = Decimal("0.9365079365")
    wilson_lower_bound: Decimal = Decimal("0.8923139")
    mean_net_return_bps: Decimal = Decimal("81.2116")
    assumed_round_trip_cost_bps: Decimal = ROUND_TRIP_COST_BPS
    version: str = EVIDENCE_VERSION


HISTORICAL_EVIDENCE = BondConvergenceEvidence()


@dataclass(frozen=True)
class BondConvergenceSnapshot:
    instrument_id: str
    ticker: str
    class_code: str
    alias: str
    figi: str
    uid: str
    lot: int
    source_time: datetime
    maturity_date: date
    clean_price: Decimal
    sessions_to_maturity: int
    currency: str
    country_of_risk: str
    risk_level: str
    amortization: bool
    subordinated: bool
    perpetual: bool
    api_trade_available: bool
    buy_available: bool
    sell_available: bool


@dataclass(frozen=True)
class BondConvergenceDecision:
    eligible: bool
    reason: str
    direction: str | None = None
    direction_sign: int = 0
    gross_distance_bps: Decimal = Decimal("0")
    expected_net_distance_bps: Decimal = Decimal("0")


def evaluate_bond_convergence(
    snapshot: BondConvergenceSnapshot,
) -> BondConvergenceDecision:
    """Evaluate a snapshot without market or framework dependencies."""

    if snapshot.currency.lower() != "rub":
        return BondConvergenceDecision(False, "currency_not_rub")
    if snapshot.country_of_risk.upper() != "RU":
        return BondConvergenceDecision(False, "country_of_risk_not_ru")
    if snapshot.risk_level.lower() not in ALLOWED_RISK_LEVELS:
        return BondConvergenceDecision(False, "risk_level_not_allowed")
    if snapshot.amortization:
        return BondConvergenceDecision(False, "amortizing_bond")
    if snapshot.subordinated:
        return BondConvergenceDecision(False, "subordinated_bond")
    if snapshot.perpetual:
        return BondConvergenceDecision(False, "perpetual_bond")
    if not (
        snapshot.api_trade_available
        and snapshot.buy_available
        and snapshot.sell_available
    ):
        return BondConvergenceDecision(False, "trading_not_available")
    if snapshot.sessions_to_maturity != TARGET_TRADING_SESSIONS:
        return BondConvergenceDecision(False, "outside_target_session")
    if snapshot.clean_price <= 0:
        return BondConvergenceDecision(False, "invalid_clean_price")

    deviation = PAR_PRICE - snapshot.clean_price
    if abs(deviation) < MINIMUM_DEVIATION_POINTS:
        return BondConvergenceDecision(False, "deviation_too_small")

    direction_sign = 1 if deviation > 0 else -1
    direction = "up" if direction_sign > 0 else "down"
    gross_distance_bps = abs(deviation) / snapshot.clean_price * Decimal("10000")
    expected_net_distance_bps = gross_distance_bps - ROUND_TRIP_COST_BPS
    if expected_net_distance_bps <= 0:
        return BondConvergenceDecision(False, "distance_below_cost")
    return BondConvergenceDecision(
        eligible=True,
        reason="eligible",
        direction=direction,
        direction_sign=direction_sign,
        gross_distance_bps=gross_distance_bps,
        expected_net_distance_bps=expected_net_distance_bps,
    )

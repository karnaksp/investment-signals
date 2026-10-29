"""Event-time futures curve anomalies, independent of transports and storage.

These observations describe unusual relative prices or activity. They neither
predict a profitable trade nor infer the direction of positions from aggregate OI.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from hashlib import sha256
import json
import math
from statistics import median
from typing import Any


SIGNAL_TYPES = (
    "calendar_spread_dislocation",
    "roll_migration_anomaly",
    "futures_curve_kink",
    "term_premium_shock",
    "futures_basis_momentum",
)
_PRICE_TYPES = set(SIGNAL_TYPES) - {"roll_migration_anomaly"}


@dataclass(frozen=True)
class CurveContract:
    instrument_id: str
    ticker: str
    chain_id: str
    expiration_at: datetime
    price_multiplier: float
    currency: str = "RUB"
    specification_key: str = ""


@dataclass(frozen=True)
class CurveMarketEvent:
    event_id: str
    instrument_id: str
    at: datetime
    kind: str
    bid: float | None = None
    ask: float | None = None
    quantity: float = 0
    open_interest: float | None = None
    received_at: datetime | None = None
    trade_price: float | None = None


@dataclass(frozen=True)
class CurveSignal:
    signal_type: str
    chain_id: str
    instrument_id: str
    ticker: str
    at: datetime
    metric_value: float
    baseline_value: float
    z_score: float
    payload: dict[str, Any]


@dataclass(frozen=True)
class CurveSettings:
    enabled_types: tuple[str, ...] = SIGNAL_TYPES
    sample_interval_seconds: float = 30
    baseline_points: int = 720
    min_baseline_points: int = 120
    min_warmup_seconds: float = 3600
    max_quote_age_seconds: float = 5
    max_quote_skew_seconds: float = 2
    max_oi_age_seconds: float = 60
    max_sample_gap_seconds: float = 120
    activity_window_seconds: int = 300
    min_total_notional_rub: float = 20_000_000
    min_leg_notional_rub: float = 5_000_000
    min_leg_trades: int = 20
    z_threshold: float = 4.5
    min_deviation_bps: float = 15
    dispersion_floor_bps: float = 2
    execution_cost_multiplier: float = 4
    min_carry_change: float = 0.03
    carry_dispersion_floor: float = 0.005
    min_kink_deviation_bps: float = 15
    momentum_horizon_seconds: float = 3600
    min_momentum_bps: float = 20
    roll_window_seconds: float = 300
    min_roll_oi_share_change: float = 0.05
    min_roll_volume_share_change: float = 0.15
    min_roll_matched_contracts: float = 500
    min_roll_relative_oi_change: float = 0.01
    min_roll_spread_deviation_bps: float = 10
    roll_dispersion_floor: float = 0.005
    persistence_seconds: float = 30
    cooldown_seconds: float = 1800

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if name == "enabled_types":
                if not set(value).issubset(SIGNAL_TYPES):
                    raise ValueError("unknown futures curve signal type")
            elif isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be numeric")
            elif not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.sample_interval_seconds <= 0 or self.baseline_points < 2:
            raise ValueError("sampling interval and baseline capacity must be positive")
        for name in ("baseline_points", "min_baseline_points", "activity_window_seconds", "min_leg_trades"):
            if not isinstance(getattr(self, name), int):
                raise ValueError(f"{name} must be an integer")
        if not 2 <= self.min_baseline_points <= self.baseline_points:
            raise ValueError("min_baseline_points exceeds baseline capacity")
        if self.activity_window_seconds < 1 or self.momentum_horizon_seconds <= 0:
            raise ValueError("activity and momentum windows must be positive")
        if self.dispersion_floor_bps <= 0 or self.carry_dispersion_floor <= 0:
            raise ValueError("dispersion floors must be positive")
        if self.roll_dispersion_floor <= 0 or self.z_threshold <= 0:
            raise ValueError("roll dispersion and z threshold must be positive")


@dataclass
class _LegState:
    quote: tuple[datetime, float, float] | None = None
    oi: tuple[datetime, float] | None = None
    # One bucket per second, independent of event rate.
    trades: deque[tuple[int, float, int]] = field(default_factory=deque)
    last_at: dict[str, datetime] = field(default_factory=dict)
    seen: deque[str] = field(default_factory=deque)
    seen_set: set[str] = field(default_factory=set)


@dataclass
class _Candidate:
    signal_type: str
    contracts: tuple[CurveContract, ...]
    metric: float
    baseline: float
    score: float
    deviation_bps: float
    direction: str
    baseline_count: int
    details: dict[str, Any]


def _finite(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None


def _dt(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if not _aware(result):
        raise ValueError("naive curve timestamp")
    return result


class FuturesCurveDetector:
    """A bounded, event-time state machine with prior-only robust baselines."""

    def __init__(
        self,
        contracts: tuple[CurveContract, ...],
        settings: CurveSettings = CurveSettings(),
    ) -> None:
        self.settings = settings
        if len({item.instrument_id for item in contracts}) != len(contracts):
            raise ValueError("duplicate futures instrument")
        for item in contracts:
            if not item.instrument_id or not item.chain_id or not _aware(item.expiration_at):
                raise ValueError("futures contract identity and aware expiry are required")
        self.contracts = {item.instrument_id: item for item in contracts}
        grouped: dict[tuple[str, str], list[CurveContract]] = {}
        for item in contracts:
            if (
                item.currency.upper() != "RUB"
                or not _finite(item.price_multiplier)
                or item.price_multiplier <= 0
                or not item.specification_key
            ):
                continue
            grouped.setdefault((item.chain_id, item.specification_key), []).append(item)
        self._groups = tuple(
            tuple(sorted(items, key=lambda item: (item.expiration_at, item.ticker)))
            for items in grouped.values()
            if len(items) >= 2
        )
        fingerprint = {
            "contracts": [
                {**asdict(item), "expiration_at": item.expiration_at.isoformat()}
                for item in sorted(contracts, key=lambda item: item.instrument_id)
            ],
            "settings": asdict(settings),
        }
        self._fingerprint = sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()
        self._reset()

    def _reset(self) -> None:
        self._legs = {key: _LegState() for key in self.contracts}
        self._history: dict[str, deque[tuple[datetime, float]]] = {}
        self._stats_cache: dict[str, tuple[datetime, int, float, float, float]] = {}
        self._prices: dict[str, deque[tuple[datetime, float, float]]] = {}
        self._roll: dict[str, deque[tuple[datetime, float, float, float]]] = {}
        self._last_sample: dict[str, datetime] = {}
        self._pending: dict[str, tuple[datetime, datetime, str]] = {}
        self._cooldowns: dict[str, datetime] = {}
        self._as_of: datetime | None = None
        self.state_revision = 0
        self._emission_enabled = True

    def process(self, event: CurveMarketEvent, *, emit_signals: bool = True) -> tuple[CurveSignal, ...]:
        self._emission_enabled = emit_signals
        contract = self.contracts.get(event.instrument_id)
        if contract is None or event.kind not in {"orderbook", "trade", "open_interest"}:
            return ()
        if not _aware(event.at) or not _aware(event.received_at or event.at):
            return ()
        now = event.received_at or event.at
        state = self._legs[event.instrument_id]
        if now < event.at:
            return ()
        if event.event_id in state.seen_set:
            return ()
        previous = state.last_at.get(event.kind)
        if previous is not None and event.at < previous:
            return ()
        state.last_at[event.kind] = event.at
        state.seen.append(event.event_id)
        state.seen_set.add(event.event_id)
        while len(state.seen) > 20_000:
            state.seen_set.discard(state.seen.popleft())
        if contract.expiration_at <= event.at:
            state.quote = None
            return ()
        if event.kind == "orderbook":
            if (
                not _finite(event.bid)
                or not _finite(event.ask)
                or event.bid <= 0
                or event.ask <= event.bid
                or (now - event.at).total_seconds() > self.settings.max_quote_age_seconds
            ):
                state.quote = None
                self._clear_pending_instrument(contract.instrument_id)
                return ()
            state.quote = (event.at, float(event.bid), float(event.ask))
        elif event.kind == "trade":
            if (
                not _finite(event.quantity)
                or event.quantity <= 0
                or not _finite(contract.price_multiplier)
                or contract.price_multiplier <= 0
            ):
                return ()
            if event.trade_price is not None:
                if not _finite(event.trade_price) or event.trade_price <= 0:
                    return ()
                price = event.trade_price
            else:
                quote = state.quote
                if quote is None or not 0 <= (event.at - quote[0]).total_seconds() <= self.settings.max_quote_age_seconds:
                    return ()
                price = quote[1] + (quote[2] - quote[1]) / 2
            notional = price * event.quantity * contract.price_multiplier
            if not math.isfinite(notional):
                return ()
            bucket = int(event.at.timestamp())
            if state.trades and state.trades[-1][0] == bucket:
                _, money, count = state.trades.pop()
                state.trades.append((bucket, money + notional, count + 1))
            else:
                state.trades.append((bucket, notional, 1))
            self._prune_trades(state, event.at)
            return ()
        else:
            if not _finite(event.open_interest) or event.open_interest < 0:
                state.oi = None
                return ()
            state.oi = (event.at, float(event.open_interest))
            return ()

        # A future or delayed message cannot age an older event into persistence.
        if self._as_of is not None and event.at < self._as_of:
            return ()
        self._as_of = event.at
        candidates: list[_Candidate] = []
        for group in self._groups:
            if event.instrument_id not in {item.instrument_id for item in group}:
                continue
            active = tuple(item for item in group if item.expiration_at > event.at)
            for index in range(len(active) - 1):
                pair = active[index:index + 2]
                candidates.extend(self._pair_candidates(pair, event.at, now))
            for index in range(len(active) - 2):
                candidates.extend(self._kink_candidates(active[index:index + 3], event.at, now))
        # Only one of the correlated price descriptions per chain; OI migration
        # has an independent cooldown because it describes different evidence.
        priority = {
            "futures_curve_kink": 4,
            "term_premium_shock": 3,
            "futures_basis_momentum": 2,
            "calendar_spread_dislocation": 1,
            "roll_migration_anomaly": 0,
        }
        candidates.sort(
            key=lambda item: (
                round(abs(item.score), 9),
                priority[item.signal_type],
                self._group_key(item.contracts),
            ),
            reverse=True,
        )
        selected: list[CurveSignal] = []
        used: set[tuple[str, str]] = set()
        for candidate in candidates:
            chain = candidate.contracts[0].chain_id
            category = "price" if candidate.signal_type in _PRICE_TYPES else "roll"
            category_key = f"{chain}|{category}"
            if (chain, category) in used:
                continue
            last = self._cooldowns.get(category_key)
            if last is not None and (event.at - last).total_seconds() < self.settings.cooldown_seconds:
                continue
            selected.append(self._signal(candidate, event.at, now))
            self._cooldowns[category_key] = event.at
            self._clear_pending(chain)
            used.add((chain, category))
            self.state_revision += 1
        return tuple(selected)

    def _clear_pending(self, chain: str) -> None:
        for key in tuple(self._pending):
            if key.startswith(chain + "|"):
                del self._pending[key]

    def _clear_pending_group(self, contracts: tuple[CurveContract, ...]) -> None:
        prefix = self._group_key(contracts) + "|"
        for key in tuple(self._pending):
            if key.startswith(prefix):
                del self._pending[key]

    def _clear_pending_instrument(self, instrument_id: str) -> None:
        for key in tuple(self._pending):
            if instrument_id in key.rsplit("|", 2)[1].split(","):
                del self._pending[key]

    def _prune_trades(self, state: _LegState, at: datetime) -> None:
        cutoff = int(at.timestamp()) - self.settings.activity_window_seconds
        while state.trades and state.trades[0][0] <= cutoff:
            state.trades.popleft()

    def _activity(self, contracts: tuple[CurveContract, ...], at: datetime) -> tuple[list[float], list[int], bool]:
        monies: list[float] = []
        counts: list[int] = []
        for item in contracts:
            state = self._legs[item.instrument_id]
            self._prune_trades(state, at)
            monies.append(sum(bucket[1] for bucket in state.trades))
            counts.append(sum(bucket[2] for bucket in state.trades))
        allowed = (
            sum(monies) >= self.settings.min_total_notional_rub
            and all(value >= self.settings.min_leg_notional_rub for value in monies)
            and all(value >= self.settings.min_leg_trades for value in counts)
        )
        return monies, counts, allowed

    def _quotes(self, contracts: tuple[CurveContract, ...], at: datetime, now: datetime) -> tuple[tuple[float, ...], float] | None:
        quotes = [self._legs[item.instrument_id].quote for item in contracts]
        if any(quote is None for quote in quotes):
            return None
        times = [quote[0] for quote in quotes if quote is not None]
        if any(not 0 <= (now - stamp).total_seconds() <= self.settings.max_quote_age_seconds for stamp in times):
            return None
        if (max(times) - min(times)).total_seconds() > self.settings.max_quote_skew_seconds:
            return None
        if len({item.expiration_at for item in contracts}) != len(contracts):
            return None
        mids = tuple(quote[1] + (quote[2] - quote[1]) / 2 for quote in quotes if quote is not None)
        cost = sum((quote[2] - quote[1]) / (quote[1] + quote[2]) * 10_000 for quote in quotes if quote is not None)
        return mids, cost

    @staticmethod
    def _group_key(contracts: tuple[CurveContract, ...]) -> str:
        return contracts[0].chain_id + "|" + ",".join(item.instrument_id for item in contracts)

    def _stats(self, key: str, at: datetime, value: float, floor: float) -> tuple[float, float, int] | None:
        history = self._history.get(key)
        if history is None or len(history) < self.settings.min_baseline_points:
            return None
        if (at - history[-1][0]).total_seconds() > self.settings.max_sample_gap_seconds:
            self._history.pop(key, None)
            self._stats_cache.pop(key, None)
            self._last_sample.pop(key, None)
            self._pending.pop(key, None)
            return None
        if (history[-1][0] - history[0][0]).total_seconds() < self.settings.min_warmup_seconds:
            return None
        cached = self._stats_cache.get(key)
        signature = (history[-1][0], len(history), floor)
        if cached is None or cached[:3] != signature:
            center = median(point[1] for point in history)
            scale = max(floor, 1.4826 * median(abs(point[1] - center) for point in history))
            self._stats_cache[key] = (*signature, center, scale)
        else:
            center, scale = cached[3:]
        return center, (value - center) / scale, len(history)

    def _append(self, key: str, at: datetime, value: float) -> None:
        last = self._last_sample.get(key)
        if last is not None and (at - last).total_seconds() < self.settings.sample_interval_seconds:
            return
        self._history.setdefault(key, deque(maxlen=self.settings.baseline_points)).append((at, value))
        self._stats_cache.pop(key, None)
        self._last_sample[key] = at
        self.state_revision += 1

    def _check(
        self,
        *,
        signal_type: str,
        contracts: tuple[CurveContract, ...],
        at: datetime,
        value: float,
        floor: float,
        min_deviation: float,
        deviation_bps_scale: float = 1,
        cost_bps: float = 0,
        direction_positive: str = "widening",
        direction_negative: str = "narrowing",
        additional_gate: bool = True,
        details: dict[str, Any] | None = None,
    ) -> _Candidate | None:
        key = self._group_key(contracts) + "|" + signal_type
        stats = self._stats(key, at, value, floor)
        candidate: _Candidate | None = None
        abnormal = False
        if stats is not None:
            center, score, count = stats
            delta = value - center
            deviation_bps = delta * deviation_bps_scale
            abnormal = abs(score) >= self.settings.z_threshold and abs(delta) >= min_deviation
            enough_activity = self._activity(contracts, at)[2]
            if (
                abnormal
                and additional_gate
                and enough_activity
                and abs(deviation_bps) >= cost_bps * self.settings.execution_cost_multiplier
                and signal_type in self.settings.enabled_types
                and self._emission_enabled
            ):
                direction = direction_positive if delta > 0 else direction_negative
                pending = self._pending.get(key)
                if pending is None or pending[2] != direction or (at - pending[1]).total_seconds() > self.settings.max_quote_age_seconds:
                    pending = (at, at, direction)
                self._pending[key] = (pending[0], at, direction)
                if (at - pending[0]).total_seconds() >= self.settings.persistence_seconds:
                    candidate = _Candidate(signal_type, contracts, value, center, score, deviation_bps, direction, count, details or {})
            else:
                self._pending.pop(key, None)
        # Extreme observations do not become their own reference while waiting
        # for persistence or sufficient activity.
        if not abnormal:
            self._append(key, at, value)
        return candidate

    def _pair_candidates(self, pair: tuple[CurveContract, ...], at: datetime, now: datetime) -> list[_Candidate]:
        key = self._group_key(pair)
        quotes = self._quotes(pair, at, now)
        if quotes is None:
            self._clear_pending_group(pair)
            return []
        mids, cost = quotes
        spread_bps = (math.log(mids[1]) - math.log(mids[0])) * 10_000
        gap_days = (pair[1].expiration_at - pair[0].expiration_at).total_seconds() / 86_400
        carry = spread_bps / 10_000 * 365 / gap_days
        spread_stats = self._stats(key + "|calendar_spread_dislocation", at, spread_bps, self.settings.dispersion_floor_bps)
        spread_delta = spread_bps - spread_stats[0] if spread_stats else 0
        details = {"spread_bps": spread_bps, "calendar_spread": mids[1] - mids[0], "expiry_gap_days": gap_days, "execution_cost_bps": cost}
        result: list[_Candidate] = []
        for candidate in (
            self._check(signal_type="calendar_spread_dislocation", contracts=pair, at=at, value=spread_bps, floor=self.settings.dispersion_floor_bps, min_deviation=self.settings.min_deviation_bps, cost_bps=cost, details={**details, "metric_unit": "bps"}),
            self._check(signal_type="term_premium_shock", contracts=pair, at=at, value=carry, floor=self.settings.carry_dispersion_floor, min_deviation=self.settings.min_carry_change, deviation_bps_scale=10_000 * gap_days / 365, cost_bps=cost, additional_gate=abs(spread_delta) >= self.settings.min_deviation_bps, details={**details, "annualized_premium": carry, "metric_unit": "annualized_fraction"}),
        ):
            if candidate:
                result.append(candidate)
        prices = self._prices.setdefault(key, deque())
        cutoff = at - timedelta(seconds=self.settings.momentum_horizon_seconds)
        anchor = next((point for point in reversed(prices) if point[0] <= cutoff), None)
        if anchor is not None and (cutoff - anchor[0]).total_seconds() <= self.settings.sample_interval_seconds * 2:
            momentum = (math.log(mids[0]) - math.log(anchor[1]) - math.log(mids[1]) + math.log(anchor[2])) * 10_000
            candidate = self._check(signal_type="futures_basis_momentum", contracts=pair, at=at, value=momentum, floor=self.settings.dispersion_floor_bps, min_deviation=self.settings.min_momentum_bps, cost_bps=cost, direction_positive="near_outperformance", direction_negative="far_outperformance", details={**details, "momentum_horizon_seconds": self.settings.momentum_horizon_seconds, "research_adaptation": "intraday_log_return_difference", "metric_unit": "bps"})
            if candidate:
                result.append(candidate)
        if not prices or (at - prices[-1][0]).total_seconds() >= self.settings.sample_interval_seconds:
            prices.append((at, mids[0], mids[1]))
            while len(prices) > 2 and prices[1][0] <= cutoff - timedelta(seconds=self.settings.sample_interval_seconds * 2):
                prices.popleft()
            self.state_revision += 1
        self._roll_candidate(pair, at, now, spread_delta, cost, result)
        return result

    def _roll_candidate(self, pair: tuple[CurveContract, ...], at: datetime, now: datetime, spread_delta: float, cost: float, result: list[_Candidate]) -> None:
        key = self._group_key(pair)
        oi = [self._legs[item.instrument_id].oi for item in pair]
        if any(point is None or not 0 <= (now - point[0]).total_seconds() <= self.settings.max_oi_age_seconds for point in oi):
            self._pending.pop(key + "|roll_migration_anomaly", None)
            return
        near, far = (point[1] for point in oi if point is not None)
        if near + far <= 0:
            return
        monies, _, _ = self._activity(pair, at)
        if sum(monies) <= 0:
            return
        volume_share = monies[1] / sum(monies)
        oi_share = far / (near + far)
        history = self._roll.setdefault(key, deque())
        cutoff = at - timedelta(seconds=self.settings.roll_window_seconds)
        anchor = next((point for point in reversed(history) if point[0] <= cutoff), None)
        gate = False
        extra: dict[str, Any] = {"oi_far_share": oi_share, "volume_far_share": volume_share, "metric_unit": "oi_share"}
        if anchor and (cutoff - anchor[0]).total_seconds() <= self.settings.sample_interval_seconds * 2:
            near_delta, far_delta = near - anchor[1], far - anchor[2]
            matched = min(-near_delta, far_delta)
            oi_change = oi_share - anchor[2] / (anchor[1] + anchor[2])
            volume_change = volume_share - anchor[3]
            gate = (
                near_delta < 0 < far_delta
                and matched >= self.settings.min_roll_matched_contracts
                and matched >= self.settings.min_roll_relative_oi_change * (anchor[1] + anchor[2])
                and oi_change >= self.settings.min_roll_oi_share_change
                and volume_change >= self.settings.min_roll_volume_share_change
                and abs(spread_delta) >= self.settings.min_roll_spread_deviation_bps
                and abs(spread_delta) >= cost * self.settings.execution_cost_multiplier
            )
            extra.update(near_oi_delta=near_delta, far_oi_delta=far_delta, matched_contracts=matched, oi_share_change=oi_change, volume_share_change=volume_change, roll_window_seconds=self.settings.roll_window_seconds, spread_deviation_bps=spread_delta)
        candidate = self._check(signal_type="roll_migration_anomaly", contracts=pair, at=at, value=oi_share, floor=self.settings.roll_dispersion_floor, min_deviation=self.settings.min_roll_oi_share_change, deviation_bps_scale=10_000, additional_gate=gate, direction_positive="migration_far", direction_negative="migration_near", details=extra)
        if candidate:
            result.append(candidate)
        if not history or (at - history[-1][0]).total_seconds() >= self.settings.sample_interval_seconds:
            history.append((at, near, far, volume_share))
            while len(history) > 2 and history[1][0] <= cutoff - timedelta(seconds=self.settings.sample_interval_seconds * 2):
                history.popleft()

    def _kink_candidates(self, contracts: tuple[CurveContract, ...], at: datetime, now: datetime) -> list[_Candidate]:
        quotes = self._quotes(contracts, at, now)
        if quotes is None:
            self._clear_pending_group(contracts)
            return []
        mids, cost = quotes
        total = (contracts[2].expiration_at - contracts[0].expiration_at).total_seconds()
        far_weight = (contracts[1].expiration_at - contracts[0].expiration_at).total_seconds() / total
        near_weight = 1 - far_weight
        value = (math.log(mids[1]) - near_weight * math.log(mids[0]) - far_weight * math.log(mids[2])) * 10_000
        candidate = self._check(signal_type="futures_curve_kink", contracts=contracts, at=at, value=value, floor=self.settings.dispersion_floor_bps, min_deviation=self.settings.min_kink_deviation_bps, cost_bps=cost, direction_positive="middle_rich", direction_negative="middle_cheap", details={"near_weight": near_weight, "far_weight": far_weight, "metric_unit": "bps", "execution_cost_bps": cost})
        return [candidate] if candidate else []

    def _signal(self, candidate: _Candidate, at: datetime, now: datetime) -> CurveSignal:
        monies, counts, _ = self._activity(candidate.contracts, at)
        legs = []
        times = []
        for index, contract in enumerate(candidate.contracts):
            state = self._legs[contract.instrument_id]
            quote = state.quote
            assert quote is not None
            times.append(quote[0])
            legs.append({"ticker": contract.ticker, "instrument_id": contract.instrument_id, "expiration_at": contract.expiration_at.isoformat(), "mid": quote[1] + (quote[2] - quote[1]) / 2, "bid": quote[1], "ask": quote[2], "notional_rub": monies[index], "trade_count": counts[index], "open_interest": state.oi[1] if state.oi else None})
        return CurveSignal(
            candidate.signal_type, candidate.contracts[0].chain_id,
            candidate.contracts[0].instrument_id, candidate.contracts[0].ticker,
            at, candidate.metric, candidate.baseline, candidate.score,
            {"curve_strong": True, "curve_version": "futures_curve_v1", "chain_id": candidate.contracts[0].chain_id, "chain_key": self._group_key(candidate.contracts), "legs": legs, "score": abs(candidate.score), "deviation_bps": candidate.deviation_bps, "window_notional": sum(monies), "window_notional_currency": "RUB", "activity_window_seconds": self.settings.activity_window_seconds, "quote_age_seconds": max((now - stamp).total_seconds() for stamp in times), "quote_skew_seconds": (max(times) - min(times)).total_seconds(), "baseline_points": candidate.baseline_count, "persistence_seconds": self.settings.persistence_seconds, "direction": candidate.direction, "evidence_status": "experimental", **candidate.details},
        )

    def export_state(self) -> dict[str, Any]:
        """Return a JSON-compatible checkpoint; callers control commit cadence."""
        return {
            "format_version": "futures-curve-state-v1",
            "fingerprint": self._fingerprint,
            "as_of": self._as_of.isoformat() if self._as_of else None,
            "legs": {
                key: {
                    "quote": [value.quote[0].isoformat(), *value.quote[1:]] if value.quote else None,
                    "oi": [value.oi[0].isoformat(), value.oi[1]] if value.oi else None,
                    "trades": list(value.trades),
                    "last_at": {kind: stamp.isoformat() for kind, stamp in value.last_at.items()},
                    "seen": list(value.seen),
                }
                for key, value in self._legs.items()
            },
            "history": {key: [[stamp.isoformat(), value] for stamp, value in points] for key, points in self._history.items()},
            "prices": {key: [[point[0].isoformat(), *point[1:]] for point in points] for key, points in self._prices.items()},
            "roll": {key: [[point[0].isoformat(), *point[1:]] for point in points] for key, points in self._roll.items()},
            "last_sample": {key: stamp.isoformat() for key, stamp in self._last_sample.items()},
            "cooldowns": {key: stamp.isoformat() for key, stamp in self._cooldowns.items()},
        }

    def restore_state(self, raw: dict[str, Any]) -> bool:
        """Fail closed on incompatible checkpoints; persistence restarts fresh."""
        self._reset()
        try:
            if raw.get("format_version") != "futures-curve-state-v1" or raw.get("fingerprint") != self._fingerprint:
                return False
            self._as_of = _dt(raw["as_of"]) if raw.get("as_of") else None
            if set(raw["legs"]) != set(self.contracts):
                raise ValueError("curve state contracts differ")
            for key, saved in raw["legs"].items():
                quote, oi = saved["quote"], saved["oi"]
                state = self._legs[key]
                if quote:
                    if len(quote) != 3 or not all(_finite(value) for value in quote[1:]) or not 0 < quote[1] < quote[2]:
                        raise ValueError("invalid quote checkpoint")
                    state.quote = (_dt(quote[0]), quote[1], quote[2])
                if oi:
                    if len(oi) != 2 or not _finite(oi[1]) or oi[1] < 0:
                        raise ValueError("invalid OI checkpoint")
                    state.oi = (_dt(oi[0]), oi[1])
                if len(saved["trades"]) > self.settings.activity_window_seconds + 1:
                    raise ValueError("oversized trade state")
                for bucket, money, count in saved["trades"]:
                    if not all(_finite(value) and value >= 0 for value in (bucket, money, count)):
                        raise ValueError("invalid activity checkpoint")
                    state.trades.append((int(bucket), float(money), int(count)))
                state.last_at = {kind: _dt(stamp) for kind, stamp in saved["last_at"].items()}
                state.seen = deque(str(value) for value in saved["seen"][-20_000:])
                state.seen_set = set(state.seen)
            self._history = self._read_points(raw["history"], 2, self.settings.baseline_points)
            max_prices = int(self.settings.momentum_horizon_seconds / self.settings.sample_interval_seconds) + 5
            self._prices = self._read_points(raw["prices"], 3, max_prices)
            max_roll = int(self.settings.roll_window_seconds / self.settings.sample_interval_seconds) + 5
            self._roll = self._read_points(raw["roll"], 4, max_roll)
            self._last_sample = {key: _dt(stamp) for key, stamp in raw["last_sample"].items()}
            self._cooldowns = {key: _dt(stamp) for key, stamp in raw["cooldowns"].items()}
        except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
            self._reset()
            return False
        self.state_revision += 1
        return True

    @staticmethod
    def _read_points(raw: dict[str, list], width: int, maximum: int) -> dict[str, deque]:
        result = {}
        for key, values in raw.items():
            if len(values) > maximum:
                raise ValueError("oversized curve checkpoint")
            points = deque(maxlen=maximum)
            previous = None
            for point in values:
                if len(point) != width or not all(_finite(value) for value in point[1:]):
                    raise ValueError("invalid curve checkpoint value")
                stamp = _dt(point[0])
                if previous is not None and stamp < previous:
                    raise ValueError("unordered curve checkpoint")
                points.append((stamp, *point[1:]))
                previous = stamp
            result[key] = points
        return result

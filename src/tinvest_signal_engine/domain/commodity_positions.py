"""Comparable position changes within a named source, market and trader group."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


DIGEST_VERSION = "commodity_positions_v1"
WEEKLY_CATEGORIES = ("managed_money", "producers_merchants", "swap_dealers", "other_reportables", "nonreportable")


@dataclass(frozen=True)
class TraderPositionChange:
    category: str
    longs: int
    shorts: int
    previous_longs: int
    previous_shorts: int
    spreading: int = 0
    previous_spreading: int = 0

    def __post_init__(self) -> None:
        if self.category not in WEEKLY_CATEGORIES:
            raise ValueError("unknown trader category")
        values = (self.longs, self.shorts, self.previous_longs, self.previous_shorts,
                  self.spreading, self.previous_spreading)
        if any(type(x) is not int or not 0 <= x < 2**63 for x in values):
            raise ValueError("position quantities must be non-negative integers")

    @property
    def net(self) -> int:
        return self.longs - self.shorts

    @property
    def net_change(self) -> int:
        return self.net - (self.previous_longs - self.previous_shorts)

    @property
    def preference(self) -> str:
        previous = self.previous_longs - self.previous_shorts
        if self.net == previous:
            return "unchanged"
        if previous <= 0 < self.net:
            return "flip_long"
        if previous >= 0 > self.net:
            return "flip_short"
        if self.net > 0:
            return "long_strengthens" if self.net_change > 0 else "long_weakens"
        if self.net < 0:
            return "short_strengthens" if self.net_change < 0 else "short_weakens"
        return "flat"


@dataclass(frozen=True)
class MarketPositionComparison:
    source: str
    market_id: str
    commodity: str
    trading_day: date
    previous_day: date
    open_interest: int
    previous_open_interest: int
    groups: tuple[TraderPositionChange, ...]
    source_url: str
    source_sha256: str

    def __post_init__(self) -> None:
        if self.source not in {"CFTC", "ICE"} or not self.market_id:
            raise ValueError("unknown weekly position source")
        if self.previous_day >= self.trading_day:
            raise ValueError("weekly comparison requires an earlier report")
        if {x.category for x in self.groups} != set(WEEKLY_CATEGORIES) or len(self.groups) != 5:
            raise ValueError("all five trader categories are required")
        for previous in (False, True):
            oi = self.previous_open_interest if previous else self.open_interest
            if type(oi) is not int or oi <= 0:
                raise ValueError("weekly open interest must be positive")
            longs = sum((g.previous_longs + g.previous_spreading) if previous else (g.longs + g.spreading)
                        for g in self.groups)
            shorts = sum((g.previous_shorts + g.previous_spreading) if previous else (g.shorts + g.spreading)
                         for g in self.groups)
            if longs != oi or shorts != oi:
                raise ValueError("weekly totals including spreading do not reconcile with open interest")


@dataclass(frozen=True)
class PositionContext:
    markets: tuple[MarketPositionComparison, ...] = ()
    unavailable_sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        keys = [(x.source, x.market_id) for x in self.markets]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate position market")

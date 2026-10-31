"""Daily exchange position totals, independent of transport and storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessDecision


PRODUCTS = ("au", "ag")
CATEGORIES = ("broker_clients", "direct_members")
REPORT_VERSION = "shfe_daily_v1"
REPORT_SIGNAL_TYPE = "daily_futures_position_report"


@dataclass(frozen=True)
class PositionTotal:
    product: str
    category: str
    longs: int
    shorts: int
    long_change: int
    short_change: int

    def __post_init__(self) -> None:
        if self.product not in PRODUCTS or self.category not in CATEGORIES:
            raise ValueError("unknown daily position group")
        values = (self.longs, self.shorts, self.long_change, self.short_change)
        if any(type(value) is not int for value in values):
            raise ValueError("position quantities must be integers")
        if min(self.longs, self.shorts) < 0:
            raise ValueError("position quantities must be non-negative")


@dataclass(frozen=True)
class PositionSnapshot:
    trading_day: date
    published_at: datetime
    totals: tuple[PositionTotal, ...]
    source_url: str
    source_sha256: str

    def __post_init__(self) -> None:
        if self.published_at.utcoffset() is None:
            raise ValueError("publication timestamp must be aware")
        if self.published_at.astimezone(ZoneInfo("Asia/Shanghai")).date() != self.trading_day:
            raise ValueError("publication date must match trading day")
        expected = {(p, c) for p in PRODUCTS for c in CATEGORIES}
        keys = [(item.product, item.category) for item in self.totals]
        if set(keys) != expected or len(keys) != len(expected):
            raise ValueError("both complete participant categories are required for each metal")
        for product in PRODUCTS:
            rows = [item for item in self.totals if item.product == product]
            longs, shorts = sum(x.longs for x in rows), sum(x.shorts for x in rows)
            if longs != shorts or longs == 0:
                raise ValueError("exchange-wide long and short totals must balance and be nonzero")


@dataclass(frozen=True)
class DailyPositionComparison:
    current: PositionSnapshot
    previous: PositionSnapshot

    def __post_init__(self) -> None:
        if self.current.trading_day <= self.previous.trading_day:
            raise ValueError("comparison requires an earlier trading session")
        previous = {(x.product, x.category): x for x in self.previous.totals}
        for row in self.current.totals:
            old = previous[row.product, row.category]
            if (row.longs - old.longs, row.shorts - old.shorts) != (row.long_change, row.short_change):
                raise ValueError("daily changes disagree with the previous exchange report")


def daily_report_freshness(
    *, trading_day: date, previous_day: date, generated_at: datetime, evaluated_at: datetime,
) -> DeliveryFreshnessDecision:
    """A dated daily bulletin has its own bounded delivery window, not a tick's TTL."""
    if generated_at.utcoffset() is None or evaluated_at.utcoffset() is None:
        raise ValueError("daily report timestamps must be aware")
    today = evaluated_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
    age = (evaluated_at - generated_at).total_seconds()
    valid_dates = previous_day < trading_day <= today and (today - trading_day).days <= 7
    allowed = valid_dates and -5 <= age <= 21_600
    return DeliveryFreshnessDecision(
        allow_external_delivery=allowed,
        reason_code="daily_report_fresh" if allowed else "daily_report_stale_or_invalid",
        event_age_seconds=age, maximum_event_age_seconds=21_600,
        source_session=trading_day.isoformat(), evaluated_session=today.isoformat(),
    )

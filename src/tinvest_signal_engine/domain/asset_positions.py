"""One commodity's positions, with independent dates for each venue."""

from dataclasses import dataclass
from datetime import date, datetime, timezone

from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessDecision


ASSET_REPORT_VERSION = "commodity_asset_v1"


@dataclass(frozen=True)
class AssetPositionGroup:
    category: str
    longs: int
    shorts: int
    previous_longs: int
    previous_shorts: int
    spreading: int = 0
    previous_spreading: int = 0

    def __post_init__(self) -> None:
        if not self.category or any(type(v) is not int or v < 0 for v in (
            self.longs, self.shorts, self.previous_longs, self.previous_shorts, self.spreading, self.previous_spreading,
        )):
            raise ValueError("asset positions require non-negative integer counts")

    @property
    def net(self) -> int:
        return self.longs - self.shorts

    @property
    def net_change(self) -> int:
        return self.net - self.previous_longs + self.previous_shorts


@dataclass(frozen=True)
class AssetPositionSection:
    source: str
    market_id: str
    period: str
    trading_day: date
    previous_day: date
    source_event_at: datetime
    groups: tuple[AssetPositionGroup, ...]
    source_url: str
    source_sha256: str
    previous_source_sha256: str | None = None
    open_interest: int | None = None
    previous_open_interest: int | None = None

    def __post_init__(self) -> None:
        if (self.source, self.period) not in {("SHFE", "daily"), ("CFTC", "weekly"), ("ICE", "weekly")}:
            raise ValueError("unsupported asset report source and period")
        if self.previous_day >= self.trading_day or self.source_event_at.utcoffset() is None:
            raise ValueError("asset report requires ordered dates and an aware source timestamp")
        if not self.groups or len({g.category for g in self.groups}) != len(self.groups):
            raise ValueError("asset report requires distinct participant groups")
        if self.period == "weekly":
            for previous in (False, True):
                oi = self.previous_open_interest if previous else self.open_interest
                if type(oi) is not int or oi <= 0:
                    raise ValueError("weekly asset report requires positive open interest")
                longs = sum(g.previous_longs + g.previous_spreading if previous else g.longs + g.spreading for g in self.groups)
                shorts = sum(g.previous_shorts + g.previous_spreading if previous else g.shorts + g.spreading for g in self.groups)
                if longs != oi or shorts != oi:
                    raise ValueError("asset report totals must reconcile with open interest")


@dataclass(frozen=True)
class AssetPositionReport:
    commodity: str
    sections: tuple[AssetPositionSection, ...]
    unavailable_sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.commodity or not self.sections:
            raise ValueError("asset report requires a commodity and source data")
        if len({(s.source, s.market_id) for s in self.sections}) != len(self.sections):
            raise ValueError("duplicate asset report source")


def asset_report_freshness(*, trading_day: date, previous_day: date, period: str,
                           generated_at: datetime, evaluated_at: datetime) -> DeliveryFreshnessDecision:
    if generated_at.utcoffset() is None or evaluated_at.utcoffset() is None:
        raise ValueError("asset report timestamps must be aware")
    today = evaluated_at.astimezone(timezone.utc).date()
    age = (evaluated_at - generated_at).total_seconds()
    maximum_days = {"daily": 7, "weekly": 14}.get(period, -1)
    valid_dates = previous_day < trading_day <= today and (today - trading_day).days <= maximum_days
    allowed = valid_dates and -5 <= age <= 21_600
    return DeliveryFreshnessDecision(allowed, "asset_report_fresh" if allowed else "asset_report_stale_or_invalid",
                                     age, 21_600, trading_day.isoformat(), today.isoformat())

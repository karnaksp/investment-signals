"""Group independently validated exchange comparisons by commodity."""

from datetime import datetime, time, timezone

from tinvest_signal_engine.domain.asset_positions import AssetPositionGroup, AssetPositionReport, AssetPositionSection
from tinvest_signal_engine.domain.commodity_positions import PositionContext
from tinvest_signal_engine.domain.daily_positions import DailyPositionComparison, PRODUCTS


def build_asset_position_reports(comparison: DailyPositionComparison,
                                 context: PositionContext | None) -> tuple[AssetPositionReport, ...]:
    sections: dict[str, list[AssetPositionSection]] = {}
    current, previous = comparison.current, comparison.previous
    old = {(g.product, g.category): g for g in previous.totals}
    for product in PRODUCTS:
        commodity = {"au": "gold", "ag": "silver"}[product]
        groups = tuple(AssetPositionGroup(g.category, g.longs, g.shorts,
                                         old[product, g.category].longs, old[product, g.category].shorts)
                       for g in current.totals if g.product == product)
        sections[commodity] = [AssetPositionSection(
            "SHFE", product, "daily", current.trading_day, previous.trading_day, current.published_at,
            groups, current.source_url, current.source_sha256, previous.source_sha256,
        )]
    if context:
        for market in context.markets:
            groups = tuple(AssetPositionGroup(g.category, g.longs, g.shorts, g.previous_longs, g.previous_shorts,
                                              g.spreading, g.previous_spreading) for g in market.groups)
            sections.setdefault(market.commodity, []).append(AssetPositionSection(
                market.source, market.market_id, "weekly", market.trading_day, market.previous_day,
                datetime.combine(market.trading_day, time.min, timezone.utc), groups,
                market.source_url, market.source_sha256, open_interest=market.open_interest,
                previous_open_interest=market.previous_open_interest,
            ))
    reports = []
    for commodity, sources in sections.items():
        expected = "ICE" if commodity == "brent" else "CFTC"
        unavailable = tuple(name for name in (context.unavailable_sources if context else ())
                            if name == expected or name.partition(":")[2] == commodity)
        reports.append(AssetPositionReport(commodity, tuple(sources), unavailable))
    return tuple(reports)

"""Пересчёт сезонных baseline в ClickHouse (Dagster / cron / ручной запуск)."""

from __future__ import annotations

import argparse

from ..config import RuntimeSettings
from ..historical_baselines import run_historical_baseline_recalc
from ..logging_utils import configure_logging


def run_once(
    settings: RuntimeSettings | None = None,
    *,
    force_truncate_and_seed_from_features: bool = False,
    seed_if_empty: bool | None = None,
    incremental_days: int | None = None,
) -> None:
    cfg = settings or RuntimeSettings.from_env()
    configure_logging(cfg.log_level)
    url = (cfg.clickhouse_http_url or "").strip()
    if not url:
        raise RuntimeError("CLICKHOUSE_HTTP_URL не задан — пересчёт baseline невозможен")
    run_historical_baseline_recalc(
        base_url=url,
        username=cfg.clickhouse_http_username,
        password=cfg.clickhouse_http_password,
        lookback_days=cfg.historical_baseline_lookback_days,
        incremental_days=(
            incremental_days
            if incremental_days is not None
            else cfg.historical_baseline_incremental_days
        ),
        seed_trade_slot_if_empty=(
            cfg.historical_baseline_seed_if_empty
            if seed_if_empty is None
            else seed_if_empty
        ),
        force_truncate_and_seed_from_features=force_truncate_and_seed_from_features,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ClickHouse: trade_slot_daily (инкремент из raw) + historical_baseline_slot_stats.",
    )
    parser.add_argument(
        "--force-seed-features",
        action="store_true",
        help="TRUNCATE trade_slot_daily и залить историю из features_trade_bar_* (VWAP как OHLC), затем обычный инкремент.",
    )
    parser.add_argument(
        "--no-seed-if-empty",
        action="store_true",
        help="Не делать первичный seed из features_trade_bar_* при пустой trade_slot_daily.",
    )
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=None,
        metavar="N",
        help="Сколько последних полных UTC-календарных дней пересобрать из market_raw_events (по умолчанию из env).",
    )
    args = parser.parse_args()
    run_once(
        force_truncate_and_seed_from_features=bool(args.force_seed_features),
        seed_if_empty=not bool(args.no_seed_if_empty),
        incremental_days=args.incremental_days,
    )


if __name__ == "__main__":
    main()

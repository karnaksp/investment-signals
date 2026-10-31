"""Dedicated Dagster code location for the nightly futures-universe refresh."""

from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
)

from tinvest_signal_engine.services.futures_universe_refresh import run_once
from tinvest_signal_engine.services.daily_positions import run_once as collect_daily_positions
from tinvest_signal_engine.services.ruble_positions import run_once as collect_ruble_positions


DEFAULT_FUTURES_REFRESH_CRON = "30 1 * * *"


@op(
    name="refresh_liquid_futures_universe",
    tags={"component": "futures-universe"},
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def refresh_liquid_futures_universe_op(context) -> None:
    receipt = run_once()
    context.log.info(
        "Futures universe refreshed changed=%s contracts=%s",
        receipt.changed,
        ", ".join(
            f"{item.alias}={item.ticker}@{item.expiration_at.date()}"
            for item in receipt.resolved
        ),
    )


@job(name="refresh_liquid_futures_universe_job")
def refresh_liquid_futures_universe_job() -> None:
    refresh_liquid_futures_universe_op()


nightly_futures_universe_schedule = ScheduleDefinition(
    name="nightly_liquid_futures_universe_refresh",
    job=refresh_liquid_futures_universe_job,
    cron_schedule=(
        (os.getenv("DAGSTER_FUTURES_REFRESH_CRON") or "").strip()
        or DEFAULT_FUTURES_REFRESH_CRON
    ),
    execution_timezone=(
        (os.getenv("DAGSTER_FUTURES_REFRESH_TIMEZONE") or "").strip()
        or "Europe/Moscow"
    ),
    default_status=DefaultScheduleStatus.RUNNING,
)


@op(name="collect_shfe_daily_positions", tags={"component": "daily-positions"},
    retry_policy=RetryPolicy(max_retries=2, delay=60))
def collect_shfe_daily_positions_op(context) -> None:
    receipt = collect_daily_positions()
    context.log.info("SHFE daily positions: %s, %s vs %s",
                     receipt.status, receipt.trading_day, receipt.previous_day)


@job(name="shfe_daily_positions_job")
def shfe_daily_positions_job() -> None:
    collect_shfe_daily_positions_op()


shfe_daily_positions_schedule = ScheduleDefinition(
    name="shfe_daily_positions", job=shfe_daily_positions_job,
    cron_schedule="0 13,15 * * 1-5", execution_timezone="Europe/Moscow",
    default_status=DefaultScheduleStatus.RUNNING,
)


@op(name="collect_ruble_daily_positions", tags={"component": "ruble-positions"},
    retry_policy=RetryPolicy(max_retries=2, delay=60))
def collect_ruble_daily_positions_op(context):
    receipt = collect_ruble_positions()
    context.log.info("Ruble positions: %s, pairs=%s, unavailable=%s", receipt.status, receipt.pairs, receipt.unavailable_pairs)
    if receipt.unavailable_pairs:
        raise RuntimeError("Some ruble reports are unavailable; retrying without resending saved reports")


@job(name="ruble_daily_positions_job")
def ruble_daily_positions_job():
    collect_ruble_daily_positions_op()


ruble_daily_positions_schedule = ScheduleDefinition(
    name="ruble_daily_positions", job=ruble_daily_positions_job,
    cron_schedule="30 0,9 * * *", execution_timezone="Europe/Moscow",
    default_status=DefaultScheduleStatus.RUNNING,
)


defs = Definitions(
    jobs=[refresh_liquid_futures_universe_job, shfe_daily_positions_job, ruble_daily_positions_job],
    schedules=[nightly_futures_universe_schedule, shfe_daily_positions_schedule, ruble_daily_positions_schedule],
)

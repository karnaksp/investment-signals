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


defs = Definitions(
    jobs=[refresh_liquid_futures_universe_job],
    schedules=[nightly_futures_universe_schedule],
)

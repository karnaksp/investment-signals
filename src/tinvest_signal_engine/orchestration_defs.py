"""Определения Dagster: расписания вместо отдельного threshold-cron / unary-цикла.

Запуск UI и демона (Docker см. docker-compose):

    dagster-webserver -h 0.0.0.0 -p 3000 -m tinvest_signal_engine.orchestration_defs
    dagster-daemon run -m tinvest_signal_engine.orchestration_defs

Переменные окружения те же, что у остальных сервисов (TINVEST_TOKEN, KAFKA_*, conf/).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from typing import Final

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    Failure,
    ScheduleDefinition,
    job,
    op,
)

from .logging_utils import configure_logging
from .services.bond_convergence_emitter import run_once as run_bond_convergence_once
from .services.daily_adaptive_calibration import run_once as run_adaptive_calibration_once
from .services.threshold_cron import run_recalc_once


@op(name="threshold_recalc_op", tags={"component": "threshold"})
def threshold_recalc_op(context) -> None:
    """Пересчёт price_move_absolute_threshold_bps → ``detectors.overrides.yaml``."""
    from .config import RuntimeSettings

    settings = RuntimeSettings.from_env(service_name="dagster")
    configure_logging(settings.log_level)
    if not (settings.tinvest_token or "").strip():
        raise Failure("TINVEST_TOKEN не задан: пересчёт порогов невозможен")
    context.log.info(
        "Пересчёт порогов: overrides=%s lookback_days=%s",
        settings.detector_overrides_path,
        settings.threshold_lookback_days,
    )
    run_recalc_once(settings)


@op(name="daily_adaptive_calibration_op", tags={"component": "calibration"})
def daily_adaptive_calibration_op(context) -> None:
    """Adapt price-jump gates from mature local outcomes after the session."""
    from .config import RuntimeSettings

    settings = RuntimeSettings.from_env(service_name="dagster")
    decision = run_adaptive_calibration_once(settings)
    context.log.info(
        "Daily adaptive calibration: status=%s reason=%s version=%s candidate=%s",
        decision.status,
        decision.reason_code,
        decision.version,
        decision.candidate,
    )


@op(name="unary_kafka_poll_once_op", tags={"component": "unary"})
def unary_kafka_poll_once_op(context) -> None:
    """Один цикл unary → Kafka (см. MARKET_UNARY_*); процесс завершается после цикла."""
    exe = shutil.which("tinvest-market-unary-emitter")
    cmd: list[str] = (
        [exe]
        if exe
        else [
            sys.executable,
            "-m",
            "tinvest_signal_engine.services.market_unary_emitter",
        ]
    )
    env = {**os.environ, "MARKET_UNARY_SINGLE_SHOT": "1"}
    poll_raw = (env.get("MARKET_UNARY_POLL_SECONDS") or "").strip()
    try:
        poll_ok = int(poll_raw) > 0 if poll_raw else False
    except ValueError:
        poll_ok = False
    if not poll_ok:
        env["MARKET_UNARY_POLL_SECONDS"] = "300"
    context.log.info("Запуск unary single-shot: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, env=env)


@op(name="bond_convergence_scan_op", tags={"component": "bond-convergence"})
def bond_convergence_scan_op(context) -> None:
    """Find bonds at the fixed pre-maturity session and publish candidates."""
    from .config import RuntimeSettings

    settings = RuntimeSettings.from_env(service_name="dagster")
    configure_logging(settings.log_level)
    receipt = run_bond_convergence_once(settings)
    context.log.info(
        "Проверено облигаций: %s; опубликовано сигналов: %s; отклонено: %s",
        receipt.inspected,
        receipt.published,
        receipt.rejected,
    )


@job(name="threshold_recalc_job")
def threshold_recalc_job() -> None:
    threshold_recalc_op()


@job(name="daily_adaptive_calibration_job")
def daily_adaptive_calibration_job() -> None:
    daily_adaptive_calibration_op()


@job(name="unary_kafka_poll_once_job")
def unary_kafka_poll_once_job() -> None:
    unary_kafka_poll_once_op()


@job(name="bond_convergence_scan_job")
def bond_convergence_scan_job() -> None:
    bond_convergence_scan_op()


_DEFAULT_THRESHOLD_CRON: Final[str] = "0 2 * * *"
_DEFAULT_ADAPTIVE_CALIBRATION_CRON: Final[str] = "20 23 * * 1-5"
_DEFAULT_UNARY_CRON: Final[str] = "*/15 * * * *"
_DEFAULT_BOND_CONVERGENCE_CRON: Final[str] = "15 11 * * 1-5"


def _threshold_cron_schedule() -> str:
    raw = (os.getenv("DAGSTER_THRESHOLD_CRON") or "").strip()
    return raw or _DEFAULT_THRESHOLD_CRON


def _unary_cron_schedule() -> str:
    raw = (os.getenv("DAGSTER_UNARY_CRON") or "").strip()
    return raw or _DEFAULT_UNARY_CRON


daily_threshold_schedule = ScheduleDefinition(
    name="daily_threshold_recalc",
    job=threshold_recalc_job,
    cron_schedule=_threshold_cron_schedule(),
    # Historical threshold calibration is an explicit research/maintenance
    # operation.  Starting the application must never trigger a replay.
    default_status=DefaultScheduleStatus.STOPPED,
)

daily_adaptive_calibration_schedule = ScheduleDefinition(
    name="daily_adaptive_calibration",
    job=daily_adaptive_calibration_job,
    cron_schedule=(
        (os.getenv("DAGSTER_ADAPTIVE_CALIBRATION_CRON") or "").strip()
        or _DEFAULT_ADAPTIVE_CALIBRATION_CRON
    ),
    execution_timezone="Europe/Moscow",
    default_status=DefaultScheduleStatus.RUNNING,
)

quarter_hourly_unary_schedule = ScheduleDefinition(
    name="market_unary_poll_schedule",
    job=unary_kafka_poll_once_job,
    cron_schedule=_unary_cron_schedule(),
    # The streaming ingestor owns the live feed.  Keep this compatibility
    # poller opt-in so it cannot duplicate traffic after an application start.
    default_status=DefaultScheduleStatus.STOPPED,
)

daily_bond_convergence_schedule = ScheduleDefinition(
    name="daily_bond_convergence_scan",
    job=bond_convergence_scan_job,
    cron_schedule=(
        (os.getenv("DAGSTER_BOND_CONVERGENCE_CRON") or "").strip()
        or _DEFAULT_BOND_CONVERGENCE_CRON
    ),
    execution_timezone="Europe/Moscow",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    jobs=[
        threshold_recalc_job,
        daily_adaptive_calibration_job,
        unary_kafka_poll_once_job,
        bond_convergence_scan_job,
    ],
    schedules=[
        daily_threshold_schedule,
        daily_adaptive_calibration_schedule,
        quarter_hourly_unary_schedule,
        daily_bond_convergence_schedule,
    ],
)

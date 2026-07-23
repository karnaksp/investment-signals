"""Composition root for automatic signal outcome evaluation."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from random import random
from threading import Event
import time
from typing import Any, Callable

from psycopg import connect

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
    TransientClickHouseError,
)
from tinvest_signal_engine.adapters.dependency_recovery import (
    DependencyRecoveryMetrics,
    NoopDependencyRecoveryMetrics,
    record_dependency_recovered,
    wait_for_dependency,
)
from tinvest_signal_engine.adapters.clickhouse_reference_ticks import (
    ClickHouseReferenceTickReader,
)
from tinvest_signal_engine.adapters.postgres_signal_outcomes import (
    PostgresDirectionalSignalOutcomeCandidateSource,
    PostgresSignalOutcomeStore,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.application.signal_outcomes import (
    DirectionalSignalOutcomeBatchProcessor,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.signal_outcomes import DirectionalOutcomePolicy
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import (
    graceful_shutdown_event,
)


logger = logging.getLogger(__name__)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="signal_outcome_worker")
    configure_logging(settings.log_level)
    if not settings.clickhouse_http_url:
        raise RuntimeError("CLICKHOUSE_HTTP_URL is required")
    if not settings.clickhouse_http_username:
        raise RuntimeError("CLICKHOUSE_USERNAME is required")
    if not settings.clickhouse_http_password:
        raise RuntimeError(
            "CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required"
        )

    policy = outcome_policy_from_env()
    worker = DirectionalSignalOutcomeBatchProcessor(
        candidates=PostgresDirectionalSignalOutcomeCandidateSource(
            _connect_postgres(settings),
            policy=policy,
        ),
        ticks=ClickHouseReferenceTickReader(
            base_url=settings.clickhouse_http_url,
            database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
            username=settings.clickhouse_http_username,
            password=settings.clickhouse_http_password,
            timeout_seconds=_env_float(
                "SIGNAL_OUTCOME_CLICKHOUSE_TIMEOUT_SECONDS",
                15.0,
            ),
            limit=_env_int("SIGNAL_OUTCOME_REFERENCE_TICK_LIMIT", 20_000),
        ),
        store=PostgresSignalOutcomeStore(_connect_postgres(settings)),
    )
    batch_size = _env_int("SIGNAL_OUTCOME_WORKER_BATCH_SIZE", 100)
    poll_seconds = _env_float("SIGNAL_OUTCOME_WORKER_POLL_SECONDS", 5.0)
    start_reliability_metrics_server(settings.metrics_listen_port)
    metrics = PrometheusReliabilityMetrics()

    logger.info("Starting automatic signal outcome worker")
    with graceful_shutdown_event(
        logger=logger,
        worker="signal_outcome_worker",
    ) as stop_event:
        run_worker_loop(
            worker,
            batch_size=batch_size,
            poll_seconds=poll_seconds,
            stop_event=stop_event,
            metrics=metrics,
        )


def run_worker_loop(
    worker: DirectionalSignalOutcomeBatchProcessor,
    *,
    batch_size: int,
    poll_seconds: float,
    stop_event: Event,
    metrics: DependencyRecoveryMetrics | None = None,
    backoff: BoundedExponentialBackoff = BoundedExponentialBackoff(),
    now: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    random_value: Callable[[], float] = random,
) -> None:
    """Process due outcomes while ClickHouse recovers and shutdown stays responsive."""

    recovery_metrics = metrics or NoopDependencyRecoveryMetrics()
    consecutive_failures = 0
    while not stop_event.is_set():
        try:
            result = worker.process_due(
                now=now(),
                limit=batch_size,
            )
        except TransientClickHouseError as error:
            consecutive_failures += 1
            if wait_for_dependency(
                worker="signal_outcome_worker",
                error=error,
                consecutive_failures=consecutive_failures,
                stop_event=stop_event,
                backoff=backoff,
                metrics=recovery_metrics,
                logger=logger,
                random_value=random_value,
            ):
                break
            continue
        record_dependency_recovered(
            worker="signal_outcome_worker",
            operation="reference_tick_select",
            consecutive_failures=consecutive_failures,
            metrics=recovery_metrics,
            logger=logger,
        )
        consecutive_failures = 0
        if result.scanned:
            logger.info(
                "Processed signal outcome batch",
                extra={
                    "scanned": result.scanned,
                    "stored": result.stored,
                    "pending": result.pending,
                    "reason_counts": dict(result.reason_counts),
                },
            )
        if result.scanned == 0:
            stop_event.wait(poll_seconds)


def outcome_policy_from_env() -> DirectionalOutcomePolicy:
    return DirectionalOutcomePolicy(
        policy_version=_env_str("SIGNAL_OUTCOME_POLICY_VERSION", "directional-v1"),
        cost_model_version=_env_str("SIGNAL_OUTCOME_COST_MODEL_VERSION", "cost-v1"),
        horizon_seconds=_env_int("SIGNAL_OUTCOME_HORIZON_SECONDS", 300),
        anchor_max_age_seconds=_env_int("SIGNAL_OUTCOME_ANCHOR_MAX_AGE_SECONDS", 5),
        forward_grace_seconds=_env_int("SIGNAL_OUTCOME_FORWARD_GRACE_SECONDS", 30),
        min_move_bps=_env_decimal("SIGNAL_OUTCOME_MIN_MOVE_BPS", "5"),
        volatility_multiplier=_env_decimal(
            "SIGNAL_OUTCOME_VOLATILITY_MULTIPLIER",
            "0",
        ),
        round_trip_cost_bps=_env_decimal(
            "SIGNAL_OUTCOME_ROUND_TRIP_COST_BPS",
            "1",
        ),
    )


def _connect_postgres(settings: RuntimeSettings) -> Any:
    deadline = time.monotonic() + settings.postgres_startup_timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            connection = connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                dbname=settings.postgres_database,
                user=settings.postgres_username,
                password=settings.postgres_password,
                autocommit=True,
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return connection
        except Exception as error:
            last_error = error
            time.sleep(settings.postgres_startup_check_interval_seconds)
    raise RuntimeError(
        "signal outcome worker could not connect to Postgres within "
        f"{settings.postgres_startup_timeout_seconds}s"
    ) from last_error


def _env_str(name: str, default: str) -> str:
    return (os.getenv(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    return float(raw) if raw else default


def _env_decimal(name: str, default: str) -> Decimal:
    return Decimal((os.getenv(name) or default).strip())

"""Composition root for durable detector-observation publication."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tinvest_signal_engine.adapters.clickhouse_detector_observations import (
    ClickHouseDetectorObservationSink,
)
from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_observation_publication_queue,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.adapters.worker_health_file import WorkerHealthFileSink
from tinvest_signal_engine.application.observation_publication import (
    DurableObservationPublisher,
)
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.market_schedule import MarketSchedule
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import graceful_shutdown_event

logger = logging.getLogger(__name__)


def validate_transport_timing(*, timeout_seconds: float, lease_seconds: int) -> None:
    if timeout_seconds <= 0:
        raise ValueError("observation ClickHouse timeout must be positive")
    if timeout_seconds >= lease_seconds:
        raise ValueError(
            "observation ClickHouse timeout must be shorter than claim lease"
        )


def should_purge_processed_events(
    *, now: datetime, market_schedule: MarketSchedule
) -> bool:
    """Keep storage maintenance outside the live collection window."""

    local_now = now.astimezone(market_schedule.timezone)
    return local_now.weekday() >= 5 or not market_schedule.is_collection_active(now)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="observation_worker")
    configure_logging(settings.log_level)
    start_reliability_metrics_server(settings.observation_worker_metrics_listen_port)
    if not settings.clickhouse_http_url:
        raise RuntimeError("CLICKHOUSE_HTTP_URL is required")
    if not settings.clickhouse_http_username:
        raise RuntimeError("CLICKHOUSE_USERNAME is required")
    if not settings.clickhouse_http_password:
        raise RuntimeError(
            "CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required"
        )
    validate_transport_timing(
        timeout_seconds=settings.observation_worker_clickhouse_timeout_seconds,
        lease_seconds=settings.observation_worker_claim_lease_seconds,
    )

    queue = connect_observation_publication_queue(settings)
    sink = ClickHouseDetectorObservationSink(
        base_url=settings.clickhouse_http_url,
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=settings.clickhouse_http_username,
        password=settings.clickhouse_http_password,
        timeout_seconds=(settings.observation_worker_clickhouse_timeout_seconds),
    )
    worker = DurableObservationPublisher(
        queue=queue,
        sink=sink,
        metrics=PrometheusReliabilityMetrics(),
        clock=lambda: datetime.now(tz=timezone.utc),
        lease_seconds=settings.observation_worker_claim_lease_seconds,
        batch_size=settings.observation_worker_batch_size,
        maximum_attempts=settings.observation_worker_max_attempts,
        retry_base_seconds=settings.observation_worker_retry_base_seconds,
        retry_maximum_seconds=settings.observation_worker_retry_max_seconds,
    )
    market_schedule = MarketSchedule.from_strings(
        timezone_name=settings.market_schedule_timezone,
        collection_start=settings.market_collection_start,
        collection_end=settings.market_collection_end,
        signal_start=settings.market_signal_start,
        signal_end=settings.market_signal_end,
    )
    logger.info("Starting detector observation publication worker")
    next_purge_at = 0.0
    health = WorkerHealthTracker(
        worker_id="observation_worker",
        sink=WorkerHealthFileSink(
            Path(
                os.getenv("OBSERVATION_HEALTH_SNAPSHOT_PATH")
                or "/tmp/observation-worker-health.json"
            )
        ),
        stale_after_seconds=int(
            os.getenv("OBSERVATION_HEALTH_STALE_AFTER_SECONDS") or "180"
        ),
    )
    try:
        with graceful_shutdown_event(
            logger=logger,
            worker="observation_worker",
        ) as stop_event:
            while not stop_event.is_set():
                health.heartbeat()
                try:
                    result = worker.run_once()
                    monotonic_now = time.monotonic()
                    if monotonic_now >= next_purge_at:
                        utc_now = datetime.now(tz=timezone.utc)
                        purged_observations = 0
                        purged_events = 0
                        if should_purge_processed_events(
                            now=utc_now,
                            market_schedule=market_schedule,
                        ):
                            purged_observations = queue.purge_published(
                                before=(
                                    utc_now
                                    - timedelta(
                                        hours=settings.observation_retention_hours
                                    )
                                ),
                                limit=settings.observation_purge_batch_size,
                            )
                            purged_events = queue.purge_processed_events(
                                before=(
                                    utc_now
                                    - timedelta(
                                        days=settings.processed_event_retention_days
                                    )
                                ),
                                limit=settings.processed_event_purge_batch_size,
                            )
                        if purged_observations or purged_events:
                            logger.info(
                                "Purged expired reliable-processing storage",
                                extra={
                                    "observation_rows": purged_observations,
                                    "processed_event_rows": purged_events,
                                },
                            )
                        next_purge_at = (
                            monotonic_now
                            + settings.processed_event_purge_interval_seconds
                        )
                except Exception:
                    health.failed("worker_cycle_failed")
                    raise
                health.succeeded(force=result.outcome != "idle")
                if result.outcome == "idle":
                    stop_event.wait(settings.observation_worker_poll_seconds)
    finally:
        queue.close()


if __name__ == "__main__":
    main()

"""Composition root for durable detector-observation publication."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

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
from tinvest_signal_engine.application.observation_publication import (
    DurableObservationPublisher,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)


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

    queue = connect_observation_publication_queue(settings)
    sink = ClickHouseDetectorObservationSink(
        base_url=settings.clickhouse_http_url,
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=settings.clickhouse_http_username,
        password=settings.clickhouse_http_password,
    )
    worker = DurableObservationPublisher(
        queue=queue,
        sink=sink,
        metrics=PrometheusReliabilityMetrics(),
        clock=lambda: datetime.now(tz=timezone.utc),
        lease_seconds=settings.observation_worker_claim_lease_seconds,
        maximum_attempts=settings.observation_worker_max_attempts,
        retry_base_seconds=settings.observation_worker_retry_base_seconds,
        retry_maximum_seconds=settings.observation_worker_retry_max_seconds,
    )
    logger.info("Starting detector observation publication worker")
    try:
        while True:
            result = worker.run_once()
            if result.outcome == "idle":
                time.sleep(settings.observation_worker_poll_seconds)
    except KeyboardInterrupt:
        logger.info("Detector observation publication worker stopped by user")
    finally:
        queue.close()


if __name__ == "__main__":
    main()

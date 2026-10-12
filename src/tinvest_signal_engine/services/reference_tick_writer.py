"""Composition root for normalized Kafka events to ClickHouse reference ticks."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from tinvest_signal_engine.adapters.clickhouse_reference_ticks import (
    ClickHouseReferenceTickStore,
)
from tinvest_signal_engine.adapters.kafka_reference_ticks import (
    ReferenceTickKafkaRuntime,
    build_reference_tick_consumer,
)
from tinvest_signal_engine.adapters.worker_health_file import WorkerHealthFileSink
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.application.reference_ticks import ReferenceTickProcessor
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.kafka_wire_config import validate_kafka_wire_settings
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import (
    graceful_shutdown_event,
)


logger = logging.getLogger(__name__)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="reference_tick_writer")
    validate_kafka_wire_settings(settings, check_signal=False)
    configure_logging(settings.log_level)
    if not settings.clickhouse_http_url:
        raise RuntimeError("CLICKHOUSE_HTTP_URL is required")
    if not settings.clickhouse_http_username:
        raise RuntimeError("CLICKHOUSE_USERNAME is required")
    if not settings.clickhouse_http_password:
        raise RuntimeError("CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required")
    start_reliability_metrics_server(settings.metrics_listen_port)
    metrics = PrometheusReliabilityMetrics()

    store = ClickHouseReferenceTickStore(
        base_url=settings.clickhouse_http_url,
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=settings.clickhouse_http_username,
        password=settings.clickhouse_http_password,
    )
    consumer = build_reference_tick_consumer(
        topic=settings.kafka_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=(
            os.getenv("REFERENCE_TICK_CONSUMER_GROUP")
            or "reference-tick-writer-v1"
        ).strip(),
        auto_offset_reset=settings.kafka_auto_offset_reset,
        value_format=settings.kafka_raw_value_format,
    )
    health = WorkerHealthTracker(
        worker_id="reference_tick_writer",
        sink=WorkerHealthFileSink(
            Path(
                os.getenv("REFERENCE_TICK_HEALTH_SNAPSHOT_PATH")
                or "/tmp/reference-tick-writer-health.json"
            )
        ),
        stale_after_seconds=int(
            os.getenv("REFERENCE_TICK_HEALTH_STALE_AFTER_SECONDS") or "180"
        ),
    )
    runtime = ReferenceTickKafkaRuntime(
        consumer=consumer,
        processor=ReferenceTickProcessor(store),
        metrics=metrics,
        health=health,
    )
    logger.info("Starting reference tick writer")
    with graceful_shutdown_event(
        logger=logger,
        worker="reference_tick_writer",
    ) as stop_event:
        runtime.run(stop_event=stop_event)


if __name__ == "__main__":
    main()

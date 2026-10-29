"""Composition root for reliable Kafka-to-signal processing."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from tinvest_signal_engine.adapters.kafka_reliability import (
    KafkaDlqPublisher,
    KafkaSignalPublisher,
    ReliableDetectorRuntime,
    build_raw_consumer,
)
from tinvest_signal_engine.adapters.kafka_startup import seek_consumer_to_recent
from tinvest_signal_engine.adapters.legacy_detection import (
    LegacyDetectionAdapter,
)
from tinvest_signal_engine.adapters.futures_curve_runtime import (
    FuturesCurveRuntimeAdapter, load_curve_configuration,
)
from tinvest_signal_engine.domain.futures_curve import FuturesCurveDetector
from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_reliable_processing_store,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.adapters.worker_health_file import WorkerHealthFileSink
from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.application.reliable_processing import (
    ReliableEventProcessor,
)
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessPolicy,
)
from tinvest_signal_engine.kafka_wire_config import validate_kafka_wire_settings
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.serialization import utc_now


logger = logging.getLogger(__name__)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="detector")
    validate_kafka_wire_settings(settings)
    configure_logging(settings.log_level)
    start_reliability_metrics_server(settings.metrics_listen_port)

    metrics = PrometheusReliabilityMetrics()
    delivery_recovery_guard = DeliveryRecoveryGuard(
        policy=DeliveryFreshnessPolicy(
            maximum_event_age_seconds=(settings.signal_delivery_max_event_age_seconds)
        ),
        metrics=metrics,
        clock=utc_now,
    )
    store = connect_reliable_processing_store(settings)
    curve_policy_path = Path(os.getenv("FUTURES_CURVE_POLICY_FILE", "/app/conf/futures-curve-policy.json"))

    def build_curve_runtime():
        config = load_curve_configuration(settings.instruments_path, curve_policy_path)
        return FuturesCurveDetector(config.contracts, config.settings), config

    futures_curve = None
    if curve_policy_path.exists():
        futures_curve = FuturesCurveRuntimeAdapter(
            factory=build_curve_runtime, instruments_path=settings.instruments_path,
            policy_path=curve_policy_path,
            state_path=Path(os.getenv("FUTURES_CURVE_STATE_PATH", "/app/var/runtime-health/futures-curve-state.json")),
        )
    detector = LegacyDetectionAdapter(
        settings,
        delivered_count_since=store.count_delivered_since,
        checkpoints=store.load_state_checkpoints(),
        config_ack_sink=store,
        delivery_recovery_guard=delivery_recovery_guard,
        futures_curve=futures_curve,
    )
    publisher = KafkaSignalPublisher(settings)
    consumer = build_raw_consumer(settings)
    seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=settings.kafka_first_boot_warmup_age_seconds,
    )
    runtime = ReliableDetectorRuntime(
        consumer=consumer,
        processor=ReliableEventProcessor(
            detector=detector,
            store=store,
            publisher=publisher,
            metrics=metrics,
        ),
        signal_publisher=publisher,
        dlq_publisher=KafkaDlqPublisher(settings),
        metrics=metrics,
        checkpoint=detector.checkpoint,
        health=WorkerHealthTracker(
            worker_id="detector",
            sink=WorkerHealthFileSink(
                Path(
                    os.getenv("DETECTOR_HEALTH_SNAPSHOT_PATH")
                    or "/tmp/detector-health.json"
                )
            ),
            stale_after_seconds=int(
                os.getenv("DETECTOR_HEALTH_STALE_AFTER_SECONDS") or "180"
            ),
        ),
    )
    logger.info("Starting reliable detector service")
    try:
        runtime.run()
    finally:
        store.close()


if __name__ == "__main__":
    main()

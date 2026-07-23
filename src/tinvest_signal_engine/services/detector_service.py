"""Composition root for reliable Kafka-to-signal processing."""

from __future__ import annotations

import logging

from tinvest_signal_engine.adapters.kafka_reliability import (
    KafkaDlqPublisher,
    KafkaSignalPublisher,
    ReliableDetectorRuntime,
    build_raw_consumer,
)
from tinvest_signal_engine.adapters.legacy_detection import (
    LegacyDetectionAdapter,
)
from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_reliable_processing_store,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.application.reliable_processing import (
    ReliableEventProcessor,
)
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
    detector = LegacyDetectionAdapter(
        settings,
        delivered_count_since=store.count_delivered_since,
        checkpoints=store.load_state_checkpoints(),
        config_ack_sink=store,
        delivery_recovery_guard=delivery_recovery_guard,
    )
    publisher = KafkaSignalPublisher(settings)
    runtime = ReliableDetectorRuntime(
        consumer=build_raw_consumer(settings),
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
    )
    logger.info("Starting reliable detector service")
    try:
        runtime.run()
    finally:
        store.close()


if __name__ == "__main__":
    main()

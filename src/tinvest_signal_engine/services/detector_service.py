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
from tinvest_signal_engine.application.reliable_processing import (
    ReliableEventProcessor,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.kafka_wire_config import validate_kafka_wire_settings
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="detector")
    validate_kafka_wire_settings(settings)
    configure_logging(settings.log_level)
    start_reliability_metrics_server(settings.metrics_listen_port)

    store = connect_reliable_processing_store(settings)
    detector = LegacyDetectionAdapter(
        settings,
        delivered_count_since=store.count_delivered_since,
        checkpoints=store.load_state_checkpoints(),
        config_ack_sink=store,
    )
    metrics = PrometheusReliabilityMetrics()
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

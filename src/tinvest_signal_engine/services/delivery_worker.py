"""Composition root for durable Telegram and webhook delivery."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from tinvest_signal_engine.adapters.delivery_senders import (
    ConfiguredDeliverySender,
)
from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_delivery_queue,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.application.delivery import DurableDeliveryWorker
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="delivery_worker")
    configure_logging(settings.log_level)
    start_reliability_metrics_server(
        settings.delivery_worker_metrics_listen_port
    )
    queue = connect_delivery_queue(settings)
    sender = ConfiguredDeliverySender(settings)
    worker = DurableDeliveryWorker(
        queue=queue,
        sender=sender,
        metrics=PrometheusReliabilityMetrics(),
        clock=lambda: datetime.now(tz=timezone.utc),
        lease_seconds=settings.delivery_worker_claim_lease_seconds,
        maximum_attempts=settings.delivery_worker_max_attempts,
        retry_base_seconds=settings.delivery_worker_retry_base_seconds,
        retry_maximum_seconds=settings.delivery_worker_retry_max_seconds,
    )
    logger.info("Starting durable delivery worker")
    try:
        while True:
            result = worker.run_once()
            if result.outcome == "idle":
                time.sleep(settings.delivery_worker_poll_seconds)
    except KeyboardInterrupt:
        logger.info("Delivery worker stopped by user")
    finally:
        sender.close()
        queue.close()


if __name__ == "__main__":
    main()

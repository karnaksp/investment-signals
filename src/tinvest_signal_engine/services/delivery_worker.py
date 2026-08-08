"""Composition root for durable Telegram and webhook delivery."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from tinvest_signal_engine.adapters.delivery_senders import (
    ConfiguredDeliverySender,
)
from tinvest_signal_engine.adapters.delivery_recovery import (
    QueuedDeliveryRecoveryAdapter,
)
from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_delivery_queue,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.adapters.worker_health_file import WorkerHealthFileSink
from tinvest_signal_engine.application.delivery import DurableDeliveryWorker
from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessPolicy,
)
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import graceful_shutdown_event


logger = logging.getLogger(__name__)


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="delivery_worker")
    configure_logging(settings.log_level)
    start_reliability_metrics_server(settings.delivery_worker_metrics_listen_port)
    metrics = PrometheusReliabilityMetrics()

    def clock() -> datetime:
        return datetime.now(tz=timezone.utc)

    recovery_guard = QueuedDeliveryRecoveryAdapter(
        DeliveryRecoveryGuard(
            policy=DeliveryFreshnessPolicy(
                maximum_event_age_seconds=(
                    settings.signal_delivery_max_event_age_seconds
                )
            ),
            metrics=metrics,
            clock=clock,
        )
    )
    queue = connect_delivery_queue(settings)
    sender = ConfiguredDeliverySender(settings)
    worker = DurableDeliveryWorker(
        queue=queue,
        sender=sender,
        metrics=metrics,
        clock=clock,
        lease_seconds=settings.delivery_worker_claim_lease_seconds,
        maximum_attempts=settings.delivery_worker_max_attempts,
        retry_base_seconds=settings.delivery_worker_retry_base_seconds,
        retry_maximum_seconds=settings.delivery_worker_retry_max_seconds,
        recovery_guard=recovery_guard,
    )
    logger.info("Starting durable delivery worker")
    health = WorkerHealthTracker(
        worker_id="delivery_worker",
        sink=WorkerHealthFileSink(
            Path(
                os.getenv("DELIVERY_HEALTH_SNAPSHOT_PATH")
                or "/tmp/delivery-worker-health.json"
            )
        ),
        stale_after_seconds=int(
            os.getenv("DELIVERY_HEALTH_STALE_AFTER_SECONDS") or "180"
        ),
    )
    try:
        with graceful_shutdown_event(
            logger=logger,
            worker="delivery_worker",
        ) as stop_event:
            while not stop_event.is_set():
                health.heartbeat()
                try:
                    result = worker.run_once()
                except Exception:
                    health.failed("worker_cycle_failed")
                    raise
                health.succeeded(force=result.outcome != "idle")
                if result.outcome == "idle":
                    stop_event.wait(settings.delivery_worker_poll_seconds)
    finally:
        sender.close()
        queue.close()


if __name__ == "__main__":
    main()

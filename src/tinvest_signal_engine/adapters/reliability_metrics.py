"""Prometheus adapter for reliable processing and delivery metrics."""

from __future__ import annotations

import threading
from typing import Sequence

from prometheus_client import Counter, Histogram, start_http_server


_lock = threading.Lock()
_started_ports: set[tuple[str, int]] = set()

_events = Counter(
    "reliable_processing_events_total",
    "Validated broker events durably processed",
    ("event_type", "outcome"),
)
_signals = Counter(
    "reliable_processing_signals_total",
    "Product signals observed after durable processing",
    ("signal_type",),
)
_duration = Histogram(
    "reliable_processing_event_seconds",
    "Event inbox, detection, transaction, and signal publication latency",
    buckets=(0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
_dlq = Counter(
    "reliable_processing_dlq_total",
    "Poison broker records published to DLQ",
    ("reason_code",),
)
_commits = Counter(
    "reliable_processing_offset_commits_total",
    "Kafka offsets committed after durable processing or DLQ publication",
)
_delivery = Counter(
    "delivery_worker_attempts_total",
    "Durable delivery attempts",
    ("destination_type", "outcome"),
)
_delivery_attempt_number = Histogram(
    "delivery_worker_attempt_number",
    "Attempt number when a delivery reached an outcome",
    ("destination_type", "outcome"),
    buckets=(1, 2, 3, 4, 5, 8, 13, 21),
)
_observation_publication = Counter(
    "detector_observation_publication_attempts_total",
    "Durable detector observation publication attempts",
    ("outcome",),
)
_observation_attempt_number = Histogram(
    "detector_observation_publication_attempt_number",
    "Attempt number when a detector observation publication reached an outcome",
    ("outcome",),
    buckets=(1, 2, 3, 4, 5, 8, 13, 21),
)


class PrometheusReliabilityMetrics:
    def event_processed(
        self,
        *,
        event_type: str,
        outcome: str,
        signal_types: Sequence[str],
        duration_seconds: float,
    ) -> None:
        _events.labels(event_type=event_type, outcome=outcome).inc()
        _duration.observe(duration_seconds)
        for signal_type in signal_types:
            _signals.labels(signal_type=signal_type).inc()

    def dead_lettered(self, *, reason_code: str) -> None:
        _dlq.labels(reason_code=reason_code).inc()

    def offset_committed(self) -> None:
        _commits.inc()

    def delivery_attempted(
        self,
        *,
        destination_type: str,
        outcome: str,
        attempt_count: int,
    ) -> None:
        labels = {
            "destination_type": destination_type,
            "outcome": outcome,
        }
        _delivery.labels(**labels).inc()
        _delivery_attempt_number.labels(**labels).observe(attempt_count)

    def publication_attempted(
        self,
        *,
        outcome: str,
        attempt_count: int,
    ) -> None:
        _observation_publication.labels(outcome=outcome).inc()
        _observation_attempt_number.labels(outcome=outcome).observe(attempt_count)


def start_reliability_metrics_server(
    port: int | None,
    *,
    addr: str = "0.0.0.0",
) -> None:
    if port is None or port <= 0:
        return
    key = (addr, port)
    with _lock:
        if key in _started_ports:
            return
        start_http_server(port, addr=addr)
        _started_ports.add(key)

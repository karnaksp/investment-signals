"""Prometheus metrics for the detector hot path (optional, port from env)."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_started = False
_lock = threading.Lock()

_messages_processed: Any = None
_signals_emitted: Any = None
_process_seconds: Any = None


def _ensure_metrics() -> None:
    global _messages_processed, _signals_emitted, _process_seconds
    if _messages_processed is not None:
        return
    try:
        from prometheus_client import Counter, Histogram
    except ImportError:
        return
    _messages_processed = Counter(
        "detector_messages_processed_total",
        "Raw Kafka messages processed by the detector",
        ["event_type", "outcome"],
    )
    _signals_emitted = Counter(
        "detector_signals_emitted_total",
        "Trigger signals emitted to sinks",
        ["signal_type"],
    )
    _process_seconds = Histogram(
        "detector_process_event_seconds",
        "Wall time to validate, detect, and sink one normalized event",
        buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    )


def start_metrics_server(port: int, *, addr: str = "0.0.0.0") -> None:
    """Idempotent: starts ``prometheus_client`` HTTP listener once."""
    global _started
    if port <= 0:
        return
    _ensure_metrics()
    try:
        from prometheus_client import start_http_server
    except ImportError:
        logger.warning(
            "METRICS_LISTEN_PORT=%s but prometheus_client is not installed",
            port,
        )
        return
    with _lock:
        if _started:
            return
        start_http_server(port, addr=addr)
        _started = True
    logger.info("Prometheus metrics listening on %s:%s", addr, port)


def observe_message(*, event_type: str, outcome: str) -> None:
    _ensure_metrics()
    if _messages_processed is not None:
        _messages_processed.labels(event_type, outcome).inc()


def observe_signals(signal_types: list[str]) -> None:
    _ensure_metrics()
    if _signals_emitted is None:
        return
    for signal_type in signal_types:
        _signals_emitted.labels(signal_type).inc()


def timed_process_block() -> Any:
    """Context manager returning a timer that records ``detector_process_event_seconds``."""
    _ensure_metrics()
    if _process_seconds is None:
        return _NoopTimer()
    return _process_seconds.time()


class _NoopTimer:
    def __enter__(self) -> None:
        self._t0 = time.perf_counter()

    def __exit__(self, exc_type, exc, tb) -> None:
        del self._t0


_unary_cycles: Any = None
_unary_published: Any = None
_unary_errors: Any = None
_unary_cycle_seconds: Any = None


def _ensure_unary_metrics() -> None:
    global _unary_cycles, _unary_published, _unary_errors, _unary_cycle_seconds
    if _unary_cycles is not None:
        return
    try:
        from prometheus_client import Counter, Histogram
    except ImportError:
        return
    _unary_cycles = Counter(
        "market_unary_cycles_total",
        "Unary emitter completed poll cycles",
    )
    _unary_published = Counter(
        "market_unary_events_published_total",
        "Normalized events published by unary emitter",
        ["event_type"],
    )
    _unary_errors = Counter(
        "market_unary_errors_total",
        "Unary emitter failures",
        ["phase"],
    )
    _unary_cycle_seconds = Histogram(
        "market_unary_cycle_wall_seconds",
        "Wall time of one unary emitter poll cycle",
        buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
    )


def observe_unary_cycle_completed() -> None:
    _ensure_unary_metrics()
    if _unary_cycles is not None:
        _unary_cycles.inc()


def observe_unary_publish(*, event_type: str, count: int = 1) -> None:
    _ensure_unary_metrics()
    if _unary_published is not None and count > 0:
        _unary_published.labels(event_type).inc(count)


def observe_unary_error(*, phase: str) -> None:
    _ensure_unary_metrics()
    if _unary_errors is not None:
        _unary_errors.labels(phase).inc()


def unary_cycle_timer() -> Any:
    _ensure_unary_metrics()
    if _unary_cycle_seconds is None:
        return _NoopTimer()
    return _unary_cycle_seconds.time()

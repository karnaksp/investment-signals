"""Choose a safe Kafka position when a detector group starts for the first time."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable


logger = logging.getLogger(__name__)


def seek_consumer_to_recent(
    consumer: Any,
    *,
    maximum_age_seconds: int,
    clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
) -> int:
    """Warm up only partitions that do not have a committed group offset.

    A committed offset is durable evidence of the next record the detector
    must process.  Its age is therefore irrelevant: after an outage we always
    rewind to that offset, including when the assignment poll returned a
    record.  The timestamp cut-off applies only to a genuinely new consumer
    group partition, where a short history window warms rolling features.

    Kafka returns ``None`` from ``offsets_for_times`` when there is no record at
    or after the requested timestamp.  In that case this function preserves
    the broker-selected ``auto_offset_reset`` position rather than seeking to
    the end.  If the assignment poll already returned a record, it rewinds to
    that record so startup positioning never silently discards it.

    The return value is the number of previously uncommitted partitions that
    were explicitly positioned at the warm-up cut-off.
    """

    if maximum_age_seconds <= 0:
        return 0
    polled = consumer.poll(timeout_ms=5_000, max_records=1) or {}
    partitions = tuple(consumer.assignment())
    if not partitions:
        return 0

    committed_offsets = {
        partition: consumer.committed(partition) for partition in partitions
    }
    uncommitted = tuple(
        partition for partition, offset in committed_offsets.items() if offset is None
    )
    recent_offsets: dict[Any, Any] = {}
    if uncommitted:
        cutoff_ms = clock_ms() - maximum_age_seconds * 1_000
        recent_offsets = consumer.offsets_for_times(
            {partition: cutoff_ms for partition in uncommitted}
        )

    first_polled_offsets = _first_polled_offsets(polled)
    positioned = 0
    for partition in partitions:
        committed = committed_offsets[partition]
        if committed is not None:
            consumer.seek(partition, int(committed))
            continue

        recent = recent_offsets.get(partition)
        if recent is not None:
            consumer.seek(partition, int(recent.offset))
            positioned += 1
            continue

        first_polled = first_polled_offsets.get(partition)
        if first_polled is not None:
            consumer.seek(partition, first_polled)

    if positioned:
        logger.info(
            "Positioned new Kafka consumer-group partitions for live warm-up",
            extra={
                "maximum_age_seconds": maximum_age_seconds,
                "partitions_positioned": positioned,
            },
        )
    return positioned


def _first_polled_offsets(polled: Any) -> dict[Any, int]:
    """Return the first record offset fetched while waiting for assignment."""

    if not isinstance(polled, dict):
        return {}
    first: dict[Any, int] = {}
    for partition, records in polled.items():
        offsets = [
            int(record.offset)
            for record in records
            if getattr(record, "offset", None) is not None
        ]
        if offsets:
            first[partition] = min(offsets)
    return first

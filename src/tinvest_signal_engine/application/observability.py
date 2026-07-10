"""Application-owned observability port."""

from __future__ import annotations

from typing import Protocol, Sequence


class ReliabilityMetrics(Protocol):
    def event_processed(
        self,
        *,
        event_type: str,
        outcome: str,
        signal_types: Sequence[str],
        duration_seconds: float,
    ) -> None: ...

    def dead_lettered(self, *, reason_code: str) -> None: ...

    def offset_committed(self) -> None: ...

    def delivery_attempted(
        self,
        *,
        destination_type: str,
        outcome: str,
        attempt_count: int,
    ) -> None: ...

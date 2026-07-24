"""Broker-stream health state without infrastructure or secret details."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum


INGESTOR_HEALTH_SCHEMA_VERSION = "ingestor-health-v1"

INGESTOR_STARTING = "ingestor_starting"
INGESTOR_CONNECTING = "ingestor_connecting"
INGESTOR_RECONNECTING = "ingestor_reconnecting"
INGESTOR_CONFIGURATION_RELOAD = "ingestor_configuration_reload"
INGESTOR_STREAMING = "ingestor_streaming"
INGESTOR_STREAM_STALE = "ingestor_stream_stale"
INGESTOR_DNS_RESOLUTION_FAILED = "ingestor_dns_resolution_failed"
INGESTOR_TINVEST_REQUEST_FAILED = "ingestor_tinvest_request_failed"
INGESTOR_MARKET_STREAM_FAILED = "ingestor_market_stream_failed"
INGESTOR_PUBLISH_FAILED = "ingestor_publish_failed"


class IngestorStreamState(StrEnum):
    CONNECTING = "connecting"
    STREAMING = "streaming"
    DEGRADED = "degraded"


_CONNECTING_REASONS = frozenset(
    {
        INGESTOR_STARTING,
        INGESTOR_CONNECTING,
        INGESTOR_RECONNECTING,
        INGESTOR_CONFIGURATION_RELOAD,
    }
)
_STREAMING_REASONS = frozenset({INGESTOR_STREAMING})
_DEGRADED_REASONS = frozenset(
    {
        INGESTOR_STREAM_STALE,
        INGESTOR_DNS_RESOLUTION_FAILED,
        INGESTOR_TINVEST_REQUEST_FAILED,
        INGESTOR_MARKET_STREAM_FAILED,
        INGESTOR_PUBLISH_FAILED,
    }
)


@dataclass(frozen=True, slots=True)
class IngestorHealthSnapshot:
    """Allow-listed, locally persisted state of the T-Invest ingestion stream."""

    state: IngestorStreamState
    started_at: datetime
    last_market_event_at: datetime | None
    last_success_at: datetime | None
    last_error_at: datetime | None
    reason_code: str
    consecutive_failures: int
    configured_instruments: int
    stale_after_seconds: int

    def __post_init__(self) -> None:
        _require_aware(self.started_at, field_name="started_at")
        for field_name in (
            "last_market_event_at",
            "last_success_at",
            "last_error_at",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_aware(value, field_name=field_name)
        if self.consecutive_failures < 0:
            raise ValueError("consecutive_failures must be non-negative")
        if self.configured_instruments < 0:
            raise ValueError("configured_instruments must be non-negative")
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        expected_reasons = {
            IngestorStreamState.CONNECTING: _CONNECTING_REASONS,
            IngestorStreamState.STREAMING: _STREAMING_REASONS,
            IngestorStreamState.DEGRADED: _DEGRADED_REASONS,
        }[self.state]
        if self.reason_code not in expected_reasons:
            raise ValueError(
                f"reason_code {self.reason_code!r} is invalid for {self.state.value}"
            )

    @classmethod
    def starting(
        cls,
        *,
        started_at: datetime,
        stale_after_seconds: int,
    ) -> "IngestorHealthSnapshot":
        return cls(
            state=IngestorStreamState.CONNECTING,
            started_at=started_at,
            last_market_event_at=None,
            last_success_at=None,
            last_error_at=None,
            reason_code=INGESTOR_STARTING,
            consecutive_failures=0,
            configured_instruments=0,
            stale_after_seconds=stale_after_seconds,
        )

    def connecting(
        self,
        *,
        configured_instruments: int,
        reason_code: str = INGESTOR_CONNECTING,
    ) -> "IngestorHealthSnapshot":
        return replace(
            self,
            state=IngestorStreamState.CONNECTING,
            reason_code=reason_code,
            configured_instruments=configured_instruments,
        )

    def market_event_observed(
        self,
        *,
        market_event_at: datetime,
    ) -> "IngestorHealthSnapshot":
        _require_aware(market_event_at, field_name="market_event_at")
        return replace(
            self,
            last_market_event_at=_latest(
                self.last_market_event_at,
                market_event_at,
            ),
        )

    def publish_succeeded(
        self,
        *,
        market_event_at: datetime,
        succeeded_at: datetime,
    ) -> "IngestorHealthSnapshot":
        _require_aware(market_event_at, field_name="market_event_at")
        _require_aware(succeeded_at, field_name="succeeded_at")
        return replace(
            self,
            state=IngestorStreamState.STREAMING,
            last_market_event_at=_latest(
                self.last_market_event_at,
                market_event_at,
            ),
            last_success_at=_latest(self.last_success_at, succeeded_at),
            reason_code=INGESTOR_STREAMING,
            consecutive_failures=0,
        )

    def failed(
        self,
        *,
        failed_at: datetime,
        reason_code: str,
    ) -> "IngestorHealthSnapshot":
        _require_aware(failed_at, field_name="failed_at")
        return replace(
            self,
            state=IngestorStreamState.DEGRADED,
            last_error_at=_latest(self.last_error_at, failed_at),
            reason_code=reason_code,
            consecutive_failures=self.consecutive_failures + 1,
        )

    def evaluate_staleness(
        self,
        *,
        evaluated_at: datetime,
    ) -> "IngestorHealthSnapshot":
        """Return a degraded view once a formerly streaming source is silent."""

        _require_aware(evaluated_at, field_name="evaluated_at")
        if self.state is not IngestorStreamState.STREAMING:
            return self
        if self.last_success_at is None:
            return replace(
                self,
                state=IngestorStreamState.DEGRADED,
                reason_code=INGESTOR_STREAM_STALE,
            )
        silence_seconds = (evaluated_at - self.last_success_at).total_seconds()
        if silence_seconds <= self.stale_after_seconds:
            return self
        return replace(
            self,
            state=IngestorStreamState.DEGRADED,
            reason_code=INGESTOR_STREAM_STALE,
        )


def _latest(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate > current:
        return candidate
    return current


def _require_aware(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")

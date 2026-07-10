"""Паузы повторных запросов к T-Invest."""

from __future__ import annotations

from collections import namedtuple

from grpc import StatusCode
from tinkoff.invest.exceptions import RequestError

from tinvest_signal_engine.instruments import request_error_retry_delay_seconds


Metadata = namedtuple(
    "Metadata",
    (
        "tracking_id",
        "ratelimit_limit",
        "ratelimit_remaining",
        "ratelimit_reset",
        "message",
    ),
)


def test_request_error_retry_delay_uses_ratelimit_reset() -> None:
    exc = RequestError(
        StatusCode.RESOURCE_EXHAUSTED,
        "RESOURCE_EXHAUSTED",
        Metadata("track", "200", 0, 59, "rate limit"),
    )

    assert request_error_retry_delay_seconds(exc) == 60


def test_request_error_retry_delay_falls_back_without_metadata() -> None:
    exc = RequestError(StatusCode.UNKNOWN, "UNKNOWN", None)

    assert request_error_retry_delay_seconds(exc, fallback_seconds=7) == 7


def test_request_error_retry_delay_caps_long_reset() -> None:
    exc = RequestError(
        StatusCode.RESOURCE_EXHAUSTED,
        "RESOURCE_EXHAUSTED",
        Metadata("track", "200", 0, 1000, "rate limit"),
    )

    assert request_error_retry_delay_seconds(exc, max_seconds=120) == 120

from __future__ import annotations

import http.client
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
import pytest

from tinvest_signal_engine.adapters.telegram_http import (
    RawTelegramHttpResponse,
    ResolvedTelegramAddress,
    SystemTelegramAddressResolver,
    TelegramAddressConnectionError,
    TelegramMultiAddressHttpClient,
)
from tinvest_signal_engine.application.manual_delivery_retry import (
    ManualDeliveryRetry,
)
from tinvest_signal_engine.domain.reliable_processing import (
    DeadLetterDelivery,
    manual_delivery_retry_decision,
)


_FIRST = ResolvedTelegramAddress(socket.AF_INET, ("1.1.1.1", 443))
_SECOND = ResolvedTelegramAddress(socket.AF_INET, ("8.8.8.8", 443))


@dataclass
class FakeResolver:
    addresses: tuple[ResolvedTelegramAddress, ...] = (_FIRST, _SECOND)

    def resolve(self, **_kwargs) -> tuple[ResolvedTelegramAddress, ...]:
        return self.addresses


@dataclass
class FakeRequester:
    outcomes: list[object]
    calls: list[ResolvedTelegramAddress] = field(default_factory=list)

    def post(self, **kwargs) -> RawTelegramHttpResponse:
        self.calls.append(kwargs["address"])
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, RawTelegramHttpResponse)
        assert kwargs["hostname"] == "api.telegram.org"
        assert kwargs["path"] == "/botredacted/sendMessage"
        return outcome


def _success() -> RawTelegramHttpResponse:
    return RawTelegramHttpResponse(
        status_code=200,
        headers=(("content-type", "application/json"),),
        content=b'{"ok":true}',
    )


def test_telegram_tries_next_dns_address_after_tcp_connect_failure() -> None:
    requester = FakeRequester(
        [TelegramAddressConnectionError("unreachable"), _success()]
    )
    client = TelegramMultiAddressHttpClient(
        resolver=FakeResolver(),
        requester=requester,
    )

    response = client.post(
        "https://api.telegram.org/botredacted/sendMessage",
        json={"chat_id": "redacted", "text": "test"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert requester.calls == [_FIRST, _SECOND]


def test_telegram_does_not_fail_over_after_http_exchange_started() -> None:
    requester = FakeRequester(
        [http.client.RemoteDisconnected("ambiguous delivery"), _success()]
    )
    client = TelegramMultiAddressHttpClient(
        resolver=FakeResolver(),
        requester=requester,
    )

    with pytest.raises(httpx.ProtocolError):
        client.post(
            "https://api.telegram.org/botredacted/sendMessage",
            json={"chat_id": "redacted", "text": "test"},
        )

    assert requester.calls == [_FIRST]


def test_telegram_reports_connect_error_after_all_addresses_fail() -> None:
    requester = FakeRequester(
        [
            TelegramAddressConnectionError("first"),
            TelegramAddressConnectionError("second"),
        ]
    )
    client = TelegramMultiAddressHttpClient(
        resolver=FakeResolver(),
        requester=requester,
    )

    with pytest.raises(httpx.ConnectError, match="All resolved"):
        client.post(
            "https://api.telegram.org/botredacted/sendMessage",
            json={"chat_id": "redacted", "text": "test"},
        )

    assert requester.calls == [_FIRST, _SECOND]


def test_system_resolver_deduplicates_and_rejects_non_public_addresses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443)),
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                6,
                "",
                ("2606:4700:4700::1111", 443, 0, 0),
            ),
        ],
    )

    addresses = SystemTelegramAddressResolver().resolve(
        hostname="api.telegram.org",
        port=443,
    )

    assert addresses == (
        ResolvedTelegramAddress(socket.AF_INET, ("1.1.1.1", 443)),
        ResolvedTelegramAddress(
            socket.AF_INET6,
            ("2606:4700:4700::1111", 443, 0, 0),
        ),
    )


@dataclass
class FakeDeadLetterQueue:
    delivery: DeadLetterDelivery | None
    changed: bool = True
    requeued: list[tuple[DeadLetterDelivery, datetime]] = field(
        default_factory=list
    )

    def get_for_manual_retry(self, **_kwargs) -> DeadLetterDelivery | None:
        return self.delivery

    def requeue_dead_letter(
        self, delivery: DeadLetterDelivery, *, available_at: datetime
    ) -> bool:
        self.requeued.append((delivery, available_at))
        return self.changed


def _dead_letter(error: str) -> DeadLetterDelivery:
    return DeadLetterDelivery(
        outbox_id="00000000-0000-0000-0000-000000000099",
        destination_type="telegram",
        status="dead_letter",
        attempt_count=8,
        last_error_code=error,
    )


@pytest.mark.parametrize(
    "error",
    [
        "delivery_network_error",
        "delivery_timeout",
        "delivery_http_429",
        "delivery_http_500",
        "delivery_http_503",
        "delivery_http_599",
    ],
)
def test_manual_retry_policy_allows_only_transient_terminal_failures(
    error: str,
) -> None:
    assert manual_delivery_retry_decision(_dead_letter(error)).allowed is True


@pytest.mark.parametrize(
    "error",
    [
        "telegram_not_configured",
        "delivery_http_400",
        "delivery_http_401",
        "delivery_http_404",
        "delivery_error",
        "",
    ],
)
def test_manual_retry_policy_rejects_permanent_or_unknown_failures(
    error: str,
) -> None:
    assert manual_delivery_retry_decision(_dead_letter(error)).allowed is False


def test_manual_retry_requeues_one_exact_dead_letter() -> None:
    delivery = _dead_letter("delivery_network_error")
    queue = FakeDeadLetterQueue(delivery)
    use_case = ManualDeliveryRetry(queue=queue)
    retry_at = datetime(2026, 7, 22, tzinfo=timezone.utc)

    preview = use_case.preview(outbox_id=delivery.outbox_id)
    result = use_case.retry(
        outbox_id=delivery.outbox_id,
        available_at=retry_at,
    )

    assert preview.outcome == "eligible"
    assert result.outcome == "requeued"
    assert queue.requeued == [(delivery, retry_at)]


def test_manual_retry_rejects_permanent_failure_without_queue_write() -> None:
    delivery = _dead_letter("delivery_http_401")
    queue = FakeDeadLetterQueue(delivery)

    result = ManualDeliveryRetry(queue=queue).retry(
        outbox_id=delivery.outbox_id,
        available_at=datetime(2026, 7, 22, tzinfo=timezone.utc),
    )

    assert result.outcome == "ineligible"
    assert queue.requeued == []


def test_manual_retry_detects_concurrent_state_change() -> None:
    delivery = _dead_letter("delivery_network_error")
    queue = FakeDeadLetterQueue(delivery, changed=False)

    result = ManualDeliveryRetry(queue=queue).retry(
        outbox_id=delivery.outbox_id,
        available_at=datetime(2026, 7, 22, tzinfo=timezone.utc),
    )

    assert result.outcome == "conflict"
    assert result.reason_code == "delivery_changed_before_retry"

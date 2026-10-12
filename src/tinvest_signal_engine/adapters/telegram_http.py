"""Telegram HTTPS adapter with DNS-derived address failover.

The request hostname remains api.telegram.org for the Host header and TLS
certificate verification. Only the TCP destination changes between addresses
returned by the system resolver; no Telegram IP range is embedded in the code.
"""

from __future__ import annotations

import http.client
import ipaddress
import json
import socket
import ssl
import time
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlsplit

import httpx


_TELEGRAM_API_HOST = "api.telegram.org"
_MAX_RESPONSE_BYTES = 1_048_576


@dataclass(frozen=True)
class ResolvedTelegramAddress:
    family: int
    socket_address: tuple[object, ...]


class TelegramAddressResolver(Protocol):
    def resolve(
        self, *, hostname: str, port: int
    ) -> tuple[ResolvedTelegramAddress, ...]: ...


class TelegramAddressConnectionError(ConnectionError):
    """TCP connection failed before TLS or HTTP bytes were exchanged."""


@dataclass(frozen=True)
class RawTelegramHttpResponse:
    status_code: int
    headers: tuple[tuple[str, str], ...]
    content: bytes


class TelegramAddressRequester(Protocol):
    def post(
        self,
        *,
        hostname: str,
        port: int,
        address: ResolvedTelegramAddress,
        path: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> RawTelegramHttpResponse: ...


class SystemTelegramAddressResolver:
    """Resolve and retain only unique, publicly routable TCP addresses."""

    def resolve(
        self, *, hostname: str, port: int
    ) -> tuple[ResolvedTelegramAddress, ...]:
        try:
            rows = socket.getaddrinfo(
                hostname,
                port,
                family=socket.AF_UNSPEC,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
            )
        except socket.gaierror as error:
            raise TelegramAddressConnectionError(
                "telegram_dns_resolution_failed"
            ) from error
        addresses: list[ResolvedTelegramAddress] = []
        seen: set[tuple[int, tuple[object, ...]]] = set()
        for family, _type, _proto, _canonical_name, socket_address in rows:
            if family not in {socket.AF_INET, socket.AF_INET6}:
                continue
            normalized = tuple(socket_address)
            try:
                address = ipaddress.ip_address(str(normalized[0]))
            except ValueError:
                continue
            if not address.is_global:
                continue
            key = (family, normalized)
            if key in seen:
                continue
            seen.add(key)
            addresses.append(
                ResolvedTelegramAddress(
                    family=family,
                    socket_address=normalized,
                )
            )
        if not addresses:
            raise TelegramAddressConnectionError(
                "telegram_dns_has_no_public_addresses"
            )
        return tuple(addresses)


class StdlibTelegramAddressRequester:
    """Send one HTTPS request to one resolved address with verified TLS SNI."""

    def __init__(self, *, ssl_context: ssl.SSLContext | None = None) -> None:
        self._ssl_context = ssl_context or ssl.create_default_context()

    def post(
        self,
        *,
        hostname: str,
        port: int,
        address: ResolvedTelegramAddress,
        path: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> RawTelegramHttpResponse:
        raw_socket = socket.socket(address.family, socket.SOCK_STREAM)
        raw_socket.settimeout(timeout_seconds)
        try:
            try:
                raw_socket.connect(address.socket_address)
            except OSError as error:
                raise TelegramAddressConnectionError(
                    "telegram_address_connect_failed"
                ) from error
            tls_socket = self._ssl_context.wrap_socket(
                raw_socket,
                server_hostname=hostname,
            )
        except Exception:
            raw_socket.close()
            raise

        connection = http.client.HTTPSConnection(
            hostname,
            port=port,
            timeout=timeout_seconds,
            context=self._ssl_context,
        )
        connection.sock = tls_socket
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            connection.request(
                "POST",
                path,
                body=body,
                headers={
                    "Host": hostname,
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            content = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(content) > _MAX_RESPONSE_BYTES:
                raise http.client.HTTPException(
                    "telegram_response_too_large"
                )
            return RawTelegramHttpResponse(
                status_code=response.status,
                headers=tuple(response.getheaders()),
                content=content,
            )
        finally:
            connection.close()


class TelegramMultiAddressHttpClient:
    """Small httpx-compatible client used by TelegramAlertSink."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        resolver: TelegramAddressResolver | None = None,
        requester: TelegramAddressRequester | None = None,
    ) -> None:
        self._timeout_seconds = max(0.1, timeout_seconds)
        self._resolver = resolver or SystemTelegramAddressResolver()
        self._requester = requester or StdlibTelegramAddressRequester()

    def post(
        self, url: str, *, json: dict[str, object]
    ) -> httpx.Response:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != _TELEGRAM_API_HOST
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or parsed.port not in {None, 443}
        ):
            raise ValueError("unsupported Telegram API endpoint")
        request = httpx.Request("POST", url, json=json)
        try:
            addresses = self._resolver.resolve(
                hostname=_TELEGRAM_API_HOST,
                port=443,
            )
        except TelegramAddressConnectionError as error:
            raise httpx.ConnectError(
                "Telegram API DNS resolution failed", request=request
            ) from error

        last_connect_error: TelegramAddressConnectionError | None = None
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        deadline = time.monotonic() + self._timeout_seconds
        for address in addresses:
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                raise httpx.ConnectTimeout(
                    "Telegram API address attempts exceeded total deadline",
                    request=request,
                )
            try:
                raw = self._requester.post(
                    hostname=_TELEGRAM_API_HOST,
                    port=443,
                    address=address,
                    path=path,
                    payload=json,
                    timeout_seconds=remaining_seconds,
                )
            except TelegramAddressConnectionError as error:
                last_connect_error = error
                continue
            except socket.timeout as error:
                raise httpx.ReadTimeout(
                    "Telegram API response timed out", request=request
                ) from error
            except ssl.SSLError as error:
                raise httpx.ConnectError(
                    "Telegram API TLS handshake failed", request=request
                ) from error
            except http.client.HTTPException as error:
                raise httpx.ProtocolError(
                    "Telegram API protocol error", request=request
                ) from error
            except OSError as error:
                raise httpx.ReadError(
                    "Telegram API connection failed after connect",
                    request=request,
                ) from error
            return httpx.Response(
                status_code=raw.status_code,
                headers=raw.headers,
                content=raw.content,
                request=request,
            )
        raise httpx.ConnectError(
            "All resolved Telegram API addresses were unreachable",
            request=request,
        ) from last_connect_error

    def close(self) -> None:
        return None

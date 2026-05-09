"""Ограничение частоты и allowlist IP для путей ``/admin/api/*``."""

from __future__ import annotations

import time
from collections import defaultdict, deque


def admin_client_ip(request) -> str:
    forwarded = (request.headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host or ""
    return ""


class AdminApiRateLimiter:
    """Скользящее окно ~60 с: не более ``max_per_minute`` запросов на ключ."""

    def __init__(self, max_per_minute: int) -> None:
        self._max = max(0, max_per_minute)
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        if self._max <= 0:
            return True
        now = time.monotonic()
        dq = self._hits[key]
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        if len(dq) >= self._max:
            return False
        dq.append(now)
        return True

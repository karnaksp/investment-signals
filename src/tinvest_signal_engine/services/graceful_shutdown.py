"""Composition-root signal handling for interruptible worker loops."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import logging
from signal import SIGINT, SIGTERM, getsignal, signal
from threading import Event
from types import FrameType
from typing import Any


@contextmanager
def graceful_shutdown_event(
    *,
    logger: logging.Logger,
    worker: str,
) -> Iterator[Event]:
    stop = Event()
    previous: dict[int, Any] = {}

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        logger.info(
            "Worker shutdown requested",
            extra={"signal_number": signum, "worker": worker},
        )
        stop.set()

    for signum in (SIGINT, SIGTERM):
        previous[signum] = getsignal(signum)
        signal(signum, request_shutdown)
    try:
        yield stop
    finally:
        for signum, handler in previous.items():
            signal(signum, handler)

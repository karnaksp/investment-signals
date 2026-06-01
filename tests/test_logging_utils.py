"""Logging setup safeguards."""

from __future__ import annotations

import logging

from tinvest_signal_engine.logging_utils import configure_logging


def test_configure_logging_suppresses_httpx_info_urls() -> None:
    configure_logging("INFO")

    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING

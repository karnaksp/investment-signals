"""Единообразная настройка логирования для CLI-сервисов."""

from __future__ import annotations

import logging


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    # httpx INFO logs include full request URLs; Telegram URLs contain bot tokens.
    logging.getLogger("httpx").setLevel(logging.WARNING)


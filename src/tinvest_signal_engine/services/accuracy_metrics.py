"""HTTP /metrics из JSON выхода ``duckdb_label_signals`` (офлайн hit-rate в Prometheus)."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_g_hit_rate: Any = None
_g_decided: Any = None
_g_hits: Any = None
_g_misses: Any = None
_g_horizon_hit: Any = None


def _ensure_gauges() -> None:
    global _g_hit_rate, _g_decided, _g_hits, _g_misses, _g_horizon_hit
    if _g_hit_rate is not None:
        return
    from prometheus_client import Gauge

    _g_hit_rate = Gauge(
        "signal_labeller_directional_hit_rate",
        "Directional hit rate from duckdb_label_signals JSON (1=100%)",
    )
    _g_decided = Gauge(
        "signal_labeller_directional_decided_total",
        "Directional decided count (hits+misses)",
    )
    _g_hits = Gauge(
        "signal_labeller_directional_hits_total",
        "Directional hits",
    )
    _g_misses = Gauge(
        "signal_labeller_directional_misses_total",
        "Directional misses",
    )
    _g_horizon_hit = Gauge(
        "signal_labeller_directional_hit_rate_by_horizon",
        "Hit rate when JSON uses by_horizon",
        ["horizon"],
    )


def _apply_summary(data: dict[str, Any]) -> None:
    _ensure_gauges()
    hr = data.get("directional_hit_rate")
    if hr is not None:
        _g_hit_rate.set(float(hr))
    decided = data.get("directional_decided")
    if decided is not None:
        _g_decided.set(float(decided))
    hits = data.get("directional_hits")
    if hits is not None:
        _g_hits.set(float(hits))
    misses = data.get("directional_misses")
    if misses is not None:
        _g_misses.set(float(misses))


def _refresh_from_path(path: Path) -> None:
    if not path.is_file():
        return
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception:
        logger.exception("Failed to read/parse accuracy JSON from %s", path)
        return
    if not isinstance(data, dict):
        return
    if "by_horizon" in data:
        by_h = data["by_horizon"]
        if isinstance(by_h, dict):
            for horizon, block in by_h.items():
                if isinstance(block, dict):
                    hr = block.get("directional_hit_rate")
                    if hr is not None:
                        _g_horizon_hit.labels(horizon=str(horizon)).set(float(hr))
    else:
        _apply_summary(data)


def main() -> None:
    from prometheus_client import start_http_server

    from ..logging_utils import configure_logging

    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    path = Path(
        os.getenv("SIGNAL_ACCURACY_JSON_PATH", "/data/signal_accuracy.json")
    ).expanduser()
    port = int(os.getenv("ACCURACY_METRICS_PORT", "9110"))
    interval = float(os.getenv("SIGNAL_ACCURACY_REFRESH_SECONDS", "30"))

    _ensure_gauges()
    _refresh_from_path(path)
    start_http_server(port, addr="0.0.0.0")
    logger.info(
        "Accuracy metrics on :%s reading %s every %ss",
        port,
        path,
        interval,
    )

    def _loop() -> None:
        while True:
            _refresh_from_path(path)
            time.sleep(max(1.0, interval))

    threading.Thread(target=_loop, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Accuracy metrics stopped")

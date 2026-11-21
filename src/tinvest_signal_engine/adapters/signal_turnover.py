"""Normalize stored turnover to rubles at the signal adapter boundary."""

from dataclasses import replace
from functools import lru_cache
import math
from pathlib import Path

import yaml

from tinvest_signal_engine.models import TriggerSignal


@lru_cache(maxsize=4)
def _futures_multipliers(path: Path, modified_at: int) -> dict[str, float]:
    document = yaml.safe_load(path.read_text()) or {}
    if not isinstance(document, dict) or not isinstance(document.get("instruments", []), list):
        raise ValueError("instruments must be a mapping containing a list")
    multipliers = {}
    for item in document.get("instruments", []):
        if not isinstance(item, dict):
            continue
        spec = item.get("futures_contract") or {}
        if not isinstance(spec, dict):
            continue
        if item.get("class_code") != "SPBFUT" or str(spec.get("settlement_currency")).upper() != "RUB":
            continue
        try:
            step = float(spec["min_price_increment"])
            amount = float(spec["min_price_increment_amount"])
            if not all(math.isfinite(value) and value > 0 for value in (step, amount)):
                continue
            multiplier = amount / step
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue
        if math.isfinite(multiplier) and multiplier > 0:
            multipliers[str(item["ticker"])] = multiplier
    return multipliers


def normalize_signal_turnover(signal: TriggerSignal, instruments_path: Path) -> TriggerSignal:
    payload = dict(signal.payload)
    multiplier = 1.0 if signal.class_code == "TQBR" else None
    if signal.class_code == "SPBFUT":
        try:
            multiplier = _futures_multipliers(
                instruments_path, instruments_path.stat().st_mtime_ns
            ).get(signal.ticker)
        except (OSError, TypeError, ValueError, yaml.YAMLError):
            multiplier = None
    for field in ("window_notional", "estimated_notional"):
        if field not in payload or str(payload.get(field + "_currency", "")).upper() == "RUB":
            continue
        currency = str(payload.get(field + "_currency", "")).lower()
        try:
            value = float(payload[field])
            value = value * multiplier if multiplier is not None and currency in {"", "price_units"} else math.nan
        except (TypeError, ValueError, OverflowError):
            value = math.nan
        payload[field + "_price_units"] = payload[field]
        payload[field] = value if math.isfinite(value) and value >= 0 else None
        payload[field + "_currency"] = "RUB"
    return replace(signal, payload=payload)

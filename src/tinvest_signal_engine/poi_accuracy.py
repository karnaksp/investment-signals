"""Pure-Python POI accuracy summaries for offline outputs.

This module intentionally does not run the DuckDB labelling pipeline.  It only
normalises already-labelled POI rows or pre-aggregated metric rows into a small
contract that can be read by UI/API code later.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

POI_ACCURACY_CONTRACT_VERSION = "poi_accuracy_v1"
DEFAULT_POI_ACCURACY_PATH = Path("var/accuracy/poi_accuracy.json")

_SUMMARY_GROUPS = (
    "horizons",
    "by_setup_type",
    "by_bias",
    "by_ticker",
    "by_score_tier",
)

_BucketKey = TypeVar("_BucketKey")


def empty_poi_accuracy_summary() -> dict[str, Any]:
    """Return the empty POI accuracy contract."""
    return {
        "contract_version": POI_ACCURACY_CONTRACT_VERSION,
        "horizons": [],
        "by_setup_type": [],
        "by_bias": [],
        "by_ticker": [],
        "by_score_tier": [],
    }


def summarize_poi_accuracy(content: Any) -> dict[str, Any]:
    """Summarise POI-level labelled rows or metric rows.

    ``content`` may be a list of rows, ``{"rows": [...]}``, ``{"summary": ...}``,
    or an already-shaped summary.  Row labels can be expressed as
    ``outcome='hit'|'miss'`` or ``directional_hit=True|False``.  Metric rows can
    provide ``poi_count``/``count`` plus ``directional_hits`` and
    ``directional_misses``.
    """
    existing = _existing_summary(content)
    if existing is not None:
        return existing

    rows = _row_list(content)
    if not rows:
        return empty_poi_accuracy_summary()

    by_horizon: dict[str, _MetricBucket] = {}
    by_setup_type: dict[tuple[str, str], _MetricBucket] = {}
    by_bias: dict[tuple[str, str], _MetricBucket] = {}
    by_ticker: dict[tuple[str, str], _MetricBucket] = {}
    by_score_tier: dict[tuple[str, str], _MetricBucket] = {}

    for row in rows:
        horizon = _horizon(row)
        _bucket(by_horizon, horizon).add(row)
        _bucket(by_setup_type, (horizon, _text(row, "setup_type", "poi_setup_type"))).add(row)
        _bucket(by_bias, (horizon, _text(row, "bias", "directional_bias"))).add(row)
        _bucket(by_ticker, (horizon, _ticker(row))).add(row)
        _bucket(by_score_tier, (horizon, _score_tier(row))).add(row)

    return {
        "contract_version": POI_ACCURACY_CONTRACT_VERSION,
        "horizons": [
            {"horizon": horizon, **bucket.as_dict()}
            for horizon, bucket in sorted(by_horizon.items(), key=lambda item: _horizon_sort_key(item[0]))
        ],
        "by_setup_type": _group_rows(by_setup_type, "setup_type"),
        "by_bias": _group_rows(by_bias, "bias"),
        "by_ticker": _group_rows(by_ticker, "ticker"),
        "by_score_tier": _group_rows(by_score_tier, "score_tier"),
    }


def load_poi_accuracy_summary(path: str | Path = DEFAULT_POI_ACCURACY_PATH) -> dict[str, Any]:
    """Load ``var/accuracy/poi_accuracy.json`` style content and summarise it."""
    p = Path(path)
    if not p.is_file():
        return {
            "status": "missing",
            "path": str(p),
            "summary": empty_poi_accuracy_summary(),
            "raw": {},
        }
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "status": "invalid",
            "path": str(p),
            "summary": empty_poi_accuracy_summary(),
            "raw": {},
            "error": str(exc),
        }
    return {
        "status": "ok",
        "path": str(p),
        "summary": summarize_poi_accuracy(raw),
        "raw": raw,
    }


@dataclass
class _MetricBucket:
    count: int = 0
    hits: int = 0
    misses: int = 0
    forward_returns: list[tuple[float, int]] = field(default_factory=list)
    mfes: list[tuple[float, int]] = field(default_factory=list)
    maes: list[tuple[float, int]] = field(default_factory=list)

    def add(self, row: Mapping[str, Any]) -> None:
        hits, misses = _directional_counts(row)
        count = max(_row_count(row), hits + misses)
        weight = max(1, count)
        self.count += count
        self.hits += hits
        self.misses += misses
        self._add_value(self.forward_returns, row, weight, "forward_return_pct", "fwd_return_pct", "return_pct")
        self._add_value(self.forward_returns, row, weight, "median_forward_return_pct")
        self._add_value(self.mfes, row, weight, "mfe_pct", "max_favorable_excursion_pct")
        self._add_value(self.mfes, row, weight, "median_mfe_pct")
        self._add_value(self.maes, row, weight, "mae_pct", "max_adverse_excursion_pct")
        self._add_value(self.maes, row, weight, "median_mae_pct")

    def as_dict(self) -> dict[str, Any]:
        decided = self.hits + self.misses
        return {
            "count": self.count,
            "poi_count": self.count,
            "directional_hits": self.hits,
            "directional_misses": self.misses,
            "directional_decided": decided,
            "directional_hit_rate": (self.hits / decided) if decided else None,
            "median_forward_return_pct": _weighted_median(self.forward_returns),
            "median_mfe_pct": _weighted_median(self.mfes),
            "median_mae_pct": _weighted_median(self.maes),
        }

    @staticmethod
    def _add_value(
        target: list[tuple[float, int]],
        row: Mapping[str, Any],
        weight: int,
        *keys: str,
    ) -> None:
        value = _first_float(row, *keys)
        if value is not None:
            target.append((value, weight))


def _bucket(buckets: dict[_BucketKey, _MetricBucket], key: _BucketKey) -> _MetricBucket:
    bucket = buckets.get(key)
    if bucket is None:
        bucket = _MetricBucket()
        buckets[key] = bucket
    return bucket


def _existing_summary(content: Any) -> dict[str, Any] | None:
    if not isinstance(content, Mapping):
        return None
    summary = content.get("summary")
    if isinstance(summary, Mapping):
        return _coerce_summary(summary)
    if any(key in content for key in _SUMMARY_GROUPS):
        return _coerce_summary(content)
    return None


def _coerce_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    out = empty_poi_accuracy_summary()
    for key in _SUMMARY_GROUPS:
        rows = summary.get(key)
        if isinstance(rows, list):
            out[key] = [dict(row) for row in rows if isinstance(row, Mapping)]
    return out


def _row_list(content: Any) -> list[Mapping[str, Any]]:
    if content is None:
        return []
    if isinstance(content, Mapping):
        for key in ("rows", "poi_rows", "metric_rows", "metrics", "items"):
            rows = content.get(key)
            if isinstance(rows, Iterable) and not isinstance(rows, (str, bytes, Mapping)):
                return [row for row in rows if isinstance(row, Mapping)]
        return []
    if isinstance(content, Iterable) and not isinstance(content, (str, bytes)):
        return [row for row in content if isinstance(row, Mapping)]
    return []


def _group_rows(groups: dict[tuple[str, str], _MetricBucket], key_name: str) -> list[dict[str, Any]]:
    rows = [
        {"horizon": horizon, key_name: bucket_key, **bucket.as_dict()}
        for (horizon, bucket_key), bucket in groups.items()
    ]
    return sorted(
        rows,
        key=lambda row: (
            _horizon_sort_key(row["horizon"]),
            -int(row["count"]),
            str(row[key_name]),
        ),
    )


def _directional_counts(row: Mapping[str, Any]) -> tuple[int, int]:
    hits = _first_int(row, "directional_hits", "hits")
    misses = _first_int(row, "directional_misses", "misses")
    decided = _first_int(row, "directional_decided", "decided")
    if hits is not None or misses is not None:
        hit_count = max(0, hits or 0)
        if misses is None and decided is not None:
            miss_count = max(0, decided - hit_count)
        else:
            miss_count = max(0, misses or 0)
        return hit_count, miss_count
    if decided is not None:
        rate = _first_float(row, "directional_hit_rate", "hit_rate")
        if rate is not None:
            hit_count = max(0, min(decided, round(decided * rate)))
            return hit_count, max(0, decided - hit_count)

    for key in ("directional_hit", "is_directional_hit", "hit", "is_hit"):
        value = row.get(key)
        if isinstance(value, bool):
            return (1, 0) if value else (0, 1)

    outcome = str(
        row.get("directional_outcome")
        or row.get("outcome")
        or row.get("label")
        or row.get("result")
        or ""
    ).lower()
    if outcome in {"hit", "directional_hit", "win", "success"}:
        return 1, 0
    if outcome in {"miss", "directional_miss", "loss", "fail", "failure"}:
        return 0, 1
    return 0, 0


def _row_count(row: Mapping[str, Any]) -> int:
    count = _first_int(row, "poi_count", "count", "signal_count")
    if count is None:
        return 1
    return max(0, count)


def _horizon(row: Mapping[str, Any]) -> str:
    value = _first_value(row, "horizon", "forward_bars", "forward_minutes", "forward_horizon")
    if value is None or str(value).strip() == "":
        return "all"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _ticker(row: Mapping[str, Any]) -> str:
    return _text(row, "ticker", "figi", "instrument_id")


def _score_tier(row: Mapping[str, Any]) -> str:
    value = _first_value(row, "score_tier", "interest_score_tier")
    if value is not None and str(value).strip():
        return str(value).strip()
    score = _first_float(row, "interest_score", "score")
    if score is None:
        return "unknown"
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium"
    return "low"


def _text(row: Mapping[str, Any], *keys: str) -> str:
    value = _first_value(row, *keys)
    if value is None or str(value).strip() == "":
        return "unknown"
    return str(value).strip()


def _first_value(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _first_int(row: Mapping[str, Any], *keys: str) -> int | None:
    value = _first_value(row, *keys)
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_float(row: Mapping[str, Any], *keys: str) -> float | None:
    value = _first_value(row, *keys)
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _weighted_median(values: list[tuple[float, int]]) -> float | None:
    weighted = [(value, max(1, int(weight))) for value, weight in values if weight > 0]
    if not weighted:
        return None
    weighted.sort(key=lambda item: item[0])
    total = sum(weight for _, weight in weighted)
    midpoint = (total - 1) / 2
    other_midpoint = total / 2
    seen = 0
    left: float | None = None
    right: float | None = None
    for value, weight in weighted:
        next_seen = seen + weight
        if left is None and midpoint < next_seen:
            left = value
        if right is None and other_midpoint < next_seen:
            right = value
            break
        seen = next_seen
    if left is None:
        left = weighted[-1][0]
    if right is None:
        right = weighted[-1][0]
    return (left + right) / 2


def _horizon_sort_key(value: str) -> tuple[int, float | str]:
    raw = str(value)
    numeric = raw[:-1] if raw.endswith("m") else raw
    try:
        return 0, float(numeric)
    except ValueError:
        return 1, raw

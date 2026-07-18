"""HTTP API поверх Postgres: последние сигналы и сводки по типам."""

from __future__ import annotations

import csv
import io
import json
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Iterator

import httpx
from dateutil.parser import isoparse
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator
import uvicorn

from ..admin_http_guard import AdminApiRateLimiter, admin_client_ip
from ..clickhouse_context import (
    fetch_instrument_insights,
    fetch_raw_events_window,
    fetch_source_health,
)
from ..config import RuntimeSettings, load_detector_config, load_instrument_configs
from ..delivery_policy import DeliveryPolicy
from ..market_unary import (
    RequestError as TinvestRequestError,
    fetch_market_values,
    fetch_tech_analysis,
    json_friendly,
    parse_indicator_interval,
    parse_indicator_type,
    parse_market_value_types_csv,
    parse_type_of_price,
    resolve_instrument_registry,
)
from ..logging_utils import configure_logging
from ..models import TriggerSignal
from ..runtime_info import runtime_fingerprint
from ..sinks import create_postgres_signal_store_with_retry

_ADMIN_HTML_PATH = (
    Path(__file__).resolve().parent.parent / "static" / "admin.html"
)
_ADMIN_JS_PATH = Path(__file__).resolve().parent.parent / "static" / "admin_app.js"
_ADMIN_CHART_JS_PATH = (
    Path(__file__).resolve().parent.parent
    / "static"
    / "vendor"
    / "chart.umd.min.js"
)


_SIGNAL_TYPE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "signal_type": "volume_spike",
        "source": "trade",
        "sources_any": ("trade",),
        "config": "volume_zscore_threshold",
        "delivery_rule": "momentum_quality_and_z",
        "enabled": lambda c: c.volume_zscore_threshold > 0,
    },
    {
        "signal_type": "trade_rate_spike",
        "source": "trade",
        "sources_any": ("trade",),
        "config": "trade_count_zscore_threshold",
        "delivery_rule": "momentum_quality_and_z",
        "enabled": lambda c: c.trade_count_zscore_threshold > 0,
    },
    {
        "signal_type": "price_jump",
        "source": "trade/last_price",
        "sources_any": ("trade", "last_price"),
        "config": "price_return_zscore_threshold",
        "delivery_rule": "price_extreme_or_activity_confirmed",
        "enabled": lambda c: c.price_return_zscore_threshold > 0,
    },
    {
        "signal_type": "spread_widening",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "spread_zscore_threshold",
        "delivery_rule": "liquidity_activity_confirmed",
        "enabled": lambda c: c.spread_zscore_threshold > 0,
    },
    {
        "signal_type": "orderbook_imbalance",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "imbalance_zscore_threshold",
        "delivery_rule": "liquidity_activity_confirmed",
        "enabled": lambda c: c.imbalance_zscore_threshold > 0,
    },
    {
        "signal_type": "microstructure_combo_long",
        "source": "trade+orderbook",
        "sources_all": ("trade", "orderbook"),
        "config": "combo_enabled",
        "delivery_rule": "combo_score",
        "enabled": lambda c: c.combo_enabled,
    },
    {
        "signal_type": "microstructure_combo_short",
        "source": "trade+orderbook",
        "sources_all": ("trade", "orderbook"),
        "config": "combo_enabled",
        "delivery_rule": "combo_score",
        "enabled": lambda c: c.combo_enabled,
    },
    {
        "signal_type": "trading_status_changed",
        "source": "trading_status",
        "sources_any": ("trading_status",),
        "config": "info subscription",
        "delivery_rule": "status_access_always",
        "enabled": lambda c: True,
    },
    {
        "signal_type": "market_access_changed",
        "source": "trading_status",
        "sources_any": ("trading_status",),
        "config": "track_market_access_flags",
        "delivery_rule": "status_access_always",
        "enabled": lambda c: c.track_market_access_flags,
    },
    {
        "signal_type": "obi_dynamics",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "obi_dynamics_enabled",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.obi_dynamics_enabled,
    },
    {
        "signal_type": "aggressive_trade_burst",
        "source": "trade",
        "sources_any": ("trade",),
        "config": "trade_burst_enabled",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.trade_burst_enabled,
    },
    {
        "signal_type": "orderbook_spoofing_bid_pull",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "spoofing_enabled",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.spoofing_enabled,
    },
    {
        "signal_type": "orderbook_spoofing_ask_pull",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "spoofing_enabled",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.spoofing_enabled,
    },
    {
        "signal_type": "lead_lag_divergence",
        "source": "trade/last_price/orderbook",
        "sources_any": ("trade", "last_price", "orderbook"),
        "config": "lead_lag_enabled + lead_lag.pairs",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.lead_lag_enabled,
        "requires_lead_lag_pairs": True,
    },
    {
        "signal_type": "open_interest_spike",
        "source": "open_interest",
        "sources_any": ("open_interest",),
        "config": "open_interest_zscore_threshold",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.open_interest_zscore_threshold > 0,
    },
    {
        "signal_type": "candle_range_spike",
        "source": "candle",
        "sources_any": ("candle",),
        "config": "candle_range_zscore_threshold",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.candle_range_zscore_threshold > 0,
    },
    {
        "signal_type": "price_near_limit_band",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "limit_band_warning_bps",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.limit_band_warning_bps > 0,
    },
    {
        "signal_type": "orderbook_snapshot_inconsistent",
        "source": "orderbook",
        "sources_any": ("orderbook",),
        "config": "signal_orderbook_inconsistent",
        "delivery_rule": "default_quality",
        "enabled": lambda c: c.signal_orderbook_inconsistent,
    },
)


def _configured_signal_catalog(settings: RuntimeSettings) -> dict[str, Any]:
    try:
        detector = load_detector_config(
            settings.detector_path,
            settings.detector_overrides_path,
        )
    except Exception as exc:
        return {
            "config_error": f"{type(exc).__name__}: {exc}",
            "instrument_count": 0,
            "source_coverage": {},
            "enabled_count": 0,
            "known_count": len(_SIGNAL_TYPE_DEFINITIONS),
            "enabled_types": [],
            "types": [],
        }

    instrument_error: str | None = None
    try:
        instruments = load_instrument_configs(settings.instruments_path)
    except Exception as exc:
        instruments = []
        instrument_error = f"{type(exc).__name__}: {exc}"

    source_coverage = {
        "trade": sum(1 for item in instruments if item.trades),
        "last_price": sum(1 for item in instruments if item.last_price),
        "orderbook": sum(1 for item in instruments if item.order_book_depth),
        "trading_status": sum(1 for item in instruments if item.info),
        "candle": sum(1 for item in instruments if item.candles),
        "open_interest": 0,
    }

    rows: list[dict[str, Any]] = []
    for definition in _SIGNAL_TYPE_DEFINITIONS:
        predicate = definition["enabled"]
        global_enabled = bool(predicate(detector.default))
        any_enabled = global_enabled or any(
            bool(predicate(item)) for item in detector.per_instrument.values()
        )
        if definition.get("requires_lead_lag_pairs"):
            any_enabled = any_enabled and bool(detector.lead_lag_pairs)
            global_enabled = global_enabled and bool(detector.lead_lag_pairs)

        source_count = _signal_source_count(definition, source_coverage)
        enabled = any_enabled and source_count > 0
        if global_enabled:
            scope = "global"
        elif any_enabled:
            scope = "per_instrument"
        else:
            scope = "disabled"
        reason = "enabled"
        if not any_enabled:
            reason = "config_disabled"
        elif source_count <= 0:
            reason = "source_not_subscribed"

        rows.append(
            {
                "signal_type": definition["signal_type"],
                "source": definition["source"],
                "source_coverage": source_count,
                "config": definition["config"],
                "enabled": enabled,
                "scope": scope,
                "reason": reason,
                "delivery_rule": definition["delivery_rule"],
            }
        )

    enabled_rows = [row for row in rows if row["enabled"]]
    return {
        "instrument_count": len(instruments),
        "source_coverage": source_coverage,
        "instrument_error": instrument_error,
        "per_instrument_overrides": len(detector.per_instrument),
        "lead_lag_pairs": len(detector.lead_lag_pairs),
        "enabled_count": len(enabled_rows),
        "known_count": len(rows),
        "enabled_types": enabled_rows,
        "types": rows,
    }


def _signal_source_count(
    definition: dict[str, Any],
    source_coverage: dict[str, int],
) -> int:
    all_sources = tuple(definition.get("sources_all") or ())
    if all_sources:
        return min(source_coverage.get(name, 0) for name in all_sources)
    any_sources = tuple(definition.get("sources_any") or ())
    if any_sources:
        return max(source_coverage.get(name, 0) for name in any_sources)
    return 0


def _configured_instrument_catalog(settings: RuntimeSettings) -> dict[str, Any]:
    try:
        instruments = load_instrument_configs(settings.instruments_path)
    except Exception as exc:
        return {
            "config_error": f"{type(exc).__name__}: {exc}",
            "count": 0,
            "source_coverage": {},
            "items": [],
        }

    items = []
    for item in instruments:
        sources = []
        if item.trades:
            sources.append("trade")
        if item.last_price:
            sources.append("last_price")
        if item.order_book_depth:
            sources.append("orderbook")
        if item.info:
            sources.append("trading_status")
        if item.candles:
            sources.append("candle")
        items.append(
            {
                "instrument_id": item.instrument_id,
                "ticker": item.ticker,
                "class_code": item.class_code,
                "alias": item.alias,
                "subscriptions": {
                    "trades": item.trades,
                    "last_price": item.last_price,
                    "info": item.info,
                    "order_book_depth": item.order_book_depth,
                    "candles": item.candles,
                    "candle_interval": item.candle_interval,
                },
                "sources": sources,
            }
        )

    return {
        "count": len(items),
        "source_coverage": {
            "trade": sum(1 for item in instruments if item.trades),
            "last_price": sum(1 for item in instruments if item.last_price),
            "orderbook": sum(1 for item in instruments if item.order_book_depth),
            "trading_status": sum(1 for item in instruments if item.info),
            "candle": sum(1 for item in instruments if item.candles),
        },
        "items": items,
    }


def _source_health_response(
    settings: RuntimeSettings,
    *,
    minutes: int,
    stale_after_minutes: int,
) -> dict[str, Any]:
    catalog = _configured_instrument_catalog(settings)
    raw_health_status = "unknown"
    raw_error: str | None = None
    raw_rows: list[dict[str, Any]] = []
    if settings.clickhouse_http_url:
        try:
            raw_rows = fetch_source_health(
                settings.clickhouse_http_url,
                minutes=minutes,
                username=settings.clickhouse_http_username,
                password=settings.clickhouse_http_password,
            )
            raw_health_status = "ok"
        except httpx.HTTPStatusError as exc:
            raw_health_status = "error"
            raw_error = f"ClickHouse HTTP {exc.response.status_code}"
        except httpx.RequestError as exc:
            raw_health_status = "error"
            raw_error = f"ClickHouse недоступен: {exc}"

    last_by_instrument: dict[str, dict[str, dict[str, Any]]] = {}
    for row in raw_rows:
        iid = str(row.get("instrument_id") or "")
        source = str(row.get("event_type") or "")
        if not iid or not source:
            continue
        last_by_instrument.setdefault(iid, {})[source] = {
            "last_source_time": row.get("last_source_time"),
            "event_count": int(row.get("event_count") or 0),
        }

    now = datetime.now(timezone.utc)
    try:
        detector = load_detector_config(
            settings.detector_path,
            settings.detector_overrides_path,
        )
        detector_error = None
    except Exception as exc:
        detector = None
        detector_error = f"{type(exc).__name__}: {exc}"

    items: list[dict[str, Any]] = []
    for raw_item in catalog.get("items", []):
        item = dict(raw_item)
        iid = str(item.get("instrument_id") or "")
        subscriptions = item.get("subscriptions") if isinstance(item.get("subscriptions"), dict) else {}
        source_status = _instrument_source_status(
            subscriptions,
            last_by_instrument.get(iid, {}),
            now=now,
            stale_after_minutes=stale_after_minutes,
            clickhouse_status=raw_health_status,
        )
        availability = _instrument_signal_availability(
            iid,
            subscriptions,
            source_status,
            detector=detector,
            detector_error=detector_error,
        )
        item["source_health"] = source_status
        item["signal_availability"] = availability
        item["impossible_signal_types"] = [
            row for row in availability if not bool(row.get("enabled"))
        ]
        items.append(item)

    ok_sources = sum(
        1
        for item in items
        for source in (item.get("source_health") or {}).values()
        if source.get("status") == "ok"
    )
    return {
        "status": raw_health_status,
        "error": raw_error,
        "minutes": minutes,
        "stale_after_minutes": stale_after_minutes,
        "count": len(items),
        "ok_source_count": ok_sources,
        "items": items,
        "config_error": catalog.get("config_error"),
        "detector_error": detector_error,
    }


def _instrument_source_status(
    subscriptions: dict[str, Any],
    raw_events: dict[str, dict[str, Any]],
    *,
    now: datetime,
    stale_after_minutes: int,
    clickhouse_status: str,
) -> dict[str, dict[str, Any]]:
    subscribed = {
        "trade": bool(subscriptions.get("trades")),
        "last_price": bool(subscriptions.get("last_price")),
        "orderbook": bool(subscriptions.get("order_book_depth")),
        "trading_status": bool(subscriptions.get("info")),
        "candle": bool(subscriptions.get("candles")),
        "open_interest": bool(raw_events.get("open_interest")),
    }
    out: dict[str, dict[str, Any]] = {}
    stale_delta = timedelta(minutes=max(1, int(stale_after_minutes)))
    for source, is_subscribed in subscribed.items():
        row = raw_events.get(source)
        last_raw = row.get("last_source_time") if row else None
        last_dt = _parse_dt(last_raw)
        if not is_subscribed:
            status = "not_subscribed"
        elif clickhouse_status != "ok":
            status = "unknown"
        elif last_dt is None:
            status = "missing"
        elif now - last_dt > stale_delta:
            status = "stale"
        else:
            status = "ok"
        out[source] = {
            "subscribed": is_subscribed,
            "status": status,
            "last_source_time": last_dt.isoformat() if last_dt else last_raw,
            "event_count": int(row.get("event_count") or 0) if row else 0,
        }
    return out


def _instrument_signal_availability(
    instrument_id: str,
    subscriptions: dict[str, Any],
    source_status: dict[str, dict[str, Any]],
    *,
    detector: Any,
    detector_error: str | None,
) -> list[dict[str, Any]]:
    cfg = None
    if detector is not None:
        cfg = detector.per_instrument.get(instrument_id, detector.default)
    rows: list[dict[str, Any]] = []
    for definition in _SIGNAL_TYPE_DEFINITIONS:
        st = str(definition["signal_type"])
        if detector_error:
            rows.append(
                {
                    "signal_type": st,
                    "enabled": False,
                    "reason": "config_error",
                    "source": definition.get("source"),
                }
            )
            continue
        predicate = definition["enabled"]
        config_enabled = bool(predicate(cfg)) if cfg is not None else False
        if definition.get("requires_lead_lag_pairs"):
            config_enabled = config_enabled and bool(detector.lead_lag_pairs)
        if not config_enabled:
            rows.append(
                {
                    "signal_type": st,
                    "enabled": False,
                    "reason": "config_disabled",
                    "source": definition.get("source"),
                }
            )
            continue
        sources_all = tuple(definition.get("sources_all") or ())
        sources_any = tuple(definition.get("sources_any") or ())
        required = sources_all or sources_any
        if sources_all:
            source_reason = _all_sources_reason(sources_all, source_status)
        else:
            source_reason = _any_source_reason(sources_any, source_status)
        rows.append(
            {
                "signal_type": st,
                "enabled": source_reason == "enabled",
                "reason": source_reason,
                "source": definition.get("source"),
                "required_sources": list(required),
            }
        )
    return rows


def _all_sources_reason(
    sources: tuple[str, ...],
    source_status: dict[str, dict[str, Any]],
) -> str:
    statuses = [source_status.get(source, {}).get("status") for source in sources]
    if any(status == "not_subscribed" for status in statuses):
        return "source_not_subscribed"
    if any(status == "unknown" for status in statuses):
        return "source_unknown"
    if any(status in {"missing", "stale"} for status in statuses):
        return "source_stale"
    return "enabled"


def _any_source_reason(
    sources: tuple[str, ...],
    source_status: dict[str, dict[str, Any]],
) -> str:
    if not sources:
        return "enabled"
    statuses = [source_status.get(source, {}).get("status") for source in sources]
    if any(status == "ok" for status in statuses):
        return "enabled"
    if all(status == "not_subscribed" for status in statuses):
        return "source_not_subscribed"
    if any(status == "unknown" for status in statuses):
        return "source_unknown"
    return "source_stale"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = isoparse(str(value))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _accuracy_response_from_path(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "status": "missing",
            "path": str(path),
            "summary": {
                "horizons": [],
                "by_type": [],
                "by_ticker": [],
                "by_quality_tier": [],
                "by_delivery_status": [],
            },
            "raw": {},
        }
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Невалидный JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raw = {"value": raw}
    return {
        "status": "ok",
        "path": str(path),
        "summary": _accuracy_summary(raw),
        "raw": raw,
    }


def _accuracy_summary(raw: dict[str, Any]) -> dict[str, Any]:
    horizons: list[dict[str, Any]] = []
    by_horizon = raw.get("by_horizon")
    if isinstance(by_horizon, dict):
        for horizon, block in sorted(by_horizon.items(), key=lambda x: str(x[0])):
            if isinstance(block, dict):
                horizons.append(
                    {
                        "horizon": str(horizon),
                        "directional_hit_rate": block.get("directional_hit_rate"),
                        "directional_hits": block.get("directional_hits"),
                        "directional_misses": block.get("directional_misses"),
                        "directional_decided": block.get("directional_decided"),
                    }
                )
    else:
        horizons.append(
            {
                "horizon": str(raw.get("forward_bars", "1")),
                "directional_hit_rate": raw.get("directional_hit_rate"),
                "directional_hits": raw.get("directional_hits"),
                "directional_misses": raw.get("directional_misses"),
                "directional_decided": raw.get("directional_decided"),
            }
        )
    return {
        "horizons": horizons,
        "by_type": _accuracy_rows(raw, "by_type"),
        "by_ticker": _accuracy_rows(raw, "by_ticker"),
        "by_quality_tier": _accuracy_rows(raw, "by_quality_tier"),
        "by_delivery_status": _accuracy_rows(raw, "by_delivery_status"),
    }


def _accuracy_rows(raw: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = raw.get(key)
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    by_horizon = raw.get("by_horizon")
    if not isinstance(by_horizon, dict):
        return []
    out: list[dict[str, Any]] = []
    for horizon, block in by_horizon.items():
        if isinstance(block, dict) and isinstance(block.get(key), list):
            for row in block[key]:
                if isinstance(row, dict):
                    out.append({"horizon": str(horizon), **row})
    return out


class HealthResponse(BaseModel):
    """Ответ проверки живости процесса (без запроса к Postgres)."""

    status: str = Field(
        description="Обычно `ok`, если процесс принимает HTTP.",
    )
    runtime: dict[str, Any] = Field(
        default_factory=dict,
        description="Build/runtime fingerprint: app_version, commit_sha, build_time.",
    )


class ReadyResponse(BaseModel):
    """Готовность к трафику: проверка соединения с Postgres."""

    status: str = Field(description="`ready`, если БД отвечает на ping.")
    runtime: dict[str, Any] = Field(default_factory=dict)


class RecentSignalsResponse(BaseModel):
    """Список последних сигналов из таблицы Postgres."""

    items: list[dict[str, Any]] = Field(
        description=(
            "Записи сигналов в том же виде, что возвращает хранилище."
        ),
    )
    count: int = Field(description="Длина списка `items`.")


class SignalSummaryRow(BaseModel):
    """Одна строка агрегированной статистики по типу сигнала."""

    signal_type: str = Field(
        description="Имя типа сигнала (`signal_type`).",
    )
    signal_count: int = Field(
        description="Число срабатываний за окно.",
    )


class SignalSummaryResponse(BaseModel):
    """Сводка по типам сигналов за последние `minutes` минут."""

    items: list[SignalSummaryRow] = Field(
        description=(
            "Строки сводки, отсортированные по убыванию счётчика."
        ),
    )
    minutes: int = Field(
        description="Размер временного окна запроса в минутах.",
    )


class AdminFeedbackIn(BaseModel):
    """Разметка сигнала в админке."""

    signal_id: str
    label: str = Field(description="useful | noise | unsure")
    note: str = ""

    @field_validator("signal_id")
    @classmethod
    def _signal_uuid(cls, v: str) -> str:
        s = v.strip()
        uuid.UUID(s)
        return s

    @field_validator("label")
    @classmethod
    def _label_ok(cls, v: str) -> str:
        allowed = {"useful", "noise", "unsure"}
        if v.strip() not in allowed:
            raise ValueError(f"label must be one of: {', '.join(sorted(allowed))}")
        return v.strip()


class DeliverySimulationIn(BaseModel):
    """Dry-run delivery settings over recent stored signals."""

    preset: str = Field(
        default="current",
        description="current | conservative | admin_only_rollout",
    )
    type_rules_json: str = Field(default="")
    min_quality: int | None = Field(default=None, ge=0, le=100)
    max_per_hour: int | None = Field(default=None, ge=0, le=1000)
    instrument_cooldown_seconds: int | None = Field(default=None, ge=0, le=86_400)
    minutes: int = Field(default=1440, ge=0, le=10_080)
    limit: int = Field(default=200, ge=1, le=200)

    @field_validator("preset")
    @classmethod
    def _preset_ok(cls, v: str) -> str:
        value = (v or "current").strip().lower()
        allowed = {"current", "conservative", "admin_only_rollout"}
        if value not in allowed:
            raise ValueError(f"preset must be one of: {', '.join(sorted(allowed))}")
        return value


_ADMIN_ONLY_ROLLOUT_RULES = {
    "candle_range_spike": {"admin_only": True},
    "obi_dynamics": {"admin_only": True},
    "open_interest_spike": {"admin_only": True},
    "aggressive_trade_burst": {"admin_only": True},
    "lead_lag_divergence": {"admin_only": True},
}


def _simulation_settings(
    settings: RuntimeSettings,
    body: DeliverySimulationIn,
) -> RuntimeSettings:
    min_quality = (
        90 if body.preset == "conservative" else settings.signal_delivery_min_quality
    )
    max_per_hour = settings.signal_delivery_max_per_hour
    cooldown = settings.signal_delivery_instrument_cooldown_seconds
    type_rules = settings.signal_delivery_type_rules_json
    if body.preset == "admin_only_rollout":
        try:
            parsed_rules = json.loads(type_rules or "{}")
        except json.JSONDecodeError:
            parsed_rules = {}
        if not isinstance(parsed_rules, dict):
            parsed_rules = {}
        parsed_rules.update(_ADMIN_ONLY_ROLLOUT_RULES)
        type_rules = json.dumps(parsed_rules, ensure_ascii=False)
    if body.min_quality is not None:
        min_quality = int(body.min_quality)
    if body.max_per_hour is not None:
        max_per_hour = int(body.max_per_hour)
    if body.instrument_cooldown_seconds is not None:
        cooldown = int(body.instrument_cooldown_seconds)
    if body.type_rules_json.strip():
        type_rules = body.type_rules_json.strip()
    return replace(
        settings,
        signal_delivery_enabled=True,
        signal_delivery_min_quality=min_quality,
        signal_delivery_min_quality_raw=str(min_quality),
        signal_delivery_max_per_hour=max_per_hour,
        signal_delivery_instrument_cooldown_seconds=cooldown,
        signal_delivery_type_rules_json=type_rules,
    )


def _row_to_signal(row: dict[str, Any]) -> TriggerSignal:
    detected_at = _parse_dt(row.get("detected_at")) or datetime.now(timezone.utc)
    return TriggerSignal(
        signal_id=str(row.get("signal_id") or uuid.uuid4()),
        detected_at=detected_at,
        instrument_id=str(row.get("instrument_id") or ""),
        ticker=str(row.get("ticker") or ""),
        class_code=str(row.get("class_code") or ""),
        alias=str(row.get("alias") or row.get("ticker") or ""),
        source_event_type=str(row.get("source_event_type") or ""),
        signal_type=str(row.get("signal_type") or ""),
        severity=int(row.get("severity") or 1),
        metric_value=float(row.get("metric_value") or 0.0),
        baseline_value=float(row.get("baseline_value") or 0.0),
        z_score=float(row.get("z_score") or 0.0),
        window_seconds=int(row.get("window_seconds") or 0),
        summary=str(row.get("summary") or ""),
        payload=dict(row.get("payload") or {}),
        source_event_id=(
            str(row["source_event_id"])
            if row.get("source_event_id") is not None
            else None
        ),
        source_event_at=_parse_dt(row.get("source_event_at")),
        signal_schema_version=str(
            row.get("signal_schema_version") or "1.0.0"
        ),
        expectation_catalog_version=(
            str(row["expectation_catalog_version"])
            if row.get("expectation_catalog_version") is not None
            else None
        ),
        detector_config_version=(
            str(row["detector_config_version"])
            if row.get("detector_config_version") is not None
            else None
        ),
        delivery_config_version=(
            str(row["delivery_config_version"])
            if row.get("delivery_config_version") is not None
            else None
        ),
        cost_model_version=(
            str(row["cost_model_version"])
            if row.get("cost_model_version") is not None
            else None
        ),
        provenance_status=str(row.get("provenance_status") or "legacy"),
    )


def _delivery_simulation_summary(
    rows: list[dict[str, Any]],
    *,
    total: int,
    sampled: int,
    preset: str,
    minutes: int,
) -> dict[str, Any]:
    by_status = _count_key(rows, "simulated_delivery_status")
    by_channel = _count_key(rows, "simulated_delivery_channel")
    by_priority = _count_key(rows, "simulated_delivery_priority")
    by_reason = _count_key(rows, "simulated_delivery_reason")
    changed = [
        row
        for row in rows
        if row.get("current_delivery_status") != row.get("simulated_delivery_status")
        or row.get("current_delivery_reason") != row.get("simulated_delivery_reason")
    ]
    return {
        "preset": preset,
        "minutes": minutes,
        "total_available": total,
        "sampled": sampled,
        "by_status": by_status,
        "by_channel": by_channel,
        "by_priority": by_priority,
        "by_reason": by_reason,
        "changed_count": len(changed),
        "changed_sample": changed[:20],
        "items": rows[:50],
    }


def _count_key(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return [
        {"key": key_value, "count": count}
        for key_value, count in sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    ]


def _iso_age_seconds(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = isoparse(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))


def _redis_ping_status(redis_url: str | None) -> tuple[bool | None, str]:
    if not redis_url:
        return None, "REDIS_URL не задан; detector продолжит без сохранения state."
    try:
        import redis

        redis.Redis.from_url(redis_url, decode_responses=True).ping()
    except Exception as exc:
        return False, f"Redis недоступен: {type(exc).__name__}: {exc}"
    return True, "Redis доступен; detector state можно загрузить и сохранить."


def _pipeline_status(checks: list[dict[str, Any]]) -> str:
    statuses = {str(check.get("status") or "unknown") for check in checks}
    if "critical" in statuses:
        return "critical"
    if "warning" in statuses:
        return "warning"
    return "ok"


def require_admin(
    request: Request,
    token: Annotated[str | None, Query(description="Значение ADMIN_API_TOKEN")] = None,
    x_admin_token: Annotated[
        str | None, Header(alias="X-Admin-Token")
    ] = None,
) -> None:
    """Проверка токена для JSON-эндпоинтов админки."""
    settings: RuntimeSettings = request.app.state.settings
    expected = settings.admin_api_token
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Админ API отключён: задайте переменную окружения ADMIN_API_TOKEN.",
        )
    provided = (token or x_admin_token or "").strip()
    if provided != expected:
        raise HTTPException(
            status_code=401,
            detail=(
                "Неверный или отсутствующий токен: query-параметр `token` "
                "или заголовок `X-Admin-Token`."
            ),
        )


def create_app() -> FastAPI:
    settings = RuntimeSettings.from_env(service_name="api")
    runtime = runtime_fingerprint()
    configure_logging(settings.log_level)
    fastapi_app = FastAPI(
        title="T-Invest Signal API",
        version=str(runtime.get("app_version") or "0.1.0"),
        description=(
            "Чтение накопленных аномалий рынка (сигналов), "
            "записанных сервисом детектора в Postgres. "
            "Источник данных — T-Invest MarketDataStream → Kafka → детектор."
        ),
        openapi_tags=[
            {
                "name": "health",
                "description": (
                    "Проверка доступности HTTP (`/health`) и готовности к "
                    "запросам с данными (`/ready`, Postgres)."
                ),
            },
            {
                "name": "signals",
                "description": "Выборки и агрегаты по таблице сигналов.",
            },
            {
                "name": "admin",
                "description": (
                    "Аналитика сигналов для настройки качества "
                    "(требуется ADMIN_API_TOKEN)."
                ),
            },
        ],
    )

    @fastapi_app.on_event("startup")
    def startup() -> None:
        fastapi_app.state.settings = settings
        fastapi_app.state.runtime = runtime
        fastapi_app.state.signal_store = create_postgres_signal_store_with_retry(
            settings,
            service_name="api",
        )
        fastapi_app.state.instrument_registry_cache = None

    _admin_limiter = AdminApiRateLimiter(settings.admin_api_rate_limit_per_minute)
    _admin_ips = settings.admin_api_allowed_ips

    @fastapi_app.middleware("http")
    async def admin_api_guard(request: Request, call_next):
        if not request.url.path.startswith("/admin/api"):
            return await call_next(request)
        ip = admin_client_ip(request)
        if _admin_ips is not None and ip not in _admin_ips:
            return JSONResponse(
                status_code=403,
                content={"detail": "admin api: IP not allowed"},
            )
        if not _admin_limiter.allow(ip or "unknown"):
            return JSONResponse(
                status_code=429,
                content={"detail": "admin api: rate limit exceeded"},
            )
        return await call_next(request)

    @fastapi_app.on_event("shutdown")
    def shutdown() -> None:
        signal_store = getattr(fastapi_app.state, "signal_store", None)
        if signal_store is not None:
            signal_store.close()

    @fastapi_app.get(
        "/health",
        tags=["health"],
        summary="Проверка живости",
        response_model=HealthResponse,
        responses={200: {"description": "Сервис принимает запросы."}},
    )
    def health() -> HealthResponse:
        """Возвращает статус без обращения к базе данных."""
        return HealthResponse(status="ok", runtime=runtime)

    @fastapi_app.get(
        "/ready",
        tags=["health"],
        summary="Готовность (Postgres)",
        response_model=ReadyResponse,
        responses={
            200: {"description": "База доступна."},
            503: {"description": "Нет соединения с Postgres."},
        },
    )
    def ready(request: Request) -> ReadyResponse:
        """Проверка ``SELECT 1`` через пул сигналов (для k8s/orchestrator readiness)."""
        store = request.app.state.signal_store
        try:
            ok = bool(store.ping())
        except Exception:
            ok = False
        if not ok:
            raise HTTPException(
                status_code=503,
                detail="postgres_unavailable",
            )
        return ReadyResponse(status="ready", runtime=runtime)

    @fastapi_app.get(
        "/signals/recent",
        tags=["signals"],
        summary="Последние сигналы",
        response_model=RecentSignalsResponse,
        responses={
            200: {"description": "Выборка из Postgres по убыванию времени."},
        },
    )
    def recent_signals(
        limit: Annotated[
            int,
            Query(
                ge=1,
                le=500,
                description="Максимум строк (ограничено и на стороне SQL).",
            ),
        ] = 50,
        instrument_id: Annotated[
            str | None,
            Query(
                description=(
                    "Фильтр по `instrument_id` (например `SBER_TQBR`). "
                    "Если не задан — все инструменты."
                ),
            ),
        ] = None,
    ) -> RecentSignalsResponse:
        """Последние сигналы; опционально фильтр по инструменту."""
        rows = fastapi_app.state.signal_store.fetch_recent(
            limit=limit, instrument_id=instrument_id
        )
        return RecentSignalsResponse(items=rows, count=len(rows))

    @fastapi_app.get(
        "/signals/summary",
        tags=["signals"],
        summary="Сводка по типам сигналов",
        response_model=SignalSummaryResponse,
        responses={
            200: {
                "description": (
                    "Группировка COUNT по `signal_type` за окно времени."
                ),
            },
        },
    )
    def signal_summary(
        minutes: Annotated[
            int,
            Query(
                ge=1,
                le=1440,
                description=(
                    "Окно в минутах от текущего момента "
                    "(UTC на стороне БД)."
                ),
            ),
        ] = 60,
    ) -> SignalSummaryResponse:
        """COUNT по каждому `signal_type` за указанный период."""
        raw_rows = fastapi_app.state.signal_store.fetch_summary(minutes=minutes)
        items = [SignalSummaryRow(**row) for row in raw_rows]
        return SignalSummaryResponse(items=items, minutes=minutes)

    @fastapi_app.get(
        "/admin",
        include_in_schema=False,
        response_class=HTMLResponse,
    )
    @fastapi_app.get(
        "/admin/",
        include_in_schema=False,
        response_class=HTMLResponse,
    )
    def admin_dashboard() -> HTMLResponse:
        """Интерактивная страница статистики (токен вводится в форме)."""
        if not _ADMIN_HTML_PATH.is_file():
            raise HTTPException(
                status_code=500,
                detail="Файл админки не найден на сервере.",
            )
        return HTMLResponse(_ADMIN_HTML_PATH.read_text(encoding="utf-8"))

    @fastapi_app.get(
        "/admin/admin_app.js",
        include_in_schema=False,
    )
    def admin_bundle_js() -> Response:
        """Клиентский SPA-бандл админки (без секретов)."""
        if not _ADMIN_JS_PATH.is_file():
            raise HTTPException(status_code=404, detail="admin_app.js not found")
        return Response(
            content=_ADMIN_JS_PATH.read_bytes(),
            media_type="application/javascript; charset=utf-8",
        )

    @fastapi_app.get(
        "/admin/vendor/chart.umd.min.js",
        include_in_schema=False,
    )
    def admin_chart_vendor() -> Response:
        """Chart.js с того же origin (без CDN), для сред без доступа к jsdelivr."""
        if not _ADMIN_CHART_JS_PATH.is_file():
            raise HTTPException(
                status_code=404,
                detail="chart.umd.min.js not found (run: curl vendor bundle into static/vendor/).",
            )
        return Response(
            content=_ADMIN_CHART_JS_PATH.read_bytes(),
            media_type="application/javascript; charset=utf-8",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @fastapi_app.get(
        "/admin/api/overview",
        tags=["admin"],
        summary="Сводка для дашборда",
        dependencies=[Depends(require_admin)],
    )
    def admin_overview(
        minutes: Annotated[
            int,
            Query(
                ge=0,
                le=10_080,
                description="Окно в минутах; 0 — вся таблица (без фильтра по времени).",
            ),
        ] = 0,
    ) -> dict[str, Any]:
        return fastapi_app.state.signal_store.fetch_admin_overview(minutes=minutes)

    @fastapi_app.get(
        "/admin/api/signals",
        tags=["admin"],
        summary="Страница сигналов для таблицы",
        dependencies=[Depends(require_admin)],
    )
    def admin_signals(
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
        minutes: Annotated[
            int,
            Query(
                ge=0,
                le=10_080,
                description="0 — все сигналы в таблице.",
            ),
        ] = 0,
        instrument_id: Annotated[
            str | None,
            Query(description="Фильтр по instrument_id."),
        ] = None,
        signal_type: Annotated[
            str | None,
            Query(description="Фильтр по signal_type."),
        ] = None,
        min_quality: Annotated[
            float | None,
            Query(description="Минимум quality_score из payload."),
        ] = None,
        quality_min: Annotated[float | None, Query()] = None,
        quality_max: Annotated[float | None, Query()] = None,
        delivery_status: Annotated[str | None, Query()] = None,
        feedback: Annotated[str | None, Query()] = None,
        severity: Annotated[int | None, Query(ge=1, le=3)] = None,
    ) -> dict[str, Any]:
        items, total = fastapi_app.state.signal_store.fetch_admin_signals_page(
            limit=limit,
            offset=offset,
            minutes=minutes,
            instrument_id=instrument_id,
            signal_type=signal_type,
            min_quality=min_quality,
            quality_min=quality_min,
            quality_max=quality_max,
            delivery_status=delivery_status,
            feedback=feedback,
            severity=severity,
        )
        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    @fastapi_app.get(
        "/admin/api/slices",
        tags=["admin"],
        summary="Доп. разрезы (heatmap UTC, повторы)",
        dependencies=[Depends(require_admin)],
    )
    def admin_slices(
        minutes: Annotated[
            int,
            Query(ge=0, le=10_080, description="0 — вся таблица."),
        ] = 0,
    ) -> dict[str, Any]:
        return fastapi_app.state.signal_store.fetch_admin_slices(minutes=minutes)

    @fastapi_app.get(
        "/admin/api/delivery/overview",
        tags=["admin"],
        summary="Сводка delivery policy",
        dependencies=[Depends(require_admin)],
    )
    def admin_delivery_overview(
        minutes: Annotated[int, Query(ge=0, le=10_080)] = 0,
    ) -> dict[str, Any]:
        return fastapi_app.state.signal_store.fetch_admin_delivery_overview(
            minutes=minutes
        )

    @fastapi_app.get(
        "/admin/api/pipeline/status",
        tags=["admin"],
        summary="Статус контура сигналов и Telegram-доставки",
        dependencies=[Depends(require_admin)],
    )
    def admin_pipeline_status(
        request: Request,
        minutes: Annotated[int, Query(ge=1, le=10_080)] = 1440,
    ) -> dict[str, Any]:
        s = request.app.state.settings
        store = request.app.state.signal_store
        checks: list[dict[str, Any]] = []

        try:
            postgres_ok = bool(store.ping())
        except Exception as exc:
            postgres_ok = False
            postgres_detail = f"Postgres недоступен: {type(exc).__name__}: {exc}"
        else:
            postgres_detail = (
                "Postgres доступен; detector может сохранять сигналы."
            )
        checks.append(
            {
                "id": "postgres",
                "label": "Storage",
                "status": "ok" if postgres_ok else "critical",
                "detail": postgres_detail,
            }
        )

        redis_ok, redis_detail = _redis_ping_status(s.redis_url)
        checks.append(
            {
                "id": "redis",
                "label": "Detector state",
                "status": (
                    "ok" if redis_ok is True else "warning"
                    if redis_ok is None else "warning"
                ),
                "detail": redis_detail,
            }
        )

        telegram_configured = bool(s.telegram_bot_token and s.telegram_chat_id)
        checks.append(
            {
                "id": "telegram_config",
                "label": "Telegram config",
                "status": "ok" if telegram_configured else "critical",
                "detail": (
                    "TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID заданы."
                    if telegram_configured
                    else "Telegram отключён: задайте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID."
                ),
            }
        )

        checks.append(
            {
                "id": "delivery_policy",
                "label": "Delivery policy",
                "status": "ok" if s.signal_delivery_enabled else "critical",
                "detail": (
                    "SIGNAL_DELIVERY_ENABLED=true; delivered сигналы уходят в sinks."
                    if s.signal_delivery_enabled
                    else "SIGNAL_DELIVERY_ENABLED=false; внешняя отправка выключена."
                ),
            }
        )

        overview = store.fetch_admin_overview(minutes=minutes)
        delivery = store.fetch_admin_delivery_overview(minutes=minutes)
        totals = overview.get("totals") or {}
        delivery_totals = delivery.get("totals") or {}
        recent_delivered = delivery.get("recent_delivered") or []
        last_signal_at = totals.get("last_detected_at")
        last_delivered_at = (
            recent_delivered[0].get("detected_at") if recent_delivered else None
        )
        generated = int(totals.get("total") or 0)
        delivered = int(delivery_totals.get("delivered") or 0)
        last_signal_age_seconds = _iso_age_seconds(last_signal_at)
        last_delivered_age_seconds = _iso_age_seconds(last_delivered_at)

        checks.append(
            {
                "id": "signal_generation",
                "label": "Signal generation",
                "status": "ok" if generated > 0 else "warning",
                "detail": (
                    f"За период сохранено сигналов: {generated}."
                    if generated > 0
                    else "За период нет сохранённых сигналов; проверьте ingestor/detector/Kafka или торговые часы."
                ),
                "last_at": last_signal_at,
                "age_seconds": last_signal_age_seconds,
            }
        )

        if delivered > 0:
            delivery_status = "ok"
            delivery_detail = f"За период delivered сигналов: {delivered}."
        elif generated > 0:
            delivery_status = "warning"
            delivery_detail = (
                "Сигналы сохраняются, но delivered за период нет; чаще всего их "
                "подавила delivery policy. Смотрите вкладку Delivery / Reasons."
            )
        else:
            delivery_status = "warning"
            delivery_detail = "Нет generated сигналов, поэтому Telegram-доставка не проверялась."
        checks.append(
            {
                "id": "telegram_delivery",
                "label": "Telegram delivery",
                "status": delivery_status,
                "detail": delivery_detail,
                "last_at": last_delivered_at,
                "age_seconds": last_delivered_age_seconds,
            }
        )

        status = _pipeline_status(checks)
        return {
            "runtime": request.app.state.runtime,
            "minutes": minutes,
            "status": status,
            "headline": {
                "ok": "Контур сигналов и Telegram-доставки выглядит рабочим.",
                "warning": "Контур работает с предупреждениями; детали ниже.",
                "critical": "Есть блокирующая проблема для Telegram-доставки.",
            }[status],
            "incident_note": (
                "Предыдущий сбой был таким: detector не мог стартовать без "
                "Postgres, а Redis тоже был остановлен; raw-события шли в Kafka, "
                "но новые сигналы не сохранялись и до Telegram не доходили."
            ),
            "checks": checks,
            "metrics": {
                "generated": generated,
                "delivered": delivered,
                "suppressed": int(delivery_totals.get("suppressed") or 0),
                "delivery_rate": delivery_totals.get("delivery_rate") or 0,
                "last_signal_at": last_signal_at,
                "last_signal_age_seconds": last_signal_age_seconds,
                "last_delivered_at": last_delivered_at,
                "last_delivered_age_seconds": last_delivered_age_seconds,
            },
        }

    @fastapi_app.get(
        "/admin/api/delivery/reasons",
        tags=["admin"],
        summary="Причины delivered/suppressed",
        dependencies=[Depends(require_admin)],
    )
    def admin_delivery_reasons(
        minutes: Annotated[int, Query(ge=0, le=10_080)] = 0,
    ) -> dict[str, Any]:
        return fastapi_app.state.signal_store.fetch_admin_delivery_reasons(
            minutes=minutes
        )

    @fastapi_app.post(
        "/admin/api/delivery/simulation",
        tags=["admin"],
        summary="Dry-run delivery policy over stored signals",
        dependencies=[Depends(require_admin)],
    )
    def admin_delivery_simulation(
        request: Request,
        body: DeliverySimulationIn,
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        sim_settings = _simulation_settings(settings, body)
        items, total = fastapi_app.state.signal_store.fetch_admin_signals_page(
            limit=body.limit,
            offset=0,
            minutes=body.minutes,
        )
        policy = DeliveryPolicy(sim_settings)
        rows = sorted(items, key=lambda row: str(row.get("detected_at") or ""))
        simulated: list[dict[str, Any]] = []
        for row in rows:
            signal = _row_to_signal(row)
            out = policy.apply(signal)
            p = out.payload or {}
            simulated.append(
                {
                    "signal_id": out.signal_id,
                    "detected_at": out.detected_at.isoformat(),
                    "ticker": out.ticker,
                    "instrument_id": out.instrument_id,
                    "signal_type": out.signal_type,
                    "quality_score": p.get("quality_score"),
                    "current_delivery_status": row.get("delivery_status"),
                    "current_delivery_reason": row.get("delivery_reason"),
                    "simulated_delivery_status": p.get("delivery_status"),
                    "simulated_delivery_reason": p.get("delivery_reason"),
                    "simulated_delivery_rule": p.get("delivery_rule"),
                    "simulated_delivery_channel": p.get("delivery_channel"),
                    "simulated_delivery_priority": p.get("delivery_priority"),
                }
            )
        return _delivery_simulation_summary(
            simulated,
            total=total,
            sampled=len(items),
            preset=body.preset,
            minutes=body.minutes,
        )

    @fastapi_app.get(
        "/admin/api/calibration",
        tags=["admin"],
        summary="Матрица калибровки signal_type × quality × delivery × feedback",
        dependencies=[Depends(require_admin)],
    )
    def admin_calibration(
        minutes: Annotated[int, Query(ge=0, le=10_080)] = 0,
    ) -> dict[str, Any]:
        return fastapi_app.state.signal_store.fetch_admin_calibration(
            minutes=minutes
        )

    @fastapi_app.get(
        "/admin/api/feedback/overview",
        tags=["admin"],
        summary="Feedback quality by type/ticker/delivery",
        dependencies=[Depends(require_admin)],
    )
    def admin_feedback_overview(
        minutes: Annotated[int, Query(ge=0, le=10_080)] = 0,
    ) -> dict[str, Any]:
        return fastapi_app.state.signal_store.fetch_admin_feedback_overview(
            minutes=minutes
        )

    @fastapi_app.get(
        "/admin/api/settings",
        tags=["admin"],
        summary="Read-only runtime settings for the cockpit",
        dependencies=[Depends(require_admin)],
    )
    def admin_settings(request: Request) -> dict[str, Any]:
        s = request.app.state.settings
        return {
            "runtime": request.app.state.runtime,
            "delivery": {
                "enabled": s.signal_delivery_enabled,
                "min_quality": s.signal_delivery_min_quality,
                "max_per_hour": s.signal_delivery_max_per_hour,
                "instrument_cooldown_seconds": (
                    s.signal_delivery_instrument_cooldown_seconds
                ),
                "type_rules_json_configured": bool(
                    s.signal_delivery_type_rules_json
                ),
                "legacy_signal_min_quality_score": s.signal_min_quality_score,
            },
            "paths": {
                "detectors_config": str(s.detector_path),
                "detectors_overrides_config": str(s.detector_overrides_path),
                "instruments_config": str(s.instruments_path),
                "signal_accuracy_json_path": str(s.signal_accuracy_json_path),
            },
            "kafka": {
                "raw_topic": s.kafka_raw_topic,
                "signal_topic": s.kafka_signal_topic,
                "signal_value_format": s.kafka_signal_value_format,
            },
            "signals": _configured_signal_catalog(s),
        }

    @fastapi_app.get(
        "/admin/api/accuracy",
        tags=["admin"],
        summary="JSON офлайн-оценки (duckdb_label_signals)",
        dependencies=[Depends(require_admin)],
    )
    def admin_accuracy(request: Request) -> dict[str, Any]:
        path = request.app.state.settings.signal_accuracy_json_path
        return _accuracy_response_from_path(path)

    @fastapi_app.get(
        "/admin/api/source-health",
        tags=["admin"],
        summary="Raw source freshness by instrument",
        dependencies=[Depends(require_admin)],
    )
    def admin_source_health(
        request: Request,
        minutes: Annotated[int, Query(ge=1, le=10_080)] = 1440,
        stale_after_minutes: Annotated[int, Query(ge=1, le=1440)] = 15,
    ) -> dict[str, Any]:
        return _source_health_response(
            request.app.state.settings,
            minutes=minutes,
            stale_after_minutes=stale_after_minutes,
        )

    @fastapi_app.get(
        "/admin/api/instrument-insights/{instrument_id}",
        tags=["admin"],
        summary="Instrument market statistics and microstructure card",
        dependencies=[Depends(require_admin)],
    )
    def admin_instrument_insights(
        request: Request,
        instrument_id: str,
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        catalog = _configured_instrument_catalog(settings)
        by_id = {
            str(item.get("instrument_id")): dict(item)
            for item in catalog.get("items", [])
        }
        instrument = by_id.get(instrument_id)
        if instrument is None:
            raise HTTPException(status_code=404, detail="Инструмент не найден в конфиге")
        if not settings.clickhouse_http_url:
            return {
                "status": "unavailable",
                "instrument": instrument,
                "reason_code": "clickhouse_not_configured",
                "message_ru": "CLICKHOUSE_HTTP_URL не задан — рыночная статистика инструмента недоступна.",
            }
        try:
            insights = fetch_instrument_insights(
                settings.clickhouse_http_url,
                instrument_id=instrument_id,
                username=settings.clickhouse_http_username,
                password=settings.clickhouse_http_password,
            )
        except httpx.HTTPStatusError as exc:
            return {
                "status": "unavailable",
                "instrument": instrument,
                "reason_code": "clickhouse_http_error",
                "message_ru": f"ClickHouse HTTP {exc.response.status_code}: статистика инструмента недоступна.",
            }
        except Exception as exc:
            return {
                "status": "unavailable",
                "instrument": instrument,
                "reason_code": "clickhouse_query_failed",
                "message_ru": f"{type(exc).__name__}: статистика инструмента недоступна.",
            }
        return {
            **insights,
            "instrument": instrument,
        }

    @fastapi_app.get(
        "/admin/api/signal/{signal_id}",
        tags=["admin"],
        summary="Один сигнал по UUID",
        dependencies=[Depends(require_admin)],
    )
    def admin_signal_one(signal_id: str) -> dict[str, Any]:
        try:
            uuid.UUID(signal_id.strip())
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="signal_id должен быть UUID"
            ) from exc
        row = fastapi_app.state.signal_store.fetch_admin_signal_by_id(signal_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Сигнал не найден")
        return row

    @fastapi_app.get(
        "/admin/api/instruments",
        tags=["admin"],
        summary="Configured instrument universe with activity stats",
        dependencies=[Depends(require_admin)],
    )
    def admin_instruments_list(
        request: Request,
        minutes: Annotated[int, Query(ge=0, le=10_080)] = 0,
        resolve: Annotated[
            bool,
            Query(description="Resolve FIGI/UID through T-Invest; slower and needs token."),
        ] = False,
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        catalog = _configured_instrument_catalog(settings)
        activity = fastapi_app.state.signal_store.fetch_admin_instrument_activity(
            minutes=minutes
        )

        metadata_by_id: dict[str, Any] = {}
        if resolve:
            if not (settings.tinvest_token or "").strip():
                raise HTTPException(
                    status_code=503,
                    detail="TINVEST_TOKEN не задан — resolve через T-Invest недоступен.",
                )
            reg, new_cache = resolve_instrument_registry(
                settings,
                getattr(request.app.state, "instrument_registry_cache", None),
            )
            request.app.state.instrument_registry_cache = new_cache
            metadata_by_id = {m.instrument_id: m for m in reg}

        items: list[dict[str, Any]] = []
        for raw in catalog.get("items", []):
            item = dict(raw)
            stat = dict(activity.get(str(item.get("instrument_id")), {}))
            total = int(stat.get("total") or 0)
            delivered = int(stat.get("delivered") or 0)
            item.update(
                {
                    "total": total,
                    "delivered": delivered,
                    "suppressed": int(stat.get("suppressed") or 0),
                    "unknown": int(stat.get("unknown") or 0),
                    "delivery_rate": (delivered / total) if total else 0.0,
                    "avg_quality": stat.get("avg_quality"),
                    "last_detected_at": stat.get("last_detected_at"),
                    "has_activity": total > 0,
                }
            )
            meta = metadata_by_id.get(str(item.get("instrument_id")))
            if meta is not None:
                item.update({"figi": meta.figi, "uid": meta.uid, "resolved": True})
            else:
                item.update({"figi": None, "uid": None, "resolved": False})
            items.append(item)

        return {
            "items": items,
            "count": len(items),
            "active_count": sum(1 for item in items if item["has_activity"]),
            "minutes": minutes,
            "source_coverage": catalog.get("source_coverage", {}),
            "config_error": catalog.get("config_error"),
        }

    @fastapi_app.get(
        "/admin/api/instruments/resolve",
        tags=["admin"],
        summary="Resolved instrument metadata from T-Invest",
        dependencies=[Depends(require_admin)],
    )
    def admin_instruments_resolve(request: Request) -> dict[str, Any]:
        settings = request.app.state.settings
        if not (settings.tinvest_token or "").strip():
            raise HTTPException(
                status_code=503,
                detail="TINVEST_TOKEN не задан — список инструментов недоступен.",
            )
        reg, new_cache = resolve_instrument_registry(
            settings,
            getattr(request.app.state, "instrument_registry_cache", None),
        )
        request.app.state.instrument_registry_cache = new_cache
        items = [
            {
                "instrument_id": m.instrument_id,
                "ticker": m.ticker,
                "class_code": m.class_code,
                "alias": m.alias,
                "figi": m.figi,
                "uid": m.uid,
            }
            for m in reg
        ]
        return {"items": items, "count": len(items)}

    @fastapi_app.get(
        "/admin/api/instruments/{instrument_id}/market-values",
        tags=["admin"],
        summary="Unary GetMarketValues по instrument_id из конфига",
        dependencies=[Depends(require_admin)],
    )
    def admin_instrument_market_values(
        request: Request,
        instrument_id: str,
        value_types: Annotated[
            str | None,
            Query(
                description=(
                    "Список через запятую: last_price, open_interest, close_price, "
                    "evening, theor, dealer. Пусто — last_price,open_interest,close_price."
                ),
            ),
        ] = None,
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        if not (settings.tinvest_token or "").strip():
            raise HTTPException(
                status_code=503,
                detail="TINVEST_TOKEN не задан — unary-запросы к T-Invest недоступны.",
            )
        try:
            parsed_types = parse_market_value_types_csv(value_types or "")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        reg, new_cache = resolve_instrument_registry(
            settings,
            getattr(request.app.state, "instrument_registry_cache", None),
        )
        request.app.state.instrument_registry_cache = new_cache
        meta = reg.resolve(instrument_id=instrument_id.strip())
        if meta is None:
            raise HTTPException(
                status_code=404,
                detail=f"instrument_id не найден в {settings.instruments_path}: {instrument_id}",
            )
        try:
            resp = fetch_market_values(
                settings,
                instrument_uid=meta.uid,
                value_types=parsed_types,
            )
        except TinvestRequestError as exc:
            raise HTTPException(
                status_code=502, detail=f"T-Invest GetMarketValues: {exc}"
            ) from exc
        return {
            "instrument_id": meta.instrument_id,
            "instrument_uid": meta.uid,
            "ticker": meta.ticker,
            "class_code": meta.class_code,
            "requested_value_types": [vt.name for vt in parsed_types],
            "response": json_friendly(resp),
        }

    @fastapi_app.get(
        "/admin/api/instruments/{instrument_id}/tech-analysis",
        tags=["admin"],
        summary="Unary GetTechAnalysis по instrument_id из конфига",
        dependencies=[Depends(require_admin)],
    )
    def admin_instrument_tech_analysis(
        request: Request,
        instrument_id: str,
        indicator: Annotated[
            str,
            Query(description="rsi | ema | sma | bb | macd"),
        ] = "rsi",
        interval: Annotated[
            str,
            Query(
                description=(
                    "1m, 5m, 15m, 1h, 1d, 2m, 3m, 10m, 30m, 2h, 4h, week, month"
                ),
            ),
        ] = "1h",
        type_of_price: Annotated[str, Query(description="close | open | high | low | avg")] = "close",
        length: Annotated[int, Query(ge=1, le=500)] = 14,
        window_minutes: Annotated[
            int,
            Query(
                ge=5,
                le=60 * 24 * 365,
                description="Окно [to − window, to], to = сейчас UTC.",
            ),
        ] = 1440,
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        if not (settings.tinvest_token or "").strip():
            raise HTTPException(
                status_code=503,
                detail="TINVEST_TOKEN не задан — unary-запросы к T-Invest недоступны.",
            )
        try:
            ind = parse_indicator_type(indicator)
            ival = parse_indicator_interval(interval)
            top = parse_type_of_price(type_of_price)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        reg, new_cache = resolve_instrument_registry(
            settings,
            getattr(request.app.state, "instrument_registry_cache", None),
        )
        request.app.state.instrument_registry_cache = new_cache
        meta = reg.resolve(instrument_id=instrument_id.strip())
        if meta is None:
            raise HTTPException(
                status_code=404,
                detail=f"instrument_id не найден в {settings.instruments_path}: {instrument_id}",
            )
        now = datetime.now(timezone.utc)
        from_ts = now - timedelta(minutes=window_minutes)
        try:
            resp = fetch_tech_analysis(
                settings,
                instrument_uid=meta.uid,
                indicator_type=ind,
                interval=ival,
                type_of_price=top,
                length=length,
                from_=from_ts,
                to=now,
            )
        except TinvestRequestError as exc:
            raise HTTPException(
                status_code=502, detail=f"T-Invest GetTechAnalysis: {exc}"
            ) from exc
        return {
            "instrument_id": meta.instrument_id,
            "instrument_uid": meta.uid,
            "ticker": meta.ticker,
            "class_code": meta.class_code,
            "indicator": ind.name,
            "interval": ival.name,
            "type_of_price": top.name,
            "length": length,
            "from": from_ts.isoformat(),
            "to": now.isoformat(),
            "response": json_friendly(resp),
        }

    @fastapi_app.get(
        "/admin/api/signal/{signal_id}/context",
        tags=["admin"],
        summary="Сырые события ClickHouse вокруг времени сигнала",
        dependencies=[Depends(require_admin)],
    )
    def admin_signal_context(
        request: Request,
        signal_id: str,
        seconds_before: Annotated[int, Query(ge=10, le=3600)] = 120,
        seconds_after: Annotated[int, Query(ge=10, le=3600)] = 120,
    ) -> dict[str, Any]:
        settings = request.app.state.settings
        if not settings.clickhouse_http_url:
            raise HTTPException(
                status_code=503,
                detail="CLICKHOUSE_HTTP_URL не задан — контекст недоступен.",
            )
        try:
            uuid.UUID(signal_id.strip())
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="signal_id должен быть UUID"
            ) from exc
        row = fastapi_app.state.signal_store.fetch_admin_signal_by_id(signal_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Сигнал не найден")
        center = isoparse(row["detected_at"])
        start = center - timedelta(seconds=seconds_before)
        end = center + timedelta(seconds=seconds_after)
        try:
            events = fetch_raw_events_window(
                settings.clickhouse_http_url,
                instrument_id=row["instrument_id"],
                start=start,
                end=end,
                username=settings.clickhouse_http_username,
                password=settings.clickhouse_http_password,
            )
        except httpx.HTTPStatusError as exc:
            body = (exc.response.text or "")[:800]
            raise HTTPException(
                status_code=502,
                detail=(
                    f"ClickHouse HTTP {exc.response.status_code}: {body}. "
                    "Проверьте, что выполнен clickhouse-init и есть БД signal_engine."
                ),
            ) from exc
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"ClickHouse недоступен: {exc}",
            ) from exc
        return {
            "signal_id": signal_id,
            "instrument_id": row["instrument_id"],
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "events": events,
            "event_count": len(events),
        }

    @fastapi_app.post(
        "/admin/api/feedback",
        tags=["admin"],
        summary="Сохранить разметку полезно/шум",
        dependencies=[Depends(require_admin)],
    )
    def admin_feedback_save(body: AdminFeedbackIn) -> dict[str, str]:
        try:
            uuid.UUID(body.signal_id.strip())
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="signal_id должен быть UUID"
            ) from exc
        fastapi_app.state.signal_store.upsert_admin_feedback(
            signal_id=body.signal_id.strip(),
            label=body.label,
            note=body.note,
        )
        return {"status": "ok"}

    @fastapi_app.get(
        "/admin/api/signals/export.csv",
        tags=["admin"],
        summary="Экспорт отфильтрованных сигналов в CSV (до 20k строк)",
        dependencies=[Depends(require_admin)],
    )
    def admin_signals_export_csv(
        minutes: Annotated[int, Query(ge=0, le=10_080)] = 0,
        instrument_id: Annotated[str | None, Query()] = None,
        signal_type: Annotated[str | None, Query()] = None,
        min_quality: Annotated[float | None, Query()] = None,
        quality_min: Annotated[float | None, Query()] = None,
        quality_max: Annotated[float | None, Query()] = None,
        delivery_status: Annotated[str | None, Query()] = None,
        feedback: Annotated[str | None, Query()] = None,
        severity: Annotated[int | None, Query(ge=1, le=3)] = None,
    ) -> StreamingResponse:
        store = fastapi_app.state.signal_store

        def rows_iter() -> Iterator[list[str]]:
            offset = 0
            page = 500
            max_rows = 20_000
            while offset < max_rows:
                items, total = store.fetch_admin_signals_page(
                    limit=page,
                    offset=offset,
                    minutes=minutes,
                    instrument_id=instrument_id,
                    signal_type=signal_type,
                    min_quality=min_quality,
                    quality_min=quality_min,
                    quality_max=quality_max,
                    delivery_status=delivery_status,
                    feedback=feedback,
                    severity=severity,
                )
                if not items:
                    break
                for r in items:
                    p = r.get("payload")
                    if not isinstance(p, dict):
                        p = {}
                    yield [
                        r.get("signal_id", ""),
                        r.get("detected_at", ""),
                        r.get("instrument_id", ""),
                        r.get("ticker", ""),
                        r.get("signal_type", ""),
                        str(r.get("severity", "")),
                        str(r.get("z_score", "")),
                        str(r.get("quality_score") or p.get("quality_score") or ""),
                        r.get("delivery_status") or "",
                        r.get("delivery_reason") or "",
                        (r.get("summary") or "").replace("\r", " ").replace("\n", " "),
                        r.get("admin_feedback_label") or "",
                    ]
                offset += len(items)
                if offset >= total:
                    break

        def gen() -> Iterator[bytes]:
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(
                [
                    "signal_id",
                    "detected_at",
                    "instrument_id",
                    "ticker",
                    "signal_type",
                    "severity",
                    "z_score",
                    "quality_score",
                    "delivery_status",
                    "delivery_reason",
                    "summary",
                    "admin_feedback",
                ],
            )
            yield ("\ufeff" + buf.getvalue()).encode("utf-8")
            buf.seek(0)
            buf.truncate(0)
            for row in rows_iter():
                writer.writerow(row)
                yield buf.getvalue().encode("utf-8")
                buf.seek(0)
                buf.truncate(0)

        return StreamingResponse(
            gen(),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="signals_export.csv"'
            },
        )

    return fastapi_app


app = create_app()


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="api")
    host, port = settings.api_host, settings.api_port
    pkg_root = str(Path(__file__).resolve().parent.parent)
    if settings.api_reload:
        uvicorn.run(
            "tinvest_signal_engine.services.api:app",
            host=host,
            port=port,
            reload=True,
            reload_dirs=[pkg_root],
        )
    else:
        uvicorn.run(app, host=host, port=port)

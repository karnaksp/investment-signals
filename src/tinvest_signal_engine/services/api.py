"""HTTP API поверх Postgres: последние сигналы и сводки по типам."""

from __future__ import annotations

import csv
import io
import json
import uuid
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
from ..clickhouse_context import fetch_raw_events_window
from ..config import RuntimeSettings, load_detector_config, load_instrument_configs
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


class HealthResponse(BaseModel):
    """Ответ проверки живости процесса (без запроса к Postgres)."""

    status: str = Field(
        description="Обычно `ok`, если процесс принимает HTTP.",
    )


class ReadyResponse(BaseModel):
    """Готовность к трафику: проверка соединения с Postgres."""

    status: str = Field(description="`ready`, если БД отвечает на ping.")


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
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    fastapi_app = FastAPI(
        title="T-Invest Signal API",
        version="0.1.0",
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
        return HealthResponse(status="ok")

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
        return ReadyResponse(status="ready")

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
        "/admin/api/settings",
        tags=["admin"],
        summary="Read-only runtime settings for the cockpit",
        dependencies=[Depends(require_admin)],
    )
    def admin_settings(request: Request) -> dict[str, Any]:
        s = request.app.state.settings
        return {
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
        if not path.is_file():
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Файл не найден: {path}. Задайте SIGNAL_ACCURACY_JSON_PATH "
                    "или смонтируйте каталог с JSON."
                ),
            )
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=500, detail=f"Невалидный JSON: {exc}"
            ) from exc

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
                    yield [
                        r.get("signal_id", ""),
                        r.get("detected_at", ""),
                        r.get("ticker", ""),
                        r.get("signal_type", ""),
                        str(r.get("severity", "")),
                        str(r.get("z_score", "")),
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
                    "ticker",
                    "signal_type",
                    "severity",
                    "z_score",
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
    settings = RuntimeSettings.from_env()
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

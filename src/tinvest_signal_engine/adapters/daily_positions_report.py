"""Render and durably enqueue the dated SHFE bulletin."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import html
import json
from uuid import NAMESPACE_URL, uuid5

from tinvest_signal_engine.adapters.postgres_reliability import PostgresReliableProcessingStore
from tinvest_signal_engine.application.reliable_processing import BrokerEvent
from tinvest_signal_engine.domain.daily_positions import (
    CATEGORIES, PRODUCTS, REPORT_SIGNAL_TYPE, REPORT_VERSION, DailyPositionComparison,
)
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, PreparedSignal, SignalRecord


def comparison_payload(comparison: DailyPositionComparison) -> dict:
    current, previous = comparison.current, comparison.previous
    old = {(x.product, x.category): x for x in previous.totals}
    products = []
    for product in PRODUCTS:
        groups = []
        for category in CATEGORIES:
            row = next(x for x in current.totals if (x.product, x.category) == (product, category))
            before = old[product, category]
            groups.append({
                "category": category, "longs": row.longs, "shorts": row.shorts,
                "previous_longs": before.longs, "previous_shorts": before.shorts,
                "long_change": row.long_change, "short_change": row.short_change,
            })
        products.append({"product": product, "groups": groups})
    return {
        "daily_report_version": REPORT_VERSION, "source_exchange": "SHFE",
        "trading_day": current.trading_day.isoformat(),
        "previous_day": previous.trading_day.isoformat(),
        "published_at": current.published_at.isoformat(),
        "source_url": current.source_url, "previous_source_url": previous.source_url,
        "source_sha256": current.source_sha256, "previous_source_sha256": previous.source_sha256,
        "scope": "all_maturities_complete_categories", "products": products,
    }


def render_daily_positions_html(payload: dict) -> str:
    names = {"au": "Золото · AU", "ag": "Серебро · AG"}
    categories = {"broker_clients": "Клиенты брокеров", "direct_members": "Неброкерские участники"}
    lines = ["<b>Металлы · дневные позиции SHFE</b>",
             f"{html.escape(payload['trading_day'])} к {html.escape(payload['previous_day'])}"]
    for product in payload["products"]:
        lines.extend(("", f"<b>{names[product['product']]}</b>"))
        for row in product["groups"]:
            net = row["longs"] - row["shorts"]
            delta = row["long_change"] - row["short_change"]
            lines.extend((
                f"<b>{categories[row['category']]}</b>",
                f"Лонги: {_number(row['longs'])} ({_change(row['long_change'], row['previous_longs'])})",
                f"Шорты: {_number(row['shorts'])} ({_change(row['short_change'], row['previous_shorts'])})",
                f"Лонг/шорт: {_ratio(row['longs'], row['shorts'])} "
                f"(было {_ratio(row['previous_longs'], row['previous_shorts'])})",
                f"Чистая позиция: {_number(net, signed=True)}; изменение {_number(delta, signed=True)}",
            ))
        oi = sum(row["longs"] for row in product["groups"])
        old_oi = sum(row["previous_longs"] for row in product["groups"])
        lines.append(f"Всего открытых контрактов: {_number(oi)} ({_change(oi - old_oi, old_oi)})")
    lines.extend(("", "Количество контрактов, все сроки вместе. Чистая позиция = лонги − шорты.",
                  "Это позиции на китайской бирже. Клиенты брокеров — не собственные ставки брокеров.",
                  f'<a href="{html.escape(payload["source_url"], quote=True)}">SHFE: исходный отчёт</a>'))
    return "\n".join(lines)


def _number(value: int, *, signed: bool = False) -> str:
    return (f"{value:+,}" if signed else f"{value:,}").replace(",", " ").replace("-", "−")


def _change(delta: int, previous: int) -> str:
    percent = f"{delta / previous * 100:+.2f}%" if previous else "база 0"
    return f"{_number(delta, signed=True)}; {percent}".replace("-", "−")


def _ratio(longs: int, shorts: int) -> str:
    return f"{longs / shorts:.2f}" if shorts else "нет шортов"


class PostgresDailyPositionPublisher:
    def __init__(self, *, store: PostgresReliableProcessingStore, target: DeliveryTarget) -> None:
        self._store = store
        self._target = target

    def publish(self, comparison: DailyPositionComparison, *, generated_at: datetime) -> bool:
        payload = comparison_payload(comparison)
        event_id = str(uuid5(NAMESPACE_URL, f"{REPORT_VERSION}:{comparison.current.trading_day}"))
        instrument = "SHFE_DAILY_METALS"
        event_body = {
            "event_id": event_id, "event_type": "daily_position_report", "instrument_id": instrument,
            "source_time": comparison.current.published_at.isoformat(), "report": payload,
        }
        # Generation time is not source identity: retrying tomorrow must not
        # create a new report or conflict merely because it is fetched again.
        canonical = json.dumps(event_body, sort_keys=True, separators=(",", ":")).encode()
        event = BrokerEvent(
            event_id, "daily_position_report", "shfe.daily-positions", 0,
            comparison.current.trading_day.toordinal(), sha256(canonical).digest(), event_body,
        )
        signal = SignalRecord(
            signal_id=str(uuid5(NAMESPACE_URL, "signal:" + event_id)), detected_at=generated_at,
            instrument_id=instrument, ticker="SHFE", class_code="REPORT", alias="shfe_daily_metals",
            source_event_type=event.event_type, signal_type=REPORT_SIGNAL_TYPE, severity=1,
            metric_value=0, baseline_value=0, z_score=0, window_seconds=86400,
            summary="Дневные позиции в золоте и серебре SHFE",
            payload={**payload, "report_generated_at": generated_at.isoformat(),
                     "delivery_status": "delivered", "delivery_channel": "daily",
                     "delivery_reason": "daily_report_subscription"},
            source_event_id=event_id, source_event_at=comparison.current.published_at,
            signal_schema_version="1.0.0", expectation_catalog_version=REPORT_VERSION,
            detector_config_version=REPORT_VERSION, delivery_config_version=REPORT_VERSION,
            cost_model_version="not_applicable", provenance_status="complete",
        )
        stored = self._store.persist_report_once(event, PreparedSignal(signal, (self._target,)))
        return not stored.replayed

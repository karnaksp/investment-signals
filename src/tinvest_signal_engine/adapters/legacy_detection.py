"""Adapter from the legacy detector model to reliable-processing records."""

from __future__ import annotations

import logging
import time
from typing import Callable

from tinvest_signal_engine.config import RuntimeSettings, load_detector_config
from tinvest_signal_engine.delivery_policy import DELIVERY_DELIVERED, DeliveryPolicy
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.domain.configuration import content_version
from tinvest_signal_engine.domain.reliable_processing import (
    DeliveryTarget,
    PreparedSignal,
    SignalRecord,
)
from tinvest_signal_engine.models import NormalizedEvent, TriggerSignal
from tinvest_signal_engine.redis_detector_state import (
    flush_detector_to_redis,
    hydrate_detector_from_redis,
)
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery


logger = logging.getLogger(__name__)


class LegacyDetectionAdapter:
    def __init__(
        self,
        settings: RuntimeSettings,
        *,
        delivered_count_since: Callable[..., int],
    ) -> None:
        self._settings = settings
        self._detector = self._build_detector()
        hydrate_detector_from_redis(self._detector, settings.redis_url)
        self._detector_mtime = settings.detector_path.stat().st_mtime
        self._overrides_mtime = self._current_overrides_mtime()
        self._last_config_poll = time.monotonic()
        self._policy = DeliveryPolicy(
            settings,
            delivered_count_since=(
                lambda since, instrument_id, signal_type: delivered_count_since(
                    since=since,
                    instrument_id=instrument_id,
                    signal_type=signal_type,
                )
            ),
        )

    def detect(self, payload: dict[str, object]) -> tuple[PreparedSignal, ...]:
        self._maybe_reload()
        event = NormalizedEvent.from_dict(payload)
        signals = self._detector.process(event)
        signals = self._detector.enrich_signals_with_unary(signals)
        prepared: list[PreparedSignal] = []
        for signal in signals:
            enriched = enrich_signal_for_delivery(signal)
            governed = self._policy.apply(enriched)
            targets = (
                self._delivery_targets()
                if governed.payload.get("delivery_status") == DELIVERY_DELIVERED
                else ()
            )
            prepared.append(
                PreparedSignal(
                    signal=_signal_record(governed),
                    delivery_targets=targets,
                )
            )
        return tuple(prepared)

    def checkpoint(self) -> None:
        flush_detector_to_redis(self._detector, self._settings.redis_url)

    def _build_detector(self) -> SignalDetector:
        loaded = load_detector_config(
            self._settings.detector_path,
            self._settings.detector_overrides_path,
        )
        return SignalDetector(
            loaded.default,
            loaded.per_instrument,
            lead_lag_pairs=loaded.lead_lag_pairs,
            expectation_catalog_version=(
                self._settings.expectation_catalog_version
            ),
            detector_config_version=self._detector_config_version(),
            delivery_config_version=self._settings.delivery_config_version,
            cost_model_version=self._settings.cost_model_version,
        )

    def _detector_config_version(self) -> str:
        if self._settings.detector_config_version:
            return self._settings.detector_config_version
        paths = (
            self._settings.detector_path,
            self._settings.detector_overrides_path,
        )
        return content_version(
            path.read_bytes() for path in paths if path.exists()
        )

    def _current_overrides_mtime(self) -> float | None:
        path = self._settings.detector_overrides_path
        return path.stat().st_mtime if path.exists() else None

    def _maybe_reload(self) -> None:
        interval = self._settings.config_reload_interval_seconds
        now = time.monotonic()
        if interval <= 0 or now - self._last_config_poll < interval:
            return
        self._last_config_poll = now
        detector_mtime = self._settings.detector_path.stat().st_mtime
        overrides_mtime = self._current_overrides_mtime()
        if (
            detector_mtime == self._detector_mtime
            and overrides_mtime == self._overrides_mtime
        ):
            return
        self.checkpoint()
        replacement = self._build_detector()
        hydrate_detector_from_redis(replacement, self._settings.redis_url)
        self._detector = replacement
        self._detector_mtime = detector_mtime
        self._overrides_mtime = overrides_mtime
        logger.info(
            "Reloaded detector config from %s (+ %s)",
            self._settings.detector_path,
            self._settings.detector_overrides_path,
        )

    def _delivery_targets(self) -> tuple[DeliveryTarget, ...]:
        targets: list[DeliveryTarget] = []
        if self._settings.alert_webhook_url:
            targets.append(
                DeliveryTarget("webhook", self._settings.alert_webhook_url)
            )
        if self._settings.telegram_bot_token and self._settings.telegram_chat_id:
            thread = self._settings.telegram_message_thread_id
            targets.append(
                DeliveryTarget(
                    "telegram",
                    f"{self._settings.telegram_chat_id}:{thread or ''}",
                )
            )
        return tuple(targets)


def _signal_record(signal: TriggerSignal) -> SignalRecord:
    return SignalRecord(
        signal_id=signal.signal_id,
        detected_at=signal.detected_at,
        instrument_id=signal.instrument_id,
        ticker=signal.ticker,
        class_code=signal.class_code,
        alias=signal.alias,
        source_event_type=signal.source_event_type,
        signal_type=signal.signal_type,
        severity=signal.severity,
        metric_value=signal.metric_value,
        baseline_value=signal.baseline_value,
        z_score=signal.z_score,
        window_seconds=signal.window_seconds,
        summary=signal.summary,
        payload=dict(signal.payload),
        source_event_id=signal.source_event_id,
        source_event_at=signal.source_event_at,
        signal_schema_version=signal.signal_schema_version,
        expectation_catalog_version=signal.expectation_catalog_version,
        detector_config_version=signal.detector_config_version,
        delivery_config_version=signal.delivery_config_version,
        cost_model_version=signal.cost_model_version,
        provenance_status=signal.provenance_status,
    )

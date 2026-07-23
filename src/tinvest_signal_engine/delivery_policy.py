"""Notification delivery policy for enriched detector signals."""

from __future__ import annotations

import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, Callable

from .config import RuntimeSettings
from .models import TriggerSignal
from .serialization import utc_now

POLICY_VERSION = "delivery_v3"
DELIVERY_DELIVERED = "delivered"
DELIVERY_SUPPRESSED = "suppressed"
logger = logging.getLogger(__name__)

_ALWAYS_TYPES = {"trading_status_changed", "market_access_changed"}
_COMBO_TYPES = {"microstructure_combo_long", "microstructure_combo_short"}
_LARGE_TRADE_TYPES = {"large_trade_print"}
_VALIDATED_LONG_HORIZON_TYPES = {"bond_maturity_convergence"}
_ACTIVITY_CONTEXT_TYPES = {
    "volume_spike",
    "trade_rate_spike",
    "microstructure_combo_long",
    "microstructure_combo_short",
}
_MOMENTUM_TYPES = {"volume_spike", "trade_rate_spike", "price_jump"}
_LIQUIDITY_TYPES = {"spread_widening", "orderbook_imbalance"}
_EXPERIMENTAL_ADMIN_ONLY_TYPES = {
    "candle_range_spike",
    "obi_dynamics",
    "open_interest_spike",
    "aggressive_trade_burst",
    "lead_lag_divergence",
}


@dataclass(frozen=True)
class DeliveryDecision:
    status: str
    reason: str
    rule: str
    delivered_at: datetime | None = None
    channel: str | None = None

    @property
    def should_send(self) -> bool:
        return self.status == DELIVERY_DELIVERED


class DeliveryPolicy:
    """Stateful delivery gate for Telegram/webhook notifications.

    The detector still stores every enriched signal. This policy only decides
    whether a saved signal should also be sent to external notification sinks.
    """

    def __init__(
        self,
        settings: RuntimeSettings,
        *,
        delivered_count_since: Callable[
            [datetime, str | None, str | None], int
        ]
        | None = None,
    ):
        self.enabled = bool(settings.signal_delivery_enabled)
        legacy_floor = settings.signal_min_quality_score
        configured_floor = settings.signal_delivery_min_quality
        if legacy_floor is not None and settings.signal_delivery_min_quality_raw is None:
            configured_floor = legacy_floor
        self.min_quality = max(0, int(configured_floor))
        self.max_per_hour = max(0, int(settings.signal_delivery_max_per_hour))
        self.instrument_cooldown = max(
            0, int(settings.signal_delivery_instrument_cooldown_seconds)
        )
        self._delivered_count_since = delivered_count_since
        self._type_rules = _parse_type_rules(settings.signal_delivery_type_rules_json)
        self._recent_activity: dict[str, deque[tuple[datetime, str]]] = defaultdict(
            deque
        )
        self._recent_deliveries: deque[datetime] = deque()
        self._last_instrument_delivery: dict[str, datetime] = {}
        self._last_status_delivery: dict[tuple[str, str], datetime] = {}

    def apply(self, signal: TriggerSignal) -> TriggerSignal:
        now = signal.detected_at or utc_now()
        self._record_activity_context(signal, now)
        decision = self.decide(signal, now=now)
        return self._annotate(signal, decision)

    def suppress(
        self,
        signal: TriggerSignal,
        *,
        reason_code: str,
        rule: str,
        metadata: dict[str, object] | None = None,
    ) -> TriggerSignal:
        """Suppress without mutating cooldown, rate-limit, or context state."""

        decision = DeliveryDecision(
            status=DELIVERY_SUPPRESSED,
            reason=reason_code,
            rule=rule,
            channel="admin_only",
        )
        return self._annotate(signal, decision, metadata=metadata)

    def _annotate(
        self,
        signal: TriggerSignal,
        decision: DeliveryDecision,
        *,
        metadata: dict[str, object] | None = None,
    ) -> TriggerSignal:
        payload = {
            **signal.payload,
            "delivery_status": decision.status,
            "delivery_reason": decision.reason,
            "delivery_reason_code": decision.reason,
            "delivery_rule": decision.rule,
            "delivery_policy_version": POLICY_VERSION,
            "delivery_priority": self._priority(signal, decision),
            "delivery_channel": self._channel(decision),
            "delivery_explanation_ru": self._explanation_ru(decision),
            "delivered_at": (
                decision.delivered_at.isoformat()
                if decision.delivered_at is not None
                else None
            ),
            **(metadata or {}),
        }
        return replace(signal, payload=payload)

    def decide(
        self, signal: TriggerSignal, *, now: datetime | None = None
    ) -> DeliveryDecision:
        ts = now or signal.detected_at or utc_now()
        if not self.enabled:
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="delivery_disabled",
                rule="global_disabled",
            )

        candidate = self._candidate_decision(signal, ts)
        if not candidate.should_send:
            return candidate

        cooled = self._instrument_cooldown_decision(signal, ts)
        if cooled is not None:
            return cooled

        limited = self._rate_limit_decision(ts)
        if limited is not None:
            return limited

        self._recent_deliveries.append(ts)
        if signal.signal_type not in _ALWAYS_TYPES:
            self._last_instrument_delivery[signal.instrument_id] = ts
        if signal.signal_type in _ALWAYS_TYPES:
            self._last_status_delivery[
                (signal.instrument_id, signal.signal_type)
            ] = ts
        return candidate

    def _candidate_decision(
        self, signal: TriggerSignal, now: datetime
    ) -> DeliveryDecision:
        custom = self._custom_decision(signal, now)
        if custom is not None:
            return custom

        quality = _quality(signal)
        abs_z = abs(float(signal.z_score))
        st = signal.signal_type

        if st in _ALWAYS_TYPES:
            key = (signal.instrument_id, st)
            last = self._last_status_delivery.get(key)
            if last is not None and now - last < timedelta(hours=1):
                return DeliveryDecision(
                    status=DELIVERY_SUPPRESSED,
                    reason="status_cooldown",
                    rule="status_access_1h_cooldown",
                )
            return DeliveryDecision(
                status=DELIVERY_DELIVERED,
                reason="status_or_access_change",
                rule="status_access_always",
                delivered_at=now,
            )

        if st in _VALIDATED_LONG_HORIZON_TYPES:
            success_rate = _payload_number(signal, "historical_success_rate")
            lower_bound = _payload_number(
                signal, "historical_wilson_lower_bound"
            )
            sample_size = _payload_number(
                signal, "historical_eligible_observations"
            )
            if success_rate >= 0.90 and lower_bound >= 0.85 and sample_size >= 100:
                return DeliveryDecision(
                    status=DELIVERY_DELIVERED,
                    reason="validated_bond_convergence",
                    rule="bond_convergence_historical_gate_v1",
                    delivered_at=now,
                )
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="bond_convergence_evidence_below_gate",
                rule="bond_convergence_historical_gate_v1",
            )

        if st in _COMBO_TYPES:
            score = _payload_number(signal, "score")
            if score >= 6:
                return DeliveryDecision(
                    status=DELIVERY_DELIVERED,
                    reason="combo_score_ge_6",
                    rule="combo_score",
                    delivered_at=now,
                )
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="combo_score_below_6",
                rule="combo_score",
            )

        if st in _LARGE_TRADE_TYPES:
            if quality >= 90 or abs_z >= 10.0:
                return DeliveryDecision(
                    status=DELIVERY_DELIVERED,
                    reason="large_trade_high_quality_or_z",
                    rule="large_trade_quality_or_z",
                    delivered_at=now,
                )
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="large_trade_below_quality_and_z",
                rule="large_trade_quality_or_z",
            )

        if st in _MOMENTUM_TYPES:
            if st == "price_jump":
                if quality >= 90 and abs_z >= 8.0:
                    return DeliveryDecision(
                        status=DELIVERY_DELIVERED,
                        reason="price_extreme_quality_and_z",
                        rule="price_extreme_or_activity_confirmed",
                        delivered_at=now,
                    )
                if quality >= 75 and self._has_recent_activity(
                    signal.instrument_id, now
                ):
                    return DeliveryDecision(
                        status=DELIVERY_DELIVERED,
                        reason="price_near_activity",
                        rule="price_extreme_or_activity_confirmed",
                        delivered_at=now,
                    )
                return DeliveryDecision(
                    status=DELIVERY_SUPPRESSED,
                    reason="price_without_confirmation",
                    rule="price_extreme_or_activity_confirmed",
                )

            if quality >= self.min_quality and abs_z >= 6.0:
                return DeliveryDecision(
                    status=DELIVERY_DELIVERED,
                    reason="momentum_quality_and_z",
                    rule="momentum_quality_and_z",
                    delivered_at=now,
                )
            if abs_z >= 10.0:
                return DeliveryDecision(
                    status=DELIVERY_DELIVERED,
                    reason="momentum_extreme_z",
                    rule="momentum_quality_and_z",
                    delivered_at=now,
                )
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="momentum_below_quality_and_z",
                rule="momentum_quality_and_z",
            )

        if st in _LIQUIDITY_TYPES:
            if quality >= 60 and self._has_recent_activity(signal.instrument_id, now):
                return DeliveryDecision(
                    status=DELIVERY_DELIVERED,
                    reason="liquidity_near_activity",
                    rule="liquidity_activity_confirmed",
                    delivered_at=now,
                )
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="liquidity_without_context",
                rule="liquidity_activity_confirmed",
            )

        if st in _EXPERIMENTAL_ADMIN_ONLY_TYPES:
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="experimental_admin_only",
                rule="controlled_rollout_admin_only",
                channel="admin_only",
            )

        if quality >= max(90, self.min_quality):
            return DeliveryDecision(
                status=DELIVERY_DELIVERED,
                reason="quality_floor",
                rule="default_quality",
                delivered_at=now,
            )
        return DeliveryDecision(
            status=DELIVERY_SUPPRESSED,
            reason="quality_below_floor",
            rule="default_quality",
        )

    def _custom_decision(
        self, signal: TriggerSignal, now: datetime
    ) -> DeliveryDecision | None:
        rule = self._type_rules.get(signal.signal_type)
        if not isinstance(rule, dict):
            return None
        channel = _normalize_rule_channel(rule)
        if bool(rule.get("admin_only")) or channel == "admin_only":
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="type_rule_admin_only",
                rule="custom_type_rule",
                channel="admin_only",
            )
        matched_reason: str | None = None
        if bool(rule.get("always")):
            matched_reason = "type_rule_always"
        min_quality = rule.get("min_quality")
        min_abs_z = rule.get("min_abs_z")
        if matched_reason is None:
            if min_quality is not None and _quality(signal) >= float(min_quality):
                matched_reason = "type_rule_quality"
            elif min_abs_z is not None and abs(float(signal.z_score)) >= float(
                min_abs_z
            ):
                matched_reason = "type_rule_abs_z"
            elif channel == "digest" and min_quality is None and min_abs_z is None:
                matched_reason = "type_rule_digest"
        if matched_reason is None:
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="custom_type_rule_not_matched",
                rule="custom_type_rule",
            )
        if channel == "digest":
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="type_rule_digest",
                rule="custom_type_rule",
                channel="digest",
            )
        return DeliveryDecision(
            status=DELIVERY_DELIVERED,
            reason=matched_reason,
            rule="custom_type_rule",
            delivered_at=now,
            channel="realtime",
        )

    def _instrument_cooldown_decision(
        self, signal: TriggerSignal, now: datetime
    ) -> DeliveryDecision | None:
        if (
            self.instrument_cooldown <= 0
            or signal.signal_type in _ALWAYS_TYPES
            or signal.signal_type in _COMBO_TYPES
        ):
            return None
        since = now - timedelta(seconds=self.instrument_cooldown)
        last = self._last_instrument_delivery.get(signal.instrument_id)
        if last is not None and last >= since:
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="instrument_cooldown",
                rule="instrument_delivery_cooldown",
            )
        if self._persistent_delivered_count(
            since=since,
            instrument_id=signal.instrument_id,
            signal_type=None,
        ) > 0:
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="instrument_cooldown",
                rule="instrument_delivery_cooldown",
            )
        return None

    def _rate_limit_decision(self, now: datetime) -> DeliveryDecision | None:
        if self.max_per_hour <= 0:
            return None
        cutoff = now - timedelta(hours=1)
        while self._recent_deliveries and self._recent_deliveries[0] < cutoff:
            self._recent_deliveries.popleft()
        if len(self._recent_deliveries) >= self.max_per_hour:
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="rate_limit_per_hour",
                rule="global_max_per_hour",
            )
        if (
            self._persistent_delivered_count(
                since=cutoff,
                instrument_id=None,
                signal_type=None,
            )
            >= self.max_per_hour
        ):
            return DeliveryDecision(
                status=DELIVERY_SUPPRESSED,
                reason="rate_limit_per_hour",
                rule="global_max_per_hour",
            )
        return None

    def _persistent_delivered_count(
        self,
        *,
        since: datetime,
        instrument_id: str | None,
        signal_type: str | None,
    ) -> int:
        if self._delivered_count_since is None:
            return 0
        try:
            return int(
                self._delivered_count_since(since, instrument_id, signal_type)
            )
        except Exception:
            logger.exception("Failed to read persistent delivery history")
            return 0

    def _record_activity_context(self, signal: TriggerSignal, now: datetime) -> None:
        dq = self._recent_activity[signal.instrument_id]
        cutoff = now - timedelta(minutes=5)
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        if signal.signal_type in _ACTIVITY_CONTEXT_TYPES:
            dq.append((now, signal.signal_type))

    def _has_recent_activity(self, instrument_id: str, now: datetime) -> bool:
        dq = self._recent_activity.get(instrument_id)
        if not dq:
            return False
        cutoff = now - timedelta(minutes=5)
        return any(ts >= cutoff for ts, _ in dq)

    def _priority(self, signal: TriggerSignal, decision: DeliveryDecision) -> str:
        if decision.should_send:
            return "high"
        if decision.channel == "digest":
            return "medium"
        quality = _quality(signal)
        abs_z = abs(float(signal.z_score))
        if quality >= self.min_quality or abs_z >= 6.0 or int(signal.severity) >= 3:
            return "medium"
        return "low"

    @staticmethod
    def _channel(decision: DeliveryDecision) -> str:
        if decision.channel:
            return decision.channel
        return "realtime" if decision.should_send else "admin_only"

    @staticmethod
    def _explanation_ru(decision: DeliveryDecision) -> str:
        if decision.status == DELIVERY_DELIVERED:
            return _REASON_RU.get(
                decision.reason,
                "Сигнал прошёл delivery policy и отправлен в realtime-канал.",
            )
        return _REASON_RU.get(
            decision.reason,
            "Сигнал сохранён для анализа, но не отправлен в Telegram.",
        )


def _parse_type_rules(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_rule_channel(rule: dict[str, Any]) -> str | None:
    value = str(rule.get("channel") or "").strip().lower()
    if value in {"admin_only", "digest", "realtime"}:
        return value
    return None


def _quality(signal: TriggerSignal) -> float:
    value = signal.payload.get("quality_score")
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _payload_number(signal: TriggerSignal, key: str) -> float:
    value = signal.payload.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


_REASON_RU: dict[str, str] = {
    "validated_bond_convergence": "Историческая проверка сигнала схождения облигации к номиналу прошла заданный порог; уведомление отправлено сразу.",
    "bond_convergence_evidence_below_gate": "Сигнал схождения облигации сохранён, но историческая проверка не прошла заданный порог.",
    "combo_score_ge_6": "Комбо-сигнал набрал проходной score и отправлен в realtime.",
    "status_or_access_change": "Режим торгов или доступ к заявкам изменился; статусные события отправляются в realtime.",
    "price_extreme_quality_and_z": "Сильное движение цены прошло строгий порог качества и |z|.",
    "price_near_activity": "Движение цены подтверждено недавней активностью по тому же инструменту.",
    "momentum_quality_and_z": "Momentum-сигнал прошёл одновременно порог качества и |z|.",
    "momentum_extreme_z": "Momentum-сигнал прошёл как экстремальный |z|.",
    "large_trade_high_quality_or_z": "Крупный принт прошёл высокий quality или экстремальный |z|.",
    "liquidity_near_activity": "Liquidity-сигнал подтверждён недавней активностью.",
    "quality_floor": "Сигнал прошёл общий высокий порог качества.",
    "delivery_disabled": "Delivery выключен: сигнал сохранён, но внешний канал не используется.",
    "combo_score_below_6": "Комбо-сигнал сохранён, но score ниже проходного realtime-порога.",
    "large_trade_below_quality_and_z": "Крупный принт сохранён, но не прошёл realtime-порог качества или |z|.",
    "momentum_below_quality_and_z": "Momentum-сигнал сохранён, но не прошёл одновременно quality и |z|.",
    "price_without_confirmation": "Price jump сохранён, но для Telegram не хватило экстремальности или соседней активности.",
    "liquidity_without_context": "Liquidity-сигнал сохранён, но без соседнего volume/trade/combo не отправляется.",
    "quality_below_floor": "Сигнал сохранён, но quality ниже общего realtime-порога.",
    "experimental_admin_only": "Новый или экспериментальный тип сигнала сохранён для анализа и не отправляется в Telegram до явного promotion.",
    "type_rule_admin_only": "Custom type rule оставил этот тип в admin-only режиме.",
    "type_rule_digest": "Custom type rule пометил сигнал как digest-кандидат без realtime-отправки.",
    "rate_limit_per_hour": "Сигнал сохранён, но подавлен глобальным лимитом сообщений в час.",
    "instrument_cooldown": "Сигнал сохранён, но подавлен cooldown по инструменту.",
    "status_cooldown": "Статусный сигнал сохранён, но повтор подавлен часовым cooldown.",
    "custom_type_rule_not_matched": "Сигнал сохранён, но custom type rule не дал realtime-доставку.",
    "delivery_event_age_exceeded": "Событие обработано для локальной статистики, но слишком устарело для Telegram или webhook.",
    "delivery_event_crossed_session": "Событие относится к предыдущей торговой сессии и сохранено только для локальной статистики.",
    "delivery_event_time_unavailable": "Время исходного события неизвестно, поэтому внешнее уведомление безопасно подавлено.",
    "delivery_event_time_in_future": "Время исходного события расходится с часами сервиса; внешнее уведомление безопасно подавлено.",
}

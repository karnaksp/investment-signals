"""Composition root for selective morning-retracement recommendations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import time
from urllib.error import URLError
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    TransientClickHouseError,
)
from tinvest_signal_engine.adapters.kafka_reliability import KafkaSignalPublisher
from tinvest_signal_engine.adapters.morning_retracement_runtime import (
    ClickHouseMorningRetracementSource,
    ClickHouseMorningRetracementTrackingStore,
    load_morning_retracement_policy,
    load_morning_retracement_settings,
)
from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_reliable_processing_store,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
)
from tinvest_signal_engine.application.morning_retracement_signals import (
    GenerateMorningRetracementRecommendations,
)
from tinvest_signal_engine.application.morning_retracement_tracking import (
    ProcessMorningRetracementOutcomes,
    RecordMorningRetracementAssessments,
)
from tinvest_signal_engine.application.reliable_processing import (
    BrokerEvent,
    DetectionBatch,
    DetectorStateCheckpoint,
    ReliableEventProcessor,
)
from tinvest_signal_engine.config import (
    RuntimeSettings,
    load_instrument_configs,
    load_secret,
)
from tinvest_signal_engine.domain.morning_retracement_signal import (
    MorningRetracementRecommendation,
    MorningRetracementRuntimePolicy,
    MorningRetracementRuntimeSettings,
)
from tinvest_signal_engine.domain.reliable_processing import (
    DeliveryTarget,
    PreparedSignal,
    SignalRecord,
)
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)
MOSCOW = ZoneInfo("Europe/Moscow")
SIGNAL_TYPE = "morning_retracement_recommendation"
_SIGNAL_NAMESPACE = UUID("cf97dd90-90af-5a5f-a95a-5c20969b4da2")


class _PreparedRecommendation:
    def __init__(self, prepared: PreparedSignal) -> None:
        self._prepared = prepared

    def detect_batch(self, payload: dict[str, object]) -> DetectionBatch:
        del payload
        return DetectionBatch(signals=(self._prepared,))

    def replace_state(
        self,
        checkpoints: tuple[DetectorStateCheckpoint, ...],
    ) -> None:
        del checkpoints


def main() -> None:
    runtime = RuntimeSettings.from_env(service_name="detector")
    configure_logging(runtime.log_level)
    policy_path = Path(
        os.getenv(
            "MORNING_RETRACEMENT_POLICY_FILE",
            "/app/config/scientific_hypotheses/morning-retracement-runtime-v2.2.json",
        )
    )
    public_config = Path(
        os.getenv(
            "PUBLIC_CONFIG_FILE",
            "/etc/investment-signals-pro/product.yaml",
        )
    )
    policy = load_morning_retracement_policy(policy_path)
    clickhouse_password = load_secret("CLICKHOUSE_PASSWORD")
    if clickhouse_password is None:
        raise ValueError("CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required")
    source = ClickHouseMorningRetracementSource(
        base_url=_required("CLICKHOUSE_HTTP_URL"),
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=_required("CLICKHOUSE_USERNAME"),
        password=clickhouse_password,
        timeout_seconds=float(
            os.getenv("MORNING_RETRACEMENT_CLICKHOUSE_TIMEOUT_SECONDS") or "30"
        ),
    )
    tracking_store = ClickHouseMorningRetracementTrackingStore(
        base_url=_required("CLICKHOUSE_HTTP_URL"),
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=_required("CLICKHOUSE_USERNAME"),
        password=clickhouse_password,
        timeout_seconds=float(
            os.getenv("MORNING_RETRACEMENT_CLICKHOUSE_TIMEOUT_SECONDS") or "30"
        ),
    )
    instruments = tuple(load_instrument_configs(runtime.instruments_path))
    store = connect_reliable_processing_store(runtime)
    publisher = KafkaSignalPublisher(runtime)
    scorer = GenerateMorningRetracementRecommendations(policy)
    record_assessments = RecordMorningRetracementAssessments(tracking_store)
    process_outcomes = ProcessMorningRetracementOutcomes(
        store=tracking_store,
        policy=policy,
    )
    metrics = PrometheusReliabilityMetrics()
    poll_seconds = max(
        10.0,
        float(os.getenv("MORNING_RETRACEMENT_POLL_SECONDS") or "60"),
    )
    logger.info("Starting live morning-retracement recommendation worker")
    retry_delay = poll_seconds
    try:
        while True:
            cycle_started = time.monotonic()
            now = datetime.now(tz=timezone.utc)
            try:
                settings = load_morning_retracement_settings(public_config, policy)
                market = source.load(as_of=now, instruments=instruments)
                if settings.enabled:
                    assessments = scorer.assess(
                        market,
                        settings=settings,
                        as_of=now,
                    )
                    observation_ids = record_assessments.execute(
                        tuple(item[1] for item in assessments),
                        recorded_at=now,
                    )
                    if observation_ids:
                        logger.info(
                            "Stored live morning-retracement assessments",
                            extra={"assessment_count": len(observation_ids)},
                        )
                    local_day = now.astimezone(MOSCOW).date()
                    emitted = store.emitted_instruments_for_trading_day(
                        signal_type=SIGNAL_TYPE,
                        trading_day=local_day.isoformat(),
                    )
                    recommendations = scorer.execute(
                        market,
                        settings=settings,
                        already_emitted_instruments=emitted,
                        as_of=now,
                    )
                    for series, recommendation in recommendations:
                        prepared = _prepared_signal(
                            series=series,
                            recommendation=recommendation,
                            policy=policy,
                            settings=settings,
                            runtime=runtime,
                        )
                        event = _broker_event(prepared.signal)
                        ReliableEventProcessor(
                            detector=_PreparedRecommendation(prepared),
                            store=store,
                            publisher=publisher,
                            metrics=metrics,
                        ).process(event)
                        logger.info(
                            "Stored morning-retracement recommendation",
                            extra={
                                "instrument_id": series.instrument_id,
                                "probability": recommendation.model_probability,
                                "target_price": recommendation.target_price,
                            },
                        )
                outcome_batch = process_outcomes.execute(now=now, market=market)
                if outcome_batch.stored:
                    logger.info(
                        "Stored morning-retracement entry outcomes",
                        extra={
                            "stored_count": outcome_batch.stored,
                            "unavailable_count": outcome_batch.unavailable,
                        },
                    )
            except (
                TimeoutError,
                URLError,
                TransientClickHouseError,
                ConnectionError,
            ) as error:
                logger.warning(
                    "Morning-retracement cycle delayed by a temporary data-store "
                    "failure; the worker will retry without restarting",
                    extra={
                        "retry_delay_seconds": retry_delay,
                        "error_type": type(error).__name__,
                    },
                )
                time.sleep(retry_delay)
                retry_delay = min(max(poll_seconds, retry_delay * 2), 300.0)
                continue
            retry_delay = poll_seconds
            elapsed = time.monotonic() - cycle_started
            time.sleep(max(0.0, poll_seconds - elapsed))
    except KeyboardInterrupt:
        logger.info("Morning-retracement recommendation worker stopped")
    finally:
        publisher.close()
        store.close()


def _prepared_signal(
    *,
    series,
    recommendation: MorningRetracementRecommendation,
    policy: MorningRetracementRuntimePolicy,
    settings: MorningRetracementRuntimeSettings,
    runtime: RuntimeSettings,
) -> PreparedSignal:
    snapshot = recommendation.snapshot
    day = series.trading_day.isoformat()
    identity = (
        f"{series.instrument_id}:{day}:{policy.policy_version}:"
        f"settings-{settings.revision}"
    )
    signal_id = str(uuid5(_SIGNAL_NAMESPACE, identity))
    local_observed = snapshot.observed_at.astimezone(MOSCOW)
    deadline = local_observed.replace(
        hour=policy.deadline_local_minute // 60,
        minute=policy.deadline_local_minute % 60,
        second=0,
        microsecond=0,
    )
    median_hit = min(
        deadline,
        local_observed + timedelta(minutes=policy.expected_hit_minutes_median),
    )
    earliest_hit = min(
        deadline,
        local_observed + timedelta(minutes=policy.expected_hit_minutes_p25),
    )
    latest_hit = min(
        deadline,
        local_observed + timedelta(minutes=policy.expected_hit_minutes_p75),
    )
    payload: dict[str, object] = {
        "trading_day": day,
        "recommendation_status": "research_recommendation",
        "recommendation_disclaimer": "historical_probability_not_guarantee",
        "hypothesis_id": policy.hypothesis_id,
        "hypothesis_version": policy.hypothesis_version,
        "policy_version": policy.policy_version,
        "settings_revision": settings.revision,
        "model_fingerprint": policy.model.fingerprint,
        "expected_direction": recommendation.expected_direction,
        "previous_close": snapshot.previous_close,
        "running_extreme": snapshot.running_extreme,
        "current_price": snapshot.current_price,
        "entry_reference_price": snapshot.current_price,
        "excursion_bps": snapshot.excursion_bps,
        "target_fraction": policy.target_fraction,
        "target_price": recommendation.target_price,
        "initial_stop_price": recommendation.initial_stop_price,
        "break_even_trigger_price": recommendation.break_even_trigger_price,
        "break_even_stop_price": recommendation.break_even_stop_price,
        "deadline_at": deadline.isoformat(),
        "expected_hit_at": median_hit.isoformat(),
        "expected_hit_window_start": earliest_hit.isoformat(),
        "expected_hit_window_end": latest_hit.isoformat(),
        "model_probability": recommendation.model_probability,
        "probability_threshold": settings.probability_threshold,
        "historical_target_probability": policy.historical_target_probability,
        "historical_target_probability_lower": (
            policy.historical_target_probability_lower
        ),
        "historical_non_loss_probability": (
            policy.historical_non_loss_probability
        ),
        "historical_non_loss_probability_lower": (
            policy.historical_non_loss_probability_lower
        ),
        "evidence_sample_count": policy.historical_sample_count,
        "evidence_trading_days": policy.historical_trading_days,
        "relative_volume": recommendation.relative_volume,
        "maximum_relative_volume": settings.maximum_relative_volume,
        "active_minute_ratio": recommendation.active_minute_ratio,
        "cost_model_round_trip_bps": policy.round_trip_cost_bps,
    }
    source_event_id = "sha256:" + sha256(identity.encode("utf-8")).hexdigest()
    signal = SignalRecord(
        signal_id=signal_id,
        detected_at=datetime.now(tz=timezone.utc),
        instrument_id=series.instrument_id,
        ticker=series.ticker,
        class_code=series.class_code,
        alias=series.alias,
        source_event_type="scientific_candle_snapshot",
        signal_type=SIGNAL_TYPE,
        severity=_severity(recommendation.model_probability),
        metric_value=recommendation.model_probability,
        baseline_value=settings.probability_threshold,
        z_score=0.0,
        window_seconds=0,
        summary=(
            f"{series.ticker}: вероятный утренний возврат "
            f"{'вверх' if recommendation.expected_direction == 'up' else 'вниз'} "
            f"к {recommendation.target_price:g} с оценкой "
            f"{recommendation.model_probability * 100:.1f}% до "
            f"{deadline.strftime('%H:%M')} МСК"
        ),
        payload=payload,
        source_event_id=source_event_id,
        source_event_at=snapshot.observed_at,
        signal_schema_version="morning-retracement-signal-v1",
        expectation_catalog_version=runtime.expectation_catalog_version,
        detector_config_version=policy.policy_version,
        delivery_config_version=f"morning-settings-{settings.revision}",
        cost_model_version=runtime.cost_model_version,
        provenance_status="complete",
    )
    targets: tuple[DeliveryTarget, ...] = ()
    if settings.telegram_enabled:
        chat_id = load_secret("TELEGRAM_CHAT_ID") or ""
        thread_id = load_secret("TELEGRAM_MESSAGE_THREAD_ID") or ""
        if chat_id and load_secret("TELEGRAM_BOT_TOKEN"):
            targets = (DeliveryTarget("telegram", f"{chat_id}:{thread_id}"),)
    return PreparedSignal(signal=signal, delivery_targets=targets)


def _broker_event(signal: SignalRecord) -> BrokerEvent:
    payload = {"signal_id": signal.signal_id}
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    digest = sha256(encoded).digest()
    return BrokerEvent(
        event_id=str(signal.source_event_id),
        event_type="morning_retracement_snapshot",
        topic="internal.morning-retracement",
        partition_id=0,
        offset_id=int.from_bytes(digest[:7], "big"),
        payload_sha256=digest,
        payload=payload,
    )


def _severity(probability: float) -> int:
    if probability >= 0.80:
        return 1
    if probability >= 0.65:
        return 2
    return 3


def _required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


if __name__ == "__main__":
    main()

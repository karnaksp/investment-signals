from __future__ import annotations

from collections import deque
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from tinvest_signal_engine.adapters.legacy_detection import LegacyDetectionAdapter
from tinvest_signal_engine.application.reliable_processing import DetectionBatch
from tinvest_signal_engine.config import DetectorSettings, RuntimeSettings
from tinvest_signal_engine.detector_core import InstrumentState, SignalDetector
from tinvest_signal_engine.domain.detector_observations import (
    HISTORY_SAMPLING_POLICY_VERSION,
    deterministic_observation_id,
)
from tinvest_signal_engine.models import NormalizedEvent


def _event() -> NormalizedEvent:
    observed_at = datetime(2026, 7, 14, 7, 0, tzinfo=timezone.utc)
    return NormalizedEvent(
        event_id="event-below-threshold",
        event_type="trade",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="figi",
        uid="uid",
        lot=10,
        source_time=observed_at,
        received_at=observed_at,
        payload={"authorization": "must-not-be-copied"},
    )


def _below_threshold_observation():
    detector = SignalDetector(
        DetectorSettings(min_baseline_points=5, volume_zscore_threshold=4.0),
        detector_config_version="detector-v7",
        expectation_catalog_version="catalog-v3",
    )
    state = InstrumentState()
    history = deque([100.0, 101.0, 99.0, 100.0, 100.0])

    signals = detector._maybe_emit_from_history(
        event=_event(),
        state=state,
        cfg=detector._default_settings,
        signal_type="volume_spike",
        source_event_type="trade",
        history=history,
        threshold=4.0,
        value=100.5,
        baseline_label="rolling volume",
        window_seconds=60,
        summary_template="{ticker}",
    )

    assert signals == []
    observations = detector.drain_observations()
    assert detector.drain_observations() == ()
    assert len(observations) == 1
    return observations[0]


def test_below_threshold_evaluation_is_sampled_without_raw_payload() -> None:
    observation = _below_threshold_observation()

    assert observation.threshold_passed is False
    assert observation.detector_passed is False
    assert observation.signal_emitted is False
    assert observation.source_event_id == "event-below-threshold"
    assert observation.detector_config_version == "detector-v7"
    assert observation.expectation_catalog_version == "catalog-v3"
    assert observation.sampling_policy_version == HISTORY_SAMPLING_POLICY_VERSION
    assert not hasattr(observation, "payload")
    assert "must-not-be-copied" not in repr(observation)


def test_observation_identity_is_idempotent_across_reprocessing() -> None:
    first = _below_threshold_observation()
    second = _below_threshold_observation()

    assert first == second
    assert first.observation_id == deterministic_observation_id(
        source_event_id=first.source_event_id,
        signal_type=first.signal_type,
        detector_config_version=first.detector_config_version,
    )


def test_detector_process_exposes_sampled_below_threshold_evaluations() -> None:
    detector = SignalDetector(
        DetectorSettings(
            sample_every_seconds=0,
            min_baseline_points=2,
            volume_zscore_threshold=999.0,
            trade_count_zscore_threshold=999.0,
            price_return_zscore_threshold=999.0,
        ),
        detector_config_version="detector-v7",
    )
    start = datetime(2026, 7, 14, 7, 0, tzinfo=timezone.utc)

    for index in range(3):
        event = _event()
        event = NormalizedEvent(
            **{
                **event.to_dict(),
                "event_id": f"process-event-{index}",
                "source_time": start.replace(second=index),
                "received_at": start.replace(second=index),
                "payload": {
                    "quantity": 10,
                    "price": {"units": 100, "nano": 0},
                },
            }
        )
        signals = detector.process(event)
        observations = detector.drain_observations()

    assert signals == []
    assert observations
    assert all(not item.threshold_passed for item in observations)
    assert {item.source_event_id for item in observations} == {"process-event-2"}


def test_detection_batch_keeps_signals_and_observations_in_one_unit() -> None:
    observation = _below_threshold_observation()

    batch = DetectionBatch(observations=(observation,))

    assert batch.signals == ()
    assert batch.observations == (observation,)


def test_observation_provenance_requires_catalog_and_closed_status() -> None:
    observation = _below_threshold_observation()
    with pytest.raises(ValueError, match="expectation catalog"):
        replace(observation, expectation_catalog_version=None)
    with pytest.raises(ValueError, match="unsupported"):
        replace(observation, provenance_status="unknown")


def test_legacy_adapter_checkpoint_hydrates_restart_from_opaque_payload() -> None:
    settings = RuntimeSettings.from_env()
    first = LegacyDetectionAdapter(
        settings,
        delivered_count_since=lambda **kwargs: 0,
    )
    event = _event()
    event = replace(
        event,
        payload={
            "quantity": 10,
            "price": {"units": 100, "nano": 0},
        },
    )

    checkpoint = first.detect_batch(event.to_dict()).checkpoint

    assert checkpoint is not None
    restarted = LegacyDetectionAdapter(
        settings,
        delivered_count_since=lambda **kwargs: 0,
        checkpoints=(checkpoint,),
    )
    restored = restarted._detector._states[event.instrument_id]
    assert len(restored.trade_points) == 1
    assert restored.trade_points[0].quantity == 10.0

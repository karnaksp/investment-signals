from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import yaml

from tinvest_signal_engine.adapters.adaptive_calibration import (
    FileActiveThresholdSource,
    FileCalibrationDecisionSink,
)
from tinvest_signal_engine.domain.adaptive_calibration import (
    CalibrationObservation,
    DailyCalibrationPolicy,
    DetectorThresholds,
    calibrate_daily_thresholds,
)


def _observations() -> tuple[CalibrationObservation, ...]:
    rows: list[CalibrationObservation] = []
    first = date(2026, 7, 1)
    for offset in range(10):
        trading_day = first + timedelta(days=offset)
        rows.extend(
            (
                CalibrationObservation(trading_day, "SBER_TQBR", 4.05, 2.0, "contradicted"),
                CalibrationObservation(trading_day, "SBER_TQBR", 4.20, 3.0, "insignificant"),
                CalibrationObservation(trading_day, "SBER_TQBR", 4.45, 5.0, "confirmed"),
                CalibrationObservation(trading_day, "LKOH_TQBR", 4.55, 6.0, "confirmed"),
                CalibrationObservation(trading_day, "LKOH_TQBR", 4.65, 7.0, "confirmed"),
                CalibrationObservation(trading_day, "GAZP_TQBR", 4.08, 2.2, "contradicted"),
                CalibrationObservation(trading_day, "GAZP_TQBR", 4.25, 3.2, "insignificant"),
                CalibrationObservation(trading_day, "ROSN_TQBR", 4.48, 5.2, "confirmed"),
                CalibrationObservation(trading_day, "ROSN_TQBR", 4.58, 6.2, "confirmed"),
                CalibrationObservation(trading_day, "ROSN_TQBR", 4.68, 7.2, "confirmed"),
            )
        )
    return tuple(rows)


def test_daily_calibration_accepts_only_chronologically_validated_improvement() -> None:
    decision = calibrate_daily_thresholds(
        _observations(),
        DetectorThresholds(4.0, 0.0),
        DailyCalibrationPolicy(),
    )

    assert decision.should_apply
    assert decision.candidate is not None
    assert (
        decision.candidate.price_return_zscore_threshold > 4.0
        or decision.candidate.price_move_absolute_threshold_bps > 0.0
    )
    assert decision.candidate.price_return_zscore_threshold <= 4.6
    assert decision.candidate.price_move_absolute_threshold_bps <= 5.0
    assert decision.validation is not None
    assert decision.baseline_validation is not None
    assert (
        decision.validation.conservative_utility
        > decision.baseline_validation.conservative_utility
    )
    assert decision.training_days[-1] < decision.validation_days[0]


def test_daily_calibration_does_not_change_thresholds_without_enough_sessions() -> None:
    decision = calibrate_daily_thresholds(
        _observations()[:20],
        DetectorThresholds(4.0, 0.0),
    )

    assert not decision.should_apply
    assert decision.reason_code == "insufficient_sessions"


def test_file_sink_publishes_global_override_and_versioned_audit(tmp_path) -> None:
    detector_path = tmp_path / "detectors.yaml"
    overrides_path = tmp_path / "detectors.overrides.yaml"
    state_directory = tmp_path / "adaptive-calibration"
    detector_path.write_text(
        yaml.safe_dump(
            {
                "detector": {
                    "price_return_zscore_threshold": 4.0,
                    "price_move_absolute_threshold_bps": 0.0,
                }
            }
        ),
        encoding="utf-8",
    )
    overrides_path.write_text(
        yaml.safe_dump({"per_instrument": {"SBER_TQBR": {"volume_zscore_threshold": 5.0}}}),
        encoding="utf-8",
    )
    decision = calibrate_daily_thresholds(
        _observations(),
        DetectorThresholds(4.0, 0.0),
    )
    assert decision.should_apply

    FileCalibrationDecisionSink(overrides_path, state_directory).persist(
        decision,
        evaluated_at=datetime(2026, 7, 11, 20, 20, tzinfo=UTC),
    )

    active = FileActiveThresholdSource(
        detector_path,
        overrides_path,
    ).active_price_jump_thresholds()
    assert active == decision.candidate
    persisted = yaml.safe_load(overrides_path.read_text(encoding="utf-8"))
    assert "SBER_TQBR" in persisted["per_instrument"]
    latest = json.loads((state_directory / "latest.json").read_text(encoding="utf-8"))
    assert latest["status"] == "accepted"
    assert (state_directory / "history" / f"{decision.version}.json").exists()

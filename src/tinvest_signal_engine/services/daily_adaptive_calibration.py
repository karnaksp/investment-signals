"""Composition root for the lightweight post-session calibration job."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

from psycopg import connect

from tinvest_signal_engine.adapters.adaptive_calibration import (
    FileActiveThresholdSource,
    FileCalibrationDecisionSink,
    PostgresCalibrationOutcomeSource,
)
from tinvest_signal_engine.application.adaptive_calibration import (
    RunDailyAdaptiveCalibration,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.adaptive_calibration import DailyCalibrationPolicy


def run_once(
    settings: RuntimeSettings | None = None,
    *,
    now: datetime | None = None,
    service_name: str = "dagster",
):
    runtime = settings or RuntimeSettings.from_env(service_name=service_name)
    connection = connect(
        host=runtime.postgres_host,
        port=runtime.postgres_port,
        dbname=runtime.postgres_database,
        user=runtime.postgres_username,
        password=runtime.postgres_password,
    )
    try:
        use_case = RunDailyAdaptiveCalibration(
            outcomes=PostgresCalibrationOutcomeSource(connection),
            active_thresholds=FileActiveThresholdSource(
                runtime.detector_path,
                runtime.detector_overrides_path,
            ),
            decisions=FileCalibrationDecisionSink(
                runtime.detector_overrides_path,
                Path(
                    os.getenv(
                        "ADAPTIVE_CALIBRATION_STATE_DIR",
                        "/var/lib/investment-signals/adaptive-calibration",
                    )
                ),
            ),
            policy=DailyCalibrationPolicy(
                minimum_sessions=_env_int("ADAPTIVE_CALIBRATION_MIN_SESSIONS", 10),
                minimum_training_observations=_env_int(
                    "ADAPTIVE_CALIBRATION_MIN_TRAINING_OBSERVATIONS", 30
                ),
                minimum_validation_observations=_env_int(
                    "ADAPTIVE_CALIBRATION_MIN_VALIDATION_OBSERVATIONS", 10
                ),
                minimum_candidate_coverage=_env_float(
                    "ADAPTIVE_CALIBRATION_MIN_COVERAGE", 0.50
                ),
                minimum_validation_improvement=_env_float(
                    "ADAPTIVE_CALIBRATION_MIN_IMPROVEMENT", 0.02
                ),
                maximum_daily_zscore_increase=_env_float(
                    "ADAPTIVE_CALIBRATION_MAX_ZSCORE_INCREASE", 0.15
                ),
                maximum_daily_absolute_increase_bps=_env_float(
                    "ADAPTIVE_CALIBRATION_MAX_ABSOLUTE_INCREASE_BPS", 5.0
                ),
            ),
            lookback_days=_env_int("ADAPTIVE_CALIBRATION_LOOKBACK_DAYS", 60),
        )
        return use_case.execute(now=now or datetime.now(UTC))
    finally:
        connection.close()


def _env_int(name: str, default: int) -> int:
    return int((os.getenv(name) or str(default)).strip())


def _env_float(name: str, default: float) -> float:
    return float((os.getenv(name) or str(default)).strip())

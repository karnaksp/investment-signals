"""PostgreSQL and file adapters for daily detector calibration."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from psycopg.rows import dict_row

from tinvest_signal_engine.config import load_detector_config
from tinvest_signal_engine.domain.adaptive_calibration import (
    CalibrationObservation,
    DailyCalibrationDecision,
    DetectorThresholds,
)


class PostgresCalibrationOutcomeSource:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def mature_price_jump_outcomes(
        self,
        *,
        since: datetime,
        until: datetime,
    ) -> tuple[CalibrationObservation, ...]:
        if since.tzinfo is None or until.tzinfo is None:
            raise ValueError("calibration window must be timezone-aware")
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    to_regclass('public.signal_auto_verdicts') AS product_relation,
                    to_regclass('public.core_directional_signal_outcomes')
                        AS core_relation
                """
            )
            relations = cursor.fetchone()
            product_verdicts_available = (
                relations["product_relation"] is not None
            )
            core_outcomes_available = relations["core_relation"] is not None
            if not product_verdicts_available and not core_outcomes_available:
                return ()
            cursor.execute(
                _PRODUCT_OUTCOMES_SQL
                if product_verdicts_available
                else _CORE_OUTCOMES_SQL,
                {"since": since, "until": until},
            )
            rows = cursor.fetchall()
        return tuple(
            CalibrationObservation(
                trading_day=row["trading_day"],
                instrument_id=str(row["instrument_id"]),
                z_score=float(row["z_score"]),
                absolute_move_bps=float(row["absolute_move_bps"]),
                verdict=str(row["verdict"]),
            )
            for row in rows
        )


_CORE_OUTCOMES_SQL = """
                SELECT
                    (outcome.source_event_at AT TIME ZONE 'Europe/Moscow')::date
                        AS trading_day,
                    outcome.instrument_id,
                    abs(signal.z_score) AS z_score,
                    COALESCE(
                        abs((signal.payload_json->>'abs_price_change_bps')::double precision),
                        abs(signal.metric_value)
                    ) AS absolute_move_bps,
                    outcome.verdict
                FROM core_directional_signal_outcomes AS outcome
                JOIN market_signals AS signal ON signal.signal_id = outcome.signal_id
                WHERE outcome.signal_type = 'price_jump'
                  AND signal.source_event_type <> 'historical_candle_replay'
                  AND outcome.verdict IN (
                      'confirmed', 'contradicted', 'insignificant'
                  )
                  AND outcome.source_event_at >= %(since)s
                  AND outcome.source_event_at < %(until)s
                  AND signal.z_score IS NOT NULL
                ORDER BY outcome.source_event_at, outcome.signal_id
"""


_PRODUCT_OUTCOMES_SQL = """
WITH current_verdict AS (
    SELECT DISTINCT ON (verdict.signal_id)
        verdict.signal_id,
        COALESCE(
            verdict.source_event_at,
            signal.source_event_at,
            signal.detected_at
        ) AS source_event_at,
        verdict.verdict_status AS verdict
    FROM signal_auto_verdicts AS verdict
    JOIN market_signals AS signal ON signal.signal_id = verdict.signal_id
    WHERE signal.signal_type = 'price_jump'
      AND signal.source_event_type <> 'historical_candle_replay'
      AND verdict.verdict_status IN (
          'confirmed', 'contradicted', 'insignificant'
      )
      AND COALESCE(
            verdict.source_event_at,
            signal.source_event_at,
            signal.detected_at
          ) >= %(since)s
      AND COALESCE(
            verdict.source_event_at,
            signal.source_event_at,
            signal.detected_at
          ) < %(until)s
    ORDER BY
        verdict.signal_id,
        (verdict.horizon_seconds = 300 AND verdict.clock_mode = 'wall_clock') DESC,
        verdict.evaluated_at DESC,
        verdict.verdict_id DESC
)
SELECT
    (verdict.source_event_at AT TIME ZONE 'Europe/Moscow')::date AS trading_day,
    signal.instrument_id,
    abs(signal.z_score) AS z_score,
    COALESCE(
        abs((signal.payload_json->>'abs_price_change_bps')::double precision),
        abs(signal.metric_value)
    ) AS absolute_move_bps,
    verdict.verdict
FROM current_verdict AS verdict
JOIN market_signals AS signal ON signal.signal_id = verdict.signal_id
ORDER BY verdict.source_event_at, verdict.signal_id
"""


class FileActiveThresholdSource:
    def __init__(self, detector_path: Path, overrides_path: Path) -> None:
        self._detector_path = detector_path
        self._overrides_path = overrides_path

    def active_price_jump_thresholds(self) -> DetectorThresholds:
        settings = load_detector_config(
            self._detector_path,
            self._overrides_path,
        ).default
        return DetectorThresholds(
            price_return_zscore_threshold=settings.price_return_zscore_threshold,
            price_move_absolute_threshold_bps=(
                settings.price_move_absolute_threshold_bps
            ),
        )


class FileCalibrationDecisionSink:
    """Atomically publish accepted overrides and a small versioned audit."""

    def __init__(self, overrides_path: Path, state_directory: Path) -> None:
        self._overrides_path = overrides_path
        self._state_directory = state_directory

    def persist(
        self,
        decision: DailyCalibrationDecision,
        *,
        evaluated_at: datetime,
    ) -> None:
        self._state_directory.mkdir(parents=True, exist_ok=True)
        payload = _decision_payload(decision, evaluated_at=evaluated_at)
        history_directory = self._state_directory / "history"
        history_directory.mkdir(parents=True, exist_ok=True)
        _atomic_json(history_directory / f"{decision.version}.json", payload)
        _atomic_json(self._state_directory / "latest.json", payload)
        if not decision.should_apply or decision.candidate is None:
            return

        current = _yaml_mapping(self._overrides_path)
        detector = current.get("detector") or {}
        if not isinstance(detector, dict):
            raise ValueError("detector override must be a mapping")
        candidate = decision.candidate
        current["generated_at"] = evaluated_at.isoformat()
        current["calibration_version"] = decision.version
        current["calibration_mode"] = "daily_mature_outcomes_chronological_holdout"
        current["detector"] = {
            **detector,
            "price_return_zscore_threshold": (
                candidate.price_return_zscore_threshold
            ),
            "price_move_absolute_threshold_bps": (
                candidate.price_move_absolute_threshold_bps
            ),
        }
        _atomic_yaml(self._overrides_path, current)


def _decision_payload(
    decision: DailyCalibrationDecision,
    *,
    evaluated_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": "daily-adaptive-calibration-v1",
        "evaluated_at": evaluated_at.isoformat(),
        "version": decision.version,
        "status": decision.status,
        "reason_code": decision.reason_code,
        "active": asdict(decision.active),
        "candidate": asdict(decision.candidate) if decision.candidate else None,
        "training": asdict(decision.training) if decision.training else None,
        "validation": asdict(decision.validation) if decision.validation else None,
        "baseline_validation": (
            asdict(decision.baseline_validation)
            if decision.baseline_validation
            else None
        ),
        "training_days": [item.isoformat() for item in decision.training_days],
        "validation_days": [item.isoformat() for item in decision.validation_days],
    }


def _yaml_mapping(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(value, dict):
        raise ValueError("detector overrides must contain a mapping")
    return dict(value)


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _atomic_yaml(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    os.replace(temporary, path)

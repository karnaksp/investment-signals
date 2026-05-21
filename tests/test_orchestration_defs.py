"""Dagster code location: jobs и расписания."""

import pytest

pytest.importorskip("dagster")

from tinvest_signal_engine import orchestration_defs


def test_dagster_definitions_exposes_jobs_and_schedules() -> None:
    assert orchestration_defs.threshold_recalc_job.name == "threshold_recalc_job"
    assert orchestration_defs.unary_kafka_poll_once_job.name == "unary_kafka_poll_once_job"
    assert (
        orchestration_defs.historical_baseline_recalc_job.name
        == "historical_baseline_recalc_job"
    )
    assert orchestration_defs.daily_threshold_schedule.name == "daily_threshold_recalc"
    assert (
        orchestration_defs.quarter_hourly_unary_schedule.name
        == "market_unary_poll_schedule"
    )
    assert (
        orchestration_defs.daily_historical_baseline_schedule.name
        == "daily_historical_baseline_recalc"
    )
    assert orchestration_defs.defs is not None

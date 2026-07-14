from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "study_tinvest_directional_hypothesis.py"
)
SPEC = importlib.util.spec_from_file_location("tinvest_directional_study", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
study = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = study
SPEC.loader.exec_module(study)


def test_cost_is_subtracted_symmetrically_outside_materiality() -> None:
    policy = study.StudyPolicy(
        outcome_min_move_bps=10.0,
        outcome_volatility_multiplier=0.0,
        round_trip_cost_bps=4.0,
    )

    below = study.classify_directional(13.999, 1.0, 1, policy)
    confirmed = study.classify_directional(14.0, 1.0, 1, policy)
    contradicted = study.classify_directional(-14.0, 1.0, 1, policy)

    assert below[0] == "insignificant"
    assert below[1:] == pytest.approx((9.999, -17.999, 10.0))
    assert confirmed == ("confirmed", 10.0, -18.0, 10.0)
    assert contradicted == ("contradicted", -18.0, 10.0, 10.0)


def test_materiality_scales_with_square_root_of_horizon() -> None:
    policy = study.StudyPolicy(
        outcome_min_move_bps=1.0,
        outcome_volatility_multiplier=2.0,
        round_trip_cost_bps=0.0,
    )

    result = study.classify_directional(20.0, 3.0, 9, policy)

    assert result == ("confirmed", 20.0, -20.0, 18.0)


def test_forward_endpoint_never_uses_pre_target_price() -> None:
    start = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)  # 10:05 MSK
    rows = [
        study.Candle("SBER", start + timedelta(minutes=index), 100.0, close, True)
        for index, close in enumerate((100.0, 500.0, 102.0, 104.0))
    ]

    result = study._forward_median(rows, 0, horizon_minutes=2, grace_minutes=1)

    assert result == 103.0


def test_missing_target_path_becomes_terminal_inconclusive() -> None:
    at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    event = study.DetectedEvent(
        ticker="SBER",
        trading_day=date(2026, 7, 10),
        session_bucket=0,
        at=at,
        direction=1,
        event_move_bps=20.0,
        baseline_move_bps=4.0,
        detector_z_score=5.0,
        baseline_sigma_bps=2.0,
    )
    grouped = {
        ("SBER", event.trading_day): [
            study.Candle("SBER", at, 100.0, 100.0, True),
            study.Candle("SBER", at + timedelta(minutes=2), 101.0, 101.0, True),
        ]
    }

    outcomes, missing = study.calculate_outcomes(
        grouped, [event], [1], study.StudyPolicy()
    )

    assert missing == {1: 1}
    assert len(outcomes) == 1
    assert outcomes[0].verdict == "inconclusive"
    assert outcomes[0].reason_code == "forward_price_unavailable_or_session_gap"


def test_chronological_split_uses_all_supplied_trading_days() -> None:
    days = [date(2026, 1, day) for day in range(1, 11)]

    train, validation = study.chronological_split(days)

    assert train == set(days[:7])
    assert validation == set(days[7:])
    assert max(train) < min(validation)


def test_day_cluster_bootstrap_is_deterministic() -> None:
    values = [
        (date(2026, 1, 1), 1.0),
        (date(2026, 1, 1), 3.0),
        (date(2026, 1, 2), 8.0),
    ]

    first = study.day_cluster_bootstrap_interval(values, samples=200, seed=7)
    second = study.day_cluster_bootstrap_interval(values, samples=200, seed=7)

    assert first == second
    assert first[0] is not None and first[1] is not None
    assert first[0] <= first[1]


def _gate_row(*, n: int, sessions: int, expected_ci: tuple, reverse_ci: tuple, lift_ci: tuple) -> dict:
    return {
        "split": "validation",
        "horizon_minutes": 5,
        "n": n,
        "sessions": sessions,
        "outcome_coverage": 0.95,
        "matched_control_coverage": 0.95,
        "net_expected_day_bootstrap_95": expected_ci,
        "net_reverse_day_bootstrap_95": reverse_ci,
        "lift_day_bootstrap_95": lift_ci,
    }


def test_inverse_gate_requires_sample_and_opposite_intervals() -> None:
    statistically_inverse = _gate_row(
        n=299,
        sessions=30,
        expected_ci=(-5.0, -1.0),
        reverse_ci=(1.0, 5.0),
        lift_ci=(-5.0, -1.0),
    )
    _, _, under_sampled = study.build_decision([statistically_inverse])
    statistically_inverse["n"] = 300
    _, continuation, admitted = study.build_decision([statistically_inverse])

    assert under_sampled is False
    assert continuation is False
    assert admitted is True


def test_continuation_gate_requires_positive_net_and_control_lift() -> None:
    row = _gate_row(
        n=300,
        sessions=30,
        expected_ci=(0.1, 3.0),
        reverse_ci=(-23.0, -20.1),
        lift_ci=(-0.1, 2.0),
    )
    _, rejected, _ = study.build_decision([row])
    row["lift_day_bootstrap_95"] = (0.1, 2.0)
    _, admitted, inverse = study.build_decision([row])

    assert rejected is False
    assert admitted is True
    assert inverse is False


def test_exploratory_horizon_cannot_clear_primary_gate() -> None:
    row = _gate_row(
        n=300,
        sessions=30,
        expected_ci=(0.1, 3.0),
        reverse_ci=(-23.0, -20.1),
        lift_ci=(0.1, 2.0),
    )
    row["horizon_minutes"] = 15

    _, continuation, inverse = study.build_decision([row])

    assert continuation is False
    assert inverse is False


def test_failure_diagnostic_redacts_secrets() -> None:
    diagnostic = study.redact_diagnostic(
        "Authorization: Bearer abc.def token=plain password:pw secret value"
    )

    assert "abc.def" not in diagnostic
    assert "plain" not in diagnostic
    assert "password:pw" not in diagnostic
    assert "<redacted>" in diagnostic


def test_failure_artifact_persists_only_redacted_operational_context(tmp_path: Path) -> None:
    run_dir = study.write_failure_artifact(
        tmp_path,
        scope={
            "tickers": ["SBER"],
            "from": "2026-07-01",
            "to": "2026-07-13",
            "horizons": [1, 5, 15],
        },
        reason_code="tls_certificate_verify_failed",
        message="TLS certificate verification failed before the study could read T-Invest data.",
        remediation="Pass the correct PEM bundle with --ca-cert.",
        ca_cert=None,
    )

    payload = json.loads((run_dir / "failure.json").read_text(encoding="utf-8"))
    report = (run_dir / "report.md").read_text(encoding="utf-8")

    assert payload["status"] == "failed"
    assert payload["data_boundary"] == {
        "raw_candles_persisted": False,
        "instrument_uids_persisted": False,
        "token_persisted": False,
    }
    assert payload["tls_verification"] == "enabled"
    assert "abc.def" not in report

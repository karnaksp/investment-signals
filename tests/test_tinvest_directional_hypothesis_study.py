from __future__ import annotations

import importlib.util
import io
import json
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import httpx


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


def test_prepare_candles_keeps_morning_phase_and_excludes_transition() -> None:
    morning = datetime(2026, 7, 15, 4, 30, tzinfo=timezone.utc)  # 07:30 Moscow
    transition = datetime(2026, 7, 15, 6, 55, tzinfo=timezone.utc)  # 09:55 Moscow
    grouped, quality = study.prepare_candles(
        [
            study.Candle("SBER", morning, 100.0, 101.0, True),
            study.Candle("SBER", transition, 101.0, 102.0, True),
        ]
    )

    assert grouped[("SBER", morning.date())][0].at == morning
    assert quality["complete_regular_candles"] == 1
    assert quality["outside_regular_session_candles"] == 1


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


def test_study_window_defaults_to_rolling_calendar_days() -> None:
    start, end, selection, days = study.resolve_study_window(
        start_day=None,
        end_day=date(2026, 7, 14),
        calendar_days=160,
        today=date(2026, 7, 15),
    )

    assert start == date(2026, 2, 5)
    assert end == date(2026, 7, 14)
    assert selection == "rolling_calendar_days"
    assert days == 160


def test_env_loader_supports_token_file_without_logging_secret(tmp_path: Path) -> None:
    secret = tmp_path / "tinvest-token"
    secret.write_text("broker-secret-token\n", encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text("TINVEST_TOKEN_FILE=tinvest-token\n", encoding="utf-8")

    assert study.load_env_value(env, "TINVEST_TOKEN") == "broker-secret-token"


def test_env_loader_prefers_direct_token_over_token_file(tmp_path: Path) -> None:
    secret = tmp_path / "tinvest-token"
    secret.write_text("file-token\n", encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text(
        "export TINVEST_TOKEN=direct-token\nTINVEST_TOKEN_FILE=tinvest-token\n",
        encoding="utf-8",
    )

    assert study.load_env_value(env, "TINVEST_TOKEN") == "direct-token"


def test_env_loader_rejects_empty_token_file_without_leaking_path(tmp_path: Path) -> None:
    secret = tmp_path / "tinvest-token"
    secret.write_text("\n", encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text(f"TINVEST_TOKEN_FILE={secret}\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="TINVEST_TOKEN file is empty") as failure:
        study.load_env_value(env, "TINVEST_TOKEN")

    assert str(secret) not in str(failure.value)


def test_study_window_supports_explicit_non_overlapping_cohort() -> None:
    start, end, selection, days = study.resolve_study_window(
        start_day=date(2026, 7, 15),
        end_day=date(2026, 8, 31),
        calendar_days=160,
        today=date(2026, 9, 1),
    )

    assert start == date(2026, 7, 15)
    assert end == date(2026, 8, 31)
    assert selection == "explicit_date_range"
    assert days == 48


def test_explicit_study_window_rejects_single_day_cohort() -> None:
    with pytest.raises(ValueError, match="at least 2 calendar days"):
        study.resolve_study_window(
            start_day=date(2026, 7, 15),
            end_day=date(2026, 7, 15),
            calendar_days=160,
            today=date(2026, 7, 16),
        )


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
        sessions=8,
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
        sessions=8,
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
        sessions=8,
        expected_ci=(0.1, 3.0),
        reverse_ci=(-23.0, -20.1),
        lift_ci=(0.1, 2.0),
    )
    row["horizon_minutes"] = 15

    _, continuation, inverse = study.build_decision([row])

    assert continuation is False
    assert inverse is False


def test_primary_gate_uses_versioned_eight_session_minimum() -> None:
    row = _gate_row(
        n=300,
        sessions=7,
        expected_ci=(0.1, 3.0),
        reverse_ci=(-23.0, -20.1),
        lift_ci=(0.1, 2.0),
    )

    _, below_minimum, _ = study.build_decision([row])
    row["sessions"] = 8
    _, at_minimum, _ = study.build_decision([row])

    assert study.StudyPolicy().minimum_validation_sessions == 8
    assert below_minimum is False
    assert at_minimum is True


def test_failure_diagnostic_redacts_secrets() -> None:
    diagnostic = study.redact_diagnostic(
        "Authorization: Bearer abc.def token=plain password:pw secret value"
    )

    assert "abc.def" not in diagnostic
    assert "plain" not in diagnostic
    assert "password:pw" not in diagnostic
    assert "<redacted>" in diagnostic


class _APIResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class _RetryingAPIClient:
    def __init__(self, failures: list[BaseException], response: _APIResponse) -> None:
        self.failures = failures
        self.response = response
        self.calls = 0

    def post(self, _url: str, *, json: dict) -> _APIResponse:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return self.response


def test_api_post_retries_timeout_without_leaking_payload() -> None:
    request = httpx.Request("POST", "https://example.invalid")
    client = _RetryingAPIClient(
        [httpx.ReadTimeout("token=secret", request=request)],
        _APIResponse(200, {"ok": True}),
    )
    sleeps: list[float] = []

    result = study.api_post(
        client,  # type: ignore[arg-type]
        "MarketDataService/GetCandles",
        {"instrumentId": "secret-uid"},
        attempts=2,
        sleeper=sleeps.append,
    )

    assert result == {"ok": True}
    assert client.calls == 2
    assert sleeps == [0.75]


def test_api_post_does_not_retry_certificate_verification_failure() -> None:
    request = httpx.Request("POST", "https://example.invalid")
    client = _RetryingAPIClient(
        [httpx.ConnectError("CERTIFICATE_VERIFY_FAILED token=secret", request=request)],
        _APIResponse(200, {"ok": True}),
    )

    with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
        study.api_post(
            client,  # type: ignore[arg-type]
            "MarketDataService/GetCandles",
            {},
            attempts=3,
            sleeper=lambda _seconds: None,
        )

    assert client.calls == 1


def test_api_post_exhausted_transport_retry_is_redacted() -> None:
    request = httpx.Request("POST", "https://example.invalid")
    client = _RetryingAPIClient(
        [httpx.ReadTimeout("token=secret", request=request)],
        _APIResponse(200, {"ok": True}),
    )

    with pytest.raises(RuntimeError) as error:
        study.api_post(
            client,  # type: ignore[arg-type]
            "MarketDataService/GetCandles",
            {},
            attempts=1,
            sleeper=lambda _seconds: None,
        )

    assert "secret" not in str(error.value)
    reason_code, _, _ = study.classify_tinvest_failure(error.value)
    assert reason_code == "tinvest_transport_retry_exhausted"


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


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class _Client:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.urls: list[str] = []

    def get(self, url: str) -> _Response:
        self.urls.append(url)
        return _Response(self.content)


def _zip_with_member(name: str, content: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, content)
    return buffer.getvalue()


def test_prepare_russian_trusted_ca_writes_only_pinned_bundle(tmp_path: Path) -> None:
    bundle = (
        b"-----BEGIN CERTIFICATE-----\nfirst\n-----END CERTIFICATE-----\n"
        b"-----BEGIN CERTIFICATE-----\nsecond\n-----END CERTIFICATE-----\n"
    )
    expected_digest = study.hashlib.sha256(bundle).hexdigest()
    old_digest = study.RUSSIAN_TRUSTED_CA_SHA256
    study.RUSSIAN_TRUSTED_CA_SHA256 = expected_digest
    client = _Client(_zip_with_member(study.RUSSIAN_TRUSTED_CA_MEMBER, bundle))
    output = tmp_path / "russiantrustedca2024.pem"
    try:
        metadata = study.prepare_russian_trusted_ca(output, client=client)
    finally:
        study.RUSSIAN_TRUSTED_CA_SHA256 = old_digest

    assert output.read_bytes() == bundle
    assert metadata == {
        "path": str(output),
        "sha256": expected_digest,
        "source_url": study.RUSSIAN_TRUSTED_CA_ZIP_URL,
        "archive_member": study.RUSSIAN_TRUSTED_CA_MEMBER,
        "certificate_count": 2,
    }
    assert client.urls == [study.RUSSIAN_TRUSTED_CA_ZIP_URL]


def test_prepare_russian_trusted_ca_rejects_changed_bundle(tmp_path: Path) -> None:
    bundle = b"-----BEGIN CERTIFICATE-----\nchanged\n-----END CERTIFICATE-----\n"
    client = _Client(_zip_with_member(study.RUSSIAN_TRUSTED_CA_MEMBER, bundle))

    with pytest.raises(RuntimeError, match="hash mismatch"):
        study.prepare_russian_trusted_ca(tmp_path / "ca.pem", client=client)

    assert not (tmp_path / "ca.pem").exists()

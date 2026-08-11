from __future__ import annotations

from pathlib import Path

import pytest

from tinvest_signal_engine.services import candle_cache


def test_ca_cert_uses_public_environment_setting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = tmp_path / "environment-ca.pem"
    monkeypatch.setenv("TINVEST_TRUSTED_CA_FILE", str(bundle))

    args = candle_cache.parse_args([])

    assert args.ca_cert == bundle
    assert args.source == "daily"
    assert args.request_interval is None


def test_ca_cert_cli_option_overrides_public_environment_setting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment_bundle = tmp_path / "environment-ca.pem"
    cli_bundle = tmp_path / "cli-ca.pem"
    monkeypatch.setenv("TINVEST_TRUSTED_CA_FILE", str(environment_bundle))

    args = candle_cache.parse_args(["--ca-cert", str(cli_bundle)])

    assert args.ca_cert == cli_bundle


def test_archive_source_can_be_selected_for_bounded_setup_backfill() -> None:
    args = candle_cache.parse_args(["--source", "archive"])

    assert args.source == "archive"


def test_command_fails_clearly_when_trusted_ca_bundle_is_missing(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing-ca.pem"

    with pytest.raises(SystemExit, match="Trusted CA bundle does not exist"):
        candle_cache.main(["--ca-cert", str(missing)])

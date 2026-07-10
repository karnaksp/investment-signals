from __future__ import annotations

from pathlib import Path

import pytest

from tinvest_signal_engine.config import load_secret


def test_secret_can_be_loaded_from_utf8_file(tmp_path: Path) -> None:
    secret_file = tmp_path / "postgres-password"
    secret_file.write_text("correct horse battery staple\n", encoding="utf-8")

    value = load_secret(
        "POSTGRES_PASSWORD",
        service_name="detector",
        environ={"POSTGRES_PASSWORD_FILE": str(secret_file)},
    )

    assert value == "correct horse battery staple"


def test_direct_and_file_secret_are_mutually_exclusive(tmp_path: Path) -> None:
    secret_file = tmp_path / "token"
    secret_file.write_text("from-file", encoding="utf-8")

    with pytest.raises(ValueError, match="Set only one"):
        load_secret(
            "TINVEST_TOKEN",
            service_name="ingestor",
            environ={
                "TINVEST_TOKEN": "direct",
                "TINVEST_TOKEN_FILE": str(secret_file),
            },
        )


def test_service_cannot_read_another_services_secret(tmp_path: Path) -> None:
    secret_file = tmp_path / "tinvest-token"
    secret_file.write_text("broker-token", encoding="utf-8")

    value = load_secret(
        "TINVEST_TOKEN",
        default="",
        service_name="detector",
        environ={"TINVEST_TOKEN_FILE": str(secret_file)},
    )

    assert value == ""


def test_unknown_service_name_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown service"):
        load_secret("TINVEST_TOKEN", service_name="typo", environ={})

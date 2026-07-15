from __future__ import annotations

from pathlib import Path

import pytest

from tinvest_signal_engine.config import RuntimeSettings, load_secret


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


def test_observation_worker_can_only_read_its_database_secrets() -> None:
    environ = {
        "CLICKHOUSE_PASSWORD": "clickhouse-secret",
        "POSTGRES_PASSWORD": "postgres-secret",
        "TINVEST_TOKEN": "broker-token",
    }

    assert (
        load_secret(
            "CLICKHOUSE_PASSWORD",
            service_name="observation_worker",
            environ=environ,
        )
        == "clickhouse-secret"
    )
    assert (
        load_secret(
            "POSTGRES_PASSWORD",
            service_name="observation_worker",
            environ=environ,
        )
        == "postgres-secret"
    )
    assert (
        load_secret(
            "TINVEST_TOKEN",
            default="",
            service_name="observation_worker",
            environ=environ,
        )
        == ""
    )


@pytest.mark.parametrize("service_name", ["api", "detector"])
def test_redis_url_secret_is_scoped_to_consumers(
    tmp_path: Path, service_name: str
) -> None:
    secret_file = tmp_path / "redis-url"
    secret_file.write_text("redis://detector:secret@redis:6379/0\n", encoding="utf-8")

    value = load_secret(
        "REDIS_URL",
        service_name=service_name,
        environ={"REDIS_URL_FILE": str(secret_file)},
    )

    assert value == "redis://detector:secret@redis:6379/0"


def test_delivery_worker_reads_telegram_config_from_service_secret_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token_file = tmp_path / "telegram-bot-token"
    chat_file = tmp_path / "telegram-chat-id"
    thread_file = tmp_path / "telegram-message-thread-id"
    token_file.write_text("bot-token\n", encoding="utf-8")
    chat_file.write_text("-1001234567890\n", encoding="utf-8")
    thread_file.write_text("42\n", encoding="utf-8")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("TELEGRAM_CHAT_ID_FILE", str(chat_file))
    monkeypatch.setenv("TELEGRAM_MESSAGE_THREAD_ID_FILE", str(thread_file))

    settings = RuntimeSettings.from_env(service_name="delivery_worker")

    assert settings.telegram_bot_token == "bot-token"
    assert settings.telegram_chat_id == "-1001234567890"
    assert settings.telegram_message_thread_id == 42

"""Проверка согласованности env для JSON/protobuf на Kafka."""

from __future__ import annotations

from .config import RuntimeSettings


def validate_kafka_wire_settings(
    settings: RuntimeSettings,
    *,
    check_raw: bool = True,
    check_signal: bool = True,
) -> None:
    checks: list[tuple[str, str]] = []
    if check_raw:
        checks.append((settings.kafka_raw_value_format, "KAFKA_RAW_VALUE_FORMAT"))
    if check_signal:
        checks.append(
            (settings.kafka_signal_value_format, "KAFKA_SIGNAL_VALUE_FORMAT")
        )
    for fmt, env_name in checks:
        if fmt not in {"json", "protobuf"}:
            raise ValueError(
                f"{env_name} must be 'json' or 'protobuf', got {fmt!r}"
            )
    if check_raw and settings.kafka_raw_value_format == "protobuf":
        if (
            settings.kafka_protobuf_schema_id_raw is None
            and not settings.schema_registry_url
        ):
            raise RuntimeError(
                "Protobuf on raw topic requires SCHEMA_REGISTRY_URL or "
                "KAFKA_PROTOBUF_SCHEMA_ID_RAW"
            )
    if check_signal and settings.kafka_signal_value_format == "protobuf":
        if (
            settings.kafka_protobuf_schema_id_signal is None
            and not settings.schema_registry_url
        ):
            raise RuntimeError(
                "Protobuf on signal topic requires SCHEMA_REGISTRY_URL or "
                "KAFKA_PROTOBUF_SCHEMA_ID_SIGNAL"
            )

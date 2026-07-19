"""Composition root for streaming scientific one-minute candles."""

from __future__ import annotations

import os

from tinvest_signal_engine.adapters.clickhouse_scientific_candles import (
    ClickHouseScientificCandleStore,
)
from tinvest_signal_engine.adapters.kafka_scientific_candles import (
    ScientificCandleKafkaRuntime,
    build_scientific_candle_consumer,
)
from tinvest_signal_engine.application.scientific_candles import (
    ScientificCandleJournalProcessor,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.kafka_wire_config import validate_kafka_wire_settings
from tinvest_signal_engine.logging_utils import configure_logging


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="scientific_candle_writer")
    validate_kafka_wire_settings(settings, check_signal=False)
    configure_logging(settings.log_level)
    if not settings.clickhouse_http_url:
        raise RuntimeError("CLICKHOUSE_HTTP_URL is required")
    if not settings.clickhouse_http_username:
        raise RuntimeError("CLICKHOUSE_USERNAME is required")
    if not settings.clickhouse_http_password:
        raise RuntimeError("CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required")
    store = ClickHouseScientificCandleStore(
        base_url=settings.clickhouse_http_url,
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=settings.clickhouse_http_username,
        password=settings.clickhouse_http_password,
    )
    ScientificCandleKafkaRuntime(
        consumer=build_scientific_candle_consumer(
            topic=settings.kafka_raw_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=(
                os.getenv("SCIENTIFIC_CANDLE_CONSUMER_GROUP")
                or "scientific-candle-writer-v1"
            ).strip(),
            auto_offset_reset=settings.kafka_auto_offset_reset,
            value_format=settings.kafka_raw_value_format,
        ),
        processor=ScientificCandleJournalProcessor(store),
    ).run()


if __name__ == "__main__":
    main()

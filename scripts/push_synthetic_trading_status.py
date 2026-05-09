"""Два события trading_status в Kafka → детектор выдаёт ``trading_status_changed``.

Запуск с хоста (Redpanda на localhost из compose):

    python scripts/push_synthetic_trading_status.py

Переменные (опционально): ``KAFKA_HOST_BOOTSTRAP_SERVERS``, ``KAFKA_RAW_TOPIC``,
``TEST_INSTRUMENT_ID`` (по умолчанию ``SBER_TQBR`` — должен быть в ``conf/instruments.yaml``).
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer

from tinvest_signal_engine.models import NormalizedEvent
from tinvest_signal_engine.serialization import kafka_json_serializer


def _event(instrument_id: str, ticker: str, class_code: str, alias: str, status: str) -> dict:
    now = datetime.now(timezone.utc)
    ev = NormalizedEvent(
        event_id=str(uuid4()),
        event_type="trading_status",
        instrument_id=instrument_id,
        ticker=ticker,
        class_code=class_code,
        alias=alias,
        figi="",
        uid="",
        lot=1,
        source_time=now,
        received_at=now,
        payload={"trading_status": status},
    )
    return ev.to_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instrument-id",
        default=os.getenv("TEST_INSTRUMENT_ID", "SBER_TQBR"),
        help="Ключ как в conf/instruments.yaml (TICKER_CLASS)",
    )
    args = parser.parse_args()
    iid = args.instrument_id
    parts = iid.rsplit("_", 1)
    if len(parts) != 2:
        raise SystemExit(f"instrument_id ожидается как TICKER_CLASS, получено: {iid!r}")
    ticker, class_code = parts[0], parts[1]

    brokers = os.getenv("KAFKA_HOST_BOOTSTRAP_SERVERS", "localhost:39092")
    topic = os.getenv("KAFKA_RAW_TOPIC", "marketdata.raw")
    alias = ticker.lower()

    producer = KafkaProducer(
        bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
        value_serializer=kafka_json_serializer,
    )
    try:
        s1 = _event(
            iid,
            ticker,
            class_code,
            alias,
            "SECURITY_TRADING_STATUS_NORMAL_TRADING",
        )
        producer.send(topic, key=iid.encode("utf-8"), value=s1)
        producer.flush()
        time.sleep(0.5)
        s2 = _event(
            iid,
            ticker,
            class_code,
            alias,
            "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING",
        )
        producer.send(topic, key=iid.encode("utf-8"), value=s2)
        producer.flush()
    finally:
        producer.close()

    print(f"Отправлено 2 события в {topic} @ {brokers} для {iid}.")
    print("Ожидается сигнал trading_status_changed → Postgres и GET /signals/recent")


if __name__ == "__main__":
    main()

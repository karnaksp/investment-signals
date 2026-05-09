"""End-to-end: produce a NormalizedEvent to Kafka, expect it in ClickHouse ``market_raw_events``."""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest
from kafka import KafkaProducer

from tinvest_signal_engine.serialization import kafka_json_serializer

pytestmark = pytest.mark.integration


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION", "").strip().lower() in {"1", "true", "yes", "on"}


def _ch_http_get(base: str, sql: str, timeout: float = 30.0) -> str:
    url = f"{base.rstrip('/')}/?{urlencode({'query': sql})}"
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8").strip()


@pytest.mark.skipif(not _integration_enabled(), reason="Set RUN_INTEGRATION=1 and stack up")
def test_produced_trade_event_lands_in_clickhouse() -> None:
    kafka_bootstrap = os.getenv(
        "KAFKA_HOST_BOOTSTRAP_SERVERS", "localhost:39092"
    )
    topic = os.getenv("KAFKA_RAW_TOPIC", "marketdata.raw")
    ch_base = os.getenv("CLICKHOUSE_HTTP_URL", "http://localhost:38123")

    event_id = f"itest-{uuid.uuid4()}"
    now = datetime.now(tz=timezone.utc).isoformat()
    payload = {
        "quantity": 10.0,
        "price": {"units": 100, "nano": 0},
        "direction": "TRADE_DIRECTION_BUY",
    }
    record = {
        "event_id": event_id,
        "event_type": "trade",
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "class_code": "TQBR",
        "alias": "sber",
        "figi": "BBG004730N88",
        "uid": "uid-sber",
        "lot": 1,
        "source_time": now,
        "received_at": now,
        "payload": payload,
    }

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap.split(","),
        value_serializer=kafka_json_serializer,
        linger_ms=5,
    )
    try:
        producer.send(topic, key=b"SBER_TQBR", value=record).get(timeout=30)
        producer.flush()
    finally:
        producer.close()

    deadline = time.monotonic() + 90.0
    count = "0"
    last_err: str | None = None
    while time.monotonic() < deadline:
        try:
            count = _ch_http_get(
                ch_base,
                f"SELECT count() FROM signal_engine.market_raw_events "
                f"WHERE event_id = '{event_id}'",
                timeout=10.0,
            )
            if count == "1":
                break
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_err = repr(exc)
        time.sleep(2.0)
    else:
        pytest.fail(
            f"ClickHouse did not observe event_id={event_id} (last count={count!r}, err={last_err})"
        )

    assert count == "1"

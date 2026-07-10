from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import pytest

from tinvest_signal_engine.adapters.clickhouse_reference_ticks import (
    ClickHouseReferenceTickStore,
)
from tinvest_signal_engine.adapters.kafka_reference_ticks import (
    ReferenceTickKafkaRuntime,
    build_reference_tick_consumer,
)
from tinvest_signal_engine.domain.reference_ticks import ReferenceTick


EVENT_ID = "fd56ea27-aeb3-47f1-b038-182f747f5aa2"
NOW_TEXT = "2026-07-10T09:30:00+00:00"


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return b""


def test_clickhouse_store_uses_parameterized_idempotent_insert(monkeypatch) -> None:
    captured = {}

    def fake_urlopen(request, *, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.clickhouse_reference_ticks.urlopen",
        fake_urlopen,
    )
    store = ClickHouseReferenceTickStore(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
    )
    store.persist(
        ReferenceTick(
            instrument_id="SBER_TQBR",
            event_at=datetime.fromisoformat(NOW_TEXT),
            received_at=datetime.fromisoformat(NOW_TEXT),
            event_id=UUID(EVENT_ID),
            source_kind="trade",
            trade_price=Decimal("312.123456789"),
            has_trade=True,
        )
    )

    request = captured["request"]
    query = parse_qs(urlparse(request.full_url).query)
    sql = request.data.decode("utf-8")
    assert "WHERE NOT EXISTS" in sql
    assert EVENT_ID not in sql
    assert query["param_event_id"] == [EVENT_ID]
    assert query["param_trade_price"] == ["312.123456789"]
    assert request.headers["X-clickhouse-key"] == "secret"


@dataclass
class _Message:
    value: object
    topic: str = "marketdata.raw"
    partition: int = 1
    offset: int = 7


@dataclass
class _Consumer:
    messages: list[_Message]
    commits: list[object] = field(default_factory=list)
    closed: bool = False

    def __iter__(self):
        return iter(self.messages)

    def commit(self, *, offsets) -> None:
        self.commits.append(offsets)

    def close(self) -> None:
        self.closed = True


@dataclass
class _Processor:
    failure: Exception | None = None
    events: list[object] = field(default_factory=list)

    def process(self, event) -> bool:
        self.events.append(event)
        if self.failure:
            raise self.failure
        return True


def _raw_trade() -> dict[str, object]:
    return {
        "event_id": EVENT_ID,
        "event_type": "trade",
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "class_code": "TQBR",
        "alias": "sber",
        "lot": 10,
        "source_time": NOW_TEXT,
        "received_at": NOW_TEXT,
        "payload": {"quantity": 1, "price": {"units": 312, "nano": 0}},
    }


def test_kafka_runtime_commits_only_after_persistence() -> None:
    consumer = _Consumer([_Message(_raw_trade())])
    processor = _Processor()

    ReferenceTickKafkaRuntime(
        consumer=consumer,
        processor=processor,  # type: ignore[arg-type]
    ).run()

    assert len(processor.events) == 1
    committed = next(iter(consumer.commits[0].values()))
    assert committed.offset == 8
    assert consumer.closed is True


def test_kafka_runtime_does_not_commit_transient_clickhouse_failure() -> None:
    consumer = _Consumer([_Message(_raw_trade())])

    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        ReferenceTickKafkaRuntime(
            consumer=consumer,
            processor=_Processor(RuntimeError("clickhouse unavailable")),  # type: ignore[arg-type]
        ).run()

    assert consumer.commits == []
    assert consumer.closed is True


def test_reference_consumer_has_independent_group_and_manual_commits(monkeypatch) -> None:
    captured = {}

    class StubConsumer:
        def __init__(self, *args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.kafka_reference_ticks.KafkaConsumer",
        StubConsumer,
    )

    build_reference_tick_consumer(
        topic="marketdata.raw",
        bootstrap_servers="redpanda:9092",
        group_id="reference-tick-writer-v1",
        auto_offset_reset="latest",
        value_format="json",
    )

    assert captured["kwargs"]["enable_auto_commit"] is False
    assert captured["kwargs"]["group_id"] == "reference-tick-writer-v1"

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import pytest

from tinvest_signal_engine.adapters.clickhouse_reference_ticks import (
    ClickHouseReferenceTickReader,
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
    def __init__(self, payload: bytes = b"") -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._payload


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


def test_clickhouse_store_batches_ticks_into_one_insert(monkeypatch) -> None:
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
    tick = ReferenceTick(
        instrument_id="SBER_TQBR",
        event_at=datetime.fromisoformat(NOW_TEXT),
        received_at=datetime.fromisoformat(NOW_TEXT),
        event_id=UUID(EVENT_ID),
        source_kind="trade",
        trade_price=Decimal("312.123456789"),
        has_trade=True,
    )

    store.persist_many((tick,))

    body = captured["request"].data.decode("utf-8")
    sql, row = body.split("FORMAT JSONEachRow\n", 1)
    assert "INSERT INTO market_reference_ticks" in sql
    assert "LEFT ANTI JOIN" not in sql
    assert EVENT_ID not in sql
    parsed_row = json.loads(row)
    assert parsed_row["event_id"] == EVENT_ID
    assert parsed_row["trade_price"] == "312.123456789"
    assert parsed_row["event_at"] == "2026-07-10 09:30:00.000000"
    assert parsed_row["received_at"] == "2026-07-10 09:30:00.000000"
    query = parse_qs(urlparse(captured["request"].full_url).query)
    assert query["database"] == ["signal_engine"]
    assert query["date_time_input_format"] == ["best_effort"]


def test_clickhouse_reader_loads_bounded_reference_ticks(monkeypatch) -> None:
    captured = {}
    payload = (
        b'{"instrument_id":"SBER_TQBR","event_at":"2026-07-10T09:30:00.123456789Z",'
        b'"received_at":"2026-07-10T09:30:00.223456789Z",'
        b'"event_id":"fd56ea27-aeb3-47f1-b038-182f747f5aa2",'
        b'"source_kind":"orderbook","bid_price":"312.100000000",'
        b'"ask_price":"312.300000000","last_price":"0","trade_price":"0",'
        b'"bid_quantity":10,"ask_quantity":12,"has_valid_book":1,'
        b'"has_last_price":0,"has_trade":0}\n'
    )

    def fake_urlopen(request, *, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return _Response(payload)

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.clickhouse_reference_ticks.urlopen",
        fake_urlopen,
    )
    start_at = datetime(2026, 7, 10, 9, 30, tzinfo=timezone.utc)
    end_at = datetime(2026, 7, 10, 9, 35, tzinfo=timezone.utc)

    ticks = ClickHouseReferenceTickReader(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
        limit=500,
    ).load(instrument_id="SBER_TQBR", start_at=start_at, end_at=end_at)

    request = captured["request"]
    query = parse_qs(urlparse(request.full_url).query)
    sql = request.data.decode("utf-8")
    assert "FORMAT JSONEachRow" in sql
    assert "market_reference_ticks" in sql
    assert EVENT_ID not in sql
    assert query["param_instrument_id"] == ["SBER_TQBR"]
    assert query["param_start_at"] == [start_at.isoformat()]
    assert query["param_end_at"] == [end_at.isoformat()]
    assert query["param_limit"] == ["500"]
    assert ticks == (
        ReferenceTick(
            instrument_id="SBER_TQBR",
            event_at=datetime(2026, 7, 10, 9, 30, 0, 123456, tzinfo=timezone.utc),
            received_at=datetime(2026, 7, 10, 9, 30, 0, 223456, tzinfo=timezone.utc),
            event_id=UUID(EVENT_ID),
            source_kind="orderbook",
            bid_price=Decimal("312.100000000"),
            ask_price=Decimal("312.300000000"),
            bid_quantity=10,
            ask_quantity=12,
            has_valid_book=True,
        ),
    )


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

    polled: bool = False

    def poll(self, *, timeout_ms: int, max_records: int):
        assert timeout_ms == 1_000
        assert max_records == 500
        if self.polled:
            raise KeyboardInterrupt
        self.polled = True
        return {"partition": self.messages}

    def commit(self, *, offsets) -> None:
        self.commits.append(offsets)

    def close(self) -> None:
        self.closed = True


@dataclass
class _Processor:
    failure: Exception | None = None
    events: list[object] = field(default_factory=list)

    def process_many(self, events) -> int:
        self.events.extend(events)
        if self.failure:
            raise self.failure
        return len(events)


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

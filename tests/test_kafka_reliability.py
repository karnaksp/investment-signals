from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from tinvest_signal_engine.adapters.kafka_reliability import (
    ReliableDetectorRuntime,
    build_raw_consumer,
)
from tinvest_signal_engine.config import RuntimeSettings


@dataclass
class FakeMessage:
    value: object
    topic: str = "marketdata.raw"
    partition: int = 2
    offset: int = 10


@dataclass
class FakeConsumer:
    messages: list[FakeMessage]
    log: list[str]
    commits: list[object] = field(default_factory=list)

    def __iter__(self):
        return iter(self.messages)

    def commit(self, *, offsets) -> None:
        self.log.append("commit")
        self.commits.append(offsets)

    def close(self) -> None:
        self.log.append("consumer_close")


@dataclass
class FakeProcessor:
    log: list[str]
    failure: Exception | None = None

    def process(self, event) -> None:
        self.log.append("process")
        if self.failure:
            raise self.failure


@dataclass
class FakePublisher:
    log: list[str]

    def close(self) -> None:
        self.log.append("signal_close")


@dataclass
class FakeDlq:
    log: list[str]

    def publish(self, message, **kwargs) -> None:
        self.log.append("dlq_publish")

    def close(self) -> None:
        self.log.append("dlq_close")


@dataclass
class FakeMetrics:
    log: list[str]

    def event_processed(self, **kwargs) -> None:
        pass

    def dead_lettered(self, **kwargs) -> None:
        self.log.append("dlq_metric")

    def offset_committed(self) -> None:
        self.log.append("commit_metric")

    def delivery_attempted(self, **kwargs) -> None:
        pass


def _valid_event() -> dict[str, object]:
    timestamp = datetime(2026, 7, 1, tzinfo=timezone.utc).isoformat()
    return {
        "event_id": "event-1",
        "event_type": "trade",
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "class_code": "TQBR",
        "alias": "sber",
        "figi": "figi",
        "uid": "uid",
        "lot": 10,
        "source_time": timestamp,
        "received_at": timestamp,
        "payload": {
            "quantity": 1,
            "price": {"units": 100, "nano": 0},
        },
    }


def _runtime(
    consumer: FakeConsumer,
    processor: FakeProcessor,
    log: list[str],
) -> ReliableDetectorRuntime:
    return ReliableDetectorRuntime(
        consumer=consumer,
        processor=processor,  # type: ignore[arg-type]
        signal_publisher=FakePublisher(log),  # type: ignore[arg-type]
        dlq_publisher=FakeDlq(log),  # type: ignore[arg-type]
        metrics=FakeMetrics(log),
        checkpoint=lambda: log.append("checkpoint"),
    )


def test_offset_commits_only_after_processing_and_checkpoint() -> None:
    log: list[str] = []
    consumer = FakeConsumer([FakeMessage(_valid_event())], log)

    _runtime(consumer, FakeProcessor(log), log).run()

    assert log[:4] == ["process", "checkpoint", "commit", "commit_metric"]
    committed = next(iter(consumer.commits[0].values()))
    assert committed.offset == 11


def test_transient_processing_failure_does_not_commit_offset() -> None:
    log: list[str] = []
    consumer = FakeConsumer([FakeMessage(_valid_event())], log)
    runtime = _runtime(
        consumer,
        FakeProcessor(log, RuntimeError("postgres unavailable")),
        log,
    )

    with pytest.raises(RuntimeError, match="postgres unavailable"):
        runtime.run()

    assert consumer.commits == []
    assert "checkpoint" not in log


def test_poison_payload_commits_only_after_dlq_ack() -> None:
    log: list[str] = []
    consumer = FakeConsumer([FakeMessage("not-a-mapping")], log)

    _runtime(consumer, FakeProcessor(log), log).run()

    assert log[:4] == [
        "dlq_publish",
        "dlq_metric",
        "commit",
        "commit_metric",
    ]
    assert "process" not in log


def test_consumer_disables_auto_commit(monkeypatch) -> None:
    captured = {}

    class StubConsumer:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.kafka_reliability.KafkaConsumer",
        StubConsumer,
    )
    settings = RuntimeSettings.from_env()

    build_raw_consumer(settings)

    assert captured["enable_auto_commit"] is False

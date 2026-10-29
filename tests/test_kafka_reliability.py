from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest
from kafka.errors import KafkaTimeoutError
from kafka.structs import TopicPartition

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
    seeks: list[tuple[TopicPartition, int]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._all_messages = list(self.messages)

    def assignment(self):
        return {TopicPartition(item.topic, item.partition) for item in self._all_messages}

    def seek(self, partition, offset) -> None:
        self.seeks.append((partition, offset))
        self.messages = [
            item for item in self.messages
            if (item.topic, item.partition) != (partition.topic, partition.partition)
        ] + [
            item for item in self._all_messages
            if (item.topic, item.partition) == (partition.topic, partition.partition)
            and item.offset >= offset
        ]

    def poll(self, **kwargs):
        self.log.append("poll")
        messages, self.messages = self.messages, []
        grouped: dict[tuple[str, int], list[FakeMessage]] = {}
        for message in messages:
            grouped.setdefault((message.topic, message.partition), []).append(message)
        return grouped

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

    def detector_batch_completed(self, **kwargs) -> None:
        self.log.append(f"batch_metric:{kwargs['outcome']}:{kwargs['message_count']}")

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

    assert _runtime(consumer, FakeProcessor(log), log).run_once() == 1

    assert log[1:5] == ["process", "checkpoint", "commit", "commit_metric"]
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
        runtime.run_once()

    assert consumer.commits == []
    assert "checkpoint" not in log


def test_poison_payload_commits_only_after_dlq_ack() -> None:
    log: list[str] = []
    consumer = FakeConsumer([FakeMessage("not-a-mapping")], log)

    _runtime(consumer, FakeProcessor(log), log).run_once()

    assert log[1:6] == [
        "dlq_publish",
        "dlq_metric",
        "checkpoint",
        "commit",
        "commit_metric",
    ]
    assert "process" not in log


def test_batch_uses_one_checkpoint_and_commit_for_many_events() -> None:
    log: list[str] = []
    messages = [
        FakeMessage({**_valid_event(), "event_id": f"event-{offset}"}, offset=offset)
        for offset in range(500)
    ]
    consumer = FakeConsumer(messages, log)

    assert _runtime(consumer, FakeProcessor(log), log).run_once() == 500

    assert log.count("process") == 500
    assert log.count("checkpoint") == 1
    assert log.count("commit") == 1
    assert log.count("commit_metric") == 1
    assert "batch_metric:completed:500" in log
    committed = next(iter(consumer.commits[0].values()))
    assert committed.offset == 500


def test_partial_failure_commits_only_successful_contiguous_prefix() -> None:
    log: list[str] = []
    messages = [
        FakeMessage({**_valid_event(), "event_id": f"event-{offset}"}, offset=offset)
        for offset in range(3)
    ]
    consumer = FakeConsumer(messages, log)

    class FailSecondProcessor:
        calls = 0

        def process(self, event) -> None:
            self.calls += 1
            log.append("process")
            if self.calls == 2:
                raise RuntimeError("postgres unavailable")

    runtime = _runtime(consumer, FakeProcessor(log), log)
    runtime._processor = FailSecondProcessor()  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="postgres unavailable"):
        runtime.run_once()

    assert log.count("process") == 2
    assert log.count("checkpoint") == 1
    committed = next(iter(consumer.commits[0].values()))
    assert committed.offset == 1
    assert "batch_metric:partial_failure:1" in log


def test_batch_commit_refuses_offset_reordering() -> None:
    log: list[str] = []
    consumer = FakeConsumer([], log)
    runtime = _runtime(consumer, FakeProcessor(log), log)

    with pytest.raises(RuntimeError, match="non-increasing"):
        runtime._commit_batch(
            [
                FakeMessage(_valid_event(), offset=12),
                FakeMessage(_valid_event(), offset=10),
            ]
        )

    assert consumer.commits == []


def test_commit_failure_replays_through_idempotent_processor() -> None:
    log: list[str] = []
    messages = [
        FakeMessage(
            {**_valid_event(), "event_id": f"event-{offset}"},
            offset=offset,
        )
        for offset in range(3)
    ]

    @dataclass
    class IdempotentProcessor:
        stored: set[str] = field(default_factory=set)
        stored_calls: int = 0
        replay_calls: int = 0

        def process(self, event) -> None:
            if event.event_id in self.stored:
                self.replay_calls += 1
                return
            self.stored.add(event.event_id)
            self.stored_calls += 1

    class CommitFailingConsumer(FakeConsumer):
        def commit(self, *, offsets) -> None:
            raise RuntimeError("coordinator unavailable")

    processor = IdempotentProcessor()
    first = _runtime(
        CommitFailingConsumer(list(messages), log),
        FakeProcessor(log),
        log,
    )
    first._processor = processor  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="coordinator unavailable"):
        first.run_once()

    second_consumer = FakeConsumer(list(messages), log)
    second = _runtime(second_consumer, FakeProcessor(log), log)
    second._processor = processor  # type: ignore[assignment]
    assert second.run_once() == 3

    assert processor.stored_calls == 3
    assert processor.replay_calls == 3
    committed = next(iter(second_consumer.commits[0].values()))
    assert committed.offset == 3


@pytest.mark.parametrize("failure_stage", ["processing", "commit"])
def test_kafka_timeout_replays_polled_batch_without_skipping_suffix(failure_stage) -> None:
    log: list[str] = []
    messages = [
        FakeMessage({**_valid_event(), "event_id": f"event-{offset}"}, offset=offset)
        for offset in range(3)
    ]

    class RetryConsumer(FakeConsumer):
        failed = False

        def poll(self, **kwargs):
            if not self.messages:
                raise KeyboardInterrupt
            return super().poll(**kwargs)

        def commit(self, *, offsets):
            if failure_stage == "commit" and not self.failed:
                self.failed = True
                raise KafkaTimeoutError("commit result unknown")
            super().commit(offsets=offsets)

    class RetryProcessor:
        def __init__(self):
            self.stored = set()
            self.replayed = []
            self.failed = False

        def process(self, event):
            if failure_stage == "processing" and event.offset_id == 1 and not self.failed:
                self.failed = True
                raise KafkaTimeoutError("publish metadata timed out")
            if event.event_id in self.stored:
                self.replayed.append(event.event_id)
            self.stored.add(event.event_id)

    consumer = RetryConsumer(messages, log)
    processor = RetryProcessor()
    runtime = _runtime(consumer, FakeProcessor(log), log)
    runtime._processor = processor

    runtime.run()

    assert processor.stored == {"event-0", "event-1", "event-2"}
    assert processor.replayed == (["event-0"] if failure_stage == "processing" else
                                  ["event-0", "event-1", "event-2"])
    assert consumer.seeks == [(TopicPartition("marketdata.raw", 2), 0)]
    committed = next(iter(consumer.commits[-1].values()))
    assert committed.offset == 3


def test_rewind_failure_stops_worker_instead_of_skipping_records() -> None:
    log: list[str] = []

    class SeekFailingConsumer(FakeConsumer):
        def seek(self, partition, offset):
            raise KafkaTimeoutError("cannot rewind")

    consumer = SeekFailingConsumer([FakeMessage(_valid_event())], log)
    runtime = _runtime(consumer, FakeProcessor(log, KafkaTimeoutError("publish failed")), log)

    with pytest.raises(RuntimeError, match="rewind failed; restart required"):
        runtime.run()

    assert log.count("poll") == 1
    assert consumer.commits == []
    assert log[-3:] == ["signal_close", "dlq_close", "consumer_close"]


def test_rewind_skips_partitions_revoked_during_failed_operation() -> None:
    log: list[str] = []

    class RevokedConsumer(FakeConsumer):
        def assignment(self):
            return set()

    consumer = RevokedConsumer([FakeMessage(_valid_event())], log)
    runtime = _runtime(consumer, FakeProcessor(log, KafkaTimeoutError("rebalanced")), log)

    with pytest.raises(KafkaTimeoutError, match="rebalanced"):
        runtime.run_once()

    assert consumer.seeks == []


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
    assert captured["max_poll_records"] == 500
    assert captured["auto_offset_reset"] == "latest"


def test_first_boot_warmup_has_an_explicit_setting(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_FIRST_BOOT_WARMUP_AGE_SECONDS", "123")

    settings = RuntimeSettings.from_env()

    assert settings.kafka_first_boot_warmup_age_seconds == 123

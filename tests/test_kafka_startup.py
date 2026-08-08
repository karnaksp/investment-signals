from dataclasses import dataclass

from tinvest_signal_engine.adapters.kafka_startup import seek_consumer_to_recent


@dataclass(frozen=True)
class _Offset:
    offset: int


@dataclass(frozen=True)
class _Record:
    offset: int


class _Consumer:
    def __init__(
        self,
        *,
        committed: int | None,
        recent: int | None,
        polled_offset: int | None = None,
    ):
        self._partition = "p0"
        self._committed = committed
        self._recent = recent
        self._polled_offset = polled_offset
        self.seeks: list[tuple[object, int]] = []
        self.timestamp_queries: list[dict[object, int]] = []

    def poll(self, **_kwargs):
        if self._polled_offset is not None:
            return {self._partition: [_Record(self._polled_offset)]}
        return {}

    def assignment(self):
        return {self._partition}

    def offsets_for_times(self, values):
        self.timestamp_queries.append(values)
        return {
            self._partition: (
                _Offset(self._recent) if self._recent is not None else None
            )
        }

    def committed(self, _partition):
        return self._committed

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))


def test_committed_backlog_is_preserved_after_a_long_outage() -> None:
    consumer = _Consumer(committed=100, recent=900, polled_offset=100)

    positioned = seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=900,
        clock_ms=lambda: 200_000_000,
    )

    assert positioned == 0
    assert consumer.seeks == [("p0", 100)]
    assert consumer.timestamp_queries == []


def test_recent_commit_is_preserved_after_assignment_poll() -> None:
    consumer = _Consumer(committed=950, recent=900)

    advanced = seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=900,
        clock_ms=lambda: 2_000_000,
    )

    assert advanced == 0
    assert consumer.seeks == [("p0", 950)]
    assert consumer.timestamp_queries == []


def test_first_boot_uses_recent_warmup_when_timestamp_offset_exists() -> None:
    consumer = _Consumer(committed=None, recent=900)

    positioned = seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=900,
        clock_ms=lambda: 2_000_000,
    )

    assert positioned == 1
    assert consumer.seeks == [("p0", 900)]
    assert consumer.timestamp_queries == [{"p0": 1_100_000}]


def test_no_recent_record_does_not_seek_uncommitted_partition_to_end() -> None:
    consumer = _Consumer(committed=None, recent=None)

    positioned = seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=900,
        clock_ms=lambda: 2_000_000,
    )

    assert positioned == 0
    assert consumer.seeks == []
    assert consumer.timestamp_queries == [{"p0": 1_100_000}]


def test_assignment_poll_record_is_not_discarded_when_lookup_has_no_record() -> None:
    consumer = _Consumer(committed=None, recent=None, polled_offset=42)

    positioned = seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=900,
        clock_ms=lambda: 2_000_000,
    )

    assert positioned == 0
    assert consumer.seeks == [("p0", 42)]


def test_disabled_first_boot_warmup_preserves_native_offset_reset() -> None:
    consumer = _Consumer(committed=None, recent=900)

    positioned = seek_consumer_to_recent(
        consumer,
        maximum_age_seconds=0,
        clock_ms=lambda: 2_000_000,
    )

    assert positioned == 0
    assert consumer.seeks == []
    assert consumer.timestamp_queries == []

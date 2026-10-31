"""Collect ruble-pair reports independently from overseas commodity reports."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.asset_positions import AssetPositionReport


@dataclass(frozen=True)
class RublePositionBatch:
    reports: tuple[AssetPositionReport, ...]
    unavailable_pairs: tuple[str, ...] = ()


class RublePositionSource(Protocol):
    def load(self, *, as_of: datetime) -> RublePositionBatch: ...


class AssetPositionPublisher(Protocol):
    def publish_reports(self, reports: tuple[AssetPositionReport, ...], *, generated_at: datetime) -> bool: ...


@dataclass(frozen=True)
class RublePositionReceipt:
    status: str
    pairs: tuple[str, ...] = ()
    unavailable_pairs: tuple[str, ...] = ()


class CollectRublePositions:
    def __init__(self, *, source: RublePositionSource, publisher: AssetPositionPublisher):
        self._source, self._publisher = source, publisher

    def execute(self, *, as_of: datetime) -> RublePositionReceipt:
        if as_of.utcoffset() is None:
            raise ValueError("ruble collection timestamp must be aware")
        batch = self._source.load(as_of=as_of)
        if not batch.reports:
            return RublePositionReceipt("unavailable", unavailable_pairs=batch.unavailable_pairs)
        queued = self._publisher.publish_reports(batch.reports, generated_at=as_of)
        return RublePositionReceipt("queued" if queued else "already_queued",
                                    tuple(r.commodity for r in batch.reports), batch.unavailable_pairs)

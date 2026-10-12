"""Application use case for publishing eligible bond convergence observations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from tinvest_signal_engine.domain.bond_convergence import (
    BondConvergenceSnapshot,
    evaluate_bond_convergence,
)


class BondConvergenceSource(Protocol):
    def load_snapshots(self) -> Sequence[BondConvergenceSnapshot]: ...


class BondConvergencePublisher(Protocol):
    def publish(self, snapshot: BondConvergenceSnapshot) -> None: ...


@dataclass(frozen=True)
class BondConvergenceScanReceipt:
    inspected: int
    published: int
    rejected: int


class ScanBondConvergence:
    def __init__(
        self,
        *,
        source: BondConvergenceSource,
        publisher: BondConvergencePublisher,
    ) -> None:
        self._source = source
        self._publisher = publisher

    def execute(self) -> BondConvergenceScanReceipt:
        snapshots = tuple(self._source.load_snapshots())
        published = 0
        for snapshot in snapshots:
            if not evaluate_bond_convergence(snapshot).eligible:
                continue
            self._publisher.publish(snapshot)
            published += 1
        return BondConvergenceScanReceipt(
            inspected=len(snapshots),
            published=published,
            rejected=len(snapshots) - published,
        )

"""Use case and application-owned ports for futures-universe refreshes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.futures_universe import (
    FutureContract,
    FutureWatchSpec,
    ResolvedFuture,
    select_futures_universe,
)


class FuturesCatalog(Protocol):
    def list_contracts(
        self,
        specs: tuple[FutureWatchSpec, ...],
        *,
        now: datetime,
    ) -> tuple[FutureContract, ...]: ...


class FuturesUniversePublisher(Protocol):
    def publish(self, resolved: tuple[ResolvedFuture, ...]) -> bool: ...


@dataclass(frozen=True)
class FuturesRefreshReceipt:
    resolved: tuple[ResolvedFuture, ...]
    changed: bool


def refresh_futures_universe(
    *,
    catalog: FuturesCatalog,
    publisher: FuturesUniversePublisher,
    specs: tuple[FutureWatchSpec, ...],
    now: datetime,
) -> FuturesRefreshReceipt:
    contracts = catalog.list_contracts(specs, now=now)
    resolved = select_futures_universe(contracts, specs, now=now)
    return FuturesRefreshReceipt(
        resolved=resolved,
        changed=publisher.publish(resolved),
    )

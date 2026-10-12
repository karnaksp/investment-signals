"""Pure domain rules for rolling a configured futures universe."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class FutureWatchSpec:
    alias: str
    basic_asset: str
    roll_before_expiry_days: int = 5
    minimum_two_sided_lots: int = 1
    require_two_sided_order_book: bool = True
    max_candidates: int = 4
    ticker_pattern: str | None = None
    require_name_substrings: tuple[str, ...] = ()
    exclude_name_substrings: tuple[str, ...] = ()


@dataclass(frozen=True)
class FutureContract:
    ticker: str
    class_code: str
    alias: str
    basic_asset: str
    name: str
    figi: str
    uid: str
    expiration_at: datetime
    api_trade_available: bool
    buy_available: bool
    sell_available: bool
    bid_lots: int = 0
    ask_lots: int = 0

    @property
    def two_sided_lots(self) -> int:
        return min(self.bid_lots, self.ask_lots)


@dataclass(frozen=True)
class ResolvedFuture:
    alias: str
    ticker: str
    class_code: str
    basic_asset: str
    name: str
    figi: str
    uid: str
    expiration_at: datetime
    two_sided_lots: int


class UnresolvedFuturesError(RuntimeError):
    """Raised when a complete configured universe cannot be resolved safely."""


def matches_watch_spec(contract: FutureContract, spec: FutureWatchSpec) -> bool:
    if contract.class_code != "SPBFUT" or contract.basic_asset != spec.basic_asset:
        return False
    if spec.ticker_pattern and re.fullmatch(spec.ticker_pattern, contract.ticker) is None:
        return False
    if any(value not in contract.name for value in spec.require_name_substrings):
        return False
    return not any(value in contract.name for value in spec.exclude_name_substrings)


def select_futures_universe(
    contracts: tuple[FutureContract, ...],
    specs: tuple[FutureWatchSpec, ...],
    *,
    now: datetime,
) -> tuple[ResolvedFuture, ...]:
    """Select one liquid, non-expiring contract for every configured alias.

    Selection is fail-closed: if any configured underlying has no eligible
    contract, callers must keep the previous published universe unchanged.
    """

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    aliases = [spec.alias for spec in specs]
    if len(set(aliases)) != len(aliases):
        raise ValueError("future watch aliases must be unique")

    resolved: list[ResolvedFuture] = []
    unresolved: list[str] = []
    for spec in specs:
        cutoff = now + timedelta(days=spec.roll_before_expiry_days)
        candidates = [
            contract
            for contract in contracts
            if matches_watch_spec(contract, spec)
            and contract.api_trade_available
            and contract.buy_available
            and contract.sell_available
            and contract.expiration_at > cutoff
            and (
                not spec.require_two_sided_order_book
                or contract.two_sided_lots >= spec.minimum_two_sided_lots
            )
        ]
        if not candidates:
            unresolved.append(spec.alias)
            continue
        selected = min(
            candidates,
            key=lambda item: (
                item.expiration_at,
                -item.two_sided_lots,
                item.ticker,
            ),
        )
        resolved.append(
            ResolvedFuture(
                alias=spec.alias,
                ticker=selected.ticker,
                class_code=selected.class_code,
                basic_asset=selected.basic_asset,
                name=selected.name,
                figi=selected.figi,
                uid=selected.uid,
                expiration_at=selected.expiration_at,
                two_sided_lots=selected.two_sided_lots,
            )
        )

    if unresolved:
        raise UnresolvedFuturesError(
            "no liquid rollover candidate for: " + ", ".join(unresolved)
        )
    return tuple(resolved)

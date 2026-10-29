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
    chain_depth: int = 1
    include_expiring_contract: bool = False


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
    chain_id: str = ""
    basic_asset_size: float = 0.0
    min_price_increment: float = 0.0
    min_price_increment_amount: float = 0.0
    settlement_currency: str = ""
    settlement_type: str = ""

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
    chain_id: str = ""
    basic_asset_size: float = 0.0
    min_price_increment: float = 0.0
    min_price_increment_amount: float = 0.0
    settlement_currency: str = ""
    settlement_type: str = ""
    maturity_rank: int = 1


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
    """Select a liquid primary contract and optional same-product maturities.

    Primary selection is fail-closed and retains the original rollover alias.
    Extra maturities may be illiquid: consumers gate signals on live quotes.
    """

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    aliases = [spec.alias for spec in specs]
    if len(set(aliases)) != len(aliases):
        raise ValueError("future watch aliases must be unique")

    resolved: list[ResolvedFuture] = []
    unresolved: list[str] = []
    for spec in specs:
        if not 1 <= spec.chain_depth <= 12:
            raise ValueError("future chain_depth must be between 1 and 12")
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
        chain_candidates = sorted(
            (
                contract
                for contract in contracts
                if matches_watch_spec(contract, spec)
                and contract.api_trade_available
                and contract.buy_available
                and contract.sell_available
                and contract.expiration_at > (
                    now if spec.include_expiring_contract else cutoff
                )
                and _same_contract_spec(selected, contract)
            ),
            key=lambda item: (
                item.expiration_at,
                -item.two_sided_lots,
                item.ticker,
            ),
        )
        # One contract per expiry; duplicate listings are not distinct tenors.
        by_expiry: dict[datetime, FutureContract] = {}
        for contract in chain_candidates:
            by_expiry.setdefault(contract.expiration_at, contract)
        by_expiry[selected.expiration_at] = selected
        extras = [
            contract
            for contract in by_expiry.values()
            if contract.uid != selected.uid
        ][: spec.chain_depth - 1]
        chronological = sorted(
            (selected, *extras), key=lambda item: (item.expiration_at, item.ticker)
        )
        for contract in (selected, *extras):
            resolved.append(
                ResolvedFuture(
                    alias=(
                        spec.alias
                        if contract.uid == selected.uid
                        else f"{spec.alias}__{contract.ticker}"
                    ),
                    ticker=contract.ticker,
                    class_code=contract.class_code,
                    basic_asset=contract.basic_asset,
                    name=contract.name,
                    figi=contract.figi,
                    uid=contract.uid,
                    expiration_at=contract.expiration_at,
                    two_sided_lots=contract.two_sided_lots,
                    chain_id=spec.alias,
                    basic_asset_size=contract.basic_asset_size,
                    min_price_increment=contract.min_price_increment,
                    min_price_increment_amount=contract.min_price_increment_amount,
                    settlement_currency=contract.settlement_currency,
                    settlement_type=contract.settlement_type,
                    maturity_rank=chronological.index(contract) + 1,
                )
            )

    if unresolved:
        raise UnresolvedFuturesError(
            "no liquid rollover candidate for: " + ", ".join(unresolved)
        )
    if len({item.alias.lower() for item in resolved}) != len(resolved):
        raise ValueError("resolved futures aliases must be unique")
    if len({item.uid for item in resolved}) != len(resolved):
        raise ValueError("futures watch specifications select overlapping contracts")
    return tuple(resolved)


def _same_contract_spec(primary: FutureContract, other: FutureContract) -> bool:
    """Keep mini/full contracts and settlement variants out of one chain."""
    primary_family = re.fullmatch(r"(.+?)[FGHJKMNQUVXZ]\d{1,2}", primary.ticker)
    other_family = re.fullmatch(r"(.+?)[FGHJKMNQUVXZ]\d{1,2}", other.ticker)
    if (
        primary_family is not None
        and other_family is not None
        and primary_family.group(1) != other_family.group(1)
    ):
        return False
    if (
        primary.basic_asset_size > 0
        and other.basic_asset_size > 0
        and primary.basic_asset_size != other.basic_asset_size
    ):
        return False
    for field in ("settlement_currency", "settlement_type"):
        left = getattr(primary, field).strip().lower()
        right = getattr(other, field).strip().lower()
        if left and right and left != right:
            return False
    return True

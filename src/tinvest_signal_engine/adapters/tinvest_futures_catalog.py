"""T-Invest adapter for futures discovery and order-book liquidity checks."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.exceptions import RequestError

from tinvest_signal_engine.domain.futures_universe import (
    FutureContract,
    FutureWatchSpec,
    matches_watch_spec,
)


logger = logging.getLogger(__name__)


def _aware_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _base_contract(raw) -> FutureContract:
    return FutureContract(
        ticker=str(raw.ticker or "").strip(),
        class_code=str(raw.class_code or "").strip(),
        alias="",
        basic_asset=str(raw.basic_asset or "").strip(),
        name=str(raw.name or "").strip(),
        figi=str(raw.figi or "").strip(),
        uid=str(raw.uid or "").strip(),
        expiration_at=_aware_utc(raw.expiration_date),
        api_trade_available=bool(raw.api_trade_available_flag),
        buy_available=bool(raw.buy_available_flag),
        sell_available=bool(raw.sell_available_flag),
    )


class TInvestFuturesCatalog:
    def __init__(
        self,
        token: str,
        *,
        sandbox: bool = False,
        app_name: str | None = None,
    ) -> None:
        self._token = token
        self._target = INVEST_GRPC_API_SANDBOX if sandbox else None
        self._app_name = app_name

    def list_contracts(
        self,
        specs: tuple[FutureWatchSpec, ...],
        *,
        now: datetime,
    ) -> tuple[FutureContract, ...]:
        with Client(
            self._token,
            target=self._target,
            app_name=self._app_name,
        ) as client:
            raw_futures = tuple(client.instruments.futures().instruments)
            base_by_uid = {
                contract.uid: contract
                for contract in (_base_contract(raw) for raw in raw_futures)
                if contract.uid
            }
            inspect_uids: set[str] = set()
            for spec in specs:
                matching = sorted(
                    (
                        item
                        for item in base_by_uid.values()
                        if item.expiration_at > now
                        and matches_watch_spec(item, spec)
                        and item.api_trade_available
                    ),
                    key=lambda item: (item.expiration_at, item.ticker),
                )
                inspect_uids.update(
                    item.uid for item in matching[: max(1, spec.max_candidates)]
                )

            result: list[FutureContract] = []
            for uid, contract in base_by_uid.items():
                bid_lots = 0
                ask_lots = 0
                if uid in inspect_uids:
                    try:
                        order_book = client.market_data.get_order_book(
                            figi=contract.figi,
                            depth=1,
                        )
                        bid_lots = sum(int(order.quantity) for order in order_book.bids)
                        ask_lots = sum(int(order.quantity) for order in order_book.asks)
                    except RequestError as exc:
                        logger.warning(
                            "Order book unavailable for %s (%s): %s",
                            contract.ticker,
                            uid,
                            exc,
                        )
                result.append(
                    FutureContract(
                        **{
                            **contract.__dict__,
                            "bid_lots": bid_lots,
                            "ask_lots": ask_lots,
                        }
                    )
                )
            return tuple(result)

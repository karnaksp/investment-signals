from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.futures_universe_yaml import (
    AtomicYamlFuturesUniversePublisher,
    load_future_watch_specs,
)
from tinvest_signal_engine.domain.futures_universe import (
    FutureContract,
    FutureWatchSpec,
    ResolvedFuture,
    UnresolvedFuturesError,
    select_futures_universe,
)


NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)


def _contract(
    ticker: str,
    *,
    expires_in_days: int,
    bid_lots: int = 10,
    ask_lots: int = 10,
    available: bool = True,
) -> FutureContract:
    return FutureContract(
        ticker=ticker,
        class_code="SPBFUT",
        alias="",
        basic_asset="USD/RUB",
        name=f"USD/RUB {ticker}",
        figi=f"figi-{ticker}",
        uid=f"uid-{ticker}",
        expiration_at=NOW + timedelta(days=expires_in_days),
        api_trade_available=available,
        buy_available=available,
        sell_available=available,
        bid_lots=bid_lots,
        ask_lots=ask_lots,
    )


def test_rolls_to_next_liquid_contract_before_expiry() -> None:
    spec = FutureWatchSpec(
        alias="usd_rub",
        basic_asset="USD/RUB",
        roll_before_expiry_days=5,
        ticker_pattern=r"^Si[FGHJKMNQUVXZ]\d$",
    )
    selected = select_futures_universe(
        (
            _contract("SiU6", expires_in_days=3),
            _contract("SiZ6", expires_in_days=80, bid_lots=20, ask_lots=15),
            _contract("SiH7", expires_in_days=170),
        ),
        (spec,),
        now=NOW,
    )
    assert selected[0].ticker == "SiZ6"
    assert selected[0].two_sided_lots == 15


def test_keeps_previous_universe_when_no_liquid_candidate_exists() -> None:
    spec = FutureWatchSpec(alias="usd_rub", basic_asset="USD/RUB")
    with pytest.raises(UnresolvedFuturesError, match="usd_rub"):
        select_futures_universe(
            (_contract("SiZ6", expires_in_days=80, ask_lots=0),),
            (spec,),
            now=NOW,
        )


def test_yaml_publisher_is_atomic_and_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "instruments.yaml"
    path.write_text(
        "instruments:\n"
        "  # >>> FUTURES_NEAREST_BEGIN old\n"
        "  - ticker: OLD\n"
        "  # <<< FUTURES_NEAREST_END\n",
        encoding="utf-8",
    )
    resolved = (
        ResolvedFuture(
            alias="usd_rub",
            ticker="SiZ6",
            class_code="SPBFUT",
            basic_asset="USD/RUB",
            name="USD/RUB-12.26",
            figi="figi",
            uid="uid",
            expiration_at=NOW + timedelta(days=80),
            two_sided_lots=10,
        ),
    )
    publisher = AtomicYamlFuturesUniversePublisher(path)
    assert publisher.publish(resolved) is True
    assert "ticker: SiZ6" in path.read_text(encoding="utf-8")
    assert publisher.publish(resolved) is False


def test_yaml_publisher_bootstraps_managed_block_when_absent(tmp_path: Path) -> None:
    path = tmp_path / "instruments.yaml"
    path.write_text("instruments:\n  - ticker: SBER\n", encoding="utf-8")
    resolved = (
        ResolvedFuture(
            alias="usd_rub",
            ticker="SiZ6",
            class_code="SPBFUT",
            basic_asset="USD/RUB",
            name="USD/RUB-12.26",
            figi="figi",
            uid="uid",
            expiration_at=NOW + timedelta(days=80),
            two_sided_lots=10,
        ),
    )
    assert AtomicYamlFuturesUniversePublisher(path).publish(resolved) is True
    rendered = path.read_text(encoding="utf-8")
    assert "ticker: SBER" in rendered
    assert "# >>> FUTURES_NEAREST_BEGIN" in rendered
    assert "ticker: SiZ6" in rendered


def test_repository_watchlist_is_valid() -> None:
    path = Path(__file__).resolve().parents[1] / "conf" / "futures_universe.yaml"
    specs = load_future_watch_specs(path)
    assert len(specs) >= 10
    assert len({item.alias for item in specs}) == len(specs)

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from tinvest_signal_engine.adapters.futures_universe_yaml import (
    AtomicYamlFuturesUniversePublisher,
    load_future_watch_specs,
    render_futures_block,
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


def test_three_maturity_chain_retains_expiring_contract_and_primary_alias() -> None:
    spec = FutureWatchSpec(
        alias="usd_rub",
        basic_asset="USD/RUB",
        chain_depth=3,
        include_expiring_contract=True,
    )
    selected = select_futures_universe(
        (
            _contract("SiU6", expires_in_days=3),
            _contract("SiZ6", expires_in_days=80),
            _contract("SiH7", expires_in_days=170, bid_lots=0, ask_lots=0),
            _contract("SiM7", expires_in_days=260),
        ),
        (spec,),
        now=NOW,
    )

    assert [item.ticker for item in selected] == ["SiZ6", "SiU6", "SiH7"]
    assert [item.alias for item in selected] == [
        "usd_rub", "usd_rub__SiU6", "usd_rub__SiH7"
    ]
    assert [item.maturity_rank for item in selected] == [2, 1, 3]
    assert {item.chain_id for item in selected} == {"usd_rub"}


def test_chain_excludes_expired_and_unavailable_contracts() -> None:
    spec = FutureWatchSpec(
        alias="usd_rub",
        basic_asset="USD/RUB",
        chain_depth=3,
        include_expiring_contract=True,
    )
    selected = select_futures_universe(
        (
            _contract("SiU6", expires_in_days=-1),
            _contract("SiV6", expires_in_days=0),
            _contract("SiX6", expires_in_days=30, available=False),
            _contract("SiZ6", expires_in_days=80),
        ),
        (spec,),
        now=NOW,
    )
    assert [item.ticker for item in selected] == ["SiZ6"]


def test_chain_can_have_fewer_than_requested_maturities() -> None:
    selected = select_futures_universe(
        (_contract("SiZ6", expires_in_days=80),),
        (FutureWatchSpec(alias="usd_rub", basic_asset="USD/RUB", chain_depth=3),),
        now=NOW,
    )
    assert len(selected) == 1


def test_chain_does_not_mix_mini_or_settlement_variants() -> None:
    primary = replace(
        _contract("SiZ6", expires_in_days=80),
        basic_asset_size=1000.0,
        settlement_currency="rub",
        settlement_type="DELIVERY_TYPE_CASH",
    )
    selected = select_futures_universe(
        (
            primary,
            replace(primary, ticker="SMH7", uid="mini", expiration_at=NOW + timedelta(days=170)),
            replace(primary, ticker="SiH7", uid="small", basic_asset_size=1.0, expiration_at=NOW + timedelta(days=170)),
            replace(primary, ticker="SiM7", uid="foreign", settlement_currency="usd", expiration_at=NOW + timedelta(days=260)),
            replace(primary, ticker="SiU7", uid="delivery", settlement_type="DELIVERY_TYPE_PHYSICAL", expiration_at=NOW + timedelta(days=350)),
        ),
        (FutureWatchSpec(alias="usd_rub", basic_asset="USD/RUB", chain_depth=3),),
        now=NOW,
    )
    assert [item.ticker for item in selected] == ["SiZ6"]


@pytest.mark.parametrize("depth", [0, -1, 13])
def test_chain_rejects_invalid_depth(depth: int) -> None:
    with pytest.raises(ValueError, match="chain_depth"):
        select_futures_universe(
            (_contract("SiZ6", expires_in_days=80),),
            (FutureWatchSpec(alias="usd_rub", basic_asset="USD/RUB", chain_depth=depth),),
            now=NOW,
        )


def test_metadata_is_published_without_invented_monetary_values() -> None:
    selected = select_futures_universe(
        (
            replace(
                _contract("SiZ6", expires_in_days=80),
                basic_asset_size=1000.0,
                min_price_increment=1.0,
                min_price_increment_amount=1.0,
                settlement_currency="rub",
                settlement_type="cash",
            ),
        ),
        (FutureWatchSpec(alias="usd_rub", basic_asset="USD/RUB"),),
        now=NOW,
    )
    parsed = yaml.safe_load("instruments:\n" + render_futures_block(selected))
    metadata = parsed["instruments"][0]["futures_contract"]
    assert metadata == {
        "chain_id": "usd_rub",
        "basic_asset": "USD/RUB",
        "expiration_at": (NOW + timedelta(days=80)).isoformat(),
        "basic_asset_size": 1000.0,
        "min_price_increment": 1.0,
        "min_price_increment_amount": 1.0,
        "settlement_currency": "rub",
        "settlement_type": "cash",
        "maturity_rank": 1,
    }


def test_chain_settings_load_from_defaults_and_override_per_family(tmp_path: Path) -> None:
    path = tmp_path / "futures.yaml"
    path.write_text(
        "version: 1\ndefaults:\n  chain_depth: 3\n  include_expiring_contract: true\n"
        "futures:\n  - alias: usd_rub\n    basic_asset: USD/RUB\n"
        "  - alias: eur_rub\n    basic_asset: EUR/RUB\n    chain_depth: 2\n",
        encoding="utf-8",
    )
    first, second = load_future_watch_specs(path)
    assert first.chain_depth == 3
    assert second.chain_depth == 2
    assert first.include_expiring_contract is True
    assert second.include_expiring_contract is True

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tinvest_signal_engine.adapters.tinvest_futures_catalog import _base_contract


def _raw(**overrides):
    values = dict(
        ticker="SiZ6", class_code="SPBFUT", basic_asset="USD/RUB",
        name="USD/RUB-12.26", figi="figi", uid="uid",
        expiration_date=datetime(2026, 12, 17, tzinfo=timezone.utc),
        api_trade_available_flag=True, buy_available_flag=True,
        sell_available_flag=True,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _quotation(units=0, nano=0):
    return SimpleNamespace(units=units, nano=nano)


def test_catalog_maps_future_contract_units_without_fx_assumptions() -> None:
    contract = _base_contract(_raw(
        basic_asset_size=_quotation(1000),
        min_price_increment=_quotation(0, 10_000_000),
        min_price_increment_amount=_quotation(2, 500_000_000),
        currency="RUB", futures_type="cash",
    ))
    assert contract.basic_asset_size == 1000.0
    assert contract.min_price_increment == 0.01
    assert contract.min_price_increment_amount == 2.5
    assert contract.settlement_currency == "rub"
    assert contract.settlement_type == "cash"


def test_catalog_missing_monetary_metadata_stays_explicitly_unavailable() -> None:
    contract = _base_contract(_raw())
    assert contract.basic_asset_size == 0.0
    assert contract.min_price_increment == 0.0
    assert contract.min_price_increment_amount == 0.0
    assert contract.settlement_currency == ""
    assert contract.settlement_type == ""


@pytest.mark.parametrize("invalid", [None, "not-quotation", _quotation(-1), _quotation(float("nan")), _quotation(float("inf"))])
def test_catalog_invalid_monetary_metadata_cannot_become_a_multiplier(invalid) -> None:
    contract = _base_contract(_raw(min_price_increment_amount=invalid))
    assert contract.min_price_increment_amount == 0.0

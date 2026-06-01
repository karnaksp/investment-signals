"""conf/instruments.yaml: разбор тикеров."""

from __future__ import annotations

from collections import Counter
import textwrap
from pathlib import Path

from tinvest_signal_engine.config import load_instrument_configs


def test_spbfut_ticker_preserves_case(tmp_path: Path) -> None:
    p = tmp_path / "instruments.yaml"
    p.write_text(
        textwrap.dedent(
            """
            instruments:
              - ticker: SiM6
                class_code: SPBFUT
                alias: usd_rub
                subscriptions:
                  trades: true
                  last_price: true
                  info: false
              - ticker: sber
                class_code: TQBR
                alias: sber
                subscriptions:
                  trades: true
                  last_price: true
                  info: false
            """
        ).strip(),
        encoding="utf-8",
    )
    cf = load_instrument_configs(p)
    assert len(cf) == 2
    assert cf[0].ticker == "SiM6"
    assert cf[0].instrument_id == "SiM6_SPBFUT"
    assert cf[1].ticker == "SBER"


def test_repo_instruments_extended_ru_universe_is_parseable() -> None:
    p = Path(__file__).resolve().parents[1] / "conf" / "instruments.yaml"
    cf = load_instrument_configs(p)
    ids = [item.instrument_id for item in cf]

    assert len(ids) == len(set(ids))
    assert [item_id for item_id, count in Counter(ids).items() if count > 1] == []

    expected = {
        "VTBR_TQBR",
        "T_TQBR",
        "YDEX_TQBR",
        "OZON_TQBR",
        "POSI_TQBR",
        "ASTR_TQBR",
        "DIAS_TQBR",
        "VKCO_TQBR",
        "SGZH_TQBR",
        "MTLR_TQBR",
        "RNFT_TQBR",
        "DATA_TQBR",
        "EUTR_TQBR",
    }
    by_id = {item.instrument_id: item for item in cf}
    assert expected <= set(by_id)
    assert all(by_id[item_id].order_book_depth == 10 for item_id in expected)

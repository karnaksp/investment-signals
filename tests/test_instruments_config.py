"""conf/instruments.yaml: разбор тикеров."""

from __future__ import annotations

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
                display_name: USD/RUB (фьючерс)
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
    assert cf[0].display_name == "USD/RUB (фьючерс)"
    assert cf[1].ticker == "SBER"

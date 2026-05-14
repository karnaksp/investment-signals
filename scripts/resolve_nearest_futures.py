#!/usr/bin/env python3
"""Подобрать ближайшие контракты SPBFUT по базовому активу (T-Invest API) и обновить conf/instruments.yaml.

Берётся минимальная дата экспирации среди контрактов с ``api_trade_available_flag`` и
датой экспирации в будущем. Для RTS — полноразмерный (``basic_asset == RTSI``), мини пропускаются;
для EUR/RUB и USD/RUB — квартальные тикеры ``Eu*`` / ``Si*``; для палладия/платины — ``PD*`` / ``PT*``
(не ``LD*`` / ``LT*`` мини); для газа США — без «микро/мини» в названии.

Пример::

  python scripts/resolve_nearest_futures.py           # только stdout
  python scripts/resolve_nearest_futures.py --write   # заменить блок между маркерами в YAML

Нужны переменные как у ingestor: ``TINVEST_TOKEN``, опционально ``TINVEST_USE_SANDBOX=1``.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.schemas import InstrumentIdType

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_YAML = ROOT / "conf" / "instruments.yaml"

BEGIN = "# >>> FUTURES_NEAREST_BEGIN"
END = "# <<< FUTURES_NEAREST_END"

FUT_SUB = """    subscriptions:
      trades: true
      last_price: true
      info: false
      order_book_depth: 50
      candles: false
      candle_interval: 1m"""


@dataclass(frozen=True)
class FutureSpec:
    alias: str
    basic_asset: str
    display_name: str
    """Точное значение ``Future.basic_asset`` из API."""
    exclude_name_substrings: tuple[str, ...] = ()
    require_name_substrings: tuple[str, ...] = ()
    ticker_pattern: str | None = None
    """Если задано — ``re.fullmatch`` по ``Future.ticker`` (регистрозависимо, как в терминале)."""


SPECS: tuple[FutureSpec, ...] = (
    FutureSpec(
        alias="brent_oil",
        basic_asset="Brent",
        display_name="Нефть Brent (фьючерс)",
    ),
    FutureSpec(
        alias="gold",
        basic_asset="Золото в долларах",
        display_name="Золото (фьючерс)",
    ),
    FutureSpec(
        alias="silver",
        basic_asset="Серебро",
        display_name="Серебро (фьючерс)",
        require_name_substrings=("SILV-",),
        exclude_name_substrings=("мини", "SILVM"),
    ),
    FutureSpec(
        alias="usd_rub",
        basic_asset="USD/RUB",
        display_name="USD/RUB (фьючерс)",
        ticker_pattern=r"^Si[FGHJKMNQUVXZ]\d$",
    ),
    FutureSpec(
        alias="eur_rub",
        basic_asset="EUR/RUB",
        display_name="EUR/RUB (фьючерс)",
        ticker_pattern=r"^Eu[FGHJKMNQUVXZ]\d$",
    ),
    FutureSpec(
        alias="cny_rub",
        basic_asset="CNY/RUB",
        display_name="CNY/RUB (фьючерс)",
        ticker_pattern=r"^CR[FGHJKMNQUVXZ]\d$",
    ),
    FutureSpec(
        alias="wheat",
        basic_asset="Пшеница",
        display_name="Пшеница (фьючерс)",
        require_name_substrings=("WHEAT-",),
    ),
    FutureSpec(
        alias="palladium",
        basic_asset="Палладий",
        display_name="Палладий (фьючерс)",
        ticker_pattern=r"^PD[FGHJKMNQUVXZ]\d$",
    ),
    FutureSpec(
        alias="platinum",
        basic_asset="Платина",
        display_name="Платина (фьючерс)",
        ticker_pattern=r"^PT[FGHJKMNQUVXZ]\d$",
    ),
    FutureSpec(
        alias="nat_gas_us",
        basic_asset="Газ (США)",
        display_name="Природный газ США (фьючерс)",
        exclude_name_substrings=("микро", "мини"),
    ),
    FutureSpec(
        alias="nasdaq100",
        basic_asset="Nasdaq 100",
        display_name="Nasdaq 100 (фьючерс)",
    ),
    FutureSpec(
        alias="sber_fut",
        basic_asset="SBER",
        display_name="Сбербанк (фьючерс)",
    ),
    FutureSpec(
        alias="gazp_fut",
        basic_asset="GAZP",
        display_name="Газпром (фьючерс)",
    ),
    FutureSpec(
        alias="gold_rub",
        basic_asset="Золото в рублях",
        display_name="Золото в рублях (фьючерс)",
    ),
    FutureSpec(
        alias="rts_index",
        basic_asset="RTSI",
        display_name="Индекс РТС (фьючерс)",
    ),
    FutureSpec(
        alias="imoex",
        basic_asset="IMOEX",
        display_name="Индекс МосБиржи (фьючерс)",
    ),
)


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def _aware_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _pick_nearest(futures: list, spec: FutureSpec) -> object | None:
    now = datetime.now(timezone.utc)
    candidates: list = []
    pat = re.compile(spec.ticker_pattern) if spec.ticker_pattern else None
    for f in futures:
        if getattr(f, "class_code", "") != "SPBFUT":
            continue
        if getattr(f, "basic_asset", "") != spec.basic_asset:
            continue
        if not getattr(f, "api_trade_available_flag", False):
            continue
        name = getattr(f, "name", "") or ""
        if any(s in name for s in spec.exclude_name_substrings):
            continue
        if any(req not in name for req in spec.require_name_substrings):
            continue
        ticker = getattr(f, "ticker", "") or ""
        if pat is not None and not pat.fullmatch(ticker):
            continue
        exp = _aware_utc(getattr(f, "expiration_date", None))
        if exp is None or exp <= now:
            continue
        candidates.append(f)
    if not candidates:
        return None
    return min(candidates, key=lambda x: _aware_utc(x.expiration_date) or now)


def _yaml_block(entries: list[tuple[str, str, str]]) -> str:
    """entries: (alias, ticker, display_name) — тикер как в API (регистр важен, напр. ``SiM6``)."""
    lines = [
        f"  {BEGIN} (python scripts/resolve_nearest_futures.py --write)",
        "  # Ближайшие контракты по дате экспирации (см. скрипт).",
    ]
    for alias, ticker, display_name in entries:
        lines.append(f"  - ticker: {ticker}")
        lines.append("    class_code: SPBFUT")
        lines.append(f"    alias: {alias}")
        lines.append(f"    display_name: {display_name}")
        lines.append(FUT_SUB)
    lines.append(f"  {END}")
    return "\n".join(lines) + "\n"


def _verify_tickers(token: str, *, sandbox: bool, tickers: list[str]) -> None:
    target = INVEST_GRPC_API_SANDBOX if sandbox else None
    with Client(token, target=target) as client:
        for t in tickers:
            r = client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                class_code="SPBFUT",
                id=t,
            )
            ins = r.instrument
            if not ins or not ins.ticker:
                raise RuntimeError(f"get_instrument_by пусто для {t!r}")
            print(f"OK  {t:8s}  uid={ins.uid[:8]}…  {ins.name[:56]}")


def _replace_block(path: Path, new_block: str) -> None:
    text = path.read_text(encoding="utf-8")
    if BEGIN not in text or END not in text:
        raise SystemExit(
            f"В {path} нет маркеров {BEGIN!r} … {END!r}. "
            "Добавьте их вокруг секции фьючерсов (см. репозиторий после --write)."
        )
    # Съедаем пробелы перед маркером BEGIN (после замены не остаётся «висячего» отступа).
    pattern = re.compile(
        r"\s*" + re.escape(BEGIN) + r".*?" + re.escape(END),
        re.DOTALL,
    )
    if not pattern.search(text):
        raise SystemExit("Не удалось сопоставить блок маркеров (проверьте вложенность).")
    updated = pattern.sub(new_block.rstrip("\n"), text, count=1)
    path.write_text(updated, encoding="utf-8")


def main() -> int:
    _load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help=f"Заменить блок {BEGIN}…{END} в conf/instruments.yaml",
    )
    parser.add_argument(
        "--yaml",
        type=Path,
        default=DEFAULT_YAML,
        help="Путь к instruments.yaml",
    )
    args = parser.parse_args()

    token = (os.getenv("TINVEST_TOKEN") or "").strip()
    if not token:
        print("TINVEST_TOKEN не задан", file=sys.stderr)
        return 1
    sandbox = os.getenv("TINVEST_USE_SANDBOX", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    target = INVEST_GRPC_API_SANDBOX if sandbox else None

    with Client(token, target=target) as client:
        fut_resp = client.instruments.futures()
        futures = list(fut_resp.instruments)

    resolved: list[tuple[str, str, str]] = []
    for spec in SPECS:
        f = _pick_nearest(futures, spec)
        if f is None:
            print(
                f"ERR нет подходящего контракта basic_asset={spec.basic_asset!r}",
                file=sys.stderr,
            )
            return 2
        ticker = (f.ticker or "").strip()
        exp = _aware_utc(f.expiration_date)
        print(
            f"{spec.alias:12s}  {ticker:8s}  exp={exp.date() if exp else '?'}  {f.name[:52]}"
        )
        resolved.append((spec.alias, ticker, spec.display_name))

    block = _yaml_block(resolved)
    print("\n--- YAML ---")
    print(block)

    tickers = [t for _, t, _ in resolved]
    print("\nПроверка GetInstrumentBy …")
    _verify_tickers(token, sandbox=sandbox, tickers=tickers)

    if args.write:
        _replace_block(args.yaml, block)
        print(f"\nЗаписано: {args.yaml}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Official CFTC API and ICE CSV, futures-only and reconciled to prior reports."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import httpx

from tinvest_signal_engine.application.daily_positions import PositionSourceUnavailable
from tinvest_signal_engine.domain.commodity_positions import (
    MarketPositionComparison, PositionContext, TraderPositionChange,
)


CFTC_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
ICE_URL = "https://www.ice.com/publicdocs/futures/COTHist{year}.csv"
CFTC_MARKETS = {
    "088691": "gold", "084691": "silver", "076651": "platinum", "075651": "palladium",
    "085692": "copper", "067651": "wti", "023651": "natural_gas", "001602": "wheat_srw",
}
# Normalized CFTC SODA names -> (category, long, short, spreading, delta-long, delta-short).
_FIELDS = (
    ("managed_money", "m_money_positions_long_all", "m_money_positions_short_all",
     "m_money_positions_spread", "change_in_m_money_long_all", "change_in_m_money_short_all"),
    ("producers_merchants", "prod_merc_positions_long", "prod_merc_positions_short", None,
     "change_in_prod_merc_long", "change_in_prod_merc_short"),
    ("swap_dealers", "swap_positions_long_all", "swap__positions_short_all", "swap__positions_spread_all",
     "change_in_swap_long_all", "change_in_swap_short_all"),
    ("other_reportables", "other_rept_positions_long", "other_rept_positions_short", "other_rept_positions_spread",
     "change_in_other_rept_long", "change_in_other_rept_short"),
    ("nonreportable", "nonrept_positions_long_all", "nonrept_positions_short_all", None,
     "change_in_nonrept_long_all", "change_in_nonrept_short_all"),
)


def _integer(value) -> int:
    if type(value) is int:
        return value
    if isinstance(value, str):
        return int(value.strip().replace(",", ""))
    raise ValueError("position quantity must be an integer")


def _pair(rows: list[dict], *, source: str, market_id: str, commodity: str,
          as_of: datetime, url: str, digest: str) -> MarketPositionComparison:
    dated = sorted(rows, key=lambda x: x["report_date_as_yyyy_mm_dd"], reverse=True)
    if len(dated) < 2:
        raise ValueError("two position reports are required")
    current, previous = dated[:2]
    day = datetime.fromisoformat(current["report_date_as_yyyy_mm_dd"]).date()
    before = datetime.fromisoformat(previous["report_date_as_yyyy_mm_dd"]).date()
    if as_of.utcoffset() is None:
        raise ValueError("weekly collection timestamp must be aware")
    if not 1 <= (day - before).days <= 10:
        raise ValueError("weekly comparison has a missing or duplicate reporting period")
    if not 0 <= (as_of.astimezone(timezone.utc).date() - day).days <= 14:
        raise ValueError("weekly report is stale or future dated")
    groups = []
    for category, long_key, short_key, spread_key, dl, ds in _FIELDS:
        row = TraderPositionChange(
            category, _integer(current[long_key]), _integer(current[short_key]),
            _integer(previous[long_key]), _integer(previous[short_key]),
            _integer(current[spread_key]) if spread_key else 0,
            _integer(previous[spread_key]) if spread_key else 0,
        )
        if source == "CFTC" and (
            row.longs - row.previous_longs != _integer(current[dl])
            or row.shorts - row.previous_shorts != _integer(current[ds])
        ):
            raise ValueError("reported weekly change does not match previous report")
        groups.append(row)
    oi, old_oi = _integer(current["open_interest_all"]), _integer(previous["open_interest_all"])
    if source == "CFTC" and oi - old_oi != _integer(current["change_in_open_interest_all"]):
        raise ValueError("open interest change does not match previous report")
    return MarketPositionComparison(source, market_id, commodity, day, before, oi, old_oi,
                                    tuple(groups), url, digest)


def parse_cftc_positions(body: bytes, *, as_of: datetime) -> PositionContext:
    rows = json.loads(body)
    if not isinstance(rows, list) or not rows:
        raise ValueError("empty or invalid CFTC response")
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("CFTC rows must be objects")
    markets, unavailable = [], []
    digest = sha256(body).hexdigest()
    for code, commodity in CFTC_MARKETS.items():
        matching = [row for row in rows if row.get("cftc_contract_market_code") == code
                    and row.get("futonly_or_combined") == "FutOnly"]
        try:
            markets.append(_pair(matching, source="CFTC", market_id=code, commodity=commodity,
                                 as_of=as_of, url=CFTC_URL, digest=digest))
        except (KeyError, TypeError, ValueError):
            unavailable.append("CFTC:" + commodity)
    if not markets:
        raise ValueError("CFTC contains no valid timely position comparisons")
    return PositionContext(tuple(markets), tuple(unavailable))


def parse_ice_positions(body: bytes, *, as_of: datetime) -> PositionContext:
    rows = csv.DictReader(StringIO(body.decode("utf-8-sig")))
    normalized = []
    aliases = {
        "prod_merc_positions_long_all": "prod_merc_positions_long",
        "prod_merc_positions_short_all": "prod_merc_positions_short",
        "swap_positions_short_all": "swap__positions_short_all",
        "swap_positions_spread_all": "swap__positions_spread_all",
        "m_money_positions_spread_all": "m_money_positions_spread",
        "other_rept_positions_long_all": "other_rept_positions_long",
        "other_rept_positions_short_all": "other_rept_positions_short",
        "other_rept_positions_spread_all": "other_rept_positions_spread",
    }
    for row in rows:
        if (row.get("Market_and_Exchange_Names", "").strip() != "ICE Brent Crude Futures - ICE Futures Europe"
                or row.get("FutOnly_or_Combined") != "FutOnly"):
            continue
        data = {aliases.get(key.lower(), key.lower()): value for key, value in row.items()}
        data["report_date_as_yyyy_mm_dd"] = datetime.strptime(
            row["As_of_Date_Form_MM/DD/YYYY"], "%m/%d/%Y",
        ).date().isoformat()
        normalized.append(data)
    year = max((row["report_date_as_yyyy_mm_dd"][:4] for row in normalized), default=str(as_of.year))
    market = _pair(normalized, source="ICE", market_id="B", commodity="brent", as_of=as_of,
                   url=ICE_URL.format(year=year), digest=sha256(body).hexdigest())
    return PositionContext((market,))


class WeeklyPositionSource:
    """A bounded daily fetch; archived raw bytes make same-day retries stable."""
    def __init__(self, name: str, *, cache_dir: Path, client: httpx.Client | None = None) -> None:
        if name not in {"CFTC", "ICE"}:
            raise ValueError("unknown weekly source")
        self.name = name
        self._cache_dir = cache_dir
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=20, follow_redirects=True)

    def load(self, *, as_of: datetime) -> PositionContext:
        path = self._cache_dir / f"{self.name.lower()}-{as_of.astimezone(timezone.utc):%Y%m%d}.raw"
        try:
            if path.exists():
                body = path.read_bytes()
            else:
                if self.name == "CFTC":
                    codes = ",".join("'" + x + "'" for x in CFTC_MARKETS)
                    response = self._client.get(CFTC_URL, params={
                        "$where": f"cftc_contract_market_code in ({codes})",
                        "$order": "report_date_as_yyyy_mm_dd DESC,cftc_contract_market_code",
                        "$limit": 96,
                    })
                else:
                    response = self._client.get(ICE_URL.format(year=as_of.year))
                response.raise_for_status()
                body = response.content
                if self.name == "ICE" and as_of.month == 1:
                    # The first annual CSV can contain only one Brent report.
                    # Keep both original CSV documents (including headers) in
                    # the archive; the parser skips the second header row.
                    rows = csv.DictReader(StringIO(body.decode("utf-8-sig")))
                    count = sum(row.get("Market_and_Exchange_Names", "").strip()
                                == "ICE Brent Crude Futures - ICE Futures Europe"
                                and row.get("FutOnly_or_Combined") == "FutOnly" for row in rows)
                    if count < 2:
                        prior = self._client.get(ICE_URL.format(year=as_of.year - 1))
                        prior.raise_for_status()
                        body = body.rstrip() + b"\n" + prior.content
            result = (parse_cftc_positions if self.name == "CFTC" else parse_ice_positions)(body, as_of=as_of)
            if not path.exists():
                self._cache_dir.mkdir(parents=True, exist_ok=True)
                with NamedTemporaryFile(dir=self._cache_dir, delete=False) as stream:
                    temporary = Path(stream.name)
                    try:
                        stream.write(body)
                        stream.flush()
                        os.fsync(stream.fileno())
                        os.replace(temporary, path)
                    finally:
                        temporary.unlink(missing_ok=True)
            return result
        except (httpx.HTTPError, OSError, ValueError, TypeError, KeyError) as error:
            raise PositionSourceUnavailable(self.name + " position data unavailable") from error

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

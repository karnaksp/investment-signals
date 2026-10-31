"""Public MOEX end-of-day website data, not the delayed intraday FUTOI API."""

from datetime import date, datetime, time, timezone
import gzip
from hashlib import sha256
from html.parser import HTMLParser
import json
import logging
from math import isfinite
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo

import httpx

from tinvest_signal_engine.application.ruble_positions import RublePositionBatch
from tinvest_signal_engine.domain.asset_positions import AssetPositionGroup, AssetPositionReport, AssetPositionSection


logger = logging.getLogger(__name__)
CATALOG_URL = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
PAIRS = (("usd_rub", "Si", "Si", "USDRUBF", "USDRUBTOM"),
         ("cny_rub", "CNY", "CR", "CNYRUBF", "CNYRUBTOM"),
         ("eur_rub", "Eu", "Eu", "EURRUBF", "EURRUBTOM"))
_COUNTS = "Кол-во контрактов, шт."
_DELTA = "Изменение к пред. торговому дню, шт."
_PERCENT = "Изменение к пред. торговому дню, %"
_COLUMNS = ("long_fiz", "short_fiz", "long_jur", "short_jur", "total")


class _NextData(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.scripts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "script" and dict(attrs).get("id") == "__NEXT_DATA__":
            if self.scripts:
                raise ValueError("duplicate public page data")
            self.active = True

    def handle_endtag(self, tag):
        if tag == "script": self.active = False

    def handle_data(self, data):
        if self.active: self.scripts.append(data)


def _integer(value) -> int:
    if type(value) is not int:
        raise ValueError("MOEX position quantities must be integers")
    return value


def select_front_contracts(body: bytes, *, today: date) -> dict[str, str]:
    raw = json.loads(body)["securities"]
    rows = [dict(zip(raw["columns"], row)) for row in raw["data"]]
    result = {}
    for _, asset, prefix, _, _ in PAIRS:
        candidates = [row for row in rows if row.get("ASSETCODE") == asset
                      and re.fullmatch(re.escape(prefix) + r"[FGHJKMNQUVXZ]\d{1,2}", str(row.get("SECID", "")))
                      and isinstance(row.get("LASTTRADEDATE"), str)
                      and date.fromisoformat(row["LASTTRADEDATE"]) >= today]
        if candidates:
            result[asset] = min(candidates, key=lambda row: (row["LASTTRADEDATE"], row["SECID"]))["SECID"]
    return result


def parse_moex_position_page(body: bytes, *, security_id: str, asset: str,
                             source_url: str, as_of: datetime) -> AssetPositionSection:
    parser = _NextData()
    parser.feed(body.decode("utf-8"))
    raw = json.loads("".join(parser.scripts))
    components = raw["props"]["pageProps"]["recursiveComponentContentProps"]
    candidates = []
    for component in components.values():
        initial = component.get("initData") if isinstance(component, dict) else None
        data = initial.get("data") if isinstance(initial, dict) else None
        if isinstance(data, dict) and "openPositions" in data:
            candidates.append(data)
    if len(candidates) != 1:
        raise ValueError("exactly one public position table is required")
    data = candidates[0]
    if data["securityData"]["SECID"] != security_id or data["securityData"]["ASSETCODE"] != asset:
        raise ValueError("MOEX page contains a different contract or asset")
    table = data["openPositions"]
    if table.get("isError") is not False:
        raise ValueError("MOEX reports a position table error")
    rows = table["data"]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("MOEX position rows must be objects")
    selected = {}
    for title in (_COUNTS, _DELTA, _PERCENT):
        matching = [r for r in rows if r.get("title") == title]
        if len(matching) != 1:
            raise ValueError("missing or duplicate MOEX totals/change row")
        selected[title] = matching[0]
    current, changes, percentages = (selected[k] for k in (_COUNTS, _DELTA, _PERCENT))
    day = date.fromisoformat(current["tradedate"])
    if as_of.utcoffset() is None:
        raise ValueError("MOEX collection timestamp must be aware")
    today = as_of.astimezone(ZoneInfo("Europe/Moscow")).date()
    if not 1 <= (today - day).days <= 7:
        raise ValueError("MOEX report is stale, future dated or not a completed day")
    if data["openPositionsPeriod"]["tillDateIso"] != day.isoformat():
        raise ValueError("MOEX latest available date disagrees with its position rows")
    for row in selected.values():
        if row["asset"] != asset or row["type"] != "F" or row["tradedate"] != day.isoformat():
            raise ValueError("MOEX rows mix dates, instruments or futures/options")
    calendar = data["offDays"]
    if calendar[day.isoformat()]["is_traded"] != 1:
        raise ValueError("MOEX calendar does not confirm the report trading day")
    before = max(date.fromisoformat(k) for k, v in calendar.items()
                 if k < day.isoformat() and v.get("is_traded") == 1)
    if (day - before).days > 14:
        raise ValueError("previous MOEX session is unavailable")
    counts = {key: _integer(current[key]) for key in _COLUMNS}
    previous = {key: counts[key] - _integer(changes[key]) for key in _COLUMNS}
    if min(*counts.values(), *previous.values()) < 0:
        raise ValueError("MOEX changes imply negative positions")
    for key in _COLUMNS:
        published = percentages[key]
        if previous[key] and (type(published) not in (float, int) or not isfinite(published)
                              or abs(published - changes[key] / previous[key] * 100) > 0.011):
            raise ValueError("MOEX percentage change disagrees with integer positions")
    groups = tuple(AssetPositionGroup(category, counts[long_key], counts[short_key], previous[long_key], previous[short_key])
                   for category, long_key, short_key in (("individuals", "long_fiz", "short_fiz"),
                                                         ("legal_entities", "long_jur", "short_jur")))
    # MOEX publishes the sum of BOTH sides. Normalize to single-count OI and
    # retain the published gross total and the reconstruction method explicitly.
    return AssetPositionSection(
        "MOEX", asset, "daily", day, before, datetime.combine(day, time.min, timezone.utc), groups,
        source_url, sha256(body).hexdigest(), open_interest=counts["long_fiz"] + counts["long_jur"],
        previous_open_interest=previous["long_fiz"] + previous["long_jur"],
        comparison_method="exchange_reported_daily_change", reported_gross_open_positions=counts["total"],
        previous_reported_gross_open_positions=previous["total"],
    )


class MoexRublePositionSource:
    def __init__(self, *, cache_dir: Path, client: httpx.Client | None = None):
        self._cache_dir = cache_dir
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=20, follow_redirects=True,
                                             headers={"User-Agent": "investment-signals-daily-report/1.0"})

    def _get(self, url: str, **kwargs) -> bytes:
        response = self._client.get(url, **kwargs)
        response.raise_for_status()
        return response.content

    def _archive(self, body: bytes, section: AssetPositionSection) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._cache_dir / f"{section.trading_day}-{section.market_id}-{section.source_sha256[:16]}.html.gz"
        if path.exists(): return
        with NamedTemporaryFile(dir=self._cache_dir, delete=False) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(gzip.compress(body, mtime=0)); stream.flush(); os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally: temporary.unlink(missing_ok=True)

    def load(self, *, as_of: datetime) -> RublePositionBatch:
        if as_of.utcoffset() is None: raise ValueError("ruble timestamp must be aware")
        catalog = self._get(CATALOG_URL, params={"iss.meta": "off", "iss.only": "securities",
                                               "securities.columns": "SECID,ASSETCODE,LASTTRADEDATE"})
        fronts = select_front_contracts(catalog, today=as_of.astimezone(ZoneInfo("Europe/Moscow")).date())
        reports, unavailable = [], []
        for commodity, asset, _, perpetual, perpetual_asset in PAIRS:
            try:
                security = fronts[asset]
                sources = ((security, asset, "futures-on-currency/" + security.lower()),
                           (perpetual, perpetual_asset, "perpetual-futures/" + perpetual))
                sections = []
                for security_id, asset_id, path in sources:
                    url = "https://www.moex.com/ru/derivatives/" + path
                    body = self._get(url)
                    section = parse_moex_position_page(body, security_id=security_id, asset=asset_id,
                                                       source_url=url, as_of=as_of)
                    self._archive(body, section)
                    sections.append(section)
                if len({(s.trading_day, s.previous_day) for s in sections}) != 1:
                    raise ValueError("dated and perpetual MOEX reports have different sessions")
                reports.append(AssetPositionReport(commodity, tuple(sections)))
            except (httpx.HTTPError, OSError, ValueError, TypeError, KeyError) as error:
                logger.warning("MOEX ruble pair unavailable: %s (%s)", commodity, type(error).__name__)
                unavailable.append(commodity)
        return RublePositionBatch(tuple(reports), tuple(unavailable))

    def close(self):
        if self._owns_client: self._client.close()

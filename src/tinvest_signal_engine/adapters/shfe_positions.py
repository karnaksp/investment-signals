"""Official SHFE daily files mapped to complete participant-category totals."""

from __future__ import annotations

from datetime import date, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo

import httpx

from tinvest_signal_engine.domain.daily_positions import PositionSnapshot, PositionTotal


SOURCE_TEMPLATE = "https://www.shfe.com.cn/data/tradedata/future/dailydata/pm{day:%Y%m%d}.dat"
_CATEGORIES = {
    "期货公司会员/境外特殊经纪参与者": "broker_clients",
    "非期货公司会员/境外特殊非经纪参与者": "direct_members",
}


def parse_shfe_positions(body: bytes, *, trading_day: date) -> PositionSnapshot | None:
    raw = json.loads(body)
    if not isinstance(raw, dict) or raw.get("o_code") != "0000":
        raise ValueError("SHFE report did not return a successful result")
    if raw.get("report_date") != trading_day.strftime("%Y%m%d"):
        raise ValueError("SHFE report date differs from requested date")
    rows = raw.get("o_cursor")
    if not isinstance(rows, list):
        raise ValueError("SHFE report has no position table")
    if not rows:
        return None
    published = datetime.strptime(raw["update_date"], "%Y%m%d %H:%M:%S").replace(
        tzinfo=ZoneInfo("Asia/Shanghai")
    )
    totals = []
    for row in rows:
        symbol = str(row.get("INSTRUMENTID", "")).strip().lower()
        if symbol not in {"auall", "agall"}:
            continue
        # Only complete all-maturity category rows. Never sum ranked members,
        # contract ranking totals, or treat a missing ranking entry as zero.
        category = _CATEGORIES.get(str(row.get("PARTICIPANTID2", "")).strip())
        if category is None or str(row.get("PARTICIPANTID3", "")).strip() not in _CATEGORIES:
            raise ValueError("unknown SHFE all-maturity participant category")
        if _CATEGORIES[str(row["PARTICIPANTID3"]).strip()] != category:
            raise ValueError("SHFE long/short category mismatch")
        totals.append(PositionTotal(
            product=symbol[:2], category=category,
            longs=_integer(row["CJ2"]), shorts=_integer(row["CJ3"]),
            long_change=_integer(row["CJ2_CHG"]), short_change=_integer(row["CJ3_CHG"]),
        ))
    return PositionSnapshot(
        trading_day, published, tuple(totals), SOURCE_TEMPLATE.format(day=trading_day),
        sha256(body).hexdigest(),
    )


def _integer(value: object) -> int:
    if type(value) is int:
        return value
    if isinstance(value, str):
        return int(value.strip().replace(",", ""))
    raise ValueError("SHFE quantity must be an integer")


class ShfePositionSource:
    def __init__(self, *, cache_dir: Path, client: httpx.Client | None = None) -> None:
        self._cache_dir = cache_dir
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=20, headers={"User-Agent": "investment-signals-daily-report/1.0"},
        )

    def load(self, trading_day: date) -> PositionSnapshot | None:
        path = self._cache_dir / f"{trading_day:%Y%m%d}.json"
        if path.exists():
            return parse_shfe_positions(path.read_bytes(), trading_day=trading_day)
        response = self._client.get(SOURCE_TEMPLATE.format(day=trading_day))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        body = response.content
        result = parse_shfe_positions(body, trading_day=trading_day)
        if result is not None:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(dir=self._cache_dir, prefix=".shfe-", delete=False) as out:
                temporary = Path(out.name)
                try:
                    out.write(body)
                    out.flush()
                    os.fsync(out.fileno())
                    os.replace(temporary, path)
                finally:
                    temporary.unlink(missing_ok=True)
        return result

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

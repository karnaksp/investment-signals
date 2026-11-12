"""Live market data provider adapter implementing MarketDataProviderPort."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from typing import Any

import httpx

from tinvest_signal_engine.application.trader_copilot import MarketDataProviderPort
from tinvest_signal_engine.domain.trader_copilot import MarketContextSnapshot

logger = logging.getLogger(__name__)

INSTRUMENT_MAP: dict[str, str] = {
    "SBER": "SBER_TQBR",
    "GAZP": "GAZP_TQBR",
    "LKOH": "LKOH_TQBR",
    "ROSN": "ROSN_TQBR",
    "VTBR": "VTBR_TQBR",
    "T": "T_TQBR",
    "PLZL": "PLZL_TQBR",
    "MOEX": "MOEX_TQBR",
    "NVTK": "NVTK_TQBR",
    "YNDX": "YNDX_TQBR",
    "MGNT": "MGNT_TQBR",
    "CHMF": "CHMF_TQBR",
    "NLMK": "NLMK_TQBR",
    "ALRS": "ALRS_TQBR",
    "AFLT": "AFLT_TQBR",
    "Si": "SiZ6_SPBFUT",
    "CR": "CRZ6_SPBFUT",
    "IMOEX": "MXZ6_SPBFUT",
    "MX": "MXZ6_SPBFUT",
    "BR": "BRF7_SPBFUT",
    "GOLD": "GLZ6_SPBFUT",
}


class CompositeMarketDataProvider(MarketDataProviderPort):
    """Fetches market ticks and bars from ClickHouse and recent signals from PostgreSQL."""

    def __init__(
        self,
        *,
        clickhouse_url: str | None = None,
        clickhouse_user: str | None = None,
        clickhouse_password: str | None = None,
        postgres_host: str | None = None,
        postgres_port: int = 5432,
        postgres_db: str | None = None,
        postgres_user: str | None = None,
        postgres_password: str | None = None,
        timeout_seconds: float = 4.0,
    ) -> None:
        self._ch_url = (clickhouse_url or os.getenv("CLICKHOUSE_HTTP_URL") or "http://clickhouse:8123").rstrip("/")
        self._ch_user = clickhouse_user or os.getenv("CLICKHOUSE_USER") or "investment_signals"
        self._ch_pass = clickhouse_password or os.getenv("CLICKHOUSE_PASSWORD") or ""
        self._pg_host = postgres_host or os.getenv("POSTGRES_HOST") or "postgres"
        self._pg_port = postgres_port
        self._pg_db = postgres_db or os.getenv("POSTGRES_DB") or "investment_signals"
        self._pg_user = postgres_user or os.getenv("POSTGRES_USER") or "investment_signals"
        self._pg_pass = postgres_password or os.getenv("POSTGRES_PASSWORD") or ""
        self._timeout = timeout_seconds

        # Read secret files if env vars are file paths
        if not self._ch_pass:
            pw_file = os.getenv("CLICKHOUSE_PASSWORD_FILE")
            if pw_file and os.path.isfile(pw_file):
                try:
                    self._ch_pass = open(pw_file).read().strip()
                except Exception:
                    pass

        if not self._pg_pass:
            pg_pw_file = os.getenv("POSTGRES_PASSWORD_FILE")
            if pg_pw_file and os.path.isfile(pg_pw_file):
                try:
                    self._pg_pass = open(pg_pw_file).read().strip()
                except Exception:
                    pass

    def get_market_snapshot(self, ticker: str) -> MarketContextSnapshot | None:
        clean_ticker = ticker.upper().strip()
        instrument_id = INSTRUMENT_MAP.get(clean_ticker, f"{clean_ticker}_TQBR")

        last_price: float | None = None
        bid_price: float | None = None
        ask_price: float | None = None
        spread_bps: float | None = None
        vwap: float | None = None
        volume: float | None = None

        # 1. Fetch latest reference tick from ClickHouse
        auth = (self._ch_user, self._ch_pass) if self._ch_user and self._ch_pass else None
        try:
            with httpx.Client(timeout=self._timeout, auth=auth, trust_env=False) as client:
                query = (
                    f"SELECT event_at, trade_price, last_price, bid_price, ask_price "
                    f"FROM signal_engine.market_reference_ticks "
                    f"WHERE instrument_id = '{instrument_id}' "
                    f"AND (trade_price > 0 OR last_price > 0 OR bid_price > 0) "
                    f"ORDER BY event_at DESC LIMIT 1 FORMAT TabSeparated"
                )
                resp = client.post(self._ch_url, content=query.encode("utf-8"))
                if resp.status_code == 200 and resp.text.strip():
                    parts = resp.text.strip().split("\t")
                    if len(parts) >= 5:
                        tp = float(parts[1]) if parts[1] != "0" else None
                        lp = float(parts[2]) if parts[2] != "0" else None
                        bp = float(parts[3]) if parts[3] != "0" else None
                        ap = float(parts[4]) if parts[4] != "0" else None
                        last_price = tp or lp or bp
                        bid_price = bp
                        ask_price = ap
                        if bp and ap and (bp + ap) > 0:
                            spread_bps = round(((ap - bp) / ((ap + bp) / 2.0)) * 10000.0, 2)

                # Fetch VWAP from recent trade bars
                vwap_query = (
                    f"SELECT sum(sum_pv)/nullIf(sum(sum_qty), 0), sum(sum_qty) "
                    f"FROM signal_engine.features_trade_bar_1m "
                    f"WHERE instrument_id = '{instrument_id}' "
                    f"AND bucket >= now() - INTERVAL 12 HOUR FORMAT TabSeparated"
                )
                vwap_resp = client.post(self._ch_url, content=vwap_query.encode("utf-8"))
                if vwap_resp.status_code == 200 and vwap_resp.text.strip():
                    v_parts = vwap_resp.text.strip().split("\t")
                    if len(v_parts) >= 2:
                        try:
                            if v_parts[0] and v_parts[0] != "\\N":
                                vwap = round(float(v_parts[0]), 2)
                            if v_parts[1] and v_parts[1] != "\\N":
                                volume = round(float(v_parts[1]), 2)
                        except (ValueError, TypeError):
                            pass
        except Exception as ex:
            logger.debug("ClickHouse market tick lookup failed for %s: %s", instrument_id, ex)

        # 2. Fetch recent signals
        signals = self.get_recent_signals(clean_ticker, limit=4)

        return MarketContextSnapshot(
            ticker=clean_ticker,
            instrument_id=instrument_id,
            last_price=last_price,
            bid_price=bid_price,
            ask_price=ask_price,
            spread_bps=spread_bps,
            vwap=vwap,
            volume_24h=volume,
            recent_signals_summary=signals,
            captured_at=datetime.now(tz=timezone.utc),
        )

    def get_recent_signals(self, ticker: str, *, limit: int = 5) -> tuple[str, ...]:
        clean_ticker = ticker.upper().strip()
        results: list[str] = []

        try:
            import psycopg

            conn = psycopg.connect(
                host=self._pg_host,
                port=self._pg_port,
                dbname=self._pg_db,
                user=self._pg_user,
                password=self._pg_pass,
                connect_timeout=3,
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT signal_type, detected_at, summary "
                        "FROM market_signals "
                        "WHERE ticker = %s "
                        "ORDER BY detected_at DESC LIMIT %s",
                        (clean_ticker, limit),
                    )
                    rows = cur.fetchall()
                    for sig_type, det_at, summary in rows:
                        t_str = det_at.strftime("%H:%M") if hasattr(det_at, "strftime") else str(det_at)
                        short_sum = (summary or "").split("\n")[0][:80]
                        results.append(f"{t_str} · {sig_type}: {short_sum}")
        except Exception as ex:
            logger.debug("PostgreSQL recent signals lookup failed for %s: %s", clean_ticker, ex)

        return tuple(results)

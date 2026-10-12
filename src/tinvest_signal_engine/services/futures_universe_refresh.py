"""Composition root and CLI for refreshing the tracked futures universe."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from tinvest_signal_engine.adapters.futures_universe_yaml import (
    AtomicYamlFuturesUniversePublisher,
    load_future_watch_specs,
)
from tinvest_signal_engine.adapters.tinvest_futures_catalog import (
    TInvestFuturesCatalog,
)
from tinvest_signal_engine.application.futures_universe import (
    FuturesRefreshReceipt,
    refresh_futures_universe,
)
from tinvest_signal_engine.config import load_secret


logger = logging.getLogger(__name__)


def _env_bool(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def run_once(*, now: datetime | None = None) -> FuturesRefreshReceipt:
    token = (load_secret("TINVEST_TOKEN", service_name="dagster") or "").strip()
    if not token:
        raise RuntimeError("TINVEST_TOKEN is required for futures-universe refresh")
    watchlist_path = Path(
        os.getenv("FUTURES_UNIVERSE_CONFIG", "conf/futures_universe.yaml")
    )
    instruments_path = Path(
        os.getenv("INSTRUMENTS_CONFIG", "conf/instruments.yaml")
    )
    observed_at = now or datetime.now(timezone.utc)
    specs = load_future_watch_specs(watchlist_path)
    receipt = refresh_futures_universe(
        catalog=TInvestFuturesCatalog(
            token,
            sandbox=_env_bool("TINVEST_USE_SANDBOX"),
            app_name=(os.getenv("TINVEST_APP_NAME") or None),
        ),
        publisher=AtomicYamlFuturesUniversePublisher(instruments_path),
        specs=specs,
        now=observed_at,
    )
    logger.info(
        "Futures universe refresh changed=%s contracts=%s",
        receipt.changed,
        ",".join(f"{item.alias}:{item.ticker}" for item in receipt.resolved),
    )
    return receipt


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    receipt = run_once()
    print(
        json.dumps(
            {
                "changed": receipt.changed,
                "contracts": [
                    {
                        "alias": item.alias,
                        "ticker": item.ticker,
                        "expiration_at": item.expiration_at.isoformat(),
                        "two_sided_lots": item.two_sided_lots,
                    }
                    for item in receipt.resolved
                ],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Composition root for one dated SHFE gold/silver report."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path

from tinvest_signal_engine.adapters.daily_positions_report import (
    PostgresDailyPositionPublisher, comparison_payload, render_daily_positions_html,
)
from tinvest_signal_engine.adapters.postgres_reliability import connect_reliable_processing_store
from tinvest_signal_engine.adapters.shfe_positions import ShfePositionSource
from tinvest_signal_engine.application.daily_positions import CollectDailyPositions, DailyPositionReceipt
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)


class _PreviewPublisher:
    def publish(self, comparison, *, generated_at: datetime) -> bool:
        del generated_at
        print(render_daily_positions_html(comparison_payload(comparison)))
        return True


def run_once(
    settings: RuntimeSettings | None = None, *, dry_run: bool = False,
    as_of: datetime | None = None,
) -> DailyPositionReceipt:
    policy_path = Path(os.getenv("SHFE_DAILY_POSITION_POLICY_FILE", "conf/shfe-daily-positions.json"))
    policy = json.loads(policy_path.read_text())
    if policy.get("version") != 1 or type(policy.get("enabled")) is not bool:
        raise ValueError("daily position policy requires version 1 and an explicit enabled flag")
    if not policy["enabled"]:
        return DailyPositionReceipt("disabled")
    source = ShfePositionSource(cache_dir=Path(os.getenv(
        "SHFE_DAILY_POSITION_CACHE_DIR", "var/shfe-daily-positions/raw",
    )))
    store = None
    try:
        if dry_run:
            publisher = _PreviewPublisher()
        else:
            settings = settings or RuntimeSettings.from_env(service_name="dagster")
            # Only the delivery worker owns the bot credential. The producer
            # needs the destination identity to create its durable outbox task.
            if not settings.telegram_chat_id:
                raise ValueError("Telegram destination must be configured for daily position reports")
            store = connect_reliable_processing_store(settings)
            target = DeliveryTarget("telegram", (
                f"{settings.telegram_chat_id}:{settings.telegram_message_thread_id or ''}"
            ))
            publisher = PostgresDailyPositionPublisher(store=store, target=target)
        receipt = CollectDailyPositions(source=source, publisher=publisher).execute(
            as_of=as_of or datetime.now(timezone.utc),
        )
        if dry_run and receipt.status == "queued":
            receipt = DailyPositionReceipt("preview", receipt.trading_day, receipt.previous_day)
        logger.info("SHFE daily positions: status=%s date=%s previous=%s",
                    receipt.status, receipt.trading_day, receipt.previous_day)
        return receipt
    finally:
        source.close()
        if store is not None:
            store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Render without database writes or Telegram delivery")
    args = parser.parse_args()
    configure_logging("INFO")
    run_once(dry_run=args.dry_run)


if __name__ == "__main__":
    main()

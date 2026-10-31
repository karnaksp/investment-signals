"""Composition root for three daily ruble-pair image reports."""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path

from tinvest_signal_engine.adapters.asset_position_cards import asset_report_payload, render_asset_positions_html, render_asset_positions_png
from tinvest_signal_engine.adapters.asset_position_publisher import PostgresAssetPositionPublisher
from tinvest_signal_engine.adapters.moex_ruble_positions import MoexRublePositionSource
from tinvest_signal_engine.adapters.postgres_reliability import connect_reliable_processing_store
from tinvest_signal_engine.application.ruble_positions import CollectRublePositions, RublePositionReceipt
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)


class _PreviewPublisher:
    def publish_reports(self, reports, *, generated_at):
        directory = Path("var/ruble-positions/cards")
        directory.mkdir(parents=True, exist_ok=True)
        for report in reports:
            payload = asset_report_payload(report)
            print(render_asset_positions_html(payload))
            (directory / (report.commodity + ".png")).write_bytes(render_asset_positions_png(payload))
        return True


def run_once(settings: RuntimeSettings | None = None, *, dry_run: bool = False,
             as_of: datetime | None = None) -> RublePositionReceipt:
    policy = json.loads(Path(os.getenv("RUBLE_POSITION_POLICY_FILE", "conf/ruble-daily-positions.json")).read_text())
    if policy.get("version") != 1 or type(policy.get("enabled")) is not bool:
        raise ValueError("ruble position policy requires version 1 and an explicit enabled flag")
    if not policy["enabled"]: return RublePositionReceipt("disabled")
    source = MoexRublePositionSource(cache_dir=Path(os.getenv("RUBLE_POSITION_CACHE_DIR", "var/ruble-positions/raw")))
    store = None
    try:
        if dry_run:
            publisher = _PreviewPublisher()
        else:
            settings = settings or RuntimeSettings.from_env(service_name="dagster")
            if not settings.telegram_chat_id:
                raise ValueError("Telegram destination is required for ruble reports")
            store = connect_reliable_processing_store(settings)
            publisher = PostgresAssetPositionPublisher(store=store, target=DeliveryTarget(
                "telegram", f"{settings.telegram_chat_id}:{settings.telegram_message_thread_id or ''}",
            ))
        receipt = CollectRublePositions(source=source, publisher=publisher).execute(
            as_of=as_of or datetime.now(timezone.utc),
        )
        if dry_run and receipt.status == "queued":
            receipt = RublePositionReceipt("preview", receipt.pairs, receipt.unavailable_pairs)
        logger.info("Ruble positions: status=%s pairs=%s unavailable=%s", receipt.status, receipt.pairs, receipt.unavailable_pairs)
        return receipt
    finally:
        source.close()
        if store is not None: store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    configure_logging("INFO")
    run_once(dry_run=args.dry_run)


if __name__ == "__main__": main()

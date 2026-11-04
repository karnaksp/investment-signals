"""Independent realtime worker for market-opening Telegram notifications."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path

from tinvest_signal_engine.adapters.postgres_reliability import connect_reliable_processing_store
from tinvest_signal_engine.adapters.session_calendar import ExchangeSessionCalendar
from tinvest_signal_engine.adapters.session_open_message import opening_payload, render_session_open_html
from tinvest_signal_engine.adapters.session_open_publisher import PostgresSessionOpenPublisher
from tinvest_signal_engine.adapters.worker_health_file import WorkerHealthFileSink
from tinvest_signal_engine.application.session_open import AnnounceSessionOpenings
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import graceful_shutdown_event


logger = logging.getLogger(__name__)


def load_calendar() -> ExchangeSessionCalendar:
    path = Path(os.getenv("SESSION_OPEN_POLICY_FILE", "conf/market-sessions.json"))
    return ExchangeSessionCalendar(json.loads(path.read_text()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preview", action="store_true", help="Print the next 24 hours; no database or sending")
    parser.add_argument("--at", type=datetime.fromisoformat, help="Preview time with an explicit UTC offset")
    args = parser.parse_args()
    if args.at and not args.preview:
        parser.error("--at is available only with --preview")
    configure_logging("INFO")
    calendar = load_calendar()
    if not calendar.enabled:
        logger.info("Session opening notifications disabled")
        return
    if args.preview:
        now = args.at or datetime.now(timezone.utc)
        upcoming = {item.session_id + item.opens_at.isoformat(): item
                    for day in (now, now + timedelta(days=1))
                    for item in calendar.openings(now=day)
                    if now <= item.opens_at < now + timedelta(days=1)}
        for item in sorted(upcoming.values(), key=lambda item: (item.opens_at, item.session_id)):
            print(item.opens_at.isoformat(), render_session_open_html(
                opening_payload(item, display_timezone=calendar.display_timezone)), "\n")
        if calendar.unavailable:
            raise RuntimeError("Some session calendars are unavailable")
        return
    settings = RuntimeSettings.from_env(service_name="session_open_worker")
    if not settings.telegram_chat_id:
        raise ValueError("Telegram destination is required for session notifications")
    store = connect_reliable_processing_store(settings)
    publisher = PostgresSessionOpenPublisher(store=store, target=DeliveryTarget(
        "telegram", f"{settings.telegram_chat_id}:{settings.telegram_message_thread_id or ''}"),
        display_timezone=calendar.display_timezone)
    use_case = AnnounceSessionOpenings(calendar=calendar, publisher=publisher)
    health = WorkerHealthTracker(worker_id="session_open_worker", sink=WorkerHealthFileSink(
        Path(os.getenv("SESSION_OPEN_HEALTH_PATH", "/tmp/session-open-worker.json"))), stale_after_seconds=60)
    try:
        with graceful_shutdown_event(logger=logger, worker="session_open_worker") as stop:
            while not stop.is_set():
                health.heartbeat()
                try:
                    # Warm the day's calendars before sampling the delivery
                    # timestamp, so cold-start computation cannot backdate alerts.
                    calendar.openings(now=datetime.now(timezone.utc))
                    queued = use_case.execute(now=datetime.now(timezone.utc))
                    if calendar.unavailable:
                        health.failed("session_calendar_unavailable")
                    else:
                        health.succeeded(force=bool(queued))
                    if queued:
                        logger.info("Session openings queued: %s", queued)
                except Exception:
                    health.failed("session_open_cycle_failed")
                    logger.exception("Session opening cycle failed")
                    raise  # Supervisor restart also recreates a broken DB connection.
                stop.wait(5)
    finally:
        store.close()


if __name__ == "__main__":
    main()

"""Host-only command for previewing and retrying one delivery dead letter."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from uuid import UUID

from tinvest_signal_engine.adapters.postgres_reliability import (
    connect_delivery_queue,
)
from tinvest_signal_engine.application.manual_delivery_retry import (
    ManualDeliveryRetry,
    ManualDeliveryRetryResult,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.logging_utils import configure_logging


logger = logging.getLogger(__name__)


def _outbox_id(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as error:
        raise argparse.ArgumentTypeError("outbox id must be a UUID") from error


def _result_payload(
    result: ManualDeliveryRetryResult,
) -> dict[str, object]:
    delivery = result.delivery
    return {
        "outcome": result.outcome,
        "reason_code": result.reason_code,
        "outbox_id": delivery.outbox_id if delivery else None,
        "destination_type": delivery.destination_type if delivery else None,
        "status_before": delivery.status if delivery else None,
        "attempt_count_before": delivery.attempt_count if delivery else None,
        "last_error_code": delivery.last_error_code if delivery else None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preview one delivery dead letter. Requeue requires repeating the "
            "same UUID with --confirm-retry."
        )
    )
    parser.add_argument("outbox_id", type=_outbox_id)
    parser.add_argument(
        "--confirm-retry",
        type=_outbox_id,
        metavar="OUTBOX_ID",
        help="atomically requeue this exact transient dead letter",
    )
    args = parser.parse_args(argv)
    if (
        args.confirm_retry is not None
        and args.confirm_retry != args.outbox_id
    ):
        parser.error("--confirm-retry must equal the positional outbox id")

    settings = RuntimeSettings.from_env(service_name="delivery_worker")
    configure_logging(settings.log_level)
    queue = connect_delivery_queue(settings)
    use_case = ManualDeliveryRetry(queue=queue)
    try:
        if args.confirm_retry is None:
            result = use_case.preview(outbox_id=args.outbox_id)
        else:
            result = use_case.retry(
                outbox_id=args.outbox_id,
                available_at=datetime.now(tz=timezone.utc),
            )
            if result.outcome == "requeued":
                logger.warning(
                    "Operator requeued delivery dead letter outbox_id=%s "
                    "destination=%s previous_error=%s previous_attempts=%s",
                    result.delivery.outbox_id if result.delivery else "unknown",
                    (
                        result.delivery.destination_type
                        if result.delivery
                        else "unknown"
                    ),
                    (
                        result.delivery.last_error_code
                        if result.delivery
                        else "unknown"
                    ),
                    (
                        result.delivery.attempt_count
                        if result.delivery
                        else "unknown"
                    ),
                )
        print(json.dumps(_result_payload(result), sort_keys=True))
        return 0 if result.outcome in {"eligible", "requeued"} else 2
    finally:
        queue.close()


if __name__ == "__main__":
    raise SystemExit(main())

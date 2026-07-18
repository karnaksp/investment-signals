"""Composition root for the daily bond convergence scan."""

from __future__ import annotations

import logging

from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX

from tinvest_signal_engine.adapters.tinvest_bond_convergence import (
    KafkaBondConvergencePublisher,
    TInvestBondConvergenceSource,
)
from tinvest_signal_engine.application.bond_convergence import (
    BondConvergenceScanReceipt,
    ScanBondConvergence,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.kafka_wire_config import validate_kafka_wire_settings
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.ingestor import build_kafka_producer


logger = logging.getLogger(__name__)


def run_once(settings: RuntimeSettings) -> BondConvergenceScanReceipt:
    if not settings.tinvest_token.strip():
        raise RuntimeError("TINVEST_TOKEN обязателен")
    validate_kafka_wire_settings(settings, check_signal=False)
    producer = build_kafka_producer(settings)
    try:
        use_case = ScanBondConvergence(
            source=TInvestBondConvergenceSource(
                token=settings.tinvest_token,
                target=(
                    INVEST_GRPC_API_SANDBOX
                    if settings.tinvest_use_sandbox
                    else None
                ),
                app_name=settings.tinvest_app_name,
            ),
            publisher=KafkaBondConvergencePublisher(
                producer=producer,
                topic=settings.kafka_raw_topic,
                protobuf_values=settings.kafka_raw_value_format == "protobuf",
            ),
        )
        receipt = use_case.execute()
        producer.flush()
        return receipt
    finally:
        producer.close()


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="bond_convergence_emitter")
    configure_logging(settings.log_level)
    receipt = run_once(settings)
    logger.info(
        "Bond convergence scan completed: inspected=%s published=%s rejected=%s",
        receipt.inspected,
        receipt.published,
        receipt.rejected,
    )


if __name__ == "__main__":
    main()

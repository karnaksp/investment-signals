"""Composition root for single-host prospective scientific evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
from pathlib import Path

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
)
from tinvest_signal_engine.adapters.file_prospective_scientific_observations import (
    ImmutableFileProspectiveOutcomeEvidenceSource,
    ImmutableFileProspectiveScientificStore,
)
from tinvest_signal_engine.adapters.worker_health_file import WorkerHealthFileSink
from tinvest_signal_engine.application.prospective_scientific_observations import (
    ProcessMatureProspectiveScientificOutcomes,
    RecordProspectiveScientificObservation,
)
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.domain.scientific_candle_models import (
    ScientificCandlePolicy,
)
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import graceful_shutdown_event


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FileProspectiveScientificRuntime:
    recorder: RecordProspectiveScientificObservation
    outcome_worker: ProcessMatureProspectiveScientificOutcomes
    store: ImmutableFileProspectiveScientificStore


def build_file_prospective_scientific_runtime(
    *,
    state_dir: str | Path,
    evidence_dir: str | Path,
    policy: ScientificCandlePolicy = ScientificCandlePolicy(),
    outcome_policy_version: str = "prospective-scientific-outcomes-v1",
    grace_seconds: int = 30,
) -> FileProspectiveScientificRuntime:
    store = ImmutableFileProspectiveScientificStore(state_dir)
    return FileProspectiveScientificRuntime(
        recorder=RecordProspectiveScientificObservation(store),
        outcome_worker=ProcessMatureProspectiveScientificOutcomes(
            store=store,
            evidence=ImmutableFileProspectiveOutcomeEvidenceSource(evidence_dir),
            policy=policy,
            outcome_policy_version=outcome_policy_version,
            grace_seconds=grace_seconds,
        ),
        store=store,
    )


def main() -> None:
    configure_logging((os.getenv("LOG_LEVEL") or "INFO").strip())
    runtime = build_file_prospective_scientific_runtime(
        state_dir=Path(
            os.getenv(
                "PROSPECTIVE_SCIENTIFIC_STATE_DIR",
                "/var/lib/investment-signals/scientific-evidence",
            )
        ),
        evidence_dir=Path(
            os.getenv(
                "PROSPECTIVE_SCIENTIFIC_OUTCOME_EVIDENCE_DIR",
                "/var/lib/investment-signals/scientific-outcome-evidence",
            )
        ),
        outcome_policy_version=(
            os.getenv(
                "PROSPECTIVE_SCIENTIFIC_OUTCOME_POLICY_VERSION",
                "prospective-scientific-outcomes-v1",
            ).strip()
        ),
        grace_seconds=_env_int("PROSPECTIVE_SCIENTIFIC_GRACE_SECONDS", 30),
    )
    batch_size = _env_int("PROSPECTIVE_SCIENTIFIC_BATCH_SIZE", 100)
    poll_seconds = _env_float("PROSPECTIVE_SCIENTIFIC_POLL_SECONDS", 5.0)
    health = WorkerHealthTracker(
        worker_id="prospective_scientific_outcome_worker",
        sink=WorkerHealthFileSink(
            Path(
                os.getenv("PROSPECTIVE_SCIENTIFIC_HEALTH_SNAPSHOT_PATH")
                or "/tmp/prospective-scientific-health.json"
            )
        ),
        stale_after_seconds=_env_int(
            "PROSPECTIVE_SCIENTIFIC_HEALTH_STALE_AFTER_SECONDS",
            180,
        ),
    )
    backoff = BoundedExponentialBackoff(base_seconds=1.0, maximum_seconds=60.0)
    consecutive_failures = 0
    logger.info("Starting prospective scientific outcome worker")
    with graceful_shutdown_event(
        logger=logger,
        worker="prospective_scientific_outcome_worker",
    ) as stop_event:
        while not stop_event.is_set():
            health.heartbeat()
            try:
                result = runtime.outcome_worker.run_once(
                    now=datetime.now(tz=timezone.utc),
                    limit=batch_size,
                )
            except OSError:
                consecutive_failures += 1
                health.failed("evidence_storage_unavailable")
                logger.warning(
                    "Prospective scientific evidence storage is unavailable; retrying",
                    extra={"consecutive_failures": consecutive_failures},
                )
                stop_event.wait(backoff.delay(consecutive_failures))
                continue
            except Exception:
                health.failed("worker_cycle_failed")
                raise
            consecutive_failures = 0
            health.succeeded(force=bool(result.scanned))
            if result.stored or result.replayed or result.unavailable:
                logger.info(
                    "Processed prospective scientific outcomes",
                    extra={
                        "pending": result.pending,
                        "replayed": result.replayed,
                        "scanned": result.scanned,
                        "stored": result.stored,
                        "unavailable": result.unavailable,
                    },
                )
            if result.stored == 0:
                stop_event.wait(poll_seconds)


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    return float(raw) if raw else default


if __name__ == "__main__":
    main()

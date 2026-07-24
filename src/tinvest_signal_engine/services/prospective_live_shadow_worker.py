"""Production composition root for prospective live-shadow evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
from random import random
from threading import Event
import time
from typing import Callable

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
    TransientClickHouseError,
)
from tinvest_signal_engine.adapters.dependency_recovery import (
    DependencyRecoveryMetrics,
    NoopDependencyRecoveryMetrics,
    record_dependency_recovered,
    wait_for_dependency,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_live_shadow import (
    ClickHouseProspectiveLiveOutcomeSource,
    ClickHouseProspectiveLiveSnapshotSource,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations import (
    ClickHouseProspectiveLiveShadowStore,
)
from tinvest_signal_engine.adapters.reliability_metrics import (
    PrometheusReliabilityMetrics,
    start_reliability_metrics_server,
)
from tinvest_signal_engine.application.prospective_live_shadow import (
    DEFAULT_LIVE_OUTCOME_POLICY_VERSION,
    ProcessProspectiveLiveOutcomes,
    ProspectiveLiveOutcomeBatchResult,
    ProspectiveLiveShadowEvent,
    ProspectivePortfolioIngestResult,
    RecordProspectivePortfolioSnapshot,
)
from tinvest_signal_engine.config import load_instrument_configs, load_secret
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveScientificPolicy,
)
from tinvest_signal_engine.logging_utils import configure_logging
from tinvest_signal_engine.services.graceful_shutdown import (
    graceful_shutdown_event,
)


logger = logging.getLogger(__name__)

PRODUCTION_LIVE_POLICY = ProspectiveScientificPolicy(
    version="prospective-live-shadow-models-v1.0.0",
    jump_horizons_seconds=(900,),
)


@dataclass(frozen=True, slots=True)
class ProspectiveLiveShadowPassResult:
    snapshots: int
    observations_stored: int
    observations_replayed: int
    outcome_result: ProspectiveLiveOutcomeBatchResult
    events: tuple[ProspectiveLiveShadowEvent, ...]


@dataclass(frozen=True, slots=True)
class ProspectiveSnapshotBatch:
    slot_at: datetime
    instrument_ids: tuple[str, ...]


@dataclass(slots=True)
class ProspectiveSnapshotBatchSchedule:
    """Process every configured instrument without materializing the portfolio."""

    instrument_ids: tuple[str, ...]
    batch_size: int = 1
    slot_minutes: int = 30
    settlement_delay_minutes: int = 2
    _slot_at: datetime | None = None
    _cursor: int = 0

    def __post_init__(self) -> None:
        normalized = tuple(dict.fromkeys(item.strip() for item in self.instrument_ids))
        if not normalized or any(not item for item in normalized):
            raise ValueError("snapshot schedule instruments must be non-empty")
        if self.batch_size <= 0:
            raise ValueError("snapshot schedule batch_size must be positive")
        if self.slot_minutes <= 0 or 60 % self.slot_minutes:
            raise ValueError("snapshot schedule slot_minutes must divide one hour")
        if not 0 <= self.settlement_delay_minutes < self.slot_minutes:
            raise ValueError(
                "snapshot schedule settlement delay must fit inside one slot"
            )
        self.instrument_ids = normalized

    def pending(self, *, now: datetime, limit: int) -> ProspectiveSnapshotBatch | None:
        cutoff = _aware_utc(now)
        if limit <= 0:
            raise ValueError("snapshot schedule limit must be positive")
        settled_cutoff = cutoff - timedelta(minutes=self.settlement_delay_minutes)
        current_slot = settled_cutoff.replace(
            minute=(settled_cutoff.minute // self.slot_minutes) * self.slot_minutes,
            second=0,
            microsecond=0,
        )
        if self._slot_at is None:
            self._slot_at = current_slot
        elif self._cursor >= len(self.instrument_ids) and current_slot > self._slot_at:
            self._slot_at = current_slot
            self._cursor = 0
        if self._cursor >= len(self.instrument_ids):
            return None
        size = min(self.batch_size, limit)
        return ProspectiveSnapshotBatch(
            slot_at=self._slot_at,
            instrument_ids=self.instrument_ids[self._cursor : self._cursor + size],
        )

    def complete(self, batch: ProspectiveSnapshotBatch) -> None:
        expected = self.instrument_ids[
            self._cursor : self._cursor + len(batch.instrument_ids)
        ]
        if batch.slot_at != self._slot_at or batch.instrument_ids != expected:
            raise ValueError("snapshot batch is stale or out of order")
        self._cursor += len(batch.instrument_ids)


@dataclass(frozen=True, slots=True)
class ClickHouseProspectiveLiveShadowRuntime:
    recorder: RecordProspectivePortfolioSnapshot
    outcome_worker: ProcessProspectiveLiveOutcomes
    snapshot_source: ClickHouseProspectiveLiveSnapshotSource
    store: ClickHouseProspectiveLiveShadowStore
    policy: ProspectiveScientificPolicy
    snapshot_schedule: ProspectiveSnapshotBatchSchedule

    def run_once(
        self,
        *,
        now: datetime,
        snapshot_limit: int = 25,
        outcome_limit: int = 100,
    ) -> ProspectiveLiveShadowPassResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        snapshot_batch = self.snapshot_schedule.pending(
            now=now,
            limit=snapshot_limit,
        )
        snapshots = (
            self.snapshot_source.load_snapshots(
                as_of=snapshot_batch.slot_at,
                policy=self.policy,
                limit=len(snapshot_batch.instrument_ids),
                instrument_ids=snapshot_batch.instrument_ids,
            )
            if snapshot_batch is not None
            else ()
        )
        ingested: list[ProspectivePortfolioIngestResult] = []
        for snapshot in snapshots:
            result = self.recorder.execute(snapshot)
            ingested.append(result)
        if snapshot_batch is not None:
            self.snapshot_schedule.complete(snapshot_batch)
        outcome_result = self.outcome_worker.run_once(now=now, limit=outcome_limit)
        return ProspectiveLiveShadowPassResult(
            snapshots=len(snapshots),
            observations_stored=sum(item.stored for item in ingested),
            observations_replayed=sum(item.replayed for item in ingested),
            outcome_result=outcome_result,
            events=tuple(item.event for item in ingested) + (outcome_result.event,),
        )


def build_clickhouse_prospective_live_shadow_runtime(
    *,
    base_url: str,
    database: str,
    username: str,
    password: str,
    instrument_ids: tuple[str, ...],
    timeout_seconds: float = 15.0,
    snapshot_query_batch_size: int = 1,
    policy: ProspectiveScientificPolicy = PRODUCTION_LIVE_POLICY,
    outcome_policy_version: str = DEFAULT_LIVE_OUTCOME_POLICY_VERSION,
) -> ClickHouseProspectiveLiveShadowRuntime:
    if len(policy.jump_horizons_seconds) != 1:
        raise ValueError("live production policy must define one jump horizon")
    store = ClickHouseProspectiveLiveShadowStore(
        base_url=base_url,
        database=database,
        username=username,
        password=password,
        instrument_ids=instrument_ids,
        timeout_seconds=timeout_seconds,
    )
    return ClickHouseProspectiveLiveShadowRuntime(
        recorder=RecordProspectivePortfolioSnapshot(
            store=store,
            policy=policy,
            outcome_policy_version=outcome_policy_version,
        ),
        outcome_worker=ProcessProspectiveLiveOutcomes(
            store=store,
            source=ClickHouseProspectiveLiveOutcomeSource(
                store,
                ewma_alpha=policy.har_ewma_alpha,
                policy=policy,
            ),
            policy=policy,
            outcome_policy_version=outcome_policy_version,
        ),
        snapshot_source=ClickHouseProspectiveLiveSnapshotSource(
            store,
            instrument_ids=instrument_ids,
        ),
        store=store,
        policy=policy,
        snapshot_schedule=ProspectiveSnapshotBatchSchedule(
            instrument_ids=instrument_ids,
            batch_size=snapshot_query_batch_size,
        ),
    )


def main() -> None:
    configure_logging((os.getenv("LOG_LEVEL") or "INFO").strip())
    password = load_secret("CLICKHOUSE_PASSWORD")
    if password is None:
        raise ValueError("CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required")
    snapshot_limit = _env_int("PROSPECTIVE_LIVE_SNAPSHOT_BATCH_SIZE", 25)
    snapshot_query_batch_size = _env_int(
        "PROSPECTIVE_LIVE_SNAPSHOT_QUERY_BATCH_SIZE",
        1,
    )
    runtime = build_clickhouse_prospective_live_shadow_runtime(
        base_url=_required_env("CLICKHOUSE_HTTP_URL"),
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=_required_env("CLICKHOUSE_USERNAME"),
        password=password,
        instrument_ids=_instrument_ids(snapshot_limit),
        timeout_seconds=_env_float("PROSPECTIVE_LIVE_CLICKHOUSE_TIMEOUT_SECONDS", 15.0),
        snapshot_query_batch_size=snapshot_query_batch_size,
        outcome_policy_version=(
            os.getenv(
                "PROSPECTIVE_LIVE_OUTCOME_POLICY_VERSION",
                DEFAULT_LIVE_OUTCOME_POLICY_VERSION,
            ).strip()
        ),
    )
    outcome_limit = _env_int("PROSPECTIVE_LIVE_OUTCOME_BATCH_SIZE", 100)
    poll_seconds = _env_float("PROSPECTIVE_LIVE_POLL_SECONDS", 60.0)
    metrics_port = _env_optional_int("METRICS_LISTEN_PORT")
    start_reliability_metrics_server(metrics_port)
    logger.info("Starting prospective live-shadow ClickHouse worker")
    with graceful_shutdown_event(
        logger=logger,
        worker="prospective_live_shadow_worker",
    ) as stop_event:
        run_worker_loop(
            runtime,
            snapshot_limit=snapshot_limit,
            outcome_limit=outcome_limit,
            poll_seconds=poll_seconds,
            stop_event=stop_event,
            metrics=PrometheusReliabilityMetrics(),
        )


def run_worker_loop(
    runtime: ClickHouseProspectiveLiveShadowRuntime,
    *,
    snapshot_limit: int,
    outcome_limit: int,
    poll_seconds: float,
    stop_event: Event | None = None,
    metrics: DependencyRecoveryMetrics | None = None,
    backoff: BoundedExponentialBackoff = BoundedExponentialBackoff(),
    now: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
    random_value: Callable[[], float] = random,
) -> None:
    """Keep the worker alive while its external data services recover."""

    stop = stop_event or Event()
    recovery_metrics = metrics or NoopDependencyRecoveryMetrics()
    consecutive_failures = 0
    while not stop.is_set():
        try:
            result = runtime.run_once(
                now=now(),
                snapshot_limit=snapshot_limit,
                outcome_limit=outcome_limit,
            )
            if (
                result.observations_stored
                or result.observations_replayed
                or result.outcome_result.stored
                or result.outcome_result.replayed
                or result.outcome_result.unavailable
            ):
                logger.info(
                    "Processed prospective live-shadow pass",
                    extra={
                        "events": len(result.events),
                        "observations_replayed": result.observations_replayed,
                        "observations_stored": result.observations_stored,
                        "outcomes_replayed": result.outcome_result.replayed,
                        "outcomes_stored": result.outcome_result.stored,
                        "outcomes_unavailable": result.outcome_result.unavailable,
                        "snapshots": result.snapshots,
                    },
                )
            record_dependency_recovered(
                worker="prospective_live_shadow_worker",
                operation="scientific_evidence_request",
                consecutive_failures=consecutive_failures,
                metrics=recovery_metrics,
                logger=logger,
            )
            consecutive_failures = 0
        except KeyboardInterrupt:
            stop.set()
            raise
        except TransientClickHouseError as error:
            consecutive_failures += 1
            if wait_for_dependency(
                worker="prospective_live_shadow_worker",
                error=error,
                consecutive_failures=consecutive_failures,
                stop_event=stop,
                backoff=backoff,
                metrics=recovery_metrics,
                logger=logger,
                random_value=random_value,
            ):
                break
            continue
        except Exception:
            logger.exception(
                "Prospective live-shadow pass failed; retrying after dependency recovery"
            )
        if stop_event is None:
            sleep(poll_seconds)
        else:
            stop.wait(poll_seconds)


def _required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _instrument_ids(limit: int) -> tuple[str, ...]:
    explicit = tuple(
        item.strip()
        for item in (os.getenv("PROSPECTIVE_LIVE_INSTRUMENT_IDS") or "").split(",")
        if item.strip()
    )
    if explicit:
        selected = explicit
    else:
        path = Path(
            (
                os.getenv("INSTRUMENTS_CONFIG")
                or "/etc/investment-signals-pro/instruments.yaml"
            ).strip()
        )
        configured = tuple(load_instrument_configs(path))
        candle_enabled = tuple(
            item.instrument_id
            for item in configured
            if item.candles and item.candle_interval == "1m"
        )
        # Installations created before the prospective evidence worker did not
        # enable the candle subscription. They may still contain replayed
        # scientific candles, so keep the worker operational for every active
        # market-data instrument while the installation is upgraded. Fresh
        # installations enable one-minute candles explicitly.
        selected = candle_enabled or tuple(
            item.instrument_id
            for item in configured
            if item.trades or item.last_price or item.candles
        )
    unique = tuple(dict.fromkeys(selected))
    if not unique:
        raise ValueError("prospective live-shadow requires at least one instrument")
    return unique[:limit]


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    value = int(raw) if raw else default
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    value = float(raw) if raw else default
    if value <= 0.0:
        raise ValueError(f"{name} must be positive")
    return value


def _env_optional_int(name: str) -> int | None:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("snapshot schedule now must be timezone-aware")
    return value.astimezone(timezone.utc)


if __name__ == "__main__":
    main()

"""Production composition root for prospective live-shadow evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import time

from tinvest_signal_engine.adapters.clickhouse_prospective_live_shadow import (
    ClickHouseProspectiveLiveOutcomeSource,
    ClickHouseProspectiveLiveSnapshotSource,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations import (
    ClickHouseProspectiveLiveShadowStore,
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
class ClickHouseProspectiveLiveShadowRuntime:
    recorder: RecordProspectivePortfolioSnapshot
    outcome_worker: ProcessProspectiveLiveOutcomes
    snapshot_source: ClickHouseProspectiveLiveSnapshotSource
    store: ClickHouseProspectiveLiveShadowStore
    policy: ProspectiveScientificPolicy

    def run_once(
        self,
        *,
        now: datetime,
        snapshot_limit: int = 25,
        outcome_limit: int = 100,
    ) -> ProspectiveLiveShadowPassResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        snapshots = (
            self.snapshot_source.load_snapshots(
                as_of=now,
                policy=self.policy,
                limit=snapshot_limit,
            )
            if now.astimezone(timezone.utc).minute % 30 == 0
            else ()
        )
        ingested: list[ProspectivePortfolioIngestResult] = []
        for snapshot in snapshots:
            result = self.recorder.execute(snapshot)
            if len(result.observation_ids) != 6:
                raise RuntimeError(
                    "production live-shadow pass must seal six observations"
                )
            ingested.append(result)
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
    )


def main() -> None:
    configure_logging((os.getenv("LOG_LEVEL") or "INFO").strip())
    password = load_secret("CLICKHOUSE_PASSWORD")
    if password is None:
        raise ValueError("CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required")
    snapshot_limit = _env_int("PROSPECTIVE_LIVE_SNAPSHOT_BATCH_SIZE", 25)
    runtime = build_clickhouse_prospective_live_shadow_runtime(
        base_url=_required_env("CLICKHOUSE_HTTP_URL"),
        database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
        username=_required_env("CLICKHOUSE_USERNAME"),
        password=password,
        instrument_ids=_instrument_ids(snapshot_limit),
        timeout_seconds=_env_float("PROSPECTIVE_LIVE_CLICKHOUSE_TIMEOUT_SECONDS", 15.0),
        outcome_policy_version=(
            os.getenv(
                "PROSPECTIVE_LIVE_OUTCOME_POLICY_VERSION",
                DEFAULT_LIVE_OUTCOME_POLICY_VERSION,
            ).strip()
        ),
    )
    outcome_limit = _env_int("PROSPECTIVE_LIVE_OUTCOME_BATCH_SIZE", 100)
    poll_seconds = _env_float("PROSPECTIVE_LIVE_POLL_SECONDS", 60.0)
    logger.info("Starting prospective live-shadow ClickHouse worker")
    try:
        while True:
            result = runtime.run_once(
                now=datetime.now(tz=timezone.utc),
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
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        logger.info("Prospective live-shadow worker stopped by user")


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


if __name__ == "__main__":
    main()

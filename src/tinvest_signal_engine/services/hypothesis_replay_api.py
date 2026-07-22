"""Internal HTTP composition root for immutable hypothesis replay.

The service deliberately owns no broker client.  It composes the existing
application use cases with local cache and filesystem adapters, and persists
the asynchronous job envelope so work can be resumed after a process restart.
"""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from threading import RLock
from typing import Any, Literal, Mapping, Protocol, Sequence
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from tinvest_signal_engine.adapters.hypothesis_replay_evidence import (
    LocalReplayEvidenceReader,
)
from tinvest_signal_engine.adapters.composite_scientific_candle_cache import (
    ClickHouseScientificCandleSource,
    CompositeScientificCandleCache,
    VersionedScientificCandleSource,
)
from tinvest_signal_engine.adapters.jump_activity_replay import (
    JsonJumpReplayArtifactAdapter,
    ParquetCandleCacheAdapter,
)
from tinvest_signal_engine.adapters.local_hypothesis_replay import (
    ImmutableReplayArtifactStore,
    LocalCandleCache,
)
from tinvest_signal_engine.adapters.scientific_candle_replay import (
    ScientificCandleReplayArtifactAdapter,
)
from tinvest_signal_engine.adapters.prospective_scientific_replay import (
    ProspectiveScientificReplayArtifactAdapter,
)
from tinvest_signal_engine.adapters.file_scientific_combination_pipeline import (
    FileProspectiveScientificPartitionStage,
    FileScientificCombinationStreamingArtifacts,
)
from tinvest_signal_engine.application.historical_hypothesis_replay import (
    DEFAULT_LIQUID_UNIVERSE,
    HistoricalReplayRequest,
    RunHistoricalHypothesisReplay,
)
from tinvest_signal_engine.application.jump_activity_replay import (
    RunJumpActivityReplay,
)
from tinvest_signal_engine.application.scientific_candle_models import (
    BuildScientificCandleModelResearch,
    ScientificCandleResearchRequest,
)
from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificRequest,
    build_partitioned_prospective_scientific_research,
    build_prospective_scientific_research,
)
from tinvest_signal_engine.application.scientific_combination_evidence import (
    EvaluateScientificCombinationPartitions,
)
from tinvest_signal_engine.application.historical_hypothesis_replay import (
    HistoricalCandleCachePort,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import ReplayCostModel
from tinvest_signal_engine.domain.hypothesis_formulas import HypothesisId
from tinvest_signal_engine.domain.jump_activity_replay import CostModel, JumpReplayPolicy
from tinvest_signal_engine.domain.scientific_candle_models import (
    ScientificCandleHypothesis,
    ScientificCandlePolicy,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveHypothesis,
    ProspectiveScientificPolicy,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    ScientificCombinationId,
)
from tinvest_signal_engine.domain.scientific_replay_contract import (
    ReplaySourceDataState,
    SCIENTIFIC_REPLAY_CONTRACT_V1,
    scientific_replay_definition,
    scientific_replay_formula_fingerprint,
)


JobState = Literal["queued", "running", "completed", "failed"]
ALL_HYPOTHESES = tuple(item.short_id for item in SCIENTIFIC_REPLAY_CONTRACT_V1)
SCIENTIFIC_CANDLE_HYPOTHESES = frozenset({"H10", "H11", "H15", "H7V2"})
PROSPECTIVE_SCIENTIFIC_HYPOTHESES = frozenset(
    {"H3V2", "H4V2", "H7V3", "H12", "H15V2", "H16", "H17"}
)
COMBINATION_SOURCE_HYPOTHESES = (
    ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
    ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
    ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
    ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
    ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
    ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
    ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
)
COMBINATION_SOURCE_IDS = frozenset(
    item.value for item in COMBINATION_SOURCE_HYPOTHESES
)
SUPPORTED_HYPOTHESES = (
    frozenset(ALL_HYPOTHESES)
    | SCIENTIFIC_CANDLE_HYPOTHESES
    | PROSPECTIVE_SCIENTIFIC_HYPOTHESES
)
LEGACY_DEFAULT_HYPOTHESES = tuple(
    item for item in ALL_HYPOTHESES if item not in SCIENTIFIC_CANDLE_HYPOTHESES
)
GENERAL_HYPOTHESES = frozenset({"H1", "H2", "H5", "H6", "H7"})
JUMP_HYPOTHESES = frozenset({"H3", "H4"})
ORDERBOOK_HYPOTHESES = frozenset({"H8", "H9"})
JOB_SCHEMA_VERSION = 2


class ReplayCostModelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = Field(default="research-cost-v1.0.0", min_length=1, max_length=128)
    commission_bps: float = Field(default=3.0, ge=0.0)
    slippage_bps: float = Field(default=3.0, ge=0.0)
    entry_half_spread_bps: float = Field(default=2.0, ge=0.0)
    exit_half_spread_bps: float = Field(default=2.0, ge=0.0)

    @property
    def round_trip_bps(self) -> float:
        return (
            self.commission_bps
            + self.slippage_bps
            + self.entry_half_spread_bps
            + self.exit_half_spread_bps
        )


class StartReplayRequest(BaseModel):
    """Transport request mapped to existing application request objects."""

    model_config = ConfigDict(extra="forbid")

    hypothesis_ids: tuple[str, ...] = LEGACY_DEFAULT_HYPOTHESES
    tickers: tuple[str, ...] = ()
    liquid_universe: tuple[str, ...] = DEFAULT_LIQUID_UNIVERSE
    cost_model: ReplayCostModelRequest = ReplayCostModelRequest()
    resume: bool = True

    @field_validator("hypothesis_ids")
    @classmethod
    def validate_hypotheses(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({value.strip().upper() for value in values}))
        if not normalized:
            raise ValueError("at least one hypothesis must be selected")
        unsupported = sorted(set(normalized) - SUPPORTED_HYPOTHESES)
        if unsupported:
            raise ValueError(f"unsupported hypotheses: {unsupported}")
        jump_selection = set(normalized) & JUMP_HYPOTHESES
        if jump_selection and jump_selection != JUMP_HYPOTHESES:
            raise ValueError("H3 and H4 must be replayed together")
        return normalized

    @field_validator("tickers", "liquid_universe")
    @classmethod
    def normalize_tickers(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({value.strip().upper() for value in values if value.strip()}))
        return normalized


class ReplayAcceptedResponse(BaseModel):
    job_id: str
    status: JobState
    idempotency_key_hash: str
    run_fingerprint: str
    status_url: str
    result_url: str
    reused: bool


class ReplayStatusResponse(BaseModel):
    job_id: str
    status: JobState
    run_fingerprint: str
    hypothesis_ids: tuple[str, ...]
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    attempt: int
    recovered_after_restart: bool = False
    error: Mapping[str, str] | None = None


class ReplayResultResponse(BaseModel):
    job_id: str
    status: Literal["completed"]
    run_fingerprint: str
    dataset_fingerprint: str
    hypothesis_ids: tuple[str, ...]
    engines: tuple[Mapping[str, Any], ...]
    evidence: tuple["ReplayEvidenceResponse", ...]
    network_download_performed: Literal[False] = False


class ReplayEvidenceReasonCountResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason_code: str = Field(min_length=1)
    count: int = Field(gt=0)


class ReplayEvidenceConfidenceIntervalResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    lower: float
    estimate: float
    upper: float
    confidence_level: float = Field(gt=0.0, lt=1.0)


class ReplayEvidenceDiagnosticsV2Response(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    version: Literal["evidence-diagnostics-v2"]
    event_prevalence: float | None = Field(default=None, ge=0.0, le=1.0)
    eligible_event_count: int = Field(ge=0)
    matched_event_count: int = Field(ge=0)
    match_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    data_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    reasons_histogram: tuple[ReplayEvidenceReasonCountResponse, ...]
    primary_effect_estimate: float | None = None
    primary_effect_interval: ReplayEvidenceConfidenceIntervalResponse | None = None
    primary_p_value: float | None = Field(default=None, ge=0.0, le=1.0)
    descriptive_only: bool


class ReplayEvidenceResponse(BaseModel):
    """Strict product-facing aggregate for one requested hypothesis."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    hypothesis_id: str = Field(pattern=r"^H(?:[1-9]|1[0-7])(?:V[1-9][0-9]*)?$")
    catalog_hypothesis_id: str = Field(min_length=1)
    expected_direction: str = Field(min_length=1)
    market_phase: str = Field(min_length=1)
    source_data_state: Literal[
        "ready",
        "insufficient_history",
        "requires_live_orderbook",
        "stale_live_orderbook",
        "sequence_gap",
        "timestamp_desynchronization",
        "unavailable",
    ]
    decision: Literal["passed", "rejected", "inconclusive", "blocked_by_data"]
    independent_validation: bool
    cost_adjusted: bool
    sample_count: int = Field(ge=0)
    trading_days: int = Field(ge=0)
    generated_at: datetime
    artifact_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    dataset_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    formula_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    cost_model_version: str = Field(min_length=1, max_length=128)
    primary_metric_value: float | None = None
    matched_control_lift_ci95_lower: float | None = None
    matched_control_lift_ci95_upper: float | None = None
    matched_controls: int = Field(ge=0)
    controls_per_event: int = Field(ge=1)
    adjusted_p_value: float | None = Field(default=None, ge=0.0, le=1.0)
    stable_blocks: int = Field(ge=0)
    total_blocks: int = Field(ge=0)
    maximum_ticker_share: float | None = Field(default=None, ge=0.0, le=1.0)
    maximum_period_share: float | None = Field(default=None, ge=0.0, le=1.0)
    abstention_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    diagnostics_v2: ReplayEvidenceDiagnosticsV2Response | None = None
    horizons: tuple["ReplayHorizonEvidenceResponse", ...]
    claim_family: str = Field(default="directional", min_length=1)
    effect_unit: str = Field(
        default="cost_adjusted_signed_return_bps", min_length=1
    )
    claim_scope: str = Field(default="price_direction", min_length=1)
    target_metric: str = Field(default="forward_return", min_length=1)


class ReplayHorizonEvidenceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    horizon_seconds: int = Field(ge=1)
    evidence_scope: Literal["independent_gate", "descriptive_only", "not_evaluated"]
    source_data_state: Literal[
        "ready",
        "insufficient_history",
        "requires_live_orderbook",
        "stale_live_orderbook",
        "sequence_gap",
        "timestamp_desynchronization",
        "unavailable",
    ]
    decision: Literal[
        "passed", "rejected", "inconclusive", "blocked_by_data"
    ] | None
    sample_count: int = Field(ge=0)
    primary_metric_value: float | None


class ReplayRunner(Protocol):
    def dataset_fingerprint(self, *, as_of: datetime | None = None) -> str: ...

    def execute(
        self,
        request: StartReplayRequest,
        *,
        run_fingerprint: str,
        dataset_as_of: datetime | None = None,
    ) -> Mapping[str, Any]: ...

    def readiness(self) -> tuple[bool, str | None]: ...


class ReplayEvidenceReader(Protocol):
    def read_general(
        self,
        artifact_dir: str | Path,
        requested_hypotheses: Sequence[str],
        *,
        generated_at: str,
    ) -> tuple[Mapping[str, Any], ...]: ...

    def read_jump(
        self,
        artifact_dir: str | Path,
        requested_hypotheses: Sequence[str],
        *,
        generated_at: str,
    ) -> tuple[Mapping[str, Any], ...]: ...


@dataclass(frozen=True, slots=True)
class _Submission:
    record: Mapping[str, Any]
    reused: bool


class LocalReplayJobStore:
    """Atomic filesystem store for recoverable asynchronous job envelopes."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        probe = self.root / ".write-probe"
        _atomic_json(probe, {"ready": True})
        probe.unlink(missing_ok=True)

    def records(self) -> tuple[dict[str, Any], ...]:
        if not self.root.is_dir():
            return ()
        records: list[dict[str, Any]] = []
        for path in sorted(self.root.glob("*/state.json")):
            try:
                records.append(_read_object(path))
            except (OSError, ValueError, TypeError):
                continue
        return tuple(records)

    def load(self, job_id: str) -> dict[str, Any] | None:
        path = self._job_dir(job_id) / "state.json"
        return _read_object(path) if path.is_file() else None

    def save(self, record: Mapping[str, Any]) -> None:
        job_dir = self._job_dir(str(record["job_id"]))
        job_dir.mkdir(parents=True, exist_ok=True)
        _atomic_json(job_dir / "state.json", record)

    def save_result(self, job_id: str, result: Mapping[str, Any]) -> None:
        job_dir = self._job_dir(job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        _atomic_json(job_dir / "result.json", result)

    def load_result(self, job_id: str) -> dict[str, Any] | None:
        path = self._job_dir(job_id) / "result.json"
        return _read_object(path) if path.is_file() else None

    def _job_dir(self, job_id: str) -> Path:
        if not job_id.startswith("job-") or len(job_id) != 36:
            raise ValueError("invalid job id")
        return self.root / job_id


class ReplayJobManager:
    """Persist, schedule, recover, and inspect local replay jobs."""

    def __init__(
        self,
        *,
        runner: ReplayRunner,
        store: LocalReplayJobStore,
        max_workers: int = 1,
    ) -> None:
        self._runner = runner
        self._store = store
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="hypothesis-replay",
        )
        self._lock = RLock()
        self._futures: dict[str, Future[None]] = {}

    def readiness(self) -> tuple[bool, str | None]:
        try:
            self._store.ensure_ready()
            return self._runner.readiness()
        except (OSError, RuntimeError, ValueError) as exc:
            return False, str(exc)

    def recover(self) -> int:
        recovered = 0
        with self._lock:
            for record in self._store.records():
                if record.get("status") not in {"queued", "running"}:
                    continue
                record["status"] = "queued"
                record["recovered_after_restart"] = True
                record["updated_at"] = _now()
                self._store.save(record)
                self._schedule(str(record["job_id"]))
                recovered += 1
        return recovered

    def submit(self, request: StartReplayRequest, idempotency_key: str) -> _Submission:
        key = idempotency_key.strip()
        if not key or len(key) > 256:
            raise ValueError("Idempotency-Key must contain 1 to 256 characters")
        key_hash = _fingerprint({"idempotency_key": key})
        request_payload = request.model_dump(mode="json")
        with self._lock:
            for existing in self._store.records():
                if existing.get("idempotency_key_hash") != key_hash:
                    continue
                if existing.get("request") != request_payload:
                    raise IdempotencyConflict
                return _Submission(existing, reused=True)
            dataset_as_of = datetime.now(timezone.utc)
            dataset_fingerprint = self._runner.dataset_fingerprint(as_of=dataset_as_of)
            run_fingerprint = _fingerprint({
                "schema_version": JOB_SCHEMA_VERSION,
                "dataset_fingerprint": dataset_fingerprint,
                "dataset_as_of": dataset_as_of.isoformat(),
                "request": request_payload,
            })
            job_id = "job-" + sha256(f"{key_hash}:{run_fingerprint}".encode()).hexdigest()[:32]
            now = _now()
            record: dict[str, Any] = {
                "schema_version": JOB_SCHEMA_VERSION,
                "job_id": job_id,
                "status": "queued",
                "idempotency_key_hash": key_hash,
                "run_fingerprint": run_fingerprint,
                "dataset_fingerprint": dataset_fingerprint,
                "dataset_as_of": dataset_as_of.isoformat(),
                "request": request_payload,
                "created_at": now,
                "updated_at": now,
                "started_at": None,
                "finished_at": None,
                "attempt": 0,
                "recovered_after_restart": False,
                "error": None,
            }
            self._store.save(record)
            self._schedule(job_id)
            return _Submission(record, reused=False)

    def status(self, job_id: str) -> Mapping[str, Any] | None:
        return self._store.load(job_id)

    def result(self, job_id: str) -> Mapping[str, Any] | None:
        return self._store.load_result(job_id)

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _schedule(self, job_id: str) -> None:
        current = self._futures.get(job_id)
        if current is not None and not current.done():
            return
        self._futures[job_id] = self._executor.submit(self._run, job_id)

    def _run(self, job_id: str) -> None:
        with self._lock:
            record = self._store.load(job_id)
            if record is None or record.get("status") == "completed":
                return
            record["status"] = "running"
            record["started_at"] = _now()
            record["updated_at"] = record["started_at"]
            record["attempt"] = int(record.get("attempt", 0)) + 1
            record["error"] = None
            self._store.save(record)
        try:
            request = StartReplayRequest.model_validate(record["request"])
            result = self._runner.execute(
                request,
                run_fingerprint=str(record["run_fingerprint"]),
                dataset_as_of=datetime.fromisoformat(
                    str(record.get("dataset_as_of", record["created_at"]))
                ),
            )
            completed = {
                "job_id": job_id,
                "status": "completed",
                "run_fingerprint": record["run_fingerprint"],
                "dataset_fingerprint": record["dataset_fingerprint"],
                "hypothesis_ids": request.hypothesis_ids,
                "engines": result["engines"],
                "evidence": result["evidence"],
                "network_download_performed": False,
            }
            self._store.save_result(job_id, completed)
            with self._lock:
                record = self._store.load(job_id) or record
                record["status"] = "completed"
                record["finished_at"] = _now()
                record["updated_at"] = record["finished_at"]
                self._store.save(record)
        except Exception as exc:  # background boundary must seal failures for polling
            with self._lock:
                record = self._store.load(job_id) or record
                record["status"] = "failed"
                record["finished_at"] = _now()
                record["updated_at"] = record["finished_at"]
                record["error"] = {
                    "code": "replay_execution_failed",
                    "message": str(exc)[:1000],
                }
                self._store.save(record)


class IdempotencyConflict(RuntimeError):
    """The same idempotency key was used with a different immutable input."""


class LocalHypothesisPortfolioRunner:
    """Composition adapter for the H1-H9 scientific replay portfolio."""

    def __init__(
        self,
        *,
        cache_dir: str | Path,
        artifact_root: str | Path,
        evidence_reader: ReplayEvidenceReader | None = None,
        scientific_artifacts: ScientificCandleReplayArtifactAdapter | None = None,
        prospective_artifacts: ProspectiveScientificReplayArtifactAdapter | None = None,
        live_candles: VersionedScientificCandleSource | None = None,
    ) -> None:
        self._cache_dir = Path(cache_dir)
        self._artifact_root = Path(artifact_root)
        self._descriptor_cache = LocalCandleCache(self._cache_dir)
        self._live_candles = live_candles
        self._evidence_reader = evidence_reader or LocalReplayEvidenceReader()
        self._scientific_artifacts = scientific_artifacts or (
            ScientificCandleReplayArtifactAdapter(
                self._artifact_root / "h10-h11-h15-h7v2"
            )
        )
        self._prospective_artifacts = prospective_artifacts or (
            ProspectiveScientificReplayArtifactAdapter(
                self._artifact_root / "h3v2-h4v2-h7v3-h15v2-h16-h17"
            )
        )

    def _candle_cache(self, as_of: datetime | None = None) -> HistoricalCandleCachePort:
        if self._live_candles is None:
            return self._descriptor_cache
        return CompositeScientificCandleCache(
            historical=LocalCandleCache(self._cache_dir),
            live=self._live_candles,
            as_of=as_of or datetime.now(timezone.utc),
        )

    def dataset_fingerprint(self, *, as_of: datetime | None = None) -> str:
        return self._candle_cache(as_of).describe().dataset_fingerprint

    def readiness(self) -> tuple[bool, str | None]:
        try:
            descriptor = self._descriptor_cache.describe()
            if descriptor.partition_count <= 0:
                return False, "candle cache is empty"
            self._artifact_root.mkdir(parents=True, exist_ok=True)
        except (OSError, RuntimeError, ValueError) as exc:
            return False, str(exc)
        return True, None

    def execute(
        self,
        request: StartReplayRequest,
        *,
        run_fingerprint: str,
        dataset_as_of: datetime | None = None,
    ) -> Mapping[str, Any]:
        candle_cache = self._candle_cache(dataset_as_of)
        engines: list[Mapping[str, Any]] = []
        evidence: list[Mapping[str, Any]] = []
        generated_at = _now()
        requested_general = tuple(
            HypothesisId(value)
            for value in request.hypothesis_ids
            if value in GENERAL_HYPOTHESES
        )
        if requested_general:
            executed_general = tuple(
                HypothesisId(value) for value in sorted(GENERAL_HYPOTHESES)
            )
            execution = RunHistoricalHypothesisReplay(
                cache=candle_cache,
                artifacts=ImmutableReplayArtifactStore(self._artifact_root / "h1-h2-h5-h6-h7"),
            ).execute(HistoricalReplayRequest(
                selected_hypotheses=executed_general,
                cost_model=ReplayCostModel(
                    version=request.cost_model.version,
                    commission_bps=request.cost_model.commission_bps,
                    slippage_bps=request.cost_model.slippage_bps,
                    half_spread_entry_bps=request.cost_model.entry_half_spread_bps,
                    half_spread_exit_bps=request.cost_model.exit_half_spread_bps,
                ),
                liquid_universe=request.liquid_universe,
                resume=request.resume,
            ))
            engines.append({
                "engine": "scientific_candle_replay",
                "hypothesis_ids": tuple(item.value for item in requested_general),
                "requested_hypothesis_ids": tuple(
                    item.value for item in requested_general
                ),
                "executed_hypothesis_ids": tuple(
                    item.value for item in executed_general
                ),
                "application_run_id": execution.completion.run_id,
                "artifact_fingerprint": execution.completion.artifact_fingerprint,
                "artifact_uri": str(
                    (self._artifact_root / "h1-h2-h5-h6-h7" / execution.completion.run_id.removeprefix("sha256:")).resolve()
                ),
                "resumed": execution.completion.resumed,
            })
            evidence.extend(self._evidence_reader.read_general(
                engines[-1]["artifact_uri"],
                tuple(item.value for item in requested_general),
                generated_at=generated_at,
            ))
        if set(request.hypothesis_ids) & JUMP_HYPOTHESES:
            jump = RunJumpActivityReplay(
                candle_cache=ParquetCandleCacheAdapter(self._cache_dir),
                artifacts=JsonJumpReplayArtifactAdapter(self._artifact_root / "h3-h4"),
            ).execute(
                policy=JumpReplayPolicy(
                    cost_model=CostModel(
                        version=request.cost_model.version,
                        round_trip_bps=request.cost_model.round_trip_bps,
                    )
                ),
                tickers=request.tickers or None,
            )
            engines.append({
                "engine": "jump_activity_replay",
                "hypothesis_ids": tuple(sorted(set(request.hypothesis_ids) & JUMP_HYPOTHESES)),
                "application_run_id": jump.run_id,
                "artifact_uri": jump.artifact_uri,
                "resumed": jump.reused,
            })
            evidence.extend(self._evidence_reader.read_jump(
                jump.artifact_uri,
                tuple(sorted(set(request.hypothesis_ids) & JUMP_HYPOTHESES)),
                generated_at=generated_at,
            ))
        requested_scientific = tuple(
            ScientificCandleHypothesis(item)
            for item in request.hypothesis_ids
            if item in SCIENTIFIC_CANDLE_HYPOTHESES
        )
        if requested_scientific:
            report = BuildScientificCandleModelResearch(
                candle_cache
            ).execute(
                ScientificCandleResearchRequest(
                    selected_hypotheses=requested_scientific,
                    market_universe=request.liquid_universe,
                    policy=ScientificCandlePolicy(
                        round_trip_cost_bps=request.cost_model.round_trip_bps
                    ),
                )
            )
            artifact = self._scientific_artifacts.save(
                report,
                requested_scientific,
                cost_model_version=request.cost_model.version,
            )
            engines.append({
                "engine": "next_scientific_candle_replay",
                "hypothesis_ids": tuple(item.value for item in requested_scientific),
                "application_run_id": report.report_fingerprint,
                "artifact_fingerprint": artifact.artifact_fingerprint,
                "artifact_uri": artifact.artifact_uri,
                "resumed": False,
            })
            evidence.extend(artifact.evidence)
        requested_prospective = tuple(
            ProspectiveHypothesis(item)
            for item in request.hypothesis_ids
            if item in PROSPECTIVE_SCIENTIFIC_HYPOTHESES
        )
        if requested_prospective:
            descriptor = candle_cache.describe()
            policy = ProspectiveScientificPolicy(
                round_trip_cost_bps=request.cost_model.round_trip_bps
            )
            partitioned = callable(
                getattr(candle_cache, "iter_ticker_partitions", None)
            )
            candles = None if partitioned else candle_cache.load()

            def build_one(
                hypothesis: ProspectiveHypothesis,
            ) -> Any:
                scientific_request = ProspectiveScientificRequest(
                    selected_hypotheses=(hypothesis,),
                    policy=policy,
                )
                if partitioned:
                    return build_partitioned_prospective_scientific_research(
                        candle_cache,  # type: ignore[arg-type]
                        dataset_fingerprint=descriptor.dataset_fingerprint,
                        request=scientific_request,
                    )
                assert candles is not None
                return build_prospective_scientific_research(
                    candles,
                    dataset_fingerprint=descriptor.dataset_fingerprint,
                    request=scientific_request,
                )

            combinations_enabled = COMBINATION_SOURCE_IDS <= set(
                request.hypothesis_ids
            )
            combination_source = (
                FileProspectiveScientificPartitionStage(
                    self._artifact_root
                    / "scientific-combinations"
                    / "source"
                    / run_fingerprint.removeprefix("sha256:")
                )
                if combinations_enabled
                else None
            )

            def reports() -> Any:
                for hypothesis in requested_prospective:
                    report = build_one(hypothesis)
                    if (
                        combination_source is not None
                        and hypothesis in COMBINATION_SOURCE_HYPOTHESES
                    ):
                        combination_source.stage(
                            report,
                            cost_model_version=request.cost_model.version,
                        )
                    yield report

            artifact = self._prospective_artifacts.save_portfolio(
                reports(),
                requested_prospective,
                cost_model_version=request.cost_model.version,
            )
            engines.append({
                "engine": "prospective_scientific_replay",
                "hypothesis_ids": tuple(item.value for item in requested_prospective),
                "application_run_id": artifact.artifact_fingerprint,
                "artifact_fingerprint": artifact.artifact_fingerprint,
                "artifact_uri": artifact.artifact_uri,
                "resumed": False,
            })
            evidence.extend(artifact.evidence)
            if combination_source is not None:
                # H1/H2 have a legacy product evidence path, but C3 requires
                # their causal prospective observations.  Build and release
                # them one at a time after the main evidence adapter has
                # already discarded every requested report graph.
                for hypothesis in (
                    ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
                    ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
                ):
                    if hypothesis in requested_prospective:
                        continue
                    combination_source.stage(
                        build_one(hypothesis),
                        cost_model_version=request.cost_model.version,
                    )
                combination_completion = EvaluateScientificCombinationPartitions(
                    artifacts=FileScientificCombinationStreamingArtifacts(
                        self._artifact_root / "scientific-combinations" / "evidence"
                    )
                ).execute(
                    combination_source,
                    cost_model_version=request.cost_model.version,
                )
                engines.append({
                    "engine": "scientific_combination_evidence",
                    "combination_ids": tuple(
                        item.value for item in ScientificCombinationId
                    ),
                    "application_run_id": combination_completion.run_id,
                    "artifact_fingerprint": (
                        combination_completion.artifact.artifact_fingerprint
                    ),
                    "artifact_uri": combination_completion.artifact.artifact_uri,
                    "partition_count": combination_completion.partition_count,
                    "observation_count": combination_completion.observation_count,
                    "result_count": combination_completion.result_count,
                    "resumed": combination_completion.resumed,
                })
        requested_orderbook = tuple(
            item for item in request.hypothesis_ids if item in ORDERBOOK_HYPOTHESES
        )
        if requested_orderbook:
            engines.append({
                "engine": "live_orderbook_replay",
                "hypothesis_ids": requested_orderbook,
                "availability": "requires_live_orderbook",
                "resumed": False,
            })
            evidence.extend(
                _blocked_orderbook_evidence(
                    hypothesis_id,
                    dataset_fingerprint=candle_cache.describe().dataset_fingerprint,
                    cost_model_version=request.cost_model.version,
                    generated_at=generated_at,
                )
                for hypothesis_id in requested_orderbook
            )
        ordered = tuple(sorted(evidence, key=lambda item: str(item["hypothesis_id"])))
        if tuple(item["hypothesis_id"] for item in ordered) != request.hypothesis_ids:
            raise ValueError("replay must produce exactly one evidence row per hypothesis")
        return {
            "run_fingerprint": run_fingerprint,
            "engines": tuple(engines),
            "evidence": ordered,
        }


def _blocked_orderbook_evidence(
    hypothesis_id: str,
    *,
    dataset_fingerprint: str,
    cost_model_version: str,
    generated_at: str,
) -> Mapping[str, Any]:
    definition = scientific_replay_definition(hypothesis_id)
    formula_fingerprint = scientific_replay_formula_fingerprint(hypothesis_id)
    return {
        "hypothesis_id": hypothesis_id,
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "expected_direction": definition.expected_direction,
        "market_phase": definition.market_phase,
        "source_data_state": ReplaySourceDataState.REQUIRES_LIVE_ORDERBOOK.value,
        "decision": "blocked_by_data",
        "independent_validation": False,
        "cost_adjusted": False,
        "sample_count": 0,
        "trading_days": 0,
        "generated_at": generated_at,
        "artifact_fingerprint": formula_fingerprint,
        "dataset_fingerprint": dataset_fingerprint,
        "formula_fingerprint": formula_fingerprint,
        "cost_model_version": cost_model_version,
        "primary_metric_value": None,
        "matched_control_lift_ci95_lower": None,
        "matched_control_lift_ci95_upper": None,
        "matched_controls": 0,
        "controls_per_event": 5,
        "adjusted_p_value": None,
        "stable_blocks": 0,
        "total_blocks": 0,
        "maximum_ticker_share": None,
        "maximum_period_share": None,
        "abstention_rate": None,
        "horizons": tuple({
            "horizon_seconds": horizon,
            "evidence_scope": "not_evaluated",
            "source_data_state": (
                ReplaySourceDataState.REQUIRES_LIVE_ORDERBOOK.value
            ),
            "decision": "blocked_by_data",
            "sample_count": 0,
            "primary_metric_value": None,
        } for horizon in definition.horizons_seconds),
    }


def create_app(
    *,
    manager: ReplayJobManager,
    close_manager: bool = False,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        manager.recover()
        yield
        if close_manager:
            manager.close()

    app = FastAPI(
        title="Investment Signals hypothesis replay (internal)",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.get("/health", include_in_schema=False)
    def health() -> Mapping[str, str]:
        return {"status": "ok"}

    @app.get("/ready", include_in_schema=False)
    def ready(response: Response) -> Mapping[str, str]:
        is_ready, reason = manager.readiness()
        if not is_ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "not_ready", "reason": reason or "unknown"}
        return {"status": "ready"}

    @app.post(
        "/internal/v1/hypothesis-replays",
        response_model=ReplayAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def start_replay(
        payload: StartReplayRequest,
        idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=256),
    ) -> ReplayAcceptedResponse:
        try:
            submission = manager.submit(payload, idempotency_key)
        except IdempotencyConflict as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "idempotency_key_conflict"},
            ) from exc
        except (OSError, RuntimeError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"code": "replay_submission_rejected", "message": str(exc)},
            ) from exc
        record = submission.record
        job_id = str(record["job_id"])
        return ReplayAcceptedResponse(
            job_id=job_id,
            status=str(record["status"]),  # type: ignore[arg-type]
            idempotency_key_hash=str(record["idempotency_key_hash"]),
            run_fingerprint=str(record["run_fingerprint"]),
            status_url=f"/internal/v1/hypothesis-replays/{job_id}",
            result_url=f"/internal/v1/hypothesis-replays/{job_id}/result",
            reused=submission.reused,
        )

    @app.get(
        "/internal/v1/hypothesis-replays/{job_id}",
        response_model=ReplayStatusResponse,
    )
    def replay_status(job_id: str) -> ReplayStatusResponse:
        try:
            record = manager.status(job_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from exc
        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return _status_response(record)

    @app.get(
        "/internal/v1/hypothesis-replays/{job_id}/result",
        response_model=ReplayResultResponse | ReplayStatusResponse,
    )
    def replay_result(job_id: str, response: Response) -> Any:
        try:
            record = manager.status(job_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from exc
        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        if record["status"] == "failed":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "replay_failed", "error": record.get("error")},
            )
        result = manager.result(job_id)
        if record["status"] != "completed" or result is None:
            response.status_code = status.HTTP_202_ACCEPTED
            return _status_response(record)
        return ReplayResultResponse.model_validate(result)

    return app


def build_app(
    *,
    cache_dir: Path,
    state_dir: Path,
    artifact_dir: Path,
    clickhouse_url: str | None = None,
    clickhouse_database: str = "signal_engine",
    clickhouse_username: str = "investment_signals",
    clickhouse_password_file: Path | None = None,
) -> FastAPI:
    live_candles: VersionedScientificCandleSource | None = None
    if clickhouse_url:
        if clickhouse_password_file is None:
            raise ValueError(
                "clickhouse_password_file is required when ClickHouse live candles are enabled"
            )
        live_candles = ClickHouseScientificCandleSource(
            base_url=clickhouse_url,
            database=clickhouse_database,
            username=clickhouse_username,
            password=clickhouse_password_file.read_text(encoding="utf-8").strip(),
        )
    manager = ReplayJobManager(
        runner=LocalHypothesisPortfolioRunner(
            cache_dir=cache_dir,
            artifact_root=artifact_dir,
            live_candles=live_candles,
        ),
        store=LocalReplayJobStore(state_dir),
    )
    return create_app(manager=manager, close_manager=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Internal API for local scientific hypothesis replay",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_candles/v1"),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("var/research/hypothesis-replay-api/jobs"),
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("var/research/hypothesis-replay-api/artifacts"),
    )
    parser.add_argument(
        "--host",
        choices=("127.0.0.1", "0.0.0.0"),
        default="127.0.0.1",
    )
    parser.add_argument("--port", type=int, default=18181)
    parser.add_argument("--clickhouse-url")
    parser.add_argument("--clickhouse-database", default="signal_engine")
    parser.add_argument("--clickhouse-username", default="investment_signals")
    parser.add_argument("--clickhouse-password-file", type=Path)
    args = parser.parse_args(argv)
    import uvicorn

    uvicorn.run(
        build_app(
            cache_dir=args.cache_dir,
            state_dir=args.state_dir,
            artifact_dir=args.artifact_dir,
            clickhouse_url=args.clickhouse_url,
            clickhouse_database=args.clickhouse_database,
            clickhouse_username=args.clickhouse_username,
            clickhouse_password_file=args.clickhouse_password_file,
        ),
        host=args.host,
        port=args.port,
    )
    return 0


def _status_response(record: Mapping[str, Any]) -> ReplayStatusResponse:
    request = StartReplayRequest.model_validate(record["request"])
    return ReplayStatusResponse(
        job_id=str(record["job_id"]),
        status=str(record["status"]),  # type: ignore[arg-type]
        run_fingerprint=str(record["run_fingerprint"]),
        hypothesis_ids=request.hypothesis_ids,
        created_at=str(record["created_at"]),
        updated_at=str(record["updated_at"]),
        started_at=_optional_string(record.get("started_at")),
        finished_at=_optional_string(record.get("finished_at")),
        attempt=int(record["attempt"]),
        recovered_after_restart=bool(record.get("recovered_after_restart", False)),
        error=record.get("error"),
    )


def _optional_string(value: object) -> str | None:
    return None if value is None else str(value)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object expected: {path}")
    return payload


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


if __name__ == "__main__":
    raise SystemExit(main())

"""Recoverable application orchestration for a scientific-hypothesis portfolio.

This module deliberately does not calculate market features, replay candles, or
reimplement statistical evidence.  Those behaviours remain behind application
ports and are supplied by the existing replay and evidence engines at a
composition root.  The use case owns only portfolio coordination, immutable
progress, preregistration checks, and deterministic idempotency.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from hashlib import sha256
import json
from typing import Protocol, Sequence

from tinvest_signal_engine.domain.scientific_hypotheses import (
    ReplicationEvidence,
    ScientificHypothesis,
)


class EvidenceGateTier(str, Enum):
    INTERMEDIATE = "intermediate"
    STRICT = "strict"


class EvidenceGateDecision(str, Enum):
    PASSED = "passed"
    REJECTED = "rejected"
    INCONCLUSIVE = "inconclusive"
    BLOCKED_BY_DATA = "blocked_by_data"


class PortfolioItemState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class PortfolioRunState(str, Enum):
    RUNNING = "running"
    PARTIAL = "partial"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class EvidenceGatePolicyReference:
    """Immutable identity of a policy evaluated by the evidence engine."""

    policy_id: str
    version: str
    fingerprint: str

    def __post_init__(self) -> None:
        if not all(
            value.strip() for value in (self.policy_id, self.version, self.fingerprint)
        ):
            raise ValueError("evidence gate policy identity must not be empty")


@dataclass(frozen=True, slots=True)
class PortfolioHypothesisRegistration:
    """One sealed hypothesis version and its release-specific primary test."""

    replay_key: str
    hypothesis: ScientificHypothesis
    family_id: str
    primary_metric: str
    primary_horizon_seconds: int
    intermediate_gate: EvidenceGatePolicyReference
    strict_gate: EvidenceGatePolicyReference

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (self.replay_key, self.family_id, self.primary_metric)
        ):
            raise ValueError("portfolio registration identity must not be empty")
        if self.primary_horizon_seconds <= 0:
            raise ValueError("primary horizon must be positive")
        preregistration = self.hypothesis.preregistration
        if preregistration is None or not preregistration.sealed:
            raise ValueError("portfolio hypothesis must be preregistered and sealed")
        if (
            preregistration.hypothesis_id != self.hypothesis.hypothesis_id
            or preregistration.hypothesis_version != self.hypothesis.version
        ):
            raise ValueError("portfolio preregistration identity does not match hypothesis")
        if (
            preregistration.expected_direction != self.hypothesis.expected_direction
            or preregistration.market_phase != self.hypothesis.market_phase
            or preregistration.horizon_seconds != self.hypothesis.horizon_seconds
            or preregistration.abstention_conditions
            != self.hypothesis.abstention_conditions
        ):
            raise ValueError("portfolio preregistration parameters do not match hypothesis")
        if self.primary_horizon_seconds not in preregistration.horizon_seconds:
            raise ValueError("primary horizon is absent from sealed preregistration")
        if self.intermediate_gate == self.strict_gate:
            raise ValueError("intermediate and strict evidence gates must be distinct")

    @property
    def item_key(self) -> str:
        return f"{self.hypothesis.hypothesis_id}@{self.hypothesis.version}"

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            {
                "family_id": self.family_id,
                "hypothesis_fingerprint": (
                    self.hypothesis.sealed_parameters_fingerprint()
                ),
                "hypothesis_id": self.hypothesis.hypothesis_id,
                "hypothesis_version": self.hypothesis.version,
                "intermediate_gate": _gate_policy_payload(self.intermediate_gate),
                "primary_horizon_seconds": self.primary_horizon_seconds,
                "primary_metric": self.primary_metric,
                "replay_key": self.replay_key,
                "strict_gate": _gate_policy_payload(self.strict_gate),
            }
        )


@dataclass(frozen=True, slots=True)
class RunHypothesisPortfolioRequest:
    dataset_fingerprint: str
    cost_model_version: str
    replay_engine_version: str
    hypotheses: tuple[PortfolioHypothesisRegistration, ...]

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.dataset_fingerprint,
                self.cost_model_version,
                self.replay_engine_version,
            )
        ):
            raise ValueError("portfolio input identity must not be empty")
        if not self.hypotheses:
            raise ValueError("portfolio must contain at least one hypothesis")
        item_keys = tuple(item.item_key for item in self.hypotheses)
        replay_keys = tuple(item.replay_key for item in self.hypotheses)
        if len(item_keys) != len(set(item_keys)):
            raise ValueError("portfolio hypothesis versions must be unique")
        if len(replay_keys) != len(set(replay_keys)):
            raise ValueError("portfolio replay keys must be unique")
        for registration in self.hypotheses:
            preregistration = registration.hypothesis.preregistration
            if (
                preregistration is None
                or preregistration.cost_model_version != self.cost_model_version
            ):
                raise ValueError("portfolio cost model differs from preregistration")

    @property
    def ordered_hypotheses(self) -> tuple[PortfolioHypothesisRegistration, ...]:
        return tuple(sorted(self.hypotheses, key=lambda item: item.item_key))

    @property
    def input_fingerprint(self) -> str:
        return _fingerprint(
            {
                "cost_model_version": self.cost_model_version,
                "dataset_fingerprint": self.dataset_fingerprint,
                "hypotheses": tuple(
                    item.fingerprint for item in self.ordered_hypotheses
                ),
                "replay_engine_version": self.replay_engine_version,
            }
        )

    @property
    def run_id(self) -> str:
        return f"hypothesis-portfolio-{self.input_fingerprint.removeprefix('sha256:')}"


@dataclass(frozen=True, slots=True)
class ReplayHypothesisCommand:
    run_id: str
    item_key: str
    replay_key: str
    hypothesis_id: str
    hypothesis_version: str
    registration_fingerprint: str
    dataset_fingerprint: str
    cost_model_version: str
    replay_engine_version: str
    primary_metric: str
    primary_horizon_seconds: int


@dataclass(frozen=True, slots=True)
class EvidenceGateAssessment:
    tier: EvidenceGateTier
    decision: EvidenceGateDecision
    policy_fingerprint: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.policy_fingerprint.strip():
            raise ValueError("gate assessment policy fingerprint must not be empty")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("gate assessment reason codes must be unique")


@dataclass(frozen=True, slots=True)
class PortfolioItemResult:
    item_key: str
    replay_key: str
    registration_fingerprint: str
    state: PortfolioItemState
    attempts: int = 0
    evidence: ReplicationEvidence | None = None
    intermediate_assessment: EvidenceGateAssessment | None = None
    strict_assessment: EvidenceGateAssessment | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.item_key,
                self.replay_key,
                self.registration_fingerprint,
            )
        ):
            raise ValueError("portfolio item identity must not be empty")
        if self.attempts < 0:
            raise ValueError("portfolio item attempts must not be negative")
        if self.state is PortfolioItemState.COMPLETED and (
            self.evidence is None
            or self.intermediate_assessment is None
            or self.strict_assessment is None
            or self.failure_code is not None
        ):
            raise ValueError("completed portfolio item requires evidence and both gates")
        if self.state is PortfolioItemState.FAILED and not self.failure_code:
            raise ValueError("failed portfolio item requires a failure code")
        if self.state in {PortfolioItemState.PENDING, PortfolioItemState.RUNNING} and (
            self.evidence is not None
            or self.intermediate_assessment is not None
            or self.strict_assessment is not None
            or self.failure_code is not None
        ):
            raise ValueError("unfinished portfolio item must not expose a result")


@dataclass(frozen=True, slots=True)
class PortfolioProgress:
    total: int
    completed: int
    failed: int
    running: int
    pending: int

    @property
    def finished(self) -> int:
        return self.completed + self.failed

    @property
    def fraction(self) -> float:
        return self.finished / self.total


@dataclass(frozen=True, slots=True)
class HypothesisPortfolioSnapshot:
    run_id: str
    input_fingerprint: str
    state: PortfolioRunState
    revision: int
    items: tuple[PortfolioItemResult, ...]

    def __post_init__(self) -> None:
        if not self.run_id.strip() or not self.input_fingerprint.strip():
            raise ValueError("portfolio snapshot identity must not be empty")
        if self.revision <= 0:
            raise ValueError("portfolio snapshot revision must be positive")
        if not self.items:
            raise ValueError("portfolio snapshot must contain items")
        keys = tuple(item.item_key for item in self.items)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("portfolio snapshot items must be sorted and unique")
        if self.state is PortfolioRunState.COMPLETED and any(
            item.state is not PortfolioItemState.COMPLETED for item in self.items
        ):
            raise ValueError("completed portfolio must contain only completed items")
        if self.state is PortfolioRunState.PARTIAL and not any(
            item.state is PortfolioItemState.FAILED for item in self.items
        ):
            raise ValueError("partial portfolio requires at least one failed item")

    @property
    def progress(self) -> PortfolioProgress:
        counts = {
            state: sum(item.state is state for item in self.items)
            for state in PortfolioItemState
        }
        return PortfolioProgress(
            total=len(self.items),
            completed=counts[PortfolioItemState.COMPLETED],
            failed=counts[PortfolioItemState.FAILED],
            running=counts[PortfolioItemState.RUNNING],
            pending=counts[PortfolioItemState.PENDING],
        )


@dataclass(frozen=True, slots=True)
class HypothesisPortfolioExecution:
    snapshot: HypothesisPortfolioSnapshot
    resumed: bool
    executed_item_keys: tuple[str, ...]


class HypothesisReplayPort(Protocol):
    """Delegate one sealed version to an existing replay engine."""

    def replay(self, command: ReplayHypothesisCommand) -> ReplicationEvidence: ...


class PortfolioEvidenceGatePort(Protocol):
    """Delegate assessment to the existing evidence/admission engine."""

    def assess(
        self,
        *,
        tier: EvidenceGateTier,
        registration: PortfolioHypothesisRegistration,
        evidence: ReplicationEvidence,
        policy: EvidenceGatePolicyReference,
    ) -> EvidenceGateAssessment: ...


class HypothesisPortfolioStorePort(Protocol):
    def load(self, run_id: str) -> HypothesisPortfolioSnapshot | None: ...

    def save(
        self,
        snapshot: HypothesisPortfolioSnapshot,
        *,
        expected_revision: int | None,
    ) -> None: ...


class HypothesisPortfolioProgressPort(Protocol):
    def publish(self, snapshot: HypothesisPortfolioSnapshot) -> None: ...


class PortfolioItemExecutionError(RuntimeError):
    """A safe, retryable failure reported by a replay/evidence adapter."""

    def __init__(self, failure_code: str) -> None:
        if not failure_code.strip():
            raise ValueError("portfolio failure code must not be empty")
        super().__init__(failure_code)
        self.failure_code = failure_code


class RunHypothesisPortfolio:
    """Run, persist, resume, and selectively retry a sealed portfolio."""

    def __init__(
        self,
        *,
        replay: HypothesisReplayPort,
        evidence_gates: PortfolioEvidenceGatePort,
        store: HypothesisPortfolioStorePort,
        progress: HypothesisPortfolioProgressPort,
    ) -> None:
        self._replay = replay
        self._evidence_gates = evidence_gates
        self._store = store
        self._progress = progress

    def execute(
        self, request: RunHypothesisPortfolioRequest
    ) -> HypothesisPortfolioExecution:
        snapshot = self._store.load(request.run_id)
        resumed = snapshot is not None
        if snapshot is None:
            snapshot = self._initial_snapshot(request)
            self._store.save(snapshot, expected_revision=None)
            self._progress.publish(snapshot)
        else:
            self._validate_resume(snapshot, request)

        if snapshot.state is PortfolioRunState.COMPLETED:
            return HypothesisPortfolioExecution(
                snapshot=snapshot,
                resumed=True,
                executed_item_keys=(),
            )

        registrations = {item.item_key: item for item in request.ordered_hypotheses}
        executed: list[str] = []
        for item in snapshot.items:
            if item.state is PortfolioItemState.COMPLETED:
                continue
            registration = registrations[item.item_key]
            snapshot = self._start_item(snapshot, item.item_key)
            executed.append(item.item_key)
            try:
                completed = self._execute_item(request, registration, snapshot)
            except Exception as exc:  # isolate one hypothesis from the portfolio
                completed = self._failed_item(
                    _item(snapshot, item.item_key),
                    _failure_code(exc),
                )
            snapshot = self._replace_and_persist(snapshot, completed)

        final_state = (
            PortfolioRunState.PARTIAL
            if any(item.state is PortfolioItemState.FAILED for item in snapshot.items)
            else PortfolioRunState.COMPLETED
        )
        if snapshot.state is not final_state:
            snapshot = self._persist(replace(snapshot, state=final_state))
        return HypothesisPortfolioExecution(
            snapshot=snapshot,
            resumed=resumed,
            executed_item_keys=tuple(executed),
        )

    def _execute_item(
        self,
        request: RunHypothesisPortfolioRequest,
        registration: PortfolioHypothesisRegistration,
        snapshot: HypothesisPortfolioSnapshot,
    ) -> PortfolioItemResult:
        evidence = self._replay.replay(
            ReplayHypothesisCommand(
                run_id=request.run_id,
                item_key=registration.item_key,
                replay_key=registration.replay_key,
                hypothesis_id=registration.hypothesis.hypothesis_id,
                hypothesis_version=registration.hypothesis.version,
                registration_fingerprint=registration.fingerprint,
                dataset_fingerprint=request.dataset_fingerprint,
                cost_model_version=request.cost_model_version,
                replay_engine_version=request.replay_engine_version,
                primary_metric=registration.primary_metric,
                primary_horizon_seconds=registration.primary_horizon_seconds,
            )
        )
        _validate_evidence(evidence, request, registration)
        intermediate = self._assess(
            EvidenceGateTier.INTERMEDIATE,
            registration,
            evidence,
            registration.intermediate_gate,
        )
        strict = self._assess(
            EvidenceGateTier.STRICT,
            registration,
            evidence,
            registration.strict_gate,
        )
        if (
            strict.decision is EvidenceGateDecision.PASSED
            and intermediate.decision is not EvidenceGateDecision.PASSED
        ):
            raise PortfolioItemExecutionError("inconsistent_evidence_gate_decisions")
        current = _item(snapshot, registration.item_key)
        return replace(
            current,
            state=PortfolioItemState.COMPLETED,
            evidence=evidence,
            intermediate_assessment=intermediate,
            strict_assessment=strict,
            failure_code=None,
        )

    def _assess(
        self,
        tier: EvidenceGateTier,
        registration: PortfolioHypothesisRegistration,
        evidence: ReplicationEvidence,
        policy: EvidenceGatePolicyReference,
    ) -> EvidenceGateAssessment:
        result = self._evidence_gates.assess(
            tier=tier,
            registration=registration,
            evidence=evidence,
            policy=policy,
        )
        if result.tier is not tier or result.policy_fingerprint != policy.fingerprint:
            raise PortfolioItemExecutionError("evidence_gate_contract_mismatch")
        return result

    def _initial_snapshot(
        self, request: RunHypothesisPortfolioRequest
    ) -> HypothesisPortfolioSnapshot:
        return HypothesisPortfolioSnapshot(
            run_id=request.run_id,
            input_fingerprint=request.input_fingerprint,
            state=PortfolioRunState.RUNNING,
            revision=1,
            items=tuple(
                PortfolioItemResult(
                    item_key=registration.item_key,
                    replay_key=registration.replay_key,
                    registration_fingerprint=registration.fingerprint,
                    state=PortfolioItemState.PENDING,
                )
                for registration in request.ordered_hypotheses
            ),
        )

    def _start_item(
        self, snapshot: HypothesisPortfolioSnapshot, item_key: str
    ) -> HypothesisPortfolioSnapshot:
        current = _item(snapshot, item_key)
        started = replace(
            current,
            state=PortfolioItemState.RUNNING,
            attempts=current.attempts + 1,
            evidence=None,
            intermediate_assessment=None,
            strict_assessment=None,
            failure_code=None,
        )
        running = replace(snapshot, state=PortfolioRunState.RUNNING)
        return self._replace_and_persist(running, started)

    def _replace_and_persist(
        self,
        snapshot: HypothesisPortfolioSnapshot,
        item: PortfolioItemResult,
    ) -> HypothesisPortfolioSnapshot:
        items = tuple(
            item if current.item_key == item.item_key else current
            for current in snapshot.items
        )
        return self._persist(replace(snapshot, items=items))

    def _persist(
        self, snapshot: HypothesisPortfolioSnapshot
    ) -> HypothesisPortfolioSnapshot:
        previous_revision = snapshot.revision
        updated = replace(snapshot, revision=previous_revision + 1)
        self._store.save(updated, expected_revision=previous_revision)
        self._progress.publish(updated)
        return updated

    @staticmethod
    def _validate_resume(
        snapshot: HypothesisPortfolioSnapshot,
        request: RunHypothesisPortfolioRequest,
    ) -> None:
        if snapshot.input_fingerprint != request.input_fingerprint:
            raise ValueError("stored portfolio input fingerprint does not match request")
        expected = {
            item.item_key: item.fingerprint for item in request.ordered_hypotheses
        }
        stored = {
            item.item_key: item.registration_fingerprint for item in snapshot.items
        }
        if stored != expected:
            raise ValueError("stored portfolio registrations do not match request")

    @staticmethod
    def _failed_item(
        item: PortfolioItemResult, failure_code: str
    ) -> PortfolioItemResult:
        return replace(
            item,
            state=PortfolioItemState.FAILED,
            evidence=None,
            intermediate_assessment=None,
            strict_assessment=None,
            failure_code=failure_code,
        )


def _validate_evidence(
    evidence: ReplicationEvidence,
    request: RunHypothesisPortfolioRequest,
    registration: PortfolioHypothesisRegistration,
) -> None:
    if (
        evidence.hypothesis_id != registration.hypothesis.hypothesis_id
        or evidence.hypothesis_version != registration.hypothesis.version
    ):
        raise PortfolioItemExecutionError("replay_evidence_identity_mismatch")
    if evidence.dataset_fingerprint != request.dataset_fingerprint:
        raise PortfolioItemExecutionError("replay_dataset_fingerprint_mismatch")
    if evidence.cost_model_version != request.cost_model_version:
        raise PortfolioItemExecutionError("replay_cost_model_version_mismatch")


def _item(
    snapshot: HypothesisPortfolioSnapshot, item_key: str
) -> PortfolioItemResult:
    try:
        return next(item for item in snapshot.items if item.item_key == item_key)
    except StopIteration as exc:
        raise ValueError(f"unknown portfolio item: {item_key}") from exc


def _failure_code(exc: Exception) -> str:
    if isinstance(exc, PortfolioItemExecutionError):
        return exc.failure_code
    return "unexpected_hypothesis_execution_failure"


def _gate_policy_payload(policy: EvidenceGatePolicyReference) -> dict[str, str]:
    return {
        "fingerprint": policy.fingerprint,
        "policy_id": policy.policy_id,
        "version": policy.version,
    }


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"


def portfolio_registration_fingerprint(
    registration: PortfolioHypothesisRegistration,
) -> str:
    return registration.fingerprint


def portfolio_input_fingerprint(request: RunHypothesisPortfolioRequest) -> str:
    return request.input_fingerprint


def completed_items(
    snapshot: HypothesisPortfolioSnapshot,
) -> Sequence[PortfolioItemResult]:
    return tuple(
        item for item in snapshot.items if item.state is PortfolioItemState.COMPLETED
    )

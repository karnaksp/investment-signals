"""Immutable filesystem adapters for recoverable hypothesis portfolios."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
from math import isfinite
import os
from pathlib import Path
import re
from typing import Any, Iterator, Mapping
from uuid import uuid4

from tinvest_signal_engine.application.hypothesis_portfolio_runner import (
    EvidenceGateAssessment,
    EvidenceGateDecision,
    EvidenceGateTier,
    HypothesisPortfolioSnapshot,
    PortfolioItemResult,
    PortfolioItemState,
    PortfolioRunState,
)
from tinvest_signal_engine.domain.scientific_hypotheses import (
    ReplicationEvidence,
    ReplicationResult,
)


SNAPSHOT_SCHEMA_VERSION = 1
PROGRESS_SCHEMA_VERSION = 1
_RUN_ID = re.compile(r"hypothesis-portfolio-[0-9a-f]{64}")
_REVISION_FILE = re.compile(r"[0-9]{20}\.json")


class PortfolioFileFormatError(ValueError):
    """A persisted portfolio file does not match the adapter contract."""


class PortfolioRevisionConflict(RuntimeError):
    """The caller attempted to replace a different immutable revision."""


class ImmutableFileHypothesisPortfolioStore:
    """Persist every snapshot revision once, using an optimistic CAS check."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def load(self, run_id: str) -> HypothesisPortfolioSnapshot | None:
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            return None
        with _exclusive_lock(self._root / ".store.lock"):
            return self._load_unlocked(run_dir)

    def save(
        self,
        snapshot: HypothesisPortfolioSnapshot,
        *,
        expected_revision: int | None,
    ) -> None:
        run_dir = self._run_dir(snapshot.run_id)
        with _exclusive_lock(self._root / ".store.lock"):
            current = self._load_unlocked(run_dir)
            if current is not None and current.revision == snapshot.revision:
                if current == snapshot:
                    return
                raise PortfolioRevisionConflict(
                    "immutable portfolio revision contains different state"
                )
            if expected_revision is None:
                if current is not None or snapshot.revision != 1:
                    raise PortfolioRevisionConflict(
                        "initial portfolio revision already exists"
                    )
            elif (
                current is None
                or current.revision != expected_revision
                or snapshot.revision != expected_revision + 1
            ):
                actual = None if current is None else current.revision
                raise PortfolioRevisionConflict(
                    f"portfolio revision conflict: expected={expected_revision}, "
                    f"actual={actual}, proposed={snapshot.revision}"
                )
            revisions = run_dir / "revisions"
            revisions.mkdir(parents=True, exist_ok=True, mode=0o700)
            payload = _snapshot_payload(snapshot)
            _write_immutable_json(
                revisions / _revision_name(snapshot.revision),
                payload,
            )

    def snapshots(self) -> tuple[HypothesisPortfolioSnapshot, ...]:
        """Return latest valid snapshots for progress repair at startup."""

        if not self._root.is_dir():
            return ()
        with _exclusive_lock(self._root / ".store.lock"):
            snapshots: list[HypothesisPortfolioSnapshot] = []
            for run_dir in sorted(self._root.glob("hypothesis-portfolio-*")):
                if not run_dir.is_dir() or _RUN_ID.fullmatch(run_dir.name) is None:
                    continue
                snapshot = self._load_unlocked(run_dir)
                if snapshot is not None:
                    snapshots.append(snapshot)
            return tuple(snapshots)

    @staticmethod
    def _load_unlocked(run_dir: Path) -> HypothesisPortfolioSnapshot | None:
        revision_files = _revision_files(run_dir / "revisions")
        if not revision_files:
            return None
        return _snapshot_from_payload(_read_object(revision_files[-1]))

    def _run_dir(self, run_id: str) -> Path:
        if _RUN_ID.fullmatch(run_id) is None:
            raise ValueError("invalid hypothesis portfolio run id")
        return self._root / run_id


class SafeFileHypothesisPortfolioProgress:
    """Publish a redacted progress projection and retain immutable revisions."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def publish(self, snapshot: HypothesisPortfolioSnapshot) -> None:
        run_dir = self._run_dir(snapshot.run_id)
        payload = _progress_payload(snapshot)
        with _exclusive_lock(self._root / ".progress.lock"):
            revisions = run_dir / "revisions"
            revision_files = _revision_files(revisions)
            if revision_files:
                latest_revision = int(revision_files[-1].stem)
                if latest_revision > snapshot.revision:
                    raise PortfolioRevisionConflict(
                        "progress projection is newer than supplied snapshot"
                    )
                if latest_revision == snapshot.revision:
                    if _encoded_json(_read_object(revision_files[-1])) != _encoded_json(
                        payload
                    ):
                        raise PortfolioRevisionConflict(
                            "immutable progress revision contains different state"
                        )
                    _atomic_json(run_dir / "latest.json", payload)
                    return
            revisions.mkdir(parents=True, exist_ok=True, mode=0o700)
            _write_immutable_json(
                revisions / _revision_name(snapshot.revision),
                payload,
            )
            _atomic_json(run_dir / "latest.json", payload)

    def read_latest(self, run_id: str) -> Mapping[str, Any] | None:
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            return None
        with _exclusive_lock(self._root / ".progress.lock"):
            revision_files = _revision_files(run_dir / "revisions")
            if not revision_files:
                return None
            return _read_object(revision_files[-1])

    def repair_from_store(
        self, store: ImmutableFileHypothesisPortfolioStore
    ) -> int:
        """Rebuild progress after a crash between state save and publication."""

        repaired = 0
        for snapshot in store.snapshots():
            current = self.read_latest(snapshot.run_id)
            current_revision = int(current["revision"]) if current is not None else 0
            if current_revision < snapshot.revision:
                self.publish(snapshot)
                repaired += 1
        return repaired

    def _run_dir(self, run_id: str) -> Path:
        if _RUN_ID.fullmatch(run_id) is None:
            raise ValueError("invalid hypothesis portfolio run id")
        return self._root / run_id


def _snapshot_payload(snapshot: HypothesisPortfolioSnapshot) -> dict[str, Any]:
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "kind": "hypothesis_portfolio_snapshot",
        "run_id": snapshot.run_id,
        "input_fingerprint": snapshot.input_fingerprint,
        "state": snapshot.state.value,
        "revision": snapshot.revision,
        "items": tuple(_item_payload(item) for item in snapshot.items),
    }


def _item_payload(item: PortfolioItemResult) -> dict[str, Any]:
    return {
        "item_key": item.item_key,
        "replay_key": item.replay_key,
        "registration_fingerprint": item.registration_fingerprint,
        "state": item.state.value,
        "attempts": item.attempts,
        "evidence": _evidence_payload(item.evidence),
        "intermediate_assessment": _assessment_payload(
            item.intermediate_assessment
        ),
        "strict_assessment": _assessment_payload(item.strict_assessment),
        "failure_code": item.failure_code,
    }


def _evidence_payload(evidence: ReplicationEvidence | None) -> dict[str, Any] | None:
    if evidence is None:
        return None
    return {
        "evidence_id": evidence.evidence_id,
        "hypothesis_id": evidence.hypothesis_id,
        "hypothesis_version": evidence.hypothesis_version,
        "market": evidence.market,
        "observed_at": evidence.observed_at.isoformat(),
        "result": evidence.result.value,
        "independent_validation": evidence.independent_validation,
        "trading_days": evidence.trading_days,
        "eligible_events": evidence.eligible_events,
        "cost_adjusted": evidence.cost_adjusted,
        "matched_controls_applied": evidence.matched_controls_applied,
        "multiple_testing_applied": evidence.multiple_testing_applied,
        "stability_checked": evidence.stability_checked,
        "mean_net_bps": evidence.mean_net_bps,
        "result_summary": evidence.result_summary,
        "artifact_uri": evidence.artifact_uri,
        "primary_metric": evidence.primary_metric,
        "controls_per_event": evidence.controls_per_event,
        "lift_ci_lower": evidence.lift_ci_lower,
        "lift_ci_upper": evidence.lift_ci_upper,
        "adjusted_p_value": evidence.adjusted_p_value,
        "stable_blocks": evidence.stable_blocks,
        "total_blocks": evidence.total_blocks,
        "max_ticker_share": evidence.max_ticker_share,
        "max_period_share": evidence.max_period_share,
        "dataset_fingerprint": evidence.dataset_fingerprint,
        "formula_fingerprint": evidence.formula_fingerprint,
        "cost_model_version": evidence.cost_model_version,
        "abstention_rate": evidence.abstention_rate,
        "success_rate": evidence.success_rate,
        "success_wilson_lower": evidence.success_wilson_lower,
    }


def _assessment_payload(
    assessment: EvidenceGateAssessment | None,
) -> dict[str, Any] | None:
    if assessment is None:
        return None
    return {
        "tier": assessment.tier.value,
        "decision": assessment.decision.value,
        "policy_fingerprint": assessment.policy_fingerprint,
        "reason_codes": assessment.reason_codes,
    }


def _snapshot_from_payload(payload: Mapping[str, Any]) -> HypothesisPortfolioSnapshot:
    if payload.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise PortfolioFileFormatError("unsupported portfolio snapshot schema")
    if payload.get("kind") != "hypothesis_portfolio_snapshot":
        raise PortfolioFileFormatError("unexpected portfolio snapshot kind")
    items = payload.get("items")
    if not isinstance(items, list):
        raise PortfolioFileFormatError("portfolio snapshot items must be a list")
    try:
        return HypothesisPortfolioSnapshot(
            run_id=_text(payload, "run_id"),
            input_fingerprint=_text(payload, "input_fingerprint"),
            state=PortfolioRunState(_text(payload, "state")),
            revision=_integer(payload, "revision"),
            items=tuple(_item_from_payload(item) for item in items),
        )
    except (TypeError, ValueError) as exc:
        raise PortfolioFileFormatError(str(exc)) from exc


def _item_from_payload(value: object) -> PortfolioItemResult:
    record = _mapping(value, "portfolio item")
    return PortfolioItemResult(
        item_key=_text(record, "item_key"),
        replay_key=_text(record, "replay_key"),
        registration_fingerprint=_text(record, "registration_fingerprint"),
        state=PortfolioItemState(_text(record, "state")),
        attempts=_integer(record, "attempts"),
        evidence=_evidence_from_payload(record.get("evidence")),
        intermediate_assessment=_assessment_from_payload(
            record.get("intermediate_assessment")
        ),
        strict_assessment=_assessment_from_payload(record.get("strict_assessment")),
        failure_code=_optional_text(record.get("failure_code")),
    )


def _evidence_from_payload(value: object) -> ReplicationEvidence | None:
    if value is None:
        return None
    record = _mapping(value, "replication evidence")
    observed_at = datetime.fromisoformat(_text(record, "observed_at"))
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise PortfolioFileFormatError("evidence observed_at must be timezone-aware")
    return ReplicationEvidence(
        evidence_id=_text(record, "evidence_id"),
        hypothesis_id=_text(record, "hypothesis_id"),
        hypothesis_version=_text(record, "hypothesis_version"),
        market=_text(record, "market"),
        observed_at=observed_at,
        result=ReplicationResult(_text(record, "result")),
        independent_validation=_boolean(record, "independent_validation"),
        trading_days=_integer(record, "trading_days"),
        eligible_events=_integer(record, "eligible_events"),
        cost_adjusted=_boolean(record, "cost_adjusted"),
        matched_controls_applied=_boolean(record, "matched_controls_applied"),
        multiple_testing_applied=_boolean(record, "multiple_testing_applied"),
        stability_checked=_boolean(record, "stability_checked"),
        mean_net_bps=_optional_number(record.get("mean_net_bps")),
        result_summary=_text(record, "result_summary", allow_empty=True),
        artifact_uri=_text(record, "artifact_uri", allow_empty=True),
        primary_metric=_text(record, "primary_metric", allow_empty=True),
        controls_per_event=_integer(record, "controls_per_event"),
        lift_ci_lower=_optional_number(record.get("lift_ci_lower")),
        lift_ci_upper=_optional_number(record.get("lift_ci_upper")),
        adjusted_p_value=_optional_number(record.get("adjusted_p_value")),
        stable_blocks=_integer(record, "stable_blocks"),
        total_blocks=_integer(record, "total_blocks"),
        max_ticker_share=_optional_number(record.get("max_ticker_share")),
        max_period_share=_optional_number(record.get("max_period_share")),
        dataset_fingerprint=_text(
            record, "dataset_fingerprint", allow_empty=True
        ),
        formula_fingerprint=_text(record, "formula_fingerprint", allow_empty=True),
        cost_model_version=_text(record, "cost_model_version", allow_empty=True),
        abstention_rate=_optional_number(record.get("abstention_rate")),
        success_rate=_optional_number(record.get("success_rate")),
        success_wilson_lower=_optional_number(record.get("success_wilson_lower")),
    )


def _assessment_from_payload(value: object) -> EvidenceGateAssessment | None:
    if value is None:
        return None
    record = _mapping(value, "evidence gate assessment")
    reasons = record.get("reason_codes")
    if not isinstance(reasons, list) or any(not isinstance(item, str) for item in reasons):
        raise PortfolioFileFormatError("assessment reason_codes must be text list")
    return EvidenceGateAssessment(
        tier=EvidenceGateTier(_text(record, "tier")),
        decision=EvidenceGateDecision(_text(record, "decision")),
        policy_fingerprint=_text(record, "policy_fingerprint"),
        reason_codes=tuple(reasons),
    )


def _progress_payload(snapshot: HypothesisPortfolioSnapshot) -> dict[str, Any]:
    progress = snapshot.progress
    return {
        "schema_version": PROGRESS_SCHEMA_VERSION,
        "kind": "hypothesis_portfolio_progress",
        "run_id": snapshot.run_id,
        "input_fingerprint": snapshot.input_fingerprint,
        "state": snapshot.state.value,
        "revision": snapshot.revision,
        "progress": {
            "total": progress.total,
            "completed": progress.completed,
            "failed": progress.failed,
            "running": progress.running,
            "pending": progress.pending,
            "finished": progress.finished,
            "fraction": progress.fraction,
        },
        "items": tuple(
            {
                "item_key": item.item_key,
                "replay_key": item.replay_key,
                "state": item.state.value,
                "attempts": item.attempts,
                "intermediate_decision": (
                    item.intermediate_assessment.decision.value
                    if item.intermediate_assessment is not None
                    else None
                ),
                "strict_decision": (
                    item.strict_assessment.decision.value
                    if item.strict_assessment is not None
                    else None
                ),
                "failure_code": item.failure_code,
            }
            for item in snapshot.items
        ),
    }


def _revision_files(directory: Path) -> tuple[Path, ...]:
    if not directory.is_dir():
        return ()
    return tuple(
        path
        for path in sorted(directory.glob("*.json"))
        if _REVISION_FILE.fullmatch(path.name) is not None
    )


def _revision_name(revision: int) -> str:
    if revision <= 0:
        raise ValueError("portfolio revision must be positive")
    return f"{revision:020d}.json"


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _write_immutable_json(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = _encoded_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        _fsync_directory(path.parent)
    except FileExistsError:
        if path.read_bytes() != encoded:
            raise PortfolioRevisionConflict(
                "immutable revision already exists with different content"
            ) from None
    finally:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = _encoded_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _encoded_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioFileFormatError(f"invalid portfolio JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise PortfolioFileFormatError(f"portfolio JSON object expected: {path}")
    return payload


def _mapping(value: object, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PortfolioFileFormatError(f"{location} must be an object")
    return value


def _text(
    record: Mapping[str, Any], field: str, *, allow_empty: bool = False
) -> str:
    value = record.get(field)
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise PortfolioFileFormatError(f"{field} must be text")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise PortfolioFileFormatError("optional text must be non-empty")
    return value


def _integer(record: Mapping[str, Any], field: str) -> int:
    value = record.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PortfolioFileFormatError(f"{field} must be a non-negative integer")
    return value


def _boolean(record: Mapping[str, Any], field: str) -> bool:
    value = record.get(field)
    if not isinstance(value, bool):
        raise PortfolioFileFormatError(f"{field} must be boolean")
    return value


def _optional_number(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise PortfolioFileFormatError("optional number must be numeric")
    converted = float(value)
    if not isfinite(converted):
        raise PortfolioFileFormatError("optional number must be finite")
    return converted

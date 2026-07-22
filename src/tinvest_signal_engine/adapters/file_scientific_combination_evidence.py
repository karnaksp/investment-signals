"""Immutable local research artifacts for C1-C4 evidence portfolios."""

from __future__ import annotations

from dataclasses import asdict
from enum import Enum
import json
import os
from pathlib import Path
from typing import Any, Mapping

from tinvest_signal_engine.application.scientific_combination_evidence import (
    ScientificCombinationArtifactReference,
    ScientificCombinationPortfolio,
)


class FileScientificCombinationEvidenceArtifacts:
    """Persist a deterministic portfolio without mutating an existing run."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def save(
        self, portfolio: ScientificCombinationPortfolio
    ) -> ScientificCombinationArtifactReference:
        artifact_fingerprint = portfolio.portfolio_fingerprint
        run_dir = self._root / artifact_fingerprint.removeprefix("sha256:")
        documents = {
            "manifest.json": {
                "artifact_schema": portfolio.evidence_version,
                "artifact_fingerprint": artifact_fingerprint,
                "dataset_fingerprint": portfolio.dataset_fingerprint,
                "source_report_fingerprint": portfolio.source_report_fingerprint,
                "cost_model_version": portfolio.cost_model_version,
                "observation_count": len(portfolio.observations),
                "result_count": len(portfolio.results),
            },
            "observations.json": [
                _json_value(asdict(item)) for item in portfolio.observations
            ],
            "outcomes.json": [
                _json_value(asdict(item)) for item in portfolio.outcomes
            ],
            "results.json": [
                _json_value(asdict(item)) for item in portfolio.results
            ],
        }
        encoded = {
            name: (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8")
            for name, payload in documents.items()
        }
        run_dir.mkdir(parents=True, exist_ok=True)
        for name, payload in encoded.items():
            _write_immutable(run_dir / name, payload)
        return ScientificCombinationArtifactReference(
            artifact_uri=str(run_dir),
            artifact_fingerprint=artifact_fingerprint,
        )


def _write_immutable(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(
                f"immutable scientific combination artifact differs: {path.name}"
            )
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value

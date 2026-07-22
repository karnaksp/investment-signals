"""Explicit immutable versions for local scientific portfolio execution."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
import json
from typing import Iterable

from tinvest_signal_engine.application.hypothesis_portfolio_runner import (
    PortfolioHypothesisRegistration,
    RunHypothesisPortfolioRequest,
)


class ScientificPortfolioVersion(str, Enum):
    SEALED_ELEVEN_V1 = "sealed-eleven-v1"
    EXTENDED_H10_H11_V1 = "sealed-eleven-plus-r2-h10-h11-v1"


_SEALED_ELEVEN_IDS = (
    "h1-morning-low-volume-reversion",
    "h2-morning-high-volume-continuation",
    "h3-jump-low-activity-reversal",
    "h4-jump-high-activity-continuation",
    "h5-same-phase-return-recurrence",
    "h6-open-close-market-continuation",
    "h7-relative-volume-future-activity",
    "h12-pair-residual-reversion",
    "h15-multi-window-volatility-forecast",
    "h16-negative-semivariance-future-risk",
    "h17-volatility-jump-persistence",
)
_R2_EXTENSION_IDS = (
    "h10-positive-main-open-gap-reversion",
    "h11-residual-move-reversion",
)


@dataclass(frozen=True, slots=True)
class ScientificPortfolioDefinition:
    version: ScientificPortfolioVersion
    hypothesis_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.hypothesis_ids or len(set(self.hypothesis_ids)) != len(
            self.hypothesis_ids
        ):
            raise ValueError("scientific portfolio hypothesis ids must be unique")

    @property
    def fingerprint(self) -> str:
        payload = {
            "hypothesis_ids": self.hypothesis_ids,
            "version": self.version.value,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class VersionedPortfolioRunPlan:
    definition: ScientificPortfolioDefinition
    requests: tuple[RunHypothesisPortfolioRequest, ...]

    def __post_init__(self) -> None:
        if not self.requests:
            raise ValueError("versioned portfolio plan requires runner requests")
        selected = tuple(
            registration.hypothesis.hypothesis_id
            for request in self.requests
            for registration in request.hypotheses
        )
        if len(selected) != len(set(selected)):
            raise ValueError("versioned portfolio plan repeats a hypothesis")
        if set(selected) != set(self.definition.hypothesis_ids):
            raise ValueError("versioned portfolio plan differs from its definition")
        if any(
            request.portfolio_definition_fingerprint != self.definition.fingerprint
            for request in self.requests
        ):
            raise ValueError("runner request is not sealed by the plan definition")

    @property
    def fingerprint(self) -> str:
        payload = {
            "definition_fingerprint": self.definition.fingerprint,
            "request_fingerprints": tuple(
                item.input_fingerprint
                for item in sorted(
                    self.requests, key=lambda request: request.cost_model_version
                )
            ),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + sha256(encoded).hexdigest()


def scientific_portfolio_definition(
    version: ScientificPortfolioVersion,
) -> ScientificPortfolioDefinition:
    hypothesis_ids = _SEALED_ELEVEN_IDS
    if version is ScientificPortfolioVersion.EXTENDED_H10_H11_V1:
        hypothesis_ids += _R2_EXTENSION_IDS
    return ScientificPortfolioDefinition(version=version, hypothesis_ids=hypothesis_ids)


def build_versioned_portfolio_plan(
    *,
    version: ScientificPortfolioVersion,
    registrations: Iterable[PortfolioHypothesisRegistration],
    dataset_fingerprint: str,
    replay_engine_version: str,
) -> VersionedPortfolioRunPlan:
    """Select exact items and group runner calls by their sealed cost model."""

    definition = scientific_portfolio_definition(version)
    by_id: dict[str, PortfolioHypothesisRegistration] = {}
    for registration in registrations:
        hypothesis_id = registration.hypothesis.hypothesis_id
        if hypothesis_id in by_id:
            raise ValueError(f"duplicate portfolio registration: {hypothesis_id}")
        by_id[hypothesis_id] = registration
    missing = tuple(
        hypothesis_id
        for hypothesis_id in definition.hypothesis_ids
        if hypothesis_id not in by_id
    )
    if missing:
        raise ValueError("missing portfolio registrations: " + ", ".join(missing))
    selected = tuple(
        by_id[hypothesis_id] for hypothesis_id in definition.hypothesis_ids
    )
    by_cost_model: dict[str, list[PortfolioHypothesisRegistration]] = {}
    for registration in selected:
        preregistration = registration.hypothesis.preregistration
        if preregistration is None:
            raise ValueError("portfolio registration is not preregistered")
        by_cost_model.setdefault(preregistration.cost_model_version, []).append(
            registration
        )
    requests = tuple(
        RunHypothesisPortfolioRequest(
            dataset_fingerprint=dataset_fingerprint,
            cost_model_version=cost_model_version,
            replay_engine_version=replay_engine_version,
            hypotheses=tuple(group),
            portfolio_definition_fingerprint=definition.fingerprint,
        )
        for cost_model_version, group in sorted(by_cost_model.items())
    )
    return VersionedPortfolioRunPlan(definition=definition, requests=requests)

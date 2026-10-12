"""Pure statistical primitives for scientific hypothesis evidence.

The module deliberately uses only the Python standard library.  It contains
the immutable records and deterministic calculations shared by historical
replay, shadow evaluation, and the product evidence gate.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from math import comb, sqrt
from random import Random
from typing import Mapping, Sequence


class DatasetPartition(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    HOLDOUT = "holdout"


class EvidenceDecision(str, Enum):
    PASSED = "passed"
    REJECTED = "rejected"
    INCONCLUSIVE = "inconclusive"
    BLOCKED_BY_DATA = "blocked_by_data"


@dataclass(frozen=True)
class ConfidenceInterval:
    lower: float
    estimate: float
    upper: float
    confidence_level: float = 0.95

    def __post_init__(self) -> None:
        if not 0.0 < self.confidence_level < 1.0:
            raise ValueError("confidence_level must be between zero and one")
        if not self.lower <= self.estimate <= self.upper:
            raise ValueError("confidence interval bounds must contain the estimate")


@dataclass(frozen=True)
class StudyPoint:
    """One target or potential control observation with sealed covariates."""

    point_id: str
    scenario_id: str | None
    instrument_id: str
    occurred_at: datetime
    trading_day: date
    session_bucket: str
    volatility_bucket: str
    liquidity_bucket: str
    features_observed_at: datetime
    partition: DatasetPartition
    net_effect_bps: float
    cost_model_version: str
    nearby_scenario_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.point_id.strip():
            raise ValueError("point_id must not be empty")
        if not self.instrument_id.strip():
            raise ValueError("instrument_id must not be empty")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        if (
            self.features_observed_at.tzinfo is None
            or self.features_observed_at.utcoffset() is None
        ):
            raise ValueError("features_observed_at must be timezone-aware")
        if self.features_observed_at > self.occurred_at:
            raise ValueError("features must not use information after the event")
        if not all(
            value.strip()
            for value in (
                self.session_bucket,
                self.volatility_bucket,
                self.liquidity_bucket,
                self.cost_model_version,
            )
        ):
            raise ValueError("matching buckets and cost_model_version are required")

    @property
    def matching_key(self) -> tuple[object, ...]:
        return (
            self.instrument_id,
            self.session_bucket,
            self.trading_day.weekday(),
            self.volatility_bucket,
            self.liquidity_bucket,
            self.partition,
        )


@dataclass(frozen=True)
class MatchedControlGroup:
    event: StudyPoint
    controls: tuple[StudyPoint, ...]

    def __post_init__(self) -> None:
        control_ids = tuple(control.point_id for control in self.controls)
        if len(set(control_ids)) != len(control_ids):
            raise ValueError("controls must be unique within a matched group")
        if self.event.point_id in control_ids:
            raise ValueError("an event cannot control itself")
        for control in self.controls:
            if control.matching_key != self.event.matching_key:
                raise ValueError("control matching strata must equal event strata")
            if control.cost_model_version != self.event.cost_model_version:
                raise ValueError("event and controls must use the same cost model")
            if (
                self.event.scenario_id is not None
                and self.event.scenario_id in control.nearby_scenario_ids
            ):
                raise ValueError("control overlaps the event scenario exclusion window")

    @property
    def control_mean_bps(self) -> float:
        if not self.controls:
            raise ValueError("control mean is undefined without controls")
        return sum(item.net_effect_bps for item in self.controls) / len(self.controls)

    @property
    def lift_bps(self) -> float:
        return self.event.net_effect_bps - self.control_mean_bps


@dataclass(frozen=True)
class ControlReuseStatistics:
    """Auditable dependence created by sharing controls between event groups."""

    distinct_controls: int
    maximum_reuse: int
    mean_reuse: float
    independent_clusters: int

    def __post_init__(self) -> None:
        if (
            self.distinct_controls < 0
            or self.maximum_reuse < 0
            or self.mean_reuse < 0.0
            or self.independent_clusters < 0
        ):
            raise ValueError("control reuse statistics must not be negative")
        if self.distinct_controls == 0 and (
            self.maximum_reuse != 0
            or self.mean_reuse != 0.0
            or self.independent_clusters != 0
        ):
            raise ValueError("empty control reuse statistics must be all zero")


@dataclass(frozen=True)
class MatchedControlsResult:
    groups: tuple[MatchedControlGroup, ...]
    unmatched_event_ids: tuple[str, ...]
    controls_per_event: int
    maximum_control_reuse: int = 1
    selection_policy_version: str = (
        "unique-control-rarity-first-exact-strata-exclusion-5m-v2"
    )

    def __post_init__(self) -> None:
        if self.maximum_control_reuse <= 0:
            raise ValueError("maximum_control_reuse must be positive")
        if not self.selection_policy_version.strip():
            raise ValueError("selection_policy_version must not be empty")
        reuse_counts = Counter(
            control.point_id for group in self.groups for control in group.controls
        )
        if reuse_counts and max(reuse_counts.values()) > self.maximum_control_reuse:
            if self.maximum_control_reuse == 1:
                raise ValueError("a control must not be reused across event groups")
            raise ValueError("a control exceeds the configured reuse limit")
        if any(len(group.controls) != self.controls_per_event for group in self.groups):
            raise ValueError("every completed group must have the requested controls")

    @property
    def reuse_statistics(self) -> ControlReuseStatistics:
        reuse_counts = Counter(
            control.point_id for group in self.groups for control in group.controls
        )
        assignments = sum(reuse_counts.values())
        return ControlReuseStatistics(
            distinct_controls=len(reuse_counts),
            maximum_reuse=max(reuse_counts.values(), default=0),
            mean_reuse=(assignments / len(reuse_counts) if reuse_counts else 0.0),
            independent_clusters=len(control_dependency_clusters(self.groups)),
        )


def control_dependency_clusters(
    groups: Sequence[MatchedControlGroup],
) -> tuple[tuple[MatchedControlGroup, ...], ...]:
    """Group events connected by a reused control into independent clusters."""

    if not groups:
        return ()
    parents = list(range(len(groups)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    first_group_by_control: dict[str, int] = {}
    for group_index, group in enumerate(groups):
        for control in group.controls:
            prior_group = first_group_by_control.setdefault(
                control.point_id, group_index
            )
            union(group_index, prior_group)
    by_root: dict[int, list[MatchedControlGroup]] = {}
    for index, group in enumerate(groups):
        by_root.setdefault(find(index), []).append(group)
    return tuple(
        tuple(
            sorted(
                cluster,
                key=lambda item: (
                    item.event.occurred_at,
                    item.event.point_id,
                ),
            )
        )
        for _, cluster in sorted(
            by_root.items(),
            key=lambda item: min(group.event.point_id for group in item[1]),
        )
    )


@dataclass(frozen=True)
class ChronologicalSplit:
    train_days: tuple[date, ...]
    validation_days: tuple[date, ...]
    holdout_days: tuple[date, ...]

    def __post_init__(self) -> None:
        combined = self.train_days + self.validation_days + self.holdout_days
        if len(combined) != len(set(combined)):
            raise ValueError("trading days must not overlap between partitions")
        if tuple(sorted(combined)) != combined:
            raise ValueError("partitions must remain in chronological order")

    def partition_for(self, trading_day: date) -> DatasetPartition:
        if trading_day in self.train_days:
            return DatasetPartition.TRAIN
        if trading_day in self.validation_days:
            return DatasetPartition.VALIDATION
        if trading_day in self.holdout_days:
            return DatasetPartition.HOLDOUT
        raise KeyError(f"trading day {trading_day.isoformat()} is outside the split")


@dataclass(frozen=True)
class AdjustedHypothesisTest:
    test_id: str
    p_value: float
    q_value: float
    significant: bool


@dataclass(frozen=True)
class StabilityBlock:
    block_number: int
    trading_days: tuple[date, ...]
    observation_count: int
    mean_lift_bps: float
    positive: bool


@dataclass(frozen=True)
class StabilityAssessment:
    blocks: tuple[StabilityBlock, ...]
    required_positive_blocks: int
    positive_blocks: int
    assessed: bool
    stable: bool


@dataclass(frozen=True)
class InstrumentConcentration:
    instrument_id: str
    event_count: int
    share: float


@dataclass(frozen=True)
class EvidenceReasonCount:
    reason_code: str
    count: int

    def __post_init__(self) -> None:
        if not self.reason_code.strip():
            raise ValueError("evidence diagnostic reason_code must not be empty")
        if self.count <= 0:
            raise ValueError("evidence diagnostic reason count must be positive")


@dataclass(frozen=True)
class EvidenceDiagnosticsV2:
    """Descriptive evidence funnel; it never grants product promotion."""

    version: str
    event_prevalence: float | None
    eligible_event_count: int
    matched_event_count: int
    match_coverage: float | None
    data_coverage: float | None
    reasons_histogram: tuple[EvidenceReasonCount, ...]
    primary_effect_estimate: float | None
    primary_effect_interval: ConfidenceInterval | None
    primary_p_value: float | None
    descriptive_only: bool

    def __post_init__(self) -> None:
        if self.version != "evidence-diagnostics-v2":
            raise ValueError("unsupported evidence diagnostics version")
        if self.eligible_event_count < 0 or self.matched_event_count < 0:
            raise ValueError("evidence diagnostic event counts must be non-negative")
        if self.matched_event_count > self.eligible_event_count:
            raise ValueError("matched events cannot exceed eligible events")
        for name, value in (
            ("event_prevalence", self.event_prevalence),
            ("match_coverage", self.match_coverage),
            ("data_coverage", self.data_coverage),
        ):
            if value is not None and not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between zero and one")
        reason_codes = tuple(item.reason_code for item in self.reasons_histogram)
        if len(reason_codes) != len(set(reason_codes)):
            raise ValueError("evidence diagnostic reasons must be unique")
        if reason_codes != tuple(sorted(reason_codes)):
            raise ValueError("evidence diagnostic reasons must be sorted")
        statistics = (
            self.primary_effect_estimate,
            self.primary_effect_interval,
            self.primary_p_value,
        )
        if any(value is None for value in statistics) and not all(
            value is None for value in statistics
        ):
            raise ValueError(
                "descriptive primary statistics must be all present or absent"
            )
        if self.primary_p_value is not None and not 0.0 <= self.primary_p_value <= 1.0:
            raise ValueError("primary_p_value must be between zero and one")


@dataclass(frozen=True)
class EvidenceBundle:
    """Immutable positive, negative, or incomplete replication result."""

    evidence_id: str
    hypothesis_id: str
    hypothesis_version: str
    dataset_fingerprint: str
    decision: EvidenceDecision
    reason_codes: tuple[str, ...]
    trading_days: int
    eligible_events: int
    matched_events: int
    matched_controls: int
    cost_model_version: str | None
    event_mean_net_bps: float | None
    control_mean_net_bps: float | None
    mean_lift_bps: float | None
    lift_interval: ConfidenceInterval | None
    positive_rate_interval: ConfidenceInterval | None
    raw_p_value: float | None
    adjusted_q_value: float | None
    fdr_significant: bool
    stability: StabilityAssessment
    instrument_concentration: tuple[InstrumentConcentration, ...]
    maximum_instrument_share: float | None
    diagnostics_v2: EvidenceDiagnosticsV2 | None = None


def chronological_split_60_20_20(trading_days: Sequence[date]) -> ChronologicalSplit:
    """Split unique trading days without random row mixing or boundary overlap."""

    days = tuple(sorted(set(trading_days)))
    if len(days) < 5:
        raise ValueError("at least five trading days are required for a 60/20/20 split")
    train_end = int(len(days) * 0.60)
    validation_end = train_end + int(len(days) * 0.20)
    return ChronologicalSplit(
        train_days=days[:train_end],
        validation_days=days[train_end:validation_end],
        holdout_days=days[validation_end:],
    )


def wilson_interval(
    successes: int,
    trials: int,
    *,
    z_score: float = 1.959963984540054,
) -> ConfidenceInterval:
    """Wilson score interval for a binomial proportion."""

    if trials <= 0:
        raise ValueError("trials must be positive")
    if successes < 0 or successes > trials:
        raise ValueError("successes must be between zero and trials")
    proportion = successes / trials
    denominator = 1.0 + (z_score * z_score / trials)
    centre = (proportion + z_score * z_score / (2.0 * trials)) / denominator
    margin = (
        z_score
        * sqrt(
            proportion * (1.0 - proportion) / trials
            + z_score * z_score / (4.0 * trials * trials)
        )
        / denominator
    )
    return ConfidenceInterval(
        lower=min(proportion, max(0.0, centre - margin)),
        estimate=proportion,
        upper=max(proportion, min(1.0, centre + margin)),
    )


def day_block_bootstrap_interval(
    values_by_day: Mapping[date, Sequence[float]],
    *,
    samples: int = 2_000,
    seed: int = 0,
    confidence_level: float = 0.95,
) -> ConfidenceInterval:
    """Resample whole trading days, retaining within-day dependence."""

    if samples <= 0:
        raise ValueError("samples must be positive")
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be between zero and one")
    days = tuple(sorted(values_by_day))
    if not days or any(not values_by_day[day] for day in days):
        raise ValueError("every bootstrap day must contain observations")
    flattened = [value for day in days for value in values_by_day[day]]
    estimate = sum(flattened) / len(flattened)
    generator = Random(seed)
    bootstrap_means: list[float] = []
    for _ in range(samples):
        sampled = [generator.choice(days) for _ in days]
        observations = [
            value for sampled_day in sampled for value in values_by_day[sampled_day]
        ]
        bootstrap_means.append(sum(observations) / len(observations))
    bootstrap_means.sort()
    alpha = (1.0 - confidence_level) / 2.0
    return ConfidenceInterval(
        lower=min(_quantile(bootstrap_means, alpha), estimate),
        estimate=estimate,
        upper=max(_quantile(bootstrap_means, 1.0 - alpha), estimate),
        confidence_level=confidence_level,
    )


def control_cluster_bootstrap_interval(
    values_by_cluster: Mapping[str, Sequence[float]],
    *,
    samples: int = 2_000,
    seed: int = 0,
    confidence_level: float = 0.95,
) -> ConfidenceInterval:
    """Resample independent control-dependency clusters as whole blocks."""

    if samples <= 0:
        raise ValueError("samples must be positive")
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be between zero and one")
    clusters = tuple(sorted(values_by_cluster))
    if not clusters or any(not values_by_cluster[item] for item in clusters):
        raise ValueError("every bootstrap cluster must contain observations")
    flattened = [value for cluster in clusters for value in values_by_cluster[cluster]]
    estimate = sum(flattened) / len(flattened)
    generator = Random(seed)
    bootstrap_means: list[float] = []
    for _ in range(samples):
        sampled = [generator.choice(clusters) for _ in clusters]
        observations = [
            value
            for sampled_cluster in sampled
            for value in values_by_cluster[sampled_cluster]
        ]
        bootstrap_means.append(sum(observations) / len(observations))
    bootstrap_means.sort()
    alpha = (1.0 - confidence_level) / 2.0
    return ConfidenceInterval(
        lower=min(_quantile(bootstrap_means, alpha), estimate),
        estimate=estimate,
        upper=max(
            _quantile(bootstrap_means, 1.0 - alpha),
            estimate,
        ),
        confidence_level=confidence_level,
    )


def benjamini_hochberg(
    p_values: Mapping[str, float],
    *,
    false_discovery_rate: float = 0.05,
) -> tuple[AdjustedHypothesisTest, ...]:
    """Return monotone BH adjusted q-values in caller-independent order."""

    if not 0.0 < false_discovery_rate < 1.0:
        raise ValueError("false_discovery_rate must be between zero and one")
    if any(value < 0.0 or value > 1.0 for value in p_values.values()):
        raise ValueError("p-values must be between zero and one")
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    count = len(ordered)
    adjusted: dict[str, float] = {}
    running = 1.0
    for reverse_index in range(count - 1, -1, -1):
        test_id, p_value = ordered[reverse_index]
        rank = reverse_index + 1
        running = min(running, p_value * count / rank)
        adjusted[test_id] = min(1.0, running)
    return tuple(
        AdjustedHypothesisTest(
            test_id=test_id,
            p_value=p_value,
            q_value=adjusted[test_id],
            significant=adjusted[test_id] <= false_discovery_rate,
        )
        for test_id, p_value in sorted(p_values.items())
    )


def five_block_stability(
    values_by_day: Mapping[date, Sequence[float]],
    *,
    required_positive_blocks: int = 4,
) -> StabilityAssessment:
    """Check that an effect survives at least four of five chronological blocks."""

    if required_positive_blocks < 1 or required_positive_blocks > 5:
        raise ValueError("required_positive_blocks must be between one and five")
    days = tuple(sorted(values_by_day))
    if len(days) < 5:
        return StabilityAssessment(
            blocks=(),
            required_positive_blocks=required_positive_blocks,
            positive_blocks=0,
            assessed=False,
            stable=False,
        )
    block_days: list[list[date]] = [[] for _ in range(5)]
    for index, trading_day in enumerate(days):
        block_days[min(4, index * 5 // len(days))].append(trading_day)
    blocks: list[StabilityBlock] = []
    for index, days_in_block in enumerate(block_days, start=1):
        values = [
            value
            for trading_day in days_in_block
            for value in values_by_day[trading_day]
        ]
        mean = sum(values) / len(values)
        blocks.append(
            StabilityBlock(
                block_number=index,
                trading_days=tuple(days_in_block),
                observation_count=len(values),
                mean_lift_bps=mean,
                positive=mean > 0.0,
            )
        )
    positive_blocks = sum(block.positive for block in blocks)
    return StabilityAssessment(
        blocks=tuple(blocks),
        required_positive_blocks=required_positive_blocks,
        positive_blocks=positive_blocks,
        assessed=True,
        stable=positive_blocks >= required_positive_blocks,
    )


def one_sided_sign_test_p_value(positive: int, total: int) -> float:
    """Exact probability of at least ``positive`` successes under p=0.5."""

    if total <= 0:
        raise ValueError("total must be positive")
    if positive < 0 or positive > total:
        raise ValueError("positive must be between zero and total")
    numerator = sum(comb(total, count) for count in range(positive, total + 1))
    return numerator / (2**total)


def _quantile(sorted_values: Sequence[float], probability: float) -> float:
    position = (len(sorted_values) - 1) * probability
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    fraction = position - lower_index
    return (
        sorted_values[lower_index]
        + (sorted_values[upper_index] - sorted_values[lower_index]) * fraction
    )

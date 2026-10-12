"""Application contract for causal live relative-value observations.

Adapters own basket aggregation, pair prices, and parameter persistence.  This
use case accepts only already-observed values plus frozen model parameters and
delegates the scientific formula to the same domain functions used by replay.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from tinvest_signal_engine.application.scientific_hypothesis_combinations import (
    ComposeScientificCombination,
    ComposeScientificCombinationRequest,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    FrozenMarketResidualParameters,
    FrozenPairParameters,
    ProspectiveFeature,
    ProspectiveScientificPolicy,
    market_residual_reversion_v2_feature,
    pair_residual_reversion_v2_feature,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    ScientificCombinationId,
    ScientificCombinationObservation,
)


@dataclass(frozen=True, slots=True)
class MarketResidualLiveInput:
    ticker: str
    stock_return_bps: float
    basket_return_bps: float
    basket_coverage: float
    parameters: FrozenMarketResidualParameters | None
    trading_gap: bool = False


@dataclass(frozen=True, slots=True)
class PairResidualLiveInput:
    left_ticker: str
    right_ticker: str
    left_price: float
    right_price: float
    parameters: FrozenPairParameters | None
    corporate_action_suspected: bool = False
    liquid: bool = True


@dataclass(frozen=True, slots=True)
class RelativeValueLiveSnapshot:
    trading_day: date
    observed_at: datetime
    market: MarketResidualLiveInput | None = None
    pair: PairResidualLiveInput | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.market is None and self.pair is None:
            raise ValueError("relative-value snapshot requires market or pair input")


class BuildRelativeValueLiveFeatures:
    """Build live H11V2/H12V2 features from causally frozen inputs."""

    def __init__(self, policy: ProspectiveScientificPolicy) -> None:
        self._policy = policy

    def execute(
        self, snapshot: RelativeValueLiveSnapshot
    ) -> tuple[ProspectiveFeature, ...]:
        features: list[ProspectiveFeature] = []
        if snapshot.market is not None:
            item = snapshot.market
            features.extend(
                market_residual_reversion_v2_feature(
                    ticker=item.ticker,
                    trading_day=snapshot.trading_day,
                    observed_at=snapshot.observed_at,
                    stock_return_bps=item.stock_return_bps,
                    basket_return_bps=item.basket_return_bps,
                    basket_coverage=item.basket_coverage,
                    parameters=item.parameters,
                    trading_gap=item.trading_gap,
                    policy=self._policy,
                    horizon_seconds=horizon,
                )
                for horizon in self._policy.market_residual_horizons_seconds
            )
        if snapshot.pair is not None:
            item = snapshot.pair
            features.extend(
                pair_residual_reversion_v2_feature(
                    left_ticker=item.left_ticker,
                    right_ticker=item.right_ticker,
                    trading_day=snapshot.trading_day,
                    observed_at=snapshot.observed_at,
                    left_price=item.left_price,
                    right_price=item.right_price,
                    parameters=item.parameters,
                    corporate_action_suspected=item.corporate_action_suspected,
                    liquid=item.liquid,
                    policy=self._policy,
                    horizon_seconds=horizon,
                )
                for horizon in self._policy.pair_horizons_seconds
            )
        return tuple(
            sorted(
                features,
                key=lambda item: (
                    item.hypothesis.value,
                    item.ticker,
                    item.horizon_seconds,
                ),
            )
        )


class BuildJointResidualLiveShadow:
    """Compose live C5 only when causal H11V2 and H12V2 inputs coexist.

    The adapter must provide already observed prices, returns, and parameters
    frozen strictly before ``observed_at``.  This use case performs no data
    access and emits the same domain formula fingerprint as historical replay.
    """

    def __init__(self, policy: ProspectiveScientificPolicy) -> None:
        self._features = BuildRelativeValueLiveFeatures(policy)
        self._composer = ComposeScientificCombination()
        self._horizons = tuple(
            sorted(
                set(policy.market_residual_horizons_seconds)
                & set(policy.pair_horizons_seconds)
                & {900, 1800}
            )
        )
        if not self._horizons:
            raise ValueError("C5 requires a shared H11V2/H12V2 horizon")

    def execute(
        self,
        snapshot: RelativeValueLiveSnapshot,
    ) -> tuple[ScientificCombinationObservation, ...]:
        if snapshot.market is None or snapshot.pair is None:
            raise ValueError("C5 live shadow requires both market and pair inputs")
        if snapshot.market.ticker != snapshot.pair.left_ticker:
            raise ValueError("C5 market ticker must equal the pair left ticker")
        features = self._features.execute(snapshot)
        pair_scope = f"{snapshot.pair.left_ticker}/{snapshot.pair.right_ticker}"
        return tuple(
            self._composer.execute(
                ComposeScientificCombinationRequest(
                    combination_id=ScientificCombinationId.C5,
                    primary_scope=pair_scope,
                    market_context_scope=snapshot.market.ticker,
                    trading_day=snapshot.trading_day,
                    observed_at=snapshot.observed_at,
                    horizon_seconds=horizon,
                    components=features,
                )
            )
            for horizon in self._horizons
        )

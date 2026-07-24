"""Stable product contract for the active scientific replay portfolio.

The contract deliberately contains no transport, registry parser, or storage
dependency.  It is the common vocabulary used at adapter boundaries to keep
the executable hypotheses aligned while the scientific registry may contain
newer versions and research-only candidates.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
import json


class ReplayDataRequirement(str, Enum):
    HISTORICAL_CANDLES = "historical_candles"
    LIVE_ORDERBOOK = "live_orderbook"


class ReplaySourceDataState(str, Enum):
    READY = "ready"
    INSUFFICIENT_HISTORY = "insufficient_history"
    REQUIRES_LIVE_ORDERBOOK = "requires_live_orderbook"
    STALE_LIVE_ORDERBOOK = "stale_live_orderbook"
    SEQUENCE_GAP = "sequence_gap"
    TIMESTAMP_DESYNCHRONIZATION = "timestamp_desynchronization"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ScientificReplayDefinition:
    short_id: str
    catalog_hypothesis_id: str
    catalog_version: str
    expected_direction: str
    market_phase: str
    horizons_seconds: tuple[int, ...]
    data_requirement: ReplayDataRequirement
    allowed_source_data_states: tuple[ReplaySourceDataState, ...]
    claim_family: str = "directional"
    effect_unit: str = "cost_adjusted_signed_return_bps"
    claim_scope: str = "price_direction"


_CANDLE_STATES = (
    ReplaySourceDataState.READY,
    ReplaySourceDataState.INSUFFICIENT_HISTORY,
    ReplaySourceDataState.UNAVAILABLE,
)
_ORDERBOOK_STATES = (
    ReplaySourceDataState.READY,
    ReplaySourceDataState.REQUIRES_LIVE_ORDERBOOK,
    ReplaySourceDataState.STALE_LIVE_ORDERBOOK,
    ReplaySourceDataState.SEQUENCE_GAP,
    ReplaySourceDataState.TIMESTAMP_DESYNCHRONIZATION,
    ReplaySourceDataState.UNAVAILABLE,
)


SCIENTIFIC_REPLAY_CONTRACT_V1 = (
    ScientificReplayDefinition(
        "H1", "h1-morning-low-volume-reversion", "1.0.0", "reversion_to_previous_close",
        "morning_0700_0949", (1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H2", "h2-morning-high-volume-continuation", "1.0.0", "continuation",
        "morning_0700_0949", (900, 1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H3", "h3-jump-low-activity-reversal", "1.0.0", "reversal", "main_session",
        (300, 900, 1800), ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H4", "h4-jump-high-activity-continuation", "1.0.0", "continuation", "main_session",
        (300, 900, 1800), ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H5", "h5-same-phase-return-recurrence", "1.0.0", "same_as_prior_20d_bucket_mean",
        "half_hour_bucket", (1800,),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H6", "h6-open-close-market-continuation", "1.0.0", "same_as_opening_basket",
        "main_open_to_preclose", (1800,),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H7", "h7-relative-volume-future-activity", "1.0.0", "activity_increase",
        "any_liquid_session_phase", (900, 1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "relative_activity_uplift", "activity_only",
    ),
    ScientificReplayDefinition(
        "H7V2", "h7-relative-volume-future-activity", "2.0.0", "activity_increase",
        "any_liquid_session_phase", (900, 1800),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "relative_variance_uplift", "volatility_only",
    ),
    ScientificReplayDefinition(
        "H7V3", "h7-relative-volume-future-activity", "3.0.0",
        "volatility_increase", "any_liquid_session_phase", (1800,),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "variance_uplift_ratio_x_10000", "volatility_only",
    ),
    ScientificReplayDefinition(
        "H10", "h10-positive-main-open-gap-reversion", "1.0.0",
        "reversion_to_previous_close", "main_session_open", (1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H11", "h11-residual-move-reversion", "1.0.0", "reversal",
        "main_session", (900, 1800),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H11V2", "h11-residual-move-reversion", "2.0.0",
        "idiosyncratic_residual_reversion", "main_session", (900, 1800),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H12", "h12-pair-residual-reversion", "1.0.0",
        "pair_residual_reversion", "main_liquid_session", (900, 1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H12V2", "h12-pair-residual-reversion", "2.0.0",
        "rolling_pair_residual_reversion", "main_liquid_session",
        (900, 1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H3V2", "h3-jump-low-activity-reversal", "2.0.0", "reversal",
        "main_session", (300, 900),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H4V2", "h4-jump-high-activity-continuation", "2.0.0", "continuation",
        "main_session", (300, 900),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H3V3", "h3-jump-low-activity-reversal", "3.0.0", "reversal",
        "main_session", (300, 900),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H4V3", "h4-jump-high-activity-continuation", "3.0.0", "continuation",
        "main_session", (300, 900),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
    ),
    ScientificReplayDefinition(
        "H15", "h15-multi-window-volatility-forecast", "1.0.0",
        "volatility_increase", "any_liquid_session_phase", (1800, 3600),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "qlike_improvement", "volatility_only",
    ),
    ScientificReplayDefinition(
        "H15V2", "h15-multi-window-volatility-forecast", "2.0.0",
        "volatility_increase", "any_liquid_session_phase", (1800,),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "qlike_improvement_x_10000", "volatility_only",
    ),
    ScientificReplayDefinition(
        "H16", "h16-negative-semivariance-future-risk", "1.0.0",
        "volatility_increase", "any_liquid_session_phase", (1800,),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "variance_uplift_ratio_x_10000", "volatility_only",
    ),
    ScientificReplayDefinition(
        "H17", "h17-volatility-jump-persistence", "1.0.0",
        "volatility_increase", "any_liquid_session_phase", (1800,),
        ReplayDataRequirement.HISTORICAL_CANDLES, _CANDLE_STATES,
        "activity", "variance_uplift_ratio_x_10000", "volatility_only",
    ),
    ScientificReplayDefinition(
        "H8", "h8-best-queue-imbalance", "1.0.0", "same_as_queue_imbalance",
        "live_orderbook_only", (1, 5),
        ReplayDataRequirement.LIVE_ORDERBOOK, _ORDERBOOK_STATES,
    ),
    ScientificReplayDefinition(
        "H9", "h9-order-flow-price-jump-coherence", "1.0.0",
        "conditional_continuation_or_reversal", "live_orderbook_only",
        (1, 5, 60, 300, 900),
        ReplayDataRequirement.LIVE_ORDERBOOK, _ORDERBOOK_STATES,
    ),
)


def scientific_replay_definition(short_id: str) -> ScientificReplayDefinition:
    normalized = short_id.strip().upper()
    try:
        return next(
            item for item in SCIENTIFIC_REPLAY_CONTRACT_V1
            if item.short_id == normalized
        )
    except StopIteration as exc:
        raise ValueError(f"unknown scientific replay hypothesis: {short_id}") from exc


def scientific_replay_formula_fingerprint(short_id: str) -> str:
    definition = scientific_replay_definition(short_id)
    payload = {
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "catalog_version": definition.catalog_version,
        "expected_direction": definition.expected_direction,
        "horizons_seconds": definition.horizons_seconds,
        "market_phase": definition.market_phase,
        "claim_family": definition.claim_family,
        "effect_unit": definition.effect_unit,
        "claim_scope": definition.claim_scope,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()

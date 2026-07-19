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
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()

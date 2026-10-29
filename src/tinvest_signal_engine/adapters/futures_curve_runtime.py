"""Map wire events/configuration to the pure futures-curve detector."""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import time
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Callable
from uuid import NAMESPACE_URL, uuid5

import yaml

from tinvest_signal_engine.domain.futures_curve import (
    CurveContract, CurveMarketEvent, CurveSettings, FuturesCurveDetector,
)
from tinvest_signal_engine.models import NormalizedEvent, TriggerSignal
from tinvest_signal_engine.serialization import parse_timestamp, quotation_to_float

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CurveRuntimeConfiguration:
    contracts: tuple[CurveContract, ...]
    settings: CurveSettings
    version: str
    secondary_instrument_ids: tuple[str, ...] = ()


def load_curve_configuration(
    instruments_path: Path, policy_path: Path,
) -> CurveRuntimeConfiguration:
    """Read immutable metadata. Missing monetary specifications fail closed."""
    instrument_bytes = instruments_path.read_bytes()
    policy_bytes = policy_path.read_bytes()
    raw = json.loads(policy_bytes)
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise ValueError("futures curve policy requires version 1")
    settings_raw = raw.get("settings", {})
    if not isinstance(settings_raw, dict):
        raise ValueError("futures curve settings must be an object")
    allowed = {field.name for field in fields(CurveSettings)}
    if set(settings_raw) - allowed:
        raise ValueError("unknown futures curve setting")
    if "enabled_types" in settings_raw:
        settings_raw["enabled_types"] = tuple(settings_raw["enabled_types"])
    settings = CurveSettings(**settings_raw)
    instruments = yaml.safe_load(instrument_bytes) or {}
    if not isinstance(instruments, dict):
        raise ValueError("instruments must be a mapping")
    contracts: list[CurveContract] = []
    secondary: list[str] = []
    for item in instruments.get("instruments", []):
        if not isinstance(item, dict) or item.get("class_code") != "SPBFUT":
            continue
        meta = item.get("futures_contract")
        if not isinstance(meta, dict):
            continue
        try:
            currency = str(meta["settlement_currency"]).upper()
            increment = float(meta["min_price_increment"])
            amount = float(meta["min_price_increment_amount"])
            size = float(meta["basic_asset_size"])
            if currency != "RUB" or any(
                not math.isfinite(value) or value <= 0 for value in (increment, amount, size)
            ):
                continue
            expiry = parse_timestamp(meta["expiration_at"])
            chain = str(meta["chain_id"]).strip()
            asset = str(meta["basic_asset"]).strip()
            if not chain or not asset:
                continue
            ticker = str(item["ticker"])
            if str(item.get("alias", "")).lower() != chain.lower():
                secondary.append(f"{ticker}_SPBFUT")
            contracts.append(CurveContract(
                instrument_id=f"{ticker}_SPBFUT", ticker=ticker, chain_id=chain,
                expiration_at=expiry, price_multiplier=amount / increment,
                currency=currency,
                specification_key=f"{asset}|{size:g}|{currency}|{meta.get('settlement_type', '')}",
            ))
        except (KeyError, ValueError, TypeError, OverflowError):
            logger.warning("Ignored incomplete curve specification for %s", item.get("ticker"))
    if not raw.get("enabled", False):
        contracts = []
    return CurveRuntimeConfiguration(
        contracts=tuple(contracts), settings=settings,
        version="sha256:" + sha256(instrument_bytes + policy_bytes).hexdigest(),
        secondary_instrument_ids=tuple(secondary),
    )


class FuturesCurveRuntimeAdapter:
    """Isolated curve state; invalid reloads retain the last valid configuration."""

    def __init__(
        self, *, factory: Callable[[], tuple[FuturesCurveDetector, CurveRuntimeConfiguration]],
        instruments_path: Path, policy_path: Path, state_path: Path,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._factory = factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._paths = (instruments_path, policy_path)
        self._engine, self._config = factory()
        self._state_path = state_path
        self._last_poll = 0.0
        self._last_save = 0.0
        self._mtimes = self._signature()
        self._restore()
        logger.info("Futures curve enabled contracts=%d config=%s", len(self._config.contracts), self._config.version)

    def _signature(self) -> tuple[int, ...]:
        return tuple(path.stat().st_mtime_ns if path.exists() else 0 for path in self._paths)

    def is_secondary(self, instrument_id: str) -> bool:
        return instrument_id in self._config.secondary_instrument_ids

    def _restore(self) -> None:
        try:
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
            # After a stream gap, warm from new quotes and activity. Never join
            # yesterday's observations to today's curve as if nothing happened.
            age = datetime.now(timezone.utc).timestamp() - self._state_path.stat().st_mtime
            if 0 <= age <= 180:
                self._engine.restore_state(state)
        except (OSError, ValueError, TypeError, KeyError):
            logger.info("Futures curve starts with a cold baseline")

    def _reload(self) -> None:
        now = time.monotonic()
        if now - self._last_poll < 10:
            return
        self._last_poll = now
        signature = self._signature()
        if signature == self._mtimes:
            return
        try:
            engine, config = self._factory()
            # Fingerprint-aware domain restore preserves compatible history only.
            engine.restore_state(self._engine.export_state())
        except (OSError, ValueError, TypeError, KeyError):
            logger.exception("Ignored invalid futures curve configuration")
            return
        self._engine, self._config = engine, config
        self._mtimes = signature
        logger.info("Futures curve reloaded contracts=%d config=%s", len(config.contracts), config.version)

    def checkpoint(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_save < 30:
            return
        self._last_save = now
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            body = json.dumps(self._engine.export_state(), allow_nan=False)
            descriptor, filename = tempfile.mkstemp(prefix=".futures-curve-", dir=self._state_path.parent)
            temporary = Path(filename)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self._state_path)
            finally:
                temporary.unlink(missing_ok=True)
        except (OSError, ValueError, TypeError):
            logger.exception("Could not persist futures curve baseline; restart will warm again")

    def reset_after_rollback(self) -> None:
        # Detection precedes the PostgreSQL transaction. A failed transaction
        # must not leave an uncommitted quote, candidate or cooldown in memory.
        self._engine, self._config = self._factory()
        logger.warning("Futures curve cold-reset after processing rollback")

    def process(self, event: NormalizedEvent, *, emit_signals: bool) -> tuple[TriggerSignal, ...]:
        self._reload()
        if event.class_code != "SPBFUT" or event.payload.get("recovery_backfill"):
            return ()
        payload = event.payload
        bid = ask = None
        quantity = 0.0
        oi = None
        if event.event_type == "orderbook":
            try:
                bids, asks = payload.get("bids") or [], payload.get("asks") or []
                bid = max(quotation_to_float(row.get("price")) or 0 for row in bids) if bids else None
                ask = min(quotation_to_float(row.get("price")) or math.inf for row in asks) if asks else None
                if payload.get("is_consistent") is False:
                    bid = ask = None
            except (ValueError, TypeError, AttributeError):
                bid = ask = None
        elif event.event_type == "trade":
            try:
                quantity = float(payload.get("quantity") or 0)
            except (ValueError, TypeError, OverflowError):
                return ()
        elif event.event_type == "open_interest":
            try:
                oi = float(payload.get("open_interest"))
            except (ValueError, TypeError, OverflowError):
                return ()
        else:
            return ()
        try:
            trade_price = quotation_to_float(payload.get("price")) if event.event_type == "trade" else None
        except (ValueError, TypeError, OverflowError):
            return ()
        # Kafka queueing time is part of quote age. A fresh timestamp at ingest
        # must not let an old queued quote pass the realtime freshness gate.
        processed_at = max(event.received_at, self._clock())
        found = self._engine.process(CurveMarketEvent(
            event_id=event.event_id, instrument_id=event.instrument_id,
            at=event.source_time, received_at=processed_at,
            kind=event.event_type, bid=bid, ask=ask, quantity=quantity, open_interest=oi,
            trade_price=trade_price,
        ), emit_signals=emit_signals)
        if not emit_signals:
            return ()
        return tuple(TriggerSignal(
            signal_id=str(uuid5(NAMESPACE_URL, f"curve:{event.event_id}:{item.signal_type}:{item.chain_id}")),
            detected_at=processed_at, instrument_id=item.instrument_id,
            ticker=item.ticker, class_code="SPBFUT", alias=item.chain_id,
            source_event_type=event.event_type, signal_type=item.signal_type,
            severity=3, metric_value=item.metric_value, baseline_value=item.baseline_value,
            z_score=item.z_score, window_seconds=(
                int(item.payload.get("momentum_horizon_seconds", 3600))
                if item.signal_type == "futures_basis_momentum" else 300
            ),
            summary=f"{item.signal_type}: {item.chain_id}",
            payload={**item.payload, "curve_config_version": self._config.version,
                     **({"annual_premium_change": item.metric_value - item.baseline_value}
                        if item.signal_type == "term_premium_shock" else {}),
                     **({"horizon_seconds": item.payload.get("momentum_horizon_seconds", 3600),
                         "momentum_difference_bps": item.metric_value}
                        if item.signal_type == "futures_basis_momentum" else {})},
            source_event_id=event.event_id, source_event_at=event.source_time,
            detector_config_version=self._config.version,
            signal_schema_version="1.0.0", provenance_status="complete",
        ) for item in found)

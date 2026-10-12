"""T-Invest and Kafka adapters for the bond convergence use case."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

from tinkoff.invest import Client
from tinkoff.invest.schemas import CandleInterval, InstrumentStatus

from tinvest_signal_engine.domain.bond_convergence import (
    POLICY_VERSION,
    TARGET_TRADING_SESSIONS,
    BondConvergenceSnapshot,
)
from tinvest_signal_engine.models import NormalizedEvent
from tinvest_signal_engine.serialization import utc_now


_EVENT_NAMESPACE = UUID("291e0740-d2ce-5a0e-b63e-3355c821b826")
_SOURCE_EVENT_VERSION = "2"
_RISK_LEVELS = {1: "low", 2: "moderate", 3: "high"}


def quotation_decimal(value: object) -> Decimal:
    units = int(getattr(value, "units", 0) or 0)
    nano = int(getattr(value, "nano", 0) or 0)
    return Decimal(units) + Decimal(nano) / Decimal("1000000000")


def count_sessions_to_maturity(
    trading_days: list[object],
    *,
    after_date: date,
    maturity_date: date,
) -> int:
    return sum(
        1
        for day in trading_days
        if bool(getattr(day, "is_trading_day", False))
        and after_date < getattr(day, "date").date() <= maturity_date
    )


def estimate_weekday_sessions(*, after_date: date, maturity_date: date) -> int:
    """Estimate future sessions when the broker API cannot expose a long calendar."""

    cursor = after_date + timedelta(days=1)
    sessions = 0
    while cursor <= maturity_date:
        if cursor.weekday() < 5:
            sessions += 1
        cursor += timedelta(days=1)
    return sessions


class TInvestBondConvergenceSource:
    def __init__(self, *, token: str, target: str | None, app_name: str) -> None:
        self._token = token
        self._target = target
        self._app_name = app_name

    def load_snapshots(self) -> tuple[BondConvergenceSnapshot, ...]:
        now = utc_now()
        today = now.date()
        horizon = today + timedelta(days=150)
        with Client(
            self._token,
            target=self._target,
            app_name=self._app_name,
        ) as client:
            bonds = client.instruments.bonds(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
            ).instruments
            candidates = [
                bond
                for bond in bonds
                if today < bond.maturity_date.date() <= horizon
                and _static_candidate(bond)
            ]
            if not candidates:
                return ()
            snapshots: list[BondConvergenceSnapshot] = []
            for bond in candidates:
                approximate_sessions = estimate_weekday_sessions(
                    after_date=today,
                    maturity_date=bond.maturity_date.date(),
                )
                if not TARGET_TRADING_SESSIONS - 10 <= approximate_sessions <= TARGET_TRADING_SESSIONS:
                    continue
                candle = _candle_at_target_session(
                    client,
                    bond.uid,
                    now,
                    maturity_date=bond.maturity_date.date(),
                )
                if candle is None:
                    continue
                sessions = estimate_weekday_sessions(
                    after_date=candle.time.date(),
                    maturity_date=bond.maturity_date.date(),
                )
                snapshots.append(
                    BondConvergenceSnapshot(
                        instrument_id=bond.uid,
                        ticker=bond.ticker,
                        class_code=bond.class_code,
                        alias=bond.name,
                        figi=bond.figi,
                        uid=bond.uid,
                        lot=int(bond.lot or 0),
                        source_time=_aware(candle.time),
                        maturity_date=bond.maturity_date.date(),
                        clean_price=quotation_decimal(candle.close),
                        sessions_to_maturity=sessions,
                        currency=bond.currency,
                        country_of_risk=bond.country_of_risk,
                        risk_level=_RISK_LEVELS.get(int(bond.risk_level), "unknown"),
                        amortization=bool(bond.amortization_flag),
                        subordinated=bool(bond.subordinated_flag),
                        perpetual=bool(bond.perpetual_flag),
                        api_trade_available=bool(bond.api_trade_available_flag),
                        buy_available=bool(bond.buy_available_flag),
                        sell_available=bool(bond.sell_available_flag),
                    )
                )
            return tuple(snapshots)


class KafkaBondConvergencePublisher:
    def __init__(self, *, producer: Any, topic: str, protobuf_values: bool) -> None:
        self._producer = producer
        self._topic = topic
        self._protobuf_values = protobuf_values

    def publish(self, snapshot: BondConvergenceSnapshot) -> None:
        event_id = str(
            uuid5(
                _EVENT_NAMESPACE,
                "\x1f".join(
                    (
                        snapshot.uid,
                        snapshot.source_time.isoformat(),
                        snapshot.maturity_date.isoformat(),
                        POLICY_VERSION,
                        _SOURCE_EVENT_VERSION,
                    )
                ),
            )
        )
        event = NormalizedEvent(
            event_id=event_id,
            event_type="bond_convergence_observation",
            instrument_id=snapshot.instrument_id,
            ticker=snapshot.ticker,
            class_code=snapshot.class_code,
            alias=snapshot.alias,
            figi=snapshot.figi,
            uid=snapshot.uid,
            lot=snapshot.lot,
            source_time=snapshot.source_time,
            received_at=utc_now(),
            payload={
                "maturity_date": snapshot.maturity_date.isoformat(),
                "clean_price": float(snapshot.clean_price),
                "sessions_to_maturity": snapshot.sessions_to_maturity,
                "session_count_basis": "weekday_estimate",
                "source_event_version": _SOURCE_EVENT_VERSION,
                "currency": snapshot.currency,
                "country_of_risk": snapshot.country_of_risk,
                "risk_level": snapshot.risk_level,
                "amortization": snapshot.amortization,
                "subordinated": snapshot.subordinated,
                "perpetual": snapshot.perpetual,
                "api_trade_available": snapshot.api_trade_available,
                "buy_available": snapshot.buy_available,
                "sell_available": snapshot.sell_available,
            },
        )
        value = event if self._protobuf_values else event.to_dict()
        self._producer.send(
            self._topic,
            key=snapshot.instrument_id,
            value=value,
        )


def _static_candidate(bond: object) -> bool:
    return bool(
        str(getattr(bond, "currency", "")).lower() == "rub"
        and str(getattr(bond, "country_of_risk", "")).upper() == "RU"
        and int(getattr(bond, "risk_level", 0)) in {1, 2}
        and not bool(getattr(bond, "amortization_flag", False))
        and not bool(getattr(bond, "subordinated_flag", False))
        and not bool(getattr(bond, "perpetual_flag", False))
        and bool(getattr(bond, "api_trade_available_flag", False))
        and bool(getattr(bond, "buy_available_flag", False))
        and bool(getattr(bond, "sell_available_flag", False))
    )


def _candle_at_target_session(
    client: Client,
    uid: str,
    now: datetime,
    *,
    maturity_date: date,
) -> object | None:
    response = client.market_data.get_candles(
        instrument_id=uid,
        from_=now - timedelta(days=14),
        to=now,
        interval=CandleInterval.CANDLE_INTERVAL_DAY,
        limit=14,
    )
    complete = sorted(
        (item for item in response.candles if item.is_complete),
        key=lambda item: item.time,
        reverse=True,
    )
    return next(
        (
            item
            for item in complete
            if estimate_weekday_sessions(
                after_date=item.time.date(),
                maturity_date=maturity_date,
            )
            == TARGET_TRADING_SESSIONS
        ),
        None,
    )


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

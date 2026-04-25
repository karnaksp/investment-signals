from __future__ import annotations

from dataclasses import dataclass

from tinkoff.invest import InstrumentIdType

from .config import InstrumentSubscriptionConfig


@dataclass(frozen=True)
class InstrumentMetadata:
    instrument_id: str
    ticker: str
    class_code: str
    alias: str
    figi: str
    uid: str
    lot: int
    currency: str
    name: str


class InstrumentRegistry:
    def __init__(self) -> None:
        self._by_instrument_id: dict[str, InstrumentMetadata] = {}
        self._by_figi: dict[str, InstrumentMetadata] = {}
        self._by_uid: dict[str, InstrumentMetadata] = {}

    def add(self, metadata: InstrumentMetadata) -> None:
        self._by_instrument_id[metadata.instrument_id] = metadata
        if metadata.figi:
            self._by_figi[metadata.figi] = metadata
        if metadata.uid:
            self._by_uid[metadata.uid] = metadata

    def resolve(
        self,
        *,
        instrument_id: str = "",
        figi: str = "",
        uid: str = "",
    ) -> InstrumentMetadata | None:
        if instrument_id and instrument_id in self._by_instrument_id:
            return self._by_instrument_id[instrument_id]
        if figi and figi in self._by_figi:
            return self._by_figi[figi]
        if uid and uid in self._by_uid:
            return self._by_uid[uid]
        return None

    def __iter__(self):
        return iter(self._by_instrument_id.values())


def build_instrument_registry(
    client,
    instrument_configs: list[InstrumentSubscriptionConfig],
) -> InstrumentRegistry:
    registry = InstrumentRegistry()
    for item in instrument_configs:
        response = client.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
            class_code=item.class_code,
            id=item.ticker,
        )
        instrument = response.instrument
        registry.add(
            InstrumentMetadata(
                instrument_id=item.instrument_id,
                ticker=instrument.ticker or item.ticker,
                class_code=instrument.class_code or item.class_code,
                alias=item.alias,
                figi=instrument.figi,
                uid=instrument.uid,
                lot=instrument.lot,
                currency=instrument.currency,
                name=instrument.name,
            )
        )
    return registry


"""Resume-safe import of local historical candles into the scientific journal."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable, Protocol
from uuid import uuid4

from tinvest_signal_engine.domain.historical_candle_import import (
    HistoricalCandleImportInventory,
    HistoricalCandleImportProgress,
    HistoricalCandleImportResult,
    HistoricalCandlePartition,
    HistoricalCandlePartitionDescriptor,
    HistoricalImportPartitionProgress,
    HistoricalImportState,
    PersistedCandleSnapshot,
    scientific_market_fingerprint,
)
from tinvest_signal_engine.domain.scientific_candles import ScientificCandle


class HistoricalCandleImportSourcePort(Protocol):
    def inventory(self) -> HistoricalCandleImportInventory: ...

    def load_partition(
        self,
        descriptor: HistoricalCandlePartitionDescriptor,
        *,
        instrument_id: str,
        received_at: datetime,
    ) -> HistoricalCandlePartition: ...


class HistoricalCandleImportDestinationPort(Protocol):
    def inspect_partitions(
        self,
        requests: tuple["HistoricalDestinationPartition", ...],
    ) -> tuple[PersistedCandleSnapshot, ...]: ...

    def persist_many(self, candles: tuple[ScientificCandle, ...]) -> None: ...


class HistoricalCandleImportProgressPort(Protocol):
    def load(self) -> HistoricalCandleImportProgress | None: ...

    def save(self, progress: HistoricalCandleImportProgress) -> None: ...

    def publish_result(self, result: HistoricalCandleImportResult) -> None: ...


@dataclass(frozen=True, slots=True)
class HistoricalDestinationPartition:
    descriptor: HistoricalCandlePartitionDescriptor
    instrument_id: str

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("historical destination instrument_id is required")


class ImportHistoricalScientificCandles:
    """Import immutable partitions in bounded, idempotent batches."""

    def __init__(
        self,
        *,
        source: HistoricalCandleImportSourcePort,
        destination: HistoricalCandleImportDestinationPort,
        progress: HistoricalCandleImportProgressPort,
        instrument_ids: dict[str, str],
        batch_size: int = 50_000,
        partition_group_size: int = 50,
        now: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    ) -> None:
        if batch_size <= 0:
            raise ValueError("historical import batch_size must be positive")
        if partition_group_size <= 0:
            raise ValueError("historical import partition_group_size must be positive")
        normalized = {
            ticker.strip().upper(): instrument_id.strip()
            for ticker, instrument_id in instrument_ids.items()
        }
        if not normalized or any(not key or not value for key, value in normalized.items()):
            raise ValueError("historical import instrument mapping is required")
        self._source = source
        self._destination = destination
        self._progress = progress
        self._instrument_ids = normalized
        self._batch_size = batch_size
        self._partition_group_size = partition_group_size
        self._now = now

    def execute(self, *, dry_run: bool = False) -> HistoricalCandleImportResult:
        inventory = self._source.inventory()
        started = _aware_utc(self._now(), "now")
        current = None if dry_run else self._progress.load()
        progress = self._resume_or_start(current, inventory, started, dry_run=dry_run)
        completed = {item.key: item for item in progress.partitions}
        planned: list[HistoricalImportPartitionProgress] = []
        insert_batches = 0
        query_batches = 0
        try:
            for descriptors in _groups(
                inventory.partitions,
                self._partition_group_size,
            ):
                pending = tuple(
                    descriptor
                    for descriptor in descriptors
                    if descriptor.key not in completed
                )
                for descriptor in descriptors:
                    prior = completed.get(descriptor.key)
                    if prior is not None and prior.file_checksum != descriptor.file_checksum:
                        raise HistoricalImportConflict(
                            "completed source partition changed after watermark"
                        )
                if not pending:
                    continue
                requests: list[HistoricalDestinationPartition] = []
                partitions: list[HistoricalCandlePartition] = []
                for descriptor in pending:
                    instrument_id = self._instrument_ids.get(descriptor.key.ticker)
                    if instrument_id is None:
                        raise HistoricalImportConflict(
                            f"instrument mapping is missing for {descriptor.key.ticker}"
                        )
                    request = HistoricalDestinationPartition(descriptor, instrument_id)
                    requests.append(request)
                    partitions.append(
                        self._source.load_partition(
                            descriptor,
                            instrument_id=instrument_id,
                            received_at=progress.started_at,
                        )
                    )
                existing = self._destination.inspect_partitions(tuple(requests))
                query_batches += 1
                missing_by_partition: list[tuple[ScientificCandle, ...]] = []
                existing_counts: list[int] = []
                for partition, request in zip(partitions, requests):
                    rows = _select_persisted(existing, request)
                    missing, existing_count = _reconcile(partition, rows)
                    missing_by_partition.append(missing)
                    existing_counts.append(existing_count)
                all_missing = tuple(
                    candle
                    for partition_missing in missing_by_partition
                    for candle in partition_missing
                )
                if not dry_run:
                    for batch in _batches(all_missing, self._batch_size):
                        self._destination.persist_many(batch)
                        insert_batches += 1
                    verified = self._destination.inspect_partitions(tuple(requests))
                    query_batches += 1
                    for partition, request in zip(partitions, requests):
                        _, verified_count = _reconcile(
                            partition,
                            _select_persisted(verified, request),
                        )
                        if verified_count != len(partition.candles):
                            raise RuntimeError(
                                "destination verification did not reconcile"
                            )
                completed_at = _aware_utc(self._now(), "now")
                group_progress = tuple(
                    HistoricalImportPartitionProgress(
                        key=partition.descriptor.key,
                        file_checksum=partition.descriptor.file_checksum,
                        content_fingerprint=partition.content_fingerprint,
                        source_rows=len(partition.candles),
                        inserted_rows=len(missing),
                        existing_rows=existing_count,
                        gap_rows=sum(item.has_gap for item in partition.candles),
                        completed_at=completed_at,
                    )
                    for partition, missing, existing_count in zip(
                        partitions,
                        missing_by_partition,
                        existing_counts,
                    )
                )
                planned.extend(group_progress)
                if not dry_run:
                    completed.update((item.key, item) for item in group_progress)
                    progress = replace(
                        progress,
                        updated_at=completed_at,
                        partitions=tuple(
                            sorted(completed.values(), key=lambda value: value.key)
                        ),
                    )
                    self._progress.save(progress)
        except Exception:
            if not dry_run:
                failed = replace(
                    progress,
                    state=HistoricalImportState.FAILED,
                    updated_at=_aware_utc(self._now(), "now"),
                    failure_reason_code="historical_candle_import_failed",
                )
                self._progress.save(failed)
            raise

        effective = (*progress.partitions, *planned) if dry_run else progress.partitions
        if not dry_run:
            progress = replace(
                progress,
                state=HistoricalImportState.COMPLETED,
                updated_at=_aware_utc(self._now(), "now"),
                failure_reason_code=None,
            )
            self._progress.save(progress)
            effective = progress.partitions
        result = HistoricalCandleImportResult(
            run_id=progress.run_id,
            state=(HistoricalImportState.COMPLETED if len(effective) == len(inventory.partitions) else HistoricalImportState.RUNNING),
            dry_run=dry_run,
            inventory_fingerprint=inventory.inventory_fingerprint,
            total_partitions=len(inventory.partitions),
            completed_partitions=len(effective),
            source_rows=sum(item.source_rows for item in effective),
            inserted_rows=sum(item.inserted_rows for item in effective),
            existing_rows=sum(item.existing_rows for item in effective),
            manifest_covered_partitions=inventory.manifest_covered_partitions,
            insert_batches=insert_batches,
            query_batches=query_batches,
            gap_rows=sum(item.gap_rows for item in effective),
        )
        if not dry_run:
            self._progress.publish_result(result)
        return result

    @staticmethod
    def _resume_or_start(
        current: HistoricalCandleImportProgress | None,
        inventory: HistoricalCandleImportInventory,
        started: datetime,
        *,
        dry_run: bool,
    ) -> HistoricalCandleImportProgress:
        if current is not None:
            if current.inventory_fingerprint != inventory.inventory_fingerprint:
                raise HistoricalImportConflict(
                    "source inventory changed after the import was started"
                )
            if current.source_manifest_checksum != inventory.source_manifest_checksum:
                raise HistoricalImportConflict(
                    "source manifest changed after the import was started"
                )
            if current.state is HistoricalImportState.COMPLETED:
                return current
            return replace(
                current,
                state=HistoricalImportState.RUNNING,
                updated_at=started,
                failure_reason_code=None,
            )
        return HistoricalCandleImportProgress(
            run_id=f"historical-candles-{uuid4().hex}",
            state=HistoricalImportState.RUNNING,
            inventory_fingerprint=inventory.inventory_fingerprint,
            source_manifest_checksum=inventory.source_manifest_checksum,
            started_at=started,
            updated_at=started,
            total_partitions=len(inventory.partitions),
            manifest_covered_partitions=inventory.manifest_covered_partitions,
        )


class HistoricalImportConflict(RuntimeError):
    """Raised before a source or destination conflict can be overwritten."""


def _reconcile(
    partition: HistoricalCandlePartition,
    persisted: tuple[PersistedCandleSnapshot, ...],
) -> tuple[tuple[ScientificCandle, ...], int]:
    by_time: dict[datetime, PersistedCandleSnapshot] = {}
    for row in persisted:
        timestamp = row.candle_at.astimezone(timezone.utc)
        current = by_time.get(timestamp)
        if current is not None and current.market_fingerprint != row.market_fingerprint:
            raise HistoricalImportConflict(
                "destination contains conflicting logical candle revisions"
            )
        by_time[timestamp] = row
    missing: list[ScientificCandle] = []
    existing = 0
    for candle in partition.candles:
        timestamp = candle.candle_at.astimezone(timezone.utc)
        row = by_time.get(timestamp)
        if row is None:
            missing.append(candle)
            continue
        if row.market_fingerprint != scientific_market_fingerprint(candle):
            raise HistoricalImportConflict(
                f"destination conflicts with source candle at {timestamp.isoformat()}"
            )
        existing += 1
    return tuple(missing), existing


def _batches(
    candles: tuple[ScientificCandle, ...],
    batch_size: int,
) -> tuple[tuple[ScientificCandle, ...], ...]:
    return tuple(
        candles[offset : offset + batch_size]
        for offset in range(0, len(candles), batch_size)
    )


def _groups(
    descriptors: tuple[HistoricalCandlePartitionDescriptor, ...],
    group_size: int,
) -> tuple[tuple[HistoricalCandlePartitionDescriptor, ...], ...]:
    return tuple(
        descriptors[offset : offset + group_size]
        for offset in range(0, len(descriptors), group_size)
    )


def _select_persisted(
    rows: tuple[PersistedCandleSnapshot, ...],
    request: HistoricalDestinationPartition,
) -> tuple[PersistedCandleSnapshot, ...]:
    return tuple(
        row
        for row in rows
        if row.instrument_id == request.instrument_id
        and row.trading_day == request.descriptor.key.trading_day
    )


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)

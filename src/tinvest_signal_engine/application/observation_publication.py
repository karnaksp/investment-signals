"""Durable detector-observation publication use case and ports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Protocol, Sequence

from tinvest_signal_engine.domain.detector_observations import DetectorObservation
from tinvest_signal_engine.domain.reliable_processing import retry_decision


@dataclass(frozen=True)
class ObservationPublicationTask:
    observation: DetectorObservation
    attempt_count: int

    def __post_init__(self) -> None:
        if self.attempt_count < 1:
            raise ValueError("claimed observation attempt_count must be positive")


class ObservationPublicationQueue(Protocol):
    def claim_many(
        self,
        *,
        available_at: datetime,
        lease_until: datetime,
        limit: int,
    ) -> tuple[ObservationPublicationTask, ...]: ...

    def mark_published(
        self,
        task: ObservationPublicationTask,
        *,
        published_at: datetime,
    ) -> None: ...

    def mark_failed(
        self,
        task: ObservationPublicationTask,
        *,
        reason_code: str,
        next_attempt_at: datetime,
        dead_letter: bool,
    ) -> None: ...

    def purge_published(self, *, before: datetime, limit: int) -> int: ...


class ObservationSink(Protocol):
    def persist_many(self, observations: Sequence[DetectorObservation]) -> None: ...


class ObservationPublicationMetrics(Protocol):
    def publication_attempted(
        self,
        *,
        outcome: str,
        attempt_count: int,
    ) -> None: ...


class ObservationPublicationFailure(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class ObservationPublicationResult:
    outcome: str
    tasks: tuple[ObservationPublicationTask, ...]


class DurableObservationPublisher:
    def __init__(
        self,
        *,
        queue: ObservationPublicationQueue,
        sink: ObservationSink,
        metrics: ObservationPublicationMetrics,
        clock: Callable[[], datetime],
        lease_seconds: int,
        batch_size: int,
        maximum_attempts: int,
        retry_base_seconds: int,
        retry_maximum_seconds: int,
    ) -> None:
        self._queue = queue
        self._sink = sink
        self._metrics = metrics
        self._clock = clock
        self._lease_seconds = max(1, lease_seconds)
        self._batch_size = max(1, batch_size)
        self._maximum_attempts = max(1, maximum_attempts)
        self._retry_base_seconds = max(1, retry_base_seconds)
        self._retry_maximum_seconds = max(1, retry_maximum_seconds)

    def run_once(self) -> ObservationPublicationResult:
        now = self._clock()
        tasks = self._queue.claim_many(
            available_at=now,
            lease_until=now + timedelta(seconds=self._lease_seconds),
            limit=self._batch_size,
        )
        if not tasks:
            return ObservationPublicationResult("idle", ())
        try:
            self._sink.persist_many(tuple(task.observation for task in tasks))
        except ObservationPublicationFailure as failure:
            failed_at = self._clock()
            outcomes: set[str] = set()
            for task in tasks:
                decision = retry_decision(
                    attempt_count=task.attempt_count,
                    maximum_attempts=self._maximum_attempts,
                    base_delay_seconds=self._retry_base_seconds,
                    maximum_delay_seconds=self._retry_maximum_seconds,
                )
                self._queue.mark_failed(
                    task,
                    reason_code=failure.reason_code,
                    next_attempt_at=(
                        failed_at + timedelta(seconds=decision.delay_seconds)
                    ),
                    dead_letter=decision.dead_letter,
                )
                outcome = "dead_letter" if decision.dead_letter else "retry"
                outcomes.add(outcome)
                self._metrics.publication_attempted(
                    outcome=outcome,
                    attempt_count=task.attempt_count,
                )
            batch_outcome = "dead_letter" if outcomes == {"dead_letter"} else "retry"
            return ObservationPublicationResult(batch_outcome, tasks)

        published_at = self._clock()
        for task in tasks:
            self._queue.mark_published(task, published_at=published_at)
            self._metrics.publication_attempted(
                outcome="published",
                attempt_count=task.attempt_count,
            )
        return ObservationPublicationResult("published", tasks)

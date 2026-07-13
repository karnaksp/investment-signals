"""At-least-once ClickHouse transport for immutable detector observations.

Retries may create duplicate rows after an ambiguous HTTP result. Evidence readers
must group by ``observation_id`` and require one canonical ``payload_fingerprint``;
transport deduplication is deliberately not an evidence-correctness mechanism.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.observation_publication import (
    ObservationPublicationFailure,
)
from tinvest_signal_engine.domain.detector_observations import DetectorObservation
from tinvest_signal_engine.serialization import parse_timestamp

_MOSCOW = ZoneInfo("Europe/Moscow")
_INSERT_SQL = "INSERT INTO detector_observations FORMAT JSONEachRow\n"
_FINGERPRINT_FIELDS = frozenset(
    {
        "signal_type",
        "instrument_id",
        "session_date",
        "observed_at",
        "observation_id",
        "source_event_id",
        "detector_config_version",
        "expectation_catalog_version",
        "metric_value",
        "threshold_value",
        "threshold_passed",
        "sample_weight",
        "features_json",
    }
)


class ClickHouseDetectorObservationSink:
    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        if timeout_seconds <= 0:
            raise ValueError("ClickHouse timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    def persist(self, observation: DetectorObservation) -> None:
        self.persist_many((observation,))

    def persist_many(
        self,
        observations: Sequence[DetectorObservation],
    ) -> None:
        if not observations:
            return
        rows = [_row(observation) for observation in observations]
        body = _INSERT_SQL + "\n".join(
            json.dumps(
                row,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            for row in rows
        )
        request = Request(
            f"{self._base_url}/?{urlencode({'database': self._database})}",
            data=body.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                response.read()
        except HTTPError as error:
            raise ObservationPublicationFailure(
                f"clickhouse_http_{error.code}"
            ) from error
        except (URLError, TimeoutError, ConnectionResetError) as error:
            raise ObservationPublicationFailure("clickhouse_unavailable") from error


def _row(observation: DetectorObservation) -> dict[str, object]:
    features = {
        "baseline_value": observation.baseline_value,
        "detector_passed": observation.detector_passed,
        "provenance_status": observation.provenance_status,
        "sampling_policy_version": observation.sampling_policy_version,
        "signal_emitted": observation.signal_emitted,
        "source_event_type": observation.source_event_type,
        "window_seconds": observation.window_seconds,
        "z_score": observation.z_score,
    }
    row: dict[str, object] = {
        "signal_type": observation.signal_type,
        "instrument_id": observation.instrument_id,
        "session_date": observation.observed_at.astimezone(_MOSCOW).date().isoformat(),
        "observed_at": _canonical_utc_timestamp(observation.observed_at),
        "observation_id": observation.observation_id,
        "source_event_id": observation.source_event_id,
        "detector_config_version": observation.detector_config_version,
        "expectation_catalog_version": observation.expectation_catalog_version or "",
        "metric_value": observation.metric_value,
        "threshold_value": observation.threshold_value,
        "threshold_passed": 1 if observation.threshold_passed else 0,
        "sample_weight": 1.0,
        "features_json": json.dumps(
            features,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    }
    row["payload_fingerprint"] = detector_observation_payload_fingerprint(row)
    return row


def detector_observation_payload_fingerprint(
    row_without_fingerprint: Mapping[str, object],
) -> str:
    """Reproduce the stored fingerprint from producer or ClickHouse readback values."""

    if set(row_without_fingerprint) != _FINGERPRINT_FIELDS:
        raise ValueError("detector observation fingerprint fields do not match contract")
    features = row_without_fingerprint["features_json"]
    if not isinstance(features, str):
        raise ValueError("detector observation features_json must be a string")
    decoded_features = json.loads(features)
    if not isinstance(decoded_features, dict):
        raise ValueError("detector observation features_json must contain an object")
    threshold_passed = int(row_without_fingerprint["threshold_passed"])
    if threshold_passed not in (0, 1):
        raise ValueError("detector observation threshold_passed must be 0 or 1")
    canonical_row = {
        "signal_type": str(row_without_fingerprint["signal_type"]),
        "instrument_id": str(row_without_fingerprint["instrument_id"]),
        "session_date": str(row_without_fingerprint["session_date"]),
        "observed_at": _canonical_utc_timestamp(
            row_without_fingerprint["observed_at"]
        ),
        "observation_id": str(row_without_fingerprint["observation_id"]),
        "source_event_id": str(row_without_fingerprint["source_event_id"]),
        "detector_config_version": str(
            row_without_fingerprint["detector_config_version"]
        ),
        "expectation_catalog_version": str(
            row_without_fingerprint["expectation_catalog_version"]
        ),
        "metric_value": float(row_without_fingerprint["metric_value"]),
        "threshold_value": float(row_without_fingerprint["threshold_value"]),
        "threshold_passed": threshold_passed,
        "sample_weight": float(row_without_fingerprint["sample_weight"]),
        "features_json": json.dumps(
            decoded_features,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    }
    canonical = json.dumps(
        canonical_row,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def _canonical_utc_timestamp(value: object) -> str:
    if not isinstance(value, (str, datetime)):
        raise ValueError("detector observation observed_at must be a timestamp")
    return (
        parse_timestamp(value)
        .astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )

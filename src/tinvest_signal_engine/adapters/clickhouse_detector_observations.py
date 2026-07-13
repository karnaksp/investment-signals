"""ClickHouse sink for validated detector observations."""

from __future__ import annotations

import json
from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.observation_publication import (
    ObservationPublicationFailure,
)
from tinvest_signal_engine.domain.detector_observations import DetectorObservation


_MOSCOW = ZoneInfo("Europe/Moscow")

INSERT_DETECTOR_OBSERVATION_SQL = """
INSERT INTO detector_observations
(
    signal_type, instrument_id, session_date, observed_at,
    observation_id, source_event_id, detector_config_version,
    expectation_catalog_version, metric_value, threshold_value,
    threshold_passed, sample_weight, features_json
)
SELECT
    {signal_type:String},
    {instrument_id:String},
    toDate({session_date:String}),
    parseDateTime64BestEffort({observed_at:String}, 9, 'UTC'),
    toUUID({observation_id:String}),
    {source_event_id:String},
    {detector_config_version:String},
    {expectation_catalog_version:String},
    {metric_value:Float64},
    {threshold_value:Float64},
    {threshold_passed:UInt8},
    {sample_weight:Float64},
    {features_json:String}
WHERE NOT EXISTS
(
    SELECT 1
    FROM detector_observations
    WHERE observation_id = toUUID({observation_id:String})
)
""".strip()


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
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    def persist(self, observation: DetectorObservation) -> None:
        parameters = _parameters(observation)
        query = {
            "database": self._database,
            "insert_deduplication_token": observation.observation_id,
        }
        query.update({f"param_{key}": value for key, value in parameters.items()})
        request = Request(
            f"{self._base_url}/?{urlencode(query)}",
            data=INSERT_DETECTOR_OBSERVATION_SQL.encode("utf-8"),
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
        except URLError as error:
            raise ObservationPublicationFailure("clickhouse_unavailable") from error


def _parameters(observation: DetectorObservation) -> Mapping[str, str]:
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
    return {
        "signal_type": observation.signal_type,
        "instrument_id": observation.instrument_id,
        "session_date": observation.observed_at.astimezone(_MOSCOW).date().isoformat(),
        "observed_at": observation.observed_at.isoformat(),
        "observation_id": observation.observation_id,
        "source_event_id": observation.source_event_id,
        "detector_config_version": observation.detector_config_version,
        "expectation_catalog_version": (observation.expectation_catalog_version or ""),
        "metric_value": repr(observation.metric_value),
        "threshold_value": repr(observation.threshold_value),
        "threshold_passed": "1" if observation.threshold_passed else "0",
        "sample_weight": "1",
        "features_json": json.dumps(
            features,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    }

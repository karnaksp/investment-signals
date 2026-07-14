#!/usr/bin/env python3
"""Run a redacted directional event study on official T-Invest candles.

The study is intentionally separate from production outcome computation.  It
uses historical one-minute exchange candles to validate the automatic-verdict
math and to decide whether a continuation detector deserves further shadow
testing.  It does *not* claim tick/order-book or execution parity.

Only aggregate JSON and Markdown are written.  Tokens, instrument UIDs, account
identifiers, raw candles and individual event rows remain in memory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import ssl
import statistics
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

import httpx


API_ROOT = "https://invest-public-api.tbank.ru/rest/"
API_SERVICE = "tinkoff.public.invest.api.contract.v1"
MOSCOW = ZoneInfo("Europe/Moscow")
UTC = timezone.utc
REGULAR_SESSION_START = datetime_time(10, 5)
REGULAR_SESSION_END = datetime_time(18, 39)
SECRET_VALUE_PATTERN = re.compile(
    r"(?i)(bearer|token|password|secret|api[-_ ]?key)([=: ]+)([^\s,;]+)"
)


@dataclass(frozen=True, slots=True)
class StudyPolicy:
    version: str = "candle-continuation-study-v1.0.0"
    detector_window_minutes: int = 3
    # Production keeps 160 samples at 15 seconds (~40 minutes) and requires
    # 24 (~6 minutes). One-minute replay preserves duration, not point count.
    detector_baseline_points: int = 40
    detector_min_baseline_points: int = 6
    detector_z_score: float = 4.0
    detector_min_move_bps: float = 0.0
    detector_min_relative_excursion: float = 0.12
    cooldown_minutes: int = 5
    volatility_lookback_points: int = 30
    volatility_min_points: int = 20
    volatility_floor_bps: float = 2.0
    outcome_min_move_bps: float = 5.0
    outcome_volatility_multiplier: float = 0.75
    round_trip_cost_bps: float = 10.0
    # One-minute candles cannot reproduce the production +5s/+30s fallback.
    # Use the exact target candle and classify a missing/gapped path as inconclusive.
    forward_grace_minutes: int = 0
    primary_horizon_minutes: int = 5
    controls_per_event: int = 5
    bootstrap_samples: int = 4_000
    seed: int = 20_260_713


@dataclass(frozen=True, slots=True)
class Candle:
    ticker: str
    at: datetime
    open: float
    close: float
    complete: bool


@dataclass(frozen=True, slots=True)
class DetectedEvent:
    ticker: str
    trading_day: date
    session_bucket: int
    at: datetime
    direction: int
    event_move_bps: float
    baseline_move_bps: float
    detector_z_score: float
    baseline_sigma_bps: float


@dataclass(frozen=True, slots=True)
class EligibleObservation:
    ticker: str
    trading_day: date
    session_bucket: int
    at: datetime
    direction: int


@dataclass(frozen=True, slots=True)
class EventOutcome:
    event: DetectedEvent
    horizon_minutes: int
    gross_expected_bps: float | None
    net_expected_bps: float | None
    net_reverse_bps: float | None
    materiality_bps: float | None
    verdict: str
    reason_code: str | None = None


def load_env_value(path: Path, key: str) -> str:
    """Read one dotenv value without adding the secret to logs or errors."""

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        candidate, value = line.split("=", 1)
        if candidate.strip() == key:
            result = value.strip().strip('"').strip("'")
            if result:
                return result
    raise RuntimeError(f"Required environment key {key} is absent")


def quotation(value: dict | None) -> float:
    item = value or {}
    return float(item.get("units", "0")) + float(item.get("nano", 0)) / 1_000_000_000


def api_post(
    client: httpx.Client,
    method: str,
    payload: dict,
    *,
    attempts: int = 7,
) -> dict:
    url = f"{API_ROOT}{API_SERVICE}.{method}"
    for attempt in range(attempts):
        response = client.post(url, json=payload)
        if response.status_code == 200:
            body = response.json()
            if not isinstance(body, dict):
                raise RuntimeError(f"T-Invest {method} returned an invalid response")
            return body
        if response.status_code in {429, 500, 502, 503, 504}:
            time.sleep(min(20.0, 0.75 * (2**attempt)))
            continue
        try:
            error = response.json()
        except ValueError:
            error = {}
        code = str(error.get("code", "unknown"))[:80]
        message = str(error.get("message", "unspecified"))[:240]
        raise RuntimeError(
            f"T-Invest {method} failed with HTTP {response.status_code}; "
            f"code={code}; message={message}"
        )
    raise RuntimeError(f"T-Invest {method} did not recover after bounded retries")


def redact_diagnostic(value: object, *, limit: int = 360) -> str:
    """Return bounded diagnostic text that is safe to persist in study artifacts."""

    text = str(value).replace("\n", " ").replace("\r", " ")
    text = SECRET_VALUE_PATTERN.sub(r"\1\2<redacted>", text)
    if len(text) > limit:
        return text[: limit - 1] + "…"
    return text


def classify_tinvest_failure(exc: BaseException) -> tuple[str, str, str]:
    """Classify a failed T-Invest study without persisting sensitive details."""

    diagnostic = redact_diagnostic(exc)
    if isinstance(exc, httpx.ConnectError) and "CERTIFICATE_VERIFY_FAILED" in str(exc):
        return (
            "tls_certificate_verify_failed",
            "TLS certificate verification failed before the study could read T-Invest data.",
            "Keep TLS verification enabled. Install the intercepting/root CA into the OS trust store "
            "or pass the correct PEM bundle with --ca-cert, then rerun the study.",
        )
    if isinstance(exc, httpx.TimeoutException):
        return (
            "tinvest_timeout",
            "T-Invest request timed out before the study could complete.",
            "Check network connectivity and rerun with the same parameters; the script uses bounded retries.",
        )
    if isinstance(exc, httpx.HTTPError):
        return (
            "tinvest_transport_error",
            f"T-Invest transport failed: {diagnostic}",
            "Check local network/proxy/TLS configuration and rerun. Do not disable TLS verification.",
        )
    if isinstance(exc, RuntimeError) and "Required environment key" in str(exc):
        return (
            "required_environment_key_absent",
            diagnostic,
            "Provide a local .env containing TINVEST_TOKEN or pass --env-file pointing to one.",
        )
    return (
        "tinvest_study_failed",
        diagnostic,
        "Fix the reported local/API condition and rerun; no raw market rows or secrets were persisted.",
    )


def write_failure_artifact(
    output_dir: Path,
    *,
    scope: dict,
    reason_code: str,
    message: str,
    remediation: str,
    ca_cert: Path | None,
) -> Path:
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "generated_at": generated_at,
                "scope": scope,
                "reason_code": reason_code,
                "message": message,
                "ca_cert": str(ca_cert) if ca_cert is not None else None,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:12]
    run_dir = output_dir / f"failed-{fingerprint}"
    run_dir.mkdir(parents=True, exist_ok=False)
    payload = {
        "status": "failed",
        "generated_at": generated_at,
        "scope": scope,
        "reason_code": reason_code,
        "message": message,
        "remediation": remediation,
        "tls_verification": "enabled",
        "additional_ca_sha256": hashlib.sha256(ca_cert.read_bytes()).hexdigest()
        if ca_cert is not None
        else None,
        "data_boundary": {
            "raw_candles_persisted": False,
            "instrument_uids_persisted": False,
            "token_persisted": False,
        },
    }
    (run_dir / "failure.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (run_dir / "report.md").write_text(
        "\n".join(
            [
                "# T-Invest directional hypothesis study failed",
                "",
                "The study did not persist raw candles, instrument UIDs, account identifiers or tokens.",
                "",
                f"- Reason: `{reason_code}`",
                f"- Message: {message}",
                f"- Remediation: {remediation}",
                "- TLS verification: enabled",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return run_dir


def resolve_instruments(client: httpx.Client, tickers: Sequence[str]) -> dict[str, str]:
    """Resolve canonical TQBR share UIDs; UIDs are never persisted."""

    result: dict[str, str] = {}
    for ticker in tickers:
        body = api_post(
            client,
            "InstrumentsService/FindInstrument",
            {
                "query": ticker,
                "instrumentKind": "INSTRUMENT_TYPE_SHARE",
                "apiTradeAvailableFlag": True,
            },
        )
        matches = [
            item
            for item in body.get("instruments", [])
            if item.get("ticker") == ticker
            and item.get("classCode") == "TQBR"
            and item.get("apiTradeAvailableFlag") is True
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"Expected one canonical TQBR share for {ticker}, got {len(matches)}"
            )
        result[ticker] = str(matches[0]["uid"])
    return result


def fetch_candles(
    client: httpx.Client,
    instruments: dict[str, str],
    start_day: date,
    end_day: date,
    *,
    request_interval_seconds: float,
) -> list[Candle]:
    rows: list[Candle] = []
    day = start_day
    while day <= end_day:
        # One-minute API periods must be shorter than one day. Request only the
        # exchange daytime window needed by the predeclared study.
        start = datetime.combine(day, datetime_time(10, 0), tzinfo=MOSCOW).astimezone(UTC)
        end = datetime.combine(day, datetime_time(19, 0), tzinfo=MOSCOW).astimezone(UTC)
        for ticker, uid in instruments.items():
            body = api_post(
                client,
                "MarketDataService/GetCandles",
                {
                    "from": start.isoformat().replace("+00:00", "Z"),
                    "to": end.isoformat().replace("+00:00", "Z"),
                    "interval": "CANDLE_INTERVAL_1_MIN",
                    "instrumentId": uid,
                    "candleSourceType": "CANDLE_SOURCE_EXCHANGE",
                },
            )
            for item in body.get("candles", []):
                rows.append(
                    Candle(
                        ticker=ticker,
                        at=datetime.fromisoformat(str(item["time"]).replace("Z", "+00:00")),
                        open=quotation(item.get("open")),
                        close=quotation(item.get("close")),
                        complete=bool(item.get("isComplete", False)),
                    )
                )
            time.sleep(request_interval_seconds)
        day += timedelta(days=1)
    return rows


def is_regular_session(candle: Candle) -> bool:
    local = candle.at.astimezone(MOSCOW)
    return REGULAR_SESSION_START <= local.time().replace(tzinfo=None) <= REGULAR_SESSION_END


def bucket_for(at: datetime) -> int:
    local = at.astimezone(MOSCOW)
    minute = local.hour * 60 + local.minute
    start = REGULAR_SESSION_START.hour * 60 + REGULAR_SESSION_START.minute
    return (minute - start) // 60


def _mean_and_z_score(history: Iterable[float], value: float) -> tuple[float, float]:
    samples = tuple(history)
    baseline = statistics.mean(samples)
    sigma = statistics.pstdev(samples)
    if sigma <= 1e-12:
        return baseline, 999.0 if value > baseline else 0.0
    return baseline, (value - baseline) / sigma


def _forward_median(
    rows: Sequence[Candle],
    index: int,
    horizon_minutes: int,
    grace_minutes: int,
) -> float | None:
    event = rows[index]
    target = event.at + timedelta(minutes=horizon_minutes)
    deadline = target + timedelta(minutes=grace_minutes)
    event_day = event.at.astimezone(MOSCOW).date()
    values: list[float] = []
    expected_at = event.at + timedelta(minutes=1)
    for candidate in rows[index + 1 :]:
        if candidate.at > deadline:
            break
        if candidate.at.astimezone(MOSCOW).date() != event_day:
            break
        if candidate.at != expected_at:
            # Do not carry a hypothesis through a data gap or trading pause.
            return None
        if target <= candidate.at <= deadline and is_regular_session(candidate):
            values.append(candidate.close)
        expected_at += timedelta(minutes=1)
    return statistics.median(values) if values else None


def classify_directional(
    gross_expected_bps: float,
    baseline_sigma_bps: float,
    horizon_minutes: int,
    policy: StudyPolicy,
) -> tuple[str, float, float, float]:
    """Return verdict, expected net, reverse net and predeclared materiality."""

    scaled_sigma = baseline_sigma_bps * math.sqrt(horizon_minutes)
    materiality = max(
        policy.outcome_min_move_bps,
        policy.outcome_volatility_multiplier * scaled_sigma,
    )
    net_expected = gross_expected_bps - policy.round_trip_cost_bps
    net_reverse = -gross_expected_bps - policy.round_trip_cost_bps
    if net_expected >= materiality:
        verdict = "confirmed"
    elif net_reverse >= materiality:
        verdict = "contradicted"
    else:
        verdict = "insignificant"
    return verdict, net_expected, net_reverse, materiality


def prepare_candles(candles: Sequence[Candle]) -> tuple[dict[tuple[str, date], list[Candle]], dict]:
    key_counts = Counter((row.ticker, row.at) for row in candles)
    duplicate_count = sum(count - 1 for count in key_counts.values() if count > 1)
    # Exclude every member of a conflicting timestamp group; never select one by price.
    unique = {
        (candle.ticker, candle.at): candle
        for candle in candles
        if key_counts[(candle.ticker, candle.at)] == 1
    }
    grouped: dict[tuple[str, date], list[Candle]] = defaultdict(list)
    for candle in unique.values():
        if candle.complete and candle.open > 0 and candle.close > 0 and is_regular_session(candle):
            grouped[(candle.ticker, candle.at.astimezone(MOSCOW).date())].append(candle)
    for rows in grouped.values():
        rows.sort(key=lambda row: row.at)
    quality = {
        "fetched_candles": len(candles),
        "duplicate_ticker_timestamps": duplicate_count,
        "incomplete_candles": sum(not row.complete for row in candles),
        "invalid_price_candles": sum(row.open <= 0 or row.close <= 0 for row in candles),
        "outside_regular_session_candles": sum(not is_regular_session(row) for row in candles),
        "complete_regular_candles": sum(len(rows) for rows in grouped.values()),
        "instrument_days": len(grouped),
        "sessions": len({day for _, day in grouped}),
    }
    return grouped, quality


def fingerprint_candles(candles: Sequence[Candle]) -> str:
    """Fingerprint the in-memory snapshot without persisting its rows."""

    digest = hashlib.sha256()
    for row in sorted(candles, key=lambda item: (item.ticker, item.at, item.close)):
        digest.update(
            (
                f"{row.ticker}|{row.at.isoformat()}|{row.open:.17g}|"
                f"{row.close:.17g}|{int(row.complete)}\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def detect_events(
    grouped: dict[tuple[str, date], list[Candle]],
    policy: StudyPolicy,
) -> tuple[list[DetectedEvent], list[EligibleObservation], dict]:
    move_history: dict[str, deque[float]] = defaultdict(
        lambda: deque(maxlen=policy.detector_baseline_points)
    )
    return_history: dict[str, deque[float]] = defaultdict(
        lambda: deque(maxlen=policy.volatility_lookback_points)
    )
    last_event: dict[str, datetime] = {}
    events: list[DetectedEvent] = []
    observations: list[EligibleObservation] = []
    gaps = 0
    eligible = 0
    cooldown_blocks = 0
    for (ticker, trading_day), rows in sorted(grouped.items()):
        by_time = {row.at: row for row in rows}
        for index, candle in enumerate(rows):
            if index == 0:
                continue
            previous = rows[index - 1]
            consecutive = candle.at - previous.at == timedelta(minutes=1)
            if consecutive:
                one_minute_return = 10_000.0 * (candle.close / previous.close - 1.0)
            else:
                gaps += 1
                one_minute_return = None
            window_start = by_time.get(
                candle.at - timedelta(minutes=policy.detector_window_minutes)
            )
            history = move_history[ticker]
            volatility = return_history[ticker]
            if window_start is not None and len(history) >= policy.detector_min_baseline_points:
                signed_move = 10_000.0 * (candle.close / window_start.close - 1.0)
                absolute_move = abs(signed_move)
                baseline, z_score = _mean_and_z_score(history, absolute_move)
                relative_excursion = (
                    abs(absolute_move - baseline) / abs(baseline)
                    if abs(baseline) >= 1e-12
                    else math.inf
                )
                observation_is_eligible = (
                    len(volatility) >= policy.volatility_min_points
                    and signed_move != 0
                )
                if observation_is_eligible:
                    eligible += 1
                    observations.append(
                        EligibleObservation(
                            ticker=ticker,
                            trading_day=trading_day,
                            session_bucket=bucket_for(candle.at),
                            at=candle.at,
                            direction=1 if signed_move > 0 else -1,
                        )
                    )
                detected = (
                    observation_is_eligible
                    and absolute_move >= policy.detector_min_move_bps
                    and z_score >= policy.detector_z_score
                    and relative_excursion >= policy.detector_min_relative_excursion
                )
                if detected:
                    prior = last_event.get(ticker)
                    if prior is not None and candle.at - prior < timedelta(minutes=policy.cooldown_minutes):
                        cooldown_blocks += 1
                    else:
                        last_event[ticker] = candle.at
                        events.append(
                            DetectedEvent(
                                ticker=ticker,
                                trading_day=trading_day,
                                session_bucket=bucket_for(candle.at),
                                at=candle.at,
                                direction=1 if signed_move > 0 else -1,
                                event_move_bps=signed_move,
                                baseline_move_bps=baseline,
                                detector_z_score=z_score,
                                baseline_sigma_bps=max(
                                    policy.volatility_floor_bps,
                                    statistics.pstdev(volatility),
                                ),
                            )
                        )
                history.append(absolute_move)
            elif window_start is not None:
                history.append(
                    abs(10_000.0 * (candle.close / window_start.close - 1.0))
                )
            if one_minute_return is not None:
                volatility.append(one_minute_return)
    return events, observations, {
        "one_minute_gaps": gaps,
        "eligible_detector_minutes": eligible,
        "detected_events": len(events),
        "events_blocked_by_cooldown": cooldown_blocks,
        "detected_events_by_ticker": dict(sorted(Counter(row.ticker for row in events).items())),
    }


def calculate_outcomes(
    grouped: dict[tuple[str, date], list[Candle]],
    events: Sequence[DetectedEvent],
    horizons: Sequence[int],
    policy: StudyPolicy,
) -> tuple[list[EventOutcome], dict[int, int]]:
    indexes = {
        key: {candle.at: index for index, candle in enumerate(rows)}
        for key, rows in grouped.items()
    }
    missing = Counter()
    outcomes: list[EventOutcome] = []
    for event in events:
        key = (event.ticker, event.trading_day)
        rows = grouped[key]
        index = indexes[key][event.at]
        anchor = rows[index].close
        for horizon in horizons:
            forward = _forward_median(rows, index, horizon, policy.forward_grace_minutes)
            if forward is None:
                missing[horizon] += 1
                outcomes.append(
                    EventOutcome(
                        event=event,
                        horizon_minutes=horizon,
                        gross_expected_bps=None,
                        net_expected_bps=None,
                        net_reverse_bps=None,
                        materiality_bps=None,
                        verdict="inconclusive",
                        reason_code="forward_price_unavailable_or_session_gap",
                    )
                )
                continue
            gross = event.direction * 10_000.0 * (forward / anchor - 1.0)
            verdict, expected, reverse, materiality = classify_directional(
                gross, event.baseline_sigma_bps, horizon, policy
            )
            outcomes.append(
                EventOutcome(
                    event=event,
                    horizon_minutes=horizon,
                    gross_expected_bps=gross,
                    net_expected_bps=expected,
                    net_reverse_bps=reverse,
                    materiality_bps=materiality,
                    verdict=verdict,
                )
            )
    return outcomes, {horizon: missing[horizon] for horizon in horizons}


def chronological_split(days: Sequence[date], train_fraction: float = 0.70) -> tuple[set[date], set[date]]:
    ordered = sorted(set(days))
    if len(ordered) < 2:
        return set(ordered), set()
    cut = min(len(ordered) - 1, max(1, math.floor(len(ordered) * train_fraction)))
    return set(ordered[:cut]), set(ordered[cut:])


def build_controls(
    grouped: dict[tuple[str, date], list[Candle]],
    events: Sequence[DetectedEvent],
    eligible_observations: Sequence[EligibleObservation],
    outcomes: Sequence[EventOutcome],
    policy: StudyPolicy,
) -> dict[tuple[str, datetime, int], list[float]]:
    event_times: dict[tuple[str, date], list[datetime]] = defaultdict(list)
    for event in events:
        event_times[(event.ticker, event.trading_day)].append(event.at)
    eligible_by_day: dict[tuple[str, date], list[EligibleObservation]] = defaultdict(list)
    for observation in eligible_observations:
        eligible_by_day[(observation.ticker, observation.trading_day)].append(observation)
    result: dict[tuple[str, datetime, int], list[float]] = {}
    for outcome in outcomes:
        if outcome.verdict == "inconclusive":
            continue
        event = outcome.event
        rows = grouped[(event.ticker, event.trading_day)]
        indexes = {row.at: index for index, row in enumerate(rows)}
        candidates: list[tuple[str, float]] = []
        for observation in eligible_by_day[(event.ticker, event.trading_day)]:
            if observation.session_bucket != event.session_bucket:
                continue
            if observation.direction != event.direction:
                continue
            if any(
                abs((observation.at - event_at).total_seconds()) <= 300
                for event_at in event_times[(event.ticker, event.trading_day)]
            ):
                continue
            index = indexes[observation.at]
            candidate = rows[index]
            forward = _forward_median(
                rows, index, outcome.horizon_minutes, policy.forward_grace_minutes
            )
            if forward is None:
                continue
            gross = event.direction * 10_000.0 * (forward / candidate.close - 1.0)
            net = gross - policy.round_trip_cost_bps
            fingerprint = hashlib.sha256(
                (
                    f"{policy.seed}|{event.ticker}|{event.at.isoformat()}|"
                    f"{outcome.horizon_minutes}|{candidate.at.isoformat()}"
                ).encode("utf-8")
            ).hexdigest()
            candidates.append((fingerprint, net))
        candidates.sort(key=lambda item: item[0])
        result[(event.ticker, event.at, outcome.horizon_minutes)] = [
            value for _, value in candidates[: policy.controls_per_event]
        ]
    return result


def wilson_interval(successes: int, total: int) -> tuple[float | None, float | None]:
    if total == 0:
        return None, None
    z = 1.959963984540054
    probability = successes / total
    denominator = 1 + z * z / total
    center = (probability + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(
        (probability * (1 - probability) + z * z / (4 * total)) / total
    ) / denominator
    return center - margin, center + margin


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def day_cluster_bootstrap_interval(
    values: Sequence[tuple[date, float]],
    *,
    samples: int,
    seed: int,
) -> tuple[float | None, float | None]:
    """Bootstrap whole trading-day clusters to retain intraday dependence."""

    clusters: dict[date, list[float]] = defaultdict(list)
    for trading_day, value in values:
        clusters[trading_day].append(value)
    days = sorted(clusters)
    if not days:
        return None, None
    rng = random.Random(seed)
    means: list[float] = []
    for _ in range(samples):
        selected = [days[rng.randrange(len(days))] for _ in days]
        sample_values = [value for day in selected for value in clusters[day]]
        means.append(statistics.mean(sample_values))
    return _percentile(means, 0.025), _percentile(means, 0.975)


def summarize(
    outcomes: Sequence[EventOutcome],
    controls: dict[tuple[str, datetime, int], list[float]],
    horizons: Sequence[int],
    split: str,
    policy: StudyPolicy,
) -> list[dict]:
    summaries: list[dict] = []
    for horizon in horizons:
        rows = [row for row in outcomes if row.horizon_minutes == horizon]
        decided = [row for row in rows if row.verdict != "inconclusive"]
        counts = Counter(row.verdict for row in decided)
        paired_lifts: list[tuple[date, float]] = []
        for row in decided:
            matches = controls.get(
                (row.event.ticker, row.event.at, row.horizon_minutes), []
            )
            if (
                len(matches) == policy.controls_per_event
                and row.net_expected_bps is not None
            ):
                paired_lifts.append(
                    (
                        row.event.trading_day,
                        row.net_expected_bps - statistics.mean(matches),
                    )
                )
        seed_prefix = int(
            hashlib.sha256(f"{policy.seed}|{split}|{horizon}".encode()).hexdigest()[:12],
            16,
        )
        expected_values = [
            (row.event.trading_day, row.net_expected_bps)
            for row in decided
            if row.net_expected_bps is not None
        ]
        reverse_values = [
            (row.event.trading_day, row.net_reverse_bps)
            for row in decided
            if row.net_reverse_bps is not None
        ]
        summaries.append(
            {
                "split": split,
                "horizon_minutes": horizon,
                "n": len(decided),
                "eligible": len(rows),
                "inconclusive": len(rows) - len(decided),
                "outcome_coverage": len(decided) / len(rows) if rows else None,
                "sessions": len({row.event.trading_day for row in decided}),
                "confirmed": counts["confirmed"],
                "confirmed_rate": counts["confirmed"] / len(decided) if decided else None,
                "confirmed_wilson_95": wilson_interval(counts["confirmed"], len(decided)),
                "contradicted": counts["contradicted"],
                "contradicted_rate": counts["contradicted"] / len(decided) if decided else None,
                "contradicted_wilson_95": wilson_interval(counts["contradicted"], len(decided)),
                "insignificant": counts["insignificant"],
                "insignificant_rate": counts["insignificant"] / len(decided) if decided else None,
                "mean_net_expected_bps": statistics.mean(value for _, value in expected_values) if expected_values else None,
                "median_net_expected_bps": statistics.median(value for _, value in expected_values) if expected_values else None,
                "net_expected_day_bootstrap_95": day_cluster_bootstrap_interval(
                    expected_values, samples=policy.bootstrap_samples, seed=seed_prefix
                ),
                "mean_net_reverse_bps": statistics.mean(value for _, value in reverse_values) if reverse_values else None,
                "net_reverse_day_bootstrap_95": day_cluster_bootstrap_interval(
                    reverse_values, samples=policy.bootstrap_samples, seed=seed_prefix + 1
                ),
                "matched_control_coverage": len(paired_lifts) / len(decided) if decided else None,
                "mean_lift_vs_controls_bps": statistics.mean(value for _, value in paired_lifts) if paired_lifts else None,
                "lift_day_bootstrap_95": day_cluster_bootstrap_interval(
                    paired_lifts, samples=policy.bootstrap_samples, seed=seed_prefix + 2
                ),
            }
        )
    return summaries


def summarize_exploratory_segments(
    outcomes: Sequence[EventOutcome],
    controls: dict[tuple[str, datetime, int], list[float]],
    policy: StudyPolicy,
) -> list[dict]:
    """Describe predeclared context slices without promoting them to evidence."""

    primary = [
        row
        for row in outcomes
        if row.horizon_minutes == policy.primary_horizon_minutes
        and row.verdict != "inconclusive"
        and row.net_expected_bps is not None
        and row.net_reverse_bps is not None
    ]
    grouped: dict[tuple[str, str], list[EventOutcome]] = defaultdict(list)
    for row in primary:
        direction = "up" if row.event.direction > 0 else "down"
        labels = (
            ("ticker", row.event.ticker),
            ("direction", direction),
            ("session_bucket", str(row.event.session_bucket)),
            ("ticker_direction", f"{row.event.ticker}:{direction}"),
        )
        for dimension, value in labels:
            grouped[(dimension, value)].append(row)
    result: list[dict] = []
    for (dimension, value), rows in sorted(grouped.items()):
        expected = [
            (row.event.trading_day, float(row.net_expected_bps)) for row in rows
        ]
        reverse = [
            (row.event.trading_day, float(row.net_reverse_bps)) for row in rows
        ]
        lifts: list[tuple[date, float]] = []
        for row in rows:
            matches = controls.get(
                (row.event.ticker, row.event.at, row.horizon_minutes), []
            )
            if len(matches) == policy.controls_per_event:
                lifts.append(
                    (
                        row.event.trading_day,
                        float(row.net_expected_bps) - statistics.mean(matches),
                    )
                )
        seed = int(
            hashlib.sha256(
                f"{policy.seed}|segment|{dimension}|{value}".encode()
            ).hexdigest()[:12],
            16,
        )
        result.append(
            {
                "dimension": dimension,
                "value": value,
                "n": len(rows),
                "sessions": len({row.event.trading_day for row in rows}),
                "confirmed_rate": sum(row.verdict == "confirmed" for row in rows)
                / len(rows),
                "contradicted_rate": sum(
                    row.verdict == "contradicted" for row in rows
                )
                / len(rows),
                "mean_net_expected_bps": statistics.mean(item for _, item in expected),
                "net_expected_day_bootstrap_95": day_cluster_bootstrap_interval(
                    expected, samples=policy.bootstrap_samples, seed=seed
                ),
                "mean_net_reverse_bps": statistics.mean(item for _, item in reverse),
                "net_reverse_day_bootstrap_95": day_cluster_bootstrap_interval(
                    reverse, samples=policy.bootstrap_samples, seed=seed + 1
                ),
                "full_control_coverage": len(lifts) / len(rows),
                "mean_lift_vs_controls_bps": statistics.mean(item for _, item in lifts)
                if lifts
                else None,
                "lift_day_bootstrap_95": day_cluster_bootstrap_interval(
                    lifts, samples=policy.bootstrap_samples, seed=seed + 2
                ),
                "interpretation": "exploratory_only_multiple_comparisons_unadjusted",
            }
        )
    return result


def _number(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{100 * value:.1f}%"


def render_report(payload: dict) -> str:
    lines = [
        "# T-Invest directional hypothesis study",
        "",
        "Redacted aggregate; generated locally from official exchange one-minute candles.",
        "",
        "## Scope",
        "",
        f"- Instruments: {', '.join(payload['scope']['tickers'])} (canonical TQBR shares).",
        f"- Calendar range: {payload['scope']['from']} through {payload['scope']['to']}.",
        f"- Trading sessions: {payload['quality']['sessions']}; completed regular-session candles: {payload['quality']['complete_regular_candles']:,}.",
        f"- Detected events: {payload['quality']['detected_events']}; eligible detector minutes: {payload['quality']['eligible_detector_minutes']:,}.",
        "- No token, UID, account identifier, raw candle, price or individual event is persisted.",
        "",
        "## Predeclared method",
        "",
        f"- Detector: absolute {payload['policy']['detector_window_minutes']}-minute move, compared only with prior rolling absolute moves; z ≥ {payload['policy']['detector_z_score']}, move ≥ {payload['policy']['detector_min_move_bps']} bps and relative excursion ≥ {payload['policy']['detector_min_relative_excursion']}.",
        "- Direction is the sign of the detected move; the anchor is that completed minute close.",
        "- Endpoint is the exact target-minute close; a missing candle or any intervening gap produces inconclusive.",
        f"- Cost proxy: {payload['policy']['round_trip_cost_bps']} bps. It is versioned but is not an observed historical spread.",
        "- net_expected = gross_expected − cost; net_reverse = −gross_expected − cost.",
        "- materiality = max(minimum move, volatility multiplier × prior 1m sigma × sqrt(horizon)).",
        "- confirmed when net_expected ≥ materiality; contradicted when net_reverse ≥ materiality; otherwise insignificant.",
        f"- Inference uses exactly {payload['policy']['controls_per_event']} deterministic eligible observations with the same instrument/day/session bucket/direction, excluding any detected event ±5 minutes.",
        "- Trading days are split chronologically 70/30 before evaluation. Confidence intervals resample whole trading days.",
        f"- The predeclared primary horizon is {payload['policy']['primary_horizon_minutes']} minutes; other horizons are exploratory.",
        "",
        "## Results",
        "",
        "| Split | Horizon | decided/eligible | coverage | sessions | confirmed (Wilson 95%) | contradicted (Wilson 95%) | insignificant | mean net bps (day-bootstrap 95%) | lift vs controls bps (day-bootstrap 95%) | controls |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["results"]:
        confirmed_ci = row["confirmed_wilson_95"]
        contradicted_ci = row["contradicted_wilson_95"]
        expected_ci = row["net_expected_day_bootstrap_95"]
        lift_ci = row["lift_day_bootstrap_95"]
        lines.append(
            f"| {row['split']} | {row['horizon_minutes']}m | {row['n']}/{row['eligible']} | "
            f"{_percent(row['outcome_coverage'])} | {row['sessions']} | "
            f"{_percent(row['confirmed_rate'])} [{_percent(confirmed_ci[0])}, {_percent(confirmed_ci[1])}] | "
            f"{_percent(row['contradicted_rate'])} [{_percent(contradicted_ci[0])}, {_percent(contradicted_ci[1])}] | "
            f"{_percent(row['insignificant_rate'])} | {_number(row['mean_net_expected_bps'])} "
            f"[{_number(expected_ci[0])}, {_number(expected_ci[1])}] | "
            f"{_number(row['mean_lift_vs_controls_bps'])} [{_number(lift_ci[0])}, {_number(lift_ci[1])}] | "
            f"{_percent(row['matched_control_coverage'])} |"
        )
    lines.extend(
        [
            "",
            "## Product decision",
            "",
            payload["decision"],
            "",
            "Exploratory ticker, direction, session-bucket and ticker×direction slices are included "
            "in aggregate JSON only. They are multiple-comparison-unadjusted and cannot change a live expectation; "
            "they may only motivate a pre-registered shadow cohort.",
            "",
            "## GA interpretation",
            "",
            "This study validates automatic labels and evidence governance, not profitability. "
            "A family may be enabled in GA only after production tick/L2 outcomes use actual half-spreads, "
            "the exact detector/catalog/cost versions, at least 30 validation sessions and 300 eligible signals.",
            "",
            "One-minute OHLCV cannot reconstruct intraminute ordering, midpoint, spread, order-book depth, "
            "latency or fills. Any inverse result remains a shadow candidate and is never applied silently.",
            "",
            f"Method fingerprint: `{payload['method_fingerprint']}`",
            f"Input snapshot fingerprint: `{payload['input_fingerprint']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def build_decision(
    results: Sequence[dict], policy: StudyPolicy | None = None
) -> tuple[str, bool, bool]:
    effective_policy = policy or StudyPolicy()
    validation = [
        row
        for row in results
        if row["split"] == "validation"
        and row["horizon_minutes"] == effective_policy.primary_horizon_minutes
    ]
    inverse_supported = any(
        row["n"] >= 300
        and row["sessions"] >= 30
        and row["outcome_coverage"] is not None
        and row["outcome_coverage"] >= 0.95
        and row["matched_control_coverage"] is not None
        and row["matched_control_coverage"] >= 0.95
        and row["net_expected_day_bootstrap_95"][1] is not None
        and row["net_expected_day_bootstrap_95"][1] < 0
        and row["net_reverse_day_bootstrap_95"][0] is not None
        and row["net_reverse_day_bootstrap_95"][0] > 0
        for row in validation
    )
    continuation_supported = any(
        row["n"] >= 300
        and row["sessions"] >= 30
        and row["outcome_coverage"] is not None
        and row["outcome_coverage"] >= 0.95
        and row["matched_control_coverage"] is not None
        and row["matched_control_coverage"] >= 0.95
        and row["net_expected_day_bootstrap_95"][0] is not None
        and row["net_expected_day_bootstrap_95"][0] > 0
        and row["lift_day_bootstrap_95"][0] is not None
        and row["lift_day_bootstrap_95"][0] > 0
        for row in validation
    )
    if inverse_supported:
        decision = (
            "The predeclared primary validation horizon clears the inverse research gate. "
            "Create a versioned shadow inverse candidate; do not change the live expectation "
            "until an independent production tick/L2 cohort repeats the result."
        )
    elif continuation_supported:
        decision = (
            "The predeclared primary validation horizon clears this candle-study continuation gate. "
            "Keep the expectation versioned and require independent production tick/L2 evidence before a GA claim."
        )
    else:
        decision = (
            "The validation set does not clear a continuation or inverse gate. Keep the family experimental, "
            "persist confirmed/insignificant/contradicted verdicts, and accumulate an independent production cohort."
        )
    return decision, continuation_supported, inverse_supported


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--tickers", default="SBER,GAZP,LKOH,YDEX,T")
    parser.add_argument("--calendar-days", type=int, default=160)
    parser.add_argument("--end-day", type=date.fromisoformat)
    parser.add_argument("--horizons", default="1,5,15")
    parser.add_argument("--output-dir", type=Path, default=Path(".tmp/ga-real-data"))
    parser.add_argument("--request-interval", type=float, default=0.22)
    parser.add_argument(
        "--ca-cert",
        type=Path,
        help="Additional PEM CA certificate. TLS verification remains enabled.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.calendar_days < 2:
        raise SystemExit("--calendar-days must be at least 2")
    tickers = tuple(item.strip().upper() for item in args.tickers.split(",") if item.strip())
    horizons = tuple(sorted({int(item) for item in args.horizons.split(",")}))
    if not tickers or not horizons or any(item <= 0 for item in horizons):
        raise SystemExit("Tickers and positive horizons are required")
    policy = StudyPolicy()
    if policy.primary_horizon_minutes not in horizons:
        raise SystemExit("Configured primary horizon must be included in --horizons")
    end_day = args.end_day or (datetime.now(MOSCOW).date() - timedelta(days=1))
    start_day = end_day - timedelta(days=args.calendar_days - 1)
    scope = {
        "tickers": list(tickers),
        "from": start_day.isoformat(),
        "to": end_day.isoformat(),
        "horizons": list(horizons),
    }
    verify: bool | ssl.SSLContext = True
    if args.ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=args.ca_cert)
    try:
        token = load_env_value(args.env_file, "TINVEST_TOKEN")
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-app-name": "investment-signals-ga-study",
        }
        with httpx.Client(headers=headers, timeout=45.0, verify=verify) as client:
            instruments = resolve_instruments(client, tickers)
            candles = fetch_candles(
                client,
                instruments,
                start_day,
                end_day,
                request_interval_seconds=args.request_interval,
            )
    except (RuntimeError, httpx.HTTPError) as exc:
        reason_code, message, remediation = classify_tinvest_failure(exc)
        run_dir = write_failure_artifact(
            args.output_dir,
            scope=scope,
            reason_code=reason_code,
            message=message,
            remediation=remediation,
            ca_cert=args.ca_cert,
        )
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": reason_code,
                    "report": str(run_dir / "report.md"),
                },
                ensure_ascii=False,
            )
        )
        return 2
    input_fingerprint = fingerprint_candles(candles)
    grouped, quality = prepare_candles(candles)
    events, eligible_observations, detector_quality = detect_events(grouped, policy)
    quality.update(detector_quality)
    outcomes, missing = calculate_outcomes(grouped, events, horizons, policy)
    quality["missing_forward_by_horizon"] = {str(key): value for key, value in missing.items()}
    session_days = sorted({day for _, day in grouped})
    train_days, validation_days = chronological_split(session_days)
    controls = build_controls(
        grouped, events, eligible_observations, outcomes, policy
    )
    train = [row for row in outcomes if row.event.trading_day in train_days]
    validation = [row for row in outcomes if row.event.trading_day in validation_days]
    results = summarize(train, controls, horizons, "train", policy) + summarize(
        validation, controls, horizons, "validation", policy
    )
    decision, continuation_supported, inverse_supported = build_decision(results, policy)
    exploratory_segments = summarize_exploratory_segments(
        validation, controls, policy
    )
    method = {
        "policy": asdict(policy),
        "tickers": tickers,
        "horizons": horizons,
        "calendar_days": args.calendar_days,
        "return_formula": "simple_return_bps",
        "candle_source": "CANDLE_SOURCE_EXCHANGE",
        "session": [
            REGULAR_SESSION_START.isoformat(),
            REGULAR_SESSION_END.isoformat(),
        ],
    }
    method_fingerprint = hashlib.sha256(
        json.dumps(method, sort_keys=True).encode("utf-8")
    ).hexdigest()
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "scope": {
            "tickers": list(tickers),
            "from": start_day.isoformat(),
            "to": end_day.isoformat(),
        },
        "quality": quality,
        "split": {
            "all_sessions": len(session_days),
            "train_sessions": len(train_days),
            "validation_sessions": len(validation_days),
            "train_range": [min(train_days).isoformat(), max(train_days).isoformat()] if train_days else None,
            "validation_range": [min(validation_days).isoformat(), max(validation_days).isoformat()] if validation_days else None,
        },
        "policy": asdict(policy),
        "method_fingerprint": method_fingerprint,
        "input_fingerprint": input_fingerprint,
        "runtime": {
            "python": sys.version.split()[0],
            "httpx": httpx.__version__,
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "additional_ca_sha256": hashlib.sha256(args.ca_cert.read_bytes()).hexdigest()
            if args.ca_cert is not None
            else None,
        },
        "results": results,
        "exploratory_segments": exploratory_segments,
        "decision": decision,
        "continuation_supported": continuation_supported,
        "inverse_supported": inverse_supported,
    }
    run_id = f"{method_fingerprint[:12]}-{input_fingerprint[:12]}"
    run_dir = args.output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "aggregate-results.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (run_dir / "report.md").write_text(render_report(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "ok",
                "sessions": len(session_days),
                "train_sessions": len(train_days),
                "validation_sessions": len(validation_days),
                "events": len(events),
                "outcomes": len(outcomes),
                "continuation_supported": continuation_supported,
                "inverse_supported": inverse_supported,
                "run_id": run_id,
                "report": str(run_dir / "report.md"),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

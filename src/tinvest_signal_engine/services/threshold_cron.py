"""Периодический пересчёт порогов по истории свечей и запись YAML overrides."""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from statistics import fmean

import yaml
from tinkoff.invest import CandleInterval, Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX

from ..config import RuntimeSettings, load_instrument_configs
from ..instruments import build_instrument_registry
from ..logging_utils import configure_logging
from ..serialization import quotation_to_float, utc_now

logger = logging.getLogger(__name__)

_OVERRIDES_FILE_HEADER = """# ============================================================================
# Автоматические переопределения порогов детектора (этот файл перезаписывает
# сервис threshold-cron при каждом успешном расчёте).
#
# Строки ниже — результат последнего прогона:
#   generated_at — время UTC расчёта
#   lookback_days — сколько дней истории брали (см. THRESHOLD_LOOKBACK_DAYS)
#   metric — что считали (среднее почасовое |open-close| в bps)
#   multiplier — множитель к среднему (см. THRESHOLD_HOURLY_DEVIATION_MULTIPLIER)
#   per_instrument — только переопределённые ключи (обычно price_move_absolute_threshold_bps)
#
# Ручные правки в per_instrument возможны; при следующем запуске cron они могут
# быть перезаписаны для инструментов, по которым удалось посчитать порог.
# ============================================================================

"""


def _compute_hourly_deviation_bps(candles) -> list[float]:
    deviations: list[float] = []
    for candle in candles:
        open_price = quotation_to_float(candle.open)
        close_price = quotation_to_float(candle.close)
        if open_price is None or close_price is None or open_price <= 0:
            continue
        deviations.append(
            abs((close_price - open_price) / open_price) * 10_000
        )
    return deviations


def _recalculate(settings: RuntimeSettings) -> None:
    instrument_configs = load_instrument_configs(settings.instruments_path)
    target = INVEST_GRPC_API_SANDBOX if settings.tinvest_use_sandbox else None
    now = utc_now()
    from_ts = now - timedelta(days=settings.threshold_lookback_days)
    per_instrument: dict[str, dict[str, float]] = {}

    with Client(
        settings.tinvest_token,
        target=target,
        app_name=settings.tinvest_app_name,
    ) as client:
        registry = build_instrument_registry(client, instrument_configs)
        for metadata in registry:
            response = client.market_data.get_candles(
                figi=metadata.figi,
                from_=from_ts,
                to=now,
                interval=CandleInterval.CANDLE_INTERVAL_HOUR,
            )
            deviations = _compute_hourly_deviation_bps(response.candles)
            if len(deviations) < 24:
                logger.warning(
                    "Skip %s: not enough hourly candles (%s)",
                    metadata.instrument_id,
                    len(deviations),
                )
                continue
            mean_deviation = fmean(deviations)
            threshold = (
                mean_deviation
                * settings.threshold_hourly_deviation_multiplier
            )
            per_instrument[metadata.instrument_id] = {
                "price_move_absolute_threshold_bps": round(threshold, 4)
            }

    payload = {
        "generated_at": now.isoformat(),
        "lookback_days": settings.threshold_lookback_days,
        "metric": "hourly_abs_return_bps_mean",
        "multiplier": settings.threshold_hourly_deviation_multiplier,
        "per_instrument": per_instrument,
    }
    settings.detector_overrides_path.parent.mkdir(
        parents=True, exist_ok=True
    )
    with settings.detector_overrides_path.open(
        "w", encoding="utf-8"
    ) as handle:
        handle.write(_OVERRIDES_FILE_HEADER)
        yaml.safe_dump(
            payload,
            handle,
            allow_unicode=False,
            sort_keys=False,
        )
    logger.info(
        "Wrote %s instrument thresholds to %s",
        len(per_instrument),
        settings.detector_overrides_path,
    )


def main() -> None:
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    if not settings.tinvest_token:
        raise RuntimeError("TINVEST_TOKEN is required")
    interval_hours = max(settings.threshold_recalc_interval_hours, 1)
    sleep_seconds = interval_hours * 3600
    while True:
        try:
            _recalculate(settings)
        except Exception:
            logger.exception("Threshold recalculation failed")
        logger.info("Next threshold recalculation in %s hours", interval_hours)
        time.sleep(sleep_seconds)

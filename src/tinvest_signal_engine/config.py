"""Загрузка YAML и настроек окружения.

Содержит типы для списка инструментов, порогов детектора и единый
:class:`RuntimeSettings`, собираемый из переменных среды для всех сервисов.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _read_yaml(path: Path) -> dict[str, Any]:
    import yaml

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def _env_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_optional_int(name: str) -> int | None:
    raw_value = os.getenv(name)
    if raw_value is None:
        return None
    value = raw_value.strip()
    return int(value) if value else None


@dataclass(frozen=True)
class InstrumentSubscriptionConfig:
    ticker: str
    class_code: str
    alias: str
    trades: bool = True
    last_price: bool = True
    info: bool = True
    order_book_depth: int | None = 10
    candles: bool = False
    candle_interval: str = "1m"

    @property
    def instrument_id(self) -> str:
        return f"{self.ticker}_{self.class_code}"


@dataclass(frozen=True)
class LoadedDetectorConfig:
    """Базовые настройки детектора и переопределения по instrument_id."""

    default: "DetectorSettings"
    per_instrument: dict[str, "DetectorSettings"]


@dataclass(frozen=True)
class DetectorSettings:
    sample_every_seconds: int = 5
    min_baseline_points: int = 12
    baseline_points: int = 120
    trade_window_seconds: int = 60
    price_window_seconds: int = 90
    orderbook_window_seconds: int = 120
    alert_cooldown_seconds: int = 120
    volume_zscore_threshold: float = 4.0
    trade_count_zscore_threshold: float = 4.0
    price_return_zscore_threshold: float = 3.5
    spread_zscore_threshold: float = 3.0
    imbalance_zscore_threshold: float = 3.0
    imbalance_absolute_threshold: float = 0.65
    price_move_absolute_threshold_bps: float = 0.0
    combo_enabled: bool = False
    combo_freshness_seconds: int = 15
    combo_min_score: int = 6
    combo_alert_cooldown_seconds: int = 180
    combo_spread_points: int = 1
    combo_imbalance_points: int = 1
    combo_tick_rate_points: int = 2
    combo_delta_points: int = 2
    combo_imbalance_long_threshold: float = 0.80
    combo_imbalance_short_threshold: float = 0.20
    combo_delta_min_abs_qty: float = 1.0


@dataclass(frozen=True)
class RuntimeSettings:
    tinvest_token: str
    tinvest_use_sandbox: bool
    tinvest_app_name: str
    kafka_bootstrap_servers: str
    kafka_host_bootstrap_servers: str
    kafka_raw_topic: str
    kafka_signal_topic: str
    kafka_consumer_group: str
    kafka_auto_offset_reset: str
    local_notifier_consumer_group: str
    local_notification_duration_seconds: int
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_username: str
    postgres_password: str
    postgres_table: str
    postgres_startup_timeout_seconds: int
    postgres_startup_check_interval_seconds: int
    api_host: str
    api_port: int
    alert_webhook_url: str | None
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    telegram_message_thread_id: int | None
    log_level: str
    instruments_path: Path
    detector_path: Path
    detector_overrides_path: Path
    config_reload_interval_seconds: int
    threshold_recalc_interval_hours: int
    threshold_lookback_days: int
    threshold_hourly_deviation_multiplier: float

    @classmethod
    def from_env(cls) -> "RuntimeSettings":
        instruments_path = Path(
            os.getenv("INSTRUMENTS_CONFIG", "conf/instruments.yaml")
        )
        detector_path = Path(
            os.getenv("DETECTORS_CONFIG", "conf/detectors.yaml")
        )
        detector_overrides_path = Path(
            os.getenv(
                "DETECTORS_OVERRIDES_CONFIG", "conf/detectors.overrides.yaml"
            )
        )
        return cls(
            tinvest_token=os.getenv("TINVEST_TOKEN", ""),
            tinvest_use_sandbox=_env_bool(
                "TINVEST_USE_SANDBOX", default=False
            ),
            tinvest_app_name=os.getenv(
                "TINVEST_APP_NAME", "tinvest-signal-engine"
            ),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"
            ),
            kafka_host_bootstrap_servers=os.getenv(
                "KAFKA_HOST_BOOTSTRAP_SERVERS", "localhost:19092"
            ),
            kafka_raw_topic=os.getenv("KAFKA_RAW_TOPIC", "marketdata.raw"),
            kafka_signal_topic=os.getenv(
                "KAFKA_SIGNAL_TOPIC", "marketdata.signals"
            ),
            kafka_consumer_group=os.getenv(
                "KAFKA_CONSUMER_GROUP", "signal-detector"
            ),
            kafka_auto_offset_reset=os.getenv(
                "KAFKA_AUTO_OFFSET_RESET", "latest"
            ),
            local_notifier_consumer_group=os.getenv(
                "LOCAL_NOTIFIER_CONSUMER_GROUP", "local-notifier"
            ),
            local_notification_duration_seconds=int(
                os.getenv("LOCAL_NOTIFICATION_DURATION_SECONDS", "5")
            ),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_database=os.getenv("POSTGRES_DATABASE", "signal_engine"),
            postgres_username=os.getenv("POSTGRES_USERNAME", "signal_engine"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "signal_engine"),
            postgres_table=os.getenv("POSTGRES_TABLE", "market_signals"),
            postgres_startup_timeout_seconds=int(
                os.getenv("POSTGRES_STARTUP_TIMEOUT_SECONDS", "90")
            ),
            postgres_startup_check_interval_seconds=int(
                os.getenv("POSTGRES_STARTUP_CHECK_INTERVAL_SECONDS", "2")
            ),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", "8000")),
            alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL") or None,
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN") or None,
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or None,
            telegram_message_thread_id=_env_optional_int(
                "TELEGRAM_MESSAGE_THREAD_ID"
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            instruments_path=instruments_path,
            detector_path=detector_path,
            detector_overrides_path=detector_overrides_path,
            config_reload_interval_seconds=int(
                os.getenv("CONFIG_RELOAD_INTERVAL_SECONDS", "10")
            ),
            threshold_recalc_interval_hours=int(
                os.getenv("THRESHOLD_RECALC_INTERVAL_HOURS", "24")
            ),
            threshold_lookback_days=int(
                os.getenv("THRESHOLD_LOOKBACK_DAYS", "7")
            ),
            threshold_hourly_deviation_multiplier=float(
                os.getenv(
                    "THRESHOLD_HOURLY_DEVIATION_MULTIPLIER", "1.0"
                )
            ),
        )


def _detector_settings_from_mapping(
    detector: dict[str, Any],
) -> DetectorSettings:
    return DetectorSettings(
        sample_every_seconds=int(detector.get("sample_every_seconds", 5)),
        min_baseline_points=int(detector.get("min_baseline_points", 12)),
        baseline_points=int(detector.get("baseline_points", 120)),
        trade_window_seconds=int(detector.get("trade_window_seconds", 60)),
        price_window_seconds=int(detector.get("price_window_seconds", 90)),
        orderbook_window_seconds=int(
            detector.get("orderbook_window_seconds", 120)
        ),
        alert_cooldown_seconds=int(
            detector.get("alert_cooldown_seconds", 120)
        ),
        volume_zscore_threshold=float(
            detector.get("volume_zscore_threshold", 4.0)
        ),
        trade_count_zscore_threshold=float(
            detector.get("trade_count_zscore_threshold", 4.0)
        ),
        price_return_zscore_threshold=float(
            detector.get("price_return_zscore_threshold", 3.5)
        ),
        spread_zscore_threshold=float(
            detector.get("spread_zscore_threshold", 3.0)
        ),
        imbalance_zscore_threshold=float(
            detector.get("imbalance_zscore_threshold", 3.0)
        ),
        imbalance_absolute_threshold=float(
            detector.get("imbalance_absolute_threshold", 0.65)
        ),
        price_move_absolute_threshold_bps=float(
            detector.get("price_move_absolute_threshold_bps", 0.0)
        ),
        combo_enabled=bool(detector.get("combo_enabled", False)),
        combo_freshness_seconds=int(
            detector.get("combo_freshness_seconds", 15)
        ),
        combo_min_score=int(detector.get("combo_min_score", 6)),
        combo_alert_cooldown_seconds=int(
            detector.get("combo_alert_cooldown_seconds", 180)
        ),
        combo_spread_points=int(detector.get("combo_spread_points", 1)),
        combo_imbalance_points=int(
            detector.get("combo_imbalance_points", 1)
        ),
        combo_tick_rate_points=int(
            detector.get("combo_tick_rate_points", 2)
        ),
        combo_delta_points=int(detector.get("combo_delta_points", 2)),
        combo_imbalance_long_threshold=float(
            detector.get("combo_imbalance_long_threshold", 0.80)
        ),
        combo_imbalance_short_threshold=float(
            detector.get("combo_imbalance_short_threshold", 0.20)
        ),
        combo_delta_min_abs_qty=float(
            detector.get("combo_delta_min_abs_qty", 1.0)
        ),
    )


def load_instrument_configs(path: Path) -> list[InstrumentSubscriptionConfig]:
    raw = _read_yaml(path)
    instruments = raw.get("instruments", [])
    if not isinstance(instruments, list):
        raise ValueError(f"'instruments' in {path} must be a list")

    result: list[InstrumentSubscriptionConfig] = []
    for item in instruments:
        if not isinstance(item, dict):
            raise ValueError(
                f"Instrument config items in {path} must be mappings"
            )
        subscriptions = item.get("subscriptions", {}) or {}
        if not isinstance(subscriptions, dict):
            raise ValueError(
                f"'subscriptions' for {item!r} in {path} must be a mapping"
            )
        ticker = str(item["ticker"]).strip().upper()
        class_code = str(item["class_code"]).strip().upper()
        result.append(
            InstrumentSubscriptionConfig(
                ticker=ticker,
                class_code=class_code,
                alias=str(item.get("alias", ticker)).strip().lower(),
                trades=bool(subscriptions.get("trades", True)),
                last_price=bool(subscriptions.get("last_price", True)),
                info=bool(subscriptions.get("info", True)),
                order_book_depth=(
                    int(subscriptions["order_book_depth"])
                    if subscriptions.get("order_book_depth") is not None
                    else None
                ),
                candles=bool(subscriptions.get("candles", False)),
                candle_interval=str(
                    subscriptions.get("candle_interval", "1m")
                ),
            )
        )
    return result


def load_detector_config(
    path: Path, overrides_path: Path | None = None
) -> LoadedDetectorConfig:
    raw = _read_yaml(path)
    detector_block = raw.get("detector", {}) or {}
    if not isinstance(detector_block, dict):
        raise ValueError(f"'detector' in {path} must be a mapping")
    base_settings = _detector_settings_from_mapping(detector_block)

    per_raw = raw.get("per_instrument") or {}
    if overrides_path is not None and overrides_path.exists():
        override_raw = _read_yaml(overrides_path)
        override_per_raw = override_raw.get("per_instrument") or {}
        if override_per_raw and not isinstance(override_per_raw, dict):
            raise ValueError(
                f"'per_instrument' in {overrides_path} must be a mapping"
            )
        per_raw = {**per_raw, **override_per_raw}
    if per_raw and not isinstance(per_raw, dict):
        raise ValueError(f"'per_instrument' in {path} must be a mapping")
    per_instrument: dict[str, DetectorSettings] = {}
    for raw_key, overrides in per_raw.items():
        key = str(raw_key).strip()
        if not key:
            continue
        if not isinstance(overrides, dict):
            raise ValueError(
                f"per_instrument['{key}'] in {path} must be a mapping"
            )
        merged = {**detector_block, **overrides}
        per_instrument[key] = _detector_settings_from_mapping(merged)

    return LoadedDetectorConfig(
        default=base_settings,
        per_instrument=per_instrument,
    )


def load_detector_settings(path: Path) -> DetectorSettings:
    """Backward-compatible: returns global detector defaults only."""
    return load_detector_config(path).default

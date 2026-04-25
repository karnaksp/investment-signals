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
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_username: str
    clickhouse_password: str
    clickhouse_table: str
    clickhouse_startup_timeout_seconds: int
    clickhouse_startup_check_interval_seconds: int
    api_host: str
    api_port: int
    alert_webhook_url: str | None
    log_level: str
    instruments_path: Path
    detector_path: Path

    @classmethod
    def from_env(cls) -> "RuntimeSettings":
        instruments_path = Path(os.getenv("INSTRUMENTS_CONFIG", "conf/instruments.yaml"))
        detector_path = Path(os.getenv("DETECTORS_CONFIG", "conf/detectors.yaml"))
        return cls(
            tinvest_token=os.getenv("TINVEST_TOKEN", ""),
            tinvest_use_sandbox=_env_bool("TINVEST_USE_SANDBOX", default=False),
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
            kafka_signal_topic=os.getenv("KAFKA_SIGNAL_TOPIC", "marketdata.signals"),
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
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "default"),
            clickhouse_username=os.getenv("CLICKHOUSE_USERNAME", "default"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            clickhouse_table=os.getenv("CLICKHOUSE_TABLE", "market_signals"),
            clickhouse_startup_timeout_seconds=int(
                os.getenv("CLICKHOUSE_STARTUP_TIMEOUT_SECONDS", "90")
            ),
            clickhouse_startup_check_interval_seconds=int(
                os.getenv("CLICKHOUSE_STARTUP_CHECK_INTERVAL_SECONDS", "2")
            ),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", "8000")),
            alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL") or None,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            instruments_path=instruments_path,
            detector_path=detector_path,
        )


def load_instrument_configs(path: Path) -> list[InstrumentSubscriptionConfig]:
    raw = _read_yaml(path)
    instruments = raw.get("instruments", [])
    if not isinstance(instruments, list):
        raise ValueError(f"'instruments' in {path} must be a list")

    result: list[InstrumentSubscriptionConfig] = []
    for item in instruments:
        if not isinstance(item, dict):
            raise ValueError(f"Instrument config items in {path} must be mappings")
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
                candle_interval=str(subscriptions.get("candle_interval", "1m")),
            )
        )
    return result


def load_detector_settings(path: Path) -> DetectorSettings:
    raw = _read_yaml(path)
    detector = raw.get("detector", {}) or {}
    if not isinstance(detector, dict):
        raise ValueError(f"'detector' in {path} must be a mapping")
    return DetectorSettings(
        sample_every_seconds=int(detector.get("sample_every_seconds", 5)),
        min_baseline_points=int(detector.get("min_baseline_points", 12)),
        baseline_points=int(detector.get("baseline_points", 120)),
        trade_window_seconds=int(detector.get("trade_window_seconds", 60)),
        price_window_seconds=int(detector.get("price_window_seconds", 90)),
        orderbook_window_seconds=int(detector.get("orderbook_window_seconds", 120)),
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
    )

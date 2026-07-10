"""Загрузка YAML и настроек окружения.

Содержит типы для списка инструментов, порогов детектора и единый
:class:`RuntimeSettings`, собираемый из переменных среды для всех сервисов.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


_SERVICE_SECRET_NAMES: dict[str, frozenset[str]] = {
    "api": frozenset(
        {
            "ADMIN_API_TOKEN",
            "CLICKHOUSE_PASSWORD",
            "POSTGRES_PASSWORD",
            "TELEGRAM_BOT_TOKEN",
            "TINVEST_TOKEN",
        }
    ),
    "dagster": frozenset({"TINVEST_TOKEN"}),
    "detector": frozenset(
        {
            "ALERT_WEBHOOK_URL",
            "POSTGRES_PASSWORD",
            "TELEGRAM_BOT_TOKEN",
        }
    ),
    "ingestor": frozenset({"TINVEST_TOKEN"}),
    "local_notifier": frozenset(),
    "market_unary_emitter": frozenset({"TINVEST_TOKEN"}),
    "migration": frozenset({"CLICKHOUSE_PASSWORD", "POSTGRES_PASSWORD"}),
    "threshold_cron": frozenset({"TINVEST_TOKEN"}),
}


def load_secret(
    name: str,
    *,
    default: str | None = None,
    service_name: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Load one secret from ``NAME`` or Docker-style ``NAME_FILE``.

    When ``service_name`` is supplied, secrets outside that service's explicit
    allowlist are ignored. This keeps one service from accidentally consuming
    credentials intended for another service.
    """
    env = os.environ if environ is None else environ
    if service_name is not None:
        allowed = _SERVICE_SECRET_NAMES.get(service_name)
        if allowed is None:
            raise ValueError(f"Unknown service name: {service_name!r}")
        if name not in allowed:
            return default

    direct = env.get(name)
    file_value = env.get(f"{name}_FILE")
    if direct and file_value:
        raise ValueError(f"Set only one of {name} and {name}_FILE")
    if file_value:
        path = Path(file_value).expanduser()
        value = path.read_text(encoding="utf-8").rstrip("\r\n")
        if "\x00" in value:
            raise ValueError(f"Secret file for {name} contains a NUL byte")
        return value
    if direct is not None:
        return direct
    return default


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


def _parse_admin_ip_allowlist(raw: str) -> frozenset[str] | None:
    parts = tuple(p.strip() for p in (raw or "").split(",") if p.strip())
    return frozenset(parts) if parts else None


def _runtime_proto_dir() -> Path:
    env = (os.getenv("PROTO_DIR") or "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (Path(__file__).resolve().parent.parent.parent / "proto").resolve()


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
    lead_lag_pairs: tuple[tuple[str, str], ...] = ()


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
    spoofing_enabled: bool = False
    spoofing_wall_ratio: float = 2.5
    spoofing_qty_drop_ratio: float = 0.55
    spoofing_max_mid_move_bps: float = 3.0
    spoofing_max_gap_seconds: float = 2.0
    spoofing_min_wall_qty: float = 500.0
    spoofing_lookback_seconds: float = 5.0
    order_book_depth_levels: int = 5
    obi_dynamics_enabled: bool = False
    obi_delta_absolute_threshold: float = 0.12
    obi_delta_zscore_threshold: float = 2.5
    trade_burst_enabled: bool = False
    trade_burst_window_ms: int = 100
    trade_burst_min_trades: int = 50
    trade_burst_min_abs_qty: float = 5.0
    lead_lag_enabled: bool = False
    lead_lag_window_seconds: int = 30
    lead_lag_leader_move_bps: float = 8.0
    lead_lag_follower_max_bps: float = 2.0
    # Дополнительно к z-score: |metric−baseline|/|baseline|, если |baseline|≥1e-9.
    # 0 = выключено. Снижает ложные срабатывания при «крошечном» std вокруг плоской базы.
    min_relative_metric_excursion: float = 0.0
    # Расстояние mid до ближайшего limit_up/limit_down (bps); 0 = не сигналить.
    limit_band_warning_bps: float = 0.0
    # Сигнал при is_consistent=false в снимке стакана из стрима.
    signal_orderbook_inconsistent: bool = False
    # z-score по истории открытого интереса (событие open_interest); 0 = выключено.
    open_interest_zscore_threshold: float = 0.0
    # z-score по диапазону свечи (high−low)/open в bps; 0 = выключено.
    candle_range_zscore_threshold: float = 0.0
    # Сигналить смену limit_order_available / market_order_available в trading_status.
    track_market_access_flags: bool = True
    # Добавлять в payload сигнала последний unary-снимок (market_values / tech_analysis), если есть.
    attach_unary_context_to_signals: bool = True


@dataclass(frozen=True)
class RuntimeSettings:
    tinvest_token: str
    tinvest_use_sandbox: bool
    tinvest_app_name: str
    kafka_bootstrap_servers: str
    kafka_host_bootstrap_servers: str
    kafka_raw_topic: str
    # Отдельный топик для unary-эмиттера (иначе None → тот же kafka_raw_topic).
    kafka_raw_unary_topic: str | None
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
    api_reload: bool
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
    metrics_listen_port: int | None
    kafka_linger_ms: int
    kafka_batch_bytes: int
    kafka_compression_codec: str
    redis_url: str | None
    redis_alert_flush_interval_seconds: int
    proto_dir: Path
    kafka_raw_value_format: str
    kafka_signal_value_format: str
    schema_registry_url: str | None
    kafka_protobuf_schema_id_raw: int | None
    kafka_protobuf_schema_id_signal: int | None
    admin_api_token: str | None
    signal_accuracy_json_path: Path
    clickhouse_http_url: str | None
    clickhouse_http_username: str | None
    clickhouse_http_password: str | None
    # Не публиковать сигналы с quality_score ниже порога (после enrich). None = выключено.
    signal_min_quality_score: int | None
    # Delivery policy: storage always keeps signals, these fields gate only outbound alerts.
    signal_delivery_enabled: bool
    signal_delivery_min_quality: int
    signal_delivery_min_quality_raw: str | None
    signal_delivery_max_per_hour: int
    signal_delivery_instrument_cooldown_seconds: int
    signal_delivery_type_rules_json: str
    # Периодический unary-эмиттер → Kafka raw (см. tinvest-market-unary-emitter). 0 = выключено.
    market_unary_poll_seconds: int
    # Один цикл опроса и выход (Dagster / ручной прогон); иначе бесконечный цикл как сервис.
    market_unary_single_shot: bool
    # Режимы через запятую: market_values, tech_analysis (регистр не важен).
    market_unary_modes_csv: str
    market_unary_market_value_types_csv: str
    market_unary_tech_indicator: str
    market_unary_tech_interval: str
    market_unary_tech_type_of_price: str
    market_unary_tech_length: int
    market_unary_tech_window_minutes: int
    market_unary_tech_sleep_ms: int
    market_unary_max_backoff_seconds: int
    market_unary_metrics_listen_port: int | None
    admin_api_rate_limit_per_minute: int
    admin_api_allowed_ips: frozenset[str] | None
    expectation_catalog_version: str | None
    detector_config_version: str | None
    delivery_config_version: str | None
    cost_model_version: str | None

    @classmethod
    def from_env(
        cls, *, service_name: str | None = None
    ) -> "RuntimeSettings":
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
            tinvest_token=load_secret(
                "TINVEST_TOKEN", default="", service_name=service_name
            )
            or "",
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
            kafka_raw_unary_topic=(
                (os.getenv("KAFKA_RAW_UNARY_TOPIC") or "").strip() or None
            ),
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
            postgres_password=load_secret(
                "POSTGRES_PASSWORD",
                default="signal_engine",
                service_name=service_name,
            )
            or "",
            postgres_table=os.getenv("POSTGRES_TABLE", "market_signals"),
            postgres_startup_timeout_seconds=int(
                os.getenv("POSTGRES_STARTUP_TIMEOUT_SECONDS", "90")
            ),
            postgres_startup_check_interval_seconds=int(
                os.getenv("POSTGRES_STARTUP_CHECK_INTERVAL_SECONDS", "2")
            ),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", "8000")),
            api_reload=_env_bool("TINVEST_API_RELOAD", default=False),
            alert_webhook_url=load_secret(
                "ALERT_WEBHOOK_URL", service_name=service_name
            )
            or None,
            telegram_bot_token=load_secret(
                "TELEGRAM_BOT_TOKEN", service_name=service_name
            )
            or None,
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
            metrics_listen_port=_env_optional_int("METRICS_LISTEN_PORT"),
            kafka_linger_ms=int(os.getenv("KAFKA_LINGER_MS", "5")),
            kafka_batch_bytes=int(os.getenv("KAFKA_BATCH_BYTES", "32768")),
            kafka_compression_codec=(
                os.getenv("KAFKA_COMPRESSION_CODEC", "lz4").strip().lower()
                or "lz4"
            ),
            redis_url=(os.getenv("REDIS_URL") or "").strip() or None,
            redis_alert_flush_interval_seconds=int(
                os.getenv("REDIS_ALERT_FLUSH_INTERVAL_SECONDS", "30")
            ),
            proto_dir=_runtime_proto_dir(),
            kafka_raw_value_format=(
                os.getenv("KAFKA_RAW_VALUE_FORMAT", "json").strip().lower()
                or "json"
            ),
            kafka_signal_value_format=(
                os.getenv("KAFKA_SIGNAL_VALUE_FORMAT", "json").strip().lower()
                or "json"
            ),
            schema_registry_url=(
                (os.getenv("SCHEMA_REGISTRY_URL") or "").strip() or None
            ),
            kafka_protobuf_schema_id_raw=_env_optional_int(
                "KAFKA_PROTOBUF_SCHEMA_ID_RAW"
            ),
            kafka_protobuf_schema_id_signal=_env_optional_int(
                "KAFKA_PROTOBUF_SCHEMA_ID_SIGNAL"
            ),
            admin_api_token=(
                load_secret("ADMIN_API_TOKEN", service_name=service_name) or ""
            ).strip()
            or None,
            signal_accuracy_json_path=Path(
                os.getenv(
                    "SIGNAL_ACCURACY_JSON_PATH",
                    "var/accuracy/signal_accuracy.json",
                )
            ).expanduser(),
            clickhouse_http_url=(
                (os.getenv("CLICKHOUSE_HTTP_URL") or "").strip() or None
            ),
            clickhouse_http_username=(
                (os.getenv("CLICKHOUSE_USERNAME") or "").strip() or None
            ),
            clickhouse_http_password=(
                load_secret("CLICKHOUSE_PASSWORD", service_name=service_name)
                or ""
            ).strip()
            or None,
            signal_min_quality_score=_env_optional_int(
                "SIGNAL_MIN_QUALITY_SCORE"
            ),
            signal_delivery_enabled=_env_bool(
                "SIGNAL_DELIVERY_ENABLED", default=True
            ),
            signal_delivery_min_quality=int(
                os.getenv("SIGNAL_DELIVERY_MIN_QUALITY", "80")
            ),
            signal_delivery_min_quality_raw=os.getenv(
                "SIGNAL_DELIVERY_MIN_QUALITY"
            ),
            signal_delivery_max_per_hour=int(
                os.getenv("SIGNAL_DELIVERY_MAX_PER_HOUR", "6")
            ),
            signal_delivery_instrument_cooldown_seconds=int(
                os.getenv("SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS", "900")
            ),
            signal_delivery_type_rules_json=(
                os.getenv("SIGNAL_DELIVERY_TYPE_RULES_JSON", "").strip()
            ),
            market_unary_poll_seconds=int(
                os.getenv("MARKET_UNARY_POLL_SECONDS", "0")
            ),
            market_unary_single_shot=_env_bool(
                "MARKET_UNARY_SINGLE_SHOT", default=False
            ),
            market_unary_modes_csv=(
                os.getenv("MARKET_UNARY_MODES", "market_values").strip()
                or "market_values"
            ),
            market_unary_market_value_types_csv=(
                os.getenv(
                    "MARKET_UNARY_MARKET_VALUE_TYPES",
                    "last_price,open_interest,close_price",
                ).strip()
                or "last_price,open_interest,close_price"
            ),
            market_unary_tech_indicator=(
                os.getenv("MARKET_UNARY_TECH_INDICATOR", "rsi").strip() or "rsi"
            ),
            market_unary_tech_interval=(
                os.getenv("MARKET_UNARY_TECH_INTERVAL", "1h").strip() or "1h"
            ),
            market_unary_tech_type_of_price=(
                os.getenv("MARKET_UNARY_TECH_TYPE_OF_PRICE", "close").strip()
                or "close"
            ),
            market_unary_tech_length=int(
                os.getenv("MARKET_UNARY_TECH_LENGTH", "14")
            ),
            market_unary_tech_window_minutes=int(
                os.getenv("MARKET_UNARY_TECH_WINDOW_MINUTES", "1440")
            ),
            market_unary_tech_sleep_ms=int(
                os.getenv("MARKET_UNARY_TECH_SLEEP_MS", "250")
            ),
            market_unary_max_backoff_seconds=int(
                os.getenv("MARKET_UNARY_MAX_BACKOFF_SECONDS", "900")
            ),
            market_unary_metrics_listen_port=_env_optional_int(
                "MARKET_UNARY_METRICS_LISTEN_PORT"
            ),
            admin_api_rate_limit_per_minute=int(
                os.getenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "120")
            ),
            admin_api_allowed_ips=_parse_admin_ip_allowlist(
                os.getenv("ADMIN_API_ALLOWED_IPS", "")
            ),
            expectation_catalog_version=(
                (os.getenv("EXPECTATION_CATALOG_VERSION") or "").strip()
                or None
            ),
            detector_config_version=(
                (os.getenv("DETECTOR_CONFIG_VERSION") or "").strip() or None
            ),
            delivery_config_version=(
                (os.getenv("DELIVERY_CONFIG_VERSION") or "").strip() or None
            ),
            cost_model_version=(
                (os.getenv("COST_MODEL_VERSION") or "").strip() or None
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
        spoofing_enabled=bool(detector.get("spoofing_enabled", False)),
        spoofing_wall_ratio=float(detector.get("spoofing_wall_ratio", 2.5)),
        spoofing_qty_drop_ratio=float(
            detector.get("spoofing_qty_drop_ratio", 0.55)
        ),
        spoofing_max_mid_move_bps=float(
            detector.get("spoofing_max_mid_move_bps", 3.0)
        ),
        spoofing_max_gap_seconds=float(
            detector.get("spoofing_max_gap_seconds", 2.0)
        ),
        spoofing_min_wall_qty=float(
            detector.get("spoofing_min_wall_qty", 500.0)
        ),
        spoofing_lookback_seconds=float(
            detector.get("spoofing_lookback_seconds", 5.0)
        ),
        order_book_depth_levels=int(detector.get("order_book_depth_levels", 5)),
        obi_dynamics_enabled=bool(detector.get("obi_dynamics_enabled", False)),
        obi_delta_absolute_threshold=float(
            detector.get("obi_delta_absolute_threshold", 0.12)
        ),
        obi_delta_zscore_threshold=float(
            detector.get("obi_delta_zscore_threshold", 2.5)
        ),
        trade_burst_enabled=bool(detector.get("trade_burst_enabled", False)),
        trade_burst_window_ms=int(detector.get("trade_burst_window_ms", 100)),
        trade_burst_min_trades=int(detector.get("trade_burst_min_trades", 50)),
        trade_burst_min_abs_qty=float(
            detector.get("trade_burst_min_abs_qty", 5.0)
        ),
        lead_lag_enabled=bool(detector.get("lead_lag_enabled", False)),
        lead_lag_window_seconds=int(
            detector.get("lead_lag_window_seconds", 30)
        ),
        lead_lag_leader_move_bps=float(
            detector.get("lead_lag_leader_move_bps", 8.0)
        ),
        lead_lag_follower_max_bps=float(
            detector.get("lead_lag_follower_max_bps", 2.0)
        ),
        min_relative_metric_excursion=float(
            detector.get("min_relative_metric_excursion", 0.0)
        ),
        limit_band_warning_bps=float(
            detector.get("limit_band_warning_bps", 0.0)
        ),
        signal_orderbook_inconsistent=bool(
            detector.get("signal_orderbook_inconsistent", False)
        ),
        open_interest_zscore_threshold=float(
            detector.get("open_interest_zscore_threshold", 0.0)
        ),
        candle_range_zscore_threshold=float(
            detector.get("candle_range_zscore_threshold", 0.0)
        ),
        track_market_access_flags=bool(
            detector.get("track_market_access_flags", True)
        ),
        attach_unary_context_to_signals=bool(
            detector.get("attach_unary_context_to_signals", True)
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
        ticker_raw = str(item["ticker"]).strip()
        class_code = str(item["class_code"]).strip().upper()
        # SPBFUT: тикер регистрозависим (напр. SiM6); иначе GetInstrumentBy → 50002.
        ticker = ticker_raw if class_code == "SPBFUT" else ticker_raw.upper()
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

    lead_block = raw.get("lead_lag") or {}
    pairs_raw = lead_block.get("pairs") if isinstance(lead_block, dict) else []
    lead_lag_pairs: list[tuple[str, str]] = []
    if isinstance(pairs_raw, list):
        for item in pairs_raw:
            if not isinstance(item, dict):
                continue
            leader = str(item.get("leader", "")).strip()
            follower = str(item.get("follower", "")).strip()
            if leader and follower:
                lead_lag_pairs.append((leader, follower))

    return LoadedDetectorConfig(
        default=base_settings,
        per_instrument=per_instrument,
        lead_lag_pairs=tuple(lead_lag_pairs),
    )


def load_detector_settings(path: Path) -> DetectorSettings:
    """Backward-compatible: returns global detector defaults only."""
    return load_detector_config(path).default

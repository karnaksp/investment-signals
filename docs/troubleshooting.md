# Решение проблем

## Telegram: сигнал в Postgres есть, в чат не приходит

1. Убедитесь, что в `.env` заданы **`TELEGRAM_BOT_TOKEN`** и **`TELEGRAM_CHAT_ID`**, контейнер **`detector`** перезапущен после правок `.env` (`docker compose up -d detector`).
2. Смотрите логи: `docker compose logs detector | findstr /i telegram` (Windows) или `docker compose logs detector 2>&1 | grep -i telegram` (Linux/macOS).  
   При ошибке Bot API в лог пишется **тело ответа** (`description`). Частые причины:
   - в режиме **`parse_mode=HTML`** не поддерживаются теги **`<br>`** / **`<br/>`** — в проекте переносы делаются символом **новой строки**;
   - неверный **`chat_id`** (для супергрупп часто нужен id вида `-100…`);
   - бот не добавлен в канал/группу или нет прав писать в топик (`message_thread_id`).
3. При **HTTP 400** детектор повторяет отправку **без HTML** (plain text), чтобы сообщение всё равно дошло.

## Быстрая проверка: сигнал без живого рынка

С хоста при работающем Redpanda и детекторе:

```bash
pip install -e .
python scripts/push_synthetic_trading_status.py
```

Ожидается **`trading_status_changed`** в Postgres и в `GET /signals/recent`. Параметры: `KAFKA_HOST_BOOTSTRAP_SERVERS`, `KAFKA_RAW_TOPIC`, опционально `--instrument-id` (ключ как в `conf/instruments.yaml`).

## ClickHouse: `clickhouse-init` завершился с ошибкой

- **TTL / DateTime64:** в `001_market_raw.sql` для MergeTree TTL используется **`toDateTime(source_time)`**, иначе в новых версиях CH возможна ошибка `BAD_TTL_EXPRESSION`.
- **Представления VWAP:** во `002_feature_store_bars.sql` агрегаты во внешнем SELECT не вкладывают `sum()` внутрь другого `sum()` — см. подзапросы с `trades_total`.

## Dagster UI недоступен

Проверьте порт **`HOST_DAGSTER_PORT`** (по умолчанию **30300**) и что подняты **`dagster-webserver`** и **`dagster-daemon`**. Подробности — [orchestration.md](orchestration.md).

## `redpanda-init` exit 1 → detector не стартует

Compose ждёт успешного завершения **`redpanda-init`**. В логах частая причина:

```text
unable to query config schema: Get "http://127.0.0.1:9644/...": connection refused
```

Init-контейнер вызывал `rpk cluster config set` без **`admin.hosts=redpanda:9644`** (Admin API на сервисе `redpanda`, не на localhost). В актуальном `docker-compose.yml` это исправлено; опциональные `cluster config` помечены `|| true`.

```bash
docker compose logs redpanda-init --tail 30
docker compose run --rm redpanda-init
docker compose up -d detector
docker compose ps detector
```

## Redis: `ModuleNotFoundError: redis` в детекторе

Пакет **`redis`** входит в зависимости основного `pyproject.toml`; пересоберите образ: `docker compose build detector && docker compose up -d detector`.

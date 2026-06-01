# T-Invest Signal Engine

Конвейер обнаружения рыночных аномалий в реальном времени по данным T-Invest.

Проект собран как небольшой data-engineering стек:

```text
T-Invest (MarketDataStream)
            |
            v
       tinvest-raw-stream (ingestor)
            |
            v
   Redpanda (+ Schema Registry): marketdata.raw / .raw.unary / .signals
      /              |                    \
     v               v                     v
ClickHouse     tinvest-detector ◄── Redis (состояние окон, опционально)
Kafka→MergeTree      |              при REDIS_URL
 + feature bars      +----------+----------+
                      v          v          v
                 Postgres    signals    sinks (TG / webhook …)
                 (сигналы)    topic

tinvest-api (FastAPI) ──► Postgres · ClickHouse (read)

Оркестрация и unary (в Kafka, те же entrypoints что в compose):
  Dagster (webserver + daemon) ──► пороги в conf/*.yaml · single-shot unary
  tinvest-market-unary-emitter   ──► профиль compose «unary», цикл unary → raw

Observability:
  Prometheus ◄── detector · Redpanda · tinvest-accuracy-metrics (JSON из var/accuracy)
  Grafana
```

## Что делает система

- Забирает рыночные данные в реальном времени из официального потока T-Invest.
- Публикует нормализованные события в Kafka (по умолчанию JSON; опционально protobuf + Schema Registry).
- Обнаруживает скользящие аномалии по каждому тикеру:
  - аномальный объём
  - аномальное число сделок
  - резкое движение цены
  - расширение спреда
  - дисбаланс стакана
  - смена торгового статуса
- Отправляет алерт в мессенджер.
- Сохраняет в Postgres только срабатывания (аномалии).
- Полный нормализованный поток остаётся в Kafka и **дублируется в ClickHouse** для аналитики; в Postgres попадают только сигналы.
- Отдаёт последние сигналы и сводки через FastAPI.

## Зачем такой стек

- `T-Invest` даёт официальный рыночный поток.
- `Redpanda` — Kafka-совместимый каркас событий с низкой стоимостью локального развёртывания.
- `Postgres` — OLTP для **сигналов** и read API (не для полного сырого потока).
- `ClickHouse` — колоночный архив **сырых** событий из того же Kafka-топика для аналитики и бэктеста.
- `FastAPI` достаточно для тонкого read API и точки интеграции.
- `Prometheus` + `Grafana` в compose — задержки/throughput детектора и health брокера.
- `Redis` — при заданном `REDIS_URL` детектор сохраняет полное состояние (окна и cooldown) в ключ `tinvest:detector:v1:full_state` между рестартами.
- `Dagster` — расписания пересчёта порогов в `detectors.overrides.yaml` и unary single-shot в Kafka (см. `docs/orchestration.md`).

Так проект остаётся близким к реальному event pipeline, но достаточно лёгким для локального запуска.

Документация: [архитектура](docs/architecture.md), [детекторы](docs/detectors.md), [Signal Cockpit](docs/admin_cockpit.md), [Dagster](docs/orchestration.md), [SQL по сигналам](docs/signal_analytics.md), [решение проблем](docs/troubleshooting.md) (Telegram, синтетический тест, ClickHouse).

## Структура проекта

```text
conf/
  detectors.yaml
  instruments.yaml
sql/postgres/init/
  001_market_signals.sql
sql/clickhouse/init/
  001_market_raw.sql
  002_feature_store_bars.sql
scripts/
  push_synthetic_trading_status.py  # тест: trading_status → Kafka → сигнал
  resolve_nearest_futures.py        # ближайшие SPBFUT → блок FUTURES_NEAREST в instruments.yaml
observability/
  prometheus.yml
  grafana/provisioning/...
src/tinvest_signal_engine/
  config.py
  detector_core.py
  instruments.py
  sinks.py
  services/
    api.py
    detector_service.py
    ingestor.py
    market_unary_emitter.py
    threshold_cron.py
  orchestration_defs.py   # Dagster: jobs + schedules
tests/
  test_detector.py
```

## Быстрый старт

1. Скопируйте `.env.example` в `.env`.
2. Укажите токен T-Invest в `TINVEST_TOKEN`.
3. Настройте `conf/instruments.yaml` под свой список инструментов.
4. Запустите стек:

```bash
docker compose up --build
```

5. Откройте в браузере (порты см. `.env.example`):

- Redpanda Console: `http://localhost:38080`
- Schema Registry (Redpanda): `http://localhost:18081` (для следующих шагов с Avro/Protobuf)
- Prometheus: `http://localhost:39090`
- Grafana: `http://localhost:33000` (логин/пароль по умолчанию `admin` / `admin`, задайте через `GRAFANA_*` в `.env`)
- ClickHouse HTTP: `http://localhost:38123` (запросы к `signal_engine.market_raw_events`)
- Dagster: `http://localhost:30300` (расписания пересчёта порогов и unary single-shot; см. `docs/orchestration.md`)
- Проверка API: `http://localhost:38000/health` (liveness), `http://localhost:38000/ready` (readiness: ping Postgres)
- Последние сигналы: `http://localhost:38000/signals/recent`
- Signal Cockpit: задайте `ADMIN_API_TOKEN` в `.env`, перезапустите API и откройте `http://localhost:38000/admin`. Опционально: `ADMIN_API_RATE_LIMIT_PER_MINUTE`, `ADMIN_API_ALLOWED_IPS` для путей `/admin/api/*` (см. `.env.example`). Токен сохраняется в `localStorage` и уходит в заголовке `X-Admin-Token` (без `?token=` в URL). Основные разделы: `#/triage`, `#/signals`, `#/delivery`, `#/calibration`, `#/instruments`, `#/accuracy`, `#/settings`, `#/signal?id=…`. Для разметки «полезно/шум» примените SQL `sql/postgres/init/002_signal_admin_feedback.sql` к существующей БД (на чистом volume init подхватит сам). Контейнер `api` монтирует `./conf` и `./var/accuracy`, задаёт `SIGNAL_ACCURACY_JSON_PATH` / `CLICKHOUSE_HTTP_URL` для accuracy и raw-контекста. Скриншоты и описание экранов — в [docs/admin_cockpit.md](docs/admin_cockpit.md). Периодическая публикация unary-снимков в Kafka: `docker compose --profile unary up -d market-unary-emitter` и переменные `MARKET_UNARY_*` в `.env.example`.

### Проверка контура без живого рынка

При запущенном `docker compose` (Redpanda + детектор) можно отправить в топик `marketdata.raw` пару событий `trading_status` и получить сигнал **`trading_status_changed`**:

```bash
pip install -e .
python scripts/push_synthetic_trading_status.py
```

Затем откройте `http://localhost:38000/signals/recent`. Типичные сбои (Telegram, ClickHouse init, Dagster) — в [docs/troubleshooting.md](docs/troubleshooting.md).

## Всплывающие уведомления на рабочем столе Windows

Это возможно без прав администратора.

Важно: всплывающие уведомления должен показывать процесс на хосте Windows, а не изнутри Docker. Поэтому в репозитории есть отдельный локальный нотификатор: он подписывается на топик сигналов и показывает системные всплывающие окна на рабочем столе.

Запустите во втором терминале на Windows:

```bash
pip install -e .
tinvest-local-notifier
```

По умолчанию он ходит в Kafka по `localhost:39092` — это тот же порт Redpanda, что проброшен в `docker-compose.yml`.

Полезные переменные окружения:

- `KAFKA_HOST_BOOTSTRAP_SERVERS=localhost:39092`
- `LOCAL_NOTIFIER_CONSUMER_GROUP=local-notifier`
- `LOCAL_NOTIFICATION_DURATION_SECONDS=5`

## Конфигурация

### Инструменты

`conf/instruments.yaml` — список инструментов. Каждый задаётся парой `ticker` + `class_code`; то же поддерживается в T-Invest как `instrument_id`. Блок фьючерсов SPBFUT между маркерами `FUTURES_NEAREST` — **ближайшие ликвидные контракты** (нефть, металлы, валютные пары, пшеница, газ, Nasdaq, Sber/Газпром, индексы и т.д.); пересборка: `python scripts/resolve_nearest_futures.py --write` (нужен `TINVEST_TOKEN`). После записи YAML **ingestor** подхватит список при следующем перечитывании конфига (см. `CONFIG_RELOAD_INTERVAL_SECONDS`) или сразу при старте контейнера.

### Пороги детектора

`conf/detectors.yaml` задаёт скользящие окна, интервал выборки, пороги z-score и длительность cooldown.

Автоматические переопределения по инструментам пишет **Dagster** (job `threshold_recalc_job` по расписанию, см. `orchestration_defs.py`) в `conf/detectors.overrides.yaml`; сервис детектора подхватывает их без перезапуска. Старый бесконечный контейнер `threshold-cron` доступен с профилем `legacy-threshold-cron`.
Ежедневный пересчёт берёт часовые свечи за последние 7 дней и считает:

`price_move_absolute_threshold_bps = mean(abs((close - open) / open)) * 10_000 * THRESHOLD_HOURLY_DEVIATION_MULTIPLIER`

Полезные переменные окружения:

- `DETECTORS_OVERRIDES_CONFIG=conf/detectors.overrides.yaml`
- `THRESHOLD_RECALC_INTERVAL_HOURS=24`
- `THRESHOLD_LOOKBACK_DAYS=7`
- `THRESHOLD_HOURLY_DEVIATION_MULTIPLIER=1.0`
- `CONFIG_RELOAD_INTERVAL_SECONDS=10`

### Kafka: JSON или protobuf

- `KAFKA_RAW_VALUE_FORMAT` / `KAFKA_SIGNAL_VALUE_FORMAT`: `json` (по умолчанию) или `protobuf`.
- Для protobuf нужен `SCHEMA_REGISTRY_URL` **или** заранее выставленные `KAFKA_PROTOBUF_SCHEMA_ID_RAW` / `KAFKA_PROTOBUF_SCHEMA_ID_SIGNAL`.
- `PROTO_DIR` — каталог с `.proto` (в Docker образе: `/app/proto`).
- Детектор при `REDIS_URL` сохраняет в Redis и cooldown, и скользящие окна (`tinvest:detector:v1:full_state`).

### Алерты в Telegram

Детектор отправляет алерты по срабатыванию через Bot API (`parse_mode=HTML`: ссылки на терминал и карточку инструмента, жирный тикер). При ошибке разметки (HTTP 400) выполняется повтор **без HTML** (plain text), чтобы сообщение дошло.

В `.env`:

- `TELEGRAM_BOT_TOKEN=<ваш_токен_бота>`
- `TELEGRAM_CHAT_ID=<id_чата_или_канала>`
- `TELEGRAM_MESSAGE_THREAD_ID=<необязательно_id_топика_в_форуме>`

Если токен или chat id не заданы, доставка в Telegram отключена. После смены `.env` перезапустите **`detector`**. Диагностика: `docker compose logs detector` (ищите `Telegram`, `sendMessage`). Подробнее — [docs/troubleshooting.md](docs/troubleshooting.md).

## Документация

- **Архитектура и роли компонентов** — `docs/architecture.md` (потоки данных, таблицы топиков и сервисов).
- **Детекторы и торговые паттерны** — `docs/detectors.md` (типы сигналов, смысл для рынка, окна и пороги).
- **Журнал отвергнутых гипотез** — `docs/research_log.md` (чтобы не повторять одни и те же ветки эволюции стека).

### Интеграционный тест и бэктест (локально)

1. Поднимите стек: `docker compose up -d --build`.
2. Экспортируйте `RUN_INTEGRATION=1` и при необходимости `CLICKHOUSE_HTTP_URL`, `KAFKA_HOST_BOOTSTRAP_SERVERS`.
3. `python -m pytest tests/ -q` — сработает `tests/integration/test_kafka_clickhouse.py`.
4. Оффлайн-прогон детектора по истории в ClickHouse (urllib, без торговли):  
   `python scripts/backtest_from_clickhouse.py --instrument SBER_TQBR --hours 168`  
   Тяжёлую аналитику на слабом ПК удобнее вести в **DuckDB** по экспорту Parquet: см. `scripts/duckdb_feature_smoke.py`.
5. Оценка «попал / промах» по forward VWAP (DuckDB, без торговли): экспортируйте сигналы (CSV/Parquet с `instrument_id`, `signal_type`, `detected_at`) и бары `vw_trade_bar_1m_vwap`, затем  
   `python scripts/duckdb_label_signals.py --signals sig.parquet --bars bars.parquet --forward-bars 1`  
   или несколько горизонтов: `--forward-bars 1,5,15`. Результат можно положить в `var/accuracy/signal_accuracy.json` — сервис `accuracy-metrics` в compose отдаёт это в Prometheus (дашборд Grafana «Signal pipeline & quality»).
6. Protobuf на wire: `proto/normalized_event.proto`, `proto/trigger_signal.proto`. Регистрация в SR:  
   `SCHEMA_REGISTRY_URL=http://localhost:18081 python scripts/register_schemas_sr.py`  
   затем в `.env`: `KAFKA_RAW_VALUE_FORMAT=protobuf`, `KAFKA_SIGNAL_VALUE_FORMAT=protobuf`, `SCHEMA_REGISTRY_URL=...`, при необходимости фиксированные `KAFKA_PROTOBUF_SCHEMA_ID_*`. **ClickHouse** Kafka Engine в `001_market_raw.sql` ожидает JSON — при protobuf на `marketdata.raw` отключите этот MV или держите отдельный JSON-топик для OLAP.
- **Статический сайт из MkDocs** (включая автосправочник по коду `tinvest_signal_engine`):

```bash
pip install -e ".[docs]"
python -m mkdocs serve
```

Сборка в каталог `site/`: `python -m mkdocs build` (или `mkdocs build`, если скрипт в `PATH`).

Публичная документация публикуется через GitHub Pages: `https://karnaksp.github.io/investment-signals/`.
Workflow `.github/workflows/docs.yml` автоматически собирает MkDocs и обновляет Pages при каждом push в `main`.
В GitHub Pages должен быть выбран source `GitHub Actions`.

- **Интерактивное описание REST** при запущенном API: Swagger UI `http://localhost:38000/docs`, ReDoc `http://localhost:38000/redoc`, схема OpenAPI `http://localhost:38000/openapi.json`.

## API

- `GET /health`
- `GET /signals/recent?limit=50&instrument_id=SBER_TQBR`
- `GET /signals/summary?minutes=60`

Подробности параметров и схем ответов — на странице `/docs` у работающего сервиса `tinvest-api`.

## Замечания

- Логика детектора намеренно модульная и находится в `detector_core.py`.
- Текущая реализация держит состояние стриминга в памяти по каждому инструменту.
- Каталог **`src/tinkoff`** (официальный Python SDK T-Invest) **не хранится в git** (см. `.gitignore`). В Docker-образе SDK **клонируется** из [RussianInvestments/invest-python](https://github.com/RussianInvestments/invest-python) при сборке (`Dockerfile`, build-arg **`INVEST_PYTHON_REF`**, по умолчанию `0.2.0-beta117`); в контекст сборки каталог с хоста не передаётся (`.dockerignore`). После `git clone` репозитория для локальных `pytest`: `python scripts/sync_invest_sdk.py` (нужен `git` в `PATH`).
- Retention Redpanda настроен с ограничением локального объёма данных (100 МБ) для лёгких локальных прогонов.

## Официальные ссылки

- T-Invest `MarketDataStream`: <https://developer.tbank.ru/invest/api/market-data-stream-service-market-data-stream>
- T-Invest `GetInstrumentBy`: <https://developer.tbank.ru/invest/api/instruments-service-get-instrument-by>
- Актуальный Python SDK (T-Invest): <https://github.com/RussianInvestments/invest-python>

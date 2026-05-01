# T-Invest Signal Engine

Конвейер обнаружения рыночных аномалий в реальном времени по данным T-Invest.

Проект собран как небольшой data-engineering стек:

```text
T-Invest MarketDataStream
        |
        v
     Redpanda
        |
        v
  Signal Detector
        |
   +----+----+
   |         |
   v         v
Postgres   Alert topic / webhook
   |
   v
 FastAPI
```

## Что делает система

- Забирает рыночные данные в реальном времени из официального потока T-Invest.
- Публикует нормализованные JSON-события в Kafka-совместимый брокер.
- Обнаруживает скользящие аномалии по каждому тикеру:
  - аномальный объём
  - аномальное число сделок
  - резкое движение цены
  - расширение спреда
  - дисбаланс стакана
  - смена торгового статуса
- Отправляет алерт в мессенджер.
- Сохраняет в Postgres только срабатывания (аномалии).
- Основной рыночный поток держит в Kafka как транзитный.
- Отдаёт последние сигналы и сводки через FastAPI.

## Зачем такой стек

- `T-Invest` даёт официальный рыночный поток.
- `Redpanda` — Kafka-совместимый каркас событий с низкой стоимостью локального развёртывания.
- `Postgres` — надёжное OLTP-хранилище для триггер-событий и чтения API.
- `FastAPI` достаточно для тонкого read API и точки интеграции.

Так проект остаётся близким к реальному event pipeline, но достаточно лёгким для локального запуска.

## Структура проекта

```text
conf/
  detectors.yaml
  instruments.yaml
sql/postgres/init/
  001_market_signals.sql
src/tinvest_signal_engine/
  config.py
  detector_core.py
  instruments.py
  sinks.py
  services/
    api.py
    detector_service.py
    ingestor.py
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

5. Откройте в браузере:

- Redpanda Console: `http://localhost:38080`
- Проверка API: `http://localhost:38000/health`
- Последние сигналы: `http://localhost:38000/signals/recent`

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

`conf/instruments.yaml` — список инструментов. Каждый задаётся парой `ticker` + `class_code`; то же поддерживается в T-Invest как `instrument_id`.

### Пороги детектора

`conf/detectors.yaml` задаёт скользящие окна, интервал выборки, пороги z-score и длительность cooldown.

Автоматические переопределения по инструментам пишет `tinvest-threshold-cron` в `conf/detectors.overrides.yaml`; сервис детектора подхватывает их без перезапуска.
Ежедневное задание берёт часовые свечи за последние 7 дней и считает:

`price_move_absolute_threshold_bps = mean(abs((close - open) / open)) * 10_000 * THRESHOLD_HOURLY_DEVIATION_MULTIPLIER`

Полезные переменные окружения:

- `DETECTORS_OVERRIDES_CONFIG=conf/detectors.overrides.yaml`
- `THRESHOLD_RECALC_INTERVAL_HOURS=24`
- `THRESHOLD_LOOKBACK_DAYS=7`
- `THRESHOLD_HOURLY_DEVIATION_MULTIPLIER=1.0`
- `CONFIG_RELOAD_INTERVAL_SECONDS=10`

### Алерты в Telegram

Детектор может отправлять алерты по срабатыванию напрямую в Telegram через Bot API.

В `.env`:

- `TELEGRAM_BOT_TOKEN=<ваш_токен_бота>`
- `TELEGRAM_CHAT_ID=<id_чата_или_канала>`
- `TELEGRAM_MESSAGE_THREAD_ID=<необязательно_id_топика_в_форуме>`

Если токен или chat id не заданы, доставка в Telegram отключена.

## Документация

- **Архитектура и роли компонентов** — `docs/architecture.md` (потоки данных, таблицы топиков и сервисов).
- **Детекторы и торговые паттерны** — `docs/detectors.md` (типы сигналов, смысл для рынка, окна и пороги).
- **Статический сайт из MkDocs** (включая автосправочник по коду `tinvest_signal_engine`):

```bash
pip install -e ".[docs]"
python -m mkdocs serve
```

Сборка в каталог `site/`: `python -m mkdocs build` (или `mkdocs build`, если скрипт в `PATH`).

- **Интерактивное описание REST** при запущенном API: Swagger UI `http://localhost:38000/docs`, ReDoc `http://localhost:38000/redoc`, схема OpenAPI `http://localhost:38000/openapi.json`.

## API

- `GET /health`
- `GET /signals/recent?limit=50&instrument_id=SBER_TQBR`
- `GET /signals/summary?minutes=60`

Подробности параметров и схем ответов — на странице `/docs` у работающего сервиса `tinvest-api`.

## Замечания

- Логика детектора намеренно модульная и находится в `detector_core.py`.
- Текущая реализация держит состояние стриминга в памяти по каждому инструменту.
- В репозитории vendored исходники официального SDK `tinkoff/invest-python` в `src/tinkoff`, чтобы Docker-сборки не зависели от доступа к PyPI-пакету только в пререлизе.
- Retention Redpanda настроен с ограничением локального объёма данных (100 МБ) для лёгких локальных прогонов.

## Официальные ссылки

- T-Invest `MarketDataStream`: <https://developer.tbank.ru/invest/api/market-data-stream-service-market-data-stream>
- T-Invest `GetInstrumentBy`: <https://developer.tbank.ru/invest/api/instruments-service-get-instrument-by>
- Официальный Python SDK: <https://github.com/Tinkoff/invest-python>

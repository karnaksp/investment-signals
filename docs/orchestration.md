# Оркестрация: Dagster в Docker Compose

Периодические задачи выполняет **Dagster** (веб-интерфейс + `dagster-daemon`), а не отдельный бесконечный `threshold-cron`. Код расположен в `src/tinvest_signal_engine/orchestration_defs.py`.

## Что запускается по умолчанию

| Schedule | Job | Смысл |
|----------|-----|--------|
| `daily_threshold_recalc` | `threshold_recalc_job` | Один вызов `run_recalc_once()` → запись `conf/detectors.overrides.yaml` из почасовых свечей T-Invest. |
| `market_unary_poll_schedule` | `unary_kafka_poll_once_job` | Один цикл `tinvest-market-unary-emitter` с `MARKET_UNARY_SINGLE_SHOT=1` → Kafka (`MARKET_UNARY_*` из `.env`). |

Cron по умолчанию (UTC): пересчёт порогов **`0 2 * * *`**, unary **`*/15 * * * *`**. Переопределение без правки кода:

- `DAGSTER_THRESHOLD_CRON` — cron для порогов;
- `DAGSTER_UNARY_CRON` — cron для unary.

После `docker compose up` UI: `http://localhost:${HOST_DAGSTER_PORT:-30300}`.

## Зависимости и тома

- Сервисы `dagster-webserver` и `dagster-daemon` используют тот же образ, что ingestor/detector (`pip install ".[orchestration]"` при сборке).
- Общий том **`dagster_home`**: метаданные запусков (SQLite в `DAGSTER_HOME`).
- Каталог **`./conf`** смонтирован **на запись**, чтобы job порогов обновлял `detectors.overrides.yaml` (как раньше у `threshold-cron`).
- Для unary в контейнере задано `MARKET_UNARY_POLL_SECONDS` (по умолчанию 300), иначе single-shot не стартует.

## Локальная разработка без Docker

```bash
pip install -e ".[orchestration]"
set DAGSTER_HOME=%CD%\.dagster_home
mkdir .dagster_home 2>nul
dagster-webserver -h 127.0.0.1 -p 3000 -m tinvest_signal_engine.orchestration_defs
```

Во втором терминале:

```bash
dagster-daemon run -m tinvest_signal_engine.orchestration_defs
```

Переменные (`TINVEST_TOKEN`, `KAFKA_BOOTSTRAP_SERVERS`, пути к `conf/`) должны совпадать с теми, что для `tinvest-threshold-cron` и emitter.

## Legacy

Бесконечный цикл **`tinvest-threshold-cron`** доступен как сервис compose с профилем **`legacy-threshold-cron`** (см. `docker-compose.yml`), если нужно повторить старое поведение без Dagster.

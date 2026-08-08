# Operations and Rollout

Эта страница описывает runtime и calibration loop, которые позволяют развивать Signal Engine без увеличения шума в delivery.

## Runtime Fingerprint

`/health`, `/ready` и `/admin/api/settings` возвращают runtime fingerprint:

| Field | Meaning |
|---|---|
| `app_version` | Версия package/runtime из `APP_VERSION` или package metadata. |
| `commit_sha` | Build commit из `APP_COMMIT_SHA`, GitHub SHA или локального `git rev-parse`. |
| `build_time` | Build timestamp из `APP_BUILD_TIME`, `BUILD_TIME` или `unknown`. |

Signal Cockpit показывает этот fingerprint в верхней status bar после первого запроса settings. Используйте его, чтобы проверить, какой commit создал Telegram alert или admin row.

Docker builds могут передавать:

```bash
docker build \
  --build-arg APP_VERSION=0.2.0 \
  --build-arg APP_COMMIT_SHA="$(git rev-parse --short=12 HEAD)" \
  --build-arg APP_BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t tinvest-signal-engine .
```

## CI

`.github/workflows/ci.yml` запускается на pushes, pull requests и manual dispatch:

```bash
python -m pytest -q
node --check src/tinvest_signal_engine/static/admin_app.js
python -m mkdocs build --strict
cp .env.example .env
docker compose config --quiet
docker build \
  --build-arg APP_COMMIT_SHA="${GITHUB_SHA}" \
  --build-arg APP_BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t tinvest-signal-engine:ci .
docker run --rm tinvest-signal-engine:ci python -c "from tinvest_signal_engine.runtime_info import runtime_fingerprint; from tinvest_signal_engine.services.api import create_app; app = create_app(); print(app.title, runtime_fingerprint())"
```

`.github/workflows/docs.yml` отвечает за GitHub Pages deployment из `main`.

## Delivery Policy v3

Delivery v3 сохраняет storage-first rule: detector и enrichment сохраняют signals, а delivery решает, что может выйти из системы наружу.

Каждый signal payload получает дополнительную read-only metadata:

| Field | Values |
|---|---|
| `delivery_priority` | `high`, `medium`, `low`. |
| `delivery_channel` | `realtime` для доставленных alerts, `digest` для digest candidates, `admin_only` для suppressed/admin-only signals. |
| `delivery_explanation_ru` | Человекочитаемое русское объяснение решения. |

`/admin/api/delivery/simulation` dry-runs candidate policy поверх сохраненных signals. Endpoint не обновляет payloads и не отправляет Telegram/webhook messages.

Simulation presets:

| Preset | Purpose |
|---|---|
| `current` | Повторяет активные runtime delivery settings. |
| `conservative` | Поднимает quality floor, чтобы проверить более строгий Telegram gate. |
| `admin_only_rollout` | Принудительно переводит experimental rollout types в `admin_only`, даже если текущие env rules продвигают их выше. |

`SIGNAL_DELIVERY_TYPE_RULES_JSON` можно использовать для явного per-type promotion или holdback:

```json
{
  "candle_range_spike": { "admin_only": true },
  "aggressive_trade_burst": { "channel": "digest", "min_quality": 75 },
  "obi_dynamics": { "always": true }
}
```

Используйте `always` только после feedback/accuracy review: это продвигает тип в realtime. `channel=digest` оставляет signal вне realtime Telegram, но помечает его как digest candidate в admin analytics.

## Feedback Loop

`/admin/api/feedback/overview` агрегирует существующие admin feedback labels и не требует отдельной migration для чтения summary:

- totals по useful/noise/unsure;
- type x delivery x feedback;
- ticker x delivery x feedback;
- delivered signals, отмеченные как noise;
- suppressed signals, отмеченные как useful.

Используйте страницу Feedback перед изменением thresholds. Useful suppressed signal - кандидат на digest или realtime review; noisy delivered signal - кандидат на более строгую delivery policy.

## Source Health

`/admin/api/source-health` объединяет `conf/instruments.yaml`, detector config и ClickHouse raw-event freshness.

Для каждого instrument endpoint показывает availability последних `trade`, `last_price`, `orderbook`, `candle`, `trading_status` и `open_interest`. Также он объясняет, почему signal type сейчас невозможен:

| Reason | Meaning |
|---|---|
| `source_not_subscribed` | Required source не включен для instrument. |
| `source_stale` | Source настроен, но recent raw events не найдены. |
| `source_unknown` | ClickHouse недоступен или не настроен. |
| `config_disabled` | Detector config отключает signal type. |

Если ClickHouse недоступен, endpoint возвращает `status=unknown`, а admin остается usable.

## Accuracy Job

`scripts/duckdb_label_signals.py` может сформировать admin accuracy JSON:

```bash
python scripts/duckdb_label_signals.py \
  --signals var/exports/signals.csv \
  --bars var/exports/bars.csv \
  --forward-bars 1,5,15 \
  --output var/accuracy/signal_accuracy.json
```

Report группирует forward VWAP metrics по signal type, ticker, quality tier и delivery status. `/admin/api/accuracy` читает `SIGNAL_ACCURACY_JSON_PATH`; если файла нет, endpoint возвращает empty state вместо ошибки страницы.

## Controlled Rollout

Новые или ранее отключенные signals должны начинаться как storage/admin-only data:

| Stage | Signal type | Initial channel |
|---|---|---|
| 1 | `candle_range_spike` на 5-10 liquid TQBR instruments с включенными candles | `admin_only` |
| 2 | `obi_dynamics` на core equities/futures со стабильным orderbook | `admin_only` |
| 3 | `open_interest_spike` только для futures с реальными source data | `admin_only` |
| 4 | `aggressive_trade_burst` на самых liquid instruments | `admin_only` |
| 5 | `lead_lag_divergence` только для явно настроенных pairs | `admin_only` |

Продвигайте signal из admin-only в digest или realtime только после того, как source health, feedback и accuracy data подтверждают изменение.

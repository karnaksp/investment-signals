# Operations and Rollout

This page documents the runtime and calibration loop used to evolve Signal Engine without returning Telegram spam.

## Runtime Fingerprint

`/health`, `/ready`, and `/admin/api/settings` include a runtime fingerprint:

| Field | Meaning |
|---|---|
| `app_version` | Package/runtime version from `APP_VERSION` or package metadata. |
| `commit_sha` | Build commit from `APP_COMMIT_SHA`, GitHub SHA, or local `git rev-parse`. |
| `build_time` | Build timestamp from `APP_BUILD_TIME`, `BUILD_TIME`, or `unknown`. |

Signal Cockpit shows this fingerprint in the top status bar after the first settings request. Use it to verify which commit produced a Telegram alert or an admin row.

Docker builds can pass:

```bash
docker build \
  --build-arg APP_VERSION=0.1.0 \
  --build-arg APP_COMMIT_SHA="$(git rev-parse --short=12 HEAD)" \
  --build-arg APP_BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t tinvest-signal-engine .
```

## CI

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch:

```bash
python -m pytest -q
node --check src/tinvest_signal_engine/static/admin_app.js
python -m mkdocs build
```

`.github/workflows/docs.yml` remains responsible for GitHub Pages deployment from `main`.

## Delivery Policy v3

Delivery v3 keeps the storage-first rule: detector and enrichment save signals, delivery decides what can leave the system.

Every signal payload gets additional read-only metadata:

| Field | Values |
|---|---|
| `delivery_priority` | `high`, `medium`, `low`. |
| `delivery_channel` | `realtime` for delivered alerts, `admin_only` for suppressed/admin-only signals. |
| `delivery_explanation_ru` | Human-readable Russian explanation for the decision. |

`/admin/api/delivery/simulation` dry-runs a candidate policy over stored signals. It never updates payloads and never sends Telegram/webhook messages.

## Feedback Loop

`/admin/api/feedback/overview` aggregates existing admin feedback labels without a required SQL migration:

- useful/noise/unsure totals;
- type x delivery x feedback;
- ticker x delivery x feedback;
- delivered signals marked noise;
- suppressed signals marked useful.

Use the Feedback page before changing thresholds. A useful suppressed signal is a candidate for digest or realtime review; a noisy delivered signal is a candidate for stricter delivery.

## Source Health

`/admin/api/source-health` combines `conf/instruments.yaml`, detector config, and ClickHouse raw-event freshness.

For each instrument it reports recent `trade`, `last_price`, `orderbook`, `candle`, `trading_status`, and `open_interest` availability. It also explains why a signal type is impossible now:

| Reason | Meaning |
|---|---|
| `source_not_subscribed` | The required source is not enabled for the instrument. |
| `source_stale` | The source is configured but no recent raw events were found. |
| `source_unknown` | ClickHouse is unavailable or not configured. |
| `config_disabled` | Detector config disables the signal type. |

If ClickHouse is unavailable, the endpoint returns `status=unknown` and the admin remains usable.

## Accuracy Job

`scripts/duckdb_label_signals.py` can produce the admin accuracy JSON:

```bash
python scripts/duckdb_label_signals.py \
  --signals var/exports/signals.csv \
  --bars var/exports/bars.csv \
  --forward-bars 1,5,15 \
  --output var/accuracy/signal_accuracy.json
```

The report groups forward VWAP metrics by signal type, ticker, quality tier, and delivery status. `/admin/api/accuracy` reads `SIGNAL_ACCURACY_JSON_PATH`; when the file is missing, it returns an empty state instead of failing the page.

## Controlled Rollout

New or previously disabled signals should start as storage/admin-only data:

| Stage | Signal type | Initial channel |
|---|---|---|
| 1 | `candle_range_spike` on 5-10 liquid TQBR instruments with candles enabled | `admin_only` |
| 2 | `obi_dynamics` on core equities/futures with stable orderbook | `admin_only` |
| 3 | `open_interest_spike` only for futures with real source data | `admin_only` |
| 4 | `aggressive_trade_burst` on the most liquid instruments | `admin_only` |
| 5 | `lead_lag_divergence` only for explicitly configured pairs | `admin_only` |

Promote a signal from admin-only to digest or realtime only after source health, feedback, and accuracy data support the change.

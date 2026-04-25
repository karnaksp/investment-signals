# T-Invest Signal Engine

Realtime market anomaly detection pipeline for T-Invest data.

The project is built as a small data-engineering stack, not as a single script:

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
ClickHouse  Alert topic / webhook
   |
   v
 FastAPI
```

## What it does

- Pulls realtime market data from the official T-Invest stream.
- Publishes normalized JSON events into a Kafka-compatible broker.
- Detects rolling anomalies per ticker:
  - abnormal volume
  - abnormal trade count
  - sharp price move
  - spread widening
  - order book imbalance
  - trading status change
- Stores signals in ClickHouse.
- Exposes recent signals and summaries through FastAPI.

## Why this stack

- `T-Invest` gives the official market stream.
- `Redpanda` gives a Kafka-compatible event backbone with low local setup cost.
- `ClickHouse` is a strong fit for fast analytics over append-only signal events.
- `FastAPI` is enough for a thin read API and integration surface.

This keeps the project close to a real event pipeline while remaining lightweight enough to run locally.

## Project layout

```text
conf/
  detectors.yaml
  instruments.yaml
sql/clickhouse/init/
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

## Quickstart

1. Copy `.env.example` to `.env`.
2. Put your T-Invest token into `TINVEST_TOKEN`.
3. Adjust `conf/instruments.yaml` with your watchlist.
4. Start the stack:

```bash
docker compose up --build
```

5. Open:

- Redpanda Console: `http://localhost:38080`
- API health: `http://localhost:38000/health`
- Recent signals: `http://localhost:38000/signals/recent`

## Desktop popup alerts on Windows

This is possible without administrator rights.

Important detail: popup notifications must be shown by a process running on your Windows host, not from inside Docker. Because of that, the repository includes a separate local notifier that subscribes to the signal topic and shows system popups on your desktop.

Run it in a second terminal on Windows:

```bash
pip install -e .
tinvest-local-notifier
```

By default it reads Kafka from `localhost:39092`, which matches the exposed Redpanda port in `docker-compose.yml`.

Relevant env vars:

- `KAFKA_HOST_BOOTSTRAP_SERVERS=localhost:39092`
- `LOCAL_NOTIFIER_CONSUMER_GROUP=local-notifier`
- `LOCAL_NOTIFICATION_DURATION_SECONDS=5`

## Local Python run

If you prefer to run the services without Docker:

```bash
pip install -e .
tinvest-raw-stream
tinvest-detector
tinvest-api
tinvest-local-notifier
```

You still need a running Kafka-compatible broker and ClickHouse.

## Configuration

### Instruments

`conf/instruments.yaml` is the watchlist. Each instrument is configured by `ticker` + `class_code`, which is also supported by T-Invest as `instrument_id`.

### Detector thresholds

`conf/detectors.yaml` controls rolling windows, sample interval, z-score thresholds, and cooldowns.

## API

- `GET /health`
- `GET /signals/recent?limit=50&instrument_id=SBER_TQBR`
- `GET /signals/summary?minutes=60`

## Notes

- The detector logic is intentionally modular and lives in `detector_core.py`.
- The current implementation keeps streaming state in memory per instrument.
- For production scale, the next step would be a stateful stream processor or partition-aware horizontal workers.
- The repository vendors the official `tinkoff/invest-python` SDK source locally under `src/tinkoff` so Docker builds do not depend on access to a pre-release-only PyPI package.

## Official references

- T-Invest `MarketDataStream`: <https://developer.tbank.ru/invest/api/market-data-stream-service-market-data-stream>
- T-Invest `GetInstrumentBy`: <https://developer.tbank.ru/invest/api/instruments-service-get-instrument-by>
- Official Python SDK: <https://github.com/Tinkoff/invest-python>

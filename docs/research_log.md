# Research log (отвергнутые гипотезы и направления)

Фиксируем решения, чтобы не повторять слепые ветки при эволюции пайплайна.

## 2026-05-09 — цикл 1 (Data Ops)

| Гипотеза | Почему отклонено / отложено | Альтернатива |
|----------|-----------------------------|----------------|
| **uvloop в текущем детекторе** | `tinvest-detector` — синхронный цикл `kafka-python`; `uvloop` влияет на asyncio и не ускоряет блокирующий consumer. | Вынос обработки в async-движок (aiokafka + asyncio) — отдельный эпик. |
| **Protobuf/Avro вместо JSON в v1** | Потребует Schema Registry + генерацию схем из `NormalizedEvent`, правки ingestor/detector/local_notifier/ClickHouse формата; высокий риск поломки hot reload конфигов. | JSON + `orjson` на горячем пути; SR уже слушает Redpanda (`8081`) для следующих итераций. |
| **Redis/RocksDB для окон детектора в этом цикле** | Нужен отдельный сервис, сериализация deque, миграции версий состояния, тесты отказов. | Пока in-memory + ClickHouse как архив сырья для оффлайн/бэктеста; Redis — после контракта состояния. |
| **Great Expectations в рантайме** | Тяжёлый стек, YAML-проекты, медленный старт в контейнере детектора. | Лёгкая валидация `data_quality.py` + JSON-логи для Loki/ELK. |
| **IMOEX / кросс-площадочный фильтр в коде** | Нет единого бесплатного потока согласованных таймстемпов в репозитории; риск галлюцинаций без контракта данных. | Зафиксировано в `docs/architecture.md` как этап: внешний индекс → отдельный топик → опциональный join в детекторе. |

## 2026-05-09 — цикл 2 (Feature store + alpha + DuckDB)

| Решение | Почему |
|---------|--------|
| **DuckDB для тяжёлой оффлайн-оценки** на ноутбуке | Меньше фоновой нагрузки, чем держать тяжёлые циклы в ClickHouse; витрины VWAP остаются в CH (инкрементальные MV), а «лаборатория» — Parquet/CSV + DuckDB (`scripts/duckdb_feature_smoke.py`). |
| **Proto на wire — пока только `proto/normalized_event.proto`** | Полная миграция producer/consumer + SR + ClickHouse `Protobuf` требует стопроцентной совместимости с hot-reload и Console; следующий PR: регистрация схемы + `WIRE_FORMAT` env. |
| **Redis в compose без полного offload deque** | Сериализация всех `deque` в Redis на каждом тике дорога по latency; пока `REDIS_URL` + ping в детекторе + том для будущего `StateStore`. |
| **Grafana «accuracy»** | Истинная точность = join сигнал ↔ forward mid (CH или DuckDB); в дашборде пока **proxy** (throughput, emits/s, p95 latency) до появления batch-лабеллера. |

**Литература (order-flow / скальпинг, вне индикаторов OHLC):** в прикладной микроструктуре упор на **order book imbalance / queue dynamics**, **burstiness агрессора**, **lead–lag между ликвидным и менее ликвидным инструментом**; классические RSI/MACD для HFT-потока почти не используются. Полезные входные точки 2024–2025: [Forecasting high frequency order flow imbalance using Hawkes processes](https://arxiv.org/abs/2408.03594), [Order Book Filtration and Directional Signal Extraction at High Frequency](https://arxiv.org/html/2507.22712v1) (фильтрация шума L2, «мигание» лимиток). Конкретные коэффициенты под инструмент калибровать на своих данных.

## 2026-05-09 — цикл 3 (субагенты / DuckDB labeller)

| Факт | Действие |
|------|----------|
| **Task-субагенты недоступны** (лимит окружения) | Итерации выполнены основным агентом без делегирования. |
| **Нужен оффлайн «accuracy» без Grafana-базы** | Добавлен `scripts/duckdb_label_signals.py`: join сигналов с экспортом `vw_trade_bar_1m_vwap`, hit/miss для directional типов, JSON summary. Тест `tests/test_duckdb_label_signals.py` (skip без `duckdb`). |
| **Исследование сигналов** | В лог добавлены ссылки на OFI/Hawkes/LOB (см. ниже в литературе цикла 2 + [Deep LOB guide](https://arxiv.org/pdf/2403.09267), [conditional order imbalance](https://www.tandfonline.com/doi/full/10.1080/14697688.2024.2358963)). |

## Идеи на следующие циклы

- **Consumer lag в Prometheus**: либо `kafka.Consumer` метрики через JMX/Exporter, либо лёгкий sidecar с `rpk group describe`.
- **RocksDB state store**: один файл на инструмент + периодический flush + версия схемы в ключе.
- **VPIN / OBI**: расширить `orderbook` payload (глубина >3) и вынести фичи в отдельный модуль фиче-инжиниринга перед z-score.

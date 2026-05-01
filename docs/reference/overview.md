# Обзор модулей `tinvest_signal_engine`

Автодокументация ниже строится из **докстрингов и сигнатур** в исходниках. Охвачен только пакет **`tinvest_signal_engine`**; каталог **`src/tinkoff`** (vendored SDK) в справочник не включён.

| Модуль | Назначение |
|--------|------------|
| [`config`](config.md) | YAML-конфиги, `RuntimeSettings`, настройки детектора |
| [`models`, `serialization`](models.md) | События, сигналы, JSON/время/котировки |
| [`detector_core`](detector_core.md) | `SignalDetector`, состояние по инструменту |
| [`instruments`](instruments.md) | Реестр инструментов T-Invest |
| [`sinks`](sinks.md) | Postgres, webhook, Telegram |
| [`services`](services.md) | CLI: ingestor, detector, api, notifier, cron |

Дочерние страницы открывают сгенерированные блоки **mkdocstrings**.

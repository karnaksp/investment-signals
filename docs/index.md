# Документация T-Invest Signal Engine

Здесь собраны:

- **[Архитектура](architecture.md)** — как устроен конвейер, какие процессы за что отвечают, потоки данных и конфигурация.
- **[Детекторы и торговые паттерны](detectors.md)** — какие типы сигналов бывают, что они означают с точки зрения рынка, формулы и пороги.
- **[HTTP API (OpenAPI)](openapi.md)** — как пользоваться интерактивной автодокументацией REST-сервиса.
- **[Signal Cockpit](admin_cockpit.md)** — новая админка для triage, delivery policy, calibration и instrument universe со скриншотами.
- **[Справочник по коду](reference/overview.md)** — автоматически извлечённые из Python-докстрингов описания модулей пакета `tinvest_signal_engine` (без vendored SDK `tinkoff`).
- **[Оркестрация (Dagster)](orchestration.md)** — расписания порогов и unary.
- **[Аналитика сигналов в Postgres](signal_analytics.md)** — примеры SQL.
- **[Решение проблем](troubleshooting.md)** — Telegram, синтетический тест сигнала, ClickHouse init, Dagster.

Краткое введение и быстрый старт — в корневом файле `README.md` репозитория.

## Сборка статического сайта

Требуется Python 3.11+.

```bash
pip install -e ".[docs]"
python -m mkdocs build
```

Результат — каталог `site/` (его можно открыть локально или выкладывать на gh-pages / внутренний хостинг).

## Локальный предпросмотр

```bash
python -m mkdocs serve
```

Откройте в браузере адрес, который выведет MkDocs (обычно `http://127.0.0.1:8000`).

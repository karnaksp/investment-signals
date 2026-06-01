# Документация T-Invest Signal Engine

<section class="doc-hero">
  <p class="hero-kicker">Realtime market anomaly pipeline</p>
  <h1>Signal Engine для T-Invest market data</h1>
  <p>
    Документация описывает весь контур: детекторы, storage-first delivery, Telegram-фильтрацию,
    публичную аналитику сигналов и новую админку Signal Cockpit для ручного triage.
  </p>
  <div class="hero-actions">
    <a class="md-button md-button--primary" href="admin_cockpit/">Signal Cockpit</a>
    <a class="md-button" href="architecture/">Архитектура</a>
    <a class="md-button" href="detectors/">Детекторы</a>
  </div>
</section>

<div class="metric-strip">
  <div class="metric"><strong>Storage-first</strong><span>все enriched-сигналы сохраняются</span></div>
  <div class="metric"><strong>Delivery policy</strong><span>Telegram получает только сильные события</span></div>
  <div class="metric"><strong>Signal Cockpit</strong><span>triage, calibration, instruments</span></div>
  <div class="metric"><strong>GitHub Pages</strong><span>документация обновляется из main</span></div>
</div>

## Быстрый маршрут

<div class="doc-grid">
  <div class="doc-card">
    <a class="doc-card-title" href="admin_cockpit/">Signal Cockpit</a>
    <p>Новая static-админка: delivery funnel, suppressed reasons, таблица сигналов, drawer, calibration и universe инструментов.</p>
    <div class="pill-row">
      <span class="pill delivered">delivered</span>
      <span class="pill suppressed">suppressed</span>
      <span class="pill">triage</span>
    </div>
  </div>
  <div class="doc-card">
    <a class="doc-card-title" href="architecture/">Архитектура</a>
    <p>Как устроены detector, storage, Kafka/Postgres, delivery gate, API и админский слой.</p>
  </div>
  <div class="doc-card">
    <a class="doc-card-title" href="detectors/">Детекторы и паттерны</a>
    <p>Типы сигналов, рыночная интерпретация, формулы, дефолтные пороги и логика combo-score.</p>
  </div>
  <div class="doc-card">
    <a class="doc-card-title" href="signal_analytics/">SQL-аналитика</a>
    <p>Запросы для проверки частоты сигналов, качества delivery и поведения по тикерам.</p>
  </div>
  <div class="doc-card">
    <a class="doc-card-title" href="openapi/">HTTP API</a>
    <p>Swagger/OpenAPI входная точка для REST-сервиса и админских endpoints.</p>
  </div>
  <div class="doc-card">
    <a class="doc-card-title" href="troubleshooting/">Troubleshooting</a>
    <p>Проверка Telegram, синтетические сигналы, ClickHouse init, Dagster и частые runtime-проблемы.</p>
  </div>
</div>

## Что смотреть первым

Если нужно понять, почему сигнал не пришёл в Telegram, начните с [Signal Cockpit](admin_cockpit.md): там видны `delivery_status`, `delivery_reason`, rate-limit и соседние события по тому же инструменту.

Если нужно калибровать шум, откройте [Детекторы](detectors.md) и [Аналитику сигналов](signal_analytics.md): они связывают формулы, thresholds и реальные распределения в Postgres.

## Сборка статического сайта

Требуется Python 3.11+.

```bash
pip install -e ".[docs]"
python -m mkdocs build
```

Результат — каталог `site/`. GitHub Pages собирает эту документацию автоматически при обновлении `main`.

## Локальный предпросмотр

```bash
python -m mkdocs serve
```

Откройте адрес, который выведет MkDocs, обычно `http://127.0.0.1:8000`.

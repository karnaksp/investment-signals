# Changelog

## Unreleased

- CI и tag-driven release стали change-aware: правки индикаторов запускают
  целевой набор тестов и lightweight package smoke без пересборки Docker-стека;
  зависимости, миграции, инфраструктура и неизвестные пути по-прежнему требуют
  полный test suite и production image smoke.
- Версия локального runtime больше не дублируется вручную в Dockerfile, API и
  Admin Cockpit: релиз получает её из тега/build argument, установленный пакет
  — из package metadata, а исходники без установки показывают local-version.

## 1.0.0 - 2026-10-12

- Зафиксирован первый стабильный контракт production-oriented движка:
  поток T-Invest через Kafka/Redpanda, детектирование, надёжная фиксация
  сигналов, API/Admin Cockpit и управляемая доставка.
- Добавлено автоматическое обновление ликвидной вселенной фьючерсов с
  атомарной записью конфигурации и systemd/Dagster-интеграцией.
- Доставка сигналов учитывает минимальный денежный оборот, объясняет решение
  пользователю и использует более компактные Telegram-сообщения.
- Live-shadow проверки переведены в fail-closed режим, ограничены торговыми
  сессиями и корректно переживают перезапуски после пропущенных checkpoint.
- Candle cache использует ограниченные чтения и архивы истории T-Invest;
  release image уменьшен и запускается с зафиксированным runtime fingerprint.
- Исследовательский контур отделён от стабильного продукта и не используется
  как обещание доходности или автоматическая торговая рекомендация.

## 0.2.0 - 2026-08-08

- Теневое наблюдение переведено на решения и зрелые исходы реального времени;
  исторический replay больше не определяет операционный статус живых правил.
- Добавлены безопасная ежедневная адаптивная калибровка, отдельный утренний
  retracement worker и восстановление Kafka consumer с зафиксированного offset.
- Ограничено накопление `detector_observation_outbox`, `processed_events` и
  snapshot-таблиц; добавлены индексированные batch-retention и настройки vacuum.
- ClickHouse ingestion переведён на более крупные пачки, native observation
  storage и ограниченное число фоновых merges без `OPTIMIZE FINAL`.
- Delivery различает постоянные ошибки конфигурации и временные сетевые сбои;
  рабочие процессы публикуют heartbeat для честного операционного статуса.
- Добавлена tag-driven SemVer-публикация: каждый тег `vMAJOR.MINOR.PATCH`
  проверяется полным CI до создания GitHub Release.

- Admin Cockpit screenshot script синхронизирован с текущими routes: `triage`, `signals`, `delivery`, `calibration`, `instruments`, `accuracy` и `settings`.
- Добавлена CI smoke validation для Docker/Compose production runtime image.
- Runtime fingerprint metadata (`app_version`, `commit_sha`, `build_time`) добавлена в health, readiness и admin settings responses.
- Добавлена delivery policy v3 metadata: `delivery_priority`, `delivery_channel` и `delivery_explanation_ru`.
- Experimental rollout signal types остаются `admin_only` по умолчанию; добавлены явные custom delivery rules для `admin_only`/`digest`.
- В Signal Cockpit Triage и Signals tables добавлены быстрые feedback controls `Useful`/`Noise`/`Unsure`.
- Добавлены admin APIs для feedback overview, source health, delivery simulation и accuracy empty-state summaries.
- Добавлен повторяемый DuckDB accuracy JSON output для горизонтов 1/5/15 минут.
- Добавлены CI, Dependabot, issue templates и security guidance.

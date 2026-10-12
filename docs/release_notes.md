# Release notes

Эта страница фиксирует изменения, которые важны для эксплуатации, демонстрации и ревью проекта. В отличие от обычного changelog, здесь описан операторский эффект: что стало проще проверить, запустить, мониторить или безопасно выкатывать.

Релизы публикуются только из SemVer-тегов `vMAJOR.MINOR.PATCH`. Несовместимое
изменение стабильного публичного контракта повышает major; совместимый набор
новых возможностей повышает minor; исправление без нового контракта повышает
patch. До `1.0.0` несовместимые экспериментальные изменения повышают minor.

## 1.1.1 — 2026-10-12

Hotfix эксплуатации релиза 1.1.0: Compose теперь сам применяет версионные
миграции Postgres и ClickHouse до запуска detector/API/Dagster, а Dagster пишет
служебные данные в доступный непривилегированному пользователю persistent volume.
Функциональность и пороги realtime rollout v1.1 не изменены.

## 1.1.0 — 2026-10-12

### Сигналы, которые можно наблюдать в Telegram

- Подтверждённый импульс объединяет движение цены, всплеск активности и
  направление агрессивной дельты; отдельные типы показывают long/short bias.
- Вакуум ликвидности требует одновременно падения глубины L5 и аномального
  расширения спреда, поэтому описывает более содержательный эпизод, чем один
  широкий spread.
- Режим фьючерсного позиционирования использует изменение open interest и
  движение цены для различения набора и закрытия long/short позиций.
- OBI dynamics и aggressive trade burst также переведены в realtime rollout.
  Глобальный rate limit остаётся активным, а custom `admin_only` может
  немедленно остановить любой отдельный тип.

### Эксплуатация и релизы

- Инжестор запрашивает open interest прямо в trade subscription.
- Hot-reload notional policy отключает неприменимый оборотный фильтр только
  для rollout-типов и сохраняет 40-миллионный порог для остальных.
- Change-aware CI из предыдущего цикла вошёл в релиз: последующие изменения,
  ограниченные индикаторами, не требуют сборки production Docker stack.
- В релизе нет миграций данных; откат выполняется возвратом на `v1.0.0` и
  прежнюю конфигурацию детекторов.

## 1.0.0 — 2026-10-12

### Первый стабильный продуктовый контракт

- Стабилизирован полный контур: получение рыночных данных T-Invest,
  нормализация и Kafka/Redpanda, детектирование, надёжное хранение, API,
  Admin Cockpit, наблюдаемость и управляемая доставка.
- Runtime fingerprint одинаково отражается в образе, API и интерфейсе, поэтому
  оператор видит версию, commit SHA и время сборки запущенного экземпляра.
- Публикация остаётся tag-driven: тег `v1.0.0` проходит полный набор тестов,
  Clean Architecture gate, strict-сборку документации, проверку JavaScript,
  сборку Python-пакета и smoke production-образа.

### Ликвидная вселенная фьючерсов

- Добавлена отдельная доменная модель вселенной фьючерсов и адаптер каталога
  T-Invest без протекания vendor SDK во внутренние слои.
- Сервис `tinvest-refresh-futures` атомарно обновляет YAML-конфигурацию,
  выбирая актуальные ликвидные контракты; доступны Dagster и systemd wiring.
- Резолвер и runtime используют один application use case, что исключает
  расхождение ручного и автоматического обновления.

### Качество сигналов и доставка

- Volume-spike доставка получила минимальные пороги денежного оборота и
  настраиваемые переопределения по инструментам.
- Telegram-сообщения стали короче, а причины доставки и подавления сохраняются
  в объяснимой policy metadata.
- Live-shadow health fail-closed при регрессии, ограничивает историю торговыми
  сессиями и не считает уже прошедшие checkpoint после рестарта ошибкой.

### Надёжность и эксплуатация

- Candle cache читает большие Parquet-наборы ограниченно и поддерживает
  архивы истории T-Invest без неограниченного роста памяти.
- Production image больше не содержит неиспользуемый Pebble и сохраняет только
  необходимые runtime-компоненты.
- Исследовательские гипотезы и их evidence явно отделены от стабильной
  операционной поверхности продукта.

### Граница утверждений

Версия `1.0.0` означает стабильность программного и операционного контракта,
а не доказанную доходность сигналов. Проект не исполняет сделки и не является
индивидуальной инвестиционной рекомендацией.

## 0.2.0 — 2026-08-08

### Живой контур и честные статусы

- Live-shadow статус строится по реальным решениям и зрелым исходам, а не по
  прогрессу исторического пересчёта.
- Каждый длительно работающий worker публикует heartbeat; stale, failed и
  authentication states не маскируются общим зелёным статусом процесса.
- Kafka recovery продолжает работу с committed offset и применяет короткий
  warm-up только к новой consumer group.

### Производительность и хранение

- Retention `detector_observation_outbox` и `processed_events` выполняется
  индексированными ограниченными пачками вне активного рыночного окна.
- Snapshot storage получил агрессивный vacuum для часто обновляемых строк, а
  ClickHouse ingestion — увеличенный flush interval и bounded part retention.
- Ежедневная калибровка использует только зрелые локальные исходы завершённых
  сессий и применяет новую версию параметров только после validation gates.

### Операционная готовность

- Добавлен CI smoke для production Docker runtime: workflow проверяет Docker Compose config, собирает production image и запускает container-level проверку. Это снижает риск, что README и compose-инструкции расходятся с реальным runtime.
- В health, readiness и admin settings responses добавлены runtime fingerprint поля `app_version`, `commit_sha`, `build_time`. Оператор может быстро понять, какой build сейчас отвечает в API/Admin Cockpit.
- Redpanda image defaults закреплены явно, чтобы локальный стек и CI меньше зависели от плавающих upstream tags.
- MkDocs собирается в strict mode, поэтому битые ссылки и ошибки навигации блокируют документационные изменения до merge.

### Admin Cockpit

- Screenshot automation синхронизирована с текущими routes: `triage`, `signals`, `delivery`, `calibration`, `instruments`, `accuracy`, `settings`. Это делает визуальные доказательства в документации воспроизводимыми.
- В Triage и Signals tables добавлены быстрые feedback controls: `Useful`, `Noise`, `Unsure`. Это помогает собирать operator feedback без ручных SQL-правок.
- Добавлены admin APIs для feedback overview, source health, delivery simulation и accuracy empty-state summaries. Эти endpoints поддерживают разбор качества сигналов и delivery behavior без доступа к production trading credentials.

### Delivery policy

- Добавлена Delivery Policy v3 metadata: `delivery_priority`, `delivery_channel`, `delivery_explanation_ru`. Теперь решение о доставке объясняется в данных, а не только в коде.
- Experimental rollout signal types остаются `admin_only` по умолчанию; для них добавлены явные custom delivery rules `admin_only` и `digest`. Это снижает риск шумных alerts при тестировании новых signal types.

### Quality and calibration

- Добавлен repeatable DuckDB accuracy JSON output для горизонтов 1/5/15 минут. Файл используется для Admin Cockpit accuracy screens и может обновляться без подключения к live market stream.
- Улучшен пустой state для accuracy summaries: cockpit показывает отсутствие данных как ожидаемое состояние, а не как поломку UI/API.

### Документация и сопровождение

- README, roadmap, operations docs и Admin Cockpit docs приведены к production/product-purpose wording: архитектура, запуск, smoke-проверки, screenshots и troubleshooting находятся в первом пути чтения.
- Добавлены contributing guide, code of conduct, PR template, security notes и Dependabot. Это делает репозиторий понятнее для внешнего reviewer и безопаснее для публичной документации.

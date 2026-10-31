# Проверка сводки позиций по сырью — 2026-10-31

## Реальные источники

- SHFE: 2026-10-30 к 2026-10-29, золото AU и серебро AG,
  полные категории по всем срокам.
- CFTC: 2026-10-27 к 2026-10-20, восемь рынков disaggregated futures only.
  Коды: 088691, 084691, 076651, 075651, 085692, 067651, 023651, 001602.
- ICE: Brent Futures ICE Futures Europe, 2026-10-27 к 2026-10-20,
  `COTHist2026.csv`, только FutOnly. Недельный Brent NYMEX не подменяет ICE.
- Все девять недельных рынков прошли сверку обеих сторон пяти групп с ОИ.
  Изменения CFTC совпали с предыдущими отчётами; изменения ICE рассчитаны
  по двум строкам, поскольку опубликованные поля изменений пусты.

Исходные ответы и HTML-просмотр сохранены локально под
`var/shfe-daily-positions/`; артефакты содержат публичные рыночные данные,
а не Telegram-учётные данные. Итоговый HTML — 2 781 символ, одно сообщение.

## Выполненные проверки

- `.venv/bin/python -m pytest -q`: **1143 passed, 5 skipped**, 26.32 с.
  Предупреждение Starlette/httpx уже присутствовало и не относится к изменению.
- `RUN_INTEGRATION=1` и реальные настройки локального Postgres:
  `tests/integration/test_daily_positions_postgres.py` и
  `tests/integration/test_reliable_processing_postgres.py`: **3 passed**.
  Для legacy и новой сводки повторы сохраняют одну outbox-запись, изменение
  основного SHA отклоняется, контрольная точка детектора не создаётся.
- `.venv/bin/python scripts/check_architecture.py`: OK.
- `.venv/bin/python -m mkdocs build --strict`: exit 0.
- `docker compose config --quiet`: exit 0.
- Реальный `run_once(dry_run=True)`: preview, SHFE 2026-10-30 к 2026-10-29,
  CFTC/ICE по девяти рынкам; верные новые названия групп.

## Доставка

Обновлены только `dagster-daemon` и `delivery-worker`:

```bash
docker compose up -d --no-deps --no-build --force-recreate dagster-daemon delivery-worker
```

Сбор в контейнере вернул `queued` для 2026-10-30 к 2026-10-29.
Запрос к `delivery_outbox JOIN market_signals`, ограниченный версией
`commodity_positions_v1`, подтвердил одну запись: `delivered`, attempt_count=1,
last_error_code=NULL, delivered_at=2026-10-31 17:51:30.651105 UTC,
weekly_markets=9. Повторный сбор вернул `already_queued`.

Расписание сохранено: 13:00 и 15:00 Europe/Moscow по будням,
не более одного отчёта новой версии на торговую дату SHFE. Новая версия
создала одну консолидированную сводку поверх ранее отправленного legacy-отчёта;
это не повтор той же версии.

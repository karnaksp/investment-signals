# Проверка карточек позиций — 2026-10-31

## Фактический результат

Девять независимых PNG с короткими подписями: gold, silver, platinum,
palladium, copper, wti, natural_gas, wheat_srw, brent.
Внутри gold/silver объединены только источники того же металла: SHFE
2026-10-30 к 2026-10-29 и CFTC 2026-10-27 к 2026-10-20.
Остальные карточки содержат по одному недельному источнику от 2026-10-27.

PNG проверены визуально на полном размере и на ширине 360 px. В контейнере
с Linux DejaVu Sans для всех девяти реальных карточек проверены границы
146 надписей: ни одна не выходит за изображение. Доли рассчитываются из
L/(L+S), без spreading; изменение перевеса — из неокруглённых контрактов.
Карточки не агрегируют Китай и США и не трактуют сокращение лонга как шорт.

## Выполненные проверки

- `.venv/bin/python -m pytest -q`: **1163 passed, 6 skipped**, 27.90 с.
- Реальный локальный PostgreSQL, `RUN_INTEGRATION=1`:
  `tests/integration/test_daily_positions_postgres.py` и
  `tests/integration/test_reliable_processing_postgres.py`: **4 passed**.
- `python scripts/check_architecture.py`: OK.
- `docker compose config --quiet`, `uv lock --check`: exit 0.
- `docker compose build dagster-daemon delivery-worker`: оба образа собраны
  с Pillow и `fonts-dejavu-core`; остальные сервисы не собирались.
- Контейнерный dry-run: `preview`, 146 надписей внутри изображения.
- Unit-проверка повторов: 9 первоначальных отчётов; повтор даёт 0 новых;
  следующий день SHFE с неизменившейся неделей даёт только 2 новых.
- HTTP: реальные multipart PNG-байты, подпись, chat/thread, проверяемый TLS-host;
  после неоднозначного разрыва HTTP нет немедленного перехода к другому адресу.

## Реальная доставка

После пересоздания только двух сервисов сбор в контейнере вернул `queued`.
`delivery_outbox JOIN market_signals`, ограниченный версией
`commodity_asset_v1`, подтвердил **9 delivered**, maximum attempt_count=1,
last_error_code=NULL. Доставка началась с gold в 18:10:25 UTC, затем silver
в 18:10:26 UTC; последний brent доставлен в 18:10:36.201395 UTC 2026-10-31.
Повторный запуск вернул `already_queued`. Старый общий отчёт не создаётся.

Картинки и короткие подписи отправлены одним `sendPhoto` на актив. Имена
получателей, идентификаторы чатов и bot token в доказательства не включены.

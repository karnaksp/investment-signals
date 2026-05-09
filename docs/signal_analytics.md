# Аналитика сигналов в Postgres

В таблице `market_signals` хранятся только срабатывания; сырой поток — в Kafka / ClickHouse. Ниже — практичные запросы для оценки **пользы** сигналов: плотность по типам, совместные события и разбор комбо.

## Разбор комбо (`microstructure_combo_*`)

Комбо-сигналы содержат в `payload_json` объект **`combo_detail`**: флаги, начисленные баллы по компонентам и пороги. Пример: выбрать комбо за сутки с явным разбором вкладов.

```sql
SELECT
  detected_at,
  ticker,
  signal_type,
  payload_json->'combo_detail'->'points_awarded' AS points_awarded,
  payload_json->'combo_detail'->'flags' AS flags,
  (payload_json->'combo_detail'->'scores'->>'long')::int AS score_long,
  (payload_json->'combo_detail'->'scores'->>'short')::int AS score_short
FROM market_signals
WHERE signal_type IN ('microstructure_combo_long', 'microstructure_combo_short')
  AND detected_at > now() - interval '1 day'
ORDER BY detected_at DESC;
```

Фильтр «только комбо, где реально участвовали и спред, и дельта»:

```sql
SELECT *
FROM market_signals
WHERE signal_type = 'microstructure_combo_long'
  AND (payload_json->'combo_detail'->'points_awarded'->>'spread')::int > 0
  AND (payload_json->'combo_detail'->'points_awarded'->>'delta_long')::int > 0;
```

## Совместность типов по инструменту (окно времени)

Сколько раз за час по тикеру встречались **разные** типы сигналов (признак «насыщенного» микроструктурного эпизода):

```sql
WITH w AS (
  SELECT *
  FROM market_signals
  WHERE instrument_id = $1
    AND detected_at > now() - interval '1 hour'
)
SELECT signal_type, count(*) AS n
FROM w
GROUP BY signal_type
ORDER BY n DESC;
```

Пары типов, попавшие в одно **узкое** окно (например 30 с) — для ручной проверки гипотез «A часто следует за B»:

```sql
SELECT
  a.signal_type AS t1,
  b.signal_type AS t2,
  count(*) AS pair_count
FROM market_signals a
JOIN market_signals b
  ON a.instrument_id = b.instrument_id
 AND b.detected_at > a.detected_at
 AND b.detected_at <= a.detected_at + interval '30 seconds'
 AND a.signal_type < b.signal_type
WHERE a.detected_at > now() - interval '7 days'
GROUP BY 1, 2
ORDER BY pair_count DESC
LIMIT 30;
```

## Качество и шум

Если в `payload_json` или в связанном слое обогащения пишется оценка качества (см. `signal_enrichment` / API), можно отфильтровать слабые срабатывания на стороне отчётов. Минимальный вариант без дополнительных колонок — по `severity` и по наличию `combo_detail.points_awarded` с ненулевыми полями.

## Связь с ClickHouse

Для привязки сигнала к микросекундной ленте событий используйте `instrument_id` + интервал `detected_at ± window_seconds` и запросы к архиву сырых событий (см. `sql/clickhouse/init/`).

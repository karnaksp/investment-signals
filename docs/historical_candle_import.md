# Импорт сохранённой истории свечей

Команда переносит уже сохранённые минутные свечи из локального Parquet-кэша
в `scientific_candles_1m`. Она не содержит клиента T‑Invest, не читает токен
брокера и поэтому не может повторно скачать готовые данные.

Перед записью проверяются:

- вид и версия манифеста, источник и минутный интервал;
- отсутствие заявленных сбоев и приватных данных;
- SHA‑256 каждого файла и неизменность файла между описью и чтением;
- число строк для всех разделов, перечисленных в манифесте;
- явный часовой пояс, торговый день по Москве и завершённость свечи;
- схема, цены, целочисленный объём, порядок и отсутствие дублей времени.

Валидные файлы, появившиеся после публикации манифеста, тоже проверяются и
импортируются. Их количество отдельно возвращается как
`additional_validated_partitions`; скрытого доверия к таким файлам нет.

## Предварительная проверка

```bash
tinvest-import-scientific-history run \
  --dry-run \
  --cache-dir /opt/investment-signals-pro/core/var/research/tinvest_candles/v1 \
  --state-dir /var/lib/investment-signals-pro/imports/scientific-candles-v1 \
  --instruments-config /etc/investment-signals-pro/instruments.yaml
```

Предварительная проверка читает ClickHouse, но не пишет свечи, прогресс или
результат. Для короткой проверки можно дополнительно указать `--tickers SBER`,
`--start-day`, `--end-day` или `--max-partitions`.

## Реальный импорт

```bash
tinvest-import-scientific-history run \
  --cache-dir /opt/investment-signals-pro/core/var/research/tinvest_candles/v1 \
  --state-dir /var/lib/investment-signals-pro/imports/scientific-candles-v1 \
  --instruments-config /etc/investment-signals-pro/instruments.yaml \
  --batch-size 50000 \
  --partition-group-size 50
```

Нужны `CLICKHOUSE_HTTP_URL`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USERNAME` и
`CLICKHOUSE_PASSWORD` либо `CLICKHOUSE_PASSWORD_FILE`. Один процесс получает
исключительную блокировку состояния. После каждой проверенной группы атомарно
сохраняются водяные знаки по инструменту и дню. Прерванный запуск можно
повторить той же командой; уже подтверждённые разделы не читаются и не пишутся
повторно.

## Состояние

```bash
tinvest-import-scientific-history status \
  --state-dir /var/lib/investment-signals-pro/imports/scientific-candles-v1
```

Команды печатают одну агрегированную строку JSON. В ней нет токенов, рыночных
строк или многотысячного списка разделов. Итог содержит число разделов и строк,
расхождение с манифестом, маркеры пробелов, уже существующие и добавленные
строки, а также количество пакетов записи и чтения.

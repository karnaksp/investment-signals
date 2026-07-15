# GA real-data studies

These studies are redacted aggregates generated locally from official T-Invest
market data. They must not persist tokens, account identifiers, instrument UIDs,
raw candles, prices or individual event rows.

## TBank TLS certificate chain

TBank uses the Russian Trusted CA chain for `invest-public-api.tbank.ru`. Do not
disable TLS verification. Prepare the pinned CA bundle first:

```bash
.venv/bin/python scripts/study_tinvest_directional_hypothesis.py \
  --prepare-russian-trusted-ca .tmp/russiantrustedca2024.pem
```

Then run the study with explicit TLS verification:

```bash
.venv/bin/python scripts/study_tinvest_directional_hypothesis.py \
  --env-file .env \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --calendar-days 45 \
  --end-day 2026-07-13 \
  --horizons 1,5,15 \
  --output-dir .tmp/ga-real-data \
  --ca-cert .tmp/russiantrustedca2024.pem
```

For an independent validation cohort, use an explicit non-overlapping date range
instead of a rolling `--calendar-days` window. Set `--start-day` to the first
calendar day after the previous packaged validation range:

```bash
.venv/bin/python scripts/study_tinvest_directional_hypothesis.py \
  --env-file .env \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --start-day 2026-07-15 \
  --end-day 2026-08-31 \
  --horizons 1,5,15 \
  --output-dir .tmp/ga-real-data-independent-cohort \
  --ca-cert .tmp/russiantrustedca2024.pem
```

The aggregate `scope.date_selection` will be `explicit_date_range`, making the
cohort boundary auditable before release qualification.

The CA preparation command verifies the pinned SHA-256 of the extracted
`russiantrustedca2024.pem` bundle before writing it. If the hash changes, stop
and review the official certificate distribution before rerunning the study.

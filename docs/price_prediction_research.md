# Price prediction research

This workflow studies price movement only at moments when the signal detector
fires. It is offline research tooling; it does not change production detector,
delivery, or trading behavior.

## Install optional research dependencies

```bash
pip install -e ".[research]"
```

The `research` extra enables DuckDB Parquet I/O, scikit-learn logistic
regression, and LightGBM. Without it, pure helper tests still run, but Parquet
cache and model training commands will report missing optional dependencies.

## 1. Cache candles once

```bash
python scripts/research_cache_tinvest_candles.py \
  --env-file .env \
  --calendar-days 180 \
  --cache-dir var/research/tinvest_candles/v1
```

The cache is partitioned as:

```text
var/research/tinvest_candles/v1/ticker=SBER/date=2026-07-15.parquet
```

The command skips existing valid partitions, so later experiments reuse the
same local market data instead of repeatedly calling T-Invest. Persisted
artifacts intentionally exclude broker tokens, account identifiers, instrument
UIDs, and FIGIs.

## 2. Build the signal price dataset

```bash
python scripts/research_build_signal_price_dataset.py \
  --cache-dir var/research/tinvest_candles/v1 \
  --horizons 60,300,900,1800 \
  --lookback-windows 5,15,30,60 \
  --output var/research/datasets/signal_price_prediction.parquet
```

The output has one row per `signal × horizon` and includes:

- signal metadata and detector strength;
- pre-signal lookback features;
- recent signal combination features;
- volatility regime features;
- forward return, direction label, cost-adjusted directional result;
- reverse-direction result;
- triple-barrier label;
- binary meta-label for whether the original signal direction was useful after
  costs.

Features are built only from candles strictly before `source_event_at`. Forward
labels use only candles after the signal; paths through gaps or trading pauses
become `unavailable`.

## 3. Train baseline models and write reports

```bash
python scripts/research_train_price_models.py \
  --dataset var/research/datasets/signal_price_prediction.parquet \
  --output-dir var/research/runs
```

Each run writes:

- `dataset-manifest.json`;
- `model-results.json`;
- `leaderboard.csv`;
- `feature-importance.csv`;
- `slice-report.csv`;
- `report.md`.

## 4. Mine interpretable working patterns

```bash
python scripts/research_mine_price_patterns.py \
  --dataset var/research/datasets/signal_price_prediction.parquet \
  --run-dir var/research/runs/<run_id> \
  --top-fraction 0.10 \
  --min-n 100 \
  --accepted-min-n 300
```

The miner retrains the LightGBM classifier on the chronological train split,
scores validation rows, and writes:

- `probability-deciles.csv`;
- `pattern-candidates.csv`;
- `pattern-report.md`.

Accepted exploratory patterns require a validation-only top-probability group
with at least 300 rows, at least 20 sessions, positive rate materially above
the naive validation rate, and positive mean cost-adjusted directional result.

The first research track is intentionally explainable:

- event-study baseline;
- Bayesian confirmation score;
- logistic regression;
- LightGBM classifier;
- LightGBM regressor.

Use DeepLOB, TFT, or other sequence models only after this baseline proves that
there is stable signal value. DeepLOB requires local order-book history; candles
alone are not enough for that model class.

## Acceptance rule

Do not promote a candidate into product claims unless:

- validation has at least 30 trading sessions;
- the claimed family/horizon has at least 300 eligible rows;
- validation is chronological, not random;
- the result beats naive baseline and matched-control/event-study baseline;
- cost-adjusted directional result is positive for directional claims;
- inverse hypotheses are reported separately;
- an independent later holdout confirms the same effect.

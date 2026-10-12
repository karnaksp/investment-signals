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

For a forward order-book holdout, cache the same dates and tickers that were
covered by order-book collection:

```bash
python scripts/research_cache_tinvest_candles.py \
  --env-file .env \
  --cache-dir var/research/tinvest_candles/v1 \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --start-day 2026-07-16 \
  --end-day 2026-07-16
```

The cache is partitioned as:

```text
var/research/tinvest_candles/v1/ticker=SBER/date=2026-07-15.parquet
```

The command skips existing valid partitions, so later experiments reuse the
same local market data instead of repeatedly calling T-Invest. Persisted
artifacts intentionally exclude broker tokens, account identifiers, instrument
UIDs, and FIGIs.

The default Moscow session window is `07:00–19:00`. Caches created by older
versions started at `10:00`; the current command detects those legacy trading
partitions, downloads only the missing `07:00–09:59` interval, and merges it
with the existing main-session rows. A completed cache records its session
window in `manifest.json`, so subsequent runs do not repeat the repair.

The cache also preserves T-Invest's historical `volume_buy` and `volume_sell`
fields. The dataset derives buyer/seller imbalance at the event and over every
pre-signal window. This is available immediately for historical research and
does not require waiting for a new order-book archive to accumulate.

If T-Invest HTTPS verification fails with a Russian trusted-chain certificate
error, pass the local CA bundle explicitly and keep that option in generated
collection plans:

```bash
python scripts/research_plan_liquidity_collection.py \
  --readiness-json var/research/liquidity_holdout/current/readiness/readiness.json \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --ca-cert "/path/to/russiantrustedca2024.pem" \
  --output-dir var/research/liquidity_holdout/current/collection_plan
```

The generated shell script and `launchd` plist must contain `--ca-cert`; otherwise
the next scheduled collection can silently return to the default system trust
store and fail before collecting prior order-book snapshots.

Long order-book collection runs also pass `--orderbook-flush-every-samples 20`
by default. The collector writes `collection-progress.json` under the order-book
cache and periodically flushes snapshots into parquet partitions. This keeps a
multi-hour session useful even if the parent process is interrupted before the
final manifest is written.

## Passive order-book collection while the product runs

Historical candles remain the main no-wait research source. Do not block the
whole investigation waiting for a full trading-day order-book collection when
historical order books are unavailable from T-Invest.

Instead, accumulate order-book features opportunistically while the local product
is already running:

```bash
python scripts/research_passive_orderbook_collector.py \
  --service-health-url http://127.0.0.1:38000/health \
  --service-health-url http://127.0.0.1:18080/health \
  --service-health-url http://127.0.0.1:18443/health \
  --service-process-marker investment-signals-pro \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --samples 4 \
  --interval-seconds 15 \
  --flush-every-samples 1 \
  --ca-cert "/path/to/russiantrustedca2024.pem"
```

The passive collector first checks whether the product is alive by health URL or
process marker. If the service is not running, it writes a skipped result and
does not call T-Invest. If the service is running, it performs a short
order-book batch, flushes every sample, writes `passive-orderbook-result.json`
and `passive-orderbook-report.md`, then exits.

This command is safe to run from a host scheduler every 1–5 minutes during
market hours. Each invocation is short-lived; it does not replace candle-only
historical mining and does not wait for an 8-hour session.

For a simpler operator flow, keep a background loop on the same host as the
product:

```bash
python scripts/research_passive_orderbook_loop.py \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --samples 4 \
  --sample-interval-seconds 15 \
  --sleep-seconds 300 \
  --ca-cert "/path/to/russiantrustedca2024.pem"
```

By default the loop checks common local product health endpoints:

- `http://127.0.0.1:38000/health`;
- `http://127.0.0.1:18080/health`;
- `http://127.0.0.1:18443/health`.

If none of them responds and no product process marker is found, the iteration
is recorded as skipped and does not call T-Invest. Status is written to
`var/research/passive_orderbook/loop/passive-orderbook-loop-status.json` and
the readable report is written to
`var/research/passive_orderbook/loop/passive-orderbook-loop-report.md`.

To generate a local schedule for these short passive runs:

```bash
python scripts/research_plan_passive_orderbook_collection.py \
  --service-health-url http://127.0.0.1:38000/health \
  --service-health-url http://127.0.0.1:18080/health \
  --service-health-url http://127.0.0.1:18443/health \
  --service-process-marker investment-signals-pro \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --samples 4 \
  --sample-interval-seconds 15 \
  --schedule-interval-seconds 300 \
  --ca-cert "/path/to/russiantrustedca2024.pem" \
  --output-dir var/research/passive_orderbook/plan
```

This writes:

- `passive-collection-plan.json`;
- `passive-collection-plan.md`;
- `run-passive-orderbook-collector.sh`;
- `com.investment-signals.research-passive-orderbook.plist`;
- `investment-signals-research-passive-orderbook.service`;
- `investment-signals-research-passive-orderbook.timer`.

The generated `launchd` task is for macOS. The generated `systemd` timer is for
Ubuntu/Linux virtual machines. Both run the passive collector every five minutes
by default and are intentionally not loaded automatically. Enable one of them
only when you want the host to accumulate research order-book features while the
product is being used.

This is the recommended path for the current 90% reliability research: use the
existing candle cache immediately for broad hypothesis mining, and let the host
build a local order-book cache in the background while the service is actually
running. A product claim is allowed only after the later liquidity-aware dataset
passes the same chronological validation gate.

## Other no-wait research tracks

Order-book history should improve the model, but it must not be the only path.
While the background cache grows, continue these candle-only and event-only
tracks:

- search for strict skip rules: when a signal is usually noise, the product
  should say «пропустить», not force an up/down prediction;
- mine inverse hypotheses separately: if the original signal is more often
  followed by the opposite move, treat it as a reversal candidate, not as a
  failed direct signal;
- test combinations instead of single signal names: recent `price_jump`,
  `volume_spike`, `candle_range_spike`, repeated same-family events, and session
  bucket can define materially different states;
- keep chronological validation by trading day only; never accept a state found
  by random row split;
- export only shadow candidates first; production UI can show them as
  «недостаточно доказательств» until later live outcomes confirm the effect.

The expected product shape is a selective decision engine:

```text
signal event → market state → one of:
  1. ожидается рост
  2. ожидается снижение
  3. пропустить: уверенности не хватает
```

The third decision is not a failure. It is the main way to reach higher
reliability without overclaiming average-quality signals.

## 2. Build the signal price dataset

```bash
python scripts/research_build_signal_price_dataset.py \
  --cache-dir var/research/tinvest_candles/v1 \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --orderbook-cache-dir var/research/tinvest_orderbooks/v1 \
  --orderbook-max-age-seconds 30 \
  --require-orderbook-features \
  --horizons 60,300,900,1800 \
  --lookback-windows 5,15,30,60 \
  --output var/research/datasets/signal_price_prediction.parquet
```

The output has one row per `signal × horizon` and includes:

- signal metadata and detector strength;
- pre-signal lookback features;
- cross-instrument market context for the same lookback windows: broad market
  return, market dispersion, signal return relative to market, and whether the
  signal direction is with or against the market move;
- recent signal combination features;
- event strength versus prior volatility, range, and volume;
- historical buyer/seller volume imbalance at the event, its alignment with
  the detected direction, and its change versus the prior 5/15/30/60 minutes;
- event candle shape: body share, upper/lower wick share, close quality in the
  signal direction, and reversal pressure. This is the no-wait candle-only
  proxy for exhaustion and inverse-hypothesis search while order-book history is
  still sparse;
- pre-signal directional alignment and consolidation features;
- ticker liquidity/noise proxies from local candles: daily volume quantile,
  ticker volume quantile, and mean daily volume;
- optional nearest prior order-book features: spread, depth, and imbalance;
- volatility regime features;
- forward return, direction label, cost-adjusted directional result;
- reverse-direction result;
- triple-barrier label;
- binary meta-label for whether the original signal direction was useful after
  costs.

Features are built only from candles strictly before `source_event_at`. Forward
labels use only candles after the signal; paths through gaps or trading pauses
become `unavailable`.

When `--orderbook-cache-dir` is provided, the builder uses only the latest
snapshot whose timestamp is not later than `source_event_at` and whose age is at
most `--orderbook-max-age-seconds`. Future order-book snapshots are ignored.
For liquidity-aware research, add `--require-orderbook-features`: the build
fails before writing a dataset if no signal row received a valid prior
order-book snapshot. This prevents treating a candle-only dataset as a
liquidity-aware model run.

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
- `confidence-threshold-report.csv`;
- `decision-audit.csv`;
- `confidence-band-audit.csv`;
- `confidence-band-audit.md`;
- `directional-state-candidates.csv`;
- `directional-state-report.md`;
- `selective-frontier.csv`;
- `candidate-watchlist.csv`;
- `high-confidence-slices.csv`;
- `temporal-stability-report.csv`;
- `temporal-stability-summary.csv`;
- `bayesian-state-threshold-report.csv`;
- `bayesian-state-temporal-summary.csv`;
- `bayesian-state-candidates.csv`;
- `selective-rule-candidates.csv`;
- `selective-rule-report.md`;
- `precision-scout-candidates.csv`;
- `precision-scout-report.md`;
- `false-positive-guards.csv`;
- `false-positive-guards.md`;
- `honest-market-states/honest-market-state-candidates.csv`;
- `honest-market-states/honest-market-state-report.md`;
- `gap-to-90.csv`;
- `gap-to-90.md`;
- `next-actions-90.md`;
- `new-feature-candidates.csv`;
- `decision-policy.json`;
- `decision-policy.md`;
- `safe-triage/safe-triage-decisions.csv`;
- `safe-triage/safe-triage-summary.json`;
- `safe-triage/safe-triage-report.md`;
- `selection-90-report.json`;
- `selection-90-report.md`;
- `report.md`.

`selection-90-report.md` is the main operator report for this research track. It
answers the product question directly: how many signals remain after each
confidence threshold, how many were successful, whether the lower reliability
bound is enough, and what the safe runtime action is. New training runs write it
automatically; for an existing run, regenerate it without retraining:

```bash
python scripts/research_report_90_selection.py \
  --run-dir var/research/runs/<run_id> \
  --output-dir var/research/runs/<run_id>
```

`confidence-threshold-report.csv` is the main artifact for high-confidence
triage. It trains separate up-move and down-move usefulness models, calibrates
their probabilities on a later slice of the training period, then reports for
each confidence threshold:

- how many validation rows remain selected;
- how many rows are skipped as insufficient confidence;
- up versus down decisions;
- direct versus inverse decisions relative to the original signal direction;
- realized success rate;
- lower 95% Wilson reliability bound;
- selected mean result after costs;
- whether the slice has observed 90% success;
- whether the lower reliability bound itself reaches 90%;
- whether the threshold clears the research gate.

This is intentionally a three-way decision: predict up, predict down, or skip.
A visible 90% success rate is not accepted when the sample is too small or the
lower reliability bound is weak.

Operationally, the product meaning of this research is strict:

- «ожидается рост» is allowed only after a validated up-direction slice passes
  the row, trading-day, reliability-bound, and positive-after-costs gates;
- «ожидается снижение» uses the same gates for down-direction or inverse
  hypotheses;
- «пропустить, недостаточно уверенности» is the default for every other row,
  including rows where the research model has a forced up/down direction but the
  evidence gate did not pass.

The target is therefore not 90% on all detector firings. The target is 90% on a
small, evidence-backed selected subset while most detector firings remain
explicit skips.

`decision-audit.csv` is the row-level audit file. It shows, for every validation
signal row, the up/down confidence, selected action, confidence band, direct or
inverse relation to the original signal, realized result after costs, and the
market-state fields used by candidate rules: session bucket, volatility bucket,
pre-signal consolidation bucket, liquidity bucket, order-book spread/depth/
imbalance buckets, recent signal cluster, combo key, and event strength versus
prior volatility and range. Use it to inspect which concrete situations are
being separated from noisy cases and to trace a candidate rule back to
individual signal rows.
`decision` is the thresholded policy action at the current policy threshold;
`frontier_decision` is the forced up/down direction used by
`selective-frontier.csv`. A row can therefore be `decision = skip` while still
having `frontier_decision = down` for research ranking.

To search direct and inverse hypotheses without using model probabilities, run:

```bash
python scripts/research_mine_honest_market_states.py \
  --dataset var/research/datasets/signal_price_prediction.parquet \
  --output-dir var/research/runs/<run_id>/honest-market-states
```

This creates `honest-market-state-candidates.csv` and
`honest-market-state-report.md`. The script turns every signal row into two
candidate actions — follow the original signal direction or use the inverse
hypothesis — then searches interpretable market states on the early period and
checks the same rules on the later period. It does not use LightGBM confidence.
The acceptance gate is still strict: at least 300 late-period rows, at least
30 trading days, at least 90% success, Wilson lower bound at least 75%, positive
mean result after costs, and no single day dominating the sample.

`confidence-reliability-report.csv` compares model confidence with realized
validation success. It groups validation rows into the product bands
`skip`, `weak_observation`, `working_hypothesis`, and `strong_signal`, then
writes observed success rate, Wilson lower bound, mean model confidence, and
the safe runtime action for each band. A 90%+ confidence band is still forced to
`safe_runtime_action = skip` unless its realized validation success, sample
size, trading-session count, lower reliability bound, and mean result after
costs all pass the research gate. This prevents wording such as «90%
confidence» from reaching the product when the model is overconfident.

`confidence-band-audit.md` is the Russian operator-facing version of the same
guard. It answers the practical question: «если модель говорит 60–75%, 75–90%
или 90%+, сколько таких случаев было, сколько успешных, какая нижняя граница
надёжности и что можно показывать пользователю?». The safe default remains
«пропустить, недостаточно уверенности» unless the band has at least 300 rows,
30 trading days, at least 90% observed success, at least 75% Wilson lower bound,
and positive result after costs. Passing this audit creates only a shadow
candidate; it still does not create a public product claim.

The `safe-triage/` files are written automatically by the training command. To
re-export the customer-safe three-way decision feed from existing run artifacts:

```bash
python scripts/research_export_safe_triage_decisions.py \
  --audit var/research/runs/<run_id>/decision-audit.csv \
  --policy var/research/runs/<run_id>/decision-policy.json \
  --reliability var/research/runs/<run_id>/confidence-reliability-report.csv \
  --output-dir var/research/runs/<run_id>/safe-triage
```

This writes:

- `safe-triage-decisions.csv`;
- `safe-triage-summary.json`;
- `safe-triage-report.md`.

When `decision-policy.json` is disabled, every row becomes
`пропустить, недостаточно уверенности`, even if `frontier_decision` has an
up/down research direction. A row can become «ожидается рост» or «ожидается
снижение» only when the policy is in shadow mode, the selected threshold is met,
and the matching confidence band has a validated safe runtime action.

`selective-frontier.csv` answers the question «what if we keep only the most
confident cases?». It sorts validation rows by model confidence and reports the
top 20, 50, 100, 300, 1,000, 3,000, and 10,000 cases overall and inside market
state groups such as decision, signal type, horizon, session bucket, volatility
bucket, signal clusters, and direct or inverse relation. This catches the common
trap where 18 successful cases out of 20 look like 90%, but the lower reliability
bound and sample size are too weak for a product claim.
When confidence values are tied, the ranking uses a stable row identifier, not
event time. This avoids accidentally promoting the latest validation tail as if
it were a stronger model signal.

`directional-state-candidates.csv` is written by the main training run and
explicitly separates direct and inverse hypotheses. Rows with
`frontier_decision_relation = inverse` mean that the useful hypothesis is not
«continue the original signal direction», but «this signal state may precede a
move in the opposite direction». Directional-state candidates use the same
sample-size, trading-day, reliability, result-after-costs, concentration, and
temporal-stability gates before becoming shadow candidates.

`selective-rule-candidates.csv` and `precision-scout-candidates.csv` also carry
temporal stability fields: `temporal_blocks`,
`temporal_blocks_with_selected`, `temporal_weak_blocks`,
`temporal_min_success_rate`, `temporal_min_mean_result_bps`, and
`temporal_supported`. A rule can pass the aggregate later-period gate and still
be blocked with `temporal_instability` if one of the later time blocks is weak.
This prevents a product candidate from being promoted just because good days
hide a bad period in the average.

`candidate-watchlist.csv` keeps those underpowered but interesting cases visible.
Rows enter this file when they already show at least 90% observed success and a
positive mean result, but still fail the research gate because of sample size,
number of trading days, or reliability bound. The file states how many rows and
days are still missing, how many additional successful observations would be
needed to keep 90% at the 300-row gate, and why the candidate remains
`watch_only`. This is the handoff list for future forward holdout collection, not
a product signal allowlist. Each row has a stable `candidate_id` based on the
scope and rule, so the same pattern can be tracked across future runs.

To inspect the concrete signal rows behind each watchlist candidate:

```bash
python scripts/research_extract_candidate_audit_rows.py \
  --watchlist var/research/runs/<run_id>/candidate-watchlist.csv \
  --audit var/research/runs/<run_id>/decision-audit.csv \
  --output var/research/runs/<run_id>/candidate-audit-rows.csv
```

The extractor applies the watchlist rule to `decision-audit.csv` and writes the
exact ranked rows used by the selective frontier. In watchlist rules,
`decision=up|down` means the forced research direction from
`frontier_decision`, not the currently enabled product policy action. This
keeps underpowered research candidates separate from customer-visible
decisions, which can still be `skip`.

To persist the watchlist across repeated research runs, update the local ledger:

```bash
python scripts/research_update_candidate_ledger.py \
  --watchlist var/research/runs/<run_id>/candidate-watchlist.csv \
  --run-dir var/research/runs/<run_id> \
  --ledger var/research/candidate-watchlist-ledger.json
```

The ledger is local research state. It stores candidate IDs, latest evidence,
and a bounded observation history. It does not contain tokens, account data, raw
market data, or a product allowlist. Re-running the same research dataset does
not increase the evidence count: observations are deduplicated by dataset
fingerprint when `model-results.json` is available, and otherwise by run ID.
This prevents a repeated historical run from looking like a new forward holdout.
For each candidate, the ledger stores both latest-run readiness and aggregate
readiness across unique observations. Aggregate readiness is the number that
should be used to decide whether the candidate has reached the 300-row and
30-trading-day research gate. Trading-day coverage is deduplicated from
`selected_trading_days` when available, so overlapping runs do not inflate the
day count.

To export a machine-readable research policy from the ledger:

```bash
python scripts/research_export_candidate_policy.py \
  --ledger var/research/candidate-watchlist-ledger.json \
  --output var/research/candidate-decision-policy.json
```

The exported policy keeps `default_action = skip`. A candidate can become
`shadow` only when aggregate readiness passes the research gate. Exported shadow
rules remain `admin_only` and `product_claim_allowed = false`; they are for
forward validation, not for customer-facing claims.

To evaluate an exported policy on a later run without changing the policy:

```bash
python scripts/research_evaluate_candidate_policy.py \
  --policy var/research/candidate-decision-policy.json \
  --frontier var/research/runs/<new_run_id>/selective-frontier.csv \
  --run-dir var/research/runs/<new_run_id> \
  --output-json var/research/candidate-policy-evaluation.json \
  --output-csv var/research/candidate-policy-evaluation.csv
```

The evaluator only scores rules already exported as `shadow`. `watch_only`
rules keep action `skip`. It also checks dataset independence: if the evaluation
run has the same dataset fingerprint as a source dataset in the policy, the row
is marked non-independent and cannot pass the shadow gate.

To apply an exported shadow candidate policy back to row-level audit data:

```bash
python scripts/research_apply_candidate_policy.py \
  --audit var/research/runs/<run_id>/decision-audit.csv \
  --policy var/research/candidate-decision-policy.json \
  --run-dir var/research/runs/<run_id> \
  --output-dir var/research/runs/<run_id>/candidate-policy
```

This writes:

- `candidate-policy-decisions.csv`;
- `candidate-policy-summary.json`;
- `candidate-policy-report.md`.

The applicator is still research-only and admin-only. It emits
«ожидается рост» or «ожидается снижение» only for rows matching exported
`shadow` rules with an explicit up/down direction. Every non-matching row, every
disabled policy, and every candidate without a direction becomes «пропустить,
недостаточно уверенности». If the current run has the same
`dataset_fingerprint` as a source dataset in the exported policy, the rule is
not applied and the row remains skipped with `shadow_policy_not_independent`.
`product_claim_allowed` remains `false`.

To search for market-state rules that separate «up», «down», and «skip»
decisions with out-of-sample verification:

```bash
python scripts/research_mine_directional_states.py \
  --audit var/research/runs/<run_id>/decision-audit.csv \
  --output-dir var/research/runs/<run_id>
```

This produces:

- `directional-state-candidates.csv`;
- `directional-state-report.md`.

The miner groups sufficiently frequent states on the earlier part of
`decision-audit.csv` and evaluates the same rules on the later part. It
explicitly includes reverse hypotheses via `frontier_decision_relation =
inverse`, so a rule can say that a signal state is more useful as a
mean-reversion warning than as a continuation signal. A state can be transferred
to shadow research only if the later period has at least 300 rows, 30 trading
days, 90% observed success, Wilson lower bound at least 75%, positive mean
result after costs, and no single day or ticker concentration.
State groups include pre-signal consolidation, liquidity, and order-book
microstructure buckets, so the miner can distinguish a jump after compression
in a liquid ticker with tight spread and one-sided depth from the same nominal
signal in a noisy, wide-spread, or already directional state. Rows without a
nearby prior order-book snapshot stay visible in `decision-audit.csv`, but are
not used as real microstructure candidate rules.

To search for selective conjunctions instead of only predefined state groups:

```bash
python scripts/research_mine_selective_rules.py \
  --audit var/research/runs/<run_id>/decision-audit.csv \
  --output-dir var/research/runs/<run_id> \
  --min-discovery-rows 50 \
  --min-discovery-success-rate 0.35 \
  --max-terms 2
```

This produces:

- `selective-rule-candidates.csv`;
- `selective-rule-report.md`.

The selective miner builds rules such as «frontier direction is down + late
session» or «price jump + high volatility» from the earlier trading days and
then evaluates them on later trading days. It exists for the core 90% goal:
most rows should stay as «пропустить, недостаточно уверенности», while only
rare, evidenced combinations may become «ожидается рост» or «ожидается
снижение». The default command searches pairs of conditions because it is fast
and reproducible on a local machine; larger conjunctions should be run only
after tightening support thresholds or preselecting candidate atoms.

For a stricter search of rare states, run the precision scout:

```bash
python scripts/research_mine_selective_rules.py \
  --audit var/research/runs/<run_id>/decision-audit.csv \
  --output-dir var/research/runs/<run_id> \
  --precision-scout \
  --min-discovery-rows 20 \
  --min-discovery-success-rate 0.65 \
  --expansion-min-success-rate 0.15 \
  --max-terms 4 \
  --beam-width 250
```

This produces:

- `precision-scout-candidates.csv`;
- `precision-scout-report.md`.

The scout searches narrower combinations, for example «inverse price jump +
late session + high day volatility + compressed pre-signal range». It is meant
to find possible 90% pockets, not to approve them. A candidate still remains
watch-only unless the later-day evaluation reaches the same gates: at least 300
rows, at least 30 trading sessions, observed success at least 90%, strong lower
reliability bound, positive result after costs, and no day/ticker concentration.
`--expansion-min-success-rate` is deliberately lower than the discovery gate so
the report can still show weak near-misses; such rows are marked
`discovery_weak` and are not product candidates. The scout canonicalizes
`frontier_confidence` and `max_confidence` into `model_confidence` to avoid
duplicated rules, and it reports the dominant later-day direction (`up` or
`down`) plus hypothesis relation (`direct`, `inverse`, or `neutral`) for each
candidate. Equivalent candidates that cover the same later-day rows are
deduplicated, and product-facing status prefers candidates with positive
cost-adjusted result over candidates that only have a higher raw hit rate.
For every scout row, the report also states how many current successes are
missing to reach 90%, how many additional successful outcomes are needed by the
300-row gate, how many future failures are still allowed, and what future
success rate would be required. This makes weak candidates visibly expensive to
prove instead of merely labeling them as promising. The `proof_viability` field
then classifies the proof burden as `forward_validation_possible`,
`severe_forward_validation_required`,
`near_perfect_forward_validation_required`, or `impossible_at_min_rows`.
`proof_next_action` turns that burden into a research action: keep collecting a
forward holdout, refine/add features, reject until cost-adjusted result becomes
positive, retire the candidate for the 300-row 90% gate, or move an accepted
candidate into shadow validation.

For the 90% goal, the target product behavior is deliberately selective. The
model should not turn every detector event into a directional call. It should
return one of three decisions: «ожидается рост», «ожидается снижение», or
«пропустить, недостаточно уверенности». The 90% target applies only to the
retained minority of cases; the skipped majority is part of the product value
because it removes noisy signals from the operator workflow.

`high-confidence-slices.csv` searches for rare market-state pockets instead of
trying to improve every signal. It groups validation decisions by combinations
such as decision direction, signal type, horizon, session bucket, volatility
bucket, consolidation bucket, liquidity bucket, spread bucket, depth bucket,
imbalance bucket, recent signal cluster, and direct versus inverse relation. A
row is only
accepted for shadow transfer when it has at least 300 observations, at least 30
trading sessions, observed success of at least 90%, lower 95% Wilson reliability
bound of at least 75%, and positive mean result after costs. A product-level 90%
claim additionally requires the lower bound itself to reach 90%.

`temporal-stability-report.csv` splits the validation period into chronological
blocks and repeats the threshold table inside each block. Use it to reject
candidates that look strong only in one time slice. A 90% claim should not rely
on one aggregate row when later or earlier blocks do not support the same
direction.

`temporal-stability-summary.csv` compresses the block-level report by threshold:
how many time blocks selected signals, the weakest block success rate, the
weakest lower reliability bound, and whether the threshold is temporally
supported. `decision-policy.json` can enter `shadow` only when the aggregate
threshold gate and this temporal gate both pass.

`bayesian-state-candidates.csv` is an interpretable alternative to a black-box
model. It estimates posterior success for repeatable states such as signal type,
horizon, session bucket, volatility bucket, consolidation bucket, liquidity
bucket, order-book spread/depth/imbalance buckets, recent signal cluster, and
signal combination. The matching validation rows are then triaged in
`bayesian-state-threshold-report.csv`, and their time-block stability is
summarized in `bayesian-state-temporal-summary.csv`. These files are research
evidence only: they can suggest a shadow candidate, but they do not create a
product claim without an independent holdout.

`decision-policy.json` is the machine-readable result for product transfer. If
no threshold passes the research gate, it sets `status = disabled` and
`default_action = skip`. If a threshold passes, it remains `status = shadow` and
`product_claim_allowed = false` until a later independent holdout confirms it.

## 4. Mine interpretable working patterns

```bash
python scripts/research_mine_price_patterns.py \
  --dataset var/research/datasets/signal_price_prediction.parquet \
  --run-dir var/research/runs/<run_id> \
  --top-fraction 0.10 \
  --min-n 100 \
  --accepted-min-n 300
```

The miner uses a chronological three-part flow:

1. train the classifier on the early training slice;
2. discover candidate rules on a later discovery slice;
3. apply the same rules and probability threshold to the final validation slice.

It writes:

- `probability-deciles.csv`;
- `pattern-candidates.csv`;
- `pattern-report.md`.

Accepted patterns require the final validation slice to have at least 300 rows,
at least 30 sessions, positive rate materially above the naive validation rate,
lower 95% Wilson reliability bound of at least 75%, and positive mean
cost-adjusted directional result. A rule that looks strong only on the discovery
slice is rejected.

The first research track is intentionally explainable:

- event-study baseline;
- Bayesian confirmation score;
- logistic regression;
- LightGBM classifier;
- LightGBM regressor.

Use DeepLOB, TFT, or other sequence models only after this baseline proves that
there is stable signal value. DeepLOB requires local order-book history; candles
alone are not enough for that model class.

## 5. Collect local order-book snapshots for the next holdout

Minute candles do not contain spread, depth, or order-book imbalance. The first
research runs therefore cannot honestly prove a high-confidence 90% signal. To
build a future local holdout with liquidity features, collect forward-looking
order-book snapshots during market sessions:

```bash
python scripts/research_collect_tinvest_orderbook_snapshots.py \
  --env-file .env \
  --cache-dir var/research/tinvest_orderbooks/v1 \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --depth 10 \
  --samples 240 \
  --interval-seconds 15
```

If the local network uses an intercepting certificate, prefer passing a trusted
certificate with `--ca-cert <path>`. For one-off local research diagnostics only,
`--insecure-skip-tls-verify` is available; TLS verification remains enabled by
default. The update command passes the same TLS options to both candle and
order-book collection.

The cache is partitioned as:

```text
var/research/tinvest_orderbooks/v1/ticker=SBER/date=2026-07-15.parquet
```

Persisted rows include only derived market data: best bid, best ask, mid,
spread, summed bid/ask depth, total depth, and order-book imbalance. The
manifest explicitly records that broker tokens, account identifiers, and
instrument UIDs are not persisted.

This does not recreate historical order books. It creates a reusable local
future sample so later research can test whether spread, depth, and imbalance
separate rare high-confidence cases from noisy candle-only signals.

For day-to-day accumulation, prefer the update command. It can collect continuous
order-book samples, collect extra post-signal context snapshots, refresh candles
for the latest order-book date, then run coverage/readiness checks and train
only when the holdout is ready:

```bash
python scripts/research_update_liquidity_holdout.py \
  --env-file .env \
  --collect-orderbook \
  --collect-signal-triggered-orderbook \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --orderbook-depth 10 \
  --orderbook-samples 1920 \
  --orderbook-interval-seconds 15 \
  --signal-triggered-polls 1920 \
  --signal-triggered-interval-seconds 15 \
  --signal-triggered-max-signal-age-seconds 180 \
  --require-full-prior-window \
  --output-dir var/research/liquidity_holdout/current
```

With 15-second intervals, `1920` samples cover about 8 hours. This is intentional:
prior order-book features require snapshots before signal timestamps. Snapshots
collected only after a signal is detected are useful context, but they cannot by
themselves prove prior-feature coverage without look-ahead.
Use `--preflight-only` with the same command to check whether the current time
can still fit the full prior window. With `--require-full-prior-window`, the
update command writes `status = preflight_blocked` and does not call T-Invest
when the full window cannot be completed today, unless `--force` is explicitly
provided.

The command writes `liquidity-update-result.json`. It reuses existing candle
partitions, but refreshes the latest order-book date by default so an intraday
candle partition does not stay incomplete. After every holdout run it also writes
`collection_plan/collection-plan.json` and `collection_plan/collection-plan.md`
inside the output directory, so the operator can see exactly how many covered
signals and trading sessions are still missing. To refresh specific dates
manually:

```bash
python scripts/research_update_liquidity_holdout.py \
  --env-file .env \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --refresh-candle-days 2026-07-16,2026-07-17
```

For more targeted collection, run the signal-triggered collector. It polls
recent candles, replays the detector for the current day, and stores an
order-book snapshot only when a fresh, previously unseen signal appears. The
update command above can already call it; direct use is mainly for diagnostics:

```bash
uv run --extra research python scripts/research_collect_signal_triggered_orderbooks.py \
  --env-file .env \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --depth 10 \
  --polls 540 \
  --interval-seconds 60 \
  --max-signal-age-seconds 180 \
  --insecure-skip-tls-verify
```

This is usually a better use of API calls than blind sampling when the goal is
to learn order-book state exactly around detector events. It keeps local state
in `var/research/tinvest_orderbooks/signal-triggered-state.json` so repeated
runs do not duplicate the same signal-triggered snapshot.

Before rebuilding the full dataset, check whether the order-book cache is dense
enough around replayed signal times:

```bash
python scripts/research_orderbook_signal_coverage.py \
  --cache-dir var/research/tinvest_candles/v1 \
  --orderbook-cache-dir var/research/tinvest_orderbooks/v1 \
  --max-age-seconds 5,15,30,60 \
  --output-dir var/research/orderbook_coverage
```

By default this report analyzes only candle dates that have order-book snapshots
for the same ticker. This prevents a new forward order-book holdout from being
diluted by older candle-only history. Use `--no-only-orderbook-dates` only when
you intentionally want to audit missing historical coverage.

The report writes:

- `coverage.json`;
- `coverage.csv`;
- `coverage-report.md`;
- `coverage-by-day.csv`;
- `coverage-by-day-report.md`.

The coverage report includes time diagnostics: first/last replayed signal,
first/last order-book snapshot, nearest prior snapshot age, and nearest absolute
signal/snapshot gap. If the nearest prior age is larger than 30–60 seconds, the
order-book collection did not overlap signal moments closely enough for
liquidity-aware training.

Use `coverage-by-day.csv` and `coverage-by-day-report.md` to see the exact
ticker-days that still have signals but no usable prior order-book snapshots.
This is the practical collection checklist for the 90% goal: a ticker-day with
zero covered signals cannot help prove liquidity-aware separation, even if the
global snapshot count looks large.

Then evaluate whether the holdout is ready for liquidity-aware research:

```bash
python scripts/research_holdout_readiness.py \
  --coverage-json var/research/orderbook_coverage/current/coverage.json \
  --min-covered-signals 300 \
  --min-covered-sessions 30 \
  --min-coverage 0.80 \
  --preferred-max-age-seconds 30 \
  --output-dir var/research/holdout_readiness/current
```

This writes:

- `readiness.json`;
- `readiness.csv`;
- `readiness-report.md`.

To turn readiness gaps into an operator collection plan:

```bash
python scripts/research_plan_liquidity_collection.py \
  --readiness-json var/research/holdout_readiness/current/readiness.json \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --preferred-max-age-seconds 30 \
  --target-calendar-days 45 \
  --output-dir var/research/liquidity_collection_plan/current
```

The plan writes:

- `collection-plan.json`;
- `collection-plan.md`;
- `run-liquidity-collector.sh`;
- `liquidity-collector.cron`;
- `com.investment-signals.research-liquidity-collector.plist`;
- `investment-signals-research-liquidity-collector.service`;
- `investment-signals-research-liquidity-collector.timer`.

It reports missing covered signals, missing covered trading sessions, and a
ready-to-run collection command. It also estimates covered signals per trading
session from the current holdout and uses that rate to estimate how many
additional sessions are needed to reach the missing signal count. Treat this as
an operator plan, not as evidence: it tells how to collect the future holdout
needed before any 90% claim. The plan also includes a collection-window
preflight against the research session window used by the detector. For an
8-hour target and 15-second interval, the full-window run must start near the
beginning of the active session; a late-session run is allowed for smoke testing
but should not be expected to improve prior order-book coverage enough for a
90% claim.

The `launchd` file is for macOS. The `systemd` service/timer pair is for
Ubuntu/Linux virtual machines. Both run the same generated shell script and then
refresh the 90% reports after collection. The timer is not loaded
automatically; use the `systemd user install` command printed in
`collection-plan.md` on Linux, or the `launchd load` command on macOS.

To run bounded collection cycles and refresh the 90% status after each cycle:

```bash
python scripts/research_collect_until_microstructure_ready.py \
  --run-dir var/research/runs/<run_id> \
  --output-dir var/research/liquidity_holdout/current \
  --status-output-dir var/research/signal_90_status/current \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --max-iterations 1
```

The loop calls `research_update_liquidity_holdout.py`, rebuilds
`signal-90-status.json/md`, and stops early if the microstructure gate is ready
or a product claim is somehow already allowed. Keep `--max-iterations` bounded;
use it as an operator-controlled market-session command, not as an unattended
infinite collector.

The loop also writes:

- `microstructure-collection-loop.json`;
- `microstructure-collection-loop.md`.

These files include `coverage_progress` after each iteration: covered signals,
coverage ratio, covered sessions, missing ticker-days, nearest prior
order-book age, the worst ticker-days, and `coverage_delta` versus the state
before the iteration. Use this to verify that collection is actually landing
before signal timestamps. More order-book snapshots alone are not progress if
`covered_signals_delta` stays at zero.

To run the full liquidity-aware flow safely, use the gated orchestrator:

```bash
python scripts/research_run_liquidity_holdout.py \
  --cache-dir var/research/tinvest_candles/v1 \
  --orderbook-cache-dir var/research/tinvest_orderbooks/v1 \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --max-age-seconds 5,15,30,60 \
  --preferred-max-age-seconds 30 \
  --min-covered-signals 300 \
  --min-covered-sessions 30 \
  --min-coverage 0.80 \
  --only-orderbook-dates \
  --output-dir var/research/liquidity_holdout/current
```

If the holdout is not ready, it writes `pipeline-result.json` with
`status = waiting_for_data` and does not build or train a model. If the holdout
is ready, it builds the liquidity-aware dataset, trains the models, and mines
out-of-sample rules. The dataset step also requires at least one real prior
order-book feature row; otherwise it fails before training.

For pipeline smoke tests only, force a run on sparse local snapshots:

```bash
python scripts/research_run_liquidity_holdout.py \
  --cache-dir var/research/tinvest_candles/v1 \
  --orderbook-cache-dir var/research/tinvest_orderbooks/v1 \
  --tickers SBER,GAZP,LKOH,YDEX,T \
  --max-age-seconds 5,15,30,60,300,3600 \
  --preferred-max-age-seconds 3600 \
  --output-dir var/research/liquidity_holdout/smoke \
  --force \
  --only-orderbook-dates
```

Forced smoke output proves only that the microstructure pipeline runs end to
end. It is not research evidence when coverage/readiness fails, when snapshots
are stale, or when validation has fewer than 30 trading sessions.

For a liquidity-aware 90% claim, sparse coverage is not enough. The useful
target is dense coverage within 15–30 seconds across many trading sessions, so
the model can learn whether spread, depth, and imbalance separate rare strong
signals from noise.

Practically, that means collecting snapshots from the beginning of the trading
session and keeping the collector running through active market hours. A short
late-session sample is useful only as a smoke test.

To publish one consolidated status for the 90% research goal:

```bash
python scripts/research_signal_90_status.py \
  --run-dir var/research/runs/<run_id> \
  --collection-plan var/research/liquidity_holdout/current/collection_plan/collection-plan.json \
  --output-dir var/research/signal_90_status/current
```

This writes:

- `signal-90-status.json`;
- `signal-90-status.md`.

To publish a customer-facing selection economics report for the same run:

```bash
python scripts/research_report_90_selection.py \
  --run-dir var/research/runs/<run_id> \
  --output-dir var/research/selection_90/current
```

This writes:

- `selection-90-report.json`;
- `selection-90-report.md`.

The report focuses on the core product question: how many signals remain at
each confidence threshold, how many are skipped, how many successes are still
missing to reach 90% on the retained subset, which rare rules are mathematically
still viable, and whether inverse hypotheses look stronger than direct
continuation hypotheses. It also prints the product confidence bands used by
the future interface:

- below 60% — «пропустить, недостаточно уверенности»;
- 60–75% — «слабое наблюдение»;
- 75–90% — «рабочая гипотеза»;
- 90% and above — «сильный сигнал».

Each band keeps `safe_runtime_action = skip` until its historical validation
passes the row count, trading-day, observed-success, reliability-bound, and
cost-adjusted result gates.

To test whether weak high-confidence candidates can be improved by excluding
bad contexts, run:

```bash
python scripts/research_mine_false_positive_guards.py \
  --decision-audit var/research/runs/<run_id>/decision-audit.csv \
  --output-dir var/research/runs/<run_id>
```

This writes:

- `false-positive-guards.csv`;
- `false-positive-guards.json`;
- `false-positive-guards.md`.

The guard report asks a narrower question than the rule scout: after a
high-confidence subset has already been selected, which context or pair of
contexts should be excluded to remove false positives? By default it checks the
best current confidence threshold and pairs of exclusions so the consolidated
refresh stays fast. Wider threshold sweeps or three-term exclusions can be run
manually with `--thresholds`, `--max-guard-terms`, and `--beam-width`.

A guard is still not a product rule by itself. It can only become a shadow
candidate after the remaining subset has at least 300 rows, at least 30 trading
days, observed success of at least 90%, a Wilson lower bound of at least 75%,
positive result after costs, and no excessive day/ticker concentration.

To make the remaining gap explicit, run:

```bash
python scripts/research_audit_90_gap.py \
  --run-dir var/research/runs/<run_id> \
  --output-dir var/research/gap_90/current
```

This writes:

- `gap-to-90.csv`;
- `gap-to-90.json`;
- `gap-to-90.md`.

The gap report combines confidence thresholds, rare precision-scout rules, and
false-positive guards. For each candidate it shows the missing successes to
reach 90% on the current subset, the missing rows and future success rate
needed at the 300-row gate, the lower-bound gap, blockers, and the next action.
If most candidates are marked `retire_or_redefine_rule`, more threshold tuning
is not a credible path; the research needs stronger features or a different
hypothesis.

To turn the gap audit into the next practical work queue, run:

```bash
python scripts/research_plan_90_next_actions.py \
  --gap-audit var/research/gap_90/current/gap-to-90.json \
  --feature-coverage var/research/objective_90_features/current/feature-coverage.json \
  --live-status var/research/liquidity_holdout/current/live_status/live-status.json \
  --output-dir var/research/next_actions_90/current
```

This writes:

- `next-actions-90.json`;
- `next-actions-90.md`;
- `new-feature-candidates.csv`.

The next-action plan keeps the research operational: it separates candidates
that should be retested with order-book features from candidates that should be
retired or redefined. In the current candle-only state the first action should
remain `collect_microstructure_holdout`.

To guard the scheduled collection window itself, run:

```bash
python scripts/research_collection_watchdog.py \
  --live-status var/research/liquidity_holdout/current/live_status/live-status.json \
  --schedule-status var/research/liquidity_holdout/current/collection_plan/schedule-status.json \
  --output-dir var/research/liquidity_holdout/current/watchdog
```

This writes:

- `collection-watchdog.json`;
- `collection-watchdog.md`.

Before the planned start the expected status is `waiting_for_start`. After the
start grace period, if no collector is running, no log exists, and no parquet
file was updated after the recommended start, the status becomes
`scheduled_start_missed` and the report prints the recovery command from the
generated shell script.

To track whether the collected snapshots are actually useful for the 90% gate,
run:

```bash
python scripts/research_microstructure_progress.py \
  --coverage-json var/research/liquidity_holdout/current/coverage/coverage.json \
  --readiness-json var/research/liquidity_holdout/current/readiness/readiness.json \
  --live-status var/research/liquidity_holdout/current/live_status/live-status.json \
  --watchdog var/research/liquidity_holdout/current/watchdog/collection-watchdog.json \
  --output-dir var/research/liquidity_holdout/current/progress
```

This writes:

- `microstructure-progress.json`;
- `microstructure-progress.md`.

The progress report counts covered signals and covered sessions, not just
order-book snapshots. This matters because late snapshots do not prove
pre-signal state. The gate remains closed until at least 300 signal rows and 30
trading sessions have usable prior spread/depth/imbalance values.

The status report summarizes the product policy, best confidence threshold,
best market-state rule, microstructure coverage, liquidity holdout gaps, and
missing gates. It must keep `product_claim_allowed = false` until a candidate
passes the sample-size, trading-session, reliability, cost, and
independent-holdout gates. If `decision-audit.csv` has no usable rows with
spread, depth, and imbalance buckets, the status includes
`no_microstructure_validation_rows`; this means order-book-based separation is
implemented but not yet evidenced by the current validation data.
The microstructure block uses the same minimum evidence convention as shadow
signal candidates: at least 300 usable validation rows and at least 30 trading
sessions with nearby prior order-book snapshots. It reports missing usable rows
and sessions, and repeats the recommended collection command from the liquidity
collection plan when one is available.
The liquidity block also reports `recommended_start_moscow` and
`recommended_end_moscow`. If the preflight status is
`outside_research_session` or `insufficient_remaining_session`, do not force the
collector just to produce more snapshots. Schedule the recommended command for
`recommended_start_moscow`, otherwise the snapshots will again be too late to
serve as prior order-book features for the signals.
The collection plan also writes operator schedule artifacts next to
`collection-plan.md`:

- `run-liquidity-collector.sh` — executable shell wrapper;
- `liquidity-collector.cron` — weekday cron line for 10:05 Moscow time;
- `com.investment-signals.research-liquidity-collector.plist` — macOS
  `launchd` job;
- `schedule-status.json` and `schedule-status.md` — read-only schedule check.

To check schedule readiness without starting background collection:

```bash
python scripts/research_collection_schedule_status.py \
  --collection-plan var/research/liquidity_holdout/current/collection_plan/collection-plan.json \
  --output-dir var/research/liquidity_holdout/current/collection_plan
```

The schedule status shows whether the files are valid, whether `launchd` is
loaded, and which manual action is next.

On macOS, inspect the plist first:

```bash
plutil -lint var/research/liquidity_holdout/current/collection_plan/com.investment-signals.research-liquidity-collector.plist
```

Then load it only when you intentionally want local background collection:

```bash
launchctl load var/research/liquidity_holdout/current/collection_plan/com.investment-signals.research-liquidity-collector.plist
```

The job reads `.env` through the normal research command. The schedule artifact
does not store the T-Invest token.
After the collection command finishes, the generated shell script runs
`research_refresh_90_reports.py`. The refresh step first resolves the current
run: if the liquidity-aware holdout produced a successful run under
`var/research/liquidity_holdout/current/runs/<run_id>`, reports use that run and
its liquidity-aware dataset; otherwise they safely fall back to the baseline
run. It then refreshes:

- `signal-90-status.md`;
- `goal-90-audit.md`;
- `selection-90-report.md`;
- `feature-coverage.md`;
- `schedule-status.md`;
- `live-status.md`;
- `objective-90-contract.md`;
- `daily_summary/daily-summary.md`.

This matters because collecting snapshots without refreshing the readiness
reports can leave the operator looking at stale evidence.
During the collection window, inspect the live status without starting or
stopping anything:

```bash
python scripts/research_liquidity_collection_live_status.py \
  --collection-plan var/research/liquidity_holdout/current/collection_plan/collection-plan.json \
  --schedule-status var/research/liquidity_holdout/current/collection_plan/schedule-status.json \
  --orderbook-cache-dir var/research/tinvest_orderbooks/v1 \
  --output-dir var/research/liquidity_holdout/current/live_status
```

This writes `live-status.json/md` and answers whether the schedule is still
waiting, whether a collector process is running, whether the log exists, and
whether the order-book cache has grown after the planned start.

If the daily report says `fix_collection_window_before_collecting_more`, the
collector produced snapshots but still did not create prior order-book coverage;
the next run must start at the planned session start instead of being forced
late in the day.

To audit the full «90% reliable cases» goal as a checklist instead of reading
all run artifacts manually:

```bash
python scripts/research_audit_90_goal_readiness.py \
  --run-dir var/research/runs/<run_id> \
  --signal-status var/research/signal_90_status/current/signal-90-status.json \
  --collection-plan var/research/liquidity_holdout/current/collection_plan/collection-plan.json \
  --output-dir var/research/goal_90_audit/current
```

This writes:

- `goal-90-audit.json`;
- `goal-90-audit.md`.

The audit maps the current research artifacts to the concrete product goal:
three interface decisions, safe default skip behavior, accepted confidence
threshold, minimum sample size, observed 90% success, lower reliability bound,
market-state separation, inverse-hypothesis search, order-book coverage,
liquidity holdout, and final product-claim policy. A failed audit does not mean
the tooling is broken; it means the current evidence is not strong enough to
show «ожидается рост» or «ожидается снижение» to a customer. In that state the
safe product decision remains «пропустить, недостаточно уверенности».

To audit the objective contract itself — whether the research system implements
the requested selective 90% workflow even when evidence is still missing, first
audit the concrete feature coverage requested by the objective:

```bash
uv run --extra research python scripts/research_audit_90_feature_coverage.py \
  --dataset var/research/datasets/signal_price_prediction.parquet \
  --decision-audit var/research/runs/<run_id>/decision-audit.csv \
  --threshold-report var/research/runs/<run_id>/confidence-threshold-report.csv \
  --precision-scout var/research/runs/<run_id>/precision-scout-candidates.csv \
  --output-dir var/research/objective_90_features/current
```

This writes:

- `feature-coverage.json`;
- `feature-coverage.md`.

It checks that the research data actually contains the requested market-state
feature groups: 5/15/30/60-minute pre-signal windows, recent signal series,
volume and range spikes, session position, day/instrument volatility, liquidity
or noise proxies, trend relation, consolidation, instrument-level abnormality,
inverse-hypothesis fields, and leakage guards. It also reports
`value_status`: columns can exist while order-book values are still missing.
For the current candle-only run the expected value status is
`waiting_for_microstructure_values` until enough prior snapshots produce at
least 300 signal rows with real spread/depth/imbalance values.

Then run the contract audit:

```bash
python scripts/research_audit_90_objective_contract.py \
  --selection-report var/research/selection_90/current/selection-90-report.json \
  --signal-status var/research/signal_90_status/current/signal-90-status.json \
  --goal-audit var/research/goal_90_audit/current/goal-90-audit.json \
  --schedule-status var/research/liquidity_holdout/current/collection_plan/schedule-status.json \
  --feature-coverage var/research/objective_90_features/current/feature-coverage.json \
  --gap-audit var/research/gap_90/current/gap-to-90.json \
  --output-dir var/research/objective_90_contract/current
```

This writes:

- `objective-90-contract.json`;
- `objective-90-contract.md`.

The contract audit separates two questions:

- is the mechanism implemented — skip-by-default, three decisions, confidence
  bands, threshold table, sample-size gate, reliability gate, feature coverage,
  market-state search, inverse-hypothesis search, scheduled microstructure
  collection, explicit tracking of order-book value coverage, and a measured
  gap to the 90% objective;
- is the 90% product claim proven by data.

The expected interim status is `mechanism_ready_waiting_for_evidence`: the
system is structurally aligned with the goal, but still must wait for enough
independent market evidence before it can show directional calls to a customer.

## Текущий вывод по цели 90%

Текущий запуск `fe7da78bab3fd474` проверяет 485 476 строк набора данных и
52 торговых дня в проверочной части. После добавления признаков тренда до
сигнала и движения относительно рынка продуктовый вывод всё ещё запрещён:
`product_claim_allowed = false`.

Проверенные дополнительные признаки:

- направление движения за 5, 15, 30 и 60 минут до сигнала;
- сила предыдущего тренда относительно волатильности;
- сигнал по предыдущему тренду или против него;
- событие по предыдущему тренду или против него;
- движение вместе с рынком или против рынка;
- инструмент сильнее или слабее рынка в предсигнальном окне.

Лучшее редкое правило после этих признаков:

```text
horizon_seconds=1800 |
pre_trend_strength_bucket=medium |
relative_market_bucket=down |
session_bucket=0
```

Оно даёт только 50 проверочных случаев, 64,00% успешных исходов и нижнюю 95%
границу 50,14%. Чтобы дойти до 90% на минимальных 300 случаях, этому правилу
нужно 238 успешных исходов из следующих 250, то есть 95,20% будущей успешности.
Такой кандидат нельзя показывать пользователю как торговый сигнал; его статус —
`discovery_weak`.

Итог по редким правилам:

- 430 кандидатов проверено;
- 0 кандидатов можно включать даже в режим наблюдения;
- 6 кандидатов теоретически ещё могут дойти до 90%, но требуют почти
  безошибочной будущей проверки;
- 424 кандидата уже нельзя довести до 90% на минимальных 300 случаях;
- 28 кандидатов имеют положительный средний результат после издержек, но этого
  недостаточно для продуктового вывода.

Отдельный поиск исключающих условий для ложных срабатываний также не нашёл
рабочего разделения. После добавления пар исключений лучший результат стал
выше, но всё ещё слишком слабый:

- 200 исключающих условий проверено;
- 0 условий можно включать даже в режим наблюдения;
- лучший прирост доли успеха — 3,78 процентного пункта;
- лучшая доля успеха после исключения — 42,95%, то есть далеко от 90%;
- 198 из 200 лучших исключающих правил состоят из двух условий.

Gap-аудит по порогам, редким правилам и исключающим условиям проверил
635 кандидатов:

- 0 кандидатов можно включать в режим наблюдения;
- лучшая доля успеха — 64,00%;
- 629 кандидатов уже нельзя довести до 90% на минимальных 300 случаях без
  переопределения правила;
- только 2 кандидата теоретически требуют не просто больше наблюдений, а новых
  признаков для разделения похожих случаев.

План следующих действий поэтому сводит текущий исследовательский backlog к
трём решениям:

- собрать стакан вокруг сигналов;
- перепроверить ближайшие кандидаты на признаках спреда, глубины и дисбаланса;
- списывать или переопределять правила, которые gap-аудит помечает как
  `retire_or_redefine_rule`.

Практический вывод: на текущих свечных признаках система не может надёжно
отделить редкие случаи с 90% успешностью. Сбор стакана остаётся полезным
фоновым экспериментом, но не блокирует выпуск продукта и не требует ждать
30 новых торговых дней. Основной быстрый путь использует уже доступную историю
Т‑Инвест и независимую позднюю проверку.

## Немедленная проверка расширенного рынка

Чтобы не ждать накопления новых сессий, дополнительно загружена готовая
минутная история 25 рыночных рядов: российские и зарубежные индексы, индекс
волатильности, облигационные индексы, валюты, нефть, газ, металлы, биткоин и
эфир. Кэш содержит 4 500 дневных разделов без ошибок. Итоговая выборка содержит
153 388 строк и 38 347 событий; утечек будущих данных нет.

К исходным признакам добавлены показатели, рассчитанные строго до события:

- индекс относительной силы;
- расхождение быстрых и медленных средних;
- положение цены относительно средней и разброса;
- средний истинный диапазон;
- необычность объёма;
- положение цены внутри недавнего диапазона.

Поздняя проверка охватывает 55 сессий. Лучший отбор дал 41,73% верных случаев
на 3 048 событиях; нижняя 95% граница — 39,99%. Ни один кандидат не прошёл
порог 90%.

Также проверены дополнительные гипотезы:

- возврат соотношения обыкновенных и привилегированных акций: лучший результат
  65% на 20 случаях, нижняя граница 43,29%;
- возврат между родственными индикативными рядами: 94% на 83 случаях, но лишь
  на 18 днях и с нижней границей 86,66%;
- расширенная история индикативных биткоина и эфира: 50 из 50 верных случаев,
  нижняя граница 92,86%, но только 9 дней; этот результат нельзя считать
  торговым, потому что один из рядов не торгуется;
- заранее отделённые обучение, настройка и последние 30 дней проверки для
  криптовалютной зависимости: 198 из 225 верных случаев, то есть 88%, нижняя
  граница 83,10%; строгий порог не пройден;
- торгуемые бессрочные фьючерсы на биткоин и эфир против базового индикатора:
  лучший результат 77,78% всего на 9 случаях;
- непрерывные фьючерсы на юань и индекс Московской биржи: возврат и продолжение
  расхождения не покрывают заданные издержки;
- опережение базового индикатора относительно торгуемого фьючерса на горизонтах
  1, 5 и 15 минут: лучший редкий результат 65% на 20 случаях.

Сильная зависимость индикативных криптовалютных рядов остаётся только
исследовательским наблюдением. Она не переносится на торгуемый инструмент и не
интегрируется в продукт. Продукт продолжает работать как система обнаружения и
проверки аномалий, а не выдаёт недоказанный 90‑процентный прогноз.

## Acceptance rule

Do not promote a candidate into product claims unless:

- validation has at least 30 trading sessions;
- the claimed family/horizon has at least 300 eligible rows;
- validation is chronological, not random;
- the result beats naive baseline and matched-control/event-study baseline;
- cost-adjusted directional result is positive for directional claims;
- inverse hypotheses are reported separately;
- an independent later holdout confirms the same effect.

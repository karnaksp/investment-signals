# Morning retracement discovery requirements

### US-1: Causal morning episodes

**As a** researcher
**I want** five-minute morning snapshots with path-dependent retracement labels
**So that** endpoint bias and future leakage are excluded.

1. WHEN a ticker has a prior-session close and a complete morning path
   THE SYSTEM SHALL build snapshots from 07:15 through 10:00 using only data
   observed at or before each snapshot.
2. WHEN the future path touches 25%, 50%, 75%, or 100% retracement
   THE SYSTEM SHALL record the first-passage time rather than an endpoint price.
3. WHEN target and stop are both possible inside one candle
   THE SYSTEM SHALL select the adverse outcome conservatively.

### US-2: Previous-session conditions

**As a** researcher
**I want** prior-session market and signal features
**So that** their incremental predictive value can be measured.

1. WHEN prior-session features are built
   THE SYSTEM SHALL timestamp them before the current snapshot.
2. WHEN prior signal outcomes are immature at the feature cutoff
   THE SYSTEM SHALL exclude those outcomes.
3. WHEN models are evaluated
   THE SYSTEM SHALL compare morning-only, prior-session-only, and combined
   feature sets on identical chronological partitions.

### US-3: Safety-first policy

**As a** product owner
**I want** entry, target, stop, break-even, and deadline policies evaluated
**So that** a selective candidate can prioritize avoiding a net loss.

1. WHEN a policy is simulated
   THE SYSTEM SHALL enter no earlier than the next candle and include round-trip
   costs.
2. WHEN the break-even trigger is reached
   THE SYSTEM SHALL move the protective level beyond costs by one tick.
3. WHEN a policy is selected
   THE SYSTEM SHALL rank the lower confidence bound of non-loss probability
   before target hit rate and net return.

### US-4: Honest evidence

1. WHEN data is partitioned
   THE SYSTEM SHALL use 60/20/20 chronological trading-day splits.
2. WHEN rules or probability thresholds are chosen
   THE SYSTEM SHALL use only train and validation days.
3. WHEN final evidence is reported
   THE SYSTEM SHALL report the untouched holdout result, sample size, day
   coverage, instrument concentration, doubled-slippage sensitivity, and all
   failed gates.

# Morning retracement discovery design

The production-independent research flow follows Clean Architecture:

```text
domain/morning_retracement.py
  immutable episode values and conservative path simulator
        ↑
application/morning_retracement_research.py
  causal feature/label builder and application-owned input records
        ↑
scripts/research_morning_retracement.py
  Parquet adapter, sklearn/LightGBM comparison, artifacts and CLI
```

The domain layer owns price levels and trade-state transitions. The application
layer groups candles into ticker/day episodes, computes current-morning and
previous-session features, and preserves future paths only for offline policy
evaluation. External libraries and file formats remain in the script adapter.

The script trains one model per retracement target and feature family. It uses
train days for fitting, validation days for model/threshold/policy selection,
and opens holdout only after selection. The selected policy is an offline
shadow candidate; this increment does not enable execution or product claims.

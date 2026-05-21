-- Existing deployments: add OHLC-style columns for slot baselines (return / range).
-- New installs get these from 004_historical_baseline.sql; ALTER is idempotent.

ALTER TABLE signal_engine.trade_slot_daily
ADD COLUMN IF NOT EXISTS open_px Float64 DEFAULT 0 AFTER sum_pv;

ALTER TABLE signal_engine.trade_slot_daily
ADD COLUMN IF NOT EXISTS max_px Float64 DEFAULT 0 AFTER open_px;

ALTER TABLE signal_engine.trade_slot_daily
ADD COLUMN IF NOT EXISTS min_px Float64 DEFAULT 0 AFTER max_px;

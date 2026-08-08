-- TOAST owns most snapshot pages, so its worker needs an explicit budget of
-- its own.  The parent table's cost settings do not propagate to TOAST.
ALTER TABLE detector_state_snapshots SET (
    toast.autovacuum_vacuum_cost_delay = 20,
    toast.autovacuum_vacuum_cost_limit = 200
);

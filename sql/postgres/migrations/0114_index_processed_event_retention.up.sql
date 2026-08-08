-- BRIN is compact, but it cannot provide the ordered cursor needed by the
-- bounded delete. Once dead rows accumulate, every batch starts by scanning
-- the same old heap pages and a 100k-row maintenance batch can take minutes.
-- The retained window is only three days, so a timestamp B-tree remains
-- bounded while making each purge batch an index-ordered range scan.
CREATE INDEX IF NOT EXISTS processed_events_retention_idx
    ON processed_events (processed_at);

DROP INDEX IF EXISTS processed_events_processed_at_brin_idx;

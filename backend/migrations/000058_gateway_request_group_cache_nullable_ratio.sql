-- Version 6 retains successful real downstream requests whose input-token
-- denominator is missing or zero. The application observes the existing
-- lower target_version and rebuilds the ledger in bounded background batches;
-- this schema migration must not clear production history synchronously.
ALTER TABLE public.gateway_request_group_cache_aggregates
    ALTER COLUMN version SET DEFAULT 6;

ALTER TABLE public.gateway_request_group_cache_backfill_state
    ALTER COLUMN target_version SET DEFAULT 6;

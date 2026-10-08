-- Add the per-request marker without wrapping the live log table in a large
-- transaction. The supporting index is built concurrently by migration 023.
ALTER TABLE public.gateway_request_logs
    ADD COLUMN IF NOT EXISTS group_cache_aggregate_version bigint NOT NULL DEFAULT 0;

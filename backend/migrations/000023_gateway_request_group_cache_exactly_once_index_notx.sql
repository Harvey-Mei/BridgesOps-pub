-- The marker index is intentionally concurrent: gateway_request_logs is a
-- hot table and must remain writable while the index is built.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gateway_logs_group_cache_backfill
    ON public.gateway_request_logs (group_cache_aggregate_version, id);

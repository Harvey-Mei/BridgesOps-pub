-- The cache-price reader walks newest buckets for the bounded input-token
-- window and performs one newest-use lookup for each current route. Build the
-- covering expression index concurrently so applying it does not block
-- request-log writes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gateway_request_group_cache_routing_window
    ON public.gateway_request_group_cache_aggregates (
        version,
        channel_id,
        (LOWER(BTRIM(group_name))),
        bucket_start DESC,
        last_normal_use_at DESC
    )
    INCLUDE (prompt_tokens, cache_read_tokens);

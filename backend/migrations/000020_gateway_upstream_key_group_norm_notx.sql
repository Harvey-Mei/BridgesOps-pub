-- Concurrent normalized group lookup index used by probe-state cleanup.
CREATE INDEX CONCURRENTLY IF NOT EXISTS gateway_upstream_keys_channel_group_norm_idx
    ON public.gateway_upstream_keys (channel_id, lower(btrim(group_name)));

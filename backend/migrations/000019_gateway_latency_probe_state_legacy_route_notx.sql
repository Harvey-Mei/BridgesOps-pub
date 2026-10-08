-- Concurrent index for legacy unscoped probe-state compatibility lookups.
CREATE INDEX CONCURRENTLY IF NOT EXISTS gateway_latency_probe_states_legacy_route_idx
    ON public.gateway_latency_probe_states (channel_id, lower(btrim(group_name)))
    WHERE upstream_group_id = 0 AND presence_generation = 0;

-- Concurrent index for bounded historical probe-state cleanup.
CREATE INDEX CONCURRENTLY IF NOT EXISTS gateway_latency_probe_states_retention_idx
    ON public.gateway_latency_probe_states (updated_at)
    WHERE upstream_group_id <> 0 OR presence_generation <> 0;

-- Concurrent normalized model lookup index used by probe-state cleanup.
CREATE INDEX CONCURRENTLY IF NOT EXISTS rate_snapshots_channel_model_norm_idx
    ON public.rate_snapshots (channel_id, lower(btrim(model_name)));

-- Version 5 narrows the group-cache ledger to successful real downstream
-- traffic and persists the exact last normal-use timestamp used for expiry.
ALTER TABLE public.gateway_request_group_cache_aggregates
    ADD COLUMN IF NOT EXISTS last_normal_use_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE public.gateway_request_group_cache_aggregates
    ALTER COLUMN version SET DEFAULT 5;

ALTER TABLE public.gateway_request_group_cache_backfill_state
    ALTER COLUMN target_version SET DEFAULT 5;

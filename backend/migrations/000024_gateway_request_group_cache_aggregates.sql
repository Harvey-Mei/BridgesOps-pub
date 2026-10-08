-- Group cache metrics live in their own ledger. This migration is deliberately
-- idempotent so it can repair databases that already applied the original
-- version-016 table before model_name and the exactly-once marker existed.
CREATE TABLE IF NOT EXISTS public.gateway_request_group_cache_aggregates (
    bucket_start timestamp with time zone NOT NULL,
    downstream_key_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    channel_name_snapshot character varying(256) NOT NULL DEFAULT ''::character varying,
    group_name character varying(128) NOT NULL DEFAULT ''::character varying,
    model_name character varying(256) NOT NULL DEFAULT ''::character varying,
    cache_read_tokens bigint NOT NULL DEFAULT 0,
    cache_creation_tokens bigint NOT NULL DEFAULT 0,
    prompt_tokens bigint NOT NULL DEFAULT 0,
    valid_requests bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 4,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.gateway_request_group_cache_backfill_state (
    id smallint PRIMARY KEY,
    last_request_id bigint NOT NULL DEFAULT 0,
    target_version bigint NOT NULL DEFAULT 4,
    completed boolean NOT NULL DEFAULT false,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_request_group_cache_backfill_state_singleton CHECK (id = 1)
);

INSERT INTO public.gateway_request_group_cache_backfill_state (id, target_version, completed)
VALUES (1, 4, false)
ON CONFLICT (id) DO NOTHING;

DO $$
DECLARE
    legacy_group_ledger boolean;
    legacy_main_aggregate boolean;
BEGIN
    SELECT NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'gateway_request_group_cache_aggregates'
           AND column_name = 'model_name'
    ) INTO legacy_group_ledger;

    SELECT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'gateway_request_aggregates'
           AND column_name = 'group_name'
    ) INTO legacy_main_aggregate;

    ALTER TABLE public.gateway_request_group_cache_aggregates
        ADD COLUMN IF NOT EXISTS model_name character varying(256) NOT NULL DEFAULT ''::character varying;

    IF legacy_group_ledger THEN
        -- Version-016 rows were keyed without model_name. They cannot be
        -- safely split after the fact, so rebuild them from raw logs.
        DELETE FROM public.gateway_request_group_cache_aggregates;
        UPDATE public.gateway_request_logs
           SET group_cache_aggregate_version = 0
         WHERE group_cache_aggregate_version <= 4;
        UPDATE public.gateway_request_group_cache_backfill_state
           SET last_request_id = 0,
               target_version = 4,
               completed = false,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = 1;
    END IF;

    IF legacy_main_aggregate THEN
        -- The old aggregate key included group_name. Clear those derived
        -- rows and replay raw logs into the current eight-column key.
        DELETE FROM public.gateway_request_aggregates;
        UPDATE public.gateway_request_logs
           SET aggregate_version = 0
         WHERE aggregate_version <= 4;
        UPDATE public.gateway_request_group_cache_backfill_state
           SET last_request_id = 0,
               target_version = 4,
               completed = false,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = 1;

        EXECUTE 'DROP INDEX IF EXISTS public.idx_gateway_request_aggregate_key';
        EXECUTE 'CREATE UNIQUE INDEX idx_gateway_request_aggregate_key
                   ON public.gateway_request_aggregates (
                       bucket_start,
                       downstream_key_id,
                       channel_id,
                       channel_name_snapshot,
                       model_name,
                       cost_source,
                       route_reason,
                       result
                   )';
    END IF;

    IF legacy_group_ledger THEN
        EXECUTE 'DROP INDEX IF EXISTS public.idx_gateway_request_group_cache_aggregates_key';
        EXECUTE 'DROP INDEX IF EXISTS public.idx_gateway_request_group_cache_aggregates_filter';
        EXECUTE 'CREATE UNIQUE INDEX idx_gateway_request_group_cache_aggregates_key
                   ON public.gateway_request_group_cache_aggregates (
                       bucket_start,
                       downstream_key_id,
                       channel_id,
                       channel_name_snapshot,
                       group_name,
                       model_name
                   )';
        EXECUTE 'CREATE INDEX idx_gateway_request_group_cache_aggregates_filter
                   ON public.gateway_request_group_cache_aggregates (
                       downstream_key_id,
                       channel_id,
                       bucket_start,
                       group_name,
                       model_name
                   )';
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gateway_request_aggregate_key
    ON public.gateway_request_aggregates (
        bucket_start,
        downstream_key_id,
        channel_id,
        channel_name_snapshot,
        model_name,
        cost_source,
        route_reason,
        result
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_gateway_request_group_cache_aggregates_key
    ON public.gateway_request_group_cache_aggregates (
        bucket_start,
        downstream_key_id,
        channel_id,
        channel_name_snapshot,
        group_name,
        model_name
    );

CREATE INDEX IF NOT EXISTS idx_gateway_request_group_cache_aggregates_filter
    ON public.gateway_request_group_cache_aggregates (
        downstream_key_id,
        channel_id,
        bucket_start,
        group_name,
        model_name
    );

-- Schema v2 keeps provider-published short/long prices and the exact input
-- and output multipliers separate. The selected tier is evaluated only after
-- final usage is known; schema-v1 facts remain byte-for-byte compatible.
ALTER TABLE public.gateway_request_price_facts
    ADD COLUMN long_context_threshold_tokens bigint,
    ADD COLUMN long_context_input_unit_price double precision,
    ADD COLUMN long_context_output_unit_price double precision,
    ADD COLUMN long_context_cache_read_unit_price double precision,
    ADD COLUMN long_context_cache_write_unit_price double precision,
    ADD COLUMN group_ratio double precision,
    ADD COLUMN completion_ratio double precision;

ALTER TABLE public.gateway_request_logs
    ADD COLUMN price_long_context_threshold_tokens bigint,
    ADD COLUMN price_long_context_input_unit double precision,
    ADD COLUMN price_long_context_output_unit double precision,
    ADD COLUMN price_long_context_cache_read_unit double precision,
    ADD COLUMN price_long_context_cache_write_unit double precision;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN price_long_context_threshold_tokens bigint,
    ADD COLUMN price_long_context_input_unit double precision,
    ADD COLUMN price_long_context_output_unit double precision,
    ADD COLUMN price_long_context_cache_read_unit double precision,
    ADD COLUMN price_long_context_cache_write_unit double precision;

ALTER TABLE public.gateway_request_price_facts
    DROP CONSTRAINT chk_gateway_price_fact_versions_positive,
    DROP CONSTRAINT chk_gateway_price_fact_prices_finite,
    DROP CONSTRAINT chk_gateway_price_fact_unit_shape,
    ADD CONSTRAINT chk_gateway_price_fact_versions_positive
        CHECK (fact_version > 0 AND schema_version IN (1, 2)),
    ADD CONSTRAINT chk_gateway_price_fact_prices_finite
        CHECK (
            (input_unit_price IS NULL OR (input_unit_price >= 0 AND input_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (output_unit_price IS NULL OR (output_unit_price >= 0 AND output_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (cache_read_unit_price IS NULL OR (cache_read_unit_price >= 0 AND cache_read_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (cache_write_unit_price IS NULL OR (cache_write_unit_price >= 0 AND cache_write_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (long_context_input_unit_price IS NULL OR (long_context_input_unit_price >= 0 AND long_context_input_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (long_context_output_unit_price IS NULL OR (long_context_output_unit_price >= 0 AND long_context_output_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (long_context_cache_read_unit_price IS NULL OR (long_context_cache_read_unit_price >= 0 AND long_context_cache_read_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (long_context_cache_write_unit_price IS NULL OR (long_context_cache_write_unit_price >= 0 AND long_context_cache_write_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (group_ratio IS NULL OR (group_ratio >= 0 AND group_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (completion_ratio IS NULL OR (completion_ratio >= 0 AND completion_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (operation_unit_price IS NULL OR (operation_unit_price >= 0 AND operation_unit_price::text NOT IN ('NaN', 'Infinity', '-Infinity')))
        ),
    ADD CONSTRAINT chk_gateway_price_fact_unit_shape
        CHECK (
            (
                schema_version = 1
                AND long_context_threshold_tokens IS NULL
                AND long_context_input_unit_price IS NULL
                AND long_context_output_unit_price IS NULL
                AND long_context_cache_read_unit_price IS NULL
                AND long_context_cache_write_unit_price IS NULL
                AND group_ratio IS NULL
                AND completion_ratio IS NULL
                AND (
                    (operation = 'text_generation' AND protocol IN ('openai_chat_completions', 'openai_completions', 'openai_responses', 'anthropic_messages')
                     AND unit = 'per_million_tokens' AND operation_unit_price IS NULL
                     AND (input_unit_price IS NOT NULL OR output_unit_price IS NOT NULL OR cache_read_unit_price IS NOT NULL OR cache_write_unit_price IS NOT NULL))
                    OR
                    (operation = 'embedding' AND protocol = 'openai_embeddings' AND unit = 'per_million_tokens'
                     AND input_unit_price IS NOT NULL AND output_unit_price IS NULL AND cache_read_unit_price IS NULL
                     AND cache_write_unit_price IS NULL AND operation_unit_price IS NULL)
                    OR
                    (operation = 'image_generation' AND protocol IN ('openai_images', 'openai_responses') AND unit = 'per_operation'
                     AND operation_unit_price IS NOT NULL AND input_unit_price IS NULL AND output_unit_price IS NULL
                     AND cache_read_unit_price IS NULL AND cache_write_unit_price IS NULL)
                )
            )
            OR
            (
                schema_version = 2
                AND operation = 'text_generation'
                AND protocol IN ('openai_chat_completions', 'openai_completions', 'openai_responses', 'anthropic_messages')
                AND unit = 'per_million_tokens'
                AND operation_unit_price IS NULL
                AND input_unit_price IS NOT NULL
                AND output_unit_price IS NOT NULL
                AND long_context_threshold_tokens > 0
                AND long_context_threshold_tokens < 9223372036854775807
                AND long_context_input_unit_price IS NOT NULL
                AND long_context_output_unit_price IS NOT NULL
                AND group_ratio IS NOT NULL
                AND completion_ratio IS NOT NULL
            )
        );

ALTER TABLE public.gateway_request_logs
    DROP CONSTRAINT chk_gateway_request_logs_price_values,
    DROP CONSTRAINT chk_gateway_request_logs_price_fact_shape,
    ADD CONSTRAINT chk_gateway_request_logs_price_values
        CHECK (
            (price_input_unit IS NULL OR (price_input_unit >= 0 AND price_input_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_output_unit IS NULL OR (price_output_unit >= 0 AND price_output_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_cache_read_unit IS NULL OR (price_cache_read_unit >= 0 AND price_cache_read_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_cache_write_unit IS NULL OR (price_cache_write_unit >= 0 AND price_cache_write_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_input_unit IS NULL OR (price_long_context_input_unit >= 0 AND price_long_context_input_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_output_unit IS NULL OR (price_long_context_output_unit >= 0 AND price_long_context_output_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_cache_read_unit IS NULL OR (price_long_context_cache_read_unit >= 0 AND price_long_context_cache_read_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_cache_write_unit IS NULL OR (price_long_context_cache_write_unit >= 0 AND price_long_context_cache_write_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_operation_unit IS NULL OR (price_operation_unit >= 0 AND price_operation_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (operation_units IS NULL OR operation_units >= 0)
            AND usage_operation_present = (operation_units IS NOT NULL)
            AND (price_observed_at IS NULL OR isfinite(price_observed_at))
            AND (price_valid_from IS NULL OR isfinite(price_valid_from))
            AND (price_valid_until IS NULL OR (price_valid_from IS NOT NULL AND isfinite(price_valid_until) AND price_valid_until > price_valid_from))
        ) NOT VALID,
    ADD CONSTRAINT chk_gateway_request_logs_price_fact_shape
        CHECK (
            (
                price_fact_id IS NULL
                AND price_cost_unit_epoch = 0 AND price_fact_version = 0 AND price_fact_schema_version = 0 AND price_fact_checksum = ''
                AND price_input_unit IS NULL AND price_output_unit IS NULL AND price_cache_read_unit IS NULL AND price_cache_write_unit IS NULL
                AND price_long_context_threshold_tokens IS NULL AND price_long_context_input_unit IS NULL AND price_long_context_output_unit IS NULL
                AND price_long_context_cache_read_unit IS NULL AND price_long_context_cache_write_unit IS NULL AND price_operation_unit IS NULL
                AND price_currency = '' AND price_unit = '' AND price_source = ''
                AND price_observed_at IS NULL AND price_valid_from IS NULL AND price_valid_until IS NULL
                AND (
                    (channel_identity_revision = 0 AND upstream_group_id = 0 AND upstream_group_generation = 0
                     AND upstream_key_local_id = 0 AND upstream_key_generation = 0
                     AND price_canonical_group_name = '' AND price_model_name = '')
                    OR
                    (channel_id IS NOT NULL AND channel_id > 0 AND channel_identity_revision > 0 AND upstream_group_id > 0
                     AND upstream_group_generation > 0 AND upstream_key_local_id > 0 AND upstream_key_generation > 0
                     AND price_canonical_group_name = lower(btrim(price_canonical_group_name)) AND price_canonical_group_name <> ''
                     AND price_model_name = btrim(price_model_name) AND price_model_name <> '' AND price_operation <> '' AND price_protocol <> '')
                )
            )
            OR
            (
                price_fact_id IS NOT NULL AND price_fact_id > 0 AND channel_id IS NOT NULL AND channel_id > 0
                AND channel_identity_revision > 0 AND upstream_group_id > 0 AND upstream_group_generation > 0
                AND upstream_key_local_id > 0 AND upstream_key_generation > 0 AND price_cost_unit_epoch > 0
                AND price_fact_version > 0 AND price_fact_schema_version IN (1, 2) AND price_fact_checksum ~ '^[0-9a-f]{64}$'
                AND price_canonical_group_name = lower(btrim(price_canonical_group_name)) AND price_canonical_group_name <> ''
                AND price_model_name = btrim(price_model_name) AND price_model_name <> ''
                AND price_operation = lower(btrim(price_operation)) AND price_operation <> ''
                AND price_protocol = lower(btrim(price_protocol)) AND price_protocol <> ''
                AND price_currency ~ '^[A-Z]{3}$' AND price_unit IN ('per_million_tokens', 'per_operation')
                AND price_source ~ '^[a-z0-9][a-z0-9_.-]{0,63}$' AND price_observed_at IS NOT NULL
                AND price_valid_from IS NOT NULL AND price_observed_at <= price_valid_from
                AND (
                    (price_fact_schema_version = 1
                     AND price_long_context_threshold_tokens IS NULL AND price_long_context_input_unit IS NULL
                     AND price_long_context_output_unit IS NULL AND price_long_context_cache_read_unit IS NULL
                     AND price_long_context_cache_write_unit IS NULL
                     AND (
                        (price_operation = 'text_generation' AND price_protocol IN ('openai_chat_completions', 'openai_completions', 'openai_responses', 'anthropic_messages')
                         AND price_unit = 'per_million_tokens' AND price_operation_unit IS NULL
                         AND (price_input_unit IS NOT NULL OR price_output_unit IS NOT NULL OR price_cache_read_unit IS NOT NULL OR price_cache_write_unit IS NOT NULL))
                        OR
                        (price_operation = 'embedding' AND price_protocol = 'openai_embeddings' AND price_unit = 'per_million_tokens'
                         AND price_input_unit IS NOT NULL AND price_output_unit IS NULL AND price_cache_read_unit IS NULL
                         AND price_cache_write_unit IS NULL AND price_operation_unit IS NULL)
                        OR
                        (price_operation = 'image_generation' AND price_protocol IN ('openai_images', 'openai_responses') AND price_unit = 'per_operation'
                         AND price_operation_unit IS NOT NULL AND price_input_unit IS NULL AND price_output_unit IS NULL
                         AND price_cache_read_unit IS NULL AND price_cache_write_unit IS NULL)
                     ))
                    OR
                    (price_fact_schema_version = 2 AND price_operation = 'text_generation'
                     AND price_protocol IN ('openai_chat_completions', 'openai_completions', 'openai_responses', 'anthropic_messages')
                     AND price_unit = 'per_million_tokens' AND price_operation_unit IS NULL
                     AND price_input_unit IS NOT NULL AND price_output_unit IS NOT NULL
                     AND price_long_context_threshold_tokens > 0
                     AND price_long_context_threshold_tokens < 9223372036854775807
                     AND price_long_context_input_unit IS NOT NULL AND price_long_context_output_unit IS NOT NULL
                     AND group_ratio >= 0 AND group_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')
                     AND completion_ratio >= 0 AND completion_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity'))
                )
            )
        ) NOT VALID;

CREATE OR REPLACE FUNCTION public.reject_gateway_request_log_price_snapshot_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.channel_id IS DISTINCT FROM OLD.channel_id
       OR NEW.channel_identity_revision IS DISTINCT FROM OLD.channel_identity_revision
       OR NEW.upstream_group_id IS DISTINCT FROM OLD.upstream_group_id
       OR NEW.upstream_group_generation IS DISTINCT FROM OLD.upstream_group_generation
       OR NEW.upstream_key_local_id IS DISTINCT FROM OLD.upstream_key_local_id
       OR NEW.upstream_key_generation IS DISTINCT FROM OLD.upstream_key_generation
       OR NEW.price_fact_id IS DISTINCT FROM OLD.price_fact_id
       OR NEW.price_cost_unit_epoch IS DISTINCT FROM OLD.price_cost_unit_epoch
       OR NEW.price_fact_version IS DISTINCT FROM OLD.price_fact_version
       OR NEW.price_fact_schema_version IS DISTINCT FROM OLD.price_fact_schema_version
       OR NEW.price_fact_checksum IS DISTINCT FROM OLD.price_fact_checksum
       OR NEW.price_canonical_group_name IS DISTINCT FROM OLD.price_canonical_group_name
       OR NEW.price_model_name IS DISTINCT FROM OLD.price_model_name
       OR NEW.price_operation IS DISTINCT FROM OLD.price_operation
       OR NEW.price_protocol IS DISTINCT FROM OLD.price_protocol
       OR NEW.price_input_unit IS DISTINCT FROM OLD.price_input_unit
       OR NEW.price_output_unit IS DISTINCT FROM OLD.price_output_unit
       OR NEW.price_cache_read_unit IS DISTINCT FROM OLD.price_cache_read_unit
       OR NEW.price_cache_write_unit IS DISTINCT FROM OLD.price_cache_write_unit
       OR NEW.price_long_context_threshold_tokens IS DISTINCT FROM OLD.price_long_context_threshold_tokens
       OR NEW.price_long_context_input_unit IS DISTINCT FROM OLD.price_long_context_input_unit
       OR NEW.price_long_context_output_unit IS DISTINCT FROM OLD.price_long_context_output_unit
       OR NEW.price_long_context_cache_read_unit IS DISTINCT FROM OLD.price_long_context_cache_read_unit
       OR NEW.price_long_context_cache_write_unit IS DISTINCT FROM OLD.price_long_context_cache_write_unit
       OR NEW.price_operation_unit IS DISTINCT FROM OLD.price_operation_unit
       OR NEW.price_currency IS DISTINCT FROM OLD.price_currency
       OR NEW.price_unit IS DISTINCT FROM OLD.price_unit
       OR NEW.price_source IS DISTINCT FROM OLD.price_source
       OR NEW.price_observed_at IS DISTINCT FROM OLD.price_observed_at
       OR NEW.price_valid_from IS DISTINCT FROM OLD.price_valid_from
       OR NEW.price_valid_until IS DISTINCT FROM OLD.price_valid_until
       OR NEW.usage_schema IS DISTINCT FROM OLD.usage_schema
       OR NEW.usage_schema_version IS DISTINCT FROM OLD.usage_schema_version
       OR (
            OLD.price_fact_id IS NOT NULL
            AND (
                NEW.group_ratio IS DISTINCT FROM OLD.group_ratio
                OR NEW.completion_ratio IS DISTINCT FROM OLD.completion_ratio
                OR NEW.raw_group_ratio IS DISTINCT FROM OLD.raw_group_ratio
                OR NEW.raw_completion_ratio IS DISTINCT FROM OLD.raw_completion_ratio
                OR NEW.funding_factor IS DISTINCT FROM OLD.funding_factor
                OR NEW.cost_basis_version IS DISTINCT FROM OLD.cost_basis_version
                OR NEW.cost_basis_source IS DISTINCT FROM OLD.cost_basis_source
            )
       ) THEN
        RAISE EXCEPTION 'gateway request log price snapshot is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER trg_gateway_request_logs_price_snapshot_immutable
    ON public.gateway_request_logs;
CREATE TRIGGER trg_gateway_request_logs_price_snapshot_immutable
BEFORE UPDATE OF
    channel_id, channel_identity_revision, upstream_group_id, upstream_group_generation,
    upstream_key_local_id, upstream_key_generation, price_fact_id, price_cost_unit_epoch,
    price_fact_version, price_fact_schema_version, price_fact_checksum, price_canonical_group_name,
    price_model_name, price_operation, price_protocol, price_input_unit, price_output_unit,
    price_cache_read_unit, price_cache_write_unit, price_long_context_threshold_tokens,
    price_long_context_input_unit, price_long_context_output_unit,
    price_long_context_cache_read_unit, price_long_context_cache_write_unit, price_operation_unit,
    price_currency, price_unit, price_source, price_observed_at, price_valid_from, price_valid_until,
    usage_schema, usage_schema_version, group_ratio, completion_ratio, raw_group_ratio,
    raw_completion_ratio, funding_factor, cost_basis_version, cost_basis_source
ON public.gateway_request_logs
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_log_price_snapshot_update();

CREATE OR REPLACE FUNCTION public.reject_gateway_request_dispatch_price_snapshot_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.channel_id IS DISTINCT FROM OLD.channel_id
       OR NEW.channel_identity_revision IS DISTINCT FROM OLD.channel_identity_revision
       OR NEW.upstream_group_id IS DISTINCT FROM OLD.upstream_group_id
       OR NEW.upstream_group_generation IS DISTINCT FROM OLD.upstream_group_generation
       OR NEW.upstream_key_local_id IS DISTINCT FROM OLD.upstream_key_local_id
       OR NEW.upstream_key_generation IS DISTINCT FROM OLD.upstream_key_generation
       OR NEW.price_fact_id IS DISTINCT FROM OLD.price_fact_id
       OR NEW.price_cost_unit_epoch IS DISTINCT FROM OLD.price_cost_unit_epoch
       OR NEW.price_fact_version IS DISTINCT FROM OLD.price_fact_version
       OR NEW.price_fact_schema_version IS DISTINCT FROM OLD.price_fact_schema_version
       OR NEW.price_fact_checksum IS DISTINCT FROM OLD.price_fact_checksum
       OR NEW.price_canonical_group_name IS DISTINCT FROM OLD.price_canonical_group_name
       OR NEW.price_model_name IS DISTINCT FROM OLD.price_model_name
       OR NEW.price_operation IS DISTINCT FROM OLD.price_operation
       OR NEW.price_protocol IS DISTINCT FROM OLD.price_protocol
       OR NEW.price_input_unit IS DISTINCT FROM OLD.price_input_unit
       OR NEW.price_output_unit IS DISTINCT FROM OLD.price_output_unit
       OR NEW.price_cache_read_unit IS DISTINCT FROM OLD.price_cache_read_unit
       OR NEW.price_cache_write_unit IS DISTINCT FROM OLD.price_cache_write_unit
       OR NEW.price_long_context_threshold_tokens IS DISTINCT FROM OLD.price_long_context_threshold_tokens
       OR NEW.price_long_context_input_unit IS DISTINCT FROM OLD.price_long_context_input_unit
       OR NEW.price_long_context_output_unit IS DISTINCT FROM OLD.price_long_context_output_unit
       OR NEW.price_long_context_cache_read_unit IS DISTINCT FROM OLD.price_long_context_cache_read_unit
       OR NEW.price_long_context_cache_write_unit IS DISTINCT FROM OLD.price_long_context_cache_write_unit
       OR NEW.price_operation_unit IS DISTINCT FROM OLD.price_operation_unit
       OR NEW.price_currency IS DISTINCT FROM OLD.price_currency
       OR NEW.price_unit IS DISTINCT FROM OLD.price_unit
       OR NEW.price_source IS DISTINCT FROM OLD.price_source
       OR NEW.price_observed_at IS DISTINCT FROM OLD.price_observed_at
       OR NEW.price_valid_from IS DISTINCT FROM OLD.price_valid_from
       OR NEW.price_valid_until IS DISTINCT FROM OLD.price_valid_until
       OR NEW.usage_schema IS DISTINCT FROM OLD.usage_schema
       OR NEW.usage_schema_version IS DISTINCT FROM OLD.usage_schema_version
       OR NEW.cost_unavailable_reason IS DISTINCT FROM OLD.cost_unavailable_reason THEN
        RAISE EXCEPTION 'gateway request dispatch price snapshot is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER trg_gateway_request_dispatch_price_snapshot_immutable
    ON public.gateway_request_dispatch_audits;
CREATE TRIGGER trg_gateway_request_dispatch_price_snapshot_immutable
BEFORE UPDATE OF
    channel_id, channel_identity_revision, upstream_group_id, upstream_group_generation,
    upstream_key_local_id, upstream_key_generation, price_fact_id, price_cost_unit_epoch,
    price_fact_version, price_fact_schema_version, price_fact_checksum, price_canonical_group_name,
    price_model_name, price_operation, price_protocol, price_input_unit, price_output_unit,
    price_cache_read_unit, price_cache_write_unit, price_long_context_threshold_tokens,
    price_long_context_input_unit, price_long_context_output_unit,
    price_long_context_cache_read_unit, price_long_context_cache_write_unit, price_operation_unit,
    price_currency, price_unit, price_source, price_observed_at, price_valid_from, price_valid_until,
    usage_schema, usage_schema_version, cost_unavailable_reason
ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_dispatch_price_snapshot_update();

CREATE OR REPLACE FUNCTION public.enforce_gateway_request_price_fact_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.price_fact_id IS NOT NULL
       AND NOT EXISTS (
            SELECT 1
            FROM public.gateway_request_price_facts AS fact
            WHERE fact.id IS NOT DISTINCT FROM NEW.price_fact_id
              AND fact.channel_id IS NOT DISTINCT FROM NEW.channel_id
              AND fact.channel_account_identity_revision IS NOT DISTINCT FROM NEW.channel_identity_revision
              AND fact.cost_unit_epoch IS NOT DISTINCT FROM NEW.price_cost_unit_epoch
              AND fact.upstream_group_id IS NOT DISTINCT FROM NEW.upstream_group_id
              AND fact.canonical_group_name IS NOT DISTINCT FROM NEW.price_canonical_group_name
              AND fact.upstream_group_presence_generation IS NOT DISTINCT FROM NEW.upstream_group_generation
              AND fact.gateway_upstream_key_id IS NOT DISTINCT FROM NEW.upstream_key_local_id
              AND fact.upstream_key_generation IS NOT DISTINCT FROM NEW.upstream_key_generation
              AND fact.model_name IS NOT DISTINCT FROM NEW.price_model_name
              AND fact.operation IS NOT DISTINCT FROM NEW.price_operation
              AND fact.protocol IS NOT DISTINCT FROM NEW.price_protocol
              AND fact.input_unit_price IS NOT DISTINCT FROM NEW.price_input_unit
              AND fact.output_unit_price IS NOT DISTINCT FROM NEW.price_output_unit
              AND fact.cache_read_unit_price IS NOT DISTINCT FROM NEW.price_cache_read_unit
              AND fact.cache_write_unit_price IS NOT DISTINCT FROM NEW.price_cache_write_unit
              AND fact.long_context_threshold_tokens IS NOT DISTINCT FROM NEW.price_long_context_threshold_tokens
              AND fact.long_context_input_unit_price IS NOT DISTINCT FROM NEW.price_long_context_input_unit
              AND fact.long_context_output_unit_price IS NOT DISTINCT FROM NEW.price_long_context_output_unit
              AND fact.long_context_cache_read_unit_price IS NOT DISTINCT FROM NEW.price_long_context_cache_read_unit
              AND fact.long_context_cache_write_unit_price IS NOT DISTINCT FROM NEW.price_long_context_cache_write_unit
              AND fact.operation_unit_price IS NOT DISTINCT FROM NEW.price_operation_unit
              AND fact.unit IS NOT DISTINCT FROM NEW.price_unit
              AND fact.currency IS NOT DISTINCT FROM NEW.price_currency
              AND fact.source IS NOT DISTINCT FROM NEW.price_source
              AND fact.observed_at IS NOT DISTINCT FROM NEW.price_observed_at
              AND fact.valid_from IS NOT DISTINCT FROM NEW.price_valid_from
              AND fact.valid_until IS NOT DISTINCT FROM NEW.price_valid_until
              AND fact.fact_version IS NOT DISTINCT FROM NEW.price_fact_version
              AND fact.schema_version IS NOT DISTINCT FROM NEW.price_fact_schema_version
              AND fact.checksum IS NOT DISTINCT FROM NEW.price_fact_checksum
              AND (
                  fact.schema_version = 1
                  OR (
                      fact.group_ratio IS NOT DISTINCT FROM NEW.group_ratio
                      AND fact.completion_ratio IS NOT DISTINCT FROM NEW.completion_ratio
                  )
              )
       ) THEN
        RAISE EXCEPTION 'gateway request price snapshot does not match referenced price fact'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_fact_snapshot_consistency';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.enforce_gateway_request_dispatch_final_price_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.request_log_id IS NOT NULL THEN
        PERFORM 1
        FROM public.gateway_request_logs AS request_log
        WHERE request_log.id IS NOT DISTINCT FROM NEW.request_log_id
          AND request_log.channel_id IS NOT DISTINCT FROM NEW.channel_id
          AND request_log.channel_identity_revision IS NOT DISTINCT FROM NEW.channel_identity_revision
          AND request_log.price_cost_unit_epoch IS NOT DISTINCT FROM NEW.price_cost_unit_epoch
          AND request_log.upstream_group_id IS NOT DISTINCT FROM NEW.upstream_group_id
          AND request_log.upstream_group_generation IS NOT DISTINCT FROM NEW.upstream_group_generation
          AND request_log.upstream_key_local_id IS NOT DISTINCT FROM NEW.upstream_key_local_id
          AND request_log.upstream_key_generation IS NOT DISTINCT FROM NEW.upstream_key_generation
          AND request_log.price_fact_id IS NOT DISTINCT FROM NEW.price_fact_id
          AND request_log.price_fact_version IS NOT DISTINCT FROM NEW.price_fact_version
          AND request_log.price_fact_schema_version IS NOT DISTINCT FROM NEW.price_fact_schema_version
          AND request_log.price_fact_checksum IS NOT DISTINCT FROM NEW.price_fact_checksum
          AND request_log.price_canonical_group_name IS NOT DISTINCT FROM NEW.price_canonical_group_name
          AND request_log.price_model_name IS NOT DISTINCT FROM NEW.price_model_name
          AND request_log.price_operation IS NOT DISTINCT FROM NEW.price_operation
          AND request_log.price_protocol IS NOT DISTINCT FROM NEW.price_protocol
          AND request_log.price_input_unit IS NOT DISTINCT FROM NEW.price_input_unit
          AND request_log.price_output_unit IS NOT DISTINCT FROM NEW.price_output_unit
          AND request_log.price_cache_read_unit IS NOT DISTINCT FROM NEW.price_cache_read_unit
          AND request_log.price_cache_write_unit IS NOT DISTINCT FROM NEW.price_cache_write_unit
          AND request_log.price_long_context_threshold_tokens IS NOT DISTINCT FROM NEW.price_long_context_threshold_tokens
          AND request_log.price_long_context_input_unit IS NOT DISTINCT FROM NEW.price_long_context_input_unit
          AND request_log.price_long_context_output_unit IS NOT DISTINCT FROM NEW.price_long_context_output_unit
          AND request_log.price_long_context_cache_read_unit IS NOT DISTINCT FROM NEW.price_long_context_cache_read_unit
          AND request_log.price_long_context_cache_write_unit IS NOT DISTINCT FROM NEW.price_long_context_cache_write_unit
          AND request_log.price_operation_unit IS NOT DISTINCT FROM NEW.price_operation_unit
          AND request_log.price_currency IS NOT DISTINCT FROM NEW.price_currency
          AND request_log.price_unit IS NOT DISTINCT FROM NEW.price_unit
          AND request_log.price_source IS NOT DISTINCT FROM NEW.price_source
          AND request_log.price_observed_at IS NOT DISTINCT FROM NEW.price_observed_at
          AND request_log.price_valid_from IS NOT DISTINCT FROM NEW.price_valid_from
          AND request_log.price_valid_until IS NOT DISTINCT FROM NEW.price_valid_until
          AND request_log.usage_schema IS NOT DISTINCT FROM NEW.usage_schema
          AND request_log.usage_schema_version IS NOT DISTINCT FROM NEW.usage_schema_version
          AND request_log.usage_operation_present IS NOT DISTINCT FROM NEW.usage_operation_present
          AND request_log.operation_units IS NOT DISTINCT FROM NEW.operation_units
          AND (
              NEW.price_fact_schema_version <> 2
              OR (
                  request_log.group_ratio IS NOT DISTINCT FROM NEW.group_ratio
                  AND request_log.completion_ratio IS NOT DISTINCT FROM NEW.completion_ratio
              )
          )
        FOR UPDATE;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'gateway request dispatch and final log price snapshots differ'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'gateway_request_dispatch_final_price_snapshot_consistency';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

ALTER TABLE public.gateway_request_dispatch_audits
    DROP CONSTRAINT chk_gateway_request_dispatch_price_values,
    DROP CONSTRAINT chk_gateway_request_dispatch_price_fact_shape,
    ADD CONSTRAINT chk_gateway_request_dispatch_price_values
        CHECK (
            (price_input_unit IS NULL OR (price_input_unit >= 0 AND price_input_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_output_unit IS NULL OR (price_output_unit >= 0 AND price_output_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_cache_read_unit IS NULL OR (price_cache_read_unit >= 0 AND price_cache_read_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_cache_write_unit IS NULL OR (price_cache_write_unit >= 0 AND price_cache_write_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_input_unit IS NULL OR (price_long_context_input_unit >= 0 AND price_long_context_input_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_output_unit IS NULL OR (price_long_context_output_unit >= 0 AND price_long_context_output_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_cache_read_unit IS NULL OR (price_long_context_cache_read_unit >= 0 AND price_long_context_cache_read_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_long_context_cache_write_unit IS NULL OR (price_long_context_cache_write_unit >= 0 AND price_long_context_cache_write_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (price_operation_unit IS NULL OR (price_operation_unit >= 0 AND price_operation_unit::text NOT IN ('NaN', 'Infinity', '-Infinity')))
            AND (operation_units IS NULL OR operation_units >= 0)
            AND usage_operation_present = (operation_units IS NOT NULL)
            AND (price_observed_at IS NULL OR isfinite(price_observed_at))
            AND (price_valid_from IS NULL OR isfinite(price_valid_from))
            AND (price_valid_until IS NULL OR (price_valid_from IS NOT NULL AND isfinite(price_valid_until) AND price_valid_until > price_valid_from))
        ) NOT VALID,
    ADD CONSTRAINT chk_gateway_request_dispatch_price_fact_shape
        CHECK (
            (
                price_fact_id IS NULL
                AND price_cost_unit_epoch = 0 AND price_fact_version = 0 AND price_fact_schema_version = 0 AND price_fact_checksum = ''
                AND price_input_unit IS NULL AND price_output_unit IS NULL AND price_cache_read_unit IS NULL AND price_cache_write_unit IS NULL
                AND price_long_context_threshold_tokens IS NULL AND price_long_context_input_unit IS NULL AND price_long_context_output_unit IS NULL
                AND price_long_context_cache_read_unit IS NULL AND price_long_context_cache_write_unit IS NULL AND price_operation_unit IS NULL
                AND price_currency = '' AND price_unit = '' AND price_source = ''
                AND price_observed_at IS NULL AND price_valid_from IS NULL AND price_valid_until IS NULL
                AND (
                    (channel_identity_revision = 0 AND upstream_group_id = 0 AND upstream_group_generation = 0
                     AND upstream_key_local_id = 0 AND upstream_key_generation = 0
                     AND price_canonical_group_name = '' AND price_model_name = '')
                    OR
                    (channel_id > 0 AND channel_identity_revision > 0 AND upstream_group_id > 0 AND upstream_group_generation > 0
                     AND upstream_key_local_id > 0 AND upstream_key_generation > 0
                     AND price_canonical_group_name = lower(btrim(price_canonical_group_name)) AND price_canonical_group_name <> ''
                     AND price_model_name = btrim(price_model_name) AND price_model_name <> '' AND price_operation <> '' AND price_protocol <> '')
                )
            )
            OR
            (
                price_fact_id IS NOT NULL AND price_fact_id > 0 AND channel_id > 0 AND channel_identity_revision > 0
                AND upstream_group_id > 0 AND upstream_group_generation > 0 AND upstream_key_local_id > 0 AND upstream_key_generation > 0
                AND price_cost_unit_epoch > 0 AND price_fact_version > 0 AND price_fact_schema_version IN (1, 2)
                AND price_fact_checksum ~ '^[0-9a-f]{64}$'
                AND price_canonical_group_name = lower(btrim(price_canonical_group_name)) AND price_canonical_group_name <> ''
                AND price_model_name = btrim(price_model_name) AND price_model_name <> ''
                AND price_operation = lower(btrim(price_operation)) AND price_operation <> ''
                AND price_protocol = lower(btrim(price_protocol)) AND price_protocol <> ''
                AND price_currency ~ '^[A-Z]{3}$' AND price_unit IN ('per_million_tokens', 'per_operation')
                AND price_source ~ '^[a-z0-9][a-z0-9_.-]{0,63}$' AND price_observed_at IS NOT NULL
                AND price_valid_from IS NOT NULL AND price_observed_at <= price_valid_from
                AND (
                    (price_fact_schema_version = 1
                     AND price_long_context_threshold_tokens IS NULL AND price_long_context_input_unit IS NULL
                     AND price_long_context_output_unit IS NULL AND price_long_context_cache_read_unit IS NULL
                     AND price_long_context_cache_write_unit IS NULL
                     AND (
                        (price_operation = 'text_generation' AND price_protocol IN ('openai_chat_completions', 'openai_completions', 'openai_responses', 'anthropic_messages')
                         AND price_unit = 'per_million_tokens' AND price_operation_unit IS NULL
                         AND (price_input_unit IS NOT NULL OR price_output_unit IS NOT NULL OR price_cache_read_unit IS NOT NULL OR price_cache_write_unit IS NOT NULL))
                        OR
                        (price_operation = 'embedding' AND price_protocol = 'openai_embeddings' AND price_unit = 'per_million_tokens'
                         AND price_input_unit IS NOT NULL AND price_output_unit IS NULL AND price_cache_read_unit IS NULL
                         AND price_cache_write_unit IS NULL AND price_operation_unit IS NULL)
                        OR
                        (price_operation = 'image_generation' AND price_protocol IN ('openai_images', 'openai_responses') AND price_unit = 'per_operation'
                         AND price_operation_unit IS NOT NULL AND price_input_unit IS NULL AND price_output_unit IS NULL
                         AND price_cache_read_unit IS NULL AND price_cache_write_unit IS NULL)
                     ))
                    OR
                    (price_fact_schema_version = 2 AND price_operation = 'text_generation'
                     AND price_protocol IN ('openai_chat_completions', 'openai_completions', 'openai_responses', 'anthropic_messages')
                     AND price_unit = 'per_million_tokens' AND price_operation_unit IS NULL
                     AND price_input_unit IS NOT NULL AND price_output_unit IS NOT NULL
                     AND price_long_context_threshold_tokens > 0
                     AND price_long_context_threshold_tokens < 9223372036854775807
                     AND price_long_context_input_unit IS NOT NULL AND price_long_context_output_unit IS NOT NULL
                     AND group_ratio >= 0 AND group_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')
                     AND completion_ratio >= 0 AND completion_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity'))
                )
            )
        ) NOT VALID;

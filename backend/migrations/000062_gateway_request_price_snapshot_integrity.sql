-- A price_fact_id alone authenticates only that some fact exists. Authenticate
-- every copied field at INSERT so a valid ID cannot be paired with another
-- fact's scope, prices, validity window, provenance, version, or checksum.
CREATE FUNCTION public.enforce_gateway_request_price_fact_snapshot()
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
       ) THEN
        RAISE EXCEPTION 'gateway request price snapshot does not match referenced price fact'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_fact_snapshot_consistency';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_logs_price_fact_consistency
BEFORE INSERT ON public.gateway_request_logs
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_price_fact_snapshot();

CREATE TRIGGER trg_gateway_request_dispatch_price_fact_consistency
BEFORE INSERT ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_price_fact_snapshot();

-- Finalization links two independently authenticated snapshots. Require the
-- audit and its final request log to describe the exact same frozen price fact
-- and request pricing contract before accepting the link.
CREATE FUNCTION public.enforce_gateway_request_dispatch_final_price_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.request_log_id IS NOT NULL THEN
        -- Serialize final linking and later audit-side usage changes with
        -- updates to the referenced log. Without this row lock, a concurrent
        -- log update could pass its own trigger before this link commits and
        -- leave the two operation-usage snapshots inconsistent.
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

CREATE TRIGGER trg_gateway_request_dispatch_final_price_snapshot_consistency
BEFORE INSERT OR UPDATE ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_dispatch_final_price_snapshot();

-- Operation usage is finalized after the request-log price snapshot is
-- inserted, so migration 45 deliberately left these two columns mutable.
-- Once a dispatch audit links the log, however, both ledgers are one frozen
-- terminal fact. Lock the linked audit before comparing so a concurrent link
-- or audit-side usage update cannot create a one-sided mutation.
CREATE FUNCTION public.enforce_gateway_request_log_linked_operation_usage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    linked_audit_usage record;
BEGIN
    SELECT audit.usage_operation_present, audit.operation_units
    INTO linked_audit_usage
    FROM public.gateway_request_dispatch_audits AS audit
    WHERE audit.request_log_id = OLD.id
    FOR UPDATE;

    IF FOUND
       AND (
           linked_audit_usage.usage_operation_present IS DISTINCT FROM NEW.usage_operation_present
           OR linked_audit_usage.operation_units IS DISTINCT FROM NEW.operation_units
       ) THEN
        RAISE EXCEPTION 'gateway request dispatch and final log operation usage differ'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_dispatch_final_price_snapshot_consistency';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_logs_linked_operation_usage_consistency
BEFORE UPDATE OF usage_operation_present, operation_units
ON public.gateway_request_logs
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_log_linked_operation_usage();

-- CREATE TRIGGER holds a write-conflicting lock until this transaction commits,
-- so these checks cover every pre-migration row without a writer racing between
-- validation and trigger installation. Refuse to make an existing forged copy
-- permanently immutable under the new enforcement boundary. Apply this
-- migration only while request writers are stopped: validating both ledgers
-- under those DDL locks is intentionally an offline release gate.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.gateway_request_logs AS request_log
        LEFT JOIN public.gateway_request_price_facts AS fact
          ON fact.id = request_log.price_fact_id
        WHERE request_log.price_fact_id IS NOT NULL
          AND NOT (
              ROW(
                  fact.id,
                  fact.channel_id,
                  fact.channel_account_identity_revision,
                  fact.cost_unit_epoch,
                  fact.upstream_group_id,
                  fact.canonical_group_name,
                  fact.upstream_group_presence_generation,
                  fact.gateway_upstream_key_id,
                  fact.upstream_key_generation,
                  fact.model_name,
                  fact.operation,
                  fact.protocol,
                  fact.input_unit_price,
                  fact.output_unit_price,
                  fact.cache_read_unit_price,
                  fact.cache_write_unit_price,
                  fact.operation_unit_price,
                  fact.unit,
                  fact.currency,
                  fact.source,
                  fact.observed_at,
                  fact.valid_from,
                  fact.valid_until,
                  fact.fact_version,
                  fact.schema_version,
                  fact.checksum
              ) IS NOT DISTINCT FROM ROW(
                  request_log.price_fact_id,
                  request_log.channel_id,
                  request_log.channel_identity_revision,
                  request_log.price_cost_unit_epoch,
                  request_log.upstream_group_id,
                  request_log.price_canonical_group_name,
                  request_log.upstream_group_generation,
                  request_log.upstream_key_local_id,
                  request_log.upstream_key_generation,
                  request_log.price_model_name,
                  request_log.price_operation,
                  request_log.price_protocol,
                  request_log.price_input_unit,
                  request_log.price_output_unit,
                  request_log.price_cache_read_unit,
                  request_log.price_cache_write_unit,
                  request_log.price_operation_unit,
                  request_log.price_unit,
                  request_log.price_currency,
                  request_log.price_source,
                  request_log.price_observed_at,
                  request_log.price_valid_from,
                  request_log.price_valid_until,
                  request_log.price_fact_version,
                  request_log.price_fact_schema_version,
                  request_log.price_fact_checksum
              )
          )
    ) THEN
        RAISE EXCEPTION 'existing gateway request log price snapshot does not match referenced price fact'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_fact_snapshot_consistency';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.gateway_request_dispatch_audits AS audit
        LEFT JOIN public.gateway_request_price_facts AS fact
          ON fact.id = audit.price_fact_id
        WHERE audit.price_fact_id IS NOT NULL
          AND NOT (
              ROW(
                  fact.id,
                  fact.channel_id,
                  fact.channel_account_identity_revision,
                  fact.cost_unit_epoch,
                  fact.upstream_group_id,
                  fact.canonical_group_name,
                  fact.upstream_group_presence_generation,
                  fact.gateway_upstream_key_id,
                  fact.upstream_key_generation,
                  fact.model_name,
                  fact.operation,
                  fact.protocol,
                  fact.input_unit_price,
                  fact.output_unit_price,
                  fact.cache_read_unit_price,
                  fact.cache_write_unit_price,
                  fact.operation_unit_price,
                  fact.unit,
                  fact.currency,
                  fact.source,
                  fact.observed_at,
                  fact.valid_from,
                  fact.valid_until,
                  fact.fact_version,
                  fact.schema_version,
                  fact.checksum
              ) IS NOT DISTINCT FROM ROW(
                  audit.price_fact_id,
                  audit.channel_id,
                  audit.channel_identity_revision,
                  audit.price_cost_unit_epoch,
                  audit.upstream_group_id,
                  audit.price_canonical_group_name,
                  audit.upstream_group_generation,
                  audit.upstream_key_local_id,
                  audit.upstream_key_generation,
                  audit.price_model_name,
                  audit.price_operation,
                  audit.price_protocol,
                  audit.price_input_unit,
                  audit.price_output_unit,
                  audit.price_cache_read_unit,
                  audit.price_cache_write_unit,
                  audit.price_operation_unit,
                  audit.price_unit,
                  audit.price_currency,
                  audit.price_source,
                  audit.price_observed_at,
                  audit.price_valid_from,
                  audit.price_valid_until,
                  audit.price_fact_version,
                  audit.price_fact_schema_version,
                  audit.price_fact_checksum
              )
          )
    ) THEN
        RAISE EXCEPTION 'existing gateway request dispatch price snapshot does not match referenced price fact'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_price_fact_snapshot_consistency';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.gateway_request_dispatch_audits AS audit
        LEFT JOIN public.gateway_request_logs AS request_log
          ON request_log.id = audit.request_log_id
        WHERE audit.request_log_id IS NOT NULL
          AND NOT (
              ROW(
                  request_log.id,
                  request_log.channel_id,
                  request_log.channel_identity_revision,
                  request_log.price_cost_unit_epoch,
                  request_log.upstream_group_id,
                  request_log.upstream_group_generation,
                  request_log.upstream_key_local_id,
                  request_log.upstream_key_generation,
                  request_log.price_fact_id,
                  request_log.price_fact_version,
                  request_log.price_fact_schema_version,
                  request_log.price_fact_checksum,
                  request_log.price_canonical_group_name,
                  request_log.price_model_name,
                  request_log.price_operation,
                  request_log.price_protocol,
                  request_log.price_input_unit,
                  request_log.price_output_unit,
                  request_log.price_cache_read_unit,
                  request_log.price_cache_write_unit,
                  request_log.price_operation_unit,
                  request_log.price_currency,
                  request_log.price_unit,
                  request_log.price_source,
                  request_log.price_observed_at,
                  request_log.price_valid_from,
                  request_log.price_valid_until,
                  request_log.usage_schema,
                  request_log.usage_schema_version,
                  request_log.usage_operation_present,
                  request_log.operation_units
              ) IS NOT DISTINCT FROM ROW(
                  audit.request_log_id,
                  audit.channel_id,
                  audit.channel_identity_revision,
                  audit.price_cost_unit_epoch,
                  audit.upstream_group_id,
                  audit.upstream_group_generation,
                  audit.upstream_key_local_id,
                  audit.upstream_key_generation,
                  audit.price_fact_id,
                  audit.price_fact_version,
                  audit.price_fact_schema_version,
                  audit.price_fact_checksum,
                  audit.price_canonical_group_name,
                  audit.price_model_name,
                  audit.price_operation,
                  audit.price_protocol,
                  audit.price_input_unit,
                  audit.price_output_unit,
                  audit.price_cache_read_unit,
                  audit.price_cache_write_unit,
                  audit.price_operation_unit,
                  audit.price_currency,
                  audit.price_unit,
                  audit.price_source,
                  audit.price_observed_at,
                  audit.price_valid_from,
                  audit.price_valid_until,
                  audit.usage_schema,
                  audit.usage_schema_version,
                  audit.usage_operation_present,
                  audit.operation_units
              )
          )
    ) THEN
        RAISE EXCEPTION 'existing gateway request dispatch and final log price snapshots differ'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'gateway_request_dispatch_final_price_snapshot_consistency';
    END IF;
END;
$$;

-- Row triggers do not fire for TRUNCATE. Protect the append-only price-fact
-- ledger from that mutation path as well.
CREATE TRIGGER trg_gateway_request_price_facts_reject_truncate
BEFORE TRUNCATE ON public.gateway_request_price_facts
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_gateway_request_price_fact_mutation();

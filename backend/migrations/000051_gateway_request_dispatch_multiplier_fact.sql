-- Item 54: dashboard multiplier history is a dispatch-time fact, never a
-- projection of a later balance correction, current rate, or current group.
-- Keep pre-existing rows at version 0: they may lack a trace, exact group
-- generation, or raw multiplier and therefore must remain outside the new
-- provenance-gated trend rather than being guessed or backfilled.
ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN multiplier_provenance_version smallint NOT NULL DEFAULT 0;

-- Every audit appended after this migration carries the v1 immutability
-- fence. The constraint deliberately permits an incomplete v1 provenance
-- shape: routing must not fail merely because a historical/unsupported path
-- cannot supply all pricing facts. Consumers that require a multiplier must
-- independently require the complete v1 shape and show no coverage instead.
ALTER TABLE public.gateway_request_dispatch_audits
    ALTER COLUMN multiplier_provenance_version SET DEFAULT 1;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_multiplier_provenance
        CHECK (
            multiplier_provenance_version = 0
            OR (
                multiplier_provenance_version = 1
                AND group_ratio >= 0
                AND group_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND completion_ratio >= 0
                AND completion_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND (
                    raw_group_ratio IS NULL
                    OR (
                        raw_group_ratio >= 0
                        AND raw_group_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')
                    )
                )
                AND (
                    raw_completion_ratio IS NULL
                    OR (
                        raw_completion_ratio >= 0
                        AND raw_completion_ratio::text NOT IN ('NaN', 'Infinity', '-Infinity')
                    )
                )
                AND (
                    funding_factor IS NULL
                    OR (
                        funding_factor >= 0
                        AND funding_factor::text NOT IN ('NaN', 'Infinity', '-Infinity')
                    )
                )
                AND channel_identity_revision >= 0
                AND upstream_group_id >= 0
                AND upstream_group_generation >= 0
                AND upstream_key_local_id >= 0
                AND upstream_key_generation >= 0
            )
        ) NOT VALID;

-- The audit is inserted before transport entry and then receives only
-- lifecycle evidence (physical-send, response, acceptance, terminal state,
-- and its one request-log link). Its routing/multiplier provenance must never
-- be rewritten by a later funding correction or by a current-state lookup.
CREATE FUNCTION public.enforce_gateway_request_dispatch_multiplier_provenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.multiplier_provenance_version <> 1 THEN
            RAISE EXCEPTION 'new gateway request dispatch must use multiplier provenance version 1'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.multiplier_provenance_version IS DISTINCT FROM OLD.multiplier_provenance_version THEN
        RAISE EXCEPTION 'gateway request dispatch multiplier provenance version is immutable'
            USING ERRCODE = '23514';
    END IF;

    -- A final audit has exactly one log. Finalization may set the link while
    -- OLD is NULL, but a later update must never retarget or clear it.
    IF OLD.request_log_id IS NOT NULL
       AND NEW.request_log_id IS DISTINCT FROM OLD.request_log_id THEN
        RAISE EXCEPTION 'gateway request dispatch request log link is immutable once set'
            USING ERRCODE = '23514';
    END IF;

    -- Version-zero rows remain historical compatibility evidence. Version-one
    -- rows freeze the complete multiplier/provenance tuple at dispatch time.
    -- Do not include lifecycle fields here: physical/billing/stage/terminal
    -- updates and the first request-log association are intentionally valid.
    IF OLD.multiplier_provenance_version = 1
       AND (
            NEW.group_ratio IS DISTINCT FROM OLD.group_ratio
            OR NEW.completion_ratio IS DISTINCT FROM OLD.completion_ratio
            OR NEW.raw_group_ratio IS DISTINCT FROM OLD.raw_group_ratio
            OR NEW.raw_completion_ratio IS DISTINCT FROM OLD.raw_completion_ratio
            OR NEW.funding_factor IS DISTINCT FROM OLD.funding_factor
            OR NEW.cost_basis_version IS DISTINCT FROM OLD.cost_basis_version
            OR NEW.cost_basis_source IS DISTINCT FROM OLD.cost_basis_source
            OR NEW.telemetry_version IS DISTINCT FROM OLD.telemetry_version
            OR NEW.logical_request_trace_id IS DISTINCT FROM OLD.logical_request_trace_id
            OR NEW.routing_mode IS DISTINCT FROM OLD.routing_mode
            OR NEW.gateway_app IS DISTINCT FROM OLD.gateway_app
            OR NEW.downstream_key_id IS DISTINCT FROM OLD.downstream_key_id
            OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
            OR NEW.channel_identity_revision IS DISTINCT FROM OLD.channel_identity_revision
            OR NEW.group_name IS DISTINCT FROM OLD.group_name
            OR NEW.upstream_group_id IS DISTINCT FROM OLD.upstream_group_id
            OR NEW.upstream_group_generation IS DISTINCT FROM OLD.upstream_group_generation
            OR NEW.upstream_key_id IS DISTINCT FROM OLD.upstream_key_id
            OR NEW.upstream_key_name IS DISTINCT FROM OLD.upstream_key_name
            OR NEW.upstream_key_local_id IS DISTINCT FROM OLD.upstream_key_local_id
            OR NEW.upstream_key_generation IS DISTINCT FROM OLD.upstream_key_generation
            OR NEW.method IS DISTINCT FROM OLD.method
            OR NEW.path IS DISTINCT FROM OLD.path
            OR NEW.model_name IS DISTINCT FROM OLD.model_name
            OR NEW.upstream_model_name IS DISTINCT FROM OLD.upstream_model_name
        ) THEN
        RAISE EXCEPTION 'gateway request dispatch multiplier provenance is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_dispatch_multiplier_provenance
BEFORE INSERT OR UPDATE
ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_dispatch_multiplier_provenance();

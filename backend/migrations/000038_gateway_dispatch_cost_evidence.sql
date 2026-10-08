-- Provider request IDs are opaque billing identifiers. Keep the request log
-- and physical-dispatch audit on the same bounded contract so an ID accepted
-- by the API cannot fail only when PostgreSQL persists it.
ALTER TABLE public.gateway_request_logs
    ALTER COLUMN upstream_request_id TYPE character varying(512);

ALTER TABLE public.gateway_request_dispatch_audits
    ALTER COLUMN provider_request_id TYPE character varying(512);

ALTER TABLE public.gateway_request_logs
    ADD CONSTRAINT chk_gateway_request_logs_provider_id_bytes
        CHECK (upstream_request_id IS NULL OR octet_length(upstream_request_id) <= 512);

ALTER TABLE public.gateway_request_dispatch_audits
    DROP CONSTRAINT IF EXISTS chk_gateway_request_dispatch_provider_id_bytes,
    ADD CONSTRAINT chk_gateway_request_dispatch_provider_id_bytes
        CHECK (octet_length(provider_request_id) <= 512);

ALTER TABLE public.gateway_request_logs
    ADD COLUMN record_kind character varying(32) NOT NULL DEFAULT 'request',
    ADD CONSTRAINT chk_gateway_request_logs_record_kind
        CHECK (record_kind IN ('request', 'dispatch_cost_evidence')),
    ADD CONSTRAINT chk_gateway_request_logs_dispatch_cost_evidence_shape
        CHECK (
            record_kind <> 'dispatch_cost_evidence'
            OR (
                COALESCE(upstream_request_id, '') <> ''
                AND COALESCE(upstream_accepted, FALSE) = FALSE
                AND COALESCE(billing_committed, FALSE) = FALSE
                AND COALESCE(cost_reconcile_legacy_pending, FALSE) = FALSE
                AND status_code = 0
                AND first_byte_ms = 0
                AND latency_ms = 0
                AND COALESCE(prompt_tokens, 0) = 0
                AND COALESCE(completion_tokens, 0) = 0
                AND COALESCE(cache_creation_tokens, 0) = 0
                AND COALESCE(cache_read_tokens, 0) = 0
                AND COALESCE(total_tokens, 0) = 0
                AND btrim(COALESCE(error, '')) = ''
                AND COALESCE(cost_estimated, FALSE) = FALSE
                AND (
                    (
                        lower(btrim(COALESCE(cost_status, 'none'))) = 'none'
                        AND COALESCE(cost, 0) = 0
                        AND raw_cost IS NULL
                        AND funding_factor IS NOT NULL
                        AND funding_factor >= 0
                        AND funding_factor::text NOT IN ('NaN', 'Infinity', '-Infinity')
                        AND btrim(COALESCE(cost_source, '')) = ''
                        AND btrim(COALESCE(upstream_usage_id, '')) = ''
                    )
                    OR (
                        lower(btrim(COALESCE(cost_status, ''))) = 'reconciled'
                        AND cost IS NOT NULL
                        AND cost >= 0
                        AND cost::text NOT IN ('NaN', 'Infinity', '-Infinity')
                        AND raw_cost IS NOT NULL
                        AND raw_cost >= 0
                        AND raw_cost::text NOT IN ('NaN', 'Infinity', '-Infinity')
                        AND funding_factor IS NOT NULL
                        AND funding_factor >= 0
                        AND funding_factor::text NOT IN ('NaN', 'Infinity', '-Infinity')
                        AND abs(cost - (raw_cost * funding_factor)) <= 0.000000001 * GREATEST(
                            1::numeric,
                            abs(cost),
                            abs(raw_cost * funding_factor)
                        )
                        AND lower(btrim(COALESCE(cost_source, ''))) IN ('newapi_quota', 'sub2api_actual_cost')
                        AND btrim(COALESCE(upstream_usage_id, '')) <> ''
                    )
                )
            )
        );

-- A row's semantic domain is fixed when it is appended. Allowing an update
-- from hidden dispatch evidence to a normal request (or the reverse) would
-- silently move counters and money between ledgers.
CREATE FUNCTION public.reject_gateway_request_log_record_kind_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.record_kind IS DISTINCT FROM OLD.record_kind THEN
        RAISE EXCEPTION 'gateway request log record_kind is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_logs_record_kind_immutable
BEFORE UPDATE OF record_kind ON public.gateway_request_logs
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_log_record_kind_update();

-- Evidence is born as a zero-cost exact-ID reconciliation clue. The only
-- monetary transition permitted later is a provider-backed reconciliation;
-- ordinary request status, latency, token, and error fields remain permanently
-- empty under the CHECK above.
CREATE FUNCTION public.enforce_gateway_dispatch_cost_evidence_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.record_kind <> 'dispatch_cost_evidence' THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.funding_factor IS NULL
           OR NEW.funding_factor < 0
           OR NEW.funding_factor::text IN ('NaN', 'Infinity', '-Infinity') THEN
            RAISE EXCEPTION 'dispatch cost evidence requires a frozen finite funding factor'
                USING ERRCODE = '23514';
        END IF;
        IF lower(btrim(COALESCE(NEW.cost_status, 'none'))) <> 'none'
           OR COALESCE(NEW.cost, 0) <> 0
           OR NEW.raw_cost IS NOT NULL
           OR COALESCE(NEW.cost_estimated, FALSE)
           OR btrim(COALESCE(NEW.cost_source, '')) <> ''
           OR btrim(COALESCE(NEW.upstream_usage_id, '')) <> '' THEN
            RAISE EXCEPTION 'dispatch cost evidence must start with zero unverified cost'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.upstream_request_id IS DISTINCT FROM OLD.upstream_request_id THEN
        RAISE EXCEPTION 'dispatch cost evidence provider request ID is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.cost IS DISTINCT FROM OLD.cost
       OR NEW.raw_cost IS DISTINCT FROM OLD.raw_cost
       OR NEW.cost_estimated IS DISTINCT FROM OLD.cost_estimated
       OR NEW.cost_source IS DISTINCT FROM OLD.cost_source
       OR NEW.cost_status IS DISTINCT FROM OLD.cost_status
       OR NEW.upstream_usage_id IS DISTINCT FROM OLD.upstream_usage_id
       OR NEW.funding_factor IS DISTINCT FROM OLD.funding_factor THEN
        IF btrim(COALESCE(OLD.upstream_usage_id, '')) <> '' THEN
            IF NEW.raw_cost IS DISTINCT FROM OLD.raw_cost
               OR NEW.cost_estimated IS DISTINCT FROM OLD.cost_estimated
               OR NEW.cost_source IS DISTINCT FROM OLD.cost_source
               OR NEW.cost_status IS DISTINCT FROM OLD.cost_status
               OR NEW.upstream_usage_id IS DISTINCT FROM OLD.upstream_usage_id
               OR NEW.cost IS NULL
               OR NEW.cost < 0
               OR NEW.cost::text IN ('NaN', 'Infinity', '-Infinity')
               OR NEW.raw_cost IS NULL
               OR NEW.raw_cost < 0
               OR NEW.raw_cost::text IN ('NaN', 'Infinity', '-Infinity')
               OR NEW.funding_factor IS NULL
               OR NEW.funding_factor < 0
               OR NEW.funding_factor::text IN ('NaN', 'Infinity', '-Infinity')
               OR abs(NEW.cost - (NEW.raw_cost * NEW.funding_factor)) > 0.000000001 * GREATEST(
                    1::numeric,
                    abs(NEW.cost),
                    abs(NEW.raw_cost * NEW.funding_factor)
               ) THEN
                RAISE EXCEPTION 'reconciled dispatch cost evidence only permits converted cost correction'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF lower(btrim(COALESCE(NEW.cost_status, ''))) <> 'reconciled'
           OR NEW.cost IS NULL
           OR NEW.cost < 0
           OR NEW.cost::text IN ('NaN', 'Infinity', '-Infinity')
           OR NEW.raw_cost IS NULL
           OR NEW.raw_cost < 0
           OR NEW.raw_cost::text IN ('NaN', 'Infinity', '-Infinity')
           OR NEW.funding_factor IS DISTINCT FROM OLD.funding_factor
           OR NEW.funding_factor IS NULL
           OR NEW.funding_factor < 0
           OR NEW.funding_factor::text IN ('NaN', 'Infinity', '-Infinity')
           OR abs(NEW.cost - (NEW.raw_cost * NEW.funding_factor)) > 0.000000001 * GREATEST(
                1::numeric,
                abs(NEW.cost),
                abs(NEW.raw_cost * NEW.funding_factor)
           )
           OR COALESCE(NEW.cost_estimated, FALSE)
           OR lower(btrim(COALESCE(NEW.cost_source, ''))) NOT IN ('newapi_quota', 'sub2api_actual_cost')
           OR btrim(COALESCE(NEW.upstream_usage_id, '')) = '' THEN
            RAISE EXCEPTION 'dispatch cost evidence may only accept exact provider reconciliation'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_dispatch_cost_evidence_transition
BEFORE INSERT OR UPDATE ON public.gateway_request_logs
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_dispatch_cost_evidence_transition();

-- Header-only provider IDs are now recoverable accounting evidence. Replace
-- the v33 partial index so startup recovery does not scan all unaccepted
-- dispatches to find them.
ALTER TABLE public.gateway_request_dispatch_audits
    DROP CONSTRAINT chk_gateway_request_dispatch_completed_unaccepted,
    ADD CONSTRAINT chk_gateway_request_dispatch_completed_unaccepted
        CHECK (
            stage <> 'completed'
            OR (
                accepted_at IS NULL
                AND NOT billing_committed
                AND COALESCE(provider_request_id, '') = ''
            )
        );

DROP INDEX public.idx_gateway_request_dispatch_recovery;

CREATE INDEX idx_gateway_request_dispatch_recovery
    ON public.gateway_request_dispatch_audits (dispatched_at, id)
    WHERE terminal_at IS NULL
      AND (
          accepted_at IS NOT NULL
          OR billing_committed
          OR COALESCE(provider_request_id, '') <> ''
      );

ALTER TABLE public.gateway_request_aggregates
    ADD COLUMN dispatch_evidence_pending bigint NOT NULL DEFAULT 0,
    ADD COLUMN dispatch_evidence_actual double precision NOT NULL DEFAULT 0,
    ADD COLUMN dispatch_evidence_reported double precision NOT NULL DEFAULT 0,
    ADD COLUMN dispatch_evidence_reconciled double precision NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_gateway_request_aggregates_dispatch_evidence_shape
        CHECK (
            (
                result = 'evidence'
                AND requests = 0
                AND prompt_tokens = 0
                AND completion_tokens = 0
                AND cache_creation_tokens = 0
                AND cache_read_tokens = 0
                AND total_tokens = 0
                AND total_cost = 0
                AND estimated_cost = 0
                AND actual_cost = 0
                AND reported_cost = 0
                AND reconciled_cost = 0
                AND latency_total_ms = 0
                AND latency_samples = 0
                AND performance_requests = 0
                AND performance_tokens = 0
                AND performance_cost = 0
                AND performance_input_tokens = 0
                AND performance_output_tokens = 0
                AND performance_input_priced_tokens = 0
                AND performance_output_priced_tokens = 0
                AND performance_cache_creation_tokens = 0
                AND performance_cache_read_tokens = 0
                AND performance_input_cost = 0
                AND performance_output_cost = 0
                AND dispatch_evidence_pending >= 0
                AND dispatch_evidence_actual >= 0
                AND dispatch_evidence_reported >= 0
                AND dispatch_evidence_reconciled >= 0
            )
            OR (
                result <> 'evidence'
                AND dispatch_evidence_pending = 0
                AND dispatch_evidence_actual = 0
                AND dispatch_evidence_reported = 0
                AND dispatch_evidence_reconciled = 0
            )
        );

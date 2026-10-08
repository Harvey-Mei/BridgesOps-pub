-- Item 46: a pre-Do audit is not billing evidence. Version the invariant so
-- legacy rows keep their historical recovery semantics while every new audit
-- separates actual wire observation from authoritative operation acceptance.
ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN physical_billing_invariant_version smallint NOT NULL DEFAULT 0;

-- Existing rows retain v0. Any insert after this migration, including one
-- made by a binary that predates the Go writer change, receives v1 and is
-- therefore fenced by the constraint below instead of silently reusing the
-- old pre-Do billing semantics.
ALTER TABLE public.gateway_request_dispatch_audits
    ALTER COLUMN physical_billing_invariant_version SET DEFAULT 1;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_physical_billing_invariant
        CHECK (
            physical_billing_invariant_version = 0
            OR (
                physical_billing_invariant_version = 1
                -- Both v1 endpoints are POST-only. Bind the frozen contract
                -- to the request method even before billing is committed.
                AND (
                    path NOT IN ('/v1/images/generations', '/v1/responses')
                    OR method = 'POST'
                )
                -- The direct image endpoint is always non-idempotent. Its
                -- request contract must not be erasable before the wire
                -- boundary, even while pricing scope is unavailable. Actual
                -- operation usage is intentionally absent until acceptance.
                AND (
                    path <> '/v1/images/generations'
                    OR (
                        price_operation = 'image_generation'
                        AND price_protocol = 'openai_images'
                        AND usage_schema = 'operation_units_v1'
                        AND usage_schema_version = 1
                        AND (
                            (NOT usage_operation_present AND operation_units IS NULL)
                            OR (usage_operation_present AND operation_units IS NOT DISTINCT FROM 1)
                        )
                    )
                )
                -- A Responses image contract must retain the protocol implied
                -- by its route even in the pre-dispatch F/F state.
                AND (
                    path <> '/v1/responses'
                    OR price_operation <> 'image_generation'
                    OR price_protocol = 'openai_responses'
                )
                -- Crossing the wire does not consume an operation unit. Usage
                -- and billing become authoritative together at acceptance.
                AND (
                    billing_committed
                    OR (
                        NOT usage_operation_present
                        AND operation_units IS NULL
                    )
                )
                -- Billing is trustworthy only after a real wire observation
                -- and an authoritative accepted/completed response shape.
                AND (
                    NOT billing_committed
                    OR (
                        physically_dispatched
                        AND response_observed_at IS NOT NULL
                        AND accepted_at IS NOT NULL
                        AND length(btrim(accepted_event_type)) > 0
                        AND price_operation = 'image_generation'
                        AND usage_schema = 'operation_units_v1'
                        AND usage_operation_present
                        AND operation_units IS NOT DISTINCT FROM 1
                        AND (
                            (path = '/v1/images/generations' AND price_protocol = 'openai_images')
                            OR (path = '/v1/responses' AND price_protocol = 'openai_responses')
                        )
                    )
                )
                -- Acceptance implies a prior wire observation, but a request
                -- can cross the wire and still remain unaccepted/ambiguous.
                AND (
                    accepted_at IS NULL
                    OR (physically_dispatched AND billing_committed)
                )
            )
        )
        NOT VALID;

-- The invariant version is written only on INSERT. Permitting a later update
-- from v1 to v0 would let a writer bypass the physical/billing coupling; an
-- old v0 row intentionally stays legacy rather than being reinterpreted.
CREATE FUNCTION public.enforce_gateway_request_dispatch_physical_billing_invariant_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.physical_billing_invariant_version <> 1 THEN
        RAISE EXCEPTION 'new gateway request dispatch must use physical billing invariant version 1'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT'
       AND (
            NEW.physically_dispatched
            OR NEW.response_observed_at IS NOT NULL
            OR NEW.accepted_at IS NOT NULL
            OR NEW.billing_committed
            OR NEW.usage_operation_present
            OR NEW.operation_units IS NOT NULL
       ) THEN
        RAISE EXCEPTION 'new gateway request dispatch must start before wire observation and acceptance'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE'
       AND NEW.physical_billing_invariant_version IS DISTINCT FROM OLD.physical_billing_invariant_version THEN
        RAISE EXCEPTION 'gateway request dispatch physical billing invariant version is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_dispatch_physical_billing_invariant_version
BEFORE INSERT OR UPDATE
ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_dispatch_physical_billing_invariant_version();

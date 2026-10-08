-- The physical marker is persisted from net/http's WroteHeaders trace, after
-- request bytes reached the configured transport. A crash before the durable
-- return/acceptance evidence remains a manual-review ambiguity.
ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN transport_confirmation_version smallint NOT NULL DEFAULT 0,
    ADD COLUMN transport_return_confirmed boolean NOT NULL DEFAULT false,
    ADD COLUMN transport_return_confirmed_at timestamp with time zone;

ALTER TABLE public.gateway_request_dispatch_audits
    ALTER COLUMN transport_confirmation_version SET DEFAULT 1;

-- Migration 48 predates the durable transport-return fence. Its original
-- constraint treated operation_units as response evidence and therefore made
-- the image writer's frozen request contract, as well as its atomic
-- physical/billing marker, impossible to persist. Keep acceptance dependent on
-- a confirmed response, but allow the one operation unit to describe the
-- immutable image request contract before the transport runs.
ALTER TABLE public.gateway_request_dispatch_audits
    DROP CONSTRAINT chk_gateway_request_dispatch_physical_billing_invariant,
    ADD CONSTRAINT chk_gateway_request_dispatch_physical_billing_invariant
        CHECK (
            physical_billing_invariant_version = 0
            OR (
                physical_billing_invariant_version = 1
                AND (
                    path NOT IN ('/v1/images/generations', '/v1/responses')
                    OR method = 'POST'
                )
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
                AND (
                    path <> '/v1/responses'
                    OR price_operation <> 'image_generation'
                    OR price_protocol = 'openai_responses'
                )
                AND (
                    NOT billing_committed
                    OR (
                        physically_dispatched
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
                AND (
                    price_operation <> 'image_generation'
                    OR accepted_at IS NULL
                    OR (
                        physically_dispatched
                        AND billing_committed
                        AND response_observed_at IS NOT NULL
                    )
                )
            )
        ) NOT VALID;

CREATE OR REPLACE FUNCTION public.enforce_gateway_request_dispatch_physical_billing_invariant_version()
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

-- An armed marker whose RoundTripper return was never confirmed is retained
-- as a terminal manual-review record. It must not be relabelled completed or
-- finalized and it never creates a usage row automatically.
ALTER TABLE public.gateway_request_dispatch_audits
    DROP CONSTRAINT chk_gateway_request_dispatch_stage,
    DROP CONSTRAINT chk_gateway_request_dispatch_terminal;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_stage
        CHECK (stage IN ('dispatched', 'responded', 'accepted', 'completed', 'finalized', 'manual_review')),
    ADD CONSTRAINT chk_gateway_request_dispatch_terminal
        CHECK (
            (terminal_at IS NULL AND terminal_kind = '' AND request_log_id IS NULL AND stage NOT IN ('completed', 'finalized', 'manual_review'))
            OR
            (terminal_at IS NOT NULL AND length(btrim(terminal_kind)) > 0 AND stage = 'completed' AND request_log_id IS NULL)
            OR
            (terminal_at IS NOT NULL AND length(btrim(terminal_kind)) > 0 AND stage = 'manual_review' AND request_log_id IS NULL)
            OR
            (terminal_at IS NOT NULL AND length(btrim(terminal_kind)) > 0 AND stage = 'finalized' AND request_log_id IS NOT NULL)
        );

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_transport_confirmation
        CHECK (
            (
                transport_confirmation_version = 0
                AND NOT transport_return_confirmed
                AND transport_return_confirmed_at IS NULL
            )
            OR (
                transport_confirmation_version = 1
                AND transport_return_confirmed = (transport_return_confirmed_at IS NOT NULL)
                AND transport_return_confirmed = (transport_state = 'confirmed')
                AND (
                    NOT transport_return_confirmed
                    OR physically_dispatched
                )
                -- Durable evidence advances in one direction: a response can
                -- only follow a returned transport and acceptance can only
                -- follow a recorded response. Image billing is paired with the
                -- physical marker and is constrained separately above.
                AND (
                    response_observed_at IS NULL
                    OR transport_return_confirmed
                )
                AND (
                    accepted_at IS NULL
                    OR response_observed_at IS NOT NULL
                )
            )
        ) NOT VALID;

CREATE INDEX idx_gateway_request_dispatch_unconfirmed_transport
    ON public.gateway_request_dispatch_audits (dispatched_at, id)
    WHERE terminal_at IS NULL
      AND transport_confirmation_version = 1
      AND physically_dispatched
      AND NOT transport_return_confirmed;

CREATE FUNCTION public.enforce_gateway_request_dispatch_transport_confirmation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.transport_confirmation_version <> 1
           OR NEW.transport_return_confirmed
           OR NEW.transport_return_confirmed_at IS NOT NULL THEN
            RAISE EXCEPTION 'new gateway request dispatch must start with unconfirmed transport version 1'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.transport_confirmation_version IS DISTINCT FROM OLD.transport_confirmation_version THEN
        RAISE EXCEPTION 'gateway request dispatch transport confirmation version is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.transport_confirmation_version = 1
       AND NOT (
            (
                (
                    NEW.transport_state = 'not_dispatched'
                    AND NEW.recovery_requested_at IS NOT NULL
                )
                OR (
                    NEW.transport_state = 'manual_review'
                    AND NEW.terminal_at IS NOT NULL
                    AND NEW.stage IN ('completed', 'manual_review')
                    AND length(btrim(NEW.terminal_kind)) > 0
                )
            )
            AND NEW.response_observed_at IS NULL
            AND NEW.accepted_at IS NULL
            AND length(btrim(NEW.provider_request_id)) = 0
            AND NOT OLD.transport_return_confirmed
            AND NOT NEW.physically_dispatched
            AND NOT NEW.billing_committed
            AND NEW.usage_operation_present IS NOT DISTINCT FROM OLD.usage_operation_present
            AND NEW.operation_units IS NOT DISTINCT FROM OLD.operation_units
       )
       AND (
            (OLD.physically_dispatched AND NOT NEW.physically_dispatched)
            OR (OLD.billing_committed AND NOT NEW.billing_committed)
            OR (OLD.transport_return_confirmed AND NOT NEW.transport_return_confirmed)
            OR (OLD.transport_return_confirmed_at IS NOT NULL
                AND NEW.transport_return_confirmed_at IS DISTINCT FROM OLD.transport_return_confirmed_at)
            OR (OLD.response_observed_at IS NOT NULL
                AND NEW.response_observed_at IS DISTINCT FROM OLD.response_observed_at)
            OR (OLD.accepted_at IS NOT NULL
                AND NEW.accepted_at IS DISTINCT FROM OLD.accepted_at)
            OR (length(btrim(OLD.accepted_event_type)) > 0
                AND NEW.accepted_event_type IS DISTINCT FROM OLD.accepted_event_type)
            OR (OLD.usage_operation_present AND NOT NEW.usage_operation_present)
            OR (OLD.operation_units IS NOT NULL
                AND NEW.operation_units IS DISTINCT FROM OLD.operation_units)
       ) THEN
        RAISE EXCEPTION 'gateway request dispatch transport evidence cannot regress'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.transport_return_confirmed AND NOT OLD.transport_return_confirmed
       AND NOT OLD.physically_dispatched THEN
        RAISE EXCEPTION 'gateway request dispatch transport confirmation requires a prior wire marker'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.transport_confirmation_version = 1
       AND NEW.transport_return_confirmed IS DISTINCT FROM (NEW.transport_state = 'confirmed') THEN
        RAISE EXCEPTION 'gateway request dispatch transport state and return confirmation must advance together'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.transport_confirmation_version = 1
       AND NEW.response_observed_at IS NOT NULL
       AND OLD.response_observed_at IS NULL
       AND NOT OLD.transport_return_confirmed THEN
        RAISE EXCEPTION 'gateway request dispatch response observation requires prior transport confirmation'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.transport_confirmation_version = 1
       AND NEW.accepted_at IS NOT NULL
       AND OLD.accepted_at IS NULL
       AND NEW.response_observed_at IS NULL THEN
        RAISE EXCEPTION 'gateway request dispatch acceptance requires prior response observation'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_dispatch_transport_confirmation
BEFORE INSERT OR UPDATE
ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_dispatch_transport_confirmation();

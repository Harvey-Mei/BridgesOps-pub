-- Item 46: make the pre-RoundTripper boundary and terminal recovery durable.
-- Existing audits remain version 0/legacy because their exact transport-entry
-- state cannot be reconstructed. Every new audit starts prepared, is armed in
-- the owner-fenced transaction immediately before the real RoundTripper call,
-- and is confirmed only after that call has actually returned control.
ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN transport_fence_version smallint NOT NULL DEFAULT 0,
    ADD COLUMN transport_state character varying(24) NOT NULL DEFAULT 'legacy',
    ADD COLUMN rollover_predecessor_id bigint,
    ADD COLUMN prior_physically_dispatched_failure_count integer NOT NULL DEFAULT 0,
    ADD COLUMN real_fallback boolean NOT NULL DEFAULT false;

ALTER TABLE public.gateway_request_dispatch_audits
    ALTER COLUMN transport_fence_version SET DEFAULT 1,
    ALTER COLUMN transport_state SET DEFAULT 'prepared';

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT fk_gateway_request_dispatch_rollover_predecessor
        FOREIGN KEY (rollover_predecessor_id)
        REFERENCES public.gateway_request_dispatch_audits(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    ADD CONSTRAINT chk_gateway_request_dispatch_transport_fence
        CHECK (
            (
                transport_fence_version = 0
                AND transport_state = 'legacy'
                AND rollover_predecessor_id IS NULL
                AND prior_physically_dispatched_failure_count = 0
                AND NOT real_fallback
            )
            OR (
                transport_fence_version = 1
                AND transport_state IN (
                    'prepared',
                    'armed',
                    'confirmed',
                    'not_dispatched',
                    'manual_review'
                )
                AND prior_physically_dispatched_failure_count >= 0
                AND (
                    (rollover_predecessor_id IS NULL AND prior_physically_dispatched_failure_count = 0 AND NOT real_fallback)
                    OR (rollover_predecessor_id IS NOT NULL AND prior_physically_dispatched_failure_count > 0)
                )
                AND (
                    (transport_state = 'prepared' AND NOT physically_dispatched AND NOT billing_committed)
                    OR (transport_state IN ('armed', 'confirmed') AND physically_dispatched)
                    OR (transport_state IN ('not_dispatched', 'manual_review') AND NOT physically_dispatched AND NOT billing_committed)
                )
            )
        ) NOT VALID;

CREATE INDEX idx_gateway_request_dispatch_transport_recovery
    ON public.gateway_request_dispatch_audits (transport_state, dispatched_at, id)
    WHERE terminal_at IS NULL
      AND transport_fence_version = 1
      AND transport_state IN ('armed', 'not_dispatched', 'manual_review');

CREATE FUNCTION public.enforce_gateway_request_dispatch_transport_fence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.transport_fence_version <> 1
           OR NEW.transport_state <> 'prepared'
           OR NEW.physically_dispatched
           OR NEW.billing_committed THEN
            RAISE EXCEPTION 'new gateway request dispatch must start at transport fence v1 prepared'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.transport_fence_version IS DISTINCT FROM OLD.transport_fence_version
       OR NEW.rollover_predecessor_id IS DISTINCT FROM OLD.rollover_predecessor_id
       OR NEW.prior_physically_dispatched_failure_count IS DISTINCT FROM OLD.prior_physically_dispatched_failure_count
       OR NEW.real_fallback IS DISTINCT FROM OLD.real_fallback THEN
        RAISE EXCEPTION 'gateway request dispatch transport provenance is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.transport_state IS DISTINCT FROM OLD.transport_state
       AND NOT (
            (OLD.transport_state = 'prepared' AND NEW.transport_state IN ('armed', 'not_dispatched', 'manual_review'))
            OR (OLD.transport_state = 'armed' AND NEW.transport_state IN ('confirmed', 'not_dispatched', 'manual_review'))
       ) THEN
        RAISE EXCEPTION 'gateway request dispatch transport state transition is invalid'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_dispatch_transport_fence
BEFORE INSERT OR UPDATE
ON public.gateway_request_dispatch_audits
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_dispatch_transport_fence();

-- A fenced owner keeps refusing new bindings until every old-identity request
-- has durably handed off its exact terminal outcome. That wait is bounded: a
-- transport that ignores cancellation must not stop this process from ever
-- taking a new incarnation. A bounded forced rotation is recorded append-only on
-- the abandoned incarnation itself. These columns carry no request, usage or
-- billing fact, so the degrade can never overwrite an exact terminal snapshot;
-- the outstanding handoffs keep retrying against the immutable audit-bound owner
-- identity, and anything still open falls back to ordinary orphan recovery.
ALTER TABLE public.gateway_request_dispatch_owners
    ADD COLUMN drain_forced_at timestamp with time zone,
    ADD COLUMN drain_forced_bindings integer NOT NULL DEFAULT 0,
    ADD COLUMN drain_forced_handoffs integer NOT NULL DEFAULT 0;

ALTER TABLE public.gateway_request_dispatch_owners
    ADD CONSTRAINT chk_gateway_request_dispatch_owner_forced_drain
        CHECK (
            (
                drain_forced_at IS NULL
                AND drain_forced_bindings = 0
                AND drain_forced_handoffs = 0
            )
            OR (
                drain_forced_at IS NOT NULL
                AND stopped_at IS NOT NULL
                AND drain_forced_bindings >= 0
                AND drain_forced_handoffs >= 0
            )
        );

CREATE FUNCTION public.enforce_gateway_request_dispatch_owner_forced_drain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.drain_forced_at IS NOT NULL
       AND (
            NEW.drain_forced_at IS DISTINCT FROM OLD.drain_forced_at
            OR NEW.drain_forced_bindings IS DISTINCT FROM OLD.drain_forced_bindings
            OR NEW.drain_forced_handoffs IS DISTINCT FROM OLD.drain_forced_handoffs
       ) THEN
        RAISE EXCEPTION 'gateway request dispatch owner forced drain audit is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_dispatch_owner_forced_drain
BEFORE UPDATE
ON public.gateway_request_dispatch_owners
FOR EACH ROW
EXECUTE FUNCTION public.enforce_gateway_request_dispatch_owner_forced_drain();

-- The in-memory queue remains only a wake-up optimization. This table is the
-- durable source of truth for the exact terminal log and lifecycle facts. A
-- dispatch has at most one first-wins handoff; Finalize remains the exactly-once
-- request-log and aggregate boundary.
CREATE TABLE public.gateway_request_dispatch_terminal_handoffs (
    dispatch_id bigint PRIMARY KEY,
    version smallint NOT NULL DEFAULT 1,
    terminal_kind character varying(64) NOT NULL,
    requested_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone NOT NULL,
    log_snapshot jsonb,
    facts_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    telemetry_version integer NOT NULL DEFAULT 0,
    physically_dispatched boolean NOT NULL DEFAULT false,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gateway_request_dispatch_terminal_handoff
        FOREIGN KEY (dispatch_id)
        REFERENCES public.gateway_request_dispatch_audits(id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT chk_gateway_request_dispatch_terminal_handoff_version
        CHECK (version = 1),
    CONSTRAINT chk_gateway_request_dispatch_terminal_handoff_kind
        CHECK (length(btrim(terminal_kind)) > 0),
    CONSTRAINT chk_gateway_request_dispatch_terminal_handoff_times
        CHECK (finished_at >= started_at AND requested_at >= started_at),
    CONSTRAINT chk_gateway_request_dispatch_terminal_handoff_log
        CHECK (
            log_snapshot IS NULL
            OR (
                jsonb_typeof(log_snapshot) = 'object'
                AND octet_length(log_snapshot::text) <= 262144
            )
        ),
    CONSTRAINT chk_gateway_request_dispatch_terminal_handoff_facts
        CHECK (
            jsonb_typeof(facts_snapshot) = 'object'
            AND octet_length(facts_snapshot::text) <= 8192
        )
);

CREATE INDEX idx_gateway_request_dispatch_terminal_handoff_ready
    ON public.gateway_request_dispatch_terminal_handoffs (requested_at, dispatch_id);

CREATE FUNCTION public.reject_gateway_request_dispatch_terminal_handoff_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'gateway request dispatch terminal handoff is immutable'
        USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER trg_gateway_request_dispatch_terminal_handoff_immutable
BEFORE UPDATE
ON public.gateway_request_dispatch_terminal_handoffs
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_dispatch_terminal_handoff_update();

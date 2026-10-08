-- Cost reconciliation is serialized by the upstream billing identity across
-- every BridgeOps instance. Request-local state remains queryable on the log,
-- while every claim and outcome is copied to an append-only audit stream.

ALTER TABLE public.gateway_request_logs
    ADD COLUMN cost_reconcile_state character varying(24) NOT NULL DEFAULT 'idle',
    ADD COLUMN cost_reconcile_outcome character varying(32) NOT NULL DEFAULT 'none',
    ADD COLUMN cost_reconcile_identity_lease_id bigint,
    ADD COLUMN cost_reconcile_claim_epoch bigint NOT NULL DEFAULT 0,
    ADD COLUMN cost_reconcile_claim_token character varying(64) NOT NULL DEFAULT '',
    ADD COLUMN cost_reconcile_claimed_at timestamp with time zone,
    ADD COLUMN cost_reconcile_lease_expires_at timestamp with time zone,
    ADD COLUMN cost_reconcile_last_event_at timestamp with time zone;

UPDATE public.gateway_request_logs
   SET cost_reconcile_state = CASE
           WHEN cost_reconcile_at IS NOT NULL THEN 'pending'
           WHEN lower(btrim(COALESCE(cost_status, ''))) = 'reconciled'
               OR upstream_usage_id IS NOT NULL THEN 'succeeded'
           WHEN btrim(COALESCE(cost_reconcile_error, '')) <> '' THEN 'terminal'
           ELSE 'idle'
       END,
       cost_reconcile_outcome = CASE
           WHEN cost_reconcile_at IS NOT NULL THEN 'none'
           WHEN lower(btrim(COALESCE(cost_status, ''))) = 'reconciled'
               OR upstream_usage_id IS NOT NULL THEN 'succeeded'
           WHEN lower(btrim(COALESCE(cost_reconcile_error, ''))) =
               'upstream usage record is already assigned to another request'
               THEN 'terminal_conflict'
           WHEN lower(btrim(COALESCE(cost_reconcile_error, ''))) =
               'request pricing basis is unavailable'
               THEN 'pricing_unavailable'
           WHEN btrim(COALESCE(cost_reconcile_error, '')) <> '' THEN 'terminal_failure'
           ELSE 'none'
       END,
       cost_reconcile_last_event_at = CASE
           WHEN cost_reconcile_at IS NOT NULL
               OR upstream_usage_id IS NOT NULL
               OR btrim(COALESCE(cost_reconcile_error, '')) <> ''
               OR lower(btrim(COALESCE(cost_status, ''))) = 'reconciled'
               THEN COALESCE(created_at, CURRENT_TIMESTAMP)
           ELSE NULL
       END;

ALTER TABLE public.gateway_request_logs
    ADD CONSTRAINT chk_gateway_request_logs_cost_reconcile_state
        CHECK (cost_reconcile_state IN (
            'idle',
            'pending',
            'claimed',
            'retry_scheduled',
            'succeeded',
            'terminal'
        )),
    ADD CONSTRAINT chk_gateway_request_logs_cost_reconcile_outcome
        CHECK (cost_reconcile_outcome IN (
            'none',
            'succeeded',
            'retry',
            'conflict_retry',
            'terminal_failure',
            'terminal_conflict',
            'pricing_unavailable',
            'ineligible'
        )),
    ADD CONSTRAINT chk_gateway_request_logs_cost_reconcile_claim
        CHECK (
            (
                cost_reconcile_state = 'claimed'
                AND cost_reconcile_at IS NOT NULL
                AND cost_reconcile_identity_lease_id > 0
                AND cost_reconcile_claim_epoch > 0
                AND cost_reconcile_claim_token ~ '^[0-9a-f]{64}$'
                AND cost_reconcile_claimed_at IS NOT NULL
                AND cost_reconcile_lease_expires_at IS NOT NULL
                AND isfinite(cost_reconcile_claimed_at)
                AND isfinite(cost_reconcile_lease_expires_at)
                AND cost_reconcile_lease_expires_at > cost_reconcile_claimed_at
                AND cost_reconcile_at = cost_reconcile_lease_expires_at
                AND cost_reconcile_outcome = 'none'
            )
            OR
            (
                cost_reconcile_state <> 'claimed'
                AND cost_reconcile_identity_lease_id IS NULL
                AND cost_reconcile_claim_epoch = 0
                AND cost_reconcile_claim_token = ''
                AND cost_reconcile_claimed_at IS NULL
                AND cost_reconcile_lease_expires_at IS NULL
            )
        ),
    ADD CONSTRAINT chk_gateway_request_logs_cost_reconcile_lifecycle
        CHECK (
            (cost_reconcile_state = 'idle' AND cost_reconcile_at IS NULL AND cost_reconcile_outcome = 'none')
            OR (cost_reconcile_state = 'pending' AND cost_reconcile_at IS NOT NULL AND cost_reconcile_outcome = 'none')
            OR (cost_reconcile_state = 'claimed')
            OR (cost_reconcile_state = 'retry_scheduled' AND cost_reconcile_at IS NOT NULL
                AND cost_reconcile_outcome IN ('retry', 'conflict_retry'))
            OR (cost_reconcile_state = 'succeeded' AND cost_reconcile_at IS NULL
                AND cost_reconcile_outcome = 'succeeded')
            OR (cost_reconcile_state = 'terminal' AND cost_reconcile_at IS NULL
                AND cost_reconcile_outcome IN (
                    'terminal_failure', 'terminal_conflict', 'pricing_unavailable', 'ineligible'
                ))
        ),
    ADD CONSTRAINT chk_gateway_request_logs_cost_reconcile_event_time
        CHECK (cost_reconcile_last_event_at IS NULL OR isfinite(cost_reconcile_last_event_at));

CREATE INDEX idx_gateway_request_logs_cost_reconcile_due_identity
    ON public.gateway_request_logs (
        cost_reconcile_at,
        channel_id,
        upstream_key_id,
        id
    )
    WHERE cost_reconcile_state IN ('pending', 'claimed', 'retry_scheduled');

CREATE TABLE public.gateway_request_cost_reconcile_identity_leases (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    channel_id bigint NOT NULL,
    upstream_key_id bigint NOT NULL,
    claim_epoch bigint NOT NULL DEFAULT 0,
    claim_token character varying(64) NOT NULL DEFAULT '',
    request_log_id bigint,
    claimed_at timestamp with time zone,
    lease_expires_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_gateway_cost_reconcile_identity_lease
        UNIQUE (channel_id, upstream_key_id),
    CONSTRAINT chk_gateway_cost_reconcile_identity_lease_scope
        CHECK (channel_id > 0 AND upstream_key_id >= 0),
    CONSTRAINT chk_gateway_cost_reconcile_identity_lease_epoch
        CHECK (claim_epoch >= 0 AND claim_epoch < 9223372036854775807),
    CONSTRAINT chk_gateway_cost_reconcile_identity_lease_claim
        CHECK (
            (
                request_log_id IS NULL
                AND claim_token = ''
                AND claimed_at IS NULL
                AND lease_expires_at IS NULL
            )
            OR
            (
                request_log_id > 0
                AND claim_epoch > 0
                AND claim_token ~ '^[0-9a-f]{64}$'
                AND claimed_at IS NOT NULL
                AND lease_expires_at IS NOT NULL
                AND isfinite(claimed_at)
                AND isfinite(lease_expires_at)
                AND lease_expires_at > claimed_at
            )
        ),
    CONSTRAINT chk_gateway_cost_reconcile_identity_lease_times
        CHECK (isfinite(created_at) AND isfinite(updated_at))
);

CREATE INDEX idx_gateway_cost_reconcile_identity_lease_active
    ON public.gateway_request_cost_reconcile_identity_leases (lease_expires_at, channel_id, upstream_key_id)
    WHERE request_log_id IS NOT NULL;

CREATE TABLE public.gateway_request_cost_reconcile_events (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    request_log_id bigint NOT NULL,
    identity_lease_id bigint,
    channel_id bigint NOT NULL,
    upstream_key_id bigint NOT NULL,
    claim_epoch bigint NOT NULL DEFAULT 0,
    claim_token character varying(64) NOT NULL DEFAULT '',
    event_kind character varying(16) NOT NULL,
    state_before character varying(24) NOT NULL,
    state_after character varying(24) NOT NULL,
    outcome character varying(32) NOT NULL,
    reason_code character varying(64) NOT NULL,
    error_summary text NOT NULL DEFAULT '',
    retry_at timestamp with time zone,
    lease_expires_at timestamp with time zone,
    recorded_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_gateway_cost_reconcile_event_scope
        CHECK (request_log_id > 0 AND channel_id > 0 AND upstream_key_id >= 0),
    CONSTRAINT chk_gateway_cost_reconcile_event_claim
        CHECK (
            (
                claim_epoch = 0
                AND identity_lease_id IS NULL
                AND claim_token = ''
                AND lease_expires_at IS NULL
            )
            OR
            (
                claim_epoch > 0
                AND identity_lease_id > 0
                AND claim_token ~ '^[0-9a-f]{64}$'
            )
        ),
    CONSTRAINT chk_gateway_cost_reconcile_event_kind
        CHECK (event_kind IN ('scheduled', 'claimed', 'released', 'outcome')),
    CONSTRAINT chk_gateway_cost_reconcile_event_state_before
        CHECK (state_before IN (
            'idle', 'pending', 'claimed', 'retry_scheduled', 'succeeded', 'terminal'
        )),
    CONSTRAINT chk_gateway_cost_reconcile_event_state_after
        CHECK (state_after IN (
            'idle', 'pending', 'claimed', 'retry_scheduled', 'succeeded', 'terminal'
        )),
    CONSTRAINT chk_gateway_cost_reconcile_event_outcome
        CHECK (outcome IN (
            'none', 'succeeded', 'retry', 'conflict_retry', 'terminal_failure',
            'terminal_conflict', 'pricing_unavailable', 'ineligible'
        )),
    CONSTRAINT chk_gateway_cost_reconcile_event_reason
        CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT chk_gateway_cost_reconcile_event_times
        CHECK (
            isfinite(recorded_at)
            AND (retry_at IS NULL OR isfinite(retry_at))
            AND (lease_expires_at IS NULL OR isfinite(lease_expires_at))
        )
);

CREATE INDEX idx_gateway_cost_reconcile_event_request
    ON public.gateway_request_cost_reconcile_events (request_log_id, id);

CREATE INDEX idx_gateway_cost_reconcile_event_identity
    ON public.gateway_request_cost_reconcile_events (channel_id, upstream_key_id, id);

CREATE FUNCTION public.reject_gateway_request_cost_reconcile_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'gateway request cost reconcile events are append-only'
        USING ERRCODE = '23514';
    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_gateway_cost_reconcile_events_append_only
BEFORE UPDATE OR DELETE ON public.gateway_request_cost_reconcile_events
FOR EACH ROW
EXECUTE FUNCTION public.reject_gateway_request_cost_reconcile_event_mutation();

CREATE TRIGGER trg_gateway_cost_reconcile_events_no_truncate
BEFORE TRUNCATE ON public.gateway_request_cost_reconcile_events
FOR EACH STATEMENT
EXECUTE FUNCTION public.reject_gateway_request_cost_reconcile_event_mutation();

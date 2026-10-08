-- Freeze the reason a session-affinity route displaced normal candidates.
-- These fields are request-time evidence, not a live routing projection.
ALTER TABLE public.gateway_request_logs
    ADD COLUMN session_affinity_expires_at timestamp with time zone,
    ADD COLUMN session_affinity_overridden_candidates text NOT NULL DEFAULT '';

ALTER TABLE public.gateway_request_logs
    ADD CONSTRAINT chk_gateway_request_logs_session_affinity_expires_at
        CHECK (
            session_affinity_expires_at IS NULL
            OR isfinite(session_affinity_expires_at)
        );

ALTER TABLE public.gateway_request_logs
    ADD CONSTRAINT chk_gateway_request_logs_session_affinity_audit_origin
        CHECK (
            COALESCE(session_affinity_origin_request_id, 0) > 0
            OR (
                session_affinity_expires_at IS NULL
                AND session_affinity_overridden_candidates = ''
            )
        );

COMMENT ON COLUMN public.gateway_request_logs.session_affinity_expires_at IS
    'Frozen expiry of the affinity source route used for this request.';

COMMENT ON COLUMN public.gateway_request_logs.session_affinity_overridden_candidates IS
    'Validated JSON audit of normal-priority candidates displaced by session affinity.';

ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN session_affinity_expires_at timestamp with time zone,
    ADD COLUMN session_affinity_overridden_candidates text NOT NULL DEFAULT '';

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_session_affinity_expires_at
        CHECK (
            session_affinity_expires_at IS NULL
            OR isfinite(session_affinity_expires_at)
        );

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_session_affinity_audit_origin
        CHECK (
            session_affinity_origin_request_id > 0
            OR (
                session_affinity_expires_at IS NULL
                AND session_affinity_overridden_candidates = ''
            )
        );

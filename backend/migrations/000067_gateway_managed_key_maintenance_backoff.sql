-- Managed-key control-plane throttling is independent from channel monitoring,
-- routing health and model-probe state. A provider management API failure must
-- not rewrite channels.last_error or cool an otherwise usable inference key.
CREATE TABLE public.gateway_managed_key_maintenance_states (
    channel_id bigint PRIMARY KEY,
    channel_identity_revision bigint NOT NULL,
    failure_count integer NOT NULL DEFAULT 0,
    retry_at timestamp with time zone,
    last_failure_kind character varying(32) NOT NULL DEFAULT '',
    last_failure_status integer NOT NULL DEFAULT 0,
    last_failure_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gateway_managed_key_maintenance_channel
        FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE,
    CONSTRAINT chk_gateway_managed_key_maintenance_identity
        CHECK (channel_identity_revision > 0),
    CONSTRAINT chk_gateway_managed_key_maintenance_failure_count
        CHECK (failure_count >= 0),
    CONSTRAINT chk_gateway_managed_key_maintenance_failure_kind
        CHECK (last_failure_kind IN ('', 'http_status', 'timeout', 'recovery_pending')),
    CONSTRAINT chk_gateway_managed_key_maintenance_failure_status
        CHECK (
            (last_failure_kind = '' AND last_failure_status = 0)
            OR (last_failure_kind = 'timeout' AND last_failure_status = 0)
            OR (last_failure_kind = 'recovery_pending' AND last_failure_status = 0)
            OR (last_failure_kind = 'http_status' AND last_failure_status >= 100 AND last_failure_status <= 599)
        ),
    CONSTRAINT chk_gateway_managed_key_maintenance_retry_facts
        CHECK (
            (failure_count = 0 AND retry_at IS NULL AND last_failure_kind = '' AND last_failure_status = 0 AND last_failure_at IS NULL)
            OR
            (failure_count > 0 AND retry_at IS NOT NULL AND last_failure_kind <> '' AND last_failure_at IS NOT NULL)
        ),
    CONSTRAINT chk_gateway_managed_key_maintenance_retry_at_finite
        CHECK (retry_at IS NULL OR isfinite(retry_at)),
    CONSTRAINT chk_gateway_managed_key_maintenance_failure_at_finite
        CHECK (last_failure_at IS NULL OR isfinite(last_failure_at))
);

CREATE INDEX idx_gateway_managed_key_maintenance_retry
    ON public.gateway_managed_key_maintenance_states (retry_at, channel_id)
    WHERE retry_at IS NOT NULL;

COMMENT ON TABLE public.gateway_managed_key_maintenance_states IS
    'Identity-fenced provider management API backoff for managed upstream key inventory, recovery and provisioning; never a routing or rate-monitor gate.';

-- A transport timeout remains recovery-only because the upstream may have
-- committed the request. Only an explicit 429 rejection can authorize a later
-- same-operation POST, after a complete inventory scan proves absence again.
ALTER TABLE public.channel_api_key_operations
    ADD COLUMN rejected_post_status integer NOT NULL DEFAULT 0,
    ADD COLUMN rejected_post_at timestamp with time zone,
    ADD COLUMN rejected_retry_at timestamp with time zone,
    ADD COLUMN rejected_retry_count integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_channel_api_key_operation_rejected_post
        CHECK (
            (rejected_post_status = 0 AND rejected_post_at IS NULL)
            OR (rejected_post_status = 429 AND rejected_post_at IS NOT NULL)
        ),
    ADD CONSTRAINT chk_channel_api_key_operation_rejected_post_at_finite
        CHECK (rejected_post_at IS NULL OR isfinite(rejected_post_at)),
    ADD CONSTRAINT chk_channel_api_key_operation_rejected_retry_at_finite
        CHECK (rejected_retry_at IS NULL OR isfinite(rejected_retry_at)),
    ADD CONSTRAINT chk_channel_api_key_operation_rejected_retry_count
        CHECK (rejected_retry_count >= 0);

CREATE TABLE public.gateway_request_dispatch_owners (
    instance_id character varying(32) PRIMARY KEY,
    fence_token bigint NOT NULL,
    started_at timestamp with time zone NOT NULL,
    heartbeat_at timestamp with time zone NOT NULL,
    lease_expires_at timestamp with time zone NOT NULL,
    stopped_at timestamp with time zone,
    CONSTRAINT uq_gateway_request_dispatch_owner_fence
        UNIQUE (instance_id, fence_token),
    CONSTRAINT chk_gateway_request_dispatch_owner_instance
        CHECK (instance_id ~ '^[0-9a-f]{32}$'),
    CONSTRAINT chk_gateway_request_dispatch_owner_fence
        CHECK (fence_token > 0),
    CONSTRAINT chk_gateway_request_dispatch_owner_times
        CHECK (
            heartbeat_at >= started_at
            AND lease_expires_at > heartbeat_at
            AND (stopped_at IS NULL OR stopped_at >= started_at)
        )
);

CREATE INDEX idx_gateway_request_dispatch_owner_expiry
    ON public.gateway_request_dispatch_owners (lease_expires_at, instance_id)
    WHERE stopped_at IS NULL;

CREATE INDEX idx_gateway_request_dispatch_owner_retention
    ON public.gateway_request_dispatch_owners (COALESCE(stopped_at, lease_expires_at), instance_id);

ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN owner_instance_id character varying(32),
    ADD COLUMN owner_fence_token bigint,
    ADD COLUMN legacy_ownerless boolean NOT NULL DEFAULT false,
    ADD COLUMN recovery_requested_at timestamp with time zone,
    ADD COLUMN recovery_terminal_kind character varying(64) NOT NULL DEFAULT '';

UPDATE public.gateway_request_dispatch_audits
SET legacy_ownerless = true;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_owner_pair
        CHECK (
            (legacy_ownerless AND owner_instance_id IS NULL AND owner_fence_token IS NULL)
            OR
            (NOT legacy_ownerless AND owner_instance_id IS NOT NULL AND owner_fence_token IS NOT NULL AND owner_fence_token > 0)
        ),
    ADD CONSTRAINT chk_gateway_request_dispatch_recovery_handoff
        CHECK (
            (recovery_requested_at IS NULL AND btrim(recovery_terminal_kind) = '')
            OR
            (recovery_requested_at IS NOT NULL AND btrim(recovery_terminal_kind) <> '')
        ),
    ADD CONSTRAINT fk_gateway_request_dispatch_owner
        FOREIGN KEY (owner_instance_id, owner_fence_token)
        REFERENCES public.gateway_request_dispatch_owners(instance_id, fence_token)
        ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE INDEX idx_gateway_request_dispatch_owner_open
    ON public.gateway_request_dispatch_audits (owner_instance_id, owner_fence_token, dispatched_at, id)
    WHERE terminal_at IS NULL;

CREATE INDEX idx_gateway_request_dispatch_recovery_handoff
    ON public.gateway_request_dispatch_audits (recovery_requested_at, id)
    WHERE terminal_at IS NULL AND recovery_requested_at IS NOT NULL;

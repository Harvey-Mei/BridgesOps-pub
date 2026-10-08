ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN ownership_provenance character varying(32) NOT NULL DEFAULT 'unknown';

ALTER TABLE public.gateway_upstream_keys
    ADD CONSTRAINT chk_gateway_upstream_key_ownership_provenance
    CHECK (ownership_provenance IN ('created', 'provisioning_claim', 'adopted', 'unknown'));

ALTER TABLE public.gateway_upstream_group_changes
    ADD COLUMN removal_ownership_claim character varying(64),
    ADD COLUMN removal_channel_identity_revision bigint;

ALTER TABLE public.gateway_upstream_group_changes
    ADD CONSTRAINT chk_gateway_upstream_group_change_removal_channel_revision
    CHECK (removal_channel_identity_revision IS NULL OR removal_channel_identity_revision > 0);

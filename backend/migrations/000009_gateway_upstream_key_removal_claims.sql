ALTER TABLE public.gateway_upstream_group_changes
    ADD COLUMN removal_claim_key VARCHAR(64);

CREATE UNIQUE INDEX idx_gateway_upstream_key_removal_claim
    ON public.gateway_upstream_group_changes (removal_claim_key);

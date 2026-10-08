ALTER TABLE public.gateway_upstream_group_assignment_overrides
    ADD COLUMN presence_generation BIGINT NOT NULL DEFAULT 0;

ALTER TABLE public.gateway_upstream_group_assignment_overrides
    ADD CONSTRAINT gateway_assignment_overrides_generation_nonnegative
    CHECK (presence_generation >= 0);

COMMENT ON COLUMN public.gateway_upstream_group_assignment_overrides.presence_generation IS
    'Presence generation in which this manual override was created; zero marks a legacy override that must not be applied.';

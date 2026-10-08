-- Durable adaptive latency-probe scheduling and confirmation state.
-- The upstream group identity (id + presence_generation) is part of the
-- scope so a late probe from a removed group cannot update its replacement.
CREATE TABLE public.gateway_latency_probe_states (
    id BIGSERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES public.channels(id) ON DELETE CASCADE,
    group_name VARCHAR(128) NOT NULL,
    gateway_app VARCHAR(32) NOT NULL,
    upstream_group_id BIGINT NOT NULL DEFAULT 0,
    presence_generation BIGINT NOT NULL DEFAULT 0,
    next_probe_at TIMESTAMPTZ,
    last_probe_at TIMESTAMPTZ,
    confirmation_outcome VARCHAR(16) NOT NULL DEFAULT '',
    confirmation_count INTEGER NOT NULL DEFAULT 0,
    last_confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_latency_probe_states_app CHECK (gateway_app IN ('codex', 'claude', 'gemini')),
    CONSTRAINT gateway_latency_probe_states_group CHECK (btrim(group_name) <> ''),
    CONSTRAINT gateway_latency_probe_states_generation CHECK (upstream_group_id >= 0 AND presence_generation >= 0),
    CONSTRAINT gateway_latency_probe_states_outcome CHECK (confirmation_outcome IN ('', 'success', 'failure')),
    CONSTRAINT gateway_latency_probe_states_count CHECK (confirmation_count BETWEEN 0 AND 3),
    UNIQUE (channel_id, group_name, gateway_app, upstream_group_id, presence_generation)
);

CREATE INDEX gateway_latency_probe_states_due_idx
    ON public.gateway_latency_probe_states (next_probe_at, channel_id, gateway_app);

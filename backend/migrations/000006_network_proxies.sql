-- Channel-scoped outbound proxy endpoints. Credentials remain encrypted by the
-- application and never appear in this schema migration.
CREATE TABLE IF NOT EXISTS public.network_proxies (
    id BIGSERIAL PRIMARY KEY,
    name character varying(128) NOT NULL,
    protocol character varying(16) NOT NULL,
    host character varying(512) NOT NULL,
    port bigint NOT NULL,
    credential_cipher text,
    source character varying(32) NOT NULL DEFAULT 'manual',
    clean_ip boolean NOT NULL DEFAULT false,
    enabled boolean NOT NULL DEFAULT true,
    last_tested_at timestamp with time zone,
    last_latency_ms bigint,
    last_egress_ip character varying(64),
    last_error text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    CONSTRAINT uq_network_proxies_name UNIQUE (name)
);

ALTER TABLE public.channels
    ADD COLUMN IF NOT EXISTS network_proxy_id bigint;

CREATE INDEX IF NOT EXISTS idx_channels_network_proxy_id
    ON public.channels USING btree (network_proxy_id);
CREATE INDEX IF NOT EXISTS idx_network_proxies_enabled
    ON public.network_proxies USING btree (enabled);

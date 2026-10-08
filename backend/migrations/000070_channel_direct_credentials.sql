CREATE TABLE public.channel_direct_credentials (
    id bigserial PRIMARY KEY,
    channel_id bigint NOT NULL REFERENCES public.channels(id) ON DELETE CASCADE,
    group_name varchar(128) NOT NULL,
    base_url varchar(512) NOT NULL,
    api_key_cipher text NOT NULL,
    api_key_prefix varchar(24) NOT NULL DEFAULT '',
    models text NOT NULL DEFAULT '[]',
    ratio double precision NOT NULL DEFAULT 1,
    completion_ratio double precision NOT NULL DEFAULT 1,
    enabled boolean NOT NULL DEFAULT true,
    revision bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_channel_direct_credentials_group_name
        CHECK (btrim(group_name) <> ''),
    CONSTRAINT chk_channel_direct_credentials_ratio
        CHECK (ratio > 0 AND ratio <> 'Infinity'::double precision AND ratio <> '-Infinity'::double precision AND ratio <> 'NaN'::double precision),
    CONSTRAINT chk_channel_direct_credentials_completion_ratio
        CHECK (completion_ratio > 0 AND completion_ratio <> 'Infinity'::double precision AND completion_ratio <> '-Infinity'::double precision AND completion_ratio <> 'NaN'::double precision),
    CONSTRAINT chk_channel_direct_credentials_revision
        CHECK (revision > 0)
);

CREATE UNIQUE INDEX idx_channel_direct_credential_group
    ON public.channel_direct_credentials (channel_id, lower(btrim(group_name)));

CREATE INDEX idx_channel_direct_credentials_channel
    ON public.channel_direct_credentials (channel_id, id);

INSERT INTO public.channel_direct_credentials (
    channel_id,
    group_name,
    base_url,
    api_key_cipher,
    models,
    ratio,
    completion_ratio,
    enabled,
    revision,
    created_at,
    updated_at
)
SELECT
    id,
    'direct',
    site_url,
    password_cipher,
    direct_models,
    1,
    1,
    true,
    1,
    created_at,
    updated_at
FROM public.channels
WHERE credential_mode = 'direct'
  AND btrim(password_cipher) <> '';

ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN direct_credential_id bigint REFERENCES public.channel_direct_credentials(id) ON DELETE SET NULL,
    ADD COLUMN direct_credential_revision bigint NOT NULL DEFAULT 0,
    ADD COLUMN direct_base_url varchar(512) NOT NULL DEFAULT '',
    ADD CONSTRAINT chk_gateway_upstream_keys_direct_credential_revision
        CHECK (direct_credential_revision >= 0);

CREATE UNIQUE INDEX idx_gateway_upstream_keys_direct_credential
    ON public.gateway_upstream_keys (direct_credential_id)
    WHERE direct_credential_id IS NOT NULL;

UPDATE public.gateway_upstream_keys AS key
SET direct_credential_id = credential.id,
    direct_credential_revision = credential.revision,
    direct_base_url = credential.base_url
FROM public.channel_direct_credentials AS credential
WHERE key.channel_id = credential.channel_id
  AND lower(btrim(key.group_name)) = lower(btrim(credential.group_name))
  AND key.upstream_key_id = 0
  AND key.upstream_key_name = 'BridgeOps Direct';

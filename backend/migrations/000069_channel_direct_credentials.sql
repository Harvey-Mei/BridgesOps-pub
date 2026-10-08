DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'channels'
          AND column_name = 'credential_mode'
    ) THEN
        RAISE EXCEPTION 'migration 000069 requires channels.credential_mode';
    END IF;
END;
$$;

ALTER TABLE public.channels
    ADD COLUMN direct_models text NOT NULL DEFAULT '[]';

ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN authentication_invalidated_channel_identity_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_gateway_upstream_keys_auth_invalidation_revision_nonnegative
        CHECK (authentication_invalidated_channel_identity_revision >= 0);

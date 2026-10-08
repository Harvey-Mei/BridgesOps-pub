DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.gateway_managed_key_rebuild_intents AS intents
          LEFT JOIN public.channels AS channels ON channels.id = intents.channel_id
         WHERE channels.id IS NULL
    ) THEN
        RAISE EXCEPTION 'gateway_managed_key_rebuild_intents contains orphan channel references; repair before 000040';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.gateway_managed_key_rebuild_intents AS intents
          LEFT JOIN public.gateway_upstream_groups AS groups ON groups.id = intents.upstream_group_id
         WHERE groups.id IS NULL
            OR groups.channel_id <> intents.channel_id
    ) THEN
        RAISE EXCEPTION 'gateway_managed_key_rebuild_intents contains orphan or cross-channel upstream group references; repair before 000040';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'uq_gateway_upstream_groups_id_channel'
           AND conrelid = 'public.gateway_upstream_groups'::regclass
    ) THEN
        ALTER TABLE public.gateway_upstream_groups
            ADD CONSTRAINT uq_gateway_upstream_groups_id_channel UNIQUE (id, channel_id);
    END IF;
END $$;

ALTER TABLE public.gateway_managed_key_rebuild_intents
    ADD COLUMN claim_epoch BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN claim_digest_version SMALLINT NOT NULL DEFAULT 2,
    ADD COLUMN evidence_digest_version SMALLINT NOT NULL DEFAULT 3,
    ADD COLUMN ownership_digest_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN manual_review_reason VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN manual_review_actor VARCHAR(128) NOT NULL DEFAULT '',
    ADD COLUMN manual_review_resolution VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN manual_review_resolved_by VARCHAR(128) NOT NULL DEFAULT '',
    ADD COLUMN manual_reviewed_at TIMESTAMPTZ,
    ADD COLUMN manual_review_resolved_at TIMESTAMPTZ;

UPDATE public.gateway_managed_key_rebuild_intents
   SET manual_review_reason = 'legacy_manual_review',
       manual_review_actor = 'system_migration',
       manual_reviewed_at = COALESCE(resolved_at, updated_at, created_at),
       resolved_at = NULL
 WHERE status = 'manual_review';

ALTER TABLE public.gateway_managed_key_rebuild_intents
    ADD CONSTRAINT chk_gateway_managed_key_rebuild_claim_epoch
        CHECK (claim_epoch > 0),
    ADD CONSTRAINT chk_gateway_managed_key_rebuild_digest_versions
        CHECK (claim_digest_version > 0 AND evidence_digest_version > 0 AND ownership_digest_version > 0),
    ADD CONSTRAINT chk_gateway_managed_key_rebuild_manual_review_metadata
        CHECK (
            status <> 'manual_review'
            OR (
                manual_review_reason <> ''
                AND manual_review_actor <> ''
                AND manual_reviewed_at IS NOT NULL
                AND resolved_at IS NULL
            )
        ),
    ADD CONSTRAINT chk_gateway_managed_key_rebuild_manual_review_resolution
        CHECK (
            (
                manual_review_resolution = ''
                AND manual_review_resolved_by = ''
                AND manual_review_resolved_at IS NULL
            )
            OR (
                manual_review_resolution <> ''
                AND manual_review_resolved_by <> ''
                AND manual_review_resolved_at IS NOT NULL
            )
        ),
    ADD CONSTRAINT fk_gateway_managed_key_rebuild_channel
        FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_gateway_managed_key_rebuild_group_channel
        FOREIGN KEY (upstream_group_id, channel_id)
        REFERENCES public.gateway_upstream_groups(id, channel_id) ON DELETE CASCADE;

CREATE UNIQUE INDEX idx_gateway_managed_key_rebuild_intent_identity
    ON public.gateway_managed_key_rebuild_intents (
        channel_id,
        channel_identity_revision,
        upstream_group_id,
        presence_generation,
        local_key_id,
        upstream_key_id,
        upstream_key_name
    );

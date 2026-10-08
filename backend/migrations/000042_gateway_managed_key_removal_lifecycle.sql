DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.gateway_upstream_group_changes AS changes
          LEFT JOIN public.channels AS channels ON channels.id = changes.channel_id
         WHERE channels.id IS NULL
    ) THEN
        RAISE EXCEPTION 'gateway_upstream_group_changes contains orphan channel references; repair before 000042';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.gateway_upstream_group_changes AS changes
          LEFT JOIN public.gateway_upstream_groups AS groups ON groups.id = changes.upstream_group_id
         WHERE changes.upstream_group_id IS NOT NULL
           AND (groups.id IS NULL OR groups.channel_id <> changes.channel_id)
    ) THEN
        RAISE EXCEPTION 'gateway_upstream_group_changes contains orphan or cross-channel upstream group references; repair before 000042';
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

ALTER TABLE public.gateway_upstream_group_changes
    ADD COLUMN removal_claim_epoch BIGINT,
    ADD COLUMN removal_claim_digest_version SMALLINT,
    ADD COLUMN removal_evidence_digest_version SMALLINT,
    ADD COLUMN removal_ownership_digest_version SMALLINT,
    ADD COLUMN removal_manual_review_reason VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN removal_manual_review_actor VARCHAR(128) NOT NULL DEFAULT '',
    ADD COLUMN removal_manual_review_resolution VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN removal_manual_review_resolved_by VARCHAR(128) NOT NULL DEFAULT '',
    ADD COLUMN removal_manual_reviewed_at TIMESTAMPTZ,
    ADD COLUMN removal_manual_review_resolved_at TIMESTAMPTZ;

UPDATE public.gateway_upstream_group_changes
   SET removal_claim_epoch = 1,
       removal_claim_digest_version = 1,
       removal_evidence_digest_version = 1,
       removal_ownership_digest_version = 1
 WHERE removal_claim_key IS NOT NULL;

UPDATE public.gateway_upstream_group_changes
   SET removal_manual_review_reason = 'legacy_manual_review',
       removal_manual_review_actor = 'system_migration',
       removal_manual_reviewed_at = COALESCE(resolved_at, evaluated_at, updated_at, created_at),
       resolved_at = NULL
 WHERE kind = 'removed'
   AND removal_claim_key IS NOT NULL
   AND assignment_reason_code = 'manual_review_required';

ALTER TABLE public.gateway_upstream_group_changes
    ADD CONSTRAINT chk_gateway_upstream_group_change_removal_claim_epoch
        CHECK (removal_claim_key IS NULL OR (removal_claim_epoch IS NOT NULL AND removal_claim_epoch > 0)),
    ADD CONSTRAINT chk_gateway_upstream_group_change_removal_digest_versions
        CHECK (
            removal_claim_key IS NULL
            OR (
                removal_claim_digest_version IS NOT NULL
                AND removal_claim_digest_version > 0
                AND removal_evidence_digest_version IS NOT NULL
                AND removal_evidence_digest_version > 0
                AND removal_ownership_digest_version IS NOT NULL
                AND removal_ownership_digest_version > 0
            )
        ),
    ADD CONSTRAINT chk_gateway_upstream_group_change_removal_manual_review_metadata
        CHECK (
            NOT (
                kind = 'removed'
                AND removal_claim_key IS NOT NULL
                AND assignment_reason_code = 'manual_review_required'
            )
            OR (
                removal_manual_review_reason <> ''
                AND removal_manual_review_actor <> ''
                AND removal_manual_reviewed_at IS NOT NULL
                AND resolved_at IS NULL
            )
        ),
    ADD CONSTRAINT chk_gateway_upstream_group_change_removal_review_resolution
        CHECK (
            (
                removal_manual_review_resolution = ''
                AND removal_manual_review_resolved_by = ''
                AND removal_manual_review_resolved_at IS NULL
            )
            OR (
                removal_manual_review_resolution <> ''
                AND removal_manual_review_resolved_by <> ''
                AND removal_manual_review_resolved_at IS NOT NULL
            )
        ),
    ADD CONSTRAINT fk_gateway_upstream_group_change_channel
        FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_gateway_upstream_group_change_group_channel
        FOREIGN KEY (upstream_group_id, channel_id)
        REFERENCES public.gateway_upstream_groups(id, channel_id) ON DELETE CASCADE;

CREATE INDEX idx_gateway_upstream_key_removal_reconcile
    ON public.gateway_upstream_group_changes (
        channel_id,
        removal_channel_identity_revision,
        status,
        id
    )
    WHERE kind = 'removed' AND removal_claim_key IS NOT NULL;

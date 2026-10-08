-- Model discovery is mutable routing input. Bind every persisted observation to
-- the exact channel account and present upstream-group generation that produced
-- it so a late response from account A cannot become evidence for account B.

ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN model_facts_upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN model_facts_presence_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN model_facts_channel_identity_revision bigint NOT NULL DEFAULT 0;

-- Pre-000065 model lists and errors have no account revision. They are cache
-- state rather than audit evidence, so clear them instead of assigning them to
-- whichever account happens to be current while the migration runs.
UPDATE public.gateway_upstream_keys
SET model_families = '',
    model_ids = '',
    models_synced_at = NULL,
    models_error = '',
    model_facts_upstream_group_id = 0,
    model_facts_presence_generation = 0,
    model_facts_channel_identity_revision = 0
WHERE btrim(COALESCE(model_families, '')) <> ''
   OR btrim(COALESCE(model_ids, '')) <> ''
   OR models_synced_at IS NOT NULL
   OR btrim(COALESCE(models_error, '')) <> '';

ALTER TABLE public.gateway_upstream_keys
    ADD CONSTRAINT chk_gateway_upstream_keys_model_fact_identity
        CHECK (
            (
                model_facts_upstream_group_id = 0
                AND model_facts_presence_generation = 0
                AND model_facts_channel_identity_revision = 0
            )
            OR (
                model_facts_upstream_group_id > 0
                AND model_facts_presence_generation > 0
                AND model_facts_channel_identity_revision > 0
            )
        );

CREATE INDEX idx_gateway_upstream_keys_model_fact_identity
    ON public.gateway_upstream_keys (
        channel_id,
        model_facts_upstream_group_id,
        model_facts_presence_generation,
        model_facts_channel_identity_revision
    );

ALTER TABLE public.gateway_model_capabilities
    ADD COLUMN upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN presence_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN channel_identity_revision bigint NOT NULL DEFAULT 0;

-- Capability and context-limit observations are rebuildable and their legacy
-- account identity is unknowable. Retaining them would make a stale negative or
-- safe-input budget authoritative for a replacement account.
DELETE FROM public.gateway_model_capabilities;

ALTER TABLE public.gateway_model_capabilities
    ADD CONSTRAINT chk_gateway_model_capabilities_identity
        CHECK (
            (
                upstream_group_id = 0
                AND presence_generation = 0
                AND channel_identity_revision = 0
            )
            OR (
                upstream_group_id > 0
                AND presence_generation > 0
                AND channel_identity_revision > 0
            )
        );

CREATE INDEX idx_gateway_model_capabilities_identity
    ON public.gateway_model_capabilities (
        channel_id,
        upstream_group_id,
        presence_generation,
        channel_identity_revision,
        model_name
    );

ALTER TABLE public.gateway_upstream_groups
    ADD COLUMN model_facts_channel_identity_revision bigint NOT NULL DEFAULT 0;

-- The denormalized automatic-assignment projection was built from the same
-- unscoped key/capability facts. Reset it atomically with the source caches.
UPDATE public.gateway_upstream_groups
SET model_family_codes = '[]'::jsonb,
    model_ids = '[]'::jsonb,
    unclassified_model_ids = '[]'::jsonb,
    ambiguous_model_ids = '[]'::jsonb,
    gateway_apps = '[]'::jsonb,
    gateway_app_sources = '{}'::jsonb,
    probed_gateway_apps = '[]'::jsonb,
    capability_profile = '{}'::jsonb,
    classification_version = '',
    facts_complete = FALSE,
    incomplete_reasons = CASE
        WHEN present THEN '["model_identity_unknown_after_migration"]'::jsonb
        ELSE '["group_not_present"]'::jsonb
    END,
    facts_version = facts_version + 1,
    source_revision = '',
    models_synced_at = NULL,
    facts_built_at = NULL,
    identity_conflict = FALSE,
    model_facts_channel_identity_revision = 0;

-- Existing automatic-rule claims were materialized from the legacy unscoped
-- projection. Revoke only those claims, retain administrator-owned manual and
-- override claims, and remove a member only when no retained claim supports it.
WITH retired AS (
    UPDATE public.gateway_candidate_group_membership_claims claims
    SET active = FALSE,
        revoked_at = CURRENT_TIMESTAMP
    FROM public.gateway_upstream_groups upstream_groups
    WHERE upstream_groups.id = claims.upstream_group_id
      AND claims.source_kind = 'auto_rule'
      AND claims.active = TRUE
    RETURNING claims.candidate_group_id
)
UPDATE public.gateway_candidate_groups candidate_groups
SET membership_version = membership_version + 1
WHERE candidate_groups.id IN (SELECT DISTINCT candidate_group_id FROM retired);

DELETE FROM public.gateway_candidate_group_members members
USING public.gateway_upstream_groups upstream_groups
WHERE upstream_groups.id = members.upstream_group_id
  AND NOT EXISTS (
      SELECT 1
      FROM public.gateway_candidate_group_membership_claims claims
      WHERE claims.candidate_group_id = members.candidate_group_id
        AND claims.upstream_group_id = members.upstream_group_id
        AND claims.active = TRUE
  );

-- Keep one current-generation pending change per present group so the first
-- account-scoped model sync can evaluate rules again. Any pre-existing pending
-- change is reset because its token and fact digest were computed from legacy
-- facts; groups whose previous change was already assigned receive a new one.
WITH ranked_pending AS (
    SELECT changes.id,
           row_number() OVER (
               PARTITION BY changes.upstream_group_id, changes.presence_generation
               ORDER BY changes.id DESC
           ) AS pending_rank
    FROM public.gateway_upstream_group_changes changes
    JOIN public.gateway_upstream_groups upstream_groups
      ON upstream_groups.id = changes.upstream_group_id
     AND upstream_groups.presence_generation = changes.presence_generation
    WHERE upstream_groups.present = TRUE
      AND changes.kind = 'added'
      AND changes.status = 'pending'
)
UPDATE public.gateway_upstream_group_changes changes
SET status = CASE WHEN ranked_pending.pending_rank = 1 THEN 'pending' ELSE 'superseded' END,
    assignment_state = CASE WHEN ranked_pending.pending_rank = 1 THEN 'awaiting_facts' ELSE 'superseded' END,
    assignment_reason_code = CASE
        WHEN ranked_pending.pending_rank = 1 THEN 'model_identity_unknown_after_migration'
        ELSE 'model_identity_migration_duplicate'
    END,
    suggested_candidate_group_ids = '[]'::jsonb,
    evaluated_at = CASE WHEN ranked_pending.pending_rank = 1 THEN NULL ELSE CURRENT_TIMESTAMP END,
    evaluation_token = NULL,
    facts_digest = '',
    resolved_at = CASE WHEN ranked_pending.pending_rank = 1 THEN NULL ELSE CURRENT_TIMESTAMP END,
    updated_at = CURRENT_TIMESTAMP
FROM ranked_pending
WHERE changes.id = ranked_pending.id;

INSERT INTO public.gateway_upstream_group_changes (
    channel_id,
    group_name,
    upstream_group_id,
    presence_generation,
    kind,
    status,
    assignment_state,
    assignment_reason_code,
    suggested_candidate_group_ids,
    detected_at,
    created_at,
    updated_at
)
SELECT upstream_groups.channel_id,
       upstream_groups.canonical_group_name,
       upstream_groups.id,
       upstream_groups.presence_generation,
       'added',
       'pending',
       'awaiting_facts',
       'model_identity_unknown_after_migration',
       '[]'::jsonb,
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP
FROM public.gateway_upstream_groups upstream_groups
WHERE upstream_groups.present = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM public.gateway_upstream_group_changes changes
      WHERE changes.upstream_group_id = upstream_groups.id
        AND changes.presence_generation = upstream_groups.presence_generation
        AND changes.kind = 'added'
        AND changes.status = 'pending'
  );

ALTER TABLE public.gateway_upstream_groups
    ADD CONSTRAINT chk_gateway_upstream_groups_model_fact_identity
        CHECK (NOT facts_complete OR model_facts_channel_identity_revision > 0);

CREATE INDEX idx_gateway_upstream_groups_model_fact_identity
    ON public.gateway_upstream_groups (
        channel_id,
        model_facts_channel_identity_revision,
        present,
        id
    );

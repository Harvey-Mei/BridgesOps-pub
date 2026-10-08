DO $$
DECLARE
    invalid_channel_id BIGINT;
    invalid_group_name TEXT;
BEGIN
    SELECT channel_id, btrim(model_name)
      INTO invalid_channel_id, invalid_group_name
      FROM public.rate_snapshots
     WHERE btrim(model_name) <> ''
       AND char_length(btrim(model_name)) > 128
     ORDER BY channel_id, model_name
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'automatic group assignment migration cannot import rate_snapshots group name longer than 128 characters (channel_id=%, length=%). Rename or remove this invalid upstream group before retrying',
            invalid_channel_id, char_length(invalid_group_name);
    END IF;
END $$;

CREATE TABLE public.gateway_upstream_groups (
    id BIGSERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES public.channels(id) ON DELETE CASCADE,
    canonical_group_name VARCHAR(128) NOT NULL,
    raw_group_names JSONB NOT NULL DEFAULT '[]'::jsonb,
    present BOOLEAN NOT NULL DEFAULT TRUE,
    presence_generation BIGINT NOT NULL DEFAULT 1,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    removed_at TIMESTAMPTZ,
    model_family_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    unclassified_model_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ambiguous_model_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    gateway_apps JSONB NOT NULL DEFAULT '[]'::jsonb,
    gateway_app_sources JSONB NOT NULL DEFAULT '{}'::jsonb,
    probed_gateway_apps JSONB NOT NULL DEFAULT '[]'::jsonb,
    channel_type VARCHAR(32) NOT NULL,
    service_tier VARCHAR(64) NOT NULL DEFAULT '',
    service_tier_source VARCHAR(24) NOT NULL DEFAULT 'manual',
    capability_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    classification_version VARCHAR(64) NOT NULL DEFAULT '',
    facts_complete BOOLEAN NOT NULL DEFAULT FALSE,
    incomplete_reasons JSONB NOT NULL DEFAULT '["migration_backfill_pending"]'::jsonb,
    facts_version BIGINT NOT NULL DEFAULT 1,
    source_revision VARCHAR(64) NOT NULL DEFAULT '',
    models_synced_at TIMESTAMPTZ,
    facts_built_at TIMESTAMPTZ,
    identity_conflict BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_upstream_groups_generation_positive CHECK (presence_generation > 0),
    CONSTRAINT gateway_upstream_groups_facts_version_positive CHECK (facts_version > 0),
    CONSTRAINT gateway_upstream_groups_canonical_valid CHECK (
        canonical_group_name <> '' AND canonical_group_name = lower(btrim(canonical_group_name))
    ),
    CONSTRAINT gateway_upstream_groups_raw_names_array CHECK (jsonb_typeof(raw_group_names) = 'array'),
    CONSTRAINT gateway_upstream_groups_model_families_array CHECK (jsonb_typeof(model_family_codes) = 'array'),
    CONSTRAINT gateway_upstream_groups_model_ids_array CHECK (jsonb_typeof(model_ids) = 'array'),
    CONSTRAINT gateway_upstream_groups_unclassified_array CHECK (jsonb_typeof(unclassified_model_ids) = 'array'),
    CONSTRAINT gateway_upstream_groups_ambiguous_array CHECK (jsonb_typeof(ambiguous_model_ids) = 'array'),
    CONSTRAINT gateway_upstream_groups_apps_array CHECK (jsonb_typeof(gateway_apps) = 'array'),
    CONSTRAINT gateway_upstream_groups_probed_apps_array CHECK (jsonb_typeof(probed_gateway_apps) = 'array'),
    CONSTRAINT gateway_upstream_groups_incomplete_reasons_array CHECK (jsonb_typeof(incomplete_reasons) = 'array'),
    CONSTRAINT gateway_upstream_groups_channel_type CHECK (channel_type IN ('newapi', 'sub2api')),
    CONSTRAINT gateway_upstream_groups_service_tier CHECK (
        service_tier = '' OR service_tier ~ '^[a-z0-9][a-z0-9_-]{0,63}$'
    ),
    CONSTRAINT gateway_upstream_groups_service_tier_source CHECK (service_tier_source IN ('manual', 'provider')),
    UNIQUE (channel_id, canonical_group_name)
);

CREATE INDEX gateway_upstream_groups_present_idx
    ON public.gateway_upstream_groups (present, channel_id, canonical_group_name);

ALTER TABLE public.gateway_candidate_groups
    ADD COLUMN membership_version BIGINT NOT NULL DEFAULT 1,
    ADD CONSTRAINT gateway_candidate_groups_membership_version_positive CHECK (membership_version > 0);

ALTER TABLE public.gateway_candidate_group_members
    ADD COLUMN upstream_group_id BIGINT;

CREATE TABLE public.gateway_candidate_group_ruleset_state (
    id SMALLINT PRIMARY KEY,
    ruleset_version BIGINT NOT NULL DEFAULT 1,
    backfill_complete BOOLEAN NOT NULL DEFAULT FALSE,
    auto_execute_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_candidate_group_ruleset_singleton CHECK (id = 1),
    CONSTRAINT gateway_candidate_group_ruleset_version_positive CHECK (ruleset_version > 0)
);

INSERT INTO public.gateway_candidate_group_ruleset_state (id) VALUES (1);

CREATE TABLE public.gateway_candidate_group_auto_rules (
    id BIGSERIAL PRIMARY KEY,
    candidate_group_id BIGINT NOT NULL REFERENCES public.gateway_candidate_groups(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 0,
    model_families JSONB NOT NULL DEFAULT '[]'::jsonb,
    family_match_mode VARCHAR(24) NOT NULL DEFAULT 'single_any',
    gateway_apps JSONB NOT NULL DEFAULT '[]'::jsonb,
    app_match_mode VARCHAR(24) NOT NULL DEFAULT 'exact_set',
    accepted_app_sources JSONB NOT NULL DEFAULT '["catalog_compatible"]'::jsonb,
    channel_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    service_tiers JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_id_conditions JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_condition_quantifier VARCHAR(8) NOT NULL DEFAULT 'any',
    model_fact_coverage VARCHAR(8) NOT NULL DEFAULT 'matched',
    capability_conditions JSONB NOT NULL DEFAULT '[]'::jsonb,
    rule_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_candidate_group_auto_rules_family_mode CHECK (family_match_mode IN ('single_any', 'exact_set', 'contains_all')),
    CONSTRAINT gateway_candidate_group_auto_rules_app_mode CHECK (app_match_mode IN ('exact_set', 'contains_all')),
    CONSTRAINT gateway_candidate_group_auto_rules_quantifier CHECK (model_condition_quantifier IN ('any', 'all')),
    CONSTRAINT gateway_candidate_group_auto_rules_coverage CHECK (model_fact_coverage IN ('matched', 'all')),
    CONSTRAINT gateway_candidate_group_auto_rules_version_positive CHECK (rule_version > 0),
    CONSTRAINT gateway_candidate_group_auto_rules_json_arrays CHECK (
        jsonb_typeof(model_families) = 'array' AND
        jsonb_typeof(gateway_apps) = 'array' AND
        jsonb_typeof(accepted_app_sources) = 'array' AND
        jsonb_typeof(channel_types) = 'array' AND
        jsonb_typeof(service_tiers) = 'array' AND
        jsonb_typeof(model_id_conditions) = 'array' AND
        jsonb_typeof(capability_conditions) = 'array'
    ),
    CONSTRAINT gateway_candidate_group_auto_rules_identity_required CHECK (
        jsonb_array_length(model_families) > 0 OR
        (jsonb_array_length(model_id_conditions) > 0 AND model_fact_coverage = 'all')
    )
);

CREATE INDEX gateway_candidate_group_auto_rules_target_idx
    ON public.gateway_candidate_group_auto_rules (candidate_group_id, enabled, priority DESC, id);

CREATE TABLE public.gateway_key_group_route_policies (
    id BIGSERIAL PRIMARY KEY,
    key_group_id BIGINT NOT NULL REFERENCES public.gateway_key_groups(id) ON DELETE CASCADE,
    gateway_app VARCHAR(32) NOT NULL,
    model_family VARCHAR(32) NOT NULL,
    default_candidate_group_id BIGINT REFERENCES public.gateway_candidate_groups(id) ON DELETE RESTRICT,
    target_mode VARCHAR(24) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    policy_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_key_group_route_policies_app CHECK (gateway_app IN ('codex', 'claude', 'gemini')),
    CONSTRAINT gateway_key_group_route_policies_target CHECK (
        (target_mode = 'candidate_group' AND default_candidate_group_id IS NOT NULL) OR
        (target_mode = 'all_candidates' AND default_candidate_group_id IS NULL)
    ),
    CONSTRAINT gateway_key_group_route_policies_version_positive CHECK (policy_version > 0),
    UNIQUE (key_group_id, gateway_app, model_family)
);

ALTER TABLE public.gateway_downstream_keys
    ADD COLUMN candidate_target_mode VARCHAR(24),
    ADD COLUMN candidate_target_source VARCHAR(24),
    ADD COLUMN route_policy_id BIGINT REFERENCES public.gateway_key_group_route_policies(id) ON DELETE SET NULL,
    ADD COLUMN route_policy_version BIGINT;

UPDATE public.gateway_downstream_keys
SET candidate_target_mode = CASE WHEN candidate_group_id > 0 THEN 'candidate_group' ELSE 'all_candidates' END,
    candidate_target_source = 'legacy';

ALTER TABLE public.gateway_downstream_keys
    ALTER COLUMN candidate_target_mode SET NOT NULL,
    ALTER COLUMN candidate_target_mode SET DEFAULT 'unset',
    ALTER COLUMN candidate_target_source SET NOT NULL,
    ALTER COLUMN candidate_target_source SET DEFAULT 'manual',
    ADD CONSTRAINT gateway_downstream_keys_candidate_target_mode CHECK (candidate_target_mode IN ('candidate_group', 'all_candidates', 'unset')),
    ADD CONSTRAINT gateway_downstream_keys_candidate_target_source CHECK (candidate_target_source IN ('manual', 'key_group_policy', 'legacy')),
    ADD CONSTRAINT gateway_downstream_keys_candidate_target_consistent CHECK (
        (candidate_target_mode = 'candidate_group' AND candidate_group_id > 0) OR
        (candidate_target_mode IN ('all_candidates', 'unset') AND candidate_group_id = 0)
    );

ALTER TABLE public.gateway_upstream_group_changes
    ADD COLUMN upstream_group_id BIGINT REFERENCES public.gateway_upstream_groups(id) ON DELETE SET NULL,
    ADD COLUMN presence_generation BIGINT,
    ADD COLUMN assignment_state VARCHAR(24),
    ADD COLUMN assignment_reason_code VARCHAR(64),
    ADD COLUMN suggested_candidate_group_ids JSONB,
    ADD COLUMN evaluated_at TIMESTAMPTZ,
    ADD COLUMN evaluation_token VARCHAR(64),
    ADD COLUMN facts_digest VARCHAR(64);

CREATE TABLE public.gateway_upstream_group_assignment_overrides (
    id BIGSERIAL PRIMARY KEY,
    upstream_group_id BIGINT NOT NULL UNIQUE REFERENCES public.gateway_upstream_groups(id) ON DELETE CASCADE,
    action VARCHAR(16) NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(128) NOT NULL DEFAULT 'system',
    override_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_upstream_group_assignment_overrides_action CHECK (action IN ('assign', 'ignore')),
    CONSTRAINT gateway_upstream_group_assignment_overrides_version_positive CHECK (override_version > 0)
);

CREATE TABLE public.gateway_upstream_group_assignment_override_targets (
    id BIGSERIAL PRIMARY KEY,
    override_id BIGINT NOT NULL REFERENCES public.gateway_upstream_group_assignment_overrides(id) ON DELETE CASCADE,
    candidate_group_id BIGINT REFERENCES public.gateway_candidate_groups(id) ON DELETE SET NULL,
    candidate_group_id_snapshot BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (override_id, candidate_group_id_snapshot)
);

CREATE TABLE public.gateway_upstream_group_assignment_audits (
    id BIGSERIAL PRIMARY KEY,
    change_id BIGINT REFERENCES public.gateway_upstream_group_changes(id) ON DELETE SET NULL,
    upstream_group_id BIGINT NOT NULL REFERENCES public.gateway_upstream_groups(id) ON DELETE CASCADE,
    raw_group_name VARCHAR(128) NOT NULL,
    canonical_group_name VARCHAR(128) NOT NULL,
    presence_generation BIGINT NOT NULL,
    classification_version VARCHAR(64) NOT NULL DEFAULT '',
    facts_version BIGINT NOT NULL DEFAULT 0,
    ruleset_version BIGINT NOT NULL DEFAULT 0,
    override_version BIGINT NOT NULL DEFAULT 0,
    action VARCHAR(64) NOT NULL,
    trigger_source VARCHAR(32) NOT NULL,
    source_ref BIGINT,
    matched_rule_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_candidate_group_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    facts_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    explanation JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor VARCHAR(128) NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT gateway_upstream_group_assignment_audits_generation_positive CHECK (presence_generation > 0)
);

CREATE INDEX gateway_upstream_group_assignment_audits_group_idx
    ON public.gateway_upstream_group_assignment_audits (upstream_group_id, presence_generation, created_at DESC);

CREATE TABLE public.gateway_candidate_group_membership_claims (
    id BIGSERIAL PRIMARY KEY,
    candidate_group_id BIGINT NOT NULL REFERENCES public.gateway_candidate_groups(id) ON DELETE CASCADE,
    upstream_group_id BIGINT NOT NULL REFERENCES public.gateway_upstream_groups(id) ON DELETE CASCADE,
    presence_generation BIGINT NOT NULL,
    source_kind VARCHAR(16) NOT NULL,
    source_id BIGINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at TIMESTAMPTZ,
    CONSTRAINT gateway_candidate_group_membership_claims_source CHECK (source_kind IN ('manual', 'auto_rule', 'override')),
    CONSTRAINT gateway_candidate_group_membership_claims_generation_positive CHECK (presence_generation > 0),
    UNIQUE (candidate_group_id, upstream_group_id, presence_generation, source_kind, source_id)
);

CREATE INDEX gateway_candidate_group_membership_claims_live_idx
    ON public.gateway_candidate_group_membership_claims (upstream_group_id, presence_generation, active);

WITH current_rates AS (
    SELECT channel_id,
           lower(btrim(model_name)) AS canonical_group_name,
           MIN(NULLIF(first_seen_at, TIMESTAMPTZ '0001-01-01 00:00:00+00')) AS first_seen_at
    FROM public.rate_snapshots
    WHERE channel_id > 0 AND btrim(COALESCE(model_name, '')) <> ''
    GROUP BY channel_id, lower(btrim(model_name))
), current_key_syncs AS (
    SELECT channel_id,
           lower(btrim(group_name)) AS canonical_group_name,
           MAX(NULLIF(models_synced_at, TIMESTAMPTZ '0001-01-01 00:00:00+00')) AS models_synced_at
    FROM public.gateway_upstream_keys
    WHERE channel_id > 0 AND btrim(COALESCE(group_name, '')) <> ''
    GROUP BY channel_id, lower(btrim(group_name))
), raw_groups AS (
    SELECT channel_id, model_name AS group_name FROM public.rate_snapshots
    UNION ALL
    SELECT channel_id, group_name FROM public.gateway_upstream_keys
    UNION ALL
    SELECT channel_id, group_name FROM public.gateway_candidate_group_members
    UNION ALL
    SELECT channel_id, group_name FROM public.gateway_upstream_group_changes
), grouped AS (
    SELECT channel_id,
           lower(btrim(group_name)) AS canonical_group_name,
           to_jsonb(array_agg(DISTINCT btrim(group_name) ORDER BY btrim(group_name))) AS raw_group_names
    FROM raw_groups
    WHERE channel_id > 0 AND btrim(COALESCE(group_name, '')) <> ''
    GROUP BY channel_id, lower(btrim(group_name))
)
INSERT INTO public.gateway_upstream_groups (
    channel_id,
    canonical_group_name,
    raw_group_names,
    present,
    presence_generation,
    discovered_at,
    removed_at,
    channel_type
)
SELECT grouped.channel_id,
       grouped.canonical_group_name,
       grouped.raw_group_names,
       rates.channel_id IS NOT NULL,
       1,
       CASE WHEN rates.channel_id IS NOT NULL
            THEN COALESCE(rates.first_seen_at, key_syncs.models_synced_at, CURRENT_TIMESTAMP)
            ELSE CURRENT_TIMESTAMP
       END,
       CASE WHEN rates.channel_id IS NOT NULL THEN NULL ELSE CURRENT_TIMESTAMP END,
       channels.type
FROM grouped
JOIN public.channels ON channels.id = grouped.channel_id
LEFT JOIN current_rates rates
  ON rates.channel_id = grouped.channel_id
 AND rates.canonical_group_name = grouped.canonical_group_name
LEFT JOIN current_key_syncs key_syncs
  ON key_syncs.channel_id = grouped.channel_id
 AND key_syncs.canonical_group_name = grouped.canonical_group_name;

UPDATE public.gateway_candidate_group_members members
SET upstream_group_id = groups.id
FROM public.gateway_upstream_groups groups
WHERE groups.channel_id = members.channel_id
  AND groups.canonical_group_name = lower(btrim(members.group_name));

DELETE FROM public.gateway_candidate_group_members
WHERE upstream_group_id IS NULL;

DELETE FROM public.gateway_candidate_group_members duplicate
USING public.gateway_candidate_group_members survivor
WHERE duplicate.candidate_group_id = survivor.candidate_group_id
  AND duplicate.upstream_group_id = survivor.upstream_group_id
  AND duplicate.id > survivor.id;

UPDATE public.gateway_candidate_group_members members
SET group_name = COALESCE((
        SELECT btrim(rates.model_name)
        FROM public.rate_snapshots rates
        WHERE rates.channel_id = members.channel_id
          AND lower(btrim(rates.model_name)) = groups.canonical_group_name
        ORDER BY
            CASE WHEN btrim(rates.model_name) = btrim(members.group_name) THEN 0 ELSE 1 END,
            rates.last_seen_at DESC NULLS LAST,
            rates.id DESC
        LIMIT 1
    ), groups.canonical_group_name)
FROM public.gateway_upstream_groups groups
WHERE groups.id = members.upstream_group_id;

ALTER TABLE public.gateway_candidate_group_members
    ALTER COLUMN upstream_group_id SET NOT NULL,
    ADD CONSTRAINT gateway_candidate_group_members_upstream_group_fk
        FOREIGN KEY (upstream_group_id) REFERENCES public.gateway_upstream_groups(id) ON DELETE CASCADE;

CREATE INDEX gateway_candidate_group_members_upstream_group_idx
    ON public.gateway_candidate_group_members (upstream_group_id);

CREATE UNIQUE INDEX gateway_candidate_group_members_identity_idx
    ON public.gateway_candidate_group_members (candidate_group_id, upstream_group_id);

UPDATE public.gateway_upstream_group_changes changes
SET upstream_group_id = groups.id,
    presence_generation = groups.presence_generation
FROM public.gateway_upstream_groups groups
WHERE groups.channel_id = changes.channel_id
  AND groups.canonical_group_name = lower(btrim(changes.group_name));

UPDATE public.gateway_upstream_group_changes changes
SET status = 'superseded',
    assignment_state = 'superseded',
    assignment_reason_code = 'migration_group_not_present',
    resolved_at = COALESCE(resolved_at, CURRENT_TIMESTAMP),
    evaluated_at = CURRENT_TIMESTAMP
FROM public.gateway_upstream_groups groups
WHERE changes.upstream_group_id = groups.id
  AND changes.kind = 'added'
  AND changes.status = 'pending'
  AND groups.present = FALSE;

WITH ranked_pending AS (
    SELECT changes.id,
           row_number() OVER (
               PARTITION BY changes.upstream_group_id, changes.presence_generation
               ORDER BY changes.detected_at DESC, changes.id DESC
           ) AS generation_rank
    FROM public.gateway_upstream_group_changes changes
    JOIN public.gateway_upstream_groups groups ON groups.id = changes.upstream_group_id
    WHERE changes.kind = 'added'
      AND changes.status = 'pending'
      AND groups.present = TRUE
)
UPDATE public.gateway_upstream_group_changes changes
SET status = 'superseded',
    assignment_state = 'superseded',
    assignment_reason_code = 'migration_duplicate_pending',
    resolved_at = COALESCE(changes.resolved_at, CURRENT_TIMESTAMP),
    evaluated_at = CURRENT_TIMESTAMP
FROM ranked_pending
WHERE changes.id = ranked_pending.id
  AND ranked_pending.generation_rank > 1;

UPDATE public.gateway_upstream_group_changes
SET assignment_state = CASE status
        WHEN 'pending' THEN 'awaiting_facts'
        WHEN 'assigned' THEN 'assigned'
        WHEN 'ignored' THEN 'ignored'
        WHEN 'superseded' THEN 'superseded'
        ELSE assignment_state
    END,
    assignment_reason_code = CASE
        WHEN status = 'pending' THEN 'migration_backfill_pending'
        ELSE COALESCE(assignment_reason_code, 'legacy_resolution')
    END
WHERE kind = 'added' AND upstream_group_id IS NOT NULL;

CREATE UNIQUE INDEX gateway_upstream_group_changes_evaluation_token_idx
    ON public.gateway_upstream_group_changes (evaluation_token)
    WHERE evaluation_token IS NOT NULL;

ALTER TABLE public.gateway_upstream_group_changes
    ADD CONSTRAINT gateway_upstream_group_changes_generation_positive CHECK (
        presence_generation IS NULL OR presence_generation > 0
    ),
    ADD CONSTRAINT gateway_upstream_group_changes_assignment_state CHECK (
        assignment_state IS NULL OR assignment_state = '' OR assignment_state IN (
            'awaiting_facts', 'unmatched', 'conflict', 'ready',
            'assigned', 'ignored', 'superseded'
        )
    ),
    ADD CONSTRAINT gateway_upstream_group_changes_suggestions_array CHECK (
        suggested_candidate_group_ids IS NULL OR jsonb_typeof(suggested_candidate_group_ids) = 'array'
    );

INSERT INTO public.gateway_upstream_group_assignment_audits (
    upstream_group_id,
    raw_group_name,
    canonical_group_name,
    presence_generation,
    facts_version,
    ruleset_version,
    action,
    trigger_source,
    source_ref,
    target_candidate_group_ids,
    explanation,
    actor
)
SELECT members.upstream_group_id,
       members.group_name,
       groups.canonical_group_name,
       groups.presence_generation,
       groups.facts_version,
       1,
       'migration_manual_claim',
       'migration',
       members.id,
       jsonb_build_array(members.candidate_group_id),
       jsonb_build_object('reason', 'preserve_existing_candidate_membership'),
       'migration'
FROM public.gateway_candidate_group_members members
JOIN public.gateway_upstream_groups groups ON groups.id = members.upstream_group_id
WHERE groups.present = TRUE;

INSERT INTO public.gateway_candidate_group_membership_claims (
    candidate_group_id,
    upstream_group_id,
    presence_generation,
    source_kind,
    source_id
)
SELECT members.candidate_group_id,
       members.upstream_group_id,
       groups.presence_generation,
       'manual',
       audits.id
FROM public.gateway_candidate_group_members members
JOIN public.gateway_upstream_groups groups ON groups.id = members.upstream_group_id
JOIN public.gateway_upstream_group_assignment_audits audits
  ON audits.action = 'migration_manual_claim'
 AND audits.source_ref = members.id
WHERE groups.present = TRUE;

INSERT INTO public.gateway_upstream_group_assignment_overrides (
    upstream_group_id,
    action,
    reason,
    created_by
)
SELECT DISTINCT members.upstream_group_id,
       'assign',
       'Preserve candidate groups configured before automatic assignment',
       'migration'
FROM public.gateway_candidate_group_members members;

INSERT INTO public.gateway_upstream_group_assignment_override_targets (
    override_id,
    candidate_group_id,
    candidate_group_id_snapshot
)
SELECT overrides.id,
       members.candidate_group_id,
       members.candidate_group_id
FROM public.gateway_upstream_group_assignment_overrides overrides
JOIN public.gateway_candidate_group_members members
  ON members.upstream_group_id = overrides.upstream_group_id;

DELETE FROM public.gateway_candidate_group_members members
USING public.gateway_upstream_groups groups
WHERE groups.id = members.upstream_group_id
  AND groups.present = FALSE;

UPDATE public.gateway_candidate_groups groups
SET membership_version = 1 + (
    SELECT COUNT(*) FROM public.gateway_candidate_group_members members
    WHERE members.candidate_group_id = groups.id
);

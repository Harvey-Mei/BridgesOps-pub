-- Items 38/44: bind mutable routing health and probe evidence to the exact
-- account/group generation that produced it. Durable success delivery and
-- physical probe-budget accounting are separate from health summaries.

ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN routing_health_upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN routing_health_presence_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN routing_health_channel_identity_revision bigint NOT NULL DEFAULT 0;

-- Pre-000055 health has no account revision. A channel may already have been
-- switched from account A to B, so assigning those facts to the current row
-- would turn A's success or cooldown into B's routing state. Reset the mutable
-- projection and let the first fully fenced observation establish a new epoch.
UPDATE public.gateway_upstream_keys
SET last_used_at = NULL,
    last_routing_failure_error = '',
    last_routing_failure_kind = '',
    last_routing_failure_status = 0,
    last_routing_failure_at = NULL,
    routing_failure_count = 0,
    routing_cooldown_until = NULL,
    routing_health_upstream_group_id = 0,
    routing_health_presence_generation = 0,
    routing_health_channel_identity_revision = 0
WHERE last_used_at IS NOT NULL
   OR last_routing_failure_at IS NOT NULL
   OR routing_failure_count > 0
   OR routing_cooldown_until IS NOT NULL;

ALTER TABLE public.gateway_upstream_keys
    ADD CONSTRAINT chk_gateway_upstream_keys_routing_health_fence
        CHECK (
            (
                routing_health_upstream_group_id = 0
                AND routing_health_presence_generation = 0
                AND routing_health_channel_identity_revision = 0
            )
            OR (
                routing_health_upstream_group_id > 0
                AND routing_health_presence_generation > 0
                AND routing_health_channel_identity_revision > 0
            )
        ) NOT VALID;

ALTER TABLE public.gateway_upstream_keys
    VALIDATE CONSTRAINT chk_gateway_upstream_keys_routing_health_fence;

CREATE INDEX idx_gateway_upstream_keys_routing_health_fence
    ON public.gateway_upstream_keys (
        channel_id,
        routing_health_upstream_group_id,
        routing_health_presence_generation,
        routing_health_channel_identity_revision
    );

ALTER TABLE public.gateway_routing_failures
    ADD COLUMN upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN presence_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN channel_identity_revision bigint NOT NULL DEFAULT 0;

-- Individual adaptive failures are mutable scheduler facts, not audit rows.
-- Their account revision is unknowable, so retaining them would allow an
-- unfenced compatibility writer to merge A's window into B's health.
DELETE FROM public.gateway_routing_failures;

ALTER TABLE public.gateway_routing_failures
    ADD CONSTRAINT chk_gateway_routing_failures_identity
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

CREATE INDEX idx_gateway_routing_failures_generation_time
    ON public.gateway_routing_failures (
        upstream_group_id,
        presence_generation,
        channel_identity_revision,
        occurred_at,
        id
    );

-- Existing operator-enable audits identify a group generation but predate the
-- channel account fence. Retain them as revision-zero history; only new,
-- positive-revision audits may clear or watermark current routing health.
ALTER TABLE public.gateway_routing_enable_audits
    ADD COLUMN channel_identity_revision bigint NOT NULL DEFAULT 0,
    DROP CONSTRAINT uq_gateway_routing_enable_idempotency,
    ADD CONSTRAINT uq_gateway_routing_enable_idempotency
        UNIQUE (
            upstream_group_id,
            channel_identity_revision,
            presence_generation,
            idempotency_key
        ),
    ADD CONSTRAINT chk_gateway_routing_enable_channel_identity_revision
        CHECK (channel_identity_revision > 0) NOT VALID;

DROP INDEX public.idx_gateway_routing_enable_group_created;

CREATE INDEX idx_gateway_routing_enable_group_created
    ON public.gateway_routing_enable_audits (
        upstream_group_id,
        channel_identity_revision,
        presence_generation,
        created_at DESC
    );

ALTER TABLE public.gateway_manual_routing_cooldown_audits
    ADD COLUMN upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN presence_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN channel_identity_revision bigint NOT NULL DEFAULT 0;

UPDATE public.gateway_manual_routing_cooldown_audits AS audits
SET upstream_group_id = logs.upstream_group_id,
    presence_generation = logs.upstream_group_generation,
    channel_identity_revision = logs.channel_identity_revision
FROM public.gateway_request_logs AS logs
WHERE logs.id = audits.request_log_id
  AND logs.upstream_group_id > 0
  AND logs.upstream_group_generation > 0
  AND logs.channel_identity_revision > 0;

ALTER TABLE public.gateway_manual_routing_cooldown_audits
    ADD CONSTRAINT chk_gateway_manual_routing_cooldown_audits_identity
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

CREATE TABLE public.gateway_routing_health_success_outbox (
    dispatch_id bigint PRIMARY KEY,
    request_log_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    channel_identity_revision bigint NOT NULL,
    upstream_group_id bigint NOT NULL,
    presence_generation bigint NOT NULL,
    canonical_group_name character varying(128) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    last_error character varying(512) NOT NULL DEFAULT '',
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_gateway_routing_health_success_outbox_identity
        CHECK (
            dispatch_id > 0
            AND request_log_id > 0
            AND channel_id > 0
            AND channel_identity_revision > 0
            AND upstream_group_id > 0
            AND presence_generation > 0
            AND length(btrim(canonical_group_name)) > 0
            AND isfinite(observed_at)
        )
);

CREATE INDEX idx_gateway_routing_health_success_outbox_ready
    ON public.gateway_routing_health_success_outbox (created_at, dispatch_id);

-- A presence generation survives a channel account switch. Persist the
-- channel identity revision in the latency-probe scope so a recovery watermark
-- or next-due write from the old account cannot affect the replacement.
ALTER TABLE public.gateway_latency_probe_states
    ADD COLUMN channel_identity_revision bigint NOT NULL DEFAULT 0;

-- Scheduler state is rebuildable and, like key health, has no historical
-- account revision. Delete it instead of promoting an old recovery watermark
-- or next-due time to whichever account happens to be current at migration.
-- This also removes legacy half-scoped (id-only or generation-only) rows.
DELETE FROM public.gateway_latency_probe_states;

ALTER TABLE public.gateway_latency_probe_states
    DROP CONSTRAINT gateway_latency_probe_states_channel_id_group_name_gateway__key,
    ADD CONSTRAINT uq_gateway_latency_probe_state_identity
        UNIQUE (
            channel_id,
            channel_identity_revision,
            group_name,
            gateway_app,
            upstream_group_id,
            presence_generation
        ),
    ADD CONSTRAINT chk_gateway_latency_probe_state_identity
        CHECK (
            (
                channel_identity_revision = 0
                AND upstream_group_id = 0
                AND presence_generation = 0
            )
            OR (
                channel_identity_revision > 0
                AND upstream_group_id > 0
                AND presence_generation > 0
            )
        );

CREATE INDEX idx_gateway_latency_probe_state_identity_due
    ON public.gateway_latency_probe_states (
        channel_id,
        channel_identity_revision,
        upstream_group_id,
        presence_generation,
        next_probe_at
    );

ALTER TABLE public.model_check_targets
    ADD COLUMN upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN upstream_group_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN next_due_at timestamp with time zone;

UPDATE public.model_check_targets AS targets
SET upstream_group_id = groups.id,
    upstream_group_generation = groups.presence_generation
FROM public.gateway_upstream_groups AS groups
WHERE groups.channel_id = targets.channel_id
  AND groups.canonical_group_name = lower(btrim(targets.group_name))
  AND groups.present = TRUE;

ALTER TABLE public.model_check_targets
    ADD CONSTRAINT chk_model_check_targets_upstream_group_identity
        CHECK (
            (upstream_group_id = 0 AND upstream_group_generation = 0)
            OR (upstream_group_id > 0 AND upstream_group_generation > 0)
        );

CREATE INDEX idx_model_check_targets_generation_due
    ON public.model_check_targets (
        upstream_group_id,
        upstream_group_generation,
        enabled,
        active,
        next_due_at,
        last_checked_at,
        id
    );

ALTER TABLE public.model_check_runs
    ADD COLUMN upstream_group_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN upstream_group_generation bigint NOT NULL DEFAULT 0;

UPDATE public.model_check_runs AS runs
SET upstream_group_id = targets.upstream_group_id,
    upstream_group_generation = targets.upstream_group_generation
FROM public.model_check_targets AS targets
WHERE targets.id = runs.target_id
  AND targets.channel_id = runs.channel_id
  AND targets.upstream_account_identity_revision = runs.upstream_account_identity_revision;

ALTER TABLE public.model_check_runs
    ADD CONSTRAINT chk_model_check_runs_upstream_group_identity
        CHECK (
            (upstream_group_id = 0 AND upstream_group_generation = 0)
            OR (upstream_group_id > 0 AND upstream_group_generation > 0)
        );

CREATE INDEX idx_model_check_runs_generation_time
    ON public.model_check_runs (
        upstream_group_id,
        upstream_group_generation,
        finished_at,
        id
    );

CREATE TABLE public.model_check_probe_budget_debits (
    id bigserial PRIMARY KEY,
    attempt_id character varying(96) NOT NULL UNIQUE,
    target_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    channel_identity_revision bigint NOT NULL,
    upstream_group_id bigint NOT NULL,
    upstream_group_generation bigint NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_model_check_probe_budget_debit_identity
        CHECK (
            length(btrim(attempt_id)) > 0
            AND target_id > 0
            AND channel_id > 0
            AND channel_identity_revision > 0
            AND upstream_group_id > 0
            AND upstream_group_generation > 0
            AND isfinite(occurred_at)
        )
);

INSERT INTO public.model_check_probe_budget_debits (
    attempt_id,
    target_id,
    channel_id,
    channel_identity_revision,
    upstream_group_id,
    upstream_group_generation,
    occurred_at,
    created_at
)
SELECT 'legacy-run:' || runs.id::text || ':' || sequence.value::text,
       runs.target_id,
       runs.channel_id,
       runs.upstream_account_identity_revision,
       runs.upstream_group_id,
       runs.upstream_group_generation,
       runs.finished_at,
       COALESCE(runs.created_at, runs.finished_at, CURRENT_TIMESTAMP)
FROM public.model_check_runs AS runs
CROSS JOIN LATERAL generate_series(1, runs.request_count) AS sequence(value)
WHERE runs.request_count > 0
  AND runs.target_id > 0
  AND runs.channel_id > 0
  AND runs.upstream_account_identity_revision > 0
  AND runs.upstream_group_id > 0
  AND runs.upstream_group_generation > 0
  AND isfinite(runs.finished_at);

CREATE INDEX idx_model_check_probe_budget_debits_time
    ON public.model_check_probe_budget_debits (occurred_at, id);

CREATE TABLE public.gateway_probe_scheduler_leases (
    lease_name character varying(64) PRIMARY KEY,
    holder_id character varying(96) NOT NULL,
    fence_token bigint NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT chk_gateway_probe_scheduler_lease
        CHECK (
            length(btrim(lease_name)) > 0
            AND length(btrim(holder_id)) > 0
            AND fence_token > 0
            AND isfinite(lease_until)
            AND isfinite(updated_at)
        )
);

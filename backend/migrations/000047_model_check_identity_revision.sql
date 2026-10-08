-- 000046 is intentionally reserved for the channel-deletion epoch. Model
-- checks need their own later fence: a BridgeOps channel ID may be switched
-- from upstream account A to B while an A probe is still in flight.

ALTER TABLE public.model_check_targets
    ADD COLUMN upstream_account_identity_revision bigint NOT NULL DEFAULT 0;

ALTER TABLE public.model_check_runs
    ADD COLUMN upstream_account_identity_revision bigint NOT NULL DEFAULT 0;

-- There is no historical account revision on pre-000047 run evidence. Keep
-- those rows at zero so they cannot be displayed as B's history. A target is
-- a mutable current-status projection, so bind it to the current account but
-- clear every health/alert fact that could have belonged to A.
UPDATE public.model_check_targets AS target
SET
    upstream_account_identity_revision = channel.upstream_account_identity_revision,
    last_checked_at = NULL,
    last_status = 'pending',
    last_score = NULL,
    baseline_score = NULL,
    baseline_samples = 0,
    recent_anomalies = 0,
    consecutive_errors = 0,
    alert_active = false,
    alert_reason = '',
    alerted_at = NULL,
    recovered_at = NULL,
    last_actual_model = '',
    last_error = ''
FROM public.channels AS channel
WHERE channel.id = target.channel_id;

-- Old targets for a deleted channel, and old run history, remain immutable
-- audit rows with revision zero. NOT VALID keeps the migration safe for that
-- legacy evidence while enforcing a positive revision for all new writes.
ALTER TABLE public.model_check_targets
    ADD CONSTRAINT chk_model_check_targets_account_identity_revision_positive
    CHECK (upstream_account_identity_revision > 0) NOT VALID;

ALTER TABLE public.model_check_runs
    ADD CONSTRAINT chk_model_check_runs_account_identity_revision_positive
    CHECK (upstream_account_identity_revision > 0) NOT VALID;

CREATE INDEX idx_model_check_current_identity
    ON public.model_check_targets (
        channel_id,
        upstream_account_identity_revision,
        enabled,
        active,
        last_checked_at
    );

CREATE INDEX idx_model_check_runs_identity_time
    ON public.model_check_runs (
        target_id,
        upstream_account_identity_revision,
        finished_at DESC,
        id DESC
    );

COMMENT ON COLUMN public.model_check_targets.upstream_account_identity_revision IS
    'Current channel account identity revision for model-check status; stale rows are hidden.';
COMMENT ON COLUMN public.model_check_runs.upstream_account_identity_revision IS
    'Account identity revision used by this model-check run; zero denotes legacy unbound evidence.';

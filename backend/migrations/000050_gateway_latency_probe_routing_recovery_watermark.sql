-- A confirmed latency-probe success can recover adaptive routing health, but
-- it is not a real downstream inference success. Keep its event-time fence
-- independently from the scheduler's latest confirmation outcome so a later
-- failed/mixed probe cannot let an earlier in-flight failure recreate a
-- cooldown after restart.
ALTER TABLE public.gateway_latency_probe_states
    ADD COLUMN last_confirmed_recovery_at timestamp with time zone;

-- Existing three-success confirmations already cleared adaptive health in the
-- prior scheduler. Preserve that recovery fence when the new column appears;
-- unconfirmed, failed and non-finite historical values intentionally remain
-- absent rather than being promoted to a recovery fact.
UPDATE public.gateway_latency_probe_states
SET last_confirmed_recovery_at = last_confirmed_at
WHERE confirmation_outcome = 'success'
  AND confirmation_count = 3
  AND last_confirmed_at IS NOT NULL
  AND isfinite(last_confirmed_at);

ALTER TABLE public.gateway_latency_probe_states
    ADD CONSTRAINT chk_gateway_latency_probe_states_confirmed_recovery_at
        CHECK (
            last_confirmed_recovery_at IS NULL
            OR isfinite(last_confirmed_recovery_at)
        );

COMMENT ON COLUMN public.gateway_latency_probe_states.last_confirmed_recovery_at IS
    'Generation-scoped monotonic event-time watermark for confirmed three-success latency-probe routing recovery; distinct from real inference success.';

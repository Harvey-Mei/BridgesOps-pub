-- Persist adaptive routing isolation independently from the bounded failure
-- event window. The timestamp records when isolation was entered; it is not a
-- release deadline and may only be cleared by generation-fenced recovery.

ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN routing_isolated_at timestamp with time zone;

UPDATE public.gateway_upstream_keys AS keys
SET routing_isolated_at = COALESCE(
    (
        SELECT failures.occurred_at
        FROM public.gateway_routing_failures AS failures
        WHERE failures.channel_id = keys.channel_id
          AND lower(btrim(failures.group_name)) = lower(btrim(keys.group_name))
          AND failures.upstream_group_id = keys.routing_health_upstream_group_id
          AND failures.presence_generation = keys.routing_health_presence_generation
          AND failures.channel_identity_revision = keys.routing_health_channel_identity_revision
        ORDER BY failures.occurred_at, failures.id
        OFFSET 2
        LIMIT 1
    ),
    keys.last_routing_failure_at
)
WHERE keys.routing_failure_count >= 3
  AND keys.last_routing_failure_at IS NOT NULL
  AND (
      keys.last_routing_failure_kind IN ('preparation', 'transport', 'response_timeout')
      OR (
          keys.last_routing_failure_kind = 'http_status'
          AND (
              keys.last_routing_failure_status IN (408, 409, 425, 429)
              OR keys.last_routing_failure_status BETWEEN 500 AND 599
          )
      )
      OR (
          keys.last_routing_failure_kind = 'manual_cooldown'
          AND (
              SELECT COUNT(*)
              FROM public.gateway_routing_failures AS failures
              WHERE failures.channel_id = keys.channel_id
                AND lower(btrim(failures.group_name)) = lower(btrim(keys.group_name))
                AND failures.upstream_group_id = keys.routing_health_upstream_group_id
                AND failures.presence_generation = keys.routing_health_presence_generation
                AND failures.channel_identity_revision = keys.routing_health_channel_identity_revision
          ) >= 3
      )
  );

-- Timed cooldowns from older adaptive-routing releases are neither an
-- isolation predicate nor a recovery deadline. Rows that reached the failure
-- threshold are retained by the latch above; rows below it remain available.
-- Administrator cooldowns keep their explicit expiry and audit semantics.
UPDATE public.gateway_upstream_keys
SET routing_cooldown_until = NULL
WHERE last_routing_failure_kind IS DISTINCT FROM 'manual_cooldown'
  AND routing_cooldown_until IS NOT NULL;

ALTER TABLE public.gateway_upstream_keys
    ADD CONSTRAINT chk_gateway_upstream_keys_routing_isolation_latch
        CHECK (
            routing_isolated_at IS NULL
            OR isfinite(routing_isolated_at)
        );

COMMENT ON COLUMN public.gateway_upstream_keys.routing_isolated_at IS
    'Generation-bound adaptive routing isolation latch; not an expiry and cleared only by confirmed probe recovery, explicit enable, or identity reset.';

ALTER TABLE public.gateway_upstream_keys
    ADD COLUMN last_routing_failure_error TEXT NOT NULL DEFAULT '';

UPDATE public.gateway_upstream_keys
SET last_routing_failure_error = last_error,
    last_error = ''
WHERE last_routing_failure_at IS NOT NULL
  AND COALESCE(last_error, '') <> ''
  AND COALESCE(last_error, '') <> COALESCE(models_error, '')
  AND (
      last_error ~ '^latency probe status [0-9]{3}$'
      OR last_error LIKE 'gateway status %'
      OR last_error LIKE 'upstream response exceeded %'
      OR last_error LIKE '%context deadline exceeded%'
      OR last_error LIKE '%Client.Timeout exceeded%'
      OR last_error LIKE 'read error response:%'
      OR last_error LIKE 'latency probe response exceeded %'
  );

UPDATE public.gateway_upstream_keys
SET routing_cooldown_until = last_routing_failure_at +
    CASE routing_failure_count
        WHEN 1 THEN INTERVAL '0 minutes'
        WHEN 2 THEN INTERVAL '0 minutes'
        WHEN 3 THEN INTERVAL '0 minutes'
        WHEN 4 THEN INTERVAL '2 minutes'
        WHEN 5 THEN INTERVAL '4 minutes'
        WHEN 6 THEN INTERVAL '8 minutes'
        WHEN 7 THEN INTERVAL '16 minutes'
        ELSE INTERVAL '30 minutes'
    END
WHERE routing_failure_count > 0
  AND last_routing_failure_at IS NOT NULL;

UPDATE public.gateway_upstream_keys
SET last_error = '',
    last_routing_failure_error = ''
WHERE (last_error ~ '^latency probe status 4[0-9]{2}$'
       OR last_routing_failure_error ~ '^latency probe status 4[0-9]{2}$')
  AND COALESCE(models_error, '') NOT LIKE 'latency probe status %';

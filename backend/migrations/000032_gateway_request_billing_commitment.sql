ALTER TABLE public.gateway_request_logs
    ADD COLUMN upstream_accepted boolean NOT NULL DEFAULT false,
    ADD COLUMN billing_committed boolean NOT NULL DEFAULT false,
    ADD COLUMN cost_reconcile_legacy_pending boolean NOT NULL DEFAULT false;

-- Preserve non-successful rows that an older release had already admitted to
-- the reconciliation queue. New writers never set this marker; it only
-- prevents the first post-upgrade cleanup pass from discarding in-flight work.
UPDATE public.gateway_request_logs
   SET cost_reconcile_legacy_pending = true
 WHERE cost_reconcile_at IS NOT NULL
   AND NOT (
       status_code >= 200
       AND status_code < 400
       AND LOWER(TRIM(COALESCE(route_reason, ''))) <> 'client_canceled'
       AND TRIM(COALESCE(error, '')) = ''
   );

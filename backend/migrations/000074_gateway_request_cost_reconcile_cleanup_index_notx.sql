-- Keep historical ineligible-reconcile cleanup on a narrow, ordered partial
-- index. Terminal rows are not queue work and should never be scanned here.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gateway_request_logs_cost_reconcile_cleanup
    ON public.gateway_request_logs (id)
    WHERE cost_reconcile_at IS NOT NULL
      AND cost_reconcile_state IN ('pending', 'retry_scheduled');

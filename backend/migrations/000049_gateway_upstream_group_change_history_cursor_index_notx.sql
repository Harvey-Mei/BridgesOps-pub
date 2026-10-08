-- Item 50: keep keyset history pages bounded without blocking writes to the
-- high-churn administrative change history table during index creation.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gateway_upstream_group_changes_history_cursor
    ON public.gateway_upstream_group_changes (detected_at DESC, id DESC);

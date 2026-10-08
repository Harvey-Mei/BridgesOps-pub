-- Item 53: structured, versioned request/dispatch telemetry. Historical rows
-- intentionally remain version 0 with no trace or endpoint-latency fact; do
-- not infer those facts from old route text or final-candidate first_byte_ms.
ALTER TABLE public.gateway_request_logs
    ADD COLUMN downstream_first_visible_ms bigint,
    ADD COLUMN logical_request_trace_id character varying(32) NOT NULL DEFAULT '',
    ADD COLUMN telemetry_version integer NOT NULL DEFAULT 0,
    ADD COLUMN routing_mode character varying(16) NOT NULL DEFAULT '',
    ADD COLUMN gateway_app character varying(32) NOT NULL DEFAULT '',
    ADD COLUMN terminal_failure_code character varying(64) NOT NULL DEFAULT '',
    ADD COLUMN prior_physically_dispatched_failure_count integer NOT NULL DEFAULT 0,
    ADD COLUMN real_fallback boolean NOT NULL DEFAULT false,
    ADD COLUMN route_exclusion_snapshot_version integer NOT NULL DEFAULT 0,
    ADD COLUMN route_exclusions_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE public.gateway_request_logs
    ADD CONSTRAINT chk_gateway_request_logs_incident_telemetry
        CHECK (
            (
                telemetry_version = 0
                AND logical_request_trace_id = ''
                AND routing_mode = ''
                AND gateway_app = ''
                AND terminal_failure_code = ''
                AND downstream_first_visible_ms IS NULL
                AND prior_physically_dispatched_failure_count = 0
                AND NOT real_fallback
                AND route_exclusion_snapshot_version = 0
                AND route_exclusions_snapshot = '{}'::jsonb
            )
            OR (
                telemetry_version = 1
                AND logical_request_trace_id ~ '^[0-9a-f]{32}$'
                AND routing_mode IN ('smart', 'fixed')
                AND gateway_app IN ('codex', 'claude', 'gemini')
                AND terminal_failure_code ~ '^[a-z0-9_]*$'
                AND (downstream_first_visible_ms IS NULL OR downstream_first_visible_ms >= 0)
                AND prior_physically_dispatched_failure_count >= 0
                AND (NOT real_fallback OR routing_mode = 'smart')
                AND route_exclusion_snapshot_version = 1
                AND jsonb_typeof(route_exclusions_snapshot) = 'object'
                AND route_exclusions_snapshot @> '{"version": 1}'::jsonb
                AND route_exclusions_snapshot ? 'captured_at'
                AND route_exclusions_snapshot ? 'total_entries'
                AND route_exclusions_snapshot ? 'truncated'
                AND route_exclusions_snapshot ? 'entries'
                AND jsonb_typeof(route_exclusions_snapshot -> 'captured_at') = 'string'
                AND jsonb_typeof(route_exclusions_snapshot -> 'total_entries') = 'number'
                AND jsonb_typeof(route_exclusions_snapshot -> 'truncated') = 'boolean'
                AND jsonb_typeof(route_exclusions_snapshot -> 'entries') = 'array'
                AND jsonb_array_length(route_exclusions_snapshot -> 'entries') <= 64
                AND octet_length(route_exclusions_snapshot::text) <= 32768
            )
        ) NOT VALID;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD COLUMN logical_request_trace_id character varying(32) NOT NULL DEFAULT '',
    ADD COLUMN telemetry_version integer NOT NULL DEFAULT 0,
    ADD COLUMN routing_mode character varying(16) NOT NULL DEFAULT '',
    ADD COLUMN gateway_app character varying(32) NOT NULL DEFAULT '',
    ADD COLUMN physically_dispatched boolean NOT NULL DEFAULT false,
    ADD COLUMN first_byte_budget_ms bigint NOT NULL DEFAULT 0,
    ADD COLUMN route_exclusion_snapshot_version integer NOT NULL DEFAULT 0,
    ADD COLUMN route_exclusions_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE public.gateway_request_dispatch_audits
    ADD CONSTRAINT chk_gateway_request_dispatch_incident_telemetry
        CHECK (
            (
                telemetry_version = 0
                AND logical_request_trace_id = ''
                AND routing_mode = ''
                AND gateway_app = ''
                AND first_byte_budget_ms = 0
                AND NOT physically_dispatched
                AND route_exclusion_snapshot_version = 0
                AND route_exclusions_snapshot = '{}'::jsonb
            )
            OR (
                telemetry_version = 1
                AND logical_request_trace_id ~ '^[0-9a-f]{32}$'
                AND routing_mode IN ('smart', 'fixed')
                AND gateway_app IN ('codex', 'claude', 'gemini')
                AND first_byte_budget_ms >= 0
                AND route_exclusion_snapshot_version = 1
                AND jsonb_typeof(route_exclusions_snapshot) = 'object'
                AND route_exclusions_snapshot @> '{"version": 1}'::jsonb
                AND route_exclusions_snapshot ? 'captured_at'
                AND route_exclusions_snapshot ? 'total_entries'
                AND route_exclusions_snapshot ? 'truncated'
                AND route_exclusions_snapshot ? 'entries'
                AND jsonb_typeof(route_exclusions_snapshot -> 'captured_at') = 'string'
                AND jsonb_typeof(route_exclusions_snapshot -> 'total_entries') = 'number'
                AND jsonb_typeof(route_exclusions_snapshot -> 'truncated') = 'boolean'
                AND jsonb_typeof(route_exclusions_snapshot -> 'entries') = 'array'
                AND jsonb_array_length(route_exclusions_snapshot -> 'entries') <= 64
                AND octet_length(route_exclusions_snapshot::text) <= 32768
            )
        ) NOT VALID;

CREATE INDEX idx_gateway_request_dispatch_trace
    ON public.gateway_request_dispatch_audits (logical_request_trace_id, dispatched_at, id);

CREATE FUNCTION public.reject_gateway_route_exclusion_snapshot_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.route_exclusion_snapshot_version IS DISTINCT FROM OLD.route_exclusion_snapshot_version
       OR NEW.route_exclusions_snapshot IS DISTINCT FROM OLD.route_exclusions_snapshot THEN
        RAISE EXCEPTION 'gateway route exclusion snapshot is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_gateway_request_logs_route_exclusion_snapshot_immutable
    BEFORE UPDATE OF route_exclusion_snapshot_version, route_exclusions_snapshot
    ON public.gateway_request_logs
    FOR EACH ROW
    EXECUTE FUNCTION public.reject_gateway_route_exclusion_snapshot_update();

CREATE TRIGGER trg_gateway_request_dispatch_route_exclusion_snapshot_immutable
    BEFORE UPDATE OF route_exclusion_snapshot_version, route_exclusions_snapshot
    ON public.gateway_request_dispatch_audits
    FOR EACH ROW
    EXECUTE FUNCTION public.reject_gateway_route_exclusion_snapshot_update();

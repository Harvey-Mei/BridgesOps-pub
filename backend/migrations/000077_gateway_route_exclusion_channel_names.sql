-- Route-exclusion snapshot v2 freezes channel names so request details do not
-- depend on mutable channel records. Keep v1 readable for historical rows.
ALTER TABLE public.gateway_request_logs
    DROP CONSTRAINT chk_gateway_request_logs_incident_telemetry;

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
                AND (
                    (
                        route_exclusion_snapshot_version = 1
                        AND route_exclusions_snapshot @> '{"version": 1}'::jsonb
                    )
                    OR (
                        route_exclusion_snapshot_version = 2
                        AND route_exclusions_snapshot @> '{"version": 2}'::jsonb
                    )
                )
                AND jsonb_typeof(route_exclusions_snapshot) = 'object'
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
    DROP CONSTRAINT chk_gateway_request_dispatch_incident_telemetry;

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
                AND (
                    (
                        route_exclusion_snapshot_version = 1
                        AND route_exclusions_snapshot @> '{"version": 1}'::jsonb
                    )
                    OR (
                        route_exclusion_snapshot_version = 2
                        AND route_exclusions_snapshot @> '{"version": 2}'::jsonb
                    )
                )
                AND jsonb_typeof(route_exclusions_snapshot) = 'object'
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

package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestIncidentTelemetryMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000043_gateway_request_incident_telemetry.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column downstream_first_visible_ms bigint",
		"add column logical_request_trace_id character varying(32) not null default ''",
		"add column telemetry_version integer not null default 0",
		"routing_mode character varying(16)",
		"gateway_app character varying(32)",
		"terminal_failure_code character varying(64)",
		"prior_physically_dispatched_failure_count integer",
		"real_fallback boolean",
		"physically_dispatched boolean",
		"first_byte_budget_ms bigint",
		"route_exclusion_snapshot_version integer not null default 0",
		"route_exclusions_snapshot jsonb not null default '{}'::jsonb",
		"jsonb_array_length(route_exclusions_snapshot -> 'entries') <= 64",
		"reject_gateway_route_exclusion_snapshot_update",
		"gateway route exclusion snapshot is immutable",
		"idx_gateway_request_dispatch_trace",
		"logical_request_trace_id, dispatched_at, id",
		"not valid",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("incident telemetry migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"request_body", "authorization_header", "raw_session_id", "upstream_response"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("incident telemetry migration persisted forbidden field %q", forbidden)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 43 {
			continue
		}
		if migration.name != "000043_gateway_request_incident_telemetry.sql" || migration.nonTransactional {
			t.Fatalf("version 43 incident telemetry migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 43 incident telemetry migration was not loaded")
}

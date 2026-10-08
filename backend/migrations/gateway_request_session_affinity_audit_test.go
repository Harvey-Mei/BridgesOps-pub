package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestSessionAffinityAuditMigrationFreezesRequestAndDispatchEvidence(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000075_gateway_request_session_affinity_audit.sql")
	if err != nil {
		t.Fatalf("read session affinity audit migration: %v", err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"alter table public.gateway_request_logs",
		"alter table public.gateway_request_dispatch_audits",
		"session_affinity_expires_at timestamp with time zone",
		"session_affinity_overridden_candidates text not null default ''",
		"session_affinity_origin_request_id, 0) > 0",
		"session_affinity_origin_request_id > 0",
		"isfinite(session_affinity_expires_at)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("session affinity audit migration missing %q", required)
		}
	}
}

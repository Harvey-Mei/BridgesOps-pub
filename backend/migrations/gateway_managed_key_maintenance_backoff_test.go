package migrations

import (
	"strings"
	"testing"
)

func TestGatewayManagedKeyMaintenanceBackoffMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000067_gateway_managed_key_maintenance_backoff.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.gateway_managed_key_maintenance_states",
		"foreign key (channel_id) references public.channels(id) on delete cascade",
		"channel_identity_revision bigint not null",
		"last_failure_kind in ('', 'http_status', 'timeout', 'recovery_pending')",
		"last_failure_kind = 'timeout' and last_failure_status = 0",
		"last_failure_kind = 'http_status' and last_failure_status >= 100",
		"idx_gateway_managed_key_maintenance_retry",
		"alter table public.channel_api_key_operations",
		"rejected_post_status = 429 and rejected_post_at is not null",
		"rejected_retry_count integer not null default 0",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("managed-key maintenance migration missing %q", required)
		}
	}
	if strings.Contains(sql, "alter table public.channels") {
		t.Fatal("managed-key maintenance backoff mutates channel monitoring state")
	}
}

package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestBillingCommitmentMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000032_gateway_request_billing_commitment.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"alter table public.gateway_request_logs",
		"add column upstream_accepted boolean not null default false",
		"add column billing_committed boolean not null default false",
		"add column cost_reconcile_legacy_pending boolean not null default false",
		"set cost_reconcile_legacy_pending = true",
		"where cost_reconcile_at is not null",
		"status_code >= 200",
		"status_code < 400",
		"lower(trim(coalesce(route_reason, ''))) <> 'client_canceled'",
		"trim(coalesce(error, '')) = ''",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("request billing commitment migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 32 {
			continue
		}
		if migration.name != "000032_gateway_request_billing_commitment.sql" || migration.nonTransactional {
			t.Fatalf("version 32 request billing commitment migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 32 request billing commitment migration was not loaded")
}

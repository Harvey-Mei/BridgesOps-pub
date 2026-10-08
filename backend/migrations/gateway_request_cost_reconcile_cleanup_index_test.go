package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestCostReconcileCleanupIndexMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000074_gateway_request_cost_reconcile_cleanup_index_notx.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create index concurrently if not exists idx_gateway_request_logs_cost_reconcile_cleanup",
		"on public.gateway_request_logs (id)",
		"cost_reconcile_state in ('pending', 'retry_scheduled')",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("cleanup index migration missing %q", required)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 74 {
			continue
		}
		if migration.name != "000074_gateway_request_cost_reconcile_cleanup_index_notx.sql" || !migration.nonTransactional {
			t.Fatalf("version 74 cleanup index migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 74 cleanup index migration was not loaded")
}

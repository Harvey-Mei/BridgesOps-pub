package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestDispatchDurableHandoffMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000053_gateway_request_dispatch_durable_handoff.sql")
	if err != nil {
		t.Fatalf("read durable dispatch handoff migration: %v", err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"transport_fence_version",
		"transport_state",
		"rollover_predecessor_id",
		"prior_physically_dispatched_failure_count",
		"'prepared'",
		"'armed'",
		"'confirmed'",
		"'not_dispatched'",
		"'manual_review'",
		"gateway_request_dispatch_terminal_handoffs",
		"log_snapshot jsonb",
		"facts_snapshot jsonb",
		"primary key",
		"on delete cascade",
		"drain_forced_at",
		"drain_forced_bindings",
		"drain_forced_handoffs",
		"chk_gateway_request_dispatch_owner_forced_drain",
		"trg_gateway_request_dispatch_owner_forced_drain",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("durable dispatch handoff migration missing %q", required)
		}
	}
	if !strings.Contains(sql, "not valid") {
		t.Fatal("transport fence constraint must preserve an isolated validation gate")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version == 53 {
			if migration.name != "000053_gateway_request_dispatch_durable_handoff.sql" || migration.nonTransactional {
				t.Fatalf("version 53 durable handoff migration = %#v", migration)
			}
			return
		}
	}
	t.Fatal("version 53 durable handoff migration was not loaded")
}

package migrations

import (
	"strings"
	"testing"
)

func TestGatewayManualRoutingCooldownMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000031_gateway_manual_routing_cooldown_audits.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, required := range []string{
		"CREATE TABLE public.gateway_manual_routing_cooldown_audits",
		"request_first_byte_ms bigint NOT NULL",
		"request_created_at timestamp with time zone NOT NULL",
		"idempotency_key character varying(128) NOT NULL",
		"CHECK (request_first_byte_ms > 30000)",
		"UNIQUE (request_log_id, idempotency_key)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("manual cooldown migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 31 {
			continue
		}
		if migration.name != "000031_gateway_manual_routing_cooldown_audits.sql" || migration.nonTransactional {
			t.Fatalf("version 31 manual cooldown migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 31 manual cooldown migration was not loaded")
}

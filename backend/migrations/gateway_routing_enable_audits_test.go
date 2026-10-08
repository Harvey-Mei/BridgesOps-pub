package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRoutingEnableAuditMigrationIsAppendOnlyEvidence(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000034_gateway_routing_enable_audits.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, required := range []string{
		"CREATE TABLE public.gateway_routing_enable_audits",
		"upstream_group_id bigint NOT NULL",
		"presence_generation bigint NOT NULL",
		"actor character varying(128) NOT NULL",
		"previous_cooldown_until timestamp with time zone",
		"UNIQUE (upstream_group_id, presence_generation, idempotency_key)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("routing enable migration missing %q", required)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, migration := range migrations {
		if migration.name == "000034_gateway_routing_enable_audits.sql" {
			found = true
			if migration.nonTransactional {
				t.Fatal("routing enable audit migration must be transactional")
			}
		}
	}
	if !found {
		t.Fatal("routing enable audit migration was not loaded")
	}
}

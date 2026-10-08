package migrations

import (
	"strings"
	"testing"
)

func TestGatewayLatencyProbeRoutingRecoveryWatermarkMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000050_gateway_latency_probe_routing_recovery_watermark.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"alter table public.gateway_latency_probe_states",
		"add column last_confirmed_recovery_at timestamp with time zone",
		"set last_confirmed_recovery_at = last_confirmed_at",
		"confirmation_outcome = 'success'",
		"confirmation_count = 3",
		"chk_gateway_latency_probe_states_confirmed_recovery_at",
		"isfinite(last_confirmed_recovery_at)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("routing recovery watermark migration missing %q", required)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.name == "000050_gateway_latency_probe_routing_recovery_watermark.sql" {
			if migration.nonTransactional {
				t.Fatal("routing recovery watermark migration must be transactional")
			}
			return
		}
	}
	t.Fatal("routing recovery watermark migration was not loaded")
}

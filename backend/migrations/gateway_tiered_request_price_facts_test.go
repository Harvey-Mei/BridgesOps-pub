package migrations

import (
	"strings"
	"testing"
)

func TestGatewayTieredRequestPriceFactsMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000080_gateway_tiered_request_price_facts.sql")
	if err != nil {
		t.Fatal(err)
	}
	compactSQL := strings.Join(strings.Fields(strings.ToLower(string(body))), " ")
	for _, required := range []string{
		"add column long_context_threshold_tokens bigint",
		"add column long_context_input_unit_price double precision",
		"add column long_context_output_unit_price double precision",
		"add column long_context_cache_read_unit_price double precision",
		"add column long_context_cache_write_unit_price double precision",
		"add column group_ratio double precision",
		"add column completion_ratio double precision",
		"add column price_long_context_threshold_tokens bigint",
		"schema_version in (1, 2)",
		"long_context_threshold_tokens > 0",
		"long_context_threshold_tokens < 9223372036854775807",
		"price_long_context_threshold_tokens > 0",
		"price_long_context_threshold_tokens < 9223372036854775807",
		"fact.group_ratio is not distinct from new.group_ratio",
		"fact.completion_ratio is not distinct from new.completion_ratio",
		"request_log.group_ratio is not distinct from new.group_ratio",
		"request_log.completion_ratio is not distinct from new.completion_ratio",
		"new.price_long_context_cache_write_unit is distinct from old.price_long_context_cache_write_unit",
		"gateway_request_price_fact_snapshot_consistency",
		"gateway_request_dispatch_final_price_snapshot_consistency",
	} {
		if !strings.Contains(compactSQL, required) {
			t.Fatalf("tiered request-price migration missing %q", required)
		}
	}
	if strings.Contains(compactSQL, "long_context_threshold_tokens = 272000") {
		t.Fatal("tiered request-price facts must preserve provider-published reachable thresholds")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 80 {
			continue
		}
		if migration.name != "000080_gateway_tiered_request_price_facts.sql" || migration.nonTransactional {
			t.Fatalf("version 80 tiered request price migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 80 tiered request price migration was not loaded")
}

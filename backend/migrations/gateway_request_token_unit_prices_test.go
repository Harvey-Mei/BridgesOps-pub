package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayRequestTokenUnitPriceMigrationAddsComponentColumns(t *testing.T) {
	raw, err := os.ReadFile("000025_gateway_request_token_unit_prices.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, column := range []string{
		"performance_input_tokens",
		"performance_output_tokens",
		"performance_input_priced_tokens",
		"performance_output_priced_tokens",
		"performance_cache_creation_tokens",
		"performance_cache_read_tokens",
		"performance_input_cost",
		"performance_output_cost",
	} {
		if !strings.Contains(sql, "add column if not exists "+column) {
			t.Fatalf("token unit-price migration missing %q", column)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version == 25 {
			if migration.name != "000025_gateway_request_token_unit_prices.sql" || migration.nonTransactional {
				t.Fatalf("version 25 migration = %#v", migration)
			}
			return
		}
	}
	t.Fatal("version 25 token unit-price migration was not loaded")
}

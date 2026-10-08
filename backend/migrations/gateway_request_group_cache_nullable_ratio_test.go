package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestGroupCacheNullableRatioMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000058_gateway_request_group_cache_nullable_ratio.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"alter column version set default 6",
		"alter column target_version set default 6",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("group-cache nullable-ratio migration missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"delete from public.gateway_request_group_cache_aggregates",
		"update public.gateway_request_logs",
		"update public.gateway_request_group_cache_backfill_state",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("group-cache nullable-ratio migration performs synchronous rebuild step %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 58 {
			continue
		}
		if migration.name != "000058_gateway_request_group_cache_nullable_ratio.sql" || migration.nonTransactional {
			t.Fatalf("version 58 group-cache nullable-ratio migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 58 group-cache nullable-ratio migration was not loaded")
}

package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayRequestGroupCacheExactlyOnceMigrations(t *testing.T) {
	markerRaw, err := os.ReadFile("000022_gateway_request_group_cache_exactly_once_notx.sql")
	if err != nil {
		t.Fatal(err)
	}
	markerSQL := strings.ToLower(string(markerRaw))
	if !strings.Contains(markerSQL, "add column if not exists group_cache_aggregate_version bigint not null default 0") {
		t.Fatal("marker migration does not add the per-request marker")
	}
	indexRaw, err := os.ReadFile("000023_gateway_request_group_cache_exactly_once_index_notx.sql")
	if err != nil {
		t.Fatal(err)
	}
	indexSQL := strings.ToLower(string(indexRaw))
	if !strings.Contains(indexSQL, "create index concurrently if not exists idx_gateway_logs_group_cache_backfill") {
		t.Fatal("marker index migration must build the index concurrently")
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]struct {
		name             string
		nonTransactional bool
	}{
		22: {name: "000022_gateway_request_group_cache_exactly_once_notx.sql", nonTransactional: true},
		23: {name: "000023_gateway_request_group_cache_exactly_once_index_notx.sql", nonTransactional: true},
	}
	for _, migration := range migrations {
		expected, ok := want[migration.version]
		if !ok {
			continue
		}
		if migration.name != expected.name || migration.nonTransactional != expected.nonTransactional {
			t.Fatalf("migration %d = (%q, nonTransactional=%t), want (%q, nonTransactional=%t)", migration.version, migration.name, migration.nonTransactional, expected.name, expected.nonTransactional)
		}
		delete(want, migration.version)
	}
	if len(want) != 0 {
		t.Fatalf("exactly-once migrations missing: %v", want)
	}
}

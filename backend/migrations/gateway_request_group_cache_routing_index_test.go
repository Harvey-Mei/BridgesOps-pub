package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayRequestGroupCacheRoutingMigrations(t *testing.T) {
	schemaRaw, err := os.ReadFile("000026_gateway_request_group_cache_routing.sql")
	if err != nil {
		t.Fatal(err)
	}
	schemaSQL := strings.ToLower(string(schemaRaw))
	for _, required := range []string{
		"add column if not exists last_normal_use_at timestamp with time zone not null",
		"alter column version set default 5",
		"alter column target_version set default 5",
	} {
		if !strings.Contains(schemaSQL, required) {
			t.Fatalf("group-cache routing schema migration missing %q", required)
		}
	}
	if strings.Contains(schemaSQL, "concurrently") {
		t.Fatal("transactional group-cache routing schema migration contains a concurrent index")
	}
	indexRaw, err := os.ReadFile("000027_gateway_request_group_cache_routing_index_notx.sql")
	if err != nil {
		t.Fatal(err)
	}
	indexSQL := strings.ToLower(string(indexRaw))
	for _, required := range []string{
		"create index concurrently if not exists idx_gateway_request_group_cache_routing_window",
		"version,",
		"(lower(btrim(group_name)))",
		"bucket_start desc",
		"last_normal_use_at desc",
		"include (prompt_tokens, cache_read_tokens)",
	} {
		if !strings.Contains(indexSQL, required) {
			t.Fatalf("group-cache routing index migration missing %q", required)
		}
	}
	if strings.Contains(indexSQL, "where prompt_tokens > 0") {
		t.Fatal("routing index must cover version readiness rows even when prompt tokens are zero")
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]struct {
		name             string
		nonTransactional bool
	}{
		26: {name: "000026_gateway_request_group_cache_routing.sql"},
		27: {name: "000027_gateway_request_group_cache_routing_index_notx.sql", nonTransactional: true},
	}
	for _, migration := range migrations {
		expected, ok := want[migration.version]
		if !ok {
			continue
		}
		if migration.name != expected.name || migration.nonTransactional != expected.nonTransactional {
			t.Fatalf("version %d migration = (%q, nonTransactional=%t)", migration.version, migration.name, migration.nonTransactional)
		}
		delete(want, migration.version)
	}
	if len(want) != 0 {
		t.Fatalf("group-cache routing migrations missing: %v", want)
	}
}

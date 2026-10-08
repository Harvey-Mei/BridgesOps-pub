package migrations

import (
	"strings"
	"testing"
)

func TestGatewayUpstreamGroupChangeHistoryCursorIndexMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000049_gateway_upstream_group_change_history_cursor_index_notx.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create index concurrently if not exists idx_gateway_upstream_group_changes_history_cursor",
		"on public.gateway_upstream_group_changes (detected_at desc, id desc)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("history cursor index migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 49 {
			continue
		}
		if migration.name != "000049_gateway_upstream_group_change_history_cursor_index_notx.sql" || !migration.nonTransactional {
			t.Fatalf("version 49 history cursor index migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 49 history cursor index migration was not loaded")
}

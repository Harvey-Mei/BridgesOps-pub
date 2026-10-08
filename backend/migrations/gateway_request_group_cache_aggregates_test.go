package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayRequestGroupCacheAggregatesMigrationIsTransactional(t *testing.T) {
	raw, err := os.ReadFile("000024_gateway_request_group_cache_aggregates.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"create table if not exists public.gateway_request_group_cache_aggregates",
		"create unique index idx_gateway_request_group_cache_aggregates_key",
		"create index idx_gateway_request_group_cache_aggregates_filter",
		"model_name character varying(256) not null",
		"create table if not exists public.gateway_request_group_cache_backfill_state",
		"last_request_id bigint not null default 0",
		"insert into public.gateway_request_group_cache_backfill_state",
		"gateway_request_aggregates",
		"column_name = 'group_name'",
		"delete from public.gateway_request_aggregates",
		"set aggregate_version = 0",
		"drop index if exists public.idx_gateway_request_aggregate_key",
		"create unique index idx_gateway_request_aggregate_key",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("group cache migration missing %q", required)
		}
	}
	if strings.Contains(sql, "concurrently") {
		t.Fatal("group cache migration must not use CREATE INDEX CONCURRENTLY inside a transaction")
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version == 24 {
			if migration.name != "000024_gateway_request_group_cache_aggregates.sql" {
				t.Fatalf("version 24 migration name = %q", migration.name)
			}
			if migration.nonTransactional {
				t.Fatal("version 24 group cache migration must be transactional")
			}
			return
		}
	}
	t.Fatal("version 24 group cache migration was not loaded")
}

package migrations

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestChannelCostUnitRequestRestatementMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000060_channel_cost_unit_request_restatements.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, required := range []string{
		"migration 000060 requires migration 000059 and request aggregate schemas",
		"('gateway_request_logs', 'price_fact_id')",
		"CREATE TABLE public.channel_cost_unit_request_restatements",
		"CREATE TABLE public.channel_cost_unit_request_restatement_items",
		"request_log_high_water bigint NOT NULL",
		"cost_sampled_at_before timestamp with time zone NOT NULL",
		"cost_sampled_at_after timestamp with time zone NOT NULL",
		"affected_started_at timestamp with time zone NOT NULL",
		"affected_ended_at timestamp with time zone NOT NULL",
		"cost_sampled_at_before < affected_started_at",
		"affected_ended_at <= cost_sampled_at_after",
		"old_funding_factor numeric NOT NULL",
		"new_funding_factor numeric NOT NULL",
		"expected_population_rows bigint NOT NULL",
		"expected_restatement_rows bigint NOT NULL",
		"expected_latency_probe_rows bigint NOT NULL",
		"expected_raw_cost_sum numeric NOT NULL",
		"expected_recorded_cost_sum numeric NOT NULL",
		"expected_corrected_cost_sum numeric NOT NULL",
		"expected_undercount_sum numeric NOT NULL",
		"expected_bucket_starts jsonb NOT NULL",
		"expected_bucket_digest character varying(64) NOT NULL",
		"UNIQUE (channel_id, channel_identity_revision, idempotency_key)",
		"UNIQUE (restatement_id, request_log_id)",
		"CREATE FUNCTION public.reject_channel_cost_unit_request_restatement_mutation()",
		"BEFORE UPDATE OR DELETE ON public.channel_cost_unit_request_restatements",
		"BEFORE TRUNCATE ON public.channel_cost_unit_request_restatements",
		"BEFORE UPDATE OR DELETE ON public.channel_cost_unit_request_restatement_items",
		"BEFORE TRUNCATE ON public.channel_cost_unit_request_restatement_items",
		"USING ERRCODE = '23514'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("channel request restatement migration missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"UPDATE public.gateway_request_logs",
		"DELETE FROM public.gateway_request_aggregates",
		"DELETE FROM public.gateway_request_group_cache_aggregates",
		"ON DELETE CASCADE",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 60 must define audit schema without applying a correction: found %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 60 {
			continue
		}
		if migration.name != "000060_channel_cost_unit_request_restatements.sql" || migration.nonTransactional {
			t.Fatalf("version 60 request restatement migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 60 request restatement migration was not loaded")
}

func TestChannelCostUnitRequestRestatementMigrationRequiresPriceFactFence(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThroughVersion(t, 59), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through version 59: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE public.gateway_request_logs DROP COLUMN price_fact_id CASCADE`); err != nil {
		t.Fatalf("remove price-fact fence fixture: %v", err)
	}
	body, err := embeddedMigrations.ReadFile("000060_channel_cost_unit_request_restatements.sql")
	if err != nil {
		t.Fatal(err)
	}
	err = applyMigrations(ctx, db, fstest.MapFS{
		"000060_channel_cost_unit_request_restatements.sql": &fstest.MapFile{Data: body},
	}, ".", defaultRunnerConfig())
	if err == nil || !strings.Contains(err.Error(), "migration 000060 requires migration 000059 and request aggregate schemas") {
		t.Fatalf("migration 60 without price_fact_id error = %v", err)
	}
	var tableName sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.channel_cost_unit_request_restatements')::text`).Scan(&tableName); err != nil {
		t.Fatal(err)
	}
	if tableName.Valid {
		t.Fatalf("failed dependency guard left request restatement table %q behind", tableName.String)
	}
}

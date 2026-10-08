package migrations

import (
	"strings"
	"testing"
)

func TestGatewayModelPricesMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000079_gateway_model_prices.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(body))
	compactSQL := strings.Join(strings.Fields(sqlText), " ")
	for _, required := range []string{
		"create table public.gateway_model_prices",
		"model_name character varying(256) not null",
		"input_unit_price_usd double precision not null",
		"output_unit_price_usd double precision not null",
		"cache_read_unit_price_usd double precision",
		"cache_write_unit_price_usd double precision",
		"constraint uq_gateway_model_prices_model_name unique (model_name)",
		"input_unit_price_usd::text not in ('nan', 'infinity', '-infinity')",
		"cache_write_unit_price_usd::text not in ('nan', 'infinity', '-infinity')",
		"audit.cache_read_unit_price_usd is not distinct from new.cache_read_unit_price_usd",
		"long_context_threshold_tokens = 272000",
		"constraint chk_gateway_model_prices_revision check (revision > 0)",
		"create table public.gateway_model_price_source_watermarks",
		"primary key (model_name, source)",
		"source = 'litellm'",
		"source_checksum ~ '^[0-9a-f]{64}$'",
		"isfinite(source_observed_at)",
		"create table public.gateway_model_price_audits",
		"constraint uq_gateway_model_price_audits_revision unique (model_price_id, revision)",
		"transaction_id bigint not null default txid_current()",
		"check (action in ('create', 'update', 'delete'))",
		"(action = 'create' and revision = 1)",
		"(action in ('update', 'delete') and revision > 1)",
		"create function public.reject_gateway_model_price_audit_mutation()",
		"gateway model price audits are append-only",
		"using errcode = '23514'",
		"create function public.stamp_gateway_model_price_audit_transaction()",
		"new.transaction_id := txid_current()",
		"before insert on public.gateway_model_price_audits",
		"before update or delete on public.gateway_model_price_audits",
		"before truncate on public.gateway_model_price_audits",
		"create function public.require_gateway_model_price_audit()",
		"gateway model price catalog mutation requires a matching audit",
		"create constraint trigger trg_gateway_model_prices_require_audit",
		"after insert or update or delete on public.gateway_model_prices",
		"deferrable initially deferred",
		"audit.transaction_id = txid_current()",
		"new.id <> old.id",
		"new.revision <> old.revision + 1",
		"new.created_at <> old.created_at",
		"create function public.require_gateway_model_price_catalog_transition()",
		"gateway model price audit requires its catalog transition",
		"create constraint trigger trg_gateway_model_price_audits_require_transition",
		"after insert on public.gateway_model_price_audits",
		"create function public.reject_gateway_model_price_catalog_truncate()",
		"before truncate on public.gateway_model_prices",
	} {
		if !strings.Contains(compactSQL, required) {
			t.Fatalf("model prices migration missing %q", required)
		}
	}
	if strings.Contains(sqlText, "references public.gateway_model_prices") {
		t.Fatal("model price audit history must survive catalog row deletion")
	}
	if strings.Contains(compactSQL, "insert into public.gateway_model_prices") ||
		strings.Contains(compactSQL, "insert into public.gateway_model_price_audits") {
		t.Fatal("model prices migration must not hard-code a model price; automatic sync owns catalog discovery")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 79 {
			continue
		}
		if migration.name != "000079_gateway_model_prices.sql" || migration.nonTransactional {
			t.Fatalf("version 79 model prices migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 79 model prices migration was not loaded")
}

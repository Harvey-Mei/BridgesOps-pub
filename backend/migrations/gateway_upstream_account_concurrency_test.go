package migrations

import (
	"strings"
	"testing"
)

func TestGatewayUpstreamAccountConcurrencyMigrationUsesHashedAccountIdentity(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000036_gateway_upstream_account_concurrency.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"000035 is reserved for managed-key provenance",
		"add column upstream_account_identity_revision bigint not null default 1",
		"add column admin_revision bigint not null default 1",
		"check (upstream_account_identity_revision > 0)",
		"check (admin_revision > 0)",
		"create table public.gateway_upstream_accounts",
		"api_origin_hash character(64) not null",
		"external_account_id_hash character(64) not null",
		"concurrency_limit integer",
		"concurrency_scope character varying(32) not null default 'account'",
		"concurrency_source character varying(128)",
		"concurrency_observed_at timestamp with time zone",
		"concurrency_limit is null and concurrency_source is null and concurrency_observed_at is null",
		"concurrency_limit is not null and concurrency_source is not null and length(btrim(concurrency_source)) > 0 and concurrency_observed_at is not null",
		"create table public.gateway_channel_upstream_account_bindings",
		"channel_identity_revision bigint not null",
		"unique (id, type, upstream_account_identity_revision)",
		"foreign key (channel_id, provider, channel_identity_revision)",
		"references public.channels(id, type, upstream_account_identity_revision)",
		"foreign key (upstream_account_id, provider)",
		"create table public.gateway_channel_upstream_account_binding_audits",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("account concurrency migration missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"api_origin character varying",
		"external_account_id character varying",
		"current_concurrency",
		"newapi",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("account concurrency migration contains forbidden identity/limit source %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 36 {
			continue
		}
		if migration.name != "000036_gateway_upstream_account_concurrency.sql" || migration.nonTransactional {
			t.Fatalf("version 36 account concurrency migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 36 account concurrency migration was not loaded")
}

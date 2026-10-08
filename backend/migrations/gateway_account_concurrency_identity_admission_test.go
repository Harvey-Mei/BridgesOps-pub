package migrations

import (
	"strings"
	"testing"
)

func TestGatewayAccountConcurrencyIdentityAdmissionMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000056_gateway_account_concurrency_identity_admission.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create view public.gateway_current_upstream_account_concurrency",
		"bindings.channel_identity_revision",
		"channels.upstream_account_identity_revision = bindings.channel_identity_revision",
		"bindings.upstream_account_id as account_record_id",
		"accounts.concurrency_limit",
		"accounts.concurrency_scope",
		"accounts.concurrency_source",
		"accounts.concurrency_observed_at",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("identity-bound concurrency migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"api_origin_hash", "external_account_id_hash", "accounts.current_concurrency"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("identity-bound concurrency view exposes forbidden field %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 56 {
			continue
		}
		if migration.name != "000056_gateway_account_concurrency_identity_admission.sql" || migration.nonTransactional {
			t.Fatalf("version 56 concurrency migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 56 account concurrency migration was not loaded")
}

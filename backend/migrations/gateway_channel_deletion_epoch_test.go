package migrations

import (
	"strings"
	"testing"
)

func TestGatewayChannelDeletionEpochMigrationHasIdentityFenceAndDurableNoFKLedgers(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000046_gateway_channel_deletion_epoch.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column deletion_pending boolean not null default false",
		"add column deletion_epoch bigint not null default 0",
		"add column deletion_identity_revision bigint not null default 0",
		"chk_channels_channel_deletion_fence",
		"deletion_identity_revision = upstream_account_identity_revision",
		"and not monitor_enabled",
		"and not latency_probe_enabled",
		"create table public.gateway_channel_deletion_intents",
		"unique (channel_id, deletion_epoch)",
		"create table public.gateway_channel_deletion_key_audits",
		"retain_adopted",
		"manual_review_required",
		"verify_managed_delete",
		"reject_gateway_channel_deletion_key_audit_mutation",
		"before update or delete on public.gateway_channel_deletion_key_audits",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("channel deletion epoch migration missing %q:\n%s", required, body)
		}
	}
	if strings.Contains(sql, "foreign key") || strings.Contains(sql, "on delete cascade") {
		t.Fatalf("channel deletion intent/audit must survive parent deletion without FK/CASCADE:\n%s", body)
	}
}

func TestGatewayChannelDeletionEpochMigrationIsEmbeddedAsVersion46(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 46 {
			continue
		}
		if migration.name != "000046_gateway_channel_deletion_epoch.sql" || migration.nonTransactional {
			t.Fatalf("version 46 channel deletion migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 46 channel deletion migration was not loaded")
}

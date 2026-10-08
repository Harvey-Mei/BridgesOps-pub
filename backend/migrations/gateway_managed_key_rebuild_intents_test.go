package migrations

import (
	"strings"
	"testing"
)

func TestGatewayManagedKeyRebuildIntentsMigrationIsDurableAndGenerationBound(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000039_gateway_managed_key_rebuild_intents.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.gateway_managed_key_rebuild_intents",
		"channel_identity_revision bigint not null",
		"upstream_group_id bigint not null",
		"presence_generation bigint not null",
		"local_key_id bigint not null",
		"ownership_claim character varying(64) not null",
		"status in ('processing', 'acknowledged', 'manual_review')",
		"check (channel_identity_revision > 0)",
		"create unique index idx_gateway_managed_key_rebuild_intent_claim",
		"(channel_id, channel_identity_revision, status, id)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("rebuild intent migration missing %q:\n%s", required, body)
		}
	}
}

func TestGatewayManagedKeyRebuildIntentsMigrationIsEmbeddedAsVersion39(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 39 {
			continue
		}
		if migration.name != "000039_gateway_managed_key_rebuild_intents.sql" || migration.nonTransactional {
			t.Fatalf("version 39 managed-key rebuild migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 39 managed-key rebuild migration was not loaded")
}

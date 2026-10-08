package migrations

import (
	"strings"
	"testing"
)

func TestGatewayManagedKeyRebuildLifecycleMigrationFencesClaimsAndCleansLifecycle(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000040_gateway_managed_key_rebuild_lifecycle.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"gateway_managed_key_rebuild_intents contains orphan channel references",
		"gateway_managed_key_rebuild_intents contains orphan or cross-channel upstream group references",
		"add constraint uq_gateway_upstream_groups_id_channel unique (id, channel_id)",
		"add column claim_epoch bigint not null default 1",
		"claim_digest_version smallint not null default 2",
		"evidence_digest_version smallint not null default 3",
		"ownership_digest_version smallint not null default 1",
		"manual_review_reason varchar(64) not null default ''",
		"manual_review_actor varchar(128) not null default ''",
		"manual_review_resolution varchar(64) not null default ''",
		"manual_review_resolved_by varchar(128) not null default ''",
		"manual_reviewed_at timestamptz",
		"manual_review_resolved_at timestamptz",
		"legacy_manual_review",
		"fk_gateway_managed_key_rebuild_channel",
		"references public.channels(id) on delete cascade",
		"fk_gateway_managed_key_rebuild_group_channel",
		"foreign key (upstream_group_id, channel_id)",
		"references public.gateway_upstream_groups(id, channel_id) on delete cascade",
		"create unique index idx_gateway_managed_key_rebuild_intent_identity",
		"channel_id,\n        channel_identity_revision",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("rebuild lifecycle migration missing %q:\n%s", required, body)
		}
	}
}

func TestGatewayManagedKeyRebuildLifecycleMigrationIsEmbeddedAsVersion40(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 40 {
			continue
		}
		if migration.name != "000040_gateway_managed_key_rebuild_lifecycle.sql" || migration.nonTransactional {
			t.Fatalf("version 40 managed-key rebuild lifecycle migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 40 managed-key rebuild lifecycle migration was not loaded")
}

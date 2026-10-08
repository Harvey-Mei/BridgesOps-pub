package migrations

import (
	"strings"
	"testing"
)

func TestGatewayManagedKeyRemovalLifecycleMigrationFencesClaimsAndKeepsReviewAudit(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000042_gateway_managed_key_removal_lifecycle.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column removal_claim_epoch bigint",
		"add column removal_claim_digest_version smallint",
		"add column removal_evidence_digest_version smallint",
		"add column removal_ownership_digest_version smallint",
		"add column removal_manual_review_reason varchar(64)",
		"check (removal_claim_key is null or (removal_claim_epoch is not null and removal_claim_epoch > 0))",
		"contains orphan or cross-channel upstream group references; repair before 000042",
		"foreign key (channel_id) references public.channels(id) on delete cascade",
		"if not exists (",
		"where conname = 'uq_gateway_upstream_groups_id_channel'",
		"unique (id, channel_id)",
		"foreign key (upstream_group_id, channel_id)",
		"references public.gateway_upstream_groups(id, channel_id) on delete cascade",
		"idx_gateway_upstream_key_removal_reconcile",
		"assignment_reason_code = 'manual_review_required'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("removal lifecycle migration missing %q:\n%s", required, body)
		}
	}
}

func TestGatewayManagedKeyRemovalLifecycleMigrationIsEmbeddedAsVersion42(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 42 {
			continue
		}
		if migration.name != "000042_gateway_managed_key_removal_lifecycle.sql" || migration.nonTransactional {
			t.Fatalf("version 42 managed-key removal migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 42 managed-key removal migration was not loaded")
}

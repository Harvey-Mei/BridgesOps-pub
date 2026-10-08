package migrations

import (
	"strings"
	"testing"
)

func TestModelCheckIdentityRevisionMigrationFencesCurrentStateAndFollowsDeletionEpoch(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000047_model_check_identity_revision.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column upstream_account_identity_revision bigint not null default 0",
		"update public.model_check_targets as target",
		"upstream_account_identity_revision = channel.upstream_account_identity_revision",
		"last_status = 'pending'",
		"baseline_score = null",
		"alert_active = false",
		"old run history, remain immutable",
		"chk_model_check_targets_account_identity_revision_positive",
		"chk_model_check_runs_account_identity_revision_positive",
		"check (upstream_account_identity_revision > 0) not valid",
		"idx_model_check_current_identity",
		"idx_model_check_runs_identity_time",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("model-check identity migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	versions := make(map[int64]string, len(migrations))
	for _, migration := range migrations {
		versions[migration.version] = migration.name
	}
	if versions[47] != "000047_model_check_identity_revision.sql" {
		t.Fatalf("migration 47 = %q", versions[47])
	}
	if versions[46] != "000046_gateway_channel_deletion_epoch.sql" {
		t.Fatalf("migration 46 = %q", versions[46])
	}
}

package migrations

import (
	"strings"
	"testing"
)

func TestChannelCostBasisOverrideMigrationKeepsManualRecoveryAuditable(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000068_channel_cost_basis_overrides.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.channel_cost_basis_overrides",
		"unique (channel_id, channel_identity_revision, idempotency_key)",
		"superseded_candidate_count integer not null",
		"superseded_candidate_digest character varying(64) not null",
		"('channel_funding_candidates', 'status')",
		"state_source_after = 'manual_override'",
		"before update or delete on public.channel_cost_basis_overrides",
		"before truncate on public.channel_cost_basis_overrides",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("cost basis override migration missing %q:\n%s", required, body)
		}
	}
}

func TestChannelCostBasisOverrideMigrationIsEmbeddedAsVersion68(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 68 {
			continue
		}
		if migration.name != "000068_channel_cost_basis_overrides.sql" || migration.nonTransactional {
			t.Fatalf("version 68 cost basis override migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 68 cost basis override migration was not loaded")
}

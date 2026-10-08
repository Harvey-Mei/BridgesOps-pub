package migrations

import (
	"strings"
	"testing"
)

func TestChannelBalanceErrorMigrationKeepsExplicitFactComplete(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000041_channel_balance_error.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column if not exists balance_error text not null default ''",
		"add column if not exists balance_error_at timestamp with time zone",
		"add column if not exists upstream_account_balance_insufficient boolean not null default false",
		"add column if not exists upstream_account_balance_insufficient_at timestamp with time zone",
		"add column if not exists upstream_account_balance_insufficient_revision bigint not null default 0",
		"chk_channels_upstream_account_balance_insufficient",
		"not upstream_account_balance_insufficient",
		"upstream_account_balance_insufficient_at is null",
		"upstream_account_balance_insufficient_revision = 0",
		"upstream_account_balance_insufficient_at is not null",
		"upstream_account_balance_insufficient_revision > 0",
		"upstream_account_balance_insufficient_revision = upstream_account_identity_revision",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("channel balance migration missing %q:\n%s", required, body)
		}
	}
}

func TestChannelBalanceErrorMigrationIsEmbeddedAsVersion41(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 41 {
			continue
		}
		if migration.name != "000041_channel_balance_error.sql" || migration.nonTransactional {
			t.Fatalf("version 41 channel balance migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 41 channel balance migration was not loaded")
}

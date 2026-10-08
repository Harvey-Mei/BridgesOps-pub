package migrations

import (
	"strings"
	"testing"
)

func TestChannelDirectCredentialsMigrationPersistsModelsAndAuthenticationInvalidationFence(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000069_channel_direct_credentials.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column direct_models text not null default '[]'",
		"add column authentication_invalidated_channel_identity_revision bigint not null default 0",
		"check (authentication_invalidated_channel_identity_revision >= 0)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("direct credentials migration missing %q:\n%s", required, body)
		}
	}
}

func TestChannelDirectCredentialsMigrationIsEmbeddedAsVersion69(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 69 {
			continue
		}
		if migration.name != "000069_channel_direct_credentials.sql" || migration.nonTransactional {
			t.Fatalf("version 69 direct credentials migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 69 direct credentials migration was not loaded")
}

func TestMultiDirectCredentialsMigrationBackfillsLegacyCiphertextAndRuntimeFence(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000070_channel_direct_credentials.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.channel_direct_credentials",
		"api_key_cipher text not null",
		"check (ratio > 0 and ratio <> 'infinity'::double precision and ratio <> '-infinity'::double precision and ratio <> 'nan'::double precision)",
		"check (completion_ratio > 0 and completion_ratio <> 'infinity'::double precision and completion_ratio <> '-infinity'::double precision and completion_ratio <> 'nan'::double precision)",
		"on public.channel_direct_credentials (channel_id, lower(btrim(group_name)))",
		"select\n    id,\n    'direct',\n    site_url,\n    password_cipher,\n    direct_models,",
		"where credential_mode = 'direct'",
		"and btrim(password_cipher) <> ''",
		"add column direct_credential_id bigint references public.channel_direct_credentials(id) on delete set null",
		"add column direct_credential_revision bigint not null default 0",
		"add column direct_base_url varchar(512) not null default ''",
		"set direct_credential_id = credential.id",
		"direct_credential_revision = credential.revision",
		"direct_base_url = credential.base_url",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("multi-direct migration missing %q:\n%s", required, body)
		}
	}
}

func TestMultiDirectCredentialsMigrationIsEmbeddedAsVersion70(t *testing.T) {
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 70 {
			continue
		}
		if migration.name != "000070_channel_direct_credentials.sql" || migration.nonTransactional {
			t.Fatalf("version 70 multi-direct credentials migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 70 multi-direct credentials migration was not loaded")
}

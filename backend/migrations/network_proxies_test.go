package migrations

import (
	"strings"
	"testing"
)

func TestNetworkProxyMigrationKeepsCredentialsOutOfSchemaColumns(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000006_network_proxies.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, expected := range []string{
		"CREATE TABLE IF NOT EXISTS public.network_proxies",
		"credential_cipher text",
		"ADD COLUMN IF NOT EXISTS network_proxy_id",
		"idx_channels_network_proxy_id",
	} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("migration missing %q", expected)
		}
	}
	if strings.Contains(sql, "password character varying") || strings.Contains(sql, "username character varying") {
		t.Fatalf("migration must not create plaintext proxy credential columns: %s", sql)
	}
	if strings.Contains(sql, "fk_channels_network_proxy") {
		t.Fatal("released 000006 migration must remain immutable")
	}
}

func TestNetworkProxyBindingIntegrityMigrationFailsClosedAndAddsForeignKey(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000008_network_proxy_binding_integrity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, expected := range []string{
		"SET network_proxy_id = NULL",
		"proxy_enabled = TRUE",
		"ADD CONSTRAINT fk_channels_network_proxy",
		"FOREIGN KEY (network_proxy_id)",
		"REFERENCES public.network_proxies (id)",
		"ON DELETE RESTRICT",
		"NOT VALID",
		"VALIDATE CONSTRAINT fk_channels_network_proxy",
	} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("binding integrity migration missing %q", expected)
		}
	}
}

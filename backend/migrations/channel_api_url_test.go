package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestChannelAPIURLMigrationKeepsHomepageSeparate(t *testing.T) {
	raw, err := os.ReadFile("000010_channel_api_url.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{"alter table public.channels", "add column api_url", "not null", "default ''"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("channel API URL migration missing %q", required)
		}
	}
	if strings.Contains(sql, "update public.channels") || strings.Contains(sql, "site_url =") {
		t.Fatal("channel API URL migration must not rewrite homepage URLs")
	}
}

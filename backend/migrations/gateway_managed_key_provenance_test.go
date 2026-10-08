package migrations

import (
	"strings"
	"testing"
)

func TestGatewayManagedKeyProvenanceMigrationFailsClosedForLegacyRows(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000035_gateway_managed_key_provenance.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column ownership_provenance",
		"not null default 'unknown'",
		"'created'",
		"'provisioning_claim'",
		"'adopted'",
		"'unknown'",
		"add column removal_ownership_claim",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration missing %q:\n%s", required, body)
		}
	}
	if strings.Contains(sql, "update public.gateway_upstream_keys") {
		t.Fatal("legacy upstream keys must not be backfilled into deletion ownership")
	}
}

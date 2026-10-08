package migrations

import (
	"strings"
	"testing"
)

func TestGatewayChannelDeletionAutomaticKeyPolicyConstrainsReservedUnknownKeys(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000078_gateway_channel_deletion_automatic_key_policy_convergence.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"drop constraint chk_gateway_channel_deletion_key_audit_disposition",
		"add constraint chk_gateway_channel_deletion_key_audit_disposition",
		"drop constraint chk_gateway_channel_deletion_key_claim_disposition",
		"add constraint chk_gateway_channel_deletion_key_claim_disposition",
		"upstream_key_name ~ '^bog-p-[0-9a-f]{16}$'",
		"ownership_provenance = 'adopted' and disposition = 'retain_adopted'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("automatic key policy migration missing %q:\n%s", required, body)
		}
	}
	if count := strings.Count(sql, "upstream_key_name ~ '^bog-p-[0-9a-f]{16}$'"); count != 2 {
		t.Fatalf("reserved unknown-key guards = %d, want audit and claim constraints", count)
	}
}

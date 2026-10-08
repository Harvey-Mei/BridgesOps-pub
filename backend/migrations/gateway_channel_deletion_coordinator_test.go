package migrations

import (
	"strings"
	"testing"
)

func TestGatewayChannelDeletionCoordinatorPersistsExactObservedRemoteTarget(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000054_gateway_channel_deletion_coordinator.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	if count := strings.Count(sql, "observed_remote_id bigint not null default 0"); count != 2 {
		t.Fatalf("observed remote target columns = %d, want claim and immutable evidence columns", count)
	}
	for _, required := range []string{
		"create table public.gateway_channel_deletion_key_claims",
		"create table public.gateway_channel_deletion_key_evidence",
		"and observed_remote_id >= 0",
		"remote_identity_bound",
		"before update or delete on public.gateway_channel_deletion_key_evidence",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("channel deletion coordinator migration missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"revealed_secret",
		"secret_hash",
		"api_key_cipher",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("channel deletion evidence schema exposes credential material %q", forbidden)
		}
	}
}

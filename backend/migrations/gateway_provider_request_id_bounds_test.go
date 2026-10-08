package migrations

import (
	"strings"
	"testing"
)

func TestGatewayProviderRequestIDMigrationsConvergeOn512ByteContract(t *testing.T) {
	initial := readGatewayProviderRequestIDMigration(t, "000001_initial_schema.sql")
	dispatch := readGatewayProviderRequestIDMigration(t, "000033_gateway_request_dispatch_audits.sql")
	evidence := readGatewayProviderRequestIDMigration(t, "000038_gateway_dispatch_cost_evidence.sql")

	if !strings.Contains(initial, "upstream_request_id character varying(128)") {
		t.Fatal("initial request-log schema no longer records the legacy 128-character boundary")
	}
	for _, required := range []string{
		"provider_request_id character varying(512) not null default ''",
		"check (octet_length(provider_request_id) <= 512)",
	} {
		if !strings.Contains(dispatch, required) {
			t.Fatalf("dispatch migration missing %q", required)
		}
	}
	for _, required := range []string{
		"alter column upstream_request_id type character varying(512)",
		"alter column provider_request_id type character varying(512)",
		"check (upstream_request_id is null or octet_length(upstream_request_id) <= 512)",
		"check (octet_length(provider_request_id) <= 512)",
	} {
		if !strings.Contains(evidence, required) {
			t.Fatalf("evidence migration missing %q", required)
		}
	}
	for name, sql := range map[string]string{"000033": dispatch, "000038": evidence} {
		for _, forbidden := range []string{
			"left(provider_request_id",
			"substring(provider_request_id",
			"left(upstream_request_id",
			"substring(upstream_request_id",
		} {
			if strings.Contains(sql, forbidden) {
				t.Fatalf("%s migration truncates provider identity via %q", name, forbidden)
			}
		}
	}
}

func readGatewayProviderRequestIDMigration(t *testing.T, name string) string {
	t.Helper()
	body, err := embeddedMigrations.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ToLower(string(body))
}

package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayRoutingFailureErrorMigration(t *testing.T) {
	raw, err := os.ReadFile("000012_gateway_routing_failure_error.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"alter table public.gateway_upstream_keys",
		"last_routing_failure_error text not null default ''",
		"last_error = ''",
		"last_error, '') <> coalesce(models_error",
		"case routing_failure_count",
		"when 1 then interval '0 minutes'",
		"when 2 then interval '0 minutes'",
		"when 3 then interval '0 minutes'",
		"when 4 then interval '2 minutes'",
		"when 5 then interval '4 minutes'",
		"when 6 then interval '8 minutes'",
		"when 7 then interval '16 minutes'",
		"else interval '30 minutes'",
		"last_error ~ '^latency probe status [0-9]{3}$'",
		"last_routing_failure_error ~ '^latency probe status 4[0-9]{2}$'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("routing failure migration missing %q", required)
		}
	}
}

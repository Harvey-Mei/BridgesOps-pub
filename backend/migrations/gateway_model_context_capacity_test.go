package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayModelContextCapacityMigration(t *testing.T) {
	raw, err := os.ReadFile("000011_gateway_model_context_capacity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"alter table public.gateway_model_capabilities",
		"context_window_tokens bigint not null default 0",
		"safe_input_tokens bigint not null default 0",
		"context_safety_tokens bigint not null default 0",
		"context_limit_observed_at timestamptz",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("context capacity migration missing %q", required)
		}
	}
}

package migrations

import (
	"strings"
	"testing"
)

func TestGatewayChannelModelPricesMigration(t *testing.T) {
	raw, err := embeddedMigrations.ReadFile("000081_gateway_channel_model_prices.sql")
	if err != nil {
		t.Fatalf("read channel model price migration: %v", err)
	}
	sql := string(raw)
	for _, required := range []string{
		"ADD COLUMN model_price_observed_at",
		"CREATE TABLE public.gateway_channel_model_prices",
		"FOREIGN KEY (channel_id) REFERENCES public.channels(id) ON DELETE CASCADE",
		"UNIQUE (channel_id, upstream_group_name, model_name)",
		"channel_identity_revision bigint NOT NULL",
		"cost_unit_epoch bigint NOT NULL",
		"upstream_group_id bigint NOT NULL",
		"gateway_upstream_group_id bigint NOT NULL",
		"upstream_group_presence_generation bigint NOT NULL",
		"upstream_group_name character varying(128) NOT NULL",
		"group_ratio double precision NOT NULL",
		"long_context_basis character varying(32) NOT NULL",
		"long_context_threshold_tokens bigint",
		"long_context_threshold_tokens > 0",
		"long_context_threshold_tokens < 9223372036854775807",
		"source_checksum ~ '^[0-9a-f]{64}$'",
		"CREATE INDEX idx_gateway_channel_model_prices_lookup",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration is missing %q", required)
		}
	}
}

package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayModelFactIdentityFencesMigration(t *testing.T) {
	raw, err := os.ReadFile("000065_gateway_model_fact_identity_fences.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, fragment := range []string{
		"add column model_facts_upstream_group_id bigint not null default 0",
		"add column model_facts_presence_generation bigint not null default 0",
		"add column model_facts_channel_identity_revision bigint not null default 0",
		"update public.gateway_upstream_keys",
		"set model_families = ''",
		"delete from public.gateway_model_capabilities",
		"add column channel_identity_revision bigint not null default 0",
		"chk_gateway_model_capabilities_identity",
		"add column model_facts_channel_identity_revision bigint not null default 0",
		"model_identity_unknown_after_migration",
		"not facts_complete or model_facts_channel_identity_revision > 0",
		"claims.source_kind = 'auto_rule'",
		"set active = false",
		"not exists (",
		"'model_identity_unknown_after_migration'",
		"insert into public.gateway_upstream_group_changes",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}

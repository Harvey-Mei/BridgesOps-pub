package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayLatencyProbeSchedulerMigration(t *testing.T) {
	raw, err := os.ReadFile("000017_gateway_latency_probe_scheduler.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"create table public.gateway_latency_probe_states",
		"upstream_group_id bigint not null default 0",
		"presence_generation bigint not null default 0",
		"next_probe_at timestamptz",
		"confirmation_outcome varchar(16) not null default ''",
		"confirmation_count integer not null default 0",
		"unique (channel_id, group_name, gateway_app, upstream_group_id, presence_generation)",
		"gateway_latency_probe_states_due_idx",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration missing %q", required)
		}
	}
}

func TestGatewayLatencyProbeStateIndexesMigration(t *testing.T) {
	for _, item := range []struct {
		file    string
		indexID string
	}{
		{file: "000018_gateway_latency_probe_state_retention_notx.sql", indexID: "gateway_latency_probe_states_retention_idx"},
		{file: "000019_gateway_latency_probe_state_legacy_route_notx.sql", indexID: "gateway_latency_probe_states_legacy_route_idx"},
		{file: "000020_gateway_upstream_key_group_norm_notx.sql", indexID: "gateway_upstream_keys_channel_group_norm_idx"},
		{file: "000021_rate_snapshot_model_norm_notx.sql", indexID: "rate_snapshots_channel_model_norm_idx"},
	} {
		raw, err := os.ReadFile(item.file)
		if err != nil {
			t.Fatal(err)
		}
		sql := strings.ToLower(string(raw))
		if !strings.Contains(sql, item.indexID) || strings.Count(sql, "create index concurrently") != 1 {
			t.Fatalf("migration %s missing single concurrent index %q", item.file, item.indexID)
		}
	}
}

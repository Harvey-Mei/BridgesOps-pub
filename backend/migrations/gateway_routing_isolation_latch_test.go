package migrations

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestGatewayRoutingIsolationLatchMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000073_gateway_routing_isolation_latch.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column routing_isolated_at timestamp with time zone",
		"set routing_isolated_at = coalesce(",
		"order by failures.occurred_at, failures.id",
		"offset 2",
		"keys.routing_failure_count >= 3",
		"keys.last_routing_failure_kind = 'manual_cooldown'",
		"from public.gateway_routing_failures as failures",
		") >= 3",
		"set routing_cooldown_until = null",
		"last_routing_failure_kind is distinct from 'manual_cooldown'",
		"isfinite(routing_isolated_at)",
		"generation-bound adaptive routing isolation latch",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("routing isolation migration missing %q", required)
		}
	}
}

func TestGatewayRoutingIsolationLatchMigrationBackfill(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 72), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 72: %v", err)
	}

	var channelID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('routing isolation migration', 'newapi', 'https://routing-isolation.example', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatalf("insert migration channel: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_upstream_keys (
			channel_id, api_key_cipher, api_key_prefix, group_name,
			last_routing_failure_kind, last_routing_failure_status,
			last_routing_failure_at, routing_failure_count,
			routing_cooldown_until,
			routing_health_upstream_group_id, routing_health_presence_generation,
			routing_health_channel_identity_revision
		) VALUES
			($1, 'cipher', 'sk-a', 'transport', 'transport', 0, CURRENT_TIMESTAMP, 3, CURRENT_TIMESTAMP + INTERVAL '10 minutes', 101, 1, 1),
			($1, 'cipher', 'sk-b', 'http-400', 'http_status', 400, CURRENT_TIMESTAMP, 3, CURRENT_TIMESTAMP + INTERVAL '10 minutes', 102, 1, 1),
			($1, 'cipher', 'sk-c', 'manual-dirty', 'manual_cooldown', 0, CURRENT_TIMESTAMP, 4, CURRENT_TIMESTAMP + INTERVAL '10 minutes', 103, 1, 1),
			($1, 'cipher', 'sk-d', 'manual-proven', 'manual_cooldown', 0, CURRENT_TIMESTAMP, 4, CURRENT_TIMESTAMP + INTERVAL '10 minutes', 104, 1, 1),
			($1, 'cipher', 'sk-e', 'below-threshold', 'transport', 0, CURRENT_TIMESTAMP, 2, CURRENT_TIMESTAMP + INTERVAL '10 minutes', 105, 1, 1)
	`, channelID); err != nil {
		t.Fatalf("insert legacy routing summaries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_routing_failures (
			channel_id, group_name, upstream_group_id, presence_generation,
			channel_identity_revision, occurred_at
		) VALUES
			($1, 'manual-dirty', 103, 1, 1, CURRENT_TIMESTAMP),
			($1, 'manual-proven', 104, 1, 1, CURRENT_TIMESTAMP - INTERVAL '2 seconds'),
			($1, 'manual-proven', 104, 1, 1, CURRENT_TIMESTAMP - INTERVAL '1 second'),
			($1, 'manual-proven', 104, 1, 1, CURRENT_TIMESTAMP)
	`, channelID); err != nil {
		t.Fatalf("insert legacy routing failure facts: %v", err)
	}

	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 73), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply routing isolation migration: %v", err)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT group_name, routing_isolated_at IS NOT NULL
		FROM public.gateway_upstream_keys
		WHERE channel_id = $1
	`, channelID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]bool)
	for rows.Next() {
		var group string
		var isolated bool
		if err := rows.Scan(&group, &isolated); err != nil {
			t.Fatal(err)
		}
		got[group] = isolated
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"transport": true, "http-400": false, "manual-dirty": false,
		"manual-proven": true, "below-threshold": false,
	}
	for group, isolated := range want {
		if got[group] != isolated {
			t.Fatalf("backfilled isolation for %q = %t, want %t (all=%v)", group, got[group], isolated, got)
		}
	}
	var adaptiveCooldowns, missingManualCooldowns int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE last_routing_failure_kind IS DISTINCT FROM 'manual_cooldown'
				  AND routing_cooldown_until IS NOT NULL
			),
			COUNT(*) FILTER (
				WHERE last_routing_failure_kind = 'manual_cooldown'
				  AND routing_cooldown_until IS NULL
			)
		FROM public.gateway_upstream_keys
		WHERE channel_id = $1
	`, channelID).Scan(&adaptiveCooldowns, &missingManualCooldowns); err != nil {
		t.Fatal(err)
	}
	if adaptiveCooldowns != 0 || missingManualCooldowns != 0 {
		t.Fatalf("legacy cooldown cleanup: adaptive=%d missing manual=%d", adaptiveCooldowns, missingManualCooldowns)
	}

	_, constraintErr := db.ExecContext(ctx, `
		UPDATE public.gateway_upstream_keys
		SET routing_isolated_at = 'infinity'::timestamptz
		WHERE channel_id = $1 AND group_name = 'transport'
	`, channelID)
	assertPostgresCode(t, constraintErr, "23514")

	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_upstream_keys
		SET last_routing_failure_at = NULL,
			routing_failure_count = 0
		WHERE channel_id = $1 AND group_name = 'transport'
	`, channelID); err != nil {
		t.Fatalf("legacy success update after latch migration: %v", err)
	}
	var latchRetained bool
	if err := db.QueryRowContext(ctx, `
		SELECT routing_isolated_at IS NOT NULL
		FROM public.gateway_upstream_keys
		WHERE channel_id = $1 AND group_name = 'transport'
	`, channelID).Scan(&latchRetained); err != nil {
		t.Fatal(err)
	}
	if !latchRetained {
		t.Fatal("legacy success update unexpectedly cleared the isolation latch")
	}
}

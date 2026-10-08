package migrations

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestGatewayRequestCostReconcileIdentityLeaseMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000057_gateway_request_cost_reconcile_identity_leases.sql")
	if err != nil {
		t.Fatalf("read cost reconcile identity lease migration: %v", err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"gateway_request_cost_reconcile_identity_leases",
		"unique (channel_id, upstream_key_id)",
		"claim_epoch",
		"claim_token",
		"lease_expires_at",
		"gateway_request_cost_reconcile_events",
		"cost_reconcile_state",
		"cost_reconcile_outcome",
		"'conflict_retry'",
		"'terminal_conflict'",
		"'pricing_unavailable'",
		"before update or delete",
		"before truncate",
		"default clock_timestamp()",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("cost reconcile identity lease migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 57 {
			continue
		}
		if migration.name != "000057_gateway_request_cost_reconcile_identity_leases.sql" || migration.nonTransactional {
			t.Fatalf("version 57 cost reconcile migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 57 cost reconcile identity lease migration was not loaded")
}

func TestGatewayRequestCostReconcileAuditIsAppendOnlyPostgres(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("BRIDGEOPS_TEST_POSTGRES_DSN is not set")
	}
	db, _ := testpostgres.OpenEmpty(t)
	if err := Apply(context.Background(), db); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin reconcile audit clock transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var transactionStarted, recordedAt time.Time
	if err := tx.QueryRow(`SELECT CURRENT_TIMESTAMP`).Scan(&transactionStarted); err != nil {
		t.Fatalf("read reconcile audit transaction time: %v", err)
	}
	if _, err := tx.Exec(`SELECT pg_sleep(0.025)`); err != nil {
		t.Fatalf("wait before reconcile audit insert: %v", err)
	}
	if err := tx.QueryRow(`
		INSERT INTO public.gateway_request_cost_reconcile_events (
			request_log_id, channel_id, upstream_key_id, event_kind,
			state_before, state_after, outcome, reason_code
		) VALUES (1, 1, 0, 'scheduled', 'idle', 'pending', 'none', 'scheduled')
		RETURNING recorded_at
	`).Scan(&recordedAt); err != nil {
		t.Fatalf("insert reconcile audit event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit reconcile audit event: %v", err)
	}
	if !recordedAt.After(transactionStarted) {
		t.Fatalf("reconcile audit recorded_at = %v, want after transaction start %v", recordedAt, transactionStarted)
	}
	for name, statement := range map[string]string{
		"update":   `UPDATE public.gateway_request_cost_reconcile_events SET reason_code = 'claimed'`,
		"delete":   `DELETE FROM public.gateway_request_cost_reconcile_events`,
		"truncate": `TRUNCATE public.gateway_request_cost_reconcile_events`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Exec(statement); err == nil || !strings.Contains(strings.ToLower(err.Error()), "append-only") {
				t.Fatalf("%s immutable audit error = %v", name, err)
			}
		})
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM public.gateway_request_cost_reconcile_events`).Scan(&count); err != nil {
		t.Fatalf("count reconcile audit events: %v", err)
	}
	if count != 1 {
		t.Fatalf("reconcile audit event count = %d, want 1", count)
	}
}

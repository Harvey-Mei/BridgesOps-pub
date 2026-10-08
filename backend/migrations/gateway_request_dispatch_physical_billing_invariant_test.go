package migrations

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestGatewayRequestDispatchPhysicalBillingInvariantMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000048_gateway_request_dispatch_physical_billing_invariant.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"physical_billing_invariant_version smallint not null default 0",
		"alter column physical_billing_invariant_version set default 1",
		"physically_dispatched",
		"path not in ('/v1/images/generations', '/v1/responses')",
		"or method = 'post'",
		"accepted_at is not null",
		"or (physically_dispatched and billing_committed)",
		"response_observed_at is not null",
		"not usage_operation_present",
		"operation_units is null",
		"new gateway request dispatch must use physical billing invariant version 1",
		"new gateway request dispatch must start before wire observation and acceptance",
		"gateway request dispatch physical billing invariant version is immutable",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("physical billing migration missing %q", required)
		}
	}
}

func TestGatewayRequestDispatchPhysicalBillingInvariantDatabase(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 48), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 48: %v", err)
	}

	insert := `
		INSERT INTO public.gateway_request_dispatch_audits (
			channel_id, method, path, request_started_at, dispatched_at, legacy_ownerless,
			logical_request_trace_id, telemetry_version, routing_mode, gateway_app,
			first_byte_budget_ms, route_exclusion_snapshot_version, route_exclusions_snapshot,
			price_operation, price_protocol, usage_schema, usage_schema_version,
			usage_operation_present, operation_units, cost_unavailable_reason
		) VALUES (
			1, 'POST', '/v1/images/generations', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE,
			repeat('a', 32), 1, 'fixed', 'codex', 30000, 1,
			'{"version":1,"captured_at":"2026-07-27T00:00:00Z","total_entries":0,"truncated":false,"entries":[]}'::jsonb,
			'image_generation', 'openai_images', 'operation_units_v1', 1,
			FALSE, NULL, 'price_fact_missing'
		) RETURNING id`
	var id int64
	if err := db.QueryRowContext(ctx, insert).Scan(&id); err != nil {
		t.Fatalf("insert prepared image audit: %v", err)
	}
	var physical, billing bool
	if err := db.QueryRowContext(ctx, `SELECT physically_dispatched, billing_committed FROM public.gateway_request_dispatch_audits WHERE id = $1`, id).Scan(&physical, &billing); err != nil {
		t.Fatalf("read prepared image audit: %v", err)
	}
	if physical || billing {
		t.Fatalf("prepared image audit physical=%v billing=%v", physical, billing)
	}

	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET physically_dispatched = TRUE WHERE id = $1`, id); err != nil {
		t.Fatalf("wire-only update: %v", err)
	}
	var usageAbsent bool
	if err := db.QueryRowContext(ctx, `SELECT physically_dispatched, billing_committed, NOT usage_operation_present AND operation_units IS NULL FROM public.gateway_request_dispatch_audits WHERE id = $1`, id).Scan(&physical, &billing, &usageAbsent); err != nil {
		t.Fatalf("read wire-only image audit: %v", err)
	}
	if !physical || billing || !usageAbsent {
		t.Fatalf("wire-only image audit physical=%v billing=%v usage_absent=%v", physical, billing, usageAbsent)
	}
	_, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET usage_operation_present = TRUE, operation_units = 1 WHERE id = $1`, id)
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "chk_gateway_request_dispatch_physical_billing_invariant" {
		t.Fatalf("operation usage without acceptance error=%v", err)
	}
	_, err = db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET billing_committed = TRUE, usage_operation_present = TRUE, operation_units = 1 WHERE id = $1`, id)
	pgErr = nil
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "chk_gateway_request_dispatch_physical_billing_invariant" {
		t.Fatalf("billing without acceptance error=%v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET stage = 'accepted', response_observed_at = CURRENT_TIMESTAMP, accepted_at = CURRENT_TIMESTAMP, accepted_event_type = 'image.completed', billing_committed = TRUE, usage_operation_present = TRUE, operation_units = 1 WHERE id = $1`, id); err != nil {
		t.Fatalf("accepted billing update: %v", err)
	}
	var accepted, usagePresent bool
	var operationUnits int64
	if err := db.QueryRowContext(ctx, `SELECT accepted_at IS NOT NULL, billing_committed, usage_operation_present, operation_units FROM public.gateway_request_dispatch_audits WHERE id = $1`, id).Scan(&accepted, &billing, &usagePresent, &operationUnits); err != nil {
		t.Fatalf("read accepted image audit: %v", err)
	}
	if !accepted || !billing || !usagePresent || operationUnits != 1 {
		t.Fatalf("accepted image audit accepted=%v billing=%v usage_present=%v units=%d", accepted, billing, usagePresent, operationUnits)
	}

	_, err = db.ExecContext(ctx, strings.Replace(insert, "'POST'", "'GET'", 1))
	pgErr = nil
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "chk_gateway_request_dispatch_physical_billing_invariant" {
		t.Fatalf("GET image insert error=%v", err)
	}
}

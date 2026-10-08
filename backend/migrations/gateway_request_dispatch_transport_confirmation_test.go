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

func TestGatewayRequestDispatchTransportConfirmationMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000064_gateway_request_dispatch_transport_confirmation.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"transport_confirmation_version smallint not null default 0",
		"transport_return_confirmed boolean not null default false",
		"transport_return_confirmed_at timestamp with time zone",
		"transport_return_confirmed = (transport_return_confirmed_at is not null)",
		"transport_return_confirmed = (transport_state = 'confirmed')",
		"response_observed_at is null",
		"or transport_return_confirmed",
		"accepted_at is null",
		"or response_observed_at is not null",
		"transport confirmation requires a prior wire marker",
		"response observation requires prior transport confirmation",
		"acceptance requires prior response observation",
		"transport evidence cannot regress",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("transport confirmation migration missing %q", required)
		}
	}
}

func TestGatewayRequestDispatchTransportConfirmationDatabase(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 64), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 64: %v", err)
	}
	const insert = `
		INSERT INTO public.gateway_request_dispatch_audits (
			channel_id, method, path, request_started_at, dispatched_at,
			legacy_ownerless,
			logical_request_trace_id, telemetry_version, routing_mode, gateway_app,
			first_byte_budget_ms, route_exclusion_snapshot_version, route_exclusions_snapshot,
			price_operation, price_protocol, usage_schema, usage_schema_version,
			usage_operation_present, operation_units, cost_unavailable_reason
		) VALUES (
			1, 'POST', '/v1/images/generations', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
			TRUE,
			repeat('b', 32), 1, 'fixed', 'codex', 30000, 1,
			'{"version":1,"captured_at":"2026-07-27T00:00:00Z","total_entries":0,"truncated":false,"entries":[]}'::jsonb,
			'image_generation', 'openai_images', 'operation_units_v1', 1,
			FALSE, NULL, 'price_fact_missing'
		) RETURNING id`
	var id int64
	if err := db.QueryRowContext(ctx, insert).Scan(&id); err != nil {
		t.Fatalf("insert prepared audit: %v", err)
	}

	assertStateViolation := func(err error) {
		t.Helper()
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatalf("dispatch state error=%v, want check violation", err)
		}
	}
	_, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET transport_return_confirmed = TRUE, transport_return_confirmed_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
	assertStateViolation(err)
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET physically_dispatched = TRUE, transport_state = 'armed' WHERE id = $1`, id); err != nil {
		t.Fatalf("mark wire observation: %v", err)
	}
	_, err = db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET stage = 'responded', response_observed_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
	assertStateViolation(err)
	_, err = db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET transport_state = 'confirmed' WHERE id = $1`, id)
	assertStateViolation(err)
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET transport_state = 'confirmed', transport_return_confirmed = TRUE, transport_return_confirmed_at = CURRENT_TIMESTAMP WHERE id = $1`, id); err != nil {
		t.Fatalf("confirm transport return: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET stage = 'accepted',
			accepted_at = CURRENT_TIMESTAMP,
			accepted_event_type = 'image.completed',
			billing_committed = TRUE,
			usage_operation_present = TRUE,
			operation_units = 1
		WHERE id = $1`, id)
	assertStateViolation(err)
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET stage = 'responded', response_observed_at = CURRENT_TIMESTAMP, provider_request_id = 'provider-1' WHERE id = $1`, id); err != nil {
		t.Fatalf("record response observation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET stage = 'accepted',
			accepted_at = CURRENT_TIMESTAMP,
			accepted_event_type = 'image.completed',
			accepted_object_id = 'image-1',
			billing_committed = TRUE,
			usage_operation_present = TRUE,
			operation_units = 1
		WHERE id = $1`, id); err != nil {
		t.Fatalf("accept operation and commit billing: %v", err)
	}
	var chainComplete bool
	if err := db.QueryRowContext(ctx, `
		SELECT physically_dispatched
			AND transport_return_confirmed
			AND response_observed_at IS NOT NULL
			AND accepted_at IS NOT NULL
			AND billing_committed
			AND usage_operation_present
			AND operation_units = 1
		FROM public.gateway_request_dispatch_audits WHERE id = $1`, id).Scan(&chainComplete); err != nil {
		t.Fatalf("read completed dispatch chain: %v", err)
	}
	if !chainComplete {
		t.Fatal("accepted dispatch did not retain the complete billing evidence chain")
	}
	_, err = db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET physically_dispatched = FALSE, transport_return_confirmed = FALSE, transport_return_confirmed_at = NULL WHERE id = $1`, id)
	assertStateViolation(err)

	var ambiguousID int64
	if err := db.QueryRowContext(ctx, insert).Scan(&ambiguousID); err != nil {
		t.Fatalf("insert ambiguous audit: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET usage_operation_present = TRUE, operation_units = 1 WHERE id = $1`, ambiguousID); err != nil {
		t.Fatalf("freeze ambiguous image operation units: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET physically_dispatched = TRUE, transport_state = 'armed' WHERE id = $1`, ambiguousID); err != nil {
		t.Fatalf("mark ambiguous wire observation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET stage = 'manual_review',
			transport_state = 'manual_review',
			physically_dispatched = FALSE,
			billing_committed = FALSE,
			recovery_requested_at = CURRENT_TIMESTAMP,
			recovery_terminal_kind = 'transport_entry_unconfirmed_manual_review',
			terminal_at = CURRENT_TIMESTAMP,
			terminal_kind = 'transport_entry_unconfirmed_manual_review'
		WHERE id = $1`, ambiguousID); err != nil {
		t.Fatalf("terminal manual-review quarantine: %v", err)
	}
	var manualReviewState string
	var physicallyDispatched, billingCommitted bool
	if err := db.QueryRowContext(ctx, `
		SELECT transport_state, physically_dispatched, billing_committed
		FROM public.gateway_request_dispatch_audits
		WHERE id = $1`, ambiguousID).Scan(&manualReviewState, &physicallyDispatched, &billingCommitted); err != nil {
		t.Fatalf("read terminal manual-review quarantine: %v", err)
	}
	if manualReviewState != "manual_review" || physicallyDispatched || billingCommitted {
		t.Fatalf("manual-review transport=%q physical=%t billing=%t", manualReviewState, physicallyDispatched, billingCommitted)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET usage_operation_present = FALSE, operation_units = NULL
		WHERE id = $1`, ambiguousID)
	assertStateViolation(err)
}

func TestGatewayRequestDispatchTransportConfirmationAllowsLegacyPreSendCleanup(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 45), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 45: %v", err)
	}
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_dispatch_audits (
			channel_id, method, path, request_started_at, dispatched_at, billing_committed,
			legacy_ownerless,
			price_operation, price_protocol, usage_schema, usage_schema_version,
			usage_operation_present, operation_units, cost_unavailable_reason
		) VALUES (
			1, 'POST', '/v1/images/generations', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE,
			TRUE,
			'image_generation', 'openai_images', 'operation_units_v1', 1,
			TRUE, 1, 'price_fact_missing'
		) RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert legacy pre-send billing audit: %v", err)
	}
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 64), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 64: %v", err)
	}
	var physicalVersion, transportVersion int
	if err := db.QueryRowContext(ctx, `SELECT physical_billing_invariant_version, transport_confirmation_version FROM public.gateway_request_dispatch_audits WHERE id = $1`, id).Scan(&physicalVersion, &transportVersion); err != nil {
		t.Fatalf("read legacy versions: %v", err)
	}
	if physicalVersion != 0 || transportVersion != 0 {
		t.Fatalf("legacy versions physical=%d transport=%d", physicalVersion, transportVersion)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.gateway_request_dispatch_audits SET stage = 'completed', billing_committed = FALSE, terminal_at = CURRENT_TIMESTAMP, terminal_kind = 'legacy_not_dispatched' WHERE id = $1`, id); err != nil {
		t.Fatalf("complete legacy pre-send billing audit: %v", err)
	}
}

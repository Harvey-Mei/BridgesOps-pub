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

func TestGatewayDispatchDeferredFundingCorrectionMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000066_gateway_dispatch_deferred_funding_corrections.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.gateway_request_funding_corrections",
		"foreign key (funding_event_id, channel_id)",
		"references public.channel_funding_events(id, channel_id)",
		"interval_end is null",
		"interval_start < interval_end",
		"idx_gateway_request_funding_corrections_lookup",
		"gateway request funding correction is immutable",
		"trg_gateway_request_funding_correction_immutable",
		"add column funding_basis_at timestamp with time zone",
		"add column deferred_funding_factor numeric",
		"add column deferred_cost_basis_version bigint",
		"add column deferred_cost_basis_source character varying(32)",
		"chk_gateway_request_logs_funding_basis_at",
		"chk_gateway_request_logs_deferred_funding_correction_shape",
		"record_kind = 'dispatch_cost_evidence'",
		"price_fact_id is null",
		"lower(btrim(coalesce(cost_status, 'none'))) = 'none'",
		"raw_cost is null",
		"btrim(coalesce(upstream_usage_id, '')) = ''",
		"dispatch evidence must start without a deferred funding correction",
		"invalid dispatch evidence deferred funding correction transition",
		"old.record_kind = 'dispatch_cost_evidence'",
		"new.record_kind = 'dispatch_cost_evidence'",
		"lower(btrim(coalesce(old.cost_status, 'none'))) = 'none'",
		"lower(btrim(coalesce(new.cost_status, 'none'))) = 'reconciled'",
		"new.raw_cost is not null",
		"btrim(coalesce(new.upstream_usage_id, '')) <> ''",
		"before insert or update of",
		"trg_gateway_dispatch_deferred_funding_correction_transition",
		"gateway request log funding basis time is immutable",
		"trg_gateway_request_log_funding_basis_at_immutable",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("deferred funding correction migration missing %q", required)
		}
	}
	if strings.Contains(sql, "drop trigger trg_gateway_dispatch_cost_evidence_transition") ||
		strings.Contains(sql, "drop function public.enforce_gateway_dispatch_cost_evidence_transition") {
		t.Fatal("deferred funding migration disables the version 38 exact-reconciliation guard")
	}
}

func TestGatewayDispatchDeferredFundingCorrectionDatabaseGuards(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 66), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through deferred funding correction: %v", err)
	}

	insertEvidence := func(requestID string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.gateway_request_logs (
				record_kind, downstream_key_id, channel_id, method, path,
				status_code, first_byte_ms, latency_ms, cost, cost_estimated,
				cost_status, funding_factor, upstream_request_id, created_at
			) VALUES (
				'dispatch_cost_evidence', 0, 1, '', '',
				0, 0, 0, 0, FALSE,
				'none', 1, $1, CURRENT_TIMESTAMP
			)
			RETURNING id
		`, requestID).Scan(&id); err != nil {
			t.Fatalf("insert pending dispatch evidence: %v", err)
		}
		return id
	}

	var fundingEventID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_funding_events (
			channel_id, kind, paid_amount, credited_amount,
			raw_balance_before, raw_balance_after,
			actual_inventory_before, actual_inventory_after,
			funding_factor_before, funding_factor_after,
			raw_today_cost_before, raw_total_cost_before,
			effective_at, created_at
		) VALUES (
			1, 'manual', 10, 10,
			0, 10, 0, 10, 1, 2, 0, 0,
			CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP
		)
		RETURNING id
	`).Scan(&fundingEventID); err != nil {
		t.Fatalf("insert funding event for correction: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_request_funding_corrections (
			channel_id, funding_event_id, interval_start, interval_end,
			funding_factor, cost_basis_version, cost_basis_source
		) VALUES (
			1, $1, CURRENT_TIMESTAMP - INTERVAL '2 hours',
			CURRENT_TIMESTAMP - INTERVAL '1 hour', 2, 8, 'funding_correction'
		)
	`, fundingEventID); err != nil {
		t.Fatalf("insert funding correction interval: %v", err)
	}
	_, updateErr := db.ExecContext(ctx, `
		UPDATE public.gateway_request_funding_corrections
		SET funding_factor = 3
		WHERE funding_event_id = $1
	`, fundingEventID)
	assertDeferredFundingStateViolation(t, updateErr, "gateway request funding correction is immutable")

	var mismatchedFundingEventID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_funding_events (
			channel_id, kind, paid_amount, credited_amount,
			raw_balance_before, raw_balance_after,
			actual_inventory_before, actual_inventory_after,
			funding_factor_before, funding_factor_after,
			raw_today_cost_before, raw_total_cost_before,
			effective_at, created_at
		) VALUES (
			2, 'manual', 10, 10,
			0, 10, 0, 10, 1, 2, 0, 0,
			CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP
		)
		RETURNING id
	`).Scan(&mismatchedFundingEventID); err != nil {
		t.Fatalf("insert mismatched funding event: %v", err)
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_request_funding_corrections (
			channel_id, funding_event_id, interval_start, interval_end,
			funding_factor, cost_basis_version, cost_basis_source
		) VALUES (
			1, $1, CURRENT_TIMESTAMP - INTERVAL '2 hours',
			CURRENT_TIMESTAMP - INTERVAL '1 hour', 2, 8, 'funding_correction'
		)
	`, mismatchedFundingEventID)
	assertPostgresCode(t, err, "23503")

	var invalidIntervalEventID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_funding_events (
			channel_id, kind, paid_amount, credited_amount,
			raw_balance_before, raw_balance_after,
			actual_inventory_before, actual_inventory_after,
			funding_factor_before, funding_factor_after,
			raw_today_cost_before, raw_total_cost_before,
			effective_at, created_at
		) VALUES (
			1, 'manual', 10, 10,
			0, 10, 0, 10, 1, 2, 0, 0,
			CURRENT_TIMESTAMP - INTERVAL '1 hour', CURRENT_TIMESTAMP
		)
		RETURNING id
	`).Scan(&invalidIntervalEventID); err != nil {
		t.Fatalf("insert invalid-interval funding event: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO public.gateway_request_funding_corrections (
			channel_id, funding_event_id, interval_start, interval_end,
			funding_factor, cost_basis_version, cost_basis_source
		) VALUES (1, $1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2, 8, 'funding_correction')
	`, invalidIntervalEventID)
	assertPostgresCode(t, err, "23514")

	fundingBasisEvidenceID := insertEvidence("provider-funding-basis-immutable")
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET funding_basis_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`, fundingBasisEvidenceID); err == nil {
		t.Fatal("funding basis timestamp update unexpectedly succeeded")
	} else {
		assertDeferredFundingStateViolation(t, err, "funding basis time is immutable")
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO public.gateway_request_logs (
			record_kind, downstream_key_id, channel_id, method, path,
			status_code, first_byte_ms, latency_ms, cost, cost_estimated,
			cost_status, funding_factor, upstream_request_id, created_at,
			deferred_funding_factor, deferred_cost_basis_version, deferred_cost_basis_source
		) VALUES (
			'dispatch_cost_evidence', 0, 1, '', '',
			0, 0, 0, 0, FALSE,
			'none', 1, 'provider-deferred-insert', CURRENT_TIMESTAMP,
			2, 8, 'funding_correction'
		)
	`)
	assertDeferredFundingStateViolation(t, err, "start without a deferred funding correction")

	partialID := insertEvidence("provider-deferred-partial")
	_, err = db.ExecContext(ctx, `UPDATE public.gateway_request_logs SET deferred_funding_factor = 2 WHERE id = $1`, partialID)
	assertDeferredFundingStateViolation(t, err, "")

	clearID := insertEvidence("provider-deferred-clear")
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET deferred_funding_factor = 2,
			deferred_cost_basis_version = 8,
			deferred_cost_basis_source = 'funding_correction'
		WHERE id = $1
	`, clearID); err != nil {
		t.Fatalf("record complete deferred correction: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET deferred_funding_factor = NULL,
			deferred_cost_basis_version = NULL,
			deferred_cost_basis_source = NULL
		WHERE id = $1
	`, clearID)
	assertDeferredFundingStateViolation(t, err, "invalid dispatch evidence deferred funding correction transition")

	evidenceID := insertEvidence("provider-deferred-valid")
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET deferred_funding_factor = 2,
			deferred_cost_basis_version = 8,
			deferred_cost_basis_source = 'funding_correction'
		WHERE id = $1
	`, evidenceID); err != nil {
		t.Fatalf("record deferred funding correction: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET deferred_funding_factor = 2.5,
			deferred_cost_basis_version = 9,
			deferred_cost_basis_source = 'later_correction'
		WHERE id = $1
	`, evidenceID); err != nil {
		t.Fatalf("replace pending deferred funding correction: %v", err)
	}

	// Changing the frozen funding factor in the provider reconciliation itself
	// must still be rejected by the version 38 trigger.
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.625,
			raw_cost = 0.25,
			funding_factor = 2.5,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-deferred-invalid',
			deferred_funding_factor = NULL,
			deferred_cost_basis_version = NULL,
			deferred_cost_basis_source = NULL
		WHERE id = $1
	`, evidenceID)
	assertDeferredFundingStateViolation(t, err, "dispatch cost evidence may only accept exact provider reconciliation")

	// The application consumes a deferred correction in two statements inside
	// one transaction: exact provider evidence under the frozen factor first,
	// then the already-reconciled conversion allowed by version 38.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin deferred reconciliation: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.25,
			raw_cost = 0.25,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-deferred-valid',
			deferred_funding_factor = NULL,
			deferred_cost_basis_version = NULL,
			deferred_cost_basis_source = NULL
		WHERE id = $1
	`, evidenceID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("apply frozen-factor exact reconciliation: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.625,
			funding_factor = 2.5,
			cost_basis_version = 9,
			cost_basis_source = 'later_correction'
		WHERE id = $1
	`, evidenceID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("apply deferred converted correction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deferred reconciliation: %v", err)
	}

	var cost, rawCost, factor float64
	var version int64
	var source string
	var deferredCount int
	if err := db.QueryRowContext(ctx, `
		SELECT cost, raw_cost, funding_factor, cost_basis_version, cost_basis_source,
			(deferred_funding_factor IS NOT NULL)::int
			+ (deferred_cost_basis_version IS NOT NULL)::int
			+ (deferred_cost_basis_source IS NOT NULL)::int
		FROM public.gateway_request_logs WHERE id = $1
	`, evidenceID).Scan(&cost, &rawCost, &factor, &version, &source, &deferredCount); err != nil {
		t.Fatalf("read consumed deferred correction: %v", err)
	}
	if cost != 0.625 || rawCost != 0.25 || factor != 2.5 || version != 9 || source != "later_correction" || deferredCount != 0 {
		t.Fatalf("consumed deferred correction = cost=%v raw=%v factor=%v version=%d source=%q deferred=%d",
			cost, rawCost, factor, version, source, deferredCount)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET deferred_funding_factor = 3,
			deferred_cost_basis_version = 10,
			deferred_cost_basis_source = 'too_late'
		WHERE id = $1
	`, evidenceID)
	assertDeferredFundingStateViolation(t, err, "invalid dispatch evidence deferred funding correction transition")
}

func assertDeferredFundingStateViolation(t *testing.T, err error, message string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("deferred funding state error=%v, want SQLSTATE 23514", err)
	}
	if message != "" && !strings.Contains(strings.ToLower(pgErr.Message), strings.ToLower(message)) {
		t.Fatalf("deferred funding state message=%q, want %q", pgErr.Message, message)
	}
}

func assertPostgresCode(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("PostgreSQL error=%v, want SQLSTATE %s", err, code)
	}
}

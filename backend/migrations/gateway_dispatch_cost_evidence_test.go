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

func TestGatewayDispatchCostEvidenceMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000038_gateway_dispatch_cost_evidence.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"alter column upstream_request_id type character varying(512)",
		"alter column provider_request_id type character varying(512)",
		"add column record_kind character varying(32) not null default 'request'",
		"check (record_kind in ('request', 'dispatch_cost_evidence'))",
		"chk_gateway_request_logs_dispatch_cost_evidence_shape",
		"coalesce(upstream_request_id, '') <> ''",
		"status_code = 0",
		"first_byte_ms = 0",
		"latency_ms = 0",
		"coalesce(total_tokens, 0) = 0",
		"coalesce(upstream_accepted, false) = false",
		"coalesce(billing_committed, false) = false",
		"dispatch cost evidence requires a frozen finite funding factor",
		"dispatch cost evidence must start with zero unverified cost",
		"dispatch cost evidence may only accept exact provider reconciliation",
		"reconciled dispatch cost evidence only permits converted cost correction",
		"new.funding_factor is distinct from old.funding_factor",
		"abs(new.cost - (new.raw_cost * new.funding_factor))",
		"abs(cost - (raw_cost * funding_factor))",
		"0.000000001 * greatest(",
		"newapi_quota",
		"sub2api_actual_cost",
		"provider request id is immutable",
		"drop constraint chk_gateway_request_dispatch_completed_unaccepted",
		"and coalesce(provider_request_id, '') = ''",
		"drop index public.idx_gateway_request_dispatch_recovery",
		"or coalesce(provider_request_id, '') <> ''",
		"chk_gateway_request_aggregates_dispatch_evidence_shape",
		"result = 'evidence'",
		"performance_requests = 0",
		"dispatch_evidence_pending >= 0",
		"result <> 'evidence'",
		"dispatch_evidence_pending = 0",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("dispatch cost evidence migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 38 {
			continue
		}
		if migration.name != "000038_gateway_dispatch_cost_evidence.sql" || migration.nonTransactional {
			t.Fatalf("version 38 dispatch cost evidence migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 38 dispatch cost evidence migration was not loaded")
}

func TestGatewayDispatchCostEvidenceDatabaseGuards(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 38), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through dispatch evidence: %v", err)
	}

	insertEvidence := func(requestID string, fundingFactor float64) int64 {
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
				'none', $2, $1, CURRENT_TIMESTAMP
			)
			RETURNING id
		`, requestID, fundingFactor).Scan(&id); err != nil {
			t.Fatalf("insert valid dispatch evidence: %v", err)
		}
		return id
	}

	for name, statement := range map[string]string{
		"missing frozen factor": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, cost, cost_estimated, cost_status, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 0, 0, 0, 0, FALSE, 'none', 'provider-factor-null', CURRENT_TIMESTAMP)`,
		"negative frozen factor": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, cost, cost_estimated, cost_status, funding_factor, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 0, 0, 0, 0, FALSE, 'none', -1, 'provider-factor-negative', CURRENT_TIMESTAMP)`,
		"nan frozen factor": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, cost, cost_estimated, cost_status, funding_factor, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 0, 0, 0, 0, FALSE, 'none', 'NaN'::numeric, 'provider-factor-nan', CURRENT_TIMESTAMP)`,
		"ordinary status": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, funding_factor, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 502, 0, 0, 1, 'provider-status', CURRENT_TIMESTAMP)`,
		"ordinary tokens": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, total_tokens, funding_factor, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 0, 0, 0, 1, 1, 'provider-tokens', CURRENT_TIMESTAMP)`,
		"accepted flag": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, upstream_accepted, funding_factor, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 0, 0, 0, TRUE, 1, 'provider-accepted', CURRENT_TIMESTAMP)`,
		"preverified cost": `
			INSERT INTO public.gateway_request_logs
				(record_kind, downstream_key_id, channel_id, method, path, status_code, first_byte_ms, latency_ms, cost, raw_cost, cost_status, cost_source, upstream_usage_id, funding_factor, upstream_request_id, created_at)
			VALUES ('dispatch_cost_evidence', 0, 1, '', '', 0, 0, 0, 1, 1, 'reconciled', 'sub2api_actual_cost', 'usage-preverified', 1, 'provider-cost', CURRENT_TIMESTAMP)`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, statement)
			assertGatewayDispatchEvidenceConstraint(t, err)
		})
	}

	exactFactorID := insertEvidence("provider-exact-factor", 2)
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.5,
			raw_cost = 0.25,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-exact-factor'
		WHERE id = $1
	`, exactFactorID); err != nil {
		t.Fatalf("apply converted exact provider reconciliation: %v", err)
	}

	exactMismatchID := insertEvidence("provider-exact-mismatch", 2)
	_, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.6,
			raw_cost = 0.25,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-exact-mismatch'
		WHERE id = $1
	`, exactMismatchID)
	assertGatewayDispatchEvidenceConstraint(t, err)

	frozenFactorID := insertEvidence("provider-frozen-factor", 1)
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.5,
			raw_cost = 0.25,
			funding_factor = 2,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-frozen-factor'
		WHERE id = $1
	`, frozenFactorID)
	assertGatewayDispatchEvidenceConstraint(t, err)

	evidenceID := insertEvidence("provider-valid", 1)
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.25,
			raw_cost = 0.25,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-valid'
		WHERE id = $1
	`, evidenceID); err != nil {
		t.Fatalf("apply exact provider reconciliation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.25,
			raw_cost = 0.25,
			cost_estimated = FALSE,
			cost_source = 'sub2api_actual_cost',
			cost_status = 'reconciled',
			upstream_usage_id = 'usage-valid'
		WHERE id = $1
	`, evidenceID); err != nil {
		t.Fatalf("repeat identical provider reconciliation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.5,
			funding_factor = 2,
			cost_basis_version = cost_basis_version + 1,
			cost_basis_source = 'funding_correction'
		WHERE id = $1
	`, evidenceID); err != nil {
		t.Fatalf("apply converted funding correction: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_logs
		SET cost = 0.6
		WHERE id = $1
	`, evidenceID)
	assertGatewayDispatchEvidenceConstraint(t, err)
	for name, statement := range map[string]string{
		"token mutation":         `UPDATE public.gateway_request_logs SET total_tokens = 1 WHERE id = $1`,
		"provider id mutation":   `UPDATE public.gateway_request_logs SET upstream_request_id = 'provider-other' WHERE id = $1`,
		"usage reassignment":     `UPDATE public.gateway_request_logs SET upstream_usage_id = 'usage-other' WHERE id = $1`,
		"raw cost rewrite":       `UPDATE public.gateway_request_logs SET raw_cost = 0.5 WHERE id = $1`,
		"cost source rewrite":    `UPDATE public.gateway_request_logs SET cost_source = 'newapi_quota' WHERE id = $1`,
		"cost status rewrite":    `UPDATE public.gateway_request_logs SET cost_status = 'none' WHERE id = $1`,
		"estimated cost rewrite": `UPDATE public.gateway_request_logs SET cost_estimated = TRUE WHERE id = $1`,
		"factor only correction": `UPDATE public.gateway_request_logs SET funding_factor = 3 WHERE id = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, statement, evidenceID)
			assertGatewayDispatchEvidenceConstraint(t, err)
		})
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_request_aggregates (
			bucket_start, downstream_key_id, channel_id, channel_name_snapshot,
			model_name, cost_source, route_reason, result, requests,
			dispatch_evidence_pending, first_request_at, last_request_at, version
		) VALUES (
			CURRENT_TIMESTAMP, 0, 1, '', '', '', 'smart_primary', 'evidence', 0,
			1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 7
		)
	`); err != nil {
		t.Fatalf("insert valid evidence aggregate: %v", err)
	}
	for name, statement := range map[string]string{
		"evidence request count": `
			INSERT INTO public.gateway_request_aggregates (
				bucket_start, downstream_key_id, channel_id, channel_name_snapshot,
				model_name, cost_source, route_reason, result, requests,
				first_request_at, last_request_at, version
			) VALUES (
				CURRENT_TIMESTAMP + INTERVAL '1 hour', 0, 1, '', '', '', 'smart_primary', 'evidence', 1,
				CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 7
			)`,
		"ordinary evidence money": `
			INSERT INTO public.gateway_request_aggregates (
				bucket_start, downstream_key_id, channel_id, channel_name_snapshot,
				model_name, cost_source, route_reason, result, requests,
				dispatch_evidence_actual, first_request_at, last_request_at, version
			) VALUES (
				CURRENT_TIMESTAMP + INTERVAL '2 hours', 0, 1, '', '', '', 'smart_primary', 'success', 1,
				0.25, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 7
			)`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, statement)
			assertGatewayDispatchEvidenceConstraint(t, err)
		})
	}

	var dispatchID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_dispatch_audits (
			downstream_key_id, channel_id, method, path, request_started_at,
			stage, response_observed_at, provider_request_id, dispatched_at, legacy_ownerless
		) VALUES (
			0, 1, 'POST', '/v1/responses', CURRENT_TIMESTAMP,
			'responded', CURRENT_TIMESTAMP, 'provider-completed', CURRENT_TIMESTAMP, TRUE
		)
		RETURNING id
	`).Scan(&dispatchID); err != nil {
		t.Fatalf("insert provider-only dispatch: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET stage = 'completed', terminal_at = CURRENT_TIMESTAMP, terminal_kind = 'completed_without_acceptance'
		WHERE id = $1
	`, dispatchID)
	assertGatewayDispatchEvidenceConstraint(t, err)
}

func assertGatewayDispatchEvidenceConstraint(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("raw SQL mutation unexpectedly bypassed dispatch evidence constraints")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("raw SQL mutation error = %v, want PostgreSQL check violation", err)
	}
}

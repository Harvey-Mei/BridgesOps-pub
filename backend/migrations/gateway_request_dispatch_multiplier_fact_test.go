package migrations

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestGatewayRequestDispatchMultiplierFactMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000051_gateway_request_dispatch_multiplier_fact.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column multiplier_provenance_version smallint not null default 0",
		"alter column multiplier_provenance_version set default 1",
		"chk_gateway_request_dispatch_multiplier_provenance",
		"multiplier_provenance_version = 0",
		"multiplier_provenance_version = 1",
		"group_ratio::text not in ('nan', 'infinity', '-infinity')",
		"completion_ratio::text not in ('nan', 'infinity', '-infinity')",
		"raw_group_ratio is null",
		"raw_completion_ratio is null",
		"funding_factor is null",
		"not valid",
		"enforce_gateway_request_dispatch_multiplier_provenance",
		"new gateway request dispatch must use multiplier provenance version 1",
		"gateway request dispatch multiplier provenance version is immutable",
		"gateway request dispatch request log link is immutable once set",
		"gateway request dispatch multiplier provenance is immutable",
		"new.group_ratio is distinct from old.group_ratio",
		"new.completion_ratio is distinct from old.completion_ratio",
		"new.raw_group_ratio is distinct from old.raw_group_ratio",
		"new.raw_completion_ratio is distinct from old.raw_completion_ratio",
		"new.funding_factor is distinct from old.funding_factor",
		"new.cost_basis_version is distinct from old.cost_basis_version",
		"new.cost_basis_source is distinct from old.cost_basis_source",
		"new.telemetry_version is distinct from old.telemetry_version",
		"new.logical_request_trace_id is distinct from old.logical_request_trace_id",
		"new.gateway_app is distinct from old.gateway_app",
		"new.downstream_key_id is distinct from old.downstream_key_id",
		"new.channel_identity_revision is distinct from old.channel_identity_revision",
		"new.upstream_group_id is distinct from old.upstream_group_id",
		"new.upstream_group_generation is distinct from old.upstream_group_generation",
		"new.upstream_key_local_id is distinct from old.upstream_key_local_id",
		"new.upstream_key_generation is distinct from old.upstream_key_generation",
		"new.method is distinct from old.method",
		"new.path is distinct from old.path",
		"new.model_name is distinct from old.model_name",
		"new.upstream_model_name is distinct from old.upstream_model_name",
		"old.request_log_id is not null",
		"trg_gateway_request_dispatch_multiplier_provenance",
		"before insert or update",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("multiplier provenance migration missing %q", required)
		}
	}
	if strings.Contains(sql, "validate constraint") {
		t.Fatal("multiplier provenance migration must leave existing rows unvalidated")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 51 {
			continue
		}
		if migration.name != "000051_gateway_request_dispatch_multiplier_fact.sql" || migration.nonTransactional {
			t.Fatalf("version 51 request dispatch multiplier fact migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 51 request dispatch multiplier fact migration was not loaded")
}

func TestGatewayRequestDispatchMultiplierFactDatabaseGuards(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 50), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 50: %v", err)
	}

	var legacyID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_dispatch_audits (
			channel_id, method, path, request_started_at, dispatched_at, legacy_ownerless
		) VALUES (901, 'POST', '/v1/responses', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE)
		RETURNING id
	`).Scan(&legacyID); err != nil {
		t.Fatalf("insert pre-51 legacy dispatch: %v", err)
	}
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 51), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migration 51: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET group_ratio = 2
		WHERE id = $1
	`, legacyID); err != nil {
		t.Fatalf("update legacy v0 dispatch provenance: %v", err)
	}

	insertAudit := func() (int64, int64) {
		t.Helper()
		var id, version int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.gateway_request_dispatch_audits (
				channel_id, method, path, request_started_at, dispatched_at, legacy_ownerless,
				logical_request_trace_id, telemetry_version, routing_mode, gateway_app,
				route_exclusion_snapshot_version, route_exclusions_snapshot
			) VALUES (
				902, 'POST', '/v1/responses', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE,
				'0123456789abcdef0123456789abcdef', 1, 'smart', 'codex', 1,
				'{"version":1,"captured_at":"2026-07-25T00:00:00Z","total_entries":0,"truncated":false,"entries":[]}'::jsonb
			)
			RETURNING id, multiplier_provenance_version
		`).Scan(&id, &version); err != nil {
			t.Fatalf("insert v1 dispatch provenance: %v", err)
		}
		return id, version
	}
	insertRequestLog := func() int64 {
		t.Helper()
		var id int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.gateway_request_logs (
				downstream_key_id, channel_id, method, path, status_code, latency_ms, created_at
			) VALUES (1, 902, 'POST', '/v1/responses', 200, 1, CURRENT_TIMESTAMP)
			RETURNING id
		`).Scan(&id); err != nil {
			t.Fatalf("insert request log: %v", err)
		}
		return id
	}

	auditID, version := insertAudit()
	if version != 1 {
		t.Fatalf("new multiplier provenance version = %d, want 1", version)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET physically_dispatched = TRUE
		WHERE id = $1
	`, auditID); err != nil {
		t.Fatalf("physical lifecycle update was rejected: %v", err)
	}
	firstLogID := insertRequestLog()
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET stage = 'finalized', terminal_at = CURRENT_TIMESTAMP,
			terminal_kind = 'migration_test', request_log_id = $2
		WHERE id = $1
	`, auditID, firstLogID); err != nil {
		t.Fatalf("first request-log association was rejected: %v", err)
	}
	secondLogID := insertRequestLog()

	for name, statement := range map[string]string{
		"explicit legacy version on new insert": `
			INSERT INTO public.gateway_request_dispatch_audits (
				channel_id, method, path, request_started_at, dispatched_at,
				legacy_ownerless, multiplier_provenance_version
			) VALUES (903, 'POST', '/v1/responses', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, 0)`,
		"frozen multiplier": `
			UPDATE public.gateway_request_dispatch_audits SET group_ratio = 3 WHERE id = ` + strconv.FormatInt(auditID, 10),
		"frozen request semantics": `
			UPDATE public.gateway_request_dispatch_audits SET path = '/v1/messages' WHERE id = ` + strconv.FormatInt(auditID, 10),
		"retarget final request log": `
			UPDATE public.gateway_request_dispatch_audits SET request_log_id = ` + strconv.FormatInt(secondLogID, 10) + ` WHERE id = ` + strconv.FormatInt(auditID, 10),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, statement)
			assertGatewayRequestDispatchMultiplierFactConstraint(t, err)
		})
	}
}

func assertGatewayRequestDispatchMultiplierFactConstraint(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("multiplier provenance constraint error = %v, want PostgreSQL 23514", err)
	}
}

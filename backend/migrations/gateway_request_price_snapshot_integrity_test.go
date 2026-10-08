package migrations

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	priceFactSnapshotConstraint  = "gateway_request_price_fact_snapshot_consistency"
	finalPriceSnapshotConstraint = "gateway_request_dispatch_final_price_snapshot_consistency"
)

func TestGatewayRequestPriceSnapshotIntegrityMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000062_gateway_request_price_snapshot_integrity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(body))

	factMappings := []string{
		"fact.id is not distinct from new.price_fact_id",
		"fact.channel_id is not distinct from new.channel_id",
		"fact.channel_account_identity_revision is not distinct from new.channel_identity_revision",
		"fact.cost_unit_epoch is not distinct from new.price_cost_unit_epoch",
		"fact.upstream_group_id is not distinct from new.upstream_group_id",
		"fact.canonical_group_name is not distinct from new.price_canonical_group_name",
		"fact.upstream_group_presence_generation is not distinct from new.upstream_group_generation",
		"fact.gateway_upstream_key_id is not distinct from new.upstream_key_local_id",
		"fact.upstream_key_generation is not distinct from new.upstream_key_generation",
		"fact.model_name is not distinct from new.price_model_name",
		"fact.operation is not distinct from new.price_operation",
		"fact.protocol is not distinct from new.price_protocol",
		"fact.input_unit_price is not distinct from new.price_input_unit",
		"fact.output_unit_price is not distinct from new.price_output_unit",
		"fact.cache_read_unit_price is not distinct from new.price_cache_read_unit",
		"fact.cache_write_unit_price is not distinct from new.price_cache_write_unit",
		"fact.operation_unit_price is not distinct from new.price_operation_unit",
		"fact.unit is not distinct from new.price_unit",
		"fact.currency is not distinct from new.price_currency",
		"fact.source is not distinct from new.price_source",
		"fact.observed_at is not distinct from new.price_observed_at",
		"fact.valid_from is not distinct from new.price_valid_from",
		"fact.valid_until is not distinct from new.price_valid_until",
		"fact.fact_version is not distinct from new.price_fact_version",
		"fact.schema_version is not distinct from new.price_fact_schema_version",
		"fact.checksum is not distinct from new.price_fact_checksum",
	}
	for _, mapping := range factMappings {
		if !strings.Contains(sqlText, mapping) {
			t.Fatalf("price-fact snapshot trigger missing %q", mapping)
		}
	}

	for _, column := range []string{
		"channel_id",
		"channel_identity_revision",
		"price_cost_unit_epoch",
		"upstream_group_id",
		"upstream_group_generation",
		"upstream_key_local_id",
		"upstream_key_generation",
		"price_fact_id",
		"price_fact_version",
		"price_fact_schema_version",
		"price_fact_checksum",
		"price_canonical_group_name",
		"price_model_name",
		"price_operation",
		"price_protocol",
		"price_input_unit",
		"price_output_unit",
		"price_cache_read_unit",
		"price_cache_write_unit",
		"price_operation_unit",
		"price_currency",
		"price_unit",
		"price_source",
		"price_observed_at",
		"price_valid_from",
		"price_valid_until",
		"usage_schema",
		"usage_schema_version",
		"usage_operation_present",
		"operation_units",
	} {
		mapping := "request_log." + column + " is not distinct from new." + column
		if !strings.Contains(sqlText, mapping) {
			t.Fatalf("dispatch-to-final-log trigger missing %q", mapping)
		}
	}

	for _, required := range []string{
		"before insert on public.gateway_request_logs",
		"before insert on public.gateway_request_dispatch_audits",
		"before insert or update on public.gateway_request_dispatch_audits",
		"before update of usage_operation_present, operation_units",
		"on public.gateway_request_logs",
		"where audit.request_log_id = old.id",
		"linked_audit_usage.usage_operation_present is distinct from new.usage_operation_present",
		"linked_audit_usage.operation_units is distinct from new.operation_units",
		"for update",
		"before truncate on public.gateway_request_price_facts",
		"existing gateway request log price snapshot does not match referenced price fact",
		"existing gateway request dispatch price snapshot does not match referenced price fact",
		"existing gateway request dispatch and final log price snapshots differ",
		"using errcode = '23514'",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("price snapshot integrity migration missing %q", required)
		}
	}
	finalTrigger := migrationSQLSection(t, sqlText,
		"create function public.enforce_gateway_request_dispatch_final_price_snapshot()",
		"create trigger trg_gateway_request_dispatch_final_price_snapshot_consistency")
	if strings.Contains(finalTrigger, "cost_unavailable_reason") {
		t.Fatal("dispatch finalization trigger compares mutable unavailable-cost state")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 62 {
			continue
		}
		if migration.name != "000062_gateway_request_price_snapshot_integrity.sql" || migration.nonTransactional {
			t.Fatalf("version 62 price snapshot integrity migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 62 price snapshot integrity migration was not loaded")
}

func TestGatewayRequestPriceSnapshotIntegrityDatabase(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 62), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 62: %v", err)
	}

	t.Run("append_only_rejects_truncate", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			TRUNCATE public.gateway_request_dispatch_audits,
			         public.gateway_request_logs,
			         public.gateway_request_price_facts CASCADE
		`)
		assertPriceSnapshotViolation(t, err, "")
	})

	factOne := insertPriceSnapshotFact(t, ctx, db, 1, 1.25, 'a', time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC))
	factTwo := insertPriceSnapshotFact(t, ctx, db, 2, 1.50, 'b', time.Date(2026, 7, 28, 0, 1, 0, 0, time.UTC))

	t.Run("request_log_accepts_exact_snapshot_with_null_operation_price", func(t *testing.T) {
		if _, err := insertPriceSnapshotLog(ctx, db, factOne, factOne); err != nil {
			t.Fatalf("insert exact request-log snapshot: %v", err)
		}
	})
	for _, field := range []string{
		"channel_id",
		"channel_identity_revision",
		"price_cost_unit_epoch",
		"upstream_group_id",
		"upstream_group_generation",
		"upstream_key_local_id",
		"upstream_key_generation",
		"price_fact_version",
		"price_fact_schema_version",
		"price_fact_checksum",
		"price_canonical_group_name",
		"price_model_name",
		"price_operation",
		"price_protocol",
		"price_input_unit",
		"price_output_unit",
		"price_cache_read_unit",
		"price_cache_write_unit",
		"price_operation_unit",
		"price_currency",
		"price_unit",
		"price_source",
		"price_observed_at",
		"price_valid_from",
		"price_valid_until",
	} {
		t.Run("request_log_rejects_mutated_"+field, func(t *testing.T) {
			_, err := insertMutatedPriceSnapshotLog(ctx, db, factOne, field)
			assertPriceSnapshotViolation(t, err, priceFactSnapshotConstraint)
		})
	}

	t.Run("dispatch_accepts_exact_snapshot_with_null_operation_price", func(t *testing.T) {
		if _, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne); err != nil {
			t.Fatalf("insert exact dispatch snapshot: %v", err)
		}
	})

	for _, target := range []struct {
		name   string
		insert func(context.Context, *sql.DB, int64, int64) (int64, error)
	}{
		{name: "request_log", insert: insertPriceSnapshotLog},
		{name: "dispatch_audit", insert: insertPriceSnapshotAudit},
	} {
		t.Run(target.name+"_rejects_another_facts_snapshot", func(t *testing.T) {
			_, err := target.insert(ctx, db, factOne, factTwo)
			assertPriceSnapshotViolation(t, err, priceFactSnapshotConstraint)
		})
		t.Run(target.name+"_rejects_missing_fact_as_snapshot_violation", func(t *testing.T) {
			_, err := target.insert(ctx, db, 9223372036854775000, factOne)
			assertPriceSnapshotViolation(t, err, priceFactSnapshotConstraint)
		})
	}

	t.Run("dispatch_finalization_accepts_same_authenticated_snapshot", func(t *testing.T) {
		auditID, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert dispatch audit: %v", err)
		}
		logID, err := insertPriceSnapshotLog(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert request log: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_finalized',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID); err != nil {
			t.Fatalf("finalize matching dispatch snapshot: %v", err)
		}
	})

	t.Run("final_link_rejects_audit_side_operation_usage_mutation", func(t *testing.T) {
		auditID, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert dispatch audit: %v", err)
		}
		logID, err := insertPriceSnapshotLog(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert request log: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_finalized',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID); err != nil {
			t.Fatalf("finalize matching dispatch snapshot: %v", err)
		}

		_, err = db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET usage_operation_present = TRUE,
			    operation_units = 1
			WHERE id = $1
		`, auditID)
		assertPriceSnapshotViolation(t, err, finalPriceSnapshotConstraint)
	})

	t.Run("final_link_rejects_request_log_side_operation_usage_mutation", func(t *testing.T) {
		auditID, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert dispatch audit: %v", err)
		}
		logID, err := insertPriceSnapshotLog(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert request log: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_finalized',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID); err != nil {
			t.Fatalf("finalize matching dispatch snapshot: %v", err)
		}

		_, err = db.ExecContext(ctx, `
			UPDATE public.gateway_request_logs
			SET usage_operation_present = TRUE,
			    operation_units = 1
			WHERE id = $1
		`, logID)
		assertPriceSnapshotViolation(t, err, finalPriceSnapshotConstraint)
	})

	t.Run("final_link_allows_operation_usage_noop", func(t *testing.T) {
		auditID, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert dispatch audit: %v", err)
		}
		logID, err := insertPriceSnapshotLog(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert request log: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_finalized',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID); err != nil {
			t.Fatalf("finalize matching dispatch snapshot: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE public.gateway_request_logs
			SET usage_operation_present = FALSE,
			    operation_units = NULL
			WHERE id = $1
		`, logID); err != nil {
			t.Fatalf("repeat matching operation usage: %v", err)
		}
	})

	t.Run("dispatch_finalization_rejects_different_authenticated_fact", func(t *testing.T) {
		auditID, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne)
		if err != nil {
			t.Fatalf("insert dispatch audit: %v", err)
		}
		logID, err := insertPriceSnapshotLog(ctx, db, factTwo, factTwo)
		if err != nil {
			t.Fatalf("insert request log: %v", err)
		}
		_, err = db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_finalized',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID)
		assertPriceSnapshotViolation(t, err, finalPriceSnapshotConstraint)
	})

	t.Run("reported_image_finalization_allows_mutable_unavailable_reason", func(t *testing.T) {
		auditID, err := insertNoFactImageAudit(ctx, db, "gpt-image-test", "fact_missing")
		if err != nil {
			t.Fatalf("insert no-fact image dispatch: %v", err)
		}
		markPriceSnapshotImageDispatched(t, ctx, db, auditID)
		logID, err := insertNoFactImageLog(ctx, db, "gpt-image-test", "")
		if err != nil {
			t.Fatalf("insert reported no-fact image log: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_reported',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID); err != nil {
			t.Fatalf("finalize reported image with cleared unavailable reason: %v", err)
		}
	})

	t.Run("no_fact_finalization_still_authenticates_frozen_scope", func(t *testing.T) {
		auditID, err := insertNoFactImageAudit(ctx, db, "gpt-image-test", "fact_missing")
		if err != nil {
			t.Fatalf("insert no-fact image dispatch: %v", err)
		}
		markPriceSnapshotImageDispatched(t, ctx, db, auditID)
		logID, err := insertNoFactImageLog(ctx, db, "gpt-image-other", "")
		if err != nil {
			t.Fatalf("insert independently valid no-fact image log: %v", err)
		}
		_, err = db.ExecContext(ctx, `
			UPDATE public.gateway_request_dispatch_audits
			SET stage = 'finalized',
			    terminal_at = CURRENT_TIMESTAMP,
			    terminal_kind = 'migration_test_reported',
			    request_log_id = $1
			WHERE id = $2
		`, logID, auditID)
		assertPriceSnapshotViolation(t, err, finalPriceSnapshotConstraint)
	})
}

func TestGatewayRequestPriceSnapshotIntegrityMigrationRejectsExistingCorruption(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	body, err := embeddedMigrations.ReadFile("000062_gateway_request_price_snapshot_integrity.sql")
	if err != nil {
		t.Fatal(err)
	}
	migrationFS := fstest.MapFS{
		"000062_gateway_request_price_snapshot_integrity.sql": &fstest.MapFile{Data: body},
	}
	tests := []struct {
		name       string
		constraint string
		seed       func(context.Context, *sql.DB, int64, int64) error
	}{
		{
			name:       "request_log_fact_mismatch",
			constraint: priceFactSnapshotConstraint,
			seed: func(ctx context.Context, db *sql.DB, factOne, factTwo int64) error {
				_, err := insertPriceSnapshotLog(ctx, db, factOne, factTwo)
				return err
			},
		},
		{
			name:       "dispatch_fact_mismatch",
			constraint: priceFactSnapshotConstraint,
			seed: func(ctx context.Context, db *sql.DB, factOne, factTwo int64) error {
				_, err := insertPriceSnapshotAudit(ctx, db, factOne, factTwo)
				return err
			},
		},
		{
			name:       "final_link_mismatch",
			constraint: finalPriceSnapshotConstraint,
			seed: func(ctx context.Context, db *sql.DB, factOne, factTwo int64) error {
				auditID, err := insertPriceSnapshotAudit(ctx, db, factOne, factOne)
				if err != nil {
					return err
				}
				logID, err := insertPriceSnapshotLog(ctx, db, factTwo, factTwo)
				if err != nil {
					return err
				}
				_, err = db.ExecContext(ctx, `
					UPDATE public.gateway_request_dispatch_audits
					SET stage = 'finalized', terminal_at = CURRENT_TIMESTAMP,
					    terminal_kind = 'migration_test_existing', request_log_id = $1
					WHERE id = $2
				`, logID, auditID)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, _ := testpostgres.OpenEmpty(t)
			ctx := context.Background()
			if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 61), ".", defaultRunnerConfig()); err != nil {
				t.Fatalf("apply migrations through 61: %v", err)
			}
			factOne := insertPriceSnapshotFact(t, ctx, db, 1, 1.25, 'a', time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC))
			factTwo := insertPriceSnapshotFact(t, ctx, db, 2, 1.50, 'b', time.Date(2026, 7, 28, 0, 1, 0, 0, time.UTC))
			if err := test.seed(ctx, db, factOne, factTwo); err != nil {
				t.Fatalf("seed pre-62 corruption: %v", err)
			}
			err := applyMigrations(ctx, db, migrationFS, ".", defaultRunnerConfig())
			assertPriceSnapshotViolation(t, err, test.constraint)
		})
	}
}

func insertPriceSnapshotFact(t *testing.T, ctx context.Context, db *sql.DB, version int64, inputPrice float64, checksumByte rune, validFrom time.Time) int64 {
	t.Helper()
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_price_facts (
			channel_id, channel_account_identity_revision, upstream_group_id,
			canonical_group_name, upstream_group_presence_generation,
			gateway_upstream_key_id, upstream_key_generation,
			model_name, operation, protocol,
			input_unit_price, output_unit_price, cache_read_unit_price, cache_write_unit_price,
			unit, currency, source, observed_at, valid_from, valid_until,
			fact_version, schema_version, checksum
		) VALUES (
			901, 11, 21, 'primary', 31, 41, 51,
			'gpt-test', 'text_generation', 'openai_chat_completions',
			$1, 2.5, 0.2, 0.4,
			'per_million_tokens', 'USD', 'migration_test', $2, $2, $3,
			$4, 1, $5
		)
		RETURNING id
	`, inputPrice, validFrom, validFrom.Add(24*time.Hour), version, strings.Repeat(string(checksumByte), 64)).Scan(&id)
	if err != nil {
		t.Fatalf("insert price fact version %d: %v", version, err)
	}
	return id
}

func insertPriceSnapshotLog(ctx context.Context, db *sql.DB, referencedFactID, snapshotFactID int64) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_logs (
			downstream_key_id, channel_id, method, path, status_code, latency_ms, created_at,
			channel_identity_revision, upstream_group_id, upstream_group_generation,
			upstream_key_local_id, upstream_key_generation,
			price_cost_unit_epoch,
			price_fact_id, price_fact_version, price_fact_schema_version, price_fact_checksum,
			price_canonical_group_name, price_model_name, price_operation, price_protocol,
			price_input_unit, price_output_unit, price_cache_read_unit, price_cache_write_unit,
			price_operation_unit, price_currency, price_unit, price_source,
			price_observed_at, price_valid_from, price_valid_until,
			usage_schema, usage_schema_version, usage_operation_present, operation_units
		)
		SELECT
			1, fact.channel_id, 'POST', '/v1/chat/completions', 200, 1, CURRENT_TIMESTAMP,
			fact.channel_account_identity_revision, fact.upstream_group_id,
			fact.upstream_group_presence_generation, fact.gateway_upstream_key_id,
			fact.upstream_key_generation,
			fact.cost_unit_epoch,
			$1, fact.fact_version, fact.schema_version, fact.checksum,
			fact.canonical_group_name, fact.model_name, fact.operation, fact.protocol,
			fact.input_unit_price, fact.output_unit_price, fact.cache_read_unit_price,
			fact.cache_write_unit_price, fact.operation_unit_price, fact.currency,
			fact.unit, fact.source, fact.observed_at, fact.valid_from, fact.valid_until,
			'token_components_v1', 1, FALSE, NULL
		FROM public.gateway_request_price_facts AS fact
		WHERE fact.id = $2
		RETURNING id
	`, referencedFactID, snapshotFactID).Scan(&id)
	return id, err
}

func insertMutatedPriceSnapshotLog(ctx context.Context, db *sql.DB, factID int64, mutation string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_logs (
			downstream_key_id, channel_id, method, path, status_code, latency_ms, created_at,
			channel_identity_revision, upstream_group_id, upstream_group_generation,
			upstream_key_local_id, upstream_key_generation, price_cost_unit_epoch,
			price_fact_id, price_fact_version, price_fact_schema_version, price_fact_checksum,
			price_canonical_group_name, price_model_name, price_operation, price_protocol,
			price_input_unit, price_output_unit, price_cache_read_unit, price_cache_write_unit,
			price_operation_unit, price_currency, price_unit, price_source,
			price_observed_at, price_valid_from, price_valid_until,
			usage_schema, usage_schema_version, usage_operation_present, operation_units
		)
		SELECT
			1,
			CASE WHEN $2 = 'channel_id' THEN fact.channel_id + 1 ELSE fact.channel_id END,
			'POST', '/v1/chat/completions', 200, 1, CURRENT_TIMESTAMP,
			CASE WHEN $2 = 'channel_identity_revision' THEN fact.channel_account_identity_revision + 1 ELSE fact.channel_account_identity_revision END,
			CASE WHEN $2 = 'upstream_group_id' THEN fact.upstream_group_id + 1 ELSE fact.upstream_group_id END,
			CASE WHEN $2 = 'upstream_group_generation' THEN fact.upstream_group_presence_generation + 1 ELSE fact.upstream_group_presence_generation END,
			CASE WHEN $2 = 'upstream_key_local_id' THEN fact.gateway_upstream_key_id + 1 ELSE fact.gateway_upstream_key_id END,
			CASE WHEN $2 = 'upstream_key_generation' THEN fact.upstream_key_generation + 1 ELSE fact.upstream_key_generation END,
			CASE WHEN $2 = 'price_cost_unit_epoch' THEN fact.cost_unit_epoch + 1 ELSE fact.cost_unit_epoch END,
			fact.id,
			CASE WHEN $2 = 'price_fact_version' THEN fact.fact_version + 1 ELSE fact.fact_version END,
			CASE WHEN $2 = 'price_fact_schema_version' THEN fact.schema_version + 1 ELSE fact.schema_version END,
			CASE WHEN $2 = 'price_fact_checksum' THEN repeat('c', 64) ELSE fact.checksum END,
			CASE WHEN $2 = 'price_canonical_group_name' THEN 'secondary' ELSE fact.canonical_group_name END,
			CASE WHEN $2 = 'price_model_name' THEN 'gpt-other' ELSE fact.model_name END,
			CASE WHEN $2 = 'price_operation' THEN 'embedding' ELSE fact.operation END,
			CASE WHEN $2 = 'price_protocol' THEN 'openai_completions' ELSE fact.protocol END,
			CASE WHEN $2 = 'price_input_unit' THEN fact.input_unit_price + 0.01 ELSE fact.input_unit_price END,
			CASE WHEN $2 = 'price_output_unit' THEN fact.output_unit_price + 0.01 ELSE fact.output_unit_price END,
			CASE WHEN $2 = 'price_cache_read_unit' THEN fact.cache_read_unit_price + 0.01 ELSE fact.cache_read_unit_price END,
			CASE WHEN $2 = 'price_cache_write_unit' THEN fact.cache_write_unit_price + 0.01 ELSE fact.cache_write_unit_price END,
			CASE WHEN $2 = 'price_operation_unit' THEN 0::double precision ELSE fact.operation_unit_price END,
			CASE WHEN $2 = 'price_currency' THEN 'EUR' ELSE fact.currency END,
			CASE WHEN $2 = 'price_unit' THEN 'per_operation' ELSE fact.unit END,
			CASE WHEN $2 = 'price_source' THEN 'migration_other' ELSE fact.source END,
			CASE WHEN $2 = 'price_observed_at' THEN fact.observed_at - INTERVAL '1 second' ELSE fact.observed_at END,
			CASE WHEN $2 = 'price_valid_from' THEN fact.valid_from + INTERVAL '1 second' ELSE fact.valid_from END,
			CASE WHEN $2 = 'price_valid_until' THEN NULL ELSE fact.valid_until END,
			'token_components_v1', 1, FALSE, NULL
		FROM public.gateway_request_price_facts AS fact
		WHERE fact.id = $1
		RETURNING id
	`, factID, mutation).Scan(&id)
	return id, err
}

func insertPriceSnapshotAudit(ctx context.Context, db *sql.DB, referencedFactID, snapshotFactID int64) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
			INSERT INTO public.gateway_request_dispatch_audits (
				channel_id, method, path, request_started_at, dispatched_at,
				legacy_ownerless,
				channel_identity_revision, upstream_group_id, upstream_group_generation,
			upstream_key_local_id, upstream_key_generation,
			price_cost_unit_epoch,
			price_fact_id, price_fact_version, price_fact_schema_version, price_fact_checksum,
			price_canonical_group_name, price_model_name, price_operation, price_protocol,
			price_input_unit, price_output_unit, price_cache_read_unit, price_cache_write_unit,
			price_operation_unit, price_currency, price_unit, price_source,
			price_observed_at, price_valid_from, price_valid_until,
			usage_schema, usage_schema_version, usage_operation_present, operation_units
		)
			SELECT
				fact.channel_id, 'POST', '/v1/chat/completions', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
				TRUE,
				fact.channel_account_identity_revision, fact.upstream_group_id,
			fact.upstream_group_presence_generation, fact.gateway_upstream_key_id,
			fact.upstream_key_generation,
			fact.cost_unit_epoch,
			$1, fact.fact_version, fact.schema_version, fact.checksum,
			fact.canonical_group_name, fact.model_name, fact.operation, fact.protocol,
			fact.input_unit_price, fact.output_unit_price, fact.cache_read_unit_price,
			fact.cache_write_unit_price, fact.operation_unit_price, fact.currency,
			fact.unit, fact.source, fact.observed_at, fact.valid_from, fact.valid_until,
			'token_components_v1', 1, FALSE, NULL
		FROM public.gateway_request_price_facts AS fact
		WHERE fact.id = $2
		RETURNING id
	`, referencedFactID, snapshotFactID).Scan(&id)
	return id, err
}

func insertNoFactImageAudit(ctx context.Context, db *sql.DB, priceModelName, unavailableReason string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
			INSERT INTO public.gateway_request_dispatch_audits (
				channel_id, method, path, request_started_at, dispatched_at,
				legacy_ownerless,
				logical_request_trace_id, telemetry_version, routing_mode, gateway_app,
			first_byte_budget_ms, route_exclusion_snapshot_version, route_exclusions_snapshot,
			channel_identity_revision, upstream_group_id, upstream_group_generation,
			upstream_key_local_id, upstream_key_generation,
			price_canonical_group_name, price_model_name, price_operation, price_protocol,
			usage_schema, usage_schema_version, usage_operation_present, operation_units,
			cost_unavailable_reason
			) VALUES (
				901, 'POST', '/v1/images/generations', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
				TRUE,
				repeat('1', 32), 1, 'smart', 'codex',
			1000, 1, jsonb_build_object(
				'version', 1,
				'captured_at', '2026-07-28T00:00:00Z',
				'total_entries', 0,
				'truncated', FALSE,
				'entries', jsonb_build_array()
			),
			11, 21, 31, 41, 51,
			'primary', $1, 'image_generation', 'openai_images',
			'operation_units_v1', 1, FALSE, NULL, $2
		)
		RETURNING id
	`, priceModelName, unavailableReason).Scan(&id)
	return id, err
}

func insertNoFactImageLog(ctx context.Context, db *sql.DB, priceModelName, unavailableReason string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_logs (
			downstream_key_id, channel_id, method, path, status_code, latency_ms, created_at,
			cost, raw_cost, cost_estimated, cost_source, cost_status,
			channel_identity_revision, upstream_group_id, upstream_group_generation,
			upstream_key_local_id, upstream_key_generation,
			price_canonical_group_name, price_model_name, price_operation, price_protocol,
			usage_schema, usage_schema_version, usage_operation_present, operation_units,
			cost_unavailable_reason
		) VALUES (
			1, 901, 'POST', '/v1/images/generations', 200, 1, CURRENT_TIMESTAMP,
			0.75, 0.75, FALSE, 'upstream_reported', 'reported',
			11, 21, 31, 41, 51,
			'primary', $1, 'image_generation', 'openai_images',
			'operation_units_v1', 1, TRUE, 1, $2
		)
		RETURNING id
	`, priceModelName, unavailableReason).Scan(&id)
	return id, err
}

func markPriceSnapshotImageDispatched(t *testing.T, ctx context.Context, db *sql.DB, auditID int64) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET physically_dispatched = TRUE,
		    transport_state = 'armed'
		WHERE id = $1
	`, auditID); err != nil {
		t.Fatalf("mark image dispatch physical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_request_dispatch_audits
		SET stage = 'accepted',
		    response_observed_at = CURRENT_TIMESTAMP,
			    accepted_at = CURRENT_TIMESTAMP,
			    accepted_event_type = 'migration_test_image_accepted',
			    billing_committed = TRUE,
			    usage_operation_present = TRUE,
			    operation_units = 1
		WHERE id = $1
	`, auditID); err != nil {
		t.Fatalf("mark image dispatch accepted: %v", err)
	}
}

func assertPriceSnapshotViolation(t *testing.T, err error, constraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("price snapshot integrity error = %v, want SQLSTATE 23514", err)
	}
	if constraint != "" && pgErr.ConstraintName != constraint {
		t.Fatalf("price snapshot integrity constraint = %q, want %q (error: %v)", pgErr.ConstraintName, constraint, err)
	}
}

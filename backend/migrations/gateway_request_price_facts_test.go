package migrations

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestGatewayRequestPriceFactsMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000045_gateway_request_price_facts.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	compactSQL := strings.Join(strings.Fields(sql), " ")
	for _, required := range []string{
		"add column key_generation bigint not null default 1",
		"check (key_generation > 0)",
		"enforce_gateway_upstream_key_generation",
		"old.key_generation >= 9223372036854775807",
		"new.key_generation := old.key_generation + 1",
		"gateway upstream key generation is database managed",
		"create table public.gateway_request_price_facts",
		"channel_account_identity_revision bigint not null",
		"upstream_group_presence_generation bigint not null",
		"gateway_upstream_key_id bigint not null",
		"upstream_key_generation bigint not null",
		"input_unit_price double precision",
		"output_unit_price double precision",
		"cache_read_unit_price double precision",
		"cache_write_unit_price double precision",
		"operation_unit_price double precision",
		"valid_from timestamp(6) with time zone not null",
		"valid_until timestamp(6) with time zone",
		"isfinite(valid_from)",
		"isfinite(valid_until) and valid_until > valid_from",
		"fact_version > 0 and schema_version = 1",
		"input_unit_price::text not in ('nan', 'infinity', '-infinity')",
		"cache_write_unit_price::text not in ('nan', 'infinity', '-infinity')",
		"unit = 'per_million_tokens'",
		"unit = 'per_operation'",
		"operation = 'text_generation'",
		"operation = 'embedding'",
		"operation = 'image_generation'",
		"protocol = 'openai_embeddings'",
		"protocol in ('openai_images', 'openai_responses')",
		"'openai_chat_completions'",
		"'openai_completions'",
		"'openai_responses'",
		"'anthropic_messages'",
		"checksum ~ '^[0-9a-f]{64}$'",
		"source ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
		"uq_gateway_price_fact_scope_version",
		"reject_gateway_request_price_fact_mutation",
		"before update or delete on public.gateway_request_price_facts",
		"gateway request price facts are append-only",
		"using errcode = '23514'",
		"add column price_canonical_group_name character varying(128) not null default ''",
		"add column price_model_name character varying(256) not null default ''",
		"add column price_source character varying(64) not null default ''",
		"add column price_valid_from timestamp(6) with time zone",
		"add column price_valid_until timestamp(6) with time zone",
		"add column usage_operation_present boolean not null default false",
		"add column operation_units bigint",
		"references public.gateway_request_price_facts(id)",
		"on delete restrict",
		"gateway request log price snapshot is immutable",
		"gateway request dispatch price snapshot is immutable",
		"new.price_canonical_group_name is distinct from old.price_canonical_group_name",
		"new.price_model_name is distinct from old.price_model_name",
		"new.price_source is distinct from old.price_source",
		"price_fact_id is null",
		"and price_fact_version = 0",
		"and price_fact_schema_version = 0",
		"and price_fact_checksum = ''",
		"price_fact_schema_version = 1",
		"chk_gateway_request_logs_usage_schema_shape",
		"usage_schema in ('token_components_v1', 'input_tokens_v1', 'operation_units_v1')",
		"usage_schema_version = 1",
		"operation_units = 1",
		"chk_gateway_request_logs_unavailable_cost_shape",
		"cost_unavailable_reason = 'legacy_unverified_estimate'",
		"do not rewrite the unbounded request history",
		"aggregate backfill persists that classification in bounded batches",
		"add column unavailable_requests bigint not null default 0",
		"unavailable_requests >= 0",
		"unavailable_requests <= requests",
		"result <> 'evidence' or unavailable_requests = 0",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("price facts migration missing %q", required)
		}
	}

	keyGeneration := migrationSQLSection(t, sql,
		"create function public.enforce_gateway_upstream_key_generation()",
		"create trigger trg_gateway_upstream_keys_generation")
	for _, identityField := range []string{
		"new.channel_id is distinct from old.channel_id",
		"new.group_name is distinct from old.group_name",
		"new.upstream_key_id is distinct from old.upstream_key_id",
		"new.upstream_key_name is distinct from old.upstream_key_name",
		"new.ownership_provenance is distinct from old.ownership_provenance",
		"new.provisioning is distinct from old.provisioning",
		"new.api_key_cipher is distinct from old.api_key_cipher",
		"new.api_key_prefix is distinct from old.api_key_prefix",
	} {
		if !strings.Contains(keyGeneration, identityField) {
			t.Fatalf("key generation trigger missing identity field %q", identityField)
		}
	}
	for _, telemetryField := range []string{
		"model_families", "model_ids", "models_synced_at", "models_error",
		"last_used_at", "last_error", "routing_failure_count", "group_ratio",
		"enabled", "provisioning_started_at",
	} {
		if strings.Contains(keyGeneration, telemetryField) {
			t.Fatalf("key generation trigger incorrectly depends on %q", telemetryField)
		}
	}

	if strings.Contains(sql, "update public.gateway_request_logs") {
		t.Fatal("price facts migration performs an unbounded gateway request history update")
	}
	if strings.Contains(sql, "usage_schema_version > 0") {
		t.Fatal("price facts migration accepts an unknown future usage schema version")
	}
	if got := strings.Count(sql, "usage_schema in ('token_components_v1', 'input_tokens_v1', 'operation_units_v1')"); got != 2 {
		t.Fatalf("usage schema allowlist occurs %d times, want request-log and dispatch constraints", got)
	}
	if strings.Contains(sql, "price_protocol <> 'openai_images'") {
		t.Fatal("responses image pricing can bypass the one-operation-unit invariant")
	}
	if got := strings.Count(compactSQL, "usage_operation_present and operation_units = 1"); got != 2 {
		t.Fatalf("image operation-unit invariant occurs %d times, want request-log and dispatch constraints", got)
	}
	if got := strings.Count(sql, "the request contract is independent from pricing scope"); got != 2 {
		t.Fatalf("scope-independent request-contract branch occurs %d times, want request-log and dispatch constraints", got)
	}
	zeroIdentityScope := "channel_identity_revision = 0 and upstream_group_id = 0 and upstream_group_generation = 0 and upstream_key_local_id = 0 and upstream_key_generation = 0 and price_canonical_group_name = '' and price_model_name = ''"
	if got := strings.Count(compactSQL, zeroIdentityScope); got != 2 {
		t.Fatalf("hard-fenced zero-identity branch occurs %d times, want request-log and dispatch constraints", got)
	}
	if got := strings.Count(compactSQL, "channel_id is not null and channel_id > 0"); got != 4 {
		t.Fatalf("explicit non-NULL channel guard occurs %d times, want no-fact and full-fact branches on both tables", got)
	}
	if got := strings.Count(compactSQL, "price_fact_id is not null and price_fact_id > 0"); got != 2 {
		t.Fatalf("explicit non-NULL price-fact guard occurs %d times, want full-fact branches on both tables", got)
	}

	requestImmutable := migrationSQLSection(t, sql,
		"create function public.reject_gateway_request_log_price_snapshot_update()",
		"create trigger trg_gateway_request_logs_price_snapshot_immutable")
	for _, mutableAfterDispatch := range []string{
		"new.usage_input_present",
		"new.usage_output_present",
		"new.usage_cache_read_present",
		"new.usage_cache_write_present",
		"new.usage_operation_present",
		"new.operation_units",
		"new.cost_unavailable_reason",
	} {
		if strings.Contains(requestImmutable, mutableAfterDispatch) {
			t.Fatalf("request-log immutable trigger blocks reconciliation field %q", mutableAfterDispatch)
		}
	}

	dispatchImmutable := migrationSQLSection(t, sql,
		"create function public.reject_gateway_request_dispatch_price_snapshot_update()",
		"create trigger trg_gateway_request_dispatch_price_snapshot_immutable")
	for _, frozenAtDispatch := range []string{
		"new.price_model_name",
		"new.price_source",
		"new.cost_unavailable_reason",
	} {
		if !strings.Contains(dispatchImmutable, frozenAtDispatch) {
			t.Fatalf("dispatch immutable trigger missing %q", frozenAtDispatch)
		}
	}
	for _, mutableAfterDispatch := range []string{
		"new.usage_operation_present",
		"new.operation_units",
	} {
		if strings.Contains(dispatchImmutable, mutableAfterDispatch) {
			t.Fatalf("dispatch immutable trigger blocks response evidence field %q", mutableAfterDispatch)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 45 {
			continue
		}
		if migration.name != "000045_gateway_request_price_facts.sql" || migration.nonTransactional {
			t.Fatalf("version 45 request price facts migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 45 request price facts migration was not loaded")
}

func migrationSQLSection(t *testing.T, sql, start, end string) string {
	t.Helper()
	startIndex := strings.Index(sql, start)
	if startIndex < 0 {
		t.Fatalf("migration section start %q was not found", start)
	}
	endIndex := strings.Index(sql[startIndex:], end)
	if endIndex < 0 {
		t.Fatalf("migration section end %q was not found", end)
	}
	return sql[startIndex : startIndex+endIndex]
}

func TestGatewayRequestPriceFactsMigrationEnforcesAllOrNoneIdentityDatabase(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 45), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 45: %v", err)
	}

	targets := []struct {
		name       string
		constraint string
		insertSQL  string
	}{
		{
			name:       "request_log",
			constraint: "chk_gateway_request_logs_price_fact_shape",
			insertSQL: `
				INSERT INTO public.gateway_request_logs (
					downstream_key_id, channel_id, method, path, status_code, latency_ms, created_at,
					channel_identity_revision, upstream_group_id, upstream_group_generation,
					upstream_key_local_id, upstream_key_generation,
					price_canonical_group_name, price_model_name,
					price_operation, price_protocol, usage_schema, usage_schema_version,
					usage_operation_present, operation_units
				) VALUES (
					1, $1, 'POST', '/v1/images/generations', 200, 1, CURRENT_TIMESTAMP,
					$2, $3, $4, $5, $6, $7, $8,
					'image_generation', 'openai_images', 'operation_units_v1', 1, TRUE, 1
				)
			`,
		},
		{
			name:       "dispatch_audit",
			constraint: "chk_gateway_request_dispatch_price_fact_shape",
			insertSQL: `
				INSERT INTO public.gateway_request_dispatch_audits (
					channel_id, method, path, request_started_at, dispatched_at, legacy_ownerless,
					channel_identity_revision, upstream_group_id, upstream_group_generation,
					upstream_key_local_id, upstream_key_generation,
					price_canonical_group_name, price_model_name,
					price_operation, price_protocol, usage_schema, usage_schema_version,
					usage_operation_present, operation_units
				) VALUES (
					$1, 'POST', '/v1/images/generations', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE,
					$2, $3, $4, $5, $6, $7, $8,
					'image_generation', 'openai_images', 'operation_units_v1', 1, TRUE, 1
				)
			`,
		},
	}

	const completeIdentityMask = (1 << 5) - 1
	scopes := []struct {
		name           string
		canonicalGroup string
		model          string
		acceptedMask   int
	}{
		{name: "empty", acceptedMask: 0},
		{name: "canonical_only", canonicalGroup: "primary", acceptedMask: -1},
		{name: "model_only", model: "gpt-image-1", acceptedMask: -1},
		{name: "complete", canonicalGroup: "primary", model: "gpt-image-1", acceptedMask: completeIdentityMask},
	}
	assertShapeViolation := func(t *testing.T, err error, constraint string) {
		t.Helper()
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != constraint {
			t.Fatalf("invalid price-fact shape error = %v, want %s check violation", err, constraint)
		}
	}
	assertChannelRequired := func(t *testing.T, err error) {
		t.Helper()
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23502" || pgErr.ColumnName != "channel_id" {
			t.Fatalf("null dispatch channel error = %v, want channel_id not-null violation", err)
		}
	}

	for _, target := range targets {
		t.Run(target.name, func(t *testing.T) {
			for _, scope := range scopes {
				for mask := 0; mask <= completeIdentityMask; mask++ {
					mask := mask
					t.Run(fmt.Sprintf("%s/identity_%05b", scope.name, mask), func(t *testing.T) {
						identity := [5]int64{}
						for index := range identity {
							if mask&(1<<index) != 0 {
								identity[index] = 1
							}
						}
						_, err := db.ExecContext(ctx, target.insertSQL, int64(901),
							identity[0], identity[1], identity[2], identity[3], identity[4],
							scope.canonicalGroup, scope.model,
						)
						if mask == scope.acceptedMask {
							if err != nil {
								t.Fatalf("valid no-fact identity shape was rejected: %v", err)
							}
							return
						}
						assertShapeViolation(t, err, target.constraint)
					})
				}
			}
		})
	}

	t.Run("request_log/full_named_no_fact_null_channel", func(t *testing.T) {
		_, err := db.ExecContext(ctx, targets[0].insertSQL,
			nil, int64(1), int64(1), int64(1), int64(1), int64(1), "primary", "gpt-image-1",
		)
		assertShapeViolation(t, err, targets[0].constraint)
	})

	var factID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_price_facts (
			channel_id, channel_account_identity_revision, upstream_group_id,
			canonical_group_name, upstream_group_presence_generation,
			gateway_upstream_key_id, upstream_key_generation,
			model_name, operation, protocol, operation_unit_price,
			unit, currency, source, observed_at, valid_from, fact_version, schema_version, checksum
		) VALUES (
			901, 1, 1, 'primary', 1, 1, 1,
			'gpt-image-1', 'image_generation', 'openai_images', 0.25,
			'per_operation', 'USD', 'migration_test', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, 1, repeat('a', 64)
		)
		RETURNING id
	`).Scan(&factID); err != nil {
		t.Fatalf("insert price fact fixture: %v", err)
	}

	factTargets := []struct {
		name       string
		constraint string
		insertSQL  string
	}{
		{
			name:       "request_log",
			constraint: "chk_gateway_request_logs_price_fact_shape",
			insertSQL: `
				INSERT INTO public.gateway_request_logs (
					downstream_key_id, channel_id, method, path, status_code, latency_ms, created_at,
					channel_identity_revision, upstream_group_id, upstream_group_generation,
					upstream_key_local_id, upstream_key_generation,
					price_fact_id, price_cost_unit_epoch, price_fact_version, price_fact_schema_version, price_fact_checksum,
					price_canonical_group_name, price_model_name, price_operation, price_protocol,
					price_operation_unit, price_currency, price_unit, price_source, price_observed_at, price_valid_from,
					usage_schema, usage_schema_version, usage_operation_present, operation_units
				) VALUES (
					1, $1, 'POST', '/v1/images/generations', 200, 1, CURRENT_TIMESTAMP,
					1, 1, 1, 1, 1,
					$2, 1, 1, 1, repeat('a', 64),
					'primary', 'gpt-image-1', 'image_generation', 'openai_images',
					0.25, 'USD', 'per_operation', 'migration_test', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
					'operation_units_v1', 1, TRUE, 1
				)
			`,
		},
		{
			name:       "dispatch_audit",
			constraint: "chk_gateway_request_dispatch_price_fact_shape",
			insertSQL: `
				INSERT INTO public.gateway_request_dispatch_audits (
					channel_id, method, path, request_started_at, dispatched_at, legacy_ownerless,
					channel_identity_revision, upstream_group_id, upstream_group_generation,
					upstream_key_local_id, upstream_key_generation,
					price_fact_id, price_cost_unit_epoch, price_fact_version, price_fact_schema_version, price_fact_checksum,
					price_canonical_group_name, price_model_name, price_operation, price_protocol,
					price_operation_unit, price_currency, price_unit, price_source, price_observed_at, price_valid_from,
					usage_schema, usage_schema_version, usage_operation_present, operation_units
				) VALUES (
					$1, 'POST', '/v1/images/generations', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE,
					1, 1, 1, 1, 1,
					$2, 1, 1, 1, repeat('a', 64),
					'primary', 'gpt-image-1', 'image_generation', 'openai_images',
					0.25, 'USD', 'per_operation', 'migration_test', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
					'operation_units_v1', 1, TRUE, 1
				)
			`,
		},
	}

	for _, target := range factTargets {
		t.Run(target.name+"/full_fact_complete", func(t *testing.T) {
			if _, err := db.ExecContext(ctx, target.insertSQL, int64(901), factID); err != nil {
				t.Fatalf("valid full price-fact shape was rejected: %v", err)
			}
		})
	}
	t.Run("request_log/full_fact_null_channel", func(t *testing.T) {
		_, err := db.ExecContext(ctx, factTargets[0].insertSQL, nil, factID)
		assertShapeViolation(t, err, factTargets[0].constraint)
	})
	t.Run("dispatch_audit/full_fact_null_channel", func(t *testing.T) {
		_, err := db.ExecContext(ctx, factTargets[1].insertSQL, nil, factID)
		assertChannelRequired(t, err)
	})
	for _, target := range factTargets {
		t.Run(target.name+"/full_fact_metadata_null_fact_id", func(t *testing.T) {
			_, err := db.ExecContext(ctx, target.insertSQL, int64(901), nil)
			assertShapeViolation(t, err, target.constraint)
		})
	}
}

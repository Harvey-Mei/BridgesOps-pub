package migrations

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestChannelAPIKeyOperationGenerationsMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000072_channel_api_key_operation_generations.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column operation_generation integer not null default 1",
		"add column channel_identity_revision bigint not null default 0",
		"add column upstream_group_id bigint not null default 0",
		"add column presence_generation bigint not null default 0",
		"add column absence_confirmation_count integer not null default 0",
		"create table public.channel_api_key_operation_generations",
		"create unique index idx_channel_api_key_operation_generation",
		"foreign key (operation_id)",
		"references public.channel_api_key_operations(id)",
		"status = 'superseded'",
		"superseded_by_generation = operation_generation + 1",
		"channel api key operation generations are append-only",
		"before update or delete on public.channel_api_key_operation_generations",
		"before truncate on public.channel_api_key_operation_generations",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("operation generation migration missing %q", required)
		}
	}
	if strings.Contains(sql, "drop index public.idx_channel_api_key_operation_scope") {
		t.Fatal("operation generation migration breaks the previous binary's scope conflict target")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version == 72 {
			if migration.name != "000072_channel_api_key_operation_generations.sql" || migration.nonTransactional {
				t.Fatalf("version 72 operation generation migration = %#v", migration)
			}
			return
		}
	}
	t.Fatal("version 72 operation generation migration was not loaded")
}

func TestChannelAPIKeyOperationGenerationsMigrationPreservesLegacyCurrentSlot(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 71), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 71: %v", err)
	}

	var channelID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('operation generations', 'newapi', 'https://operation-generations.example', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatalf("insert migration channel: %v", err)
	}
	var oldID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_api_key_operations (
			channel_id, provider, scope_hash, request_fingerprint, managed_name,
			group_identity, status, created_at, updated_at
		) VALUES ($1, 'newapi', repeat('a', 64), repeat('b', 64), 'managed',
			'default', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING id
	`, channelID).Scan(&oldID); err != nil {
		t.Fatalf("insert legacy operation: %v", err)
	}
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 72), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply operation generation migration: %v", err)
	}
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 71), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("start previous migration ledger after version 72: %v", err)
	}
	var generation int
	if err := db.QueryRowContext(ctx, `SELECT operation_generation FROM public.channel_api_key_operations WHERE id = $1`, oldID).Scan(&generation); err != nil || generation != 1 {
		t.Fatalf("legacy operation generation = %d err=%v", generation, err)
	}

	assertLegacyConflict := func(stage string) {
		t.Helper()
		result, err := db.ExecContext(ctx, `
			INSERT INTO public.channel_api_key_operations (
				channel_id, provider, scope_hash, request_fingerprint,
				managed_name, group_identity, status, created_at, updated_at
			) VALUES ($1, 'newapi', repeat('a', 64), repeat('b', 64),
				'managed', 'default', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT (channel_id, provider, scope_hash) DO NOTHING
		`, channelID)
		if err != nil {
			t.Fatalf("legacy ON CONFLICT %s: %v", stage, err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 0 {
			t.Fatalf("legacy ON CONFLICT rows %s = %d, %v", stage, rows, err)
		}
	}
	assertLegacyConflict("after migration")

	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.channel_api_key_operation_generations (
			operation_id, channel_id, provider, scope_hash, operation_generation,
			request_fingerprint, managed_name, group_identity, status,
			channel_identity_revision, upstream_group_id, presence_generation,
			canonical_group_name, absence_confirmation_count,
			absence_first_confirmed_at, absence_last_confirmed_at,
			superseded_by_generation, superseded_at, created_at, updated_at
		) SELECT id, channel_id, provider, scope_hash, operation_generation,
			request_fingerprint, managed_name, group_identity, 'superseded',
			1, 1, 1, 'default', 2,
			CURRENT_TIMESTAMP - INTERVAL '20 minutes', CURRENT_TIMESTAMP - INTERVAL '10 minutes',
			2, CURRENT_TIMESTAMP - INTERVAL '10 minutes', created_at, CURRENT_TIMESTAMP - INTERVAL '10 minutes'
		FROM public.channel_api_key_operations WHERE id = $1
	`, oldID); err != nil {
		t.Fatalf("archive legacy generation: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.channel_api_key_operations
		SET operation_generation = 2, created_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`, oldID); err != nil {
		t.Fatalf("advance current operation generation: %v", err)
	}
	assertLegacyConflict("after generation advance")

	var currentCount, historyCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.channel_api_key_operations WHERE id = $1 AND operation_generation = 2`, oldID).Scan(&currentCount); err != nil || currentCount != 1 {
		t.Fatalf("current operation slot = %d err=%v", currentCount, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.channel_api_key_operation_generations WHERE operation_id = $1 AND operation_generation = 1`, oldID).Scan(&historyCount); err != nil || historyCount != 1 {
		t.Fatalf("archived operation generation = %d err=%v", historyCount, err)
	}

	_, duplicateHistoryErr := db.ExecContext(ctx, `
		INSERT INTO public.channel_api_key_operation_generations (
			operation_id, channel_id, provider, scope_hash, operation_generation,
			request_fingerprint, managed_name, group_identity, status,
			channel_identity_revision, upstream_group_id, presence_generation,
			canonical_group_name, absence_confirmation_count,
			absence_first_confirmed_at, absence_last_confirmed_at,
			superseded_by_generation, superseded_at, created_at, updated_at
		) SELECT operation_id, channel_id, provider, scope_hash, operation_generation,
			request_fingerprint, managed_name, group_identity, status,
			channel_identity_revision, upstream_group_id, presence_generation,
			canonical_group_name, absence_confirmation_count,
			absence_first_confirmed_at, absence_last_confirmed_at,
			superseded_by_generation, superseded_at, created_at, updated_at
		FROM public.channel_api_key_operation_generations
		WHERE operation_id = $1 AND operation_generation = 1
	`, oldID)
	assertPostgresCode(t, duplicateHistoryErr, "23505")
	_, updateHistoryErr := db.ExecContext(ctx, `
		UPDATE public.channel_api_key_operation_generations
		SET updated_at = CURRENT_TIMESTAMP
		WHERE operation_id = $1 AND operation_generation = 1
	`, oldID)
	assertPostgresCode(t, updateHistoryErr, "23514")
	_, deleteHistoryErr := db.ExecContext(ctx, `
		DELETE FROM public.channel_api_key_operation_generations
		WHERE operation_id = $1 AND operation_generation = 1
	`, oldID)
	assertPostgresCode(t, deleteHistoryErr, "23514")

	_, duplicateCurrentErr := db.ExecContext(ctx, `
		INSERT INTO public.channel_api_key_operations (
			channel_id, provider, scope_hash, request_fingerprint, managed_name, group_identity, status,
			created_at, updated_at
		) VALUES ($1, 'newapi', repeat('a', 64), repeat('b', 64), 'managed', 'default', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, channelID)
	assertPostgresCode(t, duplicateCurrentErr, "23505")
}

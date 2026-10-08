package migrations

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestGatewayRequestPriceImportReceiptsMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000063_gateway_request_price_import_receipts.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.gateway_request_price_import_commands",
		"actor character varying(128) not null",
		"idempotency_key character varying(128) not null",
		"payload_digest character(64) not null",
		"payload_digest_version smallint not null",
		"unique (idempotency_key)",
		"payload_digest ~ '^[0-9a-f]{64}$'",
		"payload_digest_version = 1",
		"item_count between 1 and 500",
		"create table public.gateway_request_price_import_items",
		"primary key (command_id, item_index)",
		"unique (price_fact_id)",
		"references public.gateway_request_price_import_commands(id)",
		"references public.gateway_request_price_facts(id)",
		"enforce_gateway_request_price_import_item_consistency",
		"before insert on public.gateway_request_price_import_items",
		"fact.source = 'administrator'",
		"fact.observed_at <= new.created_at",
		"fact.valid_from is not distinct from new.created_at",
		"enforce_gateway_request_price_administrator_receipt",
		"create constraint trigger gateway_request_price_administrator_receipt",
		"existing administrator request price facts have no import receipts",
		"enforce_gateway_request_price_import_command_complete",
		"create constraint trigger gateway_request_price_import_command_complete",
		"deferrable initially deferred",
		"reject_gateway_request_price_import_receipt_mutation",
		"before update or delete on public.gateway_request_price_import_commands",
		"before truncate on public.gateway_request_price_import_commands",
		"before update or delete on public.gateway_request_price_import_items",
		"before truncate on public.gateway_request_price_import_items",
		"using errcode = '23514'",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("price import receipt migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 63 {
			continue
		}
		if migration.name != "000063_gateway_request_price_import_receipts.sql" || migration.nonTransactional {
			t.Fatalf("version 63 price import receipt migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 63 price import receipt migration was not loaded")
}

func TestGatewayRequestPriceImportReceiptsDatabase(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 63), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 63: %v", err)
	}

	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin price import receipt transaction: %v", err)
	}
	factID := insertPriceImportReceiptFact(t, ctx, tx, 1, "administrator", 1.25, 'c', now)
	commandID := insertPriceImportReceiptCommand(t, ctx, tx, "price-import-command-1", 1, now)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO public.gateway_request_price_import_items (
			command_id, item_index, price_fact_id, created_at
		) VALUES ($1, 0, $2, $3)
	`, commandID, factID, now); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert price import item: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit complete price import receipt: %v", err)
	}

	t.Run("standalone administrator fact cannot commit", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		insertPriceImportReceiptFact(t, ctx, tx, 99, "administrator", 9.99, '9', now)
		assertPriceImportReceiptViolation(t, tx.Commit())
	})

	for _, mutation := range []struct {
		name string
		sql  string
		args []any
	}{
		{name: "update_command", sql: `UPDATE public.gateway_request_price_import_commands SET actor = 'other' WHERE id = $1`, args: []any{commandID}},
		{name: "delete_command", sql: `DELETE FROM public.gateway_request_price_import_commands WHERE id = $1`, args: []any{commandID}},
		{name: "update_item", sql: `UPDATE public.gateway_request_price_import_items SET item_index = 1 WHERE command_id = $1`, args: []any{commandID}},
		{name: "delete_item", sql: `DELETE FROM public.gateway_request_price_import_items WHERE command_id = $1`, args: []any{commandID}},
		{name: "truncate_items", sql: `TRUNCATE public.gateway_request_price_import_items`},
		{name: "truncate_receipts", sql: `TRUNCATE public.gateway_request_price_import_items, public.gateway_request_price_import_commands`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, mutation.sql, mutation.args...)
			assertPriceImportReceiptViolation(t, err)
		})
	}

	var installedTriggers int
	if err := db.QueryRowContext(ctx, `
		WITH expected(trigger_name, table_name) AS (VALUES
			('trg_gateway_request_price_import_commands_append_only', 'gateway_request_price_import_commands'),
			('trg_gateway_request_price_import_commands_reject_truncate', 'gateway_request_price_import_commands'),
			('trg_gateway_request_price_import_items_append_only', 'gateway_request_price_import_items'),
			('trg_gateway_request_price_import_items_reject_truncate', 'gateway_request_price_import_items'),
			('trg_gateway_request_price_import_items_consistency', 'gateway_request_price_import_items'),
			('gateway_request_price_import_command_complete', 'gateway_request_price_import_commands'),
			('gateway_request_price_administrator_receipt', 'gateway_request_price_facts')
		)
		SELECT count(*)
		FROM expected
		JOIN pg_catalog.pg_trigger AS installed
		  ON installed.tgname = expected.trigger_name
		 AND installed.tgenabled = 'O'
		 AND NOT installed.tgisinternal
		JOIN pg_catalog.pg_class AS relation
		  ON relation.oid = installed.tgrelid
		 AND relation.relname = expected.table_name
		JOIN pg_catalog.pg_namespace AS namespace
		  ON namespace.oid = relation.relnamespace
		 AND namespace.nspname = 'public'
	`).Scan(&installedTriggers); err != nil || installedTriggers != 7 {
		t.Fatalf("installed price import receipt triggers = %d, err=%v", installedTriggers, err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO public.gateway_request_price_import_commands (
			actor, idempotency_key, payload_digest, payload_digest_version, item_count, imported_at, created_at
		) VALUES ('other-admin', 'price-import-command-1', $1, 1, 1, $2, $2)
	`, strings.Repeat("b", 64), now)
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("duplicate global idempotency key error = %v, want SQLSTATE 23505", err)
	}
}

func TestGatewayRequestPriceImportReceiptsDatabaseRejectsIncompleteSets(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 63), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 63: %v", err)
	}

	now := time.Date(2026, 7, 28, 3, 0, 0, 0, time.UTC)
	otherSourceFactID := insertPriceImportReceiptFact(t, ctx, db, 2, "migration_test", 1.50, 'e', now)
	completeTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	administratorFactID := insertPriceImportReceiptFact(t, ctx, completeTx, 1, "administrator", 1.25, 'd', now)
	completeCommandID := insertPriceImportReceiptCommand(t, ctx, completeTx, "price-import-complete", 1, now)
	if _, err := completeTx.ExecContext(ctx, `
		INSERT INTO public.gateway_request_price_import_items (
			command_id, item_index, price_fact_id, created_at
		) VALUES ($1, 0, $2, $3)
	`, completeCommandID, administratorFactID, now); err != nil {
		_ = completeTx.Rollback()
		t.Fatalf("insert complete receipt fixture: %v", err)
	}
	if err := completeTx.Commit(); err != nil {
		t.Fatalf("commit complete receipt fixture: %v", err)
	}

	t.Run("incomplete command cannot commit", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		insertPriceImportReceiptCommand(t, ctx, tx, "price-import-incomplete", 1, now)
		assertPriceImportReceiptViolation(t, tx.Commit())
	})

	for _, test := range []struct {
		name      string
		key       string
		itemIndex int
		factID    int64
		factKind  string
		createdAt time.Time
	}{
		{name: "ordinal outside command count", key: "price-import-bad-ordinal", itemIndex: 1, factID: administratorFactID, createdAt: now},
		{name: "item timestamp differs", key: "price-import-bad-item-time", itemIndex: 0, factID: administratorFactID, createdAt: now.Add(time.Second)},
		{name: "fact source is not administrator", key: "price-import-bad-source", itemIndex: 0, factID: otherSourceFactID, createdAt: now},
		{name: "fact valid from differs", key: "price-import-bad-valid-from", itemIndex: 0, factKind: "wrong_valid_from", createdAt: now},
		{name: "fact created at differs", key: "price-import-bad-created-at", itemIndex: 0, factKind: "wrong_created_at", createdAt: now},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			factID := test.factID
			switch test.factKind {
			case "wrong_valid_from":
				factID = insertPriceImportReceiptFactAt(t, ctx, tx, 3, "administrator", 1.75, 'f', now.Add(time.Minute), now)
			case "wrong_created_at":
				factID = insertPriceImportReceiptFactAt(t, ctx, tx, 4, "administrator", 1.875, '0', now, now.Add(time.Minute))
			}
			commandID := insertPriceImportReceiptCommand(t, ctx, tx, test.key, 1, now)
			_, err = tx.ExecContext(ctx, `
				INSERT INTO public.gateway_request_price_import_items (
					command_id, item_index, price_fact_id, created_at
				) VALUES ($1, $2, $3, $4)
			`, commandID, test.itemIndex, factID, test.createdAt)
			assertPriceImportReceiptViolation(t, err)
		})
	}

	t.Run("orphan command uses foreign key", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO public.gateway_request_price_import_items (
				command_id, item_index, price_fact_id, created_at
			) VALUES (9223372036854775807, 0, $1, $2)
		`, otherSourceFactID, now)
		assertPriceImportReceiptSQLState(t, err, "23503")
	})

	t.Run("orphan fact uses foreign key", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		commandID := insertPriceImportReceiptCommand(t, ctx, tx, "price-import-orphan-fact", 1, now)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO public.gateway_request_price_import_items (
				command_id, item_index, price_fact_id, created_at
			) VALUES ($1, 0, 9223372036854775807, $2)
		`, commandID, now)
		assertPriceImportReceiptSQLState(t, err, "23503")
	})

	t.Run("fact can belong to only one command", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		commandID := insertPriceImportReceiptCommand(t, ctx, tx, "price-import-duplicate-fact", 1, now)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO public.gateway_request_price_import_items (
				command_id, item_index, price_fact_id, created_at
			) VALUES ($1, 0, $2, $3)
		`, commandID, administratorFactID, now)
		assertPriceImportReceiptSQLState(t, err, "23505")
	})

	t.Run("complete command cannot gain later item", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO public.gateway_request_price_import_items (
				command_id, item_index, price_fact_id, created_at
			) VALUES ($1, 1, $2, $3)
		`, completeCommandID, otherSourceFactID, now)
		assertPriceImportReceiptViolation(t, err)
	})

	t.Run("uncommitted command cannot be reopened concurrently", func(t *testing.T) {
		commandTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		additionalFactID := insertPriceImportReceiptFact(t, ctx, commandTx, 5, "administrator", 2.00, '1', now)
		commandID := insertPriceImportReceiptCommand(t, ctx, commandTx, "price-import-concurrent-reopen", 1, now)
		if _, err := commandTx.ExecContext(ctx, `
			INSERT INTO public.gateway_request_price_import_items (
				command_id, item_index, price_fact_id, created_at
			) VALUES ($1, 0, $2, $3)
		`, commandID, additionalFactID, now); err != nil {
			_ = commandTx.Rollback()
			t.Fatalf("insert concurrent command receipt: %v", err)
		}

		insertResult := make(chan error, 1)
		go func() {
			_, insertErr := db.ExecContext(ctx, `
				INSERT INTO public.gateway_request_price_import_items (
					command_id, item_index, price_fact_id, created_at
				) VALUES ($1, 1, $2, $3)
			`, commandID, otherSourceFactID, now)
			insertResult <- insertErr
		}()
		time.Sleep(100 * time.Millisecond)
		if err := commandTx.Commit(); err != nil {
			t.Fatalf("commit concurrent command fixture: %v", err)
		}

		select {
		case err := <-insertResult:
			var pgErr *pgconn.PgError
			if err == nil || !errors.As(err, &pgErr) || (pgErr.Code != "23503" && pgErr.Code != "23514") {
				t.Fatalf("concurrent receipt insert error = %v, want SQLSTATE 23503 or 23514", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent receipt insert did not finish after command commit")
		}

		var itemCount int
		if err := db.QueryRowContext(ctx, `
			SELECT count(*)
			FROM public.gateway_request_price_import_items
			WHERE command_id = $1
		`, commandID).Scan(&itemCount); err != nil || itemCount != 1 {
			t.Fatalf("concurrent command receipt count = %d, err=%v", itemCount, err)
		}
	})
}

func TestGatewayRequestPriceImportReceiptsMigrationRejectsExistingAdministratorFacts(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 62), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 62: %v", err)
	}
	now := time.Date(2026, 7, 28, 4, 0, 0, 0, time.UTC)
	insertPriceImportReceiptFact(t, ctx, db, 1, "administrator", 1.25, 'a', now)
	err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 63), ".", defaultRunnerConfig())
	assertPriceImportReceiptViolation(t, err)
}

func assertPriceImportReceiptViolation(t *testing.T, err error) {
	t.Helper()
	assertPriceImportReceiptSQLState(t, err, "23514")
}

func assertPriceImportReceiptSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("price import receipt error = %v, want SQLSTATE %s", err, want)
	}
}

func insertPriceImportReceiptCommand(t *testing.T, ctx context.Context, tx *sql.Tx, key string, itemCount int, importedAt time.Time) int64 {
	t.Helper()
	var commandID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_price_import_commands (
			actor, idempotency_key, payload_digest, payload_digest_version,
			item_count, imported_at, created_at
		) VALUES ('admin-user', $1, $2, 1, $3, $4, $4)
		RETURNING id
	`, key, strings.Repeat("a", 64), itemCount, importedAt).Scan(&commandID); err != nil {
		t.Fatalf("insert price import command %q: %v", key, err)
	}
	return commandID
}

type priceImportReceiptQueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func insertPriceImportReceiptFact(t *testing.T, ctx context.Context, db priceImportReceiptQueryRower, version int64, source string, inputPrice float64, checksumByte rune, validFrom time.Time) int64 {
	t.Helper()
	return insertPriceImportReceiptFactAt(t, ctx, db, version, source, inputPrice, checksumByte, validFrom, validFrom)
}

func insertPriceImportReceiptFactAt(t *testing.T, ctx context.Context, db priceImportReceiptQueryRower, version int64, source string, inputPrice float64, checksumByte rune, validFrom, createdAt time.Time) int64 {
	t.Helper()
	observedAt := validFrom
	if createdAt.Before(observedAt) {
		observedAt = createdAt
	}
	var factID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_request_price_facts (
			channel_id, channel_account_identity_revision, upstream_group_id,
			canonical_group_name, upstream_group_presence_generation,
			gateway_upstream_key_id, upstream_key_generation,
			model_name, operation, protocol,
			input_unit_price, output_unit_price, cache_read_unit_price, cache_write_unit_price,
			unit, currency, source, observed_at, valid_from, valid_until,
			fact_version, schema_version, checksum, created_at
		) VALUES (
			901, 11, 21, 'primary', 31, 41, 51,
			'gpt-test', 'text_generation', 'openai_chat_completions',
			$1, 2.5, 0.2, 0.4,
			'per_million_tokens', 'USD', $2, $3, $4, $5,
			$6, 1, $7, $8
		)
		RETURNING id
	`, inputPrice, source, observedAt, validFrom, validFrom.Add(24*time.Hour), version, strings.Repeat(string(checksumByte), 64), createdAt).Scan(&factID); err != nil {
		t.Fatalf("insert price import fact version %d: %v", version, err)
	}
	return factID
}

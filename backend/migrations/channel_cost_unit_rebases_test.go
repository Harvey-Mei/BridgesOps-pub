package migrations

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestChannelCostUnitRebaseMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000059_channel_cost_unit_rebases.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, required := range []string{
		"migration 000059 requires migration 000036 channel/account identity schema",
		"('gateway_channel_upstream_account_bindings', 'channel_identity_revision')",
		"('gateway_channel_upstream_account_bindings', 'observed_at')",
		"('gateway_upstream_accounts', 'external_account_id_hash')",
		"ALTER TABLE public.channels",
		"ADD COLUMN cost_unit_epoch bigint NOT NULL DEFAULT 1",
		"ALTER TABLE public.channel_cost_states",
		"channel_cost_states_cost_unit_epoch_positive",
		"ADD COLUMN observed_provider_factor numeric",
		"ADD COLUMN observed_provider_factor_source character varying(32) NOT NULL DEFAULT ''",
		"ADD COLUMN observed_provider_factor_sampled_at timestamp with time zone",
		"observed_provider_factor IS NOT NULL",
		"legacy advertised provider factor requires administrator evidence adoption",
		"ALTER TABLE public.rate_snapshots",
		"rate_snapshots_cost_unit_epoch_positive",
		"ALTER TABLE public.rate_change_logs",
		"ADD COLUMN cost_unit_epoch_before bigint NOT NULL DEFAULT 1",
		"ADD COLUMN cost_unit_epoch_after bigint NOT NULL DEFAULT 1",
		"cost_unit_epoch_after = cost_unit_epoch_before + 1",
		"ALTER TABLE public.balance_snapshots",
		"balance_snapshots_cost_unit_epoch_positive",
		"ALTER TABLE public.cost_snapshots",
		"cost_snapshots_cost_unit_epoch_positive",
		"CREATE TABLE public.channel_cost_unit_rebases",
		"channel_identity_revision bigint NOT NULL",
		"channel_name_snapshot character varying(128) NOT NULL",
		"channel_type_snapshot character varying(32) NOT NULL",
		"account_identity_fingerprint character varying(64) NOT NULL",
		"account_identity_fingerprint_version bigint NOT NULL",
		"account_identity_fingerprint_source character varying(128) NOT NULL",
		"account_identity_fingerprint_observed_at timestamp with time zone NOT NULL",
		"account_identity_fingerprint ~ '^[0-9a-f]{64}$'",
		"account_identity_fingerprint_version > 0",
		"isfinite(account_identity_fingerprint_observed_at)",
		"request_fingerprint character varying(64) NOT NULL",
		"rate_transition_at timestamp with time zone NOT NULL",
		"rate_raw_scale numeric NOT NULL",
		"rate_evidence_group_count integer NOT NULL",
		"rate_evidence_digest character varying(64) NOT NULL",
		"counter_transition character varying(32) NOT NULL",
		"rate_transition_at > cost_sampled_at_before",
		"rate_transition_at <= cost_sampled_at_after",
		"UNIQUE (channel_id, channel_identity_revision, idempotency_key)",
		"applied_state_version = expected_state_version + 1",
		"applied_cost_unit_epoch = expected_cost_unit_epoch + 1",
		"previous_multiplier_mode IN ('divide', 'multiply')",
		"current_multiplier_mode IN ('divide', 'multiply')",
		"CREATE INDEX idx_channel_cost_unit_rebases_channel_applied",
		"CREATE TABLE public.channel_cost_unit_rebase_rate_items",
		"FOREIGN KEY (rebase_id) REFERENCES public.channel_cost_unit_rebases(id) ON DELETE RESTRICT",
		"UNIQUE (rebase_id, ordinal)",
		"UNIQUE (rebase_id, model_name)",
		"CREATE FUNCTION public.reject_channel_cost_unit_rebase_mutation()",
		"BEFORE UPDATE OR DELETE ON public.channel_cost_unit_rebases",
		"BEFORE UPDATE OR DELETE ON public.channel_cost_unit_rebase_rate_items",
		"BEFORE TRUNCATE ON public.channel_cost_unit_rebases",
		"BEFORE TRUNCATE ON public.channel_cost_unit_rebase_rate_items",
		"USING ERRCODE = '23514'",
		"ALTER TABLE public.gateway_channel_upstream_account_binding_audits",
		"ADD COLUMN cost_state_cleared boolean NOT NULL DEFAULT false",
		"ADD COLUMN raw_total_cost_before numeric",
		"ADD COLUMN actual_total_cost_before numeric",
		"chk_gateway_channel_account_audit_cost_clear_shape",
		"previous_channel_identity_revision = channel_identity_revision - 1",
		"cost_state_version_after = cost_state_version_before + 1",
		"CREATE FUNCTION public.reject_gateway_channel_account_binding_audit_mutation()",
		"BEFORE UPDATE OR DELETE ON public.gateway_channel_upstream_account_binding_audits",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("channel cost unit rebase migration missing %q", required)
		}
	}
	if strings.Contains(sql, "FOREIGN KEY (channel_id) REFERENCES public.channels(id)") || strings.Contains(sql, "ON DELETE CASCADE") {
		t.Fatal("immutable channel cost-unit audit must survive channel deletion")
	}
	for _, forbidden := range []string{
		"CREATE TABLE public.gateway_upstream_accounts",
		"CREATE TABLE public.gateway_channel_upstream_account_bindings",
		"ADD COLUMN upstream_account_identity_revision",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 59 must depend on migration 36, not recreate it: found %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 59 {
			continue
		}
		if migration.name != "000059_channel_cost_unit_rebases.sql" || migration.nonTransactional {
			t.Fatalf("version 59 channel cost unit rebase migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 59 channel cost unit rebase migration was not loaded")
}

func TestChannelCostUnitRebaseMigrationPausesLegacyAdvertisedStateWithoutGuessingFactor(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThroughVersion(t, 58), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations before version 59: %v", err)
	}

	type fixture struct {
		name               string
		channelType        string
		rechargeMultiplier any
		source             string
		status             string
		lastError          string
		wantStatus         string
		wantLastError      string
	}
	fixtures := []fixture{
		{
			name: "legacy advertised", channelType: "sub2api", source: "advertised", status: "ready",
			wantStatus: "needs_review", wantLastError: "legacy advertised provider factor requires administrator evidence adoption",
		},
		{
			name: "preserve specific review error", channelType: "sub2api", source: "advertised", status: "needs_review", lastError: "operator evidence already required",
			wantStatus: "needs_review", wantLastError: "operator evidence already required",
		},
		{
			name: "preserve approximate migration marker", channelType: "sub2api", source: "advertised", status: "needs_review", lastError: "legacy advertised provider factor requires administrator evidence adoption ",
			wantStatus: "needs_review", wantLastError: "legacy advertised provider factor requires administrator evidence adoption ",
		},
		{
			name: "explicit multiplier", channelType: "sub2api", rechargeMultiplier: 10, source: "advertised", status: "ready",
			wantStatus: "ready",
		},
		{
			name: "default source", channelType: "sub2api", source: "default", status: "ready",
			wantStatus: "ready",
		},
		{
			name: "other provider", channelType: "newapi", source: "advertised", status: "ready",
			wantStatus: "ready",
		},
	}
	type expectedState struct {
		channelID     int64
		wantStatus    string
		wantLastError string
	}
	expected := make([]expectedState, 0, len(fixtures))
	for _, item := range fixtures {
		var channelID int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.channels (name, type, site_url, username, password_cipher, recharge_multiplier)
			VALUES ($1, $2, 'https://example.com', '', '', $3)
			RETURNING id
		`, item.name, item.channelType, item.rechargeMultiplier).Scan(&channelID); err != nil {
			t.Fatalf("insert %s channel: %v", item.name, err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO public.channel_cost_states (
				channel_id, raw_balance, actual_inventory, funding_factor,
				raw_today_cost, actual_today_cost, raw_total_cost, actual_total_cost,
				balance_sampled_at, cost_sampled_at, version, source, status, last_error
			) VALUES ($1, 67.5, 6.75, 0.1, 3.4, 0.34, 802.3, 80.23,
				CURRENT_TIMESTAMP - interval '2 minutes', CURRENT_TIMESTAMP - interval '1 minute',
				7, $2, $3, $4)
		`, channelID, item.source, item.status, item.lastError); err != nil {
			t.Fatalf("insert %s state: %v", item.name, err)
		}
		expected = append(expected, expectedState{channelID: channelID, wantStatus: item.wantStatus, wantLastError: item.wantLastError})
	}

	if err := applyMigrations(ctx, db, embeddedMigrationsThroughVersion(t, 59), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migration 59: %v", err)
	}
	for index, item := range expected {
		var status, lastError string
		var version, epoch int64
		var rawBalance, fundingFactor, rawTotalCost string
		var observedFactor, observedAt sql.NullString
		var observedSource string
		if err := db.QueryRowContext(ctx, `
			SELECT status, COALESCE(last_error, ''), version, cost_unit_epoch,
			       raw_balance::text, funding_factor::text, raw_total_cost::text,
			       observed_provider_factor::text, observed_provider_factor_source,
			       observed_provider_factor_sampled_at::text
			FROM public.channel_cost_states WHERE channel_id = $1
		`, item.channelID).Scan(&status, &lastError, &version, &epoch, &rawBalance, &fundingFactor, &rawTotalCost,
			&observedFactor, &observedSource, &observedAt); err != nil {
			t.Fatalf("load %s migrated state: %v", fixtures[index].name, err)
		}
		if status != item.wantStatus || lastError != item.wantLastError {
			t.Fatalf("%s state = status %q error %q, want %q/%q", fixtures[index].name, status, lastError, item.wantStatus, item.wantLastError)
		}
		if version != 7 || epoch != 1 || rawBalance != "67.5" || fundingFactor != "0.1" || rawTotalCost != "802.3" {
			t.Fatalf("%s monetary state changed: version=%d epoch=%d raw=%s factor=%s total=%s", fixtures[index].name, version, epoch, rawBalance, fundingFactor, rawTotalCost)
		}
		if observedFactor.Valid || observedSource != "" || observedAt.Valid {
			t.Fatalf("%s migration manufactured provider evidence: factor=%v source=%q at=%v", fixtures[index].name, observedFactor, observedSource, observedAt)
		}
	}
}

func TestChannelCostUnitRebaseMigrationRejectsPartialProviderObservation(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	applyCostUnitRebaseMigrations(t, ctx, db)

	var channelID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('partial provider observation', 'newapi', 'https://example.com', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.channel_cost_states (
			channel_id, funding_factor, version, source, status,
			observed_provider_factor_source, observed_provider_factor_sampled_at
		) VALUES ($1, 1, 1, 'default', 'ready', 'advertised', CURRENT_TIMESTAMP)
	`, channelID); err == nil {
		t.Fatal("partial provider observation unexpectedly satisfied database CHECK")
	}
}

func TestChannelCostUnitRebaseAuditIsImmutableAndSurvivesChannelDeletion(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	applyCostUnitRebaseMigrations(t, ctx, db)

	var channelID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('immutable rebase audit', 'sub2api', 'https://example.com', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatal(err)
	}

	var auditID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_cost_unit_rebases (
			channel_id, channel_identity_revision, channel_name_snapshot, channel_type_snapshot,
			account_identity_fingerprint, account_identity_fingerprint_version,
			account_identity_fingerprint_source, account_identity_fingerprint_observed_at,
			idempotency_key, request_fingerprint, actor,
			expected_state_version, applied_state_version,
			expected_cost_unit_epoch, applied_cost_unit_epoch,
			previous_multiplier, current_multiplier,
			previous_multiplier_mode, current_multiplier_mode,
			previous_provider_factor, current_provider_factor,
			rate_transition_at, rate_raw_scale, rate_evidence_group_count,
			rate_evidence_digest, counter_transition,
			funding_factor_before, funding_factor_after,
			raw_balance_before, actual_inventory_before, raw_balance_after, actual_inventory_after,
			raw_today_cost_before, actual_today_cost_before, raw_today_cost_after, actual_today_cost_after,
			raw_total_cost_before, actual_total_cost_before, raw_total_cost_after, actual_total_cost_after,
			balance_sampled_at_before, balance_sampled_at_after,
			cost_sampled_at_before, cost_sampled_at_after,
			factor_source, state_source_before, state_source_after, applied_at
		) VALUES (
			$1, 1, 'immutable rebase audit', 'sub2api',
			$2, 1, 'account_binding', CURRENT_TIMESTAMP - interval '3 minutes',
			'unit-rebase-1', $3, 'migration-test',
			1, 2, 1, 2,
			10, 1, 'divide', 'divide', 0.1, 1,
			CURRENT_TIMESTAMP - interval '1 minute', 0.1, 1, $4, 'prior_unchanged',
			0.1, 1,
			10, 1, 1, 1,
			1, 0.1, 0.1, 0.1,
			100, 10, 99.1, 99.1,
			CURRENT_TIMESTAMP - interval '4 minutes', CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP - interval '2 minutes', CURRENT_TIMESTAMP,
			'advertised', 'advertised', 'manual_denomination_reset', CURRENT_TIMESTAMP
		) RETURNING id
		`, channelID, strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)).Scan(&auditID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.channel_cost_unit_rebase_rate_items (
			rebase_id, ordinal, model_name, description_before, description_after,
			ratio_before, ratio_after, completion_ratio_before, completion_ratio_after
		) VALUES ($1, 0, 'PLUS', 'before', 'after', 0.6, 0.06, 1.2, 0.12)
	`, auditID); err != nil {
		t.Fatalf("insert rebase rate item: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.channel_cost_unit_rebase_rate_items (
			rebase_id, ordinal, model_name,
			ratio_before, ratio_after, completion_ratio_before, completion_ratio_after
		) VALUES ($1, 0, 'orphan', 1, 1, 1, 1)
	`, auditID+1000); !hasPostgresCode(err, "23503") {
		t.Fatalf("orphan rebase rate item error = %v, want SQLSTATE 23503", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.channel_cost_unit_rebase_rate_items SET ratio_after = 1 WHERE rebase_id = $1`, auditID); !hasPostgresCode(err, "23514") {
		t.Fatalf("immutable rebase rate item UPDATE error = %v, want SQLSTATE 23514", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channel_cost_unit_rebase_rate_items WHERE rebase_id = $1`, auditID); !hasPostgresCode(err, "23514") {
		t.Fatalf("immutable rebase rate item DELETE error = %v, want SQLSTATE 23514", err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE TABLE public.channel_cost_unit_rebase_rate_items`); !hasPostgresCode(err, "23514") {
		t.Fatalf("immutable rebase rate item TRUNCATE error = %v, want SQLSTATE 23514", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE public.channel_cost_unit_rebases SET note = 'changed' WHERE id = $1`, auditID); !hasPostgresCode(err, "23514") {
		t.Fatalf("immutable audit UPDATE error = %v, want SQLSTATE 23514", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channel_cost_unit_rebases WHERE id = $1`, auditID); !hasPostgresCode(err, "23514") {
		t.Fatalf("immutable audit DELETE error = %v, want SQLSTATE 23514", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channels WHERE id = $1`, channelID); err != nil {
		t.Fatalf("delete channel with retained rebase audit: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.channel_cost_unit_rebases WHERE id = $1`, auditID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("retained rebase audit count = %d, want 1", count)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.channel_cost_unit_rebase_rate_items WHERE rebase_id = $1`, auditID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("retained rebase rate item count = %d, want 1", count)
	}
}

func applyCostUnitRebaseMigrations(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if err := applyMigrations(ctx, db, embeddedMigrationsThroughVersion(t, 59), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through version 59: %v", err)
	}
}

func hasPostgresCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

func embeddedMigrationsThroughVersion(t *testing.T, maxVersion int64) fstest.MapFS {
	t.Helper()
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{}
	for _, migration := range migrations {
		if migration.version > maxVersion {
			continue
		}
		data, err := embeddedMigrations.ReadFile(migration.name)
		if err != nil {
			t.Fatal(err)
		}
		files[migration.name] = &fstest.MapFile{Data: data}
	}
	return files
}

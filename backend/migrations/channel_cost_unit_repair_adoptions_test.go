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

func TestChannelCostUnitRepairAdoptionMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000061_channel_cost_unit_repair_adoptions.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(body)
	for _, required := range []string{
		"migration 000061 requires migrations 000036, 000059 and 000060",
		"CREATE TABLE public.channel_cost_unit_repair_adoptions",
		"reset_event_snapshot jsonb NOT NULL",
		"correction_event_snapshot jsonb NOT NULL",
		"source_events_digest character varying(64) NOT NULL",
		"cost_state_snapshot jsonb NOT NULL",
		"cost_state_digest character varying(64) NOT NULL",
		"channel_projection_snapshot jsonb NOT NULL",
		"channel_projection_digest character varying(64) NOT NULL",
		"reset_event_snapshot->>'id' IS NOT DISTINCT FROM reset_event_id::text",
		"jsonb_typeof(reset_event_snapshot->'funding_factor_before') IS NOT DISTINCT FROM 'number'",
		"cost_state_snapshot ?& ARRAY[",
		"cost_state_snapshot->>'channel_id' IS NOT DISTINCT FROM channel_id::text",
		"state_source_at_adoption = 'manual_denomination_reset'",
		"CASE jsonb_typeof(cost_state_snapshot->'raw_balance')",
		"CASE jsonb_typeof(cost_state_snapshot->'balance_sampled_at')",
		"channel_projection_snapshot ?& ARRAY[",
		"channel_projection_snapshot->>'upstream_account_identity_revision'",
		"factor_source = 'advertised'",
		"UNIQUE (id, channel_id)",
		"FOREIGN KEY (reset_event_id, channel_id)",
		"FOREIGN KEY (correction_event_id, channel_id)",
		"REFERENCES public.channel_funding_events(id, channel_id) ON DELETE RESTRICT",
		"UNIQUE (channel_id, channel_identity_revision, idempotency_key)",
		"UNIQUE (channel_id, reset_event_id, correction_event_id)",
		"CREATE FUNCTION public.reject_channel_cost_unit_repair_adoption_mutation()",
		"BEFORE UPDATE OR DELETE ON public.channel_cost_unit_repair_adoptions",
		"BEFORE TRUNCATE ON public.channel_cost_unit_repair_adoptions",
		"ALTER COLUMN rebase_id DROP NOT NULL",
		"ADD COLUMN repair_adoption_id bigint",
		"FOREIGN KEY (repair_adoption_id)",
		"REFERENCES public.channel_cost_unit_repair_adoptions(id) ON DELETE RESTRICT",
		"UNIQUE (repair_adoption_id)",
		"CHECK (num_nonnulls(rebase_id, repair_adoption_id) = 1)",
		"DROP CONSTRAINT uq_channel_cost_unit_request_restatements_idempotency",
		"WHERE rebase_id IS NOT NULL",
		"WHERE repair_adoption_id IS NOT NULL",
		"CREATE FUNCTION public.validate_channel_cost_unit_repair_adoption_restatement()",
		"DEFERRABLE INITIALLY DEFERRED",
		"channel_cost_unit_repair_adoption_restatement_consistency",
		"USING ERRCODE = '23514'",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("channel cost-unit repair adoption migration missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"FOREIGN KEY (channel_id) REFERENCES public.channels(id)",
		"ON DELETE CASCADE",
		"UPDATE public.channel_cost_states",
		"UPDATE public.channels",
		"UPDATE public.gateway_request_logs",
		"DELETE FROM public.",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("migration 61 must retain audit evidence without applying or cascading a repair: found %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 61 {
			continue
		}
		if migration.name != "000061_channel_cost_unit_repair_adoptions.sql" || migration.nonTransactional {
			t.Fatalf("version 61 channel cost-unit repair adoption migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 61 channel cost-unit repair adoption migration was not loaded")
}

func TestChannelCostUnitRepairAdoptionMigrationRequiresRestatementSchema(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThroughVersion(t, 59), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through version 59: %v", err)
	}
	body, err := embeddedMigrations.ReadFile("000061_channel_cost_unit_repair_adoptions.sql")
	if err != nil {
		t.Fatal(err)
	}
	err = applyMigrations(ctx, db, fstest.MapFS{
		"000061_channel_cost_unit_repair_adoptions.sql": &fstest.MapFile{Data: body},
	}, ".", defaultRunnerConfig())
	if err == nil || !strings.Contains(err.Error(), "migration 000061 requires migrations 000036, 000059 and 000060") {
		t.Fatalf("migration 61 without migration 60 error = %v", err)
	}

	var tableName sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.channel_cost_unit_repair_adoptions')::text`).Scan(&tableName); err != nil {
		t.Fatal(err)
	}
	if tableName.Valid {
		t.Fatalf("failed dependency guard left repair adoption table %q behind", tableName.String)
	}
}

func TestChannelCostUnitRepairAdoptionEvidenceAndParentConstraints(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThroughVersion(t, 61), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through version 61: %v", err)
	}

	const channelID int64 = 7101
	const otherChannelID int64 = 7102
	if _, err := db.ExecContext(ctx, `
			INSERT INTO public.channels (id, name, type, site_url, username, password_cipher)
			VALUES
				($1, 'retained repair adoption', 'sub2api', 'https://example.com', '', ''),
				($2, 'other repair adoption', 'sub2api', 'https://other.example.com', '', '')
		`, channelID, otherChannelID); err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	otherResetEventID, _ := insertRepairAdoptionSourceEvents(t, ctx, db, otherChannelID)
	resetEventID, correctionEventID := insertRepairAdoptionSourceEvents(t, ctx, db, channelID)
	adoptionID := insertRepairAdoption(t, ctx, db, channelID, resetEventID, correctionEventID)
	for index, malformed := range []struct {
		mode       string
		constraint string
	}{
		{mode: "null_reset_id", constraint: "channel_cost_unit_repair_adoptions_source_event_snapshots"},
		{mode: "empty_state", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "provider_factor_mismatch", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "provider_sample_before_correction", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "projection_raw_total_mismatch", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "empty_projection", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "wrong_state_source", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "wrong_factor_source", constraint: "channel_cost_unit_repair_adoptions_factors"},
		{mode: "negative_state_money", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "string_state_money", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "balance_sample_number", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
		{mode: "cost_sample_infinity", constraint: "channel_cost_unit_repair_adoptions_state_anchor"},
	} {
		err := cloneRepairAdoptionWithMalformedSnapshot(ctx, db, adoptionID, int64(index+1), malformed.mode)
		requireMigrationPostgresConstraint(t, err, "23514", malformed.constraint)
	}
	rebaseID := insertRepairAdoptionTestRebase(t, ctx, db, channelID)

	canonicalRestatementID, err := insertRepairAdoptionTestRestatement(ctx, db, rebaseID, nil, channelID, "adopt-repair-1")
	if err != nil {
		t.Fatalf("insert canonical-parent restatement: %v", err)
	}
	var adoptionRestatementID int64
	if err := db.QueryRowContext(ctx, `
		SELECT id FROM public.channel_cost_unit_request_restatements WHERE repair_adoption_id = $1
	`, adoptionID).Scan(&adoptionRestatementID); err != nil {
		t.Fatalf("load adoption-parent restatement: %v", err)
	}

	var loadedRebaseID, loadedAdoptionID sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT rebase_id, repair_adoption_id
		FROM public.channel_cost_unit_request_restatements
		WHERE id = $1
	`, canonicalRestatementID).Scan(&loadedRebaseID, &loadedAdoptionID); err != nil {
		t.Fatal(err)
	}
	if !loadedRebaseID.Valid || loadedRebaseID.Int64 != rebaseID || loadedAdoptionID.Valid {
		t.Fatalf("canonical parent = rebase %v adoption %v", loadedRebaseID, loadedAdoptionID)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT rebase_id, repair_adoption_id
		FROM public.channel_cost_unit_request_restatements
		WHERE id = $1
	`, adoptionRestatementID).Scan(&loadedRebaseID, &loadedAdoptionID); err != nil {
		t.Fatal(err)
	}
	if loadedRebaseID.Valid || !loadedAdoptionID.Valid || loadedAdoptionID.Int64 != adoptionID {
		t.Fatalf("adoption parent = rebase %v adoption %v", loadedRebaseID, loadedAdoptionID)
	}

	_, err = insertRepairAdoptionTestRestatement(ctx, db, nil, nil, channelID, "missing-parent")
	requireMigrationPostgresConstraint(t, err, "23514", "channel_cost_unit_request_restatements_parent_xor")
	_, err = insertRepairAdoptionTestRestatement(ctx, db, rebaseID, adoptionID, channelID, "dual-parent")
	requireMigrationPostgresConstraint(t, err, "23514", "channel_cost_unit_request_restatements_parent_xor")
	_, err = insertRepairAdoptionTestRestatement(ctx, db, rebaseID+100000, nil, channelID, "missing-rebase")
	requireMigrationPostgresConstraint(t, err, "23503", "fk_channel_cost_unit_request_restatements_rebase")
	_, err = insertRepairAdoptionTestRestatement(ctx, db, nil, adoptionID+100000, channelID, "missing-adoption")
	requireMigrationPostgresConstraint(t, err, "23503", "fk_channel_cost_unit_request_restatements_repair_adoption")
	_, err = insertRepairAdoptionTestRestatement(ctx, db, rebaseID, nil, channelID, "duplicate-rebase")
	requireMigrationPostgresConstraint(t, err, "23505", "uq_channel_cost_unit_request_restatements_rebase")
	_, err = insertRepairAdoptionTestRestatement(ctx, db, nil, adoptionID, channelID, "duplicate-adoption")
	requireMigrationPostgresConstraint(t, err, "23505", "uq_channel_cost_unit_request_restatements_repair_adoption")

	if err := cloneRepairAdoptionWithCrossChannelEvent(ctx, db, adoptionID, otherResetEventID); err == nil {
		t.Fatal("repair adoption with a cross-channel source event unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23503", "fk_channel_cost_unit_repair_adoptions_reset_event")
	}
	orphanResetID, orphanCorrectionID := insertRepairAdoptionSourceEvents(t, ctx, db, channelID)
	if _, err := insertClonedRepairAdoptionRow(ctx, db, adoptionID, orphanResetID, orphanCorrectionID, "orphan-adoption", 2000); err == nil {
		t.Fatal("orphan repair adoption unexpectedly committed")
	} else {
		requireMigrationPostgresConstraint(t, err, "23514", "channel_cost_unit_repair_adoption_restatement_consistency")
	}
	mismatchResetID, mismatchCorrectionID := insertRepairAdoptionSourceEvents(t, ctx, db, channelID)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	mismatchAdoptionID, err := insertClonedRepairAdoptionRow(
		ctx, tx, adoptionID, mismatchResetID, mismatchCorrectionID, "mismatch-parent", 3000,
	)
	if err == nil {
		_, err = insertRepairAdoptionTestRestatement(ctx, tx, nil, mismatchAdoptionID, channelID, "mismatch-child")
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	requireMigrationPostgresConstraint(t, err, "23514", "channel_cost_unit_repair_adoption_restatement_consistency")

	if _, err := db.ExecContext(ctx, `UPDATE public.channel_cost_unit_repair_adoptions SET actor = 'changed' WHERE id = $1`, adoptionID); err == nil {
		t.Fatal("repair adoption UPDATE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23514", "")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channel_cost_unit_repair_adoptions WHERE id = $1`, adoptionID); err == nil {
		t.Fatal("repair adoption DELETE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23514", "")
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.channel_cost_unit_request_restatements SET actor = 'changed' WHERE id = $1`, adoptionRestatementID); err == nil {
		t.Fatal("repair restatement UPDATE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23514", "")
	}

	itemID := insertRepairAdoptionTestRestatementItem(t, ctx, db, adoptionRestatementID)
	if _, err := db.ExecContext(ctx, `UPDATE public.channel_cost_unit_request_restatement_items SET cost_status = 'changed' WHERE id = $1`, itemID); err == nil {
		t.Fatal("repair restatement item UPDATE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23514", "")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channel_cost_unit_request_restatement_items WHERE id = $1`, itemID); err == nil {
		t.Fatal("repair restatement item DELETE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23514", "")
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM public.channel_funding_events WHERE id = $1`, resetEventID); err == nil {
		t.Fatal("referenced reset event DELETE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23503", "fk_channel_cost_unit_repair_adoptions_reset_event")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channel_funding_events WHERE id = $1`, correctionEventID); err == nil {
		t.Fatal("referenced correction event DELETE unexpectedly succeeded")
	} else {
		requireMigrationPostgresConstraint(t, err, "23503", "fk_channel_cost_unit_repair_adoptions_correction_event")
	}
	for _, table := range []string{
		"channel_cost_unit_request_restatement_items",
		"channel_cost_unit_request_restatements",
		"channel_cost_unit_repair_adoptions",
	} {
		if _, err := db.ExecContext(ctx, `TRUNCATE TABLE public.`+table+` CASCADE`); err == nil {
			t.Fatalf("%s TRUNCATE unexpectedly succeeded", table)
		} else {
			requireMigrationPostgresConstraint(t, err, "23514", "")
		}
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM public.channels WHERE id = $1`, channelID); err != nil {
		t.Fatalf("delete channel with retained repair evidence: %v", err)
	}
	var adoptionCount, sourceEventCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.channel_cost_unit_repair_adoptions WHERE id = $1`, adoptionID).Scan(&adoptionCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.channel_funding_events WHERE id IN ($1, $2)`, resetEventID, correctionEventID).Scan(&sourceEventCount); err != nil {
		t.Fatal(err)
	}
	if adoptionCount != 1 || sourceEventCount != 2 {
		t.Fatalf("retained evidence counts = adoption %d source events %d, want 1/2", adoptionCount, sourceEventCount)
	}
}

func insertRepairAdoptionSourceEvents(t *testing.T, ctx context.Context, db *sql.DB, channelID int64) (int64, int64) {
	t.Helper()
	insert := func(kind, fundingBefore, fundingAfter, effectiveAt string) int64 {
		var eventID int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.channel_funding_events (
				channel_id, kind, paid_amount, credited_amount,
				raw_balance_before, raw_balance_after,
				actual_inventory_before, actual_inventory_after,
				funding_factor_before, funding_factor_after,
				raw_today_cost_before, raw_total_cost_before,
				note, effective_at, created_at
			) VALUES (
				$1, $2, 0, 0, 100, 100, 100, 10,
				CAST($3 AS numeric), CAST($4 AS numeric), 0, 0,
				'migration 61 test evidence', CAST($5 AS timestamptz), CAST($5 AS timestamptz)
			) RETURNING id
			`, channelID, kind, fundingBefore, fundingAfter, effectiveAt).Scan(&eventID); err != nil {
			t.Fatalf("insert %s event: %v", kind, err)
		}
		return eventID
	}
	resetEventID := insert("denomination_reset", "1", "0.1", "2026-07-01T00:05:00Z")
	correctionEventID := insert("denomination_cost_fix", "0.1", "0.1", "2026-07-01T00:10:00Z")
	return resetEventID, correctionEventID
}

func insertRepairAdoption(t *testing.T, ctx context.Context, db *sql.DB, channelID, resetEventID, correctionEventID int64) int64 {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var adoptionID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO public.channel_cost_unit_repair_adoptions (
			channel_id, channel_identity_revision, channel_name_snapshot, channel_type_snapshot,
			account_identity_fingerprint, account_identity_fingerprint_version,
			account_identity_fingerprint_source, account_identity_fingerprint_observed_at,
			idempotency_key, request_fingerprint, preview_digest, actor,
			source_event_high_water, reset_event_id, correction_event_id,
			reset_event_snapshot, correction_event_snapshot, source_events_digest,
			state_version, cost_unit_epoch, state_source_at_adoption, state_status_at_adoption,
			cost_state_snapshot, cost_state_digest,
			channel_projection_snapshot, channel_projection_digest,
			cost_sampled_at_before, cost_sampled_at_after,
			affected_started_at, affected_ended_at, request_log_high_water,
			previous_provider_factor, current_provider_factor, factor_source,
			funding_factor_before, funding_factor_after
		)
		SELECT
			$1::bigint, 3, 'retained repair adoption', 'sub2api',
			repeat('a', 64), 2, 'account_binding', '2026-07-01T00:00:00Z'::timestamptz,
			'adopt-repair-1', repeat('4', 64), repeat('c', 64), 'migration-test',
			$3::bigint, reset_event.id, correction_event.id,
			to_jsonb(reset_event), to_jsonb(correction_event), repeat('d', 64),
			7, 4, 'manual_denomination_reset', 'ready',
				jsonb_build_object(
					'channel_id', $1::bigint, 'version', 7, 'cost_unit_epoch', 4,
					'source', 'manual_denomination_reset', 'status', 'ready',
					'raw_balance', 100, 'actual_inventory', 10, 'balance_sampled_at', '2026-07-01T00:09:00Z',
					'funding_factor', 0.1, 'raw_today_cost', 2, 'actual_today_cost', 0.2,
					'raw_total_cost', 20, 'actual_total_cost', 2, 'cost_sampled_at', '2026-07-01T00:10:00Z',
					'observed_provider_factor', 0.1, 'observed_provider_factor_source', 'advertised',
					'observed_provider_factor_sampled_at', '2026-07-01T00:11:00Z'
				), repeat('e', 64),
			jsonb_build_object(
				'id', $1::bigint, 'upstream_account_identity_revision', 3,
				'cost_unit_epoch', 4, 'raw_last_balance', 100, 'last_balance', 10,
				'last_balance_at', '2026-07-01T00:09:00Z', 'funding_factor', 0.1,
				'funding_factor_source', 'manual_denomination_reset',
				'raw_today_cost', 2, 'today_cost', 0.2, 'raw_total_cost', 20, 'total_cost', 2,
				'today_cost_at', '2026-07-01T00:10:00Z'
			), repeat('f', 64),
			'2026-07-01T00:00:00Z'::timestamptz, '2026-07-01T00:10:00Z'::timestamptz,
			'2026-07-01T00:01:00Z'::timestamptz, '2026-07-01T00:05:00Z'::timestamptz, 7,
				1, 0.1, 'advertised', 1, 0.1
		FROM public.channel_funding_events AS reset_event
		CROSS JOIN public.channel_funding_events AS correction_event
		WHERE reset_event.id = $2::bigint AND correction_event.id = $3::bigint
		RETURNING id
		`, channelID, resetEventID, correctionEventID).Scan(&adoptionID); err != nil {
		t.Fatalf("insert repair adoption: %v", err)
	}
	if _, err := insertRepairAdoptionTestRestatement(ctx, tx, nil, adoptionID, channelID, "adopt-repair-1"); err != nil {
		t.Fatalf("insert repair adoption restatement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit repair adoption and restatement: %v", err)
	}
	return adoptionID
}

func cloneRepairAdoptionWithCrossChannelEvent(ctx context.Context, db *sql.DB, adoptionID, otherResetEventID int64) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO public.channel_cost_unit_repair_adoptions
		SELECT (jsonb_populate_record(
			NULL::public.channel_cost_unit_repair_adoptions,
			to_jsonb(adoption) || jsonb_build_object(
				'id', adoption.id + 1000,
				'idempotency_key', 'cross-channel-event',
				'reset_event_id', $2::bigint,
				'reset_event_snapshot', jsonb_set(
					adoption.reset_event_snapshot, '{id}', to_jsonb($2::bigint)
				)
			)
		)).*
		FROM public.channel_cost_unit_repair_adoptions AS adoption
		WHERE adoption.id = $1
	`, adoptionID, otherResetEventID)
	return err
}

func insertClonedRepairAdoptionRow(
	ctx context.Context,
	db migrationQueryRower,
	adoptionID, resetEventID, correctionEventID int64,
	idempotencyKey string,
	idOffset int64,
) (int64, error) {
	var clonedID int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_cost_unit_repair_adoptions
		SELECT (jsonb_populate_record(
			NULL::public.channel_cost_unit_repair_adoptions,
			to_jsonb(adoption) || jsonb_build_object(
				'id', adoption.id + $5::bigint,
				'idempotency_key', $4::text,
				'source_event_high_water', correction_event.id,
				'reset_event_id', reset_event.id,
				'correction_event_id', correction_event.id,
				'reset_event_snapshot', to_jsonb(reset_event),
				'correction_event_snapshot', to_jsonb(correction_event)
			)
		)).*
		FROM public.channel_cost_unit_repair_adoptions AS adoption
		JOIN public.channel_funding_events AS reset_event ON reset_event.id = $2
		JOIN public.channel_funding_events AS correction_event ON correction_event.id = $3
		WHERE adoption.id = $1
		RETURNING id
	`, adoptionID, resetEventID, correctionEventID, idempotencyKey, idOffset).Scan(&clonedID)
	return clonedID, err
}

func cloneRepairAdoptionWithMalformedSnapshot(ctx context.Context, db *sql.DB, adoptionID, offset int64, mode string) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO public.channel_cost_unit_repair_adoptions
		SELECT (jsonb_populate_record(
			NULL::public.channel_cost_unit_repair_adoptions,
			to_jsonb(adoption) || jsonb_build_object(
				'id', adoption.id + $2::bigint,
				'idempotency_key', ('malformed-' || $3::text),
				'state_source_at_adoption', CASE
					WHEN $3 = 'wrong_state_source' THEN 'advertised'
					ELSE adoption.state_source_at_adoption
				END,
				'factor_source', CASE
					WHEN $3 = 'wrong_factor_source' THEN 'operator_override'
					ELSE adoption.factor_source
				END,
				'reset_event_snapshot', CASE
					WHEN $3 = 'null_reset_id' THEN jsonb_set(adoption.reset_event_snapshot, '{id}', 'null'::jsonb)
					ELSE adoption.reset_event_snapshot
				END,
				'cost_state_snapshot', CASE
					WHEN $3 = 'empty_state' THEN '{}'::jsonb
					WHEN $3 = 'provider_factor_mismatch' THEN jsonb_set(adoption.cost_state_snapshot, '{observed_provider_factor}', '0.2'::jsonb)
					WHEN $3 = 'provider_sample_before_correction' THEN jsonb_set(adoption.cost_state_snapshot, '{observed_provider_factor_sampled_at}', '"2026-07-01T00:09:59Z"'::jsonb)
					WHEN $3 = 'wrong_state_source' THEN jsonb_set(adoption.cost_state_snapshot, '{source}', '"advertised"'::jsonb)
					WHEN $3 = 'wrong_factor_source' THEN jsonb_set(adoption.cost_state_snapshot, '{observed_provider_factor_source}', '"operator_override"'::jsonb)
					WHEN $3 = 'negative_state_money' THEN jsonb_set(adoption.cost_state_snapshot, '{raw_balance}', '-1'::jsonb)
					WHEN $3 = 'string_state_money' THEN jsonb_set(adoption.cost_state_snapshot, '{raw_total_cost}', '"20"'::jsonb)
					WHEN $3 = 'balance_sample_number' THEN jsonb_set(adoption.cost_state_snapshot, '{balance_sampled_at}', '123'::jsonb)
					WHEN $3 = 'cost_sample_infinity' THEN jsonb_set(adoption.cost_state_snapshot, '{cost_sampled_at}', '"infinity"'::jsonb)
					ELSE adoption.cost_state_snapshot
				END,
				'channel_projection_snapshot', CASE
					WHEN $3 = 'empty_projection' THEN '{}'::jsonb
					WHEN $3 = 'projection_raw_total_mismatch' THEN jsonb_set(adoption.channel_projection_snapshot, '{raw_total_cost}', '21'::jsonb)
					WHEN $3 = 'wrong_state_source' THEN jsonb_set(adoption.channel_projection_snapshot, '{funding_factor_source}', '"advertised"'::jsonb)
					WHEN $3 = 'negative_state_money' THEN jsonb_set(adoption.channel_projection_snapshot, '{raw_last_balance}', '-1'::jsonb)
					WHEN $3 = 'string_state_money' THEN jsonb_set(adoption.channel_projection_snapshot, '{raw_total_cost}', '"20"'::jsonb)
					WHEN $3 = 'balance_sample_number' THEN jsonb_set(adoption.channel_projection_snapshot, '{last_balance_at}', '123'::jsonb)
					WHEN $3 = 'cost_sample_infinity' THEN jsonb_set(adoption.channel_projection_snapshot, '{today_cost_at}', '"infinity"'::jsonb)
					ELSE adoption.channel_projection_snapshot
				END
			)
		)).*
		FROM public.channel_cost_unit_repair_adoptions AS adoption
		WHERE adoption.id = $1
	`, adoptionID, offset, mode)
	return err
}

func insertRepairAdoptionTestRebase(t *testing.T, ctx context.Context, db *sql.DB, channelID int64) int64 {
	t.Helper()
	var rebaseID int64
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
			$1, 3, 'retained repair adoption', 'sub2api',
			repeat('1', 64), 2, 'account_binding', '2026-07-01T00:00:00Z'::timestamptz,
			'canonical-rebase-1', repeat('2', 64), 'migration-test',
			6, 7, 3, 4, 10, 1, 'divide', 'divide', 0.1, 1,
			'2026-07-01T00:03:00Z'::timestamptz, 0.1, 1, repeat('3', 64), 'prior_unchanged',
			0.1, 1, 100, 10, 10, 10, 2, 0.2, 0.2, 0.2, 20, 2, 18.2, 18.2,
			'2026-07-01T00:00:00Z'::timestamptz, '2026-07-01T00:04:00Z'::timestamptz,
			'2026-07-01T00:01:00Z'::timestamptz, '2026-07-01T00:04:00Z'::timestamptz,
			'advertised', 'advertised', 'manual_denomination_reset', '2026-07-01T00:05:00Z'::timestamptz
		) RETURNING id
	`, channelID).Scan(&rebaseID); err != nil {
		t.Fatalf("insert canonical rebase: %v", err)
	}
	return rebaseID
}

type migrationQueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func insertRepairAdoptionTestRestatement(ctx context.Context, db migrationQueryRower, rebaseID, adoptionID any, channelID int64, idempotencyKey string) (int64, error) {
	var restatementID int64
	basisSource := "migration_test"
	if adoptionID != nil {
		basisSource = "cost_unit_repair_adoption"
	}
	err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_cost_unit_request_restatements (
			rebase_id, repair_adoption_id, channel_id, channel_identity_revision,
			idempotency_key, request_fingerprint, actor, request_log_high_water,
			cost_sampled_at_before, cost_sampled_at_after, affected_started_at, affected_ended_at,
			old_funding_factor, new_funding_factor,
			expected_population_rows, expected_new_ratio_rows, expected_old_ratio_rows,
			expected_ratio_mismatch_rows, expected_latency_probe_rows,
			expected_restatement_rows, expected_already_correct_costed_rows,
			expected_raw_cost_sum, expected_recorded_cost_sum,
			expected_corrected_cost_sum, expected_undercount_sum,
			expected_already_correct_raw_cost_sum, expected_all_costed_raw_cost_sum,
			expected_bucket_count, expected_bucket_starts, expected_bucket_digest,
			applied_restatement_rows, applied_raw_cost_sum, applied_recorded_cost_sum,
			applied_corrected_cost_sum, applied_undercount_sum,
			applied_cost_basis_version, applied_cost_basis_source
		) VALUES (
			$1, $2, $3, 3, $4, repeat('4', 64), 'migration-test', 7,
			'2026-07-01T00:00:00Z'::timestamptz, '2026-07-01T00:10:00Z'::timestamptz,
			'2026-07-01T00:01:00Z'::timestamptz, '2026-07-01T00:05:00Z'::timestamptz,
			1, 0.1, 1, 1, 0, 0, 0, 1, 0,
			10, 0.1, 1, 0.9, 0, 10,
			1, '["2026-07-01T00:00:00Z"]'::jsonb, repeat('5', 64),
				1, 10, 0.1, 1, 0.9, 7, $5
			) RETURNING id
		`, rebaseID, adoptionID, channelID, idempotencyKey, basisSource).Scan(&restatementID)
	return restatementID, err
}

func insertRepairAdoptionTestRestatementItem(t *testing.T, ctx context.Context, db *sql.DB, restatementID int64) int64 {
	t.Helper()
	var itemID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channel_cost_unit_request_restatement_items (
			restatement_id, request_log_id, request_created_at,
			raw_cost, raw_group_ratio, raw_completion_ratio,
			cost_before, cost_after, group_ratio_before, group_ratio_after,
			completion_ratio_before, completion_ratio_after,
			funding_factor_before, funding_factor_after,
			cost_basis_version_before, cost_basis_version_after,
			cost_basis_source_before, cost_basis_source_after,
			cost_source, cost_status, route_reason
		) VALUES (
			$1, 1, '2026-07-01T00:02:00Z'::timestamptz,
			10, 1, NULL, 0.1, 1, 0.1, 1, 0, 0, 1, 0.1,
			6, 7, 'legacy', 'cost_unit_repair_adoption', 'usage', 'final', 'normal'
		) RETURNING id
	`, restatementID).Scan(&itemID); err != nil {
		t.Fatalf("insert restatement item: %v", err)
	}
	return itemID
}

func requireMigrationPostgresConstraint(t *testing.T, err error, code, constraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code || (constraint != "" && pgErr.ConstraintName != constraint) {
		t.Fatalf("PostgreSQL error = %v, want SQLSTATE %s constraint %q", err, code, constraint)
	}
}

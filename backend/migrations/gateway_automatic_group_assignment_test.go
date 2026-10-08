package migrations

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestGatewayAutomaticGroupAssignmentMigration(t *testing.T) {
	raw, err := os.ReadFile("000013_gateway_automatic_group_assignment.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"create table public.gateway_upstream_groups",
		"automatic group assignment migration cannot import rate_snapshots group name longer than 128 characters",
		"unique (channel_id, canonical_group_name)",
		"add column membership_version bigint not null default 1",
		"create table public.gateway_candidate_group_auto_rules",
		"gateway_candidate_group_auto_rules_identity_required",
		"service_tier varchar(64) not null default ''",
		"probed_gateway_apps jsonb not null default '[]'::jsonb",
		"service_tiers jsonb not null default '[]'::jsonb",
		"create unique index gateway_candidate_group_members_identity_idx",
		"gateway_upstream_group_changes_assignment_state check",
		"create table public.gateway_candidate_group_ruleset_state",
		"auto_execute_enabled boolean not null default false",
		"create table public.gateway_candidate_group_membership_claims",
		"create table public.gateway_upstream_group_assignment_overrides",
		"create table public.gateway_upstream_group_assignment_override_targets",
		"create table public.gateway_upstream_group_assignment_audits",
		"upstream_group_id bigint not null references public.gateway_upstream_groups(id) on delete cascade",
		"create table public.gateway_key_group_route_policies",
		"candidate_target_mode varchar(24)",
		"candidate_target_source varchar(24)",
		"add column upstream_group_id bigint references public.gateway_upstream_groups",
		"add column presence_generation bigint",
		"add column assignment_state varchar(24)",
		"add column evaluation_token varchar(64)",
		"migration_manual_claim",
		"preserve candidate groups configured before automatic assignment",
		"where groups.present = true",
		"and groups.present = false",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("automatic group assignment migration missing %q", required)
		}
	}
	if strings.Contains(sql, "alter table public.gateway_upstream_groups disable trigger") {
		t.Fatal("migration must not bypass upstream group integrity triggers")
	}
}

func TestGatewayAutomaticGroupAssignmentMigratesLegacyMemberships(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 12), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply legacy migrations: %v", err)
	}

	var channelID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('legacy automatic assignment', 'newapi', 'https://legacy.example', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatalf("insert legacy channel: %v", err)
	}
	var candidateGroupID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_candidate_groups (name)
		VALUES ('legacy candidate')
		RETURNING id
	`).Scan(&candidateGroupID); err != nil {
		t.Fatalf("insert legacy candidate group: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_candidate_group_members (candidate_group_id, channel_id, group_name)
		VALUES ($1, $2, 'Live'), ($1, $2, 'live'), ($1, $2, 'Absent')
	`, candidateGroupID, channelID); err != nil {
		t.Fatalf("insert legacy candidate members: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.rate_snapshots (channel_id, model_name, ratio)
		VALUES ($1, 'live', 1)
	`, channelID); err != nil {
		t.Fatalf("insert current legacy rate: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_upstream_group_changes (channel_id, group_name, kind, status, detected_at)
		VALUES
			($1, 'LIVE', 'added', 'pending', CURRENT_TIMESTAMP - INTERVAL '2 days'),
			($1, 'live', 'added', 'pending', CURRENT_TIMESTAMP - INTERVAL '1 day'),
			($1, 'Absent', 'added', 'pending', CURRENT_TIMESTAMP)
	`, channelID); err != nil {
		t.Fatalf("insert legacy pending changes: %v", err)
	}

	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 13), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply automatic assignment migration: %v", err)
	}

	var liveGroupID, absentGroupID int64
	var livePresent, absentPresent bool
	if err := db.QueryRowContext(ctx, `
		SELECT id, present FROM public.gateway_upstream_groups
		WHERE channel_id = $1 AND canonical_group_name = 'live'
	`, channelID).Scan(&liveGroupID, &livePresent); err != nil {
		t.Fatalf("read migrated live identity: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT id, present FROM public.gateway_upstream_groups
		WHERE channel_id = $1 AND canonical_group_name = 'absent'
	`, channelID).Scan(&absentGroupID, &absentPresent); err != nil {
		t.Fatalf("read migrated absent identity: %v", err)
	}
	if !livePresent || absentPresent {
		t.Fatalf("migrated presence: live=%t absent=%t", livePresent, absentPresent)
	}

	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_candidate_group_membership_claims
		WHERE upstream_group_id = $1 AND active = TRUE
	`, 1, liveGroupID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_candidate_group_members
		WHERE candidate_group_id = $1 AND upstream_group_id = $2
	`, 1, candidateGroupID, liveGroupID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*)
		FROM public.gateway_candidate_group_members members
		JOIN public.rate_snapshots rates
		  ON rates.channel_id = members.channel_id
		 AND rates.model_name = members.group_name
		WHERE members.candidate_group_id = $1
		  AND members.upstream_group_id = $2
		  AND members.group_name = 'live'
	`, 1, candidateGroupID, liveGroupID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_upstream_group_assignment_audits
		WHERE upstream_group_id = $1 AND action = 'migration_manual_claim'
	`, 1, liveGroupID)

	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_candidate_group_membership_claims
		WHERE upstream_group_id = $1 AND active = TRUE
	`, 0, absentGroupID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_candidate_group_members
		WHERE candidate_group_id = $1 AND upstream_group_id = $2
	`, 0, candidateGroupID, absentGroupID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*)
		FROM public.gateway_upstream_group_assignment_override_targets targets
		JOIN public.gateway_upstream_group_assignment_overrides overrides ON overrides.id = targets.override_id
		WHERE overrides.upstream_group_id = $1
		  AND targets.candidate_group_id = $2
		  AND targets.candidate_group_id_snapshot = $2
	`, 1, absentGroupID, candidateGroupID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_upstream_group_changes
		WHERE channel_id = $1 AND lower(btrim(group_name)) = 'absent'
		  AND status = 'superseded' AND assignment_state = 'superseded'
		  AND assignment_reason_code = 'migration_group_not_present'
	`, 1, channelID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_upstream_group_changes
		WHERE channel_id = $1 AND lower(btrim(group_name)) = 'live'
		  AND presence_generation = 1 AND status = 'pending'
	`, 1, channelID)
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_upstream_group_changes
		WHERE channel_id = $1 AND lower(btrim(group_name)) = 'live'
		  AND presence_generation = 1 AND status = 'superseded'
		  AND assignment_state = 'superseded'
		  AND assignment_reason_code = 'migration_duplicate_pending'
	`, 1, channelID)
}

func TestGatewayAutomaticGroupAssignmentClearsUnscopedLegacyModelFacts(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 12), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply legacy migrations: %v", err)
	}

	var channelID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('legacy model facts', 'newapi', 'https://legacy-model-facts.example', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatalf("insert legacy channel: %v", err)
	}
	validFirstSeen := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	validSyncedAt := validFirstSeen.Add(24 * time.Hour)
	nullSyncedAt := validSyncedAt.Add(time.Hour)
	zeroSyncedAt := validSyncedAt.Add(2 * time.Hour)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.rate_snapshots (channel_id, model_name, ratio, first_seen_at, last_seen_at)
		VALUES
			($1, 'PrO-MaX', 1, $2, $3),
			($1, 'NULL-FIRST-SEEN', 1, NULL, $3),
			($1, 'ZERO-FIRST-SEEN', 1, TIMESTAMPTZ '0001-01-01 00:00:00+00', $3)
	`, channelID, validFirstSeen, zeroSyncedAt.Add(time.Hour)); err != nil {
		t.Fatalf("insert current legacy rates: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_upstream_keys (
			channel_id, api_key_cipher, api_key_prefix, group_name, model_families,
			model_ids, models_synced_at, enabled, latency_probe_enabled
		) VALUES
			($1, '', '', 'pro-max', 'codex', 'gpt-5.4', $2, TRUE, TRUE),
			($1, '', '', 'null-first-seen', 'codex', 'gpt-5.4', $3, TRUE, TRUE),
			($1, '', '', 'zero-first-seen', 'codex', 'gpt-5.4', $4, TRUE, TRUE)
	`, channelID, validSyncedAt, nullSyncedAt, zeroSyncedAt); err != nil {
		t.Fatalf("insert synchronized legacy keys: %v", err)
	}

	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 13), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply automatic assignment migration: %v", err)
	}

	var discoveredAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT discovered_at FROM public.gateway_upstream_groups
		WHERE channel_id = $1 AND canonical_group_name = 'pro-max'
	`, channelID).Scan(&discoveredAt); err != nil {
		t.Fatalf("read migrated generation boundary: %v", err)
	}
	if !discoveredAt.Equal(validFirstSeen) {
		t.Fatalf("migrated discovered_at = %s, want rate first_seen_at %s", discoveredAt, validFirstSeen)
	}
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 66), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply current migrations before running current storage code: %v", err)
	}

	gormDB, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: db}), &gorm.Config{})
	if err != nil {
		t.Fatalf("open migrated database with gorm: %v", err)
	}
	processed, err := storage.NewGateway(gormDB).BackfillAutomaticAssignmentFacts(100)
	if err != nil {
		t.Fatalf("backfill migrated model facts: %v", err)
	}
	if processed != 0 {
		t.Fatalf("backfilled unscoped legacy keys = %d, want 0", processed)
	}
	for _, canonicalName := range []string{"pro-max", "null-first-seen", "zero-first-seen"} {
		var group storage.GatewayUpstreamGroup
		if err := gormDB.Where("channel_id = ? AND canonical_group_name = ?", channelID, canonicalName).First(&group).Error; err != nil {
			t.Fatalf("read migrated group %s: %v", canonicalName, err)
		}
		if group.FactsComplete || len(group.ModelIDs) != 0 || group.ModelsSyncedAt != nil || group.ModelFactsChannelIdentityRevision != 0 {
			t.Fatalf("unscoped legacy model facts were retained for %s: %+v", canonicalName, group)
		}
	}
}

func embeddedMigrationsThrough(t *testing.T, maxVersion int64) fstest.MapFS {
	t.Helper()
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	files := make(fstest.MapFS)
	for _, migration := range migrations {
		if migration.version > maxVersion {
			continue
		}
		data, err := embeddedMigrations.ReadFile(migration.name)
		if err != nil {
			t.Fatalf("read embedded migration %s: %v", migration.name, err)
		}
		files[migration.name] = &fstest.MapFile{Data: data}
	}
	return files
}

func assertMigrationCount(t *testing.T, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string, want int64, args ...any) {
	t.Helper()
	var got int64
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&got); err != nil {
		t.Fatalf("count migrated rows: %v", err)
	}
	if got != want {
		t.Fatalf("migrated row count = %d, want %d", got, want)
	}
}

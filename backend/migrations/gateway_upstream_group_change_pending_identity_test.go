package migrations

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestGatewayUpstreamGroupChangePendingIdentityMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000071_gateway_upstream_group_change_pending_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"row_number() over",
		"partition by upstream_group_id, presence_generation",
		"order by id desc",
		"assignment_reason_code = 'duplicate_pending_identity'",
		"create unique index gateway_upstream_group_changes_pending_added_identity_idx",
		"where status = 'pending'",
		"and kind = 'added'",
		"and upstream_group_id is not null",
		"and presence_generation is not null",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("pending identity migration missing %q", required)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version == 71 {
			if migration.name != "000071_gateway_upstream_group_change_pending_identity.sql" || migration.nonTransactional {
				t.Fatalf("version 71 pending identity migration = %#v", migration)
			}
			return
		}
	}
	t.Fatal("version 71 pending identity migration was not loaded")
}

func TestGatewayUpstreamGroupChangePendingIdentityMigrationRepairsAndGuardsRows(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 70), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 70: %v", err)
	}

	var channelID, upstreamGroupID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('pending identity migration', 'newapi', 'https://pending-identity.example', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatalf("insert migration channel: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_upstream_groups (
			channel_id, canonical_group_name, raw_group_names, channel_type
		) VALUES ($1, 'promo', '["promo"]'::jsonb, 'newapi')
		RETURNING id
	`, channelID).Scan(&upstreamGroupID); err != nil {
		t.Fatalf("insert migration upstream group: %v", err)
	}

	changeIDs := make([]int64, 0, 3)
	for index := 0; index < 3; index++ {
		var changeID int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.gateway_upstream_group_changes (
				channel_id, group_name, upstream_group_id, presence_generation,
				kind, status, assignment_state, detected_at, created_at, updated_at
			) VALUES ($1, 'promo', $2, 1, 'added', 'pending', 'awaiting_facts',
				CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			RETURNING id
		`, channelID, upstreamGroupID).Scan(&changeID); err != nil {
			t.Fatalf("insert duplicate pending change %d: %v", index, err)
		}
		changeIDs = append(changeIDs, changeID)
	}

	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 71), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply pending identity migration: %v", err)
	}
	var pendingID int64
	if err := db.QueryRowContext(ctx, `
		SELECT id FROM public.gateway_upstream_group_changes
		WHERE upstream_group_id = $1 AND presence_generation = 1 AND kind = 'added' AND status = 'pending'
	`, upstreamGroupID).Scan(&pendingID); err != nil {
		t.Fatalf("read surviving pending change: %v", err)
	}
	if pendingID != changeIDs[len(changeIDs)-1] {
		t.Fatalf("surviving pending change id = %d, want newest %d", pendingID, changeIDs[len(changeIDs)-1])
	}
	assertMigrationCount(t, db, `
		SELECT COUNT(*) FROM public.gateway_upstream_group_changes
		WHERE upstream_group_id = $1 AND presence_generation = 1
		  AND status = 'superseded' AND assignment_state = 'superseded'
		  AND assignment_reason_code = 'duplicate_pending_identity'
	`, 2, upstreamGroupID)

	_, insertErr := db.ExecContext(ctx, `
		INSERT INTO public.gateway_upstream_group_changes (
			channel_id, group_name, upstream_group_id, presence_generation,
			kind, status, assignment_state, detected_at, created_at, updated_at
		) VALUES ($1, 'promo', $2, 1, 'added', 'pending', 'awaiting_facts',
			CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, channelID, upstreamGroupID)
	assertPostgresCode(t, insertErr, "23505")
}

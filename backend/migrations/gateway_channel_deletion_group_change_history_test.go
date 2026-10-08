package migrations

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestGatewayChannelDeletionCoordinatorPreservesGroupChangeHistory(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000054_gateway_channel_deletion_coordinator.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	normalizedSQL := strings.Join(strings.Fields(sql), " ")
	for _, required := range []string{
		"drop constraint gateway_upstream_group_changes_upstream_group_id_fkey",
		"drop constraint fk_gateway_upstream_group_change_channel",
		"drop constraint fk_gateway_upstream_group_change_group_channel",
		"add constraint fk_gateway_upstream_group_change_group_channel foreign key (upstream_group_id, channel_id) references public.gateway_upstream_groups(id, channel_id) on delete set null (upstream_group_id)",
		"create index idx_gateway_upstream_group_changes_group_channel on public.gateway_upstream_group_changes (upstream_group_id, channel_id) where upstream_group_id is not null",
	} {
		if !strings.Contains(normalizedSQL, required) {
			t.Fatalf("channel deletion coordinator migration missing %q:\n%s", required, body)
		}
	}
	if strings.Contains(normalizedSQL, "foreign key (channel_id) references public.channels(id)") ||
		strings.Contains(normalizedSQL, "references public.gateway_upstream_groups(id, channel_id) on delete cascade") ||
		strings.Contains(normalizedSQL, "references public.gateway_upstream_groups(id, channel_id) on delete no action") {
		t.Fatalf("channel deletion coordinator still cascades group-change history:\n%s", body)
	}
	if strings.Count(normalizedSQL, "fk_gateway_upstream_group_change_channel") != 1 {
		t.Fatalf("channel FK must only be dropped, never recreated:\n%s", body)
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		if migration.version != 54 {
			continue
		}
		if migration.name != "000054_gateway_channel_deletion_coordinator.sql" || migration.nonTransactional {
			t.Fatalf("version 54 channel deletion migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 54 channel deletion migration was not loaded")
}

func TestGatewayChannelDeletionGroupChangeHistoryPostgres(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skipf("%s is not configured", testpostgres.EnvironmentDSN)
	}
	db, _ := testpostgres.OpenEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 54), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 54: %v", err)
	}

	var deleteAction, definition, setColumns string
	if err := db.QueryRowContext(ctx, `
		SELECT constraint_row.confdeltype::text,
		       pg_get_constraintdef(constraint_row.oid),
		       COALESCE((
		           SELECT string_agg(attribute.attname, ',' ORDER BY columns.ordinality)
		           FROM unnest(constraint_row.confdelsetcols) WITH ORDINALITY AS columns(attnum, ordinality)
		           JOIN pg_catalog.pg_attribute AS attribute
		             ON attribute.attrelid = constraint_row.conrelid
		            AND attribute.attnum = columns.attnum
		       ), '')
		FROM pg_catalog.pg_constraint AS constraint_row
		WHERE constraint_row.conrelid = 'public.gateway_upstream_group_changes'::regclass
		  AND constraint_row.conname = 'fk_gateway_upstream_group_change_group_channel'
	`).Scan(&deleteAction, &definition, &setColumns); err != nil {
		t.Fatalf("read group-change composite FK: %v", err)
	}
	if deleteAction != "n" || setColumns != "upstream_group_id" ||
		!strings.Contains(strings.ToLower(definition), "on delete set null (upstream_group_id)") {
		t.Fatalf("group-change composite FK action = action:%q columns:%q definition:%q", deleteAction, setColumns, definition)
	}
	var retiredFKCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_catalog.pg_constraint
		WHERE conrelid = 'public.gateway_upstream_group_changes'::regclass
		  AND conname IN (
		      'gateway_upstream_group_changes_upstream_group_id_fkey',
		      'fk_gateway_upstream_group_change_channel'
		  )
	`).Scan(&retiredFKCount); err != nil {
		t.Fatalf("count retired group-change FKs: %v", err)
	}
	if retiredFKCount != 0 {
		t.Fatalf("retired group-change FK count = %d, want 0", retiredFKCount)
	}

	insertChannel := func(name string) int64 {
		t.Helper()
		var channelID int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO public.channels (name, type, site_url, username, password_cipher)
			VALUES ($1, 'newapi', 'https://group-change-history.example', '', '')
			RETURNING id
		`, name).Scan(&channelID); err != nil {
			t.Fatalf("insert channel %q: %v", name, err)
		}
		return channelID
	}
	channelID := insertChannel("group-change-history-owner")
	otherChannelID := insertChannel("group-change-history-other")
	var groupID, generation int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_upstream_groups (
			channel_id, canonical_group_name, channel_type
		) VALUES ($1, 'primary', 'newapi')
		RETURNING id, presence_generation
	`, channelID).Scan(&groupID, &generation); err != nil {
		t.Fatalf("insert upstream group: %v", err)
	}
	var changeID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_upstream_group_changes (
			channel_id, group_name, upstream_group_id, presence_generation,
			kind, status, upstream_key_id, upstream_key_name, detected_at
		) VALUES ($1, 'primary', $2, $3, 'removed', 'acknowledged', 41, 'linked-key', CURRENT_TIMESTAMP)
		RETURNING id
	`, channelID, groupID, generation).Scan(&changeID); err != nil {
		t.Fatalf("insert group-change history: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_upstream_group_changes (
			channel_id, group_name, upstream_group_id, presence_generation,
			kind, status, detected_at
		) VALUES ($1, 'cross-channel', $2, $3, 'added', 'pending', CURRENT_TIMESTAMP)
	`, otherChannelID, groupID, generation); err == nil {
		t.Fatal("cross-channel group-change reference unexpectedly succeeded")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.channels WHERE id = $1`, channelID); err != nil {
		t.Fatalf("delete channel with group-change history: %v", err)
	}

	var retainedChannelID int64
	var retainedGroupID sql.NullInt64
	var retainedStatus, retainedKeyName string
	if err := db.QueryRowContext(ctx, `
		SELECT channel_id, upstream_group_id, status, upstream_key_name
		FROM public.gateway_upstream_group_changes
		WHERE id = $1
	`, changeID).Scan(&retainedChannelID, &retainedGroupID, &retainedStatus, &retainedKeyName); err != nil {
		t.Fatalf("read retained group-change history: %v", err)
	}
	if retainedChannelID != channelID || retainedGroupID.Valid || retainedStatus != "acknowledged" || retainedKeyName != "linked-key" {
		t.Fatalf("retained group-change history = channel:%d group:%v status:%q key:%q", retainedChannelID, retainedGroupID, retainedStatus, retainedKeyName)
	}
	var groupCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM public.gateway_upstream_groups WHERE id = $1`, groupID).Scan(&groupCount); err != nil {
		t.Fatalf("count deleted upstream group: %v", err)
	}
	if groupCount != 0 {
		t.Fatalf("deleted upstream group count = %d, want 0", groupCount)
	}
}

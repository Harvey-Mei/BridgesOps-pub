package migrations

import (
	"context"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestGatewayAssignmentOverrideGenerationMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000044_gateway_assignment_override_generation.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"add column presence_generation bigint not null default 0",
		"gateway_assignment_overrides_generation_nonnegative",
		"check (presence_generation >= 0)",
		"zero marks a legacy override that must not be applied",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("assignment override generation migration missing %q", required)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version == 44 {
			if migration.name != "000044_gateway_assignment_override_generation.sql" || migration.nonTransactional {
				t.Fatalf("version 44 assignment override migration = %#v", migration)
			}
			return
		}
	}
	t.Fatal("version 44 assignment override migration was not loaded")
}

func TestGatewayAssignmentOverrideGenerationMigrationLeavesLegacyOverridesDisabled(t *testing.T) {
	db, _ := testpostgres.OpenEmpty(t)
	ctx := context.Background()
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 43), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migrations through 43: %v", err)
	}
	var channelID, upstreamGroupID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.channels (name, type, site_url, username, password_cipher)
		VALUES ('override generation migration', 'newapi', 'https://override-generation.example', '', '')
		RETURNING id
	`).Scan(&channelID); err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO public.gateway_upstream_groups (
			channel_id, canonical_group_name, raw_group_names, present, presence_generation,
			discovered_at, model_family_codes, model_ids, unclassified_model_ids,
			ambiguous_model_ids, gateway_apps, gateway_app_sources, probed_gateway_apps,
			channel_type, capability_profile, classification_version, incomplete_reasons,
			source_revision
		) VALUES ($1, 'primary', '["primary"]', TRUE, 7, CURRENT_TIMESTAMP,
			'[]', '[]', '[]', '[]', '[]', '{}', '[]', 'newapi', '{}', '', '[]', '')
		RETURNING id
	`, channelID).Scan(&upstreamGroupID); err != nil {
		t.Fatalf("insert upstream group: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO public.gateway_upstream_group_assignment_overrides (
			upstream_group_id, action, reason, created_by
		) VALUES ($1, 'ignore', 'legacy override', 'migration test')
	`, upstreamGroupID); err != nil {
		t.Fatalf("insert legacy override: %v", err)
	}
	if err := applyMigrations(ctx, db, embeddedMigrationsThrough(t, 44), ".", defaultRunnerConfig()); err != nil {
		t.Fatalf("apply migration 44: %v", err)
	}
	var generation int64
	if err := db.QueryRowContext(ctx, `
		SELECT presence_generation
		FROM public.gateway_upstream_group_assignment_overrides
		WHERE upstream_group_id = $1
	`, upstreamGroupID).Scan(&generation); err != nil {
		t.Fatalf("read migrated override: %v", err)
	}
	if generation != 0 {
		t.Fatalf("legacy override generation = %d, want 0", generation)
	}
	var applicable int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM public.gateway_upstream_group_assignment_overrides AS overrides
		JOIN public.gateway_upstream_groups AS groups
		  ON groups.id = overrides.upstream_group_id
		 AND groups.presence_generation = overrides.presence_generation
		WHERE overrides.upstream_group_id = $1
	`, upstreamGroupID).Scan(&applicable); err != nil {
		t.Fatalf("check migrated override applicability: %v", err)
	}
	if applicable != 0 {
		t.Fatalf("legacy override remained applicable to the current generation: %d", applicable)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE public.gateway_upstream_group_assignment_overrides
		SET presence_generation = -1
		WHERE upstream_group_id = $1
	`, upstreamGroupID); err == nil {
		t.Fatal("negative override generation was accepted")
	}
}

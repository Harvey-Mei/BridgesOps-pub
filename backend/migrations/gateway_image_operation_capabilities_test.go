package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestGatewayImageOperationCapabilitiesMigration(t *testing.T) {
	raw, err := os.ReadFile("000030_gateway_image_operation_capabilities.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(raw))
	for _, required := range []string{
		"create table public.gateway_image_operation_capabilities",
		"upstream_group_id bigint not null references public.gateway_upstream_groups(id) on delete cascade",
		"presence_generation bigint not null",
		"task_mode in ('unknown', 'sync', 'async', 'sync_async')",
		"idempotency_key_supported boolean not null default false",
		"gateway_image_capability_idempotency_shape",
		"gateway_image_capability_task_id_shape",
		"gateway_image_capability_task_query_shape",
		"task_query_path_template varchar(512) not null default ''",
		"evidence_source in ('observed_success', 'provider_metadata', 'admin_config')",
		"gateway_image_capability_evidence_shape",
		"unique (upstream_group_id, presence_generation, model_name, operation, protocol)",
		"create table public.gateway_image_operation_capability_audits",
		"snapshot jsonb not null default '{}'::jsonb",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("image operation capability migration missing %q", required)
		}
	}
	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version == 30 {
			if migration.name != "000030_gateway_image_operation_capabilities.sql" || migration.nonTransactional {
				t.Fatalf("version 30 migration = %+v", migration)
			}
			return
		}
	}
	t.Fatal("version 30 image operation capability migration was not loaded")
}

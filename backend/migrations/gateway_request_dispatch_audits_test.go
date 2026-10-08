package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestDispatchAuditsMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000033_gateway_request_dispatch_audits.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.gateway_request_dispatch_audits",
		"stage character varying(16) not null default 'dispatched'",
		"billing_committed boolean not null default false",
		"provider_request_id character varying(512) not null default ''",
		"accepted_event_type character varying(64) not null default ''",
		"accepted_object_id character varying(128) not null default ''",
		"model_mapping_details character varying(1024) not null default ''",
		"session_hash character varying(64) not null default ''",
		"session_affinity_origin_request_id bigint not null default 0",
		"route_details character varying(4096) not null default ''",
		"route_attempted_channels character varying(16384) not null default ''",
		"foreign key (request_log_id) references public.gateway_request_logs(id) on delete cascade",
		"where terminal_at is null and (accepted_at is not null or billing_committed)",
		"where terminal_at is null and accepted_at is null and not billing_committed",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("request dispatch audit migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"request_body", "authorization_header", "raw_session_id", "private_upstream_response"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("request dispatch audit migration persisted forbidden field %q", forbidden)
		}
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 33 {
			continue
		}
		if migration.name != "000033_gateway_request_dispatch_audits.sql" || migration.nonTransactional {
			t.Fatalf("version 33 request dispatch audit migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 33 request dispatch audit migration was not loaded")
}

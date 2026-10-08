package migrations

import (
	"strings"
	"testing"
)

func TestGatewayRequestDispatchOwnerLeasesMigration(t *testing.T) {
	body, err := embeddedMigrations.ReadFile("000037_gateway_request_dispatch_owner_leases.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, required := range []string{
		"create table public.gateway_request_dispatch_owners",
		"instance_id character varying(32) primary key",
		"fence_token bigint not null",
		"lease_expires_at timestamp with time zone not null",
		"stopped_at timestamp with time zone",
		"idx_gateway_request_dispatch_owner_retention",
		"add column owner_instance_id character varying(32)",
		"add column owner_fence_token bigint",
		"add column legacy_ownerless boolean not null default false",
		"add column recovery_requested_at timestamp with time zone",
		"add column recovery_terminal_kind character varying(64) not null default ''",
		"set legacy_ownerless = true",
		"chk_gateway_request_dispatch_owner_pair",
		"legacy_ownerless and owner_instance_id is null and owner_fence_token is null",
		"not legacy_ownerless and owner_instance_id is not null and owner_fence_token is not null and owner_fence_token > 0",
		"foreign key (owner_instance_id, owner_fence_token)",
		"references public.gateway_request_dispatch_owners(instance_id, fence_token)",
		"on update restrict on delete restrict",
		"chk_gateway_request_dispatch_recovery_handoff",
		"recovery_requested_at is null and btrim(recovery_terminal_kind) = ''",
		"recovery_requested_at is not null and btrim(recovery_terminal_kind) <> ''",
		"idx_gateway_request_dispatch_recovery_handoff",
		"where terminal_at is null",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("request dispatch owner lease migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"hostname", "ip_address", "process_command", "environment"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("request dispatch owner lease migration persisted forbidden identity %q", forbidden)
		}
	}
	legacyUpdate := strings.Index(sql, "set legacy_ownerless = true")
	ownerPairConstraint := strings.Index(sql, "chk_gateway_request_dispatch_owner_pair")
	if legacyUpdate < 0 || ownerPairConstraint < 0 || legacyUpdate > ownerPairConstraint {
		t.Fatalf("migration must mark only pre-existing ownerless rows before enforcing the owner-pair constraint")
	}

	migrations, err := loadMigrations(embeddedMigrations, embeddedMigrationsRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.version != 37 {
			continue
		}
		if migration.name != "000037_gateway_request_dispatch_owner_leases.sql" || migration.nonTransactional {
			t.Fatalf("version 37 request dispatch owner lease migration = %#v", migration)
		}
		return
	}
	t.Fatal("version 37 request dispatch owner lease migration was not loaded")
}

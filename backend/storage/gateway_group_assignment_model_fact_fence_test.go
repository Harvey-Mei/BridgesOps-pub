package storage

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestAutomaticAssignmentBackfillModelFactFenceDoesNotRetargetAccount(t *testing.T) {
	key := GatewayUpstreamKey{
		ID:                                11,
		ChannelID:                         7,
		GroupName:                         "Primary",
		ModelFactsUpstreamGroupID:         13,
		ModelFactsPresenceGeneration:      17,
		ModelFactsChannelIdentityRevision: 19,
	}
	row := gatewayAutomaticAssignmentBackfillKey{
		GatewayUpstreamKey:          key,
		ModelFactCanonicalGroupName: "primary",
	}

	fence, err := row.modelFactFence()
	if err != nil {
		t.Fatalf("build observed model-fact fence: %v", err)
	}
	if !fence.MatchesUpstreamKey(&key) {
		t.Fatalf("observed fence does not match its source key: fence=%+v key=%+v", fence, key)
	}

	accountB := key
	accountB.ModelFactsChannelIdentityRevision++
	if fence.MatchesUpstreamKey(&accountB) {
		t.Fatalf("account A backfill fence matched account B key: fence=%+v key=%+v", fence, accountB)
	}
	if fence.ChannelIdentityRevision != key.ModelFactsChannelIdentityRevision {
		t.Fatalf("backfill fence was retargeted: got revision %d want %d", fence.ChannelIdentityRevision, key.ModelFactsChannelIdentityRevision)
	}
}

func TestAutomaticAssignmentBackfillModelFactFenceRejectsUnscopedKey(t *testing.T) {
	_, err := (gatewayAutomaticAssignmentBackfillKey{
		GatewayUpstreamKey:          GatewayUpstreamKey{ID: 11, ChannelID: 7, GroupName: "primary"},
		ModelFactCanonicalGroupName: "primary",
	}).modelFactFence()
	if !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("unscoped backfill fence error = %v, want %v", err, ErrUpstreamGroupGenerationChanged)
	}
}

func TestAutomaticAssignmentBackfillQueryRequiresCurrentModelFactIdentity(t *testing.T) {
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 port=1 user=bridgeops_test database=bridgeops_test sslmode=disable")
	if err != nil {
		t.Fatalf("open lazy PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true,
		DryRun:               true,
	})
	if err != nil {
		t.Fatalf("open dry-run PostgreSQL database: %v", err)
	}

	rows := []gatewayAutomaticAssignmentBackfillKey{}
	query := currentGatewayAutomaticAssignmentBackfillKeysQuery(db, 23, 41).Find(&rows)
	if query.Error != nil {
		t.Fatalf("build backfill query: %v", query.Error)
	}
	generated := strings.ToLower(query.Statement.SQL.String())
	for _, required := range []string{
		"model_fact_groups.id = keys.model_facts_upstream_group_id",
		"model_fact_groups.presence_generation = keys.model_facts_presence_generation",
		"model_fact_groups.canonical_group_name = lower(btrim(keys.group_name))",
		"model_fact_groups.present = true",
		"model_fact_channels.upstream_account_identity_revision = keys.model_facts_channel_identity_revision",
	} {
		if !strings.Contains(generated, required) {
			t.Fatalf("backfill query lost identity predicate %q: %s", required, generated)
		}
	}
}

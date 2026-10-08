package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFilterExactModelIDsIsCaseSensitive(t *testing.T) {
	got := filterExactModelIDs([]string{"Vendor-Alias", "vendor-alias", "other"}, []string{"Vendor-Alias"})
	if len(got) != 1 || got[0] != "Vendor-Alias" {
		t.Fatalf("direct model-check recent model filter = %+v", got)
	}
}

func TestListModelCheckSourcesUsesCandidatesAndRealTrafficOnly(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{Name: "source", Type: ChannelTypeNewAPI, GatewayApp: GatewayAppCodex, SiteURL: "https://example.invalid", Username: "user", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	store := NewGateway(db)
	for _, key := range []GatewayUpstreamKey{
		{ChannelID: channel.ID, GroupName: "used", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", APIKeyPrefix: "sk", ModelIDs: "gpt-5,gpt-5-mini", LatencyProbeModel: "gpt-5-mini", Enabled: true},
		{ChannelID: channel.ID, GroupName: "candidate", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", APIKeyPrefix: "sk", ModelIDs: "gpt-5-mini", LatencyProbeModel: "gpt-5-mini", Enabled: true},
		{ChannelID: channel.ID, GroupName: "internal-only", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", APIKeyPrefix: "sk", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test", Enabled: true},
	} {
		key := key
		if err := store.UpsertUpstreamKey(&key); err != nil {
			t.Fatalf("upsert %s: %v", key.GroupName, err)
		}
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "candidate", false); err != nil {
		t.Fatalf("pause candidate latency probe: %v", err)
	}
	rates := NewRates(db)
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "candidate", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert candidate rate: %v", err)
	}
	group := &GatewayCandidateGroup{Name: "active"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "candidate"}}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	now := time.Now()
	for _, log := range []GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "used", ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 10, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "used", ModelName: "fake-api-test", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 10, RouteReason: "api_test", CreatedAt: now},
		{ChannelID: channel.ID, GroupName: "internal-only", ModelName: "fake-probe", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 10, RouteReason: "latency_probe", CreatedAt: now},
	} {
		log := log
		if err := store.AppendRequestLog(&log); err != nil {
			t.Fatalf("append request: %v", err)
		}
	}
	sources, err := store.ListModelCheckSources(now.Add(time.Second))
	if err != nil {
		t.Fatalf("list sources: %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("sources = %+v", sources)
	}
	byGroup := map[string]GatewayModelCheckSource{}
	for _, source := range sources {
		byGroup[source.GroupName] = source
	}
	if got := byGroup["used"].RecentModels; len(got) != 1 || got[0] != "gpt-5" {
		t.Fatalf("used recent models = %#v", got)
	}
	if got := byGroup["candidate"].LatencyProbeModel; got != "gpt-5-mini" {
		t.Fatalf("candidate fallback = %q", got)
	}
	if _, ok := byGroup["internal-only"]; ok {
		t.Fatal("internal traffic created a detection source")
	}
}

func TestSyncModelCheckTargetsPreservesOperatorDisableAndAuditHistory(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "model-check-sync", Type: ChannelTypeNewAPI, GatewayApp: GatewayAppCodex,
		SiteURL: "https://model-check-sync.invalid",
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	store := NewModelChecks(db)
	now := time.Now()
	discovery := ModelCheckTargetDiscovery{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ChannelName: "One", GroupName: "gpt", ModelName: "gpt-5", App: GatewayAppCodex,
	}
	targets, err := store.SyncTargets([]ModelCheckTargetDiscovery{discovery}, now)
	if err != nil || len(targets) != 1 {
		t.Fatalf("first sync = %+v, %v", targets, err)
	}
	originalTargetID := targets[0].ID
	if err := store.SetTargetEnabled(targets[0].ID, false); err != nil {
		t.Fatalf("disable target: %v", err)
	}
	targets, err = store.SyncTargets([]ModelCheckTargetDiscovery{discovery}, now.Add(time.Hour))
	if err != nil || len(targets) != 1 || targets[0].Enabled {
		t.Fatalf("operator disable was lost: %+v, %v", targets, err)
	}
	replacement := discovery
	replacement.ModelName = "gpt-5-mini"
	if _, err := store.SyncTargets([]ModelCheckTargetDiscovery{replacement}, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("mark missing current target inactive: %v", err)
	}
	stored, err := store.FindTarget(originalTargetID)
	if err != nil {
		t.Fatalf("find inactive target: %v", err)
	}
	if stored.Active {
		t.Fatal("missing target remained active")
	}
	var count int64
	if err := db.Model(&ModelCheckTarget{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("target audit row count = %d, %v", count, err)
	}
}

func TestSyncModelCheckTargetsStaleDiscoveryCannotDeactivateCurrentAccountTarget(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "model-check-identity", Type: ChannelTypeNewAPI, GatewayApp: GatewayAppCodex,
		SiteURL: "https://model-check-identity.invalid", Username: "account-a", PasswordCipher: "credential-a",
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create account A channel: %v", err)
	}
	store := NewModelChecks(db)
	now := time.Now().UTC()
	accountADiscovery := ModelCheckTargetDiscovery{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ChannelName: channel.Name, GroupName: "gpt", ModelName: "gpt-5", App: GatewayAppCodex,
	}
	targets, err := store.SyncTargets([]ModelCheckTargetDiscovery{accountADiscovery}, now)
	if err != nil || len(targets) != 1 {
		t.Fatalf("sync account A target = %+v, %v", targets, err)
	}

	accountB := *channel
	accountB.Username = "account-b"
	accountB.PasswordCipher = "credential-b"
	if err := channels.UpdateWithUpstreamAccountIdentityChange(&accountB); err != nil {
		t.Fatalf("switch to account B: %v", err)
	}
	if accountB.UpstreamAccountIdentityRevision == accountADiscovery.UpstreamAccountIdentityRevision {
		t.Fatal("account identity revision did not advance")
	}
	accountBDiscovery := accountADiscovery
	accountBDiscovery.UpstreamAccountIdentityRevision = accountB.UpstreamAccountIdentityRevision
	targets, err = store.SyncTargets([]ModelCheckTargetDiscovery{accountBDiscovery}, now.Add(time.Minute))
	if err != nil || len(targets) != 1 || !targets[0].Active {
		t.Fatalf("sync account B target = %+v, %v", targets, err)
	}
	// A descriptor-only empty result has no identity-attested channel scope, so
	// it also cannot be used to deactivate account B's target.
	targets, err = store.SyncTargets(nil, now.Add(90*time.Second))
	if err != nil || len(targets) != 1 || !targets[0].Active {
		t.Fatalf("unscoped empty discovery deactivated B target: %+v, %v", targets, err)
	}

	// This models an A discovery result that arrived after B was already
	// discovered. It must be discarded, not interpreted as B having no targets.
	targets, err = store.SyncTargets([]ModelCheckTargetDiscovery{accountADiscovery}, now.Add(2*time.Minute))
	if err != nil || len(targets) != 1 || !targets[0].Active {
		t.Fatalf("late account A discovery deactivated B target: %+v, %v", targets, err)
	}
	if targets[0].UpstreamAccountIdentityRevision != accountB.UpstreamAccountIdentityRevision {
		t.Fatalf("visible target revision = %d, want B revision %d", targets[0].UpstreamAccountIdentityRevision, accountB.UpstreamAccountIdentityRevision)
	}
}

func TestPostgresModelCheckMigrationContainsRuntimeSchema(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "migrations", "000002_model_checks.sql"))
	if err != nil {
		t.Fatalf("read model check migration: %v", err)
	}
	schema := strings.ToLower(string(raw))
	for _, required := range []string{
		"create table public.model_check_targets",
		"create unique index idx_model_check_target",
		"create table public.model_check_runs",
		"idx_model_check_runs_target_time",
		"last_discovered_at timestamp with time zone",
		"evidence_json text not null",
		"finished_at timestamp with time zone not null",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres model-check migration missing %q", required)
		}
	}
}

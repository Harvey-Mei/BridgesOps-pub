package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestProvisioningUpstreamKeyIsUnavailableInProbeAndManualPreview(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "provisioning", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://provisioning.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "group", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "group", APIKeyCipher: "cipher", Enabled: true, Provisioning: true,
		ModelFamilies: "codex", ModelIDs: "gpt-5.4", LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert provisioning key: %v", err)
	}
	candidateGroup := &storage.GatewayCandidateGroup{Name: "provisioning preview"}
	if err := db.Create(candidateGroup).Error; err != nil {
		t.Fatalf("create candidate group: %v", err)
	}
	if err := db.Create(&storage.GatewayCandidateGroupMember{CandidateGroupID: candidateGroup.ID, ChannelID: channel.ID, GroupName: "group"}).Error; err != nil {
		t.Fatalf("create legacy provisioning member: %v", err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name: "preview", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: candidateGroup.ID,
		KeyHash: "provisioning-preview", APIKeyCipher: "downstream-cipher", Enabled: true, SmartRouting: false,
	}
	if err := db.Create(downstream).Error; err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	svc := NewService(channels, rates, store, nil, nil, nil)
	config, err := svc.ProbeConfig()
	if err != nil || len(config) != 1 || len(config[0].Groups) != 1 {
		t.Fatalf("probe config = %+v err=%v", config, err)
	}
	group := config[0].Groups[0]
	if group.UpstreamGroupID == 0 || group.PresenceGeneration == 0 {
		t.Fatalf("probe config omitted stable generation identity: %+v", group)
	}
	if !group.UpstreamKeyConfigured || !group.UpstreamKeyEnabled || !group.UpstreamKeyProvisioning || group.UpstreamKeyAvailable || group.ProbeEligible {
		t.Fatalf("provisioning key projection = %+v", group)
	}
	if !strings.Contains(strings.Join(group.ProbeBlockers, ","), probeBlockerKeyUnavailable) || config[0].RoutingAvailable {
		t.Fatalf("provisioning key availability = channel=%+v group=%+v", config[0], group)
	}

	previews, err := svc.ManualRouteMemberPreviewForDownstreamKey(downstream.ID)
	if err != nil || len(previews) != 1 {
		t.Fatalf("manual preview = %+v err=%v", previews, err)
	}
	preview := previews[0]
	if !preview.UpstreamKeyConfigured || !preview.UpstreamKeyEnabled || !preview.UpstreamKeyProvisioning || preview.UpstreamKeyAvailable || preview.RoutingAvailable {
		t.Fatalf("manual provisioning preview = %+v", preview)
	}
	if !strings.Contains(strings.Join(preview.AvailabilityReasons, ","), probeBlockerKeyUnavailable) {
		t.Fatalf("manual preview reasons = %v", preview.AvailabilityReasons)
	}
	if candidates, err := svc.ManualCandidateForApp(downstream.ID, downstream.App, channel.ID, "group", "gpt-5.4"); err == nil || len(candidates) != 0 {
		t.Fatalf("fixed route accepted provisioning key: candidates=%+v err=%v", candidates, err)
	}
	if candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, ""); err == nil || len(candidates) != 0 {
		t.Fatalf("smart route accepted provisioning key: candidates=%+v err=%v", candidates, err)
	}
}

func TestLatestProbeConfigStateAtExcludesStaleRoutingHealth(t *testing.T) {
	updatedAt := time.Now().UTC().Add(-3 * time.Minute)
	modelsSyncedAt := updatedAt.Add(time.Minute)
	staleFailureAt := updatedAt.Add(2 * time.Minute)
	key := &storage.GatewayUpstreamKey{
		UpdatedAt: updatedAt, ModelsSyncedAt: &modelsSyncedAt, LastRoutingFailureAt: &staleFailureAt,
	}
	if got := latestProbeConfigStateAt(key, false); got == nil || !got.Equal(modelsSyncedAt) {
		t.Fatalf("state time included stale routing health: got=%v want=%v", got, modelsSyncedAt)
	}
	if got := latestProbeConfigStateAt(key, true); got == nil || !got.Equal(staleFailureAt) {
		t.Fatalf("current routing health time was omitted: got=%v want=%v", got, staleFailureAt)
	}
}

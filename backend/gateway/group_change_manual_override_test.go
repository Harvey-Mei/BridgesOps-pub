package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestResolveUpstreamGroupChangeAllowsIncompleteCurrentGeneration(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "manual incomplete", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://manual-incomplete.invalid", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: "new-pool", Ratio: 1, LastSeenAt: time.Now(),
	}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "new-pool", UpstreamKeyID: 17, UpstreamKeyName: "managed-new-pool", APIKeyCipher: "cipher",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		ModelFamilies:       "codex", Enabled: true, LatencyProbeEnabled: false,
	}); err != nil {
		t.Fatalf("upsert unsynchronized key: %v", err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "new-pool", false); err != nil {
		t.Fatalf("disable probe for unsynchronized key: %v", err)
	}
	configured, err := store.FindUpstreamKeyAny(channel.ID, "new-pool")
	if err != nil || configured.LatencyProbeEnabled {
		t.Fatalf("unsynchronized key probe state = %+v err=%v", configured, err)
	}
	target := &storage.GatewayCandidateGroup{Name: "codex target", ModelMappingFamily: "codex"}
	// This fixture intentionally starts with an empty existing target: the
	// manual assignment under test supplies its first member without requiring
	// the incomplete upstream group to pass the normal synchronized-capability
	// validation used by candidate-group editing.
	if err := db.Create(target).Error; err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "new-pool", "added", 17, "managed-new-pool", time.Now()); err != nil {
		t.Fatalf("record upstream group change: %v", err)
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 {
		t.Fatalf("pending changes = %+v err=%v", changes, err)
	}

	previews, err := store.PreviewAutomaticAssignments([]uint{changes[0].ID}, true)
	if err != nil || len(previews) != 1 {
		t.Fatalf("automatic preview = %+v err=%v", previews, err)
	}
	preview := previews[0]
	if preview.State != "awaiting_facts" || preview.ReasonCode != "facts_incomplete" {
		t.Fatalf("automatic preview accepted incomplete facts: %+v", preview)
	}
	if _, err := store.ExecuteAutomaticAssignment(preview.ChangeID, preview.EvaluationToken, "administrator", storage.GatewayAssignmentTriggerManual); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("automatic execute with incomplete facts error = %v", err)
	}

	svc := NewService(channels, rates, store, nil, nil, nil)
	if err := svc.ResolveUpstreamGroupChange(changes[0].ID, []uint{target.ID}, false); err != nil {
		t.Fatalf("manual assignment with incomplete facts and probing disabled: %v", err)
	}
	resolved, err := store.FindUpstreamGroupChange(changes[0].ID)
	if err != nil {
		t.Fatalf("find resolved change: %v", err)
	}
	if resolved.Status != "assigned" || resolved.AssignmentReasonCode != "manual_override_incomplete_facts" {
		t.Fatalf("resolved change = %+v", resolved)
	}
	group, err := store.FindCandidateGroup(target.ID)
	if err != nil || len(group.Members) != 1 || group.Members[0].ChannelID != channel.ID || group.Members[0].GroupName != "new-pool" {
		t.Fatalf("candidate group after manual assignment = %+v err=%v", group, err)
	}
}

func TestCachedGatewayAppFromKeyRejectsProvisioningKey(t *testing.T) {
	key := &storage.GatewayUpstreamKey{
		ID: 1, Enabled: true, Provisioning: true,
		APIKeyCipher: "cipher", ModelFamilies: "codex", ModelIDs: "gpt-5.4",
	}
	if _, ok := cachedGatewayAppFromKey(key, storage.GatewayAppCodex); ok {
		t.Fatal("provisioning key entered cached candidate routing")
	}
	key.Provisioning = false
	info, ok := cachedGatewayAppFromKey(key, storage.GatewayAppCodex)
	if !ok || info.App != storage.GatewayAppCodex {
		t.Fatalf("ready key was excluded from cached candidate routing: info=%+v ok=%t", info, ok)
	}
}

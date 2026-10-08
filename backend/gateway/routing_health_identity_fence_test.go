package gateway

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func mustCurrentRoutingHealthFence(t testing.TB, store *storage.Gateway, channel storage.Channel, groupName string) storage.GatewayRoutingHealthFence {
	t.Helper()
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, groupName)
	if err != nil {
		t.Fatal(err)
	}
	fence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	if !fence.Valid() {
		t.Fatalf("invalid current routing-health fence: %+v", fence)
	}
	return fence
}

func TestCandidatesIgnoreRoutingHealthFromAnotherChannelIdentity(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "routing health identity", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://routing-health-identity.invalid", PasswordCipher: "cipher", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", Enabled: true, APIKeyCipher: "cipher",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
	}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatal(err)
	}
	failureAt := time.Now().UTC().Truncate(time.Microsecond)
	cooldownUntil := failureAt.Add(10 * time.Minute)
	updates := map[string]any{
		"last_routing_failure_error":               "other account failure",
		"last_routing_failure_kind":                RoutingFailureTransport,
		"last_routing_failure_status":              0,
		"last_routing_failure_at":                  failureAt,
		"routing_failure_count":                    4,
		"routing_cooldown_until":                   cooldownUntil,
		"routing_isolated_at":                      failureAt,
		"routing_health_upstream_group_id":         generation.ID,
		"routing_health_presence_generation":       generation.PresenceGeneration,
		"routing_health_channel_identity_revision": channel.UpstreamAccountIdentityRevision + 1,
	}
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, generation.CanonicalGroupName).
		Updates(updates).Error; err != nil {
		t.Fatal(err)
	}

	service := NewService(channels, rates, store, nil, nil, nil)
	candidates, exclusions, err := service.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil || len(candidates) != 1 || len(exclusions) != 0 || candidates[0].CoolingDown {
		t.Fatalf("foreign-identity health affected candidates: candidates=%+v exclusions=%+v err=%v", candidates, exclusions, err)
	}
	config, err := service.ProbeConfig()
	if err != nil || len(config) != 1 || !config[0].RoutingAvailable || len(config[0].Groups) != 1 || config[0].Groups[0].CoolingDown {
		t.Fatalf("foreign-identity health affected probe config: config=%+v err=%v", config, err)
	}
	fence, ok := RoutingHealthFenceForCandidate(candidates[0])
	if !ok || fence.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		fence.UpstreamGroupID != generation.ID || fence.PresenceGeneration != generation.PresenceGeneration {
		t.Fatalf("candidate routing-health fence = %+v ok=%t", fence, ok)
	}

	updates["routing_health_channel_identity_revision"] = channel.UpstreamAccountIdentityRevision
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, generation.CanonicalGroupName).
		Updates(updates).Error; err != nil {
		t.Fatal(err)
	}
	candidates, exclusions, err = service.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err == nil || len(candidates) != 0 || len(exclusions) != 1 || exclusions[0].Reason != candidateExclusionReasonUpstreamTemporarilyUnavailable {
		t.Fatalf("current-identity health was ignored: candidates=%+v exclusions=%+v err=%v", candidates, exclusions, err)
	}
	config, err = service.ProbeConfig()
	if err != nil || len(config) != 1 || config[0].RoutingAvailable || len(config[0].Groups) != 1 || !config[0].Groups[0].CoolingDown {
		t.Fatalf("current-identity health was ignored by probe config: config=%+v err=%v", config, err)
	}
}

func TestRoutingHealthFenceForCandidateRejectsIncompleteOrMismatchedIdentity(t *testing.T) {
	candidate := Candidate{
		Channel: storage.Channel{ID: 7, UpstreamAccountIdentityRevision: 3},
		Rate:    storage.RateSnapshot{ChannelID: 7, ModelName: "Primary"},
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: 11, ChannelID: 7, CanonicalGroupName: "primary", PresenceGeneration: 5,
		},
	}
	if fence, ok := RoutingHealthFenceForCandidate(candidate); !ok || !fence.Valid() {
		t.Fatalf("valid candidate fence = %+v ok=%t", fence, ok)
	}
	for _, mutate := range []func(*Candidate){
		func(value *Candidate) { value.Channel.UpstreamAccountIdentityRevision = 0 },
		func(value *Candidate) { value.Channel.ID++ },
		func(value *Candidate) { value.Rate.ModelName = "other" },
		func(value *Candidate) { value.UpstreamGeneration.PresenceGeneration = 0 },
	} {
		invalid := candidate
		mutate(&invalid)
		if fence, ok := RoutingHealthFenceForCandidate(invalid); ok || fence.Valid() {
			t.Fatalf("invalid candidate produced fence: candidate=%+v fence=%+v ok=%t", invalid, fence, ok)
		}
	}
}
